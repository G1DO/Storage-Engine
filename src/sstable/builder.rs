use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::error::Result;
use crate::sstable::block::builder::BlockBuilder;
use crate::sstable::footer::{Footer, IndexEntry, SSTableMeta, SSTABLE_MAGIC};

/// Builds an SSTable file from a sorted stream of key-value pairs.
///
/// Used during:
/// - Memtable flush (sorted memtable → SSTable)
/// - Compaction (merged iterators → new SSTables)
///
/// Build process:
/// 1. Add entries one by one (must be in sorted order)
/// 2. Entries fill up blocks; when a block is full it's written to disk
/// 3. finish() flushes the last block, writes index, footer, fsync
pub struct SSTableBuilder {
    /// Current block being filled with entries.
    block_builder: BlockBuilder,
    /// Index entries: one per flushed data block.
    index_entries: Vec<IndexEntry>,
    /// Tracks current write position in the file.
    data_offset: u64,
    /// Buffered file writer.
    writer: BufWriter<File>,
    /// Unique SSTable identifier.
    sst_id: u64,
    /// Target block size.
    block_size: usize,
    /// Smallest key added (first key, since entries are sorted).
    min_key: Option<Vec<u8>>,
    /// Largest key added (updated on every add).
    max_key: Option<Vec<u8>>,
    /// Total entries added.
    entry_count: u64,
    /// Last key added to the current block (needed for index entry).
    last_key_in_block: Option<Vec<u8>>,
}

impl SSTableBuilder {
    /// Create a new SSTable builder that writes to the given path.
    pub fn new(path: &Path, sst_id: u64, block_size: usize) -> Result<Self> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        Ok(SSTableBuilder {
            block_builder: BlockBuilder::new(block_size),
            index_entries: Vec::new(),
            data_offset: 0,
            writer,
            sst_id,
            block_size,
            min_key: None,
            max_key: None,
            entry_count: 0,
            last_key_in_block: None,
        })
    }

    /// Add a key-value pair. MUST be called in sorted key order.
    ///
    /// Internally:
    /// 1. Try adding to the current block
    /// 2. If block is full: flush block to file, record index entry, start new block
    /// 3. Add the entry to the new block
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // Track min/max keys
        if self.min_key.is_none() {
            self.min_key = Some(key.to_vec());
        }
        self.max_key = Some(key.to_vec());
        self.entry_count += 1;

        // Try adding to current block
        if self.block_builder.add(key, value) {
            self.last_key_in_block = Some(key.to_vec());
            return Ok(());
        }

        // Block is full — flush it, then add to a fresh block
        self.flush_block()?;

        // Add to the new block (guaranteed to succeed — first entry always accepted)
        assert!(self.block_builder.add(key, value));
        self.last_key_in_block = Some(key.to_vec());

        Ok(())
    }

    /// Flush the current block to disk and record an index entry.
    fn flush_block(&mut self) -> Result<()> {
        if self.block_builder.is_empty() {
            return Ok(());
        }

        // Take the current block builder, replace with a fresh one
        let old_builder = std::mem::replace(
            &mut self.block_builder,
            BlockBuilder::new(self.block_size),
        );
        let block_data = old_builder.build();
        let block_size = block_data.len() as u64;

        // Write block bytes to file
        self.writer.write_all(&block_data)?;

        // Record where this block landed
        self.index_entries.push(IndexEntry {
            last_key: self.last_key_in_block.take().unwrap(),
            offset: self.data_offset,
            size: block_size,
        });

        self.data_offset += block_size;
        Ok(())
    }

    /// Finalize the SSTable: flush last block, write index block, footer, fsync.
    pub fn finish(mut self) -> Result<SSTableMeta> {
        // 1. Flush the last data block
        self.flush_block()?;

        // 2. Meta block placeholder (bloom filter added in M18)
        let meta_block_offset = self.data_offset;
        let meta_block_size = 0u64;

        // 3. Write index block: serialize all index entries sequentially
        let index_block_offset = self.data_offset;
        let mut index_data = Vec::new();
        for entry in &self.index_entries {
            index_data.extend_from_slice(&entry.encode());
        }
        let index_block_size = index_data.len() as u64;
        self.writer.write_all(&index_data)?;

        // 4. Write footer
        let footer = Footer {
            index_block_offset,
            index_block_size,
            meta_block_offset,
            meta_block_size,
            magic: SSTABLE_MAGIC,
        };
        self.writer.write_all(&footer.encode())?;

        // 5. Flush buffer + fsync to guarantee durability
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        let file_size = index_block_offset + index_block_size + Footer::SIZE as u64;

        Ok(SSTableMeta {
            id: self.sst_id,
            level: 0,
            min_key: self.min_key.unwrap_or_default(),
            max_key: self.max_key.unwrap_or_default(),
            file_size,
            entry_count: self.entry_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::footer::Footer;
    use std::io::Read;
    use tempfile::tempdir;

    #[test]
    fn build_sstable_from_sorted_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
        for i in 0..100u32 {
            let key = format!("key_{:05}", i);
            let val = format!("val_{:05}", i);
            builder.add(key.as_bytes(), val.as_bytes()).unwrap();
        }
        let meta = builder.finish().unwrap();

        assert_eq!(meta.id, 1);
        assert_eq!(meta.entry_count, 100);
        assert_eq!(meta.min_key, b"key_00000");
        assert_eq!(meta.max_key, b"key_00099");
        assert!(meta.file_size > 0);
        assert!(path.exists());
    }

    #[test]
    fn finish_returns_correct_meta() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mut builder = SSTableBuilder::new(&path, 42, 4096).unwrap();
        builder.add(b"alpha", b"first").unwrap();
        builder.add(b"omega", b"last").unwrap();
        let meta = builder.finish().unwrap();

        assert_eq!(meta.id, 42);
        assert_eq!(meta.level, 0);
        assert_eq!(meta.min_key, b"alpha");
        assert_eq!(meta.max_key, b"omega");
        assert_eq!(meta.entry_count, 2);
    }

    #[test]
    fn file_ends_with_valid_footer() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
        for i in 0..50u32 {
            let key = format!("k{:04}", i);
            builder.add(key.as_bytes(), b"v").unwrap();
        }
        builder.finish().unwrap();

        // Read last 40 bytes = footer
        let mut file = File::open(&path).unwrap();
        let file_len = file.metadata().unwrap().len();
        let mut buf = vec![0u8; file_len as usize];
        file.read_exact(&mut buf).unwrap();

        let footer_bytes = &buf[buf.len() - Footer::SIZE..];
        let footer = Footer::decode(footer_bytes).unwrap();
        assert_eq!(footer.magic, SSTABLE_MAGIC);
        assert!(footer.index_block_offset > 0);
        assert!(footer.index_block_size > 0);
    }

    #[test]
    fn multiple_blocks_produced() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        // Use tiny block size to force multiple blocks
        let mut builder = SSTableBuilder::new(&path, 1, 64).unwrap();
        for i in 0..20u32 {
            let key = format!("key_{:05}", i);
            let val = format!("value_{:05}", i);
            builder.add(key.as_bytes(), val.as_bytes()).unwrap();
        }
        let meta = builder.finish().unwrap();

        assert_eq!(meta.entry_count, 20);
        // With 64-byte blocks and ~22 byte entries, we should have many blocks
        // File should be larger than a single block
        assert!(meta.file_size > 64);
    }
}
