use std::cell::RefCell;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::sstable::block::reader::Block;
use crate::sstable::footer::{Footer, IndexEntry, SSTableMeta};
use crate::sstable::iterator::SSTableIterator;

// TODO [M15]: Implement range iteration

/// An opened SSTable file. Supports point lookups and range scans.
///
/// On open:
/// 1. Read footer (last N bytes) → find index and meta block positions
/// 2. Read and parse index block → Vec<IndexEntry>
/// 3. Read and deserialize bloom filter
/// 4. Ready for queries (data blocks read on demand)
pub struct SSTable {
    /// Path to the SSTable file (for debugging/error messages).
    path: PathBuf,
    /// Open file handle for reading data blocks.
    /// Wrapped in RefCell to allow interior mutability for seeking/reading.
    file: RefCell<File>,
    /// Index entries parsed from the index block.
    /// Each entry maps a block's last key to its file location.
    index: Vec<IndexEntry>,
    /// Metadata about this SSTable (min/max keys, entry count, etc.).
    meta: SSTableMeta,
    /// Footer with offsets to index and meta blocks.
    footer: Footer,
}

impl SSTable {
    /// Open an SSTable file.
    ///
    /// Reads the footer from the end of the file, then uses footer
    /// offsets to read and parse the index block into memory.
    pub fn open(path: &Path) -> Result<Self> {
        // Open file for reading
        let mut file = File::open(path)?;

        // Get file size to locate footer
        let file_size = file.metadata()?.len();
        if file_size < Footer::SIZE as u64 {
            return Err(crate::error::Error::Corruption(
                "file too short to contain footer".into(),
            ));
        }

        // Read footer (last 40 bytes)
        let footer_offset = file_size - Footer::SIZE as u64;
        file.seek(SeekFrom::Start(footer_offset))?;
        let mut footer_buf = vec![0u8; Footer::SIZE];
        file.read_exact(&mut footer_buf)?;
        let footer = Footer::decode(&footer_buf)?;

        // Read index block
        file.seek(SeekFrom::Start(footer.index_block_offset))?;
        let mut index_buf = vec![0u8; footer.index_block_size as usize];
        file.read_exact(&mut index_buf)?;

        // Parse index entries
        let mut index = Vec::new();
        let mut offset = 0usize;
        while offset < index_buf.len() {
            let (entry, consumed) = IndexEntry::decode(&index_buf[offset..])?;
            index.push(entry);
            offset += consumed;
        }

        // Read meta block and parse SSTableMeta
        // Format: [id(8B)][level(4B)][min_key_len(4B)][min_key][max_key_len(4B)][max_key][entry_count(8B)]
        file.seek(SeekFrom::Start(footer.meta_block_offset))?;
        let mut meta_buf = vec![0u8; footer.meta_block_size as usize];
        file.read_exact(&mut meta_buf)?;

        let meta = if meta_buf.is_empty() {
            // Empty meta block - this shouldn't happen for valid SSTables
            // but we'll create a minimal one
            SSTableMeta {
                id: 0,
                level: 0,
                min_key: vec![],
                max_key: vec![],
                file_size,
                entry_count: 0,
            }
        } else {
            Self::parse_meta(&meta_buf, file_size)?
        };

        Ok(Self {
            path: path.to_path_buf(),
            file: RefCell::new(file),
            index,
            meta,
            footer,
        })
    }

    /// Parse SSTableMeta from bytes.
    fn parse_meta(data: &[u8], file_size: u64) -> Result<SSTableMeta> {
        use crate::error::Error;

        let mut offset = 0usize;

        // id (8 bytes)
        if data.len() < 8 {
            return Err(Error::Corruption("meta block too short for id".into()));
        }
        let id = u64::from_le_bytes(data[0..8].try_into().unwrap());
        offset += 8;

        // level (4 bytes)
        if data.len() < offset + 4 {
            return Err(Error::Corruption("meta block too short for level".into()));
        }
        let level = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // min_key_len (4 bytes) + min_key
        if data.len() < offset + 4 {
            return Err(Error::Corruption(
                "meta block too short for min_key_len".into(),
            ));
        }
        let min_key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if data.len() < offset + min_key_len {
            return Err(Error::Corruption("meta block too short for min_key".into()));
        }
        let min_key = data[offset..offset + min_key_len].to_vec();
        offset += min_key_len;

        // max_key_len (4 bytes) + max_key
        if data.len() < offset + 4 {
            return Err(Error::Corruption(
                "meta block too short for max_key_len".into(),
            ));
        }
        let max_key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if data.len() < offset + max_key_len {
            return Err(Error::Corruption("meta block too short for max_key".into()));
        }
        let max_key = data[offset..offset + max_key_len].to_vec();
        offset += max_key_len;

        // entry_count (8 bytes)
        if data.len() < offset + 8 {
            return Err(Error::Corruption(
                "meta block too short for entry_count".into(),
            ));
        }
        let entry_count = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());

        Ok(SSTableMeta {
            id,
            level,
            min_key,
            max_key,
            file_size,
            entry_count,
        })
    }

    /// Point lookup: check if key exists and return its value.
    ///
    /// Algorithm:
    /// 1. Check if key is outside [min_key, max_key] range → return None
    /// 2. Binary search index → find the right data block
    /// 3. Read that block from disk
    /// 4. Binary search within the block
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Step 1: Range check using cached metadata
        if key < self.meta.min_key.as_slice() || key > self.meta.max_key.as_slice() {
            return Ok(None);
        }

        // Step 2: Binary search the index to find the right block
        // Index is sorted by last_key, so we find the first block where
        // last_key >= key (lower_bound)
        let block_idx = match self
            .index
            .binary_search_by(|entry| entry.last_key.as_slice().cmp(key))
        {
            Ok(idx) => idx, // key == last_key, this block contains it
            Err(idx) => {
                // key < last_key, this block might contain it
                // But idx could be out of bounds (key > all last_keys)
                if idx >= self.index.len() {
                    return Ok(None);
                }
                idx
            }
        };

        let entry = &self.index[block_idx];

        // Step 3: Read the block from disk
        let mut block_data = vec![0u8; entry.size as usize];
        {
            let mut file = self.file.borrow_mut();
            file.seek(SeekFrom::Start(entry.offset))?;
            file.read_exact(&mut block_data)?;
        }

        // Step 4: Decode block and binary search within it
        let block = Block::decode(block_data)?;
        Ok(block.get(key).map(|v| v.to_vec()))
    }

    /// Create an iterator over all entries in the SSTable.
    pub fn iter(&self) -> Result<SSTableIterator> {
        todo!("[M15]: Create iterator starting at first block")
    }

    /// Create an iterator over entries in [start, end).
    pub fn range_iter(&self, _start: &[u8], _end: &[u8]) -> Result<SSTableIterator> {
        todo!("[M15]: Seek to start key, stop at end key")
    }

    /// Get metadata about this SSTable.
    pub fn meta(&self) -> &SSTableMeta {
        &self.meta
    }
}
