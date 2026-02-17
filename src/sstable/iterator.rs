use std::cell::RefMut;
use std::io::{Read, Seek, SeekFrom};

use crate::error::Result;
use crate::iterator::StorageIterator;
use crate::sstable::block::reader::Block;
use crate::sstable::reader::SSTable;

/// Sequential iterator over all entries in an SSTable.
///
/// Reads blocks one at a time, iterates within each block,
/// then moves to the next block via the index.
pub struct SSTableIterator<'a> {
    /// Reference to parent SSTable for reading blocks.
    sstable: &'a SSTable,
    /// Current block index in the index vector.
    current_block_idx: usize,
    /// Current block data, loaded from disk.
    current_block: Option<Block>,
    /// Current position within the block (entry index).
    current_entry_idx: usize,
    /// End key for range iteration (optional).
    end_key: Option<Vec<u8>>,
}

impl<'a> SSTableIterator<'a> {
    /// Create a new iterator starting at the first block.
    pub fn new(sstable: &'a SSTable) -> Result<Self> {
        let mut iter = Self {
            sstable,
            current_block_idx: 0,
            current_block: None,
            current_entry_idx: 0,
            end_key: None,
        };

        // Load the first block if there is one
        if !sstable.index().is_empty() {
            iter.load_block(0)?;
        }

        Ok(iter)
    }

    /// Create a new iterator for the range [start, end).
    pub fn new_range(sstable: &'a SSTable, start: &[u8], end: &[u8]) -> Result<Self> {
        let mut iter = Self {
            sstable,
            current_block_idx: 0,
            current_block: None,
            current_entry_idx: 0,
            end_key: Some(end.to_vec()),
        };

        // Seek to start key
        iter.seek(start)?;

        Ok(iter)
    }

    /// Load a specific block by index.
    fn load_block(&mut self, block_idx: usize) -> Result<()> {
        if block_idx >= self.sstable.index().len() {
            // No more blocks
            self.current_block = None;
            self.current_block_idx = self.sstable.index().len();
            self.current_entry_idx = 0;
            return Ok(());
        }

        let entry = &self.sstable.index()[block_idx];

        // Read block from disk
        let mut block_data = vec![0u8; entry.size as usize];
        {
            let mut file: RefMut<'_, std::fs::File> = self.sstable.file().borrow_mut();
            file.seek(SeekFrom::Start(entry.offset))?;
            file.read_exact(&mut block_data)?;
        }

        // Decode block
        self.current_block = Some(Block::decode(block_data)?);
        self.current_block_idx = block_idx;
        self.current_entry_idx = 0;

        Ok(())
    }

    /// Advance to the next block.
    fn next_block(&mut self) -> Result<()> {
        self.load_block(self.current_block_idx + 1)
    }

    /// Check if current position is past the end key.
    fn is_past_end(&self) -> bool {
        if let Some(ref end) = self.end_key {
            if let Some(ref block) = self.current_block {
                if self.current_entry_idx < block.offsets().len() {
                    return self.key() >= end.as_slice();
                }
            }
        }
        false
    }

    /// Get key at current position.
    fn key_at(&self, idx: usize) -> &[u8] {
        if let Some(ref block) = self.current_block {
            block.key_at(idx)
        } else {
            &[]
        }
    }

    /// Get value at current position.
    fn value_at(&self, idx: usize) -> &[u8] {
        if let Some(ref block) = self.current_block {
            block.value_at(idx)
        } else {
            &[]
        }
    }
}

impl<'a> StorageIterator for SSTableIterator<'a> {
    fn key(&self) -> &[u8] {
        self.key_at(self.current_entry_idx)
    }

    fn value(&self) -> &[u8] {
        self.value_at(self.current_entry_idx)
    }

    fn is_valid(&self) -> bool {
        if self.is_past_end() {
            return false;
        }
        if let Some(ref block) = self.current_block {
            return self.current_entry_idx < block.offsets().len();
        }
        false
    }

    fn next(&mut self) -> Result<()> {
        self.current_entry_idx += 1;

        // If we've exhausted the current block, load the next one
        if let Some(ref block) = self.current_block {
            if self.current_entry_idx >= block.offsets().len() {
                self.next_block()?;
            }
        }

        Ok(())
    }

    fn seek(&mut self, key: &[u8]) -> Result<()> {
        // Binary search index to find the right block
        let block_idx = match self
            .sstable
            .index()
            .binary_search_by(|entry| entry.last_key.as_slice().cmp(key))
        {
            Ok(idx) => idx, // key == last_key, this block contains it
            Err(idx) => {
                // key < last_key, this block might contain it
                if idx >= self.sstable.index().len() {
                    // key > all keys in SSTable
                    self.current_block = None;
                    self.current_block_idx = self.sstable.index().len();
                    return Ok(());
                }
                idx
            }
        };

        // Load that block
        self.load_block(block_idx)?;

        // Binary search within the block for the key
        if let Some(ref block) = self.current_block {
            let offsets = block.offsets();
            let mut lo = 0usize;
            let mut hi = offsets.len();

            // Standard "lower_bound" binary search
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if self.key_at(mid) < key {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }

            self.current_entry_idx = lo;
        }

        Ok(())
    }
}
