use crate::error::Result;
use crate::iterator::StorageIterator;

/// An iterator over in-memory key-value pairs.
///
/// Used during compaction to sidestep the SSTableIterator<'a> lifetime issue:
/// SSTableIterator borrows &'a SSTable, so you can't store both the SSTable
/// and its iterator in a Vec for MergeIterator. Instead, read all entries
/// into a Vec<(key, value)> and wrap it in VecIterator.
pub struct VecIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
}

impl VecIterator {
    pub fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self { entries, pos: 0 }
    }
}

impl StorageIterator for VecIterator {
    fn key(&self) -> &[u8] {
        &self.entries[self.pos].0
    }

    fn value(&self) -> &[u8] {
        &self.entries[self.pos].1
    }

    fn is_valid(&self) -> bool {
        self.pos < self.entries.len()
    }

    fn next(&mut self) -> Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn seek(&mut self, key: &[u8]) -> Result<()> {
        self.pos = self.entries.partition_point(|(k, _)| k.as_slice() < key);
        Ok(())
    }
}
