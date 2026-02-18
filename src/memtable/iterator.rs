use crate::error::Result;
use crate::iterator::StorageIterator;

// TODO [M04]: Implement MemTableIterator wrapping SkipListIterator

/// Iterator over memtable entries in sorted order.
/// Wraps the skip list iterator and implements StorageIterator.
pub struct MemTableIterator {
    // TODO [M04]: Fields — wrap SkipListIterator
}

impl StorageIterator for MemTableIterator {
    fn key(&self) -> &[u8] {
        todo!("[M04]: Delegate to skip list iterator")
    }

    fn value(&self) -> &[u8] {
        todo!("[M04]: Delegate to skip list iterator")
    }

    fn is_valid(&self) -> bool {
        todo!("[M04]: Delegate to skip list iterator")
    }

    fn next(&mut self) -> Result<()> {
        todo!("[M04]: Advance skip list iterator")
    }

    fn seek(&mut self, _key: &[u8]) -> Result<()> {
        todo!("[M04]: Seek in skip list")
    }
}
