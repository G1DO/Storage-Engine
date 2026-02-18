use crate::error::Result;
use crate::iterator::StorageIterator;

// TODO [M26]: Implement snapshot reads

/// A frozen view of the database at a point in time.
///
/// Holds references to the current memtable + SSTable version so that
/// ongoing writes and compaction don't affect reads through this snapshot.
pub struct Snapshot {
    // TODO [M26]: Fields
    //   - memtable reference (Arc)
    //   - immutable memtable reference (Arc, optional)
    //   - version reference (Arc<Version>)
}

impl Snapshot {
    /// Point lookup through the snapshot.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!("[M26]: Same read path as DB.get but using snapshot's frozen references")
    }

    /// Range scan through the snapshot.
    pub fn scan(&self, _start: &[u8], _end: &[u8]) -> Result<Scanner> {
        todo!("[M26]: MergeIterator over snapshot's sources")
    }
}

/// Range scan iterator returned by DB.scan() and Snapshot.scan().
pub struct Scanner {
    // TODO [M25]: Fields — wraps a MergeIterator
}

impl StorageIterator for Scanner {
    fn key(&self) -> &[u8] {
        todo!("[M25]: Delegate to inner MergeIterator")
    }

    fn value(&self) -> &[u8] {
        todo!("[M25]: Delegate to inner MergeIterator")
    }

    fn is_valid(&self) -> bool {
        todo!("[M25]: Delegate to inner MergeIterator")
    }

    fn next(&mut self) -> Result<()> {
        todo!("[M25]: Delegate to inner MergeIterator")
    }

    fn seek(&mut self, _key: &[u8]) -> Result<()> {
        todo!("[M25]: Delegate to inner MergeIterator")
    }
}
