pub mod merge;

use crate::error::Result;

// TODO [M02]: Implement this trait for SkipListIterator

// TODO [M12]: Implement this trait for BlockIterator
// TODO [M15]: Implement this trait for SSTableIterator
// TODO [M25]: Implement this trait for MergeIterator

/// The central iteration abstraction for the storage engine.
///
/// Every sorted data source (skip list, block, SSTable, merged view)
/// implements this trait. This enables composability — MergeIterator
/// takes Vec<Box<dyn StorageIterator>> and merges them.
pub trait StorageIterator {
    /// Returns the current key. Only valid when is_valid() is true.
    fn key(&self) -> &[u8];

    /// Returns the current value. Only valid when is_valid() is true.
    fn value(&self) -> &[u8];

    /// Returns true if the iterator is positioned at a valid entry.
    fn is_valid(&self) -> bool;

    /// Advances to the next entry. Returns error on IO failure.
    fn next(&mut self) -> Result<()>;

    /// Positions the iterator at the first entry with key >= target.
    fn seek(&mut self, key: &[u8]) -> Result<()>;
}
