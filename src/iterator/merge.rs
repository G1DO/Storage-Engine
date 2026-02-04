use crate::error::Result;
use crate::iterator::StorageIterator;

// TODO [M25]: Implement MergeIterator
//   - K-way merge using a BinaryHeap (min-heap ordered by current key)
//   - Deduplication: if multiple iterators have the same user_key,
//     keep only the one from the newest source (highest priority)
//   - Tombstone filtering: if the winning value is a tombstone, skip it
//   - This is the same algorithm as external merge sort

/// Merges multiple sorted iterators into a single sorted stream.
///
/// Used for:
/// - Range scans across memtable + all SSTable levels
/// - Compaction (merging SSTables)
///
/// Ordering guarantee: entries are yielded in (user_key ASC, sequence DESC) order.
/// Deduplication: only the newest version of each user_key is yielded.
pub struct MergeIterator {
    // TODO [M25]: Fields
    //   - iters: Vec<Box<dyn StorageIterator>> or BinaryHeap wrapper
    //   - current: track which iterator is active
}

impl MergeIterator {
    /// Create a new MergeIterator from multiple sorted sources.
    /// Sources are ordered by priority: index 0 = newest (memtable),
    /// higher indices = older (deeper SSTable levels).
    pub fn new(_iters: Vec<Box<dyn StorageIterator>>) -> Result<Self> {
        todo!("[M25]: Build initial heap from all valid iterators")
    }
}

impl StorageIterator for MergeIterator {
    fn key(&self) -> &[u8] {
        todo!("[M25]: Return current merged key")
    }

    fn value(&self) -> &[u8] {
        todo!("[M25]: Return current merged value")
    }

    fn is_valid(&self) -> bool {
        todo!("[M25]: Check if any iterator still has entries")
    }

    fn next(&mut self) -> Result<()> {
        todo!("[M25]: Advance past current key, dedup, skip tombstones")
    }

    fn seek(&mut self, _key: &[u8]) -> Result<()> {
        todo!("[M25]: Seek all iterators, rebuild heap")
    }
}
