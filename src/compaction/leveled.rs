use crate::compaction::{CompactionStrategy, CompactionTask};
use crate::sstable::footer::SSTableMeta;

// TODO [M21]: Implement leveled compaction

/// Leveled compaction strategy (what LevelDB/RocksDB use).
///
/// Each level has a size limit: L1=10MB, L2=100MB, L3=1GB (10x ratio).
/// When a level exceeds its limit:
///   1. Pick one SSTable from that level (round-robin or by key range)
///   2. Find all overlapping SSTables in the next level
///   3. Merge-sort: picked SSTable + overlapping next-level SSTables
///   4. Write new SSTables to the next level
///   5. Remove old SSTables
///
/// Key invariant: within Level 1+, no two SSTables have overlapping
/// key ranges. A point lookup checks at most ONE SSTable per level.
pub struct LeveledStrategy {
    // TODO [M21]: Fields
    //   - level_size_multiplier: usize (default 10)
    //   - base_level_size: usize (Level 1 target size)
    //   - max_levels: usize
}

impl LeveledStrategy {
    pub fn new(_base_level_size: usize, _multiplier: usize, _max_levels: usize) -> Self {
        todo!("[M21]: Initialize level size targets")
    }
}

impl CompactionStrategy for LeveledStrategy {
    fn pick_compaction(&self, _levels: &[Vec<SSTableMeta>]) -> Option<CompactionTask> {
        todo!("[M21]: Find level over budget, pick SSTable, find overlapping next-level")
    }
}
