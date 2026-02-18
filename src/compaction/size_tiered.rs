use crate::compaction::{CompactionStrategy, CompactionTask};
use crate::sstable::footer::SSTableMeta;

// TODO [M20]: Implement size-tiered compaction

/// Size-tiered compaction strategy.
///
/// Trigger: Level 0 has >= threshold SSTables (e.g., 4).
///
/// Action:
///   1. Pick all Level 0 SSTables
///   2. Find overlapping SSTables in Level 1
///   3. Merge-sort all of them together
///   4. Write new SSTables to Level 1
///   5. Delete old SSTables
///
/// Simpler than leveled. Good first implementation.
pub struct SizeTieredStrategy {
    // TODO [M20]: Fields
    //   - level0_threshold: usize (how many L0 SSTables trigger compaction)
}

impl SizeTieredStrategy {
    pub fn new(_level0_threshold: usize) -> Self {
        todo!("[M20]: Initialize")
    }
}

impl CompactionStrategy for SizeTieredStrategy {
    fn pick_compaction(&self, _levels: &[Vec<SSTableMeta>]) -> Option<CompactionTask> {
        todo!("[M20]: Check L0 count, find overlapping L1 SSTables")
    }
}
