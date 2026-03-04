use crate::compaction::{CompactionStrategy, CompactionTask, find_overlapping_sstables};
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
    level_size_multiplier: usize,
    base_level_size: usize,
    max_levels: usize,
}

impl LeveledStrategy {
    pub fn new(base_level_size: usize, multiplier: usize, max_levels: usize) -> Self {
        Self {
            level_size_multiplier: multiplier,
            base_level_size,
            max_levels,
        }
    }
}

impl CompactionStrategy for LeveledStrategy {
    fn pick_compaction(&self, levels: &[Vec<SSTableMeta>]) -> Option<CompactionTask> {
        let mut budget = self.base_level_size as u64;

        for level_idx in 1..self.max_levels {
            if let Some(level_ssts) = levels.get(level_idx) {
                let total_size: u64 = level_ssts.iter().map(|sst| sst.file_size).sum();

                let next_level = level_idx + 1;
                if total_size > budget && next_level < self.max_levels {
                    let picked = &level_ssts[0];
                    let mut inputs = vec![picked.clone()];

                    if let Some(next_ssts) = levels.get(next_level) {
                        let overlapping =
                            find_overlapping_sstables(next_ssts, &picked.min_key, &picked.max_key);
                        inputs.extend(overlapping);
                    }

                    return Some(CompactionTask {
                        inputs,
                        output_level: next_level as u32,
                    });
                }
            }

            budget *= self.level_size_multiplier as u64;
        }

        None
    }
}
