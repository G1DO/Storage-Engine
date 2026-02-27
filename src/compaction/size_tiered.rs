use crate::compaction::{find_overlapping_sstables, CompactionStrategy, CompactionTask};
use crate::sstable::footer::SSTableMeta;

/// Size-tiered compaction strategy.
///
/// Trigger: Level 0 has >= threshold SSTables (e.g., 4).
///
/// Action:
///   1. Pick all Level 0 SSTables
///   2. Find overlapping SSTables in Level 1
///   3. Merge-sort all of them together (done by caller, not here)
///   4. Write new SSTables to Level 1
///   5. Delete old SSTables
pub struct SizeTieredStrategy {
    /// How many L0 SSTables trigger a compaction.
    level0_threshold: usize,
}

impl SizeTieredStrategy {
    pub fn new(level0_threshold: usize) -> Self {
        Self { level0_threshold }
    }
}

impl CompactionStrategy for SizeTieredStrategy {
    fn pick_compaction(&self, levels: &[Vec<SSTableMeta>]) -> Option<CompactionTask> {
        // No levels or empty L0 → nothing to do.
        if levels.is_empty() || levels[0].len() < self.level0_threshold {
            return None;
        }

        let l0 = &levels[0];

        // Compute overall key range across ALL L0 SSTables.
        // Since L0 SSTables can have overlapping ranges, we need the
        // union: the smallest min_key and the largest max_key.
        let overall_min = l0.iter().map(|s| s.min_key.as_slice()).min().unwrap();
        let overall_max = l0.iter().map(|s| s.max_key.as_slice()).max().unwrap();

        // Start with all L0 SSTables as inputs.
        let mut inputs: Vec<SSTableMeta> = l0.clone();

        // Find overlapping L1 SSTables (if L1 exists).
        if levels.len() > 1 {
            let l1_overlapping =
                find_overlapping_sstables(&levels[1], overall_min, overall_max);
            inputs.extend(l1_overlapping);
        }

        Some(CompactionTask {
            inputs,
            output_level: 1,
        })
    }
}

