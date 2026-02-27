pub mod leveled;
pub mod scheduler;
pub mod size_tiered;

use crate::sstable::footer::SSTableMeta;

// TODO [M19]: Implement compaction core (k-way merge sort)
// TODO [M20]: Implement size-tiered compaction
// TODO [M21]: Implement leveled compaction

/// Which compaction strategy to use.
///
/// Trade-offs:
///   | Metric             | Size-Tiered | Leveled |
///   |--------------------|-------------|---------|
///   | Write amplification | Lower       | Higher  |
///   | Space amplification | Higher      | Lower   |
///   | Read amplification  | Higher      | Lower   |
///
/// Implement size-tiered first (simpler), then leveled.
/// Benchmark both on the same workload.
#[derive(Debug, Clone, Copy)]
pub enum CompactionStyle {
    SizeTiered,
    Leveled,
}

/// A compaction task: which SSTables to merge and where.
#[derive(Debug)]
pub struct CompactionTask {
    /// Input SSTables to merge (from one or two levels).
    pub inputs: Vec<SSTableMeta>,
    /// Target level for the output SSTables.
    pub output_level: u32,
}

/// Trait for compaction strategy implementations.
pub trait CompactionStrategy {
    /// Decide if compaction is needed and which SSTables to compact.
    /// Returns None if no compaction needed.
    fn pick_compaction(&self, levels: &[Vec<SSTableMeta>]) -> Option<CompactionTask>;
}

/// Given a slice of SSTables and a key range [range_min, range_max],
/// return all SSTables whose key range overlaps with the given range.
///
/// Two ranges overlap when: range_min <= sst.max_key AND sst.min_key <= range_max
pub fn find_overlapping_sstables(
    sstables: &[SSTableMeta],
    range_min: &[u8],
    range_max: &[u8],
) -> Vec<SSTableMeta> {
    sstables
        .iter()
        .filter(|sst| range_min <= sst.max_key.as_slice() && sst.min_key.as_slice() <= range_max)
        .cloned()
        .collect()
}
