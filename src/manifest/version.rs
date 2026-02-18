use crate::sstable::footer::SSTableMeta;

// TODO [M27]: Implement Version
// TODO [M28]: Use Version in DB recovery

/// A snapshot of the database structure: which SSTables exist at which levels.
///
/// Versions are immutable. When compaction completes, a new Version is
/// created with the updated SSTable set. Readers hold a reference to the
/// old Version until they're done (snapshot isolation for structure).
#[derive(Debug, Clone)]
pub struct Version {
    /// SSTables at each level. Index 0 = Level 0.
    pub levels: Vec<Vec<SSTableMeta>>,
}

impl Version {
    /// Create an empty version with the given number of levels.
    pub fn new(_num_levels: usize) -> Self {
        todo!("[M27]: Initialize empty level vectors")
    }

    /// Get SSTables at a specific level.
    pub fn level(&self, _level: usize) -> &[SSTableMeta] {
        todo!("[M27]: Return &self.levels[level]")
    }

    /// Total number of SSTables across all levels.
    pub fn total_sstables(&self) -> usize {
        todo!("[M27]: Sum across all levels")
    }
}

/// Manages version transitions. Tracks current version and allows
/// atomic swaps when compaction completes.
pub struct VersionSet {
    // TODO [M28]: Fields
    //   - current: Arc<Version>
    //   - next_sst_id: u64
}

impl VersionSet {
    pub fn new(_num_levels: usize) -> Self {
        todo!("[M28]: Initialize with empty version")
    }

    /// Install a new version (after compaction or flush).
    pub fn install(&mut self, _new_version: Version) {
        todo!("[M28]: Atomically swap current version")
    }

    /// Get a reference to the current version (for reads/snapshots).
    pub fn current(&self) -> &Version {
        todo!("[M28]: Return current version reference")
    }

    /// Allocate the next SSTable ID.
    pub fn next_sst_id(&mut self) -> u64 {
        todo!("[M28]: Increment and return")
    }
}
