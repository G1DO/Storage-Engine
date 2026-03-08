use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

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
    // TODO(human): Implement these three methods
    // new: create Self with `levels` being a vec of `num_levels` empty Vecs
    // level: return &self.levels[level]
    // total_sstables: sum the len() of each level
    pub fn new(num_levels: usize) -> Self {
        Self {
            levels: vec![Vec::new(); num_levels],
        }
    }

    pub fn level(&self, level: usize) -> &[SSTableMeta] {
        &self.levels[level]
    }

    pub fn total_sstables(&self) -> usize {
        self.levels.iter().map(|l| l.len()).sum()
    }
}

/// Manages version transitions. Tracks current version and allows
/// atomic swaps when compaction completes.
///
/// Shared across threads via Arc<VersionSet>.
/// - Readers call current() to get the RwLock, then .read() it
/// - Compaction calls install() which .write()-locks and swaps the version
pub struct VersionSet {
    current: Arc<RwLock<Version>>,
    next_sst_id: AtomicU64,
}

impl VersionSet {
    pub fn new(num_levels: usize) -> Self {
        Self {
            current: Arc::new(RwLock::new(Version::new(num_levels))),
            next_sst_id: AtomicU64::new(1),
        }
    }

    /// Create a VersionSet from recovered state (manifest replay).
    pub fn new_from(version: Version, next_sst_id: u64) -> Self {
        Self {
            current: Arc::new(RwLock::new(version)),
            next_sst_id: AtomicU64::new(next_sst_id),
        }
    }

    pub fn install(&self, new_version: Version) {
        *self.current.write().unwrap() = new_version;
    }

    pub fn current(&self) -> Arc<RwLock<Version>> {
        Arc::clone(&self.current)
    }

    pub fn next_sst_id(&self) -> u64 {
        self.next_sst_id.fetch_add(1, Ordering::SeqCst)
    }
}
