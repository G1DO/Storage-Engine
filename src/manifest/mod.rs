pub mod version;

use crate::error::Result;
use crate::sstable::footer::SSTableMeta;

// TODO [M27]: Implement manifest writer
// TODO [M29]: Implement manifest compaction (snapshot current version)

/// Types of records stored in the manifest.
///
/// The manifest is a log of every structural change to the database.
/// On recovery, replay it to know which SSTables are current.
#[derive(Debug)]
pub enum ManifestRecord {
    /// A new SSTable was created (from flush or compaction).
    NewSSTable(SSTableMeta),
    /// A compaction completed: some SSTables added, some removed.
    CompactionComplete {
        added: Vec<SSTableMeta>,
        removed: Vec<u64>, // SSTable IDs
    },
}

/// The manifest: a durable log of database structure changes.
///
/// Reuses the WAL format (CRC + records) — same append-only,
/// crash-safe log pattern.
///
/// Example manifest contents:
/// ```text
/// Record 1: NewSSTable { id: 1, level: 0, min: "a", max: "f" }
/// Record 2: NewSSTable { id: 2, level: 0, min: "d", max: "k" }
/// Record 3: CompactionComplete { added: [id:3@L1], removed: [1, 2] }
/// ```
pub struct Manifest {
    // TODO [M27]: Fields
    //   - log: WALWriter (reuse WAL implementation!)
    //   - current_version: Version
}

impl Manifest {
    /// Open or create a manifest at the given path.
    pub fn open(_path: &std::path::Path) -> Result<Self> {
        todo!("[M27]: Open manifest file, replay existing records")
    }

    /// Record that a new SSTable was created from a memtable flush.
    pub fn record_flush(&mut self, _new_sst: SSTableMeta) -> Result<()> {
        todo!("[M27]: Write NewSSTable record, update version")
    }

    /// Record that a compaction completed.
    pub fn record_compaction(
        &mut self,
        _added: Vec<SSTableMeta>,
        _removed: Vec<u64>,
    ) -> Result<()> {
        todo!("[M27]: Write CompactionComplete record, update version")
    }

    /// Get the current version (which SSTables exist at which levels).
    pub fn current_version(&self) -> &version::Version {
        todo!("[M27]: Return &self.current_version")
    }

    /// Compact the manifest: snapshot current version to a new file.
    /// The old manifest can then be deleted.
    pub fn compact(&mut self) -> Result<()> {
        todo!("[M29]: Write full Version snapshot, rotate manifest file")
    }
}
