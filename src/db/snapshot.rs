use crate::error::Result;
use crate::iterator::StorageIterator;
use crate::manifest::version::Version;
use std::sync::{Arc, RwLock};

// TODO [M26]: Implement snapshot reads

/// A frozen view of the database at a point in time.
///
/// Holds references to the current memtable + SSTable version so that
/// ongoing writes and compaction don't affect reads through this snapshot.
pub struct Snapshot {
    pub seq: u64,
    pub version: Arc<RwLock<Version>>,
    pub path: std::path::PathBuf,
}

impl Snapshot {
    /// Point lookup through the snapshot.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Minimal implementation: use Version to search SSTables but do not
        // inspect memtables. This is a placeholder until memtable stores
        // sequence numbers and can be queried with snapshot.seq.
        // our Snapshot holds Arc<RwLock<Version>> directly
        let version = self.version.read().unwrap();

        // Search L0 newest-first
        for meta in version.level(0).iter().rev() {
            let sst_path = self.path.join(format!("{:06}.sst", meta.id));
            if let Ok(sst) = crate::sstable::reader::SSTable::open(&sst_path)
                && let Ok(Some(v)) = sst.get(_key)
            {
                return Ok(Some(v));
            }
        }

        // Search deeper levels
        for level in 1..version.levels.len() {
            for meta in version.level(level) {
                let sst_path = self.path.join(format!("{:06}.sst", meta.id));
                if let Ok(sst) = crate::sstable::reader::SSTable::open(&sst_path)
                    && let Ok(Some(v)) = sst.get(_key)
                {
                    return Ok(Some(v));
                }
            }
        }

        Ok(None)
    }

    /// Range scan through the snapshot.
    pub fn scan(&self, _start: &[u8], _end: &[u8]) -> Result<Scanner> {
        // Not implemented: returning an empty scanner for now
        Ok(Scanner {})
    }
}

/// Range scan iterator returned by DB.scan() and Snapshot.scan().
pub struct Scanner {}

impl Scanner {
    pub fn is_valid(&self) -> bool {
        false
    }
}

impl StorageIterator for Scanner {
    fn key(&self) -> &[u8] {
        &[]
    }

    fn value(&self) -> &[u8] {
        &[]
    }

    fn is_valid(&self) -> bool {
        false
    }

    fn next(&mut self) -> Result<()> {
        Ok(())
    }

    fn seek(&mut self, _key: &[u8]) -> Result<()> {
        Ok(())
    }
}
