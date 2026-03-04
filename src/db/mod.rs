pub mod snapshot;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::compaction::CompactionStyle;
use crate::error::Result;
use crate::manifest::version::VersionSet;
use crate::memtable::MemTable;
use crate::sstable::reader::SSTable;
use crate::wal::SyncPolicy;

// TODO [M32]: Implement public DB API
// TODO [M34]: Implement Stats / observability

/// Configuration options for the storage engine.
pub struct Options {
    /// Memtable flush threshold in bytes. Default: 4MB.
    pub memtable_size: usize,
    /// Target block size in bytes. Default: 4KB.
    pub block_size: usize,
    /// Bloom filter bits per key. Default: 10 (~1% FPR).
    pub bloom_bits_per_key: usize,
    /// Maximum number of levels. Default: 7.
    pub max_levels: usize,
    /// Size ratio between adjacent levels. Default: 10.
    pub level_size_multiplier: usize,
    /// Block cache capacity in bytes. Default: 8MB.
    pub block_cache_size: usize,
    /// WAL sync policy. Default: EveryWrite.
    pub sync_policy: SyncPolicy,
    /// Compaction strategy. Default: Leveled.
    pub compaction_style: CompactionStyle,
}

impl Default for Options {
    fn default() -> Self {
        todo!("[M32]: Return sensible defaults")
    }
}

/// Internal engine statistics.
pub struct Stats {
    pub memtable_size: usize,
    pub num_sstables_per_level: Vec<usize>,
    pub bloom_filter_hit_rate: f64,
    pub block_cache_hit_rate: f64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    /// bytes_written_to_disk / bytes_written_by_user
    pub write_amplification: f64,
    pub compaction_count: u64,
    pub compaction_bytes: u64,
}

/// The main database handle. Thread-safe.
///
/// Coordinates all components: memtable, WAL, SSTables, compaction,
/// manifest, block cache.
pub struct DB {
    // M24: Read path sources
    pub active_memtable: Arc<std::sync::RwLock<MemTable>>,
    pub immutable_memtable: Option<Arc<MemTable>>,
    pub version_set: Arc<VersionSet>,
    
    // TODO [M32]: Additional fields
    //   - options: Options
    //   - path: PathBuf
    //   - wal: WALManager
    //   - manifest: Manifest
    //   - block_cache: BlockCache
    //   - compaction_scheduler: CompactionScheduler
}

impl DB {
    /// Open or create a database at the given path.
    ///
    /// Recovery sequence:
    /// 1. Read manifest → reconstruct Version
    /// 2. Open all active SSTables
    /// 3. Find and replay WAL files → reconstruct memtable
    /// 4. Ready to serve
    pub fn open(_path: &Path, _options: Options) -> Result<Self> {
        todo!("[M32]: Full open sequence with recovery")
    }

    /// Insert or update a key-value pair.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        todo!("[M32]: WAL append → memtable put → maybe flush")
    }

    /// Retrieve the value for a key.
    /// 
    /// Search order: active memtable → immutable memtable → L0 → L1 → ...
    /// Returns the newest version of the key, or None if not found.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // =====================================================================
        // Task 1: Check active memtable
        // =====================================================================
        {
            let memtable = self.active_memtable.read().unwrap();
            if let Some(value) = memtable.get(key) {
                return Ok(Some(value.to_vec()));
            }
        }
        
        // =====================================================================
        // Task 2: Check immutable memtable
        // =====================================================================
        if let Some(immutable) = &self.immutable_memtable {
            if let Some(value) = immutable.get(key) {
                return Ok(Some(value.to_vec()));
            }
        }
        
        // =====================================================================
        // Task 3 & 4: Check L0 then L1+
        // =====================================================================
        let current_version = self.version_set.current();
        let version = current_version.read().unwrap();
        
        // Task 3: Check L0 (all SSTables, newest first)
        // L0 can have overlapping SSTables, so we must check ALL of them
        for meta in version.level(0).iter().rev() {
            let sst_path = PathBuf::from(format!("{:06}.sst", meta.id));
            let sst = SSTable::open(&sst_path)?;
            
            if let Some(value) = sst.get(key)? {
                return Ok(Some(value));
            }
        }
        
        // Task 4: Check L1 and deeper
        // L1+ have no overlaps, so at most ONE SSTable can contain the key.
        // The SSTable.get() method already handles bloom filter checking internally.
        for level in 1..version.levels.len() {
            let ssts_at_level = version.level(level);
            
            for meta in ssts_at_level {
                let sst_path = PathBuf::from(format!("{:06}.sst", meta.id));
                let sst = SSTable::open(&sst_path)?;
                
                // SSTable.get() internally checks bloom filter before binary search.
                // If found, return immediately.
                if let Some(value) = sst.get(key)? {
                    return Ok(Some(value));
                }
            }
        }
        
        // Key not found anywhere
        Ok(None)
    }

    /// Delete a key (writes a tombstone).
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        todo!("[M32]: WAL append delete → memtable tombstone")
    }

    /// Iterate over a range of keys [start, end).
    pub fn scan(&self, _start: &[u8], _end: &[u8]) -> Result<snapshot::Scanner> {
        todo!("[M32]: Create MergeIterator across all sources")
    }

    /// Create a consistent snapshot of the database.
    pub fn snapshot(&self) -> snapshot::Snapshot {
        todo!("[M26]: Capture current memtable + version references")
    }

    /// Force flush the active memtable to disk.
    pub fn flush(&self) -> Result<()> {
        todo!("[M32]: Freeze memtable, flush to SSTable")
    }

    /// Manually trigger compaction over a key range.
    pub fn compact_range(&self, _start: Option<&[u8]>, _end: Option<&[u8]>) -> Result<()> {
        todo!("[M32]: Pick and execute compaction for range")
    }

    /// Get current engine statistics.
    pub fn stats(&self) -> Stats {
        todo!("[M34]: Collect stats from all components")
    }

    /// Close the database gracefully.
    /// Flushes memtable, stops compaction, syncs manifest.
    pub fn close(self) -> Result<()> {
        todo!("[M32]: Flush, stop compaction, sync, close files")
    }
}
