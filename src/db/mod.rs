pub mod snapshot;

use std::path::Path;

use crate::compaction::CompactionStyle;
use crate::error::Result;
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
    // TODO [M32]: Fields
    //   - options: Options
    //   - path: PathBuf
    //   - active_memtable: Arc<RwLock<MemTable>>
    //   - immutable_memtable: Option<Arc<MemTable>>
    //   - wal: WALManager
    //   - version_set: VersionSet
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
    pub fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!("[M32]: memtable → immutable → L0 → L1 → ...")
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
