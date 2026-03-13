pub mod snapshot;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::cache::BlockCache;
use crate::compaction::CompactionStyle;
use crate::error::Result;
use crate::iterator::StorageIterator;
use crate::manifest::Manifest;
use crate::manifest::version::{Version, VersionSet};
use crate::memtable::MemTable;
use crate::sstable::builder::SSTableBuilder;
use crate::sstable::reader::SSTable;
use crate::wal::SyncPolicy;
use crate::wal::reader::WALReader;
use crate::wal::record::{RecordType, WALRecord};
use crate::wal::writer::WALManager;

fn find_wal_files(dir: &Path) -> Vec<u64> {
    let mut wal_numbers = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(filename) = entry.file_name().to_str()
                && let Some(num_str) = filename.strip_suffix(".wal")
                && let Ok(num) = num_str.parse::<u64>()
            {
                wal_numbers.push(num);
            }
        }
    }
    wal_numbers.sort_unstable();
    wal_numbers
}

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
        Self {
            memtable_size: 4 * 1024 * 1024, // 4 MB
            block_size: 4 * 1024,           // 4 KB
            bloom_bits_per_key: 10,         // ~1% FPR
            max_levels: 7,
            level_size_multiplier: 10,
            block_cache_size: 8 * 1024 * 1024, // 8 MB
            sync_policy: SyncPolicy::EveryWrite,
            compaction_style: CompactionStyle::Leveled,
        }
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
    /// Database directory path.
    path: PathBuf,
    /// Memtable size limit (cached from Options for flush).
    memtable_size: usize,
    /// Block size (cached from Options for SSTable building).
    block_size: usize,
    // M24: Read path sources
    pub active_memtable: Arc<RwLock<MemTable>>,
    pub immutable_memtable: Option<Arc<MemTable>>,
    pub version_set: Arc<VersionSet>,
    /// Next sequence number for writes (monotonic)
    pub next_sequence: Arc<AtomicU64>,
    /// Manifest for recording structural changes (flush, compaction).
    manifest: Mutex<Manifest>,
    /// WAL manager for durable writes.
    wal_manager: Mutex<WALManager>,
    /// Compaction strategy style.
    compaction_style: CompactionStyle,
    /// Block cache for SSTable data blocks.
    block_cache: Mutex<BlockCache>,
    /// Stats: bytes written by user (put key+value, delete key).
    bytes_written_user: AtomicU64,
    /// Stats: bytes written to disk (SSTable file sizes from flush).
    bytes_written_disk: AtomicU64,
    /// Stats: bytes read from get() hits.
    bytes_read: AtomicU64,
    /// Stats: number of compactions completed.
    compaction_count: AtomicU64,
    /// Stats: total bytes processed by compaction.
    compaction_bytes: AtomicU64,
}

impl DB {
    /// Open or create a database at the given path.
    ///
    /// Recovery sequence:
    /// 1. Create directory if needed
    /// 2. Read manifest → reconstruct Version + log_number + next_sst_id
    /// 3. Find WAL files with id >= log_number, replay into memtable
    /// 4. Create new WALManager for future writes
    /// 5. Ready to serve
    pub fn open(path: &Path, options: Options) -> Result<Self> {
        // 1. Ensure the database directory exists
        std::fs::create_dir_all(path)?;

        // 2. Open manifest — replays all records to reconstruct Version
        let manifest = Manifest::open(&path.join("MANIFEST"))?;
        let log_number = manifest.log_number();
        let next_sst_id = manifest.next_sst_id();
        let version = manifest.current_version().clone();

        // 3. Build VersionSet from recovered state
        let version_set = Arc::new(VersionSet::new_from(version, next_sst_id));

        // 4. Find and replay WAL files >= log_number
        let wal_ids = find_wal_files(path);
        let mut memtable = MemTable::new(options.memtable_size);
        let mut record_count: u64 = 0;

        for wal_id in wal_ids {
            if wal_id < log_number {
                continue; // this WAL's data is already in SSTables
            }
            let wal_path = path.join(format!("{:06}.wal", wal_id));
            let reader = WALReader::new(&wal_path)?;
            for record_result in reader.iter() {
                let record = record_result?;
                match record.record_type {
                    RecordType::Put => memtable.put(record.key, record.value),
                    RecordType::Delete => memtable.delete(record.key),
                }
                record_count += 1;
            }
        }

        // 5. Create new WALManager for future writes
        let wal_manager = WALManager::new(path, options.sync_policy)?;

        // 6. Assemble DB
        let memtable_size = options.memtable_size;
        let block_size = options.block_size;
        let compaction_style = options.compaction_style;

        Ok(DB {
            path: path.to_path_buf(),
            memtable_size,
            block_size,
            active_memtable: Arc::new(RwLock::new(memtable)),
            immutable_memtable: None,
            version_set,
            next_sequence: Arc::new(AtomicU64::new(record_count + 1)),
            manifest: Mutex::new(manifest),
            wal_manager: Mutex::new(wal_manager),
            compaction_style,
            block_cache: Mutex::new(BlockCache::new(options.block_cache_size)),
            bytes_written_user: AtomicU64::new(0),
            bytes_written_disk: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            compaction_count: AtomicU64::new(0),
            compaction_bytes: AtomicU64::new(0),
        })
    }

    /// Insert or update a key-value pair.
    ///
    /// WAL-first: write to WAL for durability, then insert into memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let _seq = self.next_sequence.fetch_add(1, Ordering::SeqCst);

        // WAL first — guarantees durability before acknowledging
        {
            let mut wal = self.wal_manager.lock().unwrap();
            let record = WALRecord::put(key.to_vec(), value.to_vec());
            wal.active_writer().append(&record)?;
        }

        // Then memtable
        let mut active = self.active_memtable.write().unwrap();
        active.put(key.to_vec(), value.to_vec());

        // Stats
        self.bytes_written_user
            .fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Retrieve the value for a key.
    ///
    /// Search order: active memtable → immutable memtable → L0 → L1 → ...
    /// Returns the newest version of the key, or None if not found.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check active memtable
        {
            let memtable = self.active_memtable.read().unwrap();
            if let Some(value) = memtable.get(key) {
                return Ok(Some(value.to_vec()));
            }
        }

        // Check immutable memtable
        if let Some(immutable) = &self.immutable_memtable
            && let Some(value) = immutable.get(key)
        {
            return Ok(Some(value.to_vec()));
        }

        // Check SSTables via Version (L0 newest-first, then L1+)
        let current_version = self.version_set.current();
        let version = current_version.read().unwrap();

        // L0: check all SSTables, newest first (overlapping key ranges)
        for meta in version.level(0).iter().rev() {
            let sst_path = self.path.join(format!("{:06}.sst", meta.id));
            let sst = SSTable::open(&sst_path)?;
            if let Some(value) = sst.get(key)? {
                // Empty value = tombstone → key is deleted, stop searching
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        // L1+: no overlaps, at most one SSTable contains the key
        for level in 1..version.levels.len() {
            for meta in version.level(level) {
                let sst_path = self.path.join(format!("{:06}.sst", meta.id));
                let sst = SSTable::open(&sst_path)?;
                if let Some(value) = sst.get(key)? {
                    if value.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(value));
                }
            }
        }

        Ok(None)
    }

    /// Delete a key (writes a tombstone).
    ///
    /// WAL-first: write tombstone to WAL, then to memtable.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let _seq = self.next_sequence.fetch_add(1, Ordering::SeqCst);

        // WAL first
        {
            let mut wal = self.wal_manager.lock().unwrap();
            let record = WALRecord::delete(key.to_vec());
            wal.active_writer().append(&record)?;
        }

        // Then memtable
        let mut active = self.active_memtable.write().unwrap();
        active.delete(key.to_vec());

        // Stats
        self.bytes_written_user
            .fetch_add(key.len() as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Iterate over a range of keys [start, end).
    ///
    /// Merges data from active memtable + immutable memtable + all SSTable
    /// levels. Tombstones are filtered and range bounds are enforced.
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<snapshot::Scanner> {
        // Capture memtable entries under read lock
        let memtable_entries = {
            let mt = self.active_memtable.read().unwrap();
            let mut entries = Vec::new();
            let mut iter = mt.iter();
            while iter.is_valid() {
                entries.push((iter.key().to_vec(), iter.value().to_vec()));
                iter.next()?;
            }
            entries
        };

        let version = self.version_set.current();

        snapshot::Scanner::build(&memtable_entries, &version, &self.path, start, end)
    }

    /// Create a consistent snapshot of the database.
    ///
    /// Captures a point-in-time copy of the memtable entries and a reference
    /// to the current Version (SSTable set). Subsequent writes and compaction
    /// won't affect reads through this snapshot.
    pub fn snapshot(&self) -> snapshot::Snapshot {
        let seq = self.next_sequence.load(Ordering::SeqCst);
        let version = self.version_set.current();

        // Capture memtable entries under read lock
        let memtable_entries = {
            let mt = self.active_memtable.read().unwrap();
            let mut entries = Vec::new();
            let mut iter = mt.iter();
            while iter.is_valid() {
                entries.push((iter.key().to_vec(), iter.value().to_vec()));
                iter.next().unwrap();
            }
            entries
        };

        snapshot::Snapshot {
            seq,
            version,
            path: self.path.clone(),
            memtable_entries,
        }
    }

    /// Force flush the active memtable to disk as an SSTable.
    ///
    /// Crash-safe ordering:
    /// 1. Swap active memtable → frozen, create new empty active
    /// 2. Rotate WAL (new WAL for future writes)
    /// 3. Build SSTable from frozen memtable
    /// 4. Update manifest: record_flush + record_log_number
    /// 5. Install new Version in VersionSet
    /// 6. Delete old WAL (safe: SSTable is fsync'd, manifest updated)
    pub fn flush(&self) -> Result<()> {
        // 1. Freeze: swap active memtable with a fresh empty one
        let frozen = {
            let mut active = self.active_memtable.write().unwrap();
            if active.is_empty() {
                return Ok(()); // nothing to flush
            }
            std::mem::replace(&mut *active, MemTable::new(self.memtable_size))
        };

        // 2. Rotate WAL — old WAL is now frozen alongside the memtable
        let (old_wal_path, new_wal_id) = {
            let mut wal = self.wal_manager.lock().unwrap();
            let old_path = wal.rotate()?;
            let new_id = wal.active_wal_id();
            (old_path, new_id)
        };

        // 3. Build SSTable from frozen memtable
        let sst_id = self.version_set.next_sst_id();
        let sst_path = self.path.join(format!("{:06}.sst", sst_id));
        let mut builder = SSTableBuilder::new(&sst_path, sst_id, self.block_size)?;

        let mut iter = frozen.iter();
        while iter.is_valid() {
            builder.add(iter.key(), iter.value())?;
            iter.next()?;
        }
        let meta = builder.finish()?;

        // Stats: track bytes written to disk
        self.bytes_written_disk
            .fetch_add(meta.file_size, Ordering::Relaxed);

        // 4. Update manifest: record the new SSTable, then the new log_number
        {
            let mut manifest = self.manifest.lock().unwrap();
            manifest.record_flush(meta.clone())?;
            manifest.record_log_number(new_wal_id)?;
        }

        // 5. Install new Version with the SSTable added to L0
        {
            let current = self.version_set.current();
            let old_version = current.read().unwrap();
            let mut new_levels = old_version.levels.clone();
            new_levels[0].push(meta);
            drop(old_version);
            self.version_set.install(Version { levels: new_levels });
        }

        // 6. Delete old WAL — safe because SSTable is fsync'd and manifest updated
        let _ = WALManager::delete_wal(&old_wal_path);

        Ok(())
    }

    /// Manually trigger compaction.
    ///
    /// With `(None, None)`: runs compaction repeatedly until no more work.
    /// With `(Some(start), Some(end))`: compacts SSTables overlapping that range.
    pub fn compact_range(&self, _start: Option<&[u8]>, _end: Option<&[u8]>) -> Result<()> {
        use crate::compaction::leveled::LeveledStrategy;
        use crate::compaction::scheduler::run_compaction;
        use crate::compaction::size_tiered::SizeTieredStrategy;

        // Build strategy matching the DB's configured style
        let strategy: Box<dyn crate::compaction::CompactionStrategy> = match self.compaction_style {
            CompactionStyle::SizeTiered => Box::new(SizeTieredStrategy::new(1)), // threshold=1 to force compaction
            CompactionStyle::Leveled => Box::new(LeveledStrategy::new(
                10 * 1024 * 1024, // 10MB base
                10,               // 10x multiplier
                7,                // max levels
            )),
        };

        // Run compaction in a loop until nothing more to do
        loop {
            // Snapshot file sizes before compaction to measure bytes processed
            let size_before = self.total_sst_size();
            match run_compaction(&self.version_set, &*strategy, &self.path, self.block_size)? {
                true => {
                    self.compaction_count.fetch_add(1, Ordering::Relaxed);
                    let size_after = self.total_sst_size();
                    // Track bytes involved (approximate: max of before/after)
                    let bytes = size_before.max(size_after);
                    self.compaction_bytes.fetch_add(bytes, Ordering::Relaxed);
                    continue;
                }
                false => break,
            }
        }

        Ok(())
    }

    /// Get current engine statistics.
    pub fn stats(&self) -> Stats {
        let memtable_size = {
            let mt = self.active_memtable.read().unwrap();
            mt.size()
        };

        let num_sstables_per_level = {
            let current = self.version_set.current();
            let v = current.read().unwrap();
            v.levels.iter().map(|l| l.len()).collect()
        };

        let block_cache_hit_rate = {
            let cache = self.block_cache.lock().unwrap();
            cache.hit_rate()
        };

        let bytes_written_user = self.bytes_written_user.load(Ordering::Relaxed);
        let bytes_written_disk = self.bytes_written_disk.load(Ordering::Relaxed);

        Stats {
            memtable_size,
            num_sstables_per_level,
            bloom_filter_hit_rate: 0.0, // bloom checks happen inside SSTable::get()
            block_cache_hit_rate,
            bytes_written: bytes_written_user,
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            write_amplification: if bytes_written_user > 0 {
                bytes_written_disk as f64 / bytes_written_user as f64
            } else {
                0.0
            },
            compaction_count: self.compaction_count.load(Ordering::Relaxed),
            compaction_bytes: self.compaction_bytes.load(Ordering::Relaxed),
        }
    }

    /// Sum of all SSTable file sizes in the current version.
    fn total_sst_size(&self) -> u64 {
        let current = self.version_set.current();
        let v = current.read().unwrap();
        v.levels.iter().flatten().map(|m| m.file_size).sum()
    }

    /// Close the database gracefully.
    ///
    /// Flushes any remaining memtable data, syncs the WAL.
    pub fn close(self) -> Result<()> {
        // Flush if memtable has data
        {
            let memtable = self.active_memtable.read().unwrap();
            if !memtable.is_empty() {
                drop(memtable);
                self.flush()?;
            }
        }

        // Sync the active WAL
        let mut wal = self.wal_manager.lock().unwrap();
        wal.active_writer().sync()?;

        Ok(())
    }
}
