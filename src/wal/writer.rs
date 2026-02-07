use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::error::Result;
use crate::wal::record::WALRecord;
use crate::wal::SyncPolicy;

// TODO [M07]: Implement WAL writer with fsync
// TODO [M09]: Implement WAL rotation on memtable flush

/// Writes WAL records to a file on disk.
///
/// Every write must be durable before it's acknowledged to the client.
/// The WAL ensures crash recovery: on restart, replay the WAL to
/// reconstruct the memtable.
///
/// Two layers of buffering:
///   BufWriter.flush()  → Rust buffer → OS page cache
///   file.sync_all()    → OS page cache → physical disk
pub struct WALWriter {
    writer: BufWriter<File>,
    offset: u64,
    sync_policy: SyncPolicy,
    writes_since_sync: usize,
}

impl WALWriter {
    /// Create a new WAL writer at the given path.
    pub fn new(path: &Path, sync_policy: SyncPolicy) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(WALWriter {
            writer: BufWriter::new(file),
            offset: 0,
            sync_policy,
            writes_since_sync: 0,
        })
    }

    /// Append a record to the WAL.
    /// Depending on SyncPolicy, may fsync after this write.
    pub fn append(&mut self, record: &WALRecord) -> Result<()> {
        let encoded = record.encode();

        self.writer.write_all(&encoded)?;
        self.writer.flush()?;
        self.offset += encoded.len() as u64;
        self.writes_since_sync += 1;

        // Sync based on policy
        match self.sync_policy {
            SyncPolicy::EveryWrite => {
                self.writer.get_ref().sync_all()?;
                self.writes_since_sync = 0;
            }
            SyncPolicy::EveryNWrites(n) => {
                if self.writes_since_sync >= n {
                    self.writer.get_ref().sync_all()?;
                    self.writes_since_sync = 0;
                }
            }
            SyncPolicy::EveryNMillis(_) => {
                // Timer-based sync handled externally
            }
        }

        Ok(())
    }

    /// Force fsync to disk. Ensures all buffered writes are durable.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        self.writes_since_sync = 0;
        Ok(())
    }

    /// Current file offset (bytes written so far).
    pub fn offset(&self) -> u64 {
        self.offset
    }
}

/// Manages WAL file rotation.
///
/// When a memtable is flushed to SSTable:
/// 1. Create new WAL for the new active memtable
/// 2. Keep old WAL until SSTable flush is confirmed (fsync'd)
/// 3. Delete old WAL
///
/// CRITICAL INVARIANT: Old WAL is only deleted AFTER its SSTable is
/// fully written and fsync'd. Violating this loses data.
pub struct WALManager {
    dir: std::path::PathBuf,
    active_writer: WALWriter,
    active_path: std::path::PathBuf,
    next_wal_id: u64,
    sync_policy: SyncPolicy,
}

impl WALManager {
    /// Create a WAL manager for the given directory.
    ///
    /// Scans for existing WAL files to determine the next ID,
    /// then creates a new active WAL file.
    pub fn new(dir: &Path, sync_policy: SyncPolicy) -> Result<Self> {
        std::fs::create_dir_all(dir)?;

        // Find the highest existing WAL ID so we don't collide
        let max_id = Self::find_max_wal_id(dir);
        let next_id = max_id + 1;

        let active_path = dir.join(format!("{:06}.wal", next_id));
        let active_writer = WALWriter::new(&active_path, sync_policy)?;

        Ok(WALManager {
            dir: dir.to_path_buf(),
            active_writer,
            active_path,
            next_wal_id: next_id + 1,
            sync_policy,
        })
    }

    /// Rotate: sync current WAL, create a new one.
    /// Returns the path of the old WAL (caller deletes after SSTable flush).
    pub fn rotate(&mut self) -> Result<std::path::PathBuf> {
        // Sync the current WAL before freezing it
        self.active_writer.sync()?;

        let old_path = self.active_path.clone();

        // Create new WAL file
        let new_path = self.dir.join(format!("{:06}.wal", self.next_wal_id));
        let new_writer = WALWriter::new(&new_path, self.sync_policy)?;

        self.active_writer = new_writer;
        self.active_path = new_path;
        self.next_wal_id += 1;

        Ok(old_path)
    }

    /// Delete an old WAL file (safe only after SSTable is fsync'd).
    pub fn delete_wal(path: &Path) -> Result<()> {
        std::fs::remove_file(path)?;
        Ok(())
    }

    /// Access the active WAL writer for appending records.
    pub fn active_writer(&mut self) -> &mut WALWriter {
        &mut self.active_writer
    }

    /// Path of the current active WAL file.
    pub fn active_path(&self) -> &Path {
        &self.active_path
    }

    /// Scan directory for existing .wal files, return the highest ID found (0 if none).
    fn find_max_wal_id(dir: &Path) -> u64 {
        std::fs::read_dir(dir)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name();
                let name = name.to_str()?;
                let stem = name.strip_suffix(".wal")?;
                stem.parse::<u64>().ok()
            })
            .max()
            .unwrap_or(0)
    }
}
