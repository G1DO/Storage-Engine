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
    // TODO [M09]: Fields
    //   - dir: PathBuf
    //   - active_writer: WALWriter
    //   - next_wal_id: u64
}

impl WALManager {
    /// Create a WAL manager for the given directory.
    pub fn new(_dir: &Path, _sync_policy: SyncPolicy) -> Result<Self> {
        todo!("[M09]: Initialize, find existing WAL files")
    }

    /// Rotate: freeze current WAL, create a new one.
    /// Returns the path of the old WAL (caller deletes after SSTable flush).
    pub fn rotate(&mut self) -> Result<std::path::PathBuf> {
        todo!("[M09]: Create new WAL file, swap active writer")
    }

    /// Delete an old WAL file (safe only after SSTable is fsync'd).
    pub fn delete_wal(_path: &Path) -> Result<()> {
        todo!("[M09]: Remove file")
    }
}
