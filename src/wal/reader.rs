use std::fs;
use std::path::Path;

use crate::error::Result;
use crate::wal::record::WALRecord;

/// Reads WAL records from a file for crash recovery.
///
/// Loads the entire file into memory, then iterates record by record.
/// On startup:
/// 1. Find all WAL files
/// 2. Replay each record into a fresh memtable
/// 3. If CRC fails on a record, stop — it was a partial write from a crash.
///    All preceding records are valid.
pub struct WALReader {
    data: Vec<u8>,
}

impl WALReader {
    /// Open a WAL file for reading.
    pub fn new(path: &Path) -> Result<Self> {
        let data = fs::read(path)?;
        Ok(WALReader { data })
    }

    /// Create an iterator over all valid records in the WAL.
    pub fn iter(&self) -> WALIterator<'_> {
        WALIterator {
            data: &self.data,
            offset: 0,
        }
    }
}

/// Iterator over WAL records. Yields records until EOF or corruption.
///
/// On CRC mismatch: stops iteration (the record was a partial write).
/// This is safe because WAL writes are sequential and append-only —
/// a corrupted record means the crash happened here, and nothing
/// valid can follow.
pub struct WALIterator<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for WALIterator<'a> {
    type Item = Result<WALRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.data.len() {
            return None;
        }

        let remaining = &self.data[self.offset..];

        match WALRecord::decode(remaining) {
            Ok(record) => {
                self.offset += record.encoded_size();
                Some(Ok(record))
            }
            Err(_) => None,
        }
    }
}