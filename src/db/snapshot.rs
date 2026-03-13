use crate::error::Result;
use crate::iterator::StorageIterator;
use crate::iterator::merge::MergeIterator;
use crate::iterator::vec_iter::VecIterator;
use crate::manifest::version::Version;
use crate::sstable::reader::SSTable;
use std::sync::{Arc, RwLock};

/// A frozen view of the database at a point in time.
///
/// Holds a copy of the memtable entries at snapshot creation time plus
/// references to the current Version (SSTable set) so that ongoing writes
/// and compaction don't affect reads through this snapshot.
pub struct Snapshot {
    pub seq: u64,
    pub version: Arc<RwLock<Version>>,
    pub path: std::path::PathBuf,
    /// Memtable entries captured at snapshot time. Sorted by key.
    /// Includes tombstones (empty values) so they can shadow older data.
    pub memtable_entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Snapshot {
    /// Point lookup through the snapshot.
    ///
    /// Search order: memtable snapshot → L0 (newest-first) → L1+
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. Check captured memtable entries (binary search, they're sorted)
        if let Ok(idx) = self
            .memtable_entries
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
        {
            let value = &self.memtable_entries[idx].1;
            if value.is_empty() {
                return Ok(None); // tombstone
            }
            return Ok(Some(value.clone()));
        }

        // 2. Search SSTables via version
        let version = self.version.read().unwrap();

        // L0: check all SSTables, newest first
        for meta in version.level(0).iter().rev() {
            let sst_path = self.path.join(format!("{:06}.sst", meta.id));
            if let Ok(sst) = SSTable::open(&sst_path)
                && let Ok(Some(v)) = sst.get(key)
            {
                if v.is_empty() {
                    return Ok(None); // tombstone
                }
                return Ok(Some(v));
            }
        }

        // L1+: no overlaps within a level
        for level in 1..version.levels.len() {
            for meta in version.level(level) {
                let sst_path = self.path.join(format!("{:06}.sst", meta.id));
                if let Ok(sst) = SSTable::open(&sst_path)
                    && let Ok(Some(v)) = sst.get(key)
                {
                    if v.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(v));
                }
            }
        }

        Ok(None)
    }

    /// Range scan through the snapshot: yields all keys in [start, end).
    ///
    /// Merges memtable snapshot + all SSTable data using MergeIterator.
    /// Tombstones are filtered — deleted keys are not yielded.
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Scanner> {
        Scanner::build(
            &self.memtable_entries,
            &self.version,
            &self.path,
            start,
            end,
        )
    }
}

/// Range scan iterator returned by Snapshot::scan() and DB::scan().
///
/// Wraps a MergeIterator that merges all data sources (memtable + SSTables).
/// Adds two behaviors on top:
/// 1. Range bound: stops when key >= end_key
/// 2. Tombstone filtering: skips entries where value is empty
pub struct Scanner {
    merge: MergeIterator,
    end_key: Vec<u8>,
}

impl Scanner {
    /// Build a Scanner from memtable entries + SSTable version.
    pub(crate) fn build(
        memtable_entries: &[(Vec<u8>, Vec<u8>)],
        version: &Arc<RwLock<Version>>,
        path: &std::path::Path,
        start: &[u8],
        end: &[u8],
    ) -> Result<Self> {
        let mut iters: Vec<Box<dyn StorageIterator>> = Vec::new();

        // Source 0 (highest priority): memtable entries
        iters.push(Box::new(VecIterator::new(memtable_entries.to_vec())));

        // SSTable sources: L0 newest-first, then L1+
        let version = version.read().unwrap();

        // L0: iterate newest-first (higher index = newer in the levels vec)
        for meta in version.level(0).iter().rev() {
            let sst_path = path.join(format!("{:06}.sst", meta.id));
            if let Ok(sst) = SSTable::open(&sst_path) {
                let entries = read_sst_entries(&sst)?;
                iters.push(Box::new(VecIterator::new(entries)));
            }
        }

        // L1+: order within level doesn't matter for correctness
        for level in 1..version.levels.len() {
            for meta in version.level(level) {
                let sst_path = path.join(format!("{:06}.sst", meta.id));
                if let Ok(sst) = SSTable::open(&sst_path) {
                    let entries = read_sst_entries(&sst)?;
                    iters.push(Box::new(VecIterator::new(entries)));
                }
            }
        }

        drop(version); // release lock before building merge

        let mut merge = MergeIterator::new(iters)?;
        // Seek to start of range
        merge.seek(start)?;

        let mut scanner = Scanner {
            merge,
            end_key: end.to_vec(),
        };

        // Skip any initial tombstones
        scanner.skip_tombstones()?;

        Ok(scanner)
    }

    /// Skip forward past any tombstone entries.
    fn skip_tombstones(&mut self) -> Result<()> {
        while self.merge.is_valid()
            && self.merge.key() < self.end_key.as_slice()
            && self.merge.value().is_empty()
        {
            self.merge.next()?;
        }
        Ok(())
    }
}

/// Read all entries from an SSTable into a Vec for use with VecIterator.
/// This sidesteps the SSTableIterator<'a> lifetime issue.
fn read_sst_entries(sst: &SSTable) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut entries = Vec::new();
    let mut iter = sst.iter()?;
    while iter.is_valid() {
        entries.push((iter.key().to_vec(), iter.value().to_vec()));
        iter.next()?;
    }
    Ok(entries)
}

impl StorageIterator for Scanner {
    fn key(&self) -> &[u8] {
        self.merge.key()
    }

    fn value(&self) -> &[u8] {
        self.merge.value()
    }

    fn is_valid(&self) -> bool {
        self.merge.is_valid() && self.merge.key() < self.end_key.as_slice()
    }

    fn next(&mut self) -> Result<()> {
        self.merge.next()?;
        self.skip_tombstones()?;
        Ok(())
    }

    fn seek(&mut self, key: &[u8]) -> Result<()> {
        self.merge.seek(key)?;
        self.skip_tombstones()?;
        Ok(())
    }
}
