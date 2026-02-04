pub mod skiplist;

use skiplist::{SkipList, SkipListIterator};

// TODO [M04]: Implement MemTable API
// TODO [M05]: Add concurrent access with Arc<RwLock<MemTable>>

/// In-memory sorted buffer for writes. Wraps a SkipList.
///
/// Every write goes here first. When size exceeds the threshold,
/// the memtable is frozen (becomes immutable) and flushed to an SSTable.
///
/// Deletes are handled via tombstones — an empty value that means
/// "this key is deleted." You can't just remove the key because older
/// versions may exist in SSTables on disk.
pub struct MemTable {
    data: SkipList,
    size_limit: usize,
}

impl MemTable {
    /// Create a new empty memtable with given size limit.
    pub fn new(size_limit: usize) -> Self {
        MemTable {
            data: SkipList::new(),
            size_limit,
        }
    }

    /// Insert or update a key-value pair.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.data.insert(key, value);
    }

    /// Look up a key. Returns None if not found OR if tombstoned.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.data.get(key) {
            Some(v) if v.is_empty() => None,  // tombstone
            Some(v) => Some(v),
            None => None,
        }
    }

    /// Mark a key as deleted by writing a tombstone (empty value).
    pub fn delete(&mut self, key: Vec<u8>) {
        self.data.insert(key, Vec::new());  // empty = tombstone
    }

    /// Return a sorted iterator over all entries (including tombstones).
    pub fn iter(&self) -> SkipListIterator<'_> {
        self.data.iter()
    }

    /// Current memory usage in bytes.
    pub fn size(&self) -> usize {
        self.data.size_bytes()
    }

    /// Check if memtable has reached the flush threshold.
    pub fn is_full(&self) -> bool {
        self.data.size_bytes() >= self.size_limit
    }
}
