pub mod skiplist;

use skiplist::{SkipList, SkipListIterator};
use std::sync::RwLock;

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

/// Thread-safe manager for active and immutable memtables.
///
/// Uses RwLock for concurrent read access and exclusive write access.
/// The active/immutable pattern allows writes to continue during flush:
///   - active: receives new writes
///   - immutable: being flushed to SSTable (read-only)
pub struct MemTableManager {
    active: RwLock<MemTable>,
    immutable: RwLock<Option<MemTable>>,
    size_limit: usize,
}

impl MemTableManager {
    /// Create a new manager with given size limit per memtable.
    pub fn new(size_limit: usize) -> Self {
        MemTableManager {
            active: RwLock::new(MemTable::new(size_limit)),
            immutable: RwLock::new(None),
            size_limit,
        }
    }

    /// Insert or update a key-value pair.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut active = self.active.write().unwrap();
        active.put(key, value);
    }

    /// Look up a key. Checks active first, then immutable.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Check active first (newer data)
        {
            let active = self.active.read().unwrap();
            if let Some(v) = active.get(key) {
                return Some(v.to_vec());
            }
        }

        // Check immutable if exists
        {
            let immutable = self.immutable.read().unwrap();
            if let Some(ref imm) = *immutable {
                if let Some(v) = imm.get(key) {
                    return Some(v.to_vec());
                }
            }
        }

        None
    }

    /// Mark a key as deleted.
    pub fn delete(&self, key: Vec<u8>) {
        let mut active = self.active.write().unwrap();
        active.delete(key);
    }

    /// Freeze the active memtable: move it to immutable, create new active.
    /// Call this when active is full and ready to flush.
    pub fn freeze(&self) {
        let mut active = self.active.write().unwrap();
        let mut immutable = self.immutable.write().unwrap();

        // Take the current active, replace with new empty one
        let old_active = std::mem::replace(&mut *active, MemTable::new(self.size_limit));

        // Move old active to immutable
        *immutable = Some(old_active);
    }

    /// Check if there's an immutable memtable waiting to be flushed.
    pub fn has_immutable(&self) -> bool {
        let immutable = self.immutable.read().unwrap();
        immutable.is_some()
    }

    /// Clear the immutable memtable after flush is complete.
    pub fn clear_immutable(&self) {
        let mut immutable = self.immutable.write().unwrap();
        *immutable = None;
    }

    /// Check if active memtable is full.
    pub fn is_full(&self) -> bool {
        let active = self.active.read().unwrap();
        active.is_full()
    }
}
