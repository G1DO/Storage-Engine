use std::hash::Hash;

// TODO [M30]: Implement LRU cache

/// Generic LRU (Least Recently Used) cache.
///
/// Data structure: HashMap + doubly-linked list.
/// - HashMap: O(1) lookup by key → node in the linked list
/// - Linked list: O(1) move-to-front on access, O(1) evict from tail
///
/// On get: move accessed node to front of list.
/// On insert: add to front. If over capacity, evict from tail.
pub struct LRUCache<K, V> {
    // TODO [M30]: Fields
    //   - map: HashMap<K, (V, link to list position)>
    //   - order: doubly-linked list of keys (head = most recent, tail = least recent)
    //   - capacity: usize
    //   - current_size: usize
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K: Eq + Hash + Clone, V> LRUCache<K, V> {
    /// Create a new LRU cache with the given capacity.
    pub fn new(_capacity: usize) -> Self {
        todo!("[M30]: Initialize empty cache")
    }

    /// Look up a key. Moves it to most-recently-used position.
    pub fn get(&mut self, _key: &K) -> Option<&V> {
        todo!("[M30]: HashMap lookup, move to front of list")
    }

    /// Insert a key-value pair. Evicts LRU entry if over capacity.
    /// `size` is the size in bytes of this entry (for capacity tracking).
    pub fn insert(&mut self, _key: K, _value: V, _size: usize) {
        todo!("[M30]: Insert at front, evict from tail if needed")
    }

    /// Remove a key from the cache.
    pub fn remove(&mut self, _key: &K) -> Option<V> {
        todo!("[M30]: Remove from map and list")
    }

    /// Number of entries in the cache.
    pub fn len(&self) -> usize {
        todo!("[M30]: Return map.len()")
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        todo!("[M30]: Check len == 0")
    }
}
