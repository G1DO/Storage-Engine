use std::collections::HashMap;
use std::hash::Hash;

/// A node in the index-based doubly-linked list.
///
/// Instead of pointers (which would require `unsafe`), we use indices
/// into a `Vec<Node>` arena — the same pattern as the skiplist.
struct Node<K> {
    key: K,
    prev: Option<usize>,
    next: Option<usize>,
}

/// Generic LRU (Least Recently Used) cache.
///
/// Data structure: HashMap + doubly-linked list.
/// - HashMap: O(1) lookup by key → node in the linked list
/// - Linked list: O(1) move-to-front on access, O(1) evict from tail
///
/// On get: move accessed node to front of list.
/// On insert: add to front. If over capacity, evict from tail.
///
/// Values are stored in the HashMap (not in Node) so that `get()` can
/// mutate the list (`self.nodes`) while returning `&V` from `self.map`
/// — disjoint struct fields avoid borrow conflicts.
pub struct LRUCache<K, V> {
    map: HashMap<K, (usize, V, usize)>, // key → (node_index, value, entry_size)
    nodes: Vec<Node<K>>,                 // arena for linked list nodes
    head: Option<usize>,                 // most recently used (front)
    tail: Option<usize>,                 // least recently used (back)
    capacity: usize,
    current_size: usize,
    free: Vec<usize>, // recycled node slots
}

impl<K: Eq + Hash + Clone, V> LRUCache<K, V> {
    /// Create a new LRU cache with the given capacity (in bytes).
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            nodes: Vec::new(),
            head: None,
            tail: None,
            capacity,
            current_size: 0,
            free: Vec::new(),
        }
    }

    /// Look up a key. Moves it to most-recently-used position.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        // Extract node_index by value (usize is Copy) — borrow of self.map ends here
        let node_idx = self.map.get(key)?.0;

        // Mutate the list: detach from current position, push to head
        self.detach(node_idx);
        self.push_front(node_idx);

        // Fresh borrow of self.map to return &V
        self.map.get(key).map(|(_, v, _)| v)
    }

    /// Insert a key-value pair. Evicts LRU entry if over capacity.
    /// `size` is the size in bytes of this entry (for capacity tracking).
    pub fn insert(&mut self, key: K, value: V, size: usize) {
        // Handle duplicate key: remove old entry first
        if self.map.contains_key(&key) {
            self.remove(&key);
        }

        // Eviction loop: evict LRU entries until there is room
        while self.current_size + size > self.capacity && self.tail.is_some() {
            self.evict_lru();
        }

        // Allocate a node and push to front
        let idx = self.alloc_node(key.clone());
        self.push_front(idx);

        // Insert into map
        self.map.insert(key, (idx, value, size));
        self.current_size += size;
    }

    /// Remove a key from the cache.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let (node_idx, value, entry_size) = self.map.remove(key)?;

        self.detach(node_idx);
        self.current_size -= entry_size;
        self.free.push(node_idx);

        Some(value)
    }

    /// Number of entries in the cache.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    // --- Internal helpers ---

    /// Allocate a node slot: reuse from free list or push new.
    fn alloc_node(&mut self, key: K) -> usize {
        if let Some(idx) = self.free.pop() {
            self.nodes[idx] = Node {
                key,
                prev: None,
                next: None,
            };
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(Node {
                key,
                prev: None,
                next: None,
            });
            idx
        }
    }

    /// Unlink a node from its current position in the list.
    fn detach(&mut self, idx: usize) {
        let prev = self.nodes[idx].prev;
        let next = self.nodes[idx].next;

        // Patch predecessor's next
        if let Some(p) = prev {
            self.nodes[p].next = next;
        } else {
            self.head = next; // node was the head
        }

        // Patch successor's prev
        if let Some(n) = next {
            self.nodes[n].prev = prev;
        } else {
            self.tail = prev; // node was the tail
        }

        // Clear own links
        self.nodes[idx].prev = None;
        self.nodes[idx].next = None;
    }

    /// Insert a node at the head (most-recently-used position).
    fn push_front(&mut self, idx: usize) {
        self.nodes[idx].prev = None;
        self.nodes[idx].next = self.head;

        if let Some(old_head) = self.head {
            self.nodes[old_head].prev = Some(idx);
        }

        self.head = Some(idx);

        if self.tail.is_none() {
            self.tail = Some(idx); // first node in list
        }
    }

    /// Evict the tail (least-recently-used) entry.
    fn evict_lru(&mut self) {
        let tail_idx = match self.tail {
            Some(idx) => idx,
            None => return,
        };

        let key = self.nodes[tail_idx].key.clone();
        self.detach(tail_idx);

        if let Some((_, _, entry_size)) = self.map.remove(&key) {
            self.current_size -= entry_size;
        }

        self.free.push(tail_idx);
    }
}
