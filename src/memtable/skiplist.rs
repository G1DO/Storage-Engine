// TODO [M01]: Implement skip list — insert and get
// TODO [M02]: Implement skip list iterator
// TODO [M03]: Track size in bytes
use crate::error::Result;
use crate::iterator::StorageIterator;

/// Maximum height of the skip list. LevelDB uses 12.
pub const MAX_HEIGHT: usize = 12;

/// A single node in the skip list.
///
/// Each node has `height` forward pointers. Level 0 contains all nodes
/// (a regular linked list). Higher levels skip over nodes, enabling
/// O(log n) average-case search.
///
/// ```text
/// Level 3:  HEAD ──────────────────────────────► 50 ──────────► NIL
/// Level 2:  HEAD ──────────► 20 ────────────────► 50 ──────────► NIL
/// Level 1:  HEAD ──► 10 ──► 20 ────► 35 ────────► 50 ──► 60 ──► NIL
/// Level 0:  HEAD ──► 10 ──► 20 ──► 25 ──► 35 ──► 50 ──► 60 ──► 70 ► NIL
/// ```
pub struct SkipNode {
    key: Vec<u8>,
    value: Vec<u8>,
    forward: Vec<Option<usize>>, // indices into SkipList.nodes
    //   - key: Vec<u8>
    //   - value: Vec<u8>
    //   - forward: Vec<Option<pointer to next SkipNode at each level>>
    //
    // Design decision: how to represent pointers?
    //   Option A: Box<SkipNode> (owned, simple but hard for back-traversal)
    //   Option B: Raw pointers (unsafe, more flexible)
    //   Option C: Arena allocation with indices (no unsafe, good cache locality)
    //   Start with Option A or C. Avoid raw pointers until you understand why.
}

/// A probabilistic sorted data structure.
///
/// Why skip list over red-black tree?
///   - Simpler to implement correctly
///   - Better cache locality for iteration (level 0 is a linked list)
///   - Lock-free variants are easier (for future concurrent access)
///   - This is what LevelDB uses
///
/// Average case: O(log n) insert, O(log n) lookup, O(n) iteration.
/// Worst case: O(n) — but astronomically unlikely with random level assignment.
pub struct SkipList {
    nodes: Vec<SkipNode>,
    height: usize,
    len: usize,
    
    
    //   - head: SkipNode (sentinel, no real key)
    //   - height: usize (current max level in use)
    //   - len: usize (number of entries)
    //   - size_bytes: usize (total memory tracked)
}

impl SkipList {
    /// Create a new empty skip list.
    pub fn new() -> Self {
        let head = SkipNode {
        key: Vec::new(),
        value: Vec::new(),
        forward: vec![None; MAX_HEIGHT],
    };
    let mut nodes = Vec::new();

    nodes.push(head);

        return SkipList {
            nodes,
            height: 1,
            len: 0,
        };  
    }

    /// Insert a key-value pair. Overwrites if key already exists.
    ///
    /// Algorithm:
    ///   1. Find the insertion point at each level (track predecessors)
    ///   2. Generate a random height for the new node (coin flip per level)
    ///   3. Create node with that height
    ///   4. Splice into the list at each level up to the node's height
    pub fn insert(&mut  self, key: Vec<u8>, value: Vec<u8>) {
        let mut current = 0; // HEAD index
        let mut update: [usize; MAX_HEIGHT] = [0; MAX_HEIGHT];

        // Find insertion point, track predecessors at each level
        for level in (0..self.height).rev() {
            loop {
                let next = self.nodes[current].forward[level];
                if let Some(next_idx) = next {
                    if self.nodes[next_idx].key.as_slice() < key.as_slice() {
                        current = next_idx; // move right
                        continue;
                    }
                    // Check for existing key at level 0
                    if self.nodes[next_idx].key.as_slice() == key.as_slice() {
                        self.nodes[next_idx].value = value;
                        return;
                    }
                }
                break; // can't move right, drop down
            }
            update[level] = current; // record predecessor at this level
        }

        // Generate random height for new node
        let new_height = self.random_height();

        // If new node is taller than current list, update predecessors for new levels
        if new_height > self.height {
            for level in self.height..new_height {
                update[level] = 0; // HEAD is predecessor for new levels
            }
            self.height = new_height;
        }

        // Create new node
        let new_node = SkipNode {
            key,
            value,
            forward: vec![None; new_height],
        };

        // Add to arena, get its index
        let new_idx = self.nodes.len();
        self.nodes.push(new_node);

        // Splice into each level
        for level in 0..new_height {
            // new node points to what predecessor was pointing to
            self.nodes[new_idx].forward[level] = self.nodes[update[level]].forward[level];
            // predecessor now points to new node
            self.nodes[update[level]].forward[level] = Some(new_idx);
        }

        self.len += 1;
    }

    /// Look up a key. Returns the value if found.
    ///
    /// Algorithm:
    ///   1. Start at head, highest level
    ///   2. Move forward while next key < target
    ///   3. Drop down one level
    ///   4. Repeat until level 0
    ///   5. Check if the node at level 0 matches
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
    let mut current = 0; // HEAD index
    let mut level = self.height - 1;

    loop {
        let next = self.nodes[current].forward[level];
        if let Some(next_idx) = next {
            if self.nodes[next_idx].key.as_slice() < key {
                current = next_idx; // move right
                continue;
            }
        }
        // can't move right, go down
        if level == 0 {
            break;
        }
        level -= 1;
    }

    // check the node ahead at level 0
    if let Some(candidate_idx) = self.nodes[current].forward[0] {
        if self.nodes[candidate_idx].key.as_slice() == key {
            return Some(self.nodes[candidate_idx].value.as_slice());
        }
    }

    None
}

    /// Number of entries in the skip list.
    pub fn len(&self) -> usize {
        return self.len;
    }

    /// Whether the skip list is empty.
    pub fn is_empty(&self) -> bool {
        if self.len == 0 {
            return true;
        }
        return false;
    }

    /// Approximate memory usage in bytes.
    pub fn size_bytes(&self) -> usize {
        todo!("[M03]: Return tracked size")
    }

    /// Create an iterator over all entries in sorted order.

    /// Traverses level 0 (the bottom level contains all entries).
    pub fn iter(&self) -> SkipListIterator<'_> {
        SkipListIterator {
        list: self,
        current: self.nodes[0].forward[0],
    }}

    /// Generate a random level for a new node.
    /// Each level has a 1/4 probability (LevelDB uses 1/4, not 1/2).
    /// Higher branching factor = shorter skip list = fewer levels = less memory.
    fn random_height(&self) -> usize {
    let mut height = 1;
    while height < MAX_HEIGHT && rand::random::<f64>() < 0.25 {
        height += 1;
    }   
    return height;
}   

}

/// Iterator over skip list entries in sorted order.
///
/// Simply follows level 0 forward pointers — level 0 is a sorted linked list
/// containing every entry.
pub struct SkipListIterator<'a> {
    list: &'a SkipList,
    current: Option<usize>,
}

impl<'a> SkipListIterator<'a> {
    /// Returns true if iterator is at a valid position.
    pub fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    /// Returns the key at current position.
    /// Panics if iterator is not valid.
    pub fn key(&self) -> &'a [u8] {
        let idx = self.current.expect("iterator not valid");
        self.list.nodes[idx].key.as_slice()
    }

    /// Returns the value at current position.
    /// Panics if iterator is not valid.
    pub fn value(&self) -> &'a [u8] {
        let idx = self.current.expect("iterator not valid");
        self.list.nodes[idx].value.as_slice()
    }

    /// Advances to the next entry.
    pub fn advance(&mut self) {
        if let Some(idx) = self.current {
            self.current = self.list.nodes[idx].forward[0];
        }
    }

    /// Seek to the first key >= target (internal implementation).
    fn seek_to(&mut self, target: &[u8]) {
        let mut current = 0; // HEAD
        let mut level = self.list.height - 1;

        loop {
            let next = self.list.nodes[current].forward[level];
            if let Some(next_idx) = next {
                if self.list.nodes[next_idx].key.as_slice() < target {
                    current = next_idx;
                    continue;
                }
            }
            if level == 0 {
                break;
            }
            level -= 1;
        }

        // current is predecessor, forward[0] is first key >= target (or None)
        self.current = self.list.nodes[current].forward[0];
    }
}

impl<'a> StorageIterator for SkipListIterator<'a> {
    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn key(&self) -> &[u8] {
        let idx = self.current.expect("iterator not valid");
        self.list.nodes[idx].key.as_slice()
    }

    fn value(&self) -> &[u8] {
        let idx = self.current.expect("iterator not valid");
        self.list.nodes[idx].value.as_slice()
    }

    fn next(&mut self) -> Result<()> {
        self.advance();
        Ok(())
    }

    fn seek(&mut self, key: &[u8]) -> Result<()> {
        self.seek_to(key);
        Ok(())
    }
}
