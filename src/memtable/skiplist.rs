// TODO [M01]: Implement skip list — insert and get
// TODO [M02]: Implement skip list iterator
// TODO [M03]: Track size in bytes

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
        todo!("[M01]: Initialize head sentinel with MAX_HEIGHT forward pointers")
    }

    /// Insert a key-value pair. Overwrites if key already exists.
    ///
    /// Algorithm:
    ///   1. Find the insertion point at each level (track predecessors)
    ///   2. Generate a random height for the new node (coin flip per level)
    ///   3. Create node with that height
    ///   4. Splice into the list at each level up to the node's height
    pub fn insert(&mut self, _key: Vec<u8>, _value: Vec<u8>) {
        todo!("[M01]: Insert with probabilistic level assignment")
    }

    /// Look up a key. Returns the value if found.
    ///
    /// Algorithm:
    ///   1. Start at head, highest level
    ///   2. Move forward while next key < target
    ///   3. Drop down one level
    ///   4. Repeat until level 0
    ///   5. Check if the node at level 0 matches
    pub fn get(&self, _key: &[u8]) -> Option<&[u8]> {
        todo!("[M01]: Traverse from highest level down")
    }

    /// Number of entries in the skip list.
    pub fn len(&self) -> usize {
        todo!("[M01]: Return len")
    }

    /// Whether the skip list is empty.
    pub fn is_empty(&self) -> bool {
        todo!("[M01]: Check len == 0")
    }

    /// Approximate memory usage in bytes.
    pub fn size_bytes(&self) -> usize {
        todo!("[M03]: Return tracked size")
    }

    /// Create an iterator over all entries in sorted order.
    /// Traverses level 0 (the bottom level contains all entries).
    pub fn iter(&self) -> SkipListIterator<'_> {
        todo!("[M02]: Start iterator at first real node on level 0")
    }

    /// Generate a random level for a new node.
    /// Each level has a 1/4 probability (LevelDB uses 1/4, not 1/2).
    /// Higher branching factor = shorter skip list = fewer levels = less memory.
    fn random_height(&self) -> usize {
        todo!("[M01]: Coin flip loop, cap at MAX_HEIGHT")
    }
}

/// Iterator over skip list entries in sorted order.
///
/// Simply follows level 0 forward pointers — level 0 is a sorted linked list
/// containing every entry.
pub struct SkipListIterator<'a> {
    // TODO [M02]: Fields
    //   - current: reference to current node (or None if exhausted)
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> SkipListIterator<'a> {
    // TODO [M02]: Implement Iterator trait or StorageIterator trait
}
