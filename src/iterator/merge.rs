use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::error::Result;
use crate::iterator::StorageIterator;

// ---------------------------------------------------------------------------
// Heap entry: tracks which iterator produced a key, ordered for min-heap.
// ---------------------------------------------------------------------------

struct HeapEntry {
    /// Current key bytes (cloned from the iterator — needed for heap ordering
    /// because we can't borrow from the iterator and mutate it at the same time).
    key: Vec<u8>,
    /// Index into MergeIterator.iters. Lower index = newer source.
    index: usize,
}

// Rust's BinaryHeap is a max-heap. We reverse the comparison so that
// popping gives us the *smallest* key (and lowest index on ties).
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.index.cmp(&self.index))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.index == other.index
    }
}

impl Eq for HeapEntry {}

// ---------------------------------------------------------------------------
// MergeIterator: k-way merge with deduplication, keeps tombstones.
// ---------------------------------------------------------------------------

/// Merges multiple sorted iterators into a single sorted stream.
///
/// Used for:
/// - Compaction (merging SSTables) — M19
/// - Range scans across memtable + all SSTable levels — M25 adds tombstone filtering
///
/// Ordering: entries are yielded in key-ascending order.
/// Deduplication: when multiple sources have the same key, only the newest
/// (lowest iterator index) is yielded. Others are silently skipped.
///
/// Tombstones are NOT filtered — they pass through as normal entries.
/// The caller (compaction or read path) decides how to handle them.
pub struct MergeIterator {
    /// Sub-iterators, ordered by priority: index 0 = newest source.
    iters: Vec<Box<dyn StorageIterator>>,
    /// Min-heap of (key, iterator_index) for entries waiting to be yielded.
    heap: BinaryHeap<HeapEntry>,
    /// Index of the iterator currently producing key()/value(), or None if exhausted.
    current: Option<usize>,
}

impl MergeIterator {
    /// Create a new MergeIterator from multiple sorted sources.
    ///
    /// Sources are ordered by priority: index 0 = newest (e.g., memtable),
    /// higher indices = older (e.g., deeper SSTable levels).
    pub fn new(iters: Vec<Box<dyn StorageIterator>>) -> Result<Self> {
        let mut heap = BinaryHeap::new();

        for (i, iter) in iters.iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapEntry {
                    key: iter.key().to_vec(),
                    index: i,
                });
            }
        }

        let mut merge = Self {
            iters,
            heap,
            current: None,
        };

        // Position at the first unique key.
        merge.advance_to_next_unique()?;

        Ok(merge)
    }

    /// Pop the smallest key from the heap and skip any duplicate keys
    /// from older sources. After this call, `self.current` points to
    /// the iterator holding the winning entry, or is None if exhausted.
    fn advance_to_next_unique(&mut self) -> Result<()> {
        match self.heap.pop() {
            Some(entry) => {
                self.current = Some(entry.index);
                let current_key = entry.key;

                // Drain all heap entries with the same key — these are
                // older duplicates. Advance their iterators past this key.
                while let Some(top) = self.heap.peek() {
                    if top.key != current_key {
                        break;
                    }
                    let dup = self.heap.pop().unwrap();
                    self.iters[dup.index].next()?;
                    if self.iters[dup.index].is_valid() {
                        self.heap.push(HeapEntry {
                            key: self.iters[dup.index].key().to_vec(),
                            index: dup.index,
                        });
                    }
                }
            }
            None => {
                self.current = None;
            }
        }
        Ok(())
    }
}

impl StorageIterator for MergeIterator {
    fn key(&self) -> &[u8] {
        // current is guaranteed Some when is_valid() is true.
        self.iters[self.current.unwrap()].key()
    }

    fn value(&self) -> &[u8] {
        self.iters[self.current.unwrap()].value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        if let Some(idx) = self.current {
            // Advance the current winner past its entry.
            self.iters[idx].next()?;
            if self.iters[idx].is_valid() {
                self.heap.push(HeapEntry {
                    key: self.iters[idx].key().to_vec(),
                    index: idx,
                });
            }

            // Move to the next unique key.
            self.advance_to_next_unique()?;
        }
        Ok(())
    }

    fn seek(&mut self, key: &[u8]) -> Result<()> {
        // Seek every sub-iterator and rebuild the heap from scratch.
        self.heap.clear();
        for (i, iter) in self.iters.iter_mut().enumerate() {
            iter.seek(key)?;
            if iter.is_valid() {
                self.heap.push(HeapEntry {
                    key: iter.key().to_vec(),
                    index: i,
                });
            }
        }

        self.current = None;
        self.advance_to_next_unique()?;
        Ok(())
    }
}
