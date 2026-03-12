pub mod lru;

use std::sync::Arc;

use crate::cache::lru::LRUCache;

/// Cache for frequently accessed SSTable data blocks.
///
/// Why not just use the OS page cache?
/// - Can't control eviction policy (OS uses page-level LRU)
/// - Can't prioritize (index blocks > data blocks)
/// - Can't track hit rates for observability
///
/// Cache policy:
/// - Always cache index blocks (small, accessed on every lookup)
/// - Bloom filters loaded into memory on SSTable open (kept forever)
/// - LRU eviction for data blocks
/// - Track hit rate: if < 50%, cache is too small
pub struct BlockCache {
    lru: LRUCache<(u64, u64), Arc<Vec<u8>>>,
    hits: u64,
    misses: u64,
}

impl BlockCache {
    /// Create a new block cache with the given capacity in bytes.
    pub fn new(capacity: usize) -> Self {
        Self {
            lru: LRUCache::new(capacity),
            hits: 0,
            misses: 0,
        }
    }

    /// Look up a cached block.
    ///
    /// On hit: increments hit counter, moves block to MRU position, returns Arc clone.
    /// On miss: increments miss counter, returns None.
    pub fn get(&mut self, sst_id: u64, block_offset: u64) -> Option<Arc<Vec<u8>>> {
        match self.lru.get(&(sst_id, block_offset)) {
            Some(arc) => {
                self.hits += 1;
                Some(Arc::clone(arc))
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    /// Insert a block into the cache. Evicts LRU entries if over capacity.
    ///
    /// The data is wrapped in an Arc so multiple concurrent readers can
    /// share the cached block without copying.
    pub fn insert(&mut self, sst_id: u64, block_offset: u64, data: Vec<u8>) {
        let size = data.len();
        let arc_data = Arc::new(data);
        self.lru.insert((sst_id, block_offset), arc_data, size);
    }

    /// Cache hit rate (0.0 to 1.0).
    ///
    /// Returns 0.0 when no accesses have been made (avoids NaN from 0/0).
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}
