pub mod lru;

// TODO [M31]: Implement BlockCache wrapping LRU

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
    // TODO [M31]: Fields
    //   - lru: LRUCache<(u64, u64), Arc<Vec<u8>>> keyed by (sst_id, block_offset)
    //   - hits: u64
    //   - misses: u64
}

impl BlockCache {
    /// Create a new block cache with the given capacity in bytes.
    pub fn new(_capacity: usize) -> Self {
        todo!("[M31]: Initialize LRU cache with capacity")
    }

    /// Look up a cached block.
    pub fn get(&mut self, _sst_id: u64, _block_offset: u64) -> Option<std::sync::Arc<Vec<u8>>> {
        todo!("[M31]: LRU lookup, update hit/miss stats")
    }

    /// Insert a block into the cache. Evicts LRU entries if over capacity.
    pub fn insert(&mut self, _sst_id: u64, _block_offset: u64, _data: Vec<u8>) {
        todo!("[M31]: LRU insert, evict if needed")
    }

    /// Cache hit rate (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        todo!("[M31]: hits / (hits + misses)")
    }
}
