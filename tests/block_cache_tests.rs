// M31: Block Cache Integration tests
// Verify BlockCache wraps LRUCache with SSTable-aware keying and hit/miss tracking.

use std::sync::{Arc, Mutex};
use std::thread;

use lsm_engine::cache::BlockCache;

// =============================================================================
// Test 1: Cache miss returns None
// =============================================================================
#[test]
fn cache_miss_returns_none() {
    let mut cache = BlockCache::new(4096);

    // Empty cache — every get is a miss
    assert!(cache.get(1, 0).is_none());
    assert!(cache.get(1, 100).is_none());
    assert!(cache.get(2, 0).is_none());
}

// =============================================================================
// Test 2: Insert then get returns same data via Arc
// =============================================================================
#[test]
fn insert_then_get_returns_data() {
    let mut cache = BlockCache::new(4096);
    let block = vec![1, 2, 3, 4, 5];

    cache.insert(1, 0, block.clone());

    let cached = cache.get(1, 0).expect("should be a hit");
    assert_eq!(*cached, block);

    // Arc clone is cheap — both point to same allocation
    let clone = Arc::clone(&cached);
    assert_eq!(*clone, vec![1, 2, 3, 4, 5]);
}

// =============================================================================
// Test 3: Hit and miss counting → hit_rate() accuracy
// =============================================================================
#[test]
fn hit_and_miss_counting() {
    let mut cache = BlockCache::new(4096);
    cache.insert(1, 0, vec![10; 100]);

    // Hit
    cache.get(1, 0);
    // Miss
    cache.get(1, 999);
    // Hit
    cache.get(1, 0);

    // 2 hits, 1 miss → 2/3 ≈ 0.6667
    let rate = cache.hit_rate();
    assert!(
        (rate - 2.0 / 3.0).abs() < 1e-10,
        "expected hit_rate ≈ 0.6667, got {}",
        rate,
    );
}

// =============================================================================
// Test 4: 50 hits + 50 misses → hit_rate() == 0.5 (milestone requirement #4)
// =============================================================================
#[test]
fn hit_rate_fifty_percent() {
    let mut cache = BlockCache::new(1024 * 1024); // 1MB, large enough

    // Insert 50 blocks
    for i in 0..50u64 {
        cache.insert(1, i * 100, vec![0; 10]);
    }

    // 50 hits: get blocks that exist
    for i in 0..50u64 {
        let result = cache.get(1, i * 100);
        assert!(result.is_some(), "block at offset {} should hit", i * 100);
    }

    // 50 misses: get blocks that don't exist
    for i in 50..100u64 {
        let result = cache.get(1, i * 100);
        assert!(result.is_none(), "block at offset {} should miss", i * 100);
    }

    let rate = cache.hit_rate();
    assert!(
        (rate - 0.5).abs() < f64::EPSILON,
        "expected hit_rate = 0.5, got {}",
        rate,
    );
}

// =============================================================================
// Test 5: hit_rate() with zero accesses → 0.0, not NaN or panic
// =============================================================================
#[test]
fn hit_rate_zero_when_no_accesses() {
    let cache = BlockCache::new(4096);

    // No get() calls at all — 0 hits, 0 misses
    let rate = cache.hit_rate();
    assert_eq!(rate, 0.0, "hit_rate with no accesses should be 0.0");
    assert!(!rate.is_nan(), "hit_rate should not be NaN");
}

// =============================================================================
// Test 6: Eviction when capacity exceeded (milestone requirement #3)
// =============================================================================
#[test]
fn eviction_when_capacity_exceeded() {
    // Capacity = 300 bytes. Insert three 100-byte blocks → full.
    let mut cache = BlockCache::new(300);
    cache.insert(1, 0, vec![0xAA; 100]);
    cache.insert(1, 100, vec![0xBB; 100]);
    cache.insert(1, 200, vec![0xCC; 100]);

    // Cache is exactly full. All three should be present.
    assert!(cache.get(1, 0).is_some());
    assert!(cache.get(1, 100).is_some());
    assert!(cache.get(1, 200).is_some());

    // Insert a fourth block → must evict LRU.
    // After the three gets above, access order is: 200 (MRU), 100, 0 (LRU).
    // Wait — we accessed them all in order 0, 100, 200, so LRU is 0.
    // But then we called get(1,0), get(1,100), get(1,200) to verify.
    // After those gets: LRU = 0 (accessed first), MRU = 200 (accessed last).
    // Actually: get(1,0) makes 0 MRU, get(1,100) makes 100 MRU, get(1,200) makes 200 MRU.
    // So order is now: 200 (MRU) → 100 → 0 (LRU).
    // Insert new block → evicts 0.
    cache.insert(1, 300, vec![0xDD; 100]);

    assert!(cache.get(1, 0).is_none(), "block 0 should be evicted");
    assert!(cache.get(1, 100).is_some(), "block 100 should remain");
    assert!(cache.get(1, 200).is_some(), "block 200 should remain");
    assert!(cache.get(1, 300).is_some(), "block 300 should be present");
}

// =============================================================================
// Test 7: get() refreshes recency — accessed block survives eviction
// =============================================================================
#[test]
fn get_refreshes_recency() {
    let mut cache = BlockCache::new(300);
    cache.insert(1, 0, vec![0xAA; 100]); // oldest
    cache.insert(1, 100, vec![0xBB; 100]);
    cache.insert(1, 200, vec![0xCC; 100]); // newest

    // Access block 0 → moves it to MRU position
    // Order was: 200 (MRU), 100, 0 (LRU)
    // After get(1,0): 0 (MRU), 200, 100 (LRU)
    cache.get(1, 0);

    // Insert new block → should evict 100 (now LRU), NOT 0
    cache.insert(1, 300, vec![0xDD; 100]);

    assert!(cache.get(1, 0).is_some(), "block 0 was refreshed, should survive");
    assert!(cache.get(1, 100).is_none(), "block 100 should be evicted (was LRU)");
    assert!(cache.get(1, 200).is_some(), "block 200 should remain");
    assert!(cache.get(1, 300).is_some(), "block 300 should be present");
}

// =============================================================================
// Test 8: Different SSTable IDs are independent cache keys
// =============================================================================
#[test]
fn different_sst_ids_are_independent() {
    let mut cache = BlockCache::new(4096);

    // Same offset, different SSTable IDs → different entries
    cache.insert(1, 0, vec![0x11; 50]);
    cache.insert(2, 0, vec![0x22; 50]);

    let block_1 = cache.get(1, 0).expect("sst 1 block should exist");
    let block_2 = cache.get(2, 0).expect("sst 2 block should exist");

    assert_eq!(*block_1, vec![0x11; 50]);
    assert_eq!(*block_2, vec![0x22; 50]);
    assert_ne!(*block_1, *block_2, "different SSTables should have different data");
}

// =============================================================================
// Test 9: Large block evicts multiple small blocks (cascading eviction)
// =============================================================================
#[test]
fn large_block_evicts_multiple_small() {
    // Capacity = 300. Insert three 100-byte blocks → full.
    let mut cache = BlockCache::new(300);
    cache.insert(1, 0, vec![0xAA; 100]);
    cache.insert(1, 100, vec![0xBB; 100]);
    cache.insert(1, 200, vec![0xCC; 100]);

    // Insert one 250-byte block → must evict at least two small blocks to fit
    // LRU order: 0 (LRU), 100, 200 (MRU)
    // Evict 0 → 200 bytes used, need 250 more, still over (200+250=450 > 300)
    // Evict 100 → 100 bytes used, 100+250=350 > 300, still over
    // Evict 200 → 0 bytes used, 0+250=250 ≤ 300, fits
    cache.insert(1, 300, vec![0xDD; 250]);

    assert!(cache.get(1, 0).is_none(), "block 0 should be evicted");
    assert!(cache.get(1, 100).is_none(), "block 100 should be evicted");
    assert!(cache.get(1, 200).is_none(), "block 200 should be evicted");
    assert!(cache.get(1, 300).is_some(), "large block should be present");
}

// =============================================================================
// Test 10: Concurrent reads with Arc<Mutex<BlockCache>> — no data races
// (milestone requirement #5)
// =============================================================================
#[test]
fn concurrent_reads_no_data_races() {
    let cache = Arc::new(Mutex::new(BlockCache::new(64 * 1024)));

    // Pre-populate 10 blocks
    {
        let mut c = cache.lock().unwrap();
        for i in 0..10u64 {
            c.insert(1, i * 100, vec![i as u8; 100]);
        }
    }

    // Spawn 8 threads, each doing 100 get() calls
    let mut handles = vec![];
    for thread_id in 0..8u64 {
        let cache_clone = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for round in 0..100u64 {
                let offset = ((thread_id + round) % 10) * 100;
                let mut c = cache_clone.lock().unwrap();
                let result = c.get(1, offset);
                assert!(result.is_some(), "thread {} round {} offset {} should hit", thread_id, round, offset);
            }
        }));
    }

    // All threads must complete without panic
    for handle in handles {
        handle.join().expect("thread should not panic");
    }

    // Verify all blocks still intact after concurrent access
    let mut c = cache.lock().unwrap();
    for i in 0..10u64 {
        let block = c.get(1, i * 100).expect("block should still exist");
        assert_eq!(*block, vec![i as u8; 100], "block data should be intact");
    }
}
