// LRU Cache tests (M30)
//
// Tests stay `#[ignore]` until the LRU cache is implemented.

use lsm_engine::cache::lru::LRUCache;

// ---------------------------------------------------------------------------
// M30: LRU Cache Tests
// ---------------------------------------------------------------------------

#[test]
fn insert_within_capacity_all_present() {
    let mut cache = LRUCache::new(3);
    cache.insert("a", 1, 1);
    cache.insert("b", 2, 1);
    cache.insert("c", 3, 1);

    assert_eq!(cache.len(), 3);
    assert_eq!(cache.get(&"a"), Some(&1));
    assert_eq!(cache.get(&"b"), Some(&2));
    assert_eq!(cache.get(&"c"), Some(&3));
}

#[test]
fn insert_over_capacity_evicts_oldest() {
    let mut cache = LRUCache::new(3);
    cache.insert("a", 1, 1);
    cache.insert("b", 2, 1);
    cache.insert("c", 3, 1);

    // Insert 4th — "a" is LRU, should be evicted
    cache.insert("d", 4, 1);

    assert_eq!(cache.len(), 3);
    assert_eq!(cache.get(&"a"), None);
    assert_eq!(cache.get(&"b"), Some(&2));
    assert_eq!(cache.get(&"c"), Some(&3));
    assert_eq!(cache.get(&"d"), Some(&4));
}

#[test]
fn get_moves_to_most_recent() {
    let mut cache = LRUCache::new(3);
    cache.insert("a", 1, 1);
    cache.insert("b", 2, 1);
    cache.insert("c", 3, 1);

    // Access "a" — moves it to most recent
    assert_eq!(cache.get(&"a"), Some(&1));

    // Insert "d" — should evict "b" (now the LRU), not "a"
    cache.insert("d", 4, 1);

    assert_eq!(cache.len(), 3);
    assert_eq!(cache.get(&"a"), Some(&1));
    assert_eq!(cache.get(&"b"), None);
    assert_eq!(cache.get(&"c"), Some(&3));
    assert_eq!(cache.get(&"d"), Some(&4));
}

#[test]
fn access_pattern_evicts_correct_entry() {
    let mut cache = LRUCache::new(2);
    cache.insert("a", 1, 1);
    cache.insert("b", 2, 1);

    // Access "a" — "b" is now LRU
    assert_eq!(cache.get(&"a"), Some(&1));

    // Insert "c" — "b" evicted (LRU), not "a" (recently accessed)
    cache.insert("c", 3, 1);

    assert_eq!(cache.len(), 2);
    assert_eq!(cache.get(&"a"), Some(&1));
    assert_eq!(cache.get(&"b"), None);
    assert_eq!(cache.get(&"c"), Some(&3));
}

#[test]
fn remove_frees_capacity() {
    let mut cache = LRUCache::new(3);
    cache.insert("a", 1, 1);
    cache.insert("b", 2, 1);
    cache.insert("c", 3, 1);

    // Remove "b" — frees 1 byte of capacity
    let removed = cache.remove(&"b");
    assert_eq!(removed, Some(2));
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.get(&"b"), None);

    // Insert "d" — fits without eviction (capacity 3, used 2)
    cache.insert("d", 4, 1);

    assert_eq!(cache.len(), 3);
    assert_eq!(cache.get(&"a"), Some(&1));
    assert_eq!(cache.get(&"c"), Some(&3));
    assert_eq!(cache.get(&"d"), Some(&4));
}

#[test]
fn empty_cache_get_returns_none() {
    let mut cache: LRUCache<&str, i32> = LRUCache::new(3);

    assert_eq!(cache.get(&"anything"), None);
    assert_eq!(cache.len(), 0);
    assert!(cache.is_empty());
}

#[test]
fn insert_duplicate_key_updates_value() {
    let mut cache = LRUCache::new(3);
    cache.insert("x", 1, 1);
    cache.insert("x", 2, 1);

    assert_eq!(cache.get(&"x"), Some(&2));
    assert_eq!(cache.len(), 1);
}

#[test]
fn insert_duplicate_key_updates_size() {
    let mut cache = LRUCache::new(3);
    cache.insert("x", 10, 2); // 2 bytes used
    cache.insert("y", 20, 1); // 3 bytes used — full

    // Re-insert "x" with smaller size
    cache.insert("x", 11, 1); // now 2 bytes used (x=1 + y=1)

    // Insert "z" — should fit without eviction (2 + 1 = 3 ≤ 3)
    cache.insert("z", 30, 1);

    assert_eq!(cache.len(), 3);
    assert_eq!(cache.get(&"x"), Some(&11));
    assert_eq!(cache.get(&"y"), Some(&20));
    assert_eq!(cache.get(&"z"), Some(&30));
}

#[test]
fn eviction_cascades_for_large_entry() {
    let mut cache = LRUCache::new(3);
    cache.insert("a", 1, 1); // tail (LRU)
    cache.insert("b", 2, 1);
    cache.insert("c", 3, 1); // head (MRU)

    // Insert "d" with size=2 — must evict "a" (1 byte) then "b" (1 byte)
    // to free 2 bytes: current_size goes from 3 → 2 → 1, then +2 = 3
    cache.insert("d", 4, 2);

    assert_eq!(cache.get(&"a"), None);
    assert_eq!(cache.get(&"b"), None);
    assert_eq!(cache.get(&"c"), Some(&3));
    assert_eq!(cache.get(&"d"), Some(&4));
}

#[test]
fn remove_nonexistent_returns_none() {
    let mut cache: LRUCache<&str, i32> = LRUCache::new(3);
    cache.insert("a", 1, 1);

    assert_eq!(cache.remove(&"z"), None);
    assert_eq!(cache.len(), 1);
}
