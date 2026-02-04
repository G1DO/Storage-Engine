// M04: MemTable API with Tombstones
// Tests for the memtable wrapper around skip list.

use lsm_engine::memtable::MemTable;
use lsm_engine::iterator::StorageIterator;

// =============================================================================
// Test 1: Basic put and get
// =============================================================================
#[test]
fn put_then_get_returns_value() {
    let mut mt = MemTable::new(1024 * 1024); // 1MB limit
    mt.put(b"key".to_vec(), b"value".to_vec());

    assert_eq!(mt.get(b"key"), Some(b"value".as_slice()));
}

// =============================================================================
// Test 2: Get non-existent key
// =============================================================================
#[test]
fn get_nonexistent_returns_none() {
    let mt = MemTable::new(1024 * 1024);
    assert_eq!(mt.get(b"missing"), None);
}

// =============================================================================
// Test 3: Delete returns None on get
// =============================================================================
#[test]
fn delete_then_get_returns_none() {
    let mut mt = MemTable::new(1024 * 1024);
    mt.put(b"key".to_vec(), b"value".to_vec());
    mt.delete(b"key".to_vec());

    // After delete, get should return None
    assert_eq!(mt.get(b"key"), None);
}

// =============================================================================
// Test 4: Put after delete returns new value
// =============================================================================
#[test]
fn put_delete_put_returns_new_value() {
    let mut mt = MemTable::new(1024 * 1024);
    mt.put(b"key".to_vec(), b"first".to_vec());
    mt.delete(b"key".to_vec());
    mt.put(b"key".to_vec(), b"second".to_vec());

    assert_eq!(mt.get(b"key"), Some(b"second".as_slice()));
}

// =============================================================================
// Test 5: Delete on non-existent key still works
// =============================================================================
#[test]
fn delete_nonexistent_key_succeeds() {
    let mut mt = MemTable::new(1024 * 1024);
    // Should not panic — tombstone is written even for non-existent key
    mt.delete(b"never_existed".to_vec());

    // And get should return None
    assert_eq!(mt.get(b"never_existed"), None);
}

// =============================================================================
// Test 6: is_full returns false when under limit
// =============================================================================
#[test]
fn is_full_false_when_under_limit() {
    let mt = MemTable::new(1024 * 1024); // 1MB limit
    assert!(!mt.is_full());
}

// =============================================================================
// Test 7: is_full returns true when at or over limit
// =============================================================================
#[test]
fn is_full_true_when_over_limit() {
    let mut mt = MemTable::new(100); // tiny 100 byte limit

    // Insert enough data to exceed limit
    mt.put(b"key1".to_vec(), b"a]value that is pretty long".to_vec());
    mt.put(b"key2".to_vec(), b"another long value here".to_vec());
    mt.put(b"key3".to_vec(), b"and yet another one".to_vec());

    assert!(mt.is_full());
}

// =============================================================================
// Test 8: Iterator includes tombstones
// =============================================================================
// This is important: when flushing to SSTable, we MUST include tombstones
// so they propagate to disk and block old values.
#[test]
fn iterator_includes_tombstones() {
    let mut mt = MemTable::new(1024 * 1024);
    mt.put(b"a".to_vec(), b"value_a".to_vec());
    mt.put(b"b".to_vec(), b"value_b".to_vec());
    mt.delete(b"b".to_vec());  // tombstone for b
    mt.put(b"c".to_vec(), b"value_c".to_vec());

    let mut iter = mt.iter();
    let mut keys = Vec::new();

    while iter.is_valid() {
        keys.push(iter.key().to_vec());
        iter.next().unwrap();
    }

    // Should see all 3 keys: a, b (tombstone), c
    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0], b"a");
    assert_eq!(keys[1], b"b");  // tombstone entry still present
    assert_eq!(keys[2], b"c");
}

// =============================================================================
// Test 9: size returns current memory usage
// =============================================================================
#[test]
fn size_tracks_memory_usage() {
    let mut mt = MemTable::new(1024 * 1024);
    assert_eq!(mt.size(), 0);

    mt.put(b"key".to_vec(), b"value".to_vec());
    assert!(mt.size() > 0);
}
