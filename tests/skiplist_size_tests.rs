// M03: Skip List Size Tracking tests
// Tests for approximate memory usage tracking.

use lsm_engine::memtable::skiplist::SkipList;

// =============================================================================
// Test 1: Empty skip list size
// =============================================================================
// An empty skip list should have size_bytes == 0.
#[test]
fn empty_skiplist_size_is_zero() {
    let sl = SkipList::new();
    assert_eq!(sl.size_bytes(), 0);
}

// =============================================================================
// Test 2: Size increases after insert
// =============================================================================
// After inserting a key-value pair, size should increase by at least
// key.len() + value.len().
#[test]
fn size_increases_after_insert() {
    let mut sl = SkipList::new();
    let key = b"hello".to_vec();      // 5 bytes
    let value = b"world".to_vec();    // 5 bytes

    sl.insert(key.clone(), value.clone());

    // Size should be at least key + value bytes
    assert!(sl.size_bytes() >= key.len() + value.len());
}

// =============================================================================
// Test 3: Size increases on overwrite
// =============================================================================
// When overwriting a key with a larger value, size should increase.
#[test]
fn size_increases_on_overwrite() {
    let mut sl = SkipList::new();
    sl.insert(b"key".to_vec(), b"small".to_vec());
    let size_after_first = sl.size_bytes();

    sl.insert(b"key".to_vec(), b"much larger value".to_vec());
    let size_after_overwrite = sl.size_bytes();

    assert!(size_after_overwrite > size_after_first);
}

// =============================================================================
// Test 4: Size is monotonically non-decreasing
// =============================================================================
// Size should never decrease, even when overwriting with smaller values.
#[test]
fn size_is_monotonically_nondecreasing() {
    let mut sl = SkipList::new();

    sl.insert(b"key".to_vec(), b"large value here".to_vec());
    let size1 = sl.size_bytes();

    // Overwrite with smaller value
    sl.insert(b"key".to_vec(), b"tiny".to_vec());
    let size2 = sl.size_bytes();

    // Size should not decrease
    assert!(size2 >= size1);
}

// =============================================================================
// Test 5: Multiple inserts accumulate size
// =============================================================================
// Inserting multiple keys should accumulate size.
#[test]
fn multiple_inserts_accumulate_size() {
    let mut sl = SkipList::new();

    let entries = vec![
        (b"key1".to_vec(), b"value1".to_vec()),
        (b"key2".to_vec(), b"value2".to_vec()),
        (b"key3".to_vec(), b"value3".to_vec()),
    ];

    let total_data_size: usize = entries.iter()
        .map(|(k, v)| k.len() + v.len())
        .sum();

    for (k, v) in entries {
        sl.insert(k, v);
    }

    // Size should be at least the sum of all key+value bytes
    assert!(sl.size_bytes() >= total_data_size);
}
