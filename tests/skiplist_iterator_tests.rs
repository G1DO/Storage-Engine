// M02: Skip List Iterator tests
// Tests for iterating over skip list entries in sorted order.

use lsm_engine::memtable::skiplist::SkipList;
use lsm_engine::iterator::StorageIterator;

// =============================================================================
// Test 1: Empty iterator
// =============================================================================
// An iterator over an empty skip list should immediately be invalid.
// is_valid() returns false, and we shouldn't call key()/value() on it.
#[test]
fn iterator_empty_skiplist_is_invalid() {
    let sl = SkipList::new();
    let iter = sl.iter();
    assert!(!iter.is_valid());
}

// =============================================================================
// Test 2: Single entry iteration
// =============================================================================
// Insert one entry, iterate. Should see that entry, then become invalid.
#[test]
fn iterator_single_entry() {
    let mut sl = SkipList::new();
    sl.insert(b"key".to_vec(), b"value".to_vec());

    let mut iter = sl.iter();

    // Should start at the first (and only) entry
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"key");
    assert_eq!(iter.value(), b"value");

    // Move to next — should become invalid (no more entries)
    iter.next().unwrap();
    assert!(!iter.is_valid());
}

// =============================================================================
// Test 3: Multiple entries come out sorted
// =============================================================================
// Insert entries out of order. Iterator should return them in sorted order.
// This is the core guarantee of the skip list iterator.
#[test]
fn iterator_returns_sorted_order() {
    let mut sl = SkipList::new();
    // Insert out of order
    sl.insert(b"charlie".to_vec(), b"3".to_vec());
    sl.insert(b"alpha".to_vec(), b"1".to_vec());
    sl.insert(b"bravo".to_vec(), b"2".to_vec());

    let mut iter = sl.iter();

    // Should come out: alpha, bravo, charlie
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"alpha");
    assert_eq!(iter.value(), b"1");

    iter.next().unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"bravo");
    assert_eq!(iter.value(), b"2");

    iter.next().unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"charlie");
    assert_eq!(iter.value(), b"3");

    iter.next().unwrap();
    assert!(!iter.is_valid());
}

// =============================================================================
// Test 4: Collect all entries
// =============================================================================
// Insert many entries, collect them all via iteration, verify count and order.
#[test]
fn iterator_collect_all_entries() {
    let mut sl = SkipList::new();
    for i in (0..100u32).rev() {
        // Insert in reverse order to test sorting
        let key = format!("key_{:03}", i).into_bytes();
        let val = format!("val_{}", i).into_bytes();
        sl.insert(key, val);
    }

    let mut iter = sl.iter();
    let mut collected = Vec::new();

    while iter.is_valid() {
        collected.push((iter.key().to_vec(), iter.value().to_vec()));
        iter.next().unwrap();
    }

    assert_eq!(collected.len(), 100);

    // Verify sorted order
    for i in 0..100u32 {
        let expected_key = format!("key_{:03}", i).into_bytes();
        let expected_val = format!("val_{}", i).into_bytes();
        assert_eq!(collected[i as usize].0, expected_key);
        assert_eq!(collected[i as usize].1, expected_val);
    }
}

// =============================================================================
// Test 5: Seek to existing key
// =============================================================================
// seek(key) positions iterator at the first entry with key >= target.
// If the exact key exists, we land on it.
#[test]
fn iterator_seek_to_existing_key() {
    let mut sl = SkipList::new();
    sl.insert(b"a".to_vec(), b"1".to_vec());
    sl.insert(b"c".to_vec(), b"3".to_vec());
    sl.insert(b"e".to_vec(), b"5".to_vec());

    let mut iter = sl.iter();
    iter.seek(b"c").unwrap();

    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"c");
    assert_eq!(iter.value(), b"3");
}

// =============================================================================
// Test 6: Seek to non-existing key (lands on next)
// =============================================================================
// seek(key) for a key that doesn't exist lands on the first key > target.
#[test]
fn iterator_seek_to_nonexistent_key() {
    let mut sl = SkipList::new();
    sl.insert(b"a".to_vec(), b"1".to_vec());
    sl.insert(b"c".to_vec(), b"3".to_vec());
    sl.insert(b"e".to_vec(), b"5".to_vec());

    let mut iter = sl.iter();
    iter.seek(b"b").unwrap();  // "b" doesn't exist, should land on "c"

    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"c");
}

// =============================================================================
// Test 7: Seek past all entries
// =============================================================================
// seek(key) for a key greater than all entries makes iterator invalid.
#[test]
fn iterator_seek_past_end() {
    let mut sl = SkipList::new();
    sl.insert(b"a".to_vec(), b"1".to_vec());
    sl.insert(b"b".to_vec(), b"2".to_vec());

    let mut iter = sl.iter();
    iter.seek(b"z").unwrap();  // "z" > all keys

    assert!(!iter.is_valid());
}

// =============================================================================
// Test 8: Seek to beginning
// =============================================================================
// seek(key) with a key smaller than all entries lands on the first entry.
#[test]
fn iterator_seek_to_beginning() {
    let mut sl = SkipList::new();
    sl.insert(b"b".to_vec(), b"2".to_vec());
    sl.insert(b"c".to_vec(), b"3".to_vec());

    let mut iter = sl.iter();
    iter.seek(b"a").unwrap();  // "a" < all keys, lands on first

    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"b");
}
