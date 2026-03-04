// M15: SSTable Iterator tests
// Tests for sequential iteration and range scans over SSTables.

use lsm_engine::iterator::StorageIterator;
use lsm_engine::sstable::builder::SSTableBuilder;
use lsm_engine::sstable::reader::SSTable;
use tempfile::tempdir;

// =============================================================================
// Test 1: Iterate entire SSTable → all entries in sorted order
// =============================================================================
#[test]
fn iterate_entire_sstable_sorted_order() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    // Must add in sorted order
    builder.add(b"a", b"val_a").unwrap();
    builder.add(b"b", b"val_b").unwrap();
    builder.add(b"c", b"val_c").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let mut iter = sstable.iter().unwrap();

    let mut keys = Vec::new();
    while iter.is_valid() {
        keys.push(iter.key().to_vec());
        iter.next().unwrap();
    }

    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
}

// =============================================================================
// Test 2: Iterate, collect all keys → sorted
// =============================================================================
#[test]
fn iterate_collect_all_keys_sorted() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    // Must add in sorted order
    for i in 0..100u32 {
        let key = format!("key_{:05}", i);
        let val = format!("val_{:05}", i);
        builder.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let mut iter = sstable.iter().unwrap();

    let mut keys = Vec::new();
    while iter.is_valid() {
        keys.push(String::from_utf8_lossy(iter.key()).to_string());
        iter.next().unwrap();
    }

    // Verify sorted order
    for i in 0..keys.len() - 1 {
        assert!(
            keys[i] < keys[i + 1],
            "Keys not sorted: {} >= {}",
            keys[i],
            keys[i + 1]
        );
    }
    assert_eq!(keys.len(), 100);
}

// =============================================================================
// Test 3: Range [b, e) on SSTable with keys a-z → yields b, c, d
// =============================================================================
#[test]
fn range_iteration_exclusive_end() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    for c in 'a'..='z' {
        let key = c.to_string();
        let val = format!("val_{}", c);
        builder.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let mut iter = sstable.range_iter(b"b", b"e").unwrap();

    let mut keys = Vec::new();
    while iter.is_valid() {
        keys.push(String::from_utf8_lossy(iter.key()).to_string());
        iter.next().unwrap();
    }

    assert_eq!(keys, vec!["b", "c", "d"]); // Exclusive of 'e'
}

// =============================================================================
// Test 4: Range with no matching keys → empty
// =============================================================================
#[test]
fn range_no_matching_keys_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"aaa", b"val").unwrap();
    builder.add(b"bbb", b"val").unwrap();
    builder.add(b"ccc", b"val").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let iter = sstable.range_iter(b"ddd", b"eee").unwrap();

    assert!(!iter.is_valid()); // Immediately invalid
}

// =============================================================================
// Test 5: Range starting before min_key → starts at first entry
// =============================================================================
#[test]
fn range_start_before_min_starts_at_first() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"middle", b"val").unwrap();
    builder.add(b"zebra", b"val").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let iter = sstable.range_iter(b"aaa", b"zzz").unwrap();

    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"middle");
}

// =============================================================================
// Test 6: Range ending after max_key → includes all entries to end
// =============================================================================
#[test]
fn range_end_after_max_includes_to_end() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"alpha", b"val").unwrap();
    builder.add(b"beta", b"val").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let mut iter = sstable.range_iter(b"a", b"zzz").unwrap();

    let mut count = 0;
    while iter.is_valid() {
        count += 1;
        iter.next().unwrap();
    }

    assert_eq!(count, 2); // Both entries included
}

// =============================================================================
// Test 7: Empty SSTable iteration → immediately invalid
// =============================================================================
#[test]
fn empty_sstable_iteration_invalid() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let iter = sstable.iter().unwrap();

    assert!(!iter.is_valid());
}

// =============================================================================
// Test 8: Seek to existing key → positions there
// =============================================================================
#[test]
fn seek_to_existing_key() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"aaa", b"val_a").unwrap();
    builder.add(b"bbb", b"val_b").unwrap();
    builder.add(b"ccc", b"val_c").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let mut iter = sstable.iter().unwrap();

    iter.seek(b"bbb").unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"bbb");
    assert_eq!(iter.value(), b"val_b");
}

// =============================================================================
// Test 9: Seek to non-existing key → positions at next greater key
// =============================================================================
#[test]
fn seek_to_nonexistent_key_positions_next() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"aaa", b"val_a").unwrap();
    builder.add(b"ccc", b"val_c").unwrap();
    builder.add(b"eee", b"val_e").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let mut iter = sstable.iter().unwrap();

    iter.seek(b"bbb").unwrap(); // Between aaa and ccc
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"ccc"); // Should position at ccc
}

// =============================================================================
// Test 10: Seek past all keys → invalid
// =============================================================================
#[test]
fn seek_past_all_keys_invalid() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"aaa", b"val").unwrap();
    builder.add(b"bbb", b"val").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let mut iter = sstable.iter().unwrap();

    iter.seek(b"zzz").unwrap();
    assert!(!iter.is_valid());
}

// =============================================================================
// Test 11: Multiple blocks - iteration crosses block boundaries
// =============================================================================
#[test]
fn iteration_crosses_block_boundaries() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    // Use tiny block size to force multiple blocks
    let mut builder = SSTableBuilder::new(&path, 1, 128).unwrap();

    // Add 100 entries - should span multiple blocks
    for i in 0..100u32 {
        let key = format!("key_{:05}", i);
        let val = format!("value_{:05}", i);
        builder.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let mut iter = sstable.iter().unwrap();

    let mut count = 0;
    let mut last_key = String::new();
    while iter.is_valid() {
        let key = String::from_utf8_lossy(iter.key()).to_string();
        assert!(key > last_key, "Keys not sorted: {} <= {}", key, last_key);
        last_key = key;
        count += 1;
        iter.next().unwrap();
    }

    assert_eq!(count, 100);
}

// =============================================================================
// Test 12: Range iteration with multiple blocks
// =============================================================================
#[test]
fn range_iteration_crosses_block_boundaries() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    // Use tiny block size to force multiple blocks
    let mut builder = SSTableBuilder::new(&path, 1, 128).unwrap();

    for i in 0..100u32 {
        let key = format!("key_{:05}", i);
        let val = format!("value_{:05}", i);
        builder.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let mut iter = sstable.range_iter(b"key_00025", b"key_00075").unwrap();

    let mut count = 0;
    while iter.is_valid() {
        let key = String::from_utf8_lossy(iter.key()).to_string();
        assert!(
            key.as_str() >= "key_00025" && key.as_str() < "key_00075",
            "Key {} out of range",
            key
        );
        count += 1;
        iter.next().unwrap();
    }

    assert_eq!(count, 50); // 25 through 74 inclusive
}

// =============================================================================
// Test 13: Seek to first key
// =============================================================================
#[test]
fn seek_to_first_key() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"aaa", b"val_a").unwrap();
    builder.add(b"bbb", b"val_b").unwrap();
    builder.add(b"ccc", b"val_c").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let mut iter = sstable.iter().unwrap();

    iter.seek(b"aaa").unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"aaa");
}

// =============================================================================
// Test 14: Single entry SSTable iteration
// =============================================================================
#[test]
fn single_entry_sstable_iteration() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"only_key", b"only_value").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let mut iter = sstable.iter().unwrap();

    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"only_key");
    assert_eq!(iter.value(), b"only_value");

    iter.next().unwrap();
    assert!(!iter.is_valid());
}
