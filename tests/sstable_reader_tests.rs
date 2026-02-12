// M14: SSTable Reader tests
// Tests for opening SSTables and point lookups.

use lsm_engine::sstable::builder::SSTableBuilder;
use lsm_engine::sstable::reader::SSTable;
use std::fs;
use tempfile::tempdir;

// =============================================================================
// Test 1: Write 1000 entries, open with reader, get all → correct values
// =============================================================================
#[test]
fn read_1000_entries_back() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    // Build SSTable with 1000 entries
    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    for i in 0..1000u32 {
        let key = format!("key_{:05}", i);
        let val = format!("val_{:05}", i);
        builder.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    builder.finish().unwrap();

    // Open and verify all entries
    let sstable = SSTable::open(&path).unwrap();
    for i in 0..1000u32 {
        let key = format!("key_{:05}", i);
        let expected_val = format!("val_{:05}", i);
        let result = sstable.get(key.as_bytes()).unwrap();
        assert_eq!(
            result,
            Some(expected_val.into_bytes()),
            "Failed for key {}",
            key
        );
    }
}

// =============================================================================
// Test 2: Get non-existing key → None
// =============================================================================
#[test]
fn get_nonexistent_key_returns_none() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"aaa", b"value_aaa").unwrap();
    builder.add(b"ccc", b"value_ccc").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();

    // Key between existing keys
    assert_eq!(sstable.get(b"bbb").unwrap(), None);
    // Key before all keys
    assert_eq!(sstable.get(b"___").unwrap(), None);
    // Key after all keys
    assert_eq!(sstable.get(b"zzz").unwrap(), None);
}

// =============================================================================
// Test 3: Get key smaller than min_key → None
// =============================================================================
#[test]
fn get_key_smaller_than_min() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"middle", b"value").unwrap();
    builder.add(b"zebra", b"value").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    assert_eq!(sstable.meta().min_key, b"middle");
    assert_eq!(sstable.get(b"apple").unwrap(), None);
}

// =============================================================================
// Test 4: Get key larger than max_key → None
// =============================================================================
#[test]
fn get_key_larger_than_max() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"apple", b"value").unwrap();
    builder.add(b"middle", b"value").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    assert_eq!(sstable.meta().max_key, b"middle");
    assert_eq!(sstable.get(b"zebra").unwrap(), None);
}

// =============================================================================
// Test 5: Get key between two entries → None
// =============================================================================
#[test]
fn get_key_between_entries() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"aaa", b"first").unwrap();
    builder.add(b"ccc", b"third").unwrap();
    builder.add(b"eee", b"fifth").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    // Key in gap between existing keys
    assert_eq!(sstable.get(b"bbb").unwrap(), None);
    assert_eq!(sstable.get(b"ddd").unwrap(), None);
}

// =============================================================================
// Test 6: Open non-existent file → error
// =============================================================================
#[test]
fn open_nonexistent_file_fails() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("does_not_exist.sst");

    let result = SSTable::open(&path);
    assert!(result.is_err());
}

// =============================================================================
// Test 7: Open corrupted file (bad magic) → error
// =============================================================================
#[test]
fn open_corrupted_file_bad_magic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("corrupted.sst");

    // Create a file with garbage data
    fs::write(&path, b"this is not a valid sstable file").unwrap();

    let result = SSTable::open(&path);
    assert!(result.is_err());
}

// =============================================================================
// Test 8: Meta() returns correct metadata
// =============================================================================
#[test]
fn meta_returns_correct_info() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 42, 4096).unwrap();
    builder.add(b"alpha", b"first").unwrap();
    builder.add(b"beta", b"second").unwrap();
    builder.add(b"gamma", b"third").unwrap();
    let expected_meta = builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    let meta = sstable.meta();

    assert_eq!(meta.id, expected_meta.id);
    assert_eq!(meta.level, expected_meta.level);
    assert_eq!(meta.min_key, expected_meta.min_key);
    assert_eq!(meta.max_key, expected_meta.max_key);
    assert_eq!(meta.entry_count, expected_meta.entry_count);
}

// =============================================================================
// Test 9: Multiple blocks - verify index search works correctly
// =============================================================================
#[test]
fn multiple_blocks_index_search() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    // Use tiny block size to force multiple blocks
    let mut builder = SSTableBuilder::new(&path, 1, 128).unwrap();

    // Add enough entries to span multiple blocks
    for i in 0..100u32 {
        let key = format!("key_{:05}", i);
        let val = format!("value_{:05}", i);
        builder.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();

    // Spot check: first, middle, last entries
    assert_eq!(
        sstable.get(b"key_00000").unwrap(),
        Some(b"value_00000".to_vec())
    );
    assert_eq!(
        sstable.get(b"key_00050").unwrap(),
        Some(b"value_00050".to_vec())
    );
    assert_eq!(
        sstable.get(b"key_00099").unwrap(),
        Some(b"value_00099".to_vec())
    );
}

// =============================================================================
// Test 10: Empty value
// =============================================================================
#[test]
fn empty_value_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"key_with_empty_value", b"").unwrap();
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();
    assert_eq!(sstable.get(b"key_with_empty_value").unwrap(), Some(vec![]));
}
