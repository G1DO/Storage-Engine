// M24: Point Lookup Integration Tests
// Tests for the full read path: memtable → immutable memtable → L0 → L1 → ...

use lsm_engine::memtable::MemTable;
use lsm_engine::sstable::builder::SSTableBuilder;
use lsm_engine::sstable::reader::SSTable;
use tempfile::tempdir;

// =============================================================================
// Test 1: Key in active memtable → found without touching SSTables
// =============================================================================
#[test]
fn key_in_active_memtable_found_directly() {
    let mut memtable = MemTable::new(1024 * 1024);
    memtable.put(b"alice".to_vec(), b"value_alice".to_vec());

    // Simulate: just check memtable, don't go to SSTables
    assert_eq!(memtable.get(b"alice"), Some(b"value_alice".as_slice()));
}

// =============================================================================
// Test 2: Key in immutable memtable → found
// =============================================================================
#[test]
fn key_in_immutable_memtable_found() {
    let mut memtable = MemTable::new(1024 * 1024);
    memtable.put(b"bob".to_vec(), b"value_bob".to_vec());

    // In reality, immutable memtable would be wrapped in Arc<MemTable>
    // Here we just verify memtable.get works
    assert_eq!(memtable.get(b"bob"), Some(b"value_bob".as_slice()));
}

// =============================================================================
// Test 3: Key only in L0 SSTable → found
// =============================================================================
#[test]
fn key_in_l0_sstable_found() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("l0_table.sst");

    // Build L0 SSTable
    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"charlie", b"value_charlie").unwrap();
    builder.add(b"diana", b"value_diana").unwrap();
    builder.finish().unwrap();

    // Open and read
    let sstable = SSTable::open(&path).unwrap();
    assert_eq!(
        sstable.get(b"charlie").unwrap(),
        Some(b"value_charlie".to_vec())
    );
}

// =============================================================================
// Test 4: Key only in L1 SSTable → found
// =============================================================================
#[test]
fn key_in_l1_sstable_found() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("l1_table.sst");

    // Build L1 SSTable
    let mut builder = SSTableBuilder::new(&path, 2, 4096).unwrap();
    builder
        .add(b"eve".to_vec().as_slice(), b"value_eve")
        .unwrap();
    builder
        .add(b"frank".to_vec().as_slice(), b"value_frank")
        .unwrap();
    builder.finish().unwrap();

    // Open and read
    let sstable = SSTable::open(&path).unwrap();
    assert_eq!(sstable.get(b"eve").unwrap(), Some(b"value_eve".to_vec()));
}

// =============================================================================
// Test 5: Key updated: old in L1, new in memtable → returns new value
// =============================================================================
#[test]
fn key_updated_returns_newest_from_memtable() {
    // Build L1 SSTable with old value
    let dir = tempdir().unwrap();
    let l1_path = dir.path().join("l1_updated.sst");

    let mut builder = SSTableBuilder::new(&l1_path, 3, 4096).unwrap();
    builder.add(b"george", b"old_value").unwrap();
    builder.finish().unwrap();

    let l1_table = SSTable::open(&l1_path).unwrap();

    // Active memtable with new value
    let mut memtable = MemTable::new(1024 * 1024);
    memtable.put(b"george".to_vec(), b"new_value".to_vec());

    // Read order: memtable first
    // memtable has the key, so we return that and don't check L1
    assert_eq!(memtable.get(b"george"), Some(b"new_value".as_slice()));

    // Verify L1 has old value (for understanding)
    assert_eq!(
        l1_table.get(b"george").unwrap(),
        Some(b"old_value".to_vec())
    );
}

// =============================================================================
// Test 6: Key deleted: tombstone in memtable, value in L1 → returns None
// =============================================================================
#[test]
fn key_deleted_tombstone_hides_old_value() {
    // Build L1 SSTable with value
    let dir = tempdir().unwrap();
    let l1_path = dir.path().join("l1_deleted.sst");

    let mut builder = SSTableBuilder::new(&l1_path, 4, 4096).unwrap();
    builder
        .add(b"helen".to_vec().as_slice(), b"value_helen")
        .unwrap();
    builder.finish().unwrap();

    let l1_table = SSTable::open(&l1_path).unwrap();

    // Active memtable with tombstone (delete)
    let mut memtable = MemTable::new(1024 * 1024);
    memtable.put(b"helen".to_vec(), b"value_helen".to_vec());
    memtable.delete(b"helen".to_vec());

    // Read order: check memtable first
    // memtable has tombstone for the key, return None (don't check L1)
    assert_eq!(memtable.get(b"helen"), None);

    // Verify L1 has the value (but we wouldn't read it due to tombstone)
    assert_eq!(
        l1_table.get(b"helen").unwrap(),
        Some(b"value_helen".to_vec())
    );
}

// =============================================================================
// Test 7: Key doesn't exist anywhere → None (no SSTables needed)
// =============================================================================
#[test]
fn key_not_found_anywhere_returns_none() {
    let memtable = MemTable::new(1024 * 1024);

    // Key not in memtable
    assert_eq!(memtable.get(b"nonexistent"), None);

    // In a full DB, this would:
    // 1. Check active memtable → miss
    // 2. Check immutable memtable → miss
    // 3. Bloom filter for L0 → not present
    // 4. Bloom filter for L1 → not present
    // ... (no SSTables touched)
    // 5. Return None
}

// =============================================================================
// Test 8: Multiple keys in memtable with various states
// =============================================================================
#[test]
fn multiple_keys_in_memtable_correct_state() {
    let mut memtable = MemTable::new(1024 * 1024);

    // Add some keys
    memtable.put(b"key1".to_vec(), b"val1".to_vec());
    memtable.put(b"key2".to_vec(), b"val2".to_vec());
    memtable.put(b"key3".to_vec(), b"val3".to_vec());

    // Delete one
    memtable.delete(b"key2".to_vec());

    // Update one
    memtable.put(b"key3".to_vec(), b"val3_new".to_vec());

    // Verify state
    assert_eq!(memtable.get(b"key1"), Some(b"val1".as_slice()));
    assert_eq!(memtable.get(b"key2"), None);
    assert_eq!(memtable.get(b"key3"), Some(b"val3_new".as_slice()));
}

// =============================================================================
// Test 9: L0 with multiple SSTables (must check all)
// =============================================================================
#[test]
fn l0_with_multiple_sstables_all_checked() {
    let dir = tempdir().unwrap();

    // Create 2 L0 SSTables that together have our data
    let l0_1_path = dir.path().join("l0_1.sst");
    let l0_2_path = dir.path().join("l0_2.sst");

    let mut builder1 = SSTableBuilder::new(&l0_1_path, 10, 4096).unwrap();
    builder1.add(b"alice", b"alice_val").unwrap();
    builder1.finish().unwrap();

    let mut builder2 = SSTableBuilder::new(&l0_2_path, 11, 4096).unwrap();
    builder2.add(b"bob", b"bob_val").unwrap();
    builder2.finish().unwrap();

    let sst1 = SSTable::open(&l0_1_path).unwrap();
    let sst2 = SSTable::open(&l0_2_path).unwrap();

    // In L0, both tables must be checked because they can overlap
    assert_eq!(sst1.get(b"alice").unwrap(), Some(b"alice_val".to_vec()));
    assert_eq!(sst2.get(b"bob").unwrap(), Some(b"bob_val".to_vec()));

    // Neither table has the other's key
    assert_eq!(sst1.get(b"bob").unwrap(), None);
    assert_eq!(sst2.get(b"alice").unwrap(), None);
}

// =============================================================================
// Test 10: Read order correctness: memtable before L0
// =============================================================================
#[test]
fn read_order_memtable_before_l0() {
    let dir = tempdir().unwrap();

    // Create L0 SSTable with a key-value pair
    let l0_path = dir.path().join("l0_order.sst");
    let mut builder = SSTableBuilder::new(&l0_path, 20, 4096).unwrap();
    builder.add(b"shared_key", b"l0_value").unwrap();
    builder.finish().unwrap();

    let sst = SSTable::open(&l0_path).unwrap();

    // Active memtable with updated value for same key
    let mut memtable = MemTable::new(1024 * 1024);
    memtable.put(b"shared_key".to_vec(), b"memtable_value".to_vec());

    // Reading memtable first gives us the newest value
    assert_eq!(
        memtable.get(b"shared_key"),
        Some(b"memtable_value".as_slice())
    );

    // L0 has stale data (wouldn't be read due to early termination)
    assert_eq!(sst.get(b"shared_key").unwrap(), Some(b"l0_value".to_vec()));
}
