// M26: Snapshot Scan Tests
//
// Verifies that Snapshot::scan() merges data from memtable + SSTables,
// filters tombstones, respects range bounds, and is isolated from
// writes that happen after the snapshot was taken.

use lsm_engine::iterator::StorageIterator;
use lsm_engine::{DB, Options};
use tempfile::tempdir;

fn open_test_db() -> (tempfile::TempDir, DB) {
    let dir = tempdir().unwrap();
    let opts = Options {
        memtable_size: 64 * 1024,
        ..Options::default()
    };
    let db = DB::open(dir.path(), opts).unwrap();
    (dir, db)
}

/// Collect all (key, value) pairs from a Scanner.
fn collect_scan(scanner: &mut lsm_engine::db::snapshot::Scanner) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut result = Vec::new();
    while scanner.is_valid() {
        result.push((scanner.key().to_vec(), scanner.value().to_vec()));
        scanner.next().unwrap();
    }
    result
}

// =============================================================================
// Test 1: Scan returns keys in the given range [start, end)
// =============================================================================
#[test]
fn snapshot_scan_returns_keys_in_range() {
    let (_dir, db) = open_test_db();

    // Insert keys a through f
    for c in b'a'..=b'f' {
        let key = vec![c];
        let val = format!("val_{}", c as char).into_bytes();
        db.put(&key, &val).unwrap();
    }
    db.flush().unwrap();

    let snap = db.snapshot();
    let mut scanner = snap.scan(b"b", b"e").unwrap();
    let entries = collect_scan(&mut scanner);

    let keys: Vec<&[u8]> = entries.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(
        keys,
        vec![b"b", b"c", b"d"],
        "range [b, e) should yield b, c, d"
    );
}

// =============================================================================
// Test 2: Scan filters tombstones (deleted keys not yielded)
// =============================================================================
#[test]
fn snapshot_scan_filters_tombstones() {
    let (_dir, db) = open_test_db();

    db.put(b"k1", b"v1").unwrap();
    db.put(b"k2", b"v2").unwrap();
    db.put(b"k3", b"v3").unwrap();
    db.delete(b"k2").unwrap();
    db.flush().unwrap();

    let snap = db.snapshot();
    let mut scanner = snap.scan(b"k1", b"k4").unwrap();
    let entries = collect_scan(&mut scanner);

    let keys: Vec<&[u8]> = entries.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(
        keys,
        vec![b"k1", b"k3"],
        "k2 should be filtered (tombstone)"
    );
}

// =============================================================================
// Test 3: Scan on empty range returns invalid Scanner
// =============================================================================
#[test]
fn snapshot_scan_empty_range_invalid() {
    let (_dir, db) = open_test_db();

    db.put(b"a", b"1").unwrap();
    db.put(b"z", b"2").unwrap();
    db.flush().unwrap();

    let snap = db.snapshot();
    // Range [m, n) has no keys
    let scanner = snap.scan(b"m", b"n").unwrap();
    assert!(!scanner.is_valid(), "no keys in range → invalid");
}

// =============================================================================
// Test 4: Scan includes data still in memtable (not yet flushed)
// =============================================================================
#[test]
fn snapshot_scan_includes_memtable_data() {
    let (_dir, db) = open_test_db();

    // These are in the memtable only (no flush)
    db.put(b"mem_a", b"val_a").unwrap();
    db.put(b"mem_b", b"val_b").unwrap();

    let snap = db.snapshot();
    let mut scanner = snap.scan(b"mem_a", b"mem_c").unwrap();
    let entries = collect_scan(&mut scanner);

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, b"mem_a");
    assert_eq!(entries[1].0, b"mem_b");
}

// =============================================================================
// Test 5: Scan merges memtable data with SSTable data
// =============================================================================
#[test]
fn snapshot_scan_merges_memtable_and_sstable() {
    let (_dir, db) = open_test_db();

    // Flush some data to SSTable
    db.put(b"b", b"flushed_b").unwrap();
    db.put(b"d", b"flushed_d").unwrap();
    db.flush().unwrap();

    // Write more to memtable (not flushed)
    db.put(b"a", b"mem_a").unwrap();
    db.put(b"c", b"mem_c").unwrap();

    let snap = db.snapshot();
    let mut scanner = snap.scan(b"a", b"e").unwrap();
    let entries = collect_scan(&mut scanner);

    let keys: Vec<&[u8]> = entries.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(
        keys,
        vec![b"a", b"b", b"c", b"d"],
        "merge of memtable + SSTable"
    );
}

// =============================================================================
// Test 6: Scan deduplicates keys across memtable and SSTable
// =============================================================================
#[test]
fn snapshot_scan_deduplicates_across_sources() {
    let (_dir, db) = open_test_db();

    // Write and flush old value
    db.put(b"dup", b"old_value").unwrap();
    db.flush().unwrap();

    // Write new value to memtable (not flushed)
    db.put(b"dup", b"new_value").unwrap();

    let snap = db.snapshot();
    let mut scanner = snap.scan(b"dup", b"duq").unwrap();
    let entries = collect_scan(&mut scanner);

    assert_eq!(entries.len(), 1, "should deduplicate");
    assert_eq!(entries[0].1, b"new_value", "memtable (newer) wins");
}

// =============================================================================
// Test 7: Snapshot is isolated from writes that happen after it was taken
// =============================================================================
#[test]
fn snapshot_isolated_from_new_writes() {
    let (_dir, db) = open_test_db();

    db.put(b"x", b"before_snap").unwrap();

    let snap = db.snapshot();

    // These writes happen AFTER the snapshot
    db.put(b"x", b"after_snap").unwrap();
    db.put(b"y", b"new_key").unwrap();

    // Snapshot should see the old value
    let snap_val = snap.get(b"x").unwrap();
    assert_eq!(snap_val, Some(b"before_snap".to_vec()));

    // Snapshot scan should not include "y" or the updated "x"
    let mut scanner = snap.scan(b"x", b"z").unwrap();
    let entries = collect_scan(&mut scanner);

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, b"x");
    assert_eq!(entries[0].1, b"before_snap", "snapshot sees pre-snap value");
}
