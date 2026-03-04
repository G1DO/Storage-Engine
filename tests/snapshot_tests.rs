use lsm_engine::db::{DB, Options};
use lsm_engine::error::Result;
use lsm_engine::iterator::StorageIterator;
use tempfile::TempDir;

/// Helper to create a temporary DB for testing.
fn create_test_db() -> Result<(DB, TempDir)> {
    let temp_dir = TempDir::new().unwrap();
    let db = DB::open(temp_dir.path(), Options::default())?;
    Ok((db, temp_dir))
}

/// Test: Snapshot isolation from writes.
///
/// Setup:
///   - Create DB, put (a=1, b=2)
///   - Take snapshot
///   - Write new data: a=999
///
/// Expect:
///   - snapshot.get(a) → 1 (old value)
///   - db.get(a) → 999 (new value)
#[test]
#[ignore = "M26: snapshot not yet implemented"]
fn snapshot_isolation_from_writes() -> Result<()> {
    let (db, _temp) = create_test_db()?;

    // Write initial data
    db.put(b"a", b"1")?;
    db.put(b"b", b"2")?;

    // Take snapshot
    let snapshot = db.snapshot();

    // New write in active DB
    db.put(b"a", b"999")?;

    // Snapshot should see old value
    assert_eq!(snapshot.get(b"a")?, Some(b"1".to_vec()));
    // DB should see new value
    assert_eq!(db.get(b"a")?, Some(b"999".to_vec()));

    // Snapshot should still see b=2
    assert_eq!(snapshot.get(b"b")?, Some(b"2".to_vec()));

    Ok(())
}

/// Test: Snapshot isolation from deletes.
///
/// Setup:
///   - Create DB, put (k=v)
///   - Take snapshot
///   - Delete key
///
/// Expect:
///   - snapshot.get(k) → Some(v) (still present)
///   - db.get(k) → None (deleted)
#[test]
#[ignore = "M26: snapshot not yet implemented"]
fn snapshot_isolation_from_deletes() -> Result<()> {
    let (db, _temp) = create_test_db()?;

    // Write initial data
    db.put(b"mykey", b"myvalue")?;

    // Take snapshot
    let snapshot = db.snapshot();

    // Delete the key
    db.delete(b"mykey")?;

    // Snapshot should still see the value
    assert_eq!(snapshot.get(b"mykey")?, Some(b"myvalue".to_vec()));
    // Active DB should see None (deleted)
    assert_eq!(db.get(b"mykey")?, None);

    Ok(())
}

/// Test: Snapshot isolation from compaction.
///
/// Setup:
///   - Create DB, put some data
///   - Flush to SSTable
///   - Take snapshot (pointing to that SSTable)
///   - Trigger compaction
///
/// Expect:
///   - Old SSTable file not deleted (snapshot holds Arc reference)
///   - snapshot.get() still works (can still read old SSTable)
#[test]
#[ignore = "M26: snapshot not yet implemented"]
fn snapshot_isolation_from_compaction() -> Result<()> {
    let (db, _temp) = create_test_db()?;

    // Write enough data to trigger flush
    for i in 0..100 {
        let key = format!("key-{:04}", i);
        let val = format!("val-{:04}", i);
        db.put(key.as_bytes(), val.as_bytes())?;
    }

    // Flush to SSTable
    db.flush()?;

    // Take snapshot (pointing to Level 0 SSTable)
    let snapshot = db.snapshot();

    // Write more data to create more SSTables
    for i in 100..200 {
        let key = format!("key-{:04}", i);
        let val = format!("val-{:04}", i);
        db.put(key.as_bytes(), val.as_bytes())?;
    }

    // Trigger compaction
    db.flush()?;
    db.compact_range(None, None)?;

    // Snapshot should still be able to read original data
    assert_eq!(snapshot.get(b"key-0050")?, Some(b"val-0050".to_vec()));

    // New data should be visible too (in newer levels)
    assert_eq!(snapshot.get(b"key-0150")?, Some(b"val-0150".to_vec()));

    Ok(())
}

/// Test: Multiple snapshots at different times see different versions.
///
/// Setup:
///   - Put k=v1
///   - Take snapshot1
///   - Put k=v2
///   - Take snapshot2
///   - Put k=v3
///
/// Expect:
///   - snapshot1.get(k) → v1
///   - snapshot2.get(k) → v2
///   - db.get(k) → v3
#[test]
#[ignore = "M26: snapshot not yet implemented"]
fn multiple_snapshots_different_views() -> Result<()> {
    let (db, _temp) = create_test_db()?;

    // Version 1
    db.put(b"k", b"v1")?;
    let snapshot1 = db.snapshot();

    // Version 2
    db.put(b"k", b"v2")?;
    let snapshot2 = db.snapshot();

    // Version 3
    db.put(b"k", b"v3")?;

    // Each snapshot sees its own version
    assert_eq!(snapshot1.get(b"k")?, Some(b"v1".to_vec()));
    assert_eq!(snapshot2.get(b"k")?, Some(b"v2".to_vec()));
    assert_eq!(db.get(b"k")?, Some(b"v3".to_vec()));

    Ok(())
}

/// Test: Snapshot sees consistent state across multiple keys.
///
/// Setup:
///   - Put a=1, b=2, c=3
///   - Take snapshot
///   - Update all three keys
///
/// Expect:
///   - snapshot sees old values for all three at once
#[test]
#[ignore = "M26: snapshot not yet implemented"]
fn snapshot_consistent_across_keys() -> Result<()> {
    let (db, _temp) = create_test_db()?;

    // Write initial state
    db.put(b"a", b"1")?;
    db.put(b"b", b"2")?;
    db.put(b"c", b"3")?;

    // Snapshot at this point
    let snapshot = db.snapshot();

    // Update all keys
    db.put(b"a", b"100")?;
    db.put(b"b", b"200")?;
    db.put(b"c", b"300")?;

    // Snapshot sees old values consistently
    assert_eq!(snapshot.get(b"a")?, Some(b"1".to_vec()));
    assert_eq!(snapshot.get(b"b")?, Some(b"2".to_vec()));
    assert_eq!(snapshot.get(b"c")?, Some(b"3".to_vec()));

    // DB sees new values
    assert_eq!(db.get(b"a")?, Some(b"100".to_vec()));
    assert_eq!(db.get(b"b")?, Some(b"200".to_vec()));
    assert_eq!(db.get(b"c")?, Some(b"300".to_vec()));

    Ok(())
}

/// Test: Snapshot range scan returns consistent view.
///
/// Setup:
///   - Put a=1, b=2, c=3
///   - Take snapshot
///   - Delete and update keys
///
/// Expect:
///   - snapshot.scan(a, d) returns [a, b, c] with old values
///   - db.scan(a, d) returns updated values
#[test]
#[ignore = "M26: snapshot not yet implemented"]
fn snapshot_range_scan_isolation() -> Result<()> {
    let (db, _temp) = create_test_db()?;

    // Write initial data
    db.put(b"a", b"1")?;
    db.put(b"b", b"2")?;
    db.put(b"c", b"3")?;

    // Take snapshot
    let snapshot = db.snapshot();

    // Modify data
    db.put(b"a", b"1_new")?;
    db.delete(b"b")?;
    db.put(b"c", b"3_new")?;
    db.put(b"d", b"4")?;

    // Snapshot scan should see old values
    let mut snapshot_scan = snapshot.scan(b"a", b"d")?;
    let snapshot_results = collect_scan_results(&mut snapshot_scan)?;

    assert_eq!(snapshot_results.len(), 3); // a, b, c
    assert_eq!(snapshot_results[0], (b"a".to_vec(), b"1".to_vec()));
    assert_eq!(snapshot_results[1], (b"b".to_vec(), b"2".to_vec()));
    assert_eq!(snapshot_results[2], (b"c".to_vec(), b"3".to_vec()));

    // DB scan should see new values (b deleted, a and c updated, d added)
    let mut db_scan = db.scan(b"a", b"d")?;
    let db_results = collect_scan_results(&mut db_scan)?;

    assert_eq!(db_results.len(), 3); // a, c, d (b is deleted)
    assert_eq!(db_results[0], (b"a".to_vec(), b"1_new".to_vec()));
    assert_eq!(db_results[1], (b"c".to_vec(), b"3_new".to_vec()));
    assert_eq!(db_results[2], (b"d".to_vec(), b"4".to_vec()));

    Ok(())
}

/// Test: When snapshot is dropped, old SSTables can be reclaimed.
///
/// This test verifies the Arc reference counting behavior:
///   - Take snapshot (holds Arc reference to old SSTable version)
///   - Trigger compaction (creates new SSTable version, marks old for deletion)
///   - Drop snapshot
///   - Old SSTable should be eligible for cleanup
///
/// NOTE: We can't easily verify "eligible for cleanup" without file system inspection.
/// This test just ensures no panics occur and references are properly dropped.
#[test]
#[ignore = "M26: snapshot not yet implemented"]
fn snapshot_drop_releases_references() -> Result<()> {
    let (db, _temp_dir) = create_test_db()?;

    // Write and flush data
    for i in 0..100 {
        let key = format!("key-{:04}", i);
        let val = format!("val-{:04}", i);
        db.put(key.as_bytes(), val.as_bytes())?;
    }
    db.flush()?;

    // Take snapshot
    let snapshot = db.snapshot();

    // Verify snapshot works
    assert_eq!(snapshot.get(b"key-0050")?, Some(b"val-0050".to_vec()));

    // Drop snapshot explicitly
    let _ = snapshot;

    // File should still exist (we haven't deleted anything yet)
    // but if Arc counts had leaked, this would cause problems later
    // Perform another operation to ensure system is still functional
    db.put(b"new_key", b"new_value")?;
    assert_eq!(db.get(b"new_key")?, Some(b"new_value".to_vec()));

    Ok(())
}

/// Helper: collect all results from a scanner into a Vec.
fn collect_scan_results(
    scanner: &mut lsm_engine::db::snapshot::Scanner,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut results = Vec::new();
    while scanner.is_valid() {
        results.push((scanner.key().to_vec(), scanner.value().to_vec()));
        scanner.next()?;
    }
    Ok(results)
}
