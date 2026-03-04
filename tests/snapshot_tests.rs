// Snapshot tests (scaffold)
//
// These tests exercise M26: Snapshot Reads. They are currently marked
// `#[ignore]` because the DB snapshot APIs and recovery/pinning logic
// are not fully implemented yet (see TODOs in src/db and src/db/snapshot.rs).
// Once M26 (and related M27/M28) are implemented, remove `#[ignore]` to
// run them as part of the CI.

use tempfile::tempdir;

use lsm_engine::iterator::StorageIterator;
use lsm_engine::{DB, Options};

// Helper: open a temporary DB. When the DB API is implemented this
// will create files under the tempdir and return a handle.
fn open_temp_db() -> (tempfile::TempDir, DB) {
    let dir = tempdir().expect("create temp dir");
    // Options::default() is currently a todo!() placeholder. Running
    // these tests requires implementing Options::default() & DB::open().
    let opts = Options::default();
    let db = DB::open(dir.path(), opts).expect("open db");
    (dir, db)
}

// ---------------------------------------------------------------------------
// High priority snapshot tests (scaffolded)
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn snapshot_sees_old_values_after_writes() {
    let (_dir, db) = open_temp_db();

    let k = b"key1";
    db.put(k, b"v1").unwrap();

    let snap = db.snapshot();

    // Newer write should not be visible through the snapshot.
    db.put(k, b"v2").unwrap();

    let snap_val = snap.get(k).unwrap();
    assert_eq!(snap_val, Some(b"v1".to_vec()));

    let live_val = db.get(k).unwrap();
    assert_eq!(live_val, Some(b"v2".to_vec()));
}

#[test]
#[ignore]
fn snapshot_sees_old_values_after_delete() {
    let (_dir, db) = open_temp_db();

    let k = b"key2";
    db.put(k, b"v1").unwrap();

    let snap = db.snapshot();

    db.delete(k).unwrap();

    let snap_val = snap.get(k).unwrap();
    assert_eq!(snap_val, Some(b"v1".to_vec()));

    let live_val = db.get(k).unwrap();
    assert_eq!(live_val, None);
}

#[test]
#[ignore]
fn snapshot_isolated_from_compaction() {
    let (_dir, db) = open_temp_db();

    // Insert many keys and flush so SSTables exist.
    for i in 0..100u8 {
        let k = format!("k{:03}", i).into_bytes();
        db.put(&k, b"v").unwrap();
    }

    // Force a flush (requires DB::flush implementation).
    db.flush().unwrap();

    let snap = db.snapshot();

    // Trigger compaction which might delete old SSTables.
    db.compact_range(None, None).unwrap();

    // Snapshot scans must still return data from the pre-compaction view.
    let start = b"k000";
    let end = b"k255";
    let it = snap.scan(start, end).unwrap();
    // If implementation returns a Scanner that implements iterator-like
    // behavior via StorageIterator, advance and assert it's valid.
    assert!(it.is_valid());
}

#[test]
#[ignore]
fn multiple_snapshots_different_views() {
    let (_dir, db) = open_temp_db();

    let k = b"multi";
    db.put(k, b"v1").unwrap();

    let s1 = db.snapshot();

    db.put(k, b"v2").unwrap();

    let s2 = db.snapshot();

    assert_eq!(s1.get(k).unwrap(), Some(b"v1".to_vec()));
    assert_eq!(s2.get(k).unwrap(), Some(b"v2".to_vec()));
}

#[test]
#[ignore]
#[allow(clippy::drop_non_drop)]
fn snapshot_release_allows_cleanup() {
    let (_dir, db) = open_temp_db();

    // Put and flush to create SSTables on disk.
    db.put(b"x", b"v").unwrap();
    db.flush().unwrap();

    let snap = db.snapshot();

    // Trigger compaction — snapshot should pin SSTable files.
    db.compact_range(None, None).unwrap();

    // Check SSTable files still exist while snapshot alive.
    let files_before: Vec<_> = std::fs::read_dir(_dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert!(!files_before.is_empty());

    // Drop snapshot and run compaction again. Old files should become
    // eligible for deletion (behavior depends on implementation details).
    drop(snap);
    db.compact_range(None, None).unwrap();

    let files_after: Vec<_> = std::fs::read_dir(_dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();

    // Implementation-specific: the test asserts that files_after is <= files_before.
    assert!(files_after.len() <= files_before.len());
}

#[test]
#[ignore]
fn snapshot_on_empty_db() {
    let (_dir, db) = open_temp_db();

    let snap = db.snapshot();
    assert_eq!(snap.get(b"missing").unwrap(), None);

    let scanner = snap.scan(b"a", b"z").unwrap();
    assert!(!scanner.is_valid());
}
