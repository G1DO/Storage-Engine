// M32: End-to-end integration tests for the full DB lifecycle.

use lsm_engine::iterator::StorageIterator;
use lsm_engine::{DB, Options};
use tempfile::tempdir;

fn small_db_opts() -> Options {
    Options {
        memtable_size: 64 * 1024,
        ..Options::default()
    }
}

// =============================================================================
// Test 1: Write, flush, compact, read — the full data path
// =============================================================================
#[test]
fn write_flush_compact_read() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_db_opts()).unwrap();

    // Write 200 keys across multiple flushes
    for i in 0..200u32 {
        let key = format!("key_{:05}", i).into_bytes();
        let val = format!("val_{:05}", i).into_bytes();
        db.put(&key, &val).unwrap();
        if (i + 1) % 50 == 0 {
            db.flush().unwrap();
        }
    }

    // Compact everything
    db.compact_range(None, None).unwrap();

    // All keys still readable
    for i in 0..200u32 {
        let key = format!("key_{:05}", i).into_bytes();
        let val = format!("val_{:05}", i).into_bytes();
        assert_eq!(db.get(&key).unwrap(), Some(val));
    }

    db.close().unwrap();
}

// =============================================================================
// Test 2: Delete keys, compact, verify they're gone
// =============================================================================
#[test]
fn delete_compact_verify_gone() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_db_opts()).unwrap();

    for i in 0..50u32 {
        let key = format!("k{:03}", i).into_bytes();
        db.put(&key, b"val").unwrap();
    }
    db.flush().unwrap();

    // Delete even keys
    for i in (0..50u32).step_by(2) {
        let key = format!("k{:03}", i).into_bytes();
        db.delete(&key).unwrap();
    }
    db.flush().unwrap();

    // Verify: even keys gone, odd keys present
    for i in 0..50u32 {
        let key = format!("k{:03}", i).into_bytes();
        if i % 2 == 0 {
            assert_eq!(db.get(&key).unwrap(), None, "k{:03} should be deleted", i);
        } else {
            assert_eq!(
                db.get(&key).unwrap(),
                Some(b"val".to_vec()),
                "k{:03} should exist",
                i
            );
        }
    }

    db.close().unwrap();
}

// =============================================================================
// Test 3: Crash simulation — drop without close, reopen, verify
// =============================================================================
#[test]
fn crash_recovery_preserves_data() {
    let dir = tempdir().unwrap();

    // Write and drop (simulate crash)
    {
        let db = DB::open(dir.path(), small_db_opts()).unwrap();
        db.put(b"survive", b"crash").unwrap();
        db.flush().unwrap();
        db.put(b"wal_only", b"unflushed").unwrap();
        drop(db); // no close() — simulates crash
    }

    // Reopen and verify
    {
        let db = DB::open(dir.path(), small_db_opts()).unwrap();
        assert_eq!(db.get(b"survive").unwrap(), Some(b"crash".to_vec()));
        // WAL-only data should also be recovered
        assert_eq!(db.get(b"wal_only").unwrap(), Some(b"unflushed".to_vec()));
        db.close().unwrap();
    }
}

// =============================================================================
// Test 4: Scan correctness after mixed operations
// =============================================================================
#[test]
fn scan_after_mixed_operations() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_db_opts()).unwrap();

    // Insert, flush, update, delete
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();
    db.flush().unwrap();

    db.put(b"b", b"updated").unwrap(); // update in memtable
    db.delete(b"c").unwrap(); // tombstone in memtable
    db.put(b"d", b"4").unwrap(); // new key in memtable

    let mut scanner = db.scan(b"a", b"e").unwrap();
    let mut entries = Vec::new();
    while scanner.is_valid() {
        entries.push((scanner.key().to_vec(), scanner.value().to_vec()));
        scanner.next().unwrap();
    }

    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0], (b"a".to_vec(), b"1".to_vec()));
    assert_eq!(entries[1], (b"b".to_vec(), b"updated".to_vec()));
    assert_eq!(entries[2], (b"d".to_vec(), b"4".to_vec()));

    db.close().unwrap();
}
