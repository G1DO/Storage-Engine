// M32: Public DB API Tests
//
// Tests the full public API: open, put, get, delete, scan, flush,
// compact_range, snapshot, close.

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

fn collect_scan(scanner: &mut lsm_engine::db::snapshot::Scanner) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut result = Vec::new();
    while scanner.is_valid() {
        result.push((scanner.key().to_vec(), scanner.value().to_vec()));
        scanner.next().unwrap();
    }
    result
}

// =============================================================================
// Test 1: Full lifecycle — open, put, get, delete, scan, close
// =============================================================================
#[test]
fn full_lifecycle() {
    let (_dir, db) = open_test_db();

    db.put(b"k1", b"v1").unwrap();
    db.put(b"k2", b"v2").unwrap();
    db.put(b"k3", b"v3").unwrap();

    assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));

    db.delete(b"k2").unwrap();
    assert_eq!(db.get(b"k2").unwrap(), None);

    let mut scanner = db.scan(b"k1", b"k4").unwrap();
    let entries = collect_scan(&mut scanner);
    let keys: Vec<&[u8]> = entries.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"k1", b"k3"]);

    db.close().unwrap();
}

// =============================================================================
// Test 2: Reopen after close → data persists
// =============================================================================
#[test]
fn reopen_after_close_data_persists() {
    let dir = tempdir().unwrap();
    let opts = Options {
        memtable_size: 64 * 1024,
        ..Options::default()
    };

    // Write and close
    {
        let db = DB::open(dir.path(), opts).unwrap();
        db.put(b"persistent", b"data").unwrap();
        db.close().unwrap();
    }

    // Reopen and verify
    {
        let opts2 = Options {
            memtable_size: 64 * 1024,
            ..Options::default()
        };
        let db = DB::open(dir.path(), opts2).unwrap();
        assert_eq!(db.get(b"persistent").unwrap(), Some(b"data".to_vec()));
        db.close().unwrap();
    }
}

// =============================================================================
// Test 3: Scan returns correct range
// =============================================================================
#[test]
fn scan_returns_correct_range() {
    let (_dir, db) = open_test_db();

    for c in b'a'..=b'z' {
        db.put(&[c], &[c]).unwrap();
    }

    let mut scanner = db.scan(b"d", b"h").unwrap();
    let entries = collect_scan(&mut scanner);
    let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, vec![vec![b'd'], vec![b'e'], vec![b'f'], vec![b'g']]);
}

// =============================================================================
// Test 4: Scan skips tombstones
// =============================================================================
#[test]
fn scan_skips_tombstones() {
    let (_dir, db) = open_test_db();

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();
    db.delete(b"b").unwrap();

    let mut scanner = db.scan(b"a", b"d").unwrap();
    let entries = collect_scan(&mut scanner);
    let keys: Vec<&[u8]> = entries.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"a", b"c"]);
}

// =============================================================================
// Test 5: Scan on empty range
// =============================================================================
#[test]
fn scan_empty_range() {
    let (_dir, db) = open_test_db();

    db.put(b"a", b"1").unwrap();
    db.put(b"z", b"2").unwrap();

    let scanner = db.scan(b"m", b"n").unwrap();
    assert!(!scanner.is_valid());
}

// =============================================================================
// Test 6: Scan across memtable and SSTable
// =============================================================================
#[test]
fn scan_across_memtable_and_sstable() {
    let (_dir, db) = open_test_db();

    // Flush to SSTable
    db.put(b"b", b"flushed").unwrap();
    db.put(b"d", b"flushed").unwrap();
    db.flush().unwrap();

    // In memtable
    db.put(b"a", b"mem").unwrap();
    db.put(b"c", b"mem").unwrap();

    let mut scanner = db.scan(b"a", b"e").unwrap();
    let entries = collect_scan(&mut scanner);
    let keys: Vec<&[u8]> = entries.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"a", b"b", b"c", b"d"]);
}

// =============================================================================
// Test 7: compact_range(None, None) compacts all
// =============================================================================
#[test]
fn compact_range_none_none_compacts_all() {
    let (_dir, db) = open_test_db();

    // Write enough to create multiple L0 SSTables
    for i in 0..4u32 {
        for j in 0..50u32 {
            let key = format!("key_{:05}", i * 50 + j).into_bytes();
            db.put(&key, b"value").unwrap();
        }
        db.flush().unwrap();
    }

    // Compact everything
    db.compact_range(None, None).unwrap();

    // All data should still be readable
    for i in 0..200u32 {
        let key = format!("key_{:05}", i).into_bytes();
        let val = db.get(&key).unwrap();
        assert_eq!(val, Some(b"value".to_vec()), "key_{:05} missing after compact", i);
    }
}

// =============================================================================
// Test 8: Thread safety — concurrent put/get
// =============================================================================
#[test]
fn thread_safety_concurrent_put_get() {
    use std::sync::Arc;
    use std::thread;

    let dir = tempdir().unwrap();
    let opts = Options {
        memtable_size: 256 * 1024,
        ..Options::default()
    };
    let db = Arc::new(DB::open(dir.path(), opts).unwrap());

    // Pre-populate
    for i in 0..100u32 {
        let key = format!("k{:04}", i).into_bytes();
        db.put(&key, b"initial").unwrap();
    }

    // Spawn readers and writers
    let mut handles = vec![];

    // Writer thread
    let db_w = Arc::clone(&db);
    handles.push(thread::spawn(move || {
        for i in 100..200u32 {
            let key = format!("k{:04}", i).into_bytes();
            db_w.put(&key, b"written").unwrap();
        }
    }));

    // Reader threads
    for _ in 0..4 {
        let db_r = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..100u32 {
                let key = format!("k{:04}", i).into_bytes();
                let _val = db_r.get(&key).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify writer's data is visible
    for i in 100..200u32 {
        let key = format!("k{:04}", i).into_bytes();
        assert_eq!(
            db.get(&key).unwrap(),
            Some(b"written".to_vec()),
            "key k{:04} should exist", i
        );
    }
}
