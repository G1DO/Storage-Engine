// M28: Full DB Recovery Tests
//
// Each test verifies a different crash/recovery scenario.
// "Simulate crash" = drop the DB without calling close().
// This leaves WAL data un-flushed, which open() must replay.

use tempfile::tempdir;

use lsm_engine::{DB, Options};

/// Helper: open a DB with small memtable for testing.
fn open_db(path: &std::path::Path) -> DB {
    let opts = Options {
        memtable_size: 64 * 1024, // 64 KB — small enough to test flush
        ..Options::default()
    };
    DB::open(path, opts).expect("open db")
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: Empty DB open/close/reopen
// Verifies: fresh state survives a full open→close→reopen cycle
// ─────────────────────────────────────────────────────────────────────────────
#[test]
fn empty_db_survives_reopen() {
    let dir = tempdir().unwrap();

    {
        let db = open_db(dir.path());
        db.close().unwrap();
    }

    let db = open_db(dir.path());
    assert_eq!(db.get(b"any_key").unwrap(), None);
    db.close().unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: Put, close cleanly, reopen → data present
// Verifies: clean shutdown flushes memtable, manifest records SSTable
// ─────────────────────────────────────────────────────────────────────────────
#[test]
fn clean_shutdown_preserves_data() {
    let dir = tempdir().unwrap();

    {
        let db = open_db(dir.path());
        db.put(b"hello", b"world").unwrap();
        db.put(b"foo", b"bar").unwrap();
        db.close().unwrap();
    }

    let db = open_db(dir.path());
    assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));
    assert_eq!(db.get(b"foo").unwrap(), Some(b"bar".to_vec()));
    assert_eq!(db.get(b"missing").unwrap(), None);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: Put, DROP (simulate crash), reopen → data present
// Verifies: WAL replay recovers unflushed writes
// ─────────────────────────────────────────────────────────────────────────────
#[test]
fn crash_recovery_replays_wal() {
    let dir = tempdir().unwrap();

    {
        let db = open_db(dir.path());
        db.put(b"key1", b"val1").unwrap();
        db.put(b"key2", b"val2").unwrap();
        // Crash: drop without close — no flush, WAL has the data
        drop(db);
    }

    let db = open_db(dir.path());
    assert_eq!(db.get(b"key1").unwrap(), Some(b"val1".to_vec()));
    assert_eq!(db.get(b"key2").unwrap(), Some(b"val2".to_vec()));
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: Put, flush, put more, DROP, reopen → all data present
// Verifies: SSTable data (from flush) + WAL data (unflushed) both recovered
// ─────────────────────────────────────────────────────────────────────────────
#[test]
fn flush_then_crash_recovers_both_sst_and_wal() {
    let dir = tempdir().unwrap();

    {
        let db = open_db(dir.path());
        // These will be flushed to SSTable
        db.put(b"flushed_key", b"flushed_val").unwrap();
        db.flush().unwrap();

        // These stay in WAL only (not flushed)
        db.put(b"wal_key", b"wal_val").unwrap();

        // Crash
        drop(db);
    }

    let db = open_db(dir.path());
    // Flushed data comes from SSTable (via manifest)
    assert_eq!(
        db.get(b"flushed_key").unwrap(),
        Some(b"flushed_val".to_vec())
    );
    // Unflushed data comes from WAL replay
    assert_eq!(db.get(b"wal_key").unwrap(), Some(b"wal_val".to_vec()));
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5: Put, delete, DROP, reopen → key gone
// Verifies: tombstone replay from WAL
// ─────────────────────────────────────────────────────────────────────────────
#[test]
fn crash_recovery_replays_tombstones() {
    let dir = tempdir().unwrap();

    {
        let db = open_db(dir.path());
        db.put(b"ephemeral", b"exists").unwrap();
        db.delete(b"ephemeral").unwrap();
        // Crash
        drop(db);
    }

    let db = open_db(dir.path());
    // Tombstone was replayed — key should not be found
    assert_eq!(db.get(b"ephemeral").unwrap(), None);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 6: Orphaned .sst file on disk → ignored
// Verifies: only manifest-referenced SSTables are used for reads
// ─────────────────────────────────────────────────────────────────────────────
#[test]
fn orphaned_sst_file_ignored() {
    let dir = tempdir().unwrap();

    // Create an orphaned SST file that the manifest doesn't know about
    {
        use lsm_engine::sstable::builder::SSTableBuilder;
        let orphan_path = dir.path().join("999999.sst");
        let mut builder = SSTableBuilder::new(&orphan_path, 999999, 4096).unwrap();
        builder.add(b"orphan_key", b"orphan_val").unwrap();
        builder.finish().unwrap();
    }

    let db = open_db(dir.path());
    // The orphaned SSTable's data should NOT be visible
    assert_eq!(db.get(b"orphan_key").unwrap(), None);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 7: Multiple flushes, reopen → all data present
// Verifies: manifest replays multiple NewSSTable records correctly
// ─────────────────────────────────────────────────────────────────────────────
#[test]
fn multiple_flushes_all_data_recovered() {
    let dir = tempdir().unwrap();

    {
        let db = open_db(dir.path());

        db.put(b"batch1_a", b"val1a").unwrap();
        db.put(b"batch1_b", b"val1b").unwrap();
        db.flush().unwrap();

        db.put(b"batch2_a", b"val2a").unwrap();
        db.put(b"batch2_b", b"val2b").unwrap();
        db.flush().unwrap();

        db.put(b"batch3_a", b"val3a").unwrap();
        db.flush().unwrap();

        db.close().unwrap();
    }

    let db = open_db(dir.path());
    assert_eq!(db.get(b"batch1_a").unwrap(), Some(b"val1a".to_vec()));
    assert_eq!(db.get(b"batch1_b").unwrap(), Some(b"val1b".to_vec()));
    assert_eq!(db.get(b"batch2_a").unwrap(), Some(b"val2a".to_vec()));
    assert_eq!(db.get(b"batch2_b").unwrap(), Some(b"val2b".to_vec()));
    assert_eq!(db.get(b"batch3_a").unwrap(), Some(b"val3a".to_vec()));
}
