// M34: Stats / Observability Tests
//
// Verifies that DB::stats() returns meaningful metrics after various operations.

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

// =============================================================================
// Test 1: Stats on empty DB returns valid defaults
// =============================================================================
#[test]
fn stats_on_empty_db_valid_defaults() {
    let (_dir, db) = open_test_db();

    let stats = db.stats();
    assert_eq!(stats.memtable_size, 0);
    assert_eq!(stats.bytes_written, 0);
    assert_eq!(stats.bytes_read, 0);
    assert_eq!(stats.compaction_count, 0);
    assert_eq!(stats.compaction_bytes, 0);
    assert!(stats.write_amplification >= 0.0);
    assert!(!stats.block_cache_hit_rate.is_nan());
}

// =============================================================================
// Test 2: After puts, bytes_written > 0
// =============================================================================
#[test]
fn stats_after_puts_bytes_written_positive() {
    let (_dir, db) = open_test_db();

    for i in 0..10u32 {
        let key = format!("key_{:03}", i).into_bytes();
        db.put(&key, b"some_value").unwrap();
    }

    let stats = db.stats();
    assert!(stats.bytes_written > 0, "bytes_written should be > 0 after puts");
}

// =============================================================================
// Test 3: Memtable size after puts
// =============================================================================
#[test]
fn stats_memtable_size_after_puts() {
    let (_dir, db) = open_test_db();

    db.put(b"hello", b"world").unwrap();
    db.put(b"foo", b"bar").unwrap();

    let stats = db.stats();
    assert!(stats.memtable_size > 0, "memtable_size should be > 0");
}

// =============================================================================
// Test 4: After flush, SSTable count > 0
// =============================================================================
#[test]
fn stats_after_flush_sstable_count() {
    let (_dir, db) = open_test_db();

    db.put(b"key", b"val").unwrap();
    db.flush().unwrap();

    let stats = db.stats();
    assert!(
        stats.num_sstables_per_level[0] > 0,
        "L0 should have at least one SSTable after flush"
    );
}

// =============================================================================
// Test 5: Block cache hit rate is valid (between 0.0 and 1.0, not NaN)
// =============================================================================
#[test]
fn stats_block_cache_hit_rate_valid() {
    let (_dir, db) = open_test_db();

    let stats = db.stats();
    assert!(!stats.block_cache_hit_rate.is_nan());
    assert!(stats.block_cache_hit_rate >= 0.0);
    assert!(stats.block_cache_hit_rate <= 1.0);
}

// =============================================================================
// Test 6: Write amplification >= 1.0 after flush
// =============================================================================
#[test]
fn stats_write_amplification_at_least_one() {
    let (_dir, db) = open_test_db();

    for i in 0..20u32 {
        let key = format!("key_{:03}", i).into_bytes();
        db.put(&key, b"value_data").unwrap();
    }
    db.flush().unwrap();

    let stats = db.stats();
    assert!(
        stats.write_amplification >= 1.0,
        "write amplification should be >= 1.0, got {}",
        stats.write_amplification
    );
}

// =============================================================================
// Test 7: After compaction, compaction_count > 0
// =============================================================================
#[test]
fn stats_after_compaction_count_positive() {
    let dir = tempdir().unwrap();
    let opts = Options {
        memtable_size: 64 * 1024,
        compaction_style: lsm_engine::CompactionStyle::SizeTiered,
        ..Options::default()
    };
    let db = DB::open(dir.path(), opts).unwrap();

    // Create multiple L0 SSTables to trigger compaction
    for round in 0..4u32 {
        for i in 0..30u32 {
            let key = format!("key_{:05}", round * 30 + i).into_bytes();
            db.put(&key, b"val").unwrap();
        }
        db.flush().unwrap();
    }

    db.compact_range(None, None).unwrap();

    let stats = db.stats();
    assert!(
        stats.compaction_count > 0,
        "compaction_count should be > 0 after compact_range"
    );
    assert!(
        stats.compaction_bytes > 0,
        "compaction_bytes should be > 0 after compact_range"
    );
}
