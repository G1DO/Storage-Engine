// M09: WAL Rotation tests
// Tests for WAL file rotation on memtable flush.

use lsm_engine::wal::WALRecord;
use lsm_engine::wal::reader::WALReader;
use lsm_engine::wal::writer::WALManager;
use lsm_engine::wal::SyncPolicy;

// =============================================================================
// Test 1: Rotate creates a new WAL file, old one still exists
// =============================================================================
#[test]
fn rotate_creates_new_wal_old_still_exists() {
    let dir = tempfile::tempdir().unwrap();
    let mut manager = WALManager::new(dir.path(), SyncPolicy::EveryWrite).unwrap();

    // Should start with one WAL file (the active one)
    let wal_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
        .collect();
    assert_eq!(wal_files.len(), 1);

    // Rotate
    let old_path = manager.rotate().unwrap();
    assert!(old_path.exists(), "old WAL should still exist after rotate");

    // Now two WAL files on disk
    let wal_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
        .collect();
    assert_eq!(wal_files.len(), 2);
}

// =============================================================================
// Test 2: Write to new WAL after rotation → records go to new file
// =============================================================================
#[test]
fn writes_go_to_new_wal_after_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let mut manager = WALManager::new(dir.path(), SyncPolicy::EveryWrite).unwrap();

    // Write to the first WAL
    manager
        .active_writer()
        .append(&WALRecord::put(b"before".to_vec(), b"rotate".to_vec()))
        .unwrap();

    // Rotate
    let old_path = manager.rotate().unwrap();

    // Write to the new WAL
    manager
        .active_writer()
        .append(&WALRecord::put(b"after".to_vec(), b"rotate".to_vec()))
        .unwrap();
    manager.active_writer().sync().unwrap();

    // Old WAL should contain only "before"
    let old_reader = WALReader::new(&old_path).unwrap();
    let old_records: Vec<WALRecord> = old_reader.iter().map(|r| r.unwrap()).collect();
    assert_eq!(old_records.len(), 1);
    assert_eq!(old_records[0].key, b"before");

    // New WAL should contain only "after"
    let new_reader = WALReader::new(manager.active_path()).unwrap();
    let new_records: Vec<WALRecord> = new_reader.iter().map(|r| r.unwrap()).collect();
    assert_eq!(new_records.len(), 1);
    assert_eq!(new_records[0].key, b"after");
}

// =============================================================================
// Test 3: Delete old WAL → file removed from disk
// =============================================================================
#[test]
fn delete_wal_removes_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut manager = WALManager::new(dir.path(), SyncPolicy::EveryWrite).unwrap();

    let old_path = manager.rotate().unwrap();
    assert!(old_path.exists());

    WALManager::delete_wal(&old_path).unwrap();
    assert!(!old_path.exists(), "old WAL should be gone after delete");
}

// =============================================================================
// Test 4: Multiple rotations → correct number of WAL files
// =============================================================================
#[test]
fn multiple_rotations_correct_file_count() {
    let dir = tempfile::tempdir().unwrap();
    let mut manager = WALManager::new(dir.path(), SyncPolicy::EveryWrite).unwrap();

    // Rotate 3 times without deleting old WALs
    let old1 = manager.rotate().unwrap();
    let old2 = manager.rotate().unwrap();
    let _old3 = manager.rotate().unwrap();

    // 4 WAL files: 3 old + 1 active
    let wal_count = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
        .count();
    assert_eq!(wal_count, 4);

    // Delete 2 old WALs (simulating SSTable flushes completing)
    WALManager::delete_wal(&old1).unwrap();
    WALManager::delete_wal(&old2).unwrap();

    let wal_count = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
        .count();
    assert_eq!(wal_count, 2);
}
