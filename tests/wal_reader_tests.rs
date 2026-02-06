// M08: WAL Reader tests
// Tests for reading WAL records back for crash recovery.

use std::io::Write;
use lsm_engine::wal::{WALRecord, RecordType};
use lsm_engine::wal::writer::WALWriter;
use lsm_engine::wal::reader::WALReader;
use lsm_engine::wal::SyncPolicy;

/// Helper: write N put records to a WAL file, return the path.
fn write_test_wal(dir: &tempfile::TempDir, count: usize) -> std::path::PathBuf {
    let path = dir.path().join("test.wal");
    let mut writer = WALWriter::new(&path, SyncPolicy::EveryWrite).unwrap();
    for i in 0..count {
        let key = format!("key{}", i).into_bytes();
        let val = format!("val{}", i).into_bytes();
        writer.append(&WALRecord::put(key, val)).unwrap();
    }
    writer.sync().unwrap();
    path
}

// =============================================================================
// Test 1: Write 5 records, read all 5 back
// =============================================================================
#[test]
fn read_all_records_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_test_wal(&dir, 5);

    let reader = WALReader::new(&path).unwrap();
    let records: Vec<WALRecord> = reader.iter().map(|r| r.unwrap()).collect();

    assert_eq!(records.len(), 5);
    for i in 0..5 {
        assert_eq!(records[i].record_type, RecordType::Put);
        assert_eq!(records[i].key, format!("key{}", i).as_bytes());
        assert_eq!(records[i].value, format!("val{}", i).as_bytes());
    }
}

// =============================================================================
// Test 2: Truncate file mid-record → yields first 4, stops at 5th
// =============================================================================
#[test]
fn truncated_last_record_yields_preceding() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_test_wal(&dir, 5);

    // Chop off last few bytes to simulate crash mid-write
    let file_len = std::fs::metadata(&path).unwrap().len();
    let truncated_len = file_len - 3; // remove 3 bytes from end
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap();
    file.set_len(truncated_len).unwrap();

    let reader = WALReader::new(&path).unwrap();
    let records: Vec<WALRecord> = reader.iter().map(|r| r.unwrap()).collect();

    assert_eq!(records.len(), 4);
    for i in 0..4 {
        assert_eq!(records[i].key, format!("key{}", i).as_bytes());
    }
}

// =============================================================================
// Test 3: Corrupt CRC of record 3 → yields records 1-2, stops
// =============================================================================
#[test]
fn corrupt_crc_stops_iteration() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_test_wal(&dir, 5);

    // Find byte offset of record at index 2 (the 3rd record)
    // by summing encoded sizes of records at indices 0 and 1
    let offset_of_record_2: usize = (0..2)
        .map(|i| {
            let key = format!("key{}", i).into_bytes();
            let val = format!("val{}", i).into_bytes();
            WALRecord::put(key, val).encoded_size()
        })
        .sum();

    // Flip a bit in the CRC of the 3rd record
    let mut raw = std::fs::read(&path).unwrap();
    raw[offset_of_record_2] ^= 0x01;
    std::fs::write(&path, &raw).unwrap();

    let reader = WALReader::new(&path).unwrap();
    let records: Vec<WALRecord> = reader.iter().map(|r| r.unwrap()).collect();

    assert_eq!(records.len(), 2);
    for i in 0..2 {
        assert_eq!(records[i].key, format!("key{}", i).as_bytes());
        assert_eq!(records[i].value, format!("val{}", i).as_bytes());
    }
}

// =============================================================================
// Test 4: Empty WAL file → iterator immediately exhausted
// =============================================================================
#[test]
fn empty_wal_yields_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.wal");
    std::fs::File::create(&path).unwrap();

    let reader = WALReader::new(&path).unwrap();
    let records: Vec<WALRecord> = reader.iter().map(|r| r.unwrap()).collect();

    assert_eq!(records.len(), 0);
}

// =============================================================================
// Test 5: Partial first record → yields nothing
// =============================================================================
#[test]
fn partial_first_record_yields_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("partial.wal");

    // Write just 3 bytes — not even a complete header
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(&[0xDE, 0xAD, 0x00]).unwrap();
    file.sync_all().unwrap();

    let reader = WALReader::new(&path).unwrap();
    let records: Vec<WALRecord> = reader.iter().map(|r| r.unwrap()).collect();

    assert_eq!(records.len(), 0);
}
