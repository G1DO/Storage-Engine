// M07: WAL Writer tests
// Tests for writing WAL records to disk with fsync.

use std::io::Read;
use lsm_engine::wal::{WALRecord, RecordType, SyncPolicy};
use lsm_engine::wal::writer::WALWriter;

// =============================================================================
// Test 1: Write one record, read file back
// =============================================================================
#[test]
#[ignore]
fn write_one_record_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.wal");

    let record = WALRecord::put(b"key".to_vec(), b"value".to_vec());

    {
        let mut writer = WALWriter::new(&path, SyncPolicy::EveryWrite).unwrap();
        writer.append(&record).unwrap();
    }

    // Read file back and decode
    let mut file = std::fs::File::open(&path).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();

    let decoded = WALRecord::decode(&buf).unwrap();
    assert_eq!(decoded.record_type, RecordType::Put);
    assert_eq!(decoded.key, b"key");
    assert_eq!(decoded.value, b"value");
}

// =============================================================================
// Test 2: Write multiple records in order
// =============================================================================
#[test]
#[ignore]
fn write_multiple_records_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.wal");

    {
        let mut writer = WALWriter::new(&path, SyncPolicy::EveryWrite).unwrap();

        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let val = format!("val{}", i).into_bytes();
            writer.append(&WALRecord::put(key, val)).unwrap();
        }
    }

    // Read file back and decode all records
    let mut file = std::fs::File::open(&path).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();

    let mut offset = 0;
    for i in 0..5 {
        let decoded = WALRecord::decode(&buf[offset..]).unwrap();
        let expected_key = format!("key{}", i).into_bytes();
        let expected_val = format!("val{}", i).into_bytes();
        assert_eq!(decoded.key, expected_key);
        assert_eq!(decoded.value, expected_val);
        offset += decoded.encoded_size();
    }
}

// =============================================================================
// Test 3: Data survives reopen after sync
// =============================================================================
#[test]
#[ignore]
fn data_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.wal");

    // Write and sync
    {
        let mut writer = WALWriter::new(&path, SyncPolicy::EveryWrite).unwrap();
        writer.append(&WALRecord::put(b"durable".to_vec(), b"data".to_vec())).unwrap();
        writer.sync().unwrap();
    }

    // Reopen and verify
    let mut file = std::fs::File::open(&path).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();

    let decoded = WALRecord::decode(&buf).unwrap();
    assert_eq!(decoded.key, b"durable");
    assert_eq!(decoded.value, b"data");
}

// =============================================================================
// Test 4: Offset tracking matches expected size
// =============================================================================
#[test]
#[ignore]
fn offset_tracks_bytes_written() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.wal");

    let mut writer = WALWriter::new(&path, SyncPolicy::EveryWrite).unwrap();
    assert_eq!(writer.offset(), 0);

    let record = WALRecord::put(b"key".to_vec(), b"value".to_vec());
    let expected_size = record.encoded_size() as u64;

    writer.append(&record).unwrap();
    assert_eq!(writer.offset(), expected_size);

    writer.append(&record).unwrap();
    assert_eq!(writer.offset(), expected_size * 2);
}

// =============================================================================
// Test 5: Write delete record
// =============================================================================
#[test]
#[ignore]
fn write_delete_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.wal");

    {
        let mut writer = WALWriter::new(&path, SyncPolicy::EveryWrite).unwrap();
        writer.append(&WALRecord::delete(b"gone".to_vec())).unwrap();
    }

    let mut file = std::fs::File::open(&path).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();

    let decoded = WALRecord::decode(&buf).unwrap();
    assert_eq!(decoded.record_type, RecordType::Delete);
    assert_eq!(decoded.key, b"gone");
}
