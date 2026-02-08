// M10: Configurable SyncPolicy tests
// Tests that each sync policy behaves correctly.

use lsm_engine::wal::{WALRecord, RecordType};
use lsm_engine::wal::writer::WALWriter;
use lsm_engine::wal::reader::WALReader;
use lsm_engine::wal::SyncPolicy;

fn make_record(i: usize) -> WALRecord {
    WALRecord::put(format!("key{}", i).into_bytes(), format!("val{}", i).into_bytes())
}

// =============================================================================
// Test 1: EveryWrite — counter resets after every append
// =============================================================================
#[test]
fn every_write_syncs_after_each_append() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let mut writer = WALWriter::new(&path, SyncPolicy::EveryWrite).unwrap();

    assert_eq!(writer.writes_since_sync(), 0);

    writer.append(&make_record(0)).unwrap();
    assert_eq!(writer.writes_since_sync(), 0, "should reset after every write");

    writer.append(&make_record(1)).unwrap();
    assert_eq!(writer.writes_since_sync(), 0, "should reset after every write");

    writer.append(&make_record(2)).unwrap();
    assert_eq!(writer.writes_since_sync(), 0, "should reset after every write");
}

// =============================================================================
// Test 2: EveryNWrites(3) — counter accumulates, resets on 3rd write
// =============================================================================
#[test]
fn every_n_writes_syncs_after_nth_append() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let mut writer = WALWriter::new(&path, SyncPolicy::EveryNWrites(3)).unwrap();

    assert_eq!(writer.writes_since_sync(), 0);

    writer.append(&make_record(0)).unwrap();
    assert_eq!(writer.writes_since_sync(), 1, "no sync yet");

    writer.append(&make_record(1)).unwrap();
    assert_eq!(writer.writes_since_sync(), 2, "no sync yet");

    writer.append(&make_record(2)).unwrap();
    assert_eq!(writer.writes_since_sync(), 0, "should reset after 3rd write");

    // Next batch
    writer.append(&make_record(3)).unwrap();
    assert_eq!(writer.writes_since_sync(), 1, "new batch started");

    writer.append(&make_record(4)).unwrap();
    assert_eq!(writer.writes_since_sync(), 2);

    writer.append(&make_record(5)).unwrap();
    assert_eq!(writer.writes_since_sync(), 0, "should reset after 3rd write again");
}

// =============================================================================
// Test 3: All policies produce readable WAL files
// =============================================================================
#[test]
fn all_policies_produce_readable_files() {
    let policies = vec![
        SyncPolicy::EveryWrite,
        SyncPolicy::EveryNWrites(2),
        SyncPolicy::EveryNMillis(1000),
    ];

    for (idx, policy) in policies.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("test_{}.wal", idx));

        {
            let mut writer = WALWriter::new(&path, policy).unwrap();
            for i in 0..5 {
                writer.append(&make_record(i)).unwrap();
            }
            writer.sync().unwrap();
        }

        let reader = WALReader::new(&path).unwrap();
        let records: Vec<WALRecord> = reader.iter().map(|r| r.unwrap()).collect();

        assert_eq!(records.len(), 5, "policy {:?} should produce 5 readable records", policy);
        for i in 0..5 {
            assert_eq!(records[i].record_type, RecordType::Put);
            assert_eq!(records[i].key, format!("key{}", i).as_bytes());
            assert_eq!(records[i].value, format!("val{}", i).as_bytes());
        }
    }
}
