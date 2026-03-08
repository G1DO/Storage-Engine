// Manifest tests (scaffold)
//
// These tests describe the behavior spelled out in M27. They stay ignored
// until `Manifest` is implemented so they don't fail the suite.

use std::fs;

use tempfile::tempdir;

use lsm_engine::manifest::Manifest;
use lsm_engine::sstable::footer::SSTableMeta;

fn make_sst(id: u64, level: u32, min_key: &[u8], max_key: &[u8]) -> SSTableMeta {
    SSTableMeta {
        id,
        level,
        min_key: min_key.to_vec(),
        max_key: max_key.to_vec(),
        file_size: 0,
        entry_count: 0,
    }
}

#[test]
fn manifest_record_flush_updates_version() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");
    let mut manifest = Manifest::open(&path).expect("open manifest");

    let meta = make_sst(1, 0, b"a", b"m");
    manifest.record_flush(meta.clone()).expect("record flush");

    let version = manifest.current_version();
    let level0 = version.level(0);
    assert_eq!(level0.len(), 1);
    assert_eq!(level0[0].id, meta.id);
    assert_eq!(level0[0].level, meta.level);
}

#[test]
fn manifest_record_compaction_adjusts_version() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");
    let mut manifest = Manifest::open(&path).expect("open manifest");

    let meta_a = make_sst(1, 0, b"a", b"f");
    let meta_b = make_sst(2, 0, b"g", b"k");
    manifest.record_flush(meta_a.clone()).expect("record flush");
    manifest.record_flush(meta_b.clone()).expect("record flush");

    let merged = make_sst(3, 0, b"a", b"k");
    manifest
        .record_compaction(vec![merged.clone()], vec![meta_a.id, meta_b.id])
        .expect("record compaction");

    let level0 = manifest.current_version().level(0);
    assert_eq!(level0.len(), 1);
    assert_eq!(level0[0].id, merged.id);
}

#[test]
fn manifest_reopen_replays_records() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");
    {
        let mut manifest = Manifest::open(&path).expect("open manifest");
        manifest
            .record_flush(make_sst(1, 0, b"a", b"m"))
            .expect("record flush");
        manifest
            .record_flush(make_sst(2, 0, b"n", b"z"))
            .expect("record flush");
    }

    let reopened = Manifest::open(&path).expect("reopen manifest");
    assert_eq!(reopened.current_version().total_sstables(), 2);
}

#[test]
fn manifest_detects_corruption() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");

    fs::write(&path, b"not a valid manifest").expect("write junk");
    assert!(
        Manifest::open(&path).is_err(),
        "corrupt manifest should fail to open"
    );
}
