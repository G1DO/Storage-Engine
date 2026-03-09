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

// ---------------------------------------------------------------------------
// M29: Manifest Compaction Tests
// ---------------------------------------------------------------------------

/// Extract SSTable IDs at the given level from the manifest's current version.
fn sst_ids_at_level(manifest: &Manifest, level: usize) -> Vec<u64> {
    manifest
        .current_version()
        .level(level)
        .iter()
        .map(|m| m.id)
        .collect()
}

#[test]
#[ignore]
fn manifest_compact_produces_smaller_file() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");
    let mut manifest = Manifest::open(&path).expect("open manifest");

    // 20 flushes to L0
    for i in 1..=20u64 {
        let key = format!("{:04}", i);
        manifest
            .record_flush(make_sst(i, 0, key.as_bytes(), key.as_bytes()))
            .expect("record flush");
    }

    // 10 compactions: merge consecutive pairs into L1, removing the originals
    for i in 0..10u64 {
        let old_a = i * 2 + 1;
        let old_b = i * 2 + 2;
        let new_id = 21 + i;
        let min_key = format!("{:04}", old_a);
        let max_key = format!("{:04}", old_b);
        manifest
            .record_compaction(
                vec![make_sst(new_id, 1, min_key.as_bytes(), max_key.as_bytes())],
                vec![old_a, old_b],
            )
            .expect("record compaction");
    }

    let size_before = fs::metadata(&path).expect("metadata").len();
    manifest.compact().expect("compact");
    let size_after = fs::metadata(&path).expect("metadata").len();

    assert!(
        size_after < size_before,
        "compacted manifest ({size_after}) should be smaller than original ({size_before})"
    );
}

#[test]
#[ignore]
fn manifest_compact_reopen_reconstructs_version() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");

    let l0_ids;
    let l1_ids;
    let total;
    let log_num;
    let next_id;

    {
        let mut manifest = Manifest::open(&path).expect("open manifest");

        // Flush 3 SSTables to L0
        manifest
            .record_flush(make_sst(1, 0, b"a", b"d"))
            .expect("flush");
        manifest
            .record_flush(make_sst(2, 0, b"e", b"h"))
            .expect("flush");
        manifest
            .record_flush(make_sst(3, 0, b"i", b"l"))
            .expect("flush");

        // Compact two L0 SSTables into L1
        manifest
            .record_compaction(vec![make_sst(4, 1, b"a", b"h")], vec![1, 2])
            .expect("compaction");

        manifest.record_log_number(7).expect("log number");

        manifest.compact().expect("compact");

        // Snapshot after compact (in-memory state is authoritative)
        l0_ids = sst_ids_at_level(&manifest, 0);
        l1_ids = sst_ids_at_level(&manifest, 1);
        total = manifest.current_version().total_sstables();
        log_num = manifest.log_number();
        next_id = manifest.next_sst_id();
    }

    // Reopen and verify every field matches
    let reopened = Manifest::open(&path).expect("reopen");
    assert_eq!(sst_ids_at_level(&reopened, 0), l0_ids);
    assert_eq!(sst_ids_at_level(&reopened, 1), l1_ids);
    assert_eq!(reopened.current_version().total_sstables(), total);
    assert_eq!(reopened.log_number(), log_num);
    assert_eq!(reopened.next_sst_id(), next_id);
}

#[test]
#[ignore]
fn manifest_compact_crash_old_manifest_valid() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");

    let mut manifest = Manifest::open(&path).expect("open manifest");
    manifest
        .record_flush(make_sst(1, 0, b"a", b"f"))
        .expect("flush");
    manifest
        .record_flush(make_sst(2, 0, b"g", b"k"))
        .expect("flush");

    // Backup manifest BEFORE compact
    let backup = dir.path().join("MANIFEST.bak");
    fs::copy(&path, &backup).expect("backup");

    // compact() succeeds and replaces the file
    manifest.compact().expect("compact");
    drop(manifest);

    // Simulate crash before rename: restore the old manifest over the compacted one
    fs::copy(&backup, &path).expect("restore backup");

    // Reopen with old manifest — should reconstruct original version
    let reopened = Manifest::open(&path).expect("reopen");
    assert_eq!(reopened.current_version().total_sstables(), 2);
    assert_eq!(sst_ids_at_level(&reopened, 0), vec![1, 2]);
}

#[test]
#[ignore]
fn manifest_compact_preserves_log_number() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");

    {
        let mut manifest = Manifest::open(&path).expect("open manifest");
        manifest
            .record_flush(make_sst(1, 0, b"a", b"z"))
            .expect("flush");
        manifest.record_log_number(42).expect("log number");

        manifest.compact().expect("compact");
        assert_eq!(manifest.log_number(), 42);
    }

    let reopened = Manifest::open(&path).expect("reopen");
    assert_eq!(reopened.log_number(), 42);
}

#[test]
#[ignore]
fn manifest_compact_preserves_next_sst_id() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");

    {
        let mut manifest = Manifest::open(&path).expect("open manifest");
        // Non-contiguous IDs
        manifest
            .record_flush(make_sst(1, 0, b"a", b"c"))
            .expect("flush");
        manifest
            .record_flush(make_sst(5, 0, b"d", b"f"))
            .expect("flush");
        manifest
            .record_flush(make_sst(10, 0, b"g", b"i"))
            .expect("flush");

        manifest.compact().expect("compact");
        assert_eq!(manifest.next_sst_id(), 11);
    }

    let reopened = Manifest::open(&path).expect("reopen");
    assert_eq!(reopened.next_sst_id(), 11);
}

#[test]
#[ignore]
fn manifest_compact_empty_version() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");

    {
        let mut manifest = Manifest::open(&path).expect("open manifest");
        manifest.record_log_number(5).expect("log number");

        manifest.compact().expect("compact");
    }

    let reopened = Manifest::open(&path).expect("reopen");
    assert_eq!(reopened.current_version().total_sstables(), 0);
    assert_eq!(reopened.log_number(), 5);
}

#[test]
#[ignore]
fn manifest_compact_then_record_flush() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");

    {
        let mut manifest = Manifest::open(&path).expect("open manifest");

        // 5 flushes before compact
        for i in 1..=5u64 {
            let key = format!("{:04}", i);
            manifest
                .record_flush(make_sst(i, 0, key.as_bytes(), key.as_bytes()))
                .expect("flush");
        }

        manifest.compact().expect("compact");

        // 2 more flushes after compact
        for i in 6..=7u64 {
            let key = format!("{:04}", i);
            manifest
                .record_flush(make_sst(i, 0, key.as_bytes(), key.as_bytes()))
                .expect("flush after compact");
        }
    }

    let reopened = Manifest::open(&path).expect("reopen");
    assert_eq!(reopened.current_version().total_sstables(), 7);
}

#[test]
#[ignore]
fn manifest_compact_multiple_times() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");

    {
        let mut manifest = Manifest::open(&path).expect("open manifest");

        // Round 1: 5 flushes, compact
        for i in 1..=5u64 {
            let key = format!("{:04}", i);
            manifest
                .record_flush(make_sst(i, 0, key.as_bytes(), key.as_bytes()))
                .expect("flush");
        }
        manifest.compact().expect("compact 1");

        // Round 2: 3 more, compact
        for i in 6..=8u64 {
            let key = format!("{:04}", i);
            manifest
                .record_flush(make_sst(i, 0, key.as_bytes(), key.as_bytes()))
                .expect("flush");
        }
        manifest.compact().expect("compact 2");

        // Round 3: 2 more, compact
        for i in 9..=10u64 {
            let key = format!("{:04}", i);
            manifest
                .record_flush(make_sst(i, 0, key.as_bytes(), key.as_bytes()))
                .expect("flush");
        }
        manifest.compact().expect("compact 3");
    }

    let reopened = Manifest::open(&path).expect("reopen");
    assert_eq!(reopened.current_version().total_sstables(), 10);
}

#[test]
#[ignore]
fn manifest_compact_multi_level_version() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");

    {
        let mut manifest = Manifest::open(&path).expect("open manifest");

        // Flush 3 SSTables to L0
        manifest
            .record_flush(make_sst(1, 0, b"a", b"d"))
            .expect("flush");
        manifest
            .record_flush(make_sst(2, 0, b"e", b"h"))
            .expect("flush");
        manifest
            .record_flush(make_sst(3, 0, b"m", b"p"))
            .expect("flush");

        // Compact SSTs 1,2 from L0 → L1
        manifest
            .record_compaction(vec![make_sst(4, 1, b"a", b"h")], vec![1, 2])
            .expect("compaction to L1");

        // Compact SST 4 from L1 → L2
        manifest
            .record_compaction(vec![make_sst(5, 2, b"a", b"h")], vec![4])
            .expect("compaction to L2");

        manifest.compact().expect("compact");
    }

    let reopened = Manifest::open(&path).expect("reopen");
    // L0: SST 3 remains
    assert_eq!(sst_ids_at_level(&reopened, 0), vec![3]);
    // L1: empty (SST 4 was compacted to L2)
    assert_eq!(sst_ids_at_level(&reopened, 1), Vec::<u64>::new());
    // L2: SST 5
    assert_eq!(sst_ids_at_level(&reopened, 2), vec![5]);
}
