use std::sync::Arc;

use lsm_engine::compaction::scheduler::CompactionScheduler;
use lsm_engine::compaction::size_tiered::SizeTieredStrategy;
use lsm_engine::manifest::version::VersionSet;
use lsm_engine::sstable::builder::SSTableBuilder;
use lsm_engine::sstable::footer::SSTableMeta;
use lsm_engine::sstable::reader::SSTable;

use tempfile::tempdir;

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

fn test_scheduler() -> (CompactionScheduler, tempfile::TempDir, Arc<VersionSet>) {
    let dir = tempdir().unwrap();
    let vs = Arc::new(VersionSet::new(4));
    let strategy = Arc::new(SizeTieredStrategy::new(4));
    let scheduler = CompactionScheduler::start(
        Arc::clone(&vs),
        strategy,
        dir.path().to_path_buf(),
        4096,
    )
    .unwrap();
    (scheduler, dir, vs)
}

#[test]
fn start_and_shutdown() {
    let (scheduler, _dir, _vs) = test_scheduler();
    scheduler.shutdown().unwrap();
}

#[test]
fn notify_flush_does_not_panic() {
    let (scheduler, _dir, _vs) = test_scheduler();
    scheduler.notify_flush();
    scheduler.shutdown().unwrap();
}

#[test]
fn multiple_flushes_then_shutdown() {
    let (scheduler, _dir, _vs) = test_scheduler();
    for _ in 0..10 {
        scheduler.notify_flush();
    }
    scheduler.shutdown().unwrap();
}

#[test]
fn shutdown_completes_within_timeout() {
    use std::time::{Duration, Instant};

    let (scheduler, _dir, _vs) = test_scheduler();
    scheduler.notify_flush();

    let start = Instant::now();
    scheduler.shutdown().unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "shutdown took too long"
    );
}

#[test]
fn flush_with_empty_version_no_crash() {
    let (scheduler, _dir, _vs) = test_scheduler();
    scheduler.notify_flush();
    std::thread::sleep(std::time::Duration::from_millis(50));
    scheduler.shutdown().unwrap();
}

#[test]
fn flush_below_threshold_does_nothing() {
    let dir = tempdir().unwrap();
    let vs = Arc::new(VersionSet::new(4));

    // Add 2 SSTables to L0 (threshold is 4, not enough to trigger)
    {
        let current = vs.current();
        let mut v = current.write().unwrap();
        v.levels[0].push(make_sst(1, 0, b"a", b"m"));
        v.levels[0].push(make_sst(2, 0, b"n", b"z"));
    }

    let strategy = Arc::new(SizeTieredStrategy::new(4));
    let scheduler = CompactionScheduler::start(
        Arc::clone(&vs),
        strategy,
        dir.path().to_path_buf(),
        4096,
    )
    .unwrap();

    scheduler.notify_flush();
    std::thread::sleep(std::time::Duration::from_millis(50));
    scheduler.shutdown().unwrap();

    // Version unchanged
    let current = vs.current();
    let v = current.read().unwrap();
    assert_eq!(v.level(0).len(), 2);
}

#[test]
fn compaction_merges_l0_into_l1() {
    let dir = tempdir().unwrap();
    let db_path = dir.path();

    // Create 4 L0 SSTables on disk with non-overlapping keys
    // Use IDs 101-104 to avoid collision with VersionSet counter (starts at 1)
    let mut metas = Vec::new();
    for i in 0..4u64 {
        let id = 101 + i;
        let path = db_path.join(format!("{:06}.sst", id));
        let mut builder = SSTableBuilder::new(&path, id, 4096).unwrap();
        for j in 0..10u32 {
            let key = format!("key_{:05}", i as u32 * 10 + j);
            let val = format!("val_{:05}", i as u32 * 10 + j);
            builder.add(key.as_bytes(), val.as_bytes()).unwrap();
        }
        let mut meta = builder.finish().unwrap();
        meta.level = 0;
        metas.push(meta);
    }

    // Set up VersionSet with 4 SSTables in L0
    let vs = Arc::new(VersionSet::new(4));
    {
        let current = vs.current();
        let mut v = current.write().unwrap();
        for m in &metas {
            v.levels[0].push(m.clone());
        }
    }

    let strategy = Arc::new(SizeTieredStrategy::new(4));
    let scheduler = CompactionScheduler::start(
        Arc::clone(&vs),
        strategy,
        db_path.to_path_buf(),
        4096,
    )
    .unwrap();

    scheduler.notify_flush();
    std::thread::sleep(std::time::Duration::from_millis(200));
    scheduler.shutdown().unwrap();

    // L0 should be empty, L1 should have the merged SSTable
    let current = vs.current();
    let v = current.read().unwrap();
    assert_eq!(v.level(0).len(), 0, "L0 should be empty after compaction");
    assert_eq!(v.level(1).len(), 1, "L1 should have the merged SSTable");

    // Merged SSTable should have all 40 keys
    let l1_meta = &v.level(1)[0];
    assert_eq!(l1_meta.entry_count, 40);
    assert_eq!(l1_meta.level, 1);

    // Old SSTable files should be deleted
    for m in &metas {
        let old_path = db_path.join(format!("{:06}.sst", m.id));
        assert!(!old_path.exists(), "old SSTable {} should be deleted", m.id);
    }

    // New SSTable should be readable
    let new_path = db_path.join(format!("{:06}.sst", l1_meta.id));
    assert!(new_path.exists());
    let sst = SSTable::open(&new_path).unwrap();
    assert_eq!(
        sst.get(b"key_00000").unwrap(),
        Some(b"val_00000".to_vec())
    );
    assert_eq!(
        sst.get(b"key_00039").unwrap(),
        Some(b"val_00039".to_vec())
    );
}
