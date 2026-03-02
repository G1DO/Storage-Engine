use std::sync::Arc;

use lsm_engine::manifest::version::{Version, VersionSet};
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
fn version_new_creates_empty_levels() {
    let v = Version::new(4);
    assert_eq!(v.levels.len(), 4);
    for level in &v.levels {
        assert!(level.is_empty());
    }
}

#[test]
fn version_level_returns_correct_slice() {
    let mut v = Version::new(3);
    v.levels[0].push(make_sst(1, 0, b"a", b"z"));
    assert_eq!(v.level(0).len(), 1);
    assert_eq!(v.level(1).len(), 0);
}

#[test]
fn version_total_sstables_sums_all_levels() {
    let mut v = Version::new(3);
    v.levels[0].push(make_sst(1, 0, b"a", b"m"));
    v.levels[0].push(make_sst(2, 0, b"n", b"z"));
    v.levels[1].push(make_sst(3, 1, b"a", b"z"));
    assert_eq!(v.total_sstables(), 3);
}

#[test]
fn version_total_sstables_zero_when_empty() {
    let v = Version::new(7);
    assert_eq!(v.total_sstables(), 0);
}

// --- VersionSet tests ---

#[test]
fn version_set_starts_empty() {
    let vs = VersionSet::new(4);
    let current = vs.current();
    let v = current.read().unwrap();
    assert_eq!(v.total_sstables(), 0);
    assert_eq!(v.levels.len(), 4);
}

#[test]
fn version_set_install_replaces_version() {
    let vs = VersionSet::new(4);
    let mut new_v = Version::new(4);
    new_v.levels[0].push(make_sst(1, 0, b"a", b"z"));
    vs.install(new_v);

    let current = vs.current();
    let v = current.read().unwrap();
    assert_eq!(v.total_sstables(), 1);
}

#[test]
fn version_set_sst_ids_increment() {
    let vs = VersionSet::new(4);
    assert_eq!(vs.next_sst_id(), 1);
    assert_eq!(vs.next_sst_id(), 2);
    assert_eq!(vs.next_sst_id(), 3);
}

#[test]
fn version_set_shared_across_threads() {
    let vs = Arc::new(VersionSet::new(4));
    let vs2 = Arc::clone(&vs);

    let handle = std::thread::spawn(move || {
        let mut new_v = Version::new(4);
        new_v.levels[0].push(make_sst(1, 0, b"a", b"z"));
        vs2.install(new_v);
    });
    handle.join().unwrap();

    let current = vs.current();
    let v = current.read().unwrap();
    assert_eq!(v.total_sstables(), 1);
}
