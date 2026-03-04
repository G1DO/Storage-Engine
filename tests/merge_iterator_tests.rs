use lsm_engine::error::Result;
use lsm_engine::iterator::StorageIterator;
use lsm_engine::iterator::merge::MergeIterator;

// ---------------------------------------------------------------------------
// Test helper: a simple in-memory iterator over sorted (key, value) pairs.
// ---------------------------------------------------------------------------

struct VecIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
}

impl VecIterator {
    fn new(entries: Vec<(&[u8], &[u8])>) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect(),
            pos: 0,
        }
    }
}

impl StorageIterator for VecIterator {
    fn key(&self) -> &[u8] {
        &self.entries[self.pos].0
    }

    fn value(&self) -> &[u8] {
        &self.entries[self.pos].1
    }

    fn is_valid(&self) -> bool {
        self.pos < self.entries.len()
    }

    fn next(&mut self) -> Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn seek(&mut self, key: &[u8]) -> Result<()> {
        self.pos = self.entries.partition_point(|(k, _)| k.as_slice() < key);
        Ok(())
    }
}

/// Collect all (key, value) pairs from a MergeIterator into a Vec.
fn collect_all(iter: &mut MergeIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut result = Vec::new();
    while iter.is_valid() {
        result.push((iter.key().to_vec(), iter.value().to_vec()));
        iter.next().unwrap();
    }
    result
}

// ===========================================================================
// M19 Tests: K-Way Merge Sort
// ===========================================================================

#[test]
fn merge_two_sorted_sequences_no_overlap() {
    // [1, 3, 5] and [2, 4, 6] → [1, 2, 3, 4, 5, 6]
    let iter1 = VecIterator::new(vec![(b"1", b"v1"), (b"3", b"v3"), (b"5", b"v5")]);
    let iter2 = VecIterator::new(vec![(b"2", b"v2"), (b"4", b"v4"), (b"6", b"v6")]);

    let iters: Vec<Box<dyn StorageIterator>> = vec![Box::new(iter1), Box::new(iter2)];
    let mut merge = MergeIterator::new(iters).unwrap();

    let result = collect_all(&mut merge);
    let keys: Vec<&[u8]> = result.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"1", b"2", b"3", b"4", b"5", b"6"]);
}

#[test]
fn merge_with_duplicate_keys_keeps_newest() {
    // Index 0 = newest. [a=1, c=3] and [a=2, b=2] → [a=1(newest), b=2, c=3]
    let iter_newer = VecIterator::new(vec![(b"a", b"1"), (b"c", b"3")]);
    let iter_older = VecIterator::new(vec![(b"a", b"2"), (b"b", b"2")]);

    let iters: Vec<Box<dyn StorageIterator>> = vec![Box::new(iter_newer), Box::new(iter_older)];
    let mut merge = MergeIterator::new(iters).unwrap();

    let result = collect_all(&mut merge);
    assert_eq!(
        result,
        vec![
            (b"a".to_vec(), b"1".to_vec()), // from newer (index 0)
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]
    );
}

#[test]
fn merge_three_iterators() {
    let iter0 = VecIterator::new(vec![(b"b", b"0"), (b"d", b"0")]);
    let iter1 = VecIterator::new(vec![(b"a", b"1"), (b"c", b"1")]);
    let iter2 = VecIterator::new(vec![(b"c", b"2"), (b"e", b"2")]);

    let iters: Vec<Box<dyn StorageIterator>> =
        vec![Box::new(iter0), Box::new(iter1), Box::new(iter2)];
    let mut merge = MergeIterator::new(iters).unwrap();

    let result = collect_all(&mut merge);
    let keys: Vec<&[u8]> = result.iter().map(|(k, _)| k.as_slice()).collect();
    let values: Vec<&[u8]> = result.iter().map(|(_, v)| v.as_slice()).collect();

    assert_eq!(keys, vec![b"a", b"b", b"c", b"d", b"e"]);
    // c comes from iter1 (index 1, newer than iter2 at index 2)
    assert_eq!(values[2], b"1");
}

#[test]
fn merge_one_empty_one_nonempty() {
    let empty = VecIterator::new(vec![]);
    let nonempty = VecIterator::new(vec![(b"x", b"1"), (b"y", b"2")]);

    let iters: Vec<Box<dyn StorageIterator>> = vec![Box::new(empty), Box::new(nonempty)];
    let mut merge = MergeIterator::new(iters).unwrap();

    let result = collect_all(&mut merge);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, b"x");
    assert_eq!(result[1].0, b"y");
}

#[test]
fn merge_all_empty() {
    let e1 = VecIterator::new(vec![]);
    let e2 = VecIterator::new(vec![]);
    let e3 = VecIterator::new(vec![]);

    let iters: Vec<Box<dyn StorageIterator>> = vec![Box::new(e1), Box::new(e2), Box::new(e3)];
    let mut merge = MergeIterator::new(iters).unwrap();

    assert!(!merge.is_valid());
    let result = collect_all(&mut merge);
    assert!(result.is_empty());
}

#[test]
fn merge_large_ten_iterators() {
    // 10 iterators, 100 entries each, keys are formatted "key_{:05}"
    // Even-indexed iterators cover even keys, odd-indexed cover odd keys,
    // with some overlap to test dedup.
    let mut iters: Vec<Box<dyn StorageIterator>> = Vec::new();

    for i in 0..10 {
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for j in 0..100 {
            let key = format!("key_{:05}", i * 100 + j);
            let value = format!("val_iter{}_entry{}", i, j);
            entries.push((key.into_bytes(), value.into_bytes()));
        }
        iters.push(Box::new(VecIterator { entries, pos: 0 }));
    }

    let mut merge = MergeIterator::new(iters).unwrap();
    let result = collect_all(&mut merge);

    // All 1000 keys are unique (no overlap in this case)
    assert_eq!(result.len(), 1000);

    // Verify sorted order
    for i in 1..result.len() {
        assert!(
            result[i - 1].0 < result[i].0,
            "keys not sorted at index {}",
            i
        );
    }
}

#[test]
fn merge_large_with_duplicates() {
    // 5 iterators all containing the same 200 keys.
    // Iterator 0 (newest) values should win for all keys.
    let mut iters: Vec<Box<dyn StorageIterator>> = Vec::new();

    for iter_idx in 0..5 {
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for j in 0..200 {
            let key = format!("key_{:05}", j);
            let value = format!("from_iter_{}", iter_idx);
            entries.push((key.into_bytes(), value.into_bytes()));
        }
        iters.push(Box::new(VecIterator { entries, pos: 0 }));
    }

    let mut merge = MergeIterator::new(iters).unwrap();
    let result = collect_all(&mut merge);

    // Deduplication: only 200 unique keys
    assert_eq!(result.len(), 200);

    // All values should be from iterator 0 (the newest)
    for (_, v) in &result {
        assert_eq!(v, b"from_iter_0");
    }
}

#[test]
fn merge_tombstones_are_kept() {
    // M19: tombstones must NOT be filtered — compaction keeps them.
    // We represent a tombstone as a value with a special marker byte.
    let tombstone: &[u8] = b"TOMBSTONE";

    let iter_newer = VecIterator::new(vec![
        (b"a", tombstone), // delete(a) in newer source
        (b"c", b"val_c"),
    ]);
    let iter_older = VecIterator::new(vec![
        (b"a", b"old_a"), // old value for a
        (b"b", b"val_b"),
    ]);

    let iters: Vec<Box<dyn StorageIterator>> = vec![Box::new(iter_newer), Box::new(iter_older)];
    let mut merge = MergeIterator::new(iters).unwrap();

    let result = collect_all(&mut merge);
    assert_eq!(result.len(), 3);

    // a's value is the tombstone from the newer source — NOT filtered out
    assert_eq!(result[0], (b"a".to_vec(), b"TOMBSTONE".to_vec()));
    assert_eq!(result[1], (b"b".to_vec(), b"val_b".to_vec()));
    assert_eq!(result[2], (b"c".to_vec(), b"val_c".to_vec()));
}

#[test]
fn merge_single_iterator() {
    let iter = VecIterator::new(vec![(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);

    let iters: Vec<Box<dyn StorageIterator>> = vec![Box::new(iter)];
    let mut merge = MergeIterator::new(iters).unwrap();

    let result = collect_all(&mut merge);
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].0, b"a");
    assert_eq!(result[2].0, b"c");
}

#[test]
fn merge_seek_positions_correctly() {
    let iter0 = VecIterator::new(vec![(b"a", b"0"), (b"c", b"0"), (b"e", b"0")]);
    let iter1 = VecIterator::new(vec![(b"b", b"1"), (b"d", b"1"), (b"f", b"1")]);

    let iters: Vec<Box<dyn StorageIterator>> = vec![Box::new(iter0), Box::new(iter1)];
    let mut merge = MergeIterator::new(iters).unwrap();

    // Seek to "c"
    merge.seek(b"c").unwrap();
    assert!(merge.is_valid());
    assert_eq!(merge.key(), b"c");

    // Continue iterating from seek point
    let result = collect_all(&mut merge);
    let keys: Vec<&[u8]> = result.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"c", b"d", b"e", b"f"]);
}

#[test]
fn merge_seek_to_nonexistent_key() {
    let iter0 = VecIterator::new(vec![(b"a", b"0"), (b"d", b"0")]);
    let iter1 = VecIterator::new(vec![(b"b", b"1"), (b"e", b"1")]);

    let iters: Vec<Box<dyn StorageIterator>> = vec![Box::new(iter0), Box::new(iter1)];
    let mut merge = MergeIterator::new(iters).unwrap();

    // Seek to "c" — doesn't exist, should land on "d"
    merge.seek(b"c").unwrap();
    assert!(merge.is_valid());
    assert_eq!(merge.key(), b"d");
}

#[test]
fn merge_seek_past_all_keys() {
    let iter = VecIterator::new(vec![(b"a", b"1"), (b"b", b"2")]);
    let iters: Vec<Box<dyn StorageIterator>> = vec![Box::new(iter)];
    let mut merge = MergeIterator::new(iters).unwrap();

    merge.seek(b"z").unwrap();
    assert!(!merge.is_valid());
}

#[test]
fn merge_duplicate_key_across_three_sources() {
    // Same key "x" in all three. Index 0 = newest, should win.
    let i0 = VecIterator::new(vec![(b"x", b"newest")]);
    let i1 = VecIterator::new(vec![(b"x", b"middle")]);
    let i2 = VecIterator::new(vec![(b"x", b"oldest")]);

    let iters: Vec<Box<dyn StorageIterator>> = vec![Box::new(i0), Box::new(i1), Box::new(i2)];
    let mut merge = MergeIterator::new(iters).unwrap();

    assert!(merge.is_valid());
    assert_eq!(merge.key(), b"x");
    assert_eq!(merge.value(), b"newest");

    merge.next().unwrap();
    assert!(!merge.is_valid());
}

#[test]
fn merge_no_iterators() {
    let iters: Vec<Box<dyn StorageIterator>> = vec![];
    let mut merge = MergeIterator::new(iters).unwrap();

    assert!(!merge.is_valid());
    assert!(collect_all(&mut merge).is_empty());
}
