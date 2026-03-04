use lsm_engine::compaction::CompactionStrategy;
use lsm_engine::compaction::size_tiered::SizeTieredStrategy;
use lsm_engine::sstable::footer::SSTableMeta;

// ---------------------------------------------------------------------------
// Helper: build an SSTableMeta with just the fields we care about.
// ---------------------------------------------------------------------------

fn make_sst(id: u64, level: u32, min_key: &[u8], max_key: &[u8]) -> SSTableMeta {
    SSTableMeta {
        id,
        level,
        min_key: min_key.to_vec(),
        max_key: max_key.to_vec(),
        file_size: 1024,
        entry_count: 100,
    }
}

/// Build a levels vec with a given L0 and L1.
fn make_levels(l0: Vec<SSTableMeta>, l1: Vec<SSTableMeta>) -> Vec<Vec<SSTableMeta>> {
    vec![l0, l1]
}

// ===========================================================================
// M20 Tests: Size-Tiered Compaction Strategy
// ===========================================================================

#[test]
fn below_threshold_no_compaction() {
    let strategy = SizeTieredStrategy::new(4);
    let levels = make_levels(
        vec![
            make_sst(1, 0, b"a", b"d"),
            make_sst(2, 0, b"e", b"h"),
            make_sst(3, 0, b"i", b"l"),
        ],
        vec![],
    );

    assert!(strategy.pick_compaction(&levels).is_none());
}

#[test]
fn at_threshold_triggers_compaction_no_l1() {
    let strategy = SizeTieredStrategy::new(4);
    let levels = make_levels(
        vec![
            make_sst(1, 0, b"a", b"d"),
            make_sst(2, 0, b"e", b"h"),
            make_sst(3, 0, b"i", b"l"),
            make_sst(4, 0, b"m", b"p"),
        ],
        vec![], // no L1 SSTables
    );

    let task = strategy
        .pick_compaction(&levels)
        .expect("should trigger compaction");

    // All 4 L0 SSTables should be in inputs
    assert_eq!(task.inputs.len(), 4);
    assert_eq!(task.output_level, 1);

    // Verify all L0 SSTable ids are present
    let ids: Vec<u64> = task.inputs.iter().map(|s| s.id).collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&2));
    assert!(ids.contains(&3));
    assert!(ids.contains(&4));
}

#[test]
fn at_threshold_includes_overlapping_l1() {
    let strategy = SizeTieredStrategy::new(4);

    // L0 covers [a, p] overall
    // L1 has [a-f] (overlaps), [g-n] (overlaps), [o-z] (overlaps with max_key=p)
    let levels = make_levels(
        vec![
            make_sst(1, 0, b"a", b"d"),
            make_sst(2, 0, b"e", b"h"),
            make_sst(3, 0, b"i", b"l"),
            make_sst(4, 0, b"m", b"p"),
        ],
        vec![
            make_sst(10, 1, b"a", b"f"),
            make_sst(11, 1, b"g", b"n"),
            make_sst(12, 1, b"o", b"z"),
        ],
    );

    let task = strategy
        .pick_compaction(&levels)
        .expect("should trigger compaction");

    // 4 L0 + 3 overlapping L1 = 7 total
    assert_eq!(task.inputs.len(), 7);
    assert_eq!(task.output_level, 1);

    let ids: Vec<u64> = task.inputs.iter().map(|s| s.id).collect();
    assert!(ids.contains(&10));
    assert!(ids.contains(&11));
    assert!(ids.contains(&12));
}

#[test]
fn no_l1_overlap_only_l0_in_inputs() {
    let strategy = SizeTieredStrategy::new(4);

    // L0 covers [a, d], L1 has [x, z] — no overlap
    let levels = make_levels(
        vec![
            make_sst(1, 0, b"a", b"b"),
            make_sst(2, 0, b"b", b"c"),
            make_sst(3, 0, b"c", b"d"),
            make_sst(4, 0, b"a", b"d"),
        ],
        vec![make_sst(10, 1, b"x", b"z")],
    );

    let task = strategy
        .pick_compaction(&levels)
        .expect("should trigger compaction");

    // Only 4 L0, the L1 SSTable doesn't overlap
    assert_eq!(task.inputs.len(), 4);
    assert_eq!(task.output_level, 1);

    let ids: Vec<u64> = task.inputs.iter().map(|s| s.id).collect();
    assert!(!ids.contains(&10));
}

#[test]
fn partial_l1_overlap() {
    let strategy = SizeTieredStrategy::new(4);

    // L0 overall range: [d, m]
    // L1: [a-f] overlaps (f >= d && a <= m), [g-n] overlaps, [o-z] does NOT (o > m)
    let levels = make_levels(
        vec![
            make_sst(1, 0, b"d", b"f"),
            make_sst(2, 0, b"e", b"h"),
            make_sst(3, 0, b"g", b"j"),
            make_sst(4, 0, b"k", b"m"),
        ],
        vec![
            make_sst(10, 1, b"a", b"f"),
            make_sst(11, 1, b"g", b"n"),
            make_sst(12, 1, b"o", b"z"),
        ],
    );

    let task = strategy
        .pick_compaction(&levels)
        .expect("should trigger compaction");

    // 4 L0 + 2 overlapping L1 = 6
    assert_eq!(task.inputs.len(), 6);
    assert_eq!(task.output_level, 1);

    let ids: Vec<u64> = task.inputs.iter().map(|s| s.id).collect();
    assert!(ids.contains(&10)); // [a-f] overlaps
    assert!(ids.contains(&11)); // [g-n] overlaps
    assert!(!ids.contains(&12)); // [o-z] does NOT overlap
}

#[test]
fn empty_levels_no_compaction() {
    let strategy = SizeTieredStrategy::new(4);

    // Completely empty
    let levels: Vec<Vec<SSTableMeta>> = vec![vec![], vec![]];
    assert!(strategy.pick_compaction(&levels).is_none());

    // No levels at all
    let empty: Vec<Vec<SSTableMeta>> = vec![];
    assert!(strategy.pick_compaction(&empty).is_none());
}

#[test]
fn threshold_of_one() {
    let strategy = SizeTieredStrategy::new(1);
    let levels = make_levels(vec![make_sst(1, 0, b"a", b"z")], vec![]);

    let task = strategy
        .pick_compaction(&levels)
        .expect("threshold=1 should trigger");
    assert_eq!(task.inputs.len(), 1);
    assert_eq!(task.output_level, 1);
}

#[test]
fn above_threshold_also_triggers() {
    let strategy = SizeTieredStrategy::new(2);
    let levels = make_levels(
        vec![
            make_sst(1, 0, b"a", b"c"),
            make_sst(2, 0, b"d", b"f"),
            make_sst(3, 0, b"g", b"i"),
            make_sst(4, 0, b"j", b"l"),
            make_sst(5, 0, b"m", b"o"),
        ],
        vec![],
    );

    let task = strategy
        .pick_compaction(&levels)
        .expect("above threshold should trigger");
    // All 5 L0 SSTables included
    assert_eq!(task.inputs.len(), 5);
}
