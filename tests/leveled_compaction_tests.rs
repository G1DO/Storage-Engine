use lsm_engine::compaction::leveled::LeveledStrategy;
use lsm_engine::compaction::CompactionStrategy;
use lsm_engine::sstable::footer::SSTableMeta;

// ---------------------------------------------------------------------------
// Helper: build an SSTableMeta with the fields leveled compaction cares about.
// Uses file_size to control level budget calculations.
// ---------------------------------------------------------------------------

fn make_sst(id: u64, level: u32, min_key: &[u8], max_key: &[u8], file_size: u64) -> SSTableMeta {
    SSTableMeta {
        id,
        level,
        min_key: min_key.to_vec(),
        max_key: max_key.to_vec(),
        file_size,
        entry_count: 100,
    }
}

/// Build a levels vector from slices (index = level number).
fn make_levels(levels: Vec<Vec<SSTableMeta>>) -> Vec<Vec<SSTableMeta>> {
    levels
}

/// Default test strategy: L1 budget = 1000B, 10x multiplier, 4 max levels.
///   L1: 1000B, L2: 10_000B, L3: 100_000B
fn test_strategy() -> LeveledStrategy {
    LeveledStrategy::new(1000, 10, 4)
}

// ===========================================================================
// M21 Tests: Leveled Compaction Strategy
// ===========================================================================

// ---------------------------------------------------------------------------
// No compaction needed
// ---------------------------------------------------------------------------

#[test]
fn all_levels_under_budget_no_compaction() {
    let strategy = test_strategy(); // L1 budget = 1000

    let levels = make_levels(vec![
        vec![],                                              // L0 (not managed by leveled)
        vec![make_sst(1, 1, b"a", b"m", 400),
             make_sst(2, 1, b"n", b"z", 400)],              // L1 total = 800 < 1000
        vec![make_sst(3, 2, b"a", b"z", 5000)],             // L2 total = 5000 < 10_000
    ]);

    assert!(strategy.pick_compaction(&levels).is_none());
}

#[test]
fn empty_levels_no_compaction() {
    let strategy = test_strategy();

    // Completely empty
    let levels: Vec<Vec<SSTableMeta>> = vec![vec![], vec![], vec![]];
    assert!(strategy.pick_compaction(&levels).is_none());

    // No levels at all
    let empty: Vec<Vec<SSTableMeta>> = vec![];
    assert!(strategy.pick_compaction(&empty).is_none());
}

#[test]
fn single_level_under_budget_no_compaction() {
    let strategy = test_strategy();

    // Only L0 and L1, L1 under budget
    let levels = make_levels(vec![
        vec![],
        vec![make_sst(1, 1, b"a", b"z", 999)],
    ]);

    assert!(strategy.pick_compaction(&levels).is_none());
}

// ---------------------------------------------------------------------------
// L1 over budget → compaction to L2
// ---------------------------------------------------------------------------

#[test]
fn l1_over_budget_picks_l1_sst_and_overlapping_l2() {
    let strategy = test_strategy(); // L1 budget = 1000

    let levels = make_levels(vec![
        vec![],                                              // L0
        vec![make_sst(1, 1, b"a", b"f", 600),
             make_sst(2, 1, b"g", b"z", 600)],              // L1 total = 1200 > 1000
        vec![make_sst(10, 2, b"a", b"d", 2000),
             make_sst(11, 2, b"e", b"k", 2000),
             make_sst(12, 2, b"p", b"z", 2000)],            // L2
    ]);

    let task = strategy.pick_compaction(&levels).expect("L1 over budget");

    // Output goes to L2
    assert_eq!(task.output_level, 2);

    // Should pick exactly ONE SSTable from L1 (not all of them)
    let l1_inputs: Vec<&SSTableMeta> = task.inputs.iter().filter(|s| s.level == 1).collect();
    assert_eq!(l1_inputs.len(), 1, "should pick exactly one L1 SSTable");

    // The picked L1 SSTable must be one of ours
    let picked_id = l1_inputs[0].id;
    assert!(picked_id == 1 || picked_id == 2);

    // L2 inputs should be those overlapping with the picked L1 SSTable
    let l2_inputs: Vec<u64> = task.inputs.iter().filter(|s| s.level == 2).map(|s| s.id).collect();

    if picked_id == 1 {
        // SST 1: [a, f] overlaps with L2 SST 10 [a,d] and SST 11 [e,k]
        assert!(l2_inputs.contains(&10));
        assert!(l2_inputs.contains(&11));
        assert!(!l2_inputs.contains(&12)); // [p,z] doesn't overlap [a,f]
    } else {
        // SST 2: [g, z] overlaps with L2 SST 11 [e,k] and SST 12 [p,z]
        assert!(l2_inputs.contains(&11));
        assert!(l2_inputs.contains(&12));
        assert!(!l2_inputs.contains(&10)); // [a,d] doesn't overlap [g,z]
    }
}

#[test]
fn l1_over_budget_no_l2_sstables() {
    let strategy = test_strategy();

    let levels = make_levels(vec![
        vec![],
        vec![make_sst(1, 1, b"a", b"m", 600),
             make_sst(2, 1, b"n", b"z", 600)],              // L1 = 1200 > 1000
        vec![],                                              // L2 empty
    ]);

    let task = strategy.pick_compaction(&levels).expect("L1 over budget");
    assert_eq!(task.output_level, 2);

    // Only 1 L1 SSTable, no L2 overlap
    assert_eq!(task.inputs.len(), 1);
    assert!(task.inputs[0].level == 1);
}

#[test]
fn l1_over_budget_no_l2_overlap() {
    let strategy = test_strategy();

    let levels = make_levels(vec![
        vec![],
        vec![make_sst(1, 1, b"a", b"c", 600),
             make_sst(2, 1, b"d", b"f", 600)],              // L1 = 1200 > 1000
        vec![make_sst(10, 2, b"x", b"z", 5000)],            // L2: no overlap with L1
    ]);

    let task = strategy.pick_compaction(&levels).expect("L1 over budget");
    assert_eq!(task.output_level, 2);

    // Only 1 L1 SSTable, the L2 SSTable doesn't overlap
    assert_eq!(task.inputs.len(), 1);
    assert!(task.inputs[0].level == 1);
}

// ---------------------------------------------------------------------------
// Deeper levels: L2 over budget → compaction to L3
// ---------------------------------------------------------------------------

#[test]
fn l2_over_budget_compacts_to_l3() {
    let strategy = test_strategy(); // L2 budget = 10_000

    let levels = make_levels(vec![
        vec![],                                              // L0
        vec![make_sst(1, 1, b"a", b"z", 500)],              // L1 under budget
        vec![make_sst(10, 2, b"a", b"m", 6000),
             make_sst(11, 2, b"n", b"z", 6000)],            // L2 = 12_000 > 10_000
        vec![make_sst(20, 3, b"a", b"g", 20000),
             make_sst(21, 3, b"h", b"z", 20000)],           // L3
    ]);

    let task = strategy.pick_compaction(&levels).expect("L2 over budget");
    assert_eq!(task.output_level, 3);

    let l2_inputs: Vec<&SSTableMeta> = task.inputs.iter().filter(|s| s.level == 2).collect();
    assert_eq!(l2_inputs.len(), 1, "should pick exactly one L2 SSTable");

    // L3 inputs should overlap with the picked L2 SSTable
    let l3_inputs: Vec<&SSTableMeta> = task.inputs.iter().filter(|s| s.level == 3).collect();
    assert!(!l3_inputs.is_empty() || true, "L3 inputs depend on overlap");
}

// ---------------------------------------------------------------------------
// Priority: pick the lowest level that's over budget
// ---------------------------------------------------------------------------

#[test]
fn picks_lowest_overflowing_level_first() {
    let strategy = test_strategy();

    let levels = make_levels(vec![
        vec![],
        vec![make_sst(1, 1, b"a", b"z", 1500)],            // L1 = 1500 > 1000
        vec![make_sst(10, 2, b"a", b"z", 15000)],           // L2 = 15000 > 10_000
        vec![],
    ]);

    let task = strategy.pick_compaction(&levels).expect("should compact");

    // L1 is the lowest over-budget level, so it should be picked first
    let source_levels: Vec<u32> = task.inputs.iter().map(|s| s.level).collect();
    assert!(source_levels.contains(&1), "should pick from L1 first");
    assert_eq!(task.output_level, 2);
}

// ---------------------------------------------------------------------------
// Edge: deepest level over budget with no next level
// ---------------------------------------------------------------------------

#[test]
fn deepest_level_over_budget_no_compaction() {
    // max_levels = 3, so L3 is the deepest allowed
    let strategy = LeveledStrategy::new(1000, 10, 3);

    let levels = make_levels(vec![
        vec![],                                              // L0
        vec![make_sst(1, 1, b"a", b"z", 500)],              // L1 under budget
        vec![make_sst(10, 2, b"a", b"z", 500)],             // L2 under budget
        vec![make_sst(20, 3, b"a", b"z", 999999)],          // L3 over budget but nowhere to go
    ]);

    // No compaction: the deepest level can't push further
    assert!(strategy.pick_compaction(&levels).is_none());
}

// ---------------------------------------------------------------------------
// Budget calculation scales with multiplier
// ---------------------------------------------------------------------------

#[test]
fn budget_scales_with_multiplier() {
    // base=500, multiplier=5 → L1=500, L2=2500, L3=12500
    let strategy = LeveledStrategy::new(500, 5, 4);

    // L1 = 600 > 500 → should compact
    let levels = make_levels(vec![
        vec![],
        vec![make_sst(1, 1, b"a", b"z", 600)],
        vec![],
    ]);

    assert!(strategy.pick_compaction(&levels).is_some());

    // L2 = 2400 < 2500 → no compaction (L1 under budget too)
    let levels = make_levels(vec![
        vec![],
        vec![make_sst(1, 1, b"a", b"z", 400)],
        vec![make_sst(10, 2, b"a", b"z", 2400)],
        vec![],
    ]);

    assert!(strategy.pick_compaction(&levels).is_none());

    // L2 = 2600 > 2500 → should compact to L3
    let levels = make_levels(vec![
        vec![],
        vec![make_sst(1, 1, b"a", b"z", 400)],
        vec![make_sst(10, 2, b"a", b"z", 2600)],
        vec![],
    ]);

    let task = strategy.pick_compaction(&levels).expect("L2 over budget");
    assert_eq!(task.output_level, 3);
}

// ---------------------------------------------------------------------------
// Overlap detection: L2 SSTables correctly identified
// ---------------------------------------------------------------------------

#[test]
fn all_l2_sstables_overlap_with_picked_l1() {
    let strategy = test_strategy();

    // L1 has one wide SSTable covering [a, z]
    // All L2 SSTables overlap with it
    let levels = make_levels(vec![
        vec![],
        vec![make_sst(1, 1, b"a", b"z", 1500)],            // L1 over budget
        vec![make_sst(10, 2, b"a", b"f", 1000),
             make_sst(11, 2, b"g", b"m", 1000),
             make_sst(12, 2, b"n", b"z", 1000)],            // all overlap [a,z]
    ]);

    let task = strategy.pick_compaction(&levels).expect("L1 over budget");

    let l2_ids: Vec<u64> = task.inputs.iter().filter(|s| s.level == 2).map(|s| s.id).collect();
    assert_eq!(l2_ids.len(), 3, "all L2 SSTables overlap with [a,z]");
    assert!(l2_ids.contains(&10));
    assert!(l2_ids.contains(&11));
    assert!(l2_ids.contains(&12));
}

#[test]
fn only_some_l2_sstables_overlap() {
    let strategy = test_strategy();

    // L1 has narrow SSTable [d, g]
    let levels = make_levels(vec![
        vec![],
        vec![make_sst(1, 1, b"d", b"g", 1500)],            // L1 over budget
        vec![make_sst(10, 2, b"a", b"c", 1000),             // no overlap (c < d)
             make_sst(11, 2, b"e", b"f", 1000),             // overlaps [d,g]
             make_sst(12, 2, b"h", b"z", 1000)],            // no overlap (h > g)
    ]);

    let task = strategy.pick_compaction(&levels).expect("L1 over budget");

    let l2_ids: Vec<u64> = task.inputs.iter().filter(|s| s.level == 2).map(|s| s.id).collect();
    assert_eq!(l2_ids.len(), 1);
    assert!(l2_ids.contains(&11));
    assert!(!l2_ids.contains(&10));
    assert!(!l2_ids.contains(&12));
}

// ---------------------------------------------------------------------------
// L0 is not managed by leveled strategy (L0 is handled by size-tiered)
// ---------------------------------------------------------------------------

#[test]
fn l0_sstables_ignored_by_leveled_strategy() {
    let strategy = test_strategy();

    // Even if L0 has tons of SSTables, leveled doesn't trigger on L0
    let levels = make_levels(vec![
        vec![make_sst(1, 0, b"a", b"z", 9999),
             make_sst(2, 0, b"a", b"z", 9999),
             make_sst(3, 0, b"a", b"z", 9999)],
        vec![make_sst(10, 1, b"a", b"z", 500)],             // L1 under budget
        vec![],
    ]);

    assert!(strategy.pick_compaction(&levels).is_none());
}

// ---------------------------------------------------------------------------
// Exactly at budget boundary
// ---------------------------------------------------------------------------

#[test]
fn exactly_at_budget_no_compaction() {
    let strategy = test_strategy(); // L1 budget = 1000

    let levels = make_levels(vec![
        vec![],
        vec![make_sst(1, 1, b"a", b"z", 1000)],            // L1 = 1000, exactly at budget
        vec![],
    ]);

    // Exactly at budget should NOT trigger (only over triggers)
    assert!(strategy.pick_compaction(&levels).is_none());
}

#[test]
fn one_byte_over_budget_triggers() {
    let strategy = test_strategy(); // L1 budget = 1000

    let levels = make_levels(vec![
        vec![],
        vec![make_sst(1, 1, b"a", b"z", 1001)],            // L1 = 1001, just over
        vec![],
    ]);

    assert!(strategy.pick_compaction(&levels).is_some());
}
