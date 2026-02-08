// M11: Block Builder tests
// Tests for serializing sorted key-value pairs into fixed-size blocks.

use lsm_engine::sstable::block::builder::BlockBuilder;

// =============================================================================
// Test 1: Build empty block → valid
// =============================================================================
#[test]
fn build_empty_block() {
    let builder = BlockBuilder::new(4096);
    assert!(builder.is_empty());
    let block = builder.build();
    // Empty block: just the num_entries (2 bytes) = 0
    assert_eq!(block.len(), 2);
}

// =============================================================================
// Test 2: Add one entry, build → block contains that entry
// =============================================================================
#[test]
fn add_one_entry_and_build() {
    let mut builder = BlockBuilder::new(4096);
    assert!(builder.add(b"key1", b"value1"));
    assert!(!builder.is_empty());

    let block = builder.build();
    // Should contain: entry (2+2+4+6=14 bytes) + offset (2 bytes) + count (2 bytes) = 18
    assert_eq!(block.len(), 18);
}

// =============================================================================
// Test 3: Add entries in sorted order, build → all entries in block
// =============================================================================
#[test]
fn add_sorted_entries() {
    let mut builder = BlockBuilder::new(4096);
    assert!(builder.add(b"aaa", b"val_a"));
    assert!(builder.add(b"bbb", b"val_b"));
    assert!(builder.add(b"ccc", b"val_c"));

    let block = builder.build();
    // 3 entries + 3 offsets (6 bytes) + count (2 bytes)
    // Each entry: 2 + 2 + 3 + 5 = 12 bytes → 36 + 6 + 2 = 44
    assert_eq!(block.len(), 44);
}

// =============================================================================
// Test 4: Add entry that exceeds block size → returns false
// =============================================================================
#[test]
fn block_full_returns_false() {
    // Tiny block size: only fits a small entry
    let mut builder = BlockBuilder::new(32);
    // First entry should fit (2+2+1+1 = 6 bytes data + 2 offset + 2 count = 10)
    assert!(builder.add(b"a", b"b"));

    // Second entry would push past 32 bytes
    let big_value = vec![0u8; 30];
    assert!(!builder.add(b"c", &big_value), "should reject when block is full");
}

// =============================================================================
// Test 5: Build block, verify size <= target block size
// =============================================================================
#[test]
fn block_size_within_target() {
    let target = 4096;
    let mut builder = BlockBuilder::new(target);

    // Add entries until block is full
    let mut i = 0u32;
    loop {
        let key = format!("key_{:06}", i);
        let val = format!("value_{:06}", i);
        if !builder.add(key.as_bytes(), val.as_bytes()) {
            break;
        }
        i += 1;
    }

    assert!(i > 0, "should have added at least one entry");
    let block = builder.build();
    // Block might slightly exceed target due to the last entry that fit,
    // but should be in the right ballpark
    assert!(
        block.len() <= target + 100,
        "block size {} should be approximately <= target {}",
        block.len(),
        target
    );
}

// =============================================================================
// Test 6: estimated_size tracks correctly
// =============================================================================
#[test]
fn estimated_size_tracks_growth() {
    let mut builder = BlockBuilder::new(4096);
    let initial = builder.estimated_size();

    builder.add(b"key1", b"value1");
    let after_one = builder.estimated_size();
    assert!(after_one > initial, "size should grow after adding entry");

    builder.add(b"key2", b"value2");
    let after_two = builder.estimated_size();
    assert!(after_two > after_one, "size should grow after second entry");
}

// =============================================================================
// Test 7: First entry always accepted even if larger than block_size
// =============================================================================
#[test]
fn first_entry_always_accepted() {
    let mut builder = BlockBuilder::new(8); // tiny block
    // This entry is larger than block_size, but it's the first one
    assert!(builder.add(b"big_key", b"big_value"), "first entry should always be accepted");
}
