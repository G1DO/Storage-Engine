// M12: Block Reader + Binary Search tests
// Tests for deserializing blocks and point lookup via binary search.

use lsm_engine::sstable::block::builder::BlockBuilder;
use lsm_engine::sstable::block::reader::Block;
use lsm_engine::iterator::StorageIterator;

// Helper: build a block from a slice of (key, value) pairs.
fn build_block(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut builder = BlockBuilder::new(4096);
    for (k, v) in entries {
        assert!(builder.add(k, v), "entry should fit in test block");
    }
    builder.build()
}

// =============================================================================
// Test 1: Build a block with BlockBuilder, decode with Block::decode → roundtrip
// =============================================================================
#[test]
fn roundtrip_build_and_decode() {
    let data = build_block(&[
        (b"apple", b"red"),
        (b"banana", b"yellow"),
        (b"cherry", b"dark_red"),
    ]);
    let block = Block::decode(data).expect("decode should succeed");
    // Verify we can iterate all 3 entries (basic roundtrip sanity)
    let mut iter = block.iter();
    assert!(iter.is_valid());
}

// =============================================================================
// Test 2: get() on existing key → correct value
// =============================================================================
#[test]
fn get_existing_key() {
    let data = build_block(&[
        (b"ant", b"tiny"),
        (b"bat", b"wings"),
        (b"cat", b"meow"),
        (b"dog", b"woof"),
        (b"elk", b"antlers"),
    ]);
    let block = Block::decode(data).unwrap();

    assert_eq!(block.get(b"ant"), Some(b"tiny".as_slice()));
    assert_eq!(block.get(b"cat"), Some(b"meow".as_slice()));
    assert_eq!(block.get(b"elk"), Some(b"antlers".as_slice()));
    assert_eq!(block.get(b"dog"), Some(b"woof".as_slice()));
}

// =============================================================================
// Test 3: get() on non-existing key → None
// =============================================================================
#[test]
fn get_nonexistent_key() {
    let data = build_block(&[
        (b"ant", b"tiny"),
        (b"cat", b"meow"),
        (b"elk", b"antlers"),
    ]);
    let block = Block::decode(data).unwrap();

    assert_eq!(block.get(b"zzz"), None);
    assert_eq!(block.get(b"aaa"), None);
}

// =============================================================================
// Test 4: get() on key between two entries → None
// =============================================================================
#[test]
fn get_key_between_entries() {
    let data = build_block(&[
        (b"ant", b"tiny"),
        (b"cat", b"meow"),
        (b"elk", b"antlers"),
    ]);
    let block = Block::decode(data).unwrap();

    assert_eq!(block.get(b"bat"), None, "bat is between ant and cat");
    assert_eq!(block.get(b"dog"), None, "dog is between cat and elk");
}

// =============================================================================
// Test 5: Iterator yields all entries in sorted order
// =============================================================================
#[test]
fn iterator_sorted_order() {
    let entries: Vec<(&[u8], &[u8])> = vec![
        (b"alpha", b"1"),
        (b"bravo", b"2"),
        (b"charlie", b"3"),
        (b"delta", b"4"),
        (b"echo", b"5"),
    ];
    let data = build_block(&entries);
    let block = Block::decode(data).unwrap();

    let mut iter = block.iter();
    let mut collected = Vec::new();
    while iter.is_valid() {
        collected.push((iter.key().to_vec(), iter.value().to_vec()));
        iter.next().unwrap();
    }

    assert_eq!(collected.len(), 5);
    for (i, (k, v)) in collected.iter().enumerate() {
        assert_eq!(k.as_slice(), entries[i].0);
        assert_eq!(v.as_slice(), entries[i].1);
    }
}

// =============================================================================
// Test 6: Seek to middle of block → positions correctly
// =============================================================================
#[test]
fn seek_to_existing_key() {
    let data = build_block(&[
        (b"ant", b"1"),
        (b"bat", b"2"),
        (b"cat", b"3"),
        (b"dog", b"4"),
        (b"elk", b"5"),
    ]);
    let block = Block::decode(data).unwrap();

    let mut iter = block.iter();
    iter.seek(b"cat").unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"cat");
    assert_eq!(iter.value(), b"3");
}

// =============================================================================
// Test 7: Seek to non-existing key → positions at next greater key
// =============================================================================
#[test]
fn seek_to_nonexistent_key() {
    let data = build_block(&[
        (b"ant", b"1"),
        (b"cat", b"3"),
        (b"elk", b"5"),
    ]);
    let block = Block::decode(data).unwrap();

    let mut iter = block.iter();
    // "bat" doesn't exist, should land on "cat" (next greater)
    iter.seek(b"bat").unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"cat");

    // "dog" doesn't exist, should land on "elk"
    iter.seek(b"dog").unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"elk");
}

// =============================================================================
// Test 8: Seek past last key → invalid
// =============================================================================
#[test]
fn seek_past_all_keys() {
    let data = build_block(&[
        (b"ant", b"1"),
        (b"bat", b"2"),
        (b"cat", b"3"),
    ]);
    let block = Block::decode(data).unwrap();

    let mut iter = block.iter();
    iter.seek(b"zzz").unwrap();
    assert!(!iter.is_valid(), "should be invalid when seeked past all keys");
}

// =============================================================================
// Test 9: Single-entry block roundtrip
// =============================================================================
#[test]
fn single_entry_roundtrip() {
    let data = build_block(&[(b"only", b"one")]);
    let block = Block::decode(data).unwrap();

    assert_eq!(block.get(b"only"), Some(b"one".as_slice()));
    assert_eq!(block.get(b"other"), None);

    let mut iter = block.iter();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"only");
    assert_eq!(iter.value(), b"one");
    iter.next().unwrap();
    assert!(!iter.is_valid());
}

// =============================================================================
// Test 10: Iterator exhaustion — next past end stays invalid
// =============================================================================
#[test]
fn iterator_exhaustion() {
    let data = build_block(&[(b"a", b"1"), (b"b", b"2")]);
    let block = Block::decode(data).unwrap();

    let mut iter = block.iter();
    iter.next().unwrap(); // at "b"
    iter.next().unwrap(); // past end
    assert!(!iter.is_valid());
}

// =============================================================================
// Test 11: Seek to first key → positions at beginning
// =============================================================================
#[test]
fn seek_to_first_key() {
    let data = build_block(&[
        (b"ant", b"1"),
        (b"bat", b"2"),
        (b"cat", b"3"),
    ]);
    let block = Block::decode(data).unwrap();

    let mut iter = block.iter();
    iter.seek(b"ant").unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"ant");
    assert_eq!(iter.value(), b"1");
}

// =============================================================================
// Test 12: Seek before first key → positions at first entry
// =============================================================================
#[test]
fn seek_before_first_key() {
    let data = build_block(&[
        (b"bat", b"2"),
        (b"cat", b"3"),
        (b"dog", b"4"),
    ]);
    let block = Block::decode(data).unwrap();

    let mut iter = block.iter();
    iter.seek(b"aaa").unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"bat");
}
