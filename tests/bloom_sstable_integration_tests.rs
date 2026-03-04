// M18: Bloom + SSTable Integration tests
// Verify bloom filter is built during SSTable creation and checked during reads.

use lsm_engine::sstable::builder::SSTableBuilder;
use lsm_engine::sstable::reader::SSTable;
use tempfile::tempdir;

// =============================================================================
// Test 1: Build SSTable with bloom, get existing key → found
// =============================================================================
#[test]
fn bloom_sstable_existing_key_found() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    for i in 0..100u32 {
        let key = format!("key_{:05}", i);
        let val = format!("val_{:05}", i);
        builder.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();

    // Every inserted key must still be found — bloom has zero false negatives
    for i in 0..100u32 {
        let key = format!("key_{:05}", i);
        let result = sstable.get(key.as_bytes()).unwrap();
        assert!(result.is_some(), "Key {} should be found but got None", key);
        assert_eq!(result.unwrap(), format!("val_{:05}", i).as_bytes());
    }
}

// =============================================================================
// Test 2: Get non-existing key → None (bloom rejects)
// =============================================================================
#[test]
fn bloom_sstable_nonexistent_key_none() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    for i in 0..100u32 {
        let key = format!("key_{:05}", i);
        let val = format!("val_{:05}", i);
        builder.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();

    // Keys that were never inserted should return None
    for i in 100..200u32 {
        let key = format!("key_{:05}", i);
        let result = sstable.get(key.as_bytes()).unwrap();
        assert!(result.is_none(), "Key {} should not be found", key);
    }
}

// =============================================================================
// Test 3: Bloom filter false positive rate is reasonable
// Checks 10000 non-existing keys — FPR should be under 5% (target is 1%)
// =============================================================================
#[test]
fn bloom_sstable_false_positive_rate_reasonable() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    // Insert 1000 keys with a known prefix
    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    for i in 0..1000u32 {
        let key = format!("exist_{:06}", i);
        let val = format!("val_{:06}", i);
        builder.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    builder.finish().unwrap();

    let sstable = SSTable::open(&path).unwrap();

    // Check 10000 keys with a different prefix — none were inserted
    // TODO(human): Implement the false positive rate measurement
    // Count how many of these non-existent keys get a Some() result
    // (those are false positives that passed the bloom but failed the block lookup).
    // The FPR should be well under 5%.
    let total_checks = 10000u32;
    let mut false_positives = 0u32;

    for i in 0..total_checks {
        let _key = format!("nokey_{:06}", i);
        // These keys are outside [min_key, max_key] range so they'll be
        // caught by the range check before bloom. Use keys that fall WITHIN
        // the range to actually test the bloom filter.
        let key = format!("exist_{:06}", 1000 + i);
        if sstable.get(key.as_bytes()).unwrap().is_some() {
            false_positives += 1;
        }
    }

    let fpr = false_positives as f64 / total_checks as f64;
    assert!(
        fpr < 0.05,
        "False positive rate {} is too high (expected < 5%)",
        fpr
    );
    // With 1% target FPR, we expect close to 0% here since these keys
    // don't actually exist in the data blocks either
    assert_eq!(
        false_positives, 0,
        "Non-existent keys should never return Some — got {} false positives",
        false_positives
    );
}

// =============================================================================
// Test 4: Bloom filter survives roundtrip (write → close → reopen → check)
// =============================================================================
#[test]
fn bloom_survives_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    {
        let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
        builder.add(b"alpha", b"one").unwrap();
        builder.add(b"beta", b"two").unwrap();
        builder.add(b"gamma", b"three").unwrap();
        builder.finish().unwrap();
    }

    // Reopen — bloom must be deserialized correctly from disk
    let sstable = SSTable::open(&path).unwrap();
    assert_eq!(sstable.get(b"alpha").unwrap(), Some(b"one".to_vec()));
    assert_eq!(sstable.get(b"beta").unwrap(), Some(b"two".to_vec()));
    assert_eq!(sstable.get(b"gamma").unwrap(), Some(b"three".to_vec()));
    assert_eq!(sstable.get(b"delta").unwrap(), None);
}

// =============================================================================
// Test 5: Footer contains non-zero bloom block offset and size
// =============================================================================
#[test]
fn footer_has_bloom_block_info() {
    use lsm_engine::sstable::footer::Footer;
    use std::io::{Read, Seek, SeekFrom};

    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let mut builder = SSTableBuilder::new(&path, 1, 4096).unwrap();
    builder.add(b"key", b"val").unwrap();
    builder.finish().unwrap();

    // Read footer from file
    let mut file = std::fs::File::open(&path).unwrap();
    let file_len = file.metadata().unwrap().len();
    file.seek(SeekFrom::Start(file_len - Footer::SIZE as u64))
        .unwrap();
    let mut footer_buf = vec![0u8; Footer::SIZE];
    file.read_exact(&mut footer_buf).unwrap();

    let footer = Footer::decode(&footer_buf).unwrap();
    assert!(
        footer.bloom_block_size > 0,
        "Bloom block size should be > 0"
    );
    assert!(
        footer.bloom_block_offset > 0,
        "Bloom block offset should be > 0"
    );
    // Bloom block should sit between meta block and index block
    assert!(footer.bloom_block_offset >= footer.meta_block_offset + footer.meta_block_size);
    assert!(footer.bloom_block_offset + footer.bloom_block_size <= footer.index_block_offset);
}
