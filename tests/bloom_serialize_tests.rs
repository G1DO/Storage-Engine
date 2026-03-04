use lsm_engine::bloom::BloomFilter;

#[test]
fn test_serialize_deserialize_roundtrip() {
    // Create bloom filter with some keys
    let mut bf = BloomFilter::new(100, 0.01);
    bf.insert(b"hello");
    bf.insert(b"world");
    bf.insert(b"foo");

    // Serialize to bytes
    let bytes = bf.serialize();

    // Deserialize back
    let bf2 = BloomFilter::deserialize(&bytes).unwrap();

    // Check all the same keys work
    assert!(bf2.may_contain(b"hello"));
    assert!(bf2.may_contain(b"world"));
    assert!(bf2.may_contain(b"foo"));

    // Check non-inserted keys still return false
    assert!(!bf2.may_contain(b"bar"));
    assert!(!bf2.may_contain(b"baz"));
}

#[test]
fn test_serialize_empty_filter() {
    let bf = BloomFilter::new(100, 0.01);
    // Don't insert anything

    let bytes = bf.serialize();
    let bf2 = BloomFilter::deserialize(&bytes).unwrap();

    // Empty filter should return false for everything
    assert!(!bf2.may_contain(b"anything"));
    assert!(!bf2.may_contain(b""));
}

#[test]
fn test_serialize_large_filter() {
    let mut bf = BloomFilter::new(10000, 0.01);

    // Insert many keys
    for i in 0..10000 {
        let key = format!("key_{}", i);
        bf.insert(key.as_bytes());
    }

    let bytes = bf.serialize();
    let bf2 = BloomFilter::deserialize(&bytes).unwrap();

    // Verify all keys still found
    for i in 0..10000 {
        let key = format!("key_{}", i);
        assert!(bf2.may_contain(key.as_bytes()), "Key {} should be found", i);
    }
}

#[test]
fn test_deserialize_garbage() {
    // Random bytes - too short
    let garbage = vec![0xFF, 0xFF, 0xFF, 0xFF];
    assert!(BloomFilter::deserialize(&garbage).is_err());

    // Empty data
    let empty: Vec<u8> = vec![];
    assert!(BloomFilter::deserialize(&empty).is_err());
}

#[test]
fn test_deserialize_truncated() {
    // Header says we have 100 u64s, but no data
    let mut data = vec![];
    data.extend_from_slice(&7u32.to_le_bytes()); // num_hashes = 7
    data.extend_from_slice(&1000u32.to_le_bytes()); // num_bits = 1000
    data.extend_from_slice(&100u32.to_le_bytes()); // num_u64s = 100

    // Missing actual bit data
    assert!(BloomFilter::deserialize(&data).is_err());
}

#[test]
fn test_deserialize_extra_data() {
    let mut bf = BloomFilter::new(10, 0.01);
    bf.insert(b"test");

    let mut bytes = bf.serialize();
    bytes.extend_from_slice(b"extra"); // Add garbage at end

    assert!(BloomFilter::deserialize(&bytes).is_err());
}

#[test]
fn test_serialized_size() {
    let bf = BloomFilter::new(1000, 0.01);

    let bytes = bf.serialize();

    // Should be: 12 bytes header + (num_u64s * 8 bytes)
    let num_u64s = bf.num_bits() / 64
        + if !bf.num_bits().is_multiple_of(64) {
            1
        } else {
            0
        };
    let expected_size = 12 + (num_u64s as usize * 8);

    assert_eq!(bytes.len(), expected_size);
}

#[test]
fn test_serialize_different_fpr() {
    // Test with different false positive rates
    for fpr in [0.1, 0.05, 0.01, 0.001] {
        let mut bf = BloomFilter::new(1000, fpr);
        bf.insert(b"test_key");

        let bytes = bf.serialize();
        let bf2 = BloomFilter::deserialize(&bytes).unwrap();

        assert!(bf2.may_contain(b"test_key"));
    }
}

#[test]
fn test_serialize_binary_keys() {
    let mut bf = BloomFilter::new(100, 0.01);

    // Keys with null bytes and high bytes
    let key1 = vec![0x00, 0x01, 0x02, 0xFF];
    let key2 = vec![0xFF, 0xFE, 0xFD, 0xFC];

    bf.insert(&key1);

    let bytes = bf.serialize();
    let bf2 = BloomFilter::deserialize(&bytes).unwrap();

    assert!(bf2.may_contain(&key1));
    assert!(!bf2.may_contain(&key2));
}

#[test]
fn test_serialize_verify_fields_preserved() {
    let bf = BloomFilter::new(5000, 0.05);

    let num_hashes_before = bf.num_hashes();
    let num_bits_before = bf.num_bits();

    let bytes = bf.serialize();
    let bf2 = BloomFilter::deserialize(&bytes).unwrap();

    assert_eq!(bf2.num_hashes(), num_hashes_before);
    assert_eq!(bf2.num_bits(), num_bits_before);
}
