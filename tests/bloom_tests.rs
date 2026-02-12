use lsm_engine::bloom::BloomFilter;

#[test]
fn test_empty_filter_returns_false() {
    let bf = BloomFilter::new(100, 0.01);

    // Empty filter should never return true
    assert!(!bf.may_contain(b"any_key"));
    assert!(!bf.may_contain(b"hello"));
    assert!(!bf.may_contain(b""));
}

#[test]
fn test_inserted_key_found() {
    let mut bf = BloomFilter::new(100, 0.01);

    bf.insert(b"hello");

    assert!(bf.may_contain(b"hello"));
}

#[test]
fn test_different_key_not_found() {
    let mut bf = BloomFilter::new(100, 0.01);

    bf.insert(b"hello");

    // Different key should (probably) not be found
    // Note: there's a small chance of false positive
    // but with 100 capacity and 1% FPR, it's unlikely
    assert!(!bf.may_contain(b"world"));
    assert!(!bf.may_contain(b"hello!"));
    assert!(!bf.may_contain(b"hell"));
}

#[test]
fn test_duplicate_insert_no_error() {
    let mut bf = BloomFilter::new(100, 0.01);

    // Insert same key multiple times
    bf.insert(b"key");
    bf.insert(b"key");
    bf.insert(b"key");

    // Should still be found
    assert!(bf.may_contain(b"key"));
}

#[test]
fn test_multiple_keys() {
    let mut bf = BloomFilter::new(100, 0.01);

    bf.insert(b"apple");
    bf.insert(b"banana");
    bf.insert(b"cherry");

    // All inserted keys should be found
    assert!(bf.may_contain(b"apple"));
    assert!(bf.may_contain(b"banana"));
    assert!(bf.may_contain(b"cherry"));

    // Non-inserted keys should (probably) not be found
    assert!(!bf.may_contain(b"date"));
    assert!(!bf.may_contain(b"elderberry"));
}

#[test]
fn test_false_positive_rate() {
    let n = 10000;
    let target_fpr = 0.01; // 1% target
    let mut bf = BloomFilter::new(n, target_fpr);

    // Insert n keys
    for i in 0..n {
        let key = format!("key_{}", i);
        bf.insert(key.as_bytes());
    }

    // Check n different keys (not inserted)
    let mut false_positives = 0;
    for i in n..(n * 2) {
        let key = format!("key_{}", i);
        if bf.may_contain(key.as_bytes()) {
            false_positives += 1;
        }
    }

    let actual_fpr = false_positives as f64 / n as f64;

    println!("Target FPR: {}, Actual FPR: {}", target_fpr, actual_fpr);
    println!("False positives: {} out of {} checks", false_positives, n);

    // Should be within 2x of target (so under 2%)
    assert!(
        actual_fpr < target_fpr * 2.0,
        "FPR too high: {} vs target {} (2x = {})",
        actual_fpr,
        target_fpr,
        target_fpr * 2.0
    );

    // Also shouldn't be too low (would indicate bug or over-allocation)
    // Allow 0.1x of target as minimum
    assert!(
        actual_fpr > target_fpr * 0.1 || false_positives == 0,
        "FPR suspiciously low: {} vs target {} - possible bug",
        actual_fpr,
        target_fpr
    );
}

#[test]
fn test_various_fpr_values() {
    // Test with different FPR targets
    let test_cases = vec![(0.10, "10%"), (0.05, "5%"), (0.01, "1%"), (0.001, "0.1%")];

    for (fpr, desc) in test_cases {
        let n = 5000;
        let mut bf = BloomFilter::new(n, fpr);

        // Insert keys
        for i in 0..n {
            let key = format!("test_{}_{}", desc, i);
            bf.insert(key.as_bytes());
        }

        // Check non-inserted keys
        let mut false_positives = 0;
        for i in n..(n * 2) {
            let key = format!("test_{}_{}", desc, i);
            if bf.may_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }

        let actual_fpr = false_positives as f64 / n as f64;
        println!("FPR {}: target={}, actual={}", desc, fpr, actual_fpr);

        // Should be within reasonable bounds
        assert!(
            actual_fpr < fpr * 3.0,
            "FPR {} too high: {} vs target {}",
            desc,
            actual_fpr,
            fpr
        );
    }
}

#[test]
fn test_empty_key() {
    let mut bf = BloomFilter::new(100, 0.01);

    // Empty key should work
    bf.insert(b"");
    assert!(bf.may_contain(b""));
}

#[test]
fn test_large_key() {
    let mut bf = BloomFilter::new(100, 0.01);

    // Large key should work
    let large_key = vec![0u8; 1024 * 1024]; // 1MB key
    bf.insert(&large_key);
    assert!(bf.may_contain(&large_key));
}

#[test]
fn test_binary_keys() {
    let mut bf = BloomFilter::new(100, 0.01);

    // Binary data with null bytes and high bytes
    let key1 = vec![0x00, 0x01, 0x02, 0xFF, 0xFE];
    let key2 = vec![0xFF, 0xFE, 0xFD, 0xFC];

    bf.insert(&key1);

    assert!(bf.may_contain(&key1));
    assert!(!bf.may_contain(&key2));
}
