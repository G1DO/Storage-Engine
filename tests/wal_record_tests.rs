// M06: WAL Record Format tests
// Tests for encoding and decoding WAL records with CRC checksums.

use lsm_engine::wal::{WALRecord, RecordType};

// =============================================================================
// Test 1: Encode and decode Put record
// =============================================================================
#[test]
fn encode_decode_put_record() {
    let record = WALRecord::put(b"key".to_vec(), b"value".to_vec());
    let encoded = record.encode();
    let decoded = WALRecord::decode(&encoded).unwrap();

    assert_eq!(decoded.record_type, RecordType::Put);
    assert_eq!(decoded.key, b"key");
    assert_eq!(decoded.value, b"value");
}

// =============================================================================
// Test 2: Encode and decode Delete record
// =============================================================================
#[test]
fn encode_decode_delete_record() {
    let record = WALRecord::delete(b"key".to_vec());
    let encoded = record.encode();
    let decoded = WALRecord::decode(&encoded).unwrap();

    assert_eq!(decoded.record_type, RecordType::Delete);
    assert_eq!(decoded.key, b"key");
    assert!(decoded.value.is_empty());
}

// =============================================================================
// Test 3: Corrupted CRC is detected
// =============================================================================
#[test]
fn corrupted_crc_detected() {
    let record = WALRecord::put(b"key".to_vec(), b"value".to_vec());
    let mut encoded = record.encode();

    // Flip a bit in the data (not the CRC itself)
    if encoded.len() > 10 {
        encoded[10] ^= 0xFF;
    }

    let result = WALRecord::decode(&encoded);
    assert!(result.is_err());
}

// =============================================================================
// Test 4: Empty key and value
// =============================================================================
#[test]
fn empty_key_and_value() {
    let record = WALRecord::put(Vec::new(), Vec::new());
    let encoded = record.encode();
    let decoded = WALRecord::decode(&encoded).unwrap();

    assert!(decoded.key.is_empty());
    assert!(decoded.value.is_empty());
}

// =============================================================================
// Test 5: Large key and value
// =============================================================================
#[test]
fn large_key_and_value() {
    let key = vec![0xAB; 10_000];
    let value = vec![0xCD; 100_000];

    let record = WALRecord::put(key.clone(), value.clone());
    let encoded = record.encode();
    let decoded = WALRecord::decode(&encoded).unwrap();

    assert_eq!(decoded.key, key);
    assert_eq!(decoded.value, value);
}

// =============================================================================
// Test 6: encoded_size matches actual size
// =============================================================================
#[test]
fn encoded_size_matches_actual() {
    let record = WALRecord::put(b"hello".to_vec(), b"world".to_vec());
    let encoded = record.encode();

    assert_eq!(record.encoded_size(), encoded.len());
}

// =============================================================================
// Test 7: Truncated record fails decode
// =============================================================================
#[test]
fn truncated_record_fails() {
    let record = WALRecord::put(b"key".to_vec(), b"value".to_vec());
    let encoded = record.encode();

    // Truncate to half
    let truncated = &encoded[..encoded.len() / 2];

    let result = WALRecord::decode(truncated);
    assert!(result.is_err());
}
