// M01: Skip List — Insert and Lookup tests

use lsm_engine::memtable::skiplist::SkipList;

#[test]
fn insert_one_key_get_it_back() {
    let mut sl = SkipList::new();
    sl.insert(b"hello".to_vec(), b"world".to_vec());
    assert_eq!(sl.get(b"hello"), Some(b"world".as_slice()));
}

#[test]
fn insert_two_keys_out_of_order() {
    let mut sl = SkipList::new();
    sl.insert(b"b".to_vec(), b"2".to_vec());
    sl.insert(b"a".to_vec(), b"1".to_vec());
    assert_eq!(sl.get(b"a"), Some(b"1".as_slice()));
    assert_eq!(sl.get(b"b"), Some(b"2".as_slice()));
}

#[test]
fn insert_duplicate_key_overwrites() {
    let mut sl = SkipList::new();
    sl.insert(b"key".to_vec(), b"old".to_vec());
    sl.insert(b"key".to_vec(), b"new".to_vec());
    assert_eq!(sl.get(b"key"), Some(b"new".as_slice()));
    assert_eq!(sl.len(), 1);
}

#[test]
fn get_nonexistent_key_returns_none() {
    let mut sl = SkipList::new();
    sl.insert(b"a".to_vec(), b"1".to_vec());
    assert_eq!(sl.get(b"z"), None);
}

#[test]
fn insert_1000_random_keys_get_all_back() {
    let mut sl = SkipList::new();
    let mut entries = Vec::new();
    for i in 0..1000u32 {
        let key = format!("key_{:05}", i).into_bytes();
        let val = format!("val_{}", i).into_bytes();
        entries.push((key.clone(), val.clone()));
        sl.insert(key, val);
    }
    for (k, v) in &entries {
        assert_eq!(sl.get(k), Some(v.as_slice()));
    }
    assert_eq!(sl.len(), 1000);
}

#[test]
fn empty_skiplist_behavior() {
    let sl = SkipList::new();
    assert_eq!(sl.get(b"anything"), None);
    assert_eq!(sl.len(), 0);
    assert!(sl.is_empty());
}
