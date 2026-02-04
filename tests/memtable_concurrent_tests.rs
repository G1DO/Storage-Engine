// M05: MemTable Concurrent Access tests
// Tests for thread-safe memtable operations.

use std::sync::Arc;
use std::thread;
use lsm_engine::memtable::MemTableManager;

// =============================================================================
// Test 1: Concurrent readers don't block each other
// =============================================================================
#[test]
fn concurrent_readers_dont_block() {
    let manager = Arc::new(MemTableManager::new(1024 * 1024));

    // Insert some data first
    manager.put(b"key1".to_vec(), b"value1".to_vec());
    manager.put(b"key2".to_vec(), b"value2".to_vec());

    let mut handles = vec![];

    // Spawn 10 reader threads
    for _ in 0..10 {
        let mgr = Arc::clone(&manager);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                let _ = mgr.get(b"key1");
                let _ = mgr.get(b"key2");
            }
        }));
    }

    // All threads should complete without deadlock
    for h in handles {
        h.join().unwrap();
    }
}

// =============================================================================
// Test 2: Writer and readers work together
// =============================================================================
#[test]
fn writer_and_readers_concurrent() {
    let manager = Arc::new(MemTableManager::new(1024 * 1024));

    let writer_mgr = Arc::clone(&manager);
    let writer = thread::spawn(move || {
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            let val = format!("val{}", i).into_bytes();
            writer_mgr.put(key, val);
        }
    });

    let mut readers = vec![];
    for _ in 0..5 {
        let mgr = Arc::clone(&manager);
        readers.push(thread::spawn(move || {
            for _ in 0..100 {
                // May or may not find keys depending on timing — that's OK
                let _ = mgr.get(b"key50");
            }
        }));
    }

    writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }

    // After all threads done, key should exist
    assert!(manager.get(b"key50").is_some());
}

// =============================================================================
// Test 3: Freeze creates new active memtable
// =============================================================================
#[test]
fn freeze_creates_new_active() {
    let manager = MemTableManager::new(1024 * 1024);

    // Put some data
    manager.put(b"key1".to_vec(), b"value1".to_vec());

    // Freeze — should move active to immutable
    manager.freeze();

    // Put more data — goes to new active
    manager.put(b"key2".to_vec(), b"value2".to_vec());

    // Both keys should be readable
    assert_eq!(manager.get(b"key1"), Some(b"value1".to_vec()));
    assert_eq!(manager.get(b"key2"), Some(b"value2".to_vec()));
}

// =============================================================================
// Test 4: Get checks both active and immutable
// =============================================================================
#[test]
fn get_checks_active_and_immutable() {
    let manager = MemTableManager::new(1024 * 1024);

    manager.put(b"old_key".to_vec(), b"old_value".to_vec());
    manager.freeze();
    manager.put(b"new_key".to_vec(), b"new_value".to_vec());

    // old_key is in immutable, new_key is in active
    assert_eq!(manager.get(b"old_key"), Some(b"old_value".to_vec()));
    assert_eq!(manager.get(b"new_key"), Some(b"new_value".to_vec()));
}

// =============================================================================
// Test 5: Active shadows immutable
// =============================================================================
#[test]
fn active_shadows_immutable() {
    let manager = MemTableManager::new(1024 * 1024);

    manager.put(b"key".to_vec(), b"old".to_vec());
    manager.freeze();
    manager.put(b"key".to_vec(), b"new".to_vec());

    // Active has newer value — should return "new"
    assert_eq!(manager.get(b"key"), Some(b"new".to_vec()));
}

// =============================================================================
// Test 6: Clear immutable after flush
// =============================================================================
#[test]
fn clear_immutable_after_flush() {
    let manager = MemTableManager::new(1024 * 1024);

    manager.put(b"key".to_vec(), b"value".to_vec());
    manager.freeze();

    assert!(manager.has_immutable());

    manager.clear_immutable();

    assert!(!manager.has_immutable());
}
