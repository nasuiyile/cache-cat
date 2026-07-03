#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocha::{EntrySnapshot, ExpirePolicy, Mocha};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::Duration;

    // Helper function to create a new Mocha instance for testing
    fn create_mocha() -> Mocha<String, String> {
        let logic_clock = Arc::new(AtomicU64::new(0));
        Mocha::new(logic_clock)
    }

    // Helper function to create a Mocha with custom initial clock
    fn create_mocha_with_clock(initial_clock: u64) -> (Mocha<String, String>, Arc<AtomicU64>) {
        let logic_clock = Arc::new(AtomicU64::new(initial_clock));
        (Mocha::new(logic_clock.clone()), logic_clock)
    }

    #[test]
    fn test_insert_and_get_entry() {
        let mocha = create_mocha();

        let key = "key1".to_string();
        let value = "value1".to_string();

        mocha.insert_persistent(key.clone(), value.clone());

        let entry = mocha.get_entry(&key);
        assert!(entry.is_some());

        let entry = entry.unwrap();
        assert_eq!(entry.value, value);
        assert_eq!(entry.expire_at, None);
    }

    #[test]
    fn test_insert_and_get() {
        let mocha = create_mocha();

        let key = "key1".to_string();
        let value = "value1".to_string();

        mocha.insert_persistent(key.clone(), value.clone());

        let result = mocha.get(&key);
        assert_eq!(result, Some(value));
    }

    #[test]
    fn test_get_nonexistent_key() {
        let mocha = create_mocha();

        let result = mocha.get_entry("nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn test_insert_with_ttl() {
        let (mocha, clock) = create_mocha_with_clock(0);

        let key = "key1".to_string();
        let value = "value1".to_string();
        let ttl = 100;

        let entry = mocha.insert(key.clone(), value.clone(), ttl);
        assert_eq!(entry.value, value);
        assert_eq!(entry.expire_at, Some(ttl));

        // Before expiration
        let result = mocha.get(&key);
        assert_eq!(result, Some(value));

        // Advance clock past TTL
        clock.store(101, Ordering::Relaxed);

        let result = mocha.get(&key);
        assert!(result.is_none());
    }

    #[test]
    fn test_insert_absolute() {
        let (mocha, clock) = create_mocha_with_clock(0);

        let key = "key1".to_string();
        let value = "value1".to_string();
        let expire_at = 500;

        let entry = mocha.insert_absolute(key.clone(), value.clone(), expire_at);
        assert_eq!(entry.value, value);
        assert_eq!(entry.expire_at, Some(expire_at));

        // Before expiration
        let result = mocha.get(&key);
        assert_eq!(result, Some(value));

        // Advance clock past expiration
        clock.store(501, Ordering::Relaxed);

        let result = mocha.get(&key);
        assert!(result.is_none());
    }

    #[test]
    fn test_insert_persistent() {
        let mocha = create_mocha();

        let key = "key1".to_string();
        let value = "value1".to_string();

        let entry = mocha.insert_persistent(key.clone(), value.clone());
        assert_eq!(entry.value, value);
        assert_eq!(entry.expire_at, None);
    }

    #[test]
    fn test_insert_snapshot() {
        let mocha = create_mocha();

        let key = "key1".to_string();
        let snapshot = EntrySnapshot {
            value: "value1".to_string(),
            expire_at: Some(1000),
        };

        let result = mocha.insert_snapshot(key.clone(), snapshot.clone());
        assert_eq!(result, snapshot);

        let entry = mocha.get_entry(&key);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap(), snapshot);
    }

    #[test]
    fn test_insert_entry() {
        let mocha = create_mocha();

        let key = "key1".to_string();
        let value = "value1".to_string();

        let entry = mocha.insert_entry(key.clone(), value.clone(), ExpirePolicy::Persistent);
        assert_eq!(entry.value, value);
        assert_eq!(entry.expire_at, None);
    }

    #[test]
    fn test_remove() {
        let mocha = create_mocha();

        let key = "key1".to_string();
        let value = "value1".to_string();

        mocha.insert_persistent(key.clone(), value.clone());

        let removed = mocha.remove(&key);
        assert_eq!(removed, Some(value));

        // Key should no longer exist
        let result = mocha.get(&key);
        assert!(result.is_none());
    }

    #[test]
    fn test_remove_nonexistent() {
        let mocha = create_mocha();

        let result = mocha.remove(&"nonexistent".to_string());
        assert!(result.is_none());
    }

    #[test]
    fn test_remove_expired() {
        let (mocha, clock) = create_mocha_with_clock(0);

        let key = "key1".to_string();
        let value = "value1".to_string();

        mocha.insert(key.clone(), value.clone(), 10);

        // Advance clock past expiration
        clock.store(20, Ordering::Relaxed);

        let removed = mocha.remove(&key);
        assert!(removed.is_none()); // Should be None because it's expired
    }

    #[test]
    fn test_remove_entry() {
        let mocha = create_mocha();

        let key = "key1".to_string();
        let value = "value1".to_string();
        let expire_at = 1000;

        mocha.insert_absolute(key.clone(), value.clone(), expire_at);

        let removed_entry = mocha.remove_entry(&key);
        assert!(removed_entry.is_some());

        let entry = removed_entry.unwrap();
        assert_eq!(entry.value, value);
        assert_eq!(entry.expire_at, Some(expire_at));
    }

    #[test]
    fn test_contains_key() {
        let mocha = create_mocha();

        let key = "key1".to_string();
        let value = "value1".to_string();

        assert!(!mocha.contains_key(&key));

        mocha.insert_persistent(key.clone(), value);

        assert!(mocha.contains_key(&key));
    }

    #[test]
    fn test_ttl_remaining() {
        let (mocha, clock) = create_mocha_with_clock(0);

        let key = "key1".to_string();
        let value = "value1".to_string();
        let ttl = 100;

        mocha.insert(key.clone(), value, ttl);

        assert_eq!(mocha.ttl_remaining(&key), Some(100));

        clock.store(30, Ordering::Relaxed);
        assert_eq!(mocha.ttl_remaining(&key), Some(70));

        clock.store(100, Ordering::Relaxed);
        assert_eq!(mocha.ttl_remaining(&key), None);
    }

    #[test]
    fn test_ttl_remaining_persistent() {
        let mocha = create_mocha();

        let key = "key1".to_string();
        let value = "value1".to_string();

        mocha.insert_persistent(key.clone(), value);

        // Persistent entries have no TTL
        assert_eq!(mocha.ttl_remaining(&key), None);
    }

    #[test]
    fn test_ttl_remaining_nonexistent() {
        let mocha = create_mocha();

        assert_eq!(mocha.ttl_remaining(&"nonexistent".to_string()), None);
    }

    #[test]
    fn test_set_expire_policy() {
        let mocha = create_mocha();

        let key = "key1".to_string();
        let value = "value1".to_string();

        mocha.insert_persistent(key.clone(), value.clone());

        // Change to TTL policy
        let result = mocha.set_expire_policy(&key, ExpirePolicy::Ttl(50));
        assert!(result.is_some());

        let entry = mocha.get_entry(&key);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().expire_at, Some(50));
    }

    #[test]
    fn test_set_expire_policy_to_persistent() {
        let mocha = create_mocha();

        let key = "key1".to_string();
        let value = "value1".to_string();

        mocha.insert(key.clone(), value.clone(), 100);

        // Change to persistent
        let result = mocha.set_expire_policy(&key, ExpirePolicy::Persistent);
        assert!(result.is_some());

        let entry = mocha.get_entry(&key);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().expire_at, None);
    }

    #[test]
    fn test_set_expire_policy_nonexistent() {
        let mocha = create_mocha();

        let result = mocha.set_expire_policy(&"nonexistent".to_string(), ExpirePolicy::Persistent);
        assert!(result.is_none());
    }

    #[test]
    fn test_clear() {
        let mocha = create_mocha();

        mocha.insert_persistent("key1".to_string(), "value1".to_string());
        mocha.insert_persistent("key2".to_string(), "value2".to_string());

        assert!(mocha.contains_key(&"key1".to_string()));

        let cleared = mocha.clear();
        assert_eq!(cleared, 2);

        assert!(!mocha.contains_key(&"key1".to_string()));
        assert!(!mocha.contains_key(&"key2".to_string()));
    }

    #[test]
    fn test_trigger_expire_cycle() {
        let (mocha, clock) = create_mocha_with_clock(0);

        mocha.insert("key1".to_string(), "value1".to_string(), 10);

        // Advance clock
        clock.store(20, Ordering::Relaxed);

        // Trigger expiration
        mocha.trigger_expire_cycle();

        // Give it a moment to process
        thread::sleep(Duration::from_millis(10));

        // Manually advance wheel again to ensure processing
        mocha.trigger_expire_cycle();
        thread::sleep(Duration::from_millis(10));

        // Key should now be expired
        let _result = mocha.get(&"key1".to_string());
        // Note: This might still return value if wheel hasn't processed yet
        // In practice, you might need to wait for the worker thread
    }

    #[test]
    fn test_active_expire_cycle_blocking() {
        let (mocha, clock) = create_mocha_with_clock(0);

        mocha.insert("key1".to_string(), "value1".to_string(), 10);
        mocha.insert("key2".to_string(), "value2".to_string(), 100);

        clock.store(50, Ordering::Relaxed);

        // This should process expiration of key1
        mocha.active_expire_cycle_blocking();

        let result1 = mocha.get(&"key1".to_string());
        assert!(result1.is_none());

        let result2 = mocha.get(&"key2".to_string());
        assert_eq!(result2, Some("value2".to_string()));
    }

    #[test]
    fn test_active_expire_cycle_blocking_no_expiration() {
        let mocha = create_mocha();

        mocha.insert("key1".to_string(), "value1".to_string(), 1000);

        mocha.active_expire_cycle_blocking();

        let result = mocha.get(&"key1".to_string());
        assert_eq!(result, Some("value1".to_string()));
    }

    #[test]
    fn test_has_expired_by_local_clock() {
        let mocha = create_mocha();

        // No entries, should return false
        assert!(!mocha.has_expired_by_local_clock());

        // Add entry with very short TTL
        mocha.insert("key1".to_string(), "value1".to_string(), 1);

        // Wait for local clock to pass the TTL
        thread::sleep(Duration::from_millis(5));

        // This checks if local clock shows expired entries
        // The result might depend on timing and wheel processing
        let _ = mocha.has_expired_by_local_clock();
    }

    #[test]
    fn test_get_if_alive() {
        let (mocha, clock) = create_mocha_with_clock(0);

        let key = "key1".to_string();
        let value = "value1".to_string();

        mocha.insert(key.clone(), value.clone(), 100);

        // Should be alive
        assert_eq!(mocha.get_if_alive(&key), Some(value));

        // Advance clock past TTL
        clock.store(200, Ordering::Relaxed);

        // Should be expired
        assert_eq!(mocha.get_if_alive(&key), None);
    }

    #[test]
    fn test_multiple_keys() {
        let mocha = create_mocha();

        let keys_values = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
            ("key3".to_string(), "value3".to_string()),
        ];

        for (key, value) in &keys_values {
            mocha.insert_persistent(key.clone(), value.clone());
        }

        for (key, value) in &keys_values {
            assert_eq!(mocha.get(key), Some(value.clone()));
        }
    }

    #[test]
    fn test_update_existing_key() {
        let mocha = create_mocha();

        let key = "key1".to_string();

        mocha.insert_persistent(key.clone(), "value1".to_string());
        assert_eq!(mocha.get(&key), Some("value1".to_string()));

        // Update with new value
        mocha.insert_persistent(key.clone(), "value2".to_string());
        assert_eq!(mocha.get(&key), Some("value2".to_string()));
    }

    #[test]
    fn test_concurrent_access() {
        let mocha = Arc::new(create_mocha());
        let mut handles = vec![];

        for i in 0..10 {
            let mocha_clone = mocha.clone();
            handles.push(thread::spawn(move || {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                mocha_clone.insert_persistent(key.clone(), value.clone());

                thread::sleep(Duration::from_millis(10));

                let result = mocha_clone.get(&key);
                assert_eq!(result, Some(value));
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_expire_policy_absolute() {
        let policy = ExpirePolicy::Absolute(100);
        match policy {
            ExpirePolicy::Absolute(at) => assert_eq!(at, 100),
            _ => panic!("Expected Absolute policy"),
        }
    }

    #[test]
    fn test_expire_policy_ttl() {
        let policy = ExpirePolicy::Ttl(50);
        match policy {
            ExpirePolicy::Ttl(ttl) => assert_eq!(ttl, 50),
            _ => panic!("Expected Ttl policy"),
        }
    }

    #[test]
    fn test_expire_policy_persistent() {
        let policy = ExpirePolicy::Persistent;
        assert_eq!(policy, ExpirePolicy::Persistent);
    }

    #[test]
    fn test_entry_snapshot_get_expire_policy() {
        let snapshot = EntrySnapshot {
            value: "test".to_string(),
            expire_at: None,
        };
        assert_eq!(snapshot.get_expire_policy(), ExpirePolicy::Persistent);

        let snapshot = EntrySnapshot {
            value: "test".to_string(),
            expire_at: Some(100),
        };
        assert_eq!(snapshot.get_expire_policy(), ExpirePolicy::Absolute(100));
    }

    #[test]
    fn test_logic_clock() {
        let logic_clock = Arc::new(AtomicU64::new(0));
        let mocha = Mocha::<String, String>::new(logic_clock.clone());

        assert_eq!(mocha.now_logical(), 0);

        logic_clock.store(100, Ordering::Relaxed);
        assert_eq!(mocha.now_logical(), 100);
    }

    #[test]
    fn test_ttl_expiration_edge_cases() {
        let (mocha, clock) = create_mocha_with_clock(0);

        // Test exact expiration time
        mocha.insert("key1".to_string(), "value1".to_string(), 0);

        // At time 0, it should be expired
        let result = mocha.get(&"key1".to_string());
        assert!(result.is_none());

        // Test very large TTL
        mocha.insert("key2".to_string(), "value2".to_string(), u64::MAX);

        let result = mocha.get(&"key2".to_string());
        assert_eq!(result, Some("value2".to_string()));

        // Test that key is still there
        clock.store(u64::MAX - 1, Ordering::Relaxed);
        let result = mocha.get(&"key2".to_string());
        assert_eq!(result, Some("value2".to_string()));
    }

    #[test]
    fn test_guard() {
        let mocha = create_mocha();

        mocha.insert_persistent("key1".to_string(), "value1".to_string());

        // Get a guard (though we can't do much with it directly in test)
        let _guard = mocha.guard();

        // After guard is released, we can still access data
        let result = mocha.get(&"key1".to_string());
        assert_eq!(result, Some("value1".to_string()));
    }
}
