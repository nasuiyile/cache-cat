#[cfg(test)]
mod tests {
    use crate::mocha::{
        Entry, EntrySnapshot, ExpireCommand, ExpirePolicy, HierarchicalTimeWheel, Mocha,
    };
    use crossbeam_channel::{Receiver, unbounded};
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

    // Retain the receiver so tests can count scheduling work without racing
    // the background worker or adding instrumentation to production writes.
    fn create_mocha_without_worker() -> (Mocha<String, String>, Receiver<ExpireCommand<String>>) {
        let (expire_tx, expire_rx) = unbounded();
        let mocha = Mocha {
            map: Arc::new(papaya::HashMap::new()),
            logic_clock: Arc::new(AtomicU64::new(0)),
            expire_tx,
        };
        (mocha, expire_rx)
    }

    fn scheduled_deadlines(receiver: &Receiver<ExpireCommand<String>>) -> Vec<u64> {
        receiver
            .try_iter()
            .map(|command| match command {
                ExpireCommand::Schedule { expire_at, .. } => expire_at,
                other => panic!("unexpected expiration command: {other:?}"),
            })
            .collect()
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

        // The compact representation reserves MAX for persistence, so an
        // out-of-Redis-range deadline saturates at the last finite value.
        let entry = mocha.insert("key2".to_string(), "value2".to_string(), u64::MAX);
        assert_eq!(entry.expire_at, Some(u64::MAX - 1));

        let result = mocha.get(&"key2".to_string());
        assert_eq!(result, Some("value2".to_string()));

        // Test that key is still there
        clock.store(u64::MAX - 2, Ordering::Relaxed);
        let result = mocha.get(&"key2".to_string());
        assert_eq!(result, Some("value2".to_string()));

        clock.store(u64::MAX - 1, Ordering::Relaxed);
        mocha.active_expire_cycle_blocking();
        assert_eq!(mocha.len(), 0);
    }

    #[test]
    fn test_retaining_long_ttl_schedules_once_and_expires_latest_value() {
        let (mocha, receiver) = create_mocha_without_worker();
        let key = "counter".to_string();
        let deadline = 86_400_000;
        mocha.insert(key.clone(), "0".to_string(), deadline);

        // INCR and other ComputeCommands preserve this absolute deadline
        // when their result reaches insert_entry.
        for value in 1..=1000 {
            let entry = mocha.get_entry(&key).unwrap();
            mocha.insert_entry(key.clone(), value.to_string(), entry.get_expire_policy());
        }
        assert_eq!(mocha.get(&key), Some("1000".to_string()));

        let mut wheel = HierarchicalTimeWheel::new(0);
        let mut paused = false;
        let commands: Vec<_> = receiver.try_iter().collect();
        assert_eq!(commands.len(), 1);
        for command in commands {
            Mocha::handle_expire_command(
                &mocha.map,
                &mocha.logic_clock,
                &mut wheel,
                &mut paused,
                command,
            );
        }
        mocha.logic_clock.store(deadline, Ordering::Relaxed);
        Mocha::advance_wheel(&mocha.map, &mocha.logic_clock, &mut wheel);
        assert_eq!(mocha.len(), 0);
    }

    #[test]
    fn test_same_deadline_across_snapshot_and_expire_policies_schedules_once() {
        let (mocha, receiver) = create_mocha_without_worker();
        let key = "key".to_string();
        let snapshot = mocha.insert_absolute(key.clone(), "initial".to_string(), 100);
        mocha.insert_snapshot(key.clone(), snapshot);
        mocha.insert_absolute(key.clone(), "replacement".to_string(), 100);
        mocha.set_expire_policy(&key, ExpirePolicy::Absolute(100));
        mocha.logic_clock.store(10, Ordering::Relaxed);
        mocha.set_expire_policy(&key, ExpirePolicy::Ttl(90));

        assert_eq!(scheduled_deadlines(&receiver), vec![100]);
        assert_eq!(mocha.get(&key), Some("replacement".to_string()));
        assert_eq!(mocha.ttl_remaining(&key), Some(90));
    }

    #[test]
    fn test_changed_deadline_persistence_and_recreation_schedule_as_needed() {
        let (mocha, receiver) = create_mocha_without_worker();
        let key = "key".to_string();
        mocha.insert_absolute(key.clone(), "value".to_string(), 100);
        mocha.set_expire_policy(&key, ExpirePolicy::Absolute(200));
        mocha.set_expire_policy(&key, ExpirePolicy::Persistent);
        mocha.insert_persistent(key.clone(), "persistent".to_string());
        mocha.insert_absolute(key.clone(), "finite".to_string(), 100);
        mocha.remove(&key);
        mocha.insert_absolute(key.clone(), "recreated".to_string(), 100);
        mocha.clear();
        mocha.insert_absolute(key, "after clear".to_string(), 100);

        assert_eq!(
            scheduled_deadlines(&receiver),
            vec![100, 200, 100, 100, 100]
        );
    }

    #[test]
    fn test_stale_timers_do_not_remove_extended_or_persistent_entries() {
        let (mocha, clock) = create_mocha_with_clock(0);
        let key = "key".to_string();
        mocha.insert_absolute(key.clone(), "old".to_string(), 100);
        mocha.insert_absolute(key.clone(), "extended".to_string(), 200);
        clock.store(100, Ordering::Relaxed);
        mocha.active_expire_cycle_blocking();
        assert_eq!(mocha.get(&key), Some("extended".to_string()));

        mocha.insert_persistent(key.clone(), "persistent".to_string());
        clock.store(200, Ordering::Relaxed);
        mocha.active_expire_cycle_blocking();
        assert_eq!(mocha.get(&key), Some("persistent".to_string()));
    }

    #[test]
    fn test_snapshot_restore_rebuilds_timer_and_reuses_it_during_replay() {
        let (mocha, clock) = create_mocha_with_clock(0);
        let key = "key".to_string();
        let snapshot = mocha.insert_absolute(key.clone(), "snapshot".to_string(), 100);
        mocha.pause_expire_worker_blocking();
        mocha.clear();
        mocha.insert_snapshot(key.clone(), snapshot);
        mocha.insert_entry(key, "replayed".to_string(), ExpirePolicy::Absolute(100));
        clock.store(100, Ordering::Relaxed);
        mocha.resume_expire_worker_blocking();

        // Check physical removal, without get_entry's lazy expiration.
        assert_eq!(mocha.len(), 0);
    }

    #[test]
    fn test_saturated_absolute_deadline_and_unlink_at_maximum_clock() {
        let (mocha, clock) = create_mocha_with_clock(0);
        let key = "key".to_string();
        let snapshot = mocha.insert_absolute(key.clone(), "value".to_string(), u64::MAX);
        assert_eq!(snapshot.expire_at, Some(u64::MAX - 1));
        clock.store(u64::MAX, Ordering::Relaxed);
        mocha.active_expire_cycle_blocking();
        assert_eq!(mocha.len(), 0);

        mocha.insert_persistent(key.clone(), "persistent".to_string());
        assert!(mocha.unlink(&key));
        mocha.active_expire_cycle_blocking();
        assert_eq!(mocha.len(), 0);

        mocha.insert_persistent(key.clone(), "persistent".to_string());
        assert_eq!(mocha.unlink_batch(&[key]), 1);
        mocha.active_expire_cycle_blocking();
        assert_eq!(mocha.len(), 0);
    }

    #[test]
    fn test_maximum_redis_deadline_remains_exact() {
        let (mocha, clock) = create_mocha_with_clock(1);
        let deadline = i64::MAX as u64;
        let absolute = mocha.insert_absolute("absolute".to_string(), "value".to_string(), deadline);
        let relative = mocha.insert("relative".to_string(), "value".to_string(), deadline - 1);
        assert_eq!(absolute.expire_at, Some(deadline));
        assert_eq!(relative.expire_at, Some(deadline));

        clock.store(deadline - 1, Ordering::Relaxed);
        mocha.active_expire_cycle_blocking();
        assert_eq!(mocha.len(), 2);
        clock.store(deadline, Ordering::Relaxed);
        mocha.active_expire_cycle_blocking();
        assert_eq!(mocha.len(), 0);
    }

    #[test]
    fn test_entry_expiration_uses_one_u64() {
        use crate::raft::types::core::mocha::core::MyValue;

        assert_eq!(
            std::mem::size_of::<Entry<u64>>(),
            2 * std::mem::size_of::<u64>()
        );
        assert_eq!(
            std::mem::size_of::<EntrySnapshot<MyValue>>() - std::mem::size_of::<Entry<MyValue>>(),
            std::mem::size_of::<u64>()
        );
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
