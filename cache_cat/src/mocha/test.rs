#[cfg(test)]
mod tests {
    use std::hash::Hash;
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use crate::mocha::{EntryRef, EntrySnapshot, ExpirePolicy, Mocha, MochaCompute, MochaOperation};

    fn create_mocha<K, V>() -> (Arc<Mocha<K, V>>, Arc<AtomicU64>)
    where
        K: Hash + Eq + Ord + Clone + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
    {
        let clock = Arc::new(AtomicU64::new(0));
        let mocha = Mocha::new(clock.clone(), Duration::from_millis(100));
        (mocha, clock)
    }

    fn advance_clock(clock: &Arc<AtomicU64>, delta: u64) {
        clock.fetch_add(delta, Ordering::Relaxed);
    }

    fn set_clock(clock: &Arc<AtomicU64>, value: u64) {
        clock.store(value, Ordering::Relaxed);
    }

    // ========== 基本插入和获取测试 ==========

    #[tokio::test]
    async fn test_insert_and_get_persistent() {
        let (mocha, _clock) = create_mocha();

        let snapshot = mocha.insert_persistent("key1", 42);
        assert_eq!(snapshot.value, 42);
        assert_eq!(snapshot.expire_at, None);

        let value = mocha.get(&"key1");
        assert_eq!(value, Some(42));
    }

    #[tokio::test]
    async fn test_insert_and_get_with_ttl() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);

        // 在过期前可以获取
        advance_clock(&clock, 50);
        let value = mocha.get(&"key1");
        assert_eq!(value, Some(42));

        // 过期后返回 None
        advance_clock(&clock, 60);
        let value = mocha.get(&"key1");
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_insert_absolute() {
        let (mocha, clock) = create_mocha();

        mocha.insert_absolute("key1", 42, 100);

        // 在过期时间之前
        set_clock(&clock, 50);
        assert_eq!(mocha.get(&"key1"), Some(42));

        // 正好到达过期时间
        set_clock(&clock, 100);
        assert_eq!(mocha.get(&"key1"), None);

        // 超过过期时间
        set_clock(&clock, 200);
        assert_eq!(mocha.get(&"key1"), None);
    }

    #[tokio::test]
    async fn test_insert_snapshot() {
        let (mocha, clock) = create_mocha();

        let original = EntrySnapshot {
            value: 42,
            expire_at: Some(100),
        };

        mocha.insert_snapshot("key1", original);

        set_clock(&clock, 50);
        assert_eq!(mocha.get(&"key1"), Some(42));

        set_clock(&clock, 100);
        assert_eq!(mocha.get(&"key1"), None);
    }





    // ========== 获取和检查测试 ==========

    #[tokio::test]
    async fn test_get_entry() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);

        advance_clock(&clock, 50);
        let entry = mocha.get_entry(&"key1");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().value, 42);

        advance_clock(&clock, 100);
        let entry = mocha.get_entry(&"key1");
        assert!(entry.is_none());
    }

    #[tokio::test]
    async fn test_get_if_alive() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);

        advance_clock(&clock, 50);
        assert_eq!(mocha.get_if_alive(&"key1"), Some(42));

        advance_clock(&clock, 100);
        assert_eq!(mocha.get_if_alive(&"key1"), None);
    }

    #[tokio::test]
    async fn test_contains_key() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);

        advance_clock(&clock, 50);
        assert!(mocha.contains_key(&"key1"));

        advance_clock(&clock, 100);
        assert!(!mocha.contains_key(&"key1"));
    }

    #[tokio::test]
    async fn test_ttl_remaining() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);

        assert_eq!(mocha.ttl_remaining(&"key1"), Some(100));

        advance_clock(&clock, 30);
        assert_eq!(mocha.ttl_remaining(&"key1"), Some(70));

        advance_clock(&clock, 80);
        assert_eq!(mocha.ttl_remaining(&"key1"), None);
    }

    #[tokio::test]
    async fn test_ttl_remaining_persistent() {
        let (mocha, _clock) = create_mocha();

        mocha.insert_persistent("key1", 42);
        assert_eq!(mocha.ttl_remaining(&"key1"), None);
    }

    // ========== 删除测试 ==========

    #[tokio::test]
    async fn test_remove_existing() {
        let (mocha, _clock) = create_mocha();

        mocha.insert_persistent("key1", 42);

        let removed = mocha.remove(&"key1");
        assert_eq!(removed, Some(42));

        // 确认已删除
        assert_eq!(mocha.get(&"key1"), None);
    }

    #[tokio::test]
    async fn test_remove_nonexistent() {
        let (mocha, _clock) = create_mocha::<&str, i32>();
        let removed = mocha.remove(&"key1");
        assert_eq!(removed, None);
    }

    #[tokio::test]
    async fn test_remove_expired() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);

        advance_clock(&clock, 200);

        // 过期的 key 应该返回 None
        let removed = mocha.remove(&"key1");
        assert_eq!(removed, None);
    }

    #[tokio::test]
    async fn test_remove_entry() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);

        advance_clock(&clock, 50);
        let entry = mocha.remove_entry(&"key1");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().value, 42);
    }

    // ========== 更新测试 ==========

    #[tokio::test]
    async fn test_update_or_insert_with_existing() {
        let (mocha, _clock) = create_mocha();

        mocha.insert_persistent("key1", 42);

        let snapshot = mocha.update_or_insert_with(
            "key1",
            |v| v * 2,
            || 0,
            ExpirePolicy::Persistent,
        );

        assert_eq!(snapshot.value, 84);
        assert_eq!(mocha.get(&"key1"), Some(84));
    }

    #[tokio::test]
    async fn test_update_or_insert_with_new() {
        let (mocha, _clock) = create_mocha();

        let snapshot = mocha.update_or_insert_with(
            "key1",
            |v| v * 2,
            || 42,
            ExpirePolicy::Persistent,
        );

        assert_eq!(snapshot.value, 42);
        assert_eq!(mocha.get(&"key1"), Some(42));
    }

    #[tokio::test]
    async fn test_update_or_insert_with_expired() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);
        advance_clock(&clock, 200);

        // key 已过期，应该执行 insert 回调
        let snapshot = mocha.update_or_insert_with(
            "key1",
            |v| v * 2,
            || 99,
            ExpirePolicy::Persistent,
        );

        assert_eq!(snapshot.value, 99);
        assert_eq!(mocha.get(&"key1"), Some(99));
    }

    // ========== Compute 测试 ==========

    #[tokio::test]
    async fn test_compute_insert_new() {
        let (mocha, _clock) = create_mocha();

        let result = mocha.compute("key1", |entry| {
            assert!(entry.is_none());
            MochaOperation::Insert {
                value: 42,
                expire: ExpirePolicy::Persistent,
            }
        });

        match result {
            MochaCompute::Inserted(key, snapshot) => {
                assert_eq!(key, "key1");
                assert_eq!(snapshot.value, 42);
            }
            _ => panic!("Expected Inserted, got {:?}", result),
        }

        assert_eq!(mocha.get(&"key1"), Some(42));
    }

    #[tokio::test]
    async fn test_compute_update_existing() {
        let (mocha, _clock) = create_mocha();

        mocha.insert_persistent("key1", 42);

        let result = mocha.compute("key1", |entry| {
            let entry = entry.unwrap();
            assert_eq!(entry.value, &42);
            MochaOperation::Insert {
                value: 84,
                expire: ExpirePolicy::Persistent,
            }
        });

        match result {
            MochaCompute::Updated { old, new } => {
                assert_eq!(old.1.value, 42);
                assert_eq!(new.1.value, 84);
            }
            _ => panic!("Expected Updated, got {:?}", result),
        }

        assert_eq!(mocha.get(&"key1"), Some(84));
    }

    #[tokio::test]
    async fn test_compute_remove() {
        let (mocha, _clock) = create_mocha();

        mocha.insert_persistent("key1", 42);

        let result = mocha.compute("key1", |entry| {
            assert!(entry.is_some());
            MochaOperation::Remove
        });

        match result {
            MochaCompute::Removed(key, snapshot) => {
                assert_eq!(key, "key1");
                assert_eq!(snapshot.value, 42);
            }
            _ => panic!("Expected Removed, got {:?}", result),
        }

        assert_eq!(mocha.get(&"key1"), None);
    }

    #[tokio::test]
    async fn test_compute_abort() {
        let (mocha, _clock) = create_mocha();

        mocha.insert_persistent("key1", 42);

        let result = mocha.compute("key1", |entry| {
            assert!(entry.is_some());
            MochaOperation::Abort
        });

        match result {
            MochaCompute::Unchanged => {}
            _ => panic!("Expected Unchanged, got {:?}", result),
        }

        // 值保持不变
        assert_eq!(mocha.get(&"key1"), Some(42));
    }

    #[tokio::test]
    async fn test_compute_with_expired_key() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);
        advance_clock(&clock, 200);

        let result = mocha.compute("key1", |entry| {
            // 过期 key 被视为不存在
            assert!(entry.is_none());
            MochaOperation::Insert {
                value: 99,
                expire: ExpirePolicy::Persistent,
            }
        });

        match result {
            MochaCompute::Inserted(_, snapshot) => {
                assert_eq!(snapshot.value, 99);
            }
            _ => panic!("Expected Inserted, got {:?}", result),
        }

        assert_eq!(mocha.get(&"key1"), Some(99));
    }

    // ========== 迭代测试 ==========

    #[tokio::test]
    async fn test_for_each() {
        let (mocha, _clock) = create_mocha();

        mocha.insert_persistent("key1", 1);
        mocha.insert_persistent("key2", 2);
        mocha.insert_persistent("key3", 3);

        let mut sum = 0;
        mocha.for_each(|_k, entry| {
            sum += entry.value;
        });

        assert_eq!(sum, 6);
    }

    #[tokio::test]
    async fn test_for_each_skips_expired() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 1, 100);
        mocha.insert("key2", 2, 100);
        mocha.insert_persistent("key3", 3);

        advance_clock(&clock, 200);

        let mut sum = 0;
        mocha.for_each(|_k, entry| {
            sum += entry.value;
        });

        // 只有 key3 未过期
        assert_eq!(sum, 3);
    }

    #[tokio::test]
    async fn test_iter() {
        let (mocha, _clock) = create_mocha();

        mocha.insert_persistent("key1", 1);
        mocha.insert_persistent("key2", 2);

        let guard = mocha.guard();
        let values: Vec<_> = mocha.iter(&guard).map(|(_, entry)| *entry.value).collect();

        assert_eq!(values.len(), 2);
        assert!(values.contains(&1));
        assert!(values.contains(&2));
    }

    #[tokio::test]
    async fn test_keys() {
        let (mocha, _clock) = create_mocha();

        mocha.insert_persistent("alpha", 1);
        mocha.insert_persistent("beta", 2);

        let guard = mocha.guard();
        let mut keys: Vec<_> = mocha.keys(&guard).cloned().collect();
        keys.sort();

        assert_eq!(keys, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn test_iter_snapshots() {
        let (mocha, _clock) = create_mocha();

        mocha.insert("key1", 42, 100);
        mocha.insert_persistent("key2", 99);

        let guard = mocha.guard();
        let snapshots: Vec<_> = mocha.iter_snapshots(&guard).collect();

        assert_eq!(snapshots.len(), 2);
    }

    #[tokio::test]
    async fn test_for_each_snapshot() {
        let (mocha, _clock) = create_mocha();

        mocha.insert_persistent("key1", 42);
        mocha.insert_persistent("key2", 99);

        let mut values = Vec::new();
        mocha.for_each_snapshot(|_k, snapshot| {
            values.push(snapshot.value);
        });

        assert_eq!(values.len(), 2);
        assert!(values.contains(&42));
        assert!(values.contains(&99));
    }

    // ========== 过期工作线程测试 ==========

    #[tokio::test]
    async fn test_expire_worker_removes_expired() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);
        mocha.insert("key2", 99, 100);
        mocha.insert_persistent("key3", 50);

        // 推进时间使 key1 和 key2 过期
        advance_clock(&clock, 200);

        // 手动触发一个过期周期以确保处理
        mocha.active_expire_cycle().await;

        // 给工作线程一点时间处理

        // 验证过期 key 被移除
        assert_eq!(mocha.get(&"key1"), None);
        assert_eq!(mocha.get(&"key2"), None);
        // 持久化 key 不受影响
        assert_eq!(mocha.get(&"key3"), Some(50));
    }

    #[tokio::test]
    async fn test_active_expire_cycle() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);
        mocha.insert("key2", 99, 100);
        mocha.insert_persistent("key3", 50);

        // 推进时间
        advance_clock(&clock, 200);

        // 手动触发过期周期
        mocha.active_expire_cycle().await;

        assert_eq!(mocha.get(&"key1"), None);
        assert_eq!(mocha.get(&"key2"), None);
        assert_eq!(mocha.get(&"key3"), Some(50));
    }

    #[tokio::test]
    async fn test_trigger_expire_cycle() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);
        mocha.insert_persistent("key2", 99);

        // 推进时间
        advance_clock(&clock, 200);

        // 触发过期周期（非阻塞）
        mocha.trigger_expire_cycle();

        // 给后台线程一点时间处理

        assert_eq!(mocha.get(&"key1"), None);
        assert_eq!(mocha.get(&"key2"), Some(99));
    }

    #[tokio::test]
    async fn test_active_expire_cycle_blocking() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);
        mocha.insert("key2", 99, 100);
        mocha.insert_persistent("key3", 50);

        // 推进时间
        advance_clock(&clock, 200);

        // 阻塞式触发过期周期
        mocha.active_expire_cycle_blocking();

        assert_eq!(mocha.get(&"key1"), None);
        assert_eq!(mocha.get(&"key2"), None);
        assert_eq!(mocha.get(&"key3"), Some(50));
    }

    // ========== Clear 测试 ==========

    #[tokio::test]
    async fn test_clear() {
        let (mocha, _clock) = create_mocha();

        mocha.insert_persistent("key1", 1);
        mocha.insert_persistent("key2", 2);
        mocha.insert_persistent("key3", 3);

        let count = mocha.clear();
        assert_eq!(count, 3);

        assert_eq!(mocha.get(&"key1"), None);
        assert_eq!(mocha.get(&"key2"), None);
        assert_eq!(mocha.get(&"key3"), None);
    }

    #[tokio::test]
    async fn test_clear_empty() {
        let (mocha, _clock) = create_mocha::<&str, i32>();

        let count = mocha.clear();
        assert_eq!(count, 0);
    }

    // ========== 边界情况测试 ==========

    #[tokio::test]
    async fn test_insert_multiple_keys() {
        let (mocha, _clock) = create_mocha();

        for i in 0..100 {
            mocha.insert_persistent(i, i * 2);
        }

        for i in 0..100 {
            assert_eq!(mocha.get(&i), Some(i * 2));
        }
    }

    #[tokio::test]
    async fn test_overwrite_key() {
        let (mocha, _clock) = create_mocha();

        let snapshot1 = mocha.insert_persistent("key1", 42);
        assert_eq!(snapshot1.value, 42);

        let snapshot2 = mocha.insert_persistent("key1", 99);
        assert_eq!(snapshot2.value, 99);

        assert_eq!(mocha.get(&"key1"), Some(99));
    }

    #[tokio::test]
    async fn test_zero_ttl() {
        let (mocha, _clock) = create_mocha();

        mocha.insert("key1", 42, 0);

        // 零 TTL 应该立即过期
        assert_eq!(mocha.get(&"key1"), None);
    }

    #[tokio::test]
    async fn test_very_large_ttl() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, u64::MAX);

        advance_clock(&clock, u64::MAX / 2);
        assert_eq!(mocha.get(&"key1"), Some(42));
    }

    #[tokio::test]
    async fn test_expire_policy_display() {
        // 测试 ExpirePolicy 枚举
        assert_eq!(ExpirePolicy::Persistent, ExpirePolicy::Persistent);
        assert_eq!(ExpirePolicy::Absolute(100), ExpirePolicy::Absolute(100));
        assert_eq!(ExpirePolicy::Ttl(100), ExpirePolicy::Ttl(100));
    }

    #[tokio::test]
    async fn test_entry_ref_expire_policy() {
        let entry_persistent = EntryRef {
            value: &42,
            expire_at: None,
        };
        assert_eq!(entry_persistent.get_expire_policy(), ExpirePolicy::Persistent);

        let entry_absolute = EntryRef {
            value: &42,
            expire_at: Some(100),
        };
        assert_eq!(entry_absolute.get_expire_policy(), ExpirePolicy::Absolute(100));
    }

    #[tokio::test]
    async fn test_update_expire_policy() {
        let (mocha, clock) = create_mocha();

        mocha.insert_persistent("key1", 42);

        // 通过 compute 更新过期策略
        let result = mocha.compute("key1", |entry| {
            let entry = entry.unwrap();
            MochaOperation::Insert {
                value: *entry.value,
                expire: ExpirePolicy::Absolute(100),
            }
        });

        assert!(matches!(result, MochaCompute::Updated { .. }));

        // 在过期前
        set_clock(&clock, 50);
        assert_eq!(mocha.get(&"key1"), Some(42));

        // 过期后
        set_clock(&clock, 100);
        assert_eq!(mocha.get(&"key1"), None);
    }

    // ========== set_expire_policy 测试 ==========

    #[tokio::test]
    async fn test_set_expire_policy_persistent_to_ttl() {
        let (mocha, clock) = create_mocha();

        mocha.insert_persistent("key1", 42);

        // 将持久化 key 改为有 TTL
        let snapshot = mocha.set_expire_policy(&"key1", ExpirePolicy::Ttl(100));
        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().value, 42);

        // 在过期前
        advance_clock(&clock, 50);
        assert_eq!(mocha.get(&"key1"), Some(42));

        // 过期后
        advance_clock(&clock, 60);
        assert_eq!(mocha.get(&"key1"), None);
    }

    #[tokio::test]
    async fn test_set_expire_policy_ttl_to_persistent() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);

        // 将 TTL key 改为持久化
        let snapshot = mocha.set_expire_policy(&"key1", ExpirePolicy::Persistent);
        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().value, 42);

        // 即使超过原 TTL，key 也不会过期
        advance_clock(&clock, 200);
        assert_eq!(mocha.get(&"key1"), Some(42));
    }

    #[tokio::test]
    async fn test_set_expire_policy_on_expired_key() {
        let (mocha, clock) = create_mocha();

        mocha.insert("key1", 42, 100);

        // 让 key 过期
        advance_clock(&clock, 200);

        // 对已过期的 key 设置策略应该返回 None
        let snapshot = mocha.set_expire_policy(&"key1", ExpirePolicy::Persistent);
        assert!(snapshot.is_none());

        // key 应该已被清理
        assert_eq!(mocha.get(&"key1"), None);
    }

    #[tokio::test]
    async fn test_set_expire_policy_on_nonexistent_key() {
        let (mocha, _clock) = create_mocha::<&str, i32>();

        let snapshot = mocha.set_expire_policy(&"key1", ExpirePolicy::Persistent);
        assert!(snapshot.is_none());
    }
}