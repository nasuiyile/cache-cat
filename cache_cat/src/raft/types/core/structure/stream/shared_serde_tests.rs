use super::*;
use std::{pin::Pin, task::Poll};

async fn once<F: Future>(mut f: Pin<&mut F>) -> Poll<F::Output> {
    std::future::poll_fn(|cx| Poll::Ready(f.as_mut().poll(cx))).await
}

#[test]
fn direct_serde_fails_on_contention_instead_of_panicking_or_blocking() {
    let s = SharedStream::default();
    let held = s.0.state.write();
    assert!(serde_json::to_vec(&s).unwrap_err().to_string().contains("read lock is busy"));
    drop(held);
    assert!(serde_json::to_vec(&s).is_ok());
}

#[test]
fn encode_snapshot_and_memory_wait_synchronously_for_write_lock() {
    let s = SharedStream::default();
    let bytes = super::tests::with_contended_lock(s.0.state.write(), || {
        let mut bytes = Vec::new();
        s.serialize_with(&mut serde_json::Serializer::new(&mut bytes)).unwrap();
        bytes
    });
    assert!(serde_json::from_slice::<SharedStream>(&bytes).is_ok());
    let snapshot = super::tests::with_contended_lock(s.0.state.write(), || {
        s.snapshot().unwrap()
    });
    assert!(snapshot.entries.is_empty());
    let memory = super::tests::with_contended_lock(s.0.state.write(), || {
        s.memory_usage().unwrap()
    });
    assert_eq!(memory.entries, 0);
}

#[test]
fn serde_and_estimator_share_read_access() {
    let s = SharedStream::default();
    let held = s.0.state.read();
    assert!(serde_json::to_vec(&s).is_ok());
    assert!(s.snapshot().is_ok());
    assert!(s.memory_usage().is_ok());
    drop(held);
}

#[test]
fn poisoned_stream_cannot_be_serialized_or_inspected() {
    let s = SharedStream::default();
    s.0.state.write().poisoned = true;
    assert!(serde_json::to_vec(&s).unwrap_err().to_string().contains("poisoned"));
    let mut bytes = Vec::new();
    assert!(s.serialize_with(&mut serde_json::Serializer::new(&mut bytes)).is_err());
    assert!(matches!(s.snapshot(), Err(StreamError::LockPoisoned)));
    assert!(matches!(s.memory_usage(), Err(StreamError::LockPoisoned)));
}

#[tokio::test]
async fn restore_has_fresh_identity_locks_and_empty_signal_registry() {
    let mut core = RedisStream::new();
    core.xgroup_create(b"g", GroupStart::Id(StreamId::ZERO), None).unwrap();
    let s = SharedStream::new(core);
    let mut waiting = Box::pin(s.xreadgroup_blocking(
        b"g", b"c", GroupRead::New, ReadGroupOptions::default(), Block::Forever,
    ));
    assert!(once(waiting.as_mut()).await.is_pending());
    let before = s.memory_usage().unwrap();
    assert!(before.notification_bytes > 0);
    let restored: SharedStream = serde_json::from_slice(&serde_json::to_vec(&s).unwrap()).unwrap();
    let original = s.read().unwrap();
    let copy = restored.read().unwrap();
    assert_eq!(original.groups.len(), 1);
    assert!(copy.groups.is_empty());
    assert!(!Arc::ptr_eq(&s.0, &restored.0));
    assert!(!Arc::ptr_eq(&original.stream.group_identity(b"g").unwrap(),
                        &copy.stream.group_identity(b"g").unwrap()));
    drop(original);
    drop(copy);
    drop(waiting);
}

#[test]
fn serialization_finishes_before_returning_and_releases_the_lock() {
    let s = SharedStream::default();
    let mut bytes = Vec::new();
    let mut serializer = serde_json::Serializer::new(&mut bytes);
    s.serialize_with(&mut serializer).unwrap();
    assert!(!bytes.is_empty());
    assert!(s.0.state.try_write().is_some());
}

#[tokio::test]
async fn restored_group_readers_can_wait_and_receive_new_appends() {
    let mut core = RedisStream::new();
    core.xgroup_create(b"g", GroupStart::Id(StreamId::ZERO), None).unwrap();
    let restored: SharedStream = serde_json::from_slice(&serde_json::to_vec(&core).unwrap()).unwrap();
    let mut waiting = Box::pin(restored.xreadgroup_blocking(
        b"g", b"c", GroupRead::New, ReadGroupOptions::default(), Block::Forever,
    ));
    assert!(once(waiting.as_mut()).await.is_pending());
    let id = restored.xadd(AddId::Auto, vec![(b"f".to_vec(), b"v".to_vec())]).unwrap();
    let entries = tokio::time::timeout(Duration::from_secs(1), waiting).await.unwrap().unwrap();
    assert_eq!(entries[0].id, id);
    assert_eq!(restored.xpending(b"g").unwrap().total, 1);
}

#[tokio::test]
async fn expired_notification_weak_slots_still_count_their_allocation() {
    let mut core = RedisStream::new();
    core.xgroup_create(b"g", GroupStart::Id(StreamId::ZERO), None).unwrap();
    let s = SharedStream::new(core);
    let mut waiting = Box::pin(s.xreadgroup_blocking(
        b"g", b"c", GroupRead::New, ReadGroupOptions::default(), Block::Forever,
    ));
    assert!(once(waiting.as_mut()).await.is_pending());
    let active = s.memory_usage().unwrap();
    drop(waiting);
    let expired = s.memory_usage().unwrap();
    assert_eq!(active.notification_bytes, expired.notification_bytes);
    assert!(expired.notification_bytes > 0);
    let state = s.read().unwrap();
    assert!(state.groups.get(b"g".as_slice()).unwrap().upgrade().is_none());
}
