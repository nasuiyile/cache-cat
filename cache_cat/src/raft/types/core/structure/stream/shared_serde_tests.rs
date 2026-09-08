use super::*;
use std::{pin::Pin, task::Poll};

async fn once<F: Future>(mut f: Pin<&mut F>) -> Poll<F::Output> {
    std::future::poll_fn(|cx| Poll::Ready(f.as_mut().poll(cx))).await
}

#[tokio::test]
async fn direct_serde_fails_on_contention_instead_of_panicking_or_blocking() {
    let s = SharedStream::default();
    let held = s.0.state.write().await;
    assert!(serde_json::to_vec(&s).unwrap_err().to_string().contains("read lock is busy"));
    drop(held);
    assert!(serde_json::to_vec(&s).is_ok());
}

#[tokio::test]
async fn async_encode_snapshot_and_memory_wait_for_write_lock() {
    let s = SharedStream::default();
    let held = s.0.state.write().await;
    let mut bytes = Vec::new();
    let mut serializer = serde_json::Serializer::new(&mut bytes);
    let mut encoding = Box::pin(s.serialize_with(&mut serializer));
    let mut snapshot = Box::pin(s.snapshot());
    let mut memory = Box::pin(s.memory_usage());
    assert!(once(encoding.as_mut()).await.is_pending());
    assert!(once(snapshot.as_mut()).await.is_pending());
    assert!(once(memory.as_mut()).await.is_pending());
    drop(held);
    encoding.await.unwrap();
    assert_eq!(snapshot.await.unwrap().entries.len(), 0);
    assert_eq!(memory.await.unwrap().entries, 0);
}

#[tokio::test]
async fn serde_and_estimator_share_read_access() {
    let s = SharedStream::default();
    let held = s.0.state.read().await;
    assert!(serde_json::to_vec(&s).is_ok());
    let mut snapshot = Box::pin(s.snapshot());
    let mut memory = Box::pin(s.memory_usage());
    assert!(matches!(once(snapshot.as_mut()).await, Poll::Ready(Ok(_))));
    assert!(matches!(once(memory.as_mut()).await, Poll::Ready(Ok(_))));
    drop(held);
}

#[tokio::test]
async fn poisoned_stream_cannot_be_serialized_or_inspected() {
    let s = SharedStream::default();
    s.0.state.write().await.poisoned = true;
    assert!(serde_json::to_vec(&s).unwrap_err().to_string().contains("poisoned"));
    let mut bytes = Vec::new();
    assert!(s.serialize_with(&mut serde_json::Serializer::new(&mut bytes)).await.is_err());
    assert!(matches!(s.snapshot().await, Err(StreamError::LockPoisoned)));
    assert!(matches!(s.memory_usage().await, Err(StreamError::LockPoisoned)));
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
    let before = s.memory_usage().await.unwrap();
    assert!(before.notification_bytes > 0);
    let restored: SharedStream = serde_json::from_slice(&serde_json::to_vec(&s).unwrap()).unwrap();
    let original = s.read().await.unwrap();
    let copy = restored.read().await.unwrap();
    assert_eq!(original.groups.len(), 1);
    assert!(copy.groups.is_empty());
    assert!(!Arc::ptr_eq(&s.0, &restored.0));
    assert!(!Arc::ptr_eq(&original.stream.group_identity(b"g").unwrap(),
                        &copy.stream.group_identity(b"g").unwrap()));
    drop(original);
    drop(copy);
    drop(waiting);
}

#[tokio::test]
async fn cancelling_an_encoder_queued_on_the_lock_does_not_strand_writers() {
    let s = SharedStream::default();
    let held = s.0.state.write().await;
    let mut bytes = Vec::new();
    let mut serializer = serde_json::Serializer::new(&mut bytes);
    let mut encoding = Box::pin(s.serialize_with(&mut serializer));
    assert!(once(encoding.as_mut()).await.is_pending());
    drop(encoding);
    drop(held);
    assert!(bytes.is_empty());
    tokio::time::timeout(Duration::from_secs(1), s.xlen()).await.unwrap().unwrap();
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
    let id = restored.xadd(AddId::Auto, vec![(b"f".to_vec(), b"v".to_vec())]).await.unwrap();
    let entries = tokio::time::timeout(Duration::from_secs(1), waiting).await.unwrap().unwrap();
    assert_eq!(entries[0].id, id);
    assert_eq!(restored.xpending(b"g").await.unwrap().total, 1);
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
    let active = s.memory_usage().await.unwrap();
    drop(waiting);
    let expired = s.memory_usage().await.unwrap();
    assert_eq!(active.notification_bytes, expired.notification_bytes);
    assert!(expired.notification_bytes > 0);
    let state = s.read().await.unwrap();
    assert!(state.groups.get(b"g".as_slice()).unwrap().upgrade().is_none());
}
