use super::*;
use std::{pin::Pin, task::Poll};

fn data() -> Fields { vec![(b"f".to_vec(), b"v".to_vec())] }
fn options() -> ReadGroupOptions {
    ReadGroupOptions { count: Some(1), no_ack: false }
}
fn grouped() -> SharedStream {
    let mut core = RedisStream::new();
    core.xgroup_create(b"g", GroupStart::Id(StreamId::ZERO), None).unwrap();
    SharedStream::new(core)
}
async fn once<F: Future>(mut f: Pin<&mut F>) -> Poll<F::Output> {
    std::future::poll_fn(|cx| Poll::Ready(f.as_mut().poll(cx))).await
}

#[tokio::test]
async fn read_only_access_shares_the_read_lock_but_writes_wait() {
    let s = SharedStream::default();
    let held = s.0.state.read().await;
    let mut read = Box::pin(s.xlen());
    assert!(matches!(once(read.as_mut()).await, Poll::Ready(Ok(0))));
    let mut write = Box::pin(s.xadd(AddId::Auto, data()));
    assert!(once(write.as_mut()).await.is_pending());
    drop(held);
    write.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn timeout_includes_initial_lock_wait_for_both_read_modes() {
    let s = grouped();
    let held = s.0.state.write().await;
    let mut ordinary = Box::pin(s.xread_blocking(ReadStart::Tail, None,
        Block::For(Duration::from_secs(5))));
    let mut group = Box::pin(s.xreadgroup_blocking(b"g", b"c", GroupRead::New, options(),
        Block::For(Duration::from_secs(5))));
    assert!(once(ordinary.as_mut()).await.is_pending());
    assert!(once(group.as_mut()).await.is_pending());
    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(ordinary.await.unwrap().is_empty());
    assert!(group.await.unwrap().is_empty());
    drop(held);
}

#[tokio::test]
async fn cancellation_after_consuming_notify_before_relocking_hands_off() {
    let s = grouped();
    let mut a = Box::pin(s.xreadgroup_blocking(b"g", b"a", GroupRead::New, options(), Block::Forever));
    let mut b = Box::pin(s.xreadgroup_blocking(b"g", b"b", GroupRead::New, options(), Block::Forever));
    assert!(once(a.as_mut()).await.is_pending());
    assert!(once(b.as_mut()).await.is_pending());
    // Deliberately hold the lock to force A into the exact vulnerable interval.
    let mut held = s.0.state.write().await;
    let id = held.stream.xadd(AddId::Auto, data()).unwrap();
    let signal = held.groups.get(b"g".as_slice()).unwrap().upgrade().unwrap();
    signal.changed.notify_one();
    assert!(once(a.as_mut()).await.is_pending()); // notification consumed; lock queued
    drop(a); // Only the relay can cover cancellation AFTER notification completion.
    drop(held);
    let entries = timeout_at(Instant::now() + Duration::from_secs(1), b).await.unwrap().unwrap();
    assert_eq!(entries[0].id, id);
}

#[tokio::test(start_paused = true)]
async fn timeout_after_consuming_notify_before_relocking_hands_off() {
    let s = grouped();
    let mut a = Box::pin(s.xreadgroup_blocking(b"g", b"a", GroupRead::New, options(),
        Block::For(Duration::from_secs(1))));
    let mut b = Box::pin(s.xreadgroup_blocking(b"g", b"b", GroupRead::New, options(), Block::Forever));
    assert!(once(a.as_mut()).await.is_pending());
    assert!(once(b.as_mut()).await.is_pending());
    let mut held = s.0.state.write().await;
    let id = held.stream.xadd(AddId::Auto, data()).unwrap();
    let signal = held.groups.get(b"g".as_slice()).unwrap().upgrade().unwrap();
    signal.changed.notify_one();
    assert!(once(a.as_mut()).await.is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(a.await.unwrap().is_empty());
    drop(held);
    assert_eq!(b.await.unwrap()[0].id, id);
}

#[tokio::test]
async fn ack_does_not_broadcast_to_ordinary_readers() {
    let s = grouped();
    let id = s.xadd(AddId::Auto, data()).await.unwrap();
    s.xreadgroup(b"g", b"a", GroupRead::New, options()).await.unwrap();
    let mut notification = Box::pin(s.0.readers.notified());
    notification.as_mut().enable();
    s.xack(b"g", &[id]).await.unwrap();
    assert!(once(notification.as_mut()).await.is_pending());
    s.xadd(AddId::Auto, data()).await.unwrap();
    assert!(once(notification.as_mut()).await.is_ready());
}

#[tokio::test]
async fn empty_waiter_queues_are_reclaimed_on_next_append() {
    let s = grouped();
    let mut f = Box::pin(s.xreadgroup_blocking(b"g", b"a", GroupRead::New, options(), Block::Forever));
    assert!(once(f.as_mut()).await.is_pending());
    drop(f);
    {
        let state = s.0.state.read().await;
        assert_eq!(state.groups.len(), 1);
        assert!(state.groups.values().all(|signal| signal.upgrade().is_none()));
    }
    s.xadd(AddId::Auto, data()).await.unwrap();
    assert!(s.0.state.read().await.groups.is_empty());
}

#[tokio::test]
async fn fast_group_reads_do_not_allocate_a_waiter_queue() {
    let s = grouped();
    s.xadd(AddId::Auto, data()).await.unwrap();
    let entries = s.xreadgroup(b"g", b"a", GroupRead::New, options()).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert!(s.0.state.read().await.groups.is_empty());
}
