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

// Run the contender on another OS thread: parking_lot contention blocks a poll
// instead of returning Pending, so a same-thread held-lock test would deadlock.
pub(super) fn with_contended_lock<T: Send>(held: impl Sized, action: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        scope.spawn(move || {
            started_tx.send(()).unwrap();
            done_tx.send(action()).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let blocked = done_rx.recv_timeout(Duration::from_millis(20));
        drop(held);
        assert!(matches!(blocked, Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap()
    })
}

#[test]
fn read_only_access_shares_the_read_lock_but_writes_wait() {
    let s = SharedStream::default();
    let held = s.0.state.read();
    assert_eq!(s.xlen().unwrap(), 0);
    with_contended_lock(held, || s.xadd(AddId::Auto, data())).unwrap();
    assert_eq!(s.xlen().unwrap(), 1);
}

#[tokio::test]
async fn timeout_includes_initial_lock_wait_for_both_read_modes() {
    let s = grouped();
    let held = s.0.state.write();
    let started = Instant::now();
    assert!(s.xread_blocking(ReadStart::Tail, None,
        Block::For(Duration::from_millis(20))).await.unwrap().is_empty());
    assert!(started.elapsed() >= Duration::from_millis(20));
    let started = Instant::now();
    assert!(s.xreadgroup_blocking(b"g", b"c", GroupRead::New, options(),
        Block::For(Duration::from_millis(20))).await.unwrap().is_empty());
    assert!(started.elapsed() >= Duration::from_millis(20));
    assert!(held.stream.xinfo_consumers(b"g").unwrap().is_empty());
    drop(held);
}

#[tokio::test]
async fn cancellation_of_notified_group_reader_hands_off() {
    let s = grouped();
    let mut a = Box::pin(s.xreadgroup_blocking(b"g", b"a", GroupRead::New, options(), Block::Forever));
    let mut b = Box::pin(s.xreadgroup_blocking(b"g", b"b", GroupRead::New, options(), Block::Forever));
    assert!(once(a.as_mut()).await.is_pending());
    assert!(once(b.as_mut()).await.is_pending());
    // Cancel the selected waiter before its next poll. Synchronous lock
    // acquisition no longer has a cancellable Pending interval after wakeup.
    let mut held = s.0.state.write();
    let id = held.stream.xadd(AddId::Auto, data()).unwrap();
    let signal = held.groups.get(b"g".as_slice()).unwrap().upgrade().unwrap();
    signal.changed.notify_one();
    drop(a);
    drop(held);
    let entries = timeout_at(Instant::now() + Duration::from_secs(1), b).await.unwrap().unwrap();
    assert_eq!(entries[0].id, id);
}

#[tokio::test(start_paused = true)]
async fn expired_notified_group_reader_hands_off_without_relocking() {
    let s = grouped();
    let mut a = Box::pin(s.xreadgroup_blocking(b"g", b"a", GroupRead::New, options(),
        Block::For(Duration::from_secs(1))));
    let mut b = Box::pin(s.xreadgroup_blocking(b"g", b"b", GroupRead::New, options(), Block::Forever));
    assert!(once(a.as_mut()).await.is_pending());
    assert!(once(b.as_mut()).await.is_pending());
    let mut held = s.0.state.write();
    let id = held.stream.xadd(AddId::Auto, data()).unwrap();
    let signal = held.groups.get(b"g".as_slice()).unwrap().upgrade().unwrap();
    signal.changed.notify_one();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(a.await.unwrap().is_empty());
    drop(held);
    assert_eq!(b.await.unwrap()[0].id, id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_readers_are_send_and_release_locks_while_waiting() {
    let s = grouped();
    let ordinary = {
        let s = s.clone();
        tokio::spawn(async move {
            s.xread_blocking(ReadStart::After(StreamId::ZERO), None,
                Block::For(Duration::from_secs(1))).await
        })
    };
    let group = {
        let s = s.clone();
        tokio::spawn(async move {
            s.xreadgroup_blocking(b"g", b"c", GroupRead::New, options(),
                Block::For(Duration::from_secs(1))).await
        })
    };
    let id = {
        let s = s.clone();
        tokio::spawn(async move { s.xadd(AddId::Auto, data()) }).await.unwrap().unwrap()
    };
    assert_eq!(ordinary.await.unwrap().unwrap()[0].id, id);
    assert_eq!(group.await.unwrap().unwrap()[0].id, id);
}

#[tokio::test]
async fn panicking_mutation_poisoning_wakes_blocked_readers() {
    let s = grouped();
    let mut ordinary = Box::pin(s.xread_blocking(ReadStart::Tail, None, Block::Forever));
    let mut group = Box::pin(s.xreadgroup_blocking(b"g", b"c", GroupRead::New, options(), Block::Forever));
    assert!(once(ordinary.as_mut()).await.is_pending());
    assert!(once(group.as_mut()).await.is_pending());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        s.modify::<()>(|_| panic!("mutation failed"))
    }));
    assert!(panic.is_err());
    assert!(matches!(ordinary.await, Err(StreamError::LockPoisoned)));
    assert!(matches!(group.await, Err(StreamError::LockPoisoned)));
    assert!(matches!(s.xadd(AddId::Auto, data()), Err(StreamError::LockPoisoned)));
}

#[tokio::test]
async fn ack_does_not_broadcast_to_ordinary_readers() {
    let s = grouped();
    let id = s.xadd(AddId::Auto, data()).unwrap();
    s.xreadgroup(b"g", b"a", GroupRead::New, options()).unwrap();
    let mut notification = Box::pin(s.0.readers.notified());
    notification.as_mut().enable();
    s.xack(b"g", &[id]).unwrap();
    assert!(once(notification.as_mut()).await.is_pending());
    s.xadd(AddId::Auto, data()).unwrap();
    assert!(once(notification.as_mut()).await.is_ready());
}

#[tokio::test]
async fn empty_waiter_queues_are_reclaimed_on_next_append() {
    let s = grouped();
    let mut f = Box::pin(s.xreadgroup_blocking(b"g", b"a", GroupRead::New, options(), Block::Forever));
    assert!(once(f.as_mut()).await.is_pending());
    drop(f);
    {
        let state = s.0.state.read();
        assert_eq!(state.groups.len(), 1);
        assert!(state.groups.values().all(|signal| signal.upgrade().is_none()));
    }
    s.xadd(AddId::Auto, data()).unwrap();
    assert!(s.0.state.read().groups.is_empty());
}

#[test]
fn fast_group_reads_do_not_allocate_a_waiter_queue() {
    let s = grouped();
    s.xadd(AddId::Auto, data()).unwrap();
    let entries = s.xreadgroup(b"g", b"a", GroupRead::New, options()).unwrap();
    assert_eq!(entries.len(), 1);
    assert!(s.0.state.read().groups.is_empty());
}
