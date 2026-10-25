//! BLPOP through the TCP command loop and a real single-node Raft state machine.
use super::*;
use crate::cfg::config::Config;
use crate::node::raft_node::RaftNode;
use crate::raft::types::raft_types::Node;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use openraft::raft::AppendEntriesRequest;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::Framed;

const WAIT: Duration = Duration::from_secs(5);
type Socket = Framed<TcpStream, RespCodec>;
type Handler = JoinHandle<Result<(), CacheCatError>>;

fn command(parts: &[&str]) -> Value {
    Value::Array(Some(
        parts
            .iter()
            .map(|part| Value::BulkString(Some(Bytes::copy_from_slice(part.as_bytes()))))
            .collect(),
    ))
}

async fn cluster() -> (tempfile::TempDir, RaftNode, Arc<RedisServer>) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.raft.log_path = dir.path().to_str().unwrap().into();
    config.raft.address = "127.0.0.1:0".into();
    config.redis.databases = 2;
    let config = ParsedConfig::from(&config).unwrap();
    let (shutdown, _) = broadcast::channel(1);
    let node = RaftNode::create(config, shutdown).await.unwrap();
    let id = node.app.cluster.node_id();
    node.app
        .cluster
        .initialize(BTreeMap::from([(
            id,
            Node {
                node_id: id,
                endpoint: node.app.config.raft_advertise_endpoint.clone(),
            },
        )]))
        .await
        .unwrap();
    timeout(WAIT, async {
        while !node.app.cluster.is_leader() {
            tokio::task::yield_now().await;
        }
        node.app.cluster.lease_read().await.unwrap();
    })
    .await
    .expect("single-node cluster must become readable");
    let server = Arc::new(
        RedisServer::new(node.app.clone(), "127.0.0.1:0".into(), &node.app.config).unwrap(),
    );
    (dir, node, server)
}

async fn connect(server: &Arc<RedisServer>, id: u64) -> (Socket, Handler) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socket = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    socket.set_nodelay(true).unwrap();
    let (connection, peer) = listener.accept().await.unwrap();
    let handler = tokio::spawn(
        server
            .clone()
            .handle_connection_pipeline(connection, peer, id),
    );
    (Framed::new(socket, RespCodec::new()), handler)
}

async fn next(socket: &mut Socket) -> Value {
    timeout(WAIT, socket.next())
        .await
        .expect("BLPOP reply timed out")
        .expect("connection closed before reply")
        .unwrap()
}

async fn call(socket: &mut Socket, parts: &[&str]) -> Value {
    socket.send(command(parts)).await.unwrap();
    next(socket).await
}

async fn waiters(server: &RedisServer, expected: usize) {
    timeout(WAIT, async {
        while server
            .app
            .state_machine
            .data
            .kvs
            .list_blocking_keys
            .waiter_count()
            != expected
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("waiter count did not settle");
}

async fn close(socket: Socket, handler: Handler) {
    drop(socket);
    let _ = timeout(WAIT, handler)
        .await
        .expect("connection handler did not exit")
        .unwrap();
}

fn popped(key: &str, value: &str) -> Vec<u8> {
    command(&[key, value]).encode()
}

#[tokio::test]
async fn fifo_clients_consume_multi_element_and_consecutive_pushes() {
    let (_dir, node, server) = cluster().await;
    let mut consumers = Vec::new();
    for id in 1..=4 {
        let (mut socket, task) = connect(&server, id).await;
        socket
            .send(command(&["BLPOP", "queue", "0"]))
            .await
            .unwrap();
        waiters(&server, id as usize).await;
        consumers.push((socket, task));
    }
    let (mut producer, task) = connect(&server, 100).await;
    // Queue all producers before reading their replies. This also exercises
    // PUSH arriving while the oldest consumer's pop is still in flight.
    for args in [
        vec!["RPUSH", "queue", "one", "two"],
        vec!["RPUSH", "queue", "three"],
        vec!["RPUSH", "queue", "four"],
    ] {
        producer.feed(command(&args)).await.unwrap();
    }
    producer.flush().await.unwrap();
    for _ in 0..3 {
        assert!(matches!(next(&mut producer).await, Value::Integer(_)));
    }
    for ((mut socket, handler), value) in consumers.into_iter().zip(["one", "two", "three", "four"])
    {
        assert_eq!(next(&mut socket).await.encode(), popped("queue", value));
        close(socket, handler).await;
    }
    waiters(&server, 0).await;
    assert_eq!(
        call(&mut producer, &["LLEN", "queue"]).await.encode(),
        b":0\r\n"
    );
    close(producer, task).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn multiple_keys_are_ordered_and_waiters_are_scoped_to_the_selected_database() {
    let (_dir, node, server) = cluster().await;
    let (mut consumer, ct) = connect(&server, 1).await;
    let (mut producer, pt) = connect(&server, 2).await;
    consumer
        .send(command(&["BLPOP", "first", "second", "0"]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    assert_eq!(
        call(&mut producer, &["SELECT", "1"]).await.encode(),
        b"+OK\r\n"
    );
    call(&mut producer, &["RPUSH", "first", "other-db"]).await;
    assert!(
        timeout(Duration::from_millis(25), consumer.next())
            .await
            .is_err()
    );
    assert_eq!(
        server
            .app
            .state_machine
            .data
            .kvs
            .list_blocking_keys
            .waiter_count(),
        1
    );
    call(&mut producer, &["SELECT", "0"]).await;
    call(&mut producer, &["RPUSH", "second", "right-db"]).await;
    assert_eq!(
        next(&mut consumer).await.encode(),
        popped("second", "right-db")
    );
    waiters(&server, 0).await;
    call(&mut producer, &["RPUSH", "first", "priority"]).await;
    call(&mut producer, &["RPUSH", "second", "later"]).await;
    assert_eq!(
        call(&mut consumer, &["BLPOP", "first", "second", "0"])
            .await
            .encode(),
        popped("first", "priority")
    );
    call(&mut producer, &["SELECT", "1"]).await;
    assert_eq!(
        call(&mut producer, &["LPOP", "first"]).await.encode(),
        b"$8\r\nother-db\r\n"
    );
    close(consumer, ct).await;
    close(producer, pt).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn timeout_and_disconnect_clean_up_waiters_and_preserve_pipelined_input() {
    let (_dir, node, server) = cluster().await;
    let (mut consumer, ct) = connect(&server, 1).await;
    consumer
        .feed(command(&["BLPOP", "queue", "0.015"]))
        .await
        .unwrap();
    consumer.feed(command(&["PING"])).await.unwrap();
    consumer.flush().await.unwrap();
    assert_eq!(next(&mut consumer).await.encode(), b"*-1\r\n");
    assert_eq!(next(&mut consumer).await.encode(), b"+PONG\r\n");
    waiters(&server, 0).await;
    consumer
        .send(command(&["BLPOP", "queue", "0"]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    close(consumer, ct).await;
    waiters(&server, 0).await;

    let (mut consumer, ct) = connect(&server, 2).await;
    let (mut producer, pt) = connect(&server, 3).await;
    consumer
        .send(command(&["BLPOP", "queue", "0"]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    // Arrive while the command is already blocked, so input must be buffered
    // by the disconnect detector and returned to the normal connection loop.
    consumer.send(command(&["PING"])).await.unwrap();
    call(&mut producer, &["LPUSH", "queue", "alive"]).await;
    assert_eq!(next(&mut consumer).await.encode(), popped("queue", "alive"));
    assert_eq!(next(&mut consumer).await.encode(), b"+PONG\r\n");
    waiters(&server, 0).await;
    close(consumer, ct).await;
    close(producer, pt).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn multi_exec_treats_blpop_as_nonblocking_and_rejects_invalid_queue_arguments() {
    let (_dir, node, server) = cluster().await;
    let (mut socket, task) = connect(&server, 1).await;
    for args in [
        vec!["MULTI"],
        vec!["BLPOP", "queue", "0"],
        vec!["RPUSH", "queue", "first", "second"],
        vec!["BLPOP", "queue", "0"],
        vec!["EXEC"],
        vec!["PING"],
    ] {
        socket.feed(command(&args)).await.unwrap();
    }
    socket.flush().await.unwrap();
    assert_eq!(next(&mut socket).await.encode(), b"+OK\r\n");
    for _ in 0..3 {
        assert_eq!(next(&mut socket).await.encode(), b"+QUEUED\r\n");
    }
    assert_eq!(
        next(&mut socket).await.encode(),
        b"*3\r\n*-1\r\n:2\r\n*2\r\n$5\r\nqueue\r\n$5\r\nfirst\r\n"
    );
    assert_eq!(next(&mut socket).await.encode(), b"+PONG\r\n");
    waiters(&server, 0).await;
    call(&mut socket, &["MULTI"]).await;
    assert!(matches!(
        call(&mut socket, &["BLPOP", "queue", "-1"]).await,
        Value::Error(_)
    ));
    assert!(
        matches!(call(&mut socket, &["EXEC"]).await, Value::Error(error) if error.starts_with("EXECABORT"))
    );
    assert_eq!(
        call(&mut socket, &["LPOP", "queue"]).await.encode(),
        b"$6\r\nsecond\r\n"
    );
    close(socket, task).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_cancels_indefinite_blpop_and_closes_the_connection() {
    let (_dir, node, server) = cluster().await;
    let (mut socket, task) = connect(&server, 1).await;
    socket
        .send(command(&["BLPOP", "queue", "0"]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    node.app.shutdown().await;
    assert!(
        matches!(next(&mut socket).await, Value::Error(error) if error.starts_with("UNBLOCKED"))
    );
    assert!(timeout(WAIT, socket.next()).await.unwrap().is_none());
    waiters(&server, 0).await;
    close(socket, task).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_higher_raft_term_cancels_local_waiters_with_readonly_error() {
    let (_dir, node, server) = cluster().await;
    let (mut socket, task) = connect(&server, 1).await;
    socket
        .send(command(&["BLPOP", "queue", "0"]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    node.app
        .cluster
        .append_entries(AppendEntriesRequest {
            vote: openraft::Vote::new_committed(100, node.app.cluster.node_id().wrapping_add(1)),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        })
        .await
        .unwrap();
    assert!(
        matches!(next(&mut socket).await, Value::Error(error) if error.starts_with("READONLY"))
    );
    waiters(&server, 0).await;
    close(socket, task).await;
    node.app.cluster.shutdown().await.unwrap();
}

async fn applied_index(node: &RaftNode) -> u64 {
    node.app
        .state_machine
        .data
        .raft_meta_data
        .lock()
        .await
        .last_applied_log_id
        .as_ref()
        .expect("initialized cluster has an applied entry")
        .index
}

#[tokio::test]
async fn empty_wait_does_not_write_raft_and_available_data_needs_only_one_pop_log() {
    let (_dir, node, server) = cluster().await;
    let (mut socket, task) = connect(&server, 1).await;
    // create() does not start the application's logical-clock heartbeat task.
    // The completed write also places the baseline after leader initialization.
    call(&mut socket, &["RPUSH", "ready", "value"]).await;
    let before = applied_index(&node).await;
    socket
        .send(command(&["BLPOP", "missing", "0.1"]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    assert_eq!(applied_index(&node).await, before);
    assert_eq!(next(&mut socket).await.encode(), b"*-1\r\n");
    waiters(&server, 0).await;
    assert_eq!(applied_index(&node).await, before);
    assert_eq!(
        call(&mut socket, &["BLPOP", "missing", "ready", "0"])
            .await
            .encode(),
        popped("ready", "value")
    );
    assert_eq!(applied_index(&node).await, before + 1);
    close(socket, task).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_empty_raft_pop_after_a_stale_signal_registers_again_without_polling() {
    let (_dir, node, server) = cluster().await;
    let (mut consumer, ct) = connect(&server, 1).await;
    let (mut producer, pt) = connect(&server, 2).await;
    consumer
        .send(command(&["BLPOP", "queue", "0"]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    let before = applied_index(&node).await;
    // A normal LPOP may win after a producer wakes this BLPOP. Inject the
    // resulting stale readiness signal directly to make that race repeatable.
    assert_eq!(
        server
            .app
            .state_machine
            .data
            .kvs
            .list_blocking_keys
            .signal_one(&(0, Bytes::from_static(b"queue"))),
        1
    );
    timeout(WAIT, async {
        while applied_index(&node).await != before + 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the woken pop must submit its deterministic operation");
    assert!(
        timeout(Duration::from_millis(25), consumer.next())
            .await
            .is_err()
    );
    waiters(&server, 1).await;
    assert_eq!(applied_index(&node).await, before + 1);
    call(&mut producer, &["RPUSH", "queue", "eventually"]).await;
    assert_eq!(
        next(&mut consumer).await.encode(),
        popped("queue", "eventually")
    );
    waiters(&server, 0).await;
    assert_eq!(applied_index(&node).await, before + 3);
    close(consumer, ct).await;
    close(producer, pt).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn half_closed_connection_cancels_waiter_with_explicit_error() {
    let (_dir, node, server) = cluster().await;
    let (mut socket, task) = connect(&server, 1).await;
    socket
        .send(command(&["BLPOP", "queue", "0"]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    socket.get_mut().shutdown().await.unwrap();
    // EOF may race an already submitted pop, so it must not look like the
    // definitive null-array response returned by an unsubmitted timeout.
    assert!(
        matches!(next(&mut socket).await, Value::Error(error) if error.starts_with("UNBLOCKED"))
    );
    assert!(timeout(WAIT, socket.next()).await.unwrap().is_none());
    waiters(&server, 0).await;
    close(socket, task).await;
    node.app.cluster.shutdown().await.unwrap();
}
