//! Consumer-group reads through TCP and a real single-node Raft state machine.
use super::*;
use crate::cfg::config::Config;
use crate::node::raft_node::RaftNode;
use crate::raft::types::core::response_value::Resp2MapEncoding;
use crate::raft::types::core::structure::stream::StreamSnapshot;
use crate::raft::types::core::value_object::ValueObject;
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
        .expect("XREADGROUP reply timed out")
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
            .write_lock
            .lock()
            .await
            .group_blocking_keys
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

fn stream_reply(streams: &[(&str, &[(&str, Option<&str>)])], proto: u8) -> Vec<u8> {
    let entries = streams
        .iter()
        .map(|(key, entries)| {
            let messages = entries
                .iter()
                .map(|(id, value)| {
                    Value::Array(Some(vec![
                        Value::BulkString(Some(Bytes::copy_from_slice(id.as_bytes()))),
                        value
                            .map(|value| command(&["f", value]))
                            .unwrap_or(Value::Array(None)),
                    ]))
                })
                .collect();
            (
                Value::BulkString(Some(Bytes::copy_from_slice(key.as_bytes()))),
                Value::Array(Some(messages)),
            )
        })
        .collect();
    Value::MapWithResp2 {
        entries,
        resp2: Resp2MapEncoding::Pairs,
    }
    .encode_proto(proto)
}

async fn create_group(socket: &mut Socket, key: &str, group: &str) {
    assert_eq!(
        call(socket, &["XGROUP", "CREATE", key, group, "0", "MKSTREAM"])
            .await
            .encode(),
        b"+OK\r\n"
    );
}

async fn snapshot(server: &RedisServer, db: usize, key: &str) -> StreamSnapshot {
    let cache = &server.app.state_machine.data.kvs;
    let _write = cache.write_lock.lock().await;
    let entry = cache.databases[db].mocha.get_entry(key.as_bytes()).unwrap();
    let ValueObject::Stream(stream) = entry.value.data else {
        panic!("expected a stream")
    };
    let saved = stream.read().snapshot();
    saved
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
        .unwrap()
        .index
}

#[tokio::test]
async fn group_reads_record_pending_history_and_noack_without_removing_messages() {
    let (_dir, node, server) = cluster().await;
    let (mut socket, task) = connect(&server, 1).await;
    create_group(&mut socket, "s", "g").await;
    for (id, value) in [("1-0", "one"), ("2-0", "two"), ("3-0", "three")] {
        call(&mut socket, &["XADD", "s", id, "f", value]).await;
    }
    assert_eq!(
        call(
            &mut socket,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "alice",
                "COUNT",
                "1",
                "STREAMS",
                "s",
                ">"
            ]
        )
        .await
        .encode(),
        stream_reply(&[("s", &[("1-0", Some("one"))])], 2)
    );
    assert_eq!(
        call(
            &mut socket,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "bob",
                "COUNT",
                "1",
                "STREAMS",
                "s",
                ">"
            ]
        )
        .await
        .encode(),
        stream_reply(&[("s", &[("2-0", Some("two"))])], 2)
    );
    assert_eq!(
        call(
            &mut socket,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "alice",
                "NOACK",
                "STREAMS",
                "s",
                ">"
            ]
        )
        .await
        .encode(),
        stream_reply(&[("s", &[("3-0", Some("three"))])], 2)
    );
    // Explicit IDs re-deliver only this consumer's PEL; BLOCK and NOACK are ignored.
    assert_eq!(
        call(
            &mut socket,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "alice",
                "BLOCK",
                "0",
                "NOACK",
                "STREAMS",
                "s",
                "0"
            ]
        )
        .await
        .encode(),
        stream_reply(&[("s", &[("1-0", Some("one"))])], 2)
    );
    assert_eq!(
        call(
            &mut socket,
            &["XREADGROUP", "GROUP", "g", "bob", "STREAMS", "s", "0"]
        )
        .await
        .encode(),
        stream_reply(&[("s", &[("2-0", Some("two"))])], 2)
    );
    let saved = snapshot(&server, 0, "s").await;
    assert_eq!(
        saved.entries.len(),
        3,
        "group delivery does not delete stream data"
    );
    assert_eq!(saved.groups[0].pending.len(), 2);
    assert_eq!(saved.groups[0].pending[0].deliveries, 2);
    assert_eq!(saved.groups[0].pending[1].deliveries, 2);
    assert_eq!(saved.groups[0].last_delivered.to_string(), "3-0");
    // XDEL is not exposed by the protocol yet. Prepare a deleted payload
    // directly under the cache writer; the XREADGROUP reply is still tested
    // through TCP/Raft, including its retained PEL and delivery counter.
    {
        let cache = &server.app.state_machine.data.kvs;
        let _write = cache.write_lock.lock().await;
        let entry = cache.databases[0].mocha.get_entry(b"s".as_slice()).unwrap();
        let ValueObject::Stream(stream) = entry.value.data else {
            unreachable!()
        };
        assert_eq!(stream.write().xdel(&["1-0".parse().unwrap()]), 1);
    }
    assert_eq!(
        call(
            &mut socket,
            &["XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", "0"]
        )
        .await
        .encode(),
        stream_reply(&[("s", &[("1-0", None)])], 2)
    );
    assert_eq!(
        snapshot(&server, 0, "s").await.groups[0].pending[0].deliveries,
        2
    );
    close(socket, task).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_reads_create_consumers_and_history_returns_an_empty_stream_array() {
    let (_dir, node, server) = cluster().await;
    let (mut socket, task) = connect(&server, 1).await;
    create_group(&mut socket, "s", "g").await;
    assert_eq!(
        call(
            &mut socket,
            &["XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", ">"]
        )
        .await
        .encode(),
        b"*-1\r\n"
    );
    assert_eq!(
        snapshot(&server, 0, "s").await.groups[0].consumers[0].name,
        b"alice"
    );
    assert_eq!(
        call(
            &mut socket,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "bob",
                "BLOCK",
                "0",
                "STREAMS",
                "s",
                "0"
            ]
        )
        .await
        .encode(),
        stream_reply(&[("s", &[])], 2)
    );
    assert_eq!(snapshot(&server, 0, "s").await.groups[0].consumers.len(), 2);
    waiters(&server, 0).await;
    close(socket, task).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn multiple_streams_validate_before_mutation_and_duplicate_keys_advance_in_order() {
    let (_dir, node, server) = cluster().await;
    let (mut socket, task) = connect(&server, 1).await;
    create_group(&mut socket, "a", "g").await;
    call(&mut socket, &["XADD", "a", "1-0", "f", "a1"]).await;
    assert!(
        matches!(call(&mut socket, &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "a", "b", ">", ">"]).await,
        Value::Error(error) if error.starts_with("NOGROUP"))
    );
    assert!(snapshot(&server, 0, "a").await.groups[0].pending.is_empty());
    create_group(&mut socket, "b", "g").await;
    call(&mut socket, &["XADD", "b", "1-0", "f", "b1"]).await;
    assert_eq!(
        call(
            &mut socket,
            &[
                "XREADGROUP",
                "COUNT",
                "1",
                "GROUP",
                "g",
                "c",
                "STREAMS",
                "a",
                "b",
                ">",
                ">"
            ]
        )
        .await
        .encode(),
        stream_reply(
            &[("a", &[("1-0", Some("a1"))]), ("b", &[("1-0", Some("b1"))])],
            2
        )
    );
    call(&mut socket, &["XADD", "a", "2-0", "f", "a2"]).await;
    call(&mut socket, &["XADD", "a", "3-0", "f", "a3"]).await;
    assert_eq!(
        call(
            &mut socket,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "COUNT",
                "1",
                "STREAMS",
                "a",
                "a",
                ">",
                ">"
            ]
        )
        .await
        .encode(),
        stream_reply(
            &[("a", &[("2-0", Some("a2"))]), ("a", &[("3-0", Some("a3"))])],
            2
        )
    );
    assert_eq!(snapshot(&server, 0, "a").await.groups[0].pending.len(), 3);
    close(socket, task).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn fifo_consumers_share_a_group_while_other_groups_receive_the_same_entries() {
    let (_dir, node, server) = cluster().await;
    let (mut producer, pt) = connect(&server, 100).await;
    create_group(&mut producer, "s", "g").await;
    create_group(&mut producer, "s", "other").await;
    let mut consumers = Vec::new();
    for (id, group, name, count) in [
        (1, "g", "alice", "1"),
        (2, "g", "bob", "1"),
        (3, "other", "carol", "2"),
    ] {
        let (mut socket, task) = connect(&server, id).await;
        socket
            .send(command(&[
                "XREADGROUP",
                "GROUP",
                group,
                name,
                "COUNT",
                count,
                "BLOCK",
                "0",
                "STREAMS",
                "s",
                ">",
            ]))
            .await
            .unwrap();
        waiters(&server, id as usize).await;
        consumers.push((socket, task));
    }
    call(&mut producer, &["MULTI"]).await;
    call(&mut producer, &["XADD", "s", "1-0", "f", "one"]).await;
    call(&mut producer, &["XADD", "s", "2-0", "f", "two"]).await;
    assert!(matches!(
        call(&mut producer, &["EXEC"]).await,
        Value::Array(Some(_))
    ));
    let expected = [
        stream_reply(&[("s", &[("1-0", Some("one"))])], 2),
        stream_reply(&[("s", &[("2-0", Some("two"))])], 2),
        stream_reply(&[("s", &[("1-0", Some("one")), ("2-0", Some("two"))])], 2),
    ];
    for ((mut socket, task), expected) in consumers.into_iter().zip(expected) {
        assert_eq!(next(&mut socket).await.encode(), expected);
        close(socket, task).await;
    }
    waiters(&server, 0).await;
    let saved = snapshot(&server, 0, "s").await;
    assert!(saved.groups.iter().all(|group| group.pending.len() == 2));
    close(producer, pt).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn blocking_group_reads_are_scoped_to_database_and_buffer_following_commands() {
    let (_dir, node, server) = cluster().await;
    let (mut producer, pt) = connect(&server, 100).await;
    let (mut consumer, ct) = connect(&server, 1).await;
    create_group(&mut producer, "s", "g").await;
    call(&mut producer, &["SELECT", "1"]).await;
    create_group(&mut producer, "s", "g").await;
    consumer
        .send(command(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    call(&mut producer, &["XADD", "s", "1-0", "f", "other-db"]).await;
    assert!(
        timeout(Duration::from_millis(25), consumer.next())
            .await
            .is_err()
    );
    waiters(&server, 1).await;
    consumer.send(command(&["PING"])).await.unwrap();
    call(&mut producer, &["SELECT", "0"]).await;
    call(&mut producer, &["XADD", "s", "1-0", "f", "right-db"]).await;
    assert_eq!(
        next(&mut consumer).await.encode(),
        stream_reply(&[("s", &[("1-0", Some("right-db"))])], 2)
    );
    assert_eq!(next(&mut consumer).await.encode(), b"+PONG\r\n");
    assert!(snapshot(&server, 1, "s").await.groups[0].pending.is_empty());
    waiters(&server, 0).await;
    close(consumer, ct).await;
    close(producer, pt).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn group_timeout_stops_after_initial_metadata_write_and_ready_read_uses_one_log() {
    let (_dir, node, server) = cluster().await;
    let (mut socket, task) = connect(&server, 1).await;
    create_group(&mut socket, "s", "g").await;
    let before = applied_index(&node).await;
    socket
        .send(command(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "500",
            "STREAMS",
            "s",
            ">",
        ]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    let waiting = applied_index(&node).await;
    assert_eq!(
        waiting,
        before + 1,
        "initial group read replicates consumer creation"
    );
    assert_eq!(next(&mut socket).await.encode(), b"*-1\r\n");
    waiters(&server, 0).await;
    assert_eq!(
        applied_index(&node).await,
        waiting,
        "waiting must not poll through Raft"
    );
    call(&mut socket, &["XADD", "s", "1-0", "f", "one"]).await;
    let before = applied_index(&node).await;
    assert_eq!(
        call(
            &mut socket,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "BLOCK",
                "0",
                "STREAMS",
                "s",
                ">"
            ]
        )
        .await
        .encode(),
        stream_reply(&[("s", &[("1-0", Some("one"))])], 2)
    );
    assert_eq!(applied_index(&node).await, before + 1);
    close(socket, task).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn destroy_delete_and_type_replacement_unblock_group_readers_with_errors() {
    let (_dir, node, server) = cluster().await;
    let (mut producer, pt) = connect(&server, 100).await;
    for (index, (operation, prefix)) in [
        (vec!["XGROUP", "DESTROY", "s", "g"], "NOGROUP"),
        (vec!["DEL", "s"], "NOGROUP"),
        (vec!["SET", "s", "string"], "WRONGTYPE"),
        (vec!["FLUSHDB"], "NOGROUP"),
    ]
    .into_iter()
    .enumerate()
    {
        call(&mut producer, &["DEL", "s"]).await;
        create_group(&mut producer, "s", "g").await;
        let (mut consumer, ct) = connect(&server, index as u64 + 1).await;
        consumer
            .send(command(&[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "BLOCK",
                "0",
                "STREAMS",
                "s",
                ">",
            ]))
            .await
            .unwrap();
        waiters(&server, 1).await;
        call(&mut producer, &operation).await;
        assert!(
            matches!(next(&mut consumer).await, Value::Error(error) if error.starts_with(prefix)),
            "{operation:?}"
        );
        waiters(&server, 0).await;
        close(consumer, ct).await;
    }
    close(producer, pt).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn group_reads_inside_exec_do_not_block_and_resp3_replies_are_maps() {
    let (_dir, node, server) = cluster().await;
    let (mut socket, task) = connect(&server, 1).await;
    create_group(&mut socket, "s", "g").await;
    call(&mut socket, &["MULTI"]).await;
    for args in [
        vec![
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ],
        vec!["XADD", "s", "1-0", "f", "one"],
        vec![
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ],
    ] {
        assert_eq!(call(&mut socket, &args).await.encode(), b"+QUEUED\r\n");
    }
    let Value::Array(Some(replies)) = call(&mut socket, &["EXEC"]).await else {
        panic!("EXEC reply")
    };
    assert_eq!(replies.len(), 3);
    assert_eq!(replies[0].encode(), b"*-1\r\n");
    assert_eq!(
        replies[2].encode(),
        stream_reply(&[("s", &[("1-0", Some("one"))])], 2)
    );
    waiters(&server, 0).await;
    assert!(matches!(
        call(&mut socket, &["HELLO", "3"]).await,
        Value::Map(_)
    ));
    socket.codec_mut().switch_resp3();
    let reply = call(
        &mut socket,
        &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "0"],
    )
    .await;
    assert!(matches!(reply, Value::Map(_)));
    assert_eq!(
        reply.encode_proto(3),
        stream_reply(&[("s", &[("1-0", Some("one"))])], 3)
    );
    close(socket, task).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn group_waiters_are_cancelled_on_disconnect_shutdown_and_leader_loss() {
    let (_dir, node, server) = cluster().await;
    let (mut setup, st) = connect(&server, 100).await;
    create_group(&mut setup, "s", "g").await;
    close(setup, st).await;
    let (mut socket, task) = connect(&server, 1).await;
    socket
        .send(command(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    close(socket, task).await;
    waiters(&server, 0).await;

    let (mut socket, task) = connect(&server, 2).await;
    socket
        .send(command(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    socket.get_mut().shutdown().await.unwrap();
    assert!(
        matches!(next(&mut socket).await, Value::Error(error) if error.starts_with("UNBLOCKED"))
    );
    close(socket, task).await;
    waiters(&server, 0).await;

    let (mut socket, task) = connect(&server, 3).await;
    socket
        .send(command(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ]))
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

    let (_dir, node, server) = cluster().await;
    let (mut socket, task) = connect(&server, 1).await;
    create_group(&mut socket, "s", "g").await;
    socket
        .send(command(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    node.app.shutdown().await;
    assert!(
        matches!(next(&mut socket).await, Value::Error(error) if error.starts_with("UNBLOCKED"))
    );
    waiters(&server, 0).await;
    close(socket, task).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn lua_group_reads_are_nonblocking_and_only_final_producer_state_notifies() {
    let (_dir, node, server) = cluster().await;
    let (mut producer, pt) = connect(&server, 100).await;
    let (mut consumer, ct) = connect(&server, 1).await;
    create_group(&mut producer, "s", "g").await;
    let script =
        "return redis.call('XREADGROUP','GROUP','g','lua','BLOCK','0','STREAMS',KEYS[1],'>')";
    assert_eq!(
        call(&mut producer, &["EVAL", script, "1", "s"])
            .await
            .encode(),
        b"$-1\r\n"
    );
    waiters(&server, 0).await;
    consumer
        .send(command(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    let before = applied_index(&node).await;
    let script = "redis.call('XADD',KEYS[1],'1-0','f','transient'); redis.call('XGROUP','SETID',KEYS[1],'g','1-0'); return 1";
    assert_eq!(
        call(&mut producer, &["EVAL", script, "1", "s"])
            .await
            .encode(),
        b":1\r\n"
    );
    assert!(
        timeout(Duration::from_millis(25), consumer.next())
            .await
            .is_err()
    );
    waiters(&server, 1).await;
    assert_eq!(
        applied_index(&node).await,
        before + 1,
        "intermediate script data must not trigger a group read"
    );
    let script = "redis.call('XADD',KEYS[1],'2-0','f','survives'); return redis.error_reply('ERR deliberate')";
    assert!(matches!(
        call(&mut producer, &["EVAL", script, "1", "s"]).await,
        Value::Error(_)
    ));
    assert_eq!(
        next(&mut consumer).await.encode(),
        stream_reply(&[("s", &[("2-0", Some("survives"))])], 2)
    );
    waiters(&server, 0).await;
    // A script can re-read its own PEL without registering a network waiter.
    let script =
        "return redis.call('XREADGROUP','GROUP','g','c','BLOCK','0','STREAMS',KEYS[1],'0')";
    assert_eq!(
        call(&mut producer, &["EVAL", script, "1", "s"])
            .await
            .encode(),
        stream_reply(&[("s", &[("2-0", Some("survives"))])], 2)
    );
    close(consumer, ct).await;
    close(producer, pt).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn natural_expiration_and_shortened_ttl_unblock_without_polling_raft() {
    let (_dir, node, server) = cluster().await;
    let (mut producer, pt) = connect(&server, 100).await;
    for (id, key, shorten) in [(1, "natural", false), (2, "shortened", true)] {
        create_group(&mut producer, key, "g").await;
        let initial_ttl = if shorten { "60000" } else { "1000" };
        assert_eq!(
            call(&mut producer, &["PEXPIRE", key, initial_ttl])
                .await
                .encode(),
            b":1\r\n"
        );
        let (mut consumer, ct) = connect(&server, id).await;
        consumer
            .send(command(&[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "BLOCK",
                "0",
                "STREAMS",
                key,
                ">",
            ]))
            .await
            .unwrap();
        waiters(&server, 1).await;
        if shorten {
            assert_eq!(
                call(&mut producer, &["PEXPIRE", key, "100"]).await.encode(),
                b":1\r\n"
            );
        }
        let before = applied_index(&node).await;
        assert!(
            matches!(next(&mut consumer).await, Value::Error(error) if error.starts_with("NOGROUP"))
        );
        waiters(&server, 0).await;
        assert_eq!(
            applied_index(&node).await,
            before,
            "expiration readiness must not poll or mutate via Raft"
        );
        close(consumer, ct).await;
    }
    close(producer, pt).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn persist_cancels_the_previous_expiration_timer_without_consuming_data() {
    let (_dir, node, server) = cluster().await;
    let (mut producer, pt) = connect(&server, 100).await;
    let (mut consumer, ct) = connect(&server, 1).await;
    create_group(&mut producer, "s", "g").await;
    call(&mut producer, &["PEXPIRE", "s", "1000"]).await;
    consumer
        .send(command(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    assert_eq!(
        call(&mut producer, &["PERSIST", "s"]).await.encode(),
        b":1\r\n"
    );
    let before = applied_index(&node).await;
    assert!(
        timeout(Duration::from_millis(1100), consumer.next())
            .await
            .is_err()
    );
    waiters(&server, 1).await;
    assert_eq!(
        applied_index(&node).await,
        before,
        "refreshing the local timer needs no new read entry"
    );
    call(&mut producer, &["XADD", "s", "1-0", "f", "survived"]).await;
    assert_eq!(
        next(&mut consumer).await.encode(),
        stream_reply(&[("s", &[("1-0", Some("survived"))])], 2)
    );
    waiters(&server, 0).await;
    close(consumer, ct).await;
    close(producer, pt).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn destroying_and_recreating_a_group_in_one_exec_cancels_the_old_waiter() {
    let (_dir, node, server) = cluster().await;
    let (mut producer, pt) = connect(&server, 100).await;
    let (mut consumer, ct) = connect(&server, 1).await;
    create_group(&mut producer, "s", "g").await;
    consumer
        .send(command(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "old",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    call(&mut producer, &["MULTI"]).await;
    call(&mut producer, &["XGROUP", "DESTROY", "s", "g"]).await;
    call(&mut producer, &["XGROUP", "CREATE", "s", "g", "0"]).await;
    call(&mut producer, &["XADD", "s", "1-0", "f", "new-group"]).await;
    call(&mut producer, &["EXEC"]).await;
    assert!(
        matches!(next(&mut consumer).await, Value::Error(error) if error.starts_with("NOGROUP"))
    );
    waiters(&server, 0).await;
    assert!(snapshot(&server, 0, "s").await.groups[0].pending.is_empty());
    assert_eq!(
        call(
            &mut consumer,
            &["XREADGROUP", "GROUP", "g", "new", "STREAMS", "s", ">"]
        )
        .await
        .encode(),
        stream_reply(&[("s", &[("1-0", Some("new-group"))])], 2)
    );
    close(consumer, ct).await;
    close(producer, pt).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_new_request_cannot_overtake_an_existing_consumer_turn() {
    use crate::protocol::stream::xreadgroup::XReadGroupParams;
    use crate::raft::types::entry::request::{Operation, RedisOperation};
    let (_dir, node, server) = cluster().await;
    let (mut setup, st) = connect(&server, 100).await;
    create_group(&mut setup, "s", "g").await;
    call(&mut setup, &["XADD", "s", "1-0", "f", "oldest"]).await;
    let Value::Array(Some(items)) = command(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "old",
        "COUNT",
        "1",
        "STREAMS",
        "s",
        ">",
    ]) else {
        unreachable!()
    };
    let params = XReadGroupParams::parse(&items).unwrap();
    let cache = &server.app.state_machine.data.kvs;
    // Pause an older consumer after it owns the signal but before it submits
    // its Raft mutation. All data changes still pass through real Raft apply.
    let mut older = {
        let mut write = cache.write_lock.lock().await;
        let key = (0, Bytes::from_static(b"s"));
        let registration = write
            .register_stream_group(vec![key.clone()], None, params.clone())
            .unwrap();
        let clock = cache.get_and_update_read_clock();
        assert_eq!(
            write
                .group_blocking_keys
                .signal_ready_many(vec![key], |key, params| params.ready(cache, key.0, clock)),
            1
        );
        registration
    };
    older.wait().await.unwrap();
    let before = applied_index(&node).await;
    let (mut newcomer, nt) = connect(&server, 1).await;
    newcomer
        .send(command(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "new",
            "COUNT",
            "1",
            "STREAMS",
            "s",
            ">",
        ]))
        .await
        .unwrap();
    waiters(&server, 2).await;
    assert!(
        timeout(Duration::from_millis(25), newcomer.next())
            .await
            .is_err()
    );
    assert_eq!(applied_index(&node).await, before);
    let response = node
        .app
        .write(
            Operation::Redis(RedisOperation::RedisXReadGroup(params.request)),
            0,
        )
        .await
        .unwrap();
    assert_eq!(
        response.encode(),
        stream_reply(&[("s", &[("1-0", Some("oldest"))])], 2)
    );
    older.finish().await;
    assert_eq!(next(&mut newcomer).await.encode(), b"*-1\r\n");
    waiters(&server, 0).await;
    close(newcomer, nt).await;
    close(setup, st).await;
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_blocked_read_without_count_has_no_implicit_thousand_entry_limit() {
    let (_dir, node, server) = cluster().await;
    let (mut producer, pt) = connect(&server, 100).await;
    let (mut consumer, ct) = connect(&server, 1).await;
    create_group(&mut producer, "s", "g").await;
    consumer
        .send(command(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ]))
        .await
        .unwrap();
    waiters(&server, 1).await;
    let script = "for i=1,1001 do redis.call('XADD',KEYS[1],tostring(i)..'-0','f',tostring(i)) end return 1001";
    assert_eq!(
        call(&mut producer, &["EVAL", script, "1", "s"])
            .await
            .encode(),
        b":1001\r\n"
    );
    let Value::Array(Some(streams)) = next(&mut consumer).await else {
        panic!("expected stream pairs")
    };
    assert_eq!(streams.len(), 1);
    let Value::Array(Some(pair)) = &streams[0] else {
        panic!("expected stream pair")
    };
    let Value::Array(Some(entries)) = &pair[1] else {
        panic!("expected entries")
    };
    assert_eq!(entries.len(), 1001);
    assert_eq!(
        snapshot(&server, 0, "s").await.groups[0].pending.len(),
        1001
    );
    waiters(&server, 0).await;
    close(consumer, ct).await;
    close(producer, pt).await;
    node.app.cluster.shutdown().await.unwrap();
}
