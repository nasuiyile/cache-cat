//! Regression tests for Redis transaction queueing and execution semantics.
//!
//! This module is included from `protocol::command` so it can drive the same
//! command dispatch path as a client connection. Successful EXEC tests use a
//! single-node Raft cluster without starting an external transport.

use super::*;
use crate::cfg::config::Config;
use crate::node::parsed_config::ParsedConfig;
use crate::node::raft_node::RaftNode;
use crate::protocol::hash::hset::HSetReq;
use crate::protocol::string::set::SetParams;
use crate::protocol::transaction::QueuedOperation;
use crate::raft::network::redis_server::{RedisServer, RespCodec};
use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{base_request, do_request};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use crate::raft::types::raft_types::Node;
use bytes::Bytes;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::timeout;
use tokio_util::codec::Framed;
use crate::protocol::command::Client;

fn command(parts: &[&str]) -> Value {
    Value::Array(Some(
        parts
            .iter()
            .map(|part| Value::BulkString(Some(Bytes::copy_from_slice(part.as_bytes()))))
            .collect(),
    ))
}

/// Construct a command factory and a client connection without starting a
/// Redis listener.  `execute_command` only needs the application handle for
/// commands that actually reach Raft; the abort tests stop before that point.
async fn test_connection() -> (
    TempDir,
    RaftNode,
    RedisServer,
    Client,
    Framed<TcpStream, RespCodec>,
) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.raft.log_path = dir.path().to_str().unwrap().to_owned();
    config.raft.address = "127.0.0.1:0".into();
    config.redis.databases = 2;
    let config = ParsedConfig::from(&config).unwrap();
    let (shutdown_tx, _) = broadcast::channel(1);
    let node = RaftNode::create(config.clone(), shutdown_tx).await.unwrap();
    let server = RedisServer::new(node.app.clone(), "127.0.0.1:0".into(), &config).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socket = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (connection, _) = listener.accept().await.unwrap();
    let client = Client::new(1, connection, true);
    let replies = Framed::new(socket, RespCodec::new());
    (dir, node, server, client, replies)
}

async fn round_trip(
    server: &RedisServer,
    client: &mut Client,
    replies: &mut Framed<TcpStream, RespCodec>,
    parts: &[&str],
) -> Value {
    server
        .cmd_factory
        .execute_command(client, server, command(parts))
        .await
        .unwrap();
    timeout(Duration::from_secs(2), replies.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn initialize_single_node(node: &RaftNode) {
    let node_id = node.app.cluster.node_id();
    node.app
        .cluster
        .initialize(BTreeMap::from([(
            node_id,
            Node {
                node_id,
                endpoint: node.app.config.raft_advertise_endpoint.clone(),
            },
        )]))
        .await
        .unwrap();
    timeout(Duration::from_secs(5), async {
        while !node.app.cluster.is_leader() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("single-node test cluster must elect itself");
}

async fn queue_transaction(
    server: &RedisServer,
    client: &mut Client,
    replies: &mut Framed<TcpStream, RespCodec>,
    commands: &[&[&str]],
) {
    let initial_db = client.db_number;
    assert_eq!(
        round_trip(server, client, replies, &["MULTI"])
            .await
            .encode(),
        b"+OK\r\n"
    );
    for parts in commands {
        assert_eq!(
            round_trip(server, client, replies, parts).await.encode(),
            b"+QUEUED\r\n",
            "{parts:?}"
        );
        assert_eq!(client.db_number, initial_db, "{parts:?}");
    }
}

#[tokio::test]
async fn select_in_exec_preserves_command_order_and_final_client_database() {
    let (_dir, node, server, mut client, mut replies) = test_connection().await;
    initialize_single_node(&node).await;
    let Value::BulkString(Some(sha)) = round_trip(
        &server,
        &mut client,
        &mut replies,
        &["SCRIPT", "LOAD", "return redis.call('GET', KEYS[1])"],
    )
    .await
    else {
        panic!("SCRIPT LOAD must return the script SHA");
    };
    let sha = std::str::from_utf8(&sha).unwrap();
    queue_transaction(
        &server,
        &mut client,
        &mut replies,
        &[
            &["SET", "k", "zero"],
            &["EVALSHA", sha, "1", "k"],
            &["SELECT", "1"],
            &["SET", "k", "one"],
            &["EVALSHA", sha, "1", "k"],
            &["SELECT", "0"],
            &["GET", "k"],
            &["SELECT", "1"],
            &["GET", "k"],
        ],
    )
    .await;
    assert_eq!(
        client
            .transaction_queue
            .as_ref()
            .unwrap()
            .operations
            .iter()
            .map(|item| item.db_number)
            .collect::<Vec<_>>(),
        vec![0, 0, 1, 1, 1, 0, 0, 1, 1]
    );
    assert_eq!(
        timeout(
            Duration::from_secs(5),
            round_trip(&server, &mut client, &mut replies, &["EXEC"]),
        )
        .await
        .unwrap()
        .encode(),
        b"*9\r\n+OK\r\n$4\r\nzero\r\n+OK\r\n+OK\r\n$3\r\none\r\n+OK\r\n$4\r\nzero\r\n+OK\r\n$3\r\none\r\n"
    );
    assert_eq!(client.db_number, 1);
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["GET", "k"])
            .await
            .encode(),
        b"$3\r\none\r\n"
    );
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn final_select_in_exec_commits_database_from_nonzero_start() {
    let (_dir, node, server, mut client, mut replies) = test_connection().await;
    initialize_single_node(&node).await;
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["SELECT", "1"])
            .await
            .encode(),
        b"+OK\r\n"
    );
    queue_transaction(
        &server,
        &mut client,
        &mut replies,
        &[&["SET", "k", "one"], &["SELECT", "0"]],
    )
    .await;
    let queued = client.transaction_queue.as_ref().unwrap();
    assert_eq!(queued.db_number, 0);
    assert_eq!(queued.operations[0].db_number, 1);
    assert_eq!(queued.operations[1].db_number, 0);
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["EXEC"])
            .await
            .encode(),
        b"*2\r\n+OK\r\n+OK\r\n"
    );
    assert_eq!(client.db_number, 0);
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["GET", "k"])
            .await
            .encode(),
        b"$-1\r\n"
    );
    assert!(
        node.app.state_machine.data.kvs.databases[1]
            .mocha
            .get_entry(&b"k"[..])
            .is_some()
    );
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn discard_keeps_database_selected_before_multi_and_discards_writes() {
    let (_dir, node, server, mut client, mut replies) = test_connection().await;
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["SELECT", "1"])
            .await
            .encode(),
        b"+OK\r\n"
    );
    queue_transaction(
        &server,
        &mut client,
        &mut replies,
        &[
            &["SET", "k", "one"],
            &["SELECT", "0"],
            &["SET", "k", "zero"],
        ],
    )
    .await;
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["DISCARD"])
            .await
            .encode(),
        b"+OK\r\n"
    );
    assert_eq!(client.db_number, 1);
    for db in &node.app.state_machine.data.kvs.databases {
        assert!(db.mocha.get_entry(&b"k"[..]).is_none());
    }
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_select_returns_exec_element_errors_without_switching_database() {
    let (_dir, node, server, mut client, mut replies) = test_connection().await;
    initialize_single_node(&node).await;
    queue_transaction(
        &server,
        &mut client,
        &mut replies,
        &[
            &["SELECT", "1"],
            &["SELECT", "2"],
            &["SELECT", "invalid"],
            &["SET", "k", "one"],
            &["GET", "k"],
        ],
    )
    .await;
    assert_eq!(
        timeout(
            Duration::from_secs(5),
            round_trip(&server, &mut client, &mut replies, &["EXEC"]),
        )
        .await
        .unwrap()
        .encode(),
        b"*5\r\n+OK\r\n-ERR DB index is out of range\r\n-ERR invalid DB index\r\n+OK\r\n$3\r\none\r\n"
    );
    assert_eq!(client.db_number, 1);
    assert!(
        node.app.state_machine.data.kvs.databases[0]
            .mocha
            .get_entry(&b"k"[..])
            .is_none()
    );
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn select_wrong_arity_aborts_exec_without_switching_database() {
    let (_dir, node, server, mut client, mut replies) = test_connection().await;
    queue_transaction(
        &server,
        &mut client,
        &mut replies,
        &[
            &["SET", "k", "zero"],
            &["SELECT", "1"],
            &["SET", "k", "one"],
        ],
    )
    .await;
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["SELECT"])
            .await
            .encode(),
        b"-ERR wrong number of arguments for 'select' command\r\n"
    );
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["EXEC"])
            .await
            .encode(),
        b"-EXECABORT Transaction discarded because of previous errors.\r\n"
    );
    assert_eq!(client.db_number, 0);
    for db in &node.app.state_machine.data.kvs.databases {
        assert!(db.mocha.get_entry(&b"k"[..]).is_none());
    }
    node.app.cluster.shutdown().await.unwrap();
}

#[test]
fn select_in_exec_records_each_write_database_for_snapshot_replay() {
    let cache = MyCache::new(2).unwrap();
    let mut queue = Vec::new();
    let mut update_type = UpdateType::Snapshot(&mut queue);
    let mut update = Update {
        db_number: 0,
        write_clock: 1,
        update_type: &mut update_type,
    };
    let operations = vec![
        QueuedOperation::new(
            0,
            Operation::Redis(RedisOperation::RedisSet(SetParams::new("k", "old"))),
        ),
        QueuedOperation::new(1, Operation::Redis(RedisOperation::RedisReply(Value::ok()))),
        QueuedOperation::new(
            1,
            Operation::Redis(RedisOperation::RedisSet(SetParams::new("k", "one"))),
        ),
        QueuedOperation::new(0, Operation::Redis(RedisOperation::RedisReply(Value::ok()))),
        QueuedOperation::new(
            0,
            Operation::Redis(RedisOperation::RedisSet(SetParams::new("k", "zero"))),
        ),
    ];
    let response = do_request(
        &cache,
        Operation::Redis(RedisOperation::RedisExec(
            crate::protocol::transaction::exec::ExecParams { operations },
        )),
        &mut update,
        true,
    );
    assert_eq!(
        response.encode(),
        b"*5\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n"
    );
    assert_eq!(
        queue.iter().map(|item| item.db_number).collect::<Vec<_>>(),
        vec![0, 1, 0]
    );

    let restored = MyCache::new(2).unwrap();
    let bytes = bincode2::serialize(&queue).unwrap();
    let queue: Vec<crate::raft::types::entry::request::AtomicRequest> =
        bincode2::deserialize(&bytes).unwrap();
    for atomic in queue {
        let mut update_type = UpdateType::CAS(atomic.version);
        let mut update = Update {
            db_number: atomic.db_number,
            write_clock: restored.set_write_clock(atomic.write_clock),
            update_type: &mut update_type,
        };
        base_request(&restored, atomic.request, &mut update);
    }
    for (db_number, expected) in [(0, b"zero".as_slice()), (1, b"one".as_slice())] {
        let entry = restored.databases[db_number]
            .mocha
            .get_entry(&b"k"[..])
            .unwrap();
        let crate::raft::types::core::value_object::ValueObject::String(value) = entry.value.data
        else {
            panic!("SET must restore a string");
        };
        assert_eq!(value.as_ref(), expected);
    }
}

#[tokio::test]
async fn zcount_dispatches_in_transactions_and_lua() {
    let (_dir, node, server, mut client, mut replies) = test_connection().await;
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["MULTI"])
            .await
            .encode(),
        b"+OK\r\n"
    );
    for parts in [
        vec!["ZADD", "scores", "1", "a", "2", "b", "3", "c"],
        vec!["ZCOUNT", "scores", "(1", "+inf"],
    ] {
        assert_eq!(
            round_trip(&server, &mut client, &mut replies, &parts)
                .await
                .encode(),
            b"+QUEUED\r\n"
        );
    }

    let cache = &node.app.state_machine.data.kvs;
    let mut update_type = UpdateType::None;
    let mut update = Update {
        db_number: 0,
        write_clock: 1,
        update_type: &mut update_type,
    };
    let reply = do_request(
        cache,
        Operation::Redis(RedisOperation::RedisExec(
            crate::protocol::transaction::exec::ExecParams {
                operations: client.transaction_queue.take().unwrap().operations,
            },
        )),
        &mut update,
        true,
    );
    assert_eq!(reply.encode(), b"*2\r\n:3\r\n:2\r\n");

    let reply = do_request(
        cache,
        Operation::Redis(RedisOperation::RedisEval(
            crate::protocol::lua::eval::EvalParams::new(
                "return redis.call('ZCOUNT', KEYS[1], '-inf', '2')".into(),
                1,
                vec!["scores".into()],
                Vec::new(),
            ),
        )),
        &mut update,
        true,
    );
    assert_eq!(reply.encode(), b":2\r\n");
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_queued_command_aborts_exec_and_discards_writes() {
    let (_dir, node, server, mut client, mut replies) = test_connection().await;

    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["MULTI"])
            .await
            .encode(),
        b"+OK\r\n"
    );

    assert_eq!(
        round_trip(
            &server,
            &mut client,
            &mut replies,
            &["SET", "before-error", "value"],
        )
        .await
        .encode(),
        b"+QUEUED\r\n"
    );

    // SET's wrong arity is detected while queueing.  Redis marks the
    // transaction dirty but keeps it open so subsequent commands still reply
    // QUEUED.
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["SET", "only-key"])
            .await
            .encode(),
        b"-ERR wrong number of arguments for 'set' command\r\n"
    );
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["SET", "k", "v"])
            .await
            .encode(),
        b"+QUEUED\r\n"
    );
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["EXEC"])
            .await
            .encode(),
        b"-EXECABORT Transaction discarded because of previous errors.\r\n"
    );
    assert!(client.transaction_queue.is_none());
    assert!(!client.transaction_failed);
    assert!(!client.flag.multi);
    assert!(
        node.app.state_machine.data.kvs.databases[0]
            .mocha
            .get_entry(&b"k"[..])
            .is_none()
    );
    assert!(
        node.app.state_machine.data.kvs.databases[0]
            .mocha
            .get_entry(&b"before-error"[..])
            .is_none()
    );
}

#[tokio::test]
async fn unknown_queued_command_is_dirty_but_discard_resets_transaction() {
    let (_dir, _node, server, mut client, mut replies) = test_connection().await;

    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["MULTI"])
            .await
            .encode(),
        b"+OK\r\n"
    );
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["NO_SUCH_COMMAND"])
            .await
            .encode(),
        b"-ERR unknown command 'NO_SUCH_COMMAND'\r\n"
    );
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["DISCARD"])
            .await
            .encode(),
        b"+OK\r\n"
    );
    assert!(client.transaction_queue.is_none());
    assert!(!client.transaction_failed);
    assert!(!client.flag.multi);

    // A new transaction after DISCARD must be usable.  This also catches a
    // dirty flag that is accidentally kept after the queue is cleared.
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["MULTI"])
            .await
            .encode(),
        b"+OK\r\n"
    );
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["SET", "k", "v"])
            .await
            .encode(),
        b"+QUEUED\r\n"
    );
    assert!(
        client
            .transaction_queue
            .as_ref()
            .is_some_and(|q| !q.is_empty())
    );
}

#[tokio::test]
async fn ping_and_echo_are_replied_to_by_exec_in_queue_order() {
    let (_dir, _node, server, mut client, mut replies) = test_connection().await;

    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["MULTI"])
            .await
            .encode(),
        b"+OK\r\n"
    );
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["PING"])
            .await
            .encode(),
        b"+QUEUED\r\n"
    );
    assert_eq!(
        round_trip(&server, &mut client, &mut replies, &["ECHO", "hello"])
            .await
            .encode(),
        b"+QUEUED\r\n"
    );
    // The test node is intentionally not started as a cluster leader.  Apply
    // the captured queue directly to verify the same state-machine path used
    // by EXEC without depending on Raft forwarding.
    let operations = client.transaction_queue.take().unwrap().operations;
    let cache = MyCache::new(1).unwrap();
    let mut update_type = UpdateType::None;
    let mut update = Update {
        db_number: 0,
        write_clock: 1,
        update_type: &mut update_type,
    };
    let response = do_request(
        &cache,
        Operation::Redis(RedisOperation::RedisExec(
            crate::protocol::transaction::exec::ExecParams { operations },
        )),
        &mut update,
        true,
    );
    assert_eq!(response.encode(), b"*2\r\n+PONG\r\n$5\r\nhello\r\n");
}

#[test]
fn exec_returns_each_runtime_error_and_continues_with_later_commands() {
    let cache = MyCache::new(1).unwrap();
    let key: Bytes = "k".into();
    let later: Bytes = "later".into();
    let operations = vec![
        Operation::Redis(RedisOperation::RedisSet(SetParams::new(key.clone(), "v"))),
        Operation::Base(BaseOperation::HSet(HSetReq {
            key: key.clone(),
            elements: vec![("field".into(), "value".into())],
        })),
        Operation::Redis(RedisOperation::RedisSet(SetParams::new(
            later.clone(),
            "ok",
        ))),
    ]
    .into_iter()
    .map(|operation| QueuedOperation::new(0, operation))
    .collect();
    let mut update_type = UpdateType::None;
    let mut update = Update {
        db_number: 0,
        write_clock: 1,
        update_type: &mut update_type,
    };

    let response = do_request(
        &cache,
        Operation::Redis(RedisOperation::RedisExec(
            crate::protocol::transaction::exec::ExecParams { operations },
        )),
        &mut update,
        true,
    );
    let Value::Array(Some(values)) = response else {
        panic!("EXEC must return one response per queued command");
    };
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].encode(), b"+OK\r\n");
    assert_eq!(
        values[1].encode(),
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );
    assert_eq!(values[2].encode(), b"+OK\r\n");
    assert!(cache.databases[0].mocha.get_entry(&b"k"[..]).is_some());
    assert!(cache.databases[0].mocha.get_entry(&b"later"[..]).is_some());
}
