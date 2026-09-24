//! Regression tests for Redis transaction queueing and execution semantics.
//!
//! This module is included from `protocol::command` so it can drive the same
//! command dispatch path as a client connection.  The state-machine test uses
//! `do_request` directly to avoid requiring a running Raft transport.

use super::*;
use crate::cfg::config::Config;
use crate::node::parsed_config::ParsedConfig;
use crate::node::raft_node::RaftNode;
use crate::protocol::hash::hset::HSetReq;
use crate::protocol::string::set::SetParams;
use crate::raft::network::redis_server::{RedisServer, RespCodec};
use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::do_request;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use bytes::Bytes;
use futures::StreamExt;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::timeout;
use tokio_util::codec::Framed;

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
    config.redis.databases = 1;
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
    let operations = client.transaction_queue.take().unwrap();
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
    ];
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
