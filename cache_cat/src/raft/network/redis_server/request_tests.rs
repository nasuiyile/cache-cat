use super::tests::test_node;
use super::*;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::io::ErrorKind;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::Framed;

const NESTED_ERROR: &[u8] = b"-ERR Protocol error: expected '$', got '*'\r\n";
const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const WAIT: Duration = Duration::from_secs(2);

async fn socket_pair() -> (TcpStream, TcpStream, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socket = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (connection, addr) = listener.accept().await.unwrap();
    (socket, connection, addr)
}

async fn connect(
    server: Arc<RedisServer>,
    client_id: u64,
) -> (TcpStream, JoinHandle<Result<(), CacheCatError>>) {
    let (socket, connection, addr) = socket_pair().await;
    let handler = tokio::spawn(server.handle_connection_pipeline(connection, addr, client_id));
    (socket, handler)
}

async fn expect_bytes(socket: &mut TcpStream, expected: &[u8]) {
    let mut actual = vec![0; expected.len()];
    timeout(WAIT, socket.read_exact(&mut actual))
        .await
        .expect("server waited for more input instead of sending its reply")
        .unwrap();
    assert_eq!(actual, expected);
}

async fn expect_closed(socket: &mut TcpStream) {
    let mut extra = [0; 1];
    match timeout(WAIT, socket.read(&mut extra))
        .await
        .expect("server kept the connection open after a protocol error")
    {
        Ok(0) => {}
        // A close with unread pipelined input can reset the socket on some OSes.
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted
            ) => {}
        other => panic!("unexpected bytes or I/O result after the final reply: {other:?}"),
    }
}

fn command(parts: &[&'static [u8]]) -> Value {
    Value::Array(Some(
        parts
            .iter()
            .map(|part| Value::BulkString(Some(Bytes::from_static(part))))
            .collect(),
    ))
}

async fn next_reply(client: &mut Framed<TcpStream, RespCodec>) -> Value {
    timeout(WAIT, client.next())
        .await
        .expect("timed out waiting for the server reply")
        .expect("connection closed before its reply")
        .unwrap()
}

#[tokio::test]
async fn unauthenticated_nested_request_is_rejected_before_its_header_is_complete() {
    let (_dir, node, _) = test_node().await;
    let server = Arc::new(
        RedisServer::new(node.app.clone(), "127.0.0.1:0".into(), &node.app.config).unwrap(),
    );
    let (mut socket, connection, _) = socket_pair().await;
    let handler = tokio::spawn(async move {
        server
            .cmd_factory
            .process_connection(&server, Client::new(1, connection, false))
            .await
    });

    // Keep the write side open: the decoder must reject the nested marker now,
    // without waiting for its count, CRLF, contents, authentication, or EOF.
    socket.write_all(b"*1\r\n*").await.unwrap();
    expect_bytes(&mut socket, NESTED_ERROR).await;
    expect_closed(&mut socket).await;
    assert!(timeout(WAIT, handler).await.unwrap().unwrap().is_err());
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn nested_pipeline_stops_at_protocol_error_and_other_connections_still_work() {
    let (_dir, node, _) = test_node().await;
    let server = Arc::new(
        RedisServer::new(node.app.clone(), "127.0.0.1:0".into(), &node.app.config).unwrap(),
    );
    let (mut socket, handler) = connect(server.clone(), 1).await;
    let mut pipeline = Vec::from(PING);
    pipeline.extend_from_slice(b"*1\r\n");
    pipeline.extend_from_slice(PING);
    pipeline.extend_from_slice(PING);
    socket.write_all(&pipeline).await.unwrap();
    expect_bytes(&mut socket, b"+PONG\r\n").await;
    expect_bytes(&mut socket, NESTED_ERROR).await;
    expect_closed(&mut socket).await;
    assert!(timeout(WAIT, handler).await.unwrap().unwrap().is_err());

    let (mut healthy, handler) = connect(server, 2).await;
    healthy.write_all(PING).await.unwrap();
    expect_bytes(&mut healthy, b"+PONG\r\n").await;
    healthy.write_all(b"*1\r\n$4\r\nQUIT\r\n").await.unwrap();
    expect_bytes(&mut healthy, b"+OK\r\n").await;
    expect_closed(&mut healthy).await;
    timeout(WAIT, handler).await.unwrap().unwrap().unwrap();
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn nested_request_in_resp2_and_resp3_pubsub_closes_and_cleans_subscriptions() {
    let (_dir, node, _) = test_node().await;
    let server = Arc::new(
        RedisServer::new(node.app.clone(), "127.0.0.1:0".into(), &node.app.config).unwrap(),
    );
    for proto in [2, 3] {
        let client_id = u64::from(proto);
        let (socket, handler) = connect(server.clone(), client_id).await;
        let mut client = Framed::new(socket, RespCodec::new());
        if proto == 3 {
            client.send(command(&[b"HELLO", b"3"])).await.unwrap();
            assert!(matches!(next_reply(&mut client).await, Value::Map(_)));
            client.codec_mut().switch_resp3();
        }
        for (index, (name, confirmation, target)) in [
            (
                b"SUBSCRIBE".as_slice(),
                b"subscribe".as_slice(),
                b"channel".as_slice(),
            ),
            (
                b"PSUBSCRIBE".as_slice(),
                b"psubscribe".as_slice(),
                b"pattern*".as_slice(),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            client.send(command(&[name, target])).await.unwrap();
            let reply = next_reply(&mut client).await;
            assert_eq!(matches!(&reply, Value::Push(_)), proto == 3);
            let expected = Value::Push(vec![
                Value::BulkString(Some(Bytes::from_static(confirmation))),
                Value::BulkString(Some(Bytes::from_static(target))),
                Value::Integer(index as i64 + 1),
            ]);
            assert_eq!(reply.encode_proto(proto), expected.encode_proto(proto));
        }
        assert_eq!(
            server.broadcast.client_subscription_count(client_id).await,
            1
        );
        assert_eq!(server.broadcast.client_pattern_count(client_id).await, 1);

        client.get_mut().write_all(b"*1\r\n*").await.unwrap();
        assert_eq!(
            next_reply(&mut client).await.encode_proto(proto),
            NESTED_ERROR
        );
        match timeout(WAIT, client.next())
            .await
            .expect("subscribed connection was not closed after its protocol error")
        {
            None => {}
            Some(Err(error))
                if matches!(
                    error.kind(),
                    ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted
                ) => {}
            other => panic!("unexpected response after the protocol error: {other:?}"),
        }
        assert!(timeout(WAIT, handler).await.unwrap().unwrap().is_err());
        assert_eq!(
            server.broadcast.client_subscription_count(client_id).await,
            0
        );
        assert_eq!(server.broadcast.client_pattern_count(client_id).await, 0);
        assert!(matches!(
            server.broadcast.pubsub_channels(None).await,
            Value::Array(Some(channels)) if channels.is_empty()
        ));
        assert!(matches!(
            server.broadcast.pubsub_numpat().await,
            Value::Integer(0)
        ));
    }
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn dynamic_error_keeps_resp2_and_resp3_pipeline_replies_aligned() {
    let (_dir, node, _) = test_node().await;
    let server = Arc::new(
        RedisServer::new(node.app.clone(), "127.0.0.1:0".into(), &node.app.config).unwrap(),
    );
    for proto in [2_u8, 3] {
        let (socket, handler) = connect(server.clone(), u64::from(proto)).await;
        let mut client = Framed::new(socket, RespCodec::new());
        if proto == 3 {
            client.send(command(&[b"HELLO", b"3"])).await.unwrap();
            assert!(matches!(next_reply(&mut client).await, Value::Map(_)));
        }
        let mut socket = client.into_inner();

        // The command name is a valid bulk string containing a forged reply.
        // Its error must stay on one line, including isolated CR and LF bytes.
        let mut pipeline = command(&[b"MISSING\r\n:123\rBROKEN\nTAIL"]).encode_proto(2);
        pipeline.extend_from_slice(PING);
        socket.write_all(&pipeline).await.unwrap();
        expect_bytes(
            &mut socket,
            b"-ERR unknown command 'MISSING  :123 BROKEN TAIL'\r\n+PONG\r\n",
        )
        .await;

        // A subsequent round trip and clean EOF rule out extra injected frames.
        socket.write_all(PING).await.unwrap();
        expect_bytes(&mut socket, b"+PONG\r\n").await;
        socket.write_all(b"*1\r\n$4\r\nQUIT\r\n").await.unwrap();
        expect_bytes(&mut socket, b"+OK\r\n").await;
        expect_closed(&mut socket).await;
        timeout(WAIT, handler).await.unwrap().unwrap().unwrap();
    }
    node.app.cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn dynamic_error_keeps_resp2_subscribed_pipeline_replies_aligned() {
    let (_dir, node, _) = test_node().await;
    let server = Arc::new(
        RedisServer::new(node.app.clone(), "127.0.0.1:0".into(), &node.app.config).unwrap(),
    );
    let (mut socket, handler) = connect(server.clone(), 1).await;
    socket
        .write_all(&command(&[b"SUBSCRIBE", b"channel"]).encode_proto(2))
        .await
        .unwrap();
    expect_bytes(
        &mut socket,
        b"*3\r\n$9\r\nsubscribe\r\n$7\r\nchannel\r\n:1\r\n",
    )
    .await;

    let mut pipeline = command(&[b"MISSING\r\n+INJECTED"]).encode_proto(2);
    pipeline.extend_from_slice(PING);
    socket.write_all(&pipeline).await.unwrap();
    expect_bytes(
        &mut socket,
        concat!(
            "-ERR Can't execute 'missing  +injected': only (P|S)SUBSCRIBE / ",
            "(P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n",
            "*2\r\n$4\r\npong\r\n$0\r\n\r\n",
        )
        .as_bytes(),
    )
    .await;

    socket.write_all(b"*1\r\n$4\r\nQUIT\r\n").await.unwrap();
    expect_bytes(&mut socket, b"+OK\r\n").await;
    expect_closed(&mut socket).await;
    timeout(WAIT, handler).await.unwrap().unwrap().unwrap();
    assert_eq!(server.broadcast.client_subscription_count(1).await, 0);
    node.app.cluster.shutdown().await.unwrap();
}
