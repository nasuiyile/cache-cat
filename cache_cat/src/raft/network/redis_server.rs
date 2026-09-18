use crate::error::CacheCatError;
use crate::node::parsed_config::ParsedConfig;
use crate::protocol::command::{Client, CommandFactory};
use crate::protocol::resp::Parser;
use crate::raft::application::pub_sub::PubSub;
use crate::raft::network::connection::Connection;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::raft_types::CacheCatApp;
use bytes::BytesMut;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{error, info};

#[derive(Clone)]
pub struct RedisServer {
    pub(crate) app: Arc<CacheCatApp>,
    pub redis_addr: String,
    pub tls_addr: Option<String>,
    pub cmd_factory: Arc<CommandFactory>,
    pub broadcast: Arc<PubSub>,
}

pub(super) struct RedisListeners {
    tcp: TcpListener,
    tls: Option<(TcpListener, TlsAcceptor)>,
}

pub struct RespCodec {
    proto_version: u8,
}

impl RespCodec {
    pub const fn new() -> Self {
        Self { proto_version: 2 }
    }

    pub const fn switch_resp2(&mut self) {
        self.proto_version = 2;
    }

    pub const fn switch_resp3(&mut self) {
        self.proto_version = 3;
    }

    /// The RESP protocol version currently negotiated on this connection.
    pub const fn proto_version(&self) -> u8 {
        self.proto_version
    }
}

impl Default for RespCodec {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for RespCodec {
    type Item = Value;
    type Error = std::io::Error;

    #[inline]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Ok(Parser::take_from_bytes_stream(src))
    }
}

impl Encoder<Value> for RespCodec {
    type Error = std::io::Error;

    #[inline]
    fn encode(&mut self, item: Value, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode_to(self.proto_version, dst);
        Ok(())
    }
}

impl RedisServer {
    pub fn new(
        app: Arc<CacheCatApp>,
        redis_addr: String,
        config: &ParsedConfig,
    ) -> Result<Self, CacheCatError> {
        let cmd_factory = Arc::new(CommandFactory::init());
        let broadcast = app.pubsub.clone();

        let tls_addr = config
            .tls_port
            .map(|port| format!("{}:{}", config.raft_endpoint.addr(), port));
        Ok(Self {
            app,
            redis_addr,
            tls_addr,
            cmd_factory,
            broadcast,
        })
    }

    async fn handle_connection_pipeline<T>(
        self: Arc<Self>,
        connection: T,
        peer_addr: SocketAddr,
        client_id: u64,
    ) -> Result<(), CacheCatError>
    where
        T: Into<Connection>,
    {
        // let framed = Framed::new(stream, RespCodec::new());
        let auth = self.app.config.password.is_none();
        let client = Client::new(client_id, connection, auth);
        let result = self.cmd_factory.process_connection(&self, client).await;
        self.app.pubsub.remove_client(client_id).await;
        info!("Connection handler ended for {}", peer_addr);
        result
    }

    pub async fn start_redis_server(self: Arc<Self>) -> std::io::Result<()> {
        let listeners = self.bind_listeners().await?;
        self.serve(listeners).await
    }

    // Bind every configured listener before reporting a successful startup.
    pub(super) async fn bind_listeners(&self) -> std::io::Result<RedisListeners> {
        let listener = TcpListener::bind(&self.redis_addr).await?;
        info!("Redis server listening on {}", self.redis_addr);
        let tls_acceptor = self.app.tls_context.acceptor_for_client();
        let tls_listener =
            if let (Some(tls_addr), Some(tls_acceptor)) = (&self.tls_addr, tls_acceptor) {
                let listener = TcpListener::bind(tls_addr).await?;
                info!("Redis TLS server listening on {}", tls_addr);
                Some((listener, tls_acceptor.clone()))
            } else {
                None
            };

        Ok(RedisListeners {
            tcp: listener,
            tls: tls_listener,
        })
    }

    pub(super) async fn serve(self: Arc<Self>, listeners: RedisListeners) -> std::io::Result<()> {
        let RedisListeners {
            tcp: listener,
            tls: tls_listener,
        } = listeners;
        let mut client_id: u64 = 0;

        loop {
            // 关键改动：将 TLS accept 封装为一个 async 块，
            // 当没有 TLS 监听器时永远 pending，避免饥饿
            tokio::select! {
                // 非 TLS 连接分支
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            info!("New connection accepted from {}", peer_addr);
                            let server = Arc::clone(&self);
                            client_id += 1;
                            let id = client_id;

                            if let Err(e) = stream.set_nodelay(true) {
                                error!("Failed to set nodelay for {}: {}", peer_addr, e);
                            }

                            tokio::spawn(async move {
                                if let Err(e) = server
                                    .handle_connection_pipeline(stream, peer_addr, id)
                                    .await
                                {
                                    error!("Error handling connection from {}: {}", peer_addr, e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }

                // TLS 连接分支
                result = async {
                    if let Some((listener, acceptor)) = &tls_listener {
                        // 如果有 TLS 监听器，等待 accept，并将 acceptor 一起返回
                        let accept_result = listener.accept().await;
                        Some((accept_result, acceptor.clone()))
                    } else {
                        // 没有 TLS 监听器，永远 pending，不会影响另一个分支
                        std::future::pending::<Option<_>>().await
                    }
                } => {
                    // 只有当 tls_listener 存在时，这里才会被执行
                    if let Some((accept_result, acceptor)) = result {
                        match accept_result {
                            Ok((stream, peer_addr)) => {
                                info!("New TLS connection accepted from {}", peer_addr);
                                let server = Arc::clone(&self);
                                client_id += 1;
                                let id = client_id;

                                tokio::spawn(async move {
                                    // 执行 TLS 握手
                                    match acceptor.accept(stream).await {
                                        Ok(tls_stream) => {
                                            // 可选：设置 nodelay（需要获取底层 socket，此处略）
                                            if let Err(e) = server
                                                .handle_connection_pipeline(tls_stream, peer_addr, id)
                                                .await
                                            {
                                                error!(
                                                    "Error handling TLS connection from {}: {}",
                                                    peer_addr, e
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            error!("TLS handshake failed from {}: {}", peer_addr, e);
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                error!("Failed to accept TLS connection: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::config::Config;
    use crate::node::raft_node::RaftNode;
    use crate::raft::network::rpc::Server;
    use bytes::Bytes;
    use futures::{SinkExt, StreamExt};
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::sync::{broadcast, oneshot};
    use tokio::time::timeout;
    use tokio_util::codec::Framed;

    async fn test_node() -> (tempfile::TempDir, RaftNode, broadcast::Sender<()>) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.raft.log_path = dir.path().to_str().unwrap().to_owned();
        config.raft.address = "127.0.0.1:0".into();
        config.redis.databases = 1;
        let config = ParsedConfig::from(&config).unwrap();
        let (shutdown_tx, _) = broadcast::channel(1);
        let node = RaftNode::create(config, shutdown_tx.clone()).await.unwrap();
        (dir, node, shutdown_tx)
    }

    fn command(parts: &[&'static [u8]]) -> Value {
        Value::Array(Some(
            parts
                .iter()
                .map(|part| Value::BulkString(Some(Bytes::from_static(part))))
                .collect(),
        ))
    }

    #[tokio::test]
    async fn disconnect_cleans_up_subscriptions_even_on_read_error() {
        let (_dir, node, _) = test_node().await;
        let server = Arc::new(
            RedisServer::new(node.app.clone(), "127.0.0.1:0".into(), &node.app.config).unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (connection, addr) = listener.accept().await.unwrap();
        let handler = tokio::spawn(
            server
                .clone()
                .handle_connection_pipeline(connection, addr, 1),
        );
        let mut client = Framed::new(socket, RespCodec::new());
        for parts in [
            [b"SUBSCRIBE".as_slice(), b"channel".as_slice()],
            [b"PSUBSCRIBE".as_slice(), b"pattern*".as_slice()],
        ] {
            client.send(command(&parts)).await.unwrap();
            timeout(Duration::from_secs(2), client.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        assert_eq!(server.broadcast.client_subscription_count(1).await, 1);
        assert_eq!(server.broadcast.client_pattern_count(1).await, 1);

        // EOF in an incomplete frame makes Framed report a read error.
        client
            .get_mut()
            .write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$")
            .await
            .unwrap();
        client.get_mut().shutdown().await.unwrap();
        let result = timeout(Duration::from_secs(2), handler)
            .await
            .unwrap()
            .unwrap();
        node.app.cluster.shutdown().await.unwrap();
        assert!(result.is_err());
        assert_eq!(server.broadcast.client_subscription_count(1).await, 0);
        assert_eq!(server.broadcast.client_pattern_count(1).await, 0);
        assert!(
            matches!(server.broadcast.pubsub_channels(None).await, Value::Array(Some(channels)) if channels.is_empty())
        );
        assert!(matches!(
            server.broadcast.pubsub_numpat().await,
            Value::Integer(0)
        ));
    }

    #[tokio::test]
    async fn redis_bind_failure_is_reported_as_startup_failure() {
        let (_dir, node, shutdown_tx) = test_node().await;
        let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (startup_tx, startup_rx) = oneshot::channel();
        let server = Server::new(
            node.app.clone(),
            "127.0.0.1:0".into(),
            startup_tx,
            occupied.local_addr().unwrap().to_string(),
            &node.app.config,
        )
        .unwrap();
        let handle = tokio::spawn(server.start_server(shutdown_tx.subscribe()));
        let startup = timeout(Duration::from_secs(2), startup_rx)
            .await
            .unwrap()
            .unwrap();
        let _ = shutdown_tx.send(());
        let result = timeout(Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap();
        node.app.cluster.shutdown().await.unwrap();
        assert!(
            startup.is_err(),
            "Redis bind failure was reported as successful startup"
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn server_shutdown_releases_redis_listener() {
        let (_dir, node, shutdown_tx) = test_node().await;
        let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redis_addr = reservation.local_addr().unwrap();
        drop(reservation);
        let (startup_tx, startup_rx) = oneshot::channel();
        let server = Server::new(
            node.app.clone(),
            "127.0.0.1:0".into(),
            startup_tx,
            redis_addr.to_string(),
            &node.app.config,
        )
        .unwrap();
        let handle = tokio::spawn(server.start_server(shutdown_tx.subscribe()));
        timeout(Duration::from_secs(2), startup_rx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let socket = TcpStream::connect(redis_addr).await.unwrap();
        let mut client = Framed::new(socket, RespCodec::new());
        client.send(command(&[b"PING"])).await.unwrap();
        assert!(
            matches!(timeout(Duration::from_secs(2), client.next()).await.unwrap().unwrap().unwrap(), Value::SimpleString(s) if s == "PONG")
        );
        client.send(command(&[b"QUIT"])).await.unwrap();
        assert!(
            matches!(timeout(Duration::from_secs(2), client.next()).await.unwrap().unwrap().unwrap(), Value::SimpleString(s) if s == "OK")
        );
        assert!(
            timeout(Duration::from_secs(2), client.next())
                .await
                .unwrap()
                .is_none()
        );
        drop(client);
        shutdown_tx.send(()).unwrap();
        timeout(Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        node.app.cluster.shutdown().await.unwrap();
        let _rebound = TcpListener::bind(redis_addr)
            .await
            .expect("Redis listener survived service shutdown");
    }
}
