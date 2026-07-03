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
        self.cmd_factory.process_connection(&self, client).await?;
        self.app.pubsub.remove_client(client_id).await;
        info!("Connection handler ended for {}", peer_addr);
        Ok(())
    }

    pub async fn start_redis_server(self: Arc<Self>) -> std::io::Result<()> {
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
