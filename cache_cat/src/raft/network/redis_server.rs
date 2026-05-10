use crate::protocol::command::CommandFactory;
use crate::protocol::resp::Parser;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::raft_types::CacheCatApp;
use bytes::{Buf, BytesMut};
use futures::{FutureExt, SinkExt, StreamExt, future::BoxFuture, stream::FuturesOrdered};
use std::io::Result as IoResult;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{debug, error, info, warn};

pub struct RedisServer {
    pub(crate) app: Arc<CacheCatApp>,
    pub redis_addr: String,
    pub cmd_factory: Arc<CommandFactory>,
}

pub struct RespCodec;

impl Decoder for RespCodec {
    type Item = Value;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match Parser::parse(src) {
            Some((value, consumed)) => {
                src.advance(consumed);
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<Value> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Value, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(&item.encode());
        Ok(())
    }
}

impl RedisServer {
    #[inline(always)]
    async fn process_command(&self, db_number: &mut u16, value: Value) -> Value {
        self.cmd_factory.execute(db_number, value, &self).await
    }

    async fn handle_connection_pipeline(
        self: Arc<Self>,
        stream: TcpStream,
        peer_addr: SocketAddr,
    ) -> IoResult<()> {
        let framed = Framed::new(stream, RespCodec);
        let (mut writer, mut reader) = framed.split();
        let mut db_number = 0;
        while let Some(frame_result) = reader.next().await {
            match frame_result {
                Ok(value) => {
                    debug!("Received command from {}: {:?}", peer_addr, value);
                    // 串行执行：等待完成
                    let resp = self.process_command(&mut db_number, value).await;
                    // 再写回
                    if let Err(e) = writer.send(resp).await {
                        warn!("Failed to send response to {}: {}", peer_addr, e);
                        break;
                    }
                }
                Err(e) => {
                    error!("Protocol error from {}: {}", peer_addr, e);
                    break;
                }
            }
        }

        info!("Connection handler ended for {}", peer_addr);
        Ok(())
    }

    async fn handle_connection(
        self: Arc<Self>,
        stream: TcpStream,
        peer_addr: SocketAddr,
    ) -> IoResult<()> {
        let framed = Framed::new(stream, RespCodec);
        let (mut writer, mut reader) = framed.split();

        // 保序执行：前面的命令没完成，后面的响应不会越过它
        let mut pending: FuturesOrdered<BoxFuture<'static, Value>> = FuturesOrdered::new();

        // 限制并发深度，避免客户端疯狂 pipeline 把内存打爆
        const MAX_INFLIGHT: usize = 1024;
        let mut inflight: usize = 0;
        let mut peer_closed = false;

        loop {
            if peer_closed && inflight == 0 {
                break;
            }
            let mut db_number = 0;
            tokio::select! {
                // 1) 继续读命令，只要队列没满
                frame_result = reader.next(), if !peer_closed && inflight < MAX_INFLIGHT => {
                    match frame_result {
                        Some(Ok(value)) => {
                            debug!("Received command from {}: {:?}", peer_addr, value);

                            let server = Arc::clone(&self);
                            let fut = async move {
                                server.process_command(&mut db_number,value).await
                            }.boxed();

                            pending.push_back(fut);
                            inflight += 1;
                        }
                        Some(Err(e)) => {
                            error!("Protocol error from {}: {}", peer_addr, e);
                            break;
                        }
                        None => {
                            peer_closed = true;
                        }
                    }
                }

                // 2) 取出最早完成的结果并按顺序写回
                maybe_resp = pending.next(), if inflight > 0 => {
                    inflight -= 1;

                    if let Some(resp) = maybe_resp {
                        if let Err(e) = writer.send(resp).await {
                            warn!("Failed to send response to {}: {}", peer_addr, e);
                            break;
                        }
                    }
                }
            }
        }

        info!("Connection handler ended for {}", peer_addr);
        Ok(())
    }

    pub async fn start_redis_server(self: Arc<Self>) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.redis_addr.clone()).await?;

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("New connection accepted from {}", peer_addr);
                    let server = Arc::clone(&self);

                    tokio::spawn(async move {
                        if let Err(e) = server.handle_connection_pipeline(stream, peer_addr).await {
                            error!("Error handling connection from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}
