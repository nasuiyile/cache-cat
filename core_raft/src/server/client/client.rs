use bincode2;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Serialize;
use serde::de::DeserializeOwned;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// =======================
// RPC Client
// =======================

pub struct RpcClient {
    stream: TcpStream,
    next_request_id: u32,
}

impl RpcClient {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            next_request_id: 1,
        })
    }

    pub async fn call<Req, Res>(
        &mut self,
        func_id: u32,
        req: Req,
    ) -> Result<Res, Box<dyn std::error::Error>>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let request_id = self.next_request_id;
        self.next_request_id += 1;

        // ---------- 序列化请求 ----------
        let payload: Vec<u8> = bincode2::serialize(&req)?;
        let length = (4 + 4 + payload.len()) as u32;

        let mut buf = BytesMut::with_capacity(12 + payload.len());
        buf.put_u32(length); // frame length
        buf.put_u32(request_id); // request id
        buf.put_u32(func_id); // func id
        buf.extend_from_slice(&payload);

        // ---------- 发送 ----------
        self.stream.write_all(&buf).await?;

        // ---------- 读取响应长度 ----------
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;

        // ---------- 读取响应体 ----------
        let mut resp_buf = BytesMut::with_capacity(resp_len);
        resp_buf.resize(resp_len, 0);
        self.stream.read_exact(&mut resp_buf).await?;

        let mut resp_buf: Bytes = resp_buf.freeze();

        // ---------- 解析响应 ----------
        let resp_request_id = resp_buf.get_u32();
        if resp_request_id != request_id {
            return Err("request_id mismatch".into());
        }

        let res: Res = bincode2::deserialize(resp_buf.as_ref())?;
        Ok(res)
    }
}
