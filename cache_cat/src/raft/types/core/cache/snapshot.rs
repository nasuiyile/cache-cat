use crate::raft::types::core::cache::moka::{MyCache, MyValue, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{BaseOperation, DelReq, IncrReq, LPushReq, SetReq};
use crate::raft::types::entry::request::AtomicRequest;
use crate::utils::parse_i64;
use moka::Entry;
use moka::ops::compute::{CompResult, Op};
use std::collections::LinkedList;
use std::sync::Arc;
use tokio::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

impl MyCache {
    // todo 优化为字节编码
    //流式序列化和反序列化
    pub async fn dump_cache_to_writer<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWrite + Unpin + Send,
    {
        for entry in self.cache.iter() {
            let (k_arc, v) = entry;
            let key_bytes = bincode2::serialize(&*k_arc)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let val_bytes = bincode2::serialize(&v)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            writer.write_u64(key_bytes.len() as u64).await?;
            writer.write_all(&key_bytes).await?;
            writer.write_u64(val_bytes.len() as u64).await?;
            writer.write_all(&val_bytes).await?;
        }

        writer.write_u64(0).await?;
        Ok(())
    }
    pub async fn load_cache_from_reader<R>(&self, reader: &mut R) -> Result<(), io::Error>
    where
        R: AsyncRead + Unpin,
    {
        loop {
            let key_len = match reader.read_u64().await {
                Ok(v) => v as usize,
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(e);
                    }
                }
            };
            if key_len == 0 {
                break;
            }

            let mut key_buf = vec![0u8; key_len];
            reader.read_exact(&mut key_buf).await?;

            let val_len = reader.read_u64().await? as usize;
            let mut val_buf = vec![0u8; val_len];
            reader.read_exact(&mut val_buf).await?;

            let key_vec: Vec<u8> = bincode2::deserialize(&key_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let value: MyValue = bincode2::deserialize(&val_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            self.cache.insert(Arc::new(key_vec), value).await;
        }

        Ok(())
    }
}
