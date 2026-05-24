use crate::mocha::EntrySnapshot;
use crate::raft::types::core::mocha::mocha::{MyCache, MyValue};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::{io, time};

impl MyCache {
    // 流式序列化：遍历所有分片
    pub async fn dump_cache_to_writer<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWrite + Unpin + Send + io::AsyncSeek,
    {
        let shard_count = self.databases.len() as u64;
        writer.write_u64(shard_count).await?;
        for shard in &self.databases {
            // 一次遍历，收集当前分片所有存活快照
            shard.mocha.dump_snapshots_to_writer(writer).await?;
        }
        Ok(())
    }

    // 流式反序列化：重建多个分片
    pub async fn load_cache_from_reader<R>(&self, reader: &mut R) -> Result<(), io::Error>
    where
        R: AsyncRead + Unpin,
    {
        // 读取分片数量
        let shard_count = reader.read_u64().await? as usize;

        // 确保 cache 有足够的容量
        if self.databases.len() < shard_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Expected {} shards, but cache only has {}",
                    shard_count,
                    self.databases.len()
                ),
            ));
        }

        // 读取每个分片的数据
        for shard_idx in 0..shard_count {
            // 读取当前分片的条目数量
            let entry_count = reader.read_u64().await? as usize;

            // 读取当前分片的所有条目
            for _ in 0..entry_count {
                // 读取 key
                let key_len = reader.read_u64().await? as usize;
                let mut key_buf = vec![0u8; key_len];
                reader.read_exact(&mut key_buf).await?;

                // 读取 value
                let val_len = reader.read_u64().await? as usize;
                let mut val_buf = vec![0u8; val_len];
                reader.read_exact(&mut val_buf).await?;

                // 反序列化
                let key_vec: Vec<u8> = bincode2::deserialize(&key_buf)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let value: EntrySnapshot<MyValue> = bincode2::deserialize(&val_buf)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

                // 注意：这里需要根据 key 决定插入到哪个分片
                self.databases[shard_idx]
                    .mocha
                    .insert_snapshot(Arc::new(key_vec), value);
            }
        }

        Ok(())
    }
}
