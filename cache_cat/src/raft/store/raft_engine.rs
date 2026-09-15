use crate::error::{Error, Result};
use crate::raft::types::raft_types::Entry;
use raft_engine::{Config, Engine, MessageExt, ReadableSize, ValueCodec};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

pub fn create_raft_engine<P: AsRef<Path>>(path: P) -> Result<Arc<Engine>> {
    //如果找不到路径就创建
    if !path.as_ref().exists() {
        std::fs::create_dir_all(path.as_ref())?;
    }
    let path = path.as_ref().to_string_lossy().into_owned();
    let config = Config {
        dir: path.clone(),
        purge_threshold: ReadableSize::gb(2),
        batch_compression_threshold: ReadableSize::kb(0),
        ..Default::default()
    };
    match Engine::open(config) {
        Ok(raft_engine) => Ok(Arc::new(raft_engine)),
        Err(err) => Err(Error::config(format!(
            "directory does not exist: {},{}",
            err, path
        ))),
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Bincode2Codec;

impl<T> ValueCodec<T> for Bincode2Codec
where
    T: Serialize + DeserializeOwned,
{
    #[inline]
    fn encode_to(v: &T, buf: &mut Vec<u8>) -> raft_engine::Result<()> {
        // 只能追加：同一个 LogBatch 的多条日志共用一个 buf
        bincode2::serialize_into(&mut *buf, v)
            .map_err(|e| raft_engine::Error::Other(format!("bincode2 encode: {}", e).into()))
    }

    #[inline]
    fn decode(bytes: &[u8]) -> raft_engine::Result<T> {
        bincode2::deserialize(bytes)
            .map_err(|e| raft_engine::Error::Corruption(format!("bincode2 decode: {}", e)))
    }
}

/// 存进 raft-engine 的日志条目。
/// raft-engine 要求 `MessageExt::Entry: PartialEq`，而 openraft 的 `Entry` 只有在
/// `Request: PartialEq` 时才实现，所以包一层。`serde(transparent)` 保证编码和直接编码
/// `Entry` 相同。
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoredEntry(pub Entry);

impl PartialEq for StoredEntry {
    fn eq(&self, other: &Self) -> bool {
        // Raft 的 Log Matching：log_id（term+index）相同的日志内容一定相同。
        // raft-engine 内部并不比较日志，这里只是满足 trait 约束。
        self.0.log_id == other.0.log_id
    }
}

pub struct MessageExtTyped;
impl MessageExt<Bincode2Codec> for MessageExtTyped {
    type Entry = StoredEntry;

    fn index(e: &Self::Entry) -> u64 {
        e.0.log_id.index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::raft_types::{LeaderId, LogId};
    use openraft::entry::RaftEntry;

    #[test]
    fn bincode2_codec_matches_fork_encoding() {
        let entry = Entry::new_blank(LogId::new(
            LeaderId {
                term: 3,
                node_id: 1,
            },
            7,
        ));
        let stored = StoredEntry(entry.clone());

        // 与 fork 版 raft-engine 里的 `bincode2::serialize(e)` 字节一致
        let via_codec = Bincode2Codec::encode_to_vec(&stored).unwrap();
        assert_eq!(via_codec, bincode2::serialize(&entry).unwrap());

        // encode_to 只追加，不覆盖 buf 里已有内容
        let mut buf = b"prefix".to_vec();
        Bincode2Codec::encode_to(&stored, &mut buf).unwrap();
        assert_eq!(&buf[..6], b"prefix");
        assert_eq!(&buf[6..], &via_codec[..]);

        let decoded: StoredEntry = Bincode2Codec::decode(&via_codec).unwrap();
        assert_eq!(decoded.0.log_id, entry.log_id);
        assert!(<Bincode2Codec as ValueCodec<StoredEntry>>::decode(&[0xff]).is_err());
    }
}
