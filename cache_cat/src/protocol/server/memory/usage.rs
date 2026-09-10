use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, SubCommand};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use std::fmt::Display;
use std::mem::size_of_val;

/// Redis 默认 MEMORY USAGE 的 samples 数量。
const DEFAULT_SAMPLES: usize = 5;

/// MEMORY USAGE key [SAMPLES count]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryUsageParams {
    pub key: Bytes,

    /// 对支持采样的类型，0 遍历全部元素，正数按样本平均大小外推。
    /// 固定开销独立计入；标量及 Bloom 的估算不受 samples 影响。
    pub samples: usize,
}

impl Display for MemoryUsageParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MEMORY USAGE {} SAMPLES {}",
            String::from_utf8_lossy(&self.key),
            self.samples
        )
    }
}

impl MemoryUsageParams {
    /// Parse:
    ///
    /// MEMORY USAGE key
    /// MEMORY USAGE key SAMPLES count
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("MEMORY USAGE"));
        }

        // MEMORY
        let memory = items[0]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("command"))?;

        if !memory.as_ref().eq_ignore_ascii_case(b"MEMORY") {
            return Err(ProtocolError::InvalidArgument("command"));
        }

        // USAGE
        let usage = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("subcommand"))?;

        if !usage.as_ref().eq_ignore_ascii_case(b"USAGE") {
            return Err(ProtocolError::InvalidArgument("subcommand"));
        }

        // key
        let key = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        // Redis memoryCommand accepts repeated SAMPLES options; the last wins.
        let mut samples = DEFAULT_SAMPLES;
        for option in items[3..].chunks(2) {
            let [keyword, count] = option else {
                return Err(ProtocolError::SyntaxError);
            };
            if !keyword
                .string_bytes_clone()
                .is_some_and(|bytes| bytes.eq_ignore_ascii_case(b"SAMPLES"))
            {
                return Err(ProtocolError::SyntaxError);
            }
            let bytes = count
                .string_bytes_clone()
                .ok_or(ProtocolError::NotAnInteger)?;
            let count = std::str::from_utf8(&bytes)
                .ok()
                .and_then(|text| text.parse::<i64>().ok())
                // Redis string2ll accepts canonical decimal only: no '+',
                // leading zeroes, '-0', whitespace, or values outside i64.
                .filter(|count| count.to_string().as_bytes() == bytes.as_ref())
                .ok_or(ProtocolError::NotAnInteger)?;
            if count < 0 {
                return Err(ProtocolError::SyntaxError);
            }
            // A budget larger than usize::MAX already covers any container.
            samples = usize::try_from(count).unwrap_or(usize::MAX);
        }

        Ok(Self { key, samples })
    }
}

impl ReadCommand for MemoryUsageParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let Some(snapshot) = value else {
            // Redis: MEMORY USAGE missing-key -> nil
            return Value::BulkString(None);
        };
        let size = size_of_val(&snapshot.value)
            .saturating_add(std::mem::size_of::<Bytes>())
            .saturating_add(self.key.len())
            // 由各类型决定采样单位或直接统计，遵循 Redis 的按类型分派。
            .saturating_add(snapshot.value.data.estimated_heap_usage(self.samples));
        // RESP integer 是 i64。
        let size = i64::try_from(size).unwrap_or(i64::MAX);
        Value::Integer(size)
    }
}



/// MEMORY command executor.
///
/// 当前这里只实现 MEMORY USAGE。
pub struct MemoryUsageCommand;

impl ReadRaftCommand for MemoryUsageCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::MemoryUsage(MemoryUsageParams::parse(items)?))
    }
}

#[async_trait]
impl SubCommand for MemoryUsageCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // MULTI / EXEC
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}
