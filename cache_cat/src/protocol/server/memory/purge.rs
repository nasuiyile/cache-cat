use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;

use async_trait::async_trait;

pub struct MemoryPurgeCommand;

impl MemoryPurgeCommand {
    fn parse(items: &[Value]) -> Result<(), ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("memory|purge"));
        }

        let memory = items[0]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("command"))?;

        if !memory.as_ref().eq_ignore_ascii_case(b"MEMORY") {
            return Err(ProtocolError::InvalidArgument("command"));
        }

        let purge = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("subcommand"))?;

        if !purge.as_ref().eq_ignore_ascii_case(b"PURGE") {
            return Err(ProtocolError::InvalidArgument("subcommand"));
        }

        Ok(())
    }

    fn purge() {
        /*
         * Force mimalloc to eagerly collect free memory.
         *
         * mi_collect(true):
         *
         * - processes delayed/cross-thread frees
         * - collects completely empty mimalloc pages/segments
         * - aggressively purges/decommits unused memory
         * - attempts to return physical memory to the OS
         *
         * It does NOT move live allocations, so this is not a
         * compacting GC. Memory occupied by pages containing
         * live allocations cannot necessarily be reclaimed.
         *
         * SAFETY:
         *
         * mi_collect is a C FFI function exposed by
         * libmimalloc_sys as unsafe. Calling it with a bool does
         * not require us to maintain any pointer invariants.
         *
         * mimalloc guarantees that live allocations remain valid.
         */
        unsafe {
            libmimalloc_sys::mi_collect(true);
        }
    }

    fn ok() -> Value {
        Value::ok()
    }
}

#[async_trait]
impl SubCommand for MemoryPurgeCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        Self::parse(items)?;

        Self::purge();

        Ok(Self::ok())
    }
}
