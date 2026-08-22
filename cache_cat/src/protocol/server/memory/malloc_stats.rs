use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;

use async_trait::async_trait;
use bytes::Bytes;

use libmimalloc_sys::mi_stats_print_out;

use std::ffi::{c_char, c_void, CStr};
use std::sync::Mutex;

#[derive(Debug)]
struct MallocStats {
    report: Bytes,
}

impl MallocStats {
    fn collect() -> Self {
        /*
         * mimalloc 的 mi_stats_print_out() 对应 Redis/jemalloc
         * 的 malloc_stats_print()。
         *
         * 这里不解析、不重命名 allocator 字段，
         * 直接返回 mimalloc 原生统计报告。
         */
        let output = Mutex::new(
            Vec::<u8>::with_capacity(16 * 1024)
        );

        unsafe extern "C" fn output_callback(
            msg: *const c_char,
            arg: *mut c_void,
        ) {
            if msg.is_null() || arg.is_null() {
                return;
            }

            let output =
                unsafe { &*(arg as *const Mutex<Vec<u8>>) };

            let msg =
                unsafe { CStr::from_ptr(msg) };

            /*
             * mi_output_fun 要求 callback thread-safe。
             *
             * 即使 mi_stats_print_out() 当前通常同步调用，
             * 这里仍按照 API contract 使用 Mutex。
             */
            match output.lock() {
                Ok(mut output) => {
                    output.extend_from_slice(msg.to_bytes());
                }

                /*
                 * callback 本身不应该 panic 穿过 FFI boundary。
                 * 即使 mutex poisoned，也继续取得内部 buffer。
                 */
                Err(poisoned) => {
                    let mut output = poisoned.into_inner();
                    output.extend_from_slice(msg.to_bytes());
                }
            }
        }

        unsafe {
            mi_stats_print_out(
                Some(output_callback),
                (&output as *const Mutex<Vec<u8>>)
                    .cast_mut()
                    .cast::<c_void>(),
            );
        }

        let output = match output.into_inner() {
            Ok(output) => output,
            Err(poisoned) => poisoned.into_inner(),
        };

        Self {
            report: Bytes::from(output),
        }
    }

    fn into_value(self) -> Value {
        /*
         * Redis MEMORY MALLOC-STATS:
         *
         * RESP3:
         *   =<len>\r\n
         *   txt:<allocator report>
         *
         * RESP2:
         *   $<len>\r\n
         *   <allocator report>
         *
         * Value::VerbatimString 的 encoder 已经负责
         * RESP2 -> BulkString 的兼容转换。
         */
        Value::VerbatimString {
            format: "txt".to_string(),
            data: self.report,
        }
    }
}

pub struct MemoryMallocStatsCommand;

impl MemoryMallocStatsCommand {
    fn parse(items: &[Value]) -> Result<(), ProtocolError> {
        if items.len() != 2 {
            return Err(
                ProtocolError::WrongArgCount(
                    "MEMORY MALLOC-STATS"
                )
            );
        }

        let memory = items[0]
            .string_bytes_clone()
            .ok_or(
                ProtocolError::InvalidArgument("command")
            )?;

        if !memory
            .as_ref()
            .eq_ignore_ascii_case(b"MEMORY")
        {
            return Err(
                ProtocolError::InvalidArgument("command")
            );
        }

        let malloc_stats = items[1]
            .string_bytes_clone()
            .ok_or(
                ProtocolError::InvalidArgument("subcommand")
            )?;

        if !malloc_stats
            .as_ref()
            .eq_ignore_ascii_case(b"MALLOC-STATS")
        {
            return Err(
                ProtocolError::InvalidArgument("subcommand")
            );
        }

        Ok(())
    }
}

#[async_trait]
impl SubCommand for MemoryMallocStatsCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        Self::parse(items)?;

        let stats = MallocStats::collect();

        Ok(stats.into_value())
    }
}