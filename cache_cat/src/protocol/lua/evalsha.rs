use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::lua::eval::EvalParams;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

/// Parameters for EVALSHA command
///
/// Standard Redis EVALSHA command format:
/// EVALSHA sha1 numkeys key [key ...] arg [arg ...]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvalShaParams {
    /// SHA1 hash of the Lua script
    pub sha1: String,
    /// Number of keys
    pub numkeys: usize,
    /// Key names
    pub keys: Vec<Bytes>,
    /// Arguments
    pub args: Vec<Bytes>,
}

impl Display for EvalShaParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EVALSHA {} ({} keys, {} args)",
            if self.sha1.len() > 20 {
                format!("{}...", &self.sha1[..20])
            } else {
                self.sha1.clone()
            },
            self.numkeys,
            self.args.len()
        )
    }
}

impl EvalShaParams {
    /// Create a new EvalShaParams
    pub fn new(sha1: String, numkeys: usize, keys: Vec<Bytes>, args: Vec<Bytes>) -> Self {
        Self {
            sha1,
            numkeys,
            keys,
            args,
        }
    }

    /// Parse EVALSHA command parameters from RESP array items
    /// Format: EVALSHA sha1 numkeys key [key ...] arg [arg ...]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Minimum: EVALSHA sha1 numkeys
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("evalsha"));
        }

        // Parse sha1
        let sha1 = items[1]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("sha1"))?
            .into_owned();

        // Parse numkeys
        let numkeys = items[2].try_parse_usize()?;

        // Validate key count
        let remaining = items.len() - 3;
        if remaining < numkeys {
            return Err(ProtocolError::InvalidArgument("not enough keys specified"));
        }

        // Parse keys
        let keys = items[3..3 + numkeys]
            .iter()
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if keys.len() < numkeys {
            return Err(ProtocolError::InvalidArgument("key"));
        }

        let start = 3 + numkeys;
        // Parse args
        let args = items
            .iter()
            .skip(start)
            .map_while(|arg_value| match arg_value {
                Value::BulkString(Some(data)) => Some(data.clone()),
                Value::SimpleString(s) => Some(s.clone().into()),
                Value::Integer(i) => Some(i.to_string().into()),
                _ => None,
            })
            .collect::<Vec<_>>();

        if args.len() < items.len() - start {
            return Err(ProtocolError::InvalidArgument("argument"));
        }

        Ok(EvalShaParams::new(sha1, numkeys, keys, args))
    }

    /// Resolve the local script cache before replication so every node receives
    /// the script body, even when its own script cache is empty.
    pub(crate) fn into_operation(
        self,
        scripts: &HashMap<String, Bytes>,
        proto: u8,
    ) -> Result<Operation, ProtocolError> {
        let script = scripts.get(&self.sha1).cloned().ok_or_else(|| {
            ProtocolError::response("NOSCRIPT No matching script. Please use EVAL.")
        })?;
        Ok(Operation::Redis(RedisOperation::RedisEval(EvalParams {
            script,
            keys: self.keys,
            args: self.args,
            numkeys: self.numkeys,
            proto,
        })))
    }
}

/// EVALSHA command executor
pub struct EvalShaCommand;

#[async_trait]
impl Command for EvalShaCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = EvalShaParams::parse(items)?;
        let proto = client.framed.codec().proto_version();
        if let Some(queue) = client.transaction_queue.as_mut() {
            // Cache lookup is deferred until EXEC; NOSCRIPT is an execution
            // error for this command, not a queue-time transaction error.
            queue.push(Operation::Redis(RedisOperation::RedisEvalSha {
                params,
                proto,
            }));
            return Ok(Value::queued());
        }
        let operation = {
            let scripts = server.app.state_machine.data.kvs.lua_env.script_map.lock();
            params.into_operation(&scripts, proto)?
        };
        let result = server.app.write(operation, client.db_number).await?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::config::Config;
    use crate::node::parsed_config::ParsedConfig;
    use crate::node::raft_node::RaftNode;
    use crate::protocol::transaction::discard::DiscardCommand;
    use crate::protocol::transaction::exec::ExecParams;
    use crate::protocol::transaction::multi::MultiCommand;
    use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
    use crate::raft::types::core::mocha::request_handler::do_request;
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::broadcast;
    use tokio::time::timeout;

    fn items(parts: &[&str]) -> Vec<Value> {
        parts
            .iter()
            .map(|part| Value::BulkString(Some(Bytes::copy_from_slice(part.as_bytes()))))
            .collect()
    }

    async fn queue(client: &mut Client, server: &RedisServer, sha: &str, key: &str) {
        let reply = timeout(
            Duration::from_secs(2),
            EvalShaCommand.execute(client, &items(&["EVALSHA", sha, "1", key, "value"]), server),
        )
        .await
        .expect("EVALSHA must queue without submitting a Raft write")
        .unwrap();
        assert_eq!(reply.encode(), b"+QUEUED\r\n");
    }

    #[tokio::test]
    async fn evalsha_defers_execution_and_cache_lookup_until_exec() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.raft.log_path = dir.path().to_str().unwrap().to_owned();
        config.raft.address = "127.0.0.1:0".into();
        config.redis.databases = 2;
        let config = ParsedConfig::from(&config).unwrap();
        let (shutdown_tx, _) = broadcast::channel(1);
        let node = RaftNode::create(config.clone(), shutdown_tx).await.unwrap();
        let server = RedisServer::new(node.app.clone(), "127.0.0.1:0".into(), &config).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let _socket = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (connection, _) = listener.accept().await.unwrap();
        let mut client = Client::new(1, connection, true);
        client.db_number = 1;
        let cache = &node.app.state_machine.data.kvs;
        let script = Bytes::from_static(b"redis.call('SET', KEYS[1], ARGV[1]); return true");

        cache
            .lua_env
            .script_map
            .lock()
            .insert("cached".into(), script.clone());
        MultiCommand
            .execute(&mut client, &items(&["MULTI"]), &server)
            .await
            .unwrap();
        queue(&mut client, &server, "cached", "discarded").await;
        assert!(
            cache.databases[1]
                .mocha
                .get_entry(&b"discarded"[..])
                .is_none()
        );
        assert_eq!(
            DiscardCommand
                .execute(&mut client, &items(&["DISCARD"]), &server)
                .await
                .unwrap()
                .encode(),
            b"+OK\r\n"
        );
        assert!(client.transaction_queue.is_none());
        assert!(
            cache.databases[1]
                .mocha
                .get_entry(&b"discarded"[..])
                .is_none()
        );

        for proto in [2, 3] {
            if proto == 3 {
                client.framed.codec_mut().switch_resp3();
            }
            cache
                .lua_env
                .script_map
                .lock()
                .insert("removed".into(), script.clone());
            cache.lua_env.script_map.lock().remove("loaded-later");
            MultiCommand
                .execute(&mut client, &items(&["MULTI"]), &server)
                .await
                .unwrap();
            queue(&mut client, &server, "removed", "must-not-exist").await;
            queue(&mut client, &server, "loaded-later", "written").await;
            assert!(!client.transaction_failed);
            assert!(
                cache.databases[1]
                    .mocha
                    .get_entry(&b"written"[..])
                    .is_none()
            );

            // Simulate another connection flushing/loading scripts before EXEC.
            cache.lua_env.script_map.lock().clear();
            cache
                .lua_env
                .script_map
                .lock()
                .insert("loaded-later".into(), script.clone());
            let mut params = ExecParams {
                operations: client.transaction_queue.take().unwrap().operations,
            };
            params.resolve_scripts(cache);
            let encoded =
                bincode2::serialize(&Operation::Redis(RedisOperation::RedisExec(params))).unwrap();
            let operation = bincode2::deserialize(&encoded).unwrap();
            let replica = MyCache::new(2).unwrap();
            let mut update_type = UpdateType::None;
            let mut update = Update {
                db_number: 0,
                write_clock: 1,
                update_type: &mut update_type,
            };
            let reply = do_request(&replica, operation, &mut update, true);
            let expected = format!(
                "*2\r\n-NOSCRIPT No matching script. Please use EVAL.\r\n{}",
                if proto == 3 { "#t\r\n" } else { ":1\r\n" }
            );
            assert_eq!(reply.encode_proto(proto), expected.as_bytes());
            assert!(
                replica.databases[1]
                    .mocha
                    .get_entry(&b"must-not-exist"[..])
                    .is_none()
            );
            assert!(
                replica.databases[1]
                    .mocha
                    .get_entry(&b"written"[..])
                    .is_some()
            );
            assert!(
                replica.databases[0]
                    .mocha
                    .get_entry(&b"written"[..])
                    .is_none()
            );
        }
        node.app.cluster.shutdown().await.unwrap();
    }
}
