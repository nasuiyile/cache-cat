//! AUTH command implementation

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

/// AUTH command handler
pub struct AuthCommand;

#[async_trait]
impl Command for AuthCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("auth").into());
        }

        let password = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::SyntaxError)?;

        let configured_password =
            server
                .app
                .config
                .password
                .as_ref()
                .ok_or(ProtocolError::response(
                    "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
                ))?;

        if password.as_ref() == configured_password.as_bytes() {
            client.authenticated = true;
            Ok(Value::ok())
        } else {
            Err(ProtocolError::AuthenticationFailed.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::config::Config;
    use crate::node::parsed_config::ParsedConfig;
    use crate::node::raft_node::RaftNode;
    use crate::protocol::connection::hello::HelloCommand;
    use bytes::Bytes;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn auth_and_hello_compare_password_bytes() {
        for configured_password in ["secret", "\u{fffd}"] {
            let dir = tempfile::tempdir().unwrap();
            let mut config = Config::default();
            config.raft.log_path = dir.path().to_str().unwrap().to_owned();
            config.raft.address = "127.0.0.1:0".into();
            config.redis.databases = 1;
            config.redis.requirepass = Some(configured_password.to_owned());
            let config = ParsedConfig::from(&config).unwrap();
            let (shutdown_tx, _) = broadcast::channel(1);
            let node = RaftNode::create(config, shutdown_tx).await.unwrap();
            let server =
                RedisServer::new(node.app.clone(), "127.0.0.1:0".into(), &node.app.config).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let _socket = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (connection, _) = listener.accept().await.unwrap();
            let mut client = Client::new(1, connection, false);

            for (command, prefix) in [
                (&AuthCommand as &dyn Command, &["AUTH"][..]),
                (
                    &HelloCommand as &dyn Command,
                    &["HELLO", "3", "AUTH", "default"][..],
                ),
            ] {
                for (password, accepted) in [
                    (&b"\xff"[..], false),
                    (&b"\xfe"[..], false),
                    (configured_password.as_bytes(), true),
                ] {
                    client.authenticated = false;
                    let mut items: Vec<_> = prefix
                        .iter()
                        .map(|part| {
                            Value::BulkString(Some(Bytes::copy_from_slice(part.as_bytes())))
                        })
                        .collect();
                    items.push(Value::BulkString(Some(Bytes::copy_from_slice(password))));
                    let result = command.execute(&mut client, &items, &server).await;
                    if accepted {
                        assert!(result.is_ok(), "{result:?}");
                    } else {
                        assert_eq!(
                            Value::from(result.unwrap_err()).encode(),
                            Value::from(ProtocolError::AuthenticationFailed).encode()
                        );
                    }
                    assert_eq!(client.authenticated, accepted);
                }
            }
            node.app.cluster.shutdown().await.unwrap();
        }
    }
}
