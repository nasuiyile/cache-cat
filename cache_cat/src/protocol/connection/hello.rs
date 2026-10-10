use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::connection::client::parse_client_name;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;

/// Parsed HELLO arguments
#[derive(Debug)]
pub struct HelloParam {
    /// Requested protocol version. `None` when HELLO is called without a
    /// protover argument: in that case (like Redis) the connection keeps
    /// its current protocol and the command just reports the context.
    pub proto_version: Option<u8>,
    pub username: Option<String>,
    pub password: Option<Bytes>,
    pub client_name: Option<String>,
}

/// HELLO command handler
/// Supports protocol version negotiation (RESP2/RESP3)
/// Format: HELLO [protover [AUTH username password] [SETNAME clientname]]
pub struct HelloCommand;

impl HelloParam {
    /// Parse arguments from RESP items
    /// Format: HELLO [protover [AUTH username password] [SETNAME clientname]]
    pub fn parse(items: &[Value]) -> Result<HelloParam, ProtocolError> {
        if items.is_empty() {
            return Err(ProtocolError::WrongArgCount("HELLO"));
        }
        let mut proto_version: Option<u8> = None;
        let mut username = None;
        let mut password = None;
        let mut client_name = None;
        let mut idx = 1; // Skip command name
        // Parse optional protocol version
        if idx < items.len() {
            let requested = items[idx].try_parse_canonical_i64().map_err(|_| {
                ProtocolError::response("ERR Protocol version is not an integer or out of range")
            })?;
            // Validate protocol version
            if requested != 2 && requested != 3 {
                return Err(ProtocolError::response(
                    "NOPROTO unsupported protocol version",
                ));
            }
            proto_version = Some(requested as u8);
            idx += 1;
        }
        // Parse optional AUTH and/or SETNAME
        while idx < items.len() {
            let option = match &items[idx] {
                Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
                Value::SimpleString(s) => s.to_uppercase(),
                _ => {
                    return Err(ProtocolError::InvalidArgument(
                        "HELLO option must be AUTH or SETNAME",
                    ));
                }
            };
            match option.as_str() {
                "AUTH" => {
                    idx += 1;

                    // Check if we have enough arguments for AUTH
                    if idx >= items.len() {
                        return Err(ProtocolError::WrongArgCount("HELLO AUTH"));
                    }
                    // Parse username (Redis 6+ style) or password (Redis 5 style)
                    let auth_username = match &items[idx] {
                        Value::BulkString(Some(data)) => {
                            Some(String::from_utf8_lossy(data).to_string())
                        }
                        Value::BulkString(None) => None,
                        Value::SimpleString(s) => Some(s.clone()),
                        _ => {
                            return Err(ProtocolError::InvalidArgument(
                                "AUTH username must be string",
                            ));
                        }
                    };
                    idx += 1;
                    // Check if next argument is password
                    if idx >= items.len() {
                        return Err(ProtocolError::WrongArgCount("HELLO AUTH missing password"));
                    }
                    let auth_password = match &items[idx] {
                        Value::BulkString(Some(data)) => data.clone(),
                        Value::BulkString(None) => {
                            return Err(ProtocolError::InvalidArgument(
                                "AUTH password cannot be null",
                            ));
                        }
                        Value::SimpleString(s) => Bytes::copy_from_slice(s.as_bytes()),
                        _ => {
                            return Err(ProtocolError::InvalidArgument(
                                "AUTH password must be string",
                            ));
                        }
                    };
                    // Redis 6 format with username, or Redis 5 format (username is "default")
                    if auth_username.is_some() {
                        username = auth_username;
                    }
                    password = Some(auth_password);

                    idx += 1;
                }
                "SETNAME" => {
                    idx += 1;

                    if idx >= items.len() {
                        return Err(ProtocolError::WrongArgCount("HELLO SETNAME"));
                    }
                    client_name = Some(parse_client_name(&items[idx])?);
                    idx += 1;
                }
                _ => {
                    return Err(ProtocolError::UnknownCommand("HELLO".to_string()));
                }
            }
        }
        Ok(HelloParam {
            proto_version,
            username,
            password,
            client_name,
        })
    }
}

#[async_trait]
impl Command for HelloCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // Parse arguments first
        let params = match HelloParam::parse(items) {
            Ok(p) => p,
            Err(e) => return Err(e.into()),
        };
        // Handle authentication if password provided
        if let Some(password) = &params.password {
            if params.username.as_deref() != Some("default") {
                return Err(ProtocolError::AuthenticationFailed.into());
            }
            // Validate password against server config
            match &server.app.config.password {
                Some(configured_password) => {
                    if password.as_ref() != configured_password.as_bytes() {
                        return Err(ProtocolError::AuthenticationFailed.into());
                    }
                    client.authenticated = true;
                }
                None => {
                    // If no password configured, authenticate anyway (or deny based on security policy)
                    // For security, you might want to reject if server doesn't require auth
                    client.authenticated = true;
                }
            }
        }
        if !client.authenticated {
            return Err(ProtocolError::response(
                "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time",
            )
            .into());
        }
        // Set client name if provided
        if let Some(name) = params.client_name {
            client.name = name;
        }
        // Switch protocol only when a version was explicitly requested;
        // a bare HELLO just reports the current connection context.
        match params.proto_version {
            Some(2) => client.framed.codec_mut().switch_resp2(),
            Some(3) => client.framed.codec_mut().switch_resp3(),
            _ => {}
        }
        let current_proto = client.framed.codec().proto_version();
        // Build the response: a map reply, exactly like Redis.
        // (The encoder emits %7 for RESP3 and a flat *14 array for RESP2.)
        let bulk = |s: &'static [u8]| Value::BulkString(Some(Bytes::from_static(s)));
        let map_pairs = vec![
            (bulk(b"server"), bulk(b"redis")),
            (
                bulk(b"version"),
                Value::BulkString(Some(Bytes::from_static(
                    env!("CARGO_PKG_VERSION").as_bytes(),
                ))),
            ),
            (bulk(b"proto"), Value::Integer(current_proto as i64)),
            (bulk(b"id"), Value::Integer(client.id as i64)),
            (bulk(b"mode"), bulk(b"standalone")),
            (bulk(b"role"), bulk(b"master")),
            (bulk(b"modules"), Value::Array(Some(Vec::new()))),
        ];

        Ok(Value::Map(map_pairs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::config::Config;
    use crate::node::parsed_config::ParsedConfig;
    use crate::node::raft_node::RaftNode;
    use crate::protocol::connection::client::core::ClientCommand;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::broadcast;

    fn bulk(value: &[u8]) -> Value {
        Value::BulkString(Some(Bytes::copy_from_slice(value)))
    }

    #[test]
    fn protocol_version_requires_a_canonical_integer() {
        for version in [
            b"02".as_slice(),
            b"+2",
            b"-0",
            b" 2",
            b"2 ",
            b"",
            b"9223372036854775808",
        ] {
            let error = HelloParam::parse(&[bulk(b"HELLO"), bulk(version)]).unwrap_err();
            assert_eq!(
                error,
                ProtocolError::response("ERR Protocol version is not an integer or out of range"),
                "version {version:?}",
            );
        }
        for version in [b"-1".as_slice(), b"0", b"4", b"256", b"9223372036854775807"] {
            let error = HelloParam::parse(&[bulk(b"HELLO"), bulk(version)]).unwrap_err();
            assert_eq!(
                error,
                ProtocolError::response("NOPROTO unsupported protocol version"),
                "version {version:?}",
            );
        }
        for (version, expected) in [(b"2", 2), (b"3", 3)] {
            let params = HelloParam::parse(&[bulk(b"HELLO"), bulk(version)]).unwrap();
            assert_eq!(params.proto_version, Some(expected));
        }
        assert_eq!(
            HelloParam::parse(&[bulk(b"HELLO")]).unwrap().proto_version,
            None
        );
    }

    #[tokio::test]
    async fn invalid_client_names_leave_connection_state_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.raft.log_path = dir.path().to_str().unwrap().to_owned();
        config.raft.address = "127.0.0.1:0".into();
        config.redis.databases = 1;
        config.redis.requirepass = Some("secret".into());
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
        client.name = "previous".into();
        let client_command = ClientCommand::new();
        let hello_prefix = [
            b"HELLO".as_slice(),
            b"3",
            b"AUTH",
            b"default",
            b"secret",
            b"SETNAME",
        ];
        let client_prefix = [b"CLIENT".as_slice(), b"SETNAME"];

        for (command, prefix) in [
            (&HelloCommand as &dyn Command, hello_prefix.as_slice()),
            (&client_command as &dyn Command, client_prefix.as_slice()),
        ] {
            for name in [
                b"with space".as_slice(),
                b"line\nbreak",
                b"\t",
                b"\0",
                b"\x7f",
                b"\xff",
            ] {
                let mut items: Vec<_> = prefix.iter().map(|arg| bulk(arg)).collect();
                items.push(bulk(name));
                let error = command
                    .execute(&mut client, &items, &server)
                    .await
                    .unwrap_err();
                assert_eq!(
                    Value::from(error).encode(),
                    b"-ERR Client names cannot contain spaces, newlines or special characters.\r\n",
                );
                assert_eq!(client.name, "previous");
                assert!(!client.authenticated);
                assert_eq!(client.framed.codec().proto_version(), 2);
            }
        }
        for (command, prefix) in [
            (&HelloCommand as &dyn Command, hello_prefix.as_slice()),
            (&client_command as &dyn Command, client_prefix.as_slice()),
        ] {
            for name in [b"worker:1-~".as_slice(), b""] {
                let mut items: Vec<_> = prefix.iter().map(|arg| bulk(arg)).collect();
                items.push(bulk(name));
                command.execute(&mut client, &items, &server).await.unwrap();
                assert_eq!(client.name.as_bytes(), name);
                assert!(client.authenticated);
                assert_eq!(client.framed.codec().proto_version(), 3);
            }
        }
        node.app.cluster.shutdown().await.unwrap();
    }
}
