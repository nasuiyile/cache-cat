use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
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
    pub password: Option<String>,
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
            let proto_val = &items[idx];
            let requested = match proto_val {
                Value::Integer(v) => {
                    if *v < 0 || *v > 255 {
                        return Err(ProtocolError::response(
                            "NOPROTO unsupported protocol version",
                        ));
                    }
                    *v as u8
                }
                Value::BulkString(Some(data)) => {
                    String::from_utf8_lossy(data).parse::<u8>().map_err(|_| {
                        ProtocolError::response(
                            "ERR Protocol version is not an integer or out of range",
                        )
                    })?
                }
                Value::BulkString(None) => {
                    return Err(ProtocolError::InvalidArgument(
                        "protocol version cannot be null",
                    ));
                }
                _ => {
                    return Err(ProtocolError::InvalidArgument(
                        "protocol version must be integer or string",
                    ));
                }
            };
            // Validate protocol version
            if requested != 2 && requested != 3 {
                return Err(ProtocolError::response(
                    "NOPROTO unsupported protocol version",
                ));
            }
            proto_version = Some(requested);
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
                        Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
                        Value::BulkString(None) => {
                            return Err(ProtocolError::InvalidArgument(
                                "AUTH password cannot be null",
                            ));
                        }
                        Value::SimpleString(s) => s.clone(),
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
                    let name = match &items[idx] {
                        Value::BulkString(Some(data)) => {
                            Some(String::from_utf8_lossy(data).to_string())
                        }
                        Value::BulkString(None) => None,
                        Value::SimpleString(s) => Some(s.clone()),
                        _ => {
                            return Err(ProtocolError::InvalidArgument(
                                "client name must be string",
                            ));
                        }
                    };
                    client_name = name;
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
            // Validate password against server config
            match &server.app.config.password {
                Some(configured_password) => {
                    if password != configured_password {
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
