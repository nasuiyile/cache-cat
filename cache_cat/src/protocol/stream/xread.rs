//! Redis 8.10 XREAD with MAXCOUNT and MAXSIZE support.

use crate::error::{CacheCatError, Error, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::ReadRaftCommand;
use crate::raft::application::blocking_keys::{Registration, WaitError};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::{MyCache, MyValue};
use crate::raft::types::core::mocha::read_command::MultiReadCommand;
use crate::raft::types::core::response_value::{Resp2MapEncoding, Value};
use crate::raft::types::core::structure::stream::{Entry, Fields, ReadStart, StreamId};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{fmt, future::Future, time::Duration};
use tokio::io::AsyncReadExt;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum XReadId {
    After(StreamId),
    Tail,
    Latest,
}

impl XReadId {
    fn start(self) -> ReadStart {
        match self {
            Self::After(id) => ReadStart::After(id),
            Self::Tail => ReadStart::Tail,
            Self::Latest => ReadStart::Latest,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XReadParams {
    pub keys: Vec<Bytes>,
    pub ids: Vec<XReadId>,
    pub count: Option<usize>,
    pub max_count: Option<usize>,
    pub max_size: Option<usize>,
    pub block_ms: Option<u64>,
    /// Used for MAXSIZE wire-byte accounting.
    pub resp_version: u8,
}

impl fmt::Display for XReadParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "XREAD")?;
        for (name, limit) in [
            ("COUNT", self.count),
            ("MAXCOUNT", self.max_count),
            ("MAXSIZE", self.max_size),
        ] {
            if let Some(limit) = limit {
                write!(f, " {name} {limit}")?;
            }
        }
        if let Some(ms) = self.block_ms {
            write!(f, " BLOCK {ms}")?;
        }
        write!(f, " STREAMS")?;
        for key in &self.keys {
            write!(f, " {}", String::from_utf8_lossy(key))?;
        }
        for id in &self.ids {
            match id {
                XReadId::After(id) => write!(f, " {id}")?,
                XReadId::Tail => write!(f, " $")?,
                XReadId::Latest => write!(f, " +")?,
            }
        }
        Ok(())
    }
}

fn integer(value: &Value) -> Result<i64, ProtocolError> {
    let text = value.as_str_lossy().ok_or(ProtocolError::NotAnInteger)?;

    let number = text
        .parse::<i64>()
        .map_err(|_| ProtocolError::NotAnInteger)?;
    if number.to_string() != text {
        return Err(ProtocolError::NotAnInteger);
    }
    Ok(number)
}

fn decimal_len(mut value: usize) -> usize {
    let mut len = 1;

    while value >= 10 {
        value /= 10;
        len += 1;
    }

    len
}

fn aggregate_len(count: usize) -> usize {
    1 + decimal_len(count) + 2
}

fn bulk_len(len: usize) -> usize {
    1 + decimal_len(len) + 2 + len + 2
}

fn entry_wire_size(id: StreamId, fields: &Fields) -> usize {
    aggregate_len(2)
        + bulk_len(id.to_string().len())
        + aggregate_len(fields.len() * 2)
        + fields
            .iter()
            .map(|(field, value)| bulk_len(field.len()) + bulk_len(value.len()))
            .sum::<usize>()
}

fn xread_entry(entry: Entry) -> Value {
    let id = entry.id.to_string();
    let field_count = entry.fields.len() * 2;
    let mut fields = Vec::with_capacity(field_count);
    for (field, value) in entry.fields {
        fields.push(Value::BulkString(Some(Bytes::from(field))));
        fields.push(Value::BulkString(Some(Bytes::from(value))));
    }
    Value::Array(Some(vec![
        Value::BulkString(Some(Bytes::from(id))),
        Value::Array(Some(fields)),
    ]))
}

impl XReadParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("xread"));
        }
        let mut params = Self {
            keys: Vec::new(),
            ids: Vec::new(),
            count: None,
            max_count: None,
            max_size: None,
            block_ms: None,
            resp_version: 2,
        };
        let mut index = 1;
        let streams = loop {
            let option = items
                .get(index)
                .and_then(Value::as_str_lossy)
                .ok_or(ProtocolError::SyntaxError)?;
            if option.eq_ignore_ascii_case("STREAMS") {
                let streams = &items[index + 1..];
                if streams.is_empty() {
                    return Err(ProtocolError::SyntaxError);
                }
                if !streams.len().is_multiple_of(2) {
                    return Err(ProtocolError::response(
                        "ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.",
                    ));
                }
                break streams;
            }
            let argument = items.get(index + 1).ok_or(ProtocolError::SyntaxError)?;
            match option.to_ascii_uppercase().as_str() {
                "COUNT" => {
                    let count = integer(argument)?;
                    params.count = if count > 0 {
                        Some(usize::try_from(count).map_err(|_| ProtocolError::NotAnInteger)?)
                    } else {
                        None
                    };
                }
                "MAXCOUNT" | "MAXSIZE" => {
                    let limit = integer(argument)?;
                    if limit <= 0 {
                        return Err(ProtocolError::response(format!(
                            "ERR {} must be a positive integer",
                            option.to_ascii_uppercase()
                        )));
                    }
                    let limit =
                        Some(usize::try_from(limit).map_err(|_| ProtocolError::NotAnInteger)?);
                    if option.eq_ignore_ascii_case("MAXCOUNT") {
                        params.max_count = limit;
                    } else {
                        params.max_size = limit;
                    }
                }
                "BLOCK" => {
                    let ms = integer(argument).map_err(|_| {
                        ProtocolError::response("ERR timeout is not an integer or out of range")
                    })?;
                    if ms < 0 {
                        return Err(ProtocolError::response("ERR timeout is negative"));
                    }
                    if tokio::time::Instant::now()
                        .checked_add(Duration::from_millis(ms as u64))
                        .is_none()
                    {
                        return Err(ProtocolError::response("ERR timeout is out of range"));
                    }
                    params.block_ms = Some(ms as u64);
                }
                _ => return Err(ProtocolError::SyntaxError),
            }
            index += 2;
        };
        if matches!(
            (params.count, params.max_count),
            (Some(count), Some(max)) if max < count
        ) {
            return Err(ProtocolError::response(
                "ERR MAXCOUNT must be greater than or equal to COUNT",
            ));
        }
        let num_keys = streams.len() / 2;
        params.keys = streams[..num_keys]
            .iter()
            .map(|value| {
                value
                    .string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("key"))
            })
            .collect::<Result<_, _>>()?;

        params.ids = streams[num_keys..]
            .iter()
            .map(|value| {
                let id = value
                    .as_str_lossy()
                    .ok_or(ProtocolError::InvalidArgument("id"))?;

                match id.as_ref() {
                    "$" => Ok(XReadId::Tail),
                    "+" => Ok(XReadId::Latest),
                    ">" => Err(ProtocolError::response(
                        "ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option.",
                    )),
                    _ => id
                        .parse()
                        .map(XReadId::After)
                        .map_err(|_| {
                            ProtocolError::response(
                                "ERR Invalid stream ID specified as stream command argument",
                            )
                        }),
                }
            })
            .collect::<Result<_, _>>()?;
        Ok(params)
    }

    /// Resolve `$` and read under the same cache locks used by multi-key reads.
    /// Once resolved, the cursor is retained in the pending request so the
    /// writer can test future entries against this same starting point.
    fn read_once(&mut self, cache: &MyCache, db_number: u16) -> Value {
        let database = match cache.get_cache(db_number) {
            Ok(database) => database,
            Err(error) => return error,
        };
        let read_clock = cache.get_and_update_read_clock();
        let values: Vec<_> = self
            .keys
            .iter()
            .map(|key| database.mocha.get_with_read_clock(key, Some(read_clock)))
            .collect();
        for (id, value) in self.ids.iter_mut().zip(&values) {
            if *id == XReadId::Tail {
                let last_id = match value {
                    None => StreamId::ZERO,
                    Some(snapshot) => match &snapshot.value.data {
                        ValueObject::Stream(stream) => stream.read().last_generated_id(),
                        _ => return ProtocolError::WrongType.into(),
                    },
                };
                *id = XReadId::After(last_id);
            }
        }
        self.execute(values)
    }

    async fn read_with_blocking<F, Fut>(
        mut self,
        cache: &MyCache,
        db_number: u16,
        before_read: F,
    ) -> Result<Value, CacheCatError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), CacheCatError>>,
    {
        enum InitialRead {
            Ready(Value),
            Waiting(Registration<(u16, Bytes), Value, XReadParams>),
        }

        let deadline = match self.block_ms {
            Some(ms) if ms > 0 => Some(
                Instant::now()
                    .checked_add(Duration::from_millis(ms))
                    .ok_or_else(|| ProtocolError::response("ERR timeout is out of range"))?,
            ),
            _ => None,
        };
        let keys: Vec<_> = self
            .keys
            .iter()
            .map(|key| (db_number, key.clone()))
            .collect();
        let initial_read = async {
            before_read().await?;
            let registration = {
                let _write_lock = cache.write_lock.lock().await;
                let _read_lock = cache.read_lock.read();
                let result = self.read_once(cache, db_number);
                if !matches!(result, Value::Array(None)) || self.block_ms.is_none() {
                    return Ok(InitialRead::Ready(result));
                }
                // Redis supplies a default batch size only after the command
                // actually blocks; an immediately available read is unrestricted.
                self.count.get_or_insert(1000);
                // Hold the write lock through both observation and registration.
                // The writer subsequently builds the response before allowing
                // another command to delete or change the stream.
                cache
                    .blocking_keys
                    .register_with(keys, deadline, self)
                    .map_err(|error| Error::internal(format!("cannot register XREAD: {error:?}")))?
            };
            Ok::<_, CacheCatError>(InitialRead::Waiting(registration))
        };
        let initial = match deadline {
            // Bound lease checks and cache lock waits too. Once registered,
            // the registry arbitrates timeout against a writer that has already
            // selected a response but has not yet sent it to this connection.
            Some(deadline) => tokio::time::timeout_at(deadline, initial_read)
                .await
                .unwrap_or(Ok(InitialRead::Ready(Value::Array(None)))),
            None => initial_read.await,
        }?;
        match initial {
            InitialRead::Ready(result) => Ok(result),
            InitialRead::Waiting(registration) => match registration.wait().await {
                Ok((_, result)) => Ok(result),
                Err(WaitError::Timeout) => Ok(Value::Array(None)),
                Err(error) => Err(Error::internal(format!("XREAD wait failed: {error:?}"))),
            },
        }
    }
}

pub struct XReadCommand;

impl ReadRaftCommand for XReadCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::XRead(XReadParams::parse(items)?))
    }
}

#[async_trait]
impl Command for XReadCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let mut params = XReadParams::parse(items)?;
        params.resp_version = client.framed.codec().proto_version();
        if let Some(queue) = client.transaction_queue.as_mut() {
            // EXEC runs the ordinary read operation without waiting.
            params.block_ms = None;
            queue.push(Operation::Read(ReadOperation::XRead(params)));
            return Ok(Value::queued());
        }
        let blocking = params.block_ms.is_some();
        let read =
            params.read_with_blocking(&server.app.state_machine.data.kvs, client.db_number, || {
                server.app.cluster.lease_read()
            });
        if !blocking {
            return read.await;
        }
        let mut shutdown = server.app.shutdown_tx.subscribe();
        client.flag.blocking = true;
        let result = tokio::select! {
            result = read => result,
            result = buffer_until_disconnect(client) => {
                client.closed = true;
                result.map(|()| Value::Array(None)).map_err(CacheCatError::from)
            }
            _ = shutdown.recv() => {
                client.closed = true;
                Ok(Value::Array(None))
            }
        };
        client.flag.blocking = false;
        result
    }
}

/// Preserve pipelined input for the connection loop while detecting EOF.
/// Dropping the competing read future also drops its blocking registration.
async fn buffer_until_disconnect(client: &mut Client) -> std::io::Result<()> {
    let mut buffer = [0; 4096];
    loop {
        let len = client.framed.get_mut().read(&mut buffer).await?;
        if len == 0 {
            return Ok(());
        }
        client
            .framed
            .read_buffer_mut()
            .extend_from_slice(&buffer[..len]);
    }
}

impl MultiReadCommand for XReadParams {
    fn keys(&self) -> &Vec<Bytes> {
        &self.keys
    }

    fn execute(&self, values: Vec<Option<EntrySnapshot<MyValue>>>) -> Value {
        // Redis validates every key, even if an earlier stream fills a limit.
        if values
            .iter()
            .flatten()
            .any(|snapshot| !matches!(&snapshot.value.data, ValueObject::Stream(_)))
        {
            return ProtocolError::WrongType.into();
        }
        let mut result = Vec::new();
        let mut total_entries = 0usize;
        let mut wire_size = aggregate_len(0);
        for ((key, value), id) in self.keys.iter().zip(values).zip(self.ids.iter().copied()) {
            if self.max_count.is_some_and(|max| total_entries >= max) {
                break;
            }
            if self
                .max_size
                .is_some_and(|max| total_entries > 0 && wire_size >= max)
            {
                break;
            }
            let stream = match value {
                None => continue,
                Some(snapshot) => match snapshot.value.data {
                    ValueObject::Stream(stream) => stream,
                    _ => return ProtocolError::WrongType.into(),
                },
            };
            let count = match (self.count, self.max_count) {
                (Some(count), Some(max)) => Some(count.min(max - total_entries)),
                (None, Some(max)) => Some(max - total_entries),
                (count, None) => count,
            };
            let previous_wire_size = wire_size;
            wire_size += aggregate_len(result.len() + 1) - aggregate_len(result.len());
            if self.resp_version != 3 {
                wire_size += aggregate_len(2);
            }
            wire_size += bulk_len(key.len());
            wire_size += aggregate_len(0);
            let mut stream_count = 0;
            let entries = stream.read().xread_while(id.start(), count, |id, fields| {
                // MAXSIZE is a soft limit: finish the entry that reaches it.
                // Never emit a stream key with an empty entries array merely
                // because that key's framing crosses the size limit.
                if stream_count > 0 && self.max_size.is_some_and(|max| wire_size >= max) {
                    return false;
                }
                wire_size += aggregate_len(stream_count + 1) - aggregate_len(stream_count);
                wire_size += entry_wire_size(id, fields);
                stream_count += 1;
                true
            });
            if entries.is_empty() {
                wire_size = previous_wire_size;
                continue;
            }
            total_entries += entries.len();
            let stream_entries = entries.into_iter().map(xread_entry).collect();
            result.push((
                Value::BulkString(Some(key.clone())),
                Value::Array(Some(stream_entries)),
            ));
            if self.max_count.is_some_and(|max| total_entries >= max) {
                break;
            }
            if self
                .max_size
                .is_some_and(|max| total_entries > 0 && wire_size >= max)
            {
                break;
            }
        }
        if result.is_empty() {
            return Value::Array(None);
        }
        Value::MapWithResp2 {
            entries: result,
            resp2: Resp2MapEncoding::Pairs,
        }
    }
}
