//! Consumer-group reads mutate replicated metadata; only waiting is local.
use super::arg;
use crate::error::{CacheCatError, Error, ProtocolError, StorageError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::common::blocking::buffer_until_disconnect;
use crate::protocol::raft_command::RaftCommand;
use crate::raft::application::blocking_keys::WaitError;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::{MyCache, MyValue, Update};
use crate::raft::types::core::response_value::{Resp2MapEncoding, Value};
use crate::raft::types::core::structure::stream::{
    GroupEntry, GroupGeneration, GroupRead, ReadGroupOptions,
};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::{Operation, RedisOperation, Request};
use crate::raft::types::raft_types::CacheCatApp;
use async_trait::async_trait;
use bytes::Bytes;
use openraft::error::{ClientWriteError, RaftError};
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc, time::Duration};
use tokio::time::Instant;

/// One replicated request. Blocking deadlines and local group identities are
/// deliberately absent, and cannot influence replica state-machine execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XReadGroupReq {
    pub keys: Vec<Bytes>,
    pub group: Bytes,
    pub consumer: Bytes,
    pub modes: Vec<GroupRead>,
    pub count: Option<usize>,
    pub no_ack: bool,
    /// Replicated group identities observed before a local wait. Empty for
    /// EXEC/Lua; populated by the network handler to reject replaced groups.
    pub expected_generations: Vec<Option<GroupGeneration>>,
}

impl fmt::Display for XReadGroupReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XREADGROUP GROUP {} {} STREAMS {:?} {:?}",
            String::from_utf8_lossy(&self.group),
            String::from_utf8_lossy(&self.consumer),
            self.keys,
            self.modes
        )
    }
}

/// Each stream is an independent single-key read/write. The outer operation
/// validates every input before applying these operations in argument order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XReadGroupStreamReq {
    pub key: Bytes,
    pub group: Bytes,
    pub consumer: Bytes,
    pub mode: GroupRead,
    pub count: Option<usize>,
    pub no_ack: bool,
}

impl fmt::Display for XReadGroupStreamReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XREADGROUP {} {} {:?}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.group),
            self.mode
        )
    }
}

fn no_group(key: &Bytes, group: &Bytes) -> Value {
    ProtocolError::response(format!(
        "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group)
    ))
    .into()
}

fn encode_entry(entry: GroupEntry) -> Value {
    let fields = entry.fields.map(|fields| {
        fields
            .into_iter()
            .flat_map(|(key, value)| {
                [
                    Value::BulkString(Some(key.into())),
                    Value::BulkString(Some(value.into())),
                ]
            })
            .collect()
    });
    Value::Array(Some(vec![
        Value::BulkString(Some(entry.id.to_string().into())),
        Value::Array(fields),
    ]))
}

impl ComputeCommand for XReadGroupStreamReq {
    fn key(&self) -> &Bytes {
        &self.key
    }
    fn into_base_op(self) -> BaseOperation {
        BaseOperation::XReadGroup(self)
    }
    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (MochaOperation::Abort, no_group(&self.key, &self.group))
    }
    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expire = entry.get_expire_policy();
        let ValueObject::Stream(stream) = entry.value.data else {
            return (MochaOperation::Abort, ProtocolError::WrongType.into());
        };
        let result = stream.write().xreadgroup_at(
            &self.group,
            &self.consumer,
            self.mode,
            ReadGroupOptions {
                count: self.count,
                no_ack: self.no_ack,
            },
            write_clock,
        );
        match result {
            Ok(entries) => (
                // Even an empty read creates/touches the consumer. execute_compute
                // performs COW only while snapshotting and preserves the key TTL.
                MochaOperation::Insert {
                    value: MyValue::new(ValueObject::Stream(stream)),
                    expire,
                },
                Value::Array(Some(entries.into_iter().map(encode_entry).collect())),
            ),
            Err(_) => (MochaOperation::Abort, no_group(&self.key, &self.group)),
        }
    }
}

impl MyCache {
    pub(crate) fn redis_xreadgroup(
        &self,
        request: XReadGroupReq,
        update: &mut Update,
        external: bool,
    ) -> Value {
        let _read = external.then(|| self.read_lock.write());
        let database = match self.get_cache(update.db_number) {
            Ok(database) => database,
            Err(error) => return error,
        };
        // Validate all streams before changing any group/consumer or PEL.
        for (index, key) in request.keys.iter().enumerate() {
            let Some(entry) = database.mocha.get_entry(key) else {
                return no_group(key, &request.group);
            };
            let ValueObject::Stream(stream) = entry.value.data else {
                return ProtocolError::WrongType.into();
            };
            let stream = stream.read();
            let generation = match stream.group_generation(&request.group) {
                Ok(generation) => generation,
                Err(_) => return no_group(key, &request.group),
            };
            if request
                .expected_generations
                .get(index)
                .is_some_and(|expected| *expected != generation)
            {
                return no_group(key, &request.group);
            }
        }
        let mut result = Vec::new();
        for (key, mode) in request.keys.into_iter().zip(request.modes) {
            let reply = self.execute_compute(
                XReadGroupStreamReq {
                    key: key.clone(),
                    group: request.group.clone(),
                    consumer: request.consumer.clone(),
                    mode,
                    count: request.count,
                    no_ack: request.no_ack,
                },
                update,
            );
            match reply {
                Value::Array(Some(entries)) => {
                    // An explicit ID always returns a history array, even empty,
                    // and makes mixed history/new requests nonblocking too.
                    if !entries.is_empty() || matches!(mode, GroupRead::PendingAfter(_)) {
                        result.push((Value::BulkString(Some(key)), Value::Array(Some(entries))));
                    }
                }
                error => return error,
            }
        }
        if result.is_empty() {
            Value::Array(None)
        } else {
            Value::MapWithResp2 {
                entries: result,
                resp2: Resp2MapEncoding::Pairs,
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct XReadGroupParams {
    pub request: XReadGroupReq,
    pub block_ms: Option<u64>,
    /// Local tokens detect deletion/recreation while this request is waiting.
    pub identities: Vec<Arc<()>>,
    /// A local scheduling turn for the mandatory first consumer update.
    pub initial_attempt: bool,
    pub expirations: Vec<Option<u64>>,
}

impl XReadGroupParams {
    pub fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 7 {
            return Err(ProtocolError::WrongArgCount("xreadgroup"));
        }
        let mut group = None;
        let mut consumer = None;
        let mut count = None;
        let mut no_ack = false;
        let mut block_ms = None;
        let mut index = 1;
        let streams = loop {
            let option = items.get(index).ok_or(ProtocolError::SyntaxError)?;
            let option = arg(option)?;
            if option.eq_ignore_ascii_case(b"STREAMS") {
                break &items[index + 1..];
            } else if option.eq_ignore_ascii_case(b"GROUP") {
                group = Some(arg(items
                    .get(index + 1)
                    .ok_or(ProtocolError::SyntaxError)?)?);
                consumer = Some(arg(items
                    .get(index + 2)
                    .ok_or(ProtocolError::SyntaxError)?)?);
                index += 3;
            } else if option.eq_ignore_ascii_case(b"COUNT") {
                let value = items
                    .get(index + 1)
                    .ok_or(ProtocolError::SyntaxError)?
                    .try_parse_canonical_i64()?;
                count = if value > 0 {
                    Some(usize::try_from(value).map_err(|_| ProtocolError::NotAnInteger)?)
                } else {
                    None
                };
                index += 2;
            } else if option.eq_ignore_ascii_case(b"BLOCK") {
                let ms = items
                    .get(index + 1)
                    .ok_or(ProtocolError::SyntaxError)?
                    .try_parse_canonical_i64()
                    .map_err(|_| {
                        ProtocolError::response("ERR timeout is not an integer or out of range")
                    })?;
                if ms < 0 {
                    return Err(ProtocolError::response("ERR timeout is negative"));
                }
                if ms as u64 > (i64::MAX as u64).saturating_sub(crate::utils::now_ms()) {
                    return Err(ProtocolError::response("ERR timeout is out of range"));
                }
                Instant::now()
                    .checked_add(Duration::from_millis(ms as u64))
                    .ok_or_else(|| ProtocolError::response("ERR timeout is out of range"))?;
                block_ms = Some(ms as u64);
                index += 2;
            } else if option.eq_ignore_ascii_case(b"NOACK") {
                no_ack = true;
                index += 1;
            } else {
                return Err(ProtocolError::SyntaxError);
            }
        };
        let group = group
            .ok_or_else(|| ProtocolError::response("ERR Missing GROUP option for XREADGROUP"))?;
        if streams.is_empty() || !streams.len().is_multiple_of(2) {
            return Err(ProtocolError::response(
                "ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.",
            ));
        }
        let length = streams.len() / 2;
        let keys = streams[..length]
            .iter()
            .map(arg)
            .collect::<Result<Vec<_>, _>>()?;
        let modes = streams[length..].iter().map(|value| {
            let id = arg(value)?;
            if id.as_ref() == b">" { return Ok(GroupRead::New); }
            if id.as_ref() == b"+" {
                    return Err(ProtocolError::response("ERR The + ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The + ID would just return an empty result set."));
                }
            if id.as_ref() == b"$" {
                return Err(ProtocolError::response("ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set."));
            }
            std::str::from_utf8(&id).ok().and_then(|id| id.parse().ok())
                .map(GroupRead::PendingAfter).ok_or_else(|| ProtocolError::response(
                    "ERR Invalid stream ID specified as stream command argument"))
        }).collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            request: XReadGroupReq {
                keys,
                group,
                consumer: consumer.unwrap(),
                modes,
                count,
                no_ack,
                expected_generations: Vec::new(),
            },
            block_ms,
            identities: Vec::new(),
            initial_attempt: false,
            expirations: Vec::new(),
        })
    }

    /// Pure precheck. No consumer timestamps/cursors/PEL are changed here.
    /// Returns readiness and the earliest physical expiration for an event timer.
    fn inspect(&self, cache: &MyCache, db: u16, clock: u64) -> Result<(bool, Option<u64>), Value> {
        let database = cache.get_cache(db)?;
        let mut ready = false;
        let mut expiry = None;
        for (index, key) in self.request.keys.iter().enumerate() {
            let entry = database
                .mocha
                .get_with_read_clock(key, Some(clock))
                .ok_or_else(|| no_group(key, &self.request.group))?;
            let ValueObject::Stream(stream) = entry.value.data else {
                return Err(ProtocolError::WrongType.into());
            };
            let stream = stream.read();
            if let Some(expected) = self.identities.get(index) {
                let identity = stream
                    .group_identity(&self.request.group)
                    .map_err(|_| no_group(key, &self.request.group))?;
                if !Arc::ptr_eq(expected, &identity) {
                    return Err(no_group(key, &self.request.group));
                }
            }
            ready |= stream
                .group_has_new(&self.request.group)
                .map_err(|_| no_group(key, &self.request.group))?;
            if let Some(at) = entry.expire_at {
                expiry = Some(expiry.map_or(at, |old: u64| old.min(at)));
            }
        }
        Ok((ready, expiry))
    }

    pub(crate) fn ready(&self, cache: &MyCache, db: u16, clock: u64) -> bool {
        if self.initial_attempt
            || self
                .inspect(cache, db, clock)
                .map_or(true, |(ready, _)| ready)
        {
            return true;
        }
        // A TTL change refreshes this waiter's event timer, without submitting
        // another read when no entries are available.
        let Ok(database) = cache.get_cache(db) else {
            return true;
        };
        self.request
            .keys
            .iter()
            .zip(&self.expirations)
            .any(|(key, previous)| {
                database
                    .mocha
                    .get_with_read_clock(key, Some(clock))
                    .is_none_or(|entry| entry.expire_at != *previous)
            })
    }

    async fn submit(&self, app: &CacheCatApp, db: u16) -> Result<Value, CacheCatError> {
        let request = Request::new(
            app.state_machine.data.kvs.generate_new_write_clock(),
            db,
            Operation::Redis(RedisOperation::RedisXReadGroup(self.request.clone())),
        );
        app.cluster
            .client_write(request)
            .await
            .map(|response| response.data)
            .map_err(|error| match error {
                RaftError::APIError(ClientWriteError::ForwardToLeader(_)) => {
                    ProtocolError::ReadOnly.into()
                }
                error => StorageError::WriteFailed(error.to_string()).into(),
            })
    }

    async fn read(mut self, app: &CacheCatApp, db: u16) -> Result<Value, CacheCatError> {
        let cache = &app.state_machine.data.kvs;
        let deadline = self
            .block_ms
            .filter(|ms| *ms != 0)
            .map(|ms| Instant::now() + Duration::from_millis(ms));
        app.cluster.lease_read().await.map_err(|error| {
            if app.cluster.is_leader() {
                error
            } else {
                ProtocolError::ReadOnly.into()
            }
        })?;
        let mut initial_registration = {
            let mut write = cache.write_lock.lock().await;
            let _read = cache.read_lock.read();
            let clock = cache.get_and_update_read_clock();
            if let Err(error) = self.inspect(cache, db, clock) {
                return Ok(error);
            }
            // Capture group identity before registration to detect recreation.
            let database = cache.get_cache(db).map_err(|_| ProtocolError::DbNotExist)?;
            for key in &self.request.keys {
                let entry = database
                    .mocha
                    .get_with_read_clock(key, Some(clock))
                    .unwrap();
                let ValueObject::Stream(stream) = entry.value.data else {
                    unreachable!()
                };
                let stream = stream.read();
                self.identities
                    .push(stream.group_identity(&self.request.group).unwrap());
                self.request
                    .expected_generations
                    .push(stream.group_generation(&self.request.group).unwrap());
            }
            if write.group_blocking_keys.is_empty() {
                None
            } else {
                // New requests cannot overtake an older eligible blocked
                // consumer. An initial turn is eligible even on an empty group
                // because it must still replicate the consumer timestamp.
                let mut initial = self.clone();
                initial.initial_attempt = true;
                let keys: Vec<_> = self
                    .request
                    .keys
                    .iter()
                    .map(|key| (db, key.clone()))
                    .collect();
                let registration = write
                    .register_stream_group(keys.clone(), None, initial)
                    .map_err(|error| {
                        Error::internal(format!("cannot register XREADGROUP: {error:?}"))
                    })?;
                write
                    .group_blocking_keys
                    .signal_ready_many(keys, |key, params| params.ready(cache, key.0, clock));
                Some(registration)
            }
        };
        if let Some(registration) = &mut initial_registration {
            registration
                .wait()
                .await
                .map_err(|error| Error::internal(format!("XREADGROUP wait failed: {error:?}")))?;
        }
        // Unlike BLPOP, even an empty initial read updates consumer metadata.
        // This is one Raft attempt, never a polling loop while data is absent.
        let result = self.submit(app, db).await?;
        if let Some(registration) = &mut initial_registration {
            registration.finish().await;
        }
        if !matches!(result, Value::Array(None)) || self.block_ms.is_none() {
            return Ok(result);
        }
        loop {
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Ok(Value::Array(None));
            }
            let check = async {
                let mut write = cache.write_lock.lock().await;
                let _read = cache.read_lock.read();
                let clock = cache.get_and_update_read_clock();
                let (_, expiry) = match self.inspect(cache, db, clock) {
                    Ok(result) => result,
                    Err(error) => return Ok(Err(error)),
                };
                let database = cache.get_cache(db).map_err(|_| ProtocolError::DbNotExist)?;
                self.expirations = self
                    .request
                    .keys
                    .iter()
                    .map(|key| {
                        database
                            .mocha
                            .get_with_read_clock(key, Some(clock))
                            .and_then(|entry| entry.expire_at)
                    })
                    .collect();
                let keys: Vec<_> = self
                    .request
                    .keys
                    .iter()
                    .map(|key| (db, key.clone()))
                    .collect();
                let registration = write
                    .register_stream_group(keys.clone(), deadline, self.clone())
                    .map_err(|error| {
                        Error::internal(format!("cannot register XREADGROUP: {error:?}"))
                    })?;
                // Register and publish the current readiness under one guard.
                write
                    .group_blocking_keys
                    .signal_ready_many(keys, |key, params| params.ready(cache, key.0, clock));
                Ok::<_, CacheCatError>(Ok((registration, expiry)))
            };
            let checked = if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, check).await {
                    Ok(result) => result?,
                    Err(_) => return Ok(Value::Array(None)),
                }
            } else {
                check.await?
            };
            let (mut registration, expiry) = match checked {
                Ok(waiting) => waiting,
                Err(error) => return Ok(error),
            };
            let expired_key = async {
                match expiry {
                    Some(at) => {
                        tokio::time::sleep(Duration::from_millis(
                            at.saturating_sub(crate::utils::now_ms()),
                        ))
                        .await
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            let signal = tokio::select! {
                result = registration.wait() => Some(result),
                _ = expired_key => None,
            };
            match signal {
                None => {
                    registration.cancel().await;
                    continue;
                }
                Some(Err(WaitError::Timeout)) => return Ok(Value::Array(None)),
                Some(Err(error)) => {
                    return Err(Error::internal(format!(
                        "XREADGROUP wait failed: {error:?}"
                    )));
                }
                Some(Ok(_)) => {}
            }
            // A destroy/recreate notification must not read a new group's data.
            let available = {
                let _write = cache.write_lock.lock().await;
                let _read = cache.read_lock.read();
                match self.inspect(cache, db, cache.get_and_update_read_clock()) {
                    Err(error) => return Ok(error),
                    Ok((ready, _)) => ready,
                }
            };
            if !available {
                registration.finish().await;
                continue;
            }
            // Do not attach the blocking timeout to a submitted mutation.
            let result = self.submit(app, db).await?;
            registration.finish().await;
            if !matches!(result, Value::Array(None)) {
                return Ok(result);
            }
        }
    }
}

pub struct XReadGroupCommand;
impl RaftCommand for XReadGroupCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Redis(RedisOperation::RedisXReadGroup(
            XReadGroupParams::parse(items)?.request,
        )))
    }
}

#[async_trait]
impl Command for XReadGroupCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = XReadGroupParams::parse(items)?;
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(Operation::Redis(RedisOperation::RedisXReadGroup(
                params.request,
            )));
            return Ok(Value::queued());
        }
        let leadership_lost = server.app.cluster.leadership_lost();
        let mut shutdown = server.app.shutdown_tx.subscribe();
        let read = params.read(&server.app, client.db_number);
        client.flag.blocking = true;
        let result = tokio::select! {
            result = read => result,
            result = buffer_until_disconnect(client) => {
                client.closed = true;
                match result {
                    Ok(()) => Err(ProtocolError::response("UNBLOCKED XREADGROUP connection closed").into()),
                    Err(error) => Err(error.into()),
                }
            }
            _ = shutdown.recv() => {
                client.closed = true;
                Err(ProtocolError::response("UNBLOCKED XREADGROUP interrupted by node shutdown").into())
            }
            _ = leadership_lost => Err(ProtocolError::ReadOnly.into()),
        };
        client.flag.blocking = false;
        result
    }
}

#[cfg(test)]
#[path = "xreadgroup_tests.rs"]
mod tests;
