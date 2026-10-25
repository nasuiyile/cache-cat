//! Deterministic list pop used by local blocking command handlers.

use crate::error::{CacheCatError, Error, ProtocolError, StorageError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::common::{blocking::buffer_until_disconnect, float};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::application::blocking_keys::WaitError;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::{ComputedWrite, MultiReadComputeCommand};
use crate::raft::types::core::mocha::core::{MyCache, MyValue};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::request::{Operation, RedisOperation, Request};
use crate::raft::types::raft_types::CacheCatApp;
use async_trait::async_trait;
use bytes::Bytes;
use openraft::error::{ClientWriteError, RaftError};
use parking_lot::Mutex;
use rustc_apfloat::ieee::X87DoubleExtended;
use rustc_apfloat::{Float, Round, Status};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PopSide {
    Left,
    Right,
}

/// Only deterministic inputs belong in the replicated request. Timeouts,
/// connection state and waiter ordering remain on the serving leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlPopReq {
    pub keys: Vec<Bytes>,
    pub side: PopSide,
}

impl fmt::Display for BlPopReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self.side {
                PopSide::Left => "BLPOP",
                PopSide::Right => "BRPOP",
            }
        )?;
        for key in &self.keys {
            write!(f, " {}", String::from_utf8_lossy(key))?;
        }
        Ok(())
    }
}

impl MultiReadComputeCommand for BlPopReq {
    fn read_keys(&self) -> impl Iterator<Item = &Bytes> {
        self.keys.iter()
    }

    fn mutate_writes(
        self,
        read_entries: Vec<Option<EntrySnapshot<MyValue>>>,
        _write_clock: u64,
    ) -> (Vec<ComputedWrite>, Value) {
        for (key, entry) in self.keys.into_iter().zip(read_entries) {
            let Some(entry) = entry else {
                continue;
            };
            let ValueObject::List(data) = &entry.value.data else {
                return (Vec::new(), ProtocolError::WrongType.into());
            };
            // The read entries may also belong to a snapshot. Mutating their
            // shared list would invalidate both the snapshot and the write set.
            let mut list = data.lock().clone();
            let element = match self.side {
                PopSide::Left => list.pop_front(),
                PopSide::Right => list.pop_back(),
            };
            let Some(element) = element else {
                continue;
            };
            let operation = if list.is_empty() {
                MochaOperation::Remove
            } else {
                MochaOperation::Insert {
                    value: MyValue::new(ValueObject::List(Arc::new(Mutex::new(list)))),
                    expire: entry.get_expire_policy(),
                }
            };
            let response = Value::Array(Some(vec![
                Value::BulkString(Some(key.clone())),
                Value::BulkString(Some(element)),
            ]));
            // Redis stops at the first nonempty list; later keys are not
            // type-checked after a successful choice.
            return (vec![ComputedWrite { key, operation }], response);
        }
        (Vec::new(), Value::Array(None))
    }
}

/// Local parameters shared by BLPOP and future blocking list pops. Neither the
/// timeout nor the registration is serialized into the replicated request.
#[derive(Debug, Clone)]
pub struct BlPopParams {
    pub request: BlPopReq,
    pub timeout: Option<Duration>,
}

impl BlPopParams {
    pub(crate) fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("blpop"));
        }
        let invalid = || ProtocolError::response("ERR timeout is not a float or out of range");
        let argument = items
            .last()
            .unwrap()
            .string_bytes_clone()
            .ok_or_else(invalid)?;
        let seconds = float::parse(&argument).ok_or_else(invalid)?;
        if seconds.is_negative() && !seconds.is_zero() {
            return Err(ProtocolError::response("ERR timeout is negative"));
        }
        let millis = (seconds * X87DoubleExtended::from_u128(1000).value).value;
        let rounded = millis.to_i128_r(64, Round::TowardPositive, &mut false);
        if !millis.is_finite()
            || rounded.status.contains(Status::INVALID_OP)
            || rounded.value > i64::MAX as i128 - crate::utils::now_ms() as i128
        {
            return Err(ProtocolError::response("ERR timeout is out of range"));
        }
        let timeout = if seconds.is_zero() {
            None
        } else {
            let duration = Duration::from_millis(rounded.value as u64);
            Instant::now()
                .checked_add(duration)
                .ok_or_else(|| ProtocolError::response("ERR timeout is out of range"))?;
            Some(duration)
        };
        let keys = items[1..items.len() - 1]
            .iter()
            .map(|item| {
                item.string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("key"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            request: BlPopReq {
                keys,
                side: PopSide::Left,
            },
            timeout,
        })
    }

    /// Called under write_lock + read_lock. Match the replicated operation's
    /// key priority and type checks without cloning or mutating list contents.
    fn ready_key(&self, cache: &MyCache, db: u16) -> Result<Option<Bytes>, Value> {
        let database = cache.get_cache(db)?;
        let clock = cache.get_and_update_read_clock();
        for key in &self.request.keys {
            if let Some(entry) = database.mocha.get_with_read_clock(key, Some(clock)) {
                let ValueObject::List(list) = &entry.value.data else {
                    return Err(ProtocolError::WrongType.into());
                };
                if !list.lock().is_empty() {
                    return Ok(Some(key.clone()));
                }
            }
        }
        Ok(None)
    }

    async fn pop(&self, app: &CacheCatApp, db: u16) -> Result<Value, CacheCatError> {
        let cache = &app.state_machine.data.kvs;
        let deadline = self.timeout.map(|duration| Instant::now() + duration);
        loop {
            let check = async {
                app.cluster.lease_read().await.map_err(|error| {
                    if app.cluster.is_leader() {
                        error
                    } else {
                        ProtocolError::ReadOnly.into()
                    }
                })?;
                let _write = cache.write_lock.lock().await;
                let _read = cache.read_lock.read();
                let ready = match self.ready_key(cache, db) {
                    Ok(key) => key,
                    Err(error) => return Ok(Err(error)),
                };
                if ready.is_some() && cache.list_blocking_keys.is_empty() {
                    return Ok(Ok(None));
                }
                let keys = self
                    .request
                    .keys
                    .iter()
                    .map(|key| (db, key.clone()))
                    .collect();
                // Registration is atomic with observation relative to apply.
                let registration = cache
                    .list_blocking_keys
                    .register_exclusive_with(keys, deadline, self.clone())
                    .map_err(|e| Error::internal(format!("cannot register BLPOP: {e:?}")))?;
                if let Some(key) = ready {
                    // An older blocked pop owns the next turn, even if a new
                    // connection arrives while its Raft write is in flight.
                    cache.list_blocking_keys.signal_one(&(db, key));
                }
                Ok::<_, CacheCatError>(Ok(Some(registration)))
            };
            let checked = if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, check).await {
                    Ok(result) => result?,
                    Err(_) => return Ok(Value::Array(None)),
                }
            } else {
                check.await?
            };
            let mut registration = match checked {
                Ok(registration) => registration,
                Err(error) => return Ok(error),
            };
            if let Some(registration) = &mut registration {
                match registration.wait().await {
                    Ok(_) => {}
                    Err(WaitError::Timeout) => return Ok(Value::Array(None)),
                    Err(error) => {
                        return Err(Error::internal(format!("BLPOP wait failed: {error:?}")));
                    }
                }
            }
            // Never time out a submitted write: it can already be committed.
            // Only Raft may choose and remove the element on every replica.
            if !app.cluster.is_leader() {
                return Err(ProtocolError::ReadOnly.into());
            }
            let request = Request::new(
                cache.generate_new_write_clock(),
                db,
                Operation::Redis(RedisOperation::RedisBlPop(self.request.clone())),
            );
            let result = app
                .cluster
                .client_write(request)
                .await
                .map_err(|error| match error {
                    // Preserve a retryable Redis role error for Sentinel clients.
                    RaftError::APIError(ClientWriteError::ForwardToLeader(_)) => {
                        CacheCatError::from(ProtocolError::ReadOnly)
                    }
                    error => StorageError::WriteFailed(error.to_string()).into(),
                })?
                .data;
            if let Some(registration) = &mut registration {
                registration.finish();
            }
            if !matches!(result, Value::Array(None)) {
                return Ok(result);
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Ok(Value::Array(None));
            }
            // A competing ordinary pop or expiry won. Recheck/register under
            // the cache locks; there is no timer-based data polling.
        }
    }
}

pub struct BlPopCommand;

impl RaftCommand for BlPopCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Redis(RedisOperation::RedisBlPop(
            BlPopParams::parse(items)?.request,
        )))
    }
}

#[async_trait]
impl Command for BlPopCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = BlPopParams::parse(items)?;
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(Operation::Redis(RedisOperation::RedisBlPop(params.request)));
            return Ok(Value::queued());
        }
        let mut shutdown = server.app.shutdown_tx.subscribe();
        let leadership_lost = server.app.cluster.leadership_lost();
        let pop = params.pop(&server.app, client.db_number);
        client.flag.blocking = true;
        let result = tokio::select! {
            result = pop => result,
            result = buffer_until_disconnect(client) => {
                client.closed = true;
                match result {
                    Ok(()) => Err(ProtocolError::response("UNBLOCKED BLPOP connection closed").into()),
                    Err(error) => Err(error.into()),
                }
            }
            _ = shutdown.recv() => {
                client.closed = true;
                // A submitted write may still commit. Never report a timeout
                // null when shutdown leaves the write outcome unknown.
                Err(ProtocolError::response("UNBLOCKED BLPOP interrupted by node shutdown").into())
            }
            _ = leadership_lost => Err(ProtocolError::ReadOnly.into()),
        };
        client.flag.blocking = false;
        result
    }
}

#[cfg(test)]
#[path = "blpop_tests.rs"]
mod tests;
