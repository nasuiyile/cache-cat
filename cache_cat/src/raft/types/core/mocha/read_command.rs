use crate::mocha::EntrySnapshot;
use crate::raft::types::core::mocha::core::{MyCache, MyValue};
use crate::raft::types::core::response_value::Value;
use bytes::Bytes;

pub trait ReadCommand: Send + 'static {
    fn key(&self) -> &Bytes;

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value;

    /// Execute a read with the logical clock observed for this read.
    ///
    /// Most reads only need the value and keep the original implementation.
    /// Time-sensitive reads use this clock for TTL replies. Random or unordered
    /// reads override it to produce the same result on every Raft replica.
    fn execute_with_clock(&self, value: Option<EntrySnapshot<MyValue>>, _read_clock: u64) -> Value {
        self.execute(value)
    }
}

impl MyCache {
    pub fn execute_read<C: ReadCommand>(&self, cmd: C, db_number: u16, read_clock: u64) -> Value {
        self.execute_read_with_mode::<C, true>(cmd, db_number, read_clock)
    }

    /// Ordinary reads can retain their unordered/random fast path; reads whose
    /// results can affect replicated writes must use execute_with_clock.
    pub(crate) fn execute_read_with_mode<C: ReadCommand, const DETERMINISTIC: bool>(
        &self,
        cmd: C,
        db_number: u16,
        read_clock: u64,
    ) -> Value {
        let cache = match self.databases.get(db_number as usize) {
            None => return Value::error("Key not found"),
            Some(v) => &v.mocha,
        };
        let key = cmd.key();
        let option = cache.get_with_read_clock(key, Some(read_clock));
        if DETERMINISTIC {
            cmd.execute_with_clock(option, read_clock)
        } else {
            cmd.execute(option)
        }
    }
}

pub trait MultiReadCommand: Send + 'static {
    fn keys(&self) -> &Vec<Bytes>;

    fn execute(&self, values: Vec<Option<EntrySnapshot<MyValue>>>) -> Value;

    /// Use a replicated clock and canonical ordering when replaying reads.
    fn execute_with_clock(
        &self,
        values: Vec<Option<EntrySnapshot<MyValue>>>,
        _read_clock: u64,
    ) -> Value {
        self.execute(values)
    }
}

impl MyCache {
    pub fn execute_multi_read<C: MultiReadCommand>(
        &self,
        cmd: C,
        db_number: u16,
        read_clock: u64,
    ) -> Value {
        self.execute_multi_read_with_mode::<C, true>(cmd, db_number, read_clock)
    }

    pub(crate) fn execute_multi_read_with_mode<C: MultiReadCommand, const DETERMINISTIC: bool>(
        &self,
        cmd: C,
        db_number: u16,
        read_clock: u64,
    ) -> Value {
        let cache = match self.databases.get(db_number as usize) {
            None => return Value::error("Key not found"),
            Some(v) => &v.mocha,
        };
        let keys = cmd.keys();
        let mut vec = Vec::new();
        for key in keys {
            vec.push(cache.get_with_read_clock(key, Some(read_clock)));
        }
        if DETERMINISTIC {
            cmd.execute_with_clock(vec, read_clock)
        } else {
            cmd.execute(vec)
        }
    }
}
