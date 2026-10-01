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
    /// Commands whose response contains time (for example TTL/PTTL) override
    /// this method so a replayed Raft operation uses the replicated clock
    /// instead of sampling the local wall clock.
    fn execute_with_clock(&self, value: Option<EntrySnapshot<MyValue>>, _read_clock: u64) -> Value {
        self.execute(value)
    }
}

impl MyCache {
    pub fn execute_read<C: ReadCommand>(&self, cmd: C, db_number: u16, read_clock: u64) -> Value {
        let cache = match self.databases.get(db_number as usize) {
            None => return Value::error("Key not found"),
            Some(v) => &v.mocha,
        };
        let key = cmd.key();
        let option = cache.get_with_read_clock(key, Some(read_clock));
        cmd.execute_with_clock(option, read_clock)
    }
}

pub trait MultiReadCommand: Send + 'static {
    fn keys(&self) -> &Vec<Bytes>;

    fn execute(&self, values: Vec<Option<EntrySnapshot<MyValue>>>) -> Value;
}

impl MyCache {
    pub fn execute_multi_read<C: MultiReadCommand>(
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
        cmd.execute(vec)
    }
}
