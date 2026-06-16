use crate::raft::types::core::mocha::mocha::{MyCache, MyValue};
use crate::raft::types::core::response_value::Value;
use bytes::Bytes;

pub trait ReadCommand: Send + 'static {
    fn key(&self) -> &Bytes;

    fn execute(&self, value: Option<MyValue>) -> Value;
}

impl MyCache {
    pub fn execute_read<C: ReadCommand>(
        &self,
        cmd: C,
        db_number: u16,
        read_clock: Option<u64>,
    ) -> Value {
        let cache = match self.databases.get(db_number as usize) {
            None => return Value::error("Key not found"),
            Some(v) => &v.mocha,
        };
        let key = cmd.key();
        let option = cache.get_with_read_clock(key, read_clock);
        cmd.execute(option)
    }
}

pub trait MultiReadCommand: Send + 'static {
    fn keys(&self) -> &Vec<Bytes>;

    fn execute(&self, values: Vec<Option<MyValue>>) -> Value;
}

impl MyCache {
    pub fn execute_multi_read<C: MultiReadCommand>(
        &self,
        cmd: C,
        db_number: u16,
        read_clock: Option<u64>,
    ) -> Value {
        let cache = match self.databases.get(db_number as usize) {
            None => return Value::error("Key not found"),
            Some(v) => &v.mocha,
        };
        let keys = cmd.keys();
        let mut vec = Vec::new();
        for key in keys {
            vec.push(cache.get_with_read_clock(key, read_clock));
        }
        cmd.execute(vec)
    }
}
