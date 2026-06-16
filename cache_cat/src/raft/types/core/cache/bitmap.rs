use crate::error::ProtocolError;
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::bitmap::getbit::GetBitParams;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::mocha::{MyCache, MyValue, Update};
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{BaseOperation, SetBitReq};
use bytes::Bytes;
use std::sync::Arc;

impl ComputeCommand for SetBitReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::SetBit(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        // 获取字符串表示的字节数组
        let bytes = match &entry.value.data {
            ValueObject::String(data_arc) => (**data_arc).clone(),
            ValueObject::Int(int_value) => {
                // 整数转换为字符串表示
                int_value.to_string().into_bytes()
            }
            _ => {
                return (
                    MochaOperation::Abort,
                    Value::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ),
                );
            }
        };

        let mut bytes = bytes;
        let offset = self.offset;
        let bit_value = self.value & 1; // Ensure only 0 or 1

        // Calculate byte and bit positions
        let byte_index = (offset / 8) as usize;
        let bit_position = 7 - (offset % 8); // Most significant bit first (Redis behavior)

        // Expand the byte array if needed
        if byte_index >= bytes.len() {
            bytes.resize(byte_index + 1, 0);
        }

        // Get the old bit value
        let old_byte = bytes[byte_index];
        let old_bit = (old_byte >> bit_position) & 1;

        // Set the new bit
        if bit_value == 1 {
            bytes[byte_index] = old_byte | (1 << bit_position);
        } else {
            bytes[byte_index] = old_byte & !(1 << bit_position);
        }

        let new_value = MyValue::new(ValueObject::String(Arc::new(bytes)));
        (
            MochaOperation::Insert {
                value: new_value,
                expire: entry.get_expire_policy(),
            },
            Value::Integer(old_bit as i64),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let offset = self.offset;
        let bit_value = self.value & 1;

        // Calculate byte and bit positions
        let byte_index = (offset / 8) as usize;
        let bit_position = 7 - (offset % 8);

        // Create byte array of appropriate size
        let mut bytes = vec![0u8; byte_index + 1];

        // Set the bit
        if bit_value == 1 {
            bytes[byte_index] = 1 << bit_position;
        } else {
            bytes[byte_index] = 0;
        }

        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::String(Arc::new(bytes))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(0), // Original bit value is 0 for new key
        )
    }
}

impl ReadCommand for GetBitParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        let bytes: Vec<u8> = match value {
            None => return Value::Integer(0),
            Some(value) => match value.data {
                ValueObject::String(s) => s.to_vec(),
                ValueObject::Int(i) => i.to_string().into_bytes(),
                _ => return ProtocolError::WrongType.into(),
            },
        };
        let offset = self.offset; // u64
        let byte_index = (offset / 8) as usize;
        let bit_offset = (offset % 8) as usize;
        let bit = if byte_index >= bytes.len() {
            0
        } else {
            let byte = bytes[byte_index];
            ((byte >> (7 - bit_offset)) & 1) as i64
        };
        Value::Integer(bit)
    }
}

impl MyCache {
    pub fn get_bit(&self, param: GetBitParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(param, db_number, read_clock)
    }
    pub fn set_bit(&self, param: SetBitReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
