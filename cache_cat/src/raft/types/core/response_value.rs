use mlua::{Lua, Value as LuaValue};
use serde::{Deserialize, Serialize};
use crate::error::ProtocolError;
use crate::protocol::lua_env::LuaEnv;

/// A response from the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    /// Simple strings, used for simple responses like "OK"
    SimpleString(String),
    /// Errors
    Error(String),
    /// Integers
    Integer(i64),
    /// Bulk strings, used for binary-safe strings (can be null)
    BulkString(Option<Vec<u8>>),
    /// Arrays of other values (can be null)
    Array(Option<Vec<Value>>),
}

impl Value {
    /// Create a simple OK response
    pub fn ok() -> Self {
        Value::SimpleString("OK".to_string())
    }

    /// Create an error response
    pub fn error(msg: impl Into<String>) -> Self {
        Value::Error(msg.into())
    }

    /// Encode Value to RESP bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_to(&mut buf);
        buf
    }

    fn encode_to(&self, buf: &mut Vec<u8>) {
        match self {
            Value::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::Error(e) => {
                buf.push(b'-');
                buf.extend_from_slice(e.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::Integer(i) => {
                buf.push(b':');
                buf.extend_from_slice(i.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            Value::BulkString(Some(data)) => {
                buf.push(b'$');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            Value::Array(None) => {
                buf.extend_from_slice(b"*-1\r\n");
            }
            Value::Array(Some(items)) => {
                buf.push(b'*');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode_to(buf);
                }
            }
        }
    }
    pub fn into_lua_value(self, lua: &Lua) -> mlua::Result<mlua::Value> {
        match self {
            Value::SimpleString(s) => {
                let table = lua.create_table()?;
                table.set("ok", s)?;
                Ok(mlua::Value::Table(table))
            }
            Value::Error(e) => {
                let table = lua.create_table()?;
                table.set("err", e)?;
                Ok(mlua::Value::Table(table))
            }
            Value::Integer(i) => Ok(mlua::Value::Integer(i)),
            Value::BulkString(Some(bytes)) => {
                let s = lua.create_string(&bytes)?;
                Ok(mlua::Value::String(s))
            }
            Value::BulkString(None) => Ok(mlua::Value::Boolean(false)),
            Value::Array(Some(arr)) => {
                // create_table_with_capacity(数组元素个数, 哈希元素个数)
                let table = lua.create_table_with_capacity(arr.len(), 0)?;
                for (i, val) in arr.into_iter().enumerate() {
                    table.set(i + 1, val.into_lua_value(lua)?)?;
                }
                Ok(mlua::Value::Table(table))
            }
            Value::Array(None) => Ok(mlua::Value::Boolean(false)),
        }
    }
    pub fn from_lua(lua_val: LuaValue, lua: &Lua) ->  Result<Value, ProtocolError>{
        match lua_val {
            LuaValue::Nil | LuaValue::Boolean(false) => Ok(Value::BulkString(None)),
            LuaValue::Boolean(true) => Ok(Value::Integer(1)),
            LuaValue::Integer(i) => Ok(Value::Integer(i)),
            LuaValue::Number(n) => {
                // 浮点数统一转为 BulkString 形式，保持与 Redis 行为一致
                Ok(Value::BulkString(Some(n.to_string().into_bytes())))
            }
            LuaValue::String(s) => {
                let bytes = s.as_bytes().to_vec();
                Ok(Value::BulkString(Some(bytes)))
            }
            LuaValue::Table(t) => {
                // 空表直接返回空数组
                let pairs: Vec<(LuaValue, LuaValue)> = t.pairs().collect::<Result<Vec<_>, _>>()?;

                if pairs.is_empty() {
                    return Ok(Value::Array(Some(Vec::new())));
                }

                // 检查是否为状态回复：{ ok = "..." } 或 { err = "..." }
                if pairs.len() == 1 {
                    if let (LuaValue::String(key), value) = &pairs[0] {
                        if key.as_bytes() == b"ok" {
                            if let LuaValue::String(msg) = value {
                                return Ok(Value::SimpleString(
                                    String::from_utf8_lossy(msg.as_bytes().as_ref().into())
                                        .into_owned(),
                                ));
                            }
                        } else if key.as_bytes() == b"err" {
                            if let LuaValue::String(msg) = value {
                                return Ok(Value::Error(
                                    String::from_utf8_lossy(msg.as_bytes().as_ref().into())
                                        .into_owned(),
                                ));
                            }
                        }
                    }
                }

                // 判断是否为纯数组（键为 1..n 的连续整数）
                let mut is_array = true;
                let mut seen = vec![false; pairs.len()];
                for (k, _) in &pairs {
                    if let LuaValue::Integer(idx) = k {
                        if *idx >= 1 && *idx <= pairs.len() as i64 {
                            seen[(*idx - 1) as usize] = true;
                            continue;
                        }
                    }
                    is_array = false;
                    break;
                }
                is_array = is_array && seen.iter().all(|&b| b);

                if is_array {
                    // 按索引顺序组装数组
                    let mut values = vec![LuaValue::Nil; pairs.len()];
                    for (k, v) in pairs {
                        if let LuaValue::Integer(idx) = k {
                            values[(idx - 1) as usize] = v;
                        }
                    }
                    let mut redis_arr = Vec::with_capacity(values.len());
                    for v in values {
                        redis_arr.push(Value::from_lua(v, lua)?);
                    }
                    Ok(Value::Array(Some(redis_arr)))
                } else {
                    // 映射表 -> 扁平化键值对数组
                    let mut flat = Vec::with_capacity(pairs.len() * 2);
                    for (k, v) in pairs {
                        flat.push(Value::from_lua(k, lua)?);
                        flat.push(Value::from_lua(v, lua)?);
                    }
                    Ok(Value::Array(Some(flat)))
                }
            }
            // 以下类型无法安全映射为 Redis 值，返回错误
            LuaValue::Error(err) => Ok(Value::Error(err.to_string())),
            other => Ok(Value::Error(format!(
                "Cannot convert Lua value to Redis: {:?}",
                other
            ))),
        }
    }
}
