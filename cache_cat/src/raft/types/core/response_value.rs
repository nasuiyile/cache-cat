use std::borrow::Cow;

use crate::error::ProtocolError;
use bytes::{BufMut, Bytes};
use mlua::{Lua, Value as LuaValue};
use serde::{Deserialize, Serialize};

/// A response from the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<Value>>),
    /// Key-value mapping (RESP3: %N map, RESP2: flat array *2N)
    Map(Vec<(Value, Value)>),
    /// Ordered pairs, e.g. ZRANGE WITHSCORES (RESP3: array of 2-elem arrays, RESP2: flat array *2N)
    Pairs(Vec<(Value, Value)>),
    /// Boolean (RESP3: #t/#f, RESP2: :1/:0)
    Boolean(bool),
}

impl Value {
    /// Create a simple OK response
    pub fn ok() -> Self {
        Value::SimpleString(Bytes::from_static(b"OK"))
    }

    /// Create an error response
    pub fn error(msg: impl Into<Bytes>) -> Self {
        Value::Error(msg.into())
    }

    /// Encode Value to RESP bytes
    pub fn encode(&self) -> Vec<u8> {
        self.encode_proto(2)
    }

    pub fn encode_proto(&self, proto: u8) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_to(proto, &mut buf);
        buf
    }

    pub(crate) fn encode_to(&self, proto: u8, buf: &mut impl BufMut) {
        match self {
            Value::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.put_slice(s);
                buf.put_slice(b"\r\n");
            }

            Value::Error(e) => {
                buf.put_u8(b'-');
                buf.put_slice(e);
                buf.put_slice(b"\r\n");
            }

            Value::Integer(i) => {
                buf.put_u8(b':');
                buf.put_slice(i.to_string().as_bytes());
                buf.put_slice(b"\r\n");
            }

            Value::BulkString(None) => {
                if proto == 3 {
                    buf.put_slice(b"_\r\n");
                } else {
                    buf.put_slice(b"$-1\r\n");
                }
            }

            Value::BulkString(Some(data)) => {
                buf.put_u8(b'$');
                buf.put_slice(data.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(data);
                buf.put_slice(b"\r\n");
            }

            Value::Array(None) => {
                if proto == 3 {
                    buf.put_slice(b"_\r\n");
                } else {
                    buf.put_slice(b"*-1\r\n");
                }
            }

            Value::Array(Some(items)) => {
                buf.put_u8(b'*');
                buf.put_slice(items.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                for item in items {
                    item.encode_to(proto, buf);
                }
            }

            Value::Map(pairs) => {
                if proto == 3 {
                    buf.put_u8(b'%');
                    buf.put_slice(pairs.len().to_string().as_bytes());
                    buf.put_slice(b"\r\n");
                } else {
                    buf.put_u8(b'*');
                    buf.put_slice((pairs.len() * 2).to_string().as_bytes());
                    buf.put_slice(b"\r\n");
                }
                for (k, v) in pairs {
                    k.encode_to(proto, buf);
                    v.encode_to(proto, buf);
                }
            }

            Value::Pairs(pairs) => {
                if proto == 3 {
                    buf.put_u8(b'*');
                    buf.put_slice(pairs.len().to_string().as_bytes());
                    buf.put_slice(b"\r\n");
                    for (k, v) in pairs {
                        buf.put_slice(b"*2\r\n");
                        k.encode_to(proto, buf);
                        v.encode_to(proto, buf);
                    }
                } else {
                    buf.put_u8(b'*');
                    buf.put_slice((pairs.len() * 2).to_string().as_bytes());
                    buf.put_slice(b"\r\n");
                    for (k, v) in pairs {
                        k.encode_to(proto, buf);
                        v.encode_to(proto, buf);
                    }
                }
            }

            Value::Boolean(val) => {
                if proto == 3 {
                    buf.put_slice(if *val { b"#t\r\n" } else { b"#f\r\n" });
                } else {
                    buf.put_slice(if *val { b":1\r\n" } else { b":0\r\n" });
                }
            }
        }
    }

    pub fn into_lua_value(self, lua: &Lua) -> mlua::Result<mlua::Value> {
        match self {
            Value::SimpleString(s) => {
                let table = lua.create_table()?;
                table.set("ok", s.to_vec())?;
                Ok(mlua::Value::Table(table))
            }
            Value::Error(e) => {
                let table = lua.create_table()?;
                // TODO: unsafe unwrap
                table.set("err", str::from_utf8(&e).unwrap())?;
                Ok(mlua::Value::Table(table))
            }
            Value::Integer(i) => Ok(mlua::Value::Integer(i)),
            Value::Boolean(b) => Ok(mlua::Value::Boolean(b)),
            Value::BulkString(Some(bytes)) => {
                let s = lua.create_string(&bytes)?;
                Ok(mlua::Value::String(s))
            }
            Value::BulkString(None) => Ok(mlua::Value::Boolean(false)),
            Value::Array(Some(arr)) => {
                let table = lua.create_table_with_capacity(arr.len(), 0)?;
                for (i, val) in arr.into_iter().enumerate() {
                    table.set(i + 1, val.into_lua_value(lua)?)?;
                }
                Ok(mlua::Value::Table(table))
            }
            Value::Array(None) => Ok(mlua::Value::Boolean(false)),
            Value::Map(map) => {
                let table = lua.create_table()?;

                for (k, v) in map {
                    table.set(k.into_lua_value(lua)?, v.into_lua_value(lua)?)?;
                }
                Ok(mlua::Value::Table(table))
            }
            Value::Pairs(pairs) => {
                let table = lua.create_table_with_capacity(pairs.len(), 0)?;
                for (i, (k, v)) in pairs.into_iter().enumerate() {
                    let pair = lua.create_table_with_capacity(2, 0)?;
                    pair.set(1, k.into_lua_value(lua)?)?;
                    pair.set(2, v.into_lua_value(lua)?)?;
                    table.set(i + 1, pair)?;
                }
                Ok(mlua::Value::Table(table))
            }
        }
    }

    pub fn from_lua(lua_val: LuaValue, lua: &Lua) -> Result<Value, ProtocolError> {
        match lua_val {
            LuaValue::Nil | LuaValue::Boolean(false) => Ok(Value::BulkString(None)),
            LuaValue::Boolean(true) => Ok(Value::Integer(1)),
            LuaValue::Integer(i) => Ok(Value::Integer(i)),
            LuaValue::Number(n) => {
                // 浮点数统一转为 BulkString 形式，保持与 Redis 行为一致
                Ok(Value::BulkString(Some(n.to_string().into())))
            }
            LuaValue::String(s) => {
                let bytes = Bytes::copy_from_slice(&s.as_bytes());
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
                                return Ok(Value::SimpleString(Bytes::copy_from_slice(
                                    &msg.as_bytes(),
                                )));
                            }
                        } else if key.as_bytes() == b"err" {
                            if let LuaValue::String(msg) = value {
                                return Ok(Value::Error(Bytes::copy_from_slice(&msg.as_bytes())));
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
            LuaValue::Error(err) => Ok(Value::Error(err.to_string().into())),
            other => Ok(Value::Error(
                format!("Cannot convert Lua value to Redis: {:?}", other).into(),
            )),
        }
    }

    #[inline]
    pub const fn from_static_string(str: &'static str) -> Self {
        Value::SimpleString(Bytes::from_static(str.as_bytes()))
    }

    #[inline]
    pub const fn from_bluk_static_string(str: &'static str) -> Self {
        Value::BulkString(Some(Bytes::from_static(str.as_bytes())))
    }

    #[inline]
    pub const fn string_bytes_unchecked(&self) -> Option<&Bytes> {
        match self {
            Value::BulkString(Some(data)) => Some(data),
            Value::SimpleString(s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn string_bytes(&self) -> Option<&Bytes> {
        match self {
            Value::BulkString(Some(data)) if str::from_utf8(data).is_ok() => Some(data),
            Value::SimpleString(s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn as_str_checked(&self) -> Option<&str> {
        self.string_bytes_unchecked()
            .and_then(|bytes| str::from_utf8(bytes).ok())
    }

    #[inline]
    pub fn as_str_lossy(&self) -> Option<Cow<'_, str>> {
        match self {
            Value::BulkString(Some(data)) => Some(String::from_utf8_lossy(data)),
            Value::SimpleString(s) => Some(Cow::Borrowed(str::from_utf8(s).ok()?)),
            _ => None,
        }
    }

    #[inline]
    pub fn try_as_str_lossy(&self) -> Option<Option<Cow<'_, str>>> {
        match self {
            Value::BulkString(Some(data)) => Some(Some(String::from_utf8_lossy(data))),
            Value::BulkString(None) => Some(None),
            Value::SimpleString(s) => Some(Some(Cow::Borrowed(str::from_utf8(s).ok()?))),

            _ => None,
        }
    }

    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::BulkString(Some(data)) => str::from_utf8(data).ok(),
            Value::SimpleString(s) => Some(str::from_utf8(s).ok()?),
            _ => None,
        }
    }

    #[inline]
    pub fn parse_u64(&self) -> Option<u64> {
        match self {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).parse::<u64>().ok(),

            Value::SimpleString(s) => str::from_utf8(s).ok()?.parse::<u64>().ok(),

            Value::Integer(i) if *i >= 0 => Some(*i as u64),

            _ => None,
        }
    }

    #[inline]
    pub fn try_parse_u64(&self) -> Result<u64, ProtocolError> {
        self.parse_u64().ok_or(ProtocolError::NotAnInteger)
    }

    #[inline]
    pub fn parse_usize(&self) -> Option<usize> {
        match self {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).parse::<usize>().ok(),

            Value::SimpleString(s) => str::from_utf8(s).ok()?.parse::<usize>().ok(),

            Value::Integer(i) if *i >= 0 => Some(*i as usize),

            _ => None,
        }
    }

    #[inline]
    pub fn parse_i64(&self) -> Option<i64> {
        match self {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).parse::<i64>().ok(),

            Value::SimpleString(s) => str::from_utf8(s).ok()?.parse::<i64>().ok(),

            Value::Integer(i) => Some(*i),

            _ => None,
        }
    }

    #[inline]
    pub fn try_parse_i64(&self) -> Result<i64, ProtocolError> {
        self.parse_i64().ok_or(ProtocolError::NotAnInteger)
    }
}
