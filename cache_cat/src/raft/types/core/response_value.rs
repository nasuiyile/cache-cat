use crate::error::ProtocolError;
use bytes::{BufMut, Bytes};
use mlua::{Lua, Value as LuaValue};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// A response from the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    SimpleString(String),
    Error(String),
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
        Value::SimpleString("OK".to_string())
    }

    /// Create an error response
    pub fn error(msg: impl Into<String>) -> Self {
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
                buf.put_slice(s.as_bytes());
                buf.put_slice(b"\r\n");
            }
            Value::Error(e) => {
                buf.put_u8(b'-');
                buf.put_slice(e.as_bytes());
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
                table.set("ok", s)?;
                Ok(mlua::Value::Table(table))
            }
            Value::Error(e) => {
                let table = lua.create_table()?;
                table.set("err", e)?;
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

    // TODO: param `lua` is only used in recursion
    pub fn from_lua(lua_val: LuaValue, lua: &Lua) -> Result<Value, ProtocolError> {
        match lua_val {
            LuaValue::Nil | LuaValue::Boolean(false) => Ok(Value::BulkString(None)),
            LuaValue::Boolean(true) => Ok(Value::Integer(1)),
            LuaValue::Integer(i) => Ok(Value::Integer(i)),
            LuaValue::Number(n) => {
                // Convert floating-point numbers to BulkString format uniformly,
                // maintaining consistency with Redis behavior
                Ok(Value::BulkString(Some(n.to_string().into())))
            }
            LuaValue::String(s) => Ok(Value::BulkString(Some(s.as_bytes().to_vec().into()))),
            LuaValue::Table(t) => {
                // Empty table directly returns an empty array
                let pairs: Vec<(LuaValue, LuaValue)> = t.pairs().collect::<Result<Vec<_>, _>>()?;

                if pairs.is_empty() {
                    return Ok(Value::Array(Some(Vec::new())));
                }

                // Check if it is a status reply: { ok = "..." } or { err = "..." }
                if pairs.len() == 1
                    && let (LuaValue::String(key), value) = &pairs[0]
                {
                    if key.as_bytes() == b"ok" {
                        if let LuaValue::String(msg) = value {
                            return Ok(Value::SimpleString(
                                String::from_utf8_lossy(msg.as_bytes().as_ref()).into_owned(),
                            ));
                        }
                    } else if key.as_bytes() == b"err"
                        && let LuaValue::String(msg) = value
                    {
                        return Ok(Value::Error(
                            String::from_utf8_lossy(msg.as_bytes().as_ref()).into_owned(),
                        ));
                    }
                }

                // Determine whether it is a pure array (continuous integers with keys 1..n)
                let mut is_array = true;
                let mut seen = vec![false; pairs.len()];
                for (k, _) in &pairs {
                    if let LuaValue::Integer(idx) = k
                        && *idx >= 1
                        && *idx <= pairs.len() as i64
                    {
                        seen[(*idx - 1) as usize] = true;
                        continue;
                    }
                    is_array = false;
                    break;
                }
                is_array = is_array && seen.iter().all(|&b| b);

                if is_array {
                    // Assemble arrays in index order
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
                    // Mapping Table -> Flatten Key Value Pair Array
                    let mut flat = Vec::with_capacity(pairs.len() * 2);
                    for (k, v) in pairs {
                        flat.push(Value::from_lua(k, lua)?);
                        flat.push(Value::from_lua(v, lua)?);
                    }
                    Ok(Value::Array(Some(flat)))
                }
            }
            // The following types cannot be securely mapped to Redis values, resulting in an error
            LuaValue::Error(err) => Ok(Value::Error(err.to_string())),
            other => Ok(Value::Error(format!(
                "Cannot convert Lua value to Redis: {:?}",
                other
            ))),
        }
    }

    pub(crate) fn string_bytes_clone(&self) -> Option<Bytes> {
        match self {
            Value::BulkString(Some(data)) => Some(data.clone()),
            Value::SimpleString(s) => Some(s.clone().into()),
            _ => None,
        }
    }

    #[inline]
    pub fn as_str_lossy(&self) -> Option<Cow<'_, str>> {
        match self {
            Value::BulkString(Some(data)) => Some(String::from_utf8_lossy(data)),
            Value::SimpleString(s) => Some(Cow::Borrowed(s)),
            _ => None,
        }
    }

    pub(crate) fn parse_u64(&self) -> Option<u64> {
        match self {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).parse::<u64>().ok(),

            Value::SimpleString(s) => s.parse::<u64>().ok(),

            Value::Integer(i) if *i >= 0 => Some(*i as u64),

            _ => None,
        }
    }

    #[inline]
    pub(crate) fn try_parse_u64(&self) -> Result<u64, ProtocolError> {
        self.parse_u64().ok_or(ProtocolError::NotAnInteger)
    }

    pub(crate) fn parse_i64(&self) -> Option<i64> {
        match self {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).parse::<i64>().ok(),

            Value::SimpleString(s) => s.parse::<i64>().ok(),

            Value::Integer(i) => Some(*i),

            _ => None,
        }
    }

    #[inline]
    pub(crate) fn try_parse_i64(&self) -> Result<i64, ProtocolError> {
        self.parse_i64().ok_or(ProtocolError::NotAnInteger)
    }

    pub(crate) fn parse_usize(&self) -> Option<usize> {
        match self {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).parse::<usize>().ok(),

            Value::SimpleString(s) => s.parse::<usize>().ok(),

            Value::Integer(i) if *i >= 0 => Some(*i as usize),

            _ => None,
        }
    }

    #[inline]
    pub(crate) fn try_parse_usize(&self) -> Result<usize, ProtocolError> {
        self.parse_usize().ok_or(ProtocolError::NotAnInteger)
    }

    pub(crate) fn parse_bool_u8(&self) -> Option<u8> {
        match self {
            Value::BulkString(Some(data)) => {
                if let Ok(v) = String::from_utf8_lossy(data).parse::<u8>()
                    && v <= 1
                {
                    Some(v)
                } else {
                    None
                }
            }

            Value::SimpleString(s) => {
                if let Ok(v) = s.parse::<u8>()
                    && v <= 1
                {
                    Some(v)
                } else {
                    None
                }
            }

            Value::Integer(i) if matches!(*i, 0..=1) => Some(*i as u8),

            _ => None,
        }
    }
}
