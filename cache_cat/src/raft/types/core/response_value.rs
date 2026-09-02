use crate::error::ProtocolError;
use bytes::{BufMut, Bytes};
use mlua::{Lua, Value as LuaValue};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Resp2MapEncoding {
    Flat,
    Pairs,
    Values,
}

/// A response from the KV store.
///
/// The enum models the *semantic* reply type (RESP3-style). Encoding
/// downgrades RESP3-only types to their RESP2 equivalents exactly the
/// way Redis does (see `addReplyBool` / `addReplyDouble` / `addReplyMapLen`
/// and friends in Redis `networking.c`):
///
/// | Variant          | RESP3 wire        | RESP2 downgrade            |
/// |------------------|-------------------|----------------------------|
/// | Null             | `_`               | `$-1`                      |
/// | Boolean          | `#t` / `#f`       | `:1` / `:0`                |
/// | Double           | `,<dbl>`          | `$<len>` bulk string       |
/// | BigNumber        | `(<num>`          | `$<len>` bulk string       |
/// | VerbatimString   | `=<len>`          | `$<len>` bulk string       |
/// | BulkError        | `!<len>`          | `-<msg>` simple error      |
/// | Map              | `%<n>`            | `*<2n>` flat array         |
/// | Set              | `~<n>`            | `*<n>` array               |
/// | Push             | `><n>`            | `*<n>` array               |
/// | MemberScores     | `*<n>` of pairs   | `*<2n>` flat array         |
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<Value>>),
    /// Null (RESP3: `_\r\n`, RESP2: `$-1\r\n`)
    Null,
    /// Key-value mapping (RESP3: %N map, RESP2: flat array *2N)
    Map(Vec<(Value, Value)>),
    /// Unordered set (RESP3: ~N, RESP2: *N array)
    Set(Vec<Value>),
    /// Out-of-band push message, used by pub/sub (RESP3: >N, RESP2: *N array)
    Push(Vec<Value>),
    /// Boolean (RESP3: #t/#f, RESP2: :1/:0)
    Boolean(bool),
    /// Double precision float (RESP3: ,<dbl>, RESP2: bulk string)
    Double(f64),
    /// Big number (RESP3: (<num>, RESP2: bulk string)
    BigNumber(String),
    /// Verbatim string (RESP3: =<len>\r\ntxt:..., RESP2: bulk string)
    /// `format` is the 3-character hint, e.g. "txt" or "mkd".
    VerbatimString { format: String, data: Bytes },
    /// Bulk error (RESP3: !<len>, RESP2: simple error)
    BulkError(String),
    /// Sorted-set member/score pairs (ZRANGE ... WITHSCORES, ZPOPMIN with
    /// COUNT, ...). Mirrors Redis `zrangeResultEmitCBufferToClient`:
    /// RESP2 -> flat array [m1, s1, m2, s2, ...] with scores as bulk strings;
    /// RESP3 -> array of [member, score] pairs with scores as doubles.
    MemberScores(Vec<(Bytes, f64)>),
    /// A sequence of independent reply frames written back-to-back with no
    /// enclosing header. Used when a single command must emit several
    /// top-level frames (e.g. SUBSCRIBE to N channels sends N confirmations).
    Batch(Vec<Value>),
    MapWithResp2 {
        entries: Vec<(Value, Value)>,
        resp2: Resp2MapEncoding,
    },
}

/// Format a double the way Redis `d2string()` does:
/// nan -> "nan", +/-inf -> "inf"/"-inf", signed zero -> "0"/"-0",
/// integral values within i64 -> integer form ("3" not "3.0"),
/// otherwise the shortest round-trip decimal representation.
pub fn format_double(d: f64) -> String {
    if d.is_nan() {
        "nan".to_string()
    } else if d.is_infinite() {
        if d < 0.0 { "-inf" } else { "inf" }.to_string()
    } else if d == 0.0 {
        if d.is_sign_negative() { "-0" } else { "0" }.to_string()
    } else if d == d.trunc() && d >= -9.223_372_036_854_776E18 && d < 9.223_372_036_854_776E18 {
        // Same as Redis double2ll + ll2string fast path.
        format!("{}", d as i64)
    } else {
        // Rust's Display for f64 is the shortest representation that
        // round-trips, equivalent in spirit to Redis' fpconv_dtoa.
        format!("{}", d)
    }
}

impl Value {
    pub fn ok() -> Self {
        Value::SimpleString("OK".to_string())
    }

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

    #[inline]
    fn put_line(buf: &mut impl BufMut, mode: u8, line: &[u8]) {
        buf.put_u8(mode);
        buf.put_slice(line);
        buf.put_slice(b"\r\n");
    }

    #[inline]
    fn put_bulk(buf: &mut impl BufMut, data: &[u8]) {
        buf.put_u8(b'$');
        buf.put_slice(data.len().to_string().as_bytes());
        buf.put_slice(b"\r\n");
        buf.put_slice(data);
        buf.put_slice(b"\r\n");
    }

    pub(crate) fn encode_to(&self, proto: u8, buf: &mut impl BufMut) {
        match self {
            Value::SimpleString(s) => Self::put_line(buf, b'+', s.as_bytes()),
            Value::Error(e) => Self::put_line(buf, b'-', e.as_bytes()),
            Value::Integer(i) => Self::put_line(buf, b':', i.to_string().as_bytes()),
            Value::Null => {
                if proto == 3 {
                    buf.put_slice(b"_\r\n");
                } else {
                    buf.put_slice(b"$-1\r\n");
                }
            }
            Value::BulkString(None) => {
                if proto == 3 {
                    buf.put_slice(b"_\r\n");
                } else {
                    buf.put_slice(b"$-1\r\n");
                }
            }
            Value::BulkString(Some(data)) => Self::put_bulk(buf, data),
            Value::Array(None) => {
                if proto == 3 {
                    buf.put_slice(b"_\r\n");
                } else {
                    buf.put_slice(b"*-1\r\n");
                }
            }
            Value::Array(Some(items)) => {
                Self::put_line(buf, b'*', items.len().to_string().as_bytes());
                for item in items {
                    item.encode_to(proto, buf);
                }
            }
            Value::Map(pairs) => {
                if proto == 3 {
                    Self::put_line(buf, b'%', pairs.len().to_string().as_bytes());
                } else {
                    Self::put_line(buf, b'*', (pairs.len() * 2).to_string().as_bytes());
                }
                for (k, v) in pairs {
                    k.encode_to(proto, buf);
                    v.encode_to(proto, buf);
                }
            }
            Value::Set(items) => {
                let mode = if proto == 3 { b'~' } else { b'*' };
                Self::put_line(buf, mode, items.len().to_string().as_bytes());
                for item in items {
                    item.encode_to(proto, buf);
                }
            }
            Value::Push(items) => {
                let mode = if proto == 3 { b'>' } else { b'*' };
                Self::put_line(buf, mode, items.len().to_string().as_bytes());
                for item in items {
                    item.encode_to(proto, buf);
                }
            }
            Value::Boolean(val) => {
                if proto == 3 {
                    buf.put_slice(if *val { b"#t\r\n" } else { b"#f\r\n" });
                } else {
                    buf.put_slice(if *val { b":1\r\n" } else { b":0\r\n" });
                }
            }
            Value::Double(d) => {
                let repr = format_double(*d);
                if proto == 3 {
                    Self::put_line(buf, b',', repr.as_bytes());
                } else {
                    Self::put_bulk(buf, repr.as_bytes());
                }
            }
            Value::BigNumber(n) => {
                if proto == 3 {
                    Self::put_line(buf, b'(', n.as_bytes());
                } else {
                    Self::put_bulk(buf, n.as_bytes());
                }
            }
            Value::VerbatimString { format, data } => {
                if proto == 3 {
                    // Redis always uses a 3-character format hint.
                    let mut fmt = [b' '; 3];
                    for (i, b) in format.as_bytes().iter().take(3).enumerate() {
                        fmt[i] = *b;
                    }
                    buf.put_u8(b'=');
                    buf.put_slice((data.len() + 4).to_string().as_bytes());
                    buf.put_slice(b"\r\n");
                    buf.put_slice(&fmt);
                    buf.put_u8(b':');
                    buf.put_slice(data);
                    buf.put_slice(b"\r\n");
                } else {
                    Self::put_bulk(buf, data);
                }
            }
            Value::BulkError(e) => {
                if proto == 3 {
                    buf.put_u8(b'!');
                    buf.put_slice(e.len().to_string().as_bytes());
                    buf.put_slice(b"\r\n");
                    buf.put_slice(e.as_bytes());
                    buf.put_slice(b"\r\n");
                } else {
                    Self::put_line(buf, b'-', e.as_bytes());
                }
            }
            Value::MemberScores(pairs) => {
                if proto == 3 {
                    // Array of [member, score] pairs, score as double.
                    Self::put_line(buf, b'*', pairs.len().to_string().as_bytes());
                    for (member, score) in pairs {
                        buf.put_slice(b"*2\r\n");
                        Self::put_bulk(buf, member);
                        Self::put_line(buf, b',', format_double(*score).as_bytes());
                    }
                } else {
                    // Flat array [m1, s1, m2, s2, ...], scores as bulk strings.
                    Self::put_line(buf, b'*', (pairs.len() * 2).to_string().as_bytes());
                    for (member, score) in pairs {
                        Self::put_bulk(buf, member);
                        Self::put_bulk(buf, format_double(*score).as_bytes());
                    }
                }
            }
            Value::Batch(frames) => {
                // No enclosing header: each frame is an independent reply.
                for frame in frames {
                    frame.encode_to(proto, buf);
                }
            }
            Value::MapWithResp2 { entries, resp2 } => {
                if proto == 3 {
                    Self::put_line(buf, b'%', entries.len().to_string().as_bytes());
                    for (k, v) in entries {
                        k.encode_to(proto, buf);
                        v.encode_to(proto, buf);
                    }
                } else {
                    match resp2 {
                        Resp2MapEncoding::Flat => {
                            Self::put_line(buf, b'*', (entries.len() * 2).to_string().as_bytes());
                            for (k, v) in entries {
                                k.encode_to(proto, buf);
                                v.encode_to(proto, buf);
                            }
                        }
                        Resp2MapEncoding::Pairs => {
                            Self::put_line(buf, b'*', entries.len().to_string().as_bytes());
                            for (k, v) in entries {
                                buf.put_slice(b"*2\r\n");
                                k.encode_to(proto, buf);
                                v.encode_to(proto, buf);
                            }
                        }
                        Resp2MapEncoding::Values => {
                            Self::put_line(buf, b'*', entries.len().to_string().as_bytes());
                            for (_, v) in entries {
                                v.encode_to(proto, buf);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Convert a command reply into a Lua value following the Redis
    /// "RESP -> Lua" conversion rules (`redis.call()` results).
    ///
    /// `resp` is the conversion protocol selected with `redis.setresp()`
    /// (Redis default is 2). Under RESP2 conversion, RESP3-only reply
    /// types are first downgraded exactly as they would be on the wire.
    pub fn into_lua_value(self, lua: &Lua, resp: u8) -> mlua::Result<mlua::Value> {
        match self {
            Value::SimpleString(s) => {
                let table = lua.create_table()?;
                table.set("ok", s)?;
                Ok(mlua::Value::Table(table))
            }
            Value::Error(e) | Value::BulkError(e) => {
                let table = lua.create_table()?;
                table.set("err", e)?;
                Ok(mlua::Value::Table(table))
            }
            Value::Integer(i) => Ok(mlua::Value::Integer(i)),
            Value::Boolean(b) => {
                if resp == 3 {
                    Ok(mlua::Value::Boolean(b))
                } else {
                    // RESP2 wire form is :1/:0
                    Ok(mlua::Value::Integer(if b { 1 } else { 0 }))
                }
            }
            Value::BulkString(Some(bytes)) => {
                let s = lua.create_string(&bytes)?;
                Ok(mlua::Value::String(s))
            }
            Value::BulkString(None) => {
                if resp == 3 {
                    Ok(mlua::Value::Nil)
                } else {
                    Ok(mlua::Value::Boolean(false))
                }
            }
            Value::Null => {
                if resp == 3 {
                    Ok(mlua::Value::Nil)
                } else {
                    Ok(mlua::Value::Boolean(false))
                }
            }
            Value::Array(Some(arr)) => {
                let table = lua.create_table_with_capacity(arr.len(), 0)?;
                for (i, val) in arr.into_iter().enumerate() {
                    table.set(i + 1, val.into_lua_value(lua, resp)?)?;
                }
                Ok(mlua::Value::Table(table))
            }
            Value::Array(None) => {
                if resp == 3 {
                    Ok(mlua::Value::Nil)
                } else {
                    Ok(mlua::Value::Boolean(false))
                }
            }
            Value::Push(arr) => {
                Value::Array(Some(arr)).into_lua_value(lua, resp)
            }
            Value::Map(map) => {
                if resp == 3 {
                    // { map = { [k] = v, ... } }
                    let inner = lua.create_table()?;
                    for (k, v) in map {
                        inner.set(k.into_lua_value(lua, resp)?, v.into_lua_value(lua, resp)?)?;
                    }
                    let outer = lua.create_table()?;
                    outer.set("map", inner)?;
                    Ok(mlua::Value::Table(outer))
                } else {
                    // RESP2 downgrade: flat array of alternating keys/values.
                    let table = lua.create_table_with_capacity(map.len() * 2, 0)?;
                    let mut idx = 0;
                    for (k, v) in map {
                        idx += 1;
                        table.set(idx, k.into_lua_value(lua, resp)?)?;
                        idx += 1;
                        table.set(idx, v.into_lua_value(lua, resp)?)?;
                    }
                    Ok(mlua::Value::Table(table))
                }
            }
            Value::Set(items) => {
                if resp == 3 {
                    // { set = { [member] = true, ... } }
                    let inner = lua.create_table()?;
                    for item in items {
                        inner.set(item.into_lua_value(lua, resp)?, true)?;
                    }
                    let outer = lua.create_table()?;
                    outer.set("set", inner)?;
                    Ok(mlua::Value::Table(outer))
                } else {
                    // RESP2 downgrade: plain array.
                    Value::Array(Some(items)).into_lua_value(lua, resp)
                }
            }
            Value::Double(d) => {
                if resp == 3 {
                    // { double = <number> }
                    let table = lua.create_table()?;
                    table.set("double", d)?;
                    Ok(mlua::Value::Table(table))
                } else {
                    // RESP2 downgrade: bulk string.
                    let s = lua.create_string(format_double(d).as_bytes())?;
                    Ok(mlua::Value::String(s))
                }
            }
            Value::BigNumber(n) => {
                if resp == 3 {
                    let table = lua.create_table()?;
                    table.set("big_number", n)?;
                    Ok(mlua::Value::Table(table))
                } else {
                    let s = lua.create_string(n.as_bytes())?;
                    Ok(mlua::Value::String(s))
                }
            }
            Value::VerbatimString { format, data } => {
                if resp == 3 {
                    // { verbatim_string = { string = ..., format = ... } }
                    let inner = lua.create_table()?;
                    inner.set("string", lua.create_string(&data)?)?;
                    inner.set("format", format)?;
                    let outer = lua.create_table()?;
                    outer.set("verbatim_string", inner)?;
                    Ok(mlua::Value::Table(outer))
                } else {
                    let s = lua.create_string(&data)?;
                    Ok(mlua::Value::String(s))
                }
            }
            Value::MemberScores(pairs) => {
                if resp == 3 {
                    // Array of [member, double] pairs.
                    let table = lua.create_table_with_capacity(pairs.len(), 0)?;
                    for (i, (member, score)) in pairs.into_iter().enumerate() {
                        let pair = lua.create_table_with_capacity(2, 0)?;
                        pair.set(1, lua.create_string(&member)?)?;
                        let double_table = lua.create_table()?;
                        double_table.set("double", score)?;
                        pair.set(2, double_table)?;
                        table.set(i + 1, pair)?;
                    }
                    Ok(mlua::Value::Table(table))
                } else {
                    // Flat array of member/score strings.
                    let table = lua.create_table_with_capacity(pairs.len() * 2, 0)?;
                    let mut idx = 0;
                    for (member, score) in pairs {
                        idx += 1;
                        table.set(idx, lua.create_string(&member)?)?;
                        idx += 1;
                        table.set(idx, lua.create_string(format_double(score).as_bytes())?)?;
                    }
                    Ok(mlua::Value::Table(table))
                }
            }
            Value::Batch(frames) => {
                // Should not appear as a command result; expose as array.
                Value::Array(Some(frames)).into_lua_value(lua, resp)
            }
            Value::MapWithResp2 { entries, resp2 } => {
                if resp == 3 {
                    let inner = lua.create_table()?;
                    for (k, v) in entries {
                        inner.set(k.into_lua_value(lua, resp)?, v.into_lua_value(lua, resp)?)?;
                    }
                    let outer = lua.create_table()?;
                    outer.set("map", inner)?;
                    Ok(mlua::Value::Table(outer))
                } else {
                    match resp2 {
                        Resp2MapEncoding::Flat => {
                            let table = lua.create_table_with_capacity(entries.len() * 2, 0)?;
                            let mut idx = 0;
                            for (k, v) in entries {
                                idx += 1;
                                table.set(idx, k.into_lua_value(lua, resp)?)?;
                                idx += 1;
                                table.set(idx, v.into_lua_value(lua, resp)?)?;
                            }
                            Ok(mlua::Value::Table(table))
                        }
                        Resp2MapEncoding::Pairs => {
                            let table = lua.create_table_with_capacity(entries.len(), 0)?;
                            for (i, (k, v)) in entries.into_iter().enumerate() {
                                let pair = lua.create_table_with_capacity(2, 0)?;
                                pair.set(1, k.into_lua_value(lua, resp)?)?;
                                pair.set(2, v.into_lua_value(lua, resp)?)?;
                                table.set(i + 1, pair)?;
                            }
                            Ok(mlua::Value::Table(table))
                        }
                        Resp2MapEncoding::Values => {
                            let table = lua.create_table_with_capacity(entries.len(), 0)?;
                            for (i, (_, v)) in entries.into_iter().enumerate() {
                                table.set(i + 1, v.into_lua_value(lua, resp)?)?;
                            }
                            Ok(mlua::Value::Table(table))
                        }
                    }
                }
            }
        }
    }

    /// Convert a Lua script return value into a reply, following Redis
    /// "Lua -> RESP" conversion rules (`script_lua.c::luaReplyToRedisReply`).
    ///
    /// `proto` is the RESP version of the *calling client*: Lua booleans
    /// convert to RESP3 booleans for RESP3 clients, but to `:1` / null for
    /// RESP2 clients — exactly like Redis.
    pub fn from_lua(lua_val: LuaValue, proto: u8) -> Result<Value, ProtocolError> {
        match lua_val {
            LuaValue::Nil => Ok(Value::Null),
            LuaValue::Boolean(b) => {
                if proto == 3 {
                    Ok(Value::Boolean(b))
                } else if b {
                    Ok(Value::Integer(1))
                } else {
                    Ok(Value::Null)
                }
            }
            LuaValue::Integer(i) => Ok(Value::Integer(i)),
            LuaValue::Number(n) => {
                // Redis converts Lua numbers to integers (truncating the
                // decimal part). Use {double = x} to return a double.
                Ok(Value::Integer(n as i64))
            }
            LuaValue::String(s) => Ok(Value::BulkString(Some(s.as_bytes().to_vec().into()))),
            LuaValue::Table(t) => {
                // Status / error replies: { ok = "..." } / { err = "..." }
                if let LuaValue::String(msg) = t.raw_get::<LuaValue>("err")? {
                    return Ok(Value::Error(
                        String::from_utf8_lossy(msg.as_bytes().as_ref()).into_owned(),
                    ));
                }
                if let LuaValue::String(msg) = t.raw_get::<LuaValue>("ok")? {
                    return Ok(Value::SimpleString(
                        String::from_utf8_lossy(msg.as_bytes().as_ref()).into_owned(),
                    ));
                }

                // RESP3 helper shapes (accepted regardless of the client
                // protocol; the encoder downgrades for RESP2 clients):
                // { double = <number> }
                let double_field = t.raw_get::<LuaValue>("double")?;
                match double_field {
                    LuaValue::Number(n) => return Ok(Value::Double(n)),
                    LuaValue::Integer(i) => return Ok(Value::Double(i as f64)),
                    _ => {}
                }

                // { big_number = "..." }
                if let LuaValue::String(n) = t.raw_get::<LuaValue>("big_number")? {
                    return Ok(Value::BigNumber(
                        String::from_utf8_lossy(n.as_bytes().as_ref()).into_owned(),
                    ));
                }

                // { verbatim_string = { string = "...", format = "..." } }
                if let LuaValue::Table(vs) = t.raw_get::<LuaValue>("verbatim_string")? {
                    if let (LuaValue::String(data), LuaValue::String(format)) = (
                        vs.raw_get::<LuaValue>("string")?,
                        vs.raw_get::<LuaValue>("format")?,
                    ) {
                        return Ok(Value::VerbatimString {
                            format: String::from_utf8_lossy(format.as_bytes().as_ref())
                                .into_owned(),
                            data: data.as_bytes().to_vec().into(),
                        });
                    }
                }

                // { map = { k = v, ... } }
                if let LuaValue::Table(map) = t.raw_get::<LuaValue>("map")? {
                    let mut pairs = Vec::new();
                    for entry in map.pairs::<LuaValue, LuaValue>() {
                        let (k, v) = entry?;
                        pairs.push((Value::from_lua(k, proto)?, Value::from_lua(v, proto)?));
                    }
                    return Ok(Value::Map(pairs));
                }

                // { set = { member = true, ... } }
                if let LuaValue::Table(set) = t.raw_get::<LuaValue>("set")? {
                    let mut members = Vec::new();
                    for entry in set.pairs::<LuaValue, LuaValue>() {
                        let (k, _) = entry?;
                        members.push(Value::from_lua(k, proto)?);
                    }
                    return Ok(Value::Set(members));
                }

                // Plain table: Redis walks integer indices 1..n and stops at
                // the first nil (all other keys are ignored).
                let mut values = Vec::new();
                let mut i = 1;
                loop {
                    let item = t.raw_get::<LuaValue>(i)?;
                    if item.is_nil() {
                        break;
                    }
                    values.push(Value::from_lua(item, proto)?);
                    i += 1;
                }
                Ok(Value::Array(Some(values)))
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
            Value::VerbatimString { data, .. } => Some(data.clone()),
            _ => None,
        }
    }

    #[inline]
    pub fn as_str_lossy(&self) -> Option<Cow<'_, str>> {
        match self {
            Value::BulkString(Some(data)) => Some(String::from_utf8_lossy(data)),
            Value::SimpleString(s) => Some(Cow::Borrowed(s)),
            Value::VerbatimString { data, .. } => Some(String::from_utf8_lossy(data)),
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

impl From<Vec<Bytes>> for Value {
    #[inline]
    fn from(value: Vec<Bytes>) -> Self {
        let values = value
            .into_iter()
            .map(|v| Value::BulkString(Some(v)))
            .collect();

        Self::Array(Some(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(v: &Value, proto: u8) -> String {
        String::from_utf8_lossy(&v.encode_proto(proto)).into_owned()
    }

    #[test]
    fn test_format_double() {
        assert_eq!(format_double(1.0), "1");
        assert_eq!(format_double(-1.0), "-1");
        assert_eq!(format_double(1.5), "1.5");
        assert_eq!(format_double(0.0), "0");
        assert_eq!(format_double(-0.0), "-0");
        assert_eq!(format_double(f64::INFINITY), "inf");
        assert_eq!(format_double(f64::NEG_INFINITY), "-inf");
        assert_eq!(format_double(f64::NAN), "nan");
        assert_eq!(format_double(3.0e3), "3000");
    }

    #[test]
    fn test_encode_null() {
        assert_eq!(enc(&Value::Null, 2), "$-1\r\n");
        assert_eq!(enc(&Value::Null, 3), "_\r\n");
        assert_eq!(enc(&Value::BulkString(None), 2), "$-1\r\n");
        assert_eq!(enc(&Value::BulkString(None), 3), "_\r\n");
        assert_eq!(enc(&Value::Array(None), 2), "*-1\r\n");
        assert_eq!(enc(&Value::Array(None), 3), "_\r\n");
    }

    #[test]
    fn test_encode_boolean() {
        assert_eq!(enc(&Value::Boolean(true), 2), ":1\r\n");
        assert_eq!(enc(&Value::Boolean(false), 2), ":0\r\n");
        assert_eq!(enc(&Value::Boolean(true), 3), "#t\r\n");
        assert_eq!(enc(&Value::Boolean(false), 3), "#f\r\n");
    }

    #[test]
    fn test_encode_double() {
        assert_eq!(enc(&Value::Double(1.5), 3), ",1.5\r\n");
        assert_eq!(enc(&Value::Double(1.5), 2), "$3\r\n1.5\r\n");
        assert_eq!(enc(&Value::Double(10.0), 3), ",10\r\n");
        assert_eq!(enc(&Value::Double(10.0), 2), "$2\r\n10\r\n");
        assert_eq!(enc(&Value::Double(f64::INFINITY), 3), ",inf\r\n");
        assert_eq!(enc(&Value::Double(f64::NEG_INFINITY), 3), ",-inf\r\n");
        assert_eq!(enc(&Value::Double(f64::NAN), 3), ",nan\r\n");
    }

    #[test]
    fn test_encode_big_number() {
        let n = "3492890328409238509324850943850943825024385";
        assert_eq!(enc(&Value::BigNumber(n.into()), 3), format!("({}\r\n", n));
        assert_eq!(
            enc(&Value::BigNumber(n.into()), 2),
            format!("${}\r\n{}\r\n", n.len(), n)
        );
    }

    #[test]
    fn test_encode_verbatim() {
        let v = Value::VerbatimString {
            format: "txt".into(),
            data: Bytes::from_static(b"Some string"),
        };
        assert_eq!(enc(&v, 3), "=15\r\ntxt:Some string\r\n");
        assert_eq!(enc(&v, 2), "$11\r\nSome string\r\n");
    }

    #[test]
    fn test_encode_bulk_error() {
        let v = Value::BulkError("SYNTAX invalid syntax".into());
        assert_eq!(enc(&v, 3), "!21\r\nSYNTAX invalid syntax\r\n");
        assert_eq!(enc(&v, 2), "-SYNTAX invalid syntax\r\n");
    }

    #[test]
    fn test_encode_map() {
        let v = Value::Map(vec![(
            Value::BulkString(Some(Bytes::from_static(b"k"))),
            Value::Integer(1),
        )]);
        assert_eq!(enc(&v, 3), "%1\r\n$1\r\nk\r\n:1\r\n");
        assert_eq!(enc(&v, 2), "*2\r\n$1\r\nk\r\n:1\r\n");
    }

    #[test]
    fn test_encode_map_resp2_values() {
        let v = Value::MapWithResp2 {
            entries: vec![(
                Value::BulkString(Some(Bytes::from_static(b"Capacity"))),
                Value::Integer(100),
            )],
            resp2: Resp2MapEncoding::Values,
        };
        assert_eq!(enc(&v, 3), "%1\r\n$8\r\nCapacity\r\n:100\r\n");
        assert_eq!(enc(&v, 2), "*1\r\n:100\r\n");
    }

    #[test]
    fn test_encode_map_resp2_pairs() {
        let v = Value::MapWithResp2 {
            entries: vec![
                (
                    Value::BulkString(Some(Bytes::from_static(b"a"))),
                    Value::Integer(1),
                ),
                (
                    Value::BulkString(Some(Bytes::from_static(b"b"))),
                    Value::Integer(2),
                ),
            ],
            resp2: Resp2MapEncoding::Pairs,
        };
        assert_eq!(
            enc(&v, 3),
            "%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n"
        );
        assert_eq!(
            enc(&v, 2),
            "*2\r\n*2\r\n$1\r\na\r\n:1\r\n*2\r\n$1\r\nb\r\n:2\r\n"
        );
    }

    #[test]
    fn test_encode_set_and_push() {
        let items = vec![Value::BulkString(Some(Bytes::from_static(b"a")))];
        assert_eq!(enc(&Value::Set(items.clone()), 3), "~1\r\n$1\r\na\r\n");
        assert_eq!(enc(&Value::Set(items.clone()), 2), "*1\r\n$1\r\na\r\n");
        assert_eq!(enc(&Value::Push(items.clone()), 3), ">1\r\n$1\r\na\r\n");
        assert_eq!(enc(&Value::Push(items), 2), "*1\r\n$1\r\na\r\n");
    }

    #[test]
    fn test_encode_member_scores() {
        let v = Value::MemberScores(vec![
            (Bytes::from_static(b"a"), 1.0),
            (Bytes::from_static(b"b"), 2.5),
        ]);
        // RESP2: flat [a, "1", b, "2.5"]
        assert_eq!(
            enc(&v, 2),
            "*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$3\r\n2.5\r\n"
        );
        // RESP3: [[a, 1], [b, 2.5]] with doubles
        assert_eq!(
            enc(&v, 3),
            "*2\r\n*2\r\n$1\r\na\r\n,1\r\n*2\r\n$1\r\nb\r\n,2.5\r\n"
        );
    }

    #[test]
    fn test_encode_batch() {
        let v = Value::Batch(vec![
            Value::SimpleString("A".into()),
            Value::Integer(2),
        ]);
        assert_eq!(enc(&v, 2), "+A\r\n:2\r\n");
        assert_eq!(enc(&v, 3), "+A\r\n:2\r\n");
    }
}
