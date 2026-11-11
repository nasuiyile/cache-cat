//! Metadata for built-in Redis-compatible modules, shared by INFO, HELLO and MODULE LIST.

use crate::raft::types::core::response_value::Value;
use bytes::Bytes;
use std::fmt::Write as _;

struct BuiltinModule {
    name: &'static str,
    /// Redis module compatibility version: major * 10000 + minor * 100 + patch.
    version: i64,
}

// These implementations target the modules bundled with Redis 8.0.0. This is
// independent of both the server compatibility baseline and our package version.
const BUILTIN_MODULES: [BuiltinModule; 2] = [
    BuiltinModule {
        name: "bf",
        version: 80000,
    },
    BuiltinModule {
        name: "ReJSON",
        version: 80000,
    },
];

pub(super) fn info_body() -> String {
    let mut body = String::new();
    for module in &BUILTIN_MODULES {
        let _ = write!(
            body,
            "module:name={},ver={},api=1,filters=0,usedby=[],using=[],options=[]\r\n",
            module.name, module.version,
        );
    }
    body
}

pub(super) fn list() -> Value {
    let bulk = |value: &'static str| Value::BulkString(Some(Bytes::from_static(value.as_bytes())));
    Value::Array(Some(
        BUILTIN_MODULES
            .iter()
            .map(|module| {
                Value::Map(vec![
                    (bulk("name"), bulk(module.name)),
                    (bulk("ver"), Value::Integer(module.version)),
                    // Built into the server, with no shared-library path or load arguments.
                    (bulk("path"), bulk("")),
                    (bulk("args"), Value::Array(Some(Vec::new()))),
                ])
            })
            .collect(),
    ))
}
