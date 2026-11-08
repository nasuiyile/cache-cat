//! JSON commands exercise the same parsing, Raft encoding, and snapshot replay
//! boundaries as the other Redis data types.

use crate::error::ProtocolError;
use crate::protocol::lua::eval::EvalParams;
use crate::protocol::raft_command::RaftCommandFactory;
use crate::protocol::server::memory::usage::MemoryUsageParams;
use crate::protocol::transaction::{QueuedOperation, exec::ExecParams};
use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{base_request, do_request, read_request};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::{AtomicRequest, Operation, RedisOperation, Request};
use bytes::Bytes;
use serde_json::{Value as Json, json};
use std::io::Cursor;

fn parse(args: &[&str]) -> Result<Operation, ProtocolError> {
    let items = args
        .iter()
        .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
        .collect::<Vec<_>>();
    RaftCommandFactory::init_lua().parse_request(&items)
}

fn command(args: &[&str]) -> Operation {
    parse(args).unwrap_or_else(|error| panic!("{args:?}: {error}"))
}

fn apply(
    cache: &MyCache,
    operation: Operation,
    db: u16,
    clock: u64,
    snapshot: Option<(&mut Vec<AtomicRequest>, &mut u64)>,
) -> Value {
    // JSON paths and documents must survive the actual Raft request encoding.
    let request = Request::new(clock, db, operation);
    let request: Request = bincode2::deserialize(&bincode2::serialize(&request).unwrap()).unwrap();
    let (clock, db_number) = request.split_u64();
    let mut update_type = match snapshot {
        Some((queue, revision)) => UpdateType::Snapshot { queue, revision },
        None => UpdateType::None,
    };
    let mut update = Update {
        effects: None,
        waiters: None,
        db_number,
        write_clock: cache.set_write_clock(clock),
        update_type: &mut update_type,
    };
    do_request(cache, request.operation, &mut update, true)
}

fn run(cache: &MyCache, args: &[&str]) -> Value {
    match parse(args) {
        Ok(operation) => apply(cache, operation, 0, 1_000, None),
        Err(error) => error.into(),
    }
}

fn read(cache: &MyCache, args: &[&str], clock: u64) -> Value {
    let Operation::Read(operation) = command(args) else {
        panic!("expected a read command: {args:?}");
    };
    read_request(cache, operation, 0, clock)
}

fn bulk(value: Value) -> Bytes {
    match value {
        Value::BulkString(Some(bytes)) => bytes,
        other => panic!("expected bulk string, got {other:?}"),
    }
}

fn json_reply(value: Value) -> Json {
    serde_json::from_slice(&bulk(value)).unwrap()
}

fn assert_ok(value: Value) {
    assert_eq!(value.encode(), b"+OK\r\n");
}

fn assert_null(value: Value) {
    assert_eq!(value.encode(), b"$-1\r\n");
    assert_eq!(value.encode_proto(3), b"_\r\n");
}

fn assert_error(value: Value) {
    assert!(matches!(value, Value::Error(_)), "{value:?}");
}

#[tokio::test]
async fn json_partial_depth_limited_updates_are_recorded_and_replayed() {
    let cache = MyCache::new(1).unwrap();
    let document = format!(
        "{{\"shallow\":{{\"x\":0}},\"deep\":{}{{\"x\":0}}{}}}",
        "{\"n\":".repeat(100),
        "}".repeat(100),
    );
    assert_ok(run(&cache, &["JSON.SET", "doc", "$", &document]));
    let snapshot = dump(&cache).await;
    let patch = format!("{}1{}", "[".repeat(60), "]".repeat(60));
    let mut queue = Vec::new();
    let mut revision = 0;
    let response = apply(
        &cache,
        command(&["JSON.SET", "doc", "$..x", &patch]),
        0,
        2_000,
        Some((&mut queue, &mut revision)),
    );
    assert_null(response);
    assert_eq!(
        queue.len(),
        1,
        "a null reply can still contain successful writes"
    );
    let restored = restore(&snapshot, &queue, 2_000).await;
    for database in [&cache, &restored] {
        assert_eq!(
            bulk(read(database, &["JSON.GET", "doc", ".shallow.x"], 2_000)),
            patch.as_bytes(),
        );
        assert_eq!(
            json_reply(read(database, &["JSON.GET", "doc", "$.deep..x"], 2_000)),
            json!([0]),
        );
    }
}

#[test]
fn json_get_preserves_legacy_enhanced_and_multipath_reply_shapes() {
    let cache = MyCache::new(1).unwrap();
    assert_ok(run(
        &cache,
        &[
            "jSoN.sEt",
            "doc",
            "$",
            r#"{"name":"猫","items":[2,4,6],"nested":{"name":"kitten"},"nothing":null}"#,
        ],
    ));

    let document =
        json!({"name":"猫", "items":[2,4,6], "nested":{"name":"kitten"}, "nothing":null});
    assert_eq!(
        json_reply(read(&cache, &["JSON.GET", "doc"], 1_000)),
        document
    );
    assert_eq!(
        json_reply(read(&cache, &["JSON.GET", "doc", "$"], 1_000)),
        json!([document])
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "name"])),
        json!("猫")
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$.items[-1]"])),
        json!([6])
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$..name"])),
        json!(["猫", "kitten"])
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", ".name", ".items[1]"])),
        json!({".name":"猫", ".items[1]":4})
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", ".name", "$.items[1]"])),
        json!({".name":["猫"], "$.items[1]":[4]})
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$.absent"])),
        json!([])
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$.nothing"])),
        json!([null])
    );
    assert_error(run(&cache, &["JSON.GET", "doc", ".absent"]));
    assert_error(run(&cache, &["JSON.GET", "doc", ".name", ".absent"]));
    assert_null(run(&cache, &["JSON.GET", "missing", "$"]));
}

#[test]
fn json_set_conditions_create_only_the_final_object_field() {
    let cache = MyCache::new(1).unwrap();
    assert_null(run(&cache, &["JSON.SET", "doc", "$", "{}", "XX"]));
    assert_error(run(&cache, &["JSON.SET", "doc", "$.child", "{}"]));
    assert_ok(run(&cache, &["JSON.SET", "doc", "$", "{}", "NX"]));
    assert_null(run(&cache, &["JSON.SET", "doc", "$", "false", "NX"]));
    assert_null(run(&cache, &["JSON.SET", "doc", "$.child", "{}", "XX"]));
    assert_ok(run(&cache, &["JSON.SET", "doc", "$.child", "{}", "NX"]));
    assert_null(run(&cache, &["JSON.SET", "doc", "$.child", "false", "NX"]));
    assert_ok(run(
        &cache,
        &["JSON.SET", "doc", "$.child", r#"{"a":1}"#, "XX"],
    ));
    assert_null(run(
        &cache,
        &["JSON.SET", "doc", "$.missing.grandchild", "3"],
    ));
    assert_ok(run(&cache, &["JSON.SET", "doc", "$.child.b", "2"]));
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc"])),
        json!({"child":{"a":1,"b":2}})
    );
    // JSON null is an existing value for NX/XX, not a missing path.
    assert_ok(run(&cache, &["JSON.SET", "doc", "$.child.a", "null"]));
    assert_null(run(&cache, &["JSON.SET", "doc", "$.child.a", "4", "NX"]));
    assert_ok(run(&cache, &["JSON.SET", "doc", "$.child.a", "4", "XX"]));
}

#[test]
fn json_path_updates_numeric_arrays_and_all_wildcard_matches() {
    let cache = MyCache::new(1).unwrap();
    assert_ok(run(
        &cache,
        &[
            "JSON.SET",
            "doc",
            "$",
            r#"{"numbers":[1,2,3],"rows":[{"v":1},{"v":2}],"$a":7}"#,
        ],
    ));
    assert_ok(run(&cache, &["JSON.SET", "doc", "$.numbers[-1]", "65536"]));
    assert_ok(run(
        &cache,
        &["JSON.SET", "doc", "$.numbers[0]", r#""changed""#],
    ));
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$.numbers"])),
        json!([["changed", 2, 65536]])
    );
    assert_null(run(&cache, &["JSON.SET", "doc", "$.numbers[1]", "9", "NX"]));
    assert_error(run(&cache, &["JSON.SET", "doc", "$.numbers[3]", "9"]));
    assert_null(run(&cache, &["JSON.SET", "doc", "$.numbers[3]", "9", "XX"]));
    assert_ok(run(&cache, &["JSON.SET", "doc", ".rows[*].v", "8"]));
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$.rows[*].v"])),
        json!([8, 8])
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$a"])),
        json!(7)
    );
    assert_ok(run(
        &cache,
        &["JSON.SET", "doc", "$['special.key']", "true"],
    ));
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$['special.key']"])),
        json!([true])
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$.numbers[1:3]"])),
        json!([2, 65536])
    );
}

#[test]
fn json_filters_and_recursive_updates_preserve_unmatched_values() {
    let cache = MyCache::new(1).unwrap();
    assert_ok(run(
        &cache,
        &[
            "JSON.SET",
            "doc",
            "$",
            r#"{"rows":[{"price":2,"active":true},{"price":8,"active":false},{"price":9,"active":true}]}"#,
        ],
    ));
    assert_eq!(
        json_reply(run(
            &cache,
            &[
                "JSON.GET",
                "doc",
                "$.rows[?(@.price >= 8 && @.active == true)].price"
            ]
        )),
        json!([9])
    );
    assert_ok(run(
        &cache,
        &["JSON.SET", "doc", "$.rows[?(@.price >= 8)].active", "true"],
    ));
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$.rows[*].active"])),
        json!([true, true, true])
    );
    assert_ok(run(
        &cache,
        &[
            "JSON.SET",
            "nested",
            "$",
            r#"{"a":{"a":{"n":1}},"b":{"a":2}}"#,
        ],
    ));
    assert_ok(run(&cache, &["JSON.SET", "nested", "$..a", "0"]));
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "nested"])),
        json!({"a":0,"b":{"a":0}})
    );
}

#[test]
fn json_get_projections_keep_legacy_sibling_paths_scalar() {
    let cache = MyCache::new(1).unwrap();
    assert_ok(run(
        &cache,
        &["JSON.SET", "doc", "$", r#"{"n":2,"n+1":7,"a":[3,1,2]}"#],
    ));
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "n + 1"])),
        json!([3])
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", ".n", "n + 1"])),
        json!({".n":2,"n + 1":[3]})
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", ".n", "$.n + 1"])),
        json!({".n":[2],"$.n + 1":[3]})
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$.n+1"])),
        json!([7])
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$.a.min()"])),
        json!([1.0])
    );
    assert_error(run(&cache, &["JSON.SET", "doc", "$.n + 1", "0"]));
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$.n"])),
        json!([2])
    );
}

#[test]
fn json_get_formatting_and_noescape_are_compatible() {
    let cache = MyCache::new(1).unwrap();
    assert_ok(run(&cache, &["JSON.SET", "doc", "$", r#"{"a":[1,2]}"#]));
    assert_eq!(
        bulk(run(
            &cache,
            &[
                "JSON.GET", "doc", "INDENT", "  ", "NEWLINE", "\n", "SPACE", " "
            ]
        ))
        .as_ref(),
        b"{\n  \"a\": [\n    1,\n    2\n  ]\n}"
    );
    assert_eq!(
        bulk(run(&cache, &["JSON.GET", "doc", "NOESCAPE"])),
        bulk(run(&cache, &["JSON.GET", "doc"]))
    );
    assert_eq!(
        bulk(run(&cache, &["JSON.GET", "doc", "SPACE", " ", "SPACE", ""])).as_ref(),
        br#"{"a":[1,2]}"#
    );
    for args in [
        vec!["JSON.GET"],
        vec!["JSON.GET", "doc", "INDENT"],
        vec!["JSON.GET", "doc", "SPACE"],
        vec!["JSON.GET", "doc", "NEWLINE"],
    ] {
        assert!(parse(&args).is_err(), "{args:?}");
    }
}

#[test]
fn json_get_resp3_formats_preserve_the_outer_path_list() {
    let cache = MyCache::new(1).unwrap();
    assert_ok(run(
        &cache,
        &["JSON.SET", "doc", "$", r#"{"s":"text","a":[1,true,null]}"#],
    ));
    let resp3_read = |args: &[&str]| {
        let Operation::Read(ReadOperation::JsonGet(mut params)) = command(args) else {
            panic!("expected JSON.GET");
        };
        params.resp3 = true;
        apply(
            &cache,
            Operation::Read(ReadOperation::JsonGet(params)),
            0,
            1_000,
            None,
        )
    };
    assert_error(run(&cache, &["JSON.GET", "doc", "FORMAT", "EXPAND", "$"]));
    let expanded = resp3_read(&[
        "JSON.GET",
        "doc",
        "FORMAT",
        "EXPAND",
        "$.s",
        "$.a",
        "$.missing",
    ]);
    let expected = Value::Array(Some(vec![
        Value::Array(Some(vec![Value::BulkString(Some("text".into()))])),
        Value::Array(Some(vec![Value::Array(Some(vec![
            Value::Integer(1),
            Value::Boolean(true),
            Value::Null,
        ]))])),
        Value::Array(Some(vec![])),
    ]));
    assert_eq!(expanded.encode_proto(3), expected.encode_proto(3));
    let expanded_once = resp3_read(&["JSON.GET", "doc", "FORMAT", "EXPAND1", "$.s", "$.a"]);
    let expected = Value::Array(Some(vec![
        Value::Array(Some(vec![Value::BulkString(Some("\"text\"".into()))])),
        Value::Array(Some(vec![Value::BulkString(Some("[1,true,null]".into()))])),
    ]));
    assert_eq!(expanded_once.encode_proto(3), expected.encode_proto(3));
    assert_eq!(
        json_reply(resp3_read(&["JSON.GET", "doc", "FORMAT", "STRING", "$.s",])),
        json!(["text"])
    );
}

#[test]
fn json_set_format_options_and_bson_envelopes_match_redis() {
    let cache = MyCache::new(1).unwrap();
    for format in ["JSON", "STRING"] {
        assert_ok(run(
            &cache,
            &["JSON.SET", "doc", "$", r#"{"n":3}"#, "fOrMaT", format],
        ));
        assert_eq!(
            json_reply(run(&cache, &["JSON.GET", "doc"])),
            json!({"n":3})
        );
    }
    assert_ok(run(
        &cache,
        &[
            "JSON.SET", "doc", "$", "false", "FORMAT", "JSON", "FORMAT", "STRING",
        ],
    ));
    assert_eq!(json_reply(run(&cache, &["JSON.GET", "doc"])), json!(false));
    for format in ["json", "string", "bson", "other"] {
        assert_error(run(
            &cache,
            &["JSON.SET", "doc", "$", "{}", "FORMAT", format],
        ));
    }
    assert_error(run(&cache, &["JSON.SET", "doc", "$", "{}", "FORMAT"]));
    assert_eq!(json_reply(run(&cache, &["JSON.GET", "doc"])), json!(false));

    for (envelope, expected) in [
        (bson::doc! {"first": {"n": 7}, "ignored": 8}, json!({"n":7})),
        (
            bson::doc! {"first": [1, 2, 3], "ignored": false},
            json!([1, 2, 3]),
        ),
        (bson::doc! {"first": true}, json!(true)),
        (bson::doc! {"first": bson::Bson::Null}, Json::Null),
        (bson::Document::new(), Json::Null),
    ] {
        let payload = bson::to_vec(&envelope).unwrap();
        let items = [
            Value::BulkString(Some("JSON.SET".into())),
            Value::BulkString(Some("doc".into())),
            Value::BulkString(Some("$".into())),
            Value::BulkString(Some(payload.into())),
            Value::BulkString(Some("FORMAT".into())),
            Value::BulkString(Some("BSON".into())),
        ];
        let operation = RaftCommandFactory::init_lua()
            .parse_request(&items)
            .unwrap();
        assert_ok(apply(&cache, operation, 0, 1_000, None));
        assert_eq!(json_reply(run(&cache, &["JSON.GET", "doc"])), expected);
    }
}

#[test]
fn json_get_format_values_are_case_sensitive_but_keywords_are_not() {
    let cache = MyCache::new(1).unwrap();
    assert_ok(run(&cache, &["JSON.SET", "doc", "$", "[1,2]"]));
    for format in ["string", "expand", "expand1", "STRINGS", "JSON"] {
        assert_error(run(&cache, &["JSON.GET", "doc", "$", "fOrMaT", format]));
    }
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$", "fOrMaT", "STRING"])),
        json!([[1, 2]])
    );
    // Redis accepts FORMAT after a path under RESP2 and still returns JSON text.
    assert_eq!(
        json_reply(run(
            &cache,
            &["JSON.GET", "doc", "$[0]", "FORMAT", "EXPAND"]
        )),
        json!([1])
    );
}

#[tokio::test]
async fn json_fpha_precision_is_explicit_and_survives_snapshot_replay() {
    let cache = MyCache::new(2).unwrap();
    assert_ok(run(&cache, &["JSON.SET", "default", "$", "[1.1,2.2]"]));
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "default"])),
        json!([1.1, 2.2])
    );
    let cases = [
        ("fp16", json!([1.099609375, 2.19921875])),
        ("bf16", json!([1.1015625, 2.203125])),
        ("fp32", json!([f64::from(1.1f32), f64::from(2.2f32)])),
        ("fp64", json!([1.1, 2.2])),
    ];
    let mut queue = Vec::new();
    let mut revision = 0;
    let empty = dump(&cache).await;
    for (format, expected) in &cases {
        let operation = command(&["JSON.SET", format, "$", "[1.1,2.2]", "fPhA", format]);
        assert_ok(apply(
            &cache,
            operation,
            0,
            2_000,
            Some((&mut queue, &mut revision)),
        ));
        // The whole typed array uses the shortest decimal representations that
        // round-trip through its stored precision. Selecting the elements
        // individually promotes them to JSON numbers with the exact values.
        assert_eq!(
            json_reply(read(&cache, &["JSON.GET", format], 2_000)),
            json!([1.1, 2.2])
        );
        assert_eq!(
            json_reply(read(&cache, &["JSON.GET", format, "$[*]"], 2_000)),
            *expected
        );
    }
    for invalid in ["F16", "16", "FLOAT", ""] {
        assert_error(run(
            &cache,
            &["JSON.SET", "default", "$", "[1.1]", "FPHA", invalid],
        ));
    }
    let full = dump(&cache).await;
    // Test both a full-document restore and replay of the original FPHA request.
    for snapshot in [&empty, &full] {
        let restored = restore(snapshot, &queue, 2_000).await;
        for (format, expected) in &cases {
            assert_eq!(
                json_reply(read(&restored, &["JSON.GET", format], 2_000)),
                json!([1.1, 2.2])
            );
            assert_eq!(
                json_reply(read(&restored, &["JSON.GET", format, "$[*]"], 2_000)),
                *expected
            );
        }
        assert_eq!(
            json_reply(read(&restored, &["JSON.GET", "default"], 2_000)),
            json!([1.1, 2.2])
        );
    }
}

#[test]
fn json_get_preserves_redis_float_exponent_formatting() {
    let cache = MyCache::new(1).unwrap();
    for literal in ["1e308", "-1e308", "1e-308", "1.0", "-0.0"] {
        assert_ok(run(&cache, &["JSON.SET", "number", "$", literal]));
        assert_eq!(
            bulk(run(&cache, &["JSON.GET", "number"])).as_ref(),
            literal.as_bytes()
        );
        assert_eq!(
            bulk(run(&cache, &["JSON.GET", "number", "$"])).as_ref(),
            format!("[{literal}]").as_bytes()
        );
    }
    assert_ok(run(&cache, &["JSON.SET", "array", "$", "[1e308,1e-308]"]));
    assert_eq!(
        bulk(run(&cache, &["JSON.GET", "array"])).as_ref(),
        b"[1e308,1e-308]"
    );
}

#[test]
fn json_get_multiple_paths_omit_invalid_paths_without_changing_reply_mode() {
    let cache = MyCache::new(1).unwrap();
    assert_ok(run(&cache, &["JSON.SET", "doc", "$", r#"{"n":2}"#]));
    assert_error(run(&cache, &["JSON.GET", "doc", "$["]));
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$.n", "$["])),
        json!({"$.n":[2]})
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", ".n", "$["])),
        json!({".n":[2]})
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", ".n", ".bad["])),
        json!({".n":2})
    );
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "doc", "$[", "$.."])),
        json!({})
    );
}

#[test]
fn json_set_parenthesized_root_respects_conditional_path_rules() {
    let cache = MyCache::new(1).unwrap();
    assert_error(run(&cache, &["JSON.SET", "doc", "($)", "0"]));
    assert_ok(run(&cache, &["JSON.SET", "doc", "$", "1"]));
    assert_error(run(&cache, &["JSON.SET", "doc", "($)", "2", "NX"]));
    assert_eq!(json_reply(run(&cache, &["JSON.GET", "doc"])), json!(1));
    assert_ok(run(&cache, &["JSON.SET", "doc", "($)", "2", "XX"]));
    assert_eq!(json_reply(run(&cache, &["JSON.GET", "doc"])), json!(2));
}

#[test]
fn json_lua_expanded_reads_follow_redis_setresp_for_call_and_pcall() {
    let cache = MyCache::new(1).unwrap();
    assert_ok(run(
        &cache,
        &["JSON.SET", "doc", "$", r#"{"active":true,"value":1.25}"#],
    ));
    for function in ["call", "pcall"] {
        let script = format!(
            "redis.setresp(3); return redis.{function}('JSON.GET', KEYS[1], 'FORMAT', 'EXPAND', '$.active', '$.value')"
        );
        let mut params = EvalParams::new(script.into(), 1, vec!["doc".into()], Vec::new());
        params.proto = 3;
        let reply = apply(
            &cache,
            Operation::Redis(RedisOperation::RedisEval(params)),
            0,
            1_000,
            None,
        );
        let expected = Value::Array(Some(vec![
            Value::Array(Some(vec![Value::Boolean(true)])),
            Value::Array(Some(vec![Value::Double(1.25)])),
        ]));
        assert_eq!(
            reply.encode_proto(3),
            expected.encode_proto(3),
            "redis.{function}"
        );
    }
    // The client protocol alone must not enable RESP3 command semantics in Lua.
    let mut params = EvalParams::new(
        "local e = redis.pcall('JSON.GET', KEYS[1], 'FORMAT', 'EXPAND', '$'); return type(e.err) == 'string'".into(),
        1,
        vec!["doc".into()],
        Vec::new(),
    );
    params.proto = 3;
    let reply = apply(
        &cache,
        Operation::Redis(RedisOperation::RedisEval(params)),
        0,
        1_000,
        None,
    );
    assert_eq!(reply.encode_proto(3), b"#t\r\n");
}

#[test]
fn json_invalid_input_and_wrongtype_never_replace_existing_data() {
    let cache = MyCache::new(1).unwrap();
    assert_ok(run(&cache, &["JSON.SET", "doc", "$", r#"{"stable":1}"#]));
    for args in [
        vec!["JSON.SET", "doc", "$", "{"],
        vec!["JSON.SET", "doc", "$", "1 2"],
        vec!["JSON.SET", "doc", "$", "NaN"],
        vec!["JSON.SET", "doc", "$", "1e999"],
        vec!["JSON.SET", "doc", "$.broken[", "0"],
        vec!["JSON.SET", "doc", "$", "{}", "NX", "NX"],
        vec!["JSON.SET", "doc", "$", "{}", "NX", "XX"],
        vec!["JSON.SET", "doc", "$", "{}", "XX", "XX"],
        vec!["JSON.SET", "doc", "$", "{}", "invalid"],
    ] {
        assert_error(run(&cache, &args));
        assert_eq!(
            json_reply(run(&cache, &["JSON.GET", "doc"])),
            json!({"stable":1}),
            "{args:?}"
        );
    }
    assert_error(run(&cache, &["JSON.SET", "doc", "$", "invalid-json", "NX"]));
    assert_ok(run(&cache, &["SET", "string", "unchanged"]));
    assert_error(run(&cache, &["JSON.GET", "string"]));
    assert_error(run(&cache, &["JSON.SET", "string", "$", "{}"]));
    assert_eq!(bulk(run(&cache, &["GET", "string"])).as_ref(), b"unchanged");
    assert_error(run(&cache, &["GET", "doc"]));
    assert_error(run(&cache, &["HGET", "doc", "stable"]));
}

#[test]
fn json_updates_keep_ttl_and_expired_documents_follow_creation_rules() {
    let cache = MyCache::new(1).unwrap();
    assert_ok(run(&cache, &["JSON.SET", "doc", "$", r#"{"n":1}"#]));
    assert_eq!(run(&cache, &["PEXPIRE", "doc", "5000"]).encode(), b":1\r\n");
    let expiry = cache.databases[0]
        .mocha
        .get_entry(b"doc".as_slice())
        .unwrap()
        .expire_at;
    assert_eq!(expiry, Some(6_000));
    for (clock, args) in [
        (2_000, vec!["JSON.SET", "doc", "$.n", "2"]),
        (3_000, vec!["JSON.SET", "doc", "$", "[1,2,3]"]),
    ] {
        assert_ok(apply(&cache, command(&args), 0, clock, None));
        assert_eq!(
            cache.databases[0]
                .mocha
                .get_entry(b"doc".as_slice())
                .unwrap()
                .expire_at,
            expiry
        );
    }
    assert_eq!(
        json_reply(read(&cache, &["JSON.GET", "doc"], 5_999)),
        json!([1, 2, 3])
    );
    assert_null(read(&cache, &["JSON.GET", "doc"], 6_000));
    assert_null(apply(
        &cache,
        command(&["JSON.SET", "doc", "$", "0", "XX"]),
        0,
        6_000,
        None,
    ));
    assert_ok(apply(
        &cache,
        command(&["JSON.SET", "doc", "$", "0", "NX"]),
        0,
        6_000,
        None,
    ));
    assert_eq!(
        cache.databases[0]
            .mocha
            .get_entry(b"doc".as_slice())
            .unwrap()
            .expire_at,
        None
    );
}

#[test]
fn json_type_memory_and_key_commands_use_the_new_data_type() {
    let cache = MyCache::new(1).unwrap();
    assert_ok(run(
        &cache,
        &[
            "JSON.SET",
            "doc",
            "$",
            r#"{"a":[1,2,3],"b":"a longer string shared by documents"}"#,
        ],
    ));
    assert_eq!(run(&cache, &["TYPE", "doc"]).encode(), b"+ReJSON-RL\r\n");
    let memory = read_request(
        &cache,
        ReadOperation::MemoryUsage(MemoryUsageParams {
            key: "doc".into(),
            samples: 0,
        }),
        0,
        1_000,
    );
    assert!(matches!(memory, Value::Integer(bytes) if bytes > 0));
    assert_eq!(run(&cache, &["EXISTS", "doc"]).encode(), b":1\r\n");
    assert_ok(run(&cache, &["RENAME", "doc", "renamed"]));
    assert_null(run(&cache, &["JSON.GET", "doc"]));
    assert_eq!(
        json_reply(run(&cache, &["JSON.GET", "renamed", "$.a"])),
        json!([[1, 2, 3]])
    );
    assert_eq!(run(&cache, &["DEL", "renamed"]).encode(), b":1\r\n");
    assert_null(run(&cache, &["JSON.GET", "renamed"]));
}

async fn dump(cache: &MyCache) -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    cache.dump_cache_to_writer(&mut bytes).await.unwrap();
    bytes.into_inner()
}

async fn restore(full: &[u8], queue: &[AtomicRequest], clock: u64) -> MyCache {
    let cache = MyCache::new(2).unwrap();
    cache.pause_expire_workers();
    cache
        .load_cache_from_reader(&mut Cursor::new(full))
        .await
        .unwrap();
    let queue: Vec<AtomicRequest> =
        bincode2::deserialize(&bincode2::serialize(queue).unwrap()).unwrap();
    for delta in queue {
        let mut update_type = UpdateType::CAS {
            expected_revision: delta.expected_revision,
            revision: delta.version,
        };
        let mut update = Update {
            effects: None,
            waiters: None,
            db_number: delta.db_number,
            write_clock: cache.set_write_clock(delta.write_clock),
            update_type: &mut update_type,
        };
        base_request(&cache, delta.request, &mut update);
    }
    cache.set_write_clock(clock);
    cache.resume_expire_workers();
    cache
}

#[tokio::test]
async fn json_snapshot_cas_replays_every_scan_cut_with_ttl_and_recreation() {
    let cache = MyCache::new(2).unwrap();
    let mut cuts = vec![dump(&cache).await];
    let mut queue = Vec::new();
    let mut revision = 40;
    let operations = [
        vec!["JSON.SET", "doc", "$", r#"{"n":1,"arr":[1,2,3]}"#],
        vec!["PEXPIRE", "doc", "10000"],
        vec!["JSON.SET", "doc", "$.arr[-1]", "4294967296"],
        vec!["JSON.SET", "doc", "$.n", "null"],
        vec!["JSON.SET", "doc", "$.n", "5", "NX"],
        vec!["JSON.GET", "doc", "$..n"],
        vec!["DEL", "doc"],
        vec![
            "JSON.SET",
            "doc",
            "$",
            r#"{"n":7,"arr":[18446744073709551615]}"#,
            "NX",
        ],
        vec!["JSON.SET", "doc", "$.arr[0]", "9223372036854775808"],
    ];
    for (index, args) in operations.iter().enumerate() {
        let response = apply(
            &cache,
            command(args),
            1,
            1_000 + index as u64,
            Some((&mut queue, &mut revision)),
        );
        assert!(
            !matches!(response, Value::Error(_)),
            "{args:?}: {response:?}"
        );
        cuts.push(dump(&cache).await);
    }
    assert_eq!(queue.len(), 7, "GET and failed NX must not record writes");
    for (index, full) in cuts.iter().enumerate() {
        let restored = restore(full, &queue, 1_008).await;
        let response = apply(&restored, command(&["JSON.GET", "doc"]), 1, 1_008, None);
        assert_eq!(
            json_reply(response),
            json!({"n":7,"arr":[9223372036854775808u64]}),
            "scan cut {index}"
        );
        let entry = restored.databases[1]
            .mocha
            .get_entry(b"doc".as_slice())
            .unwrap();
        assert_eq!(entry.expire_at, None);
        assert_eq!(entry.value.version, revision);
        assert_null(read(&restored, &["JSON.GET", "doc"], 1_008));
    }
}

#[tokio::test]
async fn json_snapshot_writes_do_not_mutate_a_value_held_by_the_full_scan() {
    let cache = MyCache::new(2).unwrap();
    assert_ok(run(
        &cache,
        &[
            "JSON.SET",
            "doc",
            "$",
            r#"{"a":{"long field name":[1,2,3]}}"#,
        ],
    ));
    let held = cache.databases[0]
        .mocha
        .get_entry(b"doc".as_slice())
        .unwrap();
    let before = bincode2::serialize(&held).unwrap();
    let mut queue = Vec::new();
    let mut revision = 0;
    assert_ok(apply(
        &cache,
        command(&["JSON.SET", "doc", "$['a']['long field name'][1]", "false"]),
        0,
        2_000,
        Some((&mut queue, &mut revision)),
    ));
    assert_eq!(bincode2::serialize(&held).unwrap(), before);
    assert_eq!(
        json_reply(read(&cache, &["JSON.GET", "doc"], 2_000)),
        json!({"a":{"long field name":[1,false,3]}})
    );
    let queue_len = queue.len();
    let previous_revision = revision;
    assert_null(apply(
        &cache,
        command(&["JSON.SET", "doc", "$", "{}", "NX"]),
        0,
        2_001,
        Some((&mut queue, &mut revision)),
    ));
    assert_eq!(queue.len(), queue_len);
    assert_eq!(revision, previous_revision);
}

#[tokio::test]
async fn json_exec_and_lua_remain_deterministic_after_snapshot_restore() {
    let original = MyCache::new(2).unwrap();
    assert_ok(run(
        &original,
        &[
            "JSON.SET",
            "doc",
            "$",
            r#"{"rows":[{"n":1},{"n":2},{"n":3}]}"#,
        ],
    ));
    assert_eq!(
        run(&original, &["PEXPIRE", "doc", "10000"]).encode(),
        b":1\r\n"
    );
    let full = dump(&original).await;
    let restored = restore(&full, &[], 1_000).await;
    // Replica-local read clocks must not affect the replicated Lua/EXEC reads.
    assert!(restored.get_and_update_read_clock() > 11_000);
    let exec = Operation::Redis(RedisOperation::RedisExec(ExecParams {
        operations: vec![
            QueuedOperation::new(0, command(&["JSON.GET", "doc", "$.rows[*].n"])),
            QueuedOperation::new(0, command(&["JSON.SET", "doc", "$.rows[1].n", "9"])),
            QueuedOperation::new(0, command(&["JSON.GET", "doc", "$.rows[*].n"])),
        ],
    }));
    let mut first_reply = None;
    for cache in [&original, &restored] {
        let reply = apply(cache, exec.clone(), 0, 2_000, None);
        assert_eq!(
            reply.encode(),
            b"*3\r\n$7\r\n[1,2,3]\r\n+OK\r\n$7\r\n[1,9,3]\r\n"
        );
        let mut eval = EvalParams::new(
            "local v = redis.call('JSON.GET', KEYS[1], '$.rows[*].n'); redis.call('JSON.SET', KEYS[2], '$', v); return v".into(),
            2,
            vec!["doc".into(), "copy".into()],
            Vec::new(),
        );
        eval.proto = 3;
        let reply = apply(
            cache,
            Operation::Redis(RedisOperation::RedisEval(eval)),
            0,
            3_000,
            None,
        )
        .encode_proto(3);
        if let Some(expected) = &first_reply {
            assert_eq!(&reply, expected);
        } else {
            first_reply = Some(reply);
        }
        assert_eq!(
            json_reply(read(cache, &["JSON.GET", "copy"], 3_000)),
            json!([1, 9, 3])
        );
        assert_null(read(cache, &["JSON.GET", "doc"], 11_000));
    }
}
