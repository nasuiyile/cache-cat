use super::*;
use serde_json::{Value, json};

fn document(json: &str) -> IValue {
    serde_json::from_str(json).unwrap()
}

fn select(value: &IValue, path: &str) -> Value {
    let path = JsonPath::parse(path).unwrap();
    serde_json::to_value(path.select(value)).unwrap()
}

#[test]
fn path_forms_and_literal_dollar_fields() {
    let doc = document(r#"{"a":1,"$a":2,"-$x":3,"a+1":4,"a.b":5}"#);
    for (path, expected, legacy) in [
        ("a", json!([1]), true),
        (".a", json!([1]), true),
        ("$.a", json!([1]), false),
        ("$a", json!([2]), true),
        ("-$x", json!([3]), true),
        ("a+1", json!([4]), true),
        ("$['a.b']", json!([5]), false),
    ] {
        let parsed = JsonPath::parse(path).unwrap();
        assert_eq!(parsed.is_legacy(), legacy, "{path}");
        assert_eq!(
            serde_json::to_value(parsed.select(&doc)).unwrap(),
            expected,
            "{path}"
        );
    }
    assert!(JsonPath::parse(".").unwrap().is_root());
    assert!(JsonPath::parse("$").unwrap().is_root());
}

#[test]
fn packed_array_indices_slices_and_unions() {
    let doc = document(r#"{"n":[0,1,2,3,4],"x":{"a":1,"b":2}}"#);
    for (path, expected) in [
        ("$.n[-1]", json!([4])),
        ("$.n[4,0,2]", json!([4, 0, 2])),
        ("$.n[1:5:2]", json!([1, 3])),
        ("$.n[-3:]", json!([2, 3, 4])),
        ("$.n[:2]", json!([0, 1])),
        ("$.n[99]", json!([])),
        ("$.x['b','a']", json!([2, 1])),
        ("$.n.[1]", json!([1])),
    ] {
        assert_eq!(select(&doc, path), expected, "{path}");
    }
    for path in ["$[::0]", "$[::-1]", "$[01]", "$[1,]", "$.", "$["] {
        assert!(JsonPath::parse(path).is_err(), "{path}");
    }
}

#[test]
fn recursive_paths_and_parenthesized_path_preserve_order() {
    let doc = document(r#"{"a":{"x":1},"b":[{"x":2},{"x":3}],"x":4}"#);
    assert_eq!(select(&doc, "$..x"), json!([4, 1, 2, 3]));
    assert_eq!(select(&doc, "($..x)"), select(&doc, "$..x"));
    assert_eq!(select(&doc, "$.b[*].x"), json!([2, 3]));
}

#[test]
fn comparisons_boolean_filters_and_root_reference() {
    let doc = document(r#"{"limit":2,"a":[{"x":1,"ok":false},{"x":3},{"y":4}]}"#);
    assert_eq!(select(&doc, "$.a[?@.x >= $.limit]"), json!([{"x":3}]));
    assert_eq!(
        select(&doc, "$.a[?(@.ok || @.y)]"),
        json!([{"x":1,"ok":false},{"y":4}])
    );
    assert_eq!(select(&doc, "$.a[?!@.x==1]"), json!([{"x":3},{"y":4}]));
    assert_eq!(select(&doc, "$.a[?@.x && !@.ok]"), json!([{"x":3}]));
}

#[test]
fn set_filters_size_and_deep_equality() {
    let doc = document(r#"{"a":[[1,2],[4,5],[],{"a":1,"b":2},"ab"],"n":[1,2,3,4]}"#);
    assert_eq!(select(&doc, "$.n[?@ in [2,4]]"), json!([2, 4]));
    assert_eq!(select(&doc, "$.n[?@ nin [2,4]]"), json!([1, 3]));
    assert_eq!(
        select(&doc, "$.a[?@ subsetof [1,2,3]]"),
        json!([[1, 2], []])
    );
    assert_eq!(select(&doc, "$.a[?@ anyof [2,9]]"), json!([[1, 2]]));
    assert_eq!(select(&doc, "$.a[?@ noneof [1,2]]"), json!([[4, 5], []]));
    assert_eq!(select(&doc, "$.a[?@ empty true]"), json!([[]]));
    assert_eq!(
        select(&doc, "$.a[?@ size 2]"),
        json!([[1,2],[4,5],{"a":1,"b":2},"ab"])
    );
    assert_eq!(
        select(&doc, "$.a[?@ == {'b':2,'a':1}]"),
        json!([{"a":1,"b":2}])
    );
}

#[test]
fn projections_arithmetic_and_functions() {
    let doc =
        document(r#"{"a":2,"b":4,"arr":[1,2,3],"nums":[3,1,2],"s":"你好","obj":{"x":1,"y":2}}"#);
    for (path, expected) in [
        ("$.a + 1", json!([3])),
        ("a + 1", json!([3])),
        ("($.a + $.b) / 2", json!([3.0])),
        ("- $.a", json!([-2])),
        ("length ( $.arr )", json!([3])),
        ("$.s.length()", json!([2])),
        ("$.arr.index(-1)", json!([3])),
        ("$.arr.first()", json!([1])),
        ("$.nums.min()", json!([1.0])),
        ("$.nums.sum()", json!([6.0])),
        ("$.obj.keys()", json!(["x", "y"])),
        ("$.obj~", json!(["x", "y"])),
        ("$.obj.keys().length()", json!([2])),
        ("$.obj.keys().first()", json!(["x"])),
        ("$.arr.append(9)", json!([1, 2, 3, 9])),
        ("$.arr[*].append(9)", json!([1, 2, 3, 9])),
        ("$.arr.append($.missing)", json!([])),
        ("$.a / 0", json!([])),
        ("$.arr.length(99)", json!([])),
        ("$.a.bogus()", json!([])),
    ] {
        assert_eq!(select(&doc, path), expected, "{path}");
    }
    let legacy_projection = JsonPath::parse("a + 1").unwrap();
    assert!(!legacy_projection.is_legacy());
    assert!(legacy_projection.is_projection());
}

#[test]
fn value_function_distinguishes_document_nodes_and_synthesized_literals() {
    let doc = document(r#"{"arr":[1,2],"one":{"x":3}}"#);
    for (path, expected) in [
        ("first([1])", json!([1])),
        ("value(first([1]))", json!([])),
        ("value(last([1,2]))", json!([])),
        ("value(index([1,2],0))", json!([])),
        ("value(first($.arr))", json!([1])),
        ("value(last($.arr))", json!([2])),
        ("value(index($.arr,0))", json!([1])),
        ("value(first($.arr[*]))", json!([1])),
        ("$.one.keys().value()", json!(["x"])),
        ("$.one.keys().value().value()", json!([])),
        ("$.one.keys().first().value()", json!([])),
        ("$.one.keys().last().value()", json!([])),
        ("$.one.keys().index(0).value()", json!([])),
        ("$.arr.append(9).first().value()", json!([])),
    ] {
        assert_eq!(select(&doc, path), expected, "{path}");
    }
}

#[test]
fn regex_functions_and_numeric_functions() {
    let doc = document(r#"{"a":["abc","xabc","a","b"],"n":[2.1,3.9,1.0]}"#);
    assert_eq!(
        select(&doc, r#"$.a[?match(@, "a.*")]"#),
        json!(["abc", "a"])
    );
    assert_eq!(
        select(&doc, r#"$.a[?@ =~ "b"]"#),
        json!(["abc", "xabc", "b"])
    );
    assert_eq!(select(&doc, "$.n[?ceiling(@)==3]"), json!([2.1]));
    assert_eq!(select(&doc, "$.n[?floor(@)==3]"), json!([3.9]));
}

#[test]
fn set_creates_only_final_object_field_and_obeys_conditions() {
    let mut doc = document(r#"{"a":{},"b":[1,2]}"#);
    let value = document("9");
    assert!(
        JsonPath::parse("$.a.new")
            .unwrap()
            .set(&mut doc, &value, SetMode::Nx)
            .unwrap()
            .changed
    );
    assert!(
        !JsonPath::parse("$.a.new")
            .unwrap()
            .set(&mut doc, &value, SetMode::Nx)
            .unwrap()
            .changed
    );
    assert!(
        !JsonPath::parse("$.a.missing")
            .unwrap()
            .set(&mut doc, &value, SetMode::Xx)
            .unwrap()
            .changed
    );
    assert!(
        !JsonPath::parse("$.missing.child")
            .unwrap()
            .set(&mut doc, &value, SetMode::Always)
            .unwrap()
            .changed
    );
    assert!(
        JsonPath::parse("$.b[5]")
            .unwrap()
            .set(&mut doc, &value, SetMode::Always)
            .is_err()
    );
    assert!(
        JsonPath::parse("$..missing")
            .unwrap()
            .set(&mut doc, &value, SetMode::Always)
            .is_err()
    );
    assert!(
        JsonPath::parse("$.b[*]")
            .unwrap()
            .set(&mut doc, &value, SetMode::Nx)
            .is_err()
    );
}

#[test]
fn set_updates_all_legacy_matches_and_suppresses_descendants() {
    let mut doc = document(r#"{"a":[{"x":1},{"x":2}]}"#);
    assert!(
        JsonPath::parse(".a[*].x")
            .unwrap()
            .set(&mut doc, &document("9"), SetMode::Always)
            .unwrap()
            .complete
    );
    assert_eq!(select(&doc, "$.a[*].x"), json!([9, 9]));
    let mut doc = document(r#"[{"a":1}]"#);
    JsonPath::parse("$..*")
        .unwrap()
        .set(&mut doc, &document(r#"[{"a":1}]"#), SetMode::Always)
        .unwrap();
    assert_eq!(serde_json::to_value(doc).unwrap(), json!([[{"a":1}]]));
}

#[test]
fn typed_array_replacement_promotes_losslessly() {
    let mut doc = document("[1,2,3]");
    let path = JsonPath::parse("$[1]").unwrap();
    path.set(&mut doc, &document("100000"), SetMode::Always)
        .unwrap();
    assert_eq!(select(&doc, "$[*]"), json!([1, 100000, 3]));
    path.set(&mut doc, &document("1.125"), SetMode::Always)
        .unwrap();
    assert_eq!(select(&doc, "$[*]"), json!([1, 1.125, 3]));
    path.set(&mut doc, &document(r#"{"n":4}"#), SetMode::Always)
        .unwrap();
    assert_eq!(select(&doc, "$[*]"), json!([1,{"n":4},3]));
    assert_eq!(select(&doc, "$[1].n"), json!([4]));
}

#[test]
fn heterogeneous_append_reserves_after_promotion() {
    let mut array = document("[1,2,3,4]").into_array().unwrap();
    array.reserve(8).unwrap();
    array.push_heterogeneous(IValue::from("x")).unwrap();
    assert_eq!(
        serde_json::to_value(array).unwrap(),
        json!([1, 2, 3, 4, "x"])
    );
}

#[test]
fn depth_limit_can_skip_one_match_after_another_match_succeeds() {
    fn wrap(mut value: IValue, count: usize) -> IValue {
        for _ in 0..count {
            let mut object = ijson::IObject::new();
            object.insert("nested", value).unwrap();
            value = object.into();
        }
        value
    }
    let mut object = ijson::IObject::new();
    object.insert("shallow", document(r#"{"x":0}"#)).unwrap();
    object
        .insert("deep", wrap(document(r#"{"x":0}"#), 100))
        .unwrap();
    let mut doc = IValue::from(object);
    let replacement = wrap(IValue::from(9), 60);
    let result = JsonPath::parse("$..x")
        .unwrap()
        .set(&mut doc, &replacement, SetMode::Always)
        .unwrap();
    assert!(result.changed);
    assert!(!result.complete);
    assert_eq!(doc.get("shallow").unwrap().get("x"), Some(&replacement));
    assert_eq!(select(&doc, "$.deep..x"), json!([0]));
}

#[test]
fn nesting_and_projection_writes_are_rejected() {
    assert!(JsonPath::parse(&format!("{}$ {}", "(".repeat(5000), ")".repeat(5000))).is_err());
    for malformed in [
        "$.a +",
        "$.arr.length(",
        "($.a",
        "$.a * * 2",
        "$.a~b",
        "$.obj.keys()~",
    ] {
        assert!(JsonPath::parse(malformed).is_err(), "{malformed}");
    }
    let mut doc = document(r#"{"a":1}"#);
    assert!(
        JsonPath::parse("$.a + 1")
            .unwrap()
            .set(&mut doc, &document("9"), SetMode::Always)
            .is_err()
    );
    assert_eq!(select(&doc, "$.a"), json!([1]));
}
