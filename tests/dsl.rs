use serde_json::{Value, json};
use std::sync::Arc;
use tenant_kv::{
    dsl::{Prepared, Request},
    *,
};

fn setup() -> (tempfile::TempDir, Store, TenantHandle) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store
        .tenant(store.create_tenant("dsl").unwrap().id)
        .unwrap();
    t.create_collection("assets").unwrap();
    (dir, store, t)
}
fn run(t: &TenantHandle, text: &str, params: Value) -> Value {
    serde_json::to_value(
        t.execute_dsl(&Request {
            write_options: if text.starts_with("put ") || text.starts_with("delete ") {
                tenant_kv::WriteOptions {
                    retry_token: None,
                    expected_history: Some(t.history()),
                    operation_id: None,
                }
            } else {
                Default::default()
            },
            statement: text.into(),
            params: params.as_object().unwrap().clone(),
            cursor: None,
        })
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn commands_bind_values_and_keep_native_index_and_write_contracts() {
    let (_dir, _store, t) = setup();
    let plan = t.prepare_dsl("put assets $doc if_rev $rev").unwrap();
    for i in 0..5 {
        let args = json!({"doc":{"_key":format!("k{i}"),"ip":format!("ip-{i}"),"owner":"platform","score":i,"custom":{"a.b":"prod"}},"rev":0});
        plan.execute(&t, args.as_object().unwrap(), None).unwrap();
    }
    assert_eq!(run(&t, "get assets $key", json!({"key":"k0"}))["score"], 0);
    assert_eq!(
        run(&t, "mget assets [\"k1\",\"absent\",\"k1\"]", json!({}))[1],
        Value::Null
    );
    assert_eq!(run(&t, "get assets \"absent\"", json!({})), Value::Null);
    assert_eq!(run(&t, "mget assets []", json!({})), json!([]));
    t.create_index(
        "assets",
        IndexSpec {
            name: "ip".into(),
            fields: vec![IndexField {
                field: "/ip".into(),
                order: Order::Asc,
            }],
            unique: true,
            include: vec!["/owner".into()],
        },
    )
    .unwrap();
    t.create_lookup(
        "assets",
        LookupDefinition {
            name: "owners".into(),
            index: "ip".into(),
            select: vec!["/owner".into()],
            mode: MatchMode::All,
            max_matches: 1,
        },
    )
    .unwrap();
    let lookup = run(
        &t,
        "lookup assets using owners with $inputs",
        json!({"inputs":[{"ip":"ip-2"},{"ip":"none"},{"ip":"ip-2"},{}]}),
    );
    assert_eq!(
        lookup["matches"],
        json!([[{"owner":"platform"}],[],[{"owner":"platform"}],[]])
    );
    assert_eq!(lookup["stats"]["document_reads"], 0);
    let plan = run(
        &t,
        "explain from assets | where ip == $ip | fields owner",
        json!({"ip":"ip-2"}),
    );
    assert_eq!(plan["indexes"], json!(["ip"]));
    assert_eq!(plan["covering"], true);
    let rows = run(
        &t,
        "from assets | where (score >= 1 and score <= 3) and not exists(deleted) | sort score desc | fields _key, custom.a | limit $n",
        json!({"n":2}),
    );
    assert_eq!(rows["documents"], json!([{"_key":"k3"},{"_key":"k2"}]));
    let rows = run(
        &t,
        "from assets | where field(\"/custom/a.b\") == \"prod\" | fields custom | limit 10",
        json!({}),
    );
    assert_eq!(rows["documents"].as_array().unwrap().len(), 5);
    let current = t.get("assets", "k0").unwrap().unwrap();
    let changed = run(
        &t,
        "put assets $doc if_rev $rev",
        json!({"doc":{"_key":"k0","ip":"new","owner":"infra"},"rev":current["_rev"]}),
    );
    assert_eq!(
        t.write_batch_with(
            "assets",
            vec![tenant_kv::WriteOp::Put {
                document: json!({"_key":"k0"}),
                if_rev: current["_rev"].as_u64()
            }],
            &tenant_kv::WriteOptions {
                retry_token: None,
                expected_history: Some(t.history()),
                operation_id: None
            },
            "test"
        )
        .unwrap_err()
        .code,
        "REVISION_CONFLICT"
    );
    assert_eq!(
        run(
            &t,
            "lookup assets using owners with [{\"ip\":\"ip-0\"},{\"ip\":\"new\"}]",
            json!({})
        )["matches"],
        json!([[],[{"owner":"infra"}]])
    );
    run(
        &t,
        "delete assets $key if_rev $rev",
        json!({"key":"k0","rev":changed["rev"]}),
    );
    assert!(t.get("assets", "k0").unwrap().is_none());
}

#[test]
fn syntax_limits_parameter_injection_and_errors_never_execute_partial_commands() {
    let (_dir, _store, t) = setup();
    t.put("assets", json!({"_key":"keep","owner":"平台"}), None)
        .unwrap();
    for text in [
        "",
        "select * from assets",
        "get assets \"keep\"; delete assets \"keep\"",
        "put assets {\"_key\":\"bad\"} | limit 1",
        "from assets | limit 1 | where x == 1",
        "from assets | where x = 1",
        "from assets | fields $field",
        "from $collection",
        "from assets | where x == 1 or",
        "from assets | where exists(x",
        "get assets [",
        "get assets \"\u{1}\"",
    ] {
        let e = t
            .execute_dsl(&Request {
                write_options: Default::default(),
                statement: text.into(),
                params: Default::default(),
                cursor: None,
            })
            .unwrap_err();
        assert_eq!(e.code, "INVALID_ARGUMENT", "{text}: {e:?}");
        assert!(e.location.is_some(), "{text}");
    }
    assert!(t.get("assets", "keep").unwrap().is_some());
    assert!(t.get("assets", "bad").unwrap().is_none());
    let injection = "platform\" | limit 1; delete assets \"keep";
    assert_eq!(
        run(
            &t,
            "from assets | where owner == $owner",
            json!({"owner":injection})
        )["documents"],
        json!([])
    );
    for params in [
        json!({}),
        json!({"wrong":"keep"}),
        json!({"key":"keep","unused":1}),
    ] {
        assert!(
            Prepared::parse("get assets $key")
                .unwrap()
                .execute(&t, params.as_object().unwrap(), None)
                .is_err()
        );
    }
    for text in [
        format!("from assets | where {}exists(x)", "not ".repeat(10000)),
        format!(
            "from assets | where {}x == 1{}",
            "(".repeat(1000),
            ")".repeat(1000)
        ),
    ] {
        assert!(Prepared::parse(&text).is_err());
    }
    assert_eq!(
        Prepared::parse(&" ".repeat(65537)).unwrap_err().code,
        "RESOURCE_EXHAUSTED"
    );
    let err = Prepared::parse("from assets\n| where x = 1").unwrap_err();
    assert_eq!(err.location.unwrap().line, 2);
    assert!(Prepared::parse("# comment\nget \"assets\" \"#|;quotes\\\"\"").is_ok());
    let mut seed = 71u64;
    let alphabet = [
        'λ', '中', '"', '\\', '[', ']', '(', ')', '#', '$', '|', 'x', '0', ' ', '\n',
    ];
    for _ in 0..2000 {
        let s: String = (0..64)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                alphabet[seed as usize % alphabet.len()]
            })
            .collect();
        let _ = Prepared::parse(&s);
    }
}

#[test]
fn compiled_filters_keep_missing_null_types_nested_fields_and_boolean_precedence() {
    let (_dir, _store, t) = setup();
    let values = vec![
        json!(null),
        json!(false),
        json!(true),
        json!(-2),
        json!(0),
        json!(1),
        json!(1.0),
        json!(-0.0),
        json!(u64::MAX),
        json!(1e200),
        json!("1"),
        json!("λ\u{0}z"),
        json!([1]),
        json!({"a":1}),
    ];
    for (i, value) in values.iter().enumerate() {
        t.put(
            "assets",
            json!({"_key":format!("{i:03}"),"v":value,"nested":[{"a/b":value}]}),
            None,
        )
        .unwrap();
    }
    t.put("assets", json!({"_key":"missing"}), None).unwrap();
    t.engine().flush().unwrap();
    for op in ["==", "!=", ">", ">=", "<", "<="] {
        for expected in &values[..12] {
            let rows = run(
                &t,
                &format!(
                    "from assets | where field(\"/nested/0/a~1b\") {op} $v | fields _key | limit 100"
                ),
                json!({"v":expected}),
            );
            let mut wanted = Vec::new();
            for (i, actual) in values.iter().enumerate() {
                let yes = if actual.is_object() || actual.is_array() {
                    op == "!="
                } else {
                    let cmp = codec::compare(Some(actual), Some(expected)).unwrap();
                    let same = codec::same_type(Some(actual), Some(expected)).unwrap();
                    match op {
                        "==" => cmp.is_eq(),
                        "!=" => !cmp.is_eq(),
                        ">" => same && cmp.is_gt(),
                        ">=" => same && !cmp.is_lt(),
                        "<" => same && cmp.is_lt(),
                        _ => same && !cmp.is_gt(),
                    }
                };
                if yes {
                    wanted.push(json!({"_key":format!("{i:03}")}));
                }
            }
            if op == "!=" {
                wanted.push(json!({"_key":"missing"}));
            }
            assert_eq!(rows["documents"], Value::Array(wanted), "{op} {expected}");
        }
    }
    assert_eq!(
        run(
            &t,
            "from assets | where not exists(v) | fields _key",
            json!({})
        )["documents"],
        json!([{"_key":"missing"}])
    );
    assert_eq!(
        run(
            &t,
            "from assets | where v in [null,1,1.0,18446744073709551615] | fields _key",
            json!({})
        )["documents"],
        json!([{"_key":"000"},{"_key":"005"},{"_key":"006"},{"_key":"008"}])
    );
    assert_eq!(
        run(
            &t,
            "from assets | where v == 1 or v == 1.0 and exists(absent) | fields _key",
            json!({})
        )["documents"],
        json!([{"_key":"005"}])
    );
    assert!(
        t.execute_dsl(&Request {
            write_options: Default::default(),
            statement: "from assets | where v == 18446744073709551616".into(),
            params: Default::default(),
            cursor: None
        })
        .is_err()
    );
}

#[test]
fn pagination_preserves_snapshot_and_parameter_identity_across_cached_syntax() {
    let (_dir, store, t) = setup();
    for i in 0..4 {
        t.put("assets", json!({"_key":format!("k{i}"),"score":i}), None)
            .unwrap();
    }
    let text = "from assets | where score >= $min | fields _key | limit 2";
    let page = run(&t, text, json!({"min":0}));
    let cursor = page["cursor"].as_str().unwrap();
    t.delete("assets", "k2", None).unwrap();
    t.put("assets", json!({"_key":"k9","score":9}), None)
        .unwrap();
    let wrong = Request {
        write_options: Default::default(),
        statement: text.into(),
        params: json!({"min":1}).as_object().unwrap().clone(),
        cursor: Some(cursor.into()),
    };
    assert!(t.execute_dsl(&wrong).is_err());
    let other = store
        .tenant(store.create_tenant("other").unwrap().id)
        .unwrap();
    other.create_collection("assets").unwrap();
    let next = Request {
        write_options: Default::default(),
        statement: format!("# same query\n{text}"),
        params: json!({"min":0}).as_object().unwrap().clone(),
        cursor: Some(cursor.into()),
    };
    assert!(other.execute_dsl(&next).is_err());
    assert_eq!(
        serde_json::to_value(t.execute_dsl(&next).unwrap()).unwrap()["documents"],
        json!([{"_key":"k2"},{"_key":"k3"}])
    );
    assert!(t.execute_dsl(&next).is_err());
    assert!(
        Prepared::parse("get assets \"k1\"")
            .unwrap()
            .execute(&t, &Default::default(), Some(cursor))
            .is_err()
    );
}

#[test]
fn bounded_tenant_caches_bind_fresh_values_and_survive_catalog_changes() {
    let (_dir, store, t) = setup();
    let other = store
        .tenant(store.create_tenant("other").unwrap().id)
        .unwrap();
    other.create_collection("assets").unwrap();
    t.put("assets", json!({"_key":"k","value":"a"}), None)
        .unwrap();
    other
        .put("assets", json!({"_key":"k","value":"b"}), None)
        .unwrap();
    let text = "get assets $key";
    let plan = t.prepare_dsl(text).unwrap();
    assert!(Arc::ptr_eq(&plan, &t.prepare_dsl(text).unwrap()));
    assert!(!Arc::ptr_eq(&plan, &other.prepare_dsl(text).unwrap()));
    assert_eq!(run(&t, text, json!({"key":"k"}))["value"], "a");
    assert_eq!(run(&other, text, json!({"key":"k"}))["value"], "b");
    assert_eq!(run(&t, text, json!({"key":"none"})), Value::Null);
    t.drop_collection("assets").unwrap();
    t.create_collection("assets").unwrap();
    assert_eq!(run(&t, text, json!({"key":"k"})), Value::Null);
    for i in 0..1000 {
        t.prepare_dsl(&format!("get assets \"k{i}\"")).unwrap();
    }
    let stats = t.dsl_cache_stats();
    assert!(stats["entries"].as_u64().unwrap() <= 256);
    assert!(stats["reserved_bytes"].as_u64().unwrap() <= 4 << 20);
    // A caller retaining a preparation after cache eviction still keeps its
    // reservation alive; preparation never pins a tenant engine or snapshot.
    assert!(store.memory_bytes() > 0);
    drop(plan);
}

#[test]
fn catalog_cache_never_uses_new_indexes_for_an_old_cursor() {
    let (_dir, _store, t) = setup();
    for i in 0..4 {
        t.put(
            "assets",
            json!({"_key":format!("k{i}"),"owner":"team"}),
            None,
        )
        .unwrap();
    }
    let statement = "from assets | where owner == \"team\" | fields _key | limit 1";
    let first = run(&t, statement, json!({}));
    assert_eq!(first["plan"]["scan"], true);
    t.create_index(
        "assets",
        IndexSpec {
            name: "owner".into(),
            fields: vec![IndexField {
                field: "/owner".into(),
                order: Order::Asc,
            }],
            unique: false,
            include: vec![],
        },
    )
    .unwrap();
    let new = run(&t, statement, json!({}));
    assert_eq!(new["plan"]["indexes"], json!(["owner"]));
    t.release_cursor(new["cursor"].as_str().unwrap()).unwrap();
    let next = Request {
        write_options: Default::default(),
        statement: statement.into(),
        params: Default::default(),
        cursor: Some(first["cursor"].as_str().unwrap().into()),
    };
    let next = serde_json::to_value(t.execute_dsl(&next).unwrap()).unwrap();
    assert_eq!(next["plan"]["scan"], true);
    assert_eq!(next["documents"], json!([{"_key":"k1"}]));
    t.release_cursor(next["cursor"].as_str().unwrap()).unwrap();
    t.drop_index("assets", "owner").unwrap();
    let plan = run(
        &t,
        "explain from assets | where owner == \"team\" | fields _key",
        json!({}),
    );
    assert_eq!(plan["scan"], true);
}

#[test]
fn repeated_large_bindings_are_budgeted_before_expansion() {
    let (_dir, store, t) = setup();
    let text = format!(
        "from assets | where {}",
        vec!["value == $payload"; 100].join(" or ")
    );
    let plan = t.prepare_dsl(&text).unwrap();
    let params = json!({"payload":"x".repeat(1<<20)});
    let before = store.memory_bytes();
    let error = plan
        .execute(&t, params.as_object().unwrap(), None)
        .unwrap_err();
    assert_eq!(error.code, "RESOURCE_EXHAUSTED");
    assert_eq!(before, store.memory_bytes());
    let mut nested = json!(0);
    for _ in 0..80 {
        nested = json!([nested]);
    }
    assert_eq!(
        Prepared::parse("put assets $doc")
            .unwrap()
            .execute(&t, json!({"doc":nested}).as_object().unwrap(), None)
            .unwrap_err()
            .code,
        "RESOURCE_EXHAUSTED"
    );
}
