use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tenant_kv::*;

fn options() -> Options {
    Options {
        global_memory_bytes: 256 << 20,
        tenant_memory_bytes: 128 << 20,
        cache_bytes: 8 << 20,
        memtable_bytes: 128 << 10,
        block_bytes: 4096,
        target_sst_bytes: 64 << 10,
        level_base_bytes: 256 << 10,
        l0_trigger: 3,
        l0_stop: 64,
        ..Options::default()
    }
}
fn setup() -> (tempfile::TempDir, Store, TenantHandle) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options()).unwrap();
    let info = store.create_tenant("alpha").unwrap();
    let tenant = store.tenant(info.id).unwrap();
    tenant.create_collection("assets").unwrap();
    (dir, store, tenant)
}
fn index(name: &str, fields: &[(&str, Order)], unique: bool) -> IndexSpec {
    IndexSpec {
        name: name.into(),
        fields: fields
            .iter()
            .map(|(f, o)| IndexField {
                field: (*f).into(),
                order: o.clone(),
            })
            .collect(),
        unique,
        include: vec!["/owner".into(), "/env".into()],
    }
}
fn lookup(name: &str, index: &str) -> LookupDefinition {
    LookupDefinition {
        name: name.into(),
        index: index.into(),
        select: vec!["/_key".into(), "/owner".into()],
        mode: MatchMode::All,
        max_matches: 100,
    }
}
fn query(value: Value) -> Query {
    serde_json::from_value(value).unwrap()
}

#[test]
fn binary_document_precision_projection_and_types() {
    let doc: Value = serde_json::from_str(r#"{"max":18446744073709551615,"min":-9223372036854775808,"float":1.0,"a/b":{"~x":true},"array":[null,2]}"#).unwrap();
    let bytes = codec::encode(&doc).unwrap();
    assert_eq!(codec::decode(&bytes).unwrap(), doc);
    assert_eq!(
        codec::field(&bytes, "/a~1b/~0x").unwrap(),
        Some(json!(true))
    );
    assert_eq!(codec::field(&bytes, "/missing").unwrap(), None);
    assert_eq!(codec::field(&bytes, "/array/0").unwrap(), Some(Value::Null));
    assert_ne!(
        codec::scalar(Some(&json!(1)), &Order::Asc).unwrap(),
        codec::scalar(Some(&json!(1.0)), &Order::Asc).unwrap()
    );
    let invalid: Value = serde_json::from_str(r#"{"value":18446744073709551616}"#).unwrap();
    assert!(codec::encode(&invalid).is_err());
    for n in [-100, -1, 0, 1, 100] {
        assert_eq!(
            codec::compare(Some(&json!(n)), Some(&json!(n + 1))).unwrap(),
            std::cmp::Ordering::Less
        );
    }
}

#[test]
fn tenant_files_keys_and_unique_values_are_isolated() {
    let (dir, store, a) = setup();
    let b_info = store.create_tenant("../beta").unwrap();
    let b = store.tenant(b_info.id).unwrap();
    b.create_collection("assets").unwrap();
    for (tenant, owner) in [(&a, "A"), (&b, "B")] {
        tenant
            .create_index("assets", index("ip", &[("/ip", Order::Asc)], true))
            .unwrap();
        tenant
            .put(
                "assets",
                json!({"_key":"same","ip":"10.0.0.1","owner":owner,"tenant":"spoof"}),
                None,
            )
            .unwrap();
        tenant.engine().flush().unwrap();
    }
    assert_eq!(a.get("assets", "same").unwrap().unwrap()["owner"], "A");
    assert_eq!(b.get("assets", "same").unwrap().unwrap()["owner"], "B");
    assert_ne!(a.engine().path(), b.engine().path());
    assert!(
        dir.path()
            .join("tenants")
            .join(a.id().to_string())
            .join("wal")
            .is_dir()
    );
    assert_eq!(
        Store::open(dir.path(), options()).err().unwrap().code,
        "BUSY"
    );
    let id = a.id();
    drop(a);
    drop(b);
    drop(store);
    let store = Store::open(dir.path(), options()).unwrap();
    assert_eq!(
        store
            .tenant(id)
            .unwrap()
            .get("assets", "same")
            .unwrap()
            .unwrap()["owner"],
        "A"
    );
}

#[test]
fn atomic_indexes_unique_conflicts_and_conditional_updates() {
    let (_dir, _store, t) = setup();
    t.create_index("assets", index("ip", &[("/ip", Order::Asc)], true))
        .unwrap();
    let first = t
        .put(
            "assets",
            json!({"_key":"a","ip":"1","owner":"old"}),
            Some(0),
        )
        .unwrap();
    let result = t.write_batch(
        "assets",
        vec![
            WriteOp::Put {
                document: json!({"_key":"b","ip":"2"}),
                if_rev: None,
            },
            WriteOp::Put {
                document: json!({"_key":"c","ip":"1"}),
                if_rev: None,
            },
        ],
    );
    assert_eq!(result.unwrap_err().code, "UNIQUE_CONFLICT");
    assert!(t.get("assets", "b").unwrap().is_none());
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for owner in ["left", "right"] {
        let t = t.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            t.write_batch_with(
                "assets",
                vec![WriteOp::Put {
                    document: json!({"_key":"a","ip":"1","owner":owner}),
                    if_rev: Some(first.rev),
                }],
                &WriteOptions {
                    retry_token: None,
                    expected_history: Some(t.history()),
                    operation_id: None,
                },
                "test",
            )
        }));
    }
    barrier.wait();
    let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter_map(|r| r.as_ref().err())
            .next()
            .unwrap()
            .code,
        "REVISION_CONFLICT"
    );
    t.create_lookup("assets", lookup("owner", "ip")).unwrap();
    let result = t
        .lookup_many(
            "assets",
            "owner",
            &[
                json!({"ip":"1"}),
                json!({"ip":"missing"}),
                json!({"ip":"1"}),
            ],
        )
        .unwrap();
    assert_eq!(result.matches[0], result.matches[2]);
    assert!(result.matches[1].is_empty());
    assert_eq!(result.stats.document_reads, 0);
    let mut replacement = lookup("owner", "ip");
    replacement.select = vec!["/_key".into()];
    t.create_lookup("assets", replacement).unwrap();
    assert_eq!(
        t.lookup_many("assets", "owner", &[json!({"ip":"1"})])
            .unwrap()
            .matches,
        vec![vec![json!({"_key":"a"})]]
    );
    t.delete("assets", "a", None).unwrap();
    t.engine().flush().unwrap();
    t.engine().compact(true).unwrap();
    assert!(
        t.lookup_many("assets", "owner", &[json!({"ip":"1"})])
            .unwrap()
            .matches[0]
            .is_empty()
    );
}

#[test]
fn covering_ranges_sparse_unique_and_boolean_queries() {
    let (_dir, _store, t) = setup();
    t.write_batch("assets",(0..200).map(|i|WriteOp::Put{document:json!({"_key":format!("{i:04}"),"env":if i%2==0{"prod"}else{"dev"},"score":i,"owner":format!("owner-{i}")}),if_rev:None}).collect()).unwrap();
    t.create_index(
        "assets",
        index(
            "env-score",
            &[("/env", Order::Asc), ("/score", Order::Desc)],
            false,
        ),
    )
    .unwrap();
    t.create_index(
        "assets",
        index("sparse", &[("/optional", Order::Asc)], true),
    )
    .unwrap();
    t.engine().flush().unwrap();
    let result=t.query("assets",query(json!({"filter":{"and":[{"field":"/env","op":"eq","value":"prod"},{"field":"/score","op":"gte","value":180}]},"sort":[{"field":"/score","order":"desc"}],"select":["/_key","/owner"],"limit":100}))).unwrap();
    assert_eq!(result.documents.len(), 10);
    assert_eq!(result.documents[0]["_key"], "0198");
    assert_eq!(result.stats.document_reads, 0);
    assert!(!result.plan.extra_sort);
    let missing = t
        .query(
            "assets",
            query(json!({"filter":{"field":"/optional","op":"exists","value":false},"limit":1000})),
        )
        .unwrap();
    assert_eq!(missing.documents.len(), 200);
    assert!(missing.plan.scan);
    let union=t.query("assets",query(json!({"filter":{"or":[{"field":"/_key","op":"in","value":["0001","0001","0002"]},{"field":"/_key","op":"eq","value":"0002"}]},"limit":100}))).unwrap();
    assert_eq!(union.documents.len(), 2);
}

#[test]
fn bounded_primary_ranges_keep_snapshot_and_encoded_key_boundaries() {
    let (_dir, _store, t) = setup();
    let keys = ["a", "a\0", "a\0z", "aa", "b", "é", "中"];
    for key in keys {
        t.put("assets", json!({"_key":key,"owner":"old"}), None)
            .unwrap();
    }
    t.engine().flush().unwrap();
    for (lo, hi) in [("gt", "lte"), ("gte", "lt"), ("gte", "lte"), ("gt", "lt")] {
        let q = query(
            json!({"filter":{"and":[{"field":"/_key","op":lo,"value":"a\0"},{"field":"/_key","op":hi,"value":"é"}]},"select":["/_key"],"limit":100}),
        );
        let result = t.query("assets", q).unwrap();
        let expected: Vec<_> = keys
            .iter()
            .filter(|&&k| {
                (if lo == "gt" { k > "a\0" } else { k >= "a\0" })
                    && (if hi == "lt" { k < "é" } else { k <= "é" })
            })
            .map(|k| json!({"_key":k}))
            .collect();
        assert_eq!(result.documents, expected);
        assert_eq!(result.plan.indexes, ["_key"]);
        assert!(!result.plan.scan);
    }
    let mut q = query(json!({"filter":{"field":"/_key","op":"gte","value":"aa"},"limit":1}));
    let first = t.query("assets", q.clone()).unwrap();
    let mut documents = first.documents;
    q.cursor = first.cursor;
    t.delete("assets", "b", None).unwrap();
    t.put("assets", json!({"_key":"é","owner":"new"}), None)
        .unwrap();
    t.engine().flush().unwrap();
    t.engine().compact(true).unwrap();
    while q.cursor.is_some() {
        let page = t.query("assets", q.clone()).unwrap();
        documents.extend(page.documents);
        q.cursor = page.cursor;
    }
    assert_eq!(
        documents
            .iter()
            .map(|v| v["_key"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["aa", "b", "é", "中"]
    );
    assert!(documents.iter().all(|v| v["owner"] == "old"));
    let empty = t.query("assets", query(json!({"filter":{"and":[{"field":"/_key","op":"gt","value":"z"},{"field":"/_key","op":"lt","value":"a"}]}}))).unwrap();
    assert!(empty.documents.is_empty());
    assert_eq!(empty.stats.scanned_entries, 0);
}

#[test]
fn index_multi_ranges_match_scan_with_sparse_and_typed_values() {
    let (_dir, _store, t) = setup();
    t.create_collection("reference").unwrap();
    t.create_collection("compound").unwrap();
    let values = [
        json!(null),
        json!(-2),
        json!(1),
        json!(1.0),
        json!(18446744073709551615u64),
        json!("a\0"),
        json!("中"),
        json!(false),
    ];
    for (i, value) in values.into_iter().enumerate() {
        let doc = json!({"_key":format!("{i:04}"),"value":value,"group":i%2,"owner":format!("owner-{i}")});
        for collection in ["assets", "reference", "compound"] {
            t.put(collection, doc.clone(), None).unwrap();
        }
    }
    for collection in ["assets", "reference", "compound"] {
        t.put(collection, json!({"_key":"missing","group":0}), None)
            .unwrap();
    }
    t.create_index(
        "compound",
        index(
            "sparse-compound",
            &[("/group", Order::Asc), ("/value", Order::Desc)],
            true,
        ),
    )
    .unwrap();
    for (name, fields, unique) in [
        ("value", vec![("/value", Order::Desc)], true),
        (
            "group-value",
            vec![("/group", Order::Asc), ("/value", Order::Desc)],
            false,
        ),
    ] {
        t.create_index("assets", index(name, &fields, unique))
            .unwrap();
    }
    t.engine().flush().unwrap();
    t.engine().compact(true).unwrap();
    let mut predicates = vec![
        json!({"field":"/value","op":"in","value":[1,1,1.0,"中"]}),
        json!({"field":"/value","op":"in","value":[null,1]}),
        json!({"field":"/value","op":"in","value":[]}),
        json!({"field":"/value","op":"gte","value":-2}),
        json!({"field":"/value","op":"lte","value":null}),
        json!({"field":"/value","op":"exists","value":false}),
        json!({"and":[{"field":"/group","op":"in","value":[0,1,0]},{"field":"/value","op":"in","value":[null,-2,1,1.0,"中"]}]}),
        json!({"and":[{"field":"/group","op":"in","value":[1,0]},{"field":"/value","op":"gte","value":-2},{"field":"/value","op":"lt","value":2}]}),
        json!({"and":[{"field":"/group","op":"eq","value":0},{"field":"/value","op":"gte","value":2},{"field":"/value","op":"lt","value":-2}]}),
        json!({"or":[{"field":"/value","op":"in","value":[1,1.0]},{"field":"/value","op":"gte","value":-2}]}),
    ];
    predicates.push(json!({"field":"/group","op":"in","value":(0..300).collect::<Vec<_>>()}));
    predicates.push(json!({"and":[{"field":"/group","op":"in","value":(0..17).collect::<Vec<_>>()},{"field":"/value","op":"in","value":(0..17).collect::<Vec<_>>()}]}));
    predicates.push(json!({"and":[{"field":"/group","op":"eq","value":0},{"field":"/value","op":"gte","value":0}]}));
    for filter in predicates {
        for sort in [json!([]), json!([{"field":"/value","order":"desc"}])] {
            let q = query(
                json!({"filter":filter,"sort":sort,"select":["/_key","/owner","/value"],"limit":100}),
            );
            let expected = t.query("reference", q.clone()).unwrap().documents;
            let result = t.query("assets", q.clone()).unwrap();
            assert_eq!(result.documents, expected, "filter={filter}");
            let compound = t.query("compound", q.clone()).unwrap();
            assert_eq!(
                compound.documents, expected,
                "sparse compound filter={filter}"
            );
            let mut paged = q;
            paged.limit = 2;
            let mut rows = Vec::new();
            loop {
                let page = t.query("assets", paged.clone()).unwrap();
                rows.extend(page.documents);
                paged.cursor = page.cursor;
                if paged.cursor.is_none() {
                    break;
                }
            }
            assert_eq!(rows, expected);
        }
    }
    let indexed = t.query("assets",query(json!({"filter":{"field":"/value","op":"in","value":[1,1.0]},"select":["/_key","/owner"]}))).unwrap();
    assert_eq!(indexed.plan.indexes, ["value"]);
    assert_eq!(indexed.stats.document_reads, 0);
    assert_eq!(indexed.documents.len(), 2);
    let selective = t.query("assets", query(json!({"filter":{"and":[{"field":"/_key","op":"gte","value":"0000"},{"field":"/value","op":"eq","value":1}]}}))).unwrap();
    assert_eq!(selective.plan.indexes, ["value"]);
    assert_eq!(selective.documents.len(), 1);
}

#[test]
fn batched_nonunique_ranges_reuse_blocks_and_preserve_limits() {
    let (_dir, _store, t) = setup();
    t.create_index("assets", index("group", &[("/group", Order::Asc)], false))
        .unwrap();
    for batch in 0..4 {
        t.write_batch(
            "assets",
            (0..100)
                .map(|j| {
                    let i = batch * 100 + j;
                    WriteOp::Put {
                        document: json!({"_key":format!("{i:04}"),"group":i/2,"owner":i}),
                        if_rev: None,
                    }
                })
                .collect(),
        )
        .unwrap();
    }
    t.create_lookup("assets", lookup("groups", "group"))
        .unwrap();
    let mut first = lookup("first", "group");
    first.mode = MatchMode::First;
    t.create_lookup("assets", first).unwrap();
    let mut limited = lookup("limited", "group");
    limited.max_matches = 1;
    t.create_lookup("assets", limited).unwrap();
    t.engine().flush().unwrap();
    t.engine().compact(true).unwrap();
    let inputs: Vec<_> = (0..200)
        .rev()
        .map(|i| json!({"group":i}))
        .chain([json!({"group":1}), json!({"group":999}), json!({})])
        .collect();
    let result = t.lookup_many("assets", "groups", &inputs).unwrap();
    for (i, rows) in result.matches[..200].iter().enumerate() {
        let n = (199 - i) * 2;
        assert_eq!(
            rows,
            &vec![
                json!({"_key":format!("{n:04}"),"owner":n}),
                json!({"_key":format!("{:04}",n+1),"owner":n+1})
            ]
        );
    }
    assert_eq!(result.matches[200], result.matches[198]);
    assert!(result.matches[201..].iter().all(Vec::is_empty));
    assert!(result.stats.blocks_read < 100, "{:?}", result.stats);
    assert_eq!(
        t.lookup_many("assets", "limited", &inputs)
            .unwrap_err()
            .code,
        "MATCH_LIMIT_EXCEEDED"
    );
    let first = t.lookup_many("assets", "first", &inputs).unwrap();
    assert!(first.matches[..201].iter().all(|rows| rows.len() == 1));
    t.delete("assets", "0002", None).unwrap();
    t.put(
        "assets",
        json!({"_key":"0003","group":201,"owner":"moved"}),
        None,
    )
    .unwrap();
    t.engine().flush().unwrap();
    let result = t
        .lookup_many(
            "assets",
            "groups",
            &[json!({"group":1}), json!({"group":201})],
        )
        .unwrap();
    assert!(result.matches[0].is_empty());
    assert_eq!(
        result.matches[1],
        vec![json!({"_key":"0003","owner":"moved"})]
    );
}

#[test]
fn pagination_keeps_snapshot_during_compaction_and_updates() {
    let (_dir, _store, t) = setup();
    for i in 0..8 {
        t.put("assets", json!({"_key":format!("{i}"),"value":i}), None)
            .unwrap();
    }
    t.engine().flush().unwrap();
    let mut q = Query {
        limit: 2,
        ..Query::default()
    };
    let first = t.query("assets", q.clone()).unwrap();
    let mut rows = first.documents;
    q.cursor = first.cursor;
    t.delete("assets", "3", None).unwrap();
    t.put("assets", json!({"_key":"4","value":400}), None)
        .unwrap();
    t.put("assets", json!({"_key":"9","value":9}), None)
        .unwrap();
    t.engine().flush().unwrap();
    t.engine().compact(true).unwrap();
    while q.cursor.is_some() {
        let page = t.query("assets", q.clone()).unwrap();
        rows.extend(page.documents);
        q.cursor = page.cursor;
    }
    assert_eq!(rows.len(), 8);
    assert_eq!(rows[3]["value"], 3);
    assert_eq!(rows[4]["value"], 4);
    assert!(t.get("assets", "3").unwrap().is_none());
    assert_eq!(t.get("assets", "4").unwrap().unwrap()["value"], 400);
}

#[test]
fn model_comparison_over_flush_compaction_and_recovery() {
    let (dir, store, t) = setup();
    let id = t.id();
    let mut expected = BTreeMap::new();
    let mut random = 17u64;
    for step in 0..300 {
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = format!("{:03}", (random >> 32) % 50);
        if random.is_multiple_of(4) {
            t.delete("assets", &key, None).unwrap();
            expected.remove(&key);
        } else {
            let n = (random >> 16) % 1000;
            t.put("assets", json!({"_key":key,"n":n}), None).unwrap();
            expected.insert(key, n);
        }
        if step % 31 == 0 {
            t.engine().flush().unwrap();
        }
        if step % 71 == 0 {
            t.engine().compact(true).unwrap();
        }
    }
    drop(t);
    drop(store);
    let store = Store::open(dir.path(), options()).unwrap();
    let t = store.tenant(id).unwrap();
    let rows = t
        .query(
            "assets",
            Query {
                limit: 1000,
                ..Query::default()
            },
        )
        .unwrap()
        .documents;
    let actual: BTreeMap<_, _> = rows
        .iter()
        .map(|v| {
            (
                v["_key"].as_str().unwrap().to_string(),
                v["n"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(actual, expected);
    t.engine().verify().unwrap();
}

#[test]
fn backup_restore_does_not_change_other_tenants() {
    let (_dir, store, a) = setup();
    let aid = a.id();
    let bid = store.create_tenant("beta").unwrap().id;
    let b = store.tenant(bid).unwrap();
    b.create_collection("assets").unwrap();
    a.put("assets", json!({"_key":"a","value":"before"}), None)
        .unwrap();
    b.put("assets", json!({"_key":"b","value":"other"}), None)
        .unwrap();
    let backup = store.backup(aid).unwrap();
    a.put("assets", json!({"_key":"a","value":"after"}), None)
        .unwrap();
    drop(a);
    for attempt in 0..100 {
        match store.restore(aid, backup) {
            Ok(()) => break,
            Err(e) if e.code == "BUSY" && attempt < 99 => {
                std::thread::sleep(Duration::from_millis(20))
            }
            Err(e) => panic!("{e}"),
        }
    }
    assert_eq!(
        store
            .tenant(aid)
            .unwrap()
            .get("assets", "a")
            .unwrap()
            .unwrap()["value"],
        "before"
    );
    assert_eq!(b.get("assets", "b").unwrap().unwrap()["value"], "other");
    assert!(store.restore(bid, backup).is_err());
}

#[test]
fn online_index_build_tracks_concurrent_changes() {
    let (_dir, _store, t) = setup();
    for batch in 0..10 {
        t.write_batch("assets",(0..100).map(|i|{let n=batch*100+i;WriteOp::Put{document:json!({"_key":format!("{n:04}"),"ip":format!("ip-{n}"),"owner":"before"}),if_rev:None}}).collect()).unwrap();
    }
    let writer = t.clone();
    let work = std::thread::spawn(move || {
        for i in 0..40 {
            writer
                .put(
                    "assets",
                    json!({"_key":format!("{i:04}"),"ip":format!("new-{i}"),"owner":"after"}),
                    None,
                )
                .unwrap();
        }
    });
    t.create_index("assets", index("ip", &[("/ip", Order::Asc)], true))
        .unwrap();
    work.join().unwrap();
    t.create_lookup("assets", lookup("owner", "ip")).unwrap();
    let inputs: Vec<_> = (0..40).map(|i| json!({"ip":format!("new-{i}")})).collect();
    let result = t.lookup_many("assets", "owner", &inputs).unwrap();
    assert!(
        result
            .matches
            .iter()
            .all(|r| r.len() == 1 && r[0]["owner"] == "after")
    );
    assert!(
        t.lookup_many("assets", "owner", &[json!({"ip":"ip-0"})])
            .unwrap()
            .matches[0]
            .is_empty()
    );
}

#[test]
fn array_projection_preserves_shape_in_covering_indexes() {
    let (_dir, _store, t) = setup();
    let document = json!({"_key":"a","items":[{"name":"first"},{"name":"second","extra":10}],"numeric":{"0":"object-key"}});
    let encoded = codec::encode(&document).unwrap();
    assert_eq!(
        codec::project(&encoded, &["/items/1/name".into(), "/numeric/0".into()]).unwrap(),
        json!({"items":[null,{"name":"second"}],"numeric":{"0":"object-key"}})
    );
    t.put("assets", document, None).unwrap();
    t.create_index(
        "assets",
        IndexSpec {
            name: "nested".into(),
            fields: vec![IndexField {
                field: "/items/1/name".into(),
                order: Order::Asc,
            }],
            unique: false,
            include: vec!["/numeric".into()],
        },
    )
    .unwrap();
    t.engine().flush().unwrap();
    let result = t.query("assets", query(json!({"filter":{"field":"/items/1/name","op":"eq","value":"second"},"select":["/items/1/name","/numeric/0"]}))).unwrap();
    assert_eq!(result.stats.document_reads, 0);
    assert_eq!(
        result.documents[0],
        json!({"items":[null,{"name":"second"}],"numeric":{"0":"object-key"}})
    );
    assert!(
        t.put(
            "assets",
            json!({"_key":"a","items":[null,{"name":{"bad":true}}]}),
            None
        )
        .is_err()
    );
    assert_eq!(
        t.get("assets", "a").unwrap().unwrap()["items"][1]["name"],
        "second"
    );
}

#[test]
fn indexed_ranges_match_reference_with_negative_values_and_ties() {
    let (_dir, _store, t) = setup();
    let mut reference = Vec::new();
    for i in 0..300 {
        let row = json!({"_key":format!("{i:04}"),"group":if i%3==0{"a\u{0}b"}else{"a"},"n":(i*17%71)-35});
        reference.push(row);
    }
    t.write_batch(
        "assets",
        reference
            .iter()
            .cloned()
            .map(|document| WriteOp::Put {
                document,
                if_rev: None,
            })
            .collect(),
    )
    .unwrap();
    for order in [Order::Asc, Order::Desc] {
        let name = if order == Order::Asc { "asc" } else { "desc" };
        t.create_index(
            "assets",
            index(
                name,
                &[("/group", Order::Asc), ("/n", order.clone())],
                false,
            ),
        )
        .unwrap();
        t.engine().flush().unwrap();
        t.engine().compact(true).unwrap();
        for bound in [-36, -17, 0, 19, 36] {
            for op in ["gt", "gte", "lt", "lte"] {
                let mut expected: Vec<_> = reference
                    .iter()
                    .filter(|r| {
                        r["group"] == "a\u{0}b"
                            && match op {
                                "gt" => r["n"].as_i64().unwrap() > bound,
                                "gte" => r["n"].as_i64().unwrap() >= bound,
                                "lt" => r["n"].as_i64().unwrap() < bound,
                                _ => r["n"].as_i64().unwrap() <= bound,
                            }
                    })
                    .collect();
                expected.sort_by(|a, b| {
                    let compare = a["n"].as_i64().cmp(&b["n"].as_i64());
                    (if order == Order::Asc {
                        compare
                    } else {
                        compare.reverse()
                    })
                    .then(a["_key"].as_str().cmp(&b["_key"].as_str()))
                });
                let result=t.query("assets",query(json!({"filter":{"and":[{"field":"/group","op":"eq","value":"a\u{0}b"},{"field":"/n","op":op,"value":bound}]},"sort":[{"field":"/n","order":order}],"select":["/_key"],"limit":1000}))).unwrap();
                let actual: Vec<_> = result
                    .documents
                    .iter()
                    .map(|r| r["_key"].as_str().unwrap())
                    .collect();
                assert_eq!(
                    actual,
                    expected
                        .iter()
                        .map(|r| r["_key"].as_str().unwrap())
                        .collect::<Vec<_>>()
                );
            }
        }
    }
}

#[test]
fn quota_scan_and_expired_cursor_leave_other_tenants_usable() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options();
    opts.snapshot_seconds = 1;
    opts.max_scan = 12;
    opts.max_document_bytes = 2048;
    opts.max_read_keys = 4;
    opts.tenant_disk_bytes = 2 << 20;
    let store = Store::open(dir.path(), opts.clone()).unwrap();
    let a = store
        .tenant(store.create_tenant("limited").unwrap().id)
        .unwrap();
    let b = store
        .tenant(store.create_tenant("healthy").unwrap().id)
        .unwrap();
    a.create_collection("assets").unwrap();
    b.create_collection("assets").unwrap();
    a.write_batch(
        "assets",
        (0..30)
            .map(|i| WriteOp::Put {
                document: json!({"_key":format!("{i:04}"),"n":i}),
                if_rev: None,
            })
            .collect(),
    )
    .unwrap();
    assert_eq!(
        a.query(
            "assets",
            query(json!({"sort":[{"field":"/n","order":"desc"}],"limit":2}))
        )
        .unwrap_err()
        .code,
        "RESOURCE_EXHAUSTED"
    );
    assert_eq!(
        a.multi_get(
            "assets",
            &["x".into(), "x".into(), "x".into(), "x".into(), "x".into()]
        )
        .unwrap_err()
        .code,
        "RESOURCE_EXHAUSTED"
    );
    assert_eq!(
        a.put("assets", json!({"payload":"x".repeat(3000)}), None)
            .unwrap_err()
            .code,
        "RESOURCE_EXHAUSTED"
    );
    let page = a
        .query(
            "assets",
            Query {
                limit: 2,
                ..Query::default()
            },
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(1100));
    assert_eq!(
        a.query(
            "assets",
            Query {
                limit: 2,
                cursor: page.cursor,
                ..Query::default()
            }
        )
        .unwrap_err()
        .code,
        "CURSOR_EXPIRED"
    );
    let mut exhausted = false;
    for i in 0..3000 {
        let mut seed = i as u64 + 717;
        let payload: String = (0..1200)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                (33 + (seed % 90) as u8) as char
            })
            .collect();
        match a.put(
            "assets",
            json!({"_key":format!("large-{i}"),"payload":payload}),
            None,
        ) {
            Ok(_) => {}
            Err(e) => {
                assert_eq!(e.code, "RESOURCE_EXHAUSTED");
                exhausted = true;
                break;
            }
        }
    }
    assert!(exhausted);
    assert!(store.memory_bytes() <= opts.global_memory_bytes);
    b.put("assets", json!({"_key":"ok"}), None).unwrap();
    assert!(b.get("assets", "ok").unwrap().is_some());
}

#[test]
fn failed_unique_build_can_be_repaired_and_nonunique_lookup_is_bounded() {
    let (_dir, _store, t) = setup();
    t.write_batch(
        "assets",
        (0..120)
            .map(|i| WriteOp::Put {
                document: json!({"_key":format!("{i:03}"),"ip":"same","owner":i}),
                if_rev: None,
            })
            .collect(),
    )
    .unwrap();
    assert_eq!(
        t.create_index("assets", index("unique", &[("/ip", Order::Asc)], true))
            .unwrap_err()
            .code,
        "UNIQUE_CONFLICT"
    );
    assert_eq!(t.indexes("assets").unwrap()[0].state, IndexState::Failed);
    t.create_index("assets", index("many", &[("/ip", Order::Asc)], false))
        .unwrap();
    t.create_lookup("assets", lookup("all", "many")).unwrap();
    t.engine().flush().unwrap();
    t.engine().compact(true).unwrap();
    assert_eq!(
        t.lookup_many("assets", "all", &[json!({"ip":"same"})])
            .unwrap_err()
            .code,
        "MATCH_LIMIT_EXCEEDED"
    );
    let mut first = lookup("first", "many");
    first.mode = MatchMode::First;
    t.create_lookup("assets", first).unwrap();
    assert_eq!(
        t.lookup_many("assets", "first", &[json!({"ip":"same"})])
            .unwrap()
            .matches[0][0]["_key"],
        "000"
    );
    t.write_batch(
        "assets",
        (1..120)
            .map(|i| WriteOp::Delete {
                key: format!("{i:03}"),
                if_rev: None,
            })
            .collect(),
    )
    .unwrap();
    assert_eq!(
        t.create_index("assets", index("unique", &[("/ip", Order::Asc)], true))
            .unwrap()
            .state,
        IndexState::Ready
    );
}

#[test]
fn concurrent_group_commits_survive_reopen() {
    let (dir, store, t) = setup();
    let id = t.id();
    let mut threads = Vec::new();
    for n in 0..8 {
        let t = t.clone();
        threads.push(std::thread::spawn(move || {
            for i in 0..50 {
                t.put(
                    "assets",
                    json!({"_key":format!("{n}-{i}"),"n":n,"i":i}),
                    Some(0),
                )
                .unwrap();
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    drop(t);
    drop(store);
    let store = Store::open(dir.path(), options()).unwrap();
    let t = store.tenant(id).unwrap();
    assert_eq!(
        t.query(
            "assets",
            Query {
                limit: 1000,
                ..Query::default()
            }
        )
        .unwrap()
        .documents
        .len(),
        400
    );
}

#[test]
fn live_handles_and_snapshots_keep_manager_ownership_after_store_drop() {
    let (dir, store, tenant) = setup();
    let snapshot = tenant.engine().snapshot().unwrap();
    drop(store);
    assert_eq!(
        Store::open(dir.path(), options()).err().unwrap().code,
        "BUSY"
    );
    tenant
        .put("assets", json!({"_key":"still-live"}), None)
        .unwrap();
    drop(tenant);
    assert_eq!(
        Store::open(dir.path(), options()).err().unwrap().code,
        "BUSY"
    );
    drop(snapshot);
    let reopened = Store::open(dir.path(), options()).unwrap();
    let id = reopened.tenants()[0].id;
    assert!(
        reopened
            .tenant(id)
            .unwrap()
            .get("assets", "still-live")
            .unwrap()
            .is_some()
    );
}

#[test]
fn tenant_delete_respects_a_standalone_engine_directory_lock() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options()).unwrap();
    let info = store.create_tenant("standalone").unwrap();
    let budget = tenant_kv::resources::Budget {
        global: tenant_kv::resources::Counter::new(256 << 20),
        tenant: tenant_kv::resources::Counter::new(128 << 20),
    };
    let engine = tenant_kv::storage::Engine::open(
        dir.path().join("tenants").join(info.id.to_string()),
        info.clone(),
        options(),
        budget,
        tenant_kv::resources::IoScheduler::new(1),
    )
    .unwrap();
    assert_eq!(store.delete_tenant(info.id).unwrap_err().code, "BUSY");
    assert!(engine.path().is_dir());
    drop(engine);
    store.delete_tenant(info.id).unwrap();
    assert!(
        !dir.path()
            .join("tenants")
            .join(info.id.to_string())
            .exists()
    );
}

#[test]
fn collection_gc_resumes_and_preserves_recreated_collection_and_old_snapshot() {
    let (dir, store, t) = setup();
    let id = t.id();
    let old = t.collections().unwrap()[0].id;
    t.write_batch(
        "assets",
        (0..1000)
            .map(|i| WriteOp::Put {
                document: json!({"_key":format!("{i:04}")}),
                if_rev: None,
            })
            .collect(),
    )
    .unwrap();
    let page = t
        .query(
            "assets",
            Query {
                limit: 2,
                ..Query::default()
            },
        )
        .unwrap();
    assert_eq!(t.get("assets", "0000").unwrap().unwrap()["_key"], "0000");
    t.drop_collection("assets").unwrap();
    assert_eq!(t.get("assets", "0000").unwrap_err().code, "NOT_FOUND");
    t.create_collection("assets").unwrap();
    t.put("assets", json!({"_key":"new"}), None).unwrap();
    assert!(t.get("assets", "0000").unwrap().is_none());
    let values = t
        .multi_get("assets", &["new".into(), "0000".into()])
        .unwrap();
    assert_eq!(values[0].as_ref().unwrap()["_key"], "new");
    assert!(values[1].is_none());
    let next = t
        .query(
            "assets",
            Query {
                limit: 2,
                cursor: page.cursor,
                ..Query::default()
            },
        )
        .unwrap();
    assert_eq!(next.documents[0]["_key"], "0002");
    if let Some(cursor) = next.cursor {
        t.release_cursor(&cursor).unwrap();
    }
    std::thread::sleep(Duration::from_millis(150));
    drop(t);
    drop(store);
    let store = Store::open(dir.path(), options()).unwrap();
    let t = store.tenant(id).unwrap();
    let mut prefix = vec![b'D'];
    prefix.extend(old.as_bytes());
    let end = codec::prefix_end(&prefix);
    let start = std::time::Instant::now();
    loop {
        let snapshot = t.engine().snapshot().unwrap();
        let mut stats = ReadStats::default();
        let mut scan = snapshot
            .scan(prefix.clone(), end.clone(), None, &mut stats)
            .unwrap();
        if scan.next(&mut stats).unwrap().is_none() {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(t.get("assets", "new").unwrap().is_some());
}

#[test]
fn idle_tenant_waits_for_cursor_lease_then_reopens_with_new_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options();
    opts.idle_seconds = 0;
    opts.snapshot_seconds = 1;
    let store = Store::open(dir.path(), opts).unwrap();
    let id = store.create_tenant("idle").unwrap().id;
    let t = store.tenant(id).unwrap();
    t.create_collection("assets").unwrap();
    for key in ["a", "b"] {
        t.put("assets", json!({"_key":key}), None).unwrap();
    }
    let runtime = t.engine().runtime_id;
    let page = t
        .query(
            "assets",
            Query {
                limit: 1,
                ..Query::default()
            },
        )
        .unwrap();
    assert!(page.cursor.is_some());
    drop(t);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(store.status(id).unwrap()["open"], true);
    let start = std::time::Instant::now();
    while store.status(id).unwrap()["open"] == true {
        assert!(start.elapsed() < Duration::from_secs(4));
        std::thread::sleep(Duration::from_millis(100));
    }
    let t = store.tenant(id).unwrap();
    assert_ne!(runtime, t.engine().runtime_id);
    assert!(t.get("assets", "a").unwrap().is_some());
    assert_eq!(
        t.query(
            "assets",
            Query {
                limit: 1,
                cursor: page.cursor,
                ..Query::default()
            }
        )
        .unwrap_err()
        .code,
        "CURSOR_EXPIRED"
    );
}

#[test]
fn write_result_limit_rejects_before_committing_and_multiget_restores_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options();
    opts.max_response_bytes = 1 << 20;
    let store = Store::open(dir.path(), opts).unwrap();
    let t = store
        .tenant(store.create_tenant("bounded-results").unwrap().id)
        .unwrap();
    t.create_collection("assets").unwrap();
    let keys: Vec<_> = (0..1000)
        .map(|i| format!("{}{i:04}", "\n".repeat(500)))
        .collect();
    let batch = keys
        .iter()
        .map(|key| WriteOp::Put {
            document: json!({"_key":key}),
            if_rev: None,
        })
        .collect();
    assert_eq!(
        t.write_batch("assets", batch).unwrap_err().code,
        "RESOURCE_EXHAUSTED"
    );
    assert!(t.get("assets", &keys[0]).unwrap().is_none());
    for key in ["a", "b"] {
        t.put("assets", json!({"_key":key}), None).unwrap();
    }
    t.engine().flush().unwrap();
    let rows = t
        .multi_get(
            "assets",
            &["b".into(), "missing".into(), "a".into(), "b".into()],
        )
        .unwrap();
    assert_eq!(rows[0], rows[3]);
    assert!(rows[1].is_none());
    assert_eq!(rows[2].as_ref().unwrap()["_key"], "a");
}

#[test]
fn deleting_another_tenant_preserves_unpublished_restore_preparation() {
    let (dir, store, tenant) = setup();
    let other = store.create_tenant("other").unwrap();
    // Restore copies are deliberately not registered until verification completes.
    let stage = dir
        .path()
        .join("staging")
        .join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir(&stage).unwrap();
    std::fs::write(stage.join("copy-in-progress"), b"tenant alpha checkpoint").unwrap();
    store.delete_tenant(other.id).unwrap();
    assert_eq!(
        std::fs::read(stage.join("copy-in-progress")).unwrap(),
        b"tenant alpha checkpoint"
    );
    tenant
        .put("assets", json!({"_key":"still-serving"}), None)
        .unwrap();
    drop(tenant);
    drop(store);
    let reopened = Store::open(dir.path(), options()).unwrap();
    assert!(
        !stage.exists(),
        "unpublished abandoned staging is collected only on startup"
    );
    drop(reopened);
}

#[cfg(unix)]
#[test]
fn restore_cleanup_error_still_fences_the_tenant_until_store_recovery() {
    use std::os::unix::fs::PermissionsExt;
    let (dir, store, tenant) = setup();
    let id = tenant.id();
    tenant
        .put("assets", json!({"_key":"same","value":"before"}), None)
        .unwrap();
    let backup = store.backup(id).unwrap();
    tenant
        .put("assets", json!({"_key":"same","value":"after"}), None)
        .unwrap();
    drop(tenant);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while store.close_tenant(id).is_err() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let blocked = dir
        .path()
        .join("trash")
        .join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir(&blocked).unwrap();
    std::fs::write(blocked.join("debris"), b"old data").unwrap();
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o0)).unwrap();
    // Root can bypass POSIX permissions; this failure is exercised when the
    // backend suite runs as the ordinary OrbStack/CI service user.
    if std::fs::read_dir(&blocked).is_ok() {
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o700)).unwrap();
        return;
    }
    let result = store.restore(id, backup);
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(result.is_err());
    assert_eq!(store.tenant(id).err().unwrap().code, "UNAVAILABLE");
    assert_eq!(store.close_tenant(id).unwrap_err().code, "UNAVAILABLE");
    drop(store);
    let reopened = Store::open(dir.path(), options()).unwrap();
    let tenant = reopened.tenant(id).unwrap();
    assert_eq!(
        tenant.get("assets", "same").unwrap().unwrap()["value"],
        "before"
    );
    tenant
        .put("assets", json!({"_key":"recovered"}), None)
        .unwrap();
}
