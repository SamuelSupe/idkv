use serde_json::json;
use std::sync::{Arc, Barrier};
use tenant_kv::*;

#[test]
fn a_cursor_can_only_be_consumed_once_under_concurrent_requests() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store
        .tenant(store.create_tenant("cursor-race").unwrap().id)
        .unwrap();
    t.create_collection("assets").unwrap();
    for batch in 0..4 {
        t.write_batch(
            "assets",
            (0..500)
                .map(|i| WriteOp::Put {
                    document: json!({"_key":format!("{:05}",batch*500+i),"rank":batch*500+i}),
                    if_rev: None,
                })
                .collect(),
        )
        .unwrap();
    }
    let mut query: Query =
        serde_json::from_value(json!({"sort":[{"field":"/rank","order":"desc"}],"limit":1}))
            .unwrap();
    query.cursor = t.query("assets", query.clone()).unwrap().cursor;
    let start = Arc::new(Barrier::new(9));
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let (t, query, start) = (t.clone(), query.clone(), start.clone());
            std::thread::spawn(move || {
                start.wait();
                t.query("assets", query)
            })
        })
        .collect();
    start.wait();
    let mut successful = Vec::new();
    for worker in workers {
        match worker.join().unwrap() {
            Ok(page) => successful.push(page),
            Err(error) => assert_eq!(error.code, "CURSOR_EXPIRED"),
        }
    }
    assert_eq!(
        successful.len(),
        1,
        "one cursor must not fork into concurrent pages"
    );
    assert_eq!(successful[0].documents[0]["rank"], 1998);
    let cursor = successful.pop().unwrap().cursor.unwrap();
    t.release_cursor(&cursor).unwrap();
}

#[test]
fn multi_get_budget_includes_missing_rows_and_json_array_framing() {
    let dir = tempfile::tempdir().unwrap();
    let options = Options {
        max_response_bytes: 1 << 20,
        ..Options::default()
    };
    let store = Store::open(dir.path(), options).unwrap();
    let t = store
        .tenant(store.create_tenant("response-budget").unwrap().id)
        .unwrap();
    t.create_collection("assets").unwrap();
    t.put(
        "assets",
        json!({"_key":"a","payload":"x".repeat((1 << 20)-200)}),
        None,
    )
    .unwrap();
    let document = t.get("assets", "a").unwrap().unwrap();
    let wire = serde_json::to_vec(&document).unwrap().len();
    assert!(wire < 1 << 20);
    assert!(wire + 64 * 5 + 2 > 1 << 20);
    let mut keys = vec!["missing".to_string(); 64];
    keys.insert(0, "a".into());
    let error = match t.multi_get("assets", &keys) {
        Ok(_) => panic!("oversized response must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.code, "RESOURCE_EXHAUSTED");
    let valid = t
        .multi_get("assets", &["missing".into(), "a".into()])
        .unwrap();
    assert!(valid[0].is_none());
    assert_eq!(valid[1].as_ref(), Some(&document));
}

#[test]
fn hot_history_and_tombstones_keep_snapshot_order_through_flush_and_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let id = store.create_tenant("history").unwrap().id;
    let t = store.tenant(id).unwrap();
    let mut snapshots = Vec::new();
    for n in 0u64..120 {
        t.engine()
            .transact(|tx| {
                if n % 7 == 0 {
                    tx.delete(b"Zhot".to_vec())
                } else {
                    tx.put(b"Zhot".to_vec(), n.to_be_bytes().to_vec())
                }
            })
            .unwrap();
        if n % 5 == 0 || n == 119 {
            snapshots.push((t.engine().snapshot().unwrap(), (n % 7 != 0).then_some(n)));
        }
    }
    let verify = || {
        for (snapshot, expected) in &snapshots {
            let mut stats = ReadStats::default();
            let value = snapshot.get(b"Zhot", &mut stats).unwrap();
            assert_eq!(
                value
                    .as_ref()
                    .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap())),
                *expected
            );
            let mut scan = snapshot
                .scan(b"Z".to_vec(), Some(b"[".to_vec()), None, &mut stats)
                .unwrap();
            assert_eq!(
                scan.next(&mut stats)
                    .unwrap()
                    .map(|r| u64::from_be_bytes(r.value.unwrap().as_slice().try_into().unwrap())),
                *expected
            );
            assert!(scan.next(&mut stats).unwrap().is_none());
        }
    };
    verify();
    t.engine().flush().unwrap();
    t.engine().compact(true).unwrap();
    verify();
    drop(snapshots);
    t.engine().compact(true).unwrap();
    t.engine().verify().unwrap();
    drop(t);
    drop(store);
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store.tenant(id).unwrap();
    assert!(
        t.engine()
            .snapshot()
            .unwrap()
            .get(b"Zhot", &mut ReadStats::default())
            .unwrap()
            .is_none()
    );
}

#[test]
fn compiled_sort_matches_reference_type_order_and_full_document_topk() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store
        .tenant(store.create_tenant("sort").unwrap().id)
        .unwrap();
    t.create_collection("assets").unwrap();
    let values = vec![
        None,
        Some(json!(null)),
        Some(json!(false)),
        Some(json!(true)),
        Some(json!(-9)),
        Some(json!(0)),
        Some(json!(u64::MAX)),
        Some(json!(-0.0)),
        Some(json!(1.0)),
        Some(json!("")),
        Some(json!("a\u{0}b")),
        Some(json!("设备")),
    ];
    for i in 0..120 {
        let mut doc = json!({"_key":format!("{i:04}"),"payload":"x".repeat(1024)});
        if let Some(value) = &values[i % values.len()] {
            doc["v"] = value.clone();
        }
        t.put("assets", doc, None).unwrap();
    }
    let keys = (0..120).map(|i| format!("{i:04}")).collect::<Vec<_>>();
    let documents = t
        .multi_get("assets", &keys)
        .unwrap()
        .into_iter()
        .map(Option::unwrap)
        .collect::<Vec<_>>();
    for order in [Order::Asc, Order::Desc] {
        let mut reference = documents.clone();
        reference.sort_by_cached_key(|v| {
            let mut key = codec::scalar(v.get("v"), &order).unwrap();
            key.extend(codec::scalar(v.get("_key"), &Order::Asc).unwrap());
            key
        });
        let mut query = Query {
            sort: vec![IndexField {
                field: "/v".into(),
                order,
            }],
            limit: 7,
            ..Default::default()
        };
        let mut rows = Vec::new();
        loop {
            let page = t.query("assets", query.clone()).unwrap();
            rows.extend(page.documents);
            query.cursor = page.cursor;
            if query.cursor.is_none() {
                break;
            }
        }
        assert_eq!(rows, reference);
    }
}
