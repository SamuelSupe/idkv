use serde_json::{Value, json};
use tenant_kv::{identity::*, *};
use uuid::Uuid;
fn context() -> IdentityContext {
    IdentityContext {
        source: "test_connector".into(),
        actor: "integration".into(),
        can_correct: true,
    }
}
fn ident(value: &str) -> Value {
    json!({"kind":"account","namespace":"crm","value":value})
}
fn execute(t: &TenantHandle, request: Value) -> Result<IdentityReceipt> {
    t.identity_write(&context(), serde_json::from_value(request).unwrap())
}
fn entity(t: &TenantHandle, name: &str) -> Uuid {
    execute(
        t,
        json!({"action":"create_entity","operation_id":name,"attributes":{"name":name}}),
    )
    .unwrap()
    .entity
    .unwrap()
    .entity_id
}
fn init(options: Options, config: IdentityConfig) -> (tempfile::TempDir, Store, TenantHandle) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options).unwrap();
    let id = store.create_tenant("identity").unwrap().id;
    let tenant = store.tenant(id).unwrap();
    tenant.identity_initialize(&context(), config).unwrap();
    (dir, store, tenant)
}
fn rev(t: &TenantHandle, key: &str) -> Value {
    t.identity_timeline(&serde_json::from_value(ident(key)).unwrap())
        .unwrap()
        .map_or(json!("absent"), |v| json!(v.revision))
}
fn change(t: &TenantHandle, op: &str, key: &str, action: Value, effective: Value) -> Value {
    json!({"action":"bindings","operation_id":op,"changes":[{"identifier":ident(key),"expected_revision":rev(t,key),"effective":effective,"change":action}]})
}
fn resolve(t: &TenantHandle, key: &str, at: &str, known: Option<Uuid>) -> ResolveResult {
    t.identity_resolve(&ResolveRequest {
        identifier: serde_json::from_value(ident(key)).unwrap(),
        valid_at: at.into(),
        known_at: known.map(|v| v.to_string()),
    })
    .unwrap()
}
fn manual(from: &str, to: Option<&str>) -> Value {
    json!({"mode":"manual","from":from,"to":to})
}
fn bind(id: Uuid) -> Value {
    json!({"action":"bind","entity_id":id,"reason":"initial evidence"})
}
fn rebind(id: Uuid) -> Value {
    json!({"action":"rebind","entity_id":id,"reason":"correct attribution"})
}

#[test]
fn history_boundaries_noops_replace_and_recovery() {
    let (dir, store, t) = init(Options::default(), IdentityConfig::default());
    let id = t.id();
    let a = entity(&t, "a");
    let b = entity(&t, "b");
    let first = execute(
        &t,
        change(
            &t,
            "first",
            "one",
            bind(a),
            manual("2026-01-01T09:00:00Z", Some("2026-01-01T12:00:00Z")),
        ),
    )
    .unwrap();
    let correction = change(
        &t,
        "correction",
        "one",
        rebind(b),
        manual("2026-01-01T10:00:00Z", Some("2026-01-01T11:00:00Z")),
    );
    let fixed = execute(&t, correction.clone()).unwrap();
    assert_eq!(fixed.timelines[0].replaced.len(), 1);
    assert_eq!(
        resolve(&t, "one", "2026-01-01T10:00:00Z", None)
            .binding
            .unwrap()
            .entity_id,
        b
    );
    for at in ["2026-01-01T09:59:59.999999Z", "2026-01-01T11:00:00Z"] {
        assert_eq!(resolve(&t, "one", at, None).binding.unwrap().entity_id, a);
    }
    assert!(
        resolve(&t, "one", "2026-01-01T12:00:00Z", None)
            .binding
            .is_none()
    );
    assert_eq!(
        resolve(
            &t,
            "one",
            "2026-01-01T10:30:00Z",
            Some(first.knowledge.version)
        )
        .binding
        .unwrap()
        .entity_id,
        a
    );
    assert_eq!(
        serde_json::to_value(execute(&t, correction.clone()).unwrap()).unwrap(),
        serde_json::to_value(&fixed).unwrap()
    );
    let mut different = correction;
    different["changes"][0]["change"]["reason"] = json!("different");
    assert_eq!(
        execute(&t, different).unwrap_err().code,
        "OPERATION_CONFLICT"
    );
    let unchanged = rev(&t, "one");
    let noop = execute(
        &t,
        change(
            &t,
            "noop",
            "one",
            rebind(b),
            manual("2026-01-01T10:00:00Z", Some("2026-01-01T11:00:00Z")),
        ),
    )
    .unwrap();
    assert!(!noop.timelines[0].changed);
    assert_eq!(rev(&t, "one"), unchanged);
    let existing = t.identity_entity(a).unwrap();
    let attrs=execute(&t,json!({"action":"update_entity","operation_id":"attrs","entity_id":a,"expected_revision":existing.revision,"attributes":{"team":"blue"}})).unwrap().entity.unwrap();
    assert_eq!(attrs.bindings_revision, existing.bindings_revision);
    assert_eq!(attrs.attributes["name"], "a");
    assert_eq!(rev(&t, "one"), unchanged);
    let replacement = json!({"action":"replace_identifier","entity_id":b,"replacement":ident("two"),"replacement_expected_revision":"absent","reason":"rename"});
    execute(
        &t,
        change(
            &t,
            "replace",
            "one",
            replacement,
            manual("2026-01-01T10:00:00Z", Some("2026-01-01T11:00:00Z")),
        ),
    )
    .unwrap();
    assert!(
        resolve(&t, "one", "2026-01-01T10:30:00Z", None)
            .binding
            .is_none()
    );
    assert_eq!(
        resolve(&t, "two", "2026-01-01T10:30:00Z", None)
            .binding
            .unwrap()
            .entity_id,
        b
    );
    t.engine().flush().unwrap();
    t.engine().compact(true).unwrap();
    drop(t);
    drop(store);
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store.tenant(id).unwrap();
    assert_eq!(
        resolve(
            &t,
            "one",
            "2026-01-01T10:30:00Z",
            Some(first.knowledge.version)
        )
        .binding
        .unwrap()
        .entity_id,
        a
    );
    assert_eq!(
        t.identity_operation("correction")
            .unwrap()
            .knowledge
            .version,
        fixed.knowledge.version
    );
    assert_eq!(
        resolve(&t, "two", "2026-01-01T10:30:00Z", None)
            .binding
            .unwrap()
            .entity_id,
        b
    );
    let other = store
        .tenant(store.create_tenant("other").unwrap().id)
        .unwrap();
    other
        .identity_initialize(&context(), IdentityConfig::default())
        .unwrap();
    assert_eq!(
        other
            .identity_resolve(&ResolveRequest {
                identifier: serde_json::from_value(ident("one")).unwrap(),
                valid_at: "2026-01-01T10:30:00Z".into(),
                known_at: Some(first.knowledge.version.to_string())
            })
            .unwrap_err()
            .code,
        "KNOWLEDGE_UNAVAILABLE"
    );
    assert!(
        resolve(&other, "one", "2026-01-01T10:30:00Z", None)
            .binding
            .is_none()
    );
}

#[test]
fn automatic_time_preserves_expiration_future_plans_and_concurrent_receipts() {
    let (_dir, _store, t) = init(Options::default(), IdentityConfig::default());
    let a = entity(&t, "a");
    let b = entity(&t, "b");
    execute(
        &t,
        change(
            &t,
            "initial",
            "one",
            bind(a),
            manual("2000-01-01T00:00:00Z", Some("2099-01-01T00:00:00Z")),
        ),
    )
    .unwrap();
    execute(
        &t,
        change(
            &t,
            "future",
            "one",
            bind(a),
            manual("2100-01-01T00:00:00Z", None),
        ),
    )
    .unwrap();
    let request = change(&t, "auto", "one", rebind(b), json!({"mode":"auto"}));
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(16));
    let handles: Vec<_> = (0..16)
        .map(|_| {
            let t = t.clone();
            let r = request.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let receipt = execute(&t, r).unwrap();
                assert!(t.engine().seq() >= receipt.knowledge.commit_seq);
                serde_json::to_value(receipt).unwrap()
            })
        })
        .collect();
    let values: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert!(values.iter().all(|v| v == &values[0]));
    let receipt = t.identity_operation("auto").unwrap();
    assert_eq!(
        receipt.timelines[0].effective_from_us,
        receipt.effective_time_us
    );
    assert_eq!(
        receipt.timelines[0].effective_to_us,
        Some(parse_time("2099-01-01T00:00:00Z").unwrap())
    );
    let timeline = t
        .identity_timeline(&serde_json::from_value(ident("one")).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(timeline.segments.len(), 3);
    assert_eq!(
        timeline.segments[0].valid_to_us,
        Some(receipt.effective_time_us)
    );
    assert_eq!(timeline.segments[1].entity_id, b);
    assert!(
        resolve(&t, "one", "2099-06-01T00:00:00Z", None)
            .binding
            .is_none()
    );
    assert_eq!(
        resolve(&t, "one", "2100-01-01T00:00:00Z", None)
            .binding
            .unwrap()
            .entity_id,
        a
    );
    let noop = execute(
        &t,
        change(&t, "same", "one", rebind(b), json!({"mode":"auto"})),
    )
    .unwrap();
    assert!(!noop.timelines[0].changed);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let handles: Vec<_> = [a, b]
        .into_iter()
        .enumerate()
        .map(|(i, e)| {
            let request = change(
                &t,
                &format!("race{i}"),
                "race",
                bind(e),
                json!({"mode":"auto"}),
            );
            let t = t.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                execute(&t, request)
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results.into_iter().find_map(Result::err).unwrap().code,
        "REVISION_CONFLICT"
    );
}

#[test]
fn constraints_and_expanded_limits_roll_back_every_record() {
    let options = Options {
        max_batch_operations: 10,
        ..Options::default()
    };
    let (_dir, _store, t) = init(
        options,
        IdentityConfig {
            max_segments: 2,
            ..IdentityConfig::default()
        },
    );
    let a = entity(&t, "a");
    let b = entity(&t, "b");
    let change_a = change(&t, "big", "one", bind(a), json!({"mode":"auto"}))["changes"][0].clone();
    let change_b = change(&t, "big", "two", bind(a), json!({"mode":"auto"}))["changes"][0].clone();
    let seq = t.engine().seq();
    assert_eq!(
        execute(
            &t,
            json!({"action":"bindings","operation_id":"big","changes":[change_a,change_b]})
        )
        .unwrap_err()
        .code,
        "RESOURCE_EXHAUSTED"
    );
    assert_eq!(t.engine().seq(), seq);
    assert_eq!(rev(&t, "one"), "absent");
    assert_eq!(rev(&t, "two"), "absent");
    assert!(t.identity_operation("big").is_err());
    execute(
        &t,
        change(
            &t,
            "seed",
            "one",
            bind(a),
            manual("2026-01-01T09:00:00Z", Some("2026-01-01T12:00:00Z")),
        ),
    )
    .unwrap();
    let prior = rev(&t, "one");
    assert_eq!(
        execute(
            &t,
            change(
                &t,
                "overlap",
                "one",
                bind(b),
                manual("2026-01-01T10:00:00Z", None)
            )
        )
        .unwrap_err()
        .code,
        "INTERVAL_CONFLICT"
    );
    assert_eq!(
        execute(
            &t,
            change(
                &t,
                "segments",
                "one",
                rebind(b),
                manual("2026-01-01T10:00:00Z", Some("2026-01-01T11:00:00Z"))
            )
        )
        .unwrap_err()
        .code,
        "RESOURCE_EXHAUSTED"
    );
    assert_eq!(rev(&t, "one"), prior);
    let dup =
        change(&t, "duplicate", "one", rebind(b), json!({"mode":"auto"}))["changes"][0].clone();
    assert_eq!(
        execute(
            &t,
            json!({"action":"bindings","operation_id":"duplicate","changes":[dup,dup]})
        )
        .unwrap_err()
        .code,
        "INVALID_ARGUMENT"
    );
    let replace = json!({"action":"replace_identifier","entity_id":b,"replacement":ident("two"),"replacement_expected_revision":"absent","reason":"wrong owner"});
    assert_eq!(
        execute(
            &t,
            change(
                &t,
                "bad_replace",
                "one",
                replace,
                manual("2026-01-01T10:00:00Z", Some("2026-01-01T11:00:00Z"))
            )
        )
        .unwrap_err()
        .code,
        "INTERVAL_CONFLICT"
    );
    assert_eq!(rev(&t, "two"), "absent");
    let low = IdentityContext {
        can_correct: false,
        ..context()
    };
    let req = serde_json::from_value(change(
        &t,
        "denied",
        "one",
        rebind(b),
        json!({"mode":"auto"}),
    ))
    .unwrap();
    assert_eq!(t.identity_write(&low, req).unwrap_err().code, "FORBIDDEN");
    for result in [
        t.create_collection(COLLECTION).map(|_| ()),
        t.drop_collection(COLLECTION),
        t.put(COLLECTION, json!({"_key":"head:knowledge"}), None)
            .map(|_| ()),
        t.delete(COLLECTION, "head:knowledge", None).map(|_| ()),
        t.create_index(
            COLLECTION,
            IndexSpec {
                name: "bad".into(),
                fields: vec![IndexField {
                    field: "/record_type".into(),
                    order: Order::Asc,
                }],
                unique: false,
                include: vec![],
            },
        )
        .map(|_| ()),
    ] {
        assert_eq!(result.unwrap_err().code, "MANAGED_COLLECTION");
    }
}

#[test]
fn randomized_interval_corrections_match_a_discrete_reference() {
    let (_dir, _store, t) = init(Options::default(), IdentityConfig::default());
    let entities = [entity(&t, "a"), entity(&t, "b")];
    let mut state = [None; 60];
    let mut seed = 31u64;
    for step in 0..80 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let from = (seed as usize) % 58;
        let to = (from + 1 + ((seed >> 16) as usize) % (60 - from)).min(60);
        let target = entities[(seed >> 32) as usize % 2];
        let action = (seed >> 40) % 3;
        let from_time = format!("2026-01-01T00:00:{from:02}Z");
        let to_time = if to == 60 {
            "2026-01-01T00:01:00Z".into()
        } else {
            format!("2026-01-01T00:00:{to:02}Z")
        };
        let change_action = match action {
            0 => bind(target),
            1 => rebind(target),
            _ => json!({"action":"unbind","reason":"reference deletion"}),
        };
        let conflict = action == 0
            && state[from..to]
                .iter()
                .any(|e| e.is_some_and(|e| e != target));
        let result = execute(
            &t,
            change(
                &t,
                &format!("step{step}"),
                "random",
                change_action,
                manual(&from_time, Some(&to_time)),
            ),
        );
        if conflict {
            assert_eq!(result.unwrap_err().code, "INTERVAL_CONFLICT");
        } else {
            result.unwrap();
            for e in &mut state[from..to] {
                *e = if action == 2 { None } else { Some(target) };
            }
        }
        for (second, expected) in state.iter().enumerate() {
            assert_eq!(
                resolve(
                    &t,
                    "random",
                    &format!("2026-01-01T00:00:{second:02}Z"),
                    None
                )
                .binding
                .map(|b| b.entity_id),
                *expected,
                "step {step}, second {second}"
            );
        }
    }
}

#[test]
fn restore_never_reuses_a_discarded_knowledge_uuid() {
    let (_dir, store, t) = init(Options::default(), IdentityConfig::default());
    let id = t.id();
    let a = entity(&t, "a");
    let first = execute(
        &t,
        change(
            &t,
            "first",
            "one",
            bind(a),
            manual("2026-01-01T00:00:00Z", None),
        ),
    )
    .unwrap();
    let backup = store.backup(id).unwrap();
    let lost = execute(
        &t,
        change(
            &t,
            "lost",
            "one",
            json!({"action":"unbind","reason":"later cancellation"}),
            manual("2026-02-01T00:00:00Z", None),
        ),
    )
    .unwrap();
    drop(t);
    for attempt in 0..100 {
        match store.restore(id, backup) {
            Ok(()) => break,
            Err(e) if e.code == "BUSY" && attempt < 99 => {
                std::thread::sleep(std::time::Duration::from_millis(20))
            }
            Err(e) => panic!("{e}"),
        }
    }
    let t = store.tenant(id).unwrap();
    let query = ResolveRequest {
        identifier: serde_json::from_value(ident("one")).unwrap(),
        valid_at: "2026-03-01T00:00:00Z".into(),
        known_at: Some(lost.knowledge.version.to_string()),
    };
    assert_eq!(
        t.identity_resolve_many(&resolve_items(
            &[("one", "2026-03-01T00:00:00Z")],
            Some(lost.knowledge.version)
        ))
        .unwrap_err()
        .code,
        "KNOWLEDGE_UNAVAILABLE"
    );
    assert_eq!(
        t.identity_resolve(&query).unwrap_err().code,
        "KNOWLEDGE_UNAVAILABLE"
    );
    assert_eq!(
        resolve(
            &t,
            "one",
            "2026-03-01T00:00:00Z",
            Some(first.knowledge.version)
        )
        .binding
        .unwrap()
        .entity_id,
        a
    );
    let reused = execute(
        &t,
        change(
            &t,
            "lost",
            "one",
            json!({"action":"unbind","reason":"later cancellation"}),
            manual("2026-02-01T00:00:00Z", None),
        ),
    )
    .unwrap();
    assert_eq!(reused.knowledge.ordinal, lost.knowledge.ordinal);
    assert_ne!(reused.knowledge.version, lost.knowledge.version);
    assert_eq!(
        t.identity_resolve(&query).unwrap_err().code,
        "KNOWLEDGE_UNAVAILABLE"
    );
}

fn resolve_items(items: &[(&str, &str)], known: Option<Uuid>) -> ResolveManyRequest {
    ResolveManyRequest {
        known_at: known.map(|v| v.to_string()),
        items: items
            .iter()
            .map(|(key, at)| ResolveItem {
                identifier: serde_json::from_value(ident(key)).unwrap(),
                valid_at: (*at).into(),
            })
            .collect(),
    }
}

#[test]
fn resolve_many_preserves_order_boundaries_knowledge_and_prepared_tenant_isolation() {
    let (_dir, store, t) = init(Options::default(), IdentityConfig::default());
    let a = entity(&t, "a");
    let b = entity(&t, "b");
    let first = execute(
        &t,
        change(
            &t,
            "first",
            "one",
            bind(a),
            manual("2026-01-01T09:00:00Z", Some("2026-01-01T12:00:00Z")),
        ),
    )
    .unwrap();
    let prepared = t.prepare_dsl("resolvemany with $request").unwrap();
    let events = [
        ("one", "2026-01-01T10:00:00Z"),
        ("missing", "2026-01-01T10:00:00Z"),
        ("one", "2026-01-01T11:00:00Z"),
        ("one", "2026-01-01T12:00:00Z"),
        ("one", "2026-01-01T10:00:00Z"),
    ];
    let latest = resolve_items(&events, None);
    let params = json!({"request":latest});
    let before = serde_json::to_value(
        prepared
            .execute(&t, params.as_object().unwrap(), None)
            .unwrap(),
    )
    .unwrap();
    execute(
        &t,
        change(
            &t,
            "correct",
            "one",
            rebind(b),
            manual("2026-01-01T10:00:00Z", Some("2026-01-01T11:00:00Z")),
        ),
    )
    .unwrap();
    t.engine().flush().unwrap();
    t.engine().compact(true).unwrap();
    for known in [None, Some(first.knowledge.version)] {
        let request = resolve_items(&events, known);
        let result = t.identity_resolve_many(&request).unwrap();
        assert_eq!(result.history, t.history());
        assert_eq!(result.results.len(), events.len());
        assert_eq!(result.results[0], result.results[4]);
        assert_eq!(
            result.results[0].binding.as_ref().unwrap().entity_id,
            if known.is_some() { a } else { b }
        );
        assert!(result.results[1].timeline_revision.is_none());
        assert!(result.results[3].binding.is_none());
        assert!(result.results[3].timeline_revision.is_some());
        assert!(result.stats.document_reads < events.len() as u64);
        for (item, matched) in request.items.iter().zip(&result.results) {
            let single = t
                .identity_resolve(&ResolveRequest {
                    identifier: item.identifier.clone(),
                    valid_at: item.valid_at.clone(),
                    known_at: Some(result.knowledge_version.to_string()),
                })
                .unwrap();
            assert_eq!(single.binding, matched.binding);
            assert_eq!(single.timeline_revision, matched.timeline_revision);
        }
        let params = json!({"request":request});
        let dsl = serde_json::to_value(
            prepared
                .execute(&t, params.as_object().unwrap(), None)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            dsl["results"],
            serde_json::to_value(&result.results).unwrap()
        );
        assert_eq!(dsl["knowledge_version"], json!(result.knowledge_version));
        assert_eq!(
            prepared
                .execute(&t, params.as_object().unwrap(), Some("cursor"))
                .unwrap_err()
                .code,
            "INVALID_ARGUMENT"
        );
    }
    let after = serde_json::to_value(
        prepared
            .execute(&t, params.as_object().unwrap(), None)
            .unwrap(),
    )
    .unwrap();
    assert_ne!(before["knowledge_version"], after["knowledge_version"]);
    let other = store
        .tenant(store.create_tenant("other").unwrap().id)
        .unwrap();
    other
        .identity_initialize(&context(), IdentityConfig::default())
        .unwrap();
    let empty = serde_json::to_value(
        prepared
            .execute(&other, params.as_object().unwrap(), None)
            .unwrap(),
    )
    .unwrap();
    assert!(
        empty["results"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["binding"].is_null())
    );
    let pinned = json!({"request":resolve_items(&events, Some(first.knowledge.version))});
    assert_eq!(
        prepared
            .execute(&other, pinned.as_object().unwrap(), None)
            .unwrap_err()
            .code,
        "KNOWLEDGE_UNAVAILABLE"
    );
    let single = json!({"request":{"identifier":ident("one"),"valid_at":events[0].1}});
    let dsl = t
        .prepare_dsl("resolve with $request")
        .unwrap()
        .execute(&t, single.as_object().unwrap(), None)
        .unwrap();
    assert_eq!(
        serde_json::to_value(dsl).unwrap()["binding"]["entity_id"],
        json!(b)
    );
}

#[test]
fn resolve_many_limits_cancellation_and_deadline_apply_to_the_whole_call() {
    let (_dir, _store, t) = init(
        Options {
            max_read_keys: 4,
            ..Options::default()
        },
        IdentityConfig::default(),
    );
    let a = entity(&t, "a");
    for key in ["one", "two", "three"] {
        execute(
            &t,
            change(&t, key, key, bind(a), manual("2026-01-01T00:00:00Z", None)),
        )
        .unwrap();
    }
    let mut request = resolve_items(
        &[
            ("one", "2026-01-01T01:00:00Z"),
            ("two", "2026-01-01T01:00:00Z"),
            ("three", "2026-01-01T01:00:00Z"),
        ],
        None,
    );
    let first = t.identity_resolve_many(&request).unwrap();
    request.known_at = Some(first.knowledge_version.to_string());
    let reader = t
        .read_with(&ReadOptions {
            max_scan: Some(1),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        reader.identity_resolve_many(&request).unwrap_err().code,
        "RESOURCE_EXHAUSTED"
    );
    let cancellation = ReadCancellation::default();
    let options = ReadOptions {
        cancellation: Some(cancellation.clone()),
        ..Default::default()
    };
    let reader = t.read_with(&options).unwrap();
    cancellation.cancel();
    assert_eq!(
        reader.identity_resolve_many(&request).unwrap_err().code,
        "CANCELLED"
    );
    let reader = t
        .read_with(&ReadOptions {
            timeout_ms: Some(1),
            ..Default::default()
        })
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert_eq!(
        reader.identity_resolve_many(&request).unwrap_err().code,
        "DEADLINE_EXCEEDED"
    );
    let mut invalid = request.clone();
    invalid.items[2].valid_at = "no timezone".into();
    assert_eq!(
        t.identity_resolve_many(&invalid).unwrap_err().code,
        "INVALID_ARGUMENT"
    );
    invalid = request.clone();
    invalid.items.resize(5, invalid.items[0].clone());
    assert_eq!(
        t.identity_resolve_many(&invalid).unwrap_err().code,
        "RESOURCE_EXHAUSTED"
    );
    invalid.items.clear();
    assert_eq!(
        t.identity_resolve_many(&invalid).unwrap_err().code,
        "RESOURCE_EXHAUSTED"
    );
    assert_eq!(
        t.identity_resolve_many(&request).unwrap().results,
        first.results
    );
}

#[test]
fn resolve_many_never_mixes_concurrent_atomic_corrections() {
    use std::sync::{
        Barrier,
        atomic::{AtomicBool, Ordering},
    };
    let (_dir, _store, t) = init(Options::default(), IdentityConfig::default());
    let a = entity(&t, "a");
    let b = entity(&t, "b");
    let keys: Vec<_> = (0..12).map(|i| format!("id-{i}")).collect();
    let update = |op: &str, entity_id| {
        let changes: Vec<_> = keys.iter().map(|key| json!({"identifier":ident(key),"expected_revision":rev(&t,key),"effective":manual("2026-01-01T00:00:00Z",None),"change":rebind(entity_id)})).collect();
        execute(
            &t,
            json!({"action":"bindings","operation_id":op,"changes":changes}),
        )
        .unwrap()
    };
    update("initial", a);
    let request = ResolveManyRequest {
        known_at: None,
        items: (0..240)
            .map(|i| ResolveItem {
                identifier: serde_json::from_value(ident(&keys[i % keys.len()])).unwrap(),
                valid_at: "2026-01-02T00:00:00Z".into(),
            })
            .collect(),
    };
    let barrier = Barrier::new(2);
    let finished = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            barrier.wait();
            for n in 0..24 {
                update(&format!("change-{n}"), if n % 2 == 0 { b } else { a });
            }
            finished.store(true, Ordering::Release);
        });
        barrier.wait();
        let mut reads = 0;
        while !finished.load(Ordering::Acquire) || reads < 24 {
            let result = t.identity_resolve_many(&request).unwrap();
            let expected = result.results[0].binding.as_ref().unwrap().entity_id;
            assert!(
                result
                    .results
                    .iter()
                    .all(|r| r.binding.as_ref().unwrap().entity_id == expected)
            );
            let mut pinned = request.clone();
            pinned.known_at = Some(result.knowledge_version.to_string());
            assert_eq!(
                t.identity_resolve_many(&pinned).unwrap().results,
                result.results
            );
            reads += 1;
        }
        writer.join().unwrap();
    });
}

#[test]
fn resolve_many_bounds_retained_output_and_releases_memory_on_failure() {
    let (dir, store, t) = init(Options::default(), IdentityConfig::default());
    let id = t.id();
    let a = entity(&t, "a");
    execute(
        &t,
        change(
            &t,
            "first",
            "one",
            bind(a),
            manual("2026-01-01T00:00:00Z", None),
        ),
    )
    .unwrap();
    t.engine().flush().unwrap();
    drop(t);
    drop(store);
    for (memory_limit, response_limit) in [(32 << 10, 16 << 20), (64 << 20, 1 << 20)] {
        let store = Store::open(
            dir.path(),
            Options {
                max_query_bytes: memory_limit,
                max_response_bytes: response_limit,
                max_read_keys: 4096,
                ..Default::default()
            },
        )
        .unwrap();
        let t = store.tenant(id).unwrap();
        // Measure this read's reservations without sampling another maintenance job.
        drop(store);
        let mut request = resolve_items(&[("one", "2026-01-02T00:00:00Z")], None);
        let one = t.identity_resolve_many(&request).unwrap();
        request.items.resize(
            if memory_limit < 64 << 20 { 32 } else { 4096 },
            request.items[0].clone(),
        );
        let before = t.engine().memory_bytes();
        for _ in 0..5 {
            assert_eq!(
                t.identity_resolve_many(&request).unwrap_err().code,
                "RESOURCE_EXHAUSTED"
            );
            assert_eq!(t.engine().active_snapshots(), 0);
            assert_eq!(t.engine().memory_bytes(), before);
        }
        request.items.truncate(1);
        assert_eq!(
            t.identity_resolve_many(&request).unwrap().results,
            one.results
        );
    }
}

fn subject_ids(
    t: &TenantHandle,
    entity: Uuid,
    at: &str,
    known: Option<Uuid>,
) -> Vec<SubjectIdentifier> {
    let mut request: SubjectIdentifiersRequest = serde_json::from_value(
        json!({"entity_id":entity,"valid_at":at,"known_at":known,"limit":1}),
    )
    .unwrap();
    let mut items = Vec::new();
    for _ in 0..100 {
        let page = t.get_subject_identifiers(&request).unwrap();
        items.extend(page.identifiers);
        request.cursor = page.cursor;
        if request.cursor.is_none() {
            return items;
        }
    }
    panic!("subject pagination did not terminate");
}
fn subject_events(t: &TenantHandle, entity: Uuid) -> Vec<IdentityEvent> {
    let mut request: SubjectHistoryRequest =
        serde_json::from_value(json!({"entity_id":entity,"limit":1})).unwrap();
    let mut events = Vec::new();
    for _ in 0..100 {
        let page = t.get_subject_history(&request).unwrap();
        assert!(!page.coverage.legacy_history_missing);
        events.extend(page.events);
        request.cursor = page.cursor;
        if request.cursor.is_none() {
            return events;
        }
    }
    panic!("event pagination did not terminate");
}

#[test]
fn subject_reverse_matches_resolve_across_corrections_replacement_and_restart() {
    let (dir, store, t) = init(Options::default(), IdentityConfig::default());
    let tenant = t.id();
    let a = entity(&t, "subject-a");
    let b = entity(&t, "subject-b");
    let mut versions = Vec::new();
    for key in ["one", "two", "three"] {
        versions.push(
            execute(
                &t,
                change(&t, key, key, bind(a), manual("2026-01-01T00:00:00Z", None)),
            )
            .unwrap()
            .knowledge
            .version,
        );
    }
    versions.push(
        execute(
            &t,
            change(
                &t,
                "reassign",
                "one",
                rebind(b),
                manual("2026-01-01T00:00:00Z", None),
            ),
        )
        .unwrap()
        .knowledge
        .version,
    );
    versions.push(
        execute(
            &t,
            change(
                &t,
                "partial",
                "two",
                rebind(b),
                manual("2026-02-01T00:00:00Z", Some("2026-03-01T00:00:00Z")),
            ),
        )
        .unwrap()
        .knowledge
        .version,
    );
    versions.push(execute(&t, change(&t, "replace", "three", json!({"action":"replace_identifier","entity_id":a,"replacement":ident("four"),"replacement_expected_revision":"absent","reason":"renamed"}), manual("2026-01-01T00:00:00Z", None))).unwrap().knowledge.version);
    versions.push(
        execute(
            &t,
            change(
                &t,
                "remove",
                "one",
                json!({"action":"unbind","reason":"closed"}),
                manual("2026-01-01T00:00:00Z", None),
            ),
        )
        .unwrap()
        .knowledge
        .version,
    );
    let verify = |t: &TenantHandle| {
        for version in &versions {
            for at in [
                "2025-12-31T00:00:00Z",
                "2026-01-02T00:00:00Z",
                "2026-02-02T00:00:00Z",
                "2026-03-01T00:00:00Z",
            ] {
                for entity in [a, b] {
                    let reverse = subject_ids(t, entity, at, Some(*version));
                    for key in ["one", "two", "three", "four"] {
                        let forward = resolve(t, key, at, Some(*version));
                        let found = reverse.iter().find(|v| v.identifier.value == key);
                        assert_eq!(
                            found.is_some(),
                            forward
                                .binding
                                .as_ref()
                                .is_some_and(|v| v.entity_id == entity),
                            "{key} {at} {version}"
                        );
                        if let Some(found) = found {
                            assert_eq!(Some(&found.binding), forward.binding.as_ref());
                            assert_eq!(Some(found.timeline_revision), forward.timeline_revision);
                        }
                    }
                }
            }
        }
    };
    verify(&t);
    let events_a = subject_events(&t, a);
    let events_b = subject_events(&t, b);
    let reassign = events_a
        .iter()
        .find(|e| e.operation_id == "reassign")
        .unwrap();
    assert!(events_b.iter().any(|e| e.event_id == reassign.event_id));
    let replacement = events_a
        .iter()
        .find(|e| e.operation_id == "replace")
        .unwrap();
    assert_eq!(replacement.kind, IdentityEventKind::IdentifierReplaced);
    assert_eq!(replacement.changes.len(), 2);
    drop(t);
    drop(store);
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store.tenant(tenant).unwrap();
    verify(&t);
    assert_eq!(
        subject_events(&t, a)
            .iter()
            .map(|e| e.event_id)
            .collect::<Vec<_>>(),
        events_a.iter().map(|e| e.event_id).collect::<Vec<_>>()
    );
}

#[test]
fn subject_history_records_actual_diffs_and_pages_pin_the_knowledge_view() {
    let (_dir, _store, t) = init(Options::default(), IdentityConfig::default());
    let a = entity(&t, "a");
    for key in ["one", "two", "three"] {
        execute(
            &t,
            change(&t, key, key, bind(a), manual("2026-01-01T00:00:00Z", None)),
        )
        .unwrap();
    }
    let mut request: SubjectIdentifiersRequest =
        serde_json::from_value(json!({"entity_id":a,"valid_at":"2026-01-02T00:00:00Z","limit":1}))
            .unwrap();
    let first = t.get_subject_identifiers(&request).unwrap();
    let knowledge = first.knowledge_version;
    let seq = first.snapshot_seq;
    let mut collected = first.identifiers;
    request.cursor = first.cursor;
    execute(
        &t,
        change(
            &t,
            "new-after-page",
            "four",
            bind(a),
            manual("2026-01-01T00:00:00Z", None),
        ),
    )
    .unwrap();
    while request.cursor.is_some() {
        let old = request.clone();
        let next = t.get_subject_identifiers(&request).unwrap();
        assert_eq!(next.knowledge_version, knowledge);
        assert_eq!(next.snapshot_seq, seq);
        collected.extend(next.identifiers);
        request.cursor = next.cursor;
        assert_eq!(
            t.get_subject_identifiers(&old).unwrap_err().code,
            "CURSOR_EXPIRED"
        );
    }
    assert_eq!(collected.len(), 3);
    let old = t.identity_entity(a).unwrap();
    let update = json!({"action":"update_entity","operation_id":"attrs","entity_id":a,"expected_revision":old.revision,"attributes":{"name":"a","nullable":null,"new":{"nested":"literal"}}});
    execute(&t, update.clone()).unwrap();
    execute(&t, update).unwrap();
    let events = subject_events(&t, a);
    let updates: Vec<_> = events
        .iter()
        .filter(|e| e.operation_id == "attrs")
        .collect();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].attributes.len(), 2);
    let null = updates[0]
        .attributes
        .iter()
        .find(|d| d.field == "nullable")
        .unwrap();
    assert!(!null.before.present);
    assert!(null.after.present);
    assert_eq!(null.after.value, Value::Null);
    assert_eq!(updates[0].knowledge.actor, context().actor);
    let before = events.len();
    execute(
        &t,
        change(
            &t,
            "noop",
            "missing",
            json!({"action":"unbind","reason":"nothing"}),
            manual("2026-01-01T00:00:00Z", None),
        ),
    )
    .unwrap();
    assert_eq!(subject_events(&t, a).len(), before);
    assert!(t.identity_operation("noop").is_ok());
    let query: SubjectHistoryRequest =
        serde_json::from_value(json!({"entity_id":a,"known_at":knowledge})).unwrap();
    assert!(
        t.get_subject_history(&query)
            .unwrap()
            .events
            .iter()
            .all(|e| e.knowledge.commit_seq <= seq)
    );
    let missing: SubjectHistoryRequest =
        serde_json::from_value(json!({"entity_id":Uuid::new_v4()})).unwrap();
    assert_eq!(
        t.get_subject_history(&missing).unwrap_err().code,
        "NOT_FOUND"
    );
}

#[test]
fn subject_cursor_filters_principal_cancellation_and_atomic_rollback() {
    let (_dir, _store, t) = init(
        Options {
            max_batch_operations: 10,
            ..Options::default()
        },
        IdentityConfig::default(),
    );
    let a = entity(&t, "a");
    for key in ["one", "two"] {
        execute(
            &t,
            change(&t, key, key, bind(a), manual("2026-01-01T00:00:00Z", None)),
        )
        .unwrap();
    }
    let request = json!({"entity_id":a,"valid_at":"2026-01-02T00:00:00Z","limit":1});
    let prepared = dsl::Prepared::parse("ids with $q").unwrap();
    let params = serde_json::from_value(json!({"q":request})).unwrap();
    let dsl::Output::SubjectIds(first) = prepared
        .execute_with(&t, &params, None, &WriteOptions::default(), "alice")
        .unwrap()
    else {
        panic!()
    };
    let cursor = first.cursor.unwrap();
    assert_eq!(
        prepared
            .execute_with(&t, &params, Some(&cursor), &WriteOptions::default(), "bob")
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    let changed = serde_json::from_value(
        json!({"q":{"entity_id":a,"valid_at":"2026-01-03T00:00:00Z","limit":1}}),
    )
    .unwrap();
    assert!(
        prepared
            .execute_with(
                &t,
                &changed,
                Some(&cursor),
                &WriteOptions::default(),
                "alice"
            )
            .is_err()
    );
    prepared
        .execute_with(
            &t,
            &params,
            Some(&cursor),
            &WriteOptions::default(),
            "alice",
        )
        .unwrap();
    let cancel = ReadCancellation::default();
    cancel.cancel();
    let error = t
        .read_with(&ReadOptions {
            cancellation: Some(cancel),
            ..Default::default()
        })
        .err()
        .unwrap();
    assert_eq!(error.code, "CANCELLED");
    let before = subject_events(&t, a).len();
    let seq = t.engine().seq();
    let changes: Vec<_> = ["three", "four"]
        .iter()
        .map(|key| {
            change(
                &t,
                "too-many",
                key,
                bind(a),
                manual("2026-01-01T00:00:00Z", None),
            )["changes"][0]
                .clone()
        })
        .collect();
    assert_eq!(
        execute(
            &t,
            json!({"action":"bindings","operation_id":"too-many","changes":changes})
        )
        .unwrap_err()
        .code,
        "RESOURCE_EXHAUSTED"
    );
    assert_eq!(t.engine().seq(), seq);
    assert_eq!(subject_ids(&t, a, "2026-01-02T00:00:00Z", None).len(), 2);
    assert_eq!(subject_events(&t, a).len(), before);
}

#[test]
fn subject_events_preserve_nested_attributes_and_integer_precision() {
    let (_dir, _store, t) = init(Options::default(), IdentityConfig::default());
    let value = json!({"nested":{"nullable":null},"说明":"原样","integer":u64::MAX});
    let receipt = t
        .identity_write(
            &context(),
            IdentityWrite::CreateEntity {
                operation_id: "literal-attribute".into(),
                attributes: json!({"value":value}),
            },
        )
        .unwrap();
    let entity = receipt.entity.unwrap().entity_id;
    assert_eq!(
        t.identity_entity(entity).unwrap().attributes["value"],
        value
    );
    let events = subject_events(&t, entity);
    assert_eq!(events[0].attributes[0].after.value, value);
    assert_eq!(
        t.identity_operation("literal-attribute")
            .unwrap()
            .entity
            .unwrap()
            .attributes["value"],
        value
    );
}
