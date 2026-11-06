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
        max_batch_operations: 6,
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
