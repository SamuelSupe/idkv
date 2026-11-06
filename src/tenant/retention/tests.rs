use super::*;
use crate::{identity::*, *};
use serde_json::{Value, json};
use std::time::Duration;

fn setup() -> (tempfile::TempDir, Store, TenantHandle) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        dir.path(),
        Options {
            memtable_bytes: 1 << 20,
            cache_bytes: 1 << 20,
            ..Options::default()
        },
    )
    .unwrap();
    let id = store.create_tenant("retention").unwrap().id;
    let tenant = store.tenant(id).unwrap();
    (dir, store, tenant)
}
fn clean(t: &TenantHandle) {
    for _ in 0..100 {
        t.retention_step().unwrap();
        t.gc_step().unwrap();
    }
}
fn pause() {
    std::thread::sleep(Duration::from_millis(1100));
}
fn options(t: &TenantHandle, id: &str) -> WriteOptions {
    let c = t.retry_context("worker").unwrap();
    WriteOptions {
        expected_history: Some(c.history),
        operation_id: Some(id.into()),
        retry_token: Some(c.retry_token),
    }
}
fn put_op() -> Vec<WriteOp> {
    vec![WriteOp::Put {
        document: json!({"value":1}),
        if_rev: None,
    }]
}

#[test]
fn expired_receipts_never_repeat_generated_writes_and_current_documents_survive() {
    let (dir, store, t) = setup();
    t.create_collection("items").unwrap();
    t.set_retention(1).unwrap();
    let opts = options(&t, "generated");
    let first = t
        .write_batch_with("items", put_op(), &opts, "worker")
        .unwrap();
    assert_eq!(
        t.write_batch_with("items", put_op(), &opts, "worker")
            .unwrap()[0]
            .key,
        first[0].key
    );
    let other = store.create_tenant("other").unwrap().id;
    let other = store.tenant(other).unwrap();
    other.set_retention(1).unwrap();
    other.create_collection("items").unwrap();
    let mut foreign = opts.clone();
    foreign.expected_history = Some(other.history());
    assert_eq!(
        other
            .write_batch_with("items", put_op(), &foreign, "worker")
            .unwrap_err()
            .code,
        "IDEMPOTENCY_EXPIRED"
    );
    let mut changed = put_op();
    changed.push(WriteOp::Delete {
        key: "other".into(),
        if_rev: None,
    });
    assert_eq!(
        t.write_batch_with("items", changed, &opts, "worker")
            .unwrap_err()
            .code,
        "OPERATION_CONFLICT"
    );
    pause();
    assert_eq!(
        t.write_batch_with("items", put_op(), &opts, "worker")
            .unwrap_err()
            .code,
        "IDEMPOTENCY_EXPIRED"
    );
    clean(&t);
    let before = t.engine().seq();
    assert_eq!(
        t.write_batch_with("items", put_op(), &opts, "worker")
            .unwrap_err()
            .code,
        "IDEMPOTENCY_EXPIRED"
    );
    assert_eq!(before, t.engine().seq());
    assert!(t.get("items", &first[0].key).unwrap().is_some());
    let id = t.id();
    drop(t);
    drop(other);
    drop(store);
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store.tenant(id).unwrap();
    assert_eq!(
        t.write_batch_with("items", put_op(), &opts, "worker")
            .unwrap_err()
            .code,
        "IDEMPOTENCY_EXPIRED"
    );
    assert_eq!(
        t.query("items", Query::default()).unwrap().documents.len(),
        1
    );
}

#[test]
fn shortening_policy_cannot_remove_receipts_while_the_original_token_is_valid() {
    let (_dir, _store, t) = setup();
    t.create_collection("items").unwrap();
    t.set_retention(3).unwrap();
    let opts = options(&t, "delayed-first-attempt");
    t.set_retention(1).unwrap();
    let first = t
        .write_batch_with("items", put_op(), &opts, "worker")
        .unwrap();
    pause();
    clean(&t);
    assert_eq!(
        t.write_batch_with("items", put_op(), &opts, "worker")
            .unwrap()[0]
            .key,
        first[0].key
    );
    std::thread::sleep(Duration::from_millis(2100));
    clean(&t);
    assert_eq!(
        t.write_batch_with("items", put_op(), &opts, "worker")
            .unwrap_err()
            .code,
        "IDEMPOTENCY_EXPIRED"
    );
    assert!(t.get("items", &first[0].key).unwrap().is_some());
}

#[test]
fn persisted_expiry_watermark_rejects_retry_after_clock_rollback() {
    let (dir, store, t) = setup();
    t.create_collection("items").unwrap();
    t.set_retention(60).unwrap();
    let context = t.retry_context("worker").unwrap();
    t.set_retention(1).unwrap();
    let opts = WriteOptions {
        expected_history: Some(context.history.clone()),
        operation_id: Some("clock-rollback".into()),
        retry_token: Some(context.retry_token),
    };
    let first = t
        .write_batch_with("items", put_op(), &opts, "worker")
        .unwrap();
    let authorization = t
        .engine()
        .transact(|tx| {
            // A cleanup batch can remove a receipt before its token at the same
            // deadline. Reproduce that durable boundary after a wall-clock rollback.
            let receipt = tx.prefix_first(b"R", 1)?.remove(0).0;
            let at: u64 = parse(&tx.get(&key(DEADLINE, &receipt))?.unwrap())?;
            assert_eq!(at, context.submit_before_ms);
            tx.delete(receipt)?;
            let mut state: gc::State = parse(&tx.get(STATE)?.unwrap())?;
            state.watermark_ms = at;
            put(tx, STATE.to_vec(), &state)?;
            assert!(
                tx.get(&key(TOKENS, context.retry_token.as_bytes()))?
                    .is_some()
            );
            Ok(authorize_new(
                tx,
                opts.retry_token,
                "worker",
                &context.history,
            ))
        })
        .unwrap();
    assert_eq!(authorization.unwrap_err().code, "IDEMPOTENCY_EXPIRED");
    assert_eq!(
        t.write_batch_with("items", put_op(), &opts, "worker")
            .unwrap_err()
            .code,
        "IDEMPOTENCY_EXPIRED"
    );
    assert!(t.get("items", &first[0].key).unwrap().is_some());
    let fresh = t.retry_context("worker").unwrap();
    assert!(fresh.submit_before_ms >= context.submit_before_ms + 1000);
    let id = t.id();
    drop(t);
    drop(store);
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store.tenant(id).unwrap();
    assert_eq!(
        t.write_batch_with("items", put_op(), &opts, "worker")
            .unwrap_err()
            .code,
        "IDEMPOTENCY_EXPIRED"
    );
    assert_eq!(
        t.query("items", Query::default()).unwrap().documents.len(),
        1
    );
}

#[test]
fn retention_makes_progress_with_small_byte_batches_and_long_receipt_keys() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        dir.path(),
        Options {
            max_batch_bytes: 16 << 10,
            ..Options::default()
        },
    )
    .unwrap();
    let t = store
        .tenant(store.create_tenant("small-gc-batches").unwrap().id)
        .unwrap();
    t.create_collection("items").unwrap();
    let principal = "p".repeat(128);
    for i in 0..40 {
        t.write_batch_with(
            "items",
            put_op(),
            &WriteOptions {
                expected_history: Some(t.history()),
                operation_id: Some(format!("{i:03}{}", "r".repeat(125))),
                retry_token: None,
            },
            &principal,
        )
        .unwrap();
    }
    t.set_retention(1).unwrap();
    clean(&t);
    assert_eq!(
        t.retention_status().unwrap()["gc"]["migration_complete"],
        true
    );
    pause();
    clean(&t);
    assert!(t.retention_status().unwrap()["next_expiration_ms"].is_null());
    assert_eq!(
        t.query("items", Query::default()).unwrap().documents.len(),
        40
    );
    t.engine().verify().unwrap();
}
fn collection(t: &TenantHandle, name: &str) {
    t.create_collection(name).unwrap();
    t.create_index(
        name,
        IndexSpec {
            name: "key".into(),
            fields: vec![IndexField {
                field: "/_key".into(),
                order: Order::Asc,
            }],
            unique: true,
            include: vec![],
        },
    )
    .unwrap();
    t.create_lookup(
        name,
        LookupDefinition {
            name: "key".into(),
            index: "key".into(),
            select: vec!["/_key".into()],
            mode: MatchMode::All,
            max_matches: 100,
        },
    )
    .unwrap();
}
fn publish(
    t: &TenantHandle,
    resource: &str,
    collection: &str,
    previous: Option<Uuid>,
    token: Option<Uuid>,
) -> PublishedLookup {
    t.publish_lookup_with(
        resource,
        Publication {
            version: Uuid::new_v4(),
            collection: collection.into(),
            lookup: "key".into(),
            expected_version: previous,
            retry_token: token,
        },
        "worker",
    )
    .unwrap()
}
#[test]
fn superseded_publications_preserve_shared_current_collections_and_old_snapshots() {
    let (_dir, _store, t) = setup();
    collection(&t, "old");
    collection(&t, "new");
    t.put("old", json!({"_key":"k","v":"old"}), None).unwrap();
    let a = publish(&t, "a", "old", None, None);
    let b = publish(&t, "b", "old", None, None);
    t.set_retention(1).unwrap();
    pause();
    clean(&t);
    assert_eq!(
        t.published_lookup("a", Some(a.version)).unwrap().version,
        a.version
    );
    let token = t.retry_context("worker").unwrap().retry_token;
    let newest = publish(&t, "a", "new", Some(a.version), Some(token));
    let snapshot = t.engine().snapshot().unwrap();
    pause();
    clean(&t);
    assert_eq!(
        t.published_lookup("a", Some(a.version)).unwrap_err().code,
        "VERSION_UNAVAILABLE"
    );
    assert!(t.get("old", "k").unwrap().is_some());
    assert_eq!(
        t.published_lookup("b", Some(b.version)).unwrap().version,
        b.version
    );
    let token = t.retry_context("worker").unwrap().retry_token;
    publish(&t, "b", "new", Some(b.version), Some(token));
    pause();
    clean(&t);
    t.engine().flush().unwrap();
    t.engine().compact(true).unwrap();
    assert_eq!(t.get("old", "k").unwrap_err().code, "NOT_FOUND");
    let key = document_key(a.collection_id, "k");
    let old = snapshot
        .get(&key, &mut ReadStats::default())
        .unwrap()
        .unwrap();
    assert_eq!(crate::codec::field(&old, "/v").unwrap(), Some(json!("old")));
    assert_eq!(
        t.published_lookup("a", None).unwrap().version,
        newest.version
    );
    let old_lookup = t
        .lookup_snapshot(
            "old",
            "key",
            &[json!({"_key":"k"})],
            snapshot.clone(),
            &crate::read::ReadBudget::new(&crate::ReadOptions::default(), &t.engine().options)
                .unwrap(),
        )
        .unwrap();
    assert_eq!(old_lookup.matches, vec![vec![json!({"_key":"k"})]]);
    drop(snapshot);
    t.engine().compact(true).unwrap();
    t.engine().verify().unwrap();
}
fn actor() -> IdentityContext {
    IdentityContext {
        source: "test".into(),
        actor: "worker".into(),
        can_correct: true,
    }
}
fn identity(t: &TenantHandle, v: Value) -> IdentityReceipt {
    t.identity_write(&actor(), serde_json::from_value(v).unwrap())
        .unwrap()
}
fn id(value: &str) -> Value {
    json!({"kind":"account","namespace":"test","value":value})
}
fn resolve(t: &TenantHandle, value: &str, known: Uuid) -> Result<ResolveResult> {
    t.identity_resolve(
        &serde_json::from_value(
            json!({"identifier":id(value),"valid_at":"2026-01-01T00:00:00Z","known_at":known}),
        )
        .unwrap(),
    )
}
#[test]
fn temporal_gc_keeps_predecessor_baselines_including_empty_timelines_and_latest_state() {
    let (_dir, _store, t) = setup();
    t.identity_initialize(&actor(), IdentityConfig::default())
        .unwrap();
    let entity = identity(
        &t,
        json!({"action":"create_entity","operation_id":"entity","attributes":{}}),
    )
    .entity
    .unwrap()
    .entity_id;
    for name in ["unchanged", "empty"] {
        identity(
            &t,
            json!({"action":"bindings","operation_id":name,"changes":[{"identifier":id(name),"expected_revision":"absent","effective":{"mode":"manual","from":"2025-01-01T00:00:00Z"},"change":{"action":"bind","entity_id":entity,"reason":"seed"}}]}),
        );
    }
    let timeline = t
        .identity_timeline(&serde_json::from_value(id("empty")).unwrap())
        .unwrap()
        .unwrap();
    let old = identity(
        &t,
        json!({"action":"bindings","operation_id":"unbind","changes":[{"identifier":id("empty"),"expected_revision":timeline.revision,"effective":{"mode":"manual","from":"2025-01-01T00:00:00Z"},"change":{"action":"unbind","reason":"clear"}}]}),
    );
    let head = identity(
        &t,
        json!({"action":"create_entity","operation_id":"unrelated","attributes":{}}),
    );
    let expected = resolve(&t, "unchanged", head.knowledge.version)
        .unwrap()
        .binding;
    t.set_retention(1).unwrap();
    pause();
    clean(&t);
    assert_eq!(
        resolve(&t, "unchanged", head.knowledge.version)
            .unwrap()
            .binding,
        expected
    );
    assert!(
        resolve(&t, "empty", head.knowledge.version)
            .unwrap()
            .binding
            .is_none()
    );
    assert_eq!(
        resolve(&t, "empty", old.knowledge.version)
            .unwrap_err()
            .code,
        "KNOWLEDGE_UNAVAILABLE"
    );
    assert!(t.identity_entity(entity).is_ok());
    let token = t.retry_context("worker").unwrap();
    let req: IdentityWrite = serde_json::from_value(
        json!({"action":"create_entity","operation_id":"next","attributes":{}}),
    )
    .unwrap();
    let opts = WriteOptions {
        retry_token: Some(token.retry_token),
        expected_history: Some(token.history),
        operation_id: None,
    };
    let latest = t.identity_write_with(&actor(), req.clone(), &opts).unwrap();
    assert!(resolve(&t, "unchanged", head.knowledge.version).is_ok());
    pause();
    clean(&t);
    assert_eq!(
        resolve(&t, "unchanged", head.knowledge.version)
            .unwrap_err()
            .code,
        "KNOWLEDGE_UNAVAILABLE"
    );
    assert_eq!(
        resolve(&t, "unchanged", latest.knowledge.version)
            .unwrap()
            .binding,
        expected
    );
    assert_eq!(
        t.identity_write_with(&actor(), req, &opts)
            .unwrap_err()
            .code,
        "IDEMPOTENCY_EXPIRED"
    );
}
#[test]
fn rolling_publications_continue_beyond_the_collection_limit() {
    let (_dir, _store, t) = setup();
    t.set_retention(1).unwrap();
    let mut previous = None;
    let mut context = t.retry_context("worker").unwrap();
    for i in 0..1050 {
        if now_ms().unwrap() + 200 >= context.submit_before_ms {
            context = t.retry_context("worker").unwrap();
        }
        let name = format!("v{i}");
        collection(&t, &name);
        let out = publish(&t, "rolling", &name, previous, Some(context.retry_token));
        previous = Some(out.version);
        t.retention_step().unwrap();
        t.gc_step().unwrap();
    }
    pause();
    clean(&t);
    assert_eq!(t.collections().unwrap().len(), 1);
    assert_eq!(t.retention_status().unwrap()["expiration_lag_ms"], 0);
    assert_eq!(
        t.published_lookup("rolling", None).unwrap().version,
        previous.unwrap()
    );
}

#[test]
fn receipt_metadata_does_not_reduce_the_public_document_batch_limit() {
    let (_dir, _store, t) = setup();
    t.create_collection("full_batch").unwrap();
    t.set_retention(60).unwrap();
    let opts = options(&t, "full-batch");
    let count = t.engine().options.max_batch_operations;
    let batch = (0..count)
        .map(|n| WriteOp::Put {
            document: json!({"_key":n.to_string(),"value":n}),
            if_rev: None,
        })
        .collect::<Vec<_>>();
    let first = t
        .write_batch_with("full_batch", batch.clone(), &opts, "worker")
        .unwrap();
    assert_eq!(first.len(), count);
    assert_eq!(
        t.write_batch_with("full_batch", batch, &opts, "worker")
            .unwrap()[count - 1]
            .rev,
        first[count - 1].rev
    );
    assert_eq!(
        t.get("full_batch", &(count - 1).to_string())
            .unwrap()
            .unwrap()["value"],
        count - 1
    );
}
