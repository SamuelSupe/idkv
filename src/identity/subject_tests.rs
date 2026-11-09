use super::{storage::*, subjects::STATE, *};
use crate::{Options, ReadOptions, Store, TenantHandle, tenant::catalog::document_key};
use serde_json::{Value, json};
fn actor() -> IdentityContext {
    IdentityContext {
        source: "fixture".into(),
        actor: "test".into(),
        can_correct: true,
    }
}
fn write(t: &TenantHandle, v: Value) -> IdentityReceipt {
    t.identity_write(&actor(), serde_json::from_value(v).unwrap())
        .unwrap()
}
fn ids(t: &TenantHandle, entity: Uuid, known: Option<Uuid>) -> Result<SubjectIdentifiersResult> {
    t.get_subject_identifiers(
        &serde_json::from_value(
            json!({"entity_id":entity,"valid_at":"2026-01-02T00:00:00Z","known_at":known}),
        )
        .unwrap(),
    )
}

#[test]
fn legacy_subject_backfill_resumes_after_restart_and_preserves_concurrent_corrections() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let tenant = store.create_tenant("legacy").unwrap().id;
    let t = store.tenant(tenant).unwrap();
    drop(store); // Run bounded maintenance explicitly, including across a restart.
    t.identity_initialize(&actor(), IdentityConfig::default())
        .unwrap();
    let a = write(
        &t,
        json!({"action":"create_entity","operation_id":"a","attributes":{}}),
    )
    .entity
    .unwrap()
    .entity_id;
    let b = write(
        &t,
        json!({"action":"create_entity","operation_id":"b","attributes":{}}),
    )
    .entity
    .unwrap()
    .entity_id;
    let identifier = json!({"kind":"account","namespace":"test","value":"one"});
    let first = write(
        &t,
        json!({"action":"bindings","operation_id":"bind","changes":[{"identifier":identifier,"expected_revision":"absent","effective":{"mode":"manual","from":"2026-01-01T00:00:00Z"},"change":{"action":"bind","entity_id":a,"reason":"seed"}}]}),
    );
    let second = write(
        &t,
        json!({"action":"bindings","operation_id":"rebind","changes":[{"identifier":identifier,"expected_revision":first.timelines[0].revision,"effective":{"mode":"manual","from":"2026-01-01T00:00:00Z"},"change":{"action":"rebind","entity_id":b,"reason":"correct"}}]}),
    );
    let c = t
        .collection_id(&t.engine().snapshot().unwrap(), COLLECTION)
        .unwrap();
    t.engine()
        .transact(|tx| {
            for text in ["subject:", "event:", "subject-event:"] {
                for (key, _) in
                    tx.prefix_first(&super::subject_maintenance::prefix(c, text), 100)?
                {
                    tx.delete(key)?;
                }
            }
            tx.delete(document_key(c, STATE))
        })
        .unwrap();
    assert_eq!(ids(&t, a, None).unwrap_err().code, "INDEX_NOT_READY");
    t.identity_subject_step().unwrap();
    assert_eq!(ids(&t, a, None).unwrap_err().code, "INDEX_NOT_READY");
    // This new history row sorts before the saved migration cursor.
    let third = write(
        &t,
        json!({"action":"bindings","operation_id":"again","changes":[{"identifier":identifier,"expected_revision":second.timelines[0].revision,"effective":{"mode":"manual","from":"2026-01-01T00:00:00Z"},"change":{"action":"rebind","entity_id":a,"reason":"new fact"}}]}),
    );
    drop(t);
    let store = Store::open(
        dir.path(),
        Options {
            query_ms: 1,
            max_query_bytes: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let t = store.tenant(tenant).unwrap();
    drop(store);
    for _ in 0..20 {
        t.identity_subject_step().unwrap();
    }
    assert!(
        t.engine()
            .transact_maintenance(|tx| super::subjects_ready(tx, c))
            .unwrap()
    );
    drop(t);
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store.tenant(tenant).unwrap();
    drop(store);
    for (version, owner) in [
        (first.knowledge.version, a),
        (second.knowledge.version, b),
        (third.knowledge.version, a),
    ] {
        for entity in [a, b] {
            assert_eq!(
                ids(&t, entity, Some(version)).unwrap().identifiers.len(),
                usize::from(entity == owner)
            );
        }
    }
    let history: SubjectHistoryRequest = serde_json::from_value(json!({"entity_id":a})).unwrap();
    let events = t.get_subject_history(&history).unwrap();
    assert!(events.coverage.legacy_history_missing);
    assert_eq!(events.events.len(), 1);
    assert_eq!(events.events[0].operation_id, "again");
}

#[test]
fn subject_pages_advance_over_tombstones_and_failed_reads_keep_the_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store
        .tenant(store.create_tenant("physical-pages").unwrap().id)
        .unwrap();
    drop(store);
    t.identity_initialize(&actor(), IdentityConfig::default())
        .unwrap();
    let entity = write(
        &t,
        json!({"action":"create_entity","operation_id":"a","attributes":{}}),
    )
    .entity
    .unwrap()
    .entity_id;
    let identifier = Identifier {
        kind: "id".into(),
        namespace: "n".into(),
        value: "last".into(),
    };
    let receipt = write(
        &t,
        json!({"action":"bindings","operation_id":"bind","changes":[{"identifier":identifier,"expected_revision":"absent","effective":{"mode":"manual","from":"2026-01-01T00:00:00Z"},"change":{"action":"bind","entity_id":entity,"reason":"seed"}}]}),
    );
    let c = t
        .collection_id(&t.engine().snapshot().unwrap(), COLLECTION)
        .unwrap();
    t.engine()
        .transact(|tx| {
            for n in 0..90u64 {
                tx.delete(document_key(
                    c,
                    &format!(
                        "subject:{entity}:{}:{:016x}",
                        digest(&identifier)?,
                        !(receipt.knowledge.ordinal + n + 1)
                    ),
                ))?;
            }
            Ok(())
        })
        .unwrap();
    let mut request: SubjectIdentifiersRequest = serde_json::from_value(
        json!({"entity_id":entity,"valid_at":"2026-01-02T00:00:00Z","limit":1}),
    )
    .unwrap();
    let first = t.get_subject_identifiers(&request).unwrap();
    assert!(first.identifiers.is_empty());
    assert!(first.cursor.is_some());
    request.cursor = first.cursor;
    let baseline_memory = t.engine().memory_bytes();
    for _ in 0..3 {
        assert_eq!(
            t.read_with(&ReadOptions {
                max_scan: Some(1),
                ..Default::default()
            })
            .unwrap()
            .get_subject_identifiers(&request)
            .unwrap_err()
            .code,
            "RESOURCE_EXHAUSTED"
        );
        assert_eq!(t.engine().memory_bytes(), baseline_memory);
    }
    let mut items = Vec::new();
    for _ in 0..20 {
        let page = t.get_subject_identifiers(&request).unwrap();
        items.extend(page.identifiers);
        request.cursor = page.cursor;
        if request.cursor.is_none() {
            break;
        }
    }
    assert!(request.cursor.is_none());
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].identifier, identifier);
    assert_eq!(t.engine().active_snapshots(), 0);
}

#[test]
fn oversized_subject_history_releases_the_snapshot_and_can_be_read_in_smaller_pages() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        dir.path(),
        Options {
            max_response_bytes: 1 << 20,
            ..Default::default()
        },
    )
    .unwrap();
    let t = store
        .tenant(store.create_tenant("event-response-limit").unwrap().id)
        .unwrap();
    drop(store);
    t.identity_initialize(&actor(), IdentityConfig::default())
        .unwrap();
    let mut entity = write(&t, json!({"action":"create_entity","operation_id":"entity","attributes":{"blob":"a".repeat(96 << 10)}})).entity.unwrap();
    for n in 0..8 {
        entity = write(&t, json!({"action":"update_entity","operation_id":format!("change-{n}"),"entity_id":entity.entity_id,"expected_revision":entity.revision,"attributes":{"blob":char::from(b'b'+n).to_string().repeat(96 << 10)}})).entity.unwrap();
    }
    let mut request: SubjectHistoryRequest =
        serde_json::from_value(json!({"entity_id":entity.entity_id,"limit":100})).unwrap();
    let small = SubjectHistoryRequest {
        limit: 1,
        ..request.clone()
    };
    let warm = t.get_subject_history(&small).unwrap();
    t.release_cursor(warm.cursor.as_ref().unwrap()).unwrap();
    drop(warm);
    let memory = t.engine().memory_bytes();
    for _ in 0..2 {
        let error = t.get_subject_history(&request).unwrap_err();
        assert_eq!(error.code, "RESOURCE_EXHAUSTED");
        assert_eq!(t.engine().active_snapshots(), 0);
        assert_eq!(t.engine().memory_bytes(), memory);
    }
    request.limit = 1;
    let mut events = Vec::new();
    loop {
        let page = t.get_subject_history(&request).unwrap();
        events.extend(page.events);
        request.cursor = page.cursor;
        if request.cursor.is_none() {
            break;
        }
    }
    assert_eq!(events.len(), 9);
    assert_eq!(t.identity_entity(entity.entity_id).unwrap(), entity);
    assert_eq!(t.engine().active_snapshots(), 0);
}
