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
    super::subject_maintenance::step(&t, 1).unwrap();
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
            max_batch_operations: 2,
            max_batch_bytes: 16 << 10,
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
            .transact_maintenance(
                |tx| super::subject_history_gc_limit(tx, c).map(|limit| limit.is_none())
            )
            .unwrap()
    );
    assert_eq!(
        t.retention_status().unwrap()["identity_subjects"]["processed_history_rows"],
        2
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

#[test]
fn legacy_migration_batches_history_and_retains_reverse_results() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store
        .tenant(store.create_tenant("migration-batch").unwrap().id)
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
        namespace: "test".into(),
        value: "legacy".into(),
    };
    let receipt = write(
        &t,
        json!({"action":"bindings","operation_id":"bind","changes":[{"identifier":identifier,"expected_revision":"absent","effective":{"mode":"manual","from":"2026-01-01T00:00:00Z"},"change":{"action":"bind","entity_id":entity,"reason":"legacy"}}]}),
    );
    let c = t
        .collection_id(&t.engine().snapshot().unwrap(), COLLECTION)
        .unwrap();
    let timeline = t.identity_timeline(&identifier).unwrap().unwrap();
    t.engine()
        .transact(|tx| {
            for ordinal in 3..=258u64 {
                let key = format!("history:{}:{:016x}", digest(&identifier)?, !ordinal);
                tx.put(
                    document_key(c, &key),
                    crate::codec::encode(&json!({"_key":key,"data":timeline}))?,
                )?;
            }
            let mut head = receipt.knowledge.clone();
            head.version = Uuid::new_v4();
            head.ordinal = 258;
            head.commit_seq = tx.seq;
            for key in [
                "head:knowledge".to_owned(),
                format!("knowledge:{}", head.version),
            ] {
                tx.put(
                    document_key(c, &key),
                    crate::codec::encode(&json!({"_key":key,"data":head}))?,
                )?;
            }
            tx.delete(document_key(c, STATE))?;
            tx.delete(document_key(
                c,
                &format!(
                    "subject:{entity}:{}:{:016x}",
                    digest(&identifier)?,
                    !receipt.knowledge.ordinal
                ),
            ))?;
            Ok(())
        })
        .unwrap();
    t.engine().flush().unwrap();
    let started = std::time::Instant::now();
    let mut steps = 0;
    loop {
        t.identity_subject_step().unwrap();
        steps += 1;
        if t.engine()
            .transact_maintenance(|tx| {
                super::subject_history_gc_limit(tx, c).map(|limit| limit.is_none())
            })
            .unwrap()
        {
            break;
        }
        assert!(
            steps < 32,
            "migration still advances one history row per commit: {steps} steps"
        );
    }
    println!(
        "migration: 257 history rows, {steps} commits, {:?}",
        started.elapsed()
    );
    let progress = t.retention_status().unwrap()["identity_subjects"].clone();
    assert_eq!(progress["processed_history_rows"], 257);
    let mut request: SubjectIdentifiersRequest = serde_json::from_value(json!({"entity_id":entity,"valid_at":"2026-01-02T00:00:00Z","known_at":receipt.knowledge.version})).unwrap();
    let mut matches = Vec::new();
    loop {
        let page = t.get_subject_identifiers(&request).unwrap();
        matches.extend(page.identifiers);
        request.cursor = page.cursor;
        if request.cursor.is_none() {
            break;
        }
    }
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].identifier, identifier);
    let seq = t.engine().seq();
    for _ in 0..4 {
        t.identity_subject_step().unwrap();
    }
    assert_eq!(
        t.engine().seq(),
        seq,
        "idle maintenance must not generate WAL traffic"
    );
    let id = t.id();
    drop(t);
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store.tenant(id).unwrap();
    assert_eq!(t.retention_status().unwrap()["identity_subjects"], progress);
    t.identity_subject_step().unwrap();
    let metrics = store.open_engine_metrics();
    assert_eq!(metrics["identity_subjects_ready_tenants"], 1);
    assert_eq!(metrics["identity_subjects_processed_history_rows"], 257);
}

#[test]
fn recorded_from_finishes_at_the_first_older_event() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store
        .tenant(store.create_tenant("recorded-range").unwrap().id)
        .unwrap();
    drop(store);
    t.identity_initialize(&actor(), IdentityConfig::default())
        .unwrap();
    let mut receipt = write(
        &t,
        json!({"action":"create_entity","operation_id":"a","attributes":{"n":0}}),
    );
    for n in 1..=32 {
        let entity = receipt.entity.unwrap();
        receipt = write(
            &t,
            json!({"action":"update_entity","operation_id":format!("update-{n}"),"entity_id":entity.entity_id,"expected_revision":entity.revision,"attributes":{"n":n}}),
        );
    }
    let from = chrono::DateTime::from_timestamp_micros(receipt.knowledge.recorded_at_us)
        .unwrap()
        .to_rfc3339();
    let request: SubjectHistoryRequest = serde_json::from_value(
        json!({"entity_id":receipt.entity.unwrap().entity_id,"recorded_from":from,"limit":2}),
    )
    .unwrap();
    let page = t
        .read_with(&ReadOptions {
            max_scan: Some(8),
            ..Default::default()
        })
        .unwrap()
        .get_subject_history(&request)
        .unwrap();
    assert_eq!(page.events.len(), 1);
    assert_eq!(page.events[0].operation_id, "update-32");
    assert!(
        page.cursor.is_none(),
        "older events must not produce empty continuation pages"
    );
    assert!(page.stats.scanned_entries <= 2);
    // A lagging expiry sweep may leave all the older payloads physically present.
    t.set_retention(3600).unwrap();
    let c = t
        .collection_id(&t.engine().snapshot().unwrap(), COLLECTION)
        .unwrap();
    t.engine()
        .transact_maintenance(|tx| {
            for ordinal in 1..page.events[0].knowledge.ordinal {
                let event = document_key(c, &format!("event:{:016x}:00000000", !ordinal));
                crate::tenant::catalog::put(tx, [b"~d/".as_slice(), &event].concat(), &0u64)?;
            }
            Ok(())
        })
        .unwrap();
    let expired_tail = t
        .read_with(&ReadOptions {
            max_scan: Some(8),
            ..Default::default()
        })
        .unwrap()
        .get_subject_history(&request)
        .unwrap();
    assert_eq!(expired_tail.events.len(), 1);
    assert!(expired_tail.cursor.is_none());
    assert!(expired_tail.stats.scanned_entries <= 2);
}

#[test]
fn migration_releases_completed_identifiers_to_gc_and_link_sweeps_batch() {
    use crate::tenant::catalog::put;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store
        .tenant(store.create_tenant("migration-gc").unwrap().id)
        .unwrap();
    drop(store);
    let genesis = t
        .identity_initialize(&actor(), IdentityConfig::default())
        .unwrap();
    let receipt = write(
        &t,
        json!({"action":"create_entity","operation_id":"entity","attributes":{}}),
    );
    let entity = receipt.entity.unwrap().entity_id;
    let c = t
        .collection_id(&t.engine().snapshot().unwrap(), COLLECTION)
        .unwrap();
    let mut identifiers: Vec<Identifier> = ["one", "two"]
        .into_iter()
        .map(|value| Identifier {
            kind: "id".into(),
            namespace: "test".into(),
            value: value.into(),
        })
        .collect();
    identifiers.sort_by_key(|id| digest(id).unwrap());
    let binding = Binding {
        valid_from_us: parse_time("2026-01-01T00:00:00Z").unwrap(),
        valid_to_us: None,
        entity_id: entity,
        binding_id: Uuid::new_v4(),
        version_id: Uuid::new_v4(),
        source: "fixture".into(),
        evidence_id: None,
        assurance: "asserted".into(),
    };
    let mut retained = receipt.knowledge.clone();
    retained.version = Uuid::new_v4();
    retained.ordinal = 64;
    let mut head = retained.clone();
    head.version = Uuid::new_v4();
    head.ordinal = 129;
    t.set_retention(60).unwrap();
    t.engine().transact_maintenance(|tx| {
        put(tx, b"~gc".to_vec(), &json!({"migration_phase":7,"migration_after":null,"history_after":null,"history_identifier":null,"baseline_kept":false,"sweeping_floor":0,"swept_floor":0,"reclaimed_records":0,"completed_rounds":0,"watermark_ms":0}))?;
        for old in [genesis.version, receipt.knowledge.version] { tx.delete(document_key(c, &format!("knowledge:{old}")))?; }
        put(tx, [b"~k/".as_slice(), &64u64.to_be_bytes()].concat(), &retained.version)?;
        put(tx, [b"~k/".as_slice(), &129u64.to_be_bytes()].concat(), &head.version)?;
        for knowledge in [&retained, &head] {
            let key = format!("knowledge:{}", knowledge.version);
            tx.put(document_key(c, &key), crate::codec::encode(&json!({"_key":key,"data":knowledge}))?)?;
        }
        tx.put(document_key(c, "head:knowledge"), crate::codec::encode(&json!({"_key":"head:knowledge","data":head}))?)?;
        for (i, id) in identifiers.iter().enumerate() {
            for ordinal in 2..=129u64 {
                let timeline = Timeline { identifier:id.clone(),revision:Uuid::new_v4(),segments:if i==1 && ordinal>=80 { vec![] } else {vec![binding.clone()]} };
                let key = format!("history:{}:{:016x}", digest(id)?, !ordinal);
                tx.put(document_key(c, &key), crate::codec::encode(&json!({"_key":key,"data":timeline}))?)?;
                if ordinal==129 { tx.put(document_key(c, &timeline_key(id)?), crate::codec::encode(&json!({"_key":timeline_key(id)?,"data":timeline}))?)?; }
            }
        }
        tx.delete(document_key(c, STATE))?;
        Ok(())
    }).unwrap();
    let count = |text: &str| {
        t.engine()
            .transact_maintenance(|tx| {
                Ok(tx
                    .prefix_first(&super::subject_maintenance::prefix(c, text), 1000)?
                    .len())
            })
            .unwrap()
    };
    let first = format!("history:{}:", digest(&identifiers[0]).unwrap());
    let second = format!("history:{}:", digest(&identifiers[1]).unwrap());
    let mut completed_prefix = false;
    for _ in 0..128 {
        super::subject_maintenance::step(&t, 16).unwrap();
        let state: super::subjects::SubjectState = Scratch::maintenance(&t)
            .get(
                &t.engine().snapshot().unwrap(),
                c,
                STATE,
                &mut crate::ReadStats::default(),
            )
            .unwrap()
            .unwrap();
        if state
            .previous
            .as_ref()
            .is_some_and(|row| row.identifier == identifiers[1])
            && !state.ready
        {
            completed_prefix = true;
            break;
        }
    }
    assert!(completed_prefix);
    for _ in 0..24 {
        t.retention_step().unwrap();
    }
    assert_eq!(
        count(&first),
        67,
        "completed identifier should already be reclaimed"
    );
    assert_eq!(
        count(&second),
        128,
        "backfill still needs all older versions of its current identifier"
    );
    assert_eq!(ids(&t, entity, None).unwrap_err().code, "INDEX_NOT_READY");
    for _ in 0..32 {
        t.identity_subject_step().unwrap();
    }
    assert!(
        t.retention_status().unwrap()["identity_subjects"]["ready"]
            .as_bool()
            .unwrap()
    );
    for _ in 0..32 {
        t.retention_step().unwrap();
    }
    t.identity_subject_step().unwrap();
    let progress = t.retention_status().unwrap()["identity_subjects"].clone();
    assert!(
        progress["gc_scanned_rows"].as_u64().unwrap() > 4,
        "link GC must batch multiple records"
    );
    for _ in 0..16 {
        t.identity_subject_step().unwrap();
    }
    assert_eq!(
        count(&format!("subject:{entity}:")),
        85,
        "positive and absent predecessors must survive"
    );
    for (knowledge, expected) in [(retained.version, 2), (head.version, 1)] {
        let reverse = ids(&t, entity, Some(knowledge)).unwrap();
        assert_eq!(reverse.identifiers.len(), expected);
        for id in &identifiers {
            let resolved = t
                .identity_resolve(&ResolveRequest {
                    identifier: id.clone(),
                    valid_at: "2026-01-02T00:00:00Z".into(),
                    known_at: Some(knowledge.to_string()),
                })
                .unwrap();
            assert_eq!(
                reverse
                    .identifiers
                    .iter()
                    .find(|item| item.identifier == *id)
                    .map(|item| &item.binding),
                resolved.binding.as_ref()
            );
        }
    }
    let seq = t.engine().seq();
    for _ in 0..4 {
        t.identity_subject_step().unwrap();
    }
    assert_eq!(t.engine().seq(), seq);
}

#[test]
fn binding_batch_capacity_includes_audit_records_and_rejected_batches_are_atomic() {
    use crate::WriteOptions;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store
        .tenant(store.create_tenant("expanded-budget").unwrap().id)
        .unwrap();
    drop(store);
    t.identity_initialize(&actor(), IdentityConfig::default())
        .unwrap();
    let entity = write(
        &t,
        json!({"action":"create_entity","operation_id":"entity","attributes":{}}),
    )
    .entity
    .unwrap()
    .entity_id;
    let request = |prefix: &str, count: usize| -> IdentityWrite {
        serde_json::from_value(json!({"action":"bindings","operation_id":prefix,"changes":(0..count).map(|n| json!({"identifier":{"kind":"id","namespace":"budget","value":format!("{prefix}-{n}")},"expected_revision":"absent","effective":{"mode":"manual","from":"2026-01-01T00:00:00Z"},"change":{"action":"bind","entity_id":entity,"reason":"batch capacity"}})).collect::<Vec<_>>()})).unwrap()
    };
    for (retained, accepted, rejected) in [(false, 199, 200), (true, 110, 111)] {
        if retained {
            t.set_retention(3600).unwrap();
        }
        let options = if retained {
            let context = t.retry_context(&actor().actor).unwrap();
            WriteOptions {
                retry_token: Some(context.retry_token),
                expected_history: Some(context.history),
                ..Default::default()
            }
        } else {
            WriteOptions::default()
        };
        let prefix = if retained { "retained" } else { "unretained" };
        let accepted_receipt = t
            .identity_write_with(&actor(), request(prefix, accepted), &options)
            .unwrap();
        assert_eq!(accepted_receipt.timelines.len(), accepted);
        let before = t.engine().seq();
        let failed = format!("{prefix}-rejected");
        assert_eq!(
            t.identity_write_with(&actor(), request(&failed, rejected), &options)
                .unwrap_err()
                .code,
            "RESOURCE_EXHAUSTED"
        );
        assert_eq!(t.engine().seq(), before);
        assert_eq!(
            t.identity_write_with(&actor(), request(prefix, accepted), &options)
                .unwrap()
                .knowledge
                .version,
            accepted_receipt.knowledge.version
        );
        assert_eq!(t.engine().seq(), before);
    }
    let mut items = Vec::new();
    for (prefix, count, matched) in [
        ("unretained", 199, true),
        ("retained", 110, true),
        ("unretained-rejected", 200, false),
        ("retained-rejected", 111, false),
    ] {
        for n in 0..count {
            items.push((
                ResolveItem {
                    identifier: Identifier {
                        kind: "id".into(),
                        namespace: "budget".into(),
                        value: format!("{prefix}-{n}"),
                    },
                    valid_at: "2026-01-02T00:00:00Z".into(),
                },
                matched,
            ));
        }
    }
    let result = t
        .identity_resolve_many(&ResolveManyRequest {
            known_at: None,
            items: items.iter().map(|(item, _)| item.clone()).collect(),
        })
        .unwrap();
    for (result, (_, expected)) in result.results.iter().zip(items) {
        assert_eq!(
            result.binding.as_ref().map(|b| b.entity_id),
            expected.then_some(entity)
        );
    }
    let mut request: SubjectHistoryRequest =
        serde_json::from_value(json!({"entity_id":entity,"limit":1000})).unwrap();
    let mut count = 0;
    loop {
        let page = t.get_subject_history(&request).unwrap();
        for event in page.events {
            assert!(!event.operation_id.ends_with("rejected"));
            count += 1;
        }
        request.cursor = page.cursor;
        if request.cursor.is_none() {
            break;
        }
    }
    assert_eq!(count, 310);
}
