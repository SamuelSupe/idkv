//! cargo run --release --example identity_subjects -- /var/tmp/idkv-subjects-new
//! Bounded pages, concurrent acknowledged attribute writes, and forward/reverse checks.
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tenant_kv::{identity::*, *};
use uuid::Uuid;

fn actor() -> IdentityContext {
    IdentityContext {
        source: "benchmark".into(),
        actor: "subject-check".into(),
        can_correct: true,
    }
}
fn write(t: &TenantHandle, request: IdentityWrite) -> Result<IdentityReceipt> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match t.identity_write(&actor(), request.clone()) {
            Err(e)
                if matches!(e.code.as_str(), "RESOURCE_EXHAUSTED" | "BUSY")
                    && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10))
            }
            result => return result,
        }
    }
}
fn identifiers(t: &TenantHandle, entity: Uuid, expected: usize) -> Result<(u128, u64)> {
    let mut request: SubjectIdentifiersRequest = serde_json::from_value(
        json!({"entity_id":entity,"valid_at":"2026-01-02T00:00:00Z","limit":100}),
    )?;
    let start = Instant::now();
    let mut seen = BTreeMap::new();
    let mut scanned = 0;
    loop {
        let page = t.get_subject_identifiers(&request)?;
        scanned += page.stats.scanned_entries;
        for item in page.identifiers {
            assert!(seen.insert(item.identifier.value, item.binding).is_none());
        }
        request.cursor = page.cursor;
        if request.cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), expected);
    Ok((start.elapsed().as_micros(), scanned))
}
fn percentiles(mut values: Vec<u128>) -> serde_json::Value {
    values.sort_unstable();
    json!({"samples":values.len(),"p50_us":values[values.len()/2],"p99_us":values[(values.len()*99/100).min(values.len()-1)]})
}
fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .ok_or_else(|| Error::invalid("provide a new benchmark directory"))?;
    if std::path::Path::new(&root).exists() {
        return Err(Error::invalid("benchmark directory must not exist"));
    }
    let store = Store::open(&root, Options::default())?;
    for count in [64usize, 1000] {
        let t = store.tenant(store.create_tenant(&format!("subjects-{count}"))?.id)?;
        t.identity_initialize(&actor(), IdentityConfig::default())?;
        let created = write(
            &t,
            IdentityWrite::CreateEntity {
                operation_id: "entity".into(),
                attributes: json!({"counter":0}),
            },
        )?;
        let entity = created.entity.unwrap().entity_id;
        for first in (0..count).step_by(32) {
            let changes: Vec<_> = (first..(first+32).min(count)).map(|n| json!({"identifier":{"kind":"account","namespace":"bench","value":format!("id-{n}")},"expected_revision":"absent","effective":{"mode":"manual","from":"2026-01-01T00:00:00Z"},"change":{"action":"bind","entity_id":entity,"reason":"fixture"}})).collect();
            write(
                &t,
                serde_json::from_value(
                    json!({"action":"bindings","operation_id":format!("bind-{first}"),"changes":changes}),
                )?,
            )?;
        }
        t.engine().flush()?;
        t.engine().compact(true)?;
        let forward: ResolveManyRequest = serde_json::from_value(
            json!({"items":(0..count).map(|n| json!({"identifier":{"kind":"account","namespace":"bench","value":format!("id-{n}")},"valid_at":"2026-01-02T00:00:00Z"})).collect::<Vec<_>>()}),
        )?;
        assert!(
            t.identity_resolve_many(&forward)?
                .results
                .iter()
                .all(|r| r.binding.as_ref().is_some_and(|b| b.entity_id == entity))
        );
        identifiers(&t, entity, count)?;
        let mut alone = Vec::new();
        let mut scans = 0;
        for _ in 0..20 {
            let (us, n) = identifiers(&t, entity, count)?;
            alone.push(us);
            scans = n;
        }
        let writer = t.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let writer_stop = stop.clone();
        let barrier = Arc::new(Barrier::new(2));
        let writer_barrier = barrier.clone();
        let worker = std::thread::spawn(move || -> Result<Vec<u128>> {
            let mut timings = Vec::new();
            writer_barrier.wait();
            let mut n = 0;
            while n < 100 || !writer_stop.load(Ordering::Acquire) {
                n += 1;
                let revision = writer.identity_entity(entity)?.revision;
                let start = Instant::now();
                write(
                    &writer,
                    IdentityWrite::UpdateEntity {
                        operation_id: format!("update-{n}"),
                        entity_id: entity,
                        expected_revision: revision,
                        attributes: json!({"counter":n}),
                    },
                )?;
                timings.push(start.elapsed().as_micros());
            }
            Ok(timings)
        });
        barrier.wait();
        let mut mixed = Vec::new();
        for _ in 0..20 {
            mixed.push(identifiers(&t, entity, count)?.0);
        }
        stop.store(true, Ordering::Release);
        let writes = worker.join().unwrap()?;
        let acknowledged = writes.len();
        assert_eq!(
            t.identity_entity(entity)?.attributes["counter"],
            acknowledged
        );
        let mut request: SubjectHistoryRequest =
            serde_json::from_value(json!({"entity_id":entity,"limit":100}))?;
        let mut events = BTreeMap::new();
        let start = Instant::now();
        loop {
            let page = t.get_subject_history(&request)?;
            for event in page.events {
                assert!(events.insert(event.event_id, event).is_none());
            }
            request.cursor = page.cursor;
            if request.cursor.is_none() {
                break;
            }
        }
        let history_us = start.elapsed().as_micros();
        assert_eq!(events.len(), count + acknowledged + 1);
        for n in 1..=acknowledged {
            assert_eq!(
                events
                    .values()
                    .filter(|e| e.operation_id == format!("update-{n}"))
                    .count(),
                1
            );
        }
        println!(
            "{}",
            json!({"identifiers":count,"ids_all_pages":percentiles(alone),"ids_with_writer":percentiles(mixed),"writes":percentiles(writes),"scan_entries_all_pages":scans,"history_events":events.len(),"history_all_pages_us":history_us,"confirmed_values_and_events_checked":true})
        );
    }
    Ok(())
}
