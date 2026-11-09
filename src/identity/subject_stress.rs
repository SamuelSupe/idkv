//! Opt-in Linux qualification with real SSTs, WAL commits and restartable phases.
//! See docs/subject-maintenance-scale-validation-20261109.md for the commands.
use super::{storage::*, subjects::*, *};
use crate::{Options, Store, tenant::catalog::document_key};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Serialize, Deserialize)]
struct Fixture {
    tenant: Uuid,
    entities: [Uuid; 2],
    base: u64,
    rows: usize,
    head: Knowledge,
    floor: Knowledge,
}
fn actor() -> IdentityContext {
    IdentityContext {
        source: "qualification".into(),
        actor: "scale".into(),
        can_correct: true,
    }
}
fn identifier(n: usize) -> Identifier {
    Identifier {
        kind: "account".into(),
        namespace: "scale".into(),
        value: format!("id-{n:04}"),
    }
}
fn owner(n: usize) -> Option<usize> {
    let version = n / 32;
    (!version.is_multiple_of(11)).then_some(version / 8 % 2)
}
fn expected(f: &Fixture, id: usize, ordinal: u64) -> Option<Uuid> {
    let last = (ordinal - f.base) as usize;
    let n = last - (last % 32 + 32 - id) % 32;
    owner(n).map(|index| f.entities[index])
}
fn options() -> Options {
    Options {
        idle_seconds: 3600,
        ..Default::default()
    }
}
fn progress(t: &TenantHandle) -> SubjectState {
    let c = t
        .collection_id(&t.engine().snapshot().unwrap(), COLLECTION)
        .unwrap();
    Scratch::maintenance(t)
        .get(
            &t.engine().snapshot().unwrap(),
            c,
            STATE,
            &mut Default::default(),
        )
        .unwrap()
        .unwrap()
}
fn seed(root: &Path, rows: usize) {
    assert!(!root.exists(), "qualification needs a new directory");
    let store = Store::open(root, options()).unwrap();
    let tenant_count = std::env::var("TKV_SUBJECT_STRESS_TENANTS")
        .ok()
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(8);
    assert!((1..=8).contains(&tenant_count));
    let tenants: Vec<_> = (0..tenant_count)
        .map(|n| {
            store
                .tenant(store.create_tenant(&format!("scale-{n}")).unwrap().id)
                .unwrap()
        })
        .collect();
    drop(store); // Seed the legacy format before starting normal maintenance.
    let start = Instant::now();
    let mut fixtures = Vec::new();
    for (tenant_no, t) in tenants.iter().enumerate() {
        t.identity_initialize(&actor(), IdentityConfig::default())
            .unwrap();
        let mut entities = Vec::new();
        let mut receipt = None;
        for n in 0..2 {
            let r = t
                .identity_write(
                    &actor(),
                    IdentityWrite::CreateEntity {
                        operation_id: format!("entity-{n}"),
                        attributes: json!({"seed":n}),
                    },
                )
                .unwrap();
            entities.push(r.entity.as_ref().unwrap().entity_id);
            receipt = Some(r);
        }
        let mut head = receipt.unwrap().knowledge;
        let base = head.ordinal + 1;
        let count = rows / tenant_count + usize::from(tenant_no < rows % tenant_count);
        assert!(count > 512);
        head.ordinal = base + count as u64 - 1;
        head.version = Uuid::new_v4();
        let mut floor = head.clone();
        floor.ordinal -= 256;
        floor.version = Uuid::new_v4();
        let c = t
            .collection_id(&t.engine().snapshot().unwrap(), COLLECTION)
            .unwrap();
        let ids: Vec<_> = (0..32).map(identifier).collect();
        let digests: Vec<_> = ids.iter().map(|id| digest(id).unwrap()).collect();
        for first in (0..count).step_by(400) {
            let result = t.engine().transact_maintenance(|tx| {
                for n in first..(first + 400).min(count) {
                    let id = n % 32;
                    let timeline = Timeline {
                        identifier: ids[id].clone(),
                        revision: Uuid::new_v4(),
                        segments: owner(n)
                            .map(|owner| Binding {
                                valid_from_us: parse_time("2026-01-01T00:00:00Z").unwrap(),
                                valid_to_us: None,
                                entity_id: entities[owner],
                                binding_id: Uuid::new_v4(),
                                version_id: Uuid::new_v4(),
                                source: "legacy".into(),
                                evidence_id: None,
                                assurance: "asserted".into(),
                            })
                            .into_iter()
                            .collect(),
                    };
                    let key = format!("history:{}:{:016x}", digests[id], !(base + n as u64));
                    let bytes = crate::codec::encode(&json!({"_key":key,"data":timeline}))?;
                    tx.put(document_key(c, &key), bytes)?;
                    if n + 32 >= count {
                        let key = timeline_key(&timeline.identifier)?;
                        tx.put(
                            document_key(c, &key),
                            crate::codec::encode(&json!({"_key":key,"data":timeline}))?,
                        )?;
                    }
                }
                Ok(())
            });
            result.unwrap();
            t.engine().maintain().unwrap();
        }
        t.engine()
            .transact_maintenance(|tx| {
                let p = super::subject_maintenance::prefix(c, "knowledge:");
                for (key, _) in tx.prefix_first(&p, 100)? {
                    tx.delete(key)?;
                }
                for knowledge in [&head, &floor] {
                    let key = format!("knowledge:{}", knowledge.version);
                    tx.put(
                        document_key(c, &key),
                        crate::codec::encode(&json!({"_key":key,"data":knowledge}))?,
                    )?;
                }
                tx.put(
                    document_key(c, "head:knowledge"),
                    crate::codec::encode(&json!({"_key":"head:knowledge","data":head}))?,
                )?;
                tx.delete(document_key(c, STATE))
            })
            .unwrap();
        t.engine().flush().unwrap();
        t.engine().compact(true).unwrap();
        fixtures.push(Fixture {
            tenant: t.id(),
            entities: [entities[0], entities[1]],
            base,
            rows: count,
            head,
            floor,
        });
        println!(
            "{}",
            json!({"stage":"seed","tenants":tenant_no+1,"rows":fixtures.iter().map(|f|f.rows).sum::<usize>(),"elapsed_ms":start.elapsed().as_millis()})
        );
    }
    crate::storage::atomic_json(&root.join("scale.json"), &fixtures).unwrap();
}
fn verify_acks(root: &Path, store: &Store) -> usize {
    let path = root.join("acks.jsonl");
    let mut count = 0;
    if path.exists() {
        let mut bytes = fs::read(&path).unwrap();
        if !bytes.ends_with(b"\n") {
            let end = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |n| n + 1);
            eprintln!(
                "discarding {} bytes of an incomplete client ledger record",
                bytes.len() - end
            );
            let file = OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(end as u64).unwrap();
            file.sync_all().unwrap();
            bytes.truncate(end);
        }
        for line in std::str::from_utf8(&bytes).unwrap().lines() {
            let ack: serde_json::Value = serde_json::from_str(line).unwrap();
            let id: Uuid = serde_json::from_value(ack["tenant"].clone()).unwrap();
            let t = store.tenant(id).unwrap();
            let key = ack["key"].as_str().unwrap();
            assert_eq!(
                t.get("confirmed", key).unwrap().unwrap()["value"],
                ack["value"]
            );
            count += 1;
        }
    }
    count
}
fn verify_bindings(t: &TenantHandle, fixture: &Fixture) {
    for knowledge in [&fixture.floor, &fixture.head] {
        let mut forward = std::collections::BTreeMap::new();
        for n in 0..32 {
            let result = t
                .identity_resolve(&ResolveRequest {
                    identifier: identifier(n),
                    valid_at: "2026-01-02T00:00:00Z".into(),
                    known_at: Some(knowledge.version.to_string()),
                })
                .unwrap();
            assert_eq!(
                result.binding.as_ref().map(|b| b.entity_id),
                expected(fixture, n, knowledge.ordinal)
            );
            forward.insert(identifier(n).value, result.binding.map(|b| b.entity_id));
        }
        for entity in fixture.entities {
            let mut request: SubjectIdentifiersRequest = serde_json::from_value(json!({
                "entity_id":entity,"known_at":knowledge.version,"valid_at":"2026-01-02T00:00:00Z","limit":8
            })).unwrap();
            let mut reverse = std::collections::BTreeSet::new();
            loop {
                let result = t.get_subject_identifiers(&request).unwrap();
                for item in result.identifiers {
                    assert_eq!(forward[&item.identifier.value], Some(entity));
                    assert!(reverse.insert(item.identifier.value));
                }
                request.cursor = result.cursor;
                if request.cursor.is_none() {
                    break;
                }
            }
            assert_eq!(
                reverse.len(),
                forward.values().filter(|id| **id == Some(entity)).count()
            );
        }
    }
}
fn rss_kib() -> u64 {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|line| line.starts_with("VmRSS:"))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|n| n.parse().ok())
        })
        .unwrap_or(0)
}
fn run(root: &Path, stage: &str) {
    let fixtures: Vec<Fixture> = crate::storage::read_json(&root.join("scale.json")).unwrap();
    let store = Store::open(root, options()).unwrap();
    let acknowledged = verify_acks(root, &store);
    let tenants: Vec<_> = fixtures
        .iter()
        .map(|f| store.tenant(f.tenant).unwrap())
        .collect();
    let previous_path = root.join("progress.json");
    if previous_path.exists() {
        let previous: serde_json::Value = crate::storage::read_json(&previous_path).unwrap();
        for t in &tenants {
            let id = t.id().to_string();
            let old = previous["tenants"]
                .as_array()
                .unwrap()
                .iter()
                .find(|old| old["tenant"] == id)
                .unwrap();
            let state = progress(t);
            assert!(
                state.processed_history_rows
                    >= old["progress"]["processed_history_rows"].as_u64().unwrap()
            );
            assert!(state.gc_removed_rows >= old["progress"]["gc_removed_rows"].as_u64().unwrap());
            assert!(old["progress"]["ready"] != true || state.ready);
        }
    }
    if stage != "verify" {
        for t in &tenants {
            t.identity_subject_step().unwrap();
        }
    }
    let old: Vec<_> = tenants
        .iter()
        .map(|t| t.retention_status().unwrap()["identity_subjects"].clone())
        .collect();
    if stage == "gc" {
        for t in &tenants {
            t.set_retention(3600).unwrap();
        }
    }
    let stop = Arc::new(AtomicBool::new(false));
    let rejected = Arc::new(AtomicUsize::new(0));
    let ledger = Arc::new(parking_lot::Mutex::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join("acks.jsonl"))
            .unwrap(),
    ));
    let session = Uuid::new_v4();
    let mut writers = Vec::new();
    let burst = std::env::var("TKV_SUBJECT_STRESS_BURST").as_deref() == Ok("1");
    let mut drivers = Vec::new();
    if burst && matches!(stage, "run" | "gc") {
        for t in &tenants {
            let t = t.clone();
            let stop = stop.clone();
            drivers.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    let result = t
                        .engine()
                        .maintain()
                        .and_then(|_| t.retention_step())
                        .and_then(|_| t.gc_step())
                        .and_then(|_| t.identity_subject_step());
                    if let Err(e) = result {
                        assert!(
                            matches!(e.code.as_str(), "RESOURCE_EXHAUSTED" | "BUSY"),
                            "{e}"
                        );
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }));
        }
    }
    if stage != "verify" {
        let light = store
            .tenants()
            .into_iter()
            .find(|t| t.name == "scale-light")
            .unwrap_or_else(|| store.create_tenant("scale-light").unwrap());
        let mut writing: Vec<_> = tenants.iter().take(4).cloned().collect();
        writing.push(store.tenant(light.id).unwrap());
        for t in writing {
            if t.collections()
                .unwrap()
                .iter()
                .all(|c| c.name != "confirmed")
            {
                t.create_collection("confirmed").unwrap();
            }
            let stop = stop.clone();
            let ledger = ledger.clone();
            let rejected = rejected.clone();
            let phase = stage.to_string();
            writers.push(std::thread::spawn(move || {
                let mut timings = Vec::new();
                let mut n = 0;
                while !stop.load(Ordering::Acquire) {
                    let key = format!("{session}-{n}");
                    let started = Instant::now();
                    match t.put("confirmed", json!({"_key":key,"value":n}), None) {
                        Ok(receipt) => {
                            let us = started.elapsed().as_micros();
                            timings.push(us);
                            let read = Instant::now();
                            assert_eq!(t.get("confirmed", &key).unwrap().unwrap()["value"], n);
                            let read_us = read.elapsed().as_micros();
                            let mut ledger = ledger.lock();
                            let line = format!("{}\n",json!({"tenant":t.id(),"key":key,"value":n,"seq":receipt.rev,"latency_us":us,"read_us":read_us,"light":t.info().name=="scale-light","stage":phase}));
                            ledger.write_all(line.as_bytes()).unwrap();
                            ledger.sync_data().unwrap();
                        }
                        Err(e) if e.code=="RESOURCE_EXHAUSTED" || e.code=="BUSY" => {
                            rejected.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => panic!("foreground write: {e}"),
                    }
                    n += 1;
                    std::thread::sleep(Duration::from_millis(20));
                }
                timings
            }));
        }
    }
    let start = Instant::now();
    let kill_after: u64 = std::env::var("TKV_SUBJECT_KILL_AFTER")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(u64::MAX);
    let mut last = Instant::now() - Duration::from_secs(2);
    loop {
        let states: Vec<_> = tenants.iter().map(progress).collect();
        let rows: u64 = states.iter().map(|s| s.processed_history_rows).sum();
        let ready = states.iter().filter(|s| s.ready).count();
        let gc_done = stage != "gc"
            || states
                .iter()
                .zip(&fixtures)
                .zip(&tenants)
                .all(|((s, f), t)| {
                    let gc = t.retention_status().unwrap()["gc"].clone();
                    s.swept_floor >= f.floor.ordinal
                        && gc["knowledge_floor"].as_u64().unwrap_or(0) >= f.floor.ordinal
                        && gc["history_sweep_active"] == false
                });
        if last.elapsed() >= Duration::from_secs(1) {
            let report = json!({"stage":stage,"elapsed_ms":start.elapsed().as_millis(),
                "rows":rows,"ready":ready,"rss_kib":rss_kib(),"previous_acks":acknowledged,"burst":burst,
                "foreground_rejections":rejected.load(Ordering::Relaxed),
                "tenants":tenants.iter().zip(&states).map(|(t,s)|json!({
                    "tenant":t.id(),"progress":s.progress(),"swept_floor":s.swept_floor,
                    "retention":t.retention_status().unwrap(),
                    "maintenance_error":t.engine().maintenance_error.lock().clone(),
                    "io":t.engine().metrics()
                })).collect::<Vec<_>>(),"restart_progress":old});
            crate::storage::atomic_json(&root.join("progress.json"), &report).unwrap();
            println!("{report}");
            std::io::stdout().flush().unwrap();
            last = Instant::now();
            if rows >= kill_after {
                unsafe {
                    libc::kill(libc::getpid(), libc::SIGKILL);
                }
            }
        }
        if ready == tenants.len()
            && gc_done
            && (stage != "idle" || start.elapsed() >= Duration::from_secs(30))
        {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(1200),
            "maintenance made insufficient progress"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    stop.store(true, Ordering::Release);
    for driver in drivers {
        driver.join().unwrap();
    }
    let mut timings: Vec<_> = writers
        .into_iter()
        .flat_map(|w| w.join().unwrap())
        .collect();
    timings.sort_unstable();
    for (t, f) in tenants.iter().zip(&fixtures) {
        verify_bindings(t, f);
        assert!(progress(t).processed_history_rows >= f.rows as u64);
        if stage == "gc" {
            t.engine().flush().unwrap();
            t.engine().compact(true).unwrap();
            let snapshot = t.engine().snapshot().unwrap();
            let c = t.collection_id(&snapshot, COLLECTION).unwrap();
            let mut counts = Vec::new();
            for text in ["history:", "subject:"] {
                let prefix = super::subject_maintenance::prefix(c, text);
                let mut stats = Default::default();
                let mut scan = snapshot
                    .scan(
                        prefix.clone(),
                        crate::codec::prefix_end(&prefix),
                        None,
                        &mut stats,
                    )
                    .unwrap();
                let mut count = 0;
                while scan.next(&mut stats).unwrap().is_some() {
                    count += 1;
                }
                counts.push(count);
            }
            assert_eq!(
                counts[0], 289,
                "retained history plus one predecessor per identifier"
            );
            assert!(counts[1] <= 578, "old reverse versions must be reclaimed");
            println!(
                "{}",
                json!({"tenant":t.id(),"post_gc_history":counts[0],"post_gc_links":counts[1],"disk_bytes":t.engine().disk_bytes()})
            );
        }
    }
    let confirmed = verify_acks(root, &store);
    println!(
        "{}",
        json!({"stage":stage,"complete":true,"elapsed_ms":start.elapsed().as_millis(),
        "confirmed":confirmed,"write_samples":timings.len(),
        "foreground_rejections":rejected.load(Ordering::Relaxed),
        "write_p50_us":timings.get(timings.len()/2),"write_p99_us":timings.get(timings.len()*99/100)})
    );
}

#[test]
#[ignore = "million-row Linux qualification; explicit persistent root and phases"]
fn million_history() {
    let root =
        std::env::var("TKV_SUBJECT_STRESS_ROOT").expect("set a new persistent qualification root");
    let stage = std::env::var("TKV_SUBJECT_STRESS_STAGE").unwrap_or_else(|_| "run".into());
    if stage == "seed" {
        let rows = std::env::var("TKV_SUBJECT_STRESS_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1_000_000);
        seed(Path::new(&root), rows);
    } else {
        assert!(matches!(stage.as_str(), "run" | "gc" | "verify" | "idle"));
        run(Path::new(&root), &stage);
    }
}
