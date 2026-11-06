//! Bounded multi-tenant load, cold lifecycle, and noisy-neighbor measurements.
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tenant_kv::*;

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let root = args
        .get(1)
        .ok_or_else(|| Error::invalid("soak ROOT [SECONDS] [RETENTION_SECONDS]"))?;
    let seconds: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(60);
    let retention: Option<u64> = args
        .get(3)
        .map(|s| s.parse().map_err(|_| Error::invalid("retention seconds")))
        .transpose()?;
    let options = Options {
        global_memory_bytes: 512 << 20,
        tenant_memory_bytes: 64 << 20,
        cache_bytes: 8 << 20,
        memtable_bytes: 1 << 20,
        target_sst_bytes: 2 << 20,
        level_base_bytes: 8 << 20,
        l0_stop: 24,
        idle_seconds: 1,
        ..Options::default()
    };
    let store = Store::open(root, options.clone())?;
    let mut tenants = Vec::new();
    for n in 0..8 {
        let t = store.tenant(store.create_tenant(&format!("active-{n}"))?.id)?;
        t.create_collection("assets")?;
        for (name, field, unique) in [
            ("ip", "/ip", true),
            ("owner", "/owner", false),
            ("score", "/score", false),
        ] {
            t.create_index(
                "assets",
                IndexSpec {
                    name: name.into(),
                    fields: vec![IndexField {
                        field: field.into(),
                        order: Order::Asc,
                    }],
                    unique,
                    include: vec![],
                },
            )?;
        }
        for batch in 0..10 {
            t.write_batch(
                "assets",
                (0..100)
                    .map(|j| WriteOp::Put {
                        document: document(batch * 100 + j, 0),
                        if_rev: None,
                    })
                    .collect(),
            )?;
        }
        t.engine().flush()?;
        if let Some(seconds) = retention.filter(|s| *s > 0) {
            t.set_retention(seconds)?;
        }
        tenants.push(t);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::new();
    for t in tenants.iter().take(2) {
        let t = t.clone();
        let stop = stop.clone();
        workers.push(std::thread::spawn(move || -> Result<serde_json::Value> {
            let mut updates = 0;
            let mut rejected = 0;
            let mut context = if retention.is_some_and(|s| s > 0) {
                Some(t.retry_context("soak")?)
            } else {
                None
            };
            let mut context_time = Instant::now();
            let mut pending: Option<WriteOptions> = None;
            while !stop.load(Ordering::Acquire) {
                let ops = (0..20)
                    .map(|j| WriteOp::Put {
                        document: document((updates + j) % 1000, updates),
                        if_rev: None,
                    })
                    .collect();
                if pending.is_none()
                    && let Some(seconds) = retention
                {
                    if seconds > 0
                        && context_time.elapsed()
                            >= Duration::from_millis(seconds.saturating_mul(500))
                    {
                        context = Some(t.retry_context("soak")?);
                        context_time = Instant::now();
                    }
                    pending = Some(WriteOptions {
                        expected_history: Some(
                            context
                                .as_ref()
                                .map_or_else(|| t.history(), |c| c.history.clone()),
                        ),
                        operation_id: Some(format!("batch-{updates}")),
                        retry_token: context.as_ref().map(|c| c.retry_token),
                    });
                }
                let result = match &pending {
                    Some(opts) => t.write_batch_with("assets", ops, opts, "soak"),
                    None => t.write_batch("assets", ops),
                };
                match result {
                    Ok(_) => {
                        updates += 20;
                        pending = None;
                    }
                    Err(e) if e.code == "RESOURCE_EXHAUSTED" => {
                        rejected += 1;
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(json!({"kind":"writer","tenant":t.id(),"updates":updates,"backpressure":rejected}))
        }));
    }
    for t in &tenants {
        for _ in 0..2 {
            let t = t.clone();
            let stop = stop.clone();
            workers.push(std::thread::spawn(move||->Result<serde_json::Value>{
                let mut samples=Vec::new();let mut reads=0;let mut rejected=0;
                while !stop.load(Ordering::Acquire) {
                    let start=Instant::now();
                    match t.get("assets",&format!("{:04}",reads%1000)) {
                        Ok(Some(row))=>{check_document(&row,reads%1000); reads+=1;if samples.len()<200_000{samples.push(start.elapsed().as_micros()as u64);}},
                        Err(e)if e.code=="RESOURCE_EXHAUSTED"=>rejected+=1,
                        other=>return Err(Error::corrupt(format!("soak read failed: {other:?}"))),
                    }
                    if reads % 20 == 0 {
                        let keys=(0..20).map(|i|format!("{i:04}")).collect::<Vec<_>>();
                        match t.multi_get("assets",&keys) {
                            Ok(rows)=>{
                                let revision=rows[0].as_ref().unwrap()["_rev"].clone();
                                for (i,row) in rows.iter().enumerate() {
                                    let row=row.as_ref().unwrap();check_document(row,i);
                                    assert_eq!(row["_rev"],revision,"a batch read observed a partial write");
                                }
                            }
                            Err(e) if e.code=="RESOURCE_EXHAUSTED"=>rejected+=1,
                            Err(e)=>return Err(e),
                        }
                        let i=reads%1000;
                        let q=serde_json::from_value(json!({"filter":{"field":"/ip","op":"eq","value":format!("ip-{i}")}}))?;
                        match t.query("assets",q) {
                            Ok(page)=>{assert_eq!(page.documents.len(),1);check_document(&page.documents[0],i);}
                            Err(e) if e.code=="RESOURCE_EXHAUSTED"=>rejected+=1,
                            Err(e)=>return Err(e),
                        }
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                samples.sort_unstable();
                Ok(json!({"kind":"reader","tenant":t.id(),"reads":reads,"backpressure":rejected,
                    "p95_us":samples.get(samples.len()*95/100),"p99_us":samples.get(samples.len()*99/100)}))
            }));
        }
    }
    let peak = Arc::new(AtomicUsize::new(0));
    let mut cold = Vec::new();
    let mut cold_times = Vec::new();
    for n in 0..32 {
        let start = Instant::now();
        let id = store.create_tenant(&format!("cold-{n}"))?.id;
        let t = store.tenant(id)?;
        t.create_collection("assets")?;
        t.put("assets", json!({"_key":"a","n":n}), None)?;
        drop(t);
        cold.push(id);
        cold_times.push(start.elapsed().as_micros() as u64);
    }
    let start = Instant::now();
    let mut next_sample = 0;
    while start.elapsed() < Duration::from_secs(seconds) {
        let used = store.memory_bytes();
        peak.fetch_max(used, Ordering::Relaxed);
        if used > options.global_memory_bytes {
            return Err(Error::corrupt("global budget exceeded"));
        }
        if start.elapsed().as_secs() >= next_sample {
            let states = tenants
                .iter()
                .map(|t| store.status(t.id()))
                .collect::<Result<Vec<_>>>()?;
            println!(
                "{}",
                json!({"kind":"sample","elapsed_s":start.elapsed().as_secs_f64(),"engine_memory_bytes":used,"tenants":states})
            );
            next_sample += 5;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    stop.store(true, Ordering::Release);
    for worker in workers {
        println!("{}", worker.join().unwrap()?);
    }
    if let Some(seconds) = retention {
        std::thread::sleep(Duration::from_secs(seconds + 1));
    }
    for t in &tenants {
        if retention.is_some() {
            println!(
                "{}",
                json!({"kind":"retention","tenant":t.id(),"status":t.retention_status()?})
            );
        }
        t.engine().flush()?;
        t.engine().compact(true)?;
        t.engine().verify()?;
        println!("{}", store.status(t.id())?);
    }
    let active_ids = tenants.iter().map(TenantHandle::id).collect::<Vec<_>>();
    drop(tenants);
    for id in &active_ids {
        let closing = Instant::now();
        loop {
            match store.close_tenant(*id) {
                Ok(()) => break,
                Err(error)
                    if error.code == "BUSY" && closing.elapsed() < Duration::from_secs(5) =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => return Err(error),
            }
        }
        let t = store.tenant(*id)?;
        let keys = (0..1000).map(|i| format!("{i:04}")).collect::<Vec<_>>();
        let rows = t.multi_get("assets", &keys)?;
        for (i, row) in rows.iter().enumerate() {
            check_document(row.as_ref().unwrap(), i);
        }
        let expected = rows
            .iter()
            .flatten()
            .filter(|row| row["owner"] == "team-0")
            .map(|row| json!({"_key":row["_key"]}))
            .collect::<Vec<_>>();
        let page=t.query("assets",serde_json::from_value(json!({"filter":{"field":"/owner","op":"eq","value":"team-0"},"select":["/_key"],"limit":1000}))?)?;
        assert_eq!(
            page.documents, expected,
            "reopened nonunique index disagrees with documents"
        );
    }
    let cold_closed = cold
        .iter()
        .filter(|id| store.status(**id).unwrap()["open"] == false)
        .count();
    assert_eq!(cold_closed, cold.len());
    let mut reopen_times = Vec::new();
    for (n, id) in cold.iter().enumerate() {
        let begin = Instant::now();
        let t = store.tenant(*id)?;
        assert_eq!(t.get("assets", "a")?.unwrap()["n"], n);
        drop(t);
        reopen_times.push(begin.elapsed().as_micros() as u64);
    }
    reopen_times.sort_unstable();
    cold_times.sort_unstable();
    println!(
        "{}",
        json!({"kind":"summary","active_tenants":8,"cold_tenants":32,"duration_s":seconds,"peak_engine_memory":peak.load(Ordering::Relaxed),
        "cold_closed":cold_closed,"cold_reopen_p99_us":reopen_times[reopen_times.len()*99/100],"cold_create_open_p99_us":cold_times[cold_times.len()*99/100],"memory_limit":options.global_memory_bytes,"batch_atomicity_checked":true,"reopened_verified_tenants":active_ids.len(),"reopened_verified_documents":active_ids.len()*1000})
    );
    Ok(())
}
fn check_document(row: &serde_json::Value, i: usize) {
    assert_eq!(row["_key"], format!("{i:04}"));
    assert_eq!(row["ip"], format!("ip-{i}"));
    assert_eq!(row["score"], i);
    assert_eq!(row["payload"], "0123456789abcdef".repeat(64));
}
fn document(i: usize, revision: usize) -> serde_json::Value {
    json!({"_key":format!("{i:04}"),"ip":format!("ip-{i}"),"owner":format!("team-{}",revision%16),"score":i,"payload":"0123456789abcdef".repeat(64)})
}
