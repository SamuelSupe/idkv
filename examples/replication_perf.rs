//! Bounded catch-up benchmark. Use a new ROOT for each run; compare the same
//! workload and settings on an idle Linux VM, not against unrelated QPS figures.
use serde_json::json;
use std::{
    path::PathBuf,
    sync::{
        Arc, Barrier, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tenant_kv::{
    http::{self, AuthConfig, Role as HttpRole, Token},
    replication::{Config, Role, Service},
    *,
};
use uuid::Uuid;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let root = PathBuf::from(
        args.get(1)
            .expect("ROOT [TRANSACTIONS=8192] [READERS=4] [READ_INTERVAL_US=0]"),
    );
    std::fs::create_dir(&root)?;
    let transactions: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8192);
    assert!(transactions >= 1024 && transactions.is_multiple_of(4));
    let readers: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(4);
    assert!((1..=16).contains(&readers));
    let read_interval_us: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
    assert!(read_interval_us <= 1_000_000);
    let options = Options {
        memtable_bytes: 1 << 20,
        cache_bytes: 8 << 20,
        target_sst_bytes: 1 << 20,
        l0_trigger: 8,
        l0_stop: 64,
        ..Options::default()
    };
    let a = Store::open(root.join("a"), options.clone())?;
    let b = Store::open(root.join("b"), options)?;
    let token = format!("benchmark-{}", Uuid::new_v4());
    let token_file = root.join("peer-token");
    std::fs::write(&token_file, &token)?;
    std::fs::set_permissions(
        &token_file,
        std::os::unix::fs::PermissionsExt::from_mode(0o600),
    )?;
    let la = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let lb = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let ua = format!("http://{}", la.local_addr()?);
    let ub = format!("http://{}", lb.local_addr()?);
    let cluster = Uuid::new_v4();
    let config = |role, peer_url| Config {
        cluster,
        role,
        peer_url,
        token_file: token_file.clone(),
        poll_ms: 50,
        log_retention_bytes: 64 << 20,
    };
    let sa = Service::open(a.clone(), config(Role::Primary, ub))?;
    let sb = Service::open(b.clone(), config(Role::Replica, ua))?;
    let auth = || AuthConfig {
        tokens: vec![Token {
            subject: "test-service".into(),
            sha256: http::token_hash(&token),
            tenant: None,
            role: HttpRole::Replication,
        }],
    };
    let status_requests = Arc::new(AtomicU64::new(0));
    let log_requests = Arc::new(AtomicU64::new(0));
    let counted_status = status_requests.clone();
    let counted_log = log_requests.clone();
    let appa = http::app_with_replication(a.clone(), auth(), None, Some(sa.clone()))?.layer(
        axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let counter = match request.uri().path() {
                    "/v1/replication/status" => Some(counted_status.clone()),
                    "/v1/replication/log" => Some(counted_log.clone()),
                    _ => None,
                };
                if let Some(counter) = counter {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                async move { next.run(request).await }
            },
        ),
    );
    let appb = http::app_with_replication(b.clone(), auth(), None, Some(sb.clone()))?;
    let server_a = tokio::spawn(async move { axum::serve(la, appa).await.unwrap() });
    let server_b = tokio::spawn(async move { axum::serve(lb, appb).await.unwrap() });
    let hot = a.create_tenant("hot")?.id;
    let light = a.create_tenant("light")?.id;
    for id in [hot, light] {
        a.tenant(id)?.create_collection("docs")?;
    }
    a.tenant(hot)?.create_index(
        "docs",
        IndexSpec {
            name: "ip".into(),
            fields: vec![IndexField {
                field: "/ip".into(),
                order: Order::Asc,
            }],
            unique: true,
            include: vec!["/value".into()],
        },
    )?;
    a.tenant(hot)?.create_lookup(
        "docs",
        LookupDefinition {
            name: "value".into(),
            index: "ip".into(),
            select: vec!["/value".into()],
            mode: MatchMode::All,
            max_matches: 1,
        },
    )?;
    sb.sync_once().await?;
    let source = a.tenant(hot)?;
    let target = b.tenant(hot)?;
    let before = target.engine().metrics();
    let load_start = Instant::now();
    let writers: Vec<_> = (0..4)
        .map(|worker| {
            let store = a.clone();
            std::thread::spawn(move || -> Result<()> {
                let tenant = store.tenant(hot)?;
                for i in 0..transactions / 4 {
                    let key = (i * 4 + worker) % 1024;
                    tenant.put(
                        "docs",
                        json!({"_key":format!("{key:04}"),"ip":format!("ip-{key}"),
                    "value":i,"payload":"x".repeat(256)}),
                        None,
                    )?;
                }
                Ok(())
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap()?;
    }
    let load_ms = load_start.elapsed().as_secs_f64() * 1000.;
    let light_seq = a
        .tenant(light)?
        .put("docs", json!({"_key":"a","value":1}), None)?
        .rev;
    let goal = source.engine().seq();
    // Readers use independent OS threads: async timer cadence must not alias
    // replication pages, or faster catch-up produce an unusable <100-sample P99.
    target.lookup_many("docs", "value", &[json!({"ip":"ip-0"})])?;
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(readers + 1));
    let read_clock = Arc::new(OnceLock::<Instant>::new());
    let read_ticket = Arc::new(AtomicU64::new(0));
    let reader_threads: Vec<_> = (0..readers)
        .map(|_| {
            let target = target.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            let read_clock = read_clock.clone();
            let read_ticket = read_ticket.clone();
            std::thread::Builder::new()
                .name("replica-read".into())
                .spawn(move || -> Result<Vec<u64>> {
                    let input = [json!({"ip":"ip-0"})];
                    let mut samples = Vec::with_capacity(16_384);
                    barrier.wait();
                    while !stop.load(Ordering::Acquire) && samples.len() < 1_000_000 {
                        if read_interval_us > 0 {
                            // Anchor arrivals to one clock. Sleeping after each call
                            // accumulates VM timer drift and changes the offered load.
                            let ticket = read_ticket.fetch_add(1, Ordering::Relaxed);
                            let due = *read_clock.get().unwrap()
                                + Duration::from_nanos(
                                    ticket * read_interval_us * 1000 / readers as u64,
                                );
                            if let Some(wait) = due.checked_duration_since(Instant::now()) {
                                std::thread::sleep(wait);
                            }
                            if stop.load(Ordering::Acquire) {
                                break;
                            }
                        }
                        let call = Instant::now();
                        target.lookup_many("docs", "value", &input)?;
                        samples.push(call.elapsed().as_nanos() as u64);
                    }
                    Ok(samples)
                })
                .unwrap()
        })
        .collect();
    let begin = Instant::now();
    read_clock.set(begin).unwrap();
    barrier.wait();
    status_requests.store(0, Ordering::Relaxed);
    log_requests.store(0, Ordering::Relaxed);
    let worker = sb.start();
    let mut light_ms = None;
    let mut peak_budget = 0;
    while target.engine().seq() < goal {
        if light_ms.is_none() && b.tenant(light)?.engine().seq() >= light_seq {
            light_ms = Some(begin.elapsed().as_secs_f64() * 1000.);
        }
        peak_budget = peak_budget.max(b.memory_bytes());
        if begin.elapsed() > Duration::from_secs(60) {
            return Err(Error::new(
                "DEADLINE_EXCEEDED",
                format!(
                    "catch-up failed: {}",
                    sb.status()?.last_error.unwrap_or_default()
                ),
            ));
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let catchup_ms = begin.elapsed().as_secs_f64() * 1000.;
    stop.store(true, Ordering::Release);
    let mut lookup_ns = Vec::new();
    let mut reader_samples = Vec::new();
    for reader in reader_threads {
        let samples = reader.join().unwrap()?;
        reader_samples.push(samples.len());
        lookup_ns.extend(samples);
    }
    worker.abort();
    let _ = worker.await;
    for start in [0, 512] {
        let keys: Vec<_> = (start..start + 512).map(|i| format!("{i:04}")).collect();
        assert_eq!(
            source.multi_get("docs", &keys)?,
            target.multi_get("docs", &keys)?
        );
    }
    assert_eq!(
        a.tenant(light)?.get("docs", "a")?,
        b.tenant(light)?.get("docs", "a")?
    );
    lookup_ns.sort_unstable();
    assert!(
        lookup_ns.len() >= 1_000,
        "insufficient tail samples; increase transactions"
    );
    let percentile =
        |p: usize| lookup_ns[(lookup_ns.len() * p / 100).min(lookup_ns.len() - 1)] as f64 / 1000.;
    let after = target.engine().metrics();
    let jobs = |v: &serde_json::Value| v["scheduler"]["completed_jobs"].as_u64().unwrap();
    println!(
        "{}",
        json!({"version":env!("CARGO_PKG_VERSION"),"transactions":transactions,"keys":1024,
        "poll_ms":50,"load_ms":load_ms,"catchup_ms":catchup_ms,"transactions_per_second":transactions as f64/(catchup_ms/1000.),
        "light_tenant_ms":light_ms.unwrap_or(catchup_ms),"replica_io_jobs":jobs(&after)-jobs(&before),
        "peak_replica_budget_bytes":peak_budget,"lookup_samples":lookup_ns.len(),"reader_samples":reader_samples,"readers":readers,"read_interval_us":read_interval_us,"reads_per_second":lookup_ns.len() as f64/(catchup_ms/1000.),"offered_reads_per_second":if read_interval_us == 0 { None } else {Some(readers as f64*1_000_000./read_interval_us as f64)},
        "lookup_p50_us":percentile(50),"lookup_p95_us":percentile(95),"lookup_p99_us":percentile(99),"lookup_max_us":lookup_ns.last().copied().unwrap_or(0) as f64 / 1000.,
        "source_status_requests":status_requests.load(Ordering::Relaxed),"source_log_requests":log_requests.load(Ordering::Relaxed),
        "before":before,"after":after,"note":"Loopback catch-up with concurrent replica lookups, 4 source loading threads; not client write throughput or a hardware HA qualification."})
    );
    server_a.abort();
    server_b.abort();
    std::fs::remove_file(token_file)?;
    Ok(())
}
