//! Same workload for v0.10 and later: ROOT [READS_PER_WORKER=200] [WORKERS=16] [PRIMARY_CONTEXT_DELAY_MS=0].
use serde_json::json;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tenant_kv::{
    client::Client,
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
            .expect("new ROOT [READS] [WORKERS] [CONTEXT_DELAY_MS]"),
    );
    std::fs::create_dir(&root)?;
    let reads: usize = args.get(2).and_then(|n| n.parse().ok()).unwrap_or(200);
    let workers: usize = args.get(3).and_then(|n| n.parse().ok()).unwrap_or(16);
    let delay: u64 = args.get(4).and_then(|n| n.parse().ok()).unwrap_or(0);
    assert!((1..=64).contains(&workers) && (10..=100000).contains(&reads) && delay <= 1000);
    let a = Store::open(root.join("a"), Options::default())?;
    let b = Store::open(root.join("b"), Options::default())?;
    let la = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let lb = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let ua = format!("http://{}", la.local_addr()?);
    let ub = format!("http://{}", lb.local_addr()?);
    let token = format!("benchmark-{}", Uuid::new_v4());
    let token_file = root.join("token");
    std::fs::write(&token_file, &token)?;
    let cluster = Uuid::new_v4();
    let config = |role, peer_url| Config {
        role,
        peer_url,
        cluster,
        token_file: token_file.clone(),
        poll_ms: 10,
        log_retention_bytes: 16 << 20,
    };
    let sa = Service::open(a.clone(), config(Role::Primary, ub.clone()))?;
    let sb = Service::open(b.clone(), config(Role::Replica, ua.clone()))?;
    let auth = || AuthConfig {
        tokens: vec![Token {
            subject: "benchmark".into(),
            sha256: http::token_hash(&token),
            tenant: None,
            role: HttpRole::Admin,
        }],
    };
    let contexts = Arc::new(AtomicU64::new(0));
    let requests = Arc::new(AtomicU64::new(0));
    let mut servers = Vec::new();
    for (store, service, listener, primary) in [
        (a.clone(), sa, la, true),
        (b.clone(), sb.clone(), lb, false),
    ] {
        let contexts = contexts.clone();
        let requests = requests.clone();
        let app = http::app_with_replication(store, auth(), None, Some(service))?.layer(
            axum::middleware::from_fn(
                move |request: axum::extract::Request, next: axum::middleware::Next| {
                    let context = request.uri().path().ends_with("/context");
                    if context {
                        contexts.fetch_add(1, Ordering::Relaxed);
                    }
                    if request.uri().path().ends_with("/documents/k") {
                        requests.fetch_add(1, Ordering::Relaxed);
                    }
                    async move {
                        if primary && context && delay > 0 {
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                        }
                        next.run(request).await
                    }
                },
            ),
        );
        servers.push(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
    }
    let id = a.create_tenant("client-bench")?.id;
    let tenant = a.tenant(id)?;
    tenant.create_collection("docs")?;
    tenant.put("docs", json!({"_key":"k","value":42}), None)?;
    for _ in 0..100 {
        sb.sync_once().await?;
        if let Ok(replica) = b.tenant(id)
            && replica.get("docs", "k")?.is_some()
        {
            break;
        }
    }
    assert_eq!(b.tenant(id)?.get("docs", "k")?.unwrap()["value"], 42);
    let client = Client::new(&[ua, ub], token, id)?;
    let cold = Instant::now();
    assert_eq!(client.get("docs", "k", None).await?.unwrap()["value"], 42);
    let cold_ms = cold.elapsed().as_secs_f64() * 1000.;
    contexts.store(0, Ordering::Relaxed);
    requests.store(0, Ordering::Relaxed);
    let barrier = Arc::new(tokio::sync::Barrier::new(workers + 1));
    let mut tasks = Vec::new();
    for _ in 0..workers {
        let client = client.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut samples = Vec::with_capacity(reads);
            for _ in 0..reads {
                let start = Instant::now();
                assert_eq!(client.get("docs", "k", None).await?.unwrap()["value"], 42);
                samples.push(start.elapsed().as_nanos() as u64);
            }
            Ok::<_, Error>(samples)
        }));
    }
    let start = Instant::now();
    barrier.wait().await;
    let mut samples = Vec::new();
    for task in tasks {
        samples.extend(task.await.unwrap()?);
    }
    let elapsed = start.elapsed().as_secs_f64();
    samples.sort_unstable();
    let pct = |p: usize| samples[(samples.len() * p / 100).min(samples.len() - 1)] as f64 / 1e6;
    println!(
        "{}",
        json!({"version":env!("CARGO_PKG_VERSION"),"workers":workers,"reads":samples.len(),"primary_context_delay_ms":delay,"cold_read_ms":cold_ms,"elapsed_s":elapsed,"reads_per_second":samples.len() as f64/elapsed,"p50_ms":pct(50),"p95_ms":pct(95),"p99_ms":pct(99),"context_requests":contexts.load(Ordering::Relaxed),"data_requests":requests.load(Ordering::Relaxed),"note":"Loopback actual primary/replica pair, closed-loop reads after one warm-up; injected delay only on primary context endpoint, not disk I/O."})
    );
    for server in servers {
        server.abort();
    }
    Ok(())
}
