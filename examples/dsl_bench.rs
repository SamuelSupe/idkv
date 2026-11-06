//! Compare native, prepared, cache-hit and parse-per-call DSL on the same tenant.
//! Load with read_perf first. dsl_bench ROOT THREADS SECONDS [UPDATE_DOCS_PER_SEC]
use serde_json::json;
use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tenant_kv::{
    dsl::{Output, Prepared, Request},
    *,
};

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let threads: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
    let seconds: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(2);
    let rate: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
    if !(1..=32).contains(&threads) || !(1..=30).contains(&seconds) || rate > 10000 {
        return Err(Error::invalid("benchmark limits"));
    }
    let store = Store::open(
        &args[1],
        Options {
            query_ms: 30_000,
            ..Default::default()
        },
    )?;
    let t = store.tenant(store.tenants()[0].id)?;
    let (stop, writes) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicU64::new(0)),
    );
    let writer = if rate > 0 {
        let stop = stop.clone();
        let writes = writes.clone();
        let tenant = t.clone();
        Some(std::thread::spawn(move || -> Result<()> {
            let begin = Instant::now();
            let mut completed = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let due = (begin.elapsed().as_secs_f64() * rate as f64) as u64;
                if completed + 20 <= due {
                    let operations=(completed..completed+20).map(|n|{let i=n%10000;WriteOp::Put{document:json!({"_key":format!("{i:08}"),"ip":format!("ip-{i}"),"owner":format!("team-{}",i%32),"score":i,"payload":"x".repeat(512)}),if_rev:None}}).collect();
                    tenant.write_batch("assets", operations)?;
                    completed += 20;
                    writes.fetch_add(20, Ordering::Relaxed);
                } else {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            Ok(())
        }))
    } else {
        None
    };
    for kind in ["get", "query", "lookup"] {
        let statement = match kind {
            "get" => "get assets $key",
            "query" => "from assets | where ip == $ip | fields owner",
            _ => "lookup assets using owners with $inputs",
        };
        let params = match kind {
            "get" => json!({"key":"00000042"}),
            "query" => json!({"ip":"ip-42"}),
            _ => {
                json!({"inputs":(0..1000).map(|i|json!({"ip":format!("ip-{i}")})).collect::<Vec<_>>()})
            }
        };
        let request = Arc::new(Request {
            write_options: Default::default(),
            statement: statement.into(),
            params: params.as_object().unwrap().clone(),
            cursor: None,
        });
        let prepared = t.prepare_dsl(statement)?;
        let query: Query = serde_json::from_value(
            json!({"filter":{"field":"/ip","op":"eq","value":"ip-42"},"select":["/owner"]}),
        )?;
        for mode in ["native", "prepared", "cached", "parse_each"] {
            let barrier = Arc::new(Barrier::new(threads + 1));
            let mut workers = Vec::new();
            for _ in 0..threads {
                let t = t.clone();
                let request = request.clone();
                let prepared = prepared.clone();
                let query = query.clone();
                let barrier = barrier.clone();
                workers.push(std::thread::spawn(move || -> Result<Vec<u64>> {
                    let run = || -> Result<Output> {
                        match mode {
                            "native" => Ok(match kind {
                                "get" => Output::Get(t.get("assets", "00000042")?),
                                "query" => Output::Query(t.query("assets", query.clone())?),
                                _ => Output::Lookup(t.lookup_many(
                                    "assets",
                                    "owners",
                                    request.params["inputs"].as_array().unwrap(),
                                )?),
                            }),
                            "prepared" => prepared.execute(&t, &request.params, None),
                            "cached" => t.execute_dsl(&request),
                            _ => Prepared::parse(&request.statement)?.execute(
                                &t,
                                &request.params,
                                None,
                            ),
                        }
                    };
                    for _ in 0..3 {
                        let result = run()?;
                        match &result {
                            Output::Get(Some(v)) => assert_eq!(v["score"], 42),
                            Output::Query(q) => {
                                assert_eq!(q.documents, vec![json!({"owner":"team-10"})])
                            }
                            Output::Lookup(l) => assert_eq!(l.matches.len(), 1000),
                            _ => panic!("unexpected benchmark output"),
                        };
                    }
                    barrier.wait();
                    let begin = Instant::now();
                    let mut samples = Vec::new();
                    while begin.elapsed() < Duration::from_secs(seconds) {
                        let call = Instant::now();
                        std::hint::black_box(run()?);
                        samples.push(call.elapsed().as_nanos() as u64);
                    }
                    Ok(samples)
                }));
            }
            let before = writes.load(Ordering::Relaxed);
            barrier.wait();
            let begin = Instant::now();
            let mut samples = Vec::new();
            for worker in workers {
                samples.extend(worker.join().unwrap()?);
            }
            let elapsed = begin.elapsed().as_secs_f64();
            let updates = writes.load(Ordering::Relaxed) - before;
            samples.sort_unstable();
            let count = samples.len();
            let tenant_state = store.status(t.id())?;
            println!(
                "{}",
                json!({"kind":kind,"mode":mode,"threads":threads,"calls":count,"calls_per_second":count as f64/elapsed,"p50_us":samples[count/2]as f64/1000.0,"p95_us":samples[(count*95/100).min(count-1)]as f64/1000.0,"p99_us":samples[(count*99/100).min(count-1)]as f64/1000.0,"update_docs_per_second":updates as f64/elapsed,"cache":t.dsl_cache_stats(),"engine_memory_bytes":store.memory_bytes(),"tenant_state":tenant_state})
            );
        }
    }
    stop.store(true, Ordering::Relaxed);
    if let Some(writer) = writer {
        writer.join().unwrap()?;
    }
    Ok(())
}
