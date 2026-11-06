//! Reproducible load/mixed-read harness. Data is deterministic; all reported
//! latency includes the embedded document API, and no HTTP round trip.
#[path = "bench/probes.rs"]
mod probes;
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tenant_kv::*;

fn config() -> Options {
    let memory = std::env::var("TKV_BENCH_MEMORY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4usize << 30);
    Options {
        global_memory_bytes: memory,
        tenant_memory_bytes: memory,
        cache_bytes: memory / 2,
        memtable_bytes: 32 << 20,
        tenant_disk_bytes: 300 << 30,
        target_sst_bytes: 32 << 20,
        level_base_bytes: 128 << 20,
        l0_stop: 24,
        ..Options::default()
    }
}
fn payload(mut seed: u64, len: usize) -> String {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-";
    let mut bytes = Vec::with_capacity(len);
    for _ in 0..len {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        bytes.push(ALPHABET[(seed as usize) & 63]);
    }
    String::from_utf8(bytes).unwrap()
}
fn document(i: usize, bytes: usize) -> Value {
    json!({"_key":format!("{i:012}"),"ip":format!("ip-{i}"),"env":if i.is_multiple_of(2){"prod"}else{"dev"},"score":i,"owner":format!("team-{}",i%32),"payload":payload(i as u64+1234567,bytes)})
}
fn spec(name: &str, fields: &[(&str, Order)], unique: bool) -> IndexSpec {
    IndexSpec {
        name: name.into(),
        fields: fields
            .iter()
            .map(|(f, o)| IndexField {
                field: (*f).into(),
                order: o.clone(),
            })
            .collect(),
        unique,
        include: vec!["/owner".into()],
    }
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 3 {
        return Err(Error::invalid(
            "bench load ROOT ROWS PAYLOAD_BYTES | measure ROOT [SECONDS]",
        ));
    }
    let root = Path::new(&args[2]);
    let mut options = config();
    if args[1] == "verify" {
        options.snapshot_seconds = 600;
    }
    let store = Store::open(root, options)?;
    let id = if let Some(info) = store.tenants().into_iter().find(|t| t.name == "benchmark") {
        info.id
    } else {
        store.create_tenant("benchmark")?.id
    };
    let t = store.tenant(id)?;
    if t.collections()?.is_empty() {
        t.create_collection("assets")?;
        t.create_index("assets", spec("ip", &[("/ip", Order::Asc)], true))?;
        t.create_index(
            "assets",
            spec(
                "env-score",
                &[("/env", Order::Asc), ("/score", Order::Desc)],
                false,
            ),
        )?;
        t.create_index("assets", spec("owner", &[("/owner", Order::Asc)], false))?;
        t.create_lookup(
            "assets",
            LookupDefinition {
                name: "owners".into(),
                index: "ip".into(),
                select: vec!["/_key".into(), "/owner".into()],
                mode: MatchMode::All,
                max_matches: 1,
            },
        )?;
    }
    let progress = root.join("benchmark.json");
    if args[1] == "load" {
        let rows: usize = args
            .get(3)
            .ok_or_else(|| Error::invalid("rows"))?
            .parse()
            .map_err(|_| Error::invalid("rows"))?;
        let bytes: usize = args
            .get(4)
            .ok_or_else(|| Error::invalid("payload bytes"))?
            .parse()
            .map_err(|_| Error::invalid("payload bytes"))?;
        let prior: Value = if progress.exists() {
            serde_json::from_slice(&std::fs::read(&progress)?)?
        } else {
            json!({"rows":0})
        };
        let begin = prior["rows"].as_u64().unwrap() as usize;
        let started = Instant::now();
        for start in (begin..rows).step_by(500) {
            let end = (start + 500).min(rows);
            let ops: Vec<_> = (start..end)
                .map(|i| WriteOp::Put {
                    document: document(i, bytes),
                    if_rev: None,
                })
                .collect();
            loop {
                match t.write_batch("assets", ops.clone()) {
                    Ok(_) => break,
                    Err(e) if e.code == "RESOURCE_EXHAUSTED" => {
                        t.engine().maintain()?;
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => return Err(e),
                }
            }
            std::fs::write(
                &progress,
                serde_json::to_vec(&json!({"rows":end,"payload_bytes":bytes}))?,
            )?;
            if end % 100_000 == 0 || end == rows {
                println!(
                    "{}",
                    json!({"phase":"load","rows":end,"elapsed_s":started.elapsed().as_secs_f64(),"memory_bytes":store.memory_bytes(),"disk_bytes":t.engine().disk_bytes(),"levels":t.engine().table_counts()})
                );
            }
        }
        t.engine().flush()?;
        for _ in 0..32 {
            t.engine().compact(false)?;
        }
        println!(
            "{}",
            json!({"phase":"loaded","rows":rows,"elapsed_s":started.elapsed().as_secs_f64(),"disk_bytes":t.engine().disk_bytes(),"memory_bytes":store.memory_bytes()})
        );
    } else if args[1] == "verify" {
        let info: Value = serde_json::from_slice(&std::fs::read(progress)?)?;
        let started = Instant::now();
        t.engine().flush()?;
        t.engine().verify()?;
        let collection = t
            .collections()?
            .into_iter()
            .find(|c| c.name == "assets")
            .unwrap();
        let mut prefix = vec![b'D'];
        prefix.extend(collection.id.as_bytes());
        let snapshot = t.engine().snapshot()?;
        let mut stats = ReadStats::default();
        let mut scan =
            snapshot.scan(prefix.clone(), codec::prefix_end(&prefix), None, &mut stats)?;
        let mut count = 0;
        while scan.next(&mut stats)?.is_some() {
            count += 1;
        }
        if Some(count) != info["rows"].as_u64() {
            return Err(Error::corrupt("full scan row count mismatch"));
        }
        println!(
            "{}",
            json!({"phase":"verified","rows":count,"seconds":started.elapsed().as_secs_f64(),"stats":stats,"engine_memory_bytes":store.memory_bytes(),"rss":rss(),"disk_bytes":t.engine().disk_bytes()})
        );
    } else if args[1] == "probe" {
        let info: Value = serde_json::from_slice(&std::fs::read(progress)?)?;
        probes::run(&t, info["rows"].as_u64().unwrap() as usize)?;
    } else if args[1] == "measure" {
        let info: Value = serde_json::from_slice(&std::fs::read(progress)?)?;
        let rows = info["rows"].as_u64().unwrap() as usize;
        let bytes = info["payload_bytes"].as_u64().unwrap() as usize;
        let seconds: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10);
        let ids: Vec<_> = (0..1000).map(|i| i * rows / 1000).collect();
        let inputs: Vec<_> = ids
            .iter()
            .map(|i| json!({"ip":format!("ip-{i}")}))
            .collect();
        let q: Query = serde_json::from_value(
            json!({"filter":{"and":[{"field":"/env","op":"eq","value":"prod"},{"field":"/score","op":"gte","value":rows.saturating_sub(1000)}]},"sort":[{"field":"/score","order":"desc"}],"select":["/_key","/owner"],"limit":100}),
        )?;
        for i in &ids {
            t.get("assets", &format!("{i:012}"))?;
        }
        for _ in 0..4 {
            t.lookup_many("assets", "owners", &inputs)?;
            let page = t.query("assets", q.clone())?;
            if let Some(token) = page.cursor {
                t.release_cursor(&token)?;
            }
        }
        for mode in ["get", "indexed_query", "lookup_1000"] {
            let stop = Arc::new(AtomicBool::new(false));
            let completed = Arc::new(AtomicUsize::new(0));
            let writer_completed = completed.clone();
            let writer_t = t.clone();
            let writer_stop = stop.clone();
            let writer = std::thread::spawn(move || -> Result<usize> {
                let mut written = 0;
                let started = Instant::now();
                while !writer_stop.load(Ordering::Acquire) {
                    let ops = (0..20)
                        .map(|j| WriteOp::Put {
                            document: document((written + j) % rows, bytes),
                            if_rev: None,
                        })
                        .collect();
                    match writer_t.write_batch("assets", ops) {
                        Ok(_) => {
                            written += 20;
                            writer_completed.store(written, Ordering::Release);
                        }
                        Err(e) if e.code == "RESOURCE_EXHAUSTED" => {
                            writer_t.engine().maintain()?;
                        }
                        Err(e) => return Err(e),
                    }
                    let target = Duration::from_millis(written as u64);
                    if target > started.elapsed() {
                        std::thread::sleep(
                            (target - started.elapsed()).min(Duration::from_millis(20)),
                        );
                    }
                }
                Ok(written)
            });
            let barrier = Arc::new(std::sync::Barrier::new(17));
            let mut readers = Vec::new();
            for n in 0..16 {
                let t = t.clone();
                let inputs = inputs.clone();
                let ids = ids.clone();
                let q = q.clone();
                let barrier = barrier.clone();
                readers.push(std::thread::spawn(move || -> Result<Vec<u64>> {
                    barrier.wait();
                    let start = Instant::now();
                    let mut times = Vec::new();
                    let mut i = n;
                    while start.elapsed() < Duration::from_secs(seconds) {
                        let one = Instant::now();
                        match mode {
                            "get" => {
                                if t.get("assets", &format!("{:012}", ids[i % ids.len()]))?
                                    .is_none()
                                {
                                    return Err(Error::corrupt("benchmark missing key"));
                                }
                            }
                            "indexed_query" => {
                                let page = t.query("assets", q.clone())?;
                                if page.documents.len() != 100 {
                                    return Err(Error::corrupt("benchmark query cardinality"));
                                }
                                if let Some(token) = page.cursor {
                                    t.release_cursor(&token)?;
                                }
                            }
                            _ => {
                                let result = t.lookup_many("assets", "owners", &inputs)?;
                                if result.matches.iter().any(|r| r.len() != 1) {
                                    return Err(Error::corrupt("benchmark lookup cardinality"));
                                }
                            }
                        }
                        times.push(one.elapsed().as_micros() as u64);
                        i += 1;
                    }
                    Ok(times)
                }));
            }
            let started = Instant::now();
            barrier.wait();
            let mut latencies = Vec::new();
            for reader in readers {
                latencies.extend(reader.join().unwrap()?);
            }
            let read_elapsed = started.elapsed().as_secs_f64();
            let updates = completed.load(Ordering::Acquire);
            stop.store(true, Ordering::Release);
            let total_updates = writer.join().unwrap()?;
            let writer_drain = started.elapsed().as_secs_f64() - read_elapsed;
            latencies.sort_unstable();
            println!(
                "{}",
                json!({"phase":"measure","mode":mode,"rows":rows,"payload_bytes":bytes,"readers":16,"seconds":read_elapsed,"writer_drain_s":writer_drain,"write_rate":updates as f64/read_elapsed,"writes_total":total_updates,"operations":latencies.len(),"qps":latencies.len() as f64/read_elapsed,"p50_us":latencies[latencies.len()/2],"p95_us":latencies[latencies.len()*95/100],"p99_us":latencies[latencies.len()*99/100],"writes":updates,"engine_memory_bytes":store.memory_bytes(),"disk_bytes":t.engine().disk_bytes(),"rss":rss(),"io":t.engine().metrics()})
            );
        }
        let sample = t.lookup_many("assets", "owners", &inputs)?;
        println!("{}", json!({"phase":"lookup_stats","stats":sample.stats}));
    } else {
        return Err(Error::invalid("unknown benchmark command"));
    }
    Ok(())
}
fn rss() -> String {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .unwrap_or("unavailable")
        .into()
}
