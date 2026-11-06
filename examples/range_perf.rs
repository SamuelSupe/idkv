//! Reproducible native-API workload: range_perf ROOT seed|read|mixed|mixed_paced|walk [ROWS=80000] [SECONDS=60].
//! Run identical binaries against independent copies of the seeded directory.
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tenant_kv::*;

fn document(i: usize, revision: usize) -> Value {
    // Deterministic, poorly compressible payload; repeated "x" would not exercise disk residency.
    let mut state = (i as u64 + 1).wrapping_mul(6364136223846793005);
    let mut payload = String::with_capacity(4096);
    for _ in 0..512 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        payload.push_str(&format!("{:08x}", state as u32));
    }
    json!({"_key":format!("{i:08}"),"score":i,"group":i/2,"env":i%4,"revision":revision,"payload":payload})
}
fn report(mode: &str, mut samples: Vec<u64>, elapsed: f64, stats: ReadStats) -> Value {
    samples.sort_unstable();
    let n = samples.len();
    json!({"mode":mode,"calls":n,"seconds":elapsed,"calls_per_second":n as f64/elapsed,"p50_us":samples[n/2] as f64/1000.,"p95_us":samples[n*95/100] as f64/1000.,"p99_us":samples[n*99/100] as f64/1000.,"last_stats":stats})
}
fn query_for(mode: &str, rows: usize) -> Query {
    serde_json::from_value(match mode {
        "primary_tail" => json!({"filter":{"field":"/_key","op":"gte","value":format!("{:08}",rows-100)},"select":["/_key","/score"],"limit":100}),
        "unique_range" => json!({"filter":{"field":"/score","op":"gte","value":rows-100},"sort":[{"field":"/score","order":"asc"}],"select":["/_key","/score"],"limit":100}),
        _ => json!({"filter":{"field":"/group","op":"in","value":[rows/4,rows/2-1,rows/4]},"select":["/_key","/group"],"limit":100}),
    }).unwrap()
}
fn run(t: &TenantHandle, mode: &str, rows: usize, inputs: &[Value]) -> Result<ReadStats> {
    if mode == "lookup_256" {
        let result = t.lookup_many("assets", "groups", inputs)?;
        assert_eq!(result.matches.len(), 256);
        for (input, matched) in inputs.iter().zip(&result.matches) {
            assert_eq!(matched.len(), 2);
            assert!(matched.iter().all(|v| v["group"] == input["group"]));
        }
        return Ok(result.stats);
    }
    if mode == "batch_ranges_8" {
        let operations:Vec<_>=(0..8).map(|i|ReadOperation { collection:"assets".into(), request:ReadRequest::Query { query:serde_json::from_value(json!({"filter":{"and":[{"field":"/_key","op":"gte","value":format!("{:08}",rows-100-i*10)},{"field":"/_key","op":"lt","value":format!("{:08}",rows-90-i*10)}]},"select":["/_key"],"limit":10})).unwrap() }}).collect();
        let result = t.read_batch(&operations)?;
        for (i, r) in result.results.iter().enumerate() {
            let ReadResult::Query { result: r } = r else {
                panic!("query result")
            };
            assert_eq!(r.documents.len(), 10);
            assert_eq!(
                r.documents[0]["_key"],
                format!("{:08}", rows - 100 - i * 10)
            );
            assert!(r.cursor.is_none());
        }
        return Ok(result.stats);
    }
    let result = t.query("assets", query_for(mode, rows))?;
    assert_eq!(
        result.documents.len(),
        if mode == "secondary_in" { 4 } else { 100 }
    );
    assert!(result.cursor.is_none());
    if mode != "secondary_in" {
        assert_eq!(result.documents[0]["score"], rows - 100);
    }
    Ok(result.stats)
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let rows = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(80_000);
    let seconds = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(60);
    let options = Options {
        global_memory_bytes: 128 << 20,
        tenant_memory_bytes: 64 << 20,
        cache_bytes: 8 << 20,
        memtable_bytes: 2 << 20,
        target_sst_bytes: 4 << 20,
        level_base_bytes: 16 << 20,
        l0_stop: 24,
        max_scan: rows * 10,
        query_ms: 120_000,
        snapshot_seconds: 180,
        ..Options::default()
    };
    let store = Store::open(&args[1], options)?;
    if args[2] == "seed" {
        assert!(store.tenants().is_empty(), "seed directory must be empty");
        let t = store.tenant(store.create_tenant("range-perf")?.id)?;
        t.create_collection("assets")?;
        for (name, fields, unique) in [
            ("score", vec![("/score", Order::Asc)], true),
            ("group", vec![("/group", Order::Asc)], false),
            (
                "env-score",
                vec![("/env", Order::Asc), ("/score", Order::Desc)],
                false,
            ),
        ] {
            t.create_index(
                "assets",
                IndexSpec {
                    name: name.into(),
                    fields: fields
                        .into_iter()
                        .map(|(field, order)| IndexField {
                            field: field.into(),
                            order,
                        })
                        .collect(),
                    unique,
                    include: vec![],
                },
            )?;
        }
        for start in (0..rows).step_by(100) {
            let operations: Vec<_> = (start..(start + 100).min(rows))
                .map(|i| WriteOp::Put {
                    document: document(i, 0),
                    if_rev: None,
                })
                .collect();
            let deadline = Instant::now() + Duration::from_secs(120);
            loop {
                match t.write_batch("assets", operations.clone()) {
                    Ok(_) => break,
                    Err(e) if e.code == "RESOURCE_EXHAUSTED" && Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(10))
                    }
                    Err(e) => return Err(e),
                }
            }
            if start % 10000 == 0 {
                eprintln!("seed {start}/{rows}");
            }
        }
        t.create_lookup(
            "assets",
            LookupDefinition {
                name: "groups".into(),
                index: "group".into(),
                select: vec!["/_key".into(), "/group".into()],
                mode: MatchMode::All,
                max_matches: 2,
            },
        )?;
        t.engine().flush()?;
        t.engine().compact(true)?;
        t.engine().verify()?;
        println!(
            "{}",
            json!({"mode":"seed","rows":rows,"payload_bytes":rows*4096,"engine_memory_bytes":store.memory_bytes(),"metrics":t.engine().metrics()})
        );
        return Ok(());
    }
    let t = store.tenant(store.tenants()[0].id)?;
    let inputs: Vec<_> = (0..256).map(|i| json!({"group":rows/4+i})).collect();
    if args[2] == "read" {
        for mode in [
            "primary_tail",
            "unique_range",
            "secondary_in",
            "lookup_256",
            "batch_ranges_8",
        ] {
            let cold = Instant::now();
            let first = run(&t, mode, rows, &inputs)?;
            println!(
                "{}",
                json!({"mode":mode,"phase":"first_call","us":cold.elapsed().as_micros(),"stats":first})
            );
            let start = Instant::now();
            let mut samples = Vec::new();
            let mut last = ReadStats::default();
            while samples.len() < 100 || start.elapsed() < Duration::from_secs(seconds) {
                let call = Instant::now();
                last = run(&t, mode, rows, &inputs)?;
                samples.push(call.elapsed().as_nanos() as u64);
                if samples.len() >= 100_000 {
                    break;
                }
            }
            println!(
                "{}",
                report(mode, samples, start.elapsed().as_secs_f64(), last)
            );
        }
    } else if args[2] == "walk" {
        let started = Instant::now();
        let mut count = 0;
        let mut samples = Vec::new();
        let mut stats = ReadStats::default();
        while count < rows {
            let query: Query = serde_json::from_value(
                json!({"filter":{"field":"/_key","op":"gte","value":format!("{count:08}")},"limit":1000}),
            )?;
            let call = Instant::now();
            let result = t.query("assets", query)?;
            samples.push(call.elapsed().as_nanos() as u64);
            assert_eq!(result.documents.len(), (rows - count).min(1000));
            for row in &result.documents {
                assert_eq!(*row, {
                    let mut expected = document(count, 0);
                    expected["_rev"] = row["_rev"].clone();
                    expected
                });
                count += 1;
            }
            stats.add(&result.stats);
            if let Some(cursor) = result.cursor {
                t.release_cursor(&cursor)?;
            }
        }
        println!(
            "{}",
            report(
                "walk_all_documents",
                samples,
                started.elapsed().as_secs_f64(),
                stats
            )
        );
    } else if args[2] == "mixed" || args[2] == "mixed_paced" {
        let paced = args[2] == "mixed_paced";
        let stop = Arc::new(AtomicBool::new(false));
        let writer = t.clone();
        let done = stop.clone();
        let worker = std::thread::spawn(move || -> Result<Value> {
            let start = Instant::now();
            let mut updated = 0;
            let mut rejected = 0;
            while !done.load(Ordering::Relaxed) {
                let batch = (0..20)
                    .map(|n| WriteOp::Put {
                        document: document((updated + n) % rows, updated + 1),
                        if_rev: None,
                    })
                    .collect();
                match writer.write_batch("assets", batch) {
                    Ok(_) => updated += 20,
                    Err(e) if e.code == "RESOURCE_EXHAUSTED" => {
                        rejected += 1;
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(e) => return Err(e),
                }
                let target = Duration::from_millis(updated as u64);
                if let Some(wait) = target.checked_sub(start.elapsed()) {
                    std::thread::sleep(wait);
                }
            }
            Ok(
                json!({"mode":"writer","updates":updated,"backpressure":rejected,"seconds":start.elapsed().as_secs_f64()}),
            )
        });
        let deadline = Instant::now() + Duration::from_secs(seconds);
        let mut readers = Vec::new();
        for mode in ["primary_tail", "unique_range", "secondary_in", "lookup_256"] {
            let t = t.clone();
            let inputs = inputs.clone();
            readers.push(std::thread::spawn(move || -> Result<Value> {
                let start = Instant::now();
                let mut samples = Vec::new();
                let mut stats = ReadStats::default();
                while Instant::now() < deadline {
                    let call = Instant::now();
                    stats = run(&t, mode, rows, &inputs)?;
                    samples.push(call.elapsed().as_nanos() as u64);
                    if paced {
                        let period = if mode == "lookup_256" { 20 } else { 1000 };
                        let due = start + Duration::from_millis(samples.len() as u64 * period);
                        if let Some(wait) = due.min(deadline).checked_duration_since(Instant::now())
                        {
                            std::thread::sleep(wait);
                        }
                    }
                }
                Ok(report(mode, samples, start.elapsed().as_secs_f64(), stats))
            }));
        }
        let mut results = Vec::new();
        for reader in readers {
            results.push(reader.join().unwrap());
        }
        stop.store(true, Ordering::Relaxed);
        let writer = worker.join().unwrap();
        for result in results {
            println!("{}", result?);
        }
        println!("{}", writer?);
        t.engine().flush()?;
        t.engine().compact(true)?;
        t.engine().verify()?;
    } else {
        return Err(Error::invalid(
            "mode must be seed, read, mixed, mixed_paced or walk",
        ));
    }
    println!(
        "{}",
        json!({"mode":"resources","engine_memory_bytes":store.memory_bytes(),"metrics":t.engine().metrics()})
    );
    Ok(())
}
