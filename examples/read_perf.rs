//! Identical native-API workload for before/after measurements. Run on an idle
//! Linux VM: read_perf ROOT [ROWS=10000] [SECONDS=2] [MODE=all] [MIN_SAMPLES=10].
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tenant_kv::*;

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let rows: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10000);
    let seconds: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(2);
    let selected = args.get(4).map_or("all", String::as_str);
    let minimum: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(10);
    assert!((10..=200_000).contains(&minimum));
    let store = Store::open(
        &args[1],
        Options {
            query_ms: 30_000,
            ..Options::default()
        },
    )?;
    let info = if let Some(info) = store.tenants().into_iter().next() {
        info
    } else {
        let info = store.create_tenant("read-perf")?;
        let t = store.tenant(info.id)?;
        t.create_collection("assets")?;
        for start in (0..rows).step_by(500) {
            t.write_batch("assets",(start..(start+500).min(rows)).map(|i| WriteOp::Put { document:json!({"_key":format!("{i:08}"),"ip":format!("ip-{i}"),"owner":format!("team-{}",i%32),"score":i,"payload":"x".repeat(512)}),if_rev:None }).collect())?;
        }
        t.create_index(
            "assets",
            IndexSpec {
                name: "ip".into(),
                fields: vec![IndexField {
                    field: "/ip".into(),
                    order: Order::Asc,
                }],
                unique: true,
                include: vec!["/owner".into()],
            },
        )?;
        t.create_lookup(
            "assets",
            LookupDefinition {
                name: "owners".into(),
                index: "ip".into(),
                select: vec!["/owner".into()],
                mode: MatchMode::All,
                max_matches: 1,
            },
        )?;
        t.engine().flush()?;
        t.engine().compact(true)?;
        info
    };
    let t = store.tenant(info.id)?;
    let mut members: Vec<Value> = (0..255).map(|i| json!(format!("absent-{i}"))).collect();
    members.push(json!("team-1"));
    let membership: Query = serde_json::from_value(
        json!({"filter":{"field":"/owner","op":"in","value":members},"select":["/_key"],"limit":1000}),
    )?;
    let exact: Query = serde_json::from_value(
        json!({"filter":{"field":"/ip","op":"eq","value":"ip-42"},"select":["/owner"]}),
    )?;
    let inputs: Vec<Value> = (0..1000)
        .map(|i| json!({"ip":format!("ip-{}",i%rows)}))
        .collect();
    let duplicates = vec![json!({"ip":"ip-42"}); 1000];
    for mode in [
        "get",
        "unique_query",
        "in_scan_256",
        "lookup_1000",
        "lookup_duplicate_1000",
    ] {
        if selected != "all" && selected != mode {
            continue;
        }
        let mut last = ReadStats::default();
        let mut run = || -> Result<usize> {
            Ok(match mode {
                "get" => {
                    let v = t.get("assets", "00000042")?.unwrap();
                    assert_eq!(v["score"], 42);
                    1
                }
                "unique_query" => {
                    let r = t.query("assets", exact.clone())?;
                    assert_eq!(r.documents, vec![json!({"owner":"team-10"})]);
                    last = r.stats;
                    r.documents.len()
                }
                "in_scan_256" => {
                    let r = t.query("assets", membership.clone())?;
                    assert_eq!(r.documents.len(), (rows + 30) / 32);
                    last = r.stats;
                    r.documents.len()
                }
                _ => {
                    let r = t.lookup_many(
                        "assets",
                        "owners",
                        if mode == "lookup_1000" {
                            &inputs
                        } else {
                            &duplicates
                        },
                    )?;
                    assert_eq!(r.matches.len(), 1000);
                    last = r.stats;
                    1000
                }
            })
        };
        for _ in 0..3 {
            std::hint::black_box(run()?);
        }
        let begin = Instant::now();
        let io_before = t.engine().metrics();
        let mut samples = Vec::new();
        let mut count = 0;
        while (begin.elapsed() < Duration::from_secs(seconds) || count < minimum) && count < 200_000
        {
            let call = Instant::now();
            std::hint::black_box(run()?);
            samples.push(call.elapsed().as_nanos() as u64);
            count += 1;
        }
        let elapsed = begin.elapsed().as_secs_f64();
        samples.sort_unstable();
        let io_after = t.engine().metrics();
        println!(
            "{}",
            json!({"mode":mode,"rows":rows,"calls":count,"elapsed_seconds":elapsed,"calls_per_second":count as f64/elapsed,"p50_us":samples[count/2]as f64/1000.0,"p95_us":samples[(count*95/100).min(count-1)]as f64/1000.0,"p99_us":samples[(count*99/100).min(count-1)]as f64/1000.0,"last_stats":last,"engine_memory_bytes":store.memory_bytes(),"io_before":io_before,"io_after":io_after})
        );
    }
    Ok(())
}
