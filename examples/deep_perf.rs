//! Identical before/after workload: deep_perf ROOT. Uses private benchmark data.
use serde_json::json;
use std::time::{Duration, Instant};
use tenant_kv::*;

fn measure(mut run: impl FnMut() -> Result<ReadStats>, mode: &str) -> Result<()> {
    for _ in 0..3 {
        run()?;
    }
    let begin = Instant::now();
    let mut samples = Vec::new();
    let mut last = ReadStats::default();
    while begin.elapsed() < Duration::from_secs(2) && samples.len() < 100_000 {
        let start = Instant::now();
        last = run()?;
        samples.push(start.elapsed().as_nanos() as u64);
    }
    let elapsed = begin.elapsed().as_secs_f64();
    samples.sort_unstable();
    let n = samples.len();
    println!(
        "{}",
        json!({"mode":mode,"calls":n,"seconds":elapsed,"calls_per_second":n as f64/elapsed,"p50_us":samples[n/2]as f64/1000.,"p95_us":samples[n*95/100]as f64/1000.,"p99_us":samples[n*99/100]as f64/1000.,"stats":last})
    );
    Ok(())
}
fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .ok_or_else(|| Error::invalid("deep_perf ROOT"))?;
    let store = Store::open(
        root,
        Options {
            memtable_bytes: 64 << 20,
            query_ms: 30_000,
            ..Options::default()
        },
    )?;
    let t = store.tenant(store.create_tenant("deep-perf")?.id)?;
    t.create_collection("assets")?;
    for batch in 0..20 {
        t.write_batch(
            "assets",
            (0..500)
                .map(|i| {
                    let n = batch * 500 + i;
                    let mut doc =
                        json!({"_key":format!("{n:08}"),"score":n,"payload":"x".repeat(1024)});
                    for f in 0..24 {
                        doc[format!("field-{f}")] = json!(format!("value-{n}-{f}"));
                    }
                    WriteOp::Put {
                        document: doc,
                        if_rev: None,
                    }
                })
                .collect(),
        )?;
    }
    // Repeated internal-key updates expose retained-version costs without
    // introducing a second document/index workload into these measurements.
    for n in 0u64..8000 {
        t.engine()
            .transact(|tx| tx.put(b"Zhot".to_vec(), n.to_be_bytes().to_vec()))?;
    }
    let snapshot = t.engine().snapshot()?;
    measure(
        || {
            let mut stats = ReadStats::default();
            let mut scan = snapshot.scan(b"Z".to_vec(), Some(b"[".to_vec()), None, &mut stats)?;
            assert_eq!(
                scan.next(&mut stats)?
                    .unwrap()
                    .value
                    .as_ref()
                    .unwrap()
                    .as_slice(),
                7999u64.to_be_bytes()
            );
            assert!(scan.next(&mut stats)?.is_none());
            Ok(stats)
        },
        "mem_8000_versions_scan",
    )?;
    drop(snapshot);
    t.engine().flush()?;
    let snapshot = t.engine().snapshot()?;
    measure(
        || {
            let mut stats = ReadStats::default();
            assert_eq!(
                snapshot.get(b"Zhot", &mut stats)?.unwrap().as_slice(),
                7999u64.to_be_bytes()
            );
            Ok(stats)
        },
        "sst_8000_versions_get",
    )?;
    for (mode, select) in [
        ("topk_full", vec![]),
        (
            "topk_projected",
            vec!["/_key".to_string(), "/score".to_string()],
        ),
    ] {
        let query: Query = serde_json::from_value(
            json!({"sort":[{"field":"/score","order":"desc"}],"select":select,"limit":100}),
        )?;
        measure(
            || {
                let result = t.query("assets", query.clone())?;
                assert_eq!(result.documents.len(), 100);
                assert_eq!(result.documents[0]["score"], 9999);
                if let Some(cursor) = result.cursor {
                    t.release_cursor(&cursor)?;
                }
                Ok(result.stats)
            },
            mode,
        )?;
    }
    println!("{}", json!({"mode":"state","tenant":store.status(t.id())?}));
    Ok(())
}
