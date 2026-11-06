//! Bounded phase-one resolution cost by timeline size, not a native batch benchmark.
//! identity_perf ROOT [SAMPLES=1000]
use serde_json::json;
use std::time::Instant;
use tenant_kv::{identity::*, *};
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let samples: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(1000);
    assert!((100..=10_000).contains(&samples));
    let store = Store::open(
        &args[1],
        Options {
            memtable_bytes: 4 << 20,
            cache_bytes: 16 << 20,
            query_ms: 30_000,
            ..Options::default()
        },
    )?;
    let context = IdentityContext {
        source: "benchmark".into(),
        actor: "local".into(),
        can_correct: true,
    };
    for segments in [1usize, 10, 100, 1000] {
        let t = store.tenant(store.create_tenant(&format!("segments-{segments}"))?.id)?;
        t.identity_initialize(&context, IdentityConfig::default())?;
        let mut ids = Vec::new();
        for i in 0..2 {
            let request = serde_json::from_value(
                json!({"action":"create_entity","operation_id":format!("entity-{i}"),"attributes":{}}),
            )?;
            ids.push(
                t.identity_write(&context, request)?
                    .entity
                    .unwrap()
                    .entity_id,
            );
        }
        let identifier = Identifier {
            kind: "account".into(),
            namespace: "bench".into(),
            value: "one".into(),
        };
        let base = parse_time("2026-01-01T00:00:00Z")?;
        let timestamp = |n: usize| {
            chrono::DateTime::from_timestamp_micros(base + (n as i64) * 1_000_000)
                .unwrap()
                .to_rfc3339()
        };
        let mut revision = json!("absent");
        let mut version = None;
        let mut historical = None;
        let load = Instant::now();
        for i in 0..segments {
            let request = serde_json::from_value(
                json!({"action":"bindings","operation_id":format!("segment-{i}"),"changes":[{"identifier":identifier,"expected_revision":revision,"effective":{"mode":"manual","from":timestamp(i),"to":timestamp(i+1)},"change":{"action":"bind","entity_id":ids[i%2],"reason":"benchmark source"}}]}),
            )?;
            let receipt = t.identity_write(&context, request)?;
            revision = json!(receipt.timelines[0].revision);
            version = Some(receipt.knowledge.version);
            if i == segments / 2 {
                historical = version;
            }
        }
        let load_ms = load.elapsed().as_secs_f64() * 1000.;
        t.engine().flush()?;
        t.engine().compact(true)?;
        assert_eq!(
            t.identity_timeline(&identifier)?.unwrap().segments.len(),
            segments
        );
        for (mode, known_at, at) in [
            ("latest", None, segments - 1),
            ("fixed_current", version, segments - 1),
            ("fixed_earlier", historical, segments / 2),
        ] {
            let request = ResolveRequest {
                identifier: identifier.clone(),
                valid_at: timestamp(at),
                known_at: known_at.map(|v| v.to_string()),
            };
            for _ in 0..10 {
                t.identity_resolve(&request)?;
            }
            let before = t.engine().metrics();
            let mut timing = Vec::with_capacity(samples);
            let mut stats = ReadStats::default();
            for _ in 0..samples {
                let now = Instant::now();
                let result = t.identity_resolve(&request)?;
                timing.push(now.elapsed().as_nanos() as u64);
                assert_eq!(result.binding.unwrap().entity_id, ids[at % 2]);
                assert_eq!(result.stats.document_reads, 2);
                stats = result.stats;
            }
            timing.sort_unstable();
            println!(
                "{}",
                json!({"version":env!("CARGO_PKG_VERSION"),"segments":segments,"mode":mode,"samples":samples,"load_ms":load_ms,"p50_us":timing[samples/2]as f64/1000.,"p95_us":timing[samples*95/100]as f64/1000.,"p99_us":timing[samples*99/100]as f64/1000.,"stats":stats,"engine_memory_bytes":store.memory_bytes(),"io_before":before,"io_after":t.engine().metrics()})
            );
        }
    }
    Ok(())
}
