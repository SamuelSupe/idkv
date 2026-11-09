//! Compare sequential resolution and native batching with identical, verified inputs.
//! cargo run --release --example identity_resolve_many -- ROOT [SAMPLES=20]
//! ROOT must be new. Cache-disabled runs still use the OS page cache.
use serde_json::json;
use std::{path::Path, time::Instant};
use tenant_kv::{identity::*, *};

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let root = args.get(1).expect("new database root");
    let samples = args
        .get(2)
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(20);
    assert!((5..=1000).contains(&samples));
    let context = IdentityContext {
        source: "benchmark".into(),
        actor: "local".into(),
        can_correct: true,
    };
    for cache_bytes in [16 << 20, 0] {
        let store = Store::open(
            Path::new(root).join(format!("cache-{cache_bytes}")),
            Options {
                cache_bytes,
                query_ms: 30_000,
                ..Default::default()
            },
        )?;
        for (identifiers, segments) in [(1000, 1), (8, 64)] {
            let tenant = store.tenant(
                store
                    .create_tenant(&format!("ids-{identifiers}-segments-{segments}"))?
                    .id,
            )?;
            tenant.identity_initialize(&context, IdentityConfig::default())?;
            let mut entities = Vec::new();
            for n in 0..2 {
                entities.push(tenant.identity_write(&context, serde_json::from_value(json!({"action":"create_entity","operation_id":format!("entity-{n}"),"attributes":{}}))?)?.entity.unwrap().entity_id);
            }
            let ids: Vec<_> = (0..identifiers)
                .map(|n| Identifier {
                    kind: "account".into(),
                    namespace: "bench".into(),
                    value: format!("id-{n}"),
                })
                .collect();
            let mut revisions = vec![json!("absent"); identifiers];
            let base = parse_time("2026-01-01T00:00:00Z")?;
            let time = |n| {
                chrono::DateTime::from_timestamp_micros(base + n as i64 * 1_000_000)
                    .unwrap()
                    .to_rfc3339()
            };
            let mut known = None;
            for segment in 0..segments {
                for start in (0..identifiers).step_by(32) {
                    let end = (start + 32).min(identifiers);
                    let changes: Vec<_> = (start..end).map(|n| json!({"identifier":ids[n],"expected_revision":revisions[n],"effective":{"mode":"manual","from":time(segment),"to":time(segment+1)},"change":{"action":"bind","entity_id":entities[segment%2],"reason":"benchmark fixture"}})).collect();
                    let receipt = tenant.identity_write(&context, serde_json::from_value(json!({"action":"bindings","operation_id":format!("segment-{segment}-start-{start}"),"changes":changes}))?)?;
                    for (n, changed) in receipt.timelines.iter().enumerate() {
                        revisions[start + n] = json!(changed.revision);
                    }
                    known = Some(receipt.knowledge.version.to_string());
                }
            }
            tenant.engine().flush()?;
            tenant.engine().compact(true)?;
            let prepared = tenant.prepare_dsl("resolvemany with $request")?;
            for repeated in [false, true] {
                if identifiers == 8 && !repeated {
                    continue;
                }
                for fixed in [false, true] {
                    for count in [1, 32, 128, 1000] {
                        let unique = if repeated {
                            8.min(identifiers)
                        } else {
                            identifiers
                        };
                        let request = ResolveManyRequest {
                            known_at: if fixed { known.clone() } else { None },
                            items: (0..count)
                                .map(|n| ResolveItem {
                                    identifier: ids[n % unique].clone(),
                                    valid_at: time(n % segments),
                                })
                                .collect(),
                        };
                        let expected = tenant.identity_resolve_many(&request)?;
                        for (n, result) in expected.results.iter().enumerate() {
                            assert_eq!(
                                result.binding.as_ref().unwrap().entity_id,
                                entities[(n % segments) % 2]
                            );
                        }
                        let params = json!({"request":request});
                        let output = serde_json::to_value(prepared.execute(
                            &tenant,
                            params.as_object().unwrap(),
                            None,
                        )?)?;
                        assert_eq!(output["results"], serde_json::to_value(&expected.results)?);
                        let mut singles = Vec::new();
                        let mut batches = Vec::new();
                        let mut document_reads = [0u64; 2];
                        let mut blocks = [0u64; 2];
                        for round in 0..samples {
                            // Alternate order to reduce warmup/order bias.
                            for batched in if round % 2 == 0 {
                                [false, true]
                            } else {
                                [true, false]
                            } {
                                let begin = Instant::now();
                                let (results, reads, block_reads) = if batched {
                                    let result = tenant.identity_resolve_many(&request)?;
                                    (
                                        result.results,
                                        result.stats.document_reads,
                                        result.stats.blocks_read,
                                    )
                                } else {
                                    let mut out = Vec::with_capacity(count);
                                    let mut reads = 0;
                                    let mut block_reads = 0;
                                    for item in &request.items {
                                        let result = tenant.identity_resolve(&ResolveRequest {
                                            identifier: item.identifier.clone(),
                                            valid_at: item.valid_at.clone(),
                                            known_at: request.known_at.clone(),
                                        })?;
                                        reads += result.stats.document_reads;
                                        block_reads += result.stats.blocks_read;
                                        out.push(ResolveMatch {
                                            timeline_revision: result.timeline_revision,
                                            binding: result.binding,
                                        });
                                    }
                                    (out, reads, block_reads)
                                };
                                let elapsed = begin.elapsed().as_micros() as u64;
                                if batched {
                                    batches.push(elapsed);
                                } else {
                                    singles.push(elapsed);
                                }
                                document_reads[batched as usize] = reads;
                                blocks[batched as usize] = block_reads;
                                assert_eq!(results, expected.results);
                            }
                        }
                        singles.sort_unstable();
                        batches.sort_unstable();
                        println!(
                            "{}",
                            json!({"cache_bytes":cache_bytes,"segments":segments,"repeated":repeated,"fixed":fixed,"items":count,"samples":samples,"single_p50_us":singles[samples/2],"batch_p50_us":batches[samples/2],"single_p95_us":singles[samples*95/100],"batch_p95_us":batches[samples*95/100],"single_document_reads":document_reads[0],"batch_document_reads":document_reads[1],"single_blocks":blocks[0],"batch_blocks":blocks[1]})
                        );
                    }
                }
            }
        }
    }
    Ok(())
}
