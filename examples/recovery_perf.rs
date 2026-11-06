//! Cold engine opens over many SST files. OS caches are not dropped.
use serde_json::json;
use std::{path::PathBuf, time::Instant};
use tenant_kv::{
    resources::{Budget, Counter, IoScheduler},
    storage::{Engine, read_json},
    *,
};
use uuid::Uuid;

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let path = PathBuf::from(
        args.get(1)
            .ok_or_else(|| Error::invalid("ROOT seed|open [FILES=1000]"))?,
    );
    let seed = match args.get(2).map(String::as_str) {
        Some("seed") => true,
        Some("open") => false,
        _ => return Err(Error::invalid("expected seed or open")),
    };
    let files = args
        .get(3)
        .map(|n| n.parse::<usize>())
        .transpose()
        .map_err(|_| Error::invalid("file count"))?
        .unwrap_or(1000);
    if !(1..=10000).contains(&files) {
        return Err(Error::invalid("file count must be 1..10000"));
    }
    let options = Options {
        l0_trigger: 20000,
        l0_stop: 20001,
        ..Options::default()
    };
    let info = if seed {
        let info = TenantInfo {
            id: Uuid::new_v4(),
            name: "recovery-bench".into(),
            generation: Uuid::new_v4(),
            history: Uuid::new_v4(),
            memory_bytes: options.tenant_memory_bytes,
            disk_bytes: options.tenant_disk_bytes,
        };
        Engine::create(&path, &info)?;
        info
    } else {
        read_json(&path.join("TENANT.meta"))?
    };
    let budget = Budget {
        global: Counter::new(options.global_memory_bytes),
        tenant: Counter::new(info.memory_bytes),
    };
    let io = IoScheduler::new(2);
    let started = Instant::now();
    let engine = Engine::open(path, info, options, budget, io)?;
    let open_us = started.elapsed().as_micros();
    if seed {
        for i in 0..files {
            engine.transact(|tx| {
                tx.put(
                    format!("bench-{i:05}").into_bytes(),
                    i.to_be_bytes().to_vec(),
                )
            })?;
            engine.flush()?;
        }
    }
    let snapshot = engine.snapshot()?;
    let mut stats = ReadStats::default();
    let mut scan = snapshot.scan(
        b"bench-".to_vec(),
        Some(b"bench.".to_vec()),
        None,
        &mut stats,
    )?;
    let mut count = 0usize;
    while let Some(record) = scan.next(&mut stats)? {
        if record.key != format!("bench-{count:05}").as_bytes()
            || record.value.as_ref().map(|v| v.as_slice()) != Some(count.to_be_bytes().as_slice())
        {
            return Err(Error::corrupt("recovery benchmark value mismatch"));
        }
        count += 1;
    }
    if count != files || engine.seq() != files as u64 {
        return Err(Error::corrupt(
            "recovery benchmark count or sequence mismatch",
        ));
    }
    println!(
        "{}",
        json!({"version":env!("CARGO_PKG_VERSION"),"seed":seed,
        "open_us":open_us,"verified_records":count,"levels":engine.table_counts(),
        "memory_bytes":engine.memory_bytes()})
    );
    Ok(())
}
