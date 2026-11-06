use serde_json::json;
use std::time::Instant;
use tenant_kv::*;

pub fn run(t: &TenantHandle, rows: usize) -> Result<()> {
    if !t.lookups("assets")?.iter().any(|l| l.name == "body") {
        t.create_lookup(
            "assets",
            LookupDefinition {
                name: "body".into(),
                index: "ip".into(),
                select: vec!["/_key".into(), "/payload".into()],
                mode: MatchMode::All,
                max_matches: 1,
            },
        )?;
    }
    let mut seed = 72453u64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed as usize) % rows
    };
    for mode in [
        "random_get",
        "missing_get",
        "random_lookup_1000",
        "body_lookup_1000",
        "missing_lookup_1000",
    ] {
        let mut times = Vec::new();
        let mut stats = ReadStats::default();
        let started = Instant::now();
        let iterations = if mode.ends_with("get") { 5000 } else { 20 };
        for _ in 0..iterations {
            let inputs: Vec<_> = (0..if mode.ends_with("get") { 1 } else { 1000 })
                .map(|_| next())
                .collect();
            let begin = Instant::now();
            if mode.ends_with("get") {
                let key = if mode == "missing_get" {
                    format!("absent-{}", inputs[0])
                } else {
                    format!("{:012}", inputs[0])
                };
                let value = t.get("assets", &key)?;
                if value.is_some() != (mode == "random_get") {
                    return Err(Error::corrupt("probe get cardinality"));
                }
            } else {
                let values:Vec<_>=inputs.iter().map(|i|json!({"ip":if mode=="missing_lookup_1000"{format!("absent-{i}")}else{format!("ip-{i}")}})).collect();
                let result = t.lookup_many(
                    "assets",
                    if mode == "body_lookup_1000" {
                        "body"
                    } else {
                        "owners"
                    },
                    &values,
                )?;
                let expected = usize::from(mode != "missing_lookup_1000");
                if result.matches.iter().any(|v| v.len() != expected) {
                    return Err(Error::corrupt("probe lookup cardinality"));
                }
                stats.add(&result.stats);
            }
            times.push(begin.elapsed().as_micros() as u64);
        }
        times.sort_unstable();
        println!(
            "{}",
            json!({"phase":"probe","mode":mode,"rows":rows,"operations":times.len(),"seconds":started.elapsed().as_secs_f64(),
            "p50_us":times[times.len()/2],"p95_us":times[times.len()*95/100],"p99_us":times[times.len()*99/100],"stats":if mode.ends_with("get"){serde_json::Value::Null}else{serde_json::to_value(&stats)?},
            "engine_memory_bytes":t.engine().memory_bytes(),"levels":t.engine().table_counts()})
        );
    }
    Ok(())
}
