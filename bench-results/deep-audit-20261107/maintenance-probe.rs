use tenant_kv::{*, resources::{Budget, Counter, IoScheduler}, storage::Engine};
use std::time::Instant;

#[test]
fn audit_idle_maintenance_rescans_all_files() {
    for count in [32usize, 1024, 4096] {
        let dir = tempfile::Builder::new().prefix("idkv-audit-maint-").tempdir_in("/var/tmp").unwrap();
        let options = Options { cache_bytes: 0, block_bytes: 1024, target_sst_bytes: 1024, ..Options::default() };
        let info = TenantInfo { id: uuid::Uuid::new_v4(), generation: uuid::Uuid::new_v4(), history: uuid::Uuid::new_v4(), name: "idle-sst-scan".into(), memory_bytes: options.tenant_memory_bytes, disk_bytes: options.tenant_disk_bytes };
        let path = dir.path().join("tenant");
        Engine::create(&path, &info).unwrap();
        let engine = Engine::open(path.clone(), info, options.clone(), Budget { global: Counter::new(options.global_memory_bytes), tenant: Counter::new(options.tenant_memory_bytes) }, IoScheduler::new(4)).unwrap();
        engine.transact(|tx| {
            let mut random = 0x85a2bc9du64;
            for n in 0..count {
                let value = (0..1024).map(|_| {random ^= random << 13; random ^= random >> 7; random ^= random << 17; random as u8}).collect();
                tx.put(format!("Z{n:08}").into_bytes(), value)?;
            }
            Ok(())
        }).unwrap();
        engine.flush().unwrap();
        engine.compact(true).unwrap();
        let before = engine.metrics();
        let mut times = Vec::new();
        for _ in 0..7 {
            let started = Instant::now();
            engine.maintain().unwrap();
            times.push(started.elapsed().as_secs_f64()*1000.0);
        }
        let after = engine.metrics();
        assert_eq!(before["scheduler"]["completed_jobs"], after["scheduler"]["completed_jobs"]);
        assert_eq!(before["compaction_write_bytes"], after["compaction_write_bytes"]);
        times.sort_by(f64::total_cmp);
        println!("AUDIT idle tables={:?}, entries={count}, maintain_median_ms={:.3}, samples_ms={times:?}", engine.table_counts(), times[3]);
    }
}
