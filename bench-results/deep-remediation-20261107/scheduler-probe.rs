#[cfg(test)]
static AUDIT_MAINT_DELAY: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static AUDIT_MAINT_COUNTS: std::sync::OnceLock<Mutex<BTreeMap<Uuid, usize>>> = std::sync::OnceLock::new();

#[cfg(test)]
mod audit_scheduler {
    use super::*;
    #[test]
    fn fixed_full_maintenance_queue_serves_all_tenants() {
        let dir = tempfile::Builder::new().prefix("idkv-audit-queue-").tempdir_in("/var/tmp").unwrap();
        let store = Store::open(dir.path(), Options { io_workers: 1, ..Options::default() }).unwrap();
        for n in 0..160 {
            let id = store.create_tenant(&format!("queued-{n}")).unwrap().id;
            drop(store.tenant(id).unwrap());
        }
        let counts = AUDIT_MAINT_COUNTS.get_or_init(|| Mutex::new(BTreeMap::new()));
        AUDIT_MAINT_DELAY.store(true, Ordering::Release);
        thread::sleep(Duration::from_millis(3500));
        counts.lock().clear();
        thread::sleep(Duration::from_secs(4));
        AUDIT_MAINT_DELAY.store(false, Ordering::Release);
        let counts = counts.lock();
        let jobs = counts.values().sum::<usize>();
        println!("AUDIT maintenance coordinator: tenants=160, workers=1, injected_per_job_delay_ms=20, observation_ms=4000, completed_jobs={jobs}, served_tenants={}, never_served={}", counts.len(), 160-counts.len());
        assert!(jobs > 160, "not enough completed work to test starvation");
        assert_eq!(counts.len(), 160, "some tenants still starved");
    }
}
