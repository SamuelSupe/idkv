    #[test]
    fn audit_failed_drain_must_resume_interrupted_index() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let options = Options { memtable_bytes: 32 << 10, l0_trigger: 2, l0_stop: 4, ..Default::default() };
        let store = Store::open(dir.path(), options).unwrap();
        store.owner.stop.store(true, Ordering::Release);
        for worker in store.owner.workers.lock().drain(..) { worker.join().unwrap(); }
        let id = store.create_tenant("failed-drain-recovery").unwrap().id;
        let tenant = store.tenant(id).unwrap();
        tenant.create_collection("docs").unwrap();
        let payload = "x".repeat(1024);
        let keys: Vec<_> = (0..2048).map(|n| format!("{n:04}")).collect();
        let mut acknowledged = Vec::new();
        for batch in keys.chunks(256) {
            acknowledged.extend(tenant.write_batch("docs", batch.iter().map(|key| WriteOp::Put {
                document: json!({"_key":key,"value":key.parse::<u64>().unwrap(),"payload":payload}), if_rev: None
            }).collect()).unwrap());
            tenant.engine().flush().unwrap();
            tenant.engine().compact(true).unwrap();
        }
        let building = tenant.clone();
        let build = thread::spawn(move || building.create_index("docs", IndexSpec {
            name: "long".into(), fields: vec![IndexField { field: "/value".into(), order: Order::Asc }],
            unique: true, include: vec!["/payload".into()]
        }));
        let deadline = Instant::now() + Duration::from_secs(5);
        while tenant.engine().table_counts().get(&0).copied().unwrap_or(0) < 4 {
            assert!(Instant::now() < deadline, "build never entered backpressure");
            thread::sleep(Duration::from_millis(1));
        }
        // Run exactly the recovery worker's normal inspection while the build
        // is active; it records a catalog with no abandoned generation yet.
        let stop = AtomicBool::new(false);
        tenant.restart_interrupted_build(&stop).unwrap();
        let cached = tenant.core.recovery_catalog.load(Ordering::Acquire);
        assert_eq!(cached, tenant.engine().catalog_seq());
        let management = store.close_tenant(id).unwrap_err();
        assert_eq!(management.code, "BUSY"); // this retained handle prevents close
        assert_eq!(build.join().unwrap().unwrap_err().code, "BUSY");
        assert!(tenant.core.builds.lock().is_empty());
        tenant.engine().compact(true).unwrap();
        let ack = tenant.put("docs", json!({"_key":"0000","value":9999,"payload":payload}), None).unwrap();
        acknowledged[0] = ack;
        for _ in 0..20 {
            tenant.restart_interrupted_build(&stop).unwrap();
            thread::sleep(Duration::from_millis(10));
        }
        let stranded = tenant.indexes("docs").unwrap().remove(0);
        let unchanged_catalog = tenant.engine().catalog_seq() == cached;
        let mut checked = 0;
        for (batch, acks) in keys.chunks(128).zip(acknowledged.chunks(128)) {
            for (doc, ack) in tenant.multi_get("docs", batch).unwrap().into_iter().zip(acks) {
                let doc = doc.unwrap();
                assert_eq!(doc["_rev"], ack.rev);
                assert_eq!(doc["payload"], payload);
                checked += 1;
            }
        }
        // Positive control: reopen the same engine, then use real maintenance
        // to remove admission pressure while the normal recovery method runs.
        drop(tenant);
        store.close_tenant(id).unwrap();
        let reopened = store.tenant(id).unwrap();
        let engine = reopened.engine().clone();
        let finish = Arc::new(AtomicBool::new(false));
        let cleaning = finish.clone();
        let maintenance = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !cleaning.load(Ordering::Acquire) && Instant::now() < deadline {
                let _ = engine.maintain();
                thread::sleep(Duration::from_millis(2));
            }
            cleaning.store(true, Ordering::Release);
        });
        let recovered = reopened.restart_interrupted_build(&finish);
        finish.store(true, Ordering::Release);
        maintenance.join().unwrap();
        recovered.unwrap();
        let ready = reopened.indexes("docs").unwrap().remove(0);
        assert_eq!(ready.state, IndexState::Ready);
        assert_eq!(ready.entries, 2048);
        println!("AUDIT_JSON {}", json!({"case":"failed-drain-index","management_error":management.code,"state_after_20_recovery_checks":stranded.state,"active_builds":0,"catalog_unchanged":unchanged_catalog,"acknowledged_documents_checked":checked,"acknowledged_mismatches":0,"reopen_control_state":ready.state}));
        assert_eq!(stranded.state, IndexState::Ready, "recovery skipped abandoned BUILDING after failed management drain");
    }

    #[test]
    fn audit_deleted_registry_should_allow_new_tenant_after_churn() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let (healthy, ack) = {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            let id = store.create_tenant("sentinel").unwrap().id;
            let tenant = store.tenant(id).unwrap();
            tenant.create_collection("docs").unwrap();
            let ack = tenant.put("docs", json!({"_key":"ack","value":7}), None).unwrap();
            (id, ack)
        };
        let path = dir.path().join("control/tenants.json");
        let mut registry: Registry = read_json(&path).unwrap();
        let bound = (16 << 20) - 8;
        let initial = serde_json::to_vec(&registry).unwrap().len();
        let template = Registration { info: TenantInfo {
            id: Uuid::new_v4(), name: "deleted".into(), generation: Uuid::new_v4(), history: Uuid::new_v4(),
            memory_bytes: Options::default().tenant_memory_bytes, disk_bytes: Options::default().tenant_disk_bytes
        }, lifecycle: Lifecycle::Deleted };
        let row_bytes = serde_json::to_vec(&template).unwrap().len() + 40;
        let count = (bound - initial - 128) / row_bytes;
        let mut ids = Vec::new();
        for _ in 0..count {
            let mut row = template.clone();
            row.info.id = Uuid::new_v4(); row.info.generation = Uuid::new_v4(); row.info.history = Uuid::new_v4();
            ids.push(row.info.id);
            registry.tenants.insert(row.info.id, row);
        }
        let mut padding = bound - 64 - serde_json::to_vec(&registry).unwrap().len();
        for id in ids.iter().rev().take(10) {
            let name = &mut registry.tenants.get_mut(id).unwrap().info.name;
            let extra = padding.min(128 - name.len());
            name.push_str(&"x".repeat(extra)); padding -= extra;
        }
        assert_eq!(padding, 0);
        atomic_json(&path, &registry).unwrap();
        let registry_bytes = fs::metadata(&path).unwrap().len();
        drop(registry);
        let mut responses = Vec::new();
        let mut checks = 0;
        for round in 0..3 {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            let result = store.create_tenant(&format!("new-{round}"));
            responses.push(result.map(|_| "OK".to_string()).unwrap_or_else(|e| e.code));
            let tenant = store.tenant(healthy).unwrap();
            assert_eq!(tenant.get("docs", "ack").unwrap().unwrap()["_rev"], ack.rev);
            checks += 1;
        }
        {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            store.delete_tenant(healthy).unwrap();
        }
        let store = Store::open(dir.path(), Options::default()).unwrap();
        assert!(store.tenants().is_empty());
        let after_delete = store.create_tenant("no-live-tenants").map(|_| "OK".to_string()).unwrap_or_else(|e| e.code);
        println!("AUDIT_JSON {}", json!({"case":"deleted-registry-capacity","fixture_deleted_tenants":count,"registry_bytes":registry_bytes,"create_after_restarts":responses,"live_tenants_after_delete":0,"create_with_no_live_tenants":after_delete,"confirmed_sentinel_checks":checks,"acknowledged_mismatches":0}));
        assert_eq!(after_delete, "OK", "deleted history permanently exhausts tenant creation capacity");
    }
