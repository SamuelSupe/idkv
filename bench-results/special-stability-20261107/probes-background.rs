    #[test]
    fn audit_real_background_recovery_after_failed_drain() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Options { memtable_bytes: 2 << 20, ..Default::default() }).unwrap();
        let id = store.create_tenant("automatic-failed-drain").unwrap().id;
        let tenant = store.tenant(id).unwrap();
        tenant.create_collection("docs").unwrap();
        let payload = "b".repeat(1024);
        let keys: Vec<_> = (0..16384).map(|n| format!("{n:05}")).collect();
        let mut acknowledged = Vec::new();
        for batch in keys.chunks(256) {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let operations = batch.iter().map(|key| WriteOp::Put {
                    document: json!({"_key":key,"value":key.parse::<u64>().unwrap(),"payload":payload}), if_rev: None
                }).collect();
                match tenant.write_batch("docs", operations) {
                    Ok(acks) => { acknowledged.extend(acks); break; }
                    Err(error) if error.code == "RESOURCE_EXHAUSTED" => {
                        assert!(Instant::now() < deadline);
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("{error}"),
                }
            }
        }
        tenant.engine().flush().unwrap();
        tenant.engine().compact(true).unwrap();
        let building = tenant.clone();
        let build = thread::spawn(move || building.create_index("docs", IndexSpec {
            name: "automatic-long".into(),
            fields: ["/value", "/a", "/b", "/c", "/d", "/e", "/f", "/g"].iter().map(|field| IndexField { field:(*field).into(), order:Order::Asc }).collect(),
            unique:false, include:vec!["/payload".into()]
        }));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let cached = tenant.core.recovery_catalog.load(Ordering::Acquire);
            let catalog = tenant.engine().catalog_seq();
            let active = !tenant.core.builds.lock().is_empty();
            let building = tenant.indexes("docs").unwrap().iter().any(|i| i.state == IndexState::Building);
            if cached == catalog && active && building { break; }
            assert!(!build.is_finished() && Instant::now() < deadline, "probe precondition: background never observed active build");
            thread::sleep(Duration::from_millis(1));
        }
        let management = store.close_tenant(id).unwrap_err();
        assert_eq!(management.code,"BUSY");
        assert_eq!(build.join().unwrap().unwrap_err().code,"BUSY");
        let observing = Instant::now();
        let mut inspections = 0;
        while observing.elapsed() < Duration::from_secs(5) {
            inspections += 1;
            if tenant.indexes("docs").unwrap()[0].state == IndexState::Ready { break; }
            thread::sleep(Duration::from_millis(50));
        }
        let stranded = tenant.indexes("docs").unwrap().remove(0);
        let active = tenant.core.builds.lock().len();
        let cached = tenant.core.recovery_catalog.load(Ordering::Acquire);
        let catalog = tenant.engine().catalog_seq();
        let observed_ms = observing.elapsed().as_millis();
        drop(tenant);
        store.close_tenant(id).unwrap();
        let reopened = store.tenant(id).unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let index = reopened.indexes("docs").unwrap().remove(0);
            if index.state == IndexState::Ready { assert_eq!(index.entries,16384); break; }
            assert_eq!(index.state,IndexState::Building);
            assert!(Instant::now() < deadline,"reopen did not restore automatic recovery");
            thread::sleep(Duration::from_millis(20));
        }
        let mut checked = 0;
        for (batch,acks) in keys.chunks(128).zip(acknowledged.chunks(128)) {
            for (doc,ack) in reopened.multi_get("docs",batch).unwrap().into_iter().zip(acks) {
                let doc=doc.unwrap(); assert_eq!(doc["_rev"],ack.rev); assert_eq!(doc["payload"],payload); checked+=1;
            }
        }
        println!("AUDIT_JSON {}",json!({"case":"automatic-failed-drain-index","automatic_workers":true,"manual_recovery_calls":0,"observed_for_ms":observed_ms,"inspections":inspections,"state_before_reopen":stranded.state,"active_builds":active,"recovery_catalog":cached,"catalog_seq":catalog,"reopen_control_state":"READY","acknowledged_documents_checked":checked,"acknowledged_mismatches":0}));
        assert_eq!(stranded.state,IndexState::Ready,"normal background worker stranded interrupted index after failed drain");
    }
