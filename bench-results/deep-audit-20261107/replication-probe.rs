#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_index_expansion_acknowledges_a_frame_the_equal_quota_replica_cannot_apply() {
    let pair = Pair::with_options(Options {
        global_memory_bytes: 256 << 20,
        tenant_memory_bytes: 19 << 20,
        cache_bytes: 64 << 10,
        memtable_bytes: 1 << 20,
        ..options()
    }).await;
    for (store, role) in [(&pair.a, Role::Primary), (&pair.b, Role::Replica)] {
        store.configure_replication(&Config {
            cluster: store.replication_state().cluster,
            role,
            peer_url: "http://127.0.0.1:1".into(),
            token_file: std::path::PathBuf::from("unused"),
            poll_ms: 10,
            log_retention_bytes: 64 << 20,
        }).unwrap();
    }
    let id = pair.a.create_tenant("audit-index-expansion").unwrap().id;
    let source = pair.a.tenant(id).unwrap();
    source.create_collection("docs").unwrap();
    pair.catch_up(false).await;
    for number in 0..16 {
        source.create_index("docs", IndexSpec {
            name: format!("i{number}"),
            fields: vec![IndexField { field: "/n".into(), order: Order::Asc }],
            unique: false,
            include: vec!["/payload".into()],
        }).unwrap();
    }
    pair.catch_up(false).await;
    let response = reqwest::Client::new()
        .put(format!("{}/v1/tenants/{id}/collections/docs/documents", pair.ua))
        .bearer_auth(ADMIN)
        .json(&json!({"document":{"_key":"large", "n":1, "payload":"x".repeat(256<<10)}}))
        .send().await.unwrap();
    let status = response.status();
    let body = response.bytes().await.unwrap();
    assert!(status.is_success(), "HTTP source rejected: {}", String::from_utf8_lossy(&body));
    let acknowledged: WriteResult = serde_json::from_slice(&body).unwrap();
    source.engine().flush().unwrap();
    let mut failures = Vec::new();
    for _ in 0..4 {
        failures.push(pair.sb.sync_once().await.unwrap_err().code);
        tokio::time::sleep(Duration::from_millis(110)).await;
    }
    assert!(failures.iter().all(|code| code == "RESOURCE_EXHAUSTED"));
    let target = pair.b.tenant(id).unwrap();
    assert!(target.engine().seq() < acknowledged.rev);
    assert!(target.get("docs", "large").unwrap().is_none());
    println!("AUDIT equal quota=19MiB, 16 covering indexes, acknowledged seq={}, replica seq={}, failures={failures:?}, reinitializations={}", acknowledged.rev, target.engine().seq(), pair.sb.status().unwrap().reinitializations);
    let disk = source.info().disk_bytes;
    drop(target);
    drop(source);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match pair.a.set_quota(id, 32 << 20, disk) {
            Err(error) if error.code == "BUSY" && Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(10)).await,
            result => { result.unwrap(); break; }
        }
    }
    pair.catch_up(false).await;
    assert_eq!(pair.b.tenant(id).unwrap().get("docs", "large").unwrap(), pair.a.tenant(id).unwrap().get("docs", "large").unwrap());
    println!("AUDIT raising quota to 32MiB resumes replication to seq={}", acknowledged.rev);
    audit_finish(pair).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_valid_options_are_rejected_by_replica_catalog_quota_validation() {
    let settings = Options { global_memory_bytes: 64 << 20, tenant_memory_bytes: 3 << 20, memtable_bytes: 1 << 20, cache_bytes: 0, ..options() };
    settings.validate().unwrap();
    let pair = Pair::with_options(settings).await;
    let id = pair.a.create_tenant("audit-quota-validation").unwrap().id;
    let source = pair.a.tenant(id).unwrap();
    source.create_collection("docs").unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !pair.b.tenants().iter().any(|t| t.id == id) {
        let _ = pair.sb.sync_once().await;
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let ack = source.put("docs", json!({"_key":"small", "n":1}), None).unwrap();
    for _ in 0..3 {
        let error = pair.sb.sync_once().await.unwrap_err();
        assert_eq!(error.code, "RESOURCE_EXHAUSTED");
        assert!(error.message.contains("replicated quota is incompatible"));
    }
    let replica = pair.b.tenant(id).unwrap();
    assert!(replica.engine().seq() < ack.rev);
    println!("AUDIT Options.validate accepted tenant=3MiB/memtable=1MiB, source ACK={}, replica={}, catalog rejects unchanged quota on every cycle", ack.rev, replica.engine().seq());
    drop(replica);
    drop(source);
    audit_finish(pair).await;
}

async fn audit_finish(mut pair: Pair) {
    pair.sa.stop_initializations().await;
    pair.sb.stop_initializations().await;
    for server in pair.servers.drain(..) {
        server.abort();
        let _ = server.await;
    }
}
