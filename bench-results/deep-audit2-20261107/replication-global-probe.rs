
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_global_wire_budget_blocks_same_quota_replay() {
    for global_mib in [64,128] {
    let pair = Pair::with_options(Options {
        global_memory_bytes: global_mib << 20, tenant_memory_bytes: 40 << 20,
        cache_bytes: 64 << 10, memtable_bytes: 1 << 20, ..options()
    }).await;
    for (store, role) in [(&pair.a, Role::Primary), (&pair.b, Role::Replica)] {
        store.configure_replication(&Config {
            cluster: store.replication_state().cluster, role,
            peer_url: "http://127.0.0.1:1".into(), token_file: "unused".into(),
            poll_ms: 10, log_retention_bytes: 64 << 20,
        }).unwrap();
    }
    let id = pair.a.create_tenant("global-wire-budget").unwrap().id;
    let source = pair.a.tenant(id).unwrap();
    source.create_collection("docs").unwrap();
    for n in 0..16 {
        source.create_index("docs", IndexSpec { name: format!("i{n}"),
            fields: vec![IndexField {field:"/n".into(),order:Order::Asc}],
            unique:false,include:vec!["/payload".into()] }).unwrap();
    }
    pair.catch_up(false).await;
    let before = pair.b.tenant(id).unwrap().engine().seq();
    let response = reqwest::Client::new()
        .put(format!("{}/v1/tenants/{id}/collections/docs/documents", pair.ua))
        .bearer_auth(ADMIN)
        .json(&json!({"document":{"_key":"large","n":1,"payload":"x".repeat(512<<10)}}))
        .send().await.unwrap();
    let status = response.status();
    let body = response.bytes().await.unwrap();
    assert!(status.is_success(), "source rejected: {}", String::from_utf8_lossy(&body));
    let ack: WriteResult = serde_json::from_slice(&body).unwrap();
    source.engine().flush().unwrap();
    let state=pair.a.replication_state();
    let direct = reqwest::Client::new().post(format!("{}/v1/replication/log",pair.ua))
        .bearer_auth(PEER).json(&json!({"protocol":tenant_kv::replication::PROTOCOL,
        "cluster":state.cluster,"epoch":state.epoch,"tenant":id,
        "generation":source.info().generation,"after":before})).send().await.unwrap();
    let status=direct.status();let wire=direct.bytes().await.unwrap();
    assert!(status.is_success(),"direct source log export failed: {}",String::from_utf8_lossy(&wire));
    println!("AUDIT global={global_mib}MiB source_log_HTTP={status} wire_bytes={} seven_times_wire={} receiver_used_before={}",wire.len(),wire.len()*7,pair.b.memory_bytes());
    drop(wire);
    let mut errors = Vec::new();
    for _ in 0..6 {
        if let Err(error) = pair.sb.sync_once().await { errors.push(error.to_string()); }
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    let seq = pair.b.tenant(id).unwrap().engine().seq();
    let reinitializations = pair.sb.status().unwrap().reinitializations;
    println!("AUDIT global={global_mib}MiB tenant=40MiB indexes=16 HTTP_ack={} before={before} replica={seq} reinitializations={reinitializations} errors={errors:?}", ack.rev);
    if global_mib==64 {
        assert_eq!(seq,before);
        assert!(ack.rev>seq && errors.len()>=4);
        assert_eq!(reinitializations,0);
    } else {
        pair.catch_up(false).await;
        let replica=pair.b.tenant(id).unwrap();
        assert_eq!(replica.engine().seq(),ack.rev);
        assert_eq!(replica.get("docs","large").unwrap(),source.get("docs","large").unwrap());
        println!("AUDIT 128MiB global budget: content and revision equal without changing tenant quota");
    }
    let snapshot = pair.sa.snapshot(pair.a.replication_state().cluster,pair.a.replication_state().epoch,id).unwrap();
    println!("AUDIT already-flushed checkpoint bytes={} seq={}",snapshot.files.iter().map(|f|f.size).sum::<u64>(),snapshot.position);
    pair.sa.release_snapshot(snapshot.id).unwrap();
    drop(source);
    stop_test_pair(pair).await;
    }
}
