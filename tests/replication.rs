use serde_json::json;
use std::{
    path::Path,
    time::{Duration, Instant},
};
use tenant_kv::{
    http::{self, AuthConfig, Role as HttpRole, Token},
    replication::{Config, Role, Service},
    *,
};
use uuid::Uuid;

const PEER: &str = "replication-only-test-secret-12345";
const ADMIN: &str = "replication-admin-test-secret-1234";
fn options() -> Options {
    Options {
        memtable_bytes: 64 << 10,
        target_sst_bytes: 128 << 10,
        ..Options::default()
    }
}
fn config(path: &Path, cluster: Uuid, role: Role, peer_url: String) -> Config {
    let token = path.join("peer-token");
    std::fs::write(&token, PEER).unwrap();
    std::fs::set_permissions(&token, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
    Config {
        cluster,
        role,
        peer_url,
        token_file: token,
        poll_ms: 10,
        log_retention_bytes: 1 << 20,
    }
}
fn auth() -> AuthConfig {
    AuthConfig {
        tokens: vec![
            Token {
                subject: "test-service".into(),
                sha256: http::token_hash(PEER),
                tenant: None,
                role: HttpRole::Replication,
            },
            Token {
                subject: "test-service".into(),
                sha256: http::token_hash(ADMIN),
                tenant: None,
                role: HttpRole::Admin,
            },
        ],
    }
}
struct Pair {
    a: Store,
    b: Store,
    sa: Service,
    sb: Service,
    ua: String,
    ub: String,
    servers: Vec<tokio::task::JoinHandle<()>>,
    _dirs: Vec<tempfile::TempDir>,
}
#[derive(Default)]
struct SnapshotGate {
    fail_logs: std::sync::atomic::AtomicUsize,
    failed_logs: std::sync::atomic::AtomicUsize,
    contexts: std::sync::atomic::AtomicUsize,
    snapshots: std::sync::atomic::AtomicUsize,
    blocked: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl Drop for Pair {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
    }
}
impl Pair {
    async fn new() -> Self {
        Self::with_options(options()).await
    }
    async fn with_options(options: Options) -> Self {
        Self::with_replica_role(options, Role::Replica).await
    }
    async fn with_replica_role(options: Options, replica_role: Role) -> Self {
        Self::with_snapshot_gate(options, replica_role, None).await
    }
    async fn with_snapshot_gate(
        options: Options,
        replica_role: Role,
        gate: Option<std::sync::Arc<SnapshotGate>>,
    ) -> Self {
        Self::with_store_options(options.clone(), options, replica_role, gate).await
    }
    async fn with_store_options(
        primary_options: Options,
        replica_options: Options,
        replica_role: Role,
        gate: Option<std::sync::Arc<SnapshotGate>>,
    ) -> Self {
        let da = tempfile::tempdir().unwrap();
        let db = tempfile::tempdir().unwrap();
        let a = Store::open(da.path(), primary_options).unwrap();
        let b = Store::open(db.path(), replica_options).unwrap();
        let la = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let lb = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ua = format!("http://{}", la.local_addr().unwrap());
        let ub = format!("http://{}", lb.local_addr().unwrap());
        let cluster = Uuid::new_v4();
        let sa = Service::open(
            a.clone(),
            config(da.path(), cluster, Role::Primary, ub.clone()),
        )
        .unwrap();
        let sb = Service::open(
            b.clone(),
            config(db.path(), cluster, replica_role, ua.clone()),
        )
        .unwrap();
        let mut appa =
            http::app_with_replication(a.clone(), auth(), None, Some(sa.clone())).unwrap();
        if let Some(gate) = gate {
            appa = appa.layer(axum::middleware::from_fn(
                move |request: axum::extract::Request, next: axum::middleware::Next| {
                    let gate = gate.clone();
                    async move {
                        if request.uri().path() == "/v1/replication/log"
                            && gate
                                .fail_logs
                                .try_update(
                                    std::sync::atomic::Ordering::AcqRel,
                                    std::sync::atomic::Ordering::Acquire,
                                    |n| n.checked_sub(1),
                                )
                                .is_ok()
                        {
                            gate.failed_logs
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            return axum::response::IntoResponse::into_response((
                                axum::http::StatusCode::BAD_GATEWAY,
                                "<html>temporary gateway failure</html>",
                            ));
                        }
                        if request.uri().path().ends_with("/context") {
                            gate.contexts
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        if request.uri().path() == "/v1/replication/snapshot" {
                            gate.snapshots
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if gate.blocked.load(std::sync::atomic::Ordering::Acquire) {
                                gate.entered.notify_one();
                                gate.release.notified().await;
                            }
                        }
                        next.run(request).await
                    }
                },
            ));
        }
        let appb = http::app_with_replication(b.clone(), auth(), None, Some(sb.clone())).unwrap();
        let servers = vec![
            tokio::spawn(async move {
                axum::serve(la, appa).await.unwrap();
            }),
            tokio::spawn(async move {
                axum::serve(lb, appb).await.unwrap();
            }),
        ];
        Self {
            a,
            b,
            sa,
            sb,
            ua,
            ub,
            servers,
            _dirs: vec![da, db],
        }
    }
    async fn catch_up(&self, reverse: bool) {
        let (source, replica, service) = if reverse {
            (&self.b, &self.a, &self.sa)
        } else {
            (&self.a, &self.b, &self.sb)
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match service.sync_once().await {
                Ok(()) => {}
                Err(e) if e.code == "BUSY" || e.code == "REPLICATION_CONFLICT" => {
                    assert!(Instant::now() < deadline, "{e}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                Err(e) => panic!(
                    "{e}; replica status: {}",
                    serde_json::to_string(&service.status().unwrap()).unwrap()
                ),
            }
            let a = source.replication_positions().unwrap();
            let b = replica.replication_positions().unwrap();
            if a.len() == b.len()
                && a.iter()
                    .zip(&b)
                    .all(|(a, b)| a.info.id == b.info.id && a.seq == b.seq)
            {
                return;
            }
            assert!(Instant::now() < deadline, "replica did not catch up");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn log_negotiation_preserves_wal_frames_and_cursor_across_encodings() {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use tenant_kv::replication::{LogCursor, LogPage, LogRequest, PROTOCOL};

    let pair = Pair::with_options(Options::default()).await;
    let info = pair.a.create_tenant("binary-page").unwrap();
    let tenant = pair.a.tenant(info.id).unwrap();
    let after = tenant.engine().seq();
    for key in ["first", "second"] {
        tenant
            .engine()
            .transact(|tx| tx.put(key.as_bytes().to_vec(), vec![42; 1 << 20]))
            .unwrap();
    }
    let state = pair.a.replication_state();
    let request = LogRequest {
        protocol: PROTOCOL,
        cluster: state.cluster,
        epoch: state.epoch,
        tenant: info.id,
        generation: info.generation,
        after,
        cursor: None,
    };
    let http = reqwest::Client::new();
    let fetch = |request: &LogRequest| {
        http.post(format!("{}/v1/replication/log", pair.ua))
            .bearer_auth(PEER)
            .json(request)
    };
    let legacy_response = fetch(&request)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(
        legacy_response.headers()["content-type"],
        "application/json"
    );
    let legacy: LogPage = legacy_response.json().await.unwrap();
    assert_eq!(legacy.through, after + 1);
    assert_eq!(legacy.source_position, after + 2);
    let media = "application/vnd.idkv.wal-page.v1";
    let response = fetch(&request)
        .header("accept", format!("{media};q=1, application/json;q=0.5"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(response.headers()["content-type"], media);
    assert_eq!(response.headers()["vary"], "Accept");
    let body = response.bytes().await.unwrap();
    assert_eq!(&body[..16], state.epoch.as_bytes());
    assert_eq!(
        u64::from_le_bytes(body[16..24].try_into().unwrap()),
        legacy.through
    );
    assert_eq!(
        u64::from_le_bytes(body[24..32].try_into().unwrap()),
        legacy.source_position
    );
    assert_eq!(body[32], 1);
    assert_eq!(&body[57..], STANDARD.decode(&legacy.frames[0]).unwrap());
    let cursor = LogCursor {
        file: Uuid::from_bytes(body[33..49].try_into().unwrap()),
        offset: u64::from_le_bytes(body[49..57].try_into().unwrap()),
    };
    let next: LogPage = fetch(&LogRequest {
        after: legacy.through,
        cursor: Some(cursor),
        ..request
    })
    .send()
    .await
    .unwrap()
    .error_for_status()
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(next.through, legacy.source_position);
    assert_eq!(next.frames.len(), 1);

    let caught_up = LogRequest {
        after: legacy.source_position,
        ..request
    };
    for (accept, expected) in [
        (format!("{media};q=0"), "application/json"),
        (
            format!("{media};q=0.2, application/json;q=0.8"),
            "application/json",
        ),
        (
            format!("application/json;charset=utf-8, {media};q=0.5"),
            "application/json",
        ),
        (
            format!("application/json;q=0.2;charset=\"UTF-8\", {media};q=0.5"),
            media,
        ),
        (
            format!("application/json;note=\"x;q=0\", {media};q=0.5"),
            "application/json",
        ),
        (
            format!("{media};q=0.5, */*;q=1, application/json;q=0"),
            media,
        ),
        (
            format!("application/json;note=\"ignored,{media},ignored\", */*"),
            "application/json",
        ),
        ("application/*, */*;q=0.5".to_string(), "application/json"),
    ] {
        let response = fetch(&caught_up)
            .header("accept", &accept)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        assert_eq!(response.headers()["content-type"], expected, "{accept}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_flight_binary_log_frame_keeps_global_charge_without_blocking_tenant_writes() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tenant_kv::replication::{LogRequest, PROTOCOL};
    use tower::ServiceExt;

    const FRAME_BYTES: usize = 8 << 20;
    let pair = Pair::with_options(Options {
        tenant_memory_bytes: 44 << 20,
        memtable_bytes: 20 << 20,
        cache_bytes: 1 << 20,
        ..Options::default()
    })
    .await;
    let info = pair.a.create_tenant("slow-binary-log").unwrap();
    let tenant = pair.a.tenant(info.id).unwrap();
    let state = pair.a.replication_state();
    let request = serde_json::to_vec(&LogRequest {
        protocol: PROTOCOL,
        cluster: state.cluster,
        epoch: state.epoch,
        tenant: info.id,
        generation: info.generation,
        after: tenant.engine().seq(),
        cursor: None,
    })
    .unwrap();
    tenant
        .engine()
        .transact(|tx| tx.put(b"large".to_vec(), vec![1; FRAME_BYTES]))
        .unwrap();
    let non_tenant_memory = || {
        pair.a
            .memory_bytes()
            .saturating_sub(tenant.engine().memory_bytes())
    };
    let before = non_tenant_memory();
    let response = http::app_with_replication(pair.a.clone(), auth(), None, Some(pair.sa.clone()))
        .unwrap()
        .oneshot(
            Request::post("/v1/replication/log")
                .header("authorization", format!("Bearer {PEER}"))
                .header("accept", "application/vnd.idkv.wal-page.v1")
                .header("content-type", "application/json")
                .body(Body::from(request))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        "application/vnd.idkv.wal-page.v1"
    );
    let mut body = response.into_body();
    drop(body.frame().await.unwrap().unwrap()); // Page header.
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert!(frame.len() > FRAME_BYTES);
    drop(body);
    let in_flight_memory = non_tenant_memory();

    // Hold the transport's frame independently of socket buffers or the body.
    // The write needs ~16 MiB already in the memtable plus ~12 MiB each for
    // staging and commit preparation. Charging the frame to the tenant would
    // raise the peak above the 44 MiB quota.
    tenant
        .engine()
        .transact(|tx| tx.put(b"foreground".to_vec(), vec![2; 6 << 20]))
        .unwrap();
    assert!(in_flight_memory >= before + FRAME_BYTES);
    drop(frame);
    let deadline = Instant::now() + Duration::from_secs(5);
    while non_tenant_memory() > before + (1 << 20) {
        assert!(
            Instant::now() < deadline,
            "dropped send still owns the frame"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_atomic_indexes_deletes_and_switches_both_directions() {
    let pair = Pair::new().await;
    let id = pair.a.create_tenant("assets").unwrap().id;
    let tenant = pair.a.tenant(id).unwrap();
    tenant.create_collection("assets").unwrap();
    tenant
        .put(
            "assets",
            json!({"_key":"a","ip":"ip-a","owner":"one"}),
            None,
        )
        .unwrap();
    tenant
        .create_index(
            "assets",
            serde_json::from_value(
                json!({"name":"ip","fields":[{"field":"/ip"}],"unique":true,"include":["/owner"]}),
            )
            .unwrap(),
        )
        .unwrap();
    tenant
        .create_lookup(
            "assets",
            serde_json::from_value(json!({"name":"owner","index":"ip","select":["/owner"]}))
                .unwrap(),
        )
        .unwrap();
    pair.catch_up(false).await;
    let replica = pair.b.tenant(id).unwrap();
    assert_eq!(
        replica
            .put("assets", json!({"_key":"bad"}), None)
            .unwrap_err()
            .code,
        "READ_ONLY"
    );
    assert_eq!(pair.b.create_tenant("bad").unwrap_err().code, "READ_ONLY");
    for round in 0..20 {
        tenant
            .write_batch(
                "assets",
                vec![
                    WriteOp::Put {
                        document: json!({"_key":"a","ip":"ip-a","owner":round}),
                        if_rev: None,
                    },
                    WriteOp::Put {
                        document: json!({"_key":"b","ip":"ip-b","owner":round}),
                        if_rev: None,
                    },
                ],
            )
            .unwrap();
    }
    tenant.engine().flush().unwrap();
    pair.catch_up(false).await;
    let rows = replica
        .multi_get("assets", &["a".into(), "b".into()])
        .unwrap();
    assert_eq!(
        rows[0].as_ref().unwrap()["_rev"],
        rows[1].as_ref().unwrap()["_rev"]
    );
    assert_eq!(
        replica
            .lookup_many("assets", "owner", &[json!({"ip":"ip-a"})])
            .unwrap()
            .matches[0][0]["owner"],
        19
    );
    tenant.delete("assets", "b", None).unwrap();
    pair.catch_up(false).await;
    assert!(replica.get("assets", "b").unwrap().is_none());
    let old_epoch = pair.a.replication_state().epoch;
    tenant
        .put(
            "assets",
            json!({"_key":"cursor-row","ip":"cursor-ip"}),
            None,
        )
        .unwrap();
    pair.catch_up(false).await;
    let old_cursor = replica
        .query(
            "assets",
            serde_json::from_value(json!({"limit":1})).unwrap(),
        )
        .unwrap()
        .cursor
        .unwrap();
    pair.sa.switchover().await.unwrap();
    assert_eq!(
        replica
            .query(
                "assets",
                serde_json::from_value(json!({"limit":1,"cursor":old_cursor})).unwrap()
            )
            .unwrap_err()
            .code,
        "CURSOR_EXPIRED"
    );
    assert_eq!(pair.a.replication_state().role, Role::Replica);
    assert_eq!(pair.b.replication_state().role, Role::Primary);
    assert_ne!(pair.b.replication_state().epoch, old_epoch);
    assert_eq!(
        tenant
            .put("assets", json!({"_key":"bad"}), None)
            .unwrap_err()
            .code,
        "READ_ONLY"
    );
    assert_eq!(
        replica
            .put("assets", json!({"_key":"c","ip":"ip-a"}), None)
            .unwrap_err()
            .code,
        "UNIQUE_CONFLICT"
    );
    replica
        .put("assets", json!({"_key":"c","ip":"ip-c"}), None)
        .unwrap();
    pair.catch_up(true).await;
    assert!(tenant.get("assets", "c").unwrap().is_some());
    pair.sb.switchover().await.unwrap();
    tenant
        .put("assets", json!({"_key":"d","ip":"ip-d"}), None)
        .unwrap();
    pair.catch_up(false).await;
    assert!(replica.get("assets", "d").unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_short_readers_and_cached_cursors_cannot_starve_checkpoint_installation() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    let pair = Pair::new().await;
    let mut ids = Vec::new();
    for name in ["hot-a", "hot-b"] {
        let id = pair.a.create_tenant(name).unwrap().id;
        let tenant = pair.a.tenant(id).unwrap();
        tenant.create_collection("docs").unwrap();
        tenant
            .put("docs", json!({"_key":"hot","value":-1}), None)
            .unwrap();
        tenant.put("docs", json!({"_key":"other"}), None).unwrap();
        ids.push(id);
    }
    pair.catch_up(false).await;
    let cursor = pair
        .b
        .tenant(ids[0])
        .unwrap()
        .query("docs", serde_json::from_value(json!({"limit":1})).unwrap())
        .unwrap()
        .cursor
        .unwrap();
    let running = Arc::new(AtomicBool::new(true));
    let busy = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();
    for offset in [0, 10] {
        let store = pair.b.clone();
        let running = running.clone();
        let busy = busy.clone();
        let calls = calls.clone();
        let ids = ids.clone();
        threads.push(std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(offset));
            while running.load(Ordering::Acquire) {
                let mut handles = Vec::new();
                for id in &ids {
                    match store.tenant(*id) {
                        Ok(handle) => {
                            calls.fetch_add(1, Ordering::Relaxed);
                            handles.push(handle);
                        }
                        Err(error) if error.code == "BUSY" => {
                            busy.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(error) => panic!("{error}"),
                    }
                }
                std::thread::sleep(Duration::from_millis(20));
                drop(handles);
            }
        }));
    }
    for id in &ids {
        let tenant = pair.a.tenant(*id).unwrap();
        for round in 0..16 {
            tenant
                .put(
                    "docs",
                    json!({"_key":"hot","value":round,"payload":"x".repeat(128<<10)}),
                    None,
                )
                .unwrap();
            tenant.engine().flush().unwrap();
            tenant.engine().compact(true).unwrap();
        }
    }
    let deadline = Instant::now() + Duration::from_secs(8);
    let caught_up = loop {
        let result = pair.sb.sync_once().await;
        if let Err(error) = result {
            assert_eq!(error.code, "BUSY");
        }
        let expected = pair.a.replication_positions().unwrap();
        let actual = pair.b.replication_positions();
        if actual.as_ref().is_ok_and(|actual| {
            actual.len() == expected.len()
                && actual
                    .iter()
                    .zip(&expected)
                    .all(|(a, b)| a.info.id == b.info.id && a.seq == b.seq)
        }) {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    running.store(false, Ordering::Release);
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(
        caught_up,
        "overlapping short readers prevented installation"
    );
    assert!(calls.load(Ordering::Relaxed) > 0 && busy.load(Ordering::Relaxed) > 0);
    for id in &ids {
        assert_eq!(
            pair.b.tenant(*id).unwrap().get("docs", "hot").unwrap(),
            pair.a.tenant(*id).unwrap().get("docs", "hot").unwrap()
        );
    }
    let old_page = pair.b.tenant(ids[0]).unwrap().query(
        "docs",
        serde_json::from_value(json!({"limit":1,"cursor":cursor})).unwrap(),
    );
    assert_eq!(old_page.err().unwrap().code, "CURSOR_EXPIRED");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_install_waits_for_readers_without_retransferring_and_other_tenants_progress() {
    use std::sync::atomic::Ordering;
    let gate = std::sync::Arc::new(SnapshotGate::default());
    let pair = Pair::with_snapshot_gate(options(), Role::Replica, Some(gate.clone())).await;
    let id = pair.a.create_tenant("reader-race").unwrap().id;
    let tenant = pair.a.tenant(id).unwrap();
    tenant.create_collection("docs").unwrap();
    pair.catch_up(false).await;
    let baseline = gate.snapshots.load(Ordering::Relaxed);
    let old_reader = pair.b.tenant(id).unwrap();
    for round in 0..16 {
        tenant
            .put(
                "docs",
                json!({"_key":"hot","value":round,"payload":"x".repeat(128<<10)}),
                None,
            )
            .unwrap();
        tenant.engine().flush().unwrap();
        tenant.engine().compact(true).unwrap();
    }
    let until = Instant::now() + Duration::from_secs(1);
    while Instant::now() < until {
        let result = pair.sb.sync_once().await;
        assert!(result.is_ok() || result.err().unwrap().code == "BUSY");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        gate.snapshots.load(Ordering::Relaxed),
        baseline,
        "downloaded before checking active readers"
    );
    assert!(
        pair.sb.status().unwrap().initialization_attempts < 8,
        "initialization had no backoff"
    );
    drop(old_reader);
    gate.blocked.store(true, Ordering::Release);
    let deadline = Instant::now() + Duration::from_secs(5);
    while gate.snapshots.load(Ordering::Relaxed) == baseline {
        let result = pair.sb.sync_once().await;
        assert!(result.is_ok() || result.err().unwrap().code == "BUSY");
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    gate.entered.notified().await;
    let raced_reader = pair.b.tenant(id).unwrap();
    gate.blocked.store(false, Ordering::Release);
    gate.release.notify_waiters();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !pair.sb.status().unwrap().waiting_for_readers.contains(&id) {
        pair.sb.sync_once().await.unwrap();
        assert!(
            Instant::now() < deadline,
            "did not retain the downloaded checkpoint"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let downloads = gate.snapshots.load(Ordering::Relaxed);
    let attempts = pair.sb.status().unwrap().initialization_attempts;
    let healthy = pair.a.create_tenant("unblocked-initialization").unwrap().id;
    pair.a
        .tenant(healthy)
        .unwrap()
        .create_collection("docs")
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        pair.sb.sync_once().await.unwrap();
        if pair.b.tenants().iter().any(|t| t.id == healthy) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "waiting reader blocked another tenant initialization"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(gate.snapshots.load(Ordering::Relaxed), downloads + 1);
    assert_eq!(
        pair.sb.status().unwrap().initialization_attempts,
        attempts + 1
    );
    assert!(pair.sb.status().unwrap().waiting_for_readers.contains(&id));
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        pair.sb.sync_once().await.unwrap();
        if pair
            .sb
            .status()
            .unwrap()
            .initialization_errors
            .contains_key(&id)
        {
            break;
        }
        assert!(Instant::now() < deadline, "reader drain never timed out");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(pair.sb.status().unwrap().initialization_errors[&id].contains("reader drain"));
    gate.fail_logs.store(1, Ordering::Release);
    assert!(raced_reader.get("docs", "hot").unwrap().is_none());
    assert!(
        pair.b
            .tenant(id)
            .unwrap()
            .get("docs", "hot")
            .unwrap()
            .is_none()
    );
    drop(raced_reader);
    tokio::time::sleep(Duration::from_secs(31)).await;
    pair.catch_up(false).await;
    assert_eq!(
        gate.snapshots.load(Ordering::Relaxed),
        downloads + 1,
        "redownloaded verified checkpoint after drain timeout"
    );
    assert_eq!(
        pair.b.tenant(id).unwrap().get("docs", "hot").unwrap(),
        tenant.get("docs", "hot").unwrap()
    );
    assert!(!pair.sb.status().unwrap().waiting_for_readers.contains(&id));
    assert_eq!(gate.failed_logs.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_checkpoint_initialization_does_not_hold_healthy_replication_cycles() {
    let gate = std::sync::Arc::new(SnapshotGate::default());
    let pair = Pair::with_snapshot_gate(options(), Role::Replica, Some(gate.clone())).await;
    let good = pair.a.create_tenant("incremental").unwrap().id;
    pair.a
        .tenant(good)
        .unwrap()
        .create_collection("docs")
        .unwrap();
    pair.catch_up(false).await;
    gate.blocked
        .store(true, std::sync::atomic::Ordering::Release);
    let slow = pair.a.create_tenant("slow-checkpoint").unwrap().id;
    pair.a
        .tenant(slow)
        .unwrap()
        .create_collection("docs")
        .unwrap();
    let cycle = tokio::time::timeout(Duration::from_secs(2), pair.sb.sync_once()).await;
    let entered = tokio::time::timeout(Duration::from_secs(2), gate.entered.notified()).await;
    pair.a
        .tenant(good)
        .unwrap()
        .put("docs", json!({"_key":"ack","value":42}), None)
        .unwrap();
    let healthy = tokio::time::timeout(Duration::from_secs(2), pair.sb.sync_once()).await;
    let observed = pair.b.tenant(good).unwrap().get("docs", "ack").unwrap();
    gate.blocked
        .store(false, std::sync::atomic::Ordering::Release);
    gate.release.notify_waiters();
    assert!(cycle.unwrap().is_ok());
    assert!(entered.is_ok());
    assert!(healthy.unwrap().is_ok());
    assert_eq!(observed.unwrap()["value"], 42);
    pair.catch_up(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cloned_clients_reuse_primary_discovery_for_sequential_writes() {
    let observed = std::sync::Arc::new(SnapshotGate::default());
    let pair = Pair::with_snapshot_gate(options(), Role::Replica, Some(observed.clone())).await;
    let id = pair.a.create_tenant("cached-writes").unwrap().id;
    pair.a
        .tenant(id)
        .unwrap()
        .create_collection("docs")
        .unwrap();
    pair.catch_up(false).await;
    let client =
        tenant_kv::client::Client::new(&[pair.ua.clone(), pair.ub.clone()], ADMIN.into(), id)
            .unwrap();
    for n in 0..5 {
        client
            .clone()
            .write_batch(
                "docs",
                vec![WriteOp::Put {
                    document: json!({"_key":n.to_string()}),
                    if_rev: None,
                }],
                WriteOptions::default(),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        observed.contexts.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    pair.catch_up(false).await;
    for n in 0..5 {
        assert!(
            pair.b
                .tenant(id)
                .unwrap()
                .get("docs", &n.to_string())
                .unwrap()
                .is_some()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_transfer_pins_wal_beyond_the_retention_byte_window() {
    let pair = Pair::with_options(Options {
        l0_stop: 128,
        ..options()
    })
    .await;
    let info = pair.a.create_tenant("pinned-transfer").unwrap();
    let tenant = pair.a.tenant(info.id).unwrap();
    let state = pair.a.replication_state();
    let snapshot = pair
        .sa
        .snapshot(state.cluster, state.epoch, info.id)
        .unwrap();
    for n in 0..20u64 {
        tenant
            .engine()
            .transact(|tx| tx.put(format!("Z{n:04}").into_bytes(), vec![42; 64 << 10]))
            .unwrap();
        tenant.engine().flush().unwrap();
    }
    let request = tenant_kv::replication::LogRequest {
        protocol: tenant_kv::replication::PROTOCOL,
        cluster: state.cluster,
        epoch: state.epoch,
        tenant: info.id,
        generation: info.generation,
        after: snapshot.position,
        cursor: None,
    };
    let page = pair.sa.export_log(&request).unwrap();
    assert!(page.through > snapshot.position);
    pair.sa.release_snapshot(snapshot.id).unwrap();
    tenant
        .engine()
        .transact(|tx| tx.put(b"Zlast".to_vec(), vec![43; 64 << 10]))
        .unwrap();
    tenant.engine().flush().unwrap();
    assert_eq!(pair.sa.export_log(&request).err().unwrap().code, "LOG_GONE");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_checkpoint_keeps_its_log_pin_beyond_twice_normal_retention() {
    let pair = Pair::with_options(Options {
        l0_stop: 128,
        tenant_disk_bytes: 64 << 20,
        ..options()
    })
    .await;
    let info = pair.a.create_tenant("large-transfer-window").unwrap();
    let tenant = pair.a.tenant(info.id).unwrap();
    let mut random = 17u64;
    for n in 0..24 {
        let data: Vec<_> = (0..256 << 10)
            .map(|_| {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                random as u8
            })
            .collect();
        tenant
            .engine()
            .transact(|tx| tx.put(format!("Zseed{n:04}").into_bytes(), data))
            .unwrap();
        tenant.engine().flush().unwrap();
    }
    let state = pair.a.replication_state();
    let snapshot = pair
        .sa
        .snapshot(state.cluster, state.epoch, info.id)
        .unwrap();
    for n in 0..48 {
        tenant
            .engine()
            .transact(|tx| tx.put(format!("Ztail{n:04}").into_bytes(), vec![n as u8; 64 << 10]))
            .unwrap();
        tenant.engine().flush().unwrap();
    }
    assert!(
        !pair
            .sa
            .snapshot_chunk(snapshot.id, 0, 0)
            .unwrap()
            .is_empty()
    );
    let mut request = tenant_kv::replication::LogRequest {
        protocol: tenant_kv::replication::PROTOCOL,
        cluster: state.cluster,
        epoch: state.epoch,
        tenant: info.id,
        generation: info.generation,
        after: snapshot.position,
        cursor: None,
    };
    while request.after < tenant.engine().seq() {
        let page = pair.sa.export_log(&request).unwrap();
        assert!(page.through > request.after);
        request.after = page.through;
        request.cursor = page.cursor;
    }
    assert_eq!(request.after, snapshot.position + 48);
    tenant.engine().verify().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_export_has_a_bounded_wal_window_and_cannot_pin_the_slot_forever() {
    let mut pair = Pair::with_options(Options {
        l0_stop: 128,
        ..options()
    })
    .await;
    let info = pair.a.create_tenant("stalled-export").unwrap();
    let other = pair.a.create_tenant("unblocked-export").unwrap();
    let tenant = pair.a.tenant(info.id).unwrap();
    let state = pair.a.replication_state();
    let snapshot = pair
        .sa
        .snapshot(state.cluster, state.epoch, info.id)
        .unwrap();
    for n in 0..36u64 {
        tenant
            .engine()
            .transact(|tx| tx.put(format!("Z{n:04}").into_bytes(), vec![n as u8; 64 << 10]))
            .unwrap();
        tenant.engine().flush().unwrap();
    }
    assert_eq!(
        pair.sa.snapshot_chunk(snapshot.id, 0, 0).unwrap_err().code,
        "SNAPSHOT_EXPIRED"
    );
    let next = pair
        .sa
        .snapshot(state.cluster, state.epoch, other.id)
        .unwrap();
    pair.servers.push(pair.sa.start());
    let obsolete = pair
        .a
        .root()
        .join("replication-work/export")
        .join(snapshot.id.to_string());
    let deadline = Instant::now() + Duration::from_secs(5);
    while obsolete.exists() {
        assert!(Instant::now() < deadline, "expired export was not cleaned");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let bytes: u64 = std::fs::read_dir(tenant.engine().path().join("wal"))
        .unwrap()
        .map(|p| p.unwrap().metadata().unwrap().len())
        .sum();
    assert!(bytes <= 2 << 20);
    let view = tenant.engine().snapshot().unwrap();
    for n in 0..36u64 {
        assert_eq!(
            view.get(format!("Z{n:04}").as_bytes(), &mut ReadStats::default())
                .unwrap()
                .unwrap()
                .as_slice(),
            vec![n as u8; 64 << 10]
        );
    }
    assert!(tenant.engine().writable());
    pair.sa.release_snapshot(next.id).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn untracked_export_hands_off_its_slot_and_advances_the_retained_log() {
    let mut pair = Pair::with_options(Options {
        l0_stop: 128,
        ..options()
    })
    .await;
    let info = pair.a.create_tenant("untracked-export").unwrap();
    let other = pair.a.create_tenant("next-export").unwrap();
    let tenant = pair.a.tenant(info.id).unwrap();
    let state = pair.a.replication_state();
    let snapshot = pair
        .sa
        .snapshot(state.cluster, state.epoch, info.id)
        .unwrap();
    let mut request = tenant_kv::replication::LogRequest {
        protocol: tenant_kv::replication::PROTOCOL,
        cluster: state.cluster,
        epoch: state.epoch,
        tenant: info.id,
        generation: info.generation,
        after: snapshot.position,
        cursor: None,
    };
    for n in 0..4u64 {
        tenant
            .engine()
            .transact(|tx| tx.put(format!("Z{n:04}").into_bytes(), vec![42; 64 << 10]))
            .unwrap();
        tenant.engine().flush().unwrap();
    }
    pair.sa.export_log(&request).unwrap();
    // No release ID is remembered by the receiver, as after a restart. Log
    // progress must free the copy/slot without waiting for complete catch-up.
    let next = pair
        .sa
        .snapshot(state.cluster, state.epoch, other.id)
        .unwrap();
    pair.servers.push(pair.sa.start());
    let obsolete = pair
        .a
        .root()
        .join("replication-work/export")
        .join(snapshot.id.to_string());
    let deadline = Instant::now() + Duration::from_secs(5);
    while obsolete.exists() {
        assert!(
            Instant::now() < deadline,
            "handed-off export was not cleaned"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for n in 4..48u64 {
        tenant
            .engine()
            .transact(|tx| tx.put(format!("Z{n:04}").into_bytes(), vec![42; 64 << 10]))
            .unwrap();
        tenant.engine().flush().unwrap();
        request.after = tenant.engine().seq() - 2;
        request.cursor = None;
        pair.sa.export_log(&request).unwrap();
    }
    let bytes: u64 = std::fs::read_dir(tenant.engine().path().join("wal"))
        .unwrap()
        .map(|p| p.unwrap().metadata().unwrap().len())
        .sum();
    assert!(
        bytes <= 2 << 20,
        "continued progress retained {bytes} bytes"
    );
    assert_eq!(
        tenant
            .engine()
            .snapshot()
            .unwrap()
            .get(b"Z0000", &mut ReadStats::default())
            .unwrap()
            .unwrap()
            .len(),
        64 << 10
    );
    pair.sa.release_snapshot(next.id).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_log_expiry_installs_checkpoint_and_replicates_catalog_deletion() {
    let pair = Pair::new().await;
    let id = pair.a.create_tenant("large").unwrap().id;
    let tenant = pair.a.tenant(id).unwrap();
    tenant.create_collection("docs").unwrap();
    pair.catch_up(false).await;
    let initial = pair.sb.status().unwrap();
    assert_eq!(initial.reinitializations, 0);
    for round in 0..16 {
        tenant
            .put(
                "docs",
                json!({"_key":"hot","value":round,"payload":"x".repeat(128<<10)}),
                None,
            )
            .unwrap();
        tenant.engine().flush().unwrap();
        tenant.engine().compact(true).unwrap();
    }
    pair.catch_up(false).await;
    assert_eq!(
        pair.b
            .tenant(id)
            .unwrap()
            .get("docs", "hot")
            .unwrap()
            .unwrap()["value"],
        15
    );
    let status = pair.sb.status().unwrap();
    assert_eq!(status.reinitializations, 1);
    assert_eq!(
        status.initialization_attempts,
        initial.initialization_attempts + 1
    );
    assert_eq!(status.initialization_failures, 0);
    assert_eq!(
        pair.b.tenant(id).unwrap().get("docs", "hot").unwrap(),
        tenant.get("docs", "hot").unwrap()
    );
    let response = reqwest::Client::new()
        .get(format!("{}/metrics", pair.ub))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let metrics = response.text().await.unwrap();
    for (name, expected) in [
        ("initialization_attempts", status.initialization_attempts),
        ("initialization_failures", 0),
        ("reinitializations", 1),
    ] {
        let metric = format!("tkv_replication_{name}_total");
        let value = metrics
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{metric} ")))
            .unwrap();
        assert_eq!(value.parse::<u64>().unwrap(), expected);
    }
    drop(tenant);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match pair.a.delete_tenant(id) {
            Ok(()) => break,
            Err(e) => {
                assert!(e.code == "BUSY" && Instant::now() < deadline, "{e}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    pair.catch_up(false).await;
    assert!(pair.b.tenants().is_empty());
    let other = pair.a.create_tenant("large").unwrap();
    assert_ne!(other.id, id);
    pair.catch_up(false).await;
    assert_eq!(pair.b.tenants()[0].id, other.id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_credentials_and_promotion_proof_are_enforced() {
    let pair = Pair::new().await;
    pair.catch_up(false).await;
    let client = reqwest::Client::new();
    for (url, token, body, expected) in [
        (
            format!("{}/v1/tenants", pair.ua),
            PEER,
            json!({"name":"bad"}),
            403,
        ),
        (
            format!("{}/v1/replication/force-promote", pair.ub),
            PEER,
            json!({}),
            403,
        ),
        (
            format!("{}/v1/replication/promote", pair.ub),
            PEER,
            json!({"fence":Uuid::new_v4()}),
            409,
        ),
        (
            format!("{}/v1/replication/force-promote", pair.ub),
            ADMIN,
            json!({"source":pair.b.replication_state().source,"old_primary_fenced":false,"accept_data_loss":true}),
            409,
        ),
    ] {
        let response = client
            .post(url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), expected);
    }
    let old_source = pair.b.replication_state().source.unwrap();
    pair.sa.fence(pair.b.replication_state().node).unwrap();
    pair.sb.force_promote(old_source, true, true).await.unwrap();
    assert_eq!(pair.b.replication_state().role, Role::Primary);
    assert!(pair.b.replication_state().forced);
    assert!(pair.sa.follow().await.is_err());
}

#[test]
fn persisted_fence_survives_restart_and_stale_initial_role_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = Uuid::new_v4();
    let mut cfg = config(
        dir.path(),
        cluster,
        Role::Primary,
        "http://127.0.0.1:1".into(),
    );
    let target = Uuid::new_v4();
    let store = Store::open(dir.path(), options()).unwrap();
    store.configure_replication(&cfg).unwrap();
    let id = store.create_tenant("one").unwrap().id;
    let handle = store.tenant(id).unwrap();
    handle.create_collection("docs").unwrap();
    let fence = store.fence_primary(target).unwrap();
    assert_eq!(
        handle
            .put("docs", json!({"_key":"bad"}), None)
            .unwrap_err()
            .code,
        "READ_ONLY"
    );
    assert_eq!(store.fence_primary(target).unwrap().id, fence.id);
    drop(handle);
    drop(store);
    let store = Store::open(dir.path(), options()).unwrap();
    cfg.log_retention_bytes = 2 << 20;
    store.configure_replication(&cfg).unwrap();
    assert_eq!(store.replication_state().role, Role::Fenced);
    assert_eq!(store.replication_state().log_retention_bytes, 2 << 20);
    assert_eq!(store.create_tenant("bad").unwrap_err().code, "READ_ONLY");
    assert_eq!(store.replication_state().fence.unwrap().id, fence.id);
}

async fn close(store: &Store, id: Uuid) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match store.close_tenant(id) {
            Ok(()) => return,
            Err(e) => {
                assert!(e.code == "BUSY" && Instant::now() < deadline, "{e}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_replication_does_not_reopen_cold_engines_and_read_tokens_wait_for_apply() {
    let pair = Pair::new().await;
    let id = pair.a.create_tenant("cold").unwrap().id;
    pair.a
        .tenant(id)
        .unwrap()
        .create_collection("docs")
        .unwrap();
    pair.catch_up(false).await;
    close(&pair.a, id).await;
    close(&pair.b, id).await;
    for _ in 0..3 {
        pair.sb.sync_once().await.unwrap();
    }
    assert_eq!(pair.a.status(id).unwrap()["open"], false);
    assert_eq!(pair.b.status(id).unwrap()["open"], false);
    let write = pair
        .a
        .tenant(id)
        .unwrap()
        .put("docs", json!({"_key":"a"}), None)
        .unwrap();
    let replica = pair.b.tenant(id).unwrap();
    let epoch = pair.a.replication_state().epoch;
    assert_eq!(
        replica
            .wait_replication(epoch, write.rev, 0)
            .unwrap_err()
            .code,
        "REPLICA_LAG"
    );
    pair.catch_up(false).await;
    assert!(replica.wait_replication(epoch, write.rev, 0).unwrap() >= write.rev);
    assert_eq!(
        replica
            .wait_replication(Uuid::new_v4(), write.rev, 0)
            .unwrap_err()
            .code,
        "REPLICATION_CONFLICT"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_corrupt_source_tenant_does_not_delete_its_replica_or_stop_healthy_tenants() {
    let pair = Pair::new().await;
    let bad = pair.a.create_tenant("bad").unwrap().id;
    let good = pair.a.create_tenant("good").unwrap().id;
    for id in [bad, good] {
        let t = pair.a.tenant(id).unwrap();
        t.create_collection("docs").unwrap();
        t.put("docs", json!({"_key":"a","value":1}), None).unwrap();
    }
    pair.catch_up(false).await;
    close(&pair.a, bad).await;
    std::fs::write(
        pair.a
            .root()
            .join("tenants")
            .join(bad.to_string())
            .join("CURRENT"),
        b"broken",
    )
    .unwrap();
    assert!(pair.a.tenant(bad).is_err());
    pair.a
        .tenant(good)
        .unwrap()
        .put("docs", json!({"_key":"a","value":2}), None)
        .unwrap();
    let error = pair.sb.sync_once().await.unwrap_err();
    assert_eq!(error.code, "UNAVAILABLE");
    assert_eq!(
        pair.b
            .tenant(good)
            .unwrap()
            .get("docs", "a")
            .unwrap()
            .unwrap()["value"],
        2
    );
    assert_eq!(
        pair.b
            .tenant(bad)
            .unwrap()
            .get("docs", "a")
            .unwrap()
            .unwrap()["value"],
        1
    );
    assert_eq!(pair.sa.status().unwrap().unavailable.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn switchover_preserves_every_acknowledged_write_racing_with_the_fence() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let pair = Pair::new().await;
    let id = pair.a.create_tenant("racing").unwrap().id;
    let tenant = pair.a.tenant(id).unwrap();
    tenant.create_collection("docs").unwrap();
    pair.catch_up(false).await;
    let count = Arc::new(AtomicUsize::new(0));
    let progress = count.clone();
    let writer = std::thread::spawn(move || {
        let mut acknowledged = Vec::new();
        for i in 0..500 {
            match tenant.put("docs", json!({"_key":format!("k-{i}"),"value":i}), None) {
                Ok(result) => {
                    acknowledged.push(result);
                    progress.fetch_add(1, Ordering::Release);
                }
                Err(error) => {
                    assert_eq!(error.code, "READ_ONLY");
                    break;
                }
            }
        }
        acknowledged
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while count.load(Ordering::Acquire) < 10 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    pair.sa.switchover().await.unwrap();
    let acknowledged = writer.join().unwrap();
    assert!(acknowledged.len() >= 10);
    let promoted = pair.b.tenant(id).unwrap();
    for write in acknowledged {
        assert_eq!(
            promoted.get("docs", &write.key).unwrap().unwrap()["_rev"],
            write.rev
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_memory_budget_supports_replication_and_switchover() {
    let pair = Pair::with_options(Options {
        global_memory_bytes: 64 << 20,
        tenant_memory_bytes: 32 << 20,
        cache_bytes: 4 << 20,
        max_batch_bytes: 1 << 20,
        max_response_bytes: 2 << 20,
        ..options()
    })
    .await;
    let id = pair.a.create_tenant("small").unwrap().id;
    let tenant = pair.a.tenant(id).unwrap();
    tenant.create_collection("docs").unwrap();
    tenant
        .put("docs", json!({"_key":"a", "value":1}), None)
        .unwrap();
    pair.catch_up(false).await;
    tenant
        .put("docs", json!({"_key":"a", "value":2}), None)
        .unwrap();
    pair.catch_up(false).await;
    pair.sa.switchover().await.unwrap();
    assert_eq!(
        pair.b
            .tenant(id)
            .unwrap()
            .get("docs", "a")
            .unwrap()
            .unwrap()["value"],
        2
    );
    assert_eq!(pair.a.replication_state().role, Role::Replica);
    assert!(pair.a.memory_bytes() < 64 << 20);
    assert!(pair.b.memory_bytes() < 64 << 20);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn busy_catalog_tenant_does_not_stall_healthy_replication() {
    let pair = Pair::new().await;
    let busy = pair.a.create_tenant("busy").unwrap().id;
    let good = pair.a.create_tenant("good").unwrap().id;
    for id in [busy, good] {
        let tenant = pair.a.tenant(id).unwrap();
        tenant.create_collection("docs").unwrap();
        tenant
            .put("docs", json!({"_key":"a", "value":1}), None)
            .unwrap();
    }
    pair.catch_up(false).await;
    let held = pair.b.tenant(busy).unwrap();
    close(&pair.a, busy).await;
    pair.a.delete_tenant(busy).unwrap();
    pair.a
        .tenant(good)
        .unwrap()
        .put("docs", json!({"_key":"a", "value":2}), None)
        .unwrap();
    assert_eq!(pair.sb.sync_once().await.unwrap_err().code, "BUSY");
    assert_eq!(
        pair.b
            .tenant(good)
            .unwrap()
            .get("docs", "a")
            .unwrap()
            .unwrap()["value"],
        2
    );
    assert_eq!(held.get("docs", "a").unwrap().unwrap()["value"], 1);
    drop(held);
    pair.catch_up(false).await;
    assert!(pair.b.tenants().iter().all(|t| t.id != busy));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incompatible_successor_is_rejected_before_fencing_primary() {
    let pair = Pair::with_replica_role(options(), Role::Primary).await;
    assert!(pair.sa.switchover().await.is_err());
    assert_eq!(pair.a.replication_state().role, Role::Primary);
    assert!(pair.a.replication_state().fence.is_none());
    pair.a.create_tenant("still-writable").unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn temporal_history_receipts_and_effective_time_survive_replication_and_switchover() {
    use tenant_kv::identity::*;
    let pair = Pair::new().await;
    let id = pair.a.create_tenant("temporal").unwrap().id;
    let primary = pair.a.tenant(id).unwrap();
    let context = IdentityContext {
        source: "connector".into(),
        actor: "replication-test".into(),
        can_correct: true,
    };
    primary
        .identity_initialize(&context, IdentityConfig::default())
        .unwrap();
    let create: IdentityWrite = serde_json::from_value(
        json!({"action":"create_entity","operation_id":"entity","attributes":{}}),
    )
    .unwrap();
    let entity = primary
        .identity_write(&context, create)
        .unwrap()
        .entity
        .unwrap()
        .entity_id;
    let identifier = Identifier {
        kind: "user".into(),
        namespace: "crm".into(),
        value: "alice".into(),
    };
    let request:IdentityWrite=serde_json::from_value(json!({"action":"bindings","operation_id":"bind","changes":[{"identifier":identifier,"expected_revision":"absent","effective":{"mode":"manual","from":"2026-01-01T00:00:00Z"},"change":{"action":"bind","entity_id":entity,"reason":"source initialization"}}]})).unwrap();
    let first = primary.identity_write(&context, request.clone()).unwrap();
    pair.catch_up(false).await;
    let replica = pair.b.tenant(id).unwrap();
    assert_eq!(
        replica
            .identity_write(&context, request.clone())
            .unwrap_err()
            .code,
        "READ_ONLY"
    );
    let correction:IdentityWrite=serde_json::from_value(json!({"action":"bindings","operation_id":"unbind","changes":[{"identifier":identifier,"expected_revision":first.timelines[0].revision,"effective":{"mode":"manual","from":"2026-02-01T00:00:00Z"},"change":{"action":"unbind","reason":"late cancellation"}}]})).unwrap();
    let corrected = primary
        .identity_write(&context, correction.clone())
        .unwrap();
    let mut query = ResolveRequest {
        identifier: identifier.clone(),
        valid_at: "2026-03-01T00:00:00Z".into(),
        known_at: Some(corrected.knowledge.version.to_string()),
    };
    assert_eq!(
        replica.identity_resolve(&query).unwrap_err().code,
        "KNOWLEDGE_UNAVAILABLE"
    );
    let batch = ResolveManyRequest {
        known_at: query.known_at.clone(),
        items: vec![
            ResolveItem {
                identifier: identifier.clone(),
                valid_at: query.valid_at.clone()
            };
            32
        ],
    };
    let remote =
        tenant_kv::client::Client::new(std::slice::from_ref(&pair.ub), ADMIN.into(), id).unwrap();
    assert_eq!(
        remote
            .identity_resolve_many(&batch, &Default::default())
            .await
            .unwrap_err()
            .code,
        "KNOWLEDGE_UNAVAILABLE"
    );
    let current = primary.identity_resolve_many(&batch).unwrap();
    let minimum = tenant_kv::client::ClientReadOptions {
        minimum: Some((&current).into()),
        read: ReadOptions {
            timeout_ms: Some(100),
            ..Default::default()
        },
    };
    assert_eq!(
        remote
            .identity_resolve_many(&batch, &minimum)
            .await
            .unwrap_err()
            .code,
        "DEADLINE_EXCEEDED"
    );
    pair.catch_up(false).await;
    assert!(replica.identity_resolve(&query).unwrap().binding.is_none());
    let history_request: SubjectHistoryRequest =
        serde_json::from_value(json!({"entity_id":entity,"limit":1})).unwrap();
    let mut history_page = remote
        .get_subject_history(&history_request, &Default::default())
        .await
        .unwrap();
    assert_eq!(history_page.events[0].operation_id, "unbind");
    let mut next = history_request.clone();
    next.cursor = history_page.cursor.take();
    assert!(next.cursor.as_ref().unwrap().starts_with("tkv1."));
    assert_eq!(
        remote
            .get_subject_history(&next, &Default::default())
            .await
            .unwrap()
            .events[0]
            .operation_id,
        "bind"
    );
    let reverse_request: SubjectIdentifiersRequest = serde_json::from_value(json!({"entity_id":entity,"valid_at":query.valid_at,"known_at":first.knowledge.version,"limit":1})).unwrap();
    let reverse = remote
        .get_subject_identifiers(&reverse_request, &Default::default())
        .await
        .unwrap();
    assert_eq!(reverse.identifiers[0].identifier, identifier);
    assert_eq!(
        replica
            .get_subject_identifiers(&reverse_request)
            .unwrap()
            .identifiers,
        primary
            .get_subject_identifiers(&reverse_request)
            .unwrap()
            .identifiers
    );
    let mut continued = reverse_request.clone();
    continued.cursor = reverse.cursor;
    assert!(
        remote
            .get_subject_identifiers(&continued, &Default::default())
            .await
            .unwrap()
            .identifiers
            .is_empty()
    );

    let replicated = remote
        .identity_resolve_many(
            &batch,
            &tenant_kv::client::ClientReadOptions {
                minimum: Some((&current).into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(replicated.results, current.results);
    assert_eq!(replicated.history, current.history);
    assert_eq!(replicated.knowledge_version, current.knowledge_version);
    assert!(
        remote
            .identity_resolve(&query, &Default::default())
            .await
            .unwrap()
            .binding
            .is_none()
    );
    query.known_at = Some(first.knowledge.version.to_string());
    assert_eq!(
        replica
            .identity_resolve(&query)
            .unwrap()
            .binding
            .unwrap()
            .entity_id,
        entity
    );
    pair.sa.switchover().await.unwrap();
    let retry = replica.identity_write(&context, correction).unwrap();
    assert_eq!(
        serde_json::to_value(retry).unwrap(),
        serde_json::to_value(corrected).unwrap()
    );
    let next:IdentityWrite=serde_json::from_value(json!({"action":"bindings","operation_id":"resume","changes":[{"identifier":identifier,"expected_revision":replica.identity_timeline(&identifier).unwrap().unwrap().revision,"change":{"action":"bind","entity_id":entity}}]})).unwrap();
    let resumed = replica.identity_write(&context, next).unwrap();
    pair.catch_up(true).await;
    assert_eq!(
        primary
            .identity_operation("resume")
            .unwrap()
            .effective_time_us,
        resumed.effective_time_us
    );
    assert_eq!(
        primary
            .identity_resolve(&query)
            .unwrap()
            .binding
            .unwrap()
            .entity_id,
        entity
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catchup_keeps_progress_while_source_head_and_other_tenants_advance() {
    let pair = Pair::with_options(Options {
        memtable_bytes: 4 << 20,
        ..options()
    })
    .await;
    let hot = pair.a.create_tenant("hot-head").unwrap().id;
    let light = pair.a.create_tenant("light-head").unwrap().id;
    for id in [hot, light] {
        pair.a
            .tenant(id)
            .unwrap()
            .create_collection("docs")
            .unwrap();
    }
    pair.catch_up(false).await;
    let source = pair.a.tenant(hot).unwrap();
    for i in 0..768 {
        source
            .put("docs", json!({"_key": format!("{i:04}"), "round": 0}), None)
            .unwrap();
    }
    let writers: Vec<_> = (0..4)
        .map(|worker| {
            let store = pair.a.clone();
            tokio::task::spawn_blocking(move || {
                for i in 0..192 {
                    store
                        .tenant(hot)
                        .unwrap()
                        .put(
                            "docs",
                            json!({"_key": format!("{:04}", i*4+worker), "round": 1}),
                            None,
                        )
                        .unwrap();
                    if i == 0 {
                        store
                            .tenant(light)
                            .unwrap()
                            .put("docs", json!({"_key": format!("{worker}")}), None)
                            .unwrap();
                    }
                }
            })
        })
        .collect();
    // A newly observed log head may be ahead of the preceding catalog response.
    // Continuing from that page must not report a false divergent-history error.
    loop {
        pair.sb.sync_once().await.unwrap();
        if writers.iter().all(|writer| writer.is_finished()) {
            break;
        }
    }
    for writer in writers {
        writer.await.unwrap();
    }
    pair.catch_up(false).await;
    let replica = pair.b.tenant(hot).unwrap();
    for i in 0..768 {
        assert_eq!(
            replica.get("docs", &format!("{i:04}")).unwrap(),
            source.get("docs", &format!("{i:04}")).unwrap()
        );
    }
    for i in 0..4 {
        assert!(
            pair.b
                .tenant(light)
                .unwrap()
                .get("docs", &format!("{i}"))
                .unwrap()
                .is_some()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn middleware_client_retries_receipts_across_switch_and_requires_read_history() {
    let pair = Pair::new().await;
    let id = pair.a.create_tenant("client").unwrap().id;
    pair.a
        .tenant(id)
        .unwrap()
        .create_collection("docs")
        .unwrap();
    pair.catch_up(false).await;
    let client =
        tenant_kv::client::Client::new(&[pair.ua.clone(), pair.ub.clone()], ADMIN.into(), id)
            .unwrap();
    let context = client.write_context().await.unwrap();
    let options = WriteOptions {
        retry_token: None,
        expected_history: Some(context.history),
        operation_id: Some("durable-client-retry".into()),
    };
    let operations = vec![WriteOp::Put {
        document: json!({"value":"receipt"}),
        if_rev: None,
    }];
    let write = client
        .write_batch("docs", operations.clone(), options.clone())
        .await
        .unwrap();
    pair.catch_up(false).await;
    assert_eq!(
        client
            .get("docs", &write[0].key, Some(&write[0]))
            .await
            .unwrap()
            .unwrap()["value"],
        "receipt"
    );
    pair.sa.switchover().await.unwrap();
    let retry = client
        .write_batch("docs", operations, options)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&retry).unwrap(),
        serde_json::to_value(&write).unwrap()
    );
    assert_eq!(
        client
            .get("docs", &write[0].key, Some(&write[0]))
            .await
            .unwrap_err()
            .code,
        "HISTORY_CONFLICT"
    );
    let options = WriteOptions {
        retry_token: None,
        expected_history: Some(client.write_context().await.unwrap().history),
        operation_id: Some("new-history".into()),
    };
    let write = client
        .write_batch(
            "docs",
            vec![WriteOp::Put {
                document: json!({"_key":"new","value":2}),
                if_rev: Some(0),
            }],
            options,
        )
        .await
        .unwrap();
    pair.catch_up(true).await;
    assert_eq!(
        client
            .get("docs", "new", Some(&write[0]))
            .await
            .unwrap()
            .unwrap()["value"],
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_replication_protocol_is_rejected_before_data_or_fencing() {
    let pair = Pair::new().await;
    let id = pair.a.create_tenant("protocol").unwrap().id;
    let context = pair.a.replication_state();
    let http = reqwest::Client::new();
    for path in ["log", "snapshot"] {
        let mut body = json!({"cluster":context.cluster,"epoch":context.epoch,"tenant":id});
        if path == "log" {
            body["generation"] = json!(pair.a.tenant(id).unwrap().info().generation);
            body["after"] = json!(0);
        }
        let response = http
            .post(format!("{}/v1/replication/{path}", pair.ua))
            .bearer_auth(PEER)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 409);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["error"]["code"],
            "INCOMPATIBLE_VERSION"
        );
    }
    let mut legacy = pair.sa.status().unwrap();
    legacy.node.protocol = 1;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let fake = axum::Router::new().route(
        "/v1/replication/status",
        axum::routing::get(move || {
            let legacy = legacy.clone();
            async move { axum::Json(legacy) }
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, fake).await.unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options()).unwrap();
    let service = Service::open(
        store.clone(),
        config(dir.path(), context.cluster, Role::Replica, endpoint),
    )
    .unwrap();
    assert_eq!(
        service.sync_once().await.unwrap_err().code,
        "INCOMPATIBLE_VERSION"
    );
    assert!(store.tenants().is_empty());
    assert!(pair.a.writable());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_replicates_expiry_and_current_state_across_switchover() {
    let pair = Pair::new().await;
    let id = pair.a.create_tenant("retention").unwrap().id;
    let primary = pair.a.tenant(id).unwrap();
    primary.create_collection("items").unwrap();
    primary.set_retention(1).unwrap();
    let c = primary.retry_context("worker").unwrap();
    let opts = WriteOptions {
        expected_history: Some(c.history),
        operation_id: Some("once".into()),
        retry_token: Some(c.retry_token),
    };
    let ops = vec![WriteOp::Put {
        document: json!({"value":1}),
        if_rev: None,
    }];
    let first = primary
        .write_batch_with("items", ops.clone(), &opts, "worker")
        .unwrap();
    pair.catch_up(false).await;
    let replica = pair.b.tenant(id).unwrap();
    assert_eq!(
        replica.retention_status().unwrap()["policy"]["retention_seconds"],
        1
    );
    let old_snapshot = replica.engine().snapshot().unwrap();
    tokio::time::sleep(Duration::from_millis(1300)).await;
    pair.catch_up(false).await;
    assert!(replica.get("items", &first[0].key).unwrap().is_some());
    pair.sa.switchover().await.unwrap();
    pair.catch_up(true).await;
    assert!(replica.get("items", &first[0].key).unwrap().is_some());
    let error = replica
        .write_batch_with("items", ops, &opts, "worker")
        .unwrap_err();
    assert!(matches!(
        error.code.as_str(),
        "IDEMPOTENCY_EXPIRED" | "HISTORY_CONFLICT"
    ));
    drop(old_snapshot);
    assert_eq!(
        replica
            .query("items", Query::default())
            .unwrap()
            .documents
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checked_batches_and_generic_publications_survive_replication_and_switch() {
    let pair = Pair::new().await;
    let id = pair.a.create_tenant("generic-kv").unwrap().id;
    let primary = pair.a.tenant(id).unwrap();
    primary.create_collection("data").unwrap();
    let config = primary
        .put("data", json!({"_key":"config","value":1}), None)
        .unwrap();
    let request = vec![
        WriteOp::Check {
            key: "config".into(),
            if_rev: config.rev,
        },
        WriteOp::Put {
            document: json!({"_key":"row","value":2}),
            if_rev: Some(0),
        },
    ];
    let opts = WriteOptions {
        expected_history: Some(config.history),
        operation_id: Some("checked".into()),
        ..Default::default()
    };
    let committed = primary
        .write_batch_with("data", request.clone(), &opts, "writer")
        .unwrap();
    let publication = primary
        .publish_dataset(
            "catalog",
            Publication {
                version: Uuid::new_v4(),
                collection: "data".into(),
                lookup: String::new(),
                expected_version: None,
                retry_token: None,
            },
        )
        .unwrap();
    pair.catch_up(false).await;
    let replica = pair.b.tenant(id).unwrap();
    let read = replica
        .read_published(
            "catalog",
            Some(publication.version),
            &[ReadRequest::Get { key: "row".into() }],
        )
        .unwrap();
    let ReadResult::Get {
        document: Some(row),
    } = &read.result.results[0]
    else {
        panic!()
    };
    assert_eq!(row["value"], 2);
    pair.sa.switchover().await.unwrap();
    pair.catch_up(true).await;
    let retry = replica
        .write_batch_with("data", request, &opts, "writer")
        .unwrap();
    assert_eq!(retry[0].rev, committed[0].rev);
    assert_eq!(
        replica
            .query_published("catalog", None, Query::default())
            .unwrap()
            .result
            .documents
            .len(),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_read_apis_preserve_positions_and_pin_all_cursor_forms() {
    use tenant_kv::client::{Client, ClientReadOptions, ReadPosition};
    let pair = Pair::new().await;
    let id = pair.a.create_tenant("client-reads").unwrap().id;
    let t = pair.a.tenant(id).unwrap();
    for collection in ["data", "versioned"] {
        t.create_collection(collection).unwrap();
        for key in ["a", "b", "c"] {
            t.put(collection, json!({"_key":key,"ip":key,"owner":key}), None)
                .unwrap();
        }
        t.create_index(
            collection,
            IndexSpec {
                name: "ip".into(),
                fields: vec![IndexField {
                    field: "/ip".into(),
                    order: Order::Asc,
                }],
                unique: true,
                include: vec!["/owner".into()],
            },
        )
        .unwrap();
        t.create_lookup(
            collection,
            LookupDefinition {
                name: "owners".into(),
                index: "ip".into(),
                select: vec!["/owner".into()],
                mode: MatchMode::All,
                max_matches: 100,
            },
        )
        .unwrap();
    }
    let published = t
        .publish_dataset(
            "assets",
            Publication {
                version: Uuid::new_v4(),
                collection: "versioned".into(),
                lookup: "owners".into(),
                expected_version: None,
                retry_token: None,
            },
        )
        .unwrap();
    pair.catch_up(false).await;
    let client = Client::new(&[pair.ua.clone(), pair.ub.clone()], ADMIN.into(), id).unwrap();
    let opts = ClientReadOptions::default();
    assert_eq!(
        client
            .multi_get("data", &["a".into(), "absent".into(), "a".into()], &opts)
            .await
            .unwrap()
            .iter()
            .map(Option::is_some)
            .collect::<Vec<_>>(),
        vec![true, false, true]
    );
    assert_eq!(
        client
            .lookup_many("data", "owners", &[json!({"ip":"a"})], &opts)
            .await
            .unwrap()
            .matches,
        vec![vec![json!({"owner":"a"})]]
    );
    assert_eq!(
        client
            .published_dataset("assets", &opts)
            .await
            .unwrap()
            .version,
        published.version
    );
    assert_eq!(
        client
            .lookup_published(
                "assets",
                Some(published.version),
                &[json!({"ip":"b"})],
                &opts
            )
            .await
            .unwrap()
            .result
            .matches,
        vec![vec![json!({"owner":"b"})]]
    );
    let page = Query {
        limit: 1,
        ..Default::default()
    };
    let first = client.query("data", page.clone(), &opts).await.unwrap();
    assert_eq!(first.documents[0]["_key"], "a");
    assert!(client.explain("data", &page, &opts).await.unwrap().scan);
    let batch = client
        .read_batch(
            &[ReadOperation {
                collection: "data".into(),
                request: ReadRequest::Query {
                    query: page.clone(),
                },
            }],
            &opts,
        )
        .await
        .unwrap();
    let ReadResult::Query {
        result: batch_query,
    } = &batch.results[0]
    else {
        panic!("query response")
    };
    let published_batch = client
        .read_published(
            "assets",
            Some(published.version),
            &[ReadRequest::Query {
                query: page.clone(),
            }],
            &opts,
        )
        .await
        .unwrap();
    let ReadResult::Query {
        result: published_query,
    } = &published_batch.result.results[0]
    else {
        panic!("query response")
    };
    for cursor in [
        first.cursor.as_ref(),
        batch_query.cursor.as_ref(),
        published_query.cursor.as_ref(),
    ] {
        assert!(cursor.unwrap().starts_with("tkv1."));
    }
    let wrong_tenant = Client::new(
        &[pair.ua.clone(), pair.ub.clone()],
        ADMIN.into(),
        Uuid::new_v4(),
    )
    .unwrap();
    assert_eq!(
        wrong_tenant
            .release_cursor(first.cursor.as_ref().unwrap())
            .await
            .unwrap_err()
            .code,
        "CURSOR_EXPIRED"
    );
    // A fresh read with a write position skips the lagging replica, without waiting five seconds.
    let write = t.put("data", json!({"_key":"fresh"}), None).unwrap();
    let fresh = ClientReadOptions {
        minimum: Some(ReadPosition::from(&write)),
        read: ReadOptions {
            timeout_ms: Some(500),
            ..Default::default()
        },
    };
    assert!(
        client
            .multi_get("data", &["fresh".into()], &fresh)
            .await
            .unwrap()[0]
            .is_some()
    );
    pair.catch_up(false).await;
    assert_eq!(
        client
            .query(
                "data",
                Query {
                    cursor: first.cursor.clone(),
                    ..page.clone()
                },
                &fresh
            )
            .await
            .unwrap_err()
            .code,
        "SNAPSHOT_TOO_OLD"
    );
    let wrong = ClientReadOptions {
        minimum: Some(ReadPosition {
            history: "other".into(),
            seq: 0,
        }),
        ..Default::default()
    };
    assert_eq!(
        client
            .query("data", page.clone(), &wrong)
            .await
            .unwrap_err()
            .code,
        "HISTORY_CONFLICT"
    );
    let strict = ClientReadOptions {
        read: ReadOptions {
            allow_full_scan: false,
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        client
            .query("data", page.clone(), &strict)
            .await
            .unwrap_err()
            .code,
        "SCAN_NOT_ALLOWED"
    );
    assert_eq!(
        client
            .query_published("assets", Some(published.version), page.clone(), &strict)
            .await
            .unwrap_err()
            .code,
        "SCAN_NOT_ALLOWED"
    );
    assert_eq!(
        client
            .read_published(
                "assets",
                Some(published.version),
                &[ReadRequest::Query {
                    query: page.clone()
                }],
                &strict
            )
            .await
            .unwrap_err()
            .code,
        "SCAN_NOT_ALLOWED"
    );
    // Removing the other node cannot affect continuations or release on the issuing replica.
    pair.servers[0].abort();
    let short = ClientReadOptions {
        read: ReadOptions {
            timeout_ms: Some(500),
            ..Default::default()
        },
        ..Default::default()
    };
    for cursor in [first.cursor, batch_query.cursor.clone()] {
        let next = client
            .query(
                "data",
                Query {
                    cursor,
                    ..page.clone()
                },
                &short,
            )
            .await
            .unwrap();
        assert_eq!(next.documents[0]["_key"], "b");
        assert_eq!(next.snapshot_seq, first.snapshot_seq);
        client
            .release_cursor(next.cursor.as_ref().unwrap())
            .await
            .unwrap();
        assert_eq!(
            client
                .query(
                    "data",
                    Query {
                        cursor: next.cursor,
                        ..page.clone()
                    },
                    &short
                )
                .await
                .unwrap_err()
                .code,
            "CURSOR_EXPIRED"
        );
    }
    let next = client
        .query_published(
            "assets",
            Some(published.version),
            Query {
                cursor: published_query.cursor.clone(),
                ..page
            },
            &short,
        )
        .await
        .unwrap();
    assert_eq!(next.result.documents[0]["_key"], "b");
    client
        .release_cursor(next.result.cursor.as_ref().unwrap())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_fallback_never_bypasses_budgets_or_cursor_affinity_and_checks_capabilities() {
    use axum::{
        body::Body,
        http::{Request, Response, StatusCode},
        routing::any,
    };
    use std::sync::{Arc, Mutex};
    use tenant_kv::client::{Client, ClientReadOptions};
    let tenant = Uuid::new_v4();
    let mode = Arc::new(Mutex::new("budget"));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let cursor = Uuid::new_v4();
    let mut nodes = Vec::new();
    let mut servers = Vec::new();
    for writable in [true, false] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        nodes.push(format!("http://{}", listener.local_addr().unwrap()));
        let mode = mode.clone();
        let calls = calls.clone();
        let router = axum::Router::new().fallback(any(move |request: Request<Body>| {
            let mode = mode.clone(); let calls = calls.clone();
            async move {
                let mode = *mode.lock().unwrap();
                let context = request.uri().path().ends_with("/context");
                calls.lock().unwrap().push((writable, context));
                if mode == "cache" && context { tokio::time::sleep(Duration::from_millis(20)).await; }
                if (mode == "slow" && !context) || (mode == "slow_primary" && writable && context) { tokio::time::sleep(Duration::from_secs(2)).await; }
                let (status, value) = if context {
                    (StatusCode::OK,json!({"tenant":tenant,"history":"history","applied_seq":5,"writable":writable || mode == "dual"}))
                } else if !writable && matches!(mode, "budget" | "unavailable") {
                    let code = if mode == "budget" {"RESOURCE_EXHAUSTED"} else {"UNAVAILABLE"};
                    (StatusCode::SERVICE_UNAVAILABLE,json!({"error":{"code":code,"message":"injected read failure"}}))
                } else {
                    assert_eq!(request.headers()["x-tkv-max-scan"],"12");
                    (StatusCode::OK,json!({"documents":[{"_key":"a"}],"cursor":cursor,"snapshot_seq":5,"plan":{"indexes":[],"covering":false,"extra_sort":false,"scan":true},"stats":ReadStats::default()}))
                };
                let mut response = Response::builder().status(status).header("content-type","application/json");
                if mode != "legacy" { response = response.header("x-tkv-read-controls","1"); }
                response.body(Body::from(serde_json::to_vec(&value).unwrap())).unwrap()
            }
        }));
        servers.push(tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        }));
    }
    let client = Client::new(&nodes, ADMIN.into(), tenant).unwrap();
    let options = ClientReadOptions {
        read: ReadOptions {
            max_scan: Some(12),
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        client
            .query("data", Query::default(), &options)
            .await
            .unwrap_err()
            .code,
        "RESOURCE_EXHAUSTED"
    );
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, context)| !context)
            .copied()
            .collect::<Vec<_>>(),
        vec![(false, false)]
    );
    calls.lock().unwrap().clear();
    *mode.lock().unwrap() = "unavailable";
    assert!(
        client
            .query("data", Query::default(), &options)
            .await
            .is_ok()
    );
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, context)| !context)
            .copied()
            .collect::<Vec<_>>(),
        vec![(false, false), (true, false)]
    );
    *mode.lock().unwrap() = "ok";
    let page = client
        .query("data", Query::default(), &options)
        .await
        .unwrap();
    calls.lock().unwrap().clear();
    *mode.lock().unwrap() = "unavailable";
    assert_eq!(
        client
            .query(
                "data",
                Query {
                    cursor: page.cursor,
                    ..Default::default()
                },
                &options
            )
            .await
            .unwrap_err()
            .code,
        "UNAVAILABLE"
    );
    assert_eq!(
        *calls.lock().unwrap(),
        vec![(false, false)],
        "continuation must not discover or fall back"
    );
    calls.lock().unwrap().clear();
    *mode.lock().unwrap() = "legacy";
    assert_eq!(
        client
            .query("data", Query::default(), &options)
            .await
            .unwrap_err()
            .code,
        "INCOMPATIBLE_VERSION"
    );
    assert_eq!(
        *calls.lock().unwrap(),
        vec![(false, false)],
        "a warm route still checks each data response capability"
    );
    calls.lock().unwrap().clear();
    let cold = Client::new(&nodes, ADMIN.into(), tenant).unwrap();
    assert_eq!(
        cold.query("data", Query::default(), &options)
            .await
            .unwrap_err()
            .code,
        "INCOMPATIBLE_VERSION"
    );
    assert!(calls.lock().unwrap().iter().all(|(_, context)| *context));
    *mode.lock().unwrap() = "cache";
    calls.lock().unwrap().clear();
    let cached = Client::new(&nodes, ADMIN.into(), tenant).unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let client = cached.clone();
        let options = options.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            client
                .query("data", Query::default(), &options)
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, context)| *context)
            .count(),
        2,
        "one discovery shared by all clones"
    );
    cached
        .query("data", Query::default(), &options)
        .await
        .unwrap();
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, context)| *context)
            .count(),
        2
    );
    tokio::time::sleep(Duration::from_millis(1050)).await;
    cached
        .query("data", Query::default(), &options)
        .await
        .unwrap();
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, context)| *context)
            .count(),
        4
    );
    *mode.lock().unwrap() = "dual";
    assert_eq!(
        client.write_context().await.unwrap_err().code,
        "REPLICATION_CONFLICT"
    );
    *mode.lock().unwrap() = "slow_primary";
    let bounded_discovery = ClientReadOptions {
        read: ReadOptions {
            timeout_ms: Some(200),
            max_scan: Some(12),
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(
        client
            .query("data", Query::default(), &bounded_discovery)
            .await
            .is_ok(),
        "a slow peer must leave time to read a healthy node"
    );
    *mode.lock().unwrap() = "slow";
    let start = Instant::now();
    let short = ClientReadOptions {
        read: ReadOptions {
            timeout_ms: Some(30),
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        client
            .query("data", Query::default(), &short)
            .await
            .unwrap_err()
            .code,
        "DEADLINE_EXCEEDED"
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    let cancel = ReadCancellation::default();
    let token = cancel.clone();
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        token.cancel();
    });
    let start = Instant::now();
    let cancellable = ClientReadOptions {
        read: ReadOptions {
            cancellation: Some(cancel),
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        client
            .query("data", Query::default(), &cancellable)
            .await
            .unwrap_err()
            .code,
        "CANCELLED"
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    cancel_task.await.unwrap();
    for server in servers {
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_http_writes_replicate_with_the_same_small_quota() {
    let pair = Pair::with_options(Options {
        global_memory_bytes: 256 << 20,
        tenant_memory_bytes: 19 << 20,
        cache_bytes: 64 << 10,
        memtable_bytes: 1 << 20,
        ..options()
    })
    .await;
    for (store, role) in [(&pair.a, Role::Primary), (&pair.b, Role::Replica)] {
        store
            .configure_replication(&Config {
                cluster: store.replication_state().cluster,
                role,
                peer_url: "http://127.0.0.1:1".into(),
                token_file: std::path::PathBuf::from("unused"),
                poll_ms: 10,
                log_retention_bytes: 64 << 20,
            })
            .unwrap();
    }
    let id = pair.a.create_tenant("audit-index-expansion").unwrap().id;
    let source = pair.a.tenant(id).unwrap();
    source.create_collection("docs").unwrap();
    pair.catch_up(false).await;
    for number in 0..16 {
        source
            .create_index(
                "docs",
                IndexSpec {
                    name: format!("i{number}"),
                    fields: vec![IndexField {
                        field: "/n".into(),
                        order: Order::Asc,
                    }],
                    unique: false,
                    include: vec!["/payload".into()],
                },
            )
            .unwrap();
    }
    pair.catch_up(false).await;
    let response = reqwest::Client::new()
        .put(format!(
            "{}/v1/tenants/{id}/collections/docs/documents",
            pair.ua
        ))
        .bearer_auth(ADMIN)
        .json(&json!({"document":{"_key":"large", "n":1, "payload":"x".repeat(256<<10)}}))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.bytes().await.unwrap();
    assert!(
        status.is_success(),
        "HTTP source rejected: {}",
        String::from_utf8_lossy(&body)
    );
    let acknowledged: WriteResult = serde_json::from_slice(&body).unwrap();
    source.engine().flush().unwrap();
    pair.catch_up(false).await;
    let actual = pair
        .b
        .tenant(id)
        .unwrap()
        .get("docs", "large")
        .unwrap()
        .unwrap();
    assert_eq!(actual["_rev"], acknowledged.rev);
    assert_eq!(Some(actual), source.get("docs", "large").unwrap());
    drop(source);
    stop_test_pair(pair).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn valid_small_quota_configuration_keeps_replicating_after_bootstrap() {
    let settings = Options {
        global_memory_bytes: 64 << 20,
        tenant_memory_bytes: 3 << 20,
        memtable_bytes: 1 << 20,
        cache_bytes: 0,
        ..options()
    };
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
    let ack = source
        .put("docs", json!({"_key":"small", "n":1}), None)
        .unwrap();
    pair.catch_up(false).await;
    let replica = pair.b.tenant(id).unwrap();
    assert_eq!(replica.engine().seq(), ack.rev);
    assert_eq!(
        replica.get("docs", "small").unwrap(),
        source.get("docs", "small").unwrap()
    );
    drop(replica);
    drop(source);
    stop_test_pair(pair).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oversized_log_page_rebuilds_instead_of_retrying_forever() {
    let pair = Pair::with_options(Options {
        global_memory_bytes: 64 << 20,
        tenant_memory_bytes: 3 << 20,
        memtable_bytes: 1 << 20,
        cache_bytes: 0,
        ..options()
    })
    .await;
    for (store, role) in [(&pair.a, Role::Primary), (&pair.b, Role::Replica)] {
        store
            .configure_replication(&Config {
                cluster: store.replication_state().cluster,
                role,
                peer_url: "http://127.0.0.1:1".into(),
                token_file: "unused".into(),
                poll_ms: 10,
                log_retention_bytes: 64 << 20,
            })
            .unwrap();
    }
    let id = pair.a.create_tenant("bounded-log-decode").unwrap().id;
    let source = pair.a.tenant(id).unwrap();
    source.create_collection("docs").unwrap();
    pair.catch_up(false).await;
    let initializations = pair.sb.status().unwrap().reinitializations;
    let mut expected = Vec::new();
    for n in 0..300 {
        let key = format!("d{n:04}");
        let receipt = source
            .put("docs", json!({"_key":key,"value":"x".repeat(4096)}), None)
            .unwrap();
        expected.push((key, receipt.rev));
        // Keep source preparation within its tiny quota independently of the
        // maintenance tick. Retained WALs still form the same oversized page.
        if (n + 1) % 32 == 0 {
            source.engine().flush().unwrap();
        }
    }
    source.engine().flush().unwrap();
    pair.catch_up(false).await;
    assert!(pair.sb.status().unwrap().reinitializations > initializations);
    let replica = pair.b.tenant(id).unwrap();
    for (key, rev) in expected {
        let actual = replica.get("docs", &key).unwrap().unwrap();
        assert_eq!(actual["_rev"], rev);
        assert_eq!(Some(actual), source.get("docs", &key).unwrap());
    }
    drop(replica);
    drop(source);
    stop_test_pair(pair).await;
}

async fn stop_test_pair(mut pair: Pair) {
    pair.sa.stop_initializations().await;
    pair.sb.stop_initializations().await;
    for server in pair.servers.drain(..) {
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_validation_uses_the_tenant_quota_instead_of_the_node_default() {
    let pair = Pair::with_options(Options {
        global_memory_bytes: 64 << 20,
        tenant_memory_bytes: 1 << 20,
        memtable_bytes: 128 << 10,
        cache_bytes: 0,
        ..options()
    })
    .await;
    let info = pair.a.create_tenant("larger-than-default").unwrap();
    pair.a
        .set_quota(info.id, 16 << 20, info.disk_bytes)
        .unwrap();
    let source = pair.a.tenant(info.id).unwrap();
    source.create_collection("docs").unwrap();
    let ack = source
        .put(
            "docs",
            json!({"_key":"large","payload":"x".repeat(700<<10)}),
            None,
        )
        .unwrap();
    source.engine().flush().unwrap();
    let expected = source.get("docs", "large").unwrap().unwrap();
    assert_eq!(expected["_rev"], ack.rev);
    pair.catch_up(false).await;
    let replica = pair.b.tenant(info.id).unwrap();
    assert_eq!(replica.info().memory_bytes, 16 << 20);
    assert_eq!(replica.get("docs", "large").unwrap(), Some(expected));
    assert_eq!(pair.sb.status().unwrap().initialization_failures, 0);
    drop(replica);
    drop(source);
    stop_test_pair(pair).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_pages_fit_global_budget_or_rebuild_without_infinite_retry() {
    // Give the primary enough workspace to produce a transaction whose
    // binary log page exceeds the replica's foreground decode budget.
    for (tenant_mib, payload_kib, checkpoint) in [(40, 384, false), (64, 896, true)] {
        let replica_options = Options {
            global_memory_bytes: 64 << 20,
            tenant_memory_bytes: tenant_mib << 20,
            cache_bytes: 64 << 10,
            memtable_bytes: 1 << 20,
            ..options()
        };
        let pair = Pair::with_store_options(
            Options {
                global_memory_bytes: 128 << 20,
                ..replica_options.clone()
            },
            replica_options,
            Role::Replica,
            None,
        )
        .await;
        for (store, role) in [(&pair.a, Role::Primary), (&pair.b, Role::Replica)] {
            store
                .configure_replication(&Config {
                    cluster: store.replication_state().cluster,
                    role,
                    peer_url: "http://127.0.0.1:1".into(),
                    token_file: "unused".into(),
                    poll_ms: 10,
                    log_retention_bytes: 64 << 20,
                })
                .unwrap();
        }
        let id = pair.a.create_tenant("global-decode").unwrap().id;
        let source = pair.a.tenant(id).unwrap();
        source.create_collection("docs").unwrap();
        for n in 0..16 {
            source
                .create_index(
                    "docs",
                    IndexSpec {
                        name: format!("i{n}"),
                        fields: vec![IndexField {
                            field: "/n".into(),
                            order: Order::Asc,
                        }],
                        unique: false,
                        include: vec!["/payload".into()],
                    },
                )
                .unwrap();
        }
        pair.catch_up(false).await;
        let before = pair.sb.status().unwrap().reinitializations;
        let ack = source
            .put(
                "docs",
                json!({"_key":"large","n":1,"payload":"x".repeat(payload_kib << 10)}),
                None,
            )
            .unwrap();
        source.engine().flush().unwrap();
        pair.catch_up(false).await;
        let replica = pair.b.tenant(id).unwrap();
        assert_eq!(replica.engine().seq(), ack.rev);
        assert_eq!(
            replica.get("docs", "large").unwrap(),
            source.get("docs", "large").unwrap()
        );
        assert_eq!(
            pair.sb.status().unwrap().reinitializations > before,
            checkpoint,
            "{payload_kib} KiB payload, {tenant_mib} MiB tenant budget"
        );
        drop(replica);
        drop(source);
        stop_test_pair(pair).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subsystem_review_switchover_retries_a_busy_snapshot_export() {
    let pair = Pair::new().await;
    let mut expected = Vec::new();
    for name in ["first", "second", "export-owner"] {
        let id = pair.a.create_tenant(name).unwrap().id;
        let tenant = pair.a.tenant(id).unwrap();
        tenant.create_collection("docs").unwrap();
        let ack = tenant
            .put("docs", json!({"_key":"confirmed","tenant":name}), None)
            .unwrap();
        tenant.engine().flush().unwrap();
        expected.push((id, ack.rev));
    }
    let state = pair.a.replication_state();
    let export = pair
        .sa
        .snapshot(state.cluster, state.epoch, expected[2].0)
        .unwrap();
    let source = pair.sa.clone();
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(700)).await;
        source.release_snapshot(export.id).unwrap();
    });
    let result = pair.sa.switchover().await;
    release.await.unwrap();
    result.unwrap();
    assert_eq!(pair.a.replication_state().role, Role::Replica);
    assert_eq!(pair.b.replication_state().role, Role::Primary);
    for (id, rev) in expected {
        assert_eq!(
            pair.b
                .tenant(id)
                .unwrap()
                .get("docs", "confirmed")
                .unwrap()
                .unwrap()["_rev"],
            rev
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edge_review_switchover_rejects_initial_replica_but_retries_completed_transfer() {
    let pair = Pair::new().await;
    let id = pair.a.create_tenant("switch-role").unwrap().id;
    let t = pair.a.tenant(id).unwrap();
    t.create_collection("docs").unwrap();
    t.put("docs", json!({"_key":"ack"}), None).unwrap();
    pair.catch_up(false).await;
    assert_eq!(pair.sb.switchover().await.unwrap_err().code, "CONFLICT");
    assert_eq!(pair.a.replication_state().role, Role::Primary);
    let first = pair.sa.switchover().await.unwrap();
    let retry = pair.sa.switchover().await.unwrap();
    assert_eq!(first.epoch, retry.epoch);
    assert_eq!(
        pair.b.tenant(id).unwrap().get("docs", "ack").unwrap(),
        t.get("docs", "ack").unwrap()
    );
}
