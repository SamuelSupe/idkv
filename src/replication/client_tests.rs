use super::*;
use crate::Options;
use std::{
    os::unix::fs::PermissionsExt,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

fn primary(options: Options) -> (tempfile::TempDir, Store, Service) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options).unwrap();
    let token = dir.path().join("peer-token");
    fs::write(&token, "replication-unit-test-secret").unwrap();
    std::fs::set_permissions(&token, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
    let service = Service::open(
        store.clone(),
        Config {
            cluster: Uuid::new_v4(),
            role: Role::Primary,
            peer_url: "http://127.0.0.1:1".into(),
            token_file: token,
            poll_ms: 10,
            log_retention_bytes: 1 << 20,
        },
    )
    .unwrap();
    (dir, store, service)
}

#[test]
fn replica_flush_retries_reclaim_contention_in_place_and_remains_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        dir.path(),
        Options {
            global_memory_bytes: 64 << 20,
            tenant_memory_bytes: 64 << 20,
            memtable_bytes: 16 << 20,
            cache_bytes: 0,
            ..Options::default()
        },
    )
    .unwrap();
    let info = store.create_tenant("replica-flush-busy").unwrap();
    let handle = store.tenant(info.id).unwrap();
    handle.create_collection("docs").unwrap();
    let receipt = handle
        .put(
            "docs",
            json!({"_key":"confirmed","value":"x".repeat(128 << 10)}),
            None,
        )
        .unwrap();
    let (_, budget, _) = store.backup_context(info.id).unwrap();
    let held = budget
        .reserve(budget.global.foreground_limit() - budget.global.used())
        .unwrap();
    let (_flush, permit) = budget.for_flush(true).unwrap();
    assert_eq!(handle.engine().flush().unwrap_err().code, "BUSY");
    let engine = handle.engine().clone();
    let (started, ready) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        started.send(()).unwrap();
        flush_replica_buffer(&engine)
    });
    ready.recv().unwrap();
    std::thread::sleep(Duration::from_millis(25));
    drop(permit);
    worker.join().unwrap().unwrap();
    drop(held);
    assert_eq!(
        handle.get("docs", "confirmed").unwrap().unwrap()["_rev"],
        receipt.rev
    );

    handle
        .put("docs", json!({"_key":"next","value":2}), None)
        .unwrap();
    let held = budget
        .reserve(budget.global.foreground_limit() - budget.global.used())
        .unwrap();
    let (_flush, _permit) = budget.for_flush(true).unwrap();
    let started = Instant::now();
    assert_eq!(
        flush_replica_buffer(handle.engine()).unwrap_err().code,
        "BUSY"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    drop(held);
    assert_eq!(
        handle.get("docs", "confirmed").unwrap().unwrap()["_rev"],
        receipt.rev
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_log_busy_uses_short_backoff_without_resetting_initialization_state() {
    let (_dir, a, sa) = primary(Options::default());
    let info = a.create_tenant("transient-log-busy").unwrap();
    let tenant = a.tenant(info.id).unwrap();
    tenant.create_collection("docs").unwrap();
    tenant
        .put("docs", json!({"_key":"confirmed","value":1}), None)
        .unwrap();
    let blocked = Arc::new(AtomicBool::new(false));
    let filter = blocked.clone();
    let app = crate::http::app_with_replication(
        a.clone(),
        crate::http::AuthConfig {
            tokens: vec![crate::http::Token {
                subject: "replica".into(),
                sha256: crate::http::token_hash("replication-unit-test-secret"),
                tenant: None,
                role: crate::http::Role::Replication,
            }],
        },
        None,
        Some(sa),
    )
    .unwrap()
    .layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let blocked = filter.clone();
            async move {
                if request.uri().path() == "/v1/replication/log" && blocked.load(Ordering::Acquire)
                {
                    return axum::response::IntoResponse::into_response(Error::new(
                        "BUSY",
                        "temporary drain",
                    ));
                }
                next.run(request).await
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let dir = tempfile::tempdir().unwrap();
    let b = Store::open(dir.path(), Options::default()).unwrap();
    let token = dir.path().join("peer-token");
    fs::write(&token, "replication-unit-test-secret").unwrap();
    std::fs::set_permissions(&token, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
    let sb = Service::open(
        b.clone(),
        Config {
            cluster: a.replication_state().cluster,
            role: Role::Replica,
            peer_url: url,
            token_file: token,
            poll_ms: 10,
            log_retention_bytes: 1 << 20,
        },
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        sb.sync_once().await.unwrap();
        // Installation publishes its position before task completion is collected.
        // Finish that cleanup before injecting retry state.
        if b.tenant(info.id)
            .is_ok_and(|t| t.engine().seq() == tenant.engine().seq())
            && !sb.inner.progress.lock().initializing.contains_key(&info.id)
        {
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    blocked.store(true, Ordering::Release);
    let receipt = tenant
        .put("docs", json!({"_key":"confirmed","value":2}), None)
        .unwrap();
    let attempts = sb.inner.progress.lock().initialization_attempts;
    sb.inner
        .progress
        .lock()
        .initialization_backoff
        .insert(info.id, Duration::from_secs(30));
    for _ in 0..12 {
        if let Some((at, _)) = sb.inner.progress.lock().log_backoff.get_mut(&info.id) {
            *at = Instant::now();
        }
        assert_eq!(sb.sync_once().await.unwrap_err().code, "BUSY");
        let progress = sb.inner.progress.lock();
        let (at, next) = progress.log_backoff[&info.id];
        assert!(at.saturating_duration_since(Instant::now()) <= Duration::from_secs(1));
        assert!(next <= Duration::from_secs(1));
        assert_eq!(
            progress.initialization_backoff[&info.id],
            Duration::from_secs(30)
        );
    }
    blocked.store(false, Ordering::Release);
    sb.inner
        .progress
        .lock()
        .log_backoff
        .get_mut(&info.id)
        .unwrap()
        .0 = Instant::now();
    sb.sync_once().await.unwrap();
    let replica = b.tenant(info.id).unwrap();
    assert_eq!(
        replica.get("docs", "confirmed").unwrap().unwrap()["_rev"],
        receipt.rev
    );
    assert_eq!(sb.inner.progress.lock().initialization_attempts, attempts);
    assert!(!sb.inner.progress.lock().log_backoff.contains_key(&info.id));
    assert_eq!(
        sb.inner.progress.lock().initialization_backoff[&info.id],
        Duration::from_secs(30)
    );
    server.abort();
}

#[test]
fn checkpoint_free_space_reserve_rejects_replication_and_local_backup_without_poisoning_writes() {
    let (_dir, store, service) = primary(Options {
        maintenance_reserve_free_bytes: u64::MAX,
        ..Default::default()
    });
    let info = store.create_tenant("free-space-reserve").unwrap();
    let tenant = store.tenant(info.id).unwrap();
    tenant.create_collection("docs").unwrap();
    let receipt = tenant
        .put("docs", json!({"_key":"confirmed","value":1}), None)
        .unwrap();
    let state = store.replication_state();
    assert_eq!(
        service
            .snapshot(state.cluster, state.epoch, info.id)
            .err()
            .unwrap()
            .code,
        "RESOURCE_EXHAUSTED"
    );
    assert_eq!(
        store.backup(info.id).unwrap_err().code,
        "RESOURCE_EXHAUSTED"
    );
    assert!(
        tenant.engine().table_counts().is_empty(),
        "free-space rejection flushed the live engine"
    );
    assert!(tenant.engine().writable());
    assert_eq!(
        tenant.get("docs", "confirmed").unwrap().unwrap()["_rev"],
        receipt.rev
    );
    tenant
        .put("docs", json!({"_key":"next","value":2}), None)
        .unwrap();
}

#[tokio::test]
async fn checkpoint_preparation_does_not_lock_out_other_tenant_log_progress() {
    let (_dir, store, service) = primary(Options {
        io_workers: 1,
        ..Default::default()
    });
    let good = store.create_tenant("catching-up").unwrap();
    let slow = store.create_tenant("preparing").unwrap();
    let slow_tenant = store.tenant(slow.id).unwrap();
    slow_tenant
        .engine()
        .transact(|tx| tx.put(b"checkpoint-file".to_vec(), vec![42; 1024]))
        .unwrap();
    slow_tenant.engine().flush().unwrap();
    let state = store.replication_state();
    let snapshot = service
        .snapshot(state.cluster, state.epoch, good.id)
        .unwrap();
    let tenant = store.tenant(good.id).unwrap();
    tenant
        .engine()
        .transact(|tx| tx.put(b"ack".to_vec(), b"value".to_vec()))
        .unwrap();
    let request = LogRequest {
        protocol: super::super::PROTOCOL,
        cluster: state.cluster,
        epoch: state.epoch,
        tenant: good.id,
        generation: good.generation,
        after: snapshot.position,
        cursor: None,
    };
    assert!(service.export_log(&request).unwrap().through > snapshot.position);
    let (_, _, io) = store.backup_context(slow.id).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let blocker = io
        .submit(slow.id, move || {
            entered_tx.send(()).unwrap();
            let _ = release_rx.recv();
            Ok(())
        })
        .unwrap();
    entered_rx.recv().unwrap();
    let preparing = service.clone();
    let worker =
        std::thread::spawn(move || preparing.snapshot(state.cluster, state.epoch, slow.id));
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut reserved = false;
    while Instant::now() < deadline {
        reserved = service
            .inner
            .exports
            .try_lock()
            .is_some_and(|exports| exports.generating.is_some());
        if reserved {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let progressing = service.clone();
    let (tx, rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let duplicate = progressing
            .snapshot(state.cluster, state.epoch, slow.id)
            .err()
            .unwrap()
            .code;
        let page = progressing.export_log(&LogRequest {
            after: tenant.engine().seq(),
            ..request
        });
        tx.send((duplicate, page)).unwrap();
    });
    let progressed = rx.recv_timeout(Duration::from_secs(1));
    release_tx.send(()).unwrap();
    blocker.wait().unwrap();
    reader.join().unwrap();
    let generated = worker.join().unwrap().unwrap();
    assert!(reserved, "preparation held the export state lock");
    let (duplicate, page) = progressed.expect("other tenant could not renew while preparing");
    assert_eq!(duplicate, "BUSY");
    assert!(page.unwrap().frames.is_empty());
    service.release_snapshot(generated.id).unwrap();
    service.expire_exports().unwrap();
    assert!(service.inner.exports.lock().garbage.is_empty());
}

#[tokio::test]
async fn export_cleanup_failure_does_not_fail_logs_and_is_retried() {
    let (_dir, store, service) = primary(Options::default());
    let info = store.create_tenant("cleanup-retry").unwrap();
    let other = store.create_tenant("next-export").unwrap();
    let state = store.replication_state();
    let snapshot = service
        .snapshot(state.cluster, state.epoch, info.id)
        .unwrap();
    let path = store
        .root()
        .join("replication-work/export")
        .join(snapshot.id.to_string());
    let permissions = fs::metadata(&path).unwrap().permissions();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
    let page = service.export_log(&LogRequest {
        protocol: super::super::PROTOCOL,
        cluster: state.cluster,
        epoch: state.epoch,
        tenant: info.id,
        generation: info.generation,
        after: snapshot.position,
        cursor: None,
    });
    let failed = service.expire_exports();
    let queued = service.inner.exports.lock().garbage.contains(&path);
    fs::set_permissions(&path, permissions).unwrap();
    assert!(
        page.unwrap().frames.is_empty(),
        "cleanup changed a valid log result"
    );
    assert!(
        failed.is_err() && queued,
        "cleanup did not retain the failed path"
    );
    let next = service
        .snapshot(state.cluster, state.epoch, other.id)
        .unwrap();
    service.expire_exports().unwrap();
    assert!(!path.exists());
    service.release_snapshot(next.id).unwrap();
    service.expire_exports().unwrap();
}

#[tokio::test]
async fn failed_preparation_releases_the_slot_for_a_new_export() {
    let (_dir, store, service) = primary(Options::default());
    let state = store.replication_state();
    assert_eq!(
        service
            .snapshot(state.cluster, state.epoch, Uuid::new_v4())
            .err()
            .unwrap()
            .code,
        "NOT_FOUND"
    );
    let info = store.create_tenant("retry-export").unwrap();
    let snapshot = service
        .snapshot(state.cluster, state.epoch, info.id)
        .unwrap();
    service.release_snapshot(snapshot.id).unwrap();
    service.expire_exports().unwrap();
    assert!(service.inner.exports.lock().garbage.is_empty());
}

#[tokio::test]
async fn unwinding_checkpoint_preparation_releases_the_reserved_export() {
    let (_dir, store, service) = primary(Options::default());
    let info = store.create_tenant("panic-preparation").unwrap();
    let state = store.replication_state();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let super::super::snapshot::Start::Build(preparation) = service
            .inner
            .exports
            .lock()
            .begin(&store, state.cluster, state.epoch, info.id)
            .unwrap()
        else {
            panic!("expected reservation")
        };
        let _pending = PendingExport {
            exports: &service.inner.exports,
            preparation: Some(preparation),
        };
        panic!("injected preparation panic");
    }));
    assert!(unwound.is_err());
    let snapshot = service
        .snapshot(state.cluster, state.epoch, info.id)
        .unwrap();
    service.release_snapshot(snapshot.id).unwrap();
    service.expire_exports().unwrap();
    assert!(service.inner.exports.lock().garbage.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn edge_review_peer_errors_and_token_rotation_are_bounded() {
    use axum::{
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::get,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new()
        .route(
            "/gateway",
            get(|| async { (StatusCode::BAD_GATEWAY, "<html>bad gateway</html>") }),
        )
        .route(
            "/foreign",
            get(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({"error":{"code":"SURPRISE","message":"foreign"}})),
                )
            }),
        )
        .route(
            "/conflict",
            get(|| async {
                (
                    StatusCode::CONFLICT,
                    axum::Json(
                        json!({"error":{"code":"CONFLICT","message":"fence does not match"}}),
                    ),
                )
            }),
        )
        .route(
            "/read-only",
            get(|| async {
                (
                    StatusCode::CONFLICT,
                    axum::Json(json!({"error":{"code":"READ_ONLY","message":"source fenced"}})),
                )
            }),
        )
        .route(
            "/rotate",
            get(|headers: HeaderMap| async move {
                if headers.get("authorization").unwrap() == "Bearer rotated-replication-secret" {
                    axum::Json(json!({"ok":true})).into_response()
                } else {
                    StatusCode::UNAUTHORIZED.into_response()
                }
            }),
        );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let token = dir.path().join("token");
    fs::write(&token, "original-replication-secret").unwrap();
    fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).unwrap();
    let service = Service::open(
        store,
        Config {
            cluster: Uuid::new_v4(),
            role: Role::Replica,
            peer_url: url,
            token_file: token.clone(),
            poll_ms: 10,
            log_retention_bytes: 1 << 20,
        },
    )
    .unwrap();
    for path in ["/gateway", "/foreign", "/rotate"] {
        assert_eq!(
            service
                .request::<Value>(path, None)
                .await
                .err()
                .unwrap()
                .code,
            "REPLICATION_UNAVAILABLE"
        );
    }
    for (path, code) in [("/conflict", "CONFLICT"), ("/read-only", "READ_ONLY")] {
        let error = service.request::<Value>(path, None).await.err().unwrap();
        assert_eq!(error.code, code);
        assert!(error.message.contains(if code == "CONFLICT" {
            "fence does not match"
        } else {
            "source fenced"
        }));
    }
    let link = dir.path().join("hard-linked-token");
    fs::hard_link(&token, &link).unwrap();
    assert!(read_token(&token).is_err());
    fs::remove_file(link).unwrap();
    fs::write(&token, "rotated-replication-secret").unwrap();
    assert_eq!(
        service
            .request::<Value>("/rotate", None)
            .await
            .unwrap()
            .value["ok"],
        true
    );
    fs::set_permissions(&token, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(read_token(&token).is_err());
    fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&token, vec![b'x'; 4096]).unwrap();
    assert!(read_token(&token).is_err());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn edge_review_progressing_response_body_can_take_more_than_thirty_seconds() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            assert!(headers.len() < 4096);
            headers.push(socket.read_u8().await.unwrap());
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 34\r\nConnection: close\r\n\r\n\"")
            .await
            .unwrap();
        for _ in 0..32 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            socket.write_all(b"a").await.unwrap();
        }
        socket.write_all(b"\"").await.unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let token = dir.path().join("token");
    fs::write(&token, "replication-unit-test-secret").unwrap();
    fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).unwrap();
    let service = Service::open(
        store,
        Config {
            cluster: Uuid::new_v4(),
            role: Role::Replica,
            peer_url: url,
            token_file: token,
            poll_ms: 10,
            log_retention_bytes: 1 << 20,
        },
    )
    .unwrap();
    let start = Instant::now();
    let response = service.request::<String>("/slow", None).await.unwrap();
    assert_eq!(response.value, "a".repeat(32));
    assert!(start.elapsed() > Duration::from_secs(30));
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review0732_trickling_response_releases_cycle_at_total_deadline() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            assert!(headers.len() < 4096);
            headers.push(socket.read_u8().await.unwrap());
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\n\r\n\"")
            .await
            .unwrap();
        loop {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if socket.write_all(b"a").await.is_err() {
                break;
            }
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let token = dir.path().join("token");
    fs::write(&token, "replication-unit-test-secret").unwrap();
    fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).unwrap();
    let service = Service::open(
        store,
        Config {
            cluster: Uuid::new_v4(),
            role: Role::Replica,
            peer_url: url,
            token_file: token,
            poll_ms: 10,
            log_retention_bytes: 1 << 20,
        },
    )
    .unwrap();
    let task_service = service.clone();
    let task = tokio::spawn(async move {
        let _cycle = task_service.inner.cycle.lock().await;
        task_service
            .request_bounded::<Value>("/slow", None, Duration::from_millis(250))
            .await
            .err()
            .unwrap()
    });
    let error = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(error.code, "REPLICATION_UNAVAILABLE");
    let _cycle = tokio::time::timeout(Duration::from_secs(1), service.inner.cycle.lock())
        .await
        .unwrap();
    server.abort();
}
