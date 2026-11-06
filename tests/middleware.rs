use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tenant_kv::http::{self, AuthConfig, HttpRuntime, Role, Token};
use tenant_kv::*;
use tower::ServiceExt;
use uuid::Uuid;

fn options() -> Options {
    Options {
        global_memory_bytes: 64 << 20,
        tenant_memory_bytes: 32 << 20,
        cache_bytes: 1 << 20,
        memtable_bytes: 1 << 20,
        ..Default::default()
    }
}
fn auth() -> AuthConfig {
    AuthConfig {
        tokens: ["middleware-secret-one", "middleware-secret-two"]
            .iter()
            .map(|secret| Token {
                sha256: http::token_hash(secret),
                subject: "ingest-service".into(),
                tenant: None,
                role: Role::Admin,
            })
            .collect(),
    }
}
async fn call(
    app: &Router,
    method: &str,
    path: &str,
    body: Value,
    token: &str,
) -> (StatusCode, Value) {
    call_headers(app, method, path, body, token, &[]).await
}
async fn call_headers(
    app: &Router,
    method: &str,
    path: &str,
    body: Value,
    token: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"));
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(
            request
                .body(Body::from(if method == "GET" {
                    Vec::new()
                } else {
                    serde_json::to_vec(&body).unwrap()
                }))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

#[test]
fn restored_history_rejects_aba_and_receipts_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options()).unwrap();
    let id = store.create_tenant("a").unwrap().id;
    let tenant = store.tenant(id).unwrap();
    tenant.create_collection("assets").unwrap();
    tenant
        .put("assets", json!({"_key":"k","value":1}), None)
        .unwrap();
    let backup = store.backup(id).unwrap();
    let old = tenant
        .put("assets", json!({"_key":"k","value":2}), None)
        .unwrap();
    drop(tenant);
    store.restore(id, backup).unwrap();
    let tenant = store.tenant(id).unwrap();
    let new = tenant
        .put("assets", json!({"_key":"k","value":3}), None)
        .unwrap();
    assert_eq!(old.rev, new.rev);
    assert_ne!(old.history, new.history);
    let op = WriteOp::Put {
        document: json!({"_key":"k","value":"stale"}),
        if_rev: Some(old.rev),
    };
    assert_eq!(
        tenant
            .write_batch("assets", vec![op.clone()])
            .unwrap_err()
            .code,
        "HISTORY_REQUIRED"
    );
    assert_eq!(
        tenant
            .write_batch_with(
                "assets",
                vec![op],
                &WriteOptions {
                    retry_token: None,
                    expected_history: Some(old.history),
                    operation_id: None
                },
                "ingest"
            )
            .unwrap_err()
            .code,
        "HISTORY_CONFLICT"
    );
    assert_eq!(tenant.get("assets", "k").unwrap().unwrap()["value"], 3);
    let request = vec![WriteOp::Put {
        document: json!({"value":"generated"}),
        if_rev: None,
    }];
    let opts = WriteOptions {
        retry_token: None,
        expected_history: Some(tenant.history()),
        operation_id: Some("retry-001".into()),
    };
    let first = tenant
        .write_batch_with("assets", request.clone(), &opts, "ingest")
        .unwrap();
    let seq = tenant.engine().seq();
    let retry = tenant
        .write_batch_with("assets", request.clone(), &opts, "ingest")
        .unwrap();
    assert_eq!(first[0].key, retry[0].key);
    assert_eq!(seq, tenant.engine().seq());
    drop(tenant);
    drop(store);
    let store = Store::open(dir.path(), options()).unwrap();
    let tenant = store.tenant(id).unwrap();
    assert_eq!(
        tenant
            .write_batch_with("assets", request, &opts, "ingest")
            .unwrap()[0]
            .rev,
        first[0].rev
    );
    assert_eq!(
        tenant
            .write_batch_with(
                "assets",
                vec![WriteOp::Delete {
                    key: first[0].key.clone(),
                    if_rev: None
                }],
                &opts,
                "ingest"
            )
            .unwrap_err()
            .code,
        "OPERATION_CONFLICT"
    );
    assert_eq!(store.replication_state().protocol, 3);
}

#[test]
fn publication_freezes_all_mutations_and_keeps_queued_versions() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options()).unwrap();
    let id = store.create_tenant("a").unwrap().id;
    let tenant = store.tenant(id).unwrap();
    let mut previous = None;
    let mut versions = Vec::new();
    for i in 1..=2 {
        let collection = format!("assets_{i}");
        tenant.create_collection(&collection).unwrap();
        tenant
            .put(&collection, json!({"_key":"k","ip":"a","owner":i}), None)
            .unwrap();
        tenant
            .create_index(
                &collection,
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
        tenant
            .create_lookup(
                &collection,
                LookupDefinition {
                    name: "owners".into(),
                    index: "ip".into(),
                    select: vec!["/owner".into()],
                    mode: MatchMode::All,
                    max_matches: 100,
                },
            )
            .unwrap();
        let publication = Publication {
            retry_token: None,
            version: Uuid::new_v4(),
            collection: collection.clone(),
            lookup: "owners".into(),
            expected_version: previous,
        };
        let pinned = tenant
            .publish_lookup("assets", publication.clone())
            .unwrap();
        assert_eq!(
            tenant
                .publish_lookup("assets", publication)
                .unwrap()
                .version,
            pinned.version
        );
        previous = Some(pinned.version);
        versions.push(pinned.version);
        assert_eq!(
            tenant
                .put(&collection, json!({"_key":"k","owner":9}), None)
                .unwrap_err()
                .code,
            "IMMUTABLE_DATASET"
        );
        assert_eq!(
            tenant.drop_lookup(&collection, "owners").unwrap_err().code,
            "IMMUTABLE_DATASET"
        );
        assert_eq!(
            tenant.drop_index(&collection, "ip").unwrap_err().code,
            "IMMUTABLE_DATASET"
        );
        assert_eq!(
            tenant.drop_collection(&collection).unwrap_err().code,
            "IMMUTABLE_DATASET"
        );
    }
    drop(tenant);
    drop(store);
    let store = Store::open(dir.path(), options()).unwrap();
    let tenant = store.tenant(id).unwrap();
    for (i, version) in versions.iter().enumerate() {
        let result = tenant
            .lookup_published("assets", Some(*version), &[json!({"ip":"a"})])
            .unwrap();
        assert_eq!(result.result.matches[0][0]["owner"], i + 1);
        assert_eq!(result.result.stats.document_reads, 0);
    }
    assert_eq!(
        tenant
            .lookup_published("assets", None, &[json!({"ip":"a"})])
            .unwrap()
            .version,
        versions[1]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_small_waits_do_not_starve_other_tenants_and_rotation_is_stable() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options()).unwrap();
    let a = store.create_tenant("a").unwrap().id;
    let b = store.create_tenant("b").unwrap().id;
    for id in [a, b] {
        let t = store.tenant(id).unwrap();
        t.create_collection("assets").unwrap();
        t.put("assets", json!({"_key":"k"}), None).unwrap();
    }
    let runtime = HttpRuntime::default();
    let mut config = auth();
    for (secret, role) in [
        ("identity-write-secret", Role::IdentityWrite),
        ("identity-correct-secret", Role::IdentityCorrect),
    ] {
        config.tokens.push(Token {
            sha256: http::token_hash(secret),
            subject: "identity-service".into(),
            tenant: Some(a),
            role,
        });
    }
    let app = http::app_with_runtime(store.clone(), config, None, None, runtime.clone()).unwrap();
    let history = store.tenant(a).unwrap().history();
    let mut waits = Vec::new();
    for _ in 0..3 {
        let app = app.clone();
        let history = history.clone();
        waits.push(tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .uri(format!("/v1/tenants/{a}/collections/assets/documents/k"))
                    .header("authorization", "Bearer middleware-secret-one")
                    .header("x-tkv-min-history", history)
                    .header("x-tkv-min-seq", "99999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(waits.iter().all(|task| !task.is_finished()));
    assert!(store.memory_bytes() < 12 << 20, "{}", store.memory_bytes());
    let body = json!({"document":{"_key":"k","message":"not-in-logs"},"write_options":{"expected_history":store.tenant(b).unwrap().history(),"operation_id":"rotate"}});
    let path = format!("/v1/tenants/{b}/collections/assets/documents");
    let first = call(&app, "PUT", &path, body.clone(), "middleware-secret-one").await;
    assert_eq!(first.0, StatusCode::OK, "{:?}", first.1);
    assert_eq!(
        first.1,
        call(&app, "PUT", &path, body, "middleware-secret-two")
            .await
            .1
    );
    let identity = format!("/v1/tenants/{a}/identity");
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{identity}/initialize"),
            json!({}),
            "identity-write-secret"
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{identity}/initialize"),
            json!({}),
            "middleware-secret-one"
        )
        .await
        .0,
        StatusCode::OK
    );
    let create =
        json!({"action":"create_entity","operation_id":"rotating-identity","attributes":{}});
    let created = call(
        &app,
        "POST",
        &format!("{identity}/write"),
        create.clone(),
        "middleware-secret-one",
    )
    .await;
    assert_eq!(created.0, StatusCode::OK);
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{identity}/write"),
            create,
            "middleware-secret-two"
        )
        .await
        .1,
        created.1
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{identity}/write"),
            json!({"action":"create_entity","operation_id":"identity-writer","attributes":{}}),
            "identity-write-secret"
        )
        .await
        .0,
        StatusCode::OK
    );
    let binding = json!({"action":"bindings","operation_id":"manual-binding","changes":[{"identifier":{"kind":"ip","namespace":"test","value":"a"},"expected_revision":"absent","effective":{"mode":"manual","from":"2026-01-01T00:00:00Z"},"change":{"action":"bind","entity_id":created.1["entity"]["entity_id"],"reason":"correction"}}]});
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{identity}/write"),
            binding.clone(),
            "identity-write-secret"
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{identity}/write"),
            binding,
            "identity-correct-secret"
        )
        .await
        .0,
        StatusCode::OK
    );
    for wait in waits {
        assert_eq!(wait.await.unwrap().status(), StatusCode::CONFLICT);
    }
    runtime.drain();
    assert_eq!(
        call(&app, "GET", "/ready", json!({}), "middleware-secret-one")
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        call(&app, "GET", "/health", json!({}), "middleware-secret-one")
            .await
            .0,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn otlp_exports_correlated_redacted_signals_and_switches_independently() {
    use std::sync::{Arc, Mutex};
    let received = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
    let sink = received.clone();
    let collector = Router::new().route(
        "/{*path}",
        axum::routing::post(
            move |path: axum::extract::Path<String>, body: axum::Json<Value>| {
                let sink = sink.clone();
                async move {
                    sink.lock().unwrap().push((path.0, body.0));
                    axum::Json(json!({}))
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, collector).await.unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options()).unwrap();
    let id = store.create_tenant("a").unwrap().id;
    store
        .tenant(id)
        .unwrap()
        .create_collection("assets")
        .unwrap();
    for (traces, logs) in [(true, true), (true, false), (false, true), (false, false)] {
        received.lock().unwrap().clear();
        let cfg = telemetry::Config {
            endpoint: endpoint.clone(),
            traces,
            logs,
            metrics: false,
            sample_ratio: 1.0,
            export_timeout_ms: 500,
            ..Default::default()
        };
        let telemetry =
            tokio::task::spawn_blocking(move || telemetry::Telemetry::new(cfg).unwrap())
                .await
                .unwrap();
        let app = http::app_with_runtime(
            store.clone(),
            auth(),
            None,
            None,
            HttpRuntime::new(telemetry.clone()),
        )
        .unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/tenants/{id}/collections/assets/documents"))
                    .header("authorization", "Bearer middleware-secret-one")
                    .header(
                        "traceparent",
                        "00-11111111111111111111111111111111-2222222222222222-01",
                    )
                    .body(Body::from(
                        r#"{"document":{"_key":"private-key","secret":"private-body"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            call(&app, "GET", "/metrics", json!({}), "middleware-secret-one")
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        tokio::task::spawn_blocking(move || telemetry.shutdown().unwrap())
            .await
            .unwrap();
        let records = received.lock().unwrap();
        assert_eq!(records.iter().any(|(path, _)| path == "v1/traces"), traces);
        assert_eq!(records.iter().any(|(path, _)| path == "v1/logs"), logs);
        let serialized = serde_json::to_string(&*records).unwrap();
        for secret in ["private-key", "private-body", "middleware-secret-one"] {
            assert!(!serialized.contains(secret));
        }
        if traces || logs {
            assert!(
                serialized.contains("11111111111111111111111111111111"),
                "{serialized}"
            );
        }
        if logs {
            assert!(serialized.contains("ingest-service"));
        }
    }
    server.abort();
    let cfg = telemetry::Config {
        endpoint,
        traces: true,
        logs: true,
        export_timeout_ms: 100,
        queue_size: 16,
        ..Default::default()
    };
    let telemetry = tokio::task::spawn_blocking(move || telemetry::Telemetry::new(cfg).unwrap())
        .await
        .unwrap();
    let started = std::time::Instant::now();
    for _ in 0..1000 {
        telemetry
            .begin("/bounded", "GET", &Default::default())
            .finish(200);
    }
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
    let _ = tokio::task::spawn_blocking(move || telemetry.shutdown())
        .await
        .unwrap();
}

#[tokio::test]
async fn tenant_retention_http_preserves_current_data_and_rejects_expired_retries() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options()).unwrap();
    let id = store.create_tenant("expiry").unwrap().id;
    let tenant = store.tenant(id).unwrap();
    tenant.create_collection("items").unwrap();
    let app = http::app(store.clone(), auth()).unwrap();
    let base = format!("/v1/tenants/{id}");
    let token = "middleware-secret-one";
    let (status, policy) = call(
        &app,
        "PUT",
        &format!("{base}/retention"),
        json!({"retention_seconds":1}),
        token,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(policy["policy"]["retention_seconds"], 1);
    let (status, context) = call(
        &app,
        "POST",
        &format!("{base}/retry-context"),
        json!({}),
        token,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body = json!({"document":{"value":"current"},"write_options":{"expected_history":context["history"],"retry_token":context["retry_token"],"operation_id":"once"}});
    let path = format!("{base}/collections/items/documents");
    let (status, first) = call(&app, "PUT", &path, body.clone(), token).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(call(&app, "PUT", &path, body.clone(), token).await.1, first);
    tokio::time::sleep(std::time::Duration::from_millis(1150)).await;
    let (status, error) = call(&app, "PUT", &path, body, token).await;
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(error["error"]["code"], "IDEMPOTENCY_EXPIRED");
    assert!(
        tenant
            .get("items", first["key"].as_str().unwrap())
            .unwrap()
            .is_some()
    );
    let schema = http::openapi();
    assert_eq!(
        schema["paths"]["/v1/tenants/{tenant}/retention"]["put"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/RetentionConfig"
    );
}

#[test]
fn retention_backup_restore_preserves_deadlines_and_blocks_absent_old_history_retries() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options()).unwrap();
    let id = store.create_tenant("restore-expiry").unwrap().id;
    let t = store.tenant(id).unwrap();
    t.create_collection("items").unwrap();
    t.set_retention(60).unwrap();
    let context = t.retry_context("worker").unwrap();
    let opts = WriteOptions {
        expected_history: Some(context.history),
        operation_id: Some("after-backup".into()),
        retry_token: Some(context.retry_token),
    };
    let backup = store.backup(id).unwrap();
    let ops = vec![WriteOp::Put {
        document: json!({"value":1}),
        if_rev: None,
    }];
    let first = t
        .write_batch_with("items", ops.clone(), &opts, "worker")
        .unwrap();
    drop(t);
    store.restore(id, backup).unwrap();
    let t = store.tenant(id).unwrap();
    assert_eq!(
        t.retention_status().unwrap()["policy"]["retention_seconds"],
        60
    );
    assert!(t.get("items", &first[0].key).unwrap().is_none());
    assert_eq!(
        t.write_batch_with("items", ops, &opts, "worker")
            .unwrap_err()
            .code,
        "HISTORY_CONFLICT"
    );
}

#[test]
fn batch_checks_use_pre_write_state_and_retries_do_not_recheck() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options()).unwrap();
    let id = store.create_tenant("checks").unwrap().id;
    let t = store.tenant(id).unwrap();
    t.create_collection("state").unwrap();
    let config = t
        .put("state", json!({"_key":"config","value":1}), None)
        .unwrap();
    let request = vec![
        WriteOp::Put {
            document: json!({"_key":"result","value":1}),
            if_rev: None,
        },
        WriteOp::Check {
            key: "result".into(),
            if_rev: 0,
        },
        WriteOp::Check {
            key: "config".into(),
            if_rev: config.rev,
        },
    ];
    assert_eq!(
        t.write_batch("state", request.clone()).unwrap_err().code,
        "HISTORY_REQUIRED"
    );
    let opts = WriteOptions {
        expected_history: Some(config.history),
        operation_id: Some("checked-write".into()),
        ..Default::default()
    };
    let result = t
        .write_batch_with("state", request.clone(), &opts, "writer")
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        t.get("state", "config").unwrap().unwrap()["_rev"],
        config.rev
    );
    t.put("state", json!({"_key":"config","value":2}), None)
        .unwrap();
    let seq = t.engine().seq();
    assert_eq!(
        t.write_batch_with("state", request.clone(), &opts, "writer")
            .unwrap()[0]
            .rev,
        result[0].rev
    );
    assert_eq!(t.engine().seq(), seq);
    let failed = vec![
        WriteOp::Put {
            document: json!({"_key":"must-not-commit"}),
            if_rev: None,
        },
        WriteOp::Check {
            key: "config".into(),
            if_rev: config.rev,
        },
    ];
    assert_eq!(
        t.write_batch_with(
            "state",
            failed,
            &WriteOptions {
                expected_history: Some(t.history()),
                ..Default::default()
            },
            "writer"
        )
        .unwrap_err()
        .code,
        "REVISION_CONFLICT"
    );
    assert!(t.get("state", "must-not-commit").unwrap().is_none());
    let check = vec![WriteOp::Check {
        key: "absent".into(),
        if_rev: 0,
    }];
    assert!(t.write_batch("state", check).unwrap().is_empty());
    assert_eq!(t.engine().seq(), seq);
    drop(t);
    drop(store);
    let store = Store::open(dir.path(), options()).unwrap();
    let t = store.tenant(id).unwrap();
    assert_eq!(
        t.write_batch_with("state", request, &opts, "writer")
            .unwrap()[0]
            .rev,
        result[0].rev
    );
}

#[test]
fn composed_reads_share_snapshot_during_writes_and_cursor_continuation() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options()).unwrap();
    let t = store
        .tenant(store.create_tenant("reads").unwrap().id)
        .unwrap();
    t.create_collection("state").unwrap();
    t.create_collection("metadata").unwrap();
    t.put("metadata", json!({"_key":"config","enabled":true}), None)
        .unwrap();
    let update = |value| {
        vec!["a", "b"]
            .into_iter()
            .map(|key| WriteOp::Put {
                document: json!({"_key":key,"value":value}),
                if_rev: None,
            })
            .collect()
    };
    t.write_batch("state", update(0)).unwrap();
    let writer = t.clone();
    let thread = std::thread::spawn(move || {
        for i in 1..=100 {
            writer.write_batch("state", update(i)).unwrap();
        }
    });
    let reads = vec![
        ReadOperation {
            collection: "state".into(),
            request: ReadRequest::Get { key: "a".into() },
        },
        ReadOperation {
            collection: "state".into(),
            request: ReadRequest::Query {
                query: Query {
                    limit: 2,
                    ..Default::default()
                },
            },
        },
        ReadOperation {
            collection: "state".into(),
            request: ReadRequest::MultiGet {
                keys: vec!["b".into(), "a".into(), "missing".into()],
            },
        },
        ReadOperation {
            collection: "metadata".into(),
            request: ReadRequest::Get {
                key: "config".into(),
            },
        },
    ];
    for _ in 0..150 {
        let result = serde_json::to_value(t.read_batch(&reads).unwrap()).unwrap();
        let value = &result["results"][0]["document"]["value"];
        for doc in result["results"][1]["result"]["documents"]
            .as_array()
            .unwrap()
        {
            assert_eq!(&doc["value"], value);
        }
        assert_eq!(&result["results"][2]["documents"][0]["value"], value);
        assert_eq!(&result["results"][2]["documents"][1]["value"], value);
        assert!(result["results"][2]["documents"][2].is_null());
        assert_eq!(result["results"][3]["document"]["enabled"], true);
        assert_eq!(
            result["results"][1]["result"]["snapshot_seq"],
            result["snapshot_seq"]
        );
    }
    thread.join().unwrap();
    let result = t
        .read_batch(&[ReadOperation {
            collection: "state".into(),
            request: ReadRequest::Query {
                query: Query {
                    limit: 1,
                    ..Default::default()
                },
            },
        }])
        .unwrap();
    let ReadResult::Query { result: first } = &result.results[0] else {
        panic!()
    };
    t.write_batch("state", update(999)).unwrap();
    let last = t
        .query(
            "state",
            Query {
                limit: 1,
                cursor: first.cursor.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(last.documents[0]["value"], 100);
    assert_eq!(last.snapshot_seq, result.snapshot_seq);
}

#[test]
fn composed_read_limits_are_aggregate_and_failure_releases_cursors() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options();
    opts.max_scan = 4;
    opts.max_tenant_requests = 1;
    opts.max_snapshots = 2;
    opts.max_read_keys = 8;
    let store = Store::open(dir.path(), opts).unwrap();
    let t = store
        .tenant(store.create_tenant("budgets").unwrap().id)
        .unwrap();
    t.create_collection("state").unwrap();
    for i in 0..4 {
        t.put("state", json!({"_key":i.to_string()}), None).unwrap();
    }
    let query = |limit| ReadOperation {
        collection: "state".into(),
        request: ReadRequest::Query {
            query: Query {
                limit,
                ..Default::default()
            },
        },
    };
    for _ in 0..4 {
        // The first page creates a cursor; the second exceeds the shared scan allowance.
        assert_eq!(
            t.read_batch(&[query(1), query(4)]).unwrap_err().code,
            "RESOURCE_EXHAUSTED"
        );
    }
    assert!(t.read_batch(&[query(4)]).is_ok());
    let many = ReadOperation {
        collection: "state".into(),
        request: ReadRequest::MultiGet {
            keys: vec!["0".into(); 5],
        },
    };
    assert_eq!(
        t.read_batch(&[many.clone(), many]).unwrap_err().code,
        "RESOURCE_EXHAUSTED"
    );
}

#[test]
fn generic_dataset_versions_keep_cursor_snapshot_across_publication_and_gc() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options()).unwrap();
    let id = store.create_tenant("datasets").unwrap().id;
    let t = store.tenant(id).unwrap();
    t.set_retention(1).unwrap();
    let mut previous = None;
    let mut versions = Vec::new();
    let mut cursor = None;
    for value in 1..=2 {
        let collection = format!("data_{value}");
        t.create_collection(&collection).unwrap();
        for key in ["a", "b", "c"] {
            t.put(&collection, json!({"_key":key,"value":value}), None)
                .unwrap();
        }
        let token = t.retry_context("publisher").unwrap();
        let published = t
            .publish_dataset_with(
                "config",
                Publication {
                    version: Uuid::new_v4(),
                    collection: collection.clone(),
                    lookup: String::new(),
                    expected_version: previous,
                    retry_token: Some(token.retry_token),
                },
                "publisher",
            )
            .unwrap();
        assert_eq!(
            t.put(&collection, json!({"_key":"x"}), None)
                .unwrap_err()
                .code,
            "IMMUTABLE_DATASET"
        );
        previous = Some(published.version);
        versions.push(published.version);
        if value == 1 {
            let page = t
                .query_published(
                    "config",
                    None,
                    Query {
                        limit: 1,
                        ..Default::default()
                    },
                )
                .unwrap();
            cursor = page.result.cursor;
        }
    }
    assert_eq!(
        t.read_published(
            "other",
            Some(versions[0]),
            &[ReadRequest::Get { key: "a".into() }]
        )
        .unwrap_err()
        .code,
        "VERSION_UNAVAILABLE"
    );
    assert_eq!(
        t.query_published(
            "config",
            Some(versions[1]),
            Query {
                limit: 1,
                cursor: cursor.clone(),
                ..Default::default()
            }
        )
        .unwrap_err()
        .code,
        "INVALID_ARGUMENT"
    );
    std::thread::sleep(std::time::Duration::from_millis(1150));
    assert_eq!(
        t.read_published(
            "config",
            Some(versions[0]),
            &[ReadRequest::Get { key: "a".into() }]
        )
        .unwrap_err()
        .code,
        "VERSION_UNAVAILABLE"
    );
    t.engine().flush().unwrap();
    t.engine().compact(true).unwrap();
    let second = t
        .query_published(
            "config",
            None,
            Query {
                limit: 1,
                cursor,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(second.version, versions[0]);
    assert_eq!(second.result.documents[0]["value"], 1);
    let last = t
        .query_published(
            "config",
            None,
            Query {
                limit: 1,
                cursor: second.result.cursor,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(last.result.documents[0]["value"], 1);
    let latest = serde_json::to_value(
        t.read_published(
            "config",
            None,
            &[
                ReadRequest::Get { key: "a".into() },
                ReadRequest::MultiGet {
                    keys: vec!["b".into(), "missing".into(), "b".into()],
                },
            ],
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(latest["result"]["results"][0]["document"]["value"], 2);
    assert!(latest["result"]["results"][1]["documents"][1].is_null());
    drop(t);
    drop(store);
    let store = Store::open(dir.path(), options()).unwrap();
    let t = store.tenant(id).unwrap();
    assert_eq!(
        t.published_dataset("config", None).unwrap().version,
        versions[1]
    );
}

#[tokio::test]
async fn generic_kv_http_routes_enforce_roles_tenants_and_publish_without_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options()).unwrap();
    let id = store.create_tenant("http-kv").unwrap().id;
    let other = store.create_tenant("other").unwrap().id;
    let t = store.tenant(id).unwrap();
    t.create_collection("data").unwrap();
    let config = t
        .put("data", json!({"_key":"config","enabled":true}), None)
        .unwrap();
    let mut config_auth = auth();
    let reader = "kv-reader-test-secret";
    config_auth.tokens.push(Token {
        sha256: http::token_hash(reader),
        subject: "reader".into(),
        tenant: Some(id),
        role: Role::Read,
    });
    let app = http::app(store, config_auth).unwrap();
    let admin = "middleware-secret-one";
    let base = format!("/v1/tenants/{id}");
    let request = json!({"operations":[
        {"collection":"data","request":{"op":"get","key":"config"}},
        {"collection":"data","request":{"op":"multi_get","keys":["config","absent"]}}
    ]});
    let (status, result) = call(
        &app,
        "POST",
        &format!("{base}/read_batch"),
        request.clone(),
        reader,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["results"][0]["document"]["enabled"], true);
    assert!(result["results"][1]["documents"][1].is_null());
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/v1/tenants/{other}/read_batch"),
            request,
            reader
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let batch = json!({"operations":[{"op":"check","key":"config","if_rev":config.rev},{"op":"put","document":{"_key":"new","value":42}}],"write_options":{"expected_history":config.history}});
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{base}/collections/data/batch"),
            batch.clone(),
            reader
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, result) = call(
        &app,
        "POST",
        &format!("{base}/collections/data/batch"),
        batch,
        admin,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result.as_array().unwrap().len(), 1);
    let publish = json!({"version":Uuid::new_v4(),"collection":"data","expected_version":null});
    assert_eq!(
        call(
            &app,
            "PUT",
            &format!("{base}/resources/catalog"),
            publish.clone(),
            reader
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, published) = call(
        &app,
        "PUT",
        &format!("{base}/resources/catalog"),
        publish,
        admin,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{published}");
    let request =
        json!({"version":published["version"],"operations":[{"op":"query","query":{"limit":1}}]});
    let (status, result) = call(
        &app,
        "POST",
        &format!("{base}/resources/catalog/read"),
        request.clone(),
        reader,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/v1/tenants/{other}/resources/catalog/read"),
            request,
            reader
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let query = json!({"version":published["version"],"query":{"limit":1,"cursor":result["result"]["results"][0]["result"]["cursor"]}});
    let (status, result) = call(
        &app,
        "POST",
        &format!("{base}/resources/catalog/query"),
        query,
        reader,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["result"]["documents"][0]["value"], 42);
}

#[test]
fn composed_reads_enforce_total_response_and_memory_limits() {
    for memory_limited in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = options();
        opts.max_response_bytes = 1 << 20;
        if memory_limited {
            opts.max_query_bytes = 7 << 20;
        }
        let store = Store::open(dir.path(), opts).unwrap();
        let t = store
            .tenant(store.create_tenant("bounded").unwrap().id)
            .unwrap();
        t.create_collection("data").unwrap();
        t.put(
            "data",
            json!({"_key":"large","text":"x".repeat(600_000)}),
            None,
        )
        .unwrap();
        let get = ReadOperation {
            collection: "data".into(),
            request: ReadRequest::Get {
                key: "large".into(),
            },
        };
        assert!(t.read_batch(std::slice::from_ref(&get)).is_ok());
        let error = t.read_batch(&[get.clone(), get]).unwrap_err();
        assert_eq!(error.code, "RESOURCE_EXHAUSTED");
        assert!(t.get("data", "large").unwrap().is_some());
    }
}

#[test]
fn request_read_controls_preserve_limits_cursors_and_independent_requests() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        dir.path(),
        Options {
            max_scan: 8,
            max_snapshots: 4,
            ..options()
        },
    )
    .unwrap();
    let t = store
        .tenant(store.create_tenant("read-controls").unwrap().id)
        .unwrap();
    t.create_collection("data").unwrap();
    for key in ["a", "b", "c", "d"] {
        t.put("data", json!({"_key":key,"n":1}), None).unwrap();
    }
    let strict = ReadOptions {
        allow_full_scan: false,
        ..Default::default()
    };
    let reader = t.read_with(&strict).unwrap();
    assert_eq!(
        reader.query("data", Query::default()).unwrap_err().code,
        "SCAN_NOT_ALLOWED"
    );
    assert!(reader.explain("data", &Query::default()).unwrap().scan);
    let indexed: Query =
        serde_json::from_value(json!({"filter":{"field":"/_key","op":"eq","value":"a"}})).unwrap();
    assert_eq!(
        reader.query("data", indexed).unwrap().documents[0]["_key"],
        "a"
    );
    let mixed: Query = serde_json::from_value(json!({"filter":{"or":[{"field":"/_key","op":"eq","value":"a"},{"field":"/n","op":"eq","value":1}]}})).unwrap();
    assert_eq!(
        reader.query("data", mixed).unwrap_err().code,
        "SCAN_NOT_ALLOWED"
    );
    let first = t
        .query(
            "data",
            Query {
                limit: 1,
                ..Default::default()
            },
        )
        .unwrap();
    let next = Query {
        limit: 1,
        cursor: first.cursor,
        ..Default::default()
    };
    let tiny = ReadOptions {
        max_scan: Some(1),
        ..Default::default()
    };
    assert_eq!(
        t.read_with(&tiny)
            .unwrap()
            .query("data", next.clone())
            .unwrap_err()
            .code,
        "RESOURCE_EXHAUSTED"
    );
    let second = t.query("data", next).unwrap();
    assert_eq!(second.documents[0]["_key"], "b");
    t.release_cursor(second.cursor.as_ref().unwrap()).unwrap();
    let operation = |limit| ReadOperation {
        collection: "data".into(),
        request: ReadRequest::Query {
            query: Query {
                limit,
                ..Default::default()
            },
        },
    };
    let limited = ReadOptions {
        max_scan: Some(4),
        ..Default::default()
    };
    for _ in 0..5 {
        assert_eq!(
            t.read_with(&limited)
                .unwrap()
                .read_batch(&[operation(1), operation(4)])
                .unwrap_err()
                .code,
            "RESOURCE_EXHAUSTED"
        );
    }
    assert!(
        t.read_with(&limited)
            .unwrap()
            .read_batch(&[operation(4)])
            .is_ok()
    );
    // In-memory version chains seek to the snapshot rather than spending the
    // scan allowance walking newer versions of the same logical key.
    t.create_collection("history").unwrap();
    for value in 0..12 {
        t.put("history", json!({"_key":"k","value":value}), None)
            .unwrap();
    }
    let hot = t
        .read_with(&ReadOptions {
            max_scan: Some(1000),
            ..Default::default()
        })
        .unwrap()
        .query("history", Query::default())
        .unwrap();
    assert_eq!(hot.documents[0]["value"], 11);
    for value in 0..12 {
        t.put(
            "history",
            json!({"_key":format!("other-{value}"),"value":value}),
            None,
        )
        .unwrap();
    }
    // A caller still cannot raise the configured scan cap over distinct rows.
    assert_eq!(
        t.read_with(&ReadOptions {
            max_scan: Some(1000),
            ..Default::default()
        })
        .unwrap()
        .query("history", Query::default())
        .unwrap_err()
        .code,
        "RESOURCE_EXHAUSTED"
    );
    let cancellation = ReadCancellation::default();
    let controlled = t
        .read_with(&ReadOptions {
            cancellation: Some(cancellation.clone()),
            ..Default::default()
        })
        .unwrap();
    cancellation.cancel();
    assert_eq!(controlled.get("data", "a").unwrap_err().code, "CANCELLED");
    assert_eq!(
        controlled
            .multi_get("data", &["a".into()])
            .unwrap_err()
            .code,
        "CANCELLED"
    );
    assert!(t.get("data", "a").unwrap().is_some());
    assert!(t.put("data", json!({"_key":"e"}), None).is_ok());
    let expired = t
        .read_with(&ReadOptions {
            timeout_ms: Some(1),
            ..Default::default()
        })
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert_eq!(
        expired.query("data", Query::default()).unwrap_err().code,
        "DEADLINE_EXCEEDED"
    );
    assert!(
        t.read_with(&ReadOptions {
            timeout_ms: Some(0),
            ..Default::default()
        })
        .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_read_controls_cover_dsl_waiting_and_dropped_requests() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        dir.path(),
        Options {
            max_tenant_requests: 1,
            ..options()
        },
    )
    .unwrap();
    let id = store.create_tenant("controls-http").unwrap().id;
    let t = store.tenant(id).unwrap();
    t.create_collection("data").unwrap();
    t.put("data", json!({"_key":"a"}), None).unwrap();
    let app = http::app(store, auth()).unwrap();
    let path = format!("/v1/tenants/{id}/collections/data");
    let token = "middleware-secret-one";
    let no_scan = [("x-tkv-allow-full-scan", "false")];
    let (status, result) = call_headers(
        &app,
        "POST",
        &format!("{path}/query"),
        json!({}),
        token,
        &no_scan,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{result}");
    assert_eq!(result["error"]["code"], "SCAN_NOT_ALLOWED");
    assert_eq!(
        call_headers(
            &app,
            "POST",
            &format!("{path}/explain"),
            json!({}),
            token,
            &no_scan
        )
        .await
        .1["scan"],
        true
    );
    let dsl = format!("/v1/tenants/{id}/dsl");
    assert_eq!(
        call_headers(
            &app,
            "POST",
            &dsl,
            json!({"statement":"from data | limit 1"}),
            token,
            &no_scan
        )
        .await
        .1["error"]["code"],
        "SCAN_NOT_ALLOWED"
    );
    for headers in [
        vec![("x-tkv-max-scan", "0")],
        vec![("x-tkv-max-scan", "1"), ("x-tkv-max-scan", "2")],
        vec![("x-tkv-allow-full-scan", "yes")],
    ] {
        assert_eq!(
            call_headers(
                &app,
                "POST",
                &format!("{path}/query"),
                json!({}),
                token,
                &headers
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        call_headers(
            &app,
            "PUT",
            &format!("{path}/documents"),
            json!({"document":{"_key":"forbidden"}}),
            token,
            &no_scan
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call_headers(
            &app,
            "POST",
            &dsl,
            json!({"statement":"put data {\"_key\":\"forbidden\"}"}),
            token,
            &no_scan
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert!(t.get("data", "forbidden").unwrap().is_none());
    let history = t.history();
    let headers = [
        ("x-tkv-min-history", history.as_str()),
        ("x-tkv-min-seq", "9999"),
        ("x-tkv-read-timeout-ms", "20"),
    ];
    let started = std::time::Instant::now();
    assert_eq!(
        call_headers(
            &app,
            "GET",
            &format!("{path}/documents/a"),
            json!({}),
            token,
            &headers
        )
        .await
        .1["error"]["code"],
        "DEADLINE_EXCEEDED"
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
    let waiting_app = app.clone();
    let waiting_path = format!("{path}/documents/a");
    let waiting = tokio::spawn(async move {
        call_headers(
            &waiting_app,
            "GET",
            &waiting_path,
            json!({}),
            token,
            &[
                ("x-tkv-min-history", history.as_str()),
                ("x-tkv-min-seq", "9999"),
            ],
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    // Wait until the blocked request owns the tenant admission slot.
    let start = std::time::Instant::now();
    loop {
        let status = call(
            &app,
            "GET",
            &format!("{path}/documents/a"),
            json!({}),
            token,
        )
        .await
        .0;
        if status == StatusCode::TOO_MANY_REQUESTS {
            break;
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(1));
        tokio::task::yield_now().await;
    }
    waiting.abort();
    let _ = waiting.await;
    let start = std::time::Instant::now();
    loop {
        if call(
            &app,
            "GET",
            &format!("{path}/documents/a"),
            json!({}),
            token,
        )
        .await
        .0 == StatusCode::OK
        {
            break;
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(1),
            "cancelled wait retained admission slot"
        );
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
}
