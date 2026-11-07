use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tenant_kv::{
    http::{self, AuthConfig, Role, Token},
    *,
};
use tower::ServiceExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_bodies_do_not_consume_other_tenants_execution_capacity() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let a = store.create_tenant("slow-a").unwrap().id;
    let b = store.create_tenant("slow-b").unwrap().id;
    let c = store.create_tenant("healthy-c").unwrap().id;
    let secret = "slow-body-regression-secret";
    let app = http::app(
        store,
        AuthConfig {
            tokens: vec![Token {
                subject: "admin".into(),
                sha256: http::token_hash(secret),
                tenant: None,
                role: Role::Admin,
            }],
        },
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(axum::serve(listener, app.clone()).into_future());
    let mut clients = Vec::new();
    for id in [a, b] {
        for _ in 0..32 {
            let mut client = std::net::TcpStream::connect(address).unwrap();
            write!(client, "POST /v1/tenants/{id}/collections HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {secret}\r\nContent-Length: 100\r\n\r\n").unwrap();
            clients.push(client);
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let (status, body) = call(
        &app,
        "GET",
        &format!("/v1/tenants/{c}"),
        Some(secret),
        json!({}),
    )
    .await;
    drop(clients);
    server.abort();
    let _ = server.await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn local_backup_deletion_requires_admin_and_preserves_tenant_scope() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let a = store.create_tenant("backup-a").unwrap().id;
    let b = store.create_tenant("backup-b").unwrap().id;
    let tenant = store.tenant(a).unwrap();
    tenant.create_collection("docs").unwrap();
    let receipt = tenant
        .put("docs", json!({"_key":"ack","value":7}), None)
        .unwrap();
    let backup_a = store.backup(a).unwrap();
    let backup_b = store.backup(b).unwrap();
    let admin = "local-backup-admin-secret";
    let reader = "local-backup-reader-secret";
    let app = http::app(
        store.clone(),
        AuthConfig {
            tokens: vec![
                Token {
                    subject: "admin-a".into(),
                    sha256: http::token_hash(admin),
                    tenant: Some(a),
                    role: Role::Admin,
                },
                Token {
                    subject: "reader-a".into(),
                    sha256: http::token_hash(reader),
                    tenant: Some(a),
                    role: Role::Read,
                },
            ],
        },
    )
    .unwrap();
    let route = format!("/v1/tenants/{a}/backup/{backup_a}");
    assert_eq!(
        call(&app, "DELETE", &route, Some(reader), json!({}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/v1/tenants/{b}/backup/{backup_b}"),
            Some(admin),
            json!({})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/v1/tenants/{a}/backup/{backup_b}"),
            Some(admin),
            json!({})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "DELETE", &route, Some(admin), json!({})).await.0,
        StatusCode::OK
    );
    assert!(dir.path().join(format!("backups/{b}/{backup_b}")).exists());
    assert_eq!(
        tenant.get("docs", "ack").unwrap().unwrap()["_rev"],
        receipt.rev
    );
}

#[cfg(unix)]
#[tokio::test]
async fn server_exposes_not_ready_before_closing_its_listener_on_sigterm() {
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let auth_path = dir.path().join("auth.json");
    std::fs::write(
        &auth_path,
        serde_json::to_vec(&AuthConfig {
            tokens: vec![Token {
                subject: "shutdown-test".into(),
                sha256: http::token_hash("shutdown-test-secret-123456"),
                tenant: None,
                role: Role::Admin,
            }],
        })
        .unwrap(),
    )
    .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut child = Child(
        std::process::Command::new(env!("CARGO_BIN_EXE_tenant-kv"))
            .arg("serve")
            .arg("--root")
            .arg(dir.path().join("data"))
            .arg("--auth")
            .arg(auth_path)
            .arg("--listen")
            .arg(address.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(1))
        .build()
        .unwrap();
    let url = format!("http://{address}/ready");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    loop {
        if client
            .get(&url)
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &child.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let mut observed = false;
    while child.0.try_wait().unwrap().is_none() {
        if let Ok(response) = client.get(&url).send().await
            && response.status() == StatusCode::SERVICE_UNAVAILABLE
        {
            assert_eq!(response.json::<Value>().await.unwrap()["draining"], true);
            observed = true;
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(observed, "the listener closed before publishing not-ready");
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let bytes = if method == "GET" {
        Vec::new()
    } else {
        serde_json::to_vec(&body).unwrap()
    };
    let response = app
        .clone()
        .oneshot(request.body(Body::from(bytes)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn management_history_headers_do_not_reopen_closed_tenants() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let id = store.create_tenant("management-history").unwrap().id;
    let token = "management-history-admin-123456";
    let app = http::app(
        store.clone(),
        AuthConfig {
            tokens: vec![Token {
                subject: "management".into(),
                sha256: http::token_hash(token),
                tenant: None,
                role: Role::Admin,
            }],
        },
    )
    .unwrap();
    let base = format!("/v1/tenants/{id}");
    store.tenant(id).unwrap().create_collection("docs").unwrap();
    let (status, value) = call(
        &app,
        "POST",
        &format!("{base}/close"),
        Some(token),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(
        store.status(id).unwrap()["open"],
        false,
        "close reopened the engine while encoding its response"
    );
    let (status, value) = call(&app, "GET", &base, Some(token), json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["open"], false);
    assert_eq!(store.status(id).unwrap()["open"], false);
    let (status, value) = call(&app, "PATCH", &base, Some(token), json!({
        "memory_bytes":Options::default().tenant_memory_bytes, "disk_bytes":Options::default().tenant_disk_bytes,
    })).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(store.status(id).unwrap()["open"], false);
    let backup = store.backup(id).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while store.close_tenant(id).is_err() {
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let (status, value) = call(
        &app,
        "POST",
        &format!("{base}/restore"),
        Some(token),
        json!({"backup":backup}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(store.status(id).unwrap()["open"], false);
}

#[tokio::test]
async fn document_history_never_controls_server_history_headers() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store
        .tenant(store.create_tenant("history-header").unwrap().id)
        .unwrap();
    t.create_collection("docs").unwrap();
    let secret = "history-header-reader-123456";
    let app = http::app(
        store.clone(),
        AuthConfig {
            tokens: vec![Token {
                subject: "reader".into(),
                sha256: http::token_hash(secret),
                tenant: Some(t.id()),
                role: Role::Read,
            }],
        },
    )
    .unwrap();
    for (key, history) in [("forged", "forged-token"), ("invalid", "历史\n")] {
        t.put("docs", json!({"_key":key,"history":history}), None)
            .unwrap();
        let base = format!("/v1/tenants/{}/collections/docs", t.id());
        for (method, path, body) in [
            ("GET", format!("{base}/documents/{key}"), json!(null)),
            ("POST", format!("{base}/multi_get"), json!({"keys":[key]})),
            (
                "POST",
                format!("/v1/tenants/{}/dsl", t.id()),
                json!({"statement":"get docs $key","params":{"key":key}}),
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header("authorization", format!("Bearer {secret}"))
                        .header("content-type", "application/json")
                        .body(Body::from(if method == "GET" {
                            vec![]
                        } else {
                            serde_json::to_vec(&body).unwrap()
                        }))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()["x-tkv-history"], t.history());
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert!(
                value
                    .to_string()
                    .contains(&serde_json::to_string(history).unwrap())
            );
        }
    }
}

#[tokio::test]
async fn cursor_quotas_leave_point_reads_and_other_principals_available() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        dir.path(),
        Options {
            max_snapshots: 8,
            ..Default::default()
        },
    )
    .unwrap();
    let t = store
        .tenant(store.create_tenant("cursor-reserve").unwrap().id)
        .unwrap();
    t.create_collection("docs").unwrap();
    for i in 0..3 {
        t.put("docs", json!({"_key":i.to_string()}), None).unwrap();
    }
    let tokens = [
        "cursor-reader-a-123456",
        "cursor-reader-b-123456",
        "cursor-reader-c-123456",
    ];
    let app = http::app(
        store.clone(),
        AuthConfig {
            tokens: tokens
                .iter()
                .map(|secret| Token {
                    subject: secret.to_string(),
                    sha256: http::token_hash(secret),
                    tenant: Some(t.id()),
                    role: Role::Read,
                })
                .collect(),
        },
    )
    .unwrap();
    let base = format!("/v1/tenants/{}/collections/docs", t.id());
    let mut first = None;
    for secret in &tokens[..2] {
        for _ in 0..2 {
            let (status, value) = call(
                &app,
                "POST",
                &format!("{base}/query"),
                Some(secret),
                json!({"limit":1}),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{value}");
            first.get_or_insert(value["cursor"].as_str().unwrap().to_string());
        }
        assert_eq!(
            call(
                &app,
                "POST",
                &format!("{base}/query"),
                Some(secret),
                json!({"limit":1})
            )
            .await
            .0,
            StatusCode::TOO_MANY_REQUESTS
        );
    }
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{base}/query"),
            Some(tokens[2]),
            json!({"limit":1})
        )
        .await
        .0,
        StatusCode::TOO_MANY_REQUESTS
    );
    for secret in tokens {
        assert_eq!(
            call(
                &app,
                "GET",
                &format!("{base}/documents/0"),
                Some(secret),
                json!({})
            )
            .await
            .0,
            StatusCode::OK
        );
    }
    t.release_cursor(first.as_ref().unwrap()).unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{base}/query"),
            Some(tokens[2]),
            json!({"limit":1})
        )
        .await
        .0,
        StatusCode::OK
    );
}
#[tokio::test]
async fn authenticated_http_workflow_and_cross_tenant_cursor_denial() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let a = store.create_tenant("a").unwrap();
    let b = store.create_tenant("b").unwrap();
    let admin = "admin-test-secret-123456";
    let reader = "reader-test-secret-123456";
    let writer = "writer-test-secret-123456";
    let app = http::app(
        store,
        AuthConfig {
            tokens: vec![
                Token {
                    subject: "test-service".into(),
                    sha256: http::token_hash(admin),
                    tenant: None,
                    role: Role::Admin,
                },
                Token {
                    subject: "test-service".into(),
                    sha256: http::token_hash(reader),
                    tenant: Some(a.id),
                    role: Role::Read,
                },
                Token {
                    subject: "test-service".into(),
                    sha256: http::token_hash(writer),
                    tenant: Some(a.id),
                    role: Role::Write,
                },
            ],
        },
    )
    .unwrap();
    let base = format!("/v1/tenants/{}/collections/assets", a.id);
    assert_eq!(
        call(&app, "GET", "/v1/tenants", None, json!({})).await.0,
        StatusCode::UNAUTHORIZED
    );
    for id in [a.id, b.id] {
        assert_eq!(
            call(
                &app,
                "POST",
                &format!("/v1/tenants/{id}/collections"),
                Some(admin),
                json!({"name":"assets"})
            )
            .await
            .0,
            StatusCode::OK
        );
    }
    assert_eq!(
        call(
            &app,
            "PUT",
            &format!("{base}/documents"),
            Some(reader),
            json!({"document":{"_key":"x"}})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    for i in 0..3 {
        let (status, value) = call(
            &app,
            "PUT",
            &format!("{base}/documents"),
            Some(writer),
            json!({"document":{"_key":i.to_string(),"ip":format!("ip-{i}"),"owner":"test"}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
    }
    let (status, index) = call(
        &app,
        "POST",
        &format!("{base}/indexes"),
        Some(admin),
        json!({"name":"ip","fields":[{"field":"/ip"}],"unique":true,"include":["/owner"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{index}");
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{base}/lookups"),
            Some(admin),
            json!({"name":"owner","index":"ip","select":["/owner"]})
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, result) = call(
        &app,
        "POST",
        &format!("{base}/lookups/owner/execute"),
        Some(reader),
        json!({"inputs":[{"ip":"ip-1"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["matches"][0][0]["owner"], "test");
    let (_, page) = call(
        &app,
        "POST",
        &format!("{base}/query"),
        Some(reader),
        json!({"limit":1}),
    )
    .await;
    let cursor = page["cursor"].as_str().unwrap();
    let other = format!("/v1/tenants/{}/collections/assets/query", b.id);
    assert_eq!(
        call(
            &app,
            "POST",
            &other,
            Some(reader),
            json!({"limit":1,"cursor":cursor})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &other,
            Some(admin),
            json!({"limit":1,"cursor":cursor})
        )
        .await
        .0,
        StatusCode::GONE
    );
    let (_, page2) = call(
        &app,
        "POST",
        &format!("{base}/query"),
        Some(reader),
        json!({"limit":1,"cursor":cursor}),
    )
    .await;
    assert_eq!(page2["documents"][0]["_key"], "1");
    let next_cursor = page2["cursor"].as_str().unwrap();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/v1/tenants/{}/cursors/{next_cursor}", b.id),
            Some(reader),
            json!({})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/v1/tenants/{}/cursors/{next_cursor}", a.id),
            Some(reader),
            json!({})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{base}/query"),
            Some(reader),
            json!({"limit":1,"cursor":next_cursor})
        )
        .await
        .0,
        StatusCode::GONE
    );
    assert_eq!(
        call(&app, "GET", "/openapi.json", None, json!({})).await.1["openapi"],
        "3.1.0"
    );
    let body = format!(
        "{{\"document\":{{\"payload\":[{}0]}}}}",
        "0,".repeat(3_000_000)
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("{base}/documents"))
                .header("authorization", format!("Bearer {writer}"))
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap()["error"]["code"],
        "RESOURCE_EXHAUSTED"
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{base}/documents/1"),
            Some(reader),
            json!({})
        )
        .await
        .0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn remote_backup_jobs_require_admin_and_preserve_tenant_scope() {
    use tenant_kv::backup::{BackupConfig, BackupService};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data"), Options::default()).unwrap();
    let a = store.create_tenant("a").unwrap().id;
    let b = store.create_tenant("b").unwrap().id;
    store
        .tenant(a)
        .unwrap()
        .create_collection("assets")
        .unwrap();
    store
        .tenant(a)
        .unwrap()
        .put("assets", json!({"_key":"a","owner":"original"}), None)
        .unwrap();
    let cfg = BackupConfig {
        service: "fs".into(),
        options: std::collections::HashMap::from([(
            "root".into(),
            dir.path().join("remote").to_string_lossy().into(),
        )]),
        max_backup_bytes: 64 << 20,
        max_staging_bytes: 256 << 20,
        reserve_free_bytes: 0,
        ..Default::default()
    };
    let service = BackupService::open(store.clone(), cfg).unwrap();
    let tokens = [
        ("global-backup-admin-secret", None, Role::Admin),
        ("tenant-backup-admin-secret", Some(a), Role::Admin),
        ("tenant-backup-reader-secret", Some(a), Role::Read),
        ("tenant-backup-writer-secret", Some(a), Role::Write),
    ];
    let app = http::app_with_backups(
        store.clone(),
        AuthConfig {
            tokens: tokens
                .iter()
                .map(|(key, tenant, role)| Token {
                    subject: "test-service".into(),
                    sha256: http::token_hash(key),
                    tenant: *tenant,
                    role: role.clone(),
                })
                .collect(),
        },
        Some(service),
    )
    .unwrap();
    let base = format!("/v1/tenants/{a}");
    for token in [tokens[2].0, tokens[3].0] {
        for method in ["GET", "POST"] {
            assert_eq!(
                call(
                    &app,
                    method,
                    &format!("{base}/remote-backups"),
                    Some(token),
                    json!({})
                )
                .await
                .0,
                StatusCode::FORBIDDEN
            );
        }
    }
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/v1/tenants/{b}/remote-backups"),
            Some(tokens[1].0),
            json!({})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, job) = call(
        &app,
        "POST",
        &format!("{base}/remote-backups"),
        Some(tokens[1].0),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{job}");
    let job_path = format!("{base}/backup-jobs/{}", job["id"].as_str().unwrap());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let (status, current) = call(&app, "GET", &job_path, Some(tokens[1].0), json!({})).await;
        assert_eq!(status, StatusCode::OK);
        if current["state"] == "completed" {
            break;
        }
        assert!(
            current["state"] != "failed" && std::time::Instant::now() < deadline,
            "{current}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        call(
            &app,
            "GET",
            &format!(
                "/v1/tenants/{b}/backup-jobs/{}",
                job["id"].as_str().unwrap()
            ),
            Some(tokens[0].0),
            json!({})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "GET", &job_path, Some(tokens[2].0), json!({}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let (status, list) = call(
        &app,
        "GET",
        &format!("{base}/remote-backups"),
        Some(tokens[1].0),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list[0]["backup"], job["backup"]);
    assert_eq!(
        call(&app, "DELETE", &job_path, Some(tokens[1].0), json!({}))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, "GET", &job_path, Some(tokens[1].0), json!({}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{base}/remote-backups"),
            Some(tokens[1].0),
            json!({})
        )
        .await
        .1[0]["backup"],
        job["backup"]
    );
    assert_eq!(
        store
            .tenant(a)
            .unwrap()
            .get("assets", "a")
            .unwrap()
            .unwrap()["owner"],
        "original"
    );
}

#[tokio::test]
async fn dsl_http_authorizes_parsed_commands_and_reuses_read_only_cached_syntax() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let a = store.create_tenant("a").unwrap().id;
    let b = store.create_tenant("b").unwrap().id;
    store
        .tenant(a)
        .unwrap()
        .create_collection("assets")
        .unwrap();
    let reader = "dsl-reader-secret-123456";
    let writer = "dsl-writer-secret-123456";
    let app = http::app(
        store.clone(),
        AuthConfig {
            tokens: vec![
                Token {
                    subject: "test-service".into(),
                    sha256: http::token_hash(reader),
                    tenant: Some(a),
                    role: Role::Read,
                },
                Token {
                    subject: "test-service".into(),
                    sha256: http::token_hash(writer),
                    tenant: Some(a),
                    role: Role::Write,
                },
            ],
        },
    )
    .unwrap();
    let path = format!("/v1/tenants/{a}/dsl");
    let put = json!({"statement":"# leading comment\nput assets $doc if_rev 0","params":{"doc":{"_key":"a","owner":"platform"}}});
    assert_eq!(
        call(&app, "POST", &path, Some(reader), put.clone()).await.0,
        StatusCode::FORBIDDEN
    );
    assert!(
        store
            .tenant(a)
            .unwrap()
            .get("assets", "a")
            .unwrap()
            .is_none()
    );
    let (status, write) = call(&app, "POST", &path, Some(writer), put.clone()).await;
    assert_eq!(status, StatusCode::OK, "{write}");
    // Cache warmup by a writer must not grant a reader the right to execute it.
    assert_eq!(
        call(&app, "POST", &path, Some(reader), put).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/v1/tenants/{b}/dsl"),
            Some(writer),
            json!({"statement":"get assets \"a\""})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(store.status(b).unwrap()["open"], false);
    let query = json!({"statement":"from assets | where owner == $owner | fields _key","params":{"owner":"platform"}});
    for _ in 0..2 {
        let (status, value) = call(&app, "POST", &path, Some(reader), query.clone()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["documents"], json!([{"_key":"a"}]));
    }
    let (status, value) = call(
        &app,
        "POST",
        &path,
        Some(reader),
        json!({"statement":"from assets\n| where owner = \"x\""}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["error"]["location"]["line"], 2);
    let (status, _) = call(
        &app,
        "POST",
        &path,
        Some(writer),
        json!({"statement":"delete assets \"a\"; get assets \"a\""}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        store
            .tenant(a)
            .unwrap()
            .get("assets", "a")
            .unwrap()
            .is_some()
    );
    let (status, missing) = call(
        &app,
        "POST",
        &path,
        Some(reader),
        json!({"statement":"get assets \"missing\""}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(missing, Value::Null);
    assert!(
        store.tenant(a).unwrap().dsl_cache_stats()["hits"]
            .as_u64()
            .unwrap()
            > 0
    );
}

#[tokio::test]
async fn identity_http_authentication_managed_writes_and_unforgeable_provenance() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let a = store.create_tenant("identity-a").unwrap().id;
    let b = store.create_tenant("identity-b").unwrap().id;
    let admin = "identity-admin-test-token";
    let reader = "identity-reader-test-token";
    let writer = "identity-writer-test-token";
    let app = http::app(
        store,
        AuthConfig {
            tokens: vec![
                Token {
                    subject: "test-service".into(),
                    sha256: http::token_hash(admin),
                    tenant: Some(a),
                    role: Role::Admin,
                },
                Token {
                    subject: "test-service".into(),
                    sha256: http::token_hash(reader),
                    tenant: Some(a),
                    role: Role::Read,
                },
                Token {
                    subject: "test-service".into(),
                    sha256: http::token_hash(writer),
                    tenant: Some(a),
                    role: Role::Write,
                },
            ],
        },
    )
    .unwrap();
    let base = format!("/v1/tenants/{a}/identity");
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{base}/initialize"),
            Some(writer),
            json!({})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{base}/initialize"),
            Some(admin),
            json!({})
        )
        .await
        .0,
        StatusCode::OK
    );
    let create =
        json!({"action":"create_entity","operation_id":"entity","attributes":{"name":"Alice"}});
    let (status, receipt) = call(
        &app,
        "POST",
        &format!("{base}/write"),
        Some(admin),
        create.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{receipt}");
    assert_eq!(receipt["knowledge"]["source"], "http");
    assert!(
        receipt["knowledge"]["actor"]
            .as_str()
            .unwrap()
            .eq("test-service")
    );
    assert_ne!(receipt["knowledge"]["actor"], http::token_hash(admin));
    let mut forged = create;
    forged["source"] = json!("verified-connector");
    assert_eq!(
        call(&app, "POST", &format!("{base}/write"), Some(admin), forged)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let id = json!({"kind":"account","namespace":"crm","value":"10086"});
    let binding = json!({"action":"bindings","operation_id":"bind","changes":[{"identifier":id,"expected_revision":"absent","effective":{"mode":"manual","from":"2026-01-01T00:00:00Z"},"change":{"action":"bind","entity_id":receipt["entity"]["entity_id"],"reason":"source initialization"}}]});
    let mut ambiguous_time = binding.clone();
    ambiguous_time["changes"][0]["effective"]["mode"] = json!("auto");
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{base}/write"),
            Some(admin),
            ambiguous_time
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let mut malformed_revision = binding.clone();
    malformed_revision["changes"][0]["expected_revision"] = json!({"absent":null});
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{base}/write"),
            Some(admin),
            malformed_revision
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let mut forged = binding.clone();
    forged["changes"][0]["change"]["assurance"] = json!("verified");
    assert_eq!(
        call(&app, "POST", &format!("{base}/write"), Some(admin), forged)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let (status, written) = call(
        &app,
        "POST",
        &format!("{base}/write"),
        Some(admin),
        binding.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{written}");
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{base}/write"),
            Some(writer),
            binding
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let request = json!({"identifier":id,"valid_at":"2026-01-02T00:00:00Z"});
    let (status, result) = call(
        &app,
        "POST",
        &format!("{base}/resolve"),
        Some(reader),
        request.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(
        result["binding"]["entity_id"],
        receipt["entity"]["entity_id"]
    );
    assert_eq!(result["binding"]["assurance"], "asserted");
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/v1/tenants/{b}/identity/resolve"),
            Some(reader),
            request
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, error) = call(
        &app,
        "POST",
        &format!("/v1/tenants/{a}/dsl"),
        Some(writer),
        json!({"statement":"put identity_temporal {\"_key\":\"head:knowledge\"}"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert_eq!(error["error"]["code"], "MANAGED_COLLECTION");
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{base}/operations/bind"),
            Some(reader),
            json!({})
        )
        .await
        .1,
        written
    );
}
