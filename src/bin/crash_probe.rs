use serde_json::json;
use tenant_kv::*;

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let store = Store::open(&args[1], Options::default()).unwrap();
    if args[3] == "backup-admission-error" || args[3] == "backup-terminal-error" {
        use tenant_kv::backup::*;
        let id = args[2].parse().unwrap();
        let config = serde_json::from_slice(&std::fs::read(&args[4]).unwrap()).unwrap();
        let service = BackupService::open(store, config).unwrap();
        let first = service.submit(id, BackupOperation::Create);
        if args[3] == "backup-admission-error" {
            assert_eq!(first.unwrap_err().code, "RESOURCE_EXHAUSTED");
        } else {
            let job = first.unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while service.job(id, job.id).unwrap().state != JobState::Interrupted {
                assert!(std::time::Instant::now() < deadline);
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        let jobs = service.jobs(id);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].state, JobState::Interrupted);
        assert_eq!(
            service
                .submit(id, BackupOperation::Create)
                .unwrap_err()
                .code,
            "BUSY"
        );
        let retry = service.retry(id, jobs[0].id).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let state = service.job(id, retry.id).unwrap();
            if matches!(
                state.state,
                JobState::Completed | JobState::Failed | JobState::Interrupted
            ) {
                assert_eq!(state.state, JobState::Completed, "{state:?}");
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(service.jobs(id).len(), 1);
        return;
    }
    if args[3] == "create" {
        store.create_tenant(&args[2]).unwrap();
        panic!("tenant creation failpoint did not trigger");
    }
    if args[3].starts_with("remote-") {
        let id = args[2].parse().unwrap();
        let config = serde_json::from_slice(&std::fs::read(&args[4]).unwrap()).unwrap();
        let service = tenant_kv::backup::BackupService::open(store, config).unwrap();
        let operation = match args[3].as_str() {
            "remote-restore" => tenant_kv::backup::BackupOperation::Restore {
                backup: args[5].parse().unwrap(),
            },
            "remote-import" => tenant_kv::backup::BackupOperation::Import {
                backup: args[5].parse().unwrap(),
            },
            _ => tenant_kv::backup::BackupOperation::Create,
        };
        let job = service.submit(id, operation).unwrap();
        loop {
            let state = service.job(id, job.id).unwrap();
            if matches!(
                state.state,
                tenant_kv::backup::JobState::Completed | tenant_kv::backup::JobState::Failed
            ) {
                panic!("failpoint did not trigger: {state:?}");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
    let tenant = store.tenant(args[2].parse().unwrap()).unwrap();
    if args[3] == "retention" {
        std::thread::sleep(std::time::Duration::from_secs(10));
        panic!("retention failpoint did not trigger");
    } else if args[3] == "receipt" {
        tenant
            .write_batch_with(
                "assets",
                vec![WriteOp::Put {
                    document: json!({"ip":"two","owner":"retry"}),
                    if_rev: None,
                }],
                &WriteOptions {
                    retry_token: None,
                    expected_history: Some(tenant.history()),
                    operation_id: Some("crash-retry".into()),
                },
                "probe",
            )
            .unwrap();
        tenant.engine().flush().unwrap();
    } else if args[3] == "identity" {
        let request = serde_json::from_slice(&std::fs::read(&args[4]).unwrap()).unwrap();
        let context = tenant_kv::identity::IdentityContext {
            source: "crash_probe".into(),
            actor: "recovery".into(),
            can_correct: true,
        };
        tenant.identity_write(&context, request).unwrap();
        tenant.engine().flush().unwrap();
        tenant.engine().compact(true).unwrap();
    } else if args[3] == "delete-error" {
        let token_file = store.root().join("test-peer-token");
        std::fs::write(&token_file, "test-only-delete-failure-peer-token").unwrap();
        let service = tenant_kv::replication::Service::open(
            store.clone(),
            tenant_kv::replication::Config {
                cluster: uuid::Uuid::new_v4(),
                role: tenant_kv::replication::Role::Primary,
                peer_url: "http://127.0.0.1:1".into(),
                token_file,
                poll_ms: 50,
                log_retention_bytes: 1 << 20,
            },
        )
        .unwrap();
        let id = tenant.id();
        drop(tenant);
        loop {
            match store.delete_tenant(id) {
                Err(e) if e.code == "BUSY" => {
                    std::thread::sleep(std::time::Duration::from_millis(10))
                }
                result => {
                    assert!(result.is_err());
                    break;
                }
            }
        }
        if let Ok(tenant) = store.tenant(id) {
            tenant
                .put(
                    "assets",
                    json!({"_key":"ack-after-delete-error","ip":"three"}),
                    None,
                )
                .unwrap();
            panic!(
                "acknowledged a write after an uncertain durable deletion; restart will discard it"
            );
        }
        assert_eq!(store.tenant(id).err().unwrap().code, "UNAVAILABLE");
        assert_eq!(store.close_tenant(id).unwrap_err().code, "UNAVAILABLE");
        assert_eq!(
            store.replication_positions().unwrap_err().code,
            "UNAVAILABLE"
        );
        assert_eq!(
            store.fence_primary(uuid::Uuid::new_v4()).unwrap_err().code,
            "UNAVAILABLE"
        );
        assert_eq!(
            store.replication_state().role,
            tenant_kv::replication::Role::Primary
        );
        let status = service.status().unwrap();
        assert!(status.tenants.iter().all(|entry| entry.info.id != id));
        assert!(status.unavailable.iter().any(|entry| entry.info.id == id));
        let healthy = store.create_tenant("healthy-after-delete-error").unwrap();
        store
            .tenant(healthy.id)
            .unwrap()
            .create_collection("assets")
            .unwrap();
    } else if args[3] == "restore" || args[3] == "restore-error" {
        let id = tenant.id();
        drop(tenant);
        loop {
            match store.restore(id, args[4].parse().unwrap()) {
                Err(e) if e.code == "BUSY" => {
                    std::thread::sleep(std::time::Duration::from_millis(10))
                }
                result if args[3] == "restore-error" => {
                    assert!(result.is_err());
                    assert_eq!(store.tenant(id).err().unwrap().code, "UNAVAILABLE");
                    assert_eq!(store.close_tenant(id).unwrap_err().code, "UNAVAILABLE");
                    break;
                }
                result => {
                    result.unwrap();
                    break;
                }
            }
        }
    } else if args[3] == "index" {
        tenant
            .create_index(
                "assets",
                IndexSpec {
                    name: "new-index".into(),
                    fields: vec![IndexField {
                        field: "/owner".into(),
                        order: Order::Asc,
                    }],
                    unique: false,
                    include: vec![],
                },
            )
            .unwrap();
    } else {
        tenant
            .put(
                "assets",
                json!({"_key":"new","ip":"two","owner":"new-owner"}),
                None,
            )
            .unwrap();
        if args[3] == "retry-flush" {
            assert_eq!(
                tenant.engine().flush().unwrap_err().code,
                "RESOURCE_EXHAUSTED"
            );
            tenant
                .put("assets", json!({"_key":"later","ip":"three"}), None)
                .unwrap();
        }
        tenant.engine().flush().unwrap();
        tenant.engine().flush().unwrap();
        tenant.engine().compact(true).unwrap();
    }
}
