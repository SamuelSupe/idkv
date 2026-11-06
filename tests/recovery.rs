use serde_json::json;
use std::{
    fs::{self, OpenOptions},
    io::Write,
};
use tenant_kv::*;

fn seed(root: &std::path::Path) -> uuid::Uuid {
    let store = Store::open(root, Options::default()).unwrap();
    let id = store.create_tenant("recovery").unwrap().id;
    let t = store.tenant(id).unwrap();
    t.create_collection("assets").unwrap();
    t.create_index(
        "assets",
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
    t.put(
        "assets",
        json!({"_key":"old","ip":"one","owner":"old-owner"}),
        None,
    )
    .unwrap();
    t.engine().flush().unwrap();
    id
}
#[test]
fn incomplete_tail_is_discarded_but_complete_corruption_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let id = seed(dir.path());
    let wal = fs::read_dir(dir.path().join("tenants").join(id.to_string()).join("wal"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut file = OpenOptions::new().append(true).open(&wal).unwrap();
    file.write_all(b"TXN").unwrap();
    file.sync_all().unwrap();
    drop(file);
    {
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let t = store.tenant(id).unwrap();
        assert!(t.get("assets", "old").unwrap().is_some());
        t.put("assets", json!({"_key":"new","ip":"two"}), None)
            .unwrap();
    }
    let mut bytes = fs::read(&wal).unwrap();
    assert!(bytes.len() > 100);
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(&wal, bytes).unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    assert_eq!(store.tenant(id).err().unwrap().code, "CORRUPTION");
    let other = store.create_tenant("healthy").unwrap();
    assert!(
        store
            .tenant(other.id)
            .unwrap()
            .create_collection("assets")
            .is_ok()
    );
}

#[cfg(feature = "failpoints")]
#[test]
fn uncertain_tenant_deletion_rejects_new_writes_until_restart() {
    let dir = tempfile::tempdir().unwrap();
    let id = seed(dir.path());
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
        .arg(dir.path())
        .arg(id.to_string())
        .arg("delete-error")
        .env("TKV_ERRORPOINT", "tenant_delete_journal_published")
        .status()
        .unwrap();
    assert!(
        status.success(),
        "uncertain deletion exposed a writable tenant"
    );
    let store = Store::open(dir.path(), Options::default()).unwrap();
    assert!(store.tenants().iter().all(|info| info.id != id));
    let healthy = store
        .tenants()
        .into_iter()
        .find(|info| info.name == "healthy-after-delete-error")
        .unwrap();
    assert!(store.tenant(healthy.id).unwrap().collections().is_ok());
}
#[test]
fn missing_or_foreign_sst_never_becomes_an_empty_database() {
    let dir = tempfile::tempdir().unwrap();
    let id = seed(dir.path());
    let sst = fs::read_dir(dir.path().join("tenants").join(id.to_string()).join("sst"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut bytes = fs::read(&sst).unwrap();
    bytes[8] ^= 1;
    fs::write(&sst, bytes).unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    assert_eq!(store.tenant(id).err().unwrap().code, "CORRUPTION");
    drop(store);
    fs::remove_file(&sst).unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    assert!(store.tenant(id).is_err());
}
#[cfg(feature = "failpoints")]
#[test]
fn process_crashes_preserve_atomic_documents_and_indexes() {
    for point in [
        "wal_torn_header",
        "wal_torn_payload",
        "wal_appended",
        "wal_synced",
        "manifest_published",
        "sst_synced",
        "compaction_synced",
        "index_ready",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let id = seed(dir.path());
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
            .arg(dir.path())
            .arg(id.to_string())
            .arg(if point == "index_ready" {
                "index"
            } else {
                "write"
            })
            .env("TKV_FAILPOINT", point)
            .env("RUST_BACKTRACE", "0")
            .status()
            .unwrap();
        assert!(!status.success(), "failpoint {point} did not trigger");
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let t = store.tenant(id).unwrap();
        assert_eq!(
            t.get("assets", "old").unwrap().unwrap()["owner"],
            "old-owner"
        );
        let row = t.get("assets", "new").unwrap();
        let result=t.query("assets",serde_json::from_value(json!({"filter":{"field":"/ip","op":"eq","value":"two"},"select":["/_key","/owner"]})).unwrap()).unwrap();
        assert_eq!(
            row.is_some(),
            !result.documents.is_empty(),
            "atomicity at {point}"
        );
        if [
            "wal_synced",
            "manifest_published",
            "sst_synced",
            "compaction_synced",
        ]
        .contains(&point)
        {
            assert!(row.is_some(), "durability at {point}");
        }
        if point.starts_with("wal_torn") {
            assert!(row.is_none());
        }
        if point == "index_ready" {
            assert!(
                t.indexes("assets")
                    .unwrap()
                    .iter()
                    .any(|i| i.spec.name == "new-index" && i.state == IndexState::Ready)
            );
        }
        t.engine().verify().unwrap();
    }
}

#[cfg(feature = "failpoints")]
#[test]
fn unpublished_flush_failure_is_retryable_without_checkpoint_gaps() {
    for point in ["sst_unpublished", "wal_rotate_created"] {
        let dir = tempfile::tempdir().unwrap();
        let id = seed(dir.path());
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
            .arg(dir.path())
            .arg(id.to_string())
            .arg("retry-flush")
            .env("TKV_ERRORPOINT", point)
            .status()
            .unwrap();
        assert!(status.success());
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let t = store.tenant(id).unwrap();
        for key in ["old", "new", "later"] {
            assert!(t.get("assets", key).unwrap().is_some());
        }
        t.engine().verify().unwrap();
    }
}

#[cfg(feature = "failpoints")]
#[test]
fn restore_journal_survives_both_directory_switch_crashes() {
    for point in [
        "restore_old_moved",
        "restore_new_moved",
        "restore_journal_published",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let id = seed(dir.path());
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let backup = store.backup(id).unwrap();
        let tenant = store.tenant(id).unwrap();
        tenant
            .put("assets", json!({"_key":"after-backup","ip":"later"}), None)
            .unwrap();
        let other = store.create_tenant("unaffected").unwrap().id;
        store
            .tenant(other)
            .unwrap()
            .create_collection("assets")
            .unwrap();
        store
            .tenant(other)
            .unwrap()
            .put("assets", json!({"_key":"keep"}), None)
            .unwrap();
        drop(tenant);
        drop(store);
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
            .arg(dir.path())
            .arg(id.to_string())
            .arg(if point == "restore_journal_published" {
                "restore-error"
            } else {
                "restore"
            })
            .arg(backup.to_string())
            .env(
                if point == "restore_journal_published" {
                    "TKV_ERRORPOINT"
                } else {
                    "TKV_FAILPOINT"
                },
                point,
            )
            .status()
            .unwrap();
        assert_eq!(status.success(), point == "restore_journal_published");
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let tenant = store.tenant(id).unwrap();
        assert!(tenant.get("assets", "old").unwrap().is_some());
        assert!(tenant.get("assets", "after-backup").unwrap().is_none());
        assert!(
            store
                .tenant(other)
                .unwrap()
                .get("assets", "keep")
                .unwrap()
                .is_some()
        );
        tenant.engine().verify().unwrap();
    }
}

#[cfg(feature = "failpoints")]
#[test]
fn temporal_corrections_history_heads_and_receipts_recover_atomically() {
    use tenant_kv::identity::*;
    for point in [
        "wal_torn_header",
        "wal_torn_payload",
        "wal_appended",
        "wal_synced",
        "sst_synced",
        "manifest_published",
        "compaction_synced",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let id = seed(dir.path());
        let context = IdentityContext {
            source: "crash_probe".into(),
            actor: "recovery".into(),
            can_correct: true,
        };
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let t = store.tenant(id).unwrap();
        t.identity_initialize(&context, IdentityConfig::default())
            .unwrap();
        let create: IdentityWrite = serde_json::from_value(
            json!({"action":"create_entity","operation_id":"entity","attributes":{}}),
        )
        .unwrap();
        let entity = t
            .identity_write(&context, create)
            .unwrap()
            .entity
            .unwrap()
            .entity_id;
        let identifier = Identifier {
            kind: "account".into(),
            namespace: "crm".into(),
            value: "one".into(),
        };
        let initial:IdentityWrite=serde_json::from_value(json!({"action":"bindings","operation_id":"initial","changes":[{"identifier":identifier,"expected_revision":"absent","effective":{"mode":"manual","from":"2026-01-01T00:00:00Z"},"change":{"action":"bind","entity_id":entity,"reason":"initial evidence"}}]})).unwrap();
        let first = t.identity_write(&context, initial).unwrap();
        t.engine().flush().unwrap();
        let input = dir.path().join("correction.json");
        fs::write(&input,serde_json::to_vec(&json!({"action":"bindings","operation_id":"correction","changes":[{"identifier":identifier,"expected_revision":first.timelines[0].revision,"effective":{"mode":"manual","from":"2026-02-01T00:00:00Z"},"change":{"action":"unbind","reason":"late cancellation"}}]})).unwrap()).unwrap();
        drop(t);
        drop(store);
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
            .arg(dir.path())
            .arg(id.to_string())
            .arg("identity")
            .arg(&input)
            .env("TKV_FAILPOINT", point)
            .status()
            .unwrap();
        assert!(!status.success(), "{point} did not trigger");
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let t = store.tenant(id).unwrap();
        let mut query = ResolveRequest {
            identifier: identifier.clone(),
            valid_at: "2026-03-01T00:00:00Z".into(),
            known_at: None,
        };
        let result = t.identity_resolve(&query).unwrap();
        let receipt = t.identity_operation("correction");
        if point.starts_with("wal_torn") {
            assert!(receipt.is_err());
        } else if point != "wal_appended" {
            assert!(receipt.is_ok(), "durability at {point}");
        }
        assert_eq!(
            result.binding.is_none(),
            receipt.is_ok(),
            "atomic correction at {point}"
        );
        if let Ok(receipt) = receipt {
            assert_eq!(result.knowledge_version, receipt.knowledge.version);
            assert_eq!(result.timeline_revision, receipt.timelines[0].revision);
        } else {
            assert_eq!(result.knowledge_version, first.knowledge.version);
        }
        query.known_at = Some(first.knowledge.version.to_string());
        assert_eq!(
            t.identity_resolve(&query)
                .unwrap()
                .binding
                .unwrap()
                .entity_id,
            entity
        );
        t.engine().verify().unwrap();
    }
}

#[cfg(feature = "failpoints")]
#[test]
fn durable_retry_receipt_and_generated_document_recover_together() {
    for point in ["wal_synced", "sst_synced"] {
        let dir = tempfile::tempdir().unwrap();
        let id = seed(dir.path());
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
            .arg(dir.path())
            .arg(id.to_string())
            .arg("receipt")
            .env("TKV_FAILPOINT", point)
            .status()
            .unwrap();
        assert!(!status.success(), "{point}");
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let t = store.tenant(id).unwrap();
        let before = t
            .query(
                "assets",
                serde_json::from_value(json!({"filter":{"field":"/ip","op":"eq","value":"two"}}))
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(before.documents.len(), 1);
        let seq = t.engine().seq();
        let receipt = t
            .write_batch_with(
                "assets",
                vec![WriteOp::Put {
                    document: json!({"ip":"two","owner":"retry"}),
                    if_rev: None,
                }],
                &WriteOptions {
                    retry_token: None,
                    expected_history: Some(t.history()),
                    operation_id: Some("crash-retry".into()),
                },
                "probe",
            )
            .unwrap();
        assert_eq!(receipt[0].key, before.documents[0]["_key"]);
        assert_eq!(t.engine().seq(), seq);
    }
}

#[test]
fn retention_cleanup_recovers_before_and_after_atomic_progress_commits() {
    for point in ["retention_before_commit", "retention_after_commit"] {
        let dir = tempfile::tempdir().unwrap();
        let store = tenant_kv::Store::open(dir.path(), tenant_kv::Options::default()).unwrap();
        let id = store.create_tenant("retention-crash").unwrap().id;
        let t = store.tenant(id).unwrap();
        t.create_collection("items").unwrap();
        t.set_retention(1).unwrap();
        let c = t.retry_context("worker").unwrap();
        let opts = tenant_kv::WriteOptions {
            operation_id: Some("once".into()),
            expected_history: Some(c.history),
            retry_token: Some(c.retry_token),
        };
        let ops = vec![tenant_kv::WriteOp::Put {
            document: serde_json::json!({"v":"confirmed"}),
            if_rev: None,
        }];
        let first = t
            .write_batch_with("items", ops.clone(), &opts, "worker")
            .unwrap();
        drop(t);
        drop(store);
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
            .arg(dir.path())
            .arg(id.to_string())
            .arg("retention")
            .env("TKV_FAILPOINT", point)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(6), "{point}: {status}");
        let store = tenant_kv::Store::open(dir.path(), tenant_kv::Options::default()).unwrap();
        let t = store.tenant(id).unwrap();
        assert!(t.get("items", &first[0].key).unwrap().is_some());
        assert_eq!(
            t.write_batch_with("items", ops, &opts, "worker")
                .unwrap_err()
                .code,
            "IDEMPOTENCY_EXPIRED"
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while t.retention_status().unwrap()["gc"]["reclaimed_records"]
            .as_u64()
            .unwrap_or(0)
            < 2
        {
            assert!(
                std::time::Instant::now() < deadline,
                "cleanup failed to resume: {point}"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        t.engine().flush().unwrap();
        t.engine().compact(true).unwrap();
        t.engine().verify().unwrap();
    }
}
