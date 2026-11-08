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
fn uncertain_backup_publication_retries_the_same_job_without_duplicate_restart_state() {
    for (mode, point) in [
        ("backup-admission-error", "backup_job_queued_published"),
        ("backup-terminal-error", "backup_job_terminal_publish"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let id = seed(&root);
        let config = tenant_kv::backup::BackupConfig {
            service: "fs".into(),
            options: std::collections::HashMap::from([(
                "root".into(),
                dir.path().join("remote").to_string_lossy().into(),
            )]),
            ..Default::default()
        };
        let path = dir.path().join("backup.json");
        fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
            .arg(&root)
            .arg(id.to_string())
            .arg(mode)
            .arg(path)
            .env("TKV_ERRORPOINT", point)
            .status()
            .unwrap();
        assert!(status.success());
        let store = Store::open(&root, Options::default()).unwrap();
        let service = tenant_kv::backup::BackupService::open(store.clone(), config).unwrap();
        assert_eq!(service.jobs(id).len(), 1);
        assert_eq!(
            service.jobs(id)[0].state,
            tenant_kv::backup::JobState::Completed
        );
        assert_eq!(
            store
                .tenant(id)
                .unwrap()
                .get("assets", "old")
                .unwrap()
                .unwrap()["owner"],
            "old-owner"
        );
    }
}

#[test]
fn healthy_reopens_reuse_synced_wals_and_unsealed_recovery_rewrites_them() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempfile::tempdir().unwrap();
    let id = seed(dir.path());
    let expected = {
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let tenant = store.tenant(id).unwrap();
        tenant
            .put("assets", json!({"_key":"tail","ip":"tail-ip"}), None)
            .unwrap();
        tenant.get("assets", "tail").unwrap().unwrap()
    };
    let wal = fs::read_dir(dir.path().join("tenants").join(id.to_string()).join("wal"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let old_inode = wal.metadata().unwrap().ino();
    for _ in 0..2 {
        let store = Store::open(dir.path(), Options::default()).unwrap();
        assert_eq!(
            store
                .tenant(id)
                .unwrap()
                .get("assets", "tail")
                .unwrap()
                .unwrap(),
            expected
        );
        assert_eq!(wal.metadata().unwrap().ino(), old_inode);
    }
    fs::remove_file(wal.parent().unwrap().parent().unwrap().join("WAL-SYNC")).unwrap();
    let unfinished = wal.with_extension("recover");
    fs::write(&unfinished, b"interrupted recovery output").unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    assert_eq!(
        store
            .tenant(id)
            .unwrap()
            .get("assets", "tail")
            .unwrap()
            .unwrap(),
        expected
    );
    assert_ne!(wal.metadata().unwrap().ino(), old_inode);
    assert!(!unfinished.exists());
}

#[cfg(feature = "failpoints")]
#[test]
fn recovery_rewrite_survives_abort_before_and_after_publication() {
    for point in ["wal_recovery_written", "wal_recovery_published"] {
        let dir = tempfile::tempdir().unwrap();
        let id = seed(dir.path());
        let expected = {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            let tenant = store.tenant(id).unwrap();
            tenant
                .put("assets", json!({"_key":"tail","ip":"tail-ip"}), None)
                .unwrap();
            tenant.get("assets", "tail").unwrap().unwrap()
        };
        fs::remove_file(
            dir.path()
                .join("tenants")
                .join(id.to_string())
                .join("WAL-SYNC"),
        )
        .unwrap();
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
            .arg(dir.path())
            .arg(id.to_string())
            .arg("write")
            .env("TKV_FAILPOINT", point)
            .status()
            .unwrap();
        assert!(!status.success());
        let store = Store::open(dir.path(), Options::default()).unwrap();
        assert_eq!(
            store
                .tenant(id)
                .unwrap()
                .get("assets", "tail")
                .unwrap()
                .unwrap(),
            expected
        );
    }
}

#[test]
fn review0732_tail_garbage_requires_explicit_repair_before_future_writes() {
    for sealed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let id = seed(dir.path());
        let expected = {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            let tenant = store.tenant(id).unwrap();
            tenant
                .put(
                    "assets",
                    json!({"_key":"confirmed","ip":"confirmed-ip"}),
                    None,
                )
                .unwrap();
            tenant.get("assets", "confirmed").unwrap().unwrap()
        };
        let path = dir.path().join("tenants").join(id.to_string());
        if !sealed {
            fs::remove_file(path.join("WAL-SYNC")).unwrap();
        }
        let wal = fs::read_dir(path.join("wal"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let confirmed_length = wal.metadata().unwrap().len();
        let original = fs::read(&wal).unwrap();
        let mut file = OpenOptions::new().append(true).open(&wal).unwrap();
        file.write_all(&[0; 8192]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let damaged = fs::read(&wal).unwrap();
        {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            assert_eq!(store.tenant(id).err().unwrap().code, "CORRUPTION");
            assert_eq!(fs::read(&wal).unwrap(), damaged);
        }
        let repair = wal.with_extension("repair");
        fs::write(&repair, original).unwrap();
        fs::rename(repair, &wal).unwrap();
        let later = {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            let tenant = store.tenant(id).unwrap();
            assert_eq!(
                tenant.get("assets", "confirmed").unwrap().unwrap(),
                expected
            );
            assert!(tenant.get("assets", "old").unwrap().is_some());
            assert_eq!(wal.metadata().unwrap().len(), confirmed_length);
            tenant.engine().verify().unwrap();
            tenant
                .put("assets", json!({"_key":"later","ip":"later-ip"}), None)
                .unwrap();
            tenant.get("assets", "later").unwrap().unwrap()
        };
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let tenant = store.tenant(id).unwrap();
        assert_eq!(
            tenant.get("assets", "confirmed").unwrap().unwrap(),
            expected
        );
        assert_eq!(tenant.get("assets", "later").unwrap().unwrap(), later);
        tenant.engine().verify().unwrap();
    }
}

#[test]
fn incomplete_tail_is_discarded_but_synced_corruption_is_rejected() {
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
    let synced = fs::read(&wal).unwrap();
    assert!(synced.len() > 100);
    for damage in [
        "header_crc",
        "payload_crc",
        "short_header",
        "short_payload",
        "missing_frame",
    ] {
        let mut bytes = synced.clone();
        match damage {
            "header_crc" => bytes[64 + 16] ^= 1,
            "payload_crc" => *bytes.last_mut().unwrap() ^= 1,
            "short_header" => bytes.truncate(64 + 10),
            "short_payload" => {
                bytes.pop();
            }
            "missing_frame" => bytes.truncate(64),
            _ => unreachable!(),
        }
        fs::write(&wal, &bytes).unwrap();
        let store = Store::open(dir.path(), Options::default()).unwrap();
        assert_eq!(
            store.tenant(id).err().unwrap().code,
            "CORRUPTION",
            "{damage}"
        );
        assert_eq!(fs::read(&wal).unwrap(), bytes);
    }
    let store = Store::open(dir.path(), Options::default()).unwrap();
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
        "wal_torn_zero_header",
        "wal_torn_header_crc",
        "wal_torn_payload_crc",
        "wal_appended",
        "wal_synced",
        "manifest_published",
        "wal_rotation_pending",
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
        if matches!(
            point,
            "wal_torn_zero_header" | "wal_torn_header_crc" | "wal_torn_payload_crc"
        ) {
            assert_eq!(store.tenant(id).err().unwrap().code, "CORRUPTION");
            let wal = dir.path().join("tenants").join(id.to_string()).join("wal");
            assert!(fs::read_dir(wal).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "corrupt")
            }));
            continue;
        }
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
            "wal_rotation_pending",
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
        "wal_torn_zero_header",
        "wal_torn_header_crc",
        "wal_torn_payload_crc",
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
        if matches!(
            point,
            "wal_torn_zero_header" | "wal_torn_header_crc" | "wal_torn_payload_crc"
        ) {
            assert_eq!(store.tenant(id).err().unwrap().code, "CORRUPTION");
            let wal = dir.path().join("tenants").join(id.to_string()).join("wal");
            assert!(fs::read_dir(wal).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "corrupt")
            }));
            continue;
        }
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

#[cfg(feature = "failpoints")]
#[test]
fn tenant_creation_recovers_staging_and_both_registry_publications() {
    for (point, published) in [
        ("tenant_create_staged", false),
        ("tenant_create_journal", true),
        ("tenant_create_published", true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let id = seed(dir.path());
        let expected = {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            store.tenant(id).unwrap().get("assets", "old").unwrap()
        };
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
            .arg(dir.path())
            .arg("created-by-crash")
            .arg("create")
            .env("TKV_FAILPOINT", point)
            .status()
            .unwrap();
        assert!(!status.success());
        for _ in 0..2 {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            assert_eq!(
                store.tenant(id).unwrap().get("assets", "old").unwrap(),
                expected
            );
            let tenant = store
                .tenants()
                .into_iter()
                .find(|info| info.name == "created-by-crash");
            assert_eq!(tenant.is_some(), published);
            if let Some(tenant) = tenant {
                store.tenant(tenant.id).unwrap().collections().unwrap();
            }
            assert_eq!(fs::read_dir(dir.path().join("staging")).unwrap().count(), 0);
            assert_eq!(
                fs::read_dir(dir.path().join("tenants")).unwrap().count(),
                1 + usize::from(published)
            );
        }
    }
}

#[test]
fn failed_tenant_creation_is_reclaimed_without_losing_registered_data() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let id = seed(dir.path());
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let control = dir.path().join("control");
    let mode = fs::metadata(&control).unwrap().permissions();
    fs::set_permissions(&control, fs::Permissions::from_mode(0o500)).unwrap();
    let errors: Vec<_> = (0..8)
        .map(|n| store.create_tenant(&format!("failed-{n}")).unwrap_err())
        .collect();
    fs::set_permissions(&control, mode).unwrap();
    assert!(errors.iter().all(|e| e.code == "IO_ERROR"));
    assert_eq!(store.tenants().len(), 1);
    assert_eq!(fs::read_dir(dir.path().join("tenants")).unwrap().count(), 1);
    drop(store);
    let store = Store::open(dir.path(), Options::default()).unwrap();
    assert_eq!(store.tenants().len(), 1);
    assert_eq!(fs::read_dir(dir.path().join("tenants")).unwrap().count(), 1);
    assert_eq!(fs::read_dir(dir.path().join("staging")).unwrap().count(), 0);
    assert_eq!(
        store
            .tenant(id)
            .unwrap()
            .get("assets", "old")
            .unwrap()
            .unwrap()["owner"],
        "old-owner"
    );
    store.create_tenant("healthy-after-fault").unwrap();
}

#[test]
fn startup_reclaims_legacy_unregistered_databases_but_preserves_a_live_engine() {
    use tenant_kv::{
        resources::{Budget, Counter, IoScheduler},
        storage::Engine,
    };
    let dir = tempfile::tempdir().unwrap();
    let id = seed(dir.path());
    let mut info = {
        let store = Store::open(dir.path(), Options::default()).unwrap();
        store.tenants()[0].clone()
    };
    info.id = uuid::Uuid::new_v4();
    info.generation = uuid::Uuid::new_v4();
    info.history = uuid::Uuid::new_v4();
    let orphan = dir.path().join("tenants").join(info.id.to_string());
    Engine::create(&orphan, &info).unwrap();
    let options = Options::default();
    let engine = Engine::open(
        orphan.clone(),
        info,
        options.clone(),
        Budget {
            global: Counter::new(options.global_memory_bytes),
            tenant: Counter::new(options.tenant_memory_bytes),
        },
        IoScheduler::new(1),
    )
    .unwrap();
    assert_eq!(
        Store::open(dir.path(), options.clone()).err().unwrap().code,
        "BUSY"
    );
    assert!(orphan.exists());
    drop(engine);
    let store = Store::open(dir.path(), options.clone()).unwrap();
    assert!(!orphan.exists());
    assert_eq!(store.tenants().len(), 1);
    assert_eq!(
        store
            .tenant(id)
            .unwrap()
            .get("assets", "old")
            .unwrap()
            .unwrap()["owner"],
        "old-owner"
    );

    let mut info = store.tenants()[0].clone();
    info.id = uuid::Uuid::new_v4();
    let retained = dir.path().join("tenants").join(info.id.to_string());
    Engine::create(&retained, &info).unwrap();
    let engine = Engine::open(
        retained.clone(),
        info,
        options.clone(),
        Budget {
            global: Counter::new(options.global_memory_bytes),
            tenant: Counter::new(options.tenant_memory_bytes),
        },
        IoScheduler::new(1),
    )
    .unwrap();
    engine
        .transact(|tx| tx.put(b"Zprivate".to_vec(), b"preserve me".to_vec()))
        .unwrap();
    drop(engine);
    drop(store);
    let reopened = Store::open(dir.path(), options).unwrap();
    assert!(retained.exists());
    assert_eq!(reopened.tenants().len(), 1);
}

#[cfg(feature = "failpoints")]
#[test]
fn review0732_enospc_recovery_refuses_stale_reads_and_preserves_recovery_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let id = seed(dir.path());
    let crash = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
        .arg(dir.path())
        .arg(id.to_string())
        .arg("write")
        .env("TKV_FAILPOINT", "wal_appended")
        .status()
        .unwrap();
    assert!(!crash.success());
    let wal_dir = dir.path().join("tenants").join(id.to_string()).join("wal");
    let before: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap()
        .map(|e| {
            let path = e.unwrap().path();
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect();
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
        .arg(dir.path())
        .arg(id.to_string())
        .arg("readonly-recovery")
        .env("TKV_ERRORPOINT", "wal_recovery_enospc")
        .status()
        .unwrap();
    assert!(status.success());
    for (path, bytes) in before {
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let t = store.tenant(id).unwrap();
    assert!(t.get("assets", "old").unwrap().is_some());
    assert!(t.get("assets", "new").unwrap().is_some());
    t.engine().verify().unwrap();
}

#[cfg(feature = "failpoints")]
#[test]
fn edge_review_crashed_local_checkpoint_is_reclaimed_before_backup_count_limit() {
    let dir = tempfile::tempdir().unwrap();
    let id = seed(dir.path());
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
        .arg(dir.path())
        .arg(id.to_string())
        .arg("local-backup")
        .env("TKV_FAILPOINT", "checkpoint_files_synced")
        .status()
        .unwrap();
    assert!(!status.success());
    let parent = dir.path().join("backups").join(id.to_string());
    let partial = fs::read_dir(&parent)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert!(!partial.join("CURRENT").exists());
    assert!(partial.join("sst").is_dir());
    let store = Store::open(dir.path(), Options::default()).unwrap();
    assert_eq!(fs::read_dir(&parent).unwrap().count(), 0);
    let backup = store.backup(id).unwrap();
    store.restore(id, backup).unwrap();
    assert!(
        store
            .tenant(id)
            .unwrap()
            .get("assets", "old")
            .unwrap()
            .is_some()
    );
}

#[cfg(feature = "failpoints")]
#[test]
fn review0732_acknowledged_wal_corruption_preserves_evidence_and_blocks_sequence_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let id = seed(dir.path());
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
        .arg(dir.path())
        .arg(id.to_string())
        .arg("ack-and-abort")
        .status()
        .unwrap();
    assert!(!status.success());
    let acknowledged: serde_json::Value =
        serde_json::from_slice(&fs::read(dir.path().join("acknowledged.json")).unwrap()).unwrap();
    let seq = acknowledged["first"].as_u64().unwrap();
    let wal_dir = dir.path().join("tenants").join(id.to_string()).join("wal");
    let mut saved = None;
    for entry in fs::read_dir(&wal_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|ext| ext != "wal") {
            continue;
        }
        let original = fs::read(&path).unwrap();
        let mut cursor = 64;
        while cursor + 24 <= original.len() {
            let frame_seq =
                u64::from_le_bytes(original[cursor + 8..cursor + 16].try_into().unwrap());
            let length =
                u32::from_le_bytes(original[cursor + 4..cursor + 8].try_into().unwrap()) as usize;
            if frame_seq == seq {
                let mut damaged = original.clone();
                damaged[cursor + 24] ^= 1;
                fs::write(&path, &damaged).unwrap();
                saved = Some((path.clone(), original.clone(), damaged));
                break;
            }
            cursor += 24 + length;
        }
    }
    let (path, original, damaged) = saved.expect("acknowledged frame missing before corruption");
    for _ in 0..2 {
        let store = Store::open(dir.path(), Options::default()).unwrap();
        assert_eq!(store.tenant(id).err().unwrap().code, "CORRUPTION");
        assert_eq!(
            store.replication_positions().unwrap_err().code,
            "CORRUPTION"
        );
        assert_eq!(fs::read(&path).unwrap(), damaged);
    }
    assert_eq!(
        fs::read_dir(&wal_dir)
            .unwrap()
            .filter(|entry| entry
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|ext| ext == "corrupt"))
            .count(),
        1,
        "repeated failed opens duplicated the evidence inode"
    );
    assert!(fs::read_dir(&wal_dir).unwrap().any(|entry| {
        let p = entry.unwrap().path();
        p.extension().is_some_and(|ext| ext == "corrupt") && fs::read(p).unwrap() == damaged
    }));
    // Repair from the test's pre-corruption image using a fresh inode, preserving
    // the forensic hard links. Then check every externally acknowledged revision.
    let repair = path.with_extension("repair");
    fs::write(&repair, original).unwrap();
    fs::rename(repair, &path).unwrap();
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let tenant = store.tenant(id).unwrap();
    assert_eq!(
        tenant.get("assets", "confirmed-first").unwrap().unwrap()["_rev"],
        acknowledged["first"]
    );
    assert_eq!(
        tenant.get("assets", "confirmed-last").unwrap().unwrap()["_rev"],
        acknowledged["last"]
    );
    let next = tenant
        .put("assets", json!({"_key":"after-repair","ip":"five"}), None)
        .unwrap();
    assert!(next.rev > acknowledged["last"].as_u64().unwrap());
}

#[cfg(feature = "failpoints")]
#[test]
fn review0732_uncertain_local_backup_reports_an_actionable_id() {
    let dir = tempfile::tempdir().unwrap();
    let id = seed(dir.path());
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
        .arg(dir.path())
        .arg(id.to_string())
        .arg("local-backup-uncertain")
        .env("TKV_ERRORPOINT", "local_backup_renamed")
        .status()
        .unwrap();
    assert!(status.success());
    let store = Store::open(dir.path(), Options::default()).unwrap();
    assert!(
        store
            .tenant(id)
            .unwrap()
            .get("assets", "old")
            .unwrap()
            .is_some()
    );
}

#[cfg(feature = "failpoints")]
#[test]
fn reviewf647_offline_wal_repair_preserves_confirmed_prefix_and_rejects_old_replication_epoch() {
    use std::os::unix::fs::PermissionsExt;
    use tenant_kv::replication::{Config, LogRequest, PROTOCOL, Role, Service};
    for (fault, repair_crash) in [
        ("wal_torn_zero_header", None),
        ("wal_torn_header_crc", Some("restore_old_moved")),
        ("wal_torn_payload_crc", Some("restore_new_moved")),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let id = seed(&root);
        let store = Store::open(&root, Options::default()).unwrap();
        let tenant = store.tenant(id).unwrap();
        tenant
            .put(
                "assets",
                json!({"_key":"prefix","ip":"prefix","payload":"x".repeat(16384)}),
                None,
            )
            .unwrap();
        let confirmed = tenant.get("assets", "prefix").unwrap();
        let history = tenant.info().history;
        let generation = tenant.info().generation;
        let seq = tenant.engine().seq();
        let path = root.join("tenants").join(id.to_string());
        let current = fs::read_to_string(path.join("CURRENT")).unwrap();
        let manifest: serde_json::Value = storage::read_json(&path.join(&current)).unwrap();
        let wal: uuid::Uuid = manifest["wals"].as_array().unwrap().last().unwrap()["id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let wal_path = path.join("wal").join(format!("{wal}.wal"));
        let offset = fs::metadata(&wal_path).unwrap().len();
        let token = dir.path().join("token");
        fs::write(&token, "repair-replication-test-token").unwrap();
        fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).unwrap();
        let config = Config {
            cluster: uuid::Uuid::new_v4(),
            role: Role::Primary,
            peer_url: "http://127.0.0.1:1".into(),
            token_file: token,
            poll_ms: 10,
            log_retention_bytes: 1 << 20,
        };
        let service = Service::open(store.clone(), config.clone()).unwrap();
        let epoch = service.status().unwrap().node.epoch;
        assert!(
            Store::repair_wal(
                &root,
                Options::default(),
                id,
                wal,
                offset,
                dir.path().join("locked")
            )
            .is_err()
        );
        drop(service);
        drop(tenant);
        drop(store);
        let crash = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
            .arg(&root)
            .arg(id.to_string())
            .arg("write")
            .env("TKV_FAILPOINT", fault)
            .status()
            .unwrap();
        assert!(!crash.success());
        let original = fs::read(&wal_path).unwrap();
        assert!(original.len() as u64 > offset);
        let store = Store::open(&root, Options::default()).unwrap();
        assert_eq!(store.tenant(id).err().unwrap().code, "CORRUPTION");
        drop(store);
        assert!(
            Store::repair_wal(
                &root,
                Options::default(),
                id,
                wal,
                offset - 1,
                dir.path().join("invalid-boundary")
            )
            .is_err()
        );
        assert_eq!(fs::read(&wal_path).unwrap(), original);
        let ssts: Vec<_> = fs::read_dir(path.join("sst"))
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), fs::read(entry.path()).unwrap())
            })
            .collect();
        assert!(!ssts.is_empty());
        let archive = dir.path().join("evidence");
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_tenant-kv-repair"));
        command
            .current_dir(dir.path())
            .arg(&root)
            .arg(id.to_string())
            .arg(wal.to_string())
            .arg("--at")
            .arg(offset.to_string())
            .arg("--archive")
            .arg("evidence")
            .arg("--accept-data-loss");
        if let Some(point) = repair_crash {
            command.env("TKV_FAILPOINT", point);
        }
        let repaired = command.output().unwrap();
        assert_eq!(
            repaired.status.success(),
            repair_crash.is_none(),
            "{}",
            String::from_utf8_lossy(&repaired.stderr)
        );
        assert_eq!(
            fs::read(archive.join("database/wal").join(format!("{wal}.wal"))).unwrap(),
            original
        );
        assert_eq!(
            fs::read_to_string(archive.join("database/CURRENT")).unwrap(),
            current
        );
        let archived_manifest: serde_json::Value =
            storage::read_json(&archive.join("database").join(&current)).unwrap();
        assert_eq!(archived_manifest, manifest);
        for (name, bytes) in &ssts {
            assert_eq!(
                fs::read(archive.join("database/sst").join(name)).unwrap(),
                *bytes
            );
        }
        let report: serde_json::Value = storage::read_json(&archive.join("repair.json")).unwrap();
        if repair_crash.is_none() {
            assert_eq!(report["phase"], "published");
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&repaired.stdout).unwrap(),
                report
            );
        } else {
            assert_eq!(report["phase"], "publishing_tenant");
        }
        let store = Store::open(&root, Options::default()).unwrap();
        let service = Service::open(store.clone(), config.clone()).unwrap();
        assert_ne!(service.status().unwrap().node.epoch, epoch);
        let tenant = store.tenant(id).unwrap();
        assert_ne!(tenant.info().history, history);
        assert_eq!(tenant.get("assets", "prefix").unwrap(), confirmed);
        assert!(tenant.get("assets", "old").unwrap().is_some());
        assert!(tenant.get("assets", "new").unwrap().is_none());
        assert_eq!(
            service
                .export_log(&LogRequest {
                    protocol: PROTOCOL,
                    cluster: config.cluster,
                    epoch,
                    tenant: id,
                    generation,
                    after: seq,
                    cursor: None
                })
                .err()
                .unwrap()
                .code,
            "REPLICATION_CONFLICT"
        );
        tenant
            .put("assets", json!({"_key":"after","ip":"after"}), None)
            .unwrap();
        tenant.engine().verify().unwrap();
    }
}

#[test]
fn reviewf647_repeated_incomplete_appends_keep_bounded_tail_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let id = seed(dir.path());
    let root = dir.path().join("tenants").join(id.to_string());
    for n in 0..4 {
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let tenant = store.tenant(id).unwrap();
        tenant.put("assets", json!({"_key":"large-prefix","ip":"large-prefix","value":"x".repeat(128 << 10),"n":n}), None).unwrap();
        let confirmed = tenant.get("assets", "large-prefix").unwrap();
        drop(tenant);
        drop(store);
        let current = fs::read_to_string(root.join("CURRENT")).unwrap();
        let manifest: serde_json::Value = storage::read_json(&root.join(current)).unwrap();
        let wal = manifest["wals"].as_array().unwrap().last().unwrap()["id"]
            .as_str()
            .unwrap();
        let path = root.join("wal").join(format!("{wal}.wal"));
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(b"TXN1-short").unwrap();
        file.sync_all().unwrap();
        drop(file);
        let store = Store::open(dir.path(), Options::default()).unwrap();
        assert_eq!(
            store
                .tenant(id)
                .unwrap()
                .get("assets", "large-prefix")
                .unwrap(),
            confirmed
        );
        let evidence: Vec<_> = fs::read_dir(root.join("wal"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.extension().is_some_and(|e| e == "corrupt")
                    || p.file_name().unwrap() == "TORN-TAIL"
            })
            .collect();
        assert_eq!(evidence.len(), 1);
        assert!(fs::metadata(&evidence[0]).unwrap().len() < 1024);
    }
}

#[cfg(feature = "failpoints")]
#[test]
fn reviewc3b_repair_reports_epoch_and_publication_failures_without_panicking() {
    use std::os::unix::fs::PermissionsExt;
    for (point, crash, published) in [
        ("repair_epoch_changed", false, false),
        ("repair_epoch_changed", true, false),
        ("restore_journal_published", false, true),
        ("restore_cleanup", false, true),
        ("restore_cleanup_permission", false, true),
    ] {
        // The injected cleanup failure remains covered when running as root;
        // a real POSIX denial needs an ordinary service user (OrbStack/CI).
        if point == "restore_cleanup_permission" && unsafe { libc::geteuid() } == 0 {
            continue;
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let id = seed(&root);
        let store = Store::open(&root, Options::default()).unwrap();
        let tenant = store.tenant(id).unwrap();
        tenant
            .put("assets", json!({"_key":"ack","ip":"ack-ip"}), None)
            .unwrap();
        let confirmed = tenant.get("assets", "ack").unwrap();
        let history = tenant.info().history;
        let epoch = store.replication_state().epoch;
        let database = root.join("tenants").join(id.to_string());
        let current = fs::read_to_string(database.join("CURRENT")).unwrap();
        let manifest: serde_json::Value = storage::read_json(&database.join(&current)).unwrap();
        let wal = manifest["wals"].as_array().unwrap().last().unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let wal_path = database.join("wal").join(format!("{wal}.wal"));
        let offset = fs::metadata(&wal_path).unwrap().len();
        drop(tenant);
        drop(store);
        OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap()
            .write_all(&[0; 24])
            .unwrap();
        let original = fs::read(&wal_path).unwrap();
        if point == "restore_cleanup_permission" {
            fs::set_permissions(database.join("sst"), fs::Permissions::from_mode(0o500)).unwrap();
        }
        let archive = dir.path().join("evidence");
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_tenant-kv-repair"))
            .arg(&root)
            .arg(id.to_string())
            .arg(&wal)
            .arg("--at")
            .arg(offset.to_string())
            .arg("--archive")
            .arg(&archive)
            .arg("--accept-data-loss")
            .env(
                if crash {
                    "TKV_FAILPOINT"
                } else {
                    "TKV_ERRORPOINT"
                },
                point,
            )
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("panicked"), "{stderr}");
        let report: serde_json::Value = storage::read_json(&archive.join("repair.json")).unwrap();
        let new_epoch: uuid::Uuid = report["new_epoch"].as_str().unwrap().parse().unwrap();
        assert_ne!(new_epoch, epoch);
        assert_eq!(
            fs::read(archive.join("database/wal").join(format!("{wal}.wal"))).unwrap(),
            original
        );
        assert_eq!(
            fs::read_to_string(archive.join("database/CURRENT")).unwrap(),
            current
        );
        assert!(
            fs::read_dir(archive.join("database/sst"))
                .unwrap()
                .next()
                .is_some()
        );
        if point.starts_with("restore_cleanup") {
            assert!(output.status.success(), "{stderr}");
            assert_eq!(report["phase"], "published_cleanup_pending");
            assert!(
                report["warning"]
                    .as_str()
                    .unwrap()
                    .contains("RESTORE_CLEANUP_FAILED")
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
                report
            );
            assert!(fs::read_dir(root.join("trash")).unwrap().next().is_some());
        } else {
            assert!(!output.status.success());
            if crash {
                assert_eq!(report["phase"], "epoch_changed");
            } else {
                assert_eq!(report["phase"], "failed");
                assert_eq!(
                    report["failed_phase"],
                    if published {
                        "publishing_tenant"
                    } else {
                        "epoch_changed"
                    }
                );
                assert!(stderr.contains(&new_epoch.to_string()), "{stderr}");
                assert!(stderr.contains("repair.json"), "{stderr}");
            }
        }
        if point == "restore_cleanup_permission" {
            assert!(report["warning"].as_str().unwrap().contains("IO_ERROR"));
            for entry in fs::read_dir(root.join("trash")).unwrap() {
                fs::set_permissions(
                    entry.unwrap().path().join("sst"),
                    fs::Permissions::from_mode(0o700),
                )
                .unwrap();
            }
        }
        let store = Store::open(&root, Options::default()).unwrap();
        assert_eq!(store.replication_state().epoch, new_epoch);
        if published {
            let tenant = store.tenant(id).unwrap();
            assert_ne!(tenant.info().history, history);
            assert_eq!(tenant.get("assets", "ack").unwrap(), confirmed);
            tenant.engine().verify().unwrap();
        } else {
            assert_eq!(store.tenant(id).err().unwrap().code, "CORRUPTION");
            assert_eq!(fs::read(&wal_path).unwrap(), original);
            drop(store);
            // A failed pre-publication attempt is inspectable and repairable.
            let report = Store::repair_wal(
                &root,
                Options::default(),
                id,
                wal.parse().unwrap(),
                offset,
                dir.path().join("retry-evidence"),
            )
            .unwrap();
            assert_eq!(report["phase"], "published");
            let store = Store::open(&root, Options::default()).unwrap();
            assert_eq!(
                store.tenant(id).unwrap().get("assets", "ack").unwrap(),
                confirmed
            );
        }
    }
}

#[test]
fn reviewc3b_noop_repair_preserves_healthy_database_and_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let id = seed(dir.path());
    let store = Store::open(dir.path(), Options::default()).unwrap();
    let tenant = store.tenant(id).unwrap();
    tenant
        .put("assets", json!({"_key":"ack","ip":"ack-ip"}), None)
        .unwrap();
    let confirmed = tenant.get("assets", "ack").unwrap();
    let history = tenant.info().history;
    let epoch = store.replication_state().epoch;
    let database = dir.path().join("tenants").join(id.to_string());
    let current = fs::read_to_string(database.join("CURRENT")).unwrap();
    let manifest: serde_json::Value = storage::read_json(&database.join(current)).unwrap();
    let wal: uuid::Uuid = manifest["wals"].as_array().unwrap().last().unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let path = database.join("wal").join(format!("{wal}.wal"));
    drop(tenant);
    drop(store);
    let original = fs::read(&path).unwrap();
    let external = tempfile::tempdir().unwrap();
    let archive = external.path().join("unused");
    assert_eq!(
        Store::repair_wal(
            dir.path(),
            Options::default(),
            id,
            wal,
            original.len() as u64,
            &archive
        )
        .unwrap_err()
        .code,
        "INVALID_ARGUMENT"
    );
    assert!(!archive.exists());
    assert_eq!(fs::read(&path).unwrap(), original);
    let store = Store::open(dir.path(), Options::default()).unwrap();
    assert_eq!(store.replication_state().epoch, epoch);
    let tenant = store.tenant(id).unwrap();
    assert_eq!(tenant.info().history, history);
    assert_eq!(tenant.get("assets", "ack").unwrap(), confirmed);
}
