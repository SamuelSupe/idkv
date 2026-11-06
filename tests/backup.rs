use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    path::Path,
    time::{Duration, Instant},
};
use tenant_kv::{backup::*, *};
use uuid::Uuid;

fn config(remote: &Path) -> BackupConfig {
    BackupConfig {
        service: "fs".into(),
        options: HashMap::from([("root".into(), remote.to_string_lossy().into())]),
        concurrency: 2,
        max_backup_bytes: 64 << 20,
        max_staging_bytes: 512 << 20,
        reserve_free_bytes: 0,
        bytes_per_second: 100 << 20,
        gc_grace_seconds: 0,
        timeout_seconds: 2,
        ..Default::default()
    }
}
fn setup() -> (tempfile::TempDir, Store, Uuid, BackupConfig) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data"), Options::default()).unwrap();
    let id = store.create_tenant("a").unwrap().id;
    let t = store.tenant(id).unwrap();
    t.create_collection("assets").unwrap();
    t.put(
        "assets",
        json!({"_key":"a","ip":"one","owner":"before"}),
        None,
    )
    .unwrap();
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
    drop(t);
    let cfg = config(&dir.path().join("remote"));
    (dir, store, id, cfg)
}
fn start(service: &BackupService, tenant: Uuid, operation: BackupOperation) -> BackupJob {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match service.submit(tenant, operation.clone()) {
            Ok(job) => return job,
            Err(e) if e.code == "BUSY" && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10))
            }
            other => panic!("submit: {other:?}"),
        }
    }
}
fn wait(service: &BackupService, job: &BackupJob) -> BackupJob {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let job = service.job(job.tenant, job.id).unwrap();
        if matches!(
            job.state,
            JobState::Completed | JobState::Failed | JobState::Interrupted
        ) {
            return job;
        }
        assert!(Instant::now() < deadline, "job timeout: {job:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn complete(service: &BackupService, tenant: Uuid, operation: BackupOperation) -> BackupJob {
    let result = wait(service, &start(service, tenant, operation));
    assert_eq!(result.state, JobState::Completed, "{result:?}");
    result
}
fn list(service: &BackupService, tenant: Uuid) -> Vec<BackupSummary> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match service.list(tenant) {
            Ok(list) => return list,
            Err(e) if e.code == "BUSY" && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10))
            }
            other => panic!("list: {other:?}"),
        }
    }
}
fn snapshot(dir: &Path, tenant: Uuid, backup: Uuid) -> std::path::PathBuf {
    dir.join("remote")
        .join(format!("tenants/{tenant}/snapshots/{backup}"))
}

#[test]
fn opendal_incremental_backup_restore_and_reference_retention() {
    let (dir, store, id, cfg) = setup();
    let b = store.create_tenant("b").unwrap().id;
    store
        .tenant(b)
        .unwrap()
        .create_collection("assets")
        .unwrap();
    store
        .tenant(b)
        .unwrap()
        .put("assets", json!({"_key":"a","owner":"other"}), None)
        .unwrap();
    let service = BackupService::open(store.clone(), cfg.clone()).unwrap();
    let a = complete(&service, id, BackupOperation::Create);
    let second = complete(&service, id, BackupOperation::Create);
    assert!(second.reused_bytes > 0);
    assert!(second.uploaded_bytes < a.uploaded_bytes);
    let other = complete(&service, b, BackupOperation::Create);
    assert_eq!(list(&service, id).len(), 2);
    complete(
        &service,
        id,
        BackupOperation::Verify {
            backup: second.backup,
        },
    );
    let prune = complete(&service, id, BackupOperation::Prune { keep_last: 1 });
    assert_eq!(prune.result.unwrap()["deleted_backups"], 1);
    assert!(
        !snapshot(dir.path(), id, a.backup)
            .join("COMPLETED")
            .exists()
    );
    assert!(
        snapshot(dir.path(), b, other.backup)
            .join("COMPLETED")
            .exists()
    );
    store
        .tenant(id)
        .unwrap()
        .put(
            "assets",
            json!({"_key":"a","ip":"two","owner":"after"}),
            None,
        )
        .unwrap();
    complete(
        &service,
        id,
        BackupOperation::Restore {
            backup: second.backup,
        },
    );
    assert_eq!(
        store
            .tenant(id)
            .unwrap()
            .get("assets", "a")
            .unwrap()
            .unwrap()["owner"],
        "before"
    );
    let q: Query = serde_json::from_value(
        json!({"filter":{"field":"/ip","op":"eq","value":"one"},"select":["/owner"]}),
    )
    .unwrap();
    assert_eq!(
        store
            .tenant(id)
            .unwrap()
            .query("assets", q)
            .unwrap()
            .documents,
        vec![json!({"owner":"before"})]
    );
    assert_eq!(
        store
            .tenant(b)
            .unwrap()
            .get("assets", "a")
            .unwrap()
            .unwrap()["owner"],
        "other"
    );
    let cross = wait(
        &service,
        &start(
            &service,
            b,
            BackupOperation::Restore {
                backup: second.backup,
            },
        ),
    );
    assert_eq!(cross.state, JobState::Failed);
    assert!(cross.error.unwrap().contains("NOT_FOUND"));
    assert!(service.job(b, a.id).is_err());
    service.forget(id, a.id).unwrap();
    assert!(service.job(id, a.id).is_err());
    drop(service);
    drop(store);
    let store = Store::open(dir.path().join("data"), Options::default()).unwrap();
    let service = BackupService::open(store.clone(), cfg).unwrap();
    assert_eq!(
        service.job(id, second.id).unwrap().state,
        JobState::Completed
    );
    assert_eq!(list(&service, id).len(), 1);
    assert_eq!(
        store
            .tenant(id)
            .unwrap()
            .get("assets", "a")
            .unwrap()
            .unwrap()["owner"],
        "before"
    );
}

#[test]
fn corrupted_objects_and_traversal_manifests_never_replace_live_data() {
    let (dir, store, id, cfg) = setup();
    let service = BackupService::open(store.clone(), cfg).unwrap();
    let backup = complete(&service, id, BackupOperation::Create);
    let path = snapshot(dir.path(), id, backup.backup);
    let original = fs::read(path.join("manifest.json")).unwrap();
    let manifest: BackupManifest = serde_json::from_slice(&original).unwrap();
    let file = manifest
        .files
        .iter()
        .find(|f| f.path.starts_with("sst/"))
        .unwrap();
    let object = dir.path().join(format!(
        "remote/tenants/{id}/objects/{}/{}",
        manifest.tenant.generation, file.sha256
    ));
    let saved = fs::read(&object).unwrap();
    let mut broken = saved.clone();
    let last = broken.len() - 1;
    broken[last] ^= 1;
    fs::write(&object, broken).unwrap();
    store
        .tenant(id)
        .unwrap()
        .put("assets", json!({"_key":"keep"}), None)
        .unwrap();
    for operation in [
        BackupOperation::Verify {
            backup: backup.backup,
        },
        BackupOperation::Restore {
            backup: backup.backup,
        },
    ] {
        let job = wait(&service, &start(&service, id, operation));
        assert_eq!(job.state, JobState::Failed);
        assert!(job.error.unwrap().contains("CORRUPTION"));
        assert!(
            store
                .tenant(id)
                .unwrap()
                .get("assets", "keep")
                .unwrap()
                .is_some()
        );
    }
    fs::write(&object, saved).unwrap();
    let mut malformed: Value = serde_json::from_slice(&original).unwrap();
    malformed["files"][0]["path"] = json!("../../escape");
    let bytes = serde_json::to_vec(&malformed).unwrap();
    fs::write(path.join("manifest.json"), &bytes).unwrap();
    fs::write(
        path.join("COMPLETED"),
        serde_json::to_vec(
            &json!({"version":1,"manifest_sha256":format!("{:x}",Sha256::digest(&bytes))}),
        )
        .unwrap(),
    )
    .unwrap();
    let failed = wait(
        &service,
        &start(
            &service,
            id,
            BackupOperation::Restore {
                backup: backup.backup,
            },
        ),
    );
    assert_eq!(failed.state, JobState::Failed);
    assert!(!dir.path().join("escape").exists());
    let prune = wait(
        &service,
        &start(&service, id, BackupOperation::Prune { keep_last: 1 }),
    );
    assert_eq!(prune.state, JobState::Failed);
    assert!(object.exists());
}

#[test]
fn storage_failure_and_quota_do_not_stop_local_commits() {
    let (dir, store, id, mut cfg) = setup();
    cfg.max_backup_bytes = 1 << 20;
    let service = BackupService::open(store.clone(), cfg.clone()).unwrap();
    // Turn the destination namespace into a file: real backend writes fail.
    fs::write(dir.path().join("remote/tenants"), b"blocked").unwrap();
    let failed = wait(&service, &start(&service, id, BackupOperation::Create));
    assert_eq!(failed.state, JobState::Failed);
    store
        .tenant(id)
        .unwrap()
        .put("assets", json!({"_key":"while-offline"}), None)
        .unwrap();
    fs::remove_file(dir.path().join("remote/tenants")).unwrap();
    let retried = service.retry(id, failed.id).unwrap();
    assert_eq!(wait(&service, &retried).state, JobState::Completed);
    let mut seed = 41u64;
    for batch in 0..4 {
        let ops = (0..100)
            .map(|j| {
                let payload: String = (0..8192)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        char::from(33 + (seed % 90) as u8)
                    })
                    .collect();
                WriteOp::Put {
                    document: json!({"_key":format!("{batch}-{j}"),"payload":payload}),
                    if_rev: None,
                }
            })
            .collect();
        store
            .tenant(id)
            .unwrap()
            .write_batch("assets", ops)
            .unwrap();
    }
    let limited = wait(&service, &start(&service, id, BackupOperation::Create));
    assert_eq!(limited.state, JobState::Failed);
    assert!(limited.error.unwrap().contains("RESOURCE_EXHAUSTED"));
    store
        .tenant(id)
        .unwrap()
        .put("assets", json!({"_key":"after-limit"}), None)
        .unwrap();
    assert_eq!(list(&service, id).len(), 1);
}

#[cfg(feature = "failpoints")]
#[test]
fn interrupted_uploads_resume_and_only_complete_markers_are_visible() {
    for point in [
        "backup_progress",
        "backup_manifest_uploaded",
        "backup_completed",
    ] {
        let (dir, store, id, cfg) = setup();
        let config_path = dir.path().join("backup.json");
        fs::write(&config_path, serde_json::to_vec(&cfg).unwrap()).unwrap();
        drop(store);
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
            .arg(dir.path().join("data"))
            .arg(id.to_string())
            .arg("remote-backup")
            .arg(&config_path)
            .env("TKV_FAILPOINT", point)
            .status()
            .unwrap();
        assert!(!status.success());
        let store = Store::open(dir.path().join("data"), Options::default()).unwrap();
        let service = BackupService::open(store.clone(), cfg).unwrap();
        let jobs = service.jobs(id);
        assert_eq!(jobs.len(), 1);
        let finished = wait(&service, &jobs[0]);
        assert_eq!(finished.state, JobState::Completed, "{finished:?}");
        if point == "backup_progress" {
            assert!(
                finished.reused_bytes > 0,
                "uploaded objects must be reused after losing progress counters"
            );
        }
        assert_eq!(list(&service, id).len(), 1);
        complete(
            &service,
            id,
            BackupOperation::Verify {
                backup: finished.backup,
            },
        );
        assert_eq!(
            store
                .tenant(id)
                .unwrap()
                .get("assets", "a")
                .unwrap()
                .unwrap()["owner"],
            "before"
        );
    }
}

#[cfg(feature = "failpoints")]
#[test]
fn restore_after_switch_is_never_replayed_over_new_writes() {
    let (dir, store, id, cfg) = setup();
    let service = BackupService::open(store.clone(), cfg.clone()).unwrap();
    let backup = complete(&service, id, BackupOperation::Create);
    store
        .tenant(id)
        .unwrap()
        .put("assets", json!({"_key":"a","owner":"changed"}), None)
        .unwrap();
    let path = dir.path().join("backup.json");
    fs::write(&path, serde_json::to_vec(&cfg).unwrap()).unwrap();
    drop(service);
    drop(store);
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
        .arg(dir.path().join("data"))
        .arg(id.to_string())
        .arg("remote-restore")
        .arg(path)
        .arg(backup.backup.to_string())
        .env("TKV_FAILPOINT", "backup_restore_switched")
        .status()
        .unwrap();
    assert!(!status.success());
    let store = Store::open(dir.path().join("data"), Options::default()).unwrap();
    assert_eq!(
        store
            .tenant(id)
            .unwrap()
            .get("assets", "a")
            .unwrap()
            .unwrap()["owner"],
        "before"
    );
    store
        .tenant(id)
        .unwrap()
        .put("assets", json!({"_key":"a","owner":"post-crash"}), None)
        .unwrap();
    let service = BackupService::open(store.clone(), cfg.clone()).unwrap();
    let interrupted = service
        .jobs(id)
        .into_iter()
        .find(|j| matches!(j.operation, BackupOperation::Restore { .. }))
        .unwrap();
    assert_eq!(interrupted.state, JobState::Interrupted);
    assert!(service.retry(id, interrupted.id).is_err());
    assert_eq!(
        store
            .tenant(id)
            .unwrap()
            .get("assets", "a")
            .unwrap()
            .unwrap()["owner"],
        "post-crash"
    );
    drop(service);
    let reopened = BackupService::open(store.clone(), cfg).unwrap();
    assert_eq!(
        reopened.job(id, interrupted.id).unwrap().state,
        JobState::Interrupted
    );
    assert_eq!(
        store
            .tenant(id)
            .unwrap()
            .get("assets", "a")
            .unwrap()
            .unwrap()["owner"],
        "post-crash"
    );
}

#[test]
fn backup_during_writes_and_compaction_has_one_commit_boundary() {
    let (_dir, store, id, cfg) = setup();
    let service = BackupService::open(store.clone(), cfg).unwrap();
    let writer = store.tenant(id).unwrap();
    let handle = std::thread::spawn(move || {
        let mut revisions = Vec::new();
        for i in 0..80 {
            let key = format!("write-{i}");
            let result = writer
                .put("assets", json!({"_key":key,"owner":i}), None)
                .unwrap();
            revisions.push((key, result.rev));
            if i % 8 == 0 {
                writer.engine().flush().unwrap();
                writer.engine().compact(true).unwrap();
            }
        }
        revisions
    });
    let backup = complete(&service, id, BackupOperation::Create);
    let revisions = handle.join().unwrap();
    complete(
        &service,
        id,
        BackupOperation::Restore {
            backup: backup.backup,
        },
    );
    let tenant = store.tenant(id).unwrap();
    for (key, rev) in revisions {
        assert_eq!(
            tenant.get("assets", &key).unwrap().is_some(),
            rev <= backup.checkpoint_seq.unwrap(),
            "{key}"
        );
    }
}

#[test]
fn remote_restore_recovers_a_tenant_that_cannot_open() {
    let (dir, store, id, cfg) = setup();
    let service = BackupService::open(store.clone(), cfg.clone()).unwrap();
    let backup = complete(&service, id, BackupOperation::Create);
    drop(service);
    drop(store);
    let path = dir
        .path()
        .join("data/tenants")
        .join(id.to_string())
        .join("CURRENT");
    fs::write(path, "broken").unwrap();
    let store = Store::open(dir.path().join("data"), Options::default()).unwrap();
    assert!(store.tenant(id).is_err());
    let service = BackupService::open(store.clone(), cfg).unwrap();
    complete(
        &service,
        id,
        BackupOperation::Restore {
            backup: backup.backup,
        },
    );
    assert_eq!(
        store
            .tenant(id)
            .unwrap()
            .get("assets", "a")
            .unwrap()
            .unwrap()["owner"],
        "before"
    );
}

#[cfg(feature = "failpoints")]
#[test]
fn fresh_store_import_and_import_journal_crash_recovery() {
    for point in [
        None,
        Some("backup_import_journal"),
        Some("restore_new_moved"),
    ] {
        let (dir, store, id, cfg) = setup();
        let service = BackupService::open(store.clone(), cfg.clone()).unwrap();
        let backup = complete(&service, id, BackupOperation::Create);
        drop(service);
        drop(store);
        let fresh = dir.path().join("fresh");
        if let Some(point) = point {
            let path = dir.path().join("backup.json");
            fs::write(&path, serde_json::to_vec(&cfg).unwrap()).unwrap();
            let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash-probe"))
                .arg(&fresh)
                .arg(id.to_string())
                .arg("remote-import")
                .arg(path)
                .arg(backup.backup.to_string())
                .env("TKV_FAILPOINT", point)
                .status()
                .unwrap();
            assert!(!status.success());
        }
        let store = Store::open(&fresh, Options::default()).unwrap();
        let service = BackupService::open(store.clone(), cfg).unwrap();
        if point.is_none() {
            complete(
                &service,
                id,
                BackupOperation::Import {
                    backup: backup.backup,
                },
            );
        } else {
            assert_eq!(service.jobs(id)[0].state, JobState::Interrupted);
        }
        assert_eq!(
            store
                .tenant(id)
                .unwrap()
                .get("assets", "a")
                .unwrap()
                .unwrap()["owner"],
            "before"
        );
        assert_eq!(
            store.tenant(id).unwrap().indexes("assets").unwrap().len(),
            1
        );
        assert_eq!(
            service
                .submit(
                    id,
                    BackupOperation::Import {
                        backup: backup.backup
                    }
                )
                .unwrap_err()
                .code,
            "ALREADY_EXISTS"
        );
        store.delete_tenant(id).unwrap();
        assert_eq!(
            service
                .submit(
                    id,
                    BackupOperation::Import {
                        backup: backup.backup
                    }
                )
                .unwrap_err()
                .code,
            "ALREADY_EXISTS"
        );
    }
}
