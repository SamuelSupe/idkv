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
fn pruning_reclaims_snapshot_directories_and_legacy_empty_history() {
    let (dir, store, tenant, cfg) = setup();
    let service = BackupService::open(store.clone(), cfg).unwrap();
    let first = complete(&service, tenant, BackupOperation::Create);
    let last = complete(&service, tenant, BackupOperation::Create);
    complete(&service, tenant, BackupOperation::Prune { keep_last: 1 });
    assert!(!snapshot(dir.path(), tenant, first.backup).exists());
    let snapshots = snapshot(dir.path(), tenant, last.backup)
        .parent()
        .unwrap()
        .to_path_buf();
    for _ in 0..4097 {
        fs::create_dir(snapshots.join(Uuid::new_v4().to_string())).unwrap();
    }
    // An unfinished, nonempty snapshot must not be mistaken for empty residue.
    let unfinished = snapshots.join(Uuid::new_v4().to_string());
    fs::create_dir(&unfinished).unwrap();
    fs::write(unfinished.join("manifest.json"), b"pending upload").unwrap();
    assert_eq!(list(&service, tenant).len(), 1);
    complete(&service, tenant, BackupOperation::Prune { keep_last: 1 });
    assert_eq!(fs::read_dir(&snapshots).unwrap().count(), 3);
    complete(&service, tenant, BackupOperation::Prune { keep_last: 1 });
    assert_eq!(fs::read_dir(&snapshots).unwrap().count(), 2);
    assert!(unfinished.join("manifest.json").exists());
    complete(
        &service,
        tenant,
        BackupOperation::Verify {
            backup: last.backup,
        },
    );
    assert_eq!(list(&service, tenant)[0].backup, last.backup);
    assert_eq!(
        store
            .tenant(tenant)
            .unwrap()
            .get("assets", "a")
            .unwrap()
            .unwrap()["owner"],
        "before"
    );
}

#[test]
fn completed_snapshot_limit_still_applies_after_empty_history_is_ignored() {
    let (dir, store, tenant, cfg) = setup();
    let service = BackupService::open(store, cfg).unwrap();
    let backup = complete(&service, tenant, BackupOperation::Create);
    let source = snapshot(dir.path(), tenant, backup.backup);
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(source.join("manifest.json")).unwrap()).unwrap();
    for _ in 0..4096 {
        let id = Uuid::new_v4();
        let path = snapshot(dir.path(), tenant, id);
        fs::create_dir(&path).unwrap();
        manifest["backup"] = json!(id);
        let bytes = serde_json::to_vec(&manifest).unwrap();
        fs::write(path.join("manifest.json"), &bytes).unwrap();
        fs::write(
            path.join("COMPLETED"),
            serde_json::to_vec(&json!({"version":1,
            "manifest_sha256":format!("{:x}", Sha256::digest(&bytes))}))
            .unwrap(),
        )
        .unwrap();
    }
    assert_eq!(service.list(tenant).unwrap_err().code, "RESOURCE_EXHAUSTED");
    assert!(source.join("COMPLETED").exists());
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

#[test]
fn concurrent_backups_count_active_staging_only_once() {
    let (dir, store, a, mut cfg) = setup();
    let b = store.create_tenant("concurrent-staging").unwrap().id;
    store
        .tenant(b)
        .unwrap()
        .create_collection("assets")
        .unwrap();
    let mut random = 0x85a2bc9du64;
    let payload: String = (0..512 << 10)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            (b'a' + (random % 26) as u8) as char
        })
        .collect();
    for id in [a, b] {
        let tenant = store.tenant(id).unwrap();
        for n in 0..4 {
            tenant
                .put(
                    "assets",
                    json!({"_key":format!("large-{n}"),"value":payload}),
                    None,
                )
                .unwrap();
        }
    }
    cfg.max_backup_bytes = 16 << 20;
    cfg.max_staging_bytes = (32 << 20) + (1 << 20);
    cfg.bytes_per_second = 1 << 20;
    let service = BackupService::open(store.clone(), cfg).unwrap();
    let first = start(&service, a, BackupOperation::Create);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let job = service.job(a, first.id).unwrap();
        if job.state == JobState::Uploading {
            break;
        }
        assert!(
            !matches!(
                job.state,
                JobState::Completed | JobState::Failed | JobState::Interrupted
            ),
            "{job:?}"
        );
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    let second = start(&service, b, BackupOperation::Create);
    for job in [&first, &second] {
        let done = wait(&service, job);
        assert_eq!(done.state, JobState::Completed, "{done:?}");
        let summaries = list(&service, job.tenant);
        assert_eq!(summaries.len(), 1);
        assert!(summaries[0].total_bytes < 16 << 20);
    }
    for id in [a, b] {
        assert_eq!(
            store
                .tenant(id)
                .unwrap()
                .get("assets", "large-3")
                .unwrap()
                .unwrap()["value"],
            payload
        );
    }
    drop(service);
    drop(store);
    drop(dir);
}

#[test]
fn subsystem_review_failed_restore_releases_downloaded_workspace() {
    let (dir, store, id, cfg) = setup();
    let service = BackupService::open(store.clone(), cfg.clone()).unwrap();
    let backup = complete(&service, id, BackupOperation::Create);
    let handle = store.tenant(id).unwrap();
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
    assert!(failed.error.as_ref().unwrap().contains("BUSY"));
    assert!(
        !dir.path()
            .join("data/backup-work")
            .join(failed.id.to_string())
            .exists()
    );
    assert_eq!(
        handle.get("assets", "a").unwrap().unwrap()["owner"],
        "before"
    );
    drop(handle);
    drop(service);
    drop(store);
    let legacy = dir
        .path()
        .join("data/backup-work")
        .join(failed.id.to_string());
    fs::create_dir_all(&legacy).unwrap();
    fs::write(legacy.join("checkpoint"), b"legacy failed download").unwrap();
    let store = Store::open(dir.path().join("data"), Options::default()).unwrap();
    let _service = BackupService::open(store, cfg).unwrap();
    assert!(!legacy.exists());
}

#[test]
fn subsystem_review_one_tenant_cannot_fill_the_backup_job_table() {
    let (_dir, store, id, mut cfg) = setup();
    cfg.max_jobs = 4;
    let other = store.create_tenant("other").unwrap().id;
    let service = BackupService::open(store, cfg).unwrap();
    for _ in 0..2 {
        assert_eq!(
            wait(
                &service,
                &start(
                    &service,
                    id,
                    BackupOperation::Verify {
                        backup: Uuid::new_v4()
                    }
                )
            )
            .state,
            JobState::Failed
        );
    }
    assert_eq!(
        service
            .submit(id, BackupOperation::Create)
            .unwrap_err()
            .code,
        "RESOURCE_EXHAUSTED"
    );
    complete(&service, other, BackupOperation::Create);
}

#[test]
fn subsystem_review_retained_create_cannot_exhaust_other_tenants_staging() {
    let (dir, store, id, mut cfg) = setup();
    cfg.max_backup_bytes = 1 << 20;
    cfg.max_staging_bytes = 3 << 20;
    let other = store.create_tenant("other").unwrap().id;
    let service = BackupService::open(store.clone(), cfg).unwrap();
    let blocked = dir.path().join(format!("remote/tenants/{id}/objects"));
    fs::create_dir_all(blocked.parent().unwrap()).unwrap();
    fs::write(&blocked, b"blocked").unwrap();
    let failed = wait(&service, &start(&service, id, BackupOperation::Create));
    assert_eq!(failed.state, JobState::Failed);
    assert!(
        dir.path()
            .join("data/backup-work")
            .join(failed.id.to_string())
            .exists()
    );
    fs::write(
        dir.path()
            .join(format!("data/backup-work/{}/retained", failed.id)),
        vec![0; 600 << 10],
    )
    .unwrap();
    let rejected = wait(&service, &start(&service, id, BackupOperation::Create));
    assert_eq!(rejected.state, JobState::Failed);
    assert!(
        rejected
            .error
            .unwrap()
            .contains("tenant backup staging budget")
    );
    complete(&service, other, BackupOperation::Create);
    fs::remove_file(blocked).unwrap();
    assert_eq!(
        wait(&service, &service.retry(id, failed.id).unwrap()).state,
        JobState::Completed
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

fn publish_manifest_fixture(root: &Path, manifest: &BackupManifest) {
    let path = snapshot(root, manifest.tenant.id, manifest.backup);
    fs::create_dir_all(&path).unwrap();
    let bytes = serde_json::to_vec(manifest).unwrap();
    fs::write(path.join("manifest.json"), &bytes).unwrap();
    fs::write(
        path.join("COMPLETED"),
        serde_json::to_vec(
            &json!({"version":1,"manifest_sha256":format!("{:x}",Sha256::digest(&bytes))}),
        )
        .unwrap(),
    )
    .unwrap();
}

#[test]
fn subsystem_review_prune_recovers_beyond_the_snapshot_listing_limit() {
    let (dir, store, id, cfg) = setup();
    let service = BackupService::open(store, cfg).unwrap();
    let initial = complete(&service, id, BackupOperation::Create);
    let mut manifest: BackupManifest = serde_json::from_slice(
        &fs::read(snapshot(dir.path(), id, initial.backup).join("manifest.json")).unwrap(),
    )
    .unwrap();
    for _ in 0..4096 {
        manifest.backup = Uuid::new_v4();
        manifest.checkpoint_ms += 1;
        publish_manifest_fixture(dir.path(), &manifest);
    }
    let result = complete(&service, id, BackupOperation::Prune { keep_last: 1 });
    assert_eq!(result.result.unwrap()["deleted_backups"], 4096);
    let remaining = list(&service, id);
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].backup, manifest.backup);
    complete(
        &service,
        id,
        BackupOperation::Verify {
            backup: manifest.backup,
        },
    );
}

#[test]
fn subsystem_review_prune_bounds_memory_without_a_reference_count_dead_end() {
    let (dir, store, id, cfg) = setup();
    let service = BackupService::open(store, cfg).unwrap();
    let initial = complete(&service, id, BackupOperation::Create);
    let original: BackupManifest = serde_json::from_slice(
        &fs::read(snapshot(dir.path(), id, initial.backup).join("manifest.json")).unwrap(),
    )
    .unwrap();
    for n in 0..6 {
        let mut manifest = original.clone();
        manifest.backup = Uuid::new_v4();
        manifest.checkpoint_ms += n + 1;
        for i in 0..17_000 {
            manifest.files.push(BackupFile {
                path: format!("sst/{}.sst", Uuid::new_v4()),
                size: 1,
                sha256: format!("{:064x}", 1_000_000 + n * 17_000 + i),
            });
            manifest.total_bytes += 1;
        }
        publish_manifest_fixture(dir.path(), &manifest);
    }
    let objects = dir.path().join(format!(
        "remote/tenants/{id}/objects/{}",
        original.tenant.generation
    ));
    let orphan = objects.join("f".repeat(64));
    fs::write(&orphan, b"unreferenced").unwrap();
    let kept = objects.join(&original.files[0].sha256);
    assert!(kept.exists());
    let result = complete(&service, id, BackupOperation::Prune { keep_last: 6 });
    assert_eq!(result.result.unwrap()["deleted_backups"], 1);
    assert!(!orphan.exists());
    assert!(kept.exists());
    assert_eq!(list(&service, id).len(), 6);
}

#[test]
fn edge_review_lower_job_limit_preserves_startup_and_old_jobs() {
    let (_dir, store, id, mut cfg) = setup();
    let service = BackupService::open(store.clone(), cfg.clone()).unwrap();
    let mut jobs = Vec::new();
    for _ in 0..3 {
        let job = start(&service, id, BackupOperation::Create);
        assert_eq!(wait(&service, &job).state, JobState::Completed);
        jobs.push(job.id);
    }
    drop(service);
    cfg.max_jobs = 1;
    let service = BackupService::open(store.clone(), cfg).unwrap();
    assert_eq!(service.jobs(id).len(), 3);
    assert_eq!(
        service
            .submit(id, BackupOperation::Create)
            .unwrap_err()
            .code,
        "RESOURCE_EXHAUSTED"
    );
    for job in jobs {
        service.forget(id, job).unwrap();
    }
    let job = start(&service, id, BackupOperation::Create);
    assert_eq!(wait(&service, &job).state, JobState::Completed);
    assert!(
        store
            .tenant(id)
            .unwrap()
            .get("assets", "a")
            .unwrap()
            .is_some()
    );
}

#[test]
fn edge_review_backup_order_survives_clock_rollback_and_restart() {
    let (dir, store, id, cfg) = setup();
    let service = BackupService::open(store.clone(), cfg.clone()).unwrap();
    let older = start(&service, id, BackupOperation::Create);
    assert_eq!(wait(&service, &older).state, JobState::Completed);
    // A completed backup from before a wall-clock rollback must not outrank
    // a subsequently created checkpoint, including after local clock state loss.
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 86400 * 1000;
    let path = snapshot(dir.path(), id, older.backup);
    let mut manifest: BackupManifest =
        serde_json::from_slice(&fs::read(path.join("manifest.json")).unwrap()).unwrap();
    manifest.checkpoint_ms = future;
    publish_manifest_fixture(dir.path(), &manifest);
    let clock = store
        .root()
        .join("control/backup-jobs")
        .join(format!("CLOCK-{id}"));
    tenant_kv::storage::atomic_json(&clock, &1u64).unwrap();
    drop(service);
    let service = BackupService::open(store.clone(), cfg.clone()).unwrap();
    let newer = start(&service, id, BackupOperation::Create);
    let completed = wait(&service, &newer);
    assert_eq!(completed.state, JobState::Completed, "{completed:?}");
    assert_eq!(completed.result.unwrap()["checkpoint_ms"], future + 1);
    drop(service);
    let service = BackupService::open(store, cfg).unwrap();
    let newest = start(&service, id, BackupOperation::Create);
    let completed = wait(&service, &newest);
    assert_eq!(completed.result.unwrap()["checkpoint_ms"], future + 2);
    let prune = start(&service, id, BackupOperation::Prune { keep_last: 1 });
    assert_eq!(wait(&service, &prune).state, JobState::Completed);
    let backups = service.list(id).unwrap();
    assert_eq!(backups.len(), 1);
    assert_eq!(backups[0].backup, newest.backup);
    let verify = start(
        &service,
        id,
        BackupOperation::Verify {
            backup: newest.backup,
        },
    );
    assert_eq!(wait(&service, &verify).state, JobState::Completed);
}

#[test]
fn review0732_bad_or_oversized_inventory_does_not_block_new_backups_and_can_be_deleted() {
    let (dir, store, id, mut cfg) = setup();
    let service = BackupService::open(store.clone(), cfg.clone()).unwrap();
    let oversized = complete(&service, id, BackupOperation::Create);
    let broken = complete(&service, id, BackupOperation::Create);
    let future = complete(&service, id, BackupOperation::Create);
    let missing = complete(&service, id, BackupOperation::Create);
    drop(service);
    let load = |backup| -> BackupManifest {
        serde_json::from_slice(
            &fs::read(snapshot(dir.path(), id, backup).join("manifest.json")).unwrap(),
        )
        .unwrap()
    };
    let mut large = load(oversized.backup);
    large.files[0].size += 2 << 20;
    large.total_bytes += 2 << 20;
    publish_manifest_fixture(dir.path(), &large);
    fs::write(
        snapshot(dir.path(), id, broken.backup).join("manifest.json"),
        b"damaged",
    )
    .unwrap();
    let mut invalid_time = load(future.backup);
    invalid_time.checkpoint_ms = u64::MAX;
    publish_manifest_fixture(dir.path(), &invalid_time);
    fs::remove_file(snapshot(dir.path(), id, missing.backup).join("manifest.json")).unwrap();
    let clock = store
        .root()
        .join("control/backup-jobs")
        .join(format!("CLOCK-{id}"));
    tenant_kv::storage::atomic_json(&clock, &u64::MAX).unwrap();
    cfg.max_backup_bytes = 1 << 20;
    let service = BackupService::open(store, cfg).unwrap();
    let newest = complete(&service, id, BackupOperation::Create);
    let failed_prune = wait(
        &service,
        &start(&service, id, BackupOperation::Prune { keep_last: 1 }),
    );
    assert_eq!(failed_prune.state, JobState::Failed);
    assert!(
        snapshot(dir.path(), id, newest.backup)
            .join("COMPLETED")
            .exists()
    );
    for backup in [broken.backup, future.backup, missing.backup] {
        complete(&service, id, BackupOperation::Delete { backup });
    }
    complete(&service, id, BackupOperation::Prune { keep_last: 1 });
    let remaining = service.list(id).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].backup, newest.backup);
    complete(
        &service,
        id,
        BackupOperation::Verify {
            backup: newest.backup,
        },
    );
}

#[test]
fn reviewf647_future_timestamp_does_not_prevent_disaster_restore() {
    let (dir, store, id, cfg) = setup();
    let service = BackupService::open(store.clone(), cfg).unwrap();
    let created = complete(&service, id, BackupOperation::Create);
    let mut manifest: BackupManifest = serde_json::from_slice(
        &fs::read(snapshot(dir.path(), id, created.backup).join("manifest.json")).unwrap(),
    )
    .unwrap();
    manifest.checkpoint_ms = u64::MAX;
    publish_manifest_fixture(dir.path(), &manifest);
    let confirmed = store.tenant(id).unwrap().get("assets", "a").unwrap();
    complete(
        &service,
        id,
        BackupOperation::Verify {
            backup: created.backup,
        },
    );
    assert_eq!(list(&service, id)[0].checkpoint_ms, u64::MAX);
    store
        .tenant(id)
        .unwrap()
        .put("assets", json!({"_key":"a","ip":"changed"}), None)
        .unwrap();
    complete(
        &service,
        id,
        BackupOperation::Restore {
            backup: created.backup,
        },
    );
    assert_eq!(
        store.tenant(id).unwrap().get("assets", "a").unwrap(),
        confirmed
    );
}
