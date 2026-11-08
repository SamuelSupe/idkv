use super::lifecycle::TenantPreparation;
use super::*;

impl Store {
    /// Offline, explicitly destructive repair of the final WAL at an exact frame
    /// boundary. The root must not be served by another Store. Archives the original
    /// database outside the root before publishing a new tenant history and node epoch.
    /// Replicas of the old epoch must be rebuilt; this is not automatic recovery.
    pub fn repair_wal(
        root: impl AsRef<Path>,
        options: Options,
        tenant: Uuid,
        wal: Uuid,
        offset: u64,
        archive: impl AsRef<Path>,
    ) -> Result<serde_json::Value> {
        let store = Store::open(root, options)?;
        store.owner.stop.store(true, Ordering::Release);
        for worker in store.owner.workers.lock().drain(..) {
            let _ = worker.join();
        }
        let inner = &store.owner.inner;
        let permission = inner.node.write()?;
        let mut registration = inner
            .registry
            .read()
            .tenants
            .get(&tenant)
            .cloned()
            .ok_or_else(|| Error::missing("tenant"))?;
        if !matches!(registration.lifecycle, Lifecycle::Active) {
            return Err(Error::new(
                "RECOVERY_REQUIRED",
                "resolve the lifecycle journal before WAL repair",
            ));
        }
        let original_history = registration.info.history;
        let original = inner.root.join("tenants").join(tenant.to_string());
        let owner = store.offline_lock(tenant)?;
        let archive = archive.as_ref();
        let name = archive
            .file_name()
            .ok_or_else(|| Error::invalid("archive directory name"))?;
        let parent = archive
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()?;
        if parent.starts_with(&inner.root) {
            return Err(Error::invalid(
                "repair archive must be outside the Store root",
            ));
        }
        let archive = parent.join(name);
        let original_wal = original.join("wal").join(format!("{wal}.wal"));
        let original_bytes = fs::metadata(&original_wal)?.len();
        if offset >= original_bytes {
            return Err(Error::invalid(
                "truncate offset must discard a nonempty WAL suffix",
            ));
        }
        let slot = store.slot(tenant);
        let budget = Budget {
            global: inner.memory.clone(),
            tenant: slot.lock().memory.clone(),
        };
        let stage = Uuid::new_v4();
        let staged = inner.root.join("staging").join(stage.to_string());
        let mut preparation = TenantPreparation {
            slot: None,
            path: staged.clone(),
            journaled: false,
        };
        fs::create_dir(&staged)?;
        for entry in fs::read_dir(&original)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            reject_symlink(&entry.path())?;
            if name == "sst" {
                lifecycle::copy_tree(
                    &entry.path(),
                    &staged.join("sst"),
                    tenant,
                    &budget,
                    &inner.io,
                )?;
            } else if name == "wal" {
                fs::create_dir(staged.join("wal"))?;
                for file in fs::read_dir(entry.path())? {
                    let file = file?;
                    if file.path().extension().is_some_and(|e| e == "wal") {
                        crate::storage::copy_file(
                            &file.path(),
                            &staged.join("wal").join(file.file_name()),
                            tenant,
                            &budget,
                            &inner.io,
                        )?;
                    }
                }
                sync_dir(&staged.join("wal"))?;
            } else if name == "CURRENT" || name == "TENANT.meta" || name.starts_with("MANIFEST-") {
                crate::storage::copy_file(
                    &entry.path(),
                    &staged.join(entry.file_name()),
                    tenant,
                    &budget,
                    &inner.io,
                )?;
            }
        }
        Engine::truncate_repair_copy(&staged, &registration.info, wal, offset, &budget)?;
        registration.info.history = Uuid::new_v4();
        atomic_json(&staged.join("TENANT.meta"), &registration.info)?;
        let check = Engine::open(
            staged.clone(),
            registration.info.clone(),
            inner.options.clone(),
            budget.clone(),
            inner.io.clone(),
        )?;
        check.verify()?;
        let seq = check.replication_position()?;
        drop(check);
        // No serving state is changed until both the valid replacement and the
        // independent full archive have been synchronized successfully. Keeping
        // only WAL bytes is insufficient if the chosen boundary was too early.
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new().mode(0o700).create(&archive)?;
        let database = archive.join("database");
        lifecycle::copy_tree(&original, &database, tenant, &budget, &inner.io)?;
        let evidence = database.join("wal").join(format!("{wal}.wal"));
        let new_epoch = Uuid::new_v4();
        let mut report = serde_json::json!({"tenant":tenant,"wal":wal,"truncate_at":offset,
            "original_bytes":original_bytes,"discarded_bytes":original_bytes - offset,"retained_seq":seq,"new_epoch":new_epoch,
            "old_history":original_history,"new_history":registration.info.history,
            "database_archive":database,"evidence":evidence,"replicas_require_rebuild":true,
            "phase":"prepared"});
        let report_path = archive.join("repair.json");
        atomic_json(&report_path, &report)?;
        sync_dir(&archive)?;
        sync_dir(&parent)?;
        sync_dir(&inner.root.join("staging"))?;
        drop(permission);
        let result = (|| -> Result<()> {
            // This is a durable intent, not an atomic transaction with the
            // separate node state and tenant lifecycle journals.
            report["phase"] = "changing_epoch".into();
            atomic_json(&report_path, &report)?;
            {
                let _barrier = inner.node.barrier.write();
                let mut state = store.replication_state();
                state.epoch = new_epoch;
                state.parent = None;
                state.forced = true;
                inner.node.save(state)?;
            }
            report["phase"] = "epoch_changed".into();
            atomic_json(&report_path, &report)?;
            crate::storage::failpoint("repair_epoch_changed");
            crate::storage::errorpoint("repair_epoch_changed")?;
            drop(owner);
            report["phase"] = "publishing_tenant".into();
            atomic_json(&report_path, &report)?;
            match store.publish_ready_restore(registration, None, &mut preparation, seq, true) {
                Err(error) if error.code == "RESTORE_CLEANUP_FAILED" => {
                    report["phase"] = "published_cleanup_pending".into();
                    report["warning"] = error.to_string().into();
                }
                Err(error) => return Err(error),
                Ok(()) => report["phase"] = "published".into(),
            }
            atomic_json(&report_path, &report)
        })();
        if let Err(error) = result {
            let phase = report["phase"].as_str().unwrap_or("unknown").to_owned();
            report["failed_phase"] = phase.clone().into();
            report["phase"] = "failed".into();
            report["error"] = error.to_string().into();
            let saved = atomic_json(&report_path, &report);
            return Err(Error::new(
                &error.code,
                format!(
                    "repair failed during {phase}: {error}; epoch change may be durable (new_epoch={new_epoch}); inspect {} and node/lifecycle state before retry; failure report persisted={}",
                    report_path.display(),
                    saved.is_ok()
                ),
            ));
        }
        Ok(report)
    }
}
