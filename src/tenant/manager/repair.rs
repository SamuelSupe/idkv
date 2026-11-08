use super::lifecycle::TenantPreparation;
use super::*;

impl Store {
    /// Offline, explicitly destructive repair of the final WAL at an exact frame
    /// boundary. The root must not be served by another Store. Archives the original
    /// WAL outside the root before publishing a new tenant history and node epoch.
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
        let parent = archive
            .parent()
            .ok_or_else(|| Error::invalid("archive parent"))?
            .canonicalize()?;
        if parent.starts_with(&inner.root) {
            return Err(Error::invalid(
                "repair archive must be outside the Store root",
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
        // independent evidence copy have been synchronized successfully.
        fs::create_dir(archive)?;
        let evidence = archive.join(format!("{wal}.corrupt"));
        let original_wal = original.join("wal").join(format!("{wal}.wal"));
        crate::storage::copy_file(&original_wal, &evidence, tenant, &budget, &inner.io)?;
        for entry in fs::read_dir(original.join("wal"))? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|ext| ext == "corrupt")
                || entry.file_name() == "TORN-TAIL"
            {
                crate::storage::copy_file(
                    &entry.path(),
                    &archive.join(entry.file_name()),
                    tenant,
                    &budget,
                    &inner.io,
                )?;
            }
        }
        let new_epoch = Uuid::new_v4();
        let report = serde_json::json!({"tenant":tenant,"wal":wal,"truncate_at":offset,
            "original_bytes":fs::metadata(&original_wal)?.len(),"discarded_bytes":fs::metadata(&original_wal)?.len() - offset,"retained_seq":seq,"new_epoch":new_epoch,
            "old_history":original_history,"new_history":registration.info.history,
            "evidence":evidence,"replicas_require_rebuild":true});
        atomic_json(&archive.join("repair.json"), &report)?;
        sync_dir(archive)?;
        sync_dir(&parent)?;
        sync_dir(&inner.root.join("staging"))?;
        drop(permission);
        {
            let _barrier = inner.node.barrier.write();
            let mut state = store.replication_state();
            state.epoch = new_epoch;
            state.parent = None;
            state.forced = true;
            inner.node.save(state)?;
        }
        drop(owner);
        store.publish_ready_restore(registration, None, &mut preparation, seq, true)?;
        Ok(report)
    }
}
