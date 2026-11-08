use super::*;
use crate::storage::Engine;
use transfer::{Context, Staging};

impl Inner {
    pub(super) fn run(&self, id: Uuid) {
        let Some(job) = self.state.lock().jobs.get(&id).cloned() else {
            return;
        };
        let lock = self.tenant_lock(job.tenant);
        let _tenant = lock.lock();
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.execute(job.clone())))
                .unwrap_or_else(|_| Err(Error::new("INTERNAL", "backup worker panicked")));
        let completed = result.is_ok();
        if let Err(error) = &result
            && error.code == "BACKUP_CLOCK_UNSAFE"
        {
            self.prune_clock_unsafe.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "backup prune blocked: tenant={} job={id}: {error}",
                job.tenant
            );
        }
        let persisted = self.persist_update(id, |j| match result {
            Ok(result) => {
                j.state = JobState::Completed;
                j.result = Some(result);
                j.error = None;
            }
            Err(error) => {
                j.state = if error.code == "INTERRUPTED" {
                    JobState::Interrupted
                } else {
                    JobState::Failed
                };
                j.error = Some(error.to_string());
            }
        });
        let published = persisted.is_ok();
        if (published && completed)
            || matches!(
                job.operation,
                BackupOperation::Restore { .. } | BackupOperation::Import { .. }
            )
        {
            let _staging = self.staging.lock();
            let work = self.work_path.join(id.to_string());
            if work.exists()
                && let Err(error) = reject_symlink(&work)
                    .and_then(|_| fs::remove_dir_all(work).map_err(Error::from))
                    .and_then(|_| sync_dir(&self.work_path))
            {
                eprintln!("backup workspace cleanup {id}: {error}");
            }
        }
        let mut state = self.state.lock();
        match persisted {
            Ok(job) => {
                state.jobs.insert(id, job);
            }
            Err(_) => {
                if let Some(j) = state.jobs.get_mut(&id) {
                    j.state = JobState::Interrupted;
                    j.error = Some("could not persist final job status; inspect remote completion before retry".into());
                }
            }
        }
        if published {
            state.active.remove(&job.tenant);
        }
        // Publish the terminal state and release tenant ownership together.
        // Seeing Completed must not race with cleanup or a still-held gate.
        drop(_tenant);
        drop(state);
    }
    fn execute(&self, job: BackupJob) -> Result<serde_json::Value> {
        let (budget, io) = if matches!(job.operation, BackupOperation::Import { .. }) {
            self.store.import_context(None)
        } else {
            let (_, budget, io) = self.store.backup_context(job.tenant)?;
            (budget, io)
        };
        let _memory = budget.reserve(48 << 20)?;
        let _gate = loop {
            if self.stop.load(Ordering::Acquire) {
                return Err(Error::new("INTERRUPTED", "backup service stopping"));
            }
            match self.gate.enter() {
                Ok(permit) => break permit,
                Err(_) => thread::sleep(Duration::from_millis(20)),
            }
        };
        let context = Context {
            inner: self,
            job: job.clone(),
            budget,
            io,
            started: Instant::now(),
        };
        match job.operation {
            BackupOperation::Create => context.create(),
            BackupOperation::Verify { backup } => {
                self.update(job.id, |j| j.state = JobState::Verifying)?;
                let manifest = self.manifest(job.tenant, backup)?;
                for file in &manifest.files {
                    context.download(file, manifest.tenant.generation, None)?;
                    self.progress(job.id, 0, 0)?;
                }
                Ok(serde_json::to_value(manifest.summary())?)
            }
            BackupOperation::Restore { backup } => {
                self.update(job.id, |j| j.state = JobState::Restoring)?;
                let manifest = self.manifest(job.tenant, backup)?;
                let (info, _, _) = self.store.backup_context(job.tenant)?;
                if manifest.tenant.generation != info.generation {
                    return Err(Error::invalid(
                        "backup belongs to a different database generation",
                    ));
                }
                context.restore(&manifest, false)
            }
            BackupOperation::Import { backup } => {
                self.update(job.id, |j| j.state = JobState::Restoring)?;
                let manifest = self.manifest(job.tenant, backup)?;
                context.restore(&manifest, true)
            }
            BackupOperation::Delete { backup } => {
                self.update(job.id, |j| j.state = JobState::Pruning)?;
                context.check()?;
                self.delete_backup(job.tenant, backup)
            }
            BackupOperation::Prune { keep_last } => {
                self.update(job.id, |j| j.state = JobState::Pruning)?;
                self.prune(job.tenant, keep_last, || context.check())
            }
        }
    }
}

impl Context<'_> {
    fn work(&self) -> PathBuf {
        self.inner.work_path.join(self.job.id.to_string())
    }
    fn create(&self) -> Result<serde_json::Value> {
        // The marker may already be durable even if the old process lost its
        // response or crashed before persisting Completed locally.
        match self.inner.manifest(self.job.tenant, self.job.backup) {
            Ok(manifest) => return Ok(serde_json::to_value(manifest.summary())?),
            Err(e) if e.code == "NOT_FOUND" => {}
            Err(e) => return Err(e),
        }
        let _staging = Staging::reserve(
            self.inner,
            self.inner.config.max_backup_bytes,
            self.job.id,
            self.job.tenant,
            false,
        )?;
        let work = self.work();
        fs::create_dir_all(&work)?;
        reject_symlink(&work)?;
        let local = work.join("manifest.local");
        let checkpoint = work.join("checkpoint");
        let manifest: BackupManifest = if local.exists() {
            let m: BackupManifest = read_json(&local)?;
            m.validate(
                self.job.tenant,
                self.job.backup,
                self.inner.config.max_backup_bytes,
            )?;
            m
        } else {
            self.inner
                .update(self.job.id, |j| j.state = JobState::Checkpointing)?;
            if checkpoint.exists() {
                reject_symlink(&checkpoint)?;
                fs::remove_dir_all(&checkpoint)?;
            }
            let tenant = self.inner.store.tenant(self.job.tenant)?;
            let info = tenant.info();
            let seq = tenant.engine().checkpoint_limited(
                &checkpoint,
                self.inner.config.max_backup_bytes,
                self.inner.config.reserve_free_bytes,
            )?;
            let checkpoint_ms = self
                .inner
                .checkpoint_time(self.job.tenant, || self.check())?;
            drop(tenant);
            self.checkpoint_verify(&checkpoint, info.clone(), seq)?;
            let mut paths = vec!["TENANT.meta".to_string(), "CURRENT".to_string()];
            let current = fs::read_to_string(checkpoint.join("CURRENT"))?;
            if !valid_file(&current) || !current.starts_with("MANIFEST-") {
                return Err(Error::corrupt("checkpoint CURRENT"));
            }
            paths.push(current);
            for dir in ["sst", "wal"] {
                for entry in fs::read_dir(checkpoint.join(dir))? {
                    let name = entry?.file_name().to_string_lossy().to_string();
                    let path = format!("{dir}/{name}");
                    if !valid_file(&path) {
                        return Err(Error::corrupt("checkpoint filename"));
                    }
                    paths.push(path);
                    if paths.len() > FILE_LIMIT {
                        return Err(Error::limit("backup file count"));
                    }
                }
            }
            paths.sort();
            let mut files = Vec::new();
            let mut total = 0;
            for path in paths {
                let (size, sha256) = self.local_hash(&checkpoint.join(&path))?;
                total += size;
                files.push(BackupFile { path, size, sha256 });
            }
            let manifest = BackupManifest {
                version: 1,
                backup: self.job.backup,
                tenant: info,
                checkpoint_seq: seq,
                checkpoint_ms,
                total_bytes: total,
                files,
            };
            manifest.validate(
                self.job.tenant,
                self.job.backup,
                self.inner.config.max_backup_bytes,
            )?;
            if serde_json::to_vec(&manifest)?.len() > META_LIMIT {
                return Err(Error::limit("backup manifest size"));
            }
            atomic_json(&local, &manifest)?;
            manifest
        };
        self.inner.update(self.job.id, |j| {
            j.state = JobState::Uploading;
            j.checkpoint_seq = Some(manifest.checkpoint_seq);
        })?;
        for file in &manifest.files {
            self.check()?;
            let uploaded = self.upload(
                file,
                &checkpoint.join(&file.path),
                manifest.tenant.generation,
            )?;
            self.inner.progress(
                self.job.id,
                if uploaded { file.size } else { 0 },
                if uploaded { 0 } else { file.size },
            )?;
        }
        self.inner.publish_backup(&manifest)?;
        Ok(serde_json::to_value(manifest.summary())?)
    }
    fn checkpoint_verify(&self, path: &Path, info: crate::TenantInfo, seq: u64) -> Result<()> {
        let engine = Engine::open(
            path.to_path_buf(),
            info,
            self.inner.store.options().clone(),
            self.budget.clone(),
            self.io.clone(),
        )?;
        if engine.seq() != seq {
            return Err(Error::corrupt("backup checkpoint sequence mismatch"));
        }
        engine.verify()
    }
    fn restore(&self, manifest: &BackupManifest, import: bool) -> Result<serde_json::Value> {
        let reserve = manifest
            .total_bytes
            .saturating_mul(2)
            .saturating_add(1 << 20);
        let _staging = Staging::reserve(self.inner, reserve, self.job.id, self.job.tenant, true)?;
        if fs2::available_space(&self.inner.work_path)?
            < reserve.saturating_add(self.inner.config.reserve_free_bytes)
        {
            return Err(Error::limit("restore free disk reserve"));
        }
        // Recovery must not require opening the damaged serving engine. The
        // lifecycle publication rechecks the registration and directory lock.
        let info = if import {
            let mut info = manifest.tenant.clone();
            info.memory_bytes = self.inner.store.options().tenant_memory_bytes;
            info.disk_bytes = self.inner.store.options().tenant_disk_bytes;
            info
        } else {
            self.inner.store.backup_context(self.job.tenant)?.0
        };
        if manifest.total_bytes > info.disk_bytes {
            return Err(Error::limit("restore tenant disk quota"));
        }
        let work = self.work();
        fs::create_dir_all(&work)?;
        reject_symlink(&work)?;
        let checkpoint = work.join("checkpoint");
        if checkpoint.exists() {
            reject_symlink(&checkpoint)?;
            fs::remove_dir_all(&checkpoint)?;
        }
        fs::create_dir(&checkpoint)?;
        fs::create_dir(checkpoint.join("sst"))?;
        fs::create_dir(checkpoint.join("wal"))?;
        for file in &manifest.files {
            self.download(
                file,
                manifest.tenant.generation,
                Some(&checkpoint.join(&file.path)),
            )?;
            self.inner.progress(self.job.id, 0, 0)?;
        }
        for dir in [
            checkpoint.join("sst"),
            checkpoint.join("wal"),
            checkpoint.clone(),
            work.clone(),
        ] {
            sync_dir(&dir)?;
        }
        self.checkpoint_verify(&checkpoint, info.clone(), manifest.checkpoint_seq)?;
        if import {
            self.inner.store.import_checkpoint(&checkpoint, info)?;
            crate::storage::failpoint("backup_restore_switched");
            return Ok(serde_json::to_value(manifest.summary())?);
        }
        self.inner
            .store
            .restore_checkpoint(self.job.tenant, &checkpoint)?;
        crate::storage::failpoint("backup_restore_switched");
        Ok(serde_json::to_value(manifest.summary())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Options;
    use serde_json::json;

    #[test]
    fn checkpoint_hashing_does_not_keep_the_serving_tenant_open() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data"), Options::default()).unwrap();
        let info = store.create_tenant("hash-drain").unwrap();
        let ack = {
            let tenant = store.tenant(info.id).unwrap();
            tenant.create_collection("docs").unwrap();
            tenant
                .put("docs", json!({"_key":"ack","value":7}), None)
                .unwrap()
        };
        let service = BackupService::open(
            store.clone(),
            BackupConfig {
                service: "fs".into(),
                options: HashMap::from([(
                    "root".into(),
                    dir.path().join("remote").to_string_lossy().into(),
                )]),
                ..Default::default()
            },
        )
        .unwrap();
        // A delayed hash read exposes the ownership boundary without needing a
        // large fixture. The private checkpoint must outlive the serving core.
        *service.owner.inner.bandwidth.lock() = Instant::now() + Duration::from_secs(3);
        let job = service.submit(info.id, BackupOperation::Create).unwrap();
        let checkpoint = store
            .root()
            .join("backup-work")
            .join(job.id.to_string())
            .join("checkpoint");
        let deadline = Instant::now() + Duration::from_secs(10);
        while !checkpoint.join("WAL-SYNC").exists() {
            assert!(
                Instant::now() < deadline,
                "checkpoint verification did not finish"
            );
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            service.job(info.id, job.id).unwrap().state,
            JobState::Checkpointing
        );
        store
            .close_tenant(info.id)
            .expect("private checkpoint hashing pinned the serving tenant");
        store
            .set_quota(info.id, info.memory_bytes * 2, info.disk_bytes)
            .unwrap();
        loop {
            let current = service.job(info.id, job.id).unwrap();
            if current.state.terminal() {
                assert_eq!(current.state, JobState::Completed, "{current:?}");
                break;
            }
            assert!(Instant::now() < deadline, "backup did not finish");
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(service.list(info.id).unwrap().len(), 1);
        drop(service);
        drop(store);
        let store = Store::open(dir.path().join("data"), Options::default()).unwrap();
        let doc = store
            .tenant(info.id)
            .unwrap()
            .get("docs", "ack")
            .unwrap()
            .unwrap();
        assert_eq!(doc["_rev"], ack.rev);
        assert_eq!(doc["value"], 7);
    }
}
