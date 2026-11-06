use super::*;
use crate::storage::Engine;
use transfer::{Context, Staging};

impl Inner {
    pub(super) fn run(&self, id: Uuid) {
        let Some(job) = self.state.lock().jobs.get(&id).cloned() else {
            return;
        };
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.execute(job.clone())))
                .unwrap_or_else(|_| Err(Error::new("INTERNAL", "backup worker panicked")));
        let completed = result.is_ok();
        let persisted = self.update(id, |j| match result {
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
        if persisted.is_err() {
            if let Some(j) = self.state.lock().jobs.get_mut(&id) {
                j.state = JobState::Interrupted;
                j.error = Some(
                    "could not persist final job status; inspect remote completion before retry"
                        .into(),
                );
            }
        } else if completed {
            let _staging = self.staging.lock();
            let work = self.work_path.join(id.to_string());
            if work.exists() {
                let _ = fs::remove_dir_all(work);
            }
        }
        self.state.lock().active.remove(&job.tenant);
    }
    fn execute(&self, job: BackupJob) -> Result<serde_json::Value> {
        let (budget, io) = if matches!(job.operation, BackupOperation::Import { .. }) {
            self.store.import_context()
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
        let lock = self.tenant_lock(job.tenant);
        let _tenant = lock.lock();
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
                    self.update(job.id, |j| j.processed_files += 1)?;
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
        let _staging =
            Staging::reserve(self.inner, self.inner.config.max_backup_bytes, self.job.id)?;
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
            let seq = tenant.engine().checkpoint_limited(
                &checkpoint,
                self.inner.config.max_backup_bytes,
                self.inner.config.reserve_free_bytes,
            )?;
            let checkpoint_ms = now_ms();
            self.checkpoint_verify(&checkpoint, tenant.info(), seq)?;
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
                tenant: tenant.info(),
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
            self.inner.update(self.job.id, |j| {
                j.processed_files += 1;
                if uploaded {
                    j.uploaded_bytes += file.size;
                } else {
                    j.reused_bytes += file.size;
                }
            })?;
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
        let _staging = Staging::reserve(self.inner, reserve, self.job.id)?;
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
            self.inner.update(self.job.id, |j| j.processed_files += 1)?;
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
