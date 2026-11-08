use super::*;
use std::{
    io::{Read, Write},
    os::unix::fs::FileExt,
};

pub(super) struct Context<'a> {
    pub inner: &'a Inner,
    pub job: BackupJob,
    pub budget: Budget,
    pub io: Arc<IoScheduler>,
    pub started: Instant,
}
impl Context<'_> {
    pub fn check(&self) -> Result<()> {
        if self.inner.stop.load(Ordering::Acquire) {
            return Err(Error::new("INTERRUPTED", "backup service is stopping"));
        }
        if self.started.elapsed() > Duration::from_secs(self.inner.config.job_timeout_seconds) {
            return Err(Error::new("DEADLINE_EXCEEDED", "backup job deadline"));
        }
        Ok(())
    }
    fn throttle(&self, bytes: usize) -> Result<()> {
        self.check()?;
        let end = {
            let mut next = self.inner.bandwidth.lock();
            *next = (*next).max(Instant::now())
                + Duration::from_secs_f64(bytes as f64 / self.inner.config.bytes_per_second as f64);
            *next
        };
        while Instant::now() < end {
            self.check()?;
            thread::sleep(
                end.saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(100)),
            );
        }
        Ok(())
    }
    fn chunk(&self, file: Arc<File>, offset: u64, count: usize) -> Result<Vec<u8>> {
        self.throttle(count)?;
        self.io.run_background(self.job.tenant, move || {
            let mut bytes = vec![0; count];
            file.read_exact_at(&mut bytes, offset)?;
            Ok(bytes)
        })
    }
    pub fn local_hash(&self, path: &Path) -> Result<(u64, String)> {
        reject_symlink(path)?;
        let file = Arc::new(File::open(path)?);
        let size = file.metadata()?.len();
        if size > self.inner.config.max_backup_bytes {
            return Err(Error::limit("backup file byte limit"));
        }
        let mut digest = Sha256::new();
        let mut offset = 0;
        while offset < size {
            let bytes = self.chunk(
                file.clone(),
                offset,
                (size - offset).min(CHUNK as u64) as usize,
            )?;
            offset += bytes.len() as u64;
            digest.update(bytes);
        }
        Ok((size, format!("{:x}", digest.finalize())))
    }
    pub fn upload(&self, file: &BackupFile, path: &Path, generation: Uuid) -> Result<bool> {
        self.check()?;
        let key = object_key(self.job.tenant, generation, &file.sha256);
        match self.inner.op.stat(&key) {
            Ok(meta) => {
                if meta.content_length() != file.size {
                    return Err(Error::corrupt("existing backup object length mismatch"));
                }
                let tagged = meta
                    .user_metadata()
                    .and_then(|m| m.get("sha256"))
                    .is_some_and(|s| s == &file.sha256);
                // S3 validates CRC32C per transfer; immutable objects carry our
                // full-file digest. Backends without metadata are read back.
                if !tagged {
                    self.download(file, generation, None)?;
                }
                return Ok(false);
            }
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {}
            Err(e) => return Err(remote_error(e)),
        }
        let input = Arc::new(File::open(path)?);
        if input.metadata()?.len() != file.size {
            return Err(Error::corrupt("checkpoint file changed"));
        }
        let metadata = if self
            .inner
            .op
            .info()
            .full_capability()
            .write_with_user_metadata
        {
            Some(HashMap::from([("sha256".into(), file.sha256.clone())]))
        } else {
            None
        };
        let options = opendal::options::WriteOptions {
            if_not_exists: true,
            chunk: Some(8 << 20),
            concurrent: 1,
            user_metadata: metadata,
            ..Default::default()
        };
        self.inner.network.block_on(async {
            let mut writer = self
                .inner
                .upload_op
                .writer_options(&key, options)
                .await
                .map_err(remote_error)?;
            let result = async {
                let mut digest = Sha256::new();
                let mut offset = 0;
                while offset < file.size {
                    let bytes = self.chunk(
                        input.clone(),
                        offset,
                        (file.size - offset).min(CHUNK as u64) as usize,
                    )?;
                    offset += bytes.len() as u64;
                    digest.update(&bytes);
                    writer.write(bytes).await.map_err(remote_error)?;
                }
                if format!("{:x}", digest.finalize()) != file.sha256 {
                    return Err(Error::corrupt("checkpoint digest changed"));
                }
                writer.close().await.map_err(remote_error)?;
                Ok(())
            }
            .await;
            if result.is_err()
                && let Err(error) = writer.abort().await
            {
                eprintln!("backup upload abort failed for {}: {error}", self.job.id);
            }
            result
        })?;
        let meta = self.inner.op.stat(&key).map_err(remote_error)?;
        if meta.content_length() != file.size {
            return Err(Error::corrupt("uploaded backup object length mismatch"));
        }
        Ok(true)
    }
    pub fn download(
        &self,
        file: &BackupFile,
        generation: Uuid,
        destination: Option<&Path>,
    ) -> Result<()> {
        self.check()?;
        let key = object_key(self.job.tenant, generation, &file.sha256);
        let meta = self.inner.op.stat(&key).map_err(remote_error)?;
        if meta.content_length() != file.size {
            return Err(Error::corrupt("backup object length mismatch"));
        }
        let mut input = self
            .inner
            .op
            .reader(&key)
            .map_err(remote_error)?
            .into_std_read(..file.size)
            .map_err(remote_error)?;
        let mut target = if let Some(path) = destination {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
                reject_symlink(parent)?;
            }
            Some(OpenOptions::new().write(true).create_new(true).open(path)?)
        } else {
            None
        };
        let mut digest = Sha256::new();
        let mut remaining = file.size;
        while remaining > 0 {
            let count = remaining.min(CHUNK as u64) as usize;
            self.throttle(count)?;
            let mut bytes = vec![0; count];
            input
                .read_exact(&mut bytes)
                .map_err(|_| Error::new("BACKUP_STORAGE_ERROR", "backup stream read failed"))?;
            digest.update(&bytes);
            remaining -= count as u64;
            if let Some(mut output) = target.take() {
                target = Some(self.io.run_background(self.job.tenant, move || {
                    output.write_all(&bytes)?;
                    Ok(output)
                })?);
            }
        }
        if format!("{:x}", digest.finalize()) != file.sha256 {
            return Err(Error::corrupt("backup object SHA-256 mismatch"));
        }
        if let Some(output) = target {
            self.io.run_background(self.job.tenant, move || {
                output.sync_all()?;
                Ok(())
            })?;
        }
        Ok(())
    }
}
pub(super) fn object_key(tenant: Uuid, generation: Uuid, digest: &str) -> String {
    format!("tenants/{tenant}/objects/{generation}/{digest}")
}

pub(super) struct Staging<'a> {
    inner: &'a Inner,
    job: Uuid,
}
impl<'a> Staging<'a> {
    pub fn reserve(inner: &'a Inner, bytes: u64, own: Uuid, tenant: Uuid) -> Result<Self> {
        let mut reserved = inner.staging.lock();
        if reserved.contains_key(&own) {
            return Err(Error::new("BUSY", "backup staging already reserved"));
        }
        let mut retained = 0;
        let mut tenant_retained = 0;
        let owners: HashMap<_, _> = inner
            .state
            .lock()
            .jobs
            .values()
            .map(|job| (job.id, job.tenant))
            .collect();
        for entry in fs::read_dir(&inner.work_path)? {
            let path = entry?.path();
            let job = path
                .file_name()
                .and_then(|s| s.to_str())
                .and_then(|s| Uuid::parse_str(s).ok());
            let covered = if job == Some(own) {
                bytes
            } else {
                job.and_then(|id| reserved.get(&id))
                    .map_or(0, |(_, bytes)| *bytes)
            };
            let uncovered = tree_size(&path)?.saturating_sub(covered);
            retained += uncovered;
            if job == Some(own) || job.and_then(|id| owners.get(&id)).copied() == Some(tenant) {
                tenant_retained += uncovered;
            }
        }
        let tenant_limit = (inner.config.max_staging_bytes / 2).max(inner.config.max_backup_bytes);
        if tenant_retained
            .saturating_add(
                reserved
                    .values()
                    .filter(|(owner, _)| *owner == tenant)
                    .map(|(_, bytes)| *bytes)
                    .sum::<u64>(),
            )
            .saturating_add(bytes)
            > tenant_limit
        {
            return Err(Error::limit(
                "tenant backup staging budget; retry or forget retained creates",
            ));
        }
        if retained
            .saturating_add(reserved.values().map(|(_, bytes)| *bytes).sum::<u64>())
            .saturating_add(bytes)
            > inner.config.max_staging_bytes
        {
            return Err(Error::limit(
                "backup staging budget; forget failed jobs or increase max_staging_bytes",
            ));
        }
        reserved.insert(own, (tenant, bytes));
        Ok(Self { inner, job: own })
    }
}
impl Drop for Staging<'_> {
    fn drop(&mut self) {
        self.inner.staging.lock().remove(&self.job);
    }
}
pub(super) fn tree_size(path: &Path) -> Result<u64> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() {
        return Err(Error::invalid("backup workspace symlink"));
    }
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let mut size = 0u64;
    for entry in entries {
        match entry {
            Ok(entry) => size = size.saturating_add(tree_size(&entry.path())?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn staging_counts_uncovered_files_and_failed_workspaces() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data"), crate::Options::default()).unwrap();
        let service = BackupService::open(
            store,
            BackupConfig {
                service: "fs".into(),
                options: HashMap::from([(
                    "root".into(),
                    dir.path().join("remote").to_string_lossy().into(),
                )]),
                max_backup_bytes: 1 << 20,
                max_staging_bytes: 2 << 20,
                reserve_free_bytes: 0,
                ..Default::default()
            },
        )
        .unwrap();
        let inner = &service.owner.inner;
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let third = Uuid::new_v4();
        let held = Staging::reserve(inner, 1 << 20, first, first).unwrap();
        let work = inner.work_path.join(first.to_string());
        fs::create_dir(&work).unwrap();
        fs::write(work.join("extra"), vec![0; 1536 << 10]).unwrap();
        assert_eq!(
            Staging::reserve(inner, 1 << 20, second, second)
                .err()
                .unwrap()
                .code,
            "RESOURCE_EXHAUSTED"
        );
        fs::write(work.join("extra"), vec![0; 512 << 10]).unwrap();
        let other = Staging::reserve(inner, 1 << 20, second, second).unwrap();
        drop(held);
        assert_eq!(
            Staging::reserve(inner, 1 << 20, third, third)
                .err()
                .unwrap()
                .code,
            "RESOURCE_EXHAUSTED"
        );
        fs::remove_dir_all(work).unwrap();
        let last = Staging::reserve(inner, 1 << 20, third, third).unwrap();
        drop(last);
        drop(other);
    }
    #[test]
    fn edge_review_failed_stream_aborts_fs_upload_and_cleans_stale_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data"), crate::Options::default()).unwrap();
        let tenant = store.create_tenant("abort-upload").unwrap();
        let config = BackupConfig {
            service: "fs".into(),
            options: HashMap::from([(
                "root".into(),
                dir.path().join("remote").to_string_lossy().into(),
            )]),
            reserve_free_bytes: 0,
            bytes_per_second: 100 << 20,
            ..Default::default()
        };
        let service = BackupService::open(store.clone(), config.clone()).unwrap();
        let inner = &service.owner.inner;
        let (_, budget, io) = store.backup_context(tenant.id).unwrap();
        let job = BackupJob {
            id: Uuid::new_v4(),
            tenant: tenant.id,
            backup: Uuid::new_v4(),
            operation: BackupOperation::Create,
            state: JobState::Uploading,
            created_ms: 0,
            updated_ms: 0,
            uploaded_bytes: 0,
            reused_bytes: 0,
            processed_files: 0,
            checkpoint_seq: None,
            result: None,
            error: None,
        };
        let input = dir.path().join("input");
        fs::write(&input, vec![1; 12 << 20]).unwrap();
        let context = Context {
            inner,
            job,
            budget,
            io,
            started: Instant::now(),
        };
        let file = BackupFile {
            path: "sst/test.sst".into(),
            size: 12 << 20,
            sha256: "0".repeat(64),
        };
        assert_eq!(
            context
                .upload(&file, &input, tenant.generation)
                .unwrap_err()
                .code,
            "CORRUPTION"
        );
        let temp = PathBuf::from(&inner.config.options["atomic_write_dir"]);
        assert_eq!(
            fs::read_dir(&temp).unwrap().count(),
            0,
            "aborted upload leaked a temporary file"
        );
        assert!(
            !inner
                .op
                .exists(&object_key(tenant.id, tenant.generation, &file.sha256))
                .unwrap()
        );
        fs::write(temp.join("interrupted-process"), b"partial").unwrap();
        drop(service);
        let _service = BackupService::open(store, config).unwrap();
        assert_eq!(fs::read_dir(temp).unwrap().count(), 0);
        assert_eq!(tree_size(&dir.path().join("already-removed")).unwrap(), 0);
    }
}
