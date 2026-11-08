//! Asynchronous, tenant-scoped checkpoint backups. Online commits never wait for
//! the remote store. One Store owns this repository's tenant namespaces; remote
//! multi-writer coordination and continuous WAL archiving are not provided.
mod jobs;
mod model;
mod repository;
mod transfer;
use crate::{
    Error, Result, Store,
    resources::{Budget, IoScheduler, RequestGate},
    storage::{atomic_json, read_json, reject_symlink, sync_dir},
};
use fs2::FileExt;
use model::*;
pub use model::{
    BackupConfig, BackupFile, BackupJob, BackupManifest, BackupOperation, BackupSummary, JobState,
};
use opendal::{
    blocking::Operator,
    layers::{RetryLayer, TimeoutLayer},
};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

struct State {
    jobs: BTreeMap<Uuid, BackupJob>,
    active: HashMap<Uuid, Uuid>,
}
struct Inner {
    store: Store,
    config: BackupConfig,
    op: Operator,
    upload_op: opendal::Operator,
    network: tokio::runtime::Handle,
    jobs_path: PathBuf,
    work_path: PathBuf,
    _lock: File,
    state: Mutex<State>,
    tenant_locks: Mutex<HashMap<Uuid, Arc<Mutex<()>>>>,
    sender: mpsc::SyncSender<Uuid>,
    stop: Arc<AtomicBool>,
    gate: RequestGate,
    bandwidth: Mutex<Instant>,
    staging: Mutex<HashMap<Uuid, (Uuid, u64)>>,
}
struct Owner {
    inner: Arc<Inner>,
    workers: Vec<thread::JoinHandle<()>>,
    _runtime: NetworkRuntime,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.inner.stop.store(true, Ordering::Release);
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}
struct NetworkRuntime(Option<tokio::runtime::Runtime>);
impl Drop for NetworkRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}
#[derive(Clone)]
pub struct BackupService {
    owner: Arc<Owner>,
}

impl BackupService {
    pub(crate) fn store_root(&self) -> &Path {
        self.owner.inner.store.root()
    }
    /// Start bounded workers and resume incomplete uploads. A restore that had
    /// started before a restart is never replayed automatically: doing so could
    /// overwrite writes made after an already successful directory switch.
    pub fn open(store: Store, mut config: BackupConfig) -> Result<Self> {
        config.validate()?;
        if config.service == "fs" {
            let path = config
                .options
                .get("root")
                .ok_or_else(|| Error::invalid("fs backup root required"))?;
            fs::create_dir_all(path)?;
            let path = fs::canonicalize(path)?;
            if path.starts_with(store.root()) || store.root().starts_with(&path) {
                return Err(Error::invalid(
                    "backup fs root must be separate from the database root",
                ));
            }
            config
                .options
                .insert("root".into(), path.to_string_lossy().into());
            config.options.insert(
                "atomic_write_dir".into(),
                path.join(".opendal-tmp")
                    .join(hash(store.root().as_os_str().as_encoded_bytes()))
                    .to_string_lossy()
                    .into(),
            );
        } else {
            config
                .options
                .entry("checksum_algorithm".into())
                .or_insert_with(|| "crc32c".into());
        }
        let jobs_path = store.root().join("control/backup-jobs");
        let work_path = store.root().join("backup-work");
        for path in [&jobs_path, &work_path] {
            fs::create_dir_all(path)?;
            reject_symlink(path)?;
        }
        let lock_path = jobs_path.join("LOCK");
        if lock_path.exists() {
            reject_symlink(&lock_path)?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        lock.try_lock_exclusive()
            .map_err(|_| Error::new("BUSY", "backup manager already open"))?;
        let identity: BTreeMap<_, _> = ["root", "bucket", "endpoint", "region"]
            .iter()
            .filter_map(|k| config.options.get(*k).map(|v| (*k, v)))
            .collect();
        let fingerprint = hash(&serde_json::to_vec(&(config.service.clone(), identity))?);
        let target = jobs_path.join("TARGET");
        if target.exists() {
            if read_json::<String>(&target)? != fingerprint {
                return Err(Error::invalid(
                    "backup target differs from persisted jobs; use a separate Store or archive the old backup job state first",
                ));
            }
        } else {
            atomic_json(&target, &fingerprint)?;
        }
        if config.service == "fs" {
            let temp = PathBuf::from(&config.options["atomic_write_dir"]);
            fs::create_dir_all(&temp)?;
            reject_symlink(&temp)?;
            for entry in fs::read_dir(&temp)? {
                let entry = entry?;
                reject_symlink(&entry.path())?;
                if entry.file_type()?.is_file() {
                    fs::remove_file(entry.path())?;
                }
            }
        }
        let runtime = NetworkRuntime(Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?,
        ));
        let op_result = {
            let _enter = runtime.0.as_ref().unwrap().enter();
            (|| {
                let op = opendal::Operator::via_iter(&config.service, config.options.clone())
                    .map_err(remote_error)?
                    .layer(
                        TimeoutLayer::new()
                            .with_timeout(Duration::from_secs(config.timeout_seconds)),
                    )
                    .layer(RetryLayer::new().with_jitter().with_max_times(3));
                let cap = op.info().full_capability();
                if !cap.read
                    || !cap.write
                    || !cap.stat
                    || !cap.list
                    || !cap.write_with_if_not_exists
                {
                    return Err(Error::invalid(
                        "backup backend requires read/write/stat/list and conditional create",
                    ));
                }
                Ok::<_, Error>((Operator::new(op.clone()).map_err(remote_error)?, op))
            })()
        };
        let (op, upload_op) = op_result?;
        let network = runtime.0.as_ref().unwrap().handle().clone();
        let mut jobs = BTreeMap::new();
        let mut active = HashMap::new();
        let mut resume = Vec::new();
        for entry in fs::read_dir(&jobs_path)? {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let mut job: BackupJob = read_json(&path)?;
            if path.file_stem().and_then(|s| s.to_str()) != Some(&job.id.to_string()) {
                return Err(Error::corrupt("backup job identity"));
            }
            if !job.state.terminal() {
                if matches!(
                    job.operation,
                    BackupOperation::Restore { .. } | BackupOperation::Import { .. }
                ) && job.state != JobState::Queued
                {
                    job.state = JobState::Interrupted;
                    job.error = Some(
                        "restore outcome requires inspection; it is not replayed automatically"
                            .into(),
                    );
                } else {
                    if active.insert(job.tenant, job.id).is_some() {
                        return Err(Error::corrupt("multiple active backup jobs for tenant"));
                    }
                    job.state = JobState::Queued;
                    job.uploaded_bytes = 0;
                    job.reused_bytes = 0;
                    job.processed_files = 0;
                    resume.push(job.id);
                }
                // Normalize only in memory. Workers persist their execution
                // state before side effects; a full disk must not prevent the
                // data service from starting. Interrupted restores are never queued.
            }
            jobs.insert(job.id, job);
            if jobs.len() > 4096 {
                return Err(Error::limit("persisted backup jobs exceed hard limit"));
            }
        }
        let mut cleaned_work = false;
        for job in jobs.values().filter(|job| {
            job.state.terminal()
                && (job.state == JobState::Completed
                    || !matches!(job.operation, BackupOperation::Create))
        }) {
            let work = work_path.join(job.id.to_string());
            if work.exists() {
                reject_symlink(&work)?;
                fs::remove_dir_all(work)?;
                cleaned_work = true;
            }
        }
        if cleaned_work {
            sync_dir(&work_path)?;
        }
        let (sender, receiver) = mpsc::sync_channel(config.max_jobs.max(resume.len()));
        let receiver = Arc::new(Mutex::new(receiver));
        let stop = Arc::new(AtomicBool::new(false));
        let inner = Arc::new(Inner {
            store,
            op,
            upload_op,
            network,
            jobs_path,
            work_path,
            _lock: lock,
            state: Mutex::new(State { jobs, active }),
            tenant_locks: Mutex::new(HashMap::new()),
            sender,
            stop,
            gate: RequestGate::new(config.concurrency),
            bandwidth: Mutex::new(Instant::now()),
            staging: Mutex::new(HashMap::new()),
            config,
        });
        let mut workers = Vec::new();
        for _ in 0..inner.config.concurrency {
            let inner = inner.clone();
            let receiver = receiver.clone();
            workers.push(thread::spawn(move || {
                while !inner.stop.load(Ordering::Acquire) {
                    let job = receiver.lock().recv_timeout(Duration::from_millis(100));
                    if let Ok(id) = job {
                        inner.run(id);
                    }
                }
            }));
        }
        for id in resume {
            inner
                .sender
                .try_send(id)
                .map_err(|_| Error::limit("backup restart queue"))?;
        }
        Ok(Self {
            owner: Arc::new(Owner {
                inner,
                workers,
                _runtime: runtime,
            }),
        })
    }

    /// Queue an operation. The returned job is not a completed remote backup.
    pub fn submit(&self, tenant: Uuid, operation: BackupOperation) -> Result<BackupJob> {
        let inner = &self.owner.inner;
        if matches!(operation, BackupOperation::Import { .. }) {
            if inner.store.contains_tenant(tenant) {
                return Err(Error::new(
                    "ALREADY_EXISTS",
                    "import requires an unregistered tenant UUID",
                ));
            }
        } else {
            inner.store.backup_context(tenant)?;
        }
        if let BackupOperation::Prune { keep_last } = &operation
            && !(1..=4096).contains(keep_last)
        {
            return Err(Error::invalid("keep_last must be 1..4096"));
        }
        let lock = inner.tenant_lock(tenant);
        let _tenant = lock
            .try_lock()
            .ok_or_else(|| Error::new("BUSY", "tenant backup operation active"))?;
        let mut state = inner.state.lock();
        if state.active.contains_key(&tenant) {
            return Err(Error::new("BUSY", "tenant already has a backup operation"));
        }
        if state.jobs.len() >= inner.config.max_jobs {
            return Err(Error::limit("backup job limit; forget terminal jobs"));
        }
        let tenant_limit = inner.config.max_jobs.div_ceil(2).min(64);
        if state
            .jobs
            .values()
            .filter(|job| job.tenant == tenant)
            .count()
            >= tenant_limit
        {
            return Err(Error::limit(
                "tenant backup job limit; forget terminal jobs",
            ));
        }
        let backup = match operation {
            BackupOperation::Restore { backup }
            | BackupOperation::Verify { backup }
            | BackupOperation::Import { backup } => backup,
            _ => Uuid::new_v4(),
        };
        let job = BackupJob {
            id: Uuid::new_v4(),
            tenant,
            backup,
            operation,
            state: JobState::Queued,
            created_ms: now_ms(),
            updated_ms: now_ms(),
            uploaded_bytes: 0,
            reused_bytes: 0,
            processed_files: 0,
            checkpoint_seq: None,
            result: None,
            error: None,
        };
        state.jobs.insert(job.id, job.clone());
        state.active.insert(tenant, job.id);
        drop(state);
        inner.queue_job(&job)?;
        Ok(job)
    }
    pub fn job(&self, tenant: Uuid, id: Uuid) -> Result<BackupJob> {
        self.owner
            .inner
            .state
            .lock()
            .jobs
            .get(&id)
            .filter(|j| j.tenant == tenant)
            .cloned()
            .ok_or_else(|| Error::missing("backup job"))
    }
    pub fn jobs(&self, tenant: Uuid) -> Vec<BackupJob> {
        self.owner
            .inner
            .state
            .lock()
            .jobs
            .values()
            .filter(|j| j.tenant == tenant)
            .cloned()
            .collect()
    }
    /// Retry failed creates/verifications/prunes. Restores require a new explicit
    /// operation after inspecting the database, because their result can be unknown.
    pub fn retry(&self, tenant: Uuid, id: Uuid) -> Result<BackupJob> {
        let inner = &self.owner.inner;
        let lock = inner.tenant_lock(tenant);
        let _tenant = lock
            .try_lock()
            .ok_or_else(|| Error::new("BUSY", "tenant backup operation active"))?;
        let mut state = inner.state.lock();
        if state
            .active
            .get(&tenant)
            .is_some_and(|active| *active != id)
        {
            return Err(Error::new("BUSY", "tenant backup operation active"));
        }
        let mut job = state
            .jobs
            .get(&id)
            .filter(|j| j.tenant == tenant)
            .cloned()
            .ok_or_else(|| Error::missing("backup job"))?;
        if state.active.get(&tenant) == Some(&id) && !job.state.terminal() {
            return Err(Error::new("BUSY", "tenant backup operation active"));
        }
        if !matches!(job.state, JobState::Failed | JobState::Interrupted)
            || matches!(
                job.operation,
                BackupOperation::Restore { .. } | BackupOperation::Import { .. }
            )
        {
            return Err(Error::invalid(
                "job cannot be retried; restore requires a new explicit operation",
            ));
        }
        job.state = JobState::Queued;
        job.error = None;
        job.updated_ms = now_ms();
        job.uploaded_bytes = 0;
        job.reused_bytes = 0;
        job.processed_files = 0;
        state.jobs.insert(id, job.clone());
        state.active.insert(tenant, id);
        drop(state);
        inner.queue_job(&job)?;
        Ok(job)
    }
    /// Remove local terminal-job state and staging only; remote backups are retained.
    pub fn forget(&self, tenant: Uuid, id: Uuid) -> Result<()> {
        let inner = &self.owner.inner;
        let lock = inner.tenant_lock(tenant);
        let _tenant = lock
            .try_lock()
            .ok_or_else(|| Error::new("BUSY", "tenant backup operation active"))?;
        let job = inner
            .state
            .lock()
            .jobs
            .get(&id)
            .filter(|j| j.tenant == tenant)
            .cloned()
            .ok_or_else(|| Error::missing("backup job"))?;
        if !job.state.terminal() {
            return Err(Error::new("BUSY", "backup job is active"));
        }
        let _staging = inner.staging.lock();
        let path = inner.work_path.join(id.to_string());
        if path.exists() {
            reject_symlink(&path)?;
            fs::remove_dir_all(path)?;
        }
        match fs::remove_file(inner.job_path(id)) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
            _ => {}
        }
        sync_dir(&inner.jobs_path)?;
        let mut state = inner.state.lock();
        state.jobs.remove(&id);
        if state.active.get(&tenant) == Some(&id) {
            state.active.remove(&tenant);
        }
        Ok(())
    }
    /// Lists only fully published remote snapshots. This is a blocking network operation.
    pub fn list(&self, tenant: Uuid) -> Result<Vec<BackupSummary>> {
        let inner = &self.owner.inner;
        let (_, budget, _) = inner.store.backup_context(tenant)?;
        let _memory = budget.reserve(48 << 20)?;
        let _gate = inner.gate.enter()?;
        let lock = inner.tenant_lock(tenant);
        let _tenant = lock
            .try_lock()
            .ok_or_else(|| Error::new("BUSY", "tenant backup operation active"))?;
        inner.summaries(tenant)
    }
}
impl Inner {
    // Admission reserves the row and tenant before publishing. An fsync error
    // may follow rename: retain that ownership until retry/forget or restart
    // resolves the same job, instead of admitting a second durable queued job.
    fn queue_job(&self, job: &BackupJob) -> Result<()> {
        let result = (|| {
            atomic_json(&self.job_path(job.id), job)?;
            crate::storage::errorpoint("backup_job_queued_published")?;
            self.sender
                .try_send(job.id)
                .map_err(|_| Error::limit("backup queue"))
        })();
        if let Err(error) = &result {
            let mut state = self.state.lock();
            let current = state.jobs.get_mut(&job.id).unwrap();
            current.state = JobState::Interrupted;
            current.error = Some(format!(
                "job admission uncertain; inspect, retry or forget this job: {error}"
            ));
        }
        result
    }
    fn job_path(&self, id: Uuid) -> PathBuf {
        self.jobs_path.join(format!("{id}.json"))
    }
    fn tenant_lock(&self, id: Uuid) -> Arc<Mutex<()>> {
        self.tenant_locks
            .lock()
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
    fn update(&self, id: Uuid, f: impl FnOnce(&mut BackupJob)) -> Result<()> {
        let job = self.persist_update(id, f)?;
        self.state.lock().jobs.insert(id, job);
        Ok(())
    }
    fn persist_update(&self, id: Uuid, f: impl FnOnce(&mut BackupJob)) -> Result<BackupJob> {
        let mut job = self
            .state
            .lock()
            .jobs
            .get(&id)
            .ok_or_else(|| Error::missing("backup job"))?
            .clone();
        f(&mut job);
        job.updated_ms = now_ms();
        if job.state.terminal() {
            crate::storage::errorpoint("backup_job_terminal_publish")?;
        }
        atomic_json(&self.job_path(id), &job)?;
        // The worker owns the tenant through terminal publication and cleanup;
        // readers can observe the previous durable phase while this I/O runs.
        Ok(job)
    }

    fn progress(&self, id: Uuid, uploaded_bytes: u64, reused_bytes: u64) -> Result<()> {
        let mut state = self.state.lock();
        let job = state
            .jobs
            .get_mut(&id)
            .ok_or_else(|| Error::missing("backup job"))?;
        job.processed_files += 1;
        job.uploaded_bytes += uploaded_bytes;
        job.reused_bytes += reused_bytes;
        job.updated_ms = now_ms();
        // Progress resets on resume; only phase transitions and terminal results
        // need durability. Per-file fsyncs compete with unrelated tenants' WALs.
        crate::storage::failpoint("backup_progress");
        Ok(())
    }
}
pub(crate) fn remote_error(e: opendal::Error) -> Error {
    let code = if e.kind() == opendal::ErrorKind::NotFound {
        "NOT_FOUND"
    } else {
        "BACKUP_STORAGE_ERROR"
    };
    // Backend error contexts may contain signed URLs or credential configuration.
    Error::new(
        code,
        format!("backup storage operation failed: {:?}", e.kind()),
    )
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readers_and_forget_remain_usable_after_slow_or_uncertain_metadata_updates() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data"), crate::Options::default()).unwrap();
        let id = store.create_tenant("metadata").unwrap().id;
        let service = BackupService::open(
            store,
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
        let job = service.submit(id, BackupOperation::Create).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while service.job(id, job.id).unwrap().state != JobState::Completed {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(10));
        }
        let inner = service.owner.inner.clone();
        let (entered, ready) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let updating = thread::spawn(move || {
            let lock = inner.tenant_lock(id);
            let _tenant = lock.lock();
            inner
                .update(job.id, |_| {
                    entered.send(()).unwrap();
                    wait.recv_timeout(Duration::from_secs(5)).unwrap();
                })
                .unwrap();
        });
        ready.recv_timeout(Duration::from_secs(5)).unwrap();
        let reader = service.clone();
        let (sent, received) = mpsc::channel();
        let reading = thread::spawn(move || {
            sent.send(reader.jobs(id)).unwrap();
        });
        let observed = received.recv_timeout(Duration::from_millis(200));
        release.send(()).unwrap();
        updating.join().unwrap();
        reading.join().unwrap();
        assert_eq!(
            observed
                .expect("task status blocked behind another metadata update")
                .len(),
            1
        );
        // A failed directory sync after unlink can leave only the in-memory row.
        // Retrying forget must finish this state instead of being stuck at ENOENT.
        fs::remove_file(service.owner.inner.job_path(job.id)).unwrap();
        service.forget(id, job.id).unwrap();
        assert!(service.jobs(id).is_empty());
    }
}
