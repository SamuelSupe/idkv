use super::{
    Config, Exports, Fence, LogPage, LogRequest, MAX_WIRE_BYTES, NodeState, Role, Source,
    TenantPosition,
    wire::{LOG_ACCEPT, LOG_MEDIA_TYPE, LogExport, LogReply, decode_log_reply},
};
use crate::{Error, Result, Store};
use fs2::FileExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;

#[derive(Clone, Serialize, Deserialize)]
pub struct Status {
    pub node: NodeState,
    pub tenants: Vec<TenantPosition>,
    pub unavailable: Vec<UnavailableTenant>,
    pub last_error: Option<String>,
    pub completed_cycles: u64,
    #[serde(default)]
    pub initialization_attempts: u64,
    #[serde(default)]
    pub initialization_failures: u64,
    #[serde(default)]
    pub reinitializations: u64,
    #[serde(default)]
    pub initializing: Vec<Uuid>,
    #[serde(default)]
    pub waiting_for_readers: Vec<Uuid>,
    #[serde(default)]
    pub initialization_errors: HashMap<Uuid, String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct UnavailableTenant {
    pub info: crate::TenantInfo,
    pub error: String,
}
enum TenantSync {
    Idle,
    Pending(u64),
}
struct Progress {
    error: Option<String>,
    cycles: u64,
    cursors: HashMap<Uuid, super::LogCursor>,
    initializing: HashMap<Uuid, tokio::task::JoinHandle<Result<()>>>,
    retry_at: HashMap<Uuid, Instant>,
    confirmed: HashMap<Uuid, u64>,
    initialization_attempts: u64,
    initialization_failures: u64,
    reinitializations: u64,
    initialization_backoff: HashMap<Uuid, Duration>,
    log_backoff: HashMap<Uuid, (Instant, Duration)>,
    interrupted_log_bodies: HashMap<Uuid, u8>,
    waiting_for_readers: HashSet<Uuid>,
    initialization_errors: HashMap<Uuid, String>,
    status_cache: Option<(Instant, Arc<Received<Status>>)>,
    catalog_seen: Option<Instant>,
    last_tenant: Option<Uuid>,
}
struct Inner {
    store: Store,
    config: Config,
    client: reqwest::Client,
    cycle: AsyncMutex<()>,
    progress: Mutex<Progress>,
    exports: Mutex<Exports>,
    _lock: File,
}
#[derive(Clone)]
pub struct Service {
    inner: Arc<Inner>,
}

struct PendingExport<'a> {
    exports: &'a Mutex<Exports>,
    preparation: Option<super::snapshot::Preparation>,
}

fn flush_replica_buffer(engine: &crate::storage::Engine) -> Result<()> {
    let deadline = Instant::now() + Duration::from_millis(100);
    loop {
        match engine.flush() {
            Err(error) if error.code == "BUSY" && Instant::now() < deadline => {
                // Keep the downloaded, unappended page while another tenant
                // finishes a short reclaim. Never retry an uncertain append.
                std::thread::sleep(Duration::from_millis(2));
            }
            result => return result,
        }
    }
}
impl Drop for PendingExport<'_> {
    fn drop(&mut self) {
        if let Some(preparation) = self.preparation.take() {
            let _ = self.exports.lock().finish(
                preparation,
                Err(Error::new("INTERNAL", "checkpoint preparation interrupted")),
            );
        }
    }
}

pub(super) struct Received<T> {
    value: T,
    _memory: crate::resources::Reservation,
}
impl<T> std::ops::Deref for Received<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl Service {
    pub fn open(store: Store, config: Config) -> Result<Self> {
        let url = reqwest::Url::parse(&config.peer_url)
            .map_err(|_| Error::invalid("replication peer URL"))?;
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
            || !matches!(url.scheme(), "http" | "https")
        {
            return Err(Error::invalid(
                "peer URL must be an http(s) origin without credentials",
            ));
        }
        if url.scheme() == "http"
            && !matches!(
                url.host_str(),
                Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
            )
        {
            return Err(Error::invalid(
                "remote replication requires HTTPS; HTTP is limited to loopback",
            ));
        }
        read_token(&config.token_file)?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(3))
            .pool_idle_timeout(Duration::from_secs(4))
            .build()
            .map_err(|_| Error::new("UNAVAILABLE", "replication HTTP client"))?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(store.root().join("control/replication-worker.lock"))?;
        lock.try_lock_exclusive()
            .map_err(|_| Error::new("BUSY", "replication service already open"))?;
        store.configure_replication(&config)?;
        let exports = Exports::open(&store)?;
        let imports = store.root().join("replication-work/import");
        fs::create_dir_all(&imports)?;
        crate::storage::reject_symlink(&imports)?;
        for entry in fs::read_dir(&imports)? {
            let entry = entry?;
            crate::storage::reject_symlink(&entry.path())?;
            if Uuid::parse_str(&entry.file_name().to_string_lossy()).is_ok() {
                fs::remove_dir_all(entry.path())?;
            }
        }
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                config,
                client,
                _lock: lock,
                cycle: AsyncMutex::new(()),
                exports: Mutex::new(exports),
                progress: Mutex::new(Progress {
                    error: None,
                    cycles: 0,
                    cursors: HashMap::new(),
                    initializing: HashMap::new(),
                    retry_at: HashMap::new(),
                    confirmed: HashMap::new(),
                    initialization_attempts: 0,
                    initialization_failures: 0,
                    reinitializations: 0,
                    initialization_backoff: HashMap::new(),
                    log_backoff: HashMap::new(),
                    interrupted_log_bodies: HashMap::new(),
                    waiting_for_readers: HashSet::new(),
                    initialization_errors: HashMap::new(),
                    status_cache: None,
                    catalog_seen: None,
                    last_tenant: None,
                }),
            }),
        })
    }
    pub(super) fn store(&self) -> &Store {
        &self.inner.store
    }
    pub fn store_root(&self) -> &std::path::Path {
        self.inner.store.root()
    }
    pub fn status(&self) -> Result<Status> {
        let mut tenants = Vec::new();
        let mut unavailable = Vec::new();
        for info in self.inner.store.tenants() {
            match self.inner.store.replication_position(info.id) {
                Ok(seq) => tenants.push(TenantPosition { info, seq }),
                Err(error) => unavailable.push(UnavailableTenant {
                    info,
                    error: error.to_string(),
                }),
            }
        }
        // Pending deletion is not an authoritative absence. Preserve the peer's
        // copy until restart resolves the uncertain lifecycle journal.
        for info in self.inner.store.pending_lifecycle_tenants() {
            tenants.retain(|entry| entry.info.id != info.id);
            if !unavailable.iter().any(|entry| entry.info.id == info.id) {
                unavailable.push(UnavailableTenant {
                    error: self.inner.store.lifecycle_error(info.id),
                    info,
                });
            }
        }
        let progress = self.inner.progress.lock();
        Ok(Status {
            node: self.inner.store.replication_state(),
            tenants,
            unavailable,
            last_error: progress.error.clone(),
            completed_cycles: progress.cycles,
            initialization_attempts: progress.initialization_attempts,
            initialization_failures: progress.initialization_failures,
            reinitializations: progress.reinitializations,
            initializing: progress.initializing.keys().copied().collect(),
            waiting_for_readers: progress.waiting_for_readers.iter().copied().collect(),
            initialization_errors: progress.initialization_errors.clone(),
        })
    }
    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            let mut expiration = Instant::now();
            let mut error_delay = Duration::from_millis(service.inner.config.poll_ms);
            loop {
                if expiration.elapsed() >= Duration::from_secs(1) {
                    let expiring = service.clone();
                    let result =
                        tokio::task::spawn_blocking(move || expiring.expire_exports()).await;
                    if let Ok(Err(error)) = result {
                        eprintln!("replication export cleanup: {error}");
                        service.inner.progress.lock().error = Some(error.to_string());
                    }
                    expiration = Instant::now();
                }
                if service.inner.store.replication_state().role == Role::Replica {
                    let result = {
                        let _cycle = service.inner.cycle.lock().await;
                        service.sync_inner(false).await
                    };
                    let pending = matches!(result, Ok(true));
                    if let Err(error) = &result {
                        service.inner.progress.lock().error = Some(error.to_string());
                        tokio::time::sleep(error_delay).await;
                        error_delay = (error_delay * 2).min(Duration::from_secs(30));
                        continue;
                    }
                    error_delay = Duration::from_millis(service.inner.config.poll_ms);
                    if pending {
                        // Each round gives every pending tenant a page before revisiting
                        // a hot tenant. Sleep only at the head or after an error.
                        tokio::task::yield_now().await;
                        continue;
                    }
                }
                tokio::time::sleep(Duration::from_millis(service.inner.config.poll_ms)).await;
            }
        })
    }
    /// Stop background checkpoint transfers after stopping the replication loop.
    /// An installation already handed to a blocking worker finishes atomically.
    pub async fn stop_initializations(&self) {
        let tasks: Vec<_> = self
            .inner
            .progress
            .lock()
            .initializing
            .drain()
            .map(|(_, task)| task)
            .collect();
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
        self.inner.progress.lock().waiting_for_readers.clear();
    }
    pub(super) async fn request<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Option<Value>,
    ) -> Result<Received<T>> {
        let timeout = Duration::from_secs(if path == "/v1/replication/promote" {
            180
        } else {
            120
        });
        self.request_bounded(path, body, timeout).await
    }
    pub(super) async fn request_bounded<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Option<Value>,
        timeout: Duration,
    ) -> Result<Received<T>> {
        self.request_with_deadlines(path, body, timeout, Duration::from_secs(120))
            .await
    }
    pub(super) async fn request_log(
        &self,
        request: Value,
        remaining: Duration,
    ) -> Result<Received<LogReply>> {
        self.request_decoded(
            "/v1/replication/log",
            Some(request),
            remaining,
            Duration::from_secs(120),
            true,
            decode_log_reply,
        )
        .await
    }
    async fn request_with_deadlines<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Option<Value>,
        remaining: Duration,
        log_timeout: Duration,
    ) -> Result<Received<T>> {
        self.request_decoded(path, body, remaining, log_timeout, false, |bytes, _| {
            Ok(serde_json::from_slice(&bytes)?)
        })
        .await
    }

    async fn request_decoded<T>(
        &self,
        path: &str,
        body: Option<Value>,
        remaining: Duration,
        log_timeout: Duration,
        binary_log: bool,
        decode: impl FnOnce(Vec<u8>, bool) -> Result<T>,
    ) -> Result<Received<T>> {
        let log = path == "/v1/replication/log";
        let timeout = if log {
            remaining.min(log_timeout)
        } else {
            remaining
        };
        let mut received_log_body = false;
        let tenant = log
            .then(|| body.as_ref()?.get("tenant")?.as_str()?.parse::<Uuid>().ok())
            .flatten();
        let received = tokio::time::timeout(
            timeout,
            self.receive_response(path, body, &mut received_log_body, binary_log, decode),
        )
        .await;
        if (received.is_ok() || !received_log_body)
            && let Some(tenant) = tenant
        {
            self.inner
                .progress
                .lock()
                .interrupted_log_bodies
                .remove(&tenant);
        }
        received.map_err(|_| {
            // Rotation must remain fair, but a tenant repeatedly receiving only
            // partial bodies also needs a way to progress without a full window.
            let repeated = if remaining < log_timeout
                && received_log_body
                && let Some(tenant) = tenant
            {
                let mut progress = self.inner.progress.lock();
                let attempts = progress.interrupted_log_bodies.entry(tenant).or_default();
                *attempts = attempts.saturating_add(1);
                *attempts >= 3
            } else {
                false
            };
            let snapshot = log && received_log_body && (remaining >= log_timeout || repeated);
            Error::new(
                if snapshot {
                    "REPLICA_SNAPSHOT_REQUIRED"
                } else if log {
                    "DEADLINE_EXCEEDED"
                } else {
                    "REPLICATION_UNAVAILABLE"
                },
                if snapshot {
                    "log body exceeded full request deadline or repeated cycle deadlines"
                } else {
                    "peer request deadline exceeded"
                },
            )
        })?
    }
    async fn receive_response<T>(
        &self,
        path: &str,
        body: Option<Value>,
        received_log_body: &mut bool,
        binary_log: bool,
        decode: impl FnOnce(Vec<u8>, bool) -> Result<T>,
    ) -> Result<Received<T>> {
        let url = format!(
            "{}{}",
            self.inner.config.peer_url.trim_end_matches('/'),
            path
        );
        let request = match body {
            Some(body) => self.inner.client.post(url).json(&body),
            None => self.inner.client.get(url),
        };
        let request = if binary_log {
            request.header(reqwest::header::ACCEPT, LOG_ACCEPT)
        } else {
            request
        };
        let token_path = self.inner.config.token_file.clone();
        let token = tokio::task::spawn_blocking(move || read_token(&token_path))
            .await
            .map_err(|_| Error::new("INTERNAL", "token reader failed"))?
            .map_err(|error| {
                if error.code == "REPLICATION_CREDENTIAL_INVALID" {
                    error
                } else {
                    Error::new(
                        "REPLICATION_UNAVAILABLE",
                        format!("peer credential unavailable: {error}"),
                    )
                }
            })?;
        let header_timeout = if path == "/v1/replication/promote" {
            150
        } else {
            30
        };
        let mut response = tokio::time::timeout(
            Duration::from_secs(header_timeout),
            request.bearer_auth(token).send(),
        )
        .await
        .map_err(|_| Error::new("REPLICATION_UNAVAILABLE", "peer headers timed out"))?
        .map_err(|_| Error::new("REPLICATION_UNAVAILABLE", "peer request failed"))?;
        let success = response.status().is_success();
        if response
            .content_length()
            .is_some_and(|n| n > MAX_WIRE_BYTES as u64)
        {
            return Err(Error::limit("peer response too large"));
        }
        let check_memory = |bytes: usize| -> Result<()> {
            if bytes > self.inner.store.foreground_memory_limit() {
                return Err(if success && path == "/v1/replication/log" {
                    Error::new(
                        "REPLICA_SNAPSHOT_REQUIRED",
                        "log response exceeds global decode budget",
                    )
                } else {
                    Error::limit("peer response exceeds global decode budget")
                });
            }
            Ok(())
        };
        let capacity = response.content_length().unwrap_or(0) as usize;
        check_memory(capacity)?;
        let mut input_memory = self.inner.store.reserve_http(capacity)?;
        let mut bytes = Vec::with_capacity(capacity);
        while let Some(chunk) = tokio::time::timeout(Duration::from_secs(30), response.chunk())
            .await
            .map_err(|_| Error::new("REPLICATION_UNAVAILABLE", "peer response stalled"))?
            .map_err(|_| Error::new("REPLICATION_UNAVAILABLE", "peer response interrupted"))?
        {
            if bytes.len() + chunk.len() > MAX_WIRE_BYTES {
                return Err(Error::limit("peer response too large"));
            }
            let needed = bytes.len() + chunk.len();
            if needed > bytes.capacity() {
                let capacity = needed.next_power_of_two().min(MAX_WIRE_BYTES);
                // Account for both allocations while the receive buffer grows.
                check_memory(capacity.saturating_add(bytes.capacity()))?;
                let next_memory = self.inner.store.reserve_http(capacity)?;
                bytes.reserve_exact(capacity - bytes.len());
                input_memory = next_memory;
            }
            if success && path == "/v1/replication/log" && !chunk.is_empty() {
                *received_log_body = true;
            }
            bytes.extend_from_slice(&chunk);
        }
        if success
            && binary_log
            && response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .is_some_and(|value| value == LOG_MEDIA_TYPE)
        {
            return Ok(Received {
                value: decode(bytes, true)?,
                _memory: input_memory,
            });
        }
        let decoded = crate::json::allocation_bound(&bytes);
        check_memory(input_memory.size().saturating_add(decoded))?;
        let memory = self.inner.store.reserve_http(decoded)?;
        if !success {
            let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            let code = value["error"]["code"].as_str().unwrap_or("");
            let code = match code {
                "BUSY"
                | "REPLICA_LAG"
                | "LOG_GONE"
                | "SNAPSHOT_EXPIRED"
                | "REPLICATION_CONFLICT"
                | "CONFLICT"
                | "READ_ONLY"
                | "RECOVERY_REQUIRED"
                | "REPLICATION_CREDENTIAL_INVALID"
                | "INCOMPATIBLE_VERSION"
                | "RESOURCE_EXHAUSTED"
                | "DEADLINE_EXCEEDED"
                | "REPLICA_SNAPSHOT_REQUIRED"
                | "CORRUPTION"
                | "COMMIT_UNKNOWN"
                | "REPLICATION_UNAVAILABLE" => code,
                _ => "REPLICATION_UNAVAILABLE",
            };
            let detail: String = value["error"]["message"]
                .as_str()
                .unwrap_or("")
                .chars()
                .filter(|c| !c.is_control())
                .take(512)
                .collect();
            return Err(Error::new(
                code,
                format!(
                    "peer rejected replication request ({}): {detail}",
                    response.status()
                ),
            ));
        }
        let value = decode(bytes, false)?;
        Ok(Received {
            value,
            _memory: memory,
        })
    }
    pub async fn sync_once(&self) -> Result<()> {
        let _cycle = self.inner.cycle.lock().await;
        self.sync_inner(true).await.map(|_| ())
    }
    async fn sync_inner(&self, report_error: bool) -> Result<bool> {
        self.sync_until(report_error, Instant::now() + Duration::from_secs(150))
            .await
    }
    async fn sync_until(&self, report_error: bool, deadline: Instant) -> Result<bool> {
        if self.inner.store.replication_state().role != Role::Replica {
            return Err(Error::new("CONFLICT", "node is not a replica"));
        }
        let cached = if report_error {
            None
        } else {
            self.inner
                .progress
                .lock()
                .status_cache
                .as_ref()
                .filter(|(at, _)| at.elapsed() < Duration::from_millis(100))
                .map(|(at, status)| (*at, status.clone()))
        };
        let (catalog_stamp, upstream) = if let Some(cached) = cached {
            cached
        } else {
            let status: Received<Status> = self
                .request_bounded(
                    "/v1/replication/status",
                    None,
                    deadline.saturating_duration_since(Instant::now()),
                )
                .await?;
            let entry = (Instant::now(), Arc::new(status));
            self.inner.progress.lock().status_cache = Some(entry.clone());
            entry
        };
        super::compatible(upstream.node.protocol)?;
        let store = self.inner.store.clone();
        let node = upstream.node.clone();
        let binding = store.clone();
        let source = tokio::task::spawn_blocking(move || binding.bind_source(&node))
            .await
            .map_err(|_| Error::new("INTERNAL", "source binding worker failed"))??;
        let catalog_seen = self.inner.progress.lock().catalog_seen == Some(catalog_stamp);
        let mut catalog = upstream.tenants.clone();
        catalog.extend(upstream.unavailable.iter().map(|t| TenantPosition {
            info: t.info.clone(),
            seq: 0,
        }));
        catalog.sort_by_key(|p| p.info.id);
        let catalog_store = store.clone();
        let catalog_source = source.clone();
        let blocked = if catalog_seen {
            HashMap::new()
        } else {
            tokio::task::spawn_blocking(move || {
                catalog_store.replica_catalog(&catalog_source, &catalog)
            })
            .await
            .map_err(|_| Error::new("INTERNAL", "replica catalog worker failed"))??
        };
        if blocked.is_empty() {
            self.inner.progress.lock().catalog_seen = Some(catalog_stamp);
        }
        let mut catalog = upstream.tenants.clone();
        catalog.extend(upstream.unavailable.iter().map(|t| TenantPosition {
            info: t.info.clone(),
            seq: 0,
        }));
        let active: HashSet<_> = catalog.iter().map(|p| p.info.id).collect();
        {
            let mut progress = self.inner.progress.lock();
            progress.cursors.retain(|id, _| active.contains(id));
            progress.confirmed.retain(|id, _| active.contains(id));
            progress.retry_at.retain(|id, _| active.contains(id));
            progress.log_backoff.retain(|id, _| active.contains(id));
            progress
                .interrupted_log_bodies
                .retain(|id, _| active.contains(id));
            progress
                .initialization_errors
                .retain(|id, _| active.contains(id));
            progress
                .initialization_backoff
                .retain(|id, _| active.contains(id));
        }
        let mut failure = blocked.values().next().cloned().or_else(|| {
            upstream.unavailable.first().map(|t| {
                Error::new(
                    "UNAVAILABLE",
                    format!("source tenant {} unavailable: {}", t.info.id, t.error),
                )
            })
        });
        let finished: Vec<_> = {
            let progress = self.inner.progress.lock();
            progress
                .initializing
                .iter()
                .filter(|(_, task)| task.is_finished())
                .map(|(id, _)| *id)
                .collect()
        };
        for id in finished {
            let task = self.inner.progress.lock().initializing.remove(&id).unwrap();
            let result = task.await.unwrap_or_else(|_| {
                Err(Error::new(
                    "INTERNAL",
                    "replica initialization worker failed",
                ))
            });
            let mut progress = self.inner.progress.lock();
            progress.waiting_for_readers.remove(&id);
            if let Err(error) = result {
                progress.initialization_failures += 1;
                progress.initialization_errors.insert(id, error.to_string());
                let delay = *progress
                    .initialization_backoff
                    .entry(id)
                    .or_insert(Duration::from_millis(100));
                progress.retry_at.insert(id, Instant::now() + delay);
                progress
                    .initialization_backoff
                    .insert(id, (delay * 2).min(Duration::from_secs(30)));
                failure.get_or_insert(error);
            } else {
                progress.initialization_errors.remove(&id);
                progress.initialization_backoff.remove(&id);
                progress.retry_at.remove(&id);
            }
        }
        let started = Instant::now();
        let mut positions = upstream.tenants.clone();
        if let Some(last) = self.inner.progress.lock().last_tenant
            && !positions.is_empty()
        {
            let start = positions.partition_point(|p| p.info.id <= last) % positions.len();
            positions.rotate_left(start);
        }
        for round in 0..8 {
            let mut next = Vec::new();
            for mut position in positions {
                if deadline.saturating_duration_since(Instant::now()) < Duration::from_millis(100) {
                    let error =
                        Error::new("DEADLINE_EXCEEDED", "replication cycle network deadline");
                    self.inner.progress.lock().error = Some(error.to_string());
                    return if report_error { Err(error) } else { Ok(true) };
                }
                if blocked.contains_key(&position.info.id) {
                    continue;
                }
                {
                    let progress = self.inner.progress.lock();
                    if progress.initializing.contains_key(&position.info.id)
                        || progress
                            .retry_at
                            .get(&position.info.id)
                            .is_some_and(|at| *at > Instant::now())
                        || progress
                            .log_backoff
                            .get(&position.info.id)
                            .is_some_and(|(at, _)| *at > Instant::now())
                    {
                        continue;
                    }
                }
                self.inner.progress.lock().last_tenant = Some(position.info.id);
                match self
                    .sync_tenant_until(&source, upstream.node.cluster, &position, deadline)
                    .await
                {
                    Ok(TenantSync::Idle) => {
                        let mut progress = self.inner.progress.lock();
                        progress.log_backoff.remove(&position.info.id);
                    }
                    Ok(TenantSync::Pending(head)) => {
                        self.inner
                            .progress
                            .lock()
                            .log_backoff
                            .remove(&position.info.id);
                        // The source may advance beyond the catalog snapshot.
                        // Compare the next page with the latest authenticated head.
                        position.seq = head;
                        next.push(position);
                    }
                    Err(error) => {
                        let mut progress = self.inner.progress.lock();
                        let cap = if matches!(
                            error.code.as_str(),
                            "BUSY" | "RESOURCE_EXHAUSTED" | "DEADLINE_EXCEEDED"
                        ) {
                            Duration::from_secs(1)
                        } else {
                            Duration::from_secs(30)
                        };
                        let delay = progress
                            .log_backoff
                            .get(&position.info.id)
                            .map_or(Duration::from_millis(100), |(_, next)| *next)
                            .min(cap);
                        progress.log_backoff.insert(
                            position.info.id,
                            (Instant::now() + delay, (delay * 2).min(cap)),
                        );
                        failure.get_or_insert(error);
                    }
                }
            }
            if next.is_empty() || round == 7 || started.elapsed() >= Duration::from_millis(10) {
                let mut progress = self.inner.progress.lock();
                progress.cycles += 1;
                progress.error = failure.as_ref().map(ToString::to_string);
                if report_error && let Some(error) = failure {
                    return Err(error);
                }
                return Ok(!next.is_empty());
            }
            // Every pending tenant receives one page per round. Refresh lifecycle
            // and role information after at most eight rounds or 10 ms of work;
            // each log request still checks epoch and storage generation itself.
            positions = next;
            tokio::task::yield_now().await;
        }
        unreachable!("bounded replication rounds return at the limit")
    }
    async fn sync_tenant_until(
        &self,
        source: &Source,
        cluster: Uuid,
        position: &TenantPosition,
        deadline: Instant,
    ) -> Result<TenantSync> {
        let store = self.inner.store.clone();
        let id = position.info.id;
        let position_store = store.clone();
        let after = tokio::task::spawn_blocking(move || {
            if position_store.contains_tenant(id) {
                position_store.replication_position(id).map(Some)
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(|_| Error::new("INTERNAL", "replica position worker failed"))??;
        let Some(after) = after else {
            self.start_initialize(source, id, false);
            return Ok(TenantSync::Idle);
        };
        // An asynchronous checkpoint can install a position newer than this
        // cycle's catalog. The authenticated log endpoint checks the live head.
        if after == position.seq && self.inner.progress.lock().confirmed.get(&id) == Some(&after) {
            return Ok(TenantSync::Idle);
        }
        let request = LogRequest {
            protocol: crate::replication::PROTOCOL,
            cluster,
            epoch: source.epoch,
            tenant: position.info.id,
            generation: position.info.generation,
            after,
            cursor: self
                .inner
                .progress
                .lock()
                .cursors
                .get(&position.info.id)
                .cloned(),
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining < Duration::from_millis(100) {
            return Err(Error::new(
                "DEADLINE_EXCEEDED",
                "insufficient cycle budget for log request",
            ));
        }
        let page = self
            .request_log(serde_json::to_value(&request)?, remaining)
            .await;
        match page {
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "LOG_GONE" | "REPLICA_SNAPSHOT_REQUIRED"
                ) =>
            {
                let mut progress = self.inner.progress.lock();
                progress.cursors.remove(&position.info.id);
                progress.interrupted_log_bodies.remove(&position.info.id);
                drop(progress);
                self.start_initialize(source, position.info.id, true);
                Ok(TenantSync::Idle)
            }
            Err(error) => Err(error),
            Ok(page) => {
                // A restart probes each durable position once to finish any
                // source-side handoff. A valid empty head page needs no engine
                // open or replay on an otherwise cold replica.
                if page.page.epoch == source.epoch
                    && page.page.frames.is_empty()
                    && page.raw.as_ref().is_none_or(|bytes| bytes.is_empty())
                    && page.page.through == after
                    && page.page.source_position == after
                {
                    self.inner.progress.lock().confirmed.insert(id, after);
                    return Ok(TenantSync::Idle);
                }
                let confirmed = (after == page.page.source_position).then_some(after);
                let pending = if page.page.through < page.page.source_position {
                    TenantSync::Pending(page.page.source_position)
                } else {
                    TenantSync::Idle
                };
                let next_cursor = page.page.cursor.clone();
                let store = store.clone();
                let apply_source = source.clone();
                let wire_memory = page._memory.size();
                let applied = tokio::task::spawn_blocking(move || {
                    let handle = store.tenant(request.tenant)?;
                    if handle.engine().write_buffer_full() {
                        handle.engine().flush_if_needed()?;
                    }
                    let apply = || {
                        handle.engine().apply_log_page(
                            &apply_source,
                            after,
                            &page.page,
                            page.raw.as_deref(),
                            wire_memory,
                        )
                    };
                    match apply() {
                        Err(error) if error.code == "REPLICA_MEMORY_PRESSURE" => {
                            // Decode failures occur before append. Release buffered
                            // records once, without retrying uncertain WAL writes.
                            flush_replica_buffer(handle.engine())?;
                            apply().map_err(|error| {
                                if error.code == "REPLICA_MEMORY_PRESSURE" {
                                    Error::new("REPLICA_SNAPSHOT_REQUIRED", error.to_string())
                                } else {
                                    error
                                }
                            })
                        }
                        result => result,
                    }
                })
                .await
                .map_err(|_| Error::new("INTERNAL", "replication apply worker failed"))?;
                if applied
                    .as_ref()
                    .is_err_and(|e| e.code == "REPLICA_SNAPSHOT_REQUIRED")
                {
                    self.inner.progress.lock().cursors.remove(&id);
                    self.start_initialize(source, id, true);
                    return Ok(TenantSync::Idle);
                }
                applied?;
                if let Some(cursor) = next_cursor {
                    self.inner
                        .progress
                        .lock()
                        .cursors
                        .insert(position.info.id, cursor);
                }
                if let Some(confirmed) = confirmed {
                    self.inner.progress.lock().confirmed.insert(id, confirmed);
                }
                Ok(pending)
            }
        }
    }
    fn start_initialize(&self, source: &Source, tenant: Uuid, reinitializing: bool) {
        let mut progress = self.inner.progress.lock();
        if progress.initializing.len() >= 2 || progress.initializing.contains_key(&tenant) {
            return;
        }
        let service = self.clone();
        let source = source.clone();
        progress.initialization_attempts += 1;
        progress.reinitializations += u64::from(reinitializing);
        progress.confirmed.remove(&tenant);
        progress.initializing.insert(
            tenant,
            tokio::spawn(async move { service.initialize(&source, tenant).await }),
        );
    }
    pub fn export_log(&self, request: &LogRequest) -> Result<LogPage> {
        Ok(self.export_log_page(request, false)?.page)
    }
    pub(crate) fn export_log_page(&self, request: &LogRequest, binary: bool) -> Result<LogExport> {
        let export = self.inner.store.export_log_page(request, binary)?;
        self.inner
            .exports
            .lock()
            .progress(request, export.page.source_position);
        Ok(export)
    }
    fn expire_exports(&self) -> Result<()> {
        let paths = {
            let mut exports = self.inner.exports.lock();
            exports.expire();
            std::mem::take(&mut exports.garbage)
        };
        let mut retry = Vec::new();
        let mut failure = None;
        for path in paths {
            let result = fs::remove_dir_all(&path)
                .or_else(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        Ok(())
                    } else {
                        Err(error)
                    }
                })
                .map_err(Error::from)
                .and_then(|_| crate::storage::sync_dir(path.parent().unwrap()));
            if let Err(error) = result {
                retry.push(path);
                failure.get_or_insert(error);
            }
        }
        self.inner.exports.lock().garbage.extend(retry);
        failure.map_or(Ok(()), Err)
    }
    pub fn snapshot(
        &self,
        cluster: Uuid,
        epoch: Uuid,
        tenant: Uuid,
    ) -> Result<super::snapshot::SnapshotInfo> {
        let start = self
            .inner
            .exports
            .lock()
            .begin(&self.inner.store, cluster, epoch, tenant)?;
        match start {
            super::snapshot::Start::Ready(info) => Ok(info),
            super::snapshot::Start::Build(preparation) => {
                let mut pending = PendingExport {
                    exports: &self.inner.exports,
                    preparation: Some(preparation),
                };
                let result = pending
                    .preparation
                    .as_ref()
                    .unwrap()
                    .create(&self.inner.store);
                self.inner
                    .exports
                    .lock()
                    .finish(pending.preparation.take().unwrap(), result)
            }
        }
    }
    pub(super) fn reader_drain_timed_out(&self, tenant: Uuid) {
        self.inner.progress.lock().initialization_errors.insert(
            tenant,
            "tenant reader drain deadline; verified checkpoint retained, retrying in 30 seconds"
                .into(),
        );
    }
    pub(super) fn waiting_for_readers(&self, tenant: Uuid, waiting: bool) {
        let mut progress = self.inner.progress.lock();
        if waiting {
            progress.waiting_for_readers.insert(tenant);
        } else {
            progress.waiting_for_readers.remove(&tenant);
        }
    }
    pub fn snapshot_chunk(&self, id: Uuid, file: usize, offset: u64) -> Result<String> {
        let chunk = self.inner.exports.lock().chunk(id, file, offset)?;
        chunk.read()
    }
    pub fn release_snapshot(&self, id: Uuid) -> Result<()> {
        self.inner.exports.lock().release(id)
    }
    pub(crate) fn render_metrics(&self) -> String {
        let progress = self.inner.progress.lock();
        [
            ("initialization_attempts", progress.initialization_attempts),
            ("initialization_failures", progress.initialization_failures),
            ("reinitializations", progress.reinitializations),
        ].into_iter().map(|(name, value)| format!(
            "# TYPE tkv_replication_{name}_total counter\ntkv_replication_{name}_total {value}\n"
        )).collect()
    }
    pub fn fence(&self, target: Uuid) -> Result<Fence> {
        self.inner.store.fence_primary(target)
    }

    pub async fn promote(&self, fence_id: Uuid) -> Result<NodeState> {
        self.promote_bounded(fence_id, Duration::from_secs(120))
            .await
    }
    async fn promote_bounded(&self, fence_id: Uuid, timeout: Duration) -> Result<NodeState> {
        let deadline = Instant::now() + timeout;
        let _cycle = tokio::time::timeout(timeout, self.inner.cycle.lock())
            .await
            .map_err(|_| {
                Error::new(
                    "DEADLINE_EXCEEDED",
                    "promotion waiting for replication cycle",
                )
            })?;
        let local = self.inner.store.replication_state();
        if local.role == Role::Primary && local.parent.as_ref().is_some_and(|p| p.id == fence_id) {
            return Ok(local);
        }
        loop {
            if Instant::now() >= deadline {
                return Err(Error::new("DEADLINE_EXCEEDED", "promotion deadline"));
            }
            let result = self.promote_once(fence_id, deadline).await;
            match result {
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        "REPLICA_LAG"
                            | "BUSY"
                            | "REPLICATION_UNAVAILABLE"
                            | "RESOURCE_EXHAUSTED"
                            | "DEADLINE_EXCEEDED"
                    ) && Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                result => return result,
            }
        }
    }
    async fn promote_once(&self, fence_id: Uuid, deadline: Instant) -> Result<NodeState> {
        let upstream: Received<Status> = self
            .request_bounded(
                "/v1/replication/status",
                None,
                deadline.saturating_duration_since(Instant::now()),
            )
            .await?;
        super::compatible(upstream.node.protocol)?;
        let fence = upstream
            .node
            .fence
            .as_ref()
            .filter(|f| f.id == fence_id && upstream.node.role == Role::Fenced)
            .ok_or_else(|| Error::new("CONFLICT", "source does not confirm this durable fence"))?;
        self.sync_until(true, deadline).await?;
        if Instant::now() >= deadline {
            return Err(Error::new("DEADLINE_EXCEEDED", "promotion deadline"));
        }
        let store = self.inner.store.clone();
        let fence = fence.clone();
        tokio::task::spawn_blocking(move || store.promote_verified(&fence))
            .await
            .map_err(|_| Error::new("INTERNAL", "promotion worker failed"))?
    }
    pub async fn follow(&self) -> Result<NodeState> {
        let _cycle = self.inner.cycle.lock().await;
        let upstream: Received<Status> = self.request("/v1/replication/status", None).await?;
        super::compatible(upstream.node.protocol)?;
        let store = self.inner.store.clone();
        let node = upstream.node.clone();
        let state = tokio::task::spawn_blocking(move || store.follow_verified(&node))
            .await
            .map_err(|_| Error::new("INTERNAL", "follow worker failed"))??;
        {
            let mut progress = self.inner.progress.lock();
            progress.cursors.clear();
            progress.confirmed.clear();
            progress.initialization_backoff.clear();
            progress.log_backoff.clear();
            progress.initialization_errors.clear();
            progress.retry_at.clear();
        }
        Ok(state)
    }
    /// An interrupted switchover leaves the old primary durably fenced. Retry this
    /// operation to finish; never reopen the old write path to recover availability.
    pub async fn switchover(&self) -> Result<NodeState> {
        let _cycle = self.inner.cycle.lock().await;
        let peer: Received<Status> = self.request("/v1/replication/status", None).await?;
        super::compatible(peer.node.protocol)?;
        let state = self.inner.store.replication_state();
        if state.role == Role::Replica
            && state
                .source
                .as_ref()
                .is_some_and(|s| s.node == peer.node.node && s.epoch == peer.node.epoch)
        {
            if peer.node.role == Role::Primary
                && peer.node.parent.as_ref().is_some_and(|fence| {
                    fence.source.node == state.node
                        && fence.target == peer.node.node
                        && fence.cluster == state.cluster
                        && state.cluster == peer.node.cluster
                })
            {
                return Ok(state);
            }
            return Err(Error::new(
                "CONFLICT",
                "switchover must start on the primary",
            ));
        }
        if state.cluster != peer.node.cluster {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "peer belongs to a different cluster",
            ));
        }
        if state.role == Role::Primary {
            let source = Source {
                node: state.node,
                epoch: state.epoch,
            };
            let follows_primary = peer
                .node
                .source
                .as_ref()
                .map_or_else(|| peer.tenants.is_empty(), |bound| *bound == source);
            if !peer.node.enabled
                || peer.node.role != Role::Replica
                || peer.node.node == state.node
                || !follows_primary
                || !peer.unavailable.is_empty()
            {
                return Err(Error::new(
                    "REPLICATION_CONFLICT",
                    "successor must be a healthy replica following this primary, or an empty unbound replica",
                ));
            }
        }
        let store = self.inner.store.clone();
        let target = peer.node.node;
        let fence = tokio::task::spawn_blocking(move || store.fence_primary(target))
            .await
            .map_err(|_| Error::new("INTERNAL", "fence worker failed"))??;
        let _: Received<NodeState> = self
            .request("/v1/replication/promote", Some(json!({"fence":fence.id})))
            .await?;
        let peer: Received<Status> = self.request("/v1/replication/status", None).await?;
        super::compatible(peer.node.protocol)?;
        let store = self.inner.store.clone();
        let node = peer.node.clone();
        let state = tokio::task::spawn_blocking(move || store.follow_verified(&node))
            .await
            .map_err(|_| Error::new("INTERNAL", "follow worker failed"))??;
        {
            let mut progress = self.inner.progress.lock();
            progress.cursors.clear();
            progress.confirmed.clear();
            progress.initialization_backoff.clear();
            progress.log_backoff.clear();
            progress.initialization_errors.clear();
            progress.retry_at.clear();
        }
        Ok(state)
    }
    pub async fn force_promote(
        &self,
        source: Source,
        old_primary_fenced: bool,
        accept_data_loss: bool,
    ) -> Result<NodeState> {
        let _cycle = self.inner.cycle.lock().await;
        let store = self.inner.store.clone();
        tokio::task::spawn_blocking(move || {
            store.force_promote(&source, old_primary_fenced, accept_data_loss)
        })
        .await
        .map_err(|_| Error::new("INTERNAL", "force promotion worker failed"))?
    }
}

fn read_token(path: &std::path::Path) -> Result<String> {
    use std::{
        io::Read,
        os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    };
    // Open without following a link and validate the opened inode, not a prior stat.
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::PermissionDenied
                || error.raw_os_error() == Some(libc::ELOOP)
            {
                Error::new(
                    "REPLICATION_CREDENTIAL_INVALID",
                    "replication token permissions or symlink are invalid",
                )
            } else {
                error.into()
            }
        })?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(Error::new(
            "REPLICATION_CREDENTIAL_INVALID",
            "replication token must be a private regular file (0600), owned by this user with one link",
        ));
    }
    let mut token = String::new();
    file.take(514).read_to_string(&mut token)?;
    if token.len() > 513 || !(16..=512).contains(&token.trim().len()) {
        return Err(Error::invalid("replication token length"));
    }
    Ok(token.trim().to_owned())
}
