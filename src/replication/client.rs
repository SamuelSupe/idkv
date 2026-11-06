use super::{
    Config, Exports, Fence, LogPage, LogRequest, MAX_WIRE_BYTES, NodeState, Role, Source,
    TenantPosition,
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

#[derive(Clone, Serialize, Deserialize)]
pub struct Status {
    pub node: NodeState,
    pub tenants: Vec<TenantPosition>,
    pub unavailable: Vec<UnavailableTenant>,
    pub last_error: Option<String>,
    pub completed_cycles: u64,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct UnavailableTenant {
    pub info: crate::TenantInfo,
    pub error: String,
}
enum TenantSync {
    Idle,
    Pending(u64),
    Refresh,
}
struct Progress {
    error: Option<String>,
    cycles: u64,
    cursors: HashMap<Uuid, super::LogCursor>,
}
struct Inner {
    store: Store,
    config: Config,
    token: String,
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
        let token = fs::read_to_string(&config.token_file)?.trim().to_string();
        if !(16..=512).contains(&token.len()) {
            return Err(Error::invalid("replication token length"));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(30))
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
                token,
                client,
                _lock: lock,
                cycle: AsyncMutex::new(()),
                exports: Mutex::new(exports),
                progress: Mutex::new(Progress {
                    error: None,
                    cycles: 0,
                    cursors: HashMap::new(),
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
                    info,
                    error: "UNAVAILABLE: tenant lifecycle recovery is pending".into(),
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
        })
    }
    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            loop {
                if service.inner.store.replication_state().role == Role::Replica {
                    let result = {
                        let _cycle = service.inner.cycle.lock().await;
                        service.sync_inner().await
                    };
                    let pending = matches!(result, Ok(true));
                    service.inner.progress.lock().error = result.err().map(|e| e.to_string());
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
    pub(super) async fn request<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Option<Value>,
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
        let mut response = request
            .bearer_auth(&self.inner.token)
            .send()
            .await
            .map_err(|_| {
                Error::new(
                    "REPLICATION_UNAVAILABLE",
                    "peer request failed or timed out",
                )
            })?;
        let success = response.status().is_success();
        if response
            .content_length()
            .is_some_and(|n| n > MAX_WIRE_BYTES as u64)
        {
            return Err(Error::limit("peer response too large"));
        }
        let mut bytes = Vec::new();
        let mut charges = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Error::new("REPLICATION_UNAVAILABLE", "peer response interrupted"))?
        {
            if bytes.len() + chunk.len() > MAX_WIRE_BYTES {
                return Err(Error::limit("peer response too large"));
            }
            charges.push(
                self.inner
                    .store
                    .reserve_http(chunk.len().saturating_mul(4))?,
            );
            bytes.extend_from_slice(&chunk);
        }
        let memory = self
            .inner
            .store
            .reserve_http(crate::json::allocation_bound(&bytes))?;
        if !success {
            let value: Value = serde_json::from_slice(&bytes)?;
            return Err(Error::new(
                value["error"]["code"]
                    .as_str()
                    .unwrap_or("REPLICATION_UNAVAILABLE"),
                value["error"]["message"]
                    .as_str()
                    .unwrap_or("peer rejected request"),
            ));
        }
        let value = serde_json::from_slice(&bytes)?;
        Ok(Received {
            value,
            _memory: memory,
        })
    }
    pub async fn sync_once(&self) -> Result<()> {
        let _cycle = self.inner.cycle.lock().await;
        self.sync_inner().await.map(|_| ())
    }
    async fn sync_inner(&self) -> Result<bool> {
        if self.inner.store.replication_state().role != Role::Replica {
            return Err(Error::new("CONFLICT", "node is not a replica"));
        }
        let upstream: Received<Status> = self.request("/v1/replication/status", None).await?;
        super::compatible(upstream.node.protocol)?;
        let store = self.inner.store.clone();
        let source = store.bind_source(&upstream.node)?;
        let mut catalog = upstream.tenants.clone();
        catalog.extend(upstream.unavailable.iter().map(|t| TenantPosition {
            info: t.info.clone(),
            seq: 0,
        }));
        catalog.sort_by_key(|p| p.info.id);
        let blocked = store.replica_catalog(&source, &catalog)?;
        let active: HashSet<_> = catalog.iter().map(|p| p.info.id).collect();
        self.inner
            .progress
            .lock()
            .cursors
            .retain(|id, _| active.contains(id));
        let mut failure = blocked.values().next().cloned().or_else(|| {
            upstream.unavailable.first().map(|t| {
                Error::new(
                    "UNAVAILABLE",
                    format!("source tenant {} unavailable: {}", t.info.id, t.error),
                )
            })
        });
        let started = Instant::now();
        let mut positions = upstream.tenants.clone();
        for round in 0..8 {
            let mut next = Vec::new();
            let mut refresh = false;
            for mut position in positions {
                if blocked.contains_key(&position.info.id) {
                    continue;
                }
                match self
                    .sync_tenant(&source, upstream.node.cluster, &position)
                    .await
                {
                    Ok(TenantSync::Idle) => {}
                    Ok(TenantSync::Pending(head)) => {
                        // The source may advance beyond the catalog snapshot.
                        // Compare the next page with the latest authenticated head.
                        position.seq = head;
                        next.push(position);
                    }
                    Ok(TenantSync::Refresh) => refresh = true,
                    Err(error) => {
                        failure.get_or_insert(error);
                    }
                }
            }
            if let Some(error) = failure {
                return Err(error);
            }
            if next.is_empty()
                || refresh
                || round == 7
                || started.elapsed() >= Duration::from_millis(10)
            {
                self.inner.progress.lock().cycles += 1;
                return Ok(refresh || !next.is_empty());
            }
            // Every pending tenant receives one page per round. Refresh lifecycle
            // and role information after at most eight rounds or 10 ms of work;
            // each log request still checks epoch and storage generation itself.
            positions = next;
            tokio::task::yield_now().await;
        }
        unreachable!("bounded replication rounds return at the limit")
    }
    async fn sync_tenant(
        &self,
        source: &Source,
        cluster: Uuid,
        position: &TenantPosition,
    ) -> Result<TenantSync> {
        let store = self.inner.store.clone();
        if !store.contains_tenant(position.info.id) {
            self.initialize(source, position.info.id).await?;
            return Ok(TenantSync::Refresh);
        }
        let after = store.replication_position(position.info.id)?;
        if after > position.seq {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "replica contains transactions absent from source",
            ));
        }
        if after == position.seq {
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
        let page: Result<Received<LogPage>> = self
            .request("/v1/replication/log", Some(serde_json::to_value(&request)?))
            .await;
        match page {
            Err(error) if error.code == "LOG_GONE" => {
                self.inner.progress.lock().cursors.remove(&position.info.id);
                self.initialize(source, position.info.id).await?;
                Ok(TenantSync::Refresh)
            }
            Err(error) => Err(error),
            Ok(page) => {
                let pending = if page.through < page.source_position {
                    TenantSync::Pending(page.source_position)
                } else {
                    TenantSync::Idle
                };
                let next_cursor = page.cursor.clone();
                let store = store.clone();
                let source = source.clone();
                tokio::task::spawn_blocking(move || {
                    let handle = store.tenant(request.tenant)?;
                    if handle.engine().write_buffer_full() {
                        handle.engine().flush()?;
                    }
                    handle.engine().apply_replica_page(&source, after, &page)
                })
                .await
                .map_err(|_| Error::new("INTERNAL", "replication apply worker failed"))??;
                if let Some(cursor) = next_cursor {
                    self.inner
                        .progress
                        .lock()
                        .cursors
                        .insert(position.info.id, cursor);
                }
                Ok(pending)
            }
        }
    }
    pub fn export_log(&self, request: &LogRequest) -> Result<LogPage> {
        self.inner.store.export_logs(request)
    }
    pub fn snapshot(
        &self,
        cluster: Uuid,
        epoch: Uuid,
        tenant: Uuid,
    ) -> Result<super::snapshot::SnapshotInfo> {
        self.inner
            .exports
            .lock()
            .create(&self.inner.store, cluster, epoch, tenant)
    }
    pub fn snapshot_chunk(&self, id: Uuid, file: usize, offset: u64) -> Result<String> {
        self.inner.exports.lock().chunk(id, file, offset)
    }
    pub fn release_snapshot(&self, id: Uuid) -> Result<()> {
        self.inner.exports.lock().release(id)
    }
    pub fn fence(&self, target: Uuid) -> Result<Fence> {
        self.inner.store.fence_primary(target)
    }

    pub async fn promote(&self, fence_id: Uuid) -> Result<NodeState> {
        let _cycle = self.inner.cycle.lock().await;
        let local = self.inner.store.replication_state();
        if local.role == Role::Primary && local.parent.as_ref().is_some_and(|p| p.id == fence_id) {
            return Ok(local);
        }
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let upstream: Received<Status> = self.request("/v1/replication/status", None).await?;
            super::compatible(upstream.node.protocol)?;
            let fence = upstream
                .node
                .fence
                .as_ref()
                .filter(|f| f.id == fence_id && upstream.node.role == Role::Fenced)
                .ok_or_else(|| {
                    Error::new("CONFLICT", "source does not confirm this durable fence")
                })?;
            self.sync_inner().await?;
            match self.inner.store.promote_verified(fence) {
                Err(error) if error.code == "REPLICA_LAG" && Instant::now() < deadline => {}
                result => return result,
            }
        }
    }
    pub async fn follow(&self) -> Result<NodeState> {
        let _cycle = self.inner.cycle.lock().await;
        let upstream: Received<Status> = self.request("/v1/replication/status", None).await?;
        super::compatible(upstream.node.protocol)?;
        let state = self.inner.store.follow_verified(&upstream.node)?;
        self.inner.progress.lock().cursors.clear();
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
            return Ok(state);
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
        let fence = self.inner.store.fence_primary(peer.node.node)?;
        let _: Received<NodeState> = self
            .request("/v1/replication/promote", Some(json!({"fence":fence.id})))
            .await?;
        let peer: Received<Status> = self.request("/v1/replication/status", None).await?;
        super::compatible(peer.node.protocol)?;
        let state = self.inner.store.follow_verified(&peer.node)?;
        self.inner.progress.lock().cursors.clear();
        Ok(state)
    }
    pub async fn force_promote(
        &self,
        source: Source,
        old_primary_fenced: bool,
        accept_data_loss: bool,
    ) -> Result<NodeState> {
        let _cycle = self.inner.cycle.lock().await;
        self.inner
            .store
            .force_promote(&source, old_primary_fenced, accept_data_loss)
    }
}
