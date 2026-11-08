use super::{TenantCore, TenantHandle};
use crate::{
    Error, Result,
    model::*,
    resources::{Budget, Counter, IoScheduler, RequestGate},
    storage::{Engine, atomic_json, read_json, reject_symlink, sync_dir},
};
use fs2::FileExt;
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

mod admission;
mod lifecycle;
mod quota;
mod registry;
mod repair;
mod replication;
use lifecycle::{
    clean_lifecycle_debris, collect_lifecycle_debris, recover_lifecycle, remove_lifecycle_debris,
};
use registry::{Lifecycle, Registration, Registry};

struct Slot {
    last_seq: Option<u64>,
    restoring: bool,
    installing: Option<Uuid>,
    memory: Arc<Counter>,
    core: Option<Arc<TenantCore>>,
    failure: Option<Error>,
    failure_until: Option<Instant>,
    queued: bool,
    local_backups: Arc<Mutex<()>>,
}
struct Inner {
    root: PathBuf,
    options: Options,
    _lock: Arc<File>,
    registry: RwLock<Registry>,
    lifecycle_errors: Mutex<HashMap<Uuid, String>>,
    slots: Mutex<HashMap<Uuid, Arc<Mutex<Slot>>>>,
    memory: Arc<Counter>,
    io: Arc<IoScheduler>,
    node: Arc<crate::replication::Node>,
}
struct Owner {
    inner: Arc<Inner>,
    stop: Arc<AtomicBool>,
    workers: Mutex<Vec<thread::JoinHandle<()>>>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        for worker in self.workers.lock().drain(..) {
            let _ = worker.join();
        }
    }
}
#[derive(Clone)]
pub struct Store {
    owner: Arc<Owner>,
}

impl Store {
    /// Global accounted memory pressure and protected flush capacity, in bytes
    /// except for `active` (0 or 1). Retained snapshots may prevent reclamation.
    pub fn memory_pressure(&self) -> BTreeMap<&'static str, u64> {
        let memory = &self.owner.inner.memory;
        BTreeMap::from([
            ("active", u64::from(memory.under_pressure())),
            ("cache_active", u64::from(memory.cache_pressure())),
            ("foreground_limit_bytes", memory.foreground_limit() as u64),
            ("headroom_bytes", memory.headroom() as u64),
            ("headroom_in_use_bytes", memory.headroom_in_use() as u64),
        ])
    }
    /// Registry and open-engine gauges. Engine cumulative values can decrease
    /// when an idle tenant closes, so these are deliberately not counters.
    pub fn open_engine_metrics(&self) -> BTreeMap<String, u64> {
        let slots: Vec<_> = self.owner.inner.slots.lock().values().cloned().collect();
        let mut out = BTreeMap::new();
        for (name, value) in self.memory_pressure() {
            out.insert(format!("memory_pressure_{name}"), value);
        }
        {
            let registry = self.owner.inner.registry.read();
            out.insert(
                "registry_deleted_tenants".into(),
                registry
                    .tenants
                    .values()
                    .filter(|row| matches!(row.lifecycle, Lifecycle::Deleted))
                    .count() as u64,
            );
        }
        out.insert(
            "registry_metadata_capacity_bytes".into(),
            registry::MAX_BYTES as u64,
        );
        if let Ok(metadata) = fs::metadata(self.root().join("control/tenants.json")) {
            out.insert("registry_metadata_bytes".into(), metadata.len());
        }
        for slot in slots {
            let slot = slot.lock();
            if slot.failure.is_some() {
                *out.entry("failed_tenants".into()).or_default() += 1;
            }
            if let Some(core) = &slot.core {
                *out.entry("open_tenants".into()).or_default() += 1;
                *out.entry("disk_bytes".into()).or_default() += core.engine.disk_bytes();
                *out.entry("snapshots".into()).or_default() +=
                    core.engine.active_snapshots() as u64;
                *out.entry("l0_files".into()).or_default() +=
                    core.engine.table_counts().get(&0).copied().unwrap_or(0) as u64;
                for (name, value) in core.engine.metrics().as_object().unwrap() {
                    if let Some(value) = value.as_u64() {
                        *out.entry(name.clone()).or_default() += value;
                    }
                }
            }
        }
        out
    }
    pub fn writable(&self) -> bool {
        self.owner.inner.node.writable()
    }
    pub fn open(root: impl AsRef<Path>, options: Options) -> Result<Self> {
        options.validate()?;
        fs::create_dir_all(root.as_ref())?;
        reject_symlink(root.as_ref())?;
        let root = fs::canonicalize(root)?;
        for dir in ["control", "tenants", "trash", "staging", "backups"] {
            fs::create_dir_all(root.join(dir))?;
            reject_symlink(&root.join(dir))?;
        }
        let lock_path = root.join("control/LOCK");
        if lock_path.exists() {
            reject_symlink(&lock_path)?;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        lock.try_lock_exclusive()
            .map_err(|_| Error::new("BUSY", "store manager is already open"))?;
        let registry_path = root.join("control/tenants.json");
        let mut registry = if registry_path.exists() {
            read_json::<Registry>(&registry_path)?
        } else {
            let registry = Registry {
                version: registry::VERSION,
                replication_enabled: false,
                tenants: BTreeMap::new(),
            };
            atomic_json(&registry_path, &registry)?;
            registry
        };
        if !matches!(registry.version, 1..=registry::VERSION) {
            return Err(Error::corrupt("registry version"));
        }
        let previous = serde_json::to_vec(&registry)?;
        registry.replication_enabled |= registry.version == 2;
        // A healthy v4 root can serve reads even on a full disk. Upgrade its
        // representation only when a registry mutation already needs a write.
        registry.version = registry.version.max(4);
        let lifecycle_errors = recover_lifecycle(&root, &mut registry, None)?;
        if serde_json::to_vec(&registry)? != previous {
            registry.prepare_publish()?;
            atomic_json(&registry_path, &registry)?;
        }
        clean_lifecycle_debris(&root, &registry, true)?;
        if registry.replication_enabled && !root.join("control/replication.json").exists() {
            return Err(Error::corrupt(
                "replicated Store lost its durable node identity",
            ));
        }
        let node = crate::replication::Node::open(&root)?;
        if registry.replication_enabled && !node.state.read().enabled {
            // Initialization can crash between the registry compatibility fence
            // and role publication. Only configure_replication may complete it.
            node.state.write().role = crate::replication::Role::Fenced;
        }
        let inner = Arc::new(Inner {
            node,
            root,
            options: options.clone(),
            _lock: Arc::new(lock),
            registry: RwLock::new(registry),
            lifecycle_errors: Mutex::new(lifecycle_errors),
            slots: Mutex::new(HashMap::new()),
            memory: Counter::with_headroom(
                options.global_memory_bytes,
                (options.max_document_bytes + options.block_bytes) * 4 + (2 << 20),
            ),
            io: IoScheduler::new(options.io_workers),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let (sender, receiver) =
            std::sync::mpsc::sync_channel::<(Arc<Mutex<Slot>>, Option<usize>)>(128);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::new();
        for _ in 0..options.io_workers.min(4) {
            let weak = Arc::downgrade(&inner);
            let receiver = receiver.clone();
            let stop = stop.clone();
            workers.push(thread::spawn(move || {
                loop {
                    let job = receiver.lock().recv();
                    let Ok((slot, pressure_minimum)) = job else {
                        break;
                    };
                    if !stop.load(Ordering::Acquire)
                        && let Some(inner) = weak.upgrade()
                    {
                        maintain_tenant(&inner, &slot, pressure_minimum);
                    }
                    slot.lock().queued = false;
                }
            }));
        }
        let weak = Arc::downgrade(&inner);
        let worker_stop = stop.clone();
        let coordinator = thread::spawn(move || {
            let mut next = 0;
            let mut cache_next = 0;
            while !worker_stop.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(100));
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                if !enqueue_maintenance(&inner, &sender, &mut next, &mut cache_next) {
                    break;
                }
            }
        });
        workers.insert(0, coordinator);
        let weak = Arc::downgrade(&inner);
        let build_stop = stop.clone();
        // Recovery builds have a separate, single bounded worker. A large index
        // must not consume all flush/compaction workers for other tenants.
        workers.push(thread::spawn(move || {
            while !build_stop.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(100));
                let Some(inner) = weak.upgrade() else { break };
                if !inner.node.writable() {
                    continue;
                }
                let slots: Vec<_> = inner.slots.lock().values().cloned().collect();
                for slot in slots {
                    if build_stop.load(Ordering::Acquire) {
                        break;
                    }
                    let core = slot.try_lock().and_then(|slot| {
                        slot.core
                            .as_ref()
                            .filter(|core| {
                                slot.installing.is_none()
                                    && !core.engine.recovery_required()
                                    && !core.engine.recovery_read_only()
                            })
                            .cloned()
                    });
                    if let Some(core) = core {
                        let handle = TenantHandle { core };
                        if let Err(error) = handle.restart_interrupted_build(&build_stop) {
                            *handle.engine().maintenance_error.lock() = Some(error.to_string());
                        }
                    }
                }
            }
        }));
        Ok(Self {
            owner: Arc::new(Owner {
                inner,
                stop,
                workers: Mutex::new(workers),
            }),
        })
    }

    pub fn root(&self) -> &Path {
        &self.owner.inner.root
    }
    pub fn memory_bytes(&self) -> usize {
        self.owner.inner.memory.used()
    }
    pub(crate) fn foreground_memory_limit(&self) -> usize {
        self.owner.inner.memory.foreground_limit()
    }
    pub(crate) fn reserve_http(&self, bytes: usize) -> Result<crate::resources::Reservation> {
        Budget {
            global: self.owner.inner.memory.clone(),
            tenant: Counter::new(bytes),
        }
        .reserve(bytes)
    }
    pub(crate) fn contains_tenant(&self, id: Uuid) -> bool {
        self.owner.inner.registry.read().tenants.contains_key(&id)
    }
    pub(crate) fn tenant_history(&self, id: Uuid) -> Result<String> {
        let info = self
            .owner
            .inner
            .registry
            .read()
            .tenants
            .get(&id)
            .map(|r| r.info.clone())
            .ok_or_else(|| Error::missing("tenant"))?;
        let state = self.replication_state();
        Ok(super::contracts::history_token(
            &info,
            state.enabled.then_some(state.epoch),
        ))
    }
    pub(crate) fn reserve_http_input(
        &self,
        tenant: Option<Uuid>,
        bytes: usize,
    ) -> Result<crate::resources::Reservation> {
        self.http_input_budget(tenant)?.reserve(bytes)
    }
    pub(crate) fn http_input_budget(&self, tenant: Option<Uuid>) -> Result<Budget> {
        match tenant {
            Some(id) => Ok(self.backup_context(id)?.1),
            None => Ok(Budget {
                global: self.owner.inner.memory.clone(),
                tenant: Counter::new(self.options().global_memory_bytes),
            }),
        }
    }
    // BUSY is global catalog contention, suitable for an async retry. None
    // requires tenant-local preparation in the bounded blocking admission pool.
    pub(crate) fn try_http_input_budget(&self, tenant: Option<Uuid>) -> Result<Option<Budget>> {
        let Some(id) = tenant else {
            return self.http_input_budget(None).map(Some);
        };
        let inner = &self.owner.inner;
        let Some(registry) = inner.registry.try_read() else {
            return Err(Error::new("BUSY", "HTTP input registry contention"));
        };
        if !registry.tenants.contains_key(&id) {
            return Err(Error::missing("tenant"));
        }
        let Some(slots) = inner.slots.try_lock() else {
            return Err(Error::new("BUSY", "HTTP input slots contention"));
        };
        let Some(slot) = slots.get(&id) else {
            return Ok(None);
        };
        let Some(slot) = slot.try_lock() else {
            return Ok(None);
        };
        Ok(Some(Budget {
            global: inner.memory.clone(),
            tenant: slot.memory.clone(),
        }))
    }
    pub fn options(&self) -> &Options {
        &self.owner.inner.options
    }
    pub(crate) fn backup_context(
        &self,
        id: Uuid,
    ) -> Result<(TenantInfo, Budget, Arc<IoScheduler>)> {
        let inner = &self.owner.inner;
        let info = inner
            .registry
            .read()
            .tenants
            .get(&id)
            .map(|r| r.info.clone())
            .ok_or_else(|| Error::missing("tenant"))?;
        let slot = self.slot(id);
        let memory = slot.lock().memory.clone();
        Ok((
            info,
            Budget {
                global: inner.memory.clone(),
                tenant: memory,
            },
            inner.io.clone(),
        ))
    }
    pub(crate) fn import_context(&self, tenant: Option<&TenantInfo>) -> (Budget, Arc<IoScheduler>) {
        // Replica validation shares serving reservations. Disaster-recovery
        // imports have no registered tenant and use this node's default quota.
        let memory = match tenant {
            Some(info) if self.contains_tenant(info.id) => self.slot(info.id).lock().memory.clone(),
            Some(info) => Counter::new(info.memory_bytes),
            None => Counter::new(self.options().tenant_memory_bytes),
        };
        (
            Budget {
                global: self.owner.inner.memory.clone(),
                tenant: memory,
            },
            self.owner.inner.io.clone(),
        )
    }
    fn slot(&self, id: Uuid) -> Arc<Mutex<Slot>> {
        if !self.owner.inner.registry.read().tenants.contains_key(&id) {
            return Arc::new(Mutex::new(Slot {
                last_seq: None,
                core: None,
                failure: None,
                queued: false,
                local_backups: Arc::new(Mutex::new(())),
                restoring: false,
                installing: None,
                failure_until: None,
                memory: Counter::new(self.options().tenant_memory_bytes),
            }));
        }
        self.owner
            .inner
            .slots
            .lock()
            .entry(id)
            .or_insert_with(|| {
                Arc::new(Mutex::new(Slot {
                    last_seq: None,
                    core: None,
                    failure: None,
                    queued: false,
                    local_backups: Arc::new(Mutex::new(())),
                    restoring: false,
                    installing: None,
                    failure_until: None,
                    memory: Counter::new(
                        self.owner.inner.registry.read().tenants[&id]
                            .info
                            .memory_bytes,
                    ),
                }))
            })
            .clone()
    }
    pub fn create_tenant(&self, tenant_name: &str) -> Result<TenantInfo> {
        let _permission = self.owner.inner.node.write()?;
        super::catalog::name(tenant_name)?;
        let inner = &self.owner.inner;
        let mut registry = inner.registry.write();
        if registry
            .tenants
            .values()
            .any(|r| r.info.name == tenant_name && !matches!(r.lifecycle, Lifecycle::Deleted))
        {
            return Err(Error::new("ALREADY_EXISTS", "tenant name"));
        }
        if registry
            .tenants
            .values()
            .filter(|r| !matches!(r.lifecycle, Lifecycle::Deleted))
            .count()
            >= 16_384
        {
            return Err(Error::limit("tenant registry limit"));
        }
        let info = TenantInfo {
            id: Uuid::new_v4(),
            name: tenant_name.into(),
            generation: Uuid::new_v4(),
            history: Uuid::new_v4(),
            memory_bytes: inner.options.tenant_memory_bytes,
            disk_bytes: inner.options.tenant_disk_bytes,
        };
        self.create_tenant_inner(&mut registry, info.clone())?;
        Ok(info)
    }
    pub fn tenants(&self) -> Vec<TenantInfo> {
        self.owner
            .inner
            .registry
            .read()
            .tenants
            .values()
            .filter(|r| matches!(r.lifecycle, Lifecycle::Active))
            .map(|r| r.info.clone())
            .collect()
    }
    pub fn tenant(&self, id: Uuid) -> Result<TenantHandle> {
        self.tenant_with_open_marker(id).map(|(tenant, _)| tenant)
    }
    fn tenant_with_open_marker(&self, id: Uuid) -> Result<(TenantHandle, Option<Instant>)> {
        let slot = self.slot(id);
        let mut slot = slot.lock();
        let inner = &self.owner.inner;
        let registration = inner
            .registry
            .read()
            .tenants
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::missing("tenant"))?;
        if !matches!(registration.lifecycle, Lifecycle::Active) {
            return Err(Error::new("UNAVAILABLE", "tenant is not active"));
        }
        if slot.installing.is_some() {
            return Err(Error::new("BUSY", "tenant offline preparation is active"));
        }
        slot.check_failure()?;
        if slot
            .core
            .as_ref()
            .is_some_and(|core| core.engine.recovery_required())
        {
            if let Some(core) = &slot.core {
                core.gate.close();
                core.cursors.lock().clear();
                core.engine.invalidate_snapshots();
            }
            if self.close_slot(&mut slot).is_err() {
                return Err(Error::new(
                    "COMMIT_UNKNOWN",
                    "tenant is draining for WAL recovery",
                ));
            }
        }
        let opened = slot.core.is_none();
        if opened {
            let info = registration.info;
            let budget = Budget {
                global: inner.memory.clone(),
                tenant: slot.memory.clone(),
            };
            let engine = Engine::open(
                inner.root.join("tenants").join(id.to_string()),
                info,
                inner.options.clone(),
                budget,
                inner.io.clone(),
            );
            match engine {
                Ok(engine) => {
                    engine.attach_manager(inner._lock.clone(), inner.node.clone());
                    slot.core = Some(Arc::new(TenantCore {
                        identity_clock: Default::default(),
                        collection_id: Default::default(),
                        query_catalog: Default::default(),
                        dsl: crate::dsl::Cache::default(),
                        engine,
                        gate: RequestGate::new(inner.options.max_tenant_requests),
                        exported_cold: AtomicBool::new(false),
                        last_used: Mutex::new(Instant::now()),
                        cursors: Mutex::new(HashMap::new()),
                        plans: Mutex::new(HashMap::new()),
                        builds: Mutex::new(HashSet::new()),
                        recovery_catalog: std::sync::atomic::AtomicU64::new(u64::MAX),
                    }))
                }
                Err(error) => {
                    if matches!(error.code.as_str(), "CORRUPTION" | "IO_ERROR") {
                        slot.record_failure(error.clone());
                    }
                    return Err(error);
                }
            }
        }
        let core = slot.core.as_ref().unwrap().clone();
        let touched = Instant::now();
        *core.last_used.lock() = touched;
        Ok((TenantHandle { core }, opened.then_some(touched)))
    }
    fn close_unmodified_tenant(&self, id: Uuid, runtime: Uuid, touched: Instant) -> Result<()> {
        let slot = self.slot(id);
        let mut slot = slot.lock();
        if slot.core.as_ref().is_some_and(|core| {
            !core.engine.recovery_read_only()
                && core.engine.runtime_id == runtime
                && *core.last_used.lock() == touched
        }) {
            self.close_slot(&mut slot)?;
        }
        Ok(())
    }
    fn close_slot(&self, slot: &mut Slot) -> Result<()> {
        if let Some(error) = &slot.failure
            && error.code == "UNAVAILABLE"
        {
            return Err(error.clone());
        }
        if let Some(core) = &slot.core {
            core.cursors.lock().retain(|_, c| c.valid());
            if Arc::strong_count(core) != 1
                || Arc::strong_count(&core.engine) != 1
                || core.engine.replication_pinned()
                || core.engine.active_snapshots() != 0
            {
                return Err(Error::new(
                    "BUSY",
                    "tenant has active requests, handles or snapshots",
                ));
            }
        }
        if let Some(core) = &slot.core {
            slot.last_seq = core.engine.replication_position().ok();
        }
        slot.core = None;
        slot.failure = None;
        slot.failure_until = None;
        Ok(())
    }
    fn offline_lock(&self, id: Uuid) -> Result<Option<File>> {
        let inner = &self.owner.inner;
        let registration = inner
            .registry
            .read()
            .tenants
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::missing("tenant"))?;
        let path = inner.root.join("tenants").join(id.to_string()).join("LOCK");
        if !path.parent().unwrap().try_exists()? {
            return if matches!(registration.lifecycle, Lifecycle::Active) {
                Err(Error::corrupt("active tenant directory is missing"))
            } else {
                Ok(None)
            };
        }
        if !path.exists() && matches!(registration.lifecycle, Lifecycle::Deleted) {
            return Ok(None);
        }
        if path.exists() {
            reject_symlink(&path)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.try_lock_exclusive()
            .map_err(|_| Error::new("BUSY", "tenant directory is owned by a live engine"))?;
        Ok(Some(file))
    }
    pub fn close_tenant(&self, id: Uuid) -> Result<()> {
        let _drain = self.drain_for_management(id, false)?;
        let slot = self.slot(id);
        let mut slot = slot.lock();
        self.close_slot(&mut slot)?;
        let _offline = self.offline_lock(id)?;
        Ok(())
    }
    pub fn delete_tenant(&self, id: Uuid) -> Result<()> {
        self.delete_tenant_inner(id, None)
    }
    fn delete_tenant_inner(
        &self,
        id: Uuid,
        source: Option<&crate::replication::Source>,
    ) -> Result<()> {
        let permission = match source {
            Some(source) => self.owner.inner.node.replica_write(source)?,
            None => self.owner.inner.node.write()?,
        };
        drop(permission);
        let backup_lock = self.slot(id).lock().local_backups.clone();
        let _backup = backup_lock.lock();
        let _drain = self.drain_for_management(id, false)?;
        let permission = match source {
            Some(source) => self.owner.inner.node.replica_write(source)?,
            None => self.owner.inner.node.write()?,
        };
        let slot = self.slot(id);
        let mut slot = slot.lock();
        self.close_slot(&mut slot)?;
        let _offline = self.offline_lock(id)?;
        let inner = &self.owner.inner;
        let mut registry = inner.registry.write();
        let mut next = registry.clone();
        let registration = next
            .tenants
            .get_mut(&id)
            .ok_or_else(|| Error::missing("tenant"))?;
        if matches!(registration.lifecycle, Lifecycle::Deleted) {
            return Ok(());
        }
        if let Lifecycle::Restoring { old, .. } = registration.lifecycle {
            let previous = inner.root.join("trash").join(old.to_string());
            if previous.try_exists()? {
                return Err(Error::new(
                    "RECOVERY_REQUIRED",
                    format!(
                        "restore still retains a previous database at {}; archive or recover it before deleting this tenant",
                        previous.display()
                    ),
                ));
            }
        }
        registration.lifecycle = Lifecycle::Deleting;
        next.prepare_publish()?;
        // A failed publication may already be durable. Never reopen the old
        // generation for writes before restart has resolved the journal.
        *registry = next;
        let result = (|| {
            atomic_json(&inner.root.join("control/tenants.json"), &*registry)?;
            crate::storage::errorpoint("tenant_delete_journal_published")?;
            recover_lifecycle(&inner.root, &mut registry, Some(id))?;
            atomic_json(&inner.root.join("control/tenants.json"), &*registry)?;
            collect_lifecycle_debris(&inner.root, &registry, false)
        })();
        if result.is_err() {
            slot.record_failure(Error::new(
                "UNAVAILABLE",
                "tenant deletion outcome is uncertain; restart Store to recover",
            ));
        } else {
            inner.lifecycle_errors.lock().remove(&id);
        }
        drop(registry);
        drop(slot);
        drop(permission);
        let cleanup = remove_lifecycle_debris(&inner.root, result?);
        if cleanup.is_err() {
            self.slot(id).lock().record_failure(Error::new(
                "UNAVAILABLE",
                "tenant deletion cleanup failed; reopen Store to recover",
            ));
        }
        cleanup
    }
    pub fn status(&self, id: Uuid) -> Result<serde_json::Value> {
        let inner = &self.owner.inner;
        let registration = inner
            .registry
            .read()
            .tenants
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::missing("tenant"))?;
        let slot = self.slot(id);
        let slot = slot.lock();
        let stats = slot.core.as_ref().map(|core| serde_json::json!({
            "commit_seq":core.engine.seq(), "io":core.engine.metrics(), "memory_bytes":core.engine.memory_bytes(), "disk_bytes":core.engine.disk_bytes(), "dsl_cache":core.dsl.stats(),
            "levels":core.engine.table_counts(), "snapshots":core.engine.active_snapshots(), "recovery_read_only":core.engine.recovery_read_only(), "maintenance_error":core.engine.maintenance_error.lock().clone()
        }));
        Ok(
            serde_json::json!({"tenant":registration.info,"lifecycle":registration.lifecycle,"open":slot.core.is_some(),"installing":slot.installing.is_some(),"failure":slot.failure,"stats":stats}),
        )
    }
}

fn enqueue_maintenance(
    inner: &Inner,
    sender: &std::sync::mpsc::SyncSender<(Arc<Mutex<Slot>>, Option<usize>)>,
    next: &mut usize,
    cache_next: &mut usize,
) -> bool {
    let slots: Vec<_> = inner.slots.lock().values().cloned().collect();
    if slots.is_empty() {
        return true;
    }
    let mut candidate = None;
    if inner.memory.under_pressure() {
        // Cache reclamation does no disk I/O and must precede selecting dirty
        // data. Otherwise warm readers make every tiny writer rotate its WAL.
        let mut reclaim = crate::storage::ReclaimBudget::default();
        let start = *cache_next % slots.len();
        for offset in 0..slots.len() {
            if reclaim.exhausted() || !inner.memory.cache_pressure() {
                break;
            }
            let index = (start + offset) % slots.len();
            let core = slots[index].try_lock().and_then(|guard| guard.core.clone());
            if let Some(core) = core {
                core.engine.reclaim_cache(&mut reclaim);
            }
            *cache_next = (index + 1) % slots.len();
        }
        if inner.memory.under_pressure() {
            let mut open = 0;
            let mut largest = (0, 0);
            for (index, slot) in slots.iter().enumerate() {
                let core = {
                    let Some(guard) = slot.try_lock() else {
                        continue;
                    };
                    let Some(core) = &guard.core else { continue };
                    open += 1;
                    if guard.queued || guard.installing.is_some() {
                        continue;
                    }
                    core.clone()
                };
                let bytes = core.engine.pressure_flush_bytes();
                if bytes < inner.options.memtable_bytes && bytes > largest.1 {
                    largest = (index, bytes);
                }
            }
            // Scale the floor with tenant count: a fixed floor alone can leave
            // thousands of small memtables stuck below it at the global limit.
            let minimum = (inner.options.memtable_bytes / 8)
                .min(inner.memory.foreground_limit() / open.max(1) / 8)
                .max(1);
            if largest.1 >= minimum {
                candidate = Some((largest.0, minimum));
            }
        }
    }
    let start = *next % slots.len();
    let pressure_index = candidate.map(|(index, _)| index);
    for index in pressure_index.into_iter().chain(
        (0..slots.len())
            .map(|offset| (start + offset) % slots.len())
            .filter(|index| Some(*index) != pressure_index),
    ) {
        let slot = &slots[index];
        let Some(mut guard) = slot.try_lock() else {
            continue;
        };
        if guard.core.is_none() || guard.queued {
            continue;
        }
        guard.queued = true;
        let minimum = candidate
            .filter(|(chosen, _)| *chosen == index)
            .map(|(_, minimum)| minimum);
        match sender.try_send((slot.clone(), minimum)) {
            Ok(()) => *next = (index + 1) % slots.len(),
            Err(error) => {
                guard.queued = false;
                // Retry this tenant first instead of repeatedly filling the
                // bounded queue from the beginning of the same HashMap order.
                *next = index;
                return matches!(error, std::sync::mpsc::TrySendError::Full(_));
            }
        }
    }
    true
}

fn maintain_tenant(inner: &Arc<Inner>, slot: &Arc<Mutex<Slot>>, pressure_minimum: Option<usize>) {
    let Some(mut guard) = slot.try_lock() else {
        return;
    };
    if guard
        .core
        .as_ref()
        .is_some_and(|core| core.engine.recovery_required())
    {
        let core = guard.core.as_ref().unwrap();
        core.gate.close();
        core.cursors.lock().clear();
        core.engine.invalidate_snapshots();
        if Arc::strong_count(core) == 1 && Arc::strong_count(&core.engine) == 1 {
            guard.last_seq = None;
            guard.core = None;
        }
        return;
    }
    if guard.installing.is_some() {
        return;
    }
    let core = guard.core.clone();
    drop(guard);
    if let Some(core) = core {
        core.cursors.lock().retain(|_, c| c.valid());
        let handle = TenantHandle { core: core.clone() };
        let result = core
            .engine
            .ensure_durable()
            .and_then(|_| {
                pressure_minimum.map_or(Ok(()), |minimum| core.engine.flush_under_pressure(minimum))
            })
            .and_then(|_| core.engine.maintain())
            .and_then(|_| {
                if inner.node.writable() {
                    handle.retention_step().and_then(|_| handle.gc_step())
                } else {
                    Ok(())
                }
            });
        *core.engine.maintenance_error.lock() = result.err().map(|error| error.to_string());
        drop(handle);
        drop(core);
        let mut slot = slot.lock();
        if slot.core.as_ref().is_some_and(|core| {
            Arc::strong_count(core) == 1
                && Arc::strong_count(&core.engine) == 1
                && !core.engine.replication_pinned()
                && core.last_used.lock().elapsed()
                    >= Duration::from_secs(inner.options.idle_seconds)
        }) {
            slot.last_seq = slot
                .core
                .as_ref()
                .and_then(|core| core.engine.replication_position().ok());
            slot.core = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_cold_get_admission_does_not_hold_execution_permits() {
        use crate::http::{self, AuthConfig, Role, Token};
        use http_body_util::BodyExt;
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let a = store.create_tenant("cold-a").unwrap().id;
        let b = store.create_tenant("cold-b").unwrap().id;
        let good = store
            .tenant(store.create_tenant("warm").unwrap().id)
            .unwrap();
        good.create_collection("docs").unwrap();
        let ack = good
            .put(
                "docs",
                serde_json::json!({"_key":"confirmed","value":7}),
                None,
            )
            .unwrap();
        store.owner.stop.store(true, Ordering::Release);
        for worker in store.owner.workers.lock().drain(..) {
            worker.join().unwrap();
        }
        let app = http::app(
            store.clone(),
            AuthConfig {
                tokens: vec![Token {
                    subject: "admin".into(),
                    sha256: http::token_hash("cold-admission-secret"),
                    tenant: None,
                    role: Role::Admin,
                }],
            },
        )
        .unwrap();
        let slots = [store.slot(a), store.slot(b)];
        let held_slots = slots.clone();
        let (entered, ready) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let holder = thread::spawn(move || {
            let _a = held_slots[0].lock();
            let _b = held_slots[1].lock();
            entered.send(()).unwrap();
            let _ = wait.recv_timeout(Duration::from_secs(10));
        });
        ready.recv().unwrap();
        let references = slots.each_ref().map(Arc::strong_count);
        let mut waiting = Vec::new();
        for id in [a, b] {
            for _ in 0..32 {
                let app = app.clone();
                waiting.push(tokio::spawn(async move {
                    app.oneshot(
                        axum::http::Request::builder()
                            .uri(format!("/v1/tenants/{id}"))
                            .header("authorization", "Bearer cold-admission-secret")
                            .body(axum::body::Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap()
                }));
            }
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while slots
            .iter()
            .zip(references)
            .any(|(slot, n)| Arc::strong_count(slot) < n + 32)
        {
            assert!(
                Instant::now() < deadline,
                "requests did not enter blocked admission"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        for request in waiting {
            request.abort();
            let _ = request.await;
        }
        let request = |uri: String| {
            axum::http::Request::builder()
                .uri(uri)
                .header("authorization", "Bearer cold-admission-secret")
                .body(axum::body::Body::empty())
                .unwrap()
        };
        let cold = app
            .clone()
            .oneshot(request(format!("/v1/tenants/{a}")))
            .await
            .unwrap();
        let healthy = tokio::time::timeout(
            Duration::from_secs(1),
            app.oneshot(request(format!(
                "/v1/tenants/{}/collections/docs/documents/confirmed",
                good.id()
            ))),
        )
        .await;
        release.send(()).unwrap();
        holder.join().unwrap();
        assert_eq!(cold.status(), axum::http::StatusCode::TOO_MANY_REQUESTS);
        let healthy = healthy.unwrap().unwrap();
        assert!(healthy.status().is_success());
        let body: serde_json::Value =
            serde_json::from_slice(&healthy.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["_rev"], ack.rev);
    }
    #[test]
    fn management_drains_overlapping_short_readers_before_changing_quota() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let info = store.create_tenant("hot-management").unwrap();
        let receipt = {
            let tenant = store.tenant(info.id).unwrap();
            tenant.create_collection("docs").unwrap();
            tenant
                .put("docs", serde_json::json!({"_key":"ack","value":7}), None)
                .unwrap()
        };
        let barrier = Arc::new(std::sync::Barrier::new(3));
        thread::scope(|scope| {
            for _ in 0..2 {
                let store = store.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    let tenant = store.tenant(info.id).unwrap();
                    barrier.wait();
                    thread::sleep(Duration::from_millis(30));
                    drop(tenant);
                    let until = Instant::now() + Duration::from_millis(100);
                    while Instant::now() < until {
                        if let Ok(tenant) = store.tenant(info.id) {
                            let _ = tenant.get("docs", "ack");
                        }
                    }
                });
            }
            barrier.wait();
            store
                .set_quota(info.id, info.memory_bytes, info.disk_bytes + (1 << 20))
                .unwrap();
        });
        let tenant = store.tenant(info.id).unwrap();
        assert_eq!(
            tenant.get("docs", "ack").unwrap().unwrap()["_rev"],
            receipt.rev
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn locked_cold_tenant_does_not_block_async_health_or_timers() {
        use crate::http::{self, AuthConfig, Role, Token};
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let id = store.create_tenant("cold-http").unwrap().id;
        let app = http::app(
            store.clone(),
            AuthConfig {
                tokens: vec![Token {
                    subject: "admin".into(),
                    sha256: http::token_hash("cold-tenant-test-secret"),
                    tenant: None,
                    role: Role::Admin,
                }],
            },
        )
        .unwrap();
        let slot = store.slot(id);
        let (entered, ready) = std::sync::mpsc::channel();
        let holder = thread::spawn(move || {
            let _slot = slot.lock();
            entered.send(()).unwrap();
            thread::sleep(Duration::from_millis(350));
        });
        ready.recv().unwrap();
        let started = Instant::now();
        let requests: Vec<_> = (0..4)
            .map(|_| {
                let app = app.clone();
                tokio::spawn(async move {
                    app.oneshot(
                        axum::http::Request::builder()
                            .uri(format!("/v1/tenants/{id}"))
                            .header("authorization", "Bearer cold-tenant-test-secret")
                            .body(axum::body::Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap()
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let health = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/health")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let elapsed = started.elapsed();
        holder.join().unwrap();
        for request in requests {
            assert!(request.await.unwrap().status().is_success());
        }
        assert!(health.status().is_success());
        assert!(
            elapsed < Duration::from_millis(200),
            "async workers stalled for {elapsed:?}"
        );
    }
    #[test]
    fn replica_checkpoint_validation_shares_the_serving_tenant_budget() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(
            dir.path(),
            Options {
                global_memory_bytes: 64 << 20,
                tenant_memory_bytes: 8 << 20,
                memtable_bytes: 1 << 20,
                cache_bytes: 0,
                ..Options::default()
            },
        )
        .unwrap();
        let info = store.create_tenant("shared-checkpoint-budget").unwrap();
        let info = store.set_quota(info.id, 16 << 20, info.disk_bytes).unwrap();
        let (_, serving, _) = store.backup_context(info.id).unwrap();
        let held = serving.reserve(7 << 20).unwrap();
        let (validation, _) = store.import_context(Some(&info));
        assert_eq!(
            validation.reserve(10 << 20).unwrap_err().code,
            "RESOURCE_EXHAUSTED"
        );
        drop(held);
        drop(validation.reserve(10 << 20).unwrap());
        assert_eq!(store.memory_bytes(), 0);
        let (disaster_import, _) = store.import_context(None);
        assert_eq!(
            disaster_import.reserve(10 << 20).unwrap_err().code,
            "RESOURCE_EXHAUSTED"
        );
    }

    #[test]
    fn pressure_maintenance_selects_large_unpinned_tenants_and_scales_its_floor() {
        for (count, value_bytes, rows) in [(2, 512 << 10, 4), (64, 128 << 10, 1)] {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(
                dir.path(),
                Options {
                    global_memory_bytes: 64 << 20,
                    tenant_memory_bytes: 64 << 20,
                    memtable_bytes: 16 << 20,
                    cache_bytes: 0,
                    ..Options::default()
                },
            )
            .unwrap();
            store.owner.stop.store(true, Ordering::Release);
            for worker in store.owner.workers.lock().drain(..) {
                worker.join().unwrap();
            }
            let mut tenants = Vec::new();
            for n in 0..count {
                let id = store.create_tenant(&format!("dirty-{n}")).unwrap().id;
                let tenant = store.tenant(id).unwrap();
                tenant
                    .engine()
                    .transact(|tx| tx.put(b"confirmed".to_vec(), vec![n as u8; value_bytes]))
                    .unwrap();
                for row in 1..rows {
                    tenant
                        .engine()
                        .transact(|tx| {
                            tx.put(
                                format!("extra-{row}").into_bytes(),
                                vec![n as u8; value_bytes],
                            )
                        })
                        .unwrap();
                }
                tenants.push(tenant);
            }
            let light_id = store.create_tenant("light").unwrap().id;
            let light = store.tenant(light_id).unwrap();
            light
                .engine()
                .transact(|tx| tx.put(b"small".to_vec(), vec![1; 128]))
                .unwrap();
            let pinned = tenants[0].engine().snapshot().unwrap();
            thread::sleep(Duration::from_millis(1010));
            let padding = Budget {
                global: store.owner.inner.memory.clone(),
                tenant: Counter::new(64 << 20),
            };
            let _held = padding
                .reserve(padding.global.foreground_limit() - padding.global.used())
                .unwrap();
            let (sender, receiver) = std::sync::mpsc::sync_channel(128);
            assert!(enqueue_maintenance(
                &store.owner.inner,
                &sender,
                &mut 0,
                &mut 0
            ));
            let jobs: Vec<_> = receiver.try_iter().collect();
            assert_eq!(
                jobs.iter().filter(|(_, minimum)| minimum.is_some()).count(),
                1
            );
            let (slot, minimum) = &jobs[0];
            assert!(minimum.is_some());
            let selected = slot.lock().core.as_ref().unwrap().engine.info.id;
            assert_ne!(selected, light_id);
            assert_ne!(selected, tenants[0].engine().info.id);
            maintain_tenant(&store.owner.inner, slot, *minimum);
            let flushed = slot.lock().core.as_ref().unwrap().engine.metrics()["flush_bytes"]
                .as_u64()
                .unwrap();
            assert!(flushed > 0, "{}", store.status(selected).unwrap());
            assert_eq!(light.engine().metrics()["flush_bytes"], 0);
            assert_eq!(tenants[0].engine().metrics()["flush_bytes"], 0);
            assert_eq!(
                pinned
                    .get(b"confirmed", &mut Default::default())
                    .unwrap()
                    .unwrap()
                    .len(),
                value_bytes
            );
        }
    }

    #[test]
    fn a_full_maintenance_queue_keeps_scheduling_later_tenants() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Options::default()).unwrap();
        store.owner.stop.store(true, Ordering::Release);
        for worker in store.owner.workers.lock().drain(..) {
            worker.join().unwrap();
        }
        for n in 0..160 {
            let id = store.create_tenant(&format!("fair-{n}")).unwrap().id;
            drop(store.tenant(id).unwrap());
        }
        let (sender, receiver) = std::sync::mpsc::sync_channel(128);
        let mut next = 0;
        let mut served = std::collections::HashSet::new();
        for _ in 0..64 {
            assert!(enqueue_maintenance(
                &store.owner.inner,
                &sender,
                &mut next,
                &mut 0
            ));
            for _ in 0..5 {
                let (slot, _) = receiver.try_recv().unwrap();
                let mut slot = slot.lock();
                served.insert(slot.core.as_ref().unwrap().engine.info.id);
                slot.queued = false;
            }
        }
        assert_eq!(served.len(), 160, "repeated work starved later tenants");
    }
    #[test]
    fn io_open_failures_are_cached_briefly_and_recover_without_manual_close() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let id = store.create_tenant("cached-io-error").unwrap().id;
        let wal = fs::read_dir(dir.path().join("tenants").join(id.to_string()).join("wal"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mode = fs::metadata(&wal).unwrap().permissions();
        fs::set_permissions(&wal, fs::Permissions::from_mode(0o000)).unwrap();
        let failed = store.tenant(id).err().unwrap();
        fs::set_permissions(&wal, mode).unwrap();
        assert_eq!(failed.code, "IO_ERROR");
        assert_eq!(
            store.tenant(id).err().unwrap().code,
            "IO_ERROR",
            "replayed immediately after cached failure"
        );
        std::thread::sleep(Duration::from_millis(1100));
        store
            .tenant(id)
            .unwrap()
            .create_collection("recovered")
            .unwrap();
    }

    #[test]
    fn cold_export_reuses_the_open_engine_until_its_log_pages_catch_up() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Options::default()).unwrap();
        // Keep the cold-export lifetime check independent of maintenance handles.
        store.owner.stop.store(true, Ordering::Release);
        for worker in store.owner.workers.lock().drain(..) {
            worker.join().unwrap();
        }
        store
            .configure_replication(&crate::replication::Config {
                cluster: Uuid::new_v4(),
                role: crate::replication::Role::Primary,
                peer_url: "http://127.0.0.1:1".into(),
                token_file: dir.path().join("unused"),
                poll_ms: 10,
                log_retention_bytes: 1 << 20,
            })
            .unwrap();
        let info = store.create_tenant("paged-cold-export").unwrap();
        let tenant = store.tenant(info.id).unwrap();
        for n in 0..600u64 {
            tenant
                .engine()
                .transact(|tx| tx.put(format!("Z{n:04}").into_bytes(), vec![42; 1024]))
                .unwrap();
        }
        drop(tenant);
        store.close_tenant(info.id).unwrap();
        let state = store.replication_state();
        let mut request = crate::replication::LogRequest {
            protocol: crate::replication::PROTOCOL,
            cluster: state.cluster,
            epoch: state.epoch,
            tenant: info.id,
            generation: info.generation,
            after: 0,
            cursor: None,
        };
        let mut runtime = None;
        let mut pages = 0;
        loop {
            let page = store.export_logs(&request).unwrap();
            pages += 1;
            if page.through == page.source_position {
                assert!(
                    store.slot(info.id).lock().core.is_none(),
                    "caught-up cold export stayed open"
                );
                break;
            }
            let id = store
                .slot(info.id)
                .lock()
                .core
                .as_ref()
                .unwrap()
                .engine
                .runtime_id;
            assert_eq!(
                *runtime.get_or_insert(id),
                id,
                "recovered the WAL again for a later page"
            );
            request.after = page.through;
            request.cursor = page.cursor;
        }
        assert!(pages >= 3);
    }

    #[test]
    fn a_temporary_position_reader_does_not_close_a_reused_or_reopened_engine() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let id = store.create_tenant("cold-position-reader").unwrap().id;
        let (handle, opened) = store.tenant_with_open_marker(id).unwrap();
        let runtime = handle.engine().runtime_id;
        let touched = opened.unwrap();
        drop(handle);
        std::thread::sleep(Duration::from_millis(1));
        drop(store.tenant(id).unwrap());
        store.close_unmodified_tenant(id, runtime, touched).unwrap();
        assert_eq!(store.status(id).unwrap()["open"], true);
        let deadline = Instant::now() + Duration::from_secs(5);
        while store.close_tenant(id).is_err() {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(10));
        }
        let (fresh, _) = store.tenant_with_open_marker(id).unwrap();
        assert_ne!(fresh.engine().runtime_id, runtime);
        drop(fresh);
        store.close_unmodified_tenant(id, runtime, touched).unwrap();
        assert_eq!(store.status(id).unwrap()["open"], true);
    }
    #[test]
    fn deleted_tenant_history_does_not_consume_live_tenant_capacity() {
        let dir = tempfile::tempdir().unwrap();
        drop(Store::open(dir.path(), Options::default()).unwrap());
        let path = dir.path().join("control/tenants.json");
        let mut registry: Registry = read_json(&path).unwrap();
        for _ in 0..16_384 {
            let id = Uuid::new_v4();
            registry.tenants.insert(
                id,
                Registration {
                    info: TenantInfo {
                        id,
                        name: "deleted".into(),
                        generation: Uuid::new_v4(),
                        history: Uuid::new_v4(),
                        memory_bytes: Options::default().tenant_memory_bytes,
                        disk_bytes: Options::default().tenant_disk_bytes,
                    },
                    lifecycle: Lifecycle::Deleted,
                },
            );
        }
        atomic_json(&path, &registry).unwrap();
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let tenant = store.create_tenant("new-live-tenant").unwrap();
        store
            .tenant(tenant.id)
            .unwrap()
            .create_collection("docs")
            .unwrap();
        assert_eq!(store.tenants().len(), 1);
    }
}
