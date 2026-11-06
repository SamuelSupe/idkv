use super::{TenantCore, TenantHandle};
use crate::{
    Error, Result,
    model::*,
    resources::{Budget, Counter, IoScheduler, RequestGate},
    storage::{Engine, atomic_json, read_json, reject_symlink, sync_dir},
};
use fs2::FileExt;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
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

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum Lifecycle {
    Active,
    Deleting,
    Deleted,
    Restoring { stage: Uuid, old: Uuid },
}
#[derive(Clone, Serialize, Deserialize)]
struct Registration {
    info: TenantInfo,
    lifecycle: Lifecycle,
}
#[derive(Clone, Serialize, Deserialize)]
struct Registry {
    version: u32,
    #[serde(default)]
    replication_enabled: bool,
    tenants: BTreeMap<Uuid, Registration>,
}
mod admission;
mod lifecycle;
mod replication;
use lifecycle::{
    clean_lifecycle_debris, collect_lifecycle_debris, recover_lifecycle, remove_lifecycle_debris,
};

struct Slot {
    last_seq: Option<u64>,
    restoring: bool,
    installing: Option<Uuid>,
    memory: Arc<Counter>,
    core: Option<Arc<TenantCore>>,
    failure: Option<Error>,
    failure_until: Option<Instant>,
    queued: bool,
}
struct Inner {
    root: PathBuf,
    options: Options,
    _lock: Arc<File>,
    registry: RwLock<Registry>,
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
    /// Gauges for currently open engines. Engine cumulative values can decrease
    /// when an idle tenant closes, so these are deliberately not counters.
    pub fn open_engine_metrics(&self) -> BTreeMap<String, u64> {
        let slots: Vec<_> = self.owner.inner.slots.lock().values().cloned().collect();
        let mut out = BTreeMap::new();
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
                version: 4,
                replication_enabled: false,
                tenants: BTreeMap::new(),
            };
            atomic_json(&registry_path, &registry)?;
            registry
        };
        if !matches!(registry.version, 1..=4) {
            return Err(Error::corrupt("registry version"));
        }
        let previous = serde_json::to_vec(&registry)?;
        registry.replication_enabled |= registry.version == 2;
        registry.version = 4;
        recover_lifecycle(&root, &mut registry)?;
        if serde_json::to_vec(&registry)? != previous {
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
            slots: Mutex::new(HashMap::new()),
            memory: Counter::new(options.global_memory_bytes),
            io: IoScheduler::new(options.io_workers),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = std::sync::mpsc::sync_channel::<Arc<Mutex<Slot>>>(128);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::new();
        for _ in 0..options.io_workers.min(4) {
            let weak = Arc::downgrade(&inner);
            let receiver = receiver.clone();
            let stop = stop.clone();
            workers.push(thread::spawn(move || {
                loop {
                    let job = receiver.lock().recv();
                    let Ok(slot) = job else {
                        break;
                    };
                    if !stop.load(Ordering::Acquire)
                        && let Some(inner) = weak.upgrade()
                    {
                        maintain_tenant(&inner, &slot);
                    }
                    slot.lock().queued = false;
                }
            }));
        }
        let weak = Arc::downgrade(&inner);
        let worker_stop = stop.clone();
        let coordinator = thread::spawn(move || {
            while !worker_stop.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(100));
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                let slots: Vec<_> = inner.slots.lock().values().cloned().collect();
                for slot in slots {
                    let Some(mut guard) = slot.try_lock() else {
                        continue;
                    };
                    if guard.core.is_some() && !guard.queued {
                        guard.queued = true;
                        if sender.try_send(slot.clone()).is_err() {
                            guard.queued = false;
                        }
                    }
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
        match tenant {
            Some(id) => self.backup_context(id)?.1.reserve(bytes),
            None => self.reserve_http(bytes),
        }
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
    pub(crate) fn import_context(&self) -> (Budget, Arc<IoScheduler>) {
        (
            Budget {
                global: self.owner.inner.memory.clone(),
                tenant: Counter::new(self.options().tenant_memory_bytes),
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
        let path = inner.root.join("tenants").join(info.id.to_string());
        Engine::create(&path, &info)?;
        let mut next = registry.clone();
        next.tenants.insert(
            info.id,
            Registration {
                info: info.clone(),
                lifecycle: Lifecycle::Active,
            },
        );
        atomic_json(&inner.root.join("control/tenants.json"), &next)?;
        *registry = next;
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
            return Err(Error::new(
                "BUSY",
                "replica checkpoint installation is draining readers",
            ));
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
        let slot = self.slot(id);
        let mut slot = slot.lock();
        if slot.restoring || slot.installing.is_some() {
            return Err(Error::new("BUSY", "tenant restore preparation is active"));
        }
        self.close_slot(&mut slot)?;
        let _offline = self.offline_lock(id)?;
        Ok(())
    }
    pub fn set_quota(&self, id: Uuid, memory_bytes: usize, disk_bytes: u64) -> Result<TenantInfo> {
        let _permission = self.owner.inner.node.write()?;
        if memory_bytes < self.options().memtable_bytes * 4
            || memory_bytes > self.options().global_memory_bytes
            || disk_bytes < 1 << 20
        {
            return Err(Error::invalid("quota bounds"));
        }
        let slot = self.slot(id);
        let mut slot = slot.lock();
        if slot.restoring || slot.installing.is_some() {
            return Err(Error::new("BUSY", "tenant restore preparation is active"));
        }
        self.close_slot(&mut slot)?;
        let _offline = self.offline_lock(id)?;
        let inner = &self.owner.inner;
        let mut registry = inner.registry.write();
        let mut next = registry.clone();
        let registration = next
            .tenants
            .get_mut(&id)
            .ok_or_else(|| Error::missing("tenant"))?;
        if !matches!(registration.lifecycle, Lifecycle::Active) {
            return Err(Error::new("UNAVAILABLE", "tenant not active"));
        }
        slot.memory.set_limit(memory_bytes)?;
        registration.info.memory_bytes = memory_bytes;
        registration.info.disk_bytes = disk_bytes;
        let info = registration.info.clone();
        atomic_json(&inner.root.join("control/tenants.json"), &next)?;
        *registry = next;
        atomic_json(
            &inner
                .root
                .join("tenants")
                .join(id.to_string())
                .join("TENANT.meta"),
            &info,
        )?;
        Ok(info)
    }
    pub fn delete_tenant(&self, id: Uuid) -> Result<()> {
        self.delete_tenant_inner(id, self.owner.inner.node.write()?)
    }
    fn delete_tenant_inner(
        &self,
        id: Uuid,
        permission: parking_lot::RwLockReadGuard<'_, ()>,
    ) -> Result<()> {
        let slot = self.slot(id);
        let mut slot = slot.lock();
        if slot.restoring || slot.installing.is_some() {
            return Err(Error::new("BUSY", "tenant restore preparation is active"));
        }
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
        registration.lifecycle = Lifecycle::Deleting;
        // A failed publication may already be durable. Never reopen the old
        // generation for writes before restart has resolved the journal.
        *registry = next;
        let result = (|| {
            atomic_json(&inner.root.join("control/tenants.json"), &*registry)?;
            crate::storage::errorpoint("tenant_delete_journal_published")?;
            recover_lifecycle(&inner.root, &mut registry)?;
            atomic_json(&inner.root.join("control/tenants.json"), &*registry)?;
            collect_lifecycle_debris(&inner.root, &registry, false)
        })();
        if result.is_err() {
            slot.record_failure(Error::new(
                "UNAVAILABLE",
                "tenant deletion outcome is uncertain; restart Store to recover",
            ));
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

fn maintain_tenant(inner: &Arc<Inner>, slot: &Arc<Mutex<Slot>>) {
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
