mod maintenance;
mod read;
mod recovery;
mod replication;
mod write;
use super::{
    cache::Cache,
    files::{atomic_bytes, atomic_json, read_json, reject_symlink, sync_dir},
    sst::{Record, Table, TableCursor, TableInfo, TableWriter},
    wal::{self, Wal, WalInfo},
};
use crate::resources::Bytes;
use crate::{
    Error, Result, codec,
    model::{Options, ReadStats, TenantInfo},
    resources::{Budget, IoScheduler, Reservation},
};
use fs2::FileExt;
use parking_lot::{Condvar, Mutex, RwLock};
pub use read::Scan;
pub(crate) use read::ScanRange;
use read::{HeapRecord, get_one};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BinaryHeap, HashSet},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use uuid::Uuid;
pub use write::Transaction;

pub(crate) struct LogRetention {
    engine: Arc<Engine>,
    id: Uuid,
}
impl LogRetention {
    pub(crate) fn valid(&self) -> bool {
        self.engine
            .log_retention
            .lock()
            .get(&self.id)
            .is_some_and(|(_, expires)| expires.is_none_or(|expires| expires > Instant::now()))
    }
    pub(crate) fn renew(&self, position: u64) -> bool {
        self.renew_at(position, Instant::now())
    }
    fn renew_at(&self, position: u64, now: Instant) -> bool {
        let mut leases = self.engine.log_retention.lock();
        let Some((floor, expires)) = leases.get_mut(&self.id) else {
            return false;
        };
        if expires.is_some_and(|expires| expires <= now) {
            leases.remove(&self.id);
            return false;
        }
        *floor = (*floor).max(position);
        *expires = Some(now + Duration::from_secs(300));
        true
    }
}
impl Drop for LogRetention {
    fn drop(&mut self) {
        self.engine.log_retention.lock().remove(&self.id);
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    tenant: Uuid,
    generation: Uuid,
    checkpoint: u64,
    tables: Vec<TableInfo>,
    wals: Vec<WalInfo>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    retained_wals: Vec<WalInfo>,
}
const REPLICA_INSERT_CHUNK: usize = 8;

struct MemTable {
    data: RwLock<BTreeMap<Vec<u8>, Vec<Record>>>,
    memory: Mutex<Vec<Arc<Reservation>>>,
    bytes: AtomicUsize,
}
impl MemTable {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            data: RwLock::new(BTreeMap::new()),
            memory: Mutex::new(vec![]),
            bytes: AtomicUsize::new(0),
        })
    }
    fn apply(&self, mut records: Vec<Record>, memory: Reservation) {
        self.retain(&mut records, memory);
        self.insert(records);
    }
    fn retain(&self, records: &mut [Record], memory: Reservation) {
        self.bytes.fetch_add(memory.size(), Ordering::AcqRel);
        let memory = Arc::new(memory);
        for record in records {
            if let Some(value) = &mut record.value {
                Arc::make_mut(value).charge = Some(memory.clone());
            }
        }
        self.memory.lock().push(memory);
    }
    fn apply_page(&self, batches: impl Iterator<Item = (Vec<Record>, Reservation)>) {
        let mut pending = Vec::with_capacity(REPLICA_INSERT_CHUNK);
        for (mut records, memory) in batches {
            self.retain(&mut records, memory);
            // Coalesce tiny transactions without shrinking the insertion quantum
            // of existing large batches, which already amortize lock handoffs.
            if records.len() >= REPLICA_INSERT_CHUNK {
                self.insert(pending.drain(..));
                self.insert(records);
                continue;
            }
            for record in records {
                pending.push(record);
                if pending.len() == REPLICA_INSERT_CHUNK {
                    self.insert(pending.drain(..));
                }
            }
        }
        self.insert(pending);
    }
    fn insert(&self, records: impl IntoIterator<Item = Record>) {
        let mut records = records.into_iter().peekable();
        while records.peek().is_some() {
            let mut data = self.data.write();
            for record in records.by_ref().take(64) {
                // Keep one tree search for both new and existing keys. Moving
                // the record avoids a second key allocation and value Arc clone.
                let versions = data.entry(record.key.clone()).or_default();
                debug_assert!(versions.last().is_none_or(|old| old.seq < record.seq));
                versions.push(record);
            }
        }
    }
    fn get(&self, key: &[u8], seq: u64) -> Option<Record> {
        let data = self.data.read();
        let versions = data.get(key)?;
        let latest = versions.last()?;
        if latest.seq <= seq {
            return Some(latest.clone());
        }
        let end = versions.partition_point(|r| r.seq <= seq);
        end.checked_sub(1).map(|i| versions[i].clone())
    }
}
#[derive(Clone)]
struct View {
    mem: Arc<MemTable>,
    frozen: Vec<Arc<MemTable>>,
    tables: Arc<VersionSet>,
}
struct VersionSet {
    files: Vec<Arc<Table>>,
    levels: Vec<Vec<Arc<Table>>>,
    level_bytes: Vec<u64>,
    l0_pressure: usize,
    _memory: Reservation,
}
impl std::ops::Deref for VersionSet {
    type Target = [Arc<Table>];
    fn deref(&self) -> &Self::Target {
        &self.files
    }
}
impl VersionSet {
    fn new(files: Vec<Arc<Table>>, budget: &Budget) -> Result<Arc<Self>> {
        validate_levels(&files)?;
        let max_level = files.iter().map(|t| t.info.level).max().unwrap_or(0) as usize;
        if max_level > 16 {
            return Err(Error::corrupt("LSM level limit"));
        }
        let memory = budget.reserve(files.len() * 64 + 256 + (max_level + 1) * 8)?;
        let mut levels = vec![Vec::new(); max_level + 1];
        let mut level_bytes = vec![0u64; max_level + 1];
        for table in &files {
            levels[table.info.level as usize].push(table.clone());
            level_bytes[table.info.level as usize] += table.info.size;
        }
        for level in &mut levels {
            level.sort_by(|a, b| a.info.first.cmp(&b.info.first));
        }
        let l0_pressure = l0_pressure(&files);
        Ok(Arc::new(Self {
            files,
            levels,
            level_bytes,
            l0_pressure,
            _memory: memory,
        }))
    }
    fn candidates(&self, keys: &[Vec<u8>]) -> Vec<Arc<Table>> {
        let mut chosen = BTreeMap::new();
        for (level, tables) in self.levels.iter().enumerate() {
            if level == 0 {
                for table in tables {
                    if keys
                        .iter()
                        .any(|k| table.info.first <= *k && table.info.last >= *k)
                    {
                        chosen.insert(table.info.id, table.clone());
                    }
                }
            } else {
                for key in keys {
                    let i = tables.partition_point(|t| t.info.last < *key);
                    if let Some(table) = tables.get(i)
                        && table.info.first <= *key
                    {
                        chosen.insert(table.info.id, table.clone());
                    }
                }
            }
        }
        let mut files: Vec<_> = chosen.into_values().collect();
        files.sort_by_key(|t| std::cmp::Reverse(t.info.max_seq));
        files
    }
}
struct Writer {
    wal: Wal,
    manifest: Manifest,
    staged: u64,
    catalog_staged: u64,
    syncing: bool,
    poison: Option<Error>,
}
struct DiskInventory {
    checked: Instant,
    non_wal_bytes: u64,
}

pub struct Engine {
    pub info: TenantInfo,
    pub runtime_id: Uuid,
    pub options: Options,
    pub budget: Budget,
    pub io: Arc<IoScheduler>,
    path: PathBuf,
    _lock: File,
    // A handle or snapshot may outlive Store; keep the root ownership lock.
    manager_guard: Mutex<Option<Arc<File>>>,
    authority: RwLock<Option<Arc<crate::replication::Node>>>,
    view: RwLock<View>,
    writer: Mutex<Writer>,
    committed: AtomicU64,
    catalog_seq: AtomicU64,
    durable: Condvar,
    progress: Mutex<()>,
    progress_wake: Condvar,
    maintenance: Mutex<()>,
    cache: Arc<Cache>,
    snapshots: Mutex<BTreeMap<Uuid, Weak<Lease>>>,
    log_retention: Mutex<BTreeMap<Uuid, (u64, Option<Instant>)>>,
    disk: AtomicU64,
    disk_inventory: Mutex<DiskInventory>,
    metrics: Metrics,
    closed: AtomicBool,
    recovery_required: AtomicBool,
    recovery_read_only: bool,
    draining: AtomicBool,
    pub maintenance_error: Mutex<Option<String>>,
}
struct Lease {
    id: Uuid,
    seq: u64,
    expires: Instant,
    revoked: AtomicBool,
    view: View,
    engine: Arc<Engine>,
}

#[derive(Clone)]
pub struct Snapshot(Arc<Lease>);

impl Engine {
    pub fn create(path: &Path, info: &TenantInfo) -> Result<()> {
        fs::create_dir(path)?;
        fs::create_dir(path.join("wal"))?;
        fs::create_dir(path.join("sst"))?;
        atomic_json(&path.join("TENANT.meta"), info)?;
        let wal = Wal::create(&path.join("wal"), info.id, info.generation, 1)?;
        let manifest = Manifest {
            version: 1,
            tenant: info.id,
            generation: info.generation,
            checkpoint: 0,
            tables: vec![],
            wals: vec![wal.info],
            retained_wals: vec![],
        };
        publish(path, &manifest)?;
        sync_dir(path.parent().unwrap())
    }
    pub fn open(
        path: PathBuf,
        info: TenantInfo,
        options: Options,
        budget: Budget,
        io: Arc<IoScheduler>,
    ) -> Result<Arc<Self>> {
        reject_symlink(&path)?;
        reject_symlink(&path.join("wal"))?;
        reject_symlink(&path.join("sst"))?;
        let lock_path = path.join("LOCK");
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
            .map_err(|_| Error::new("BUSY", "tenant database is already open"))?;
        let actual: TenantInfo = read_json(&path.join("TENANT.meta"))?;
        if actual.id != info.id || actual.generation != info.generation {
            return Err(Error::corrupt("tenant identity mismatch"));
        }
        let manifest = load_manifest(&path)?;
        if manifest.version != 1
            || manifest.tenant != info.id
            || manifest.generation != info.generation
            || manifest.wals.is_empty()
        {
            return Err(Error::corrupt("manifest identity or version"));
        }
        let cache = Arc::new(Cache::new(options.cache_bytes.min(info.memory_bytes / 3)));
        let mut tables = Vec::new();
        for table in &manifest.tables {
            tables.push(Table::open(
                &path.join("sst"),
                table.clone(),
                info.id,
                info.generation,
                io.clone(),
                budget.clone(),
                cache.clone(),
            )?);
        }
        validate_levels(&tables)?;
        let mem = MemTable::new();
        let mut staged = manifest.checkpoint;
        let mut active = None;
        let synced = recovery::SyncedWals::load(&path, &info)?;
        let mut recovery_read_only = false;
        for (i, wal_info) in manifest.wals.iter().enumerate() {
            let wal = Wal::recover(
                &path.join("wal"),
                wal_info,
                &info,
                i + 1 == manifest.wals.len(),
                synced.as_ref().and_then(|mark| mark.length(wal_info.id)),
                &budget,
                |seq, records| {
                    if seq <= manifest.checkpoint {
                        return Ok(());
                    }
                    if seq != staged + 1 {
                        return Err(Error::corrupt("WAL sequence gap"));
                    }
                    let reservation =
                        budget.reserve(records.iter().map(Record::size).sum::<usize>() * 2)?;
                    mem.apply(records, reservation);
                    staged = seq;
                    Ok(())
                },
            )?;
            recovery_read_only |= wal.recovery_read_only;
            active = Some(wal);
        }
        if !recovery_read_only {
            cleanup_unreferenced(&path, &manifest, &budget)?;
        }
        let disk = directory_size(&path)?;
        let non_wal_bytes = disk - active.as_ref().unwrap().file.metadata()?.len();
        let versions = VersionSet::new(tables, &budget)?;
        Ok(Arc::new(Self {
            info,
            runtime_id: Uuid::new_v4(),
            options,
            budget,
            io,
            path,
            _lock: lock,
            manager_guard: Mutex::new(None),
            authority: RwLock::new(None),
            view: RwLock::new(View {
                mem,
                frozen: vec![],
                tables: versions,
            }),
            writer: Mutex::new(Writer {
                wal: active.unwrap(),
                manifest,
                staged,
                catalog_staged: staged,
                syncing: false,
                poison: None,
            }),
            committed: AtomicU64::new(staged),
            catalog_seq: AtomicU64::new(staged),
            durable: Condvar::new(),
            progress: Mutex::new(()),
            progress_wake: Condvar::new(),
            maintenance: Mutex::new(()),
            cache,
            snapshots: Mutex::new(BTreeMap::new()),
            log_retention: Mutex::new(BTreeMap::new()),
            disk: AtomicU64::new(disk),
            disk_inventory: Mutex::new(DiskInventory {
                checked: Instant::now(),
                non_wal_bytes,
            }),
            metrics: Metrics::default(),
            closed: AtomicBool::new(false),
            recovery_required: AtomicBool::new(false),
            recovery_read_only,
            draining: AtomicBool::new(false),
            maintenance_error: Mutex::new(None),
        }))
    }
    pub(crate) fn attach_manager(
        &self,
        guard: Arc<File>,
        authority: Arc<crate::replication::Node>,
    ) {
        *self.manager_guard.lock() = Some(guard);
        *self.authority.write() = Some(authority);
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn catalog_seq(&self) -> u64 {
        self.catalog_seq.load(Ordering::Acquire)
    }
    pub(crate) fn replication_epoch(&self) -> Option<Uuid> {
        self.authority.read().as_ref().and_then(|node| {
            let state = node.state.read();
            state.enabled.then_some(state.epoch)
        })
    }
    pub fn seq(&self) -> u64 {
        self.committed.load(Ordering::Acquire)
    }
    pub(crate) fn replication_position(&self) -> Result<u64> {
        self.ensure_durable()?;
        let writer = self.writer.lock();
        if let Some(error) = &writer.poison {
            return Err(error.clone());
        }
        Ok(self.seq())
    }
    fn poison_writer(&self, writer: &mut Writer, error: Error) {
        self.recovery_required.store(true, Ordering::Release);
        writer.poison = Some(error);
    }
    pub(crate) fn recovery_required(&self) -> bool {
        self.recovery_required.load(Ordering::Acquire)
    }
    pub(crate) fn invalidate_snapshots(&self) {
        for lease in self.snapshots.lock().values().filter_map(Weak::upgrade) {
            lease.revoked.store(true, Ordering::Release);
        }
    }
    pub(crate) fn wait_progress(&self, observed: u64, timeout: Duration) {
        let mut progress = self.progress.lock();
        if self.seq() == observed && !self.draining.load(Ordering::Acquire) {
            self.progress_wake
                .wait_for(&mut progress, timeout.min(Duration::from_millis(10)));
        }
    }
    pub(crate) fn set_draining(&self, draining: bool) {
        self.draining.store(draining, Ordering::Release);
        let _progress = self.progress.lock();
        self.progress_wake.notify_all();
    }
    pub(crate) fn check_draining(&self) -> Result<()> {
        if self.draining.load(Ordering::Acquire) {
            return Err(Error::new(
                "BUSY",
                "tenant is draining for checkpoint installation",
            ));
        }
        Ok(())
    }
    fn notify_durable(&self) {
        self.durable.notify_all();
        let _progress = self.progress.lock();
        self.progress_wake.notify_all();
    }
    pub fn writable(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
            && !self.recovery_read_only
            && self.writer.lock().poison.is_none()
            && self
                .authority
                .read()
                .as_ref()
                .is_none_or(|node| node.writable())
    }
    pub fn disk_bytes(&self) -> u64 {
        self.disk.load(Ordering::Acquire)
    }
    pub fn memory_bytes(&self) -> usize {
        self.budget.tenant.used()
    }
    pub fn active_snapshots(&self) -> usize {
        self.snapshots
            .lock()
            .values()
            .filter(|w| w.strong_count() > 0)
            .count()
    }
    pub fn snapshot(self: &Arc<Self>) -> Result<Snapshot> {
        let view = self.view.read();
        self.pin(view.clone(), self.seq(), self.options.snapshot_seconds)
    }
    pub(crate) fn retain_replication_log(self: &Arc<Self>) -> LogRetention {
        let lease = LogRetention {
            engine: self.clone(),
            id: Uuid::new_v4(),
        };
        self.log_retention
            .lock()
            // Preparation owns this handle while copying and hashing. The byte
            // window can invalidate it; inactivity starts after preparation.
            .insert(lease.id, (0, None));
        lease
    }
    fn pin(self: &Arc<Self>, view: View, seq: u64, seconds: u64) -> Result<Snapshot> {
        let mut registry = self.snapshots.lock();
        registry.retain(|_, weak| weak.strong_count() > 0);
        if registry.len() >= self.options.max_snapshots {
            return Err(Error::limit("snapshot limit reached"));
        }
        let lease = Arc::new(Lease {
            id: Uuid::new_v4(),
            seq,
            expires: Instant::now() + Duration::from_secs(seconds),
            revoked: AtomicBool::new(false),
            view,
            engine: self.clone(),
        });
        registry.insert(lease.id, Arc::downgrade(&lease));
        Ok(Snapshot(lease))
    }
}

impl Snapshot {
    pub(crate) fn replication_epoch(&self) -> Option<Uuid> {
        self.0.engine.replication_epoch()
    }
}

fn publish(path: &Path, manifest: &Manifest) -> Result<()> {
    let name = format!("MANIFEST-{}", Uuid::new_v4());
    atomic_json(&path.join(&name), manifest)?;
    atomic_bytes(&path.join("CURRENT"), name.as_bytes())?;
    super::failpoint("manifest_published");
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let old = entry.file_name().to_string_lossy().to_string();
        if old.starts_with("MANIFEST-") && old != name {
            let _ = fs::remove_file(entry.path());
        }
    }
    Ok(())
}
fn load_manifest(path: &Path) -> Result<Manifest> {
    reject_symlink(&path.join("CURRENT"))?;
    let name = fs::read_to_string(path.join("CURRENT"))?;
    let Some(id) = name.strip_prefix("MANIFEST-") else {
        return Err(Error::corrupt("CURRENT manifest name"));
    };
    Uuid::parse_str(id).map_err(|_| Error::corrupt("CURRENT manifest UUID"))?;
    read_json(&path.join(name))
}
fn validate_levels(tables: &[Arc<Table>]) -> Result<()> {
    for level in 1..=16 {
        let mut files: Vec<_> = tables.iter().filter(|t| t.info.level == level).collect();
        files.sort_by(|a, b| a.info.first.cmp(&b.info.first));
        if files
            .windows(2)
            .any(|pair| pair[0].info.last >= pair[1].info.first)
        {
            return Err(Error::corrupt("overlapping leveled SSTs"));
        }
    }
    Ok(())
}
pub(crate) fn directory_size(path: &Path) -> Result<u64> {
    directory_size_except(path, None)
}
fn directory_size_except(path: &Path, excluded: Option<&Path>) -> Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        let meta = match fs::symlink_metadata(&entry_path) {
            Ok(meta) => meta,
            // Obsolete SSTs can disappear when the last snapshot drops.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if meta.file_type().is_symlink() {
            return Err(Error::corrupt("unexpected symlink in tenant directory"));
        }
        if excluded == Some(entry_path.as_path()) {
            continue;
        }
        let size = if meta.is_dir() {
            directory_size_except(&entry_path, excluded)?
        } else {
            meta.len()
        };
        total = total
            .checked_add(size)
            .ok_or_else(|| Error::limit("directory size overflow"))?;
    }
    Ok(total)
}
fn cleanup_unreferenced(path: &Path, manifest: &Manifest, budget: &Budget) -> Result<()> {
    let count = manifest.tables.len() + manifest.wals.len() + manifest.retained_wals.len();
    // The linear membership pass is an optimization, not a prerequisite for
    // recovering from a failed flush under memory pressure.
    let _memory = budget
        .reserve(count.saturating_mul(128).saturating_add(512))
        .ok();
    let tables: Option<HashSet<_>> = _memory.as_ref().map(|_| {
        manifest
            .tables
            .iter()
            .map(|t| format!("{}.sst", t.id))
            .collect()
    });
    let wals: Option<HashSet<_>> = _memory.as_ref().map(|_| {
        manifest
            .wals
            .iter()
            .chain(&manifest.retained_wals)
            .map(|w| format!("{}.wal", w.id))
            .collect()
    });
    for entry in fs::read_dir(path.join("sst"))? {
        let entry = entry?;
        reject_symlink(&entry.path())?;
        let name = entry.file_name().to_string_lossy().to_string();
        if !tables.as_ref().map_or_else(
            || {
                manifest
                    .tables
                    .iter()
                    .any(|t| name == format!("{}.sst", t.id))
            },
            |files| files.contains(&name),
        ) {
            fs::remove_file(entry.path())?;
        }
    }
    for entry in fs::read_dir(path.join("wal"))? {
        let entry = entry?;
        reject_symlink(&entry.path())?;
        let name = entry.file_name().to_string_lossy().to_string();
        if !wals.as_ref().map_or_else(
            || {
                manifest
                    .wals
                    .iter()
                    .chain(&manifest.retained_wals)
                    .any(|w| name == format!("{}.wal", w.id))
            },
            |files| files.contains(&name),
        ) {
            fs::remove_file(entry.path())?;
        }
    }
    let current = fs::read_to_string(path.join("CURRENT"))?;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if (name.starts_with("MANIFEST-") && name != current) || name.starts_with(".tmp-") {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn key_domain(key: &[u8]) -> &[u8] {
    let length = match key.first() {
        Some(b'D') => 17,
        Some(b'I' | b'B') => 33,
        _ => 1,
    };
    &key[..key.len().min(length)]
}
fn l0_pressure(tables: &[Arc<Table>]) -> usize {
    let mut counts = BTreeMap::new();
    for table in tables.iter().filter(|t| t.info.level == 0) {
        *counts
            .entry(key_domain(&table.info.first))
            .or_insert(0usize) += 1;
    }
    counts.into_values().max().unwrap_or(0)
}

#[derive(Default)]
struct Metrics {
    document_bytes: AtomicU64,
    wal_bytes: AtomicU64,
    flush_bytes: AtomicU64,
    compaction_read_bytes: AtomicU64,
    compaction_write_bytes: AtomicU64,
    commits: AtomicU64,
    fsyncs: AtomicU64,
    write_lock_wait_ns: AtomicU64,
}
impl Metrics {
    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "document_bytes": self.document_bytes.load(Ordering::Relaxed),
            "wal_bytes": self.wal_bytes.load(Ordering::Relaxed),
            "flush_bytes": self.flush_bytes.load(Ordering::Relaxed),
            "compaction_read_bytes": self.compaction_read_bytes.load(Ordering::Relaxed),
            "compaction_write_bytes": self.compaction_write_bytes.load(Ordering::Relaxed),
            "commits": self.commits.load(Ordering::Relaxed),
            "fsyncs": self.fsyncs.load(Ordering::Relaxed),
            "write_lock_wait_ns": self.write_lock_wait_ns.load(Ordering::Relaxed),
        })
    }
}
