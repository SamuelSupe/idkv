use super::*;
impl Engine {
    pub fn transact<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        self.transact_inner(f, false, false, None)
            .map(|(result, _)| result)
    }
    pub fn transact_snapshot<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut Transaction<'_>) -> Result<T>,
    ) -> Result<(T, Snapshot)> {
        self.transact_inner(f, true, false, None)
            .map(|(result, snapshot)| (result, snapshot.unwrap()))
    }
    pub(crate) fn transact_maintenance<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        self.transact_inner(f, false, true, None)
            .map(|(result, _)| result)
    }
    pub(crate) fn transact_waiting<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut Transaction<'_>) -> Result<T>,
        check: &dyn Fn() -> Result<()>,
        limit: Option<Duration>,
    ) -> Result<T> {
        let deadline = limit.map(|limit| Instant::now() + limit);
        let bounded = || {
            check()?;
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(Error::new(
                    "DEADLINE_EXCEEDED",
                    "index write admission deadline",
                ));
            }
            Ok(())
        };
        self.transact_inner(f, false, false, Some(&bounded))
            .map(|(result, _)| result)
    }
    fn transact_inner<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut Transaction<'_>) -> Result<T>,
        pin: bool,
        maintenance: bool,
        check: Option<&dyn Fn() -> Result<()>>,
    ) -> Result<(T, Option<Snapshot>)> {
        self.ensure_durable()?;
        let authority = self.authority.read().clone();
        let (_permission, mut writer, view) = loop {
            if let Some(check) = check {
                check()?;
            }
            let permission = authority.as_ref().map(|a| a.write()).transpose()?;
            if self.closed.load(Ordering::Acquire) {
                return Err(Error::new("UNAVAILABLE", "tenant closed"));
            }
            let waiting = Instant::now();
            let writer = self.writer.lock();
            if let Some(check) = check {
                check()?;
            }
            self.metrics
                .write_lock_wait_ns
                .fetch_add(waiting.elapsed().as_nanos() as u64, Ordering::Relaxed);
            if let Some(error) = &writer.poison {
                return Err(error.clone());
            }
            let view = self.view.read().clone();
            let backlog = if l0_pressure(&view.tables) >= self.options.l0_stop {
                Some("L0 backlog: retry after compaction")
            } else if view.mem.bytes.load(Ordering::Acquire) > self.options.memtable_bytes * 4 {
                Some("memtable backlog: retry after flush")
            } else {
                None
            };
            let Some(backlog) = backlog else {
                break (permission, writer, view);
            };
            if check.is_none() {
                return Err(Error::limit(backlog));
            }
            // A durable index build already exists. Wait before invoking its
            // FnOnce transaction instead of failing halfway through publication.
            // Neither the writer lock nor the node fence is held while waiting.
            drop(writer);
            drop(permission);
            self.wait_progress(self.seq(), Duration::from_millis(10));
        };
        let seq = writer
            .staged
            .checked_add(1)
            .ok_or_else(|| Error::limit("commit sequence exhausted"))?;
        let mut tx = Transaction {
            engine: self,
            view: &view,
            read_seq: writer.staged,
            seq,
            changes: BTreeMap::new(),
            memory: Vec::new(),
        };
        let result = f(&mut tx)?;
        if tx.changes.is_empty() {
            // Idempotent retries can observe a preceding staged transaction.
            // Its receipt must not escape before the original WAL is durable.
            let observed = writer.staged;
            let snapshot = if pin {
                Some(self.pin(view.clone(), observed, self.options.index_build_seconds)?)
            } else {
                None
            };
            while self.seq() < observed && writer.poison.is_none() {
                self.durable.wait(&mut writer);
            }
            if let Some(error) = &writer.poison {
                return Err(error.clone());
            }
            return Ok((result, snapshot));
        }
        let records: Vec<Record> = tx.changes.into_values().collect();
        let memory = self.cache.reserve(
            &self.budget,
            records.iter().map(Record::size).sum::<usize>() * 2,
        )?;
        let bytes = wal::encode(seq, &records)?;
        let snapshot = if pin {
            Some(self.pin(view.clone(), seq, self.options.index_build_seconds)?)
        } else {
            None
        };
        let reserved_disk = bytes.len() as u64 + 4096;
        self.reserve_disk(reserved_disk, maintenance)?;
        let wal_bytes = bytes.len() as u64;
        if let Err(error) = writer.wal.append(bytes, &self.io, self.info.id) {
            if error.code == "COMMIT_UNKNOWN" {
                self.poison_writer(&mut writer, error.clone());
                self.notify_durable();
            } else {
                self.disk.fetch_sub(reserved_disk, Ordering::AcqRel);
            }
            return Err(error);
        }
        self.record_staged_commit(&mut writer, seq, &records, wal_bytes);
        view.mem.apply(records, memory);
        writer.staged = seq;
        while self.seq() < seq && writer.poison.is_none() {
            if writer.syncing {
                self.durable.wait(&mut writer);
                continue;
            }
            writer.syncing = true;
            drop(writer);
            std::thread::sleep(Duration::from_millis(1));
            writer = self.writer.lock();
            let through = writer.staged;
            let catalog_through = writer.catalog_staged;
            let file = writer.wal.file.clone();
            drop(writer);
            let synced = self.io.run(self.info.id, move || Ok(file.sync_all()?));
            super::super::failpoint("wal_synced");
            writer = self.writer.lock();
            writer.syncing = false;
            if let Err(error) = synced {
                self.poison_writer(&mut writer, Error::new("COMMIT_UNKNOWN", error.to_string()));
            } else if writer.poison.is_none() {
                // Appends may have arrived during fsync. Only the prefix captured
                // before it began is acknowledged; later appends need another sync.
                self.metrics.fsyncs.fetch_add(1, Ordering::Relaxed);
                self.catalog_seq.store(catalog_through, Ordering::Release);
                self.committed.store(through, Ordering::Release);
            }
            self.notify_durable();
        }
        if let Some(error) = &writer.poison {
            return Err(error.clone());
        }
        Ok((result, snapshot))
    }
    pub(super) fn record_staged_commit(
        &self,
        writer: &mut Writer,
        seq: u64,
        records: &[Record],
        wal_bytes: u64,
    ) {
        self.metrics
            .wal_bytes
            .fetch_add(wal_bytes, Ordering::Relaxed);
        self.metrics.document_bytes.fetch_add(
            records
                .iter()
                .filter(|r| r.key.first() == Some(&b'D'))
                .filter_map(|r| r.value.as_ref())
                .map(|v| v.len() as u64)
                .sum(),
            Ordering::Relaxed,
        );
        self.metrics.commits.fetch_add(1, Ordering::Relaxed);
        if records
            .iter()
            .any(|r| matches!(r.key.first(), Some(b'C' | b'J' | b'L')))
        {
            writer.catalog_staged = seq;
        }
    }
    pub(super) fn sync_writer(
        &self,
        writer: &mut parking_lot::MutexGuard<'_, Writer>,
    ) -> Result<()> {
        while writer.syncing && writer.poison.is_none() {
            self.durable.wait(writer);
        }
        if let Some(error) = &writer.poison {
            return Err(error.clone());
        }
        if let Err(error) = writer.wal.sync(&self.io, self.info.id) {
            let error = Error::new("COMMIT_UNKNOWN", error.to_string());
            self.poison_writer(&mut *writer, error.clone());
            writer.syncing = false;
            self.notify_durable();
            return Err(error);
        }
        self.metrics.fsyncs.fetch_add(1, Ordering::Relaxed);
        self.catalog_seq
            .store(writer.catalog_staged, Ordering::Release);
        self.committed.store(writer.staged, Ordering::Release);
        writer.syncing = false;
        self.notify_durable();
        Ok(())
    }
    pub(super) fn reserve_disk(&self, bytes: u64, maintenance: bool) -> Result<()> {
        let limit = if maintenance {
            self.info.disk_bytes
        } else {
            self.info.disk_bytes - self.info.disk_bytes / 5
        };
        self.disk
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(bytes).filter(|v| *v <= limit)
            })
            .map_err(|_| Error::limit("tenant disk quota/headroom exhausted"))?;
        Ok(())
    }
}
pub struct Transaction<'a> {
    pub(super) engine: &'a Engine,
    pub(super) view: &'a View,
    pub(super) read_seq: u64,
    pub seq: u64,
    pub(super) changes: BTreeMap<Vec<u8>, Record>,
    pub(super) memory: Vec<Reservation>,
}
impl Transaction<'_> {
    pub(crate) fn check_expanded_limits(&self) -> Result<()> {
        let bytes = 28usize.saturating_add(
            self.changes
                .values()
                .map(|r| 12 + r.key.len() + r.value.as_ref().map_or(0, |v| v.len()))
                .sum::<usize>(),
        );
        if self.changes.len() > self.engine.options.max_batch_operations
            || bytes > self.engine.options.max_batch_bytes
        {
            return Err(Error::limit(
                "expanded atomic batch exceeds configured limit",
            ));
        }
        Ok(())
    }

    pub fn prefix_first(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Arc<Bytes>)>> {
        self.prefix_first_after(prefix, None, limit)
    }
    pub fn prefix_first_after(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Arc<Bytes>)>> {
        let mut start = after.map_or_else(
            || prefix.to_vec(),
            |key| {
                let mut next = key.to_vec();
                next.push(0);
                next
            },
        );
        if start.as_slice() < prefix {
            start = prefix.to_vec();
        }
        let mut stats = ReadStats::default();
        let mut scan = Scan::new(
            self.view,
            self.read_seq,
            start.clone(),
            codec::prefix_end(prefix),
            None,
            None,
            &mut stats,
        )?;
        let mut records = BTreeMap::new();
        while records.len() < limit.saturating_add(self.changes.len()) {
            let Some(record) = scan.next(&mut stats)? else {
                break;
            };
            records.insert(record.key, record.value.unwrap());
        }
        for (key, record) in self.changes.range(start..) {
            if !key.starts_with(prefix) {
                break;
            }
            if let Some(value) = &record.value {
                records.insert(key.clone(), value.clone());
            } else {
                records.remove(key);
            }
        }
        Ok(records.into_iter().take(limit).collect())
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<Arc<Bytes>>> {
        if let Some(record) = self.changes.get(key) {
            return Ok(record.value.clone());
        }
        Ok(
            get_one(self.view, key, self.read_seq, &mut ReadStats::default())?
                .and_then(|r| r.value),
        )
    }
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.put_with_prefix(key, value, 0)
    }
    pub fn put_with_prefix(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        prefix_len: usize,
    ) -> Result<()> {
        if key.len() > codec::MAX_KEY || prefix_len > key.len() {
            return Err(Error::limit("encoded key exceeds limit"));
        }
        self.memory.push(
            self.engine
                .budget
                .reserve(key.len() * 2 + value.len() * 2 + 256)?,
        );
        self.changes.insert(
            key.clone(),
            Record {
                key,
                seq: self.seq,
                value: Some(Arc::new(Bytes::new(value))),
                prefix_len,
            },
        );
        Ok(())
    }
    pub fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.delete_with_prefix(key, 0)
    }
    pub fn delete_with_prefix(&mut self, key: Vec<u8>, prefix_len: usize) -> Result<()> {
        if key.len() > codec::MAX_KEY || prefix_len > key.len() {
            return Err(Error::limit("encoded key exceeds limit"));
        }
        self.memory
            .push(self.engine.budget.reserve(key.len() * 2 + 256)?);
        self.changes.insert(
            key.clone(),
            Record {
                key,
                seq: self.seq,
                value: None,
                prefix_len,
            },
        );
        Ok(())
    }
    pub fn prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Arc<Bytes>)>> {
        let mut stats = ReadStats::default();
        let mut scan = Scan::new(
            self.view,
            self.read_seq,
            prefix.to_vec(),
            codec::prefix_end(prefix),
            None,
            None,
            &mut stats,
        )?;
        let mut records = BTreeMap::new();
        while let Some(record) = scan.next(&mut stats)? {
            if records.len() >= limit {
                return Err(Error::limit("metadata scan limit"));
            }
            records.insert(record.key, record.value.unwrap());
        }
        for (key, record) in self.changes.range(prefix.to_vec()..) {
            if !key.starts_with(prefix) {
                break;
            }
            if let Some(value) = &record.value {
                records.insert(key.clone(), value.clone());
            } else {
                records.remove(key);
            }
        }
        if records.len() > limit {
            return Err(Error::limit("metadata count limit"));
        }
        let _ = self.engine;
        Ok(records.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hot_retries_do_not_keep_an_uncertain_engine_alive() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(dir.path(), Options::default()).unwrap();
        let id = store.create_tenant("automatic-recovery").unwrap().id;
        let tenant = store.tenant(id).unwrap();
        let old_runtime = tenant.engine().runtime_id;
        let touched = *tenant.core.last_used.lock();
        tenant.engine().poison_writer(
            &mut tenant.engine().writer.lock(),
            Error::new("COMMIT_UNKNOWN", "injected failed sync"),
        );
        for _ in 0..10 {
            assert_eq!(store.tenant(id).err().unwrap().code, "COMMIT_UNKNOWN");
            assert_eq!(*tenant.core.last_used.lock(), touched);
        }
        drop(tenant);
        let deadline = Instant::now() + Duration::from_secs(3);
        while store.status(id).unwrap()["open"] == true {
            assert!(
                Instant::now() < deadline,
                "poisoned engine waited for idle timeout"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let recovered = store.tenant(id).unwrap();
        assert_ne!(recovered.engine().runtime_id, old_runtime);
        recovered.create_collection("healthy").unwrap();
    }

    #[test]
    fn uncertain_wal_tail_cannot_be_fenced_or_cached_as_the_old_position() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(dir.path(), Options::default()).unwrap();
        let cluster = Uuid::new_v4();
        store
            .configure_replication(&crate::replication::Config {
                cluster,
                role: crate::replication::Role::Primary,
                peer_url: "http://127.0.0.1:1".into(),
                token_file: dir.path().join("unused"),
                poll_ms: 10,
                log_retention_bytes: 1 << 20,
            })
            .unwrap();
        let tenant = store
            .tenant(store.create_tenant("uncertain-tail").unwrap().id)
            .unwrap();
        let id = tenant.info().id;
        let engine = tenant.engine();
        let committed = engine.seq();
        let seq = committed + 1;
        let records = vec![Record {
            key: b"Zunknown".to_vec(),
            seq,
            value: Some(Arc::new(Bytes::new(b"complete-frame".to_vec()))),
            prefix_len: 0,
        }];
        let mut writer = engine.writer.lock();
        writer
            .wal
            .append(wal::encode(seq, &records).unwrap(), &engine.io, id)
            .unwrap();
        writer.staged = seq;
        writer.poison = Some(Error::new(
            "COMMIT_UNKNOWN",
            "simulated fsync failure after complete append",
        ));
        drop(writer);
        assert_eq!(
            store.fence_primary(Uuid::new_v4()).unwrap_err().code,
            "COMMIT_UNKNOWN"
        );
        drop(tenant);
        store.close_tenant(id).unwrap();
        let state = store.replication_state();
        let page = store
            .export_logs(&crate::replication::LogRequest {
                protocol: crate::replication::PROTOCOL,
                cluster,
                epoch: state.epoch,
                tenant: id,
                generation: store.tenants()[0].generation,
                after: committed,
                cursor: None,
            })
            .unwrap();
        assert_eq!(
            page.through, seq,
            "cold fast path hid the recoverable frame"
        );
        let fence = store.fence_primary(Uuid::new_v4()).unwrap();
        assert_eq!(fence.tenants[0].seq, seq);
    }

    #[test]
    fn index_admission_waits_for_compaction_before_running_the_transaction_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(
            dir.path(),
            Options {
                l0_trigger: 2,
                l0_stop: 4,
                ..Default::default()
            },
        )
        .unwrap();
        let tenant = store
            .tenant(store.create_tenant("index-backpressure").unwrap().id)
            .unwrap();
        let engine = tenant.engine().clone();
        drop(tenant);
        drop(store);
        // Retain the managed engine after stopping automatic maintenance so the
        // backlog and its release are deterministic, without timing a large merge.
        for n in 0..4 {
            engine
                .transact(|tx| tx.put(b"Zhot".to_vec(), vec![n; 128]))
                .unwrap();
            engine.flush().unwrap();
        }
        assert_eq!(engine.table_counts()[&0], 4);
        let ran = AtomicBool::new(false);
        let denied = engine.transact_waiting(
            |_| {
                ran.store(true, Ordering::Release);
                Ok(())
            },
            &|| Ok(()),
            Some(Duration::from_millis(20)),
        );
        assert_eq!(denied.unwrap_err().code, "DEADLINE_EXCEEDED");
        assert!(!ran.load(Ordering::Acquire));
        let snapshot = engine.snapshot().unwrap();
        let (checked_tx, checked_rx) = std::sync::mpsc::channel();
        let (ran_tx, ran_rx) = std::sync::mpsc::channel();
        let waiting = engine.clone();
        let task = std::thread::spawn(move || {
            waiting.transact_waiting(
                |tx| {
                    ran_tx.send(()).unwrap();
                    tx.put(b"Znew".to_vec(), b"committed".to_vec())
                },
                &|| {
                    let _ = checked_tx.send(());
                    snapshot.check()
                },
                None,
            )
        });
        checked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let premature = ran_rx.recv_timeout(Duration::from_millis(30));
        engine.compact(true).unwrap();
        task.join().unwrap().unwrap();
        assert!(premature.is_err());
        ran_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(ran_rx.try_recv().is_err());
        assert!(
            engine
                .snapshot()
                .unwrap()
                .get(b"Znew", &mut ReadStats::default())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn active_replication_leases_survive_long_preparation_and_transfer_but_not_idle_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(dir.path(), Options::default()).unwrap();
        let tenant = store
            .tenant(store.create_tenant("long-transfer").unwrap().id)
            .unwrap();
        let engine = tenant.engine();
        let lease = engine.retain_replication_log();
        assert!(lease.valid());
        let start = Instant::now() + Duration::from_secs(7200);
        assert!(
            lease.renew_at(0, start),
            "two-hour preparation expired its pin"
        );
        for step in 1..=60 {
            assert!(lease.renew_at(step, start + Duration::from_secs(step * 240)));
        }
        let after_idle = start + Duration::from_secs(60 * 240 + 301);
        assert!(!lease.renew_at(61, after_idle));
        assert!(
            !lease.renew_at(61, after_idle),
            "expired pin was resurrected"
        );
    }

    #[test]
    fn consistency_deadline_does_not_wait_for_the_writer_mutex() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(dir.path(), Options::default()).unwrap();
        let tenant = store
            .tenant(store.create_tenant("wait-deadline").unwrap().id)
            .unwrap();
        let history = tenant.history();
        let target = tenant.engine().seq() + 1;
        let engine = tenant.engine().clone();
        let writer = engine.writer.lock();
        let (tx, rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let result = tenant
                .read_with(&crate::ReadOptions {
                    timeout_ms: Some(20),
                    ..Default::default()
                })
                .unwrap()
                .wait_context(&history, target);
            tx.send(result).unwrap();
        });
        let completed = rx.recv_timeout(Duration::from_millis(250));
        drop(writer);
        thread.join().unwrap();
        assert_eq!(
            completed
                .expect("read deadline waited for a replica fsync")
                .unwrap_err()
                .code,
            "DEADLINE_EXCEEDED"
        );
    }
    #[test]
    fn saturated_read_queue_cannot_poison_another_tenants_writer() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(
            dir.path(),
            Options {
                io_workers: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let t = store
            .tenant(store.create_tenant("writer").unwrap().id)
            .unwrap();
        let engine = t.engine().clone();
        let _maintenance = engine.maintenance.lock();
        let reader = Uuid::new_v4();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = engine
            .io
            .submit(reader, move || {
                entered_tx.send(()).unwrap();
                let _ = release_rx.recv();
                Ok(())
            })
            .unwrap();
        entered_rx.recv().unwrap();
        let reads: Vec<_> = (0..4096)
            .map(|_| engine.io.submit_read(reader, None, || Ok(())).unwrap())
            .collect();
        assert!(engine.io.submit_read(reader, None, || Ok(())).is_err());
        let writer = engine.clone();
        let task = std::thread::spawn(move || {
            writer.transact(|tx| tx.put(b"confirmed".to_vec(), b"value".to_vec()))
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while engine.io.stats(engine.info.id)["queued_jobs"] == 0
            && !task.is_finished()
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        drop(reads);
        release_tx.send(()).unwrap();
        blocker.wait().unwrap();
        task.join().unwrap().unwrap();
        engine
            .transact(|tx| tx.put(b"next".to_vec(), b"value".to_vec()))
            .unwrap();
        assert!(engine.writable());
        assert!(
            engine
                .snapshot()
                .unwrap()
                .get(b"confirmed", &mut ReadStats::default())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn snapshot_admission_failure_does_not_leak_disk_reservations() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(
            dir.path(),
            Options {
                max_snapshots: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let t = store
            .tenant(store.create_tenant("snapshot-quota").unwrap().id)
            .unwrap();
        let engine = t.engine();
        let _maintenance = engine.maintenance.lock();
        let snapshot = engine.snapshot().unwrap();
        let before = engine.disk_bytes();
        for _ in 0..3 {
            assert!(
                engine
                    .transact_snapshot(|tx| tx.put(b"key".to_vec(), vec![42; 1024]))
                    .is_err()
            );
            assert_eq!(engine.disk_bytes(), before);
        }
        drop(snapshot);
        engine
            .transact_snapshot(|tx| tx.put(b"key".to_vec(), vec![42; 1024]))
            .unwrap();
    }
    #[test]
    fn cleanup_can_use_reserved_disk_headroom_when_user_writes_are_backpressured() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(dir.path(), Options::default()).unwrap();
        let tenant = store
            .tenant(store.create_tenant("gc-headroom").unwrap().id)
            .unwrap();
        let engine = tenant.engine();
        engine
            .transact(|tx| {
                tx.put(b"expired".to_vec(), b"receipt".to_vec())?;
                tx.put(b"current".to_vec(), b"business-state".to_vec())
            })
            .unwrap();
        let _maintenance = engine.maintenance.lock();
        let actual = engine.disk.load(Ordering::Acquire);
        engine.disk.store(
            engine.info.disk_bytes - engine.info.disk_bytes / 5,
            Ordering::Release,
        );
        assert_eq!(
            engine
                .transact(|tx| tx.put(b"new".to_vec(), b"value".to_vec()))
                .unwrap_err()
                .code,
            "RESOURCE_EXHAUSTED"
        );
        engine
            .transact_maintenance(|tx| tx.delete(b"expired".to_vec()))
            .unwrap();
        let snapshot = engine.snapshot().unwrap();
        assert!(
            snapshot
                .get(b"expired", &mut ReadStats::default())
                .unwrap()
                .is_none()
        );
        assert!(
            snapshot
                .get(b"current", &mut ReadStats::default())
                .unwrap()
                .is_some()
        );
        engine.disk.store(actual, Ordering::Release);
        let orphan = engine.path.join("sst/unpublished.sst");
        fs::write(&orphan, b"unfinished output").unwrap();
        let writer = engine.writer.lock();
        let limit = engine.budget.tenant.limit();
        engine
            .budget
            .tenant
            .set_limit(engine.budget.tenant.used())
            .unwrap();
        cleanup_unreferenced(&engine.path, &writer.manifest, &engine.budget).unwrap();
        engine.budget.tenant.set_limit(limit).unwrap();
        assert!(!orphan.exists());
        assert!(
            engine
                .path
                .join("wal")
                .join(format!("{}.wal", writer.wal.info.id))
                .exists()
        );
    }

    #[test]
    fn read_only_transaction_waits_for_the_observed_group_commit() {
        for pin in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let store = crate::Store::open(dir.path(), Options::default()).unwrap();
            let tenant = store
                .tenant(store.create_tenant("receipt").unwrap().id)
                .unwrap();
            let engine = tenant.engine().clone();
            // Reproduce the valid interval after WAL append + staging and before
            // the group leader wakes to fsync. The reader must not acknowledge it.
            let mut writer = engine.writer.lock();
            let seq = writer.staged + 1;
            let records = vec![Record {
                key: b"receipt".to_vec(),
                seq,
                value: Some(Arc::new(Bytes::new(b"committed".to_vec()))),
                prefix_len: 0,
            }];
            writer
                .wal
                .append(
                    wal::encode(seq, &records).unwrap(),
                    &engine.io,
                    engine.info.id,
                )
                .unwrap();
            engine
                .view
                .read()
                .mem
                .apply(records, engine.budget.reserve(4096).unwrap());
            writer.staged = seq;
            writer.syncing = true;
            drop(writer);
            let (entered, ready) = std::sync::mpsc::channel();
            let (completed, result) = std::sync::mpsc::channel();
            let reader = engine.clone();
            let handle = std::thread::spawn(move || {
                let value = reader
                    .transact_inner(
                        |tx| {
                            let value = tx.get(b"receipt")?.unwrap();
                            entered.send(()).unwrap();
                            Ok(value)
                        },
                        pin,
                        false,
                        None,
                    )
                    .unwrap();
                completed.send(value.0.to_vec()).unwrap();
            });
            ready.recv_timeout(Duration::from_secs(5)).unwrap();
            let premature = result.recv_timeout(Duration::from_millis(30));
            let mut writer = engine.writer.lock();
            writer.syncing = false;
            engine.sync_writer(&mut writer).unwrap();
            drop(writer);
            assert!(
                premature.is_err(),
                "read-only transaction acknowledged staged data before fsync"
            );
            assert_eq!(
                result.recv_timeout(Duration::from_secs(5)).unwrap(),
                b"committed"
            );
            handle.join().unwrap();
            assert_eq!(engine.seq(), seq);
        }
    }
}
