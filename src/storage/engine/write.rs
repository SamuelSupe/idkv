use super::*;
impl Engine {
    pub fn transact<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        self.transact_inner(f, false, false)
            .map(|(result, _)| result)
    }
    pub fn transact_snapshot<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut Transaction<'_>) -> Result<T>,
    ) -> Result<(T, Snapshot)> {
        self.transact_inner(f, true, false)
            .map(|(result, snapshot)| (result, snapshot.unwrap()))
    }
    pub(crate) fn transact_maintenance<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        self.transact_inner(f, false, true)
            .map(|(result, _)| result)
    }
    fn transact_inner<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut Transaction<'_>) -> Result<T>,
        pin: bool,
        maintenance: bool,
    ) -> Result<(T, Option<Snapshot>)> {
        let authority = self.authority.read().clone();
        let _permission = authority.as_ref().map(|a| a.write()).transpose()?;
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::new("UNAVAILABLE", "tenant closed"));
        }
        let waiting = Instant::now();
        let mut writer = self.writer.lock();
        self.metrics
            .write_lock_wait_ns
            .fetch_add(waiting.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if let Some(error) = &writer.poison {
            return Err(error.clone());
        }
        let view = self.view.read().clone();
        if l0_pressure(&view.tables) >= self.options.l0_stop {
            return Err(Error::limit("L0 backlog: retry after compaction"));
        }
        if view.mem.bytes.load(Ordering::Acquire) > self.options.memtable_bytes * 4 {
            return Err(Error::limit("memtable backlog: retry after flush"));
        }
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
        let memory = self
            .budget
            .reserve(records.iter().map(Record::size).sum::<usize>() * 2)
            .or_else(|_| {
                self.cache.clear();
                self.budget
                    .reserve(records.iter().map(Record::size).sum::<usize>() * 2)
            })?;
        let bytes = wal::encode(seq, &records)?;
        self.reserve_disk(bytes.len() as u64 + 4096, maintenance)?;
        let snapshot = if pin {
            Some(self.pin(view.clone(), seq, self.options.index_build_seconds)?)
        } else {
            None
        };
        let wal_bytes = bytes.len() as u64;
        if let Err(error) = writer.wal.append(bytes, &self.io, self.info.id) {
            let error = Error::new("COMMIT_UNKNOWN", error.to_string());
            writer.poison = Some(error.clone());
            self.durable.notify_all();
            return Err(error);
        }
        self.record_staged_commit(&mut writer, seq, &records, wal_bytes);
        view.mem.apply(records, memory);
        writer.staged = seq;
        if !writer.syncing {
            writer.syncing = true;
            drop(writer);
            std::thread::sleep(Duration::from_millis(1));
            writer = self.writer.lock();
            self.sync_writer(&mut writer)?;
        } else {
            while self.seq() < seq && writer.poison.is_none() {
                self.durable.wait(&mut writer);
            }
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
    pub(super) fn sync_writer(&self, writer: &mut Writer) -> Result<()> {
        if let Err(error) = writer.wal.sync(&self.io, self.info.id) {
            let error = Error::new("COMMIT_UNKNOWN", error.to_string());
            writer.poison = Some(error.clone());
            writer.syncing = false;
            self.durable.notify_all();
            return Err(error);
        }
        self.metrics.fsyncs.fetch_add(1, Ordering::Relaxed);
        self.catalog_seq
            .store(writer.catalog_staged, Ordering::Release);
        self.committed.store(writer.staged, Ordering::Release);
        writer.syncing = false;
        self.durable.notify_all();
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
                    )
                    .unwrap();
                completed.send(value.0.to_vec()).unwrap();
            });
            ready.recv_timeout(Duration::from_secs(5)).unwrap();
            let premature = result.recv_timeout(Duration::from_millis(30));
            engine.sync_writer(&mut engine.writer.lock()).unwrap();
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
