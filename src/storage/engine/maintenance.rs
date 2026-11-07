use super::*;
impl Engine {
    pub fn needs_flush(&self) -> bool {
        let view = self.view.read();
        !view.frozen.is_empty()
            || view.mem.bytes.load(Ordering::Acquire) >= self.options.memtable_bytes
    }
    pub(crate) fn write_buffer_full(&self) -> bool {
        self.view.read().mem.bytes.load(Ordering::Acquire) >= self.options.memtable_bytes * 3
    }
    pub fn maintain(&self) -> Result<()> {
        let started = Instant::now();
        // Small domain-local merges/promotions must not each wait for the next
        // coordinator tick. Bound the burst so another tenant can make progress;
        // a running compaction remains indivisible, as before.
        for _ in 0..4 {
            if self.needs_flush() {
                self.flush_if_needed()?;
            }
            self.compact(false)?;
            if started.elapsed() >= Duration::from_millis(20)
                || self.view.read().tables.l0_pressure < self.options.l0_trigger
            {
                break;
            }
        }
        if let Some(_maintenance) = self.maintenance.try_lock() {
            let scan_due = self.disk_inventory.lock().checked.elapsed() >= Duration::from_secs(5)
                || self.disk.load(Ordering::Acquire) >= self.info.disk_bytes / 5 * 3;
            if scan_due {
                self.reconcile_disk_locked()?;
            } else {
                let other = self.disk_inventory.lock().non_wal_bytes;
                let writer = self.writer.lock();
                self.disk
                    .store(other + writer.wal.file.metadata()?.len(), Ordering::Release);
            }
        }
        Ok(())
    }
    // Caller holds maintenance: the active WAL identity and non-WAL outputs
    // cannot change. Only WAL appends and deletion of obsolete SSTs can race.
    fn reconcile_disk_locked(&self) -> Result<()> {
        let active = self.writer.lock().wal.info.id;
        let active_path = self.path.join("wal").join(format!("{active}.wal"));
        let other_bytes = directory_size_except(&self.path, Some(&active_path))?;
        let writer = self.writer.lock();
        let active_bytes = writer.wal.file.metadata()?.len();
        let bytes = other_bytes
            .checked_add(active_bytes)
            .ok_or_else(|| Error::limit("directory size overflow"))?;
        self.disk.store(bytes, Ordering::Release);
        *self.disk_inventory.lock() = DiskInventory {
            checked: Instant::now(),
            non_wal_bytes: other_bytes,
        };
        Ok(())
    }
    pub fn flush(&self) -> Result<()> {
        self.flush_inner(false)
    }
    pub fn flush_if_needed(&self) -> Result<()> {
        self.flush_inner(true)
    }
    fn flush_inner(&self, only_if_needed: bool) -> Result<()> {
        self.ensure_durable()?;
        let _maintenance = if only_if_needed {
            let Some(guard) = self.maintenance.try_lock() else {
                // Foreground backpressure is preferable to waiting for an entire
                // compaction. The next maintenance pass will flush this buffer.
                return Ok(());
            };
            guard
        } else {
            self.maintenance.lock()
        };
        if only_if_needed && !self.needs_flush() {
            return Ok(());
        }
        let result = self.flush_locked();
        if let Err(error) = &result {
            *self.maintenance_error.lock() = Some(error.to_string());
            let writer = self.writer.lock();
            if writer.poison.is_none() {
                // Unpublished outputs can be removed; the frozen memtable and
                // registered WALs remain authoritative and can be retried.
                cleanup_unreferenced(&self.path, &writer.manifest, &self.budget)?;
                drop(writer);
                self.reconcile_disk_locked()?;
                self.cache.clear();
            }
        }
        result
    }
    fn flush_locked(&self) -> Result<()> {
        let (mem, checkpoint, old_wals) = {
            let mut writer = self.writer.lock();
            if let Some(error) = &writer.poison {
                return Err(error.clone());
            }
            self.sync_writer(&mut writer)?;
            // The writer lock already prevents new memtable writes. Keep the
            // view readable while WAL creation and Manifest publication fsync.
            let view = self.view.read();
            if view.mem.bytes.load(Ordering::Acquire) == 0 && view.frozen.is_empty() {
                return Ok(());
            }
            if let Some(mem) = view.frozen.first().cloned() {
                let checkpoint = mem
                    .data
                    .read()
                    .values()
                    .flat_map(|v| v.iter())
                    .map(|r| r.seq)
                    .max()
                    .unwrap();
                let old_wals = writer
                    .manifest
                    .wals
                    .iter()
                    .filter(|w| w.start_seq <= checkpoint)
                    .cloned()
                    .collect::<Vec<_>>();
                self.reserve_disk(mem.bytes.load(Ordering::Acquire) as u64 * 3 + 65536, true)?;
                (mem, checkpoint, old_wals)
            } else {
                let mem = view.mem.clone();
                let checkpoint = writer.staged;
                self.reserve_disk(mem.bytes.load(Ordering::Acquire) as u64 * 3 + 65536, true)?;
                let new_wal = Wal::create(
                    &self.path.join("wal"),
                    self.info.id,
                    self.info.generation,
                    checkpoint + 1,
                )?;
                crate::storage::errorpoint("wal_rotate_created")?;
                let mut manifest = writer.manifest.clone();
                let old_wals = manifest.wals.clone();
                manifest.wals.push(new_wal.info.clone());
                if let Err(e) = publish(&self.path, &manifest) {
                    self.poison_writer(&mut writer, Error::new("COMMIT_UNKNOWN", e.to_string()));
                    return Err(e);
                }
                writer.manifest = manifest;
                writer.wal = new_wal;
                drop(view);
                let mut view = self.view.write();
                view.mem = MemTable::new();
                view.frozen.push(mem.clone());
                (mem, checkpoint, old_wals)
            }
        };
        let result: Result<()> = (|| {
            let mut outputs = Vec::new();
            let mut builder: Option<TableWriter> = None;
            let mut domain = Vec::new();
            let guard = mem.data.read();
            for versions in guard.values() {
                let next_domain = key_domain(&versions[0].key);
                if builder.as_ref().is_some_and(|b| {
                    b.size() >= self.options.target_sst_bytes as u64 || domain != next_domain
                }) {
                    outputs.push(builder.take().unwrap().finish()?);
                }
                domain = next_domain.to_vec();
                if builder.is_none() {
                    builder = Some(TableWriter::new(
                        &self.path.join("sst"),
                        self.info.id,
                        self.info.generation,
                        0,
                        self.options.block_bytes,
                        self.io.clone(),
                        self.budget.clone(),
                    )?);
                }
                for record in versions.iter().rev() {
                    builder.as_mut().unwrap().add(record.clone())?;
                }
            }
            if let Some(builder) = builder {
                outputs.push(builder.finish()?);
            }
            drop(guard);
            crate::storage::failpoint("sst_synced");
            crate::storage::errorpoint("sst_unpublished")?;
            let mut tables = Vec::new();
            for info in &outputs {
                tables.push(Table::open(
                    &self.path.join("sst"),
                    info.clone(),
                    self.info.id,
                    self.info.generation,
                    self.io.clone(),
                    self.budget.clone(),
                    self.cache.clone(),
                )?);
            }
            let mut all = self.view.read().tables.files.clone();
            all.extend(tables);
            let versions = VersionSet::new(all, &self.budget)?;
            let mut manifest = self.writer.lock().manifest.clone();
            manifest.checkpoint = checkpoint;
            self.metrics
                .flush_bytes
                .fetch_add(outputs.iter().map(|t| t.size).sum(), Ordering::Relaxed);
            manifest.tables.extend(outputs);
            manifest
                .wals
                .retain(|w| !old_wals.iter().any(|old| old.id == w.id));
            let retention = self
                .authority
                .read()
                .as_ref()
                .map_or(0, |node| node.retention.load(Ordering::Acquire));
            let mut obsolete = old_wals;
            if retention != 0 {
                manifest.retained_wals.append(&mut obsolete);
            }
            let mut retained_sizes = BTreeMap::new();
            for wal in std::mem::take(&mut manifest.retained_wals) {
                match fs::metadata(self.path.join("wal").join(format!("{}.wal", wal.id))) {
                    Ok(metadata) => {
                        retained_sizes.insert(wal.id, metadata.len());
                        manifest.retained_wals.push(wal);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        // Only checkpointed history is optional. Invalidate old
                        // exports so LOG_GONE cannot reinstall the same stale copy.
                        self.log_retention.lock().clear();
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            let mut retained_bytes: u64 = retained_sizes.values().sum();
            let mut floor = {
                let mut leases = self.log_retention.lock();
                leases.retain(|_, (_, expires)| {
                    expires.is_none_or(|expires| expires > Instant::now())
                });
                leases.values().map(|(seq, _)| *seq).min()
            };
            while retained_bytes > retention && !manifest.retained_wals.is_empty() {
                let next = manifest
                    .retained_wals
                    .get(1)
                    .or_else(|| manifest.wals.first())
                    .unwrap()
                    .start_seq;
                if floor.is_some_and(|floor| next > floor.saturating_add(1)) {
                    if retained_bytes <= retention.saturating_mul(2) {
                        break;
                    }
                    // A stalled checkpoint/catch-up must not consume the primary's
                    // entire write quota. Handles cannot resurrect invalidated pins.
                    self.log_retention.lock().clear();
                    floor = None;
                }
                let old = manifest.retained_wals.remove(0);
                retained_bytes -= retained_sizes[&old.id];
                obsolete.push(old);
            }
            if let Err(error) = publish(&self.path, &manifest) {
                let mut writer = self.writer.lock();
                self.poison_writer(&mut writer, Error::new("COMMIT_UNKNOWN", error.to_string()));
                self.notify_durable();
                return Err(error);
            }
            let mut writer = self.writer.lock();
            writer.manifest = manifest;
            let mut view = self.view.write();
            view.tables = versions;
            view.frozen.retain(|m| !Arc::ptr_eq(m, &mem));
            drop(view);
            drop(writer);
            for wal in obsolete {
                if let Err(error) =
                    fs::remove_file(self.path.join("wal").join(format!("{}.wal", wal.id)))
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    return Err(error.into());
                }
            }
            sync_dir(&self.path.join("wal"))?;
            self.reconcile_disk_locked()?;
            Ok(())
        })();
        result
    }
    pub fn compact(&self, force: bool) -> Result<()> {
        self.ensure_durable()?;
        let _maintenance = if force {
            self.maintenance.lock()
        } else {
            let Some(guard) = self.maintenance.try_lock() else {
                return Ok(());
            };
            guard
        };
        let (selected, destination, oldest) = {
            let view = self.view.read();
            if !force
                && view.tables.l0_pressure < self.options.l0_trigger
                && !(1..=8usize).any(|level| {
                    view.tables.level_bytes.get(level).copied().unwrap_or(0)
                        > self
                            .options
                            .level_base_bytes
                            .saturating_mul(10u64.saturating_pow(level as u32 - 1))
                })
            {
                return Ok(());
            }
            let l0: Vec<_> = view
                .tables
                .iter()
                .filter(|t| t.info.level == 0)
                .cloned()
                .collect();
            let mut chosen = if force {
                view.tables.files.clone()
            } else if view.tables.l0_pressure >= self.options.l0_trigger {
                let mut chosen = vec![l0.iter().min_by_key(|t| t.info.max_seq).unwrap().clone()];
                loop {
                    let first = chosen.iter().map(|t| &t.info.first).min().unwrap().clone();
                    let last = chosen.iter().map(|t| &t.info.last).max().unwrap().clone();
                    let added: Vec<_> = l0
                        .iter()
                        .filter(|t| {
                            t.info.last >= first
                                && t.info.first <= last
                                && !chosen.iter().any(|c| c.info.id == t.info.id)
                        })
                        .cloned()
                        .collect();
                    if added.is_empty() {
                        break;
                    }
                    chosen.extend(added);
                }
                chosen
            } else {
                vec![]
            };
            let mut destination = if force {
                view.tables
                    .iter()
                    .map(|t| t.info.level)
                    .max()
                    .unwrap_or(0)
                    .max(1)
            } else {
                1
            };
            if !force {
                let mut score = if chosen.is_empty() {
                    1.0
                } else {
                    view.tables.l0_pressure as f64 / self.options.l0_trigger as f64
                };
                for level in 1..=8u32 {
                    let size = view
                        .tables
                        .level_bytes
                        .get(level as usize)
                        .copied()
                        .unwrap_or(0);
                    let limit = self
                        .options
                        .level_base_bytes
                        .saturating_mul(10u64.saturating_pow(level - 1));
                    let pressure = size as f64 / limit as f64;
                    if pressure > score
                        && let Some(table) = view
                            .tables
                            .iter()
                            .filter(|t| t.info.level == level)
                            .min_by_key(|t| t.info.max_seq)
                    {
                        chosen = vec![table.clone()];
                        destination = level + 1;
                        score = pressure;
                    }
                }
            }
            if chosen.is_empty() {
                return Ok(());
            }
            if !force {
                let first = chosen.iter().map(|t| t.info.first.clone()).min().unwrap();
                let last = chosen.iter().map(|t| t.info.last.clone()).max().unwrap();
                chosen.extend(
                    view.tables
                        .iter()
                        .filter(|t| {
                            t.info.level == destination
                                && t.info.last >= first
                                && t.info.first <= last
                        })
                        .cloned(),
                );
            }
            let oldest = self
                .snapshots
                .lock()
                .values()
                .filter_map(Weak::upgrade)
                .filter(|l| l.expires > Instant::now())
                .map(|l| l.seq)
                .min()
                .unwrap_or_else(|| self.seq());
            (chosen, destination, oldest)
        };
        if !force && selected.len() == 1 {
            // A disjoint run can advance a level without rewriting its blocks.
            // Both versions share file lifetime ownership for pinned readers.
            let promoted = selected[0].promoted(destination)?;
            let mut files = self.view.read().tables.files.clone();
            files.retain(|t| t.info.id != promoted.info.id);
            files.push(promoted.clone());
            let versions = VersionSet::new(files, &self.budget)?;
            let mut manifest = self.writer.lock().manifest.clone();
            manifest
                .tables
                .iter_mut()
                .find(|t| t.id == promoted.info.id)
                .unwrap()
                .level = destination;
            self.reserve_disk(crate::json::encoded_len(&manifest)? as u64 + 8 + 4096, true)?;
            if let Err(error) = publish(&self.path, &manifest) {
                let mut writer = self.writer.lock();
                self.poison_writer(&mut writer, Error::new("COMMIT_UNKNOWN", error.to_string()));
                self.notify_durable();
                return Err(error);
            }
            self.writer.lock().manifest = manifest;
            self.view.write().tables = versions;
            self.reconcile_disk_locked()?;
            return Ok(());
        }
        // Keep publication headroom, then charge SST output before each write.
        // A wide L0 range must not require several copies of the target layer.
        const METADATA_RESERVE: u64 = 65536;
        self.reserve_disk(METADATA_RESERVE, true)?;
        let reserve_output = |bytes| self.reserve_disk(bytes, true);
        let result: Result<()> = (|| {
            let mut stats = ReadStats::default();
            let mut cursors = Vec::new();
            let mut heap = BinaryHeap::new();
            for table in &selected {
                cursors.push(table.cursor(None, None, false, None, &mut stats)?);
            }
            for (i, cursor) in cursors.iter_mut().enumerate() {
                if let Some(record) = cursor.next(&mut stats)? {
                    heap.push(HeapRecord { record, source: i });
                }
            }
            let deeper: Vec<_> = self
                .view
                .read()
                .tables
                .iter()
                .filter(|t| !selected.iter().any(|s| s.info.id == t.info.id))
                .cloned()
                .collect();
            let mut previous: Option<Vec<u8>> = None;
            let mut last_internal: Option<(Vec<u8>, u64)> = None;
            let mut kept_base = false;
            let mut outputs = Vec::new();
            let mut builder: Option<TableWriter> = None;
            while let Some(item) = heap.pop() {
                let record = item.record;
                if let Some(next) = cursors[item.source].next(&mut stats)? {
                    heap.push(HeapRecord {
                        record: next,
                        source: item.source,
                    });
                }
                let internal = (record.key.clone(), record.seq);
                if last_internal.as_ref() == Some(&internal) {
                    continue;
                }
                last_internal = Some(internal);
                if previous.as_ref() != Some(&record.key) {
                    if builder.as_ref().is_some_and(|b| {
                        b.size() >= self.options.target_sst_bytes as u64
                            || previous
                                .as_ref()
                                .is_some_and(|old| key_domain(old) != key_domain(&record.key))
                    }) {
                        outputs.push(builder.take().unwrap().finish()?);
                    }
                    previous = Some(record.key.clone());
                    kept_base = false;
                }
                if record.seq <= oldest {
                    if kept_base {
                        continue;
                    }
                    kept_base = true;
                    if record.value.is_none()
                        && !deeper
                            .iter()
                            .any(|t| t.info.first <= record.key && t.info.last >= record.key)
                    {
                        continue;
                    }
                }
                if builder.is_none() {
                    builder = Some(
                        TableWriter::new(
                            &self.path.join("sst"),
                            self.info.id,
                            self.info.generation,
                            destination,
                            self.options.block_bytes,
                            self.io.clone(),
                            self.budget.clone(),
                        )?
                        .with_disk_reservation(&reserve_output)?,
                    );
                }
                builder.as_mut().unwrap().add(record)?;
            }
            if let Some(builder) = builder {
                outputs.push(builder.finish()?);
            }
            crate::storage::failpoint("compaction_synced");
            let mut tables = Vec::new();
            for info in &outputs {
                tables.push(Table::open(
                    &self.path.join("sst"),
                    info.clone(),
                    self.info.id,
                    self.info.generation,
                    self.io.clone(),
                    self.budget.clone(),
                    self.cache.clone(),
                )?);
            }
            let mut all = self.view.read().tables.files.clone();
            all.retain(|t| !selected.iter().any(|s| s.info.id == t.info.id));
            all.extend(tables);
            let versions = VersionSet::new(all, &self.budget)?;
            let mut manifest = self.writer.lock().manifest.clone();
            manifest
                .tables
                .retain(|t| !selected.iter().any(|s| s.info.id == t.id));
            self.metrics.compaction_read_bytes.fetch_add(
                selected.iter().map(|t| t.info.size).sum(),
                Ordering::Relaxed,
            );
            self.metrics
                .compaction_write_bytes
                .fetch_add(outputs.iter().map(|t| t.size).sum(), Ordering::Relaxed);
            manifest.tables.extend(outputs);
            let metadata = crate::json::encoded_len(&manifest)? as u64 + 8 + 4096;
            if metadata > METADATA_RESERVE {
                self.reserve_disk(metadata - METADATA_RESERVE, true)?;
            }
            if let Err(error) = publish(&self.path, &manifest) {
                let mut writer = self.writer.lock();
                self.poison_writer(&mut writer, Error::new("COMMIT_UNKNOWN", error.to_string()));
                self.notify_durable();
                return Err(error);
            }
            self.writer.lock().manifest = manifest;
            let mut view = self.view.write();
            view.tables = versions;
            validate_levels(&view.tables)?;
            drop(view);
            for table in &selected {
                table.mark_obsolete();
            }
            drop(cursors);
            drop(deeper);
            drop(selected);
            self.reconcile_disk_locked()?;
            Ok(())
        })();
        if let Err(error) = &result {
            *self.maintenance_error.lock() = Some(error.to_string());
            let writer = self.writer.lock();
            if writer.poison.is_none() {
                cleanup_unreferenced(&self.path, &writer.manifest, &self.budget)?;
                drop(writer);
                self.reconcile_disk_locked()?;
            }
        }
        result
    }
    pub fn verify(&self) -> Result<()> {
        let view = self.view.read().clone();
        for table in view.tables.iter() {
            table.verify()?;
        }
        Ok(())
    }
    pub fn checkpoint(&self, destination: &Path) -> Result<()> {
        self.checkpoint_limited(destination, u64::MAX, 0)
            .map(|_| ())
    }
    /// Copy a consistent durable checkpoint without holding the maintenance lock
    /// during file I/O. Open descriptors remain readable even after compaction
    /// unlinks an old file; publication always uses the captured Manifest.
    pub fn checkpoint_limited(
        &self,
        destination: &Path,
        max_bytes: u64,
        reserve_free: u64,
    ) -> Result<u64> {
        self.flush()?;
        let maintenance = self.maintenance.lock();
        let writer = self.writer.lock();
        if destination.exists() {
            return Err(Error::invalid("backup destination must not exist"));
        }
        let manifest = writer.manifest.clone();
        let view = self.view.read();
        let files: Vec<_> = view
            .tables
            .iter()
            .map(|t| (t.info.id, t.checkpoint_file()))
            .collect();
        let estimate = manifest.tables.iter().map(|t| t.size).sum::<u64>()
            + serde_json::to_vec(&manifest)?.len() as u64
            + 4096;
        if estimate > max_bytes {
            return Err(Error::limit("checkpoint exceeds backup byte limit"));
        }
        let parent = destination
            .parent()
            .ok_or_else(|| Error::invalid("checkpoint parent"))?;
        if fs2::available_space(parent)? < estimate.saturating_add(reserve_free) {
            return Err(Error::limit("checkpoint free disk reserve"));
        }
        drop(view);
        drop(writer);
        drop(maintenance);
        fs::create_dir(destination)?;
        fs::create_dir(destination.join("sst"))?;
        fs::create_dir(destination.join("wal"))?;
        atomic_json(&destination.join("TENANT.meta"), &self.info)?;
        for (id, file) in files {
            crate::storage::copy_open_file(
                file,
                &destination.join("sst").join(format!("{id}.sst")),
                self.info.id,
                &self.budget,
                &self.io,
            )?;
        }
        let new_wal = Wal::create(
            &destination.join("wal"),
            self.info.id,
            self.info.generation,
            manifest.checkpoint + 1,
        )?;
        let mut backup = manifest;
        backup.wals = vec![new_wal.info];
        backup.retained_wals.clear();
        publish(destination, &backup)?;
        sync_dir(&destination.join("sst"))?;
        sync_dir(destination)?;
        sync_dir(parent)?;
        Ok(backup.checkpoint)
    }
    pub fn metrics(&self) -> serde_json::Value {
        let mut metrics = self.metrics.snapshot();
        metrics["scheduler"] = self.io.stats(self.info.id);
        metrics
    }
    pub fn table_counts(&self) -> BTreeMap<u32, usize> {
        let mut counts = BTreeMap::new();
        for t in self.view.read().tables.iter() {
            *counts.entry(t.info.level).or_default() += 1;
        }
        counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn idle_disk_accounting_tracks_wal_growth_and_checks_drift_near_quota() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(
            dir.path(),
            Options {
                tenant_disk_bytes: 1 << 20,
                ..Options::default()
            },
        )
        .unwrap();
        let id = store.create_tenant("idle-accounting").unwrap().id;
        let engine = store.tenant(id).unwrap().engine().clone();
        drop(store);
        engine
            .transact(|tx| tx.put(b"Zkey".to_vec(), b"value".to_vec()))
            .unwrap();
        let checked = engine.disk_inventory.lock().checked;
        engine.maintain().unwrap();
        assert_eq!(engine.disk_bytes(), directory_size(&engine.path).unwrap());
        assert_eq!(
            engine.disk_inventory.lock().checked,
            checked,
            "idle tick walked the directory"
        );
        let extra = engine.path.join("external-drift");
        fs::write(&extra, vec![0; 800_000]).unwrap();
        engine.disk_inventory.lock().checked = Instant::now() - Duration::from_secs(5);
        engine.maintain().unwrap();
        assert_eq!(engine.disk_bytes(), directory_size(&engine.path).unwrap());
        fs::remove_file(extra).unwrap();
        engine.maintain().unwrap();
        assert_eq!(
            engine.disk_bytes(),
            directory_size(&engine.path).unwrap(),
            "near-quota reconciliation waited five seconds to reclaim space"
        );
    }

    #[test]
    fn missing_checkpointed_wal_does_not_block_flush_or_lose_current_data() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(dir.path(), Options::default()).unwrap();
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
        let id = store.create_tenant("missing-retained-wal").unwrap().id;
        let tenant = store.tenant(id).unwrap();
        let engine = tenant.engine();
        engine
            .transact(|tx| tx.put(b"Zone".to_vec(), b"first".to_vec()))
            .unwrap();
        engine.flush().unwrap();
        let missing = engine.writer.lock().manifest.retained_wals[0].id;
        fs::remove_file(engine.path.join("wal").join(format!("{missing}.wal"))).unwrap();
        engine
            .transact(|tx| tx.put(b"Ztwo".to_vec(), b"second".to_vec()))
            .unwrap();
        engine.flush().unwrap();
        assert!(
            engine
                .writer
                .lock()
                .manifest
                .retained_wals
                .iter()
                .all(|w| w.id != missing)
        );
        drop(tenant);
        store.close_tenant(id).unwrap();
        let tenant = store.tenant(id).unwrap();
        let view = tenant.engine().snapshot().unwrap();
        assert_eq!(
            view.get(b"Zone", &mut ReadStats::default())
                .unwrap()
                .unwrap()
                .as_slice(),
            b"first"
        );
        assert_eq!(
            view.get(b"Ztwo", &mut ReadStats::default())
                .unwrap()
                .unwrap()
                .as_slice(),
            b"second"
        );
    }

    #[test]
    fn disk_reconciliation_accounts_for_concurrent_wal_growth() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(dir.path(), Options::default()).unwrap();
        let tenant = store
            .tenant(store.create_tenant("accounting").unwrap().id)
            .unwrap();
        tenant.create_collection("data").unwrap();
        let engine = tenant.engine().clone();
        let maintenance = engine.maintenance.lock();
        let active_path = engine
            .path
            .join("wal")
            .join(format!("{}.wal", engine.writer.lock().wal.info.id));
        let non_wal = directory_size_except(&engine.path, Some(&active_path)).unwrap();
        let writer_tenant = tenant.clone();
        let writer = std::thread::spawn(move || {
            for n in 0..100 {
                writer_tenant
                    .put(
                        "data",
                        serde_json::json!({"_key":n.to_string(),"payload":"x".repeat(4096)}),
                        None,
                    )
                    .unwrap();
            }
        });
        for _ in 0..100 {
            engine.reconcile_disk_locked().unwrap();
            let guard = engine.writer.lock();
            assert!(
                engine.disk_bytes() >= non_wal + guard.wal.len,
                "concurrent reservations must not be erased"
            );
            drop(guard);
            std::thread::yield_now();
        }
        writer.join().unwrap();
        engine.reconcile_disk_locked().unwrap();
        assert_eq!(engine.disk_bytes(), directory_size(&engine.path).unwrap());
        drop(maintenance);
        tenant.engine().flush().unwrap();
        tenant.engine().compact(true).unwrap();
        tenant.engine().verify().unwrap();
        assert!(tenant.get("data", "99").unwrap().is_some());
    }
}
