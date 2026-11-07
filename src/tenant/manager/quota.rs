use super::*;

struct QuotaGate {
    slot: Arc<Mutex<Slot>>,
    id: Uuid,
}
impl Drop for QuotaGate {
    fn drop(&mut self) {
        let mut slot = self.slot.lock();
        if slot.installing == Some(self.id) {
            slot.installing = None;
        }
    }
}
impl Store {
    /// Validate a smaller budget before publishing it. Validation failure keeps
    /// the old quota; uncertain publication requires reopening Store.
    pub fn set_quota(&self, id: Uuid, memory_bytes: usize, disk_bytes: u64) -> Result<TenantInfo> {
        let permission = self.owner.inner.node.write()?;
        if !self.options().supports_quota(memory_bytes, disk_bytes) {
            return Err(Error::invalid("quota bounds"));
        }
        let mut info = self
            .owner
            .inner
            .registry
            .read()
            .tenants
            .get(&id)
            .ok_or_else(|| Error::missing("tenant"))?
            .info
            .clone();
        info.memory_bytes = memory_bytes;
        info.disk_bytes = disk_bytes;
        drop(permission);
        self.change_quota(id, &info, None)?;
        Ok(info)
    }

    pub(super) fn change_quota(
        &self,
        id: Uuid,
        info: &TenantInfo,
        source: Option<&crate::replication::Source>,
    ) -> Result<()> {
        let inner = &self.owner.inner;
        let permission = match source {
            Some(source) => inner.node.replica_write(source)?,
            None => inner.node.write()?,
        };
        drop(permission);
        let _drain = self.drain_for_management(id, false)?;
        let permission = match source {
            Some(source) => inner.node.replica_write(source)?,
            None => inner.node.write()?,
        };
        let slot = self.slot(id);
        let mut guard = slot.lock();
        let current = inner.registry.read().tenants[&id].clone();
        if !matches!(current.lifecycle, Lifecycle::Active)
            || current.info.generation != info.generation
            || current.info.history != info.history
            || current.info.name != info.name
        {
            return Err(Error::new("CONFLICT", "tenant changed during quota update"));
        }
        self.close_slot(&mut guard)?;
        if info.memory_bytes >= guard.memory.limit() {
            return self.publish_quota(&mut guard, info);
        }
        let memory = guard.memory.clone();
        let gate = Uuid::new_v4();
        guard.installing = Some(gate);
        guard.last_seq = None;
        let _gate = QuotaGate {
            slot: slot.clone(),
            id: gate,
        };
        drop(guard);
        drop(permission);
        let path = inner.root.join("tenants").join(id.to_string());
        if source.is_some() {
            let serving = Engine::open(
                path.clone(),
                current.info,
                inner.options.clone(),
                Budget {
                    global: inner.memory.clone(),
                    tenant: memory.clone(),
                },
                inner.io.clone(),
            )?;
            serving.attach_manager(inner._lock.clone(), inner.node.clone());
            // A replica can retain WAL already checkpointed by its source.
            serving.flush()?;
            drop(serving);
        }
        let available = info
            .memory_bytes
            .checked_sub(memory.used())
            .ok_or_else(|| Error::limit("new quota is below current reservations"))?;
        let check = Engine::open(
            path,
            info.clone(),
            inner.options.clone(),
            Budget {
                global: inner.memory.clone(),
                tenant: Counter::new(available),
            },
            inner.io.clone(),
        )?;
        let position = check.replication_position()?;
        check.verify()?;
        // Storage decoding alone is cheaper than returning a JSON document.
        // A successful shrink must also leave Get's working set available.
        {
            let snapshot = check.snapshot()?;
            let mut stats = crate::ReadStats::default();
            let mut documents = snapshot.scan(vec![b'D'], Some(vec![b'E']), None, &mut stats)?;
            while let Some(record) = documents.next(&mut stats)? {
                if let Some(value) = record.value {
                    let _read = check
                        .budget
                        .reserve(crate::tenant::documents::point_read_memory(value.len()))?;
                }
            }
        }
        drop(check);
        // Reacquire in node-before-slot order. A fence must not wait behind this
        // full storage scan while blocking all other writers.
        let _permission = match source {
            Some(source) => inner.node.replica_write(source)?,
            None => inner.node.write()?,
        };
        let mut guard = slot.lock();
        let current = inner.registry.read().tenants[&id].clone();
        if guard.installing != Some(gate)
            || guard.restoring
            || !matches!(current.lifecycle, Lifecycle::Active)
            || current.info.generation != info.generation
            || current.info.history != info.history
            || current.info.name != info.name
        {
            return Err(Error::new(
                "CONFLICT",
                "tenant changed during quota validation",
            ));
        }
        let result = self.publish_quota(&mut guard, info);
        if result.is_ok() {
            guard.last_seq = Some(position);
            guard.installing = None;
        }
        drop(guard);
        result
    }

    fn publish_quota(&self, slot: &mut Slot, info: &TenantInfo) -> Result<()> {
        let inner = &self.owner.inner;
        let _offline = self.offline_lock(info.id)?;
        let mut registry = inner.registry.write();
        let mut next = registry.clone();
        next.tenants.get_mut(&info.id).unwrap().info = info.clone();
        next.prepare_publish()?;
        slot.memory.set_limit(info.memory_bytes)?;
        let result = (|| {
            atomic_json(&inner.root.join("control/tenants.json"), &next)?;
            *registry = next;
            atomic_json(
                &inner
                    .root
                    .join("tenants")
                    .join(info.id.to_string())
                    .join("TENANT.meta"),
                info,
            )
        })();
        if let Err(error) = &result {
            slot.last_seq = None;
            slot.record_failure(Error::new(
                "UNAVAILABLE",
                format!("quota publication uncertain; reopen Store: {error}"),
            ));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_failed_shrink_keeps_existing_data_readable_across_restart() {
        for (payload_kib, flushed) in [(700, false), (700, true), (400, false)] {
            let dir = tempfile::Builder::new()
                .prefix("idkv-quota-audit-")
                .tempdir_in("/var/tmp")
                .unwrap();
            let options = Options {
                global_memory_bytes: 64 << 20,
                tenant_memory_bytes: 8 << 20,
                memtable_bytes: 1 << 20,
                cache_bytes: 0,
                ..Options::default()
            };
            let store = Store::open(dir.path(), options.clone()).unwrap();
            // Keep the WAL/checkpoint selection independent of maintenance timing.
            store.owner.stop.store(true, Ordering::Release);
            for worker in store.owner.workers.lock().drain(..) {
                worker.join().unwrap();
            }
            let id = store.create_tenant("quota-reopen").unwrap().id;
            let source = store.tenant(id).unwrap();
            source.create_collection("docs").unwrap();
            let ack = source
                .put(
                    "docs",
                    serde_json::json!({"_key":"large","payload":"x".repeat(payload_kib<<10)}),
                    None,
                )
                .unwrap();
            let doc = source.get("docs", "large").unwrap().unwrap();
            assert_eq!(doc["_rev"], ack.rev);
            if flushed {
                source.engine().flush().unwrap();
            }
            let before = source.engine().memory_bytes();
            drop(source);
            assert!(before < 2 << 20);
            assert_eq!(
                store.set_quota(id, 2 << 20, 100 << 30).unwrap_err().code,
                "RESOURCE_EXHAUSTED"
            );
            assert_eq!(store.tenants()[0].memory_bytes, 8 << 20);
            assert_eq!(
                Some(doc.clone()),
                store.tenant(id).unwrap().get("docs", "large").unwrap()
            );
            drop(store);
            let store = Store::open(dir.path(), options).unwrap();
            assert_eq!(store.tenants()[0].memory_bytes, 8 << 20);
            let reopened = store.tenant(id).unwrap();
            assert_eq!(Some(doc), reopened.get("docs", "large").unwrap());
        }
    }

    #[test]
    fn replica_quota_shrink_flushes_a_lagging_checkpoint_under_the_old_budget() {
        use crate::replication::{Config, LogRequest, PROTOCOL, Role, TenantPosition};
        let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
        let options = Options {
            global_memory_bytes: 64 << 20,
            tenant_memory_bytes: 8 << 20,
            memtable_bytes: 1 << 20,
            cache_bytes: 0,
            ..Options::default()
        };
        let primary = Store::open(dirs[0].path(), options.clone()).unwrap();
        let replica = Store::open(dirs[1].path(), options).unwrap();
        let cluster = Uuid::new_v4();
        for (store, role) in [(&primary, Role::Primary), (&replica, Role::Replica)] {
            store.owner.stop.store(true, Ordering::Release);
            for worker in store.owner.workers.lock().drain(..) {
                worker.join().unwrap();
            }
            store
                .configure_replication(&Config {
                    cluster,
                    role,
                    peer_url: "http://127.0.0.1:1".into(),
                    token_file: "unused".into(),
                    poll_ms: 10,
                    log_retention_bytes: 64 << 20,
                })
                .unwrap();
        }
        let info = primary.create_tenant("shrinking-replica").unwrap();
        let source = replica.bind_source(&primary.replication_state()).unwrap();
        let tenant = primary.tenant(info.id).unwrap();
        tenant.create_collection("docs").unwrap();
        let path = dirs[0].path().join("checkpoint");
        tenant.engine().checkpoint(&path).unwrap();
        replica
            .install_replica(&source, &path, info.clone())
            .unwrap();
        let mut request = LogRequest {
            protocol: PROTOCOL,
            cluster,
            epoch: source.epoch,
            tenant: info.id,
            generation: info.generation,
            after: 1,
            cursor: None,
        };
        for n in 0..300 {
            tenant
                .put(
                    "docs",
                    serde_json::json!({"_key":format!("d{n}"),"payload":"x".repeat(4096)}),
                    None,
                )
                .unwrap();
        }
        let handle = replica.tenant(info.id).unwrap();
        loop {
            let page = primary.export_logs(&request).unwrap();
            handle
                .engine()
                .apply_replica_page(&source, request.after, &page, 0)
                .unwrap();
            request.after = page.through;
            request.cursor = page.cursor;
            if page.through == page.source_position {
                break;
            }
        }
        assert!(handle.engine().memory_bytes() > 2 << 20);
        let expected = tenant.get("docs", "d299").unwrap();
        tenant.engine().flush().unwrap();
        drop(handle);
        drop(tenant);
        let changed = primary
            .set_quota(info.id, 2 << 20, info.disk_bytes)
            .unwrap();
        let blocked = replica
            .replica_catalog(
                &source,
                &[TenantPosition {
                    info: changed,
                    seq: request.after,
                }],
            )
            .unwrap();
        assert!(blocked.is_empty(), "{blocked:?}");
        assert_eq!(replica.tenants()[0].memory_bytes, 2 << 20);
        assert_eq!(
            replica
                .tenant(info.id)
                .unwrap()
                .get("docs", "d299")
                .unwrap(),
            expected
        );
    }

    #[test]
    fn quota_validation_does_not_hold_the_node_barrier_or_slot_during_io() {
        for replicated in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(
                dir.path(),
                Options {
                    io_workers: 1,
                    tenant_memory_bytes: 16 << 20,
                    memtable_bytes: 1 << 20,
                    cache_bytes: 0,
                    ..Options::default()
                },
            )
            .unwrap();
            if replicated {
                store
                    .configure_replication(&crate::replication::Config {
                        cluster: Uuid::new_v4(),
                        role: crate::replication::Role::Primary,
                        peer_url: "http://127.0.0.1:1".into(),
                        token_file: "unused".into(),
                        poll_ms: 10,
                        log_retention_bytes: 64 << 20,
                    })
                    .unwrap();
            }
            store.owner.stop.store(true, Ordering::Release);
            for worker in store.owner.workers.lock().drain(..) {
                worker.join().unwrap();
            }
            let info = store.create_tenant("quota-gate").unwrap();
            let tenant = store.tenant(info.id).unwrap();
            tenant.create_collection("docs").unwrap();
            tenant
                .put("docs", serde_json::json!({"_key":"one","value":1}), None)
                .unwrap();
            tenant.engine().flush().unwrap();
            drop(tenant);
            let backup = store.backup(info.id).unwrap();

            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = store
                .owner
                .inner
                .io
                .submit(info.id, move || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .unwrap();
            entered_rx.recv().unwrap();
            let changing = store.clone();
            let update =
                thread::spawn(move || changing.set_quota(info.id, 8 << 20, info.disk_bytes));
            let slot = store.slot(info.id);
            let deadline = Instant::now() + Duration::from_secs(2);
            while slot.try_lock().is_none_or(|s| s.installing.is_none()) {
                assert!(Instant::now() < deadline);
                thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(store.tenant(info.id).err().unwrap().code, "BUSY");
            let fencing = store.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            let fence = thread::spawn(move || {
                let result = if replicated {
                    fencing.fence_primary(Uuid::new_v4()).map(|_| ())
                } else {
                    fencing.restore(info.id, backup)
                };
                tx.send(result).unwrap();
            });
            let result = rx.recv_timeout(Duration::from_millis(250));
            release_tx.send(()).unwrap();
            blocker.wait().unwrap();
            let changed = update.join().unwrap();
            fence.join().unwrap();
            assert_eq!(
                result
                    .expect("fence waited for quota I/O")
                    .unwrap_err()
                    .code,
                "BUSY"
            );
            changed.unwrap();
            assert_eq!(
                store.replication_state().role,
                crate::replication::Role::Primary
            );
            assert_eq!(
                store
                    .tenant(info.id)
                    .unwrap()
                    .get("docs", "one")
                    .unwrap()
                    .unwrap()["value"],
                1
            );
        }
    }
}
