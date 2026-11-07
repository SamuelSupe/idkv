use super::*;
use crate::replication::{
    Config, Fence, LogPage, LogRequest, NodeState, Role, Source, TenantPosition,
};

impl Store {
    pub fn replication_state(&self) -> NodeState {
        self.owner.inner.node.state.read().clone()
    }

    /// Configuration initializes the durable role once. Restarting with an old
    /// config never reverses a promotion or clears a persisted fence.
    pub fn configure_replication(&self, config: &Config) -> Result<NodeState> {
        let node = &self.owner.inner.node;
        let _barrier = node.barrier.write();
        let mut state = self.replication_state();
        if config.role == Role::Fenced
            || !(10..=60_000).contains(&config.poll_ms)
            || !(1 << 20..=1u64 << 40).contains(&config.log_retention_bytes)
        {
            return Err(Error::invalid("replication configuration bounds"));
        }
        if state.enabled {
            if state.cluster != config.cluster {
                return Err(Error::new(
                    "REPLICATION_CONFLICT",
                    "cluster identity changed",
                ));
            }
            if state.log_retention_bytes != config.log_retention_bytes {
                state.log_retention_bytes = config.log_retention_bytes;
                node.save(state.clone())?;
            }
            return Ok(state);
        }
        if config.role == Role::Replica && !self.owner.inner.registry.read().tenants.is_empty() {
            return Err(Error::new(
                "CONFLICT",
                "initialize replica in an empty Store",
            ));
        }
        state.enabled = true;
        // Older Store implementations must not ignore a persisted replica/fence
        // and open this root as a writable standalone database.
        {
            let mut registry = self.owner.inner.registry.write();
            let mut next = registry.clone();
            next.version = 4;
            next.replication_enabled = true;
            atomic_json(&self.root().join("control/tenants.json"), &next)?;
            *registry = next;
        }
        state.cluster = config.cluster;
        state.role = config.role;
        state.log_retention_bytes = config.log_retention_bytes;
        node.save(state.clone())?;
        Ok(state)
    }

    pub(crate) fn require_local_restore(&self) -> Result<()> {
        if self.replication_state().enabled {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "restore/import requires a new unconfigured Store, then replica reinitialization",
            ));
        }
        Ok(())
    }

    pub fn replication_positions(&self) -> Result<Vec<TenantPosition>> {
        if !self.pending_lifecycle_tenants().is_empty() {
            return Err(Error::new(
                "UNAVAILABLE",
                "tenant lifecycle recovery is pending; restart Store before role changes",
            ));
        }
        self.tenants()
            .into_iter()
            .map(|info| {
                let seq = self.replication_position(info.id)?;
                Ok(TenantPosition { info, seq })
            })
            .collect()
    }
    fn cached_replication_positions(&self) -> Result<Vec<TenantPosition>> {
        if !self.pending_lifecycle_tenants().is_empty() {
            return Err(Error::new(
                "UNAVAILABLE",
                "tenant lifecycle recovery is pending",
            ));
        }
        self.tenants()
            .into_iter()
            .map(|info| {
                let slot = self.slot(info.id);
                let mut guard = slot
                    .try_lock()
                    .ok_or_else(|| Error::new("BUSY", "tenant opening; retry role change"))?;
                guard.check_failure()?;
                let seq = match &guard.core {
                    Some(core) => core.engine.try_replication_position()?,
                    None => guard.last_seq.ok_or_else(|| {
                        Error::new("BUSY", "tenant catalog changed; retry role change")
                    })?,
                };
                Ok(TenantPosition { info, seq })
            })
            .collect()
    }

    pub(crate) fn pending_lifecycle_tenants(&self) -> Vec<TenantInfo> {
        self.owner
            .inner
            .registry
            .read()
            .tenants
            .values()
            .filter(|entry| !matches!(entry.lifecycle, Lifecycle::Active | Lifecycle::Deleted))
            .map(|entry| entry.info.clone())
            .collect()
    }

    pub(crate) fn replication_position(&self, id: Uuid) -> Result<u64> {
        let slot = self.slot(id);
        {
            let mut guard = slot.lock();
            guard.check_failure()?;
            if let Some(core) = &guard.core {
                return core.engine.replication_position();
            }
            if let Some(seq) = guard.last_seq {
                return Ok(seq);
            }
        }
        let (handle, opened) = self.tenant_with_open_marker(id)?;
        let seq = handle.engine().replication_position()?;
        let runtime = handle.engine().runtime_id;
        slot.lock().last_seq = Some(seq);
        drop(handle);
        // Reading positions must not retain every cold engine (and its open
        // descriptors) for a node-wide fence or status request.
        if let Some(touched) = opened {
            let _ = self.close_unmodified_tenant(id, runtime, touched);
        }
        Ok(seq)
    }

    /// Durably stop all business writes, including existing embedded handles.
    /// This is restart-safe and idempotent for the same intended successor.
    pub fn fence_primary(&self, target: Uuid) -> Result<Fence> {
        self.replication_positions()?;
        let node = &self.owner.inner.node;
        let _barrier = node.barrier.write();
        let mut state = self.replication_state();
        if !state.enabled || target == state.node {
            return Err(Error::invalid("configured distinct successor required"));
        }
        if let Some(fence) = &state.fence {
            if fence.target == target {
                return Ok(fence.clone());
            }
            return Err(Error::new(
                "CONFLICT",
                "already fenced for another successor",
            ));
        }
        if state.role != Role::Primary {
            return Err(Error::new("READ_ONLY", "only a primary can be fenced"));
        }
        let fence = Fence {
            id: Uuid::new_v4(),
            cluster: state.cluster,
            source: Source {
                node: state.node,
                epoch: state.epoch,
            },
            target,
            tenants: self.cached_replication_positions()?,
        };
        state.role = Role::Fenced;
        state.fence = Some(fence.clone());
        node.save(state)?;
        crate::storage::failpoint("replication_fenced");
        Ok(fence)
    }

    pub(crate) fn bind_source(&self, upstream: &NodeState) -> Result<Source> {
        crate::replication::compatible(upstream.protocol)?;
        let node = &self.owner.inner.node;
        let _barrier = node.barrier.write();
        let mut state = self.replication_state();
        if !state.enabled
            || state.role != Role::Replica
            || state.cluster != upstream.cluster
            || upstream.node == state.node
            || !matches!(upstream.role, Role::Primary | Role::Fenced)
        {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "incompatible source identity or role",
            ));
        }
        let source = Source {
            node: upstream.node,
            epoch: upstream.epoch,
        };
        if state.source.as_ref().is_some_and(|s| *s != source) {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "source changed; explicit follow is required",
            ));
        }
        if state.source.is_none() {
            state.epoch = source.epoch;
            state.source = Some(source.clone());
            node.save(state)?;
        }
        Ok(source)
    }

    /// Promote only after the caller has authenticated the old primary's durable
    /// fence. Network callers use Service::promote, which fetches that proof itself.
    pub(crate) fn promote_verified(&self, fence: &Fence) -> Result<NodeState> {
        self.replication_positions()?;
        let node = &self.owner.inner.node;
        let _barrier = node.barrier.write();
        let mut state = self.replication_state();
        if state.role == Role::Primary && state.parent.as_ref().is_some_and(|p| p.id == fence.id) {
            return Ok(state);
        }
        if state.role != Role::Replica
            || state.node != fence.target
            || state.cluster != fence.cluster
            || state.source.as_ref() != Some(&fence.source)
        {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "fence does not authorize this successor",
            ));
        }
        self.check_positions(&fence.tenants)?;
        state.role = Role::Primary;
        state.epoch = Uuid::new_v4();
        state.source = None;
        state.parent = Some(fence.clone());
        state.forced = false;
        node.save(state.clone())?;
        crate::storage::failpoint("replication_promoted");
        Ok(state)
    }

    fn check_positions(&self, expected: &[TenantPosition]) -> Result<()> {
        let actual = self.cached_replication_positions()?;
        if actual.len() != expected.len()
            || actual.iter().zip(expected).any(|(a, b)| {
                a.info.id != b.info.id
                    || a.info.generation != b.info.generation
                    || a.info.history != b.info.history
                    || a.seq != b.seq
                    || a.info.name != b.info.name
                    || a.info.memory_bytes != b.info.memory_bytes
                    || a.info.disk_bytes != b.info.disk_bytes
            })
        {
            return Err(Error::new(
                "REPLICA_LAG",
                "replica has not applied the complete fenced tenant catalog and positions",
            ));
        }
        Ok(())
    }

    pub(crate) fn follow_verified(&self, upstream: &NodeState) -> Result<NodeState> {
        self.replication_positions()?;
        let node = &self.owner.inner.node;
        let _barrier = node.barrier.write();
        let mut state = self.replication_state();
        let source = Source {
            node: upstream.node,
            epoch: upstream.epoch,
        };
        if state.role == Role::Replica && state.source.as_ref() == Some(&source) {
            return Ok(state);
        }
        let fence = state
            .fence
            .as_ref()
            .ok_or_else(|| Error::new("CONFLICT", "old primary must be durably fenced"))?;
        if upstream.role != Role::Primary
            || upstream.cluster != state.cluster
            || upstream.node != fence.target
            || upstream.parent.as_ref().is_none_or(|p| p.id != fence.id)
            || upstream.forced
        {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "successor is not the authorized planned promotion",
            ));
        }
        self.check_positions(&fence.tenants)?;
        state.role = Role::Replica;
        state.epoch = source.epoch;
        state.source = Some(source);
        state.fence = None;
        state.parent = None;
        node.save(state.clone())?;
        Ok(state)
    }

    pub(crate) fn force_promote(
        &self,
        expected: &Source,
        old_primary_fenced: bool,
        accept_data_loss: bool,
    ) -> Result<NodeState> {
        let node = &self.owner.inner.node;
        let _barrier = node.barrier.write();
        let mut state = self.replication_state();
        if !old_primary_fenced
            || !accept_data_loss
            || state.role != Role::Replica
            || state.source.as_ref() != Some(expected)
        {
            return Err(Error::new(
                "CONFLICT",
                "force promotion requires matching source, external fencing and explicit acceptance of data loss",
            ));
        }
        state.role = Role::Primary;
        state.source = None;
        state.epoch = Uuid::new_v4();
        state.forced = true;
        state.parent = None;
        node.save(state.clone())?;
        Ok(state)
    }

    pub(crate) fn export_logs(&self, request: &LogRequest) -> Result<LogPage> {
        crate::replication::compatible(request.protocol)?;
        let node = &self.owner.inner.node;
        let _barrier = node.barrier.read();
        let state = self.replication_state();
        if !state.enabled
            || state.cluster != request.cluster
            || state.epoch != request.epoch
            || !matches!(state.role, Role::Primary | Role::Fenced)
        {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "source role or history changed",
            ));
        }
        let registration = self
            .owner
            .inner
            .registry
            .read()
            .tenants
            .get(&request.tenant)
            .cloned()
            .ok_or_else(|| Error::missing("tenant"))?;
        if !matches!(registration.lifecycle, Lifecycle::Active)
            || registration.info.generation != request.generation
        {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "tenant identity or lifecycle changed",
            ));
        }
        let slot = self.slot(request.tenant);
        {
            let mut guard = slot.lock();
            guard.check_failure()?;
            if guard.core.is_none() && guard.last_seq == Some(request.after) {
                return Ok(LogPage {
                    epoch: request.epoch,
                    through: request.after,
                    source_position: request.after,
                    frames: vec![],
                    cursor: request.cursor.clone(),
                });
            }
        }
        let (handle, opened) = self.tenant_with_open_marker(request.tenant)?;
        let runtime = handle.engine().runtime_id;
        if opened.is_some() {
            handle.core.exported_cold.store(true, Ordering::Release);
        }
        let close = handle.core.exported_cold.load(Ordering::Acquire);
        let touched = *handle.core.last_used.lock();
        let page = handle.engine().log_page(request);
        drop(handle);
        if close
            && page
                .as_ref()
                .map_or(true, |page| page.through == page.source_position)
        {
            let _ = self.close_unmodified_tenant(request.tenant, runtime, touched);
        }
        page
    }

    pub(crate) fn install_replica(
        &self,
        source: &Source,
        path: &Path,
        info: TenantInfo,
    ) -> Result<()> {
        if self.contains_tenant(info.id) {
            self.restore_checkpoint_inner(info.id, path, source)
        } else {
            let _permission = self.owner.inner.node.replica_write(source)?;
            self.import_checkpoint_inner(path, info)
        }
    }

    pub(crate) fn check_replica_install(&self, source: &Source, id: Uuid) -> Result<()> {
        let _permission = self.owner.inner.node.replica_write(source)?;
        if !self.contains_tenant(id) {
            return Ok(());
        }
        let slot = self.slot(id);
        let mut slot = slot.lock();
        if slot.restoring {
            return Err(Error::new("BUSY", "tenant restore preparation is active"));
        }
        self.close_slot(&mut slot)
    }

    pub(crate) fn replica_catalog(
        &self,
        source: &Source,
        expected: &[TenantPosition],
    ) -> Result<HashMap<Uuid, Error>> {
        let _permission = self.owner.inner.node.replica_write(source)?;
        if expected.len() > 16_384 {
            return Err(Error::limit("replication tenant limit"));
        }
        let unique: HashSet<_> = expected.iter().map(|p| p.info.id).collect();
        if unique.len() != expected.len() {
            return Err(Error::corrupt("duplicate replicated tenant"));
        }
        drop(_permission);
        let mut blocked = HashMap::new();
        for info in self.tenants() {
            if !unique.contains(&info.id)
                && let Err(error) = self
                    .owner
                    .inner
                    .node
                    .replica_write(source)
                    .and_then(|permission| self.delete_tenant_inner(info.id, permission))
            {
                blocked.insert(info.id, error);
            }
        }
        for entry in expected {
            let info = &entry.info;
            if !self.contains_tenant(info.id) {
                continue;
            }
            if let Err(error) = self.update_replica_quota(source, info) {
                blocked.insert(info.id, error);
            }
        }
        Ok(blocked)
    }

    fn update_replica_quota(&self, source: &Source, info: &TenantInfo) -> Result<()> {
        let _permission = self.owner.inner.node.replica_write(source)?;
        if !self
            .options()
            .supports_quota(info.memory_bytes, info.disk_bytes)
        {
            return Err(Error::limit(
                "replicated quota is incompatible with this node",
            ));
        }
        let slot = self.slot(info.id);
        let slot_guard = slot.lock();
        let registry = self.owner.inner.registry.read();
        let current = registry.tenants.get(&info.id).unwrap();
        if current.info.generation != info.generation
            || current.info.history != info.history
            || current.info.name != info.name
            || !matches!(current.lifecycle, Lifecycle::Active)
        {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "replicated tenant identity changed",
            ));
        }
        if current.info.memory_bytes == info.memory_bytes
            && current.info.disk_bytes == info.disk_bytes
        {
            return Ok(());
        }
        drop(registry);
        drop(slot_guard);
        drop(_permission);
        self.change_quota(info.id, info, Some(source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replica_quota_updates_wait_until_restore_preparation_finishes() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let primary = Store::open(a.path(), Options::default()).unwrap();
        let replica = Store::open(b.path(), Options::default()).unwrap();
        let cluster = Uuid::new_v4();
        for (store, role) in [(&primary, Role::Primary), (&replica, Role::Replica)] {
            store
                .configure_replication(&Config {
                    cluster,
                    role,
                    peer_url: "http://127.0.0.1:1".into(),
                    token_file: a.path().join("unused"),
                    poll_ms: 10,
                    log_retention_bytes: 1 << 20,
                })
                .unwrap();
        }
        let source = replica.bind_source(&primary.replication_state()).unwrap();
        let mut info = primary.create_tenant("quota-race").unwrap();
        let checkpoint = a.path().join("checkpoint");
        primary
            .tenant(info.id)
            .unwrap()
            .engine()
            .checkpoint(&checkpoint)
            .unwrap();
        replica
            .install_replica(&source, &checkpoint, info.clone())
            .unwrap();
        let installation = replica.begin_replica_install(&source, info.id).unwrap();
        primary
            .tenant(info.id)
            .unwrap()
            .engine()
            .checkpoint(&checkpoint)
            .unwrap();
        replica
            .install_replica(&source, &checkpoint, info.clone())
            .unwrap();
        assert!(replica.slot(info.id).lock().installing.is_none());
        assert!(
            replica.tenant(info.id).is_ok(),
            "position was published before reads reopened"
        );
        drop(installation);
        let old_disk = info.disk_bytes;
        info.disk_bytes *= 2;
        replica.slot(info.id).lock().restoring = true;
        assert_eq!(
            replica
                .update_replica_quota(&source, &info)
                .unwrap_err()
                .code,
            "BUSY"
        );
        assert_eq!(replica.tenants()[0].disk_bytes, old_disk);
        replica.slot(info.id).lock().restoring = false;
        replica.update_replica_quota(&source, &info).unwrap();
        assert_eq!(replica.tenants()[0].disk_bytes, info.disk_bytes);
    }
}
