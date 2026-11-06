use super::*;

impl Slot {
    pub(super) fn check_failure(&mut self) -> Result<()> {
        if self
            .failure_until
            .is_some_and(|until| Instant::now() >= until)
        {
            self.failure = None;
            self.failure_until = None;
        }
        self.failure.clone().map_or(Ok(()), Err)
    }
    pub(super) fn record_failure(&mut self, error: Error) {
        self.failure_until =
            (error.code == "IO_ERROR").then(|| Instant::now() + Duration::from_secs(1));
        self.failure = Some(error);
    }
}

pub(crate) struct ReplicaInstall {
    slot: Option<Arc<Mutex<Slot>>>,
    id: Uuid,
    started: Instant,
    revoked: bool,
}
impl ReplicaInstall {
    pub(crate) fn expire_readers(&mut self) {
        if !self.revoked && self.started.elapsed() >= Duration::from_secs(1) {
            if let Some(slot) = &self.slot {
                let slot = slot.lock();
                if let Some(core) = &slot.core {
                    core.engine.invalidate_snapshots();
                    core.cursors.lock().clear();
                }
            }
            self.revoked = true;
        }
    }
}
impl Drop for ReplicaInstall {
    fn drop(&mut self) {
        if let Some(slot) = &self.slot {
            let mut slot = slot.lock();
            if slot.installing == Some(self.id) {
                slot.installing = None;
                if let Some(core) = &slot.core
                    && !core.engine.recovery_required()
                {
                    core.gate.open();
                }
            }
        }
    }
}
impl Store {
    pub(crate) fn begin_replica_install(
        &self,
        source: &crate::replication::Source,
        id: Uuid,
    ) -> Result<ReplicaInstall> {
        let _permission = self.owner.inner.node.replica_write(source)?;
        let gate = Uuid::new_v4();
        let slot = if self.contains_tenant(id) {
            let slot = self.slot(id);
            {
                let mut guard = slot.lock();
                if guard.installing.is_some() || guard.restoring {
                    return Err(Error::new(
                        "BUSY",
                        "replica checkpoint installation already active",
                    ));
                }
                guard.installing = Some(gate);
                if let Some(core) = &guard.core {
                    core.gate.close();
                }
            }
            Some(slot)
        } else {
            None
        };
        Ok(ReplicaInstall {
            slot,
            id: gate,
            started: Instant::now(),
            revoked: false,
        })
    }
}
