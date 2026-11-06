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
    pub(crate) fn expire_readers(&mut self) -> Result<()> {
        if self.started.elapsed() >= Duration::from_secs(10) {
            return Err(Error::new(
                "DEADLINE_EXCEEDED",
                "checkpoint reader drain exceeded ten seconds",
            ));
        }
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
        Ok(())
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
                    core.engine.set_draining(false);
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
                    core.engine.set_draining(true);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn draining_interrupts_consistency_waits_and_timeout_reopens_old_readers() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let id = store.create_tenant("draining-waits").unwrap().id;
        let handle = store.tenant(id).unwrap();
        handle.create_collection("docs").unwrap();
        let source = crate::replication::Source {
            node: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
        };
        {
            let mut state = store.owner.inner.node.state.write();
            state.enabled = true;
            state.role = crate::replication::Role::Replica;
            state.epoch = source.epoch;
            state.source = Some(source.clone());
        }
        let mut waiting = Vec::new();
        let (send, receive) = std::sync::mpsc::channel();
        for context in [true, false] {
            let handle = handle.clone();
            let send = send.clone();
            waiting.push(thread::spawn(move || {
                let error = if context {
                    handle.wait_context(&handle.history(), 999, 5000)
                } else {
                    handle
                        .wait_replication(handle.engine().replication_epoch().unwrap(), 999, 5000)
                        .map(|_| ())
                }
                .unwrap_err();
                send.send(error).unwrap();
            }));
        }
        thread::sleep(Duration::from_millis(50));
        let mut install = store.begin_replica_install(&source, id).unwrap();
        for _ in 0..2 {
            assert_eq!(
                receive
                    .recv_timeout(Duration::from_millis(250))
                    .unwrap()
                    .code,
                "BUSY"
            );
        }
        assert_eq!(handle.collections().unwrap_err().code, "BUSY");
        install.started = Instant::now() - Duration::from_secs(10);
        assert_eq!(
            install.expire_readers().unwrap_err().code,
            "DEADLINE_EXCEEDED"
        );
        drop(install);
        assert_eq!(handle.collections().unwrap().len(), 1);
        assert_eq!(store.tenant(id).unwrap().collections().unwrap().len(), 1);
        for thread in waiting {
            thread.join().unwrap();
        }
    }
}
