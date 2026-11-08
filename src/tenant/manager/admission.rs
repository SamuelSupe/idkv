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
    revoke_readers: bool,
    timeout: Duration,
}
impl ReplicaInstall {
    pub(crate) fn expire_readers(&mut self) -> Result<()> {
        if self.started.elapsed() >= self.timeout {
            return Err(Error::new(
                "DEADLINE_EXCEEDED",
                "tenant reader drain deadline",
            ));
        }
        if self.revoke_readers
            && !self.revoked
            && self.started.elapsed() >= Duration::from_secs(1).min(self.timeout / 2)
        {
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
            timeout: Duration::from_secs(10),
            revoke_readers: true,
        })
    }
    pub(super) fn drain_for_management(&self, id: Uuid, restoring: bool) -> Result<ReplicaInstall> {
        self.backup_context(id)?;
        let slot = self.slot(id);
        let gate = Uuid::new_v4();
        {
            let mut guard = slot.lock();
            if guard.installing.is_some() || (guard.restoring && !restoring) {
                return Err(Error::new("BUSY", "tenant offline preparation is active"));
            }
            guard.installing = Some(gate);
            if let Some(core) = &guard.core {
                core.gate.close();
                core.engine.set_draining(true);
            }
        }
        let mut drain = ReplicaInstall {
            slot: Some(slot.clone()),
            id: gate,
            started: Instant::now(),
            revoked: false,
            timeout: Duration::from_secs(1),
            revoke_readers: false,
        };
        loop {
            drain.expire_readers().map_err(|_| {
                Error::new("BUSY", "tenant handles did not drain within one second")
            })?;
            match self.close_slot(&mut slot.lock()) {
                Ok(()) => return Ok(drain),
                Err(error) if error.code == "BUSY" => thread::sleep(Duration::from_millis(5)),
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn management_interrupts_index_backpressure_and_preserves_recoverable_builds() {
        use serde_json::json;
        for operation in ["close", "quota", "delete"] {
            let dir = tempfile::tempdir().unwrap();
            let options = Options {
                memtable_bytes: 32 << 10,
                l0_trigger: 2,
                l0_stop: 4,
                ..Default::default()
            };
            let store = Store::open(dir.path(), options.clone()).unwrap();
            store.owner.stop.store(true, Ordering::Release);
            for worker in store.owner.workers.lock().drain(..) {
                worker.join().unwrap();
            }
            let info = store.create_tenant(operation).unwrap();
            let tenant = store.tenant(info.id).unwrap();
            tenant.create_collection("docs").unwrap();
            let payload = "x".repeat(1024);
            let keys: Vec<_> = (0..2048).map(|n| format!("{n:04}")).collect();
            let mut acknowledged = Vec::new();
            for batch in keys.chunks(256) {
                acknowledged.extend(
                    tenant
                        .write_batch(
                            "docs",
                            batch
                                .iter()
                                .map(|key| WriteOp::Put {
                                    document: json!({"_key":key,"value":key.parse::<u64>().unwrap(),"payload":payload}),
                                    if_rev: None,
                                })
                                .collect(),
                        )
                        .unwrap(),
                );
                tenant.engine().flush().unwrap();
                tenant.engine().compact(true).unwrap();
            }
            // No maintenance is running: a real build reaches L0 backpressure
            // and stays in its write-admission loop until interrupted.
            let build = thread::spawn(move || {
                tenant.create_index(
                    "docs",
                    IndexSpec {
                        name: "long".into(),
                        fields: vec![IndexField {
                            field: "/value".into(),
                            order: Order::Asc,
                        }],
                        unique: true,
                        include: vec!["/payload".into()],
                    },
                )
            });
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let slot = store.slot(info.id);
                let slot = slot.lock();
                let core = slot.core.as_ref().unwrap();
                if core.engine.table_counts().get(&0).copied().unwrap_or(0) >= 4 {
                    assert!(!core.builds.lock().is_empty());
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "build did not reach backpressure"
                );
                drop(slot);
                thread::sleep(Duration::from_millis(1));
            }
            let managed = match operation {
                "close" => store.close_tenant(info.id),
                "quota" => store
                    .set_quota(info.id, info.memory_bytes, info.disk_bytes)
                    .map(|_| ()),
                _ => store.delete_tenant(info.id),
            };
            let interrupted = build.join().unwrap();
            managed.unwrap();
            assert_eq!(interrupted.unwrap_err().code, "BUSY");
            if operation == "delete" {
                assert!(store.tenant(info.id).is_err());
                continue;
            }
            {
                let tenant = store.tenant(info.id).unwrap();
                let definition = tenant.indexes("docs").unwrap().remove(0);
                assert_eq!(definition.state, IndexState::Building);
                assert!(definition.error.is_none());
            }
            drop(store);
            let recovered = Store::open(dir.path(), options).unwrap();
            let tenant = recovered.tenant(info.id).unwrap();
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                let definition = tenant.indexes("docs").unwrap().remove(0);
                if definition.state == IndexState::Ready {
                    assert_eq!(definition.entries, keys.len() as u64);
                    break;
                }
                assert_eq!(definition.state, IndexState::Building);
                assert!(
                    Instant::now() < deadline,
                    "interrupted index did not recover"
                );
                thread::sleep(Duration::from_millis(10));
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match tenant.create_lookup(
                    "docs",
                    LookupDefinition {
                        name: "by-value".into(),
                        index: "long".into(),
                        select: vec!["/_key".into(), "/_rev".into(), "/payload".into()],
                        mode: MatchMode::All,
                        max_matches: 1,
                    },
                ) {
                    Ok(()) => break,
                    Err(error)
                        if error.code == "RESOURCE_EXHAUSTED" && Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(10))
                    }
                    Err(error) => panic!("lookup admission after recovered build: {error}"),
                }
            }
            for (batch, acks) in keys.chunks(128).zip(acknowledged.chunks(128)) {
                let docs = tenant.multi_get("docs", batch).unwrap();
                let inputs: Vec<_> = batch
                    .iter()
                    .map(|key| json!({"value":key.parse::<u64>().unwrap()}))
                    .collect();
                let indexed = tenant.lookup_many("docs", "by-value", &inputs).unwrap();
                for (((key, ack), doc), matches) in
                    batch.iter().zip(acks).zip(docs).zip(indexed.matches)
                {
                    let doc = doc.unwrap();
                    assert_eq!(doc["_key"], *key);
                    assert_eq!(doc["_rev"], ack.rev);
                    assert_eq!(doc["payload"], payload);
                    assert_eq!(matches.len(), 1);
                    assert_eq!(matches[0]["_key"], *key);
                    assert_eq!(matches[0]["_rev"], ack.rev);
                    assert_eq!(matches[0]["payload"], payload);
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn full_cold_admission_pool_does_not_reject_global_lock_contention() {
        use crate::http::{self, AuthConfig, Role, Token};
        use http_body_util::BodyExt;
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(
            dir.path(),
            Options {
                max_http_admission_requests: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let cold = store.create_tenant("stuck-cold").unwrap().id;
        let other = store.create_tenant("another-cold").unwrap().id;
        let warm = store
            .tenant(store.create_tenant("warm-contention").unwrap().id)
            .unwrap();
        warm.create_collection("docs").unwrap();
        let ack = warm
            .put("docs", serde_json::json!({"_key":"ack","value":7}), None)
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
                    sha256: http::token_hash("contention-secret"),
                    tenant: None,
                    role: Role::Admin,
                }],
            },
        )
        .unwrap();
        let request = |uri: String| {
            axum::http::Request::builder()
                .uri(uri)
                .header("authorization", "Bearer contention-secret")
                .body(axum::body::Body::empty())
                .unwrap()
        };
        let slot = store.slot(cold);
        let held = slot.clone();
        let (entered, ready) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let holder = thread::spawn(move || {
            let _guard = held.lock();
            entered.send(()).unwrap();
            let _ = wait.recv_timeout(Duration::from_secs(10));
        });
        ready.recv().unwrap();
        let references = Arc::strong_count(&slot);
        let blocked = tokio::spawn(app.clone().oneshot(request(format!("/v1/tenants/{cold}"))));
        let deadline = Instant::now() + Duration::from_secs(3);
        while Arc::strong_count(&slot) < references + 1 {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        blocked.abort();
        let _ = blocked.await;
        let denied = app
            .clone()
            .oneshot(request(format!("/v1/tenants/{other}")))
            .await
            .unwrap();
        let mut responses = Vec::new();
        for (registry, expire) in [(true, false), (false, false), (true, true)] {
            let inner = store.owner.inner.clone();
            let (entered, ready) = std::sync::mpsc::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let held = thread::spawn(move || {
                if registry {
                    let _guard = inner.registry.write();
                    entered.send(()).unwrap();
                    let _ = wait.recv_timeout(Duration::from_secs(5));
                } else {
                    let _guard = inner.slots.lock();
                    entered.send(()).unwrap();
                    let _ = wait.recv_timeout(Duration::from_secs(5));
                }
            });
            ready.recv().unwrap();
            let mut request = request(format!(
                "/v1/tenants/{}/collections/docs/documents/ack",
                warm.id()
            ));
            if expire {
                request.headers_mut().insert(
                    "x-tkv-read-timeout-ms",
                    axum::http::HeaderValue::from_static("20"),
                );
            }
            let mut response = Box::pin(app.clone().oneshot(request));
            let first = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(match std::future::Future::poll(response.as_mut(), cx) {
                    std::task::Poll::Ready(value) => Some(value),
                    std::task::Poll::Pending => None,
                })
            })
            .await;
            let first = if expire && first.is_none() {
                Some(
                    tokio::time::timeout(Duration::from_secs(1), response.as_mut())
                        .await
                        .unwrap(),
                )
            } else {
                first
            };
            release.send(()).unwrap();
            held.join().unwrap();
            let response = match first {
                Some(response) => response,
                None => tokio::time::timeout(Duration::from_secs(1), response)
                    .await
                    .unwrap(),
            };
            responses.push((expire, response.unwrap()));
        }
        release.send(()).unwrap();
        holder.join().unwrap();
        assert_eq!(denied.status(), axum::http::StatusCode::TOO_MANY_REQUESTS);
        for (expire, response) in responses {
            let success = response.status().is_success();
            let body: serde_json::Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            if expire {
                assert_eq!(body["error"]["code"], "DEADLINE_EXCEEDED");
            } else {
                assert!(success);
                assert_eq!(body["_rev"], ack.rev);
            }
        }
    }

    #[test]
    fn management_drain_keeps_a_writable_index_build_resumable() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(
            dir.path(),
            Options {
                io_workers: 1,
                ..Default::default()
            },
        )
        .unwrap();
        store.owner.stop.store(true, Ordering::Release);
        for worker in store.owner.workers.lock().drain(..) {
            worker.join().unwrap();
        }
        let id = store.create_tenant("writable-index-drain").unwrap().id;
        let tenant = store.tenant(id).unwrap();
        tenant.create_collection("docs").unwrap();
        let ack = tenant
            .put("docs", serde_json::json!({"_key":"ack","value":7}), None)
            .unwrap();
        let io = store.owner.inner.io.clone();
        let (entered, ready) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let blocked = io
            .submit(id, move || {
                entered.send(()).unwrap();
                let _ = wait.recv_timeout(Duration::from_secs(5));
                Ok(())
            })
            .unwrap();
        ready.recv().unwrap();
        let build = thread::spawn(move || {
            tenant.create_index(
                "docs",
                IndexSpec {
                    name: "interrupted".into(),
                    fields: vec![IndexField {
                        field: "/value".into(),
                        order: Order::Asc,
                    }],
                    unique: false,
                    include: vec![],
                },
            )
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        while io.stats(id)["queued_jobs"].as_u64().unwrap() == 0 {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        let closing = store.clone();
        let close = thread::spawn(move || closing.close_tenant(id));
        while store.slot(id).lock().installing.is_none() {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        // Let BUILDING become durable after draining starts. There is no L0
        // backlog to accidentally prevent an incorrect FAILED publication.
        release.send(()).unwrap();
        blocked.wait().unwrap();
        assert_eq!(build.join().unwrap().unwrap_err().code, "BUSY");
        close.join().unwrap().unwrap();
        let tenant = store.tenant(id).unwrap();
        let definition = tenant.indexes("docs").unwrap().remove(0);
        assert_eq!(definition.state, IndexState::Building);
        assert!(definition.error.is_none());
        assert_eq!(tenant.get("docs", "ack").unwrap().unwrap()["_rev"], ack.rev);
    }

    #[test]
    fn failed_management_drain_preserves_snapshots_and_does_not_pin_node_authority() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let info = store.create_tenant("quota-drain").unwrap();
        let tenant = store.tenant(info.id).unwrap();
        tenant.create_collection("docs").unwrap();
        let ack = tenant
            .put(
                "docs",
                serde_json::json!({"_key":"confirmed","value":1}),
                None,
            )
            .unwrap();
        let snapshot = tenant.engine().snapshot().unwrap();
        store.owner.stop.store(true, Ordering::Release);
        for worker in store.owner.workers.lock().drain(..) {
            worker.join().unwrap();
        }
        let changing = store.clone();
        let quota =
            thread::spawn(move || changing.set_quota(info.id, info.memory_bytes, info.disk_bytes));
        let deadline = Instant::now() + Duration::from_secs(1);
        while store.slot(info.id).lock().installing.is_none() {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        let authority = store
            .owner
            .inner
            .node
            .barrier
            .try_write_for(Duration::from_millis(50));
        assert!(authority.is_some(), "management drain held node authority");
        drop(authority);
        assert_eq!(quota.join().unwrap().unwrap_err().code, "BUSY");
        snapshot.check().unwrap();
        assert_eq!(
            tenant.get("docs", "confirmed").unwrap().unwrap()["_rev"],
            ack.rev
        );
    }

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
    #[test]
    fn failed_management_drain_resumes_index_without_reopening() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(
            dir.path(),
            Options {
                memtable_bytes: 2 << 20,
                ..Default::default()
            },
        )
        .unwrap();
        let id = store.create_tenant("automatic-failed-drain").unwrap().id;
        let tenant = store.tenant(id).unwrap();
        tenant.create_collection("docs").unwrap();
        let payload = "b".repeat(1024);
        let keys: Vec<_> = (0..16384).map(|n| format!("{n:05}")).collect();
        let mut acknowledged = Vec::new();
        for batch in keys.chunks(256) {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let operations = batch.iter().map(|key| WriteOp::Put {
                    document: json!({"_key":key,"value":key.parse::<u64>().unwrap(),"payload":payload}), if_rev: None
                }).collect();
                match tenant.write_batch("docs", operations) {
                    Ok(acks) => {
                        acknowledged.extend(acks);
                        break;
                    }
                    Err(error) if error.code == "RESOURCE_EXHAUSTED" => {
                        assert!(Instant::now() < deadline);
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("{error}"),
                }
            }
        }
        tenant.engine().flush().unwrap();
        tenant.engine().compact(true).unwrap();
        let building = tenant.clone();
        let build = thread::spawn(move || {
            building.create_index(
                "docs",
                IndexSpec {
                    name: "automatic-long".into(),
                    fields: ["/value", "/a", "/b", "/c", "/d", "/e", "/f", "/g"]
                        .iter()
                        .map(|field| IndexField {
                            field: (*field).into(),
                            order: Order::Asc,
                        })
                        .collect(),
                    unique: false,
                    include: vec!["/payload".into()],
                },
            )
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let cached = tenant.core.recovery_catalog.load(Ordering::Acquire);
            let catalog = tenant.engine().catalog_seq();
            let active = !tenant.core.builds.lock().is_empty();
            let building = tenant
                .indexes("docs")
                .unwrap()
                .iter()
                .any(|i| i.state == IndexState::Building);
            if cached == catalog && active && building {
                break;
            }
            assert!(
                !build.is_finished() && Instant::now() < deadline,
                "probe precondition: background never observed active build"
            );
            thread::sleep(Duration::from_millis(1));
        }
        let management = store.close_tenant(id).unwrap_err();
        assert_eq!(management.code, "BUSY");
        assert_eq!(build.join().unwrap().unwrap_err().code, "BUSY");
        let observing = Instant::now();
        let mut inspections = 0;
        while observing.elapsed() < Duration::from_secs(5) {
            inspections += 1;
            if tenant.indexes("docs").unwrap()[0].state == IndexState::Ready {
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        let stranded = tenant.indexes("docs").unwrap().remove(0);
        assert_eq!(stranded.state, IndexState::Ready);
        assert_eq!(stranded.entries, 16384);
        let observed_ms = observing.elapsed().as_millis();
        let mut checked = 0;
        for (batch, acks) in keys.chunks(128).zip(acknowledged.chunks(128)) {
            for (doc, ack) in tenant
                .multi_get("docs", batch)
                .unwrap()
                .into_iter()
                .zip(acks)
            {
                let doc = doc.unwrap();
                assert_eq!(doc["_rev"], ack.rev);
                assert_eq!(doc["payload"], payload);
                checked += 1;
            }
        }
        println!(
            "INDEX_RECOVERY_JSON {}",
            json!({"automatic_workers":true,"observed_for_ms":observed_ms,"inspections":inspections,"state":stranded.state,"acknowledged_documents_checked":checked,"acknowledged_mismatches":0})
        );
    }
}
