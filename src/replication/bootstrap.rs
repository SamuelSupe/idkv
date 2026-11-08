use super::{Service, Source};
use crate::{
    Error, Result,
    storage::{Engine, sync_dir},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use uuid::Uuid;

impl Service {
    pub(super) async fn initialize(&self, source: &Source, tenant: Uuid) -> Result<()> {
        let service = self.clone();
        let source = source.clone();
        let runtime = tokio::runtime::Handle::current();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelTransfer(cancelled.clone());
        // File creation, hashing, fsync and cleanup all run on this bounded
        // transfer worker. HTTP futures still use the runtime's network driver.
        tokio::task::spawn_blocking(move || {
            runtime.block_on(service.receive_checkpoint(&source, tenant, &cancelled))
        })
        .await
        .map_err(|_| Error::new("INTERNAL", "replica transfer worker failed"))?
    }
    async fn receive_checkpoint(
        &self,
        source: &Source,
        tenant: Uuid,
        cancelled: &AtomicBool,
    ) -> Result<()> {
        let check_cancelled = || {
            if cancelled.load(Ordering::Acquire) {
                Err(Error::new("CANCELLED", "replica transfer stopped"))
            } else {
                Ok(())
            }
        };
        check_cancelled()?;
        // Drain first without holding the gate throughout the network transfer.
        let ready = self.drain_before_download(source, tenant, cancelled).await;
        self.waiting_for_readers(tenant, false);
        ready?;
        let state = self.store().replication_state();
        let info: super::client::Received<super::snapshot::SnapshotInfo> = self
            .request(
                "/v1/replication/snapshot",
                Some(json!({"protocol": super::PROTOCOL, "cluster":state.cluster,"epoch":source.epoch,"tenant":tenant})),
            )
            .await?;
        let result = async {
            if info.cluster != state.cluster
                || info.epoch != source.epoch
                || info.tenant.id != tenant
                || info.files.len() > 20_000
            {
                return Err(Error::new("REPLICATION_CONFLICT", "checkpoint identity"));
            }
            let mut names = HashSet::new();
            let mut total = 0u64;
            for file in &info.files {
                if !super::snapshot::valid_path(&file.path)
                    || !names.insert(file.path.clone())
                    || file.sha256.len() != 64
                {
                    return Err(Error::corrupt("checkpoint file manifest"));
                }
                total = total
                    .checked_add(file.size)
                    .ok_or_else(|| Error::limit("checkpoint size"))?;
            }
            if total > info.tenant.disk_bytes
                || !self
                    .store()
                    .options()
                    .supports_quota(info.tenant.memory_bytes, info.tenant.disk_bytes)
            {
                return Err(Error::limit("checkpoint exceeds replica quota"));
            }
            let path = self
                .store()
                .root()
                .join("replication-work/import")
                .join(info.id.to_string());
            let _cleanup = ImportCleanup(path.clone());
            fs::create_dir(&path)?;
            fs::create_dir(path.join("sst"))?;
            fs::create_dir(path.join("wal"))?;
            let reserve_free = self.store().options().maintenance_reserve_free_bytes;
            if fs2::available_space(&path)? < total.saturating_add(reserve_free) {
                return Err(Error::limit("checkpoint staging free space"));
            }
            for (index, file) in info.files.iter().enumerate() {
                check_cancelled()?;
                let mut output = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(path.join(&file.path))?;
                let mut offset = 0u64;
                let mut digest = Sha256::new();
                while offset < file.size {
                    check_cancelled()?;
                    let chunk: super::client::Received<Value> = self
                        .request(
                            "/v1/replication/snapshot/chunk",
                            Some(json!({"id":info.id,"file":index,"offset":offset})),
                        )
                        .await?;
                    let data = STANDARD
                        .decode(
                            chunk["data"]
                                .as_str()
                                .ok_or_else(|| Error::corrupt("snapshot chunk"))?,
                        )
                        .map_err(|_| Error::corrupt("snapshot chunk encoding"))?;
                    if data.is_empty()
                        || data.len() > super::PAGE_BYTES
                        || data.len() as u64 > file.size - offset
                    {
                        return Err(Error::corrupt("snapshot chunk size"));
                    }
                    if fs2::available_space(&path)? < reserve_free.saturating_add(data.len() as u64)
                    {
                        return Err(Error::limit("checkpoint receive free disk reserve"));
                    }
                    output.write_all(&data)?;
                    digest.update(&data);
                    offset += data.len() as u64;
                }
                if format!("{:x}", digest.finalize()) != file.sha256 {
                    return Err(Error::corrupt("snapshot file checksum"));
                }
                output.sync_all()?;
            }
            sync_dir(&path.join("sst"))?;
            sync_dir(&path.join("wal"))?;
            sync_dir(&path)?;
            let position = info.position;
            let tenant_info = info.tenant.clone();
            {
                let store = self.store();
                let (budget, io) = store.import_context(Some(&tenant_info));
                let check = Engine::open(
                    path.clone(),
                    tenant_info.clone(),
                    store.options().clone(),
                    budget,
                    io,
                )?;
                check.verify()?;
                if check.replication_position()? != position {
                    return Err(Error::corrupt("checkpoint position mismatch"));
                }
                drop(check);
                check_cancelled()?;
                // The verified checkpoint is durable locally. Hand off the source
                // slot now; only its WAL pin is needed while readers drain.
                let mut probed = std::time::Instant::now();
                let probe = || {
                    self.request::<super::LogPage>(
                        "/v1/replication/log",
                        Some(json!({"protocol":super::PROTOCOL,"cluster":state.cluster,
                    "epoch":source.epoch,"tenant":tenant,"generation":info.tenant.generation,
                    "after":position})),
                    )
                };
                probe().await?;
                let mut installation = Some(store.begin_replica_install(source, tenant)?);
                let mut retry_at = std::time::Instant::now();
                let mut drain_attempts = 1;
                loop {
                    check_cancelled()?;
                    if probed.elapsed() >= std::time::Duration::from_secs(30) {
                        probe().await?;
                        probed = std::time::Instant::now();
                    }
                    if installation
                        .as_mut()
                        .is_some_and(|gate| gate.expire_readers().is_err())
                    {
                        // Reopen reads during backoff, retaining the durable copy and
                        // renewing its WAL lease independently from the drain gate.
                        installation.take();
                        if drain_attempts >= 2 {
                            return Err(Error::new(
                                "DEADLINE_EXCEEDED",
                                "tenant reader drain deadline after retained-checkpoint retry",
                            ));
                        }
                        self.reader_drain_timed_out(tenant);
                        retry_at = std::time::Instant::now() + std::time::Duration::from_secs(30);
                    }
                    if installation.is_none() {
                        if std::time::Instant::now() < retry_at {
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            continue;
                        }
                        match store.begin_replica_install(source, tenant) {
                            Ok(gate) => {
                                installation = Some(gate);
                                drain_attempts += 1;
                            }
                            Err(error) if error.code == "BUSY" => {
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                continue;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    let installed = store
                        .check_replica_install(source, tenant)
                        .and_then(|_| store.install_replica(source, &path, tenant_info.clone()));
                    match installed {
                        Ok(()) => {
                            self.waiting_for_readers(tenant, false);
                            break;
                        }
                        Err(error) if error.code == "BUSY" => {
                            self.waiting_for_readers(tenant, true);
                            if probed.elapsed() >= std::time::Duration::from_secs(30) {
                                probe().await?;
                                probed = std::time::Instant::now();
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            Ok::<_, Error>(())
        }
        .await;
        self.waiting_for_readers(tenant, false);
        if result.is_ok() {
            // The source hands off its export slot when it observes our durable
            // position, even if this process later restarts and forgets all IDs.
            let _: Result<super::client::Received<super::LogPage>> = self
                .request(
                    "/v1/replication/log",
                    Some(json!({"protocol":super::PROTOCOL,
                    "cluster":state.cluster,"epoch":source.epoch,"tenant":tenant,
                    "generation":info.tenant.generation,"after":info.position})),
                )
                .await;
        } else {
            let _: Result<super::client::Received<Value>> = self
                .request(
                    "/v1/replication/snapshot/release",
                    Some(json!({"id":info.id})),
                )
                .await;
        }
        result
    }
    async fn drain_before_download(
        &self,
        source: &Source,
        tenant: Uuid,
        cancelled: &AtomicBool,
    ) -> Result<()> {
        let mut installation = self.store().begin_replica_install(source, tenant)?;
        loop {
            if cancelled.load(Ordering::Acquire) {
                return Err(Error::new("CANCELLED", "replica transfer stopped"));
            }
            installation.expire_readers()?;
            match self.store().check_replica_install(source, tenant) {
                Ok(()) => return Ok(()),
                Err(error) if error.code == "BUSY" => {
                    self.waiting_for_readers(tenant, true);
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
}
struct CancelTransfer(Arc<AtomicBool>);
impl Drop for CancelTransfer {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
struct ImportCleanup(PathBuf);
impl Drop for ImportCleanup {
    fn drop(&mut self) {
        if self.0.exists() {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}
