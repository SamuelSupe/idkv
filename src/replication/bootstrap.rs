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
                || info.tenant.memory_bytes > self.store().options().global_memory_bytes
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
            if fs2::available_space(&path)? < total.saturating_mul(2).saturating_add(64 << 20) {
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
                let (budget, io) = store.import_context();
                let check = Engine::open(
                    path.clone(),
                    tenant_info.clone(),
                    store.options().clone(),
                    budget,
                    io,
                )?;
                check.verify()?;
                if check.seq() != position {
                    return Err(Error::corrupt("checkpoint position mismatch"));
                }
                drop(check);
                check_cancelled()?;
                store.install_replica(source, &path, tenant_info)?;
            }
            Ok::<_, Error>(())
        }
        .await;
        if result.is_ok() {
            // The snapshot position can be far behind the head after a large
            // transfer. Keep its WAL lease until incremental catch-up completes.
            self.hold_transfer(tenant, info.id);
            let head: Result<super::client::Received<super::client::Status>> =
                self.request("/v1/replication/status", None).await;
            if let Ok(head) = head
                && head.node.epoch == source.epoch
                && head
                    .tenants
                    .iter()
                    .any(|entry| entry.info.id == tenant && entry.seq <= info.position)
            {
                self.finish_transfer(tenant).await?;
            }
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
