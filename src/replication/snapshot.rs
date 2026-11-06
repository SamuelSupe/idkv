use crate::{
    Error, Result, Store,
    model::TenantInfo,
    storage::{reject_symlink, sync_dir},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Read,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub size: u64,
    pub sha256: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub id: Uuid,
    pub cluster: Uuid,
    pub epoch: Uuid,
    pub tenant: TenantInfo,
    pub position: u64,
    pub files: Vec<FileInfo>,
}
pub(crate) struct Exports {
    root: PathBuf,
    current: Option<(SnapshotInfo, Instant, PathBuf)>,
}
impl Exports {
    pub fn open(store: &Store) -> Result<Self> {
        let root = store.root().join("replication-work/export");
        fs::create_dir_all(&root)?;
        reject_symlink(&root)?;
        for item in fs::read_dir(&root)? {
            let item = item?;
            reject_symlink(&item.path())?;
            if Uuid::parse_str(&item.file_name().to_string_lossy()).is_ok() {
                fs::remove_dir_all(item.path())?;
            }
        }
        Ok(Self {
            root,
            current: None,
        })
    }
    pub fn create(
        &mut self,
        store: &Store,
        cluster: Uuid,
        epoch: Uuid,
        tenant: Uuid,
    ) -> Result<SnapshotInfo> {
        let state = store.replication_state();
        if !state.enabled
            || state.cluster != cluster
            || state.epoch != epoch
            || state.role == super::Role::Replica
        {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "checkpoint source changed",
            ));
        }
        if let Some((info, created, _)) = &self.current {
            if created.elapsed() < Duration::from_secs(300) {
                if info.tenant.id == tenant && info.epoch == epoch {
                    return Ok(info.clone());
                }
                return Err(Error::new(
                    "BUSY",
                    "another replica checkpoint transfer is active",
                ));
            }
            self.clear()?;
        }
        let handle = store.tenant(tenant)?;
        let info = handle.info();
        let id = Uuid::new_v4();
        let path = self.root.join(id.to_string());
        let result = (|| {
            let position = handle
                .engine()
                .checkpoint_limited(&path, info.disk_bytes, 0)?;
            let mut files = Vec::new();
            enumerate(&path, &path, &mut files)?;
            if files.len() > 20_000 {
                return Err(Error::limit("replication snapshot file limit"));
            }
            files.sort_by(|a, b| a.path.cmp(&b.path));
            Ok(SnapshotInfo {
                id,
                cluster,
                epoch,
                tenant: info,
                position,
                files,
            })
        })();
        match result {
            Ok(info) => {
                self.current = Some((info.clone(), Instant::now(), path));
                Ok(info)
            }
            Err(error) => {
                let _ = fs::remove_dir_all(path);
                Err(error)
            }
        }
    }
    pub fn chunk(&mut self, id: Uuid, file: usize, offset: u64) -> Result<String> {
        let (info, created, path) = self
            .current
            .as_mut()
            .filter(|(s, _, _)| s.id == id)
            .ok_or_else(|| Error::missing("replication snapshot"))?;
        if created.elapsed() > Duration::from_secs(300) {
            return Err(Error::new(
                "SNAPSHOT_EXPIRED",
                "replication snapshot expired",
            ));
        }
        let item = info
            .files
            .get(file)
            .ok_or_else(|| Error::invalid("snapshot file index"))?;
        if offset > item.size {
            return Err(Error::invalid("snapshot file offset"));
        }
        let mut bytes = vec![0; (item.size - offset).min(super::PAGE_BYTES as u64) as usize];
        File::open(path.join(&item.path))?.read_exact_at(&mut bytes, offset)?;
        *created = Instant::now();
        Ok(STANDARD.encode(bytes))
    }
    pub fn release(&mut self, id: Uuid) -> Result<()> {
        if self.current.as_ref().is_some_and(|(s, _, _)| s.id == id) {
            self.clear()?;
        }
        Ok(())
    }
    fn clear(&mut self) -> Result<()> {
        if let Some((_, _, path)) = self.current.take() {
            fs::remove_dir_all(path)?;
            sync_dir(&self.root)?;
        }
        Ok(())
    }
}
fn enumerate(root: &Path, dir: &Path, files: &mut Vec<FileInfo>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        reject_symlink(&entry.path())?;
        if entry.file_type()?.is_dir() {
            enumerate(root, &entry.path(), files)?;
        } else {
            let path = entry.path();
            let mut file = File::open(&path)?;
            let mut hash = Sha256::new();
            let mut bytes = vec![0; super::PAGE_BYTES];
            loop {
                let n = file.read(&mut bytes)?;
                if n == 0 {
                    break;
                }
                hash.update(&bytes[..n]);
            }
            files.push(FileInfo {
                path: path.strip_prefix(root).unwrap().to_string_lossy().into(),
                size: file.metadata()?.len(),
                sha256: format!("{:x}", hash.finalize()),
            });
        }
    }
    Ok(())
}

pub(crate) fn valid_path(value: &str) -> bool {
    if value == "CURRENT" || value == "TENANT.meta" {
        return true;
    }
    if let Some(id) = value.strip_prefix("MANIFEST-") {
        return Uuid::parse_str(id).is_ok();
    }
    for (prefix, suffix) in [("sst/", ".sst"), ("wal/", ".wal")] {
        if let Some(id) = value
            .strip_prefix(prefix)
            .and_then(|p| p.strip_suffix(suffix))
        {
            return Uuid::parse_str(id).is_ok();
        }
    }
    false
}
