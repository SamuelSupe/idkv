use super::*;

#[derive(Serialize, Deserialize)]
pub(super) struct SyncedWals {
    tenant: Uuid,
    generation: Uuid,
    lengths: BTreeMap<Uuid, u64>,
}
impl SyncedWals {
    pub(super) fn load(path: &Path, info: &TenantInfo) -> Result<Option<Self>> {
        let path = path.join("WAL-SYNC");
        if !path.try_exists()? {
            return Ok(None);
        }
        reject_symlink(&path)?;
        Ok(read_json::<Self>(&path)
            .ok()
            .filter(|mark| mark.tenant == info.id && mark.generation == info.generation))
    }
    pub(super) fn length(&self, id: Uuid) -> Option<u64> {
        self.lengths.get(&id).copied()
    }
}

impl Engine {
    pub(crate) fn ensure_durable(&self) -> Result<()> {
        if self.recovery_read_only {
            return Err(Error::limit(
                "WAL recovery needs free disk space; read-only until closed and reopened",
            ));
        }
        Ok(())
    }
    pub(crate) fn recovery_read_only(&self) -> bool {
        self.recovery_read_only
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let writer = self.writer.get_mut();
        if self.recovery_read_only || writer.poison.is_some() {
            return;
        }
        let result = (|| -> Result<()> {
            let mut lengths = BTreeMap::new();
            let mut files = Vec::new();
            for info in &writer.manifest.wals {
                let (file, length) = if info.id == writer.wal.info.id {
                    (writer.wal.file.clone(), writer.wal.len)
                } else {
                    let file = Arc::new(File::open(
                        self.path.join("wal").join(format!("{}.wal", info.id)),
                    )?);
                    let length = file.metadata()?.len();
                    (file, length)
                };
                lengths.insert(info.id, length);
                files.push(file);
            }
            if SyncedWals::load(&self.path, &self.info)?.is_some_and(|mark| mark.lengths == lengths)
            {
                return Ok(());
            }
            for file in files {
                file.sync_all()?;
            }
            // The marker covers immutable prefixes only. Later appends exceed
            // these lengths, so a crash or uncertain append cannot reuse it.
            atomic_json(
                &self.path.join("WAL-SYNC"),
                &SyncedWals {
                    tenant: self.info.id,
                    generation: self.info.generation,
                    lengths,
                },
            )
        })();
        if let Err(error) = result {
            eprintln!("tenant {} clean-close watermark: {error}", self.info.id);
        }
    }
}
