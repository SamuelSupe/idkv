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
    /// Validate and truncate a private repair copy at an exact boundary in its last WAL.
    pub(crate) fn truncate_repair_copy(
        path: &Path,
        info: &TenantInfo,
        id: Uuid,
        offset: u64,
        budget: &Budget,
    ) -> Result<()> {
        let manifest = load_manifest(path)?;
        let tail = manifest
            .wals
            .last()
            .ok_or_else(|| Error::corrupt("missing tail WAL"))?;
        if tail.id != id {
            return Err(Error::invalid("only the last active WAL can be truncated"));
        }
        let wal = path.join("wal").join(format!("{id}.wal"));
        reject_symlink(&wal)?;
        let file = OpenOptions::new().write(true).open(&wal)?;
        if offset < wal::HEADER as u64 || offset > file.metadata()?.len() {
            return Err(Error::invalid("truncate offset outside WAL"));
        }
        file.set_len(offset)?;
        file.sync_all()?;
        // A non-tail replay rejects a partial frame: never silently round the
        // operator's chosen boundary down. The serving file is untouched here.
        Wal::recover(
            &path.join("wal"),
            tail,
            info,
            false,
            None,
            budget,
            |_, _| Ok(()),
        )?;
        match fs::remove_file(path.join("WAL-SYNC")) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
            _ => {}
        }
        sync_dir(path)
    }
    pub(crate) fn empty_unregistered(path: &Path) -> Result<bool> {
        reject_symlink(&path.join("wal"))?;
        reject_symlink(&path.join("sst"))?;
        let info: TenantInfo = read_json(&path.join("TENANT.meta"))?;
        let manifest = load_manifest(path)?;
        if path.file_name().and_then(|name| name.to_str()) != Some(&info.id.to_string())
            || manifest.version != 1
            || manifest.tenant != info.id
            || manifest.generation != info.generation
            || manifest.checkpoint != 0
            || !manifest.tables.is_empty()
            || !manifest.retained_wals.is_empty()
            || manifest.wals.len() != 1
            || fs::read_dir(path.join("sst"))?.next().is_some()
        {
            return Ok(false);
        }
        let wal = path
            .join("wal")
            .join(format!("{}.wal", manifest.wals[0].id));
        reject_symlink(&wal)?;
        Ok(fs::metadata(wal)?.len() == wal::HEADER as u64
            && fs::read_dir(path.join("wal"))?.count() == 1)
    }

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
        self.log_retention.pins.lock().clear();
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
