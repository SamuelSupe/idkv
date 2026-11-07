use super::*;

struct TenantPreparation {
    slot: Option<Arc<Mutex<Slot>>>,
    path: PathBuf,
    journaled: bool,
}
impl Drop for TenantPreparation {
    fn drop(&mut self) {
        if !self.journaled && self.path.exists() {
            let _ = fs::remove_dir_all(&self.path);
        }
        if let Some(slot) = &self.slot {
            slot.lock().restoring = false;
        }
    }
}

impl Store {
    pub(super) fn create_tenant_inner(
        &self,
        registry: &mut Registry,
        info: TenantInfo,
    ) -> Result<()> {
        let root = self.root();
        let stage = Uuid::new_v4();
        let path = root.join("staging").join(stage.to_string());
        let mut preparation = TenantPreparation {
            slot: None,
            path: path.clone(),
            journaled: false,
        };
        Engine::create(&path, &info)?;
        crate::storage::failpoint("tenant_create_staged");
        let mut next = registry.clone();
        next.tenants.insert(
            info.id,
            Registration {
                info,
                lifecycle: Lifecycle::Restoring {
                    stage,
                    old: Uuid::new_v4(),
                },
            },
        );
        // Publication may have renamed the journal before reporting an error.
        // Keep its stage and fence the registration until startup resolves it.
        preparation.journaled = true;
        *registry = next;
        atomic_json(&root.join("control/tenants.json"), registry)?;
        crate::storage::failpoint("tenant_create_journal");
        recover_lifecycle(root, registry)?;
        atomic_json(&root.join("control/tenants.json"), registry)?;
        crate::storage::failpoint("tenant_create_published");
        Ok(())
    }

    /// Publish a verified backup under its original UUID on a new Store. A UUID
    /// already present in the registry (including a deleted tenant) is never reused.
    pub(crate) fn import_checkpoint(&self, source: &Path, mut info: TenantInfo) -> Result<()> {
        let _permission = self.owner.inner.node.write()?;
        self.require_local_restore()?;
        info.history = Uuid::new_v4();
        self.import_checkpoint_inner(source, info)
    }
    pub(super) fn import_checkpoint_inner(&self, source: &Path, info: TenantInfo) -> Result<()> {
        super::super::catalog::name(&info.name)?;
        let inner = &self.owner.inner;
        let stage = Uuid::new_v4();
        let staged = inner.root.join("staging").join(stage.to_string());
        let mut preparation = TenantPreparation {
            slot: Some(self.slot(info.id)),
            path: staged.clone(),
            journaled: false,
        };
        let mut registry = inner.registry.write();
        if registry.tenants.contains_key(&info.id)
            || registry
                .tenants
                .values()
                .any(|r| r.info.name == info.name && !matches!(r.lifecycle, Lifecycle::Deleted))
            || inner
                .root
                .join("tenants")
                .join(info.id.to_string())
                .exists()
        {
            return Err(Error::new(
                "ALREADY_EXISTS",
                "import tenant UUID, name, or directory already exists",
            ));
        }
        if registry
            .tenants
            .values()
            .filter(|r| !matches!(r.lifecycle, Lifecycle::Deleted))
            .count()
            >= 16_384
        {
            return Err(Error::limit("tenant registry limit"));
        }
        // source is the already verified, same-filesystem private backup workspace.
        // Journal before publication so startup can finish an interrupted import.
        atomic_json(&source.join("TENANT.meta"), &info)?;
        fs::rename(source, &staged)?;
        sync_dir(
            source
                .parent()
                .ok_or_else(|| Error::invalid("import source"))?,
        )?;
        sync_dir(&inner.root.join("staging"))?;
        let mut next = registry.clone();
        next.tenants.insert(
            info.id,
            Registration {
                info,
                lifecycle: Lifecycle::Restoring {
                    stage,
                    old: Uuid::new_v4(),
                },
            },
        );
        // Fence the registration even when durable publication returns an uncertain
        // I/O result. Reopening Store resolves the existing lifecycle journal.
        *registry = next;
        preparation.journaled = true;
        atomic_json(&inner.root.join("control/tenants.json"), &*registry)?;
        crate::storage::failpoint("backup_import_journal");
        recover_lifecycle(&inner.root, &mut registry)?;
        atomic_json(&inner.root.join("control/tenants.json"), &*registry)
    }
    pub fn backup(&self, id: Uuid) -> Result<Uuid> {
        let _backup = self.owner.inner.local_backups.lock();
        let tenant = self.tenant(id)?;
        let backup = Uuid::new_v4();
        let parent = self.root().join("backups").join(id.to_string());
        fs::create_dir_all(&parent)?;
        reject_symlink(&parent)?;
        if fs::read_dir(&parent)?.count() >= 64 {
            return Err(Error::limit("local backup count limit; delete old backups"));
        }
        let limit = tenant
            .info()
            .disk_bytes
            .min(self.options().max_local_backup_bytes);
        let remaining = limit
            .checked_sub(crate::storage::directory_size(&parent)?)
            .ok_or_else(|| Error::limit("local backup byte limit"))?;
        let destination = parent.join(backup.to_string());
        let result = (|| -> Result<()> {
            tenant.engine().checkpoint_limited(
                &destination,
                remaining,
                self.options().maintenance_reserve_free_bytes,
            )?;
            let check = Engine::open(
                destination.clone(),
                tenant.info(),
                self.options().clone(),
                Budget {
                    global: self.owner.inner.memory.clone(),
                    tenant: tenant.engine().budget.tenant.clone(),
                },
                self.owner.inner.io.clone(),
            )?;
            check.verify()?;
            drop(check);
            sync_dir(&parent)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_dir_all(&destination);
        }
        result?;
        Ok(backup)
    }
    /// Delete a local backup. The online database and remote backups are unaffected.
    pub fn delete_backup(&self, id: Uuid, backup: Uuid) -> Result<()> {
        let _backup = self.owner.inner.local_backups.lock();
        self.backup_context(id)?;
        let parent = self.root().join("backups").join(id.to_string());
        let path = parent.join(backup.to_string());
        if !path.try_exists()? {
            return Err(Error::missing("local backup"));
        }
        reject_symlink(&path)?;
        fs::remove_dir_all(path)?;
        sync_dir(&parent)
    }
    pub fn restore(&self, id: Uuid, backup: Uuid) -> Result<()> {
        let _backup = self.owner.inner.local_backups.lock();
        let source = self
            .root()
            .join("backups")
            .join(id.to_string())
            .join(backup.to_string());
        self.restore_checkpoint(id, &source)
    }
    pub(crate) fn restore_checkpoint(&self, id: Uuid, source: &Path) -> Result<()> {
        self.require_local_restore()?;
        self.restore_checkpoint_branch(id, source, None)
    }
    pub(super) fn restore_checkpoint_inner(
        &self,
        id: Uuid,
        source: &Path,
        upstream: &crate::replication::Source,
    ) -> Result<()> {
        self.restore_checkpoint_branch(id, source, Some(upstream))
    }
    fn restore_checkpoint_branch(
        &self,
        id: Uuid,
        source: &Path,
        upstream: Option<&crate::replication::Source>,
    ) -> Result<()> {
        let slot = self.slot(id);
        let inner = &self.owner.inner;
        let stage = Uuid::new_v4();
        let staged = inner.root.join("staging").join(stage.to_string());
        let (mut registration, memory) = {
            let mut guard = slot.lock();
            if guard.restoring || (upstream.is_none() && guard.installing.is_some()) {
                return Err(Error::new("BUSY", "tenant restore preparation is active"));
            }
            let registration = inner
                .registry
                .read()
                .tenants
                .get(&id)
                .cloned()
                .ok_or_else(|| Error::missing("tenant"))?;
            if !matches!(registration.lifecycle, Lifecycle::Active) {
                return Err(Error::new("UNAVAILABLE", "tenant not active"));
            }
            guard.restoring = true;
            (registration, guard.memory.clone())
        };
        let mut preparation = TenantPreparation {
            slot: Some(slot.clone()),
            path: staged.clone(),
            journaled: false,
        };
        let backup_info: TenantInfo = read_json(&source.join("TENANT.meta"))?;
        if backup_info.id != id || backup_info.generation != registration.info.generation {
            return Err(Error::invalid(
                "backup belongs to a different tenant database",
            ));
        }
        registration.info.history = if upstream.is_none() {
            Uuid::new_v4()
        } else {
            backup_info.history
        };
        if crate::storage::directory_size(source)? > registration.info.disk_bytes {
            return Err(Error::limit("restore tenant disk quota"));
        }
        let budget = Budget {
            global: inner.memory.clone(),
            tenant: memory,
        };
        if upstream.is_some()
            && source.parent() == Some(inner.root.join("replication-work/import").as_path())
        {
            fs::rename(source, &staged)?;
            sync_dir(source.parent().unwrap())?;
            sync_dir(&inner.root.join("staging"))?;
        } else {
            copy_tree(source, &staged, id, &budget, &inner.io)?;
        }
        atomic_json(&staged.join("TENANT.meta"), &registration.info)?;
        let check = Engine::open(
            staged.clone(),
            registration.info.clone(),
            inner.options.clone(),
            budget,
            inner.io.clone(),
        )?;
        check.verify()?;
        let restored_seq = check.replication_position()?;
        drop(check);
        // Copy and verification run without the tenant lifecycle lock. Only publication
        // needs a stopped tenant; a failed attempt leaves the serving database intact.
        let _permission = match upstream {
            Some(source) => inner.node.replica_write(source)?,
            None => {
                let permission = inner.node.write()?;
                self.require_local_restore()?;
                permission
            }
        };
        let _drain = if upstream.is_none() {
            Some(self.drain_for_management(id, true)?)
        } else {
            None
        };
        let mut slot = slot.lock();
        self.close_slot(&mut slot)?;
        slot.last_seq = None;
        let _offline = self.offline_lock(id)?;
        let mut registry = inner.registry.write();
        let mut next = registry.clone();
        next.tenants.get_mut(&id).unwrap().info = registration.info.clone();
        next.tenants.get_mut(&id).unwrap().lifecycle = Lifecycle::Restoring {
            stage,
            old: Uuid::new_v4(),
        };
        // An fsync/rename error can leave a durable journal. Preserve its staged files
        // and fence this tenant until the manager is reopened.
        preparation.journaled = true;
        let result = (|| {
            atomic_json(&inner.root.join("control/tenants.json"), &next)?;
            crate::storage::errorpoint("restore_journal_published")?;
            *registry = next;
            recover_lifecycle(&inner.root, &mut registry)?;
            atomic_json(&inner.root.join("control/tenants.json"), &*registry)?;
            collect_lifecycle_debris(&inner.root, &registry, false)
        })();
        if let Err(error) = &result {
            slot.record_failure(Error::new(
                "UNAVAILABLE",
                format!("restore publication uncertain; reopen Store: {error}"),
            ));
        }
        if result.is_ok() {
            slot.last_seq = Some(restored_seq);
            slot.installing = None;
        }
        drop(registry);
        drop(slot);
        // Large obsolete directories are reclaimed after publication, without
        // holding the shared registry or this tenant's lifecycle lock.
        drop(_permission);
        let cleanup = remove_lifecycle_debris(&inner.root, result?);
        if let Err(error) = &cleanup {
            preparation
                .slot
                .as_ref()
                .unwrap()
                .lock()
                .record_failure(Error::new(
                    "UNAVAILABLE",
                    format!("restore cleanup failed; reopen Store to recover: {error}"),
                ));
        }
        cleanup
    }
}

pub(super) fn recover_lifecycle(root: &Path, registry: &mut Registry) -> Result<()> {
    for (id, registration) in &mut registry.tenants {
        let current = root.join("tenants").join(id.to_string());
        match registration.lifecycle.clone() {
            Lifecycle::Deleting => {
                let trash = root.join("trash").join(id.to_string());
                if current.exists() {
                    reject_symlink(&current)?;
                    fs::rename(&current, &trash)?;
                    sync_dir(&root.join("tenants"))?;
                    sync_dir(&root.join("trash"))?;
                }
                registration.lifecycle = Lifecycle::Deleted;
            }
            Lifecycle::Restoring { stage, old } => {
                let staged = root.join("staging").join(stage.to_string());
                let previous = root.join("trash").join(old.to_string());
                if staged.exists() {
                    reject_symlink(&staged)?;
                    if current.exists() {
                        if previous.exists() {
                            return Err(Error::corrupt("ambiguous restore journal"));
                        }
                        reject_symlink(&current)?;
                        fs::rename(&current, &previous)?;
                        sync_dir(&root.join("tenants"))?;
                        sync_dir(&root.join("trash"))?;
                        crate::storage::failpoint("restore_old_moved");
                    }
                    fs::rename(&staged, &current)?;
                    sync_dir(&root.join("staging"))?;
                    sync_dir(&root.join("tenants"))?;
                    crate::storage::failpoint("restore_new_moved");
                }
                if !current.exists() {
                    return Err(Error::corrupt(
                        "restore lost both current and staged database",
                    ));
                }
                let actual: TenantInfo = read_json(&current.join("TENANT.meta"))?;
                if actual.id != *id
                    || actual.generation != registration.info.generation
                    || actual.history != registration.info.history
                {
                    return Err(Error::corrupt("restore tenant mismatch"));
                }
                registration.lifecycle = Lifecycle::Active;
                // Keep the old directory until the Active registry is durable.
                // A later startup can remove unreferenced trash safely.
            }
            _ => {}
        }
    }
    Ok(())
}
fn copy_tree(
    source: &Path,
    destination: &Path,
    id: Uuid,
    budget: &Budget,
    io: &IoScheduler,
) -> Result<()> {
    reject_symlink(source)?;
    fs::create_dir(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        reject_symlink(&entry.path())?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target, id, budget, io)?;
        } else {
            crate::storage::copy_file(&entry.path(), &target, id, budget, io)?;
        }
    }
    sync_dir(destination)
}

pub(super) fn clean_lifecycle_debris(
    root: &Path,
    registry: &Registry,
    startup: bool,
) -> Result<()> {
    remove_lifecycle_debris(root, collect_lifecycle_debris(root, registry, startup)?)
}
pub(super) fn collect_lifecycle_debris(
    root: &Path,
    registry: &Registry,
    startup: bool,
) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for directory in ["trash", "staging", "tenants"] {
        // Another tenant may be preparing an unpublished restore while serving traffic.
        if directory != "trash" && !startup {
            continue;
        }
        for entry in fs::read_dir(root.join(directory))? {
            let entry = entry?;
            let Ok(id) = Uuid::parse_str(&entry.file_name().to_string_lossy()) else {
                continue;
            };
            let referenced = if directory == "tenants" {
                registry.tenants.contains_key(&id)
            } else {
                registry
                    .tenants
                    .iter()
                    .any(|(tenant, registration)| match registration.lifecycle {
                        Lifecycle::Restoring { stage, old } => id == stage || id == old,
                        Lifecycle::Deleting => id == *tenant,
                        _ => false,
                    })
            };
            if !referenced {
                reject_symlink(&entry.path())?;
                paths.push(entry.path());
            }
        }
    }
    Ok(paths)
}
pub(super) fn remove_lifecycle_debris(root: &Path, paths: Vec<PathBuf>) -> Result<()> {
    let mut removed_orphan = false;
    for path in paths {
        // Older failed creates left UUID directories outside the registry.
        // A directly opened Engine may still own one; never unlink its database.
        let lock = path.join("LOCK");
        let orphan = path.parent() == Some(root.join("tenants").as_path());
        let _owner = if orphan {
            if lock.exists() {
                reject_symlink(&lock)?;
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock)?;
            file.try_lock_exclusive()
                .map_err(|_| Error::new("BUSY", "unregistered tenant is owned by a live engine"))?;
            Some(file)
        } else {
            None
        };
        if orphan && !matches!(Engine::empty_unregistered(&path), Ok(true)) {
            eprintln!(
                "retaining unregistered database for inspection: {}",
                path.display()
            );
            continue;
        }
        match fs::remove_dir_all(path) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
            _ => {}
        }
        removed_orphan |= orphan;
    }
    if removed_orphan {
        sync_dir(&root.join("tenants"))?;
    }
    sync_dir(&root.join("trash"))?;
    sync_dir(&root.join("staging"))?;
    Ok(())
}
