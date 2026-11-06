use super::*;

struct RestorePreparation {
    slot: Arc<Mutex<Slot>>,
    path: PathBuf,
    journaled: bool,
}
impl Drop for RestorePreparation {
    fn drop(&mut self) {
        if !self.journaled && self.path.exists() {
            let _ = fs::remove_dir_all(&self.path);
        }
        self.slot.lock().restoring = false;
    }
}

impl Store {
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
        let mut preparation = RestorePreparation {
            slot: self.slot(info.id),
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
        let tenant = self.tenant(id)?;
        let backup = Uuid::new_v4();
        let parent = self.root().join("backups").join(id.to_string());
        fs::create_dir_all(&parent)?;
        reject_symlink(&parent)?;
        let destination = parent.join(backup.to_string());
        tenant.engine().checkpoint(&destination)?;
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
        Ok(backup)
    }
    pub fn restore(&self, id: Uuid, backup: Uuid) -> Result<()> {
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
            if guard.restoring {
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
        let mut preparation = RestorePreparation {
            slot: slot.clone(),
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
        copy_tree(source, &staged, id, &budget, &inner.io)?;
        atomic_json(&staged.join("TENANT.meta"), &registration.info)?;
        let check = Engine::open(
            staged.clone(),
            registration.info.clone(),
            inner.options.clone(),
            budget,
            inner.io.clone(),
        )?;
        check.verify()?;
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
            slot.failure = Some(Error::new(
                "UNAVAILABLE",
                format!("restore publication uncertain; reopen Store: {error}"),
            ));
        }
        drop(registry);
        drop(slot);
        // Large obsolete directories are reclaimed after publication, without
        // holding the shared registry or this tenant's lifecycle lock.
        drop(_permission);
        let cleanup = remove_lifecycle_debris(&inner.root, result?);
        if let Err(error) = &cleanup {
            preparation.slot.lock().failure = Some(Error::new(
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
    for directory in ["trash", "staging"] {
        // Another tenant may be preparing an unpublished restore while serving traffic.
        if directory == "staging" && !startup {
            continue;
        }
        for entry in fs::read_dir(root.join(directory))? {
            let entry = entry?;
            let Ok(id) = Uuid::parse_str(&entry.file_name().to_string_lossy()) else {
                continue;
            };
            let referenced =
                registry.tenants.iter().any(|(tenant, registration)| {
                    match registration.lifecycle {
                        Lifecycle::Restoring { stage, old } => id == stage || id == old,
                        Lifecycle::Deleting => id == *tenant,
                        _ => false,
                    }
                });
            if !referenced {
                reject_symlink(&entry.path())?;
                paths.push(entry.path());
            }
        }
    }
    Ok(paths)
}
pub(super) fn remove_lifecycle_debris(root: &Path, paths: Vec<PathBuf>) -> Result<()> {
    for path in paths {
        match fs::remove_dir_all(path) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
            _ => {}
        }
    }
    sync_dir(&root.join("trash"))?;
    sync_dir(&root.join("staging"))?;
    Ok(())
}
