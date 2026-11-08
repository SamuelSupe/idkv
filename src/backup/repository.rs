use super::*;
use std::io::Read;
use transfer::object_key;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Completed {
    version: u32,
    manifest_sha256: String,
}

fn snapshot_prefix(tenant: Uuid, backup: Uuid) -> String {
    format!("tenants/{tenant}/snapshots/{backup}/")
}
impl Inner {
    fn remote_bytes(&self, path: &str, limit: usize) -> Result<Vec<u8>> {
        let meta = self.op.stat(path).map_err(remote_error)?;
        if meta.content_length() > limit as u64 {
            return Err(Error::corrupt("remote metadata exceeds byte limit"));
        }
        let mut reader = self
            .op
            .reader(path)
            .map_err(remote_error)?
            .into_std_read(..)
            .map_err(remote_error)?
            .take(limit as u64 + 1);
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(|_| Error::new("BACKUP_STORAGE_ERROR", "metadata read failed"))?;
        if bytes.len() > limit || bytes.len() as u64 != meta.content_length() {
            return Err(Error::corrupt("remote metadata length mismatch"));
        }
        Ok(bytes)
    }
    fn immutable(&self, path: &str, bytes: Vec<u8>) -> Result<()> {
        match self.op.write_options(
            path,
            bytes.clone(),
            opendal::options::WriteOptions {
                if_not_exists: true,
                ..Default::default()
            },
        ) {
            Ok(_) => Ok(()),
            Err(e)
                if matches!(
                    e.kind(),
                    opendal::ErrorKind::ConditionNotMatch | opendal::ErrorKind::AlreadyExists
                ) =>
            {
                if self.remote_bytes(path, META_LIMIT)? == bytes {
                    Ok(())
                } else {
                    Err(Error::corrupt(
                        "immutable backup object already has different content",
                    ))
                }
            }
            Err(e) => Err(remote_error(e)),
        }
    }
    pub(super) fn manifest(&self, tenant: Uuid, backup: Uuid) -> Result<BackupManifest> {
        let prefix = snapshot_prefix(tenant, backup);
        let marker: Completed =
            serde_json::from_slice(&self.remote_bytes(&format!("{prefix}COMPLETED"), 1024)?)
                .map_err(|_| Error::corrupt("backup completion marker"))?;
        let bytes = self
            .remote_bytes(&format!("{prefix}manifest.json"), META_LIMIT)
            .map_err(|e| {
                if e.code == "NOT_FOUND" {
                    Error::corrupt("completed backup has no manifest")
                } else {
                    e
                }
            })?;
        if marker.version != 1 || hash(&bytes) != marker.manifest_sha256 {
            return Err(Error::corrupt("backup manifest digest"));
        }
        let manifest: BackupManifest = serde_json::from_slice(&bytes)
            .map_err(|_| Error::corrupt("backup manifest encoding"))?;
        manifest.validate(tenant, backup, self.config.max_backup_bytes)?;
        Ok(manifest)
    }
    fn scan_snapshots(
        &self,
        tenant: Uuid,
        scan_limit: Option<usize>,
        check: &impl Fn() -> Result<()>,
        mut visit: impl FnMut(BackupSummary) -> Result<()>,
    ) -> Result<()> {
        let prefix = format!("tenants/{tenant}/snapshots/");
        for (visited, entry) in self.op.lister(&prefix).map_err(remote_error)?.enumerate() {
            check()?;
            if scan_limit.is_some_and(|limit| visited >= limit) {
                return Err(Error::limit("backup inventory scan limit; run prune"));
            }
            let entry = entry.map_err(remote_error)?;
            let name = entry
                .path()
                .strip_prefix(&prefix)
                .ok_or_else(|| Error::corrupt("backup listing escaped tenant prefix"))?
                .trim_end_matches('/');
            let Ok(id) = Uuid::parse_str(name) else {
                continue;
            };
            match self.manifest(tenant, id) {
                Ok(manifest) => visit(manifest.summary())?,
                Err(error) if error.code == "NOT_FOUND" => {
                    if self
                        .op
                        .exists(&format!("{}COMPLETED", snapshot_prefix(tenant, id)))
                        .map_err(remote_error)?
                    {
                        return Err(Error::corrupt("completed backup has no manifest"));
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
    pub(super) fn summaries(&self, tenant: Uuid) -> Result<Vec<BackupSummary>> {
        let mut result = Vec::new();
        self.scan_snapshots(tenant, Some(200_000), &|| Ok(()), |summary| {
            result.push(summary);
            if result.len() > 4096 {
                return Err(Error::limit("remote backup listing limit; run prune"));
            }
            Ok(())
        })?;
        result.sort_by_key(|m| {
            (
                std::cmp::Reverse(m.checkpoint_ms),
                std::cmp::Reverse(m.backup),
            )
        });
        Ok(result)
    }
    pub(super) fn checkpoint_time(
        &self,
        tenant: Uuid,
        check: impl Fn() -> Result<()>,
    ) -> Result<u64> {
        check()?;
        let path = self.jobs_path.join(format!("CLOCK-{tenant}"));
        let previous: u64 = if path.try_exists()? {
            read_json(&path)?
        } else {
            let mut latest = 0;
            self.scan_snapshots(tenant, None, &check, |summary| {
                latest = latest.max(summary.checkpoint_ms);
                Ok(())
            })?;
            latest
        };
        let next = now_ms().max(
            previous
                .checked_add(1)
                .ok_or_else(|| Error::limit("backup clock exhausted"))?,
        );
        atomic_json(&path, &next)?;
        Ok(next)
    }
    pub(super) fn publish_backup(&self, manifest: &BackupManifest) -> Result<()> {
        manifest.validate(
            manifest.tenant.id,
            manifest.backup,
            self.config.max_backup_bytes,
        )?;
        let bytes = serde_json::to_vec(manifest)?;
        if bytes.len() > META_LIMIT {
            return Err(Error::limit("backup manifest size"));
        }
        let marker = serde_json::to_vec(&Completed {
            version: 1,
            manifest_sha256: hash(&bytes),
        })?;
        let prefix = snapshot_prefix(manifest.tenant.id, manifest.backup);
        self.immutable(&format!("{prefix}manifest.json"), bytes)?;
        crate::storage::failpoint("backup_manifest_uploaded");
        self.immutable(&format!("{prefix}COMPLETED"), marker)?;
        crate::storage::failpoint("backup_completed");
        Ok(())
    }
    pub(super) fn prune(
        &self,
        tenant: Uuid,
        keep: usize,
        check: impl Fn() -> Result<()>,
    ) -> Result<serde_json::Value> {
        self.reclaim_empty_snapshots(tenant, &check)?;
        // Validate the inventory before deleting. Selection retains only the
        // newest `keep` summaries, so list's response cap cannot block pruning.
        let mut newest = BTreeMap::new();
        self.scan_snapshots(tenant, None, &check, |summary| {
            newest.insert((summary.checkpoint_ms, summary.backup), summary);
            if newest.len() > keep {
                newest.pop_first();
            }
            Ok(())
        })?;
        let retained: Vec<_> = newest.into_values().collect();
        let ids: HashSet<_> = retained.iter().map(|m| m.backup).collect();
        let mut removed = 0;
        self.scan_snapshots(tenant, None, &check, |summary| {
            if !ids.contains(&summary.backup) {
                let prefix = snapshot_prefix(tenant, summary.backup);
                self.op
                    .delete(&format!("{prefix}COMPLETED"))
                    .map_err(remote_error)?;
                self.op
                    .delete(&format!("{prefix}manifest.json"))
                    .map_err(remote_error)?;
                self.op.delete(&prefix).map_err(remote_error)?;
                removed += 1;
            }
            Ok(())
        })?;
        let cutoff = now_ms().saturating_sub(self.config.gc_grace_seconds.saturating_mul(1000));
        let prefix = format!("tenants/{tenant}/objects/");
        let mut deleted = 0;
        let mut candidates = HashSet::new();
        let options = opendal::options::ListOptions {
            recursive: true,
            ..Default::default()
        };
        for entry in self
            .op
            .lister_options(&prefix, options)
            .map_err(remote_error)?
        {
            check()?;
            let entry = entry.map_err(remote_error)?;
            if !entry.path().starts_with(&prefix) {
                return Err(Error::corrupt("GC listing escaped tenant prefix"));
            }
            if entry.metadata().is_dir() {
                continue;
            }
            let meta = self.op.stat(entry.path()).map_err(remote_error)?;
            if meta.last_modified().is_some_and(|t| {
                t.into_inner().as_millisecond() >= 0
                    && t.into_inner().as_millisecond() as u64 <= cutoff
            }) {
                candidates.insert(entry.path().to_owned());
            }
            if candidates.len() == 4096 {
                deleted += self.prune_object_batch(tenant, &retained, &mut candidates, &check)?;
            }
        }
        deleted += self.prune_object_batch(tenant, &retained, &mut candidates, &check)?;
        Ok(
            serde_json::json!({"retained_backups":retained.len(),"deleted_backups":removed,"deleted_objects":deleted}),
        )
    }

    fn prune_object_batch(
        &self,
        tenant: Uuid,
        retained: &[BackupSummary],
        candidates: &mut HashSet<String>,
        check: &impl Fn() -> Result<()>,
    ) -> Result<usize> {
        // Bound memory by candidate objects, not the total number of references.
        // Large inventories trade repeated manifest reads for bounded workspace.
        for summary in retained {
            if candidates.is_empty() {
                return Ok(0);
            }
            check()?;
            let manifest = self.manifest(tenant, summary.backup)?;
            for file in &manifest.files {
                candidates.remove(&object_key(
                    tenant,
                    manifest.tenant.generation,
                    &file.sha256,
                ));
            }
        }
        let deleted = candidates.len();
        for path in candidates.drain() {
            check()?;
            self.op.delete(&path).map_err(remote_error)?;
        }
        Ok(deleted)
    }

    fn reclaim_empty_snapshots(&self, tenant: Uuid, check: &impl Fn() -> Result<()>) -> Result<()> {
        let prefix = format!("tenants/{tenant}/snapshots/");
        let mut removed = 0;
        for (visited, entry) in self.op.lister(&prefix).map_err(remote_error)?.enumerate() {
            check()?;
            if visited >= 200_000 || removed >= 4096 {
                break;
            }
            let entry = entry.map_err(remote_error)?;
            let name = entry
                .path()
                .strip_prefix(&prefix)
                .ok_or_else(|| Error::corrupt("backup listing escaped tenant prefix"))?
                .trim_end_matches('/');
            if !entry.metadata().is_dir() || Uuid::parse_str(name).is_err() {
                continue;
            }
            let mut empty = true;
            for child in self.op.lister(entry.path()).map_err(remote_error)? {
                let child = child.map_err(remote_error)?;
                // The fs backend also lists the directory itself.
                if child.path() != entry.path() {
                    empty = false;
                    break;
                }
            }
            if empty {
                self.op.delete(entry.path()).map_err(remote_error)?;
                removed += 1;
            }
        }
        Ok(())
    }
}
