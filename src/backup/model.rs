use crate::{Error, Result, TenantInfo};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

pub(crate) const META_LIMIT: usize = 4 << 20;
pub(crate) const FILE_LIMIT: usize = 20_000;
pub(crate) const CHUNK: usize = 1 << 20;

/// Operator settings are administrator configuration, never accepted from a data request.
/// Prefer the S3 environment/identity credential chain over credentials in this file.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackupConfig {
    pub service: String,
    pub options: HashMap<String, String>,
    pub concurrency: usize,
    pub max_jobs: usize,
    pub max_backup_bytes: u64,
    pub max_staging_bytes: u64,
    pub reserve_free_bytes: u64,
    pub bytes_per_second: u64,
    pub timeout_seconds: u64,
    pub job_timeout_seconds: u64,
    pub gc_grace_seconds: u64,
}
impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            service: "s3".into(),
            options: HashMap::new(),
            concurrency: 2,
            max_jobs: 256,
            max_backup_bytes: 100 << 30,
            max_staging_bytes: 200 << 30,
            reserve_free_bytes: 1 << 30,
            bytes_per_second: 50 << 20,
            timeout_seconds: 30,
            job_timeout_seconds: 24 * 3600,
            gc_grace_seconds: 3600,
        }
    }
}
impl BackupConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if !["s3", "fs"].contains(&self.service.as_str())
            || !(1..=8).contains(&self.concurrency)
            || !(1..=4096).contains(&self.max_jobs)
            || self.max_backup_bytes < 1 << 20
            || self.max_backup_bytes > 16 << 40
            || self.max_staging_bytes < self.max_backup_bytes
            || self.max_staging_bytes > 32 << 40
            || !(1 << 20..=10 << 30).contains(&self.bytes_per_second)
            || !(1..=300).contains(&self.timeout_seconds)
            || !(1..=7 * 86400).contains(&self.job_timeout_seconds)
            || self.gc_grace_seconds > 365 * 86400
        {
            return Err(Error::invalid("backup service or resource limits"));
        }
        if self.options.len() > 64
            || self
                .options
                .iter()
                .any(|(k, v)| k.len() > 128 || v.len() > 4096)
        {
            return Err(Error::invalid("backup operator configuration too large"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum BackupOperation {
    Create,
    Verify {
        backup: Uuid,
    },
    Restore {
        backup: Uuid,
    },
    /// Disaster recovery into a Store that has never registered this tenant UUID.
    Import {
        backup: Uuid,
    },
    Prune {
        keep_last: usize,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Checkpointing,
    Uploading,
    Verifying,
    Restoring,
    Pruning,
    Completed,
    Failed,
    Interrupted,
}
impl JobState {
    pub(crate) fn terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Interrupted)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupJob {
    pub id: Uuid,
    pub tenant: Uuid,
    pub backup: Uuid,
    pub operation: BackupOperation,
    pub state: JobState,
    pub created_ms: u64,
    pub updated_ms: u64,
    pub uploaded_bytes: u64,
    pub reused_bytes: u64,
    pub processed_files: usize,
    pub checkpoint_seq: Option<u64>,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupFile {
    pub path: String,
    pub size: u64,
    pub sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupManifest {
    pub version: u32,
    pub backup: Uuid,
    pub tenant: TenantInfo,
    pub checkpoint_seq: u64,
    pub checkpoint_ms: u64,
    pub total_bytes: u64,
    pub files: Vec<BackupFile>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupSummary {
    pub backup: Uuid,
    pub tenant: Uuid,
    pub generation: Uuid,
    pub checkpoint_seq: u64,
    pub checkpoint_ms: u64,
    pub total_bytes: u64,
    pub files: usize,
}
impl BackupManifest {
    pub fn summary(&self) -> BackupSummary {
        BackupSummary {
            backup: self.backup,
            tenant: self.tenant.id,
            generation: self.tenant.generation,
            checkpoint_seq: self.checkpoint_seq,
            checkpoint_ms: self.checkpoint_ms,
            total_bytes: self.total_bytes,
            files: self.files.len(),
        }
    }
    pub(crate) fn validate(&self, tenant: Uuid, backup: Uuid, max_bytes: u64) -> Result<()> {
        let mut names = std::collections::HashSet::new();
        let mut total = 0u64;
        if self.version != 1
            || self.backup != backup
            || self.tenant.id != tenant
            || self.files.is_empty()
            || self.files.len() > FILE_LIMIT
        {
            return Err(Error::corrupt("backup identity or format"));
        }
        for f in &self.files {
            if !valid_file(&f.path)
                || !names.insert(&f.path)
                || f.sha256.len() != 64
                || !f
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                return Err(Error::corrupt("backup file descriptor"));
            }
            total = total
                .checked_add(f.size)
                .ok_or_else(|| Error::corrupt("backup size overflow"))?;
        }
        if total != self.total_bytes || total > max_bytes {
            return Err(Error::limit("backup total byte limit"));
        }
        if !names.contains(&"CURRENT".to_string())
            || !names.contains(&"TENANT.meta".to_string())
            || !self.files.iter().any(|f| f.path.starts_with("MANIFEST-"))
            || !self.files.iter().any(|f| f.path.starts_with("wal/"))
        {
            return Err(Error::corrupt("incomplete backup file set"));
        }
        Ok(())
    }
}
pub(crate) fn valid_file(path: &str) -> bool {
    if matches!(path, "CURRENT" | "TENANT.meta") {
        return true;
    }
    let id = path
        .strip_prefix("sst/")
        .and_then(|p| p.strip_suffix(".sst"))
        .or_else(|| {
            path.strip_prefix("wal/")
                .and_then(|p| p.strip_suffix(".wal"))
        })
        .or_else(|| path.strip_prefix("MANIFEST-"));
    id.is_some_and(|s| Uuid::parse_str(s).is_ok_and(|u| u.to_string() == s))
}
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
