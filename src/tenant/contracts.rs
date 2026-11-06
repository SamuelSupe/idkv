use super::TenantHandle;
use crate::{Error, ReadContext, Result, WriteOptions, WriteResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(super) fn history_token(info: &crate::TenantInfo, epoch: Option<uuid::Uuid>) -> String {
    format!(
        "{}:{}:{}",
        info.id,
        if info.history.is_nil() {
            info.generation
        } else {
            info.history
        },
        epoch.unwrap_or_default()
    )
}

#[derive(Serialize, Deserialize)]
pub(super) struct Receipt {
    pub digest: String,
    pub results: Vec<WriteResult>,
}

pub(super) fn digest(value: &impl Serialize) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}

impl TenantHandle {
    /// Pair this token with `_rev`. The token changes on restore or role-history
    /// change; numeric revisions alone are not safe across those boundaries.
    pub fn history(&self) -> String {
        history_token(&self.engine().info, self.engine().replication_epoch())
    }

    pub fn read_context(&self) -> ReadContext {
        loop {
            let history = self.history();
            let applied_seq = self.engine().seq();
            if history == self.history() {
                return ReadContext {
                    tenant: self.id(),
                    history,
                    applied_seq,
                    writable: self.engine().writable(),
                };
            }
        }
    }

    pub fn get_with_context(
        &self,
        collection: &str,
        key: &str,
    ) -> Result<(Option<serde_json::Value>, ReadContext)> {
        let context = self.read_context();
        let value = self.get(collection, key)?;
        if context.history != self.history() {
            return Err(Error::new(
                "HISTORY_CONFLICT",
                "read crossed a history change",
            ));
        }
        Ok((value, context))
    }

    /// Waits only within the requested history; it never translates a stale
    /// sequence into the new primary's unrelated history.
    pub fn wait_context(&self, history: &str, seq: u64, timeout_ms: u64) -> Result<()> {
        if timeout_ms > 5000 {
            return Err(Error::invalid("consistency wait exceeds five seconds"));
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        loop {
            if self.history() != history {
                return Err(Error::new(
                    "HISTORY_CONFLICT",
                    "read token belongs to another history",
                ));
            }
            let applied = self.engine().seq();
            if applied >= seq {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(Error::new("REPLICA_LAG", "read consistency deadline"));
            }
            self.engine().wait_progress(
                applied,
                deadline.saturating_duration_since(std::time::Instant::now()),
            );
        }
    }

    pub(super) fn check_write_options(
        &self,
        options: &WriteOptions,
        conditional: bool,
    ) -> Result<String> {
        let history = self.history();
        if options.expected_history.is_none() && (conditional || options.operation_id.is_some()) {
            return Err(Error::new(
                "HISTORY_REQUIRED",
                "conditional writes and retry IDs require expected_history",
            ));
        }
        if options
            .expected_history
            .as_ref()
            .is_some_and(|h| h != &history)
        {
            return Err(Error::new(
                "HISTORY_CONFLICT",
                "database history changed; re-read before writing",
            ));
        }
        Ok(history)
    }
}
