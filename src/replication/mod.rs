//! Single-source replication. Roles are durable and promotion is explicit;
//! replication does not provide an election or fence an unreachable machine.
mod bootstrap;
mod client;
mod node;
mod snapshot;
pub use client::Service;
pub(crate) use node::Node;
pub use node::{Config, Fence, NodeState, Role, Source, TenantPosition};
pub(crate) use snapshot::Exports;

pub const PAGE_BYTES: usize = 1 << 20;
pub const MAX_WIRE_BYTES: usize = 96 << 20;
pub const PROTOCOL: u32 = 3;
pub fn capabilities() -> serde_json::Value {
    serde_json::json!({"version":env!("CARGO_PKG_VERSION"), "replication_protocol":PROTOCOL, "storage_registry":4,
        "features":["request_read_controls_v1", "batch_checks", "snapshot_read_batch", "immutable_dataset_reads", "tenant_retention_v1", "history_cas", "durable_write_receipts", "immutable_lookup_versions", "identity_temporal_v1", "prometheus", "otlp_traces", "otlp_logs"]})
}
pub(crate) fn compatible(protocol: u32) -> crate::Result<()> {
    if protocol != PROTOCOL {
        return Err(crate::Error::new(
            "INCOMPATIBLE_VERSION",
            "replication protocol mismatch; upgrade both nodes before resuming writes",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogRequest {
    #[serde(default)]
    pub protocol: u32,
    pub cluster: uuid::Uuid,
    pub epoch: uuid::Uuid,
    pub tenant: uuid::Uuid,
    pub generation: uuid::Uuid,
    pub after: u64,
    #[serde(default)]
    pub cursor: Option<LogCursor>,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogCursor {
    pub file: uuid::Uuid,
    pub offset: u64,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct LogPage {
    pub epoch: uuid::Uuid,
    pub through: u64,
    pub source_position: u64,
    pub frames: Vec<String>,
    pub cursor: Option<LogCursor>,
}

impl crate::TenantHandle {
    /// Wait for this node to apply a transaction from the specified source epoch.
    /// This guarantees read-your-writes, not global freshness. A role/history
    /// change invalidates the token; waits are bounded to five seconds.
    pub fn wait_replication(
        &self,
        epoch: uuid::Uuid,
        seq: u64,
        timeout_ms: u64,
    ) -> crate::Result<u64> {
        if timeout_ms > 5000 {
            return Err(crate::Error::invalid(
                "replication wait exceeds five seconds",
            ));
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        loop {
            if self.engine().replication_epoch() != Some(epoch) {
                return Err(crate::Error::new(
                    "REPLICATION_CONFLICT",
                    "read token belongs to another replication history",
                ));
            }
            let applied = self.engine().seq();
            if applied >= seq {
                return Ok(applied);
            }
            if std::time::Instant::now() >= deadline {
                return Err(crate::Error::new(
                    "REPLICA_LAG",
                    "replica has not applied the requested transaction",
                ));
            }
            self.engine().wait_progress(
                applied,
                deadline.saturating_duration_since(std::time::Instant::now()),
            );
        }
    }
}
