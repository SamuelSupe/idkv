use crate::{
    Error, Result,
    model::TenantInfo,
    storage::{atomic_json, read_json},
};
use parking_lot::{RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Primary,
    Replica,
    Fenced,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub node: Uuid,
    pub epoch: Uuid,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantPosition {
    pub info: TenantInfo,
    pub seq: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Fence {
    pub id: Uuid,
    pub cluster: Uuid,
    pub source: Source,
    pub target: Uuid,
    pub tenants: Vec<TenantPosition>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeState {
    pub version: u32,
    #[serde(default)]
    pub protocol: u32,
    pub node: Uuid,
    pub cluster: Uuid,
    pub epoch: Uuid,
    pub enabled: bool,
    pub role: Role,
    pub source: Option<Source>,
    pub fence: Option<Fence>,
    pub parent: Option<Fence>,
    pub forced: bool,
    pub log_retention_bytes: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub cluster: Uuid,
    pub role: Role,
    pub peer_url: String,
    pub token_file: PathBuf,
    #[serde(default = "default_poll")]
    pub poll_ms: u64,
    #[serde(default = "default_retention")]
    pub log_retention_bytes: u64,
}
fn default_poll() -> u64 {
    50
}
fn default_retention() -> u64 {
    256 << 20
}

pub(crate) struct Node {
    pub barrier: RwLock<()>,
    pub state: RwLock<NodeState>,
    path: PathBuf,
    poisoned: AtomicBool,
    pub retention: AtomicU64,
}
impl Node {
    pub fn open(root: &Path) -> Result<Arc<Self>> {
        let path = root.join("control/replication.json");
        let mut state: NodeState = if path.exists() {
            read_json(&path)?
        } else {
            let state = NodeState {
                version: 1,
                protocol: super::PROTOCOL,
                node: Uuid::new_v4(),
                cluster: Uuid::new_v4(),
                epoch: Uuid::new_v4(),
                enabled: false,
                role: Role::Primary,
                source: None,
                fence: None,
                parent: None,
                forced: false,
                log_retention_bytes: default_retention(),
            };
            atomic_json(&path, &state)?;
            state
        };
        if state.version != 1 {
            return Err(Error::corrupt("replication state version"));
        }
        state.protocol = super::PROTOCOL;
        Ok(Arc::new(Self {
            retention: AtomicU64::new(if state.enabled {
                state.log_retention_bytes
            } else {
                0
            }),
            barrier: RwLock::new(()),
            state: RwLock::new(state),
            path,
            poisoned: AtomicBool::new(false),
        }))
    }
    pub fn writable(&self) -> bool {
        !self.poisoned.load(Ordering::Acquire) && self.state.read().role == Role::Primary
    }
    pub fn write(&self) -> Result<RwLockReadGuard<'_, ()>> {
        let guard = self.barrier.read();
        if !self.writable() {
            return Err(Error::new(
                "READ_ONLY",
                "node is a replica or durably fenced",
            ));
        }
        Ok(guard)
    }
    pub fn replica_write(&self, source: &Source) -> Result<RwLockReadGuard<'_, ()>> {
        let guard = self.barrier.read();
        let state = self.state.read();
        if self.poisoned.load(Ordering::Acquire)
            || state.role != Role::Replica
            || state.source.as_ref() != Some(source)
        {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "replication source or role changed",
            ));
        }
        Ok(guard)
    }
    // Callers hold the exclusive barrier. A publication error fences this process;
    // reopening resolves the durable state instead of guessing whether rename won.
    pub fn save(&self, next: NodeState) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(Error::new(
                "UNAVAILABLE",
                "reopen store after uncertain role publication",
            ));
        }
        if let Err(error) = atomic_json(&self.path, &next) {
            self.poisoned.store(true, Ordering::Release);
            return Err(error);
        }
        self.retention.store(
            if next.enabled {
                next.log_retention_bytes
            } else {
                0
            },
            Ordering::Release,
        );
        *self.state.write() = next;
        Ok(())
    }
}
