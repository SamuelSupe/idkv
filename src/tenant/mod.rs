pub(crate) mod catalog;
mod contracts;
pub(crate) mod retention;
pub use retention::{RetentionPolicy, RetryContext};
pub(crate) mod dataset_reads;
pub(crate) mod datasets;
mod delta;
mod documents;
pub(crate) mod indexes;
mod manager;
mod reads;

use crate::{model::*, resources::RequestGate, storage::Engine};
pub use manager::Store;
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};
use uuid::Uuid;

pub(crate) struct TenantCore {
    pub identity_clock: std::sync::OnceLock<crate::identity::ClockAnchor>,
    pub collection_id: parking_lot::RwLock<Option<catalog::CachedCollection>>,
    pub query_catalog: parking_lot::RwLock<crate::query::catalog::CatalogCache>,
    pub dsl: crate::dsl::Cache,
    pub engine: Arc<Engine>,
    pub gate: RequestGate,
    pub last_used: Mutex<Instant>,
    pub cursors: Mutex<HashMap<Uuid, crate::query::Cursor>>,
    pub plans: Mutex<HashMap<(Uuid, String), Arc<crate::query::LookupPlan>>>,
    pub builds: Mutex<HashSet<Uuid>>,
}
#[derive(Clone)]
pub struct TenantHandle {
    pub(crate) core: Arc<TenantCore>,
}
impl TenantHandle {
    pub fn id(&self) -> Uuid {
        self.core.engine.info.id
    }
    pub fn info(&self) -> TenantInfo {
        self.core.engine.info.clone()
    }
    pub fn engine(&self) -> &Arc<Engine> {
        &self.core.engine
    }
    pub(crate) fn touch(&self) {
        *self.core.last_used.lock() = Instant::now();
    }
    pub fn release_cursor(&self, token: &str) -> crate::Result<()> {
        let id = Uuid::parse_str(token).map_err(|_| crate::Error::invalid("cursor UUID"))?;
        self.core.cursors.lock().remove(&id);
        Ok(())
    }
}
