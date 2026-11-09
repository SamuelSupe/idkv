//! Managed exclusive temporal identities. All writes use one tenant WAL transaction.
//! Historical knowledge and receipts use durable tenant retention, not MVCC leases.
mod model;
mod read;
mod resolve;
mod storage;
mod subject_maintenance;
mod subject_model;
mod subject_read;
#[cfg(test)]
mod subject_stress;
#[cfg(test)]
mod subject_tests;
mod subjects;
mod time;
mod timeline;
mod write;
use crate::{Error, Result, TenantHandle};
pub use model::*;
pub(crate) use subject_maintenance::subject_history_gc_limit;
pub use subject_model::*;
pub use subjects::SubjectMaintenanceProgress;
pub(crate) use time::ClockAnchor;
pub use time::parse_time;
use uuid::Uuid;

pub const COLLECTION: &str = "identity_temporal";
pub(crate) fn unmanaged(collection: &str) -> Result<()> {
    if collection == COLLECTION {
        Err(Error::new(
            "MANAGED_COLLECTION",
            "use the identity API for this managed collection",
        ))
    } else {
        Ok(())
    }
}
