//! Managed exclusive temporal identities. All writes use one tenant WAL transaction.
//! Historical knowledge and receipts use durable tenant retention, not MVCC leases.
mod model;
mod read;
mod storage;
mod time;
mod timeline;
mod write;
use crate::{Error, Result, TenantHandle};
pub use model::*;
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
