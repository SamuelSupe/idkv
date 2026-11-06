pub(crate) mod catalog;
mod execute;
mod filter;
mod lookup;
mod planner;

use crate::{model::*, storage::Snapshot};
use uuid::Uuid;

pub(crate) struct Cursor {
    pub(crate) snapshot: Snapshot,
    pub(crate) publication: Option<(String, Uuid)>,
    replication_epoch: Option<Uuid>,
    fingerprint: Vec<u8>,
    after: Vec<u8>,
    position: Vec<u8>,
    collection: Uuid,
    _memory: crate::resources::Reservation,
}
impl Cursor {
    pub fn valid(&self) -> bool {
        self.snapshot.check().is_ok() && self.snapshot.replication_epoch() == self.replication_epoch
    }
}
pub(crate) struct LookupPlan {
    pub _memory: crate::resources::Reservation,
    definition: LookupDefinition,
    index: IndexDefinition,
    catalog_seq: u64,
}
