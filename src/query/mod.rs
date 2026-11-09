pub(crate) mod catalog;
mod execute;
mod filter;
mod lookup;
mod planner;

use crate::{model::*, storage::Snapshot};
use uuid::Uuid;

pub(crate) struct Cursor {
    pub(crate) principal: Option<std::sync::Arc<str>>,
    pub(crate) snapshot: Snapshot,
    pub(crate) publication: Option<(String, Uuid)>,
    pub(crate) replication_epoch: Option<Uuid>,
    pub(crate) fingerprint: Vec<u8>,
    pub(crate) after: Vec<u8>,
    pub(crate) position: Vec<u8>,
    pub(crate) collection: Uuid,
    pub(crate) identity: Option<(crate::identity::Knowledge, String)>,
    pub(crate) _memory: crate::resources::Reservation,
}
impl crate::TenantHandle {
    pub(crate) fn publish_cursor(
        &self,
        old: Option<&str>,
        next: Option<Cursor>,
        budget: &crate::read::ReadBudget,
    ) -> crate::Result<Option<String>> {
        let mut cursors = self.core.cursors.lock();
        cursors.retain(|_, c| c.valid());
        let old = old
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|_| crate::Error::new("CURSOR_EXPIRED", "invalid cursor"))?;
        if old.is_some_and(|id| !cursors.contains_key(&id)) {
            return Err(crate::Error::new(
                "CURSOR_EXPIRED",
                "cursor expired or already consumed",
            ));
        }
        if next.is_some()
            && old.is_none()
            && (cursors.len() >= self.engine().options.max_snapshots / 2
                || cursors
                    .values()
                    .filter(|c| c.principal == budget.principal)
                    .count()
                    >= (self.engine().options.max_snapshots / 4).max(1))
        {
            return Err(crate::Error::limit("cursor count limit"));
        }
        if let Some(id) = old {
            cursors.remove(&id);
        }
        Ok(next.map(|cursor| {
            let id = Uuid::new_v4();
            cursors.insert(id, cursor);
            id.to_string()
        }))
    }
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
