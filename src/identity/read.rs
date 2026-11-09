use super::{storage::*, timeline::validate_identifier, write::StoredOperation, *};
use crate::ReadStats;
impl TenantHandle {
    pub fn identity_entity(&self, id: Uuid) -> Result<Entity> {
        let _permit = self.core.gate.enter()?;
        self.touch();
        let snapshot = self.engine().snapshot()?;
        let mut scratch = Scratch::new(self);
        let collection = self.collection_id(&snapshot, COLLECTION)?;
        let entity = scratch
            .get(
                &snapshot,
                collection,
                &entity_key(id),
                &mut ReadStats::default(),
            )?
            .ok_or_else(|| Error::missing("identity entity"))?;
        scratch.response(&entity)?;
        Ok(entity)
    }
    /// Read the latest timeline revision for optimistic updates, using a single snapshot.
    pub fn identity_timeline(&self, id: &Identifier) -> Result<Option<Timeline>> {
        validate_identifier(id)?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        let snapshot = self.engine().snapshot()?;
        let mut scratch = Scratch::new(self);
        let collection = self.collection_id(&snapshot, COLLECTION)?;
        let timeline: Option<Timeline> = scratch.get_typed(
            &snapshot,
            collection,
            &timeline_key(id)?,
            &mut ReadStats::default(),
        )?;
        if timeline.as_ref().is_some_and(|t| t.identifier != *id) {
            return Err(Error::corrupt("identifier digest collision"));
        }
        scratch.response(&timeline)?;
        Ok(timeline)
    }
    pub fn identity_operation(&self, id: &str) -> Result<IdentityReceipt> {
        if id.is_empty() || id.len() > 128 {
            return Err(Error::invalid("operation_id must contain 1..128 bytes"));
        }
        let _permit = self.core.gate.enter()?;
        self.touch();
        let snapshot = self.engine().snapshot()?;
        let mut scratch = Scratch::new(self);
        let collection = self.collection_id(&snapshot, COLLECTION)?;
        let stored: StoredOperation = scratch
            .get(
                &snapshot,
                collection,
                &operation_key(id)?,
                &mut ReadStats::default(),
            )?
            .ok_or_else(|| Error::missing("identity operation"))?;
        crate::tenant::retention::check_snapshot(
            &snapshot,
            &crate::tenant::catalog::document_key(collection, &operation_key(id)?),
            true,
            "IDEMPOTENCY_EXPIRED",
        )?;
        scratch.response(&stored.receipt)?;
        Ok(stored.receipt)
    }
}
