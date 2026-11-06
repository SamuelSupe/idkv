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
    /// Resolve one exclusive identifier at event time, under current or a persistent
    /// knowledge UUID. Unknown versions fail instead of silently falling back.
    pub fn identity_resolve(&self, request: &ResolveRequest) -> Result<ResolveResult> {
        validate_identifier(&request.identifier)?;
        let at = parse_time(&request.valid_at)?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        let snapshot = self.engine().snapshot()?;
        let mut scratch = Scratch::new(self);
        let collection = self.collection_id(&snapshot, COLLECTION)?;
        let mut stats = ReadStats::default();
        let known = request.known_at.as_deref().filter(|v| *v != "latest");
        let head: Knowledge = match known {
            None => scratch
                .get_typed(&snapshot, collection, "head:knowledge", &mut stats)?
                .ok_or_else(|| Error::corrupt("identity knowledge head missing"))?,
            Some(version) => {
                let version = Uuid::parse_str(version)
                    .map_err(|_| Error::invalid("known_at must be latest or a knowledge UUID"))?;
                scratch.get_typed(&snapshot,collection,&format!("knowledge:{version}"),&mut stats)?.ok_or_else(||Error::new("KNOWLEDGE_UNAVAILABLE","knowledge version absent from this tenant history or not yet replicated"))?
            }
        };
        if known.is_some() {
            let current: Knowledge = scratch
                .get_typed(&snapshot, collection, "head:knowledge", &mut stats)?
                .ok_or_else(|| Error::corrupt("knowledge head missing"))?;
            if head.version != current.version {
                crate::tenant::retention::check_snapshot(
                    &snapshot,
                    &crate::tenant::catalog::document_key(
                        collection,
                        &format!("knowledge:{}", head.version),
                    ),
                    true,
                    "KNOWLEDGE_UNAVAILABLE",
                )?;
            }
        }
        let timeline: Option<Timeline> = if known.is_some() {
            scratch.history(
                &snapshot,
                collection,
                &request.identifier,
                head.ordinal,
                &mut stats,
            )?
        } else {
            scratch.get_typed(
                &snapshot,
                collection,
                &timeline_key(&request.identifier)?,
                &mut stats,
            )?
        };
        if timeline
            .as_ref()
            .is_some_and(|t| t.identifier != request.identifier)
        {
            return Err(Error::corrupt("identifier digest collision"));
        }
        let binding = timeline.as_ref().and_then(|t| {
            let pos = t.segments.partition_point(|b| b.valid_from_us <= at);
            pos.checked_sub(1)
                .and_then(|i| t.segments.get(i))
                .filter(|b| b.valid_to_us.is_none_or(|to| at < to))
                .cloned()
        });
        let result = ResolveResult {
            knowledge_version: head.version,
            timeline_revision: timeline.map(|t| t.revision),
            binding,
            stats,
        };
        scratch.response(&result)?;
        Ok(result)
    }
}
