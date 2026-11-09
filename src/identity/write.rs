use super::{storage::*, timeline::*, *};
use crate::WriteOptions;
use crate::storage::Transaction;
use crate::tenant::catalog::document_key;
use crate::tenant::retention::{self, Expiration};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeSet;

#[derive(Serialize, Deserialize)]
pub(super) struct StoredOperation {
    pub fingerprint: String,
    pub request: IdentityWrite,
    pub receipt: IdentityReceipt,
}
fn text(value: &str, max: usize, label: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > max || value.chars().any(char::is_control) {
        Err(Error::invalid(format!("invalid {label}")))
    } else {
        Ok(())
    }
}
fn validate_context(context: &IdentityContext) -> Result<()> {
    text(&context.source, 128, "source")?;
    text(&context.actor, 256, "actor")
}
fn expected(actual: Option<Uuid>, expected: &ExpectedRevision) -> Result<()> {
    if matches!((actual, expected), (None, ExpectedRevision::Absent))
        || matches!((actual,expected),(Some(a),ExpectedRevision::Revision(b)) if a==*b)
    {
        Ok(())
    } else {
        Err(Error::new(
            "REVISION_CONFLICT",
            "identity timeline revision changed",
        ))
    }
}
fn load_timeline(
    scratch: &mut Scratch<'_>,
    tx: &Transaction<'_>,
    collection: Uuid,
    id: &Identifier,
    revision: &ExpectedRevision,
) -> Result<(Timeline, bool)> {
    let old: Option<Timeline> = scratch.read(tx, collection, &timeline_key(id)?)?;
    if old.as_ref().is_some_and(|old| old.identifier != *id) {
        return Err(Error::corrupt("identifier digest collision"));
    }
    expected(old.as_ref().map(|t| t.revision), revision)?;
    let existed = old.is_some();
    Ok((
        old.unwrap_or_else(|| Timeline {
            identifier: id.clone(),
            revision: Uuid::new_v4(),
            segments: Vec::new(),
        }),
        existed,
    ))
}
fn entity(
    scratch: &mut Scratch<'_>,
    tx: &Transaction<'_>,
    collection: Uuid,
    id: Uuid,
) -> Result<Entity> {
    scratch
        .read(tx, collection, &entity_key(id))?
        .ok_or_else(|| Error::missing("identity entity"))
}
fn save_timeline(
    scratch: &mut Scratch<'_>,
    tx: &mut Transaction<'_>,
    collection: Uuid,
    timeline: &Timeline,
    ordinal: u64,
) -> Result<()> {
    super::subjects::update_links(scratch, tx, collection, timeline, ordinal)?;
    scratch.write(
        tx,
        collection,
        &timeline_key(&timeline.identifier)?,
        "timeline",
        timeline,
    )?;
    scratch.write(
        tx,
        collection,
        &format!(
            "history:{}:{:016x}",
            digest(&timeline.identifier)?,
            !ordinal
        ),
        "history",
        timeline,
    )
}
fn binding(
    entity_id: Uuid,
    from: i64,
    to: Option<i64>,
    evidence: &Option<String>,
    context: &IdentityContext,
) -> Binding {
    Binding {
        entity_id,
        valid_from_us: from,
        valid_to_us: to,
        binding_id: Uuid::new_v4(),
        version_id: Uuid::new_v4(),
        source: context.source.clone(),
        evidence_id: evidence.clone(),
        assurance: "asserted".into(),
    }
}
fn change_result(
    timeline: &Timeline,
    previous: Uuid,
    existed: bool,
    range: (i64, Option<i64>),
    replaced: Vec<Binding>,
) -> TimelineChange {
    let changed = timeline.revision != previous;
    TimelineChange {
        identifier: timeline.identifier.clone(),
        revision: if changed || existed {
            Some(timeline.revision)
        } else {
            None
        },
        changed,
        effective_from_us: range.0,
        effective_to_us: range.1,
        replaced,
    }
}
impl TenantHandle {
    /// Create the managed collection and genesis knowledge atomically. Repeating
    /// initialization with identical limits returns the existing head.
    pub fn identity_initialize(
        &self,
        context: &IdentityContext,
        config: IdentityConfig,
    ) -> Result<Knowledge> {
        validate_context(context)?;
        if !context.can_correct {
            return Err(Error::new(
                "FORBIDDEN",
                "identity initialization requires administrative context",
            ));
        }
        if config.max_segments == 0
            || config.max_segments > 1000
            || config.clock_tolerance_ms == 0
            || config.clock_tolerance_ms > 60_000
        {
            return Err(Error::invalid(
                "identity limits must be 1..1000 segments and 1..60000 ms clock tolerance",
            ));
        }
        let _permit = self.core.gate.enter()?;
        self.touch();
        self.identity_prepare()?;
        let mut scratch = Scratch::new(self);
        self.engine().transact(|tx| {
            let existing_collection = match collection_for_write(tx) {
                Ok(id) => Some(id),
                Err(e) if e.code == "NOT_FOUND" => None,
                Err(e) => return Err(e),
            };
            if let Some(collection) = existing_collection {
                let existing: IdentityConfig = scratch
                    .read(tx, collection, "head:config")?
                    .ok_or_else(|| {
                        Error::new("CONFLICT", "unmanaged collection occupies identity name")
                    })?;
                if existing != config {
                    return Err(Error::new(
                        "CONFLICT",
                        "identity configuration is immutable",
                    ));
                }
                return scratch
                    .read(tx, collection, "head:knowledge")?
                    .ok_or_else(|| Error::corrupt("identity knowledge head missing"));
            }
            // Only absence permits creation; corruption and storage errors must propagate.
            let collection = create_collection(tx)?;
            let recorded = self.identity_time(0, config.clock_tolerance_ms)?;
            let head = Knowledge {
                version: Uuid::new_v4(),
                ordinal: 0,
                previous: None,
                recorded_at_us: recorded,
                commit_seq: tx.seq,
                source: context.source.clone(),
                actor: context.actor.clone(),
            };
            scratch.write(tx, collection, "head:config", "config", &config)?;
            super::subjects::ensure_state(&mut scratch, tx, collection, &head, true)?;
            if retention::policy(tx)?.is_some() {
                crate::tenant::catalog::put(
                    tx,
                    retention::key(retention::KNOWLEDGE, &head.ordinal.to_be_bytes()),
                    &head.version,
                )?;
            }
            scratch.write(tx, collection, "head:knowledge", "head", &head)?;
            scratch.write(
                tx,
                collection,
                &format!("knowledge:{}", head.version),
                "knowledge",
                &head,
            )?;
            Ok(head)
        })
    }
    fn identity_prepare(&self) -> Result<()> {
        if self.engine().write_buffer_full() {
            self.engine().flush_if_needed()?;
        }
        if self.core.identity_clock.get().is_none() {
            let _ = self.core.identity_clock.set(ClockAnchor::new()?);
        }
        Ok(())
    }
    fn identity_time(&self, previous: i64, tolerance: u64) -> Result<i64> {
        self.core
            .identity_clock
            .get()
            .ok_or_else(|| Error::new("INTERNAL", "identity clock not initialized"))?
            .sample(previous, tolerance)
    }
    /// Commit one idempotent operation. Revisions, interval changes, history,
    /// knowledge and the receipt share one WAL batch and one durability barrier.
    pub fn identity_write(
        &self,
        context: &IdentityContext,
        request: IdentityWrite,
    ) -> Result<IdentityReceipt> {
        self.identity_write_with(context, request, &WriteOptions::default())
    }
    /// The original retry token/history must accompany every retry when retention is enabled.
    pub fn identity_write_with(
        &self,
        context: &IdentityContext,
        request: IdentityWrite,
        options: &WriteOptions,
    ) -> Result<IdentityReceipt> {
        validate_context(context)?;
        if options.operation_id.is_some() {
            return Err(Error::invalid(
                "identity operation_id belongs in the request",
            ));
        }
        text(request.operation_id(), 128, "operation_id")?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        let mut scratch = Scratch::new(self);
        let size = crate::json::encoded_len(&request)?;
        if size > self.engine().options.max_batch_bytes {
            return Err(Error::limit("identity request byte limit"));
        }
        scratch.charge(size.saturating_mul(8) + 4096)?;
        validate_request(
            &request,
            context,
            self.engine().options.max_batch_operations,
        )?;
        let mut fingerprint_value =
            json!({"request":request,"actor":context.actor,"source":context.source});
        if options.retry_token.is_some() || options.expected_history.is_some() {
            fingerprint_value["write_options"] = serde_json::to_value(options)?;
        }
        let fingerprint = digest(&fingerprint_value)?;
        self.identity_prepare()?;
        self.engine().transact(|tx| {
            self.identity_stage(tx, context, &request, options, &fingerprint, &mut scratch)
        })
    }
    fn identity_stage(
        &self,
        tx: &mut Transaction<'_>,
        context: &IdentityContext,
        request: &IdentityWrite,
        options: &WriteOptions,
        fingerprint: &str,
        scratch: &mut Scratch<'_>,
    ) -> Result<IdentityReceipt> {
        let collection = collection_for_write(tx)?;
        let config: IdentityConfig = scratch
            .read(tx, collection, "head:config")?
            .ok_or_else(|| Error::corrupt("identity config missing"))?;
        let receipt_key = operation_key(request.operation_id())?;
        if let Some(stored) = scratch.read::<StoredOperation>(tx, collection, &receipt_key)? {
            retention::check_tx(
                tx,
                &document_key(collection, &receipt_key),
                true,
                "IDEMPOTENCY_EXPIRED",
            )?;
            if stored.fingerprint != fingerprint {
                return Err(Error::new(
                    "OPERATION_CONFLICT",
                    "operation_id already used for a different request or actor",
                ));
            }
            scratch.response(&stored.receipt)?;
            return Ok(stored.receipt);
        }
        let history = self.history();
        if options
            .expected_history
            .as_ref()
            .is_some_and(|h| h != &history)
        {
            return Err(Error::new(
                "HISTORY_CONFLICT",
                "identity write belongs to another history",
            ));
        }
        retention::authorize_new(tx, options.retry_token, &context.actor, &history)?;
        let previous: Knowledge = scratch
            .read(tx, collection, "head:knowledge")?
            .ok_or_else(|| Error::corrupt("identity knowledge head missing"))?;
        let now = self.identity_time(previous.recorded_at_us, config.clock_tolerance_ms)?;
        let knowledge = Knowledge {
            version: Uuid::new_v4(),
            ordinal: previous
                .ordinal
                .checked_add(1)
                .ok_or_else(|| Error::limit("identity knowledge sequence exhausted"))?,
            previous: Some(previous.version),
            recorded_at_us: now,
            commit_seq: tx.seq,
            source: context.source.clone(),
            actor: context.actor.clone(),
        };
        let mut receipt = IdentityReceipt {
            operation_id: request.operation_id().into(),
            knowledge: knowledge.clone(),
            effective_time_us: now,
            entity: None,
            timelines: Vec::new(),
        };
        super::subjects::ensure_state(scratch, tx, collection, &knowledge, false)?;
        let old_attributes = match request {
            IdentityWrite::UpdateEntity { entity_id, .. } => {
                entity(scratch, tx, collection, *entity_id)?.attributes
            }
            _ => serde_json::Value::Null,
        };
        match &request {
            IdentityWrite::CreateEntity { attributes, .. } => {
                let entity = Entity {
                    entity_id: Uuid::new_v4(),
                    revision: Uuid::new_v4(),
                    bindings_revision: Uuid::new_v4(),
                    attributes: attributes.clone(),
                };
                scratch.write(
                    tx,
                    collection,
                    &entity_key(entity.entity_id),
                    "entity",
                    &entity,
                )?;
                receipt.entity = Some(entity);
            }
            IdentityWrite::UpdateEntity {
                entity_id,
                expected_revision,
                attributes,
                ..
            } => {
                let mut entity = entity(scratch, tx, collection, *entity_id)?;
                if entity.revision != *expected_revision {
                    return Err(Error::new(
                        "REVISION_CONFLICT",
                        "entity attribute revision changed",
                    ));
                }
                entity
                    .attributes
                    .as_object_mut()
                    .ok_or_else(|| Error::corrupt("entity attributes"))?
                    .extend(attributes.as_object().unwrap().clone());
                entity.revision = Uuid::new_v4();
                scratch.write(
                    tx,
                    collection,
                    &entity_key(entity.entity_id),
                    "entity",
                    &entity,
                )?;
                receipt.entity = Some(entity);
            }
            IdentityWrite::Bindings { changes, .. } => {
                let mut affected = BTreeSet::new();
                for change in changes {
                    scratch.check()?;
                    let (mut timeline, existed) = load_timeline(
                        scratch,
                        tx,
                        collection,
                        &change.identifier,
                        &change.expected_revision,
                    )?;
                    let previous = timeline.revision;
                    let mut target = if let BindingAction::ReplaceIdentifier {
                        replacement,
                        replacement_expected_revision,
                        ..
                    } = &change.change
                    {
                        Some(load_timeline(
                            scratch,
                            tx,
                            collection,
                            replacement,
                            replacement_expected_revision,
                        )?)
                    } else {
                        None
                    };
                    let (from, to) = match &change.effective {
                        Effective::Auto {} => {
                            let next = next_boundary(&timeline, now);
                            let next_target =
                                target.as_ref().and_then(|(t, _)| next_boundary(t, now));
                            (now, next.into_iter().chain(next_target).min())
                        }
                        Effective::Manual { from, to } => (
                            parse_time(from)?,
                            to.as_deref().map(parse_time).transpose()?,
                        ),
                    };
                    let (replacement, exclusive) = match &change.change {
                        BindingAction::Bind {
                            entity_id,
                            evidence_id,
                            ..
                        }
                        | BindingAction::Rebind {
                            entity_id,
                            evidence_id,
                            ..
                        } => {
                            entity(scratch, tx, collection, *entity_id)?;
                            (
                                Some(binding(*entity_id, from, to, evidence_id, context)),
                                matches!(change.change, BindingAction::Bind { .. }),
                            )
                        }
                        BindingAction::Unbind { .. } => (None, false),
                        BindingAction::ReplaceIdentifier {
                            entity_id,
                            evidence_id,
                            ..
                        } => {
                            entity(scratch, tx, collection, *entity_id)?;
                            if !covered_by(&timeline, from, to, *entity_id) {
                                return Err(Error::new(
                                    "INTERVAL_CONFLICT",
                                    "old identifier must belong to this entity throughout replacement interval",
                                ));
                            }
                            let (target, target_existed) = target.as_mut().unwrap();
                            let old = target.revision;
                            let replaced = replace(
                                target,
                                from,
                                to,
                                Some(binding(*entity_id, from, to, evidence_id, context)),
                                true,
                                config.max_segments,
                            )?;
                            if old != target.revision {
                                affected.insert(*entity_id);
                            }
                            if old != target.revision {
                                save_timeline(scratch, tx, collection, target, knowledge.ordinal)?;
                            }
                            receipt.timelines.push(change_result(
                                target,
                                old,
                                *target_existed,
                                (from, to),
                                replaced,
                            ));
                            (None, false)
                        }
                    };
                    let new_entity = replacement.as_ref().map(|b| b.entity_id);
                    let replaced = replace(
                        &mut timeline,
                        from,
                        to,
                        replacement,
                        exclusive,
                        config.max_segments,
                    )?;
                    if timeline.revision != previous {
                        affected.extend(replaced.iter().map(|b| b.entity_id));
                        affected.extend(new_entity);
                    }
                    if timeline.revision != previous {
                        save_timeline(scratch, tx, collection, &timeline, knowledge.ordinal)?;
                    }
                    receipt.timelines.push(change_result(
                        &timeline,
                        previous,
                        existed,
                        (from, to),
                        replaced,
                    ));
                }
                for id in affected {
                    let mut entity = entity(scratch, tx, collection, id)?;
                    entity.bindings_revision = Uuid::new_v4();
                    scratch.write(tx, collection, &entity_key(id), "entity", &entity)?;
                }
            }
        }
        super::subjects::record_operation(
            scratch,
            tx,
            collection,
            request,
            &receipt,
            &old_attributes,
        )?;
        scratch.response(&receipt)?;
        retention::schedule(
            tx,
            &document_key(collection, &format!("knowledge:{}", previous.version)),
            Expiration::Knowledge {
                ordinal: previous.ordinal,
            },
        )?;
        if retention::policy(tx)?.is_some() {
            crate::tenant::catalog::put(
                tx,
                retention::key(retention::KNOWLEDGE, &knowledge.ordinal.to_be_bytes()),
                &knowledge.version,
            )?;
        }
        scratch.write(tx, collection, "head:knowledge", "head", &knowledge)?;
        scratch.write(
            tx,
            collection,
            &format!("knowledge:{}", knowledge.version),
            "knowledge",
            &knowledge,
        )?;
        scratch.write(
            tx,
            collection,
            &receipt_key,
            "operation",
            &json!({"fingerprint":fingerprint,"request":request,"receipt":receipt}),
        )?;
        retention::schedule_receipt(
            tx,
            &document_key(collection, &receipt_key),
            options.retry_token,
        )?;
        tx.check_expanded_limits()?;
        Ok(receipt)
    }
}

fn validate_request(request: &IdentityWrite, context: &IdentityContext, max: usize) -> Result<()> {
    match request {
        IdentityWrite::CreateEntity { attributes, .. }
        | IdentityWrite::UpdateEntity { attributes, .. } => {
            if !attributes.is_object() {
                return Err(Error::invalid("entity attributes must be an object"));
            }
        }
        IdentityWrite::Bindings { changes, .. } => {
            if changes.is_empty() || changes.len() > max {
                return Err(Error::limit("identity change count limit"));
            }
            let mut seen = BTreeSet::new();
            for change in changes {
                validate_identifier(&change.identifier)?;
                if !seen.insert(digest(&change.identifier)?) {
                    return Err(Error::invalid(
                        "batch modifies an identifier more than once",
                    ));
                }
                let (reason, evidence) = match &change.change {
                    BindingAction::Bind {
                        evidence_id,
                        reason,
                        ..
                    } => (reason.as_ref(), evidence_id),
                    BindingAction::Rebind {
                        reason,
                        evidence_id,
                        ..
                    } => (Some(reason), evidence_id),
                    BindingAction::Unbind { reason } => (Some(reason), &None),
                    BindingAction::ReplaceIdentifier {
                        replacement,
                        reason,
                        evidence_id,
                        ..
                    } => {
                        validate_identifier(replacement)?;
                        if !seen.insert(digest(replacement)?) {
                            return Err(Error::invalid(
                                "batch modifies an identifier more than once",
                            ));
                        }
                        (Some(reason), evidence_id)
                    }
                };
                if let Some(reason) = reason {
                    text(reason, 2048, "correction reason")?;
                }
                if let Some(evidence) = evidence {
                    text(evidence, 256, "evidence reference")?;
                }
                if reason.is_some() && !context.can_correct {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "identity correction requires administrative context",
                    ));
                }
                if let Effective::Manual { from, to } = &change.effective {
                    if !context.can_correct {
                        return Err(Error::new(
                            "FORBIDDEN",
                            "manual effective time requires correction permission",
                        ));
                    }
                    if reason.is_none() {
                        return Err(Error::invalid("manual effective time requires a reason"));
                    }
                    let from = parse_time(from)?;
                    let to = to.as_deref().map(parse_time).transpose()?;
                    if to.is_some_and(|to| to <= from) {
                        return Err(Error::invalid("effective interval must have from < to"));
                    }
                }
            }
        }
    }
    Ok(())
}
