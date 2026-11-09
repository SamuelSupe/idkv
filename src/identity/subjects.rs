use super::{storage::*, *};
use crate::{
    storage::Transaction,
    tenant::{
        catalog::document_key,
        retention::{self, Expiration},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) const STATE: &str = "head:subjects";

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct SubjectState {
    pub ready: bool,
    pub recording_started: Knowledge,
    pub legacy: bool,
    pub after: Option<Vec<u8>>,
    pub pending: Option<Backfill>,
    pub previous: Option<Membership>,
    pub gc_after: Option<Vec<u8>>,
    pub gc_pair: String,
    pub gc_baseline: bool,
    pub gc_floor: u64,
    pub swept_floor: u64,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Membership {
    pub identifier: Identifier,
    pub ordinal: u64,
    pub entities: BTreeSet<Uuid>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Backfill {
    pub key: Vec<u8>,
    pub row: Membership,
    pub offset: usize,
}
#[derive(Serialize, Deserialize)]
pub(super) struct SubjectLink {
    pub identifier: Identifier,
    pub present: bool,
}

pub(super) fn ensure_state(
    scratch: &mut Scratch<'_>,
    tx: &mut Transaction<'_>,
    collection: Uuid,
    head: &Knowledge,
    fresh: bool,
) -> Result<SubjectState> {
    if let Some(state) = scratch.read(tx, collection, STATE)? {
        return Ok(state);
    }
    let state = SubjectState {
        ready: fresh,
        recording_started: head.clone(),
        legacy: !fresh,
        after: None,
        pending: None,
        previous: None,
        gc_after: None,
        gc_pair: String::new(),
        gc_baseline: false,
        gc_floor: 0,
        swept_floor: 0,
    };
    scratch.write(tx, collection, STATE, "subject_state", &state)?;
    Ok(state)
}
pub(super) fn entities(timeline: &Timeline) -> BTreeSet<Uuid> {
    timeline.segments.iter().map(|b| b.entity_id).collect()
}
pub(super) fn link(
    scratch: &mut Scratch<'_>,
    tx: &mut Transaction<'_>,
    collection: Uuid,
    entity: Uuid,
    row: &Membership,
    present: bool,
) -> Result<()> {
    let key = format!(
        "subject:{entity}:{}:{:016x}",
        digest(&row.identifier)?,
        !row.ordinal
    );
    scratch.write(
        tx,
        collection,
        &key,
        "subject_link",
        &SubjectLink {
            identifier: row.identifier.clone(),
            present,
        },
    )
}
pub(super) fn update_links(
    scratch: &mut Scratch<'_>,
    tx: &mut Transaction<'_>,
    collection: Uuid,
    timeline: &Timeline,
    ordinal: u64,
) -> Result<()> {
    let old: Option<Timeline> =
        scratch.read(tx, collection, &timeline_key(&timeline.identifier)?)?;
    let before = old.as_ref().map(entities).unwrap_or_default();
    let after = entities(timeline);
    let row = Membership {
        identifier: timeline.identifier.clone(),
        ordinal,
        entities: after,
    };
    for id in before.symmetric_difference(&row.entities) {
        link(
            scratch,
            tx,
            collection,
            *id,
            &row,
            row.entities.contains(id),
        )?;
    }
    Ok(())
}
fn attribute_changes(before: &Value, after: &Value) -> Vec<AttributeChange> {
    let fields: BTreeSet<_> = before
        .as_object()
        .into_iter()
        .flat_map(|v| v.keys())
        .chain(after.as_object().into_iter().flat_map(|v| v.keys()))
        .collect();
    fields
        .into_iter()
        .filter_map(|field| {
            let old = before.get(field);
            let new = after.get(field);
            (old != new).then(|| AttributeChange {
                field: field.clone(),
                before: AttributeValue {
                    present: old.is_some(),
                    value: old.cloned().unwrap_or(Value::Null),
                },
                after: AttributeValue {
                    present: new.is_some(),
                    value: new.cloned().unwrap_or(Value::Null),
                },
            })
        })
        .collect()
}
fn clipped(segments: &[Binding], from: i64, to: Option<i64>) -> Vec<Binding> {
    segments
        .iter()
        .filter(|b| timeline::overlaps(b, from, to))
        .map(|b| {
            let mut b = b.clone();
            b.valid_from_us = b.valid_from_us.max(from);
            b.valid_to_us = b.valid_to_us.into_iter().chain(to).min();
            b
        })
        .collect()
}
fn save_event(
    scratch: &mut Scratch<'_>,
    tx: &mut Transaction<'_>,
    collection: Uuid,
    event: &IdentityEvent,
) -> Result<()> {
    let suffix = format!(
        "{:016x}:{:08x}",
        !event.knowledge.ordinal, event.change_index
    );
    let key = format!("event:{suffix}");
    scratch.write(tx, collection, &key, "identity_event", event)?;
    retention::schedule(tx, &document_key(collection, &key), Expiration::Delete)?;
    for id in &event.entity_ids {
        let pointer = format!("subject-event:{id}:{suffix}");
        scratch.write(tx, collection, &pointer, "event_pointer", &key)?;
        // Independent jobs keep deletion bounded even for a correction affecting
        // hundreds of former owners. A missing expired payload is skipped on read.
        retention::schedule(tx, &document_key(collection, &pointer), Expiration::Delete)?;
    }
    Ok(())
}
pub(super) fn record_operation(
    scratch: &mut Scratch<'_>,
    tx: &mut Transaction<'_>,
    collection: Uuid,
    request: &IdentityWrite,
    receipt: &IdentityReceipt,
    old_attributes: &Value,
) -> Result<()> {
    let mut event = IdentityEvent {
        event_id: Uuid::new_v4(),
        operation_id: receipt.operation_id.clone(),
        knowledge: receipt.knowledge.clone(),
        change_index: 0,
        kind: IdentityEventKind::EntityCreated,
        entity_ids: Vec::new(),
        reason: None,
        changes: Vec::new(),
        attributes: Vec::new(),
    };
    match request {
        IdentityWrite::CreateEntity { .. } | IdentityWrite::UpdateEntity { .. } => {
            let entity = receipt.entity.as_ref().unwrap();
            event.entity_ids.push(entity.entity_id);
            event.attributes = attribute_changes(old_attributes, &entity.attributes);
            if matches!(request, IdentityWrite::UpdateEntity { .. }) {
                if event.attributes.is_empty() {
                    return Ok(());
                }
                event.kind = IdentityEventKind::AttributesUpdated;
            }
            save_event(scratch, tx, collection, &event)
        }
        IdentityWrite::Bindings { changes, .. } => {
            for (index, change) in changes.iter().enumerate() {
                event.event_id = Uuid::new_v4();
                event.change_index = index;
                event.changes.clear();
                let (kind, reason, replacement) = match &change.change {
                    BindingAction::Bind { reason, .. } => {
                        (IdentityEventKind::IdentifierBound, reason.clone(), None)
                    }
                    BindingAction::Rebind { reason, .. } => (
                        IdentityEventKind::IdentifierRebound,
                        Some(reason.clone()),
                        None,
                    ),
                    BindingAction::Unbind { reason } => (
                        IdentityEventKind::IdentifierUnbound,
                        Some(reason.clone()),
                        None,
                    ),
                    BindingAction::ReplaceIdentifier {
                        reason,
                        replacement,
                        ..
                    } => (
                        IdentityEventKind::IdentifierReplaced,
                        Some(reason.clone()),
                        Some(replacement),
                    ),
                };
                event.kind = kind;
                event.reason = reason;
                let mut affected = BTreeSet::new();
                for result in receipt.timelines.iter().filter(|r| {
                    r.changed
                        && (r.identifier == change.identifier || replacement == Some(&r.identifier))
                }) {
                    let timeline: Timeline = scratch
                        .read(tx, collection, &timeline_key(&result.identifier)?)?
                        .ok_or_else(|| Error::corrupt("changed timeline missing"))?;
                    let before = clipped(
                        &result.replaced,
                        result.effective_from_us,
                        result.effective_to_us,
                    );
                    let after = clipped(
                        &timeline.segments,
                        result.effective_from_us,
                        result.effective_to_us,
                    );
                    affected.extend(before.iter().chain(&after).map(|b| b.entity_id));
                    event.changes.push(IdentifierChange {
                        identifier: result.identifier.clone(),
                        timeline_revision: timeline.revision,
                        effective_from_us: result.effective_from_us,
                        effective_to_us: result.effective_to_us,
                        before,
                        after,
                    });
                }
                if !event.changes.is_empty() {
                    event.entity_ids = affected.into_iter().collect();
                    save_event(scratch, tx, collection, &event)?;
                }
            }
            Ok(())
        }
    }
}
