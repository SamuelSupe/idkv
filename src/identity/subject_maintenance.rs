use super::{storage::*, subjects::*, *};
use crate::{codec, storage::Transaction, tenant::catalog::document_key};

pub(super) fn prefix(collection: Uuid, text: &str) -> Vec<u8> {
    let mut key = document_key(collection, text);
    key.truncate(key.len() - 2);
    key
}
pub(crate) fn subjects_ready(tx: &Transaction<'_>, collection: Uuid) -> Result<bool> {
    Ok(tx
        .get(&document_key(collection, STATE))?
        .map(|b| codec::field_as::<SubjectState>(&b, "/data"))
        .transpose()?
        .flatten()
        .is_some_and(|s| s.ready))
}
fn migrate(
    scratch: &mut Scratch<'_>,
    tx: &mut Transaction<'_>,
    collection: Uuid,
    state: &mut SubjectState,
) -> Result<()> {
    tx.require_foreground_disk();
    if state.pending.is_none() {
        let (rows, exhausted) =
            tx.gc_prefix(&prefix(collection, "history:"), state.after.as_deref(), 1)?;
        for record in rows {
            state.after = Some(record.key.clone());
            if let Some(bytes) = record.value {
                let timeline: Timeline = scratch.decode(&bytes)?;
                let text =
                    std::str::from_utf8(&record.key[record.key.len() - 18..record.key.len() - 2])
                        .map_err(|_| Error::corrupt("identity history ordinal"))?;
                let ordinal = !u64::from_str_radix(text, 16)
                    .map_err(|_| Error::corrupt("identity history ordinal"))?;
                state.pending = Some(Backfill {
                    key: record.key,
                    row: Membership {
                        identifier: timeline.identifier.clone(),
                        ordinal,
                        entities: entities(&timeline),
                    },
                    offset: 0,
                });
                break;
            }
        }
        if exhausted && state.pending.is_none() {
            state.ready = true;
            state.after = None;
            state.previous = None;
            return Ok(());
        }
    }
    if let Some(pending) = state.pending.as_mut() {
        let previous = state
            .previous
            .as_ref()
            .filter(|p| p.identifier == pending.row.identifier);
        let actions: Vec<_> = pending
            .row
            .entities
            .iter()
            .map(|id| (*id, &pending.row, true))
            .chain(previous.into_iter().flat_map(|newer| {
                pending
                    .row
                    .entities
                    .difference(&newer.entities)
                    .map(move |id| (*id, newer, false))
            }))
            .collect();
        for (id, row, present) in actions.iter().skip(pending.offset).take(4) {
            link(scratch, tx, collection, *id, row, *present)?;
            pending.offset += 1;
        }
        if pending.offset >= actions.len() {
            state.previous = Some(pending.row.clone());
            state.pending = None;
        }
    }
    Ok(())
}
fn reclaim(tx: &mut Transaction<'_>, collection: Uuid, state: &mut SubjectState) -> Result<bool> {
    if state.gc_after.is_none() {
        let floor = crate::tenant::retention::knowledge_floor(tx)?;
        if floor <= state.swept_floor {
            return Ok(false);
        }
        state.gc_floor = floor;
        state.gc_pair.clear();
        state.gc_baseline = false;
    }
    let prefix = prefix(collection, "subject:");
    let (rows, exhausted) = tx.gc_prefix(&prefix, state.gc_after.as_deref(), 4)?;
    for row in rows {
        state.gc_after = Some(row.key.clone());
        if row.value.is_none() {
            continue;
        }
        let text = std::str::from_utf8(&row.key[prefix.len()..row.key.len() - 2])
            .map_err(|_| Error::corrupt("subject link key"))?;
        let (pair, ordinal) = text
            .rsplit_once(':')
            .ok_or_else(|| Error::corrupt("subject link key"))?;
        let ordinal = !u64::from_str_radix(ordinal, 16)
            .map_err(|_| Error::corrupt("subject link ordinal"))?;
        if pair != state.gc_pair {
            state.gc_pair = pair.into();
            state.gc_baseline = false;
        }
        if ordinal < state.gc_floor {
            if state.gc_baseline {
                tx.delete(row.key)?;
            }
            state.gc_baseline = true;
        }
    }
    if exhausted {
        state.swept_floor = state.gc_floor;
        state.gc_after = None;
        state.gc_pair.clear();
    }
    Ok(true)
}
impl TenantHandle {
    pub(crate) fn identity_subject_step(&self) -> Result<()> {
        self.engine().transact_maintenance(|tx| {
            let collection = match collection_for_write(tx) {
                Ok(c) => c,
                Err(e) if e.code == "NOT_FOUND" => return Ok(()),
                Err(e) => return Err(e),
            };
            let mut scratch = Scratch::maintenance(self);
            let Some(head) = scratch.read::<Knowledge>(tx, collection, "head:knowledge")? else {
                return Ok(());
            };
            let mut state = ensure_state(&mut scratch, tx, collection, &head, false)?;
            if !state.ready {
                migrate(&mut scratch, tx, collection, &mut state)?;
            } else if !reclaim(tx, collection, &mut state)? {
                return Ok(());
            }
            scratch.write(tx, collection, STATE, "subject_state", &state)?;
            tx.check_expanded_limits()
        })
    }
}
