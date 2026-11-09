use super::{storage::*, subjects::*, *};
use crate::{
    codec,
    storage::{Snapshot, Transaction},
    tenant::catalog::document_key,
};
use std::time::{Duration, Instant};

const STEP_ROWS: usize = 64;
const STEP_BYTES: usize = 1 << 20;
const STEP_TIME: Duration = Duration::from_millis(10);

pub(super) fn prefix(collection: Uuid, text: &str) -> Vec<u8> {
    let mut key = document_key(collection, text);
    key.truncate(key.len() - 2);
    key
}
// Only completed identifiers may be reclaimed while backfill still needs the
// older revisions of its current identifier to construct negative memberships.
pub(crate) fn subject_history_gc_limit(
    tx: &Transaction<'_>,
    collection: Uuid,
) -> Result<Option<Vec<u8>>> {
    let state = tx
        .get(&document_key(collection, STATE))?
        .map(|b| codec::field_as::<SubjectState>(&b, "/data"))
        .transpose()?
        .flatten();
    let Some(state) = state else {
        return Ok(Some(prefix(collection, "history:")));
    };
    if state.ready {
        return Ok(None);
    }
    let current = state
        .pending
        .as_ref()
        .map(|p| &p.row)
        .or(state.previous.as_ref());
    Ok(Some(match current {
        Some(row) => prefix(
            collection,
            &format!("history:{}:", digest(&row.identifier)?),
        ),
        None => prefix(collection, "history:"),
    }))
}
fn migrate(
    scratch: &mut Scratch<'_>,
    tx: &mut Transaction<'_>,
    collection: Uuid,
    state: &mut SubjectState,
    batch: usize,
) -> Result<()> {
    tx.require_foreground_disk();
    let floor = crate::tenant::retention::knowledge_floor(tx)?;
    let started = Instant::now();
    let source = prefix(collection, "history:");
    let mut rows = usize::from(state.pending.is_some());
    let mut links = 0;
    let mut bytes = 0usize;
    let mut moved = false;
    while (state.pending.is_some() || rows < batch)
        && links < batch
        && (!moved || started.elapsed() < STEP_TIME)
    {
        if state.pending.is_none() {
            // Read one physical row at a time: a byte limit must not first load
            // 64 maximum-sized timelines or lose progress through tombstones.
            let (records, exhausted) = tx.gc_prefix(&source, state.after.as_deref(), 1)?;
            let Some(record) = records.into_iter().next() else {
                if exhausted {
                    state.ready = true;
                    state.after = None;
                    state.previous = None;
                }
                break;
            };
            let size = record.value.as_ref().map_or(0, |v| v.len());
            if moved && bytes.saturating_add(size) > STEP_BYTES {
                break;
            }
            bytes = bytes.saturating_add(size);
            state.after = Some(record.key.clone());
            rows += 1;
            moved = true;
            let Some(value) = record.value else {
                state.processed_history_rows = state.processed_history_rows.saturating_add(1);
                continue;
            };
            let timeline: Timeline = scratch.decode(&value)?;
            let text =
                std::str::from_utf8(&record.key[record.key.len() - 18..record.key.len() - 2])
                    .map_err(|_| Error::corrupt("identity history ordinal"))?;
            let ordinal = !u64::from_str_radix(text, 16)
                .map_err(|_| Error::corrupt("identity history ordinal"))?;
            if record.key
                != document_key(
                    collection,
                    &format!(
                        "history:{}:{:016x}",
                        digest(&timeline.identifier)?,
                        !ordinal
                    ),
                )
            {
                return Err(Error::corrupt("identity history key mismatch"));
            }
            state.pending = Some(Backfill {
                key: record.key,
                row: Membership {
                    identifier: timeline.identifier.clone(),
                    ordinal,
                    entities: entities(&timeline),
                },
                offset: 0,
            });
        }
        let pending = state.pending.as_mut().unwrap();
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
        for (id, row, present) in actions.iter().skip(pending.offset).take(batch - links) {
            if links > 0 && started.elapsed() >= STEP_TIME {
                break;
            }
            link(scratch, tx, collection, *id, row, *present)?;
            pending.offset += 1;
            links += 1;
            state.written_links = state.written_links.saturating_add(1);
            moved = true;
        }
        if pending.offset < actions.len() {
            break;
        }
        let completed = state.pending.take().unwrap();
        state.processed_history_rows = state.processed_history_rows.saturating_add(1);
        if completed.row.ordinal < floor {
            // The retention sweep selected the oldest possibly live knowledge.
            // Keep its predecessor; older versions need no reverse reconstruction,
            // even if physical history deletion has not finished yet.
            state.after = codec::prefix_end(&prefix(
                collection,
                &format!("history:{}:", digest(&completed.row.identifier)?),
            ));
        }
        state.previous = Some(completed.row);
    }
    Ok(())
}
fn reclaim(
    tx: &mut Transaction<'_>,
    collection: Uuid,
    state: &mut SubjectState,
    batch: usize,
) -> Result<bool> {
    if state.gc_after.is_none() {
        let floor = crate::tenant::retention::knowledge_floor(tx)?;
        if floor <= state.swept_floor {
            return Ok(false);
        }
        state.gc_floor = floor;
        state.gc_pair.clear();
        state.gc_baseline = false;
    }
    let started = Instant::now();
    let source = prefix(collection, "subject:");
    let mut bytes = 0usize;
    for n in 0..batch {
        if n > 0 && (started.elapsed() >= STEP_TIME || bytes >= STEP_BYTES) {
            break;
        }
        let (records, exhausted) = tx.gc_prefix(&source, state.gc_after.as_deref(), 1)?;
        let Some(row) = records.into_iter().next() else {
            if exhausted {
                state.swept_floor = state.gc_floor;
                state.gc_after = None;
                state.gc_pair.clear();
            }
            break;
        };
        state.gc_after = Some(row.key.clone());
        state.gc_scanned_rows = state.gc_scanned_rows.saturating_add(1);
        let Some(value) = row.value else {
            continue;
        };
        bytes = bytes.saturating_add(value.len());
        let text = std::str::from_utf8(&row.key[source.len()..row.key.len() - 2])
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
                state.gc_removed_rows = state.gc_removed_rows.saturating_add(1);
            }
            state.gc_baseline = true;
        }
        if ordinal < state.swept_floor {
            state.gc_after =
                codec::prefix_end(&prefix(collection, &format!("subject:{}:", state.gc_pair)));
        }
    }
    Ok(true)
}

pub(super) fn step(tenant: &TenantHandle, mut batch: usize) -> Result<()> {
    batch = batch.clamp(1, STEP_ROWS);
    loop {
        let mut shrink = false;
        let result = tenant.engine().transact_maintenance(|tx| {
            let prepared = (|| {
                let collection = match collection_for_write(tx) {
                    Ok(c) => c,
                    Err(e) if e.code == "NOT_FOUND" => return Ok(None),
                    Err(e) => return Err(e),
                };
                let mut scratch = Scratch::maintenance(tenant);
                let mut state = if let Some(state) =
                    scratch.read::<SubjectState>(tx, collection, STATE)?
                {
                    state
                } else {
                    let Some(head) = scratch.read::<Knowledge>(tx, collection, "head:knowledge")?
                    else {
                        return Ok(None);
                    };
                    ensure_state(&mut scratch, tx, collection, &head, false)?
                };
                if !state.ready {
                    migrate(&mut scratch, tx, collection, &mut state, batch)?;
                } else if !reclaim(tx, collection, &mut state, batch)? {
                    return Ok(Some(state.progress()));
                }
                scratch.write(tx, collection, STATE, "subject_state", &state)?;
                tx.check_expanded_limits()?;
                Ok(Some(state.progress()))
            })();
            // Only pre-WAL preparation failures may shrink and retry; an uncertain
            // append or durable-publication failure must never replay this work.
            shrink = prepared
                .as_ref()
                .is_err_and(|e: &Error| e.code == "RESOURCE_EXHAUSTED");
            prepared
        });
        if shrink && batch > 1 {
            batch = batch.div_ceil(2);
            continue;
        }
        *tenant.core.subject_progress.lock() = result?;
        return Ok(());
    }
}
impl TenantHandle {
    pub(crate) fn identity_subject_step(&self) -> Result<()> {
        step(self, STEP_ROWS)
    }
    pub(crate) fn subject_maintenance_progress(
        &self,
        snapshot: &Snapshot,
    ) -> Result<Option<SubjectMaintenanceProgress>> {
        let collection = match self.collection_id(snapshot, COLLECTION) {
            Ok(c) => c,
            Err(e) if e.code == "NOT_FOUND" => return Ok(None),
            Err(e) => return Err(e),
        };
        let Some(bytes) = snapshot.get(
            &document_key(collection, STATE),
            &mut crate::ReadStats::default(),
        )?
        else {
            return Ok(Some(SubjectMaintenanceProgress::default()));
        };
        let _memory = self
            .engine()
            .budget
            .reserve(bytes.len().saturating_mul(16).saturating_add(4096))?;
        let state: SubjectState = codec::field_as(&bytes, "/data")?
            .ok_or_else(|| Error::corrupt("subject maintenance state"))?;
        Ok(Some(state.progress()))
    }
}
