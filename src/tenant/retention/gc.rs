use super::*;
use crate::codec;
use crate::identity::{COLLECTION, Knowledge};

#[derive(Default, Serialize, Deserialize)]
pub(super) struct State {
    migration_phase: u8,
    migration_after: Option<Vec<u8>>,
    history_after: Option<Vec<u8>>,
    #[serde(default)]
    pub(super) expiry_after: Option<Vec<u8>>,
    history_identifier: Option<String>,
    baseline_kept: bool,
    sweeping_floor: u64,
    swept_floor: u64,
    pub reclaimed_records: u64,
    pub completed_rounds: u64,
    pub watermark_ms: u64,
}
impl State {
    pub(super) fn status(&self) -> serde_json::Value {
        serde_json::json!({"migration_complete":self.migration_phase>=5,"reclaimed_records":self.reclaimed_records,
            "completed_rounds":self.completed_rounds,"watermark_ms":self.watermark_ms,
            "knowledge_floor":self.swept_floor,"history_sweep_active":self.history_after.is_some()})
    }
}
fn identity_collection(tx: &Transaction<'_>) -> Result<Option<Uuid>> {
    tx.get(&named(b"C", COLLECTION))?
        .map(|b| parse::<crate::Collection>(&b).map(|c| c.id))
        .transpose()
}
fn doc_prefix(collection: Uuid, text: &str) -> Vec<u8> {
    let mut out = document_key(collection, text);
    out.truncate(out.len() - 2);
    out
}
fn migration(
    tx: &mut Transaction<'_>,
    p: &RetentionPolicy,
    state: &mut State,
    batch: usize,
) -> Result<()> {
    let collection = identity_collection(tx)?;
    let prefix = match state.migration_phase {
        0 => b"R".to_vec(),
        1 => PUBLICATIONS.to_vec(),
        2 => b"V".to_vec(),
        3 => collection
            .map(|c| doc_prefix(c, "operation:"))
            .unwrap_or_else(|| b"~absent".to_vec()),
        4 => collection
            .map(|c| doc_prefix(c, "knowledge:"))
            .unwrap_or_else(|| b"~absent".to_vec()),
        _ => return Ok(()),
    };
    let rows = tx.prefix_first_after(&prefix, state.migration_after.as_deref(), batch)?;
    for (k, bytes) in &rows {
        match state.migration_phase {
            2 => {
                let version = crate::tenant::datasets::parse_published(bytes)?;
                let current: Option<Uuid> = tx
                    .get(&named(b"P", &version.name))?
                    .map(|b| parse(&b))
                    .transpose()?;
                // Build references even for legacy/current publications. A frozen
                // collection can be shared by several resources and versions.
                put(
                    tx,
                    key(
                        &key(REFERENCES, version.collection_id.as_bytes()),
                        version.version.as_bytes(),
                    ),
                    &true,
                )?;
                if current != Some(version.version) {
                    schedule_at(
                        tx,
                        k,
                        p.legacy_expires_at_ms,
                        Expiration::Publication {
                            version: version.version,
                            collection: version.collection_id,
                            name: version.publication.collection,
                        },
                    )?;
                }
            }
            4 => {
                let knowledge: Knowledge = codec::field_as(bytes, "/data")?
                    .ok_or_else(|| Error::corrupt("knowledge data"))?;
                put(
                    tx,
                    key(KNOWLEDGE, &knowledge.ordinal.to_be_bytes()),
                    &knowledge.version,
                )?;
                let c = collection.unwrap();
                let head = tx
                    .get(&document_key(c, "head:knowledge"))?
                    .ok_or_else(|| Error::corrupt("knowledge head"))?;
                let head: Knowledge = codec::field_as(&head, "/data")?
                    .ok_or_else(|| Error::corrupt("knowledge data"))?;
                if knowledge.version != head.version {
                    schedule_at(
                        tx,
                        k,
                        p.legacy_expires_at_ms,
                        Expiration::Knowledge {
                            ordinal: knowledge.ordinal,
                        },
                    )?;
                }
            }
            _ => schedule_at(tx, k, p.legacy_expires_at_ms, Expiration::Delete)?,
        }
        state.migration_after = Some(k.clone());
    }
    if rows.len() < batch {
        state.migration_phase += 1;
        state.migration_after = None;
    }
    Ok(())
}
fn expire(tx: &mut Transaction<'_>, state: &mut State, batch: usize) -> Result<()> {
    let rows = tx.gc_prefix(EXPIRY, state.expiry_after.as_deref(), batch)?;
    for record in rows {
        let queue_key = record.key;
        let at = u64::from_be_bytes(
            queue_key[EXPIRY.len()..EXPIRY.len() + 8]
                .try_into()
                .map_err(|_| Error::corrupt("expiry key"))?,
        );
        if at > state.watermark_ms {
            break;
        }
        state.expiry_after = Some(queue_key.clone());
        let Some(bytes) = record.value else {
            continue;
        };
        let job: Scheduled = parse(&bytes)?;
        match job.action {
            Expiration::Delete => {
                tx.delete(job.target.clone())?;
            }
            Expiration::Knowledge { ordinal } => {
                tx.delete(job.target.clone())?;
                tx.delete(key(KNOWLEDGE, &ordinal.to_be_bytes()))?;
            }
            Expiration::Publication {
                version,
                collection,
                name,
            } => {
                let bytes = tx
                    .get(&job.target)?
                    .ok_or_else(|| Error::corrupt("publication expiration target"))?;
                let publication = crate::tenant::datasets::parse_published(&bytes)?;
                let current: Option<Uuid> = tx
                    .get(&named(b"P", &publication.name))?
                    .map(|b| parse(&b))
                    .transpose()?;
                if current == Some(version) {
                    return Err(Error::corrupt(
                        "current publication scheduled for expiration",
                    ));
                }
                tx.delete(job.target.clone())?;
                let refs = key(REFERENCES, collection.as_bytes());
                tx.delete(key(&refs, version.as_bytes()))?;
                if tx.prefix_first(&refs, 1)?.is_empty() {
                    let catalog_key = named(b"C", &name);
                    if let Some(bytes) = tx.get(&catalog_key)? {
                        let c: crate::Collection = parse(&bytes)?;
                        if c.id != collection {
                            return Err(Error::corrupt("published collection identity changed"));
                        }
                        tx.delete(catalog_key)?;
                        tx.delete(prefix(b'S', collection))?;
                        queue_gc(
                            tx,
                            [b'D', b'I', b'J', b'L', b'B', b'R']
                                .iter()
                                .map(|kind| prefix(*kind, collection))
                                .collect(),
                        )?;
                    }
                }
            }
        }
        tx.delete(key(DEADLINE, &job.target))?;
        tx.delete(queue_key)?;
        state.reclaimed_records = state.reclaimed_records.saturating_add(1);
    }
    Ok(())
}
fn history(tx: &mut Transaction<'_>, state: &mut State, batch: usize) -> Result<()> {
    let Some(collection) = identity_collection(tx)? else {
        return Ok(());
    };
    if state.history_after.is_none() {
        let Some((key, _)) = tx.prefix_first(KNOWLEDGE, 1)?.into_iter().next() else {
            return Ok(());
        };
        let floor = u64::from_be_bytes(
            key[KNOWLEDGE.len()..]
                .try_into()
                .map_err(|_| Error::corrupt("knowledge ordinal"))?,
        );
        if floor <= state.swept_floor {
            return Ok(());
        }
        state.sweeping_floor = floor;
        state.history_identifier = None;
        state.baseline_kept = false;
    }
    let prefix = doc_prefix(collection, "history:");
    let rows = tx.prefix_first_after(&prefix, state.history_after.as_deref(), batch)?;
    for (k, _) in &rows {
        let text = std::str::from_utf8(&k[prefix.len()..k.len() - 2])
            .map_err(|_| Error::corrupt("history key"))?;
        let (identifier, ordinal) = text
            .rsplit_once(':')
            .ok_or_else(|| Error::corrupt("history key"))?;
        let ordinal =
            !u64::from_str_radix(ordinal, 16).map_err(|_| Error::corrupt("history ordinal"))?;
        if state.history_identifier.as_deref() != Some(identifier) {
            state.history_identifier = Some(identifier.into());
            state.baseline_kept = false;
        }
        if ordinal < state.sweeping_floor {
            if state.baseline_kept {
                tx.delete(k.clone())?;
                state.reclaimed_records = state.reclaimed_records.saturating_add(1);
            } else {
                state.baseline_kept = true;
            }
        }
        state.history_after = Some(k.clone());
    }
    if rows.len() < batch {
        state.history_after = None;
        state.swept_floor = state.sweeping_floor;
        state.history_identifier = None;
    }
    Ok(())
}
pub(super) fn step(tenant: &TenantHandle) -> Result<()> {
    let mut expiry_batch = (tenant.engine().options.max_batch_operations / 12).clamp(1, 64);
    loop {
        let batch = expiry_batch.min(16);
        let mut attempted_batch = expiry_batch;
        let mut oversized = false;
        let result = tenant.engine().transact_maintenance(|tx| {
            let Some(p) = policy(tx)? else {
                return Ok(false);
            };
            let mut state: State = tx
                .get(STATE)?
                .map(|b| parse(&b))
                .transpose()?
                .unwrap_or_default();
            let before = serde_json::to_vec(&state)?;
            let previous_reclaimed = state.reclaimed_records;
            if state.migration_phase < 5 {
                attempted_batch = batch;
                migration(tx, &p, &mut state, batch)?;
            } else {
                let now = now_ms()?.max(state.watermark_ms);
                // Do not generate empty maintenance WAL traffic simply to tick time.
                let due = tx
                    .gc_prefix(EXPIRY, state.expiry_after.as_deref(), 1)?
                    .first()
                    .is_some_and(|r| {
                        &r.key[EXPIRY.len()..EXPIRY.len() + 8] <= now.to_be_bytes().as_slice()
                    });
                if due {
                    state.watermark_ms = now;
                    expire(tx, &mut state, expiry_batch)?;
                }
                history(tx, &mut state, batch)?;
            }
            if serde_json::to_vec(&state)? != before {
                state.completed_rounds = state.completed_rounds.saturating_add(1);
                put(tx, STATE.to_vec(), &state)?;
                if let Err(error) = tx.check_expanded_limits() {
                    oversized = true;
                    return Err(error);
                }
                crate::storage::errorpoint("retention_before_commit")?;
                crate::storage::failpoint("retention_before_commit");
            }
            Ok(state.reclaimed_records > previous_reclaimed)
        });
        // Long receipt keys can exceed the byte budget even when the operation
        // count fits. Retry only a pre-WAL size rejection, with a smaller batch;
        // neither deletions nor progress from the rejected attempt were published.
        if oversized && attempted_batch > 1 {
            expiry_batch = attempted_batch.div_ceil(2);
            continue;
        }
        let reclaimed = result?;
        if reclaimed {
            crate::storage::failpoint("retention_after_commit");
        }
        return Ok(());
    }
}
