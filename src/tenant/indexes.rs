use super::{TenantHandle, catalog::*, delta::Delta};
use crate::{
    Error, Result, codec,
    model::*,
    storage::{Snapshot, Transaction},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct IndexEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub prefix_len: usize,
    pub document_key: String,
}

pub(crate) fn tuple(index: &IndexDefinition, values: &[Option<Value>]) -> Result<Vec<u8>> {
    if values.len() != index.spec.fields.len() {
        return Err(Error::invalid("index tuple arity"));
    }
    let mut out = Vec::new();
    for (field, value) in index.spec.fields.iter().zip(values) {
        out.extend(codec::scalar(value.as_ref(), &field.order)?);
    }
    Ok(out)
}
pub(crate) fn index_entry(
    collection: Uuid,
    index: &IndexDefinition,
    document: &[u8],
) -> Result<Option<IndexEntry>> {
    let values: Vec<_> = index
        .spec
        .fields
        .iter()
        .map(|f| codec::field(document, &f.field))
        .collect::<Result<_>>()?;
    let encoded = tuple(index, &values)?;
    if index.spec.unique && values.iter().any(|v| v.as_ref().is_none_or(Value::is_null)) {
        return Ok(None);
    }
    let document_key = codec::field(document, "/_key")?
        .and_then(|v| v.as_str().map(str::to_owned))
        .ok_or_else(|| Error::corrupt("document primary key"))?;
    let mut key = index_prefix(collection, index.generation);
    key.extend(encoded);
    let prefix_len = if index.spec.unique { 0 } else { key.len() };
    if !index.spec.unique {
        codec::escape(document_key.as_bytes(), &mut key);
    }
    if key.len() > codec::MAX_KEY {
        return Err(Error::limit("index key exceeds 8 KiB"));
    }
    let mut paths: Vec<_> = index
        .spec
        .fields
        .iter()
        .map(|f| f.field.clone())
        .chain(index.spec.include.iter().cloned())
        .chain(["/_key".into(), "/_rev".into()])
        .collect();
    paths.sort();
    paths.dedup();
    let covered = codec::project(document, &paths)?;
    Ok(Some(IndexEntry {
        key,
        value: codec::encode(&covered)?,
        prefix_len,
        document_key,
    }))
}
pub(crate) fn maintain_index(
    tx: &mut Transaction<'_>,
    index: &IndexDefinition,
    old: Option<&IndexEntry>,
    new: Option<&IndexEntry>,
) -> Result<()> {
    if let Some(old) = old {
        tx.delete_with_prefix(old.key.clone(), old.prefix_len)?;
    }
    if let Some(new) = new {
        if index.spec.unique
            && let Some(existing) = tx.get(&new.key)?
        {
            let owner =
                codec::field(&existing, "/_key")?.and_then(|v| v.as_str().map(str::to_owned));
            if owner.as_deref() != Some(&new.document_key) {
                return Err(Error::new(
                    "UNIQUE_CONFLICT",
                    format!("duplicate value in {}", index.spec.name),
                ));
            }
        }
        tx.put_with_prefix(new.key.clone(), new.value.clone(), new.prefix_len)?;
    }
    Ok(())
}
pub(crate) fn covers(index: &IndexDefinition, paths: &[String]) -> bool {
    !paths.is_empty()
        && paths.iter().all(|path| {
            path == "/_key"
                || path == "/_rev"
                || index
                    .spec
                    .fields
                    .iter()
                    .map(|f| &f.field)
                    .chain(index.spec.include.iter())
                    .any(|p| {
                        path == p
                            || path
                                .strip_prefix(p)
                                .is_some_and(|suffix| suffix.starts_with('/'))
                    })
        })
}
fn validate_spec(spec: &IndexSpec) -> Result<()> {
    name(&spec.name)?;
    if spec.fields.is_empty() || spec.fields.len() > 8 || spec.include.len() > 32 {
        return Err(Error::invalid(
            "index requires 1..8 fields and at most 32 included paths",
        ));
    }
    let mut fields = HashSet::new();
    for field in &spec.fields {
        codec::validate_pointer(&field.field)?;
        if !fields.insert(&field.field) {
            return Err(Error::invalid("duplicate index field"));
        }
    }
    for path in &spec.include {
        codec::validate_pointer(path)?;
    }
    Ok(())
}
impl TenantHandle {
    pub fn create_index(&self, collection: &str, spec: IndexSpec) -> Result<IndexDefinition> {
        crate::identity::unmanaged(collection)?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        self.build_index(collection, spec, false, None)
    }
    fn build_index(
        &self,
        collection_name: &str,
        spec: IndexSpec,
        restart: bool,
        stop: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<IndexDefinition> {
        validate_spec(&spec)?;
        let generation = Uuid::new_v4();
        self.core.builds.lock().insert(generation);
        let result = (|| {
            let ((collection, mut definition), snapshot) =
                self.engine().transact_snapshot(|tx| {
                    let collection = collection_tx(tx, collection_name)?;
                    super::datasets::mutable(tx, collection.id)?;
                    let key = named(&prefix(b'J', collection.id), &spec.name);
                    let old: Option<IndexDefinition> =
                        tx.get(&key)?.map(|v| parse(&v)).transpose()?;
                    if old.as_ref().is_some_and(|d| {
                        d.state == IndexState::Ready
                            || (d.state == IndexState::Building && !restart)
                    }) {
                        return Err(Error::new(
                            "ALREADY_EXISTS",
                            "index already ready or building",
                        ));
                    }
                    if old.is_none() && indexes_tx(tx, collection.id)?.len() >= 16 {
                        return Err(Error::limit("index count limit: 16"));
                    }
                    if let Some(old) = &old {
                        queue_gc(
                            tx,
                            vec![
                                index_prefix(collection.id, old.generation),
                                delta_prefix(collection.id, old.generation),
                            ],
                        )?;
                    }
                    let definition = IndexDefinition {
                        id: old.map_or_else(Uuid::new_v4, |d| d.id),
                        generation,
                        spec,
                        state: IndexState::Building,
                        start_seq: tx.seq,
                        ready_seq: 0,
                        error: None,
                        entries: 0,
                    };
                    put(tx, key, &definition)?;
                    Ok((collection, definition))
                })?;
            let built = self.populate_index(&collection, &mut definition, &snapshot, stop);
            if let Err(error) = built {
                if stop.is_some_and(|stop| stop.load(std::sync::atomic::Ordering::Acquire)) {
                    return Err(error);
                }
                let key = named(&prefix(b'J', collection.id), &definition.spec.name);
                let _ = self.engine().transact(|tx| {
                    if let Some(value) = tx.get(&key)? {
                        let mut current: IndexDefinition = parse(&value)?;
                        if current.generation == generation {
                            current.state = IndexState::Failed;
                            current.error = Some(error.to_string());
                            put(tx, key, &current)?;
                            queue_gc(
                                tx,
                                vec![
                                    index_prefix(collection.id, generation),
                                    delta_prefix(collection.id, generation),
                                ],
                            )?;
                        }
                    }
                    Ok(())
                });
                return Err(error);
            }
            self.core.plans.lock().clear();
            Ok(definition)
        })();
        self.core.builds.lock().remove(&generation);
        result
    }
    fn populate_index(
        &self,
        collection: &Collection,
        definition: &mut IndexDefinition,
        snapshot: &Snapshot,
        stop: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<()> {
        let check_stop = || {
            if stop.is_some_and(|stop| stop.load(std::sync::atomic::Ordering::Acquire)) {
                Err(Error::new("CANCELLED", "index recovery stopping"))
            } else {
                Ok(())
            }
        };
        let docs = prefix(b'D', collection.id);
        let mut stats = ReadStats::default();
        let mut scan = snapshot.scan(docs.clone(), codec::prefix_end(&docs), None, &mut stats)?;
        let mut entries = Vec::new();
        let mut entry_memory = Vec::new();
        let mut bytes = 0;
        while let Some(record) = scan.next(&mut stats)? {
            check_stop()?;
            if let Some(entry) = index_entry(collection.id, definition, &record.value.unwrap())? {
                entry_memory.push(
                    self.engine()
                        .budget
                        .reserve((entry.key.len() + entry.value.len()) * 2 + 256)?,
                );
                bytes += entry.key.len() + entry.value.len();
                entries.push(entry);
                definition.entries += 1;
            }
            if entries.len() >= 256 || bytes >= 2 << 20 {
                self.write_index_entries(collection, definition, &entries, snapshot, stop)?;
                entries.clear();
                entry_memory.clear();
                bytes = 0;
            }
        }
        if !entries.is_empty() {
            self.write_index_entries(collection, definition, &entries, snapshot, stop)?;
        }
        drop(entries);
        drop(entry_memory);
        let delta = delta_prefix(collection.id, definition.generation);
        let mut delta_after: Option<Vec<u8>> = None;
        loop {
            check_stop()?;
            snapshot.check()?;
            if self.engine().needs_flush() {
                self.engine().flush_if_needed()?;
            }
            let finished = self.engine().transact_waiting(
                |tx| {
                    let key = named(&prefix(b'J', collection.id), &definition.spec.name);
                    ensure_build(tx, &key, definition.generation)?;
                    let pending = tx.prefix_first_after(&delta, delta_after.as_deref(), 64)?;
                    if pending.is_empty() {
                        definition.state = IndexState::Ready;
                        definition.ready_seq = tx.seq;
                        put(tx, key, definition)?;
                        return Ok(true);
                    }
                    let mut bytes = 0;
                    for (key, value) in pending {
                        let _memory = self.engine().budget.reserve(value.len() * 2 + 512)?;
                        let update = Delta::decode(&value)?;
                        maintain_index(tx, definition, update.old.as_ref(), update.new.as_ref())?;
                        if update.old.is_some() {
                            definition.entries = definition.entries.saturating_sub(1);
                        }
                        if update.new.is_some() {
                            definition.entries += 1;
                        }
                        delta_after = Some(key.clone());
                        tx.delete(key)?;
                        bytes += value.len();
                        if bytes >= 2 << 20 {
                            break;
                        }
                    }
                    Ok(false)
                },
                &|| {
                    snapshot.check()?;
                    check_stop()
                },
                stop.is_none().then_some(std::time::Duration::from_secs(5)),
            )?;
            if finished {
                crate::storage::failpoint("index_ready");
                return Ok(());
            }
        }
    }
    fn write_index_entries(
        &self,
        collection: &Collection,
        index: &IndexDefinition,
        entries: &[IndexEntry],
        snapshot: &Snapshot,
        stop: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<()> {
        if self.engine().needs_flush() {
            self.engine().flush_if_needed()?;
        }
        self.engine().transact_waiting(
            |tx| {
                ensure_build(
                    tx,
                    &named(&prefix(b'J', collection.id), &index.spec.name),
                    index.generation,
                )?;
                for entry in entries {
                    maintain_index(tx, index, None, Some(entry))?;
                }
                Ok(())
            },
            &|| {
                snapshot.check()?;
                if stop.is_some_and(|stop| stop.load(std::sync::atomic::Ordering::Acquire)) {
                    Err(Error::new("CANCELLED", "index recovery stopping"))
                } else {
                    Ok(())
                }
            },
            stop.is_none().then_some(std::time::Duration::from_secs(5)),
        )
    }
    pub(crate) fn restart_interrupted_build(
        &self,
        stop: &std::sync::atomic::AtomicBool,
    ) -> Result<()> {
        let catalog = self.engine().catalog_seq();
        if self
            .core
            .recovery_catalog
            .load(std::sync::atomic::Ordering::Acquire)
            == catalog
        {
            return Ok(());
        }
        let snapshot = self.engine().snapshot()?;
        for collection in list_snapshot::<Collection>(&snapshot, b"C", 1024)? {
            for index in
                list_snapshot::<IndexDefinition>(&snapshot, &prefix(b'J', collection.id), 32)?
            {
                if index.state == IndexState::Building
                    && !self.core.builds.lock().contains(&index.generation)
                {
                    drop(snapshot);
                    self.build_index(&collection.name, index.spec, true, Some(stop))?;
                    return Ok(());
                }
            }
        }
        self.core
            .recovery_catalog
            .store(catalog, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}
fn ensure_build(tx: &Transaction<'_>, key: &[u8], generation: Uuid) -> Result<()> {
    let def: IndexDefinition = parse(
        &tx.get(key)?
            .ok_or_else(|| Error::new("CANCELLED", "index dropped"))?,
    )?;
    if def.generation != generation || def.state != IndexState::Building {
        return Err(Error::new("CANCELLED", "index build superseded"));
    }
    Ok(())
}
