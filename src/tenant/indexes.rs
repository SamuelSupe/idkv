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
        self.check_index_build(stop)?;
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
                // Draining is resumable, like shutdown. Do not acquire another
                // transaction to mark FAILED while management waits for us.
                if error.code == "BUSY"
                    || stop.is_some_and(|stop| stop.load(std::sync::atomic::Ordering::Acquire))
                {
                    return Err(error);
                }
                let key = named(&prefix(b'J', collection.id), &definition.spec.name);
                let _ = self.engine().transact(|tx| {
                    if let Err(error) = ensure_build(tx, &collection, &key, generation) {
                        return if error.code == "CANCELLED" {
                            Ok(())
                        } else {
                            Err(error)
                        };
                    }
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
        let mut builds = self.core.builds.lock();
        builds.remove(&generation);
        self.core
            .recovery_catalog
            .store(u64::MAX, std::sync::atomic::Ordering::Release);
        result
    }
    fn check_index_build(&self, stop: Option<&std::sync::atomic::AtomicBool>) -> Result<()> {
        self.engine().check_draining()?;
        if stop.is_some_and(|stop| stop.load(std::sync::atomic::Ordering::Acquire)) {
            return Err(Error::new("CANCELLED", "index recovery stopping"));
        }
        Ok(())
    }
    fn populate_index(
        &self,
        collection: &Collection,
        definition: &mut IndexDefinition,
        snapshot: &Snapshot,
        stop: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<()> {
        let check_stop = || self.check_index_build(stop);
        check_stop()?;
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
                    ensure_build(tx, collection, &key, definition.generation)?;
                    let pending = tx.prefix_first_after(&delta, delta_after.as_deref(), 1)?;
                    if pending.is_empty() {
                        definition.state = IndexState::Ready;
                        definition.ready_seq = tx.seq;
                        put(tx, key, definition)?;
                        return Ok(true);
                    }
                    // A source transaction may swap unique values. Never split
                    // its deltas across catch-up commits or validate intermediate owners.
                    let group = &pending[0].0[..delta.len() + 8];
                    let pending = tx.prefix(group, self.engine().options.max_batch_operations)?;
                    let mut changes = std::collections::BTreeMap::new();
                    let mut memory = Vec::new();
                    for (key, value) in pending {
                        check_stop()?;
                        memory.push(self.engine().budget.reserve(value.len() * 2 + 512)?);
                        let update = Delta::decode(&value)?;
                        let owner = update
                            .new
                            .as_ref()
                            .or(update.old.as_ref())
                            .map(|entry| entry.document_key.clone());
                        if let Some(owner) = owner {
                            changes
                                .entry(owner)
                                .and_modify(|change: &mut Delta| change.new = update.new.clone())
                                .or_insert(update);
                        }
                        delta_after = Some(key.clone());
                        tx.delete(key)?;
                    }
                    for change in changes.values() {
                        maintain_index(tx, definition, change.old.as_ref(), None)?;
                        if change.old.is_some() {
                            definition.entries = definition.entries.saturating_sub(1);
                        }
                    }
                    for change in changes.values() {
                        maintain_index(tx, definition, None, change.new.as_ref())?;
                        if change.new.is_some() {
                            definition.entries += 1;
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
        self.check_index_build(stop)?;
        if self.engine().needs_flush() {
            self.engine().flush_if_needed()?;
        }
        self.engine().transact_waiting(
            |tx| {
                ensure_build(
                    tx,
                    collection,
                    &named(&prefix(b'J', collection.id), &index.spec.name),
                    index.generation,
                )?;
                for entry in entries {
                    self.check_index_build(stop)?;
                    maintain_index(tx, index, None, Some(entry))?;
                }
                Ok(())
            },
            &|| {
                snapshot.check()?;
                self.check_index_build(stop)
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
        if snapshot.seq() < catalog {
            return Ok(());
        }
        let mut active = Vec::new();
        for collection in list_snapshot::<Collection>(&snapshot, b"C", 1024)? {
            for index in
                list_snapshot::<IndexDefinition>(&snapshot, &prefix(b'J', collection.id), 32)?
            {
                if index.state == IndexState::Building {
                    if !self.core.builds.lock().contains(&index.generation) {
                        drop(snapshot);
                        self.build_index(&collection.name, index.spec, true, Some(stop))?;
                        return Ok(());
                    }
                    active.push(index.generation);
                }
            }
        }
        // Publish under the same lock as build exit. A late inspection must
        // not overwrite the invalidation of an abandoned generation.
        let builds = self.core.builds.lock();
        if active.iter().all(|generation| builds.contains(generation)) {
            self.core
                .recovery_catalog
                .store(catalog, std::sync::atomic::Ordering::Release);
        }
        Ok(())
    }
}
fn ensure_build(
    tx: &Transaction<'_>,
    collection: &Collection,
    key: &[u8],
    generation: Uuid,
) -> Result<()> {
    let current = tx
        .get(&named(b"C", &collection.name))?
        .map(|value| parse::<Collection>(&value))
        .transpose()?;
    if current.is_none_or(|current| current.id != collection.id) {
        return Err(Error::new(
            "CANCELLED",
            "index collection dropped or replaced",
        ));
    }
    let def: IndexDefinition = parse(
        &tx.get(key)?
            .ok_or_else(|| Error::new("CANCELLED", "index dropped"))?,
    )?;
    if def.generation != generation || def.state != IndexState::Building {
        return Err(Error::new("CANCELLED", "index build superseded"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Options, Store};
    use serde_json::json;

    #[test]
    fn subsystem_review_dropped_collection_stops_snapshot_index_batches() {
        for recreate in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(dir.path(), Options::default()).unwrap();
            let id = store.create_tenant("index-drop").unwrap().id;
            let tenant = store.tenant(id).unwrap();
            // Pause the coordinator before its GC pass so the old index
            // definition is still present when the delayed build resumes.
            let _cursors = tenant.core.cursors.lock();
            let collection = tenant.create_collection("docs").unwrap();
            tenant
                .put("docs", json!({"_key":"ack","value":1}), None)
                .unwrap();
            let (mut definition, snapshot) = tenant
                .engine()
                .transact_snapshot(|tx| {
                    let definition = IndexDefinition {
                        id: Uuid::new_v4(),
                        generation: Uuid::new_v4(),
                        spec: IndexSpec {
                            name: "value".into(),
                            fields: vec![IndexField {
                                field: "/value".into(),
                                order: Order::Asc,
                            }],
                            unique: false,
                            include: vec![],
                        },
                        state: IndexState::Building,
                        start_seq: tx.seq,
                        ready_seq: 0,
                        error: None,
                        entries: 0,
                    };
                    put(
                        tx,
                        named(&prefix(b'J', collection.id), "value"),
                        &definition,
                    )?;
                    Ok(definition)
                })
                .unwrap();
            tenant.drop_collection("docs").unwrap();
            if recreate {
                assert_ne!(tenant.create_collection("docs").unwrap().id, collection.id);
            }
            let before = tenant.engine().seq();
            assert_eq!(
                tenant
                    .populate_index(&collection, &mut definition, &snapshot, None)
                    .unwrap_err()
                    .code,
                "CANCELLED"
            );
            assert_eq!(
                tenant.engine().seq(),
                before,
                "cancelled builder published orphan records"
            );
            drop(snapshot);
            for _ in 0..4 {
                tenant.gc_step().unwrap();
            }
            let snapshot = tenant.engine().snapshot().unwrap();
            let mut stats = ReadStats::default();
            let start = prefix(b'I', collection.id);
            assert!(
                snapshot
                    .scan(start.clone(), codec::prefix_end(&start), None, &mut stats)
                    .unwrap()
                    .next(&mut stats)
                    .unwrap()
                    .is_none()
            );
        }
    }
    #[test]
    fn edge_review_online_unique_build_applies_complete_source_transactions() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let t = store
            .tenant(store.create_tenant("unique-deltas").unwrap().id)
            .unwrap();
        let _cursors = t.core.cursors.lock();
        let collection = t.create_collection("docs").unwrap();
        t.write_batch(
            "docs",
            (0..130)
                .map(|n| WriteOp::Put {
                    document: json!({"_key":format!("{n:03}"),"value":n}),
                    if_rev: None,
                })
                .collect(),
        )
        .unwrap();
        let (mut definition, snapshot) = t
            .engine()
            .transact_snapshot(|tx| {
                let definition = IndexDefinition {
                    id: Uuid::new_v4(),
                    generation: Uuid::new_v4(),
                    spec: IndexSpec {
                        name: "value".into(),
                        fields: vec![IndexField {
                            field: "/value".into(),
                            order: Order::Asc,
                        }],
                        unique: true,
                        include: vec![],
                    },
                    state: IndexState::Building,
                    start_seq: tx.seq,
                    ready_seq: 0,
                    error: None,
                    entries: 0,
                };
                put(
                    tx,
                    named(&prefix(b'J', collection.id), "value"),
                    &definition,
                )?;
                Ok(definition)
            })
            .unwrap();
        t.core.builds.lock().insert(definition.generation);
        let receipts = t
            .write_batch(
                "docs",
                (0..130)
                    .map(|n| WriteOp::Put {
                        document: json!({"_key":format!("{n:03}"),"value":129-n}),
                        if_rev: None,
                    })
                    .collect(),
            )
            .unwrap();
        t.populate_index(&collection, &mut definition, &snapshot, None)
            .unwrap();
        assert_eq!(definition.state, IndexState::Ready);
        assert_eq!(definition.entries, 130);
        t.create_lookup(
            "docs",
            LookupDefinition {
                name: "value".into(),
                index: "value".into(),
                select: vec!["/_key".into()],
                mode: MatchMode::All,
                max_matches: 1,
            },
        )
        .unwrap();
        let indexed = t
            .lookup_many(
                "docs",
                "value",
                &(0..130)
                    .map(|value| json!({"value":value}))
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        for (n, matches) in indexed.matches.iter().enumerate() {
            assert_eq!(matches.len(), 1);
            assert_eq!(matches[0]["_key"], format!("{:03}", 129 - n));
        }
        for (n, receipt) in receipts.iter().enumerate() {
            let document = t.get("docs", &format!("{n:03}")).unwrap().unwrap();
            assert_eq!(document["_rev"], receipt.rev);
            assert_eq!(document["value"], 129 - n);
        }
        t.core.builds.lock().remove(&definition.generation);
        t.engine().verify().unwrap();
    }
}
