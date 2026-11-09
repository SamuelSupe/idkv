use super::TenantHandle;
use crate::{
    Error, Result, codec,
    model::*,
    resources::Reservation,
    storage::{Snapshot, Transaction},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::Arc;
use uuid::Uuid;

const MAX_WRITE_INDEX_CACHE_BYTES: usize = 256 << 10;

pub(crate) struct CachedCollection {
    name: String,
    id: Uuid,
    seq: u64,
    _memory: Reservation,
}

pub(crate) struct WriteIndexes {
    catalog_seq: u64,
    collection: Uuid,
    pub(super) entries: Vec<IndexDefinition>,
    _memory: Option<Reservation>,
}

pub(crate) fn name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 128 || name.chars().any(char::is_control) {
        return Err(Error::invalid(
            "name must contain 1..128 bytes without control characters",
        ));
    }
    Ok(())
}
pub(crate) fn named(prefix: &[u8], name: &str) -> Vec<u8> {
    let mut key = prefix.to_vec();
    codec::escape(name.as_bytes(), &mut key);
    key
}
pub(crate) fn prefix(kind: u8, collection: Uuid) -> Vec<u8> {
    let mut out = vec![kind];
    out.extend(collection.as_bytes());
    out
}
pub(crate) fn document_key(collection: Uuid, key: &str) -> Vec<u8> {
    named(&prefix(b'D', collection), key)
}
pub(crate) fn index_prefix(collection: Uuid, generation: Uuid) -> Vec<u8> {
    let mut out = prefix(b'I', collection);
    out.extend(generation.as_bytes());
    out
}
pub(crate) fn delta_prefix(collection: Uuid, generation: Uuid) -> Vec<u8> {
    let mut out = prefix(b'B', collection);
    out.extend(generation.as_bytes());
    out
}
pub(crate) fn parse<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|e| Error::corrupt(e.to_string()))
}
pub(crate) fn put<T: Serialize>(tx: &mut Transaction<'_>, key: Vec<u8>, value: &T) -> Result<()> {
    tx.put(key, serde_json::to_vec(value)?)
}
pub(crate) fn collection_tx(tx: &Transaction<'_>, name: &str) -> Result<Collection> {
    name_check(name)?;
    let value = tx
        .get(&named(b"C", name))?
        .ok_or_else(|| Error::missing("collection"))?;
    parse(&value)
}
fn name_check(value: &str) -> Result<()> {
    name(value)
}
pub(crate) fn collection_snapshot(snapshot: &Snapshot, name: &str) -> Result<Collection> {
    name_check(name)?;
    let value = snapshot
        .get(&named(b"C", name), &mut ReadStats::default())?
        .ok_or_else(|| Error::missing("collection"))?;
    parse(&value)
}
pub(crate) fn list_snapshot<T: DeserializeOwned>(
    snapshot: &Snapshot,
    prefix: &[u8],
    max: usize,
) -> Result<Vec<T>> {
    let mut stats = ReadStats::default();
    let mut scan = snapshot.scan(prefix.to_vec(), codec::prefix_end(prefix), None, &mut stats)?;
    let mut out = Vec::new();
    while let Some(record) = scan.next(&mut stats)? {
        if out.len() >= max {
            return Err(Error::limit("metadata limit"));
        }
        out.push(parse(&record.value.unwrap())?);
    }
    Ok(out)
}
pub(crate) fn indexes_tx(tx: &Transaction<'_>, collection: Uuid) -> Result<Vec<IndexDefinition>> {
    tx.prefix(&prefix(b'J', collection), 32)?
        .into_iter()
        .map(|(_, v)| parse(&v))
        .collect()
}
pub(crate) fn retain_receipt_namespace(
    tx: &mut Transaction<'_>,
    collection: &Collection,
) -> Result<()> {
    let namespace = named(b"r", &collection.name);
    if tx.get(&namespace)?.is_none()
        && !tx.prefix_first(&prefix(b'R', collection.id), 1)?.is_empty()
    {
        put(tx, namespace, &collection.id)?;
    }
    Ok(())
}
pub(crate) fn queue_gc(tx: &mut Transaction<'_>, prefixes: Vec<Vec<u8>>) -> Result<()> {
    put(
        tx,
        named(b"G", &Uuid::new_v4().to_string()),
        &GcJob {
            prefixes,
            position: 0,
            after: None,
        },
    )
}
impl TenantHandle {
    pub(super) fn indexes_for_write(
        &self,
        tx: &Transaction<'_>,
        collection: Uuid,
    ) -> Result<Arc<WriteIndexes>> {
        // A writer can observe a catalog change before its fsync is published.
        let seq = tx.catalog_read_seq;
        let mut cache = self.core.write_indexes.lock();
        if let Some(entry) = cache.as_ref()
            && entry.catalog_seq == seq
            && entry.collection == collection
        {
            return Ok(entry.clone());
        }
        *cache = None;
        let entries = indexes_tx(tx, collection)?;
        let memory = crate::json::encoded_len(&entries)
            .ok()
            .map(|len| len.saturating_mul(4).saturating_add(512))
            .filter(|size| *size <= MAX_WRITE_INDEX_CACHE_BYTES)
            .and_then(|size| self.engine().budget.reserve(size).ok());
        let cacheable = memory.is_some();
        let entry = Arc::new(WriteIndexes {
            catalog_seq: seq,
            collection,
            entries,
            _memory: memory,
        });
        if cacheable {
            *cache = Some(entry.clone());
        }
        Ok(entry)
    }

    pub(crate) fn collection_id(&self, snapshot: &Snapshot, name: &str) -> Result<Uuid> {
        let seq = self.engine().catalog_seq();
        // An older ReadView must resolve its own collection generation. A
        // single budgeted entry keeps the primary-key path independent of indexes.
        let cacheable = seq <= snapshot.seq();
        if cacheable {
            let cache = self.core.collection_id.read();
            if let Some(entry) = cache.as_ref()
                && entry.seq == seq
                && entry.name == name
            {
                return Ok(entry.id);
            }
        }
        let collection = collection_snapshot(snapshot, name)?;
        if cacheable && let Ok(memory) = self.engine().budget.reserve(name.len() + 128) {
            let mut cache = self.core.collection_id.write();
            if cache.as_ref().is_none_or(|entry| entry.seq <= seq) {
                *cache = Some(CachedCollection {
                    name: name.into(),
                    id: collection.id,
                    seq,
                    _memory: memory,
                });
            }
        }
        Ok(collection.id)
    }

    pub fn create_collection(&self, collection_name: &str) -> Result<Collection> {
        let _permit = self.core.gate.enter()?;
        self.touch();
        crate::identity::unmanaged(collection_name)?;
        name(collection_name)?;
        self.engine().transact(|tx| {
            let key = named(b"C", collection_name);
            if tx.get(&key)?.is_some() {
                return Err(Error::new("ALREADY_EXISTS", "collection"));
            }
            if tx.prefix(b"C", 1024)?.len() >= 1024 {
                return Err(Error::limit("collection count limit"));
            }
            let collection = Collection {
                id: Uuid::new_v4(),
                name: collection_name.into(),
            };
            put(tx, key, &collection)?;
            Ok(collection)
        })
    }
    pub fn collections(&self) -> Result<Vec<Collection>> {
        let _permit = self.core.gate.enter()?;
        self.touch();
        list_snapshot(&self.engine().snapshot()?, b"C", 1024)
    }
    pub fn drop_collection(&self, name: &str) -> Result<()> {
        crate::identity::unmanaged(name)?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        self.engine().transact(|tx| {
            let collection = collection_tx(tx, name)?;
            super::datasets::mutable(tx, collection.id)?;
            // Preserve the receipt namespace across drop/recreate. Existing UUID
            // namespaces stay readable without rewriting every retained receipt.
            retain_receipt_namespace(tx, &collection)?;
            tx.delete(named(b"C", name))?;
            queue_gc(
                tx,
                b"DIJLB"
                    .iter()
                    .map(|kind| prefix(*kind, collection.id))
                    .collect(),
            )
        })?;
        self.core.plans.lock().clear();
        Ok(())
    }
    pub fn indexes(&self, name: &str) -> Result<Vec<IndexDefinition>> {
        let _permit = self.core.gate.enter()?;
        self.touch();
        let snapshot = self.engine().snapshot()?;
        let collection = collection_snapshot(&snapshot, name)?;
        list_snapshot(&snapshot, &prefix(b'J', collection.id), 32)
    }
    pub fn drop_index(&self, collection_name: &str, index_name: &str) -> Result<()> {
        crate::identity::unmanaged(collection_name)?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        self.engine().transact(|tx| {
            let collection = collection_tx(tx, collection_name)?;
            super::datasets::mutable(tx, collection.id)?;
            let key = named(&prefix(b'J', collection.id), index_name);
            let def: IndexDefinition =
                parse(&tx.get(&key)?.ok_or_else(|| Error::missing("index"))?)?;
            for (_, bytes) in tx.prefix(&prefix(b'L', collection.id), 1024)? {
                let lookup: LookupDefinition = parse(&bytes)?;
                if lookup.index == index_name {
                    return Err(Error::new("CONFLICT", "index is referenced by a lookup"));
                }
            }
            tx.delete(key)?;
            queue_gc(
                tx,
                vec![
                    index_prefix(collection.id, def.generation),
                    delta_prefix(collection.id, def.generation),
                ],
            )
        })
    }
    pub fn create_lookup(&self, collection_name: &str, definition: LookupDefinition) -> Result<()> {
        crate::identity::unmanaged(collection_name)?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        name(&definition.name)?;
        if definition.max_matches == 0
            || definition.max_matches > 1000
            || definition.select.len() > 64
        {
            return Err(Error::invalid("lookup result limits"));
        }
        for path in &definition.select {
            codec::validate_pointer(path)?;
        }
        self.engine().transact(|tx| {
            let collection = collection_tx(tx, collection_name)?;
            super::datasets::mutable(tx, collection.id)?;
            let bytes = tx
                .get(&named(&prefix(b'J', collection.id), &definition.index))?
                .ok_or_else(|| Error::missing("lookup index"))?;
            let index: IndexDefinition = parse(&bytes)?;
            if index.state != IndexState::Ready {
                return Err(Error::new(
                    "INDEX_NOT_READY",
                    "lookup requires a ready index",
                ));
            }
            let key = named(&prefix(b'L', collection.id), &definition.name);
            if tx.get(&key)?.is_none()
                && tx.prefix(&prefix(b'L', collection.id), 1024)?.len() >= 1024
            {
                return Err(Error::limit("lookup count limit"));
            }
            put(tx, key, &definition)
        })?;
        self.core.plans.lock().clear();
        Ok(())
    }
    pub fn lookups(&self, name: &str) -> Result<Vec<LookupDefinition>> {
        let _permit = self.core.gate.enter()?;
        self.touch();
        let snapshot = self.engine().snapshot()?;
        let collection = collection_snapshot(&snapshot, name)?;
        list_snapshot(&snapshot, &prefix(b'L', collection.id), 1024)
    }
    pub fn drop_lookup(&self, collection_name: &str, lookup_name: &str) -> Result<()> {
        crate::identity::unmanaged(collection_name)?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        self.engine().transact(|tx| {
            let collection = collection_tx(tx, collection_name)?;
            super::datasets::mutable(tx, collection.id)?;
            tx.delete(named(&prefix(b'L', collection.id), lookup_name))
        })?;
        self.core.plans.lock().clear();
        Ok(())
    }
    pub(crate) fn gc_step(&self) -> Result<()> {
        let snapshot = self.engine().snapshot()?;
        let mut stats = ReadStats::default();
        let mut jobs = snapshot.scan(b"G".to_vec(), Some(b"H".to_vec()), None, &mut stats)?;
        let Some(job) = jobs.next(&mut stats)? else {
            return Ok(());
        };
        let mut state: GcJob = parse(&job.value.unwrap())?;
        let mut keys = Vec::new();
        while let Some(prefix) = state.prefixes.get(state.position) {
            let start = state.after.as_ref().map_or_else(
                || prefix.clone(),
                |key| {
                    let mut next = key.clone();
                    next.push(0);
                    next
                },
            );
            let mut scan = snapshot.scan(start, codec::prefix_end(prefix), None, &mut stats)?;
            while keys.len() < 256 {
                if let Some(record) = scan.next(&mut stats)? {
                    state.after = Some(record.key.clone());
                    keys.push(record.key);
                } else {
                    state.position += 1;
                    state.after = None;
                    break;
                }
            }
            if keys.len() >= 256 {
                break;
            }
        }
        self.engine().transact_maintenance(|tx| {
            for key in keys {
                tx.delete(key)?;
            }
            if state.position == state.prefixes.len() {
                tx.delete(job.key)?;
            } else {
                put(tx, job.key, &state)?;
            }
            Ok(())
        })
    }
}

#[derive(Serialize, Deserialize)]
struct GcJob {
    prefixes: Vec<Vec<u8>>,
    position: usize,
    after: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use crate::{Options, Store};

    #[test]
    fn collection_resolution_preserves_snapshot_generations() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path(), Options::default()).unwrap();
        let id = store.create_tenant("snapshot").unwrap().id;
        let tenant = store.tenant(id).unwrap();
        let old_id = tenant.create_collection("assets").unwrap().id;
        let old = tenant.engine().snapshot().unwrap();
        assert_eq!(tenant.collection_id(&old, "assets").unwrap(), old_id);
        tenant.drop_collection("assets").unwrap();
        let absent = tenant.engine().snapshot().unwrap();
        let new_id = tenant.create_collection("assets").unwrap().id;
        let new = tenant.engine().snapshot().unwrap();
        assert_eq!(tenant.collection_id(&new, "assets").unwrap(), new_id);
        assert_eq!(tenant.collection_id(&old, "assets").unwrap(), old_id);
        assert_eq!(
            tenant.collection_id(&absent, "assets").unwrap_err().code,
            "NOT_FOUND"
        );
        assert_eq!(tenant.collection_id(&new, "assets").unwrap(), new_id);
    }
    #[test]
    fn edge_review_collection_gc_keeps_unexpired_write_receipts() {
        use crate::{WriteOp, WriteOptions};
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path(), Options::default()).unwrap();
        let tenant = store
            .tenant(store.create_tenant("receipt-gc").unwrap().id)
            .unwrap();
        drop(store);
        tenant.create_collection("docs").unwrap();
        tenant.set_retention(60).unwrap();
        let context = tenant.retry_context("writer").unwrap();
        let options = WriteOptions {
            operation_id: Some("op".into()),
            expected_history: Some(context.history),
            retry_token: Some(context.retry_token),
        };
        let operations = vec![WriteOp::Put {
            document: serde_json::json!({"value":"once"}),
            if_rev: None,
        }];
        let first = tenant
            .write_batch_with("docs", operations.clone(), &options, "writer")
            .unwrap();
        tenant.drop_collection("docs").unwrap();
        for _ in 0..16 {
            tenant.gc_step().unwrap();
            tenant.retention_step().unwrap();
        }
        tenant.create_collection("docs").unwrap();
        let before = tenant.engine().seq();
        let retry = tenant
            .write_batch_with("docs", operations, &options, "writer")
            .unwrap();
        assert_eq!(retry[0].key, first[0].key);
        assert_eq!(retry[0].rev, first[0].rev);
        assert_eq!(tenant.engine().seq(), before);
        assert!(tenant.get("docs", &retry[0].key).unwrap().is_none());
    }
}
