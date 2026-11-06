use crate::{
    Result,
    model::*,
    resources::Reservation,
    storage::Snapshot,
    tenant::{TenantHandle, catalog::*},
};
use std::{collections::HashMap, sync::Arc};

pub(crate) struct Catalog {
    pub collection: Collection,
    pub indexes: Vec<IndexDefinition>,
    _memory: Reservation,
}
#[derive(Default)]
pub(crate) struct CatalogCache {
    seq: u64,
    bytes: usize,
    entries: HashMap<String, Arc<Catalog>>,
}

pub(super) fn read(tenant: &TenantHandle, snapshot: &Snapshot, name: &str) -> Result<Arc<Catalog>> {
    let seq = tenant.engine().catalog_seq();
    // catalog_seq is published before the commit sequence. Old cursor snapshots
    // must read their own catalog; latest metadata cannot describe their indexes.
    let cacheable = seq <= snapshot.seq();
    if cacheable {
        let cache = tenant.core.query_catalog.read();
        if cache.seq == seq
            && let Some(entry) = cache.entries.get(name)
        {
            return Ok(entry.clone());
        }
    }
    let collection = collection_snapshot(snapshot, name)?;
    let indexes = list_snapshot(snapshot, &prefix(b'J', collection.id), 32)?;
    let size = crate::json::encoded_len(&(&collection, &indexes))? * 4 + name.len() * 2 + 512;
    let catalog = Arc::new(Catalog {
        collection,
        indexes,
        _memory: tenant.engine().budget.reserve(size)?,
    });
    if cacheable && size <= 4 << 20 {
        let mut cache = tenant.core.query_catalog.write();
        if cache.seq > seq {
            return Ok(catalog);
        }
        if cache.seq != seq || cache.entries.len() >= 128 || cache.bytes + size > 4 << 20 {
            cache.entries.clear();
            cache.bytes = 0;
            cache.seq = seq;
        }
        if let Some(existing) = cache.entries.get(name) {
            return Ok(existing.clone());
        }
        cache.bytes += size;
        cache.entries.insert(name.into(), catalog.clone());
    }
    Ok(catalog)
}
