use super::*;
use crate::{
    codec,
    resources::Reservation,
    storage::{Snapshot, Transaction},
    tenant::catalog::{collection_tx, document_key, named, put},
};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

pub(super) struct Scratch<'a> {
    pub tenant: &'a TenantHandle,
    bytes: usize,
    memory: Vec<Reservation>,
    deadline: Option<Instant>,
    limit: usize,
}
impl<'a> Scratch<'a> {
    pub fn new(tenant: &'a TenantHandle) -> Self {
        Self {
            tenant,
            bytes: 0,
            memory: Vec::new(),
            deadline: Some(
                Instant::now() + Duration::from_millis(tenant.engine().options.query_ms),
            ),
            limit: tenant.engine().options.max_query_bytes,
        }
    }
    pub fn maintenance(tenant: &'a TenantHandle) -> Self {
        Self {
            tenant,
            bytes: 0,
            memory: Vec::new(),
            deadline: None,
            // Maintenance has its own row/byte/time limits; client query limits
            // must not permanently block background repair.
            limit: tenant
                .engine()
                .options
                .max_document_bytes
                .saturating_mul(32)
                .saturating_add(1 << 20),
        }
    }
    pub fn charge(&mut self, bytes: usize) -> Result<()> {
        self.check()?;
        self.bytes = self.bytes.saturating_add(bytes);
        if self.bytes > self.limit {
            return Err(Error::limit("identity temporary memory limit"));
        }
        self.memory
            .push(self.tenant.engine().budget.reserve(bytes)?);
        Ok(())
    }
    pub fn check(&self) -> Result<()> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            Err(Error::new(
                "DEADLINE_EXCEEDED",
                "identity operation deadline",
            ))
        } else {
            Ok(())
        }
    }
    pub fn decode<T: DeserializeOwned>(&mut self, bytes: &[u8]) -> Result<T> {
        self.charge(bytes.len().saturating_mul(16).saturating_add(4096))?;
        let data = codec::field(bytes, "/data")?
            .ok_or_else(|| Error::corrupt("identity record has no data"))?;
        serde_json::from_value(data).map_err(|e| Error::corrupt(e.to_string()))
    }
    pub fn read<T: DeserializeOwned>(
        &mut self,
        tx: &Transaction<'_>,
        collection: Uuid,
        key: &str,
    ) -> Result<Option<T>> {
        tx.get(&document_key(collection, key))?
            .map(|bytes| self.decode(&bytes))
            .transpose()
    }
    pub fn get<T: DeserializeOwned>(
        &mut self,
        snapshot: &Snapshot,
        collection: Uuid,
        key: &str,
        stats: &mut crate::ReadStats,
    ) -> Result<Option<T>> {
        stats.document_reads += 1;
        snapshot
            .get(&document_key(collection, key), stats)?
            .map(|bytes| self.decode(&bytes))
            .transpose()
    }
    // Fixed record schemas only. Dynamic entity attributes retain the JSON
    // decoder so arbitrary object keys never acquire serde's private meanings.
    fn decode_typed<T: DeserializeOwned>(&mut self, bytes: &[u8]) -> Result<T> {
        self.charge(bytes.len().saturating_mul(16).saturating_add(4096))?;
        codec::field_as(bytes, "/data")?
            .ok_or_else(|| Error::corrupt("identity record has no data"))
    }
    pub fn get_typed<T: DeserializeOwned>(
        &mut self,
        snapshot: &Snapshot,
        collection: Uuid,
        key: &str,
        stats: &mut crate::ReadStats,
    ) -> Result<Option<T>> {
        stats.document_reads += 1;
        snapshot
            .get(&document_key(collection, key), stats)?
            .map(|bytes| self.decode_typed(&bytes))
            .transpose()
    }
    pub fn write<T: Serialize>(
        &mut self,
        tx: &mut Transaction<'_>,
        collection: Uuid,
        key: &str,
        kind: &str,
        data: &T,
    ) -> Result<()> {
        let size = crate::json::encoded_len(data)?;
        self.charge(size.saturating_mul(8).saturating_add(4096))?;
        let value = serde_json::json!({"_key":key,"_rev":tx.seq,"record_type":kind,"data":data});
        let encoded = codec::encode(&value)?;
        if encoded.len() > self.tenant.engine().options.max_document_bytes {
            return Err(Error::limit("identity record exceeds document limit"));
        }
        tx.put(document_key(collection, key), encoded)?;
        tx.check_expanded_limits()
    }
    pub fn response<T: Serialize>(&self, value: &T) -> Result<()> {
        self.check()?;
        if crate::json::encoded_len(value)? > self.tenant.engine().options.max_response_bytes {
            Err(Error::limit("identity response byte limit"))
        } else {
            Ok(())
        }
    }
}

pub(super) fn digest(value: &impl Serialize) -> Result<String> {
    Ok(Sha256::digest(serde_json::to_vec(value)?)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}
pub(super) fn timeline_key(id: &Identifier) -> Result<String> {
    Ok(format!("timeline:{}", digest(id)?))
}
pub(super) fn entity_key(id: Uuid) -> String {
    format!("entity:{id}")
}
pub(super) fn operation_key(id: &str) -> Result<String> {
    Ok(format!("operation:{}", digest(&id)?))
}
pub(super) fn collection_for_write(tx: &Transaction<'_>) -> Result<Uuid> {
    Ok(collection_tx(tx, COLLECTION)?.id)
}
pub(super) fn create_collection(tx: &mut Transaction<'_>) -> Result<Uuid> {
    let key = named(b"C", COLLECTION);
    if tx.get(&key)?.is_some() {
        return Err(Error::new(
            "ALREADY_EXISTS",
            "identity collection exists without valid managed metadata",
        ));
    }
    if tx.prefix(b"C", 1024)?.len() >= 1024 {
        return Err(Error::limit("collection count limit"));
    }
    let id = Uuid::new_v4();
    put(
        tx,
        key,
        &crate::Collection {
            id,
            name: COLLECTION.into(),
        },
    )?;
    Ok(id)
}
