use super::{
    TenantHandle,
    catalog::*,
    delta::Delta,
    indexes::{index_entry, maintain_index},
};
use crate::{Error, Result, codec, model::*};
use serde_json::Value;
use uuid::Uuid;

pub(super) fn point_read_memory(bytes: usize) -> usize {
    bytes * 8 + 256
}

impl TenantHandle {
    pub fn put(
        &self,
        collection: &str,
        document: Value,
        if_rev: Option<u64>,
    ) -> Result<WriteResult> {
        Ok(self
            .write_batch(collection, vec![WriteOp::Put { document, if_rev }])?
            .remove(0))
    }
    pub fn delete(&self, collection: &str, key: &str, if_rev: Option<u64>) -> Result<WriteResult> {
        Ok(self
            .write_batch(
                collection,
                vec![WriteOp::Delete {
                    key: key.into(),
                    if_rev,
                }],
            )?
            .remove(0))
    }
    pub fn write_batch(
        &self,
        collection_name: &str,
        operations: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        self.write_batch_with(
            collection_name,
            operations,
            &WriteOptions::default(),
            "embedded",
        )
    }

    /// Receipts share the batch WAL and obey tenant retention when configured.
    /// Expired retries are rejected. Reusing an ID with different input is a conflict, including
    /// after restart. The caller authenticates and supplies a stable principal.
    pub fn write_batch_with(
        &self,
        collection_name: &str,
        operations: Vec<WriteOp>,
        options: &WriteOptions,
        principal: &str,
    ) -> Result<Vec<WriteResult>> {
        crate::identity::unmanaged(collection_name)?;
        name(principal)?;
        if options
            .expected_history
            .as_ref()
            .is_some_and(|history| history.len() > 128)
        {
            return Err(Error::invalid("history token too long"));
        }
        if let Some(id) = &options.operation_id {
            name(id)?;
        }
        let conditional = operations.iter().any(|op| match op {
            WriteOp::Check { if_rev, .. } => *if_rev > 0,
            WriteOp::Put { if_rev, .. } | WriteOp::Delete { if_rev, .. } => {
                if_rev.is_some_and(|r| r > 0)
            }
        });
        let _permit = self.core.gate.enter()?;
        self.touch();
        if operations.is_empty() || operations.len() > self.engine().options.max_batch_operations {
            return Err(Error::invalid(
                "batch operation count exceeds configured limit",
            ));
        }
        let size = crate::json::encoded_len(&operations)?;
        if size > self.engine().options.max_batch_bytes {
            return Err(Error::limit("write batch exceeds configured byte limit"));
        }
        let mut result_bound = 2;
        for operation in &operations {
            let key = match operation {
                WriteOp::Check { key, .. } => {
                    validate_key(key)?;
                    continue;
                }
                WriteOp::Put { document, .. } => document
                    .get("_key")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("00000000-0000-0000-0000-000000000000"),
                WriteOp::Delete { key, .. } => key,
            };
            result_bound += crate::json::encoded_len(key)? + 256;
        }
        if result_bound > self.engine().options.max_response_bytes {
            return Err(Error::limit(
                "batch result would exceed response byte limit",
            ));
        }
        let _memory = self.engine().budget.reserve(size * 4 + 4096)?;
        let fingerprint = options
            .operation_id
            .as_ref()
            .map(|_| super::contracts::digest(&(options, &operations)))
            .transpose()?;

        // Let the shared maintenance pool flush normally. Foreground writers
        // only wait when the bounded write buffer approaches its stop threshold.
        if self.engine().write_buffer_full() {
            self.engine().flush_if_needed()?;
        }
        self.engine().transact(|tx| {
            let collection = collection_tx(tx, collection_name)?;
            let receipt_key = options
                .operation_id
                .as_ref()
                .map(|id| -> Result<Vec<u8>> {
                    let namespace = tx
                        .get(&named(b"r", collection_name))?
                        .map(|bytes| parse::<Uuid>(&bytes))
                        .transpose()?
                        .unwrap_or(collection.id);
                    let mut key = prefix(b'R', namespace);
                    codec::escape(principal.as_bytes(), &mut key);
                    codec::escape(id.as_bytes(), &mut key);
                    Ok(key)
                })
                .transpose()?;
            if let Some(key) = &receipt_key
                && let Some(bytes) = tx.get(key)?
            {
                super::retention::check_tx(tx, key, true, "IDEMPOTENCY_EXPIRED")?;
                let receipt: super::contracts::Receipt = parse(&bytes)?;
                if Some(&receipt.digest) != fingerprint.as_ref() {
                    return Err(Error::new(
                        "OPERATION_CONFLICT",
                        "operation ID was used for different input",
                    ));
                }
                return Ok(receipt.results);
            }
            let history = self.check_write_options(options, conditional)?;
            if options.operation_id.is_some() {
                super::retention::authorize_new(tx, options.retry_token, principal, &history)?;
            }
            // Evaluate the complete read set before staging any write. A check
            // after a Put must not accidentally validate that Put's new value.
            for operation in &operations {
                if let WriteOp::Check { key, if_rev } = operation {
                    let old = tx.get(&document_key(collection.id, key))?;
                    let revision = old
                        .as_ref()
                        .map(|bytes| codec::field(bytes, "/_rev"))
                        .transpose()?
                        .flatten()
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    if revision != *if_rev {
                        return Err(Error::new(
                            "REVISION_CONFLICT",
                            "checked document revision changed",
                        ));
                    }
                }
            }
            let indexes = if operations
                .iter()
                .any(|op| !matches!(op, WriteOp::Check { .. }))
            {
                super::datasets::mutable(tx, collection.id)?;
                indexes_tx(tx, collection.id)?
            } else {
                Vec::new()
            };
            let mut results = Vec::with_capacity(operations.len());
            let mut index_changes = std::collections::BTreeMap::new();
            let mut index_memory = Vec::new();
            for (ordinal, operation) in operations.into_iter().enumerate() {
                let (key, document, if_rev) = match operation {
                    WriteOp::Check { .. } => continue,
                    WriteOp::Put {
                        mut document,
                        if_rev,
                    } => {
                        let map = document
                            .as_object_mut()
                            .ok_or_else(|| Error::invalid("document must be an object"))?;
                        if map.contains_key("_rev") {
                            return Err(Error::invalid("_rev is server generated; use if_rev"));
                        }
                        let key = match map.get("_key") {
                            Some(Value::String(key)) => key.clone(),
                            None => Uuid::new_v4().to_string(),
                            _ => return Err(Error::invalid("_key must be a string")),
                        };
                        validate_key(&key)?;
                        map.insert("_key".into(), Value::String(key.clone()));
                        map.insert("_rev".into(), tx.seq.into());
                        {
                            let encoded = codec::encode(&document)?;
                            if encoded.len() > self.engine().options.max_document_bytes {
                                return Err(Error::limit("document byte limit"));
                            }
                            (key, Some(encoded), if_rev)
                        }
                    }
                    WriteOp::Delete { key, if_rev } => {
                        validate_key(&key)?;
                        (key, None, if_rev)
                    }
                };
                let data_key = document_key(collection.id, &key);
                let old = tx.get(&data_key)?;
                let old_rev = old
                    .as_ref()
                    .map(|bytes| codec::field(bytes, "/_rev"))
                    .transpose()?
                    .flatten()
                    .and_then(|v| v.as_u64());
                if if_rev.is_some_and(|expected| old_rev.unwrap_or(0) != expected) {
                    return Err(Error::new("REVISION_CONFLICT", "document revision changed"));
                }
                for (index_number, index) in indexes.iter().enumerate() {
                    if index.state == IndexState::Failed {
                        continue;
                    }
                    let old_entry = old
                        .as_ref()
                        .map(|d| index_entry(collection.id, index, d))
                        .transpose()?
                        .flatten();
                    let new_entry = document
                        .as_ref()
                        .map(|d| index_entry(collection.id, index, d))
                        .transpose()?
                        .flatten();
                    if index.state == IndexState::Ready && !index.spec.unique {
                        maintain_index(tx, index, old_entry.as_ref(), new_entry.as_ref())?;
                    } else if index.state == IndexState::Ready {
                        let bytes = old_entry
                            .iter()
                            .chain(new_entry.iter())
                            .map(|entry| {
                                entry.key.len() + entry.value.len() + entry.document_key.len() + 256
                            })
                            .sum::<usize>();
                        index_memory.push(self.engine().budget.reserve(bytes + 256)?);
                        index_changes
                            .entry((index_number, key.clone()))
                            .and_modify(|change: &mut Delta| change.new = new_entry.clone())
                            .or_insert(Delta {
                                old: old_entry,
                                new: new_entry,
                            });
                    } else {
                        let mut delta_key = delta_prefix(collection.id, index.generation);
                        delta_key.extend(tx.seq.to_be_bytes());
                        delta_key.extend((ordinal as u32).to_be_bytes());
                        tx.put(
                            delta_key,
                            Delta {
                                old: old_entry,
                                new: new_entry,
                            }
                            .encode(),
                        )?;
                    }
                }
                let deleted = document.is_none();
                if let Some(document) = document {
                    tx.put(data_key, document)?;
                } else {
                    tx.delete(data_key)?;
                }
                results.push(WriteResult {
                    key,
                    rev: tx.seq,
                    deleted,
                    history: history.clone(),
                });
            }
            // Unique constraints apply to the final batch, including swaps.
            for ((number, _), change) in &index_changes {
                maintain_index(tx, &indexes[*number], change.old.as_ref(), None)?;
            }
            for ((number, _), change) in &index_changes {
                maintain_index(tx, &indexes[*number], None, change.new.as_ref())?;
            }
            if let Some(key) = receipt_key {
                put(
                    tx,
                    key.clone(),
                    &super::contracts::Receipt {
                        digest: fingerprint.unwrap(),
                        results: results.clone(),
                    },
                )?;
                super::retention::schedule_receipt(tx, &key, options.retry_token)?;
            }
            Ok(results)
        })
    }
    pub fn get(&self, collection: &str, key: &str) -> Result<Option<Value>> {
        self.get_controlled(collection, key, None)
    }
    pub(crate) fn get_controlled(
        &self,
        collection: &str,
        key: &str,
        budget: Option<&crate::read::ReadBudget>,
    ) -> Result<Option<Value>> {
        let _permit = self.core.gate.enter()?;
        self.touch();
        validate_key(key)?;
        let snapshot = self.engine().snapshot()?;
        let collection = self.collection_id(&snapshot, collection)?;
        let encoded = document_key(collection, key);
        let bytes = if let Some(budget) = budget {
            snapshot.get_controlled(&encoded, &mut ReadStats::default(), budget)?
        } else {
            snapshot.get(&encoded, &mut ReadStats::default())?
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let _memory = self
            .engine()
            .budget
            .reserve(point_read_memory(bytes.len()))?;
        let value = codec::decode(&bytes)?;
        if crate::json::encoded_len(&value)? > self.engine().options.max_response_bytes {
            return Err(Error::limit("response byte limit"));
        }
        Ok(Some(value))
    }
    pub fn multi_get(&self, collection_name: &str, keys: &[String]) -> Result<Vec<Option<Value>>> {
        self.read_with(&crate::ReadOptions::default())?
            .multi_get(collection_name, keys)
    }

    pub(crate) fn multi_get_controlled(
        &self,
        collection_name: &str,
        keys: &[String],
        budget: &crate::read::ReadBudget,
    ) -> Result<Vec<Option<Value>>> {
        budget.check()?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        if keys.len() > self.engine().options.max_read_keys {
            return Err(Error::limit("multi_get input limit"));
        }
        for key in keys {
            validate_key(key)?;
        }
        let snapshot = self.engine().snapshot()?;
        let collection = self.collection_id(&snapshot, collection_name)?;
        let encoded: Vec<_> = keys.iter().map(|k| document_key(collection, k)).collect();
        let values = snapshot.multi_get_controlled(&encoded, &mut ReadStats::default(), budget)?;
        let mut memory = Vec::new();
        let mut size = 2 + keys.len().saturating_sub(1);
        let mut output = Vec::with_capacity(values.len());
        for value in values {
            budget.check()?;
            if let Some(value) = value {
                memory.push(self.engine().budget.reserve(value.len() * 8 + 256)?);
                let document = codec::decode(&value)?;
                size += crate::json::encoded_len(&document)?;
                output.push(Some(document));
            } else {
                size += 4;
                output.push(None);
            }
            if size > self.engine().options.max_response_bytes {
                return Err(Error::limit("response byte limit"));
            }
        }
        budget.check()?;
        Ok(output)
    }
}
pub(crate) fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > 512 {
        return Err(Error::invalid("_key must contain 1..512 bytes"));
    }
    Ok(())
}
