use super::{TenantHandle, catalog::document_key, documents::validate_key};
use crate::{Error, Result, codec, model::*, storage::Snapshot};
use std::collections::HashMap;
use uuid::Uuid;

impl TenantHandle {
    /// Execute reads across collections in this tenant using one snapshot.
    /// Limits apply to the entire batch. Query cursors must be absent on input;
    /// returned cursors continue the original view through `query`.
    pub fn read_batch(&self, operations: &[ReadOperation]) -> Result<ReadBatchResult> {
        self.read_with(&crate::ReadOptions::default())?
            .read_batch(operations)
    }

    pub(crate) fn read_batch_controlled(
        &self,
        operations: &[ReadOperation],
        budget: &crate::read::ReadBudget,
    ) -> Result<ReadBatchResult> {
        budget.check()?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        let history = self.history();
        let snapshot = self.engine().snapshot()?;
        self.read_batch_snapshot(operations, snapshot, history, None, budget)
    }

    pub(crate) fn read_batch_snapshot(
        &self,
        operations: &[ReadOperation],
        snapshot: Snapshot,
        history: String,
        publication: Option<(String, Uuid)>,
        budget: &crate::read::ReadBudget,
    ) -> Result<ReadBatchResult> {
        budget.check()?;
        let options = &self.engine().options;
        if operations.is_empty() || operations.len() > options.max_batch_operations {
            return Err(Error::limit("read batch operation limit"));
        }
        let input_size = crate::json::encoded_len(operations)?;
        if input_size > options.max_batch_bytes {
            return Err(Error::limit("read batch input bytes"));
        }
        let _input = self.engine().budget.reserve(input_size * 4 + 4096)?;
        let mut units = 0usize;
        for operation in operations {
            units = units.saturating_add(match &operation.request {
                ReadRequest::Get { key } => {
                    validate_key(key)?;
                    1
                }
                ReadRequest::MultiGet { keys } => {
                    for key in keys {
                        validate_key(key)?;
                    }
                    keys.len()
                }
                ReadRequest::Query { query } => {
                    if query.cursor.is_some() {
                        return Err(Error::invalid(
                            "read_batch cannot combine existing cursor snapshots",
                        ));
                    }
                    query.limit
                }
            });
        }
        if units > options.max_read_keys {
            return Err(Error::limit("read batch total keys and query limits"));
        }
        // All point reads share storage MultiGet, including across collections.
        let mut collections = HashMap::new();
        let mut keys = Vec::new();
        for operation in operations {
            let collection = match collections.get(&operation.collection) {
                Some(id) => *id,
                None => {
                    let id = self.collection_id(&snapshot, &operation.collection)?;
                    collections.insert(operation.collection.clone(), id);
                    id
                }
            };
            match &operation.request {
                ReadRequest::Get { key } => keys.push(document_key(collection, key)),
                ReadRequest::MultiGet { keys: input } => {
                    keys.extend(input.iter().map(|key| document_key(collection, key)));
                }
                ReadRequest::Query { .. } => {}
            }
        }
        let mut stats = ReadStats {
            document_reads: keys.len() as u64,
            ..Default::default()
        };
        budget.check()?;
        let values = snapshot.multi_get_controlled(&keys, &mut stats, budget)?;
        budget.check()?;
        let mut values = values.into_iter();
        let mut cursors = Vec::new();
        let result = (|| {
            let mut output_memory = Vec::new();
            let mut results = Vec::with_capacity(operations.len());
            let mut size = 2usize;
            let mut retained = 0usize;
            for operation in operations {
                budget.check_scan(&stats)?;
                let result = match &operation.request {
                    ReadRequest::Get { .. } | ReadRequest::MultiGet { .. } => {
                        let count = match &operation.request {
                            ReadRequest::Get { .. } => 1,
                            ReadRequest::MultiGet { keys } => keys.len(),
                            _ => unreachable!(),
                        };
                        let mut documents = Vec::with_capacity(count);
                        for _ in 0..count {
                            documents.push(match values.next().unwrap() {
                                Some(bytes) => {
                                    retained = retained.saturating_add(bytes.len() * 8 + 256);
                                    if retained > options.max_query_bytes {
                                        return Err(Error::limit("read batch memory limit"));
                                    }
                                    output_memory
                                        .push(self.engine().budget.reserve(bytes.len() * 8 + 256)?);
                                    Some(codec::decode(&bytes)?)
                                }
                                None => None,
                            });
                        }
                        if matches!(operation.request, ReadRequest::Get { .. }) {
                            ReadResult::Get {
                                document: documents.pop().unwrap(),
                            }
                        } else {
                            ReadResult::MultiGet { documents }
                        }
                    }
                    ReadRequest::Query { query } => {
                        let result = self.query_view(
                            &operation.collection,
                            query.clone(),
                            Some(snapshot.clone()),
                            publication.clone(),
                            budget,
                            stats.scanned_entries,
                        )?;
                        if let Some(cursor) = &result.cursor {
                            cursors.push(cursor.clone());
                        }
                        stats.add(&result.stats);
                        let result_bytes = crate::json::encoded_len(&result)?
                            .saturating_mul(8)
                            .saturating_add(256);
                        retained = retained.saturating_add(result_bytes);
                        if retained > options.max_query_bytes {
                            return Err(Error::limit("read batch memory limit"));
                        }
                        output_memory.push(self.engine().budget.reserve(result_bytes)?);
                        ReadResult::Query { result }
                    }
                };
                size = size.saturating_add(crate::json::encoded_len(&result)? + 1);
                if size > options.max_response_bytes {
                    return Err(Error::limit("read batch response bytes"));
                }
                results.push(result);
            }
            budget.check_scan(&stats)?;
            snapshot.check()?;
            if history != self.history() {
                return Err(Error::new(
                    "HISTORY_CONFLICT",
                    "read batch crossed a history change",
                ));
            }
            let result = ReadBatchResult {
                results,
                history,
                snapshot_seq: snapshot.seq(),
                stats,
            };
            if crate::json::encoded_len(&result)? > options.max_response_bytes {
                return Err(Error::limit("read batch response bytes"));
            }
            Ok(result)
        })();
        if result.is_err() {
            for cursor in cursors {
                let _ = self.release_cursor(&cursor);
            }
        }
        result
    }
}
