use super::{LookupPlan, execute::row_key};
use crate::resources::Bytes;
use crate::{
    Error, Result, codec,
    model::*,
    tenant::{
        TenantHandle,
        catalog::*,
        indexes::{covers, tuple},
    },
};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

impl TenantHandle {
    pub fn lookup_many(
        &self,
        collection_name: &str,
        lookup_name: &str,
        inputs: &[Value],
    ) -> Result<LookupResult> {
        self.read_with(&crate::ReadOptions::default())?.lookup_many(
            collection_name,
            lookup_name,
            inputs,
        )
    }
    pub(crate) fn lookup_snapshot(
        &self,
        collection_name: &str,
        lookup_name: &str,
        inputs: &[Value],
        snapshot: crate::storage::Snapshot,
        budget: &crate::read::ReadBudget,
    ) -> Result<LookupResult> {
        budget.check()?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        if inputs.len() > self.engine().options.max_read_keys {
            return Err(Error::limit("lookup input limit"));
        }
        let input_size = crate::json::encoded_len(inputs)?;
        if input_size > self.engine().options.max_batch_bytes {
            return Err(Error::limit("lookup input bytes"));
        }
        let _memory = self.engine().budget.reserve(input_size * 4 + 4096)?;
        let collection = self.collection_id(&snapshot, collection_name)?;
        let catalog_seq = self.engine().catalog_seq();
        let cached = self
            .core
            .plans
            .lock()
            .get(&(collection, lookup_name.to_string()))
            .cloned();
        let plan = if let Some(plan) =
            cached.filter(|p| p.catalog_seq == catalog_seq && catalog_seq <= snapshot.seq())
        {
            plan
        } else {
            let definition: LookupDefinition = parse(
                &snapshot
                    .get(
                        &named(&prefix(b'L', collection), lookup_name),
                        &mut ReadStats::default(),
                    )?
                    .ok_or_else(|| Error::missing("lookup"))?,
            )?;
            let index: IndexDefinition = parse(
                &snapshot
                    .get(
                        &named(&prefix(b'J', collection), &definition.index),
                        &mut ReadStats::default(),
                    )?
                    .ok_or_else(|| Error::missing("lookup index"))?,
            )?;
            if index.state != IndexState::Ready || index.ready_seq > snapshot.seq() {
                return Err(Error::new("INDEX_NOT_READY", "lookup index not ready"));
            }
            let memory = self
                .engine()
                .budget
                .reserve(serde_json::to_vec(&(&definition, &index))?.len() * 4 + 512)?;
            let plan = Arc::new(LookupPlan {
                _memory: memory,
                definition,
                index,
                catalog_seq: if catalog_seq <= snapshot.seq() {
                    catalog_seq
                } else {
                    u64::MAX
                },
            });
            let mut plans = self.core.plans.lock();
            if plans.len() >= 1024 {
                plans.clear();
            }
            plans.insert((collection, lookup_name.into()), plan.clone());
            plan
        };
        let mut unique_keys = BTreeMap::new();
        let mut mapping = Vec::new();
        for input in inputs {
            budget.check()?;
            if !input.is_object() {
                return Err(Error::invalid("lookup input must be an object"));
            }
            let values: Vec<_> = plan
                .index
                .spec
                .fields
                .iter()
                .map(|field| input.pointer(&field.field).cloned())
                .collect();
            if values.iter().any(Option::is_none)
                || (plan.index.spec.unique
                    && values
                        .iter()
                        .any(|v| v.as_ref().is_some_and(Value::is_null)))
            {
                mapping.push(None);
                continue;
            }
            let mut key = index_prefix(collection, plan.index.generation);
            key.extend(tuple(&plan.index, &values)?);
            if key.len() > codec::MAX_KEY {
                return Err(Error::limit("lookup key length"));
            }
            unique_keys
                .entry(key.clone())
                .or_insert_with(Vec::<Arc<Bytes>>::new);
            mapping.push(Some(key));
        }
        let mut stats = ReadStats::default();
        if plan.index.spec.unique {
            let keys: Vec<_> = unique_keys.keys().cloned().collect();
            for (key, value) in keys
                .iter()
                .zip(snapshot.multi_get_controlled(&keys, &mut stats, budget)?)
            {
                if let Some(value) = value {
                    unique_keys.get_mut(key).unwrap().push(value);
                    stats.scanned_entries += 1;
                }
            }
        } else {
            let keys: Vec<_> = unique_keys.keys().cloned().collect();
            let ranges = keys
                .iter()
                .map(|key| crate::storage::ScanRange {
                    start: key.clone(),
                    end: codec::prefix_end(key),
                    prefix: Some(key.clone()),
                })
                .collect();
            let mut scan = snapshot.scan_ranges(ranges, &mut stats, Some(budget))?;
            while let Some(record) = scan.next(&mut stats)? {
                budget.check_scan(&stats)?;
                let values = unique_keys.get_mut(&keys[scan.range_index()]).unwrap();
                if values.len() >= plan.definition.max_matches {
                    return Err(Error::new("MATCH_LIMIT_EXCEEDED", "lookup match limit"));
                }
                values.push(record.value.unwrap());
                if plan.definition.mode == MatchMode::First {
                    scan.next_range(&mut stats)?;
                }
            }
        }
        if !covers(&plan.index, &plan.definition.select) {
            let mut locations = Vec::new();
            let mut keys = Vec::new();
            for (tuple, rows) in &unique_keys {
                for (i, row) in rows.iter().enumerate() {
                    locations.push((tuple.clone(), i));
                    keys.push(document_key(collection, &row_key(row)?));
                }
            }
            for (key_chunk, location_chunk) in keys.chunks(256).zip(locations.chunks(256)) {
                stats.document_reads += key_chunk.len() as u64;
                for ((tuple, i), value) in location_chunk
                    .iter()
                    .zip(snapshot.multi_get_controlled(key_chunk, &mut stats, budget)?)
                {
                    unique_keys.get_mut(tuple).unwrap()[*i] =
                        value.ok_or_else(|| Error::corrupt("index points to missing document"))?;
                }
            }
        }
        let mut results = BTreeMap::new();
        let mut result_memory = Vec::new();
        let mut projected_bytes = 0;
        for (key, values) in unique_keys {
            let values = values
                .iter()
                .map(|v| codec::project(v, &plan.definition.select))
                .collect::<Result<Vec<_>>>()?;
            let size = crate::json::encoded_len(&values)?;
            projected_bytes += size;
            if projected_bytes > self.engine().options.max_response_bytes {
                return Err(Error::limit("lookup projection bytes"));
            }
            result_memory.push(self.engine().budget.reserve(size * 4 + 128)?);
            results.insert(key, (values, size));
        }
        let mut size = 2;
        let mut matches = Vec::with_capacity(inputs.len());
        for key in mapping {
            let entry = key.as_ref().and_then(|k| results.get(k));
            let row_size = entry.map_or(2, |(_, size)| *size);
            size += row_size + usize::from(!matches.is_empty());
            if size > self.engine().options.max_response_bytes {
                return Err(Error::limit("lookup response bytes"));
            }
            result_memory.push(self.engine().budget.reserve(row_size * 4 + 32)?);
            matches.push(entry.map(|(values, _)| values.clone()).unwrap_or_default());
        }
        budget.check_scan(&stats)?;
        Ok(LookupResult {
            matches,
            snapshot_seq: snapshot.seq(),
            stats,
        })
    }
}
