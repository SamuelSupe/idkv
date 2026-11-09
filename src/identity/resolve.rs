use super::{storage::digest, timeline::validate_identifier, *};
use crate::{
    ReadOptions, ReadStats, TenantReader, codec,
    read::ReadBudget,
    resources::Reservation,
    storage::{ScanRange, Snapshot},
    tenant::catalog::document_key,
};
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;

impl TenantHandle {
    /// Resolve one exclusive identifier at event time. Unknown or expired knowledge
    /// versions fail instead of silently falling back to current knowledge.
    pub fn identity_resolve(&self, request: &ResolveRequest) -> Result<ResolveResult> {
        self.read_with(&ReadOptions::default())?
            .identity_resolve(request)
    }

    /// Resolve 1..max_read_keys events using one snapshot and knowledge version.
    /// Limits apply to the whole batch. Errors return no partial results.
    pub fn identity_resolve_many(&self, request: &ResolveManyRequest) -> Result<ResolveManyResult> {
        self.read_with(&ReadOptions::default())?
            .identity_resolve_many(request)
    }
}
impl TenantReader<'_> {
    pub fn identity_resolve(&self, request: &ResolveRequest) -> Result<ResolveResult> {
        self.budget.check()?;
        validate_identifier(&request.identifier)?;
        parse_time(&request.valid_at)?;
        if let Some(known) = request.known_at.as_deref().filter(|v| *v != "latest") {
            Uuid::parse_str(known)
                .map_err(|_| Error::invalid("known_at must be latest or a knowledge UUID"))?;
        }
        let mut result = self.identity_resolve_many(&ResolveManyRequest {
            known_at: request.known_at.clone(),
            items: vec![ResolveItem {
                identifier: request.identifier.clone(),
                valid_at: request.valid_at.clone(),
            }],
        })?;
        let matched = result.results.pop().unwrap();
        Ok(ResolveResult {
            knowledge_version: result.knowledge_version,
            timeline_revision: matched.timeline_revision,
            binding: matched.binding,
            stats: result.stats,
        })
    }

    /// Shares this reader's deadline and cancellation with validation and all I/O.
    /// A missing binding is a successful result, including for unknown identifiers.
    pub fn identity_resolve_many(&self, request: &ResolveManyRequest) -> Result<ResolveManyResult> {
        self.budget.check()?;
        let tenant = self.tenant;
        let options = &tenant.engine().options;
        if request.items.is_empty() || request.items.len() > options.max_read_keys {
            return Err(Error::limit("identity resolve_many item limit"));
        }
        let size = crate::json::encoded_len(request)?;
        if size > options.max_batch_bytes {
            return Err(Error::limit("identity resolve_many input bytes"));
        }
        let mut work = Resolution {
            tenant,
            budget: &self.budget,
            retained: 0,
            charges: Vec::new(),
            results: Vec::new(),
            response_bytes: 0,
        };
        work.retain(
            size.saturating_mul(4)
                .saturating_add(request.items.len() * 256 + 4096),
            0,
        )?;
        let known = request
            .known_at
            .as_deref()
            .filter(|v| *v != "latest")
            .map(|s| {
                Uuid::parse_str(s)
                    .map_err(|_| Error::invalid("known_at must be latest or a knowledge UUID"))
            })
            .transpose()?;
        let mut groups: BTreeMap<String, Group<'_>> = BTreeMap::new();
        for (index, item) in request.items.iter().enumerate() {
            self.budget.check()?;
            validate_identifier(&item.identifier)?;
            let at = parse_time(&item.valid_at)?;
            let group = groups
                .entry(digest(&item.identifier)?)
                .or_insert_with(|| Group {
                    identifier: &item.identifier,
                    events: Vec::new(),
                });
            if group.identifier != &item.identifier {
                return Err(Error::corrupt("identifier digest collision"));
            }
            group.events.push((index, at));
        }
        work.results = vec![ResolveMatch::default(); request.items.len()];
        let _permit = tenant.core.gate.enter()?;
        tenant.touch();
        let history = tenant.history();
        let snapshot = tenant.engine().snapshot()?;
        if self
            .budget
            .minimum_seq
            .is_some_and(|seq| snapshot.seq() < seq)
        {
            return Err(Error::new(
                "REPLICA_LAG",
                "identity snapshot is behind requested position",
            ));
        }
        let collection = tenant.collection_id(&snapshot, COLLECTION)?;
        let mut stats = ReadStats::default();
        let current: Knowledge = work
            .get(&snapshot, collection, "head:knowledge", &mut stats)?
            .ok_or_else(|| Error::corrupt("identity knowledge head missing"))?;
        let head = match known {
            Some(version) if version != current.version => {
                let key = format!("knowledge:{version}");
                let head: Knowledge = work.get(&snapshot, collection, &key, &mut stats)?
                    .ok_or_else(|| Error::new("KNOWLEDGE_UNAVAILABLE", "knowledge version absent from this tenant history or not yet replicated"))?;
                crate::tenant::retention::check_snapshot_controlled(
                    &snapshot,
                    &document_key(collection, &key),
                    true,
                    "KNOWLEDGE_UNAVAILABLE",
                    Some(&self.budget),
                    &mut stats,
                )?;
                head
            }
            _ => current,
        };
        // Digest order is also physical key order. Reuse cursors for historical
        // predecessor seeks, skipping all older revisions after the first live row.
        let groups: Vec<_> = groups.into_iter().collect();
        if known.is_some() {
            let ranges = groups
                .iter()
                .map(|(digest, _)| {
                    let text = format!("history:{digest}:");
                    let mut prefix = document_key(collection, &text);
                    prefix.truncate(prefix.len() - 2);
                    ScanRange {
                        start: document_key(collection, &format!("{text}{:016x}", !head.ordinal)),
                        end: codec::prefix_end(&prefix),
                        prefix: None,
                    }
                })
                .collect();
            let mut scan = snapshot.scan_ranges(ranges, &mut stats, Some(&self.budget))?;
            while let Some(record) = scan.next(&mut stats)? {
                stats.document_reads += 1;
                let bytes = record.value.as_ref().unwrap();
                work.resolve(&groups[scan.range_index()].1, bytes, bytes.len())?;
                scan.next_range(&mut stats)?;
            }
        } else {
            // Bound live raw timelines as well as decoded scratch; do not retain
            // every timeline in a large batch until the response has been built.
            for chunk in groups.chunks(8) {
                self.budget.check_scan(&stats)?;
                let keys: Vec<_> = chunk
                    .iter()
                    .map(|(digest, _)| document_key(collection, &format!("timeline:{digest}")))
                    .collect();
                let values = snapshot.multi_get_controlled(&keys, &mut stats, &self.budget)?;
                stats.document_reads += keys.len() as u64;
                let raw_bytes = values.iter().flatten().map(|v| v.len()).sum();
                for ((_, group), value) in chunk.iter().zip(&values) {
                    if let Some(bytes) = value {
                        work.resolve(group, bytes, raw_bytes)?;
                    }
                }
            }
        }
        self.budget.check_scan(&stats)?;
        snapshot.check()?;
        if history != tenant.history() {
            return Err(Error::new(
                "HISTORY_CONFLICT",
                "identity resolution crossed a history change",
            ));
        }
        let result = ResolveManyResult {
            knowledge_version: head.version,
            history,
            snapshot_seq: snapshot.seq(),
            results: std::mem::take(&mut work.results),
            stats,
        };
        if crate::json::encoded_len(&result)? > options.max_response_bytes {
            return Err(Error::limit("identity resolve_many response bytes"));
        }
        self.budget.check()?;
        Ok(result)
    }
}
struct Group<'a> {
    identifier: &'a Identifier,
    events: Vec<(usize, i64)>,
}
struct Resolution<'a> {
    tenant: &'a TenantHandle,
    budget: &'a ReadBudget,
    retained: usize,
    charges: Vec<Reservation>,
    results: Vec<ResolveMatch>,
    response_bytes: usize,
}
impl Resolution<'_> {
    fn reserve(&self, bytes: usize, live: usize) -> Result<Reservation> {
        self.budget.check()?;
        if self.retained.saturating_add(live).saturating_add(bytes)
            > self.tenant.engine().options.max_query_bytes
        {
            return Err(Error::limit("identity resolution memory limit"));
        }
        self.tenant.engine().budget.reserve(bytes)
    }
    fn retain(&mut self, bytes: usize, live: usize) -> Result<()> {
        self.charges.push(self.reserve(bytes, live)?);
        self.retained += bytes;
        Ok(())
    }
    fn get<T: DeserializeOwned>(
        &self,
        snapshot: &Snapshot,
        collection: Uuid,
        key: &str,
        stats: &mut ReadStats,
    ) -> Result<Option<T>> {
        stats.document_reads += 1;
        snapshot
            .get_controlled(&document_key(collection, key), stats, self.budget)?
            .map(|bytes| {
                let _scratch = self.reserve(
                    bytes.len().saturating_mul(16).saturating_add(4096),
                    bytes.len(),
                )?;
                codec::field_as(&bytes, "/data")?
                    .ok_or_else(|| Error::corrupt("identity record has no data"))
            })
            .transpose()
    }
    fn resolve(&mut self, group: &Group<'_>, bytes: &[u8], raw_bytes: usize) -> Result<()> {
        let scratch_bytes = bytes.len().saturating_mul(16).saturating_add(4096);
        let _scratch = self.reserve(scratch_bytes, raw_bytes)?;
        let timeline: Timeline = codec::field_as(bytes, "/data")?
            .ok_or_else(|| Error::corrupt("identity record has no data"))?;
        if &timeline.identifier != group.identifier {
            return Err(Error::corrupt("identifier digest collision"));
        }
        for &(index, at) in &group.events {
            self.budget.check()?;
            let pos = timeline.segments.partition_point(|b| b.valid_from_us <= at);
            let binding = pos
                .checked_sub(1)
                .and_then(|i| timeline.segments.get(i))
                .filter(|b| b.valid_to_us.is_none_or(|to| at < to));
            let size = crate::json::encoded_len(&binding)?.saturating_add(128);
            self.response_bytes = self.response_bytes.saturating_add(size);
            if self.response_bytes > self.tenant.engine().options.max_response_bytes {
                return Err(Error::limit("identity resolve_many response bytes"));
            }
            self.retain(
                size.saturating_mul(4),
                raw_bytes.saturating_add(scratch_bytes),
            )?;
            self.results[index] = ResolveMatch {
                timeline_revision: Some(timeline.revision),
                binding: binding.cloned(),
            };
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Options, Store};
    use serde_json::json;

    #[test]
    fn historical_batch_charges_tombstones_across_ranges_and_releases_failed_reads() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Options::default()).unwrap();
        let tenant = store
            .tenant(store.create_tenant("batch-history").unwrap().id)
            .unwrap();
        let context = IdentityContext {
            source: "test".into(),
            actor: "test".into(),
            can_correct: true,
        };
        tenant
            .identity_initialize(&context, IdentityConfig::default())
            .unwrap();
        let write = |value| {
            tenant
                .identity_write(&context, serde_json::from_value(value).unwrap())
                .unwrap()
        };
        let entity =
            write(json!({"action":"create_entity","operation_id":"entity","attributes":{}}))
                .entity
                .unwrap()
                .entity_id;
        let ids: Vec<Identifier> = ["one", "two"]
            .into_iter()
            .map(|value| Identifier {
                kind: "account".into(),
                namespace: "test".into(),
                value: value.into(),
            })
            .collect();
        for (i, id) in ids.iter().enumerate() {
            write(
                json!({"action":"bindings","operation_id":format!("bind-{i}"),"changes":[{"identifier":id,"expected_revision":"absent","effective":{"mode":"manual","from":"2026-01-01T00:00:00Z"},"change":{"action":"bind","entity_id":entity,"reason":"test"}}]}),
            );
        }
        let mut head = None;
        for i in 0..12 {
            head = Some(write(json!({"action":"create_entity","operation_id":format!("later-{i}"),"attributes":{}})).knowledge);
        }
        let head = head.unwrap();
        let collection = tenant
            .collection_id(&tenant.engine().snapshot().unwrap(), COLLECTION)
            .unwrap();
        tenant
            .engine()
            .transact(|tx| {
                for id in &ids {
                    // Retention can leave deleted newer revisions before a live predecessor.
                    for ordinal in 5..=head.ordinal {
                        tx.delete(document_key(
                            collection,
                            &format!("history:{}:{:016x}", digest(id)?, !ordinal),
                        ))?;
                    }
                }
                Ok(())
            })
            .unwrap();
        let request = ResolveManyRequest {
            known_at: Some(head.version.to_string()),
            items: ids
                .into_iter()
                .map(|identifier| ResolveItem {
                    identifier,
                    valid_at: "2026-01-02T00:00:00Z".into(),
                })
                .collect(),
        };
        let full = tenant.identity_resolve_many(&request).unwrap();
        assert!(full.stats.scanned_entries >= 20);
        assert!(
            full.results
                .iter()
                .all(|r| r.binding.as_ref().unwrap().entity_id == entity)
        );
        let memory = tenant.engine().memory_bytes();
        for _ in 0..5 {
            let reader = tenant
                .read_with(&ReadOptions {
                    max_scan: Some(16),
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(
                reader.identity_resolve_many(&request).unwrap_err().code,
                "RESOURCE_EXHAUSTED"
            );
            assert_eq!(tenant.engine().active_snapshots(), 0);
            assert_eq!(tenant.engine().memory_bytes(), memory);
        }
        assert_eq!(
            tenant.identity_resolve_many(&request).unwrap().results,
            full.results
        );
    }
}
