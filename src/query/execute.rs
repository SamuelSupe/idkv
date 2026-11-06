use super::{
    Cursor, filter,
    planner::{self, Access},
};
use crate::resources::Bytes;
use crate::{
    Error, Result, codec,
    model::*,
    resources::Reservation,
    storage::Snapshot,
    tenant::{TenantHandle, catalog::*},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BinaryHeap, HashSet},
    sync::Arc,
};
use uuid::Uuid;

struct Candidate {
    order: Vec<u8>,
    position: Vec<u8>,
    document: CandidateDocument,
    _memory: Reservation,
}
enum CandidateDocument {
    Encoded(Arc<Bytes>),
    Projected(Value),
}
impl CandidateDocument {
    fn materialize(self) -> Result<Value> {
        match self {
            Self::Encoded(bytes) => codec::decode(&bytes),
            Self::Projected(value) => Ok(value),
        }
    }
}
impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.order == other.order
    }
}
impl Eq for Candidate {}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order.cmp(&other.order)
    }
}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl TenantHandle {
    pub fn query(&self, collection_name: &str, query: Query) -> Result<QueryResult> {
        self.read_with(&crate::ReadOptions::default())?
            .query(collection_name, query)
    }

    pub(crate) fn query_view(
        &self,
        collection_name: &str,
        query: Query,
        view: Option<Snapshot>,
        publication: Option<(String, Uuid)>,
        budget: &crate::read::ReadBudget,
        scanned_before: u64,
    ) -> Result<QueryResult> {
        budget.check()?;
        self.touch();
        let replication_epoch = self.engine().replication_epoch();
        filter::validate_query(&query, self.engine().options.max_page_size)?;
        let mut stats = ReadStats {
            scanned_entries: scanned_before,
            ..Default::default()
        };
        let fingerprint = Sha256::digest(serde_json::to_vec(&(
            collection_name,
            &publication,
            &query.filter,
            &query.sort,
            &query.select,
            query.limit,
        ))?)
        .to_vec();
        let compiled_size = crate::json::encoded_len(&query.filter)?
            .saturating_mul(16)
            .saturating_add(128 << 10);
        let _filter_memory = self
            .engine()
            .budget
            .reserve(compiled_size.saturating_add(self.engine().options.max_document_bytes * 2))?;
        let (snapshot, after, resume, expected_collection) = if let Some(token) = &query.cursor {
            let id = Uuid::parse_str(token)
                .map_err(|_| Error::new("CURSOR_EXPIRED", "invalid cursor"))?;
            let cursors = self.core.cursors.lock();
            let cursor = cursors.get(&id).ok_or_else(|| {
                Error::new(
                    "CURSOR_EXPIRED",
                    "cursor expired or belongs to another tenant",
                )
            })?;
            if cursor.replication_epoch != replication_epoch {
                return Err(Error::new("CURSOR_EXPIRED", "replication history changed"));
            }
            if cursor.fingerprint != fingerprint
                || cursor.principal != budget.principal
                || cursor.snapshot.tenant() != self.id()
                || cursor.snapshot.generation() != self.engine().runtime_id
            {
                return Err(Error::invalid(
                    "cursor does not match tenant, query or storage generation",
                ));
            }
            cursor.snapshot.check()?;
            (
                cursor.snapshot.clone(),
                Some(cursor.after.clone()),
                Some(cursor.position.clone()),
                Some(cursor.collection),
            )
        } else {
            (
                match view {
                    Some(snapshot) => snapshot,
                    None => self.engine().snapshot()?,
                },
                None,
                None,
                None,
            )
        };
        if budget
            .minimum_seq
            .is_some_and(|minimum| snapshot.seq() < minimum)
        {
            return Err(Error::new(
                "SNAPSHOT_TOO_OLD",
                "cursor snapshot predates the requested minimum position",
            ));
        }
        let catalog = super::catalog::read(self, &snapshot, collection_name)?;
        let collection = &catalog.collection;
        if expected_collection.is_some_and(|id| id != collection.id) {
            return Err(Error::new("CURSOR_EXPIRED", "collection changed"));
        }
        let indexes = &catalog.indexes;
        let branches = planner::branches(query.filter.as_ref());
        let accesses = branches
            .iter()
            .map(|f| planner::plan(collection.id, *f, &query, indexes, snapshot.seq()))
            .collect::<Result<Vec<_>>>()?;
        let sort = filter::normalized_sort(&query.sort);
        let sort_key = filter::SortKey::new(&sort)?;
        let explain = planner::explain(&accesses, &sort);
        budget.check()?;
        if explain.scan && !budget.allow_full_scan {
            return Err(Error::new(
                "SCAN_NOT_ALLOWED",
                "query requires a collection scan",
            ));
        }
        let mut heap = BinaryHeap::new();
        let mut seen = HashSet::new();
        let mut seen_memory = Vec::new();
        let mut retained = 0;
        for (branch, access) in branches.into_iter().zip(&accesses) {
            let predicate = filter::Compiled::new(branch)?;
            let mut scratch = Vec::new();
            let ordered = accesses.len() == 1
                && match access {
                    Access::Index { ordered, .. } => *ordered,
                    Access::Scan | Access::PrimaryRange(_) => {
                        sort == vec![IndexField {
                            field: "/_key".into(),
                            order: Order::Asc,
                        }]
                    }
                    Access::Primary(keys) => keys.len() <= 1,
                };
            let mut accept =
                |data: Arc<Bytes>, position: Vec<u8>, stats: &mut ReadStats| -> Result<bool> {
                    budget.check_scan(stats)?;
                    let key = if accesses.len() > 1 {
                        Some(row_key(&data)?)
                    } else {
                        None
                    };
                    if key.as_ref().is_some_and(|key| seen.contains(key)) {
                        return Ok(false);
                    }
                    if !predicate.matches(&data, &mut scratch)? {
                        return Ok(false);
                    }
                    let order = sort_key.encode(&data, &mut scratch)?;
                    if after.as_ref().is_some_and(|last| order <= *last) {
                        return Ok(false);
                    }
                    if accesses.len() > 1 {
                        let key = key.unwrap();
                        if seen.len() >= self.engine().options.max_scan {
                            return Err(Error::limit("OR deduplication budget"));
                        }
                        seen_memory.push(self.engine().budget.reserve(key.len() + 96)?);
                        seen.insert(key);
                    }
                    if heap.len() < query.limit + 1
                        || heap.peek().is_some_and(|c: &Candidate| c.order > order)
                    {
                        // Full documents are decoded only after Top-K selection.
                        // Narrow projections stay compact rather than pinning a
                        // large source document just to return a few small fields.
                        let (document, size) = if query.select.is_empty() {
                            let size = data.len() * 4;
                            (CandidateDocument::Encoded(data), size)
                        } else {
                            let value = codec::project(&data, &query.select)?;
                            let size = crate::json::encoded_len(&value)? * 4;
                            (CandidateDocument::Projected(value), size)
                        };
                        let size = size + order.len() + position.len() + 256;
                        if heap.len() == query.limit + 1 {
                            let old: Candidate = heap.pop().unwrap();
                            retained -= old._memory.size();
                        }
                        if retained + size > self.engine().options.max_query_bytes {
                            return Err(Error::limit("Top-K memory budget"));
                        }
                        let memory = self.engine().budget.reserve(size)?;
                        retained += size;
                        heap.push(Candidate {
                            order,
                            position,
                            document,
                            _memory: memory,
                        });
                    }
                    Ok(ordered && heap.len() > query.limit)
                };
            match access {
                Access::Primary(keys) => {
                    let encoded: Vec<_> = keys
                        .iter()
                        .map(|key| document_key(collection.id, key))
                        .collect();
                    for (position, data) in encoded
                        .iter()
                        .zip(snapshot.multi_get_controlled(&encoded, &mut stats, budget)?)
                    {
                        if let Some(data) = data {
                            stats.scanned_entries += 1;
                            stats.document_reads += 1;
                            if accept(data, position.clone(), &mut stats)? {
                                break;
                            }
                        }
                    }
                }
                Access::Scan | Access::PrimaryRange(_) => {
                    let p = prefix(b'D', collection.id);
                    let (lower, end) = match access {
                        Access::PrimaryRange(range) => (range.start.clone(), range.end.clone()),
                        _ => (p.clone(), codec::prefix_end(&p)),
                    };
                    let start = if ordered {
                        resume
                            .clone()
                            .map_or_else(|| lower.clone(), |r| lower.clone().max(r))
                    } else {
                        lower
                    };
                    let mut scan =
                        snapshot.scan_controlled(start, end, None, &mut stats, budget)?;
                    while let Some(record) = scan.next(&mut stats)? {
                        stats.document_reads += 1;
                        if accept(record.value.unwrap(), record.key, &mut stats)? {
                            break;
                        }
                    }
                }
                Access::Index {
                    index,
                    ranges,
                    exact,
                    covering,
                    ..
                } => {
                    if *exact && index.spec.unique && ranges.len() == 1 {
                        let start = &ranges[0].start;
                        if let Some(value) = snapshot.get_controlled(start, &mut stats, budget)? {
                            stats.scanned_entries += 1;
                            let data = if *covering {
                                Some(value)
                            } else {
                                fetch_document(
                                    &snapshot,
                                    collection.id,
                                    &value,
                                    &mut stats,
                                    budget,
                                )?
                            };
                            if let Some(data) = data {
                                accept(data, start.clone(), &mut stats)?;
                            }
                        }
                    } else if *exact && index.spec.unique {
                        let keys: Vec<_> = ranges.iter().map(|range| range.start.clone()).collect();
                        let values = snapshot.multi_get_controlled(&keys, &mut stats, budget)?;
                        let mut rows = Vec::new();
                        for (start, value) in keys.iter().zip(values) {
                            if let Some(value) = value {
                                stats.scanned_entries += 1;
                                rows.push((start, value));
                            }
                        }
                        if *covering {
                            for (start, value) in rows {
                                accept(value, start.clone(), &mut stats)?;
                            }
                        } else if rows.len() == 1 {
                            let (start, value) = rows.pop().unwrap();
                            if let Some(data) = fetch_document(
                                &snapshot,
                                collection.id,
                                &value,
                                &mut stats,
                                budget,
                            )? {
                                accept(data, start.clone(), &mut stats)?;
                            }
                        } else {
                            let keys = rows
                                .iter()
                                .map(|(_, v)| {
                                    row_key(v).map(|key| document_key(collection.id, &key))
                                })
                                .collect::<Result<Vec<_>>>()?;
                            stats.document_reads += keys.len() as u64;
                            for ((start, _), value) in rows
                                .into_iter()
                                .zip(snapshot.multi_get_controlled(&keys, &mut stats, budget)?)
                            {
                                let data = value.ok_or_else(|| {
                                    Error::corrupt("index references missing document")
                                })?;
                                accept(data, start.clone(), &mut stats)?;
                            }
                        }
                    } else {
                        let mut ranges = ranges.clone();
                        if ordered
                            && let (Some(range), Some(resume)) = (ranges.first_mut(), &resume)
                        {
                            range.start = range.start.clone().max(resume.clone());
                        }
                        let mut scan = snapshot.scan_ranges(ranges, &mut stats, Some(budget))?;
                        let mut finished = false;
                        while !finished {
                            let mut values = Vec::new();
                            let mut positions = Vec::new();
                            let chunk = if ordered {
                                (query.limit + 1).min(64)
                            } else {
                                64
                            };
                            for _ in 0..chunk {
                                if let Some(record) = scan.next(&mut stats)? {
                                    values.push(record.value.unwrap());
                                    positions.push(record.key);
                                    budget.check_scan(&stats)?;
                                } else {
                                    finished = true;
                                    break;
                                }
                            }
                            if !*covering {
                                let keys = values
                                    .iter()
                                    .map(|v| {
                                        row_key(v).map(|key| document_key(collection.id, &key))
                                    })
                                    .collect::<Result<Vec<_>>>()?;
                                stats.document_reads += keys.len() as u64;
                                values = snapshot
                                    .multi_get_controlled(&keys, &mut stats, budget)?
                                    .into_iter()
                                    .map(|v| {
                                        v.ok_or_else(|| {
                                            Error::corrupt("index references missing document")
                                        })
                                    })
                                    .collect::<Result<_>>()?;
                            }
                            for (value, position) in values.into_iter().zip(positions) {
                                if accept(value, position, &mut stats)? {
                                    finished = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        budget.check_scan(&stats)?;
        let mut candidates = heap.into_sorted_vec();
        let more = candidates.len() > query.limit;
        if more {
            candidates.pop();
        }
        let next_cursor = if more {
            let last = candidates.last().unwrap();
            Some(Cursor {
                principal: budget.principal.clone(),
                publication,
                replication_epoch,
                snapshot: snapshot.clone(),
                fingerprint,
                after: last.order.clone(),
                position: last.position.clone(),
                collection: collection.id,
                _memory: self
                    .engine()
                    .budget
                    .reserve(last.order.len() + last.position.len() + 256)?,
            })
        } else {
            None
        };
        let mut documents = Vec::with_capacity(candidates.len());
        let mut output_size = 2usize;
        let mut output_memory = Vec::new();
        for candidate in candidates {
            let document = candidate.document.materialize()?;
            output_size +=
                crate::json::encoded_len(&document)? + usize::from(!documents.is_empty());
            if output_size > self.engine().options.max_response_bytes {
                return Err(Error::limit("query response byte limit"));
            }
            output_memory.push(candidate._memory);
            documents.push(document);
        }
        budget.check_scan(&stats)?;
        snapshot.check()?;
        if self.engine().replication_epoch() != replication_epoch {
            return Err(Error::new(
                "CURSOR_EXPIRED",
                "replication history changed during query",
            ));
        }
        let next = {
            let mut cursors = self.core.cursors.lock();
            cursors.retain(|_, c| c.valid());
            if let Some(old) = &query.cursor
                && let Ok(id) = Uuid::parse_str(old)
                && !cursors.contains_key(&id)
            {
                return Err(Error::new(
                    "CURSOR_EXPIRED",
                    "cursor expired or already consumed",
                ));
            }
            if next_cursor.is_some()
                && query.cursor.is_none()
                && (cursors.len() >= self.engine().options.max_snapshots / 2
                    || cursors
                        .values()
                        .filter(|c| c.principal == budget.principal)
                        .count()
                        >= (self.engine().options.max_snapshots / 4).max(1))
            {
                return Err(Error::limit("cursor count limit"));
            }
            if let Some(old) = &query.cursor {
                cursors.remove(&Uuid::parse_str(old).unwrap());
            }
            if let Some(cursor) = next_cursor {
                let token = Uuid::new_v4();
                cursors.insert(token, cursor);
                Some(token.to_string())
            } else {
                None
            }
        };
        stats.scanned_entries -= scanned_before;
        Ok(QueryResult {
            documents,
            cursor: next,
            snapshot_seq: snapshot.seq(),
            plan: explain,
            stats,
        })
    }
    pub fn explain(&self, collection_name: &str, query: &Query) -> Result<Explain> {
        let _permit = self.core.gate.enter()?;
        self.touch();
        filter::validate_query(query, self.engine().options.max_page_size)?;
        let snapshot = self.engine().snapshot()?;
        let catalog = super::catalog::read(self, &snapshot, collection_name)?;
        let collection = &catalog.collection;
        let indexes = &catalog.indexes;
        let plans = planner::branches(query.filter.as_ref())
            .iter()
            .map(|f| planner::plan(collection.id, *f, query, indexes, snapshot.seq()))
            .collect::<Result<Vec<_>>>()?;
        Ok(planner::explain(
            &plans,
            &filter::normalized_sort(&query.sort),
        ))
    }
}
pub(super) fn row_key(data: &[u8]) -> Result<String> {
    codec::field(data, "/_key")?
        .and_then(|v| v.as_str().map(str::to_owned))
        .ok_or_else(|| Error::corrupt("index document key"))
}
fn fetch_document(
    snapshot: &Snapshot,
    collection: Uuid,
    data: &[u8],
    stats: &mut ReadStats,
    budget: &crate::read::ReadBudget,
) -> Result<Option<Arc<Bytes>>> {
    stats.document_reads += 1;
    snapshot.get_controlled(&document_key(collection, &row_key(data)?), stats, budget)
}
