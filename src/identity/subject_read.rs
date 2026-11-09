use super::{
    storage::digest,
    subject_maintenance::prefix,
    subjects::{STATE, SubjectLink, SubjectState},
    *,
};
use crate::{
    ReadOptions, ReadStats, TenantReader, codec,
    query::Cursor,
    read::ReadBudget,
    resources::Reservation,
    storage::{ScanRange, Snapshot},
    tenant::{catalog::document_key, retention},
};
use serde::{Serialize, de::DeserializeOwned};

struct Page<'a> {
    tenant: &'a TenantHandle,
    budget: &'a ReadBudget,
    snapshot: Snapshot,
    collection: Uuid,
    knowledge: Knowledge,
    history: String,
    fingerprint: Vec<u8>,
    start: Vec<u8>,
    stats: ReadStats,
    retained: usize,
    memory: Vec<Reservation>,
}
impl<'a> Page<'a> {
    fn reserve(&self, size: usize) -> Result<Reservation> {
        self.budget.check()?;
        if self.retained.saturating_add(size) > self.tenant.engine().options.max_query_bytes {
            return Err(Error::limit("identity subject read memory"));
        }
        self.tenant.engine().budget.reserve(size)
    }
    fn decode<T: DeserializeOwned>(&mut self, bytes: &[u8]) -> Result<T> {
        let size = bytes.len().saturating_mul(16).saturating_add(4096);
        self.memory.push(self.reserve(size)?);
        self.retained += size;
        // Events contain arbitrary user attributes; preserve JSON object semantics.
        let value =
            codec::field(bytes, "/data")?.ok_or_else(|| Error::corrupt("identity record data"))?;
        serde_json::from_value(value).map_err(|e| Error::corrupt(e.to_string()))
    }
    fn get<T: DeserializeOwned>(&mut self, key: &str) -> Result<Option<T>> {
        self.stats.document_reads += 1;
        self.snapshot
            .get_controlled(
                &document_key(self.collection, key),
                &mut self.stats,
                self.budget,
            )?
            .map(|b| self.decode(&b))
            .transpose()
    }
    fn retain<T: Serialize>(&mut self, value: &T) -> Result<()> {
        let size = crate::json::encoded_len(value)?
            .saturating_mul(8)
            .saturating_add(256);
        self.memory.push(self.reserve(size)?);
        self.retained += size;
        Ok(())
    }
    fn open(
        reader: &'a TenantReader<'_>,
        entity: Uuid,
        known: Option<&str>,
        limit: usize,
        cursor: Option<&str>,
        fingerprint: Vec<u8>,
    ) -> Result<Self> {
        reader.budget.check()?;
        let tenant = reader.tenant;
        if limit == 0 || limit > tenant.engine().options.max_page_size {
            return Err(Error::limit("identity subject page size"));
        }
        let known = known
            .filter(|s| *s != "latest")
            .map(|s| {
                Uuid::parse_str(s)
                    .map_err(|_| Error::invalid("known_at must be latest or a knowledge UUID"))
            })
            .transpose()?;
        let (snapshot, collection, pinned, start) = if let Some(token) = cursor {
            let id = Uuid::parse_str(token)
                .map_err(|_| Error::new("CURSOR_EXPIRED", "invalid cursor"))?;
            let cursors = tenant.core.cursors.lock();
            let c = cursors.get(&id).ok_or_else(|| {
                Error::new(
                    "CURSOR_EXPIRED",
                    "cursor expired or belongs to another tenant",
                )
            })?;
            if !c.valid() {
                return Err(Error::new("CURSOR_EXPIRED", "cursor snapshot expired"));
            }
            if c.principal != reader.budget.principal
                || c.fingerprint != fingerprint
                || c.snapshot.tenant() != tenant.id()
                || c.snapshot.generation() != tenant.engine().runtime_id
            {
                return Err(Error::invalid(
                    "cursor does not match reader, filters or storage generation",
                ));
            }
            (
                c.snapshot.clone(),
                c.collection,
                Some(
                    c.identity
                        .clone()
                        .ok_or_else(|| Error::invalid("not an identity cursor"))?,
                ),
                c.after.clone(),
            )
        } else {
            let snapshot = tenant.engine().snapshot()?;
            let collection = tenant.collection_id(&snapshot, COLLECTION)?;
            (snapshot, collection, None, Vec::new())
        };
        if reader
            .budget
            .minimum_seq
            .is_some_and(|s| snapshot.seq() < s)
        {
            return Err(Error::new(
                if cursor.is_some() {
                    "SNAPSHOT_TOO_OLD"
                } else {
                    "REPLICA_LAG"
                },
                "identity snapshot is behind requested position",
            ));
        }
        let mut page = Self {
            tenant,
            budget: &reader.budget,
            snapshot,
            collection,
            knowledge: Knowledge {
                version: Uuid::nil(),
                ordinal: 0,
                previous: None,
                recorded_at_us: 0,
                commit_seq: 0,
                source: String::new(),
                actor: String::new(),
            },
            history: tenant.history(),
            fingerprint,
            start,
            stats: ReadStats::default(),
            retained: 0,
            memory: Vec::new(),
        };
        if let Some((knowledge, history)) = pinned {
            page.knowledge = knowledge;
            page.history = history;
            let head: Knowledge = page
                .get("head:knowledge")?
                .ok_or_else(|| Error::corrupt("identity knowledge head missing"))?;
            if page.knowledge.version != head.version {
                retention::check_snapshot_controlled(
                    &page.snapshot,
                    &document_key(collection, &format!("knowledge:{}", page.knowledge.version)),
                    true,
                    "KNOWLEDGE_UNAVAILABLE",
                    Some(page.budget),
                    &mut page.stats,
                )?;
            }
        } else {
            let head: Knowledge = page
                .get("head:knowledge")?
                .ok_or_else(|| Error::corrupt("identity knowledge head missing"))?;
            page.knowledge = if let Some(version) = known.filter(|v| *v != head.version) {
                let key = format!("knowledge:{version}");
                let knowledge = page.get(&key)?.ok_or_else(|| {
                    Error::new(
                        "KNOWLEDGE_UNAVAILABLE",
                        "knowledge version absent from this history",
                    )
                })?;
                retention::check_snapshot_controlled(
                    &page.snapshot,
                    &document_key(collection, &key),
                    true,
                    "KNOWLEDGE_UNAVAILABLE",
                    Some(page.budget),
                    &mut page.stats,
                )?;
                knowledge
            } else {
                head
            };
        }
        if cursor.is_none() {
            page.stats.document_reads += 1;
            if page
                .snapshot
                .get_controlled(
                    &document_key(collection, &format!("entity:{entity}")),
                    &mut page.stats,
                    page.budget,
                )?
                .is_none()
            {
                return Err(Error::missing("identity entity"));
            }
        }
        Ok(page)
    }
    fn finish<T: Serialize>(
        &mut self,
        response: &T,
        old: Option<&str>,
        next: Option<Vec<u8>>,
    ) -> Result<Option<String>> {
        self.budget.check_scan(&self.stats)?;
        self.snapshot.check()?;
        if self.history != self.tenant.history()
            || self.snapshot.replication_epoch() != self.tenant.engine().replication_epoch()
        {
            return Err(Error::new(
                "CURSOR_EXPIRED",
                "identity history changed during read",
            ));
        }
        if crate::json::encoded_len(response)?.saturating_add(40)
            > self.tenant.engine().options.max_response_bytes
        {
            return Err(Error::limit("identity subject response bytes"));
        }
        let cursor = next
            .map(|after| -> Result<Cursor> {
                Ok(Cursor {
                    principal: self.budget.principal.clone(),
                    snapshot: self.snapshot.clone(),
                    publication: None,
                    replication_epoch: self.snapshot.replication_epoch(),
                    fingerprint: self.fingerprint.clone(),
                    _memory: self.tenant.engine().budget.reserve(
                        after.len() + crate::json::encoded_len(&self.knowledge)? * 4 + 512,
                    )?,
                    after,
                    position: Vec::new(),
                    collection: self.collection,
                    identity: Some((self.knowledge.clone(), self.history.clone())),
                })
            })
            .transpose()?;
        self.tenant.publish_cursor(old, cursor, self.budget)
    }
    fn state(&mut self) -> Result<SubjectState> {
        self.get(STATE)?.ok_or_else(|| {
            Error::new(
                "INDEX_NOT_READY",
                "identity subject indexes are awaiting background migration",
            )
        })
    }
}
fn fingerprint<T: Serialize>(kind: &str, request: &T) -> Result<Vec<u8>> {
    if crate::json::encoded_len(request)? > 8192 {
        return Err(Error::limit("identity subject filter bytes"));
    }
    Ok(digest(&(kind, request))?.into_bytes())
}
fn advance(key: &[u8]) -> Vec<u8> {
    let mut key = key.to_vec();
    key.push(0);
    key
}

impl TenantHandle {
    pub fn get_subject_identifiers(
        &self,
        request: &SubjectIdentifiersRequest,
    ) -> Result<SubjectIdentifiersResult> {
        self.read_with(&ReadOptions::default())?
            .get_subject_identifiers(request)
    }
    pub fn get_subject_history(
        &self,
        request: &SubjectHistoryRequest,
    ) -> Result<SubjectHistoryResult> {
        self.read_with(&ReadOptions::default())?
            .get_subject_history(request)
    }
}
impl TenantReader<'_> {
    pub fn get_subject_identifiers(
        &self,
        request: &SubjectIdentifiersRequest,
    ) -> Result<SubjectIdentifiersResult> {
        self.budget.check()?;
        let at = parse_time(&request.valid_at)?;
        for (filter, maximum) in [(&request.kind, 64), (&request.namespace, 256)] {
            if filter.as_ref().is_some_and(|s| {
                s.is_empty() || s.len() > maximum || s.chars().any(char::is_control)
            }) {
                return Err(Error::invalid("invalid identifier filter"));
            }
        }
        let _permit = self.tenant.core.gate.enter()?;
        self.tenant.touch();
        let key = fingerprint(
            "ids",
            &(
                request.entity_id,
                &request.valid_at,
                &request.known_at,
                &request.kind,
                &request.namespace,
                request.limit,
            ),
        )?;
        let mut page = Page::open(
            self,
            request.entity_id,
            request.known_at.as_deref(),
            request.limit,
            request.cursor.as_deref(),
            key,
        )?;
        if !page.state()?.ready {
            return Err(Error::new(
                "INDEX_NOT_READY",
                "identity subject index rebuild in progress",
            ));
        }
        let prefix = prefix(page.collection, &format!("subject:{}:", request.entity_id));
        let mut start = if page.start.is_empty() {
            prefix.clone()
        } else {
            page.start.clone()
        };
        let end = codec::prefix_end(&prefix);
        let snapshot = page.snapshot.clone();
        let mut scan = snapshot.scan_controlled(
            start.clone(),
            end.clone(),
            None,
            &mut page.stats,
            &self.budget,
        )?;
        let stop = page
            .stats
            .scanned_entries
            .saturating_add((request.limit.saturating_mul(4)).clamp(16, 256) as u64);
        let mut identifiers = Vec::new();
        let mut exhausted = false;
        while identifiers.len() < request.limit && page.stats.scanned_entries < stop {
            let Some(row) = scan.next_for_gc(&mut page.stats, stop as usize)? else {
                exhausted = page.stats.scanned_entries < stop;
                break;
            };
            start = advance(&row.key);
            let text = std::str::from_utf8(&row.key[prefix.len()..row.key.len() - 2])
                .map_err(|_| Error::corrupt("subject link key"))?;
            let (id_digest, ordinal) = text
                .rsplit_once(':')
                .ok_or_else(|| Error::corrupt("subject link key"))?;
            let ordinal = !u64::from_str_radix(ordinal, 16)
                .map_err(|_| Error::corrupt("subject link ordinal"))?;
            if ordinal > page.knowledge.ordinal || row.value.is_none() {
                continue;
            }
            page.stats.document_reads += 1;
            let link: SubjectLink = page.decode(row.value.as_ref().unwrap())?;
            if digest(&link.identifier)? != id_digest {
                return Err(Error::corrupt("identifier digest collision"));
            }
            let group = super::subject_maintenance::prefix(
                page.collection,
                &format!("subject:{}:{id_digest}:", request.entity_id),
            );
            start = codec::prefix_end(&group).ok_or_else(|| Error::corrupt("subject key range"))?;
            scan.seek(
                &ScanRange {
                    start: start.clone(),
                    end: end.clone(),
                    prefix: None,
                },
                &mut page.stats,
            )?;
            if !link.present
                || request
                    .kind
                    .as_ref()
                    .is_some_and(|s| *s != link.identifier.kind)
                || request
                    .namespace
                    .as_ref()
                    .is_some_and(|s| *s != link.identifier.namespace)
            {
                continue;
            }
            let group = super::subject_maintenance::prefix(
                page.collection,
                &format!("history:{id_digest}:"),
            );
            let mut timeline_scan = snapshot.scan_controlled(
                document_key(
                    page.collection,
                    &format!("history:{id_digest}:{:016x}", !page.knowledge.ordinal),
                ),
                codec::prefix_end(&group),
                None,
                &mut page.stats,
                &self.budget,
            )?;
            let Some(row) = timeline_scan.next(&mut page.stats)? else {
                return Err(Error::corrupt("subject link has no timeline"));
            };
            page.stats.document_reads += 1;
            let timeline: Timeline = page.decode(row.value.as_ref().unwrap())?;
            if timeline.identifier != link.identifier {
                return Err(Error::corrupt("identifier digest collision"));
            }
            if let Some(binding) = timeline.segments.iter().find(|b| {
                b.entity_id == request.entity_id
                    && b.valid_from_us <= at
                    && b.valid_to_us.is_none_or(|to| at < to)
            }) {
                let item = SubjectIdentifier {
                    identifier: link.identifier,
                    timeline_revision: timeline.revision,
                    binding: binding.clone(),
                };
                page.retain(&item)?;
                identifiers.push(item);
            }
        }
        let mut result = SubjectIdentifiersResult {
            entity_id: request.entity_id,
            knowledge_version: page.knowledge.version,
            history: page.history.clone(),
            snapshot_seq: page.snapshot.seq(),
            identifiers,
            cursor: None,
            stats: page.stats.clone(),
        };
        result.cursor = page.finish(
            &result,
            request.cursor.as_deref(),
            (!exhausted).then_some(start),
        )?;
        Ok(result)
    }
    pub fn get_subject_history(
        &self,
        request: &SubjectHistoryRequest,
    ) -> Result<SubjectHistoryResult> {
        self.budget.check()?;
        let from = request
            .recorded_from
            .as_deref()
            .map(parse_time)
            .transpose()?;
        let to = request.recorded_to.as_deref().map(parse_time).transpose()?;
        if from.zip(to).is_some_and(|(f, t)| f >= t) {
            return Err(Error::invalid("recorded_from must precede recorded_to"));
        }
        let _permit = self.tenant.core.gate.enter()?;
        self.tenant.touch();
        let key = fingerprint(
            "history",
            &(
                request.entity_id,
                &request.known_at,
                &request.recorded_from,
                &request.recorded_to,
                request.limit,
            ),
        )?;
        let mut page = Page::open(
            self,
            request.entity_id,
            request.known_at.as_deref(),
            request.limit,
            request.cursor.as_deref(),
            key,
        )?;
        let state = page.state()?;
        let policy = page
            .snapshot
            .get_controlled(retention::POLICY, &mut page.stats, &self.budget)?
            .is_some();
        let coverage = HistoryCoverage {
            recording_started: state.recording_started,
            legacy_history_missing: state.legacy,
            retention_enabled: policy,
        };
        let prefix = prefix(
            page.collection,
            &format!("subject-event:{}:", request.entity_id),
        );
        let mut start = if page.start.is_empty() {
            super::subject_maintenance::prefix(
                page.collection,
                &format!(
                    "subject-event:{}:{:016x}:",
                    request.entity_id, !page.knowledge.ordinal
                ),
            )
        } else {
            page.start.clone()
        };
        let snapshot = page.snapshot.clone();
        let mut scan = snapshot.scan_controlled(
            start.clone(),
            codec::prefix_end(&prefix),
            None,
            &mut page.stats,
            &self.budget,
        )?;
        let stop = page
            .stats
            .scanned_entries
            .saturating_add((request.limit.saturating_mul(4)).clamp(16, 256) as u64);
        let mut events = Vec::new();
        let mut exhausted = false;
        while events.len() < request.limit && page.stats.scanned_entries < stop {
            let Some(row) = scan.next_for_gc(&mut page.stats, stop as usize)? else {
                exhausted = page.stats.scanned_entries < stop;
                break;
            };
            start = advance(&row.key);
            let Some(bytes) = row.value else {
                continue;
            };
            page.stats.document_reads += 1;
            let key: String = page.decode(&bytes)?;
            let valid = retention::check_snapshot_controlled(
                &snapshot,
                &document_key(page.collection, &key),
                true,
                "HISTORY_EXPIRED",
                Some(&self.budget),
                &mut page.stats,
            );
            match valid {
                Err(e) if e.code == "HISTORY_EXPIRED" => continue,
                result => result?,
            }
            let Some(event) = page.get::<IdentityEvent>(&key)? else {
                if policy {
                    continue;
                }
                return Err(Error::corrupt("subject event payload missing"));
            };
            if !event.entity_ids.contains(&request.entity_id)
                || event.knowledge.ordinal > page.knowledge.ordinal
            {
                return Err(Error::corrupt("subject event index mismatch"));
            }
            if from.is_some_and(|f| event.knowledge.recorded_at_us < f)
                || to.is_some_and(|t| event.knowledge.recorded_at_us >= t)
            {
                continue;
            }
            page.retain(&event)?;
            events.push(event);
        }
        let mut result = SubjectHistoryResult {
            entity_id: request.entity_id,
            knowledge_version: page.knowledge.version,
            history: page.history.clone(),
            snapshot_seq: page.snapshot.seq(),
            events,
            coverage,
            cursor: None,
            stats: page.stats.clone(),
        };
        result.cursor = page.finish(
            &result,
            request.cursor.as_deref(),
            (!exhausted).then_some(start),
        )?;
        Ok(result)
    }
}
