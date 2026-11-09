use super::{Binding, Identifier, Knowledge};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

fn page_size() -> usize {
    100
}

/// Reverse resolution at one business time and one knowledge version. Continuations
/// must repeat all filters and are bound to the issuing node and authenticated reader.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectIdentifiersRequest {
    pub entity_id: Uuid,
    pub valid_at: String,
    #[serde(default)]
    pub known_at: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default = "page_size")]
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubjectIdentifier {
    pub identifier: Identifier,
    pub timeline_revision: Uuid,
    pub binding: Binding,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubjectIdentifiersResult {
    pub entity_id: Uuid,
    pub knowledge_version: Uuid,
    pub history: String,
    pub snapshot_seq: u64,
    pub identifiers: Vec<SubjectIdentifier>,
    /// Only null means exhausted; a bounded page may contain no matches.
    pub cursor: Option<String>,
    pub stats: crate::ReadStats,
}

/// Recorded-time bounds are inclusive from and exclusive to. Events are ordered
/// by descending knowledge ordinal, then ascending change index within a write.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectHistoryRequest {
    pub entity_id: Uuid,
    #[serde(default)]
    pub known_at: Option<String>,
    #[serde(default)]
    pub recorded_from: Option<String>,
    #[serde(default)]
    pub recorded_to: Option<String>,
    #[serde(default = "page_size")]
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityEventKind {
    EntityCreated,
    AttributesUpdated,
    IdentifierBound,
    IdentifierRebound,
    IdentifierUnbound,
    IdentifierReplaced,
}

/// `present` distinguishes an absent attribute from a stored JSON null.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AttributeValue {
    pub present: bool,
    pub value: Value,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AttributeChange {
    pub field: String,
    pub before: AttributeValue,
    pub after: AttributeValue,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IdentifierChange {
    pub identifier: Identifier,
    pub timeline_revision: Uuid,
    pub effective_from_us: i64,
    pub effective_to_us: Option<i64>,
    /// Segments clipped to the affected business-time interval.
    pub before: Vec<Binding>,
    pub after: Vec<Binding>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IdentityEvent {
    pub event_id: Uuid,
    pub operation_id: String,
    pub knowledge: Knowledge,
    pub change_index: usize,
    pub kind: IdentityEventKind,
    pub entity_ids: Vec<Uuid>,
    pub reason: Option<String>,
    pub changes: Vec<IdentifierChange>,
    pub attributes: Vec<AttributeChange>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryCoverage {
    pub recording_started: Knowledge,
    pub legacy_history_missing: bool,
    /// Retention was enabled in this snapshot; history is not a lifetime audit.
    pub retention_enabled: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubjectHistoryResult {
    pub entity_id: Uuid,
    pub knowledge_version: Uuid,
    pub history: String,
    pub snapshot_seq: u64,
    pub events: Vec<IdentityEvent>,
    pub coverage: HistoryCoverage,
    pub cursor: Option<String>,
    pub stats: crate::ReadStats,
}
