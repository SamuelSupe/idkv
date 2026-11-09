use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// An exclusive identifier. Values are exact, case-sensitive UTF-8 in phase one.
/// Connectors must choose a stable namespace and normalize before submission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identifier {
    pub kind: String,
    pub namespace: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IdentityConfig {
    pub max_segments: usize,
    pub clock_tolerance_ms: u64,
}
impl Default for IdentityConfig {
    fn default() -> Self {
        Self {
            max_segments: 1000,
            clock_tolerance_ms: 1000,
        }
    }
}

/// Trusted embedding context, never deserialized from a caller's request.
/// HTTP supplies this from its authenticated admin credential.
#[derive(Clone, Debug, Serialize)]
pub struct IdentityContext {
    pub source: String,
    pub actor: String,
    pub can_correct: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum Effective {
    // A struct variant enforces deny_unknown_fields; a tagged unit variant
    // silently consumes extra fields even when the enum denies them.
    Auto {},
    Manual {
        from: String,
        #[serde(default)]
        to: Option<String>,
    },
}

impl Default for Effective {
    fn default() -> Self {
        Self::Auto {}
    }
}

/// A revision UUID, or the literal `absent`. This field is always required.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum ExpectedRevision {
    Revision(Uuid),
    Absent,
}
impl TryFrom<String> for ExpectedRevision {
    type Error = crate::Error;
    fn try_from(value: String) -> crate::Result<Self> {
        if value == "absent" {
            Ok(Self::Absent)
        } else {
            Uuid::parse_str(&value)
                .map(Self::Revision)
                .map_err(|_| crate::Error::invalid("expected_revision must be absent or a UUID"))
        }
    }
}
impl From<ExpectedRevision> for String {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(id) => id.to_string(),
            ExpectedRevision::Absent => "absent".into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum BindingAction {
    Bind {
        entity_id: Uuid,
        #[serde(default)]
        evidence_id: Option<String>,
        #[serde(default)]
        reason: Option<String>,
    },
    Rebind {
        entity_id: Uuid,
        reason: String,
        #[serde(default)]
        evidence_id: Option<String>,
    },
    Unbind {
        reason: String,
    },
    ReplaceIdentifier {
        entity_id: Uuid,
        replacement: Identifier,
        replacement_expected_revision: ExpectedRevision,
        reason: String,
        #[serde(default)]
        evidence_id: Option<String>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingChange {
    pub identifier: Identifier,
    pub expected_revision: ExpectedRevision,
    #[serde(default)]
    pub effective: Effective,
    pub change: BindingAction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum IdentityWrite {
    CreateEntity {
        operation_id: String,
        attributes: Value,
    },
    UpdateEntity {
        operation_id: String,
        entity_id: Uuid,
        expected_revision: Uuid,
        attributes: Value,
    },
    Bindings {
        operation_id: String,
        changes: Vec<BindingChange>,
    },
}
impl IdentityWrite {
    pub(crate) fn operation_id(&self) -> &str {
        match self {
            Self::CreateEntity { operation_id, .. }
            | Self::UpdateEntity { operation_id, .. }
            | Self::Bindings { operation_id, .. } => operation_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entity {
    pub entity_id: Uuid,
    pub revision: Uuid,
    pub bindings_revision: Uuid,
    pub attributes: Value,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    pub valid_from_us: i64,
    pub valid_to_us: Option<i64>,
    pub entity_id: Uuid,
    pub binding_id: Uuid,
    pub version_id: Uuid,
    pub source: String,
    pub evidence_id: Option<String>,
    pub assurance: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timeline {
    pub identifier: Identifier,
    pub revision: Uuid,
    pub segments: Vec<Binding>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimelineChange {
    pub identifier: Identifier,
    pub revision: Option<Uuid>,
    pub changed: bool,
    pub effective_from_us: i64,
    pub effective_to_us: Option<i64>,
    /// Prior segments intersecting the changed range, including their old boundaries.
    pub replaced: Vec<Binding>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Knowledge {
    pub version: Uuid,
    pub ordinal: u64,
    pub previous: Option<Uuid>,
    pub recorded_at_us: i64,
    pub commit_seq: u64,
    pub source: String,
    pub actor: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IdentityReceipt {
    pub operation_id: String,
    pub knowledge: Knowledge,
    /// The one coordinator time used for all automatic changes in the transaction.
    pub effective_time_us: i64,
    pub entity: Option<Entity>,
    pub timelines: Vec<TimelineChange>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveRequest {
    pub identifier: Identifier,
    pub valid_at: String,
    /// Missing or `latest` pins the current knowledge head for this call.
    #[serde(default)]
    pub known_at: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolveResult {
    pub knowledge_version: Uuid,
    pub timeline_revision: Option<Uuid>,
    pub binding: Option<Binding>,
    pub stats: crate::ReadStats,
}

/// One event-time lookup in an ordered, single-snapshot resolution batch.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveItem {
    pub identifier: Identifier,
    pub valid_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveManyRequest {
    /// Missing, null or `latest` pins one knowledge head for the entire batch.
    #[serde(default)]
    pub known_at: Option<String>,
    pub items: Vec<ResolveItem>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveMatch {
    pub timeline_revision: Option<Uuid>,
    pub binding: Option<Binding>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolveManyResult {
    pub knowledge_version: Uuid,
    pub history: String,
    pub snapshot_seq: u64,
    /// One result per input, in input order, including duplicates and misses.
    pub results: Vec<ResolveMatch>,
    pub stats: crate::ReadStats,
}
