use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
    pub global_memory_bytes: usize,
    pub tenant_memory_bytes: usize,
    pub tenant_disk_bytes: u64,
    pub cache_bytes: usize,
    pub memtable_bytes: usize,
    pub block_bytes: usize,
    pub io_workers: usize,
    pub max_tenant_requests: usize,
    pub max_http_requests: usize,
    pub max_http_body_requests: usize,
    pub max_http_maintenance_requests: usize,
    pub max_http_connections: usize,
    pub http_write_timeout_ms: u64,
    pub http_header_timeout_ms: u64,
    pub maintenance_reserve_free_bytes: u64,
    pub max_local_backup_bytes: u64,
    pub idle_seconds: u64,
    pub snapshot_seconds: u64,
    pub index_build_seconds: u64,
    pub max_snapshots: usize,
    pub max_document_bytes: usize,
    pub max_batch_operations: usize,
    pub max_batch_bytes: usize,
    pub max_read_keys: usize,
    pub max_page_size: usize,
    pub max_scan: usize,
    pub query_ms: u64,
    pub max_response_bytes: usize,
    pub max_query_bytes: usize,
    pub l0_trigger: usize,
    pub l0_stop: usize,
    pub level_base_bytes: u64,
    pub target_sst_bytes: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            global_memory_bytes: 4 << 30,
            tenant_memory_bytes: 512 << 20,
            tenant_disk_bytes: 100 << 30,
            cache_bytes: 128 << 20,
            memtable_bytes: 16 << 20,
            block_bytes: 16 << 10,
            io_workers: 4,
            max_tenant_requests: 32,
            max_http_requests: 64,
            max_http_body_requests: 256,
            max_http_maintenance_requests: 4,
            max_http_connections: 1024,
            http_write_timeout_ms: 30_000,
            http_header_timeout_ms: 5_000,
            maintenance_reserve_free_bytes: 64 << 20,
            max_local_backup_bytes: 1 << 30,
            idle_seconds: 300,
            snapshot_seconds: 60,
            index_build_seconds: 3600,
            max_snapshots: 256,
            max_document_bytes: 1 << 20,
            max_batch_operations: 1000,
            max_batch_bytes: 16 << 20,
            max_read_keys: 1000,
            max_page_size: 1000,
            max_scan: 100_000,
            query_ms: 5_000,
            max_response_bytes: 16 << 20,
            max_query_bytes: 64 << 20,
            l0_trigger: 4,
            l0_stop: 12,
            level_base_bytes: 64 << 20,
            target_sst_bytes: 16 << 20,
        }
    }
}
impl Options {
    pub(crate) fn supports_quota(&self, memory: usize, disk: u64) -> bool {
        memory <= self.global_memory_bytes
            && memory
                >= self
                    .memtable_bytes
                    .saturating_mul(2)
                    .saturating_add(self.cache_bytes.min(memory / 3))
            && disk >= 1 << 20
    }
    pub fn validate(&self) -> crate::Result<()> {
        if self.global_memory_bytes > 1usize << 40
            || self.cache_bytes > 1usize << 40
            || self.memtable_bytes > 1usize << 38
            || self.snapshot_seconds > 86400
            || self.index_build_seconds > 604800
            || self.max_response_bytes > 64 << 20
            || self.max_query_bytes > 1usize << 40
            || self.query_ms > 86_400_000
            || self.max_tenant_requests > 100_000
            || !(1..=100_000).contains(&self.max_http_requests)
            || !(1..=100_000).contains(&self.max_http_body_requests)
            || !(1..=128).contains(&self.max_http_maintenance_requests)
            || !(1..=100_000).contains(&self.max_http_connections)
            || !(1..=3_600_000).contains(&self.http_write_timeout_ms)
            || !(1..=3_600_000).contains(&self.http_header_timeout_ms)
            || self.max_local_backup_bytes < 1 << 20
            || self.max_snapshots > 100_000
            || self.io_workers == 0
            || self.io_workers > 128
            || self.max_tenant_requests == 0
            || self.block_bytes < 1024
            || self.block_bytes > 1 << 20
            || self.memtable_bytes < 4096
            || self.target_sst_bytes < self.block_bytes
            || self.l0_trigger < 2
            || self.l0_stop <= self.l0_trigger
            || self.snapshot_seconds == 0
            || self.max_snapshots == 0
            || self.max_document_bytes == 0
            || self.max_document_bytes > crate::codec::MAX_DOC
            || self.max_batch_operations == 0
            || self.max_batch_operations > 100_000
            || self.max_batch_bytes == 0
            || self.max_batch_bytes > 64 << 20
            || self.max_read_keys == 0
            || self.max_read_keys > 100_000
            || self.max_page_size == 0
            || self.max_page_size > 100_000
            || self.index_build_seconds == 0
            || self.max_query_bytes == 0
            || self.max_scan == 0
            || self.query_ms == 0
            || self.max_response_bytes < 1 << 20
            || !self.supports_quota(self.tenant_memory_bytes, self.tenant_disk_bytes)
            || self.cache_bytes + self.memtable_bytes * 2 > self.tenant_memory_bytes
            || self.tenant_disk_bytes < 1 << 20
            || self.level_base_bytes == 0
        {
            return Err(crate::Error::invalid(
                "invalid resource or storage configuration",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantInfo {
    pub id: Uuid,
    pub name: String,
    pub generation: Uuid,
    /// Changes on local restore; replicated checkpoints preserve this identity.
    #[serde(default)]
    pub history: Uuid,
    pub memory_bytes: usize,
    pub disk_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Collection {
    pub id: Uuid,
    pub name: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Order {
    #[default]
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexField {
    pub field: String,
    #[serde(default)]
    pub order: Order,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexSpec {
    pub name: String,
    pub fields: Vec<IndexField>,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub include: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IndexState {
    Building,
    Ready,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub id: Uuid,
    pub generation: Uuid,
    pub spec: IndexSpec,
    pub state: IndexState,
    pub start_seq: u64,
    pub ready_seq: u64,
    pub error: Option<String>,
    #[serde(default)]
    pub entries: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteOp {
    /// Check the pre-batch revision without writing. Zero requires absence.
    /// Checks run before every mutation, irrespective of their position.
    Check { key: String, if_rev: u64 },
    Put {
        document: Value,
        #[serde(default)]
        if_rev: Option<u64>,
    },
    Delete {
        key: String,
        #[serde(default)]
        if_rev: Option<u64>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriteResult {
    pub key: String,
    pub rev: u64,
    pub deleted: bool,
    pub history: String,
}

/// Positive CAS revisions and durable retry IDs require the history observed
/// with the read. A principal is supplied by the trusted embedding application.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WriteOptions {
    pub expected_history: Option<String>,
    pub operation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_token: Option<Uuid>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadContext {
    pub tenant: Uuid,
    pub history: String,
    pub applied_seq: u64,
    pub writable: bool,
}

/// A bounded read. Batch queries start a new cursor in the batch's snapshot;
/// resume that cursor through query (or query_published for a published dataset).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadRequest {
    Get { key: String },
    MultiGet { keys: Vec<String> },
    Query { query: Query },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadOperation {
    pub collection: String,
    pub request: ReadRequest,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ReadResult {
    Get { document: Option<Value> },
    MultiGet { documents: Vec<Option<Value>> },
    Query { result: QueryResult },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadBatchResult {
    pub results: Vec<ReadResult>,
    pub history: String,
    pub snapshot_seq: u64,
    pub stats: ReadStats,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Filter {
    And {
        and: Vec<Filter>,
    },
    Or {
        or: Vec<Filter>,
    },
    Not {
        not: Box<Filter>,
    },
    Compare {
        field: String,
        op: CompareOp,
        value: Value,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompareOp {
    Eq,
    Ne,
    In,
    Gt,
    Gte,
    Lt,
    Lte,
    Exists,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    #[serde(default)]
    pub filter: Option<Filter>,
    #[serde(default)]
    pub sort: Vec<IndexField>,
    #[serde(default)]
    pub select: Vec<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
}
fn default_limit() -> usize {
    100
}
impl Default for Query {
    fn default() -> Self {
        Self {
            filter: None,
            sort: vec![],
            select: vec![],
            limit: 100,
            cursor: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReadStats {
    pub candidate_files: u64,
    pub scanned_entries: u64,
    pub blocks_read: u64,
    pub decompressed_bytes: u64,
    pub cache_hits: u64,
    pub document_reads: u64,
}
impl ReadStats {
    pub fn add(&mut self, other: &Self) {
        self.candidate_files += other.candidate_files;
        self.scanned_entries += other.scanned_entries;
        self.blocks_read += other.blocks_read;
        self.decompressed_bytes += other.decompressed_bytes;
        self.cache_hits += other.cache_hits;
        self.document_reads += other.document_reads;
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub documents: Vec<Value>,
    pub cursor: Option<String>,
    pub snapshot_seq: u64,
    pub plan: Explain,
    pub stats: ReadStats,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Explain {
    pub indexes: Vec<String>,
    pub covering: bool,
    pub extra_sort: bool,
    pub scan: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchMode {
    #[default]
    All,
    First,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LookupDefinition {
    pub name: String,
    pub index: String,
    pub select: Vec<String>,
    #[serde(default)]
    pub mode: MatchMode,
    #[serde(default = "default_limit")]
    pub max_matches: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LookupResult {
    pub matches: Vec<Vec<Value>>,
    pub snapshot_seq: u64,
    pub stats: ReadStats,
}
