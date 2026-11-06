//! Per-request read controls. They only tighten the engine's limits.
use crate::{Error, Result, TenantHandle, model::*};
use serde_json::Value;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

/// Cooperative, one-way cancellation shared by the caller and an executing read.
/// It does not interrupt an OS read or affect writes and other requests.
#[derive(Clone, Debug, Default)]
pub struct ReadCancellation(Arc<AtomicBool>);
impl ReadCancellation {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Debug)]
pub struct ReadOptions {
    /// Relative deadline, in milliseconds; zero is invalid.
    pub timeout_ms: Option<u64>,
    /// Maximum physical scan entries, including historical versions and tombstones.
    /// Point reads remain bounded by the engine's key and response limits.
    pub max_scan: Option<usize>,
    /// Reject queries whose plan contains a collection scan. Explain still returns the plan.
    pub allow_full_scan: bool,
    pub cancellation: Option<ReadCancellation>,
}
impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            timeout_ms: None,
            max_scan: None,
            allow_full_scan: true,
            cancellation: None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ReadBudget {
    pub(crate) deadline: Instant,
    pub(crate) max_scan: usize,
    pub(crate) allow_full_scan: bool,
    pub(crate) minimum_seq: Option<u64>,
    pub(crate) cancellation: Option<ReadCancellation>,
}
impl ReadBudget {
    pub(crate) fn new(options: &ReadOptions, engine: &Options) -> Result<Self> {
        if options.timeout_ms == Some(0) || options.max_scan == Some(0) {
            return Err(Error::invalid(
                "read timeout and scan limit must be positive",
            ));
        }
        let out = Self {
            deadline: Instant::now()
                .checked_add(Duration::from_millis(
                    options
                        .timeout_ms
                        .unwrap_or(engine.query_ms)
                        .min(engine.query_ms),
                ))
                .ok_or_else(|| Error::invalid("read deadline overflow"))?,
            max_scan: options
                .max_scan
                .unwrap_or(engine.max_scan)
                .min(engine.max_scan),
            allow_full_scan: options.allow_full_scan,
            minimum_seq: None,
            cancellation: options.cancellation.clone(),
        };
        out.check()?;
        Ok(out)
    }
    pub(crate) fn check(&self) -> Result<()> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(ReadCancellation::is_cancelled)
        {
            return Err(Error::new("CANCELLED", "read cancelled"));
        }
        if Instant::now() >= self.deadline {
            return Err(Error::new("DEADLINE_EXCEEDED", "read deadline"));
        }
        Ok(())
    }
    pub(crate) fn check_scan(&self, stats: &ReadStats) -> Result<()> {
        self.check()?;
        if stats.scanned_entries > self.max_scan as u64 {
            return Err(Error::limit("read scan budget exceeded"));
        }
        Ok(())
    }
}

/// A bounded read session. Its deadline starts at creation and is shared by all
/// calls through this value. Each call takes its own snapshot; use read_batch
/// when multiple operations must see the same snapshot.
pub struct TenantReader<'a> {
    pub(crate) tenant: &'a TenantHandle,
    pub(crate) budget: ReadBudget,
}
impl TenantHandle {
    pub fn read_with(&self, options: &ReadOptions) -> Result<TenantReader<'_>> {
        Ok(TenantReader {
            tenant: self,
            budget: ReadBudget::new(options, &self.engine().options)?,
        })
    }
}
impl TenantReader<'_> {
    pub fn get(&self, collection: &str, key: &str) -> Result<Option<Value>> {
        self.budget.check()?;
        let out = self
            .tenant
            .get_controlled(collection, key, Some(&self.budget))?;
        self.budget.check()?;
        Ok(out)
    }
    pub fn multi_get(&self, collection: &str, keys: &[String]) -> Result<Vec<Option<Value>>> {
        self.tenant
            .multi_get_controlled(collection, keys, &self.budget)
    }
    pub fn query(&self, collection: &str, query: Query) -> Result<QueryResult> {
        let _permit = self.tenant.core.gate.enter()?;
        self.tenant
            .query_view(collection, query, None, None, &self.budget, 0)
    }
    pub fn explain(&self, collection: &str, query: &Query) -> Result<Explain> {
        self.budget.check()?;
        let out = self.tenant.explain(collection, query)?;
        self.budget.check()?;
        Ok(out)
    }
    pub fn lookup_many(
        &self,
        collection: &str,
        lookup: &str,
        inputs: &[Value],
    ) -> Result<LookupResult> {
        self.budget.check()?;
        self.tenant.lookup_snapshot(
            collection,
            lookup,
            inputs,
            self.tenant.engine().snapshot()?,
            &self.budget,
        )
    }
    pub fn read_batch(&self, operations: &[ReadOperation]) -> Result<ReadBatchResult> {
        self.tenant.read_batch_controlled(operations, &self.budget)
    }
    pub fn published_dataset(
        &self,
        resource: &str,
        version: Option<Uuid>,
    ) -> Result<crate::PublishedDataset> {
        self.budget.check()?;
        let out = self.tenant.published_dataset(resource, version)?;
        self.budget.check()?;
        Ok(out)
    }
    pub fn read_published(
        &self,
        resource: &str,
        version: Option<Uuid>,
        requests: &[ReadRequest],
    ) -> Result<crate::PublishedReadBatch> {
        self.tenant
            .read_published_controlled(resource, version, requests, &self.budget)
    }
    pub fn query_published(
        &self,
        resource: &str,
        version: Option<Uuid>,
        query: Query,
    ) -> Result<crate::PublishedQuery> {
        self.tenant
            .query_published_controlled(resource, version, query, &self.budget)
    }
    pub fn lookup_published(
        &self,
        resource: &str,
        version: Option<Uuid>,
        inputs: &[Value],
    ) -> Result<crate::PublishedResult> {
        self.tenant
            .lookup_published_controlled(resource, version, inputs, &self.budget)
    }
    /// Consistency waiting consumes the same deadline as execution.
    pub fn wait_context(&self, history: &str, seq: u64) -> Result<()> {
        let wait_deadline = self
            .budget
            .deadline
            .min(Instant::now() + Duration::from_secs(5));
        loop {
            self.budget.check()?;
            match self.tenant.wait_context(history, seq, 0) {
                Err(error) if error.code == "REPLICA_LAG" => {
                    if Instant::now() >= wait_deadline {
                        return Err(error);
                    }
                    std::thread::sleep(Duration::from_millis(2))
                }
                result => return result,
            }
        }
    }
}
