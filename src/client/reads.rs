use super::{Client, Request};
use crate::{
    Error, Explain, LookupResult, PublishedDataset, PublishedQuery, PublishedReadBatch,
    PublishedResult, Query, QueryResult, ReadBatchResult, ReadContext, ReadOperation, ReadOptions,
    ReadRequest, ReadResult, Result, WriteResult,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// A position is meaningful only within its opaque database history.
#[derive(Clone, Debug)]
pub struct ReadPosition {
    pub history: String,
    pub seq: u64,
}
impl From<&WriteResult> for ReadPosition {
    fn from(value: &WriteResult) -> Self {
        Self {
            history: value.history.clone(),
            seq: value.rev,
        }
    }
}
impl From<&ReadBatchResult> for ReadPosition {
    fn from(value: &ReadBatchResult) -> Self {
        Self {
            history: value.history.clone(),
            seq: value.snapshot_seq,
        }
    }
}
impl From<&ReadContext> for ReadPosition {
    fn from(value: &ReadContext) -> Self {
        Self {
            history: value.history.clone(),
            seq: value.applied_seq,
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct ClientReadOptions {
    pub minimum: Option<ReadPosition>,
    pub read: ReadOptions,
}

pub(super) struct ReadRuntime<'a> {
    pub(super) options: &'a ClientReadOptions,
    pub(super) deadline: Instant,
}
impl ReadRuntime<'_> {
    pub(super) fn check(&self) -> Result<()> {
        if self
            .options
            .read
            .cancellation
            .as_ref()
            .is_some_and(crate::ReadCancellation::is_cancelled)
        {
            return Err(Error::new("CANCELLED", "client read cancelled"));
        }
        if Instant::now() >= self.deadline {
            return Err(Error::new("DEADLINE_EXCEEDED", "client read deadline"));
        }
        Ok(())
    }
}

impl Client {
    // Discovery, fallback, body download and cancellation share one bounded call.
    async fn read<T: DeserializeOwned>(
        &self,
        path: &[&str],
        body: Option<Value>,
        options: &ClientReadOptions,
        pinned: Option<&reqwest::Url>,
        method: reqwest::Method,
    ) -> Result<(T, reqwest::Url)> {
        if options.read.timeout_ms == Some(0) || options.read.max_scan == Some(0) {
            return Err(Error::invalid(
                "read timeout and scan limit must be positive",
            ));
        }
        let runtime = ReadRuntime {
            options,
            deadline: Instant::now()
                + Duration::from_millis(options.read.timeout_ms.unwrap_or(8000).min(8000)),
        };
        runtime.check()?;
        let execute = async {
            if let Some(node) = pinned {
                // Cursor advancement is stateful. Retrying can consume or change a page.
                let value = self
                    .send(
                        node,
                        path,
                        Request {
                            method,
                            body,
                            minimum: options.minimum.as_ref(),
                            runtime: Some(&runtime),
                            read_headers: true,
                        },
                    )
                    .await?;
                return Ok((value, node.clone()));
            }
            let mut routes = self.read_routes(&runtime).await?;
            let mut attempted = [false; 2];
            let mut last = Error::new("UNAVAILABLE", "no readable node");
            for round in 0..2 {
                let mut candidates: Vec<_> = routes.nodes.iter().collect();
                candidates.sort_by_key(|(_, context)| {
                    let behind = options.minimum.as_ref().is_some_and(|minimum| {
                        context.history == minimum.history && context.applied_seq < minimum.seq
                    });
                    if context.writable {
                        1
                    } else if behind {
                        2
                    } else {
                        0
                    }
                });
                for (index, _) in candidates {
                    if attempted[*index] {
                        continue;
                    }
                    attempted[*index] = true;
                    runtime.check()?;
                    let node = &self.nodes[*index];
                    match self
                        .send(
                            node,
                            path,
                            Request {
                                method: method.clone(),
                                body: body.clone(),
                                minimum: options.minimum.as_ref(),
                                runtime: Some(&runtime),
                                read_headers: true,
                            },
                        )
                        .await
                    {
                        Ok(value) => return Ok((value, node.clone())),
                        Err(error) if retryable(&error) => {
                            self.routes.invalidate(&routes);
                            last = error;
                        }
                        Err(error) => {
                            if matches!(
                                error.code.as_str(),
                                "HISTORY_CONFLICT" | "INCOMPATIBLE_VERSION"
                            ) {
                                self.routes.invalidate(&routes);
                            }
                            return Err(error);
                        }
                    }
                }
                if round == 0
                    && attempted[..self.nodes.len()]
                        .iter()
                        .any(|attempted| !attempted)
                {
                    self.routes.invalidate(&routes);
                    routes = self.read_routes(&runtime).await?;
                } else {
                    break;
                }
            }
            Err(last)
        };
        let cancelled = async {
            loop {
                if options
                    .read
                    .cancellation
                    .as_ref()
                    .is_some_and(crate::ReadCancellation::is_cancelled)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::select! {
            biased;
            _ = cancelled, if options.read.cancellation.is_some() => Err(Error::new("CANCELLED", "client read cancelled")),
            _ = tokio::time::sleep_until(runtime.deadline.into()) => Err(Error::new("DEADLINE_EXCEEDED", "client read deadline")),
            result = execute => { runtime.check()?; result }
        }
    }
    fn cursor_origin(&self, node: &reqwest::Url) -> String {
        format!(
            "{:x}",
            Sha256::digest(format!("{}:{}", self.tenant, node).as_bytes())
        )
    }
    fn wrap_cursor(&self, result: &mut QueryResult, node: &reqwest::Url) {
        if let Some(token) = &mut result.cursor {
            *token = format!("tkv1.{}.{}", self.cursor_origin(node), token);
        }
    }
    fn wrap_batch(&self, result: &mut ReadBatchResult, node: &reqwest::Url) {
        for item in &mut result.results {
            if let ReadResult::Query { result } = item {
                self.wrap_cursor(result, node);
            }
        }
    }
    fn decode_cursor(&self, token: &str) -> Result<(&reqwest::Url, String)> {
        let invalid = || {
            Error::new(
                "CURSOR_EXPIRED",
                "cursor does not belong to a configured node and tenant",
            )
        };
        if token.len() > 110 {
            return Err(invalid());
        }
        let parts: Vec<_> = token.split('.').collect();
        if parts.len() != 3 || parts[0] != "tkv1" {
            return Err(invalid());
        }
        let key = Uuid::parse_str(parts[2]).map_err(|_| invalid())?;
        let node = self
            .nodes
            .iter()
            .find(|node| self.cursor_origin(node) == parts[1])
            .ok_or_else(invalid)?;
        Ok((node, key.to_string()))
    }
    pub async fn get_with_options(
        &self,
        collection: &str,
        key: &str,
        options: &ClientReadOptions,
    ) -> Result<Option<Value>> {
        match self
            .read(
                &[
                    "v1",
                    "tenants",
                    &self.tenant.to_string(),
                    "collections",
                    collection,
                    "documents",
                    key,
                ],
                None,
                options,
                None,
                reqwest::Method::GET,
            )
            .await
        {
            Ok((value, _)) => Ok(Some(value)),
            Err(error) if error.code == "NOT_FOUND" => Ok(None),
            Err(error) => Err(error),
        }
    }
    pub async fn multi_get(
        &self,
        collection: &str,
        keys: &[String],
        options: &ClientReadOptions,
    ) -> Result<Vec<Option<Value>>> {
        Ok(self
            .read(
                &[
                    "v1",
                    "tenants",
                    &self.tenant.to_string(),
                    "collections",
                    collection,
                    "multi_get",
                ],
                Some(json!({"keys":keys})),
                options,
                None,
                reqwest::Method::POST,
            )
            .await?
            .0)
    }
    /// Returned cursors are SDK tokens bound to this tenant and the original node.
    pub async fn query(
        &self,
        collection: &str,
        mut query: Query,
        options: &ClientReadOptions,
    ) -> Result<QueryResult> {
        let pinned = query
            .cursor
            .as_deref()
            .map(|token| self.decode_cursor(token))
            .transpose()?;
        query.cursor = pinned.as_ref().map(|(_, key)| key.clone());
        let (mut result, node) = self
            .read(
                &[
                    "v1",
                    "tenants",
                    &self.tenant.to_string(),
                    "collections",
                    collection,
                    "query",
                ],
                Some(serde_json::to_value(query)?),
                options,
                pinned.map(|(node, _)| node),
                reqwest::Method::POST,
            )
            .await?;
        self.wrap_cursor(&mut result, &node);
        Ok(result)
    }
    pub async fn explain(
        &self,
        collection: &str,
        query: &Query,
        options: &ClientReadOptions,
    ) -> Result<Explain> {
        let mut query = query.clone();
        let pinned = query
            .cursor
            .as_deref()
            .map(|token| self.decode_cursor(token))
            .transpose()?;
        query.cursor = pinned.as_ref().map(|(_, key)| key.clone());
        Ok(self
            .read(
                &[
                    "v1",
                    "tenants",
                    &self.tenant.to_string(),
                    "collections",
                    collection,
                    "explain",
                ],
                Some(serde_json::to_value(query)?),
                options,
                pinned.map(|(node, _)| node),
                reqwest::Method::POST,
            )
            .await?
            .0)
    }
    pub async fn lookup_many(
        &self,
        collection: &str,
        lookup: &str,
        inputs: &[Value],
        options: &ClientReadOptions,
    ) -> Result<LookupResult> {
        Ok(self
            .read(
                &[
                    "v1",
                    "tenants",
                    &self.tenant.to_string(),
                    "collections",
                    collection,
                    "lookups",
                    lookup,
                    "execute",
                ],
                Some(json!({"inputs":inputs})),
                options,
                None,
                reqwest::Method::POST,
            )
            .await?
            .0)
    }
    pub async fn read_batch(
        &self,
        operations: &[ReadOperation],
        options: &ClientReadOptions,
    ) -> Result<ReadBatchResult> {
        let (mut result, node) = self
            .read(
                &["v1", "tenants", &self.tenant.to_string(), "read_batch"],
                Some(json!({"operations":operations})),
                options,
                None,
                reqwest::Method::POST,
            )
            .await?;
        self.wrap_batch(&mut result, &node);
        Ok(result)
    }
    /// Read the latest publication metadata; explicit versions are selected by the data APIs.
    pub async fn published_dataset(
        &self,
        resource: &str,
        options: &ClientReadOptions,
    ) -> Result<PublishedDataset> {
        Ok(self
            .read(
                &[
                    "v1",
                    "tenants",
                    &self.tenant.to_string(),
                    "resources",
                    resource,
                ],
                None,
                options,
                None,
                reqwest::Method::GET,
            )
            .await?
            .0)
    }
    pub async fn read_published(
        &self,
        resource: &str,
        version: Option<Uuid>,
        operations: &[ReadRequest],
        options: &ClientReadOptions,
    ) -> Result<PublishedReadBatch> {
        let (mut result, node): (PublishedReadBatch, _) = self
            .read(
                &[
                    "v1",
                    "tenants",
                    &self.tenant.to_string(),
                    "resources",
                    resource,
                    "read",
                ],
                Some(json!({"version":version,"operations":operations})),
                options,
                None,
                reqwest::Method::POST,
            )
            .await?;
        self.wrap_batch(&mut result.result, &node);
        Ok(result)
    }
    pub async fn query_published(
        &self,
        resource: &str,
        version: Option<Uuid>,
        mut query: Query,
        options: &ClientReadOptions,
    ) -> Result<PublishedQuery> {
        let pinned = query
            .cursor
            .as_deref()
            .map(|token| self.decode_cursor(token))
            .transpose()?;
        query.cursor = pinned.as_ref().map(|(_, key)| key.clone());
        let (mut result, node): (PublishedQuery, _) = self
            .read(
                &[
                    "v1",
                    "tenants",
                    &self.tenant.to_string(),
                    "resources",
                    resource,
                    "query",
                ],
                Some(json!({"version":version,"query":query})),
                options,
                pinned.map(|(node, _)| node),
                reqwest::Method::POST,
            )
            .await?;
        self.wrap_cursor(&mut result.result, &node);
        Ok(result)
    }
    pub async fn lookup_published(
        &self,
        resource: &str,
        version: Option<Uuid>,
        inputs: &[Value],
        options: &ClientReadOptions,
    ) -> Result<PublishedResult> {
        Ok(self
            .read(
                &[
                    "v1",
                    "tenants",
                    &self.tenant.to_string(),
                    "resources",
                    resource,
                    "execute",
                ],
                Some(json!({"version":version,"inputs":inputs})),
                options,
                None,
                reqwest::Method::POST,
            )
            .await?
            .0)
    }
    /// Release on the issuing node only. An unreachable node reclaims by snapshot lease.
    pub async fn release_cursor(&self, cursor: &str) -> Result<()> {
        let (node, key) = self.decode_cursor(cursor)?;
        let _: Value = self
            .send(
                node,
                &["v1", "tenants", &self.tenant.to_string(), "cursors", &key],
                Request {
                    method: reqwest::Method::DELETE,
                    body: None,
                    minimum: None,
                    runtime: None,
                    read_headers: false,
                },
            )
            .await?;
        Ok(())
    }
}
pub(super) fn retryable(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "TRANSPORT_UNAVAILABLE" | "UNAVAILABLE" | "REPLICA_LAG"
    )
}
