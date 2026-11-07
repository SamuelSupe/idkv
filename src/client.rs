//! Bounded HTTP client for a primary/replica pair. Discovery never promotes a
//! node. Only durable, caller-assigned operation IDs permit write retries.
mod reads;
mod routes;
use crate::{Error, ReadContext, Result, WriteOp, WriteOptions, WriteResult};
use opentelemetry::{
    Context,
    propagation::{Injector, TextMapPropagator},
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
pub use reads::{ClientReadOptions, ReadPosition};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    nodes: Vec<reqwest::Url>,
    token: String,
    tenant: Uuid,
    routes: std::sync::Arc<routes::RouteCache>,
}

impl Client {
    pub fn new(nodes: &[String], token: String, tenant: Uuid) -> Result<Self> {
        if nodes.is_empty() || nodes.len() > 2 || !(16..=512).contains(&token.len()) {
            return Err(Error::invalid(
                "configure one or two nodes and a valid token",
            ));
        }
        let nodes = nodes
            .iter()
            .map(|node| {
                let url = reqwest::Url::parse(node).map_err(|_| Error::invalid("node URL"))?;
                let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
                if !(url.scheme() == "https" || (url.scheme() == "http" && local))
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.path() != "/"
                    || url.query().is_some()
                    || url.fragment().is_some()
                {
                    return Err(Error::invalid(
                        "node must be an HTTPS origin (HTTP allowed on loopback)",
                    ));
                }
                Ok(url)
            })
            .collect::<Result<Vec<_>>>()?;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(2))
            .pool_idle_timeout(std::time::Duration::from_secs(4))
            .timeout(std::time::Duration::from_secs(8))
            .build()
            .map_err(|_| Error::invalid("client configuration"))?;
        Ok(Self {
            http,
            nodes,
            token,
            tenant,
            routes: Default::default(),
        })
    }

    async fn request<T: DeserializeOwned>(
        &self,
        node: &reqwest::Url,
        path: &[&str],
        body: Option<Value>,
        minimum: Option<&WriteResult>,
    ) -> Result<T> {
        let minimum = minimum.map(ReadPosition::from);
        self.send(
            node,
            path,
            Request {
                method: if body.is_some() {
                    reqwest::Method::POST
                } else {
                    reqwest::Method::GET
                },
                body,
                minimum: minimum.as_ref(),
                runtime: None,
                read_headers: false,
            },
        )
        .await
    }

    async fn send<T: DeserializeOwned>(
        &self,
        node: &reqwest::Url,
        path: &[&str],
        spec: Request<'_>,
    ) -> Result<T> {
        let mut url = node.clone();
        url.path_segments_mut()
            .map_err(|_| Error::invalid("node URL"))?
            .clear()
            .extend(path);
        let mut headers = reqwest::header::HeaderMap::new();
        TraceContextPropagator::new()
            .inject_context(&Context::current(), &mut Headers(&mut headers));
        let mut request = self.http.request(spec.method, url);
        if let Some(body) = spec.body {
            request = request.json(&body);
        }
        request = request.bearer_auth(&self.token).headers(headers);
        if let Some(minimum) = spec.minimum {
            request = request
                .header("x-tkv-min-history", &minimum.history)
                .header("x-tkv-min-seq", minimum.seq);
        }
        if let Some(runtime) = spec.runtime {
            runtime.check()?;
            let remaining = runtime
                .deadline
                .saturating_duration_since(std::time::Instant::now());
            request = request.timeout(if spec.read_headers {
                remaining
            } else {
                (remaining / 4).min(std::time::Duration::from_secs(2))
            });
            if spec.read_headers {
                request = request
                    .header(
                        "x-tkv-read-timeout-ms",
                        remaining.as_millis().max(1).min(u64::MAX as u128) as u64,
                    )
                    .header(
                        "x-tkv-allow-full-scan",
                        runtime.options.read.allow_full_scan.to_string(),
                    );
                if let Some(limit) = runtime.options.read.max_scan {
                    request = request.header("x-tkv-max-scan", limit);
                }
            }
        }
        let mut response = request.send().await.map_err(|_| {
            Error::new(
                "TRANSPORT_UNAVAILABLE",
                "request outcome unknown; retry writes only with the same operation ID",
            )
        })?;
        let success = response.status().is_success();
        if success
            && spec.runtime.is_some()
            && (spec.read_headers || path.last() == Some(&"context"))
            && response
                .headers()
                .get("x-tkv-read-controls")
                .and_then(|v| v.to_str().ok())
                != Some("1")
        {
            return Err(Error::new(
                "INCOMPATIBLE_VERSION",
                "node does not acknowledge request read controls v1",
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Error::new("TRANSPORT_UNAVAILABLE", "response interrupted"))?
        {
            if bytes.len().saturating_add(chunk.len()) > 16 << 20 {
                return Err(Error::limit("client response size"));
            }
            bytes.extend_from_slice(&chunk);
        }
        if !success {
            let body: Value = serde_json::from_slice(&bytes)
                .map_err(|_| Error::new("UNAVAILABLE", "invalid server error response"))?;
            return Err(serde_json::from_value(body["error"].clone())
                .map_err(|_| Error::new("UNAVAILABLE", "invalid server error response"))?);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn contexts(&self) -> Result<Vec<(&reqwest::Url, ReadContext)>> {
        let tenant = self.tenant.to_string();
        let mut nodes = Vec::new();
        let mut last = Error::new("UNAVAILABLE", "no reachable compatible node");
        for node in &self.nodes {
            match self
                .request::<ReadContext>(node, &["v1", "tenants", &tenant, "context"], None, None)
                .await
            {
                Ok(context) if context.tenant == self.tenant => nodes.push((node, context)),
                Ok(_) => {
                    return Err(Error::new(
                        "INCOMPATIBLE_VERSION",
                        "server returned wrong tenant context",
                    ));
                }
                Err(error) => last = error,
            }
        }
        if nodes.is_empty() {
            Err(last)
        } else {
            Ok(nodes)
        }
    }

    /// Obtain a primary history token before preparing conditional/idempotent writes.
    pub async fn write_context(&self) -> Result<ReadContext> {
        let mut primaries = self
            .contexts()
            .await?
            .into_iter()
            .filter(|(_, context)| context.writable);
        let (_, context) = primaries
            .next()
            .ok_or_else(|| Error::new("READ_ONLY", "no writable primary"))?;
        if primaries.next().is_some() {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "multiple writable nodes",
            ));
        }
        Ok(context)
    }

    /// Obtain once, persist with the intended operation, and reuse unchanged on retries.
    pub async fn retry_context(&self) -> Result<crate::RetryContext> {
        let nodes = self.contexts().await?;
        let mut primary = nodes.into_iter().filter(|(_, c)| c.writable);
        let (node, _) = primary
            .next()
            .ok_or_else(|| Error::new("READ_ONLY", "no writable primary"))?;
        if primary.next().is_some() {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "multiple writable nodes",
            ));
        }
        self.request(
            node,
            &["v1", "tenants", &self.tenant.to_string(), "retry-context"],
            Some(json!({})),
            None,
        )
        .await
    }

    /// Prefer the replica. Existing callers may continue to pass a write receipt.
    pub async fn get(
        &self,
        collection: &str,
        key: &str,
        minimum: Option<&WriteResult>,
    ) -> Result<Option<Value>> {
        self.get_with_options(
            collection,
            key,
            &ClientReadOptions {
                minimum: minimum.map(ReadPosition::from),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn write_batch(
        &self,
        collection: &str,
        operations: Vec<WriteOp>,
        options: WriteOptions,
    ) -> Result<Vec<WriteResult>> {
        let attempts = if options.operation_id.is_some() && options.expected_history.is_some() {
            2
        } else {
            1
        };
        let tenant = self.tenant.to_string();
        let body = json!({"operations":operations,"write_options":options});
        let mut last = Error::new("READ_ONLY", "no writable primary");
        let discovery_options = ClientReadOptions::default();
        let runtime = reads::ReadRuntime {
            options: &discovery_options,
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(8),
        };
        for _ in 0..attempts {
            let routes = self.read_routes(&runtime).await?;
            let mut primary = routes.nodes.iter().filter(|(_, context)| context.writable);
            let Some((index, _)) = primary.next() else {
                self.routes.invalidate(&routes);
                return Err(last);
            };
            if primary.next().is_some() {
                return Err(Error::new(
                    "REPLICATION_CONFLICT",
                    "multiple writable nodes; operator fencing required",
                ));
            }
            match self
                .request(
                    &self.nodes[*index],
                    &["v1", "tenants", &tenant, "collections", collection, "batch"],
                    Some(body.clone()),
                    None,
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        "TRANSPORT_UNAVAILABLE"
                            | "READ_ONLY"
                            | "UNAVAILABLE"
                            | "COMMIT_UNKNOWN"
                            | "RESPONSE_UNAVAILABLE"
                    ) =>
                {
                    self.routes.invalidate(&routes);
                    last = error
                }
                Err(error) => return Err(error),
            }
        }
        Err(last)
    }
}
struct Headers<'a>(&'a mut reqwest::header::HeaderMap);
impl Injector for Headers<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(key), Ok(value)) = (
            reqwest::header::HeaderName::from_bytes(key.as_bytes()),
            value.parse(),
        ) {
            self.0.insert(key, value);
        }
    }
}

struct Request<'a> {
    method: reqwest::Method,
    body: Option<Value>,
    minimum: Option<&'a ReadPosition>,
    runtime: Option<&'a reads::ReadRuntime<'a>>,
    read_headers: bool,
}
