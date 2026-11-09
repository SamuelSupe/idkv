//! HTTP authentication is applied before opening any tenant database or reading
//! request bodies. A URL tenant ID never establishes authorization.
mod auth;
mod operations;
mod read_controls;
use auth::allows;
pub use auth::{AuthConfig, Role, Token, token_hash};
mod backups;
mod body;
mod transport;
pub use transport::Listener;
mod contracts;
mod identity;
mod openapi;
mod replication;
use crate::{Error, Result, Store, model::*};
use axum::{
    Json, Router,
    extract::{MatchedPath, Path, Request, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
pub use openapi::openapi;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Semaphore;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    store: Store,
    auth: Arc<AuthConfig>,
    backups: Option<crate::backup::BackupService>,
    replication: Option<crate::replication::Service>,
    replication_workers: Arc<Semaphore>,
    metrics_workers: Arc<Semaphore>,
    workers: Arc<Semaphore>,
    body_workers: Arc<Semaphore>,
    admission_workers: Arc<Semaphore>,
    index_workers: Arc<Semaphore>,
    lifecycle_workers: Arc<Semaphore>,
    lifecycle_tenants: Arc<Mutex<HashMap<Uuid, std::sync::Weak<Semaphore>>>>,
    maintenance_workers: Arc<Semaphore>,
    maintenance_tenants: Arc<Mutex<HashMap<Uuid, std::sync::Weak<Semaphore>>>>,
    tenants: Arc<Mutex<HashMap<Uuid, std::sync::Weak<Semaphore>>>>,
    runtime: HttpRuntime,
    openapi: axum::body::Bytes,
}

/// Shared lifecycle and observability for the HTTP adapter. Embedders own its
/// shutdown; constructing a router never installs a process-global subscriber.
#[derive(Clone, Default)]
pub struct HttpRuntime {
    pub telemetry: crate::telemetry::Telemetry,
    draining: Arc<std::sync::atomic::AtomicBool>,
    not_ready: Arc<std::sync::atomic::AtomicBool>,
    backup_startup_degraded: bool,
}
impl HttpRuntime {
    pub fn new(telemetry: crate::telemetry::Telemetry) -> Self {
        Self {
            telemetry,
            ..Self::default()
        }
    }
    /// Expose a startup failure of the optional remote backup service.
    pub fn mark_backup_startup_degraded(&mut self) {
        self.backup_startup_degraded = true;
    }
    /// Withdraw readiness while continuing to serve traffic during LB propagation.
    pub fn withdraw_readiness(&self) {
        self.not_ready
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub fn drain(&self) {
        self.withdraw_readiness();
        self.draining
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub fn is_draining(&self) -> bool {
        self.draining.load(std::sync::atomic::Ordering::Acquire)
    }
}
const ROUTES: &[&str] = &[
    "/ready",
    "/ready/write",
    "/metrics",
    "/v1/capabilities",
    "/v1/tenants/{tenant}/context",
    "/v1/tenants/{tenant}/read_batch",
    "/v1/tenants/{tenant}/retention",
    "/v1/tenants/{tenant}/retry-context",
    "/v1/tenants/{tenant}/resources/{resource}",
    "/v1/tenants/{tenant}/resources/{resource}/execute",
    "/v1/tenants/{tenant}/resources/{resource}/read",
    "/v1/tenants/{tenant}/resources/{resource}/query",
    "/v1/tenants/{tenant}/identity/initialize",
    "/v1/tenants/{tenant}/identity/write",
    "/v1/tenants/{tenant}/identity/resolve",
    "/v1/tenants/{tenant}/identity/resolve_many",
    "/v1/tenants/{tenant}/identity/ids",
    "/v1/tenants/{tenant}/identity/history",
    "/v1/tenants/{tenant}/identity/timeline",
    "/v1/tenants/{tenant}/identity/entities/{entity}",
    "/v1/tenants/{tenant}/identity/operations/{operation}",
    "/v1/replication/status",
    "/v1/replication/log",
    "/v1/replication/snapshot",
    "/v1/replication/snapshot/chunk",
    "/v1/replication/snapshot/release",
    "/v1/replication/fence",
    "/v1/replication/promote",
    "/v1/replication/follow",
    "/v1/replication/switchover",
    "/v1/replication/force-promote",
    "/health",
    "/openapi.json",
    "/v1/tenants",
    "/v1/metrics",
    "/v1/tenants/{tenant}",
    "/v1/tenants/{tenant}/dsl",
    "/v1/tenants/{tenant}/replication/wait",
    "/v1/tenants/{tenant}/close",
    "/v1/tenants/{tenant}/backup",
    "/v1/tenants/{tenant}/backup/{backup}",
    "/v1/tenants/{tenant}/restore",
    "/v1/tenants/{tenant}/remote-backups",
    "/v1/tenants/{tenant}/remote-backups/prune",
    "/v1/tenants/{tenant}/remote-backups/{backup}/verify",
    "/v1/tenants/{tenant}/remote-backups/{backup}/restore",
    "/v1/tenants/{tenant}/remote-backups/{backup}/delete",
    "/v1/tenants/{tenant}/backup-jobs",
    "/v1/tenants/{tenant}/backup-jobs/{job}",
    "/v1/tenants/{tenant}/backup-jobs/{job}/retry",
    "/v1/tenants/{tenant}/check",
    "/v1/tenants/{tenant}/checkpoint",
    "/v1/tenants/{tenant}/compact",
    "/v1/tenants/{tenant}/collections",
    "/v1/tenants/{tenant}/collections/{collection}",
    "/v1/tenants/{tenant}/collections/{collection}/documents",
    "/v1/tenants/{tenant}/collections/{collection}/documents/{key}",
    "/v1/tenants/{tenant}/collections/{collection}/batch",
    "/v1/tenants/{tenant}/collections/{collection}/multi_get",
    "/v1/tenants/{tenant}/collections/{collection}/indexes",
    "/v1/tenants/{tenant}/collections/{collection}/indexes/{index}",
    "/v1/tenants/{tenant}/collections/{collection}/lookups",
    "/v1/tenants/{tenant}/collections/{collection}/lookups/{lookup}",
    "/v1/tenants/{tenant}/collections/{collection}/lookups/{lookup}/execute",
    "/v1/tenants/{tenant}/collections/{collection}/query",
    "/v1/tenants/{tenant}/collections/{collection}/explain",
    "/v1/tenants/{tenant}/cursors/{cursor}",
];
pub fn app(store: Store, auth: AuthConfig) -> Result<Router> {
    app_with_backups(store, auth, None)
}
pub fn app_with_backups(
    store: Store,
    auth: AuthConfig,
    backups: Option<crate::backup::BackupService>,
) -> Result<Router> {
    app_with_replication(store, auth, backups, None)
}
pub fn app_with_replication(
    store: Store,
    auth: AuthConfig,
    backups: Option<crate::backup::BackupService>,
    replication: Option<crate::replication::Service>,
) -> Result<Router> {
    app_with_runtime(store, auth, backups, replication, HttpRuntime::default())
}
pub fn app_with_runtime(
    store: Store,
    auth: AuthConfig,
    backups: Option<crate::backup::BackupService>,
    replication: Option<crate::replication::Service>,
    runtime: HttpRuntime,
) -> Result<Router> {
    auth.validate()?;
    if replication
        .as_ref()
        .is_some_and(|r| r.store_root() != store.root())
    {
        return Err(Error::invalid(
            "replication service belongs to a different Store",
        ));
    }
    if backups
        .as_ref()
        .is_some_and(|b| b.store_root() != store.root())
    {
        return Err(Error::invalid(
            "backup service belongs to a different Store",
        ));
    }
    let workers = Arc::new(Semaphore::new(store.options().max_http_requests));
    let body_workers = Arc::new(Semaphore::new(store.options().max_http_body_requests));
    let maintenance_workers = Arc::new(Semaphore::new(
        store.options().max_http_maintenance_requests,
    ));
    let admission_workers = Arc::new(Semaphore::new(store.options().max_http_admission_requests));
    let index_workers = Arc::new(Semaphore::new(store.options().max_http_index_requests));
    let lifecycle_workers = Arc::new(Semaphore::new(store.options().max_http_lifecycle_requests));
    let openapi = serde_json::to_vec(&openapi::openapi_for(store.options()))?.into();
    let state = AppState {
        openapi,
        store,
        auth: Arc::new(auth),
        backups,
        replication,
        replication_workers: Arc::new(Semaphore::new(4)),
        metrics_workers: Arc::new(Semaphore::new(1)),
        workers,
        body_workers,
        admission_workers,
        index_workers,
        lifecycle_workers,
        lifecycle_tenants: Arc::new(Mutex::new(HashMap::new())),
        maintenance_workers,
        maintenance_tenants: Arc::new(Mutex::new(HashMap::new())),
        tenants: Arc::new(Mutex::new(HashMap::new())),
        runtime,
    };
    let mut router = Router::new();
    for route in ROUTES {
        router = router.route(route, any(dispatch));
    }
    Ok(router.with_state(state))
}
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match self.code.as_str() {
            "UNAUTHENTICATED" => StatusCode::UNAUTHORIZED,
            "FORBIDDEN" => StatusCode::FORBIDDEN,
            "NOT_FOUND" => StatusCode::NOT_FOUND,
            "METHOD_NOT_ALLOWED" => StatusCode::METHOD_NOT_ALLOWED,
            "INVALID_ARGUMENT" => StatusCode::BAD_REQUEST,
            "HISTORY_REQUIRED" | "RETRY_TOKEN_REQUIRED" => StatusCode::PRECONDITION_REQUIRED,
            "RESOURCE_EXHAUSTED" | "MATCH_LIMIT_EXCEEDED" => StatusCode::TOO_MANY_REQUESTS,
            "IDEMPOTENCY_EXPIRED" | "VERSION_UNAVAILABLE" | "CURSOR_EXPIRED" => StatusCode::GONE,
            "DEADLINE_EXCEEDED" => StatusCode::REQUEST_TIMEOUT,
            "SCAN_NOT_ALLOWED" => StatusCode::UNPROCESSABLE_ENTITY,
            "UNIQUE_CONFLICT"
            | "REVISION_CONFLICT"
            | "ALREADY_EXISTS"
            | "CONFLICT"
            | "BUSY"
            | "INDEX_NOT_READY"
            | "CANCELLED"
            | "INTERVAL_CONFLICT"
            | "OPERATION_CONFLICT"
            | "MANAGED_COLLECTION"
            | "KNOWLEDGE_UNAVAILABLE"
            | "SNAPSHOT_TOO_OLD" => StatusCode::CONFLICT,
            "RETENTION_DISABLED"
            | "HISTORY_CONFLICT"
            | "IMMUTABLE_DATASET"
            | "INCOMPATIBLE_VERSION" => StatusCode::CONFLICT,
            "CLOCK_SKEW" => StatusCode::SERVICE_UNAVAILABLE,
            "READ_ONLY" | "REPLICATION_CONFLICT" | "REPLICA_LAG" | "REPLICA_SNAPSHOT_REQUIRED" => {
                StatusCode::CONFLICT
            }
            "LOG_GONE" | "SNAPSHOT_EXPIRED" => StatusCode::GONE,
            "REPLICATION_UNAVAILABLE" => StatusCode::SERVICE_UNAVAILABLE,
            "UNAVAILABLE" | "RESPONSE_UNAVAILABLE" | "RECOVERY_REQUIRED" => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({"error":self}))).into_response()
    }
}
fn tenant_admission(
    tenants: &Mutex<HashMap<Uuid, std::sync::Weak<Semaphore>>>,
    id: Uuid,
    limit: usize,
    label: &str,
) -> Result<Arc<tokio::sync::OwnedSemaphorePermit>> {
    let mut tenants = tenants.lock();
    if tenants.len() >= 16_384 {
        tenants.retain(|_, gate| gate.strong_count() > 0);
    }
    let semaphore = tenants
        .get(&id)
        .and_then(std::sync::Weak::upgrade)
        .unwrap_or_else(|| {
            let gate = Arc::new(Semaphore::new(limit));
            tenants.insert(id, Arc::downgrade(&gate));
            gate
        });
    Ok(Arc::new(
        semaphore
            .try_acquire_owned()
            .map_err(|_| Error::limit(label))?,
    ))
}
async fn dispatch(
    State(state): State<AppState>,
    route: MatchedPath,
    Path(params): Path<HashMap<String, String>>,
    method: Method,
    headers: HeaderMap,
    request: Request,
) -> Response {
    let observation = state
        .runtime
        .telemetry
        .begin(route.as_str(), method.as_str(), &headers);
    let context = observation.context();
    let result = dispatch_async(
        state,
        route.as_str().to_owned(),
        params,
        method,
        headers,
        request,
        context,
    )
    .await;
    let response = match result {
        Ok(response) => response,
        Err(error) => error.into_response(),
    };
    observation.finish(response.status().as_u16());
    response
}
async fn dispatch_async(
    state: AppState,
    route: String,
    params: HashMap<String, String>,
    method: Method,
    headers: HeaderMap,
    request: Request,
    trace_context: opentelemetry::Context,
) -> Result<Response> {
    if route == "/health" && method == Method::GET {
        return Ok(Json(json!({"status":"ok"})).into_response());
    }
    if route == "/v1/capabilities" && method == Method::GET {
        return Ok(Json(crate::replication::capabilities()).into_response());
    }
    if route == "/openapi.json" && method == Method::GET {
        return Ok((
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            state.openapi.clone(),
        )
            .into_response());
    }
    if matches!(route.as_str(), "/ready" | "/ready/write") && method == Method::GET {
        let role = state.store.replication_state().role;
        let ready = !state
            .runtime
            .not_ready
            .load(std::sync::atomic::Ordering::Acquire)
            && role != crate::replication::Role::Fenced
            && (route == "/ready" || state.store.writable());
        return Ok((
            if ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            },
            Json(json!({"ready":ready,"role":role,"draining":state.runtime.not_ready.load(std::sync::atomic::Ordering::Acquire)})),
        )
            .into_response());
    }
    if state.runtime.is_draining() {
        return Err(Error::new("UNAVAILABLE", "server is draining"));
    }
    let token = state.auth.authenticate(&headers)?;
    let requested_read_options = read_controls::parse(&headers)?;
    let explicit_read_controls = requested_read_options.is_some();
    let supports_controls = read_controls::supports(&route, &method);
    if explicit_read_controls && !supports_controls {
        return Err(Error::invalid(
            "read controls are only valid on KV read routes",
        ));
    }
    let cancel = crate::ReadCancellation::default();
    let _cancel_on_drop = read_controls::CancelOnDrop(cancel.clone());
    let mut read_budget = if supports_controls {
        let mut options = requested_read_options.unwrap_or_default();
        options.cancellation = Some(cancel);
        Some(crate::read::ReadBudget::new(
            &options,
            state.store.options(),
        )?)
    } else {
        None
    };
    crate::telemetry::authenticated(&trace_context, &token.subject);
    if matches!(route.as_str(), "/metrics" | "/v1/metrics")
        && !state.runtime.telemetry.metrics_enabled()
    {
        return Err(Error::missing("metrics disabled"));
    }
    if matches!(route.as_str(), "/metrics" | "/v1/metrics")
        && (token.role != Role::Admin || token.tenant.is_some())
    {
        return Err(Error::new("FORBIDDEN", "global admin required for metrics"));
    }
    if route == "/metrics" && method == Method::GET {
        let permit = state
            .metrics_workers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::limit("metrics concurrency"))?;
        return tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut memory = state.store.reserve_http(2 << 20)?;
            let mut rendered = state.runtime.telemetry.render_metrics(&state.store);
            if let Some(replication) = &state.replication {
                rendered.push_str(&replication.render_metrics());
            }
            if let Some(backups) = &state.backups {
                rendered.push_str(&backups.render_metrics());
            }
            rendered.push_str(&format!(
                "# HELP tkv_backup_startup_degraded Whether remote backups failed to start.\n# TYPE tkv_backup_startup_degraded gauge\ntkv_backup_startup_degraded {}\n",
                u8::from(state.runtime.backup_startup_degraded)
            ));
            let bytes = rendered.into_bytes();
            memory.shrink(bytes.capacity());
            Ok(Response::builder()
                .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                .body(axum::body::Body::from(axum::body::Bytes::from_owner(
                    ResponseBytes {
                        bytes,
                        _memory: vec![memory],
                    },
                )))
                .unwrap())
        })
        .await
        .map_err(|_| Error::new("INTERNAL", "metrics worker failed"))?;
    }
    if route.starts_with("/v1/replication/") {
        if token.tenant.is_some()
            || !matches!(token.role, Role::Admin | Role::Replication)
            || (token.role == Role::Replication && !replication::peer_route(&route))
        {
            return Err(Error::new(
                "FORBIDDEN",
                "replication or global administration permission required",
            ));
        }
        return replication::dispatch(state, route, method, request).await;
    }
    if token.role == Role::Replication {
        return Err(Error::new(
            "FORBIDDEN",
            "replication credential cannot access application routes",
        ));
    }
    let tenant = params
        .get("tenant")
        .map(|s| Uuid::parse_str(s).map_err(|_| Error::invalid("tenant UUID")))
        .transpose()?;
    if token.tenant.is_some() && token.tenant != tenant {
        return Err(Error::new("FORBIDDEN", "token cannot access this tenant"));
    }
    if tenant.is_none() && token.tenant.is_some() {
        return Err(Error::new("FORBIDDEN", "global admin required"));
    }
    let required = required_role(&route, &method)?;
    if (route.ends_with("/retry-context") && token.role == Role::Read)
        || !allows(&token.role, &required)
    {
        return Err(Error::new("FORBIDDEN", "insufficient token role"));
    }
    if route == "/v1/tenants/{tenant}" && method != Method::GET && token.tenant.is_some() {
        return Err(Error::new(
            "FORBIDDEN",
            "global tenant administration required",
        ));
    }
    let index = route.ends_with("/indexes") && method == Method::POST;
    let lifecycle = route == "/v1/tenants/{tenant}/close"
        || (route == "/v1/tenants/{tenant}" && method != Method::GET);
    let maintenance = matches!(
        route.as_str(),
        "/v1/tenants/{tenant}/compact"
            | "/v1/tenants/{tenant}/check"
            | "/v1/tenants/{tenant}/checkpoint"
            | "/v1/tenants/{tenant}/backup"
            | "/v1/tenants/{tenant}/backup/{backup}"
            | "/v1/tenants/{tenant}/restore"
    );
    let workers = if index {
        &state.index_workers
    } else if lifecycle {
        &state.lifecycle_workers
    } else if maintenance {
        &state.maintenance_workers
    } else {
        &state.workers
    };
    if workers.available_permits() == 0 {
        return Err(Error::limit("HTTP worker concurrency"));
    }
    let tenant_permit = tenant
        .map(|id| {
            tenant_admission(
                &state.tenants,
                id,
                state.store.options().max_tenant_requests,
                "tenant HTTP concurrency",
            )
        })
        .transpose()?;
    let maintenance_permit = tenant
        .filter(|_| maintenance || index)
        .map(|id| {
            tenant_admission(
                &state.maintenance_tenants,
                id,
                1,
                "tenant maintenance concurrency",
            )
        })
        .transpose()?;
    let lifecycle_permit = tenant
        .filter(|_| lifecycle)
        .map(|id| {
            tenant_admission(
                &state.lifecycle_tenants,
                id,
                1,
                "tenant lifecycle concurrency",
            )
        })
        .transpose()?;
    let empty_body = axum::body::HttpBody::is_end_stream(request.body());
    let receiving = if empty_body {
        None
    } else {
        Some(Arc::new(
            state
                .body_workers
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::limit("HTTP body concurrency"))?,
        ))
    };
    let body_timeout = read_budget
        .as_ref()
        .map_or(std::time::Duration::from_secs(5), |budget| {
            budget
                .deadline
                .saturating_duration_since(std::time::Instant::now())
                .min(std::time::Duration::from_secs(5))
        });
    let body_deadline = tokio::time::Instant::now() + body_timeout;
    let prepared_budget = loop {
        match state.store.try_http_input_budget(tenant) {
            Ok(budget) => break budget,
            Err(error) if error.code == "BUSY" => {
                tokio::time::timeout_at(
                    body_deadline,
                    tokio::time::sleep(std::time::Duration::from_millis(1)),
                )
                .await
                .map_err(|_| Error::new("DEADLINE_EXCEEDED", "HTTP input admission deadline"))?;
            }
            Err(error) => return Err(error),
        }
    };
    let input_budget = if let Some(budget) = prepared_budget {
        budget
    } else {
        // Keep admission permits on the blocking task after HTTP cancellation.
        // Hot requests avoid this thread hop; no async path waits on these locks.
        let input_store = state.store.clone();
        let input_receiving = receiving.clone();
        let input_tenant = tenant_permit.clone();
        let input_maintenance = maintenance_permit.clone();
        let input_lifecycle = lifecycle_permit.clone();
        let input_admission = state
            .admission_workers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::limit("HTTP input admission concurrency"))?;
        let admission = tokio::task::spawn_blocking(move || {
            let _receiving = input_receiving;
            let _tenant = input_tenant;
            let _maintenance = input_maintenance;
            let _lifecycle = input_lifecycle;
            let _admission = input_admission;
            input_store.http_input_budget(tenant)
        });
        tokio::time::timeout_at(body_deadline, admission)
            .await
            .map_err(|_| Error::new("DEADLINE_EXCEEDED", "HTTP input admission deadline"))?
            .map_err(|_| Error::new("INTERNAL", "HTTP input admission worker failed"))??
    };
    let (bytes, body_memory) = tokio::time::timeout(
        body_deadline.saturating_duration_since(tokio::time::Instant::now()),
        body::read(
            &input_budget,
            request.into_body(),
            if method == Method::GET {
                0
            } else {
                state.store.options().max_batch_bytes
            },
        ),
    )
    .await
    .map_err(|_| Error::new("DEADLINE_EXCEEDED", "request body deadline"))??;
    drop(receiving);
    let worker = workers
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::limit("HTTP worker concurrency"))?;
    let minimum = match (
        headers.get("x-tkv-min-history"),
        headers.get("x-tkv-min-seq"),
    ) {
        (None, None) => None,
        (Some(history), Some(seq)) => Some((
            history
                .to_str()
                .map_err(|_| Error::invalid("read history"))?
                .to_owned(),
            seq.to_str()
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| Error::invalid("read sequence"))?,
        )),
        _ => {
            return Err(Error::invalid(
                "both minimum history and sequence headers are required",
            ));
        }
    };
    if let (Some(budget), Some((_, seq))) = (&mut read_budget, &minimum) {
        budget.minimum_seq = Some(*seq);
    }
    let store = state.store.clone();
    let output = tokio::task::spawn_blocking(move || {
        let _trace_guard = trace_context.attach();
        let input_memory = input_budget.reserve(crate::json::allocation_bound(&bytes))?;
        let body = if bytes.is_empty() {
            json!({})
        } else {
            serde_json::from_slice(&bytes)?
        };
        drop(bytes);
        drop(body_memory);
        let _worker = worker;
        let _maintenance = maintenance_permit;
        let _lifecycle = lifecycle_permit;
        let _tenant = tenant_permit;
        let read_only = !route.ends_with("/retry-context")
            && required_role(&route, &method)? == Role::Read
            && (!route.ends_with("/dsl")
                || !crate::dsl::is_write(
                    body.get("statement").and_then(Value::as_str).unwrap_or(""),
                )?);
        if explicit_read_controls && !read_only {
            return Err(Error::invalid("read controls require a read operation"));
        }
        if read_only && let Some(budget) = &read_budget {
            budget.check()?;
        }
        if let Some((history, seq)) = &minimum {
            if !read_only {
                return Err(Error::invalid(
                    "read consistency headers require a read operation",
                ));
            }
            let handle =
                store.tenant(tenant.ok_or_else(|| Error::invalid("tenant read required"))?)?;
            if let Some(budget) = &read_budget {
                crate::TenantReader {
                    tenant: &handle,
                    budget: budget.clone(),
                }
                .wait_context(history, *seq)
                .map_err(|error| {
                    if !explicit_read_controls && error.code == "DEADLINE_EXCEEDED" {
                        Error::new("REPLICA_LAG", "read consistency deadline")
                    } else {
                        error
                    }
                })?;
            } else {
                handle.wait_context(history, *seq, 5000)?;
            }
        }
        let before = tenant.map(|id| store.tenant_history(id)).transpose()?;
        if let (Some((history, _)), Some(before)) = (&minimum, &before)
            && history != before
        {
            return Err(Error::new(
                "HISTORY_CONFLICT",
                "read history changed after waiting",
            ));
        }
        let value = if route.ends_with("/dsl") {
            let request: crate::dsl::Request = decode(body)?;
            if crate::dsl::is_write(&request.statement)? && !allows(&token.role, &Role::Write) {
                return Err(Error::new("FORBIDDEN", "DSL write requires write role"));
            }
            let tenant = store.tenant(tenant.ok_or_else(|| Error::invalid("tenant required"))?)?;
            let prepared = tenant.prepare_dsl(&request.statement)?;
            if prepared.is_write() && !allows(&token.role, &Role::Write) {
                return Err(Error::new("FORBIDDEN", "DSL write requires write role"));
            }
            let output = if prepared.is_write() {
                prepared.execute_with(
                    &tenant,
                    &request.params,
                    request.cursor.as_deref(),
                    &request.write_options,
                    &token.subject,
                )?
            } else {
                if request.write_options.operation_id.is_some()
                    || request.write_options.expected_history.is_some()
                {
                    return Err(Error::invalid("write_options require a write statement"));
                }
                let context = read_controls::OperationContext {
                    principal: &token.subject,
                    budget: read_budget.as_ref(),
                };
                prepared.execute_read(
                    &context.reader(&tenant)?,
                    &request.params,
                    request.cursor.as_deref(),
                )?
            };
            match &output {
                crate::dsl::Output::Query(result) => {
                    state.runtime.telemetry.record_read(&result.stats)
                }
                crate::dsl::Output::Lookup(result) => {
                    state.runtime.telemetry.record_read(&result.stats)
                }
                crate::dsl::Output::Resolve(result) => {
                    state.runtime.telemetry.record_read(&result.stats)
                }
                crate::dsl::Output::SubjectIds(result) => {
                    state.runtime.telemetry.record_read(&result.stats)
                }
                crate::dsl::Output::SubjectHistory(result) => {
                    state.runtime.telemetry.record_read(&result.stats)
                }
                crate::dsl::Output::ResolveMany(result) => {
                    state.runtime.telemetry.record_read(&result.stats)
                }
                _ => {}
            }
            serde_json::to_value(output)?
        } else if route.contains("/identity/") {
            let tenant = store.tenant(tenant.ok_or_else(|| Error::invalid("tenant required"))?)?;
            identity::dispatch(
                &tenant,
                &route,
                &params,
                body,
                &token,
                &state.runtime.telemetry,
                &read_controls::OperationContext {
                    principal: &token.subject,
                    budget: read_budget.as_ref(),
                },
            )?
        } else {
            operations::dispatch(
                &state,
                &route,
                &params,
                &method,
                tenant,
                body,
                &read_controls::OperationContext {
                    principal: &token.subject,
                    budget: read_budget.as_ref(),
                },
            )?
        };
        if read_only
            && let (Some(id), Some(history)) = (tenant, before.as_ref())
            && store.tenant_history(id)? != *history
        {
            return Err(Error::new(
                "HISTORY_CONFLICT",
                "read crossed a database history change; retry",
            ));
        }
        // JSON may be an arbitrary user document. History is manager-owned
        // metadata and must never be inferred from the response payload.
        let response_history = if read_only {
            before.clone()
        } else {
            tenant.and_then(|id| store.tenant_history(id).ok())
        };
        let encoded = body::encode(&store, tenant, value, read_only)?;
        drop(input_memory);
        let body = axum::body::Body::from(axum::body::Bytes::from_owner(encoded));
        let mut response = Response::builder()
            .status(if backups::is_job_post(&route, method.as_str()) {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            })
            .header("content-type", "application/json")
            .body(body)
            .unwrap();
        if read_only && (read_budget.is_some() || route.ends_with("/context")) {
            response
                .headers_mut()
                .insert("x-tkv-read-controls", "1".parse().unwrap());
        }
        if let Some(history) =
            response_history.and_then(|history| axum::http::HeaderValue::from_str(&history).ok())
        {
            response.headers_mut().insert("x-tkv-history", history);
        }
        Ok::<_, Error>(response)
    })
    .await
    .map_err(|_| Error::new("INTERNAL", "request worker failed"))??;
    Ok(output)
}
fn required_role(route: &str, method: &Method) -> Result<Role> {
    let methods = allowed_methods(route);
    if !methods.contains(&method.as_str()) {
        return Err(Error::new("METHOD_NOT_ALLOWED", "unsupported method"));
    }
    if backups::is_backup_route(route) || route.ends_with("/retention") {
        return Ok(Role::Admin);
    }
    if route.ends_with("/retry-context") {
        return Ok(Role::Read);
    }
    if route.ends_with("/identity/write") {
        return Ok(Role::IdentityWrite);
    }
    if *method == Method::GET
        || route.ends_with("/dsl")
        || route.ends_with("/query")
        || route.ends_with("/explain")
        || route.ends_with("/multi_get")
        || route.ends_with("/read_batch")
        || route.ends_with("/cursors/{cursor}")
        || route.ends_with("/resources/{resource}/read")
        || route.ends_with("/execute")
        || route.ends_with("/identity/resolve")
        || route.ends_with("/identity/resolve_many")
        || route.ends_with("/identity/ids")
        || route.ends_with("/identity/history")
        || route.ends_with("/identity/timeline")
        || route.ends_with("/replication/wait")
    {
        return Ok(Role::Read);
    }
    if route.ends_with("/documents")
        || route.ends_with("/documents/{key}")
        || route.ends_with("/batch")
    {
        return Ok(Role::Write);
    }
    Ok(Role::Admin)
}
fn allowed_methods(route: &str) -> &'static [&'static str] {
    if route == "/v1/tenants/{tenant}/backup/{backup}" {
        return &["DELETE"];
    }
    if route.ends_with("/identity/entities/{entity}")
        || route.ends_with("/identity/operations/{operation}")
    {
        return &["GET"];
    }
    if route.starts_with("/v1/replication/") {
        return if route.ends_with("/status") {
            &["GET"]
        } else {
            &["POST"]
        };
    }
    match route {
        "/health"
        | "/ready"
        | "/ready/write"
        | "/metrics"
        | "/v1/capabilities"
        | "/v1/tenants/{tenant}/context"
        | "/openapi.json"
        | "/v1/metrics"
        | "/v1/tenants/{tenant}/backup-jobs" => &["GET"],
        "/v1/tenants/{tenant}/remote-backups" => &["GET", "POST"],
        "/v1/tenants/{tenant}/resources/{resource}" | "/v1/tenants/{tenant}/retention" => {
            &["GET", "PUT"]
        }
        "/v1/tenants/{tenant}/backup-jobs/{job}" => &["GET", "DELETE"],
        "/v1/tenants" | "/v1/tenants/{tenant}/collections" => &["GET", "POST"],
        "/v1/tenants/{tenant}" => &["GET", "PATCH", "DELETE"],
        "/v1/tenants/{tenant}/collections/{collection}"
        | "/v1/tenants/{tenant}/collections/{collection}/lookups/{lookup}"
        | "/v1/tenants/{tenant}/cursors/{cursor}" => &["DELETE"],
        "/v1/tenants/{tenant}/collections/{collection}/documents" => &["PUT"],
        "/v1/tenants/{tenant}/collections/{collection}/documents/{key}"
        | "/v1/tenants/{tenant}/collections/{collection}/indexes/{index}" => &["GET", "DELETE"],
        "/v1/tenants/{tenant}/collections/{collection}/indexes"
        | "/v1/tenants/{tenant}/collections/{collection}/lookups" => &["GET", "POST"],
        _ => &["POST"],
    }
}
fn decode<T: serde::de::DeserializeOwned>(body: Value) -> Result<T> {
    Ok(serde_json::from_value(body)?)
}
fn value<T: Serialize>(value: T) -> Result<Value> {
    Ok(serde_json::to_value(value)?)
}

struct ResponseBytes {
    bytes: Vec<u8>,
    _memory: Vec<crate::resources::Reservation>,
}
impl AsRef<[u8]> for ResponseBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
