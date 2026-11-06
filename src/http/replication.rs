use super::*;
use axum::body::to_bytes;

pub(super) fn peer_route(route: &str) -> bool {
    matches!(
        route,
        "/v1/replication/status"
            | "/v1/replication/log"
            | "/v1/replication/snapshot"
            | "/v1/replication/snapshot/chunk"
            | "/v1/replication/snapshot/release"
            | "/v1/replication/promote"
    )
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotRequest {
    #[serde(default)]
    protocol: u32,
    cluster: Uuid,
    epoch: Uuid,
    tenant: Uuid,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChunkRequest {
    id: Uuid,
    file: usize,
    offset: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Id {
    id: Uuid,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FenceRequest {
    target: Uuid,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Promote {
    fence: Uuid,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Force {
    source: crate::replication::Source,
    old_primary_fenced: bool,
    accept_data_loss: bool,
}

pub(super) async fn dispatch(
    state: AppState,
    route: String,
    method: Method,
    request: Request,
) -> Result<Response> {
    if !allowed_methods(&route).contains(&method.as_str()) {
        return Err(Error::new("METHOD_NOT_ALLOWED", "replication method"));
    }
    let service = state
        .replication
        .clone()
        .ok_or_else(|| Error::new("UNAVAILABLE", "replication is not configured"))?;
    let permit = state
        .replication_workers
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::limit("replication HTTP concurrency"))?;
    let input_memory = state.store.reserve_http(2 << 20)?;
    let bytes = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        to_bytes(request.into_body(), 16 << 10),
    )
    .await
    .map_err(|_| Error::new("DEADLINE_EXCEEDED", "replication body timeout"))?
    .map_err(|_| Error::limit("replication request size"))?;
    let body = if bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&bytes)?
    };
    match route.as_str() {
        "/v1/replication/switchover" => {
            decode::<Empty>(body)?;
            response(&state.store, service.switchover().await?)
        }
        "/v1/replication/promote" => response(
            &state.store,
            service.promote(decode::<Promote>(body)?.fence).await?,
        ),
        "/v1/replication/follow" => {
            decode::<Empty>(body)?;
            response(&state.store, service.follow().await?)
        }
        "/v1/replication/force-promote" => {
            let b: Force = decode(body)?;
            response(
                &state.store,
                service
                    .force_promote(b.source, b.old_primary_fenced, b.accept_data_loss)
                    .await?,
            )
        }
        _ => tokio::task::spawn_blocking(move || -> Result<Response> {
            // Blocking work survives HTTP cancellation. Keep its concurrency and
            // input reservations until the worker, rather than the request, ends.
            let _permit = permit;
            let _input_memory = input_memory;
            match route.as_str() {
                "/v1/replication/status" => response(&state.store, service.status()?),
                "/v1/replication/log" => {
                    response(&state.store, service.export_log(&decode(body)?)?)
                }
                "/v1/replication/snapshot" => {
                    let b: SnapshotRequest = decode(body)?;
                    crate::replication::compatible(b.protocol)?;
                    response(
                        &state.store,
                        service.snapshot(b.cluster, b.epoch, b.tenant)?,
                    )
                }
                "/v1/replication/snapshot/chunk" => {
                    let b: ChunkRequest = decode(body)?;
                    response(
                        &state.store,
                        json!({"data":service.snapshot_chunk(b.id,b.file,b.offset)?}),
                    )
                }
                "/v1/replication/snapshot/release" => {
                    service.release_snapshot(decode::<Id>(body)?.id)?;
                    response(&state.store, json!({"ok":true}))
                }
                "/v1/replication/fence" => response(
                    &state.store,
                    service.fence(decode::<FenceRequest>(body)?.target)?,
                ),
                _ => Err(Error::missing("replication route")),
            }
        })
        .await
        .map_err(|_| Error::new("INTERNAL", "replication HTTP worker failed"))?,
    }
}

fn response<T: serde::Serialize>(store: &Store, result: T) -> Result<Response> {
    let size = crate::json::encoded_len(&result)?;
    if size > crate::replication::MAX_WIRE_BYTES {
        return Err(Error::limit("replication response size"));
    }
    let mut memory = store.reserve_http(size.saturating_mul(3) + 4096)?;
    let mut bytes = Vec::with_capacity(size);
    serde_json::to_writer(&mut bytes, &result)?;
    drop(result);
    memory.shrink(bytes.capacity());
    let body = axum::body::Body::from(axum::body::Bytes::from_owner(ResponseBytes {
        bytes,
        _memory: vec![memory],
    }));
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(body)
        .unwrap())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

pub(super) fn schemas() -> serde_json::Map<String, Value> {
    let uuid = json!({"type":"string","format":"uuid"});
    let seq = json!({"type":"integer","minimum":0});
    let reference = |name: &str| json!({"$ref":format!("#/components/schemas/{name}")});
    let object = |properties: Value, required: &[&str]| json!({"type":"object","additionalProperties":false,"properties":properties,"required":required});
    let mut out = serde_json::Map::new();
    out.insert(
        "ReplicationSource".into(),
        object(json!({"node":uuid,"epoch":uuid}), &["node", "epoch"]),
    );
    out.insert(
        "ReplicationPosition".into(),
        object(
            json!({"info":reference("Tenant"),"seq":seq}),
            &["info", "seq"],
        ),
    );
    out.insert(
        "ReplicationCursor".into(),
        object(json!({"file":uuid,"offset":seq}), &["file", "offset"]),
    );
    let cursor = json!({"anyOf":[reference("ReplicationCursor"),{"type":"null"}]});
    out.insert("ReplicationFence".into(),object(json!({"id":uuid,"cluster":uuid,"source":reference("ReplicationSource"),"target":uuid,"tenants":{"type":"array","items":reference("ReplicationPosition")}}),&["id","cluster","source","target","tenants"]));
    let nullable = |name: &str| json!({"anyOf":[reference(name),{"type":"null"}]});
    out.insert(
        "ReplicationNode".into(),
        object(
            json!({
                "version":{"type":"integer","const":1},"node":uuid,"cluster":uuid,"epoch":uuid,
                "role":{"type":"string","enum":["primary","replica","fenced"]},
                "enabled":{"type":"boolean"},"forced":{"type":"boolean"},
                "source":nullable("ReplicationSource"),"fence":nullable("ReplicationFence"),
                "parent":nullable("ReplicationFence"),"log_retention_bytes":seq
            }),
            &[
                "version",
                "node",
                "cluster",
                "epoch",
                "role",
                "enabled",
                "forced",
                "source",
                "fence",
                "parent",
                "log_retention_bytes",
            ],
        ),
    );
    out.insert(
        "ReplicationUnavailableTenant".into(),
        object(
            json!({"info":reference("Tenant"),"error":{"type":"string"}}),
            &["info", "error"],
        ),
    );
    out.insert("ReplicationStatus".into(),object(json!({"node":reference("ReplicationNode"),"tenants":{"type":"array","items":reference("ReplicationPosition")},"unavailable":{"type":"array","items":reference("ReplicationUnavailableTenant")},"last_error":{"type":["string","null"]},"completed_cycles":seq,"initialization_attempts":seq,"initialization_failures":seq,"reinitializations":seq,"initializing":{"type":"array","items":uuid},"waiting_for_readers":{"type":"array","items":uuid}}),&["node","tenants","unavailable","last_error","completed_cycles","initialization_attempts","initialization_failures","reinitializations","initializing","waiting_for_readers"]));
    out.insert("ReplicationLogRequest".into(),object(json!({"cluster":uuid,"epoch":uuid,"tenant":uuid,"generation":uuid,"after":seq,"cursor":cursor}),&["cluster","epoch","tenant","generation","after"]));
    out.insert("ReplicationLogPage".into(),object(json!({"epoch":uuid,"through":seq,"source_position":seq,"frames":{"type":"array","maxItems":256,"items":{"type":"string","contentEncoding":"base64"}},"cursor":cursor}),&["epoch","through","source_position","frames","cursor"]));
    out.insert(
        "ReplicationSnapshotRequest".into(),
        object(
            json!({"cluster":uuid,"epoch":uuid,"tenant":uuid}),
            &["cluster", "epoch", "tenant"],
        ),
    );
    out.insert("ReplicationSnapshot".into(),object(json!({"id":uuid,"cluster":uuid,"epoch":uuid,"tenant":reference("Tenant"),"position":seq,"files":{"type":"array","maxItems":20000,"items":object(json!({"path":{"type":"string"},"size":seq,"sha256":{"type":"string"}}),&["path","size","sha256"])}}),&["id","cluster","epoch","tenant","position","files"]));
    out.insert(
        "ReplicationChunkRequest".into(),
        object(
            json!({"id":uuid,"file":seq,"offset":seq}),
            &["id", "file", "offset"],
        ),
    );
    out.insert("ReplicationId".into(), object(json!({"id":uuid}), &["id"]));
    out.insert(
        "ReplicationFenceRequest".into(),
        object(json!({"target":uuid}), &["target"]),
    );
    out.insert(
        "ReplicationPromote".into(),
        object(json!({"fence":uuid}), &["fence"]),
    );
    out.insert("ReplicationForce".into(),object(json!({"source":reference("ReplicationSource"),"old_primary_fenced":{"type":"boolean","const":true},"accept_data_loss":{"type":"boolean","const":true}}),&["source","old_primary_fenced","accept_data_loss"]));
    out.insert("ReplicationEmpty".into(), object(json!({}), &[]));
    out.insert("ReplicationWait".into(),object(json!({"epoch":uuid,"seq":seq,"timeout_ms":{"type":"integer","minimum":0,"maximum":5000}}),&["epoch","seq","timeout_ms"]));
    out
}
