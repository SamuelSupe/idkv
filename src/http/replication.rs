use super::*;
use crate::replication::{
    MAX_LOG_PAGE_FRAMES, MAX_WIRE_BYTES,
    wire::{LOG_MEDIA_TYPE, LogExport, RawFrame, log_header},
};
use axum::{
    body::{Body, Bytes, to_bytes},
    http::header::{ACCEPT, CONTENT_TYPE, VARY},
};
use hyper::body::{Frame, SizeHint};
use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};

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
    let binary_log = route == "/v1/replication/log" && prefers_binary_log(request.headers());
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
                    let export = service.export_log_page(&decode(body)?, binary_log)?;
                    let mut response = if binary_log {
                        binary_log_response(export)?
                    } else {
                        response(&state.store, export.page)?
                    };
                    response
                        .headers_mut()
                        .insert(VARY, "Accept".parse().unwrap());
                    Ok(response)
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

fn prefers_binary_log(headers: &HeaderMap) -> bool {
    let mut binary_quality = 0;
    let mut json_preference = (0, 0);
    for header in headers.get_all(ACCEPT) {
        let Ok(value) = header.to_str() else { continue };
        for range in split_unquoted(value, ',') {
            let (media, parameters) = range.split_once(';').unwrap_or((range, ""));
            let media = media.trim();
            let binary = media.eq_ignore_ascii_case(LOG_MEDIA_TYPE);
            let specificity = if media.eq_ignore_ascii_case("application/json") {
                3
            } else if media.eq_ignore_ascii_case("application/*") {
                2
            } else if media == "*/*" {
                1
            } else if binary {
                0
            } else {
                continue;
            };
            let Some(quality) = accept_quality(parameters) else {
                continue;
            };
            if binary {
                binary_quality = binary_quality.max(quality);
            } else {
                json_preference = json_preference.max((specificity, quality));
            }
        }
    }
    // Wildcards alone keep the legacy JSON response.
    binary_quality > 0 && binary_quality >= json_preference.1
}

fn split_unquoted(value: &str, delimiter: char) -> impl Iterator<Item = &str> {
    let mut quoted = false;
    let mut escaped = false;
    value.split(move |ch| {
        if escaped {
            escaped = false;
            return false;
        }
        if quoted && ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            quoted = !quoted;
        }
        ch == delimiter && !quoted
    })
}

fn accept_quality(parameters: &str) -> Option<u16> {
    let mut weight = None;
    for parameter in split_unquoted(parameters, ';') {
        let (name, value) = parameter.split_once('=').unwrap_or((parameter, ""));
        if name.trim().eq_ignore_ascii_case("q") && weight.replace(value.trim()).is_some() {
            return None;
        }
    }
    let Some(value) = weight else {
        return Some(1000);
    };
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if fraction.len() > 3 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    match whole {
        "1" if fraction.bytes().all(|b| b == b'0') => Some(1000),
        "0" => Some(
            fraction
                .bytes()
                .zip([100, 10, 1])
                .map(|(b, place)| u16::from(b - b'0') * place)
                .sum(),
        ),
        _ => None,
    }
}

fn binary_log_response(mut export: LogExport) -> Result<Response> {
    let header = log_header(&export.page);
    let remaining = header.len()
        + export
            .raw
            .iter()
            .map(|frame| frame.bytes.len())
            .sum::<usize>();
    if remaining > MAX_WIRE_BYTES {
        return Err(Error::limit("replication response size"));
    }
    // The tenant's work is complete. Keep the existing global reservation
    // with the allocation through socket backpressure, without reserving twice.
    for frame in &mut export.raw {
        frame.memory.release_tenant();
    }
    let body = BinaryLogBody {
        header: Some(Bytes::copy_from_slice(&header)),
        frames: export.raw.into_iter(),
        remaining,
    };
    Ok(Response::builder()
        .header(CONTENT_TYPE, LOG_MEDIA_TYPE)
        .body(Body::new(body))
        .unwrap())
}

// Each WAL frame owns its memory reservation through the HTTP send.
struct BinaryLogBody {
    header: Option<Bytes>,
    frames: std::vec::IntoIter<RawFrame>,
    remaining: usize,
}

impl hyper::body::Body for BinaryLogBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(
            self.header
                .take()
                .or_else(|| self.frames.next().map(Bytes::from_owner))
                .map(|bytes| {
                    self.remaining -= bytes.len();
                    Ok(Frame::data(bytes))
                }),
        )
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining as u64)
    }
}

fn response<T: serde::Serialize>(store: &Store, result: T) -> Result<Response> {
    let size = crate::json::encoded_len(&result)?;
    if size > MAX_WIRE_BYTES {
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
    out.insert("ReplicationStatus".into(),object(json!({"node":reference("ReplicationNode"),"tenants":{"type":"array","items":reference("ReplicationPosition")},"unavailable":{"type":"array","items":reference("ReplicationUnavailableTenant")},"last_error":{"type":["string","null"]},"completed_cycles":seq,"initialization_attempts":seq,"initialization_failures":seq,"reinitializations":seq,"initializing":{"type":"array","items":uuid},"waiting_for_readers":{"type":"array","items":uuid},"initialization_errors":{"type":"object","additionalProperties":{"type":"string"}}}),&["node","tenants","unavailable","last_error","completed_cycles","initialization_attempts","initialization_failures","reinitializations","initializing","waiting_for_readers","initialization_errors"]));
    out.insert("ReplicationLogRequest".into(),object(json!({"cluster":uuid,"epoch":uuid,"tenant":uuid,"generation":uuid,"after":seq,"cursor":cursor}),&["cluster","epoch","tenant","generation","after"]));
    out.insert("ReplicationLogPage".into(),object(json!({"epoch":uuid,"through":seq,"source_position":seq,"frames":{"type":"array","maxItems":MAX_LOG_PAGE_FRAMES,"items":{"type":"string","contentEncoding":"base64"}},"cursor":cursor}),&["epoch","through","source_position","frames","cursor"]));
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
