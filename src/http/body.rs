use crate::{Error, Result, Store, resources::Reservation};
use axum::body::Body;
use http_body_util::BodyExt;
use uuid::Uuid;

/// Charge each owned allocation before copying network frames. Content-Length
/// is deliberately not trusted and chunked requests use the same bounded path.
pub(super) async fn read(
    budget: &crate::resources::Budget,
    mut body: Body,
    limit: usize,
) -> Result<(Vec<u8>, Reservation)> {
    let mut bytes = Vec::new();
    let mut charge = budget.reserve(0)?;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| Error::invalid("invalid request body"))?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        let size = bytes
            .len()
            .checked_add(data.len())
            .filter(|size| *size <= limit)
            .ok_or_else(|| Error::limit("request body exceeds limit"))?;
        if size > bytes.capacity() {
            let capacity = size.max(bytes.capacity().saturating_mul(2)).min(limit);
            let next_charge = budget.reserve(capacity)?;
            let mut next = Vec::with_capacity(capacity);
            next.extend_from_slice(&bytes);
            bytes = next;
            charge = next_charge;
        }
        bytes.extend_from_slice(&data);
    }
    Ok((bytes, charge))
}

/// `value` exists only after the operation succeeded. A failure to encode or
/// budget that response must not claim that a mutation was rejected.
pub(super) fn encode(
    store: &Store,
    tenant: Option<Uuid>,
    value: serde_json::Value,
    read_only: bool,
) -> Result<super::ResponseBytes> {
    let response_error = |error: Error| {
        if read_only {
            error
        } else {
            Error::new(
                "RESPONSE_UNAVAILABLE",
                format!(
                    "operation completed but response could not be delivered ({}); retry only with the original operation ID",
                    error.code
                ),
            )
        }
    };
    let len = crate::json::encoded_len(&value).map_err(response_error)?;
    if len > store.options().max_response_bytes {
        return Err(response_error(Error::limit("HTTP response byte limit")));
    }
    let mut memory = store
        .reserve_http_input(
            tenant.filter(|id| store.contains_tenant(*id)),
            len.saturating_mul(8).saturating_add(4096),
        )
        .map_err(response_error)?;
    let mut bytes = Vec::with_capacity(len);
    serde_json::to_writer(&mut bytes, &value)
        .map_err(Error::from)
        .map_err(response_error)?;
    drop(value);
    memory.shrink(bytes.capacity());
    Ok(super::ResponseBytes {
        bytes,
        _memory: vec![memory],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    #[test]
    fn response_budget_failure_preserves_completed_mutation_status() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), crate::Options::default()).unwrap();
        let committed = store.create_tenant("completed").unwrap();
        let response = serde_json::to_value(&committed).unwrap();
        let _pressure = store
            .reserve_http(store.options().global_memory_bytes - store.memory_bytes())
            .unwrap();
        let write_error = encode(&store, None, response.clone(), false).err().unwrap();
        assert_eq!(write_error.code, "RESPONSE_UNAVAILABLE");
        assert_eq!(
            write_error.into_response().status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(store.contains_tenant(committed.id));
        let read_error = encode(&store, None, response, true).err().unwrap();
        assert_eq!(read_error.code, "RESOURCE_EXHAUSTED");
        assert_eq!(
            read_error.into_response().status(),
            axum::http::StatusCode::TOO_MANY_REQUESTS
        );
    }
}
