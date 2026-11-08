//! Synchronous JSON operations after the HTTP adapter authenticates and budgets input.
use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NameBody {
    name: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PutBody {
    document: Value,
    #[serde(default)]
    if_rev: Option<u64>,
    #[serde(default)]
    write_options: WriteOptions,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteBody {
    #[serde(default)]
    if_rev: Option<u64>,
    #[serde(default)]
    write_options: WriteOptions,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchBody {
    operations: Vec<WriteOp>,
    #[serde(default)]
    write_options: WriteOptions,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeysBody {
    keys: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadBatchBody {
    operations: Vec<ReadOperation>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishedReads {
    version: Option<Uuid>,
    operations: Vec<ReadRequest>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishedQueryBody {
    version: Option<Uuid>,
    query: Query,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputsBody {
    inputs: Vec<Value>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishedInputs {
    inputs: Vec<Value>,
    version: Option<Uuid>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetentionBody {
    retention_seconds: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QuotaBody {
    memory_bytes: usize,
    disk_bytes: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreBody {
    backup: Uuid,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicationWait {
    epoch: Uuid,
    seq: u64,
    timeout_ms: u64,
}
pub(super) fn dispatch(
    state: &AppState,
    route: &str,
    params: &HashMap<String, String>,
    method: &Method,
    tenant_id: Option<Uuid>,
    body: Value,
    context: &read_controls::OperationContext<'_>,
) -> Result<Value> {
    let principal = context.principal;
    let store = &state.store;
    let backup_service = state.backups.as_ref();
    let ok = || Ok(json!({"ok":true}));
    if route == "/v1/capabilities" {
        return Ok(crate::replication::capabilities());
    }
    if route == "/v1/tenants" {
        return if *method == Method::GET {
            value(store.tenants())
        } else {
            value(store.create_tenant(&decode::<NameBody>(body)?.name)?)
        };
    }
    if route == "/v1/metrics" {
        return Ok(
            json!({"memory_bytes":store.memory_bytes(),"memory_pressure":store.memory_pressure(),"tenants":store.tenants().iter().map(|t|store.status(t.id)).collect::<Result<Vec<_>>>()?}),
        );
    }
    let id = tenant_id.ok_or_else(|| Error::invalid("tenant required"))?;
    if backups::is_backup_route(route) {
        return backups::dispatch(backup_service, route, params, method, id, body);
    }
    match route {
        "/v1/tenants/{tenant}" => {
            return match *method {
                Method::GET => store.status(id),
                Method::DELETE => {
                    store.delete_tenant(id)?;
                    ok()
                }
                _ => {
                    let b: QuotaBody = decode(body)?;
                    value(store.set_quota(id, b.memory_bytes, b.disk_bytes)?)
                }
            };
        }
        "/v1/tenants/{tenant}/close" => {
            store.close_tenant(id)?;
            return ok();
        }
        "/v1/tenants/{tenant}/backup" => return Ok(json!({"backup":store.backup(id)?})),
        "/v1/tenants/{tenant}/backup/{backup}" => {
            store.delete_backup(
                id,
                Uuid::parse_str(&params["backup"]).map_err(|_| Error::invalid("backup UUID"))?,
            )?;
            return ok();
        }
        "/v1/tenants/{tenant}/restore" => {
            store.restore(id, decode::<RestoreBody>(body)?.backup)?;
            return ok();
        }
        _ => {}
    }
    let tenant = store.tenant(id)?;
    if route.ends_with("/retention") {
        if *method == Method::PUT {
            tenant.set_retention(decode::<RetentionBody>(body)?.retention_seconds)?;
        }
        return tenant.retention_status();
    }
    if route.ends_with("/retry-context") {
        return value(tenant.retry_context(principal)?);
    }
    if route.ends_with("/context") {
        return value(tenant.read_context());
    }
    if route.ends_with("/read_batch") {
        let result = context
            .reader(&tenant)?
            .read_batch(&decode::<ReadBatchBody>(body)?.operations)?;
        state.runtime.telemetry.record_read(&result.stats);
        return value(result);
    }
    if route.ends_with("/resources/{resource}") {
        return if *method == Method::GET {
            value(
                context
                    .reader(&tenant)?
                    .published_dataset(&params["resource"], None)?,
            )
        } else {
            value(tenant.publish_dataset_with(&params["resource"], decode(body)?, principal)?)
        };
    }
    if route.ends_with("/resources/{resource}/read") {
        let b: PublishedReads = decode(body)?;
        let result = context.reader(&tenant)?.read_published(
            &params["resource"],
            b.version,
            &b.operations,
        )?;
        state.runtime.telemetry.record_read(&result.result.stats);
        return value(result);
    }
    if route.ends_with("/resources/{resource}/query") {
        let b: PublishedQueryBody = decode(body)?;
        let result =
            context
                .reader(&tenant)?
                .query_published(&params["resource"], b.version, b.query)?;
        state.runtime.telemetry.record_read(&result.result.stats);
        return value(result);
    }
    if route.ends_with("/resources/{resource}/execute") {
        let b: PublishedInputs = decode(body)?;
        let result =
            context
                .reader(&tenant)?
                .lookup_published(&params["resource"], b.version, &b.inputs)?;
        state.runtime.telemetry.record_read(&result.result.stats);
        return value(result);
    }
    match route {
        "/v1/tenants/{tenant}/replication/wait" => {
            let b: ReplicationWait = decode(body)?;
            return Ok(json!({"applied_seq":tenant.wait_replication(b.epoch,b.seq,b.timeout_ms)?}));
        }
        "/v1/tenants/{tenant}/check" => {
            tenant.engine().verify()?;
            return ok();
        }
        "/v1/tenants/{tenant}/checkpoint" => {
            tenant.engine().flush()?;
            return ok();
        }
        "/v1/tenants/{tenant}/compact" => {
            tenant.engine().flush()?;
            tenant.engine().compact(true)?;
            return ok();
        }
        "/v1/tenants/{tenant}/cursors/{cursor}" => {
            let token =
                Uuid::parse_str(&params["cursor"]).map_err(|_| Error::invalid("cursor UUID"))?;
            tenant.core.cursors.lock().remove(&token);
            return ok();
        }
        "/v1/tenants/{tenant}/collections" => {
            return if *method == Method::GET {
                value(tenant.collections()?)
            } else {
                value(tenant.create_collection(&decode::<NameBody>(body)?.name)?)
            };
        }
        _ => {}
    }
    let collection = params
        .get("collection")
        .ok_or_else(|| Error::invalid("collection required"))?;
    if route.ends_with("/collections/{collection}") {
        tenant.drop_collection(collection)?;
        return ok();
    }
    if route.ends_with("/documents") {
        let b: PutBody = decode(body)?;
        return value(
            tenant
                .write_batch_with(
                    collection,
                    vec![WriteOp::Put {
                        document: b.document,
                        if_rev: b.if_rev,
                    }],
                    &b.write_options,
                    principal,
                )?
                .remove(0),
        );
    }
    if route.ends_with("/documents/{key}") {
        return if *method == Method::GET {
            value(
                context
                    .reader(&tenant)?
                    .get(collection, &params["key"])?
                    .ok_or_else(|| Error::missing("document"))?,
            )
        } else {
            let b: DeleteBody = decode(body)?;
            value(
                tenant
                    .write_batch_with(
                        collection,
                        vec![WriteOp::Delete {
                            key: params["key"].clone(),
                            if_rev: b.if_rev,
                        }],
                        &b.write_options,
                        principal,
                    )?
                    .remove(0),
            )
        };
    }
    if route.ends_with("/batch") {
        let b: BatchBody = decode(body)?;
        return value(tenant.write_batch_with(
            collection,
            b.operations,
            &b.write_options,
            principal,
        )?);
    }
    if route.ends_with("/multi_get") {
        return value(
            context
                .reader(&tenant)?
                .multi_get(collection, &decode::<KeysBody>(body)?.keys)?,
        );
    }
    if route.ends_with("/indexes") {
        return if *method == Method::GET {
            value(tenant.indexes(collection)?)
        } else {
            value(tenant.create_index(collection, decode(body)?)?)
        };
    }
    if route.ends_with("/indexes/{index}") {
        return if *method == Method::GET {
            value(
                tenant
                    .indexes(collection)?
                    .into_iter()
                    .find(|i| i.spec.name == params["index"])
                    .ok_or_else(|| Error::missing("index"))?,
            )
        } else {
            tenant.drop_index(collection, &params["index"])?;
            ok()
        };
    }
    if route.ends_with("/lookups") {
        return if *method == Method::GET {
            value(tenant.lookups(collection)?)
        } else {
            tenant.create_lookup(collection, decode(body)?)?;
            ok()
        };
    }
    if route.ends_with("/lookups/{lookup}") {
        tenant.drop_lookup(collection, &params["lookup"])?;
        return ok();
    }
    if route.ends_with("/execute") {
        let result = context.reader(&tenant)?.lookup_many(
            collection,
            &params["lookup"],
            &decode::<InputsBody>(body)?.inputs,
        )?;
        state.runtime.telemetry.record_read(&result.stats);
        return value(result);
    }
    if route.ends_with("/query") {
        let result = context.reader(&tenant)?.query(collection, decode(body)?)?;
        state.runtime.telemetry.record_read(&result.stats);
        return value(result);
    }
    if route.ends_with("/explain") {
        return value(
            context
                .reader(&tenant)?
                .explain(collection, &decode(body)?)?,
        );
    }
    Err(Error::missing("route"))
}
