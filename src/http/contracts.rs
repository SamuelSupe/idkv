use serde_json::{Value, json};
fn reference(name: &str) -> Value {
    json!({"$ref":format!("#/components/schemas/{name}")})
}
fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}

pub(super) fn response_schema(route: &str) -> Option<Value> {
    Some(match route {
        "/ready" | "/ready/write" => reference("Readiness"),
        "/v1/capabilities" => reference("Capabilities"),
        "/v1/tenants/{tenant}/context" => reference("ReadContext"),
        "/v1/tenants/{tenant}/read_batch" => reference("ReadBatchResult"),
        "/v1/tenants/{tenant}/resources/{resource}/read" => reference("PublishedReadBatch"),
        "/v1/tenants/{tenant}/resources/{resource}/query" => reference("PublishedQuery"),
        "/v1/tenants/{tenant}/resources/{resource}" => reference("PublishedLookup"),
        "/v1/tenants/{tenant}/resources/{resource}/execute" => reference("PublishedResult"),
        _ => return None,
    })
}

pub(super) fn extend(api: &mut Value) {
    let uuid = json!({"type":"string","format":"uuid"});
    let text = json!({"type":"string"});
    let seq = json!({"type":"integer","minimum":0});
    let schemas = &mut api["components"]["schemas"];
    schemas["WriteOp"]["oneOf"]
        .as_array_mut()
        .unwrap()
        .push(object(
            json!({"op":{"const":"check"},"key":text,"if_rev":seq}),
            &["op", "key", "if_rev"],
        ));
    schemas["ReadRequest"] = json!({"oneOf":[
        object(json!({"op":{"const":"get"},"key":text}), &["op","key"]),
        object(json!({"op":{"const":"multi_get"},"keys":{"type":"array","items":text}}), &["op","keys"]),
        object(json!({"op":{"const":"query"},"query":reference("Query")}), &["op","query"])
    ],"description":"Batch queries require an absent cursor. Returned cursors retain this read snapshot."});
    schemas["ReadOperation"] = object(
        json!({"collection":text,"request":reference("ReadRequest")}),
        &["collection", "request"],
    );
    schemas["ReadBatch"] = object(
        json!({"operations":{"type":"array","minItems":1,"items":reference("ReadOperation")}}),
        &["operations"],
    );
    schemas["ReadResult"] = json!({"oneOf":[
        object(json!({"op":{"const":"get"},"document":{"type":["object","null"]}}), &["op","document"]),
        object(json!({"op":{"const":"multi_get"},"documents":{"type":"array","items":{"type":["object","null"]}}}), &["op","documents"]),
        object(json!({"op":{"const":"query"},"result":reference("QueryResult")}), &["op","result"])
    ]});
    schemas["ReadBatchResult"] = object(
        json!({"results":{"type":"array","items":reference("ReadResult")},"history":text,"snapshot_seq":seq,"stats":reference("ReadStats")}),
        &["results", "history", "snapshot_seq", "stats"],
    );
    schemas["PublishedReads"] = object(
        json!({"version":{"oneOf":[uuid,{"type":"null"}]},"operations":{"type":"array","minItems":1,"items":reference("ReadRequest")}}),
        &["operations"],
    );
    schemas["PublishedReadBatch"] = object(
        json!({"version":uuid,"result":reference("ReadBatchResult")}),
        &["version", "result"],
    );
    schemas["PublishedQueryRequest"] = object(
        json!({"version":{"oneOf":[uuid,{"type":"null"}]},"query":reference("Query")}),
        &["query"],
    );
    schemas["PublishedQuery"] = object(
        json!({"version":uuid,"history":text,"result":reference("QueryResult")}),
        &["version", "history", "result"],
    );
    schemas["WriteOptions"] = object(
        json!({"expected_history":{"type":["string","null"],"description":"History captured with the read; required for positive if_rev and operation_id."},"operation_id":{"type":["string","null"],"minLength":1,"maxLength":128,"description":"Stable within principal and collection lifetime. Retry identical payload. Receipt obeys the configured tenant retention."}}),
        &[],
    );
    schemas["WriteOptions"]["properties"]["retry_token"] = json!({"oneOf":[uuid,{"type":"null"}],"description":"Persist the issued token with the original request; never refresh it to retry."});
    schemas["WriteOptions"]["properties"]["operation_id"]["description"] = json!(
        "Retry the identical request and token. Receipts expire after the tenant duration; expired requests must not be resubmitted with new tokens."
    );
    schemas["RetentionConfig"] = object(
        json!({"retention_seconds":{"type":"integer","minimum":1,"maximum":315360000}}),
        &["retention_seconds"],
    );
    schemas["RetentionPolicy"] = object(
        json!({"retention_seconds":seq,"activated_seq":seq,"legacy_expires_at_ms":seq}),
        &["retention_seconds", "activated_seq", "legacy_expires_at_ms"],
    );
    schemas["RetentionStatus"] = object(
        json!({"policy":{"oneOf":[reference("RetentionPolicy"),{"type":"null"}]},"gc":{"type":["object","null"],"additionalProperties":true},"physical_reclamation":text,"next_expiration_ms":{"type":["integer","null"],"minimum":0},"expiration_lag_ms":seq}),
        &[
            "policy",
            "gc",
            "physical_reclamation",
            "next_expiration_ms",
            "expiration_lag_ms",
        ],
    );
    schemas["RetryContext"] = object(
        json!({"retry_token":uuid,"history":text,"submit_before_ms":seq}),
        &["retry_token", "history", "submit_before_ms"],
    );
    for alternative in schemas["IdentityWrite"]["oneOf"].as_array_mut().unwrap() {
        alternative["properties"]["write_options"] = reference("WriteOptions");
    }
    for name in ["Put", "ConditionalDelete", "Batch", "DslRequest"] {
        schemas[name]["properties"]["write_options"] = reference("WriteOptions");
    }
    schemas["WriteResult"]["properties"]["history"] = text.clone();
    schemas["WriteResult"]["required"]
        .as_array_mut()
        .unwrap()
        .push(json!("history"));
    schemas["Tenant"]["properties"]["history"] = uuid.clone();
    schemas["Tenant"]["required"]
        .as_array_mut()
        .unwrap()
        .push(json!("history"));
    schemas["ReadContext"] = object(
        json!({"tenant":uuid,"history":text,"applied_seq":seq,"writable":{"type":"boolean"}}),
        &["tenant", "history", "applied_seq", "writable"],
    );
    schemas["Readiness"] = object(
        json!({"ready":{"type":"boolean"},"draining":{"type":"boolean"},"role":{"enum":["primary","replica","fenced"]}}),
        &["ready", "draining", "role"],
    );
    schemas["Capabilities"] = object(
        json!({"version":text,"replication_protocol":{"const":3},"storage_registry":{"const":crate::tenant::STORAGE_REGISTRY_VERSION},"features":{"type":"array","items":text}}),
        &[
            "version",
            "replication_protocol",
            "storage_registry",
            "features",
        ],
    );
    schemas["Publication"] = object(
        json!({"version":uuid,"collection":text,"lookup":text,"expected_version":{"oneOf":[uuid,{"type":"null"}]}}),
        &["version", "collection"],
    );
    schemas["Publication"]["properties"]["retry_token"] = json!({"oneOf":[uuid,{"type":"null"}]});
    schemas["Publication"]["properties"]["lookup"]["description"] = json!(
        "Optional default Lookup. Omit or use an empty string for a general immutable dataset."
    );
    schemas["Publication"]["properties"]["version"]["description"] = json!(
        "Client publication request ID, not the resulting version. Use response.version to execute or replace."
    );
    schemas["PublishedLookup"] = object(
        json!({"version":uuid,"name":text,"publication":reference("Publication"),"collection_id":uuid,"commit_seq":seq}),
        &[
            "version",
            "name",
            "publication",
            "collection_id",
            "commit_seq",
        ],
    );
    schemas["PublishedInput"] = object(
        json!({"version":{"oneOf":[uuid,{"type":"null"}]},"inputs":{"type":"array","maxItems":1000,"items":{"type":"object"}}}),
        &["inputs"],
    );
    schemas["PublishedResult"] = object(
        json!({"version":uuid,"result":reference("LookupResult")}),
        &["version", "result"],
    );
    for name in [
        "ReplicationNode",
        "ReplicationLogRequest",
        "ReplicationSnapshotRequest",
    ] {
        schemas[name]["properties"]["protocol"] = json!({"const":3,"type":"integer"});
        schemas[name]["required"]
            .as_array_mut()
            .unwrap()
            .push(json!("protocol"));
    }
    for (path, schema) in [
        ("/v1/tenants/{tenant}/retention", "RetentionStatus"),
        ("/v1/tenants/{tenant}/retry-context", "RetryContext"),
    ] {
        for op in api["paths"][path].as_object_mut().unwrap().values_mut() {
            op["responses"]["200"]["content"]["application/json"]["schema"] = reference(schema);
        }
    }
    api["paths"]["/metrics"]["get"]["responses"]["200"]["content"] =
        json!({"text/plain":{"schema":{"type":"string"}}});
    for path in ["/ready", "/ready/write"] {
        api["paths"][path]["get"]["responses"]["503"] = json!({"description":"Not ready or draining","content":{"application/json":{"schema":reference("Readiness")}}});
    }
    for (path, operations) in api["paths"].as_object_mut().unwrap() {
        if path.starts_with("/v1/tenants/{tenant}/") {
            for operation in operations.as_object_mut().unwrap().values_mut() {
                for (name, schema) in [
                    ("X-TKV-Min-History", text.clone()),
                    ("X-TKV-Min-Seq", seq.clone()),
                ] {
                    operation["parameters"].as_array_mut().unwrap().push(json!({"name":name,"in":"header","required":false,"schema":schema,"description":"Read-only consistency. Supply both headers. Wait at most five seconds; controlled KV reads share their request deadline with this wait. History mismatch is an error."}));
                }
            }
        }
    }
}
