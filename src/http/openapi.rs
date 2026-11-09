use super::*;
pub fn openapi() -> Value {
    openapi_for(&Options::default())
}
pub(super) fn openapi_for(options: &Options) -> Value {
    let mut paths = serde_json::Map::new();
    for route in ROUTES {
        let mut operations = serde_json::Map::new();
        for method in allowed_methods(route) {
            let params: Vec<_> = route.split('/').filter(|s| s.starts_with('{')).map(|s| json!({"name":s.trim_matches(['{','}']),"in":"path","required":true,"schema":{"type":"string"}})).collect();
            let mut op = json!({"parameters":params,"responses":{"200":{"description":"Success","content":{"application/json":{"schema":response_schema(route,method)}}}}});
            if read_controls::supports(route, &Method::from_bytes(method.as_bytes()).unwrap()) {
                let params = op["parameters"].as_array_mut().unwrap();
                for (name, schema, description) in [
                    (
                        "X-TKV-Read-Timeout-Ms",
                        json!({"type":"integer","minimum":1}),
                        "Shared deadline for body receipt, consistency waiting and read execution. Clamped to query_ms. DSL writes reject read controls.",
                    ),
                    (
                        "X-TKV-Max-Scan",
                        json!({"type":"integer","minimum":1}),
                        "Shared scan entry limit, clamped to max_scan. Historical versions and tombstones count.",
                    ),
                    (
                        "X-TKV-Allow-Full-Scan",
                        json!({"type":"boolean","default":true}),
                        "False rejects plans with a collection scan. Explain still returns the plan.",
                    ),
                ] {
                    params.push(json!({"name":name,"in":"header","required":false,"schema":schema,"description":description}));
                }
                op["responses"]["200"]["headers"]["X-TKV-Read-Controls"] = json!({"schema":{"type":"string","const":"1"},"description":"Server enforces read controls v1 on read-only execution."});
            }
            if route.starts_with("/v1/tenants/") && !backups::is_backup_route(route) {
                op["responses"]["200"]["headers"]["X-TKV-History"] = json!({"description":"Opaque tenant/history/epoch token. Pair with _rev in write_options.expected_history; pair with write rev in X-TKV-Min-History and X-TKV-Min-Seq for consistent reads.","schema":{"type":"string"}});
            }
            if route.starts_with("/v1/replication/") {
                op["description"] = json!(if replication::peer_route(route) {
                    "Requires a global admin or replication credential. Promotion independently verifies the configured source's durable fence."
                } else {
                    "Requires a global admin. An interrupted switch may leave a durable fence; inspect both nodes and retry."
                });
            }
            if *route == "/v1/replication/log" {
                op["responses"]["200"]["content"][crate::replication::wire::LOG_MEDIA_TYPE] = json!({
                    "schema":{"type":"string","format":"binary"},
                    "description":"Opt in with Accept: application/vnd.idkv.wal-page.v1. The 57-byte page header is followed by the original WAL frames."
                });
            }
            if backups::is_job_post(route, method) {
                let success = op["responses"]
                    .as_object_mut()
                    .unwrap()
                    .remove("200")
                    .unwrap();
                op["responses"]["202"] = success;
            }
            for (status, description) in [
                ("400", "Invalid request"),
                ("401", "Authentication required"),
                ("403", "Tenant or role denied"),
                ("404", "Not found"),
                ("405", "Method not allowed"),
                ("408", "Read deadline"),
                ("422", "Collection scan disallowed by request"),
                ("409", "Conflict"),
                ("410", "Expired cursor, replication log or snapshot"),
                (
                    "428",
                    "History token required for conditional/idempotent writes",
                ),
                ("429", "Resource budget exceeded"),
                ("500", "Storage error or unknown commit outcome"),
                ("503", "Tenant unavailable"),
            ] {
                op["responses"][status] = json!({"description":description,"content":{"application/json":{"schema":schema_ref("Error")}}});
            }
            if let Some(schema) = request_schema(route, method) {
                op["requestBody"] = json!({"required":*method!="DELETE","content":{"application/json":{"schema":schema_ref(schema)}}});
            }
            if matches!(
                *route,
                "/health" | "/ready" | "/ready/write" | "/openapi.json" | "/v1/capabilities"
            ) {
                op["security"] = json!([]);
            }
            operations.insert(method.to_lowercase(), op);
        }
        paths.insert((*route).into(), Value::Object(operations));
    }
    let mut api = json!({"openapi":"3.1.0","info":{"title":"Tenant KV","version":env!("CARGO_PKG_VERSION"),"description":"Physically isolated tenant databases. Document writes acknowledge durable local WAL commits. Optional one-primary/one-replica asynchronous replication supports durable fencing, planned switchover and explicit externally fenced emergency promotion. Replica writes are rejected; replicated roots reject ordinary restore/import. Remote backup submissions return 202 jobs; only completed creates are remotely published. Index creation waits for READY or failure."},"security":[{"bearerAuth":[]}],"components":{"securitySchemes":{"bearerAuth":{"type":"http","scheme":"bearer"}},"schemas":schemas()},"paths":paths});
    let schemas = &mut api["components"]["schemas"];
    schemas
        .as_object_mut()
        .unwrap()
        .extend(replication::schemas());
    schemas.as_object_mut().unwrap().extend(identity::schemas());
    super::contracts::extend(&mut api);
    let schemas = &mut api["components"]["schemas"];
    schemas["Batch"]["properties"]["operations"]["maxItems"] = json!(options.max_batch_operations);
    for schema in ["ReadBatch", "PublishedReads"] {
        schemas[schema]["properties"]["operations"]["maxItems"] =
            json!(options.max_batch_operations);
    }
    schemas["IdentityResolveMany"]["properties"]["items"]["maxItems"] =
        json!(options.max_read_keys);
    schemas["Keys"]["properties"]["keys"]["maxItems"] = json!(options.max_read_keys);
    schemas["LookupInput"]["properties"]["inputs"]["maxItems"] = json!(options.max_read_keys);
    schemas["PublishedInput"]["properties"]["inputs"]["maxItems"] = json!(options.max_read_keys);
    for name in [
        "Query",
        "SubjectIdentifiersRequest",
        "SubjectHistoryRequest",
    ] {
        schemas[name]["properties"]["limit"]["maximum"] = json!(options.max_page_size);
    }
    schemas["Document"]["x-max-binary-bytes"] = json!(options.max_document_bytes);
    api["x-resource-limits"] = json!({"document_bytes":options.max_document_bytes,"batch_bytes":options.max_batch_bytes,"response_bytes":options.max_response_bytes,"scan_entries":options.max_scan,"query_ms":options.query_ms,"snapshot_seconds":options.snapshot_seconds,"read_batch_keys_plus_query_limits":options.max_read_keys,"identity_resolve_many_items":options.max_read_keys});
    api
}
fn schema_ref(name: &str) -> Value {
    json!({"$ref":format!("#/components/schemas/{name}")})
}
fn array(schema: Value) -> Value {
    json!({"type":"array","items":schema})
}
fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn request_schema(route: &str, method: &str) -> Option<&'static str> {
    if route.ends_with("/read_batch") {
        return Some("ReadBatch");
    }
    if route.ends_with("/resources/{resource}/read") {
        return Some("PublishedReads");
    }
    if route.ends_with("/resources/{resource}/query") {
        return Some("PublishedQueryRequest");
    }
    if route.contains("/identity/") {
        return identity::request_schema(route);
    }
    if method == "GET" {
        return None;
    }
    if route.ends_with("/retention") {
        return Some("RetentionConfig");
    }
    if route.ends_with("/resources/{resource}") {
        return Some("Publication");
    }
    if route.ends_with("/resources/{resource}/execute") {
        return Some("PublishedInput");
    }
    if route.ends_with("/replication/wait") {
        return Some("ReplicationWait");
    }
    if route.starts_with("/v1/replication/") {
        return Some(match route {
            "/v1/replication/log" => "ReplicationLogRequest",
            "/v1/replication/snapshot" => "ReplicationSnapshotRequest",
            "/v1/replication/snapshot/chunk" => "ReplicationChunkRequest",
            "/v1/replication/snapshot/release" => "ReplicationId",
            "/v1/replication/fence" => "ReplicationFenceRequest",
            "/v1/replication/promote" => "ReplicationPromote",
            "/v1/replication/force-promote" => "ReplicationForce",
            _ => "ReplicationEmpty",
        });
    }
    if route.ends_with("/dsl") {
        return Some("DslRequest");
    }
    if backups::is_backup_route(route) {
        return if route.ends_with("/prune") {
            Some("BackupPrune")
        } else {
            None
        };
    }
    if route == "/v1/tenants" || route.ends_with("/{tenant}/collections") {
        return Some("Name");
    }
    if route == "/v1/tenants/{tenant}" && method == "PATCH" {
        return Some("Quota");
    }
    if route.ends_with("/restore") {
        return Some("Restore");
    }
    if route.ends_with("/documents") {
        return Some("Put");
    }
    if route.ends_with("/documents/{key}") {
        return Some("ConditionalDelete");
    }
    if route.ends_with("/batch") {
        return Some("Batch");
    }
    if route.ends_with("/multi_get") {
        return Some("Keys");
    }
    if route.ends_with("/indexes") {
        return Some("IndexSpec");
    }
    if route.ends_with("/lookups") {
        return Some("LookupDefinition");
    }
    if route.ends_with("/execute") {
        return Some("LookupInput");
    }
    if route.ends_with("/query") || route.ends_with("/explain") {
        return Some("Query");
    }
    None
}
fn response_schema(route: &str, method: &str) -> Value {
    if let Some(schema) = super::contracts::response_schema(route) {
        return schema;
    }
    if route.contains("/identity/") {
        return identity::response_schema(route);
    }
    if route.starts_with("/v1/replication/") {
        return match route {
            "/v1/replication/status" => schema_ref("ReplicationStatus"),
            "/v1/replication/log" => schema_ref("ReplicationLogPage"),
            "/v1/replication/snapshot" => schema_ref("ReplicationSnapshot"),
            "/v1/replication/snapshot/chunk" => object(
                json!({"data":{"type":"string","contentEncoding":"base64"}}),
                &["data"],
            ),
            "/v1/replication/snapshot/release" => schema_ref("Ok"),
            "/v1/replication/fence" => schema_ref("ReplicationFence"),
            _ => schema_ref("ReplicationNode"),
        };
    }
    if route.ends_with("/replication/wait") {
        return object(
            json!({"applied_seq":{"type":"integer","minimum":0}}),
            &["applied_seq"],
        );
    }
    if route.ends_with("/dsl") {
        return json!({"description":"Native operation response: query, explain, get/null, multi_get, lookup, resolve, resolvemany, ids, history or write result. Read role for reads; write role for put/delete."});
    }
    if backups::is_backup_route(route) {
        return if route.ends_with("/remote-backups") && method == "GET" {
            array(schema_ref("RemoteBackup"))
        } else if route.ends_with("/backup-jobs") {
            array(schema_ref("BackupJob"))
        } else if method == "DELETE" {
            schema_ref("Ok")
        } else {
            schema_ref("BackupJob")
        };
    }
    if route == "/v1/tenants" {
        return if method == "GET" {
            array(schema_ref("Tenant"))
        } else {
            schema_ref("Tenant")
        };
    }
    if route == "/v1/tenants/{tenant}" {
        return match method {
            "PATCH" => schema_ref("Tenant"),
            "GET" => {
                json!({"type":"object","description":"Tenant lifecycle, failure and storage metrics"})
            }
            _ => schema_ref("Ok"),
        };
    }
    if route.ends_with("/backup") {
        return schema_ref("Backup");
    }
    if route.ends_with("/{tenant}/collections") {
        return if method == "GET" {
            array(schema_ref("Collection"))
        } else {
            schema_ref("Collection")
        };
    }
    if route.ends_with("/documents") {
        return schema_ref("WriteResult");
    }
    if route.ends_with("/documents/{key}") {
        return schema_ref(if method == "GET" {
            "Document"
        } else {
            "WriteResult"
        });
    }
    if route.ends_with("/batch") {
        return array(schema_ref("WriteResult"));
    }
    if route.ends_with("/multi_get") {
        return array(json!({"oneOf":[schema_ref("Document"),{"type":"null"}]}));
    }
    if route.ends_with("/indexes") {
        return if method == "GET" {
            array(schema_ref("Index"))
        } else {
            schema_ref("Index")
        };
    }
    if route.ends_with("/indexes/{index}") && method == "GET" {
        return schema_ref("Index");
    }
    if route.ends_with("/lookups") && method == "GET" {
        return array(schema_ref("LookupDefinition"));
    }
    if route.ends_with("/execute") {
        return schema_ref("LookupResult");
    }
    if route.ends_with("/query") {
        return schema_ref("QueryResult");
    }
    if route.ends_with("/explain") {
        return schema_ref("Explain");
    }
    if route == "/openapi.json" || route == "/v1/metrics" || route == "/health" {
        return json!({"type":"object"});
    }
    schema_ref("Ok")
}
fn schemas() -> Value {
    let string = json!({"type":"string"});
    let integer = json!({"type":"integer","minimum":0});
    let pointer = json!({"type":"string","pattern":"^/","maxLength":1024});
    let mut s = serde_json::Map::new();
    s.insert("Ok".into(), object(json!({"ok":{"const":true}}), &["ok"]));
    s.insert(
        "Error".into(),
        object(
            json!({"error":object(json!({"code":string,"message":string,"location":object(json!({"byte_offset":{"type":"integer","minimum":0},"line":{"type":"integer","minimum":1},"column":{"type":"integer","minimum":1}}),&["byte_offset","line","column"])}),&["code","message"])}),
            &["error"],
        ),
    );
    s.insert("DslRequest".into(),object(json!({"statement":{"type":"string","minLength":1,"maxLength":65536,"description":"One DSL statement, at most 64 KiB UTF-8."},"params":{"type":"object","additionalProperties":true,"default":{}},"cursor":{"type":["string","null"],"description":"For from, ids and history; bind the same filters."}}),&["statement"]));
    s.insert(
        "Name".into(),
        object(
            json!({"name":{"type":"string","minLength":1,"maxLength":128}}),
            &["name"],
        ),
    );
    s.insert("Document".into(),json!({"type":"object","properties":{"_key":{"type":"string","minLength":1,"maxLength":512},"_rev":{"type":"integer","readOnly":true}},"additionalProperties":true,"description":"Dynamic document; 1 MiB binary size, nesting 32, scalar indexed paths. _rev is forbidden in writes; use if_rev. Integer and floating point values remain distinct."}));
    s.insert(
        "Put".into(),
        object(
            json!({"document":schema_ref("Document"),"if_rev":integer}),
            &["document"],
        ),
    );
    s.insert(
        "ConditionalDelete".into(),
        object(json!({"if_rev":integer}), &[]),
    );
    s.insert("WriteOp".into(),json!({"oneOf":[object(json!({"op":{"const":"put"},"document":schema_ref("Document"),"if_rev":integer}),&["op","document"]),object(json!({"op":{"const":"delete"},"key":string,"if_rev":integer}),&["op","key"])]}));
    s.insert("Batch".into(),object(json!({"operations":{"type":"array","minItems":1,"maxItems":1000,"items":schema_ref("WriteOp")}}),&["operations"]));
    s.insert(
        "Keys".into(),
        object(
            json!({"keys":{"type":"array","maxItems":1000,"items":string}}),
            &["keys"],
        ),
    );
    s.insert(
        "WriteResult".into(),
        object(
            json!({"key":string,"rev":integer,"deleted":{"type":"boolean"}}),
            &["key", "rev", "deleted"],
        ),
    );
    s.insert("IndexField".into(),object(json!({"field":pointer,"order":{"type":"string","enum":["asc","desc"],"default":"asc"}}),&["field"]));
    s.insert("IndexSpec".into(),object(json!({"name":string,"fields":{"type":"array","minItems":1,"maxItems":8,"items":schema_ref("IndexField")},"unique":{"type":"boolean","default":false},"include":{"type":"array","maxItems":32,"items":pointer}}),&["name","fields"]));
    s.insert("Index".into(),object(json!({"id":{"type":"string","format":"uuid"},"generation":{"type":"string","format":"uuid"},"spec":schema_ref("IndexSpec"),"state":{"enum":["BUILDING","READY","FAILED"]},"start_seq":integer,"ready_seq":integer,"error":{"type":["string","null"]},"entries":integer}),&["id","generation","spec","state","start_seq","ready_seq","error","entries"]));
    s.insert("LookupDefinition".into(),object(json!({"name":string,"index":string,"select":{"type":"array","maxItems":64,"items":pointer},"mode":{"enum":["all","first"],"default":"all"},"max_matches":{"type":"integer","minimum":1,"maximum":1000,"default":100}}),&["name","index","select"]));
    s.insert(
        "LookupInput".into(),
        object(
            json!({"inputs":{"type":"array","maxItems":1000,"items":{"type":"object"}}}),
            &["inputs"],
        ),
    );
    s.insert("Filter".into(),json!({"oneOf":[
        object(json!({"and":array(schema_ref("Filter"))}),&["and"]),object(json!({"or":array(schema_ref("Filter"))}),&["or"]),object(json!({"not":schema_ref("Filter")}),&["not"]),
        object(json!({"field":pointer,"op":{"enum":["eq","ne","gt","gte","lt","lte"]},"value":{"type":["string","number","boolean","null"]}}),&["field","op","value"]),
        object(json!({"field":pointer,"op":{"const":"in"},"value":{"type":"array","maxItems":1000,"items":{"type":["string","number","boolean","null"]}}}),&["field","op","value"]),
        object(json!({"field":pointer,"op":{"const":"exists"},"value":{"type":"boolean"}}),&["field","op","value"])
    ]}));
    s.insert("Query".into(),object(json!({"filter":schema_ref("Filter"),"sort":{"type":"array","maxItems":8,"items":schema_ref("IndexField")},"select":{"type":"array","maxItems":64,"items":pointer},"limit":{"type":"integer","minimum":1,"maximum":1000,"default":100},"cursor":{"type":["string","null"]}}),&[]));
    s.insert("ReadStats".into(),object(json!({"candidate_files":integer,"scanned_entries":integer,"blocks_read":integer,"decompressed_bytes":integer,"cache_hits":integer,"document_reads":integer}),&["candidate_files","scanned_entries","blocks_read","decompressed_bytes","cache_hits","document_reads"]));
    s.insert("Explain".into(),object(json!({"indexes":array(string.clone()),"covering":{"type":"boolean"},"extra_sort":{"type":"boolean"},"scan":{"type":"boolean"}}),&["indexes","covering","extra_sort","scan"]));
    s.insert("QueryResult".into(),object(json!({"documents":array(json!({"type":"object"})),"cursor":{"type":["string","null"]},"snapshot_seq":integer,"plan":schema_ref("Explain"),"stats":schema_ref("ReadStats")}),&["documents","cursor","snapshot_seq","plan","stats"]));
    s.insert("LookupResult".into(),object(json!({"matches":array(array(json!({"type":"object"}))),"snapshot_seq":integer,"stats":schema_ref("ReadStats")}),&["matches","snapshot_seq","stats"]));
    s.insert(
        "Collection".into(),
        object(
            json!({"id":{"type":"string","format":"uuid"},"name":string}),
            &["id", "name"],
        ),
    );
    s.insert("Tenant".into(),object(json!({"id":{"type":"string","format":"uuid"},"name":string,"generation":{"type":"string","format":"uuid"},"memory_bytes":integer,"disk_bytes":integer}),&["id","name","generation","memory_bytes","disk_bytes"]));
    s.insert(
        "Quota".into(),
        object(
            json!({"memory_bytes":integer,"disk_bytes":integer}),
            &["memory_bytes", "disk_bytes"],
        ),
    );
    for key in ["Backup", "Restore"] {
        s.insert(
            key.into(),
            object(
                json!({"backup":{"type":"string","format":"uuid"}}),
                &["backup"],
            ),
        );
    }
    s.extend(backups::schemas().as_object().unwrap().clone());
    Value::Object(s)
}
