use super::*;
use crate::identity::*;

pub(super) fn dispatch(
    tenant: &crate::TenantHandle,
    route: &str,
    params: &HashMap<String, String>,
    body: Value,
    token: &Token,
    telemetry: &crate::telemetry::Telemetry,
    read: &read_controls::OperationContext<'_>,
) -> Result<Value> {
    let context = IdentityContext {
        source: "http".into(),
        actor: token.subject.clone(),
        can_correct: matches!(token.role, Role::Admin | Role::IdentityCorrect),
    };
    Ok(if route.ends_with("/initialize") {
        serde_json::to_value(tenant.identity_initialize(&context, decode(body)?)?)?
    } else if route.ends_with("/write") {
        let mut body = body;
        let options = body
            .as_object_mut()
            .and_then(|map| map.remove("write_options"))
            .map(decode::<WriteOptions>)
            .transpose()?
            .unwrap_or_default();
        serde_json::to_value(tenant.identity_write_with(&context, decode(body)?, &options)?)?
    } else if route.ends_with("/resolve") {
        let result = read.reader(tenant)?.identity_resolve(&decode(body)?)?;
        telemetry.record_read(&result.stats);
        serde_json::to_value(result)?
    } else if route.ends_with("/resolve_many") {
        let result = read.reader(tenant)?.identity_resolve_many(&decode(body)?)?;
        telemetry.record_read(&result.stats);
        serde_json::to_value(result)?
    } else if route.ends_with("/ids") {
        let result = read
            .reader(tenant)?
            .get_subject_identifiers(&decode(body)?)?;
        telemetry.record_read(&result.stats);
        serde_json::to_value(result)?
    } else if route.ends_with("/history") {
        let result = read.reader(tenant)?.get_subject_history(&decode(body)?)?;
        telemetry.record_read(&result.stats);
        serde_json::to_value(result)?
    } else if route.ends_with("/timeline") {
        serde_json::to_value(tenant.identity_timeline(&decode(body)?)?)?
    } else if route.ends_with("/{entity}") {
        serde_json::to_value(
            tenant.identity_entity(
                params["entity"]
                    .parse()
                    .map_err(|_| Error::invalid("entity UUID"))?,
            )?,
        )?
    } else {
        serde_json::to_value(tenant.identity_operation(&params["operation"])?)?
    })
}
pub(super) fn request_schema(route: &str) -> Option<&'static str> {
    if route.ends_with("/initialize") {
        Some("IdentityConfig")
    } else if route.ends_with("/write") {
        Some("IdentityWrite")
    } else if route.ends_with("/resolve") {
        Some("IdentityResolve")
    } else if route.ends_with("/resolve_many") {
        Some("IdentityResolveMany")
    } else if route.ends_with("/ids") {
        Some("SubjectIdentifiersRequest")
    } else if route.ends_with("/history") {
        Some("SubjectHistoryRequest")
    } else if route.ends_with("/timeline") {
        Some("Identifier")
    } else {
        None
    }
}
fn reference(name: &str) -> Value {
    json!({"$ref":format!("#/components/schemas/{name}")})
}
fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn nullable(schema: Value) -> Value {
    json!({"oneOf":[schema,{"type":"null"}]})
}
pub(super) fn response_schema(route: &str) -> Value {
    if route.ends_with("/initialize") {
        reference("IdentityKnowledge")
    } else if route.ends_with("/resolve") {
        reference("IdentityResolution")
    } else if route.ends_with("/resolve_many") {
        reference("IdentityResolutions")
    } else if route.ends_with("/ids") {
        reference("SubjectIdentifiersResult")
    } else if route.ends_with("/history") {
        reference("SubjectHistoryResult")
    } else if route.ends_with("/timeline") {
        nullable(reference("IdentityTimeline"))
    } else if route.ends_with("/{entity}") {
        reference("IdentityEntity")
    } else {
        reference("IdentityReceipt")
    }
}
pub(super) fn schemas() -> serde_json::Map<String, Value> {
    let uuid = json!({"type":"string","format":"uuid"});
    let micros = json!({"type":"integer","format":"int64","description":"UTC microseconds since Unix epoch"});
    let text = json!({"type":"string"});
    let revision = json!({"oneOf":[uuid,{"const":"absent"}]});
    let reason = json!({"type":"string","minLength":1,"maxLength":2048});
    let evidence = json!({"type":["string","null"],"maxLength":256,"description":"Administrator asserted reference, not independently verified evidence"});
    let mut out = serde_json::Map::new();
    out.insert("Identifier".into(),object(json!({"kind":{"type":"string","minLength":1,"maxLength":64},"namespace":{"type":"string","minLength":1,"maxLength":256},"value":{"type":"string","minLength":1,"maxLength":2048}}),&["kind","namespace","value"]));
    out.insert("IdentityConfig".into(),object(json!({"max_segments":{"type":"integer","minimum":1,"maximum":1000,"default":1000},"clock_tolerance_ms":{"type":"integer","minimum":1,"maximum":60000,"default":1000}}),&[]));
    out.insert("IdentityEffective".into(),json!({"oneOf":[object(json!({"mode":{"const":"auto"}}),&["mode"]),object(json!({"mode":{"const":"manual"},"from":{"type":"string","format":"date-time"},"to":{"type":["string","null"],"format":"date-time"}}),&["mode","from"])]}));
    out.insert("IdentityAction".into(),json!({"oneOf":[
        object(json!({"action":{"const":"bind"},"entity_id":uuid,"evidence_id":evidence,"reason":nullable(reason.clone())}),&["action","entity_id"]),
        object(json!({"action":{"const":"rebind"},"entity_id":uuid,"evidence_id":evidence,"reason":reason}),&["action","entity_id","reason"]),
        object(json!({"action":{"const":"unbind"},"reason":reason}),&["action","reason"]),
        object(json!({"action":{"const":"replace_identifier"},"entity_id":uuid,"replacement":reference("Identifier"),"replacement_expected_revision":revision,"evidence_id":evidence,"reason":reason}),&["action","entity_id","replacement","replacement_expected_revision","reason"])
    ]}));
    out.insert("IdentityChange".into(),object(json!({"identifier":reference("Identifier"),"expected_revision":revision,"effective":reference("IdentityEffective"),"change":reference("IdentityAction")}),&["identifier","expected_revision","change"]));
    let op = json!({"type":"string","minLength":1,"maxLength":128});
    let attributes = json!({"type":"object","additionalProperties":true});
    out.insert("IdentityWrite".into(),json!({"description":"Requires admin, identity_write, or identity_correct. Manual correction requires admin or identity_correct. Same operation ID and authenticated actor must retry the identical request. Manual intervals require a reason. Actual expanded records and WAL bytes must fit configured atomic limits.","oneOf":[
        object(json!({"action":{"const":"create_entity"},"operation_id":op,"attributes":attributes}),&["action","operation_id","attributes"]),
        object(json!({"action":{"const":"update_entity"},"operation_id":op,"entity_id":uuid,"expected_revision":uuid,"attributes":attributes}),&["action","operation_id","entity_id","expected_revision","attributes"]),
        object(json!({"action":{"const":"bindings"},"operation_id":op,"changes":{"type":"array","minItems":1,"items":reference("IdentityChange")}}),&["action","operation_id","changes"])
    ]}));
    out.insert("IdentityResolve".into(),object(json!({"identifier":reference("Identifier"),"valid_at":{"type":"string","format":"date-time"},"known_at":{"oneOf":[uuid,{"const":"latest"},{"type":"null"}]}}),&["identifier","valid_at"]));
    out.insert("IdentityEntity".into(),object(json!({"entity_id":uuid,"revision":uuid,"bindings_revision":uuid,"attributes":attributes}),&["entity_id","revision","bindings_revision","attributes"]));
    out.insert("IdentityBinding".into(),object(json!({"valid_from_us":micros,"valid_to_us":nullable(micros.clone()),"entity_id":uuid,"binding_id":uuid,"version_id":uuid,"source":text,"evidence_id":evidence,"assurance":{"const":"asserted"}}),&["valid_from_us","valid_to_us","entity_id","binding_id","version_id","source","evidence_id","assurance"]));
    let segments = json!({"type":"array","items":reference("IdentityBinding")});
    out.insert(
        "IdentityTimeline".into(),
        object(
            json!({"identifier":reference("Identifier"),"revision":uuid,"segments":segments}),
            &["identifier", "revision", "segments"],
        ),
    );
    out.insert("IdentityKnowledge".into(),object(json!({"version":uuid,"ordinal":{"type":"integer","minimum":0},"previous":nullable(uuid.clone()),"recorded_at_us":micros,"commit_seq":{"type":"integer","minimum":0},"source":text,"actor":text}),&["version","ordinal","previous","recorded_at_us","commit_seq","source","actor"]));
    out.insert("IdentityTimelineChange".into(),object(json!({"identifier":reference("Identifier"),"revision":nullable(uuid.clone()),"changed":{"type":"boolean"},"effective_from_us":micros,"effective_to_us":nullable(micros.clone()),"replaced":segments}),&["identifier","revision","changed","effective_from_us","effective_to_us","replaced"]));
    out.insert("IdentityReceipt".into(),object(json!({"operation_id":text,"knowledge":reference("IdentityKnowledge"),"effective_time_us":micros,"entity":nullable(reference("IdentityEntity")),"timelines":{"type":"array","items":reference("IdentityTimelineChange")}}),&["operation_id","knowledge","effective_time_us","entity","timelines"]));
    out.insert("IdentityResolution".into(),object(json!({"knowledge_version":uuid,"timeline_revision":nullable(uuid.clone()),"binding":nullable(reference("IdentityBinding")),"stats":reference("ReadStats")}),&["knowledge_version","timeline_revision","binding","stats"]));
    out.insert("IdentityResolveItem".into(), object(json!({"identifier":reference("Identifier"),"valid_at":{"type":"string","format":"date-time"}}), &["identifier","valid_at"]));
    out.insert("IdentityResolveMany".into(), object(json!({"known_at":{"oneOf":[uuid,{"const":"latest"},{"type":"null"}]},"items":{"type":"array","minItems":1,"items":reference("IdentityResolveItem")}}), &["items"]));
    out.insert("IdentityResolveMatch".into(), object(json!({"timeline_revision":nullable(uuid.clone()),"binding":nullable(reference("IdentityBinding"))}), &["timeline_revision","binding"]));
    out.insert("IdentityResolutions".into(), object(json!({"knowledge_version":uuid,"history":{"type":"string"},"snapshot_seq":{"type":"integer","minimum":0},"results":{"type":"array","items":reference("IdentityResolveMatch")},"stats":reference("ReadStats")}), &["knowledge_version","history","snapshot_seq","results","stats"]));
    let known = json!({"oneOf":[uuid,{"const":"latest"},{"type":"null"}]});
    let limit = json!({"type":"integer","minimum":1,"maximum":1000,"default":100});
    let cursor = json!({"type":["string","null"],"description":"Repeat filters; only null means exhausted. Bound to tenant, principal and node."});
    let timestamp = json!({"type":"string","format":"date-time"});
    out.insert("SubjectIdentifiersRequest".into(), object(json!({"entity_id":uuid,"valid_at":timestamp,"known_at":known,"kind":{"type":["string","null"],"minLength":1,"maxLength":64},"namespace":{"type":["string","null"],"minLength":1,"maxLength":256},"limit":limit,"cursor":cursor}), &["entity_id","valid_at"]));
    out.insert("SubjectHistoryRequest".into(), object(json!({"entity_id":uuid,"known_at":known,"recorded_from":nullable(timestamp.clone()),"recorded_to":nullable(timestamp),"limit":limit,"cursor":cursor}), &["entity_id"]));
    out.insert("SubjectIdentifier".into(), object(json!({"identifier":reference("Identifier"),"timeline_revision":uuid,"binding":reference("IdentityBinding")}), &["identifier","timeline_revision","binding"]));
    out.insert(
        "IdentityAttributeValue".into(),
        object(
            json!({"present":{"type":"boolean"},"value":{}}),
            &["present", "value"],
        ),
    );
    out.insert("IdentityAttributeChange".into(), object(json!({"field":text,"before":reference("IdentityAttributeValue"),"after":reference("IdentityAttributeValue")}), &["field","before","after"]));
    out.insert("IdentityIdentifierChange".into(), object(json!({"identifier":reference("Identifier"),"timeline_revision":uuid,"effective_from_us":micros,"effective_to_us":nullable(micros),"before":segments,"after":segments}), &["identifier","timeline_revision","effective_from_us","effective_to_us","before","after"]));
    out.insert("IdentityEvent".into(), object(json!({"event_id":uuid,"operation_id":text,"knowledge":reference("IdentityKnowledge"),"change_index":{"type":"integer","minimum":0},"kind":{"enum":["entity_created","attributes_updated","identifier_bound","identifier_rebound","identifier_unbound","identifier_replaced"]},"entity_ids":{"type":"array","items":uuid},"reason":nullable(reason),"changes":{"type":"array","items":reference("IdentityIdentifierChange")},"attributes":{"type":"array","items":reference("IdentityAttributeChange")}}), &["event_id","operation_id","knowledge","change_index","kind","entity_ids","reason","changes","attributes"]));
    out.insert("IdentityHistoryCoverage".into(), object(json!({"recording_started":reference("IdentityKnowledge"),"legacy_history_missing":{"type":"boolean"},"retention_enabled":{"type":"boolean"}}), &["recording_started","legacy_history_missing","retention_enabled"]));
    out.insert("SubjectIdentifiersResult".into(), object(json!({"entity_id":uuid,"knowledge_version":uuid,"history":text,"snapshot_seq":{"type":"integer","minimum":0},"identifiers":{"type":"array","items":reference("SubjectIdentifier")},"cursor":cursor,"stats":reference("ReadStats")}), &["entity_id","knowledge_version","history","snapshot_seq","identifiers","cursor","stats"]));
    out.insert("SubjectHistoryResult".into(), object(json!({"entity_id":uuid,"knowledge_version":uuid,"history":text,"snapshot_seq":{"type":"integer","minimum":0},"events":{"type":"array","items":reference("IdentityEvent")},"coverage":reference("IdentityHistoryCoverage"),"cursor":cursor,"stats":reference("ReadStats")}), &["entity_id","knowledge_version","history","snapshot_seq","events","coverage","cursor","stats"]));
    out
}
