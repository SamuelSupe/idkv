//! cargo run --locked --example identity -- /var/tmp/tenant-kv-identity-demo
use serde_json::json;
use tenant_kv::{identity::*, *};
fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./identity-data".into());
    let store = Store::open(root, Options::default())?;
    let tenant = store.tenant(store.create_tenant("identity-demo")?.id)?;
    let context = IdentityContext {
        source: "trusted_embedding".into(),
        actor: "example".into(),
        can_correct: true,
    };
    tenant.identity_initialize(&context, IdentityConfig::default())?;
    let mut entities = Vec::new();
    for name in ["Alice", "Bob"] {
        let request = serde_json::from_value(
            json!({"action":"create_entity","operation_id":format!("create-{name}"),"attributes":{"name":name}}),
        )?;
        entities.push(
            tenant
                .identity_write(&context, request)?
                .entity
                .unwrap()
                .entity_id,
        );
    }
    let identifier = Identifier {
        kind: "account".into(),
        namespace: "crm".into(),
        value: "10086".into(),
    };
    let initial = serde_json::from_value(
        json!({"action":"bindings","operation_id":"bind-alice","changes":[{"identifier":identifier,"expected_revision":"absent","effective":{"mode":"manual","from":"2026-11-05T09:00:00Z","to":"2026-11-05T12:00:00Z"},"change":{"action":"bind","entity_id":entities[0],"reason":"initial source evidence"}}]}),
    )?;
    let first = tenant.identity_write(&context, initial)?;
    let correction = serde_json::from_value(
        json!({"action":"bindings","operation_id":"correct-bob","changes":[{"identifier":identifier,"expected_revision":first.timelines[0].revision,"effective":{"mode":"manual","from":"2026-11-05T10:00:00Z","to":"2026-11-05T11:00:00Z"},"change":{"action":"rebind","entity_id":entities[1],"reason":"late account handover notification"}}]}),
    )?;
    let receipt = tenant.identity_write(&context, correction)?;
    let query = ResolveRequest {
        identifier: identifier.clone(),
        valid_at: "2026-11-05T10:30:00Z".into(),
        known_at: None,
    };
    let latest = tenant.identity_resolve(&query)?;
    let old = tenant.identity_resolve(&ResolveRequest {
        known_at: Some(first.knowledge.version.to_string()),
        ..query
    })?;
    assert_eq!(latest.binding.as_ref().unwrap().entity_id, entities[1]);
    assert_eq!(old.binding.as_ref().unwrap().entity_id, entities[0]);
    let batch_request = ResolveManyRequest {
        known_at: None,
        items: ["2026-11-05T10:30:00Z", "2026-11-05T11:00:00Z"]
            .into_iter()
            .map(|valid_at| ResolveItem {
                identifier: identifier.clone(),
                valid_at: valid_at.into(),
            })
            .collect(),
    };
    let batch = tenant
        .read_with(&ReadOptions::default())?
        .identity_resolve_many(&batch_request)?;
    assert_eq!(
        batch.results[0].binding.as_ref().unwrap().entity_id,
        entities[1]
    );
    assert_eq!(
        batch.results[1].binding.as_ref().unwrap().entity_id,
        entities[0]
    );
    let prepared = tenant.prepare_dsl("resolvemany with $request")?;
    let params = json!({"request":batch_request});
    let dsl =
        serde_json::to_value(prepared.execute(&tenant, params.as_object().unwrap(), None)?)?;
    assert_eq!(dsl["results"], serde_json::to_value(&batch.results)?);
    let ids_request: SubjectIdentifiersRequest = serde_json::from_value(json!({
        "entity_id":entities[1], "valid_at":"2026-11-05T10:30:00Z", "limit":100
    }))?;
    let ids = tenant.get_subject_identifiers(&ids_request)?;
    assert_eq!(ids.identifiers[0].identifier, identifier);
    let history_request: SubjectHistoryRequest =
        serde_json::from_value(json!({"entity_id":entities[1],"limit":100}))?;
    let history = tenant.get_subject_history(&history_request)?;
    let ids_params = json!({"request":ids_request});
    let short_dsl = serde_json::to_value(tenant.prepare_dsl("ids with $request")?.execute(
        &tenant,
        ids_params.as_object().unwrap(),
        None,
    )?)?;
    assert_eq!(
        short_dsl["identifiers"],
        serde_json::to_value(&ids.identifiers)?
    );
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"tenant":tenant.id(),"entities":{"alice":entities[0],"bob":entities[1]},"correction":receipt,"latest":latest,"original_knowledge":old,"batch":batch,"ids":ids,"subject_history":history,"timeline":tenant.identity_timeline(&identifier)?})
        )?
    );
    Ok(())
}
