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
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"tenant":tenant.id(),"entities":{"alice":entities[0],"bob":entities[1]},"correction":receipt,"latest":latest,"original_knowledge":old,"timeline":tenant.identity_timeline(&identifier)?})
        )?
    );
    Ok(())
}
