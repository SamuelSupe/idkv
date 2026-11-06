use serde_json::json;
use tenant_kv::*;

fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "data/demo".into());
    let store = Store::open(root, Options::default())?;
    let info = store
        .tenants()
        .into_iter()
        .find(|t| t.name == "demo")
        .map(Ok)
        .unwrap_or_else(|| store.create_tenant("demo"))?;
    let tenant = store.tenant(info.id)?;
    if tenant.collections()?.is_empty() {
        tenant.create_collection("assets")?;
        tenant.create_index(
            "assets",
            IndexSpec {
                name: "ip".into(),
                fields: vec![IndexField {
                    field: "/ip".into(),
                    order: Order::Asc,
                }],
                unique: true,
                include: vec!["/owner".into(), "/service".into()],
            },
        )?;
        tenant.create_lookup(
            "assets",
            LookupDefinition {
                name: "asset_owner".into(),
                index: "ip".into(),
                select: vec!["/ip".into(), "/owner".into(), "/service".into()],
                mode: MatchMode::All,
                max_matches: 100,
            },
        )?;
    }
    let write = tenant.put(
        "assets",
        json!({
            "_key":"gateway", "ip":"10.0.0.1", "owner":"platform", "service":"edge",
            "custom":{"region":"ap-southeast-1"}
        }),
        None,
    )?;
    let result = tenant.lookup_many(
        "assets",
        "asset_owner",
        &[
            json!({"ip":"10.0.0.1"}),
            json!({"ip":"unknown"}),
            json!({"missing":true}),
        ],
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"tenant":info.id,"write":write,"lookup":result}))?
    );
    tenant.engine().flush()?;
    Ok(())
}
