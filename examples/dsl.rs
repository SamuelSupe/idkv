//! cargo run --example dsl -- /var/tmp/tenant-kv-dsl-demo
use serde_json::json;
use tenant_kv::{
    dsl::{Output, Request},
    *,
};

fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "data/dsl-demo".into());
    let store = Store::open(root, Options::default())?;
    let info = if let Some(info) = store.tenants().into_iter().find(|t| t.name == "dsl-demo") {
        info
    } else {
        store.create_tenant("dsl-demo")?
    };
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
                select: vec!["/owner".into(), "/service".into()],
                mode: MatchMode::All,
                max_matches: 1,
            },
        )?;
    }
    let put = tenant.prepare_dsl("put assets $document")?;
    put.execute(&tenant,json!({"document":{"_key":"gateway-1","ip":"10.0.0.1","owner":"platform","service":"edge","env":"prod","score":95}}).as_object().unwrap(),None)?;
    let query=Request { write_options: Default::default(),statement:"from assets | where env == $env and score >= 80 | sort score desc | fields _key, owner, service | limit 10".into(),params:json!({"env":"prod"}).as_object().unwrap().clone(),cursor:None};
    let Output::Query(result) = tenant.execute_dsl(&query)? else {
        unreachable!()
    };
    let lookup = tenant.prepare_dsl("lookup assets using asset_owner with $inputs")?;
    let bindings = json!({"inputs":[{"ip":"10.0.0.1"},{"ip":"unknown"}]});
    let matched = lookup.execute(&tenant, bindings.as_object().unwrap(), None)?;
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"tenant":info.id,"query":result,"lookup":matched,"cache":tenant.dsl_cache_stats()})
        )?
    );
    Ok(())
}
