//! cargo run --example read_controls -- /tmp/tenant-kv-controlled-reads
use serde_json::json;
use tenant_kv::{
    client::{Client, ClientReadOptions, ReadPosition},
    http::{self, AuthConfig, Role, Token},
    *,
};

#[tokio::main]
async fn main() -> Result<()> {
    let root = std::env::args().nth(1).expect("database directory");
    let store = Store::open(root, Options::default())?;
    let id = store.create_tenant("lookup-reader")?.id;
    let tenant = store.tenant(id)?;
    tenant.create_collection("assets")?;
    let writes = tenant.write_batch(
        "assets",
        (0..5)
            .map(|n| WriteOp::Put {
                document: json!({"_key":format!("asset-{n}"),"owner":"platform"}),
                if_rev: None,
            })
            .collect(),
    )?;
    let controls = ReadOptions {
        timeout_ms: Some(1000),
        max_scan: Some(50),
        allow_full_scan: false,
        cancellation: None,
    };
    let query: Query = serde_json::from_value(
        json!({"filter":{"field":"/_key","op":"gte","value":"asset-0"},"select":["/_key","/owner"],"limit":2}),
    )?;
    let reader = tenant.read_with(&controls)?;
    println!("embedded plan: {:?}", reader.explain("assets", &query)?);
    let page = reader.query("assets", query.clone())?;
    println!("embedded: {}", serde_json::to_string(&page.documents)?);
    if let Some(cursor) = page.cursor {
        tenant.release_cursor(&cursor)?;
    }

    let secret = "local-example-read-token";
    let auth = AuthConfig {
        tokens: vec![Token {
            subject: "example".into(),
            sha256: http::token_hash(secret),
            tenant: Some(id),
            role: Role::Read,
        }],
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let app = http::app(store, auth)?;
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = Client::new(&[origin], secret.into(), id)?;
    let options = ClientReadOptions {
        minimum: Some(ReadPosition::from(writes.last().unwrap())),
        read: controls,
    };
    let first = client.query("assets", query.clone(), &options).await?;
    let next = client
        .query(
            "assets",
            Query {
                cursor: first.cursor,
                ..query
            },
            &options,
        )
        .await?;
    println!(
        "HTTP next page: {}",
        serde_json::to_string(&next.documents)?
    );
    if let Some(cursor) = next.cursor {
        client.release_cursor(&cursor).await?;
    }
    let cancellation = ReadCancellation::default();
    let reader = tenant.read_with(&ReadOptions {
        cancellation: Some(cancellation.clone()),
        ..Default::default()
    })?;
    cancellation.cancel();
    assert_eq!(
        reader.get("assets", "asset-0").unwrap_err().code,
        "CANCELLED"
    );
    server.abort();
    Ok(())
}
