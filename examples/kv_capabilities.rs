//! Business code validates its own rules; KV checks versions and commits state.
use serde_json::json;
use tenant_kv::*;
use uuid::Uuid;

fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .expect("usage: kv_capabilities NEW_DATA_DIRECTORY");
    let store = Store::open(root, Options::default())?;
    let tenant = store.tenant(store.create_tenant("kv-example")?.id)?;
    tenant.create_collection("state")?;
    tenant.set_retention(3600)?;
    tenant.put("state", json!({"_key":"rules","enabled":true}), Some(0))?;

    let read = tenant.read_batch(&[
        ReadOperation {
            collection: "state".into(),
            request: ReadRequest::Get {
                key: "rules".into(),
            },
        },
        ReadOperation {
            collection: "state".into(),
            request: ReadRequest::Get {
                key: "progress".into(),
            },
        },
    ])?;
    let ReadResult::Get {
        document: Some(rules),
    } = &read.results[0]
    else {
        unreachable!()
    };
    assert_eq!(rules["enabled"], true);
    let retry = tenant.retry_context("loader")?;
    let operations = vec![
        WriteOp::Check {
            key: "rules".into(),
            if_rev: rules["_rev"].as_u64().unwrap(),
        },
        WriteOp::Check {
            key: "progress".into(),
            if_rev: 0,
        },
        WriteOp::Put {
            document: json!({"_key":"record-1","value":"ready"}),
            if_rev: Some(0),
        },
        WriteOp::Put {
            document: json!({"_key":"progress","offset":1}),
            if_rev: Some(0),
        },
    ];
    let options = WriteOptions {
        expected_history: Some(read.history),
        operation_id: Some("load-1".into()),
        retry_token: Some(retry.retry_token),
    };
    let written = tenant.write_batch_with("state", operations.clone(), &options, "loader")?;
    let replay = tenant.write_batch_with("state", operations, &options, "loader")?;
    assert_eq!(written[0].rev, replay[0].rev);

    let published = tenant.publish_dataset_with(
        "catalog",
        Publication {
            version: Uuid::new_v4(),
            collection: "state".into(),
            lookup: String::new(),
            expected_version: None,
            retry_token: Some(retry.retry_token),
        },
        "loader",
    )?;
    let result = tenant.read_published(
        "catalog",
        Some(published.version),
        &[
            ReadRequest::Get {
                key: "record-1".into(),
            },
            ReadRequest::MultiGet {
                keys: vec!["progress".into(), "missing".into()],
            },
            ReadRequest::Query {
                query: Query::default(),
            },
        ],
    )?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
