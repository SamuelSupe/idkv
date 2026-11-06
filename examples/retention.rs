//! One tenant duration; current documents survive receipt expiry.
use serde_json::json;
use tenant_kv::*;

fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .ok_or_else(|| Error::invalid("retention NEW_ROOT"))?;
    let store = Store::open(root, Options::default())?;
    let tenant = store.tenant(store.create_tenant("retention-demo")?.id)?;
    tenant.create_collection("assets")?;
    tenant.set_retention(2)?;
    let context = tenant.retry_context("example-worker")?;
    let options = WriteOptions {
        expected_history: Some(context.history),
        operation_id: Some("create-asset-1".into()),
        retry_token: Some(context.retry_token),
    };
    let batch = vec![WriteOp::Put {
        document: json!({"owner":"platform"}),
        if_rev: None,
    }];
    let first = tenant.write_batch_with("assets", batch.clone(), &options, "example-worker")?;
    let replay = tenant.write_batch_with("assets", batch.clone(), &options, "example-worker")?;
    assert_eq!(first[0].key, replay[0].key);
    std::thread::sleep(std::time::Duration::from_millis(2200));
    let expired = tenant
        .write_batch_with("assets", batch, &options, "example-worker")
        .unwrap_err();
    assert_eq!(expired.code, "IDEMPOTENCY_EXPIRED");
    assert!(tenant.get("assets", &first[0].key)?.is_some());
    println!(
        "{}",
        json!({"same_key_on_retry":true,"expired_retry":expired.code,"current_document_retained":true,"retention":tenant.retention_status()?})
    );
    Ok(())
}
