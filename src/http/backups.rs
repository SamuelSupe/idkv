use super::*;
use crate::backup::{BackupOperation, BackupService};

pub(super) fn is_backup_route(route: &str) -> bool {
    route.contains("/remote-backups") || route.contains("/backup-jobs")
}
pub(super) fn is_job_post(route: &str, method: &str) -> bool {
    is_backup_route(route) && method == "POST"
}
pub(super) fn dispatch(
    service: Option<&BackupService>,
    route: &str,
    params: &HashMap<String, String>,
    method: &Method,
    tenant: Uuid,
    body: Value,
) -> Result<Value> {
    let service =
        service.ok_or_else(|| Error::new("UNAVAILABLE", "remote backups are not configured"))?;
    let id = |name: &str| -> Result<Uuid> {
        params
            .get(name)
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| Error::invalid(format!("{name} UUID")))
    };
    if *method != Method::GET && !route.ends_with("/prune") && body != json!({}) {
        return Err(Error::invalid("this backup operation takes an empty body"));
    }
    if route.ends_with("/remote-backups") {
        return if *method == Method::GET {
            value(service.list(tenant)?)
        } else {
            value(service.submit(tenant, BackupOperation::Create)?)
        };
    }
    if route.ends_with("/backup-jobs") {
        return value(service.jobs(tenant));
    }
    if route.ends_with("/backup-jobs/{job}") {
        return if *method == Method::GET {
            value(service.job(tenant, id("job")?)?)
        } else {
            service.forget(tenant, id("job")?)?;
            Ok(json!({"ok":true}))
        };
    }
    if route.ends_with("/retry") {
        return value(service.retry(tenant, id("job")?)?);
    }
    let operation = if route.ends_with("/verify") {
        BackupOperation::Verify {
            backup: id("backup")?,
        }
    } else if route.ends_with("/delete") {
        BackupOperation::Delete {
            backup: id("backup")?,
        }
    } else if route.ends_with("/restore") {
        BackupOperation::Restore {
            backup: id("backup")?,
        }
    } else {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Prune {
            keep_last: usize,
        }
        BackupOperation::Prune {
            keep_last: decode::<Prune>(body)?.keep_last,
        }
    };
    value(service.submit(tenant, operation)?)
}

pub(super) fn schemas() -> Value {
    json!({
        "RemoteBackup":{"type":"object","required":["backup","tenant","generation","checkpoint_seq","checkpoint_ms","total_bytes","files"],"properties":{
            "backup":{"type":"string","format":"uuid"},"tenant":{"type":"string","format":"uuid"},"generation":{"type":"string","format":"uuid"},
            "checkpoint_seq":{"type":"integer","minimum":0},"checkpoint_ms":{"type":"integer","minimum":0},"total_bytes":{"type":"integer","minimum":0},"files":{"type":"integer","minimum":1}}},
        "BackupPrune":{"type":"object","additionalProperties":false,"required":["keep_last"],"properties":{"keep_last":{"type":"integer","minimum":1,"maximum":4096}}},
        "BackupJob":{"type":"object","required":["id","tenant","backup","operation","state","created_ms","updated_ms","uploaded_bytes","reused_bytes","processed_files"],"properties":{
            "id":{"type":"string","format":"uuid"},"tenant":{"type":"string","format":"uuid"},"backup":{"type":"string","format":"uuid"},
            "operation":{"type":"object","required":["operation"],"properties":{"operation":{"enum":["create","verify","restore","import","prune","delete"]},"backup":{"type":"string","format":"uuid"},"keep_last":{"type":"integer","minimum":1,"maximum":4096}}},
            "state":{"enum":["queued","checkpointing","uploading","verifying","restoring","pruning","completed","failed","interrupted"]},
            "created_ms":{"type":"integer"},"updated_ms":{"type":"integer"},"uploaded_bytes":{"type":"integer"},"reused_bytes":{"type":"integer"},"processed_files":{"type":"integer"},
            "checkpoint_seq":{"type":["integer","null"]},"result":{"type":["object","null"]},"error":{"type":["string","null"]}},
            "description":"202 means queued, not remotely durable. Only completed creates represent published backups. Failed/interrupted restores require inspection before a new explicit restore."}
    })
}
