//! Run a complete OpenDAL backup/verify/restore workflow against an administrator
//! supplied S3 or fs configuration: remote_backup ROOT CONFIG [ROWS].
use serde_json::json;
use std::{
    fs,
    time::{Duration, Instant},
};
use tenant_kv::{backup::*, *};

fn finish(
    service: &BackupService,
    id: uuid::Uuid,
    operation: BackupOperation,
) -> Result<BackupJob> {
    let started = Instant::now();
    let job = loop {
        match service.submit(id, operation.clone()) {
            Ok(job) => break job,
            Err(e) if e.code == "BUSY" && started.elapsed() < Duration::from_secs(10) => {
                std::thread::sleep(Duration::from_millis(20))
            }
            Err(e) => return Err(e),
        }
    };
    loop {
        let current = service.job(id, job.id)?;
        match current.state {
            JobState::Completed => return Ok(current),
            JobState::Failed | JobState::Interrupted => {
                return Err(Error::new(
                    "BACKUP_FAILED",
                    current.error.unwrap_or_default(),
                ));
            }
            _ => {}
        }
        if started.elapsed() > Duration::from_secs(600) {
            return Err(Error::new("DEADLINE_EXCEEDED", "example timeout"));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 3 {
        return Err(Error::invalid("remote_backup ROOT CONFIG [ROWS]"));
    }
    let rows: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(2000);
    let store = Store::open(
        &args[1],
        Options {
            memtable_bytes: 64 << 20,
            target_sst_bytes: 32 << 20,
            ..Default::default()
        },
    )?;
    let id = store
        .create_tenant(&format!("remote-{}", uuid::Uuid::new_v4()))?
        .id;
    let tenant = store.tenant(id)?;
    tenant.create_collection("assets")?;
    let mut random = 53u64;
    for base in (0..rows).step_by(100) {
        let ops = (base..(base + 100).min(rows))
            .map(|i| {
                let payload: String = (0..8192)
                    .map(|_| {
                        random ^= random << 13;
                        random ^= random >> 7;
                        random ^= random << 17;
                        char::from(33 + (random % 90) as u8)
                    })
                    .collect();
                WriteOp::Put {
                    document: json!({"_key":i.to_string(),"payload":payload,"owner":"before"}),
                    if_rev: None,
                }
            })
            .collect();
        tenant.write_batch("assets", ops)?;
    }
    drop(tenant);
    let config: BackupConfig = serde_json::from_slice(&fs::read(&args[2])?)?;
    let service = BackupService::open(store.clone(), config)?;
    let first = finish(&service, id, BackupOperation::Create)?;
    let second = finish(&service, id, BackupOperation::Create)?;
    if second.reused_bytes == 0 {
        return Err(Error::corrupt("incremental backup did not reuse objects"));
    }
    finish(
        &service,
        id,
        BackupOperation::Verify {
            backup: second.backup,
        },
    )?;
    store
        .tenant(id)?
        .put("assets", json!({"_key":"0","owner":"after"}), None)?;
    finish(
        &service,
        id,
        BackupOperation::Restore {
            backup: second.backup,
        },
    )?;
    let restored = store.tenant(id)?.get("assets", "0")?.unwrap();
    if restored["owner"] != "before" || restored["payload"].as_str().unwrap().len() != 8192 {
        return Err(Error::corrupt("remote restore data mismatch"));
    }
    let pruned = finish(&service, id, BackupOperation::Prune { keep_last: 1 })?;
    finish(
        &service,
        id,
        BackupOperation::Verify {
            backup: second.backup,
        },
    )?;
    println!(
        "{}",
        json!({"result":"passed","tenant":id,"rows":rows,"first":first,"second":second,"prune":pruned.result,"restored":true})
    );
    Ok(())
}
