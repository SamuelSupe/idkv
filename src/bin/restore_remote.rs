use std::{fs, time::Duration};
use tenant_kv::{
    Options, Store,
    backup::{BackupConfig, BackupOperation, BackupService, JobState},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 5 {
        return Err("usage: tenant-kv-restore ROOT BACKUP_CONFIG TENANT_UUID BACKUP_UUID (offline import into an unregistered tenant UUID)".into());
    }
    let config: BackupConfig = serde_json::from_slice(&fs::read(&args[2])?)?;
    let store = Store::open(&args[1], Options::default())?;
    let service = BackupService::open(store, config)?;
    let tenant = args[3].parse()?;
    let job = service.submit(
        tenant,
        BackupOperation::Import {
            backup: args[4].parse()?,
        },
    )?;
    loop {
        let current = service.job(tenant, job.id)?;
        match current.state {
            JobState::Completed => {
                println!("{}", serde_json::to_string(&current)?);
                return Ok(());
            }
            JobState::Failed | JobState::Interrupted => {
                return Err(current
                    .error
                    .unwrap_or_else(|| "import failed".into())
                    .into());
            }
            _ => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}
