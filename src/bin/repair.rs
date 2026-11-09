use tenant_kv::{Options, Store};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() == 2 && matches!(args[1].as_str(), "--version" | "-V") {
        println!("tenant-kv-repair {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    if args.len() == 2 && matches!(args[1].as_str(), "--help" | "-h") {
        println!(
            "usage: tenant-kv-repair ROOT TENANT_UUID WAL_UUID --at BYTES --archive NEW_DIRECTORY --accept-data-loss [--options OPTIONS_JSON]\nOffline WAL prefix repair. Changes history and requires replica rebuild. Archives the complete original database."
        );
        return Ok(());
    }
    if !matches!(args.len(), 9 | 11)
        || args[4] != "--at"
        || args[6] != "--archive"
        || args[8] != "--accept-data-loss"
        || (args.len() == 11 && args[9] != "--options")
    {
        return Err("usage: tenant-kv-repair ROOT TENANT_UUID WAL_UUID --at BYTES --archive NEW_DIRECTORY --accept-data-loss [--options OPTIONS_JSON] (offline; retain the prefix, change history and require replica rebuild)".into());
    }
    let options = if args.len() == 11 {
        serde_json::from_slice(&std::fs::read(&args[10])?)?
    } else {
        Options::default()
    };
    let result = Store::repair_wal(
        &args[1],
        options,
        args[2].parse()?,
        args[3].parse()?,
        args[5].parse()?,
        &args[7],
    )?;
    for field in ["warning", "report_warning"] {
        if let Some(warning) = result[field].as_str() {
            eprintln!("warning: {warning}");
        }
    }
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
