use std::{io::Read, path::PathBuf};
use tenant_kv::{
    Options, Store,
    http::{self, AuthConfig, HttpRuntime},
    telemetry::{self, Telemetry},
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("token-hash") => {
            let mut token = String::new();
            std::io::stdin().read_to_string(&mut token)?;
            let token = token.trim();
            if !(16..=512).contains(&token.len()) { return Err("token must contain 16..512 bytes".into()); }
            println!("{}", http::token_hash(token));
        }
        Some("openapi") => println!("{}", serde_json::to_string_pretty(&http::openapi())?),
        Some("serve") => {
            let mut root = PathBuf::from("data");
            let mut listen = "127.0.0.1:8080".to_string();
            let (mut auth, mut config, mut backup_config, mut replication_config, mut telemetry_config) = (None, None, None, None, None);
            while let Some(flag) = args.next() {
                let value = args.next().ok_or("missing flag value")?;
                match flag.as_str() {
                    "--root" => root = value.into(), "--listen" => listen = value,
                    "--auth" => auth = Some(value), "--config" => config = Some(value),
                    "--backup-config" => backup_config = Some(value),
                    "--replication-config" => replication_config = Some(value),
                    "--telemetry-config" => telemetry_config = Some(value),
                    _ => return Err(format!("unknown flag: {flag}").into()),
                }
            }
            let auth: AuthConfig = serde_json::from_slice(&std::fs::read(auth.ok_or("--auth is required")?)?)?;
            let options = if let Some(path) = config { serde_json::from_slice(&std::fs::read(path)?)? } else { Options::default() };
            let store = Store::open(&root, options)?;
            let transport_options = store.options().clone();
            let backups = if let Some(path) = backup_config { Some(tenant_kv::backup::BackupService::open(store.clone(), serde_json::from_slice(&std::fs::read(path)?)?)?) } else { None };
            let replication = if let Some(path) = replication_config { Some(tenant_kv::replication::Service::open(store.clone(), serde_json::from_slice(&std::fs::read(path)?)?)?) } else { None };
            let telemetry_config: telemetry::Config = if let Some(path) = telemetry_config { serde_json::from_slice(&std::fs::read(path)?)? } else { Default::default() };
            let telemetry = tokio::task::spawn_blocking(move || Telemetry::new(telemetry_config)).await??;
            let runtime = HttpRuntime::new(telemetry.clone());
            let router = http::app_with_runtime(store, auth, backups, replication.clone(), runtime.clone())?;
            let listener = tokio::net::TcpListener::bind(&listen).await?;
            let replication_worker = replication.as_ref().map(|service| service.start());
            eprintln!("tenant-kv listening on {}", listener.local_addr()?);
            let (draining, drained) = tokio::sync::oneshot::channel();
            let listener = http::Listener::new(listener, &transport_options);
            let server = listener.serve(router, async move {
                shutdown_signal().await;
                runtime.drain();
                let _ = draining.send(());
                eprintln!("tenant-kv draining requests");
                // Give health checks a visible not-ready interval before the
                // listener closes. Existing accepted work then has a deadline.
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            });
            tokio::pin!(server);
            tokio::select! {
                result = &mut server => result?,
                _ = drained => {
                    match tokio::time::timeout(std::time::Duration::from_secs(32), &mut server).await {
                        Ok(result) => result?,
                        Err(_) => {
                            eprintln!("tenant-kv request drain exceeded 30 seconds; terminating");
                            std::process::exit(1);
                        }
                    }
                }
            }
            if let Some(worker) = replication_worker { worker.abort(); let _ = worker.await; }
            if let Some(service) = replication { service.stop_initializations().await; }
            let result = tokio::task::spawn_blocking(move || telemetry.shutdown()).await?;
            if let Err(error) = result { eprintln!("{error}"); }
            eprintln!("tenant-kv shutdown complete");
        }
        _ => return Err("usage: tenant-kv serve --root DIR --auth FILE [--config FILE] [--backup-config FILE] [--replication-config FILE] [--telemetry-config FILE] [--listen ADDRESS] | token-hash | openapi".into()),
    }
    Ok(())
}
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
