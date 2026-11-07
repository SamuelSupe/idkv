use tenant_kv::{Options,Store};
use std::{fs,os::unix::fs::PermissionsExt};
#[test]
fn audit_failed_tenant_creation_leaks_unregistered_databases_across_restart() {
    let dir=tempfile::Builder::new().prefix("idkv-orphan-audit-").tempdir_in("/var/tmp").unwrap();
    let store=Store::open(dir.path(),Options::default()).unwrap();
    let control=dir.path().join("control");
    let mode=fs::metadata(&control).unwrap().permissions();
    fs::set_permissions(&control,fs::Permissions::from_mode(0o500)).unwrap();
    let errors:Vec<_>=(0..8).map(|n|store.create_tenant(&format!("failed-{n}")).unwrap_err()).collect();
    fs::set_permissions(&control,mode).unwrap();
    assert!(errors.iter().all(|e| e.code=="IO_ERROR"),"{errors:?}");
    let before=fs::read_dir(dir.path().join("tenants")).unwrap().count();
    assert!(store.tenants().is_empty());
    drop(store);
    let reopened=Store::open(dir.path(),Options::default()).unwrap();
    let after=fs::read_dir(dir.path().join("tenants")).unwrap().count();
    assert!(reopened.tenants().is_empty());
    println!("AUDIT failed_creates={} registered=0 database_directories_before_restart={before} after_restart={after} errors={errors:?}",errors.len());
    assert_eq!((before,after),(8,8));
    reopened.create_tenant("healthy-after-fault").unwrap();
    assert_eq!(reopened.tenants().len(),1);
    assert_eq!(fs::read_dir(dir.path().join("tenants")).unwrap().count(),9);
}
