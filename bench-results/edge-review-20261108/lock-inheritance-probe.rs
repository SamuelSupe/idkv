fn main() {
    let root = std::path::PathBuf::from(format!("/var/tmp/idkv-lock-inheritance-{}", std::process::id()));
    let store = tenant_kv::Store::open(&root, Default::default()).unwrap();
    let id = store.create_tenant("probe").unwrap().id;
    let t = store.tenant(id).unwrap();
    t.create_collection("docs").unwrap();
    let confirmed = t.put("docs", serde_json_value(), None).unwrap();
    drop(t);
    // Child performs only async-signal-safe syscalls until it exits. It retains
    // the inherited descriptors, modelling the pre-exec interval of a spawn.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 { unsafe { libc::usleep(800_000); libc::_exit(0) }; }
    drop(store);
    let error = tenant_kv::Store::open(&root, Default::default()).err().unwrap();
    assert_eq!(error.code, "BUSY");
    println!("parent released all handles, child retains inherited descriptors: {}", error.code);
    let mut status = 0;
    assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
    let store = tenant_kv::Store::open(&root, Default::default()).unwrap();
    assert_eq!(store.tenant(id).unwrap().get("docs", "ack").unwrap().unwrap()["_rev"], confirmed.rev);
    println!("after child exit: reopened, acknowledged revision {} verified", confirmed.rev);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
fn serde_json_value() -> serde_json::Value { serde_json::json!({"_key":"ack"}) }
