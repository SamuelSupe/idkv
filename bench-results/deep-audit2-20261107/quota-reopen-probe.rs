
#[cfg(test)]
mod audit_quota_reopen {
    use super::*;
    #[test]
    fn audit_lowering_quota_accepts_a_wal_that_cannot_be_reopened() {
        let dir = tempfile::Builder::new().prefix("idkv-quota-audit-").tempdir_in("/var/tmp").unwrap();
        let options = Options { global_memory_bytes:64<<20,tenant_memory_bytes:8<<20,
            memtable_bytes:1<<20,cache_bytes:0,..Options::default() };
        let store = Store::open(dir.path(),options.clone()).unwrap();
        // Exclude maintenance races: the WAL itself, not incomplete compaction,
        // must be sufficient to retain already-confirmed writes across a quota change.
        store.owner.stop.store(true, Ordering::Release);
        for worker in store.owner.workers.lock().drain(..) {worker.join().unwrap();}
        let id = store.create_tenant("quota-reopen").unwrap().id;
        let source=store.tenant(id).unwrap();
        source.create_collection("docs").unwrap();
        let ack=source.put("docs",serde_json::json!({"_key":"large","payload":"x".repeat(700<<10)}),None).unwrap();
        let doc=source.get("docs","large").unwrap().unwrap();
        assert_eq!(doc["_rev"],ack.rev);
        let before=source.engine().memory_bytes();
        drop(source);
        let changed=store.set_quota(id,2<<20,100<<30).unwrap();
        let error=store.tenant(id).err().expect("new quota unexpectedly opened unflushed data");
        println!("AUDIT healthy pre-change memory={before}, quota response memory={}, ACK={}, open_after={error}",changed.memory_bytes,ack.rev);
        assert_eq!(error.code,"RESOURCE_EXHAUSTED");
        drop(store);
        let store=Store::open(dir.path(),options).unwrap();
        let after_restart=store.tenant(id).err().unwrap();
        assert_eq!(after_restart.code,"RESOURCE_EXHAUSTED");
        println!("AUDIT reopen Store with persisted reduced quota still fails: {after_restart}");
        store.set_quota(id,8<<20,100<<30).unwrap();
        let reopened=store.tenant(id).unwrap();
        assert_eq!(Some(doc),reopened.get("docs","large").unwrap());
        println!("AUDIT expanding back to 8MiB restores all confirmed content and revision");
    }
}
