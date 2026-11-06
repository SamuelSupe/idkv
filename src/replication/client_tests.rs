use super::*;
use crate::Options;
use std::{os::unix::fs::PermissionsExt, sync::mpsc};

fn primary(options: Options) -> (tempfile::TempDir, Store, Service) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), options).unwrap();
    let token = dir.path().join("peer-token");
    fs::write(&token, "replication-unit-test-secret").unwrap();
    let service = Service::open(
        store.clone(),
        Config {
            cluster: Uuid::new_v4(),
            role: Role::Primary,
            peer_url: "http://127.0.0.1:1".into(),
            token_file: token,
            poll_ms: 10,
            log_retention_bytes: 1 << 20,
        },
    )
    .unwrap();
    (dir, store, service)
}

#[tokio::test]
async fn checkpoint_preparation_does_not_lock_out_other_tenant_log_progress() {
    let (_dir, store, service) = primary(Options {
        io_workers: 1,
        ..Default::default()
    });
    let good = store.create_tenant("catching-up").unwrap();
    let slow = store.create_tenant("preparing").unwrap();
    let slow_tenant = store.tenant(slow.id).unwrap();
    slow_tenant
        .engine()
        .transact(|tx| tx.put(b"checkpoint-file".to_vec(), vec![42; 1024]))
        .unwrap();
    slow_tenant.engine().flush().unwrap();
    let state = store.replication_state();
    let snapshot = service
        .snapshot(state.cluster, state.epoch, good.id)
        .unwrap();
    let tenant = store.tenant(good.id).unwrap();
    tenant
        .engine()
        .transact(|tx| tx.put(b"ack".to_vec(), b"value".to_vec()))
        .unwrap();
    let request = LogRequest {
        protocol: super::super::PROTOCOL,
        cluster: state.cluster,
        epoch: state.epoch,
        tenant: good.id,
        generation: good.generation,
        after: snapshot.position,
        cursor: None,
    };
    assert!(service.export_log(&request).unwrap().through > snapshot.position);
    let (_, _, io) = store.backup_context(slow.id).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let blocker = io
        .submit(slow.id, move || {
            entered_tx.send(()).unwrap();
            let _ = release_rx.recv();
            Ok(())
        })
        .unwrap();
    entered_rx.recv().unwrap();
    let preparing = service.clone();
    let worker =
        std::thread::spawn(move || preparing.snapshot(state.cluster, state.epoch, slow.id));
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut reserved = false;
    while Instant::now() < deadline {
        reserved = service
            .inner
            .exports
            .try_lock()
            .is_some_and(|exports| exports.generating.is_some());
        if reserved {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let progressing = service.clone();
    let (tx, rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let duplicate = progressing
            .snapshot(state.cluster, state.epoch, slow.id)
            .err()
            .unwrap()
            .code;
        let page = progressing.export_log(&LogRequest {
            after: tenant.engine().seq(),
            ..request
        });
        tx.send((duplicate, page)).unwrap();
    });
    let progressed = rx.recv_timeout(Duration::from_secs(1));
    release_tx.send(()).unwrap();
    blocker.wait().unwrap();
    reader.join().unwrap();
    let generated = worker.join().unwrap().unwrap();
    assert!(reserved, "preparation held the export state lock");
    let (duplicate, page) = progressed.expect("other tenant could not renew while preparing");
    assert_eq!(duplicate, "BUSY");
    assert!(page.unwrap().frames.is_empty());
    service.release_snapshot(generated.id).unwrap();
    service.expire_exports().unwrap();
    assert!(service.inner.exports.lock().garbage.is_empty());
}

#[tokio::test]
async fn export_cleanup_failure_does_not_fail_logs_and_is_retried() {
    let (_dir, store, service) = primary(Options::default());
    let info = store.create_tenant("cleanup-retry").unwrap();
    let other = store.create_tenant("next-export").unwrap();
    let state = store.replication_state();
    let snapshot = service
        .snapshot(state.cluster, state.epoch, info.id)
        .unwrap();
    let path = store
        .root()
        .join("replication-work/export")
        .join(snapshot.id.to_string());
    let permissions = fs::metadata(&path).unwrap().permissions();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
    let page = service.export_log(&LogRequest {
        protocol: super::super::PROTOCOL,
        cluster: state.cluster,
        epoch: state.epoch,
        tenant: info.id,
        generation: info.generation,
        after: snapshot.position,
        cursor: None,
    });
    let failed = service.expire_exports();
    let queued = service.inner.exports.lock().garbage.contains(&path);
    fs::set_permissions(&path, permissions).unwrap();
    assert!(
        page.unwrap().frames.is_empty(),
        "cleanup changed a valid log result"
    );
    assert!(
        failed.is_err() && queued,
        "cleanup did not retain the failed path"
    );
    let next = service
        .snapshot(state.cluster, state.epoch, other.id)
        .unwrap();
    service.expire_exports().unwrap();
    assert!(!path.exists());
    service.release_snapshot(next.id).unwrap();
    service.expire_exports().unwrap();
}

#[tokio::test]
async fn failed_preparation_releases_the_slot_for_a_new_export() {
    let (_dir, store, service) = primary(Options::default());
    let state = store.replication_state();
    assert_eq!(
        service
            .snapshot(state.cluster, state.epoch, Uuid::new_v4())
            .err()
            .unwrap()
            .code,
        "NOT_FOUND"
    );
    let info = store.create_tenant("retry-export").unwrap();
    let snapshot = service
        .snapshot(state.cluster, state.epoch, info.id)
        .unwrap();
    service.release_snapshot(snapshot.id).unwrap();
    service.expire_exports().unwrap();
    assert!(service.inner.exports.lock().garbage.is_empty());
}

#[tokio::test]
async fn unwinding_checkpoint_preparation_releases_the_reserved_export() {
    let (_dir, store, service) = primary(Options::default());
    let info = store.create_tenant("panic-preparation").unwrap();
    let state = store.replication_state();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let super::super::snapshot::Start::Build(preparation) = service
            .inner
            .exports
            .lock()
            .begin(&store, state.cluster, state.epoch, info.id)
            .unwrap()
        else {
            panic!("expected reservation")
        };
        let _pending = PendingExport {
            exports: &service.inner.exports,
            preparation: Some(preparation),
        };
        panic!("injected preparation panic");
    }));
    assert!(unwound.is_err());
    let snapshot = service
        .snapshot(state.cluster, state.epoch, info.id)
        .unwrap();
    service.release_snapshot(snapshot.id).unwrap();
    service.expire_exports().unwrap();
    assert!(service.inner.exports.lock().garbage.is_empty());
}
