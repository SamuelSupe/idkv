mod cache;
mod engine;
mod files;
mod sst;
mod wal;

pub(crate) use engine::LogRetention;
pub(crate) use engine::ScanRange;
pub(crate) use engine::directory_size;
pub use engine::{Engine, Scan, Snapshot, Transaction};
pub(crate) use files::copy_file;
pub(crate) use files::copy_open_file;
pub use files::{atomic_json, read_json, reject_symlink, sync_dir};
pub use sst::Record;

pub(crate) fn failpoint(name: &str) {
    #[cfg(feature = "failpoints")]
    if std::env::var("TKV_FAILPOINT").ok().as_deref() == Some(name) {
        std::process::abort();
    }
    let _ = name;
}

pub(crate) fn errorpoint(name: &str) -> crate::Result<()> {
    #[cfg(feature = "failpoints")]
    {
        use std::sync::atomic::{AtomicBool, Ordering};
        static FIRED: AtomicBool = AtomicBool::new(false);
        if std::env::var("TKV_ERRORPOINT").ok().as_deref() == Some(name)
            && !FIRED.swap(true, Ordering::AcqRel)
        {
            return Err(crate::Error::limit("injected temporary output failure"));
        }
    }
    let _ = name;
    Ok(())
}
