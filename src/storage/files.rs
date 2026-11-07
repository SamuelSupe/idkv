use crate::{Error, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
};
use uuid::Uuid;

pub fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}
pub fn reject_symlink(path: &Path) -> Result<()> {
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(Error::invalid(
            "symbolic links are not allowed in database paths",
        ));
    }
    Ok(())
}
pub fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let payload = serde_json::to_vec(value)?;
    if payload.len() > (16 << 20) - 8 {
        return Err(Error::limit("metadata exceeds format size bound"));
    }
    let mut bytes = b"TKJ1".to_vec();
    bytes.extend(crc32fast::hash(&payload).to_le_bytes());
    bytes.extend(payload);
    atomic_bytes(path, &bytes)
}
pub fn atomic_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::invalid("missing parent"))?;
    let temp = parent.join(format!(".tmp-{}", Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        sync_dir(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}
pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    reject_symlink(path)?;
    if fs::metadata(path)?.len() > 16 << 20 {
        return Err(Error::corrupt("metadata exceeds size bound"));
    }
    let bytes = fs::read(path)?;
    if bytes.len() < 8
        || &bytes[..4] != b"TKJ1"
        || crc32fast::hash(&bytes[8..]) != u32::from_le_bytes(bytes[4..8].try_into().unwrap())
    {
        return Err(Error::corrupt(format!(
            "metadata checksum: {}",
            path.display()
        )));
    }
    serde_json::from_slice(&bytes[8..]).map_err(|e| Error::corrupt(e.to_string()))
}

/// Copy in bounded jobs so backup/restore cannot monopolize a shared I/O worker.
pub(crate) fn copy_file(
    source: &Path,
    destination: &Path,
    tenant: Uuid,
    budget: &crate::resources::Budget,
    io: &crate::resources::IoScheduler,
) -> Result<()> {
    reject_symlink(source)?;
    copy_open_file(
        std::sync::Arc::new(File::open(source)?),
        destination,
        tenant,
        budget,
        io,
    )
}

pub(crate) fn copy_open_file(
    source: std::sync::Arc<File>,
    destination: &Path,
    tenant: Uuid,
    budget: &crate::resources::Budget,
    io: &crate::resources::IoScheduler,
) -> Result<()> {
    copy_open_file_limited(source, destination, tenant, budget, io, 0)
}
pub(crate) fn copy_open_file_limited(
    source: std::sync::Arc<File>,
    destination: &Path,
    tenant: Uuid,
    budget: &crate::resources::Budget,
    io: &crate::resources::IoScheduler,
    reserve_free: u64,
) -> Result<()> {
    use std::os::unix::fs::FileExt;
    let _charge = budget.reserve(256 << 10)?;
    let mut target = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let size = source.metadata()?.len();
    let mut offset = 0;
    while offset < size {
        let count = (size - offset).min(256 << 10) as usize;
        if fs2::available_space(destination)? < reserve_free.saturating_add(count as u64) {
            return Err(Error::limit("checkpoint copy free disk reserve"));
        }
        let input = source.clone();
        target = io.run(tenant, move || {
            let mut buffer = vec![0; count];
            input.read_exact_at(&mut buffer, offset)?;
            target.write_all(&buffer)?;
            Ok(target)
        })?;
        offset += count as u64;
    }
    io.run(tenant, move || {
        target.sync_all()?;
        Ok(())
    })
}
