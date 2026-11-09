use super::{
    files::{reject_symlink, sync_dir},
    sst::Record,
};
use crate::resources::Bytes;
use crate::{
    Error, Result,
    codec::{Reader, put_bytes, put_u32, put_u64},
    resources::IoScheduler,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};
use uuid::Uuid;

static CORRUPT_RECOVERIES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static TORN_TAIL_RECOVERIES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub(crate) fn recovery_metrics() -> [(&'static str, u64); 2] {
    use std::sync::atomic::Ordering;
    [
        (
            "wal_corrupt_recoveries_total",
            CORRUPT_RECOVERIES.load(Ordering::Relaxed),
        ),
        (
            "wal_torn_tail_recoveries_total",
            TORN_TAIL_RECOVERIES.load(Ordering::Relaxed),
        ),
    ]
}

fn preserve_wal(dir: &Path, info: &WalInfo) -> Result<std::path::PathBuf> {
    use std::os::unix::fs::MetadataExt;
    // A separate name preserves the old inode without a WAL-sized copy on a
    // nearly full disk. Repeated failed opens must reuse that evidence name.
    let source = dir.join(format!("{}.wal", info.id));
    let metadata = fs::metadata(&source)?;
    let evidence = dir.join(format!(
        "{}.{}-{}.corrupt",
        info.id,
        metadata.dev(),
        metadata.ino()
    ));
    match fs::hard_link(&source, &evidence) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            reject_symlink(&evidence)?;
            let previous = fs::metadata(&evidence)?;
            if previous.dev() != metadata.dev() || previous.ino() != metadata.ino() {
                return Err(Error::corrupt(
                    "WAL evidence name collision; original retained",
                ));
            }
        }
        Err(error) => return Err(error.into()),
    }
    sync_dir(dir)?;
    Ok(evidence)
}

fn preserve_torn_tail(
    dir: &Path,
    info: &WalInfo,
    file: &mut File,
    offset: u64,
    length: u64,
) -> Result<()> {
    // Only the discarded suffix is evidence for a length-truncated append. Keep
    // one bounded entry per tenant rather than another copy of every valid WAL.
    use std::os::unix::fs::OpenOptionsExt;
    let temp = dir.join("TORN-TAIL.tmp");
    match fs::symlink_metadata(&temp) {
        Ok(_) => reject_symlink(&temp)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut output = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temp)?;
    writeln!(
        output,
        "{}",
        serde_json::json!({"wal": info.id, "offset": offset, "length": length})
    )?;
    file.seek(SeekFrom::Start(offset))?;
    let mut buffer = [0; 8192];
    let mut remaining = length - offset;
    while remaining != 0 {
        let size = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..size])?;
        output.write_all(&buffer[..size])?;
        remaining -= size as u64;
    }
    output.sync_all()?;
    fs::rename(temp, dir.join("TORN-TAIL"))?;
    sync_dir(dir)
}

fn corrupt_wal(dir: &Path, info: &WalInfo, offset: u64, reason: &str) -> Error {
    CORRUPT_RECOVERIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let evidence = match preserve_wal(dir, info) {
        Ok(path) => format!("evidence {}", path.display()),
        Err(error) => format!("evidence link failed: {error}; original WAL unchanged"),
    };
    eprintln!(
        "ERROR WAL recovery refused: {} offset {offset}: {reason}; {evidence}",
        info.id
    );
    Error::corrupt(format!(
        "{reason} at byte {offset}; original WAL retained; restore or rebuild this tenant"
    ))
}

pub(super) const HEADER: usize = 64;
pub const MAX_BATCH: usize = 64 << 20;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalInfo {
    pub id: Uuid,
    pub start_seq: u64,
}
pub struct Wal {
    pub info: WalInfo,
    pub file: Arc<File>,
    pub len: u64,
    pub recovery_read_only: bool,
}
impl Wal {
    pub fn create(dir: &Path, tenant: Uuid, generation: Uuid, start_seq: u64) -> Result<Self> {
        let info = WalInfo {
            id: Uuid::new_v4(),
            start_seq,
        };
        let path = dir.join(format!("{}.wal", info.id));
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(&path)?;
        file.write_all(b"TKWAL001")?;
        file.write_all(tenant.as_bytes())?;
        file.write_all(generation.as_bytes())?;
        file.write_all(info.id.as_bytes())?;
        file.write_all(&start_seq.to_le_bytes())?;
        file.sync_all()?;
        sync_dir(dir)?;
        Ok(Self {
            info,
            file: Arc::new(file),
            len: HEADER as u64,
            recovery_read_only: false,
        })
    }
    pub fn recover(
        dir: &Path,
        info: &WalInfo,
        tenant: &crate::TenantInfo,
        tail: bool,
        synced_len: Option<u64>,
        budget: &crate::resources::Budget,
        mut apply: impl FnMut(u64, Vec<Record>) -> Result<()>,
    ) -> Result<Self> {
        let path = dir.join(format!("{}.wal", info.id));
        reject_symlink(&path)?;
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let length = file.metadata()?.len();
        let mut header = [0; HEADER];
        file.read_exact(&mut header)
            .map_err(|_| Error::corrupt("truncated WAL header"))?;
        if &header[..8] != b"TKWAL001"
            || &header[8..24] != tenant.id.as_bytes()
            || &header[24..40] != tenant.generation.as_bytes()
            || &header[40..56] != info.id.as_bytes()
            || u64::from_le_bytes(header[56..].try_into().unwrap()) != info.start_seq
        {
            return Err(Error::corrupt("WAL identity or version mismatch"));
        }
        if synced_len.is_some_and(|synced| length < synced) {
            return Err(Error::corrupt("WAL shorter than its synced prefix"));
        }
        let fully_synced = synced_len == Some(length);
        let mut offset = HEADER as u64;
        while offset < length {
            // The clean-close watermark does not identify all acknowledged
            // commits. Only incomplete frames are eligible for tail recovery;
            // checksum failures can be corruption of acknowledged data.
            let unsealed_tail = tail && offset >= synced_len.unwrap_or(HEADER as u64);
            if length - offset < 24 {
                if unsealed_tail {
                    break;
                }
                return Err(Error::corrupt("truncated WAL batch header"));
            }
            let mut frame = [0; 24];
            file.read_exact(&mut frame)?;
            if &frame[..4] != b"TXN1"
                || crc32fast::hash(&frame[..16])
                    != u32::from_le_bytes(frame[16..20].try_into().unwrap())
            {
                return Err(corrupt_wal(dir, info, offset, "WAL batch header checksum"));
            }
            let len = u32::from_le_bytes(frame[4..8].try_into().unwrap()) as usize;
            let seq = u64::from_le_bytes(frame[8..16].try_into().unwrap());
            if len > MAX_BATCH || seq < info.start_seq {
                return Err(Error::corrupt("WAL batch bounds"));
            }
            if length - offset - 24 < len as u64 {
                if unsealed_tail {
                    break;
                }
                return Err(Error::corrupt("truncated WAL batch payload"));
            }
            let _payload_memory = budget.reserve(len)?;
            let mut payload = vec![0; len];
            file.read_exact(&mut payload)?;
            if crc32fast::hash(&payload) != u32::from_le_bytes(frame[20..24].try_into().unwrap()) {
                return Err(corrupt_wal(dir, info, offset, "WAL batch checksum"));
            }
            let count = Reader::new(&payload).u32()? as usize;
            if count > payload.len() / 12 {
                return Err(Error::corrupt("WAL entry count"));
            }
            let _decoded_memory =
                budget.reserve(len + count * (std::mem::size_of::<Record>() + 96))?;
            let records = decode(&payload, seq)?;
            if fully_synced {
                apply(seq, records)?;
            }
            offset += 24 + len as u64;
        }
        if offset < length {
            preserve_torn_tail(dir, info, &mut file, offset, length)?;
            eprintln!(
                "ERROR incomplete WAL tail: {} offset {offset}, suffix bytes {}; saved in wal/TORN-TAIL",
                info.id,
                length - offset,
            );
        }
        let temp = dir.join(format!("{}.recover", info.id));
        if temp.exists() {
            reject_symlink(&temp)?;
            fs::remove_file(&temp)?;
        }
        if synced_len == Some(length) && offset == length {
            return Ok(Self {
                info: info.clone(),
                file: Arc::new(file),
                len: offset,
                recovery_read_only: false,
            });
        }
        // Retrying fsync on the old inode is insufficient after writeback errors:
        // clean page-cache data can survive without being durable. Fresh writes
        // to a new inode re-establish durability, including across process opens.
        let _memory = budget.reserve(64 << 10)?;
        let rewritten = (|| -> std::io::Result<File> {
            use std::os::unix::fs::OpenOptionsExt;
            #[cfg(feature = "failpoints")]
            if std::env::var("TKV_ERRORPOINT").as_deref() == Ok("wal_recovery_enospc") {
                return Err(std::io::Error::from_raw_os_error(28));
            }
            if fs2::available_space(dir)? < offset {
                return Err(std::io::Error::from_raw_os_error(28));
            }
            let mut output = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temp)?;
            file.seek(SeekFrom::Start(0))?;
            let mut buffer = vec![0; 64 << 10];
            let mut remaining = offset;
            // Reflinks or copy_file_range can retain the failed physical extent.
            while remaining != 0 {
                let count = remaining.min(buffer.len() as u64) as usize;
                file.read_exact(&mut buffer[..count])?;
                output.write_all(&buffer[..count])?;
                remaining -= count as u64;
            }
            output.set_permissions(file.metadata()?.permissions())?;
            output.sync_all()?;
            super::failpoint("wal_recovery_written");
            fs::rename(&temp, &path)?;
            File::open(dir)?.sync_all()?;
            super::failpoint("wal_recovery_published");
            Ok(output)
        })();
        if rewritten.is_err() {
            let _ = fs::remove_file(&temp);
        }
        let recovery_read_only = match rewritten {
            Ok(output) => {
                if offset < length {
                    TORN_TAIL_RECOVERIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                file = output;
                false
            }
            Err(error) if error.raw_os_error() == Some(28) => true,
            Err(error) => return Err(error.into()),
        };
        // On ENOSPC expose only a proven durable prefix. The original bytes are
        // retained for a later successful recovery, but cannot become read promises.
        let replay_end = if recovery_read_only {
            synced_len.unwrap_or(HEADER as u64).min(offset)
        } else {
            offset
        };
        if !fully_synced {
            file.seek(SeekFrom::Start(HEADER as u64))?;
            let mut cursor = HEADER as u64;
            while cursor < replay_end {
                let mut frame = [0; 24];
                file.read_exact(&mut frame)?;
                let len = u32::from_le_bytes(frame[4..8].try_into().unwrap()) as usize;
                if cursor + 24 + len as u64 > replay_end {
                    return Err(Error::corrupt("WAL watermark splits frame"));
                }
                let _payload = budget.reserve(len)?;
                let mut payload = vec![0; len];
                file.read_exact(&mut payload)?;
                let count = Reader::new(&payload).u32()? as usize;
                let _decoded =
                    budget.reserve(len + count * (std::mem::size_of::<Record>() + 96))?;
                let seq = u64::from_le_bytes(frame[8..16].try_into().unwrap());
                apply(seq, decode(&payload, seq)?)?;
                cursor += 24 + len as u64;
            }
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            info: info.clone(),
            file: Arc::new(file),
            len: offset,
            recovery_read_only,
        })
    }
    pub fn append(&mut self, bytes: Vec<u8>, io: &IoScheduler, tenant: Uuid) -> Result<()> {
        use std::os::unix::fs::FileExt;
        let file = self.file.clone();
        let offset = self.len;
        let len = bytes.len();
        #[cfg(feature = "failpoints")]
        if let Ok(point) = std::env::var("TKV_FAILPOINT") {
            let torn = match point.as_str() {
                "wal_torn_header" => Some(bytes[..10].to_vec()),
                "wal_torn_payload" => Some(bytes[..bytes.len() - 1].to_vec()),
                "wal_torn_zero_header" | "wal_torn_header_crc" | "wal_torn_payload_crc" => {
                    let mut torn = bytes.clone();
                    match point.as_str() {
                        "wal_torn_zero_header" => torn[..24].fill(0),
                        "wal_torn_header_crc" => torn[16] ^= 1,
                        _ => torn[24] ^= 1,
                    }
                    Some(torn)
                }
                _ => None,
            };
            if let Some(torn) = torn {
                file.write_all_at(&torn, offset)?;
                file.sync_all()?;
                std::process::abort();
            }
        }
        io.submit(tenant, move || {
            file.write_all_at(&bytes, offset)?;
            Ok(())
        })?
        .wait()
        .map_err(|error| Error::new("COMMIT_UNKNOWN", error.to_string()))?;
        self.len += len as u64;
        super::failpoint("wal_appended");
        Ok(())
    }
    pub(super) fn set_start_seq(&mut self, start_seq: u64) -> Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.write_all_at(&start_seq.to_le_bytes(), 56)?;
        self.info.start_seq = start_seq;
        Ok(())
    }
    pub fn sync(&self, io: &IoScheduler, tenant: Uuid) -> Result<()> {
        let file = self.file.clone();
        io.run(tenant, move || {
            file.sync_all()?;
            Ok(())
        })?;
        super::failpoint("wal_synced");
        Ok(())
    }
}
pub fn encode(seq: u64, records: &[Record]) -> Result<Vec<u8>> {
    let payload_len = records.iter().try_fold(4usize, |len, record| {
        let len = len
            .saturating_add(12)
            .saturating_add(record.key.len())
            .saturating_add(record.value.as_ref().map_or(0, |value| value.len()));
        (len <= MAX_BATCH)
            .then_some(len)
            .ok_or_else(|| Error::limit("expanded WAL batch exceeds 64 MiB"))
    })?;
    let mut out = Vec::with_capacity(24 + payload_len);
    out.extend_from_slice(b"TXN1");
    put_u32(&mut out, payload_len as u32);
    put_u64(&mut out, seq);
    let header_crc = crc32fast::hash(&out);
    put_u32(&mut out, header_crc);
    put_u32(&mut out, 0);
    put_u32(&mut out, records.len() as u32);
    for r in records {
        put_bytes(&mut out, &r.key);
        put_u32(&mut out, r.prefix_len as u32);
        if let Some(value) = &r.value {
            put_bytes(&mut out, value);
        } else {
            put_u32(&mut out, u32::MAX);
        }
    }
    let payload_crc = crc32fast::hash(&out[24..]);
    out[20..24].copy_from_slice(&payload_crc.to_le_bytes());
    Ok(out)
}
pub(super) fn decode(payload: &[u8], seq: u64) -> Result<Vec<Record>> {
    let mut r = Reader::new(payload);
    let count = r.u32()? as usize;
    if count > payload.len() / 12 {
        return Err(Error::corrupt("WAL entry count"));
    }
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let key = r.bytes()?.to_vec();
        let prefix_len = r.u32()? as usize;
        if key.len() > crate::codec::MAX_KEY || prefix_len > key.len() {
            return Err(Error::corrupt("WAL key bounds"));
        }
        let len = r.u32()?;
        let value = if len == u32::MAX {
            None
        } else {
            Some(Arc::new(Bytes::new(r.take(len as usize)?.to_vec())))
        };
        records.push(Record {
            key,
            seq,
            value,
            prefix_len,
        });
    }
    if !r.remaining().is_empty() {
        return Err(Error::corrupt("WAL trailing bytes"));
    }
    Ok(records)
}

pub(super) fn frame(bytes: &[u8]) -> Result<u64> {
    if bytes.len() < 24
        || &bytes[..4] != b"TXN1"
        || crc32fast::hash(&bytes[..16]) != u32::from_le_bytes(bytes[16..20].try_into().unwrap())
    {
        return Err(Error::corrupt("replication WAL header"));
    }
    let len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    if len > MAX_BATCH
        || bytes.len() != len + 24
        || crc32fast::hash(&bytes[24..]) != u32::from_le_bytes(bytes[20..24].try_into().unwrap())
    {
        return Err(Error::corrupt("replication WAL payload"));
    }
    Ok(u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
}

// Reuse one bounded read-ahead buffer across a page of small transactions.
// `take` freezes the visible prefix even while the primary appends to this WAL.
pub(super) struct FrameReader<'a> {
    reader: Option<std::io::BufReader<std::io::Take<File>>>,
    offset: u64,
    remaining: u64,
    budget: &'a crate::resources::Budget,
    buffer_memory: Option<crate::resources::Reservation>,
}
impl<'a> FrameReader<'a> {
    pub fn new(
        mut file: File,
        offset: u64,
        end: u64,
        budget: &'a crate::resources::Budget,
    ) -> Result<Self> {
        if offset > end {
            return Err(Error::corrupt("replication WAL offset"));
        }
        file.seek(SeekFrom::Start(offset))?;
        let remaining = end - offset;
        let capacity = remaining.min(64 << 10) as usize;
        // Read-ahead is optional: an otherwise valid page must still work under
        // a budget that only has space for its required frame and wire encoding.
        let memory = budget.reserve(capacity).ok();
        let reader = std::io::BufReader::with_capacity(
            if memory.is_some() { capacity } else { 0 },
            file.take(remaining),
        );
        Ok(Self {
            reader: Some(reader),
            offset,
            remaining,
            budget,
            buffer_memory: memory,
        })
    }
    pub fn unbuffer(&mut self) -> Result<()> {
        self.unbuffer_at(self.offset)
    }
    fn unbuffer_at(&mut self, offset: u64) -> Result<()> {
        if self.buffer_memory.is_some() {
            let mut file = self.reader.take().unwrap().into_inner().into_inner();
            self.buffer_memory.take();
            file.seek(SeekFrom::Start(offset))?;
            self.reader = Some(std::io::BufReader::with_capacity(
                0,
                file.take(self.remaining - (offset - self.offset)),
            ));
        }
        Ok(())
    }
    pub fn next(
        &mut self,
        allocation_overhead: usize,
    ) -> Result<Option<(u64, Vec<u8>, crate::resources::Reservation)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        if self.remaining < 24 {
            return Err(Error::corrupt("replication WAL offset"));
        }
        let mut header = [0u8; 24];
        self.reader.as_mut().unwrap().read_exact(&mut header)?;
        if &header[..4] != b"TXN1"
            || crc32fast::hash(&header[..16])
                != u32::from_le_bytes(header[16..20].try_into().unwrap())
        {
            return Err(Error::corrupt("replication WAL header checksum"));
        }
        let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        if len > MAX_BATCH || len as u64 + 24 > self.remaining {
            return Err(Error::corrupt("replication WAL frame bounds"));
        }
        let charge = len + 24 + allocation_overhead;
        let memory = self.budget.reserve(charge).or_else(|error| {
            if self.buffer_memory.is_none() {
                return Err(error);
            }
            self.unbuffer_at(self.offset + 24)?;
            self.budget.reserve(charge)
        })?;
        let mut bytes = vec![0u8; len + 24];
        bytes[..24].copy_from_slice(&header);
        self.reader.as_mut().unwrap().read_exact(&mut bytes[24..])?;
        let seq = frame(&bytes)?;
        self.remaining -= bytes.len() as u64;
        self.offset += bytes.len() as u64;
        Ok(Some((seq, bytes, memory)))
    }
}

pub(super) fn decoded_memory(payload: &[u8]) -> Result<usize> {
    let count = Reader::new(payload).u32()? as usize;
    if count > payload.len() / 12 {
        return Err(Error::corrupt("WAL entry count"));
    }
    Ok(payload.len() * 2 + count * 256)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{Budget, Counter};
    #[test]
    fn review0732_recovery_never_discards_checksum_corruption() {
        let info = crate::TenantInfo {
            id: Uuid::new_v4(),
            generation: Uuid::new_v4(),
            history: Uuid::new_v4(),
            name: "torn-tail".into(),
            memory_bytes: 1 << 20,
            disk_bytes: 1 << 20,
        };
        let budget = Budget {
            global: Counter::new(info.memory_bytes),
            tenant: Counter::new(info.memory_bytes),
        };
        let frames: Vec<_> = (1..=3)
            .map(|seq| {
                encode(
                    seq,
                    &[Record {
                        key: b"key".to_vec(),
                        seq,
                        value: Some(Arc::new(Bytes::new(vec![seq as u8; 4096]))),
                        prefix_len: 0,
                    }],
                )
                .unwrap()
            })
            .collect();
        let prefix_len = (HEADER + frames[0].len()) as u64;
        for damage in [
            "magic",
            "header_crc",
            "payload_crc",
            "zero_page",
            "short_header",
            "short_payload",
        ] {
            let mut broken = frames[1].clone();
            match damage {
                "magic" => broken[0] ^= 1,
                "header_crc" => broken[16] ^= 1,
                "payload_crc" => *broken.last_mut().unwrap() ^= 1,
                "zero_page" => broken[24..].fill(0),
                "short_header" => broken.truncate(10),
                "short_payload" => {
                    broken.pop();
                }
                _ => unreachable!(),
            }
            // Later pages can reach disk before the hole. They must not be
            // resynchronized or applied after the first damaged transaction.
            if !damage.starts_with("short") {
                broken.extend_from_slice(&frames[2]);
            }
            for tail in [false, true] {
                for synced_len in [
                    None,
                    Some(prefix_len),
                    Some(prefix_len + frames[1].len() as u64),
                ] {
                    let dir = tempfile::tempdir().unwrap();
                    let wal = Wal::create(dir.path(), info.id, info.generation, 1).unwrap();
                    let mut file = wal.file.as_ref();
                    file.write_all(&frames[0]).unwrap();
                    file.write_all(&broken).unwrap();
                    wal.file.sync_all().unwrap();
                    let wal_info = wal.info.clone();
                    drop(wal);
                    let path = dir.path().join(format!("{}.wal", wal_info.id));
                    let original = fs::read(&path).unwrap();
                    let mut applied = Vec::new();
                    let result = Wal::recover(
                        dir.path(),
                        &wal_info,
                        &info,
                        tail,
                        synced_len,
                        &budget,
                        |seq, records| {
                            applied.push((seq, records[0].value.as_ref().unwrap()[0]));
                            Ok(())
                        },
                    );
                    let recoverable = damage.starts_with("short")
                        && tail
                        && synced_len.is_none_or(|length| length <= prefix_len);
                    if recoverable {
                        let recovered =
                            result.unwrap_or_else(|e| panic!("{damage}, {synced_len:?}: {e}"));
                        assert_eq!(applied, [(1, 1)]);
                        assert_eq!(recovered.len, prefix_len);
                        assert!(!recovered.recovery_read_only);
                        assert_eq!(fs::read(&path).unwrap(), original[..prefix_len as usize]);
                    } else {
                        assert_eq!(
                            result.err().unwrap().code,
                            "CORRUPTION",
                            "{damage}, {tail}, {synced_len:?}"
                        );
                        assert_eq!(fs::read(&path).unwrap(), original);
                    }
                }
            }
        }
    }

    #[test]
    fn buffered_replication_preserves_frames_limits_and_pinned_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let frames: Vec<_> = [16, 40_000, 80_000, 29]
            .into_iter()
            .enumerate()
            .map(|(i, n)| {
                encode(
                    i as u64 + 1,
                    &[Record {
                        key: b"key".to_vec(),
                        seq: i as u64 + 1,
                        value: Some(Arc::new(Bytes::new(vec![i as u8; n]))),
                        prefix_len: 0,
                    }],
                )
                .unwrap()
            })
            .collect();
        let bytes: Vec<_> = frames.concat();
        std::fs::write(&path, &bytes).unwrap();
        // Limits cover buffered reads and fallback when read-ahead would consume
        // the memory needed by the next required (larger than buffer) frame.
        for limit in [128 << 10, frames[2].len()] {
            let counter = Counter::new(limit);
            let budget = Budget {
                global: counter.clone(),
                tenant: Counter::new(limit),
            };
            {
                let mut reader =
                    FrameReader::new(File::open(&path).unwrap(), 0, bytes.len() as u64, &budget)
                        .unwrap();
                for (i, expected) in frames.iter().enumerate() {
                    let (seq, data, _charge) = reader.next(0).unwrap().unwrap();
                    assert_eq!(seq, i as u64 + 1);
                    assert_eq!(&data, expected);
                }
                assert!(reader.next(0).unwrap().is_none());
            }
            assert_eq!(counter.used(), 0);
        }
        let budget = Budget {
            global: Counter::new(1 << 20),
            tenant: Counter::new(1 << 20),
        };
        let offset = frames[0].len() as u64;
        let end = offset + frames[1].len() as u64;
        let mut reader =
            FrameReader::new(File::open(&path).unwrap(), offset, end, &budget).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&frames[3])
            .unwrap();
        assert_eq!(reader.next(0).unwrap().unwrap().1, frames[1]);
        assert!(reader.next(0).unwrap().is_none());
        for end in [end - 1, offset + 23] {
            let mut reader =
                FrameReader::new(File::open(&path).unwrap(), offset, end, &budget).unwrap();
            assert_eq!(reader.next(0).unwrap_err().code, "CORRUPTION");
        }
        let mut corrupt = bytes.clone();
        corrupt[frames[0].len() + 30] ^= 1;
        std::fs::write(&path, corrupt).unwrap();
        let mut reader =
            FrameReader::new(File::open(&path).unwrap(), offset, end, &budget).unwrap();
        assert_eq!(reader.next(0).unwrap_err().code, "CORRUPTION");
    }
}
