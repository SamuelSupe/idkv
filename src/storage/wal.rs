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
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};
use uuid::Uuid;

const HEADER: usize = 64;
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
        })
    }
    pub fn recover(
        dir: &Path,
        info: &WalInfo,
        tenant: Uuid,
        generation: Uuid,
        tail: bool,
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
            || &header[8..24] != tenant.as_bytes()
            || &header[24..40] != generation.as_bytes()
            || &header[40..56] != info.id.as_bytes()
            || u64::from_le_bytes(header[56..].try_into().unwrap()) != info.start_seq
        {
            return Err(Error::corrupt("WAL identity or version mismatch"));
        }
        let mut offset = HEADER as u64;
        while offset < length {
            if length - offset < 24 {
                if tail {
                    break;
                }
                return Err(Error::corrupt("truncated non-tail WAL"));
            }
            let mut frame = [0; 24];
            file.read_exact(&mut frame)?;
            if &frame[..4] != b"TXN1"
                || crc32fast::hash(&frame[..16])
                    != u32::from_le_bytes(frame[16..20].try_into().unwrap())
            {
                return Err(Error::corrupt("WAL batch header checksum"));
            }
            let len = u32::from_le_bytes(frame[4..8].try_into().unwrap()) as usize;
            let seq = u64::from_le_bytes(frame[8..16].try_into().unwrap());
            if len > MAX_BATCH || seq < info.start_seq {
                return Err(Error::corrupt("WAL batch bounds"));
            }
            if length - offset - 24 < len as u64 {
                if tail {
                    break;
                }
                return Err(Error::corrupt("truncated non-tail WAL payload"));
            }
            let _payload_memory = budget.reserve(len)?;
            let mut payload = vec![0; len];
            file.read_exact(&mut payload)?;
            if crc32fast::hash(&payload) != u32::from_le_bytes(frame[20..24].try_into().unwrap()) {
                return Err(Error::corrupt("WAL batch checksum"));
            }
            let count = Reader::new(&payload).u32()? as usize;
            if count > payload.len() / 12 {
                return Err(Error::corrupt("WAL entry count"));
            }
            let _decoded_memory =
                budget.reserve(len + count * (std::mem::size_of::<Record>() + 96))?;
            let records = decode(&payload, seq)?;
            apply(seq, records)?;
            offset += 24 + len as u64;
        }
        if offset != length {
            file.set_len(offset)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            info: info.clone(),
            file: Arc::new(file),
            len: offset,
        })
    }
    pub fn append(&mut self, bytes: Vec<u8>, io: &IoScheduler, tenant: Uuid) -> Result<()> {
        use std::os::unix::fs::FileExt;
        let file = self.file.clone();
        let offset = self.len;
        let len = bytes.len();
        #[cfg(feature = "failpoints")]
        if let Ok(point) = std::env::var("TKV_FAILPOINT") {
            let cut = match point.as_str() {
                "wal_torn_header" => Some(10),
                "wal_torn_payload" => Some(bytes.len() - 1),
                _ => None,
            };
            if let Some(cut) = cut {
                file.write_all_at(&bytes[..cut], offset)?;
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
    let mut payload = Vec::new();
    put_u32(&mut payload, records.len() as u32);
    for r in records {
        put_bytes(&mut payload, &r.key);
        put_u32(&mut payload, r.prefix_len as u32);
        if let Some(value) = &r.value {
            put_bytes(&mut payload, value);
        } else {
            put_u32(&mut payload, u32::MAX);
        }
        if payload.len() > MAX_BATCH {
            return Err(Error::limit("expanded WAL batch exceeds 64 MiB"));
        }
    }
    let mut out = b"TXN1".to_vec();
    put_u32(&mut out, payload.len() as u32);
    put_u64(&mut out, seq);
    let crc = crc32fast::hash(&out);
    put_u32(&mut out, crc);
    put_u32(&mut out, crc32fast::hash(&payload));
    out.extend(payload);
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
    pub fn next(&mut self) -> Result<Option<(u64, Vec<u8>, crate::resources::Reservation)>> {
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
        let memory = self.budget.reserve(len + 24).or_else(|error| {
            if self.buffer_memory.is_none() {
                return Err(error);
            }
            self.unbuffer_at(self.offset + 24)?;
            self.budget.reserve(len + 24)
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
                    let (seq, data, _charge) = reader.next().unwrap().unwrap();
                    assert_eq!(seq, i as u64 + 1);
                    assert_eq!(&data, expected);
                }
                assert!(reader.next().unwrap().is_none());
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
        assert_eq!(reader.next().unwrap().unwrap().1, frames[1]);
        assert!(reader.next().unwrap().is_none());
        for end in [end - 1, offset + 23] {
            let mut reader =
                FrameReader::new(File::open(&path).unwrap(), offset, end, &budget).unwrap();
            assert_eq!(reader.next().unwrap_err().code, "CORRUPTION");
        }
        let mut corrupt = bytes.clone();
        corrupt[frames[0].len() + 30] ^= 1;
        std::fs::write(&path, corrupt).unwrap();
        let mut reader =
            FrameReader::new(File::open(&path).unwrap(), offset, end, &budget).unwrap();
        assert_eq!(reader.next().unwrap_err().code, "CORRUPTION");
    }
}
