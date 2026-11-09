use super::{LogCursor, LogPage};
use crate::{Error, Result, resources::Reservation};
use axum::body::Bytes;
use uuid::Uuid;

pub(crate) const LOG_MEDIA_TYPE: &str = "application/vnd.idkv.wal-page.v1";
pub(crate) const LOG_ACCEPT: &str = "application/vnd.idkv.wal-page.v1, application/json;q=0.5";
const HEADER_BYTES: usize = 57;

pub(crate) struct LogExport {
    pub page: LogPage,
    pub raw: Vec<Bytes>,
}

pub(crate) struct RawFrame {
    pub bytes: Vec<u8>,
    pub _memory: Reservation,
}

impl AsRef<[u8]> for RawFrame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

pub(crate) struct LogReply {
    pub page: LogPage,
    pub raw: Option<Bytes>,
}

pub(crate) fn log_header(page: &LogPage) -> [u8; HEADER_BYTES] {
    let mut header = [0; HEADER_BYTES];
    header[..16].copy_from_slice(page.epoch.as_bytes());
    header[16..24].copy_from_slice(&page.through.to_le_bytes());
    header[24..32].copy_from_slice(&page.source_position.to_le_bytes());
    if let Some(cursor) = &page.cursor {
        header[32] = 1;
        header[33..49].copy_from_slice(cursor.file.as_bytes());
        header[49..57].copy_from_slice(&cursor.offset.to_le_bytes());
    }
    header
}

pub(super) fn decode_log_reply(bytes: Vec<u8>, binary: bool) -> Result<LogReply> {
    if !binary {
        return Ok(LogReply {
            page: serde_json::from_slice(&bytes)?,
            raw: None,
        });
    }
    let bytes = Bytes::from(bytes);
    let header = bytes
        .first_chunk::<HEADER_BYTES>()
        .ok_or_else(|| Error::corrupt("truncated binary log page"))?;
    let cursor = match header[32] {
        0 => None,
        1 => Some(LogCursor {
            file: Uuid::from_bytes(header[33..49].try_into().unwrap()),
            offset: u64::from_le_bytes(header[49..57].try_into().unwrap()),
        }),
        _ => return Err(Error::corrupt("binary log cursor flag")),
    };
    Ok(LogReply {
        page: LogPage {
            epoch: Uuid::from_bytes(header[..16].try_into().unwrap()),
            through: u64::from_le_bytes(header[16..24].try_into().unwrap()),
            source_position: u64::from_le_bytes(header[24..32].try_into().unwrap()),
            frames: vec![],
            cursor,
        },
        raw: Some(bytes.slice(HEADER_BYTES..)),
    })
}
