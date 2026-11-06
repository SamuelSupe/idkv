use super::indexes::IndexEntry;
use crate::{
    Error, Result,
    codec::{self, Reader, put_bytes, put_u32},
};

pub(super) struct Delta {
    pub old: Option<IndexEntry>,
    pub new: Option<IndexEntry>,
}
impl Delta {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = b"DLT1".to_vec();
        for entry in [&self.old, &self.new] {
            bytes.push(u8::from(entry.is_some()));
            if let Some(entry) = entry {
                put_bytes(&mut bytes, &entry.key);
                put_bytes(&mut bytes, &entry.value);
                put_u32(&mut bytes, entry.prefix_len as u32);
                put_bytes(&mut bytes, entry.document_key.as_bytes());
            }
        }
        bytes
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(
            bytes
                .strip_prefix(b"DLT1")
                .ok_or_else(|| Error::corrupt("index delta version"))?,
        );
        let old = entry(&mut reader)?;
        let new = entry(&mut reader)?;
        if !reader.remaining().is_empty() {
            return Err(Error::corrupt("index delta trailing data"));
        }
        Ok(Self { old, new })
    }
}
fn entry(reader: &mut Reader<'_>) -> Result<Option<IndexEntry>> {
    match reader.u8()? {
        0 => Ok(None),
        1 => {
            let key = reader.bytes()?.to_vec();
            let value = reader.bytes()?.to_vec();
            let prefix_len = reader.u32()? as usize;
            let document_key = reader.string()?;
            if key.len() > codec::MAX_KEY
                || prefix_len > key.len()
                || value.len() > codec::MAX_DOC
                || document_key.len() > 512
            {
                return Err(Error::corrupt("index delta bounds"));
            }
            Ok(Some(IndexEntry {
                key,
                value,
                prefix_len,
                document_key,
            }))
        }
        _ => Err(Error::corrupt("index delta entry tag")),
    }
}
