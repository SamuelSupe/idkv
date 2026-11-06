//! Typed reads of internal records without an intermediate JSON tree. All nodes,
//! including ignored fields, still pass framing, ordering and depth validation.
use super::*;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};

impl de::Error for Error {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self::corrupt(message.to_string())
    }
}

pub(crate) fn field_as<T: de::DeserializeOwned>(data: &[u8], path: &str) -> Result<Option<T>> {
    locate_parts(data, &segments(path)?, None)?
        .map(|node| {
            T::deserialize(Node {
                bytes: node,
                depth: 0,
            })
        })
        .transpose()
}

struct Node<'a> {
    bytes: &'a [u8],
    depth: usize,
}
impl<'de> de::Deserializer<'de> for Node<'de> {
    type Error = Error;
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if self.depth > 32 {
            return Err(Error::corrupt("document nesting"));
        }
        let mut r = Reader::new(self.bytes);
        let value = match r.u8()? {
            NULL => visitor.visit_unit::<Error>(),
            BOOL => match r.u8()? {
                0 => visitor.visit_bool(false),
                1 => visitor.visit_bool(true),
                _ => return Err(Error::corrupt("boolean")),
            },
            I64 => visitor.visit_i64(r.u64()? as i64),
            U64 => visitor.visit_u64(r.u64()?),
            F64 => {
                let value = f64::from_bits(r.u64()?);
                if !value.is_finite() {
                    return Err(Error::corrupt("non-finite float"));
                }
                visitor.visit_f64(value)
            }
            STR => visitor.visit_borrowed_str(text(r.bytes()?)?),
            ARRAY => {
                let count = count(&mut r)?;
                let mut sequence = Sequence {
                    reader: r,
                    count,
                    depth: self.depth + 1,
                };
                let value = visitor.visit_seq(&mut sequence)?;
                if sequence.count != 0 || !sequence.reader.remaining().is_empty() {
                    return Err(Error::corrupt("array trailing entries"));
                }
                return Ok(value);
            }
            OBJECT => {
                let count = count(&mut r)?;
                let directory = r.remaining();
                let mut last = None;
                let mut end = 0usize;
                for _ in 0..count {
                    let key = text(r.bytes()?)?;
                    if last.is_some_and(|old| old >= key) {
                        return Err(Error::corrupt("unordered object directory"));
                    }
                    last = Some(key);
                    let offset = r.u32()? as usize;
                    let len = r.u32()? as usize;
                    if offset != end {
                        return Err(Error::corrupt("object offsets"));
                    }
                    end = offset
                        .checked_add(len)
                        .ok_or_else(|| Error::corrupt("offset overflow"))?;
                }
                if end != r.remaining().len() {
                    return Err(Error::corrupt("object trailing bytes"));
                }
                let mut fields = Fields {
                    directory: Reader::new(directory),
                    payload: r.remaining(),
                    count,
                    pending: None,
                    depth: self.depth + 1,
                };
                let value = visitor.visit_map(&mut fields)?;
                if fields.count != 0 || fields.pending.is_some() {
                    return Err(Error::corrupt("object trailing entries"));
                }
                return Ok(value);
            }
            _ => return Err(Error::corrupt("document type tag")),
        }?;
        if !r.remaining().is_empty() {
            return Err(Error::corrupt("document trailing bytes"));
        }
        Ok(value)
    }
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if self.bytes == [NULL] {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        // Internal identity enums are tagged maps. This branch handles string
        // enums; use the existing JSON decoder for externally tagged objects.
        let mut r = Reader::new(self.bytes);
        if r.u8()? != STR {
            return Err(Error::corrupt("expected string enum"));
        }
        let name = text(r.bytes()?)?;
        if !r.remaining().is_empty() {
            return Err(Error::corrupt("document trailing bytes"));
        }
        visitor.visit_enum(de::value::BorrowedStrDeserializer::<Error>::new(name))
    }
    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes byte_buf
        unit unit_struct seq tuple tuple_struct map struct identifier ignored_any
    }
}
fn text(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes).map_err(|_| Error::corrupt("invalid UTF-8"))
}
fn count(r: &mut Reader<'_>) -> Result<usize> {
    let count = r.u32()? as usize;
    if count > 16_384 {
        return Err(Error::corrupt("container length"));
    }
    Ok(count)
}
struct Sequence<'a> {
    reader: Reader<'a>,
    count: usize,
    depth: usize,
}
impl<'de> SeqAccess<'de> for &mut Sequence<'de> {
    type Error = Error;
    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.count == 0 {
            return Ok(None);
        }
        self.count -= 1;
        seed.deserialize(Node {
            bytes: self.reader.bytes()?,
            depth: self.depth,
        })
        .map(Some)
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.count)
    }
}
struct Fields<'a> {
    directory: Reader<'a>,
    payload: &'a [u8],
    count: usize,
    pending: Option<&'a [u8]>,
    depth: usize,
}
impl<'de> MapAccess<'de> for &mut Fields<'de> {
    type Error = Error;
    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        if self.pending.is_some() {
            return Err(Error::corrupt("unconsumed object field"));
        }
        if self.count == 0 {
            return Ok(None);
        }
        let name = text(self.directory.bytes()?)?;
        let offset = self.directory.u32()? as usize;
        let len = self.directory.u32()? as usize;
        self.pending = Some(slice(self.payload, offset, len)?);
        self.count -= 1;
        seed.deserialize(de::value::BorrowedStrDeserializer::<Error>::new(name))
            .map(Some)
    }
    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        let bytes = self
            .pending
            .take()
            .ok_or_else(|| Error::corrupt("object value without key"))?;
        seed.deserialize(Node {
            bytes,
            depth: self.depth,
        })
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct Record {
        low: i64,
        high: u64,
        score: f64,
        labels: Vec<Option<String>>,
        id: uuid::Uuid,
    }
    #[test]
    fn typed_records_match_json_decoder_and_reject_malformed_nodes() {
        let document = encode(&json!({"data": {
            "low": i64::MIN, "high": u64::MAX, "score": 1.5,
            "labels": [null, "中文\u{0}"], "id": uuid::Uuid::new_v4(),
            "ignored": {"nested": [true, false, 1, 1.0, null, "hello"]}
        }}))
        .unwrap();
        let compare = |bytes: &[u8]| {
            let old = field(bytes, "/data").and_then(|v| {
                v.map(|v| {
                    serde_json::from_value::<Record>(v).map_err(|e| Error::corrupt(e.to_string()))
                })
                .transpose()
            });
            let new = field_as::<Record>(bytes, "/data");
            match (old, new) {
                (Ok(old), Ok(new)) => assert_eq!(old, new),
                (Err(_), Err(e)) => assert_eq!(e.code, "CORRUPTION"),
                (old, new) => panic!("decoder mismatch: {old:?}, {new:?}"),
            }
        };
        compare(&document);
        for length in 0..document.len() {
            compare(&document[..length]);
        }
        for i in 0..document.len() {
            for mask in [1, 0xff] {
                let mut corrupt = document.clone();
                corrupt[i] ^= mask;
                compare(&corrupt);
            }
        }
    }
}
