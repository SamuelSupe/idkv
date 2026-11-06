//! Stable type-tagged values and memcomparable scalar keys. Object directories
//! allow projection without decoding unrelated values.
mod typed;
use crate::{Error, Result, model::Order};
use serde_json::{Map, Number, Value};
use std::cmp::Ordering;
pub(crate) use typed::field_as;

const NULL: u8 = 1;
const BOOL: u8 = 2;
const I64: u8 = 3;
const U64: u8 = 4;
const F64: u8 = 5;
const STR: u8 = 6;
const ARRAY: u8 = 7;
const OBJECT: u8 = 8;
pub const MAX_DOC: usize = 16 << 20;
pub const MAX_KEY: usize = 8192;

pub fn number_kind(n: &Number) -> Result<u8> {
    let text = n.to_string();
    if text.contains(['.', 'e', 'E']) {
        if n.as_f64().is_some_and(f64::is_finite) {
            Ok(F64)
        } else {
            Err(Error::invalid("non-finite number"))
        }
    } else if text.parse::<i64>().is_ok() {
        Ok(I64)
    } else if text.parse::<u64>().is_ok() {
        Ok(U64)
    } else {
        Err(Error::invalid("integer exceeds i64/u64 range"))
    }
}

pub fn validate_pointer(path: &str) -> Result<()> {
    if !path.starts_with('/') || path.len() > 1024 {
        return Err(Error::invalid(
            "field must be a non-root JSON Pointer of at most 1024 bytes",
        ));
    }
    for part in path[1..].split('/') {
        let mut chars = part.chars();
        while let Some(c) = chars.next() {
            if c == '~' && !matches!(chars.next(), Some('0' | '1')) {
                return Err(Error::invalid("invalid JSON Pointer escape"));
            }
        }
    }
    Ok(())
}
fn segments(path: &str) -> Result<Vec<String>> {
    validate_pointer(path)?;
    Ok(path[1..]
        .split('/')
        .map(|s| s.replace("~1", "/").replace("~0", "~"))
        .collect())
}

pub fn encode(value: &Value) -> Result<Vec<u8>> {
    if !value.is_object() {
        return Err(Error::invalid("document must be an object"));
    }
    let mut out = b"DOC1".to_vec();
    let mut fields = 0;
    encode_node(value, &mut out, 0, &mut fields)?;
    if out.len() > MAX_DOC {
        return Err(Error::limit("document exceeds 16 MiB format limit"));
    }
    Ok(out)
}
fn encode_node(value: &Value, out: &mut Vec<u8>, depth: usize, fields: &mut usize) -> Result<()> {
    if depth > 32 || *fields > 16_384 {
        return Err(Error::limit("document nesting or field count exceeded"));
    }
    match value {
        Value::Null => out.push(NULL),
        Value::Bool(b) => {
            out.push(BOOL);
            out.push(u8::from(*b));
        }
        Value::Number(n) => {
            let kind = number_kind(n)?;
            out.push(kind);
            match kind {
                I64 => out.extend(n.to_string().parse::<i64>().unwrap().to_le_bytes()),
                U64 => out.extend(n.to_string().parse::<u64>().unwrap().to_le_bytes()),
                _ => out.extend(n.as_f64().unwrap().to_bits().to_le_bytes()),
            }
        }
        Value::String(s) => {
            out.push(STR);
            put_bytes(out, s.as_bytes());
        }
        Value::Array(items) => {
            out.push(ARRAY);
            put_u32(out, items.len() as u32);
            for item in items {
                *fields += 1;
                let start = out.len();
                put_u32(out, 0);
                encode_node(item, out, depth + 1, fields)?;
                let len = (out.len() - start - 4) as u32;
                out[start..start + 4].copy_from_slice(&len.to_le_bytes());
            }
        }
        Value::Object(map) => {
            out.push(OBJECT);
            put_u32(out, map.len() as u32);
            let mut payload = Vec::new();
            for (key, value) in map {
                *fields += 1;
                put_bytes(out, key.as_bytes());
                put_u32(out, payload.len() as u32);
                let start = payload.len();
                encode_node(value, &mut payload, depth + 1, fields)?;
                put_u32(out, (payload.len() - start) as u32);
            }
            out.extend(payload);
        }
    }
    if out.len() > MAX_DOC {
        return Err(Error::limit("document exceeds 16 MiB format limit"));
    }
    Ok(())
}
pub fn decode(data: &[u8]) -> Result<Value> {
    let node = data
        .strip_prefix(b"DOC1")
        .ok_or_else(|| Error::corrupt("document version"))?;
    decode_node(node, 0)
}
fn decode_node(data: &[u8], depth: usize) -> Result<Value> {
    if depth > 32 {
        return Err(Error::corrupt("document nesting"));
    }
    let mut r = Reader::new(data);
    let value = match r.u8()? {
        NULL => Value::Null,
        BOOL => match r.u8()? {
            0 => Value::Bool(false),
            1 => Value::Bool(true),
            _ => return Err(Error::corrupt("boolean")),
        },
        I64 => Value::Number(i64::from_le_bytes(r.take(8)?.try_into().unwrap()).into()),
        U64 => Value::Number(u64::from_le_bytes(r.take(8)?.try_into().unwrap()).into()),
        F64 => {
            let f = f64::from_bits(u64::from_le_bytes(r.take(8)?.try_into().unwrap()));
            Value::Number(Number::from_f64(f).ok_or_else(|| Error::corrupt("non-finite float"))?)
        }
        STR => Value::String(r.string()?),
        ARRAY => {
            let count = r.u32()? as usize;
            if count > 16_384 {
                return Err(Error::corrupt("array length"));
            }
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                out.push(decode_node(r.bytes()?, depth + 1)?);
            }
            Value::Array(out)
        }
        OBJECT => {
            let entries = object_directory(&mut r)?;
            let payload = r.remaining();
            let mut map = Map::new();
            let mut end = 0;
            for (key, offset, len) in entries {
                if offset != end {
                    return Err(Error::corrupt("object offsets"));
                }
                let node = slice(payload, offset, len)?;
                map.insert(key, decode_node(node, depth + 1)?);
                end = offset + len;
            }
            if end != payload.len() {
                return Err(Error::corrupt("object trailing bytes"));
            }
            return Ok(Value::Object(map));
        }
        _ => return Err(Error::corrupt("document type tag")),
    };
    if !r.remaining().is_empty() {
        return Err(Error::corrupt("document trailing bytes"));
    }
    Ok(value)
}
fn object_directory(r: &mut Reader<'_>) -> Result<Vec<(String, usize, usize)>> {
    let count = r.u32()? as usize;
    if count > 16_384 {
        return Err(Error::corrupt("object field count"));
    }
    let mut entries = Vec::with_capacity(count);
    let mut last: Option<String> = None;
    for _ in 0..count {
        let key = r.string()?;
        if last.as_ref().is_some_and(|old| old >= &key) {
            return Err(Error::corrupt("unordered object directory"));
        }
        last = Some(key.clone());
        entries.push((key, r.u32()? as usize, r.u32()? as usize));
    }
    Ok(entries)
}
fn slice(data: &[u8], offset: usize, len: usize) -> Result<&[u8]> {
    data.get(
        offset
            ..offset
                .checked_add(len)
                .ok_or_else(|| Error::corrupt("offset overflow"))?,
    )
    .ok_or_else(|| Error::corrupt("out-of-bounds offset"))
}
fn locate<'a>(data: &'a [u8], path: &str) -> Result<Option<(&'a [u8], Vec<bool>)>> {
    let mut containers = Vec::new();
    Ok(locate_parts(data, &segments(path)?, Some(&mut containers))?.map(|node| (node, containers)))
}
fn locate_parts<'a>(
    data: &'a [u8],
    parts: &[String],
    mut containers: Option<&mut Vec<bool>>,
) -> Result<Option<&'a [u8]>> {
    let mut node = data
        .strip_prefix(b"DOC1")
        .ok_or_else(|| Error::corrupt("document version"))?;
    for segment in parts {
        let mut r = Reader::new(node);
        match r.u8()? {
            OBJECT => {
                if let Some(containers) = containers.as_mut() {
                    containers.push(false);
                }
                // Read the variable-width directory without allocating every key.
                // Offsets are relative to its end, so validate the entire directory.
                let count = r.u32()? as usize;
                if count > 16_384 {
                    return Err(Error::corrupt("object field count"));
                }
                let mut last: Option<&str> = None;
                let mut selected = None;
                let mut end = 0usize;
                for _ in 0..count {
                    let key = std::str::from_utf8(r.bytes()?)
                        .map_err(|_| Error::corrupt("invalid UTF-8"))?;
                    if last.is_some_and(|last| last >= key) {
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
                    if key == segment {
                        selected = Some((offset, len));
                    }
                }
                if end != r.remaining().len() {
                    return Err(Error::corrupt("object trailing bytes"));
                }
                let Some((offset, len)) = selected else {
                    return Ok(None);
                };
                node = slice(r.remaining(), offset, len)?;
            }
            ARRAY => {
                if let Some(containers) = containers.as_mut() {
                    containers.push(true);
                }
                let Ok(index) = segment.parse::<usize>() else {
                    return Ok(None);
                };
                if !segment.bytes().all(|b| b.is_ascii_digit())
                    || (segment.len() > 1 && segment.starts_with('0'))
                {
                    return Ok(None);
                }
                let count = r.u32()? as usize;
                if count > 16_384 {
                    return Err(Error::corrupt("array length"));
                }
                if index >= count {
                    return Ok(None);
                }
                for _ in 0..index {
                    r.bytes()?;
                }
                node = r.bytes()?;
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(node))
}

/// Compiled pointer for row predicates: no path parsing, JSON number conversion,
/// or temporary object-directory allocations in the row loop.
pub(crate) struct FieldPath(Vec<String>);
impl FieldPath {
    pub fn new(path: &str) -> Result<Self> {
        Ok(Self(segments(path)?))
    }
    pub fn exists(&self, data: &[u8]) -> Result<bool> {
        Ok(locate_parts(data, &self.0, None)?.is_some())
    }
    pub fn scalar(&self, data: &[u8], out: &mut Vec<u8>) -> Result<bool> {
        out.clear();
        let Some(node) = locate_parts(data, &self.0, None)? else {
            out.push(0);
            return Ok(true);
        };
        let mut r = Reader::new(node);
        let tag = r.u8()?;
        if matches!(tag, OBJECT | ARRAY) {
            return Ok(false);
        }
        out.push(tag);
        match tag {
            NULL => {}
            BOOL => {
                let b = r.u8()?;
                if b > 1 {
                    return Err(Error::corrupt("boolean"));
                }
                out.push(b);
            }
            I64 => out.extend((r.u64()? ^ (1 << 63)).to_be_bytes()),
            U64 => out.extend(r.u64()?.to_be_bytes()),
            F64 => {
                let mut value = f64::from_bits(r.u64()?);
                if !value.is_finite() {
                    return Err(Error::corrupt("non-finite float"));
                }
                if value == 0.0 {
                    value = 0.0;
                }
                let bits = value.to_bits();
                out.extend(
                    (if bits >> 63 != 0 {
                        !bits
                    } else {
                        bits ^ (1 << 63)
                    })
                    .to_be_bytes(),
                );
            }
            STR => {
                let bytes = r.bytes()?;
                std::str::from_utf8(bytes).map_err(|_| Error::corrupt("invalid UTF-8"))?;
                escape(bytes, out);
            }
            _ => return Err(Error::corrupt("document type tag")),
        }
        if !r.remaining().is_empty() {
            return Err(Error::corrupt("document trailing bytes"));
        }
        Ok(true)
    }
}
pub fn field(data: &[u8], path: &str) -> Result<Option<Value>> {
    locate(data, path)?
        .map(|(node, _)| decode_node(node, 0))
        .transpose()
}

/// Memcomparable scalar bytes; missing sorts before null. Type domains remain
/// distinct, so an integer never silently compares equal to a rounded float.
pub fn scalar(value: Option<&Value>, order: &Order) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    match value {
        None => out.push(0),
        Some(Value::Null) => out.push(NULL),
        Some(Value::Bool(b)) => {
            out.push(BOOL);
            out.push(u8::from(*b));
        }
        Some(Value::Number(n)) => {
            let kind = number_kind(n)?;
            out.push(kind);
            let bits = match kind {
                I64 => (n.to_string().parse::<i64>().unwrap() as u64) ^ (1 << 63),
                U64 => n.to_string().parse::<u64>().unwrap(),
                _ => {
                    let mut f = n.as_f64().unwrap();
                    if f == 0.0 {
                        f = 0.0;
                    }
                    let bits = f.to_bits();
                    if bits >> 63 != 0 {
                        !bits
                    } else {
                        bits ^ (1 << 63)
                    }
                }
            };
            out.extend(bits.to_be_bytes());
        }
        Some(Value::String(s)) => {
            out.push(STR);
            escape(s.as_bytes(), &mut out);
        }
        _ => return Err(Error::invalid("indexed and compared values must be scalar")),
    }
    if *order == Order::Desc {
        for byte in &mut out {
            *byte = !*byte;
        }
    }
    Ok(out)
}
pub fn compare(a: Option<&Value>, b: Option<&Value>) -> Result<Ordering> {
    Ok(scalar(a, &Order::Asc)?.cmp(&scalar(b, &Order::Asc)?))
}
pub fn same_type(a: Option<&Value>, b: Option<&Value>) -> Result<bool> {
    Ok(scalar(a, &Order::Asc)?[0] == scalar(b, &Order::Asc)?[0])
}
pub fn escape(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        if b == 0 {
            out.extend([0, 255]);
        } else {
            out.push(b);
        }
    }
    out.extend([0, 0]);
}
pub fn unescape(data: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < data.len() {
        let b = data[i];
        i += 1;
        if b == 0 {
            let next = *data
                .get(i)
                .ok_or_else(|| Error::corrupt("truncated key escape"))?;
            i += 1;
            match next {
                0 => return Ok((out, i)),
                255 => out.push(0),
                _ => return Err(Error::corrupt("key escape")),
            }
        } else {
            out.push(b);
        }
    }
    Err(Error::corrupt("unterminated key"))
}
pub fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(byte) = end.pop() {
        if byte != 255 {
            end.push(byte + 1);
            return Some(end);
        }
    }
    None
}
pub fn project(data: &[u8], paths: &[String]) -> Result<Value> {
    if paths.is_empty() {
        return decode(data);
    }
    let mut result = Value::Object(Map::new());
    let mut sorted = paths.to_vec();
    sorted.sort();
    sorted.dedup();
    for path in &sorted {
        if let Some((node, containers)) = locate(data, path)? {
            let value = decode_node(node, 0)?;
            let parts = segments(path)?;
            insert_projected(&mut result, &parts, &containers, value)?;
        }
    }
    Ok(result)
}
fn insert_projected(
    target: &mut Value,
    parts: &[String],
    arrays: &[bool],
    value: Value,
) -> Result<()> {
    let child = if arrays[0] {
        let array = target
            .as_array_mut()
            .ok_or_else(|| Error::corrupt("projection container type"))?;
        let index = parts[0]
            .parse::<usize>()
            .map_err(|_| Error::corrupt("projection array index"))?;
        if index >= 16_384 {
            return Err(Error::limit("projection array size"));
        }
        array.resize_with(array.len().max(index + 1), || Value::Null);
        &mut array[index]
    } else {
        target
            .as_object_mut()
            .ok_or_else(|| Error::corrupt("projection container type"))?
            .entry(parts[0].clone())
            .or_insert(Value::Null)
    };
    if parts.len() == 1 {
        *child = value;
    } else {
        if child.is_null() {
            *child = if arrays[1] {
                Value::Array(Vec::new())
            } else {
                Value::Object(Map::new())
            };
        }
        insert_projected(child, &parts[1..], &arrays[1..], value)?;
    }
    Ok(())
}

pub fn put_u32(out: &mut Vec<u8>, n: u32) {
    out.extend(n.to_le_bytes());
}
pub fn put_u64(out: &mut Vec<u8>, n: u64) {
    out.extend(n.to_le_bytes());
}
pub fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(out, bytes.len() as u32);
    out.extend(bytes);
}
pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let bytes = slice(self.data, self.pos, n)?;
        self.pos += n;
        Ok(bytes)
    }
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.u32()? as usize;
        self.take(len)
    }
    pub fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| Error::corrupt("invalid UTF-8"))
    }
    pub fn remaining(&self) -> &'a [u8] {
        &self.data[self.pos..]
    }
}
