use crate::Result;
use serde::Serialize;
use std::io::{self, Write};

/// Exact JSON wire size without allocating a throwaway serialization buffer.
pub(crate) fn encoded_len<T: Serialize + ?Sized>(value: &T) -> Result<usize> {
    struct Count(usize);
    impl Write for Count {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("JSON size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, value)?;
    Ok(count.0)
}

/// Conservative live-tree bound before cloning parameter values. Depth is
/// checked before serialization as embedded callers can construct arbitrary JSON.
pub(crate) fn value_memory(value: &serde_json::Value) -> Result<usize> {
    fn visit(value: &serde_json::Value, depth: usize) -> Result<usize> {
        use serde_json::Value;
        if depth > 64 {
            return Err(crate::Error::limit("DSL parameter nesting exceeds 64"));
        }
        let mut bytes = 128usize;
        match value {
            Value::String(s) => bytes = bytes.saturating_add(s.len().saturating_mul(2)),
            Value::Number(n) => {
                crate::codec::number_kind(n)?;
            }
            Value::Array(values) => {
                for v in values {
                    bytes = bytes.saturating_add(visit(v, depth + 1)?);
                }
            }
            Value::Object(values) => {
                for (key, v) in values {
                    bytes = bytes
                        .saturating_add(key.len() * 2 + 96)
                        .saturating_add(visit(v, depth + 1)?);
                }
            }
            _ => {}
        }
        Ok(bytes)
    }
    visit(value, 0)
}

pub(crate) fn allocation_bound(bytes: &[u8]) -> usize {
    // Bound value/tree overhead before serde allocates the request's JSON tree.
    // Delimiters inside strings are data, not additional containers or values.
    let mut strings = false;
    let mut escaped = false;
    let mut slots = 1usize;
    for byte in bytes {
        if strings {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                strings = false;
            }
        } else if *byte == b'"' {
            strings = true;
        } else if matches!(byte, b'{' | b'[' | b',' | b':') {
            slots += 1;
        }
    }
    bytes
        .len()
        .saturating_mul(3)
        .saturating_add(slots.saturating_mul(192))
}
