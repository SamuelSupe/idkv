use crate::{Error, Result, codec, model::*};
use std::collections::HashSet;

pub(super) enum Compiled {
    All,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Exists(codec::FieldPath, bool),
    Compare(codec::FieldPath, CompareOp, Vec<u8>),
    In(codec::FieldPath, HashSet<Vec<u8>>),
}
impl Compiled {
    pub fn new(filter: Option<&Filter>) -> Result<Self> {
        Ok(match filter {
            None => Self::All,
            Some(Filter::And { and }) => Self::And(
                and.iter()
                    .map(|f| Self::new(Some(f)))
                    .collect::<Result<_>>()?,
            ),
            Some(Filter::Or { or }) => Self::Or(
                or.iter()
                    .map(|f| Self::new(Some(f)))
                    .collect::<Result<_>>()?,
            ),
            Some(Filter::Not { not }) => Self::Not(Box::new(Self::new(Some(not))?)),
            Some(Filter::Compare { field, op, value }) => {
                let path = codec::FieldPath::new(field)?;
                match op {
                    CompareOp::Exists => Self::Exists(path, value.as_bool().unwrap()),
                    CompareOp::In => Self::In(
                        path,
                        value
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|v| codec::scalar(Some(v), &Order::Asc))
                            .collect::<Result<_>>()?,
                    ),
                    _ => Self::Compare(path, op.clone(), codec::scalar(Some(value), &Order::Asc)?),
                }
            }
        })
    }
    pub fn matches(&self, data: &[u8], scratch: &mut Vec<u8>) -> Result<bool> {
        Ok(match self {
            Self::All => true,
            Self::And(parts) => {
                for part in parts {
                    if !part.matches(data, scratch)? {
                        return Ok(false);
                    }
                }
                true
            }
            Self::Or(parts) => {
                for part in parts {
                    if part.matches(data, scratch)? {
                        return Ok(true);
                    }
                }
                false
            }
            Self::Not(inner) => !inner.matches(data, scratch)?,
            Self::Exists(path, expected) => path.exists(data)? == *expected,
            Self::In(path, values) => {
                path.scalar(data, scratch)? && values.contains(scratch.as_slice())
            }
            Self::Compare(path, op, value) => {
                if !path.scalar(data, scratch)? {
                    return Ok(*op == CompareOp::Ne);
                }
                match op {
                    CompareOp::Eq => scratch == value,
                    CompareOp::Ne => scratch != value,
                    _ if scratch[0] != value[0] => false,
                    CompareOp::Gt => scratch.as_slice() > value.as_slice(),
                    CompareOp::Gte => scratch.as_slice() >= value.as_slice(),
                    CompareOp::Lt => scratch.as_slice() < value.as_slice(),
                    CompareOp::Lte => scratch.as_slice() <= value.as_slice(),
                    _ => unreachable!(),
                }
            }
        })
    }
}

pub(super) fn validate_query(query: &Query, max_page_size: usize) -> Result<()> {
    if query.limit == 0
        || query.limit > max_page_size
        || query.sort.len() > 8
        || query.select.len() > 64
    {
        return Err(Error::invalid("query limits"));
    }
    let mut seen = HashSet::new();
    for field in &query.sort {
        codec::validate_pointer(&field.field)?;
        if !seen.insert(&field.field) {
            return Err(Error::invalid("duplicate sort field"));
        }
    }
    for path in &query.select {
        codec::validate_pointer(path)?;
    }
    if let Some(filter) = &query.filter {
        let mut count = 0;
        validate_filter(filter, 0, &mut count)?;
    }
    Ok(())
}
fn validate_filter(filter: &Filter, depth: usize, count: &mut usize) -> Result<()> {
    *count += 1;
    if depth > 16 || *count > 128 {
        return Err(Error::limit("filter complexity"));
    }
    match filter {
        Filter::And { and } => {
            for f in and {
                validate_filter(f, depth + 1, count)?;
            }
        }
        Filter::Or { or } => {
            for f in or {
                validate_filter(f, depth + 1, count)?;
            }
        }
        Filter::Not { not } => validate_filter(not, depth + 1, count)?,
        Filter::Compare { field, op, value } => {
            codec::validate_pointer(field)?;
            match op {
                CompareOp::Exists => {
                    if !value.is_boolean() {
                        return Err(Error::invalid("exists requires a boolean value"));
                    }
                }
                CompareOp::In => {
                    let values = value
                        .as_array()
                        .ok_or_else(|| Error::invalid("in requires an array"))?;
                    if values.len() > 1000 {
                        return Err(Error::limit("in value count"));
                    }
                    for value in values {
                        codec::scalar(Some(value), &Order::Asc)?;
                    }
                }
                _ => {
                    codec::scalar(Some(value), &Order::Asc)?;
                }
            }
        }
    }
    Ok(())
}
pub(super) fn normalized_sort(sort: &[IndexField]) -> Vec<IndexField> {
    let mut fields = sort.to_vec();
    if !fields.iter().any(|f| f.field == "/_key") {
        fields.push(IndexField {
            field: "/_key".into(),
            order: Order::Asc,
        });
    }
    fields
}
pub(super) struct SortKey(Vec<(codec::FieldPath, Order)>);
impl SortKey {
    pub fn new(fields: &[IndexField]) -> Result<Self> {
        fields
            .iter()
            .map(|f| Ok((codec::FieldPath::new(&f.field)?, f.order.clone())))
            .collect::<Result<Vec<_>>>()
            .map(Self)
    }
    pub fn encode(&self, data: &[u8], scratch: &mut Vec<u8>) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        for (field, order) in &self.0 {
            if !field.scalar(data, scratch)? {
                return Err(Error::invalid("sort fields must be scalar"));
            }
            if *order == Order::Desc {
                for byte in scratch.iter_mut() {
                    *byte = !*byte;
                }
            }
            out.extend_from_slice(scratch);
        }
        Ok(out)
    }
}
pub(super) fn paths(filter: Option<&Filter>, out: &mut Vec<String>) {
    match filter {
        Some(Filter::Compare { field, .. }) => out.push(field.clone()),
        Some(Filter::And { and }) => {
            for f in and {
                paths(Some(f), out);
            }
        }
        Some(Filter::Or { or }) => {
            for f in or {
                paths(Some(f), out);
            }
        }
        Some(Filter::Not { not }) => paths(Some(not), out),
        None => {}
    }
}
