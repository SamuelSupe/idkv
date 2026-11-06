use super::{Binding, Identifier, Timeline};
use crate::{Error, Result};
use uuid::Uuid;

pub(super) fn validate_identifier(id: &Identifier) -> Result<()> {
    for (value, max) in [(&id.kind, 64), (&id.namespace, 256), (&id.value, 2048)] {
        if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
            return Err(Error::invalid(
                "identifier fields must be nonempty bounded UTF-8 without control characters",
            ));
        }
    }
    Ok(())
}
pub(super) fn end(value: Option<i64>) -> i128 {
    value.map_or(i128::MAX, i128::from)
}
pub(super) fn overlaps(b: &Binding, from: i64, to: Option<i64>) -> bool {
    i128::from(b.valid_from_us) < end(to) && i128::from(from) < end(b.valid_to_us)
}
pub(super) fn next_boundary(timeline: &Timeline, at: i64) -> Option<i64> {
    timeline
        .segments
        .iter()
        .flat_map(|b| [Some(b.valid_from_us), b.valid_to_us])
        .flatten()
        .filter(|v| *v > at)
        .min()
}
fn same(a: &Binding, b: &Binding) -> bool {
    a.entity_id == b.entity_id
        && a.source == b.source
        && a.evidence_id == b.evidence_id
        && a.assurance == b.assurance
}
fn semantic_eq(a: &Binding, b: &Binding) -> bool {
    same(a, b) && a.valid_from_us == b.valid_from_us && a.valid_to_us == b.valid_to_us
}
pub(super) fn covered_by(timeline: &Timeline, from: i64, to: Option<i64>, entity: Uuid) -> bool {
    let mut reached = i128::from(from);
    for b in &timeline.segments {
        if !overlaps(b, from, to) {
            continue;
        }
        if i128::from(b.valid_from_us) > reached || b.entity_id != entity {
            return false;
        }
        reached = end(b.valid_to_us);
        if reached >= end(to) {
            return true;
        }
    }
    false
}
/// Replace precisely one half-open interval; canonicalize equal adjacent facts.
/// UUIDs are reused for untouched versions, while historical documents remain immutable.
pub(super) fn replace(
    timeline: &mut Timeline,
    from: i64,
    to: Option<i64>,
    new: Option<Binding>,
    exclusive: bool,
    max: usize,
) -> Result<Vec<Binding>> {
    if exclusive
        && let Some(new) = &new
        && timeline
            .segments
            .iter()
            .any(|b| overlaps(b, from, to) && b.entity_id != new.entity_id)
    {
        return Err(Error::new(
            "INTERVAL_CONFLICT",
            "identifier already belongs to another entity in this interval",
        ));
    }
    let old = &timeline.segments;
    let mut next = Vec::with_capacity(old.len() + 2);
    let replaced: Vec<_> = old
        .iter()
        .filter(|b| overlaps(b, from, to))
        .cloned()
        .collect();
    for b in old {
        if !overlaps(b, from, to) {
            next.push(b.clone());
            continue;
        }
        if b.valid_from_us < from {
            let mut left = b.clone();
            left.valid_to_us = Some(from);
            next.push(left);
        }
        if let Some(to) = to
            && end(b.valid_to_us) > i128::from(to)
        {
            let mut right = b.clone();
            right.valid_from_us = to;
            next.push(right);
        }
    }
    if let Some(b) = new {
        next.push(b);
    }
    next.sort_by_key(|b| b.valid_from_us);
    let mut merged: Vec<Binding> = Vec::with_capacity(next.len());
    for b in next {
        if let Some(last) = merged.last_mut()
            && last.valid_to_us == Some(b.valid_from_us)
            && same(last, &b)
        {
            last.valid_to_us = b.valid_to_us;
        } else {
            merged.push(b);
        }
    }
    if merged.len() > max {
        return Err(Error::limit("identity timeline segment limit"));
    }
    if old.len() == merged.len() && old.iter().zip(&merged).all(|(a, b)| semantic_eq(a, b)) {
        return Ok(Vec::new());
    }
    for b in &mut merged {
        if let Some(original) = old
            .binary_search_by_key(&b.valid_from_us, |o| o.valid_from_us)
            .ok()
            .map(|i| &old[i])
            .filter(|o| semantic_eq(o, b))
        {
            *b = original.clone();
        } else {
            b.version_id = Uuid::new_v4();
        }
    }
    timeline.segments = merged;
    timeline.revision = Uuid::new_v4();
    Ok(replaced)
}
