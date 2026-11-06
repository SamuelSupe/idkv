use super::filter;
use crate::{
    Result, codec,
    model::*,
    storage::ScanRange,
    tenant::{
        catalog::{document_key, index_prefix, prefix},
        indexes::covers,
    },
};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone)]
pub(super) enum Access {
    Primary(Vec<String>),
    PrimaryRange(ScanRange),
    Index {
        index: IndexDefinition,
        ranges: Vec<ScanRange>,
        exact: bool,
        ordered: bool,
        covering: bool,
    },
    Scan,
}
pub(super) fn branches(filter: Option<&Filter>) -> Vec<Option<&Filter>> {
    match filter {
        Some(Filter::Or { or }) => or.iter().flat_map(|f| branches(Some(f))).collect(),
        _ => vec![filter],
    }
}
fn predicates<'a>(filter: Option<&'a Filter>, out: &mut Vec<(&'a str, &'a CompareOp, &'a Value)>) {
    match filter {
        Some(Filter::Compare { field, op, value }) => out.push((field, op, value)),
        Some(Filter::And { and }) => {
            for f in and {
                predicates(Some(f), out);
            }
        }
        _ => {}
    }
}
pub(super) fn plan(
    collection: Uuid,
    predicate: Option<&Filter>,
    query: &Query,
    indexes: &[IndexDefinition],
    seq: u64,
) -> Result<Access> {
    let mut conditions = Vec::new();
    predicates(predicate, &mut conditions);
    for (field, op, value) in &conditions {
        if *field == "/_key" {
            if **op == CompareOp::Eq
                && let Some(key) = value.as_str()
            {
                return Ok(Access::Primary(vec![key.into()]));
            }
            if **op == CompareOp::In
                && let Some(values) = value.as_array()
                && values.iter().all(Value::is_string)
            {
                let mut keys: Vec<String> = values
                    .iter()
                    .map(|v| v.as_str().unwrap().to_owned())
                    .collect();
                keys.sort();
                keys.dedup();
                return Ok(Access::Primary(keys));
            }
        }
    }
    let sort = filter::normalized_sort(&query.sort);
    let mut primary = None;
    for (field, op, value) in &conditions {
        if *field == "/_key"
            && is_range(op)
            && let Some(key) = value.as_str()
        {
            let range = primary.get_or_insert_with(|| {
                let start = prefix(b'D', collection);
                let end = codec::prefix_end(&start);
                ScanRange {
                    start,
                    end,
                    prefix: None,
                }
            });
            bound(range, document_key(collection, key), op, &Order::Asc);
        }
    }
    let mut paths = query.select.clone();
    filter::paths(predicate, &mut paths);
    paths.extend(sort.iter().map(|f| f.field.clone()));
    let mut best = None;
    for index in indexes
        .iter()
        .filter(|i| i.state == IndexState::Ready && i.ready_seq <= seq)
    {
        // Sparse unique indexes are complete only when every component is
        // proven present and non-null by a conjunctive predicate.
        if index.spec.unique
            && !index.spec.fields.iter().all(|field| {
                conditions.iter().any(|(path, op, value)| {
                    *path == field.field
                        && match op {
                            CompareOp::Eq => !value.is_null(),
                            CompareOp::In => value
                                .as_array()
                                .is_some_and(|v| v.iter().all(|v| !v.is_null())),
                            op if is_range(op) => !value.is_null(),
                            _ => false,
                        }
                })
            })
        {
            continue;
        }
        let mut bases = vec![index_prefix(collection, index.generation)];
        let mut equals = 0;
        let mut fixed = Vec::new();
        for field in &index.spec.fields {
            let condition = conditions
                .iter()
                .find(|(path, op, _)| *path == field.field && **op == CompareOp::Eq)
                .or_else(|| {
                    conditions
                        .iter()
                        .find(|(path, op, _)| *path == field.field && **op == CompareOp::In)
                });
            if let Some((_, op, value)) = condition {
                if **op == CompareOp::Eq {
                    let suffix = codec::scalar(Some(value), &field.order)?;
                    if bases.iter().map(Vec::len).sum::<usize>() + bases.len() * suffix.len()
                        > 1 << 20
                    {
                        break;
                    }
                    for base in &mut bases {
                        base.extend(&suffix);
                    }
                    fixed.push(field.field.as_str());
                    equals += 1;
                    continue;
                }
                let mut encoded = value
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| codec::scalar(Some(v), &field.order))
                    .collect::<Result<Vec<_>>>()?;
                encoded.sort();
                encoded.dedup();
                if encoded.is_empty() {
                    return Ok(Access::Primary(vec![]));
                }
                // Bound Cartesian expansion independently of the input size.
                let bytes = bases.iter().map(Vec::len).sum::<usize>() * encoded.len()
                    + encoded.iter().map(Vec::len).sum::<usize>() * bases.len();
                if bases.len() * encoded.len() > 256 || bytes > 1 << 20 {
                    break;
                }
                if encoded.len() == 1 {
                    fixed.push(field.field.as_str());
                }
                bases = bases
                    .into_iter()
                    .flat_map(|base| {
                        encoded.iter().map(move |suffix| {
                            let mut key = base.clone();
                            key.extend(suffix);
                            key
                        })
                    })
                    .collect();
                equals += 1;
            } else {
                break;
            }
        }
        let exact = equals == index.spec.fields.len();
        bases.sort();
        let mut ranges = Vec::with_capacity(bases.len());
        let mut range = false;
        for base in bases {
            if exact && index.spec.unique {
                ranges.push(ScanRange {
                    start: base,
                    end: None,
                    prefix: None,
                });
                continue;
            }
            let mut span = ScanRange {
                start: base.clone(),
                end: codec::prefix_end(&base),
                prefix: if exact { Some(base.clone()) } else { None },
            };
            if !exact {
                let field = &index.spec.fields[equals];
                for (_, op, value) in conditions.iter().filter(|(path, op, _)| {
                    *path == field.field
                        && matches!(
                            op,
                            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
                        )
                }) {
                    let encoded = codec::scalar(Some(value), &field.order)?;
                    let mut type_start = base.clone();
                    type_start.push(encoded[0]);
                    span.start = span.start.max(type_start.clone());
                    span.end = min_end(span.end, codec::prefix_end(&type_start));
                    let mut bound = base.clone();
                    bound.extend(encoded);
                    self::bound(&mut span, bound, op, &field.order);
                    range = true;
                }
            }
            if span.end.as_ref().is_none_or(|end| span.start < *end) {
                ranges.push(span);
            }
        }
        let mut index_sort = index.spec.fields[equals..].to_vec();
        index_sort.push(IndexField {
            field: "/_key".into(),
            order: Order::Asc,
        });
        let requested_sort: Vec<_> = sort
            .iter()
            .filter(|f| !fixed.contains(&f.field.as_str()))
            .cloned()
            .collect();
        let ordered =
            ranges.len() <= 1 && ((index.spec.unique && exact) || index_sort == requested_sort);
        let covering = !query.select.is_empty() && covers(index, &paths);
        if equals == 0 && !range && !ordered {
            continue;
        }
        let score = equals * 100
            + usize::from(index.spec.unique && exact) * 10000
            + usize::from(ordered) * 50
            + usize::from(covering) * 20
            + usize::from(range) * 10;
        let rank = (score, std::cmp::Reverse(index.entries));
        if best.as_ref().is_none_or(|(old, _)| rank > *old) {
            best = Some((
                rank,
                Access::Index {
                    index: index.clone(),
                    ranges,
                    exact,
                    ordered,
                    covering,
                },
            ));
        }
    }
    Ok(best.map_or_else(
        || primary.map_or(Access::Scan, Access::PrimaryRange),
        |(_, access)| access,
    ))
}
fn is_range(op: &CompareOp) -> bool {
    matches!(
        op,
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
    )
}
fn bound(range: &mut ScanRange, key: Vec<u8>, op: &CompareOp, order: &Order) {
    let lower = matches!(op, CompareOp::Gt | CompareOp::Gte) ^ (*order == Order::Desc);
    let inclusive = matches!(op, CompareOp::Gte | CompareOp::Lte);
    if lower {
        range.start = std::mem::take(&mut range.start).max(if inclusive {
            key
        } else {
            codec::prefix_end(&key).unwrap_or_else(|| vec![255])
        });
    } else {
        range.end = min_end(
            range.end.take(),
            if inclusive {
                codec::prefix_end(&key)
            } else {
                Some(key)
            },
        );
    }
}
fn min_end(a: Option<Vec<u8>>, b: Option<Vec<u8>>) -> Option<Vec<u8>> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, None) | (None, a) => a,
    }
}
pub(super) fn explain(accesses: &[Access], sort: &[IndexField]) -> Explain {
    let mut result = Explain {
        covering: !accesses.is_empty(),
        ..Explain::default()
    };
    for access in accesses {
        match access {
            Access::Primary(keys) => {
                result.indexes.push("_key".into());
                result.covering = false;
                result.extra_sort |= keys.len() > 1;
            }
            Access::Index {
                index,
                covering,
                ordered,
                ..
            } => {
                result.indexes.push(index.spec.name.clone());
                result.covering &= covering;
                result.extra_sort |= !ordered;
            }
            Access::Scan | Access::PrimaryRange(_) => {
                if matches!(access, Access::Scan) {
                    result.scan = true;
                } else {
                    result.indexes.push("_key".into());
                }
                result.covering = false;
                result.extra_sort |= sort
                    != [IndexField {
                        field: "/_key".into(),
                        order: Order::Asc,
                    }];
            }
        }
    }
    if accesses.len() > 1 {
        result.extra_sort = true;
    }
    result
}
