use super::*;
use crate::{ReadCancellation, ReadOptions, TenantHandle, TenantReader, read::ReadBudget};

pub(super) const HEADERS: [&str; 3] = [
    "x-tkv-read-timeout-ms",
    "x-tkv-max-scan",
    "x-tkv-allow-full-scan",
];
pub(super) fn supports(route: &str, method: &Method) -> bool {
    if route.contains("/identity/") || route.starts_with("/v1/replication/") {
        return false;
    }
    (*method == Method::GET
        && (route.ends_with("/documents/{key}") || route.ends_with("/resources/{resource}")))
        || (*method == Method::POST
            && [
                "/multi_get",
                "/read_batch",
                "/query",
                "/explain",
                "/execute",
                "/dsl",
                "/resources/{resource}/read",
            ]
            .iter()
            .any(|suffix| route.ends_with(suffix)))
}
pub(super) fn parse(headers: &HeaderMap) -> Result<Option<ReadOptions>> {
    if !HEADERS.iter().any(|name| headers.contains_key(*name)) {
        return Ok(None);
    }
    let mut values = Vec::new();
    for name in HEADERS {
        let mut all = headers.get_all(name).iter();
        let value = all
            .next()
            .map(|v| v.to_str().map_err(|_| Error::invalid(name)))
            .transpose()?;
        if all.next().is_some() {
            return Err(Error::invalid("duplicate read control header"));
        }
        values.push(value);
    }
    let number = |s: &str| -> Result<u64> {
        if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
            return Err(Error::invalid("read control must be a positive integer"));
        }
        s.parse::<u64>()
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| Error::invalid("read control must be a positive integer"))
    };
    Ok(Some(ReadOptions {
        timeout_ms: values[0].map(number).transpose()?,
        max_scan: values[1]
            .map(number)
            .transpose()?
            .map(|n| usize::try_from(n).map_err(|_| Error::invalid("scan limit overflow")))
            .transpose()?,
        allow_full_scan: match values[2] {
            None | Some("true") => true,
            Some("false") => false,
            _ => return Err(Error::invalid("allow-full-scan must be true or false")),
        },
        cancellation: None,
    }))
}

// The handler owns this guard; dropping its future signals the blocking reader.
// A write never receives the token, even when it uses the shared DSL route.
pub(super) struct CancelOnDrop(pub(super) ReadCancellation);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) struct OperationContext<'a> {
    pub(super) principal: &'a str,
    pub(super) budget: Option<&'a ReadBudget>,
}
impl OperationContext<'_> {
    pub(super) fn reader<'a>(&self, tenant: &'a TenantHandle) -> Result<TenantReader<'a>> {
        let mut budget = self
            .budget
            .cloned()
            .ok_or_else(|| Error::new("INTERNAL", "missing read budget"))?;
        budget.principal = Some(self.principal.into());
        Ok(TenantReader { tenant, budget })
    }
}
