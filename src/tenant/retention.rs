//! Tenant-wide history retention. Deadlines and cleanup decisions share the tenant WAL.
mod gc;
use super::{TenantHandle, catalog::*};
use crate::{
    Error, ReadStats, Result,
    storage::{Snapshot, Transaction},
};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const POLICY: &[u8] = b"~policy";
const STATE: &[u8] = b"~gc";
const EXPIRY: &[u8] = b"~e/";
const DEADLINE: &[u8] = b"~d/";
const TOKENS: &[u8] = b"~t/";
pub(crate) const KNOWLEDGE: &[u8] = b"~k/";
pub(crate) const PUBLICATIONS: &[u8] = b"~p/";
pub(crate) const REFERENCES: &[u8] = b"~r/";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub retention_seconds: u64,
    pub activated_seq: u64,
    pub legacy_expires_at_ms: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetryContext {
    pub retry_token: Uuid,
    pub history: String,
    pub submit_before_ms: u64,
}
#[derive(Serialize, Deserialize)]
struct RetryToken {
    context: RetryContext,
    principal: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Expiration {
    Delete,
    Publication {
        version: Uuid,
        collection: Uuid,
        name: String,
    },
    Knowledge {
        ordinal: u64,
    },
}
#[derive(Serialize, Deserialize)]
struct Scheduled {
    target: Vec<u8>,
    action: Expiration,
}

pub(crate) fn now_ms() -> Result<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::new("CLOCK_SKEW", "system time precedes epoch"))?
            .as_millis(),
    )
    .map_err(|_| Error::new("CLOCK_SKEW", "system time overflow"))
}
fn deadline(now: u64, seconds: u64) -> Result<u64> {
    now.checked_add(
        seconds
            .checked_mul(1000)
            .ok_or_else(|| Error::invalid("retention overflow"))?,
    )
    .ok_or_else(|| Error::invalid("retention overflow"))
}
pub(crate) fn key(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    [prefix, suffix].concat()
}
pub(crate) fn policy(tx: &Transaction<'_>) -> Result<Option<RetentionPolicy>> {
    Ok(tx
        .cached_json::<RetentionPolicy>(POLICY)?
        .map(|p| (**p).clone()))
}
fn transaction_time(tx: &Transaction<'_>) -> Result<u64> {
    let watermark = tx
        .cached_json::<gc::State>(STATE)?
        .map_or(0, |s| s.watermark_ms);
    Ok(now_ms()?.max(watermark))
}
fn snapshot_policy(snapshot: &Snapshot) -> Result<Option<RetentionPolicy>> {
    snapshot
        .get(POLICY, &mut ReadStats::default())?
        .map(|b| parse(&b))
        .transpose()
}
fn expiry_key(at: u64, target: &[u8]) -> Vec<u8> {
    [EXPIRY, &at.to_be_bytes(), target].concat()
}
pub(crate) fn schedule_at(
    tx: &mut Transaction<'_>,
    target: &[u8],
    at: u64,
    action: Expiration,
) -> Result<()> {
    let dk = key(DEADLINE, target);
    if tx.get(&dk)?.is_some() {
        return Ok(());
    }
    let queue_key = expiry_key(at, target);
    if let Some(state) = tx.cached_json::<gc::State>(STATE)?
        && state
            .expiry_after
            .as_ref()
            .is_some_and(|after| queue_key <= *after)
    {
        let mut state = (**state).clone();
        // Backdated insertions must be visited without restarting at all
        // historical tombstones. A strict prefix sorts before this key.
        state.expiry_after = Some(queue_key[..queue_key.len() - 1].to_vec());
        put(tx, STATE.to_vec(), &state)?;
    }
    put(tx, dk, &at)?;
    put(
        tx,
        queue_key,
        &Scheduled {
            target: target.to_vec(),
            action,
        },
    )
}
pub(crate) fn schedule(tx: &mut Transaction<'_>, target: &[u8], action: Expiration) -> Result<()> {
    if let Some(p) = policy(tx)? {
        schedule_at(
            tx,
            target,
            deadline(transaction_time(tx)?, p.retention_seconds)?,
            action,
        )?;
    }
    Ok(())
}
pub(crate) fn schedule_receipt(
    tx: &mut Transaction<'_>,
    target: &[u8],
    token: Option<Uuid>,
) -> Result<()> {
    if let Some(p) = policy(tx)? {
        let mut at = deadline(transaction_time(tx)?, p.retention_seconds)?;
        if let Some(id) = token
            && let Some(bytes) = tx.get(&key(TOKENS, id.as_bytes()))?
        {
            let issued: RetryToken = parse(&bytes)?;
            // A reduced policy must not delete a receipt while its original
            // token can still authorize a first submission.
            at = at.max(issued.context.submit_before_ms);
        }
        schedule_at(tx, target, at, Expiration::Delete)?;
    }
    Ok(())
}
pub(crate) fn check_tx(
    tx: &Transaction<'_>,
    target: &[u8],
    legacy: bool,
    code: &str,
) -> Result<()> {
    let at: Option<u64> = tx
        .get(&key(DEADLINE, target))?
        .map(|b| parse(&b))
        .transpose()?;
    let at = match at {
        Some(at) => Some(at),
        None if legacy => policy(tx)?.map(|p| p.legacy_expires_at_ms),
        None => None,
    };
    let now = transaction_time(tx)?;
    if at.is_some_and(|at| now >= at) {
        return Err(Error::new(
            code,
            "retained history has expired; do not replay as a new operation",
        ));
    }
    Ok(())
}
pub(crate) fn check_snapshot(
    snapshot: &Snapshot,
    target: &[u8],
    legacy: bool,
    code: &str,
) -> Result<()> {
    let at: Option<u64> = snapshot
        .get(&key(DEADLINE, target), &mut ReadStats::default())?
        .map(|b| parse(&b))
        .transpose()?;
    let at = match at {
        Some(at) => Some(at),
        None if legacy => snapshot_policy(snapshot)?.map(|p| p.legacy_expires_at_ms),
        None => None,
    };
    let watermark = snapshot
        .get(STATE, &mut ReadStats::default())?
        .map(|b| parse::<gc::State>(&b))
        .transpose()?
        .map_or(0, |s| s.watermark_ms);
    let now = now_ms()?.max(watermark);
    if at.is_some_and(|at| now >= at) {
        return Err(Error::new(code, "retained history has expired"));
    }
    Ok(())
}
/// Existing receipts are checked before this function: role changes may replay a
/// durable result, but a missing receipt never authorizes a write in another history.
pub(crate) fn authorize_new(
    tx: &Transaction<'_>,
    token: Option<Uuid>,
    principal: &str,
    history: &str,
) -> Result<()> {
    if policy(tx)?.is_none() && token.is_none() {
        return Ok(());
    }
    let id = token.ok_or_else(|| {
        Error::new(
            "RETRY_TOKEN_REQUIRED",
            "obtain a retry context before the first submission",
        )
    })?;
    let bytes = tx.get(&key(TOKENS, id.as_bytes()))?.ok_or_else(|| {
        Error::new(
            "IDEMPOTENCY_EXPIRED",
            "retry token is expired or absent from this history",
        )
    })?;
    let stored: RetryToken = parse(&bytes)?;
    if stored.principal != principal {
        return Err(Error::new(
            "FORBIDDEN",
            "retry token belongs to another principal",
        ));
    }
    if stored.context.history != history {
        return Err(Error::new(
            "HISTORY_CONFLICT",
            "retry token belongs to another history",
        ));
    }
    // Receipt cleanup and first-submission authorization must use the same
    // durable clock floor, even if the wall clock moves backwards mid-sweep.
    if transaction_time(tx)? >= stored.context.submit_before_ms {
        return Err(Error::new(
            "IDEMPOTENCY_EXPIRED",
            "submission window expired; do not obtain a new token to retry",
        ));
    }
    Ok(())
}

impl TenantHandle {
    /// One duration for superseded publication/knowledge states and committed
    /// receipts. Existing fixed deadlines are never shortened by a policy change.
    pub fn set_retention(&self, retention_seconds: u64) -> Result<RetentionPolicy> {
        if !(1..=315_360_000).contains(&retention_seconds) {
            return Err(Error::invalid("retention_seconds must be 1..315360000"));
        }
        if self.engine().options.max_batch_operations < 16
            || self.engine().options.max_batch_bytes < 16384
        {
            return Err(Error::invalid(
                "retention requires an expanded batch budget of at least 16 records and 16 KiB",
            ));
        }
        let _permit = self.core.gate.enter()?;
        self.touch();
        self.engine().transact(|tx| {
            let mut p = policy(tx)?.unwrap_or(RetentionPolicy {
                retention_seconds,
                activated_seq: tx.seq,
                legacy_expires_at_ms: deadline(transaction_time(tx)?, retention_seconds)?,
            });
            p.retention_seconds = retention_seconds;
            if tx.get(STATE)?.is_none() {
                put(tx, STATE.to_vec(), &gc::State::default())?;
            }
            put(tx, POLICY.to_vec(), &p)?;
            Ok(p)
        })
    }
    pub fn retention_status(&self) -> Result<serde_json::Value> {
        let _permit = self.core.gate.enter()?;
        let snapshot = self.engine().snapshot()?;
        let state: Option<gc::State> = snapshot
            .get(STATE, &mut ReadStats::default())?
            .map(|b| parse(&b))
            .transpose()?;
        let mut stats = ReadStats::default();
        let mut scan = snapshot.scan(
            EXPIRY.to_vec(),
            crate::codec::prefix_end(EXPIRY),
            None,
            &mut stats,
        )?;
        let next = scan
            .next(&mut stats)?
            .map(|r| {
                r.key
                    .get(EXPIRY.len()..EXPIRY.len() + 8)
                    .and_then(|b| b.try_into().ok())
                    .map(u64::from_be_bytes)
                    .ok_or_else(|| Error::corrupt("expiration index key"))
            })
            .transpose()?;
        let now = now_ms()?.max(state.as_ref().map_or(0, |s| s.watermark_ms));
        Ok(
            serde_json::json!({"policy":snapshot_policy(&snapshot)?,"gc":state.map(|s|s.status()),
            "next_expiration_ms":next,"expiration_lag_ms":next.map_or(0,|at|now.saturating_sub(at)),
            "physical_reclamation":"as snapshots release and compaction completes"}),
        )
    }
    /// Persist this context with the original request. Never refresh it to retry
    /// an uncertain write. Token issuance itself has no business side effects.
    pub fn retry_context(&self, principal: &str) -> Result<RetryContext> {
        name(principal)?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        self.engine().transact(|tx| {
            let p = policy(tx)?.ok_or_else(|| {
                Error::new("RETENTION_DISABLED", "tenant retention is not configured")
            })?;
            let context = RetryContext {
                retry_token: Uuid::new_v4(),
                history: self.history(),
                submit_before_ms: deadline(transaction_time(tx)?, p.retention_seconds)?,
            };
            let target = key(TOKENS, context.retry_token.as_bytes());
            put(
                tx,
                target.clone(),
                &RetryToken {
                    context: context.clone(),
                    principal: principal.into(),
                },
            )?;
            schedule_at(tx, &target, context.submit_before_ms, Expiration::Delete)?;
            Ok(context)
        })
    }
    pub(crate) fn retention_step(&self) -> Result<()> {
        gc::step(self)
    }
}

#[cfg(test)]
mod tests;
