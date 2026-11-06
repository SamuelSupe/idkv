use crate::{Error, Result};
use chrono::{DateTime, Timelike};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub fn parse_time(value: &str) -> Result<i64> {
    if value.len() > 40 {
        return Err(Error::invalid("RFC3339 time exceeds 40 bytes"));
    }
    let date = DateTime::parse_from_rfc3339(value)
        .map_err(|_| Error::invalid("time must be RFC3339 with an explicit timezone"))?;
    if date.nanosecond() >= 1_000_000_000 || date.nanosecond() % 1000 != 0 {
        return Err(Error::invalid(
            "time must have microsecond precision; leap seconds are unsupported",
        ));
    }
    date.timestamp()
        .checked_mul(1_000_000)
        .and_then(|s| s.checked_add(i64::from(date.timestamp_subsec_micros())))
        .ok_or_else(|| Error::invalid("time is outside the supported microsecond range"))
}
fn wall() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::new("CLOCK_SKEW", "system clock precedes Unix epoch"))?;
    i64::try_from(duration.as_micros())
        .map_err(|_| Error::new("CLOCK_SKEW", "system time overflow"))
}
pub(crate) struct ClockAnchor {
    wall: i64,
    monotonic: Instant,
}
impl ClockAnchor {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            wall: wall()?,
            monotonic: Instant::now(),
        })
    }
    pub(crate) fn sample(&self, previous: i64, tolerance_ms: u64) -> Result<i64> {
        let now = wall()?;
        let elapsed = i64::try_from(self.monotonic.elapsed().as_micros())
            .map_err(|_| Error::new("CLOCK_SKEW", "monotonic time overflow"))?;
        Self::validate(
            now,
            self.wall.saturating_add(elapsed),
            previous,
            tolerance_ms,
        )
    }
    fn validate(now: i64, expected: i64, previous: i64, tolerance_ms: u64) -> Result<i64> {
        let tolerance = tolerance_ms.saturating_mul(1000);
        if now.abs_diff(expected) > tolerance
            || (now < previous && now.abs_diff(previous) > tolerance)
        {
            return Err(Error::new(
                "CLOCK_SKEW",
                "clock offset exceeds identity tolerance",
            ));
        }
        Ok(now.max(
            previous
                .checked_add(1)
                .ok_or_else(|| Error::new("CLOCK_SKEW", "time exhausted"))?,
        ))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn timezone_precision_and_clock_discontinuities() {
        assert_eq!(
            parse_time("2026-11-05T08:00:00+08:00").unwrap(),
            parse_time("2026-11-05T00:00:00Z").unwrap()
        );
        assert_eq!(parse_time("1969-12-31T23:59:59.999999Z").unwrap(), -1);
        for bad in [
            "2026-11-05T00:00:00",
            "2026-11-05T00:00:00.0000001Z",
            "2016-12-31T23:59:60Z",
        ] {
            assert!(parse_time(bad).is_err());
        }
        assert_eq!(ClockAnchor::validate(10, 10, 10, 1).unwrap(), 11);
        for (now, expected, previous) in [(100, 100, 2000), (5000, 100, 100), (100, 5000, 100)] {
            assert_eq!(
                ClockAnchor::validate(now, expected, previous, 1)
                    .unwrap_err()
                    .code,
                "CLOCK_SKEW"
            );
        }
    }
}
