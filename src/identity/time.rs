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
    sample: parking_lot::Mutex<(i64, Instant)>,
}
impl ClockAnchor {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            sample: parking_lot::Mutex::new((wall()?, Instant::now())),
        })
    }
    pub(crate) fn sample(&self, previous: i64, tolerance_ms: u64) -> Result<i64> {
        let mut anchor = self.sample.lock();
        let now = wall()?;
        let monotonic = Instant::now();
        Self::advance(&mut anchor, now, monotonic, previous, tolerance_ms)
    }
    fn advance(
        anchor: &mut (i64, Instant),
        now: i64,
        monotonic: Instant,
        previous: i64,
        tolerance_ms: u64,
    ) -> Result<i64> {
        let elapsed = i64::try_from(monotonic.duration_since(anchor.1).as_micros())
            .map_err(|_| Error::new("CLOCK_SKEW", "monotonic time overflow"))?;
        let recorded = Self::validate(
            now,
            anchor.0.saturating_add(elapsed),
            previous,
            tolerance_ms,
        )?;
        // Track normal clock discipline without forgiving a discontinuity: a
        // failed sample never moves the anchor or the durable previous time.
        *anchor = (now, monotonic);
        Ok(recorded)
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
    #[test]
    fn normal_clock_discipline_refreshes_the_anchor_but_jumps_still_fail_closed() {
        let start = Instant::now();
        let mut anchor = (1_000_000, start);
        let mut previous = 1_000_000;
        for tick in 1..20 {
            previous = ClockAnchor::advance(
                &mut anchor,
                1_000_000 + tick * 1500,
                start + std::time::Duration::from_micros(tick as u64 * 1000),
                previous,
                1,
            )
            .unwrap();
        }
        let unchanged = anchor;
        assert_eq!(
            ClockAnchor::advance(
                &mut anchor,
                2_000_000,
                start + std::time::Duration::from_millis(20),
                previous,
                1
            )
            .unwrap_err()
            .code,
            "CLOCK_SKEW"
        );
        assert_eq!(anchor, unchanged);
    }
}
