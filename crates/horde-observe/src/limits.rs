//! Per-agent rate limiting for log reads (ADR-0015).
//!
//! A token bucket per agent: `burst` calls back to back, refilled at
//! `calls_per_minute`. Enforced at the agent's endpoint, and again at the
//! server for remote targets; per-agent buckets mean one agent cannot
//! starve another's reads.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::settings::AgentReadLimits;

/// One agent's bucket.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bucket {
    tokens: f64,
    updated: Instant,
}

impl Bucket {
    /// A full bucket at `now`.
    #[must_use]
    pub fn full(limits: &AgentReadLimits, now: Instant) -> Self {
        Self {
            tokens: f64::from(limits.burst),
            updated: now,
        }
    }

    /// Takes one call at `now`, or says how long until one is available.
    ///
    /// # Errors
    ///
    /// The wait until the next token.
    pub fn take(&mut self, limits: &AgentReadLimits, now: Instant) -> Result<(), Duration> {
        let per_second = f64::from(limits.calls_per_minute) / 60.0;
        let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();
        self.tokens = (self.tokens + elapsed * per_second).min(f64::from(limits.burst));
        self.updated = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return Ok(());
        }
        if per_second <= 0.0 {
            return Err(Duration::from_secs(60));
        }
        Err(Duration::from_secs_f64((1.0 - self.tokens) / per_second))
    }
}

/// Every agent's bucket.
#[derive(Debug)]
pub struct RateBuckets {
    limits: AgentReadLimits,
    buckets: Mutex<BTreeMap<String, Bucket>>,
}

impl RateBuckets {
    #[must_use]
    pub fn new(limits: AgentReadLimits) -> Self {
        Self {
            limits,
            buckets: Mutex::new(BTreeMap::new()),
        }
    }

    /// The limits these buckets enforce.
    #[must_use]
    pub fn limits(&self) -> &AgentReadLimits {
        &self.limits
    }

    /// Takes one call for `agent` at `now`.
    ///
    /// # Errors
    ///
    /// How long the agent must wait.
    pub fn take(&self, agent: &str, now: Instant) -> Result<(), Duration> {
        let mut buckets = match self.buckets.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        buckets
            .entry(agent.to_owned())
            .or_insert_with(|| Bucket::full(&self.limits, now))
            .take(&self.limits, now)
    }
}

/// The whole seconds a caller is told to wait, never zero.
#[must_use]
pub fn retry_after_secs(wait: Duration) -> u64 {
    wait.as_secs() + u64::from(wait.subsec_nanos() > 0)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{RateBuckets, retry_after_secs};
    use crate::settings::AgentReadLimits;

    #[test]
    fn a_burst_then_the_sustained_rate() {
        let buckets = RateBuckets::new(AgentReadLimits::default());
        let start = Instant::now();
        for _ in 0..3 {
            assert_eq!(buckets.take("a1", start), Ok(()));
        }
        let refused = buckets.take("a1", start);
        assert_eq!(
            refused,
            Err(Duration::from_secs(10)),
            "6/min is one per 10s"
        );
        assert_eq!(
            buckets.take("a1", start + Duration::from_secs(9)),
            Err(Duration::from_secs(1))
        );
        assert_eq!(buckets.take("a1", start + Duration::from_secs(10)), Ok(()));
        assert!(buckets.take("a1", start + Duration::from_secs(10)).is_err());
        assert_eq!(buckets.limits().burst, 3);
    }

    #[test]
    fn buckets_are_per_agent_and_refill_only_to_the_burst() {
        let buckets = RateBuckets::new(AgentReadLimits::default());
        let start = Instant::now();
        for _ in 0..3 {
            assert!(buckets.take("greedy", start).is_ok());
        }
        assert!(buckets.take("greedy", start).is_err());
        assert!(
            buckets.take("patient", start).is_ok(),
            "one agent cannot starve another"
        );
        let much_later = start + Duration::from_secs(3600);
        for _ in 0..3 {
            assert!(buckets.take("greedy", much_later).is_ok());
        }
        assert!(
            buckets.take("greedy", much_later).is_err(),
            "never above the burst"
        );
    }

    #[test]
    fn the_buckets_report_the_limits_they_enforce() {
        let limits = AgentReadLimits {
            default_bytes: 1,
            max_bytes: 2,
            calls_per_minute: 3,
            burst: 4,
        };
        assert_eq!(*RateBuckets::new(limits).limits(), limits);
    }

    #[test]
    fn a_zero_rate_waits_a_minute() {
        let limits = AgentReadLimits {
            calls_per_minute: 0,
            burst: 1,
            ..AgentReadLimits::default()
        };
        let buckets = RateBuckets::new(limits);
        let now = Instant::now();
        assert!(buckets.take("a", now).is_ok());
        assert_eq!(buckets.take("a", now), Err(Duration::from_secs(60)));
    }

    #[test]
    fn retry_after_rounds_up_and_is_never_zero() {
        assert_eq!(retry_after_secs(Duration::from_secs(10)), 10);
        assert_eq!(retry_after_secs(Duration::from_millis(10_001)), 11);
        assert_eq!(retry_after_secs(Duration::from_millis(1)), 1);
    }
}
