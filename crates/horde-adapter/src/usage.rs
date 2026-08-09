//! Token accounting across the adapter seam.
//!
//! The seam's contract is **per-turn deltas**. Harnesses disagree about this:
//! Claude Code reports `total_cost_usd` and `modelUsage` as running totals for
//! the lifetime of one child process, so a Worker that summed the numbers it
//! was handed would over-bill quadratically (spike W1c measured 0.0037 →
//! 0.0063 → 0.0078 across three turns of one process). Normalizing to deltas
//! *inside* each adapter means the Worker never has to know which harness
//! accumulates and which does not.
//!
//! [`UsageAccumulator`] exists so adapters that receive running totals can do
//! that subtraction in one audited place instead of reinventing it.

use core::ops::{Add, AddAssign, Sub};

/// Token counts for one turn.
///
/// Mirrors `horde.v1.TokenUsage`; the conversion happens in the Worker, at the
/// transport boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub cache_write_input_tokens: u64,
}

impl TokenUsage {
    /// Total tokens that touched the model, cache included.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.input_tokens
            .saturating_add(self.output_tokens)
            .saturating_add(self.cache_read_input_tokens)
            .saturating_add(self.cache_write_input_tokens)
    }

    /// True when nothing was billed — an interrupted turn, typically.
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.total() == 0
    }
}

impl Add for TokenUsage {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self {
            input_tokens: self.input_tokens.saturating_add(rhs.input_tokens),
            output_tokens: self.output_tokens.saturating_add(rhs.output_tokens),
            cache_read_input_tokens: self
                .cache_read_input_tokens
                .saturating_add(rhs.cache_read_input_tokens),
            cache_write_input_tokens: self
                .cache_write_input_tokens
                .saturating_add(rhs.cache_write_input_tokens),
        }
    }
}

impl AddAssign for TokenUsage {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

impl Sub for TokenUsage {
    type Output = Self;

    /// Saturating field-wise difference.
    ///
    /// Saturating rather than wrapping because a counter that appears to go
    /// backwards means the harness reset it (a new process, a fresh
    /// accumulator), and the honest reading of that is "no new tokens", never a
    /// gigantic unsigned wrap.
    fn sub(self, rhs: Self) -> Self {
        Self {
            input_tokens: self.input_tokens.saturating_sub(rhs.input_tokens),
            output_tokens: self.output_tokens.saturating_sub(rhs.output_tokens),
            cache_read_input_tokens: self
                .cache_read_input_tokens
                .saturating_sub(rhs.cache_read_input_tokens),
            cache_write_input_tokens: self
                .cache_write_input_tokens
                .saturating_sub(rhs.cache_write_input_tokens),
        }
    }
}

/// Turns a harness's running totals into per-turn deltas.
///
/// Feed it whatever cumulative figure the harness reported; it returns what
/// changed since last time. Because a harness restart resets its own counters,
/// a total that moved backwards is treated as a new accumulation epoch rather
/// than as negative usage.
///
/// ```
/// use horde_adapter::{TokenUsage, UsageAccumulator};
///
/// let tokens = |output| TokenUsage { output_tokens: output, ..TokenUsage::default() };
/// let mut accumulator = UsageAccumulator::new();
///
/// // The harness reports lifetime totals, so each reading includes the last.
/// assert_eq!(accumulator.observe(tokens(451), 0.0037).tokens.output_tokens, 451);
/// assert_eq!(accumulator.observe(tokens(654), 0.0063).tokens.output_tokens, 203);
///
/// // A replacement process starts its counters over; that is not negative usage.
/// assert_eq!(accumulator.observe(tokens(41), 0.0011).tokens.output_tokens, 41);
/// ```
#[derive(Debug, Clone, Default)]
pub struct UsageAccumulator {
    previous_tokens: TokenUsage,
    previous_cost_usd: f64,
}

impl UsageAccumulator {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a cumulative reading and returns the delta since the last one.
    pub fn observe(&mut self, cumulative: TokenUsage, cumulative_cost_usd: f64) -> UsageDelta {
        let restarted = Self::went_backwards(cumulative, self.previous_tokens)
            || cumulative_cost_usd + f64::EPSILON < self.previous_cost_usd;

        let (tokens, cost_usd) = if restarted {
            (cumulative, cumulative_cost_usd)
        } else {
            (
                cumulative - self.previous_tokens,
                (cumulative_cost_usd - self.previous_cost_usd).max(0.0),
            )
        };

        self.previous_tokens = cumulative;
        self.previous_cost_usd = cumulative_cost_usd;

        UsageDelta { tokens, cost_usd }
    }

    /// Forgets the running totals, so the next reading is taken whole.
    ///
    /// Call this when the underlying process is replaced — a `--resume` into a
    /// fresh child starts its counters at zero.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    fn went_backwards(next: TokenUsage, previous: TokenUsage) -> bool {
        next.input_tokens < previous.input_tokens
            || next.output_tokens < previous.output_tokens
            || next.cache_read_input_tokens < previous.cache_read_input_tokens
            || next.cache_write_input_tokens < previous.cache_write_input_tokens
    }
}

/// What one turn actually consumed.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct UsageDelta {
    pub tokens: TokenUsage,
    /// Provider-reported cost for this turn alone. Zero when the harness does
    /// not price its own turns.
    pub cost_usd: f64,
}

/// Where a [`RateWindow`]'s numbers came from.
///
/// Mirrors `horde.v1.HeadroomSource`: provider-reported state is truth where
/// the provider exposes it, and Horde's own metering stands in where it does
/// not. The two have different trust, so callers must be able to tell them
/// apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadroomSource {
    ProviderReported,
    HordeMetered,
}

/// Rate-limit state for one provider account, as the harness reported it.
///
/// Drives the `headroom` selection strategy in DESIGN.md §5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateWindow {
    pub source: HeadroomSource,
    /// Harness's own name for the window, e.g. `"five_hour"`. Opaque to Horde.
    pub window: String,
    /// Whether the provider is currently letting requests through.
    pub allowed: bool,
    /// Unix seconds at which the window resets; `None` when unreported.
    pub resets_at_unix: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::{TokenUsage, UsageAccumulator};

    fn usage(input: u64, output: u64) -> TokenUsage {
        TokenUsage {
            input_tokens: input,
            output_tokens: output,
            ..TokenUsage::default()
        }
    }

    #[test]
    fn first_observation_is_taken_whole() {
        let mut acc = UsageAccumulator::new();
        let delta = acc.observe(usage(10, 440), 0.0037);
        assert_eq!(delta.tokens, usage(10, 440));
        assert!((delta.cost_usd - 0.0037).abs() < f64::EPSILON);
    }

    /// The exact numbers spike W1c measured across three turns of one process.
    #[test]
    fn running_totals_become_per_turn_deltas() {
        let mut acc = UsageAccumulator::new();
        let first = acc.observe(usage(539, 451), 0.0037098);
        let second = acc.observe(usage(549, 654), 0.0063184);
        let third = acc.observe(usage(559, 713), 0.0077755);

        assert_eq!(first.tokens.output_tokens, 451);
        assert_eq!(second.tokens.output_tokens, 203);
        assert_eq!(third.tokens.output_tokens, 59);

        let summed: f64 = first.cost_usd + second.cost_usd + third.cost_usd;
        assert!(
            (summed - 0.0077755).abs() < 1e-9,
            "deltas must sum back to the final cumulative total, got {summed}"
        );
    }

    #[test]
    fn a_counter_that_moves_backwards_starts_a_new_epoch() {
        let mut acc = UsageAccumulator::new();
        acc.observe(usage(559, 713), 0.0078);

        // A replacement process reports its own, smaller, running total.
        let delta = acc.observe(usage(10, 41), 0.0011);

        assert_eq!(
            delta.tokens,
            usage(10, 41),
            "a restart must be read whole, never as a saturating subtraction"
        );
        assert!((delta.cost_usd - 0.0011).abs() < f64::EPSILON);
    }

    #[test]
    fn reset_makes_the_next_reading_whole_again() {
        let mut acc = UsageAccumulator::new();
        acc.observe(usage(100, 200), 0.5);
        acc.reset();
        let delta = acc.observe(usage(300, 400), 1.5);
        assert_eq!(delta.tokens, usage(300, 400));
        assert!((delta.cost_usd - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn an_unchanged_total_yields_a_zero_delta() {
        let mut acc = UsageAccumulator::new();
        acc.observe(usage(10, 20), 0.001);
        let delta = acc.observe(usage(10, 20), 0.001);
        assert!(delta.tokens.is_zero());
        assert_eq!(delta.cost_usd, 0.0);
    }

    #[test]
    fn total_counts_cache_traffic() {
        let usage = TokenUsage {
            input_tokens: 1,
            output_tokens: 2,
            cache_read_input_tokens: 4,
            cache_write_input_tokens: 8,
        };
        assert_eq!(usage.total(), 15);
    }
}
