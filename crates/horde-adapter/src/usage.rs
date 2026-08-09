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
use core::time::Duration;

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
    /// Reasoning tokens, where the provider bills and reports them separately.
    ///
    /// A **breakdown, never an addend**. Providers disagree about whether
    /// reasoning already sits inside `output_tokens` — Codex reports both and
    /// folds reasoning into output (spike W2c) — so [`TokenUsage::total`]
    /// deliberately excludes this field. Adding it would double-count every
    /// reasoning token on such a provider.
    pub reasoning_output_tokens: u64,
}

impl TokenUsage {
    /// Total tokens that touched the model, cache included.
    ///
    /// Excludes `reasoning_output_tokens` on purpose: it is a breakdown of
    /// output, not a fifth bucket. See that field's documentation.
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
            reasoning_output_tokens: self
                .reasoning_output_tokens
                .saturating_add(rhs.reasoning_output_tokens),
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
            reasoning_output_tokens: self
                .reasoning_output_tokens
                .saturating_sub(rhs.reasoning_output_tokens),
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
            || next.reasoning_output_tokens < previous.reasoning_output_tokens
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
///
/// Not `Eq`: `used_fraction` is a float, and float equality is not an
/// equivalence relation.
#[derive(Debug, Clone, PartialEq)]
pub struct RateWindow {
    pub source: HeadroomSource,
    /// Harness's own name for the window, e.g. `"five_hour"`. Opaque to Horde.
    pub window: String,
    /// Whether the provider is currently letting requests through.
    pub allowed: bool,
    /// Unix seconds at which the window resets; `None` when unreported.
    pub resets_at_unix: Option<i64>,
    /// Consumption so far, when the provider reports counts. Claude does.
    ///
    /// `None` is "not reported", which is emphatically not zero — reading an
    /// unreported window as an empty one tells the scheduler it has full
    /// headroom on an account that may be exhausted.
    pub used: Option<u64>,
    /// Ceiling for the window, when the provider publishes one. `None` means
    /// no published ceiling, distinct from a ceiling of zero.
    pub limit: Option<u64>,
    /// Fraction of the window consumed, `0.0..=1.0`, for providers that report
    /// a percentage instead of counts. Codex does (spike W2c).
    ///
    /// Both shapes exist because normalising either way is lossy: a fraction
    /// cannot be turned into counts without a ceiling the provider never sent,
    /// and counts cannot be turned into a fraction without discarding the
    /// ceiling it did send.
    pub used_fraction: Option<f64>,
    /// Length of the window, when reported. Codex sends 10080 minutes.
    pub window_length: Option<Duration>,
}

impl RateWindow {
    /// How much of the window remains, `0.0..=1.0`, from whichever shape the
    /// provider reported — the number the `headroom` selection strategy sorts
    /// on (DESIGN.md §5).
    ///
    /// `None` when the provider reported neither shape, so callers must decide
    /// what an unmeasured account is worth rather than being handed a
    /// misleading `1.0`.
    #[must_use]
    pub fn headroom_fraction(&self) -> Option<f64> {
        if let Some(fraction) = self.used_fraction {
            return Some((1.0 - fraction).clamp(0.0, 1.0));
        }
        match (self.used, self.limit) {
            // A zero ceiling has no room by definition; dividing would be NaN.
            (Some(_), Some(0)) => Some(0.0),
            (Some(used), Some(limit)) => {
                let remaining = limit.saturating_sub(used);
                Some((remaining as f64 / limit as f64).clamp(0.0, 1.0))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{HeadroomSource, RateWindow, TokenUsage, UsageAccumulator};

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
            reasoning_output_tokens: 0,
        };
        assert_eq!(usage.total(), 15);
    }

    #[test]
    fn total_never_double_counts_reasoning_folded_into_output() {
        // Codex reports 64 reasoning tokens that are already inside its 200
        // output tokens (spike W2c). A total that added the breakdown would
        // bill 264 output tokens for 200 tokens of work.
        let folded = TokenUsage {
            input_tokens: 100,
            output_tokens: 200,
            cache_read_input_tokens: 0,
            cache_write_input_tokens: 0,
            reasoning_output_tokens: 64,
        };
        let without_breakdown = TokenUsage {
            reasoning_output_tokens: 0,
            ..folded
        };

        assert_eq!(folded.total(), 300);
        assert_eq!(
            folded.total(),
            without_breakdown.total(),
            "reporting the reasoning breakdown must not change what is billed"
        );
    }

    #[test]
    fn reasoning_breakdown_still_deltas_across_turns() {
        // It is excluded from total(), not ignored: a Worker attributing
        // reasoning per turn needs the difference, not the running total.
        let mut acc = UsageAccumulator::new();
        let cumulative = |reasoning| TokenUsage {
            output_tokens: reasoning,
            reasoning_output_tokens: reasoning,
            ..TokenUsage::default()
        };

        assert_eq!(
            acc.observe(cumulative(64), 0.0)
                .tokens
                .reasoning_output_tokens,
            64
        );
        assert_eq!(
            acc.observe(cumulative(100), 0.0)
                .tokens
                .reasoning_output_tokens,
            36
        );
    }

    #[test]
    fn headroom_comes_from_whichever_shape_the_provider_reported() {
        let base = RateWindow {
            source: HeadroomSource::ProviderReported,
            window: "weekly".to_owned(),
            allowed: true,
            resets_at_unix: None,
            used: None,
            limit: None,
            used_fraction: None,
            window_length: None,
        };

        // Codex-shaped: a fraction and nothing else.
        let codex = RateWindow {
            used_fraction: Some(0.25),
            ..base.clone()
        };
        assert_eq!(codex.headroom_fraction(), Some(0.75));

        // Counts-shaped.
        let counts = RateWindow {
            used: Some(2_500),
            limit: Some(10_000),
            ..base.clone()
        };
        assert_eq!(counts.headroom_fraction(), Some(0.75));

        // A zero ceiling has no room, and must not divide by zero.
        let zero_ceiling = RateWindow {
            used: Some(0),
            limit: Some(0),
            ..base.clone()
        };
        assert_eq!(zero_ceiling.headroom_fraction(), Some(0.0));

        // Unreported is None, not full headroom — the whole point of Option.
        assert_eq!(base.headroom_fraction(), None);
    }
}
