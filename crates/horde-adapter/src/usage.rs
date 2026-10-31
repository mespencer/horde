//! Token accounting across the adapter seam.
//!
//! The seam's contract is **per-turn usage**, and the two harnesses Horde
//! drives arrive at it from opposite directions:
//!
//! * Claude Code reports `total_cost_usd` and `modelUsage` as running totals
//!   for the lifetime of one child process, so a Worker that summed the numbers
//!   it was handed would over-bill quadratically (spike W1c measured 0.0037 →
//!   0.0063 → 0.0078 across three turns of one process).
//! * Codex reports `thread/tokenUsage/updated` once per *model call*, with
//!   `last` covering that call alone and `total` running for the thread
//!   (transcript 33: six readings in one turn). Taking the first `last` as the
//!   turn undercounts; the thread `total` is the figure to difference.
//!
//! Normalizing *inside* each adapter means the Worker never has to know which
//! harness accumulates. Both adapters feed their running totals to
//! [`UsageAccumulator`], so that subtraction happens in one audited place.

use core::ops::{Add, AddAssign, Sub};
use core::time::Duration;

/// Token counts for one turn.
///
/// Mirrors `horde.v1.TokenUsage`, with one addition: reasoning output is
/// counted separately because Codex reports it as its own counter and folding
/// it into `output_tokens` would lose the distinction irreversibly. The wire
/// conversion happens in the Worker, at the transport boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub cache_write_input_tokens: u64,
    /// Tokens spent thinking, where the provider counts them separately.
    ///
    /// Zero means "none reported", which for a harness that does not break
    /// reasoning out is indistinguishable from "none spent" — the honest
    /// reading is that this counter is only meaningful for providers that
    /// populate it.
    pub reasoning_output_tokens: u64,
}

impl TokenUsage {
    /// Total tokens that touched the model, cache included.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.input_tokens
            .saturating_add(self.output_tokens)
            .saturating_add(self.cache_read_input_tokens)
            .saturating_add(self.cache_write_input_tokens)
            .saturating_add(self.reasoning_output_tokens)
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

/// How much of a rate-limit window has been consumed.
///
/// Providers disagree about the unit and neither form converts to the other:
/// Claude Code reports only whether requests are still allowed, and Codex
/// reports a percentage with no absolute counts (spike W2c). Collapsing both
/// into `used`/`limit` integers would mean inventing numbers, so the variants
/// stay distinct and callers handle what they were actually told.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Utilization {
    /// A fraction of the window consumed, in `0.0..=1.0`.
    Fraction(f64),
    /// Absolute counts in the window's own unit.
    Absolute { used: u64, limit: u64 },
    /// The provider says only whether it is still serving requests.
    AllowedOnly { allowed: bool },
}

impl Utilization {
    /// Remaining headroom as a fraction, where that can be known.
    ///
    /// `None` when the provider reported only a yes/no, or an absolute limit of
    /// zero — a limit of zero is unknown rather than exhausted, and guessing
    /// either way would mislead the scheduler.
    #[must_use]
    pub fn remaining_fraction(&self) -> Option<f64> {
        match *self {
            Self::Fraction(used) => Some((1.0 - used).clamp(0.0, 1.0)),
            Self::Absolute { limit: 0, .. } => None,
            Self::Absolute { used, limit } => {
                let ratio = used as f64 / limit as f64;
                Some((1.0 - ratio).clamp(0.0, 1.0))
            }
            Self::AllowedOnly { .. } => None,
        }
    }

    /// Whether the provider will currently serve a request.
    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        match *self {
            Self::AllowedOnly { allowed } => !allowed,
            _ => self.remaining_fraction().is_some_and(|left| left <= 0.0),
        }
    }
}

/// Rate-limit state for one provider account, as the harness reported it.
///
/// Drives the `headroom` selection strategy in DESIGN.md §5.
#[derive(Debug, Clone, PartialEq)]
pub struct RateWindow {
    pub source: HeadroomSource,
    /// Harness's own name for the window, e.g. `"five_hour"` or a Codex
    /// `limitId`. Opaque to Horde; two windows with different names are
    /// different windows.
    pub window: String,
    pub utilization: Utilization,
    /// How long the window spans, where the provider says.
    pub window_duration: Option<Duration>,
    /// Unix seconds at which the window resets; `None` when unreported.
    pub resets_at_unix: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::{TokenUsage, UsageAccumulator, Utilization};

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
    fn a_percentage_window_reports_the_remaining_fraction() {
        // Codex reports usedPercent with no absolute counts.
        let window = Utilization::Fraction(0.25);
        assert_eq!(window.remaining_fraction(), Some(0.75));
        assert!(!window.is_exhausted());
    }

    #[test]
    fn an_absolute_window_reports_the_remaining_fraction() {
        let window = Utilization::Absolute {
            used: 750,
            limit: 1000,
        };
        assert_eq!(window.remaining_fraction(), Some(0.25));
        assert!(!window.is_exhausted());
    }

    #[test]
    fn a_fully_consumed_window_is_exhausted() {
        assert!(Utilization::Fraction(1.0).is_exhausted());
        assert!(
            Utilization::Absolute {
                used: 10,
                limit: 10
            }
            .is_exhausted()
        );
    }

    /// A limit of zero means the provider did not say, which is not the same as
    /// a provider that has nothing left. Guessing either way misleads the
    /// scheduler's headroom strategy.
    #[test]
    fn an_absolute_limit_of_zero_is_unknown_rather_than_exhausted() {
        let window = Utilization::Absolute { used: 0, limit: 0 };
        assert_eq!(window.remaining_fraction(), None);
        assert!(!window.is_exhausted());
    }

    #[test]
    fn a_yes_no_window_yields_no_fraction_but_still_reports_exhaustion() {
        let serving = Utilization::AllowedOnly { allowed: true };
        assert_eq!(serving.remaining_fraction(), None);
        assert!(!serving.is_exhausted());

        let refusing = Utilization::AllowedOnly { allowed: false };
        assert_eq!(refusing.remaining_fraction(), None);
        assert!(refusing.is_exhausted());
    }

    #[test]
    fn an_over_consumed_window_clamps_instead_of_going_negative() {
        assert_eq!(Utilization::Fraction(1.5).remaining_fraction(), Some(0.0));
        assert_eq!(
            Utilization::Absolute {
                used: 30,
                limit: 10
            }
            .remaining_fraction(),
            Some(0.0)
        );
    }

    #[test]
    fn reasoning_tokens_survive_differencing_as_their_own_counter() {
        let with_reasoning = |reasoning| TokenUsage {
            output_tokens: 100,
            reasoning_output_tokens: reasoning,
            ..TokenUsage::default()
        };
        let mut acc = UsageAccumulator::new();
        acc.observe(with_reasoning(40), 0.0);
        let delta = acc.observe(with_reasoning(65), 0.0);
        assert_eq!(delta.tokens.reasoning_output_tokens, 25);
        assert_eq!(
            delta.tokens.output_tokens, 0,
            "reasoning must not be folded into output"
        );
    }

    #[test]
    fn total_counts_cache_traffic() {
        let usage = TokenUsage {
            input_tokens: 1,
            output_tokens: 2,
            cache_read_input_tokens: 4,
            cache_write_input_tokens: 8,
            reasoning_output_tokens: 16,
        };
        assert_eq!(usage.total(), 31);
    }
}

#[cfg(test)]
mod arithmetic_tests {
    use super::{HeadroomSource, RateWindow, TokenUsage, UsageAccumulator, Utilization};

    fn usage(input: u64, output: u64) -> TokenUsage {
        TokenUsage {
            input_tokens: input,
            output_tokens: output,
            ..TokenUsage::default()
        }
    }

    /// `is_zero` must distinguish an interrupted turn from a billed one.
    ///
    /// A mutant returning `true` survived, which would report every turn as
    /// free — and usage that always reads zero is worse than absent usage,
    /// because it looks like a measurement.
    #[test]
    fn only_an_unbilled_turn_reads_as_zero() {
        assert!(TokenUsage::default().is_zero());
        assert!(!usage(1, 0).is_zero(), "input alone is still billed");
        assert!(!usage(0, 1).is_zero(), "so is output alone");
        assert!(
            !TokenUsage {
                cache_read_input_tokens: 5,
                ..TokenUsage::default()
            }
            .is_zero(),
            "cache traffic is billed too"
        );
    }

    /// Addition must add every field, not return a default.
    #[test]
    fn adding_two_readings_sums_every_field() {
        let a = TokenUsage {
            input_tokens: 1,
            output_tokens: 2,
            cache_read_input_tokens: 3,
            cache_write_input_tokens: 4,
            reasoning_output_tokens: 5,
        };
        let sum = a + a;

        assert_eq!(sum.input_tokens, 2);
        assert_eq!(sum.output_tokens, 4);
        assert_eq!(sum.cache_read_input_tokens, 6);
        assert_eq!(sum.cache_write_input_tokens, 8);
        assert_eq!(sum.reasoning_output_tokens, 10);
        assert_ne!(sum, TokenUsage::default(), "a sum is not a default");
    }

    /// `+=` must accumulate, and in the right direction.
    ///
    /// Mutants replacing the body with `()` and the `+` with `-` both survived.
    /// A no-op `+=` silently under-bills a tree; a subtracting one saturates to
    /// zero and reports the same thing.
    #[test]
    fn add_assign_accumulates_forward() {
        let mut running = usage(10, 20);
        running += usage(5, 7);

        assert_eq!(running.input_tokens, 15);
        assert_eq!(running.output_tokens, 27);
    }

    /// The restart check must look at **every** field.
    ///
    /// Four `||`-to-`&&` mutants survived, meaning a restart visible in only
    /// one counter went unnoticed. That is the case that matters: a `--resume`
    /// into a fresh child restarts its counters, and a Worker that missed it
    /// would bill the difference between two unrelated processes.
    #[test]
    fn a_restart_is_detected_from_any_counter_going_backwards() {
        let fields: [(&str, TokenUsage, TokenUsage); 4] = [
            ("input", usage(10, 10), usage(1, 10)),
            ("output", usage(10, 10), usage(10, 1)),
            (
                "cache read",
                TokenUsage {
                    cache_read_input_tokens: 10,
                    ..TokenUsage::default()
                },
                TokenUsage {
                    cache_read_input_tokens: 1,
                    ..TokenUsage::default()
                },
            ),
            (
                "cache write",
                TokenUsage {
                    cache_write_input_tokens: 10,
                    ..TokenUsage::default()
                },
                TokenUsage {
                    cache_write_input_tokens: 1,
                    ..TokenUsage::default()
                },
            ),
        ];

        for (name, first, restarted) in fields {
            let mut accumulator = UsageAccumulator::new();
            accumulator.observe(first, 0.0);
            let delta = accumulator.observe(restarted, 0.0);

            assert_eq!(
                delta.tokens, restarted,
                "a restart visible in {name} alone must be taken whole, not \
                 differenced against a process that no longer exists"
            );
        }
    }

    /// A reading that did not go backwards is differenced, not taken whole.
    ///
    /// The other direction of the same guard: mutants flipping `<` to `>` would
    /// treat ordinary growth as a restart and re-bill the running total every
    /// turn.
    #[test]
    fn ordinary_growth_is_differenced() {
        let mut accumulator = UsageAccumulator::new();
        accumulator.observe(usage(10, 10), 0.001);

        let delta = accumulator.observe(usage(15, 30), 0.003);
        assert_eq!(delta.tokens, usage(5, 20), "only the new tokens are billed");
        assert!((delta.cost_usd - 0.002).abs() < 1e-9);
    }

    /// A cost that drops without any counter dropping is still a restart.
    ///
    /// The `||` in `observe` had mutants surviving in both directions: cost and
    /// tokens are independent restart signals, and a harness can reset one
    /// before the other becomes visible.
    #[test]
    fn a_cost_going_backwards_alone_signals_a_restart() {
        let mut accumulator = UsageAccumulator::new();
        accumulator.observe(usage(10, 10), 0.005);

        // Same counters, lower cost — the process was replaced.
        let delta = accumulator.observe(usage(10, 10), 0.001);
        assert!(
            (delta.cost_usd - 0.001).abs() < 1e-9,
            "the reading is taken whole, not differenced to a negative"
        );
    }

    /// Equal readings are not a restart — they are a turn that billed nothing.
    ///
    /// `<`-to-`<=` survived, which would have taken every repeated reading
    /// whole and re-billed the running total.
    #[test]
    fn an_unchanged_reading_bills_nothing() {
        let mut accumulator = UsageAccumulator::new();
        accumulator.observe(usage(10, 20), 0.001);

        let delta = accumulator.observe(usage(10, 20), 0.001);
        assert!(
            delta.tokens.is_zero(),
            "a repeated reading is no new work, not a fresh process"
        );
        assert_eq!(delta.cost_usd, 0.0);
    }

    /// A decrease of exactly one epsilon is noise, not a restart.
    ///
    /// `observe` compares `cost + EPSILON < previous`, and the `<`-to-`<=`
    /// mutant differs only at that exact boundary. Pinning it documents the
    /// deliberate choice: the epsilon exists to absorb float noise, so a drop
    /// no larger than the noise floor is not treated as a replaced process.
    #[test]
    fn a_cost_drop_within_the_noise_floor_is_not_a_restart() {
        let mut accumulator = UsageAccumulator::new();
        accumulator.observe(usage(10, 10), f64::EPSILON);

        // Same counters, cost lower by exactly one epsilon.
        let delta = accumulator.observe(usage(10, 10), 0.0);
        assert!(
            delta.tokens.is_zero(),
            "a drop at the noise floor bills nothing rather than re-billing \
             the whole reading as a fresh process"
        );
    }

    /// `remaining_fraction` on a window carrying nothing measurable.
    #[test]
    fn an_allowed_only_window_reports_no_fraction() {
        let window = RateWindow {
            source: HeadroomSource::ProviderReported,
            window: "weekly".to_owned(),
            utilization: Utilization::AllowedOnly { allowed: true },
            window_duration: None,
            resets_at_unix: None,
        };
        assert_eq!(window.utilization.remaining_fraction(), None);
        assert!(!window.utilization.is_exhausted());
    }
}
