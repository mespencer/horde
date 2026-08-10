//! Token counts and rate windows, both directions.
//!
//! Providers report headroom in three incompatible shapes and the domain models
//! all three as [`Utilization`]: Codex publishes a percentage, other harnesses
//! publish counts, and Claude Code publishes nothing but "still serving"
//! (spikes W1c/W2c). The wire mirrors all three rather than normalizing,
//! because every normalization loses something the provider actually sent — a
//! fraction cannot become counts without a ceiling nobody sent, counts cannot
//! become a fraction without discarding the ceiling that was sent, and a bare
//! yes/no cannot become either without inventing a quantity.
//!
//! The mapping is therefore **total and lossless in both directions**, and the
//! one asymmetry that remains — the harness's own window label, which the wire
//! has no field for — is named at the conversion.

use horde_adapter::usage::{HeadroomSource, RateWindow, TokenUsage, Utilization};
use horde_proto::prost_types::{Duration as WireDuration, Timestamp};
use horde_proto::v1::{
    HeadroomSource as WireHeadroomSource, RateWindow as WireRateWindow, RateWindowUnit,
    TokenUsage as WireTokenUsage,
};

/// A wire rate window whose shape cannot be told apart.
///
/// Every [`Utilization`] arm sets exactly one shape, so a window carrying none
/// of them came from a peer that sent an empty message or a shape this build
/// does not know. Guessing would hand the scheduler a headroom nobody reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnreadableUtilization;

impl core::fmt::Display for UnreadableUtilization {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("RateWindow carries no recognizable utilization shape")
    }
}

impl core::error::Error for UnreadableUtilization {}

/// Domain → wire.
#[must_use]
pub fn token_usage_to_wire(usage: TokenUsage) -> WireTokenUsage {
    WireTokenUsage {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_input_tokens: usage.cache_read_input_tokens,
        cache_write_input_tokens: usage.cache_write_input_tokens,
        reasoning_output_tokens: usage.reasoning_output_tokens,
    }
}

/// Wire → domain.
#[must_use]
pub fn token_usage_from_wire(usage: &WireTokenUsage) -> TokenUsage {
    TokenUsage {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_input_tokens: usage.cache_read_input_tokens,
        cache_write_input_tokens: usage.cache_write_input_tokens,
        reasoning_output_tokens: usage.reasoning_output_tokens,
    }
}

fn source_to_wire(source: HeadroomSource) -> WireHeadroomSource {
    match source {
        HeadroomSource::ProviderReported => WireHeadroomSource::ProviderReported,
        HeadroomSource::HordeMetered => WireHeadroomSource::HordeMetered,
    }
}

fn source_from_wire(value: i32) -> HeadroomSource {
    // Horde's own metering is the safe reading of an unknown source: it is the
    // weaker claim, so treating a future PROVIDER_REPORTED variant as metered
    // understates trust rather than overstating it.
    match WireHeadroomSource::try_from(value) {
        Ok(WireHeadroomSource::ProviderReported) => HeadroomSource::ProviderReported,
        Ok(WireHeadroomSource::HordeMetered | WireHeadroomSource::Unspecified) | Err(_) => {
            HeadroomSource::HordeMetered
        }
    }
}

/// Domain → wire.
///
/// Exhaustive over [`Utilization`]: a new arm must decide how it crosses before
/// this compiles. Each arm sets exactly one shape, which is what lets
/// [`rate_window_from_wire`] tell them apart again.
///
/// The one thing that does not survive: `RateWindow::window`, the harness's own
/// label for the window (`"five_hour"`, `"weekly"`). The wire has no field for
/// it because nothing downstream matches on it — it is local diagnostic colour,
/// and a round trip through the wire returns an empty label.
#[must_use]
pub fn rate_window_to_wire(window: &RateWindow) -> WireRateWindow {
    let mut wire = WireRateWindow {
        source: source_to_wire(window.source).into(),
        unit: RateWindowUnit::Unspecified.into(),
        used: None,
        limit: None,
        used_fraction: None,
        allowed: None,
        window_length: window.window_duration.map(|d| WireDuration {
            seconds: i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
            nanos: i32::try_from(d.subsec_nanos()).unwrap_or(0),
        }),
        resets_at: window
            .resets_at_unix
            .map(|seconds| Timestamp { seconds, nanos: 0 }),
    };

    match window.utilization {
        Utilization::Fraction(used) => wire.used_fraction = Some(used),
        Utilization::Absolute { used, limit } => {
            wire.used = Some(used);
            wire.limit = Some(limit);
            // A unit only means something alongside counts.
            wire.unit = RateWindowUnit::Tokens.into();
        }
        Utilization::AllowedOnly { allowed } => wire.allowed = Some(allowed),
    }

    wire
}

/// Wire → domain.
///
/// Shapes are checked in the order of how much they say, so a peer that sent
/// more than one is read at its most informative rather than its least.
pub fn rate_window_from_wire(wire: &WireRateWindow) -> Result<RateWindow, UnreadableUtilization> {
    let utilization = match (wire.used_fraction, wire.used, wire.limit, wire.allowed) {
        (Some(fraction), ..) => Utilization::Fraction(fraction),
        (None, Some(used), Some(limit), _) => Utilization::Absolute { used, limit },
        (None, None, None, Some(allowed)) => Utilization::AllowedOnly { allowed },
        _ => return Err(UnreadableUtilization),
    };

    Ok(RateWindow {
        source: source_from_wire(wire.source),
        // No wire field; see `rate_window_to_wire`.
        window: String::new(),
        utilization,
        window_duration: wire
            .window_length
            .map(|d| core::time::Duration::new(d.seconds.max(0) as u64, d.nanos.max(0) as u32)),
        resets_at_unix: wire.resets_at.map(|t| t.seconds),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        UnreadableUtilization, rate_window_from_wire, rate_window_to_wire, token_usage_from_wire,
        token_usage_to_wire,
    };
    use core::time::Duration;
    use horde_adapter::usage::{HeadroomSource, RateWindow, TokenUsage, Utilization};
    use horde_proto::v1::{RateWindow as WireRateWindow, RateWindowUnit};

    fn window(utilization: Utilization) -> RateWindow {
        RateWindow {
            source: HeadroomSource::ProviderReported,
            window: "weekly".to_owned(),
            utilization,
            window_duration: Some(Duration::from_secs(10_080 * 60)),
            resets_at_unix: Some(1_786_915_482),
        }
    }

    #[test]
    fn token_usage_round_trips_including_the_reasoning_breakdown() {
        let usage = TokenUsage {
            input_tokens: 17_098,
            output_tokens: 200,
            cache_read_input_tokens: 11_008,
            cache_write_input_tokens: 3,
            reasoning_output_tokens: 64,
        };
        assert_eq!(token_usage_from_wire(&token_usage_to_wire(usage)), usage);
    }

    /// Every utilization shape survives the wire unchanged.
    ///
    /// The label is the known casualty, so it is compared separately rather
    /// than quietly excluded from the assertion.
    #[test]
    fn every_utilization_shape_round_trips() {
        for utilization in [
            Utilization::Fraction(0.42),
            Utilization::Absolute {
                used: 2_500,
                limit: 10_000,
            },
            Utilization::AllowedOnly { allowed: true },
            Utilization::AllowedOnly { allowed: false },
        ] {
            let original = window(utilization);
            let back = rate_window_from_wire(&rate_window_to_wire(&original))
                .expect("a window this crate produced is readable");

            assert_eq!(back.utilization, original.utilization, "{utilization:?}");
            assert_eq!(back.source, original.source);
            assert_eq!(back.window_duration, original.window_duration);
            assert_eq!(back.resets_at_unix, original.resets_at_unix);
            assert_eq!(
                back.window, "",
                "the harness's window label has no wire home"
            );
        }
    }

    #[test]
    fn a_percentage_window_gains_neither_counts_nor_a_unit() {
        // Codex's shape. Claiming TOKENS here would imply counts that never
        // arrived.
        let wire = rate_window_to_wire(&window(Utilization::Fraction(0.42)));

        assert_eq!(wire.used_fraction, Some(0.42));
        assert_eq!(wire.used, None);
        assert_eq!(wire.limit, None);
        assert_eq!(wire.allowed, None);
        assert_eq!(wire.unit, i32::from(RateWindowUnit::Unspecified));
    }

    #[test]
    fn an_allowed_only_window_is_not_forced_into_a_quantity() {
        // Claude's shape. Encoding "still serving" as a fraction would invent a
        // number the provider never reported.
        let wire = rate_window_to_wire(&window(Utilization::AllowedOnly { allowed: true }));

        assert_eq!(wire.allowed, Some(true));
        assert_eq!(wire.used_fraction, None);
        assert_eq!(wire.used, None);
        assert_eq!(wire.limit, None);
    }

    /// The message an unreadable window produces.
    ///
    /// This error means the scheduler was handed headroom it cannot interpret,
    /// which is a decision-affecting condition rather than noise — so it has to
    /// say what it was reading, not merely that something went wrong.
    #[test]
    fn an_unreadable_window_explains_what_it_could_not_read() {
        let rendered = UnreadableUtilization.to_string();
        assert!(rendered.contains("RateWindow"), "got {rendered}");
        assert!(
            rendered.contains("utilization"),
            "and must name the missing shape, got {rendered}"
        );
    }

    #[test]
    fn a_shapeless_window_is_refused_rather_than_guessed() {
        // An empty message must not read as full headroom.
        assert_eq!(
            rate_window_from_wire(&WireRateWindow::default()),
            Err(UnreadableUtilization)
        );
    }

    #[test]
    fn remaining_fraction_survives_the_crossing() {
        // The number the scheduler actually sorts on (DESIGN.md §5) must mean
        // the same thing on both sides of the wire.
        for utilization in [
            Utilization::Fraction(0.25),
            Utilization::Absolute {
                used: 2_500,
                limit: 10_000,
            },
        ] {
            let back = rate_window_from_wire(&rate_window_to_wire(&window(utilization)))
                .expect("readable");
            assert_eq!(
                back.utilization.remaining_fraction(),
                utilization.remaining_fraction()
            );
        }
    }
}
