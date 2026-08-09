//! Token counts and rate windows, both directions.
//!
//! The interesting case is [`rate_window_to_wire`]. Providers report headroom
//! in two incompatible shapes — Claude publishes counts, Codex publishes a
//! percentage (spike W2c) — and both the domain and the wire model both
//! shapes rather than normalizing, because either conversion loses something
//! the provider actually sent. These functions carry whichever shape arrived
//! and invent neither.

use horde_adapter::usage::{HeadroomSource, RateWindow, TokenUsage};
use horde_proto::prost_types::Duration as WireDuration;
use horde_proto::v1::{
    HeadroomSource as WireHeadroomSource, RateWindow as WireRateWindow, RateWindowUnit,
    TokenUsage as WireTokenUsage,
};

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

fn headroom_source_to_wire(source: HeadroomSource) -> WireHeadroomSource {
    match source {
        HeadroomSource::ProviderReported => WireHeadroomSource::ProviderReported,
        HeadroomSource::HordeMetered => WireHeadroomSource::HordeMetered,
    }
}

/// Domain → wire.
///
/// Two asymmetries are handled rather than hidden:
///
/// * `unit` is `TOKENS` only when counts are present. A window reporting a
///   percentage has no unit, and claiming one would imply counts that never
///   arrived.
/// * The domain's `window` name and `allowed` flag have no wire home. `allowed`
///   is recoverable — an exhausted window is `used_fraction >= 1.0` or
///   `used == limit` — but the harness's own window label is dropped, which is
///   why the domain keeps it for local diagnostics.
#[must_use]
pub fn rate_window_to_wire(window: &RateWindow) -> WireRateWindow {
    let unit = if window.used.is_some() || window.limit.is_some() {
        RateWindowUnit::Tokens
    } else {
        RateWindowUnit::Unspecified
    };

    WireRateWindow {
        source: headroom_source_to_wire(window.source).into(),
        unit: unit.into(),
        used: window.used,
        limit: window.limit,
        used_fraction: window.used_fraction,
        window_length: window.window_length.map(|d| WireDuration {
            seconds: i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
            nanos: i32::try_from(d.subsec_nanos()).unwrap_or(0),
        }),
        resets_at: window
            .resets_at_unix
            .map(|secs| horde_proto::prost_types::Timestamp {
                seconds: secs,
                nanos: 0,
            }),
    }
}

#[cfg(test)]
mod tests {
    use super::{rate_window_to_wire, token_usage_from_wire, token_usage_to_wire};
    use core::time::Duration;
    use horde_adapter::usage::{HeadroomSource, RateWindow, TokenUsage};
    use horde_proto::v1::RateWindowUnit;

    fn window() -> RateWindow {
        RateWindow {
            source: HeadroomSource::ProviderReported,
            window: "weekly".to_owned(),
            allowed: true,
            resets_at_unix: Some(1_786_915_482),
            used: None,
            limit: None,
            used_fraction: None,
            window_length: None,
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

    #[test]
    fn a_percentage_window_crosses_without_gaining_counts() {
        // Codex's shape. The wire must not invent a `used`/`limit` pair, and
        // must not claim a unit for counts it does not have.
        let codex = RateWindow {
            used_fraction: Some(0.42),
            window_length: Some(Duration::from_secs(10_080 * 60)),
            ..window()
        };
        let wire = rate_window_to_wire(&codex);

        assert_eq!(wire.used_fraction, Some(0.42));
        assert_eq!(wire.used, None);
        assert_eq!(wire.limit, None);
        assert_eq!(wire.unit, i32::from(RateWindowUnit::Unspecified));
        assert_eq!(wire.window_length.map(|d| d.seconds), Some(604_800));
    }

    #[test]
    fn a_counts_window_crosses_with_its_unit() {
        let counts = RateWindow {
            used: Some(2_500),
            limit: Some(10_000),
            ..window()
        };
        let wire = rate_window_to_wire(&counts);

        assert_eq!(wire.used, Some(2_500));
        assert_eq!(wire.limit, Some(10_000));
        assert_eq!(wire.unit, i32::from(RateWindowUnit::Tokens));
        assert_eq!(wire.used_fraction, None);
    }

    #[test]
    fn an_unreported_window_stays_unreported_across_the_wire() {
        // Claude's rate-limit event is qualitative — no counts, no percentage.
        // If either side turned that into a zero, the scheduler would read an
        // unmeasured account as an empty one and send work to it.
        let wire = rate_window_to_wire(&window());

        assert_eq!(wire.used, None);
        assert_eq!(wire.limit, None);
        assert_eq!(wire.used_fraction, None);
        assert_eq!(wire.resets_at.map(|t| t.seconds), Some(1_786_915_482));
    }
}
