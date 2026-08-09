//! Headroom and token accounting have to survive contact with providers that
//! report different things. Codex publishes a percentage and no counts; Claude
//! publishes counts. `RateWindow` models both rather than normalising, so these
//! tests pin the distinction that makes that worth the extra fields: absent is
//! not zero.

use horde_proto::prost::Message;
use horde_proto::v1::{
    AdapterCapability, HeadroomSource, ProviderAccountCapability, RateWindow, RateWindowUnit,
    TokenUsage, UsageRecord,
};

#[track_caller]
fn round_trip<M: Message + Default + PartialEq + std::fmt::Debug>(message: &M) {
    let decoded = M::decode(message.encode_to_vec().as_slice()).ok();
    assert_eq!(decoded.as_ref(), Some(message));
}

/// Claude-shaped: absolute counts, no percentage.
fn counts_window() -> RateWindow {
    RateWindow {
        source: HeadroomSource::ProviderReported.into(),
        unit: RateWindowUnit::Tokens.into(),
        used: Some(1_000),
        limit: Some(10_000),
        window_length: None,
        resets_at: None,
        used_fraction: None,
    }
}

/// Codex-shaped: a percentage and nothing else. `unit` stays UNSPECIFIED
/// because there are no counts for it to describe.
fn fraction_window() -> RateWindow {
    RateWindow {
        source: HeadroomSource::ProviderReported.into(),
        unit: RateWindowUnit::Unspecified.into(),
        used: None,
        limit: None,
        window_length: None,
        resets_at: None,
        used_fraction: Some(0.42),
    }
}

#[test]
fn rate_window_round_trips_both_reporting_shapes() {
    round_trip(&counts_window());
    round_trip(&fraction_window());
}

#[test]
fn absent_counts_are_distinct_from_zero_counts() {
    // The whole reason `used`/`limit` are optional. A provider that reports
    // nothing must not be indistinguishable from one reporting a fresh window,
    // or the scheduler will read "no data" as "full headroom".
    let exhausted = RateWindow {
        used: Some(0),
        limit: Some(0),
        ..fraction_window()
    };
    let unreported = fraction_window();

    assert_ne!(exhausted, unreported);
    assert_eq!(unreported.used, None);
    assert_eq!(exhausted.used, Some(0));

    // And the distinction has to survive the wire, not just the Rust type.
    let decoded = RateWindow::decode(unreported.encode_to_vec().as_slice()).ok();
    assert_eq!(decoded.and_then(|w| w.used), None);
}

#[test]
fn a_window_may_carry_only_a_fraction_or_only_counts() {
    let counts = counts_window();
    assert!(counts.used.is_some() && counts.used_fraction.is_none());

    let fraction = fraction_window();
    assert!(fraction.used_fraction.is_some() && fraction.limit.is_none());
}

#[test]
fn reasoning_tokens_are_carried_separately() {
    // Providers differ on whether reasoning is folded into output_tokens, so
    // the field is a breakdown for attribution, never something to add on.
    let usage = TokenUsage {
        input_tokens: 17_098,
        output_tokens: 7,
        cache_read_input_tokens: 11_008,
        cache_write_input_tokens: 0,
        reasoning_output_tokens: 64,
    };
    round_trip(&usage);

    round_trip(&UsageRecord {
        binding: None,
        tokens: Some(usage),
        rate_window: Some(fraction_window()),
        turn_id: "turn-3".to_owned(),
    });
}

#[test]
fn tool_call_gating_is_advertised_per_adapter() {
    // Absence is the load-bearing state: a provider that adjudicates tool calls
    // internally simply does not list it, and placement reads that as
    // "ungateable" rather than as a missing advertisement.
    let gateable: [i32; 2] = [
        AdapterCapability::Resumable.into(),
        AdapterCapability::ToolCallGating.into(),
    ];
    let ungateable: [i32; 2] = [
        AdapterCapability::Resumable.into(),
        AdapterCapability::Interruptible.into(),
    ];

    assert!(gateable.contains(&i32::from(AdapterCapability::ToolCallGating)));
    assert!(!ungateable.contains(&i32::from(AdapterCapability::ToolCallGating)));

    round_trip(&ProviderAccountCapability {
        provider: "codex".to_owned(),
        account: "default".to_owned(),
        models: vec!["gpt-5.6-sol".to_owned()],
        headroom: Some(fraction_window()),
    });
}

#[test]
fn tool_call_gating_did_not_disturb_existing_capability_numbers() {
    // Renumbering a shipped capability would silently re-label every worker's
    // advertisement, so pin the assignment.
    assert_eq!(i32::from(AdapterCapability::Unspecified), 0);
    assert_eq!(i32::from(AdapterCapability::Resumable), 1);
    assert_eq!(i32::from(AdapterCapability::Interruptible), 2);
    assert_eq!(i32::from(AdapterCapability::Steerable), 3);
    assert_eq!(i32::from(AdapterCapability::PermissionPromptTool), 4);
    assert_eq!(i32::from(AdapterCapability::ToolCallGating), 5);
}
