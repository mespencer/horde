//! Adapter capabilities, both directions.
//!
//! The two enums are not the same size and never will be: the wire carries
//! only what placement and permission modes match on, while the domain enum
//! carries everything a Worker must refuse to do. Both matches are exhaustive
//! anyway, so the asymmetry is a decision recorded here rather than a gap
//! discovered later.

use horde_adapter::Capability;
use horde_proto::v1::AdapterCapability;

/// A wire capability this build does not understand.
///
/// Protobuf enums are open, so a newer Worker can advertise a capability an
/// older Server has never heard of. That is not corruption — it is the version
/// skew the design expects (DESIGN.md §2) — so it is an ordinary error to
/// handle, and never a panic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownCapability(pub i32);

impl core::fmt::Display for UnknownCapability {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "unrecognized AdapterCapability value {}", self.0)
    }
}

impl core::error::Error for UnknownCapability {}

/// Domain → wire.
///
/// Exhaustive: a new [`Capability`] must decide whether it is something
/// placement can match on before this compiles.
#[must_use]
pub fn capability_to_wire(capability: Capability) -> Option<AdapterCapability> {
    match capability {
        Capability::Resumable => Some(AdapterCapability::Resumable),
        Capability::InterruptibleTurn => Some(AdapterCapability::Interruptible),
        Capability::AmendActiveTurn => Some(AdapterCapability::Steerable),

        // The domain models the capability; the wire's PERMISSION_PROMPT_TOOL
        // names one provider's mechanism and is deprecated in favour of this.
        Capability::PreAuthorizeToolCalls => Some(AdapterCapability::ToolCallGating),

        // On the wire because the Server needs it before it composes a
        // narrowing decision, not after the Worker bounces one.
        Capability::RewritesToolInput => Some(AdapterCapability::ToolInputRewrite),

        // Worker-local facts with no placement consequence on the wire yet.
        // Deliberately not advertised: a capability nothing matches on invites
        // readers to assume it is enforced somewhere.
        //
        // `SandboxTierAtSpawn` is the one most likely to graduate — it is what
        // an ungateable provider offers *instead* of interception, so a
        // scheduler that learns to place high-trust work will want to see it.
        // It stays off the wire until something reads it.
        //
        // `PreAuthorizeMcpToolCalls` and `PreAuthorizeExecCommands` (ADR-0005
        // amendments: Codex intercepts MCP calls, and shell/exec at 0.147+,
        // but never file changes) stay off the wire for the same reason and
        // one more: mapping either to TOOL_CALL_GATING would overstate it —
        // placement reads that flag as "high-trust work can land here", which
        // is exactly what ungated file changes forbid. They graduate as their
        // own wire values when placement learns to match per call kind (W5d's
        // seam), not before.
        Capability::PreAuthorizeMcpToolCalls
        | Capability::PreAuthorizeExecCommands
        | Capability::PreAuthorizeFileChanges
        | Capability::SandboxTierAtSpawn
        | Capability::ReportsHeadroom
        | Capability::SystemPromptOverlay
        | Capability::ModelSelection
        | Capability::AccountIsolation => None,
    }
}

/// Wire → domain.
///
/// Exhaustive over the wire enum. `UNSPECIFIED` maps to `Ok(None)` rather than
/// an error: an unset field is a peer that said nothing, which is normal, while
/// an unrecognized number is a peer from the future, which the caller may want
/// to log.
// Reading the deprecated variant is the entire point of this direction:
// Workers built before TOOL_CALL_GATING still advertise it, and refusing to
// decode it would silently downgrade them to ungateable.
#[allow(deprecated)]
pub fn capability_from_wire(value: i32) -> Result<Option<Capability>, UnknownCapability> {
    let Ok(capability) = AdapterCapability::try_from(value) else {
        return Err(UnknownCapability(value));
    };

    Ok(match capability {
        AdapterCapability::Unspecified => None,
        AdapterCapability::Resumable => Some(Capability::Resumable),
        AdapterCapability::Interruptible => Some(Capability::InterruptibleTurn),
        AdapterCapability::Steerable => Some(Capability::AmendActiveTurn),
        AdapterCapability::ToolCallGating => Some(Capability::PreAuthorizeToolCalls),
        AdapterCapability::ToolInputRewrite => Some(Capability::RewritesToolInput),

        // Deprecated, but old Workers still send it, and it means the same
        // thing they would now send as TOOL_CALL_GATING.
        AdapterCapability::PermissionPromptTool => Some(Capability::PreAuthorizeToolCalls),
    })
}

/// Whether a Worker advertising `advertised` can apply replacement tool
/// arguments.
///
/// The Server asks this *before* composing a narrowing decision. Sending one to
/// a Worker without the capability produces a decision the Worker must reject
/// rather than silently widen, which strands the Agent on an answer that was
/// never deliverable.
///
/// Unknown values are ignored rather than refused: a peer from the future
/// advertising capabilities this build cannot name says nothing about whether
/// it rewrites.
#[must_use]
pub fn advertises_tool_input_rewrite(advertised: &[i32]) -> bool {
    advertised
        .iter()
        .filter_map(|value| capability_from_wire(*value).ok().flatten())
        .any(|capability| capability == Capability::RewritesToolInput)
}

#[cfg(test)]
mod tests {
    use super::{
        UnknownCapability, advertises_tool_input_rewrite, capability_from_wire, capability_to_wire,
    };
    use horde_adapter::Capability;
    use horde_proto::v1::AdapterCapability;

    #[test]
    fn every_domain_capability_has_a_decision() {
        // Not "maps to something" — a decision. `None` is a recorded choice
        // that the wire does not carry it, and Capability::ALL is what makes
        // this a total check rather than a spot check.
        for capability in Capability::ALL {
            let _ = capability_to_wire(capability);
        }
        assert_eq!(Capability::ALL.len(), 13);
    }

    /// The scoped per-kind gating claims must never widen into the wire's
    /// blanket gating flag: placement reads TOOL_CALL_GATING as "high-trust
    /// work can land here", which ungated file changes forbid.
    #[test]
    fn scoped_per_kind_gating_does_not_advertise_as_blanket_gating() {
        assert_eq!(
            capability_to_wire(Capability::PreAuthorizeMcpToolCalls),
            None
        );
        assert_eq!(
            capability_to_wire(Capability::PreAuthorizeExecCommands),
            None
        );
        assert_eq!(
            capability_to_wire(Capability::PreAuthorizeFileChanges),
            None
        );
    }

    #[test]
    fn wire_capabilities_round_trip_through_the_domain() {
        for wire in [
            AdapterCapability::Resumable,
            AdapterCapability::Interruptible,
            AdapterCapability::Steerable,
            AdapterCapability::ToolCallGating,
        ] {
            let domain = capability_from_wire(wire.into())
                .ok()
                .flatten()
                .expect("a mapped wire capability resolves to a domain one");
            assert_eq!(
                capability_to_wire(domain),
                Some(wire),
                "{wire:?} did not survive the round trip"
            );
        }
    }

    #[test]
    #[allow(deprecated)] // asserting the deprecated variant is still honoured
    fn the_deprecated_mechanism_flag_still_reads_as_gating() {
        // An older Worker advertising PERMISSION_PROMPT_TOOL means exactly what
        // a newer one means by TOOL_CALL_GATING; dropping it would silently
        // downgrade that Worker to ungateable and refuse high-trust work on it.
        assert_eq!(
            capability_from_wire(AdapterCapability::PermissionPromptTool.into()),
            Ok(Some(Capability::PreAuthorizeToolCalls))
        );
    }

    /// The message a peer-from-the-future produces.
    ///
    /// Worth pinning because this is the only place the offending number
    /// survives: an operator diagnosing a version-skew refusal has the log line
    /// and nothing else, so a Display that dropped the value would leave them
    /// knowing something was rejected but not what.
    #[test]
    fn an_unknown_capability_names_the_number_it_could_not_read() {
        let rendered = UnknownCapability(9_999).to_string();
        assert!(rendered.contains("9999"), "got {rendered}");
        assert!(
            rendered.contains("AdapterCapability"),
            "and must say which enum it was reading, got {rendered}"
        );
    }

    #[test]
    fn unset_is_not_an_error_but_an_unknown_number_is() {
        assert_eq!(capability_from_wire(0), Ok(None));
        assert_eq!(capability_from_wire(9_999), Err(UnknownCapability(9_999)));
    }

    #[test]
    fn a_rewrite_is_only_offered_to_a_worker_that_advertised_it() {
        // The Server asks this before composing a narrowing decision. Without
        // the advertisement it would only learn after the Worker rejected one.
        let claude: Vec<i32> = [
            Capability::PreAuthorizeToolCalls,
            Capability::RewritesToolInput,
        ]
        .into_iter()
        .filter_map(capability_to_wire)
        .map(i32::from)
        .collect();

        let codex: Vec<i32> = [Capability::Resumable, Capability::SandboxTierAtSpawn]
            .into_iter()
            .filter_map(capability_to_wire)
            .map(i32::from)
            .collect();

        assert!(advertises_tool_input_rewrite(&claude));
        assert!(!advertises_tool_input_rewrite(&codex));

        // A peer from the future says nothing about rewriting by being unknown.
        assert!(!advertises_tool_input_rewrite(&[9_999]));
    }

    #[test]
    fn gating_is_absent_rather_than_negated_for_ungateable_providers() {
        // Codex cannot pre-authorize tool calls (ADR-0005). It expresses that
        // by not advertising the capability — there is no negative flag, and a
        // reader must treat absence as "cannot", not "unknown".
        let codex: Vec<AdapterCapability> = [Capability::Resumable, Capability::AmendActiveTurn]
            .into_iter()
            .filter_map(capability_to_wire)
            .collect();

        assert!(!codex.contains(&AdapterCapability::ToolCallGating));
        assert!(codex.contains(&AdapterCapability::Steerable));
    }
}
