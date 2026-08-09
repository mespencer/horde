//! A permission decision may *narrow* a tool call rather than merely permit
//! it, and the narrowing has to survive the trip from Server to Worker.
//!
//! Spike W1c established that Claude Code executes whatever arguments the
//! permission tool returns, so a rewrite that silently fails to cross the wire
//! means the original, unnarrowed call runs. These tests pin the two states
//! that make that detectable: absent (allow as proposed) and present-but-empty
//! (allow with no arguments), which are not the same instruction.

use horde_proto::prost::Message;
use horde_proto::prost_types::{Struct, Value, value::Kind};
use horde_proto::v1::{
    PermissionDecision, PermissionLayer, PermissionOutcome, PermissionRequestId,
};

#[track_caller]
fn round_trip<M: Message + Default + PartialEq + std::fmt::Debug>(message: &M) {
    let decoded = M::decode(message.encode_to_vec().as_slice()).ok();
    assert_eq!(decoded.as_ref(), Some(message));
}

fn string_value(s: &str) -> Value {
    Value {
        kind: Some(Kind::StringValue(s.to_owned())),
    }
}

/// A decision that clamps the path a command was going to touch.
fn narrowed() -> PermissionDecision {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert("path".to_owned(), string_value("/workspace/atlas"));
    fields.insert(
        "recursive".to_owned(),
        Value {
            kind: Some(Kind::BoolValue(false)),
        },
    );

    PermissionDecision {
        request_id: Some(PermissionRequestId::new("perm-1")),
        outcome: PermissionOutcome::Allow.into(),
        decided_by: PermissionLayer::Human.into(),
        decided_by_actor: None,
        reason: "clamped to the workspace root".to_owned(),
        remember_for_session: false,
        updated_input: Some(Struct { fields }),
    }
}

#[test]
fn a_narrowed_decision_carries_its_replacement_arguments() {
    let decision = narrowed();
    round_trip(&decision);

    let decoded = PermissionDecision::decode(decision.encode_to_vec().as_slice())
        .ok()
        .and_then(|d| d.updated_input);
    let fields = decoded.map(|s| s.fields).unwrap_or_default();

    assert_eq!(
        fields.get("path").and_then(|v| v.kind.clone()),
        Some(Kind::StringValue("/workspace/atlas".to_owned())),
        "the clamped path must survive the wire, or the original call runs"
    );
    assert_eq!(fields.len(), 2);
}

#[test]
fn absent_updated_input_is_distinguishable_from_an_empty_struct() {
    // "Allow as proposed" and "allow with no arguments" are different
    // instructions to the Worker. Collapsing them would let a rewrite that
    // emptied the arguments read as a plain approval.
    let as_proposed = PermissionDecision {
        updated_input: None,
        ..narrowed()
    };
    let emptied = PermissionDecision {
        updated_input: Some(Struct::default()),
        ..narrowed()
    };

    assert_ne!(as_proposed, emptied);
    round_trip(&as_proposed);
    round_trip(&emptied);

    // And the distinction has to hold after a wire round trip, not just in Rust.
    let decoded_absent = PermissionDecision::decode(as_proposed.encode_to_vec().as_slice()).ok();
    let decoded_empty = PermissionDecision::decode(emptied.encode_to_vec().as_slice()).ok();

    assert_eq!(decoded_absent.and_then(|d| d.updated_input), None);
    assert_eq!(
        decoded_empty.and_then(|d| d.updated_input),
        Some(Struct::default())
    );
}

#[test]
fn a_denial_round_trips_without_replacement_arguments() {
    round_trip(&PermissionDecision {
        request_id: Some(PermissionRequestId::new("perm-2")),
        outcome: PermissionOutcome::Deny.into(),
        decided_by: PermissionLayer::DeterministicDeny.into(),
        decided_by_actor: None,
        reason: "writes outside the workspace".to_owned(),
        remember_for_session: false,
        updated_input: None,
    });
}

#[test]
fn permission_decision_field_numbers_are_stable() {
    // updated_input is additive; renumbering any of the shipped fields would
    // reinterpret decisions already sitting in Session logs.
    use horde_proto::prost_types::FileDescriptorSet;

    let Ok(set) = FileDescriptorSet::decode(horde_proto::FILE_DESCRIPTOR_SET) else {
        panic!("descriptor set should decode");
    };
    let fields: Vec<(String, i32)> = set
        .file
        .into_iter()
        .find(|f| f.name() == "horde/v1/permission.proto")
        .into_iter()
        .flat_map(|f| f.message_type)
        .find(|m| m.name() == "PermissionDecision")
        .map(|m| {
            m.field
                .iter()
                .map(|f| (f.name().to_owned(), f.number()))
                .collect()
        })
        .unwrap_or_default();

    assert_eq!(
        fields,
        vec![
            ("request_id".to_owned(), 1),
            ("outcome".to_owned(), 2),
            ("decided_by".to_owned(), 3),
            ("decided_by_actor".to_owned(), 4),
            ("reason".to_owned(), 5),
            ("remember_for_session".to_owned(), 6),
            ("updated_input".to_owned(), 7),
        ]
    );
}
