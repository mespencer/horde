//! Schema-derived names for event kinds.
//!
//! Diagnostics all over the workspace want to say *which* kind of event they
//! are talking about — a collision error, a rendered log line — and the only
//! honest source for those names is `event.proto` itself. Each consumer used
//! to keep its own fifteen-arm match; two copies of the same table is a drift
//! waiting for its moment, and the table outgrew the complexity gate besides.
//! Here the names come from the compiled [`FileDescriptorSet`](crate::FILE_DESCRIPTOR_SET),
//! so a kind added to the schema names itself and no match has to grow.
//!
//! The lookup works by encoding the kind and reading the field tag off the
//! wire: a `Kind` value *is* one field of `Event`'s oneof, and the descriptor
//! knows what that field is called.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use prost::Message;
use prost_types::FileDescriptorSet;

use crate::v1;
use crate::v1::event::Kind;

/// The `Event.kind` oneof field name a kind is declared under, e.g.
/// `"placement_changed"`.
///
/// `None` only if the compiled descriptor does not describe the kind — which
/// a matching build of this crate makes impossible, but inventing a name here
/// would hide exactly the skew the caller is trying to diagnose.
#[must_use]
pub fn kind_field_name(kind: &Kind) -> Option<&'static str> {
    let event = v1::Event {
        agent_id: None,
        occurred_at: None,
        idempotency_key: String::new(),
        kind: Some(kind.clone()),
    };
    // With every other field empty, the encoding is exactly one field: the
    // kind's. Its key carries the field tag the descriptor names.
    let encoded = event.encode_to_vec();
    let mut buffer = encoded.as_slice();
    let key = prost::encoding::decode_varint(&mut buffer).ok()?;
    let tag = u32::try_from(key >> 3).ok()?;
    kind_names_by_tag().get(&tag).copied()
}

/// The oneof's `tag → field name` table, read from the descriptor once.
fn kind_names_by_tag() -> &'static BTreeMap<u32, &'static str> {
    static NAMES: OnceLock<BTreeMap<u32, &'static str>> = OnceLock::new();
    NAMES.get_or_init(|| {
        let Ok(set) = FileDescriptorSet::decode(crate::FILE_DESCRIPTOR_SET) else {
            return BTreeMap::new();
        };
        let mut names = BTreeMap::new();
        let events = set
            .file
            .iter()
            .flat_map(|file| &file.message_type)
            .filter(|message| message.name() == "Event");
        for message in events {
            // An Event without a `kind` oneof has nothing to name; skipping it
            // outright keeps the field check below a single comparison against
            // a known index rather than a compound over two optionals.
            let Some(kind_oneof) = message
                .oneof_decl
                .iter()
                .position(|oneof| oneof.name() == "kind")
                .and_then(|index| i32::try_from(index).ok())
            else {
                continue;
            };
            for field in &message.field {
                if field.oneof_index == Some(kind_oneof) {
                    let name: &'static str = Box::leak(field.name().to_owned().into_boxed_str());
                    names.insert(u32::try_from(field.number()).unwrap_or(0), name);
                }
            }
        }
        names
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The names are the proto's own field names, read back for a spread of
    /// kinds across both varint widths (tags 10 and 24 encode in one and two
    /// bytes respectively — the decode must not assume a single byte).
    #[test]
    fn kinds_name_themselves_after_their_schema_fields() {
        assert_eq!(
            kind_field_name(&Kind::AgentOutputChunk(v1::AgentOutputChunk::default())),
            Some("agent_output_chunk")
        );
        assert_eq!(
            kind_field_name(&Kind::PlacementChanged(v1::PlacementChanged::default())),
            Some("placement_changed")
        );
        assert_eq!(
            kind_field_name(&Kind::WorkspacePrewarmRequested(
                v1::WorkspacePrewarmRequested::default()
            )),
            Some("workspace_prewarm_requested"),
            "tag 24 is a two-byte key; a one-byte decode would misname it"
        );
    }
}
