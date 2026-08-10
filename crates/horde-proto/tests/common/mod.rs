//! Shared assertions for the wire-contract tests.
//!
//! Integration test files are separate crates, so anything two of them need
//! lives here rather than being written twice.

use horde_proto::prost::Message;

/// Asserts a message survives an encode/decode round trip unchanged.
///
/// The whole point of these tests: a field that does not survive the trip is a
/// field the Worker never sees, and prost will happily drop one it does not
/// know about rather than fail.
#[track_caller]
#[expect(
    unreachable_pub,
    reason = "included as a module by each test binary that needs it"
)]
pub fn round_trip<M: Message + Default + PartialEq + std::fmt::Debug>(message: &M) {
    let decoded = M::decode(message.encode_to_vec().as_slice()).ok();
    assert_eq!(decoded.as_ref(), Some(message));
}
