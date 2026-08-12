//! The domain↔wire correspondence, in one place.
//!
//! `horde-adapter` owns domain types and knows nothing about protobuf.
//! `horde-proto` owns wire types and knows nothing about domain semantics.
//! That separation is deliberate — but it leaves the correspondence between
//! them unowned, and an unowned correspondence drifts silently: two enums grow
//! apart, a conversion quietly drops a field, and nothing fails until a
//! scheduler reads a headroom it never received.
//!
//! This crate is that owner. Every mapping here is an **exhaustive match in
//! both directions**, so adding a variant on either side stops compiling until
//! someone decides what it means on the other. That is the same trick the
//! event-vocabulary test plays on `Event.kind`, applied to types instead of a
//! oneof.
//!
//! # What is deliberately *not* here
//!
//! Lossless round-tripping. Some wire states have no domain equivalent
//! (`*_UNSPECIFIED`), and some domain states carry more than the wire does.
//! Those asymmetries are named at each conversion rather than smoothed over.

pub mod capability;
pub mod permission;
pub mod usage;

pub use capability::{UnknownCapability, capability_from_wire, capability_to_wire};
pub use permission::{
    ConversionError, permission_decision_from_wire, permission_decision_to_wire,
    proposed_tool_call_to_wire,
};
pub use usage::{
    UnreadableUtilization, rate_window_from_wire, rate_window_to_wire, token_usage_from_wire,
    token_usage_to_wire,
};
