//! Wire contracts for Horde.
//!
//! The `.proto` sources under `proto/horde/v1/` are the contract; everything in
//! this crate is generated from them plus a thin layer of ergonomics over the
//! identifier types. Nothing here holds domain logic: callers convert these
//! transport types into their own domain types at the boundary, which is where
//! protobuf's optional-everything shape is supposed to stop.
//!
//! Two sequence spaces run through the event contract and are easy to confuse:
//!
//! * [`v1::EventEnvelope::sequence`] is per Session and assigned by the Server.
//!   Clients resume from it.
//! * [`v1::JournaledEvent::journal_sequence`] is per Worker and assigned by the
//!   Worker's local journal. The Server acks it so a Worker that was
//!   partitioned knows what to replay and what it may truncate.

/// Generated types and gRPC stubs for proto package `horde.v1`.
pub mod v1 {
    // Generated code: not hand-edited, and not held to the handwritten style
    // policy. Scoped to this module so handwritten code stays covered.
    #![allow(clippy::all, clippy::pedantic, missing_docs)]

    include!(concat!(env!("OUT_DIR"), "/horde.v1.rs"));
}

mod ids;
mod names;

pub use names::kind_field_name;

/// Compiled `FileDescriptorSet` for every file in `horde.v1`.
///
/// Backs the contract tests and, later, gRPC server reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/horde_v1_descriptor.bin"));

// Re-exported so downstream crates depend on exactly the runtime versions this
// crate's generated code was built against, instead of pinning them again.
pub use prost;
pub use prost_types;
pub use tonic;
