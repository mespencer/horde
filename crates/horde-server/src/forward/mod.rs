//! Port forwards (ADR-0006): durable intent in the log, ephemeral
//! realization on live connections, and the relay that splices the two
//! authenticated legs of every tunneled TCP connection.
//!
//! Three parts, three modules:
//!
//! * [`intent`] — the durable half: spec validation, the liveness rules the
//!   fold's [`crate::state::forward_is_live`] is read through, and the opener
//!   that turns a settled permission decision into a `ForwardOpened` event.
//! * [`registry`] — the ephemeral half: which workers and clients hold live
//!   forward streams right now, and the pending pairings of tunnel legs.
//!   Nothing here survives a restart, which is the point.
//! * [`relay`] — the byte plane: one task per tunnel leg, paired through the
//!   registry, spliced frame by frame with gRPC's own HTTP/2 flow control as
//!   the backpressure.

pub mod intent;
pub mod registry;
pub mod relay;

pub use registry::ForwardRegistry;
