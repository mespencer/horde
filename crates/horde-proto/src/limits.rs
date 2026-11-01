//! Wire limits both ends of a channel agree on.
//!
//! tonic's default decoding limit is 4 MiB, applied silently on whichever end
//! happens to receive: a Worker pushing a batch one byte over it gets an
//! opaque `OutOfRange` and redials into the same batch forever. These limits
//! are set deliberately, on the Server's worker-facing service and on every
//! Worker client of it, so the ceiling is a decision both sides can see — and
//! the Worker sizes its push batches well inside it.

/// The largest gRPC message either end of the worker spine encodes or
/// decodes. Generous on purpose: a single journaled event (a permission
/// request carrying a large file write) must fit whole, because an event
/// cannot be split across messages.
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// The encoded-event budget of one Worker push batch: a sixteenth of
/// [`MAX_MESSAGE_BYTES`], so a batch never nears the ceiling and one slow
/// batch never holds megabytes hostage across a flaky link.
pub const PUSH_BATCH_BYTES: usize = MAX_MESSAGE_BYTES / 16;
