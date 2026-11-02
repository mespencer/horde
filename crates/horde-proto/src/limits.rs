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

/// Holds a generated tonic server or client to [`MAX_MESSAGE_BYTES`] in both
/// directions. Every server and client in the workspace is built through
/// this, so no channel falls back to tonic's 4 MiB: an event the worker
/// spine can ingest is one every reader can be sent back.
///
/// A macro because tonic's generated types share no trait for the two
/// setters; the one spelling keeps the ceiling from drifting per call site.
#[macro_export]
macro_rules! limited {
    ($service:expr) => {
        $service
            .max_decoding_message_size($crate::limits::MAX_MESSAGE_BYTES)
            .max_encoding_message_size($crate::limits::MAX_MESSAGE_BYTES)
    };
}

#[cfg(test)]
mod tests {
    /// The ceiling both ends agree on, pinned by value: a different number
    /// on either side is a refusal the other side cannot explain.
    #[test]
    fn the_limits_are_sixteen_mebibytes_and_one_for_a_batch() {
        assert_eq!(super::MAX_MESSAGE_BYTES, 16_777_216);
        assert_eq!(super::PUSH_BATCH_BYTES, 1_048_576);
    }
}
