//! The wall clock, read in one place.
//!
//! Certificate validity, code expiry, and event timestamps all take explicit
//! `now_unix_ms` parameters so their decisions are testable at any instant;
//! this is the single place the edges read the real clock to supply them.

/// The current wall clock as unix milliseconds.
///
/// A clock before the epoch answers zero rather than panicking: every
/// consumer treats time as data, and refusing to start because the clock is
/// absurd helps nobody the log could not help more.
#[must_use]
pub fn unix_now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_millis())
            .unwrap_or_default(),
    )
    .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::unix_now_ms;

    #[test]
    fn the_clock_reads_as_the_present_and_moves_forward() {
        let first = unix_now_ms();
        // 2020-01-01T00:00:00Z; a machine reporting earlier is broken in a
        // way this function must not hide.
        assert!(first > 1_577_836_800_000, "the clock reads {first}");
        assert!(unix_now_ms() >= first, "time must not run backwards here");
    }
}
