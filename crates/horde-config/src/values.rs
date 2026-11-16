//! Rendering values the way a config file spells them, so `config show`
//! output can be pasted back into a file. Parsing is `horde_proto::units`,
//! the grammar the flags already share.

use std::time::Duration;

/// `0`, `90s`, `10m`, `4h` or `1d` — the largest whole unit.
#[must_use]
pub fn show_duration(duration: &Duration) -> String {
    let seconds = duration.as_secs();
    if seconds == 0 {
        return "0".to_owned();
    }
    [(86_400, "d"), (3_600, "h"), (60, "m")]
        .into_iter()
        .find(|(scale, _)| seconds.is_multiple_of(*scale))
        .map_or_else(
            || format!("{seconds}s"),
            |(scale, unit)| format!("{}{unit}", seconds / scale),
        )
}

/// A duration that may be unset, which the keys that take one read as
/// "never" or "no limit".
#[must_use]
pub fn show_optional_duration(duration: &Option<Duration>) -> String {
    duration
        .as_ref()
        .map_or_else(|| "unset".to_owned(), show_duration)
}

/// Bytes in the largest whole binary unit: `512`, `16MiB`, `20GiB`.
#[must_use]
pub fn show_size(bytes: &u64) -> String {
    if *bytes == 0 {
        return "0".to_owned();
    }
    [
        (1u64 << 40, "TiB"),
        (1 << 30, "GiB"),
        (1 << 20, "MiB"),
        (1 << 10, "KiB"),
    ]
    .into_iter()
    .find(|(scale, _)| bytes.is_multiple_of(*scale))
    .map_or_else(
        || bytes.to_string(),
        |(scale, unit)| format!("{}{unit}", bytes / scale),
    )
}

/// `(basis points, bytes)` as `10%`, `20GiB` or `10%,20GiB`.
#[must_use]
pub fn show_threshold(threshold: &(u64, u64)) -> String {
    let (basis_points, bytes) = *threshold;
    let percent = format!("{}%", basis_points / 100);
    match (basis_points, bytes) {
        (_, 0) => percent,
        (0, bytes) => show_size(&bytes),
        (_, bytes) => format!("{percent},{}", show_size(&bytes)),
    }
}

/// A string quoted the way TOML would.
#[must_use]
pub fn show_string(value: &String) -> String {
    format!("{value:?}")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{show_duration, show_optional_duration, show_size, show_string, show_threshold};

    #[test]
    fn durations_render_in_their_largest_whole_unit_and_parse_back() {
        for (seconds, shown) in [
            (0, "0"),
            (90, "90s"),
            (60, "1m"),
            (600, "10m"),
            (3_600, "1h"),
            (5_400, "90m"),
            (86_400, "1d"),
            (14 * 86_400, "14d"),
            (86_401, "86401s"),
        ] {
            let duration = Duration::from_secs(seconds);
            assert_eq!(show_duration(&duration), shown);
            assert_eq!(horde_proto::units::parse_duration(shown), Ok(duration));
        }
        assert_eq!(show_optional_duration(&None), "unset");
        assert_eq!(show_optional_duration(&Some(Duration::from_secs(60))), "1m");
    }

    #[test]
    fn sizes_render_in_their_largest_whole_unit_and_parse_back() {
        for (bytes, shown) in [
            (0, "0"),
            (512, "512"),
            (1 << 10, "1KiB"),
            (1536, "1536"),
            (16 << 20, "16MiB"),
            (20 << 30, "20GiB"),
            (1 << 40, "1TiB"),
            (3 << 29, "1536MiB"),
        ] {
            assert_eq!(show_size(&bytes), shown);
            assert_eq!(horde_proto::units::parse_size(shown), Ok(bytes));
        }
    }

    #[test]
    fn thresholds_render_either_or_both_halves_and_parse_back() {
        for (threshold, shown) in [
            ((1_000, 20 << 30), "10%,20GiB"),
            ((1_500, 0), "15%"),
            ((0, 2 << 30), "2GiB"),
            ((0, 0), "0%"),
        ] {
            assert_eq!(show_threshold(&threshold), shown);
            assert_eq!(horde_proto::units::parse_threshold(shown), Ok(threshold));
        }
    }

    #[test]
    fn strings_are_quoted() {
        assert_eq!(show_string(&"a \"b\"".to_owned()), r#""a \"b\"""#);
    }
}
