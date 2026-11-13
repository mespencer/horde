//! The duration and size spellings every binary's flags share (ADR-0014's
//! configuration keys: `60s`, `4h`, `20GiB`, `10%`).
//!
//! One grammar, here, because the Server, the Worker and the CLI all parse
//! the same keys, and three parsers would drift on what `1d` or `0` means.

use std::time::Duration;

/// A duration as `0`, `90s`, `10m`, `4h` or `1d`; a bare number is seconds.
///
/// # Errors
///
/// Anything else, in words naming the accepted forms.
pub fn parse_duration(value: &str) -> Result<Duration, String> {
    let refuse = || format!("{value:?} is not a duration such as 90s, 10m, 4h or 1d");
    let value = value.trim();
    let split = value.find(|c: char| !c.is_ascii_digit()).unwrap_or(value.len());
    let (digits, unit) = value.split_at(split);
    let amount: u64 = digits.parse().map_err(|_| refuse())?;
    let scale = match unit {
        "" | "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        _ => return Err(refuse()),
    };
    amount
        .checked_mul(scale)
        .map(Duration::from_secs)
        .ok_or_else(refuse)
}

/// A size as bytes or `KiB`/`MiB`/`GiB`/`TiB`.
///
/// # Errors
///
/// Anything else, in words naming the accepted forms.
pub fn parse_size(value: &str) -> Result<u64, String> {
    let refuse = || format!("{value:?} is not a size such as 512MiB or 20GiB");
    let value = value.trim();
    let split = value.find(|c: char| !c.is_ascii_digit()).unwrap_or(value.len());
    let (digits, unit) = value.split_at(split);
    let amount: u64 = digits.parse().map_err(|_| refuse())?;
    let scale: u64 = match unit {
        "" | "B" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "TiB" => 1 << 40,
        _ => return Err(refuse()),
    };
    amount.checked_mul(scale).ok_or_else(refuse)
}

/// A free-space threshold as `10%`, `20GiB`, or both comma-separated (the
/// larger applies): `(basis points, bytes)`, where 1000 basis points is 10 %.
///
/// # Errors
///
/// A part that is neither a percentage up to 100 nor a size.
pub fn parse_threshold(value: &str) -> Result<(u64, u64), String> {
    let mut threshold = (0, 0);
    for part in value.split(',').map(str::trim) {
        match part.strip_suffix('%') {
            Some(percent) => {
                let percent: u64 = percent
                    .parse()
                    .ok()
                    .filter(|percent| *percent <= 100)
                    .ok_or_else(|| format!("{part:?} is not a percentage from 0% to 100%"))?;
                threshold.0 = percent * 100;
            }
            None => threshold.1 = parse_size(part)?,
        }
    }
    Ok(threshold)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_take_seconds_minutes_hours_and_days() {
        for (value, seconds) in [("0", 0), ("90", 90), ("90s", 90), ("10m", 600), ("4h", 14_400), ("1d", 86_400)] {
            assert_eq!(parse_duration(value), Ok(Duration::from_secs(seconds)), "{value}");
        }
        for wrong in ["", "4w", "h", "-1s", "1.5h", "99999999999999999999d"] {
            assert!(parse_duration(wrong).is_err(), "{wrong}");
        }
    }

    #[test]
    fn sizes_take_bytes_and_binary_units() {
        assert_eq!(parse_size("512"), Ok(512));
        assert_eq!(parse_size("512B"), Ok(512));
        assert_eq!(parse_size("2KiB"), Ok(2048));
        assert_eq!(parse_size("3MiB"), Ok(3 << 20));
        assert_eq!(parse_size("20GiB"), Ok(20 << 30));
        assert_eq!(parse_size("1TiB"), Ok(1 << 40));
        for wrong in ["", "1GB", "x", "99999999999TiB"] {
            assert!(parse_size(wrong).is_err(), "{wrong}");
        }
    }

    #[test]
    fn thresholds_take_a_share_a_size_or_both() {
        assert_eq!(parse_threshold("10%"), Ok((1_000, 0)));
        assert_eq!(parse_threshold("20GiB"), Ok((0, 20 << 30)));
        assert_eq!(parse_threshold("10%,20GiB"), Ok((1_000, 20 << 30)));
        assert!(parse_threshold("101%").is_err());
        assert!(parse_threshold("ten%").is_err());
    }
}
