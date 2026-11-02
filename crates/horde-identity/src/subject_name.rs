//! Sanity checks for the extra names a server certificate answers to.
//!
//! A `--san` is minted into the listener certificate and named by every
//! join token, and the joiner dials it over mTLS forever after. A typo'd
//! name used to surface only at that dial, as a certificate mismatch on
//! another machine; these checks refuse the obviously invalid ones at mint
//! time instead, naming what is wrong. They are syntactic — whether the
//! name resolves is the network's question, and the dial-time diagnosis
//! remains the backstop for a well-formed name that points nowhere.

/// The longest DNS name, in characters, without the trailing dot.
const MAX_NAME_LEN: usize = 253;

/// The longest DNS label.
const MAX_LABEL_LEN: usize = 63;

/// Refuses a subject name no certificate should carry, with the reason.
///
/// An IP literal is accepted as is. Anything else must be a DNS host name:
/// dot-separated labels of ASCII letters, digits, and inner hyphens, at
/// most 63 characters each and 253 in all, whose last label is not all
/// digits (that is a mistyped address, not a name).
///
/// # Errors
///
/// A sentence naming the flaw: a URL or port where a host belongs,
/// whitespace, a wildcard, an empty or overlong label, or a bad character.
pub fn check_subject_name(name: &str) -> Result<(), String> {
    if name.parse::<std::net::IpAddr>().is_ok() {
        return Ok(());
    }
    match shape_refusal(name).or_else(|| dns_refusal(name)) {
        Some(reason) => Err(format!(
            "{name:?} is not a usable certificate name: {reason}"
        )),
        None => Ok(()),
    }
}

/// The mistakes that are not about DNS at all: a whole URL, a port, a
/// wildcard, or whitespace pasted where a host belongs.
fn shape_refusal(name: &str) -> Option<String> {
    let reason = if name.contains("://") {
        "that is a URL; pass only its host"
    } else if name.contains(':') {
        "a port or an address with a colon; pass only the host (IPv6 without brackets)"
    } else if name.contains('*') {
        "wildcard names are not supported; name each host"
    } else if name.chars().any(char::is_whitespace) {
        "it contains whitespace"
    } else {
        return None;
    };
    Some(reason.to_owned())
}

/// What makes `name` an invalid DNS host name, if anything.
fn dns_refusal(name: &str) -> Option<String> {
    if name.is_empty() {
        return Some("it is empty".to_owned());
    }
    if name.len() > MAX_NAME_LEN {
        return Some(format!("it is longer than {MAX_NAME_LEN} characters"));
    }
    let labels: Vec<&str> = name.split('.').collect();
    if let Some(reason) = labels.iter().find_map(|label| label_refusal(label)) {
        return Some(reason);
    }
    labels
        .last()
        .filter(|last| last.chars().all(|c| c.is_ascii_digit()))
        .map(|_| "its last label is all digits, which is a malformed IP address".to_owned())
}

/// What is wrong with one dot-separated label, if anything.
fn label_refusal(label: &str) -> Option<String> {
    if label.is_empty() {
        return Some("it has an empty label (a leading, trailing, or doubled dot)".to_owned());
    }
    if label.len() > MAX_LABEL_LEN {
        return Some(format!(
            "the label {label:?} is longer than {MAX_LABEL_LEN} characters"
        ));
    }
    if label.starts_with('-') || label.ends_with('-') {
        return Some(format!("the label {label:?} starts or ends with a hyphen"));
    }
    label
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '-'))
        .map(|bad| format!("{bad:?} is not allowed in a host name"))
}

#[cfg(test)]
mod tests {
    use super::check_subject_name;

    #[test]
    fn host_names_and_ip_literals_pass() {
        for good in [
            "localhost",
            "build-box",
            "horde.example.com",
            "a1.b2",
            "10.0.0.7",
            "::1",
            "fe80::1",
            &"a".repeat(63),
            &format!(
                "{}.{}.{}.{}",
                "a".repeat(63),
                "b".repeat(63),
                "c".repeat(63),
                "d".repeat(61)
            ),
        ] {
            assert_eq!(check_subject_name(good), Ok(()), "{good}");
        }
    }

    fn reason(name: &str) -> String {
        check_subject_name(name).expect_err("refused")
    }

    #[test]
    fn each_obvious_mistake_is_refused_by_name() {
        let cases = [
            ("https://horde.example", "URL"),
            ("horde.example:7777", "port"),
            ("[::1]", "port"),
            ("*.example.com", "wildcard"),
            ("horde example", "whitespace"),
            ("", "empty"),
            (".example", "empty label"),
            ("example.", "empty label"),
            ("a..b", "empty label"),
            ("-box", "hyphen"),
            ("box-", "hyphen"),
            ("under_score", "'_'"),
            ("10.0.0.256", "all digits"),
            ("1234", "all digits"),
        ];
        for (name, expected) in cases {
            let refusal = reason(name);
            assert!(refusal.contains(expected), "{name}: {refusal}");
            assert!(refusal.contains(&format!("{name:?}")), "{refusal}");
        }
    }

    #[test]
    fn the_length_limits_are_exact() {
        assert!(reason(&"a".repeat(64)).contains("longer than 63"));
        let long = format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(62)
        );
        assert_eq!(long.len(), 254);
        assert!(reason(&long).contains("longer than 253"));
    }
}
