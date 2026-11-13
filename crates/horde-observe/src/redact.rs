//! Secret redaction at egress (ADR-0015).
//!
//! Harness stderr is uncontrolled text, so redaction is enforced in one
//! place, on the way out of a machine: every path that serves a record to a
//! reader — the worker's tunnel, the server's `ReadLogs`, the endpoint's
//! local `logs` — goes through [`crate::reader::egress`], which calls
//! [`redact_line`]. Nothing else has a copy of these rules.
//!
//! Three parts, and no entropy heuristic (it would eat commit hashes and
//! ids, which are what a debugging reader needs):
//!
//! - **Known shapes**: join tokens, PEM private keys, `Authorization` and
//!   `Bearer` values, secret URL query values, and common provider and
//!   forge key prefixes.
//! - **Secret-named fields**: a JSON key containing `token`, `secret`,
//!   `password`, … has its *string* value replaced at any depth. Numbers,
//!   booleans and nulls survive, so usage counts such as `output_tokens`
//!   stay readable.
//! - **Registered values**: the exact secrets this process holds (minted
//!   join secrets, the bootstrap token), scrubbed wherever they appear.

use std::sync::{LazyLock, Mutex};
use std::time::SystemTime;

use regex::Regex;
use serde_json::Value;

/// Values shorter than this are never registered: a short "secret" would
/// scrub ordinary words out of every record.
pub const MIN_REGISTERED_LEN: usize = 8;

/// How many registered values a process keeps; the oldest goes first.
pub const MAX_REGISTERED: usize = 1024;

/// Key fragments that mark a field as holding a secret.
const SECRET_KEYS: [&str; 9] = [
    "token",
    "secret",
    "password",
    "passwd",
    "credential",
    "api_key",
    "apikey",
    "cookie",
    "authorization",
];

/// One known secret shape: its pattern, its replacement, and its kind.
struct Shape {
    pattern: Regex,
    replacement: &'static str,
}

/// The shapes, in the order they run. `Authorization` runs before `Bearer`
/// so a header's value is named as the header's.
static SHAPES: LazyLock<Vec<Shape>> = LazyLock::new(|| {
    [
        (r#"horde://join/[^\s"'<>]+"#, "[REDACTED:join-token]"),
        (
            r"(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----",
            "[REDACTED:private-key]",
        ),
        (
            r#"(?i)\b(authorization["']?\s*[:=]\s*["']?)(?:(?:bearer|basic|token)\s+)?[^\s"',;]+"#,
            "${1}[REDACTED:authorization]",
        ),
        (
            r"(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]{8,}",
            "${1}[REDACTED:bearer]",
        ),
        (
            r#"(?i)([?&](?:code|access_token|refresh_token|token|secret)=)[^&\s"'#]+"#,
            "${1}[REDACTED:url-secret]",
        ),
        (
            r"\b(?:sk-ant-|sk-|ghp_|gho_|github_pat_|xoxb-|xoxp-)[A-Za-z0-9_-]{8,}",
            "[REDACTED:api-key]",
        ),
        (r"\bAKIA[0-9A-Z]{16}\b", "[REDACTED:api-key]"),
    ]
    .into_iter()
    .filter_map(|(pattern, replacement)| {
        // The patterns are literals tested below; one that failed to compile
        // would be a build defect the table test catches by count.
        Regex::new(pattern).ok().map(|pattern| Shape {
            pattern,
            replacement,
        })
    })
    .collect()
});

/// How many shapes the redactor knows — pinned by a test, so a pattern that
/// stops compiling cannot silently leave the table.
#[must_use]
pub fn shape_count() -> usize {
    SHAPES.len()
}

/// The exact secret values a process holds, each until it is redeemed or
/// expires.
#[derive(Debug, Default)]
pub struct SecretRegistry {
    entries: Mutex<Vec<(String, Option<SystemTime>)>>,
}

impl SecretRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, Vec<(String, Option<SystemTime>)>> {
        match self.entries.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Remembers `value` until `expires` (forever when `None`). Values
    /// shorter than [`MIN_REGISTERED_LEN`] are ignored.
    pub fn register(&self, value: &str, expires: Option<SystemTime>) {
        if value.len() < MIN_REGISTERED_LEN {
            return;
        }
        let mut entries = self.locked();
        entries.retain(|(held, _)| held != value);
        if entries.len() >= MAX_REGISTERED {
            entries.remove(0);
        }
        entries.push((value.to_owned(), expires));
    }

    /// Forgets `value` — its secret was redeemed.
    pub fn forget(&self, value: &str) {
        self.locked().retain(|(held, _)| held != value);
    }

    /// The values still live at `now`; expired ones are dropped.
    #[must_use]
    pub fn live(&self, now: SystemTime) -> Vec<String> {
        let mut entries = self.locked();
        entries.retain(|(_, expires)| expires.is_none_or(|at| at > now));
        entries.iter().map(|(value, _)| value.clone()).collect()
    }
}

/// The process's registry, which [`redact_line`] consults.
static REGISTRY: LazyLock<SecretRegistry> = LazyLock::new(SecretRegistry::new);

/// Registers a secret this process holds with the process registry.
pub fn register_secret(value: &str, expires: Option<SystemTime>) {
    REGISTRY.register(value, expires);
}

/// Forgets a secret this process no longer holds.
pub fn forget_secret(value: &str) {
    REGISTRY.forget(value);
}

/// Redacts one record against the process registry. Returns the redacted
/// line and whether anything was replaced.
#[must_use]
pub fn redact_line(line: &str) -> (String, bool) {
    redact_with(line, &REGISTRY.live(SystemTime::now()))
}

/// Redacts one record against an explicit list of registered values. A line
/// that is a JSON object is walked (keys and string values); anything else
/// is redacted as text.
#[must_use]
pub fn redact_with(line: &str, registered: &[String]) -> (String, bool) {
    match serde_json::from_str::<Value>(line) {
        Ok(mut value @ Value::Object(_)) => {
            let mut touched = false;
            walk(&mut value, registered, &mut touched);
            if touched {
                (value.to_string(), true)
            } else {
                (line.to_owned(), false)
            }
        }
        _ => {
            let redacted = redact_text(line, registered);
            let touched = redacted != line;
            (redacted, touched)
        }
    }
}

/// Whether a JSON key names a secret.
#[must_use]
pub fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    SECRET_KEYS.iter().any(|fragment| key.contains(fragment))
}

fn walk(value: &mut Value, registered: &[String], touched: &mut bool) {
    match value {
        Value::Object(map) => {
            for (key, inner) in map.iter_mut() {
                if is_secret_key(key) && inner.is_string() {
                    *inner = Value::String("[REDACTED:field]".to_owned());
                    *touched = true;
                } else {
                    walk(inner, registered, touched);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                walk(item, registered, touched);
            }
        }
        Value::String(text) => {
            let redacted = redact_text(text, registered);
            if redacted != *text {
                *text = redacted;
                *touched = true;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Redacts free text: the known shapes, then the registered values.
#[must_use]
pub fn redact_text(text: &str, registered: &[String]) -> String {
    let mut out = text.to_owned();
    for shape in SHAPES.iter() {
        if shape.pattern.is_match(&out) {
            out = shape
                .pattern
                .replace_all(&out, shape.replacement)
                .into_owned();
        }
    }
    for value in registered {
        if value.len() >= MIN_REGISTERED_LEN && out.contains(value.as_str()) {
            out = out.replace(value.as_str(), "[REDACTED:secret]");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::{
        MAX_REGISTERED, SecretRegistry, is_secret_key, redact_text, redact_with, shape_count,
    };

    /// Each shape: a positive case that must be scrubbed, and a near miss
    /// that must survive untouched.
    #[test]
    fn every_known_shape_is_scrubbed_and_its_near_miss_survives() {
        let cases: [(&str, &str, &str); 9] = [
            (
                "join with horde://join/host:7000/abcd.ef01 now",
                "join with [REDACTED:join-token] now",
                "horde join is a verb",
            ),
            (
                "k=-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY----- end",
                "k=[REDACTED:private-key] end",
                "-----BEGIN CERTIFICATE----- public -----END CERTIFICATE-----",
            ),
            (
                "Authorization: Bearer abc.def.ghi",
                "Authorization: [REDACTED:authorization]",
                "authorization was granted",
            ),
            (
                "sent bearer eyJhbGciOiJIUzI1NiJ9 upstream",
                "sent bearer [REDACTED:bearer] upstream",
                "the bearer of bad news",
            ),
            (
                "GET /cb?state=x&code=s3cr3tc0de&scope=a",
                "GET /cb?state=x&code=[REDACTED:url-secret]&scope=a",
                "GET /cb?state=x&encode=1",
            ),
            (
                "key sk-ant-api03-AAAAAAAAAAAA used",
                "key [REDACTED:api-key] used",
                "the sketch-pad and task-12345678 stay",
            ),
            (
                "ghp_0123456789abcdef and github_pat_11ABCDEFG0123",
                "[REDACTED:api-key] and [REDACTED:api-key]",
                "commit 0123456789abcdef0123456789abcdef01234567",
            ),
            (
                "slack xoxb-1234-5678-abcdef",
                "slack [REDACTED:api-key]",
                "agent-7f3e2a1b-xoxo",
            ),
            (
                "aws AKIAIOSFODNN7EXAMPLE here",
                "aws [REDACTED:api-key] here",
                "AKIA-short and XAKIAIOSFODNN7EXAMPLE",
            ),
        ];
        for (secret, expected, near_miss) in cases {
            assert_eq!(redact_text(secret, &[]), expected, "{secret:?}");
            assert_eq!(redact_text(near_miss, &[]), near_miss, "{near_miss:?}");
        }
        assert_eq!(shape_count(), 7, "every pattern compiled");
    }

    #[test]
    fn secret_named_fields_are_replaced_at_any_depth_but_numbers_survive() {
        let line = r#"{"msg":"m","fields":{"api_key":"abc","nested":{"Password":"p","list":[{"refresh_token":"r"}]},"output_tokens":42,"cookie":null}}"#;
        let (out, touched) = redact_with(line, &[]);
        assert!(touched);
        let value: serde_json::Value = serde_json::from_str(&out).expect("json");
        let fields = &value["fields"];
        assert_eq!(fields["api_key"], "[REDACTED:field]");
        assert_eq!(fields["nested"]["Password"], "[REDACTED:field]");
        assert_eq!(
            fields["nested"]["list"][0]["refresh_token"],
            "[REDACTED:field]"
        );
        assert_eq!(fields["output_tokens"], 42, "a count is not a secret");
        assert!(fields["cookie"].is_null());
        assert_eq!(value["msg"], "m");
    }

    #[test]
    fn a_clean_record_comes_back_byte_identical() {
        let line = r#"{"msg":"agent-1 committed 0123abcd","seq":1}"#;
        assert_eq!(redact_with(line, &[]), (line.to_owned(), false));
        assert_eq!(
            redact_with("plain text", &[]),
            ("plain text".to_owned(), false)
        );
    }

    #[test]
    fn text_that_is_not_json_is_redacted_as_text() {
        let (out, touched) = redact_with("token horde://join/h:1/x.y", &[]);
        assert!(touched);
        assert_eq!(out, "token [REDACTED:join-token]");
    }

    #[test]
    fn registered_values_are_scrubbed_until_forgotten_or_expired() {
        let registry = SecretRegistry::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        registry.register("short", None);
        registry.register("held-forever-1", None);
        registry.register("expires-soon-2", Some(now + Duration::from_secs(10)));
        assert_eq!(registry.live(now).len(), 2, "short values are never held");

        let line = r#"{"msg":"saw held-forever-1 and expires-soon-2"}"#;
        let (out, touched) = redact_with(line, &registry.live(now));
        assert!(touched);
        assert_eq!(
            out,
            r#"{"msg":"saw [REDACTED:secret] and [REDACTED:secret]"}"#
        );

        let later = now + Duration::from_secs(10);
        assert_eq!(registry.live(later), ["held-forever-1"], "expiry forgets");
        registry.forget("held-forever-1");
        assert!(registry.live(later).is_empty(), "redemption forgets");
    }

    #[test]
    fn the_registry_is_bounded_and_re_registering_does_not_duplicate() {
        let registry = SecretRegistry::new();
        for n in 0..MAX_REGISTERED + 5 {
            registry.register(&format!("secret-value-{n}"), None);
        }
        registry.register("secret-value-1000", None);
        let live = registry.live(SystemTime::now());
        assert_eq!(live.len(), MAX_REGISTERED);
        assert!(
            !live.iter().any(|v| v == "secret-value-0"),
            "oldest went first"
        );
        assert_eq!(live.last().map(String::as_str), Some("secret-value-1000"));
        assert_eq!(live.iter().filter(|v| *v == "secret-value-1000").count(), 1);
    }

    #[test]
    fn secret_keys_match_by_fragment_and_case() {
        for key in [
            "token",
            "ACCESS_TOKEN",
            "x-api_key",
            "apikey",
            "Set-Cookie",
            "passwd",
            "credentials",
            "client_secret",
            "authorization",
        ] {
            assert!(is_secret_key(key), "{key}");
        }
        for key in ["msg", "agent", "session", "target", "level"] {
            assert!(!is_secret_key(key), "{key}");
        }
    }
}
