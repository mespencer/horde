//! What turns export on, read once at startup.
//!
//! The environment is the one configuration surface every Horde binary
//! already shares (`RUST_LOG`, `HORDE_ENDPOINT`), so export opts in the same
//! way. The lookup is injected rather than read from [`std::env`] directly:
//! process environment is global mutable state, and a test that mutates it
//! races every other test in the harness.

/// Environment variable naming the OTLP/gRPC collector endpoint.
///
/// Setting it is what opts a binary in; absent or empty means off, and off
/// means the binary behaves exactly as it did before this crate existed.
pub const ENDPOINT_VAR: &str = "HORDE_OTLP_ENDPOINT";

/// Environment variable holding the trace-sampling ratio, `0.0..=1.0`.
pub const SAMPLE_VAR: &str = "HORDE_OTLP_SAMPLE";

/// A validated decision to export, and how much of it.
#[derive(Debug, Clone, PartialEq)]
pub struct OtlpConfig {
    /// The collector, scheme and all: `http://127.0.0.1:4317`.
    pub endpoint: String,
    /// The fraction of traces exported, `0.0..=1.0`.
    pub sample_ratio: f64,
}

/// Why a configuration was refused rather than used.
///
/// Every variant is a startup refusal on purpose. Both shapes are what a typo
/// produces, and a typo that silently exports nothing — or everything — would
/// only be discovered the day the missing traces were needed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// A ratio was given but no collector was; sampling nothing is not a
    /// configuration anyone means.
    #[error(
        "{SAMPLE_VAR} is set but {ENDPOINT_VAR} is not; \
         set the collector endpoint or unset the ratio"
    )]
    SampleWithoutEndpoint,
    /// The ratio did not parse, or fell outside `0.0..=1.0`.
    #[error("{SAMPLE_VAR} must be a number in 0.0..=1.0, got {given:?}")]
    InvalidSampleRatio {
        /// What the variable actually held.
        given: String,
    },
}

/// Reads the export decision out of the environment.
///
/// `Ok(None)` means off — the caller builds nothing and changes nothing.
/// The ratio defaults to `1.0` (export every trace) when only the endpoint
/// is set.
///
/// # Errors
///
/// [`ConfigError`], for either variable being present but unusable.
pub fn from_env(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<OtlpConfig>, ConfigError> {
    let endpoint = lookup(ENDPOINT_VAR)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    match (endpoint, lookup(SAMPLE_VAR)) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(ConfigError::SampleWithoutEndpoint),
        (Some(endpoint), None) => Ok(Some(OtlpConfig {
            endpoint,
            sample_ratio: 1.0,
        })),
        (Some(endpoint), Some(given)) => {
            let ratio = given
                .trim()
                .parse::<f64>()
                .ok()
                .filter(|ratio| is_valid_ratio(*ratio))
                .ok_or(ConfigError::InvalidSampleRatio { given })?;
            Ok(Some(OtlpConfig {
                endpoint,
                sample_ratio: ratio,
            }))
        }
    }
}

/// Whether `ratio` is a usable sampling fraction: finite, in `0.0..=1.0`.
///
/// Extracted so its truth table is testable in place (QUALITY_GATES.md: a
/// compound condition inside a larger expression cannot have its operator
/// exercised).
fn is_valid_ratio(ratio: f64) -> bool {
    ratio.is_finite() && (0.0..=1.0).contains(&ratio)
}

#[cfg(test)]
mod tests {
    use super::{ConfigError, ENDPOINT_VAR, OtlpConfig, SAMPLE_VAR, from_env, is_valid_ratio};

    /// A lookup over a fixed pair: endpoint value, sample value.
    fn env(
        endpoint: Option<&'static str>,
        sample: Option<&'static str>,
    ) -> impl Fn(&str) -> Option<String> {
        move |name| match name {
            name if name == ENDPOINT_VAR => endpoint.map(str::to_owned),
            name if name == SAMPLE_VAR => sample.map(str::to_owned),
            _ => None,
        }
    }

    #[test]
    fn nothing_set_means_off() {
        assert_eq!(from_env(env(None, None)), Ok(None));
    }

    #[test]
    fn an_empty_or_blank_endpoint_is_off_too() {
        // `HORDE_OTLP_ENDPOINT= horde-server` is how a shell turns a variable
        // off for one invocation; treating it as an endpoint would refuse it.
        assert_eq!(from_env(env(Some(""), None)), Ok(None));
        assert_eq!(from_env(env(Some("   "), None)), Ok(None));
    }

    #[test]
    fn an_endpoint_alone_exports_everything() {
        assert_eq!(
            from_env(env(Some("http://127.0.0.1:4317"), None)),
            Ok(Some(OtlpConfig {
                endpoint: "http://127.0.0.1:4317".to_owned(),
                sample_ratio: 1.0,
            }))
        );
    }

    #[test]
    fn the_endpoint_is_trimmed() {
        let read = from_env(env(Some("  http://collector:4317 "), None));
        assert_eq!(
            read,
            Ok(Some(OtlpConfig {
                endpoint: "http://collector:4317".to_owned(),
                sample_ratio: 1.0,
            }))
        );
    }

    #[test]
    fn a_ratio_with_an_endpoint_is_used() {
        assert_eq!(
            from_env(env(Some("http://c:4317"), Some("0.25"))),
            Ok(Some(OtlpConfig {
                endpoint: "http://c:4317".to_owned(),
                sample_ratio: 0.25,
            }))
        );
        // Both boundary values are legitimate: 0.0 is "spans off, pipeline
        // warm", 1.0 is the default written out.
        for boundary in ["0.0", "1.0", " 0.5 "] {
            assert!(
                from_env(env(Some("http://c:4317"), Some(boundary))).is_ok(),
                "{boundary:?} must be accepted"
            );
        }
    }

    #[test]
    fn a_ratio_without_an_endpoint_is_refused() {
        assert_eq!(
            from_env(env(None, Some("0.5"))),
            Err(ConfigError::SampleWithoutEndpoint)
        );
        // An emptied endpoint does not license the ratio either.
        assert_eq!(
            from_env(env(Some(""), Some("0.5"))),
            Err(ConfigError::SampleWithoutEndpoint)
        );
    }

    #[test]
    fn an_unusable_ratio_is_refused_with_what_it_held() {
        for given in ["fast", "-0.1", "1.01", "NaN", "inf", ""] {
            assert_eq!(
                from_env(env(Some("http://c:4317"), Some(given))),
                Err(ConfigError::InvalidSampleRatio {
                    given: given.to_owned()
                }),
                "{given:?} must be refused"
            );
        }
    }

    #[test]
    fn the_ratio_truth_table() {
        assert!(is_valid_ratio(0.0));
        assert!(is_valid_ratio(0.5));
        assert!(is_valid_ratio(1.0));
        assert!(!is_valid_ratio(-0.000_1));
        assert!(!is_valid_ratio(1.000_1));
        assert!(!is_valid_ratio(f64::NAN));
        assert!(!is_valid_ratio(f64::INFINITY));
        assert!(!is_valid_ratio(f64::NEG_INFINITY));
    }

    #[test]
    fn the_refusals_name_the_variables_they_refuse() {
        // The message is the operator interface: it has to say which variable
        // to go fix.
        let refusal = ConfigError::SampleWithoutEndpoint.to_string();
        assert!(refusal.contains(ENDPOINT_VAR), "{refusal}");
        assert!(refusal.contains(SAMPLE_VAR), "{refusal}");
        let refusal = ConfigError::InvalidSampleRatio {
            given: "fast".to_owned(),
        }
        .to_string();
        assert!(refusal.contains(SAMPLE_VAR), "{refusal}");
        assert!(refusal.contains("fast"), "{refusal}");
    }
}
