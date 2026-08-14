//! The `horde` binary's telemetry wiring, run for real.
//!
//! Like horde-server's `tests/binary.rs`: what the bin decides — refusals
//! and exit codes — is only observable from outside the process, so these
//! run the binary cargo just built.

#[cfg(test)]
mod harness {
    pub(crate) type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

    /// Runs `horde hello` against a dead server with the given OTLP
    /// environment, capturing everything. `hello` because it is the verb
    /// with no verb-side state: whatever happens is startup wiring plus one
    /// refused connection.
    pub(crate) async fn hello_with_env(
        environment: &[(&str, &str)],
    ) -> Setup<std::process::Output> {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_horde"));
        command.args(["hello", "--endpoint", "http://127.0.0.1:1"]);
        // The variables under test never leak in from the harness's shell.
        command.env_remove("HORDE_OTLP_ENDPOINT");
        command.env_remove("HORDE_OTLP_SAMPLE");
        for (name, value) in environment {
            command.env(name, value);
        }
        Ok(command.output().await?)
    }
}

/// OTLP misconfiguration refuses before any verb runs, naming the variable —
/// the same contract the server binary holds.
#[tokio::test]
async fn a_sample_ratio_without_an_endpoint_refuses_startup() {
    let output = harness::hello_with_env(&[("HORDE_OTLP_SAMPLE", "0.5")])
        .await
        .expect("the binary must run");

    assert!(!output.status.success(), "misconfiguration must refuse");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("HORDE_OTLP_SAMPLE"),
        "the refusal must name the variable to fix: {stderr}"
    );
    assert!(
        !stderr.contains("error:"),
        "the refusal must come before the verb ever runs: {stderr}"
    );
}

/// A dead collector changes nothing a user can see: same exit code, same
/// error message as the plain run — telemetry costs spans, never behavior.
#[tokio::test]
async fn a_dead_collector_leaves_the_verb_outcome_alone() {
    let plain = harness::hello_with_env(&[])
        .await
        .expect("the binary must run");
    let exported = harness::hello_with_env(&[("HORDE_OTLP_ENDPOINT", "http://127.0.0.1:1")])
        .await
        .expect("the binary must run");

    assert_eq!(
        plain.status.code(),
        exported.status.code(),
        "an unreachable collector must not change the exit code"
    );
    let plain_stderr = String::from_utf8_lossy(&plain.stderr);
    let exported_stderr = String::from_utf8_lossy(&exported.stderr);
    assert_eq!(
        plain_stderr, exported_stderr,
        "an unreachable collector must not change what the user is told"
    );
}
