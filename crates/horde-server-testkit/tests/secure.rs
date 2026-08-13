//! The secured harness, observed through the server it stands up.
//!
//! Same rule as `harness.rs`: cargo-mutants runs a mutated package's own
//! tests, so every helper in `secure.rs` is exercised here through its
//! effect — an identity that opens a listener, an endpoint that reaches the
//! right spine — rather than through returned values alone. The refusal
//! outcomes (wrong CA, spent codes, revocation) belong to `horde-server`'s
//! enrollment suite; this file holds the harness itself to its promises.

use horde_identity::PrincipalRole;
use horde_server_testkit::secure::{hello, serve_secured, within};

type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn the_harness_issues_identities_that_open_the_spines_they_are_for() -> Setup<()> {
    let harness = serve_secured().await?;

    assert!(
        harness.main_endpoint().starts_with("https://localhost:"),
        "the endpoint names the host the credential was issued for: {}",
        harness.main_endpoint()
    );
    assert_ne!(
        harness.main_endpoint(),
        harness.enrollment_endpoint(),
        "two listeners, two trust tiers, two ports"
    );

    // The production issuance path, exercised by the harness's own helper.
    let client_identity = within(
        "the client join",
        harness.enroll(PrincipalRole::Client, "laptop"),
    )
    .await??;
    assert!(
        client_identity
            .certificate_pem
            .contains("BEGIN CERTIFICATE"),
        "the identity carries the signed certificate"
    );
    assert_eq!(
        client_identity.ca_pem,
        harness.authority.ca_pem(),
        "the identity trusts exactly the harness CA"
    );

    // The identity opens the control plane: the helper's whole purpose.
    let mut client = harness.client(&client_identity).await?;
    let server = within("the handshake", client.hello("0.1.0")).await??;
    assert!(!server.semver.is_empty());

    // And the worker helper reaches the worker spine as an enrolled worker.
    let worker_identity = within(
        "the worker join",
        harness.enroll(PrincipalRole::Worker, "mac-studio"),
    )
    .await??;
    let mut worker = harness.worker_client(&worker_identity).await?;
    let registered = within(
        "the registration",
        worker.register_worker(horde_proto::v1::RegisterWorkerRequest {
            hello: Some(hello(PrincipalRole::Worker, "mac-studio")),
            worker_id: Some(horde_proto::v1::WorkerId::new("mac-studio")),
            capabilities: Some(horde_proto::v1::WorkerCapabilities::default()),
        }),
    )
    .await??
    .into_inner();
    assert_eq!(
        registered.worker_id.map(|id| id.to_string()),
        Some("mac-studio".to_owned())
    );
    Ok(())
}

#[test]
fn the_hello_helper_speaks_the_role_it_was_asked_for() {
    let worker = hello(PrincipalRole::Worker, "mac-studio");
    assert_eq!(worker.role, horde_proto::v1::Role::Worker as i32);
    assert_eq!(worker.display_name, "mac-studio");
    assert!(
        !worker.semver.is_empty(),
        "a Hello without a version is refused"
    );

    let client = hello(PrincipalRole::Client, "laptop");
    assert_eq!(client.role, horde_proto::v1::Role::Client as i32);
}
