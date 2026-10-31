//! The secured harness, observed through the server it stands up.
//!
//! Same rule as `harness.rs`: cargo-mutants runs a mutated package's own
//! tests, so every helper in `secure.rs` is exercised here through its
//! effect — an identity that opens a listener, an endpoint that reaches the
//! right spine — rather than through returned values alone. The refusal
//! outcomes (wrong CA, spent codes, revocation) belong to `horde-server`'s
//! enrollment suite; this file holds the harness itself to its promises.

mod common;

use horde_identity::PrincipalRole;
use horde_server_testkit::secure::{hello, serve_secured, serve_secured_with_blob_store, within};

type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The blob-serving variant keeps every promise of [`serve_secured`] and
/// adds a live store on the caller's directory: an enrolled principal moves
/// bytes over mTLS and they land on the disk the caller named. Asserted by
/// the on-disk effect, which is what a mutant of the wiring cannot fake.
#[tokio::test]
async fn the_secured_blob_harness_moves_bytes_for_an_enrolled_principal() -> Setup<()> {
    let blob_root = tempfile::tempdir()?;
    let harness = serve_secured_with_blob_store(blob_root.path()).await?;
    let identity = within(
        "the worker join",
        harness.enroll(PrincipalRole::Worker, "blob-box"),
    )
    .await??;

    let channel = harness.main_endpoint_as(&identity)?.connect().await?;
    let mut blobs = horde_proto::v1::blob_service_client::BlobServiceClient::new(channel);

    within(
        "the blob round trip",
        common::assert_blob_lands_under_root(&mut blobs, blob_root.path(), b"secured testkit blob"),
    )
    .await??;
    Ok(())
}

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
            last_local_journal_sequence: None,
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

/// `tls_trusting_ca` must actually trust the CA it was given: a defaulted
/// config trusts nothing, so the enrollment listener's certificate would be
/// refused before any RPC. The garbage code proves the channel worked — the
/// refusal comes from the server, not the transport.
#[tokio::test]
async fn the_ca_trusting_config_reaches_the_enrollment_listener() -> Setup<()> {
    use horde_proto::v1::enrollment_service_client::EnrollmentServiceClient;

    let harness = serve_secured().await?;
    let channel = tonic::transport::Endpoint::from_shared(harness.enrollment_endpoint())?
        .tls_config(horde_server_testkit::secure::tls_trusting_ca(
            harness.authority.ca_pem(),
        ))?
        .connect()
        .await?;

    let refused = within(
        "the server's own refusal",
        EnrollmentServiceClient::new(channel).join(horde_proto::v1::JoinRequest {
            enrollment_code: "not-a-code".to_owned(),
            csr_pem: String::new(),
            hello: Some(hello(PrincipalRole::Worker, "probe")),
            name: String::new(),
        }),
    )
    .await?
    .expect_err("a garbage code is refused by the server");
    assert_eq!(
        refused.code(),
        tonic::Code::Unauthenticated,
        "the refusal must be the server's verdict, proving TLS carried the call"
    );
    Ok(())
}

#[tokio::test]
async fn the_harness_debug_names_its_listeners() -> Setup<()> {
    let harness = serve_secured().await?;
    let debugged = format!("{harness:?}");
    assert!(debugged.contains("SecureHarness"), "{debugged}");
    assert!(debugged.contains("main"), "{debugged}");
    Ok(())
}

#[test]
fn the_hello_helper_speaks_the_role_it_was_asked_for() {
    let worker = hello(PrincipalRole::Worker, "mac-studio");
    assert_eq!(worker.role, horde_proto::v1::Role::Worker as i32);
    assert_eq!(worker.display_name, "mac-studio");
    assert!(
        !worker.semver.is_empty(),
        "the helper must carry a version: the server's version gate refuses \
         a Hello without one, and a versionless helper would fail every \
         registration this harness makes"
    );

    let client = hello(PrincipalRole::Client, "laptop");
    assert_eq!(client.role, horde_proto::v1::Role::Client as i32);
}
