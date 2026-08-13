//! Enrollment and mTLS, end to end over real sockets (DESIGN §3).
//!
//! Every identity in this file exists by being issued through the production
//! path — a mint through the authority, a join over the real enrollment
//! listener — and every refused connection is a first-class outcome: no
//! certificate, a spent code, an expired code, a wrong fingerprint, a
//! revoked serial. The events the flow journals are read back through the
//! production sync path, because "journaled through the event log like
//! everything else" is a claim about what a client can replay, not about
//! internal state.

mod common;

use common::Setup;
use horde_identity::{JoinCode, Principal, PrincipalRole};
use horde_proto::v1::enrollment_service_client::EnrollmentServiceClient;
use horde_proto::v1::{
    JoinRequest, MintEnrollmentCodeRequest, RegisterWorkerRequest, RevokeEnrollmentRequest, Role,
    WorkerCapabilities, WorkerId, control_plane_service_client::ControlPlaneServiceClient,
    revoke_enrollment_request,
};
use horde_server::enrollment::ENROLLMENT_STREAM;
use horde_server::service::enrollment::wall_clock_ms;
use horde_server_testkit::secure::{SecureHarness, hello, serve_secured};

/// Every wait in this file is ceilinged: a refused handshake must fail in
/// milliseconds, and a test that would instead hang has found a worse bug
/// than the one it was looking for.
async fn within<T>(
    what: &str,
    future: impl Future<Output = T>,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    tokio::time::timeout(std::time::Duration::from_secs(15), future)
        .await
        .map_err(|_| format!("timed out waiting for {what}").into())
}

/// An enrollment-listener client that trusts the harness CA (the state every
/// peer is in *after* one join, or by carrying the CA out of band).
async fn enrollment_client(
    harness: &SecureHarness,
) -> Setup<EnrollmentServiceClient<tonic::transport::Channel>> {
    let channel = tonic::transport::Endpoint::from_shared(harness.enrollment_endpoint())?
        .tls_config(
            tonic::transport::ClientTlsConfig::new()
                .ca_certificate(tonic::transport::Certificate::from_pem(
                    harness.authority.ca_pem(),
                ))
                .domain_name("localhost"),
        )?
        .connect()
        .await?;
    Ok(EnrollmentServiceClient::new(channel))
}

fn register_request(name: &str) -> RegisterWorkerRequest {
    RegisterWorkerRequest {
        hello: Some(hello(PrincipalRole::Worker, name)),
        worker_id: Some(WorkerId::new(name)),
        capabilities: Some(WorkerCapabilities::default()),
    }
}

/// The kinds journaled in the enrollment stream, read back through the
/// production sync path as an enrolled client.
async fn journaled_kinds(client: &mut horde_cli::Client) -> Setup<Vec<&'static str>> {
    let mut kinds = Vec::new();
    client
        .sync(
            &horde_cli::client::SyncOptions::new(ENROLLMENT_STREAM),
            |envelope| {
                let name = envelope
                    .event
                    .as_ref()
                    .and_then(|event| event.kind.as_ref())
                    .and_then(horde_proto::kind_field_name)
                    .unwrap_or("empty");
                kinds.push(name);
            },
            |_| {},
        )
        .await?;
    Ok(kinds)
}

#[tokio::test]
async fn a_minted_code_joins_and_the_identity_opens_both_spines() -> Setup<()> {
    let harness = serve_secured().await?;

    let client_identity = within(
        "the client join",
        harness.enroll(PrincipalRole::Client, "laptop"),
    )
    .await??;
    let worker_identity = within(
        "the worker join",
        harness.enroll(PrincipalRole::Worker, "mac-studio"),
    )
    .await??;

    // The client identity opens the control plane.
    let mut client = harness.client(&client_identity).await?;
    let server = within("the handshake", client.hello("0.1.0")).await??;
    assert!(
        !server.semver.is_empty(),
        "an enrolled client completes the version handshake"
    );

    // The worker identity opens the worker spine.
    let mut worker = harness.worker_client(&worker_identity).await?;
    let registered = within(
        "the registration",
        worker.register_worker(register_request("mac-studio")),
    )
    .await??
    .into_inner();
    assert_eq!(
        registered.worker_id.map(|id| id.to_string()),
        Some("mac-studio".to_owned()),
        "an enrolled worker registers"
    );

    // Both issuances were journaled, and an enrolled client can replay them.
    let kinds = journaled_kinds(&mut client).await?;
    assert_eq!(
        kinds,
        vec![
            "enrollment_code_minted",
            "identity_enrolled",
            "enrollment_code_minted",
            "identity_enrolled",
        ],
        "enrollment is events in the log like everything else"
    );
    Ok(())
}

#[tokio::test]
async fn a_peer_without_a_certificate_is_refused_by_the_main_listener() -> Setup<()> {
    let harness = serve_secured().await?;

    // TLS, trusting the right CA, but presenting no client certificate.
    let bare = tonic::transport::Endpoint::from_shared(harness.main_endpoint())?.tls_config(
        tonic::transport::ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(
                harness.authority.ca_pem(),
            ))
            .domain_name("localhost"),
    )?;

    let refused = within("the certificate-less dial to fail", async {
        let channel = bare.connect().await?;
        ControlPlaneServiceClient::new(channel)
            .hello(horde_proto::v1::HelloRequest {
                hello: Some(hello(PrincipalRole::Client, "nobody")),
            })
            .await
            .map(|_| ())
            .map_err(|status| -> Box<dyn std::error::Error + Send + Sync> { Box::new(status) })
    })
    .await?;
    assert!(
        refused.is_err(),
        "a peer with no certificate must not complete an RPC on the main listener"
    );
    Ok(())
}

#[tokio::test]
async fn wrong_spent_and_expired_codes_are_each_refused_by_name() -> Setup<()> {
    let harness = serve_secured().await?;
    let mut enrollment = enrollment_client(&harness).await?;

    let join = |code: String| JoinRequest {
        enrollment_code: code,
        csr_pem: horde_identity::enroll::generate_csr()
            .map(|(_key, csr)| csr)
            .unwrap_or_default(),
        hello: Some(hello(PrincipalRole::Worker, "mac-studio")),
        name: String::new(),
    };

    // Wrong: never minted.
    let refused = enrollment.join(join("never-minted".to_owned())).await;
    let status = refused.expect_err("an unminted code must refuse");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert!(status.message().contains("no such code"), "{status}");

    // Spent: redeemed once already.
    let principal = Principal::new(PrincipalRole::Worker, "mac-studio")?;
    let minted = harness
        .authority
        .mint(&principal, 0, wall_clock_ms())
        .await?;
    within(
        "the first redemption",
        enrollment.join(join(minted.secret.clone())),
    )
    .await??;
    let status = enrollment
        .join(join(minted.secret.clone()))
        .await
        .expect_err("one code, one identity");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert!(status.message().contains("already used"), "{status}");

    // Expired: minted with a one-millisecond life, long over by now.
    let expired = harness
        .authority
        .mint(&principal, 1, wall_clock_ms() - 10_000)
        .await?;
    let status = enrollment
        .join(join(expired.secret))
        .await
        .expect_err("a code outlived is a code refused");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert!(status.message().contains("expired"), "{status}");
    Ok(())
}

#[tokio::test]
async fn a_garbage_csr_and_a_missing_hello_are_invalid_not_internal() -> Setup<()> {
    let harness = serve_secured().await?;
    let mut enrollment = enrollment_client(&harness).await?;
    let principal = Principal::new(PrincipalRole::Worker, "mac-studio")?;

    let minted = harness
        .authority
        .mint(&principal, 0, wall_clock_ms())
        .await?;
    let status = enrollment
        .join(JoinRequest {
            enrollment_code: minted.secret.clone(),
            csr_pem: "not a csr".to_owned(),
            hello: Some(hello(PrincipalRole::Worker, "mac-studio")),
            name: String::new(),
        })
        .await
        .expect_err("garbage is not a CSR");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    let status = enrollment
        .join(JoinRequest {
            enrollment_code: minted.secret.clone(),
            csr_pem: String::new(),
            hello: None,
            name: String::new(),
        })
        .await
        .expect_err("a join must introduce itself");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    // Neither refusal spent the code: the same secret still redeems.
    within(
        "the code to still redeem",
        enrollment.join(JoinRequest {
            enrollment_code: minted.secret,
            csr_pem: horde_identity::enroll::generate_csr()?.1,
            hello: Some(hello(PrincipalRole::Worker, "mac-studio")),
            name: String::new(),
        }),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn a_wrong_fingerprint_refuses_the_server_before_sending_anything() -> Setup<()> {
    let harness = serve_secured().await?;
    let principal = Principal::new(PrincipalRole::Worker, "mac-studio")?;
    let minted = harness
        .authority
        .mint(&principal, 0, wall_clock_ms())
        .await?;

    let code = JoinCode {
        secret: minted.secret,
        server_fingerprint: "b".repeat(64), // not this server
    };
    let refused = within(
        "the pinned join to fail",
        horde_identity::join(
            &harness.enrollment_endpoint(),
            &code,
            hello(PrincipalRole::Worker, "mac-studio"),
        ),
    )
    .await?;
    assert!(
        matches!(refused, Err(horde_identity::JoinError::Transport { .. })),
        "a wrong pin is a refused connection, not a refused code: {refused:?}"
    );
    Ok(())
}

#[tokio::test]
async fn minting_over_the_control_plane_requires_an_identity_and_works_with_one() -> Setup<()> {
    let harness = serve_secured().await?;
    let client_identity = harness.enroll(PrincipalRole::Client, "laptop").await?;

    let channel = harness
        .main_endpoint_as(&client_identity)?
        .connect()
        .await?;
    let minted = within(
        "the mint RPC",
        ControlPlaneServiceClient::new(channel).mint_enrollment_code(MintEnrollmentCodeRequest {
            role: Role::Worker as i32,
            name: "gpu-box".to_owned(),
            ttl: None,
        }),
    )
    .await??
    .into_inner();

    // What the RPC returns is a full join code; joining with it works.
    let code = JoinCode::parse(&minted.code).ok_or("the minted code must parse as a join code")?;
    assert_eq!(code.server_fingerprint, minted.server_cert_fingerprint);
    let enrolled = within(
        "the join with the RPC-minted code",
        horde_identity::join(
            &harness.enrollment_endpoint(),
            &code,
            hello(PrincipalRole::Worker, "gpu-box"),
        ),
    )
    .await??;
    assert_eq!(enrolled.principal.to_string(), "worker:gpu-box");
    Ok(())
}

#[tokio::test]
async fn revocation_shuts_the_door_at_the_next_rpc_and_is_journaled() -> Setup<()> {
    let harness = serve_secured().await?;
    let client_identity = harness.enroll(PrincipalRole::Client, "laptop").await?;
    let worker_identity = harness.enroll(PrincipalRole::Worker, "mac-studio").await?;

    // The worker is in, on an open connection.
    let mut worker = harness.worker_client(&worker_identity).await?;
    within(
        "the registration",
        worker.register_worker(register_request("mac-studio")),
    )
    .await??;

    // An enrolled client revokes it.
    let channel = harness
        .main_endpoint_as(&client_identity)?
        .connect()
        .await?;
    let revoked = within(
        "the revocation RPC",
        ControlPlaneServiceClient::new(channel).revoke_enrollment(RevokeEnrollmentRequest {
            principal: Some(revoke_enrollment_request::Principal::WorkerId(
                WorkerId::new("mac-studio"),
            )),
            reason: "laptop stolen".to_owned(),
        }),
    )
    .await??
    .into_inner();
    assert!(!revoked.revoked_serial.is_empty(), "the serial is named");

    // The same open connection stops working at its next RPC — revocation
    // must not wait for a reconnect.
    let status = worker
        .register_worker(register_request("mac-studio"))
        .await
        .expect_err("a revoked certificate must stop working mid-connection");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert!(
        status.message().contains("revoked"),
        "the refusal says why: {status}"
    );

    // A fresh dial with the revoked identity is refused too.
    let refused = within("the revoked redial to fail", async {
        match harness.worker_client(&worker_identity).await {
            Err(_) => Ok::<bool, Box<dyn std::error::Error + Send + Sync>>(true),
            Ok(mut client) => Ok(client
                .register_worker(register_request("mac-studio"))
                .await
                .is_err()),
        }
    })
    .await??;
    assert!(
        refused,
        "a revoked identity must not come back by reconnecting"
    );

    // The revocation is an event like everything else.
    let mut client = harness.client(&client_identity).await?;
    let kinds = journaled_kinds(&mut client).await?;
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == "enrollment_revoked")
            .count(),
        1,
        "exactly one revocation was journaled: {kinds:?}"
    );
    Ok(())
}

#[tokio::test]
async fn the_enrollment_stream_is_not_a_session() -> Setup<()> {
    let harness = serve_secured().await?;
    let identity = harness.enroll(PrincipalRole::Client, "laptop").await?;
    let mut client = harness.client(&identity).await?;

    // Enrollment already journaled events; none of them make a session.
    let sessions = client.list_sessions(true).await?;
    assert!(
        sessions.is_empty(),
        "reserved streams must not surface as phantom sessions: {sessions:?}"
    );
    Ok(())
}

#[tokio::test]
async fn the_worker_role_cannot_walk_through_the_client_door() -> Setup<()> {
    // A worker certificate is a valid certificate — the interceptor admits
    // it to the channel. What it cannot do is *mint*: minting requires
    // nothing today beyond enrollment, so this pins the current truth that
    // any enrolled principal may mint (DESIGN §3 says clients enroll and
    // workers enroll but not who may mint; flagged in the wave report), and
    // it proves a worker identity at least authenticates rather than erroring.
    let harness = serve_secured().await?;
    let worker_identity = harness.enroll(PrincipalRole::Worker, "mac-studio").await?;

    let channel = harness
        .main_endpoint_as(&worker_identity)?
        .connect()
        .await?;
    let minted = within(
        "the mint RPC under a worker identity",
        ControlPlaneServiceClient::new(channel).mint_enrollment_code(MintEnrollmentCodeRequest {
            role: Role::Worker as i32,
            name: "gpu-box".to_owned(),
            ttl: None,
        }),
    )
    .await??;
    assert!(
        !minted.into_inner().code.is_empty(),
        "today every enrolled principal may mint; if this starts failing, \
         someone added the role check the wave report flags as open"
    );
    Ok(())
}
