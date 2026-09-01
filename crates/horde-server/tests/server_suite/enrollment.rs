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

use crate::common::Setup;
use horde_identity::{JoinCode, Principal, PrincipalRole};
use horde_proto::v1::enrollment_service_client::EnrollmentServiceClient;
use horde_proto::v1::{
    JoinRequest, MintEnrollmentCodeRequest, RegisterWorkerRequest, RevokeEnrollmentRequest, Role,
    WorkerCapabilities, WorkerId, control_plane_service_client::ControlPlaneServiceClient,
    revoke_enrollment_request,
};
use horde_server::enrollment::ENROLLMENT_STREAM;
use horde_server::service::enrollment::wall_clock_ms;
use horde_server_testkit::secure::{SecureHarness, hello, serve_secured, tls_trusting_ca, within};

/// An enrollment-listener client that trusts the harness CA (the state every
/// peer is in *after* one join, or by carrying the CA out of band).
async fn enrollment_client(
    harness: &SecureHarness,
) -> Setup<EnrollmentServiceClient<tonic::transport::Channel>> {
    let channel = tonic::transport::Endpoint::from_shared(harness.enrollment_endpoint())?
        .tls_config(tls_trusting_ca(harness.authority.ca_pem()))?
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

/// An enrolled `laptop` client's control-plane connection — the two-line
/// preamble half the client-side tests open with.
async fn enrolled_laptop(harness: &SecureHarness) -> Setup<horde_cli::Client> {
    let identity = harness.enroll(PrincipalRole::Client, "laptop").await?;
    harness.client(&identity).await
}

/// The revocation arrangement both revocation tests share: an enrolled
/// client (the revoker), an enrolled and *registered* worker `mac-studio`
/// (the revokee) with its identity, and the worker's live service client.
async fn revocable_worker(
    harness: &SecureHarness,
) -> Setup<(
    horde_identity::StoredIdentity,
    horde_identity::StoredIdentity,
    horde_proto::v1::worker_service_client::WorkerServiceClient<tonic::transport::Channel>,
)> {
    let client_identity = harness.enroll(PrincipalRole::Client, "laptop").await?;
    let worker_identity = harness.enroll(PrincipalRole::Worker, "mac-studio").await?;
    let mut worker = harness.worker_client(&worker_identity).await?;
    within(
        "the registration",
        worker.register_worker(register_request("mac-studio")),
    )
    .await??;
    Ok((client_identity, worker_identity, worker))
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

/// The two ports ADR-0007's token packaging rides, over the wire: the mint
/// answer names the enrollment listener (so the minting client can print one
/// self-contained token), the join answer names the control plane (so the
/// joiner can store its endpoint) — both the real bound ports, not zeros.
#[tokio::test]
async fn the_answers_teach_the_listener_ports_the_token_packaging_needs() -> Setup<()> {
    let harness = serve_secured().await?;
    let port_of = |endpoint: String| -> Setup<u32> {
        Ok(endpoint
            .rsplit(':')
            .next()
            .ok_or("an endpoint has a port")?
            .parse()?)
    };

    let client_identity = within(
        "the client join",
        harness.enroll(PrincipalRole::Client, "laptop"),
    )
    .await??;
    let mut client = harness.client(&client_identity).await?;
    let minted = within(
        "the mint",
        client.mint_enrollment_code(Role::Worker, "gpu-box", None),
    )
    .await??;
    assert_eq!(
        minted.enrollment_port,
        port_of(harness.enrollment_endpoint())?,
        "the mint answer names the enrollment listener"
    );

    let code = JoinCode::parse(&minted.code).ok_or("the minted code parses")?;
    let enrolled = within(
        "the worker join",
        horde_identity::join(
            &harness.enrollment_endpoint(),
            &code,
            hello(PrincipalRole::Worker, "gpu-box"),
        ),
    )
    .await??;
    assert_eq!(
        enrolled.control_plane_port.map(u32::from),
        Some(port_of(harness.main_endpoint())?),
        "the join answer names the control plane"
    );
    Ok(())
}

#[tokio::test]
async fn a_peer_without_a_certificate_is_refused_by_the_main_listener() -> Setup<()> {
    let harness = serve_secured().await?;

    // TLS, trusting the right CA, but presenting no client certificate.
    let bare = tonic::transport::Endpoint::from_shared(harness.main_endpoint())?
        .tls_config(tls_trusting_ca(harness.authority.ca_pem()))?;

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
    // The worker is in, on an open connection.
    let (client_identity, worker_identity, mut worker) = revocable_worker(&harness).await?;

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

/// Revocation reaches a stream already in flight (wave-7 fix a4): a revoked
/// worker's open `StreamCommands` is severed with `PermissionDenied` rather
/// than surviving until the worker deigns to reconnect. Before the fix, the
/// per-RPC interceptor was the only check, and a worker's whole life is two
/// long-lived streams it never re-opens.
#[tokio::test]
async fn revocation_severs_a_live_command_stream() -> Setup<()> {
    let harness = serve_secured().await?;
    // The worker registers and opens its command stream — the quiet state a
    // deployed worker sits in for hours.
    let (client_identity, _worker_identity, mut worker) = revocable_worker(&harness).await?;
    let mut commands = within(
        "the command stream opening",
        worker.stream_commands(horde_proto::v1::StreamCommandsRequest {
            worker_id: Some(WorkerId::new("mac-studio")),
            resume_from_command_sequence: 1,
        }),
    )
    .await??
    .into_inner();

    // An enrolled client revokes the worker while the stream is open.
    let channel = harness
        .main_endpoint_as(&client_identity)?
        .connect()
        .await?;
    within(
        "the revocation RPC",
        ControlPlaneServiceClient::new(channel).revoke_enrollment(RevokeEnrollmentRequest {
            principal: Some(revoke_enrollment_request::Principal::WorkerId(
                WorkerId::new("mac-studio"),
            )),
            reason: "machine compromised".to_owned(),
        }),
    )
    .await??;

    // The open stream ends with the revocation refusal — no command was ever
    // sent on it, so nothing but the severing can end it.
    let severed = within("the live stream to be severed", commands.message())
        .await?
        .expect_err("a revoked worker's stream must end in refusal, not silence");
    assert_eq!(severed.code(), tonic::Code::PermissionDenied);
    assert!(
        severed.message().contains("revoked"),
        "the refusal says why: {severed}"
    );
    Ok(())
}

/// Attribution over the real mTLS door (fix a13): an enrolled client's
/// interrupt is journaled with that client's own name as the actor. Multi-
/// attach is why the field exists, and a `requested_by` that vanished on the
/// way to the log would leave every event in a shared session anonymous.
#[tokio::test]
async fn a_clients_interrupt_is_attributed_to_that_client() -> Setup<()> {
    let harness = serve_secured().await?;
    let mut client = enrolled_laptop(&harness).await?;

    let (summary, root) = within("the session", client.create_session("attributed", "")).await??;
    let session_id = summary
        .session_id
        .as_ref()
        .map(ToString::to_string)
        .ok_or("a session id")?;
    within("the interrupt", client.interrupt(&root.to_string(), "stop")).await??;

    let mut actors = Vec::new();
    within(
        "the replay",
        client.sync(
            &horde_cli::client::SyncOptions::new(session_id),
            |envelope| {
                if let Some(horde_proto::v1::event::Kind::TurnInterrupted(interrupted)) = envelope
                    .event
                    .as_ref()
                    .and_then(|event| event.kind.as_ref())
                {
                    actors.push(interrupted.requested_by.clone());
                }
            },
            |_| {},
        ),
    )
    .await??;

    assert_eq!(
        actors,
        vec![Some(horde_proto::v1::Actor {
            actor: Some(horde_proto::v1::actor::Actor::ClientId(
                horde_proto::v1::ClientId::new("laptop")
            )),
        })],
        "the interrupt is the laptop's, and the log says so"
    );
    Ok(())
}

/// The bootstrap question, answered against the live authority: `false` on a
/// fresh server, `true` the moment a client holds a certificate — the
/// distinction that opens and closes the printed-code door.
#[tokio::test]
async fn a_client_enrollment_closes_the_bootstrap_door() -> Setup<()> {
    let harness = serve_secured().await?;
    assert!(
        !harness.authority.any_client_enrolled().await?,
        "a fresh server has no client, so bootstrap must stay open"
    );

    harness.enroll(PrincipalRole::Worker, "mac-studio").await?;
    assert!(
        !harness.authority.any_client_enrolled().await?,
        "workers do not close the client bootstrap"
    );

    harness.enroll(PrincipalRole::Client, "laptop").await?;
    assert!(
        harness.authority.any_client_enrolled().await?,
        "an enrolled client closes it"
    );
    Ok(())
}

/// Dropping a listener stops it. The handle's whole contract is that its
/// lifetime is the listener's — a drop that leaked the accept task would
/// keep serving a plane its owner believes is down.
#[tokio::test]
async fn dropping_the_harness_stops_its_listeners() -> Setup<()> {
    let harness = serve_secured().await?;
    let identity = harness.enroll(PrincipalRole::Client, "laptop").await?;
    let endpoint = harness.main_endpoint_as(&identity)?;

    // Live while held…
    let mut client = horde_cli::Client::with_channel(endpoint.connect().await?);
    within("the pre-drop handshake", client.hello("0.1.0")).await??;

    // …dead when dropped: a fresh dial must fail outright.
    drop(harness);
    let refused = within("the post-drop dial to fail", endpoint.connect()).await?;
    assert!(
        refused.is_err(),
        "a dropped listener must stop accepting connections"
    );
    Ok(())
}

#[tokio::test]
async fn the_enrollment_stream_is_not_a_session() -> Setup<()> {
    let harness = serve_secured().await?;
    let mut client = enrolled_laptop(&harness).await?;

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
    // it to the channel. What it may not do is *mint or revoke*: those are
    // operator verbs (wave-7 fix a3), and before the role check a worker
    // could mint codes for any role and revoke every client.
    let harness = serve_secured().await?;
    let worker_identity = harness.enroll(PrincipalRole::Worker, "mac-studio").await?;
    let client_identity = harness.enroll(PrincipalRole::Client, "laptop").await?;

    let channel = harness
        .main_endpoint_as(&worker_identity)?
        .connect()
        .await?;
    let mut control = ControlPlaneServiceClient::new(channel);
    let refused = within(
        "the mint RPC under a worker identity",
        control.mint_enrollment_code(MintEnrollmentCodeRequest {
            role: Role::Worker as i32,
            name: "gpu-box".to_owned(),
            ttl: None,
        }),
    )
    .await?
    .expect_err("a worker identity must not mint");
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);
    assert!(
        refused.message().contains("worker:mac-studio"),
        "the refusal names the caller: {refused}"
    );

    let refused = within(
        "the revoke RPC under a worker identity",
        control.revoke_enrollment(RevokeEnrollmentRequest {
            principal: Some(revoke_enrollment_request::Principal::ClientId(
                horde_proto::v1::ClientId::new("laptop"),
            )),
            reason: "a worker trying to revoke the operator".to_owned(),
        }),
    )
    .await?
    .expect_err("a worker identity must not revoke");
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);

    // And the client the worker aimed at still works — nothing was revoked.
    let mut client = harness.client(&client_identity).await?;
    within("the client's post-attempt handshake", client.hello("0.1.0")).await??;
    Ok(())
}
