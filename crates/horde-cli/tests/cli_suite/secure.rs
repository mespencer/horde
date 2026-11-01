//! The client over mTLS (DESIGN §3), driven through the same `cli::run`
//! dispatch the binary uses.
//!
//! Effects over exit codes, as in `verbs.rs`: a mint is proven by its code
//! joining, a join by the identity it stored opening the control plane, a
//! revocation by the door actually closing. The refused outcomes — plaintext
//! against the secured listener, a connection under a missing identity — are
//! first-class.

use clap::Parser;
use horde_cli::Format;
use horde_cli::cli::{Cli, run};
use horde_identity::PrincipalRole;
use horde_server_testkit::secure::{serve_secured, within};

type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Runs one argument vector through the real dispatch.
async fn invoke(args: &[&str]) -> Setup<()> {
    let cli = Cli::try_parse_from(std::iter::once("horde").chain(args.iter().copied()))
        .map_err(|error| format!("{args:?}: {error}"))?;
    run(&cli, Format::from_flag(cli.json)).await?;
    Ok(())
}

/// A TLS failure is diagnosed from its source chain (ADR-0007, 2026-10-31
/// amendment): an identity from another server's CA names that cause, not
/// a bare `transport error`. (The other rustls causes are tabled in
/// `horde_identity::diagnosis`'s own tests.)
#[tokio::test]
async fn tls_failures_are_diagnosed_in_plain_words() -> Setup<()> {
    let serving = serve_secured().await?;
    let other = serve_secured().await?;
    let identity_dir = tempfile::tempdir()?;
    let foreign = identity_dir.path().join("foreign");
    other
        .enroll(PrincipalRole::Client, "laptop")
        .await?
        .save(&foreign)?;
    let refused = invoke(&[
        "--identity",
        &foreign.to_string_lossy(),
        "--endpoint",
        &serving.main_endpoint(),
        "hello",
    ])
    .await
    .expect_err("an identity from another CA must not connect");
    let refusal = refused.to_string();
    let cli_error = refused
        .downcast_ref::<horde_cli::ClientError>()
        .ok_or("a client error")?;
    assert!(
        cli_error
            .report()
            .contains("hint: the server's certificate was not issued by the CA"),
        "{refusal}: {}",
        cli_error.report()
    );
    Ok(())
}

/// Re-joining over an existing identity refuses without `--force`
/// (spending no code), and with it backs the old identity up first.
#[tokio::test]
async fn a_rejoin_needs_force_and_keeps_a_backup() -> Setup<()> {
    let harness = serve_secured().await?;
    let root = tempfile::tempdir()?;
    let identity_path = root.path().join("id");
    let principal = horde_identity::Principal::new(PrincipalRole::Client, "laptop")?;
    let mint = || async {
        harness
            .authority
            .mint(
                &principal,
                0,
                horde_server::service::enrollment::wall_clock_ms(),
            )
            .await
            .map(|minted| harness.join_token(&minted).to_string())
    };
    let first = mint().await?;
    invoke(&[
        "--identity",
        &identity_path.to_string_lossy(),
        "enroll",
        "join",
        &first,
    ])
    .await?;

    let second = mint().await?;
    let refused = invoke(&[
        "--identity",
        &identity_path.to_string_lossy(),
        "enroll",
        "join",
        &second,
    ])
    .await
    .expect_err("an existing identity is not replaced silently");
    assert!(refused.to_string().contains("--force"), "{refused}");

    let token_file = root.path().join("token");
    std::fs::write(&token_file, format!("{second}\n"))?;
    invoke(&[
        "--identity",
        &identity_path.to_string_lossy(),
        "enroll",
        "join",
        "--token-file",
        &token_file.to_string_lossy(),
        "--force",
    ])
    .await?;
    let backups: Vec<_> = std::fs::read_dir(root.path())?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("id.backup-")
        })
        .collect();
    assert_eq!(backups.len(), 1, "one backup of the replaced identity");
    assert!(horde_identity::StoredIdentity::exists(&backups[0].path()));
    assert!(horde_identity::StoredIdentity::exists(&identity_path));
    Ok(())
}

#[tokio::test]
async fn a_join_stores_an_identity_that_opens_the_control_plane() -> Setup<()> {
    let harness = serve_secured().await?;
    let identity_dir = tempfile::tempdir()?;
    let identity_path = identity_dir.path().join("identity");

    // A code minted by the authority (the bootstrap shape), joined through
    // the CLI's own verb — one token, done (ADR-0007).
    let principal = horde_identity::Principal::new(PrincipalRole::Client, "laptop")?;
    let minted = harness
        .authority
        .mint(
            &principal,
            0,
            horde_server::service::enrollment::wall_clock_ms(),
        )
        .await?;
    within(
        "the join verb",
        invoke(&[
            "--identity",
            &identity_path.to_string_lossy(),
            "enroll",
            "join",
            &harness.join_token(&minted).to_string(),
        ]),
    )
    .await??;

    assert!(
        horde_identity::StoredIdentity::exists(&identity_path),
        "the join must store a complete identity where --identity points"
    );
    assert_eq!(
        horde_identity::store::load_endpoint(&identity_path),
        Some(harness.main_endpoint()),
        "the join must store the control-plane endpoint beside the credential"
    );

    // The stored identity is real, and the stored endpoint is the server:
    // `hello` over mTLS through the dispatch with no --endpoint at all —
    // ADR-0007's bare-invocation-just-works claim, held in process.
    within(
        "the enrolled handshake",
        invoke(&["--identity", &identity_path.to_string_lossy(), "hello"]),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn the_worker_enroll_verb_mints_a_code_that_actually_joins() -> Setup<()> {
    let harness = serve_secured().await?;
    let client_identity = harness.enroll(PrincipalRole::Client, "laptop").await?;

    // Minting through the typed client (what the verb calls under the hood),
    // so the minted code's *effect* is assertable: it joins a worker.
    let mut client = harness.client(&client_identity).await?;
    let minted = within(
        "the mint",
        client.mint_enrollment_code(horde_proto::v1::Role::Worker, "gpu-box", Some(5)),
    )
    .await??;
    let code = horde_identity::JoinCode::parse(&minted.code)
        .ok_or("the minted code must be a join code")?;

    let enrolled = within(
        "the worker join",
        horde_identity::join(
            &harness.enrollment_endpoint(),
            &code,
            horde_server_testkit::secure::hello(PrincipalRole::Worker, "gpu-box"),
        ),
    )
    .await??;
    assert_eq!(enrolled.principal.to_string(), "worker:gpu-box");

    // And the revoke verb closes the door it names.
    let serials = within(
        "the revocation",
        client.revoke_enrollment(
            horde_proto::v1::revoke_enrollment_request::Principal::WorkerId(
                horde_proto::v1::WorkerId::new("gpu-box"),
            ),
            "test",
        ),
    )
    .await??;
    assert!(!serials.is_empty(), "the revoked serial is reported");

    let refused = within("the revoked worker's dial", async {
        match tonic::transport::Endpoint::from_shared(harness.main_endpoint()) {
            Err(_) => true,
            Ok(endpoint) => match endpoint
                .tls_config(horde_identity::tls::grpc_client_mtls(
                    &enrolled.identity,
                    "localhost",
                ))
                .map(|endpoint| endpoint.connect_lazy())
            {
                Err(_) => true,
                Ok(channel) => horde_cli::Client::with_channel(channel)
                    .hello("0.1.0")
                    .await
                    .is_err(),
            },
        }
    })
    .await?;
    assert!(
        refused,
        "a revoked worker identity must stop opening the plane"
    );
    Ok(())
}

#[tokio::test]
async fn plaintext_and_missing_identities_are_refused_not_degraded() -> Setup<()> {
    let harness = serve_secured().await?;

    // Plaintext against the secured listener: refused, never served. The TCP
    // connect itself may succeed — the refusal is that no RPC ever will.
    let plaintext = harness.main_endpoint().replace("https://", "http://");
    let refused = within("the plaintext dial to be refused", async {
        match horde_cli::Client::connect(&plaintext).await {
            Err(_) => true,
            Ok(mut client) => client.hello("0.1.0").await.is_err(),
        }
    })
    .await?;
    assert!(
        refused,
        "the secured listener must not serve a plaintext client"
    );

    // `--identity` pointing nowhere is an error naming the directory, not a
    // silent plaintext connection.
    let missing = tempfile::tempdir()?;
    let missing_path = missing.path().join("nowhere");
    let outcome = invoke(&[
        "--endpoint",
        &harness.main_endpoint(),
        "--identity",
        &missing_path.to_string_lossy(),
        "hello",
    ])
    .await;
    let message = format!("{:?}", outcome.expect_err("a missing identity must refuse"));
    assert!(
        message.contains("nowhere"),
        "the refusal names the directory: {message}"
    );
    Ok(())
}

/// How many mints and revocations the enrollment stream holds.
async fn enrollment_counts(client: &mut horde_cli::Client) -> Setup<(u32, u32)> {
    let mut counts = (0_u32, 0_u32);
    client
        .sync(
            &horde_cli::client::SyncOptions::new(horde_server::enrollment::ENROLLMENT_STREAM),
            |envelope| {
                let kind = envelope
                    .event
                    .as_ref()
                    .and_then(|event| event.kind.as_ref())
                    .and_then(horde_proto::kind_field_name);
                match kind {
                    Some("enrollment_code_minted") => counts.0 += 1,
                    Some("enrollment_revoked") => counts.1 += 1,
                    _ => {}
                }
            },
            |_| {},
        )
        .await?;
    Ok(counts)
}

/// Every enrollment verb, through the same `cli::run` dispatch the binary
/// uses, asserted by its effect on the enrollment stream — a verb that
/// quietly did nothing also returns `Ok`, and only the log can tell.
#[tokio::test]
async fn the_enrollment_verbs_dispatch_with_real_effects() -> Setup<()> {
    let harness = serve_secured().await?;

    // An enrolled client identity on disk, for the --identity flag.
    let identity_dir = tempfile::tempdir()?;
    let identity = harness.enroll(PrincipalRole::Client, "laptop").await?;
    identity.save(identity_dir.path())?;
    let identity_flag = identity_dir.path().to_string_lossy().into_owned();
    let base = [
        "--endpoint",
        &harness.main_endpoint(),
        "--identity",
        &identity_flag,
    ];

    let mut reader = harness.client(&identity).await?;
    let (minted_before, _) = enrollment_counts(&mut reader).await?;

    // `horde worker enroll` mints a worker code.
    within(
        "the worker enroll verb",
        invoke(&[&base[..], &["worker", "enroll", "gpu-box"]].concat()),
    )
    .await??;
    // `horde enroll client` mints a client code.
    within(
        "the enroll client verb",
        invoke(&[&base[..], &["enroll", "client", "other-laptop"]].concat()),
    )
    .await??;
    let (minted_after, revoked_before) = enrollment_counts(&mut reader).await?;
    assert_eq!(
        minted_after,
        minted_before + 2,
        "each mint verb journals exactly one code"
    );

    // Enroll a worker for real, then `horde worker revoke` it.
    harness.enroll(PrincipalRole::Worker, "gpu-box").await?;
    within(
        "the worker revoke verb",
        invoke(
            &[
                &base[..],
                &["worker", "revoke", "gpu-box", "--reason", "test"],
            ]
            .concat(),
        ),
    )
    .await??;
    // `horde enroll revoke-client` on a name with nothing active: a no-op
    // that still runs its arm and journals nothing.
    within(
        "the revoke-client verb",
        invoke(&[&base[..], &["enroll", "revoke-client", "never-joined"]].concat()),
    )
    .await??;

    let (_, revoked_after) = enrollment_counts(&mut reader).await?;
    assert_eq!(
        revoked_after,
        revoked_before + 1,
        "revoking an enrolled worker journals; revoking nobody journals nothing"
    );
    Ok(())
}
