//! What is `horde-cli`-shaped about the shared server harness.
//!
//! The harness itself — both spines on a loopback socket, session seeding,
//! and the worker-shaped journal push — lives in `horde-server-testkit`,
//! because `horde-mcp` and `horde-server` test against the same arrangement
//! and three private copies of a transport harness drift. What stays here is
//! only what no other suite wants: the [`Client::with_channel`] entry and the
//! note spammer.
//!
//! Each test binary compiles its own copy of this module and uses a subset of
//! it, so items unused by one file are dead code there — and re-exports unused
//! by one file are unused imports there. The allow is scoped to this module
//! rather than sprinkled at the call sites, which is the conventional shape
//! for an integration-test `common` module.
#![allow(dead_code, unreachable_pub, unused_imports)]

pub use horde_server_testkit::{Setup, journal_event, seed, seeded, serve};

use horde_cli::Client;
use horde_proto::v1::DeliveryMode;

/// A client wired to its own private server, over a channel the caller built
/// rather than one [`Client::connect`] dialled.
///
/// This is the [`Client::with_channel`] seam, and entering through it here is
/// what keeps it exercised: a frontend that owns its transport — an embedded
/// server, a custom connector, a pre-warmed pool — constructs a client this
/// way, and a constructor no test ever calls is one nobody knows still works.
pub async fn client() -> Setup<Client> {
    let endpoint = serve().await?;
    let channel = tonic::transport::Endpoint::try_from(endpoint)?
        .connect()
        .await?;
    Ok(Client::with_channel(channel))
}

/// Appends `count` notes to an agent, which is how a test gets a log with
/// enough in it to replay from an offset.
pub async fn send_notes(client: &mut Client, agent_id: &str, count: usize) -> Setup<()> {
    for index in 0..count {
        client
            .send_note(
                agent_id,
                &format!("note {index}"),
                DeliveryMode::TurnBoundary,
                "",
            )
            .await?;
    }
    Ok(())
}
