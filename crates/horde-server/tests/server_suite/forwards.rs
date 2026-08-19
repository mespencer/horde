//! The forward intent verbs (ADR-0006): every refusal through the RPC's own
//! surface, and the idempotency the wire promises.
//!
//! The byte plane is proven over real sockets in the worker suite (the
//! dependency points that way); what lives here is the durable half — what
//! may be journaled, what is refused, and what a retry gets back.

use horde_proto::v1::{AgentLifecycleState, ForwardDirection, ForwardSpec};
use horde_server_testkit::{Setup, journal_event_at, seeded};

fn spec(
    direction: ForwardDirection,
    name: &str,
    worker_port: u32,
    client_port: u32,
) -> ForwardSpec {
    ForwardSpec {
        direction: direction as i32,
        name: name.to_owned(),
        worker_port,
        client_port,
    }
}

fn expose(name: &str) -> ForwardSpec {
    spec(ForwardDirection::Expose, name, 5173, 0)
}

/// The refusal table, each through the live RPC: specs the log must not
/// hold, owners that cannot carry, names already live, and an inject with
/// nobody identifiable to back it.
#[tokio::test]
async fn open_forward_refuses_what_the_log_must_not_hold() -> Setup<()> {
    let endpoint = horde_server_testkit::serve().await?;
    let (mut client, session, root) = seeded(&endpoint, "forwards").await?;

    for (spec, needle) in [
        (spec(ForwardDirection::Unspecified, "x", 1, 0), "which way"),
        (spec(ForwardDirection::Expose, "", 1, 0), "named"),
        (
            spec(ForwardDirection::Expose, "x", 0, 0),
            "worker-side port",
        ),
        (
            spec(ForwardDirection::Inject, "x", 1, 0),
            "client-side port",
        ),
    ] {
        let refused = client
            .open_forward(&session, &root, spec, "")
            .await
            .expect_err("an incomplete spec must be refused");
        assert!(
            refused.to_string().contains(needle),
            "the refusal must name the gap ({needle}): {refused}"
        );
    }

    let ghost = client
        .open_forward(&session, "no-such-agent", expose("vite"), "")
        .await
        .expect_err("an unknown owner must be refused");
    assert!(ghost.to_string().contains("no-such-agent"), "{ghost}");

    // A live duplicate name is refused; the first open stands.
    client
        .open_forward(&session, &root, expose("vite"), "open-1")
        .await
        .expect("the first open journals");
    let doubled = client
        .open_forward(&session, &root, expose("vite"), "open-2")
        .await
        .expect_err("a live name cannot be doubled");
    assert!(doubled.to_string().contains("vite"), "{doubled}");

    // A plaintext channel names no client principal, and an inject with no
    // backer would be an intent nothing can ever serve.
    let unbackable = client
        .open_forward(
            &session,
            &root,
            spec(ForwardDirection::Inject, "db", 5432, 5432),
            "",
        )
        .await
        .expect_err("an unidentifiable initiator cannot back");
    assert!(unbackable.to_string().contains("mTLS"), "{unbackable}");
    Ok(())
}

/// A retried open deduplicates against the stored event and answers with
/// the id the first attempt minted — not a fresh id the log never heard of.
#[tokio::test]
async fn a_retried_open_returns_the_stored_forward_id() -> Setup<()> {
    let endpoint = horde_server_testkit::serve().await?;
    let (mut client, session, root) = seeded(&endpoint, "forwards").await?;

    let first = client
        .open_forward(&session, &root, expose("vite"), "same-key")
        .await?;
    let retried = client
        .open_forward(&session, &root, expose("vite"), "same-key")
        .await?;

    assert_eq!(
        first.forward_id, retried.forward_id,
        "a retry must not mint a second identity for the same open"
    );
    assert_eq!(first.sequence, retried.sequence);
    Ok(())
}

/// An agent that is `Failed` cannot take a new forward — the process behind
/// the port is gone — and a live forward dies from the list the moment its
/// agent does, without any close being journaled.
#[tokio::test]
async fn intent_follows_the_owning_agents_life() -> Setup<()> {
    let endpoint = horde_server_testkit::serve().await?;
    let (mut client, session, root) = seeded(&endpoint, "forwards").await?;

    client
        .open_forward(&session, &root, expose("vite"), "open-1")
        .await?;
    assert_eq!(client.list_forwards(Some(&session)).await?.len(), 1);

    let failed = horde_proto::v1::Event {
        agent_id: Some(horde_proto::v1::AgentId::new(root.clone())),
        occurred_at: None,
        idempotency_key: "fail-root".to_owned(),
        kind: Some(horde_proto::v1::event::Kind::AgentLifecycleChanged(
            horde_proto::v1::AgentLifecycleChanged {
                previous_state: AgentLifecycleState::Running as i32,
                state: AgentLifecycleState::Failed as i32,
                reason: "harness exited".to_owned(),
            },
        )),
    };
    journal_event_at(&endpoint, &session, 1, failed).await?;

    assert!(
        client.list_forwards(Some(&session)).await?.is_empty(),
        "a dead agent's forward has nothing left to mean"
    );
    let refused = client
        .open_forward(&session, &root, expose("later"), "")
        .await
        .expect_err("a failed agent cannot take a new forward");
    assert!(refused.to_string().contains("Failed"), "{refused}");

    // Revival: the agent leaves Failed, and the intent is live again —
    // derived, never re-journaled.
    let revived = horde_proto::v1::Event {
        agent_id: Some(horde_proto::v1::AgentId::new(root.clone())),
        occurred_at: None,
        idempotency_key: "revive-root".to_owned(),
        kind: Some(horde_proto::v1::event::Kind::AgentLifecycleChanged(
            horde_proto::v1::AgentLifecycleChanged {
                previous_state: AgentLifecycleState::Failed as i32,
                state: AgentLifecycleState::Running as i32,
                reason: "revived".to_owned(),
            },
        )),
    };
    journal_event_at(&endpoint, &session, 2, revived).await?;
    assert_eq!(
        client.list_forwards(Some(&session)).await?.len(),
        1,
        "revival restores the intent without re-declaration"
    );
    Ok(())
}

/// Closing: unknown ids are refused, a close removes the forward from the
/// live list for good, and closing again is an idempotent no-op.
#[tokio::test]
async fn close_forward_settles_the_intent_idempotently() -> Setup<()> {
    let endpoint = horde_server_testkit::serve().await?;
    let (mut client, session, root) = seeded(&endpoint, "forwards").await?;

    let unknown = client
        .close_forward(&session, "fwd-nothing", "")
        .await
        .expect_err("an unknown forward cannot be closed");
    assert!(unknown.to_string().contains("fwd-nothing"), "{unknown}");

    let opened = client
        .open_forward(&session, &root, expose("vite"), "open-1")
        .await?;
    let forward_id = opened.forward_id.ok_or("an id")?.to_string();

    let first = client.close_forward(&session, &forward_id, "done").await?;
    assert!(client.list_forwards(Some(&session)).await?.is_empty());

    let again = client.close_forward(&session, &forward_id, "done").await?;
    assert_eq!(first, again, "a repeated close deduplicates onto the first");

    // The name is free again: closed is not merely hidden.
    client
        .open_forward(&session, &root, expose("vite"), "open-2")
        .await
        .expect("a closed forward's name may be reused");
    Ok(())
}

/// The list's scope: a session filter restricts, and the empty filter walks
/// every session.
#[tokio::test]
async fn list_forwards_scopes_by_session() -> Setup<()> {
    let endpoint = horde_server_testkit::serve().await?;
    let (mut client, session_a, root_a) = seeded(&endpoint, "one").await?;
    let (summary_b, root_b) = client.create_session("two", "key-2").await?;
    let session_b = summary_b.session_id.ok_or("a session id")?.to_string();
    let root_b = root_b.to_string();

    client
        .open_forward(&session_a, &root_a, expose("vite"), "")
        .await?;
    client
        .open_forward(&session_b, &root_b, expose("docs"), "")
        .await?;

    assert_eq!(client.list_forwards(None).await?.len(), 2);
    let scoped = client.list_forwards(Some(&session_a)).await?;
    assert_eq!(scoped.len(), 1);
    let name = scoped[0]
        .intent
        .as_ref()
        .and_then(|intent| intent.spec.as_ref())
        .map(|spec| spec.name.clone());
    assert_eq!(name.as_deref(), Some("vite"));
    Ok(())
}
