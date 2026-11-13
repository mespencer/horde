//! The payload seam, end to end: two Runtimes as two Workers, a real Relay
//! between them, and a commit that travels because the *production* stamp and
//! obtain paths moved it — nothing in this file pushes or fetches by hand.
//!
//! Sender side: an agent's result is recorded through the Worker's
//! [`StampingSink`](horde_worker::StampingSink) — the seam `horde-mcp`'s
//! `report_result` writes into — and comes out stamped with the observed HEAD
//! and staged onto the Relay's share ref. Receiver side: a
//! `DeliverMessageCommand` carrying that payload makes the commit present in
//! the receiving agent's checkout before the message reaches the agent.

#[cfg(test)]
mod suite {
    use std::path::{Path, PathBuf};

    use horde_adapter_fake::{FakeAdapter, Script};
    use horde_proto::v1::stream_commands_response::Command;
    use horde_proto::v1::{
        AgentId, AgentResult, Assignment, DeliverMessageCommand, ResultStatus, SessionId,
        StreamCommandsResponse, deliver_message_command, event::Kind,
    };
    use horde_worker::{ApplyCommand, Journal, JournalEntry, Runtime, WorkspaceManager};

    /// Everything here is loopback or on-disk; a wait that outlives this is a
    /// hang, not a slow machine (docs/QUALITY_GATES.md).
    const CEILING: std::time::Duration = std::time::Duration::from_secs(10);

    async fn within<F: std::future::Future>(what: &str, future: F) -> F::Output {
        match tokio::time::timeout(CEILING, future).await {
            Ok(value) => value,
            Err(elapsed) => panic!("{what} stalled: {elapsed} past {CEILING:?}"),
        }
    }

    /// The shared git driver, panicking on the failures a test's
    /// arrangement cannot survive — production git runs live inside
    /// `horde-relay`.
    async fn git(dir: &Path, args: &[&str]) -> String {
        horde_server_testkit::process::git(dir, args)
            .await
            .expect("the git arrangement succeeds")
    }

    use horde_server_testkit::process::holds_commit;

    /// One Worker of the pair: a Runtime over its own journal and workspace
    /// root, with an echoing fake agent and the shared Relay.
    struct Worker {
        runtime: Runtime,
        journal: Journal,
        root: PathBuf,
    }

    impl Worker {
        fn new(id: &str, scratch: &Path, relay: Option<horde_relay::RelayLocation>) -> Self {
            let journal = Journal::in_memory().expect("an in-memory journal");
            let root = scratch.join(id);
            let mut runtime = Runtime::new(journal.clone())
                .with_adapter(std::sync::Arc::new(FakeAdapter::new(Script::echoing())))
                .with_workspaces(WorkspaceManager::new(id, root.clone()));
            if let Some(relay) = relay {
                runtime = runtime.with_relay(relay);
            }
            Self {
                runtime,
                journal,
                root,
            }
        }

        /// Materializes `ws-1` from `origin` and spawns `agent` into it.
        async fn host_agent(&mut self, origin: &Path, agent: &str) {
            self.runtime
                .apply(materialize(1, origin))
                .await
                .expect("the materialize applies");
            self.runtime
                .apply(spawn(2, agent))
                .await
                .expect("the spawn applies");
            // Acceptance is not completion: the spawn runs on its lane.
            within("the spawn completes", self.runtime.settled()).await;
            assert_eq!(self.runtime.running_agents(), 1);
        }

        /// The path of `agent`'s checkout of the `atlas` repo.
        fn checkout(&self, agent: &str) -> PathBuf {
            self.root
                .join("ws-1")
                .join(".horde-agents")
                .join(agent)
                .join("atlas")
        }

        /// Journal entries so far, decoded to their kinds.
        async fn journaled(&self) -> Vec<horde_proto::v1::JournaledEvent> {
            self.journal
                .read_from(1, 200)
                .await
                .expect("a readable journal")
        }
    }

    fn definition(origin: &Path) -> horde_proto::v1::WorkspaceDefinition {
        let mut definition = horde_proto::v1::WorkspaceDefinition {
            workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new("ws-1")),
            name: "atlas-workspace".to_owned(),
            etag: "1".to_owned(),
            ..horde_proto::v1::WorkspaceDefinition::default()
        };
        definition.repos.push(horde_proto::v1::WorkspaceRepo {
            name: "atlas".to_owned(),
            remote_url: origin.display().to_string(),
            default_branch: "main".to_owned(),
            layout_path: String::new(),
            ..horde_proto::v1::WorkspaceRepo::default()
        });
        definition
    }

    /// A fresh origin under `scratch` with one seed commit — what the two
    /// Workers' mirrors and clones all descend from.
    async fn seeded_origin(scratch: &Path) -> PathBuf {
        let origin = scratch.join("origin");
        std::fs::create_dir_all(&origin).expect("mkdir");
        within("origin init", git(&origin, &["init", "-q", "-b", "main"])).await;
        within(
            "seed commit",
            git(&origin, &["commit", "-qm", "seed", "--allow-empty"]),
        )
        .await;
        origin
    }

    fn command(sequence: u64, command: Command) -> StreamCommandsResponse {
        StreamCommandsResponse {
            command_sequence: sequence,
            command_id: format!("share:{sequence}"),
            log_position: 0,
            command: Some(command),
        }
    }

    fn materialize(sequence: u64, origin: &Path) -> StreamCommandsResponse {
        command(
            sequence,
            Command::MaterializeWorkspace(horde_proto::v1::MaterializeWorkspaceCommand {
                definition: Some(definition(origin)),
                session_id: Some(SessionId::new("s1")),
                ..horde_proto::v1::MaterializeWorkspaceCommand::default()
            }),
        )
    }

    fn spawn(sequence: u64, agent: &str) -> StreamCommandsResponse {
        command(
            sequence,
            Command::SpawnAgent(horde_proto::v1::SpawnAgentCommand {
                session_id: Some(SessionId::new("s1")),
                agent_id: Some(AgentId::new(agent)),
                spec: Some(horde_proto::v1::AgentSpec {
                    workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(
                        "ws-1",
                    )),
                    checkout_policy: horde_proto::v1::CheckoutPolicy::Worktree as i32,
                    ..horde_proto::v1::AgentSpec::default()
                }),
                ..horde_proto::v1::SpawnAgentCommand::default()
            }),
        )
    }

    fn deliver(
        sequence: u64,
        agent: &str,
        text: &str,
        payload: Option<horde_proto::v1::Payload>,
    ) -> StreamCommandsResponse {
        command(
            sequence,
            Command::DeliverMessage(DeliverMessageCommand {
                agent_id: Some(AgentId::new(agent)),
                body: Some(deliver_message_command::Body::Assignment(Assignment {
                    assignment_id: None,
                    text: text.to_owned(),
                    payload,
                })),
                delivery: horde_proto::v1::DeliveryMode::TurnBoundary as i32,
            }),
        )
    }

    /// A result entry as the MCP verb records it: status and summary only,
    /// payload deliberately unset — "the Worker's to stamp".
    fn unstamped_result(agent: &str) -> JournalEntry {
        JournalEntry {
            session_id: "s1".to_owned(),
            agent_id: Some(agent.to_owned()),
            idempotency_key: format!("{agent}:result"),
            kind: Kind::ResultReported(horde_proto::v1::ResultReported {
                result: Some(AgentResult {
                    assignment_id: None,
                    status: ResultStatus::Success as i32,
                    summary: "the feature is in".to_owned(),
                    payload: None,
                }),
            }),
        }
    }

    /// The stamped payload out of a journal's `ResultReported`, if present.
    fn stamped_payload(
        events: &[horde_proto::v1::JournaledEvent],
    ) -> Option<horde_proto::v1::Payload> {
        events
            .iter()
            .find_map(|journaled| match journaled.event.as_ref()?.kind.as_ref()? {
                Kind::ResultReported(reported) => reported.result.as_ref()?.payload.clone(),
                _ => None,
            })
    }

    /// A served Relay whose `atlas` mirror tracks `origin`.
    async fn serve_relay(
        origin: &Path,
        mirrors: &Path,
    ) -> (
        horde_relay::RelayServer,
        std::sync::Arc<horde_relay::MirrorStore>,
    ) {
        let store = std::sync::Arc::new(horde_relay::MirrorStore::new(mirrors.join("relay")));
        store
            .register("atlas", &origin.display().to_string())
            .expect("the repo registers");
        let server = horde_relay::RelayServer::bind(
            "127.0.0.1:0".parse().expect("an address"),
            std::sync::Arc::clone(&store),
        )
        .await
        .expect("the relay binds loopback");
        (server, store)
    }

    /// "Commit, then share": a result reported from a dirty working tree is
    /// refused with ADR-0002's helpful error, and nothing is journaled. The
    /// alternative — stamping whatever HEAD holds — would ack a result whose
    /// payload silently omits the uncommitted half of the work it reports
    /// (the audit's collision #9).
    #[tokio::test]
    async fn a_dirty_tree_refuses_the_stamp_rather_than_omitting_work() {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let origin = seeded_origin(scratch.path()).await;

        let mut worker = Worker::new("w1", scratch.path(), None);
        within("hosts agent-a", worker.host_agent(&origin, "agent-a")).await;

        // Work happens but is never committed.
        let repo = worker.checkout("agent-a");
        std::fs::write(repo.join("feature.rs"), "pub fn feature() {}").expect("write");

        let before = worker.journaled().await.len();
        let sink = worker.runtime.stamping_sink();
        let refused = within("record", sink.record(unstamped_result("agent-a")))
            .await
            .expect_err("a dirty tree must refuse the stamp");
        let message = refused.to_string();
        assert!(
            message.contains("commit, then share"),
            "the refusal is the designed helpful error: {message}"
        );
        assert!(
            message.contains("feature.rs"),
            "the refusal names the uncommitted paths: {message}"
        );
        assert_eq!(
            worker.journaled().await.len(),
            before,
            "a refused result must not be journaled"
        );

        // The same report after committing goes through — the refusal asks
        // for exactly one action, and that action suffices.
        within("stage", git(&repo, &["add", "."])).await;
        within("commit", git(&repo, &["commit", "-qm", "the feature"])).await;
        within("record again", sink.record(unstamped_result("agent-a")))
            .await
            .expect("a committed tree stamps");
        assert!(
            stamped_payload(&worker.journaled().await).is_some(),
            "the committed report carries its stamped payload"
        );
    }

    /// The whole seam: agent A commits on Worker 1, its result is stamped and
    /// staged by the production sink, and delivering that payload to agent B
    /// on Worker 2 makes the commit present in B's checkout — through the
    /// Relay, because nothing else connects the two roots.
    #[tokio::test]
    async fn a_result_stamped_on_one_worker_is_obtainable_on_another() {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let origin = seeded_origin(scratch.path()).await;

        let (server, store) = serve_relay(&origin, scratch.path()).await;
        let relay = server.location();

        let mut sender = Worker::new("w1", scratch.path(), Some(relay.clone()));
        within(
            "worker 1 hosts agent-a",
            sender.host_agent(&origin, "agent-a"),
        )
        .await;

        // Agent A works: a commit lands in its checkout. The test plays the
        // agent; everything after this line is production code.
        let a_repo = sender.checkout("agent-a");
        std::fs::write(a_repo.join("feature.rs"), "pub fn feature() {}").expect("write");
        within("stage", git(&a_repo, &["add", "."])).await;
        within("commit", git(&a_repo, &["commit", "-qm", "the feature"])).await;
        let shared = within("rev-parse", git(&a_repo, &["rev-parse", "HEAD"])).await;

        // The MCP verb path: report_result journals status and summary; the
        // sink stamps what the Worker observed.
        let sink = sender.runtime.stamping_sink();
        within("record", sink.record(unstamped_result("agent-a")))
            .await
            .expect("the stamped result journals");

        let payload = stamped_payload(&sender.journaled().await)
            .expect("the journaled result carries the stamped payload");
        assert_eq!(payload.git_refs.len(), 1);
        assert_eq!(payload.git_refs[0].repo_name, "atlas");
        assert_eq!(
            payload.git_refs[0].commit_sha, shared,
            "the stamp is the observed HEAD, not a guess"
        );
        assert_eq!(
            payload.git_refs[0].branch, "horde/agent-a",
            "the advisory branch is the checkout's own"
        );

        // Staging pushed the share ref to the mirror — the commit is
        // obtainable before any receiver exists.
        let mirror_refs = within("mirror refs", git(&store.path("atlas"), &["show-ref"])).await;
        assert!(
            mirror_refs.contains(&format!("refs/horde/share/{shared}")),
            "the share ref must be on the mirror: {mirror_refs}"
        );

        // Worker 2, a different machine as far as the roots are concerned.
        let mut receiver = Worker::new("w2", scratch.path(), Some(relay));
        within(
            "worker 2 hosts agent-b",
            receiver.host_agent(&origin, "agent-b"),
        )
        .await;
        let b_repo = receiver.checkout("agent-b");
        assert!(
            !holds_commit(&b_repo, &shared).await,
            "the receiver must not already hold the commit, or the test proves nothing"
        );

        // The server folds the result into a follow-up assignment; the
        // delivery obtains the payload before the agent reads the text.
        receiver
            .runtime
            .apply(deliver(3, "agent-b", "review the feature", Some(payload)))
            .await
            .expect("the delivery applies");
        within("the delivery completes", receiver.runtime.settled()).await;

        assert!(
            holds_commit(&b_repo, &shared).await,
            "the commit must be present in the receiving agent's checkout"
        );

        // And the message itself still reached the agent: the fake echoes it
        // through the whole pump into Worker 2's journal.
        let mut polls = 0_u32;
        let mut head = receiver.journal.watch_head();
        let echoed = loop {
            let events = receiver.journaled().await;
            let echoed = events.iter().any(|journaled| {
                matches!(
                    journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
                    Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("review the feature")
                )
            });
            if echoed {
                break true;
            }
            polls += 1;
            assert!(polls < 64, "the echo never reached the journal: {events:?}");
            within("journal moves", head.changed())
                .await
                .expect("the journal outlives the wait");
        };
        assert!(echoed);

        receiver.runtime.shutdown().await;
        sender.runtime.shutdown().await;
    }

    /// The loud-failure half: a payload naming a commit that is nowhere on
    /// the Worker and no Relay to fetch it from fails the *agent*, with the
    /// reason journaled — never a silent drop that hands the agent a name it
    /// cannot check out.
    #[tokio::test]
    async fn an_unobtainable_payload_fails_the_receiving_agent_loudly() {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let origin = seeded_origin(scratch.path()).await;

        let mut lonely = Worker::new("w3", scratch.path(), None);
        within(
            "worker hosts agent-c",
            lonely.host_agent(&origin, "agent-c"),
        )
        .await;

        let absent = horde_proto::v1::Payload {
            git_refs: vec![horde_proto::v1::GitRef {
                repo_name: "atlas".to_owned(),
                commit_sha: "1111111111111111111111111111111111111111".to_owned(),
                branch: String::new(),
            }],
            attachments: Vec::new(),
        };
        lonely
            .runtime
            .apply(deliver(3, "agent-c", "act on this", Some(absent)))
            .await
            .expect("the command applies; the failure is the agent's");
        within("the delivery completes", lonely.runtime.settled()).await;

        assert_eq!(
            lonely.runtime.running_agents(),
            0,
            "an agent that cannot see the work its message names is retired"
        );
        let failures: Vec<String> = lonely
            .journaled()
            .await
            .iter()
            .filter_map(|journaled| {
                let Kind::AgentLifecycleChanged(changed) =
                    journaled.event.as_ref()?.kind.as_ref()?
                else {
                    return None;
                };
                let failed =
                    changed.state == i32::from(horde_proto::v1::AgentLifecycleState::Failed);
                failed.then(|| changed.reason.clone())
            })
            .collect();
        assert!(
            failures.iter().any(|reason| reason.contains("payload")),
            "the failure and its reason must be journaled: {failures:?}"
        );
        lonely.runtime.shutdown().await;
    }
}
