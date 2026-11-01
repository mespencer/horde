//! Tests of the journaled inbox (ADR-0012). The inbox itself lives in
//! [`crate::journal`], beside the transactions it extends; its tests are kept
//! apart so the journal's own suite stays about the journal.

#[cfg(test)]
mod tests {
    use horde_proto::v1::stream_commands_response::Command;
    use horde_proto::v1::{AgentId, ArchiveAgentCommand, StreamCommandsResponse};

    use crate::journal::{AgentBookkeeping, CommandApplication, InboxDisposition, Journal};

    fn command(sequence: u64, agent: &str) -> StreamCommandsResponse {
        StreamCommandsResponse {
            command_sequence: sequence,
            command_id: format!("c{sequence}"),
            log_position: sequence * 10,
            command: Some(Command::ArchiveAgent(ArchiveAgentCommand {
                agent_id: Some(AgentId::new(agent)),
                reason: String::new(),
            })),
        }
    }

    async fn accepted(journal: &Journal, sequence: u64, lane: &str) {
        let application = journal
            .accept_command(&command(sequence, lane), lane)
            .await
            .expect("the command is accepted");
        assert_eq!(application, CommandApplication::Applied);
    }

    async fn complete(journal: &Journal, id: &str, disposition: InboxDisposition) {
        journal
            .complete_command(
                Some(id.to_owned()),
                Vec::new(),
                AgentBookkeeping::None,
                disposition,
            )
            .await
            .expect("the command completes");
    }

    fn ids(commands: &[StreamCommandsResponse]) -> Vec<&str> {
        commands
            .iter()
            .map(|command| command.command_id.as_str())
            .collect()
    }

    /// Acceptance is the ack: both watermarks ratchet and the command is
    /// held — and a redelivery is recognized and holds nothing twice.
    #[tokio::test]
    async fn accepting_acks_and_holds_the_command_once() {
        let journal = Journal::in_memory().expect("a journal");
        accepted(&journal, 3, "a1").await;
        assert_eq!(
            journal
                .applied_command_sequence()
                .await
                .expect("a watermark"),
            3
        );
        assert_eq!(
            journal
                .applied_command_position()
                .await
                .expect("a watermark"),
            30
        );
        let again = journal
            .accept_command(&command(3, "a1"), "a1")
            .await
            .expect("a redelivery is no error");
        assert_eq!(again, CommandApplication::AlreadyApplied);

        let held = journal.replay_inbox().await.expect("a readable inbox");
        assert_eq!(held.len(), 1, "held once: {held:?}");
        assert_eq!(held[0].lane, "a1");
        assert_eq!(held[0].command, command(3, "a1"));
    }

    /// Settling removes the row; handing keeps it as a handed message the
    /// pump settles later; a failed revival settles every handed message of
    /// its agent and nobody else's.
    #[tokio::test]
    async fn dispositions_settle_hand_and_settle_an_agent() {
        let journal = Journal::in_memory().expect("a journal");
        for (sequence, lane) in [(1, "a1"), (2, "a1"), (3, "a2"), (4, "a1")] {
            accepted(&journal, sequence, lane).await;
        }
        complete(&journal, "c1", InboxDisposition::Hand).await;
        complete(&journal, "c2", InboxDisposition::Hand).await;
        complete(&journal, "c3", InboxDisposition::Hand).await;
        assert_eq!(
            ids(&journal.handed_deliveries("a1").await.expect("readable")),
            vec!["c1", "c2"],
            "oldest first, and only this agent's"
        );

        journal.settle_delivery("c1").await.expect("settles");
        assert_eq!(
            ids(&journal.handed_deliveries("a1").await.expect("readable")),
            vec!["c2"]
        );

        complete(
            &journal,
            "c4",
            InboxDisposition::SettleAgent("a1".to_owned()),
        )
        .await;
        assert!(
            journal
                .handed_deliveries("a1")
                .await
                .expect("readable")
                .is_empty()
        );
        assert_eq!(
            ids(&journal.handed_deliveries("a2").await.expect("readable")),
            vec!["c3"],
            "another agent's handed message is not this failure's to settle"
        );

        accepted(&journal, 5, "a2").await;
        complete(&journal, "c5", InboxDisposition::Settle).await;
        assert_eq!(
            replayed_ids(&journal).await,
            vec!["c3".to_owned()],
            "a settled command is gone"
        );
    }

    /// The command ids a replay would run, in order.
    async fn replayed_ids(journal: &Journal) -> Vec<String> {
        journal
            .replay_inbox()
            .await
            .expect("readable")
            .into_iter()
            .map(|entry| entry.command.command_id)
            .collect()
    }

    /// A replay hands every row back unhanded, in acceptance order: after a
    /// restart nothing is running, so each runs again as its own command.
    #[tokio::test]
    async fn a_replay_returns_everything_unhanded_in_order() {
        let journal = Journal::in_memory().expect("a journal");
        accepted(&journal, 1, "a1").await;
        accepted(&journal, 2, "ws:w").await;
        complete(&journal, "c1", InboxDisposition::Hand).await;

        let replayed = journal.replay_inbox().await.expect("readable");
        let lanes: Vec<&str> = replayed.iter().map(|entry| entry.lane.as_str()).collect();
        assert_eq!(lanes, vec!["a1", "ws:w"]);
        assert!(
            journal
                .handed_deliveries("a1")
                .await
                .expect("readable")
                .is_empty(),
            "a replayed row is a command again, not a handed message"
        );
    }

    /// Forgetting an agent drops what its incarnation was handed, but not a
    /// command accepted after — a re-placement's spawn must survive the
    /// cancel that precedes it.
    #[tokio::test]
    async fn forgetting_drops_handed_messages_and_keeps_unrun_commands() {
        let journal = Journal::in_memory().expect("a journal");
        accepted(&journal, 1, "a1").await;
        complete(&journal, "c1", InboxDisposition::Hand).await;
        accepted(&journal, 2, "a1").await;
        accepted(&journal, 3, "a1").await;
        journal
            .complete_command(
                Some("c2".to_owned()),
                Vec::new(),
                AgentBookkeeping::Forget {
                    agent_id: "a1".to_owned(),
                },
                InboxDisposition::Settle,
            )
            .await
            .expect("the forget completes");

        assert_eq!(replayed_ids(&journal).await, vec!["c3".to_owned()]);
    }

    /// A row this Worker wrote that no longer decodes is named, not skipped.
    #[tokio::test]
    async fn a_corrupt_row_names_its_command() {
        let journal = Journal::in_memory().expect("a journal");
        journal
            .with_connection("corrupting a row", |connection| {
                connection
                    .execute(
                        "INSERT INTO inbox (command_id, lane, command) VALUES ('bad', 'a1', x'ff')",
                        [],
                    )
                    .map_err(crate::journal::read_error)
            })
            .await
            .expect("the corrupt row lands");
        let error = journal
            .replay_inbox()
            .await
            .expect_err("a corrupt row cannot replay");
        assert!(error.to_string().contains("bad"), "got {error}");
    }

    /// A completion that appends effects announces the new head; one with
    /// none leaves the watch quiet.
    #[tokio::test]
    async fn only_a_completion_with_effects_announces_a_head() {
        let journal = Journal::in_memory().expect("a journal");
        accepted(&journal, 1, "a1").await;
        let mut head = journal.watch_head();
        head.mark_unchanged();
        complete(&journal, "c1", InboxDisposition::Settle).await;
        assert!(!head.has_changed().expect("alive"), "nothing appended");

        accepted(&journal, 2, "a1").await;
        journal
            .complete_command(
                Some("c2".to_owned()),
                vec![crate::journal::JournalEntry {
                    session_id: "s1".to_owned(),
                    agent_id: Some("a1".to_owned()),
                    idempotency_key: String::new(),
                    kind: horde_proto::v1::event::Kind::TurnInterrupted(
                        horde_proto::v1::TurnInterrupted::default(),
                    ),
                }],
                AgentBookkeeping::None,
                InboxDisposition::Settle,
            )
            .await
            .expect("completes");
        assert_eq!(*head.borrow_and_update(), 1, "the effect's sequence");
    }
}
