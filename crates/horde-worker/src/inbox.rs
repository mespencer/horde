//! The journaled inbox: every accepted command, until it is done (ADR-0012).
//!
//! The Runtime splits applying a command in two. **Accepting** it is one
//! transaction — the dedupe mark, both ack watermarks, and the command itself
//! written into this table — after which the command is this Worker's to
//! finish, and the Server may stop owing it. **Completing** it is a second
//! transaction — the command's effects, its bookkeeping, and what becomes of
//! its inbox row. In between, the command may run as long as it needs to (a
//! clone, a harness start) without holding anything else up, and a crash in
//! between loses nothing: the row is still here, and the next start replays
//! it.
//!
//! A message has one more step. Handing it to a live agent's pump is not
//! delivering it — the pump may hold it for a turn boundary, and the harness
//! may die before it is written. So a delivered message's row is only
//! *handed* when its command completes, and settled by the pump once the
//! harness actually took the text. Whatever is still handed when a harness
//! dies is what a revival replays, in order, before anything newer.

use horde_proto::prost::Message;
use horde_proto::v1::StreamCommandsResponse;
use rusqlite::params;

use crate::error::WorkerError;
use crate::journal::{
    AgentBookkeeping, CommandApplication, Journal, JournalEntry, append_all, apply_bookkeeping,
    in_transaction, mark_applied, read_error,
};

/// What completing a command does with its inbox row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxDisposition {
    /// The command is done: its row goes.
    Settle,
    /// A message handed to a live pump: the row stays, marked handed, until
    /// the pump settles it after the harness took the text.
    Hand,
    /// The command is done, and so is every message the agent was handed
    /// and never sent — a revival that failed, whose journaled failure is
    /// the answer to all of them.
    SettleAgent(String),
}

/// One accepted command, as the inbox holds it.
#[derive(Debug, Clone, PartialEq)]
pub struct InboxEntry {
    /// The lane that runs it: an agent id, or a workspace's lane key.
    pub lane: String,
    pub command: StreamCommandsResponse,
}

impl Journal {
    /// Accepts a command: marks it applied, ratchets the ack watermarks, and
    /// holds it in the inbox under `lane`, in one transaction.
    ///
    /// A redelivery appends nothing and reports
    /// [`CommandApplication::AlreadyApplied`] — the caller must not run it.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the transaction cannot commit.
    pub async fn accept_command(
        &self,
        command: &StreamCommandsResponse,
        lane: &str,
    ) -> Result<CommandApplication, WorkerError> {
        const CONTEXT: &str = "accepting a command into the inbox";
        let command_id = command.command_id.clone();
        let sequence = command.command_sequence;
        let position = command.log_position;
        let encoded = command.encode_to_vec();
        let lane = lane.to_owned();
        self.with_connection(CONTEXT, move |connection| {
            in_transaction(connection, CONTEXT, |transaction| {
                let application = mark_applied(transaction, &command_id, sequence, position)?;
                if application == CommandApplication::Applied {
                    transaction.execute(
                        "INSERT INTO inbox (command_id, lane, command) VALUES (?1, ?2, ?3)",
                        params![command_id, lane, encoded],
                    )?;
                }
                Ok(application)
            })
        })
        .await
    }

    /// Completes a command: journals its effects, applies its bookkeeping,
    /// and disposes of its inbox row, in one transaction. `command_id` is
    /// `None` for work no command caused (a revival after a harness died
    /// idle), whose disposition then touches only what it names.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the transaction cannot commit.
    pub async fn complete_command(
        &self,
        command_id: Option<String>,
        effects: Vec<JournalEntry>,
        bookkeeping: AgentBookkeeping,
        disposition: InboxDisposition,
    ) -> Result<(), WorkerError> {
        const CONTEXT: &str = "completing an accepted command";
        let appended = !effects.is_empty();
        let head = self
            .with_connection(CONTEXT, move |connection| {
                in_transaction(connection, CONTEXT, |transaction| {
                    let head = append_all(transaction, &effects)?;
                    apply_bookkeeping(transaction, &bookkeeping)?;
                    let id = command_id.unwrap_or_default();
                    match &disposition {
                        InboxDisposition::Settle => {
                            transaction
                                .execute("DELETE FROM inbox WHERE command_id = ?1", params![id])?;
                        }
                        InboxDisposition::Hand => {
                            transaction.execute(
                                "UPDATE inbox SET handed = 1 WHERE command_id = ?1",
                                params![id],
                            )?;
                        }
                        InboxDisposition::SettleAgent(agent_id) => {
                            transaction.execute(
                                "DELETE FROM inbox WHERE command_id = ?1
                                     OR (lane = ?2 AND handed = 1)",
                                params![id, agent_id],
                            )?;
                        }
                    }
                    Ok(head)
                })
            })
            .await?;
        if appended {
            self.announce(head);
        }
        Ok(())
    }

    /// Settles one handed message: the harness took its text.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the row cannot be deleted.
    pub async fn settle_delivery(&self, command_id: &str) -> Result<(), WorkerError> {
        let command_id = command_id.to_owned();
        self.with_connection("settling a delivered message", move |connection| {
            connection
                .execute(
                    "DELETE FROM inbox WHERE command_id = ?1",
                    params![command_id],
                )
                .map(|_| ())
                .map_err(read_error)
        })
        .await
    }

    /// The messages `agent_id` was handed and never sent, oldest first —
    /// what a revival delivers before anything newer.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the rows cannot be read, and
    /// [`WorkerError::CorruptInboxEntry`] when one no longer decodes.
    pub async fn handed_deliveries(
        &self,
        agent_id: &str,
    ) -> Result<Vec<StreamCommandsResponse>, WorkerError> {
        let agent_id = agent_id.to_owned();
        let entries = self
            .with_connection("reading an agent's handed messages", move |connection| {
                inbox_rows(connection, Some(&agent_id))
            })
            .await?;
        Ok(entries.into_iter().map(|entry| entry.command).collect())
    }

    /// Every command the inbox still holds, oldest first, all marked
    /// unhanded again — what a fresh start replays. Nothing is running yet,
    /// so nothing is handed: each row runs again as the command it was, and
    /// a message reaches its agent by the same revival any message would.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the rows cannot be read or reset, and
    /// [`WorkerError::CorruptInboxEntry`] when one no longer decodes.
    pub async fn replay_inbox(&self) -> Result<Vec<InboxEntry>, WorkerError> {
        self.with_connection("replaying the inbox", |connection| {
            connection
                .execute("UPDATE inbox SET handed = 0", [])
                .map_err(read_error)?;
            inbox_rows(connection, None)
        })
        .await
    }
}

/// Reads and decodes inbox rows in acceptance order: `agent`'s handed
/// messages, or with `None` every row.
fn inbox_rows(
    connection: &rusqlite::Connection,
    agent: Option<&str>,
) -> Result<Vec<InboxEntry>, WorkerError> {
    let mut statement = connection
        .prepare(
            "SELECT command_id, lane, command FROM inbox
                 WHERE ?1 IS NULL OR (lane = ?1 AND handed = 1)
                 ORDER BY ordinal",
        )
        .map_err(read_error)?;
    let stored: Vec<(String, String, Vec<u8>)> = statement
        .query_map(params![agent], |row| {
            Ok((
                row.get("command_id")?,
                row.get("lane")?,
                row.get("command")?,
            ))
        })
        .and_then(Iterator::collect)
        .map_err(read_error)?;
    stored
        .into_iter()
        .map(|(command_id, lane, encoded)| {
            StreamCommandsResponse::decode(encoded.as_slice())
                .map(|command| InboxEntry { lane, command })
                .map_err(|source| WorkerError::CorruptInboxEntry { command_id, source })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::stream_commands_response::Command;
    use horde_proto::v1::{AgentId, ArchiveAgentCommand, StreamCommandsResponse};

    use super::InboxDisposition;
    use crate::journal::{AgentBookkeeping, CommandApplication, Journal};

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
