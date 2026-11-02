//! Client-side session and root-agent resolution (ADR-0008).
//!
//! Wherever a verb takes a session, a name or a unique id prefix resolves to
//! the id, git-style, against the session list — the wire keeps speaking
//! ids. The exact-name case — the common one, since generated names are
//! titles — asks the server's `ListSessions` name filter (ADR-0013) rather
//! than listing everything; only a slug, an id or a prefix falls back to the
//! whole list. Ambiguity is an error naming the candidates, never a guess.
//! A reference nothing matches is refused here, naming the fix — and when
//! what it matches is archived and the verb was not asked to look there,
//! the refusal says so and names `--all`.

use horde_proto::v1::{SessionLifecycleState, SessionSummary};

use crate::names::slugify;
use crate::{Client, ClientError};

/// The prefix the server mints every agent id with: a reference carrying it
/// is an agent id already, so resolving it needs no session list.
const AGENT_ID_PREFIX: &str = "agent-";

/// The session id `reference` names, against the live list (or the whole
/// list with `include_archived`).
///
/// # Errors
///
/// Listing failing, the reference matching more than one session, matching
/// none, or matching only an archived session without `include_archived`.
pub async fn session_id(
    client: &mut Client,
    reference: &str,
    include_archived: bool,
) -> Result<String, ClientError> {
    let named = client.list_sessions_named(reference, true).await?;
    if let Some(id) = pick(&named, reference, include_archived)? {
        return Ok(id);
    }
    let everything = client.list_sessions(true).await?;
    pick(&everything, reference, include_archived)?.ok_or_else(|| ClientError::UnknownSession {
        reference: reference.to_owned(),
    })
}

/// The id `reference` resolves to among `sessions` — the visible ones
/// (live, or all with `include_archived`) — or the archived refusal when
/// only a hidden session answers.
///
/// # Errors
///
/// Ambiguity among the visible sessions, or the archived refusal.
pub fn pick(
    sessions: &[SessionSummary],
    reference: &str,
    include_archived: bool,
) -> Result<Option<String>, ClientError> {
    // A full id is exact whatever the session's state: nothing that named
    // an archived session by id needs `--all` to keep working.
    if let Some(exact) = sessions.iter().find(|summary| id_of(summary) == reference) {
        return Ok(Some(id_of(exact)));
    }
    let (visible, hidden): (Vec<SessionSummary>, Vec<SessionSummary>) = sessions
        .iter()
        .cloned()
        .partition(|summary| include_archived || !is_archived(summary));
    if let Some(found) = resolve(&visible, reference)? {
        return Ok(Some(id_of(found)));
    }
    match resolve(&hidden, reference) {
        Ok(None) => Ok(None),
        Ok(Some(archived)) => Err(archived_refusal(reference, &id_of(archived))),
        Err(_) => Err(archived_refusal(reference, "(several)")),
    }
}

fn archived_refusal(reference: &str, session_id: &str) -> ClientError {
    ClientError::ArchivedSession {
        reference: reference.to_owned(),
        session_id: session_id.to_owned(),
    }
}

fn is_archived(summary: &SessionSummary) -> bool {
    summary.state == SessionLifecycleState::Archived as i32
}

/// The agent id `reference` names: a session's name resolves to that
/// session's root agent — "talk to the session" needs no second id — and
/// anything else passes through as the agent id it already is. Only names
/// alias; an id prefix does not, because a prefix of a *session* id where an
/// agent is expected reads as the typo it probably is.
///
/// # Errors
///
/// Listing failing, or the name matching more than one session.
pub async fn agent_id(client: &mut Client, reference: &str) -> Result<String, ClientError> {
    let named = client.list_sessions_named(reference, false).await?;
    if let Some(root) = root_agent_for(&named, reference)? {
        return Ok(root);
    }
    if reference.starts_with(AGENT_ID_PREFIX) {
        return Ok(reference.to_owned());
    }
    let sessions = client.list_sessions(false).await?;
    Ok(root_agent_for(&sessions, reference)?.unwrap_or_else(|| reference.to_owned()))
}

/// The one session `reference` names, or `None` when nothing matches.
///
/// Tiers, most exact first: a full session id, then a name (the slugified
/// title), then a unique id prefix. Each tier is decided before the next is
/// tried, so a name can never be shadowed by being some id's prefix.
///
/// # Errors
///
/// More than one session answering within a tier.
pub fn resolve<'sessions>(
    sessions: &'sessions [SessionSummary],
    reference: &str,
) -> Result<Option<&'sessions SessionSummary>, ClientError> {
    if let Some(exact) = sessions.iter().find(|summary| id_of(summary) == reference) {
        return Ok(Some(exact));
    }
    for tier in [named_exactly, id_starts_with] {
        let matches: Vec<&SessionSummary> = sessions
            .iter()
            .filter(|summary| tier(summary, reference))
            .collect();
        match matches[..] {
            [] => {}
            [one] => return Ok(Some(one)),
            _ => return Err(ambiguous(reference, &matches)),
        }
    }
    Ok(None)
}

/// The root agent of the one session named `reference`, or `None` when no
/// session answers to that name.
///
/// # Errors
///
/// More than one session answering to the name.
pub fn root_agent_for(
    sessions: &[SessionSummary],
    reference: &str,
) -> Result<Option<String>, ClientError> {
    let matches: Vec<&SessionSummary> = sessions
        .iter()
        .filter(|summary| named_exactly(summary, reference))
        .collect();
    match matches[..] {
        [] => Ok(None),
        [one] => Ok(one.root_agent_id.as_ref().map(ToString::to_string)),
        _ => Err(ambiguous(reference, &matches)),
    }
}

/// The name a session answers to: its slugified title.
#[must_use]
pub fn name_of(summary: &SessionSummary) -> String {
    slugify(&summary.title)
}

fn id_of(summary: &SessionSummary) -> String {
    summary
        .session_id
        .as_ref()
        .map_or_else(String::new, ToString::to_string)
}

/// Answers to its name — the slug — or to its title verbatim, which is
/// what the server's exact-name filter matched on.
fn named_exactly(summary: &SessionSummary, reference: &str) -> bool {
    !reference.is_empty() && (name_of(summary) == reference || summary.title == reference)
}

fn id_starts_with(summary: &SessionSummary, reference: &str) -> bool {
    !reference.is_empty() && id_of(summary).starts_with(reference)
}

/// The refusal, with every candidate named so the next attempt can be exact.
fn ambiguous(reference: &str, matches: &[&SessionSummary]) -> ClientError {
    ClientError::AmbiguousSession {
        reference: reference.to_owned(),
        candidates: matches
            .iter()
            .map(|summary| {
                let name = name_of(summary);
                let id = id_of(summary);
                if name.is_empty() {
                    id
                } else {
                    format!("{name}  {id}")
                }
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{AgentId, SessionId, SessionSummary};

    use super::{resolve, root_agent_for};

    fn session(id: &str, title: &str, root: &str) -> SessionSummary {
        SessionSummary {
            session_id: Some(SessionId::new(id)),
            title: title.to_owned(),
            root_agent_id: Some(AgentId::new(root)),
            ..SessionSummary::default()
        }
    }

    fn fixtures() -> Vec<SessionSummary> {
        vec![
            session("session-a1f0-0", "Fix Auth Timeout", "session-a1f0-0-root"),
            session("session-a1f0-1", "brisk-warband", "session-a1f0-1-root"),
            session("session-b2e1-0", "", "session-b2e1-0-root"),
        ]
    }

    fn resolved_id(reference: &str) -> Option<String> {
        resolve(&fixtures(), reference)
            .expect("unambiguous")
            .and_then(|summary| summary.session_id.as_ref().map(ToString::to_string))
    }

    /// The three tiers, most exact first — and the slugified title is the
    /// name, so a multi-word title answers to its handle.
    #[test]
    fn full_ids_names_and_unique_prefixes_each_resolve() {
        assert_eq!(
            resolved_id("session-b2e1-0").as_deref(),
            Some("session-b2e1-0")
        );
        assert_eq!(
            resolved_id("fix-auth-timeout").as_deref(),
            Some("session-a1f0-0")
        );
        assert_eq!(
            resolved_id("brisk-warband").as_deref(),
            Some("session-a1f0-1")
        );
        assert_eq!(resolved_id("session-b2").as_deref(), Some("session-b2e1-0"));
    }

    /// A full id wins outright even while it is also a prefix of a sibling:
    /// exactness must never be reinterpreted as ambiguity.
    #[test]
    fn a_full_id_beats_the_prefix_tier() {
        let sessions = vec![
            session("session-key-1", "one", "r1"),
            session("session-key-12", "two", "r2"),
        ];
        let resolved = resolve(&sessions, "session-key-1")
            .expect("exact id is never ambiguous")
            .expect("a match");
        assert_eq!(resolved.title, "one");
    }

    /// Ambiguity is an error naming every candidate, never a guess — for a
    /// shared name and for a shared prefix alike.
    #[test]
    fn ambiguity_is_refused_naming_the_candidates() {
        let twins = vec![
            session("session-1", "deploy", "r1"),
            session("session-2", "deploy", "r2"),
        ];
        let refused = resolve(&twins, "deploy").expect_err("two sessions, one name");
        let message = refused.to_string();
        assert!(
            message.contains("session-1") && message.contains("session-2"),
            "{message}"
        );
        assert!(message.contains("deploy"), "{message}");

        let refused = resolve(&fixtures(), "session-a1f0").expect_err("a shared prefix");
        let message = refused.to_string();
        assert!(
            message.contains("session-a1f0-0") && message.contains("session-a1f0-1"),
            "{message}"
        );
    }

    /// Nothing matching passes through unchanged (the server owns that
    /// refusal), and the empty reference matches nothing rather than every
    /// untitled session.
    #[test]
    fn no_match_passes_through_and_empty_matches_nothing() {
        assert_eq!(resolved_id("archived-elsewhere"), None);
        assert_eq!(resolved_id(""), None);
    }

    fn archived(id: &str, title: &str) -> SessionSummary {
        SessionSummary {
            state: horde_proto::v1::SessionLifecycleState::Archived as i32,
            ..session(id, title, "r")
        }
    }

    /// The visible sessions answer first; a reference only an archived
    /// session answers is refused naming `--all`, unless `--all` was given;
    /// nothing at all is `None` for the caller to refuse.
    #[test]
    fn pick_hides_archived_sessions_behind_a_hint_naming_all() {
        let sessions = vec![
            session("session-live", "deploy", "r1"),
            archived("session-gone", "old-work"),
        ];
        assert_eq!(
            super::pick(&sessions, "deploy", false).expect("live"),
            Some("session-live".to_owned())
        );
        let refused = super::pick(&sessions, "old-work", false).expect_err("archived");
        assert!(
            refused.to_string().contains("--all") && refused.to_string().contains("session-gone"),
            "{refused}"
        );
        assert_eq!(
            super::pick(&sessions, "old-work", true).expect("--all looks there"),
            Some("session-gone".to_owned())
        );
        assert_eq!(
            super::pick(&sessions, "nothing", false).expect("no match"),
            None
        );
        assert_eq!(
            super::pick(&sessions, "session-gone", false).expect("a full id is exact"),
            Some("session-gone".to_owned())
        );
        assert!(
            super::pick(&sessions, "session-go", false).is_err(),
            "an archived id's prefix is hidden like its name"
        );

        let twins = vec![archived("session-a", "x"), archived("session-b", "x")];
        let refused = super::pick(&twins, "x", false).expect_err("archived twins");
        assert!(refused.to_string().contains("--all"), "{refused}");
    }

    /// The title verbatim answers too — what the server's exact-name filter
    /// matched on — beside the slug.
    #[test]
    fn a_title_verbatim_resolves_like_its_slug() {
        assert_eq!(
            resolved_id("Fix Auth Timeout").as_deref(),
            Some("session-a1f0-0")
        );
    }

    /// The agent alias: a session name yields its root agent, anything else
    /// passes through — including an id prefix, which must not alias where
    /// an agent is expected.
    #[test]
    fn a_session_name_aliases_its_root_agent_and_nothing_else_does() {
        let sessions = fixtures();
        assert_eq!(
            root_agent_for(&sessions, "fix-auth-timeout").expect("unambiguous"),
            Some("session-a1f0-0-root".to_owned())
        );
        assert_eq!(
            root_agent_for(&sessions, "session-a1f0").expect("prefixes do not alias"),
            None
        );
        assert_eq!(
            root_agent_for(&sessions, "agent-77").expect("an agent id passes through"),
            None
        );
        assert!(
            root_agent_for(
                &[
                    session("session-1", "deploy", "r1"),
                    session("session-2", "deploy", "r2"),
                ],
                "deploy"
            )
            .is_err(),
            "a shared name is refused where an agent is expected too"
        );
    }
}
