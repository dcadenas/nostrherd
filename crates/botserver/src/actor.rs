//! Per-bot actor: start a corpus occupant, then ask on each trigger.

use std::fmt;
use std::path::Path;

use botserver_domain::{Bot, EventId, SessionName};

use crate::{
    AdoptedWaiter, AskDelivery, HostRepository, KelpieClient, KelpieError, NewTurn, OccupantLaunch,
    SessionRecord, TurnState, OCCUPANT_BOOTSTRAP,
};

/// Kelpie readiness wait for a newly started occupant.
const OCCUPANT_START_TIMEOUT_MS: u64 = 90_000;

/// Herdr pane created for one session occupant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OccupantPane {
    pub pane_id: String,
    pub terminal_id: String,
}

/// Allocate a pane and terminal for `kelpie start`.
pub trait OccupantPaneAllocator {
    /// Allocator failure type.
    type Error: fmt::Display;

    /// Create an empty pane in the occupant corpus directory.
    ///
    /// # Errors
    ///
    /// Returns an error when Herdr cannot create the pane.
    fn allocate(&self, session_name: &str, cwd: &Path) -> Result<OccupantPane, Self::Error>;
}

/// One triggering event ready for the bot actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerWork {
    pub event_id: EventId,
    pub channel_id: String,
    pub channel_display: String,
    pub reply_to_event_id: Option<EventId>,
    pub nostr_body: String,
}

/// How the actor treated one trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerOutcome {
    /// A Kelpie ask was delivered and the turn is open.
    Asked,
    /// The event was persisted and must wait for an in-flight turn.
    Queued,
    /// The event was already processed.
    Duplicate,
    /// The action was acknowledged and left for a later issue.
    Declined,
}

/// Failure while starting or asking an occupant.
#[derive(Debug)]
pub enum ActorError<E> {
    /// Host persistence failed.
    Repository(E),
    /// Herdr could not allocate a pane.
    Pane(String),
    /// Kelpie rejected start or ask.
    Kelpie(KelpieError),
    /// The channel could not be named as a session.
    UnnameableSession,
    /// Ask delivery was rejected or had no recipient.
    AskNotDelivered(AskDelivery),
    /// The trigger text after `bot:` was empty.
    EmptyAskBody,
    /// `open_next_turn` did not bind a delivered ask.
    TurnNotOpened { ask_id: String },
}

impl<E: fmt::Display> fmt::Display for ActorError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Repository(error) => write!(formatter, "host persistence failed: {error}"),
            Self::Pane(error) => write!(formatter, "occupant pane allocation failed: {error}"),
            Self::Kelpie(error) => write!(formatter, "{error}"),
            Self::UnnameableSession => formatter.write_str("channel cannot be named as a session"),
            Self::AskNotDelivered(delivery) => {
                write!(formatter, "occupant ask was not delivered ({delivery:?})")
            }
            Self::EmptyAskBody => formatter.write_str("trigger request is empty"),
            Self::TurnNotOpened { ask_id } => {
                write!(formatter, "delivered ask {ask_id} was not bound to a turn")
            }
        }
    }
}

impl<E> std::error::Error for ActorError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Repository(error) => Some(error),
            Self::Kelpie(error) => Some(error),
            Self::Pane(_)
            | Self::UnnameableSession
            | Self::AskNotDelivered(_)
            | Self::EmptyAskBody
            | Self::TurnNotOpened { .. } => None,
        }
    }
}

/// Serialized in-process actor for one configured bot.
#[derive(Debug)]
pub struct BotActor<R, P> {
    bot: Bot,
    pub(crate) repository: R,
    panes: P,
}

impl<R, P> BotActor<R, P>
where
    R: HostRepository,
    P: OccupantPaneAllocator,
{
    /// Create an actor for one configured bot.
    #[must_use]
    pub fn new(bot: Bot, repository: R, panes: P) -> Self {
        Self {
            bot,
            repository,
            panes,
        }
    }

    /// Return the configured bot.
    #[must_use]
    pub fn bot(&self) -> &Bot {
        &self.bot
    }

    /// Persist a trigger and start or ask the channel occupant.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence, pane allocation, or Kelpie fails.
    pub fn handle_trigger(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &AdoptedWaiter<'_>,
        work: &TriggerWork,
    ) -> Result<TriggerOutcome, ActorError<R::Error>> {
        if work.nostr_body.trim().is_empty() {
            return Err(ActorError::EmptyAskBody);
        }
        let display = if work.channel_display.is_empty() {
            work.channel_id.as_str()
        } else {
            work.channel_display.as_str()
        };
        self.ensure_session(&work.channel_id, display)?;
        let Some(_) = self
            .repository
            .enqueue_unprocessed_turn(&NewTurn {
                bot_id: self.bot.id().clone(),
                channel_id: work.channel_id.clone(),
                event_id: work.event_id.clone(),
                reply_to_event_id: work.reply_to_event_id.clone(),
            })
            .map_err(ActorError::Repository)?
        else {
            return Ok(TriggerOutcome::Duplicate);
        };
        if !self.should_ask_event(&work.channel_id, &work.event_id)? {
            return Ok(TriggerOutcome::Queued);
        }
        self.ask_oldest_queued(kelpie, waiter, &work.channel_id, &work.nostr_body)?;
        Ok(TriggerOutcome::Asked)
    }

    /// Handle one ingest action for this bot.
    ///
    /// Turn candidates start or ask the occupant. Edits and deletes are
    /// acknowledged without changing turns; those transitions belong to a later
    /// issue.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence, pane allocation, or Kelpie fails.
    pub fn handle_ingest(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &AdoptedWaiter<'_>,
        action: &crate::relay::IngestAction,
        channel_display: &str,
    ) -> Result<TriggerOutcome, ActorError<R::Error>> {
        match action {
            crate::relay::IngestAction::TurnCandidate {
                event_id,
                channel_id,
                reply_to_event_id,
                trigger,
            } => {
                if trigger.request().is_empty() {
                    self.repository
                        .mark_event_processed(event_id)
                        .map_err(ActorError::Repository)?;
                    return Ok(TriggerOutcome::Declined);
                }
                self.handle_trigger(
                    kelpie,
                    waiter,
                    &TriggerWork {
                        event_id: event_id.clone(),
                        channel_id: channel_id.clone(),
                        channel_display: channel_display.to_owned(),
                        reply_to_event_id: reply_to_event_id.clone(),
                        nostr_body: trigger.request().to_owned(),
                    },
                )
            }
            crate::relay::IngestAction::Edit { event_id, .. }
            | crate::relay::IngestAction::Delete { event_id, .. } => {
                self.repository
                    .mark_event_processed(event_id)
                    .map_err(ActorError::Repository)?;
                Ok(TriggerOutcome::Declined)
            }
        }
    }

    /// Start or ask the oldest queued turn that has no open sibling.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence, pane allocation, or Kelpie fails.
    pub fn resume_queued(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &AdoptedWaiter<'_>,
    ) -> Result<Option<TriggerOutcome>, ActorError<R::Error>> {
        let sessions = self
            .repository
            .sessions_with_pending_turns()
            .map_err(ActorError::Repository)?;
        for session in sessions {
            if session.bot_id != *self.bot.id() {
                continue;
            }
            let turns = self
                .repository
                .turns_for_session(&session.bot_id, &session.channel_id)
                .map_err(ActorError::Repository)?;
            if turns.iter().any(|turn| turn.state == TurnState::Open) {
                continue;
            }
            let Some(queued) = turns
                .iter()
                .filter(|turn| turn.state == TurnState::Queued)
                .min_by_key(|turn| turn.sequence)
            else {
                continue;
            };
            let body = self
                .repository
                .indexed_event(&queued.event_id)
                .map_err(ActorError::Repository)?
                .and_then(|event| botserver_domain::TriggerMatch::from_body(&event.content))
                .map(|trigger| trigger.request().to_owned())
                .filter(|content| !content.is_empty());
            let Some(body) = body else {
                continue;
            };
            match self.ask_oldest_queued(kelpie, waiter, &session.channel_id, &body) {
                Ok(()) => return Ok(Some(TriggerOutcome::Asked)),
                Err(_) => continue,
            }
        }
        Ok(None)
    }

    fn should_ask_event(
        &self,
        channel_id: &str,
        event_id: &EventId,
    ) -> Result<bool, ActorError<R::Error>> {
        let turns = self
            .repository
            .turns_for_session(self.bot.id(), channel_id)
            .map_err(ActorError::Repository)?;
        if turns.iter().any(|turn| turn.state == TurnState::Open) {
            return Ok(false);
        }
        Ok(turns
            .iter()
            .filter(|turn| turn.state == TurnState::Queued)
            .min_by_key(|turn| turn.sequence)
            .is_some_and(|turn| turn.event_id == *event_id))
    }

    fn ask_oldest_queued(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &AdoptedWaiter<'_>,
        channel_id: &str,
        nostr_body: &str,
    ) -> Result<(), ActorError<R::Error>> {
        let mut session = self
            .repository
            .session(self.bot.id(), channel_id)
            .map_err(ActorError::Repository)?
            .ok_or(ActorError::UnnameableSession)?;
        if session.occupant_logical_id.is_none() {
            let occupant = self.start_occupant(kelpie, waiter, &session)?;
            session.occupant_logical_id = Some(occupant.logical_agent_id().to_owned());
            self.repository
                .save_session(&session)
                .map_err(ActorError::Repository)?;
        }
        let queued = self
            .repository
            .turns_for_session(self.bot.id(), channel_id)
            .map_err(ActorError::Repository)?
            .into_iter()
            .filter(|turn| turn.state == TurnState::Queued)
            .min_by_key(|turn| turn.sequence)
            .ok_or(ActorError::UnnameableSession)?;
        let idempotency_key = format!("{}:{}", queued.event_id.as_str(), queued.sequence);
        let receipt = waiter
            .ask_named(
                &session.session_name,
                session.occupant_logical_id.as_deref(),
                nostr_body,
                &idempotency_key,
            )
            .map_err(ActorError::Kelpie)?;
        match receipt.delivery() {
            AskDelivery::Accepted | AskDelivery::Unknown => {}
            delivery @ (AskDelivery::Rejected | AskDelivery::TargetUnavailable) => {
                return Err(ActorError::AskNotDelivered(delivery));
            }
        }
        self.repository
            .open_next_turn(self.bot.id(), channel_id, receipt.message_id())
            .map_err(ActorError::Repository)?
            .ok_or_else(|| ActorError::TurnNotOpened {
                ask_id: receipt.message_id().to_owned(),
            })?;
        Ok(())
    }

    fn start_occupant(
        &self,
        kelpie: &KelpieClient,
        waiter: &AdoptedWaiter<'_>,
        session: &SessionRecord,
    ) -> Result<crate::StartedOccupant, ActorError<R::Error>> {
        let pane = self
            .panes
            .allocate(&session.session_name, self.bot.corpus_path())
            .map_err(|error| ActorError::Pane(error.to_string()))?;
        kelpie
            .start_occupant(
                &OccupantLaunch {
                    name: session.session_name.clone(),
                    pane_id: pane.pane_id,
                    terminal_id: pane.terminal_id,
                    backend: self.bot.occupant_kind().to_owned(),
                    cwd: self.bot.corpus_path().to_path_buf(),
                    timeout_ms: OCCUPANT_START_TIMEOUT_MS,
                },
                OCCUPANT_BOOTSTRAP,
                Some(waiter.identity().logical_agent_id()),
            )
            .map_err(ActorError::Kelpie)
    }

    pub(crate) fn ensure_session(
        &mut self,
        channel_id: &str,
        channel_display: &str,
    ) -> Result<SessionRecord, ActorError<R::Error>> {
        if let Some(session) = self
            .repository
            .session(self.bot.id(), channel_id)
            .map_err(ActorError::Repository)?
        {
            return Ok(session);
        }
        let mut name_error = None;
        let name = SessionName::from_bot_and_channel(
            self.bot.id(),
            channel_id,
            channel_display,
            |candidate| match self.repository.session_by_name(candidate) {
                Ok(existing) => existing.is_some(),
                Err(error) => {
                    name_error = Some(error);
                    true
                }
            },
        );
        if let Some(error) = name_error {
            return Err(ActorError::Repository(error));
        }
        let name = name.ok_or(ActorError::UnnameableSession)?;
        let session = SessionRecord {
            bot_id: self.bot.id().clone(),
            channel_id: channel_id.to_owned(),
            session_name: name.as_str().to_owned(),
            occupant_logical_id: None,
            renew_id: None,
        };
        self.repository
            .save_session(&session)
            .map_err(ActorError::Repository)?;
        Ok(session)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use rusqlite::Connection;
    use serde_json::Value;

    use super::*;
    use crate::sqlite::SqliteRepository;
    use crate::{CommandOutput, CommandRunner, IndexedRelayEvent, WAITER_NAME};

    #[derive(Debug)]
    struct FakePanes {
        calls: Mutex<Vec<(String, PathBuf)>>,
    }

    impl OccupantPaneAllocator for Arc<FakePanes> {
        type Error = io::Error;

        fn allocate(&self, session_name: &str, cwd: &Path) -> Result<OccupantPane, Self::Error> {
            self.calls
                .lock()
                .expect("calls")
                .push((session_name.to_owned(), cwd.to_path_buf()));
            Ok(OccupantPane {
                pane_id: "w2:p1".to_owned(),
                terminal_id: "term-9".to_owned(),
            })
        }
    }

    #[derive(Debug)]
    struct FakeRunner {
        calls: Mutex<Vec<(Vec<String>, Vec<u8>)>>,
        outputs: Mutex<VecDeque<CommandOutput>>,
    }

    impl CommandRunner for Arc<FakeRunner> {
        fn run(&self, arguments: &[String], stdin: &[u8]) -> io::Result<CommandOutput> {
            self.calls
                .lock()
                .expect("calls")
                .push((arguments.to_vec(), stdin.to_vec()));
            self.outputs
                .lock()
                .expect("outputs")
                .pop_front()
                .ok_or_else(|| io::Error::other("missing fake output"))
        }
    }

    fn success(result: &Value) -> CommandOutput {
        CommandOutput {
            success: true,
            status: "exit status: 0".to_owned(),
            stdout: serde_json::to_vec(&serde_json::json!({
                "id": "request-id",
                "result": result
            }))
            .expect("json"),
            stderr: Vec::new(),
        }
    }

    fn adopt() -> CommandOutput {
        success(&serde_json::json!({
            "logical_agent_id": "waiter-agent",
            "incarnation_id": "waiter-incarnation",
            "operation_id": "adopt-operation",
            "outcome": "succeeded"
        }))
    }

    fn start() -> CommandOutput {
        success(&serde_json::json!({
            "logical_agent_id": "occupant-agent",
            "incarnation_id": "occupant-incarnation",
            "runtime_start": {
                "operation_id": "start-operation",
                "outcome": "succeeded"
            },
            "initial_message": {
                "message_id": "tell-id",
                "operation_id": "tell-operation",
                "outcome": "accepted"
            }
        }))
    }

    fn whoami() -> CommandOutput {
        success(&serde_json::json!({
            "logical_agent_id": "occupant-agent",
            "incarnation_id": "occupant-incarnation",
            "public_name": "bot-foobar"
        }))
    }

    fn asked(message_id: &str) -> CommandOutput {
        success(&serde_json::json!({
            "message_id": message_id,
            "operation_id": "ask-operation",
            "recipient": "occupant-agent",
            "delivery_outcome": "accepted"
        }))
    }

    fn event_id(character: char) -> EventId {
        EventId::parse_hex(&character.to_string().repeat(64)).expect("event")
    }

    fn bot() -> Bot {
        Bot::new(
            botserver_domain::BotId::new("bot").expect("id"),
            PathBuf::from("/corpus"),
            "opencode",
        )
        .expect("bot")
    }

    fn work(character: char, body: &str, reply: Option<char>) -> TriggerWork {
        TriggerWork {
            event_id: event_id(character),
            channel_id: "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
            channel_display: "Foobar".to_owned(),
            reply_to_event_id: reply.map(event_id),
            nostr_body: body.to_owned(),
        }
    }

    fn actor(
        outputs: impl IntoIterator<Item = CommandOutput>,
    ) -> (
        BotActor<SqliteRepository, Arc<FakePanes>>,
        KelpieClient,
        Arc<FakeRunner>,
        Arc<FakePanes>,
    ) {
        let runner = Arc::new(FakeRunner {
            calls: Mutex::new(Vec::new()),
            outputs: Mutex::new(outputs.into_iter().collect()),
        });
        let panes = Arc::new(FakePanes {
            calls: Mutex::new(Vec::new()),
        });
        let kelpie = KelpieClient::with_runner(Arc::clone(&runner));
        let repository = SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
            .expect("repository");
        (
            BotActor::new(bot(), repository, Arc::clone(&panes)),
            kelpie,
            runner,
            panes,
        )
    }

    #[test]
    fn first_trigger_starts_then_asks_and_stores_reply_to() {
        let (mut actor, kelpie, runner, panes) =
            actor([adopt(), start(), whoami(), asked("ask-1")]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let trigger = work('a', "@daniel bot: hello", Some('c'));

        assert_eq!(
            actor
                .handle_trigger(&kelpie, &waiter, &trigger)
                .expect("handle"),
            TriggerOutcome::Asked
        );

        let session = actor
            .repository
            .session(actor.bot.id(), &trigger.channel_id)
            .expect("session")
            .expect("bound");
        assert_eq!(session.session_name, "bot-foobar");
        assert_eq!(
            session.occupant_logical_id.as_deref(),
            Some("occupant-agent")
        );
        let turns = actor
            .repository
            .turns_for_session(actor.bot.id(), &trigger.channel_id)
            .expect("turns");
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].state, TurnState::Open);
        assert_eq!(turns[0].ask_id.as_deref(), Some("ask-1"));
        assert_eq!(turns[0].reply_to_event_id, trigger.reply_to_event_id);
        assert_eq!(
            panes.calls.lock().expect("pane calls").as_slice(),
            &[("bot-foobar".to_owned(), PathBuf::from("/corpus"))]
        );
        let calls = runner.calls.lock().expect("calls");
        assert_eq!(calls[1].0[1], "start");
        assert!(calls[1]
            .0
            .windows(2)
            .any(|pair| pair == ["--sender-id", "waiter-agent"]));
        assert_eq!(calls[1].1, OCCUPANT_BOOTSTRAP.as_bytes());
        assert_eq!(calls[3].0[1], "ask");
        assert_eq!(
            calls[3].0[calls[3]
                .0
                .iter()
                .position(|arg| arg == "--idempotency-key")
                .expect("key")
                + 1],
            format!("{}:1", trigger.event_id.as_str())
        );
        assert_eq!(calls[3].1, trigger.nostr_body.as_bytes());
        assert_eq!(waiter.identity().logical_agent_id(), "waiter-agent");
        assert_eq!(WAITER_NAME, "botserver");
    }

    #[test]
    fn later_trigger_asks_the_same_occupant_without_starting() {
        let (mut actor, kelpie, runner, panes) = actor([
            adopt(),
            start(),
            whoami(),
            asked("ask-1"),
            whoami(),
            asked("ask-2"),
        ]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        actor
            .handle_trigger(&kelpie, &waiter, &work('a', "bot: first", None))
            .expect("first");
        actor
            .repository
            .set_turn_state("ask-1", TurnState::Posted)
            .expect("posted");

        assert_eq!(
            actor
                .handle_trigger(&kelpie, &waiter, &work('b', "bot: second", None))
                .expect("second"),
            TriggerOutcome::Asked
        );
        assert_eq!(panes.calls.lock().expect("pane calls").len(), 1);
        let calls = runner.calls.lock().expect("calls");
        assert_eq!(calls.iter().filter(|call| call.0[1] == "start").count(), 1);
        assert_eq!(calls.iter().filter(|call| call.0[1] == "ask").count(), 2);
        assert_eq!(calls.last().expect("ask").1, b"bot: second");
    }

    #[test]
    fn open_turn_queues_without_a_second_ask() {
        let (mut actor, kelpie, runner, _panes) =
            actor([adopt(), start(), whoami(), asked("ask-1")]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        actor
            .handle_trigger(&kelpie, &waiter, &work('a', "bot: first", None))
            .expect("first");

        assert_eq!(
            actor
                .handle_trigger(&kelpie, &waiter, &work('b', "bot: second", None))
                .expect("queued"),
            TriggerOutcome::Queued
        );
        let turns = actor
            .repository
            .turns_for_session(actor.bot.id(), "ab12cd34-5678-90ab-cdef-0123456789ab")
            .expect("turns");
        assert_eq!(turns[1].state, TurnState::Queued);
        assert_eq!(turns[1].ask_id, None);
        assert_eq!(
            runner
                .calls
                .lock()
                .expect("calls")
                .iter()
                .filter(|call| call.0[1] == "ask")
                .count(),
            1
        );
    }

    #[test]
    fn resume_queued_starts_an_occupant_for_bootstrapping() {
        let (mut actor, kelpie, runner, _panes) =
            actor([adopt(), start(), whoami(), asked("ask-1")]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let trigger = work('a', "bot: hello", None);
        actor
            .ensure_session(&trigger.channel_id, &trigger.channel_display)
            .expect("session");
        actor
            .repository
            .index_event(
                &IndexedRelayEvent {
                    event_id: trigger.event_id.clone(),
                    author_pubkey: "b".repeat(64),
                    created_at: 1,
                    kind: 9,
                    content: trigger.nostr_body.clone(),
                    tags_json: "[]".to_owned(),
                    channel_id: Some(trigger.channel_id.clone()),
                    target_event_id: None,
                },
                false,
            )
            .expect("index");
        actor
            .repository
            .enqueue_unprocessed_turn(&NewTurn {
                bot_id: actor.bot.id().clone(),
                channel_id: trigger.channel_id.clone(),
                event_id: trigger.event_id.clone(),
                reply_to_event_id: None,
            })
            .expect("enqueue");

        assert_eq!(
            actor.resume_queued(&kelpie, &waiter).expect("resume"),
            Some(TriggerOutcome::Asked)
        );
        assert_eq!(runner.calls.lock().expect("calls")[1].0[1], "start");
    }

    #[test]
    fn resume_queued_skips_a_session_without_indexed_body() {
        let (mut actor, kelpie, _runner, _panes) = actor([adopt()]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let trigger = work('a', "bot: hello", None);
        actor
            .ensure_session(&trigger.channel_id, &trigger.channel_display)
            .expect("session");
        actor
            .repository
            .enqueue_unprocessed_turn(&NewTurn {
                bot_id: actor.bot.id().clone(),
                channel_id: trigger.channel_id.clone(),
                event_id: trigger.event_id.clone(),
                reply_to_event_id: None,
            })
            .expect("enqueue");

        assert_eq!(actor.resume_queued(&kelpie, &waiter).expect("skip"), None);
    }

    #[test]
    fn ingest_turn_candidate_uses_the_actor_path() {
        let (mut actor, kelpie, _runner, _panes) =
            actor([adopt(), start(), whoami(), asked("ask-1")]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let trigger = work('a', "@daniel bot: hello", Some('c'));
        let action = crate::relay::IngestAction::TurnCandidate {
            event_id: trigger.event_id.clone(),
            channel_id: trigger.channel_id.clone(),
            reply_to_event_id: trigger.reply_to_event_id.clone(),
            trigger: botserver_domain::TriggerMatch::parse(
                "operator",
                ["operator"],
                "@daniel bot: hello",
            )
            .expect("trigger"),
        };

        assert_eq!(
            actor
                .handle_ingest(&kelpie, &waiter, &action, &trigger.channel_display)
                .expect("ingest"),
            TriggerOutcome::Asked
        );
    }

    #[test]
    fn empty_trigger_request_is_not_asked() {
        let (mut actor, kelpie, runner, _panes) = actor([adopt()]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let error = actor
            .handle_trigger(&kelpie, &waiter, &work('a', "   ", None))
            .expect_err("empty");
        assert!(error.to_string().contains("trigger request is empty"));
        assert!(runner
            .calls
            .lock()
            .expect("calls")
            .iter()
            .all(|call| call.0[1] != "start"));
    }

    #[test]
    fn ingest_edit_is_acknowledged_without_asking() {
        let (mut actor, kelpie, runner, _panes) = actor([adopt()]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let edit_id = event_id('a');
        assert_eq!(
            actor
                .handle_ingest(
                    &kelpie,
                    &waiter,
                    &crate::relay::IngestAction::Edit {
                        event_id: edit_id.clone(),
                        target_event_id: event_id('b'),
                        replacement: None,
                    },
                    "foobar",
                )
                .expect("declined"),
            TriggerOutcome::Declined
        );
        assert!(actor
            .repository
            .event_processed(&edit_id)
            .expect("processed"));
        assert!(runner
            .calls
            .lock()
            .expect("calls")
            .iter()
            .all(|call| call.0[1] != "ask"));
    }
}
