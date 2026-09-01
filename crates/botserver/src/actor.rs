//! Per-bot actor: start a corpus occupant, then ask on each trigger.

use std::fmt;
use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use botserver_domain::{Bot, EventId, SessionName};

use crate::snapshot::{place_snapshot_relpath, refresh_place_snapshot, render_place_snapshot};
use crate::{
    occupant_bootstrap, AdoptedWaiter, AskDelivery, HostRepository, KelpieClient, KelpieError,
    NewTurn, OccupantLaunch, SessionRecord, TurnState,
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
    /// The action was acknowledged without changing turns.
    Declined,
    /// Queued or open work was cancelled and not replaced.
    Cancelled,
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
    /// The host could not write the channel snapshot.
    Snapshot(io::Error),
    /// A live occupant under the session name is not the recorded agent.
    OccupantTwin { recorded: String, live: String },
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
            Self::Snapshot(error) => write!(formatter, "place snapshot failed: {error}"),
            Self::OccupantTwin { recorded, live } => write!(
                formatter,
                "session occupant {live} is not the recorded logical agent {recorded}"
            ),
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
            Self::Snapshot(error) => Some(error),
            Self::Pane(_)
            | Self::UnnameableSession
            | Self::AskNotDelivered(_)
            | Self::EmptyAskBody
            | Self::TurnNotOpened { .. }
            | Self::OccupantTwin { .. } => None,
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
    /// Turn candidates start or ask the occupant. Edits and deletes cancel
    /// unclaimed in-flight work and may open a replacement turn.
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
            crate::relay::IngestAction::Edit {
                event_id,
                target_event_id,
                replacement,
            } => self.handle_edit(
                kelpie,
                waiter,
                event_id,
                target_event_id,
                replacement.as_ref(),
            ),
            crate::relay::IngestAction::Delete {
                event_id,
                target_event_id,
            } => {
                self.abandon_unclaimed(kelpie, waiter, event_id, target_event_id, "trigger deleted")
            }
        }
    }

    /// Drain the next queued turn after an in-flight turn is posted.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence, pane allocation, or Kelpie fails.
    pub fn handle_turn_completed(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &AdoptedWaiter<'_>,
    ) -> Result<Option<TriggerOutcome>, ActorError<R::Error>> {
        self.resume_queued(kelpie, waiter)
    }

    /// Rebind gone occupants that still owe an open ask.
    ///
    /// Recovery continues the recorded logical agent. It does not mint a twin
    /// and does not send a second ask for that Turn.
    ///
    /// # Errors
    ///
    /// Returns an error when pane allocation or Kelpie start fails.
    pub fn recover_open_occupants(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &AdoptedWaiter<'_>,
    ) -> Result<usize, ActorError<R::Error>> {
        let sessions = self
            .repository
            .sessions_with_pending_turns()
            .map_err(ActorError::Repository)?;
        let mut continued = 0;
        let mut first_error = None;
        for session in sessions {
            if session.bot_id != *self.bot.id() {
                continue;
            }
            match self.recover_open_session(kelpie, waiter, &session) {
                Ok(true) => continued += 1,
                Ok(false) => {}
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(continued),
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
        let mut first_error = self.recover_open_occupants(kelpie, waiter).err();
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
            let body = self.queued_ask_body(&queued.event_id)?;
            let Some(body) = body else {
                continue;
            };
            match self.ask_oldest_queued(kelpie, waiter, &session.channel_id, &body) {
                Ok(()) => return Ok(Some(TriggerOutcome::Asked)),
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }

    fn handle_edit(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &AdoptedWaiter<'_>,
        event_id: &EventId,
        target_event_id: &EventId,
        replacement: Option<&botserver_domain::TriggerMatch>,
    ) -> Result<TriggerOutcome, ActorError<R::Error>> {
        let Some(request) = replacement
            .map(botserver_domain::TriggerMatch::request)
            .filter(|request| !request.is_empty())
        else {
            return self.abandon_unclaimed(
                kelpie,
                waiter,
                event_id,
                target_event_id,
                "trigger edited",
            );
        };
        let Some(active) = self
            .repository
            .active_turn_for_event(target_event_id)
            .map_err(ActorError::Repository)?
        else {
            self.repository
                .mark_event_processed(event_id)
                .map_err(ActorError::Repository)?;
            return Ok(TriggerOutcome::Declined);
        };
        let Some(replaced) = self
            .repository
            .replace_unclaimed_turn(&NewTurn {
                bot_id: self.bot.id().clone(),
                channel_id: active.channel_id.clone(),
                event_id: target_event_id.clone(),
                reply_to_event_id: active.reply_to_event_id.clone(),
            })
            .map_err(ActorError::Repository)?
        else {
            self.repository
                .mark_event_processed(event_id)
                .map_err(ActorError::Repository)?;
            return Ok(TriggerOutcome::Declined);
        };
        if let Some(ask_id) = replaced.cancelled.ask_id.as_deref() {
            waiter
                .cancel(ask_id, "trigger edited")
                .map_err(ActorError::Kelpie)?;
        }
        self.repository
            .mark_event_processed(event_id)
            .map_err(ActorError::Repository)?;
        if self.should_ask_event(&replaced.queued.channel_id, &replaced.queued.event_id)? {
            self.ask_oldest_queued(kelpie, waiter, &replaced.queued.channel_id, request)?;
            return Ok(TriggerOutcome::Asked);
        }
        match self.resume_queued(kelpie, waiter)? {
            Some(outcome) => Ok(outcome),
            None => Ok(TriggerOutcome::Queued),
        }
    }

    fn abandon_unclaimed(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &AdoptedWaiter<'_>,
        event_id: &EventId,
        target_event_id: &EventId,
        reason: &str,
    ) -> Result<TriggerOutcome, ActorError<R::Error>> {
        let Some(cancelled) = self
            .repository
            .cancel_unclaimed_turn(target_event_id)
            .map_err(ActorError::Repository)?
        else {
            self.repository
                .mark_event_processed(event_id)
                .map_err(ActorError::Repository)?;
            return Ok(TriggerOutcome::Declined);
        };
        if let Some(ask_id) = cancelled.ask_id.as_deref() {
            waiter.cancel(ask_id, reason).map_err(ActorError::Kelpie)?;
        }
        self.repository
            .mark_event_processed(event_id)
            .map_err(ActorError::Repository)?;
        self.resume_queued(kelpie, waiter)?;
        Ok(TriggerOutcome::Cancelled)
    }

    fn queued_ask_body(&self, event_id: &EventId) -> Result<Option<String>, ActorError<R::Error>> {
        Ok(self
            .repository
            .latest_body_for_event(event_id)
            .map_err(ActorError::Repository)?
            .and_then(|content| botserver_domain::TriggerMatch::from_body(&content))
            .map(|trigger| trigger.request().to_owned())
            .filter(|content| !content.is_empty()))
    }

    fn recover_open_session(
        &self,
        kelpie: &KelpieClient,
        waiter: &AdoptedWaiter<'_>,
        session: &SessionRecord,
    ) -> Result<bool, ActorError<R::Error>> {
        let Some(logical_id) = session.occupant_logical_id.as_deref() else {
            return Ok(false);
        };
        let turns = self
            .repository
            .turns_for_session(&session.bot_id, &session.channel_id)
            .map_err(ActorError::Repository)?;
        if !turns.iter().any(|turn| turn.state == TurnState::Open) {
            return Ok(false);
        }
        match kelpie.occupant_whoami(&session.session_name) {
            Ok(live) if live.logical_agent_id() == logical_id => Ok(false),
            Ok(live) => Err(ActorError::OccupantTwin {
                recorded: logical_id.to_owned(),
                live: live.logical_agent_id().to_owned(),
            }),
            Err(KelpieError::TargetUnavailable) => {
                let snapshot_relpath = self.refresh_snapshot(session)?;
                self.start_occupant(
                    kelpie,
                    waiter,
                    session,
                    &snapshot_relpath,
                    Some(logical_id),
                )?;
                Ok(true)
            }
            Err(error) => Err(ActorError::Kelpie(error)),
        }
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
        let snapshot_relpath = self.refresh_snapshot(&session)?;
        if session.occupant_logical_id.is_none() {
            let occupant =
                self.start_occupant(kelpie, waiter, &session, &snapshot_relpath, None)?;
            session.occupant_logical_id = Some(occupant.logical_agent_id().to_owned());
            self.repository
                .save_session(&session)
                .map_err(ActorError::Repository)?;
            self.try_arm_renew(
                kelpie,
                occupant.logical_agent_id(),
                occupant.incarnation_id(),
                &snapshot_relpath,
                &mut session,
            )?;
        } else if session.renew_id.is_none() {
            if let Ok((logical_id, incarnation_id)) = waiter.occupant_ids(
                &session.session_name,
                session.occupant_logical_id.as_deref(),
            ) {
                self.try_arm_renew(
                    kelpie,
                    &logical_id,
                    &incarnation_id,
                    &snapshot_relpath,
                    &mut session,
                )?;
            }
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
        snapshot_relpath: &str,
        continue_as: Option<&str>,
    ) -> Result<crate::StartedOccupant, ActorError<R::Error>> {
        let pane = self
            .panes
            .allocate(&session.session_name, self.bot.corpus_path())
            .map_err(|error| ActorError::Pane(error.to_string()))?;
        let bootstrap = occupant_bootstrap(snapshot_relpath);
        kelpie
            .start_occupant(
                &OccupantLaunch {
                    name: session.session_name.clone(),
                    pane_id: pane.pane_id,
                    terminal_id: pane.terminal_id,
                    backend: self.bot.occupant_kind().to_owned(),
                    cwd: self.bot.corpus_path().to_path_buf(),
                    timeout_ms: OCCUPANT_START_TIMEOUT_MS,
                    logical_agent_id: continue_as.map(str::to_owned),
                },
                &bootstrap,
                Some(waiter.identity().logical_agent_id()),
            )
            .map_err(ActorError::Kelpie)
    }

    fn try_arm_renew(
        &mut self,
        kelpie: &KelpieClient,
        logical_id: &str,
        incarnation_id: &str,
        snapshot_relpath: &str,
        session: &mut SessionRecord,
    ) -> Result<(), ActorError<R::Error>> {
        if let Ok(renew_id) =
            kelpie.arm_occupant_renew(logical_id, incarnation_id, snapshot_relpath)
        {
            session.renew_id = Some(renew_id);
            self.repository
                .save_session(session)
                .map_err(ActorError::Repository)?;
        }
        Ok(())
    }

    fn refresh_snapshot(&self, session: &SessionRecord) -> Result<String, ActorError<R::Error>> {
        let relpath = place_snapshot_relpath(&session.session_name).ok_or_else(|| {
            ActorError::Snapshot(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session name is not a safe snapshot filename",
            ))
        })?;
        let events = self
            .repository
            .indexed_events_for_channel(&session.channel_id)
            .map_err(ActorError::Repository)?;
        let markdown = render_place_snapshot(
            &session.session_name,
            &session.channel_id,
            unix_now().map_err(ActorError::Snapshot)?,
            &events,
        );
        refresh_place_snapshot(self.bot.corpus_path(), &session.session_name, &markdown)
            .map_err(ActorError::Snapshot)?;
        Ok(relpath)
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

fn unix_now() -> io::Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    i64::try_from(elapsed.as_secs()).map_err(|_| io::Error::other("unix time does not fit i64"))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use rusqlite::Connection;
    use serde_json::Value;

    use super::*;
    use crate::sqlite::SqliteRepository;
    use crate::{occupant_bootstrap, CommandOutput, CommandRunner, IndexedRelayEvent, WAITER_NAME};

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

    fn whoami_other() -> CommandOutput {
        success(&serde_json::json!({
            "logical_agent_id": "twin-agent",
            "incarnation_id": "twin-incarnation",
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

    fn renewed() -> CommandOutput {
        success(&serde_json::json!({
            "renew_id": "renew-id",
            "recipient": "occupant-agent",
            "recipient_incarnation": "occupant-incarnation",
            "scheduled_at_ms": 1,
            "on_timeout": "abort",
            "phase": "scheduled",
            "every_ms": 2_700_000
        }))
    }

    fn cancelled() -> CommandOutput {
        success(&serde_json::json!({}))
    }

    fn failure(class: &str, message: &str) -> CommandOutput {
        CommandOutput {
            success: false,
            status: "exit status: 1".to_owned(),
            stdout: serde_json::to_vec(&serde_json::json!({
                "id": "request-id",
                "error": {"class": class, "message": message}
            }))
            .expect("json"),
            stderr: b"kelpie: request failed".to_vec(),
        }
    }

    fn event_id(character: char) -> EventId {
        EventId::parse_hex(&character.to_string().repeat(64)).expect("event")
    }

    fn temp_corpus() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "botserver-actor-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("corpus");
        path
    }

    fn bot() -> Bot {
        Bot::new(
            botserver_domain::BotId::new("bot").expect("id"),
            temp_corpus(),
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
            actor([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
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
        assert_eq!(session.renew_id.as_deref(), Some("renew-id"));
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
            &[(
                "bot-foobar".to_owned(),
                actor.bot().corpus_path().to_path_buf()
            )]
        );
        let snapshot = actor
            .bot()
            .corpus_path()
            .join(".botserver/places/bot-foobar.md");
        assert!(snapshot.exists());
        assert!(
            std::fs::read_to_string(actor.bot().corpus_path().join("startup.md"))
                .expect("startup")
                .contains(".botserver/places/<your public Kelpie name>.md")
        );
        let calls = runner.calls.lock().expect("calls");
        assert_eq!(calls[1].0[1], "start");
        assert!(calls[1]
            .0
            .windows(2)
            .any(|pair| pair == ["--sender-id", "waiter-agent"]));
        assert_eq!(
            calls[1].1,
            occupant_bootstrap(".botserver/places/bot-foobar.md").as_bytes()
        );
        assert_eq!(calls[2].0[1], "renew");
        assert_eq!(calls[4].0[1], "ask");
        assert_eq!(
            calls[4].0[calls[4]
                .0
                .iter()
                .position(|arg| arg == "--idempotency-key")
                .expect("key")
                + 1],
            format!("{}:1", trigger.event_id.as_str())
        );
        assert_eq!(calls[4].1, trigger.nostr_body.as_bytes());
        assert_eq!(waiter.identity().logical_agent_id(), "waiter-agent");
        assert_eq!(WAITER_NAME, "botserver");
    }

    fn ask_count(runner: &Arc<FakeRunner>) -> usize {
        runner
            .calls
            .lock()
            .expect("calls")
            .iter()
            .filter(|call| call.0[1] == "ask")
            .count()
    }

    fn start_count(runner: &Arc<FakeRunner>) -> usize {
        runner
            .calls
            .lock()
            .expect("calls")
            .iter()
            .filter(|call| call.0[1] == "start")
            .count()
    }

    fn continued_starts(runner: &Arc<FakeRunner>) -> Vec<Vec<String>> {
        runner
            .calls
            .lock()
            .expect("calls")
            .iter()
            .filter(|call| {
                call.0[1] == "start"
                    && call
                        .0
                        .windows(2)
                        .any(|pair| pair == ["--logical-id", "occupant-agent"])
            })
            .map(|call| call.0.clone())
            .collect()
    }

    #[test]
    fn failed_renew_still_asks() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            start(),
            failure("conflict", "incarnation already has a renew"),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let trigger = work('a', "bot: hello", None);

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
        assert_eq!(session.renew_id, None);
        let turns = actor
            .repository
            .turns_for_session(actor.bot.id(), &trigger.channel_id)
            .expect("turns");
        assert_eq!(turns[0].state, TurnState::Open);
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
    fn later_trigger_asks_the_same_occupant_without_starting() {
        let (mut actor, kelpie, runner, panes) = actor([
            adopt(),
            start(),
            renewed(),
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
        assert_eq!(calls.iter().filter(|call| call.0[1] == "renew").count(), 1);
        assert_eq!(calls.iter().filter(|call| call.0[1] == "ask").count(), 2);
        assert_eq!(calls.last().expect("ask").1, b"bot: second");
    }

    #[test]
    fn open_turn_queues_without_a_second_ask() {
        let (mut actor, kelpie, runner, _panes) =
            actor([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
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
    fn gone_open_occupant_is_continued_without_a_second_ask() {
        let (mut actor, kelpie, runner, panes) = actor([
            adopt(),
            start(),
            whoami(),
            asked("ask-1"),
            failure("target_unavailable", "no ready occupant"),
            start(),
        ]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let trigger = work('a', "bot: hello", None);
        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("asked");

        assert_eq!(
            actor
                .recover_open_occupants(&kelpie, &waiter)
                .expect("recover"),
            1
        );

        let session = actor
            .repository
            .session(actor.bot.id(), &trigger.channel_id)
            .expect("session")
            .expect("bound");
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
        assert_eq!(ask_count(&runner), 1);
        assert_eq!(start_count(&runner), 2);
        assert_eq!(panes.calls.lock().expect("pane calls").len(), 2);
        let continued = continued_starts(&runner);
        assert_eq!(continued.len(), 1);
        assert!(continued[0]
            .windows(2)
            .any(|pair| pair == ["--name", "bot-foobar"]));
    }

    #[test]
    fn ready_open_occupant_is_left_bound_without_asking() {
        let (mut actor, kelpie, runner, panes) =
            actor([adopt(), start(), whoami(), asked("ask-1"), whoami()]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        actor
            .handle_trigger(&kelpie, &waiter, &work('a', "bot: hello", None))
            .expect("asked");

        assert_eq!(
            actor
                .recover_open_occupants(&kelpie, &waiter)
                .expect("recover"),
            0
        );
        assert_eq!(ask_count(&runner), 1);
        assert_eq!(start_count(&runner), 1);
        assert_eq!(panes.calls.lock().expect("pane calls").len(), 1);
        assert!(continued_starts(&runner).is_empty());
    }

    #[test]
    fn recovery_refuses_a_namesake_twin() {
        let (mut actor, kelpie, runner, _panes) =
            actor([adopt(), start(), whoami(), asked("ask-1"), whoami_other()]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        actor
            .handle_trigger(&kelpie, &waiter, &work('a', "bot: hello", None))
            .expect("asked");

        let error = actor
            .recover_open_occupants(&kelpie, &waiter)
            .expect_err("twin");
        assert!(error.to_string().contains("twin-agent"));
        assert_eq!(ask_count(&runner), 1);
        assert_eq!(start_count(&runner), 1);
        assert!(continued_starts(&runner).is_empty());
    }

    #[test]
    fn resume_queued_recovers_a_gone_open_occupant_without_asking() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            start(),
            whoami(),
            asked("ask-1"),
            failure("target_unavailable", "no ready occupant"),
            start(),
        ]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        actor
            .handle_trigger(&kelpie, &waiter, &work('a', "bot: hello", None))
            .expect("asked");

        assert_eq!(actor.resume_queued(&kelpie, &waiter).expect("resume"), None);
        assert_eq!(ask_count(&runner), 1);
        assert_eq!(start_count(&runner), 2);
        assert_eq!(continued_starts(&runner).len(), 1);
    }

    #[test]
    fn whoami_invalid_receipt_does_not_start_a_replacement() {
        let (mut actor, kelpie, runner, panes) = actor([
            adopt(),
            start(),
            whoami(),
            asked("ask-1"),
            success(&serde_json::json!({ "incarnation_id": "broken" })),
        ]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        actor
            .handle_trigger(&kelpie, &waiter, &work('a', "bot: hello", None))
            .expect("asked");

        actor
            .recover_open_occupants(&kelpie, &waiter)
            .expect_err("invalid whoami");
        assert_eq!(start_count(&runner), 1);
        assert_eq!(panes.calls.lock().expect("pane calls").len(), 1);
        assert!(continued_starts(&runner).is_empty());
    }

    #[test]
    fn whoami_rejection_does_not_start_a_replacement() {
        let (mut actor, kelpie, runner, panes) = actor([
            adopt(),
            start(),
            whoami(),
            asked("ask-1"),
            failure("rejected", "kelpie daemon unavailable"),
        ]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        actor
            .handle_trigger(&kelpie, &waiter, &work('a', "bot: hello", None))
            .expect("asked");

        actor
            .recover_open_occupants(&kelpie, &waiter)
            .expect_err("rejected whoami");
        assert_eq!(start_count(&runner), 1);
        assert_eq!(panes.calls.lock().expect("pane calls").len(), 1);
        assert!(continued_starts(&runner).is_empty());
    }

    #[test]
    fn resume_queued_asks_later_channel_when_open_recovery_fails() {
        let first_channel = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        let second_channel = "ab12cd34-5678-90ab-cdef-0123456789ab";
        let (mut actor, kelpie, runner, _panes) =
            actor([adopt(), whoami_other(), whoami(), asked("ask-2")]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        actor
            .repository
            .save_session(&crate::SessionRecord {
                bot_id: actor.bot.id().clone(),
                channel_id: first_channel.to_owned(),
                session_name: "bot-aaa".to_owned(),
                occupant_logical_id: Some("occupant-agent".to_owned()),
                renew_id: None,
            })
            .expect("first session");
        actor
            .repository
            .enqueue_unprocessed_turn(&NewTurn {
                bot_id: actor.bot.id().clone(),
                channel_id: first_channel.to_owned(),
                event_id: event_id('a'),
                reply_to_event_id: None,
            })
            .expect("first enqueue");
        actor
            .repository
            .open_next_turn(actor.bot.id(), first_channel, "ask-1")
            .expect("open");
        actor
            .repository
            .save_session(&crate::SessionRecord {
                bot_id: actor.bot.id().clone(),
                channel_id: second_channel.to_owned(),
                session_name: "bot-foobar".to_owned(),
                occupant_logical_id: Some("occupant-agent".to_owned()),
                renew_id: None,
            })
            .expect("second session");
        actor
            .repository
            .index_event(
                &IndexedRelayEvent {
                    event_id: event_id('b'),
                    author_pubkey: "b".repeat(64),
                    created_at: 1,
                    kind: 9,
                    content: "bot: second".to_owned(),
                    tags_json: "[]".to_owned(),
                    channel_id: Some(second_channel.to_owned()),
                    target_event_id: None,
                },
                false,
            )
            .expect("index");
        actor
            .repository
            .enqueue_unprocessed_turn(&NewTurn {
                bot_id: actor.bot.id().clone(),
                channel_id: second_channel.to_owned(),
                event_id: event_id('b'),
                reply_to_event_id: None,
            })
            .expect("second enqueue");

        assert_eq!(
            actor
                .resume_queued(&kelpie, &waiter)
                .expect("second channel"),
            Some(TriggerOutcome::Asked)
        );
        assert_eq!(ask_count(&runner), 1);
        assert_eq!(start_count(&runner), 0);
        assert_eq!(
            runner.calls.lock().expect("calls").last().expect("ask").1,
            b"second"
        );
    }

    #[test]
    fn resume_queued_starts_an_occupant_for_bootstrapping() {
        let (mut actor, kelpie, runner, _panes) =
            actor([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
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
    fn resume_queued_asks_a_later_channel_when_the_first_whoami_fails() {
        let first_channel = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        let second_channel = "ab12cd34-5678-90ab-cdef-0123456789ab";
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            failure("target_unavailable", "no ready occupant"),
            failure("target_unavailable", "no ready occupant"),
            whoami(),
            renewed(),
            whoami(),
            asked("ask-2"),
        ]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        for (channel, display, character, body) in [
            (first_channel, "aaa", 'a', "bot: first"),
            (second_channel, "foobar", 'b', "bot: second"),
        ] {
            actor
                .repository
                .save_session(&crate::SessionRecord {
                    bot_id: actor.bot.id().clone(),
                    channel_id: channel.to_owned(),
                    session_name: format!("bot-{display}"),
                    occupant_logical_id: Some("occupant-agent".to_owned()),
                    renew_id: None,
                })
                .expect("session");
            let event = event_id(character);
            actor
                .repository
                .index_event(
                    &IndexedRelayEvent {
                        event_id: event.clone(),
                        author_pubkey: "b".repeat(64),
                        created_at: 1,
                        kind: 9,
                        content: body.to_owned(),
                        tags_json: "[]".to_owned(),
                        channel_id: Some(channel.to_owned()),
                        target_event_id: None,
                    },
                    false,
                )
                .expect("index");
            actor
                .repository
                .enqueue_unprocessed_turn(&NewTurn {
                    bot_id: actor.bot.id().clone(),
                    channel_id: channel.to_owned(),
                    event_id: event,
                    reply_to_event_id: None,
                })
                .expect("enqueue");
        }

        assert_eq!(
            actor
                .resume_queued(&kelpie, &waiter)
                .expect("second channel"),
            Some(TriggerOutcome::Asked)
        );
        let calls = runner.calls.lock().expect("calls");
        assert_eq!(calls.iter().filter(|call| call.0[1] == "ask").count(), 1);
        assert_eq!(calls.last().expect("ask").1, b"second");
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
            actor([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
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

    #[test]
    fn snapshot_excludes_other_channel_dms() {
        let (mut actor, kelpie, _runner, _panes) =
            actor([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let trigger = work('a', "bot: hello", None);
        let now = unix_now().expect("now");
        let dm_channel = "ffffffff-ffff-ffff-ffff-ffffffffffff";
        actor
            .repository
            .index_event(
                &IndexedRelayEvent {
                    event_id: event_id('d'),
                    author_pubkey: "b".repeat(64),
                    created_at: now - 30,
                    kind: 9,
                    content: "secret dm".to_owned(),
                    tags_json: "[]".to_owned(),
                    channel_id: Some(dm_channel.to_owned()),
                    target_event_id: None,
                },
                false,
            )
            .expect("index dm");
        actor
            .repository
            .index_event(
                &IndexedRelayEvent {
                    event_id: trigger.event_id.clone(),
                    author_pubkey: "b".repeat(64),
                    created_at: now - 10,
                    kind: 9,
                    content: "channel hello".to_owned(),
                    tags_json: "[]".to_owned(),
                    channel_id: Some(trigger.channel_id.clone()),
                    target_event_id: None,
                },
                false,
            )
            .expect("index channel");

        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("handle");

        let snapshot = std::fs::read_to_string(
            actor
                .bot()
                .corpus_path()
                .join(".botserver/places/bot-foobar.md"),
        )
        .expect("snapshot");
        assert!(snapshot.contains("channel hello"));
        assert!(!snapshot.contains("secret dm"));
        assert!(!snapshot.contains(dm_channel));
    }

    fn index_trigger(
        actor: &mut BotActor<SqliteRepository, Arc<FakePanes>>,
        trigger: &TriggerWork,
    ) {
        actor
            .repository
            .index_event(
                &IndexedRelayEvent {
                    event_id: trigger.event_id.clone(),
                    author_pubkey: "b".repeat(64),
                    created_at: 1,
                    kind: 9,
                    content: format!("bot: {}", trigger.nostr_body),
                    tags_json: "[]".to_owned(),
                    channel_id: Some(trigger.channel_id.clone()),
                    target_event_id: None,
                },
                false,
            )
            .expect("index");
    }

    #[test]
    fn handle_turn_completed_asks_the_queued_turn() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            whoami(),
            asked("ask-2"),
        ]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let first = work('a', "first", None);
        let second = work('b', "second", None);
        index_trigger(&mut actor, &first);
        index_trigger(&mut actor, &second);
        actor
            .handle_trigger(&kelpie, &waiter, &first)
            .expect("first");
        actor
            .handle_trigger(&kelpie, &waiter, &second)
            .expect("queued");
        actor
            .repository
            .set_turn_state("ask-1", TurnState::Posted)
            .expect("posted");

        assert_eq!(
            actor
                .handle_turn_completed(&kelpie, &waiter)
                .expect("drain"),
            Some(TriggerOutcome::Asked)
        );
        let turns = actor
            .repository
            .turns_for_session(actor.bot.id(), &first.channel_id)
            .expect("turns");
        assert_eq!(turns[1].state, TurnState::Open);
        assert_eq!(turns[1].ask_id.as_deref(), Some("ask-2"));
        assert_eq!(
            runner.calls.lock().expect("calls").last().expect("ask").1,
            b"second"
        );
    }

    #[test]
    fn ingest_edit_cancels_open_work_and_asks_the_latest_body() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            cancelled(),
            whoami(),
            asked("ask-2"),
        ]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let trigger = work('a', "hello", Some('c'));
        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("first");
        let edit_id = event_id('e');
        let replacement = botserver_domain::TriggerMatch::from_body("bot: latest").expect("edit");

        assert_eq!(
            actor
                .handle_ingest(
                    &kelpie,
                    &waiter,
                    &crate::relay::IngestAction::Edit {
                        event_id: edit_id.clone(),
                        target_event_id: trigger.event_id.clone(),
                        replacement: Some(replacement),
                    },
                    &trigger.channel_display,
                )
                .expect("replaced"),
            TriggerOutcome::Asked
        );
        let turns = actor
            .repository
            .turns_for_session(actor.bot.id(), &trigger.channel_id)
            .expect("turns");
        assert_eq!(turns[0].state, TurnState::Cancelled);
        assert_eq!(turns[1].state, TurnState::Open);
        assert_eq!(turns[1].ask_id.as_deref(), Some("ask-2"));
        assert_eq!(turns[1].event_id, trigger.event_id);
        assert_eq!(turns[1].reply_to_event_id, trigger.reply_to_event_id);
        let calls = runner.calls.lock().expect("calls");
        assert_eq!(
            calls
                .iter()
                .find(|call| call.0[1] == "cancel")
                .expect("cancel")
                .0[2],
            "ask-1"
        );
        assert_eq!(calls.last().expect("ask").1, b"latest");
        assert!(actor
            .repository
            .event_processed(&edit_id)
            .expect("processed"));
    }

    #[test]
    fn ingest_delete_cancels_open_work_without_asking() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            cancelled(),
        ]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let trigger = work('a', "hello", None);
        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("first");
        let delete_id = event_id('d');

        assert_eq!(
            actor
                .handle_ingest(
                    &kelpie,
                    &waiter,
                    &crate::relay::IngestAction::Delete {
                        event_id: delete_id.clone(),
                        target_event_id: trigger.event_id.clone(),
                    },
                    &trigger.channel_display,
                )
                .expect("cancelled"),
            TriggerOutcome::Cancelled
        );
        let turns = actor
            .repository
            .turns_for_session(actor.bot.id(), &trigger.channel_id)
            .expect("turns");
        assert_eq!(turns[0].state, TurnState::Cancelled);
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
        assert!(actor
            .repository
            .event_processed(&delete_id)
            .expect("processed"));
    }

    #[test]
    fn claimed_open_turn_ignores_later_edits() {
        let (mut actor, kelpie, runner, _panes) =
            actor([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let trigger = work('a', "hello", None);
        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("first");
        assert!(actor
            .repository
            .claim_turn_for_publish("ask-1")
            .expect("claim"));

        assert_eq!(
            actor
                .handle_ingest(
                    &kelpie,
                    &waiter,
                    &crate::relay::IngestAction::Edit {
                        event_id: event_id('e'),
                        target_event_id: trigger.event_id.clone(),
                        replacement: botserver_domain::TriggerMatch::from_body("bot: stale"),
                    },
                    &trigger.channel_display,
                )
                .expect("ignored"),
            TriggerOutcome::Declined
        );
        let turns = actor
            .repository
            .turns_for_session(actor.bot.id(), &trigger.channel_id)
            .expect("turns");
        assert_eq!(turns[0].state, TurnState::Open);
        assert!(runner
            .calls
            .lock()
            .expect("calls")
            .iter()
            .all(|call| call.0[1] != "cancel"));
    }

    #[test]
    fn ingest_edit_of_queued_work_replaces_without_a_second_ask() {
        let (mut actor, kelpie, runner, _panes) =
            actor([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let first = work('a', "first", None);
        let second = work('b', "second", None);
        actor
            .handle_trigger(&kelpie, &waiter, &first)
            .expect("first");
        actor
            .handle_trigger(&kelpie, &waiter, &second)
            .expect("queued");

        assert_eq!(
            actor
                .handle_ingest(
                    &kelpie,
                    &waiter,
                    &crate::relay::IngestAction::Edit {
                        event_id: event_id('e'),
                        target_event_id: second.event_id.clone(),
                        replacement: botserver_domain::TriggerMatch::from_body("bot: later"),
                    },
                    &second.channel_display,
                )
                .expect("queued replacement"),
            TriggerOutcome::Queued
        );
        let turns = actor
            .repository
            .turns_for_session(actor.bot.id(), &first.channel_id)
            .expect("turns");
        assert_eq!(turns[0].state, TurnState::Open);
        assert_eq!(turns[1].state, TurnState::Cancelled);
        assert_eq!(turns[2].state, TurnState::Queued);
        assert_eq!(turns[2].event_id, second.event_id);
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
    fn ingest_edit_of_open_turn_drains_a_queued_sibling() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            cancelled(),
            whoami(),
            asked("ask-2"),
        ]);
        let waiter = kelpie.adopt_waiter("w1:p2", "term-2").expect("waiter");
        let first = work('a', "first", None);
        let second = work('b', "second", None);
        index_trigger(&mut actor, &first);
        index_trigger(&mut actor, &second);
        actor
            .handle_trigger(&kelpie, &waiter, &first)
            .expect("first");
        actor
            .handle_trigger(&kelpie, &waiter, &second)
            .expect("queued");

        assert_eq!(
            actor
                .handle_ingest(
                    &kelpie,
                    &waiter,
                    &crate::relay::IngestAction::Edit {
                        event_id: event_id('e'),
                        target_event_id: first.event_id.clone(),
                        replacement: botserver_domain::TriggerMatch::from_body("bot: latest"),
                    },
                    &first.channel_display,
                )
                .expect("drained"),
            TriggerOutcome::Asked
        );
        let turns = actor
            .repository
            .turns_for_session(actor.bot.id(), &first.channel_id)
            .expect("turns");
        assert_eq!(turns[0].state, TurnState::Cancelled);
        assert_eq!(turns[1].state, TurnState::Open);
        assert_eq!(turns[1].event_id, second.event_id);
        assert_eq!(turns[1].ask_id.as_deref(), Some("ask-2"));
        assert_eq!(turns[2].state, TurnState::Queued);
        assert_eq!(turns[2].event_id, first.event_id);
        assert_eq!(
            runner.calls.lock().expect("calls").last().expect("ask").1,
            b"second"
        );
    }
}
