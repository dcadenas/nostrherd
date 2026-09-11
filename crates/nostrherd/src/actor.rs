//! Per-bot actor: start a corpus occupant, then ask on each trigger.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::Path;
use std::sync::Arc;

use nostrherd_domain::{Bot, BotId, EventId, SessionName};

use crate::ask_body::{render_ask_body, AskContextCursor, TriggerRequest};
use crate::inbox::InboxDelivery;
use crate::outbox::{
    self, InFlightReaction, InboxAction, NoopInFlightReaction, OutboundPublisher, OutboxError,
    OutputGuard,
};
use crate::progress::{self, NoopProgressRelay, ProgressRelay};
use crate::relay::ChannelAudience;
use crate::snapshot::{place_snapshot_relpath, refresh_place_snapshot, render_place_snapshot};
use crate::watch::{parse_watch_command, WatchCommand, WatchFire, WatchRecord};
use crate::{
    occupant_bootstrap, AskDelivery, HostRepository, HostWaiter, IndexedRelayEvent, KelpieClient,
    KelpieError, NewTurn, OccupantLaunch, SessionRecord, TurnState,
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

    /// Allocate a session-named Herdr workspace whose root pane hosts the occupant.
    ///
    /// # Errors
    ///
    /// Returns an error when Herdr cannot create the workspace.
    fn allocate(&self, session_name: &str, cwd: &Path) -> Result<OccupantPane, Self::Error>;

    /// Close a pane whose occupant never started, or whose occupant is gone.
    ///
    /// A start error alone does not prove a pane is empty, so a caller must
    /// establish that the occupant is not running before calling this.
    ///
    /// # Errors
    ///
    /// Returns an error when Herdr cannot close the pane.
    fn release(&self, pane: &OccupantPane) -> Result<(), Self::Error>;

    /// Find the pane already holding `session_name`, within this bot's corpus.
    ///
    /// Herdr allows one claim on a name, so a pane left behind by a previous
    /// occupant blocks every later start with `agent_name_taken`. The cwd is
    /// the ownership proof: a pane holding the name somewhere else belongs to
    /// something that is not this bot, and is never touched (D62).
    ///
    /// # Errors
    ///
    /// Returns an error when Herdr cannot be asked.
    fn claimed(&self, session_name: &str, cwd: &Path) -> Result<Option<ClaimedPane>, Self::Error>;
}

/// A Herdr pane already holding a session's name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedPane {
    pub pane: OccupantPane,
    /// The backend's own session token, when Herdr recorded one for this pane.
    pub backend_session: Option<String>,
}

/// One triggering event ready for the bot actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerWork {
    pub author_pubkey: String,
    pub event_id: EventId,
    pub channel_id: String,
    pub channel_display: String,
    pub reply_to_event_id: Option<EventId>,
    pub nostr_body: String,
}

/// Known channels and active triggering event ids for relay filters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingScope {
    pub channel_ids: Vec<String>,
    pub active_event_ids: Vec<EventId>,
    pub watched_author_pubkeys: Vec<String>,
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
    /// Host publish of an occupant final failed.
    Outbox(String),
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
            Self::Outbox(error) => write!(formatter, "{error}"),
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
            | Self::Outbox(_) => None,
        }
    }
}

#[derive(Clone)]
struct ReactionHost {
    inner: Arc<dyn InFlightReaction>,
}

impl fmt::Debug for ReactionHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReactionHost")
    }
}

impl InFlightReaction for ReactionHost {
    fn add(&self, trigger_event_id: &EventId) {
        self.inner.add(trigger_event_id);
    }

    fn remove(&self, trigger_event_id: &EventId) {
        self.inner.remove(trigger_event_id);
    }
}

/// Serialized in-process actor for one configured bot.
#[derive(Debug)]
pub struct BotActor<R, P> {
    operator_pubkey: String,
    bot: Bot,
    pub(crate) repository: R,
    panes: P,
    reactions: ReactionHost,
    progress_relay: ProgressHost,
    audiences: HashMap<String, ChannelAudience>,
    output_guard: OutputGuard,
}

/// Progress relay sink with a `Debug` that does not describe the adapter.
#[derive(Clone)]
struct ProgressHost {
    inner: Arc<dyn ProgressRelay>,
}

impl fmt::Debug for ProgressHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProgressHost")
    }
}

impl ProgressRelay for ProgressHost {
    fn edit(
        &self,
        ask_id: &str,
        channel_id: &str,
        post_event_id: &EventId,
        content: &str,
    ) -> Result<progress::ProgressRelayDispatch, crate::outbox::PublishError> {
        self.inner.edit(ask_id, channel_id, post_event_id, content)
    }

    fn delete(
        &self,
        ask_id: &str,
        channel_id: &str,
        post_event_id: &EventId,
    ) -> Result<progress::ProgressRelayDispatch, crate::outbox::PublishError> {
        self.inner.delete(ask_id, channel_id, post_event_id)
    }

    fn drain_completions(&self) -> Vec<progress::ProgressRelayCompletion> {
        self.inner.drain_completions()
    }
}

impl<R, P> BotActor<R, P>
where
    R: HostRepository,
    P: OccupantPaneAllocator,
{
    /// Create an actor for one configured bot.
    #[must_use]
    pub fn new(bot: Bot, repository: R, panes: P, operator_pubkey: String) -> Self {
        Self {
            operator_pubkey,
            bot,
            repository,
            panes,
            reactions: ReactionHost {
                inner: Arc::new(NoopInFlightReaction),
            },
            progress_relay: ProgressHost {
                inner: Arc::new(NoopProgressRelay),
            },
            audiences: HashMap::new(),
            output_guard: OutputGuard::default(),
        }
    }

    /// Use a host reaction sink for in-flight trigger markers.
    #[must_use]
    pub fn with_reactions(mut self, reactions: Arc<dyn InFlightReaction>) -> Self {
        self.reactions = ReactionHost { inner: reactions };
        self
    }

    /// Use a host relay for progress post edits and deletes (D42).
    #[must_use]
    pub fn with_progress_relay(mut self, relay: Arc<dyn ProgressRelay>) -> Self {
        self.progress_relay = ProgressHost { inner: relay };
        self
    }

    /// Apply mechanical output screening before channel publication.
    #[must_use]
    pub fn with_output_guard(mut self, output_guard: OutputGuard) -> Self {
        self.output_guard = output_guard;
        self
    }

    /// Return the configured bot.
    #[must_use]
    pub fn bot(&self) -> &Bot {
        &self.bot
    }

    /// Cache host-resolved audience evidence for the next ask in a channel.
    pub fn set_audience(&mut self, channel_id: impl Into<String>, audience: ChannelAudience) {
        self.audiences.insert(channel_id.into(), audience);
    }

    /// Return indexed channel events used for audience fallback.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence cannot read the channel index.
    pub fn audience_events(
        &self,
        channel_id: &str,
    ) -> Result<Vec<IndexedRelayEvent>, ActorError<R::Error>> {
        self.channel_events_for_occupant(channel_id)
    }

    /// Return whether this bot already has a session for `channel_id`.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence cannot load the session.
    pub fn has_session(&self, channel_id: &str) -> Result<bool, ActorError<R::Error>> {
        Ok(self
            .repository
            .session(self.bot.id(), channel_id)
            .map_err(ActorError::Repository)?
            .is_some())
    }

    /// Known session channels and queued or open triggering event ids.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence cannot list sessions or turns.
    pub fn pending_scope(&self) -> Result<PendingScope, ActorError<R::Error>> {
        let now = crate::unix_now().map_err(ActorError::Snapshot)?;
        Ok(PendingScope {
            channel_ids: self
                .repository
                .known_channel_ids()
                .map_err(ActorError::Repository)?,
            active_event_ids: self
                .repository
                .active_event_ids()
                .map_err(ActorError::Repository)?,
            watched_author_pubkeys: self
                .repository
                .watched_author_pubkeys(now)
                .map_err(ActorError::Repository)?,
        })
    }

    /// Persist a trigger and start or ask the channel occupant.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence, pane allocation, or Kelpie fails.
    pub fn handle_trigger(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &HostWaiter<'_>,
        work: &TriggerWork,
    ) -> Result<TriggerOutcome, ActorError<R::Error>> {
        if work.nostr_body.trim().is_empty() {
            return Err(ActorError::EmptyAskBody);
        }
        if !self
            .bot
            .authorizes(&self.operator_pubkey, &work.author_pubkey)
        {
            self.repository
                .mark_event_processed(&work.event_id)
                .map_err(ActorError::Repository)?;
            return Ok(TriggerOutcome::Declined);
        }
        let request = TriggerRequest {
            author_pubkey: work.author_pubkey.clone(),
            request: work.nostr_body.clone(),
        };
        if request.stamped(&self.operator_pubkey).is_none() {
            self.repository
                .mark_event_processed(&work.event_id)
                .map_err(ActorError::Repository)?;
            return Ok(TriggerOutcome::Declined);
        }
        let display = if work.channel_display.is_empty() {
            work.channel_id.as_str()
        } else {
            work.channel_display.as_str()
        };
        self.ensure_session(&work.channel_id, display)?;
        self.apply_watch_command(&work.event_id, &work.channel_id, &work.nostr_body)?;
        let Some(_) = self
            .repository
            .enqueue_unprocessed_turn(&NewTurn {
                bot_id: self.bot.id().clone(),
                channel_id: work.channel_id.clone(),
                event_id: work.event_id.clone(),
                reply_to_event_id: work.reply_to_event_id.clone(),
                ask_body: Some(
                    serde_json::to_string(&request)
                        .map_err(|error| ActorError::Snapshot(io::Error::other(error)))?,
                ),
                publish_reply_to_event_id: Some(work.event_id.clone()),
            })
            .map_err(ActorError::Repository)?
        else {
            return Ok(TriggerOutcome::Duplicate);
        };
        self.reactions.add(&work.event_id);
        if !self.should_ask_event(&work.channel_id, &work.event_id)? {
            return Ok(TriggerOutcome::Queued);
        }
        self.ask_oldest_queued(kelpie, waiter, &work.channel_id)?;
        Ok(TriggerOutcome::Asked)
    }

    /// Queue a previously recorded host watch fire on its declaring session.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence, pane allocation, or Kelpie fails.
    pub fn handle_watch_fire(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &HostWaiter<'_>,
        fire: &WatchFire,
    ) -> Result<TriggerOutcome, ActorError<R::Error>> {
        self.repository
            .enqueue_turn(&NewTurn {
                bot_id: self.bot.id().clone(),
                channel_id: fire.channel_id.clone(),
                event_id: fire.wake_event_id.clone(),
                reply_to_event_id: Some(fire.source_event_id.clone()),
                ask_body: Some(fire.ask_body()),
                publish_reply_to_event_id: None,
            })
            .map_err(ActorError::Repository)?;
        if !self.should_ask_event(&fire.channel_id, &fire.wake_event_id)? {
            return Ok(TriggerOutcome::Queued);
        }
        self.ask_oldest_queued(kelpie, waiter, &fire.channel_id)?;
        Ok(TriggerOutcome::Asked)
    }

    fn apply_watch_command(
        &mut self,
        declaration_event_id: &EventId,
        channel_id: &str,
        request: &str,
    ) -> Result<(), ActorError<R::Error>> {
        let Some(command) = parse_watch_command(request) else {
            return Ok(());
        };
        match command {
            WatchCommand::Create(watch) => {
                let now = crate::unix_now().map_err(ActorError::Snapshot)?;
                let created_at = self
                    .repository
                    .indexed_event(declaration_event_id)
                    .map_err(ActorError::Repository)?
                    .map_or(now, |event| event.created_at);
                self.repository
                    .save_watch(&WatchRecord {
                        watch_id: declaration_event_id.clone(),
                        created_at,
                        bot_id: self.bot.id().clone(),
                        channel_id: channel_id.to_owned(),
                        author_pubkeys: watch.author_pubkeys,
                        predicate_channel_id: watch.channel_only.then(|| channel_id.to_owned()),
                        predicate_kind: watch.kind,
                        cooldown_secs: watch.cooldown_secs,
                        expires_at: watch
                            .expires_after_secs
                            .map(|duration| now.saturating_add(duration)),
                        max_fires: watch.max_fires,
                    })
                    .map_err(ActorError::Repository)?;
            }
            WatchCommand::Cancel { author_pubkey } => {
                let now = crate::unix_now().map_err(ActorError::Snapshot)?;
                let created_at = self
                    .repository
                    .indexed_event(declaration_event_id)
                    .map_err(ActorError::Repository)?
                    .map_or(now, |event| event.created_at);
                self.repository
                    .cancel_watches(
                        self.bot.id(),
                        channel_id,
                        &author_pubkey,
                        declaration_event_id,
                        created_at,
                    )
                    .map_err(ActorError::Repository)?;
            }
        }
        Ok(())
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
        waiter: &HostWaiter<'_>,
        action: &crate::relay::IngestAction,
        channel_display: &str,
    ) -> Result<TriggerOutcome, ActorError<R::Error>> {
        match action {
            crate::relay::IngestAction::TurnCandidate {
                bot_id: _,
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
                match self.handle_trigger(
                    kelpie,
                    waiter,
                    &TriggerWork {
                        author_pubkey: self
                            .repository
                            .indexed_event(event_id)
                            .map_err(ActorError::Repository)?
                            .map(|event| event.author_pubkey)
                            .unwrap_or_default(),
                        event_id: event_id.clone(),
                        channel_id: channel_id.clone(),
                        channel_display: channel_display.to_owned(),
                        reply_to_event_id: reply_to_event_id.clone(),
                        nostr_body: trigger.request().to_owned(),
                    },
                ) {
                    Err(ActorError::UnnameableSession) => {
                        self.repository
                            .mark_event_processed(event_id)
                            .map_err(ActorError::Repository)?;
                        Ok(TriggerOutcome::Declined)
                    }
                    other => other,
                }
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

    /// Classify an occupant inbox delivery, publish a final, then ACK.
    ///
    /// Busy-queue resume runs only after `posted` is durable and the host has
    /// refreshed the audience for the next ask.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence, publish, or queued resume fails.
    pub fn handle_occupant_delivery<Pub: OutboundPublisher>(
        &mut self,
        _kelpie: &KelpieClient,
        waiter: &HostWaiter<'_>,
        publisher: &Pub,
        delivery: &InboxDelivery,
    ) -> Result<InboxAction, ActorError<R::Error>>
    where
        R::Error: fmt::Display,
        Pub::Error: fmt::Display,
    {
        let mut notice = |text: &str| eprintln!("operator notice: {text}");
        let mut occupant_feedback = |session_name: &str, body: &str| {
            if let Err(error) = waiter.tell_occupant(session_name, body) {
                eprintln!("operator notice: occupant feedback to {session_name} failed: {error}");
            }
        };
        let mut operator_feedback = |body: &str| {
            if let Some(session_name) = self.bot.operator_session() {
                if let Err(error) = waiter.tell_occupant(session_name, body) {
                    eprintln!(
                        "operator notice: private feedback to {session_name} failed: {error}"
                    );
                }
            }
        };
        let action = outbox::handle_delivery_with_feedback_and_audiences(
            &mut self.repository,
            publisher,
            &mut notice,
            delivery,
            &self.reactions,
            &mut occupant_feedback,
            &self.output_guard,
            &mut operator_feedback,
            &self.audiences,
        )
        .map_err(|error| match error {
            OutboxError::Repository(error) => ActorError::Repository(error),
            OutboxError::Publish(error) => ActorError::Outbox(error.to_string()),
        })?;
        Ok(action)
    }

    /// Return the bot that owns a turn identified by its ask id.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence cannot read the turn.
    pub fn bot_id_for_ask(&self, ask_id: &str) -> Result<Option<BotId>, ActorError<R::Error>> {
        Ok(self
            .repository
            .turn_by_ask_id(ask_id)
            .map_err(ActorError::Repository)?
            .map(|turn| turn.bot_id))
    }

    /// Return the channel of a posted ask before resuming its queued sibling.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence cannot read the turn.
    pub fn posted_channel_for_ask(
        &self,
        ask_id: &str,
    ) -> Result<Option<String>, ActorError<R::Error>> {
        Ok(self
            .repository
            .turn_by_ask_id(ask_id)
            .map_err(ActorError::Repository)?
            .filter(|turn| turn.state == TurnState::Posted && turn.bot_id == *self.bot.id())
            .map(|turn| turn.channel_id))
    }

    /// Resume this bot's queue after its turn reached `posted`.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence, pane allocation, or Kelpie fails.
    pub fn resume_if_posted(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &HostWaiter<'_>,
        ask_id: &str,
    ) -> Result<(), ActorError<R::Error>> {
        let Some(turn) = self
            .repository
            .turn_by_ask_id(ask_id)
            .map_err(ActorError::Repository)?
        else {
            return Ok(());
        };
        if turn.state != TurnState::Posted || turn.bot_id != *self.bot.id() {
            return Ok(());
        }
        self.resume_queued(kelpie, waiter)?;
        Ok(())
    }

    /// Drain undispatched outbound attempts on the host tick (D48).
    ///
    /// Tells and ask finals that failed with a retryable transport error
    /// are resent without waiting for a Kelpie reconnect, reusing the
    /// prepared event id. One retryable failure does not skip the rest.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence fails.
    pub fn retry_outbound<Pub: OutboundPublisher>(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &HostWaiter<'_>,
        publisher: &Pub,
    ) -> Result<(), ActorError<R::Error>>
    where
        R::Error: fmt::Display,
        Pub::Error: fmt::Display,
    {
        let now = crate::unix_now().unwrap_or_default();
        let attempts = self
            .repository
            .pending_outbound_attempts(self.bot.id())
            .map_err(ActorError::Repository)?;
        let mut notice = |text: &str| eprintln!("operator notice: {text}");
        let mut posted = false;
        let mut failed_open = false;
        for attempt in attempts {
            if !outbox::outbound_retry_due(&attempt, now) {
                continue;
            }
            match outbox::retry_undispatched(
                &mut self.repository,
                publisher,
                &mut notice,
                &self.reactions,
                &attempt,
                self.bot.id(),
                now,
            ) {
                Ok(InboxAction::Ack) => {
                    if self
                        .repository
                        .turn_by_ask_id(&attempt.ask_id)
                        .map_err(ActorError::Repository)?
                        .is_some_and(|turn| {
                            turn.state == TurnState::Posted || turn.state == TurnState::Failed
                        })
                    {
                        if self
                            .repository
                            .turn_by_ask_id(&attempt.ask_id)
                            .map_err(ActorError::Repository)?
                            .is_some_and(|turn| turn.state == TurnState::Posted)
                        {
                            posted = true;
                        } else {
                            failed_open = true;
                        }
                    }
                }
                Ok(InboxAction::Hold) | Err(OutboxError::Publish(_)) => {}
                Err(OutboxError::Repository(error)) => return Err(ActorError::Repository(error)),
            }
        }
        if posted || failed_open {
            self.resume_queued(kelpie, waiter)?;
        }
        Ok(())
    }

    /// Relay pending occupant progress on the host refresh tick (D42).
    ///
    /// Creates each ask's progress post once the hold elapsed, edits it
    /// in place under the interval and cap, and finishes a create that
    /// was prepared without an accepted id. Relay failures are
    /// operator notices and never fail the turn.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence fails.
    pub fn flush_progress<Pub: OutboundPublisher>(
        &mut self,
        publisher: &Pub,
        now: i64,
    ) -> Result<(), ActorError<R::Error>>
    where
        Pub::Error: fmt::Display,
    {
        let mut notice = |text: &str| eprintln!("operator notice: {text}");
        progress::apply_relay_completions(&mut self.repository, &self.progress_relay, &mut notice)
            .map_err(ActorError::Repository)?;
        let posts = self
            .repository
            .progress_posts_pending_flush(self.bot.id())
            .map_err(ActorError::Repository)?;
        for (post, turn) in posts {
            progress::flush_progress(
                &mut self.repository,
                publisher,
                &self.progress_relay,
                &mut notice,
                &turn,
                post,
                now,
            )
            .map_err(ActorError::Repository)?;
        }
        Ok(())
    }

    /// Drain the next queued turn after an in-flight turn is posted.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence, pane allocation, or Kelpie fails.
    pub fn handle_turn_completed(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &HostWaiter<'_>,
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
        waiter: &HostWaiter<'_>,
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
        waiter: &HostWaiter<'_>,
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
            if self.queued_ask_body(&queued.event_id)?.is_none() {
                self.repository
                    .cancel_queued_turn(&queued.event_id)
                    .map_err(ActorError::Repository)?;
                self.reactions.remove(&queued.event_id);
                continue;
            }
            match self.ask_oldest_queued(kelpie, waiter, &session.channel_id) {
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
        waiter: &HostWaiter<'_>,
        event_id: &EventId,
        target_event_id: &EventId,
        replacement: Option<&nostrherd_domain::TriggerMatch>,
    ) -> Result<TriggerOutcome, ActorError<R::Error>> {
        let Some(request) = replacement
            .map(nostrherd_domain::TriggerMatch::request)
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
        let author = active
            .ask_body
            .as_deref()
            .and_then(|body| serde_json::from_str::<TriggerRequest>(body).ok())
            .map(|request| request.author_pubkey);
        let author = match author {
            Some(author) => Some(author),
            None => self
                .repository
                .indexed_event(target_event_id)
                .map_err(ActorError::Repository)?
                .map(|event| event.author_pubkey),
        };
        let Some(author) = author else {
            return self.abandon_unclaimed(
                kelpie,
                waiter,
                event_id,
                target_event_id,
                "requester unavailable",
            );
        };
        if !self.bot.authorizes(&self.operator_pubkey, &author) {
            return self.abandon_unclaimed(
                kelpie,
                waiter,
                event_id,
                target_event_id,
                "requester not authorized",
            );
        }
        let request_record = TriggerRequest {
            author_pubkey: author,
            request: request.to_owned(),
        };
        let Some(replaced) = self
            .repository
            .replace_unclaimed_turn(&NewTurn {
                bot_id: self.bot.id().clone(),
                channel_id: active.channel_id.clone(),
                event_id: target_event_id.clone(),
                reply_to_event_id: active.reply_to_event_id.clone(),
                ask_body: Some(
                    serde_json::to_string(&request_record)
                        .map_err(|error| ActorError::Snapshot(io::Error::other(error)))?,
                ),
                publish_reply_to_event_id: Some(target_event_id.clone()),
            })
            .map_err(ActorError::Repository)?
        else {
            self.repository
                .mark_event_processed(event_id)
                .map_err(ActorError::Repository)?;
            return Ok(TriggerOutcome::Declined);
        };
        if let Some(ask_id) = replaced.cancelled.ask_id.as_deref() {
            let cancel = waiter
                .cancel(ask_id, "trigger edited")
                .map_err(ActorError::Kelpie);
            // The replacement ask starts with no progress post (D42), and the
            // turn is already replaced here, so this pass is the only one that
            // can end the old post even when the Kelpie cancel fails.
            self.delete_progress_post(ask_id)?;
            cancel?;
        }
        self.repository
            .mark_event_processed(event_id)
            .map_err(ActorError::Repository)?;
        if self.should_ask_event(&replaced.queued.channel_id, &replaced.queued.event_id)? {
            self.ask_oldest_queued(kelpie, waiter, &replaced.queued.channel_id)?;
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
        waiter: &HostWaiter<'_>,
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
            let cancel = waiter.cancel(ask_id, reason).map_err(ActorError::Kelpie);
            // The turn is already cancelled in the host database, so a later
            // ingest of the same event finds nothing to cancel and never comes
            // back here. Returning on the Kelpie error before the delete would
            // leave the stamped progress post up for good, and D42 makes that
            // delete best-effort rather than conditional on the cancel.
            self.delete_progress_post(ask_id)?;
            cancel?;
        }
        self.reactions.remove(&cancelled.event_id);
        self.repository
            .mark_event_processed(event_id)
            .map_err(ActorError::Repository)?;
        self.resume_queued(kelpie, waiter)?;
        Ok(TriggerOutcome::Cancelled)
    }

    /// End progress for a cancelled ask and Buzz-delete its post (D42).
    fn delete_progress_post(&mut self, ask_id: &str) -> Result<(), ActorError<R::Error>> {
        let mut notice = |text: &str| eprintln!("operator notice: {text}");
        progress::delete_progress_post(
            &mut self.repository,
            &self.progress_relay,
            &mut notice,
            ask_id,
        )
        .map_err(ActorError::Repository)
    }

    /// Indexed channel events minus the host's own progress posts (D42).
    ///
    /// The host indexes its stamped kind 9 but never fetches its own edits,
    /// so a progress post would otherwise show a stale first body in
    /// snapshots and ask Context.
    fn channel_events_for_occupant(
        &self,
        channel_id: &str,
    ) -> Result<Vec<IndexedRelayEvent>, ActorError<R::Error>> {
        let excluded = self
            .repository
            .progress_post_event_ids(channel_id)
            .map_err(ActorError::Repository)?;
        let events = self
            .repository
            .indexed_events_for_channel(channel_id)
            .map_err(ActorError::Repository)?;
        Ok(events
            .into_iter()
            .filter(|event| !excluded.contains(&event.event_id))
            .collect())
    }

    fn queued_ask_body(&self, event_id: &EventId) -> Result<Option<String>, ActorError<R::Error>> {
        let turn = self
            .repository
            .active_turn_for_event(event_id)
            .map_err(ActorError::Repository)?;
        if let Some(turn) = &turn {
            if turn.publish_reply_to_event_id.is_none() {
                return Ok(turn.ask_body.clone());
            }
            if let Some(body) = &turn.ask_body {
                let Ok(request) = serde_json::from_str::<TriggerRequest>(body) else {
                    return Ok(None);
                };
                return Ok(self
                    .bot
                    .authorizes(&self.operator_pubkey, &request.author_pubkey)
                    .then(|| request.stamped(&self.operator_pubkey))
                    .flatten());
            }
        }
        let Some(author) = self
            .repository
            .indexed_event(event_id)
            .map_err(ActorError::Repository)?
            .map(|event| event.author_pubkey)
        else {
            return Ok(None);
        };
        if !self.bot.authorizes(&self.operator_pubkey, &author) {
            return Ok(None);
        }
        Ok(self
            .repository
            .latest_body_for_event(event_id)
            .map_err(ActorError::Repository)?
            .and_then(|content| {
                nostrherd_domain::TriggerMatch::from_body(&content, self.bot.inbound_trigger())
            })
            .filter(|trigger| !trigger.request().is_empty())
            .and_then(|trigger| {
                TriggerRequest {
                    author_pubkey: author,
                    request: trigger.request().to_owned(),
                }
                .stamped(&self.operator_pubkey)
            }))
    }

    fn recover_open_session(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &HostWaiter<'_>,
        session: &SessionRecord,
    ) -> Result<bool, ActorError<R::Error>> {
        let turns = self
            .repository
            .turns_for_session(&session.bot_id, &session.channel_id)
            .map_err(ActorError::Repository)?;
        if !turns.iter().any(|turn| turn.state == TurnState::Open) {
            return Ok(false);
        }
        // An open turn means an occupant owes an answer. If the name still
        // answers, nothing was lost and the occupant is already working on it.
        match kelpie.occupant_whoami(&session.session_name) {
            Ok(_) => return Ok(false),
            Err(KelpieError::TargetUnavailable) => {}
            Err(error) => return Err(ActorError::Kelpie(error)),
        }
        let snapshot_relpath = self.refresh_snapshot(session)?;
        let mut session = session.clone();
        self.restart_occupant(kelpie, waiter, &mut session, &snapshot_relpath)?;
        Ok(true)
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

    #[allow(clippy::too_many_lines)]
    fn ask_oldest_queued(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &HostWaiter<'_>,
        channel_id: &str,
    ) -> Result<(), ActorError<R::Error>> {
        let mut session = self
            .repository
            .session(self.bot.id(), channel_id)
            .map_err(ActorError::Repository)?
            .ok_or(ActorError::UnnameableSession)?;
        let queued = self
            .repository
            .turns_for_session(self.bot.id(), channel_id)
            .map_err(ActorError::Repository)?
            .into_iter()
            .filter(|turn| turn.state == TurnState::Queued)
            .min_by_key(|turn| turn.sequence)
            .ok_or(ActorError::UnnameableSession)?;
        let Some(stamped_body) = self.queued_ask_body(&queued.event_id)? else {
            self.repository
                .cancel_queued_turn(&queued.event_id)
                .map_err(ActorError::Repository)?;
            self.reactions.remove(&queued.event_id);
            return Ok(());
        };
        let snapshot_relpath = self.refresh_snapshot(&session)?;
        let events = self.channel_events_for_occupant(channel_id)?;
        let context_event_id = if queued.publish_reply_to_event_id.is_none() {
            queued
                .reply_to_event_id
                .as_ref()
                .unwrap_or(&queued.event_id)
        } else {
            &queued.event_id
        };
        let trigger_created_at = match self
            .repository
            .indexed_event(context_event_id)
            .map_err(ActorError::Repository)?
        {
            Some(event) => event.created_at,
            None => crate::unix_now().map_err(ActorError::Snapshot)?,
        };
        let cursor = match (
            session.ask_context_event_id.clone(),
            session.ask_context_created_at,
        ) {
            (Some(event_id), Some(created_at)) => Some(AskContextCursor {
                event_id,
                created_at,
            }),
            _ => None,
        };
        let rendered = render_ask_body(
            &stamped_body,
            &session.session_name,
            channel_id,
            cursor.as_ref(),
            context_event_id,
            trigger_created_at,
            &events,
            &self.audience_for(channel_id, &events),
            &self.operator_pubkey,
        );
        let idempotency_key = format!("{}:{}", queued.event_id.as_str(), queued.sequence);
        let receipt = self.ask_queued_with_recovery(
            kelpie,
            waiter,
            &mut session,
            &snapshot_relpath,
            &rendered.body,
            &idempotency_key,
        )?;
        match receipt.delivery() {
            AskDelivery::Accepted | AskDelivery::Unknown => {}
            delivery @ (AskDelivery::Rejected | AskDelivery::TargetUnavailable) => {
                waiter
                    .cancel(receipt.message_id(), "queued ask was not delivered")
                    .map_err(ActorError::Kelpie)?;
                return Err(ActorError::AskNotDelivered(delivery));
            }
        }
        // A live occupant with no renew is one whose schedule was lost, not one
        // that never had a seat. The receipt names who took the ask, so arming
        // from it costs no extra round trip and cannot target a stale identity.
        if session.renew_id.is_none() {
            if let Some(incarnation) = receipt.recipient_incarnation() {
                self.try_arm_renew(
                    kelpie,
                    receipt.recipient(),
                    incarnation,
                    &snapshot_relpath,
                    &mut session,
                )?;
            }
        }
        self.repository
            .open_next_turn(self.bot.id(), channel_id, receipt.message_id())
            .map_err(ActorError::Repository)?
            .ok_or_else(|| ActorError::TurnNotOpened {
                ask_id: receipt.message_id().to_owned(),
            })?;
        session.ask_context_event_id = Some(rendered.cursor.event_id);
        session.ask_context_created_at = Some(rendered.cursor.created_at);
        self.repository
            .save_session(&session)
            .map_err(ActorError::Repository)?;
        Ok(())
    }

    fn ask_queued_with_recovery(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &HostWaiter<'_>,
        session: &mut SessionRecord,
        snapshot_relpath: &str,
        body: &str,
        idempotency_key: &str,
    ) -> Result<crate::AskReceipt, ActorError<R::Error>> {
        let first_attempt = waiter.ask_named(&session.session_name, None, body, idempotency_key);
        let unavailable_incarnation = match first_attempt {
            Err(KelpieError::TargetUnavailable) => None,
            Ok(receipt) if receipt.delivery() == AskDelivery::TargetUnavailable => {
                let incarnation_id = receipt.recipient_incarnation().ok_or_else(|| {
                    ActorError::Kelpie(KelpieError::InvalidReceipt(
                        "unavailable ask omitted its recipient incarnation".to_owned(),
                    ))
                })?;
                waiter
                    .cancel(receipt.message_id(), "queued ask target unavailable")
                    .map_err(ActorError::Kelpie)?;
                Some(incarnation_id.to_owned())
            }
            other => return other.map_err(ActorError::Kelpie),
        };
        if let Some(incarnation_id) = unavailable_incarnation.as_deref() {
            waiter
                .retire_occupant(
                    incarnation_id,
                    &format!("{idempotency_key}:retire:{incarnation_id}"),
                )
                .map_err(ActorError::Kelpie)?;
        }
        // Nothing answers to the name: it has never been started, or its
        // runtime ended. Either way, converge and ask again.
        self.restart_occupant(kelpie, waiter, session, snapshot_relpath)?;
        waiter
            .ask_named(&session.session_name, None, body, idempotency_key)
            .map_err(ActorError::Kelpie)
    }

    /// Put a working occupant behind a name that is not answering (D62).
    ///
    /// Call this only once the name has been found unaddressable, which an
    /// ask reports on its own: probing first would cost a round trip and still
    /// leave a window where the answer goes stale before it is used.
    ///
    /// The name is the identity, so this converges rather than reconciling.
    /// Nothing about the occupant is stored between calls, so there is no
    /// recorded state to drift, and no state this can fail to anticipate: a
    /// name Kelpie has never seen starts fresh, and one whose runtimes have
    /// ended continues its newest identity.
    fn restart_occupant(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &HostWaiter<'_>,
        session: &mut SessionRecord,
        snapshot_relpath: &str,
    ) -> Result<crate::StartedOccupant, ActorError<R::Error>> {
        // Nothing answers to the name, so a pane still holding it in this
        // bot's corpus is one whose occupant has gone: a live one would have
        // answered. Reclaim that seat, or Herdr refuses the start with
        // `agent_name_taken`, and carry its backend session forward.
        if let Some(claimed) = self
            .panes
            .claimed(&session.session_name, self.bot.corpus_path())
            .map_err(|error| ActorError::Pane(error.to_string()))?
        {
            if session.backend_session.is_none() {
                session.backend_session.clone_from(&claimed.backend_session);
            }
            self.panes
                .release(&claimed.pane)
                .map_err(|error| ActorError::Pane(error.to_string()))?;
        }
        let continue_as = kelpie
            .resolve_name(&session.session_name)
            .map_err(ActorError::Kelpie)?;
        let started = self.fresh_start(
            kelpie,
            waiter,
            session,
            snapshot_relpath,
            continue_as.as_deref(),
        )?;
        // A new runtime never inherits the old one's renew schedule.
        session.renew_id = None;
        self.try_arm_renew(
            kelpie,
            started.logical_agent_id(),
            started.incarnation_id(),
            snapshot_relpath,
            session,
        )?;
        Ok(started)
    }

    /// Allocate a seat and start one runtime on it.
    ///
    /// `continue_as` carries the name's existing identity forward when it has
    /// one. Kelpie reserves the idempotency key with identity creation, so a
    /// retry of the same seat either starts once or is refused; it cannot mint
    /// a second identity for the name.
    ///
    /// # Errors
    ///
    /// Returns an error when the seat cannot be allocated or the start fails.
    /// The pane is kept on failure: a start error does not prove the pane is
    /// empty, and closing one whose occupant may be running loses it.
    fn fresh_start(
        &mut self,
        kelpie: &KelpieClient,
        waiter: &HostWaiter<'_>,
        session: &mut SessionRecord,
        snapshot_relpath: &str,
        continue_as: Option<&str>,
    ) -> Result<crate::StartedOccupant, ActorError<R::Error>> {
        let pane = self
            .panes
            .allocate(&session.session_name, self.bot.corpus_path())
            .map_err(|error| ActorError::Pane(error.to_string()))?;
        let launch = OccupantLaunch {
            name: session.session_name.clone(),
            pane_id: pane.pane_id.clone(),
            terminal_id: pane.terminal_id.clone(),
            backend: self.bot.occupant_kind().to_owned(),
            cwd: self.bot.corpus_path().to_path_buf(),
            timeout_ms: OCCUPANT_START_TIMEOUT_MS,
            logical_agent_id: continue_as.map(str::to_owned),
            backend_session: session.backend_session.clone(),
            model: self.bot.occupant_model().map(str::to_owned),
        };
        let key = format!(
            "nostrherd:{}:{}:{}",
            session.session_name, pane.pane_id, pane.terminal_id
        );
        let started = kelpie
            .start_occupant_with_receipt(
                &launch,
                &occupant_bootstrap(snapshot_relpath),
                Some(waiter.identity().logical_agent_id()),
                Some(&key),
                &mut None,
            )
            .map_err(ActorError::Kelpie)?;
        // Herdr records the backend's session only once the runtime is up, so
        // read it back now rather than from the start receipt.
        if let Ok(Some(claimed)) = self
            .panes
            .claimed(&session.session_name, self.bot.corpus_path())
        {
            session.backend_session = claimed.backend_session;
        }
        self.repository
            .save_session(session)
            .map_err(ActorError::Repository)?;
        Ok(started)
    }

    fn try_arm_renew(
        &mut self,
        kelpie: &KelpieClient,
        logical_id: &str,
        incarnation_id: &str,
        snapshot_relpath: &str,
        session: &mut SessionRecord,
    ) -> Result<(), ActorError<R::Error>> {
        let progress_relpath = crate::snapshot::session_progress_relpath(&session.session_name)
            .ok_or_else(|| {
                ActorError::Snapshot(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "session name is not a safe checkpoint path",
                ))
            })?;
        if let Ok(renew_id) = kelpie.arm_occupant_renew(
            logical_id,
            incarnation_id,
            snapshot_relpath,
            &progress_relpath,
        ) {
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
        let events = self.channel_events_for_occupant(&session.channel_id)?;
        let audience = self.audience_for(&session.channel_id, &events);
        let markdown = render_place_snapshot(
            &session.session_name,
            &session.channel_id,
            crate::unix_now().map_err(ActorError::Snapshot)?,
            &events,
            &audience,
            &self.operator_pubkey,
        );
        refresh_place_snapshot(
            self.bot.corpus_path(),
            &self.bot,
            &session.session_name,
            &markdown,
        )
        .map_err(ActorError::Snapshot)?;
        Ok(relpath)
    }

    fn audience_for(&self, channel_id: &str, events: &[IndexedRelayEvent]) -> ChannelAudience {
        self.audiences
            .get(channel_id)
            .cloned()
            .unwrap_or_else(|| ChannelAudience::from_observed(events))
    }

    pub(crate) fn ensure_session(
        &mut self,
        channel_id: &str,
        channel_display: &str,
    ) -> Result<SessionRecord, ActorError<R::Error>> {
        ensure_bot_session(&self.bot, &mut self.repository, channel_id, channel_display)
    }
}

/// Persist one ingest action without starting an occupant or sending an ask.
///
/// # Errors
///
/// Returns an error when session naming or host persistence fails.
pub fn persist_ingest<R: HostRepository>(
    bot: &Bot,
    repository: &mut R,
    action: &crate::relay::IngestAction,
    channel_display: &str,
) -> Result<TriggerOutcome, ActorError<R::Error>> {
    match action {
        crate::relay::IngestAction::TurnCandidate {
            bot_id: _,
            event_id,
            channel_id,
            reply_to_event_id,
            trigger,
        } => {
            if trigger.request().is_empty() {
                repository
                    .mark_event_processed(event_id)
                    .map_err(ActorError::Repository)?;
                return Ok(TriggerOutcome::Declined);
            }
            let display = if channel_display.is_empty() {
                channel_id.as_str()
            } else {
                channel_display
            };
            let Some(event) = repository
                .indexed_event(event_id)
                .map_err(ActorError::Repository)?
            else {
                return Ok(TriggerOutcome::Declined);
            };
            let request = TriggerRequest {
                author_pubkey: event.author_pubkey,
                request: trigger.request().to_owned(),
            };
            match ensure_bot_session(bot, repository, channel_id, display) {
                Ok(_) => {}
                Err(ActorError::UnnameableSession) => {
                    repository
                        .mark_event_processed(event_id)
                        .map_err(ActorError::Repository)?;
                    return Ok(TriggerOutcome::Declined);
                }
                Err(error) => return Err(error),
            }
            let Some(_) = repository
                .enqueue_unprocessed_turn(&NewTurn {
                    bot_id: bot.id().clone(),
                    channel_id: channel_id.clone(),
                    event_id: event_id.clone(),
                    reply_to_event_id: reply_to_event_id.clone(),
                    ask_body: Some(
                        serde_json::to_string(&request)
                            .map_err(|error| ActorError::Snapshot(io::Error::other(error)))?,
                    ),
                    publish_reply_to_event_id: Some(event_id.clone()),
                })
                .map_err(ActorError::Repository)?
            else {
                return Ok(TriggerOutcome::Duplicate);
            };
            Ok(TriggerOutcome::Queued)
        }
        crate::relay::IngestAction::Edit { event_id, .. }
        | crate::relay::IngestAction::Delete { event_id, .. } => {
            repository
                .mark_event_processed(event_id)
                .map_err(ActorError::Repository)?;
            Ok(TriggerOutcome::Declined)
        }
    }
}

fn ensure_bot_session<R: HostRepository>(
    bot: &Bot,
    repository: &mut R,
    channel_id: &str,
    channel_display: &str,
) -> Result<SessionRecord, ActorError<R::Error>> {
    if let Some(session) = repository
        .session(bot.id(), channel_id)
        .map_err(ActorError::Repository)?
    {
        return Ok(session);
    }
    let mut name_error = None;
    let name =
        SessionName::from_bot_and_channel(bot.id(), channel_id, channel_display, |candidate| {
            match repository.session_by_name(candidate) {
                Ok(existing) => existing.is_some(),
                Err(error) => {
                    name_error = Some(error);
                    true
                }
            }
        });
    if let Some(error) = name_error {
        return Err(ActorError::Repository(error));
    }
    let name = name.ok_or(ActorError::UnnameableSession)?;
    let session = SessionRecord {
        bot_id: bot.id().clone(),
        channel_id: channel_id.to_owned(),
        session_name: name.as_str().to_owned(),
        backend_session: None,
        renew_id: None,
        ask_context_event_id: None,
        ask_context_created_at: None,
    };
    repository
        .save_session(&session)
        .map_err(ActorError::Repository)?;
    Ok(session)
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

    use nostr_sdk::prelude::FinalizeEvent as _;

    use super::*;
    use crate::sqlite::SqliteRepository;
    use crate::{occupant_bootstrap, CommandOutput, CommandRunner, IndexedRelayEvent, WAITER_NAME};

    #[derive(Debug)]
    struct FakePanes {
        calls: Mutex<Vec<(String, PathBuf)>>,
        released: Mutex<Vec<String>>,
        /// Panes Herdr already reports as holding a session's name.
        claims: Mutex<std::collections::HashMap<String, ClaimedPane>>,
    }

    impl OccupantPaneAllocator for Arc<FakePanes> {
        type Error = io::Error;

        fn allocate(&self, session_name: &str, cwd: &Path) -> Result<OccupantPane, Self::Error> {
            let mut calls = self.calls.lock().expect("calls");
            calls.push((session_name.to_owned(), cwd.to_path_buf()));
            Ok(OccupantPane {
                pane_id: format!("w2:p{}", calls.len()),
                terminal_id: format!("term-{}", calls.len() + 8),
            })
        }

        fn release(&self, pane: &OccupantPane) -> Result<(), Self::Error> {
            self.released
                .lock()
                .expect("released")
                .push(pane.pane_id.clone());
            Ok(())
        }

        fn claimed(
            &self,
            session_name: &str,
            _cwd: &Path,
        ) -> Result<Option<ClaimedPane>, Self::Error> {
            Ok(self
                .claims
                .lock()
                .expect("claims")
                .get(session_name)
                .cloned())
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
            "public_name": "nostrherd",
            "delivery_transport": "socket_inbox"
        }))
    }

    fn start() -> CommandOutput {
        success(&serde_json::json!({
            "logical_agent_id": 1990,
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

    /// A start whose runtime succeeded but whose receipt cannot be read: the
    /// occupant is live in the pane even though Kelpie returns an error.
    fn start_with_unreadable_receipt() -> CommandOutput {
        success(&serde_json::json!({
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
            "logical_agent_id": 1990,
            "incarnation_id": "occupant-incarnation",
            "public_name": "bot-foobar"
        }))
    }

    fn whoami_other() -> CommandOutput {
        success(&serde_json::json!({
            "logical_agent_id": 2016,
            "incarnation_id": "twin-incarnation",
            "public_name": "bot-foobar"
        }))
    }

    fn asked(message_id: &str) -> CommandOutput {
        success(&serde_json::json!({
            "message_id": message_id,
            "operation_id": "ask-operation",
            "recipient": 1990,
            "recipient_incarnation": "occupant-incarnation",
            "delivery_outcome": "accepted"
        }))
    }

    fn retired() -> CommandOutput {
        success(&serde_json::json!({
            "operation_id": "retire-operation",
            "pane_released": false
        }))
    }

    fn pending_ask(message_id: &str) -> CommandOutput {
        success(&serde_json::json!([{
            "ask_message_id": message_id,
            "waiting_agent_id": "waiter-agent",
            "state": "open"
        }]))
    }

    fn renewed() -> CommandOutput {
        success(&serde_json::json!({
            "renew_id": "renew-id",
            "recipient": 1990,
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

    /// `whoami` for a name no live claimant answers to.
    fn unbound() -> CommandOutput {
        failure("conflict", "no ready agent for alias bot-foobar")
    }

    /// `who --resolve` for a name nothing has ever held.
    ///
    /// Kelpie answers this with the same conflict as an unbound `whoami`, so
    /// the host reads it as "nothing to continue" rather than as a failure.
    fn no_claimants() -> CommandOutput {
        failure("conflict", "no ready agent for alias bot-foobar")
    }

    /// `who --resolve` for a name whose runtimes have all ended.
    ///
    /// Shaped from a real receipt: `logical_agent_id` is a JSON number at the
    /// top level and a string inside `claimants`, and Kelpie resolves a name
    /// with no live claimant rather than reporting a conflict for it.
    fn dead_claimants() -> CommandOutput {
        success(&serde_json::json!({
            "public_name": "bot-foobar",
            "addressable": false,
            "continue": "newest_claimant",
            "logical_agent_id": 1990,
            "incarnation_id": null,
            "claimants": [
                {"logical_agent_id": "1611", "created_at_ms": 1, "live": false},
                {"logical_agent_id": "1990", "created_at_ms": 9, "live": false},
            ],
            "unresolved": []
        }))
    }

    /// The first Kelpie invocation of a subcommand.
    ///
    /// Kelpie calls are asserted by name, not by position: convergence adds
    /// probes ahead of a start, and an index-based assertion breaks on that
    /// without saying anything about the behaviour it was written to pin.
    fn call<'a>(
        calls: &'a [(Vec<String>, Vec<u8>)],
        subcommand: &str,
    ) -> &'a (Vec<String>, Vec<u8>) {
        calls
            .iter()
            .find(|(arguments, _)| arguments.get(1).is_some_and(|name| name == subcommand))
            .unwrap_or_else(|| panic!("no {subcommand} call in {calls:?}"))
    }

    /// The most recent Kelpie invocation of a subcommand.
    fn last_call<'a>(
        calls: &'a [(Vec<String>, Vec<u8>)],
        subcommand: &str,
    ) -> &'a (Vec<String>, Vec<u8>) {
        calls
            .iter()
            .rev()
            .find(|(arguments, _)| arguments.get(1).is_some_and(|name| name == subcommand))
            .unwrap_or_else(|| panic!("no {subcommand} call in {calls:?}"))
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
            "nostrherd-actor-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("corpus");
        path
    }

    fn bot() -> Bot {
        Bot::new(
            nostrherd_domain::BotId::new("bot").expect("id"),
            temp_corpus(),
            "opencode",
        )
        .expect("bot")
    }

    fn work(character: char, body: &str, reply: Option<char>) -> TriggerWork {
        TriggerWork {
            author_pubkey: "b".repeat(64),
            event_id: event_id(character),
            channel_id: "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
            channel_display: "Foobar".to_owned(),
            reply_to_event_id: reply.map(event_id),
            nostr_body: body.to_owned(),
        }
    }

    fn index_work(repository: &mut SqliteRepository, work: &TriggerWork) {
        repository
            .index_event(
                &IndexedRelayEvent {
                    event_id: work.event_id.clone(),
                    author_pubkey: work.author_pubkey.clone(),
                    created_at: 1,
                    kind: 9,
                    content: format!("bot: {}", work.nostr_body),
                    tags_json: "[]".to_owned(),
                    channel_id: Some(work.channel_id.clone()),
                    target_event_id: None,
                },
                true,
            )
            .unwrap();
    }

    #[test]
    fn queued_requester_checks_cover_legacy_rows_revocation_and_missing_identity() {
        for (stored, indexed, allowed) in [
            (false, true, false),
            (true, true, false),
            (false, false, true),
        ] {
            let (mut actor, kelpie, runner, panes) = actor([adopt()]);
            actor.operator_pubkey = "a".repeat(64);
            let work = work('c', "status", None);
            actor.bot = actor
                .bot
                .clone()
                .with_allowed_requesters(vec![actor.operator_pubkey.clone()]);
            actor.ensure_session(&work.channel_id, "Foobar").unwrap();
            if indexed {
                index_work(&mut actor.repository, &work);
            }
            if allowed {
                actor.bot = actor
                    .bot
                    .clone()
                    .with_allowed_requesters(vec![work.author_pubkey.clone()]);
            }
            actor
                .repository
                .enqueue_turn(&NewTurn {
                    bot_id: actor.bot.id().clone(),
                    channel_id: work.channel_id.clone(),
                    event_id: work.event_id.clone(),
                    reply_to_event_id: None,
                    publish_reply_to_event_id: Some(work.event_id.clone()),
                    ask_body: stored.then(|| {
                        serde_json::to_string(&TriggerRequest {
                            author_pubkey: work.author_pubkey.clone(),
                            request: work.nostr_body.clone(),
                        })
                        .unwrap()
                    }),
                })
                .unwrap();
            let waiter = kelpie.register_waiter().unwrap();
            assert_eq!(actor.resume_queued(&kelpie, &waiter).unwrap(), None);
            assert!(panes.calls.lock().unwrap().is_empty());
            assert_eq!(runner.calls.lock().unwrap().len(), 1);
            assert_eq!(
                actor
                    .repository
                    .turns_for_session(actor.bot.id(), &work.channel_id)
                    .unwrap()[0]
                    .state,
                TurnState::Cancelled
            );
        }
    }

    #[test]
    fn invalid_requester_decline_is_acknowledged_without_a_wake() {
        let (mut actor, kelpie, _runner, panes) = actor([adopt()]);
        actor.operator_pubkey = "invalid".to_owned();
        let work = TriggerWork {
            author_pubkey: "invalid".to_owned(),
            ..work('c', "status", None)
        };
        let waiter = kelpie.register_waiter().unwrap();
        assert_eq!(
            actor.handle_trigger(&kelpie, &waiter, &work).unwrap(),
            TriggerOutcome::Declined
        );
        assert!(actor.repository.event_processed(&work.event_id).unwrap());
        assert!(panes.calls.lock().unwrap().is_empty());
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
            released: Mutex::new(Vec::new()),
            claims: Mutex::new(std::collections::HashMap::new()),
        });
        let kelpie = KelpieClient::with_runner(Arc::clone(&runner));
        let repository = SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
            .expect("repository");
        (
            BotActor::new(bot(), repository, Arc::clone(&panes), "b".repeat(64)),
            kelpie,
            runner,
            panes,
        )
    }

    #[test]
    fn host_watch_fire_opens_a_normal_turn_with_a_typed_section() {
        let (mut actor, kelpie, runner, _panes) =
            actor([adopt(), whoami(), asked("ask-watch"), renewed()]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        actor
            .repository
            .save_session(&SessionRecord {
                bot_id: actor.bot.id().clone(),
                channel_id: channel_id.to_owned(),
                session_name: "bot-foobar".to_owned(),
                backend_session: None,
                renew_id: None,
                ask_context_event_id: None,
                ask_context_created_at: None,
            })
            .expect("session");
        let fire = WatchFire {
            watch_id: event_id('a'),
            wake_event_id: event_id('b'),
            source_event_id: event_id('c'),
            bot_id: actor.bot.id().clone(),
            channel_id: channel_id.to_owned(),
            author_pubkey: "d".repeat(64),
            source_channel_id: Some("elsewhere".to_owned()),
            source_kind: 9,
        };

        assert_eq!(
            actor
                .handle_watch_fire(&kelpie, &waiter, &fire)
                .expect("wake"),
            TriggerOutcome::Asked
        );
        let turn = actor
            .repository
            .turns_for_session(actor.bot.id(), channel_id)
            .expect("turns")
            .pop()
            .expect("turn");
        assert_eq!(turn.state, TurnState::Open);
        assert!(turn.publish_reply_to_event_id.is_none());
        let calls = runner.calls.lock().expect("calls");
        let body = String::from_utf8_lossy(&call(&calls, "ask").1);
        assert!(body.starts_with("## Watch event\n\n"));
        assert!(body.contains("## Context"));
    }

    #[test]
    fn host_watch_fire_queues_behind_an_open_turn() {
        let (mut actor, kelpie, runner, _panes) = actor([adopt()]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        actor
            .repository
            .save_session(&SessionRecord {
                bot_id: actor.bot.id().clone(),
                channel_id: channel_id.to_owned(),
                session_name: "bot-foobar".to_owned(),
                backend_session: None,
                renew_id: None,
                ask_context_event_id: None,
                ask_context_created_at: None,
            })
            .expect("session");
        actor
            .repository
            .enqueue_turn(&NewTurn {
                bot_id: actor.bot.id().clone(),
                channel_id: channel_id.to_owned(),
                event_id: event_id('a'),
                reply_to_event_id: None,
                ask_body: Some("## Existing wake".to_owned()),
                publish_reply_to_event_id: None,
            })
            .expect("existing turn");
        actor
            .repository
            .open_next_turn(actor.bot.id(), channel_id, "ask-open")
            .expect("open");
        let fire = WatchFire {
            watch_id: event_id('b'),
            wake_event_id: event_id('c'),
            source_event_id: event_id('d'),
            bot_id: actor.bot.id().clone(),
            channel_id: channel_id.to_owned(),
            author_pubkey: "e".repeat(64),
            source_channel_id: Some("elsewhere".to_owned()),
            source_kind: 9,
        };

        assert_eq!(
            actor
                .handle_watch_fire(&kelpie, &waiter, &fire)
                .expect("wake"),
            TriggerOutcome::Queued
        );
        assert_eq!(
            actor
                .repository
                .turns_for_session(actor.bot.id(), channel_id)
                .expect("turns")
                .iter()
                .map(|turn| turn.state)
                .collect::<Vec<_>>(),
            [TurnState::Open, TurnState::Queued]
        );
        assert_eq!(runner.calls.lock().expect("calls").len(), 1);
        assert!(actor
            .pending_scope()
            .expect("scope")
            .active_event_ids
            .is_empty());
    }

    #[test]
    fn trigger_phrase_creates_a_watched_author_scope() {
        let (mut actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-watch"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let author = "d".repeat(64);
        let trigger = work('a', &format!("watch {author} max 2"), None);

        assert_eq!(
            actor
                .handle_trigger(&kelpie, &waiter, &trigger)
                .expect("handle"),
            TriggerOutcome::Asked
        );
        assert_eq!(
            actor.pending_scope().expect("scope").watched_author_pubkeys,
            [author]
        );
    }

    #[test]
    fn a_rejected_start_without_runtime_evidence_keeps_its_pane() {
        let (mut actor, kelpie, _runner, panes) = actor([
            adopt(),
            failure(
                "rejected",
                "Herdr rejected the request with agent_pane_busy: \
                 agent target pane w2:p1 is not an available shell",
            ),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "bot: hello", None);

        let error = actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect_err("start rejected");

        assert!(error.to_string().contains("agent_pane_busy"), "{error}");
        assert!(panes.released.lock().expect("released").is_empty());
    }

    #[test]
    fn an_unproven_start_failure_keeps_the_pane_for_a_possibly_live_occupant() {
        let (mut actor, kelpie, _runner, panes) = actor([adopt(), start_with_unreadable_receipt()]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "bot: hello", None);

        let error = actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect_err("unreadable receipt");

        assert!(
            error.to_string().contains("invalid Kelpie receipt"),
            "{error}"
        );
        assert!(
            panes.released.lock().expect("released").is_empty(),
            "a pane whose occupant may be running must not be closed"
        );
    }

    #[test]
    fn a_failed_kelpie_cancel_still_deletes_the_progress_post() {
        let relay = Arc::new(crate::progress::RecordingProgressRelay::default());
        let (actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            failure("rejected", "kelpie cancel refused"),
        ]);
        let mut actor = actor
            .with_progress_relay(Arc::clone(&relay) as Arc<dyn crate::progress::ProgressRelay>);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "hello", None);
        let post_id = "e".repeat(64);
        open_turn_with_progress_post(&mut actor, &kelpie, &waiter, &trigger, &post_id);

        let error = actor
            .handle_ingest(
                &kelpie,
                &waiter,
                &crate::relay::IngestAction::Delete {
                    event_id: event_id('d'),
                    target_event_id: trigger.event_id.clone(),
                },
                &trigger.channel_display,
            )
            .expect_err("cancel refused");

        assert!(
            error.to_string().contains("kelpie cancel refused"),
            "{error}"
        );
        assert_eq!(
            relay.deletes.lock().expect("deletes").as_slice(),
            [(trigger.channel_id.clone(), post_id)],
            "the progress post is deleted even when the Kelpie cancel fails"
        );
        let post = actor
            .repository
            .progress_post("ask-1")
            .expect("row")
            .expect("progress row");
        assert!(post.ended);
    }

    #[test]
    fn first_trigger_starts_then_asks_and_stores_reply_to() {
        let (mut actor, kelpie, runner, panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
            .join(".nostrherd/places/bot-foobar.md");
        assert!(snapshot.exists());
        let startup =
            std::fs::read_to_string(actor.bot().corpus_path().join("startup.md")).expect("startup");
        assert!(startup.contains("<!-- nostrherd-contract -->"));
        assert!(startup.contains("The host stamps `**[bot]**:`"));
        assert!(startup.contains(crate::snapshot::BOT_CONDUCT_RELPATH));
        // The advice ships in the binary, so the corpus copy appears without
        // anything being installed beside the executable.
        assert!(actor
            .bot()
            .corpus_path()
            .join(crate::snapshot::BOT_CONDUCT_RELPATH)
            .is_file());
        assert!(
            std::fs::read_to_string(actor.bot().corpus_path().join("startup.md"))
                .expect("startup")
                .contains(".nostrherd/places/<your public Kelpie name>.md")
        );
        let calls = runner.calls.lock().expect("calls");
        let start = call(&calls, "start");
        assert!(String::from_utf8_lossy(&start.1).contains("Read startup.md before answering"));
        assert!(start
            .0
            .windows(2)
            .any(|pair| pair == ["--sender-id", "waiter-agent"]));
        assert_eq!(
            start.1,
            occupant_bootstrap(".nostrherd/places/bot-foobar.md").as_bytes()
        );
        let renew = call(&calls, "renew");
        assert!(!renew.0.iter().any(|argument| argument == "--sender-id"));
        let ask = call(&calls, "ask");
        assert_eq!(
            ask.0[ask
                .0
                .iter()
                .position(|arg| arg == "--idempotency-key")
                .expect("key")
                + 1],
            format!("{}:1", trigger.event_id.as_str())
        );
        assert_eq!(ask_request(&ask.1), trigger.nostr_body);
        assert!(ask_has_context(&ask.1));
        assert_eq!(waiter.identity().logical_agent_id(), "waiter-agent");
        assert_eq!(WAITER_NAME, "nostrherd");
    }

    #[test]
    fn first_trigger_names_the_occupant_from_the_place_display() {
        let (mut actor, kelpie, _runner, panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let mut trigger = work('a', "@daniel bot: hello", None);
        trigger.channel_display = "#eng".to_owned();

        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("handle");

        let session = actor
            .repository
            .session(actor.bot.id(), &trigger.channel_id)
            .expect("session")
            .expect("bound");
        assert_eq!(session.session_name, "bot-eng");
        assert_eq!(panes.calls.lock().expect("panes")[0].0, "bot-eng");
    }

    #[test]
    fn existing_session_keeps_its_stored_name() {
        let (mut actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "bot: hello", None);
        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("first");

        actor
            .ensure_session(&trigger.channel_id, "#eng")
            .expect("again");

        let session = actor
            .repository
            .session(actor.bot.id(), &trigger.channel_id)
            .expect("session")
            .expect("bound");
        assert_eq!(session.session_name, "bot-foobar");
    }

    fn ask_request(body: &[u8]) -> &str {
        crate::ask_body::ask_body_request(std::str::from_utf8(body).expect("utf8"))
            .strip_prefix("self: ")
            .expect("operator requester stamp")
    }

    fn ask_has_context(body: &[u8]) -> bool {
        std::str::from_utf8(body)
            .expect("utf8")
            .contains("## Context")
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
                        .any(|pair| pair == ["--logical-id", "1990"])
            })
            .map(|call| call.0.clone())
            .collect()
    }

    #[test]
    fn failed_renew_still_asks() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            failure("conflict", "incarnation already has a renew"),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            whoami(),
            asked("ask-2"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
        assert_eq!(ask_request(&calls.last().expect("ask").1), "bot: second");
    }

    #[test]
    fn open_turn_queues_without_a_second_ask() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            failure("conflict", "no ready agent for alias bot-foobar"),
            dead_claimants(),
            start(),
            renewed(),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
        assert_eq!(session.renew_id.as_deref(), Some("renew-id"));
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

    /// A name Kelpie has never seen starts fresh, carrying no identity.
    ///
    /// Passing `--logical-id` here would name an agent that does not exist,
    /// which is how a channel stopped dead before D62.
    #[test]
    fn an_unknown_name_starts_without_continuing_an_identity() {
        let (mut actor, kelpie, runner, panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");

        actor
            .handle_trigger(&kelpie, &waiter, &work('a', "bot: hello", None))
            .expect("asked");

        assert_eq!(start_count(&runner), 1);
        assert!(
            continued_starts(&runner).is_empty(),
            "nothing held the name, so there is no identity to continue"
        );
        assert_eq!(panes.calls.lock().expect("pane calls").len(), 1);
    }

    /// A name whose runtimes have all ended continues its newest claimant.
    ///
    /// Kelpie keeps every identity a name ever had, so the choice is the one
    /// whose conversation the channel was last having, by creation time.
    #[test]
    fn dead_claimants_continue_the_newest_identity() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            unbound(),
            dead_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");

        actor
            .handle_trigger(&kelpie, &waiter, &work('a', "bot: hello", None))
            .expect("asked");

        {
            let calls = runner.calls.lock().expect("calls");
            let (who, _) = call(&calls, "who");
            assert!(
                who.iter().any(|argument| argument == "--resolve"),
                "Kelpie picks the identity to continue, so the host must ask it to: {who:?}"
            );
        }

        let continued = continued_starts(&runner);
        assert_eq!(continued.len(), 1);
        assert!(
            continued[0]
                .windows(2)
                .any(|pair| pair == ["--logical-id", "1990"]),
            "the resolved id is a JSON number, and reaches the start as 1990: {:?}",
            continued[0]
        );
    }

    /// A pane still holding the name is reclaimed before the start.
    ///
    /// Nothing answers to the name, so a pane holding it in this bot's corpus
    /// is one whose occupant has gone. Leaving it there makes Herdr refuse the
    /// start with `agent_name_taken`, which stopped a channel for four hours.
    #[test]
    fn a_pane_still_holding_the_name_is_reclaimed_and_its_session_carried_over() {
        let (mut actor, kelpie, runner, panes) = actor([
            adopt(),
            unbound(),
            dead_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        panes.claims.lock().expect("claims").insert(
            "bot-foobar".to_owned(),
            ClaimedPane {
                pane: OccupantPane {
                    pane_id: "w1EW:p1".to_owned(),
                    terminal_id: "term_65b128dba71fc2e".to_owned(),
                },
                backend_session: Some("ses_f7e8c964affeaMRRyT4cVoGkDc".to_owned()),
            },
        );
        let waiter = kelpie.register_waiter().expect("waiter");

        actor
            .handle_trigger(&kelpie, &waiter, &work('a', "bot: hello", None))
            .expect("asked");

        assert_eq!(
            panes.released.lock().expect("released").as_slice(),
            &["w1EW:p1".to_owned()],
            "the husk holding the name must be closed before the start"
        );
        let start = call(&runner.calls.lock().expect("calls"), "start")
            .0
            .clone();
        assert!(
            start
                .windows(2)
                .any(|pair| pair == ["--session", "ses_f7e8c964affeaMRRyT4cVoGkDc"]),
            "the reclaimed pane's backend session is replayed verbatim: {start:?}"
        );
    }

    #[test]
    fn ready_open_occupant_is_left_bound_without_asking() {
        let (mut actor, kelpie, runner, panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            whoami(),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
    fn pending_scope_lists_open_turn_channels_and_event_ids() {
        let (mut actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "bot: hello", None);
        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("asked");
        let scope = actor.pending_scope().expect("scope");
        assert_eq!(scope.channel_ids, vec![trigger.channel_id.clone()]);
        assert_eq!(scope.active_event_ids, vec![trigger.event_id.clone()]);
        actor
            .repository
            .set_turn_state("ask-1", TurnState::Posted)
            .expect("posted");
        let posted = actor.pending_scope().expect("posted scope");
        assert_eq!(posted.channel_ids, vec![trigger.channel_id]);
        assert!(posted.active_event_ids.is_empty());
    }

    #[test]
    fn resume_queued_recovers_a_gone_open_occupant_without_asking() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            failure("conflict", "no ready agent for alias bot-foobar"),
            dead_claimants(),
            start(),
            renewed(),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            success(&serde_json::json!({ "incarnation_id": "broken" })),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            failure("rejected", "kelpie daemon unavailable"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
            actor([adopt(), whoami_other(), whoami(), asked("ask-2"), renewed()]);
        let waiter = kelpie.register_waiter().expect("waiter");
        actor
            .repository
            .save_session(&crate::SessionRecord {
                bot_id: actor.bot.id().clone(),
                channel_id: first_channel.to_owned(),
                session_name: "bot-aaa".to_owned(),
                backend_session: None,
                renew_id: None,
                ask_context_event_id: None,
                ask_context_created_at: None,
            })
            .expect("first session");
        actor
            .repository
            .enqueue_unprocessed_turn(&NewTurn {
                bot_id: actor.bot.id().clone(),
                channel_id: first_channel.to_owned(),
                event_id: event_id('a'),
                reply_to_event_id: None,
                ask_body: None,
                publish_reply_to_event_id: Some(event_id('a')),
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
                backend_session: None,
                renew_id: None,
                ask_context_event_id: None,
                ask_context_created_at: None,
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
                ask_body: None,
                publish_reply_to_event_id: Some(event_id('b')),
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
            ask_request(&last_call(&runner.calls.lock().expect("calls"), "ask").1),
            "second"
        );
    }

    #[test]
    fn resume_queued_starts_an_occupant_for_bootstrapping() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
                ask_body: None,
                publish_reply_to_event_id: Some(trigger.event_id.clone()),
            })
            .expect("enqueue");

        assert_eq!(
            actor.resume_queued(&kelpie, &waiter).expect("resume"),
            Some(TriggerOutcome::Asked)
        );
        let calls = runner.calls.lock().expect("calls");
        call(&calls, "start");
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn queued_turn_recovers_an_unavailable_recorded_occupant_and_drains() {
        let (mut actor, kelpie, runner, panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            whoami(),
            failure("target_unavailable", "occupant pane is gone"),
            pending_ask("ask-unavailable"),
            cancelled(),
            retired(),
            dead_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-2"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let first = work('a', "bot: first", None);
        let second = work('b', "bot: second", None);
        actor
            .repository
            .index_event(
                &IndexedRelayEvent {
                    event_id: second.event_id.clone(),
                    author_pubkey: "b".repeat(64),
                    created_at: 2,
                    kind: 9,
                    content: second.nostr_body.clone(),
                    tags_json: "[]".to_owned(),
                    channel_id: Some(second.channel_id.clone()),
                    target_event_id: None,
                },
                false,
            )
            .expect("index second");
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
            actor.resume_queued(&kelpie, &waiter).expect("recovered"),
            Some(TriggerOutcome::Asked)
        );

        let session = actor
            .repository
            .session(actor.bot.id(), &second.channel_id)
            .expect("session")
            .expect("bound");
        assert_eq!(session.renew_id.as_deref(), Some("renew-id"));
        let turns = actor
            .repository
            .turns_for_session(actor.bot.id(), &second.channel_id)
            .expect("turns");
        assert_eq!(turns[1].state, TurnState::Open);
        assert_eq!(turns[1].ask_id.as_deref(), Some("ask-2"));
        assert_eq!(start_count(&runner), 2);
        assert_eq!(continued_starts(&runner).len(), 1);
        assert_eq!(panes.calls.lock().expect("pane calls").len(), 2);
        let calls = runner.calls.lock().expect("calls");
        let retire = calls
            .iter()
            .find(|call| call.0[1] == "retire")
            .expect("retire stale incarnation");
        assert!(retire
            .0
            .windows(2)
            .any(|pair| pair == ["--incarnation", "occupant-incarnation"]));
        let cancel = calls
            .iter()
            .find(|call| call.0[1] == "cancel")
            .expect("cancel unavailable ask");
        assert_eq!(cancel.0[2], "ask-unavailable");
        let ask_keys = calls
            .iter()
            .filter(|call| call.0[1] == "ask")
            .map(|call| {
                let key = call
                    .0
                    .iter()
                    .position(|argument| argument == "--idempotency-key")
                    .expect("idempotency key");
                call.0[key + 1].clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(ask_keys.len(), 3);
        assert_eq!(
            &ask_keys[1..],
            &[
                format!("{}:2", second.event_id.as_str()),
                format!("{}:2", second.event_id.as_str())
            ]
        );
    }

    #[test]
    fn rejected_queued_ask_cancels_its_obligation() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            whoami(),
            failure("rejected", "occupant rejected the prompt"),
            pending_ask("ask-rejected"),
            cancelled(),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "bot: rejected", None);
        actor
            .repository
            .save_session(&crate::SessionRecord {
                bot_id: actor.bot.id().clone(),
                channel_id: trigger.channel_id.clone(),
                session_name: "bot-foobar".to_owned(),
                backend_session: None,
                renew_id: None,
                ask_context_event_id: None,
                ask_context_created_at: None,
            })
            .expect("session");
        actor
            .repository
            .index_event(
                &IndexedRelayEvent {
                    event_id: trigger.event_id.clone(),
                    author_pubkey: "b".repeat(64),
                    created_at: 1,
                    kind: 9,
                    content: trigger.nostr_body,
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
                channel_id: trigger.channel_id,
                event_id: trigger.event_id,
                reply_to_event_id: None,
                ask_body: None,
                publish_reply_to_event_id: Some(event_id('a')),
            })
            .expect("enqueue");

        assert!(matches!(
            actor.resume_queued(&kelpie, &waiter),
            Err(ActorError::AskNotDelivered(AskDelivery::Rejected))
        ));
        let calls = runner.calls.lock().expect("calls");
        let cancel = calls
            .iter()
            .find(|call| call.0[1] == "cancel")
            .expect("cancel rejected ask");
        assert_eq!(cancel.0[2], "ask-rejected");
    }

    #[test]
    fn resume_queued_asks_a_later_channel_when_the_first_recovery_fails() {
        let first_channel = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        let second_channel = "ab12cd34-5678-90ab-cdef-0123456789ab";
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            failure("conflict", "no ready agent for alias bot-aaa"),
            failure("conflict", "no ready agent for alias bot-aaa"),
            failure("rejected", "recovery start failed"),
            whoami(),
            asked("ask-2"),
            renewed(),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
                    backend_session: None,
                    renew_id: None,
                    ask_context_event_id: None,
                    ask_context_created_at: None,
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
                    ask_body: None,
                    publish_reply_to_event_id: Some(event_id(character)),
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
        assert_eq!(calls.iter().filter(|call| call.0[1] == "start").count(), 1);
        assert_eq!(ask_request(&last_call(&calls, "ask").1), "second");
    }

    #[test]
    fn resume_queued_skips_a_session_without_indexed_body() {
        let (mut actor, kelpie, _runner, _panes) = actor([adopt()]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
                ask_body: None,
                publish_reply_to_event_id: Some(trigger.event_id.clone()),
            })
            .expect("enqueue");

        assert_eq!(actor.resume_queued(&kelpie, &waiter).expect("skip"), None);
    }

    #[test]
    fn ingest_turn_candidate_uses_the_actor_path() {
        let (mut actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "@daniel bot: hello", Some('c'));
        let action = crate::relay::IngestAction::TurnCandidate {
            bot_id: nostrherd_domain::BotId::new("bot").expect("id"),
            event_id: trigger.event_id.clone(),
            channel_id: trigger.channel_id.clone(),
            reply_to_event_id: trigger.reply_to_event_id.clone(),
            trigger: nostrherd_domain::TriggerMatch::parse(
                "operator",
                "someone-else",
                ["operator"],
                "bot:",
                "@daniel bot: hello",
            )
            .expect("trigger"),
        };

        index_work(&mut actor.repository, &trigger);
        assert_eq!(
            actor
                .handle_ingest(&kelpie, &waiter, &action, &trigger.channel_display)
                .expect("ingest"),
            TriggerOutcome::Asked
        );
    }

    #[test]
    fn persist_ingest_queues_a_trigger_without_kelpie() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot = bot();
        let trigger = work('a', "@daniel bot: hello", Some('c'));
        index_work(&mut repository, &trigger);
        let action = crate::relay::IngestAction::TurnCandidate {
            bot_id: nostrherd_domain::BotId::new("bot").expect("id"),
            event_id: trigger.event_id.clone(),
            channel_id: trigger.channel_id.clone(),
            reply_to_event_id: trigger.reply_to_event_id.clone(),
            trigger: nostrherd_domain::TriggerMatch::parse(
                "operator",
                "someone-else",
                ["operator"],
                "bot:",
                "@daniel bot: hello",
            )
            .expect("trigger"),
        };

        assert_eq!(
            persist_ingest(&bot, &mut repository, &action, &trigger.channel_display)
                .expect("persist"),
            TriggerOutcome::Queued
        );
        assert_eq!(
            persist_ingest(&bot, &mut repository, &action, &trigger.channel_display)
                .expect("replay"),
            TriggerOutcome::Duplicate
        );
        let turns = repository
            .turns_for_session(bot.id(), &trigger.channel_id)
            .expect("turns");
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].state, TurnState::Queued);
        assert_eq!(turns[0].ask_id, None);
        assert_eq!(turns[0].reply_to_event_id, trigger.reply_to_event_id);
        assert!(repository
            .event_processed(&trigger.event_id)
            .expect("processed"));
    }

    #[test]
    fn persist_ingest_does_not_queue_an_empty_request() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot = bot();
        let event = event_id('a');
        let channel = "ab12cd34-5678-90ab-cdef-0123456789ab";
        let action = crate::relay::IngestAction::TurnCandidate {
            bot_id: nostrherd_domain::BotId::new("bot").expect("id"),
            event_id: event.clone(),
            channel_id: channel.to_owned(),
            reply_to_event_id: None,
            trigger: nostrherd_domain::TriggerMatch::parse(
                "operator",
                "someone-else",
                ["operator"],
                "bot:",
                "bot:",
            )
            .expect("trigger"),
        };

        assert_eq!(
            persist_ingest(&bot, &mut repository, &action, "Foobar").expect("persist"),
            TriggerOutcome::Declined
        );
        assert!(repository
            .turns_for_session(bot.id(), channel)
            .expect("turns")
            .is_empty());
        assert!(repository.event_processed(&event).expect("processed"));
    }

    #[test]
    fn persist_ingest_queues_a_non_uuid_channel() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot = bot();
        let event = event_id('a');
        index_work(
            &mut repository,
            &TriggerWork {
                channel_id: "not-a-uuid".to_owned(),
                ..work('a', "hi", None)
            },
        );
        let action = crate::relay::IngestAction::TurnCandidate {
            bot_id: nostrherd_domain::BotId::new("bot").expect("id"),
            event_id: event.clone(),
            channel_id: "not-a-uuid".to_owned(),
            reply_to_event_id: None,
            trigger: nostrherd_domain::TriggerMatch::parse(
                "operator",
                "someone-else",
                ["operator"],
                "bot:",
                "bot: hi",
            )
            .expect("trigger"),
        };

        assert_eq!(
            persist_ingest(&bot, &mut repository, &action, "").expect("persist"),
            TriggerOutcome::Queued
        );
        let turns = repository
            .turns_for_session(bot.id(), "not-a-uuid")
            .expect("turns");
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].state, TurnState::Queued);
        assert!(repository.event_processed(&event).expect("processed"));
    }

    #[test]
    fn handle_ingest_acks_an_unnameable_channel_without_kelpie() {
        let (mut actor, kelpie, runner, panes) = actor([adopt()]);
        actor.bot = Bot::new(
            BotId::new(&"a".repeat(32)).expect("id"),
            actor.bot.corpus_path().to_owned(),
            "opencode",
        )
        .expect("bot");
        let waiter = kelpie.register_waiter().expect("waiter");
        let event = event_id('a');
        let action = crate::relay::IngestAction::TurnCandidate {
            bot_id: actor.bot.id().clone(),
            event_id: event.clone(),
            channel_id: "not-a-uuid".to_owned(),
            reply_to_event_id: None,
            trigger: nostrherd_domain::TriggerMatch::parse(
                "operator",
                "someone-else",
                ["operator"],
                "bot:",
                "bot: hi",
            )
            .expect("trigger"),
        };

        assert_eq!(
            actor
                .handle_ingest(&kelpie, &waiter, &action, "")
                .expect("ingest"),
            TriggerOutcome::Declined
        );
        assert!(actor.repository.event_processed(&event).expect("processed"));
        assert!(actor
            .repository
            .turns_for_session(actor.bot().id(), "not-a-uuid")
            .expect("turns")
            .is_empty());
        assert_eq!(
            runner
                .calls
                .lock()
                .expect("calls")
                .iter()
                .filter(|call| call.0.get(1).is_some_and(|verb| verb != "waiter-register"))
                .count(),
            0
        );
        assert!(panes.calls.lock().expect("panes").is_empty());
    }

    #[test]
    fn empty_trigger_request_is_not_asked() {
        let (mut actor, kelpie, runner, _panes) = actor([adopt()]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
        let waiter = kelpie.register_waiter().expect("waiter");
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
        let (mut actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "bot: hello", None);
        let now = crate::unix_now().expect("now");
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
                .join(".nostrherd/places/bot-foobar.md"),
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
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            whoami(),
            asked("ask-2"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
            ask_request(&last_call(&runner.calls.lock().expect("calls"), "ask").1),
            "second"
        );
    }

    #[test]
    fn ingest_edit_cancels_open_work_and_asks_the_latest_body() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            cancelled(),
            whoami(),
            asked("ask-2"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "hello", Some('c'));
        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("first");
        let edit_id = event_id('e');
        let replacement =
            nostrherd_domain::TriggerMatch::from_body("bot: latest", "bot:").expect("edit");

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
        assert_eq!(ask_request(&calls.last().expect("ask").1), "latest");
        assert!(actor
            .repository
            .event_processed(&edit_id)
            .expect("processed"));
    }

    #[test]
    fn ingest_delete_cancels_open_work_without_asking() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            cancelled(),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
                        replacement: nostrherd_domain::TriggerMatch::from_body(
                            "bot: stale",
                            "bot:"
                        ),
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
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            whoami(),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
                        replacement: nostrherd_domain::TriggerMatch::from_body(
                            "bot: later",
                            "bot:"
                        ),
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
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            cancelled(),
            whoami(),
            asked("ask-2"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
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
                        replacement: nostrherd_domain::TriggerMatch::from_body(
                            "bot: latest",
                            "bot:",
                        ),
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
            ask_request(&last_call(&runner.calls.lock().expect("calls"), "ask").1),
            "second"
        );
    }

    struct FakeOutbound {
        event_id: String,
    }

    impl OutboundPublisher for FakeOutbound {
        type Error = crate::outbox::PublishError;

        fn prepare(
            &self,
            attempt: &crate::outbox::OutboundAttempt,
        ) -> Result<crate::outbox::PreparedOutbound, Self::Error> {
            let created_at = attempt.prepared_created_at.unwrap_or(1_700_000_000);
            let signed =
                nostr_sdk::prelude::EventBuilder::new(nostr_sdk::prelude::Kind::Custom(9), "")
                    .finalize(&nostr_sdk::prelude::Keys::generate())
                    .expect("dummy event");
            Ok(crate::outbox::PreparedOutbound::from_parts(
                signed,
                self.event_id.clone(),
                created_at,
            ))
        }

        fn publish(
            &self,
            prepared: &crate::outbox::PreparedOutbound,
        ) -> Result<String, Self::Error> {
            Ok(prepared.event_id().to_owned())
        }

        fn retryable(error: &Self::Error) -> bool {
            error.is_retryable()
        }
    }

    fn occupant_final(ask_id: &str, body: &str) -> InboxDelivery {
        crate::inbox::parse_delivery(&serde_json::json!({
            "method": "inbox.delivery",
            "params": {
                "message_id": "msg-1",
                "kind": "reply",
                "disposition": "final",
                "reply_to": ask_id,
                "body": body
            }
        }))
        .expect("delivery")
    }

    #[test]
    fn trigger_adds_in_flight_reaction() {
        let recorded = Arc::new(crate::outbox::RecordingInFlightReaction::default());
        let (actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let mut actor = actor.with_reactions(Arc::clone(&recorded) as Arc<dyn InFlightReaction>);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "hello", None);
        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("asked");
        assert_eq!(
            recorded.adds.lock().expect("adds").as_slice(),
            [trigger.event_id.as_str()]
        );
        assert!(recorded.removes.lock().expect("removes").is_empty());
    }

    #[test]
    fn queued_trigger_adds_its_own_in_flight_reaction() {
        let recorded = Arc::new(crate::outbox::RecordingInFlightReaction::default());
        let (actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let mut actor = actor.with_reactions(Arc::clone(&recorded) as Arc<dyn InFlightReaction>);
        let waiter = kelpie.register_waiter().expect("waiter");
        let first = work('a', "first", None);
        let second = work('b', "second", None);
        actor
            .handle_trigger(&kelpie, &waiter, &first)
            .expect("first");
        actor
            .handle_trigger(&kelpie, &waiter, &second)
            .expect("queued");
        assert_eq!(
            recorded.adds.lock().expect("adds").as_slice(),
            [first.event_id.as_str(), second.event_id.as_str()]
        );
    }

    #[test]
    fn duplicate_trigger_does_not_add_again() {
        let recorded = Arc::new(crate::outbox::RecordingInFlightReaction::default());
        let (actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let mut actor = actor.with_reactions(Arc::clone(&recorded) as Arc<dyn InFlightReaction>);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "hello", None);
        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("asked");
        assert_eq!(
            actor
                .handle_trigger(&kelpie, &waiter, &trigger)
                .expect("dup"),
            TriggerOutcome::Duplicate
        );
        assert_eq!(recorded.adds.lock().expect("adds").len(), 1);
    }

    #[test]
    fn delete_removes_in_flight_reaction() {
        let recorded = Arc::new(crate::outbox::RecordingInFlightReaction::default());
        let (actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            cancelled(),
        ]);
        let mut actor = actor.with_reactions(Arc::clone(&recorded) as Arc<dyn InFlightReaction>);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "hello", None);
        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("first");
        actor
            .handle_ingest(
                &kelpie,
                &waiter,
                &crate::relay::IngestAction::Delete {
                    event_id: event_id('d'),
                    target_event_id: trigger.event_id.clone(),
                },
                &trigger.channel_display,
            )
            .expect("deleted");
        assert_eq!(
            recorded.removes.lock().expect("removes").as_slice(),
            [trigger.event_id.as_str()]
        );
    }

    #[test]
    fn edit_keeps_in_flight_reaction_on_the_same_event() {
        let recorded = Arc::new(crate::outbox::RecordingInFlightReaction::default());
        let (actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            cancelled(),
            whoami(),
            asked("ask-2"),
        ]);
        let mut actor = actor.with_reactions(Arc::clone(&recorded) as Arc<dyn InFlightReaction>);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "hello", Some('c'));
        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("first");
        let replacement =
            nostrherd_domain::TriggerMatch::from_body("bot: latest", "bot:").expect("edit");
        actor
            .handle_ingest(
                &kelpie,
                &waiter,
                &crate::relay::IngestAction::Edit {
                    event_id: event_id('e'),
                    target_event_id: trigger.event_id.clone(),
                    replacement: Some(replacement),
                },
                &trigger.channel_display,
            )
            .expect("replaced");
        assert_eq!(recorded.adds.lock().expect("adds").len(), 1);
        assert!(recorded.removes.lock().expect("removes").is_empty());
    }

    #[test]
    fn occupant_final_removes_in_flight_reaction() {
        let recorded = Arc::new(crate::outbox::RecordingInFlightReaction::default());
        let (actor, kelpie, runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let mut actor = actor.with_reactions(Arc::clone(&recorded) as Arc<dyn InFlightReaction>);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "hello", None);
        index_trigger(&mut actor, &trigger);
        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("asked");
        actor
            .handle_occupant_delivery(
                &kelpie,
                &waiter,
                &FakeOutbound {
                    event_id: "d".repeat(64),
                },
                &occupant_final("ask-1", "done"),
            )
            .expect("posted");
        assert_eq!(
            recorded.removes.lock().expect("removes").as_slice(),
            [trigger.event_id.as_str()]
        );
        assert_eq!(
            actor.posted_channel_for_ask("ask-1").expect("channel"),
            Some(trigger.channel_id.clone())
        );
        assert!(runner
            .calls
            .lock()
            .expect("calls")
            .iter()
            .all(|call| call.0.iter().all(|arg| arg != "reactions")));
    }

    #[test]
    fn late_final_on_edited_trigger_keeps_in_flight_reaction() {
        let recorded = Arc::new(crate::outbox::RecordingInFlightReaction::default());
        let (actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            cancelled(),
            whoami(),
            asked("ask-2"),
        ]);
        let mut actor = actor.with_reactions(Arc::clone(&recorded) as Arc<dyn InFlightReaction>);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "hello", Some('c'));
        index_trigger(&mut actor, &trigger);
        actor
            .handle_trigger(&kelpie, &waiter, &trigger)
            .expect("first");
        let replacement =
            nostrherd_domain::TriggerMatch::from_body("bot: latest", "bot:").expect("edit");
        actor
            .handle_ingest(
                &kelpie,
                &waiter,
                &crate::relay::IngestAction::Edit {
                    event_id: event_id('e'),
                    target_event_id: trigger.event_id.clone(),
                    replacement: Some(replacement),
                },
                &trigger.channel_display,
            )
            .expect("replaced");
        actor
            .handle_occupant_delivery(
                &kelpie,
                &waiter,
                &FakeOutbound {
                    event_id: "d".repeat(64),
                },
                &occupant_final("ask-1", "stale"),
            )
            .expect("cancelled final");
        assert!(recorded.removes.lock().expect("removes").is_empty());
        assert_eq!(
            actor
                .repository
                .turn_by_ask_id("ask-2")
                .unwrap()
                .unwrap()
                .state,
            TurnState::Open
        );
    }

    fn occupant_progress(ask_id: &str, body: &str) -> InboxDelivery {
        crate::inbox::parse_delivery(&serde_json::json!({
            "method": "inbox.delivery",
            "params": {
                "message_id": "msg-progress",
                "kind": "reply",
                "disposition": "progress",
                "reply_to": ask_id,
                "body": body
            }
        }))
        .expect("delivery")
    }

    /// Trigger, deliver one progress body, and flush past the hold so the
    /// host's progress post exists with the fake publisher's id.
    fn open_turn_with_progress_post(
        actor: &mut BotActor<SqliteRepository, Arc<FakePanes>>,
        kelpie: &KelpieClient,
        waiter: &HostWaiter<'_>,
        trigger: &TriggerWork,
        post_id: &str,
    ) -> i64 {
        actor
            .handle_trigger(kelpie, waiter, trigger)
            .expect("asked");
        let publisher = FakeOutbound {
            event_id: post_id.to_owned(),
        };
        actor
            .handle_occupant_delivery(
                kelpie,
                waiter,
                &publisher,
                &occupant_progress("ask-1", "working on it"),
            )
            .expect("progress recorded");
        let now = crate::unix_now().expect("now")
            + nostrherd_domain::progress::PROGRESS_INITIAL_HOLD_SECS;
        actor.flush_progress(&publisher, now).expect("flush");
        let post = actor
            .repository
            .progress_post("ask-1")
            .expect("row")
            .expect("progress row");
        assert_eq!(post.post_event_id.as_deref(), Some(post_id));
        now
    }

    #[test]
    fn cancelled_progress_post_deleted() {
        let relay = Arc::new(crate::progress::RecordingProgressRelay::default());
        let (actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            cancelled(),
        ]);
        let mut actor = actor
            .with_progress_relay(Arc::clone(&relay) as Arc<dyn crate::progress::ProgressRelay>);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "hello", None);
        let post_id = "e".repeat(64);
        open_turn_with_progress_post(&mut actor, &kelpie, &waiter, &trigger, &post_id);
        actor
            .handle_ingest(
                &kelpie,
                &waiter,
                &crate::relay::IngestAction::Delete {
                    event_id: event_id('d'),
                    target_event_id: trigger.event_id.clone(),
                },
                &trigger.channel_display,
            )
            .expect("deleted");
        assert_eq!(
            relay.deletes.lock().expect("deletes").as_slice(),
            [(trigger.channel_id.clone(), post_id)]
        );
        let post = actor
            .repository
            .progress_post("ask-1")
            .expect("row")
            .expect("progress row");
        assert!(post.ended);
        assert_eq!(
            actor
                .repository
                .turn_by_ask_id("ask-1")
                .expect("turn")
                .expect("turn")
                .state,
            TurnState::Cancelled
        );
    }

    #[test]
    fn edit_replacement_deletes_the_old_progress_post() {
        let relay = Arc::new(crate::progress::RecordingProgressRelay::default());
        let (actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            cancelled(),
            whoami(),
            asked("ask-2"),
        ]);
        let mut actor = actor
            .with_progress_relay(Arc::clone(&relay) as Arc<dyn crate::progress::ProgressRelay>);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "hello", None);
        index_trigger(&mut actor, &trigger);
        let post_id = "e".repeat(64);
        open_turn_with_progress_post(&mut actor, &kelpie, &waiter, &trigger, &post_id);
        let replacement =
            nostrherd_domain::TriggerMatch::from_body("bot: latest", "bot:").expect("trigger");
        actor
            .handle_ingest(
                &kelpie,
                &waiter,
                &crate::relay::IngestAction::Edit {
                    event_id: event_id('f'),
                    target_event_id: trigger.event_id.clone(),
                    replacement: Some(replacement),
                },
                &trigger.channel_display,
            )
            .expect("edited");
        assert_eq!(
            relay.deletes.lock().expect("deletes").as_slice(),
            [(trigger.channel_id.clone(), post_id)]
        );
        // The replacement ask starts with no progress row of its own.
        assert!(actor
            .repository
            .progress_post("ask-2")
            .expect("row")
            .is_none());
        assert_eq!(
            actor
                .repository
                .turn_by_ask_id("ask-2")
                .expect("turn")
                .expect("turn")
                .state,
            TurnState::Open
        );
    }

    #[test]
    fn snapshot_progress_post_excluded() {
        let (mut actor, kelpie, _runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "hello", None);
        let post_id = "e".repeat(64);
        let now = open_turn_with_progress_post(&mut actor, &kelpie, &waiter, &trigger, &post_id);
        // The host indexes its own stamped kind 9 (first body, never the edits).
        for (id, content) in [
            (post_id.clone(), "**[bot]**: working on it".to_owned()),
            ("b".repeat(64), "human line stays".to_owned()),
        ] {
            actor
                .repository
                .index_event(
                    &IndexedRelayEvent {
                        event_id: EventId::parse_hex(&id).expect("id"),
                        author_pubkey: "a".repeat(64),
                        created_at: now,
                        kind: 9,
                        content,
                        tags_json: "[]".to_owned(),
                        channel_id: Some(trigger.channel_id.clone()),
                        target_event_id: None,
                    },
                    false,
                )
                .expect("index");
        }
        let session = actor
            .repository
            .session(actor.bot.id(), &trigger.channel_id)
            .expect("session")
            .expect("session");
        actor.refresh_snapshot(&session).expect("snapshot");
        let snapshot = std::fs::read_to_string(
            actor
                .bot()
                .corpus_path()
                .join(".nostrherd/places/bot-foobar.md"),
        )
        .expect("snapshot");
        assert!(snapshot.contains("human line stays"));
        assert!(!snapshot.contains("working on it"));
        assert!(!snapshot.contains(&post_id));
    }

    #[test]
    fn ask_context_excludes_progress_post() {
        let (mut actor, kelpie, runner, _panes) = actor([
            adopt(),
            unbound(),
            no_claimants(),
            start(),
            renewed(),
            whoami(),
            asked("ask-1"),
            whoami(),
            asked("ask-2"),
        ]);
        let waiter = kelpie.register_waiter().expect("waiter");
        let trigger = work('a', "hello", None);
        index_trigger(&mut actor, &trigger);
        let post_id = "e".repeat(64);
        let now = open_turn_with_progress_post(&mut actor, &kelpie, &waiter, &trigger, &post_id);
        for (id, content) in [
            (post_id.clone(), "**[bot]**: working on it".to_owned()),
            ("b".repeat(64), "and the PR?".to_owned()),
        ] {
            actor
                .repository
                .index_event(
                    &IndexedRelayEvent {
                        event_id: EventId::parse_hex(&id).expect("id"),
                        author_pubkey: "a".repeat(64),
                        created_at: now,
                        kind: 9,
                        content,
                        tags_json: "[]".to_owned(),
                        channel_id: Some(trigger.channel_id.clone()),
                        target_event_id: None,
                    },
                    false,
                )
                .expect("index");
        }
        let publisher = FakeOutbound {
            event_id: "d".repeat(64),
        };
        actor
            .handle_occupant_delivery(
                &kelpie,
                &waiter,
                &publisher,
                &occupant_final("ask-1", "done"),
            )
            .expect("final");
        let second = TriggerWork {
            event_id: event_id('c'),
            nostr_body: "later".to_owned(),
            ..work('c', "later", None)
        };
        actor
            .repository
            .index_event(
                &IndexedRelayEvent {
                    event_id: second.event_id.clone(),
                    author_pubkey: "b".repeat(64),
                    created_at: now + 5,
                    kind: 9,
                    content: "bot: later".to_owned(),
                    tags_json: "[]".to_owned(),
                    channel_id: Some(second.channel_id.clone()),
                    target_event_id: None,
                },
                false,
            )
            .expect("index second");
        actor
            .handle_trigger(&kelpie, &waiter, &second)
            .expect("second ask");
        let calls = runner.calls.lock().expect("calls");
        let body = std::str::from_utf8(&calls.last().expect("ask").1).expect("utf8");
        assert!(body.contains("## Context"));
        assert!(body.contains("and the PR?"));
        assert!(!body.contains("working on it"));
        assert!(!body.contains(&post_id));
    }
}
