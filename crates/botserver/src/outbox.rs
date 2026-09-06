//! Host publish path: classify inbox replies, persist an outbound attempt, ACK last.
//!
//! The host publishes through the nostr-sdk client it already holds
//! (D43): `BuzzPublisher` signs in-process, so the event id exists
//! before send (D28, amended) and a retry redelivers the same event,
//! which the relay dedups.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use botserver_domain::buzz::{self, BuzzEvent};
use botserver_domain::restraint::{
    evaluate_restraint, HostRestraint, RestraintVerdict, POST_CEILING_WINDOW_SECS,
};
use botserver_domain::{
    outbound_prefix_for, parse_occupant_tell, stamp_outbound, BotId, EventId, OccupantTell,
    TurnState,
};
use chrono::Timelike;
use nostr_sdk::prelude::{
    Client, Event, EventBuilder, Filter, FinalizeEvent, Keys, Kind, SingleLetterTag, Tag, Timestamp,
};

use crate::inbox::InboxDelivery;
use crate::progress::{self, ProgressRelay};
use crate::{HostRepository, IndexedRelayEvent, SessionRecord, TurnRecord};

/// Whether the claimed inbox delivery may be acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxAction {
    /// The host decided; `inbox.ack` is allowed.
    Ack,
    /// Leave the delivery queued so a later valid final can still arrive.
    Hold,
}

/// D47 restraint inputs for one publish-path evaluation: the bot's
/// configured limits plus the clock they are judged against.
#[derive(Debug, Clone)]
pub struct PublishRestraint {
    config: HostRestraint,
    now_unix: i64,
    local_minute_of_day: u16,
}

impl PublishRestraint {
    /// Restraint inputs for one publish decision.
    #[must_use]
    pub fn new(config: HostRestraint, now_unix: i64, local_minute_of_day: u16) -> Self {
        Self {
            config,
            now_unix,
            local_minute_of_day,
        }
    }

    /// The bot's live restraint on the host's local clock.
    ///
    /// # Panics
    ///
    /// Never in practice: a minute of day is at most `1_439`.
    #[must_use]
    pub fn local_now(config: &HostRestraint) -> Self {
        let now = chrono::Local::now();
        let minute_of_day =
            u16::try_from(now.hour() * 60 + now.minute()).expect("minute of day fits");
        Self::new(config.clone(), now.timestamp(), minute_of_day)
    }

    /// Inputs under which nothing is ever suppressed.
    ///
    /// # Panics
    ///
    /// Never in practice: `u32::MAX` passes the `ceiling >= 1` check.
    #[cfg(test)]
    #[must_use]
    pub fn unrestrained() -> Self {
        Self::new(
            HostRestraint::new(u32::MAX, None).expect("ceiling is at least one"),
            0,
            12 * 60,
        )
    }
}

/// Count this channel's accepted host-initiated posts in the rolling
/// ceiling window and evaluate D47 against it.
fn restraint_verdict<R: HostRepository>(
    repository: &R,
    restraint: &PublishRestraint,
    bot_id: &BotId,
    channel_id: &str,
) -> Result<RestraintVerdict, R::Error> {
    let since = restraint
        .now_unix
        .checked_sub(POST_CEILING_WINDOW_SECS)
        .unwrap_or(i64::MIN);
    let published = repository.count_host_initiated_posts(bot_id, channel_id, since)?;
    Ok(evaluate_restraint(
        &restraint.config,
        restraint.local_minute_of_day,
        published,
    ))
}

/// Notice for one suppressed host-initiated post (D47): dropped, never
/// held, so the operator learns what did not land.
fn suppressed_notice(
    verdict: &RestraintVerdict,
    bot_id: &BotId,
    channel_id: &str,
    key: &str,
) -> String {
    match verdict {
        RestraintVerdict::QuietHours(window) => format!(
            "suppressed host post from {} to {channel_id} ({key}): quiet hours {window} on the host clock; dropped",
            bot_id.as_str()
        ),
        RestraintVerdict::Ceiling { published, ceiling } => format!(
            "suppressed host post from {} to {channel_id} ({key}): post ceiling {ceiling} per 24h reached ({published} in window); dropped",
            bot_id.as_str()
        ),
        RestraintVerdict::Allow => format!(
            "suppressed host post from {} to {channel_id} ({key})",
            bot_id.as_str()
        ),
    }
}

/// How long a retryable publish failure waits before the tick resends (D48).
pub(crate) const OUTBOUND_RETRY_INTERVAL_SECS: i64 = 10;

/// Drain retries after the first send; about five minutes at the interval (D48).
pub(crate) const OUTBOUND_RETRY_CAP: i64 = 30;

/// Durable outbound attempt for one occupant final.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundAttempt {
    pub ask_id: String,
    pub body: String,
    pub channel_id: String,
    pub reply_to_event_id: Option<EventId>,
    /// Thread root resolved from the trigger's own `e` tags, or `None`
    /// for a direct reply (D43).
    pub thread_root_event_id: Option<EventId>,
    pub mention: String,
    pub outbound_event_id: Option<String>,
    /// Event id recorded before send (D28, amended by D43). A retry
    /// re-prepares the identical event and the relay dedups.
    pub prepared_event_id: Option<String>,
    /// Fixed timestamp backing `prepared_event_id`.
    pub prepared_created_at: Option<i64>,
    pub dispatched: bool,
    /// Bot that owns the attempt. Missing only on rows the migration
    /// could not attribute; the drain skips those.
    pub bot_id: Option<BotId>,
    pub retry_count: i64,
    pub last_retry_at: Option<i64>,
    pub abandoned_at: Option<i64>,
}

impl OutboundAttempt {
    /// Build an attempt with empty publish and retry fields.
    pub fn new(
        ask_id: impl Into<String>,
        body: impl Into<String>,
        channel_id: impl Into<String>,
    ) -> Self {
        Self {
            ask_id: ask_id.into(),
            body: body.into(),
            channel_id: channel_id.into(),
            reply_to_event_id: None,
            thread_root_event_id: None,
            mention: String::new(),
            outbound_event_id: None,
            prepared_event_id: None,
            prepared_created_at: None,
            dispatched: false,
            bot_id: None,
            retry_count: 0,
            last_retry_at: None,
            abandoned_at: None,
        }
    }

    fn payload_is_mutable(&self) -> bool {
        self.outbound_event_id.is_none() && self.prepared_event_id.is_none() && !self.dispatched
    }

    fn retry_is_due(&self, now: i64) -> bool {
        match self.last_retry_at {
            None => true,
            Some(last) => now.saturating_sub(last) >= OUTBOUND_RETRY_INTERVAL_SECS,
        }
    }
}

/// Failure while classifying or publishing an occupant final.
#[derive(Debug)]
pub enum OutboxError<E, P> {
    /// Host persistence failed.
    Repository(E),
    /// Relay publish failed. The delivery is not acknowledged.
    Publish(P),
}

impl<E: fmt::Display, P: fmt::Display> fmt::Display for OutboxError<E, P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Repository(error) => write!(formatter, "host persistence failed: {error}"),
            Self::Publish(error) => write!(formatter, "{error}"),
        }
    }
}

impl<E, P> std::error::Error for OutboxError<E, P>
where
    E: std::error::Error + 'static,
    P: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Repository(error) => Some(error),
            Self::Publish(error) => Some(error),
        }
    }
}

/// Failure while preparing or publishing a stamped reply.
#[derive(Debug)]
pub enum PublishError {
    /// Building or signing the event failed.
    Build(String),
    /// No relay accepted the event: transport, timeout, or a dropped
    /// connection before `OK`. A retry redelivers the same event id.
    NotAccepted { detail: String },
    /// A relay explicitly rejected the event.
    Rejected { detail: String },
}

impl fmt::Display for PublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Build(reason) => {
                write!(formatter, "failed to build the outbound event: {reason}")
            }
            Self::NotAccepted { detail } => {
                write!(formatter, "relay did not accept the event: {detail}")
            }
            Self::Rejected { detail } => write!(formatter, "relay rejected the event: {detail}"),
        }
    }
}

impl PublishError {
    /// Transport failures may redeliver. A build failure or an explicit
    /// relay rejection must not.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::NotAccepted { .. })
    }
}

impl std::error::Error for PublishError {}

/// Host NIP-25 marker while a turn is queued or open (D35).
pub const IN_FLIGHT_REACTION: &str = "⏳";

/// Best-effort in-flight marker on a triggering event.
pub trait InFlightReaction: Send + Sync {
    /// Add the in-flight emoji without failing the turn.
    fn add(&self, trigger_event_id: &EventId);
    /// Remove the in-flight emoji without failing the turn.
    fn remove(&self, trigger_event_id: &EventId);
}

/// Ignore in-flight reaction side effects.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopInFlightReaction;

impl InFlightReaction for NoopInFlightReaction {
    fn add(&self, _trigger_event_id: &EventId) {}

    fn remove(&self, _trigger_event_id: &EventId) {}
}

impl InFlightReaction for Arc<dyn InFlightReaction> {
    fn add(&self, trigger_event_id: &EventId) {
        (**self).add(trigger_event_id);
    }

    fn remove(&self, trigger_event_id: &EventId) {
        (**self).remove(trigger_event_id);
    }
}

/// A signed outbound event ready to send (D43).
///
/// `event_id` and `created_at` are recorded before send, so a retry
/// re-prepares the identical event and the relay dedups the redelivery.
#[derive(Debug, Clone)]
pub struct PreparedOutbound {
    event: Event,
    event_id: String,
    created_at: i64,
}

impl PreparedOutbound {
    pub(crate) fn from_parts(event: Event, event_id: String, created_at: i64) -> Self {
        Self {
            event,
            event_id,
            created_at,
        }
    }

    /// Event id to record before send (D28, amended by D43).
    #[must_use]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    /// Fixed timestamp that makes re-preparation deterministic.
    #[must_use]
    pub fn created_at(&self) -> i64 {
        self.created_at
    }
}

/// Publish one outbound attempt over the host nostr connection.
pub trait OutboundPublisher {
    /// Publisher failure type.
    type Error: fmt::Display;

    /// Build and sign the event without sending it.
    ///
    /// Preparing the same recorded attempt twice returns the same event
    /// id: the timestamp is fixed at first preparation.
    ///
    /// # Errors
    ///
    /// Returns an error when the event cannot be built or signed.
    fn prepare(&self, attempt: &OutboundAttempt) -> Result<PreparedOutbound, Self::Error>;

    /// Send the prepared event. Returns the accepted event id.
    ///
    /// # Errors
    ///
    /// Returns an error when no relay accepts the event.
    fn publish(&self, prepared: &PreparedOutbound) -> Result<String, Self::Error>;

    /// Return whether this error may call send again.
    fn retryable(error: &Self::Error) -> bool {
        let _ = error;
        true
    }
}

/// Host nostr-client adapter for stamped replies and in-flight
/// reactions (D43).
///
/// Construction must happen inside the tokio runtime: the sync actor
/// layer bridges to the client through `block_in_place`.
#[derive(Clone)]
pub struct BuzzPublisher {
    client: Client,
    keys: Keys,
    relay_url: String,
    handle: tokio::runtime::Handle,
}

impl fmt::Debug for BuzzPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BuzzPublisher")
    }
}

impl BuzzPublisher {
    /// Wrap the host relay client for outbound publishes.
    #[must_use]
    pub fn new(client: Client, keys: Keys, relay_url: impl Into<String>) -> Self {
        Self {
            client,
            keys,
            relay_url: relay_url.into(),
            handle: tokio::runtime::Handle::current(),
        }
    }

    /// Sign and send one Buzz-shaped event; returns its event id.
    ///
    /// The [`OutboundPublisher`] path uses this for stamped replies and
    /// the in-flight marker; the live E2E also drives each Buzz kind
    /// (9, 40003, 9005, 7) through it so schema drift fails tests.
    ///
    /// # Errors
    ///
    /// Returns an error when the event cannot be signed or no relay
    /// accepts it.
    pub async fn send_buzz(&self, event: &BuzzEvent) -> Result<String, PublishError> {
        let mut builder = EventBuilder::new(Kind::Custom(event.kind()), event.content());
        for tag in event.tags() {
            let tag = Tag::parse(tag.iter().map(String::as_str))
                .map_err(|error| PublishError::Build(error.to_string()))?;
            builder = builder.tag(tag);
        }
        let signed = builder
            .finalize(&self.keys)
            .map_err(|error| PublishError::Build(error.to_string()))?;
        let event_id = signed.id.to_hex();
        self.deliver(&signed).await?;
        Ok(event_id)
    }

    /// Send one signed event to the host relay.
    async fn deliver(&self, signed: &Event) -> Result<(), PublishError> {
        let relay = self
            .client
            .relay(self.relay_url.as_str())
            .await
            .map_err(|error| PublishError::NotAccepted {
                detail: error.to_string(),
            })?
            .ok_or_else(|| PublishError::NotAccepted {
                detail: format!("relay {} is not registered", self.relay_url),
            })?;
        relay.send_event(signed).await.map_err(|error| {
            if matches!(error.kind(), nostr_sdk::error::ErrorKind::Rejected) {
                PublishError::Rejected {
                    detail: error.to_string(),
                }
            } else {
                PublishError::NotAccepted {
                    detail: error.to_string(),
                }
            }
        })?;
        Ok(())
    }

    /// Add the in-flight marker (D35, published by the host client).
    async fn add_reaction(&self, trigger_event_id: &EventId) -> Result<(), PublishError> {
        let event = buzz::reaction(trigger_event_id, IN_FLIGHT_REACTION);
        self.send_buzz(&event).await.map(|_| ())
    }

    /// Remove every in-flight marker this operator has on the trigger:
    /// find the operator's kind-7 reactions, then publish a kind-5
    /// removal of each (D35, matching buzz `reactions remove`).
    async fn remove_reaction(&self, trigger_event_id: &EventId) -> Result<(), PublishError> {
        let filter = Filter::new()
            .kind(Kind::Custom(buzz::REACTION_KIND))
            .author(self.keys.public_key())
            .custom_tag(SingleLetterTag::LOWERCASE_E, trigger_event_id.as_str());
        let events = self
            .client
            .fetch_events(filter)
            .timeout(Duration::from_secs(5))
            .await
            .map_err(|error| PublishError::NotAccepted {
                detail: error.to_string(),
            })?;
        let matches = events
            .into_iter()
            .filter(|event| event.content == IN_FLIGHT_REACTION)
            .collect::<Vec<_>>();
        for event in matches {
            let reaction_id = EventId::parse_hex(&event.id.to_hex())
                .ok_or_else(|| PublishError::Build("reaction id was not 64-hex".to_owned()))?;
            let removal = buzz::reaction_removal(&reaction_id);
            self.send_buzz(&removal).await?;
        }
        Ok(())
    }
}

impl OutboundPublisher for BuzzPublisher {
    type Error = PublishError;

    fn prepare(&self, attempt: &OutboundAttempt) -> Result<PreparedOutbound, Self::Error> {
        let created_at = attempt
            .prepared_created_at
            .unwrap_or_else(|| crate::unix_now().unwrap_or_default());
        let event = attempt_buzz_event(attempt);
        let mut builder = EventBuilder::new(Kind::Custom(event.kind()), event.content());
        for tag in event.tags() {
            let tag = Tag::parse(tag.iter().map(String::as_str))
                .map_err(|error| PublishError::Build(error.to_string()))?;
            builder = builder.tag(tag);
        }
        let signed = builder
            .custom_created_at(Timestamp::from(
                u64::try_from(created_at)
                    .map_err(|error| PublishError::Build(error.to_string()))?,
            ))
            .finalize(&self.keys)
            .map_err(|error| PublishError::Build(error.to_string()))?;
        let event_id = signed.id.to_hex();
        if let Some(recorded) = &attempt.prepared_event_id {
            if recorded != &event_id {
                return Err(PublishError::Build(format!(
                    "recorded prepared id {recorded} does not match the rebuilt event {event_id}"
                )));
            }
        }
        Ok(PreparedOutbound::from_parts(signed, event_id, created_at))
    }

    fn publish(&self, prepared: &PreparedOutbound) -> Result<String, Self::Error> {
        let publisher = self.clone();
        let event = prepared.event.clone();
        let handle = publisher.handle.clone();
        tokio::task::block_in_place(move || {
            handle.block_on(async move { publisher.deliver(&event).await })
        })?;
        Ok(prepared.event_id.clone())
    }

    fn retryable(error: &Self::Error) -> bool {
        error.is_retryable()
    }
}

impl InFlightReaction for BuzzPublisher {
    fn add(&self, trigger_event_id: &EventId) {
        let publisher = self.clone();
        let trigger = trigger_event_id.clone();
        let handle = publisher.handle.clone();
        if let Err(error) = tokio::task::block_in_place(move || {
            handle.block_on(async move { publisher.add_reaction(&trigger).await })
        }) {
            eprintln!("operator notice: in-flight reaction add failed: {error}");
        }
    }

    fn remove(&self, trigger_event_id: &EventId) {
        let publisher = self.clone();
        let trigger = trigger_event_id.clone();
        let handle = publisher.handle.clone();
        if let Err(error) = tokio::task::block_in_place(move || {
            handle.block_on(async move { publisher.remove_reaction(&trigger).await })
        }) {
            eprintln!("operator notice: in-flight reaction remove failed: {error}");
        }
    }
}

impl ProgressRelay for BuzzPublisher {
    fn edit(
        &self,
        _ask_id: &str,
        channel_id: &str,
        post_event_id: &EventId,
        content: &str,
    ) -> Result<crate::progress::ProgressRelayDispatch, PublishError> {
        let event = buzz::message_edit(channel_id, post_event_id, content);
        self.send_buzz_blocking(&event)?;
        Ok(crate::progress::ProgressRelayDispatch::Accepted)
    }

    fn delete(
        &self,
        _ask_id: &str,
        channel_id: &str,
        post_event_id: &EventId,
    ) -> Result<crate::progress::ProgressRelayDispatch, PublishError> {
        let event = buzz::message_delete(channel_id, post_event_id);
        self.send_buzz_blocking(&event)?;
        Ok(crate::progress::ProgressRelayDispatch::Accepted)
    }
}

impl BuzzPublisher {
    /// Bridge one Buzz event send onto the runtime from the sync actor layer.
    fn send_buzz_blocking(&self, event: &BuzzEvent) -> Result<(), PublishError> {
        let publisher = self.clone();
        let event = event.clone();
        let handle = publisher.handle.clone();
        tokio::task::block_in_place(move || {
            handle.block_on(async move { publisher.send_buzz(&event).await })
        })
        .map(|_| ())
    }
}

/// Stamped kind-9 channel message for one attempt (D43).
fn attempt_buzz_event(attempt: &OutboundAttempt) -> BuzzEvent {
    let thread_tags = match (&attempt.reply_to_event_id, &attempt.thread_root_event_id) {
        (Some(trigger), thread_root) => buzz::reply_thread_tags(trigger, thread_root.as_ref()),
        (None, _) => Vec::new(),
    };
    buzz::channel_message(
        &attempt.channel_id,
        &attempt.body,
        &thread_tags,
        Some(&attempt.mention),
    )
}

/// Classify one inbox delivery against a persisted turn.
#[must_use]
pub fn classify_delivery(delivery: &InboxDelivery, turn: Option<&TurnRecord>) -> InboxAction {
    if delivery.kind() == "tell" {
        return InboxAction::Ack;
    }
    match decide(delivery, turn) {
        Decision::Hold => InboxAction::Hold,
        Decision::AckWithoutPublish | Decision::Publish { .. } | Decision::Progress { .. } => {
            InboxAction::Ack
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Decision {
    AckWithoutPublish,
    Hold,
    Publish {
        body: String,
    },
    /// Record the progress body for the refresh tick (D42), then ACK.
    Progress {
        body: String,
    },
}

fn decide(delivery: &InboxDelivery, turn: Option<&TurnRecord>) -> Decision {
    let Some(turn) = turn else {
        return Decision::Hold;
    };
    if delivery.kind() == "reply" && delivery.disposition() == Some("progress") {
        return Decision::Progress {
            body: delivery.body().to_owned(),
        };
    }
    if delivery.kind() != "reply" {
        return Decision::AckWithoutPublish;
    }
    if delivery.disposition() != Some("final") {
        return Decision::Hold;
    }
    let body = delivery.body().trim();
    if body.is_empty() {
        if turn.state == TurnState::Posted {
            return Decision::AckWithoutPublish;
        }
        return Decision::Hold;
    }
    match turn.state {
        TurnState::Cancelled | TurnState::Posted | TurnState::Failed => Decision::AckWithoutPublish,
        TurnState::Open => Decision::Publish {
            body: delivery.body().to_owned(),
        },
        TurnState::Queued => Decision::Hold,
    }
}

fn unpublishable_tell_feedback(message_id: &str) -> String {
    format!(
        "your tell {message_id} did not publish: the body had no publishable text (empty, unclosed, or more than one routing tag). To quote the tag as prose, write \\<botserver and \\</botserver>."
    )
}

fn unroutable_tell_feedback(message_id: &str) -> String {
    format!("your tell {message_id} did not publish: the destination did not route.")
}

fn handle_occupant_tell<R, P>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    restraint: &PublishRestraint,
    occupant_feedback: &mut impl FnMut(&str, &str),
    delivery: &InboxDelivery,
) -> Result<InboxAction, OutboxError<R::Error, P::Error>>
where
    R: HostRepository,
    P: OutboundPublisher,
{
    let Some(session) = occupant_session(repository, delivery).map_err(OutboxError::Repository)?
    else {
        return Ok(InboxAction::Ack);
    };
    let Some(parsed) = parse_occupant_tell(delivery.body()) else {
        notice(&format!(
            "occupant tell {} from {} had no publishable body",
            delivery.message_id(),
            session.session_name
        ));
        occupant_feedback(
            &session.session_name,
            &unpublishable_tell_feedback(delivery.message_id()),
        );
        return Ok(InboxAction::Ack);
    };
    let Some(destination) =
        route_tell(repository, &session, &parsed).map_err(OutboxError::Repository)?
    else {
        notice(&format!(
            "occupant tell {} from {} did not route",
            delivery.message_id(),
            session.session_name
        ));
        occupant_feedback(
            &session.session_name,
            &unroutable_tell_feedback(delivery.message_id()),
        );
        return Ok(InboxAction::Ack);
    };
    publish_initiated(
        repository,
        publisher,
        notice,
        restraint,
        delivery.message_id(),
        &parsed.body,
        &destination,
        &session.bot_id,
    )
}

fn occupant_session<R: HostRepository>(
    repository: &R,
    delivery: &InboxDelivery,
) -> Result<Option<SessionRecord>, R::Error> {
    let name = delivery.sender_public_name();
    let id = delivery.sender_agent_id();
    let by_name = name
        .map(|value| repository.session_by_name(value))
        .transpose()?
        .flatten();
    let by_id = id
        .map(|value| repository.session_by_occupant_logical_id(value))
        .transpose()?
        .flatten();
    Ok(match (name, id, by_name, by_id) {
        (Some(_), Some(id), Some(named), Some(bound))
            if named == bound && named.occupant_logical_id.as_deref() == Some(id) =>
        {
            Some(named)
        }
        (Some(_), None, Some(named), None) => Some(named),
        (None, Some(_), None, Some(bound)) => Some(bound),
        _ => None,
    })
}

fn route_tell<R: HostRepository>(
    repository: &R,
    session: &SessionRecord,
    parsed: &OccupantTell,
) -> Result<Option<SessionRecord>, R::Error> {
    let Some(to) = parsed.to.as_deref() else {
        return Ok(Some(session.clone()));
    };
    let mut matches = Vec::new();
    push_unique(&mut matches, repository.session(&session.bot_id, to)?);
    push_unique(
        &mut matches,
        named_for_bot(repository, &session.bot_id, to)?,
    );
    let prefixed = format!("{}-{to}", session.bot_id.as_str());
    if prefixed != to {
        push_unique(
            &mut matches,
            named_for_bot(repository, &session.bot_id, &prefixed)?,
        );
    }
    Ok((matches.len() == 1).then(|| matches.remove(0)))
}

fn named_for_bot<R: HostRepository>(
    repository: &R,
    bot_id: &BotId,
    name: &str,
) -> Result<Option<SessionRecord>, R::Error> {
    Ok(repository
        .session_by_name(name)?
        .filter(|session| session.bot_id == *bot_id))
}

fn push_unique(matches: &mut Vec<SessionRecord>, session: Option<SessionRecord>) {
    if let Some(session) = session {
        if !matches
            .iter()
            .any(|existing| existing.channel_id == session.channel_id)
        {
            matches.push(session);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_initiated<R, P>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    restraint: &PublishRestraint,
    message_id: &str,
    body: &str,
    destination: &SessionRecord,
    bot_id: &BotId,
) -> Result<InboxAction, OutboxError<R::Error, P::Error>>
where
    R: HostRepository,
    P: OutboundPublisher,
{
    let mut attempt = repository
        .outbound_attempt(message_id)
        .map_err(OutboxError::Repository)?
        .unwrap_or_else(|| OutboundAttempt {
            bot_id: Some(bot_id.clone()),
            ..OutboundAttempt::new(message_id, body, destination.channel_id.clone())
        });
    if attempt.payload_is_mutable() {
        body.clone_into(&mut attempt.body);
        attempt.channel_id.clone_from(&destination.channel_id);
        attempt.reply_to_event_id = None;
        attempt.thread_root_event_id = None;
        attempt.mention.clear();
    }
    repository
        .save_outbound_attempt(&attempt)
        .map_err(OutboxError::Repository)?;
    if attempt.outbound_event_id.is_some() || attempt.abandoned_at.is_some() {
        return Ok(InboxAction::Ack);
    }
    if attempt.bot_id.is_none() {
        attempt.bot_id = Some(bot_id.clone());
    }
    if attempt.dispatched && attempt.prepared_event_id.is_none() {
        // Attempt recorded before prepared ids existed: a send may have
        // landed under the old path. Never send again.
        notice(&format!(
            "not retrying outbound for tell {message_id}; send already invoked"
        ));
        return Ok(InboxAction::Ack);
    }
    if attempt.prepared_event_id.is_none() {
        let verdict = restraint_verdict(repository, restraint, bot_id, &destination.channel_id)
            .map_err(OutboxError::Repository)?;
        if verdict != RestraintVerdict::Allow {
            notice(&suppressed_notice(
                &verdict,
                bot_id,
                &destination.channel_id,
                message_id,
            ));
            return Ok(InboxAction::Ack);
        }
    }
    let mut to_publish = attempt.clone();
    to_publish.body = stamp_outbound(&attempt.body, &outbound_prefix_for(bot_id));
    match record_and_send(publisher, &mut to_publish, |attempt| {
        repository.save_outbound_attempt(attempt)
    })
    .map_err(OutboxError::Repository)?
    {
        SendOutcome::Accepted(event_id) => {
            let _ = repository
                .mark_outbound_accepted(message_id, &event_id)
                .map_err(OutboxError::Repository)?;
            repository
                .note_host_initiated_post(
                    message_id,
                    bot_id,
                    &destination.channel_id,
                    restraint.now_unix,
                )
                .map_err(OutboxError::Repository)?;
            Ok(InboxAction::Ack)
        }
        SendOutcome::PrepareFailed(error) | SendOutcome::Rejected(error) => {
            notice(&format!(
                "not retrying outbound for tell {message_id}: {error}"
            ));
            stop_tell_retries(repository, &to_publish).map_err(OutboxError::Repository)?;
            Ok(InboxAction::Ack)
        }
        SendOutcome::Retry(error) => Err(OutboxError::Publish(error)),
    }
}

/// Outcome of [`record_and_send`]: prepare, record, and send one
/// stamped attempt.
///
/// A build failure is terminal for the attempt; a retryable transport
/// failure keeps the recorded prepared id so the retry redelivers the
/// same event and the relay dedups (D43).
pub(crate) enum SendOutcome<E> {
    /// A relay accepted the event.
    Accepted(String),
    /// The event could not be built and must not be sent.
    PrepareFailed(E),
    /// The event was not sent and must not be sent again.
    Rejected(E),
    /// The event was not accepted; redelivering is safe.
    Retry(E),
}

/// Prepare, durably record, and send one stamped attempt.
pub(crate) fn record_and_send<P, E>(
    publisher: &P,
    to_publish: &mut OutboundAttempt,
    mut save: impl FnMut(&OutboundAttempt) -> Result<(), E>,
) -> Result<SendOutcome<P::Error>, E>
where
    P: OutboundPublisher,
{
    let prepared = match publisher.prepare(to_publish) {
        Ok(prepared) => prepared,
        Err(error) => return Ok(SendOutcome::PrepareFailed(error)),
    };
    if to_publish.prepared_event_id.is_none() {
        to_publish.prepared_event_id = Some(prepared.event_id().to_owned());
        to_publish.prepared_created_at = Some(prepared.created_at());
    }
    to_publish.dispatched = true;
    save(to_publish)?;
    match publisher.publish(&prepared) {
        Ok(event_id) => Ok(SendOutcome::Accepted(event_id)),
        Err(error) if !P::retryable(&error) => Ok(SendOutcome::Rejected(error)),
        Err(error) => {
            to_publish.dispatched = false;
            save(to_publish)?;
            Ok(SendOutcome::Retry(error))
        }
    }
}

/// Persist, publish, and decide ACK for one occupant delivery.
///
/// # Errors
///
/// Returns an error when persistence or publish fails. A publish failure does
/// not ACK, so reconnect can retry the same outbound attempt.
pub fn handle_delivery<R, P>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    restraint: &PublishRestraint,
    delivery: &InboxDelivery,
) -> Result<InboxAction, OutboxError<R::Error, P::Error>>
where
    R: HostRepository,
    P: OutboundPublisher,
{
    handle_delivery_with(
        repository,
        publisher,
        notice,
        restraint,
        delivery,
        &NoopInFlightReaction,
    )
}

/// Persist, publish, and clear the in-flight marker for one occupant delivery.
///
/// # Errors
///
/// Returns an error when persistence or publish fails. A publish failure does
/// not ACK, so reconnect can retry the same outbound attempt.
pub fn handle_delivery_with<R, P, I>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    restraint: &PublishRestraint,
    delivery: &InboxDelivery,
    reactions: &I,
) -> Result<InboxAction, OutboxError<R::Error, P::Error>>
where
    R: HostRepository,
    P: OutboundPublisher,
    I: InFlightReaction,
{
    handle_delivery_with_feedback(
        repository,
        publisher,
        notice,
        restraint,
        delivery,
        reactions,
        &mut |_, _| {},
    )
}

/// Persist, publish, and notify the occupant when a tell cannot be posted.
///
/// # Errors
///
/// Returns an error when persistence or publish fails. A publish failure does
/// not ACK, so reconnect can retry the same outbound attempt.
pub fn handle_delivery_with_feedback<R, P, I>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    restraint: &PublishRestraint,
    delivery: &InboxDelivery,
    reactions: &I,
    occupant_feedback: &mut impl FnMut(&str, &str),
) -> Result<InboxAction, OutboxError<R::Error, P::Error>>
where
    R: HostRepository,
    P: OutboundPublisher,
    I: InFlightReaction,
{
    if delivery.kind() == "tell" {
        return handle_occupant_tell(
            repository,
            publisher,
            notice,
            restraint,
            occupant_feedback,
            delivery,
        );
    }
    let Some(ask_id) = delivery.reply_to() else {
        return Ok(InboxAction::Ack);
    };
    let turn = repository
        .turn_by_ask_id(ask_id)
        .map_err(OutboxError::Repository)?;
    match decide(delivery, turn.as_ref()) {
        Decision::Hold => {
            if delivery.disposition() == Some("final") && delivery.body().trim().is_empty() {
                notice(&format!(
                    "empty occupant final for ask {ask_id}; leaving the obligation open"
                ));
            }
            Ok(InboxAction::Hold)
        }
        Decision::AckWithoutPublish => Ok(InboxAction::Ack),
        Decision::Progress { body } => {
            if let Some(turn) = turn {
                progress::record_progress(
                    repository,
                    notice,
                    &turn,
                    &body,
                    crate::unix_now().unwrap_or_default(),
                )
                .map_err(OutboxError::Repository)?;
            }
            Ok(InboxAction::Ack)
        }
        Decision::Publish { body } => match turn {
            Some(turn) => complete_outbound_with(
                repository,
                publisher,
                notice,
                restraint,
                &turn,
                Some(&body),
                reactions,
            ),
            None => Ok(InboxAction::Hold),
        },
    }
}

/// Finish publish for an open turn, using a stored attempt when `body` is None.
///
/// # Errors
///
/// Returns an error when persistence or a retryable publish fails.
pub fn complete_outbound<R, P>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    restraint: &PublishRestraint,
    turn: &TurnRecord,
    body: Option<&str>,
) -> Result<InboxAction, OutboxError<R::Error, P::Error>>
where
    R: HostRepository,
    P: OutboundPublisher,
{
    complete_outbound_with(
        repository,
        publisher,
        notice,
        restraint,
        turn,
        body,
        &NoopInFlightReaction,
    )
}

/// Finish publish and clear the in-flight marker when the turn is terminal.
///
/// # Errors
///
/// Returns an error when persistence or a retryable publish fails.
#[allow(clippy::too_many_lines)]
pub fn complete_outbound_with<R, P, I>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    restraint: &PublishRestraint,
    turn: &TurnRecord,
    body: Option<&str>,
    reactions: &I,
) -> Result<InboxAction, OutboxError<R::Error, P::Error>>
where
    R: HostRepository,
    P: OutboundPublisher,
    I: InFlightReaction,
{
    let Some(ask_id) = turn.ask_id.as_deref() else {
        return Ok(InboxAction::Hold);
    };
    let indexed = match turn.publish_reply_to_event_id.as_ref() {
        Some(event_id) => repository
            .indexed_event(event_id)
            .map_err(OutboxError::Repository)?,
        None => None,
    };
    let mention = indexed
        .as_ref()
        .map_or_else(String::new, |event: &IndexedRelayEvent| {
            event.author_pubkey.clone()
        });
    let thread_root_event_id = match turn.publish_reply_to_event_id.as_ref() {
        Some(event_id) => {
            crate::thread_root_for(repository, event_id).map_err(OutboxError::Repository)?
        }
        None => None,
    };
    let mut attempt = repository
        .outbound_attempt(ask_id)
        .map_err(OutboxError::Repository)?
        .unwrap_or_else(|| OutboundAttempt {
            reply_to_event_id: turn.publish_reply_to_event_id.clone(),
            thread_root_event_id: thread_root_event_id.clone(),
            mention,
            bot_id: Some(turn.bot_id.clone()),
            ..OutboundAttempt::new(ask_id, body.unwrap_or(""), turn.channel_id.clone())
        });
    if attempt.payload_is_mutable() {
        if let Some(body) = body {
            body.clone_into(&mut attempt.body);
        }
        attempt.channel_id.clone_from(&turn.channel_id);
        attempt
            .reply_to_event_id
            .clone_from(&turn.publish_reply_to_event_id);
        attempt.thread_root_event_id = thread_root_event_id;
    }
    repository
        .save_outbound_attempt(&attempt)
        .map_err(OutboxError::Repository)?;
    if attempt.bot_id.is_none() {
        attempt.bot_id = Some(turn.bot_id.clone());
    }
    if attempt.abandoned_at.is_some() {
        return fail_turn(repository, reactions, turn, ask_id);
    }
    if attempt.outbound_event_id.is_some() {
        if turn.ask_body.is_some() {
            // Accepted earlier: backfill the D47 ledger idempotently, in
            // case the crash happened between send and the note.
            repository
                .note_host_initiated_post(
                    ask_id,
                    &turn.bot_id,
                    &turn.channel_id,
                    restraint.now_unix,
                )
                .map_err(OutboxError::Repository)?;
        }
        return mark_posted(repository, reactions, turn, ask_id);
    }
    if attempt.dispatched && attempt.prepared_event_id.is_none() {
        // Attempt recorded before prepared ids existed: a send may have
        // landed under the old path. Never send again.
        notice(&format!(
            "not retrying outbound for ask {ask_id}; send already invoked"
        ));
        let _ = repository
            .set_turn_state(ask_id, TurnState::Failed)
            .map_err(OutboxError::Repository)?;
        if let Some(event_id) = turn.publish_reply_to_event_id.as_ref() {
            reactions.remove(event_id);
        }
        return Ok(InboxAction::Ack);
    }
    if turn.ask_body.is_some() && attempt.prepared_event_id.is_none() {
        // Host-initiated wake (D45, D46): the final is unprompted, so the
        // D47 restraint gates a first publish here. A prepared event is
        // already on the wire (D43); re-gating a retry would drop a post
        // the relay may already have. A suppressed first publish ends
        // the turn without sending.
        let verdict = restraint_verdict(repository, restraint, &turn.bot_id, &turn.channel_id)
            .map_err(OutboxError::Repository)?;
        if verdict != RestraintVerdict::Allow {
            notice(&suppressed_notice(
                &verdict,
                &turn.bot_id,
                &turn.channel_id,
                ask_id,
            ));
            let _ = repository
                .set_turn_state(ask_id, TurnState::Failed)
                .map_err(OutboxError::Repository)?;
            if let Some(event_id) = turn.publish_reply_to_event_id.as_ref() {
                reactions.remove(event_id);
            }
            return Ok(InboxAction::Ack);
        }
    }
    let claimed = repository
        .claim_turn_for_publish(ask_id)
        .map_err(OutboxError::Repository)?;
    let still_open = repository
        .turn_by_ask_id(ask_id)
        .map_err(OutboxError::Repository)?
        .is_some_and(|turn| turn.state == TurnState::Open);
    if !claimed && !still_open {
        if let Some(event_id) = turn.publish_reply_to_event_id.as_ref() {
            reactions.remove(event_id);
        }
        return Ok(InboxAction::Ack);
    }
    let mut to_publish = attempt.clone();
    to_publish.body = stamp_outbound(&attempt.body, &outbound_prefix_for(&turn.bot_id));
    match record_and_send(publisher, &mut to_publish, |attempt| {
        repository.save_outbound_attempt(attempt)
    })
    .map_err(OutboxError::Repository)?
    {
        SendOutcome::Accepted(event_id) => {
            if !repository
                .mark_outbound_accepted(ask_id, &event_id)
                .map_err(OutboxError::Repository)?
            {
                notice(&format!(
                    "outbound event id for ask {ask_id} did not replace a prior id"
                ));
            }
            if turn.ask_body.is_some() {
                repository
                    .note_host_initiated_post(
                        ask_id,
                        &turn.bot_id,
                        &turn.channel_id,
                        restraint.now_unix,
                    )
                    .map_err(OutboxError::Repository)?;
            }
        }
        SendOutcome::PrepareFailed(error) | SendOutcome::Rejected(error) => {
            notice(&format!("not retrying outbound for ask {ask_id}: {error}"));
            let _ = repository
                .set_turn_state(ask_id, TurnState::Failed)
                .map_err(OutboxError::Repository)?;
            if let Some(event_id) = turn.publish_reply_to_event_id.as_ref() {
                reactions.remove(event_id);
            }
            return Ok(InboxAction::Ack);
        }
        SendOutcome::Retry(error) => {
            let _ = repository.release_publish_claim(ask_id);
            return Err(OutboxError::Publish(error));
        }
    }
    mark_posted(repository, reactions, turn, ask_id)
}

/// Record `failed` and clear the marker (D35) without publishing.
fn fail_turn<R, P, I>(
    repository: &mut R,
    reactions: &I,
    turn: &TurnRecord,
    ask_id: &str,
) -> Result<InboxAction, OutboxError<R::Error, P>>
where
    R: HostRepository,
    I: InFlightReaction,
{
    let _ = repository
        .set_turn_state(ask_id, TurnState::Failed)
        .map_err(OutboxError::Repository)?;
    if let Some(event_id) = turn.publish_reply_to_event_id.as_ref() {
        reactions.remove(event_id);
    }
    Ok(InboxAction::Ack)
}

/// Whether this undispatched attempt should be resent on the current tick (D47).
#[must_use]
pub fn outbound_retry_due(attempt: &OutboundAttempt, now: i64) -> bool {
    attempt.abandoned_at.is_none()
        && attempt.outbound_event_id.is_none()
        && attempt.retry_is_due(now)
}

/// Bound, notice, and stop retrying one outbound attempt (D47).
///
/// # Errors
///
/// Returns an error when persistence fails.
pub fn abandon_outbound<R, I>(
    repository: &mut R,
    notice: &mut impl FnMut(&str),
    reactions: &I,
    attempt: &OutboundAttempt,
    now: i64,
    reason: &str,
) -> Result<(), R::Error>
where
    R: HostRepository,
    I: InFlightReaction,
{
    let mut abandoned = attempt.clone();
    abandoned.abandoned_at = Some(now);
    repository.save_outbound_attempt(&abandoned)?;
    notice(&format!(
        "abandoned outbound for {} after {} retries: {reason}",
        attempt.ask_id, attempt.retry_count
    ));
    if let Some(turn) = repository.turn_by_ask_id(&attempt.ask_id)? {
        if turn.state == TurnState::Open {
            let _ = repository.release_publish_claim(&attempt.ask_id)?;
            let _ = repository.set_turn_state(&attempt.ask_id, TurnState::Failed)?;
            if let Some(event_id) = turn.publish_reply_to_event_id.as_ref() {
                reactions.remove(event_id);
            }
        }
    }
    Ok(())
}

/// Resend one undispatched attempt, reusing its prepared event id (D47).
///
/// Does not ACK. A later reconnect finds `outbound_event_id` or
/// `abandoned_at` and ACKs without a second send.
///
/// # Errors
///
/// Returns an error when persistence or a retryable publish fails.
pub fn retry_undispatched<R, P, I>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    restraint: &PublishRestraint,
    reactions: &I,
    attempt: &OutboundAttempt,
    bot_id: &BotId,
    now: i64,
) -> Result<InboxAction, OutboxError<R::Error, P::Error>>
where
    R: HostRepository,
    P: OutboundPublisher,
    I: InFlightReaction,
{
    if attempt.abandoned_at.is_some() || attempt.outbound_event_id.is_some() {
        return Ok(InboxAction::Ack);
    }
    if attempt.retry_count >= OUTBOUND_RETRY_CAP
        || (attempt.dispatched && attempt.prepared_event_id.is_none())
    {
        let reason = if attempt.dispatched && attempt.prepared_event_id.is_none() {
            "send already invoked without a prepared event id"
        } else {
            "retry bound reached"
        };
        abandon_outbound(repository, notice, reactions, attempt, now, reason)
            .map_err(OutboxError::Repository)?;
        return Ok(InboxAction::Ack);
    }
    let mut paced = attempt.clone();
    paced.retry_count = attempt.retry_count.saturating_add(1);
    paced.last_retry_at = Some(now);
    if paced.bot_id.is_none() {
        paced.bot_id = Some(bot_id.clone());
    }
    repository
        .save_outbound_attempt(&paced)
        .map_err(OutboxError::Repository)?;
    if let Some(turn) = repository
        .turn_by_ask_id(&paced.ask_id)
        .map_err(OutboxError::Repository)?
    {
        if turn.state != TurnState::Open {
            let mut abandoned = paced.clone();
            abandoned.abandoned_at = Some(now);
            repository
                .save_outbound_attempt(&abandoned)
                .map_err(OutboxError::Repository)?;
            return Ok(InboxAction::Ack);
        }
        return complete_outbound_with(
            repository,
            publisher,
            notice,
            restraint,
            &turn,
            None,
            reactions,
        );
    }
    republish_tell(repository, publisher, notice, &paced, bot_id)
}

fn republish_tell<R, P>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    attempt: &OutboundAttempt,
    bot_id: &BotId,
) -> Result<InboxAction, OutboxError<R::Error, P::Error>>
where
    R: HostRepository,
    P: OutboundPublisher,
{
    if attempt.dispatched && attempt.prepared_event_id.is_none() {
        notice(&format!(
            "not retrying outbound for tell {}; send already invoked",
            attempt.ask_id
        ));
        return Ok(InboxAction::Ack);
    }
    let mut to_publish = attempt.clone();
    to_publish.body = stamp_outbound(&attempt.body, &outbound_prefix_for(bot_id));
    match record_and_send(publisher, &mut to_publish, |attempt| {
        repository.save_outbound_attempt(attempt)
    })
    .map_err(OutboxError::Repository)?
    {
        SendOutcome::Accepted(event_id) => {
            let _ = repository
                .mark_outbound_accepted(&attempt.ask_id, &event_id)
                .map_err(OutboxError::Repository)?;
            Ok(InboxAction::Ack)
        }
        SendOutcome::PrepareFailed(error) | SendOutcome::Rejected(error) => {
            notice(&format!(
                "not retrying outbound for tell {}: {error}",
                attempt.ask_id
            ));
            stop_tell_retries(repository, &to_publish).map_err(OutboxError::Repository)?;
            Ok(InboxAction::Ack)
        }
        SendOutcome::Retry(error) => Err(OutboxError::Publish(error)),
    }
}

fn stop_tell_retries<R: HostRepository>(
    repository: &mut R,
    attempt: &OutboundAttempt,
) -> Result<(), R::Error> {
    let mut stopped = attempt.clone();
    stopped.abandoned_at = Some(crate::unix_now().unwrap_or_default());
    repository.save_outbound_attempt(&stopped)
}

/// Drop pending progress (D42), record `posted`, and clear the marker (D35).
fn mark_posted<R, P, I>(
    repository: &mut R,
    reactions: &I,
    turn: &TurnRecord,
    ask_id: &str,
) -> Result<InboxAction, OutboxError<R::Error, P>>
where
    R: HostRepository,
    I: InFlightReaction,
{
    progress::discard_pending(repository, ask_id).map_err(OutboxError::Repository)?;
    let _ = repository
        .set_turn_state(ask_id, TurnState::Posted)
        .map_err(OutboxError::Repository)?;
    if let Some(event_id) = turn.publish_reply_to_event_id.as_ref() {
        reactions.remove(event_id);
    }
    Ok(InboxAction::Ack)
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct RecordingInFlightReaction {
    pub adds: std::sync::Mutex<Vec<String>>,
    pub removes: std::sync::Mutex<Vec<String>>,
}

#[cfg(test)]
impl InFlightReaction for RecordingInFlightReaction {
    fn add(&self, trigger_event_id: &EventId) {
        self.adds
            .lock()
            .expect("adds")
            .push(trigger_event_id.as_str().to_owned());
    }

    fn remove(&self, trigger_event_id: &EventId) {
        self.removes
            .lock()
            .expect("removes")
            .push(trigger_event_id.as_str().to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        event_id, fake_event_id, open_repo, open_repo_for, FakePublisher, CHANNEL,
    };
    use botserver_domain::restraint::QuietHours;
    use botserver_domain::BotId;

    struct NonRetryPublisher;

    impl OutboundPublisher for NonRetryPublisher {
        type Error = PublishError;

        fn prepare(&self, attempt: &OutboundAttempt) -> Result<PreparedOutbound, Self::Error> {
            let created_at = attempt.prepared_created_at.unwrap_or(1_700_000_000);
            let event_id = attempt
                .prepared_event_id
                .clone()
                .unwrap_or_else(|| fake_event_id(attempt, created_at));
            let signed =
                nostr_sdk::prelude::EventBuilder::new(nostr_sdk::prelude::Kind::Custom(9), "")
                    .finalize(&nostr_sdk::prelude::Keys::generate())
                    .expect("dummy event");
            Ok(PreparedOutbound::from_parts(signed, event_id, created_at))
        }

        fn publish(&self, _prepared: &PreparedOutbound) -> Result<String, Self::Error> {
            Err(PublishError::Rejected {
                detail: "delivery_unknown: timeout".to_owned(),
            })
        }

        fn retryable(error: &Self::Error) -> bool {
            error.is_retryable()
        }
    }

    fn delivery(disposition: &str, reply_to: &str, body: &str) -> InboxDelivery {
        parse_test_delivery(disposition, reply_to, body, "reply")
    }

    fn parse_test_delivery(
        disposition: &str,
        reply_to: &str,
        body: &str,
        kind: &str,
    ) -> InboxDelivery {
        crate::inbox::parse_delivery(&serde_json::json!({
            "method": "inbox.delivery",
            "params": {
                "message_id": "msg-1",
                "kind": kind,
                "disposition": disposition,
                "reply_to": reply_to,
                "body": body
            }
        }))
        .expect("delivery")
    }

    fn notices() -> impl FnMut(&str) {
        |_| {}
    }

    fn restrained(
        ceiling: u32,
        quiet_hours: Option<&str>,
        now_unix: i64,
        minute_of_day: u16,
    ) -> PublishRestraint {
        PublishRestraint::new(
            HostRestraint::new(ceiling, quiet_hours.and_then(QuietHours::parse))
                .expect("restraint"),
            now_unix,
            minute_of_day,
        )
    }

    fn bot_id() -> BotId {
        BotId::new("bot").expect("bot")
    }

    #[test]
    fn progress_acks_without_publish() {
        let (mut repository, publisher) = open_repo();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("progress", "ask-1", "working"),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(
            publisher.calls.lock().expect("calls").is_empty(),
            "the delivery handler never relays progress; the refresh tick does (D42)"
        );
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Open
        );
        // The row is durable before the ACK, with the trigger as reply target.
        let post = repository
            .progress_post("ask-1")
            .unwrap()
            .expect("progress row");
        assert_eq!(post.pending_body.as_deref(), Some("working"));
        assert_eq!(post.reply_to_event_id, event_id('a'));
        assert!(post.post_event_id.is_none());
        assert!(post.prepared_event_id.is_none());
    }

    #[test]
    fn final_after_progress_discards_the_pending_body() {
        let (mut repository, publisher) = open_repo();
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("progress", "ask-1", "almost there"),
        )
        .expect("progress");
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "done"),
        )
        .expect("final");
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["[bot]: done".to_owned()]
        );
        let post = repository
            .progress_post("ask-1")
            .unwrap()
            .expect("progress row");
        assert!(post.pending_body.is_none(), "final first discards it");
        assert!(post.post_event_id.is_none(), "no progress post was created");
    }

    #[test]
    fn empty_final_is_held_and_not_acked() {
        let (mut repository, publisher) = open_repo();
        let mut notices = Vec::new();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut |notice| notices.push(notice.to_owned()),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "  \n"),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Hold);
        assert!(publisher.calls.lock().expect("calls").is_empty());
        assert_eq!(notices.len(), 1);
        assert!(repository.claim_turn_for_publish("ask-1").unwrap());
    }

    #[test]
    fn cancelled_turn_acks_without_publish() {
        let (mut repository, publisher) = open_repo();
        repository
            .set_turn_state("ask-1", TurnState::Cancelled)
            .unwrap();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "late"),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(publisher.calls.lock().expect("calls").is_empty());
    }

    #[test]
    fn unknown_reply_to_is_held() {
        let (mut repository, publisher) = open_repo();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-unknown", "hello"),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Hold);
        assert!(publisher.calls.lock().expect("calls").is_empty());
    }

    #[test]
    fn final_with_text_publishes_then_acks() {
        let (mut repository, publisher) = open_repo();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "hello"),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["[bot]: hello".to_owned()]
        );
        let turn = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        assert_eq!(turn.state, TurnState::Posted);
        let attempt = repository.outbound_attempt("ask-1").unwrap().unwrap();
        let prepared_id = attempt.prepared_event_id.clone().expect("prepared id");
        assert_eq!(
            attempt.outbound_event_id.as_deref(),
            Some(prepared_id.as_str()),
            "the accepted id is the prepared id"
        );
        assert_eq!(attempt.prepared_created_at, Some(1_700_000_000));
        assert_eq!(attempt.reply_to_event_id, Some(event_id('a')));
        assert_eq!(attempt.mention, "c".repeat(64));
        assert_eq!(
            attempt.body, "[bot]: hello",
            "the row records the stamped body the prepared id signed"
        );
    }

    #[test]
    fn host_wake_final_publishes_without_a_reply_or_mention() {
        let (mut repository, publisher) = open_repo();
        repository.execute_batch_for_test(
            "UPDATE turns SET publish_reply_to_event_id = NULL,
                 ask_body = '## Watch event';",
        );

        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "watched author posted"),
        )
        .expect("handle");

        assert_eq!(action, InboxAction::Ack);
        let attempt = repository.outbound_attempt("ask-1").unwrap().unwrap();
        assert_eq!(attempt.body, "[bot]: watched author posted");
        assert!(attempt.reply_to_event_id.is_none());
        assert!(attempt.mention.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn buzz_publisher_reprepares_recorded_attempt_with_same_id() {
        let keys = Keys::generate();
        let publisher =
            BuzzPublisher::new(Client::builder().build(), keys.clone(), "ws://127.0.0.1:1");
        let mut attempt = OutboundAttempt {
            reply_to_event_id: Some(event_id('a')),
            mention: keys.public_key().to_hex(),
            ..OutboundAttempt::new("ask-1", "[bot]: hello", crate::test_support::CHANNEL)
        };

        attempt.prepared_created_at = Some(1_700_000_000);
        let first = publisher.prepare(&attempt).expect("first prepare");
        assert_eq!(first.created_at(), 1_700_000_000);
        attempt.prepared_event_id = Some(first.event_id().to_owned());
        attempt.prepared_created_at = Some(first.created_at());
        let rebuilt = publisher.prepare(&attempt).expect("reprepare");
        assert_eq!(rebuilt.event_id(), first.event_id());

        attempt.prepared_event_id = Some("f".repeat(64));
        assert!(matches!(
            publisher.prepare(&attempt),
            Err(PublishError::Build(_))
        ));
    }

    #[test]
    fn crash_after_dispatch_with_prepared_event_redelivers_same_id() {
        let (mut repository, publisher) = open_repo();
        repository
            .save_outbound_attempt(&OutboundAttempt {
                reply_to_event_id: Some(event_id('a')),
                mention: "c".repeat(64),
                prepared_event_id: Some("e".repeat(64)),
                prepared_created_at: Some(1_700_000_000),
                dispatched: true,
                bot_id: Some(BotId::new("bot").expect("bot")),
                ..OutboundAttempt::new("ask-1", "hello", crate::test_support::CHANNEL)
            })
            .unwrap();
        repository.claim_turn_for_publish("ask-1").unwrap();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "hello"),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        // The recorded prepared id is redelivered untouched and accepted.
        let sends = publisher.sends.lock().expect("sends");
        assert_eq!(sends.as_slice(), ["e".repeat(64).as_str()]);
        let attempt = repository.outbound_attempt("ask-1").unwrap().unwrap();
        assert_eq!(
            attempt.prepared_event_id.as_deref(),
            Some("e".repeat(64).as_str())
        );
        assert_eq!(attempt.prepared_created_at, Some(1_700_000_000));
        assert_eq!(
            attempt.outbound_event_id.as_deref(),
            Some("e".repeat(64).as_str())
        );
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Posted
        );
    }

    #[test]
    fn pr_bot_final_publishes_pr_stamp_once() {
        let (mut repository, publisher) = open_repo_for("pr");
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "hello"),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["[pr]: hello".to_owned()]
        );
        let (mut repository, publisher) = open_repo_for("pr");
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "  [pr]: already  "),
        )
        .expect("handle");
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["[pr]: already".to_owned()]
        );
    }

    #[test]
    fn crash_after_accept_retries_the_same_event() {
        let (mut repository, publisher) = open_repo();
        repository
            .save_outbound_attempt(&OutboundAttempt {
                reply_to_event_id: Some(event_id('a')),
                mention: "c".repeat(64),
                outbound_event_id: Some("d".repeat(64)),
                prepared_event_id: Some("d".repeat(64)),
                prepared_created_at: Some(1_700_000_000),
                dispatched: true,
                bot_id: Some(BotId::new("bot").expect("bot")),
                ..OutboundAttempt::new("ask-1", "hello", crate::test_support::CHANNEL)
            })
            .unwrap();
        repository.claim_turn_for_publish("ask-1").unwrap();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "hello"),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(publisher.calls.lock().expect("calls").is_empty());
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Posted
        );
    }

    #[test]
    fn legacy_dispatch_without_prepared_event_does_not_publish_again() {
        let (mut repository, publisher) = open_repo();
        repository
            .save_outbound_attempt(&OutboundAttempt {
                reply_to_event_id: Some(event_id('a')),
                mention: "c".repeat(64),
                dispatched: true,
                bot_id: Some(BotId::new("bot").expect("bot")),
                ..OutboundAttempt::new("ask-1", "hello", crate::test_support::CHANNEL)
            })
            .unwrap();
        repository.claim_turn_for_publish("ask-1").unwrap();
        let mut notices = Vec::new();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut |notice| notices.push(notice.to_owned()),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "hello"),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(publisher.calls.lock().expect("calls").is_empty());
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Failed
        );
        assert_eq!(notices.len(), 1);
    }

    #[test]
    fn missing_reply_to_acks_without_publish() {
        let (mut repository, publisher) = open_repo();
        let delivery = crate::inbox::parse_delivery(&serde_json::json!({
            "method": "inbox.delivery",
            "params": {
                "message_id": "msg-1",
                "kind": "tell",
                "body": "hi"
            }
        }))
        .expect("delivery");
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery,
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(publisher.calls.lock().expect("calls").is_empty());
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Open
        );
    }

    fn occupant_tell(
        message_id: &str,
        body: &str,
        sender_name: Option<&str>,
        sender_id: Option<&str>,
    ) -> InboxDelivery {
        crate::inbox::parse_delivery(&serde_json::json!({
            "method": "inbox.delivery",
            "params": {
                "message_id": message_id,
                "kind": "tell",
                "body": body,
                "sender_public_name": sender_name,
                "sender_agent_id": sender_id
            }
        }))
        .expect("delivery")
    }

    #[test]
    fn agreeing_tell_identity_posts() {
        let (mut repository, publisher) = open_repo();
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &occupant_tell(
                "tell-both",
                "both fields",
                Some("bot-foobar"),
                Some("occupant-agent"),
            ),
        )
        .expect("handle");
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["[bot]: both fields".to_owned()]
        );
    }

    #[test]
    fn known_occupant_tell_posts_without_trigger_reply_to() {
        let (mut repository, publisher) = open_repo();
        let reactions = RecordingInFlightReaction::default();
        let action = handle_delivery_with(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &occupant_tell("tell-1", "queue is clear", Some("bot-foobar"), None),
            &reactions,
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["[bot]: queue is clear".to_owned()]
        );
        assert_eq!(
            publisher.reply_to.lock().expect("reply_to").as_slice(),
            &[None]
        );
        assert!(reactions.adds.lock().expect("adds").is_empty());
        assert!(reactions.removes.lock().expect("removes").is_empty());
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Open
        );
        let attempt = repository.outbound_attempt("tell-1").unwrap().unwrap();
        assert!(attempt.reply_to_event_id.is_none());
        assert!(attempt.mention.is_empty());
        assert_eq!(attempt.bot_id.as_ref().map(BotId::as_str), Some("bot"));
    }

    #[test]
    fn occupant_tell_tag_routes_and_drops_scratch() {
        let (mut repository, publisher) = open_repo();
        let eng = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        repository
            .save_session(&SessionRecord {
                bot_id: BotId::new("bot").expect("bot"),
                channel_id: eng.to_owned(),
                session_name: "bot-eng".to_owned(),
                occupant_logical_id: Some("eng-occupant".to_owned()),
                renew_id: None,
                ask_context_event_id: None,
                ask_context_created_at: None,
            })
            .unwrap();
        let body = "scratch the human should not see\n\n<botserver to=\"eng\">\nqueue is clear except divine-mobile#8013\n</botserver>\n";
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &occupant_tell("tell-2", body, Some("bot-foobar"), None),
        )
        .expect("handle");
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["[bot]: queue is clear except divine-mobile#8013".to_owned()]
        );
        let attempt = repository.outbound_attempt("tell-2").unwrap().unwrap();
        assert_eq!(attempt.channel_id, eng);
        assert!(attempt.reply_to_event_id.is_none());
    }

    #[test]
    fn unknown_tell_destination_does_not_post() {
        let (mut repository, publisher) = open_repo();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &occupant_tell(
                "tell-3",
                "<botserver to=\"missing\">nope</botserver>",
                Some("bot-foobar"),
                None,
            ),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(publisher.calls.lock().expect("calls").is_empty());
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Open
        );
    }

    #[test]
    fn unknown_named_sender_tell_does_not_post() {
        let (mut repository, publisher) = open_repo();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &occupant_tell("tell-5", "hi", Some("stranger"), None),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(publisher.calls.lock().expect("calls").is_empty());
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Open
        );
    }

    #[test]
    fn disagreeing_tell_identity_does_not_post() {
        let (mut repository, publisher) = open_repo();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &occupant_tell("tell-6", "hi", Some("bot-foobar"), Some("other-occupant")),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(publisher.calls.lock().expect("calls").is_empty());
    }

    #[test]
    fn known_occupant_unroutable_tell_notices() {
        let (mut repository, publisher) = open_repo();
        let mut notices = Vec::new();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut |notice| notices.push(notice.to_owned()),
            &PublishRestraint::unrestrained(),
            &occupant_tell(
                "tell-7",
                "<botserver to=\"missing\">nope</botserver>",
                Some("bot-foobar"),
                None,
            ),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(publisher.calls.lock().expect("calls").is_empty());
        assert_eq!(notices.len(), 1);
        assert!(notices[0].contains("did not route"));
    }

    #[test]
    fn occupant_logical_id_tell_posts_to_that_session() {
        let (mut repository, publisher) = open_repo();
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &occupant_tell("tell-4", "from id", None, Some("occupant-agent")),
        )
        .expect("handle");
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["[bot]: from id".to_owned()]
        );
    }

    #[test]
    fn posted_duplicate_empty_final_acks() {
        let (mut repository, publisher) = open_repo();
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "hello"),
        )
        .unwrap();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", " "),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert_eq!(publisher.calls.lock().expect("calls").len(), 1);
    }

    #[test]
    fn claimed_turn_keeps_the_landing_reply() {
        let (mut repository, publisher) = open_repo();
        assert!(repository.claim_turn_for_publish("ask-1").unwrap());
        assert!(repository
            .cancel_unclaimed_turn(&event_id('a'))
            .unwrap()
            .is_none());
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "landing"),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Posted
        );
    }

    #[test]
    fn non_retryable_buzz_failure_does_not_publish_again() {
        let (mut repository, _publisher) = open_repo();
        let mut notices = Vec::new();
        let action = handle_delivery(
            &mut repository,
            &NonRetryPublisher,
            &mut |notice| notices.push(notice.to_owned()),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "hello"),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Failed
        );
        assert!(notices
            .iter()
            .any(|notice| notice.contains("delivery_unknown")));
        let again = handle_delivery(
            &mut repository,
            &NonRetryPublisher,
            &mut |_: &str| {},
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "hello"),
        )
        .expect("second");
        assert_eq!(again, InboxAction::Ack);
    }

    #[test]
    fn rejected_and_built_events_are_not_retryable_but_transport_is() {
        let rejected = PublishError::Rejected {
            detail: "invalid: bad kind".to_owned(),
        };
        assert!(!rejected.is_retryable());
        let built = PublishError::Build("bad tag".to_owned());
        assert!(!built.is_retryable());
        let transport = PublishError::NotAccepted {
            detail: "relay not connected".to_owned(),
        };
        assert!(transport.is_retryable());
    }

    #[test]
    fn in_flight_reaction_marker_is_visually_distinct() {
        assert_ne!(IN_FLIGHT_REACTION, "👀");
        assert_ne!(IN_FLIGHT_REACTION, "💬");
    }

    #[test]
    fn posted_turn_removes_in_flight_reaction() {
        let (mut repository, publisher) = open_repo();
        let reactions = RecordingInFlightReaction::default();
        handle_delivery_with(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "hello"),
            &reactions,
        )
        .expect("handle");
        assert_eq!(
            reactions.removes.lock().expect("removes").as_slice(),
            [event_id('a').as_str()]
        );
        assert!(reactions.adds.lock().expect("adds").is_empty());
    }

    #[test]
    fn failed_turn_removes_in_flight_reaction() {
        let (mut repository, _publisher) = open_repo();
        let reactions = RecordingInFlightReaction::default();
        handle_delivery_with(
            &mut repository,
            &NonRetryPublisher,
            &mut |_: &str| {},
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "hello"),
            &reactions,
        )
        .expect("handle");
        assert_eq!(
            reactions.removes.lock().expect("removes").as_slice(),
            [event_id('a').as_str()]
        );
    }

    #[test]
    fn progress_does_not_remove_in_flight_reaction() {
        let (mut repository, publisher) = open_repo();
        let reactions = RecordingInFlightReaction::default();
        handle_delivery_with(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("progress", "ask-1", "working"),
            &reactions,
        )
        .expect("handle");
        assert!(reactions.removes.lock().expect("removes").is_empty());
    }

    #[test]
    fn retryable_publish_failure_keeps_in_flight_reaction() {
        let (mut repository, publisher) = open_repo();
        *publisher.fail.lock().expect("fail") = Some(PublishError::NotAccepted {
            detail: "connection dropped before OK".to_owned(),
        });
        let reactions = RecordingInFlightReaction::default();
        let error = handle_delivery_with(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "hello"),
            &reactions,
        )
        .expect_err("retryable");
        assert!(matches!(error, OutboxError::Publish(_)));
        assert!(reactions.removes.lock().expect("removes").is_empty());
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Open
        );
    }

    #[test]
    fn posted_transition_retries_after_progress_read_failure() {
        let (mut repository, publisher) = open_repo();
        repository.execute_batch_for_test(
            "INSERT INTO progress_posts(ask_id, channel_id, reply_to_event_id, opened_at)
             VALUES ('ask-1', 'ab12cd34-5678-90ab-cdef-0123456789ab',
                     'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz', 1);",
        );
        let reactions = RecordingInFlightReaction::default();
        let error = handle_delivery_with(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "hello"),
            &reactions,
        )
        .expect_err("invalid progress row");
        assert!(matches!(error, OutboxError::Repository(_)));
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Open,
            "Posted is committed only after fallible progress cleanup"
        );
        assert!(reactions.removes.lock().expect("removes").is_empty());

        repository.execute_batch_for_test("DELETE FROM progress_posts WHERE ask_id = 'ask-1';");
        let action = handle_delivery_with(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &delivery("final", "ask-1", "hello"),
            &reactions,
        )
        .expect("retry");
        assert_eq!(action, InboxAction::Ack);
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Posted
        );
        assert_eq!(reactions.removes.lock().expect("removes").len(), 1);
        assert_eq!(publisher.sends.lock().expect("sends").len(), 1);
    }

    #[test]
    fn host_tell_beyond_the_ceiling_is_dropped_with_a_notice() {
        let (mut repository, publisher) = open_repo();
        let restraint = restrained(1, None, 1_700_000_000, 12 * 60);
        let mut notices = Vec::new();
        handle_delivery(
            &mut repository,
            &publisher,
            &mut |notice| notices.push(notice.to_owned()),
            &restraint,
            &occupant_tell("tell-a", "one", Some("bot-foobar"), None),
        )
        .expect("first");
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut |notice| notices.push(notice.to_owned()),
            &restraint,
            &occupant_tell("tell-b", "two", Some("bot-foobar"), None),
        )
        .expect("second");
        assert_eq!(action, InboxAction::Ack);
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["[bot]: one".to_owned()]
        );
        assert!(notices
            .iter()
            .any(|notice| notice.contains("post ceiling 1") && notice.contains("dropped")));
        assert_eq!(
            repository
                .count_host_initiated_posts(&bot_id(), CHANNEL, 0)
                .unwrap(),
            1
        );
    }

    #[test]
    fn quiet_hours_drop_a_host_tell_with_a_notice() {
        let (mut repository, publisher) = open_repo();
        let mut notices = Vec::new();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut |notice| notices.push(notice.to_owned()),
            &restrained(24, Some("23:00-07:00"), 1_700_000_000, 23 * 60),
            &occupant_tell("tell-quiet", "later", Some("bot-foobar"), None),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(publisher.calls.lock().expect("calls").is_empty());
        assert!(notices.iter().any(|notice| {
            notice.contains("quiet hours 23:00-07:00") && notice.contains("dropped")
        }));
        assert_eq!(
            repository
                .count_host_initiated_posts(&bot_id(), CHANNEL, 0)
                .unwrap(),
            0
        );
    }

    #[test]
    fn ceiling_is_per_channel_and_several_tells_share_it() {
        let (mut repository, publisher) = open_repo();
        let eng = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        repository
            .save_session(&SessionRecord {
                bot_id: bot_id(),
                channel_id: eng.to_owned(),
                session_name: "bot-eng".to_owned(),
                occupant_logical_id: Some("eng-occupant".to_owned()),
                renew_id: None,
                ask_context_event_id: None,
                ask_context_created_at: None,
            })
            .unwrap();
        let restraint = restrained(1, None, 1_700_000_000, 12 * 60);
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &restraint,
            &occupant_tell("tell-home", "home", Some("bot-foobar"), None),
        )
        .expect("home");
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &restraint,
            &occupant_tell("tell-home-2", "again", Some("bot-foobar"), None),
        )
        .expect("home again");
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &restraint,
            &occupant_tell(
                "tell-eng",
                "<botserver to=\"eng\">eng</botserver>",
                Some("bot-foobar"),
                None,
            ),
        )
        .expect("eng");
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["[bot]: home".to_owned(), "[bot]: eng".to_owned()]
        );
        assert_eq!(
            repository
                .count_host_initiated_posts(&bot_id(), CHANNEL, 0)
                .unwrap(),
            1
        );
        assert_eq!(
            repository
                .count_host_initiated_posts(&bot_id(), eng, 0)
                .unwrap(),
            1
        );
    }

    #[test]
    fn trigger_answer_publishes_regardless_of_the_ceiling() {
        let (mut repository, publisher) = open_repo();
        let restraint = restrained(1, None, 1_700_000_000, 12 * 60);
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &restraint,
            &occupant_tell("tell-cap", "unprompted", Some("bot-foobar"), None),
        )
        .expect("tell");
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &restraint,
            &delivery("final", "ask-1", "the answer"),
        )
        .expect("final");
        assert_eq!(action, InboxAction::Ack);
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &[
                "[bot]: unprompted".to_owned(),
                "[bot]: the answer".to_owned()
            ]
        );
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Posted
        );
        assert_eq!(
            repository
                .count_host_initiated_posts(&bot_id(), CHANNEL, 0)
                .unwrap(),
            1
        );
    }

    #[test]
    fn prepared_tell_retry_is_not_re_gated_by_quiet_hours() {
        let (mut repository, publisher) = open_repo();
        repository
            .save_outbound_attempt(&OutboundAttempt {
                ask_id: "tell-retry".to_owned(),
                body: "already sent".to_owned(),
                channel_id: CHANNEL.to_owned(),
                reply_to_event_id: None,
                thread_root_event_id: None,
                mention: String::new(),
                outbound_event_id: None,
                prepared_event_id: Some("e".repeat(64)),
                prepared_created_at: Some(1_700_000_000),
                dispatched: true,
            })
            .unwrap();
        let mut notices = Vec::new();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut |notice| notices.push(notice.to_owned()),
            &restrained(1, Some("23:00-07:00"), 1_700_000_000, 23 * 60),
            &occupant_tell("tell-retry", "already sent", Some("bot-foobar"), None),
        )
        .expect("retry");
        assert_eq!(action, InboxAction::Ack);
        assert_eq!(
            publisher.sends.lock().expect("sends").as_slice(),
            ["e".repeat(64).as_str()]
        );
        assert!(notices.iter().all(|notice| !notice.contains("quiet hours")));
    }

    #[test]
    fn prepared_wake_retry_is_not_re_gated_by_quiet_hours() {
        let (mut repository, publisher) = open_repo();
        repository.execute_batch_for_test(
            "UPDATE turns SET publish_reply_to_event_id = NULL,
                 ask_body = '## Watch event';",
        );
        repository
            .save_outbound_attempt(&OutboundAttempt {
                ask_id: "ask-1".to_owned(),
                body: "watched author posted".to_owned(),
                channel_id: CHANNEL.to_owned(),
                reply_to_event_id: None,
                thread_root_event_id: None,
                mention: String::new(),
                outbound_event_id: None,
                prepared_event_id: Some("e".repeat(64)),
                prepared_created_at: Some(1_700_000_000),
                dispatched: true,
            })
            .unwrap();
        let mut notices = Vec::new();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut |notice| notices.push(notice.to_owned()),
            &restrained(1, Some("23:00-07:00"), 1_700_000_000, 23 * 60),
            &delivery("final", "ask-1", "watched author posted"),
        )
        .expect("retry");
        assert_eq!(action, InboxAction::Ack);
        assert_eq!(
            publisher.sends.lock().expect("sends").as_slice(),
            ["e".repeat(64).as_str()]
        );
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Posted
        );
        assert!(notices.iter().all(|notice| !notice.contains("quiet hours")));
    }

    #[test]
    fn suppressed_wake_final_fails_the_turn_without_publishing() {
        let (mut repository, publisher) = open_repo();
        repository.execute_batch_for_test(
            "UPDATE turns SET publish_reply_to_event_id = NULL,
                 ask_body = '## Watch event';",
        );
        let restraint = restrained(1, None, 1_700_000_000, 12 * 60);
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &restraint,
            &occupant_tell("tell-cap", "unprompted", Some("bot-foobar"), None),
        )
        .expect("tell");
        let mut notices = Vec::new();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut |notice| notices.push(notice.to_owned()),
            &restraint,
            &delivery("final", "ask-1", "watched author posted"),
        )
        .expect("wake");
        assert_eq!(action, InboxAction::Ack);
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["[bot]: unprompted".to_owned()]
        );
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Failed
        );
        assert!(notices
            .iter()
            .any(|notice| notice.contains("post ceiling 1") && notice.contains("dropped")));
    }

    #[test]
    fn classify_delivery_matches_handle_outcomes() {
        let (repository, _publisher) = open_repo();
        let open = repository.turn_by_ask_id("ask-1").unwrap();
        assert_eq!(
            classify_delivery(&delivery("progress", "ask-1", "x"), open.as_ref()),
            InboxAction::Ack
        );
        assert_eq!(
            classify_delivery(&delivery("final", "ask-1", " "), open.as_ref()),
            InboxAction::Hold
        );
        assert_eq!(
            classify_delivery(&delivery("final", "missing", "x"), None),
            InboxAction::Hold
        );
    }

    #[test]
    fn occupant_tell_timeout_is_retried_on_the_tick_with_the_same_id() {
        let (mut repository, publisher) = open_repo();
        *publisher.fail.lock().expect("fail") = Some(PublishError::NotAccepted {
            detail: "timeout".to_owned(),
        });
        let err = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &occupant_tell("tell-retry", "queue is clear", Some("bot-foobar"), None),
        )
        .expect_err("retryable");
        assert!(err.to_string().contains("timeout"));
        let first = repository
            .outbound_attempt("tell-retry")
            .unwrap()
            .expect("row");
        let prepared = first.prepared_event_id.clone().expect("prepared");
        assert!(!first.dispatched);
        assert!(first.outbound_event_id.is_none());

        let action = retry_undispatched(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &NoopInFlightReaction,
            &first,
            &BotId::new("bot").expect("bot"),
            1_700_000_010,
        )
        .expect("drain");
        assert_eq!(action, InboxAction::Ack);
        let retried = repository
            .outbound_attempt("tell-retry")
            .unwrap()
            .expect("row");
        assert_eq!(
            retried.prepared_event_id.as_deref(),
            Some(prepared.as_str())
        );
        assert_eq!(
            retried.outbound_event_id.as_deref(),
            Some(prepared.as_str())
        );
        assert_eq!(retried.retry_count, 1);
        let sends = publisher.sends.lock().expect("sends");
        assert_eq!(sends.len(), 2);
        assert!(sends.iter().all(|event_id| event_id == &prepared));
    }

    #[test]
    fn occupant_tell_rejected_is_not_drained() {
        let (mut repository, _publisher) = open_repo();
        let mut notices = Vec::new();
        let action = handle_delivery(
            &mut repository,
            &NonRetryPublisher,
            &mut |notice| notices.push(notice.to_owned()),
            &occupant_tell("tell-reject", "queue is clear", Some("bot-foobar"), None),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        let bot_id = BotId::new("bot").expect("bot");
        let stored = repository
            .outbound_attempt("tell-reject")
            .unwrap()
            .expect("row");
        assert!(stored.abandoned_at.is_some());
        assert!(stored.outbound_event_id.is_none());
        assert!(repository
            .pending_outbound_attempts(&bot_id)
            .unwrap()
            .is_empty());
        retry_undispatched(
            &mut repository,
            &FakePublisher::default(),
            &mut |notice| notices.push(notice.to_owned()),
            &NoopInFlightReaction,
            &stored,
            &bot_id,
            stored.abandoned_at.unwrap_or(1) + OUTBOUND_RETRY_INTERVAL_SECS,
        )
        .expect("drain");
        assert!(
            repository
                .outbound_attempt("tell-reject")
                .unwrap()
                .expect("row")
                .outbound_event_id
                .is_none(),
            "a rejected tell must not be sent again"
        );
        assert!(notices.iter().any(|notice| notice.contains("not retrying")));
    }

    #[test]
    fn ask_final_abandon_after_the_retry_bound_fails_the_turn() {
        let (mut repository, publisher) = open_repo();
        let attempt = OutboundAttempt {
            reply_to_event_id: Some(event_id('a')),
            mention: "c".repeat(64),
            prepared_event_id: Some("e".repeat(64)),
            prepared_created_at: Some(1_700_000_000),
            bot_id: Some(BotId::new("bot").expect("bot")),
            retry_count: OUTBOUND_RETRY_CAP,
            last_retry_at: Some(1),
            ..OutboundAttempt::new("ask-1", "[bot]: hello", crate::test_support::CHANNEL)
        };
        repository.save_outbound_attempt(&attempt).unwrap();
        let mut notices = Vec::new();
        retry_undispatched(
            &mut repository,
            &publisher,
            &mut |notice| notices.push(notice.to_owned()),
            &PublishRestraint::unrestrained(),
            &NoopInFlightReaction,
            &attempt,
            &BotId::new("bot").expect("bot"),
            1_700_000_000,
        )
        .expect("abandon");
        assert!(notices.iter().any(|notice| notice.contains("abandoned")));
        assert_eq!(
            repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
            TurnState::Failed
        );
        assert!(publisher.sends.lock().expect("sends").is_empty());
        let stored = repository.outbound_attempt("ask-1").unwrap().unwrap();
        assert!(stored.abandoned_at.is_some());
    }

    #[test]
    fn escaped_routing_marker_in_a_tell_publishes() {
        let (mut repository, publisher) = open_repo();
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &occupant_tell(
                "tell-escape",
                "route with the \\<botserver to=\"eng\"> tag",
                Some("bot-foobar"),
                None,
            ),
        )
        .expect("handle");
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["[bot]: route with the <botserver to=\"eng\"> tag".to_owned()]
        );
    }

    #[test]
    fn unpublishable_tell_feeds_back_to_the_occupant() {
        let (mut repository, publisher) = open_repo();
        let mut feedback = Vec::new();
        let action = handle_delivery_with_feedback(
            &mut repository,
            &publisher,
            &mut notices(),
            &PublishRestraint::unrestrained(),
            &occupant_tell(
                "tell-bad",
                "<botserver to=\"eng\">a</botserver><botserver>b</botserver>",
                Some("bot-foobar"),
                None,
            ),
            &NoopInFlightReaction,
            &mut |name, body| feedback.push((name.to_owned(), body.to_owned())),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(publisher.calls.lock().expect("calls").is_empty());
        assert_eq!(feedback.len(), 1);
        assert_eq!(feedback[0].0, "bot-foobar");
        assert!(feedback[0].1.contains("tell-bad"));
        assert!(feedback[0].1.contains("\\<botserver"));
    }

    #[test]
    fn abandoned_tell_acks_without_sending_on_reconnect() {
        let (mut repository, publisher) = open_repo();
        repository
            .save_outbound_attempt(&OutboundAttempt {
                bot_id: Some(BotId::new("bot").expect("bot")),
                abandoned_at: Some(1),
                ..OutboundAttempt::new("tell-dead", "queue is clear", crate::test_support::CHANNEL)
            })
            .unwrap();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &occupant_tell("tell-dead", "queue is clear", Some("bot-foobar"), None),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(publisher.calls.lock().expect("calls").is_empty());
    }
}
