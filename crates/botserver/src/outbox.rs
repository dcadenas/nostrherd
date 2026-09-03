//! Host publish path: classify inbox replies, persist an outbound attempt, ACK last.
//!
//! The host publishes through the nostr-sdk client it already holds
//! (D43): `BuzzPublisher` signs in-process, so the event id exists
//! before send (D28, amended) and a retry redelivers the same event,
//! which the relay dedups.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use botserver_domain::buzz::{self, BuzzEvent};
use botserver_domain::{
    outbound_prefix_for, parse_occupant_tell, stamp_outbound, BotId, EventId, OccupantTell,
    TurnState,
};
use nostr_sdk::prelude::{
    Client, Event, EventBuilder, Filter, FinalizeEvent, Keys, Kind, SingleLetterTag, Tag, Timestamp,
};

use crate::inbox::InboxDelivery;
use crate::{HostRepository, IndexedRelayEvent, SessionRecord, TurnRecord};

/// Whether the claimed inbox delivery may be acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxAction {
    /// The host decided; `inbox.ack` is allowed.
    Ack,
    /// Leave the delivery queued so a later valid final can still arrive.
    Hold,
}

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
}

impl OutboundAttempt {
    fn payload_is_mutable(&self) -> bool {
        self.outbound_event_id.is_none() && self.prepared_event_id.is_none() && !self.dispatched
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
            .unwrap_or_else(current_timestamp);
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

fn current_timestamp() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or_default(),
    )
    .unwrap_or_default()
}

/// Classify one inbox delivery against a persisted turn.
#[must_use]
pub fn classify_delivery(delivery: &InboxDelivery, turn: Option<&TurnRecord>) -> InboxAction {
    if delivery.kind() == "tell" {
        return InboxAction::Ack;
    }
    match decide(delivery, turn) {
        Decision::Hold => InboxAction::Hold,
        Decision::AckWithoutPublish | Decision::Publish { .. } => InboxAction::Ack,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Decision {
    AckWithoutPublish,
    Hold,
    Publish { body: String },
}

fn decide(delivery: &InboxDelivery, turn: Option<&TurnRecord>) -> Decision {
    let Some(turn) = turn else {
        return Decision::Hold;
    };
    if delivery.kind() == "reply" && delivery.disposition() == Some("progress") {
        return Decision::AckWithoutPublish;
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

fn handle_occupant_tell<R, P>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
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
        return Ok(InboxAction::Ack);
    };
    publish_initiated(
        repository,
        publisher,
        notice,
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

fn publish_initiated<R, P>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
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
            ask_id: message_id.to_owned(),
            body: body.to_owned(),
            channel_id: destination.channel_id.clone(),
            reply_to_event_id: None,
            thread_root_event_id: None,
            mention: String::new(),
            outbound_event_id: None,
            prepared_event_id: None,
            prepared_created_at: None,
            dispatched: false,
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
    if attempt.outbound_event_id.is_some() {
        return Ok(InboxAction::Ack);
    }
    if attempt.dispatched && attempt.prepared_event_id.is_none() {
        // Attempt recorded before prepared ids existed: a send may have
        // landed under the old path. Never send again.
        notice(&format!(
            "not retrying outbound for tell {message_id}; send already invoked"
        ));
        return Ok(InboxAction::Ack);
    }
    let mut to_publish = attempt.clone();
    to_publish.body = stamp_outbound(&attempt.body, &outbound_prefix_for(bot_id));
    match record_and_send(repository, publisher, &mut to_publish)? {
        SendOutcome::Accepted(event_id) => {
            let _ = repository
                .mark_outbound_accepted(message_id, &event_id)
                .map_err(OutboxError::Repository)?;
            Ok(InboxAction::Ack)
        }
        SendOutcome::Rejected(error) => {
            notice(&format!(
                "not retrying outbound for tell {message_id}: {error}"
            ));
            Ok(InboxAction::Ack)
        }
        SendOutcome::Retry(error) => Err(OutboxError::Publish(error)),
    }
}

/// Prepare, record the prepared id, and send one stamped attempt.
/// Outcome of [`record_and_send`]: prepare, record, and send one
/// stamped attempt.
///
/// A build failure is terminal for the attempt; a retryable transport
/// failure keeps the recorded prepared id so the retry redelivers the
/// same event and the relay dedups (D43).
enum SendOutcome<E> {
    /// A relay accepted the event.
    Accepted(String),
    /// The event was not sent and must not be sent again.
    Rejected(E),
    /// The event was not accepted; redelivering is safe.
    Retry(E),
}

type SendOutcomeResult<R, P> = Result<
    SendOutcome<<P as OutboundPublisher>::Error>,
    OutboxError<<R as HostRepository>::Error, <P as OutboundPublisher>::Error>,
>;

fn record_and_send<R, P>(
    repository: &mut R,
    publisher: &P,
    to_publish: &mut OutboundAttempt,
) -> SendOutcomeResult<R, P>
where
    R: HostRepository,
    P: OutboundPublisher,
{
    let prepared = match publisher.prepare(to_publish) {
        Ok(prepared) => prepared,
        Err(error) => return Ok(SendOutcome::Rejected(error)),
    };
    if to_publish.prepared_event_id.is_none() {
        to_publish.prepared_event_id = Some(prepared.event_id().to_owned());
        to_publish.prepared_created_at = Some(prepared.created_at());
    }
    to_publish.dispatched = true;
    repository
        .save_outbound_attempt(to_publish)
        .map_err(OutboxError::Repository)?;
    match publisher.publish(&prepared) {
        Ok(event_id) => Ok(SendOutcome::Accepted(event_id)),
        Err(error) if !P::retryable(&error) => Ok(SendOutcome::Rejected(error)),
        Err(error) => {
            to_publish.dispatched = false;
            repository
                .save_outbound_attempt(to_publish)
                .map_err(OutboxError::Repository)?;
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
    delivery: &InboxDelivery,
    reactions: &I,
) -> Result<InboxAction, OutboxError<R::Error, P::Error>>
where
    R: HostRepository,
    P: OutboundPublisher,
    I: InFlightReaction,
{
    if delivery.kind() == "tell" {
        return handle_occupant_tell(repository, publisher, notice, delivery);
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
        Decision::Publish { body } => match turn {
            Some(turn) => {
                complete_outbound_with(repository, publisher, notice, &turn, Some(&body), reactions)
            }
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
pub fn complete_outbound_with<R, P, I>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
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
    let indexed = repository
        .indexed_event(&turn.event_id)
        .map_err(OutboxError::Repository)?;
    let mention = indexed
        .as_ref()
        .map_or_else(String::new, |event: &IndexedRelayEvent| {
            event.author_pubkey.clone()
        });
    let thread_root_event_id = indexed.as_ref().and_then(|event: &IndexedRelayEvent| {
        let tags = serde_json::from_str::<Vec<Vec<String>>>(&event.tags_json).unwrap_or_default();
        buzz::reply_thread_root(&turn.event_id, &tags)
    });
    let mut attempt = repository
        .outbound_attempt(ask_id)
        .map_err(OutboxError::Repository)?
        .unwrap_or_else(|| OutboundAttempt {
            ask_id: ask_id.to_owned(),
            body: body.unwrap_or("").to_owned(),
            channel_id: turn.channel_id.clone(),
            reply_to_event_id: Some(turn.event_id.clone()),
            thread_root_event_id: thread_root_event_id.clone(),
            mention,
            outbound_event_id: None,
            prepared_event_id: None,
            prepared_created_at: None,
            dispatched: false,
        });
    if attempt.payload_is_mutable() {
        if let Some(body) = body {
            body.clone_into(&mut attempt.body);
        }
        attempt.channel_id.clone_from(&turn.channel_id);
        attempt.reply_to_event_id = Some(turn.event_id.clone());
        attempt.thread_root_event_id = thread_root_event_id;
    }
    repository
        .save_outbound_attempt(&attempt)
        .map_err(OutboxError::Repository)?;
    if attempt.outbound_event_id.is_some() {
        let _ = repository
            .set_turn_state(ask_id, TurnState::Posted)
            .map_err(OutboxError::Repository)?;
        reactions.remove(&turn.event_id);
        return Ok(InboxAction::Ack);
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
        reactions.remove(&turn.event_id);
        return Ok(InboxAction::Ack);
    }
    let claimed = repository
        .claim_turn_for_publish(ask_id)
        .map_err(OutboxError::Repository)?;
    let still_open = repository
        .turn_by_ask_id(ask_id)
        .map_err(OutboxError::Repository)?
        .is_some_and(|turn| turn.state == TurnState::Open);
    if !claimed && !still_open {
        reactions.remove(&turn.event_id);
        return Ok(InboxAction::Ack);
    }
    let mut to_publish = attempt.clone();
    to_publish.body = stamp_outbound(&attempt.body, &outbound_prefix_for(&turn.bot_id));
    match record_and_send(repository, publisher, &mut to_publish)? {
        SendOutcome::Accepted(event_id) => {
            if !repository
                .mark_outbound_accepted(ask_id, &event_id)
                .map_err(OutboxError::Repository)?
            {
                notice(&format!(
                    "outbound event id for ask {ask_id} did not replace a prior id"
                ));
            }
        }
        SendOutcome::Rejected(error) => {
            notice(&format!("not retrying outbound for ask {ask_id}: {error}"));
            let _ = repository
                .set_turn_state(ask_id, TurnState::Failed)
                .map_err(OutboxError::Repository)?;
            reactions.remove(&turn.event_id);
            return Ok(InboxAction::Ack);
        }
        SendOutcome::Retry(error) => {
            let _ = repository.release_publish_claim(ask_id);
            return Err(OutboxError::Publish(error));
        }
    }
    let _ = repository
        .set_turn_state(ask_id, TurnState::Posted)
        .map_err(OutboxError::Repository)?;
    reactions.remove(&turn.event_id);
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
    use std::sync::Mutex;

    use rusqlite::Connection;

    use super::*;
    use crate::sqlite::SqliteRepository;
    use crate::{NewTurn, SessionRecord};
    use botserver_domain::BotId;

    #[derive(Debug)]
    struct FakePublisher {
        calls: Mutex<Vec<String>>,
        reply_to: Mutex<Vec<Option<String>>>,
        sends: Mutex<Vec<String>>,
        fail: Mutex<bool>,
    }

    /// Deterministic stand-in id: the same attempt content and timestamp
    /// prepare to the same id, which is what a redelivery relies on.
    fn fake_event_id(attempt: &OutboundAttempt, created_at: i64) -> String {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        attempt.body.hash(&mut hasher);
        attempt.channel_id.hash(&mut hasher);
        attempt.reply_to_event_id.hash(&mut hasher);
        attempt.thread_root_event_id.hash(&mut hasher);
        attempt.mention.hash(&mut hasher);
        created_at.hash(&mut hasher);
        format!("{:064}", hasher.finish())
    }

    impl OutboundPublisher for FakePublisher {
        type Error = PublishError;

        fn prepare(&self, attempt: &OutboundAttempt) -> Result<PreparedOutbound, Self::Error> {
            let created_at = attempt.prepared_created_at.unwrap_or(1_700_000_000);
            let event_id = attempt
                .prepared_event_id
                .clone()
                .unwrap_or_else(|| fake_event_id(attempt, created_at));
            self.calls.lock().expect("calls").push(attempt.body.clone());
            self.reply_to.lock().expect("reply_to").push(
                attempt
                    .reply_to_event_id
                    .as_ref()
                    .map(|event_id| event_id.as_str().to_owned()),
            );
            let signed =
                nostr_sdk::prelude::EventBuilder::new(nostr_sdk::prelude::Kind::Custom(9), "")
                    .finalize(&nostr_sdk::prelude::Keys::generate())
                    .expect("dummy event");
            Ok(PreparedOutbound::from_parts(signed, event_id, created_at))
        }

        fn publish(&self, prepared: &PreparedOutbound) -> Result<String, Self::Error> {
            self.sends
                .lock()
                .expect("sends")
                .push(prepared.event_id().to_owned());
            if *self.fail.lock().expect("fail") {
                return Err(PublishError::NotAccepted {
                    detail: "connection dropped before OK".to_owned(),
                });
            }
            Ok(prepared.event_id().to_owned())
        }

        fn retryable(error: &Self::Error) -> bool {
            error.is_retryable()
        }
    }

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

    fn event_id(character: char) -> EventId {
        EventId::parse_hex(&character.to_string().repeat(64)).expect("event")
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

    fn open_repo() -> (SqliteRepository, FakePublisher) {
        open_repo_for("bot")
    }

    fn open_repo_for(bot: &str) -> (SqliteRepository, FakePublisher) {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new(bot).expect("bot");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        repository
            .save_session(&SessionRecord {
                bot_id: bot_id.clone(),
                channel_id: channel_id.to_owned(),
                session_name: format!("{bot}-foobar"),
                occupant_logical_id: Some("occupant-agent".to_owned()),
                renew_id: None,
                ask_context_event_id: None,
                ask_context_created_at: None,
            })
            .unwrap();
        repository
            .enqueue_unprocessed_turn(&NewTurn {
                bot_id: bot_id.clone(),
                channel_id: channel_id.to_owned(),
                event_id: event_id('a'),
                reply_to_event_id: None,
            })
            .unwrap();
        repository
            .open_next_turn(&bot_id, channel_id, "ask-1")
            .unwrap();
        repository
            .index_event(
                &IndexedRelayEvent {
                    event_id: event_id('a'),
                    author_pubkey: "c".repeat(64),
                    created_at: 1,
                    kind: 9,
                    content: format!("{bot}: hello"),
                    tags_json: "[]".to_owned(),
                    channel_id: Some(channel_id.to_owned()),
                    target_event_id: None,
                },
                false,
            )
            .unwrap();
        let publisher = FakePublisher {
            calls: Mutex::new(Vec::new()),
            reply_to: Mutex::new(Vec::new()),
            sends: Mutex::new(Vec::new()),
            fail: Mutex::new(false),
        };
        (repository, publisher)
    }

    fn notices() -> impl FnMut(&str) {
        |_| {}
    }

    #[test]
    fn progress_acks_without_publish() {
        let (mut repository, publisher) = open_repo();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &delivery("progress", "ask-1", "working"),
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
    fn empty_final_is_held_and_not_acked() {
        let (mut repository, publisher) = open_repo();
        let mut notices = Vec::new();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut |notice| notices.push(notice.to_owned()),
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

    #[tokio::test(flavor = "multi_thread")]
    async fn buzz_publisher_reprepares_recorded_attempt_with_same_id() {
        let keys = Keys::generate();
        let publisher =
            BuzzPublisher::new(Client::builder().build(), keys.clone(), "ws://127.0.0.1:1");
        let mut attempt = OutboundAttempt {
            ask_id: "ask-1".to_owned(),
            body: "[bot]: hello".to_owned(),
            channel_id: "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
            reply_to_event_id: Some(event_id('a')),
            thread_root_event_id: None,
            mention: keys.public_key().to_hex(),
            outbound_event_id: None,
            prepared_event_id: None,
            prepared_created_at: None,
            dispatched: false,
        };

        let first = publisher.prepare(&attempt).expect("first prepare");
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
                ask_id: "ask-1".to_owned(),
                body: "hello".to_owned(),
                channel_id: "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
                reply_to_event_id: Some(event_id('a')),
                thread_root_event_id: None,
                mention: "c".repeat(64),
                outbound_event_id: None,
                prepared_event_id: Some("e".repeat(64)),
                prepared_created_at: Some(1_700_000_000),
                dispatched: true,
            })
            .unwrap();
        repository.claim_turn_for_publish("ask-1").unwrap();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
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
                ask_id: "ask-1".to_owned(),
                body: "hello".to_owned(),
                channel_id: "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
                reply_to_event_id: Some(event_id('a')),
                thread_root_event_id: None,
                mention: "c".repeat(64),
                outbound_event_id: Some("d".repeat(64)),
                prepared_event_id: Some("d".repeat(64)),
                prepared_created_at: Some(1_700_000_000),
                dispatched: true,
            })
            .unwrap();
        repository.claim_turn_for_publish("ask-1").unwrap();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
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
                ask_id: "ask-1".to_owned(),
                body: "hello".to_owned(),
                channel_id: "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
                reply_to_event_id: Some(event_id('a')),
                thread_root_event_id: None,
                mention: "c".repeat(64),
                outbound_event_id: None,
                prepared_event_id: None,
                prepared_created_at: None,
                dispatched: true,
            })
            .unwrap();
        repository.claim_turn_for_publish("ask-1").unwrap();
        let mut notices = Vec::new();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut |notice| notices.push(notice.to_owned()),
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
        let action = handle_delivery(&mut repository, &publisher, &mut notices(), &delivery)
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
            &delivery("final", "ask-1", "hello"),
        )
        .unwrap();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
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
            &delivery("progress", "ask-1", "working"),
            &reactions,
        )
        .expect("handle");
        assert!(reactions.removes.lock().expect("removes").is_empty());
    }

    #[test]
    fn retryable_publish_failure_keeps_in_flight_reaction() {
        let (mut repository, publisher) = open_repo();
        *publisher.fail.lock().expect("fail") = true;
        let reactions = RecordingInFlightReaction::default();
        let error = handle_delivery_with(
            &mut repository,
            &publisher,
            &mut notices(),
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
}
