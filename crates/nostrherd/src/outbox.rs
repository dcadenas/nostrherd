//! Host publish path: classify inbox replies, persist an outbound attempt, ACK last.
//!
//! The host publishes through the nostr-sdk client it already holds
//! (D43): `BuzzPublisher` signs in-process, so the event id exists
//! before send (D28, amended) and a retry redelivers the same event,
//! which the relay dedups.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use nostr_sdk::prelude::{
    Client, Event, EventBuilder, Filter, FinalizeEvent, Keys, Kind, SingleLetterTag, Tag, Timestamp,
};
use nostrherd_domain::buzz::{self, BuzzEvent};
use nostrherd_domain::{
    outbound_prefix_for, parse_occupant_tell, stamp_outbound, BotId, EventId, TurnState,
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

/// Mechanical accident checks applied before occupant prose can be published.
#[derive(Debug, Clone, Default)]
pub struct OutputGuard {
    home: Option<PathBuf>,
    kelpie_socket: Option<PathBuf>,
}

impl OutputGuard {
    /// Configure the operator paths that must not appear in channel output.
    #[must_use]
    pub fn new(home: Option<PathBuf>, kelpie_socket: PathBuf) -> Self {
        Self {
            home,
            kelpie_socket: Some(kelpie_socket),
        }
    }

    fn refusal(&self, body: &str) -> Option<&'static str> {
        if contains_nsec(body) {
            return Some("possible nsec");
        }
        if self
            .kelpie_socket
            .as_deref()
            .is_some_and(|path| contains_path(body, path))
        {
            return Some("Kelpie socket path");
        }
        self.home
            .as_deref()
            .filter(|path| contains_path(body, path))
            .map(|_| "absolute home path")
    }
}

fn contains_path(body: &str, path: &Path) -> bool {
    path.parent()
        .is_some_and(|parent| !parent.as_os_str().is_empty())
        && body.contains(path.to_string_lossy().as_ref())
}

fn contains_nsec(body: &str) -> bool {
    body.to_ascii_lowercase()
        .as_bytes()
        .windows(63)
        .any(|window| window.starts_with(b"nsec1") && window.iter().all(u8::is_ascii_alphanumeric))
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
    /// Mechanical output screening refused occupant prose.
    OutputRefused { reason: &'static str },
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
            Self::OutputRefused { reason } => {
                write!(formatter, "outbound content refused: {reason}")
            }
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
    output_guard: OutputGuard,
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
            output_guard: OutputGuard::default(),
        }
    }

    /// Apply mechanical output screening to every channel event.
    #[must_use]
    pub fn with_output_guard(mut self, output_guard: OutputGuard) -> Self {
        self.output_guard = output_guard;
        self
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
        if let Some(reason) = self.output_guard.refusal(event.content()) {
            return Err(PublishError::OutputRefused { reason });
        }
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
        if let Some(reason) = self.output_guard.refusal(&attempt.body) {
            return Err(PublishError::OutputRefused { reason });
        }
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
    format!("your tell {message_id} did not publish: the body was empty.")
}

fn handle_occupant_tell<R, P>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    occupant_feedback: &mut impl FnMut(&str, &str),
    delivery: &InboxDelivery,
    output_guard: &OutputGuard,
    operator_feedback: &mut impl FnMut(&str),
) -> Result<InboxAction, OutboxError<R::Error, P::Error>>
where
    R: HostRepository,
    P: OutboundPublisher,
{
    let Some(session) = occupant_session(repository, delivery).map_err(OutboxError::Repository)?
    else {
        return Ok(InboxAction::Ack);
    };
    let Some(body) = parse_occupant_tell(delivery.body()) else {
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
    if let Some(reason) = output_guard.refusal(&body) {
        notice(&format!(
            "occupant tell {} was refused by mechanical output screening: {reason}",
            delivery.message_id()
        ));
        operator_feedback(&format!(
            "nostrherd refused occupant tell {} before channel publish: {reason}.",
            delivery.message_id()
        ));
        occupant_feedback(
            &session.session_name,
            &format!(
                "your tell {} did not publish: mechanical output screening found {reason}.",
                delivery.message_id()
            ),
        );
        return Ok(InboxAction::Ack);
    }
    let destination = session.clone();
    publish_initiated(
        repository,
        publisher,
        notice,
        delivery.message_id(),
        &body,
        &destination,
        &session.bot_id,
    )
}

fn occupant_session<R: HostRepository>(
    repository: &R,
    delivery: &InboxDelivery,
) -> Result<Option<SessionRecord>, R::Error> {
    // The name is the identity (D62), and Kelpie fills the envelope's sender
    // name from its own records, so it is the routing key. The host used to
    // cross-check it against a stored logical id; there is no stored id now,
    // and Kelpie admits only one holder of a name, so the name alone decides.
    delivery
        .sender_public_name()
        .map(|name| repository.session_by_name(name))
        .transpose()
        .map(Option::flatten)
}

#[allow(clippy::too_many_arguments)]
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
    handle_delivery_with_feedback(
        repository,
        publisher,
        notice,
        delivery,
        reactions,
        &mut |_, _| {},
        &OutputGuard::default(),
        &mut |_| {},
    )
}

/// Persist, publish, and notify the occupant when a tell cannot be posted.
///
/// # Errors
///
/// Returns an error when persistence or publish fails. A publish failure does
/// not ACK, so reconnect can retry the same outbound attempt.
#[allow(clippy::too_many_arguments)]
pub fn handle_delivery_with_feedback<R, P, I>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    delivery: &InboxDelivery,
    reactions: &I,
    occupant_feedback: &mut impl FnMut(&str, &str),
    output_guard: &OutputGuard,
    operator_feedback: &mut impl FnMut(&str),
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
            occupant_feedback,
            delivery,
            output_guard,
            operator_feedback,
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
            if let Some(reason) = output_guard.refusal(&body) {
                notice(&format!(
                    "progress for ask {ask_id} was refused by mechanical output screening: {reason}"
                ));
                operator_feedback(&format!(
                    "nostrherd refused progress for ask {ask_id} before channel publish: {reason}."
                ));
                if let Some(turn) = turn.as_ref() {
                    if let Some(session) = repository
                        .session(&turn.bot_id, &turn.channel_id)
                        .map_err(OutboxError::Repository)?
                    {
                        occupant_feedback(
                            &session.session_name,
                            &format!(
                                "your progress for ask {ask_id} did not publish: mechanical output screening found {reason}."
                            ),
                        );
                    }
                }
                return Ok(InboxAction::Ack);
            }
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
            Some(turn) => {
                if let Some(reason) = output_guard.refusal(&body) {
                    notice(&format!(
                        "final for ask {ask_id} was refused by mechanical output screening: {reason}"
                    ));
                    operator_feedback(&format!(
                        "nostrherd refused the final for ask {ask_id} before channel publish: {reason}."
                    ));
                    if let Some(session) = repository
                        .session(&turn.bot_id, &turn.channel_id)
                        .map_err(OutboxError::Repository)?
                    {
                        occupant_feedback(
                            &session.session_name,
                            &format!(
                                "your final for ask {ask_id} did not publish and the turn is closed: mechanical output screening found {reason}. Keep that content out of future channel output."
                            ),
                        );
                    }
                    progress::discard_pending(repository, ask_id)
                        .map_err(OutboxError::Repository)?;
                    let _ = repository
                        .set_turn_state(ask_id, TurnState::Failed)
                        .map_err(OutboxError::Repository)?;
                    if let Some(event_id) = turn.publish_reply_to_event_id.as_ref() {
                        reactions.remove(event_id);
                    }
                    return Ok(InboxAction::Ack);
                }
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
#[allow(clippy::too_many_lines)]
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
#[allow(clippy::too_many_arguments)]
pub fn retry_undispatched<R, P, I>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
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
        return complete_outbound_with(repository, publisher, notice, &turn, None, reactions);
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
    use crate::test_support::{event_id, fake_event_id, open_repo, open_repo_for, FakePublisher};
    use nostrherd_domain::BotId;

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
            &delivery("progress", "ask-1", "almost there"),
        )
        .expect("progress");
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &delivery("final", "ask-1", "done"),
        )
        .expect("final");
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["**[bot]**: done".to_owned()]
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
            &["**[bot]**: hello".to_owned()]
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
            attempt.body, "**[bot]**: hello",
            "the row records the stamped body the prepared id signed"
        );
    }

    #[test]
    fn mechanical_output_scrub_refuses_secrets_and_operator_paths_privately() {
        let cases = [
            format!("credential nsec1{}", "q".repeat(58)),
            "socket /run/user/1000/kelpie/kelpie.sock".to_owned(),
            "read /home/operator/private/notes".to_owned(),
        ];
        for body in cases {
            let (mut repository, publisher) = open_repo();
            let mut private = Vec::new();
            let mut occupant = Vec::new();
            let action = handle_delivery_with_feedback(
                &mut repository,
                &publisher,
                &mut notices(),
                &delivery("final", "ask-1", &body),
                &NoopInFlightReaction,
                &mut |_, message| occupant.push(message.to_owned()),
                &OutputGuard::new(
                    Some(PathBuf::from("/home/operator")),
                    PathBuf::from("/run/user/1000/kelpie/kelpie.sock"),
                ),
                &mut |message| private.push(message.to_owned()),
            )
            .expect("handle");
            assert_eq!(action, InboxAction::Ack);
            assert!(publisher.calls.lock().expect("calls").is_empty());
            assert_eq!(
                repository.turn_by_ask_id("ask-1").unwrap().unwrap().state,
                TurnState::Failed
            );
            assert_eq!(private.len(), 1);
            assert!(!private[0].contains(&body));
            assert_eq!(occupant.len(), 1);
            assert!(!occupant[0].contains(&body));
        }
    }

    #[test]
    fn filesystem_root_is_not_treated_as_an_operator_home() {
        let guard = OutputGuard::new(
            Some(PathBuf::from("/")),
            PathBuf::from("/run/user/1000/kelpie/kelpie.sock"),
        );
        assert_eq!(guard.refusal("see https://example.test/status"), None);
    }

    #[test]
    fn scrubbed_progress_and_tell_feed_back_without_publishing() {
        let blocked = "read /home/operator/private";
        let guard = OutputGuard::new(
            Some(PathBuf::from("/home/operator")),
            PathBuf::from("/run/user/1000/kelpie/kelpie.sock"),
        );

        let (mut repository, publisher) = open_repo();
        let mut progress_feedback = Vec::new();
        let progress = handle_delivery_with_feedback(
            &mut repository,
            &publisher,
            &mut notices(),
            &delivery("progress", "ask-1", blocked),
            &NoopInFlightReaction,
            &mut |_, message| progress_feedback.push(message.to_owned()),
            &guard,
            &mut |_| {},
        )
        .expect("progress");
        assert_eq!(progress, InboxAction::Ack);
        assert_eq!(progress_feedback.len(), 1);
        assert!(!progress_feedback[0].contains(blocked));
        assert!(repository.progress_post("ask-1").unwrap().is_none());
        assert!(publisher.calls.lock().expect("calls").is_empty());

        let (mut repository, publisher) = open_repo();
        let mut tell_feedback = Vec::new();
        let tell = handle_delivery_with_feedback(
            &mut repository,
            &publisher,
            &mut notices(),
            &occupant_tell("tell-guarded", blocked, Some("bot-foobar"), None),
            &NoopInFlightReaction,
            &mut |_, message| tell_feedback.push(message.to_owned()),
            &guard,
            &mut |_| {},
        )
        .expect("tell");
        assert_eq!(tell, InboxAction::Ack);
        assert_eq!(tell_feedback.len(), 1);
        assert!(!tell_feedback[0].contains(blocked));
        assert!(publisher.calls.lock().expect("calls").is_empty());
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
            &delivery("final", "ask-1", "watched author posted"),
        )
        .expect("handle");

        assert_eq!(action, InboxAction::Ack);
        let attempt = repository.outbound_attempt("ask-1").unwrap().unwrap();
        assert_eq!(attempt.body, "**[bot]**: watched author posted");
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
            ..OutboundAttempt::new("ask-1", "**[bot]**: hello", crate::test_support::CHANNEL)
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

    #[tokio::test(flavor = "multi_thread")]
    async fn concrete_publisher_refuses_a_persisted_operator_path() {
        let publisher = BuzzPublisher::new(
            Client::builder().build(),
            Keys::generate(),
            "ws://127.0.0.1:1",
        )
        .with_output_guard(OutputGuard::new(
            Some(PathBuf::from("/home/operator")),
            PathBuf::from("/run/user/1000/kelpie/kelpie.sock"),
        ));
        let attempt = OutboundAttempt::new("ask", "read /home/operator/private", "channel");
        assert!(matches!(
            publisher.prepare(&attempt),
            Err(PublishError::OutputRefused {
                reason: "absolute home path"
            })
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
            &["**[pr]**: hello".to_owned()]
        );
        let (mut repository, publisher) = open_repo_for("pr");
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &delivery("final", "ask-1", "  **[pr]**: already  "),
        )
        .expect("handle");
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["**[pr]**: already".to_owned()]
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
            &occupant_tell("tell-both", "both fields", Some("bot-foobar"), Some("1990")),
        )
        .expect("handle");
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["**[bot]**: both fields".to_owned()]
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
            &["**[bot]**: queue is clear".to_owned()]
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
    /// The sender's name decides, and a stale id alongside it changes nothing.
    ///
    /// The host recorded no identity to disagree with (D62), and Kelpie admits
    /// only one holder of a name, so the name alone is the routing key.
    fn a_stale_agent_id_does_not_stop_a_named_sender_posting() {
        let (mut repository, publisher) = open_repo();
        handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &occupant_tell("tell-6", "hi", Some("bot-foobar"), Some("1611")),
        )
        .expect("handle");
        assert_eq!(
            publisher.calls.lock().expect("calls").as_slice(),
            &["**[bot]**: hi".to_owned()]
        );
    }

    #[test]
    /// An unnamed sender has no session, so its tell is acked and dropped.
    ///
    /// An agent id used to be a second way in. Nothing maps one to a session
    /// now, and inventing a mapping would let any id post to any channel.
    fn an_unnamed_sender_posts_nowhere() {
        let (mut repository, publisher) = open_repo();
        let action = handle_delivery(
            &mut repository,
            &publisher,
            &mut notices(),
            &occupant_tell("tell-4", "from id", None, Some("1990")),
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(publisher.calls.lock().expect("calls").is_empty());
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
        *publisher.fail.lock().expect("fail") = Some(PublishError::NotAccepted {
            detail: "connection dropped before OK".to_owned(),
        });
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
            ..OutboundAttempt::new("ask-1", "**[bot]**: hello", crate::test_support::CHANNEL)
        };
        repository.save_outbound_attempt(&attempt).unwrap();
        let mut notices = Vec::new();
        retry_undispatched(
            &mut repository,
            &publisher,
            &mut |notice| notices.push(notice.to_owned()),
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
    fn unpublishable_tell_feeds_back_to_the_occupant() {
        let (mut repository, publisher) = open_repo();
        let mut feedback = Vec::new();
        let action = handle_delivery_with_feedback(
            &mut repository,
            &publisher,
            &mut notices(),
            &occupant_tell("tell-bad", "   ", Some("bot-foobar"), None),
            &NoopInFlightReaction,
            &mut |name, body| feedback.push((name.to_owned(), body.to_owned())),
            &OutputGuard::default(),
            &mut |_| {},
        )
        .expect("handle");
        assert_eq!(action, InboxAction::Ack);
        assert!(publisher.calls.lock().expect("calls").is_empty());
        assert_eq!(feedback.len(), 1);
        assert_eq!(feedback[0].0, "bot-foobar");
        assert!(feedback[0].1.contains("tell-bad"));
        assert!(feedback[0].1.contains("empty"));
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
