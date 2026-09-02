//! Host publish path: classify inbox replies, persist an outbound attempt, ACK last.

use std::fmt;
use std::io::{self, Write};
use std::process::{Command, Stdio};
use std::sync::Arc;

use botserver_domain::{stamp_outbound, EventId, TurnState};

use crate::inbox::InboxDelivery;
use crate::{HostRepository, IndexedRelayEvent, TurnRecord};

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
    pub reply_to_event_id: EventId,
    pub mention: String,
    pub outbound_event_id: Option<String>,
    pub dispatched: bool,
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

/// Failure while publishing a stamped reply.
#[derive(Debug)]
pub enum PublishError {
    /// The Buzz process could not be started or completed.
    Io(io::Error),
    /// Buzz rejected the send.
    CommandFailed { status: String, stderr: String },
    /// Buzz returned a receipt the host cannot retry safely.
    InvalidReceipt(String),
    /// Buzz exited 0 so the relay accepted; the id could not be recorded.
    AcceptedUnrecorded(String),
}

impl fmt::Display for PublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "failed to invoke buzz: {error}"),
            Self::CommandFailed { status, stderr } => {
                write!(formatter, "buzz exited with {status}: {stderr}")
            }
            Self::InvalidReceipt(reason) => write!(formatter, "invalid buzz receipt: {reason}"),
            Self::AcceptedUnrecorded(reason) => {
                write!(formatter, "relay accepted; not retrying: {reason}")
            }
        }
    }
}

impl PublishError {
    /// Command and I/O failures may retry. An accepted send must not.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Io(_) | Self::CommandFailed { .. } | Self::InvalidReceipt(_)
        )
    }
}

impl std::error::Error for PublishError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::CommandFailed { .. } | Self::InvalidReceipt(_) | Self::AcceptedUnrecorded(_) => {
                None
            }
        }
    }
}

impl From<io::Error> for PublishError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

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

/// Buzz CLI arguments for an in-flight reaction add or remove.
#[must_use]
pub fn in_flight_reaction_args<'a>(action: &'a str, trigger_event_id: &'a EventId) -> [&'a str; 6] {
    [
        "reactions",
        action,
        "--event",
        trigger_event_id.as_str(),
        "--emoji",
        IN_FLIGHT_REACTION,
    ]
}

fn run_buzz_reaction(action: &str, trigger_event_id: &EventId) {
    match Command::new("buzz")
        .args(in_flight_reaction_args(action, trigger_event_id))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
    {
        Ok(output) if output.status.success() => {}
        Ok(output) => eprintln!(
            "operator notice: in-flight reaction {action} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
        Err(error) => {
            eprintln!("operator notice: in-flight reaction {action} failed: {error}");
        }
    }
}

/// Publish one outbound attempt, returning the accepted event id.
pub trait OutboundPublisher {
    /// Publisher failure type.
    type Error: fmt::Display;

    /// Send the stamped body. Retry must return the same event id.
    ///
    /// # Errors
    ///
    /// Returns an error when the relay does not accept the event.
    fn publish(&self, attempt: &OutboundAttempt) -> Result<String, Self::Error>;

    /// Return whether this error may call send again.
    fn retryable(error: &Self::Error) -> bool {
        let _ = error;
        true
    }
}

/// Host wrapper around `buzz messages send` that records the accepted id.
#[derive(Debug, Default, Clone, Copy)]
pub struct BuzzPublisher;

impl OutboundPublisher for BuzzPublisher {
    type Error = PublishError;

    fn publish(&self, attempt: &OutboundAttempt) -> Result<String, Self::Error> {
        if let Some(event_id) = &attempt.outbound_event_id {
            return Ok(event_id.clone());
        }
        let mut arguments = vec![
            "messages".to_owned(),
            "send".to_owned(),
            "--channel".to_owned(),
            attempt.channel_id.clone(),
            "--content".to_owned(),
            "-".to_owned(),
            "--reply-to".to_owned(),
            attempt.reply_to_event_id.as_str().to_owned(),
        ];
        if !attempt.mention.is_empty() {
            arguments.extend(["--mention".to_owned(), attempt.mention.clone()]);
        }
        let mut child = Command::new("buzz")
            .args(&arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let write_result = child
            .stdin
            .take()
            .expect("piped stdin is available")
            .write_all(stamp_outbound(&attempt.body).as_bytes());
        let output = child.wait_with_output().map_err(|error| {
            PublishError::AcceptedUnrecorded(format!("buzz wait failed after start: {error}"))
        })?;
        if !output.status.success() {
            let _ = write_result;
            return Err(classify_buzz_failure(
                output.status.to_string(),
                String::from_utf8_lossy(&output.stderr).trim(),
            ));
        }
        let _ = write_result;
        let receipt: serde_json::Value =
            serde_json::from_slice(&output.stdout).map_err(|error| {
                PublishError::AcceptedUnrecorded(format!("unreadable receipt: {error}"))
            })?;
        if receipt.get("accepted").and_then(serde_json::Value::as_bool) != Some(true) {
            return Err(PublishError::InvalidReceipt(
                "relay did not accept the event".to_owned(),
            ));
        }
        receipt
            .get("event_id")
            .and_then(serde_json::Value::as_str)
            .and_then(EventId::parse_hex)
            .map(|event_id| event_id.as_str().to_owned())
            .ok_or_else(|| PublishError::AcceptedUnrecorded("missing event_id".to_owned()))
    }

    fn retryable(error: &Self::Error) -> bool {
        error.is_retryable()
    }
}

impl InFlightReaction for BuzzPublisher {
    fn add(&self, trigger_event_id: &EventId) {
        run_buzz_reaction("add", trigger_event_id);
    }

    fn remove(&self, trigger_event_id: &EventId) {
        run_buzz_reaction("remove", trigger_event_id);
    }
}

fn classify_buzz_failure(status: String, stderr: &str) -> PublishError {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(stderr) {
        if value.get("retryable").and_then(serde_json::Value::as_bool) == Some(true) {
            return PublishError::CommandFailed {
                status,
                stderr: stderr.to_owned(),
            };
        }
        let reason = value
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("buzz reported a non-retryable failure");
        return PublishError::AcceptedUnrecorded(format!("{reason}: {stderr}"));
    }
    PublishError::AcceptedUnrecorded(format!("buzz exited {status} without a retryable receipt"))
}

/// Classify one inbox delivery against a persisted turn.
#[must_use]
pub fn classify_delivery(delivery: &InboxDelivery, turn: Option<&TurnRecord>) -> InboxAction {
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
    let mention = repository
        .indexed_event(&turn.event_id)
        .map_err(OutboxError::Repository)?
        .map_or_else(String::new, |event: IndexedRelayEvent| event.author_pubkey);
    let mut attempt = repository
        .outbound_attempt(ask_id)
        .map_err(OutboxError::Repository)?
        .unwrap_or_else(|| OutboundAttempt {
            ask_id: ask_id.to_owned(),
            body: body.unwrap_or("").to_owned(),
            channel_id: turn.channel_id.clone(),
            reply_to_event_id: turn.event_id.clone(),
            mention,
            outbound_event_id: None,
            dispatched: false,
        });
    if attempt.outbound_event_id.is_none() && !attempt.dispatched {
        if let Some(body) = body {
            body.clone_into(&mut attempt.body);
        }
        attempt.channel_id.clone_from(&turn.channel_id);
        attempt.reply_to_event_id.clone_from(&turn.event_id);
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
    if attempt.dispatched {
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
    attempt.dispatched = true;
    repository
        .save_outbound_attempt(&attempt)
        .map_err(OutboxError::Repository)?;
    match publisher.publish(&attempt) {
        Ok(event_id) => {
            if !repository
                .mark_outbound_accepted(ask_id, &event_id)
                .map_err(OutboxError::Repository)?
            {
                notice(&format!(
                    "outbound event id for ask {ask_id} did not replace a prior id"
                ));
            }
        }
        Err(error) if !P::retryable(&error) => {
            notice(&format!("not retrying outbound for ask {ask_id}: {error}"));
            let _ = repository
                .set_turn_state(ask_id, TurnState::Failed)
                .map_err(OutboxError::Repository)?;
            reactions.remove(&turn.event_id);
            return Ok(InboxAction::Ack);
        }
        Err(error) => {
            attempt.dispatched = false;
            repository
                .save_outbound_attempt(&attempt)
                .map_err(OutboxError::Repository)?;
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
        event_id: String,
        fail: Mutex<bool>,
    }

    impl OutboundPublisher for FakePublisher {
        type Error = PublishError;

        fn publish(&self, attempt: &OutboundAttempt) -> Result<String, Self::Error> {
            if let Some(event_id) = &attempt.outbound_event_id {
                return Ok(event_id.clone());
            }
            self.calls.lock().expect("calls").push(attempt.body.clone());
            if *self.fail.lock().expect("fail") {
                return Err(PublishError::InvalidReceipt("rejected".to_owned()));
            }
            Ok(self.event_id.clone())
        }

        fn retryable(error: &Self::Error) -> bool {
            error.is_retryable()
        }
    }

    struct NonRetryPublisher;

    impl OutboundPublisher for NonRetryPublisher {
        type Error = PublishError;

        fn publish(&self, _attempt: &OutboundAttempt) -> Result<String, Self::Error> {
            Err(PublishError::AcceptedUnrecorded(
                "delivery_unknown: timeout".to_owned(),
            ))
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
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new("bot").expect("bot");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        repository
            .save_session(&SessionRecord {
                bot_id: bot_id.clone(),
                channel_id: channel_id.to_owned(),
                session_name: "bot-foobar".to_owned(),
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
                    content: "bot: hello".to_owned(),
                    tags_json: "[]".to_owned(),
                    channel_id: Some(channel_id.to_owned()),
                    target_event_id: None,
                },
                false,
            )
            .unwrap();
        let publisher = FakePublisher {
            calls: Mutex::new(Vec::new()),
            event_id: "d".repeat(64),
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
            &["hello".to_owned()]
        );
        let turn = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        assert_eq!(turn.state, TurnState::Posted);
        let attempt = repository.outbound_attempt("ask-1").unwrap().unwrap();
        assert_eq!(
            attempt.outbound_event_id.as_deref(),
            Some("d".repeat(64).as_str())
        );
        assert_eq!(attempt.reply_to_event_id, event_id('a'));
        assert_eq!(attempt.mention, "c".repeat(64));
    }

    #[test]
    fn crash_after_accept_retries_the_same_event() {
        let (mut repository, publisher) = open_repo();
        repository
            .save_outbound_attempt(&OutboundAttempt {
                ask_id: "ask-1".to_owned(),
                body: "hello".to_owned(),
                channel_id: "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
                reply_to_event_id: event_id('a'),
                mention: "c".repeat(64),
                outbound_event_id: Some("d".repeat(64)),
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
    fn crash_after_dispatch_without_id_does_not_publish_again() {
        let (mut repository, publisher) = open_repo();
        repository
            .save_outbound_attempt(&OutboundAttempt {
                ask_id: "ask-1".to_owned(),
                body: "hello".to_owned(),
                channel_id: "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
                reply_to_event_id: event_id('a'),
                mention: "c".repeat(64),
                outbound_event_id: None,
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
    fn buzz_stderr_retryable_false_is_not_retryable() {
        let error = classify_buzz_failure(
            "exit status: 2".to_owned(),
            r#"{"error":"delivery_unknown","message":"timeout","retryable":false}"#,
        );
        assert!(!error.is_retryable());
        let retry = classify_buzz_failure(
            "exit status: 2".to_owned(),
            r#"{"error":"network_error","message":"connect","retryable":true}"#,
        );
        assert!(retry.is_retryable());
        let opaque = classify_buzz_failure("exit status: 2".to_owned(), "not json");
        assert!(!opaque.is_retryable());
    }

    #[test]
    fn in_flight_reaction_args_are_kind_seven_add_and_kind_five_remove() {
        let event = event_id('a');
        assert_eq!(
            in_flight_reaction_args("add", &event),
            [
                "reactions",
                "add",
                "--event",
                event.as_str(),
                "--emoji",
                "⏳"
            ]
        );
        assert_eq!(
            in_flight_reaction_args("remove", &event),
            [
                "reactions",
                "remove",
                "--event",
                event.as_str(),
                "--emoji",
                "⏳"
            ]
        );
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
