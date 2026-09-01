//! Read-only relay subscription and event classification.

use std::fmt;

use botserver_domain::{EventId, TriggerMatch};
use nostr_sdk::prelude::{Client, Event, Filter, Kind, RelayPoolNotification, SubscriptionId};

use crate::{HostRepository, IndexedRelayEvent};

const CHANNEL_MESSAGE_KIND: u16 = 9;
const MESSAGE_EDIT_KIND: u16 = 40_003;
const NIP09_DELETE_KIND: u16 = 5;
const BUZZ_DELETE_KIND: u16 = 9_005;

/// Relay event emitted to the per-bot actor layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestAction {
    /// A new channel message matched the configured trigger.
    TurnCandidate {
        event_id: EventId,
        channel_id: String,
        reply_to_event_id: Option<EventId>,
        trigger: TriggerMatch,
    },
    /// A new edit targets an event with queued or open work.
    Edit {
        event_id: EventId,
        target_event_id: EventId,
        replacement: Option<TriggerMatch>,
    },
    /// A new deletion targets an event with queued or open work.
    Delete {
        event_id: EventId,
        target_event_id: EventId,
    },
}

/// Failure while indexing or classifying relay traffic.
#[derive(Debug)]
pub enum IngestError<E> {
    /// A relay event carried an invalid event id.
    InvalidEventId(String),
    /// A relay timestamp does not fit SQLite's signed integer range.
    InvalidCreatedAt(u64),
    /// Event tags could not be serialized for the snapshot index.
    InvalidTags(serde_json::Error),
    /// The host repository rejected an index or lookup operation.
    Repository(E),
}

/// Failure while establishing the read-only relay subscription.
#[derive(Debug)]
pub enum RelaySubscribeError {
    /// The Nostr client rejected the subscription request.
    Client(nostr_sdk::client::Error),
    /// No configured relay accepted the subscription.
    NoRelayAccepted,
}

impl fmt::Display for RelaySubscribeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) => write!(formatter, "relay subscription failed: {error}"),
            Self::NoRelayAccepted => formatter.write_str("no relay accepted the subscription"),
        }
    }
}

impl std::error::Error for RelaySubscribeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Client(error) => Some(error),
            Self::NoRelayAccepted => None,
        }
    }
}

impl From<nostr_sdk::client::Error> for RelaySubscribeError {
    fn from(error: nostr_sdk::client::Error) -> Self {
        Self::Client(error)
    }
}

impl<E: fmt::Display> fmt::Display for IngestError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEventId(value) => write!(formatter, "invalid relay event id: {value}"),
            Self::InvalidCreatedAt(value) => {
                write!(formatter, "relay event timestamp is out of range: {value}")
            }
            Self::InvalidTags(error) => write!(formatter, "invalid relay event tags: {error}"),
            Self::Repository(error) => write!(formatter, "relay event persistence failed: {error}"),
        }
    }
}

impl<E> std::error::Error for IngestError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidTags(error) => Some(error),
            Self::Repository(error) => Some(error),
            Self::InvalidEventId(_) | Self::InvalidCreatedAt(_) => None,
        }
    }
}

/// Idempotently index relay traffic and emit only actionable events.
#[derive(Debug)]
pub struct RelayIngest<R> {
    operator_pubkey: String,
    repository: R,
}

impl<R: HostRepository> RelayIngest<R> {
    /// Create an ingest classifier for one operator.
    #[must_use]
    pub fn new(operator_pubkey: impl Into<String>, repository: R) -> Self {
        Self {
            operator_pubkey: operator_pubkey.into(),
            repository,
        }
    }

    /// Index one verified Nostr event and return an actor action when needed.
    ///
    /// Replayed event ids return `Ok(None)`. Ordinary kind 9 channel messages
    /// are indexed but do not emit an action.
    ///
    /// # Errors
    ///
    /// Returns an error when event coordinates are invalid or persistence fails.
    pub fn ingest(&mut self, event: &Event) -> Result<Option<IngestAction>, IngestError<R::Error>> {
        if !matches!(
            event.kind.as_u16(),
            CHANNEL_MESSAGE_KIND | MESSAGE_EDIT_KIND | NIP09_DELETE_KIND | BUZZ_DELETE_KIND
        ) {
            return Ok(None);
        }
        let event_id_hex = event.id.to_hex();
        let event_id = EventId::parse_hex(&event_id_hex)
            .ok_or_else(|| IngestError::InvalidEventId(event_id_hex.clone()))?;
        let tags = event
            .tags
            .iter()
            .map(|tag| tag.as_slice().to_vec())
            .collect::<Vec<_>>();
        let channel_id = tag_value(&tags, "h").map(ToOwned::to_owned);
        let target_event_id = target_event_id(&tags);
        let indexed = IndexedRelayEvent {
            event_id: event_id.clone(),
            author_pubkey: event.pubkey.to_hex(),
            created_at: i64::try_from(event.created_at.as_secs())
                .map_err(|_| IngestError::InvalidCreatedAt(event.created_at.as_secs()))?,
            kind: event.kind.as_u16(),
            content: event.content.clone(),
            tags_json: serde_json::to_string(&tags).map_err(IngestError::InvalidTags)?,
            channel_id: channel_id.clone(),
            target_event_id: target_event_id.clone(),
        };
        if !self
            .repository
            .index_unprocessed_event(&indexed)
            .map_err(IngestError::Repository)?
        {
            return Ok(None);
        }

        match event.kind.as_u16() {
            CHANNEL_MESSAGE_KIND => {
                Ok(self.message_action(event_id, channel_id, &tags, &event.content))
            }
            MESSAGE_EDIT_KIND => self.edit_action(event_id, target_event_id, &event.content),
            NIP09_DELETE_KIND | BUZZ_DELETE_KIND => self.delete_action(event_id, target_event_id),
            _ => Ok(None),
        }
    }

    /// Return the repository after ingest shutdown.
    #[must_use]
    pub fn into_repository(self) -> R {
        self.repository
    }

    fn message_action(
        &self,
        event_id: EventId,
        channel_id: Option<String>,
        tags: &[Vec<String>],
        content: &str,
    ) -> Option<IngestAction> {
        let channel_id = channel_id?;
        let p_tags = tag_values(tags, "p");
        let trigger = TriggerMatch::parse(&self.operator_pubkey, p_tags, content)?;
        Some(IngestAction::TurnCandidate {
            event_id,
            channel_id,
            reply_to_event_id: reply_target(tags),
            trigger,
        })
    }

    fn edit_action(
        &self,
        event_id: EventId,
        target_event_id: Option<EventId>,
        content: &str,
    ) -> Result<Option<IngestAction>, IngestError<R::Error>> {
        let Some(target_event_id) = target_event_id else {
            return Ok(None);
        };
        if self
            .repository
            .active_turn_for_event(&target_event_id)
            .map_err(IngestError::Repository)?
            .is_none()
        {
            return Ok(None);
        }
        let replacement =
            TriggerMatch::parse(&self.operator_pubkey, [&self.operator_pubkey], content);
        Ok(Some(IngestAction::Edit {
            event_id,
            target_event_id,
            replacement,
        }))
    }

    fn delete_action(
        &self,
        event_id: EventId,
        target_event_id: Option<EventId>,
    ) -> Result<Option<IngestAction>, IngestError<R::Error>> {
        let Some(target_event_id) = target_event_id else {
            return Ok(None);
        };
        if self
            .repository
            .active_turn_for_event(&target_event_id)
            .map_err(IngestError::Repository)?
            .is_none()
        {
            return Ok(None);
        }
        Ok(Some(IngestAction::Delete {
            event_id,
            target_event_id,
        }))
    }
}

/// Read-only Nostr subscription for channel messages, edits, and deletes.
#[derive(Debug)]
pub struct RelaySubscriber {
    client: Client,
}

impl RelaySubscriber {
    /// Wrap a configured client without adding signing keys or publish methods.
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Subscribe to the event kinds consumed by [`RelayIngest`].
    ///
    /// # Errors
    ///
    /// Returns an SDK error when no connected relay accepts the subscription.
    pub async fn subscribe(&self) -> Result<(), RelaySubscribeError> {
        let filter = Filter::new().kinds([
            Kind::Custom(CHANNEL_MESSAGE_KIND),
            Kind::Custom(MESSAGE_EDIT_KIND),
            Kind::Custom(NIP09_DELETE_KIND),
            Kind::Custom(BUZZ_DELETE_KIND),
        ]);
        let output = self
            .client
            .subscribe_with_id(SubscriptionId::new("botserver-ingest"), filter, None)
            .await?;
        if output.success.is_empty() {
            return Err(RelaySubscribeError::NoRelayAccepted);
        }
        Ok(())
    }

    /// Receive relay-pool notifications for the ingest loop.
    #[must_use]
    pub fn notifications(&self) -> tokio::sync::broadcast::Receiver<RelayPoolNotification> {
        self.client.notifications()
    }
}

fn tag_value<'a>(tags: &'a [Vec<String>], name: &str) -> Option<&'a str> {
    tags.iter().find_map(|tag| match tag.as_slice() {
        [tag_name, value, ..] if tag_name == name => Some(value.as_str()),
        _ => None,
    })
}

fn tag_values<'a>(tags: &'a [Vec<String>], name: &'a str) -> impl Iterator<Item = &'a str> {
    tags.iter().filter_map(move |tag| match tag.as_slice() {
        [tag_name, value, ..] if tag_name == name => Some(value.as_str()),
        _ => None,
    })
}

fn target_event_id(tags: &[Vec<String>]) -> Option<EventId> {
    tag_values(tags, "e").find_map(EventId::parse_hex)
}

fn reply_target(tags: &[Vec<String>]) -> Option<EventId> {
    tags.iter().rev().find_map(|tag| match tag.as_slice() {
        [name, value, _, marker, ..] if name == "e" && marker == "reply" => {
            EventId::parse_hex(value)
        }
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use botserver_domain::BotId;
    use nostr_sdk::prelude::{EventBuilder, Keys, Tag};

    use super::*;
    use crate::{NewTurn, SessionRecord, TurnRecord, TurnState};

    #[derive(Debug, Default)]
    struct FakeRepository {
        indexed: Vec<IndexedRelayEvent>,
        active_event_id: Option<EventId>,
    }

    impl HostRepository for FakeRepository {
        type Error = std::convert::Infallible;

        fn mark_event_processed(&mut self, event_id: &EventId) -> Result<bool, Self::Error> {
            Ok(!self.indexed.iter().any(|event| &event.event_id == event_id))
        }

        fn index_unprocessed_event(
            &mut self,
            event: &IndexedRelayEvent,
        ) -> Result<bool, Self::Error> {
            if self
                .indexed
                .iter()
                .any(|known| known.event_id == event.event_id)
            {
                return Ok(false);
            }
            self.indexed.push(event.clone());
            Ok(true)
        }

        fn indexed_events_for_channel(
            &self,
            channel_id: &str,
        ) -> Result<Vec<IndexedRelayEvent>, Self::Error> {
            Ok(self
                .indexed
                .iter()
                .filter(|event| event.channel_id.as_deref() == Some(channel_id))
                .cloned()
                .collect())
        }

        fn enqueue_unprocessed_turn(
            &mut self,
            _turn: &NewTurn,
        ) -> Result<Option<TurnRecord>, Self::Error> {
            unreachable!()
        }

        fn enqueue_turn(&mut self, _turn: &NewTurn) -> Result<TurnRecord, Self::Error> {
            unreachable!()
        }

        fn save_session(&mut self, _session: &SessionRecord) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn session(
            &self,
            _bot_id: &BotId,
            _channel_id: &str,
        ) -> Result<Option<SessionRecord>, Self::Error> {
            unreachable!()
        }

        fn open_next_turn(
            &mut self,
            _bot_id: &BotId,
            _channel_id: &str,
            _ask_id: &str,
        ) -> Result<Option<TurnRecord>, Self::Error> {
            unreachable!()
        }

        fn set_turn_state(
            &mut self,
            _ask_id: &str,
            _state: TurnState,
        ) -> Result<bool, Self::Error> {
            unreachable!()
        }

        fn cancel_queued_turn(&mut self, _event_id: &EventId) -> Result<bool, Self::Error> {
            unreachable!()
        }

        fn turn_by_ask_id(&self, _ask_id: &str) -> Result<Option<TurnRecord>, Self::Error> {
            unreachable!()
        }

        fn active_turn_for_event(
            &self,
            event_id: &EventId,
        ) -> Result<Option<TurnRecord>, Self::Error> {
            Ok(
                (self.active_event_id.as_ref() == Some(event_id)).then(|| TurnRecord {
                    sequence: 1,
                    bot_id: BotId::new("bot").expect("bot"),
                    channel_id: "channel".to_owned(),
                    event_id: event_id.clone(),
                    ask_id: Some("ask-id".to_owned()),
                    reply_to_event_id: None,
                    state: TurnState::Open,
                }),
            )
        }

        fn turns_for_session(
            &self,
            _bot_id: &BotId,
            _channel_id: &str,
        ) -> Result<Vec<TurnRecord>, Self::Error> {
            unreachable!()
        }

        fn sessions_with_pending_turns(&self) -> Result<Vec<SessionRecord>, Self::Error> {
            unreachable!()
        }
    }

    fn event(kind: u16, content: &str, tags: impl IntoIterator<Item = Tag>) -> Event {
        EventBuilder::new(Kind::Custom(kind), content)
            .tags(tags)
            .sign_with_keys(&Keys::generate())
            .expect("event")
    }

    fn tag(parts: &[&str]) -> Tag {
        Tag::parse(parts.iter().copied()).expect("tag")
    }

    #[test]
    fn ordinary_messages_are_indexed_without_emitting() {
        let mut ingest = RelayIngest::new("a".repeat(64), FakeRepository::default());
        let message = event(CHANNEL_MESSAGE_KIND, "ordinary", [tag(&["h", "channel"])]);

        assert_eq!(ingest.ingest(&message).unwrap(), None);
        assert_eq!(ingest.repository.indexed.len(), 1);
        assert_eq!(ingest.repository.indexed[0].content, "ordinary");
    }

    #[test]
    fn unsupported_event_kinds_are_ignored() {
        let mut ingest = RelayIngest::new("a".repeat(64), FakeRepository::default());
        let reaction = event(7, "+", [tag(&["h", "channel"])]);

        assert_eq!(ingest.ingest(&reaction).unwrap(), None);
        assert!(ingest.repository.indexed.is_empty());
    }

    #[test]
    fn triggers_emit_once_with_channel_and_reply_coordinates() {
        let operator = "a".repeat(64);
        let reply = "b".repeat(64);
        let mut ingest = RelayIngest::new(&operator, FakeRepository::default());
        let message = event(
            CHANNEL_MESSAGE_KIND,
            "@daniel bot: inspect this",
            [
                tag(&["h", "channel"]),
                tag(&["p", &operator]),
                tag(&["e", &reply, "", "reply"]),
            ],
        );

        let action = ingest.ingest(&message).unwrap().expect("trigger");
        let IngestAction::TurnCandidate {
            channel_id,
            reply_to_event_id,
            trigger,
            ..
        } = action
        else {
            panic!("expected turn candidate");
        };
        assert_eq!(channel_id, "channel");
        assert_eq!(reply_to_event_id.expect("reply").as_str(), reply);
        assert_eq!(trigger.request(), "inspect this");
        assert_eq!(ingest.ingest(&message).unwrap(), None);
    }

    #[test]
    fn edits_and_deletes_emit_only_for_active_turns() {
        let operator = "a".repeat(64);
        let target = EventId::parse_hex(&"b".repeat(64)).expect("target");
        let repository = FakeRepository {
            indexed: Vec::new(),
            active_event_id: Some(target.clone()),
        };
        let mut ingest = RelayIngest::new(&operator, repository);
        let edit = event(
            MESSAGE_EDIT_KIND,
            "bot: replacement",
            [tag(&["h", "channel"]), tag(&["e", target.as_str()])],
        );
        let delete = event(
            BUZZ_DELETE_KIND,
            "",
            [tag(&["h", "channel"]), tag(&["e", target.as_str()])],
        );

        let IngestAction::Edit {
            target_event_id,
            replacement,
            ..
        } = ingest.ingest(&edit).unwrap().expect("edit")
        else {
            panic!("expected edit");
        };
        assert_eq!(target_event_id, target);
        assert_eq!(replacement.expect("replacement").request(), "replacement");
        assert!(matches!(
            ingest.ingest(&delete).unwrap(),
            Some(IngestAction::Delete { .. })
        ));

        ingest.repository.active_event_id = None;
        let inactive_delete = event(NIP09_DELETE_KIND, "", [tag(&["e", &"c".repeat(64)])]);
        assert_eq!(ingest.ingest(&inactive_delete).unwrap(), None);
    }

    #[test]
    fn edits_that_remove_the_trigger_emit_without_replacement_work() {
        let operator = "a".repeat(64);
        let target = EventId::parse_hex(&"b".repeat(64)).expect("target");
        let repository = FakeRepository {
            indexed: Vec::new(),
            active_event_id: Some(target.clone()),
        };
        let mut ingest = RelayIngest::new(operator, repository);
        let edit = event(
            MESSAGE_EDIT_KIND,
            "human reply now",
            [tag(&["e", target.as_str()])],
        );

        assert!(matches!(
            ingest.ingest(&edit).unwrap(),
            Some(IngestAction::Edit {
                replacement: None,
                ..
            })
        ));
    }
}
