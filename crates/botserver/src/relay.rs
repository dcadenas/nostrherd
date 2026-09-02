//! Read-only relay subscription and event classification.

use std::fmt;
use std::time::Duration;

use botserver_domain::{EventId, TriggerMatch};
use futures::Stream;
use nostr_sdk::prelude::{
    Client, ClientNotification, Event, Filter, Kind, SingleLetterTag, SubscriptionId, Timestamp,
};

use crate::{HostRepository, IndexedRelayEvent};

const CHANNEL_MESSAGE_KIND: u16 = 9;
const STREAM_MESSAGE_V2_KIND: u16 = 40_002;
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
    Client(nostr_sdk::error::Error),
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

impl From<nostr_sdk::error::Error> for RelaySubscribeError {
    fn from(error: nostr_sdk::error::Error) -> Self {
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
    relay_pubkey: String,
    repository: R,
}

impl<R: HostRepository> RelayIngest<R> {
    /// Create an ingest classifier for one operator.
    #[must_use]
    pub fn new(
        operator_pubkey: impl Into<String>,
        relay_pubkey: impl Into<String>,
        repository: R,
    ) -> Self {
        Self {
            operator_pubkey: operator_pubkey.into().to_ascii_lowercase(),
            relay_pubkey: relay_pubkey.into().to_ascii_lowercase(),
            repository,
        }
    }

    /// Index one verified Nostr event and return an actor action when needed.
    ///
    /// Replayed event ids return `Ok(None)` after downstream processing. Ordinary
    /// kind 9 channel messages are indexed but do not emit an action. Consumers
    /// atomically persist a trigger candidate. Every emitted action must be
    /// acknowledged with [`HostRepository::mark_event_processed`], including
    /// actions the consumer intentionally declines; turn enqueue does this
    /// atomically for accepted trigger candidates.
    ///
    /// # Errors
    ///
    /// Returns an error when event coordinates are invalid or persistence fails.
    pub fn ingest(&mut self, event: &Event) -> Result<Option<IngestAction>, IngestError<R::Error>> {
        if !matches!(
            event.kind.as_u16(),
            CHANNEL_MESSAGE_KIND
                | STREAM_MESSAGE_V2_KIND
                | MESSAGE_EDIT_KIND
                | NIP09_DELETE_KIND
                | BUZZ_DELETE_KIND
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
        let author = effective_author(&event.pubkey.to_hex(), &self.relay_pubkey, &tags);
        let action = match event.kind.as_u16() {
            CHANNEL_MESSAGE_KIND | STREAM_MESSAGE_V2_KIND => Ok(self.message_action(
                event_id.clone(),
                channel_id.clone(),
                &tags,
                &author,
                &event.content,
            )),
            MESSAGE_EDIT_KIND => self.edit_action(
                event_id.clone(),
                target_event_id.clone(),
                &author.pubkey,
                &event.content,
            ),
            NIP09_DELETE_KIND | BUZZ_DELETE_KIND => self.delete_action(
                event_id.clone(),
                target_event_id.clone(),
                &author.pubkey,
                event.kind.as_u16(),
            ),
            _ => Ok(None),
        }?;
        let indexed = IndexedRelayEvent {
            event_id: event_id.clone(),
            author_pubkey: author.pubkey,
            created_at: i64::try_from(event.created_at.as_secs())
                .map_err(|_| IngestError::InvalidCreatedAt(event.created_at.as_secs()))?,
            kind: event.kind.as_u16(),
            content: event.content.clone(),
            tags_json: serde_json::to_string(&tags).map_err(IngestError::InvalidTags)?,
            channel_id: channel_id.clone(),
            target_event_id: target_event_id.clone(),
        };
        let processed = self
            .repository
            .event_processed(&event_id)
            .map_err(IngestError::Repository)?;
        self.repository
            .index_event(&indexed, action.is_some() && !processed)
            .map_err(IngestError::Repository)?;
        // Indexing and processing are separate so actions remain replayable
        // until the actor persists their corresponding state change.
        if processed {
            return Ok(None);
        }

        Ok(action)
    }

    /// Return the repository after ingest shutdown.
    #[must_use]
    pub fn into_repository(self) -> R {
        self.repository
    }

    /// Borrow the repository to atomically persist an emitted action.
    #[must_use]
    pub fn repository_mut(&mut self) -> &mut R {
        &mut self.repository
    }

    fn message_action(
        &self,
        event_id: EventId,
        channel_id: Option<String>,
        tags: &[Vec<String>],
        author: &EffectiveAuthor,
        content: &str,
    ) -> Option<IngestAction> {
        let channel_id = channel_id?;
        let mut skipped_attribution = false;
        let p_tags = tag_values(tags, "p").filter(|pubkey| {
            if !skipped_attribution
                && author
                    .attribution_p_tag
                    .as_deref()
                    .is_some_and(|attribution| attribution.eq_ignore_ascii_case(pubkey))
            {
                skipped_attribution = true;
                false
            } else {
                true
            }
        });
        let p_tags = p_tags.map(str::to_ascii_lowercase).collect::<Vec<_>>();
        let trigger = TriggerMatch::parse(
            &self.operator_pubkey,
            p_tags.iter().map(String::as_str),
            content,
        )?;
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
        author_pubkey: &str,
        content: &str,
    ) -> Result<Option<IngestAction>, IngestError<R::Error>> {
        let Some(target_event_id) = target_event_id else {
            return Ok(None);
        };
        let Some(target) = self
            .repository
            .indexed_event(&target_event_id)
            .map_err(IngestError::Repository)?
        else {
            return Ok(None);
        };
        if target.author_pubkey != author_pubkey {
            return Ok(None);
        }
        if self
            .repository
            .active_turn_for_event(&target_event_id)
            .map_err(IngestError::Repository)?
            .is_none()
        {
            return Ok(None);
        }
        // An active target already proved the original event p-tagged the operator.
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
        author_pubkey: &str,
        kind: u16,
    ) -> Result<Option<IngestAction>, IngestError<R::Error>> {
        let Some(target_event_id) = target_event_id else {
            return Ok(None);
        };
        let Some(target) = self
            .repository
            .indexed_event(&target_event_id)
            .map_err(IngestError::Repository)?
        else {
            return Ok(None);
        };
        // Buzz kind 9005 is a relay-authorized moderator tombstone. NIP-09
        // kind 5 must be signed by the target event's author.
        if kind == NIP09_DELETE_KIND && target.author_pubkey != author_pubkey {
            return Ok(None);
        }
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

    /// Subscribe to operator mentions, known channels, and active-turn mutations.
    ///
    /// Call this again whenever `channel_ids` or `active_event_ids` changes.
    /// Empty sets close the corresponding prior subscription. `since` is an
    /// inclusive persisted cursor, not the current wall clock.
    ///
    /// # Errors
    ///
    /// Returns an SDK error when no connected relay accepts a requested
    /// subscription. Earlier filters can already be live when a later filter
    /// fails; callers should treat an error as fatal and replace the client.
    pub async fn subscribe(
        &self,
        operator_pubkey: &str,
        channel_ids: &[String],
        active_event_ids: &[EventId],
        since: Timestamp,
    ) -> Result<(), RelaySubscribeError> {
        self.subscribe_filter("botserver-messages", message_filter(operator_pubkey, since))
            .await?;
        self.update_filter("botserver-channels", channel_filter(channel_ids, since))
            .await?;
        self.update_filter(
            "botserver-mutations",
            mutation_filter(active_event_ids, since),
        )
        .await?;
        Ok(())
    }

    async fn update_filter(
        &self,
        id: &str,
        filter: Option<Filter>,
    ) -> Result<(), RelaySubscribeError> {
        if let Some(filter) = filter {
            self.subscribe_filter(id, filter).await
        } else {
            self.client.unsubscribe(&SubscriptionId::new(id)).await?;
            Ok(())
        }
    }

    async fn subscribe_filter(&self, id: &str, filter: Filter) -> Result<(), RelaySubscribeError> {
        let output = self
            .client
            .subscribe(filter)
            .with_id(SubscriptionId::new(id))
            .await?;
        if output.success.is_empty() {
            return Err(RelaySubscribeError::NoRelayAccepted);
        }
        Ok(())
    }

    /// Receive client notifications for the ingest loop.
    pub fn notifications(&self) -> impl Stream<Item = ClientNotification> + Send {
        self.client.notifications()
    }

    /// Fetch stored operator-mention messages since `since`.
    ///
    /// # Errors
    ///
    /// Returns an SDK error when the fetch cannot complete.
    pub async fn fetch_messages(
        &self,
        operator_pubkey: &str,
        since: Timestamp,
    ) -> Result<Vec<Event>, RelaySubscribeError> {
        self.fetch_filtered(Some(message_filter(operator_pubkey, since)))
            .await
    }

    /// Fetch stored `h`-tag traffic for known channels since `since`.
    ///
    /// # Errors
    ///
    /// Returns an SDK error when the fetch cannot complete.
    pub async fn fetch_channel_messages(
        &self,
        channel_ids: &[String],
        since: Timestamp,
    ) -> Result<Vec<Event>, RelaySubscribeError> {
        self.fetch_filtered(channel_filter(channel_ids, since))
            .await
    }

    /// Fetch stored edits and deletes for active turns since `since`.
    ///
    /// # Errors
    ///
    /// Returns an SDK error when the fetch cannot complete.
    pub async fn fetch_mutations(
        &self,
        active_event_ids: &[EventId],
        since: Timestamp,
    ) -> Result<Vec<Event>, RelaySubscribeError> {
        self.fetch_filtered(mutation_filter(active_event_ids, since))
            .await
    }

    async fn fetch_filtered(
        &self,
        filter: Option<Filter>,
    ) -> Result<Vec<Event>, RelaySubscribeError> {
        let Some(filter) = filter else {
            return Ok(Vec::new());
        };
        let events = self
            .client
            .fetch_events(filter)
            .timeout(Duration::from_secs(5))
            .await?;
        Ok(events.into_iter().collect())
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

#[derive(Debug)]
struct EffectiveAuthor {
    pubkey: String,
    attribution_p_tag: Option<String>,
}

fn effective_author(
    signing_pubkey: &str,
    relay_pubkey: &str,
    tags: &[Vec<String>],
) -> EffectiveAuthor {
    if signing_pubkey != relay_pubkey {
        return EffectiveAuthor {
            pubkey: signing_pubkey.to_owned(),
            attribution_p_tag: None,
        };
    }
    if let Some(actor) = tag_values(tags, "actor").find(|value| is_pubkey(value)) {
        return EffectiveAuthor {
            pubkey: actor.to_ascii_lowercase(),
            attribution_p_tag: None,
        };
    }
    if let Some(author) = tag_values(tags, "p").find(|value| is_pubkey(value)) {
        return EffectiveAuthor {
            pubkey: author.to_ascii_lowercase(),
            attribution_p_tag: Some(author.to_ascii_lowercase()),
        };
    }
    EffectiveAuthor {
        pubkey: signing_pubkey.to_owned(),
        attribution_p_tag: None,
    }
}

fn is_pubkey(value: &str) -> bool {
    value.len() == 64 && value.chars().all(|character| character.is_ascii_hexdigit())
}

fn target_event_id(tags: &[Vec<String>]) -> Option<EventId> {
    let mut targets = tag_values(tags, "e").filter_map(EventId::parse_hex);
    let target = targets.next()?;
    targets.next().is_none().then_some(target)
}

fn reply_target(tags: &[Vec<String>]) -> Option<EventId> {
    tags.iter().rev().find_map(|tag| match tag.as_slice() {
        [name, value, _, marker, ..] if name == "e" && marker == "reply" => {
            EventId::parse_hex(value)
        }
        _ => None,
    })
}

fn message_filter(operator_pubkey: &str, since: Timestamp) -> Filter {
    Filter::new()
        .kinds([
            Kind::Custom(CHANNEL_MESSAGE_KIND),
            Kind::Custom(STREAM_MESSAGE_V2_KIND),
        ])
        .custom_tag(SingleLetterTag::LOWERCASE_P, operator_pubkey)
        .since(since)
}

fn channel_filter(channel_ids: &[String], since: Timestamp) -> Option<Filter> {
    if channel_ids.is_empty() {
        return None;
    }
    Some(
        Filter::new()
            .kinds([
                Kind::Custom(CHANNEL_MESSAGE_KIND),
                Kind::Custom(STREAM_MESSAGE_V2_KIND),
            ])
            .custom_tags(
                SingleLetterTag::LOWERCASE_H,
                channel_ids.iter().map(String::as_str),
            )
            .since(since),
    )
}

fn mutation_filter(active_event_ids: &[EventId], since: Timestamp) -> Option<Filter> {
    if active_event_ids.is_empty() {
        return None;
    }
    Some(
        Filter::new()
            .kinds([
                Kind::Custom(MESSAGE_EDIT_KIND),
                Kind::Custom(NIP09_DELETE_KIND),
                Kind::Custom(BUZZ_DELETE_KIND),
            ])
            .custom_tags(
                SingleLetterTag::LOWERCASE_E,
                active_event_ids.iter().map(EventId::as_str),
            )
            .since(since),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use botserver_domain::BotId;
    use nostr_sdk::prelude::{EventBuilder, FinalizeEvent, Keys, Tag};

    use super::*;
    use crate::{NewTurn, SessionRecord, TurnRecord, TurnState};

    #[derive(Debug, Default)]
    struct FakeRepository {
        indexed: Vec<IndexedRelayEvent>,
        pending: HashSet<EventId>,
        processed: HashSet<EventId>,
        active_event_id: Option<EventId>,
    }

    impl HostRepository for FakeRepository {
        type Error = std::convert::Infallible;

        fn mark_event_processed(&mut self, event_id: &EventId) -> Result<bool, Self::Error> {
            self.pending.remove(event_id);
            Ok(self.processed.insert(event_id.clone()))
        }

        fn index_event(
            &mut self,
            event: &IndexedRelayEvent,
            pending_action: bool,
        ) -> Result<bool, Self::Error> {
            let exists = self
                .indexed
                .iter()
                .any(|known| known.event_id == event.event_id);
            if !exists {
                self.indexed.push(event.clone());
            }
            if pending_action {
                self.pending.insert(event.event_id.clone());
            }
            Ok(!exists)
        }

        fn event_processed(&self, event_id: &EventId) -> Result<bool, Self::Error> {
            Ok(self.processed.contains(event_id))
        }

        fn indexed_event(
            &self,
            event_id: &EventId,
        ) -> Result<Option<IndexedRelayEvent>, Self::Error> {
            Ok(self
                .indexed
                .iter()
                .find(|event| &event.event_id == event_id)
                .cloned())
        }

        fn latest_body_for_event(&self, event_id: &EventId) -> Result<Option<String>, Self::Error> {
            Ok(self
                .indexed
                .iter()
                .filter(|event| {
                    if event.event_id == *event_id {
                        return true;
                    }
                    event.target_event_id.as_ref() == Some(event_id)
                        && event.kind == 40003
                        && self.indexed.iter().any(|original| {
                            original.event_id == *event_id
                                && original.author_pubkey == event.author_pubkey
                        })
                })
                .max_by_key(|event| (event.created_at, event.event_id.as_str().to_owned()))
                .map(|event| event.content.clone()))
        }

        fn relay_replay_since(&self) -> Result<Option<i64>, Self::Error> {
            let oldest_pending = self
                .indexed
                .iter()
                .filter(|event| {
                    self.pending.contains(&event.event_id)
                        && !self.processed.contains(&event.event_id)
                })
                .map(|event| event.created_at)
                .min();
            Ok(oldest_pending.or_else(|| {
                self.indexed
                    .iter()
                    .map(|event| event.created_at)
                    .max()
                    .map(|latest| latest.saturating_sub(900).max(0))
            }))
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

        fn session_by_name(
            &self,
            _session_name: &str,
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

        fn claim_turn_for_publish(&mut self, _ask_id: &str) -> Result<bool, Self::Error> {
            unreachable!()
        }

        fn release_publish_claim(&mut self, _ask_id: &str) -> Result<bool, Self::Error> {
            unreachable!()
        }

        fn cancel_unclaimed_turn(
            &mut self,
            _event_id: &EventId,
        ) -> Result<Option<TurnRecord>, Self::Error> {
            unreachable!()
        }

        fn replace_unclaimed_turn(
            &mut self,
            _turn: &NewTurn,
        ) -> Result<Option<crate::TurnReplacement>, Self::Error> {
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

        fn known_channel_ids(&self) -> Result<Vec<String>, Self::Error> {
            unreachable!()
        }

        fn active_event_ids(&self) -> Result<Vec<EventId>, Self::Error> {
            unreachable!()
        }

        fn save_outbound_attempt(
            &mut self,
            _attempt: &crate::outbox::OutboundAttempt,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn outbound_attempt(
            &self,
            _ask_id: &str,
        ) -> Result<Option<crate::outbox::OutboundAttempt>, Self::Error> {
            unreachable!()
        }

        fn mark_outbound_accepted(
            &mut self,
            _ask_id: &str,
            _event_id: &str,
        ) -> Result<bool, Self::Error> {
            unreachable!()
        }
    }

    fn event(kind: u16, content: &str, tags: impl IntoIterator<Item = Tag>) -> Event {
        event_with_keys(&Keys::generate(), kind, content, tags)
    }

    fn event_with_keys(
        keys: &Keys,
        kind: u16,
        content: &str,
        tags: impl IntoIterator<Item = Tag>,
    ) -> Event {
        EventBuilder::new(Kind::Custom(kind), content)
            .tags(tags)
            .finalize(keys)
            .expect("event")
    }

    fn tag(parts: &[&str]) -> Tag {
        Tag::parse(parts.iter().copied()).expect("tag")
    }

    fn relay_pubkey() -> String {
        "f".repeat(64)
    }

    #[test]
    fn ordinary_messages_are_indexed_without_emitting() {
        let mut ingest =
            RelayIngest::new("a".repeat(64), relay_pubkey(), FakeRepository::default());
        let message = event(CHANNEL_MESSAGE_KIND, "ordinary", [tag(&["h", "channel"])]);

        assert_eq!(ingest.ingest(&message).unwrap(), None);
        assert_eq!(ingest.repository.indexed.len(), 1);
        assert_eq!(ingest.repository.indexed[0].content, "ordinary");
    }

    #[test]
    fn stream_message_v2_can_emit_a_trigger() {
        let operator = "a".repeat(64);
        let mut ingest = RelayIngest::new(&operator, relay_pubkey(), FakeRepository::default());
        let message = event_with_keys(
            &Keys::generate(),
            STREAM_MESSAGE_V2_KIND,
            "bot: rich message",
            [tag(&["h", "channel"]), tag(&["p", &operator])],
        );

        assert!(matches!(
            ingest.ingest(&message).unwrap(),
            Some(IngestAction::TurnCandidate { .. })
        ));
    }

    #[test]
    fn unsupported_event_kinds_are_ignored() {
        let mut ingest =
            RelayIngest::new("a".repeat(64), relay_pubkey(), FakeRepository::default());
        let reaction = event(7, "+", [tag(&["h", "channel"])]);

        assert_eq!(ingest.ingest(&reaction).unwrap(), None);
        assert!(ingest.repository.indexed.is_empty());
    }

    #[test]
    fn triggers_emit_once_with_channel_and_reply_coordinates() {
        let operator = "a".repeat(64);
        let reply = "b".repeat(64);
        let mut ingest = RelayIngest::new(&operator, relay_pubkey(), FakeRepository::default());
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
        ingest
            .repository
            .mark_event_processed(&EventId::parse_hex(&message.id.to_hex()).expect("id"))
            .unwrap();
        assert_eq!(ingest.ingest(&message).unwrap(), None);
    }

    #[test]
    fn relay_signed_actor_can_edit_its_trigger() {
        let operator = "a".repeat(64);
        let actor = Keys::generate();
        let actor_pubkey = actor.public_key().to_hex();
        let uppercase_actor = actor_pubkey.to_ascii_uppercase();
        let relay = Keys::generate();
        let relay_pubkey = relay.public_key().to_hex();
        let mut ingest = RelayIngest::new(&operator, relay_pubkey, FakeRepository::default());
        let message = event_with_keys(
            &relay,
            CHANNEL_MESSAGE_KIND,
            "bot: original",
            [
                tag(&["h", "channel"]),
                tag(&["actor", &uppercase_actor]),
                tag(&["p", &operator]),
            ],
        );
        let target = EventId::parse_hex(&message.id.to_hex()).expect("target");

        assert!(matches!(
            ingest.ingest(&message).unwrap(),
            Some(IngestAction::TurnCandidate { .. })
        ));
        assert_eq!(
            ingest
                .repository
                .indexed_event(&target)
                .unwrap()
                .expect("indexed")
                .author_pubkey,
            actor_pubkey
        );
        ingest.repository.active_event_id = Some(target.clone());
        let edit = event_with_keys(
            &actor,
            MESSAGE_EDIT_KIND,
            "bot: replacement",
            [tag(&["e", target.as_str()])],
        );
        assert!(matches!(
            ingest.ingest(&edit).unwrap(),
            Some(IngestAction::Edit { .. })
        ));
    }

    #[test]
    fn relay_attribution_p_tag_is_not_an_operator_mention() {
        let operator = "a".repeat(64);
        let relay = Keys::generate();
        let relay_pubkey = relay.public_key().to_hex();
        let mut ingest = RelayIngest::new(&operator, relay_pubkey, FakeRepository::default());
        let message = event_with_keys(
            &relay,
            CHANNEL_MESSAGE_KIND,
            "bot: human message",
            [tag(&["h", "channel"]), tag(&["p", &operator])],
        );

        assert_eq!(ingest.ingest(&message).unwrap(), None);
    }

    #[test]
    fn edits_and_deletes_emit_only_for_active_turns() {
        let operator = "a".repeat(64);
        let target = EventId::parse_hex(&"b".repeat(64)).expect("target");
        let author = Keys::generate();
        let repository = FakeRepository {
            indexed: vec![indexed_target(&target, &author)],
            pending: HashSet::new(),
            processed: HashSet::new(),
            active_event_id: Some(target.clone()),
        };
        let mut ingest = RelayIngest::new(&operator, relay_pubkey(), repository);
        let edit = event_with_keys(
            &author,
            MESSAGE_EDIT_KIND,
            "bot: replacement",
            [tag(&["h", "channel"]), tag(&["e", target.as_str()])],
        );
        let delete = event_with_keys(
            &author,
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
        let author = Keys::generate();
        let repository = FakeRepository {
            indexed: vec![indexed_target(&target, &author)],
            pending: HashSet::new(),
            processed: HashSet::new(),
            active_event_id: Some(target.clone()),
        };
        let mut ingest = RelayIngest::new(operator, relay_pubkey(), repository);
        let edit = event_with_keys(
            &author,
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

    #[test]
    fn third_party_edits_and_nip09_deletes_are_ignored() {
        let operator = "a".repeat(64);
        let target = EventId::parse_hex(&"b".repeat(64)).expect("target");
        let author = Keys::generate();
        let repository = FakeRepository {
            indexed: vec![indexed_target(&target, &author)],
            pending: HashSet::new(),
            processed: HashSet::new(),
            active_event_id: Some(target.clone()),
        };
        let mut ingest = RelayIngest::new(operator, relay_pubkey(), repository);
        let attacker = Keys::generate();
        let edit = event_with_keys(
            &attacker,
            MESSAGE_EDIT_KIND,
            "bot: injected",
            [tag(&["e", target.as_str()])],
        );
        let delete = event_with_keys(
            &attacker,
            NIP09_DELETE_KIND,
            "",
            [tag(&["e", target.as_str()])],
        );
        let moderator_delete = event_with_keys(
            &attacker,
            BUZZ_DELETE_KIND,
            "",
            [tag(&["e", target.as_str()])],
        );

        assert_eq!(ingest.ingest(&edit).unwrap(), None);
        assert_eq!(ingest.ingest(&delete).unwrap(), None);
        assert!(matches!(
            ingest.ingest(&moderator_delete).unwrap(),
            Some(IngestAction::Delete { .. })
        ));
    }

    #[test]
    fn mutations_with_multiple_targets_are_ignored() {
        let target = EventId::parse_hex(&"b".repeat(64)).expect("target");
        let author = Keys::generate();
        let repository = FakeRepository {
            indexed: vec![indexed_target(&target, &author)],
            pending: HashSet::new(),
            processed: HashSet::new(),
            active_event_id: Some(target.clone()),
        };
        let mut ingest = RelayIngest::new("a".repeat(64), relay_pubkey(), repository);
        let delete = event_with_keys(
            &author,
            NIP09_DELETE_KIND,
            "",
            [tag(&["e", target.as_str()]), tag(&["e", &"c".repeat(64)])],
        );

        assert_eq!(ingest.ingest(&delete).unwrap(), None);
    }

    #[test]
    fn stamped_self_posts_do_not_emit_triggers() {
        let operator = "a".repeat(64);
        let mut ingest = RelayIngest::new(&operator, relay_pubkey(), FakeRepository::default());
        let message = event_with_keys(
            &Keys::generate(),
            CHANNEL_MESSAGE_KIND,
            "[bot]: bot: loop",
            [tag(&["h", "channel"]), tag(&["p", &operator])],
        );

        assert_eq!(ingest.ingest(&message).unwrap(), None);
    }

    #[test]
    fn subscription_filters_scope_messages_and_mutations() {
        let operator = "a".repeat(64);
        let active = EventId::parse_hex(&"b".repeat(64)).expect("active");
        let since = Timestamp::from(42_u64);

        let message_json = serde_json::to_value(message_filter(&operator, since)).unwrap();
        assert_eq!(message_json["kinds"], serde_json::json!([9, 40002]));
        assert_eq!(message_json["#p"], serde_json::json!([operator]));
        assert_eq!(message_json["since"], 42);

        let channel_json = serde_json::to_value(
            channel_filter(&["channel-a".to_owned(), "channel-b".to_owned()], since)
                .expect("filter"),
        )
        .unwrap();
        assert_eq!(channel_json["kinds"], serde_json::json!([9, 40002]));
        assert_eq!(
            channel_json["#h"],
            serde_json::json!(["channel-a", "channel-b"])
        );
        assert_eq!(channel_json["since"], 42);
        assert!(channel_filter(&[], since).is_none());

        let mutation_json =
            serde_json::to_value(mutation_filter(&[active], since).expect("filter")).unwrap();
        assert_eq!(mutation_json["kinds"], serde_json::json!([5, 9005, 40003]));
        assert_eq!(mutation_json["#e"], serde_json::json!(["b".repeat(64)]));
        assert_eq!(mutation_json["since"], 42);
        assert!(mutation_filter(&[], since).is_none());
    }

    fn indexed_target(event_id: &EventId, author: &Keys) -> IndexedRelayEvent {
        IndexedRelayEvent {
            event_id: event_id.clone(),
            author_pubkey: author.public_key().to_hex(),
            created_at: 1,
            kind: CHANNEL_MESSAGE_KIND,
            content: "bot: original".to_owned(),
            tags_json: "[]".to_owned(),
            channel_id: Some("channel".to_owned()),
            target_event_id: None,
        }
    }
}
