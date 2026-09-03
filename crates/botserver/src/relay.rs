//! Read-only relay subscription and event classification.

use std::fmt;
use std::time::Duration;

use botserver_domain::{
    place_display as usable_place_display, BotId, EventId, TriggerMatch, INBOUND_TRIGGER,
};
use futures::Stream;
use nostr_sdk::prelude::{
    Client, ClientNotification, Event, Filter, Kind, PublicKey, SingleLetterTag, SubscriptionId,
    Timestamp,
};

use crate::{HostRepository, IndexedRelayEvent};

const CHANNEL_MESSAGE_KIND: u16 = 9;
const STREAM_MESSAGE_V2_KIND: u16 = 40_002;
const MESSAGE_EDIT_KIND: u16 = 40_003;
const NIP09_DELETE_KIND: u16 = 5;
const BUZZ_DELETE_KIND: u16 = 9_005;
const GROUP_METADATA_KIND: u16 = 39_000;
const PROFILE_KIND: u16 = 0;

/// Relay event emitted to the per-bot actor layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestAction {
    /// A new channel message matched one configured bot's trigger.
    TurnCandidate {
        bot_id: BotId,
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

fn trigger_bot_id(token: &str) -> Option<BotId> {
    token.strip_suffix(':').and_then(BotId::new)
}

fn default_inbound_triggers() -> Vec<(BotId, String)> {
    trigger_bot_id(INBOUND_TRIGGER)
        .map(|id| vec![(id, INBOUND_TRIGGER.to_owned())])
        .unwrap_or_default()
}

/// Idempotently index relay traffic and emit only actionable events.
#[derive(Debug)]
pub struct RelayIngest<R> {
    operator_pubkey: String,
    relay_pubkey: String,
    inbound_triggers: Vec<(BotId, String)>,
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
            inbound_triggers: default_inbound_triggers(),
            repository,
        }
    }

    /// Classify inbound bodies with this `{bot-id}:` token.
    #[must_use]
    pub fn with_inbound_trigger(self, inbound_trigger: impl Into<String>) -> Self {
        let token = inbound_trigger.into();
        match trigger_bot_id(&token) {
            Some(bot_id) => self.with_inbound_triggers([(bot_id, token)]),
            None => self,
        }
    }

    /// Classify inbound bodies against every configured `{bot-id}:` token.
    #[must_use]
    pub fn with_inbound_triggers(
        mut self,
        triggers: impl IntoIterator<Item = (BotId, String)>,
    ) -> Self {
        self.inbound_triggers = triggers
            .into_iter()
            .filter(|(_, token)| !token.is_empty())
            .collect();
        self
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
                &event.pubkey.to_hex(),
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

    fn trigger_token(&self, bot_id: &BotId) -> Option<&str> {
        self.inbound_triggers
            .iter()
            .find(|(id, _)| id == bot_id)
            .map(|(_, token)| token.as_str())
    }

    fn match_trigger(
        &self,
        signing_pubkey: &str,
        p_tags: &[String],
        content: &str,
    ) -> Option<(BotId, TriggerMatch)> {
        self.inbound_triggers.iter().find_map(|(bot_id, token)| {
            TriggerMatch::parse(
                &self.operator_pubkey,
                signing_pubkey,
                p_tags.iter().map(String::as_str),
                token,
                content,
            )
            .map(|trigger| (bot_id.clone(), trigger))
        })
    }

    fn message_action(
        &self,
        event_id: EventId,
        channel_id: Option<String>,
        tags: &[Vec<String>],
        signing_pubkey: &str,
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
        let (bot_id, trigger) = self.match_trigger(signing_pubkey, &p_tags, content)?;
        Some(IngestAction::TurnCandidate {
            bot_id,
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
        let Some(active) = self
            .repository
            .active_turn_for_event(&target_event_id)
            .map_err(IngestError::Repository)?
        else {
            return Ok(None);
        };
        // An active target already proved the original event p-tagged the operator.
        let replacement = self.trigger_token(&active.bot_id).and_then(|token| {
            TriggerMatch::parse(
                &self.operator_pubkey,
                author_pubkey,
                [&self.operator_pubkey],
                token,
                content,
            )
        });
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

    /// Subscribe to operator mentions, operator-authored messages, known
    /// channels, and active-turn mutations.
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
        if let Some(filter) = operator_authored_filter(operator_pubkey, since) {
            self.subscribe_filter("botserver-authored", filter).await?;
        }
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

    /// Fetch stored operator-mention and operator-authored messages since `since`.
    ///
    /// # Errors
    ///
    /// Returns an SDK error when the fetch cannot complete.
    pub async fn fetch_messages(
        &self,
        operator_pubkey: &str,
        since: Timestamp,
    ) -> Result<Vec<Event>, RelaySubscribeError> {
        let mut events = self
            .fetch_filtered(Some(message_filter(operator_pubkey, since)))
            .await?;
        events.extend(
            self.fetch_filtered(operator_authored_filter(operator_pubkey, since))
                .await?,
        );
        Ok(events)
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

    /// Resolve a readable place label from Buzz group metadata.
    ///
    /// Kind 39000 supplies the channel `name`. A generic 1-1 `DM` title
    /// uses the other participant's kind-0 profile display.
    ///
    /// # Errors
    ///
    /// Returns an SDK error when the fetch cannot complete.
    pub async fn place_display(
        &self,
        operator_pubkey: &str,
        channel_id: &str,
    ) -> Result<String, RelaySubscribeError> {
        let events = self
            .fetch_filtered(Some(place_metadata_filter(channel_id)))
            .await?;
        let Some(event) = newest_event(&events) else {
            return Ok(String::new());
        };
        let tags = event_tags(event);
        let metadata = parse_place_metadata(&tags);
        let peer = other_participant(operator_pubkey, &metadata.participants);
        let peer_display = if wants_peer_display(&metadata.name) {
            match peer {
                Some(pubkey) => self.profile_display(pubkey).await?,
                None => None,
            }
        } else {
            None
        };
        Ok(usable_place_display(
            &metadata.name,
            peer_display.as_deref(),
        ))
    }

    async fn profile_display(&self, pubkey: &str) -> Result<Option<String>, RelaySubscribeError> {
        let events = self.fetch_filtered(profile_filter(pubkey)).await?;
        Ok(newest_event(&events).and_then(|event| parse_profile_display(&event.content)))
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

fn operator_authored_filter(operator_pubkey: &str, since: Timestamp) -> Option<Filter> {
    let pubkey = PublicKey::parse(operator_pubkey).ok()?;
    Some(
        Filter::new()
            .kinds([
                Kind::Custom(CHANNEL_MESSAGE_KIND),
                Kind::Custom(STREAM_MESSAGE_V2_KIND),
            ])
            .author(pubkey)
            .since(since),
    )
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

fn place_metadata_filter(channel_id: &str) -> Filter {
    Filter::new()
        .kind(Kind::Custom(GROUP_METADATA_KIND))
        .identifier(channel_id)
        .limit(10)
}

fn profile_filter(pubkey: &str) -> Option<Filter> {
    let author = PublicKey::parse(pubkey).ok()?;
    Some(
        Filter::new()
            .kind(Kind::Custom(PROFILE_KIND))
            .author(author)
            .limit(10),
    )
}

fn event_tags(event: &Event) -> Vec<Vec<String>> {
    event
        .tags
        .iter()
        .map(|tag| tag.as_slice().to_vec())
        .collect()
}

fn newest_event(events: &[Event]) -> Option<&Event> {
    events.iter().max_by_key(|event| event.created_at.as_secs())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlaceMetadata {
    name: String,
    participants: Vec<String>,
}

fn parse_place_metadata(tags: &[Vec<String>]) -> PlaceMetadata {
    PlaceMetadata {
        name: tag_value(tags, "name").unwrap_or("").to_owned(),
        participants: tag_values(tags, "p")
            .filter(|value| is_pubkey(value))
            .map(str::to_ascii_lowercase)
            .collect(),
    }
}

fn wants_peer_display(channel_name: &str) -> bool {
    channel_name.trim().is_empty() || botserver_domain::is_generic_dm_title(channel_name)
}

fn other_participant<'a>(operator_pubkey: &str, participants: &'a [String]) -> Option<&'a str> {
    let mut other = None;
    for participant in participants {
        if participant.eq_ignore_ascii_case(operator_pubkey) {
            continue;
        }
        match other {
            None => other = Some(participant.as_str()),
            Some(existing) if existing.eq_ignore_ascii_case(participant) => {}
            Some(_) => return None,
        }
    }
    other
}

fn parse_profile_display(content: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(content).ok()?;
    for key in ["display_name", "displayName", "name"] {
        if let Some(name) = value
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            return Some(name.to_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use botserver_domain::BotId;
    use nostr_sdk::prelude::{EventBuilder, FinalizeEvent, Keys, Tag};

    use super::*;
    use crate::{NewTurn, ProgressPostRecord, SessionRecord, TurnRecord, TurnState};

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

        fn record_pending_progress(
            &mut self,
            _ask_id: &str,
            _body: &str,
        ) -> Result<bool, Self::Error> {
            unreachable!()
        }

        fn progress_post(&self, _ask_id: &str) -> Result<Option<ProgressPostRecord>, Self::Error> {
            unreachable!()
        }

        fn save_progress_post(
            &mut self,
            _progress: &ProgressPostRecord,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn clear_pending_progress(&mut self, _ask_id: &str) -> Result<(), Self::Error> {
            unreachable!()
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

        fn session_by_occupant_logical_id(
            &self,
            _occupant_logical_id: &str,
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
                    opened_at: Some(1),
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

        let mut ingest = RelayIngest::new(&operator, relay_pubkey(), FakeRepository::default())
            .with_inbound_trigger("pr:");
        let message = event_with_keys(
            &Keys::generate(),
            CHANNEL_MESSAGE_KIND,
            "[pr]: pr: loop",
            [tag(&["h", "channel"]), tag(&["p", &operator])],
        );
        assert_eq!(ingest.ingest(&message).unwrap(), None);
    }

    #[test]
    fn operator_authored_bot_colon_without_self_p_tag_emits() {
        let operator_keys = Keys::generate();
        let operator = operator_keys.public_key().to_hex();
        let peer = "b".repeat(64);
        let mut ingest = RelayIngest::new(&operator, relay_pubkey(), FakeRepository::default());
        let message = event_with_keys(
            &operator_keys,
            CHANNEL_MESSAGE_KIND,
            "bot: testing",
            [tag(&["h", "channel"]), tag(&["p", &peer])],
        );

        let action = ingest.ingest(&message).unwrap().expect("trigger");
        let IngestAction::TurnCandidate { trigger, .. } = action else {
            panic!("expected turn candidate");
        };
        assert_eq!(trigger.request(), "testing");
    }

    #[test]
    fn inbound_trigger_token_is_the_configured_bot_id() {
        let operator_keys = Keys::generate();
        let operator = operator_keys.public_key().to_hex();
        let peer = "b".repeat(64);
        let mut ingest = RelayIngest::new(&operator, relay_pubkey(), FakeRepository::default())
            .with_inbound_trigger("review:");
        let matched = event_with_keys(
            &operator_keys,
            CHANNEL_MESSAGE_KIND,
            "review: hello",
            [tag(&["h", "channel"]), tag(&["p", &peer])],
        );
        let missed = event_with_keys(
            &operator_keys,
            CHANNEL_MESSAGE_KIND,
            "bot: hello",
            [tag(&["h", "channel"]), tag(&["p", &peer])],
        );

        let action = ingest.ingest(&matched).unwrap().expect("trigger");
        let IngestAction::TurnCandidate { trigger, .. } = action else {
            panic!("expected turn candidate");
        };
        assert_eq!(trigger.request(), "hello");
        assert_eq!(ingest.ingest(&missed).unwrap(), None);
    }

    #[test]
    fn inbound_tokens_route_to_the_matching_bot() {
        let operator_keys = Keys::generate();
        let operator = operator_keys.public_key().to_hex();
        let peer = "b".repeat(64);
        let mut ingest = RelayIngest::new(&operator, relay_pubkey(), FakeRepository::default())
            .with_inbound_triggers([
                (BotId::new("bot").expect("bot"), "bot:".to_owned()),
                (BotId::new("pr").expect("pr"), "pr:".to_owned()),
            ]);
        let bot_message = event_with_keys(
            &operator_keys,
            CHANNEL_MESSAGE_KIND,
            "bot: hello",
            [tag(&["h", "channel"]), tag(&["p", &peer])],
        );
        let pr_message = event_with_keys(
            &operator_keys,
            CHANNEL_MESSAGE_KIND,
            "pr: review this",
            [tag(&["h", "channel"]), tag(&["p", &peer])],
        );

        let bot_action = ingest.ingest(&bot_message).unwrap().expect("bot trigger");
        let IngestAction::TurnCandidate {
            bot_id, trigger, ..
        } = bot_action
        else {
            panic!("expected turn candidate");
        };
        assert_eq!(bot_id.as_str(), "bot");
        assert_eq!(trigger.request(), "hello");

        let pr_action = ingest.ingest(&pr_message).unwrap().expect("pr trigger");
        let IngestAction::TurnCandidate {
            bot_id, trigger, ..
        } = pr_action
        else {
            panic!("expected turn candidate");
        };
        assert_eq!(bot_id.as_str(), "pr");
        assert_eq!(trigger.request(), "review this");
    }

    #[test]
    fn peer_authored_bot_colon_without_operator_p_tag_is_indexed_only() {
        let operator = "a".repeat(64);
        let mut ingest = RelayIngest::new(&operator, relay_pubkey(), FakeRepository::default());
        let message = event(
            CHANNEL_MESSAGE_KIND,
            "bot: testing",
            [tag(&["h", "channel"]), tag(&["p", &"b".repeat(64)])],
        );

        assert_eq!(ingest.ingest(&message).unwrap(), None);
        assert_eq!(ingest.repository.indexed.len(), 1);
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

        let authored_json =
            serde_json::to_value(operator_authored_filter(&operator, since).expect("filter"))
                .unwrap();
        assert_eq!(authored_json["kinds"], serde_json::json!([9, 40002]));
        assert_eq!(authored_json["authors"], serde_json::json!([operator]));
        assert_eq!(authored_json["since"], 42);
        assert!(authored_json.get("#p").is_none());

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

        let metadata_json = serde_json::to_value(place_metadata_filter(
            "ab12cd34-5678-90ab-cdef-0123456789ab",
        ))
        .unwrap();
        assert_eq!(metadata_json["kinds"], serde_json::json!([39_000]));
        assert_eq!(
            metadata_json["#d"],
            serde_json::json!(["ab12cd34-5678-90ab-cdef-0123456789ab"])
        );
        assert_eq!(metadata_json["limit"], 10);
    }

    #[test]
    fn place_metadata_uses_stream_name_and_dm_peer() {
        let operator = "a".repeat(64);
        let peer = "b".repeat(64);
        let stream = parse_place_metadata(&[
            vec![
                "d".to_owned(),
                "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
            ],
            vec!["name".to_owned(), "#eng".to_owned()],
            vec!["t".to_owned(), "stream".to_owned()],
        ]);
        assert_eq!(stream.name, "#eng");
        assert!(!wants_peer_display(&stream.name));
        assert_eq!(usable_place_display(&stream.name, None), "#eng");

        let dm = parse_place_metadata(&[
            vec![
                "d".to_owned(),
                "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
            ],
            vec!["name".to_owned(), "DM".to_owned()],
            vec!["hidden".to_owned()],
            vec!["t".to_owned(), "dm".to_owned()],
            vec!["p".to_owned(), operator.clone()],
            vec!["p".to_owned(), peer.clone()],
        ]);
        assert!(wants_peer_display(&dm.name));
        assert_eq!(
            other_participant(&operator, &dm.participants),
            Some(peer.as_str())
        );
        let duplicated = [peer.clone(), peer.clone()];
        assert_eq!(
            other_participant(&operator, &duplicated),
            Some(peer.as_str())
        );
        assert_eq!(
            usable_place_display(&dm.name, Some("Sebastian")),
            "Sebastian"
        );
        assert_eq!(
            parse_profile_display(r#"{"display_name":"Sebastian","name":"seb"}"#).as_deref(),
            Some("Sebastian")
        );
        assert_eq!(
            parse_profile_display(r#"{"name":"seb"}"#).as_deref(),
            Some("seb")
        );
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
