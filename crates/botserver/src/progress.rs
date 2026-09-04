//! Host side of D42: relay occupant progress as one edited stamped post.
//!
//! The delivery handler only records the row (before the ACK). The relay
//! work runs on the host refresh tick: one kind-9 create after the
//! initial hold, then coalesced kind-40003 edits under the interval and
//! cap. A cancel deletes the post (kind 9005). Every relay step is
//! best-effort and never fails the turn.

use std::fmt;
use std::sync::Arc;

use botserver_domain::buzz;
use botserver_domain::progress::{
    cap_progress_body, next_progress_step, ProgressClock, ProgressStep, PROGRESS_EDIT_CAP,
};
use botserver_domain::{outbound_prefix_for, stamp_outbound, BotId, EventId, TurnState};

use crate::outbox::{OutboundAttempt, OutboundPublisher, PublishError};
use crate::{HostRepository, IndexedRelayEvent, TurnRecord};

/// Durable progress state for one ask (D42 Durability).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressPost {
    pub ask_id: String,
    pub channel_id: String,
    /// The triggering `EventId`: the post replies to it like the final.
    pub reply_to_event_id: EventId,
    pub thread_root_event_id: Option<EventId>,
    /// Hold start: the turn's open time, or the first progress delivery
    /// for an open turn recorded before `turns.opened_at` existed.
    pub opened_at: i64,
    /// Newest unsent body, trimmed and capped, unstamped.
    pub pending_body: Option<String>,
    /// Stamped body the create was signed with, so a redelivery
    /// re-prepares the identical event (D43).
    pub post_body: Option<String>,
    /// Create event id recorded before send (D28, amended by D43).
    pub prepared_event_id: Option<String>,
    pub prepared_created_at: Option<i64>,
    /// The create send was invoked at least once.
    pub dispatched: bool,
    /// Create accepted by the relay; edits and the delete target this id.
    pub post_event_id: Option<String>,
    pub edit_count: u32,
    /// Last accepted send (create or edit).
    pub last_send_at: Option<i64>,
    /// No further relay for this ask: cancelled, lost create, or a
    /// non-retryable create failure.
    pub ended: bool,
    /// The one operator notice for the edit cap was already emitted.
    pub cap_noticed: bool,
}

impl ProgressPost {
    fn clock(&self) -> ProgressClock {
        ProgressClock {
            opened_at: self.opened_at,
            last_send_at: self.last_send_at,
            edit_count: self.edit_count,
            post_exists: self.post_event_id.is_some(),
            pending_body: self.pending_body.is_some(),
        }
    }

    /// The relay event the delete must target, if a create may have landed.
    fn delete_target(&self) -> Option<EventId> {
        let id = self.post_event_id.as_deref().or_else(|| {
            self.dispatched
                .then_some(self.prepared_event_id.as_deref()?)
        })?;
        EventId::parse_hex(id)
    }

    fn create_attempt(&self, stamped_body: &str) -> OutboundAttempt {
        OutboundAttempt {
            ask_id: format!("progress:{}", self.ask_id),
            body: stamped_body.to_owned(),
            channel_id: self.channel_id.clone(),
            reply_to_event_id: Some(self.reply_to_event_id.clone()),
            thread_root_event_id: self.thread_root_event_id.clone(),
            // D42: the progress post carries no mention.
            mention: String::new(),
            outbound_event_id: self.post_event_id.clone(),
            prepared_event_id: self.prepared_event_id.clone(),
            prepared_created_at: self.prepared_created_at,
            dispatched: self.dispatched,
        }
    }
}

/// Best-effort kind-40003 edit and kind-9005 delete of a progress post.
pub trait ProgressRelay: Send + Sync {
    /// Replace the post content in place.
    ///
    /// # Errors
    ///
    /// Returns an error when the relay did not accept the edit.
    fn edit(
        &self,
        channel_id: &str,
        post_event_id: &EventId,
        content: &str,
    ) -> Result<(), PublishError>;

    /// Buzz-delete the post.
    ///
    /// # Errors
    ///
    /// Returns an error when the relay did not accept the delete.
    fn delete(&self, channel_id: &str, post_event_id: &EventId) -> Result<(), PublishError>;
}

/// Ignore progress edits and deletes.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopProgressRelay;

impl ProgressRelay for NoopProgressRelay {
    fn edit(
        &self,
        _channel_id: &str,
        _post_event_id: &EventId,
        _content: &str,
    ) -> Result<(), PublishError> {
        Ok(())
    }

    fn delete(&self, _channel_id: &str, _post_event_id: &EventId) -> Result<(), PublishError> {
        Ok(())
    }
}

impl ProgressRelay for Arc<dyn ProgressRelay> {
    fn edit(
        &self,
        channel_id: &str,
        post_event_id: &EventId,
        content: &str,
    ) -> Result<(), PublishError> {
        (**self).edit(channel_id, post_event_id, content)
    }

    fn delete(&self, channel_id: &str, post_event_id: &EventId) -> Result<(), PublishError> {
        (**self).delete(channel_id, post_event_id)
    }
}

/// Record one occupant `--progress` body for an ask (D33, D42).
///
/// Never relays: the row is persisted so the ACK can follow, and the
/// refresh tick does the send. Progress on a turn that is not open, on
/// a dispatched final attempt, with an empty body, or past the edit cap
/// records nothing and still ACKs.
///
/// # Errors
///
/// Returns an adapter error when the row cannot be read or persisted.
pub fn record_progress<R: HostRepository>(
    repository: &mut R,
    notice: &mut impl FnMut(&str),
    turn: &TurnRecord,
    body: &str,
    now: i64,
) -> Result<(), R::Error> {
    let Some(ask_id) = turn.ask_id.as_deref() else {
        return Ok(());
    };
    if turn.state != TurnState::Open {
        return Ok(());
    }
    if final_in_flight(repository, ask_id)? {
        return Ok(());
    }
    let Some(capped) = cap_progress_body(body) else {
        return Ok(());
    };
    let mut post = match repository.progress_post(ask_id)? {
        Some(post) => post,
        None => new_progress_post(repository, turn, ask_id, now)?,
    };
    if post.ended {
        return Ok(());
    }
    if post.post_event_id.is_some() && post.edit_count >= PROGRESS_EDIT_CAP {
        if !post.cap_noticed {
            notice(&format!(
                "progress for ask {ask_id} reached the {PROGRESS_EDIT_CAP}-edit cap; later bodies are dropped"
            ));
            post.cap_noticed = true;
            repository.save_progress_post(&post)?;
        }
        return Ok(());
    }
    post.pending_body = Some(capped);
    repository.save_progress_post(&post)
}

/// Whether a final for this ask has reached the host (D42: "whose outbound
/// attempt is already dispatched").
///
/// The attempt row is saved first thing on a final and never deleted, so
/// its presence is the durable signal. The row's `dispatched` flag is not:
/// a retryable publish failure clears it while the turn stays open and the
/// final is redelivered on the tick.
fn final_in_flight<R: HostRepository>(repository: &R, ask_id: &str) -> Result<bool, R::Error> {
    Ok(repository.outbound_attempt(ask_id)?.is_some())
}

fn new_progress_post<R: HostRepository>(
    repository: &R,
    turn: &TurnRecord,
    ask_id: &str,
    now: i64,
) -> Result<ProgressPost, R::Error> {
    let thread_root_event_id =
        repository
            .indexed_event(&turn.event_id)?
            .and_then(|event: IndexedRelayEvent| {
                let tags =
                    serde_json::from_str::<Vec<Vec<String>>>(&event.tags_json).unwrap_or_default();
                buzz::reply_thread_root(&turn.event_id, &tags)
            });
    Ok(ProgressPost {
        ask_id: ask_id.to_owned(),
        channel_id: turn.channel_id.clone(),
        reply_to_event_id: turn.event_id.clone(),
        thread_root_event_id,
        opened_at: turn.opened_at.unwrap_or(now),
        pending_body: None,
        post_body: None,
        prepared_event_id: None,
        prepared_created_at: None,
        dispatched: false,
        post_event_id: None,
        edit_count: 0,
        last_send_at: None,
        ended: false,
        cap_noticed: false,
    })
}

/// Drop the pending body once a final has landed (D42: a final that
/// arrives first discards it). The post itself stays up.
///
/// # Errors
///
/// Returns an adapter error when the row cannot be read or persisted.
pub fn discard_pending<R: HostRepository>(
    repository: &mut R,
    ask_id: &str,
) -> Result<(), R::Error> {
    let Some(mut post) = repository.progress_post(ask_id)? else {
        return Ok(());
    };
    if post.pending_body.is_none() {
        return Ok(());
    }
    post.pending_body = None;
    repository.save_progress_post(&post)
}

/// End progress for a cancelled ask and Buzz-delete its post (D14, D15).
///
/// The row is marked ended before the delete so the tick never sends
/// again; the delete itself is best-effort and only reported.
///
/// # Errors
///
/// Returns an adapter error when the row cannot be read or persisted.
pub fn delete_progress_post<R: HostRepository>(
    repository: &mut R,
    relay: &impl ProgressRelay,
    notice: &mut impl FnMut(&str),
    ask_id: &str,
) -> Result<(), R::Error> {
    let Some(mut post) = repository.progress_post(ask_id)? else {
        return Ok(());
    };
    if post.ended {
        return Ok(());
    }
    post.ended = true;
    post.pending_body = None;
    repository.save_progress_post(&post)?;
    if let Some(target) = post.delete_target() {
        if let Err(error) = relay.delete(&post.channel_id, &target) {
            notice(&format!(
                "progress post delete for ask {ask_id} failed: {error}"
            ));
        }
    }
    Ok(())
}

/// Run the D42 clock once for one ask on the refresh tick.
///
/// At most one relay action per call: finish a dispatched create, create
/// after the hold, or edit after the interval. Failures are notices;
/// `retryable` is not consulted for edits.
///
/// # Errors
///
/// Returns an adapter error when the row cannot be read or persisted.
pub fn flush_progress<R, P, L>(
    repository: &mut R,
    publisher: &P,
    relay: &L,
    notice: &mut impl FnMut(&str),
    turn: &TurnRecord,
    mut post: ProgressPost,
    now: i64,
) -> Result<(), R::Error>
where
    R: HostRepository,
    P: OutboundPublisher,
    P::Error: fmt::Display,
    L: ProgressRelay,
{
    if post.ended {
        return Ok(());
    }
    let closed = turn.state != TurnState::Open;
    if closed || final_in_flight(repository, &post.ask_id)? {
        // The final landed or is landing (D42: no relay once the final's
        // attempt exists): keep the post, drop what was pending. Once the
        // turn has left Open, a create that never stored an accepted id ends
        // here so the tick stops scanning it; its prepared id still excludes
        // it from snapshots. While the turn is still Open the row stays live
        // so a cancel during the final's retry window can still delete it.
        let mut changed = post.pending_body.take().is_some();
        if closed && post.dispatched && post.post_event_id.is_none() {
            post.ended = true;
            changed = true;
        }
        if changed {
            repository.save_progress_post(&post)?;
        }
        return Ok(());
    }
    if post.dispatched && post.post_event_id.is_none() {
        return finish_dispatched_create(repository, publisher, notice, post, now);
    }
    match next_progress_step(&post.clock(), now) {
        ProgressStep::Wait => Ok(()),
        ProgressStep::Create => create_post(repository, publisher, notice, &turn.bot_id, post, now),
        ProgressStep::Edit => edit_post(repository, relay, notice, &turn.bot_id, post, now),
        ProgressStep::Capped => {
            post.pending_body = None;
            if !post.cap_noticed {
                notice(&format!(
                    "progress for ask {} reached the {PROGRESS_EDIT_CAP}-edit cap; later bodies are dropped",
                    post.ask_id
                ));
                post.cap_noticed = true;
            }
            repository.save_progress_post(&post)
        }
    }
}

fn create_post<R, P>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    bot_id: &BotId,
    mut post: ProgressPost,
    now: i64,
) -> Result<(), R::Error>
where
    R: HostRepository,
    P: OutboundPublisher,
    P::Error: fmt::Display,
{
    let Some(body) = post.pending_body.take() else {
        return Ok(());
    };
    let stamped = stamp_outbound(&body, &outbound_prefix_for(bot_id));
    let attempt = post.create_attempt(&stamped);
    let prepared = match publisher.prepare(&attempt) {
        Ok(prepared) => prepared,
        Err(error) => {
            notice(&format!(
                "progress post for ask {} could not be built; progress ended: {error}",
                post.ask_id
            ));
            post.ended = true;
            return repository.save_progress_post(&post);
        }
    };
    post.post_body = Some(stamped);
    post.prepared_event_id = Some(prepared.event_id().to_owned());
    post.prepared_created_at = Some(prepared.created_at());
    post.dispatched = true;
    // Record before send (D28): a crash here redelivers the same id.
    repository.save_progress_post(&post)?;
    send_create(repository, publisher, notice, post, &prepared, now)
}

fn finish_dispatched_create<R, P>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    mut post: ProgressPost,
    now: i64,
) -> Result<(), R::Error>
where
    R: HostRepository,
    P: OutboundPublisher,
    P::Error: fmt::Display,
{
    let Some(body) = post.post_body.clone() else {
        // Dispatched before a prepared id was recorded: a post may exist
        // under an id the host never stored. Never create a second one.
        notice(&format!(
            "progress post for ask {} was dispatched without a stored id; progress ended",
            post.ask_id
        ));
        post.ended = true;
        post.pending_body = None;
        return repository.save_progress_post(&post);
    };
    if post.prepared_event_id.is_none() {
        notice(&format!(
            "progress post for ask {} was dispatched without a stored id; progress ended",
            post.ask_id
        ));
        post.ended = true;
        post.pending_body = None;
        return repository.save_progress_post(&post);
    }
    let attempt = post.create_attempt(&body);
    let prepared = match publisher.prepare(&attempt) {
        Ok(prepared) => prepared,
        Err(error) => {
            notice(&format!(
                "progress post for ask {} could not be rebuilt; progress ended: {error}",
                post.ask_id
            ));
            post.ended = true;
            post.pending_body = None;
            return repository.save_progress_post(&post);
        }
    };
    send_create(repository, publisher, notice, post, &prepared, now)
}

fn send_create<R, P>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    mut post: ProgressPost,
    prepared: &crate::outbox::PreparedOutbound,
    now: i64,
) -> Result<(), R::Error>
where
    R: HostRepository,
    P: OutboundPublisher,
    P::Error: fmt::Display,
{
    match publisher.publish(prepared) {
        Ok(event_id) => {
            post.post_event_id = Some(event_id);
            post.last_send_at = Some(now);
            repository.save_progress_post(&post)
        }
        Err(error) if P::retryable(&error) => {
            // The prepared id stays recorded; the next tick redelivers it.
            notice(&format!(
                "progress post for ask {} was not accepted; retrying: {error}",
                post.ask_id
            ));
            Ok(())
        }
        Err(error) => {
            notice(&format!(
                "progress post for ask {} was rejected; progress ended: {error}",
                post.ask_id
            ));
            post.ended = true;
            post.pending_body = None;
            repository.save_progress_post(&post)
        }
    }
}

fn edit_post<R, L>(
    repository: &mut R,
    relay: &L,
    notice: &mut impl FnMut(&str),
    bot_id: &BotId,
    mut post: ProgressPost,
    now: i64,
) -> Result<(), R::Error>
where
    R: HostRepository,
    L: ProgressRelay,
{
    let (Some(body), Some(target)) = (post.pending_body.take(), post.post_event_id.as_deref())
    else {
        return Ok(());
    };
    let Some(target) = EventId::parse_hex(target) else {
        notice(&format!(
            "progress post id for ask {} is not an event id; progress ended",
            post.ask_id
        ));
        post.ended = true;
        return repository.save_progress_post(&post);
    };
    let stamped = stamp_outbound(&body, &outbound_prefix_for(bot_id));
    post.edit_count += 1;
    post.last_send_at = Some(now);
    // Record before edit (D28). A lost edit is superseded by the next body.
    repository.save_progress_post(&post)?;
    if let Err(error) = relay.edit(&post.channel_id, &target, &stamped) {
        notice(&format!(
            "progress edit for ask {} failed; the next body supersedes it: {error}",
            post.ask_id
        ));
    }
    Ok(())
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct RecordingProgressRelay {
    pub edits: std::sync::Mutex<Vec<(String, String, String)>>,
    pub deletes: std::sync::Mutex<Vec<(String, String)>>,
    pub fail_edit: std::sync::Mutex<bool>,
}

#[cfg(test)]
impl ProgressRelay for RecordingProgressRelay {
    fn edit(
        &self,
        channel_id: &str,
        post_event_id: &EventId,
        content: &str,
    ) -> Result<(), PublishError> {
        self.edits.lock().expect("edits").push((
            channel_id.to_owned(),
            post_event_id.as_str().to_owned(),
            content.to_owned(),
        ));
        if *self.fail_edit.lock().expect("fail") {
            return Err(PublishError::NotAccepted {
                detail: "edit dropped".to_owned(),
            });
        }
        Ok(())
    }

    fn delete(&self, channel_id: &str, post_event_id: &EventId) -> Result<(), PublishError> {
        self.deletes
            .lock()
            .expect("deletes")
            .push((channel_id.to_owned(), post_event_id.as_str().to_owned()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use botserver_domain::progress::{
        PROGRESS_BODY_MAX_BYTES, PROGRESS_EDIT_INTERVAL_SECS, PROGRESS_INITIAL_HOLD_SECS,
    };
    use nostr_sdk::prelude::FinalizeEvent;
    use rusqlite::Connection;

    use super::*;
    use crate::outbox::PreparedOutbound;
    use crate::sqlite::SqliteRepository;
    use crate::{NewTurn, SessionRecord};

    const CHANNEL: &str = "ab12cd34-5678-90ab-cdef-0123456789ab";

    #[derive(Debug, Default)]
    struct FakePublisher {
        prepared: Mutex<Vec<OutboundAttempt>>,
        sends: Mutex<Vec<String>>,
        fail: Mutex<Option<PublishError>>,
    }

    fn fake_event_id(attempt: &OutboundAttempt, created_at: i64) -> String {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        attempt.body.hash(&mut hasher);
        attempt.channel_id.hash(&mut hasher);
        attempt.reply_to_event_id.hash(&mut hasher);
        attempt.mention.hash(&mut hasher);
        created_at.hash(&mut hasher);
        format!("{:064x}", hasher.finish())
    }

    impl OutboundPublisher for FakePublisher {
        type Error = PublishError;

        fn prepare(&self, attempt: &OutboundAttempt) -> Result<PreparedOutbound, Self::Error> {
            let created_at = attempt.prepared_created_at.unwrap_or(1_700_000_000);
            let event_id = attempt
                .prepared_event_id
                .clone()
                .unwrap_or_else(|| fake_event_id(attempt, created_at));
            self.prepared
                .lock()
                .expect("prepared")
                .push(attempt.clone());
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
            if let Some(error) = self.fail.lock().expect("fail").take() {
                return Err(error);
            }
            Ok(prepared.event_id().to_owned())
        }

        fn retryable(error: &Self::Error) -> bool {
            error.is_retryable()
        }
    }

    fn event_id(character: char) -> EventId {
        EventId::parse_hex(&character.to_string().repeat(64)).expect("event")
    }

    fn open_repo() -> SqliteRepository {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new("bot").expect("bot");
        repository
            .save_session(&SessionRecord {
                bot_id: bot_id.clone(),
                channel_id: CHANNEL.to_owned(),
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
                channel_id: CHANNEL.to_owned(),
                event_id: event_id('a'),
                reply_to_event_id: None,
            })
            .unwrap();
        repository
            .open_next_turn(&bot_id, CHANNEL, "ask-1")
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
                    channel_id: Some(CHANNEL.to_owned()),
                    target_event_id: None,
                },
                false,
            )
            .unwrap();
        repository
    }

    /// Fix the open time so the hold is deterministic in tests.
    fn open_turn_at(repository: &mut SqliteRepository, opened_at: i64) -> TurnRecord {
        let mut turn = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        let mut post = new_progress_post(repository, &turn, "ask-1", opened_at).unwrap();
        post.opened_at = opened_at;
        repository.save_progress_post(&post).unwrap();
        turn.opened_at = Some(opened_at);
        turn
    }

    fn quiet() -> impl FnMut(&str) {
        |_| {}
    }

    fn flush_all(
        repository: &mut SqliteRepository,
        publisher: &FakePublisher,
        relay: &RecordingProgressRelay,
        notices: &mut Vec<String>,
        turn: &TurnRecord,
        now: i64,
    ) {
        for post in repository.progress_posts_pending_flush().unwrap() {
            flush_progress(
                repository,
                publisher,
                relay,
                &mut |text: &str| notices.push(text.to_owned()),
                turn,
                post,
                now,
            )
            .unwrap();
        }
    }

    #[test]
    fn progress_before_hold_is_recorded_not_posted() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "  working  ", 1_005).unwrap();
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert_eq!(post.pending_body.as_deref(), Some("working"));
        assert_eq!(post.reply_to_event_id, event_id('a'));
        assert!(!post.dispatched);
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            1_000 + PROGRESS_INITIAL_HOLD_SECS - 1,
        );
        assert!(publisher.sends.lock().unwrap().is_empty());
        assert!(notices.is_empty());
    }

    #[test]
    fn progress_posts_once_after_hold_without_mention() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        let now = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            now,
        );
        let prepared = publisher.prepared.lock().unwrap();
        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].body, "[bot]: working");
        assert_eq!(prepared[0].reply_to_event_id, Some(event_id('a')));
        assert_eq!(prepared[0].channel_id, CHANNEL);
        assert!(prepared[0].mention.is_empty(), "no --mention on progress");
        drop(prepared);
        let sends = publisher.sends.lock().unwrap().clone();
        assert_eq!(sends.len(), 1);
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert_eq!(post.post_event_id.as_deref(), Some(sends[0].as_str()));
        assert_eq!(post.prepared_event_id.as_deref(), Some(sends[0].as_str()));
        assert_eq!(post.post_body.as_deref(), Some("[bot]: working"));
        assert_eq!(post.last_send_at, Some(now));
        assert_eq!(post.edit_count, 0);
        assert!(post.pending_body.is_none());
        // The tick is idle once the body is sent.
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            now + 1,
        );
        assert_eq!(publisher.sends.lock().unwrap().len(), 1);
        assert!(relay.edits.lock().unwrap().is_empty());
        assert!(notices.is_empty());
    }

    #[test]
    fn later_bodies_coalesce_into_one_edit_after_the_interval() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "step 1", 1_005).unwrap();
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        let post_id = repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .post_event_id
            .unwrap();

        record_progress(&mut repository, &mut quiet(), &turn, "step 2", created + 2).unwrap();
        record_progress(&mut repository, &mut quiet(), &turn, "step 3", created + 4).unwrap();
        // Inside the interval: the newest body waits, nothing is sent.
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created + PROGRESS_EDIT_INTERVAL_SECS - 1,
        );
        assert!(relay.edits.lock().unwrap().is_empty());
        assert_eq!(
            repository
                .progress_post("ask-1")
                .unwrap()
                .unwrap()
                .pending_body
                .as_deref(),
            Some("step 3")
        );

        let edited = created + PROGRESS_EDIT_INTERVAL_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            edited,
        );
        let edits = relay.edits.lock().unwrap().clone();
        assert_eq!(
            edits,
            vec![(
                CHANNEL.to_owned(),
                post_id.clone(),
                "[bot]: step 3".to_owned()
            )]
        );
        assert_eq!(publisher.sends.lock().unwrap().len(), 1, "no second post");
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert_eq!(post.edit_count, 1);
        assert_eq!(post.last_send_at, Some(edited));
        assert!(post.pending_body.is_none());
        assert!(notices.is_empty());
    }

    #[test]
    fn edit_cap_drops_later_bodies_with_one_notice() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "start", 1_005).unwrap();
        let mut now = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            now,
        );
        for index in 0..PROGRESS_EDIT_CAP {
            let mut recorded = Vec::new();
            record_progress(
                &mut repository,
                &mut |text: &str| recorded.push(text.to_owned()),
                &turn,
                &format!("edit {index}"),
                now + 1,
            )
            .unwrap();
            assert!(recorded.is_empty());
            now += PROGRESS_EDIT_INTERVAL_SECS;
            flush_all(
                &mut repository,
                &publisher,
                &relay,
                &mut notices,
                &turn,
                now,
            );
        }
        let cap = usize::try_from(PROGRESS_EDIT_CAP).expect("cap");
        assert_eq!(relay.edits.lock().unwrap().len(), cap, "exactly the cap");
        let mut recorded = Vec::new();
        for _ in 0..3 {
            record_progress(
                &mut repository,
                &mut |text: &str| recorded.push(text.to_owned()),
                &turn,
                "past the cap",
                now + 1,
            )
            .unwrap();
        }
        assert_eq!(recorded.len(), 1, "one operator notice");
        assert!(recorded[0].contains("edit cap"));
        now += PROGRESS_EDIT_INTERVAL_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            now,
        );
        assert_eq!(relay.edits.lock().unwrap().len(), cap);
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.pending_body.is_none());
        assert!(post.cap_noticed);
        assert!(notices.is_empty());
    }

    #[test]
    fn progress_body_is_trimmed_and_capped_at_1024_bytes() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let long = format!("  {}  ", "z".repeat(PROGRESS_BODY_MAX_BYTES * 2));
        record_progress(&mut repository, &mut quiet(), &turn, &long, 1_005).unwrap();
        let pending = repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .pending_body
            .unwrap();
        assert_eq!(pending.len(), PROGRESS_BODY_MAX_BYTES);
        assert!(pending.ends_with('…'));
        assert!(!pending.starts_with(' '));
    }

    #[test]
    fn empty_progress_body_records_nothing() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        record_progress(&mut repository, &mut quiet(), &turn, " \n ", 1_005).unwrap();
        assert!(repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .pending_body
            .is_none());
    }

    #[test]
    fn final_first_discards_the_pending_body_and_never_posts() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "almost", 1_005).unwrap();
        discard_pending(&mut repository, "ask-1").unwrap();
        repository
            .set_turn_state("ask-1", TurnState::Posted)
            .unwrap();
        let posted = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &posted,
            1_000_000,
        );
        assert!(publisher.sends.lock().unwrap().is_empty());
        assert!(repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .pending_body
            .is_none());
    }

    #[test]
    fn final_leaves_the_progress_post_up_and_drops_a_pending_edit() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        record_progress(&mut repository, &mut quiet(), &turn, "nearly", created + 1).unwrap();
        repository
            .set_turn_state("ask-1", TurnState::Posted)
            .unwrap();
        let posted = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &posted,
            created + PROGRESS_EDIT_INTERVAL_SECS,
        );
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.post_event_id.is_some(), "post stays up");
        assert!(post.pending_body.is_none());
        assert!(relay.edits.lock().unwrap().is_empty());
        assert!(relay.deletes.lock().unwrap().is_empty());
    }

    #[test]
    fn cancel_ends_progress_and_buzz_deletes_the_post() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        let post_id = repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .post_event_id
            .unwrap();
        record_progress(&mut repository, &mut quiet(), &turn, "later", created + 1).unwrap();
        delete_progress_post(&mut repository, &relay, &mut quiet(), "ask-1").unwrap();
        assert_eq!(
            relay.deletes.lock().unwrap().clone(),
            vec![(CHANNEL.to_owned(), post_id)]
        );
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.ended);
        assert!(post.pending_body.is_none());
        // Ended rows never flush again, even with an open turn.
        assert!(repository
            .progress_posts_pending_flush()
            .unwrap()
            .is_empty());
        record_progress(&mut repository, &mut quiet(), &turn, "ignored", created + 2).unwrap();
        assert!(repository
            .progress_posts_pending_flush()
            .unwrap()
            .is_empty());
        // A second cancel does not delete twice.
        delete_progress_post(&mut repository, &relay, &mut quiet(), "ask-1").unwrap();
        assert_eq!(relay.deletes.lock().unwrap().len(), 1);
    }

    #[test]
    fn cancel_before_any_post_deletes_nothing() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let relay = RecordingProgressRelay::default();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        delete_progress_post(&mut repository, &relay, &mut quiet(), "ask-1").unwrap();
        assert!(relay.deletes.lock().unwrap().is_empty());
        assert!(repository.progress_post("ask-1").unwrap().unwrap().ended);
    }

    #[test]
    fn crash_after_create_dispatch_redelivers_the_same_id() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        *publisher.fail.lock().unwrap() = Some(PublishError::NotAccepted {
            detail: "connection dropped before OK".to_owned(),
        });
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.dispatched);
        assert!(post.post_event_id.is_none());
        let prepared_id = post
            .prepared_event_id
            .clone()
            .expect("recorded before send");
        assert!(post.pending_body.is_none(), "body moved to post_body");
        assert_eq!(post.post_body.as_deref(), Some("[bot]: working"));
        assert_eq!(notices.len(), 1);
        assert!(notices[0].contains("retrying"));
        // The dispatched-without-accept row is redelivered next tick.
        assert_eq!(repository.progress_posts_pending_flush().unwrap().len(), 1);
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created + 1,
        );
        let sends = publisher.sends.lock().unwrap().clone();
        assert_eq!(sends, vec![prepared_id.clone(), prepared_id.clone()]);
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert_eq!(post.post_event_id.as_deref(), Some(prepared_id.as_str()));
        assert_eq!(post.last_send_at, Some(created + 1));
        assert!(!post.ended);
    }

    #[test]
    fn legacy_dispatch_without_prepared_id_ends_progress_with_one_notice() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut post = repository.progress_post("ask-1").unwrap().unwrap();
        post.dispatched = true;
        post.pending_body = Some("next".to_owned());
        repository.save_progress_post(&post).unwrap();
        let mut notices = Vec::new();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            1_000_000,
        );
        assert!(publisher.sends.lock().unwrap().is_empty());
        assert_eq!(notices.len(), 1);
        assert!(notices[0].contains("without a stored id"));
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.ended);
        assert!(post.pending_body.is_none());
        assert!(repository
            .progress_posts_pending_flush()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn rejected_create_ends_progress_without_a_retry() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        *publisher.fail.lock().unwrap() = Some(PublishError::Rejected {
            detail: "invalid: not a member".to_owned(),
        });
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created + 1,
        );
        assert_eq!(publisher.sends.lock().unwrap().len(), 1);
        assert!(repository.progress_post("ask-1").unwrap().unwrap().ended);
        assert_eq!(notices.len(), 1);
    }

    #[test]
    fn failed_edit_is_superseded_by_the_next_body() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "one", 1_005).unwrap();
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        record_progress(&mut repository, &mut quiet(), &turn, "two", created + 1).unwrap();
        *relay.fail_edit.lock().unwrap() = true;
        let first_edit = created + PROGRESS_EDIT_INTERVAL_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            first_edit,
        );
        assert_eq!(notices.len(), 1);
        assert!(notices[0].contains("supersedes"));
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert_eq!(post.edit_count, 1, "recorded before the edit");
        assert!(post.pending_body.is_none());
        *relay.fail_edit.lock().unwrap() = false;
        record_progress(
            &mut repository,
            &mut quiet(),
            &turn,
            "three",
            first_edit + 1,
        )
        .unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            first_edit + PROGRESS_EDIT_INTERVAL_SECS,
        );
        let edits = relay.edits.lock().unwrap();
        assert_eq!(edits.len(), 2);
        assert_eq!(edits[1].2, "[bot]: three");
    }

    /// The stored attempt after a retryable final publish failure: the
    /// turn stays open, `record_and_send` cleared `dispatched`, and
    /// `retry_outbound` redelivers the prepared event on the tick (D28).
    fn final_attempt_in_retry_window() -> OutboundAttempt {
        OutboundAttempt {
            ask_id: "ask-1".to_owned(),
            body: "[bot]: done".to_owned(),
            channel_id: CHANNEL.to_owned(),
            reply_to_event_id: Some(event_id('a')),
            thread_root_event_id: None,
            mention: "c".repeat(64),
            outbound_event_id: None,
            prepared_event_id: Some("e".repeat(64)),
            prepared_created_at: Some(1_700_000_000),
            dispatched: false,
        }
    }

    #[test]
    fn flush_skips_relay_once_a_final_is_in_flight() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "pending", 1_005).unwrap();
        repository
            .save_outbound_attempt(&final_attempt_in_retry_window())
            .unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            1_000 + PROGRESS_INITIAL_HOLD_SECS,
        );
        assert!(publisher.sends.lock().unwrap().is_empty());
        assert!(relay.edits.lock().unwrap().is_empty());
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.pending_body.is_none(), "the pending body is discarded");
        assert!(post.post_event_id.is_none());
        assert!(!post.ended, "the turn is still open");
        assert!(notices.is_empty());
    }

    #[test]
    fn cancel_during_the_final_retry_window_still_deletes_an_unaccepted_create() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        *publisher.fail.lock().unwrap() = Some(PublishError::NotAccepted {
            detail: "dropped".to_owned(),
        });
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        let prepared = repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .prepared_event_id
            .unwrap();
        // A final arrives and its publish also fails transiently; the turn
        // is still open, so a trigger delete can still cancel it (D28).
        repository
            .save_outbound_attempt(&final_attempt_in_retry_window())
            .unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created + 1,
        );
        assert_eq!(
            publisher.sends.lock().unwrap().len(),
            1,
            "no redelivery once a final is in flight"
        );
        assert!(!repository.progress_post("ask-1").unwrap().unwrap().ended);
        delete_progress_post(&mut repository, &relay, &mut quiet(), "ask-1").unwrap();
        assert_eq!(
            relay.deletes.lock().unwrap().clone(),
            vec![(CHANNEL.to_owned(), prepared)]
        );
    }

    #[test]
    fn unaccepted_create_on_a_closed_turn_is_ended_and_leaves_the_scan() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        *publisher.fail.lock().unwrap() = Some(PublishError::NotAccepted {
            detail: "dropped".to_owned(),
        });
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.dispatched && post.post_event_id.is_none());
        let prepared = post.prepared_event_id.clone().unwrap();
        repository
            .set_turn_state("ask-1", TurnState::Posted)
            .unwrap();
        let posted = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        assert_eq!(repository.progress_posts_pending_flush().unwrap().len(), 1);
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &posted,
            created + 1,
        );
        assert_eq!(publisher.sends.lock().unwrap().len(), 1, "no redelivery");
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.ended);
        assert!(repository
            .progress_posts_pending_flush()
            .unwrap()
            .is_empty());
        // The dispatched create may have landed: it stays excluded.
        assert_eq!(
            repository.progress_post_event_ids(CHANNEL).unwrap(),
            vec![EventId::parse_hex(&prepared).unwrap()]
        );
    }

    #[test]
    fn progress_once_a_final_is_in_flight_records_nothing() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        repository
            .save_outbound_attempt(&final_attempt_in_retry_window())
            .unwrap();
        record_progress(&mut repository, &mut quiet(), &turn, "late", 1_005).unwrap();
        assert!(repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .pending_body
            .is_none());
    }

    #[test]
    fn progress_on_a_non_open_turn_records_nothing() {
        let mut repository = open_repo();
        repository
            .set_turn_state("ask-1", TurnState::Cancelled)
            .unwrap();
        let turn = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        record_progress(&mut repository, &mut quiet(), &turn, "late", 1_005).unwrap();
        assert!(repository.progress_post("ask-1").unwrap().is_none());
    }

    #[test]
    fn hold_counts_from_the_turn_open_time_not_the_first_body() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        // First body arrives after the hold already elapsed: post at once.
        let now = 1_000 + PROGRESS_INITIAL_HOLD_SECS + 5;
        record_progress(&mut repository, &mut quiet(), &turn, "late start", now).unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            now,
        );
        assert_eq!(publisher.sends.lock().unwrap().len(), 1);
    }

    #[test]
    fn open_next_turn_records_the_open_time() {
        let repository = open_repo();
        let turn = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        let opened_at = turn.opened_at.expect("opened_at set on open");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(i64::try_from(now).unwrap() - opened_at < 60);
    }

    #[test]
    fn progress_post_event_ids_cover_accepted_and_dispatched_creates() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        assert!(repository
            .progress_post_event_ids(CHANNEL)
            .unwrap()
            .is_empty());
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        *publisher.fail.lock().unwrap() = Some(PublishError::NotAccepted {
            detail: "dropped".to_owned(),
        });
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        let prepared = post.prepared_event_id.clone().unwrap();
        assert_eq!(
            repository.progress_post_event_ids(CHANNEL).unwrap(),
            vec![EventId::parse_hex(&prepared).unwrap()],
            "a dispatched create may have landed"
        );
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created + 1,
        );
        assert_eq!(
            repository.progress_post_event_ids(CHANNEL).unwrap(),
            vec![EventId::parse_hex(&prepared).unwrap()]
        );
        assert!(repository
            .progress_post_event_ids("ffffffff-ffff-ffff-ffff-ffffffffffff")
            .unwrap()
            .is_empty());
    }
}
