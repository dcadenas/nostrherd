use std::sync::Mutex;

use nostr_sdk::prelude::FinalizeEvent;
use nostrherd_domain::{BotId, EventId};
use rusqlite::Connection;

use crate::outbox::{OutboundAttempt, OutboundPublisher, PreparedOutbound, PublishError};
use crate::sqlite::SqliteRepository;
use crate::{HostRepository, IndexedRelayEvent, NewTurn, SessionRecord};

pub(crate) const CHANNEL: &str = "ab12cd34-5678-90ab-cdef-0123456789ab";

#[derive(Debug, Default)]
pub(crate) struct FakePublisher {
    pub(crate) calls: Mutex<Vec<String>>,
    pub(crate) reply_to: Mutex<Vec<Option<String>>>,
    pub(crate) prepared: Mutex<Vec<OutboundAttempt>>,
    pub(crate) sends: Mutex<Vec<String>>,
    pub(crate) fail_prepare: Mutex<Option<PublishError>>,
    pub(crate) fail: Mutex<Option<PublishError>>,
}

pub(crate) fn fake_event_id(attempt: &OutboundAttempt, created_at: i64) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    attempt.body.hash(&mut hasher);
    attempt.channel_id.hash(&mut hasher);
    attempt.reply_to_event_id.hash(&mut hasher);
    attempt.thread_root_event_id.hash(&mut hasher);
    attempt.mention.hash(&mut hasher);
    created_at.hash(&mut hasher);
    format!("{:064x}", hasher.finish())
}

impl OutboundPublisher for FakePublisher {
    type Error = PublishError;

    fn prepare(&self, attempt: &OutboundAttempt) -> Result<PreparedOutbound, Self::Error> {
        if let Some(error) = self.fail_prepare.lock().expect("fail_prepare").take() {
            return Err(error);
        }
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
        self.prepared
            .lock()
            .expect("prepared")
            .push(attempt.clone());
        let signed = nostr_sdk::prelude::EventBuilder::new(nostr_sdk::prelude::Kind::Custom(9), "")
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

pub(crate) fn event_id(character: char) -> EventId {
    EventId::parse_hex(&character.to_string().repeat(64)).expect("event")
}

pub(crate) fn open_repo() -> (SqliteRepository, FakePublisher) {
    open_repo_for("bot")
}

pub(crate) fn open_repository() -> SqliteRepository {
    open_repo().0
}

pub(crate) fn open_repo_for(bot: &str) -> (SqliteRepository, FakePublisher) {
    open_repo_for_tags(bot, "[]")
}

pub(crate) fn open_repository_with_tags(tags_json: &str) -> SqliteRepository {
    open_repo_for_tags("bot", tags_json).0
}

fn open_repo_for_tags(bot: &str, tags_json: &str) -> (SqliteRepository, FakePublisher) {
    let mut repository = SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
        .expect("repository");
    let bot_id = BotId::new(bot).expect("bot");
    repository
        .save_session(&SessionRecord {
            bot_id: bot_id.clone(),
            channel_id: CHANNEL.to_owned(),
            session_name: format!("{bot}-foobar"),
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
            ask_body: None,
            publish_reply_to_event_id: Some(event_id('a')),
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
                content: format!("{bot}: hello"),
                tags_json: tags_json.to_owned(),
                channel_id: Some(CHANNEL.to_owned()),
                target_event_id: None,
            },
            false,
        )
        .unwrap();
    (repository, FakePublisher::default())
}

pub(crate) fn quiet() -> impl FnMut(&str) {
    |_| {}
}
