//! Live publish proofs for issues 54 and 55 (D43 and D42) against the
//! throwaway local Buzz relay. Skipped unless explicitly requested; run with the
//! local-relay harness up (see `skills/local-relay/SKILL.md` and
//! `docs/testing.md`):
//!
//! ```bash
//! ./tools/local-relay up
//! env -u BUZZ_AUTH_TAG envchain botserver-proof \
//!   cargo test --test live_publish -- --ignored --nocapture
//! ```

use std::time::Duration;

use botserver::outbox::{
    flush_progress, BuzzPublisher, OutboundAttempt, OutboundPublisher, ProgressPublisher,
};
use botserver::sqlite::SqliteRepository;
use botserver::{HostRepository, IndexedRelayEvent, NewTurn, SessionRecord, TurnRecord};
use botserver_domain::{buzz, stamp_outbound, BotId, EventId};
use nostr_sdk::prelude::{Client, Filter, Keys, Kind, SignerAuthenticator, SingleLetterTag};
use rusqlite::Connection;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);

fn relay_url() -> String {
    std::env::var("BUZZ_RELAY_URL").expect("BUZZ_RELAY_URL (local relay)")
}

fn operator_keys() -> Keys {
    let secret = std::env::var("BUZZ_PRIVATE_KEY").expect("BUZZ_PRIVATE_KEY");
    Keys::parse(&secret).expect("operator key")
}

async fn connect(keys: Keys, relay_url: &str) -> Client {
    let client = Client::builder()
        .authenticator(SignerAuthenticator::new(keys))
        .build();
    client.add_relay(relay_url).await.expect("add relay");
    client.connect().and_wait(CONNECT_TIMEOUT).await;
    client
}

async fn fetch_one(client: &Client, event_id: &str) -> Option<nostr_sdk::prelude::Event> {
    let filter = Filter::new()
        .id(nostr_sdk::prelude::EventId::from_hex(event_id).expect("event id"))
        .limit(1);
    let events = client
        .fetch_events(filter)
        .timeout(FETCH_TIMEOUT)
        .await
        .expect("fetch");
    events.into_iter().next()
}

fn tag_values(event: &nostr_sdk::prelude::Event, name: &str) -> Vec<String> {
    event
        .tags
        .iter()
        .filter_map(|tag| match tag.as_slice() {
            [tag_name, value, ..] if tag_name == name => Some(value.clone()),
            _ => None,
        })
        .collect()
}

fn attempt_for(channel: &str, trigger: &EventId, body: &str) -> OutboundAttempt {
    OutboundAttempt {
        ask_id: format!("live-{}", trigger.as_str()),
        body: body.to_owned(),
        channel_id: channel.to_owned(),
        reply_to_event_id: Some(trigger.clone()),
        thread_root_event_id: None,
        mention: String::new(),
        outbound_event_id: None,
        prepared_event_id: None,
        prepared_created_at: None,
        dispatched: false,
    }
}

/// One event of each kind (9, 40003, 9005, 7 plus the kind-5 removal),
/// then the relay-dedup retry proof: disconnect before re-publish and
/// the relay still holds exactly one event with the same id.
///
/// Requires `BOTSERVER_LIVE_CHANNEL`: a channel UUID the operator is a
/// member of, since the Buzz relay restricts channel writes to members.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live proof; run against tools/local-relay (see docs/testing.md)"]
async fn live_publishes_each_kind_and_dedups_a_redelivery() {
    let relay_url = relay_url();
    let keys = operator_keys();
    let channel = std::env::var("BOTSERVER_LIVE_CHANNEL").expect(
        "BOTSERVER_LIVE_CHANNEL (a channel UUID the operator is a member of, \
         created by the docs/testing.md recipe)",
    );
    let client = connect(keys.clone(), &relay_url).await;
    let publisher = BuzzPublisher::new(client.clone(), keys.clone(), relay_url.clone());

    // A real trigger on the channel: the operator posts a plain kind 9.
    let trigger_message = buzz::channel_message(&channel, "live trigger", &[], None);
    let trigger_id = publisher
        .send_buzz(&trigger_message)
        .await
        .expect("trigger post");
    let trigger = EventId::parse_hex(&trigger_id).expect("trigger id");

    // Kind 9 through the OutboundPublisher path: prepare gives the id
    // before send, and the accepted id is the prepared id. The
    // markerless trigger replies with a reply marker only.
    let stamped = stamp_outbound("live kind 9 body", "[bot]:");
    let attempt = attempt_for(&channel, &trigger, &stamped);
    let prepared = publisher.prepare(&attempt).expect("prepare");
    let accepted = publisher.publish(&prepared).expect("publish");
    assert_eq!(accepted, prepared.event_id());

    let event = fetch_one(&client, &accepted)
        .await
        .expect("kind 9 on the relay");
    assert_eq!(event.kind.as_u16(), buzz::CHANNEL_MESSAGE_KIND);
    assert_eq!(event.content, stamped);
    assert_eq!(tag_values(&event, "h"), vec![channel.clone()]);
    assert_eq!(
        tag_values(&event, "e"),
        vec![trigger.as_str().to_owned()],
        "markerless trigger replies with a reply marker only"
    );

    // Relay-dedup retry proof: drop the connection, reconnect, and
    // republish the identical prepared event. The relay dedups; exactly
    // one event with this id exists.
    client.disconnect().await;
    client.connect().and_wait(CONNECT_TIMEOUT).await;
    let redelivered = publisher.publish(&prepared).expect("republish");
    assert_eq!(redelivered, accepted);
    let filter = Filter::new()
        .id(nostr_sdk::prelude::EventId::from_hex(&accepted).expect("event id"))
        .limit(10);
    let events_after_redelivery = client
        .fetch_events(filter)
        .timeout(FETCH_TIMEOUT)
        .await
        .expect("fetch after redelivery")
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(
        events_after_redelivery.len(),
        1,
        "a redelivered event is relay-deduped to one event"
    );

    // Kind 40003 edit of the kind-9 post.
    let target = EventId::parse_hex(&accepted).expect("target");
    let edited = buzz::message_edit(&channel, &target, "live kind 40003 body");
    let edit_id = publisher.send_buzz(&edited).await.expect("edit");
    let edit_event = fetch_one(&client, &edit_id)
        .await
        .expect("kind 40003 on the relay");
    assert_eq!(edit_event.kind.as_u16(), buzz::MESSAGE_EDIT_KIND);
    assert_eq!(edit_event.content, "live kind 40003 body");
    assert_eq!(tag_values(&edit_event, "e"), vec![accepted.clone()]);

    // Kind 9005 delete of the kind-9 post.
    let deleted = buzz::message_delete(&channel, &target);
    let delete_id = publisher.send_buzz(&deleted).await.expect("delete");
    let delete_event = fetch_one(&client, &delete_id)
        .await
        .expect("kind 9005 on the relay");
    assert_eq!(delete_event.kind.as_u16(), buzz::MESSAGE_DELETE_KIND);
    assert_eq!(tag_values(&delete_event, "e"), vec![accepted]);

    // Kind 7 reaction and its kind-5 removal. The host marks the
    // trigger, which still exists (the reply above was deleted, and a
    // deleted event cannot take a reaction).
    let reaction = buzz::reaction(&trigger, botserver::outbox::IN_FLIGHT_REACTION);
    let reaction_id = publisher.send_buzz(&reaction).await.expect("reaction");
    let reaction_event = fetch_one(&client, &reaction_id)
        .await
        .expect("kind 7 on the relay");
    assert_eq!(reaction_event.kind.as_u16(), buzz::REACTION_KIND);
    assert_eq!(
        reaction_event.content,
        botserver::outbox::IN_FLIGHT_REACTION
    );
    assert_eq!(
        tag_values(&reaction_event, "e"),
        vec![trigger.as_str().to_owned()]
    );

    let reaction_event_id = EventId::parse_hex(&reaction_id).expect("reaction id");
    let removal = buzz::reaction_removal(&reaction_event_id);
    let removal_id = publisher
        .send_buzz(&removal)
        .await
        .expect("reaction removal");
    let removal_event = fetch_one(&client, &removal_id)
        .await
        .expect("kind 5 on the relay");
    assert_eq!(removal_event.kind.as_u16(), buzz::REACTION_REMOVAL_KIND);
    assert_eq!(tag_values(&removal_event, "e"), vec![reaction_id]);

    client.disconnect().await;
    println!("live publish proof complete");
}

struct LiveProgress {
    client: Client,
    publisher: BuzzPublisher,
    channel: String,
    repository: SqliteRepository,
    turn: TurnRecord,
    trigger_id: String,
}

async fn setup_live_progress() -> LiveProgress {
    let relay_url = relay_url();
    let keys = operator_keys();
    let channel = std::env::var("BOTSERVER_LIVE_CHANNEL").expect("BOTSERVER_LIVE_CHANNEL");
    let client = connect(keys.clone(), &relay_url).await;
    let publisher = BuzzPublisher::new(client.clone(), keys, relay_url);
    let trigger_id = publisher
        .send_buzz(&buzz::channel_message(
            &channel,
            "live progress trigger",
            &[],
            None,
        ))
        .await
        .expect("trigger post");
    let trigger = EventId::parse_hex(&trigger_id).expect("trigger id");
    let bot_id = BotId::new("bot").expect("bot id");
    let mut repository =
        SqliteRepository::from_connection(Connection::open_in_memory().expect("sqlite"))
            .expect("repository");
    repository
        .save_session(&SessionRecord {
            bot_id: bot_id.clone(),
            channel_id: channel.clone(),
            session_name: "bot-live-progress".to_owned(),
            occupant_logical_id: Some("live-occupant".to_owned()),
            renew_id: None,
            ask_context_event_id: None,
            ask_context_created_at: None,
        })
        .expect("session");
    repository
        .index_event(
            &IndexedRelayEvent {
                event_id: trigger.clone(),
                author_pubkey: "a".repeat(64),
                created_at: 1,
                kind: 9,
                content: "bot: long work".to_owned(),
                tags_json: serde_json::to_string(&vec![vec!["h", channel.as_str()]]).expect("tags"),
                channel_id: Some(channel.clone()),
                target_event_id: None,
            },
            false,
        )
        .expect("index trigger");
    repository
        .enqueue_turn(&NewTurn {
            bot_id: bot_id.clone(),
            channel_id: channel.clone(),
            event_id: trigger,
            reply_to_event_id: None,
        })
        .expect("queued turn");
    let turn = repository
        .open_next_turn(&bot_id, &channel, "live-progress-ask")
        .expect("open query")
        .expect("open turn");
    LiveProgress {
        client,
        publisher,
        channel,
        repository,
        turn,
        trigger_id,
    }
}

/// D42's persisted refresh path creates one post, edits that event, and
/// deletes it on cancellation without registering a host waiter.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live proof; run against tools/local-relay (see docs/testing.md)"]
async fn live_progress_create_edit_and_delete() {
    let LiveProgress {
        client,
        publisher,
        channel,
        mut repository,
        turn,
        trigger_id,
    } = setup_live_progress().await;
    repository
        .record_pending_progress("live-progress-ask", "first live status")
        .expect("record progress");
    let opened_at = turn.opened_at.expect("opened at");
    flush_progress(
        &mut repository,
        &publisher,
        &turn,
        opened_at + 19,
        &mut |_| {},
    )
    .expect("early flush");
    assert!(repository
        .progress_post("live-progress-ask")
        .expect("progress")
        .expect("progress row")
        .post_event_id
        .is_none());
    flush_progress(
        &mut repository,
        &publisher,
        &turn,
        opened_at + 20,
        &mut |_| {},
    )
    .expect("create flush");
    let progress_id = repository
        .progress_post("live-progress-ask")
        .expect("progress")
        .expect("progress row")
        .post_event_id
        .expect("progress id");
    let progress_event = fetch_one(&client, progress_id.as_str())
        .await
        .expect("progress create");
    assert_eq!(progress_event.content, "[bot]: first live status");
    assert!(tag_values(&progress_event, "p").is_empty());
    assert_eq!(tag_values(&progress_event, "e"), vec![trigger_id]);

    repository
        .record_pending_progress("live-progress-ask", "second live status")
        .expect("record edit");
    flush_progress(
        &mut repository,
        &publisher,
        &turn,
        opened_at + 50,
        &mut |_| {},
    )
    .expect("edit flush");
    let edit_filter = Filter::new()
        .kind(Kind::Custom(buzz::MESSAGE_EDIT_KIND))
        .custom_tag(SingleLetterTag::LOWERCASE_E, progress_id.as_str());
    let edits = client
        .fetch_events(edit_filter)
        .timeout(FETCH_TIMEOUT)
        .await
        .expect("fetch edits");
    assert!(edits
        .iter()
        .any(|event| event.content == "[bot]: second live status"));

    publisher
        .delete_progress(&channel, &progress_id)
        .expect("delete progress");
    let delete_filter = Filter::new()
        .kind(Kind::Custom(buzz::MESSAGE_DELETE_KIND))
        .custom_tag(SingleLetterTag::LOWERCASE_E, progress_id.as_str());
    let deletes = client
        .fetch_events(delete_filter)
        .timeout(FETCH_TIMEOUT)
        .await
        .expect("fetch deletes");
    assert!(!deletes.is_empty());
    client.disconnect().await;
    println!("live progress proof complete");
}
