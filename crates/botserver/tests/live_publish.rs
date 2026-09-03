//! Live publish proof for issue 54 (D43) against the throwaway local
//! Buzz relay. Skipped unless explicitly requested; run it with the
//! local-relay harness up (see `skills/local-relay/SKILL.md` and
//! `docs/testing.md`):
//!
//! ```bash
//! ./tools/local-relay up
//! env -u BUZZ_AUTH_TAG envchain botserver-proof \
//!   cargo test --test live_publish -- --ignored --nocapture
//! ```

use std::time::Duration;

use botserver::outbox::{BuzzPublisher, OutboundAttempt, OutboundPublisher};
use botserver_domain::{buzz, stamp_outbound, EventId};
use nostr_sdk::prelude::{Client, Filter, Keys, SignerAuthenticator};

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
