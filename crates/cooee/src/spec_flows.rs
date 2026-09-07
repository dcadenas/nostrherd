//! Executable acceptance of SPEC user-visible flows 1–12.

use std::collections::VecDeque;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use cooee_domain::{Bot, BotId, EventId};
use nostr_sdk::prelude::{Client, Event, EventBuilder, FinalizeEvent, Keys, Kind, Tag};
use serde_json::Value;

use crate::actor::{BotActor, OccupantPane, OccupantPaneAllocator, TriggerOutcome};
use crate::ask_body::ask_body_request;
use crate::relay::{IngestAction, RelayIngest, RelaySubscriber};
use crate::sqlite::SqliteRepository;
use crate::{
    CommandOutput, CommandRunner, HostRepository, IndexedRelayEvent, KelpieClient, TurnState,
    WAITER_NAME,
};

const FOOBAR: &str = "ab12cd34-5678-90ab-cdef-0123456789ab";
const ENG: &str = "cd34ef56-7890-12ab-cdef-34567890abcd";
const DM: &str = "11111111-2222-3333-4444-555555555555";
const CHANNEL_KIND: u16 = 9;

#[derive(Debug)]
struct FakePanes {
    calls: Mutex<Vec<(String, PathBuf)>>,
    released: Mutex<Vec<String>>,
}

impl OccupantPaneAllocator for Arc<FakePanes> {
    type Error = io::Error;

    fn allocate(&self, session_name: &str, cwd: &Path) -> Result<OccupantPane, Self::Error> {
        self.calls
            .lock()
            .expect("calls")
            .push((session_name.to_owned(), cwd.to_path_buf()));
        Ok(OccupantPane {
            pane_id: "w2:p1".to_owned(),
            terminal_id: "term-9".to_owned(),
        })
    }

    fn release(&self, pane: &OccupantPane) -> Result<(), Self::Error> {
        self.released
            .lock()
            .expect("released")
            .push(pane.pane_id.clone());
        Ok(())
    }
}

#[derive(Debug)]
struct FakeRunner {
    calls: Mutex<Vec<(Vec<String>, Vec<u8>)>>,
    outputs: Mutex<VecDeque<CommandOutput>>,
}

impl CommandRunner for Arc<FakeRunner> {
    fn run(&self, arguments: &[String], stdin: &[u8]) -> io::Result<CommandOutput> {
        self.calls
            .lock()
            .expect("calls")
            .push((arguments.to_vec(), stdin.to_vec()));
        self.outputs
            .lock()
            .expect("outputs")
            .pop_front()
            .ok_or_else(|| io::Error::other("missing fake output"))
    }
}

fn success(result: &Value) -> CommandOutput {
    CommandOutput {
        success: true,
        status: "exit status: 0".to_owned(),
        stdout: serde_json::to_vec(&serde_json::json!({
            "id": "request-id",
            "result": result
        }))
        .expect("json"),
        stderr: Vec::new(),
    }
}

fn adopt() -> CommandOutput {
    success(&serde_json::json!({
        "logical_agent_id": "waiter-agent",
        "public_name": "cooee",
        "delivery_transport": "socket_inbox"
    }))
}

fn start() -> CommandOutput {
    success(&serde_json::json!({
        "logical_agent_id": "occupant-agent",
        "incarnation_id": "occupant-incarnation",
        "runtime_start": {
            "operation_id": "start-operation",
            "outcome": "succeeded"
        },
        "initial_message": {
            "message_id": "tell-id",
            "operation_id": "tell-operation",
            "outcome": "accepted"
        }
    }))
}

fn whoami() -> CommandOutput {
    success(&serde_json::json!({
        "logical_agent_id": "occupant-agent",
        "incarnation_id": "occupant-incarnation",
        "public_name": "bot-foobar"
    }))
}

fn asked(message_id: &str) -> CommandOutput {
    success(&serde_json::json!({
        "message_id": message_id,
        "operation_id": "ask-operation",
        "recipient": "occupant-agent",
        "delivery_outcome": "accepted"
    }))
}

fn renewed() -> CommandOutput {
    success(&serde_json::json!({
        "renew_id": "renew-id",
        "recipient": "occupant-agent",
        "recipient_incarnation": "occupant-incarnation",
        "scheduled_at_ms": 1,
        "on_timeout": "abort",
        "phase": "scheduled",
        "every_ms": 2_700_000
    }))
}

fn cancelled() -> CommandOutput {
    success(&serde_json::json!({}))
}

fn failure(class: &str, message: &str) -> CommandOutput {
    CommandOutput {
        success: false,
        status: "exit status: 1".to_owned(),
        stdout: serde_json::to_vec(&serde_json::json!({
            "id": "request-id",
            "error": {"class": class, "message": message}
        }))
        .expect("json"),
        stderr: b"kelpie: request failed".to_vec(),
    }
}

fn temp_path(label: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "cooee-spec-{label}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).expect("temp");
    path
}

fn operator() -> String {
    "a".repeat(64)
}

fn tag(parts: &[&str]) -> Tag {
    Tag::parse(parts.iter().copied()).expect("tag")
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

fn event(kind: u16, content: &str, tags: impl IntoIterator<Item = Tag>) -> Event {
    event_with_keys(&Keys::generate(), kind, content, tags)
}

fn trigger_event(channel: &str, body: &str, reply: Option<&str>) -> Event {
    let operator = operator();
    let mut tags = vec![tag(&["h", channel]), tag(&["p", &operator])];
    if let Some(reply) = reply {
        tags.push(tag(&["e", reply, "", "reply"]));
    }
    event(CHANNEL_KIND, body, tags)
}

fn ordinary_event(channel: &str, body: &str) -> Event {
    event(CHANNEL_KIND, body, [tag(&["h", channel])])
}

struct Harness {
    db_path: PathBuf,
    operator: String,
    kelpie: KelpieClient,
    runner: Arc<FakeRunner>,
    panes: Arc<FakePanes>,
    bot: Bot,
}

impl Harness {
    fn new(outputs: impl IntoIterator<Item = CommandOutput>) -> Self {
        let root = temp_path("flow");
        let db_path = root.join("host.sqlite");
        let corpus = root.join("corpus");
        std::fs::create_dir_all(&corpus).expect("corpus");
        SqliteRepository::open(&db_path).expect("schema");
        let runner = Arc::new(FakeRunner {
            calls: Mutex::new(Vec::new()),
            outputs: Mutex::new(outputs.into_iter().collect()),
        });
        Self {
            db_path,
            operator: operator(),
            kelpie: KelpieClient::with_runner(Arc::clone(&runner)),
            runner,
            panes: Arc::new(FakePanes {
                calls: Mutex::new(Vec::new()),
                released: Mutex::new(Vec::new()),
            }),
            bot: Bot::new(BotId::new("bot").expect("id"), corpus, "opencode").expect("bot"),
        }
    }

    fn ingest(&self, event: &Event) -> Option<IngestAction> {
        let mut ingest = RelayIngest::new(
            &self.operator,
            "f".repeat(64),
            SqliteRepository::open(&self.db_path).expect("ingest db"),
        )
        .with_inbound_trigger(self.bot.inbound_trigger());
        ingest.ingest(event).expect("ingest")
    }

    fn actor(&self) -> BotActor<SqliteRepository, Arc<FakePanes>> {
        BotActor::new(
            self.bot.clone(),
            SqliteRepository::open(&self.db_path).expect("actor db"),
            Arc::clone(&self.panes),
        )
    }

    fn verbs(&self) -> Vec<String> {
        self.runner
            .calls
            .lock()
            .expect("calls")
            .iter()
            .map(|call| call.0[1].clone())
            .collect()
    }

    fn ask_bodies(&self) -> Vec<Vec<u8>> {
        self.runner
            .calls
            .lock()
            .expect("calls")
            .iter()
            .filter(|call| call.0[1] == "ask")
            .map(|call| call.1.clone())
            .collect()
    }
}

fn ask_requests(harness: &Harness) -> Vec<String> {
    harness
        .ask_bodies()
        .iter()
        .map(|body| ask_body_request(std::str::from_utf8(body).expect("utf8")).to_owned())
        .collect()
}

fn ask_has_context(body: &[u8]) -> bool {
    std::str::from_utf8(body)
        .expect("utf8")
        .contains("## Context")
}

fn session_name(
    actor: &BotActor<SqliteRepository, Arc<FakePanes>>,
    channel: &str,
) -> Option<String> {
    actor
        .repository
        .session(actor.bot().id(), channel)
        .expect("session")
        .map(|session| session.session_name)
}

fn turns(
    actor: &BotActor<SqliteRepository, Arc<FakePanes>>,
    channel: &str,
) -> Vec<crate::TurnRecord> {
    actor
        .repository
        .turns_for_session(actor.bot().id(), channel)
        .expect("turns")
}

#[test]
fn flow_01_silence_indexes_without_an_occupant() {
    let harness = Harness::new([]);
    let ordinary = ordinary_event(FOOBAR, "hello from the channel");
    let mention = event(
        CHANNEL_KIND,
        "@daniel did you see the PR?",
        [tag(&["h", FOOBAR]), tag(&["p", &operator()])],
    );
    let metadata = event(0, "{}", []);

    assert_eq!(harness.ingest(&ordinary), None);
    assert_eq!(harness.ingest(&mention), None);
    assert_eq!(harness.ingest(&metadata), None);

    let actor = harness.actor();
    assert!(session_name(&actor, FOOBAR).is_none());
    assert!(turns(&actor, FOOBAR).is_empty());
    assert!(harness.verbs().is_empty());
    assert!(harness.panes.calls.lock().expect("panes").is_empty());
}

#[test]
fn flow_02_first_call_starts_bot_foobar_and_asks() {
    let harness = Harness::new([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
    let message = trigger_event(FOOBAR, "@daniel bot: hello", None);
    let action = harness.ingest(&message).expect("trigger");
    let mut actor = harness.actor();
    let waiter = harness.kelpie.register_waiter().expect("waiter");

    assert_eq!(
        actor
            .handle_ingest(&harness.kelpie, &waiter, &action, "Foobar")
            .expect("first call"),
        TriggerOutcome::Asked
    );
    assert_eq!(session_name(&actor, FOOBAR).as_deref(), Some("bot-foobar"));
    let turns = turns(&actor, FOOBAR);
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].state, TurnState::Open);
    assert_eq!(turns[0].ask_id.as_deref(), Some("ask-1"));
    assert_eq!(turns[0].event_id.as_str(), message.id.to_hex());
    assert_eq!(ask_requests(&harness), vec!["hello".to_owned()]);
    assert!(ask_has_context(&harness.ask_bodies()[0]));
    assert_eq!(
        harness.panes.calls.lock().expect("panes")[0].0,
        "bot-foobar"
    );
    assert_eq!(waiter.identity().logical_agent_id(), "waiter-agent");
    let register = &harness.runner.calls.lock().expect("calls")[0].0;
    assert!(register
        .windows(2)
        .any(|pair| pair == ["--name", WAITER_NAME]));
    assert!(register.iter().any(|arg| arg == "waiter-register"));
}

#[test]
fn flow_03_follow_up_without_prefix_does_not_poke() {
    let harness = Harness::new([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
    let first = trigger_event(FOOBAR, "@daniel bot: hello", None);
    let action = harness.ingest(&first).expect("trigger");
    let mut actor = harness.actor();
    let waiter = harness.kelpie.register_waiter().expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &action, "Foobar")
        .expect("first");

    let follow_up = event(
        CHANNEL_KIND,
        "and the PR?",
        [tag(&["h", FOOBAR]), tag(&["p", &operator()])],
    );
    assert_eq!(harness.ingest(&follow_up), None);
    assert_eq!(turns(&actor, FOOBAR).len(), 1);
    assert_eq!(
        harness.verbs().iter().filter(|verb| *verb == "ask").count(),
        1
    );
}

#[test]
fn flow_04_second_call_reuses_the_same_occupant() {
    let harness = Harness::new([
        adopt(),
        start(),
        renewed(),
        whoami(),
        asked("ask-1"),
        whoami(),
        asked("ask-2"),
    ]);
    let first = trigger_event(FOOBAR, "@daniel bot: hello", None);
    let first_action = harness.ingest(&first).expect("first");
    let mut actor = harness.actor();
    let waiter = harness.kelpie.register_waiter().expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &first_action, "Foobar")
        .expect("first");
    actor
        .repository
        .set_turn_state("ask-1", TurnState::Posted)
        .expect("posted");

    let first_hex = first.id.to_hex();
    let second = trigger_event(FOOBAR, "@daniel bot: later", Some(&first_hex));
    let second_action = harness.ingest(&second).expect("second");
    assert_eq!(
        actor
            .handle_ingest(&harness.kelpie, &waiter, &second_action, "Foobar")
            .expect("second"),
        TriggerOutcome::Asked
    );
    assert_eq!(session_name(&actor, FOOBAR).as_deref(), Some("bot-foobar"));
    let second_turn = &turns(&actor, FOOBAR)[1];
    assert_eq!(second_turn.event_id.as_str(), second.id.to_hex());
    assert_eq!(
        second_turn.reply_to_event_id.as_ref().map(EventId::as_str),
        Some(first_hex.as_str())
    );
    assert_eq!(harness.panes.calls.lock().expect("panes").len(), 1);
    assert_eq!(
        harness
            .verbs()
            .iter()
            .filter(|verb| *verb == "start")
            .count(),
        1
    );
    assert_eq!(
        ask_requests(&harness).last().map(String::as_str),
        Some("later")
    );
    assert!(ask_has_context(harness.ask_bodies().last().expect("ask")));
}

#[test]
fn ask_context_includes_unprefixed_line_between_triggers() {
    let harness = Harness::new([
        adopt(),
        start(),
        renewed(),
        whoami(),
        asked("ask-1"),
        whoami(),
        asked("ask-2"),
    ]);
    let first = trigger_event(FOOBAR, "@daniel bot: hello", None);
    let first_action = harness.ingest(&first).expect("first");
    let mut actor = harness.actor();
    let waiter = harness.kelpie.register_waiter().expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &first_action, "Foobar")
        .expect("first");
    actor
        .repository
        .set_turn_state("ask-1", TurnState::Posted)
        .expect("posted");

    let follow_up = ordinary_event(FOOBAR, "and the PR?");
    assert_eq!(harness.ingest(&follow_up), None);
    let first_created = actor
        .repository
        .indexed_event(&turns(&actor, FOOBAR)[0].event_id)
        .expect("first event")
        .expect("indexed")
        .created_at;
    actor
        .repository
        .index_event(
            &IndexedRelayEvent {
                event_id: EventId::parse_hex(&"c".repeat(64)).expect("event"),
                author_pubkey: "b".repeat(64),
                created_at: first_created + 1,
                kind: 9,
                content: "and the PR?".to_owned(),
                tags_json: "[]".to_owned(),
                channel_id: Some(FOOBAR.to_owned()),
                target_event_id: None,
            },
            false,
        )
        .expect("later unprefixed");
    actor
        .repository
        .index_event(
            &IndexedRelayEvent {
                event_id: EventId::parse_hex(&"d".repeat(64)).expect("event"),
                author_pubkey: "b".repeat(64),
                created_at: first_created + 1,
                kind: 9,
                content: "secret dm".to_owned(),
                tags_json: "[]".to_owned(),
                channel_id: Some(DM.to_owned()),
                target_event_id: None,
            },
            false,
        )
        .expect("other place");

    let second = trigger_event(FOOBAR, "@daniel bot: later", None);
    let second_action = harness.ingest(&second).expect("second");
    assert_eq!(
        actor
            .handle_ingest(&harness.kelpie, &waiter, &second_action, "Foobar")
            .expect("second"),
        TriggerOutcome::Asked
    );
    let bodies = harness.ask_bodies();
    let text = std::str::from_utf8(bodies.last().expect("ask")).expect("utf8");
    assert_eq!(ask_body_request(text), "later");
    assert!(text.contains("## Context"));
    assert!(text.contains("Untrusted indexed channel text"));
    assert!(text.contains("and the PR?"));
    assert!(!text.contains("secret dm"));
}

#[test]
fn flow_05_another_channel_is_an_independent_occupant() {
    let harness = Harness::new([
        adopt(),
        start(),
        renewed(),
        whoami(),
        asked("ask-1"),
        start(),
        renewed(),
        whoami(),
        asked("ask-2"),
    ]);
    let foobar = trigger_event(FOOBAR, "@daniel bot: hello", None);
    let eng = trigger_event(ENG, "@daniel bot: status", None);
    let foobar_action = harness.ingest(&foobar).expect("foobar");
    let eng_action = harness.ingest(&eng).expect("eng");
    let mut actor = harness.actor();
    let waiter = harness.kelpie.register_waiter().expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &foobar_action, "Foobar")
        .expect("foobar");
    assert_eq!(
        actor
            .handle_ingest(&harness.kelpie, &waiter, &eng_action, "Eng")
            .expect("eng"),
        TriggerOutcome::Asked
    );
    assert_eq!(session_name(&actor, FOOBAR).as_deref(), Some("bot-foobar"));
    assert_eq!(session_name(&actor, ENG).as_deref(), Some("bot-eng"));
    assert_eq!(turns(&actor, FOOBAR)[0].state, TurnState::Open);
    assert_eq!(turns(&actor, ENG)[0].state, TurnState::Open);
    let names: Vec<String> = harness
        .panes
        .calls
        .lock()
        .expect("panes")
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    assert_eq!(names, ["bot-foobar", "bot-eng"]);
}

#[test]
fn flow_06_dm_is_its_own_channel_session() {
    let harness = Harness::new([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
    let dm = trigger_event(DM, "@daniel bot: ping", None);
    let action = harness.ingest(&dm).expect("dm trigger");
    let mut actor = harness.actor();
    let waiter = harness.kelpie.register_waiter().expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &action, "sebastian")
        .expect("dm");
    assert_eq!(session_name(&actor, DM).as_deref(), Some("bot-sebastian"));

    let follow_up = ordinary_event(DM, "unprefixed dm line");
    assert_eq!(harness.ingest(&follow_up), None);
    assert_eq!(turns(&actor, DM).len(), 1);
}

#[test]
fn flow_07_thread_stays_on_the_channel_occupant() {
    let harness = Harness::new([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
    let thread = "b".repeat(64);
    let message = trigger_event(FOOBAR, "@daniel bot: in thread", Some(&thread));
    let action = harness.ingest(&message).expect("thread trigger");
    let IngestAction::TurnCandidate {
        reply_to_event_id, ..
    } = &action
    else {
        panic!("expected turn candidate");
    };
    assert_eq!(
        reply_to_event_id.as_ref().map(EventId::as_str),
        Some(thread.as_str())
    );
    let mut actor = harness.actor();
    let waiter = harness.kelpie.register_waiter().expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &action, "Foobar")
        .expect("thread");
    assert_eq!(session_name(&actor, FOOBAR).as_deref(), Some("bot-foobar"));
    assert_eq!(
        turns(&actor, FOOBAR)[0]
            .reply_to_event_id
            .as_ref()
            .map(EventId::as_str),
        Some(thread.as_str())
    );
}

#[test]
fn flow_08_busy_queues_the_second_turn() {
    let harness = Harness::new([
        adopt(),
        start(),
        renewed(),
        whoami(),
        asked("ask-1"),
        whoami(),
        asked("ask-2"),
    ]);
    let first = trigger_event(FOOBAR, "@daniel bot: first", None);
    let second = trigger_event(FOOBAR, "@daniel bot: second", None);
    let first_action = harness.ingest(&first).expect("first");
    let second_action = harness.ingest(&second).expect("second");
    let mut actor = harness.actor();
    let waiter = harness.kelpie.register_waiter().expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &first_action, "Foobar")
        .expect("first");
    assert_eq!(
        actor
            .handle_ingest(&harness.kelpie, &waiter, &second_action, "Foobar")
            .expect("queued"),
        TriggerOutcome::Queued
    );
    assert_eq!(harness.panes.calls.lock().expect("panes").len(), 1);
    assert_eq!(turns(&actor, FOOBAR)[1].state, TurnState::Queued);
    actor
        .repository
        .set_turn_state("ask-1", TurnState::Posted)
        .expect("posted");
    assert_eq!(
        actor
            .handle_turn_completed(&harness.kelpie, &waiter)
            .expect("drain"),
        Some(TriggerOutcome::Asked)
    );
    assert_eq!(turns(&actor, FOOBAR)[1].ask_id.as_deref(), Some("ask-2"));
    assert_eq!(
        ask_requests(&harness).last().map(String::as_str),
        Some("second")
    );
    assert!(ask_has_context(harness.ask_bodies().last().expect("ask")));
}

#[test]
fn flow_09_gone_pane_continues_the_logical_agent() {
    let harness = Harness::new([
        adopt(),
        start(),
        renewed(),
        whoami(),
        asked("ask-1"),
        failure("conflict", "no ready agent for alias bot-foobar"),
        start(),
        renewed(),
    ]);
    let message = trigger_event(FOOBAR, "@daniel bot: hello", None);
    let action = harness.ingest(&message).expect("trigger");
    let mut actor = harness.actor();
    let waiter = harness.kelpie.register_waiter().expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &action, "Foobar")
        .expect("asked");
    assert_eq!(
        actor
            .recover_open_occupants(&harness.kelpie, &waiter)
            .expect("recover"),
        1
    );
    assert_eq!(turns(&actor, FOOBAR).len(), 1);
    assert_eq!(turns(&actor, FOOBAR)[0].ask_id.as_deref(), Some("ask-1"));
    assert_eq!(
        harness.verbs().iter().filter(|verb| *verb == "ask").count(),
        1
    );
    assert_eq!(
        harness
            .verbs()
            .iter()
            .filter(|verb| *verb == "start")
            .count(),
        2
    );
    let continued = harness
        .runner
        .calls
        .lock()
        .expect("calls")
        .iter()
        .rfind(|call| call.0[1] == "start")
        .expect("continued start")
        .0
        .clone();
    assert!(continued
        .windows(2)
        .any(|pair| pair == ["--logical-id", "occupant-agent"]));
}

#[test]
fn flow_10_edit_answers_latest_text_and_delete_abandons() {
    let harness = Harness::new([
        adopt(),
        start(),
        renewed(),
        whoami(),
        asked("ask-1"),
        cancelled(),
        whoami(),
        asked("ask-2"),
        cancelled(),
    ]);
    let author = Keys::generate();
    let operator = operator();
    let first = event_with_keys(
        &author,
        CHANNEL_KIND,
        "@daniel bot: hello",
        [tag(&["h", FOOBAR]), tag(&["p", &operator])],
    );
    let first_action = harness.ingest(&first).expect("first");
    let mut actor = harness.actor();
    let waiter = harness.kelpie.register_waiter().expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &first_action, "Foobar")
        .expect("first");

    let target = first.id.to_hex();
    let edit = event_with_keys(
        &author,
        40_003,
        "bot: latest",
        [
            tag(&["h", FOOBAR]),
            tag(&["e", &target]),
            tag(&["p", &operator]),
        ],
    );
    let edit_action = harness.ingest(&edit).expect("edit");
    assert_eq!(
        actor
            .handle_ingest(&harness.kelpie, &waiter, &edit_action, "Foobar")
            .expect("replaced"),
        TriggerOutcome::Asked
    );
    assert_eq!(turns(&actor, FOOBAR)[0].state, TurnState::Cancelled);
    assert_eq!(turns(&actor, FOOBAR)[1].state, TurnState::Open);
    assert_eq!(
        ask_requests(&harness).last().map(String::as_str),
        Some("latest")
    );
    assert!(ask_has_context(harness.ask_bodies().last().expect("ask")));

    let delete = event_with_keys(&author, 5, "", [tag(&["h", FOOBAR]), tag(&["e", &target])]);
    let delete_action = harness.ingest(&delete).expect("delete");
    assert_eq!(
        actor
            .handle_ingest(&harness.kelpie, &waiter, &delete_action, "Foobar")
            .expect("cancelled"),
        TriggerOutcome::Cancelled
    );
    assert_eq!(turns(&actor, FOOBAR)[1].state, TurnState::Cancelled);
}

#[test]
fn flow_10_claimed_turn_keeps_the_landing_reply() {
    let harness = Harness::new([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
    let author = Keys::generate();
    let operator = operator();
    let first = event_with_keys(
        &author,
        CHANNEL_KIND,
        "@daniel bot: hello",
        [tag(&["h", FOOBAR]), tag(&["p", &operator])],
    );
    let action = harness.ingest(&first).expect("first");
    let mut actor = harness.actor();
    let waiter = harness.kelpie.register_waiter().expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &action, "Foobar")
        .expect("first");
    assert!(actor
        .repository
        .claim_turn_for_publish("ask-1")
        .expect("claim"));

    let target = first.id.to_hex();
    let edit = event_with_keys(
        &author,
        40_003,
        "bot: too late",
        [
            tag(&["h", FOOBAR]),
            tag(&["e", &target]),
            tag(&["p", &operator]),
        ],
    );
    let edit_action = harness.ingest(&edit).expect("edit");
    assert_eq!(
        actor
            .handle_ingest(&harness.kelpie, &waiter, &edit_action, "Foobar")
            .expect("ignored"),
        TriggerOutcome::Declined
    );
    assert_eq!(turns(&actor, FOOBAR)[0].state, TurnState::Open);
    assert!(harness.verbs().iter().all(|verb| verb != "cancel"));
}

#[test]
fn flow_10_posted_turn_is_left_up_after_delete() {
    let harness = Harness::new([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
    let author = Keys::generate();
    let operator = operator();
    let first = event_with_keys(
        &author,
        CHANNEL_KIND,
        "@daniel bot: hello",
        [tag(&["h", FOOBAR]), tag(&["p", &operator])],
    );
    let action = harness.ingest(&first).expect("first");
    let mut actor = harness.actor();
    let waiter = harness.kelpie.register_waiter().expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &action, "Foobar")
        .expect("first");
    actor
        .repository
        .set_turn_state("ask-1", TurnState::Posted)
        .expect("posted");

    let target = first.id.to_hex();
    let delete = event_with_keys(&author, 5, "", [tag(&["h", FOOBAR]), tag(&["e", &target])]);
    assert_eq!(harness.ingest(&delete), None);
    assert_eq!(turns(&actor, FOOBAR)[0].state, TurnState::Posted);
    assert!(harness.verbs().iter().all(|verb| verb != "cancel"));
}

struct FlowPublisher {
    prepared: Mutex<Vec<crate::outbox::OutboundAttempt>>,
}

impl crate::outbox::OutboundPublisher for FlowPublisher {
    type Error = crate::outbox::PublishError;

    fn prepare(
        &self,
        attempt: &crate::outbox::OutboundAttempt,
    ) -> Result<crate::outbox::PreparedOutbound, Self::Error> {
        self.prepared
            .lock()
            .expect("prepared")
            .push(attempt.clone());
        let created_at = attempt.prepared_created_at.unwrap_or(1_700_000_000);
        let index = self.prepared.lock().expect("prepared").len();
        let event_id = attempt
            .prepared_event_id
            .clone()
            .unwrap_or_else(|| format!("{index:064x}"));
        let signed = EventBuilder::new(Kind::Custom(CHANNEL_KIND), "")
            .finalize(&Keys::generate())
            .expect("dummy event");
        Ok(crate::outbox::PreparedOutbound::from_parts(
            signed, event_id, created_at,
        ))
    }

    fn publish(&self, prepared: &crate::outbox::PreparedOutbound) -> Result<String, Self::Error> {
        Ok(prepared.event_id().to_owned())
    }
}

fn occupant_reply(ask_id: &str, disposition: &str, body: &str) -> crate::inbox::InboxDelivery {
    crate::inbox::parse_delivery(&serde_json::json!({
        "method": "inbox.delivery",
        "params": {
            "message_id": format!("msg-{disposition}-{}", body.len()),
            "kind": "reply",
            "disposition": disposition,
            "reply_to": ask_id,
            "body": body
        }
    }))
    .expect("delivery")
}

#[test]
fn flow_11_one_ask_while_the_occupant_works() {
    let harness = Harness::new([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
    let message = trigger_event(FOOBAR, "@daniel bot: long job", None);
    let action = harness.ingest(&message).expect("trigger");
    let mut actor = harness.actor();
    let waiter = harness.kelpie.register_waiter().expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &action, "Foobar")
        .expect("asked");
    assert_eq!(
        harness.ingest(&ordinary_event(FOOBAR, "still waiting")),
        None
    );
    assert_eq!(
        harness.verbs().iter().filter(|verb| *verb == "ask").count(),
        1
    );
    assert!(harness.verbs().iter().all(|verb| verb != "tell"));
}

/// Flow 11 under D42: the occupant reports progress; the host relays one
/// stamped post after the hold, edits it in place, and the final is a
/// second post that leaves it up. The host never invents progress.
#[test]
fn flow_11_progress_is_one_edited_post_then_a_final() {
    use cooee_domain::progress::{PROGRESS_EDIT_INTERVAL_SECS, PROGRESS_INITIAL_HOLD_SECS};

    let harness = Harness::new([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
    let relay = Arc::new(crate::progress::RecordingProgressRelay::default());
    let message = trigger_event(FOOBAR, "@daniel bot: long job", None);
    let action = harness.ingest(&message).expect("trigger");
    let mut actor = harness
        .actor()
        .with_progress_relay(Arc::clone(&relay) as Arc<dyn crate::progress::ProgressRelay>);
    let waiter = harness.kelpie.register_waiter().expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &action, "Foobar")
        .expect("asked");
    let publisher = FlowPublisher {
        prepared: Mutex::new(Vec::new()),
    };
    let opened_at = turns(&actor, FOOBAR)[0].opened_at.expect("opened_at");
    let trigger_id = turns(&actor, FOOBAR)[0].event_id.clone();
    actor
        .handle_occupant_delivery(
            &harness.kelpie,
            &waiter,
            &publisher,
            &occupant_reply("ask-1", "progress", "reading the repo"),
        )
        .expect("progress");
    actor
        .flush_progress(&publisher, opened_at + PROGRESS_INITIAL_HOLD_SECS - 1)
        .expect("flush before hold");
    assert!(publisher.prepared.lock().expect("prepared").is_empty());

    let created = opened_at + PROGRESS_INITIAL_HOLD_SECS;
    actor
        .flush_progress(&publisher, created)
        .expect("flush after hold");
    let prepared = publisher.prepared.lock().expect("prepared").clone();
    assert_eq!(prepared.len(), 1);
    assert_eq!(prepared[0].body, "[bot]: reading the repo");
    assert_eq!(prepared[0].reply_to_event_id.as_ref(), Some(&trigger_id));
    assert!(prepared[0].mention.is_empty());
    let post_id = actor
        .repository
        .progress_post("ask-1")
        .expect("row")
        .expect("row")
        .post_event_id
        .expect("posted");

    actor
        .handle_occupant_delivery(
            &harness.kelpie,
            &waiter,
            &publisher,
            &occupant_reply("ask-1", "progress", "drafting the answer"),
        )
        .expect("second progress");
    actor
        .flush_progress(&publisher, created + PROGRESS_EDIT_INTERVAL_SECS)
        .expect("flush edit");
    assert_eq!(
        relay.edits.lock().expect("edits").as_slice(),
        [(
            FOOBAR.to_owned(),
            post_id.clone(),
            "[bot]: drafting the answer".to_owned()
        )]
    );
    assert_eq!(
        publisher.prepared.lock().expect("prepared").len(),
        1,
        "edits never create a second post"
    );

    // The final is a second stamped post; the progress post stays up.
    actor
        .handle_occupant_delivery(
            &harness.kelpie,
            &waiter,
            &publisher,
            &occupant_reply("ask-1", "final", "long job done"),
        )
        .expect("final");
    let prepared = publisher.prepared.lock().expect("prepared").clone();
    assert_eq!(prepared.len(), 2);
    assert_eq!(prepared[1].body, "[bot]: long job done");
    assert_eq!(turns(&actor, FOOBAR)[0].state, TurnState::Posted);
    assert!(relay.deletes.lock().expect("deletes").is_empty());
    assert_eq!(
        actor
            .repository
            .progress_post("ask-1")
            .expect("row")
            .expect("row")
            .post_event_id
            .as_deref(),
        Some(post_id.as_str())
    );
    assert!(harness.verbs().iter().all(|verb| verb != "tell"));
}

#[test]
fn flow_12_host_does_not_publish_presence_or_typing() {
    let subscriber = RelaySubscriber::new(Client::default());
    let _notifications = subscriber.notifications();
    let harness = Harness::new([]);
    for kind in [0_u16, 7, 30_315] {
        assert_eq!(harness.ingest(&event(kind, "typing", [])), None);
    }
    let actor = harness.actor();
    assert!(session_name(&actor, FOOBAR).is_none());
    assert!(harness.verbs().is_empty());
    assert!(harness
        .runner
        .calls
        .lock()
        .expect("calls")
        .iter()
        .all(|call| call.0.iter().all(|arg| arg != "envchain" && arg != "send")));
}
