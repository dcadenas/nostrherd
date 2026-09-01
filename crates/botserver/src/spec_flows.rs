//! Executable acceptance of SPEC user-visible flows 1–12.

use std::collections::VecDeque;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use botserver_domain::{Bot, BotId, EventId};
use nostr_sdk::prelude::{Event, EventBuilder, Keys, Kind, Tag};
use serde_json::Value;

use crate::actor::{BotActor, OccupantPane, OccupantPaneAllocator, TriggerOutcome};
use crate::relay::{IngestAction, RelayIngest, RelaySubscriber};
use crate::sqlite::SqliteRepository;
use crate::{CommandOutput, CommandRunner, HostRepository, KelpieClient, TurnState, WAITER_NAME};

const FOOBAR: &str = "ab12cd34-5678-90ab-cdef-0123456789ab";
const ENG: &str = "cd34ef56-7890-12ab-cdef-34567890abcd";
const DM: &str = "11111111-2222-3333-4444-555555555555";
const CHANNEL_KIND: u16 = 9;

#[derive(Debug)]
struct FakePanes {
    calls: Mutex<Vec<(String, PathBuf)>>,
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
        "incarnation_id": "waiter-incarnation",
        "operation_id": "adopt-operation",
        "outcome": "succeeded"
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
        "botserver-spec-{label}-{}-{}",
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
        .sign_with_keys(keys)
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
            }),
            bot: Bot::new(BotId::new("bot").expect("id"), corpus, "opencode").expect("bot"),
        }
    }

    fn ingest(&self, event: &Event) -> Option<IngestAction> {
        let mut ingest = RelayIngest::new(
            &self.operator,
            "f".repeat(64),
            SqliteRepository::open(&self.db_path).expect("ingest db"),
        );
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
    let waiter = harness
        .kelpie
        .adopt_waiter("w1:p2", "term-2")
        .expect("waiter");

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
    assert_eq!(harness.ask_bodies(), vec![b"hello".to_vec()]);
    assert_eq!(
        harness.panes.calls.lock().expect("panes")[0].0,
        "bot-foobar"
    );
    assert_eq!(waiter.identity().logical_agent_id(), "waiter-agent");
    assert_eq!(WAITER_NAME, "botserver");
}

#[test]
fn flow_03_follow_up_without_prefix_does_not_poke() {
    let harness = Harness::new([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
    let first = trigger_event(FOOBAR, "@daniel bot: hello", None);
    let action = harness.ingest(&first).expect("trigger");
    let mut actor = harness.actor();
    let waiter = harness
        .kelpie
        .adopt_waiter("w1:p2", "term-2")
        .expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &action, "Foobar")
        .expect("first");

    let follow_up = ordinary_event(FOOBAR, "and the PR?");
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
    let waiter = harness
        .kelpie
        .adopt_waiter("w1:p2", "term-2")
        .expect("waiter");
    actor
        .handle_ingest(&harness.kelpie, &waiter, &first_action, "Foobar")
        .expect("first");
    actor
        .repository
        .set_turn_state("ask-1", TurnState::Posted)
        .expect("posted");

    let second = trigger_event(FOOBAR, "@daniel bot: later", None);
    let second_action = harness.ingest(&second).expect("second");
    assert_eq!(
        actor
            .handle_ingest(&harness.kelpie, &waiter, &second_action, "Foobar")
            .expect("second"),
        TriggerOutcome::Asked
    );
    assert_eq!(session_name(&actor, FOOBAR).as_deref(), Some("bot-foobar"));
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
        harness.ask_bodies().last().map(Vec::as_slice),
        Some(&b"later"[..])
    );
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
    let waiter = harness
        .kelpie
        .adopt_waiter("w1:p2", "term-2")
        .expect("waiter");
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
    let waiter = harness
        .kelpie
        .adopt_waiter("w1:p2", "term-2")
        .expect("waiter");
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
    let waiter = harness
        .kelpie
        .adopt_waiter("w1:p2", "term-2")
        .expect("waiter");
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
    let waiter = harness
        .kelpie
        .adopt_waiter("w1:p2", "term-2")
        .expect("waiter");
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
        harness.ask_bodies().last().map(Vec::as_slice),
        Some(&b"second"[..])
    );
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
    let waiter = harness
        .kelpie
        .adopt_waiter("w1:p2", "term-2")
        .expect("waiter");
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
    let waiter = harness
        .kelpie
        .adopt_waiter("w1:p2", "term-2")
        .expect("waiter");
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
        harness.ask_bodies().last().map(Vec::as_slice),
        Some(&b"latest"[..])
    );

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
    let waiter = harness
        .kelpie
        .adopt_waiter("w1:p2", "term-2")
        .expect("waiter");
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
fn flow_11_one_ask_while_the_occupant_works() {
    let harness = Harness::new([adopt(), start(), renewed(), whoami(), asked("ask-1")]);
    let message = trigger_event(FOOBAR, "@daniel bot: long job", None);
    let action = harness.ingest(&message).expect("trigger");
    let mut actor = harness.actor();
    let waiter = harness
        .kelpie
        .adopt_waiter("w1:p2", "term-2")
        .expect("waiter");
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

#[test]
fn flow_12_host_does_not_publish_presence_or_typing() {
    let _subscriber = RelaySubscriber::new(nostr_sdk::Client::default());
    let harness = Harness::new([]);
    for kind in [0_u16, 7, 30_315] {
        assert_eq!(harness.ingest(&event(kind, "typing", [])), None);
    }
    let actor = harness.actor();
    assert!(session_name(&actor, FOOBAR).is_none());
    assert!(harness.verbs().is_empty());
}
