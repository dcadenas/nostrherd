//! Host process: relay subscriber, per-bot actors, Kelpie waiter.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(test)]
use botserver::actor::persist_ingest;
#[cfg(test)]
use botserver::actor::TriggerOutcome;
use botserver::actor::{ActorError, BotActor};
use botserver::config::{BotRegistry, ConfigError};
use botserver::herdr::{HerdrError, HerdrPaneAllocator};
use botserver::inbox::{default_socket, spawn_inbox};
use botserver::outbox::{BuzzPublisher, InboxAction};
use botserver::relay::{
    IngestAction, IngestError, RelayIngest, RelaySubscribeError, RelaySubscriber,
};
use botserver::sqlite::SqliteRepository;
use botserver::{HostRepository, HostWaiter, KelpieClient, KelpieError, WAITER_IDEMPOTENCY_KEY};
use botserver_domain::{Bot, EventId};
use clap::Parser;
use futures::StreamExt;
use nostr_sdk::prelude::{Client, ClientNotification, Event, Keys, SignerAuthenticator, Timestamp};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const EMPTY_REPLAY_OVERLAP_SECS: u64 = 900;
const SUBSCRIPTION_REFRESH: Duration = Duration::from_secs(1);
const RESUME_QUEUED_EVERY: Duration = Duration::from_secs(30);

#[derive(Debug, Parser)]
#[command(about = "Host occupant and per-bot actors")]
struct Args {
    /// Path to the bot registry TOML.
    #[arg(long)]
    config: PathBuf,

    /// Path to the host SQLite database.
    #[arg(long)]
    database: PathBuf,

    /// Load config and database, then exit.
    #[arg(long)]
    check: bool,
}

struct OperatorEnv {
    keys: Keys,
    relay_url: String,
}

impl OperatorEnv {
    fn from_env() -> Result<Self, HostError> {
        let private_key = required_env("BUZZ_PRIVATE_KEY")?;
        let relay_url = required_env("BUZZ_RELAY_URL")?;
        let keys = Keys::parse(&private_key).map_err(|_| HostError::InvalidOperatorKey)?;
        Ok(Self { keys, relay_url })
    }
}

fn required_env(name: &'static str) -> Result<String, HostError> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or(HostError::MissingEnv(name))
}

#[derive(Debug)]
enum HostError {
    Config(ConfigError),
    Database(rusqlite::Error),
    MissingEnv(&'static str),
    InvalidOperatorKey,
    Runtime(std::io::Error),
    WaiterKey(std::io::Error),
    Relay(nostr_sdk::error::Error),
    Subscribe(RelaySubscribeError),
    Ingest(IngestError<rusqlite::Error>),
    Herdr(HerdrError),
    Kelpie(KelpieError),
    Actor(ActorError<rusqlite::Error>),
    NoBots,
    NotificationClosed,
    InboxClosed,
}

impl fmt::Display for HostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "{error}"),
            Self::Database(error) => write!(formatter, "failed to open host database: {error}"),
            Self::MissingEnv(name) => write!(formatter, "missing {name}"),
            Self::InvalidOperatorKey => formatter.write_str("invalid BUZZ_PRIVATE_KEY"),
            Self::Runtime(error) => write!(formatter, "failed to start async runtime: {error}"),
            Self::WaiterKey(error) => {
                write!(
                    formatter,
                    "failed to persist waiter idempotency key: {error}"
                )
            }
            Self::Relay(error) => write!(formatter, "relay client failed: {error}"),
            Self::Subscribe(error) => write!(formatter, "{error}"),
            Self::Ingest(error) => write!(formatter, "{error}"),
            Self::Herdr(error) => write!(formatter, "{error}"),
            Self::Kelpie(error) => write!(formatter, "{error}"),
            Self::Actor(error) => write!(formatter, "{error}"),
            Self::NoBots => formatter.write_str("bot config has no bots"),
            Self::NotificationClosed => formatter.write_str("relay notification channel closed"),
            Self::InboxClosed => formatter.write_str("kelpie inbox closed"),
        }
    }
}

impl std::error::Error for HostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Database(error) => Some(error),
            Self::Runtime(error) | Self::WaiterKey(error) => Some(error),
            Self::Relay(error) => Some(error),
            Self::Subscribe(error) => Some(error),
            Self::Ingest(error) => Some(error),
            Self::Herdr(error) => Some(error),
            Self::Kelpie(error) => Some(error),
            Self::Actor(error) => Some(error),
            Self::MissingEnv(_)
            | Self::InvalidOperatorKey
            | Self::NoBots
            | Self::NotificationClosed
            | Self::InboxClosed => None,
        }
    }
}

impl From<ConfigError> for HostError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<rusqlite::Error> for HostError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

impl From<nostr_sdk::error::Error> for HostError {
    fn from(error: nostr_sdk::error::Error) -> Self {
        Self::Relay(error)
    }
}

impl From<RelaySubscribeError> for HostError {
    fn from(error: RelaySubscribeError) -> Self {
        Self::Subscribe(error)
    }
}

impl From<IngestError<rusqlite::Error>> for HostError {
    fn from(error: IngestError<rusqlite::Error>) -> Self {
        Self::Ingest(error)
    }
}

impl From<HerdrError> for HostError {
    fn from(error: HerdrError) -> Self {
        Self::Herdr(error)
    }
}

impl From<KelpieError> for HostError {
    fn from(error: KelpieError) -> Self {
        Self::Kelpie(error)
    }
}

impl From<ActorError<rusqlite::Error>> for HostError {
    fn from(error: ActorError<rusqlite::Error>) -> Self {
        Self::Actor(error)
    }
}

fn load_host(config: &Path, database: &Path) -> Result<(BotRegistry, SqliteRepository), HostError> {
    Ok((
        BotRegistry::load(config)?,
        SqliteRepository::open(database)?,
    ))
}

fn waiter_key_path(database: &Path) -> PathBuf {
    let mut path = database.as_os_str().to_owned();
    path.push(".waiter-key");
    PathBuf::from(path)
}

fn read_waiter_key(database: &Path) -> String {
    fs::read_to_string(waiter_key_path(database))
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| WAITER_IDEMPOTENCY_KEY.to_owned())
}

fn ended_waiter_key(error: &KelpieError) -> bool {
    matches!(
        error,
        KelpieError::Rejected { stderr, .. } if stderr.contains("ended waiter")
    )
}

fn mint_waiter_key(database: &Path) -> Result<String, HostError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let key = format!("botserver-host-waiter-{millis}");
    fs::write(waiter_key_path(database), format!("{key}\n")).map_err(HostError::WaiterKey)?;
    Ok(key)
}

fn register_host_waiter<'a>(
    kelpie: &'a KelpieClient,
    database: &Path,
) -> Result<HostWaiter<'a>, HostError> {
    let key = read_waiter_key(database);
    match kelpie.register_waiter_with_key(&key) {
        Ok(waiter) => Ok(waiter),
        Err(error) if ended_waiter_key(&error) => {
            let fresh = mint_waiter_key(database)?;
            Ok(kelpie.register_waiter_with_key(&fresh)?)
        }
        Err(error) => Err(error.into()),
    }
}

fn replay_since(repository: &SqliteRepository) -> Result<Timestamp, HostError> {
    Ok(match repository.relay_replay_since()? {
        Some(seconds) => Timestamp::from(u64::try_from(seconds.max(0)).unwrap_or(0)),
        None => Timestamp::now() - EMPTY_REPLAY_OVERLAP_SECS,
    })
}

fn ingest_event_id(action: &IngestAction) -> &EventId {
    match action {
        IngestAction::TurnCandidate { event_id, .. }
        | IngestAction::Edit { event_id, .. }
        | IngestAction::Delete { event_id, .. } => event_id,
    }
}

#[cfg(test)]
fn dispatch_ingest(
    bots: &[Bot],
    repository: &mut SqliteRepository,
    action: &IngestAction,
) -> Result<TriggerOutcome, HostError> {
    if let (IngestAction::TurnCandidate { .. }, Some(bot)) = (action, bots.first()) {
        Ok(persist_ingest(bot, repository, action, "")?)
    } else {
        repository.mark_event_processed(ingest_event_id(action))?;
        Ok(TriggerOutcome::Declined)
    }
}

fn observe_event(
    actor: &mut BotActor<SqliteRepository, HerdrPaneAllocator>,
    kelpie: &KelpieClient,
    waiter: &HostWaiter<'_>,
    ingest: &mut RelayIngest<SqliteRepository>,
    event: &Event,
) -> Result<(), HostError> {
    if let Some(action) = ingest.ingest(event)? {
        let event_id = ingest_event_id(&action).clone();
        let outcome = match actor.handle_ingest(kelpie, waiter, &action, "") {
            Ok(outcome) => outcome,
            Err(error) => {
                eprintln!("occupant dispatch failed {} {error}", event_id.as_str());
                return Ok(());
            }
        };
        match action {
            IngestAction::TurnCandidate { .. } => {
                eprintln!("observed trigger {} {outcome:?}", event_id.as_str());
            }
            IngestAction::Edit { .. } | IngestAction::Delete { .. } => {
                eprintln!("observed ingest {} {outcome:?}", event_id.as_str());
            }
        }
    }
    Ok(())
}

async fn refresh_subscription(
    subscriber: &RelaySubscriber,
    operator_pubkey: &str,
    repository: &SqliteRepository,
    channel_ids: &[String],
    active_event_ids: &[EventId],
) -> Result<(), HostError> {
    subscriber
        .subscribe(
            operator_pubkey,
            channel_ids,
            active_event_ids,
            replay_since(repository)?,
        )
        .await?;
    Ok(())
}

struct RelayPoll {
    announced: bool,
    last_retry_error: Option<String>,
    last_queued_resume: Instant,
    last_channel_ids: Vec<String>,
    last_active_event_ids: Vec<EventId>,
}

fn note_retry(last_retry_error: &mut Option<String>, message: String) {
    if last_retry_error.as_ref() != Some(&message) {
        eprintln!("{message}");
        *last_retry_error = Some(message);
    }
}

async fn fetch_stored_events(
    subscriber: &RelaySubscriber,
    operator_pubkey: &str,
    channel_ids: &[String],
    active_event_ids: &[EventId],
    since: Timestamp,
) -> Result<Vec<Event>, RelaySubscribeError> {
    let mut events = subscriber.fetch_messages(operator_pubkey, since).await?;
    events.extend(
        subscriber
            .fetch_channel_messages(channel_ids, since)
            .await?,
    );
    events.extend(subscriber.fetch_mutations(active_event_ids, since).await?);
    Ok(events)
}

async fn poll_relay(
    actor: &mut BotActor<SqliteRepository, HerdrPaneAllocator>,
    kelpie: &KelpieClient,
    waiter: &HostWaiter<'_>,
    ingest: &mut RelayIngest<SqliteRepository>,
    subscriber: &RelaySubscriber,
    operator_pubkey: &str,
    poll: &mut RelayPoll,
) -> Result<(), HostError> {
    let scope = match actor.pending_scope() {
        Ok(scope) => scope,
        Err(error) => {
            note_retry(&mut poll.last_retry_error, error.to_string());
            return Ok(());
        }
    };
    if !poll.announced
        || scope.channel_ids != poll.last_channel_ids
        || scope.active_event_ids != poll.last_active_event_ids
    {
        match refresh_subscription(
            subscriber,
            operator_pubkey,
            ingest.repository_mut(),
            &scope.channel_ids,
            &scope.active_event_ids,
        )
        .await
        {
            Ok(()) => {
                poll.last_retry_error = None;
                poll.last_channel_ids.clone_from(&scope.channel_ids);
                poll.last_active_event_ids
                    .clone_from(&scope.active_event_ids);
                if !poll.announced {
                    eprintln!("botserver connected");
                    poll.announced = true;
                }
            }
            Err(error) => {
                note_retry(&mut poll.last_retry_error, error.to_string());
            }
        }
        if !poll.announced {
            return Ok(());
        }
    }
    // HTTP publishes on the local Buzz relay are stored immediately
    // but are not fanned out to operator #p websocket subscribers.
    let since = match replay_since(ingest.repository_mut()) {
        Ok(since) => since,
        Err(error) => {
            note_retry(
                &mut poll.last_retry_error,
                format!("relay replay cursor failed: {error}"),
            );
            return Ok(());
        }
    };
    let events = match fetch_stored_events(
        subscriber,
        operator_pubkey,
        &scope.channel_ids,
        &scope.active_event_ids,
        since,
    )
    .await
    {
        Ok(events) => events,
        Err(error) => {
            note_retry(&mut poll.last_retry_error, error.to_string());
            return Ok(());
        }
    };
    poll.last_retry_error = None;
    for event in events {
        observe_event(actor, kelpie, waiter, ingest, &event)?;
    }
    if poll.last_queued_resume.elapsed() >= RESUME_QUEUED_EVERY {
        if let Err(error) = actor.resume_queued(kelpie, waiter) {
            eprintln!("queued occupant resume failed: {error}");
        }
        poll.last_queued_resume = Instant::now();
    }
    Ok(())
}

async fn serve(
    operator: OperatorEnv,
    bot: Bot,
    repository: SqliteRepository,
    database: &Path,
) -> Result<(), HostError> {
    let operator_pubkey = operator.keys.public_key().to_hex();
    let kelpie = KelpieClient::default();
    let waiter = register_host_waiter(&kelpie, database)?;
    let mut inbox = spawn_inbox(
        default_socket(),
        waiter.identity().logical_agent_id().to_owned(),
    );
    let publisher = BuzzPublisher;
    let inbound_trigger = bot.inbound_trigger().to_owned();
    let mut actor = BotActor::new(bot, repository, HerdrPaneAllocator::default())
        .with_reactions(Arc::new(publisher));
    if let Err(error) = actor.resume_queued(&kelpie, &waiter) {
        eprintln!("queued occupant resume failed: {error}");
    }
    let client = Client::builder()
        .authenticator(SignerAuthenticator::new(operator.keys))
        .build();
    client.add_relay(&operator.relay_url).await?;
    client.connect().and_wait(CONNECT_TIMEOUT).await;
    let subscriber = RelaySubscriber::new(client);
    let mut notifications = pin!(subscriber.notifications());
    let mut ingest = RelayIngest::new(
        operator_pubkey.clone(),
        String::new(),
        SqliteRepository::open(database)?,
    )
    .with_inbound_trigger(inbound_trigger);
    let mut refresh = tokio::time::interval(SUBSCRIPTION_REFRESH);
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut poll = RelayPoll {
        announced: false,
        last_retry_error: None,
        last_queued_resume: Instant::now(),
        last_channel_ids: Vec::new(),
        last_active_event_ids: Vec::new(),
    };
    loop {
        tokio::select! {
            notification = notifications.next() => match notification {
                Some(ClientNotification::Event { event, .. }) => {
                    observe_event(&mut actor, &kelpie, &waiter, &mut ingest, &event)?;
                }
                Some(ClientNotification::Shutdown) => return Ok(()),
                Some(ClientNotification::Message { .. }) => {}
                None => return Err(HostError::NotificationClosed),
            },
            _ = refresh.tick() => {
                poll_relay(
                    &mut actor,
                    &kelpie,
                    &waiter,
                    &mut ingest,
                    &subscriber,
                    &operator_pubkey,
                    &mut poll,
                )
                .await?;
                if let Err(error) = actor.retry_outbound(&kelpie, &waiter, &publisher) {
                    eprintln!("outbound retry failed: {error}");
                }
            }
            delivery = inbox.recv() => match delivery {
                Some(delivery) => {
                    match actor.handle_occupant_delivery(&kelpie, &waiter, &publisher, &delivery) {
                        Ok(InboxAction::Ack) => inbox.ack(delivery.message_id()),
                        Ok(InboxAction::Hold) => {}
                        Err(error) => eprintln!("occupant delivery failed: {error}"),
                    }
                }
                None => return Err(HostError::InboxClosed),
            }
        }
    }
}

fn run(args: &Args) -> Result<(), HostError> {
    let (registry, repository) = load_host(&args.config, &args.database)?;
    if args.check {
        return Ok(());
    }
    let operator = OperatorEnv::from_env()?;
    let bots = registry.bots();
    let bot = bots.first().cloned().ok_or(HostError::NoBots)?;
    if bots.len() > 1 {
        let extra = bots[1..]
            .iter()
            .map(|bot| bot.id().as_str())
            .collect::<Vec<_>>()
            .join(", ");
        eprintln!("using first bot {}; ignoring {extra}", bot.id().as_str());
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(HostError::Runtime)?
        .block_on(serve(operator, bot, repository, &args.database))
}

fn main() -> ExitCode {
    match run(&Args::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, MutexGuard};
    use std::time::{SystemTime, UNIX_EPOCH};

    use botserver::HostRepository;
    use clap::{CommandFactory, Parser};
    use nostr_sdk::prelude::ToBech32;

    use super::*;

    static NEXT_TEMP_PATH: AtomicU64 = AtomicU64::new(0);
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn temp_path(label: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let sequence = NEXT_TEMP_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "botserver-main-{label}-{}-{timestamp}-{sequence}",
            std::process::id()
        ))
    }

    fn write_config(id: &str) -> PathBuf {
        let path = temp_path("config").with_extension("toml");
        fs::write(
            &path,
            format!(
                r#"
                [[bots]]
                id = "{id}"
                corpus = "/corpus/{id}"
                kind = "opencode"
                "#
            ),
        )
        .expect("write config");
        path
    }

    fn lock_env() -> MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    struct EnvRestore {
        key: Option<String>,
        url: Option<String>,
        pane: Option<String>,
    }

    impl EnvRestore {
        fn capture() -> Self {
            Self {
                key: std::env::var("BUZZ_PRIVATE_KEY").ok(),
                url: std::env::var("BUZZ_RELAY_URL").ok(),
                pane: std::env::var("HERDR_PANE_ID").ok(),
            }
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            restore_var("BUZZ_PRIVATE_KEY", self.key.as_deref());
            restore_var("BUZZ_RELAY_URL", self.url.as_deref());
            restore_var("HERDR_PANE_ID", self.pane.as_deref());
        }
    }

    fn restore_var(name: &str, value: Option<&str>) {
        match value {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
    }

    fn trigger_action(content: &str) -> IngestAction {
        IngestAction::TurnCandidate {
            event_id: botserver_domain::EventId::parse_hex(&"a".repeat(64)).expect("event"),
            channel_id: "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
            reply_to_event_id: None,
            trigger: botserver_domain::TriggerMatch::parse(
                "operator",
                "someone-else",
                ["operator"],
                "bot:",
                content,
            )
            .expect("trigger"),
        }
    }

    #[test]
    fn trigger_dispatch_queues_one_turn_for_the_first_bot() {
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let (registry, mut repository) = load_host(&config, &database).expect("load");
        let action = trigger_action("bot: hello");

        assert_eq!(
            dispatch_ingest(registry.bots(), &mut repository, &action).expect("dispatch"),
            TriggerOutcome::Queued
        );

        let channel = "ab12cd34-5678-90ab-cdef-0123456789ab";
        let turns = repository
            .turns_for_session(registry.bots()[0].id(), channel)
            .expect("turns");
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].state, botserver::TurnState::Queued);
        assert_eq!(turns[0].ask_id, None);
    }

    #[test]
    fn unnameable_channel_is_acknowledged_without_a_turn() {
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let (registry, mut repository) = load_host(&config, &database).expect("load");
        let event_id = botserver_domain::EventId::parse_hex(&"d".repeat(64)).expect("event");
        let action = IngestAction::TurnCandidate {
            event_id: event_id.clone(),
            channel_id: "not-a-uuid".to_owned(),
            reply_to_event_id: None,
            trigger: botserver_domain::TriggerMatch::parse(
                "operator",
                "someone-else",
                ["operator"],
                "bot:",
                "bot: hi",
            )
            .expect("trigger"),
        };

        assert_eq!(
            dispatch_ingest(registry.bots(), &mut repository, &action).expect("dispatch"),
            TriggerOutcome::Declined
        );
        assert!(repository.event_processed(&event_id).expect("processed"));
        assert!(repository
            .turns_for_session(registry.bots()[0].id(), "not-a-uuid")
            .expect("turns")
            .is_empty());
    }

    #[test]
    fn non_trigger_dispatch_does_not_create_a_turn() {
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let (registry, mut repository) = load_host(&config, &database).expect("load");
        let event_id = botserver_domain::EventId::parse_hex(&"b".repeat(64)).expect("event");
        let action = IngestAction::Delete {
            event_id: event_id.clone(),
            target_event_id: botserver_domain::EventId::parse_hex(&"c".repeat(64)).expect("target"),
        };

        assert_eq!(
            dispatch_ingest(registry.bots(), &mut repository, &action).expect("dispatch"),
            TriggerOutcome::Declined
        );
        assert!(repository.event_processed(&event_id).expect("processed"));
        assert!(repository
            .turns_for_session(
                registry.bots()[0].id(),
                "ab12cd34-5678-90ab-cdef-0123456789ab"
            )
            .expect("turns")
            .is_empty());
    }

    #[test]
    fn check_loads_config_and_sqlite_then_returns() {
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        run(&Args::parse_from([
            "botserver",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
            "--check",
        ]))
        .expect("check");

        let (registry, repository) = load_host(&config, &database).expect("reload");
        assert_eq!(registry.bots().len(), 1);
        assert_eq!(registry.bots()[0].id().as_str(), "bot");
        assert!(!repository
            .event_processed(
                &botserver_domain::EventId::parse_hex(&"a".repeat(64)).expect("event id")
            )
            .expect("schema"));
    }

    #[test]
    fn missing_config_is_an_error() {
        let config = temp_path("missing").with_extension("toml");
        let database = temp_path("host").with_extension("sqlite");
        let error = run(&Args::parse_from([
            "botserver",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
            "--check",
        ]))
        .expect_err("missing config");
        assert!(error.to_string().contains("failed to read bot config"));
    }

    #[test]
    fn invalid_registry_is_an_error() {
        let config = temp_path("invalid").with_extension("toml");
        fs::write(&config, "not = toml [[").expect("write");
        let database = temp_path("host").with_extension("sqlite");
        let error = run(&Args::parse_from([
            "botserver",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
            "--check",
        ]))
        .expect_err("invalid config");
        assert!(error.to_string().contains("invalid bot config"));
    }

    #[test]
    fn botserver_parser_rejects_envchain() {
        assert!(Args::try_parse_from([
            "botserver",
            "--config",
            "bots.toml",
            "--database",
            "host.sqlite",
            "--envchain",
            "botserver",
        ])
        .is_err());
        assert!(!Args::command()
            .render_long_help()
            .to_string()
            .contains("envchain"));
    }

    #[test]
    fn operator_env_reads_buzz_private_key_and_relay_url() {
        let _lock = lock_env();
        let _restore = EnvRestore::capture();
        let keys = Keys::generate();
        let secret = keys.secret_key().to_secret_hex();
        let relay_url = "ws://127.0.0.1:13001";
        std::env::set_var("BUZZ_PRIVATE_KEY", &secret);
        std::env::set_var("BUZZ_RELAY_URL", relay_url);

        let operator = OperatorEnv::from_env().expect("read env");
        assert_eq!(operator.relay_url, relay_url);
        assert_eq!(operator.keys.public_key(), keys.public_key());
    }

    #[test]
    fn sqlite_does_not_persist_an_nsec() {
        let _lock = lock_env();
        let _restore = EnvRestore::capture();
        let keys = Keys::generate();
        let secret = keys.secret_key().to_bech32().expect("nsec");
        std::env::set_var("BUZZ_PRIVATE_KEY", &secret);
        std::env::set_var("BUZZ_RELAY_URL", "ws://127.0.0.1:13001");
        OperatorEnv::from_env().expect("read env");

        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let (registry, mut repository) = load_host(&config, &database).expect("load");
        dispatch_ingest(
            registry.bots(),
            &mut repository,
            &trigger_action("bot: hello"),
        )
        .expect("dispatch");
        drop(repository);

        let bytes = fs::read(&database).expect("sqlite bytes");
        let haystack = String::from_utf8_lossy(&bytes);
        assert!(!haystack.contains(&secret));
        assert!(!haystack.contains("nsec1"));
    }

    #[test]
    fn check_does_not_require_operator_env() {
        let _lock = lock_env();
        let _restore = EnvRestore::capture();
        std::env::remove_var("BUZZ_PRIVATE_KEY");
        std::env::remove_var("BUZZ_RELAY_URL");
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        run(&Args::parse_from([
            "botserver",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
            "--check",
        ]))
        .expect("check");
    }

    #[test]
    fn runtime_requires_operator_key_from_env() {
        let _lock = lock_env();
        let _restore = EnvRestore::capture();
        std::env::remove_var("BUZZ_PRIVATE_KEY");
        std::env::set_var("BUZZ_RELAY_URL", "ws://127.0.0.1:13001");
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let error = run(&Args::parse_from([
            "botserver",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
        ]))
        .expect_err("missing key");
        assert_eq!(error.to_string(), "missing BUZZ_PRIVATE_KEY");
    }

    #[test]
    fn runtime_requires_relay_url_from_env() {
        let _lock = lock_env();
        let _restore = EnvRestore::capture();
        std::env::set_var("BUZZ_PRIVATE_KEY", "not-a-key");
        std::env::remove_var("BUZZ_RELAY_URL");
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let error = run(&Args::parse_from([
            "botserver",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
        ]))
        .expect_err("missing url");
        assert_eq!(error.to_string(), "missing BUZZ_RELAY_URL");
    }

    #[test]
    fn runtime_requires_a_configured_bot() {
        let _lock = lock_env();
        let _restore = EnvRestore::capture();
        let keys = Keys::generate();
        std::env::set_var("BUZZ_PRIVATE_KEY", keys.secret_key().to_secret_hex());
        std::env::set_var("BUZZ_RELAY_URL", "ws://127.0.0.1:13001");
        let config = temp_path("empty").with_extension("toml");
        fs::write(&config, "bots = []").expect("write");
        let database = temp_path("host").with_extension("sqlite");
        let error = run(&Args::parse_from([
            "botserver",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
        ]))
        .expect_err("no bots");
        assert_eq!(error.to_string(), "bot config has no bots");
    }

    #[test]
    fn check_does_not_need_a_waiter_pane() {
        let _lock = lock_env();
        let _restore = EnvRestore::capture();
        std::env::remove_var("HERDR_PANE_ID");
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        run(&Args::parse_from([
            "botserver",
            "--check",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
        ]))
        .expect("check");
    }

    #[test]
    fn invalid_operator_key_does_not_print_the_secret() {
        let _lock = lock_env();
        let _restore = EnvRestore::capture();
        let secret = "nsec1invalidsecretmustnotappear";
        std::env::set_var("BUZZ_PRIVATE_KEY", secret);
        std::env::set_var("BUZZ_RELAY_URL", "ws://127.0.0.1:13001");
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let error = run(&Args::parse_from([
            "botserver",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
        ]))
        .expect_err("invalid key");
        let message = error.to_string();
        assert_eq!(message, "invalid BUZZ_PRIVATE_KEY");
        assert!(!message.contains(secret));
        assert!(!format!("{error:?}").contains(secret));
    }

    #[test]
    fn generated_operator_key_is_accepted_without_logging_it() {
        let keys = Keys::generate();
        let parsed = Keys::parse(&keys.secret_key().to_secret_hex()).expect("parse");
        assert_eq!(parsed.public_key(), keys.public_key());
        assert!(!format!("{parsed:?}").contains("secret"));
    }
}
