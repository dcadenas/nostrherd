//! Host process: relay subscriber, per-bot actors, Kelpie waiter.

use std::fmt;
use std::fmt::Write as _;
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Args as ClapArgs, Parser, Subcommand};
use futures::StreamExt;
use nostr_sdk::prelude::{Client, ClientNotification, Event, Keys, SignerAuthenticator, Timestamp};
#[cfg(test)]
use nostrherd::actor::persist_ingest;
#[cfg(test)]
use nostrherd::actor::TriggerOutcome;
use nostrherd::actor::{ActorError, BotActor};
use nostrherd::config::{BotRegistry, ConfigError};
use nostrherd::herdr::{HerdrError, HerdrPaneAllocator};
use nostrherd::inbox::{default_socket, spawn_inbox, HostInbox, InboxDelivery};
use nostrherd::outbox::{BuzzPublisher, InFlightReaction, InboxAction};
use nostrherd::progress::{BackgroundProgressRelay, ProgressRelay};
use nostrherd::relay::{
    IngestAction, IngestError, RelayIngest, RelaySubscribeError, RelaySubscriber,
};
use nostrherd::sqlite::SqliteRepository;
use nostrherd::{
    unix_now, HostRepository, HostWaiter, KelpieClient, KelpieError, WAITER_IDEMPOTENCY_KEY,
};
use nostrherd_domain::{Bot, BotId, EventId};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const EMPTY_REPLAY_OVERLAP_SECS: u64 = 900;
const SUBSCRIPTION_REFRESH: Duration = Duration::from_secs(1);
const SUBSCRIPTION_FAILURE_NOTICE_INTERVAL: Duration = Duration::from_mins(5);
const RESUME_QUEUED_EVERY: Duration = Duration::from_secs(30);

#[derive(Debug, Parser)]
// `version` reads CARGO_PKG_VERSION, so an operator can always say which build
// they are running. Without it there is no way to tell a stale host from a
// current one, which is how a bot kept publishing an outdated stamp unnoticed.
#[command(version, about = "Host occupant and per-bot actors")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    /// Bot registry TOML. Defaults to `$XDG_CONFIG_HOME/nostrherd/bots.toml`.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Host `SQLite` database. Defaults to
    /// `$XDG_DATA_HOME/nostrherd/nostrherd.sqlite`.
    #[arg(long)]
    database: Option<PathBuf>,

    /// Load config and database, then exit.
    #[arg(long)]
    check: bool,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Scaffold a new bot corpus in a directory.
    Init(InitArgs),
}

#[derive(Debug, ClapArgs)]
struct InitArgs {
    /// Directory to create the corpus in. It must not already hold files.
    dir: PathBuf,

    /// Bot id, which is also its `{id}:` trigger. Prompted when omitted.
    #[arg(long)]
    id: Option<String>,

    /// Agent CLI Herdr launches for it, such as `opencode`. Prompted when omitted.
    #[arg(long)]
    kind: Option<String>,

    /// Registry TOML to register the bot in. Defaults to the conventional path.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Print the registry entry instead of writing it into the registry.
    #[arg(long)]
    print_only: bool,
}

struct OperatorEnv {
    keys: Keys,
    relay_url: String,
}

impl OperatorEnv {
    fn from_env() -> Result<Self, HostError> {
        let private_key = required_env("NOSTRHERD_PRIVATE_KEY")?;
        let relay_url = required_env("NOSTRHERD_RELAY_URL")?;
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
    MissingRegistry(PathBuf),
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
    Path(nostrherd::paths::PathError),
    Io {
        what: &'static str,
        error: std::io::Error,
    },
    MissingInitArg(String),
    Init(nostrherd::init::InitError),
    Prompt(std::io::Error),
    RegisteredId(String),
    RegisteredCorpus {
        id: String,
        corpus: PathBuf,
    },
}

impl fmt::Display for HostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "{error}"),
            Self::Path(error) => write!(formatter, "{error}"),
            Self::Io { what, error } => write!(formatter, "failed to {what}: {error}"),
            Self::MissingInitArg(label) => {
                write!(
                    formatter,
                    "{} is required without a terminal; run nostrherd init <dir> --id <id> --kind <agent-cli>",
                    if label == "bot id" { "--id" } else { "--kind" }
                )
            }
            Self::Init(error) => write!(formatter, "{error}"),
            Self::Prompt(error) => write!(formatter, "{error}"),
            Self::RegisteredId(id) => write!(
                formatter,
                "a bot with id {id:?} is already registered; choose a different --id, or use the existing bot"
            ),
            Self::RegisteredCorpus { id, corpus } => write!(
                formatter,
                "bot {:?} already uses corpus {}; choose a different empty directory for the new bot",
                id,
                corpus.display()
            ),
            Self::Database(error) => write!(formatter, "failed to open host database: {error}"),
            Self::MissingRegistry(path) => write!(formatter,
                "no bot registry at {}; create a bot with nostrherd init <dir> --config {}, or select an existing registry with --config",
                path.display(), shell_path(path)),
            Self::MissingEnv(name) => write!(formatter,
                "missing {name}; set it in the environment supplied to nostrherd (for example, through your secret manager), then run nostrherd again"),
            Self::InvalidOperatorKey => formatter.write_str(
                "invalid NOSTRHERD_PRIVATE_KEY; supply a hex or nsec secret key through the environment, not a command-line flag"),
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
            Self::NoBots => formatter.write_str("bot config has no bots; run nostrherd init <dir> to register one (use --config for a custom registry)"),
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
            Self::Runtime(error)
            | Self::WaiterKey(error)
            | Self::Prompt(error)
            | Self::Io { error, .. } => Some(error),
            Self::Relay(error) => Some(error),
            Self::Subscribe(error) => Some(error),
            Self::Ingest(error) => Some(error),
            Self::Herdr(error) => Some(error),
            Self::Kelpie(error) => Some(error),
            Self::Actor(error) => Some(error),
            Self::Path(error) => Some(error),
            Self::MissingEnv(_)
            | Self::MissingRegistry(_)
            | Self::InvalidOperatorKey
            | Self::NoBots
            | Self::NotificationClosed
            | Self::InboxClosed
            | Self::MissingInitArg(_)
            | Self::RegisteredId(_)
            | Self::RegisteredCorpus { .. }
            | Self::Init(_) => None,
        }
    }
}

impl From<ConfigError> for HostError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<nostrherd::paths::PathError> for HostError {
    fn from(error: nostrherd::paths::PathError) -> Self {
        Self::Path(error)
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
    let registry = BotRegistry::load(config).map_err(|error| match error {
        ConfigError::Io(ref io) if io.kind() == io::ErrorKind::NotFound => {
            HostError::MissingRegistry(config.to_path_buf())
        }
        error => HostError::Config(error),
    })?;
    // The conventional database directory does not exist before the first run,
    // and SQLite will not create it.
    if let Some(parent) = database
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|error| HostError::Io {
            what: "create the database directory",
            error,
        })?;
    }
    Ok((registry, SqliteRepository::open(database)?))
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
    let key = format!("nostrherd-host-waiter-{millis}");
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

fn action_bot_id(
    repository: &SqliteRepository,
    action: &IngestAction,
) -> Result<Option<BotId>, HostError> {
    match action {
        IngestAction::TurnCandidate { bot_id, .. } => Ok(Some(bot_id.clone())),
        IngestAction::Edit {
            target_event_id, ..
        }
        | IngestAction::Delete {
            target_event_id, ..
        } => Ok(repository
            .active_turn_for_event(target_event_id)?
            .map(|turn| turn.bot_id)),
    }
}

fn actor_for_bot<'a>(
    actors: &'a mut [BotActor<SqliteRepository, HerdrPaneAllocator>],
    bot_id: &BotId,
) -> Option<&'a mut BotActor<SqliteRepository, HerdrPaneAllocator>> {
    actors.iter_mut().find(|actor| actor.bot().id() == bot_id)
}

#[cfg(test)]
fn dispatch_ingest(
    bots: &[Bot],
    repository: &mut SqliteRepository,
    action: &IngestAction,
) -> Result<TriggerOutcome, HostError> {
    let bot = action_bot_id(repository, action)?
        .and_then(|bot_id| bots.iter().find(|bot| bot.id() == &bot_id));
    if let Some(bot) = bot {
        Ok(persist_ingest(bot, repository, action, "")?)
    } else {
        repository.mark_event_processed(ingest_event_id(action))?;
        Ok(TriggerOutcome::Declined)
    }
}

async fn channel_display_for(
    actor: &BotActor<SqliteRepository, HerdrPaneAllocator>,
    subscriber: &RelaySubscriber,
    operator_pubkey: &str,
    action: &IngestAction,
) -> String {
    let IngestAction::TurnCandidate {
        channel_id,
        trigger,
        ..
    } = action
    else {
        return String::new();
    };
    if trigger.request().is_empty() {
        return String::new();
    }
    match actor.has_session(channel_id) {
        Ok(true) => return String::new(),
        Ok(false) => {}
        Err(error) => eprintln!("session lookup failed: {error}"),
    }
    match subscriber.place_display(operator_pubkey, channel_id).await {
        Ok(display) if !display.is_empty() => display,
        Ok(_) => {
            eprintln!("place display missing for new session, using channel ID slug");
            String::new()
        }
        Err(error) => {
            eprintln!("place display lookup failed: {error}");
            String::new()
        }
    }
}

async fn observe_event(
    actors: &mut [BotActor<SqliteRepository, HerdrPaneAllocator>],
    kelpie: &KelpieClient,
    waiter: &HostWaiter<'_>,
    ingest: &mut RelayIngest<SqliteRepository>,
    subscriber: &RelaySubscriber,
    operator_pubkey: &str,
    event: &Event,
) -> Result<(), HostError> {
    let action = ingest.ingest(event)?;
    let event_id = EventId::parse_hex(&event.id.to_hex()).expect("SDK event ids are 32 bytes");
    if let Some(action) = action {
        let event_id = ingest_event_id(&action).clone();
        if let Some(bot_id) = action_bot_id(ingest.repository_mut(), &action)? {
            if let Some(actor) = actor_for_bot(actors, &bot_id) {
                let display =
                    channel_display_for(actor, subscriber, operator_pubkey, &action).await;
                match actor.handle_ingest(kelpie, waiter, &action, &display) {
                    Ok(outcome) => match action {
                        IngestAction::TurnCandidate { .. } => {
                            eprintln!("observed trigger {} {outcome:?}", event_id.as_str());
                        }
                        IngestAction::Edit { .. } | IngestAction::Delete { .. } => {
                            eprintln!("observed ingest {} {outcome:?}", event_id.as_str());
                        }
                    },
                    Err(error) => {
                        eprintln!("occupant dispatch failed {} {error}", event_id.as_str());
                    }
                }
            } else {
                ingest.repository_mut().mark_event_processed(&event_id)?;
            }
        } else {
            ingest.repository_mut().mark_event_processed(&event_id)?;
        }
    }
    let fires = ingest.record_watch_fires(&event_id, unix_now().map_err(HostError::Runtime)?)?;
    for fire in fires {
        let Some(actor) = actor_for_bot(actors, &fire.bot_id) else {
            continue;
        };
        match actor.handle_watch_fire(kelpie, waiter, &fire) {
            Ok(outcome) => eprintln!(
                "observed watch fire {} {outcome:?}",
                fire.wake_event_id.as_str()
            ),
            Err(error) => eprintln!(
                "watch wake dispatch failed {} {error}",
                fire.wake_event_id.as_str()
            ),
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
    watched_author_pubkeys: &[String],
) -> Result<(), HostError> {
    subscriber
        .subscribe(
            operator_pubkey,
            channel_ids,
            active_event_ids,
            watched_author_pubkeys,
            replay_since(repository)?,
        )
        .await?;
    Ok(())
}

struct RelayPoll {
    announced: bool,
    subscription_error: Option<SubscriptionErrorNotice>,
    last_retry_error: Option<String>,
    /// Deduped separately from `last_retry_error`, which a good fetch clears.
    ///
    /// Resuming queued work runs on its own timer, so an error here repeats at
    /// that cadence for as long as the cause lasts. Reporting each repeat once
    /// buried the one line that mattered under hundreds of identical ones.
    last_resume_error: Option<String>,
    last_queued_resume: Instant,
    last_channel_ids: Vec<String>,
    last_active_event_ids: Vec<EventId>,
    last_watched_author_pubkeys: Vec<String>,
}

struct SubscriptionErrorNotice {
    message: String,
    last_reported: Instant,
}

impl RelayPoll {
    fn subscription_refresh_needed(
        &self,
        channel_ids: &[String],
        active_event_ids: &[EventId],
        watched_author_pubkeys: &[String],
    ) -> bool {
        !self.announced
            || self.subscription_error.is_some()
            || channel_ids != self.last_channel_ids
            || active_event_ids != self.last_active_event_ids
            || watched_author_pubkeys != self.last_watched_author_pubkeys
    }
}

fn update_retry(last_retry_error: &mut Option<String>, message: String) -> Option<&str> {
    if last_retry_error.as_ref() == Some(&message) {
        None
    } else {
        *last_retry_error = Some(message);
        last_retry_error.as_deref()
    }
}

fn note_retry(last_retry_error: &mut Option<String>, message: String) {
    if let Some(message) = update_retry(last_retry_error, message) {
        eprintln!("{message}");
    }
}

fn update_subscription_error(
    notice: &mut Option<SubscriptionErrorNotice>,
    message: String,
    now: Instant,
) -> Option<&str> {
    let should_report = notice.as_ref().is_none_or(|previous| {
        previous.message != message
            || now.saturating_duration_since(previous.last_reported)
                >= SUBSCRIPTION_FAILURE_NOTICE_INTERVAL
    });
    if should_report {
        *notice = Some(SubscriptionErrorNotice {
            message,
            last_reported: now,
        });
        notice.as_ref().map(|current| current.message.as_str())
    } else {
        None
    }
}

fn note_subscription_error(notice: &mut Option<SubscriptionErrorNotice>, message: String) {
    if let Some(message) = update_subscription_error(notice, message, Instant::now()) {
        eprintln!("{message}");
    }
}

async fn fetch_stored_events(
    subscriber: &RelaySubscriber,
    operator_pubkey: &str,
    channel_ids: &[String],
    active_event_ids: &[EventId],
    watched_author_pubkeys: &[String],
    since: Timestamp,
) -> Result<Vec<Event>, RelaySubscribeError> {
    let mut events = subscriber.fetch_messages(operator_pubkey, since).await?;
    events.extend(
        subscriber
            .fetch_channel_messages(channel_ids, since)
            .await?,
    );
    events.extend(subscriber.fetch_mutations(active_event_ids, since).await?);
    events.extend(
        subscriber
            .fetch_watched_author_messages(watched_author_pubkeys, since)
            .await?,
    );
    Ok(events)
}

async fn poll_relay(
    actors: &mut [BotActor<SqliteRepository, HerdrPaneAllocator>],
    kelpie: &KelpieClient,
    waiter: &HostWaiter<'_>,
    ingest: &mut RelayIngest<SqliteRepository>,
    subscriber: &RelaySubscriber,
    operator_pubkey: &str,
    poll: &mut RelayPoll,
) -> Result<(), HostError> {
    let scope = match actors.first() {
        None => return Ok(()),
        Some(actor) => match actor.pending_scope() {
            Ok(scope) => scope,
            Err(error) => {
                note_retry(&mut poll.last_retry_error, error.to_string());
                return Ok(());
            }
        },
    };
    if poll.subscription_refresh_needed(
        &scope.channel_ids,
        &scope.active_event_ids,
        &scope.watched_author_pubkeys,
    ) {
        match refresh_subscription(
            subscriber,
            operator_pubkey,
            ingest.repository_mut(),
            &scope.channel_ids,
            &scope.active_event_ids,
            &scope.watched_author_pubkeys,
        )
        .await
        {
            Ok(()) => {
                poll.subscription_error = None;
                poll.last_channel_ids.clone_from(&scope.channel_ids);
                poll.last_active_event_ids
                    .clone_from(&scope.active_event_ids);
                poll.last_watched_author_pubkeys
                    .clone_from(&scope.watched_author_pubkeys);
                if !poll.announced {
                    eprintln!("nostrherd connected");
                    poll.announced = true;
                }
            }
            Err(error) => {
                note_subscription_error(&mut poll.subscription_error, error.to_string());
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
        &scope.watched_author_pubkeys,
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
        observe_event(
            actors,
            kelpie,
            waiter,
            ingest,
            subscriber,
            operator_pubkey,
            &event,
        )
        .await?;
    }
    if poll.last_queued_resume.elapsed() >= RESUME_QUEUED_EVERY {
        let mut failure = None;
        for actor in actors.iter_mut() {
            if let Err(error) = actor.resume_queued(kelpie, waiter) {
                failure = Some(format!("queued occupant resume failed: {error}"));
            }
        }
        match failure {
            Some(message) => note_retry(&mut poll.last_resume_error, message),
            None => poll.last_resume_error = None,
        }
        poll.last_queued_resume = Instant::now();
    }
    Ok(())
}

fn start_actors(
    bots: Vec<Bot>,
    operator_pubkey: &str,
    database: &Path,
    kelpie: &KelpieClient,
    waiter: &HostWaiter<'_>,
    publisher: &BuzzPublisher,
) -> Result<Vec<BotActor<SqliteRepository, HerdrPaneAllocator>>, HostError> {
    let reactions: Arc<dyn InFlightReaction> = Arc::new(publisher.clone());
    let progress_relay: Arc<dyn ProgressRelay> =
        Arc::new(BackgroundProgressRelay::new(Arc::new(publisher.clone())));
    let mut actors = bots
        .into_iter()
        .map(|bot| {
            SqliteRepository::open(database).map(|repository| {
                BotActor::new(
                    bot,
                    repository,
                    HerdrPaneAllocator::default(),
                    operator_pubkey.to_owned(),
                )
                .with_reactions(Arc::clone(&reactions))
                .with_progress_relay(Arc::clone(&progress_relay))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    for actor in &mut actors {
        if let Err(error) = actor.resume_queued(kelpie, waiter) {
            eprintln!("queued occupant resume failed: {error}");
        }
    }
    Ok(actors)
}

fn handle_host_delivery(
    actors: &mut [BotActor<SqliteRepository, HerdrPaneAllocator>],
    kelpie: &KelpieClient,
    waiter: &HostWaiter<'_>,
    publisher: &BuzzPublisher,
    inbox: &mut HostInbox,
    delivery: &InboxDelivery,
) -> Result<(), HostError> {
    let bot_id = {
        let Some(actor) = actors.first() else {
            return Err(HostError::NoBots);
        };
        match delivery.reply_to() {
            Some(ask_id) => actor.bot_id_for_ask(ask_id)?,
            None => None,
        }
    };
    let idx = bot_id
        .and_then(|id| actors.iter().position(|actor| actor.bot().id() == &id))
        .unwrap_or(0);
    let Some(actor) = actors.get_mut(idx) else {
        return Err(HostError::NoBots);
    };
    let action = actor.handle_occupant_delivery(kelpie, waiter, publisher, delivery);
    match action {
        Ok(InboxAction::Ack) => {
            if let Some(ask_id) = delivery.reply_to() {
                for actor in actors.iter_mut() {
                    if let Err(error) = actor.resume_if_posted(kelpie, waiter, ask_id) {
                        eprintln!("queued occupant resume failed: {error}");
                    }
                }
            }
            inbox.ack(delivery.message_id());
        }
        Ok(InboxAction::Hold) => {}
        Err(error) => eprintln!("occupant delivery failed: {error}"),
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn serve(operator: OperatorEnv, bots: Vec<Bot>, database: &Path) -> Result<(), HostError> {
    let operator_pubkey = operator.keys.public_key().to_hex();
    let kelpie = KelpieClient::default();
    let waiter = register_host_waiter(&kelpie, database)?;
    let mut inbox = spawn_inbox(
        default_socket(),
        waiter.identity().logical_agent_id().to_owned(),
    );
    let client = Client::builder()
        .authenticator(SignerAuthenticator::new(operator.keys.clone()))
        .build();
    client.add_relay(&operator.relay_url).await?;
    client.connect().and_wait(CONNECT_TIMEOUT).await;
    // Constructed inside the runtime so the sync actor layer can bridge
    // publishes onto the client (D43).
    let publisher = BuzzPublisher::new(
        client.clone(),
        operator.keys.clone(),
        operator.relay_url.clone(),
    );
    let mut actors = start_actors(
        bots,
        &operator_pubkey,
        database,
        &kelpie,
        &waiter,
        &publisher,
    )?;
    let bots = actors
        .iter()
        .map(|actor| actor.bot().clone())
        .collect::<Vec<_>>();
    let subscriber = RelaySubscriber::new(client);
    let mut notifications = pin!(subscriber.notifications());
    let mut ingest = RelayIngest::new(
        operator_pubkey.clone(),
        String::new(),
        SqliteRepository::open(database)?,
    )
    .with_bots(&bots);
    let mut refresh = tokio::time::interval(SUBSCRIPTION_REFRESH);
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut poll = RelayPoll {
        announced: false,
        subscription_error: None,
        last_retry_error: None,
        last_resume_error: None,
        last_queued_resume: Instant::now(),
        last_channel_ids: Vec::new(),
        last_active_event_ids: Vec::new(),
        last_watched_author_pubkeys: Vec::new(),
    };
    loop {
        tokio::select! {
            notification = notifications.next() => match notification {
                Some(ClientNotification::Event { event, .. }) => {
                    observe_event(
                        &mut actors,
                        &kelpie,
                        &waiter,
                        &mut ingest,
                        &subscriber,
                        &operator_pubkey,
                        &event,
                    ).await?;
                }
                Some(ClientNotification::Shutdown) => return Ok(()),
                Some(ClientNotification::Message { .. }) => {}
                None => return Err(HostError::NotificationClosed),
            },
            _ = refresh.tick() => {
                poll_relay(
                    &mut actors,
                    &kelpie,
                    &waiter,
                    &mut ingest,
                    &subscriber,
                    &operator_pubkey,
                    &mut poll,
                )
                .await?;
                for actor in &mut actors {
                    if let Err(error) = actor.retry_outbound(&kelpie, &waiter, &publisher) {
                        eprintln!("outbound retry failed: {error}");
                    }
                    // Progress relays on the tick, never in the delivery handler (D42).
                    if let Err(error) = actor.flush_progress(&publisher, unix_now().unwrap_or_default()) {
                        eprintln!("progress flush failed: {error}");
                    }
                }
            }
            delivery = inbox.recv() => match delivery {
                Some(delivery) => {
                    handle_host_delivery(
                        &mut actors,
                        &kelpie,
                        &waiter,
                        &publisher,
                        &mut inbox,
                        &delivery,
                    )?;
                }
                None => return Err(HostError::InboxClosed),
            }
        }
    }
}

fn run(args: &Args) -> Result<(), HostError> {
    if let Some(Command::Init(init)) = &args.command {
        return run_init(init);
    }
    let config = match args.config.clone() {
        Some(config) => config,
        None => nostrherd::paths::default_config()?,
    };
    let database = match args.database.clone() {
        Some(database) => database,
        None => nostrherd::paths::default_database()?,
    };
    let (config, database) = (&config, &database);
    let (registry, repository) = load_host(config, database)?;
    if args.check {
        return Ok(());
    }
    let operator = OperatorEnv::from_env()?;
    let bots = registry.bots().to_vec();
    if bots.is_empty() {
        return Err(HostError::NoBots);
    }
    drop(repository);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(HostError::Runtime)?
        .block_on(serve(operator, bots, database))
}

/// Scaffold a corpus and register it, so one command produces a working bot.
///
/// `--print-only` writes the entry to stdout instead, leaving the registry
/// untouched; the rest of the output goes to stderr so a redirect captures
/// only the TOML.
fn run_init(args: &InitArgs) -> Result<(), HostError> {
    // The registry entry records this path, and the host resolves it from its
    // own working directory, so a relative argument must not survive into it.
    let dir = std::path::absolute(&args.dir).map_err(|error| HostError::Init(error.into()))?;
    let default_id = dir
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned);
    let id = match args.id.as_deref() {
        Some(id) => id.to_owned(),
        None => prompt("bot id", default_id.as_deref())?,
    };
    let kind = match args.kind.as_deref() {
        Some(kind) => kind.to_owned(),
        None => prompt("agent kind", Some("opencode"))?,
    };
    let config = match args.config.clone() {
        Some(config) => config,
        None => nostrherd::paths::default_config()?,
    };
    // Check before writing anything, so a rejected bot leaves no half-made
    // corpus behind for the user to clean up.
    check_registry(&config, &id, &dir)?;
    let scaffold = nostrherd::init::write(&dir, &id, &kind).map_err(HostError::Init)?;
    eprintln!("Wrote {} files to {}", scaffold.files.len(), dir.display());
    if args.print_only {
        eprintln!();
        eprintln!("Add this entry to the registry the host runs with:");
        eprintln!();
        print!("{}", scaffold.registry_entry);
    } else {
        append_registry_entry(&config, &scaffold.registry_entry)?;
        eprintln!("Registered {:?} in {}", id, config.display());
    }
    let missing = ["NOSTRHERD_PRIVATE_KEY", "NOSTRHERD_RELAY_URL"]
        .into_iter()
        .filter(|name| required_env(name).is_err())
        .collect::<Vec<_>>();
    eprint!(
        "{}",
        init_guidance(&dir, &id, args.config.as_deref(), args.print_only, &missing)
    );
    Ok(())
}

fn shell_path(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}

fn init_guidance(
    dir: &Path,
    id: &str,
    config: Option<&Path>,
    print_only: bool,
    missing: &[&str],
) -> String {
    let mut text = format!(
        "\nNext: describe the bot in {}.\n",
        dir.join("AGENTS.md").display()
    );
    if !missing.is_empty() {
        let _ = writeln!(
            text,
            "Before starting: supply {} through your secret manager. No key flag is accepted.",
            missing.join(" and ")
        );
    }
    text.push_str("Keep Herdr and kelpied running; start kelpied in another terminal if needed.\n");
    if print_only {
        text.push_str("After adding the printed entry to your registry, run:\n");
    } else {
        text.push_str("Then run:\n");
    }
    let flags = config.map_or_else(String::new, |path| {
        format!(" --config {}", shell_path(path))
    });
    let _ = write!(text,
        "  nostrherd{flags} --check\n  nostrherd{flags}\nFrom your configured account, say \"{id}: hello\" in a channel it can post in.\n\nOnly you can wake this bot by default. To let others ask, see \"Let others ask\" in {} (allowed_requesters).\n",
        dir.join("README.md").display()
    );
    text
}

/// Append one entry to the registry, creating it and its directory if needed.
///
/// Separated from the surrounding blank line by construction, so repeated
/// appends stay readable in the file a person edits by hand.
fn append_registry_entry(config: &Path, entry: &str) -> Result<(), HostError> {
    if let Some(parent) = config.parent() {
        fs::create_dir_all(parent).map_err(|error| HostError::Io {
            what: "create the registry directory",
            error,
        })?;
    }
    let existing = match fs::read_to_string(config) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
        Err(error) => {
            return Err(HostError::Io {
                what: "read the registry",
                error,
            })
        }
    };
    let mut updated = String::new();
    let kept = existing.trim_end();
    if !kept.is_empty() {
        updated.push_str(kept);
        updated.push_str("\n\n");
    }
    updated.push_str(entry);
    fs::write(config, updated).map_err(|error| HostError::Io {
        what: "write the registry",
        error,
    })
}

/// Refuse a scaffold that an existing registry would reject, or that would
/// share a corpus with a bot already in it.
///
/// A missing or empty registry is not an error. The first bot is scaffolded
/// before the file it will be registered in has any bots, and `>> bots.toml`
/// creates that file empty before this runs.
fn check_registry(config: &Path, id: &str, dir: &Path) -> Result<(), HostError> {
    let text = match fs::read_to_string(config) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(HostError::Config(ConfigError::Io(error))),
    };
    if text.trim().is_empty() {
        return Ok(());
    }
    let registry = BotRegistry::from_toml(&text)?;
    for bot in registry.bots() {
        if bot.id().as_str() == id {
            return Err(HostError::RegisteredId(id.to_owned()));
        }
        // Compare resolved paths so `./mybot` and an absolute entry for the
        // same directory are recognised as the same corpus.
        let registered = std::path::absolute(bot.corpus_path());
        if registered.is_ok_and(|registered| registered == dir) {
            return Err(HostError::RegisteredCorpus {
                id: bot.id().as_str().to_owned(),
                corpus: bot.corpus_path().to_path_buf(),
            });
        }
    }
    Ok(())
}

/// Ask for one value on a terminal. Without a terminal the flag is required,
/// so scripted use fails loudly instead of blocking on a prompt nobody sees.
fn prompt(label: &str, default: Option<&str>) -> Result<String, HostError> {
    if !io::stdin().is_terminal() {
        return Err(HostError::MissingInitArg(label.to_owned()));
    }
    loop {
        match default {
            Some(default) => eprint!("{label} [{default}]: "),
            None => eprint!("{label}: "),
        }
        io::stderr().flush().map_err(HostError::Prompt)?;
        let mut line = String::new();
        if io::stdin()
            .read_line(&mut line)
            .map_err(HostError::Prompt)?
            == 0
        {
            return Err(HostError::MissingInitArg(label.to_owned()));
        }
        let line = line.trim();
        if !line.is_empty() {
            return Ok(line.to_owned());
        }
        if let Some(default) = default {
            return Ok(default.to_owned());
        }
    }
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

    use clap::{CommandFactory, Parser};
    use nostr_sdk::prelude::ToBech32;
    use nostrherd::HostRepository;

    use super::*;

    static NEXT_TEMP_PATH: AtomicU64 = AtomicU64::new(0);
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn init_guidance_shrinks_when_environment_is_supplied() {
        let dir = Path::new("/bots/mybot");
        let missing = init_guidance(
            dir,
            "mybot",
            None,
            false,
            &["NOSTRHERD_PRIVATE_KEY", "NOSTRHERD_RELAY_URL"],
        );
        assert!(missing.contains("NOSTRHERD_PRIVATE_KEY and NOSTRHERD_RELAY_URL"));
        assert!(missing.contains("secret manager"));
        let ready = init_guidance(dir, "mybot", None, false, &[]);
        assert!(!ready.contains("NOSTRHERD_PRIVATE_KEY"));
        assert!(!ready.contains("NOSTRHERD_RELAY_URL"));
        assert!(ready.contains("Herdr and kelpied"));
        assert!(ready.contains("  nostrherd --check\n  nostrherd\n"));
        assert!(ready.contains("mybot: hello"));
        assert!(ready.contains("allowed_requesters"));
        assert!(ready.contains("/bots/mybot/README.md"));
        let partial = init_guidance(dir, "mybot", None, false, &["NOSTRHERD_RELAY_URL"]);
        assert!(!partial.contains("NOSTRHERD_PRIVATE_KEY"));
        assert!(partial.contains("NOSTRHERD_RELAY_URL"));
    }

    #[test]
    fn init_guidance_preserves_custom_registry_and_print_only_order() {
        let text = init_guidance(
            Path::new("/bots/test"),
            "test",
            Some(Path::new("/tmp/bot's $config.toml")),
            true,
            &[],
        );
        assert!(text.contains("After adding the printed entry"));
        assert!(text.contains("nostrherd --config '/tmp/bot'\\''s $config.toml' --check"));
        assert!(!text.contains("Registered"));
    }

    #[test]
    fn setup_errors_name_the_next_action() {
        assert!(HostError::MissingInitArg("bot id".into())
            .to_string()
            .starts_with("--id"));
        assert!(HostError::MissingInitArg("agent kind".into())
            .to_string()
            .starts_with("--kind"));
        assert!(HostError::InvalidOperatorKey
            .to_string()
            .contains("hex or nsec"));
        assert!(HostError::NoBots.to_string().contains("nostrherd init"));
        let config = temp_path("missing-registry");
        let database = temp_path("missing-database").join("host.sqlite");
        let error = load_host(&config, &database).unwrap_err();
        assert!(matches!(error, HostError::MissingRegistry(_)));
        assert!(error.to_string().contains(&shell_path(&config)));
        assert!(!database.parent().unwrap().exists());
    }

    fn temp_path(label: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let sequence = NEXT_TEMP_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "nostrherd-main-{label}-{}-{timestamp}-{sequence}",
            std::process::id()
        ))
    }

    fn write_config(id: &str) -> PathBuf {
        write_bots_config(&[id])
    }

    fn write_bots_config(ids: &[&str]) -> PathBuf {
        let path = temp_path("config").with_extension("toml");
        let mut body = String::new();
        for id in ids {
            body.push_str("[[bots]]\n");
            body.push_str("id = \"");
            body.push_str(id);
            body.push_str("\"\ncorpus = \"/corpus/");
            body.push_str(id);
            body.push_str("\"\nkind = \"opencode\"\n");
        }
        fs::write(&path, body).expect("write config");
        path
    }

    fn lock_env() -> MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[test]
    fn persistent_retry_notice_is_emitted_once_until_the_error_changes() {
        let mut last = None;

        assert_eq!(
            update_retry(&mut last, "first failure".to_owned()),
            Some("first failure")
        );
        assert_eq!(update_retry(&mut last, "first failure".to_owned()), None);
        assert_eq!(
            update_retry(&mut last, "different failure".to_owned()),
            Some("different failure")
        );
    }

    #[test]
    fn persistent_subscription_failure_is_reported_at_a_bounded_interval() {
        let mut notice = None;
        let started = Instant::now();

        assert_eq!(
            update_subscription_error(&mut notice, "refresh failed".to_owned(), started),
            Some("refresh failed")
        );
        assert_eq!(
            update_subscription_error(
                &mut notice,
                "refresh failed".to_owned(),
                started + SUBSCRIPTION_REFRESH,
            ),
            None
        );
        assert_eq!(
            update_subscription_error(
                &mut notice,
                "refresh failed".to_owned(),
                started + SUBSCRIPTION_FAILURE_NOTICE_INTERVAL,
            ),
            Some("refresh failed")
        );

        notice = None;
        assert_eq!(
            update_subscription_error(
                &mut notice,
                "refresh failed".to_owned(),
                started + SUBSCRIPTION_REFRESH,
            ),
            Some("refresh failed")
        );
    }

    #[test]
    fn subscription_refresh_gate_covers_scope_errors_and_idle() {
        let channel_ids = vec!["channel-a".to_owned()];
        let active_event_ids = vec![EventId::parse_hex(&"a".repeat(64)).expect("event")];
        let mut poll = RelayPoll {
            announced: true,
            subscription_error: Some(SubscriptionErrorNotice {
                message: "refresh failed".to_owned(),
                last_reported: Instant::now(),
            }),
            last_retry_error: None,
            last_resume_error: None,
            last_queued_resume: Instant::now(),
            last_channel_ids: channel_ids.clone(),
            last_active_event_ids: active_event_ids.clone(),
            last_watched_author_pubkeys: Vec::new(),
        };

        assert!(poll.subscription_refresh_needed(&channel_ids, &active_event_ids, &[]));
        poll.subscription_error = None;
        assert!(!poll.subscription_refresh_needed(&channel_ids, &active_event_ids, &[]));
        poll.announced = false;
        assert!(poll.subscription_refresh_needed(&channel_ids, &active_event_ids, &[]));
        poll.announced = true;
        assert!(poll.subscription_refresh_needed(
            &["channel-b".to_owned()],
            &active_event_ids,
            &[]
        ));
        let other_event = EventId::parse_hex(&"b".repeat(64)).expect("event");
        assert!(poll.subscription_refresh_needed(&channel_ids, &[other_event], &[]));
        assert!(poll.subscription_refresh_needed(
            &channel_ids,
            &active_event_ids,
            &["c".repeat(64)]
        ));
    }

    struct EnvRestore {
        key: Option<String>,
        url: Option<String>,
        pane: Option<String>,
    }

    impl EnvRestore {
        fn capture() -> Self {
            Self {
                key: std::env::var("NOSTRHERD_PRIVATE_KEY").ok(),
                url: std::env::var("NOSTRHERD_RELAY_URL").ok(),
                pane: std::env::var("HERDR_PANE_ID").ok(),
            }
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            restore_var("NOSTRHERD_PRIVATE_KEY", self.key.as_deref());
            restore_var("NOSTRHERD_RELAY_URL", self.url.as_deref());
            restore_var("HERDR_PANE_ID", self.pane.as_deref());
        }
    }

    fn restore_var(name: &str, value: Option<&str>) {
        match value {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
    }

    fn trigger_action(bot_id: &str, event_char: char, content: &str) -> IngestAction {
        let inbound = format!("{bot_id}:");
        IngestAction::TurnCandidate {
            bot_id: BotId::new(bot_id).expect("id"),
            event_id: nostrherd_domain::EventId::parse_hex(&event_char.to_string().repeat(64))
                .expect("event"),
            channel_id: "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
            reply_to_event_id: None,
            trigger: nostrherd_domain::TriggerMatch::parse(
                "operator",
                "someone-else",
                ["operator"],
                &inbound,
                content,
            )
            .expect("trigger"),
        }
    }

    fn dispatch_ingest(
        bots: &[Bot],
        repository: &mut SqliteRepository,
        action: &IngestAction,
    ) -> Result<TriggerOutcome, HostError> {
        if let IngestAction::TurnCandidate {
            event_id,
            channel_id,
            trigger,
            ..
        } = action
        {
            repository.index_event(
                &nostrherd::IndexedRelayEvent {
                    event_id: event_id.clone(),
                    author_pubkey: "a".repeat(64),
                    created_at: 1,
                    kind: 9,
                    content: trigger.request().to_owned(),
                    tags_json: "[]".to_owned(),
                    channel_id: Some(channel_id.clone()),
                    target_event_id: None,
                },
                true,
            )?;
        }
        super::dispatch_ingest(bots, repository, action)
    }

    #[test]
    fn trigger_dispatch_queues_one_turn_for_the_first_bot() {
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let (registry, mut repository) = load_host(&config, &database).expect("load");
        let action = trigger_action("bot", 'a', "bot: hello");

        assert_eq!(
            dispatch_ingest(registry.bots(), &mut repository, &action).expect("dispatch"),
            TriggerOutcome::Queued
        );

        let channel = "ab12cd34-5678-90ab-cdef-0123456789ab";
        let turns = repository
            .turns_for_session(registry.bots()[0].id(), channel)
            .expect("turns");
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].state, nostrherd::TurnState::Queued);
        assert_eq!(turns[0].ask_id, None);
    }

    #[test]
    fn inbound_tokens_queue_separate_sessions_for_each_bot() {
        let config = write_bots_config(&["bot", "pr"]);
        let database = temp_path("host").with_extension("sqlite");
        let (registry, mut repository) = load_host(&config, &database).expect("load");
        let channel = "ab12cd34-5678-90ab-cdef-0123456789ab";

        assert_eq!(
            dispatch_ingest(
                registry.bots(),
                &mut repository,
                &trigger_action("bot", 'a', "bot: hello")
            )
            .expect("bot"),
            TriggerOutcome::Queued
        );
        assert_eq!(
            dispatch_ingest(
                registry.bots(),
                &mut repository,
                &trigger_action("pr", 'e', "pr: hello")
            )
            .expect("pr"),
            TriggerOutcome::Queued
        );

        let bot = registry.get(&BotId::new("bot").expect("id")).expect("bot");
        let pr = registry.get(&BotId::new("pr").expect("id")).expect("pr");
        assert_eq!(
            repository
                .turns_for_session(bot.id(), channel)
                .expect("bot turns")
                .len(),
            1
        );
        assert_eq!(
            repository
                .turns_for_session(pr.id(), channel)
                .expect("pr turns")
                .len(),
            1
        );
        let bot_session = repository.session(bot.id(), channel).expect("bot session");
        let pr_session = repository.session(pr.id(), channel).expect("pr session");
        let bot_session = bot_session.expect("bot bound");
        let pr_session = pr_session.expect("pr bound");
        assert_ne!(bot_session.session_name, pr_session.session_name);
        assert!(bot_session.session_name.starts_with("bot-"));
        assert!(pr_session.session_name.starts_with("pr-"));
    }

    #[test]
    fn action_bot_id_follows_the_active_turn_for_edits() {
        let config = write_bots_config(&["bot", "pr"]);
        let database = temp_path("host").with_extension("sqlite");
        let (registry, mut repository) = load_host(&config, &database).expect("load");
        dispatch_ingest(
            registry.bots(),
            &mut repository,
            &trigger_action("pr", 'e', "pr: hello"),
        )
        .expect("pr");
        let target = nostrherd_domain::EventId::parse_hex(&"e".repeat(64)).expect("target");
        let edit = IngestAction::Edit {
            event_id: nostrherd_domain::EventId::parse_hex(&"f".repeat(64)).expect("edit"),
            target_event_id: target,
            replacement: None,
        };
        assert_eq!(
            action_bot_id(&repository, &edit)
                .expect("bot id")
                .expect("present")
                .as_str(),
            "pr"
        );
        assert_eq!(
            action_bot_id(&repository, &trigger_action("bot", 'a', "bot: hello"))
                .expect("candidate")
                .expect("present")
                .as_str(),
            "bot"
        );
    }

    #[test]
    fn actor_for_bot_selects_the_matching_actor() {
        let config = write_bots_config(&["bot", "pr"]);
        let database = temp_path("host").with_extension("sqlite");
        let (registry, _) = load_host(&config, &database).expect("load");
        let mut actors = registry
            .bots()
            .iter()
            .map(|bot| {
                BotActor::new(
                    bot.clone(),
                    SqliteRepository::open(&database).expect("db"),
                    HerdrPaneAllocator::default(),
                    "a".repeat(64),
                )
            })
            .collect::<Vec<_>>();
        let pr = BotId::new("pr").expect("id");
        assert_eq!(
            actor_for_bot(&mut actors, &pr)
                .expect("actor")
                .bot()
                .id()
                .as_str(),
            "pr"
        );
        assert!(actor_for_bot(&mut actors, &BotId::new("review").expect("id")).is_none());
    }

    #[test]
    fn non_uuid_channel_dispatch_queues_a_turn() {
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let (registry, mut repository) = load_host(&config, &database).expect("load");
        let event_id = nostrherd_domain::EventId::parse_hex(&"d".repeat(64)).expect("event");
        let action = IngestAction::TurnCandidate {
            bot_id: BotId::new("bot").expect("id"),
            event_id: event_id.clone(),
            channel_id: "not-a-uuid".to_owned(),
            reply_to_event_id: None,
            trigger: nostrherd_domain::TriggerMatch::parse(
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
            TriggerOutcome::Queued
        );
        assert!(repository.event_processed(&event_id).expect("processed"));
        let turns = repository
            .turns_for_session(registry.bots()[0].id(), "not-a-uuid")
            .expect("turns");
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].state, nostrherd::TurnState::Queued);
    }

    #[test]
    fn non_trigger_dispatch_does_not_create_a_turn() {
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let (registry, mut repository) = load_host(&config, &database).expect("load");
        let event_id = nostrherd_domain::EventId::parse_hex(&"b".repeat(64)).expect("event");
        let action = IngestAction::Delete {
            event_id: event_id.clone(),
            target_event_id: nostrherd_domain::EventId::parse_hex(&"c".repeat(64)).expect("target"),
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
            "nostrherd",
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
                &nostrherd_domain::EventId::parse_hex(&"a".repeat(64)).expect("event id")
            )
            .expect("schema"));
    }

    #[test]
    fn missing_config_is_an_error() {
        let config = temp_path("missing").with_extension("toml");
        let database = temp_path("host").with_extension("sqlite");
        let error = run(&Args::parse_from([
            "nostrherd",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
            "--check",
        ]))
        .expect_err("missing config");
        assert!(matches!(error, HostError::MissingRegistry(_)));
        assert!(error.to_string().contains("nostrherd init"));
    }

    #[test]
    fn invalid_registry_is_an_error() {
        let config = temp_path("invalid").with_extension("toml");
        fs::write(&config, "not = toml [[").expect("write");
        let database = temp_path("host").with_extension("sqlite");
        let error = run(&Args::parse_from([
            "nostrherd",
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
    fn version_reports_the_crate_version() {
        // An operator must be able to say which build is running; a stale host
        // is otherwise indistinguishable from a current one.
        let error = Args::try_parse_from(["nostrherd", "--version"]).expect_err("version exits");
        assert_eq!(error.kind(), clap::error::ErrorKind::DisplayVersion);
        assert!(
            error.to_string().contains(env!("CARGO_PKG_VERSION")),
            "{error}"
        );
    }

    #[test]
    fn nostrherd_parser_rejects_envchain() {
        assert!(Args::try_parse_from([
            "nostrherd",
            "--config",
            "bots.toml",
            "--database",
            "host.sqlite",
            "--envchain",
            "nostrherd",
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
        std::env::set_var("NOSTRHERD_PRIVATE_KEY", &secret);
        std::env::set_var("NOSTRHERD_RELAY_URL", relay_url);

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
        std::env::set_var("NOSTRHERD_PRIVATE_KEY", &secret);
        std::env::set_var("NOSTRHERD_RELAY_URL", "ws://127.0.0.1:13001");
        OperatorEnv::from_env().expect("read env");

        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let (registry, mut repository) = load_host(&config, &database).expect("load");
        dispatch_ingest(
            registry.bots(),
            &mut repository,
            &trigger_action("bot", 'a', "bot: hello"),
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
        std::env::remove_var("NOSTRHERD_PRIVATE_KEY");
        std::env::remove_var("NOSTRHERD_RELAY_URL");
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        run(&Args::parse_from([
            "nostrherd",
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
        std::env::remove_var("NOSTRHERD_PRIVATE_KEY");
        std::env::set_var("NOSTRHERD_RELAY_URL", "ws://127.0.0.1:13001");
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let error = run(&Args::parse_from([
            "nostrherd",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
        ]))
        .expect_err("missing key");
        assert!(matches!(
            error,
            HostError::MissingEnv("NOSTRHERD_PRIVATE_KEY")
        ));
        assert!(error.to_string().contains("secret manager"));
    }

    #[test]
    fn runtime_requires_relay_url_from_env() {
        let _lock = lock_env();
        let _restore = EnvRestore::capture();
        std::env::set_var("NOSTRHERD_PRIVATE_KEY", "not-a-key");
        std::env::remove_var("NOSTRHERD_RELAY_URL");
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let error = run(&Args::parse_from([
            "nostrherd",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
        ]))
        .expect_err("missing url");
        assert!(matches!(
            error,
            HostError::MissingEnv("NOSTRHERD_RELAY_URL")
        ));
        assert!(error
            .to_string()
            .contains("environment supplied to nostrherd"));
    }

    #[test]
    fn runtime_requires_a_configured_bot() {
        let _lock = lock_env();
        let _restore = EnvRestore::capture();
        let keys = Keys::generate();
        std::env::set_var("NOSTRHERD_PRIVATE_KEY", keys.secret_key().to_secret_hex());
        std::env::set_var("NOSTRHERD_RELAY_URL", "ws://127.0.0.1:13001");
        let config = temp_path("empty").with_extension("toml");
        fs::write(&config, "bots = []").expect("write");
        let database = temp_path("host").with_extension("sqlite");
        let error = run(&Args::parse_from([
            "nostrherd",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
        ]))
        .expect_err("no bots");
        assert!(matches!(error, HostError::NoBots));
        assert!(error.to_string().contains("nostrherd init"));
    }

    #[test]
    fn check_does_not_need_a_waiter_pane() {
        let _lock = lock_env();
        let _restore = EnvRestore::capture();
        std::env::remove_var("HERDR_PANE_ID");
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        run(&Args::parse_from([
            "nostrherd",
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
        std::env::set_var("NOSTRHERD_PRIVATE_KEY", secret);
        std::env::set_var("NOSTRHERD_RELAY_URL", "ws://127.0.0.1:13001");
        let config = write_config("bot");
        let database = temp_path("host").with_extension("sqlite");
        let error = run(&Args::parse_from([
            "nostrherd",
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
        ]))
        .expect_err("invalid key");
        let message = error.to_string();
        assert!(matches!(error, HostError::InvalidOperatorKey));
        assert!(message.starts_with("invalid NOSTRHERD_PRIVATE_KEY;"));
        assert!(message.contains("hex or nsec"));
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
