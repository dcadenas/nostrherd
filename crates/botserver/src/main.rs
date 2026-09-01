//! Host process: relay subscriber, per-bot actors, Kelpie waiter.

use std::fmt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use botserver::config::{BotRegistry, ConfigError};
use botserver::relay::{
    IngestAction, IngestError, RelayIngest, RelaySubscribeError, RelaySubscriber,
};
use botserver::sqlite::SqliteRepository;
use botserver::HostRepository;
use botserver_domain::EventId;
use clap::Parser;
use nostr_sdk::prelude::{Client, Event, Keys, RelayMessage, RelayPoolNotification, Timestamp};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const EMPTY_REPLAY_OVERLAP_SECS: u64 = 900;
const SUBSCRIPTION_REFRESH: Duration = Duration::from_secs(1);

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
    relay_pubkey: String,
}

impl OperatorEnv {
    fn from_env() -> Result<Self, HostError> {
        let private_key = required_env("BUZZ_PRIVATE_KEY")?;
        let relay_url = required_env("BUZZ_RELAY_URL")?;
        let relay_pubkey = std::env::var("BUZZ_RELAY_PUBKEY")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        let keys = Keys::parse(&private_key).map_err(|_| HostError::InvalidOperatorKey)?;
        Ok(Self {
            keys,
            relay_url,
            relay_pubkey,
        })
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
    Relay(nostr_sdk::client::Error),
    Subscribe(RelaySubscribeError),
    Ingest(IngestError<rusqlite::Error>),
    NotificationClosed,
}

impl fmt::Display for HostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "{error}"),
            Self::Database(error) => write!(formatter, "failed to open host database: {error}"),
            Self::MissingEnv(name) => write!(formatter, "missing {name}"),
            Self::InvalidOperatorKey => formatter.write_str("invalid BUZZ_PRIVATE_KEY"),
            Self::Runtime(error) => write!(formatter, "failed to start async runtime: {error}"),
            Self::Relay(error) => write!(formatter, "relay client failed: {error}"),
            Self::Subscribe(error) => write!(formatter, "{error}"),
            Self::Ingest(error) => write!(formatter, "{error}"),
            Self::NotificationClosed => formatter.write_str("relay notification channel closed"),
        }
    }
}

impl std::error::Error for HostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Database(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Relay(error) => Some(error),
            Self::Subscribe(error) => Some(error),
            Self::Ingest(error) => Some(error),
            Self::MissingEnv(_) | Self::InvalidOperatorKey | Self::NotificationClosed => None,
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

impl From<nostr_sdk::client::Error> for HostError {
    fn from(error: nostr_sdk::client::Error) -> Self {
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

fn load_host(config: &Path, database: &Path) -> Result<(BotRegistry, SqliteRepository), HostError> {
    Ok((
        BotRegistry::load(config)?,
        SqliteRepository::open(database)?,
    ))
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

fn observe_event(
    ingest: &mut RelayIngest<SqliteRepository>,
    event: &Event,
) -> Result<(), HostError> {
    if let Some(action) = ingest.ingest(event)? {
        let event_id = ingest_event_id(&action).clone();
        match action {
            IngestAction::TurnCandidate { .. } => {
                eprintln!("observed trigger {}", event_id.as_str());
            }
            IngestAction::Edit { .. } | IngestAction::Delete { .. } => {
                eprintln!("observed ingest {}", event_id.as_str());
            }
        }
        ingest.repository_mut().mark_event_processed(&event_id)?;
    }
    Ok(())
}

async fn refresh_subscription(
    subscriber: &RelaySubscriber,
    operator_pubkey: &str,
    repository: &SqliteRepository,
) -> Result<(), HostError> {
    subscriber
        .subscribe(operator_pubkey, &[], &[], replay_since(repository)?)
        .await?;
    Ok(())
}

async fn serve(operator: OperatorEnv, repository: SqliteRepository) -> Result<(), HostError> {
    let operator_pubkey = operator.keys.public_key.to_hex();
    let client = Client::new(operator.keys);
    client.add_relay(&operator.relay_url).await?;
    client.connect().await;
    client.wait_for_connection(CONNECT_TIMEOUT).await;
    let subscriber = RelaySubscriber::new(client);
    let mut notifications = subscriber.notifications();
    refresh_subscription(&subscriber, &operator_pubkey, &repository).await?;
    eprintln!("botserver connected");
    let mut ingest = RelayIngest::new(operator_pubkey.clone(), operator.relay_pubkey, repository);
    let mut refresh = tokio::time::interval(SUBSCRIPTION_REFRESH);
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            notification = notifications.recv() => match notification {
                Ok(RelayPoolNotification::Event { event, .. }) => {
                    observe_event(&mut ingest, &event)?;
                }
                Ok(RelayPoolNotification::Message {
                    message: RelayMessage::Closed { message, .. },
                    ..
                }) if message.contains("auth-required") => {
                    refresh_subscription(
                        &subscriber,
                        &operator_pubkey,
                        ingest.repository_mut(),
                    )
                    .await?;
                }
                Ok(RelayPoolNotification::Shutdown) => return Ok(()),
                Ok(RelayPoolNotification::Message { .. })
                | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(HostError::NotificationClosed);
                }
            },
            _ = refresh.tick() => {
                // HTTP publishes on the local Buzz relay are stored immediately
                // but are not fanned out to operator #p websocket subscribers.
                refresh_subscription(
                    &subscriber,
                    &operator_pubkey,
                    ingest.repository_mut(),
                )
                .await?;
            }
        }
    }
}

fn run(args: &Args) -> Result<(), HostError> {
    let (_registry, repository) = load_host(&args.config, &args.database)?;
    if args.check {
        return Ok(());
    }
    let operator = OperatorEnv::from_env()?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(HostError::Runtime)?
        .block_on(serve(operator, repository))
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
    use clap::Parser;

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
        relay_pubkey: Option<String>,
    }

    impl EnvRestore {
        fn capture() -> Self {
            Self {
                key: std::env::var("BUZZ_PRIVATE_KEY").ok(),
                url: std::env::var("BUZZ_RELAY_URL").ok(),
                relay_pubkey: std::env::var("BUZZ_RELAY_PUBKEY").ok(),
            }
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            restore_var("BUZZ_PRIVATE_KEY", self.key.as_deref());
            restore_var("BUZZ_RELAY_URL", self.url.as_deref());
            restore_var("BUZZ_RELAY_PUBKEY", self.relay_pubkey.as_deref());
        }
    }

    fn restore_var(name: &str, value: Option<&str>) {
        match value {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
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
        assert_eq!(parsed.public_key, keys.public_key);
        assert!(!format!("{parsed:?}").contains("secret"));
    }
}
