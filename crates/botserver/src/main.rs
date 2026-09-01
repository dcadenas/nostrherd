//! Host process: relay subscriber, per-bot actors, Kelpie waiter.

use std::fmt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use botserver::config::{BotRegistry, ConfigError};
use botserver::sqlite::SqliteRepository;
use clap::Parser;

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

#[derive(Debug)]
enum HostError {
    Config(ConfigError),
    Database(rusqlite::Error),
}

impl fmt::Display for HostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "{error}"),
            Self::Database(error) => write!(formatter, "failed to open host database: {error}"),
        }
    }
}

impl std::error::Error for HostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Database(error) => Some(error),
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

fn load_host(config: &Path, database: &Path) -> Result<(BotRegistry, SqliteRepository), HostError> {
    Ok((
        BotRegistry::load(config)?,
        SqliteRepository::open(database)?,
    ))
}

fn run(args: &Args) -> Result<(), HostError> {
    let (_registry, _repository) = load_host(&args.config, &args.database)?;
    if args.check {
        return Ok(());
    }
    loop {
        std::thread::park();
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
    use std::time::{SystemTime, UNIX_EPOCH};

    use botserver::HostRepository;
    use clap::Parser;

    use super::*;

    static NEXT_TEMP_PATH: AtomicU64 = AtomicU64::new(0);

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
}
