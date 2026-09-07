//! Host bot registry loaded from TOML.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use cooee_domain::restraint::{HostRestraint, QuietHours};
use cooee_domain::{Bot, BotId};
use serde::Deserialize;

/// Configured bots available to the host process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BotRegistry {
    bots: Vec<Bot>,
}

#[derive(Debug, Deserialize)]
struct FileConfig {
    bots: Vec<FileBot>,
}

#[derive(Debug, Deserialize)]
struct FileBot {
    id: String,
    corpus: PathBuf,
    kind: String,
    /// Host-initiated posts per channel per rolling 24 h (D47).
    post_ceiling: Option<u32>,
    /// `HH:MM-HH:MM` quiet window on the host's local clock (D47).
    quiet_hours: Option<String>,
}

/// Failure while reading bot configuration.
#[derive(Debug)]
pub enum ConfigError {
    /// The config file could not be read.
    Io(io::Error),
    /// The file was not valid registry TOML.
    Parse(toml::de::Error),
    /// A bot record violated domain constraints.
    InvalidBot { id: String },
    /// A bot's post ceiling was below one.
    InvalidCeiling { id: String, ceiling: u32 },
    /// A bot's quiet-hours window was not `HH:MM-HH:MM`.
    InvalidQuietHours { id: String, window: String },
    /// Two bots shared the same id.
    DuplicateBot { id: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "failed to read bot config: {error}"),
            Self::Parse(error) => write!(formatter, "invalid bot config: {error}"),
            Self::InvalidBot { id } => write!(formatter, "invalid bot record: {id}"),
            Self::InvalidCeiling { id, ceiling } => {
                write!(formatter, "invalid post_ceiling {ceiling} for bot: {id}")
            }
            Self::InvalidQuietHours { id, window } => {
                write!(formatter, "invalid quiet_hours {window:?} for bot: {id}")
            }
            Self::DuplicateBot { id } => write!(formatter, "duplicate bot id: {id}"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Parse(error) => Some(error),
            Self::InvalidBot { .. }
            | Self::InvalidCeiling { .. }
            | Self::InvalidQuietHours { .. }
            | Self::DuplicateBot { .. } => None,
        }
    }
}

impl BotRegistry {
    /// Load a registry from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or parsed.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path).map_err(ConfigError::Io)?;
        Self::from_toml(&text)
    }

    /// Parse a registry from TOML text.
    ///
    /// # Errors
    ///
    /// Returns an error when the TOML is invalid or a bot record is rejected.
    pub fn from_toml(text: &str) -> Result<Self, ConfigError> {
        let parsed: FileConfig = toml::from_str(text).map_err(ConfigError::Parse)?;
        let mut bots = Vec::with_capacity(parsed.bots.len());
        for record in parsed.bots {
            let id = record.id.clone();
            let bot_id =
                BotId::new(&record.id).ok_or_else(|| ConfigError::InvalidBot { id: id.clone() })?;
            let quiet_hours =
                match record.quiet_hours.as_deref() {
                    None => None,
                    Some(raw) => Some(QuietHours::parse(raw).ok_or_else(|| {
                        ConfigError::InvalidQuietHours {
                            id: id.clone(),
                            window: raw.to_owned(),
                        }
                    })?),
                };
            let ceiling = record
                .post_ceiling
                .unwrap_or(cooee_domain::restraint::DEFAULT_POST_CEILING_24H);
            let restraint =
                HostRestraint::new(ceiling, quiet_hours).ok_or(ConfigError::InvalidCeiling {
                    id: id.clone(),
                    ceiling,
                })?;
            let bot = Bot::new(bot_id, record.corpus, record.kind)
                .ok_or(ConfigError::InvalidBot { id: id.clone() })?
                .with_restraint(restraint);
            if bots.iter().any(|existing: &Bot| existing.id() == bot.id()) {
                return Err(ConfigError::DuplicateBot { id });
            }
            bots.push(bot);
        }
        Ok(Self { bots })
    }

    /// Return configured bots in file order.
    #[must_use]
    pub fn bots(&self) -> &[Bot] {
        &self.bots
    }

    /// Find one bot by id.
    #[must_use]
    pub fn get(&self, id: &BotId) -> Option<&Bot> {
        self.bots.iter().find(|bot| bot.id() == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_loads_bots_with_corpus_and_kind() {
        let registry = BotRegistry::from_toml(
            r#"
            [[bots]]
            id = "bot"
            corpus = "/corpus/bot"
            kind = "opencode"
            "#,
        )
        .expect("registry");
        let bot = &registry.bots()[0];
        assert_eq!(bot.id().as_str(), "bot");
        assert_eq!(bot.corpus_path(), Path::new("/corpus/bot"));
        assert_eq!(bot.occupant_kind(), "opencode");
        assert_eq!(bot.inbound_trigger(), cooee_domain::INBOUND_TRIGGER);
        assert_eq!(bot.outbound_prefix(), cooee_domain::OUTBOUND_PREFIX);
    }

    #[test]
    fn registry_loads_every_configured_bot() {
        let registry = BotRegistry::from_toml(
            r#"
            [[bots]]
            id = "bot"
            corpus = "/corpus/bot"
            kind = "opencode"
            [[bots]]
            id = "pr"
            corpus = "/corpus/pr"
            kind = "opencode"
            "#,
        )
        .expect("registry");
        assert_eq!(registry.bots().len(), 2);
        assert_eq!(registry.bots()[0].inbound_trigger(), "bot:");
        assert_eq!(registry.bots()[0].outbound_prefix(), "[bot]:");
        assert_eq!(registry.bots()[1].inbound_trigger(), "pr:");
        assert_eq!(registry.bots()[1].outbound_prefix(), "[pr]:");
    }

    #[test]
    fn registry_rejects_duplicate_ids() {
        let error = BotRegistry::from_toml(
            r#"
            [[bots]]
            id = "bot"
            corpus = "/a"
            kind = "opencode"
            [[bots]]
            id = "bot"
            corpus = "/b"
            kind = "claude"
            "#,
        )
        .expect_err("duplicate");
        assert!(error.to_string().contains("duplicate bot id: bot"));
    }

    #[test]
    fn restraint_defaults_and_overrides_load_per_bot() {
        let registry = BotRegistry::from_toml(
            r#"
            [[bots]]
            id = "bot"
            corpus = "/corpus/bot"
            kind = "opencode"
            post_ceiling = 3
            quiet_hours = "23:00-07:00"
            [[bots]]
            id = "pr"
            corpus = "/corpus/pr"
            kind = "opencode"
            "#,
        )
        .expect("registry");
        let bot = &registry.bots()[0];
        assert_eq!(bot.restraint().ceiling_24h(), 3);
        assert_eq!(
            bot.restraint().quiet_hours(),
            QuietHours::parse("23:00-07:00").as_ref()
        );
        let pr = &registry.bots()[1];
        assert_eq!(
            pr.restraint().ceiling_24h(),
            cooee_domain::restraint::DEFAULT_POST_CEILING_24H
        );
        assert!(pr.restraint().quiet_hours().is_none());
    }

    #[test]
    fn registry_rejects_a_ceiling_below_one() {
        let error = BotRegistry::from_toml(
            r#"
            [[bots]]
            id = "bot"
            corpus = "/corpus/bot"
            kind = "opencode"
            post_ceiling = 0
            "#,
        )
        .expect_err("ceiling");
        assert!(error.to_string().contains("invalid post_ceiling 0"));
    }

    #[test]
    fn registry_rejects_a_malformed_quiet_window() {
        let error = BotRegistry::from_toml(
            r#"
            [[bots]]
            id = "bot"
            corpus = "/corpus/bot"
            kind = "opencode"
            quiet_hours = "until morning"
            "#,
        )
        .expect_err("quiet hours");
        assert!(error.to_string().contains("invalid quiet_hours"));
    }
}
