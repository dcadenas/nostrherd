//! Host bot registry loaded from TOML.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use nostr_sdk::prelude::PublicKey;
use nostrherd_domain::{Bot, BotId};
use serde::Deserialize;

use crate::WAITER_NAME;

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
    #[serde(default)]
    allowed_requesters: Vec<String>,
    operator_session: Option<String>,
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
    /// Two bots shared the same id.
    DuplicateBot { id: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "failed to read bot config: {error}"),
            Self::Parse(error) => write!(formatter, "invalid bot config: {error}"),
            Self::InvalidBot { id } => write!(formatter, "invalid bot record: {id}"),
            Self::DuplicateBot { id } => write!(formatter, "duplicate bot id: {id}"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Parse(error) => Some(error),
            Self::InvalidBot { .. } | Self::DuplicateBot { .. } => None,
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
            let operator_session = record
                .operator_session
                .as_deref()
                .map(str::trim)
                .map(str::to_owned)
                .filter(|session| {
                    !session.is_empty()
                        && !session.chars().any(char::is_whitespace)
                        && session != WAITER_NAME
                });
            if record.operator_session.is_some() && operator_session.is_none() {
                return Err(ConfigError::InvalidBot { id });
            }
            let bot = Bot::new(bot_id, record.corpus, record.kind)
                .ok_or(ConfigError::InvalidBot { id: id.clone() })?
                .with_allowed_requesters(
                    record
                        .allowed_requesters
                        .iter()
                        .map(|key| {
                            PublicKey::parse(key)
                                .map(|key| key.to_hex())
                                .map_err(|_| ConfigError::InvalidBot { id: id.clone() })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .with_operator_session(operator_session);
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
    fn requester_allowlist_defaults_to_everyone_and_normalizes_keys() {
        use nostr_sdk::prelude::ToBech32;
        let prefix = "[[bots]]\nid = 'bot'\ncorpus = '/corpus/bot'\nkind = 'opencode'\n";
        let operator = "a".repeat(64);
        let peer = "b".repeat(64);
        for suffix in ["", "allowed_requesters = []"] {
            let registry = BotRegistry::from_toml(&format!("{prefix}{suffix}")).unwrap();
            assert!(registry.bots()[0].authorizes(&operator, &operator));
            assert!(registry.bots()[0].authorizes(&operator, &peer));
        }
        let npub = PublicKey::parse(&peer).unwrap().to_bech32().unwrap();
        for key in [peer.to_ascii_uppercase(), npub] {
            let registry =
                BotRegistry::from_toml(&format!("{prefix}allowed_requesters = ['{key}']")).unwrap();
            assert_eq!(
                registry.bots()[0].allowed_requesters(),
                std::slice::from_ref(&peer)
            );
            assert!(registry.bots()[0].authorizes(&operator, &peer));
            assert!(registry.bots()[0].authorizes(&operator, &operator));
        }
        for value in ["['not-a-public-key']", "['']", "'all'", "[7]"] {
            assert!(
                BotRegistry::from_toml(&format!("{prefix}allowed_requesters = {value}")).is_err()
            );
        }

        let operator_only =
            BotRegistry::from_toml(&format!("{prefix}allowed_requesters = ['{operator}']"))
                .unwrap();
        assert!(!operator_only.bots()[0].authorizes(&operator, &peer));
    }

    #[test]
    fn operator_session_is_optional_and_must_be_one_token() {
        let prefix = "[[bots]]\nid = 'bot'\ncorpus = '/corpus/bot'\nkind = 'opencode'\n";
        let registry =
            BotRegistry::from_toml(&format!("{prefix}operator_session = 'nostrherd-dev'")).unwrap();
        assert_eq!(registry.bots()[0].operator_session(), Some("nostrherd-dev"));
        for value in ["''", "'two words'", "'nostrherd'"] {
            assert!(
                BotRegistry::from_toml(&format!("{prefix}operator_session = {value}")).is_err()
            );
        }
    }

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
        assert_eq!(bot.inbound_trigger(), nostrherd_domain::INBOUND_TRIGGER);
        assert_eq!(bot.outbound_prefix(), nostrherd_domain::OUTBOUND_PREFIX);
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
        assert_eq!(registry.bots()[0].outbound_prefix(), "**[bot]**:");
        assert_eq!(registry.bots()[1].inbound_trigger(), "pr:");
        assert_eq!(registry.bots()[1].outbound_prefix(), "**[pr]**:");
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
}
