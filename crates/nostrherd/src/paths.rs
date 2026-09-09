//! Conventional locations for the host's registry and database.
//!
//! Resolved the way Kelpie resolves its own paths, so the two tools agree on
//! where a user's configuration lives: the XDG base when it is set, otherwise
//! the documented fallback under `HOME`. There is no platform branch, so macOS
//! lands on `~/.config` and `~/.local/share` like Linux, rather than on
//! `~/Library/Application Support`.
//!
//! `--config` and `--database` override these; nothing here reads them.

use std::env;
use std::fmt;
use std::path::PathBuf;

/// Directory holding both files, under whichever XDG base applies.
const DIR: &str = "nostrherd";

/// Registry file name inside the config directory.
const REGISTRY: &str = "bots.toml";

/// Database file name inside the data directory.
const DATABASE: &str = "nostrherd.sqlite";

/// A conventional path could not be resolved.
#[derive(Debug, PartialEq, Eq)]
pub struct PathError {
    /// The XDG variable that would have answered, had it been set.
    pub xdg_variable: &'static str,
}

impl fmt::Display for PathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot locate the {DIR} directory: neither {} nor HOME is set",
            self.xdg_variable
        )
    }
}

impl std::error::Error for PathError {}

/// Conventional registry path: `$XDG_CONFIG_HOME/nostrherd/bots.toml`.
///
/// # Errors
///
/// Returns an error when neither `XDG_CONFIG_HOME` nor `HOME` is set.
pub fn default_config() -> Result<PathBuf, PathError> {
    default_config_with(from_env)
}

/// Conventional database path: `$XDG_DATA_HOME/nostrherd/nostrherd.sqlite`.
///
/// # Errors
///
/// Returns an error when neither `XDG_DATA_HOME` nor `HOME` is set.
pub fn default_database() -> Result<PathBuf, PathError> {
    default_database_with(from_env)
}

fn default_config_with(get: impl Fn(&str) -> Option<String>) -> Result<PathBuf, PathError> {
    Ok(base("XDG_CONFIG_HOME", ".config", get)?
        .join(DIR)
        .join(REGISTRY))
}

fn default_database_with(get: impl Fn(&str) -> Option<String>) -> Result<PathBuf, PathError> {
    Ok(base("XDG_DATA_HOME", ".local/share", get)?
        .join(DIR)
        .join(DATABASE))
}

/// One XDG base directory, or its documented fallback under `HOME`.
///
/// An empty variable counts as unset, which is what the XDG spec requires and
/// what a shell leaves behind after `export XDG_CONFIG_HOME=`.
fn base(
    xdg_variable: &'static str,
    fallback: &str,
    get: impl Fn(&str) -> Option<String>,
) -> Result<PathBuf, PathError> {
    if let Some(root) = get(xdg_variable).filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(root));
    }
    let home = get("HOME")
        .filter(|value| !value.is_empty())
        .ok_or(PathError { xdg_variable })?;
    Ok(PathBuf::from(home).join(fallback))
}

fn from_env(name: &str) -> Option<String> {
    env::var(name).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Env as it looks on a stock macOS or Linux login: HOME set, no XDG.
    fn home_only(name: &str) -> Option<String> {
        (name == "HOME").then(|| "/Users/alice".to_owned())
    }

    #[test]
    fn without_xdg_both_paths_fall_back_under_home() {
        // A stock macOS shell sets no XDG variables, so this is the path a Mac
        // user gets. It matches Kelpie, which has no platform branch either.
        assert_eq!(
            default_config_with(home_only).expect("config"),
            PathBuf::from("/Users/alice/.config/nostrherd/bots.toml")
        );
        assert_eq!(
            default_database_with(home_only).expect("database"),
            PathBuf::from("/Users/alice/.local/share/nostrherd/nostrherd.sqlite")
        );
    }

    #[test]
    fn a_set_xdg_base_wins_over_home() {
        let get = |name: &str| match name {
            "HOME" => Some("/Users/alice".to_owned()),
            "XDG_CONFIG_HOME" => Some("/config".to_owned()),
            "XDG_DATA_HOME" => Some("/data".to_owned()),
            _ => None,
        };
        assert_eq!(
            default_config_with(get).expect("config"),
            PathBuf::from("/config/nostrherd/bots.toml")
        );
        assert_eq!(
            default_database_with(get).expect("database"),
            PathBuf::from("/data/nostrherd/nostrherd.sqlite")
        );
    }

    #[test]
    fn an_empty_xdg_variable_counts_as_unset() {
        let get = |name: &str| match name {
            "HOME" => Some("/Users/alice".to_owned()),
            "XDG_CONFIG_HOME" => Some(String::new()),
            _ => None,
        };
        assert_eq!(
            default_config_with(get).expect("config"),
            PathBuf::from("/Users/alice/.config/nostrherd/bots.toml")
        );
    }

    #[test]
    fn the_registry_and_database_sit_under_different_bases() {
        // Separate bases, so backing up config does not silently capture the
        // database and vice versa.
        let config = default_config_with(home_only).expect("config");
        let database = default_database_with(home_only).expect("database");
        assert_ne!(config.parent(), database.parent());
    }

    #[test]
    fn without_home_or_xdg_the_error_names_both() {
        let error = default_config_with(|_| None).expect_err("no home");
        assert_eq!(error.xdg_variable, "XDG_CONFIG_HOME");
        assert!(error.to_string().contains("XDG_CONFIG_HOME"));
        assert!(error.to_string().contains("HOME"));
    }
}
