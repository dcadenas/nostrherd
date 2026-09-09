//! `nostrherd init`: scaffold a bot corpus from templates shipped in the binary.
//!
//! The templates are the files under `corpus/template-bot/`, embedded at build
//! time so a released binary can scaffold with no checkout and no network.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use nostrherd_domain::BotId;

const AGENTS_MD: &str = include_str!("../../../corpus/template-bot/AGENTS.md");
const CLAUDE_MD: &str = include_str!("../../../corpus/template-bot/CLAUDE.md");
const README_MD: &str = include_str!("../../../corpus/template-bot/README.md");
const GITIGNORE: &str = include_str!("../../../corpus/template-bot/.gitignore");
const STARTUP_MD: &str = include_str!("../../../corpus/template-bot/startup.md");

const BOT_ID: &str = "{{BOT_ID}}";
const CORPUS_PATH: &str = "{{CORPUS_PATH}}";
const KIND: &str = "{{KIND}}";

/// Failure while scaffolding a corpus.
#[derive(Debug)]
pub enum InitError {
    /// The bot id is not a `[a-z][a-z0-9-]*` slug.
    InvalidId(String),
    /// The occupant kind was empty.
    EmptyKind,
    /// The destination exists and holds entries.
    DestinationNotEmpty(PathBuf),
    /// The destination could not be created or written.
    Io(io::Error),
    /// `git init` did not succeed.
    Git(String),
}

impl fmt::Display for InitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidId(raw) => write!(
                f,
                "bot id {raw:?} is not a slug of lowercase letters, digits, and hyphens starting with a letter"
            ),
            Self::EmptyKind => write!(f, "occupant kind must not be empty; pass --kind opencode or another installed agent CLI"),
            Self::DestinationNotEmpty(path) => {
                write!(f, "{} already has files in it; choose a new or empty directory, or use the existing corpus without init", path.display())
            }
            Self::Io(error) => write!(f, "{error}"),
            Self::Git(message) => write!(f, "git init failed: {message}"),
        }
    }
}

impl From<io::Error> for InitError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// One scaffolded corpus, rendered but not yet written.
#[derive(Debug, PartialEq, Eq)]
pub struct Scaffold {
    /// Corpus-relative file name and its rendered contents.
    pub files: Vec<(&'static str, String)>,
    /// The registry entry that registers this bot with the host.
    pub registry_entry: String,
}

/// Render a corpus for `id` and `kind` at `dir`, without touching the disk.
///
/// # Errors
///
/// Returns an error when the id is not a valid slug or the kind is empty.
pub fn render(dir: &Path, id: &str, kind: &str) -> Result<Scaffold, InitError> {
    let bot_id = BotId::new(id).ok_or_else(|| InitError::InvalidId(id.to_owned()))?;
    let kind = kind.trim();
    if kind.is_empty() {
        return Err(InitError::EmptyKind);
    }
    let corpus = dir.display().to_string();
    let fill = |template: &str| {
        template
            .replace(BOT_ID, bot_id.as_str())
            .replace(CORPUS_PATH, &corpus)
            .replace(KIND, kind)
    };
    Ok(Scaffold {
        files: vec![
            ("AGENTS.md", fill(AGENTS_MD)),
            ("CLAUDE.md", fill(CLAUDE_MD)),
            ("README.md", fill(README_MD)),
            (".gitignore", fill(GITIGNORE)),
            ("startup.md", fill(STARTUP_MD)),
        ],
        registry_entry: format!(
            "[[bots]]\nid = \"{}\"\ncorpus = \"{corpus}\"\nkind = \"{kind}\"\n",
            bot_id.as_str()
        ),
    })
}

/// Create `dir`, write the rendered corpus into it, and `git init` it.
///
/// Refuses a destination that already holds entries, so an existing corpus is
/// never overwritten.
///
/// # Errors
///
/// Returns an error when the id or kind is invalid, the destination is not
/// empty, a write fails, or `git init` fails.
pub fn write(dir: &Path, id: &str, kind: &str) -> Result<Scaffold, InitError> {
    let scaffold = render(dir, id, kind)?;
    if dir
        .read_dir()
        .is_ok_and(|mut entries| entries.next().is_some())
    {
        return Err(InitError::DestinationNotEmpty(dir.to_path_buf()));
    }
    fs::create_dir_all(dir)?;
    for (name, contents) in &scaffold.files {
        fs::write(dir.join(name), contents)?;
    }
    git_init(dir)?;
    Ok(scaffold)
}

fn git_init(dir: &Path) -> Result<(), InitError> {
    let output = Command::new("git")
        .arg("init")
        .arg("--quiet")
        .current_dir(dir)
        .output()
        .map_err(|error| InitError::Git(error.to_string()))?;
    if output.status.success() {
        return Ok(());
    }
    let message = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    Err(InitError::Git(if message.is_empty() {
        format!("exited with {}", output.status)
    } else {
        message
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("nostrherd-init-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn every_placeholder_is_filled_in_every_file() {
        let scaffold = render(Path::new("/corpus/mine"), "mybot", "opencode").expect("render");
        for (name, contents) in &scaffold.files {
            assert!(
                !contents.contains("{{"),
                "{name} still holds an unfilled placeholder"
            );
        }
    }

    #[test]
    fn the_registry_entry_names_the_id_path_and_kind() {
        let scaffold = render(Path::new("/corpus/mine"), "mybot", "opencode").expect("render");
        assert_eq!(
            scaffold.registry_entry,
            "[[bots]]\nid = \"mybot\"\ncorpus = \"/corpus/mine\"\nkind = \"opencode\"\n"
        );
    }

    #[test]
    fn the_generated_corpus_ignores_generated_state() {
        let scaffold = render(Path::new("/corpus/mine"), "mybot", "opencode").expect("render");
        let (_, gitignore) = scaffold
            .files
            .iter()
            .find(|(name, _)| *name == ".gitignore")
            .expect("gitignore");
        assert!(gitignore.contains(".nostrherd/"));
    }

    #[test]
    fn the_generated_startup_keeps_both_host_managed_blocks_empty() {
        let scaffold = render(Path::new("/corpus/mine"), "mybot", "opencode").expect("render");
        let (_, startup) = scaffold
            .files
            .iter()
            .find(|(name, _)| *name == "startup.md")
            .expect("startup");
        // The host fills these on its first run; init must not pre-fill them.
        assert!(startup.contains("<!-- nostrherd-contract -->"));
        assert!(startup.contains("<!-- /nostrherd-contract -->"));
    }

    #[test]
    fn an_invalid_id_is_refused_before_anything_is_written() {
        let dir = temp_dir("invalid");
        let error = write(&dir, "My Bot", "opencode").expect_err("invalid id");
        assert!(matches!(error, InitError::InvalidId(_)), "{error:?}");
        assert!(!dir.exists(), "nothing may be created for an invalid id");
    }

    #[test]
    fn an_empty_kind_is_refused() {
        let error = render(Path::new("/corpus/mine"), "mybot", "  ").expect_err("empty kind");
        assert!(matches!(error, InitError::EmptyKind), "{error:?}");
    }

    #[test]
    fn a_destination_holding_files_is_never_overwritten() {
        let dir = temp_dir("occupied");
        fs::create_dir_all(&dir).expect("create");
        fs::write(dir.join("AGENTS.md"), "mine, hand written").expect("write");
        let error = write(&dir, "mybot", "opencode").expect_err("occupied");
        assert!(
            matches!(error, InitError::DestinationNotEmpty(_)),
            "{error:?}"
        );
        assert_eq!(
            fs::read_to_string(dir.join("AGENTS.md")).expect("read"),
            "mine, hand written"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn writing_produces_a_git_repository_holding_every_file() {
        let dir = temp_dir("written");
        let scaffold = write(&dir, "mybot", "opencode").expect("write");
        for (name, _) in &scaffold.files {
            assert!(dir.join(name).is_file(), "{name} was not written");
        }
        assert!(dir.join(".git").is_dir(), "git init did not run");
        let agents = fs::read_to_string(dir.join("AGENTS.md")).expect("read");
        assert!(agents.contains("# mybot"));
        let _ = fs::remove_dir_all(&dir);
    }
}
