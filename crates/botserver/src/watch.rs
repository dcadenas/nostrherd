//! Author-watch commands and persisted watch values.

use botserver_domain::{BotId, EventId};

/// Default cooldown between fires of one watch.
pub const DEFAULT_WATCH_COOLDOWN_SECS: i64 = 30 * 60;

/// A host watch-management command in a trigger request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchCommand {
    Create(NewWatch),
    Cancel { author_pubkey: String },
}

/// Validated watch declaration parsed from a trigger request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewWatch {
    pub author_pubkeys: Vec<String>,
    pub channel_only: bool,
    pub kind: Option<u16>,
    pub cooldown_secs: i64,
    pub expires_after_secs: Option<i64>,
    pub max_fires: Option<u32>,
}

/// Durable watch owned by one bot session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchRecord {
    pub watch_id: EventId,
    pub created_at: i64,
    pub bot_id: BotId,
    pub channel_id: String,
    pub author_pubkeys: Vec<String>,
    pub predicate_channel_id: Option<String>,
    pub predicate_kind: Option<u16>,
    pub cooldown_secs: i64,
    pub expires_at: Option<i64>,
    pub max_fires: Option<u32>,
}

/// A fire recorded before its host-initiated wake is attempted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchFire {
    pub watch_id: EventId,
    pub wake_event_id: EventId,
    pub source_event_id: EventId,
    pub bot_id: BotId,
    pub channel_id: String,
    pub author_pubkey: String,
    pub source_channel_id: Option<String>,
    pub source_kind: u16,
}

impl WatchFire {
    #[must_use]
    pub fn ask_body(&self) -> String {
        let place = self.source_channel_id.as_deref().unwrap_or("no channel");
        format!(
            "## Watch event\n\nAuthor `{}` posted kind {} in channel `{place}`.",
            self.author_pubkey, self.source_kind
        )
    }
}

/// Parse the exact v1 watch-management grammar.
///
/// Create: `watch <pubkey[,pubkey...]> [here] [kind <n>] [cooldown <minutes>]
/// [expires <minutes>] [max <fires>]`. A declaration without an explicit
/// lifetime defaults to one fire. Cancel: `cancel watch <pubkey>`.
#[must_use]
pub fn parse_watch_command(request: &str) -> Option<WatchCommand> {
    let words = request.split_whitespace().collect::<Vec<_>>();
    if let ["cancel", "watch", author] = words.as_slice() {
        return valid_pubkey(author).then(|| WatchCommand::Cancel {
            author_pubkey: author.to_ascii_lowercase(),
        });
    }
    let ["watch", authors, rest @ ..] = words.as_slice() else {
        return None;
    };
    let author_pubkeys = authors
        .split(',')
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    if author_pubkeys.is_empty() || author_pubkeys.iter().any(|key| !valid_pubkey(key)) {
        return None;
    }
    let mut channel_only = false;
    let mut kind = None;
    let mut cooldown_secs = DEFAULT_WATCH_COOLDOWN_SECS;
    let mut expires_after_secs = None;
    let mut max_fires = None;
    let mut index = 0;
    while index < rest.len() {
        match rest[index] {
            "here" => {
                channel_only = true;
                index += 1;
            }
            "kind" => {
                kind = Some(
                    rest.get(index + 1)?
                        .parse()
                        .ok()
                        .filter(|kind| matches!(kind, 9 | 40_002))?,
                );
                index += 2;
            }
            "cooldown" => {
                cooldown_secs = minutes(rest.get(index + 1)?)?;
                index += 2;
            }
            "expires" => {
                expires_after_secs = Some(minutes(rest.get(index + 1)?)?);
                index += 2;
            }
            "max" => {
                max_fires = Some(
                    rest.get(index + 1)?
                        .parse::<u32>()
                        .ok()
                        .filter(|n| *n > 0)?,
                );
                index += 2;
            }
            _ => return None,
        }
    }
    if expires_after_secs.is_none() && max_fires.is_none() {
        max_fires = Some(1);
    }
    Some(WatchCommand::Create(NewWatch {
        author_pubkeys,
        channel_only,
        kind,
        cooldown_secs,
        expires_after_secs,
        max_fires,
    }))
}

fn minutes(raw: &str) -> Option<i64> {
    raw.parse::<i64>()
        .ok()
        .filter(|minutes| *minutes > 0)
        .and_then(|minutes| minutes.checked_mul(60))
}

fn valid_pubkey(raw: &str) -> bool {
    raw.len() == 64 && raw.chars().all(|character| character.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_defaults_to_one_fire_and_thirty_minute_cooldown() {
        let key = "a".repeat(64);
        let Some(WatchCommand::Create(watch)) = parse_watch_command(&format!("watch {key}")) else {
            panic!("create command");
        };
        assert_eq!(watch.author_pubkeys, [key]);
        assert_eq!(watch.cooldown_secs, DEFAULT_WATCH_COOLDOWN_SECS);
        assert_eq!(watch.max_fires, Some(1));
    }

    #[test]
    fn create_accepts_scope_and_explicit_lifetime() {
        let keys = format!("{},{}", "a".repeat(64), "b".repeat(64));
        let Some(WatchCommand::Create(watch)) = parse_watch_command(&format!(
            "watch {keys} here kind 40002 cooldown 5 expires 60 max 3"
        )) else {
            panic!("create command");
        };
        assert!(watch.channel_only);
        assert_eq!(watch.kind, Some(40_002));
        assert_eq!(watch.cooldown_secs, 300);
        assert_eq!(watch.expires_after_secs, Some(3600));
        assert_eq!(watch.max_fires, Some(3));
    }

    #[test]
    fn cancel_requires_a_full_pubkey() {
        assert!(parse_watch_command("cancel watch short").is_none());
        assert!(matches!(
            parse_watch_command(&format!("cancel watch {}", "a".repeat(64))),
            Some(WatchCommand::Cancel { .. })
        ));
    }

    #[test]
    fn create_rejects_activity_kinds_outside_v1_messages() {
        assert!(parse_watch_command(&format!("watch {} kind 7", "a".repeat(64))).is_none());
    }
}
