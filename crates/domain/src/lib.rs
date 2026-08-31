//! Domain types for the personal Nostr bot host.
//!
//! No I/O. SQLite, Kelpie, Herdr, and the relay live in adapters.

use std::fmt;

/// Stable configured bot slug, e.g. `bot`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BotId(String);

impl BotId {
    /// Parse a non-empty slug of `[a-z][a-z0-9-]*`.
    #[must_use]
    pub fn new(raw: &str) -> Option<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }
        let mut chars = trimmed.chars();
        let first = chars.next()?;
        if !first.is_ascii_lowercase() {
            return None;
        }
        if chars.any(|c| !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '-') {
            return None;
        }
        Some(Self(trimmed.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Public Herdr/Kelpie name for one bot in one place.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionName(String);

impl SessionName {
    /// Derive `bot-foobar` / `review-dm-ab12cd34` from bot id and place slug.
    #[must_use]
    pub fn from_bot_and_place(bot: &BotId, place_slug: &str) -> Option<Self> {
        if place_slug.is_empty()
            || !place_slug
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return None;
        }
        let name = format!("{}-{place_slug}", bot.as_str());
        if name.len() > 32 {
            return None;
        }
        Some(Self(name))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque relay event id (32-byte hex).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventId(String);

impl EventId {
    #[must_use]
    pub fn parse_hex(raw: &str) -> Option<Self> {
        let t = raw.trim();
        if t.len() != 64 || !t.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }
        Some(Self(t.to_ascii_lowercase()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bot_id_rejects_empty_and_uppercase() {
        assert!(BotId::new("").is_none());
        assert!(BotId::new("Bot").is_none());
        assert_eq!(BotId::new("bot").map(|b| b.to_string()), Some("bot".into()));
    }

    #[test]
    fn session_name_joins_bot_and_place() {
        let bot = BotId::new("bot").expect("bot");
        let name = SessionName::from_bot_and_place(&bot, "foobar").expect("name");
        assert_eq!(name.as_str(), "bot-foobar");
    }

    #[test]
    fn event_id_is_64_hex() {
        assert!(EventId::parse_hex("ab").is_none());
        let hex = "a".repeat(64);
        assert_eq!(EventId::parse_hex(&hex).map(|e| e.as_str().len()), Some(64));
    }
}
