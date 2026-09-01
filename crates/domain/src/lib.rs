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
    /// Derive a unique name from a bot and Buzz channel.
    ///
    /// The readable `bot-foobar` form is preferred. If `is_taken` reports that
    /// candidate is bound to another channel, the channel UUID is added as a
    /// stable suffix. Every candidate passed to `is_taken` is at most 32 bytes.
    #[must_use]
    pub fn from_bot_and_channel(
        bot: &BotId,
        channel_id: &str,
        channel_display: &str,
        mut is_taken: impl FnMut(&str) -> bool,
    ) -> Option<Self> {
        let display_slug = slugify(channel_display);
        let compact_id = compact_uuid(channel_id)?;
        let base = session_candidate(bot.as_str(), &display_slug, None)?;
        if !is_taken(&base) {
            return Some(Self(base));
        }

        let maximum_suffix_len = 32_usize.checked_sub(bot.as_str().len() + 1)?;
        let maximum_suffix_len = compact_id.len().min(maximum_suffix_len);
        let mut suffix_len = 8.min(maximum_suffix_len);
        while suffix_len <= maximum_suffix_len {
            if suffix_len == 0 {
                break;
            }
            let candidate =
                session_candidate(bot.as_str(), &display_slug, Some(&compact_id[..suffix_len]))?;
            if !is_taken(&candidate) {
                return Some(Self(candidate));
            }
            if suffix_len == maximum_suffix_len {
                break;
            }
            suffix_len = (suffix_len + 4).min(maximum_suffix_len);
        }

        None
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn slugify(display: &str) -> String {
    let mut slug = String::new();
    let mut separator_pending = false;

    for character in display.chars() {
        if character.is_ascii_alphanumeric() {
            if separator_pending && !slug.is_empty() {
                slug.push('-');
            }
            slug.push(character.to_ascii_lowercase());
            separator_pending = false;
        } else if !slug.is_empty() {
            separator_pending = true;
        }
    }

    if slug.is_empty() {
        "channel".to_owned()
    } else {
        slug
    }
}

fn compact_uuid(raw: &str) -> Option<String> {
    if raw.len() != 36
        || raw.char_indices().any(|(index, character)| match index {
            8 | 13 | 18 | 23 => character != '-',
            _ => !character.is_ascii_hexdigit(),
        })
    {
        return None;
    }

    Some(
        raw.chars()
            .filter(|character| *character != '-')
            .map(|character| character.to_ascii_lowercase())
            .collect(),
    )
}

fn session_candidate(bot: &str, display: &str, suffix: Option<&str>) -> Option<String> {
    let candidate = if let Some(suffix) = suffix {
        let fixed_len = bot.len() + 1 + suffix.len();
        let display_len = 32_usize.saturating_sub(fixed_len.saturating_add(1));
        let display = display
            .get(..display.len().min(display_len))?
            .trim_end_matches('-');
        if display.is_empty() {
            format!("{bot}-{suffix}")
        } else {
            format!("{bot}-{display}-{suffix}")
        }
    } else {
        let display_len = 32_usize.checked_sub(bot.len() + 1)?;
        let display = display
            .get(..display.len().min(display_len))?
            .trim_end_matches('-');
        if display.is_empty() {
            return None;
        }
        format!("{bot}-{display}")
    };
    (candidate.len() <= 32).then_some(candidate)
}

/// A body accepted by the configured inbound trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerMatch {
    request: String,
}

impl TriggerMatch {
    /// Match an operator `p`-tag and an exact leading `bot:` token.
    ///
    /// One leading `@mention` token is allowed. The returned request excludes
    /// both the mention and trigger tokens.
    #[must_use]
    pub fn parse<I, S>(operator_pubkey: &str, p_tags: I, body: &str) -> Option<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if !p_tags
            .into_iter()
            .any(|pubkey| pubkey.as_ref() == operator_pubkey)
        {
            return None;
        }

        let body = body.trim_start();
        let (first, remainder) = split_first_token(body)?;
        let request = if first == "bot:" {
            remainder
        } else if first.starts_with('@') && first.len() > 1 {
            let (trigger, remainder) = split_first_token(remainder)?;
            (trigger == "bot:").then_some(remainder)?
        } else {
            return None;
        };

        Some(Self {
            request: request.trim().to_owned(),
        })
    }

    /// Return the text after the trigger token.
    #[must_use]
    pub fn request(&self) -> &str {
        &self.request
    }
}

fn split_first_token(input: &str) -> Option<(&str, &str)> {
    if input.is_empty() {
        return None;
    }

    match input.find(char::is_whitespace) {
        Some(index) => Some((&input[..index], input[index..].trim_start())),
        None => Some((input, "")),
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
    fn session_name_prefers_readable_channel_display() {
        let bot = BotId::new("bot").expect("bot");
        let name = SessionName::from_bot_and_channel(
            &bot,
            "ab12cd34-5678-90ab-cdef-0123456789ab",
            "#Foobar",
            |_| false,
        )
        .expect("name");
        assert_eq!(name.as_str(), "bot-foobar");
    }

    #[test]
    fn session_name_uses_uuid_to_disambiguate_and_stays_bounded() {
        let bot = BotId::new("review").expect("bot");
        let mut first_candidate = true;
        let name = SessionName::from_bot_and_channel(
            &bot,
            "ab12cd34-5678-90ab-cdef-0123456789ab",
            "A channel display name that is much too long",
            |_| std::mem::replace(&mut first_candidate, false),
        )
        .expect("name");

        assert_eq!(name.as_str(), "review-a-channel-displa-ab12cd34");
        assert!(name.as_str().len() <= 32);
    }

    #[test]
    fn session_name_tries_more_of_uuid_after_a_prefix_collision() {
        let bot = BotId::new("bot").expect("bot");
        let name = SessionName::from_bot_and_channel(
            &bot,
            "ab12cd34-5678-90ab-cdef-0123456789ab",
            "foobar",
            |candidate| matches!(candidate, "bot-foobar" | "bot-foobar-ab12cd34"),
        )
        .expect("name");

        assert_eq!(name.as_str(), "bot-foobar-ab12cd345678");
    }

    #[test]
    fn session_name_shortens_uuid_suffix_for_a_long_bot_id() {
        let bot_id = "a".repeat(28);
        let bot = BotId::new(&bot_id).expect("bot");
        let mut first_candidate = true;
        let name = SessionName::from_bot_and_channel(
            &bot,
            "ab12cd34-5678-90ab-cdef-0123456789ab",
            "foo",
            |_| std::mem::replace(&mut first_candidate, false),
        )
        .expect("name");

        assert_eq!(name.as_str(), format!("{bot_id}-ab1"));
        assert_eq!(name.as_str().len(), 32);
    }

    #[test]
    fn session_name_rejects_a_non_uuid_channel_id() {
        let bot = BotId::new("bot").expect("bot");
        assert!(
            SessionName::from_bot_and_channel(&bot, "not-a-uuid", "foobar", |_| false).is_none()
        );
    }

    #[test]
    fn trigger_requires_operator_p_tag() {
        assert!(TriggerMatch::parse("operator", ["someone-else"], "bot: hello").is_none());
        assert_eq!(
            TriggerMatch::parse("operator", ["someone-else", "operator"], "bot: hello")
                .map(|matched| matched.request),
            Some("hello".to_owned())
        );
    }

    #[test]
    fn trigger_allows_one_leading_mention() {
        let matched = TriggerMatch::parse(
            "operator",
            ["operator"],
            "  @daniel\n bot:   review the PR  ",
        )
        .expect("trigger");

        assert_eq!(matched.request(), "review the PR");
    }

    #[test]
    fn trigger_rejects_non_prefix_and_inexact_tokens() {
        for body in [
            "and the PR?",
            "@daniel and the PR?",
            "please bot: help",
            "bot:help",
            "@daniel bot:help",
            "@daniel @bot bot: help",
            "[bot]: bot: help",
        ] {
            assert!(
                TriggerMatch::parse("operator", ["operator"], body).is_none(),
                "unexpected trigger: {body}"
            );
        }
    }

    #[test]
    fn event_id_is_64_hex() {
        assert!(EventId::parse_hex("ab").is_none());
        let hex = "a".repeat(64);
        assert_eq!(EventId::parse_hex(&hex).map(|e| e.as_str().len()), Some(64));
    }
}
