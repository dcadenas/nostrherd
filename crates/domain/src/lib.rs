//! Domain types for the personal Nostr bot host.
//!
//! No I/O. SQLite, Kelpie, Herdr, and the relay live in adapters.

use std::fmt;
use std::path::{Path, PathBuf};

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

/// Inbound trigger token required by D9.
pub const INBOUND_TRIGGER: &str = "bot:";

/// Outbound stamp applied by the host.
pub const OUTBOUND_PREFIX: &str = "[bot]:";

/// Prefix occupant prose with `[bot]:` once.
#[must_use]
pub fn stamp_outbound(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.starts_with(OUTBOUND_PREFIX) {
        trimmed.to_owned()
    } else if trimmed.is_empty() {
        OUTBOUND_PREFIX.to_owned()
    } else {
        format!("{OUTBOUND_PREFIX} {trimmed}")
    }
}

/// Configured personality mapped to one in-process actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bot {
    id: BotId,
    corpus_path: PathBuf,
    inbound_trigger: String,
    outbound_prefix: String,
    occupant_kind: String,
}

impl Bot {
    /// Construct a bot with the D9 trigger and stamp protocol.
    #[must_use]
    pub fn new(id: BotId, corpus_path: PathBuf, occupant_kind: impl Into<String>) -> Option<Self> {
        let occupant_kind = occupant_kind.into();
        if occupant_kind.is_empty() || occupant_kind.chars().any(char::is_whitespace) {
            return None;
        }
        Some(Self {
            id,
            corpus_path,
            inbound_trigger: INBOUND_TRIGGER.to_owned(),
            outbound_prefix: OUTBOUND_PREFIX.to_owned(),
            occupant_kind,
        })
    }

    #[must_use]
    pub fn id(&self) -> &BotId {
        &self.id
    }

    #[must_use]
    pub fn corpus_path(&self) -> &Path {
        &self.corpus_path
    }

    #[must_use]
    pub fn inbound_trigger(&self) -> &str {
        &self.inbound_trigger
    }

    #[must_use]
    pub fn outbound_prefix(&self) -> &str {
        &self.outbound_prefix
    }

    #[must_use]
    pub fn occupant_kind(&self) -> &str {
        &self.occupant_kind
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
        Self::from_body(body)
    }

    /// Parse the inbound trigger token without checking `p`-tags.
    ///
    /// Use this on already-classified trigger text, such as a stored channel
    /// body being replayed as an ask.
    #[must_use]
    pub fn from_body(body: &str) -> Option<Self> {
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

/// Lifecycle state of one triggered turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnState {
    Queued,
    Open,
    Posted,
    Failed,
    Cancelled,
}

impl TurnState {
    /// Parse a stored turn-state token.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "queued" => Some(Self::Queued),
            "open" => Some(Self::Open),
            "posted" => Some(Self::Posted),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    /// Return the stored token for this state.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Open => "open",
            Self::Posted => "posted",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for TurnState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Legal change from one turn state to another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnTransition {
    from: TurnState,
    to: TurnState,
}

impl TurnTransition {
    /// Parse a legal turn-state change.
    ///
    /// Queued work may open or cancel. Open work may post, fail, or cancel.
    /// Terminal states have no outgoing transition. There is no `publishing`
    /// state.
    #[must_use]
    pub fn parse(from: TurnState, to: TurnState) -> Option<Self> {
        let allowed = matches!(
            (from, to),
            (TurnState::Queued, TurnState::Open | TurnState::Cancelled)
                | (
                    TurnState::Open,
                    TurnState::Posted | TurnState::Failed | TurnState::Cancelled
                )
        );
        allowed.then_some(Self { from, to })
    }

    #[must_use]
    pub fn from_state(self) -> TurnState {
        self.from
    }

    #[must_use]
    pub fn to_state(self) -> TurnState {
        self.to
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
    fn bot_uses_fixed_trigger_protocol_and_rejects_empty_kind() {
        let id = BotId::new("bot").expect("bot");
        assert!(Bot::new(id.clone(), PathBuf::from("/corpus"), "").is_none());
        assert!(Bot::new(id.clone(), PathBuf::from("/corpus"), "open code").is_none());
        let bot = Bot::new(id, PathBuf::from("/corpus"), "opencode").expect("bot");
        assert_eq!(bot.inbound_trigger(), INBOUND_TRIGGER);
        assert_eq!(bot.outbound_prefix(), OUTBOUND_PREFIX);
        assert_eq!(bot.occupant_kind(), "opencode");
        assert_eq!(bot.corpus_path(), Path::new("/corpus"));
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
    fn session_names_include_the_bot_id() {
        let bot = BotId::new("bot").expect("bot");
        let review = BotId::new("review").expect("bot");
        let channel = "ab12cd34-5678-90ab-cdef-0123456789ab";
        let bot_name =
            SessionName::from_bot_and_channel(&bot, channel, "foobar", |_| false).expect("bot");
        let review_name = SessionName::from_bot_and_channel(&review, channel, "foobar", |_| false)
            .expect("review");
        assert_eq!(bot_name.as_str(), "bot-foobar");
        assert_eq!(review_name.as_str(), "review-foobar");
        assert_ne!(bot_name.as_str(), review_name.as_str());
    }

    #[test]
    fn session_names_disambiguate_when_bot_and_display_collide() {
        let first = BotId::new("a").expect("bot");
        let second = BotId::new("a-b").expect("bot");
        let channel = "ab12cd34-5678-90ab-cdef-0123456789ab";
        let mut taken = std::collections::HashSet::new();
        let first_name = SessionName::from_bot_and_channel(&first, channel, "b-c", |candidate| {
            taken.contains(candidate)
        })
        .expect("first");
        taken.insert(first_name.as_str().to_owned());
        let second_name = SessionName::from_bot_and_channel(&second, channel, "c", |candidate| {
            taken.contains(candidate)
        })
        .expect("second");
        assert_eq!(first_name.as_str(), "a-b-c");
        assert_ne!(first_name.as_str(), second_name.as_str());
        assert!(second_name.as_str().starts_with("a-b-c-"));
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
        assert_eq!(
            TriggerMatch::from_body("@daniel bot: review the PR")
                .expect("body")
                .request(),
            "review the PR"
        );
        assert_eq!(
            TriggerMatch::from_body("bot:").expect("empty").request(),
            ""
        );
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
    fn stamp_outbound_prefixes_once() {
        assert_eq!(stamp_outbound("hello"), "[bot]: hello");
        assert_eq!(stamp_outbound("  [bot]: already  "), "[bot]: already");
        assert_eq!(stamp_outbound("[bot]:already"), "[bot]:already");
    }

    #[test]
    fn event_id_is_64_hex() {
        assert!(EventId::parse_hex("ab").is_none());
        let hex = "a".repeat(64);
        assert_eq!(EventId::parse_hex(&hex).map(|e| e.as_str().len()), Some(64));
    }

    #[test]
    fn turn_state_parses_known_tokens_only() {
        assert_eq!(TurnState::parse("queued"), Some(TurnState::Queued));
        assert_eq!(TurnState::parse("open"), Some(TurnState::Open));
        assert_eq!(TurnState::parse("posted"), Some(TurnState::Posted));
        assert_eq!(TurnState::parse("failed"), Some(TurnState::Failed));
        assert_eq!(TurnState::parse("cancelled"), Some(TurnState::Cancelled));
        assert!(TurnState::parse("publishing").is_none());
        assert!(TurnState::parse("Open").is_none());
        assert_eq!(TurnState::Open.to_string(), "open");
    }

    #[test]
    fn turn_transition_parses_legal_changes_only() {
        let allowed = [
            (TurnState::Queued, TurnState::Open),
            (TurnState::Queued, TurnState::Cancelled),
            (TurnState::Open, TurnState::Posted),
            (TurnState::Open, TurnState::Failed),
            (TurnState::Open, TurnState::Cancelled),
        ];
        for (from, to) in allowed {
            let transition = TurnTransition::parse(from, to).expect("legal");
            assert_eq!(transition.from_state(), from);
            assert_eq!(transition.to_state(), to);
        }
        for from in [
            TurnState::Queued,
            TurnState::Open,
            TurnState::Posted,
            TurnState::Failed,
            TurnState::Cancelled,
        ] {
            for to in [
                TurnState::Queued,
                TurnState::Open,
                TurnState::Posted,
                TurnState::Failed,
                TurnState::Cancelled,
            ] {
                if allowed.contains(&(from, to)) {
                    continue;
                }
                assert!(
                    TurnTransition::parse(from, to).is_none(),
                    "unexpected {from} -> {to}"
                );
            }
        }
    }
}
