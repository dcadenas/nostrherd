//! Domain types for the personal Nostr bot host.
//!
//! No I/O. SQLite, Kelpie, Herdr, and the relay live in adapters.

pub mod buzz;
pub mod progress;
pub mod restraint;

use std::fmt;
use std::path::{Path, PathBuf};

use restraint::HostRestraint;

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

/// Inbound trigger token for the example bot id `bot` (`{id}:`).
pub const INBOUND_TRIGGER: &str = "bot:";

/// Inbound trigger token for a bot id, e.g. `bot` → `bot:`.
#[must_use]
pub fn inbound_trigger_for(id: &BotId) -> String {
    format!("{}:", id.as_str())
}

/// Outbound stamp for the example bot id `bot` (`[{id}]:`).
pub const OUTBOUND_PREFIX: &str = "[bot]:";

/// Outbound stamp for a bot id, e.g. `bot` → `[bot]:`.
#[must_use]
pub fn outbound_prefix_for(id: &BotId) -> String {
    format!("[{}]:", id.as_str())
}

/// Prefix occupant prose with the given stamp once.
#[must_use]
pub fn stamp_outbound(body: &str, prefix: &str) -> String {
    let trimmed = body.trim();
    if trimmed.starts_with(prefix) {
        trimmed.to_owned()
    } else if trimmed.is_empty() {
        prefix.to_owned()
    } else {
        format!("{prefix} {trimmed}")
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
    restraint: HostRestraint,
}

impl Bot {
    /// Construct a bot with inbound token `{id}:` and stamp `[{id}]:`.
    ///
    /// Restraint starts at [`HostRestraint::default`]; chain
    /// [`Bot::with_restraint`] to override it.
    #[must_use]
    pub fn new(id: BotId, corpus_path: PathBuf, occupant_kind: impl Into<String>) -> Option<Self> {
        let occupant_kind = occupant_kind.into();
        if occupant_kind.is_empty() || occupant_kind.chars().any(char::is_whitespace) {
            return None;
        }
        let inbound_trigger = inbound_trigger_for(&id);
        let outbound_prefix = outbound_prefix_for(&id);
        Some(Self {
            id,
            corpus_path,
            inbound_trigger,
            outbound_prefix,
            occupant_kind,
            restraint: HostRestraint::default(),
        })
    }

    /// Replace this bot's host-initiated post restraint (D47).
    #[must_use]
    pub fn with_restraint(mut self, restraint: HostRestraint) -> Self {
        self.restraint = restraint;
        self
    }

    /// Restraint applied to this bot's host-initiated posts (D47).
    #[must_use]
    pub fn restraint(&self) -> &HostRestraint {
        &self.restraint
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

/// True when a Buzz 1-1 DM title is not a human name.
#[must_use]
pub fn is_generic_dm_title(display: &str) -> bool {
    display.trim().eq_ignore_ascii_case("dm")
}

/// Choose the occupant place label from channel metadata.
///
/// Stream titles win. A generic `DM` title uses the peer display instead.
///
/// # Examples
///
/// ```
/// use botserver_domain::place_display;
///
/// assert_eq!(place_display("#eng", None), "#eng");
/// assert_eq!(place_display("DM", Some("Sebastian")), "Sebastian");
/// ```
#[must_use]
pub fn place_display(channel_display: &str, peer_display: Option<&str>) -> String {
    let channel = channel_display.trim();
    if !channel.is_empty() && !is_generic_dm_title(channel) {
        return channel.to_owned();
    }
    if let Some(peer) = peer_display.map(str::trim).filter(|peer| !peer.is_empty()) {
        return peer.to_owned();
    }
    channel.to_owned()
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
    /// Match an operator-authored or operator-mentioned `{id}:` body.
    ///
    /// `inbound_trigger` is `{bot-id}:` (D9). Other authors MUST `p`-tag the
    /// operator. The operator's own `{id}:` body is a trigger even when Buzz
    /// only `p`-tags the DM peer (D11, D34). One leading `@mention` token is
    /// allowed. The returned request excludes both the mention and trigger
    /// tokens.
    #[must_use]
    pub fn parse<I, S>(
        operator_pubkey: &str,
        author_pubkey: &str,
        p_tags: I,
        inbound_trigger: &str,
        body: &str,
    ) -> Option<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let from_operator = author_pubkey.eq_ignore_ascii_case(operator_pubkey);
        let mentioned = p_tags
            .into_iter()
            .any(|pubkey| pubkey.as_ref().eq_ignore_ascii_case(operator_pubkey));
        if !from_operator && !mentioned {
            return None;
        }
        Self::from_body(body, inbound_trigger)
    }

    /// Parse the inbound trigger token without checking `p`-tags.
    ///
    /// Use this on already-classified trigger text, such as a stored channel
    /// body being replayed as an ask. `inbound_trigger` is `{bot-id}:`.
    #[must_use]
    pub fn from_body(body: &str, inbound_trigger: &str) -> Option<Self> {
        if inbound_trigger.is_empty() {
            return None;
        }
        let body = body.trim_start();
        let (first, remainder) = split_first_token(body)?;
        let request = if first == inbound_trigger {
            remainder
        } else if first.starts_with('@') && first.len() > 1 {
            let (trigger, remainder) = split_first_token(remainder)?;
            (trigger == inbound_trigger).then_some(remainder)?
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

/// Channel body parsed from an occupant tell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OccupantTell {
    /// Unstamped prose to publish.
    pub body: String,
    /// Exact channel UUID or known slug. `None` is this session's channel.
    pub to: Option<String>,
}

/// Parse a nested `<botserver>` routing tag from an occupant tell body.
///
/// No tag posts the whole body. A tag posts only its inner text. More than
/// one tag, a malformed tag, or empty publishable text is `None`.
///
/// A marker preceded by a backslash is prose, not a tag boundary: the
/// occupant writes `\<botserver` to publish that literal text, and the
/// published body unescapes exactly that form (and `\</botserver>`). An
/// open marker inside a tag's inner text is published as-is, but a close
/// marker still ends the tag, so quoting one also takes the escape.
///
/// # Examples
///
/// ```
/// use botserver_domain::parse_occupant_tell;
///
/// let parsed = parse_occupant_tell(
///     "scratch\n<botserver to=\"eng\">\nqueue is clear\n</botserver>\n",
/// )
/// .expect("tag");
/// assert_eq!(parsed.body, "queue is clear");
/// assert_eq!(parsed.to.as_deref(), Some("eng"));
///
/// let parsed = parse_occupant_tell("the \\<botserver> tag, explained").expect("escaped");
/// assert_eq!(parsed.body, "the <botserver> tag, explained");
/// assert_eq!(parsed.to, None);
/// ```
#[must_use]
pub fn parse_occupant_tell(raw: &str) -> Option<OccupantTell> {
    const OPEN: &str = "<botserver";
    const CLOSE: &str = "</botserver>";
    let Some(open_at) = find_unescaped_marker(raw, OPEN, 0) else {
        let body = unescape_routing_markers(raw.trim());
        return (!body.is_empty()).then_some(OccupantTell { body, to: None });
    };
    let after_name = open_at + OPEN.len();
    let relative_gt = raw[after_name..].find('>')?;
    let attr = raw[after_name..after_name + relative_gt].trim();
    let to = if attr.is_empty() {
        None
    } else {
        let rest = attr.strip_prefix("to=\"")?;
        let value = rest.strip_suffix('"')?;
        (!value.is_empty()).then(|| value.to_owned())
    };
    let inner_at = after_name + relative_gt + 1;
    let close_at = find_unescaped_marker(raw, CLOSE, inner_at)?;
    if find_unescaped_marker(raw, OPEN, close_at + CLOSE.len()).is_some() {
        return None;
    }
    let body = unescape_routing_markers(raw[inner_at..close_at].trim());
    (!body.is_empty()).then_some(OccupantTell { body, to })
}

/// Find the next marker occurrence that is not escaped as prose.
fn find_unescaped_marker(raw: &str, marker: &str, from: usize) -> Option<usize> {
    let mut search_from = from;
    while let Some(rest) = raw.get(search_from..) {
        let found = rest.find(marker)?;
        let at = search_from + found;
        if !raw[..at].ends_with('\\') {
            return Some(at);
        }
        search_from = at + marker.len();
    }
    None
}

/// Undo the occupant's marker escapes in text that will be published.
fn unescape_routing_markers(text: &str) -> String {
    text.replace("\\<botserver", "<botserver")
        .replace("\\</botserver>", "</botserver>")
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
    fn bot_uses_id_as_inbound_trigger_and_rejects_empty_kind() {
        let id = BotId::new("bot").expect("bot");
        assert!(Bot::new(id.clone(), PathBuf::from("/corpus"), "").is_none());
        assert!(Bot::new(id.clone(), PathBuf::from("/corpus"), "open code").is_none());
        let bot = Bot::new(id, PathBuf::from("/corpus"), "opencode").expect("bot");
        assert_eq!(bot.inbound_trigger(), INBOUND_TRIGGER);
        assert_eq!(bot.outbound_prefix(), OUTBOUND_PREFIX);
        assert_eq!(bot.occupant_kind(), "opencode");
        assert_eq!(bot.corpus_path(), Path::new("/corpus"));
        let review = Bot::new(
            BotId::new("review").expect("id"),
            PathBuf::from("/corpus"),
            "opencode",
        )
        .expect("bot");
        assert_eq!(review.inbound_trigger(), "review:");
        assert_eq!(review.outbound_prefix(), "[review]:");
        assert_ne!(review.inbound_trigger(), INBOUND_TRIGGER);
        assert_ne!(review.outbound_prefix(), OUTBOUND_PREFIX);
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
    fn place_display_keeps_a_usable_stream_title() {
        assert_eq!(place_display("#eng", Some("Sebastian")), "#eng");
        assert!(!is_generic_dm_title("#eng"));
    }

    #[test]
    fn place_display_uses_peer_when_the_channel_title_is_generic_dm() {
        assert!(is_generic_dm_title("DM"));
        assert!(is_generic_dm_title(" dm "));
        assert_eq!(place_display("DM", Some("Sebastian")), "Sebastian");
        assert_eq!(place_display("DM", None), "DM");
        let bot = BotId::new("bot").expect("bot");
        let name = SessionName::from_bot_and_channel(
            &bot,
            "ab12cd34-5678-90ab-cdef-0123456789ab",
            &place_display("DM", Some("Sebastian")),
            |_| false,
        )
        .expect("name");
        assert_eq!(name.as_str(), "bot-sebastian");
    }

    #[test]
    fn trigger_requires_operator_p_tag_unless_operator_authored() {
        assert!(TriggerMatch::parse(
            "operator",
            "someone-else",
            ["someone-else"],
            INBOUND_TRIGGER,
            "bot: hello"
        )
        .is_none());
        assert_eq!(
            TriggerMatch::parse(
                "operator",
                "someone-else",
                ["someone-else", "operator"],
                INBOUND_TRIGGER,
                "bot: hello"
            )
            .map(|matched| matched.request),
            Some("hello".to_owned())
        );
        assert_eq!(
            TriggerMatch::parse(
                "operator",
                "operator",
                ["someone-else"],
                INBOUND_TRIGGER,
                "bot: hello"
            )
            .map(|matched| matched.request),
            Some("hello".to_owned())
        );
        assert_eq!(
            TriggerMatch::parse(
                "operator",
                "operator",
                ["someone-else"],
                INBOUND_TRIGGER,
                "@daniel bot: testing"
            )
            .map(|matched| matched.request),
            Some("testing".to_owned())
        );
        assert!(TriggerMatch::parse(
            "operator",
            "operator",
            ["someone-else"],
            INBOUND_TRIGGER,
            "[bot]: pong"
        )
        .is_none());
        assert!(TriggerMatch::parse(
            "operator",
            "operator",
            ["someone-else"],
            "pr:",
            "[pr]: pong"
        )
        .is_none());
        assert!(TriggerMatch::parse(
            "operator",
            "operator",
            ["someone-else"],
            "pr:",
            "[pr]: hello"
        )
        .is_none());
        assert_eq!(
            TriggerMatch::parse(
                "operator",
                "operator",
                ["someone-else"],
                "review:",
                "review: hello"
            )
            .map(|matched| matched.request),
            Some("hello".to_owned())
        );
        assert!(TriggerMatch::parse(
            "operator",
            "operator",
            ["someone-else"],
            "review:",
            "bot: hello"
        )
        .is_none());
    }

    #[test]
    fn trigger_allows_one_leading_mention() {
        let matched = TriggerMatch::parse(
            "operator",
            "someone-else",
            ["operator"],
            INBOUND_TRIGGER,
            "  @daniel\n bot:   review the PR  ",
        )
        .expect("trigger");

        assert_eq!(matched.request(), "review the PR");
        assert_eq!(
            TriggerMatch::from_body("@daniel bot: review the PR", INBOUND_TRIGGER)
                .expect("body")
                .request(),
            "review the PR"
        );
        assert_eq!(
            TriggerMatch::from_body("bot:", INBOUND_TRIGGER)
                .expect("empty")
                .request(),
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
                TriggerMatch::parse(
                    "operator",
                    "someone-else",
                    ["operator"],
                    INBOUND_TRIGGER,
                    body
                )
                .is_none(),
                "unexpected trigger: {body}"
            );
        }
    }

    #[test]
    fn stamp_outbound_prefixes_once() {
        assert_eq!(stamp_outbound("hello", OUTBOUND_PREFIX), "[bot]: hello");
        assert_eq!(
            stamp_outbound("  [bot]: already  ", OUTBOUND_PREFIX),
            "[bot]: already"
        );
        assert_eq!(
            stamp_outbound("[bot]:already", OUTBOUND_PREFIX),
            "[bot]:already"
        );
        assert_eq!(stamp_outbound("hello", "[pr]:"), "[pr]: hello");
        assert_eq!(
            stamp_outbound("  [pr]: already  ", "[pr]:"),
            "[pr]: already"
        );
        assert_eq!(stamp_outbound("[pr]:already", "[pr]:"), "[pr]:already");
        assert_eq!(
            stamp_outbound("[bot]: leftover", "[pr]:"),
            "[pr]: [bot]: leftover"
        );
        let pr = BotId::new("pr").expect("id");
        assert_eq!(outbound_prefix_for(&pr), "[pr]:");
        assert_eq!(
            outbound_prefix_for(&BotId::new("bot").expect("id")),
            OUTBOUND_PREFIX
        );
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

    #[test]
    fn occupant_tell_without_tag_posts_the_whole_body() {
        let parsed = parse_occupant_tell("  queue is clear  ").expect("body");
        assert_eq!(parsed.body, "queue is clear");
        assert_eq!(parsed.to, None);
    }

    #[test]
    fn occupant_tell_tag_posts_inner_text_and_drops_scratch() {
        let parsed = parse_occupant_tell(
            "scratch the human should not see\n\n<botserver to=\"eng\">\nqueue is clear except divine-mobile#8013\n</botserver>\n",
        )
        .expect("tag");
        assert_eq!(parsed.body, "queue is clear except divine-mobile#8013");
        assert_eq!(parsed.to.as_deref(), Some("eng"));
    }

    #[test]
    fn occupant_tell_tag_without_to_uses_this_session() {
        let parsed = parse_occupant_tell("<botserver>\nping\n</botserver>").expect("tag");
        assert_eq!(parsed.body, "ping");
        assert_eq!(parsed.to, None);
    }

    #[test]
    fn occupant_tell_rejects_empty_malformed_and_multiple_tags() {
        assert!(parse_occupant_tell("").is_none());
        assert!(parse_occupant_tell("   ").is_none());
        assert!(parse_occupant_tell("<botserver to=\"eng\"></botserver>").is_none());
        assert!(parse_occupant_tell("<botserver to=eng>x</botserver>").is_none());
        assert!(parse_occupant_tell("<botserver to=\"eng\">x").is_none());
        assert!(
            parse_occupant_tell("<botserver to=\"eng\">a</botserver><botserver>b</botserver>")
                .is_none()
        );
    }

    #[test]
    fn occupant_tell_escapes_the_marker_as_prose() {
        let parsed =
            parse_occupant_tell("route with the \\<botserver to=\"eng\"> tag").expect("escaped");
        assert_eq!(parsed.body, "route with the <botserver to=\"eng\"> tag");
        assert_eq!(parsed.to, None);

        let parsed = parse_occupant_tell("closes too: \\</botserver>").expect("escaped close");
        assert_eq!(parsed.body, "closes too: </botserver>");
        assert_eq!(parsed.to, None);
    }

    #[test]
    fn occupant_tell_inner_text_may_quote_the_marker() {
        let parsed = parse_occupant_tell(
            "<botserver to=\"eng\">use <botserver> tags, closed by \\</botserver>, carefully</botserver>",
        )
        .expect("routed quote");
        assert_eq!(
            parsed.body,
            "use <botserver> tags, closed by </botserver>, carefully"
        );
        assert_eq!(parsed.to.as_deref(), Some("eng"));

        let parsed = parse_occupant_tell("<botserver>escaped inner \\<botserver></botserver>")
            .expect("escaped inner");
        assert_eq!(parsed.body, "escaped inner <botserver>");
        assert_eq!(parsed.to, None);

        // An unescaped close marker inside the inner text still ends the
        // tag; the tail after it is scratch, as with any text outside.
        let parsed =
            parse_occupant_tell("<botserver to=\"eng\">quote: </botserver> tail</botserver>")
                .expect("early close");
        assert_eq!(parsed.body, "quote:");
        assert_eq!(parsed.to.as_deref(), Some("eng"));
    }

    #[test]
    fn occupant_tell_still_refuses_a_second_real_tag_after_an_escaped_one() {
        assert!(parse_occupant_tell(
            "\\</botserver> prose <botserver to=\"eng\">a</botserver><botserver>b</botserver>"
        )
        .is_none());
        assert!(parse_occupant_tell("<botserver>a</botserver> then \\<botserver> ok").is_some());
    }
}
