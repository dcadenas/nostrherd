//! Buzz event shapes the host publishes over its own nostr connection
//! (D43).
//!
//! Shapes mirror the reference builders in buzz
//! `crates/buzz-sdk/src/builders.rs` (`build_message`, `build_edit`,
//! `build_delete_message`, `build_reaction`, `build_remove_reaction`)
//! and D23's relay contract pins them. The live E2E publishes one event
//! of each kind against the local relay so schema drift fails tests,
//! not production. Pure data only: signing and relay I/O live in
//! adapters.

use crate::EventId;

/// Buzz stream message (kind 9).
pub const CHANNEL_MESSAGE_KIND: u16 = 9;
/// Buzz message edit (kind 40003).
pub const MESSAGE_EDIT_KIND: u16 = 40_003;
/// Buzz delete (kind 9005).
pub const MESSAGE_DELETE_KIND: u16 = 9_005;
/// NIP-25 reaction (kind 7).
pub const REACTION_KIND: u16 = 7;
/// NIP-09 deletion of a reaction event (kind 5).
pub const REACTION_REMOVAL_KIND: u16 = 5;

/// A Buzz-shaped relay event, ready for an adapter to sign and publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuzzEvent {
    kind: u16,
    content: String,
    tags: Vec<Vec<String>>,
}

impl BuzzEvent {
    /// Relay event kind.
    #[must_use]
    pub fn kind(&self) -> u16 {
        self.kind
    }

    /// Event content.
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Event tags in wire order.
    #[must_use]
    pub fn tags(&self) -> &[Vec<String>] {
        &self.tags
    }
}

/// Build a channel message (kind 9; buzz `build_message`).
///
/// Tag order follows the reference builder: `h`, then NIP-10 `e` thread
/// tags, then the mention `p`. `thread_tags` comes from
/// [`reply_thread_tags`]; a top-level post carries none.
#[must_use]
pub fn channel_message(
    channel_id: &str,
    content: &str,
    thread_tags: &[Vec<String>],
    mention: Option<&str>,
) -> BuzzEvent {
    let mut tags = vec![vec!["h".to_owned(), channel_id.to_owned()]];
    tags.extend(thread_tags.iter().cloned());
    if let Some(pubkey) = mention.map(str::trim).filter(|pubkey| !pubkey.is_empty()) {
        tags.push(vec!["p".to_owned(), pubkey.to_ascii_lowercase()]);
    }
    BuzzEvent {
        kind: CHANNEL_MESSAGE_KIND,
        content: content.to_owned(),
        tags,
    }
}

/// Build an edit event (kind 40003; buzz `build_edit`): `h` + `e`.
///
/// The content is the full replacement text.
#[must_use]
pub fn message_edit(channel_id: &str, target_event_id: &EventId, content: &str) -> BuzzEvent {
    BuzzEvent {
        kind: MESSAGE_EDIT_KIND,
        content: content.to_owned(),
        tags: vec![
            vec!["h".to_owned(), channel_id.to_owned()],
            vec!["e".to_owned(), target_event_id.as_str().to_owned()],
        ],
    }
}

/// Build a Buzz delete (kind 9005; buzz `build_delete_message`): `h` + `e`.
#[must_use]
pub fn message_delete(channel_id: &str, target_event_id: &EventId) -> BuzzEvent {
    BuzzEvent {
        kind: MESSAGE_DELETE_KIND,
        content: String::new(),
        tags: vec![
            vec!["h".to_owned(), channel_id.to_owned()],
            vec!["e".to_owned(), target_event_id.as_str().to_owned()],
        ],
    }
}

/// Build a NIP-25 reaction (kind 7; buzz `build_reaction`): `e` + emoji.
#[must_use]
pub fn reaction(target_event_id: &EventId, emoji: &str) -> BuzzEvent {
    BuzzEvent {
        kind: REACTION_KIND,
        content: emoji.to_owned(),
        tags: vec![vec!["e".to_owned(), target_event_id.as_str().to_owned()]],
    }
}

/// Build the kind-5 removal of a reaction (buzz `build_remove_reaction`).
#[must_use]
pub fn reaction_removal(reaction_event_id: &EventId) -> BuzzEvent {
    BuzzEvent {
        kind: REACTION_REMOVAL_KIND,
        content: String::new(),
        tags: vec![vec!["e".to_owned(), reaction_event_id.as_str().to_owned()]],
    }
}

/// Resolve the thread root a reply to `trigger_event_id` must carry.
///
/// Marked `e` tags follow Buzz semantics (`root` + `reply` names the
/// root; `reply` only makes the reply target the root). If no valid
/// markers exist, the first valid unmarked `e` tag is the legacy
/// positional root. A trigger without either form is top-level, so the
/// reply falls back to a reply marker pointing at the trigger alone.
/// `None` means the reply carries a reply marker only.
#[must_use]
pub fn reply_thread_root(
    trigger_event_id: &EventId,
    trigger_tags: &[Vec<String>],
) -> Option<EventId> {
    let mut markers = (None, None);
    let mut legacy_root = None;
    for tag in trigger_tags {
        let [name, value, rest @ ..] = tag.as_slice() else {
            continue;
        };
        if name != "e" {
            continue;
        }
        let Some(id) = EventId::parse_hex(value) else {
            continue;
        };
        let marker = rest.get(1).map(String::as_str);
        match marker {
            Some("root") => markers.0 = Some(id),
            Some("reply") => markers.1 = Some(id),
            None if rest.len() <= 1 && legacy_root.is_none() => legacy_root = Some(id),
            _ => {}
        }
    }
    match markers {
        (Some(root), Some(_)) if root != *trigger_event_id => Some(root),
        (None, Some(reply)) if reply != *trigger_event_id => Some(reply),
        (None, None) => legacy_root.filter(|root| *root != *trigger_event_id),
        _ => None,
    }
}

/// NIP-10 `e` tags for a reply event to `trigger_event_id`.
///
/// `thread_root` is the resolved [`reply_thread_root`]. A direct reply
/// (no separate root) emits `["e", trigger, "", "reply"]`; a nested
/// reply emits the root marker first, then the reply marker, matching
/// buzz `thread_tags`.
#[must_use]
pub fn reply_thread_tags(
    trigger_event_id: &EventId,
    thread_root: Option<&EventId>,
) -> Vec<Vec<String>> {
    match thread_root.filter(|root| **root != *trigger_event_id) {
        Some(root) => vec![
            vec![
                "e".to_owned(),
                root.as_str().to_owned(),
                String::new(),
                "root".to_owned(),
            ],
            vec![
                "e".to_owned(),
                trigger_event_id.as_str().to_owned(),
                String::new(),
                "reply".to_owned(),
            ],
        ],
        None => vec![vec![
            "e".to_owned(),
            trigger_event_id.as_str().to_owned(),
            String::new(),
            "reply".to_owned(),
        ]],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event_id(character: char) -> EventId {
        EventId::parse_hex(&character.to_string().repeat(64)).expect("event id")
    }

    fn trigger_tags(tags: &[&[&str]]) -> Vec<Vec<String>> {
        tags.iter()
            .map(|tag| tag.iter().map(ToString::to_string).collect())
            .collect()
    }

    #[test]
    fn channel_message_orders_h_thread_and_mention_tags() {
        let trigger = event_id('a');
        let root = event_id('b');
        let thread = reply_thread_tags(&trigger, Some(&root));
        let event = channel_message(
            "ch-1",
            "**[bot]**: hi",
            &thread,
            Some("C".repeat(64).as_str()),
        );
        assert_eq!(event.kind(), CHANNEL_MESSAGE_KIND);
        assert_eq!(event.content(), "**[bot]**: hi");
        assert_eq!(
            event.tags(),
            &[
                vec!["h".to_owned(), "ch-1".to_owned()],
                vec![
                    "e".to_owned(),
                    root.as_str().to_owned(),
                    String::new(),
                    "root".to_owned()
                ],
                vec![
                    "e".to_owned(),
                    trigger.as_str().to_owned(),
                    String::new(),
                    "reply".to_owned()
                ],
                vec!["p".to_owned(), "c".repeat(64)],
            ]
        );
    }

    #[test]
    fn channel_message_skips_empty_mention_and_thread() {
        let event = channel_message("ch-1", "hello", &[], Some("  "));
        assert_eq!(event.tags(), &[vec!["h".to_owned(), "ch-1".to_owned()]]);

        let event = channel_message("ch-1", "hello", &[], None);
        assert_eq!(event.tags(), &[vec!["h".to_owned(), "ch-1".to_owned()]]);
    }

    #[test]
    fn message_edit_delete_reaction_and_removal_shapes() {
        let target = event_id('d');
        let edit = message_edit("ch-1", &target, "new text");
        assert_eq!(edit.kind(), MESSAGE_EDIT_KIND);
        assert_eq!(edit.content(), "new text");
        assert_eq!(
            edit.tags(),
            &[
                vec!["h".to_owned(), "ch-1".to_owned()],
                vec!["e".to_owned(), target.as_str().to_owned()],
            ]
        );

        let delete = message_delete("ch-1", &target);
        assert_eq!(delete.kind(), MESSAGE_DELETE_KIND);
        assert_eq!(delete.content(), "");
        assert_eq!(
            delete.tags(),
            &[
                vec!["h".to_owned(), "ch-1".to_owned()],
                vec!["e".to_owned(), target.as_str().to_owned()],
            ]
        );

        let reaction = reaction(&target, "⏳");
        assert_eq!(reaction.kind(), REACTION_KIND);
        assert_eq!(reaction.content(), "⏳");
        assert_eq!(
            reaction.tags(),
            &[vec!["e".to_owned(), target.as_str().to_owned()]]
        );

        let removal = reaction_removal(&target);
        assert_eq!(removal.kind(), REACTION_REMOVAL_KIND);
        assert_eq!(removal.content(), "");
        assert_eq!(
            removal.tags(),
            &[vec!["e".to_owned(), target.as_str().to_owned()]]
        );
    }

    #[test]
    fn nested_trigger_yields_root_and_reply_tags() {
        let trigger = event_id('a');
        let root = event_id('b');
        let tags = trigger_tags(&[
            &["e", root.as_str(), "", "root"],
            &["e", trigger.as_str(), "", "reply"],
        ]);
        let resolved = reply_thread_root(&trigger, &tags);
        assert_eq!(resolved.as_ref(), Some(&root));
        assert_eq!(
            reply_thread_tags(&trigger, resolved.as_ref()),
            vec![
                vec![
                    "e".to_owned(),
                    root.as_str().to_owned(),
                    String::new(),
                    "root".to_owned()
                ],
                vec![
                    "e".to_owned(),
                    trigger.as_str().to_owned(),
                    String::new(),
                    "reply".to_owned()
                ],
            ]
        );
    }

    #[test]
    fn extended_nip10_tags_still_resolve_the_thread_root() {
        let trigger = event_id('a');
        let root = event_id('b');
        let tags = trigger_tags(&[
            &["e", root.as_str(), "", "root", &"c".repeat(64)],
            &["e", trigger.as_str(), "", "reply", &"d".repeat(64)],
        ]);
        assert_eq!(reply_thread_root(&trigger, &tags), Some(root));
    }

    #[test]
    fn reply_only_trigger_is_its_own_root() {
        // A direct reply's reply marker points at the thread root, which is
        // the trigger itself: the reply falls back to a reply marker only.
        let trigger = event_id('a');
        let tags = trigger_tags(&[&["e", trigger.as_str(), "", "reply"]]);
        assert_eq!(reply_thread_root(&trigger, &tags), None);
        assert_eq!(
            reply_thread_tags(&trigger, None),
            vec![vec![
                "e".to_owned(),
                trigger.as_str().to_owned(),
                String::new(),
                "reply".to_owned(),
            ]]
        );
    }

    #[test]
    fn legacy_id_only_tags_recover_the_first_thread_root() {
        let trigger = event_id('a');
        let root = event_id('b');
        let parent = event_id('c');
        let id_only = trigger_tags(&[
            &["e", root.as_str()],
            &["e", parent.as_str(), "wss://relay.example"],
        ]);
        assert_eq!(reply_thread_root(&trigger, &id_only), Some(root.clone()));

        let malformed_first = trigger_tags(&[["e", "bad"].as_slice(), &["e", root.as_str()]]);
        assert_eq!(reply_thread_root(&trigger, &malformed_first), Some(root));

        let self_reference = trigger_tags(&[&["e", trigger.as_str()]]);
        assert_eq!(reply_thread_root(&trigger, &self_reference), None);
    }

    #[test]
    fn markerless_trigger_falls_back_to_reply_marker_only() {
        let trigger = event_id('a');
        let markerless = trigger_tags(&[&["h", "ch-1"], &["p", &"c".repeat(64)]]);
        assert_eq!(reply_thread_root(&trigger, &markerless), None);
        assert_eq!(
            reply_thread_tags(&trigger, None),
            vec![vec![
                "e".to_owned(),
                trigger.as_str().to_owned(),
                String::new(),
                "reply".to_owned(),
            ]]
        );
    }

    #[test]
    fn root_only_and_malformed_markers_are_top_level() {
        let trigger = event_id('a');
        let root_only = trigger_tags(&[&["e", &"b".repeat(64), "", "root"]]);
        assert_eq!(reply_thread_root(&trigger, &root_only), None);
        let malformed = trigger_tags(&[
            &["e", "bad", "", "reply"],
            &["e", &"b".repeat(64), "", "root"],
        ]);
        assert_eq!(reply_thread_root(&trigger, &malformed), None);
    }

    #[test]
    fn marked_tags_take_precedence_over_legacy_positions() {
        let trigger = event_id('a');
        let legacy = event_id('b');
        let root = event_id('c');
        let tags = trigger_tags(&[
            &["e", legacy.as_str()],
            &["e", root.as_str(), "", "root"],
            &["e", trigger.as_str(), "", "reply"],
        ]);
        assert_eq!(reply_thread_root(&trigger, &tags), Some(root.clone()));

        let root_only = trigger_tags(&[&["e", legacy.as_str()], &["e", root.as_str(), "", "root"]]);
        assert_eq!(reply_thread_root(&trigger, &root_only), None);

        let reply_only =
            trigger_tags(&[&["e", legacy.as_str()], &["e", root.as_str(), "", "reply"]]);
        assert_eq!(reply_thread_root(&trigger, &reply_only), Some(root));
    }
}
