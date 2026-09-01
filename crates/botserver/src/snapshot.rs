//! Host-written per-channel place snapshots for occupant renew.

use std::fmt::Write as _;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::IndexedRelayEvent;

/// Inclusive last-N-days window written into each place snapshot.
pub const PLACE_SNAPSHOT_WINDOW_SECS: i64 = 7 * 24 * 60 * 60;

const STARTUP_BEGIN: &str = "<!-- botserver-place-snapshots -->";
const STARTUP_END: &str = "<!-- /botserver-place-snapshots -->";
const STARTUP_BLOCK: &str = "<!-- botserver-place-snapshots -->
Read `.botserver/places/<your public Kelpie name>.md` for the last 7 days in this channel. Do not read other place files.
<!-- /botserver-place-snapshots -->
";

/// Relative corpus path of one session's snapshot file.
#[must_use]
pub fn place_snapshot_relpath(session_name: &str) -> Option<String> {
    snapshot_file_stem(session_name).map(|name| format!(".botserver/places/{name}.md"))
}

/// Render last-N-days events for exactly one channel.
#[must_use]
pub fn render_place_snapshot(
    session_name: &str,
    channel_id: &str,
    now_unix: i64,
    events: &[IndexedRelayEvent],
) -> String {
    let cutoff = now_unix.saturating_sub(PLACE_SNAPSHOT_WINDOW_SECS);
    let mut body = format!(
        "# Channel snapshot\n\nSession: {session_name}\nChannel: {channel_id}\nWindow: last 7 days\n\n"
    );
    let mut wrote_event = false;
    for event in events {
        if event.channel_id.as_deref() != Some(channel_id) {
            continue;
        }
        if event.created_at < cutoff {
            continue;
        }
        if !wrote_event {
            body.push_str("## Events\n\n");
            wrote_event = true;
        }
        let _ = write!(
            body,
            "- created_at={} kind={} author={}\n  {}\n",
            event.created_at,
            event.kind,
            event.author_pubkey,
            event.content.replace('\n', "\n  ")
        );
    }
    if !wrote_event {
        body.push_str("No indexed events in this window.\n");
    }
    body
}

/// Write a session snapshot under the corpus and point `startup.md` at it.
///
/// # Errors
///
/// Returns an I/O error when the snapshot or `startup.md` cannot be written.
pub fn refresh_place_snapshot(
    corpus: &Path,
    session_name: &str,
    markdown: &str,
) -> io::Result<PathBuf> {
    let relpath = place_snapshot_relpath(session_name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "session name is not a safe snapshot filename",
        )
    })?;
    let path = corpus.join(&relpath);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("md.tmp");
    {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(markdown.as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    point_startup_at_snapshots(corpus)?;
    Ok(path)
}

fn snapshot_file_stem(session_name: &str) -> Option<&str> {
    if session_name.is_empty() {
        return None;
    }
    session_name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '-')
        .then_some(session_name)
}

fn point_startup_at_snapshots(corpus: &Path) -> io::Result<()> {
    let path = corpus.join("startup.md");
    let existing = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error),
    };
    let updated = upsert_startup_block(&existing);
    if updated == existing {
        return Ok(());
    }
    fs::write(path, updated)
}

fn upsert_startup_block(existing: &str) -> String {
    if let (Some(start), Some(end)) = (existing.find(STARTUP_BEGIN), existing.find(STARTUP_END)) {
        if start < end {
            let mut updated = String::new();
            updated.push_str(&existing[..start]);
            updated.push_str(STARTUP_BLOCK.trim_end());
            updated.push_str(&existing[end + STARTUP_END.len()..]);
            return updated;
        }
    }
    if existing.is_empty() {
        return STARTUP_BLOCK.to_owned();
    }
    let mut updated = existing.to_owned();
    if !updated.ends_with('\n') {
        updated.push('\n');
    }
    updated.push('\n');
    updated.push_str(STARTUP_BLOCK);
    updated
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use botserver_domain::EventId;

    use super::*;
    use crate::IndexedRelayEvent;

    fn event_id(character: char) -> EventId {
        EventId::parse_hex(&character.to_string().repeat(64)).expect("event")
    }

    fn event(channel: &str, created_at: i64, content: &str, character: char) -> IndexedRelayEvent {
        IndexedRelayEvent {
            event_id: event_id(character),
            author_pubkey: "b".repeat(64),
            created_at,
            kind: 9,
            content: content.to_owned(),
            tags_json: "[]".to_owned(),
            channel_id: Some(channel.to_owned()),
            target_event_id: None,
        }
    }

    fn temp_corpus() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "botserver-snapshot-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).expect("corpus");
        path
    }

    #[test]
    fn snapshot_omits_other_channels_including_dms() {
        let channel = "ab12cd34-5678-90ab-cdef-0123456789ab";
        let dm = "ffffffff-ffff-ffff-ffff-ffffffffffff";
        let now = 1_800_000_000;
        let rendered = render_place_snapshot(
            "bot-foobar",
            channel,
            now,
            &[
                event(channel, now - 60, "channel hello", 'a'),
                event(dm, now - 30, "secret dm", 'b'),
                event(channel, now - PLACE_SNAPSHOT_WINDOW_SECS - 1, "old", 'c'),
            ],
        );
        assert!(rendered.contains("channel hello"));
        assert!(!rendered.contains("secret dm"));
        assert!(!rendered.contains(dm));
        assert!(!rendered.contains("old"));
    }

    #[test]
    fn refresh_writes_snapshot_and_points_startup_md() {
        let corpus = temp_corpus();
        let path =
            refresh_place_snapshot(&corpus, "bot-foobar", "# Channel snapshot\n").expect("write");
        assert_eq!(path, corpus.join(".botserver/places/bot-foobar.md"));
        assert_eq!(
            fs::read_to_string(&path).expect("snapshot"),
            "# Channel snapshot\n"
        );
        let startup = fs::read_to_string(corpus.join("startup.md")).expect("startup");
        assert!(startup.contains(".botserver/places/<your public Kelpie name>.md"));
        assert!(startup.contains(STARTUP_BEGIN));
    }

    #[test]
    fn refresh_rejects_unsafe_session_names() {
        let corpus = temp_corpus();
        let error = refresh_place_snapshot(&corpus, "../other", "x").expect_err("unsafe");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
