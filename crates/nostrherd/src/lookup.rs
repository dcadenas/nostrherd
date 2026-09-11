//! Occupant-facing channel search issued by the running host (D72).

use std::fmt::Write as _;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::relay::RelaySubscriber;
use crate::sqlite::SqliteRepository;
use crate::HostRepository;

/// Maximum kind-9 hits returned for one occupant search.
pub const SEARCH_LIMIT: usize = 20;

/// Occupant-visible heading for a completed search with zero hits.
pub const NO_RESULTS_HEADING: &str = "## No results";

/// Occupant-visible heading for a lookup that did not complete.
pub const COULD_NOT_CHECK_HEADING: &str = "## Could not check";

const MAX_QUERY_CHARS: usize = 512;
const MAX_REQUEST_BYTES: u64 = 8192;
const SOCKET_TIMEOUT: Duration = Duration::from_secs(10);

/// One kind-9 hit the occupant can name as a checked surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookupHit {
    pub created_at: i64,
    pub kind: u16,
    pub author_pubkey: String,
    pub content: String,
}

/// Result of one occupant channel search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupOutcome {
    /// The relay returned matching kind-9 events.
    Matches {
        session: String,
        channel_id: String,
        query: String,
        events: Vec<LookupHit>,
    },
    /// The relay completed the search and found nothing.
    NoResults {
        session: String,
        channel_id: String,
        query: String,
    },
    /// The lookup did not complete.
    Failed { reason: String },
}

#[derive(Debug, Serialize, Deserialize)]
struct WireRequest {
    session: String,
    query: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status")]
enum WireResponse {
    #[serde(rename = "matches")]
    Matches {
        session: String,
        channel: String,
        query: String,
        events: Vec<LookupHit>,
    },
    #[serde(rename = "no-results")]
    NoResults {
        session: String,
        channel: String,
        query: String,
    },
    #[serde(rename = "failed")]
    Failed { reason: String },
}

impl From<LookupOutcome> for WireResponse {
    fn from(outcome: LookupOutcome) -> Self {
        match outcome {
            LookupOutcome::Matches {
                session,
                channel_id,
                query,
                events,
            } => Self::Matches {
                session,
                channel: channel_id,
                query,
                events,
            },
            LookupOutcome::NoResults {
                session,
                channel_id,
                query,
            } => Self::NoResults {
                session,
                channel: channel_id,
                query,
            },
            LookupOutcome::Failed { reason } => Self::Failed { reason },
        }
    }
}

impl From<WireResponse> for LookupOutcome {
    fn from(response: WireResponse) -> Self {
        match response {
            WireResponse::Matches {
                session,
                channel,
                query,
                events,
            } => Self::Matches {
                session,
                channel_id: channel,
                query,
                events,
            },
            WireResponse::NoResults {
                session,
                channel,
                query,
            } => Self::NoResults {
                session,
                channel_id: channel,
                query,
            },
            WireResponse::Failed { reason } => Self::Failed { reason },
        }
    }
}

/// Trim and reject an occupant search query that cannot be issued.
///
/// # Errors
///
/// Returns a reason when the query is empty, too long, or contains NUL.
pub fn parse_lookup_query(raw: &str) -> Result<String, String> {
    let query = raw.trim();
    if query.is_empty() {
        return Err("query is empty".to_owned());
    }
    if query.chars().count() > MAX_QUERY_CHARS {
        return Err("query is too long".to_owned());
    }
    if query.contains('\0') {
        return Err("query is invalid".to_owned());
    }
    Ok(query.to_owned())
}

/// Render an occupant-facing search body.
#[must_use]
pub fn render_lookup(outcome: &LookupOutcome) -> String {
    match outcome {
        LookupOutcome::Matches {
            session,
            channel_id,
            query,
            events,
        } => {
            let mut body = lookup_header(session, channel_id, query);
            body.push_str("## Results\n\n");
            for event in events {
                let _ = write!(
                    body,
                    "- created_at={} kind={} author={}\n  {}\n",
                    event.created_at,
                    event.kind,
                    event.author_pubkey,
                    event.content.replace('\n', "\n  ")
                );
            }
            body
        }
        LookupOutcome::NoResults {
            session,
            channel_id,
            query,
        } => {
            let mut body = lookup_header(session, channel_id, query);
            body.push_str(NO_RESULTS_HEADING);
            body.push('\n');
            body.push_str("No indexed kind-9 matches in this channel for that query.\n");
            body
        }
        LookupOutcome::Failed { reason } => {
            format!("{COULD_NOT_CHECK_HEADING}\nCould not check: {reason}\n")
        }
    }
}

fn lookup_header(session: &str, channel_id: &str, query: &str) -> String {
    format!(
        "# Channel search\n\nSession: {session}\nChannel: {channel_id}\nQuery: {query}\nSurface: NIP-50 kind 9 in this channel (not the 7-day snapshot; gift-wrapped DMs are not indexed)\n\n"
    )
}

/// Resolve a search after the query has been parsed.
pub fn resolve_lookup(
    session: &str,
    query: &str,
    channel_for: impl FnOnce(&str) -> Result<Option<String>, String>,
    search: impl FnOnce(&str, &str) -> Result<Vec<LookupHit>, String>,
) -> LookupOutcome {
    let query = match parse_lookup_query(query) {
        Ok(query) => query,
        Err(reason) => return LookupOutcome::Failed { reason },
    };
    let channel_id = match channel_for(session) {
        Ok(Some(channel_id)) => channel_id,
        Ok(None) => {
            return LookupOutcome::Failed {
                reason: "unknown session".to_owned(),
            }
        }
        Err(reason) => return LookupOutcome::Failed { reason },
    };
    match search(&channel_id, &query) {
        Ok(events) if events.is_empty() => LookupOutcome::NoResults {
            session: session.to_owned(),
            channel_id,
            query,
        },
        Ok(events) => LookupOutcome::Matches {
            session: session.to_owned(),
            channel_id,
            query,
            events,
        },
        Err(reason) => LookupOutcome::Failed { reason },
    }
}

/// Conventional lookup socket path.
///
/// Absent unless `NOSTRHERD_LOOKUP_SOCKET` or `XDG_RUNTIME_DIR` is set. The
/// host does not bind under `/tmp`.
#[must_use]
pub fn default_lookup_socket() -> Option<PathBuf> {
    lookup_socket_path(
        std::env::var_os("NOSTRHERD_LOOKUP_SOCKET"),
        std::env::var_os("XDG_RUNTIME_DIR"),
    )
}

fn lookup_socket_path(
    explicit: Option<std::ffi::OsString>,
    runtime: Option<std::ffi::OsString>,
) -> Option<PathBuf> {
    if let Some(path) = explicit.filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(path));
    }
    let runtime = runtime.filter(|value| !value.is_empty())?;
    Some(PathBuf::from(runtime).join("nostrherd/lookup.sock"))
}

/// Ask the running host to search one session's channel.
#[must_use]
pub fn request_lookup(socket: &Path, session: &str, query: &str) -> LookupOutcome {
    match request_lookup_io(socket, session, query) {
        Ok(outcome) => outcome,
        Err(error) if error.kind() == io::ErrorKind::NotFound => LookupOutcome::Failed {
            reason: "host lookup socket is not listening".to_owned(),
        },
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => LookupOutcome::Failed {
            reason: "host lookup socket is not listening".to_owned(),
        },
        Err(_) => LookupOutcome::Failed {
            reason: "host lookup socket is not listening".to_owned(),
        },
    }
}

fn request_lookup_io(socket: &Path, session: &str, query: &str) -> io::Result<LookupOutcome> {
    let mut stream = UnixStream::connect(socket)?;
    stream.set_read_timeout(Some(SOCKET_TIMEOUT))?;
    stream.set_write_timeout(Some(SOCKET_TIMEOUT))?;
    let request = WireRequest {
        session: session.to_owned(),
        query: query.to_owned(),
    };
    let encoded = serde_json::to_string(&request).map_err(json_error)?;
    writeln!(stream, "{encoded}")?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    if line.trim().is_empty() {
        return Ok(LookupOutcome::Failed {
            reason: "lookup reply is invalid".to_owned(),
        });
    }
    let response = serde_json::from_str::<WireResponse>(line.trim())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "lookup reply is invalid"))?;
    Ok(response.into())
}

fn json_error(error: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

/// Serve occupant search requests on a unix socket.
pub fn spawn_lookup_server(socket: PathBuf, database: PathBuf, subscriber: RelaySubscriber) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        eprintln!("channel lookup socket needs the host runtime");
        return;
    };
    thread::spawn(move || {
        if let Err(error) = serve_lookup(&socket, &database, &subscriber, &handle) {
            eprintln!("channel lookup socket failed: {error}");
        }
    });
}

fn serve_lookup(
    socket: &Path,
    database: &Path,
    subscriber: &RelaySubscriber,
    handle: &tokio::runtime::Handle,
) -> io::Result<()> {
    if let Some(parent) = socket.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match std::fs::remove_file(socket) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = UnixListener::bind(socket)?;
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let database = database.to_path_buf();
                let subscriber = subscriber.clone();
                let handle = handle.clone();
                thread::spawn(move || handle_connection(stream, &database, &subscriber, &handle));
            }
            Err(error) => eprintln!("channel lookup accept failed: {error}"),
        }
    }
    Ok(())
}

fn handle_connection(
    stream: UnixStream,
    database: &Path,
    subscriber: &RelaySubscriber,
    handle: &tokio::runtime::Handle,
) {
    if let Err(error) = answer_connection(stream, database, subscriber, handle) {
        eprintln!("channel lookup request failed: {error}");
    }
}

fn answer_connection(
    stream: UnixStream,
    database: &Path,
    subscriber: &RelaySubscriber,
    handle: &tokio::runtime::Handle,
) -> io::Result<()> {
    stream.set_read_timeout(Some(SOCKET_TIMEOUT))?;
    stream.set_write_timeout(Some(SOCKET_TIMEOUT))?;
    let mut reader = BufReader::new(stream.try_clone()?.take(MAX_REQUEST_BYTES));
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let outcome = if line.ends_with('\n') {
        match serde_json::from_str::<WireRequest>(line.trim()) {
            Ok(request) => lookup_from_host(&request, database, subscriber, handle),
            Err(_) => LookupOutcome::Failed {
                reason: "lookup request is invalid".to_owned(),
            },
        }
    } else {
        LookupOutcome::Failed {
            reason: "lookup request is invalid".to_owned(),
        }
    };
    let encoded = serde_json::to_string(&WireResponse::from(outcome)).map_err(json_error)?;
    let mut writer = stream;
    writeln!(writer, "{encoded}")
}

fn lookup_from_host(
    request: &WireRequest,
    database: &Path,
    subscriber: &RelaySubscriber,
    handle: &tokio::runtime::Handle,
) -> LookupOutcome {
    resolve_lookup(
        &request.session,
        &request.query,
        |session| match SqliteRepository::open(database) {
            Ok(repository) => repository
                .session_by_name(session)
                .map(|found| found.map(|record| record.channel_id))
                .map_err(|_| "host database unavailable".to_owned()),
            Err(_) => Err("host database unavailable".to_owned()),
        },
        |channel_id, query| {
            handle.block_on(async {
                subscriber
                    .search_channel(channel_id, query)
                    .await
                    .map(|events| {
                        events
                            .into_iter()
                            .map(|event| LookupHit {
                                created_at: i64::try_from(event.created_at.as_secs())
                                    .unwrap_or(i64::MAX),
                                kind: event.kind.as_u16(),
                                author_pubkey: event.pubkey.to_hex(),
                                content: event.content,
                            })
                            .collect()
                    })
                    .map_err(|_| "channel search failed".to_owned())
            })
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_results() -> LookupOutcome {
        LookupOutcome::NoResults {
            session: "bot-foobar".to_owned(),
            channel_id: "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
            query: "old thread".to_owned(),
        }
    }

    #[test]
    fn empty_search_is_not_rendered_as_failure() {
        let rendered = render_lookup(&no_results());
        assert!(rendered.contains(NO_RESULTS_HEADING), "{rendered}");
        assert!(rendered.contains("No indexed kind-9 matches"), "{rendered}");
        assert!(!rendered.contains(COULD_NOT_CHECK_HEADING), "{rendered}");
        assert!(!rendered.contains("Could not check:"), "{rendered}");
    }

    #[test]
    fn failed_search_is_not_rendered_as_empty() {
        let rendered = render_lookup(&LookupOutcome::Failed {
            reason: "host lookup socket is not listening".to_owned(),
        });
        assert!(rendered.contains(COULD_NOT_CHECK_HEADING), "{rendered}");
        assert!(
            rendered.contains("Could not check: host lookup socket is not listening"),
            "{rendered}"
        );
        assert!(!rendered.contains(NO_RESULTS_HEADING), "{rendered}");
        assert!(
            !rendered.contains("No indexed kind-9 matches"),
            "{rendered}"
        );
    }

    #[test]
    fn missing_socket_is_could_not_check_not_no_results() {
        let socket = std::env::temp_dir().join(format!(
            "nostrherd-lookup-missing-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let rendered = render_lookup(&request_lookup(&socket, "bot-foobar", "hello"));
        assert!(rendered.contains(COULD_NOT_CHECK_HEADING), "{rendered}");
        assert!(!rendered.contains(NO_RESULTS_HEADING), "{rendered}");
    }

    #[test]
    fn unknown_session_is_could_not_check_not_no_results() {
        let outcome = resolve_lookup(
            "bot-missing",
            "hello",
            |_| Ok(None),
            |_, _| panic!("search must not run for an unknown session"),
        );
        let rendered = render_lookup(&outcome);
        assert!(rendered.contains(COULD_NOT_CHECK_HEADING), "{rendered}");
        assert!(!rendered.contains(NO_RESULTS_HEADING), "{rendered}");
    }

    #[test]
    fn completed_empty_search_stays_no_results() {
        let outcome = resolve_lookup(
            "bot-foobar",
            "hello",
            |_| Ok(Some("channel".to_owned())),
            |channel, query| {
                assert_eq!(channel, "channel");
                assert_eq!(query, "hello");
                Ok(Vec::new())
            },
        );
        let rendered = render_lookup(&outcome);
        assert!(rendered.contains(NO_RESULTS_HEADING), "{rendered}");
        assert!(!rendered.contains(COULD_NOT_CHECK_HEADING), "{rendered}");
    }

    #[test]
    fn parse_lookup_query_rejects_empty_and_keeps_text() {
        assert_eq!(parse_lookup_query("  old bug  ").as_deref(), Ok("old bug"));
        assert_eq!(
            parse_lookup_query("   ").unwrap_err().as_str(),
            "query is empty"
        );
        assert!(parse_lookup_query(&"x".repeat(MAX_QUERY_CHARS + 1)).is_err());
    }

    #[test]
    fn lookup_server_unknown_session_is_could_not_check() {
        use nostr_sdk::prelude::Client;

        use crate::relay::RelaySubscriber;
        use crate::sqlite::SqliteRepository;

        let root = std::env::temp_dir().join(format!(
            "nostrherd-lookup-server-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("dir");
        let database = root.join("host.sqlite");
        SqliteRepository::open(&database).expect("database");
        let socket = root.join("lookup.sock");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let _enter = runtime.enter();
        spawn_lookup_server(
            socket.clone(),
            database,
            RelaySubscriber::new(Client::new()),
        );
        let started = std::time::Instant::now();
        while !socket.exists() {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(2),
                "lookup socket did not bind"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let rendered = render_lookup(&request_lookup(&socket, "bot-missing", "hello"));
        assert!(rendered.contains(COULD_NOT_CHECK_HEADING), "{rendered}");
        assert!(!rendered.contains(NO_RESULTS_HEADING), "{rendered}");
    }

    #[test]
    fn default_socket_honors_explicit_override() {
        let path = lookup_socket_path(
            Some("/run/user/1/custom-lookup.sock".into()),
            Some("/run/user/1".into()),
        );
        assert_eq!(
            path.as_deref(),
            Some(std::path::Path::new("/run/user/1/custom-lookup.sock"))
        );
        let nested = lookup_socket_path(None, Some("/run/user/1".into()));
        assert_eq!(
            nested.as_deref(),
            Some(std::path::Path::new("/run/user/1/nostrherd/lookup.sock"))
        );
        assert_eq!(lookup_socket_path(None, None), None);
        assert_eq!(
            lookup_socket_path(None, Some(std::ffi::OsString::new())),
            None
        );
    }
}
