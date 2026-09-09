//! Reconnecting Kelpie socket inbox for the pane-less host waiter.

use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::KelpieError;

/// Default reconnect pause after a dropped inbox connection.
const RECONNECT_WAIT: Duration = Duration::from_secs(1);
const RECONNECT_NOTICE_INTERVAL: Duration = Duration::from_secs(30);

/// One queued delivery offered on a claimed inbox connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxDelivery {
    message_id: String,
    kind: String,
    disposition: Option<String>,
    reply_to: Option<String>,
    sender_agent_id: Option<String>,
    sender_public_name: Option<String>,
    body: String,
}

impl InboxDelivery {
    /// Return the Kelpie message id to acknowledge.
    #[must_use]
    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    /// Return the stored message kind (`reply`, `cancellation`, …).
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Return the reply disposition when this delivery is a reply.
    #[must_use]
    pub fn disposition(&self) -> Option<&str> {
        self.disposition.as_deref()
    }

    /// Return the ask id a reply or cancellation refers to.
    #[must_use]
    pub fn reply_to(&self) -> Option<&str> {
        self.reply_to.as_deref()
    }

    /// Return the occupant reply body.
    #[must_use]
    pub fn body(&self) -> &str {
        &self.body
    }

    /// Return the sender logical id when the delivery names one.
    #[must_use]
    pub fn sender_agent_id(&self) -> Option<&str> {
        self.sender_agent_id.as_deref()
    }

    /// Return the sender public name when the delivery names one.
    #[must_use]
    pub fn sender_public_name(&self) -> Option<&str> {
        self.sender_public_name.as_deref()
    }
}

/// Reconnecting inbox that forwards deliveries until the host ACKs.
#[derive(Debug)]
pub struct HostInbox {
    rx: tokio::sync::mpsc::UnboundedReceiver<InboxDelivery>,
    ack_tx: Sender<String>,
}

impl HostInbox {
    /// Receive the next complete delivery, without acknowledging it.
    pub async fn recv(&mut self) -> Option<InboxDelivery> {
        self.rx.recv().await
    }

    /// Acknowledge one delivery on the claimed connection.
    pub fn ack(&self, message_id: &str) {
        let _ = self.ack_tx.send(message_id.to_owned());
    }
}

/// Default Kelpie daemon socket path.
#[must_use]
pub fn default_socket() -> PathBuf {
    socket_path(
        std::env::var_os("KELPIE_SOCKET"),
        std::env::var_os("XDG_RUNTIME_DIR"),
        &std::env::temp_dir(),
    )
}

fn socket_path(
    explicit: Option<std::ffi::OsString>,
    runtime: Option<std::ffi::OsString>,
    temp: &Path,
) -> PathBuf {
    if let Some(path) = explicit.filter(|value| !value.is_empty()) {
        return PathBuf::from(path);
    }
    // Match Kelpie's paths::runtime_root_with, including an empty XDG value.
    let runtime = runtime
        .filter(|value| !value.is_empty())
        .map_or_else(|| temp.join("kelpie-client"), PathBuf::from);
    runtime.join("kelpie/kelpie.sock")
}

/// One claimed inbox connection. `inbox.ack` is valid only here.
#[derive(Debug)]
pub struct InboxConn {
    stream: UnixStream,
    reader: BufReader<UnixStream>,
    next_id: u64,
    pending: VecDeque<Value>,
    partial: String,
}

impl InboxConn {
    /// Claim the reconnectable inbox for one socket waiter.
    ///
    /// # Errors
    ///
    /// Returns an error when the socket cannot be opened or Kelpie refuses the
    /// claim.
    pub fn claim(socket: &Path, waiter_id: &str) -> Result<Self, KelpieError> {
        let mut stream = UnixStream::connect(socket).map_err(KelpieError::from)?;
        let request = serde_json::json!({
            "id": "claim",
            "method": "inbox.claim",
            "params": {"logical_agent_id": waiter_id},
        });
        write_json(&mut stream, &request)?;
        let reader = BufReader::new(stream.try_clone().map_err(KelpieError::from)?);
        let mut conn = Self {
            stream,
            reader,
            next_id: 1,
            pending: VecDeque::new(),
            partial: String::new(),
        };
        let claimed = conn.read_json()?;
        if claimed.pointer("/result/claimed").and_then(Value::as_bool) != Some(true) {
            return Err(claim_error(&claimed));
        }
        Ok(conn)
    }

    /// Read the next complete JSON line, discarding a torn line on EOF.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection ends or a complete line is not JSON.
    pub fn read_event(&mut self) -> Result<Value, KelpieError> {
        self.read_json()
    }

    /// Acknowledge one offered delivery on this claimed connection.
    ///
    /// Persist is not acceptance. The obligation closes only after this ACK.
    ///
    /// # Errors
    ///
    /// Returns an error when Kelpie rejects the ACK or the connection drops.
    pub fn ack(&mut self, message_id: &str) -> Result<(), KelpieError> {
        self.stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(KelpieError::from)?;
        let result = self.ack_inner(message_id);
        let _ = self
            .stream
            .set_read_timeout(Some(Duration::from_millis(100)));
        result
    }

    fn ack_inner(&mut self, message_id: &str) -> Result<(), KelpieError> {
        let id = format!("ack-{}", self.next_id);
        self.next_id += 1;
        let request = serde_json::json!({
            "id": id,
            "method": "inbox.ack",
            "params": {"message_id": message_id},
        });
        write_json(&mut self.stream, &request)?;
        loop {
            let event = self.read_json()?;
            if event.get("method").and_then(Value::as_str) == Some("inbox.delivery") {
                self.pending.push_back(event);
                continue;
            }
            if event.get("id").and_then(Value::as_str) != Some(id.as_str()) {
                continue;
            }
            if event.get("error").is_some_and(|error| !error.is_null()) {
                return Err(KelpieError::InvalidReceipt(format!(
                    "inbox.ack failed: {event}"
                )));
            }
            if event.pointer("/result/outcome").and_then(Value::as_str) != Some("accepted") {
                return Err(KelpieError::InvalidReceipt(
                    "inbox.ack did not accept the delivery".to_owned(),
                ));
            }
            return Ok(());
        }
    }
}

/// Parse one `inbox.delivery` event.
///
/// # Errors
///
/// Returns an error when the event is not a delivery or lacks `message_id`.
pub fn parse_delivery(event: &Value) -> Result<InboxDelivery, KelpieError> {
    if event.get("method").and_then(Value::as_str) != Some("inbox.delivery") {
        return Err(KelpieError::InvalidReceipt(
            "inbox event is not a delivery".to_owned(),
        ));
    }
    let params = event
        .get("params")
        .ok_or_else(|| KelpieError::InvalidReceipt("inbox.delivery missing params".to_owned()))?;
    let message_id = crate::json_text(params.get("message_id")).ok_or_else(|| {
        KelpieError::InvalidReceipt("inbox.delivery missing message_id".to_owned())
    })?;
    Ok(InboxDelivery {
        kind: params
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("reply")
            .to_owned(),
        disposition: params
            .get("disposition")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        reply_to: crate::json_text(params.get("reply_to")),
        sender_agent_id: optional_text(params, "sender_agent_id"),
        sender_public_name: optional_text(params, "sender_public_name"),
        body: params
            .get("body")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        message_id,
    })
}

fn optional_text(params: &Value, key: &str) -> Option<String> {
    crate::json_text(params.get(key))
}

/// Spawn a reconnecting inbox that keeps the claim until the host ACKs.
#[must_use]
pub fn spawn_inbox(socket: PathBuf, waiter_id: String) -> HostInbox {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let (ack_tx, ack_rx) = mpsc::channel();
    thread::spawn(move || {
        let mut last_notice: Option<Instant> = None;
        loop {
            let Err(error) = drain_inbox(&socket, &waiter_id, &tx, &ack_rx) else {
                return;
            };
            if tx.is_closed() {
                return;
            }
            if last_notice.is_none_or(|last| last.elapsed() >= RECONNECT_NOTICE_INTERVAL) {
                eprintln!(
                    "nostrherd: inbox failed at {}: {}; reconnecting every second (notices limited to every 30 seconds)",
                    socket.to_string_lossy().escape_debug(),
                    inbox_error_summary(&error)
                );
                last_notice = Some(Instant::now());
            }
            thread::sleep(RECONNECT_WAIT);
        }
    });
    HostInbox { rx, ack_tx }
}

fn inbox_error_summary(error: &KelpieError) -> String {
    // Receipt text can contain message bodies. Never include it in diagnostics.
    match error {
        KelpieError::Io(error) => format!("I/O {:?}", error.kind()),
        _ => "Kelpie protocol or receipt failure".to_owned(),
    }
}

fn drain_inbox(
    socket: &Path,
    waiter_id: &str,
    tx: &tokio::sync::mpsc::UnboundedSender<InboxDelivery>,
    ack_rx: &Receiver<String>,
) -> Result<(), KelpieError> {
    let mut conn = InboxConn::claim(socket, waiter_id)?;
    conn.stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .map_err(KelpieError::from)?;
    loop {
        while let Ok(message_id) = ack_rx.try_recv() {
            conn.ack(&message_id)?;
        }
        let event = match conn.read_event() {
            Ok(event) => event,
            Err(KelpieError::Io(error))
                if error.kind() == io::ErrorKind::TimedOut
                    || error.kind() == io::ErrorKind::WouldBlock =>
            {
                continue;
            }
            Err(error) => return Err(error),
        };
        if event.get("method").and_then(Value::as_str) != Some("inbox.delivery") {
            continue;
        }
        let delivery = parse_delivery(&event)?;
        if tx.send(delivery).is_err() {
            return Ok(());
        }
    }
}

fn write_json(stream: &mut UnixStream, value: &Value) -> Result<(), KelpieError> {
    serde_json::to_writer(&mut *stream, value).map_err(|error| json_error(&error))?;
    stream.write_all(b"\n").map_err(KelpieError::from)?;
    stream.flush().map_err(KelpieError::from)
}

fn json_error(error: &serde_json::Error) -> KelpieError {
    KelpieError::InvalidReceipt(error.to_string())
}

impl InboxConn {
    fn read_json(&mut self) -> Result<Value, KelpieError> {
        if let Some(pending) = self.pending.pop_front() {
            return Ok(pending);
        }
        match self.reader.read_line(&mut self.partial) {
            Ok(0) => Err(KelpieError::from(io::Error::from(
                io::ErrorKind::UnexpectedEof,
            ))),
            Ok(_) if self.partial.ends_with('\n') => {
                let line = std::mem::take(&mut self.partial);
                serde_json::from_str(line.trim_end()).map_err(|error| json_error(&error))
            }
            Ok(_) => Err(KelpieError::from(io::Error::from(
                io::ErrorKind::UnexpectedEof,
            ))),
            Err(error)
                if error.kind() == io::ErrorKind::TimedOut
                    || error.kind() == io::ErrorKind::WouldBlock =>
            {
                Err(KelpieError::from(error))
            }
            Err(error) => Err(KelpieError::from(error)),
        }
    }
}

fn claim_error(claimed: &Value) -> KelpieError {
    if let Some(message) = claimed.pointer("/error/message").and_then(Value::as_str) {
        return KelpieError::InvalidReceipt(message.to_owned());
    }
    KelpieError::InvalidReceipt("inbox.claim did not succeed".to_owned())
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{parse_delivery, InboxConn};

    #[test]
    fn delivery_ids_parse_as_numbers_and_as_strings() {
        // Kelpie ids were UUID strings and are now integers, sent as JSON
        // numbers. Reading them with `as_str` silently yielded None, which
        // failed every delivery as a missing message_id and took the host's
        // inbox down while it looked merely disconnected.
        let numeric = serde_json::json!({
            "method": "inbox.delivery",
            "params": {
                "message_id": 20639,
                "reply_to": 1847,
                "sender_agent_id": 1572,
                "body": "hello",
            }
        });
        let parsed = parse_delivery(&numeric).expect("numeric ids");
        assert_eq!(parsed.message_id(), "20639");
        assert_eq!(parsed.reply_to(), Some("1847"));
        assert_eq!(parsed.sender_agent_id(), Some("1572"));

        let textual = serde_json::json!({
            "method": "inbox.delivery",
            "params": {
                "message_id": "01a08854-de56-7830-8045-abcd8a92e865",
                "reply_to": "01a0869e-eb6a-7fb2-873d-978f9598193a",
                "body": "hello",
            }
        });
        let parsed = parse_delivery(&textual).expect("string ids");
        assert_eq!(parsed.message_id(), "01a08854-de56-7830-8045-abcd8a92e865");
        assert_eq!(
            parsed.reply_to(),
            Some("01a0869e-eb6a-7fb2-873d-978f9598193a")
        );
    }

    #[test]
    fn socket_resolution_matches_kelpie_fallbacks() {
        let temp = std::path::Path::new("/custom-temp");
        assert_eq!(
            super::socket_path(Some("".into()), None, temp),
            temp.join("kelpie-client/kelpie/kelpie.sock")
        );
        for runtime in [None, Some("".into())] {
            assert_eq!(
                super::socket_path(None, runtime, temp),
                temp.join("kelpie-client/kelpie/kelpie.sock")
            );
        }
        assert_eq!(
            super::socket_path(None, Some("/run/user/42".into()), temp),
            PathBuf::from("/run/user/42/kelpie/kelpie.sock")
        );
        for runtime in [None, Some("/run/user/42".into())] {
            assert_eq!(
                super::socket_path(Some("/override.sock".into()), runtime, temp),
                PathBuf::from("/override.sock")
            );
        }
    }

    #[test]
    fn inbox_diagnostics_do_not_include_receipt_contents() {
        let error = crate::KelpieError::InvalidReceipt("private reply body".to_owned());
        assert_eq!(
            super::inbox_error_summary(&error),
            "Kelpie protocol or receipt failure"
        );
        let error = crate::KelpieError::from(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "private error detail",
        ));
        assert_eq!(super::inbox_error_summary(&error), "I/O PermissionDenied");
    }

    fn temp_socket() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("nostrherd-inbox-{nanos}.sock"))
    }

    fn write_line(stream: &mut std::os::unix::net::UnixStream, value: &serde_json::Value) {
        serde_json::to_writer(&mut *stream, value).expect("write");
        stream.write_all(b"\n").expect("nl");
        stream.flush().expect("flush");
    }

    fn read_line(reader: &mut BufReader<std::os::unix::net::UnixStream>) -> serde_json::Value {
        let mut line = String::new();
        reader.read_line(&mut line).expect("read");
        serde_json::from_str(&line).expect("json")
    }

    #[test]
    fn claim_then_ack_uses_the_same_connection() {
        let socket = temp_socket();
        let listener = UnixListener::bind(&socket).expect("bind");
        let (ready, started) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            let claim = read_line(&mut reader);
            assert_eq!(claim["method"], "inbox.claim");
            assert_eq!(claim["params"]["logical_agent_id"], "waiter-agent");
            write_line(
                &mut stream,
                &serde_json::json!({
                    "id": claim["id"],
                    "result": {
                        "logical_agent_id": "waiter-agent",
                        "claimed": true,
                        "delivery_transport": "socket_inbox"
                    }
                }),
            );
            ready.send(()).expect("ready");
            write_line(
                &mut stream,
                &serde_json::json!({
                    "id": "delivery-1",
                    "method": "inbox.delivery",
                                    "params": {
                                        "message_id": "reply-1",
                                        "kind": "reply",
                                        "disposition": "final",
                                        "reply_to": "ask-1",
                                        "body": "hello from occupant"
                                    }
                }),
            );
            let ack = read_line(&mut reader);
            assert_eq!(ack["method"], "inbox.ack");
            assert_eq!(ack["params"]["message_id"], "reply-1");
            write_line(
                &mut stream,
                &serde_json::json!({
                    "id": ack["id"],
                    "result": {"message_id": "reply-1", "outcome": "accepted"}
                }),
            );
        });

        let mut conn = InboxConn::claim(&socket, "waiter-agent").expect("claim");
        started.recv().expect("offered");
        let event = conn.read_event().expect("delivery");
        let delivery = parse_delivery(&event).expect("parse");
        assert_eq!(delivery.message_id(), "reply-1");
        assert_eq!(delivery.kind(), "reply");
        assert_eq!(delivery.disposition(), Some("final"));
        assert_eq!(delivery.body(), "hello from occupant");
        conn.ack(delivery.message_id()).expect("ack");
        server.join().expect("server");
        let _ = std::fs::remove_file(socket);
    }

    #[test]
    fn torn_delivery_line_is_not_json() {
        let socket = temp_socket();
        let listener = UnixListener::bind(&socket).expect("bind");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            let claim = read_line(&mut reader);
            write_line(
                &mut stream,
                &serde_json::json!({
                    "id": claim["id"],
                    "result": {"claimed": true, "logical_agent_id": "waiter-agent"}
                }),
            );
            stream
                .write_all(b"{\"method\":\"inbox.delivery\",\"params\":{\"message_id\":\"")
                .expect("partial");
        });

        let mut conn = InboxConn::claim(&socket, "waiter-agent").expect("claim");
        conn.read_event().expect_err("torn line");
        server.join().expect("server");
        let _ = std::fs::remove_file(socket);
    }

    #[test]
    fn drain_forwards_the_body_before_ack() {
        let socket = temp_socket();
        let listener = UnixListener::bind(&socket).expect("bind");
        let (offered_tx, offered_rx) = mpsc::channel();
        let (hold_tx, hold_rx) = mpsc::channel();
        let (acked_tx, acked_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            let claim = read_line(&mut reader);
            assert_eq!(claim["method"], "inbox.claim");
            write_line(
                &mut stream,
                &serde_json::json!({
                    "id": claim["id"],
                    "result": {"claimed": true, "logical_agent_id": "waiter-agent"}
                }),
            );
            write_line(
                &mut stream,
                &serde_json::json!({
                    "id": "delivery-1",
                    "method": "inbox.delivery",
                    "params": {
                        "message_id": "reply-1",
                        "kind": "reply",
                        "disposition": "final",
                        "reply_to": "ask-1",
                        "body": "durable body"
                    }
                }),
            );
            offered_tx.send(()).expect("offered");
            stream
                .set_read_timeout(Some(std::time::Duration::from_millis(80)))
                .expect("timeout");
            let mut line = String::new();
            let early = reader.read_line(&mut line);
            assert!(
                early.is_err() || line.is_empty(),
                "acked before host held the body: {line}"
            );
            hold_rx.recv().expect("host holds body");
            stream.set_read_timeout(None).expect("clear timeout");
            let ack = read_line(&mut reader);
            assert_eq!(ack["method"], "inbox.ack");
            assert_eq!(ack["params"]["message_id"], "reply-1");
            write_line(
                &mut stream,
                &serde_json::json!({
                    "id": ack["id"],
                    "result": {"message_id": "reply-1", "outcome": "accepted"}
                }),
            );
            acked_tx.send(()).expect("acked");
        });

        let mut inbox = super::spawn_inbox(socket.clone(), "waiter-agent".to_owned());
        offered_rx.recv().expect("offered");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let delivery = runtime.block_on(inbox.recv()).expect("delivery");
        assert_eq!(delivery.body(), "durable body");
        assert_eq!(delivery.reply_to(), Some("ask-1"));
        thread::sleep(std::time::Duration::from_millis(120));
        hold_tx.send(()).expect("held");
        inbox.ack(delivery.message_id());
        acked_rx.recv().expect("server saw ack");
        server.join().expect("server");
        let _ = std::fs::remove_file(socket);
    }
}
