//! Reconnecting Kelpie socket inbox for the pane-less host waiter.

use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use serde_json::Value;

use crate::KelpieError;

/// Default reconnect pause after a dropped inbox connection.
const RECONNECT_WAIT: Duration = Duration::from_secs(1);

/// One queued delivery offered on a claimed inbox connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxDelivery {
    message_id: String,
    kind: String,
    disposition: Option<String>,
    reply_to: Option<String>,
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
}

/// Default Kelpie daemon socket path.
#[must_use]
pub fn default_socket() -> PathBuf {
    if let Some(path) = std::env::var_os("KELPIE_SOCKET") {
        return PathBuf::from(path);
    }
    let runtime =
        std::env::var_os("XDG_RUNTIME_DIR").map_or_else(|| PathBuf::from("/tmp"), PathBuf::from);
    runtime.join("kelpie/kelpie.sock")
}

/// One claimed inbox connection. `inbox.ack` is valid only here.
#[derive(Debug)]
pub struct InboxConn {
    stream: UnixStream,
    reader: BufReader<UnixStream>,
    next_id: u64,
    pending: VecDeque<Value>,
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
    let message_id = params
        .get("message_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| KelpieError::InvalidReceipt("inbox.delivery missing message_id".to_owned()))?
        .to_owned();
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
        reply_to: params
            .get("reply_to")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        message_id,
    })
}

/// Spawn a reconnecting inbox that ACKs each complete delivery.
#[must_use]
pub fn spawn_inbox(
    socket: PathBuf,
    waiter_id: String,
) -> tokio::sync::mpsc::UnboundedReceiver<InboxDelivery> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    thread::spawn(move || loop {
        if drain_inbox(&socket, &waiter_id, &tx).is_ok() {
            return;
        }
        if tx.is_closed() {
            return;
        }
        thread::sleep(RECONNECT_WAIT);
    });
    rx
}

fn drain_inbox(
    socket: &Path,
    waiter_id: &str,
    tx: &tokio::sync::mpsc::UnboundedSender<InboxDelivery>,
) -> Result<(), KelpieError> {
    let mut conn = InboxConn::claim(socket, waiter_id)?;
    loop {
        let event = conn.read_event()?;
        if event.get("method").and_then(Value::as_str) != Some("inbox.delivery") {
            continue;
        }
        let delivery = parse_delivery(&event)?;
        conn.ack(delivery.message_id())?;
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
        let mut line = String::new();
        let read = self
            .reader
            .read_line(&mut line)
            .map_err(KelpieError::from)?;
        if read == 0 || !line.ends_with('\n') {
            return Err(KelpieError::from(io::Error::from(
                io::ErrorKind::UnexpectedEof,
            )));
        }
        serde_json::from_str(line.trim_end()).map_err(|error| json_error(&error))
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

    fn temp_socket() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("botserver-inbox-{nanos}.sock"))
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
                        "reply_to": "ask-1"
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
}
