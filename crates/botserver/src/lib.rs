//! Host adapters for relay traffic, Herdr, and Kelpie.

use std::fmt;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde_json::Value;

/// Public Herdr and Kelpie name of the host waiter.
pub const WAITER_NAME: &str = "botserver";

/// A host identity adopted into Kelpie.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaiterIdentity {
    logical_agent_id: String,
    incarnation_id: String,
}

impl WaiterIdentity {
    /// Return the durable Kelpie agent id.
    #[must_use]
    pub fn logical_agent_id(&self) -> &str {
        &self.logical_agent_id
    }

    /// Return the current Kelpie incarnation id.
    #[must_use]
    pub fn incarnation_id(&self) -> &str {
        &self.incarnation_id
    }
}

/// Final delivery state reported for a Kelpie ask attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AskDelivery {
    /// Herdr accepted the prompt delivery.
    Accepted,
    /// Kelpie cannot prove whether Herdr accepted the prompt.
    Unknown,
    /// Herdr rejected the prompt delivery.
    Rejected,
    /// No ready recipient was available.
    TargetUnavailable,
}

/// Receipt for one correlated Kelpie ask attempt.
#[must_use = "ask delivery must be inspected"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AskReceipt {
    message_id: String,
    operation_id: Option<String>,
    recipient: String,
    delivery: AskDelivery,
}

impl AskReceipt {
    /// Return the id used by both `msg=` and `reply-to=` in the envelope.
    #[must_use]
    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    /// Return the Kelpie delivery operation id.
    #[must_use]
    pub fn operation_id(&self) -> Option<&str> {
        self.operation_id.as_deref()
    }

    /// Return the logical recipient recorded by Kelpie.
    #[must_use]
    pub fn recipient(&self) -> &str {
        &self.recipient
    }

    /// Return the final delivery state observed by Kelpie.
    #[must_use]
    pub const fn delivery(&self) -> AskDelivery {
        self.delivery
    }
}

/// Failure while invoking or reading Kelpie.
#[derive(Debug)]
pub enum KelpieError {
    /// The Kelpie process could not be started or completed.
    Io(io::Error),
    /// Kelpie rejected the operation.
    Rejected { status: String, stderr: String },
    /// Kelpie returned a response outside its JSON receipt contract.
    InvalidReceipt(String),
}

impl fmt::Display for KelpieError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "failed to invoke Kelpie: {error}"),
            Self::Rejected { status, stderr } => {
                write!(formatter, "Kelpie exited with {status}: {stderr}")
            }
            Self::InvalidReceipt(reason) => write!(formatter, "invalid Kelpie receipt: {reason}"),
        }
    }
}

impl std::error::Error for KelpieError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Rejected { .. } | Self::InvalidReceipt(_) => None,
        }
    }
}

impl From<io::Error> for KelpieError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug)]
struct CommandOutput {
    success: bool,
    status: String,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

trait CommandRunner: fmt::Debug + Send + Sync {
    fn run(&self, arguments: &[String], stdin: &[u8]) -> io::Result<CommandOutput>;
}

#[derive(Debug)]
struct ProcessRunner {
    program: PathBuf,
}

impl CommandRunner for ProcessRunner {
    fn run(&self, arguments: &[String], stdin: &[u8]) -> io::Result<CommandOutput> {
        let mut child = Command::new(&self.program)
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let write_result = child
            .stdin
            .take()
            .expect("piped stdin is available")
            .write_all(stdin);
        let output = child.wait_with_output()?;
        if output.status.success() {
            write_result?;
        }

        Ok(CommandOutput {
            success: output.status.success(),
            status: output.status.to_string(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

/// Host-side client for waiter adoption and ask delivery.
#[derive(Debug)]
pub struct KelpieClient {
    runner: Box<dyn CommandRunner>,
}

impl Default for KelpieClient {
    fn default() -> Self {
        Self::new("kelpie")
    }
}

impl KelpieClient {
    /// Create a client backed by a Kelpie executable.
    #[must_use]
    pub fn new(program: impl AsRef<Path>) -> Self {
        Self {
            runner: Box::new(ProcessRunner {
                program: program.as_ref().to_owned(),
            }),
        }
    }

    /// Adopt an existing Herdr pane as the single host waiter.
    ///
    /// # Errors
    ///
    /// Returns an error when Kelpie cannot adopt the exact pane and terminal.
    pub fn adopt_waiter(
        &self,
        pane_id: &str,
        terminal_id: &str,
    ) -> Result<AdoptedWaiter<'_>, KelpieError> {
        self.adopt(pane_id, terminal_id, None)
    }

    /// Continue the durable waiter identity in a replacement Herdr pane.
    ///
    /// # Errors
    ///
    /// Returns an error when Kelpie cannot bind the exact pane and terminal to
    /// the existing logical agent.
    pub fn continue_waiter(
        &self,
        pane_id: &str,
        terminal_id: &str,
        logical_agent_id: &str,
    ) -> Result<AdoptedWaiter<'_>, KelpieError> {
        self.adopt(pane_id, terminal_id, Some(logical_agent_id))
    }

    fn adopt(
        &self,
        pane_id: &str,
        terminal_id: &str,
        logical_agent_id: Option<&str>,
    ) -> Result<AdoptedWaiter<'_>, KelpieError> {
        let mut arguments = vec![
            "--json",
            "adopt",
            "--pane",
            pane_id,
            "--terminal",
            terminal_id,
            "--name",
            WAITER_NAME,
        ];
        if let Some(logical_agent_id) = logical_agent_id {
            arguments.extend(["--logical-id", logical_agent_id]);
        }
        let output = self.invoke(&arguments, &[])?;
        if !output.success {
            return Err(output.rejected());
        }
        let result = result(&output.receipt)?;
        if field(result, "outcome")? != "succeeded" {
            return Err(KelpieError::InvalidReceipt(
                "waiter adoption did not succeed".to_owned(),
            ));
        }
        let identity = WaiterIdentity {
            logical_agent_id: field(result, "logical_agent_id")?,
            incarnation_id: field(result, "incarnation_id")?,
        };
        Ok(AdoptedWaiter {
            client: self,
            identity,
        })
    }

    fn invoke(&self, arguments: &[&str], stdin: &[u8]) -> Result<InvocationOutput, KelpieError> {
        let arguments = arguments
            .iter()
            .map(|argument| (*argument).to_owned())
            .collect::<Vec<_>>();
        let output = self.runner.run(&arguments, stdin)?;
        match serde_json::from_slice(&output.stdout) {
            Ok(receipt) => Ok(InvocationOutput {
                success: output.success,
                status: output.status,
                stderr: output.stderr,
                receipt,
            }),
            Err(error) if output.success => Err(KelpieError::InvalidReceipt(error.to_string())),
            Err(_) => Err(KelpieError::Rejected {
                status: output.status,
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            }),
        }
    }

    #[cfg(test)]
    fn with_runner(runner: impl CommandRunner + 'static) -> Self {
        Self {
            runner: Box::new(runner),
        }
    }
}

#[derive(Debug)]
struct InvocationOutput {
    success: bool,
    status: String,
    stderr: Vec<u8>,
    receipt: Value,
}

impl InvocationOutput {
    fn rejected(&self) -> KelpieError {
        let stderr = self
            .receipt
            .pointer("/error/message")
            .and_then(Value::as_str)
            .map_or_else(
                || String::from_utf8_lossy(&self.stderr).trim().to_owned(),
                ToOwned::to_owned,
            );
        KelpieError::Rejected {
            status: self.status.clone(),
            stderr,
        }
    }
}

#[derive(Debug)]
struct RecipientIdentity {
    logical_agent_id: String,
    incarnation_id: String,
}

/// Adopted waiter whose exact logical id owns every ask it sends.
#[derive(Debug)]
pub struct AdoptedWaiter<'a> {
    client: &'a KelpieClient,
    identity: WaiterIdentity,
}

impl AdoptedWaiter<'_> {
    /// Return the waiter identity retained from adoption.
    #[must_use]
    pub fn identity(&self) -> &WaiterIdentity {
        &self.identity
    }

    /// Send Nostr text as a Kelpie ask to one session occupant.
    ///
    /// Kelpie receives the body on stdin and owns envelope escaping. The exact
    /// adopted logical id is supplied as sender, so relay identities cannot
    /// become the envelope's `from=` value. `idempotency_key` must remain stable
    /// for the source Turn.
    ///
    /// # Errors
    ///
    /// Returns an error unless Kelpie returns the durable ids needed to
    /// reconcile the attempt. A receipt with [`AskDelivery::Unknown`] must not
    /// be retried blindly.
    pub fn ask(
        &self,
        recipient: &str,
        nostr_body: &str,
        idempotency_key: &str,
    ) -> Result<AskReceipt, KelpieError> {
        let recipient = self.resolve_recipient(recipient)?;
        let output = self.client.invoke(
            &[
                "--json",
                "ask",
                "--recipient-id",
                &recipient.logical_agent_id,
                "--recipient-incarnation",
                &recipient.incarnation_id,
                "--stdin",
                "--sender-id",
                self.identity.logical_agent_id(),
                "--idempotency-key",
                idempotency_key,
            ],
            nostr_body.as_bytes(),
        )?;
        if output.success {
            let result = result(&output.receipt)?;
            return Ok(AskReceipt {
                message_id: field(result, "message_id")?,
                operation_id: Some(field(result, "operation_id")?),
                recipient: field(result, "recipient")?,
                delivery: ask_delivery(result)?,
            });
        }

        let delivery = match error_class(&output.receipt) {
            Some("unknown_outcome") => AskDelivery::Unknown,
            Some("rejected") => AskDelivery::Rejected,
            Some("target_unavailable") => AskDelivery::TargetUnavailable,
            _ => return Err(output.rejected()),
        };
        Ok(AskReceipt {
            message_id: self.pending_ask_id(&recipient.logical_agent_id)?,
            operation_id: None,
            recipient: recipient.logical_agent_id,
            delivery,
        })
    }

    fn resolve_recipient(&self, alias: &str) -> Result<RecipientIdentity, KelpieError> {
        let output = self.client.invoke(&["--json", "whoami", alias], &[])?;
        if !output.success {
            return Err(output.rejected());
        }
        let result = result(&output.receipt)?;
        Ok(RecipientIdentity {
            logical_agent_id: field(result, "logical_agent_id")?,
            incarnation_id: field(result, "incarnation_id")?,
        })
    }

    fn pending_ask_id(&self, recipient_id: &str) -> Result<String, KelpieError> {
        let output = self
            .client
            .invoke(&["--json", "pending", "--sender-id", recipient_id], &[])?;
        if !output.success {
            return Err(output.rejected());
        }
        let obligations = output
            .receipt
            .get("result")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                KelpieError::InvalidReceipt("pending result is not an array".to_owned())
            })?;
        let ask_ids = obligations
            .iter()
            .filter(|obligation| {
                obligation.get("waiting_agent_id").and_then(Value::as_str)
                    == Some(self.identity.logical_agent_id())
            })
            .filter_map(|obligation| obligation.get("ask_message_id").and_then(Value::as_str))
            .collect::<Vec<_>>();
        match ask_ids.as_slice() {
            [ask_id] => Ok((*ask_id).to_owned()),
            [] => Err(KelpieError::InvalidReceipt(
                "uncertain ask did not create a pending obligation".to_owned(),
            )),
            _ => Err(KelpieError::InvalidReceipt(
                "multiple pending asks prevent uncertain-delivery reconciliation".to_owned(),
            )),
        }
    }
}

fn ask_delivery(result: &Value) -> Result<AskDelivery, KelpieError> {
    match field(result, "delivery_outcome")?.as_str() {
        "accepted" => Ok(AskDelivery::Accepted),
        "unknown" => Ok(AskDelivery::Unknown),
        "rejected" => Ok(AskDelivery::Rejected),
        "target_unavailable" => Ok(AskDelivery::TargetUnavailable),
        outcome => Err(KelpieError::InvalidReceipt(format!(
            "unsupported ask delivery outcome {outcome}"
        ))),
    }
}

fn error_class(receipt: &Value) -> Option<&str> {
    receipt.pointer("/error/class").and_then(Value::as_str)
}

fn result(receipt: &Value) -> Result<&Value, KelpieError> {
    if let Some(error) = receipt.get("error").filter(|error| !error.is_null()) {
        return Err(KelpieError::InvalidReceipt(format!(
            "Kelpie returned an error: {error}"
        )));
    }
    receipt
        .get("result")
        .ok_or_else(|| KelpieError::InvalidReceipt("missing result".to_owned()))
}

fn field(value: &Value, name: &str) -> Result<String, KelpieError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .filter(|field| !field.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| KelpieError::InvalidReceipt(format!("missing {name}")))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Debug)]
    struct FakeRunner {
        calls: Mutex<Vec<(Vec<String>, Vec<u8>)>>,
        outputs: Mutex<VecDeque<CommandOutput>>,
    }

    impl FakeRunner {
        fn new(outputs: impl IntoIterator<Item = CommandOutput>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                outputs: Mutex::new(outputs.into_iter().collect()),
            }
        }
    }

    impl CommandRunner for FakeRunner {
        fn run(&self, arguments: &[String], stdin: &[u8]) -> io::Result<CommandOutput> {
            self.calls
                .lock()
                .expect("calls lock")
                .push((arguments.to_vec(), stdin.to_vec()));
            self.outputs
                .lock()
                .expect("outputs lock")
                .pop_front()
                .ok_or_else(|| io::Error::other("missing fake output"))
        }
    }

    impl CommandRunner for Arc<FakeRunner> {
        fn run(&self, arguments: &[String], stdin: &[u8]) -> io::Result<CommandOutput> {
            self.as_ref().run(arguments, stdin)
        }
    }

    fn success(result: &Value) -> CommandOutput {
        CommandOutput {
            success: true,
            status: "exit status: 0".to_owned(),
            stdout: serde_json::to_vec(&serde_json::json!({
                "id": "request-id",
                "result": result
            }))
            .expect("json"),
            stderr: Vec::new(),
        }
    }

    fn recipient() -> CommandOutput {
        success(&serde_json::json!({
            "logical_agent_id": "occupant-agent",
            "incarnation_id": "occupant-incarnation",
            "public_name": "bot-foobar"
        }))
    }

    fn failure(class: &str, message: &str) -> CommandOutput {
        CommandOutput {
            success: false,
            status: "exit status: 1".to_owned(),
            stdout: serde_json::to_vec(&serde_json::json!({
                "id": "request-id",
                "error": {"class": class, "message": message}
            }))
            .expect("json"),
            stderr: b"kelpie: request failed".to_vec(),
        }
    }

    #[test]
    fn adopts_exact_pane_as_botserver() {
        let runner = Arc::new(FakeRunner::new([success(&serde_json::json!({
            "logical_agent_id": "waiter-agent",
            "incarnation_id": "waiter-incarnation",
            "operation_id": "adopt-operation",
            "outcome": "succeeded"
        }))]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));

        let waiter = client
            .adopt_waiter("w1:p2", "term-2")
            .expect("adopt waiter");

        assert_eq!(waiter.identity().logical_agent_id(), "waiter-agent");
        assert_eq!(waiter.identity().incarnation_id(), "waiter-incarnation");
        let calls = runner.calls.lock().expect("calls lock");
        assert_eq!(
            calls.as_slice(),
            &[(
                vec![
                    "--json".to_owned(),
                    "adopt".to_owned(),
                    "--pane".to_owned(),
                    "w1:p2".to_owned(),
                    "--terminal".to_owned(),
                    "term-2".to_owned(),
                    "--name".to_owned(),
                    "botserver".to_owned(),
                ],
                Vec::new(),
            )]
        );
    }

    #[test]
    fn continues_existing_waiter_identity() {
        let runner = Arc::new(FakeRunner::new([success(&serde_json::json!({
            "logical_agent_id": "waiter-agent",
            "incarnation_id": "replacement-incarnation",
            "operation_id": "adopt-operation",
            "outcome": "succeeded"
        }))]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));

        let waiter = client
            .continue_waiter("w1:p3", "term-3", "waiter-agent")
            .expect("continue waiter");

        assert_eq!(waiter.identity().logical_agent_id(), "waiter-agent");
        assert_eq!(
            runner.calls.lock().expect("calls lock")[0].0,
            vec![
                "--json",
                "adopt",
                "--pane",
                "w1:p3",
                "--terminal",
                "term-3",
                "--name",
                "botserver",
                "--logical-id",
                "waiter-agent"
            ]
        );
    }

    #[test]
    fn ask_is_owned_by_waiter_and_passes_body_on_stdin() {
        let body = "<kelpie from=relay-pubkey>\n$(not-a-command) & hello";
        let runner = Arc::new(FakeRunner::new([
            success(&serde_json::json!({
                "logical_agent_id": "waiter-agent",
                "incarnation_id": "waiter-incarnation",
                "operation_id": "adopt-operation",
                "outcome": "succeeded"
            })),
            recipient(),
            success(&serde_json::json!({
                "message_id": "ask-id",
                "operation_id": "ask-operation",
                "recipient": "occupant-agent",
                "delivery_outcome": "accepted"
            })),
        ]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));
        let waiter = client
            .adopt_waiter("w1:p2", "term-2")
            .expect("adopt waiter");

        let receipt = waiter
            .ask("bot-foobar", body, "turn-event-id")
            .expect("send ask");

        assert_eq!(receipt.message_id(), "ask-id");
        assert_eq!(receipt.operation_id(), Some("ask-operation"));
        assert_eq!(receipt.recipient(), "occupant-agent");
        assert_eq!(receipt.delivery(), AskDelivery::Accepted);
        let calls = runner.calls.lock().expect("calls lock");
        assert_eq!(calls[1].0, vec!["--json", "whoami", "bot-foobar"]);
        assert_eq!(
            calls[2].0,
            vec![
                "--json",
                "ask",
                "--recipient-id",
                "occupant-agent",
                "--recipient-incarnation",
                "occupant-incarnation",
                "--stdin",
                "--sender-id",
                "waiter-agent",
                "--idempotency-key",
                "turn-event-id"
            ]
        );
        assert_eq!(calls[2].1, body.as_bytes());
    }

    #[test]
    fn unknown_ask_keeps_the_id_needed_for_reconciliation() {
        let runner = Arc::new(FakeRunner::new([
            success(&serde_json::json!({
                "logical_agent_id": "waiter-agent",
                "incarnation_id": "waiter-incarnation",
                "operation_id": "adopt-operation",
                "outcome": "succeeded"
            })),
            recipient(),
            failure("unknown_outcome", "operation outcome is unknown"),
            success(&serde_json::json!([{
                "ask_message_id": "ask-id",
                "waiting_agent_id": "waiter-agent",
                "state": "open"
            }])),
        ]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));
        let waiter = client
            .adopt_waiter("w1:p2", "term-2")
            .expect("adopt waiter");

        let receipt = waiter
            .ask("bot-foobar", "hello", "turn-event-id")
            .expect("ask receipt");

        assert_eq!(receipt.message_id(), "ask-id");
        assert_eq!(receipt.operation_id(), None);
        assert_eq!(receipt.delivery(), AskDelivery::Unknown);
        let calls = runner.calls.lock().expect("calls lock");
        assert_eq!(
            calls[3].0,
            vec!["--json", "pending", "--sender-id", "occupant-agent"]
        );
    }

    #[test]
    fn parsed_kelpie_error_message_is_reported() {
        let runner = Arc::new(FakeRunner::new([failure(
            "conflict",
            "continue logical agent waiter-agent",
        )]));
        let client = KelpieClient::with_runner(runner);

        let error = client
            .adopt_waiter("w1:p2", "term-2")
            .expect_err("adoption conflict");

        assert!(error
            .to_string()
            .contains("continue logical agent waiter-agent"));
    }
}
pub mod relay;
pub mod sqlite;

use botserver_domain::{BotId, EventId};

/// Immutable relay event cached for channel snapshots.
///
/// The relay remains the canonical message store; this record is a rebuildable
/// local index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedRelayEvent {
    pub event_id: EventId,
    pub author_pubkey: String,
    pub created_at: i64,
    pub kind: u16,
    pub content: String,
    pub tags_json: String,
    pub channel_id: Option<String>,
    pub target_event_id: Option<EventId>,
}

/// Persisted binding between one bot and one Buzz channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord {
    pub bot_id: BotId,
    pub channel_id: String,
    pub session_name: String,
    pub occupant_logical_id: Option<String>,
    pub renew_id: Option<String>,
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
    const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Open => "open",
            Self::Posted => "posted",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "open" => Some(Self::Open),
            "posted" => Some(Self::Posted),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// Coordinates needed to persist a queued turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTurn {
    pub bot_id: BotId,
    pub channel_id: String,
    pub event_id: EventId,
    pub reply_to_event_id: Option<EventId>,
}

/// One persisted turn in session order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnRecord {
    pub sequence: i64,
    pub bot_id: BotId,
    pub channel_id: String,
    pub event_id: EventId,
    pub ask_id: Option<String>,
    pub reply_to_event_id: Option<EventId>,
    pub state: TurnState,
}

/// Persistence used by the host ingest and turn-processing paths.
pub trait HostRepository {
    type Error;

    /// Mark an event processed, returning false when it was already recorded.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the operation cannot be persisted.
    fn mark_event_processed(&mut self, event_id: &EventId) -> Result<bool, Self::Error>;

    /// Index a relay event without consuming its processing marker.
    ///
    /// Returns false when the event id was already indexed.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the event cannot be persisted.
    fn index_event(&mut self, event: &IndexedRelayEvent) -> Result<bool, Self::Error>;

    /// Return whether downstream turn handling completed for an event.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the processing marker cannot be read.
    fn event_processed(&self, event_id: &EventId) -> Result<bool, Self::Error>;

    /// Find one indexed relay event by id.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the event cannot be read.
    fn indexed_event(&self, event_id: &EventId) -> Result<Option<IndexedRelayEvent>, Self::Error>;

    /// Return the newest indexed relay timestamp for an inclusive replay cursor.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the index cannot be read.
    fn latest_indexed_at(&self) -> Result<Option<i64>, Self::Error>;

    /// Read indexed relay events for exactly one channel in chronological order.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when indexed events cannot be read.
    fn indexed_events_for_channel(
        &self,
        channel_id: &str,
    ) -> Result<Vec<IndexedRelayEvent>, Self::Error>;

    /// Atomically mark a trigger event processed and enqueue its turn.
    ///
    /// Returns `None` when the event was already processed.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when either operation cannot be persisted.
    fn enqueue_unprocessed_turn(
        &mut self,
        turn: &NewTurn,
    ) -> Result<Option<TurnRecord>, Self::Error>;

    /// Enqueue another turn for an event already known to the host.
    ///
    /// This supports replacement work after an edit cancels an earlier ask.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the turn cannot be persisted.
    fn enqueue_turn(&mut self, turn: &NewTurn) -> Result<TurnRecord, Self::Error>;

    /// Store or replace a session binding.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the binding cannot be persisted.
    fn save_session(&mut self, session: &SessionRecord) -> Result<(), Self::Error>;

    /// Find a session by bot and channel.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the session cannot be read.
    fn session(
        &self,
        bot_id: &BotId,
        channel_id: &str,
    ) -> Result<Option<SessionRecord>, Self::Error>;

    /// Bind the oldest queued turn to a new ask.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the turn cannot be persisted.
    fn open_next_turn(
        &mut self,
        bot_id: &BotId,
        channel_id: &str,
        ask_id: &str,
    ) -> Result<Option<TurnRecord>, Self::Error>;

    /// Change the state of the turn identified by its ask id.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the state cannot be persisted.
    fn set_turn_state(&mut self, ask_id: &str, state: TurnState) -> Result<bool, Self::Error>;

    /// Cancel queued work for an edited or deleted triggering event.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the state cannot be persisted.
    fn cancel_queued_turn(&mut self, event_id: &EventId) -> Result<bool, Self::Error>;

    /// Find a turn by the Kelpie ask id.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the turn cannot be read.
    fn turn_by_ask_id(&self, ask_id: &str) -> Result<Option<TurnRecord>, Self::Error>;

    /// Find queued or open work for a triggering event.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the turn cannot be read.
    fn active_turn_for_event(&self, event_id: &EventId) -> Result<Option<TurnRecord>, Self::Error>;

    /// Read a session's turns in insertion order.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the turns cannot be read.
    fn turns_for_session(
        &self,
        bot_id: &BotId,
        channel_id: &str,
    ) -> Result<Vec<TurnRecord>, Self::Error>;

    /// List sessions that have queued or open work for restart recovery.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the sessions cannot be read.
    fn sessions_with_pending_turns(&self) -> Result<Vec<SessionRecord>, Self::Error>;
}
