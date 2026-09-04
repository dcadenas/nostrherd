//! Host adapters for relay traffic, Herdr, and Kelpie.

use std::fmt;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde_json::Value;

#[cfg(test)]
mod test_support;

/// Public Kelpie name of the host socket waiter (D2).
pub const WAITER_NAME: &str = "botserver";

/// Stable `waiter.register` idempotency key so process restarts reuse the waiter.
pub const WAITER_IDEMPOTENCY_KEY: &str = "botserver-host-waiter";

/// A newly started session occupant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedOccupant {
    logical_agent_id: String,
    incarnation_id: String,
}

impl StartedOccupant {
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

/// Launch coordinates for a corpus occupant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OccupantLaunch {
    pub name: String,
    pub pane_id: String,
    pub terminal_id: String,
    pub backend: String,
    pub cwd: PathBuf,
    pub timeout_ms: u64,
    pub logical_agent_id: Option<String>,
}

/// Short trusted body used only to finish `kelpie start --tell`.
pub const OCCUPANT_BOOTSTRAP: &str = "Wait for Kelpie asks from botserver.";

/// Wall-clock renew interval for a session occupant (D27).
pub const OCCUPANT_RENEW_EVERY: &str = "45m";

/// Prepare prompt stored on the occupant renew policy (D27).
pub const OCCUPANT_RENEW_PREPARE: &str = "Write progress.md so a later instance of you can resume this channel work with no memory of this conversation: what is done, what is next, decisions and why, absolute paths.";

/// Return the current Unix timestamp in seconds.
///
/// # Errors
///
/// Returns an error when the system clock predates the Unix epoch or the
/// timestamp does not fit in an `i64`.
pub fn unix_now() -> io::Result<i64> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(io::Error::other)?;
    i64::try_from(elapsed.as_secs()).map_err(|_| io::Error::other("unix time does not fit i64"))
}

/// Bootstrap tell body that points at the channel snapshot.
#[must_use]
pub fn occupant_bootstrap(snapshot_relpath: &str) -> String {
    format!("{OCCUPANT_BOOTSTRAP}\nChannel snapshot: {snapshot_relpath}")
}

/// Resume prompt stored on the occupant renew policy.
#[must_use]
pub fn occupant_renew_resume(snapshot_relpath: &str) -> String {
    format!(
        "Read startup.md, then the channel snapshot at {snapshot_relpath}. Continue from progress.md if it exists."
    )
}

/// A pane-less host identity registered with Kelpie.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaiterIdentity {
    logical_agent_id: String,
}

impl WaiterIdentity {
    /// Return the durable Kelpie agent id.
    #[must_use]
    pub fn logical_agent_id(&self) -> &str {
        &self.logical_agent_id
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
    /// No Ready occupant was bound to the requested alias.
    TargetUnavailable,
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
            Self::TargetUnavailable => {
                formatter.write_str("no ready occupant is bound to that name")
            }
            Self::InvalidReceipt(reason) => write!(formatter, "invalid Kelpie receipt: {reason}"),
        }
    }
}

impl std::error::Error for KelpieError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Rejected { .. } | Self::TargetUnavailable | Self::InvalidReceipt(_) => None,
        }
    }
}

impl From<io::Error> for KelpieError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug)]
pub(crate) struct CommandOutput {
    pub(crate) success: bool,
    pub(crate) status: String,
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
}

pub(crate) trait CommandRunner: fmt::Debug + Send + Sync {
    fn run(&self, arguments: &[String], stdin: &[u8]) -> io::Result<CommandOutput>;
}

#[derive(Debug)]
pub(crate) struct ProcessRunner {
    program: PathBuf,
}

impl ProcessRunner {
    pub(crate) fn new(program: impl AsRef<Path>) -> Self {
        Self {
            program: program.as_ref().to_owned(),
        }
    }
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

/// Host-side client for waiter registration and ask delivery.
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
            runner: Box::new(ProcessRunner::new(program)),
        }
    }

    /// Register or reuse the pane-less host waiter named [`WAITER_NAME`].
    ///
    /// # Errors
    ///
    /// Returns an error when Kelpie cannot register the socket waiter.
    pub fn register_waiter(&self) -> Result<HostWaiter<'_>, KelpieError> {
        self.register_waiter_with_key(WAITER_IDEMPOTENCY_KEY)
    }

    /// Register or reuse the pane-less host waiter with an explicit idempotency key.
    ///
    /// Use a fresh key after `waiter.retire` of the previous host waiter. Kelpie
    /// refuses replay of an ended key.
    ///
    /// # Errors
    ///
    /// Returns an error when Kelpie cannot register the socket waiter.
    pub fn register_waiter_with_key<'a>(
        &'a self,
        idempotency_key: &str,
    ) -> Result<HostWaiter<'a>, KelpieError> {
        let output = self.invoke(
            &[
                "--json",
                "waiter-register",
                "--name",
                WAITER_NAME,
                "--parentless",
                "--idempotency-key",
                idempotency_key,
            ],
            &[],
        )?;
        if !output.success {
            return Err(output.rejected());
        }
        let result = result(&output.receipt)?;
        if field(result, "delivery_transport")? != "socket_inbox" {
            return Err(KelpieError::InvalidReceipt(
                "waiter is not a socket inbox".to_owned(),
            ));
        }
        Ok(HostWaiter {
            client: self,
            identity: WaiterIdentity {
                logical_agent_id: field(result, "logical_agent_id")?,
            },
        })
    }

    /// Continue a recorded occupant in a live pane without minting a twin.
    ///
    /// # Errors
    ///
    /// Returns an error when Kelpie cannot bind the pane to that logical agent.
    pub fn adopt_occupant(
        &self,
        pane_id: &str,
        terminal_id: &str,
        name: &str,
        logical_agent_id: &str,
    ) -> Result<StartedOccupant, KelpieError> {
        let occupant = self.bind_agent(pane_id, terminal_id, name, Some(logical_agent_id))?;
        if occupant.logical_agent_id() != logical_agent_id {
            return Err(KelpieError::InvalidReceipt(
                "session occupant is not the recorded logical agent".to_owned(),
            ));
        }
        Ok(occupant)
    }

    /// Return the Ready occupant currently bound to a public name.
    ///
    /// # Errors
    ///
    /// Returns an error when Kelpie cannot resolve the alias.
    pub fn occupant_whoami(&self, alias: &str) -> Result<StartedOccupant, KelpieError> {
        let output = self.invoke(&["--json", "whoami", alias], &[])?;
        if !output.success {
            if occupant_alias_unbound(&output.receipt) {
                return Err(KelpieError::TargetUnavailable);
            }
            return Err(output.rejected());
        }
        let result = result(&output.receipt)?;
        Ok(StartedOccupant {
            logical_agent_id: field(result, "logical_agent_id")?,
            incarnation_id: field(result, "incarnation_id")?,
        })
    }

    /// Start a session occupant in an existing Herdr pane.
    ///
    /// The initial message is a short trusted tell so start and trigger ask
    /// receipts stay separate. Triggered Nostr work uses [`HostWaiter::ask`].
    ///
    /// # Errors
    ///
    /// Returns an error when Kelpie cannot start the occupant or the runtime
    /// start did not succeed.
    pub fn start_occupant(
        &self,
        launch: &OccupantLaunch,
        bootstrap: &str,
        sender_id: Option<&str>,
    ) -> Result<StartedOccupant, KelpieError> {
        let timeout_ms = launch.timeout_ms.to_string();
        let cwd = launch.cwd.to_str().ok_or_else(|| {
            KelpieError::InvalidReceipt("occupant corpus path is not valid UTF-8".to_owned())
        })?;
        let mut arguments = vec![
            "--json".to_owned(),
            "start".to_owned(),
            "--name".to_owned(),
            launch.name.clone(),
            "--pane".to_owned(),
            launch.pane_id.clone(),
            "--terminal".to_owned(),
            launch.terminal_id.clone(),
            "--backend".to_owned(),
            launch.backend.clone(),
            "--cwd".to_owned(),
            cwd.to_owned(),
            "--timeout-ms".to_owned(),
            timeout_ms,
            "--keep-open".to_owned(),
            "--parentless".to_owned(),
            "--tell".to_owned(),
            "--stdin".to_owned(),
        ];
        if let Some(sender_id) = sender_id {
            arguments.extend(["--sender-id".to_owned(), sender_id.to_owned()]);
        }
        if let Some(logical_agent_id) = &launch.logical_agent_id {
            arguments.extend(["--logical-id".to_owned(), logical_agent_id.clone()]);
        }
        let arguments = arguments.iter().map(String::as_str).collect::<Vec<_>>();
        let output = self.invoke(&arguments, bootstrap.as_bytes())?;
        if !output.success {
            return Err(output.rejected());
        }
        let result = result(&output.receipt)?;
        let runtime = result
            .get("runtime_start")
            .ok_or_else(|| KelpieError::InvalidReceipt("missing runtime_start".to_owned()))?;
        if field(runtime, "outcome")? != "succeeded" {
            return Err(KelpieError::InvalidReceipt(
                "occupant runtime start did not succeed".to_owned(),
            ));
        }
        let started = StartedOccupant {
            logical_agent_id: field(result, "logical_agent_id")?,
            incarnation_id: field(result, "incarnation_id")?,
        };
        if launch
            .logical_agent_id
            .as_deref()
            .is_some_and(|expected| expected != started.logical_agent_id())
        {
            return Err(KelpieError::InvalidReceipt(
                "session occupant is not the recorded logical agent".to_owned(),
            ));
        }
        Ok(started)
    }

    /// Arm wall-clock renew on one occupant's exact incarnation.
    ///
    /// # Errors
    ///
    /// Returns an error when Kelpie rejects the policy or the receipt is invalid.
    pub fn arm_occupant_renew(
        &self,
        logical_agent_id: &str,
        incarnation_id: &str,
        snapshot_relpath: &str,
    ) -> Result<String, KelpieError> {
        let resume = occupant_renew_resume(snapshot_relpath);
        let output = self.invoke(
            &[
                "--json",
                "renew",
                "--recipient-id",
                logical_agent_id,
                "--recipient-incarnation",
                incarnation_id,
                "--prepare-prompt",
                OCCUPANT_RENEW_PREPARE,
                "--prompt",
                &resume,
                "--on-timeout",
                "abort",
                "--every",
                OCCUPANT_RENEW_EVERY,
            ],
            &[],
        )?;
        if !output.success {
            return Err(output.rejected());
        }
        field(result(&output.receipt)?, "renew_id")
    }

    fn bind_agent(
        &self,
        pane_id: &str,
        terminal_id: &str,
        name: &str,
        logical_agent_id: Option<&str>,
    ) -> Result<StartedOccupant, KelpieError> {
        let mut arguments = vec![
            "--json",
            "adopt",
            "--pane",
            pane_id,
            "--terminal",
            terminal_id,
            "--name",
            name,
        ];
        if let Some(logical_agent_id) = logical_agent_id {
            arguments.extend(["--logical-id", logical_agent_id]);
        }
        let output = self.invoke(&arguments, &[])?;
        if !output.success {
            if let Some(incarnation_id) = already_adopted_incarnation(&output.receipt) {
                return self.identity_from_whoami(name, incarnation_id);
            }
            return Err(output.rejected());
        }
        let result = result(&output.receipt)?;
        if field(result, "outcome")? != "succeeded" {
            return Err(KelpieError::InvalidReceipt(
                "adoption did not succeed".to_owned(),
            ));
        }
        Ok(StartedOccupant {
            logical_agent_id: field(result, "logical_agent_id")?,
            incarnation_id: field(result, "incarnation_id")?,
        })
    }

    fn identity_from_whoami(
        &self,
        name: &str,
        expected_incarnation: &str,
    ) -> Result<StartedOccupant, KelpieError> {
        let output = self.invoke(&["--json", "whoami", name], &[])?;
        if !output.success {
            return Err(output.rejected());
        }
        let result = result(&output.receipt)?;
        let occupant = StartedOccupant {
            logical_agent_id: field(result, "logical_agent_id")?,
            incarnation_id: field(result, "incarnation_id")?,
        };
        if occupant.incarnation_id() != expected_incarnation {
            return Err(KelpieError::InvalidReceipt(
                "already-adopted pane is not the requested alias".to_owned(),
            ));
        }
        Ok(occupant)
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
    pub(crate) fn with_runner(runner: impl CommandRunner + 'static) -> Self {
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

/// Socket waiter whose exact logical id owns every ask it sends.
#[derive(Debug)]
pub struct HostWaiter<'a> {
    client: &'a KelpieClient,
    identity: WaiterIdentity,
}

impl HostWaiter<'_> {
    /// Return the waiter identity retained from registration.
    #[must_use]
    pub fn identity(&self) -> &WaiterIdentity {
        &self.identity
    }

    /// Send Nostr text as a Kelpie ask to one session occupant.
    ///
    /// Kelpie receives the body on stdin and owns envelope escaping. The exact
    /// waiter logical id is supplied as sender, so relay identities cannot
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
        self.ask_named(recipient, None, nostr_body, idempotency_key)
    }

    /// Resolve a live occupant alias to durable ids.
    ///
    /// # Errors
    ///
    /// Returns an error when Kelpie cannot resolve the alias, or when it is not
    /// the recorded occupant.
    pub fn occupant_ids(
        &self,
        alias: &str,
        expected_logical_id: Option<&str>,
    ) -> Result<(String, String), KelpieError> {
        let recipient = self.resolve_recipient(alias)?;
        if expected_logical_id.is_some_and(|expected| expected != recipient.logical_agent_id) {
            return Err(KelpieError::InvalidReceipt(
                "session occupant is not the recorded logical agent".to_owned(),
            ));
        }
        Ok((recipient.logical_agent_id, recipient.incarnation_id))
    }

    /// Send an ask and require the alias to resolve to a recorded logical id.
    ///
    /// # Errors
    ///
    /// Returns an error unless Kelpie returns the durable ids needed to
    /// reconcile the attempt, or when the alias is not the recorded occupant.
    pub fn ask_named(
        &self,
        recipient: &str,
        expected_logical_id: Option<&str>,
        nostr_body: &str,
        idempotency_key: &str,
    ) -> Result<AskReceipt, KelpieError> {
        let recipient = self.resolve_recipient(recipient)?;
        if expected_logical_id.is_some_and(|expected| expected != recipient.logical_agent_id) {
            return Err(KelpieError::InvalidReceipt(
                "session occupant is not the recorded logical agent".to_owned(),
            ));
        }
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

    /// Cancel an in-flight ask owned by this waiter.
    ///
    /// # Errors
    ///
    /// Returns an error when Kelpie rejects the cancel.
    pub fn cancel(&self, ask_id: &str, reason: &str) -> Result<(), KelpieError> {
        let output = self.client.invoke(
            &[
                "--json",
                "cancel",
                ask_id,
                "--reason",
                reason,
                "--sender-id",
                self.identity.logical_agent_id(),
            ],
            &[],
        )?;
        if output.success {
            Ok(())
        } else {
            Err(output.rejected())
        }
    }

    fn resolve_recipient(&self, alias: &str) -> Result<RecipientIdentity, KelpieError> {
        let occupant = self.client.occupant_whoami(alias)?;
        Ok(RecipientIdentity {
            logical_agent_id: occupant.logical_agent_id().to_owned(),
            incarnation_id: occupant.incarnation_id().to_owned(),
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

fn occupant_alias_unbound(receipt: &Value) -> bool {
    error_class(receipt) == Some("conflict")
        && receipt
            .pointer("/error/message")
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("no ready agent for alias"))
}

fn already_adopted_incarnation(receipt: &Value) -> Option<&str> {
    if error_class(receipt) != Some("conflict") {
        return None;
    }
    let message = receipt.pointer("/error/message").and_then(Value::as_str)?;
    let remainder = message.rsplit_once("already adopted by ready incarnation ")?;
    remainder
        .1
        .split_whitespace()
        .next()
        .filter(|id| !id.is_empty())
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

    fn registered_waiter() -> CommandOutput {
        success(&serde_json::json!({
            "logical_agent_id": "waiter-agent",
            "public_name": "botserver",
            "delivery_transport": "socket_inbox"
        }))
    }

    #[test]
    fn names_socket_waiter_botserver() {
        assert_eq!(WAITER_NAME, "botserver");
    }

    #[test]
    fn registers_pane_less_socket_waiter_botserver() {
        let runner = Arc::new(FakeRunner::new([registered_waiter()]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));

        let waiter = client.register_waiter().expect("register waiter");

        assert_eq!(waiter.identity().logical_agent_id(), "waiter-agent");
        let calls = runner.calls.lock().expect("calls lock");
        assert_eq!(
            calls.as_slice(),
            &[(
                vec![
                    "--json".to_owned(),
                    "waiter-register".to_owned(),
                    "--name".to_owned(),
                    "botserver".to_owned(),
                    "--parentless".to_owned(),
                    "--idempotency-key".to_owned(),
                    WAITER_IDEMPOTENCY_KEY.to_owned(),
                ],
                Vec::new(),
            )]
        );
    }

    #[test]
    fn register_waiter_reuses_the_stable_idempotency_key() {
        let runner = Arc::new(FakeRunner::new([registered_waiter()]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));
        client.register_waiter().expect("reuse waiter");
        assert_eq!(
            runner.calls.lock().expect("calls lock")[0].0,
            vec![
                "--json",
                "waiter-register",
                "--name",
                "botserver",
                "--parentless",
                "--idempotency-key",
                WAITER_IDEMPOTENCY_KEY,
            ]
        );
    }

    #[test]
    fn register_waiter_with_key_passes_the_explicit_key() {
        let runner = Arc::new(FakeRunner::new([registered_waiter()]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));
        client
            .register_waiter_with_key("botserver-host-waiter-next")
            .expect("register");
        assert_eq!(
            runner.calls.lock().expect("calls lock")[0].0[6],
            "botserver-host-waiter-next"
        );
    }

    #[test]
    fn register_waiter_rejects_a_herdr_prompt_waiter() {
        let runner = Arc::new(FakeRunner::new([success(&serde_json::json!({
            "logical_agent_id": "waiter-agent",
            "public_name": "botserver",
            "delivery_transport": "herdr_prompt"
        }))]));
        let client = KelpieClient::with_runner(runner);
        let error = client.register_waiter().expect_err("pane waiter");
        assert!(error.to_string().contains("socket inbox"));
    }

    #[test]
    fn adopt_occupant_continues_recorded_logical_id() {
        let runner = Arc::new(FakeRunner::new([success(&serde_json::json!({
            "logical_agent_id": "occupant-agent",
            "incarnation_id": "occupant-incarnation-2",
            "operation_id": "adopt-operation",
            "outcome": "succeeded"
        }))]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));

        let occupant = client
            .adopt_occupant("w2:p1", "term-9", "bot-foobar", "occupant-agent")
            .expect("adopt occupant");

        assert_eq!(occupant.logical_agent_id(), "occupant-agent");
        assert_eq!(occupant.incarnation_id(), "occupant-incarnation-2");
        assert_eq!(
            runner.calls.lock().expect("calls lock")[0].0,
            vec![
                "--json",
                "adopt",
                "--pane",
                "w2:p1",
                "--terminal",
                "term-9",
                "--name",
                "bot-foobar",
                "--logical-id",
                "occupant-agent"
            ]
        );
    }

    #[test]
    fn adopt_occupant_rejects_a_different_logical_id() {
        let runner = Arc::new(FakeRunner::new([success(&serde_json::json!({
            "logical_agent_id": "twin-agent",
            "incarnation_id": "twin-incarnation",
            "operation_id": "adopt-operation",
            "outcome": "succeeded"
        }))]));
        let client = KelpieClient::with_runner(runner);

        let error = client
            .adopt_occupant("w2:p1", "term-9", "bot-foobar", "occupant-agent")
            .expect_err("twin");
        assert!(error
            .to_string()
            .contains("session occupant is not the recorded logical agent"));
    }

    #[test]
    fn occupant_whoami_treats_an_unbound_alias_as_unavailable() {
        let runner = Arc::new(FakeRunner::new([failure(
            "conflict",
            "no ready agent for alias bot-foobar; a live Herdr agent may hold that name unadopted",
        )]));
        let client = KelpieClient::with_runner(runner);
        let error = client.occupant_whoami("bot-foobar").expect_err("unbound");
        assert!(matches!(error, KelpieError::TargetUnavailable));
    }

    #[test]
    fn start_occupant_uses_tell_bootstrap_and_keeps_runtime_ids() {
        let runner = Arc::new(FakeRunner::new([success(&serde_json::json!({
            "logical_agent_id": "occupant-agent",
            "incarnation_id": "occupant-incarnation",
            "runtime_start": {
                "operation_id": "start-operation",
                "outcome": "succeeded"
            },
            "initial_message": {
                "message_id": "tell-id",
                "operation_id": "tell-operation",
                "outcome": "accepted"
            }
        }))]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));
        let started = client
            .start_occupant(
                &OccupantLaunch {
                    name: "bot-foobar".to_owned(),
                    pane_id: "w1:p4".to_owned(),
                    terminal_id: "term-4".to_owned(),
                    backend: "opencode".to_owned(),
                    cwd: PathBuf::from("/corpus"),
                    timeout_ms: 90_000,
                    logical_agent_id: None,
                },
                OCCUPANT_BOOTSTRAP,
                None,
            )
            .expect("start occupant");

        assert_eq!(started.logical_agent_id(), "occupant-agent");
        assert_eq!(started.incarnation_id(), "occupant-incarnation");
        let calls = runner.calls.lock().expect("calls lock");
        assert_eq!(
            calls[0].0,
            vec![
                "--json",
                "start",
                "--name",
                "bot-foobar",
                "--pane",
                "w1:p4",
                "--terminal",
                "term-4",
                "--backend",
                "opencode",
                "--cwd",
                "/corpus",
                "--timeout-ms",
                "90000",
                "--keep-open",
                "--parentless",
                "--tell",
                "--stdin"
            ]
        );
        assert_eq!(calls[0].1, OCCUPANT_BOOTSTRAP.as_bytes());
    }

    #[test]
    fn arm_occupant_renew_targets_exact_ids_and_aborts_on_timeout() {
        let runner = Arc::new(FakeRunner::new([success(&serde_json::json!({
            "renew_id": "renew-1",
            "recipient": "occupant-agent",
            "recipient_incarnation": "occupant-incarnation",
            "scheduled_at_ms": 1,
            "on_timeout": "abort",
            "phase": "scheduled",
            "every_ms": 2_700_000
        }))]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));
        let snapshot = ".botserver/places/bot-foobar.md";

        let renew_id = client
            .arm_occupant_renew("occupant-agent", "occupant-incarnation", snapshot)
            .expect("renew");

        assert_eq!(renew_id, "renew-1");
        let calls = runner.calls.lock().expect("calls lock");
        assert_eq!(calls[0].0[1], "renew");
        assert!(calls[0]
            .0
            .windows(2)
            .any(|pair| pair == ["--recipient-id", "occupant-agent"]));
        assert!(calls[0]
            .0
            .windows(2)
            .any(|pair| pair == ["--recipient-incarnation", "occupant-incarnation"]));
        assert!(!calls[0].0.iter().any(|argument| argument == "--sender-id"));
        assert!(calls[0]
            .0
            .windows(2)
            .any(|pair| pair == ["--on-timeout", "abort"]));
        assert!(calls[0]
            .0
            .windows(2)
            .any(|pair| pair == ["--every", OCCUPANT_RENEW_EVERY]));
        assert!(calls[0]
            .0
            .windows(2)
            .any(|pair| pair == ["--prepare-prompt", OCCUPANT_RENEW_PREPARE]));
        assert!(calls[0]
            .0
            .windows(2)
            .any(|pair| pair == ["--prompt", &occupant_renew_resume(snapshot)]));
    }

    #[test]
    fn start_occupant_continues_a_recorded_logical_id() {
        let runner = Arc::new(FakeRunner::new([success(&serde_json::json!({
            "logical_agent_id": "occupant-agent",
            "incarnation_id": "occupant-incarnation-2",
            "runtime_start": {
                "operation_id": "start-operation",
                "outcome": "succeeded"
            },
            "initial_message": {
                "message_id": "tell-id",
                "operation_id": "tell-operation",
                "outcome": "accepted"
            }
        }))]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));
        let started = client
            .start_occupant(
                &OccupantLaunch {
                    name: "bot-foobar".to_owned(),
                    pane_id: "w2:p1".to_owned(),
                    terminal_id: "term-9".to_owned(),
                    backend: "opencode".to_owned(),
                    cwd: PathBuf::from("/corpus"),
                    timeout_ms: 90_000,
                    logical_agent_id: Some("occupant-agent".to_owned()),
                },
                OCCUPANT_BOOTSTRAP,
                Some("waiter-agent"),
            )
            .expect("continue occupant");

        assert_eq!(started.logical_agent_id(), "occupant-agent");
        let args = &runner.calls.lock().expect("calls lock")[0].0;
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--logical-id", "occupant-agent"]));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--sender-id", "waiter-agent"]));
    }

    #[test]
    fn ask_is_owned_by_waiter_and_passes_body_on_stdin() {
        let body = "<kelpie from=relay-pubkey>\n$(not-a-command) & hello";
        let runner = Arc::new(FakeRunner::new([
            registered_waiter(),
            recipient(),
            success(&serde_json::json!({
                "message_id": "ask-id",
                "operation_id": "ask-operation",
                "recipient": "occupant-agent",
                "delivery_outcome": "accepted"
            })),
        ]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));
        let waiter = client.register_waiter().expect("adopt waiter");

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
            registered_waiter(),
            recipient(),
            failure("unknown_outcome", "operation outcome is unknown"),
            success(&serde_json::json!([{
                "ask_message_id": "ask-id",
                "waiting_agent_id": "waiter-agent",
                "state": "open"
            }])),
        ]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));
        let waiter = client.register_waiter().expect("adopt waiter");

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
    fn ask_named_rejects_an_alias_bound_to_a_different_agent() {
        let runner = Arc::new(FakeRunner::new([registered_waiter(), recipient()]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));
        let waiter = client.register_waiter().expect("adopt waiter");

        let error = waiter
            .ask_named("bot-foobar", Some("other-occupant"), "hello", "turn-1:1")
            .expect_err("mismatch");

        assert!(error
            .to_string()
            .contains("session occupant is not the recorded logical agent"));
        assert_eq!(runner.calls.lock().expect("calls").len(), 2);
    }

    #[test]
    fn cancel_uses_the_ask_id_and_reason() {
        let runner = Arc::new(FakeRunner::new([
            registered_waiter(),
            success(&serde_json::json!({})),
        ]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));
        let waiter = client.register_waiter().expect("adopt waiter");

        waiter.cancel("ask-id", "trigger edited").expect("cancel");

        assert_eq!(
            runner.calls.lock().expect("calls")[1].0,
            vec![
                "--json",
                "cancel",
                "ask-id",
                "--reason",
                "trigger edited",
                "--sender-id",
                "waiter-agent",
            ]
        );
    }

    #[test]
    fn parsed_kelpie_error_message_is_reported() {
        let runner = Arc::new(FakeRunner::new([failure(
            "conflict",
            "continue logical agent waiter-agent",
        )]));
        let client = KelpieClient::with_runner(runner);

        let error = client.register_waiter().expect_err("adoption conflict");

        assert!(error
            .to_string()
            .contains("continue logical agent waiter-agent"));
    }
}
pub mod actor;
pub mod ask_body;
pub mod config;
pub mod herdr;
pub mod inbox;
pub mod outbox;
pub mod progress;
pub mod relay;
pub mod snapshot;
pub mod sqlite;

#[cfg(test)]
mod spec_flows;

use botserver_domain::{BotId, EventId};

pub use botserver_domain::{TurnState, TurnTransition};

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

pub(crate) fn thread_root_for<R: HostRepository>(
    repository: &R,
    event_id: &botserver_domain::EventId,
) -> Result<Option<botserver_domain::EventId>, R::Error> {
    Ok(repository.indexed_event(event_id)?.and_then(|event| {
        let tags = serde_json::from_str::<Vec<Vec<String>>>(&event.tags_json).unwrap_or_default();
        botserver_domain::buzz::reply_thread_root(event_id, &tags)
    }))
}

/// Persisted binding between one bot and one Buzz channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord {
    pub bot_id: BotId,
    pub channel_id: String,
    pub session_name: String,
    pub occupant_logical_id: Option<String>,
    pub renew_id: Option<String>,
    pub ask_context_event_id: Option<EventId>,
    pub ask_context_created_at: Option<i64>,
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
    /// Unix seconds when the turn opened (D42 hold start). `None` for a
    /// queued turn, and for open turns recorded before this column existed.
    pub opened_at: Option<i64>,
}

/// Result of cancelling unclaimed work and enqueueing a replacement turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnReplacement {
    pub cancelled: TurnRecord,
    pub queued: TurnRecord,
}

/// Persistence used by the host ingest and turn-processing paths.
pub trait HostRepository {
    type Error;

    /// Acknowledge an emitted ingest action, returning false if already acknowledged.
    ///
    /// Consumers must acknowledge every emitted action, including actions they
    /// intentionally decline, so it no longer holds the relay replay cursor.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the operation cannot be persisted.
    fn mark_event_processed(&mut self, event_id: &EventId) -> Result<bool, Self::Error>;

    /// Index a relay event without consuming its processing marker.
    ///
    /// `pending_action` records that the classifier emitted an action whose
    /// acknowledgement must precede cursor advancement. Returns false when the
    /// event id was already indexed.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the event cannot be persisted.
    fn index_event(
        &mut self,
        event: &IndexedRelayEvent,
        pending_action: bool,
    ) -> Result<bool, Self::Error>;

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

    /// Return the latest indexed body for a triggering event, including edits.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when indexed events cannot be read.
    fn latest_body_for_event(&self, event_id: &EventId) -> Result<Option<String>, Self::Error>;

    /// Return a safe inclusive relay replay cursor.
    ///
    /// This is the oldest unacknowledged pending action timestamp, or the newest
    /// indexed timestamp minus the relay's accepted clock drift when none await
    /// acknowledgement. An empty index returns `None`.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the index cannot be read.
    fn relay_replay_since(&self) -> Result<Option<i64>, Self::Error>;

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

    /// Find a session by its public occupant name.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the session cannot be read.
    fn session_by_name(&self, session_name: &str) -> Result<Option<SessionRecord>, Self::Error>;

    /// Find a session by its bound occupant logical id.
    ///
    /// Unknown or ambiguous ids return `None`.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the session cannot be read.
    fn session_by_occupant_logical_id(
        &self,
        occupant_logical_id: &str,
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

    /// Claim an open turn so host cancel cannot win the publish race.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the claim cannot be persisted.
    fn claim_turn_for_publish(&mut self, ask_id: &str) -> Result<bool, Self::Error>;

    /// Release a publish claim after a failed relay publish.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the claim cannot be persisted.
    fn release_publish_claim(&mut self, ask_id: &str) -> Result<bool, Self::Error>;

    /// Cancel queued work for an edited or deleted triggering event.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the state cannot be persisted.
    fn cancel_queued_turn(&mut self, event_id: &EventId) -> Result<bool, Self::Error>;

    /// Cancel queued or unclaimed-open work for an edited or deleted trigger.
    ///
    /// Returns the cancelled turn when the host won the reservation race.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the state cannot be persisted.
    fn cancel_unclaimed_turn(
        &mut self,
        event_id: &EventId,
    ) -> Result<Option<TurnRecord>, Self::Error>;

    /// Cancel unclaimed work and enqueue a replacement turn for the same event.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the replacement cannot be persisted.
    fn replace_unclaimed_turn(
        &mut self,
        turn: &NewTurn,
    ) -> Result<Option<TurnReplacement>, Self::Error>;

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

    /// Channel ids of every persisted session, including posted ones.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the sessions cannot be read.
    fn known_channel_ids(&self) -> Result<Vec<String>, Self::Error>;

    /// Event ids of queued or open turns, in session order.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the turns cannot be read.
    fn active_event_ids(&self) -> Result<Vec<EventId>, Self::Error>;

    /// Persist an outbound attempt without overwriting an accepted event id.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the attempt cannot be persisted.
    fn save_outbound_attempt(
        &mut self,
        attempt: &crate::outbox::OutboundAttempt,
    ) -> Result<(), Self::Error>;

    /// Load a durable outbound attempt by ask id.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the attempt cannot be read.
    fn outbound_attempt(
        &self,
        ask_id: &str,
    ) -> Result<Option<crate::outbox::OutboundAttempt>, Self::Error>;

    /// Record the accepted outbound event id for a retry-safe republish.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the id cannot be persisted.
    fn mark_outbound_accepted(&mut self, ask_id: &str, event_id: &str)
        -> Result<bool, Self::Error>;

    /// Load the progress post row for one ask (D42).
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the row cannot be read.
    fn progress_post(
        &self,
        ask_id: &str,
    ) -> Result<Option<crate::progress::ProgressPost>, Self::Error>;

    /// Persist the progress post row for one ask (D42).
    ///
    /// Recorded before the ACK, before `send`, and before each `edit`.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the row cannot be persisted.
    fn save_progress_post(
        &mut self,
        post: &crate::progress::ProgressPost,
    ) -> Result<(), Self::Error>;

    /// This bot's progress rows and turns with relay work left.
    ///
    /// A pending body or a prepared create without an accepted id is
    /// returned. Ended rows are skipped.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the rows cannot be read.
    fn progress_posts_pending_flush(
        &self,
        bot_id: &BotId,
    ) -> Result<Vec<(crate::progress::ProgressPost, TurnRecord)>, Self::Error>;

    /// Event ids of this channel's accepted or prepared host progress
    /// posts, for snapshot and ask Context exclusion (D42).
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the rows cannot be read.
    fn progress_post_event_ids(&self, channel_id: &str) -> Result<Vec<EventId>, Self::Error>;
}
