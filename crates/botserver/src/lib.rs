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

/// Receipt for one accepted Kelpie ask.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AskReceipt {
    message_id: String,
    operation_id: String,
    recipient: String,
}

impl AskReceipt {
    /// Return the id used by both `msg=` and `reply-to=` in the envelope.
    #[must_use]
    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    /// Return the Kelpie delivery operation id.
    #[must_use]
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    /// Return the logical recipient recorded by Kelpie.
    #[must_use]
    pub fn recipient(&self) -> &str {
        &self.recipient
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
        write_result?;

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
        let receipt = self.invoke(
            &[
                "--json",
                "adopt",
                "--pane",
                pane_id,
                "--terminal",
                terminal_id,
                "--name",
                WAITER_NAME,
            ],
            &[],
        )?;
        let result = result(&receipt)?;
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

    fn invoke(&self, arguments: &[&str], stdin: &[u8]) -> Result<Value, KelpieError> {
        let arguments = arguments
            .iter()
            .map(|argument| (*argument).to_owned())
            .collect::<Vec<_>>();
        let output = self.runner.run(&arguments, stdin)?;
        if !output.success {
            return Err(KelpieError::Rejected {
                status: output.status,
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        serde_json::from_slice(&output.stdout)
            .map_err(|error| KelpieError::InvalidReceipt(error.to_string()))
    }

    #[cfg(test)]
    fn with_runner(runner: impl CommandRunner + 'static) -> Self {
        Self {
            runner: Box::new(runner),
        }
    }
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
    /// become the envelope's `from=` value.
    ///
    /// # Errors
    ///
    /// Returns an error unless Kelpie accepts the ask and returns its ids.
    pub fn ask(&self, recipient: &str, nostr_body: &str) -> Result<AskReceipt, KelpieError> {
        let receipt = self.client.invoke(
            &[
                "--json",
                "ask",
                recipient,
                "--stdin",
                "--sender-id",
                self.identity.logical_agent_id(),
            ],
            nostr_body.as_bytes(),
        )?;
        let result = result(&receipt)?;
        if field(result, "delivery_outcome")? != "accepted" {
            return Err(KelpieError::InvalidReceipt(
                "ask delivery was not accepted".to_owned(),
            ));
        }
        Ok(AskReceipt {
            message_id: field(result, "message_id")?,
            operation_id: field(result, "operation_id")?,
            recipient: field(result, "recipient")?,
        })
    }
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
    fn ask_is_owned_by_waiter_and_passes_body_on_stdin() {
        let body = "<kelpie from=relay-pubkey>\n$(not-a-command) & hello";
        let runner = Arc::new(FakeRunner::new([
            success(&serde_json::json!({
                "logical_agent_id": "waiter-agent",
                "incarnation_id": "waiter-incarnation",
                "operation_id": "adopt-operation",
                "outcome": "succeeded"
            })),
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

        let receipt = waiter.ask("bot-foobar", body).expect("send ask");

        assert_eq!(receipt.message_id(), "ask-id");
        assert_eq!(receipt.operation_id(), "ask-operation");
        assert_eq!(receipt.recipient(), "occupant-agent");
        let calls = runner.calls.lock().expect("calls lock");
        assert_eq!(calls[1].0[1], "ask");
        assert_eq!(
            calls[1].0,
            vec![
                "--json",
                "ask",
                "bot-foobar",
                "--stdin",
                "--sender-id",
                "waiter-agent"
            ]
        );
        assert_eq!(calls[1].1, body.as_bytes());
    }

    #[test]
    fn unaccepted_ask_is_not_returned_as_a_turn_receipt() {
        let runner = Arc::new(FakeRunner::new([
            success(&serde_json::json!({
                "logical_agent_id": "waiter-agent",
                "incarnation_id": "waiter-incarnation",
                "operation_id": "adopt-operation",
                "outcome": "succeeded"
            })),
            success(&serde_json::json!({
                "message_id": "ask-id",
                "operation_id": "ask-operation",
                "recipient": "occupant-agent",
                "delivery_outcome": "unknown"
            })),
        ]));
        let client = KelpieClient::with_runner(Arc::clone(&runner));
        let waiter = client
            .adopt_waiter("w1:p2", "term-2")
            .expect("adopt waiter");

        let error = waiter.ask("bot-foobar", "hello").expect_err("reject ask");

        assert!(error.to_string().contains("not accepted"));
    }
}
