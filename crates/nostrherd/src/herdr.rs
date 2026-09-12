//! Herdr workspace allocation for newly started occupants.

use std::fmt;
use std::io;
use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::Value;

use crate::actor::{OccupantPane, OccupantPaneAllocator};
use crate::{CommandRunner, ProcessRunner};

/// Process-backed allocator that creates one Herdr workspace per occupant.
#[derive(Debug)]
pub struct HerdrPaneAllocator {
    runner: Box<dyn CommandRunner>,
}

impl Default for HerdrPaneAllocator {
    fn default() -> Self {
        Self::new("herdr")
    }
}

impl HerdrPaneAllocator {
    /// Create an allocator backed by a Herdr executable.
    #[must_use]
    pub fn new(program: impl AsRef<Path>) -> Self {
        Self {
            runner: Box::new(ProcessRunner::new(program)),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_runner(runner: impl CommandRunner + 'static) -> Self {
        Self {
            runner: Box::new(runner),
        }
    }
}

/// Failure while creating an occupant pane.
#[derive(Debug)]
pub enum HerdrError {
    /// The Herdr process could not be started or completed.
    Io(io::Error),
    /// Herdr rejected the workspace creation.
    Rejected { status: String, stderr: String },
    /// Herdr returned JSON without pane and terminal ids.
    InvalidReceipt(String),
    /// More than one live pane holds the name in this corpus.
    Ambiguous { session_name: String },
}

impl fmt::Display for HerdrError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(
                formatter,
                "failed to invoke Herdr: {error}; install herdr on PATH and leave Herdr running"
            ),
            Self::Rejected { status, stderr } => {
                write!(formatter, "Herdr exited with {status}: {stderr}; check that Herdr is running and reachable by this host")
            }
            Self::InvalidReceipt(reason) => write!(formatter, "invalid Herdr receipt: {reason}"),
            Self::Ambiguous { session_name } => write!(
                formatter,
                "Herdr reports more than one pane holding {session_name} in this corpus; refusing to choose"
            ),
        }
    }
}

impl std::error::Error for HerdrError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Rejected { .. } | Self::InvalidReceipt(_) | Self::Ambiguous { .. } => None,
        }
    }
}

impl OccupantPaneAllocator for HerdrPaneAllocator {
    type Error = HerdrError;

    fn allocate(&self, session_name: &str, cwd: &Path) -> Result<OccupantPane, Self::Error> {
        let cwd = cwd.to_str().ok_or_else(|| {
            HerdrError::InvalidReceipt("occupant corpus path is not valid UTF-8".to_owned())
        })?;
        let output = self
            .runner
            .run(
                &[
                    "workspace".to_owned(),
                    "create".to_owned(),
                    "--cwd".to_owned(),
                    cwd.to_owned(),
                    "--label".to_owned(),
                    session_name.to_owned(),
                    "--no-focus".to_owned(),
                ],
                &[],
            )
            .map_err(HerdrError::Io)?;
        if !output.success {
            return Err(HerdrError::Rejected {
                status: output.status,
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        let receipt: Value = serde_json::from_slice(&output.stdout).map_err(|error| {
            HerdrError::InvalidReceipt(format!("herdr workspace create was not JSON: {error}"))
        })?;
        occupant_pane(&receipt)
    }

    fn claimed(&self, session_name: &str, cwd: &Path) -> Result<Option<OccupantPane>, Self::Error> {
        let cwd = cwd.to_str().ok_or_else(|| {
            HerdrError::InvalidReceipt("occupant corpus path is not valid UTF-8".to_owned())
        })?;
        let output = self
            .runner
            .run(&["agent".to_owned(), "list".to_owned()], &[])
            .map_err(HerdrError::Io)?;
        if !output.success {
            return Err(HerdrError::Rejected {
                status: output.status,
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        let receipt: Value = serde_json::from_slice(&output.stdout).map_err(|error| {
            HerdrError::InvalidReceipt(format!("herdr agent list was not JSON: {error}"))
        })?;
        // One pane per name in this corpus. More than one is a state the
        // caller cannot resolve by name alone, and choosing is worse than
        // refusing: the loser may be the pane actually in use (D74).
        let mut matches = receipt
            .pointer("/result/agents")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|agent| {
                agent.get("name").and_then(Value::as_str) == Some(session_name)
                    && agent.get("cwd").and_then(Value::as_str) == Some(cwd)
            })
            .filter_map(claimed_pane);
        let Some(first) = matches.next() else {
            return Ok(None);
        };
        if matches.next().is_some() {
            return Err(HerdrError::Ambiguous {
                session_name: session_name.to_owned(),
            });
        }
        Ok(Some(first))
    }

    fn release(&self, pane: &OccupantPane) -> Result<(), Self::Error> {
        // Closing the root pane closes the workspace it was created with.
        let output = self
            .runner
            .run(
                &["pane".to_owned(), "close".to_owned(), pane.pane_id.clone()],
                &[],
            )
            .map_err(HerdrError::Io)?;
        if !output.success {
            return Err(HerdrError::Rejected {
                status: output.status,
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        Ok(())
    }
}

/// Read the current Herdr pane and terminal from `HERDR_PANE_ID`.
///
/// # Errors
///
/// Returns an error when the pane id is missing or Herdr cannot describe it.
pub fn current_pane() -> Result<OccupantPane, HerdrError> {
    current_pane_with("herdr")
}

/// Read the current Herdr pane using an explicit Herdr executable.
///
/// # Errors
///
/// Returns an error when the pane id is missing or Herdr cannot describe it.
pub fn current_pane_with(program: impl AsRef<Path>) -> Result<OccupantPane, HerdrError> {
    let pane_id = std::env::var("HERDR_PANE_ID")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| HerdrError::InvalidReceipt("missing HERDR_PANE_ID".to_owned()))?;
    let child = Command::new(program.as_ref())
        .args(["pane", "get", &pane_id])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(HerdrError::Io)?;
    let output = child.wait_with_output().map_err(HerdrError::Io)?;
    if !output.status.success() {
        return Err(HerdrError::Rejected {
            status: output.status.to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    let receipt: Value = serde_json::from_slice(&output.stdout).map_err(|error| {
        HerdrError::InvalidReceipt(format!("herdr pane get was not JSON: {error}"))
    })?;
    occupant_pane(&receipt)
}

/// Read one `herdr agent list` entry as a pane already holding a name.
fn claimed_pane(agent: &Value) -> Option<OccupantPane> {
    let pane_id = agent.get("pane_id").and_then(Value::as_str)?;
    let terminal_id = agent.get("terminal_id").and_then(Value::as_str)?;
    Some(OccupantPane {
        pane_id: pane_id.to_owned(),
        terminal_id: terminal_id.to_owned(),
    })
}

fn occupant_pane(receipt: &Value) -> Result<OccupantPane, HerdrError> {
    let pane = receipt
        .pointer("/result/root_pane")
        .or_else(|| receipt.pointer("/root_pane"))
        .or_else(|| receipt.pointer("/result/pane"))
        .or_else(|| receipt.pointer("/pane"))
        .ok_or_else(|| HerdrError::InvalidReceipt("missing root_pane".to_owned()))?;
    let pane_id = pane
        .get("pane_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| HerdrError::InvalidReceipt("missing pane_id".to_owned()))?;
    let terminal_id = pane
        .get("terminal_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| HerdrError::InvalidReceipt("missing terminal_id".to_owned()))?;
    Ok(OccupantPane {
        pane_id: pane_id.to_owned(),
        terminal_id: terminal_id.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::CommandOutput;

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

    fn workspace_created() -> CommandOutput {
        CommandOutput {
            success: true,
            status: "exit status: 0".to_owned(),
            stdout: serde_json::to_vec(&serde_json::json!({
                "result": {
                    "workspace": {
                        "workspace_id": "w9",
                        "label": "pr-sebastian"
                    },
                    "tab": { "tab_id": "w9:t1" },
                    "root_pane": {
                        "pane_id": "w9:p1",
                        "terminal_id": "term-1"
                    }
                }
            }))
            .expect("json"),
            stderr: Vec::new(),
        }
    }

    #[test]
    fn occupant_pane_reads_root_pane_ids() {
        let receipt = serde_json::json!({
            "result": {
                "root_pane": {
                    "pane_id": "w2:p1",
                    "terminal_id": "term-9"
                }
            }
        });
        assert_eq!(
            occupant_pane(&receipt).expect("pane"),
            OccupantPane {
                pane_id: "w2:p1".to_owned(),
                terminal_id: "term-9".to_owned(),
            }
        );
    }

    #[test]
    fn occupant_pane_reads_pane_get_ids() {
        let receipt = serde_json::json!({
            "result": {
                "pane": {
                    "pane_id": "w1:p2",
                    "terminal_id": "term-2"
                }
            }
        });
        assert_eq!(
            occupant_pane(&receipt).expect("pane"),
            OccupantPane {
                pane_id: "w1:p2".to_owned(),
                terminal_id: "term-2".to_owned(),
            }
        );
    }

    #[test]
    fn allocate_uses_workspace_create_not_tab_create() {
        let runner = Arc::new(FakeRunner::new([workspace_created()]));
        let allocator = HerdrPaneAllocator::with_runner(Arc::clone(&runner));

        let pane = allocator
            .allocate("pr-sebastian", Path::new("/corpus/pr"))
            .expect("allocate");

        assert_eq!(
            pane,
            OccupantPane {
                pane_id: "w9:p1".to_owned(),
                terminal_id: "term-1".to_owned(),
            }
        );
        let calls = runner.calls.lock().expect("calls lock");
        assert_eq!(
            calls.as_slice(),
            &[(
                vec![
                    "workspace".to_owned(),
                    "create".to_owned(),
                    "--cwd".to_owned(),
                    "/corpus/pr".to_owned(),
                    "--label".to_owned(),
                    "pr-sebastian".to_owned(),
                    "--no-focus".to_owned(),
                ],
                Vec::new(),
            )]
        );
        assert!(!calls[0].0.iter().any(|argument| argument == "tab"));
    }

    #[test]
    fn release_closes_the_allocated_pane() {
        let runner = Arc::new(FakeRunner::new([CommandOutput {
            success: true,
            status: "exit status: 0".to_owned(),
            stdout: b"{}".to_vec(),
            stderr: Vec::new(),
        }]));
        let allocator = HerdrPaneAllocator::with_runner(Arc::clone(&runner));

        allocator
            .release(&OccupantPane {
                pane_id: "w9:p1".to_owned(),
                terminal_id: "term-1".to_owned(),
            })
            .expect("release");

        let calls = runner.calls.lock().expect("calls lock");
        assert_eq!(
            calls.as_slice(),
            &[(
                vec!["pane".to_owned(), "close".to_owned(), "w9:p1".to_owned()],
                Vec::new(),
            )]
        );
    }

    #[test]
    fn release_reports_a_refused_close() {
        let runner = FakeRunner::new([CommandOutput {
            success: false,
            status: "exit status: 1".to_owned(),
            stdout: Vec::new(),
            stderr: b"pane w9:p1 not found".to_vec(),
        }]);
        let error = HerdrPaneAllocator::with_runner(runner)
            .release(&OccupantPane {
                pane_id: "w9:p1".to_owned(),
                terminal_id: "term-1".to_owned(),
            })
            .expect_err("refused");
        assert!(error.to_string().contains("not found"), "{error}");
    }

    #[test]
    fn allocate_rejects_a_non_json_workspace_create_receipt() {
        let runner = FakeRunner::new([CommandOutput {
            success: true,
            status: "exit status: 0".to_owned(),
            stdout: b"not json".to_vec(),
            stderr: Vec::new(),
        }]);
        let error = HerdrPaneAllocator::with_runner(runner)
            .allocate("bot-foobar", Path::new("/corpus"))
            .expect_err("invalid");
        assert!(error.to_string().contains("herdr workspace create"));
    }

    fn agent_list(agents: &serde_json::Value) -> CommandOutput {
        CommandOutput {
            success: true,
            status: "exit status: 0".to_owned(),
            stdout: serde_json::to_vec(&serde_json::json!({
                "id": "cli:agent:list",
                "result": { "agents": agents }
            }))
            .expect("json"),
            stderr: Vec::new(),
        }
    }

    fn live_opencode_agent(name: &str, cwd: &str) -> serde_json::Value {
        serde_json::json!({
            "name": name,
            "pane_id": "w1VS:p1",
            "terminal_id": "term_65b4bb0fe052885",
            "cwd": cwd,
        })
    }

    #[test]
    fn claimed_finds_the_pane_holding_the_name_in_the_corpus() {
        let runner = FakeRunner::new([agent_list(&serde_json::json!([live_opencode_agent(
            "bot-eng-prs",
            "/home/daniel/code/botserver-bot",
        )]))]);
        let claimed = HerdrPaneAllocator::with_runner(runner)
            .claimed("bot-eng-prs", Path::new("/home/daniel/code/botserver-bot"))
            .expect("claimed")
            .expect("found");
        assert_eq!(claimed.pane_id, "w1VS:p1");
        assert_eq!(claimed.terminal_id, "term_65b4bb0fe052885");
    }

    #[test]
    fn claimed_ignores_a_name_held_in_another_corpus() {
        let runner = FakeRunner::new([agent_list(&serde_json::json!([live_opencode_agent(
            "bot-eng-prs",
            "/other/corpus",
        )]))]);
        let claimed = HerdrPaneAllocator::with_runner(runner)
            .claimed("bot-eng-prs", Path::new("/home/daniel/code/botserver-bot"))
            .expect("claimed");
        assert_eq!(claimed, None);
    }

    /// Two live panes in one corpus holding one name cannot be told apart.
    ///
    /// The caller closes a pane before starting, so choosing the wrong one
    /// would end a working occupant (D74).
    #[test]
    fn claimed_refuses_two_panes_holding_the_same_name_in_the_same_corpus() {
        let mut second = live_opencode_agent("bot-eng-prs", "/corpus");
        second["pane_id"] = serde_json::json!("w1XX:p1");
        second["terminal_id"] = serde_json::json!("term_husk");
        let runner = FakeRunner::new([agent_list(&serde_json::json!([
            live_opencode_agent("bot-eng-prs", "/corpus"),
            second,
        ]))]);
        let error = HerdrPaneAllocator::with_runner(runner)
            .claimed("bot-eng-prs", Path::new("/corpus"))
            .expect_err("ambiguous");
        assert!(error.to_string().contains("more than one pane"), "{error}");
    }
}
