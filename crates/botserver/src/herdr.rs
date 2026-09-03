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
}

impl fmt::Display for HerdrError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "failed to invoke Herdr: {error}"),
            Self::Rejected { status, stderr } => {
                write!(formatter, "Herdr exited with {status}: {stderr}")
            }
            Self::InvalidReceipt(reason) => write!(formatter, "invalid Herdr receipt: {reason}"),
        }
    }
}

impl std::error::Error for HerdrError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Rejected { .. } | Self::InvalidReceipt(_) => None,
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
}
