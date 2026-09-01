//! Herdr pane allocation for newly started occupants.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde_json::Value;

use crate::actor::{OccupantPane, OccupantPaneAllocator};

/// Process-backed allocator that creates one Herdr tab per occupant.
#[derive(Debug, Clone)]
pub struct HerdrPaneAllocator {
    program: PathBuf,
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
            program: program.as_ref().to_owned(),
        }
    }
}

/// Failure while creating an occupant pane.
#[derive(Debug)]
pub enum HerdrError {
    /// The Herdr process could not be started or completed.
    Io(io::Error),
    /// Herdr rejected the tab creation.
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
        let child = Command::new(&self.program)
            .args([
                "tab",
                "create",
                "--cwd",
                cwd,
                "--label",
                session_name,
                "--no-focus",
            ])
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
            HerdrError::InvalidReceipt(format!("herdr tab create was not JSON: {error}"))
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
    use super::*;

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
}
