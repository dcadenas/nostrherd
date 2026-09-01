//! Occupant CLI: publish as the operator, then resolve the Kelpie ask.

use std::fmt;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::process::{Command, ExitCode, Stdio};

use botserver::sqlite::SqliteRepository;
use botserver::{HostRepository, TurnRecord, TurnState};
use botserver_domain::EventId;
use clap::error::ErrorKind;
use clap::{ArgGroup, Parser, Subcommand};
use serde_json::Value;

const OUTBOUND_PREFIX: &str = "[bot]:";

#[derive(Debug, Parser)]
#[command(about = "Publish stamped Nostr messages as the operator")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Send one stamped message to a Buzz channel.
    Send(SendArgs),
}

#[derive(Debug, clap::Args)]
#[command(group(
    ArgGroup::new("body_source")
        .required(true)
        .multiple(false)
        .args(["stdin", "file"])
))]
struct SendArgs {
    /// Read the message body from standard input.
    #[arg(long)]
    stdin: bool,

    /// Read the message body from this file.
    #[arg(long)]
    file: Option<PathBuf>,

    /// SQLite host-state database containing the in-flight turn.
    #[arg(long, requires = "ask_id")]
    database: Option<PathBuf>,

    /// Kelpie ask id for the in-flight turn.
    #[arg(long, requires = "database")]
    ask_id: Option<String>,

    /// Buzz channel UUID for the in-flight turn.
    #[arg(long)]
    channel: String,

    /// Trigger event id to reply to when the turn is threaded.
    #[arg(long)]
    reply_to: Option<String>,

    /// Pubkey to mention in the outbound event; may be repeated.
    #[arg(long)]
    mention: Vec<String>,

    /// Envchain namespace containing the Buzz credentials.
    #[arg(long)]
    envchain: String,
}

#[derive(Debug)]
enum BotcliError {
    Arguments(String),
    Io(io::Error),
    Database(rusqlite::Error),
    TurnNotFound,
    TurnNotOpen(TurnState),
    CoordinatesMismatch,
    AskNotActive(String),
    CommandFailed {
        program: &'static str,
        status: String,
        stderr: String,
    },
    InvalidKelpieReceipt(String),
    InvalidPublishReceipt(String),
    StateChanged,
}

impl fmt::Display for BotcliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Arguments(message) => formatter.write_str(message),
            Self::Io(error) => write!(formatter, "I/O failed: {error}"),
            Self::Database(error) => write!(formatter, "host database failed: {error}"),
            Self::TurnNotFound => formatter.write_str("the ask has no persisted turn"),
            Self::TurnNotOpen(state) => write!(formatter, "the turn is not open ({state:?})"),
            Self::CoordinatesMismatch => {
                formatter.write_str("the supplied channel or reply target does not match the turn")
            }
            Self::AskNotActive(state) => {
                write!(formatter, "the Kelpie ask is not active ({state})")
            }
            Self::CommandFailed {
                program,
                status,
                stderr,
            } => write!(formatter, "{program} exited with {status}: {stderr}"),
            Self::InvalidKelpieReceipt(reason) => {
                write!(formatter, "invalid Kelpie receipt: {reason}")
            }
            Self::InvalidPublishReceipt(reason) => {
                write!(formatter, "invalid publish receipt: {reason}")
            }
            Self::StateChanged => formatter.write_str("the turn state changed during publication"),
        }
    }
}

impl std::error::Error for BotcliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Database(error) => Some(error),
            Self::Arguments(_)
            | Self::TurnNotFound
            | Self::TurnNotOpen(_)
            | Self::CoordinatesMismatch
            | Self::AskNotActive(_)
            | Self::CommandFailed { .. }
            | Self::InvalidKelpieReceipt(_)
            | Self::InvalidPublishReceipt(_)
            | Self::StateChanged => None,
        }
    }
}

impl From<io::Error> for BotcliError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<rusqlite::Error> for BotcliError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

#[derive(Debug)]
struct Output {
    success: bool,
    status: String,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

trait CommandRunner {
    fn run(
        &mut self,
        program: &'static str,
        arguments: &[String],
        stdin: &[u8],
    ) -> io::Result<Output>;
}

#[derive(Debug)]
struct ProcessRunner;

impl CommandRunner for ProcessRunner {
    fn run(
        &mut self,
        program: &'static str,
        arguments: &[String],
        stdin: &[u8],
    ) -> io::Result<Output> {
        let mut child = Command::new(program)
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .expect("piped stdin is available")
            .write_all(stdin)?;
        let output = child.wait_with_output()?;
        Ok(Output {
            success: output.status.success(),
            status: output.status.to_string(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

trait TurnRepository {
    fn turn_by_ask_id(&self, ask_id: &str) -> Result<Option<TurnRecord>, rusqlite::Error>;
    fn mark_posted(&mut self, ask_id: &str) -> Result<bool, rusqlite::Error>;
}

impl TurnRepository for SqliteRepository {
    fn turn_by_ask_id(&self, ask_id: &str) -> Result<Option<TurnRecord>, rusqlite::Error> {
        HostRepository::turn_by_ask_id(self, ask_id)
    }

    fn mark_posted(&mut self, ask_id: &str) -> Result<bool, rusqlite::Error> {
        self.set_turn_state(ask_id, TurnState::Posted)
    }
}

fn main() -> ExitCode {
    match execute() {
        Ok(Execution::Receipt(receipt)) => {
            println!("{receipt}");
            ExitCode::SUCCESS
        }
        Ok(Execution::Help(help)) => {
            print!("{help}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{}", serde_json::json!({ "error": error.to_string() }));
            ExitCode::FAILURE
        }
    }
}

enum Execution {
    Receipt(Value),
    Help(String),
}

fn execute() -> Result<Execution, BotcliError> {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            return Ok(Execution::Help(error.to_string()));
        }
        Err(error) => return Err(BotcliError::Arguments(error.to_string())),
    };
    let Cli {
        command: Commands::Send(arguments),
    } = cli;
    let body = read_body(&arguments)?;
    let mut repository = arguments
        .database
        .as_ref()
        .map(SqliteRepository::open)
        .transpose()?;
    publish(&arguments, &body, repository.as_mut(), &mut ProcessRunner).map(Execution::Receipt)
}

fn read_body(arguments: &SendArgs) -> Result<Vec<u8>, BotcliError> {
    let mut body = Vec::new();
    if arguments.stdin {
        io::stdin().read_to_end(&mut body)?;
    } else if let Some(path) = &arguments.file {
        body = std::fs::read(path)?;
    }
    Ok(body)
}

fn publish(
    arguments: &SendArgs,
    body: &[u8],
    repository: Option<&mut impl TurnRepository>,
    runner: &mut impl CommandRunner,
) -> Result<Value, BotcliError> {
    let mut repository = repository;
    if let Some(ask_id) = &arguments.ask_id {
        let turn = repository
            .as_mut()
            .expect("clap requires a database with an ask id")
            .turn_by_ask_id(ask_id)?
            .ok_or(BotcliError::TurnNotFound)?;
        validate_turn(arguments, &turn)?;
        ensure_ask_active(runner, ask_id)?;
    }

    let stamped = stamp(body);
    let event_id = run_publish(runner, arguments, &stamped)?;
    if let Some(ask_id) = &arguments.ask_id {
        if !repository
            .expect("clap requires a database with an ask id")
            .mark_posted(ask_id)?
        {
            return Err(BotcliError::StateChanged);
        }
        run_final(runner, ask_id)?;
    }

    let mut receipt = serde_json::Map::from_iter([("event_id".to_owned(), event_id.into())]);
    if let Some(ask_id) = &arguments.ask_id {
        receipt.insert("ask_id".to_owned(), ask_id.clone().into());
    }
    Ok(receipt.into())
}

fn validate_turn(arguments: &SendArgs, turn: &TurnRecord) -> Result<(), BotcliError> {
    if turn.state != TurnState::Open {
        return Err(BotcliError::TurnNotOpen(turn.state));
    }
    let persisted_reply = turn.reply_to_event_id.as_ref().map(EventId::as_str);
    if turn.channel_id != arguments.channel || persisted_reply != arguments.reply_to.as_deref() {
        return Err(BotcliError::CoordinatesMismatch);
    }
    Ok(())
}

fn ensure_ask_active(runner: &mut impl CommandRunner, ask_id: &str) -> Result<(), BotcliError> {
    let output = runner.run(
        "kelpie",
        &[
            "--json".to_owned(),
            "ask-info".to_owned(),
            ask_id.to_owned(),
        ],
        &[],
    )?;
    ensure_success("kelpie", &output)?;
    let receipt: Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| BotcliError::InvalidKelpieReceipt(error.to_string()))?;
    let state = receipt
        .pointer("/result/state")
        .and_then(Value::as_str)
        .ok_or_else(|| BotcliError::InvalidKelpieReceipt("missing ask state".to_owned()))?;
    if !matches!(state, "open" | "in_progress") {
        return Err(BotcliError::AskNotActive(state.to_owned()));
    }
    Ok(())
}

fn stamp(body: &[u8]) -> Vec<u8> {
    let mut stamped = Vec::with_capacity(OUTBOUND_PREFIX.len() + 1 + body.len());
    stamped.extend_from_slice(OUTBOUND_PREFIX.as_bytes());
    if !body.is_empty() {
        stamped.push(b' ');
        stamped.extend_from_slice(body);
    }
    stamped
}

fn run_publish(
    runner: &mut impl CommandRunner,
    arguments: &SendArgs,
    body: &[u8],
) -> Result<String, BotcliError> {
    let mut command_arguments = vec![
        arguments.envchain.clone(),
        "buzz".to_owned(),
        "messages".to_owned(),
        "send".to_owned(),
        "--channel".to_owned(),
        arguments.channel.clone(),
        "--content".to_owned(),
        "-".to_owned(),
    ];
    if let Some(reply_to) = &arguments.reply_to {
        command_arguments.extend(["--reply-to".to_owned(), reply_to.clone()]);
    }
    for mention in &arguments.mention {
        command_arguments.extend(["--mention".to_owned(), mention.clone()]);
    }
    let output = runner.run("envchain", &command_arguments, body)?;
    ensure_success("envchain", &output)?;
    let receipt: Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| BotcliError::InvalidPublishReceipt(error.to_string()))?;
    if receipt.get("accepted").and_then(Value::as_bool) != Some(true) {
        return Err(BotcliError::InvalidPublishReceipt(
            "relay did not accept the event".to_owned(),
        ));
    }
    receipt
        .get("event_id")
        .and_then(Value::as_str)
        .filter(|event_id| !event_id.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| BotcliError::InvalidPublishReceipt("missing event_id".to_owned()))
}

fn run_final(runner: &mut impl CommandRunner, ask_id: &str) -> Result<(), BotcliError> {
    let output = runner.run(
        "kelpie",
        &[
            "reply".to_owned(),
            ask_id.to_owned(),
            "--final".to_owned(),
            "--stdin".to_owned(),
        ],
        b"published",
    )?;
    ensure_success("kelpie", &output)
}

fn ensure_success(program: &'static str, output: &Output) -> Result<(), BotcliError> {
    if output.success {
        return Ok(());
    }
    Err(BotcliError::CommandFailed {
        program,
        status: output.status.clone(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use botserver_domain::{BotId, EventId};

    use super::*;

    #[derive(Debug)]
    struct FakeRepository {
        turn: TurnRecord,
        marked_posted: bool,
    }

    impl TurnRepository for FakeRepository {
        fn turn_by_ask_id(&self, ask_id: &str) -> Result<Option<TurnRecord>, rusqlite::Error> {
            Ok((self.turn.ask_id.as_deref() == Some(ask_id)).then(|| self.turn.clone()))
        }

        fn mark_posted(&mut self, ask_id: &str) -> Result<bool, rusqlite::Error> {
            if self.turn.ask_id.as_deref() != Some(ask_id) || self.turn.state != TurnState::Open {
                return Ok(false);
            }
            self.turn.state = TurnState::Posted;
            self.marked_posted = true;
            Ok(true)
        }
    }

    #[derive(Debug)]
    struct FakeRunner {
        outputs: VecDeque<Output>,
        calls: Vec<(&'static str, Vec<String>, Vec<u8>)>,
    }

    impl CommandRunner for FakeRunner {
        fn run(
            &mut self,
            program: &'static str,
            arguments: &[String],
            stdin: &[u8],
        ) -> io::Result<Output> {
            self.calls
                .push((program, arguments.to_vec(), stdin.to_vec()));
            self.outputs
                .pop_front()
                .ok_or_else(|| io::Error::other("missing fake output"))
        }
    }

    fn send_args() -> SendArgs {
        SendArgs {
            stdin: true,
            file: None,
            database: Some(PathBuf::from("state.sqlite")),
            ask_id: Some("ask-id".to_owned()),
            channel: "ab12cd34-5678-90ab-cdef-0123456789ab".to_owned(),
            reply_to: Some("a".repeat(64)),
            mention: vec!["b".repeat(64)],
            envchain: "botserver".to_owned(),
        }
    }

    fn turn(state: TurnState) -> TurnRecord {
        TurnRecord {
            sequence: 1,
            bot_id: BotId::new("bot").expect("bot id"),
            channel_id: send_args().channel,
            event_id: EventId::parse_hex(&"c".repeat(64)).expect("event id"),
            ask_id: Some("ask-id".to_owned()),
            reply_to_event_id: EventId::parse_hex(&"a".repeat(64)),
            state,
        }
    }

    fn output(success: bool, stdout: &[u8]) -> Output {
        Output {
            success,
            status: if success {
                "exit status: 0"
            } else {
                "exit status: 1"
            }
            .to_owned(),
            stdout: stdout.to_vec(),
            stderr: if success {
                Vec::new()
            } else {
                b"failed".to_vec()
            },
        }
    }

    fn active_ask() -> Output {
        output(
            true,
            br#"{"id":"request","result":{"state":"in_progress"}}"#,
        )
    }

    #[test]
    fn publishes_stamped_body_then_marks_posted_and_resolves_final() {
        let mut repository = FakeRepository {
            turn: turn(TurnState::Open),
            marked_posted: false,
        };
        let mut runner = FakeRunner {
            outputs: VecDeque::from([
                active_ask(),
                output(
                    true,
                    br#"{"event_id":"published","accepted":true,"message":""}"#,
                ),
                output(true, b"reply accepted"),
            ]),
            calls: Vec::new(),
        };

        let receipt = publish(
            &send_args(),
            b"hello $(world)",
            Some(&mut repository),
            &mut runner,
        )
        .expect("publish succeeds");

        assert_eq!(
            receipt,
            serde_json::json!({"event_id": "published", "ask_id": "ask-id"})
        );
        assert!(repository.marked_posted);
        assert_eq!(runner.calls.len(), 3);
        assert_eq!(runner.calls[1].0, "envchain");
        assert_eq!(
            runner.calls[1].1,
            vec![
                "botserver",
                "buzz",
                "messages",
                "send",
                "--channel",
                "ab12cd34-5678-90ab-cdef-0123456789ab",
                "--content",
                "-",
                "--reply-to",
                &"a".repeat(64),
                "--mention",
                &"b".repeat(64),
            ]
        );
        assert_eq!(runner.calls[1].2, b"[bot]: hello $(world)");
        assert_eq!(
            runner.calls[2],
            (
                "kelpie",
                vec![
                    "reply".to_owned(),
                    "ask-id".to_owned(),
                    "--final".to_owned(),
                    "--stdin".to_owned(),
                ],
                b"published".to_vec(),
            )
        );
    }

    #[test]
    fn cancelled_ask_never_reaches_envchain() {
        let mut repository = FakeRepository {
            turn: turn(TurnState::Open),
            marked_posted: false,
        };
        let mut runner = FakeRunner {
            outputs: VecDeque::from([output(
                true,
                br#"{"id":"request","result":{"state":"cancelled"}}"#,
            )]),
            calls: Vec::new(),
        };

        let error = publish(&send_args(), b"stale", Some(&mut repository), &mut runner)
            .expect_err("cancelled ask is rejected");

        assert!(matches!(error, BotcliError::AskNotActive(state) if state == "cancelled"));
        assert_eq!(runner.calls.len(), 1);
        assert!(!repository.marked_posted);
    }

    #[test]
    fn failed_publish_keeps_turn_open_and_does_not_reply() {
        let mut repository = FakeRepository {
            turn: turn(TurnState::Open),
            marked_posted: false,
        };
        let mut runner = FakeRunner {
            outputs: VecDeque::from([active_ask(), output(false, b"")]),
            calls: Vec::new(),
        };

        let error = publish(
            &send_args(),
            b"retry later",
            Some(&mut repository),
            &mut runner,
        )
        .expect_err("publish failure is returned");

        assert!(matches!(
            error,
            BotcliError::CommandFailed {
                program: "envchain",
                ..
            }
        ));
        assert_eq!(runner.calls.len(), 2);
        assert_eq!(repository.turn.state, TurnState::Open);
        assert!(!repository.marked_posted);
    }

    #[test]
    fn unaccepted_publish_receipt_keeps_turn_open() {
        let mut repository = FakeRepository {
            turn: turn(TurnState::Open),
            marked_posted: false,
        };
        let mut runner = FakeRunner {
            outputs: VecDeque::from([
                active_ask(),
                output(
                    true,
                    br#"{"event_id":"rejected","accepted":false,"message":"rejected"}"#,
                ),
            ]),
            calls: Vec::new(),
        };

        let error = publish(
            &send_args(),
            b"not accepted",
            Some(&mut repository),
            &mut runner,
        )
        .expect_err("an unaccepted receipt is not success");

        assert!(matches!(error, BotcliError::InvalidPublishReceipt(_)));
        assert_eq!(repository.turn.state, TurnState::Open);
        assert_eq!(runner.calls.len(), 2);
    }

    #[test]
    fn failed_final_keeps_posted_state_to_prevent_duplicate_send() {
        let mut repository = FakeRepository {
            turn: turn(TurnState::Open),
            marked_posted: false,
        };
        let mut runner = FakeRunner {
            outputs: VecDeque::from([
                active_ask(),
                output(
                    true,
                    br#"{"event_id":"published","accepted":true,"message":""}"#,
                ),
                output(false, b""),
            ]),
            calls: Vec::new(),
        };

        let error = publish(
            &send_args(),
            b"published once",
            Some(&mut repository),
            &mut runner,
        )
        .expect_err("final delivery failure is returned");

        assert!(matches!(
            error,
            BotcliError::CommandFailed {
                program: "kelpie",
                ..
            }
        ));
        assert_eq!(repository.turn.state, TurnState::Posted);
    }

    #[test]
    fn posted_turn_cannot_publish_twice() {
        let mut repository = FakeRepository {
            turn: turn(TurnState::Posted),
            marked_posted: false,
        };
        let mut runner = FakeRunner {
            outputs: VecDeque::new(),
            calls: Vec::new(),
        };

        let error = publish(
            &send_args(),
            b"duplicate",
            Some(&mut repository),
            &mut runner,
        )
        .expect_err("posted turn is terminal");

        assert!(matches!(error, BotcliError::TurnNotOpen(TurnState::Posted)));
        assert!(runner.calls.is_empty());
    }

    #[test]
    fn mismatched_coordinates_are_rejected_before_external_calls() {
        let mut request = send_args();
        request.reply_to = Some("d".repeat(64));
        let mut repository = FakeRepository {
            turn: turn(TurnState::Open),
            marked_posted: false,
        };
        let mut runner = FakeRunner {
            outputs: VecDeque::new(),
            calls: Vec::new(),
        };

        let error = publish(
            &request,
            b"wrong thread",
            Some(&mut repository),
            &mut runner,
        )
        .expect_err("coordinates do not match");

        assert!(matches!(error, BotcliError::CoordinatesMismatch));
        assert!(runner.calls.is_empty());
    }

    #[test]
    fn send_without_ask_publishes_without_kelpie_or_database() {
        let mut arguments = send_args();
        arguments.ask_id = None;
        arguments.database = None;
        let mut runner = FakeRunner {
            outputs: VecDeque::from([output(
                true,
                br#"{"event_id":"published","accepted":true,"message":""}"#,
            )]),
            calls: Vec::new(),
        };

        let receipt = publish(
            &arguments,
            b"announcement",
            None::<&mut FakeRepository>,
            &mut runner,
        )
        .expect("plain send succeeds");

        assert_eq!(receipt, serde_json::json!({"event_id": "published"}));
        assert_eq!(runner.calls.len(), 1);
        assert_eq!(runner.calls[0].0, "envchain");
    }

    #[test]
    fn parser_requires_send_and_exactly_one_body_source() {
        assert!(Cli::try_parse_from([
            "botcli",
            "send",
            "--stdin",
            "--channel",
            "ab12cd34-5678-90ab-cdef-0123456789ab",
            "--envchain",
            "botserver",
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "botcli",
            "send",
            "--channel",
            "ab12cd34-5678-90ab-cdef-0123456789ab",
            "--envchain",
            "botserver",
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "botcli",
            "send",
            "--stdin",
            "--file",
            "message.md",
            "--channel",
            "ab12cd34-5678-90ab-cdef-0123456789ab",
            "--envchain",
            "botserver",
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "botcli",
            "send",
            "--stdin",
            "--ask-id",
            "ask-id",
            "--channel",
            "ab12cd34-5678-90ab-cdef-0123456789ab",
            "--envchain",
            "botserver",
        ])
        .is_err());
    }
}
