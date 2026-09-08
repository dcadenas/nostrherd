//! Real isolated Herdr/Kelpie/relay proof. Never run against operator services.
//! Usage: `dispatch_proof CASE_DIRECTORY HOST_BINARY [EXPECTED_IDENTITIES]`
//! Requires a fresh isolated runtime and `groups_relay` at 127.0.0.1:18082.

use std::error::Error;
use std::fs::{self, File};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nostr_sdk::prelude::*;
use serde_json::Value;

struct Host(Child);

impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn Error>> {
    if std::env::var("NOSTRHERD_DISPATCH_PROOF").as_deref() != Ok("isolated") {
        return Err("requires an explicitly isolated Herdr/Kelpie/relay fixture".into());
    }
    let mut args = std::env::args().skip(1);
    let directory = PathBuf::from(args.next().ok_or("case directory required")?);
    let binary = PathBuf::from(args.next().ok_or("host binary required")?);
    let expected: Option<usize> = args.next().map(|arg| arg.parse()).transpose()?;
    if directory.exists() {
        return Err("case directory already exists; refusing to reuse state".into());
    }
    let corpus = directory.join("corpus");
    fs::create_dir_all(&corpus)?;
    for file in ["AGENTS.md", "startup.md", "README.md", ".gitignore"] {
        fs::copy(
            PathBuf::from("/src/corpus/template-bot").join(file),
            corpus.join(file),
        )?;
    }
    fs::write(
        directory.join("bots.toml"),
        toml::to_string(
            &serde_json::json!({"bots": [{"id": "bot", "corpus": corpus, "kind": "pi"}]}),
        )?,
    )?;

    // The key exists only in this process and the host child environment.
    // Herdr and its occupants were started separately and never inherit it.
    let keys = Keys::generate();
    let channel = format!("is82-{}", std::process::id());
    let client = Client::builder()
        .authenticator(SignerAuthenticator::new(keys.clone()))
        .build();
    client.add_relay("ws://127.0.0.1:18082").await?;
    client.connect().and_wait(Duration::from_secs(10)).await;
    client
        .send_event(
            &EventBuilder::new(Kind::Custom(9007), "")
                .tag(Tag::parse(["h", &channel])?)
                .finalize(&keys)?,
        )
        .await?;

    let log = File::create(directory.join("host.log"))?;
    let mut command = Command::new(&binary);
    command
        .args([
            "--config",
            directory.join("bots.toml").to_str().ok_or("config path")?,
            "--database",
            directory
                .join("host.sqlite")
                .to_str()
                .ok_or("database path")?,
        ])
        .env("NOSTRHERD_PRIVATE_KEY", keys.secret_key().to_secret_hex())
        .env("NOSTRHERD_RELAY_URL", "ws://127.0.0.1:18082")
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log));
    if let Ok(path) = std::env::var("NOSTRHERD_PROOF_KELPIE_DIR") {
        command.env("PATH", format!("{path}:{}", std::env::var("PATH")?));
    }
    let mut host = Host(command.spawn()?);
    tokio::time::sleep(Duration::from_secs(5)).await;
    if host.0.try_wait()?.is_some() {
        return Err("host exited before trigger; see case host.log".into());
    }
    let started = Instant::now();
    let trigger = EventBuilder::new(
        Kind::Custom(9),
        "bot: hello, reply with the single word pong",
    )
    .tag(Tag::parse(["h", &channel])?)
    .finalize(&keys)?;
    client.send_event(&trigger).await?;
    let filter = Filter::new()
        .kind(Kind::Custom(9))
        .custom_tag(SingleLetterTag::from_char('h')?, channel.clone());
    let mut posted = false;
    while started.elapsed() < Duration::from_secs(300) {
        if host.0.try_wait()?.is_some() {
            return Err("host exited while waiting for final".into());
        }
        let events = client
            .fetch_events(filter.clone())
            .timeout(Duration::from_secs(3))
            .await?;
        let finals: Vec<_> = events
            .iter()
            .filter(|event| event.content.starts_with("[bot]:"))
            .collect();
        if !finals.is_empty() {
            if finals.len() != 1 || !finals[0].content.to_lowercase().contains("pong") {
                return Err("expected one stamped pong".into());
            }
            if !finals[0].tags.iter().any(|tag| {
                tag.as_slice().first().is_some_and(|name| name == "e")
                    && tag
                        .as_slice()
                        .get(1)
                        .is_some_and(|value| value == &trigger.id.to_hex())
            }) {
                return Err("final did not reference the trigger".into());
            }
            posted = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let report = Command::new("/work/bin/kelpie")
        .args(["--json", "report"])
        .output()?;
    if !report.status.success() {
        return Err("Kelpie report failed".into());
    }
    let report: Value = serde_json::from_slice(&report.stdout)?;
    fs::write(
        directory.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    let agents = report["result"]["agents"]
        .as_array()
        .ok_or("missing agents")?;
    let connection = rusqlite::Connection::open_with_flags(
        directory.join("host.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let session: String = connection.query_row(
        "SELECT session_name FROM sessions WHERE channel_id = ?1",
        [&channel],
        |row| row.get(0),
    )?;
    let identities = agents
        .iter()
        .filter(|agent| {
            agent["public_name"]
                .as_str()
                .is_some_and(|name| name == session)
        })
        .count();
    println!(
        "posted={posted} logical_identities={identities} elapsed_seconds={}",
        started.elapsed().as_secs()
    );
    if !posted || expected.is_some_and(|expected| expected != identities) {
        return Err("dispatch proof failed; case receipts retained".into());
    }
    client.disconnect().await;
    Ok(())
}
