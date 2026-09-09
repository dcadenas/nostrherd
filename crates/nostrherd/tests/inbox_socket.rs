//! Process-isolated socket environment regression coverage.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nostrherd::inbox::{default_socket, parse_delivery, InboxConn};
use serde_json::{json, Value};

#[test]
fn fallback_socket_receives_and_acks_a_reply() {
    socket_roundtrip(false);
}

#[test]
#[ignore = "requires an isolated tools/local-relay and throwaway credentials"]
fn live_fallback_socket_publishes_before_ack() {
    socket_roundtrip(true);
}

fn socket_roundtrip(publish: bool) {
    if std::env::var_os("NOSTRHERD_SOCKET_TEST_CHILD").is_some() {
        let mut conn = InboxConn::claim(&default_socket(), "synthetic-waiter").expect("claim");
        let delivery = parse_delivery(&conn.read_event().expect("event")).expect("delivery");
        assert_eq!(delivery.body(), "synthetic final");
        assert_eq!(delivery.reply_to(), Some("synthetic-ask"));
        if publish {
            publish_live_final(delivery.body());
        }
        conn.ack(delivery.message_id()).expect("ack");
        return;
    }

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("nh83-{nonce}"));
    let socket_dir = root.join("kelpie-client/kelpie");
    std::fs::create_dir_all(&socket_dir).unwrap();
    let listener = UnixListener::bind(socket_dir.join("kelpie.sock")).unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            if publish {
                "live_fallback_socket_publishes_before_ack"
            } else {
                "fallback_socket_receives_and_acks_a_reply"
            },
            "--nocapture",
            "--include-ignored",
        ])
        .env_remove("KELPIE_SOCKET")
        .env_remove("XDG_RUNTIME_DIR")
        .env("TMPDIR", &root)
        .env("NOSTRHERD_SOCKET_TEST_CHILD", "1")
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let (mut stream, _) = loop {
        match listener.accept() {
            Ok(pair) => break pair,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if child.try_wait().unwrap().is_some() || std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    std::fs::remove_dir_all(&root).unwrap();
                    panic!("child did not connect to the temporary-directory fallback");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("accept: {error}"),
        }
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(if publish { 40 } else { 5 })))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let claim: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(claim["method"], "inbox.claim");
    writeln!(
        stream,
        "{}",
        json!({"id": "claim", "result": {"claimed": true}})
    )
    .unwrap();
    writeln!(
        stream,
        "{}",
        json!({"method": "inbox.delivery", "params": {
            "message_id": "synthetic-reply", "kind": "reply", "disposition": "final",
            "reply_to": "synthetic-ask", "body": "synthetic final"
        }})
    )
    .unwrap();
    line.clear();
    reader.read_line(&mut line).unwrap();
    let ack: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(ack["method"], "inbox.ack");
    assert_eq!(ack["params"]["message_id"], "synthetic-reply");
    writeln!(
        stream,
        "{}",
        json!({"id": ack["id"], "result": {"outcome": "accepted"}})
    )
    .unwrap();
    assert!(child.wait().unwrap().success());
    std::fs::remove_dir_all(root).unwrap();
}

fn publish_live_final(body: &str) {
    use nostr_sdk::prelude::{Client, Filter, Keys, SignerAuthenticator};
    use nostrherd::outbox::{BuzzPublisher, OutboundAttempt, OutboundPublisher};
    use nostrherd_domain::{buzz, stamp_outbound, EventId};

    let url = std::env::var("NOSTRHERD_RELAY_URL").expect("local relay URL");
    assert!(
        url.starts_with("ws://127.0.0.1:"),
        "proof must stay on loopback"
    );
    let channel = std::env::var("BOTSERVER_LIVE_CHANNEL").expect("throwaway channel");
    let keys = Keys::parse(&std::env::var("NOSTRHERD_PRIVATE_KEY").expect("throwaway key"))
        .expect("valid throwaway key");
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let client = Client::builder()
            .authenticator(SignerAuthenticator::new(keys.clone()))
            .build();
        client.add_relay(&url).await.unwrap();
        client.connect().and_wait(Duration::from_secs(10)).await;
        let publisher = BuzzPublisher::new(client.clone(), keys, &url);
        let trigger = publisher
            .send_buzz(&buzz::channel_message(
                &channel,
                "bot: socket proof",
                &[],
                None,
            ))
            .await
            .unwrap();
        let mut attempt = OutboundAttempt::new(
            "synthetic-ask",
            stamp_outbound(body, "**[bot]**:"),
            &channel,
        );
        attempt.reply_to_event_id = Some(EventId::parse_hex(&trigger).unwrap());
        let prepared = publisher.prepare(&attempt).unwrap();
        let event_id = publisher.publish(&prepared).unwrap();
        let events = client
            .fetch_events(
                Filter::new().id(nostr_sdk::prelude::EventId::from_hex(&event_id).unwrap()),
            )
            .timeout(Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        let event = events.into_iter().next().unwrap();
        assert_eq!(event.content, "**[bot]**: synthetic final");
        assert!(event.tags.iter().any(|tag| tag
            .as_slice()
            .first()
            .is_some_and(|value| value == "e")
            && tag.as_slice().get(1) == Some(&trigger)));
        client.disconnect().await;
    });
}

#[test]
fn reconnect_failures_are_visible_without_retry_spam() {
    if std::env::var_os("NOSTRHERD_SOCKET_TEST_CHILD").is_some() {
        let inbox = nostrherd::inbox::spawn_inbox(default_socket(), "synthetic-waiter".into());
        std::thread::sleep(Duration::from_millis(1200));
        drop(inbox);
        return;
    }
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let absent = std::env::temp_dir().join(format!("nh83-absent-{nonce}.sock"));
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "reconnect_failures_are_visible_without_retry_spam",
            "--nocapture",
        ])
        .env("KELPIE_SOCKET", &absent)
        .env("NOSTRHERD_SOCKET_TEST_CHILD", "1")
        .output()
        .unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stderr.matches("inbox failed").count(), 1);
    assert!(stderr.contains("I/O NotFound"));
    assert!(stderr.contains("reconnecting every second"));
}

#[test]
fn outbound_cli_and_inbox_share_the_socket_override() {
    use std::os::unix::fs::PermissionsExt;

    if let Some(program) = std::env::var_os("NOSTRHERD_SOCKET_TEST_CLI") {
        let expected = std::env::var_os("NOSTRHERD_SOCKET_TEST_EXPECTED").unwrap();
        assert_eq!(default_socket(), std::path::PathBuf::from(expected));
        let client = nostrherd::KelpieClient::new(program);
        assert_eq!(
            client
                .register_waiter()
                .unwrap()
                .identity()
                .logical_agent_id(),
            "synthetic-waiter"
        );
        return;
    }
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("nh83-cli-{nonce}"));
    std::fs::create_dir(&root).unwrap();
    let program = root.join("kelpie-fixture");
    std::fs::write(&program, r#"#!/bin/sh
test "$1" = --socket && test "$2" = "$NOSTRHERD_SOCKET_TEST_EXPECTED" || exit 1
printf '%s\n' '{"result":{"logical_agent_id":"synthetic-waiter","delivery_transport":"socket_inbox"}}'
"#).unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "outbound_cli_and_inbox_share_the_socket_override",
            "--nocapture",
        ])
        .env("NOSTRHERD_SOCKET_TEST_CLI", &program)
        .env("NOSTRHERD_SOCKET_TEST_EXPECTED", root.join("override.sock"))
        .env("KELPIE_SOCKET", root.join("override.sock"))
        .env("XDG_RUNTIME_DIR", root.join("different-runtime"))
        .output()
        .unwrap();
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
