use std::process::{Command, Stdio};

#[test]
fn send_without_buzz_private_key_exits_nonzero() {
    let output = Command::new(env!("CARGO_BIN_EXE_botcli"))
        .args([
            "send",
            "--stdin",
            "--channel",
            "ab12cd34-5678-90ab-cdef-0123456789ab",
        ])
        .env_remove("BUZZ_PRIVATE_KEY")
        .env("BUZZ_RELAY_URL", "ws://127.0.0.1:13001")
        .stdin(Stdio::null())
        .output()
        .expect("run botcli");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("missing BUZZ_PRIVATE_KEY"), "{stderr}");
    assert!(!stderr.contains("nsec"));
}

#[test]
fn send_without_buzz_relay_url_exits_nonzero() {
    let secret = "nsec1throwawaybotserveris26inprocess";
    let output = Command::new(env!("CARGO_BIN_EXE_botcli"))
        .args([
            "send",
            "--stdin",
            "--channel",
            "ab12cd34-5678-90ab-cdef-0123456789ab",
        ])
        .env("BUZZ_PRIVATE_KEY", secret)
        .env_remove("BUZZ_RELAY_URL")
        .stdin(Stdio::null())
        .output()
        .expect("run botcli");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("missing BUZZ_RELAY_URL"), "{stderr}");
    assert!(!stderr.contains(secret));
}
