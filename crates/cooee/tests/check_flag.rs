use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_TEMP_PATH: AtomicU64 = AtomicU64::new(0);

fn temp_path(label: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let sequence = NEXT_TEMP_PATH.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "cooee-check-{label}-{}-{timestamp}-{sequence}",
        std::process::id()
    ))
}

#[test]
fn check_exits_zero_after_loading_config_and_database() {
    let config = temp_path("config").with_extension("toml");
    fs::write(
        &config,
        r#"
        [[bots]]
        id = "bot"
        corpus = "/corpus/bot"
        kind = "opencode"
        "#,
    )
    .expect("write config");
    let database = temp_path("host").with_extension("sqlite");

    let output = Command::new(env!("CARGO_BIN_EXE_cooee"))
        .args([
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
            "--check",
        ])
        .output()
        .expect("run cooee");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(database.is_file());
}

#[test]
fn check_exits_nonzero_when_config_is_missing() {
    let config = temp_path("missing").with_extension("toml");
    let database = temp_path("host").with_extension("sqlite");

    let output = Command::new(env!("CARGO_BIN_EXE_cooee"))
        .args([
            "--config",
            config.to_str().expect("utf8"),
            "--database",
            database.to_str().expect("utf8"),
            "--check",
        ])
        .output()
        .expect("run cooee");

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("failed to read bot config"));
}
