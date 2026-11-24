//! A bouncer that cannot start says why and fails, so that whatever
//! started it knows.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::process::Command;

fn start(name: &str, config: &str) -> (bool, String) {
    let path = std::env::temp_dir().join(format!("rfbouncer-{name}-{}.toml", std::process::id()));
    std::fs::write(&path, config).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_rfbouncer"))
        .arg("--config")
        .arg(&path)
        .output()
        .unwrap();
    let _ = std::fs::remove_file(&path);
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn a_port_that_is_taken_is_a_failure_with_its_reason() {
    let port = support::free_port();
    let _taken = support::listen(port).unwrap();
    let config = format!(
        "listen = \"127.0.0.1:{port}\"\n[[pool]]\nname = \"main\"\nbackends = [\"127.0.0.1:1\"]\n"
    );
    let (ok, said) = start("taken", &config);
    assert!(!ok, "it started on a port that was taken");
    assert!(said.to_lowercase().contains("in use"), "{said}");
}

#[test]
fn a_configuration_that_does_not_parse_is_a_failure_with_its_reason() {
    let (ok, said) = start("empty", "listen = \"127.0.0.1:1\"\npool = []\n");
    assert!(!ok);
    assert!(said.contains("at least one [[pool]]"), "{said}");
}
