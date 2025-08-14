use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn prints_help() {
    let mut cmd = Command::cargo_bin("dynamate").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage:"));
}

#[test]
fn bad_argument_fails() {
    let mut cmd = Command::cargo_bin("dynamate").unwrap();
    cmd.arg("--does-not-exist")
        .assert()
        .failure()
        .stderr(predicate::str::contains("error:"));
}
