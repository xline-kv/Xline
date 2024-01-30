use assert_cmd::Command;

#[test]
fn should_fail() {
    let mut cmd = Command::cargo_bin("with_abort").unwrap();
    cmd.assert().failure();
}

#[test]
fn should_success() {
    let mut cmd = Command::cargo_bin("no_abort").unwrap();
    cmd.assert().success();
}
