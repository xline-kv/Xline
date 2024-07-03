use clap::{arg, ArgMatches, Command};
use std::process::Command as StdCommand;
use tokio::signal;
use xline_client::{clients::Xutex, error::Result, Client};

/// Definition of `lock` command
pub(crate) fn command() -> Command {
    Command::new("lock")
        .about("Acquire a lock, which will return a unique key that exists so long as the lock is held")
        .arg(arg!(<lockname> "name of the lock"))
        .arg(arg!(<exec_command> "command to execute").num_args(1..).required(false))
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let prefix = matches.get_one::<String>("lockname").expect("required");
    let mut xutex = Xutex::new(client.lock_client(), prefix, None, None).await?;

    let xutex_guard = xutex.lock_unsafe().await?;

    let exec_command = matches.get_many::<String>("exec_command");
    let mut should_wait_for_ctrl_c = true;

    if let Some(exec_command) = exec_command {
        let exec_command_vec: Vec<&String> = exec_command.collect();
        if !exec_command_vec.is_empty() {
            let output = execute_exec_command(&exec_command_vec);
            println!("{output}");
            should_wait_for_ctrl_c = false;
        }
    }
    if should_wait_for_ctrl_c {
        println!("{}", xutex_guard.key());
        signal::ctrl_c().await.expect("failed to listen for event");
        println!("releasing the lock, ");
    }
    // let res = lock_resp.unlock().await;
    Ok(())
}

/// Execute an exec command
fn execute_exec_command(command_and_args: &[&String]) -> String {
    let (command, args) = command_and_args
        .split_first()
        .expect("Expected at least one exec command");
    let output = StdCommand::new(command)
        .args(args)
        .output()
        .expect("failed to execute command");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestCase {
        exec_command: Vec<&'static str>,
        expected_output: String,
    }

    impl TestCase {
        fn new(exec_command: Vec<&'static str>, expected_output: String) -> TestCase {
            TestCase {
                exec_command,
                expected_output,
            }
        }

        fn run_test(&self) {
            let exec_command_owned: Vec<String> =
                self.exec_command.iter().map(ToString::to_string).collect();
            let exec_command_refs: Vec<&String> = exec_command_owned.iter().collect();
            let output = execute_exec_command(&exec_command_refs);

            assert_eq!(
                output.trim(),
                self.expected_output.trim(),
                "Failed executing {:?}. Expected output: {}, got: {}",
                self.exec_command,
                self.expected_output,
                output
            );
        }
    }

    #[test]
    fn test_execute_exec_command() {
        let test_cases = vec![
            TestCase::new(vec!["echo", "success"], "success".to_owned()),
            TestCase::new(vec!["echo", "fail"], "fail".to_owned()),
            TestCase::new(vec!["echo", "lock acquired"], "lock acquired".to_owned()),
            TestCase::new(vec!["echo", "-n"], String::new()),
        ];

        for test_case in test_cases {
            test_case.run_test();
        }
    }
}
