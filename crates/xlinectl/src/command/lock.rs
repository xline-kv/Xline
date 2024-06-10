use clap::{arg, ArgMatches, Command};
use std::process::Command as StdCommand;
use tokio::signal;
use xline_client::{
    error::Result,
    types::lock::{LockRequest, UnlockRequest},
    Client,
};

use crate::utils::printer::Printer;

/// Definition of `lock` command
pub(crate) fn command() -> Command {
    Command::new("lock")
        .about("Acquire a lock, which will return a unique key that exists so long as the lock is held")
        .arg(arg!(<lockname> "name of the lock"))
        .arg(arg!(<exec_command> "command to execute").num_args(1..).required(false))
}

/// Build request from matches
pub(crate) fn build_request(matches: &ArgMatches) -> LockRequest {
    let name = matches.get_one::<String>("lockname").expect("required");
    LockRequest::new(name.as_bytes())
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);

    let resp = client.lock_client().lock(req).await?;

    resp.print();

    let exec_command = matches.get_many::<String>("exec_command");

    if let Some(exec_command) = exec_command {
        let exec_command_vec: Vec<&String> = exec_command.collect();
        if !exec_command_vec.is_empty() {
            let (command, args) = exec_command_vec
                .split_first()
                .expect("Expected at least one exec command");
            let output = StdCommand::new(command)
                .args(args)
                .output()
                .expect("failed to execute command");
            println!("{}", String::from_utf8_lossy(&output.stdout));
        }
    }

    signal::ctrl_c().await.expect("failed to listen for event");

    println!("releasing the lock");

    let unlock_req = UnlockRequest::new(resp.key);
    let _unlock_resp = client.lock_client().unlock(unlock_req).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(LockRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(vec!["lock", "my_lock"], Some(LockRequest::new("my_lock"))),
            TestCase::new(
                vec!["lock", "my_lock", "echo", "lock", "acquired"],
                Some(LockRequest::new("my_lock")),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
