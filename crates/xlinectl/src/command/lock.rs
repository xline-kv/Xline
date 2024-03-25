use clap::{arg, ArgMatches, Command};
use tokio::signal::ctrl_c;
use xline_client::{
    error::Result,
    types::{
        lease::{LeaseGrantRequest, LeaseKeepAliveRequest},
        lock::{LockRequest, UnlockRequest, DEFAULT_SESSION_TTL},
    },
    Client,
};

use crate::{lease::keep_alive::keep_alive_loop, utils::printer::Printer};

/// Definition of `lock` command
pub(crate) fn command() -> Command {
    Command::new("lock")
        .about("Acquire a lock, which will return a unique key that exists so long as the lock is held")
        .arg(arg!(<lockname> "name of the lock"))
}

/// Build request from matches
pub(crate) fn build_request(matches: &ArgMatches) -> LockRequest {
    let name = matches.get_one::<String>("lockname").expect("required");
    LockRequest::new(name.as_bytes())
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let lease_client = client.lease_client();
    let lease_resp = lease_client
        .grant(LeaseGrantRequest::new(DEFAULT_SESSION_TTL))
        .await?;
    let lock_lease_id = lease_resp.id;
    let req = build_request(matches).with_lease(lock_lease_id);
    let lock_resp = client.lock_client().lock(req).await?;

    lock_resp.print();

    let (keeper, stream) = client
        .lease_client()
        .keep_alive(LeaseKeepAliveRequest::new(lock_lease_id))
        .await?;

    tokio::select! {
        _ = ctrl_c() => {
            println!("releasing the lock");
            let unlock_req = UnlockRequest::new(lock_resp.key);
            let _unlock_resp = client.lock_client().unlock(unlock_req).await?;
            Ok(())
        }
        result = keep_alive_loop(keeper, stream, false) => {
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(LockRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![TestCase::new(
            vec!["lock", "my_lock"],
            Some(LockRequest::new("my_lock")),
        )];

        for case in test_cases {
            case.run_test();
        }
    }
}
