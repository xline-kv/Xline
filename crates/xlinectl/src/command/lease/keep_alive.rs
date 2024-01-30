use std::time::Duration;

use clap::{arg, value_parser, ArgMatches, Command};
use tokio::signal::ctrl_c;
use tonic::Streaming;
use xline_client::{
    error::{Result, XlineClientError},
    types::lease::{LeaseKeepAliveRequest, LeaseKeeper},
    Client,
};
use xlineapi::LeaseKeepAliveResponse;

use crate::utils::printer::Printer;

/// Definition of `keep_alive` command
pub(super) fn command() -> Command {
    Command::new("keep_alive")
        .about("Lease keep alive periodically")
        .arg(arg!(<leaseId> "Lease Id to keep alive").value_parser(value_parser!(i64)))
        .arg(arg!(--once "keep alive once"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> LeaseKeepAliveRequest {
    let lease_id = matches.get_one::<i64>("leaseId").expect("required");
    LeaseKeepAliveRequest::new(*lease_id)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let once = matches.get_flag("once");

    let req = build_request(matches);

    let (mut keeper, mut stream) = client.lease_client().keep_alive(req).await?;

    #[allow(clippy::arithmetic_side_effects)] // introduced by tokio::select
    if once {
        keeper.keep_alive()?;
        if let Some(resp) = stream.message().await? {
            resp.print();
        }
    } else {
        tokio::select! {
            _ = ctrl_c() => {}
            result = keep_alive_loop(keeper, stream) => {
                return result;
            }
        }
    }

    Ok(())
}

/// keep alive forever unless encounter error
async fn keep_alive_loop(
    mut keeper: LeaseKeeper,
    mut stream: Streaming<LeaseKeepAliveResponse>,
) -> Result<()> {
    loop {
        keeper.keep_alive()?;
        if let Some(resp) = stream.message().await? {
            resp.print();
            if resp.ttl < 0 {
                return Err(XlineClientError::InvalidArgs(String::from(
                    "lease keepalive response has negative ttl",
                )));
            }
            tokio::time::sleep(Duration::from_secs(resp.ttl.unsigned_abs() / 3)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(LeaseKeepAliveRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(
                vec!["keep_alive", "123"],
                Some(LeaseKeepAliveRequest::new(123)),
            ),
            TestCase::new(
                vec!["keep_alive", "456", "--once"],
                Some(LeaseKeepAliveRequest::new(456)),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
