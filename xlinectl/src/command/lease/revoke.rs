use clap::{arg, value_parser, ArgMatches, Command};
use xline_client::{error::Result, types::lease::LeaseRevokeRequest, Client};

use crate::utils::printer::Printer;

/// Definition of `revoke` command
pub(super) fn command() -> Command {
    Command::new("revoke")
        .about("Revoke a lease")
        .arg(arg!(<leaseId> "Lease Id to revoke").value_parser(value_parser!(i64)))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> LeaseRevokeRequest {
    let lease_id = matches.get_one::<i64>("leaseId").expect("required");
    LeaseRevokeRequest::new(*lease_id)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.lease_client().revoke(req).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(LeaseRevokeRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![TestCase::new(
            vec!["revoke", "123"],
            Some(LeaseRevokeRequest::new(123)),
        )];

        for case in test_cases {
            case.run_test();
        }
    }
}
