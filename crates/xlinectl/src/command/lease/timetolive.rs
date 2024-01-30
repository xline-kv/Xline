use clap::{arg, value_parser, ArgMatches, Command};
use xline_client::{error::Result, types::lease::LeaseTimeToLiveRequest, Client};

use crate::utils::printer::Printer;

/// Definition of `timetolive` command
pub(super) fn command() -> Command {
    Command::new("timetolive")
        .about("Get lease ttl information")
        .arg(arg!(<leaseId> "Lease id to get").value_parser(value_parser!(i64)))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> LeaseTimeToLiveRequest {
    let lease_id = matches.get_one::<i64>("leaseId").expect("required");
    LeaseTimeToLiveRequest::new(*lease_id)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.lease_client().time_to_live(req).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(LeaseTimeToLiveRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![TestCase::new(
            vec!["timetolive", "123"],
            Some(LeaseTimeToLiveRequest::new(123)),
        )];

        for case in test_cases {
            case.run_test();
        }
    }
}
