use clap::{arg, value_parser, ArgMatches, Command};
use xline_client::{error::Result, types::cluster::MemberPromoteRequest, Client};

use crate::utils::printer::Printer;

/// Definition of `promote` command
pub(super) fn command() -> Command {
    Command::new("promote")
        .about("Promotes a member in the cluster")
        .arg(arg!(<ID> "The member ID").value_parser(value_parser!(u64)))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> MemberPromoteRequest {
    let member_id = matches.get_one::<u64>("ID").expect("required");

    MemberPromoteRequest::new(*member_id)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let request = build_request(matches);
    let resp = client.cluster_client().member_promote(request).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(MemberPromoteRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![TestCase::new(
            vec!["remove", "1"],
            Some(MemberPromoteRequest::new(1)),
        )];

        for case in test_cases {
            case.run_test();
        }
    }
}
