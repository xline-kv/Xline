use clap::{arg, ArgMatches, Command};
use xline_client::{error::Result, Client};

use crate::utils::printer::Printer;

/// Definition of `list` command
pub(super) fn command() -> Command {
    Command::new("list")
        .about("Lists all members in the cluster")
        .arg(arg!(--linearizable "To use linearizable fetch"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> bool {
    matches.get_flag("linearizable")
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let request = build_request(matches);
    let resp = client.cluster_client().member_list(request).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(bool);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(vec!["list", "--linearizable"], Some(true)),
            TestCase::new(vec!["list"], Some(false)),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
