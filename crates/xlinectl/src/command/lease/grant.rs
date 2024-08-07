use clap::{arg, value_parser, ArgMatches, Command};
use xline_client::{error::Result, Client};

use crate::utils::printer::Printer;

/// Definition of `grant` command
pub(super) fn command() -> Command {
    Command::new("grant")
        .about("Create a lease with a given TTL")
        .arg(arg!(<ttl> "time to live of the lease").value_parser(value_parser!(i64)))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> i64 {
    let ttl = matches.get_one::<i64>("ttl").expect("required");
    *ttl
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let ttl = build_request(matches);
    let resp = client.lease_client().grant(ttl, None).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(i64);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![TestCase::new(vec!["grant", "100"], Some(100))];

        for case in test_cases {
            case.run_test();
        }
    }
}
