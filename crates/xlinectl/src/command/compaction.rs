use anyhow::Result;
use clap::{arg, value_parser, ArgMatches, Command};
use xline_client::Client;

use crate::utils::printer::Printer;

/// Temp type for build a compaction request, indicates `(revision, physical)`
type CompactionRequest = (i64, bool);

/// Definition of `compaction` command
pub(crate) fn command() -> Command {
    Command::new("compaction")
        .about("Discards all Xline event history prior to a given revision")
        .arg(arg!(<revision> "The revision to compact").value_parser(value_parser!(i64)))
        .arg(arg!(--physical "To wait for compaction to physically remove all old revisions"))
}

/// Build request from matches
pub(crate) fn build_request(matches: &ArgMatches) -> CompactionRequest {
    let revision = matches.get_one::<i64>("revision").expect("required");
    let physical = matches.get_flag("physical");

    (*revision, physical)
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.kv_client().compact(req.0, req.1).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(CompactionRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(vec!["compaction", "123"], Some((123, false))),
            TestCase::new(vec!["compaction", "123", "--physical"], Some((123, true))),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
