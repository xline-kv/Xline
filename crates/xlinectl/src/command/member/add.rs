use clap::{arg, ArgMatches, Command};
use xline_client::{error::Result, Client};

use super::parse_peer_urls;
use crate::utils::printer::Printer;

/// Temp type for cluster member `add` command, indicates `(peer_urls, is_learner)`
type MemberAddRequest = (Vec<String>, bool);

/// Definition of `add` command
pub(super) fn command() -> Command {
    Command::new("add")
        .about("Adds a member into the cluster")
        .arg(
            arg!(<peer_urls> "Comma separated peer URLs for the new member.")
                .value_parser(parse_peer_urls),
        )
        .arg(arg!(--is_learner "Add as learner"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> MemberAddRequest {
    let peer_urls = matches
        .get_one::<Vec<String>>("peer_urls")
        .expect("required");
    let is_learner = matches.get_flag("is_learner");

    (peer_urls.clone(), is_learner)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let request = build_request(matches);
    let resp = client
        .cluster_client()
        .member_add(request.0, request.1)
        .await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(MemberAddRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(
                vec!["add", "127.0.0.1:2379", "--is_learner"],
                Some((["127.0.0.1:2379".to_owned()].into(), true)),
            ),
            TestCase::new(
                vec!["add", "127.0.0.1:2379,127.0.0.1:2380"],
                Some((
                    ["127.0.0.1:2379".to_owned(), "127.0.0.1:2380".to_owned()].into(),
                    false,
                )),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
