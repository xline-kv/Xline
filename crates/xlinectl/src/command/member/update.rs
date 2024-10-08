use clap::{arg, value_parser, ArgMatches, Command};
use xline_client::{error::Result, Client};

use super::parse_peer_urls;
use crate::utils::printer::Printer;

/// Temp type for request and testing, indicates `(id, peer_urls)`
type MemberUpdateRequest = (u64, Vec<String>);

/// Definition of `update` command
pub(super) fn command() -> Command {
    Command::new("update")
        .about("Updates a member in the cluster")
        .arg(arg!(<ID> "The member ID").value_parser(value_parser!(u64)))
        .arg(
            arg!(<peer_urls> "Comma separated peer URLs for the new member.")
                .value_parser(parse_peer_urls),
        )
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> MemberUpdateRequest {
    let member_id = matches.get_one::<u64>("ID").expect("required");
    let peer_urls = matches
        .get_one::<Vec<String>>("peer_urls")
        .expect("required");

    (*member_id, peer_urls.clone())
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let request = build_request(matches);
    let resp = client
        .cluster_client()
        .member_update(request.0, request.1)
        .await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(MemberUpdateRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(
                vec!["update", "1", "127.0.0.1:2379"],
                Some((1, ["127.0.0.1:2379".to_owned()].into())),
            ),
            TestCase::new(
                vec!["update", "2", "127.0.0.1:2379,127.0.0.1:2380"],
                Some((
                    2,
                    ["127.0.0.1:2379".to_owned(), "127.0.0.1:2380".to_owned()].into(),
                )),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
