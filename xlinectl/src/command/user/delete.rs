use clap::{arg, ArgMatches, Command};
use xline_client::{error::Result, types::auth::AuthUserDeleteRequest, Client};

use crate::utils::printer::Printer;

/// Definition of `delete` command
pub(super) fn command() -> Command {
    Command::new("delete")
        .about("Delete a user")
        .arg(arg!(<name> "The name of the user"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthUserDeleteRequest {
    let name = matches.get_one::<String>("name").expect("required");
    AuthUserDeleteRequest::new(name)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.auth_client().user_delete(req).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(AuthUserDeleteRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![TestCase::new(
            vec!["delete", "JohnDoe"],
            Some(AuthUserDeleteRequest::new("JohnDoe")),
        )];

        for case in test_cases {
            case.run_test();
        }
    }
}
