use clap::{arg, ArgMatches, Command};
use xline_client::{error::Result, types::auth::AuthUserGrantRoleRequest, Client};

use crate::utils::printer::Printer;

/// Definition of `grant_role` command
pub(super) fn command() -> Command {
    Command::new("grant_role")
        .about("Grant role to a user")
        .arg(arg!(<name> "The name of the user"))
        .arg(arg!(<role> "The name of the role"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthUserGrantRoleRequest {
    let name = matches.get_one::<String>("name").expect("required");
    let role = matches.get_one::<String>("role").expect("required");
    AuthUserGrantRoleRequest::new(name, role)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.auth_client().user_grant_role(req).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(AuthUserGrantRoleRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![TestCase::new(
            vec!["grant_role", "JohnDoe", "Admin"],
            Some(AuthUserGrantRoleRequest::new("JohnDoe", "Admin")),
        )];

        for case in test_cases {
            case.run_test();
        }
    }
}
