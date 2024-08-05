use clap::{arg, ArgMatches, Command};
use xline_client::{error::Result, Client};

use crate::utils::printer::Printer;

/// Temporary struct for testing, indicates `(user_name, role)`
type AuthUserGrantRoleRequest = (String, String);

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
    (name.into(), role.into())
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.auth_client().user_grant_role(req.0, req.1).await?;
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
            Some(("JohnDoe".into(), "Admin".into())),
        )];

        for case in test_cases {
            case.run_test();
        }
    }
}
