use clap::{arg, ArgMatches, Command};
use xline_client::{error::Result, types::auth::AuthRoleDeleteRequest, Client};

use crate::utils::printer::Printer;

/// Definition of `delete` command
pub(super) fn command() -> Command {
    Command::new("delete")
        .about("delete a role")
        .arg(arg!(<name> "The name of the role"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthRoleDeleteRequest {
    let name = matches.get_one::<String>("name").expect("required");
    AuthRoleDeleteRequest::new(name)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.auth_client().role_delete(req).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(AuthRoleDeleteRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![TestCase::new(
            vec!["delete", "Admin"],
            Some(AuthRoleDeleteRequest::new("Admin")),
        )];

        for case in test_cases {
            case.run_test();
        }
    }
}
