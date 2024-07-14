use clap::{arg, ArgMatches, Command};
use xline_client::{error::Result, Client};

use crate::utils::printer::Printer;

/// Temporary request for changing password. 0 is name, 1 is password
type AuthUserChangePasswordRequest = (String, String);

/// Definition of `passwd` command
// TODO: interactive mode
pub(super) fn command() -> Command {
    Command::new("passwd")
        .about("Change the password of a user")
        .arg(arg!(<name> "The name of the user"))
        .arg(arg!(<password> "Password to change"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthUserChangePasswordRequest {
    let name = matches.get_one::<String>("name").expect("required");
    let password = matches.get_one::<String>("password").expect("required");
    (name.to_owned(), password.to_owned())
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client
        .auth_client()
        .user_change_password(req.0, req.1)
        .await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(AuthUserChangePasswordRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![TestCase::new(
            vec!["passwd", "JohnDoe", "new_password"],
            Some(("JohnDoe".into(), "new_password".into())),
        )];

        for case in test_cases {
            case.run_test();
        }
    }
}
