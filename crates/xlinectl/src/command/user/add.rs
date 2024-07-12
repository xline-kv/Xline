use crate::utils::printer::Printer;
use clap::{arg, ArgMatches, Command};
use xline_client::{error::Result, Client};

/// Parameters of `AuthClient::user_add`.
///
/// The first parameter is the name of the user.
/// The second parameter is the password of the user. If the user has no password, set it to empty string.
/// The third parameter is whether the user could has no password.
/// If set, the user is allowed to have no password.
type AuthUserAddRequest = (String, String, bool);

/// Definition of `add` command
pub(super) fn command() -> Command {
    Command::new("add")
        .about("Add a new user")
        .arg(arg!(<name> "The name of the user"))
        .arg(
            arg!([password] "Password of the user, set to empty string if the user has no password")
                .required_if_eq("no_password", "false")
                .required_unless_present("no_password"),
        )
        .arg(arg!(--no_password "Create without password"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthUserAddRequest {
    let name = matches
        .get_one::<String>("name")
        .expect("required")
        .to_owned();
    let no_password = matches.get_flag("no_password");

    (
        name,
        if no_password {
            String::new()
        } else {
            matches
                .get_one::<String>("password")
                .expect("required")
                .to_owned()
        },
        no_password,
    )
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.auth_client().user_add(req.0, req.1, req.2).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(AuthUserAddRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(
                vec!["add", "JaneSmith", "password123"],
                Some(("JaneSmith".into(), "password123".into(), false)),
            ),
            TestCase::new(
                vec!["add", "--no_password", "BobJohnson"],
                Some(("BobJohnson".into(), String::new(), true)),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }

    #[test]
    fn command_parse_should_be_invalid() {
        let test_cases = vec![TestCase::new(vec!["add", "JaneSmith"], None)];

        for case in test_cases {
            case.run_test();
        }
    }
}
