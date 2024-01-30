use clap::{arg, ArgMatches, Command};
use xline_client::{error::Result, types::auth::AuthUserAddRequest, Client};

use crate::utils::printer::Printer;

/// Definition of `add` command
pub(super) fn command() -> Command {
    Command::new("add")
        .about("Add a new user")
        .arg(arg!(<name> "The name of the user"))
        .arg(
            arg!([password] "Password of the user")
                .required_if_eq("no_password", "false")
                .required_unless_present("no_password"),
        )
        .arg(arg!(--no_password "Create without password"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthUserAddRequest {
    let name = matches.get_one::<String>("name").expect("required");
    let no_password = matches.get_flag("no_password");
    if no_password {
        AuthUserAddRequest::new(name)
    } else {
        let password = matches.get_one::<String>("password").expect("required");
        AuthUserAddRequest::new(name).with_pwd(password)
    }
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.auth_client().user_add(req).await?;
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
                Some(AuthUserAddRequest::new("JaneSmith").with_pwd("password123")),
            ),
            TestCase::new(
                vec!["add", "--no_password", "BobJohnson"],
                Some(AuthUserAddRequest::new("BobJohnson")),
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
