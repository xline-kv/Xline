use clap::{arg, ArgMatches, Command};
use xline_client::{
    error::Result,
    types::auth::{AuthRoleGetRequest, AuthUserGetRequest},
    Client,
};

use crate::utils::printer::Printer;

/// Definition of `get` command
pub(super) fn command() -> Command {
    Command::new("get")
        .about("Get a new user")
        .arg(arg!(<name> "The name of the user"))
        .arg(arg!(--detail "Show permissions of roles granted to the user"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthUserGetRequest {
    let name = matches.get_one::<String>("name").expect("required");
    AuthUserGetRequest::new(name.as_str())
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let detail = matches.get_flag("detail");

    let req = build_request(matches);

    let resp = client.auth_client().user_get(req).await?;

    if detail {
        for role in resp.roles {
            println!("{role}");
            let resp_role_get = client
                .auth_client()
                .role_get(AuthRoleGetRequest::new(&role))
                .await?;
            resp_role_get.print();
        }
    } else {
        resp.print();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(AuthUserGetRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(
                vec!["get", "JohnDoe"],
                Some(AuthUserGetRequest::new("JohnDoe")),
            ),
            TestCase::new(
                vec!["get", "--detail", "JaneSmith"],
                Some(AuthUserGetRequest::new("JaneSmith")),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
