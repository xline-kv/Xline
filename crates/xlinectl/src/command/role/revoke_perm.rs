use clap::{arg, ArgMatches, Command};
use xline_client::{error::Result, types::auth::AuthRoleRevokePermissionRequest, Client};

use crate::utils::printer::Printer;

/// Definition of `revoke_perm` command
pub(super) fn command() -> Command {
    Command::new("revoke_perm")
        .about("Revoke permission from a role")
        .arg(arg!(<name> "The name of the role"))
        .arg(arg!(<key> "The Key"))
        .arg(arg!([range_end] "Range end of the key"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthRoleRevokePermissionRequest {
    let name = matches.get_one::<String>("name").expect("required");
    let key = matches.get_one::<String>("key").expect("required");
    let range_end = matches.get_one::<String>("range_end");

    let mut request = AuthRoleRevokePermissionRequest::new(name, key.as_bytes());

    if let Some(range_end) = range_end {
        request = request.with_range_end(range_end.as_bytes());
    };

    request
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.auth_client().role_revoke_permission(req).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(AuthRoleRevokePermissionRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(
                vec!["revoke_perm", "Admin", "key1", "key2"],
                Some(AuthRoleRevokePermissionRequest::new("Admin", "key1").with_range_end("key2")),
            ),
            TestCase::new(
                vec!["revoke_perm", "Admin", "key3"],
                Some(AuthRoleRevokePermissionRequest::new("Admin", "key3")),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
