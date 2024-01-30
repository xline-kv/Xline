use clap::{arg, ArgMatches, Command};
use xline_client::{
    error::Result,
    types::auth::{AuthRoleGrantPermissionRequest, Permission},
    Client,
};
use xlineapi::Type;

use crate::utils::printer::Printer;

/// Definition of `grant_perm` command
pub(super) fn command() -> Command {
    Command::new("grant_perm")
        .about("Grant permission to a role")
        .arg(arg!(<name> "The name of the role"))
        .arg(arg!(<perm_type> "The type of the permission").value_parser(["Read", "Write", "ReadWrite"]))
        .arg(arg!(<key> "The Key"))
        .arg(arg!([range_end] "Range end of the key"))
        .arg(arg!(--prefix "Get keys with matching prefix"))
        .arg(
            arg!(--from_key "Get keys that are greater than or equal to the given key using byte compare")
                .conflicts_with("range_end")
        )
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthRoleGrantPermissionRequest {
    let name = matches.get_one::<String>("name").expect("required");
    let perm_type_local = matches.get_one::<String>("perm_type").expect("required");
    let key = matches.get_one::<String>("key").expect("required");
    let range_end = matches.get_one::<String>("range_end");
    let prefix = matches.get_flag("prefix");
    let from_key = matches.get_flag("from_key");

    let perm_type = match perm_type_local.as_str() {
        "Read" => Type::Read,
        "Write" => Type::Write,
        "ReadWrite" => Type::Readwrite,
        _ => unreachable!("should be checked by clap"),
    };

    let mut perm = Permission::new(perm_type, key.as_bytes());

    if let Some(range_end) = range_end {
        perm = perm.with_range_end(range_end.as_bytes());
    };

    if prefix {
        perm = perm.with_prefix();
    }

    if from_key {
        perm = perm.with_from_key();
    }

    AuthRoleGrantPermissionRequest::new(name, perm)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client.auth_client().role_grant_permission(req).await?;
    resp.print();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_case_struct;

    test_case_struct!(AuthRoleGrantPermissionRequest);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![
            TestCase::new(
                vec!["grant_perm", "Admin", "Read", "key1", "key2"],
                Some(AuthRoleGrantPermissionRequest::new(
                    "Admin",
                    Permission::new(Type::Read, "key1").with_range_end("key2"),
                )),
            ),
            TestCase::new(
                vec!["grant_perm", "Admin", "Write", "key3", "--from_key"],
                Some(AuthRoleGrantPermissionRequest::new(
                    "Admin",
                    Permission::new(Type::Write, "key3").with_from_key(),
                )),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
