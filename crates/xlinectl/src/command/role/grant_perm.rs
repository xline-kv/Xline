use clap::{arg, ArgMatches, Command};
use xline_client::{error::Result, types::range_end::RangeOption, Client};
use xlineapi::Type;

use crate::utils::printer::Printer;

/// Temp return type for `grant_perm` command, indicates `(name, PermissionType, key, RangeOption)`
type AuthRoleGrantPermissionRequest = (String, Type, Vec<u8>, Option<RangeOption>);

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

    let perm_type = match perm_type_local.to_lowercase().as_str() {
        "read" => Type::Read,
        "write" => Type::Write,
        "readwrite" => Type::Readwrite,
        _ => unreachable!("should be checked by clap"),
    };

    let range_option = if prefix {
        Some(RangeOption::Prefix)
    } else if from_key {
        Some(RangeOption::FromKey)
    } else {
        range_end.map(|inner| RangeOption::RangeEnd(inner.as_bytes().to_vec()))
    };

    (
        name.to_owned(),
        perm_type,
        key.as_bytes().to_vec(),
        range_option,
    )
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);
    let resp = client
        .auth_client()
        .role_grant_permission(req.0, req.1, req.2, req.3)
        .await?;
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
                Some((
                    "Admin".into(),
                    Type::Read,
                    "key1".into(),
                    Some(RangeOption::RangeEnd("key2".into())),
                )),
            ),
            TestCase::new(
                vec!["grant_perm", "Admin", "Write", "key3", "--from_key"],
                Some((
                    "Admin".into(),
                    Type::Write,
                    "key3".into(),
                    Some(RangeOption::FromKey),
                )),
            ),
        ];

        for case in test_cases {
            case.run_test();
        }
    }
}
