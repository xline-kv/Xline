use clap::{arg, ArgMatches, Command};
use xline_client::{error::Result, Client};

use crate::utils::printer::Printer;

/// Definition of `delete` command
pub(super) fn command() -> Command {
    Command::new("delete")
        .about("delete a role")
        .arg(arg!(<name> "The name of the role"))
}

/// Build request from matches
///
/// Returns the name of the role to be deleted
pub(super) fn build_request(matches: &ArgMatches) -> String {
    let name = matches.get_one::<String>("name").expect("required");
    name.to_owned()
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

    test_case_struct!(String);

    #[test]
    fn command_parse_should_be_valid() {
        let test_cases = vec![TestCase::new(vec!["delete", "Admin"], Some("Admin".into()))];

        for case in test_cases {
            case.run_test();
        }
    }
}
