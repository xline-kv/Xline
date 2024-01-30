use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};

use crate::utils::printer::Printer;

/// Definition of `list` command
pub(super) fn command() -> Command {
    Command::new("list").about("List all users")
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, _matches: &ArgMatches) -> Result<()> {
    let resp = client.auth_client().user_list().await?;
    resp.print();

    Ok(())
}
