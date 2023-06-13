use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};

use crate::utils::printer::Printer;

/// Definition of `disable` command
pub(super) fn command() -> Command {
    Command::new("disable").about("disable authentication")
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, _matches: &ArgMatches) -> Result<()> {
    let resp = client.auth_client().auth_disable().await?;
    resp.print();

    Ok(())
}
