use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};

use crate::utils::printer::Printer;

/// Definition of `List` command
pub(super) fn command() -> Command {
    Command::new("list").about("List all active leases")
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, _matches: &ArgMatches) -> Result<()> {
    let resp = client.lease_client().leases().await?;
    resp.print();

    Ok(())
}
