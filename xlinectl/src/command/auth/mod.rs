use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};

use crate::handle_matches;

/// Auth disable command
mod disable;
/// Auth enable command
mod enable;
/// Auth status command
mod status;

/// Definition of `auth` command
pub(crate) fn command() -> Command {
    Command::new("auth")
        .about("Manage authentication")
        .subcommand(enable::command())
        .subcommand(disable::command())
        .subcommand(status::command())
}

/// Execute the command
pub(crate) async fn execute(mut client: &mut Client, matches: &ArgMatches) -> Result<()> {
    handle_matches!(matches, client, {enable, disable, status});

    Ok(())
}
