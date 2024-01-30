use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};

use crate::handle_matches;

/// `grant` command
mod grant;
/// `keep_alive` command
mod keep_alive;
/// `list` command
mod list;
/// `revoke` command
mod revoke;
/// `timetolive` command
mod timetolive;

/// Definition of `lease` command
pub(crate) fn command() -> Command {
    Command::new("lease")
        .about("Lease related commands")
        .subcommand(grant::command())
        .subcommand(keep_alive::command())
        .subcommand(list::command())
        .subcommand(revoke::command())
        .subcommand(timetolive::command())
}

/// Get matches and generate request
pub(crate) async fn execute(mut client: &mut Client, matches: &ArgMatches) -> Result<()> {
    handle_matches!(matches, client, { grant, keep_alive, list, revoke, timetolive });
    Ok(())
}
