use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};

use crate::handle_matches;

/// `add` command
mod add;
/// `list` command
mod list;
/// `promote` command
mod promote;
/// `remove` command
mod remove;
/// `update` command
mod update;

/// Definition of `lease` command
pub(crate) fn command() -> Command {
    Command::new("member")
        .about("Member related commands")
        .subcommand(add::command())
        .subcommand(update::command())
        .subcommand(list::command())
        .subcommand(remove::command())
        .subcommand(promote::command())
}

/// Get matches and generate request
pub(crate) async fn execute(mut client: &mut Client, matches: &ArgMatches) -> Result<()> {
    handle_matches!(matches, client, { add, update, list, remove, promote });
    Ok(())
}

#[allow(clippy::unnecessary_wraps)] // The Result is required by clap
/// Parse comma separated peer urls
pub(super) fn parse_peer_urls(arg: &str) -> Result<Vec<String>> {
    Ok(arg.split(',').map(ToOwned::to_owned).collect())
}
