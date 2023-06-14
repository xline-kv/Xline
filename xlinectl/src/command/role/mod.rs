use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};

use crate::handle_matches;

/// Role add command
pub(super) mod add;
/// Role delete command
pub(super) mod delete;
/// Role get command
pub(super) mod get;
/// Role grant permission command
pub(super) mod grant_perm;
/// Role list command
pub(super) mod list;
/// Role revoke permission command
pub(super) mod revoke_perm;

/// Definition of `auth` command
pub(crate) fn command() -> Command {
    Command::new("role")
        .about("Role related commands")
        .subcommand(add::command())
        .subcommand(delete::command())
        .subcommand(get::command())
        .subcommand(grant_perm::command())
        .subcommand(list::command())
        .subcommand(revoke_perm::command())
}

/// Execute the command
pub(crate) async fn execute(mut client: &mut Client, matches: &ArgMatches) -> Result<()> {
    handle_matches!(matches, client, { add, delete, get, grant_perm, list, revoke_perm });

    Ok(())
}
