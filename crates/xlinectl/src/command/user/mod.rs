use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};

use crate::handle_matches;

/// User add command
mod add;
/// User delete command
mod delete;
/// User get command
mod get;
/// User grant role command
mod grant_role;
/// User list command
mod list;
/// User change password command
mod passwd;
/// User revoke role command
mod revoke_role;

/// Definition of `auth` command
pub(crate) fn command() -> Command {
    Command::new("user")
        .about("User related commands")
        .subcommand(add::command())
        .subcommand(delete::command())
        .subcommand(get::command())
        .subcommand(grant_role::command())
        .subcommand(list::command())
        .subcommand(passwd::command())
        .subcommand(revoke_role::command())
}

/// Execute the command
pub(crate) async fn execute(mut client: &mut Client, matches: &ArgMatches) -> Result<()> {
    handle_matches!(matches, client, { add, delete, get, grant_role, list, passwd, revoke_role });

    Ok(())
}
