use clap::{arg, ArgMatches, Command};
use tokio::signal;
use xline_client::{clients::Xutex, error::Result, Client};

/// Definition of `lock` command
pub(crate) fn command() -> Command {
    Command::new("lock")
        .about("Acquire a lock, which will return a unique key that exists so long as the lock is held")
        .arg(arg!(<lockname> "name of the lock"))
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let prefix = matches.get_one::<String>("lockname").expect("required");
    let mut xutex = Xutex::new(client.lock_client(), prefix, None, None).await?;

    let xutex_guard = xutex.lock_unsafe().await?;
    println!("{}", xutex_guard.key());
    signal::ctrl_c().await.expect("failed to listen for event");
    // let res = lock_resp.unlock().await;
    println!("releasing the lock, ");
    Ok(())
}
