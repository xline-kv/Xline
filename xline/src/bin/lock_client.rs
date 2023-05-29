//! this binary is only used for the validation of lock service

use clap::{Parser, Subcommand};
use etcd_client::Client;

#[derive(Parser, Debug, Clone, PartialEq, Eq)]
struct ClientArgs {
    #[clap(short, long, value_delimiter = ',')]
    endpoints: Vec<String>,
    #[clap(subcommand)]
    command: Commands,
}

/// Types of sub command
#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum Commands {
    /// Lock args
    Lock {
        /// Lock name
        #[clap(value_parser)]
        name: String,
    },
    /// UnLock args
    Unlock {
        /// Lock name
        #[clap(value_parser)]
        key: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: ClientArgs = ClientArgs::parse();
    let endpoints = if args.endpoints.is_empty() {
        vec!["http://127.0.0.1:2379".to_owned()]
    } else {
        args.endpoints
    };
    let mut client = Client::connect(endpoints, None).await?;
    match args.command {
        Commands::Lock { name } => {
            let lock_res = client.lock(name, None).await?;
            println!("{}", String::from_utf8_lossy(lock_res.key()))
        }
        Commands::Unlock { key } => {
            let _unlock_res = client.unlock(key).await?;
            println!("unlock success");
        }
    };
    Ok(())
}

#[cfg(test)]
mod test {
    use coverage_helper::test;

    use super::*;

    #[test]
    fn it_works() {
        let args: ClientArgs = ClientArgs::parse_from([
            "lock_client",
            "--endpoints",
            "http://127.0.0.1:1234",
            "lock",
            "test",
        ]);
        assert_eq!(args.endpoints, vec!["http://127.0.0.1:1234"]);
        assert_eq!(
            args.command,
            Commands::Lock {
                name: "test".to_owned()
            }
        );
        let args2: ClientArgs = ClientArgs::parse_from(["lock_client", "unlock", "test"]);
        assert_eq!(args2.endpoints, Vec::<String>::new());
        assert_eq!(
            args2.command,
            Commands::Unlock {
                key: "test".to_owned()
            }
        );
    }
}
