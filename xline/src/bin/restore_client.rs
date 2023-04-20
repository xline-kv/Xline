use std::path::PathBuf;

use clap::Parser;
use xline::client::restore::restore;

#[derive(Parser, Debug)]
struct ClientArgs {
    snapshot_path: PathBuf,
    #[clap(short, long)]
    data_dir: PathBuf,
}

#[tokio::main]
async fn main() {
    let args: ClientArgs = ClientArgs::parse();
    if let Err(e) = restore(args.snapshot_path, args.data_dir).await {
        eprintln!("Error: {:?}", e);
    }
}
