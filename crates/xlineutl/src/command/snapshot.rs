use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::{arg, ArgMatches, Command};
use engine::{Engine, EngineType};
use utils::table_names::XLINE_TABLES;

/// Definition of `snapshot` command
pub(crate) fn command() -> Command {
    Command::new("snapshot")
        .about("Manages xline node snapshots")
        .subcommand(
            Command::new("restore")
                .about("Restores an xline member snapshot to an xline directory")
                .arg(arg!(<filename> "Path to the snapshot file"))
                .arg(arg!(--"data-dir" <DATA_DIR> "Path to the output data directory")),
        )
}

/// Execute the command
pub(crate) async fn execute(matches: &ArgMatches) -> Result<()> {
    if let Some(("restore", sub_matches)) = matches.subcommand() {
        let snapshot_path = sub_matches.get_one::<String>("filename").expect("required");
        let data_dir = sub_matches.get_one::<String>("data-dir").expect("required");
        handle_restore(snapshot_path, data_dir).await?;
    }
    Ok(())
}

/// handle restore snapshot to data dir
#[inline]
async fn handle_restore<P: AsRef<Path>, D: Into<PathBuf>>(
    snapshot_path: P,
    data_dir: D,
) -> Result<()> {
    let restore_rocks_engine = Engine::new(EngineType::Rocks(data_dir.into()), &XLINE_TABLES)?;
    restore_rocks_engine
        .apply_snapshot_from_file(snapshot_path, &XLINE_TABLES)
        .await?;
    Ok(())
}
