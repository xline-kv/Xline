use std::{
    hash::Hasher,
    path::{Path, PathBuf},
};

use anyhow::Result;
use clap::{arg, ArgMatches, Command};
use engine::{Engine, EngineType, StorageEngine};
use serde::Serialize;
use tempfile::tempdir;
use utils::table_names::{KV_TABLE, XLINE_TABLES};
use xline::storage::Revision;

use crate::printer::Printer;

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
        .subcommand(
            Command::new("status")
                .about("Gets backend snapshot status of a given file")
                .arg(arg!(<filename> "Path to the snapshot file")),
        )
}

/// Execute the command
pub(crate) async fn execute(matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("restore", sub_matches)) => {
            let snapshot_path = sub_matches.get_one::<String>("filename").expect("required");
            let data_dir = sub_matches.get_one::<String>("data-dir").expect("required");
            handle_restore(snapshot_path, data_dir).await?;
        }
        Some(("status", sub_matches)) => {
            let snapshot_path = sub_matches.get_one::<String>("filename").expect("required");
            handle_status(snapshot_path).await?;
        }
        _ => {}
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

/// Snapshot status
#[derive(Debug, Default, Serialize)]
struct Status {
    /// Hash of the snapshot
    hash: u32,
    /// Last revision of the snapshot
    revision: i64,
    /// Total key-value pair count of the snapshot
    total_count: u64,
    /// Total size of the snapshot
    total_size: u64,
}

/// handle restore snapshot to data dir
#[inline]
#[allow(clippy::arithmetic_side_effects)] // u64 is big enough
async fn handle_status<P: AsRef<Path>>(snapshot_path: P) -> Result<()> {
    let tempdir = tempdir()?;
    let restore_rocks_engine = Engine::new(
        EngineType::Rocks(tempdir.path().to_path_buf()),
        &XLINE_TABLES,
    )?;
    restore_rocks_engine
        .apply_snapshot_from_file(snapshot_path, &XLINE_TABLES)
        .await?;
    let mut status = Status {
        total_size: restore_rocks_engine.file_size()?,
        ..Default::default()
    };
    let mut hasher = crc32fast::Hasher::new();
    for table in XLINE_TABLES {
        hasher.write(table.as_bytes());
        let kv_pairs = restore_rocks_engine.get_all(table)?;
        let is_kv_table = table == KV_TABLE;
        for (k, v) in kv_pairs {
            hasher.write(k.as_slice());
            hasher.write(v.as_slice());
            if is_kv_table {
                let rev = Revision::decode(k.as_slice());
                status.revision = status.revision.max(rev.revision());
            }
            status.total_count += 1;
        }
    }
    status.hash = hasher.finalize();
    status.print();

    Ok(())
}

impl Printer for Status {
    fn simple(&self) {
        println!(
            "{:x}, {}, {}, {}",
            self.hash, self.revision, self.total_count, self.total_size
        );
    }

    fn field(&self) {
        println!("Hash : {}", self.hash);
        println!("Revision : {}", self.revision);
        println!("Keys : {}", self.total_count);
        println!("Size : {}", self.total_size);
    }
}
