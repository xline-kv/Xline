use std::path::{Path, PathBuf};

use clippy_utilities::Cast;
use engine::{Engine, EngineType, Snapshot, SnapshotApi, StorageEngine};
use tokio::io::AsyncReadExt;

use super::errors::ClientError;
use crate::{server::MAINTENANCE_SNAPSHOT_CHUNK_SIZE, storage::db::XLINE_TABLES};

/// Restore snapshot to data dir
/// # Errors
/// return `ClientError::IoError` if meet io errors
/// return `ClientError::EngineError` if meet engine errors
#[inline]
#[allow(clippy::indexing_slicing)] // safe operation
pub async fn restore(
    snapshot_path: impl AsRef<Path>,
    data_dir: impl Into<PathBuf>,
) -> Result<(), ClientError> {
    let mut snapshot_f = tokio::fs::File::open(snapshot_path).await?;
    let tmp_path = format!("/tmp/snapshot-{}", uuid::Uuid::new_v4());
    let mut rocks_snapshot = Snapshot::new_for_receiving(EngineType::Rocks((&tmp_path).into()))?;
    let mut buf = vec![0; MAINTENANCE_SNAPSHOT_CHUNK_SIZE.cast()];
    while let Ok(n) = snapshot_f.read(&mut buf).await {
        if n == 0 {
            break;
        }
        rocks_snapshot.write_all(&buf[..n]).await?;
    }

    let restore_rocks_engine = Engine::new(EngineType::Rocks(data_dir.into()), &XLINE_TABLES)?;
    restore_rocks_engine
        .apply_snapshot(rocks_snapshot, &XLINE_TABLES)
        .await?;
    Ok(())
}
