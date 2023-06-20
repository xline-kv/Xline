use std::path::{Path, PathBuf};

use bytes::BytesMut;
use clippy_utilities::Cast;
use engine::{Engine, EngineType, Snapshot, SnapshotApi, StorageEngine};
use tokio_util::io::read_buf;

use super::errors::ClientError;
use crate::{server::MAINTENANCE_SNAPSHOT_CHUNK_SIZE, storage::db::XLINE_TABLES};

/// Restore snapshot to data dir
/// # Errors
/// return `ClientError::IoError` if meet io errors
/// return `ClientError::EngineError` if meet engine errors
#[inline]
#[allow(clippy::indexing_slicing)] // safe operation
pub async fn restore<P: AsRef<Path>, D: Into<PathBuf>>(
    snapshot_path: P,
    data_dir: D,
) -> Result<(), ClientError> {
    let mut snapshot_f = tokio::fs::File::open(snapshot_path).await?;
    let tmp_path = format!("/tmp/snapshot-{}", uuid::Uuid::new_v4());
    let mut rocks_snapshot = Snapshot::new_for_receiving(EngineType::Rocks((&tmp_path).into()))?;
    let mut buf = BytesMut::with_capacity(MAINTENANCE_SNAPSHOT_CHUNK_SIZE.cast());
    while let Ok(n) = read_buf(&mut snapshot_f, &mut buf).await {
        if n == 0 {
            break;
        }
        rocks_snapshot.write_all(buf.split().freeze()).await?;
    }

    let restore_rocks_engine = Engine::new(EngineType::Rocks(data_dir.into()), &XLINE_TABLES)?;
    restore_rocks_engine
        .apply_snapshot(rocks_snapshot, &XLINE_TABLES)
        .await?;
    Ok(())
}
