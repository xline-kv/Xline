use std::path::Path;

use clippy_utilities::Cast;
use engine::{
    rocksdb_engine::RocksEngine,
    snapshot_api::{RocksSnapshot, SnapshotApi, SnapshotProxy},
    StorageEngine,
};
use tokio::io::AsyncReadExt;

use crate::{
    client::errors::ClientError, server::MAINTENANCE_SNAPSHOT_CHUNK_SIZE, storage::db::XLINE_TABLES,
};

/// Restore snapshot to data dir
/// # Errors
/// return `ClientError::IoError` if meet io errors
/// return `ClientError::EngineError` if meet engine errors
#[inline]
#[allow(clippy::indexing_slicing)] // safe operation
pub async fn restore(
    snapshot_path: impl AsRef<Path>,
    data_dir: impl AsRef<Path>,
) -> Result<(), ClientError> {
    let mut snapshot_f = tokio::fs::File::open(snapshot_path).await?;
    let tmp_path = format!("/tmp/snapshot-{}", uuid::Uuid::new_v4());
    let mut rocks_snapshot = RocksSnapshot::new_for_receiving(tmp_path.clone())?;
    let mut buf = vec![0; MAINTENANCE_SNAPSHOT_CHUNK_SIZE.cast()];
    while let Ok(n) = snapshot_f.read(&mut buf).await {
        if n == 0 {
            break;
        }
        rocks_snapshot.write_all(&buf[..n]).await?;
    }

    let restore_rocks_engine = RocksEngine::new(data_dir, &XLINE_TABLES)?;
    restore_rocks_engine
        .apply_snapshot(SnapshotProxy::Rocks(rocks_snapshot), &XLINE_TABLES)
        .await?;
    Ok(())
}
