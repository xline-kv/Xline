use std::sync::Arc;

use clippy_utilities::{Cast, OverflowArithmetic};
use engine::snapshot_api::SnapshotApi;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::error;

use crate::{
    header_gen::HeaderGenerator,
    rpc::{
        AlarmRequest, AlarmResponse, DefragmentRequest, DefragmentResponse, DowngradeRequest,
        DowngradeResponse, HashKvRequest, HashKvResponse, HashRequest, HashResponse, Maintenance,
        MoveLeaderRequest, MoveLeaderResponse, SnapshotRequest, SnapshotResponse, StatusRequest,
        StatusResponse,
    },
    storage::storage_api::StorageApi,
};

/// Minimum page size
const MIN_PAGE_SIZE: u64 = 512;
/// Snapshot chunk size
pub(crate) const MAINTENANCE_SNAPSHOT_CHUNK_SIZE: u64 = 64 * 1024;

/// Maintenance Server
#[derive(Debug)]
pub(crate) struct MaintenanceServer<S>
where
    S: StorageApi,
{
    /// persistent storage
    persistent: Arc<S>, // TODO: `persistent` is not a good name, rename it in a better way
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
}

impl<S> MaintenanceServer<S>
where
    S: StorageApi,
{
    /// New `LeaseServer`
    pub(crate) fn new(persistent: Arc<S>, header_gen: Arc<HeaderGenerator>) -> Self {
        Self {
            persistent,
            header_gen,
        }
    }
}

#[tonic::async_trait]
impl<S> Maintenance for MaintenanceServer<S>
where
    S: StorageApi,
{
    async fn alarm(
        &self,
        _request: tonic::Request<AlarmRequest>,
    ) -> Result<tonic::Response<AlarmResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "alarm is unimplemented".to_owned(),
        ))
    }

    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "status is unimplemented".to_owned(),
        ))
    }

    async fn defragment(
        &self,
        _request: tonic::Request<DefragmentRequest>,
    ) -> Result<tonic::Response<DefragmentResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "defragment is unimplemented".to_owned(),
        ))
    }

    async fn hash(
        &self,
        _request: tonic::Request<HashRequest>,
    ) -> Result<tonic::Response<HashResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "hash is unimplemented".to_owned(),
        ))
    }

    async fn hash_kv(
        &self,
        _request: tonic::Request<HashKvRequest>,
    ) -> Result<tonic::Response<HashKvResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "hash_kv is unimplemented".to_owned(),
        ))
    }

    type SnapshotStream = ReceiverStream<Result<SnapshotResponse, tonic::Status>>;

    async fn snapshot(
        &self,
        _request: tonic::Request<SnapshotRequest>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, tonic::Status> {
        let tmp_path = format!("/tmp/snapshot-{}", uuid::Uuid::new_v4());
        let mut snapshot = self.persistent.get_snapshot(tmp_path).map_err(|e| {
            error!("get snapshot failed, {e}");
            tonic::Status::internal("get snapshot failed")
        })?;

        let header = self.header_gen.gen_header();
        let (tx, rx) = mpsc::channel(1);
        let _ig = tokio::spawn(async move {
            if let Err(e) = snapshot.rewind() {
                error!("snapshot rewind failed, {e}");
                return;
            }

            let mut remain_size = snapshot.size();
            let mut checksum_gen = Sha256::new();
            while remain_size > 0 {
                let buf_size = std::cmp::min(MAINTENANCE_SNAPSHOT_CHUNK_SIZE, remain_size);
                let mut buf = vec![0; buf_size.cast()];
                remain_size = remain_size.overflow_sub(buf_size);
                if snapshot.read_exact(&mut buf).await.is_err() {
                    if let Err(e) = tx
                        .send(Err(tonic::Status::internal("snapshot read failed")))
                        .await
                    {
                        error!("snapshot send failed, {e}");
                    }
                    return;
                }
                // etcd client will use the size of the snapshot to determine whether checksum is included,
                // and the check method size % 512 == sha256.size, So we need to pad snapshots to multiples
                // of 512 bytes
                let padding = MIN_PAGE_SIZE.overflow_sub(buf_size.overflow_rem(MIN_PAGE_SIZE));
                if padding != 0 {
                    buf.append(&mut vec![0; padding.cast()]);
                }
                checksum_gen.update(&buf);
                let resp: SnapshotResponse = SnapshotResponse {
                    header: Some(header.clone()),
                    remaining_bytes: remain_size,
                    blob: buf,
                };
                if let Err(e) = tx.send(Ok(resp)).await {
                    error!("snapshot send failed, {e}");
                    return;
                }
            }
            let checksum = checksum_gen.finalize().to_vec();
            let resp = SnapshotResponse {
                header: Some(header),
                remaining_bytes: 0,
                blob: checksum,
            };
            if let Err(e) = tx.send(Ok(resp)).await {
                error!("snapshot send failed, {e}");
                return;
            }
            if let Err(e) = snapshot.clean().await {
                error!("snapshot clean failed, {e}");
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn move_leader(
        &self,
        _request: tonic::Request<MoveLeaderRequest>,
    ) -> Result<tonic::Response<MoveLeaderResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "move_leader is unimplemented".to_owned(),
        ))
    }

    async fn downgrade(
        &self,
        _request: tonic::Request<DowngradeRequest>,
    ) -> Result<tonic::Response<DowngradeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "downgrade is unimplemented".to_owned(),
        ))
    }
}

#[cfg(test)]
mod test {
    use std::{error::Error, path::PathBuf};

    use tokio_stream::StreamExt;
    use utils::config::StorageConfig;

    use super::*;
    use crate::storage::db::DBProxy;

    #[tokio::test]
    async fn test_snapshot_rpc() -> Result<(), Box<dyn Error>> {
        let dir = PathBuf::from("/tmp/test_snapshot_rpc");
        let db_path = dir.join("db");
        let snapshot_path = dir.join("snapshot");

        let persistent = DBProxy::open(&StorageConfig::RocksDB(db_path.clone()))?;
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let maintenance_server = MaintenanceServer::new(persistent, header_gen);
        let mut snap1_stream = maintenance_server
            .snapshot(tonic::Request::new(SnapshotRequest {}))
            .await?
            .into_inner();
        let mut recv_data = Vec::new();
        while let Some(data) = snap1_stream.next().await {
            recv_data.append(&mut data?.blob);
        }
        assert_eq!(
            recv_data.len() % MIN_PAGE_SIZE.cast::<usize>(),
            Sha256::output_size()
        );

        let mut snap2 = maintenance_server
            .persistent
            .get_snapshot(snapshot_path)
            .unwrap();
        let size = snap2.size().cast();
        let mut snap2_data = vec![0; size];
        snap2.read_exact(&mut snap2_data).await.unwrap();
        let snap1_data = recv_data[..size].to_vec();
        assert_eq!(snap1_data, snap2_data);

        snap2.clean().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
        Ok(())
    }
}
