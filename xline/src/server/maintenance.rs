use std::{pin::Pin, sync::Arc};

use async_stream::try_stream;
use bytes::BytesMut;
use clippy_utilities::{Cast, OverflowArithmetic};
use curp::{client::Client, members::ClusterInfo};
use engine::SnapshotApi;
use futures::stream::Stream;
use sha2::{Digest, Sha256};
use tracing::error;
use xlineapi::command::Command;

use super::command::client_err_to_status;
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
    /// Consensus client
    client: Arc<Client<Command>>,
    /// cluster information
    cluster_info: Arc<ClusterInfo>,
}

impl<S> MaintenanceServer<S>
where
    S: StorageApi,
{
    /// New `MaintenanceServer`
    pub(crate) fn new(
        client: Arc<Client<Command>>,
        persistent: Arc<S>,
        header_gen: Arc<HeaderGenerator>,
        cluster_info: Arc<ClusterInfo>,
    ) -> Self {
        Self {
            persistent,
            header_gen,
            client,
            cluster_info,
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
        let cluster = self
            .client
            .get_cluster_from_curp(false)
            .await
            .map_err(client_err_to_status)?;
        let header = self.header_gen.gen_header();
        let self_id = self.cluster_info.self_id();
        let is_learner = cluster
            .members
            .iter()
            .find(|member| member.id == self_id)
            .map_or(false, |member| member.is_learner);
        // TODO bypass CurpNode in client to get more detailed information
        let response = StatusResponse {
            header: Some(header),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            db_size: -1,
            leader: cluster.leader_id.unwrap_or(0), // None means this member believes there is no leader
            raft_index: 0,
            raft_term: cluster.term,
            raft_applied_index: 0,
            errors: vec![],
            db_size_in_use: -1,
            is_learner,
        };
        Ok(tonic::Response::new(response))
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

    type SnapshotStream =
        Pin<Box<dyn Stream<Item = Result<SnapshotResponse, tonic::Status>> + Send>>;

    async fn snapshot(
        &self,
        _request: tonic::Request<SnapshotRequest>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, tonic::Status> {
        let stream = snapshot_stream(self.header_gen.as_ref(), self.persistent.as_ref())?;

        Ok(tonic::Response::new(Box::pin(stream)))
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

/// Generate snapshot stream
fn snapshot_stream<S: StorageApi>(
    header_gen: &HeaderGenerator,
    persistent: &S,
) -> Result<impl Stream<Item = Result<SnapshotResponse, tonic::Status>>, tonic::Status> {
    let tmp_path = format!("/tmp/snapshot-{}", uuid::Uuid::new_v4());
    let mut snapshot = persistent.get_snapshot(tmp_path).map_err(|e| {
        error!("get snapshot failed, {e}");
        tonic::Status::internal("get snapshot failed")
    })?;

    let header = header_gen.gen_header();

    let stream = try_stream! {
        if let Err(e) = snapshot.rewind() {
            error!("snapshot rewind failed, {e}");
            return;
        }

        let mut remain_size = snapshot.size();
        let mut checksum_gen = Sha256::new();
        while remain_size > 0 {
            let buf_size = std::cmp::min(MAINTENANCE_SNAPSHOT_CHUNK_SIZE, remain_size);
            let mut buf = BytesMut::with_capacity(buf_size.cast());
            remain_size = remain_size.overflow_sub(buf_size);
            snapshot.read_buf_exact(&mut buf).await.map_err(|_e| {tonic::Status::internal("snapshot read failed")})?;
            // etcd client will use the size of the snapshot to determine whether checksum is included,
            // and the check method size % 512 == sha256.size, So we need to pad snapshots to multiples
            // of 512 bytes
            let padding = MIN_PAGE_SIZE.overflow_sub(buf_size.overflow_rem(MIN_PAGE_SIZE));
            if padding != 0 {
                buf.extend_from_slice(&vec![0; padding.cast()]);
            }
            checksum_gen.update(&buf);
            yield SnapshotResponse {
                header: Some(header.clone()),
                remaining_bytes: remain_size,
                blob: Vec::from(buf)
            };
        }
        let checksum = checksum_gen.finalize().to_vec();
        yield SnapshotResponse {
            header: Some(header),
            remaining_bytes: 0,
            blob: checksum,
        };
        if let Err(e) = snapshot.clean().await {
            error!("snapshot clean failed, {e}");
        }
    };

    Ok(stream)
}

#[cfg(test)]
mod test {
    use std::{error::Error, path::PathBuf};

    use test_macros::abort_on_panic;
    use tokio_stream::StreamExt;
    use utils::config::EngineConfig;

    use super::*;
    use crate::storage::db::DB;

    #[tokio::test]
    #[abort_on_panic]
    async fn test_snapshot_stream() -> Result<(), Box<dyn Error>> {
        let dir = PathBuf::from("/tmp/test_snapshot_rpc");
        let db_path = dir.join("db");
        let snapshot_path = dir.join("snapshot");

        let persistent = DB::open(&EngineConfig::RocksDB(db_path.clone()))?;
        let header_gen = HeaderGenerator::new(0, 0);
        let snap1_stream = snapshot_stream(&header_gen, persistent.as_ref())?;
        tokio::pin!(snap1_stream);
        let mut recv_data = Vec::new();
        while let Some(data) = snap1_stream.next().await {
            recv_data.append(&mut data?.blob);
        }
        assert_eq!(
            recv_data.len() % MIN_PAGE_SIZE.cast::<usize>(),
            Sha256::output_size()
        );

        let mut snap2 = persistent.get_snapshot(snapshot_path).unwrap();
        let size = snap2.size().cast();
        let mut snap2_data = BytesMut::with_capacity(size);
        snap2.read_buf_exact(&mut snap2_data).await.unwrap();
        let snap1_data = recv_data[..size].to_vec();
        assert_eq!(snap1_data, snap2_data);

        snap2.clean().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
        Ok(())
    }
}
