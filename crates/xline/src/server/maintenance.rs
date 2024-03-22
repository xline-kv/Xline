use std::{fmt::Debug, pin::Pin, sync::Arc};

use async_stream::try_stream;
use bytes::BytesMut;
use clippy_utilities::{NumericCast, OverflowArithmetic};
use curp::{cmd::CommandExecutor as _, members::ClusterInfo, server::RawCurp};
use engine::SnapshotApi;
use futures::stream::Stream;
use sha2::{Digest, Sha256};
use tracing::{debug, error};
use xlineapi::{
    command::{Command, CommandResponse, CurpClient, SyncResponse},
    RequestWrapper,
};

use super::command::CommandExecutor;
use crate::{
    header_gen::HeaderGenerator,
    rpc::{
        AlarmRequest, AlarmResponse, DefragmentRequest, DefragmentResponse, DowngradeRequest,
        DowngradeResponse, HashKvRequest, HashKvResponse, HashRequest, HashResponse, Maintenance,
        MoveLeaderRequest, MoveLeaderResponse, SnapshotRequest, SnapshotResponse, StatusRequest,
        StatusResponse,
    },
    state::State,
    storage::{storage_api::StorageApi, AlarmStore, AuthStore, KvStore},
};

/// Minimum page size
const MIN_PAGE_SIZE: u64 = 512;
/// Snapshot chunk size
pub(crate) const MAINTENANCE_SNAPSHOT_CHUNK_SIZE: u64 = 64 * 1024;

/// Maintenance Server
pub(crate) struct MaintenanceServer<S>
where
    S: StorageApi,
{
    /// Kv Storage
    kv_store: Arc<KvStore<S>>,
    /// Auth Storage
    auth_store: Arc<AuthStore<S>>,
    /// persistent storage
    persistent: Arc<S>, // TODO: `persistent` is not a good name, rename it in a better way
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
    /// Consensus client
    client: Arc<CurpClient>,
    /// cluster information
    cluster_info: Arc<ClusterInfo>,
    /// Raw curp
    raw_curp: Arc<RawCurp<Command, State<S, Arc<CurpClient>>>>,
    /// Command executor
    ce: Arc<CommandExecutor<S>>,
    /// Alarm store
    alarm_store: Arc<AlarmStore<S>>,
}

impl<S> MaintenanceServer<S>
where
    S: StorageApi,
{
    /// New `MaintenanceServer`
    #[allow(clippy::too_many_arguments)] // Consistent with other servers
    pub(crate) fn new(
        kv_store: Arc<KvStore<S>>,
        auth_store: Arc<AuthStore<S>>,
        client: Arc<CurpClient>,
        persistent: Arc<S>,
        header_gen: Arc<HeaderGenerator>,
        cluster_info: Arc<ClusterInfo>,
        raw_curp: Arc<RawCurp<Command, State<S, Arc<CurpClient>>>>,
        ce: Arc<CommandExecutor<S>>,
        alarm_store: Arc<AlarmStore<S>>,
    ) -> Self {
        Self {
            kv_store,
            auth_store,
            persistent,
            header_gen,
            client,
            cluster_info,
            raw_curp,
            ce,
            alarm_store,
        }
    }

    /// Propose request and get result with fast/slow path
    async fn propose<T>(
        &self,
        request: tonic::Request<T>,
        use_fast_path: bool,
    ) -> Result<(CommandResponse, Option<SyncResponse>), tonic::Status>
    where
        T: Into<RequestWrapper> + Debug,
    {
        let auth_info = self.auth_store.try_get_auth_info_from_request(&request)?;
        let request = request.into_inner().into();
        let cmd = Command::new_with_auth_info(request.keys(), request, auth_info);
        let res = self.client.propose(&cmd, None, use_fast_path).await??;
        Ok(res)
    }
}

#[tonic::async_trait]
impl<S> Maintenance for MaintenanceServer<S>
where
    S: StorageApi,
{
    async fn alarm(
        &self,
        request: tonic::Request<AlarmRequest>,
    ) -> Result<tonic::Response<AlarmResponse>, tonic::Status> {
        let is_fast_path = true;
        let (res, sync_res) = self.propose(request, is_fast_path).await?;
        let mut res: AlarmResponse = res.into_inner().into();
        if let Some(sync_res) = sync_res {
            let revision = sync_res.revision();
            debug!("Get revision {:?} for AlarmResponse", revision);
            if let Some(header) = res.header.as_mut() {
                header.revision = revision;
            }
        }
        Ok(tonic::Response::new(res))
    }

    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let is_learner = self.cluster_info.self_member().is_learner;
        let (leader, term, _) = self.raw_curp.leader();
        let commit_index = self.raw_curp.commit_index();
        let size = self.persistent.file_size().map_err(|e| {
            error!("get file size failed, {e}");
            tonic::Status::internal("get file size failed")
        })?;
        let last_applied = self.ce.last_applied().map_err(|e| {
            error!("get last applied failed, {e}");
            tonic::Status::internal("get last applied failed")
        })?;
        let mut errors = vec![];
        if leader.is_none() {
            errors.push("etcdserver: no leader".to_owned());
        }
        for a in self.alarm_store.get_all_alarms() {
            errors.push(a.to_string());
        }
        let response = StatusResponse {
            header: Some(self.header_gen.gen_header()),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            db_size: size.numeric_cast(),
            leader: leader.unwrap_or(0), // None means this member believes there is no leader
            raft_index: commit_index,
            raft_term: term,
            raft_applied_index: last_applied,
            errors,
            db_size_in_use: size.numeric_cast(),
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
        Ok(tonic::Response::new(HashResponse {
            header: Some(self.header_gen.gen_header()),
            hash: self.persistent.hash()?,
        }))
    }

    async fn hash_kv(
        &self,
        request: tonic::Request<HashKvRequest>,
    ) -> Result<tonic::Response<HashKvResponse>, tonic::Status> {
        let revision = request.get_ref().revision;
        let (hash, compact_revision, _hash_revision) = self.kv_store.hash_kv(revision)?;
        Ok(tonic::Response::new(HashKvResponse {
            header: Some(self.header_gen.gen_header()),
            hash,
            compact_revision,
            // TODO: hash_revision was introduced in etcd 3.6, and xline is currently compatible with etcd 3.5
        }))
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
        request: tonic::Request<MoveLeaderRequest>,
    ) -> Result<tonic::Response<MoveLeaderResponse>, tonic::Status> {
        let node_id = request.into_inner().target_id;
        self.client.move_leader(node_id).await?;
        Ok(tonic::Response::new(MoveLeaderResponse {
            header: Some(self.header_gen.gen_header()),
        }))
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
            let mut buf = BytesMut::with_capacity(buf_size.numeric_cast());
            remain_size = remain_size.overflow_sub(buf_size);
            snapshot.read_buf_exact(&mut buf).await.map_err(|_e| {tonic::Status::internal("snapshot read failed")})?;
            // etcd client will use the size of the snapshot to determine whether checksum is included,
            // and the check method size % 512 == sha256.size, So we need to pad snapshots to multiples
            // of 512 bytes
            let padding = MIN_PAGE_SIZE.overflow_sub(buf_size.overflow_rem(MIN_PAGE_SIZE));
            if padding != 0 {
                buf.extend_from_slice(&vec![0; padding.numeric_cast()]);
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
            recv_data.len() % MIN_PAGE_SIZE.numeric_cast::<usize>(),
            Sha256::output_size()
        );

        let mut snap2 = persistent.get_snapshot(snapshot_path).unwrap();
        let size = snap2.size().numeric_cast();
        let mut snap2_data = BytesMut::with_capacity(size);
        snap2.read_buf_exact(&mut snap2_data).await.unwrap();
        let snap1_data = recv_data[..size].to_vec();
        assert_eq!(snap1_data, snap2_data);

        snap2.clean().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
        Ok(())
    }
}
