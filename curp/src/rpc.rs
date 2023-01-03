#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::HashMap, sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, instrument};

pub use self::proto::protocol_server::ProtocolServer;
pub(crate) use self::proto::{
    propose_response::ExeResult,
    protocol_client::ProtocolClient,
    protocol_server::Protocol,
    wait_synced_response::{Success, SyncResult as SyncResultRaw},
    AppendEntriesRequest, AppendEntriesResponse, FetchLeaderRequest, FetchLeaderResponse,
    ProposeRequest, ProposeResponse, VoteRequest, VoteResponse, WaitSyncedRequest,
    WaitSyncedResponse,
};
use crate::{
    cmd::{Command, ProposeId},
    error::{ExecuteError, ProposeError},
    log::LogEntry,
    message::{ServerId, TermNum},
    util::Inject,
};

// Skip for generated code
#[allow(
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    unused_qualifications,
    unreachable_pub,
    variant_size_differences,
    missing_copy_implementations
)]
mod proto {
    tonic::include_proto!("messagepb");
}

impl FetchLeaderRequest {
    /// Create a new `FetchLeaderRequest`
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl FetchLeaderResponse {
    /// Create a new `FetchLeaderResponse`
    pub(crate) fn new(leader_id: Option<ServerId>, term: TermNum) -> Self {
        Self { leader_id, term }
    }
}

impl ProposeRequest {
    /// Create a new `Propose` request
    pub(crate) fn new_from_rc<C, R>(cmd: R) -> bincode::Result<Self>
    where
        C: Command,
        R: AsRef<C>,
    {
        Ok(Self {
            command: bincode::serialize(cmd.as_ref())?,
        })
    }

    /// Get command
    pub(crate) fn cmd<C: Command>(&self) -> bincode::Result<C> {
        bincode::deserialize(&self.command)
    }
}

impl ProposeResponse {
    /// Create an ok propose response
    pub(crate) fn new_result<C: Command>(
        leader_id: Option<ServerId>,
        term: u64,
        result: &C::ER,
    ) -> bincode::Result<Self> {
        Ok(Self {
            leader_id,
            term,
            exe_result: Some(ExeResult::Result(bincode::serialize(result)?)),
        })
    }

    /// Create an empty propose response
    #[allow(clippy::unnecessary_wraps)] // To keep the new functions return the same type
    pub(crate) fn new_empty(leader_id: Option<ServerId>, term: u64) -> bincode::Result<Self> {
        Ok(Self {
            leader_id,
            term,
            exe_result: None,
        })
    }

    /// Create an error propose response
    pub(crate) fn new_error(
        leader_id: Option<ServerId>,
        term: u64,
        error: &ProposeError,
    ) -> bincode::Result<Self> {
        Ok(Self {
            leader_id,
            term,
            exe_result: Some(ExeResult::Error(bincode::serialize(error)?)),
        })
    }

    /// Response term
    pub(crate) fn term(&self) -> u64 {
        self.term
    }

    /// Map response to functions `success` and `failure`
    pub(crate) fn map_or_else<C: Command, SF, FF, R>(
        &self,
        success: SF,
        failure: FF,
    ) -> bincode::Result<R>
    where
        SF: FnOnce(Option<C::ER>) -> R,
        FF: FnOnce(ProposeError) -> R,
    {
        match self.exe_result {
            Some(ExeResult::Result(ref rv)) => Ok(success(Some(bincode::deserialize(rv)?))),
            Some(ExeResult::Error(ref e)) => Ok(failure(bincode::deserialize(e)?)),
            None => Ok(success(None)),
        }
    }
}

impl WaitSyncedRequest {
    /// Create a `WaitSynced` request
    pub(crate) fn new(id: &ProposeId) -> bincode::Result<Self> {
        Ok(Self {
            id: bincode::serialize(id)?,
        })
    }

    /// Get the propose id
    pub(crate) fn id(&self) -> bincode::Result<ProposeId> {
        bincode::deserialize(&self.id)
    }
}

impl WaitSyncedResponse {
    /// Create a success response
    pub(crate) fn new_success<C: Command>(
        asr: &C::ASR,
        er: &Option<C::ER>,
    ) -> bincode::Result<Self> {
        Ok(Self {
            sync_result: Some(SyncResultRaw::Success(Success {
                after_sync_result: bincode::serialize(&asr)?,
                exe_result: bincode::serialize(&er)?,
            })),
        })
    }

    /// Create an error response
    pub(crate) fn new_error(err: &SyncError) -> bincode::Result<Self> {
        Ok(Self {
            sync_result: Some(SyncResultRaw::Error(bincode::serialize(&err)?)),
        })
    }

    /// Create a new response from execution result and `after_sync` result
    pub(crate) fn new_from_result<C: Command>(
        er: Option<Result<C::ER, ExecuteError>>,
        asr: Option<Result<C::ASR, ExecuteError>>,
    ) -> bincode::Result<Self> {
        match (er, asr) {
            (None, Some(Err(err))) => {
                WaitSyncedResponse::new_error(&SyncError::AfterSyncError(err.to_string()))
            }
            (None, Some(Ok(asr))) => WaitSyncedResponse::new_success::<C>(&asr, &None),
            (None, None) => WaitSyncedResponse::new_error(&SyncError::AfterSyncError(
                "can't get asr result".to_owned(),
            )), // this is highly unlikely to happen,
            (Some(Err(_)), Some(_)) => {
                unreachable!("should not call after_sync when exe failed")
            }
            (Some(Err(err)), None) => {
                WaitSyncedResponse::new_error(&SyncError::ExecuteError(err.to_string()))
            }
            (Some(Ok(_er)), Some(Err(err))) => {
                // FIXME: should er be returned?
                WaitSyncedResponse::new_error(&SyncError::AfterSyncError(err.to_string()))
            }
            (Some(Ok(er)), Some(Ok(asr))) => WaitSyncedResponse::new_success::<C>(&asr, &Some(er)),
            (Some(Ok(_er)), None) => {
                // FIXME: should er be returned?
                WaitSyncedResponse::new_error(&SyncError::AfterSyncError(
                    "can't get after sync result".to_owned(),
                )) // this is highly unlikely to happen,
            }
        }
    }

    /// Into deserialized result
    pub(crate) fn into<C: Command>(self) -> bincode::Result<SyncResult<C>> {
        let res = match self.sync_result {
            None => unreachable!("WaitSyncedResponse should contain valid sync_result"),
            Some(SyncResultRaw::Success(success)) => SyncResult::Success {
                asr: bincode::deserialize(&success.after_sync_result)?,
                er: bincode::deserialize(&success.exe_result)?,
            },
            Some(SyncResultRaw::Error(err)) => SyncResult::Error(bincode::deserialize(&err)?),
        };
        Ok(res)
    }
}

/// Sync Result
pub(crate) enum SyncResult<C: Command> {
    /// If sync succeeds, return asr and er
    Success {
        /// After Sync Result
        asr: C::ASR,
        /// Execution Result
        er: Option<C::ER>,
    },
    /// If sync fails, return `SyncError`
    Error(SyncError),
}

/// Wait Synced error
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) enum SyncError {
    /// If client sent a wait synced request to a non-leader
    Redirect(Option<ServerId>, TermNum),
    /// If the execution of the cmd went wrong
    ExecuteError(String),
    /// If after sync of the cmd went wrong
    AfterSyncError(String),
    /// If there is no such cmd to be waited
    NoSuchCmd(ProposeId),
    /// Wait timeout
    Timeout,
}

impl AppendEntriesRequest {
    /// Create a new `append_entries` request
    pub(crate) fn new<C: Command + Serialize>(
        term: TermNum,
        leader_id: ServerId,
        prev_log_index: usize,
        prev_log_term: TermNum,
        entries: Vec<LogEntry<C>>,
        leader_commit: usize,
    ) -> bincode::Result<Self> {
        Ok(Self {
            term,
            leader_id,
            prev_log_index: prev_log_index.numeric_cast(),
            prev_log_term: prev_log_term.numeric_cast(),
            entries: entries
                .into_iter()
                .map(|e| bincode::serialize(&e))
                .collect::<bincode::Result<Vec<Vec<u8>>>>()?,
            leader_commit: leader_commit.numeric_cast(),
        })
    }

    /// Create a new `append_entries` heartbeat request
    pub(crate) fn new_heartbeat(
        term: TermNum,
        leader_id: ServerId,
        prev_log_index: usize,
        prev_log_term: TermNum,
        leader_commit: usize,
    ) -> Self {
        Self {
            term,
            leader_id,
            prev_log_index: prev_log_index.numeric_cast(),
            prev_log_term: prev_log_term.numeric_cast(),
            entries: vec![],
            leader_commit: leader_commit.numeric_cast(),
        }
    }

    /// Get log entries
    pub(crate) fn entries<C: Command>(&self) -> bincode::Result<Vec<LogEntry<C>>> {
        self.entries
            .iter()
            .map(|entry| bincode::deserialize(entry))
            .collect()
    }
}

impl AppendEntriesResponse {
    /// Create a new rejected response
    pub(crate) fn new_reject(term: TermNum, commit_index: usize) -> Self {
        Self {
            term,
            success: false,
            commit_index: commit_index.numeric_cast(),
        }
    }

    /// Create a new accepted response
    pub(crate) fn new_accept(term: TermNum) -> Self {
        Self {
            term,
            success: true,
            commit_index: 0,
        }
    }
}

impl VoteRequest {
    /// Create a new vote request
    pub fn new(term: u64, candidate_id: String, last_log_index: usize, last_log_term: u64) -> Self {
        Self {
            term,
            candidate_id,
            last_log_index: last_log_index.numeric_cast(),
            last_log_term,
        }
    }
}

impl VoteResponse {
    /// Create a new accepted vote response
    pub fn new_accept(term: TermNum) -> Self {
        Self {
            term: term.numeric_cast(),
            vote_granted: true,
        }
    }
    /// Create a new rejected vote response
    pub fn new_reject(term: TermNum) -> Self {
        Self {
            term: term.numeric_cast(),
            vote_granted: false,
        }
    }
}

/// The connection struct to hold the real rpc connections, it may failed to connect, but it also
/// retries the next time
#[derive(Debug)]
pub(crate) struct Connect {
    /// Server id
    id: ServerId,
    /// The rpc connection, if it fails it contains a error, otherwise the rpc client is there
    rpc_connect: RwLock<Result<ProtocolClient<tonic::transport::Channel>, tonic::transport::Error>>,
    /// The addr used to connect if failing met
    pub(crate) addr: String,
    /// Whether the client can reach others, useful for testings
    #[cfg(test)]
    reachable: Arc<AtomicBool>,
}

impl Connect {
    /// Get server id
    pub(crate) fn id(&self) -> &ServerId {
        &self.id
    }

    /// Get the internal rpc connection/client
    pub(crate) async fn get(
        &self,
    ) -> Result<ProtocolClient<tonic::transport::Channel>, tonic::transport::Error> {
        if let Ok(ref client) = *self.rpc_connect.read().await {
            return Ok(client.clone());
        }
        let mut connect_write = self.rpc_connect.write().await;
        if let Ok(ref client) = *connect_write {
            return Ok(client.clone());
        }
        let client = ProtocolClient::<_>::connect(self.addr.clone())
            .await
            .map(|client| {
                *connect_write = Ok(client.clone());
                client
            })?;
        *connect_write = Ok(client.clone());
        Ok(client)
    }

    /// send "propose" request
    #[instrument(skip(self), name = "client propose")]
    pub(crate) async fn propose(
        &self,
        request: ProposeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeResponse>, ProposeError> {
        #[cfg(test)]
        if !self.reachable.load(Ordering::Relaxed) {
            return Err(ProposeError::RpcError("unreachable".to_owned()));
        }

        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.propose(req).await.map_err(Into::into)
    }

    /// send "wait synced" request
    #[instrument(skip(self), name = "client wait_synced")]
    pub(crate) async fn wait_synced(
        &self,
        request: WaitSyncedRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<WaitSyncedResponse>, ProposeError> {
        #[cfg(test)]
        if !self.reachable.load(Ordering::Relaxed) {
            return Err(ProposeError::RpcError("unreachable".to_owned()));
        }

        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.wait_synced(req).await.map_err(Into::into)
    }

    /// Send `AppendEntries` request
    pub(crate) async fn append_entries(
        &self,
        request: AppendEntriesRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AppendEntriesResponse>, ProposeError> {
        #[cfg(test)]
        if !self.reachable.load(Ordering::Relaxed) {
            return Err(ProposeError::RpcError("unreachable".to_owned()));
        }

        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.append_entries(req).await.map_err(Into::into)
    }

    /// Send `Vote` request
    pub(crate) async fn vote(
        &self,
        request: VoteRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<VoteResponse>, ProposeError> {
        #[cfg(test)]
        if !self.reachable.load(Ordering::Relaxed) {
            return Err(ProposeError::RpcError("unreachable".to_owned()));
        }

        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.vote(req).await.map_err(Into::into)
    }

    /// Send `FetchLeaderRequest`
    pub(crate) async fn fetch_leader(
        &self,
        request: FetchLeaderRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchLeaderResponse>, ProposeError> {
        #[cfg(test)]
        if !self.reachable.load(Ordering::Relaxed) {
            return Err(ProposeError::RpcError("unreachable".to_owned()));
        }

        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.fetch_leader(req).await.map_err(Into::into)
    }
}

/// Convert a vec of addr string to a vec of `Connect`
pub(crate) async fn try_connect(
    addrs: HashMap<ServerId, String>,
    #[cfg(test)] reachable: Arc<AtomicBool>,
) -> Vec<Arc<Connect>> {
    futures::future::join_all(addrs.into_iter().map(|(id, mut addr)| async move {
        // Addrs must start with "http" to communicate with the server
        if !addr.starts_with("http") {
            addr.insert_str(0, "http://");
        }
        (
            id,
            addr.clone(),
            ProtocolClient::<_>::connect(addr.clone()).await,
        )
    }))
    .await
    .into_iter()
    .map(|(id, addr, conn)| {
        debug!("successfully establish connection with {addr}");
        Arc::new(Connect {
            id,
            rpc_connect: RwLock::new(conn),
            addr,
            #[cfg(test)]
            reachable: Arc::clone(&reachable),
        })
    })
    .collect()
}
