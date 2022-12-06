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

use clippy_utilities::NumericCast;
use serde::Serialize;
use std::{sync::Arc, time::Duration};

use opentelemetry::global;
use tokio::sync::RwLock;
use tracing::{debug, info_span, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::error::ExecuteError;
use crate::log::LogEntry;
use crate::message::TermNum;
use crate::{
    cmd::{Command, ProposeId},
    error::ProposeError,
    util::InjectMap,
};

pub(crate) use self::proto::{
    propose_response::ExeResult,
    protocol_client::ProtocolClient,
    protocol_server::Protocol,
    wait_synced_response::{Success, SyncResult},
    AppendEntriesRequest, AppendEntriesResponse, ProposeRequest, ProposeResponse, VoteRequest,
    VoteResponse, WaitSyncedRequest, WaitSyncedResponse,
};

pub use self::proto::protocol_server::ProtocolServer;

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
        is_leader: bool,
        term: u64,
        result: &C::ER,
    ) -> bincode::Result<Self> {
        Ok(Self {
            is_leader,
            term,
            exe_result: Some(ExeResult::Result(bincode::serialize(result)?)),
        })
    }

    /// Create an empty propose response
    #[allow(clippy::unnecessary_wraps)] // To keep the new functions return the same type
    pub(crate) fn new_empty(is_leader: bool, term: u64) -> bincode::Result<Self> {
        Ok(Self {
            is_leader,
            term,
            exe_result: None,
        })
    }

    /// Create an error propose response
    pub(crate) fn new_error(
        is_leader: bool,
        term: u64,
        error: &ProposeError,
    ) -> bincode::Result<Self> {
        Ok(Self {
            is_leader,
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
            sync_result: Some(SyncResult::Success(Success {
                after_sync_result: bincode::serialize(&asr)?,
                exe_result: bincode::serialize(&er)?,
            })),
        })
    }

    /// Create an error response
    pub(crate) fn new_error(err: &str) -> bincode::Result<Self> {
        Ok(Self {
            sync_result: Some(SyncResult::Error(bincode::serialize(err)?)),
        })
    }

    /// Create a new response from execution result and `after_sync` result
    pub(crate) fn new_from_result<C: Command>(
        er: Option<Result<C::ER, ExecuteError>>,
        asr: Option<Result<C::ASR, ExecuteError>>,
    ) -> bincode::Result<Self> {
        match (er, asr) {
            (None, Some(Err(err))) => {
                WaitSyncedResponse::new_error(&format!("after sync error: {:?}", err))
            }
            (None, Some(Ok(asr))) => WaitSyncedResponse::new_success::<C>(&asr, &None),
            (None, None) => WaitSyncedResponse::new_error("after sync error: no asr result"), // this is highly unlikely to happen,
            (Some(Err(_)), Some(_)) => {
                unreachable!("should not call after_sync when exe failed")
            }
            (Some(Err(err)), None) => {
                WaitSyncedResponse::new_error(&format!("execution error: {:?}", err))
            }
            (Some(Ok(_er)), Some(Err(err))) => {
                // FIXME: should er be returned?
                WaitSyncedResponse::new_error(&format!("after sync error: {:?}", err))
            }
            (Some(Ok(er)), Some(Ok(asr))) => WaitSyncedResponse::new_success::<C>(&asr, &Some(er)),
            (Some(Ok(_er)), None) => {
                // FIXME: should er be returned?
                WaitSyncedResponse::new_error("after sync error: no asr result")
            }
        }
    }

    /// Handle response based on the closures
    pub(crate) fn map_success_error<C: Command, SF, SFR, FF>(
        self,
        sf: SF,
        ff: FF,
    ) -> Result<SFR, ProposeError>
    where
        SF: FnOnce((C::ASR, Option<C::ER>)) -> Result<SFR, ProposeError>,
        FF: FnOnce(String) -> Result<SFR, ProposeError>,
    {
        match self.sync_result {
            None => unreachable!("WaitSyncedResponse should contain valid sync_result"),
            Some(SyncResult::Success(success)) => sf((
                bincode::deserialize(&success.after_sync_result)?,
                bincode::deserialize(&success.exe_result)?,
            )),
            Some(SyncResult::Error(err)) => ff(bincode::deserialize(&err)?),
        }
    }
}

impl AppendEntriesRequest {
    /// Create a new `append_entries` request
    pub(crate) fn new<C: Command + Serialize>(
        term: TermNum,
        prev_log_index: usize,
        prev_log_term: TermNum,
        entries: Vec<LogEntry<C>>,
        leader_commit: usize,
    ) -> bincode::Result<Self> {
        Ok(Self {
            term,
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
        prev_log_index: usize,
        prev_log_term: TermNum,
        leader_commit: usize,
    ) -> Self {
        Self {
            term,
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
    /// The rpc connection, if it fails it contains a error, otherwise the rpc client is there
    rpc_connect: RwLock<Result<ProtocolClient<tonic::transport::Channel>, tonic::transport::Error>>,
    /// The addr used to connect if failing met
    pub(crate) addr: String,
}

impl Connect {
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
    pub(crate) async fn propose(
        &self,
        request: ProposeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeResponse>, ProposeError> {
        let mut client = self.get().await?;
        let mut tr = tonic::Request::new(request);
        tr.set_timeout(timeout);

        let rpc_span = info_span!("client propose");
        global::get_text_map_propagator(|prop| {
            prop.inject_context(&rpc_span.context(), &mut InjectMap(tr.metadata_mut()));
        });

        client
            .propose(tr)
            .instrument(rpc_span)
            .await
            .map_err(Into::into)
    }

    /// send "wait synced" request
    #[allow(dead_code)] // false positive report
    pub(crate) async fn wait_synced(
        &self,
        request: WaitSyncedRequest,
    ) -> Result<tonic::Response<WaitSyncedResponse>, ProposeError> {
        let option_client = self.get().await;
        match option_client {
            Ok(mut client) => Ok(client.wait_synced(tonic::Request::new(request)).await?),
            Err(e) => Err(e.into()),
        }
    }

    /// Send `AppendEntries` request
    pub(crate) async fn append_entries(
        &self,
        request: AppendEntriesRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AppendEntriesResponse>, ProposeError> {
        let option_client = self.get().await;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        match option_client {
            Ok(mut client) => Ok(client.append_entries(req).await?),
            Err(e) => Err(e.into()),
        }
    }

    /// Send `Vote` request
    pub(crate) async fn vote(
        &self,
        request: VoteRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<VoteResponse>, ProposeError> {
        let option_client = self.get().await;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        match option_client {
            Ok(mut client) => Ok(client.vote(req).await?),
            Err(e) => Err(e.into()),
        }
    }
}

/// Convert a vec of addr string to a vec of `Connect`
pub(crate) async fn try_connect(addrs: Vec<String>) -> Vec<Arc<Connect>> {
    futures::future::join_all(
        addrs
            .iter()
            .map(|addr| ProtocolClient::<_>::connect(addr.clone())),
    )
    .await
    .into_iter()
    .zip(addrs.into_iter())
    .map(|(conn, addr)| {
        debug!("successfully establish connection with {addr}");
        Arc::new(Connect {
            rpc_connect: RwLock::new(conn),
            addr,
        })
    })
    .collect()
}
