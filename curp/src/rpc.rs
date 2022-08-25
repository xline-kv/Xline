// Skip for generated code
#[allow(
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    unused_qualifications,
    unreachable_pub,
    variant_size_differences
)]
mod proto {
    tonic::include_proto!("messagepb");
}

use std::{sync::Arc, time::Duration};

use tokio::sync::RwLock;

use crate::{
    cmd::{Command, ProposeId},
    error::ProposeError,
};

pub(crate) use self::proto::{
    propose_response::ExeResult,
    protocol_client::ProtocolClient,
    protocol_server::{Protocol, ProtocolServer},
    sync_response,
    wait_synced_response::{Success, SyncResult},
    ProposeRequest, ProposeResponse, SyncRequest, SyncResponse, WaitSyncedRequest,
    WaitSyncedResponse,
};

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

    /// Create a new `Propose` request
    pub(crate) fn new<C>(cmd: &C) -> bincode::Result<Self>
    where
        C: Command,
    {
        Ok(Self {
            command: bincode::serialize(&cmd)?,
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
    #[allow(clippy::unnecessary_wraps)] // To keep the new funtions return the same type
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
    pub(crate) fn new_error<C: Command>(err: &str) -> bincode::Result<Self> {
        Ok(Self {
            sync_result: Some(SyncResult::Error(bincode::serialize(err)?)),
        })
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

impl SyncRequest {
    /// Create a new `SyncResult`
    pub(crate) fn new<C: Command>(term: u64, index: u64, cmd: &Arc<C>) -> bincode::Result<Self> {
        Ok(Self {
            term,
            index,
            cmd: bincode::serialize(cmd.as_ref())?,
        })
    }

    /// Get term number
    pub(crate) fn term(&self) -> u64 {
        self.term
    }

    /// Get log index
    pub(crate) fn index(&self) -> u64 {
        self.index
    }

    /// Get command
    pub(crate) fn cmd<C: Command>(&self) -> bincode::Result<C> {
        bincode::deserialize(&self.cmd)
    }
}

impl SyncResponse {
    /// Create a "synced" response
    pub(crate) fn new_synced() -> Self {
        Self {
            sync_response: Some(sync_response::SyncResponse::Synced(true)),
        }
    }

    /// Create a "wrong term" reponse
    pub(crate) fn new_wrong_term(term: u64) -> Self {
        Self {
            sync_response: Some(sync_response::SyncResponse::WrongTerm(term)),
        }
    }

    /// Create a "previous not ready" response
    pub(crate) fn new_prev_not_ready(index: u64) -> Self {
        Self {
            sync_response: Some(sync_response::SyncResponse::PrevNotReady(index)),
        }
    }

    /// Create a "entry not empty" response
    pub(crate) fn new_entry_not_empty<C: Command>(term: u64, cmd: &C) -> bincode::Result<Self> {
        Ok(Self {
            sync_response: Some(sync_response::SyncResponse::EntryNotEmpty(
                sync_response::EntryNotEmpty {
                    term,
                    cmd: bincode::serialize(cmd)?,
                },
            )),
        })
    }
}

/// The connection struct to hold the real rpc connections, it may failed to connect, but it also
/// retries the next time
#[derive(Debug)]
pub(crate) struct Connect {
    /// The rpc connection, if it fails it contains a error, otherwise the rpc client is there
    rpc_connect: RwLock<Result<ProtocolClient<tonic::transport::Channel>, tonic::transport::Error>>,
    /// The addr used to connect if failing met
    addr: String,
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
        let option_client = self.get().await;
        let mut tr = tonic::Request::new(request);
        tr.set_timeout(timeout);
        match option_client {
            Ok(mut client) => Ok(client.propose(tr).await?),
            Err(e) => Err(e.into()),
        }
    }

    /// send "wait synced" request
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

    /// send "sync" request
    pub(crate) async fn sync(
        &self,
        request: SyncRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<SyncResponse>, ProposeError> {
        let option_client = self.get().await;
        let mut tr = tonic::Request::new(request);
        tr.set_timeout(timeout);
        match option_client {
            Ok(mut client) => Ok(client.sync(tr).await?),
            Err(e) => Err(e.into()),
        }
    }
}

/// Convert a vec of addr string to a vec of `Connect`
pub(crate) async fn try_connect(addrs: Vec<String>) -> Vec<Connect> {
    futures::future::join_all(
        addrs
            .iter()
            .map(|addr| ProtocolClient::<_>::connect(addr.clone())),
    )
    .await
    .into_iter()
    .zip(addrs.into_iter())
    .map(|(conn, addr)| Connect {
        rpc_connect: RwLock::new(conn),
        addr,
    })
    .collect()
}
