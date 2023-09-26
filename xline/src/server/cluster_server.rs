use std::marker::PhantomData;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use curp::{client::Client, cmd::generate_propose_id};
use xlineapi::{
    Cluster, MemberAddRequest, MemberAddResponse, MemberListRequest, MemberListResponse,
    MemberPromoteRequest, MemberPromoteResponse, MemberRemoveRequest, MemberRemoveResponse,
    MemberUpdateRequest, MemberUpdateResponse,
};

use super::command::{
    command_from_request_wrapper, propose_err_to_status, Command, CommandResponse, SyncResponse,
};
use crate::storage::storage_api::StorageApi;

/// Cluster Server
pub(crate) struct ClusterServer<S>
where
    S: StorageApi,
{
    /// Consensus client
    client: Arc<Client<Command>>,
    /// Server name
    name: String,
    /// Phantom
    phantom: PhantomData<S>,
}

impl<S> ClusterServer<S>
where
    S: StorageApi,
{
    /// New `ClusterServer`
    pub(crate) fn new(client: Arc<Client<Command>>, name: String) -> Self {
        Self {
            client,
            name,
            phantom: PhantomData,
        }
    }
}

#[tonic::async_trait]
impl<S> Cluster for ClusterServer<S>
where
    S: StorageApi,
{
    async fn member_add(
        &self,
        request: Request<MemberAddRequest>,
    ) -> Result<Response<MemberAddResponse>, Status> {
        todo!()
    }

    async fn member_remove(
        &self,
        request: Request<MemberRemoveRequest>,
    ) -> Result<Response<MemberRemoveResponse>, Status> {
        todo!()
    }

    async fn member_update(
        &self,
        request: Request<MemberUpdateRequest>,
    ) -> Result<Response<MemberUpdateResponse>, Status> {
        todo!()
    }

    async fn member_list(
        &self,
        request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListResponse>, Status> {
        todo!()
    }

    async fn member_promote(
        &self,
        request: Request<MemberPromoteRequest>,
    ) -> Result<Response<MemberPromoteResponse>, Status> {
        todo!()
    }
}
