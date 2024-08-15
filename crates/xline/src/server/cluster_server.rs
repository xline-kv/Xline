// FIXME: implement cluster server
#![allow(unused, clippy::unimplemented)]

use std::sync::Arc;

use tonic::{Request, Response, Status};
use xlineapi::{
    command::CurpClient, Cluster, MemberAddRequest, MemberAddResponse, MemberListRequest,
    MemberListResponse, MemberPromoteRequest, MemberPromoteResponse, MemberRemoveRequest,
    MemberRemoveResponse, MemberUpdateRequest, MemberUpdateResponse,
};

use crate::header_gen::HeaderGenerator;

/// Cluster Server
pub(crate) struct ClusterServer {
    /// Consensus client
    client: Arc<CurpClient>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
}

impl ClusterServer {
    /// New `ClusterServer`
    pub(crate) fn new(client: Arc<CurpClient>, header_gen: Arc<HeaderGenerator>) -> Self {
        Self { client, header_gen }
    }
}

#[tonic::async_trait]
impl Cluster for ClusterServer {
    async fn member_add(
        &self,
        request: Request<MemberAddRequest>,
    ) -> Result<Response<MemberAddResponse>, Status> {
        unimplemented!()
    }

    async fn member_remove(
        &self,
        request: Request<MemberRemoveRequest>,
    ) -> Result<Response<MemberRemoveResponse>, Status> {
        unimplemented!()
    }

    async fn member_update(
        &self,
        request: Request<MemberUpdateRequest>,
    ) -> Result<Response<MemberUpdateResponse>, Status> {
        unimplemented!()
    }

    async fn member_list(
        &self,
        request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListResponse>, Status> {
        unimplemented!()
    }

    async fn member_promote(
        &self,
        request: Request<MemberPromoteRequest>,
    ) -> Result<Response<MemberPromoteResponse>, Status> {
        unimplemented!()
    }
}
