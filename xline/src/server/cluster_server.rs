use std::marker::PhantomData;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use curp::ConfChangeType::{Add, AddLearner, Promote, Remove, Update};
use curp::{client::Client, cmd::generate_propose_id, ConfChange, ProposeConfChangeRequest};
use xlineapi::{
    Cluster, Member, MemberAddRequest, MemberAddResponse, MemberListRequest, MemberListResponse,
    MemberPromoteRequest, MemberPromoteResponse, MemberRemoveRequest, MemberRemoveResponse,
    MemberUpdateRequest, MemberUpdateResponse,
};

use super::command::{propose_err_to_status, Command};
use crate::header_gen::HeaderGenerator;
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
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
    /// Phantom
    phantom: PhantomData<S>,
}

impl<S> ClusterServer<S>
where
    S: StorageApi,
{
    /// New `ClusterServer`
    pub(crate) fn new(
        client: Arc<Client<Command>>,
        header_gen: Arc<HeaderGenerator>,
        name: String,
    ) -> Self {
        Self {
            client,
            name,
            header_gen,
            phantom: PhantomData,
        }
    }

    /// Send propose conf change request
    async fn propose_conf_change(&self, change: ConfChange) -> Result<Vec<Member>, Status> {
        Ok(self
            .client
            .propose_conf_change(ProposeConfChangeRequest {
                id: generate_propose_id(&self.name),
                changes: vec![change],
            })
            .await
            .map_err(propose_err_to_status)??
            .into_iter()
            .map(|member| Member {
                id: member.id(),
                name: member.name().to_owned(),
                peer_ur_ls: member.addrs().to_vec(),
                client_ur_ls: member.addrs().to_vec(),
                is_learner: member.is_learner(),
            })
            .collect())
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
        let req = request.into_inner();
        let change_type = if req.is_learner {
            i32::from(AddLearner)
        } else {
            i32::from(Add)
        };
        let members = self
            .propose_conf_change(ConfChange {
                change_type,
                node_id: 0, // the added member does not have node id yet
                address: req.peer_ur_ls.clone(),
            })
            .await?;
        let resp = MemberAddResponse {
            header: Some(self.header_gen.gen_header()),
            member: Some(Member {
                id: 0,
                name: String::new(), // the added member does not have name yet.
                peer_ur_ls: req.peer_ur_ls.clone(),
                client_ur_ls: req.peer_ur_ls,
                is_learner: req.is_learner,
            }),
            members,
        };
        Ok(Response::new(resp))
    }

    async fn member_remove(
        &self,
        request: Request<MemberRemoveRequest>,
    ) -> Result<Response<MemberRemoveResponse>, Status> {
        let req = request.into_inner();
        let members = self
            .propose_conf_change(ConfChange {
                change_type: i32::from(Remove),
                node_id: req.id,
                address: vec![],
            })
            .await?;
        let resp = MemberRemoveResponse {
            header: Some(self.header_gen.gen_header()),
            members,
        };
        Ok(Response::new(resp))
    }

    async fn member_update(
        &self,
        request: Request<MemberUpdateRequest>,
    ) -> Result<Response<MemberUpdateResponse>, Status> {
        let req = request.into_inner();
        let members = self
            .propose_conf_change(ConfChange {
                change_type: i32::from(Update),
                node_id: req.id,
                address: req.peer_ur_ls,
            })
            .await?;
        let resp = MemberUpdateResponse {
            header: Some(self.header_gen.gen_header()),
            members,
        };
        Ok(Response::new(resp))
    }

    async fn member_list(
        &self,
        request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListResponse>, Status> {
        let req = request.into_inner();
        let members = self
            .client
            .get_cluster_from_curp(req.linearizable)
            .await
            .map_err(propose_err_to_status)?
            .members;
        let resp = MemberListResponse {
            header: Some(self.header_gen.gen_header()),
            members: members
                .into_iter()
                .map(|member| Member {
                    id: member.id,
                    name: member.name,
                    peer_ur_ls: member.addrs.clone(),
                    client_ur_ls: member.addrs,
                    is_learner: member.is_learner,
                })
                .collect(),
        };
        Ok(Response::new(resp))
    }

    async fn member_promote(
        &self,
        request: Request<MemberPromoteRequest>,
    ) -> Result<Response<MemberPromoteResponse>, Status> {
        let req = request.into_inner();
        let members = self
            .propose_conf_change(ConfChange {
                change_type: i32::from(Promote),
                node_id: req.id,
                address: vec![],
            })
            .await?;
        let resp = MemberPromoteResponse {
            header: Some(self.header_gen.gen_header()),
            members,
        };
        Ok(Response::new(resp))
    }
}
