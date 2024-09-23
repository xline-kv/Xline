use std::{collections::BTreeSet, sync::Arc};

use curp::rpc::{Node, NodeMetadata};
use rand::Rng;
use tonic::{Request, Response, Status};
use xlineapi::{
    command::CurpClient, Cluster, Member, MemberAddRequest, MemberAddResponse, MemberListRequest,
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

    /// Fetch members
    async fn fetch_members(&self, linearizable: bool) -> Result<Vec<Member>, Status> {
        let resp = self.client.fetch_cluster(linearizable).await?;
        let member_ids: BTreeSet<_> = resp.members.into_iter().flat_map(|q| q.set).collect();
        Ok(resp
            .nodes
            .into_iter()
            .map(|n| {
                let (id, meta) = n.into_parts();
                Member {
                    id,
                    name: meta.name,
                    peer_ur_ls: meta.peer_urls,
                    client_ur_ls: meta.client_urls,
                    is_learner: !member_ids.contains(&id),
                }
            })
            .collect())
    }

    /// Generate a random node name
    fn gen_rand_node_name() -> String {
        let mut rng = rand::thread_rng();
        let suffix_num: u32 = rng.gen();
        format!("xline_{suffix_num:08x}")
    }

    /// Generates a random node ID.
    fn gen_rand_node_id() -> u64 {
        rand::thread_rng().gen()
    }
}

#[tonic::async_trait]
impl Cluster for ClusterServer {
    async fn member_add(
        &self,
        request: Request<MemberAddRequest>,
    ) -> Result<Response<MemberAddResponse>, Status> {
        let header = self.header_gen.gen_header();
        let request = request.into_inner();
        let name = Self::gen_rand_node_name();
        let id = Self::gen_rand_node_id();
        let meta = NodeMetadata::new(name, request.peer_ur_ls, vec![]);
        let node = Node::new(id, meta);
        self.client.add_learner(vec![node]).await?;
        if !request.is_learner {
            self.client.add_member(vec![id]).await?;
        }
        let members = self.fetch_members(true).await?;
        let added = members
            .iter()
            .find(|m| m.id == id)
            .ok_or(tonic::Status::internal("added member not found"))?
            .clone();

        Ok(tonic::Response::new(MemberAddResponse {
            header: Some(header),
            member: Some(added),
            members,
        }))
    }

    async fn member_remove(
        &self,
        request: Request<MemberRemoveRequest>,
    ) -> Result<Response<MemberRemoveResponse>, Status> {
        let header = self.header_gen.gen_header();
        let id = request.into_inner().id;
        // In etcd a member could be a learner, and could return CurpError::InvalidMemberChange
        // TODO: handle other errors that may returned
        let _ignore = self.client.remove_member(vec![id]).await;
        self.client.remove_learner(vec![id]).await?;
        let members = self.fetch_members(true).await?;

        Ok(tonic::Response::new(MemberRemoveResponse {
            header: Some(header),
            members,
        }))
    }

    async fn member_update(
        &self,
        _request: Request<MemberUpdateRequest>,
    ) -> Result<Response<MemberUpdateResponse>, Status> {
        unimplemented!()
    }

    async fn member_list(
        &self,
        request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListResponse>, Status> {
        let header = self.header_gen.gen_header();
        let members = self
            .fetch_members(request.into_inner().linearizable)
            .await?;
        Ok(tonic::Response::new(MemberListResponse {
            header: Some(header),
            members,
        }))
    }

    async fn member_promote(
        &self,
        request: Request<MemberPromoteRequest>,
    ) -> Result<Response<MemberPromoteResponse>, Status> {
        let header = self.header_gen.gen_header();
        self.client
            .add_member(vec![request.into_inner().id])
            .await?;
        let members = self.fetch_members(true).await?;
        Ok(tonic::Response::new(MemberPromoteResponse {
            header: Some(header),
            members,
        }))
    }
}
