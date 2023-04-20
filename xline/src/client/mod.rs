use std::{collections::HashMap, fmt::Debug};

use curp::{client::Client as CurpClient, cmd::ProposeId};
use etcd_client::{
    AuthClient, Client as EtcdClient, KvClient, LeaseClient, LeaseKeepAliveStream, LeaseKeeper,
    LockClient, MaintenanceClient, WatchClient,
};
use itertools::Itertools;
use utils::config::ClientTimeout;
use uuid::Uuid;

use crate::{
    client::{
        errors::ClientError,
        kv_types::{
            DeleteRangeRequest, LeaseGrantRequest, LeaseKeepAliveRequest, LeaseRevokeRequest,
            LeaseTimeToLiveRequest, PutRequest, RangeRequest,
        },
    },
    rpc::{
        self, DeleteRangeResponse, LeaseGrantResponse, LeaseLeasesResponse, LeaseRevokeResponse,
        LeaseTimeToLiveResponse, PutResponse, RangeResponse, RequestWithToken,
    },
    server::command::{Command, KeyRange},
};

/// covert struct between etcd and curp
mod convert;
/// Error types
pub mod errors;
/// Requests used by Client
pub mod kv_types;
/// Restore from snapshot
pub mod restore;

/// Xline client
pub struct Client {
    /// Name of the client
    name: String,
    /// Curp client
    curp_client: CurpClient<Command>,
    /// Etcd client
    etcd_client: EtcdClient,
    /// Use curp client to send requests when true
    use_curp_client: bool,
}

impl Debug for Client {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("name", &self.name)
            .field("use_curp_client", &self.use_curp_client)
            .field("curp_client", &self.curp_client)
            .finish()
    }
}

impl Client {
    /// New `Client`
    ///
    /// # Errors
    ///
    /// If `EtcdClient::connect` fails.
    #[inline]
    pub async fn new(
        all_members: HashMap<String, String>,
        use_curp_client: bool,
        timeout: ClientTimeout,
    ) -> Result<Self, ClientError> {
        let etcd_client =
            EtcdClient::connect(all_members.values().cloned().collect_vec(), None).await?;
        let curp_client = CurpClient::new(all_members, timeout).await;
        Ok(Self {
            name: String::from("client"),
            curp_client,
            etcd_client,
            use_curp_client,
        })
    }

    /// set `use_curp_client`
    #[inline]
    pub fn set_use_curp_client(&mut self, use_curp_client: bool) {
        self.use_curp_client = use_curp_client;
    }

    /// Generate a new `ProposeId`
    fn generate_propose_id(&self) -> ProposeId {
        ProposeId::new(format!("{}-{}", self.name, Uuid::new_v4()))
    }

    /// Send `PutRequest` by `CurpClient` or `EtcdClient`
    ///
    /// # Errors
    ///
    /// If `CurpClient` or `EtcdClient` failed to send request
    #[inline]
    pub async fn put(&mut self, request: PutRequest) -> Result<PutResponse, ClientError> {
        if self.use_curp_client {
            let key_ranges = vec![KeyRange::new_one_key(request.key())];
            let propose_id = self.generate_propose_id();
            let request = RequestWithToken::new(rpc::PutRequest::from(request).into());
            let cmd = Command::new(key_ranges, request, propose_id);
            let cmd_res = self.curp_client.propose(cmd).await?;
            Ok(cmd_res.decode().into())
        } else {
            let opts = (&request).into();
            let response = self
                .etcd_client
                .put(request.key(), request.value(), Some(opts))
                .await?;
            Ok(response.into())
        }
    }

    /// Send `RangeRequest` by `CurpClient` or `EtcdClient`
    ///
    /// # Errors
    ///
    /// If `CurpClient` or `EtcdClient` failed to send request
    #[inline]
    pub async fn range(&mut self, request: RangeRequest) -> Result<RangeResponse, ClientError> {
        if self.use_curp_client {
            let key_ranges = vec![KeyRange::new(request.key(), request.range_end())];
            let propose_id = self.generate_propose_id();
            let request = RequestWithToken::new(rpc::RangeRequest::from(request).into());
            let cmd = Command::new(key_ranges, request, propose_id);
            let cmd_res = self.curp_client.propose(cmd).await?;
            Ok(cmd_res.decode().into())
        } else {
            let opts = (&request).into();
            let response = self.etcd_client.get(request.key(), Some(opts)).await?;
            Ok(response.into())
        }
    }

    /// Send `DeleteRangeRequest` by `CurpClient` or `EtcdClient`
    ///
    /// # Errors
    ///
    /// If `CurpClient` or `EtcdClient` failed to send request
    #[inline]
    pub async fn delete(
        &mut self,
        request: DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse, ClientError> {
        if self.use_curp_client {
            let key_ranges = vec![KeyRange::new(request.key(), request.range_end())];
            let propose_id = self.generate_propose_id();
            let request = RequestWithToken::new(rpc::DeleteRangeRequest::from(request).into());
            let cmd = Command::new(key_ranges, request, propose_id);
            let cmd_res = self.curp_client.propose(cmd).await?;
            Ok(cmd_res.decode().into())
        } else {
            let opts = (&request).into();
            let response = self.etcd_client.delete(request.key(), Some(opts)).await?;
            Ok(response.into())
        }
    }

    /// Send `LeaseGrantRequest` by `EtcdClient`
    ///
    /// # Errors
    ///
    /// If `EtcdClient` failed to send request
    #[inline]
    pub async fn lease_grant(
        &mut self,
        request: LeaseGrantRequest,
    ) -> Result<LeaseGrantResponse, ClientError> {
        // Cannot use curp client to send lease grant request
        // because unique lease id must generated by server
        let opts = (&request).into();
        let response = self
            .etcd_client
            .lease_grant(request.ttl(), Some(opts))
            .await?;
        Ok(response.into())
    }

    /// Send `LeaseRevokeRequest` by `EtcdClient`
    ///
    /// # Errors
    ///
    /// If `EtcdClient` failed to send request
    #[inline]
    pub async fn lease_revoke(
        &mut self,
        request: LeaseRevokeRequest,
    ) -> Result<LeaseRevokeResponse, ClientError> {
        // Cannot use curp client to send lease revoke request
        // because client cannot get keys attached to the lease
        let response = self.etcd_client.lease_revoke(request.id()).await?;
        Ok(response.into())
    }

    /// Send `LeaseKeepAliveRequest` by `EtcdClient`
    ///
    /// # Errors
    ///
    /// If `EtcdClient` failed to send request
    #[inline]
    pub async fn lease_keep_alive(
        &mut self,
        request: LeaseKeepAliveRequest,
    ) -> Result<(LeaseKeeper, LeaseKeepAliveStream), ClientError> {
        Ok(self.etcd_client.lease_keep_alive(request.id()).await?)
    }

    /// Send `LeaseTimeToLiveRequest` by `EtcdClient`
    ///
    /// # Errors
    ///
    /// If `EtcdClient` failed to send request
    #[inline]
    pub async fn lease_time_to_live(
        &mut self,
        request: LeaseTimeToLiveRequest,
    ) -> Result<LeaseTimeToLiveResponse, ClientError> {
        let opts = (&request).into();
        let response = self
            .etcd_client
            .lease_time_to_live(request.id(), Some(opts))
            .await?;
        Ok(response.into())
    }

    /// Send `LeaseLeasesRequest` by `EtcdClient`
    ///
    /// # Errors
    ///
    /// If `EtcdClient` failed to send request
    #[inline]
    pub async fn lease_leases(&mut self) -> Result<LeaseLeasesResponse, ClientError> {
        let response = self.etcd_client.leases().await?;
        Ok(response.into())
    }

    /// Gets a kv client.
    #[inline]
    pub fn kv_client(&self) -> KvClient {
        self.etcd_client.kv_client()
    }

    /// Gets an auth client.
    #[inline]
    pub fn auth_client(&self) -> AuthClient {
        self.etcd_client.auth_client()
    }

    /// Gets a watch client.
    #[inline]
    pub fn watch_client(&self) -> WatchClient {
        self.etcd_client.watch_client()
    }

    /// Gets a lock client.
    #[inline]
    pub fn lock_client(&self) -> LockClient {
        self.etcd_client.lock_client()
    }

    /// Gets a lease client.
    #[inline]
    pub fn lease_client(&self) -> LeaseClient {
        self.etcd_client.lease_client()
    }

    /// Gets a maintenance client.
    #[inline]
    pub fn maintenance_client(&self) -> MaintenanceClient {
        self.etcd_client.maintenance_client()
    }
}
