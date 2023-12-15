use std::{collections::HashMap, sync::Arc, time::Duration};

use curp::members::{ClusterInfo, ServerId};
use itertools::Itertools;
use madsim::runtime::NodeHandle;
use tonic::transport::Channel;
use tracing::debug;
use utils::config::{
    ClientConfig, CompactConfig, CurpConfig, EngineConfig, ServerTimeout, StorageConfig,
};
use xline::{server::XlineServer, storage::db::DB};
use xline_client::{
    error::XlineClientError,
    types::{
        kv::{
            CompactionRequest, CompactionResponse, PutRequest, PutResponse, RangeRequest,
            RangeResponse,
        },
        watch::{WatchRequest, WatchStreaming, Watcher},
    },
    Client, ClientOptions,
};
use xlineapi::{command::Command, KvClient, RequestUnion, WatchClient};

pub struct XlineNode {
    pub id: ServerId,
    pub addr: String,
    pub name: String,
    pub handle: NodeHandle,
}

pub struct XlineGroup {
    pub nodes: HashMap<ServerId, XlineNode>,
    pub client_handle: NodeHandle,
}

impl XlineGroup {
    pub async fn new(size: usize) -> Self {
        assert!(size >= 3, "the number of nodes must >= 3");
        let handle = madsim::runtime::Handle::current();

        let all: HashMap<_, _> = (0..size)
            .map(|x| (format!("S{x}"), vec![format!("192.168.1.{}:12345", x + 1)]))
            .collect();
        let nodes = (0..size)
            .map(|i| {
                let name = format!("S{i}");
                let addr = format!("192.168.1.{}:12345", i + 1);
                let cluster_info = Arc::new(ClusterInfo::new(all.clone(), &name));
                let id = cluster_info.self_id();

                let handle = handle
                    .create_node()
                    .name(id.to_string())
                    .ip(format!("192.168.1.{}", i + 1).parse().unwrap())
                    .init(move || {
                        let server = XlineServer::new(
                            cluster_info.clone(),
                            false,
                            CurpConfig::default(),
                            ClientConfig::default(),
                            ServerTimeout::default(),
                            StorageConfig::default(),
                            CompactConfig::default(),
                        );
                        let db = DB::open(&EngineConfig::Memory).unwrap();
                        async move {
                            server
                                .start_from_single_addr("0.0.0.0:12345".parse().unwrap(), db, None)
                                .await
                                .unwrap()
                                .await
                                .unwrap()
                                .unwrap();
                        }
                    })
                    .build();
                (
                    id,
                    XlineNode {
                        id,
                        addr,
                        name,
                        handle,
                    },
                )
            })
            .collect();
        let client_handle = handle
            .create_node()
            .name("client")
            .ip("192.168.2.1".parse().unwrap())
            .build();
        madsim::time::sleep(Duration::from_secs(20)).await;
        Self {
            nodes,
            client_handle,
        }
    }

    pub async fn client(&self) -> SimClient {
        let all_members = self
            .nodes
            .values()
            .map(|node| node.addr.clone())
            .collect_vec();
        let client = self
            .client_handle
            .spawn(async move {
                Client::connect(all_members, ClientOptions::default())
                    .await
                    .unwrap()
            })
            .await
            .unwrap();
        SimClient {
            inner: Arc::new(client),
            handle: self.client_handle.clone(),
        }
    }

    pub fn get_node_by_name(&self, name: &str) -> &XlineNode {
        self.nodes
            .iter()
            .find(|(_, node)| node.name == name)
            .unwrap()
            .1
    }
}

pub struct SimClient {
    inner: Arc<Client>,
    handle: NodeHandle,
}

macro_rules! impl_client_method {
    ($method:ident, $client:ident, $request:ty, $response:ty) => {
        pub async fn $method(
            &self,
            request: $request,
        ) -> Result<$response, XlineClientError<Command>> {
            let client = self.inner.clone();
            self.handle
                .spawn(async move { client.$client().$method(request).await })
                .await
                .unwrap()
        }
    };
}

impl SimClient {
    impl_client_method!(put, kv_client, PutRequest, PutResponse);
    impl_client_method!(range, kv_client, RangeRequest, RangeResponse);
    impl_client_method!(compact, kv_client, CompactionRequest, CompactionResponse);

    impl_client_method!(watch, watch_client, WatchRequest, (Watcher, WatchStreaming));
}

impl Drop for XlineGroup {
    fn drop(&mut self) {
        let handle = madsim::runtime::Handle::current();
        for node in self.nodes.values() {
            handle.send_ctrl_c(node.handle.id());
        }
        handle.send_ctrl_c(self.client_handle.id());
        for (name, node) in &self.nodes {
            if !handle.is_exit(node.handle.id()) {
                panic!("failed to graceful shutdown {name}");
            }
        }
        debug!("all nodes shutdowned");
    }
}

pub struct SimEtcdClient {
    watch: WatchClient<Channel>,
    kv: KvClient<Channel>,
    handle: NodeHandle,
}

impl SimEtcdClient {
    pub async fn new(addr: String, handle: NodeHandle) -> Self {
        let (watch, kv) = handle
            .spawn(async move {
                (
                    WatchClient::connect(addr.clone()).await.unwrap(),
                    KvClient::connect(addr).await.unwrap(),
                )
            })
            .await
            .unwrap();
        Self { watch, kv, handle }
    }

    pub async fn put(&self, request: PutRequest) -> Result<PutResponse, XlineClientError<Command>> {
        let mut client = self.kv.clone();
        self.handle
            .spawn(async move {
                client
                    .put(xlineapi::PutRequest::from(request))
                    .await
                    .map(|r| r.into_inner())
                    .map_err(Into::into)
            })
            .await
            .unwrap()
    }

    pub async fn compact(
        &self,
        request: CompactionRequest,
    ) -> Result<CompactionResponse, XlineClientError<Command>> {
        let mut client = self.kv.clone();
        self.handle
            .spawn(async move {
                client
                    .compact(xlineapi::CompactionRequest::from(request))
                    .await
                    .map(|r| r.into_inner())
                    .map_err(Into::into)
            })
            .await
            .unwrap()
    }

    pub async fn watch(
        &self,
        request: WatchRequest,
    ) -> Result<(Watcher, WatchStreaming), XlineClientError<Command>> {
        let mut client = self.watch.clone();

        self.handle
            .spawn(async move {
                let (mut request_sender, request_receiver) =
                    futures::channel::mpsc::channel::<xlineapi::WatchRequest>(128);

                let request = xlineapi::WatchRequest {
                    request_union: Some(RequestUnion::CreateRequest(request.into())),
                };

                request_sender
                    .try_send(request)
                    .map_err(|e| XlineClientError::WatchError(e.to_string()))?;

                let mut response_stream = client.watch(request_receiver).await?.into_inner();

                let watch_id = match response_stream.message().await? {
                    Some(resp) => {
                        assert!(resp.created, "not a create watch response");
                        resp.watch_id
                    }
                    None => {
                        return Err(XlineClientError::WatchError(String::from(
                            "failed to create watch",
                        )));
                    }
                };

                Ok((
                    Watcher::new(watch_id, request_sender.clone()),
                    WatchStreaming::new(response_stream, request_sender),
                ))
            })
            .await
            .unwrap()
    }
}
