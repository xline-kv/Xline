use std::{collections::HashMap, sync::Arc, time::Duration};

use curp::members::{ClusterInfo, ServerId};
use itertools::Itertools;
use madsim::runtime::NodeHandle;
use tracing::debug;
use utils::config::{ClientConfig, CompactConfig, CurpConfig, ServerTimeout, StorageConfig};
use xline::{server::XlineServer, storage::db::DB};
use xline_client::{error::XlineClientError, types::kv::PutRequest, Client, ClientOptions};
use xlineapi::{command::Command, PutResponse};

struct XlineNode {
    addr: String,
    handle: NodeHandle,
}

pub struct XlineGroup {
    nodes: HashMap<ServerId, XlineNode>,
    client_handle: NodeHandle,
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
                            i == 0,
                            CurpConfig::default(),
                            ClientConfig::default(),
                            ServerTimeout::default(),
                            StorageConfig::Memory,
                            CompactConfig::default(),
                        );
                        let db = DB::open(&StorageConfig::Memory).unwrap();
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
                (id, XlineNode { addr, handle })
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
}

pub struct SimClient {
    inner: Arc<Client>,
    handle: NodeHandle,
}

impl SimClient {
    pub async fn put(&self, request: PutRequest) -> Result<PutResponse, XlineClientError<Command>> {
        let client = self.inner.clone();
        self.handle
            .spawn(async move { client.kv_client().put(request).await })
            .await
            .unwrap()
    }
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
