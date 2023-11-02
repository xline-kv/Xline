use std::fmt::Debug;

use anyhow::Result;
use etcd_client::{Client as EtcdClient, PutOptions};
use thiserror::Error;
#[cfg(test)]
use xline_client::types::kv::{RangeRequest, RangeResponse};
use xline_client::{
    error::XlineClientError as ClientError,
    types::kv::{PutRequest, PutResponse},
    Client, ClientOptions,
};
use xlineapi::command::Command;

/// The client used in benchmark
#[derive(Error, Debug)]
#[non_exhaustive]
pub(crate) enum BenchClientError {
    /// Error from `etcd_client`
    #[error("etcd_client error: {0}")]
    EtcdError(#[from] etcd_client::Error),
    /// Error from `xline_client`
    #[error("xline_client error: {0}")]
    XlineError(#[from] ClientError<Command>),
}

/// The KV client enum used in benchmark
pub(crate) enum KVClient {
    /// Xline client
    Xline(Client),
    /// Etcd client
    Etcd(EtcdClient),
}

impl Debug for KVClient {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            KVClient::Xline(ref client) => write!(f, "Xline({client:?})"),
            KVClient::Etcd(ref _client) => write!(f, "Etcd"),
        }
    }
}

/// Benchmark client
pub(crate) struct BenchClient {
    /// Name of the client
    name: String,
    /// KV client instance
    kv_client: KVClient,
}

impl Debug for BenchClient {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("name", &self.name)
            .field("kv_client", &self.kv_client)
            .finish()
    }
}

/// transform `PutRequest` into `PutOptions`
fn from_request_to_option(req: &PutRequest) -> PutOptions {
    let mut opts = PutOptions::new().with_lease(req.lease());
    if req.prev_kv() {
        opts = opts.with_prev_key();
    }
    if req.ignore_value() {
        opts = opts.with_ignore_value();
    }
    if req.ignore_lease() {
        opts = opts.with_ignore_lease();
    }
    opts
}

impl BenchClient {
    /// New `Client`
    ///
    /// # Errors
    ///
    /// If `EtcdClient::connect` fails.
    #[inline]
    pub(crate) async fn new(
        addrs: Vec<String>,
        use_curp_client: bool,
        config: ClientOptions,
    ) -> Result<Self> {
        let name = String::from("client");
        let kv_client = if use_curp_client {
            KVClient::Xline(Client::connect(addrs, config).await?)
        } else {
            KVClient::Etcd(EtcdClient::connect(addrs.clone(), None).await?)
        };
        Ok(Self { name, kv_client })
    }

    /// Send `PutRequest` by `XlineClient` or `EtcdClient`
    ///
    /// # Errors
    ///
    /// If `XlineClient` or `EtcdClient` failed to send request
    #[inline]
    pub(crate) async fn put(
        &mut self,
        request: PutRequest,
    ) -> Result<PutResponse, BenchClientError> {
        match self.kv_client {
            KVClient::Xline(ref mut xline_client) => {
                let response = xline_client.kv_client().put(request).await?;
                Ok(response)
            }
            KVClient::Etcd(ref mut etcd_client) => {
                let opts = from_request_to_option(&request);
                let response = etcd_client
                    .put(request.key(), request.value(), Some(opts))
                    .await?;
                Ok(response.into())
            }
        }
    }

    /// Send `RangeRequest` by `XlineClient` or `EtcdClient`
    ///
    /// # Errors
    ///
    /// If `XlineClient` or `EtcdClient` failed to send request
    #[inline]
    #[cfg(test)]
    pub(crate) async fn get(
        &mut self,
        request: RangeRequest,
    ) -> Result<RangeResponse, BenchClientError> {
        match self.kv_client {
            KVClient::Xline(ref mut xline_client) => {
                let response = xline_client.kv_client().range(request).await?;
                Ok(response)
            }
            KVClient::Etcd(ref mut etcd_client) => {
                let response = etcd_client.get(request.key(), None).await?;
                Ok(response.into())
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::indexing_slicing)]
mod test {
    use crate::bench_client::{BenchClient, ClientOptions, PutRequest};
    use xline_client::types::kv::RangeRequest;
    use xline_test_utils::Cluster;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_xline_client() {
        // create xline client
        let mut cluster = Cluster::new(3).await;
        cluster.start().await;
        let use_curp_client = true;
        let config = ClientOptions::default();
        let mut client = BenchClient::new(cluster.addrs(), use_curp_client, config)
            .await
            .unwrap();
        //check xline client put value exist
        let request = PutRequest::new("put", "123");
        let _put_response = client.put(request).await;
        let range_request = RangeRequest::new("put");
        let response = client.get(range_request).await.unwrap();
        assert_eq!(response.kvs[0].value, b"123");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_etcd_client() {
        let mut cluster = Cluster::new(3).await;
        cluster.start().await;
        let use_curp_client = false;
        let config = ClientOptions::default();
        let mut client = BenchClient::new(cluster.addrs(), use_curp_client, config)
            .await
            .unwrap();

        let request = PutRequest::new("put", "123");
        let _put_response = client.put(request).await;
        let range_request = RangeRequest::new("put");
        let response = client.get(range_request).await.unwrap();
        assert_eq!(response.kvs[0].value, b"123");
    }
}
