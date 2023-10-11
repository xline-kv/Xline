use std::fmt::Debug;

use anyhow::Result;
use etcd_client::{Client as EtcdClient, PutOptions};
use thiserror::Error;
use xline::server::Command;
use xline_client::{
    error::XlineClientError as ClientError,
    types::kv::{PutRequest, PutResponse},
    Client, ClientOptions,
};

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

/// Benchmark client
pub(crate) struct BenchClient {
    /// Name of the client
    name: String,
    /// etcd client
    etcd_client: EtcdClient,
    /// xline client
    xline_client: Client,
    /// Use xline client to send requests when true
    use_curp_client: bool,
}

impl Debug for BenchClient {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("name", &self.name)
            .field("use_curp_client", &self.use_curp_client)
            .field("xline_client", &self.xline_client)
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
        Ok(Self {
            name,
            etcd_client: EtcdClient::connect(addrs.clone(), None).await?,
            xline_client: Client::connect(addrs, config).await?,
            use_curp_client,
        })
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
        if self.use_curp_client {
            let response = self.xline_client.kv_client().put(request).await?;
            Ok(response)
        } else {
            let opts = from_request_to_option(&request);
            let response = self
                .etcd_client
                .put(request.key(), request.value(), Some(opts))
                .await?;
            Ok(response.into())
        }
    }
}
