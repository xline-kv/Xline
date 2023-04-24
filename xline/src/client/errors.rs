use thiserror::Error;

/// Client Error
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// Error from `etcd_client`
    #[error("etcd_client error {0}")]
    EtcdError(String),
    /// Propose error
    #[error("propose error {0}")]
    ProposeError(#[from] curp::error::ProposeError),
    /// IO error
    #[error("IO error {0}")]
    IoError(#[from] std::io::Error),
    /// Engine error
    #[error("Engine error {0}")]
    EngineError(#[from] engine::EngineError),
}

impl From<etcd_client::Error> for ClientError {
    #[inline]
    fn from(err: etcd_client::Error) -> Self {
        ClientError::EtcdError(err.to_string())
    }
}
