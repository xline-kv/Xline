use thiserror::Error;

/// Client Error
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// Error from `etcd_client`
    #[error("etcd_client err {0}")]
    EtcdError(String),
    /// Response parse error
    #[error("response parse error {0}")]
    ParseError(String),
    /// Propose error
    #[error("propose error {0}")]
    ProposeError(#[from] curp::error::ProposeError),
}

impl From<etcd_client::Error> for ClientError {
    #[inline]
    fn from(err: etcd_client::Error) -> Self {
        ClientError::EtcdError(err.to_string())
    }
}
