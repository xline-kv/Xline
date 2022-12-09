use crate::rpc::RequestWrapper;

/// `RequestWrapper` with `met_err`
#[derive(Debug)]
pub(crate) struct RequestCtx {
    /// Request
    req: RequestWrapper,
    /// if error occurs in execute
    met_err: bool,
}

impl RequestCtx {
    /// New `RequestCtx`
    pub(crate) fn new(req: RequestWrapper, met_err: bool) -> Self {
        Self { req, met_err }
    }

    /// Met error
    pub(crate) fn met_err(&self) -> bool {
        self.met_err
    }

    /// Request
    pub(crate) fn req(self) -> RequestWrapper {
        self.req
    }
}
