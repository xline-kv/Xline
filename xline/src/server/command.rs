use std::sync::Arc;

use prost::Message;
use rpaxos::{error, Command as EpaxosCommand, CommandExecutor as EpaxosCommandExecutor};
use serde::{Deserialize, Serialize};

use crate::rpc::ResponseOp;
use crate::storage::KvStore;

/// Command Executor
#[derive(Debug, Clone)]
pub(crate) struct CommandExecutor {
    /// Kv Storage
    storage: Arc<KvStore>,
}

impl CommandExecutor {
    /// New `CommandExecutor`
    pub(crate) fn new(storage: Arc<KvStore>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl EpaxosCommandExecutor<Command> for CommandExecutor {
    async fn execute(&self, cmd: &Command) -> Result<CommandResponse, error::ExecuteError> {
        let receiver = self.storage.send_req(cmd.clone()).await;
        receiver
            .await
            .or_else(|_| panic!("Failed to receive response from storage"))
    }
}

/// Command to run consensus protocal
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Command {
    /// Key of request
    key: String,
    /// Encoded request data
    request: Vec<u8>,
    /// Propose id
    id: String,
}

impl Command {
    /// New `Command`
    pub(crate) fn new(key: String, request: Vec<u8>, id: String) -> Self {
        Self { key, request, id }
    }

    /*
    /// Get key of `Command`
    pub(crate) fn key(&self) -> &String {
        &self.key
    }
    /// Get request of `Command`
    pub(crate) fn request(&self) -> &Vec<u8> {
        &self.request
    }
    */

    #[allow(dead_code)]
    /// Get id of `Command`
    pub(crate) fn id(&self) -> &String {
        &self.id
    }

    /// Consume `Command` and get ownership of each field
    pub(crate) fn unpack(self) -> (String, Vec<u8>, String) {
        let Self { key, request, id } = self;
        (key, request, id)
    }
}

/// Command to run consensus protocal
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CommandResponse {
    /// Encoded response data
    response: Vec<u8>,
}

impl CommandResponse {
    /// New `ResponseOp` from `CommandResponse`
    pub(crate) fn new(res: &ResponseOp) -> Self {
        Self {
            response: res.encode_to_vec(),
        }
    }

    /// Decode `CommandResponse` and get `ResponseOp`
    pub(crate) fn decode(&self) -> ResponseOp {
        ResponseOp::decode(self.response.as_slice())
            .unwrap_or_else(|e| panic!("Failed to decode CommandResponse, error is {:?}", e))
    }
}

#[async_trait::async_trait]
impl EpaxosCommand for Command {
    type K = String;
    type ER = CommandResponse;

    fn key(&self) -> &Self::K {
        &self.key
    }
}
