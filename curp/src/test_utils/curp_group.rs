use std::collections::HashMap;

use tokio::sync::mpsc;
use utils::config::ClientTimeout;

use crate::{
    client::Client,
    message::ServerId,
    test_utils::test_cmd::{TestCommand, TestCommandResult},
    LogIndex,
};

pub(crate) trait CurpNode {
    fn id(&self) -> ServerId;
    fn addr(&self) -> String;
    fn exe_rx(&mut self) -> &mut mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)>;
    fn as_rx(&mut self) -> &mut mpsc::UnboundedReceiver<(TestCommand, LogIndex)>;
}

pub(crate) struct CurpGroup<T> {
    pub(crate) nodes: HashMap<ServerId, T>,
}

impl<T: CurpNode> CurpGroup<T> {
    pub(crate) async fn new_client(&self, timeout: ClientTimeout) -> Client<TestCommand> {
        let addrs = self
            .nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.addr().clone()))
            .collect();
        Client::<TestCommand>::new(addrs, timeout).await
    }

    pub(crate) fn exe_rxs(
        &mut self,
    ) -> impl Iterator<Item = &mut mpsc::UnboundedReceiver<(TestCommand, TestCommandResult)>> {
        self.nodes.values_mut().map(|node| node.exe_rx())
    }

    pub(crate) fn as_rxs(
        &mut self,
    ) -> impl Iterator<Item = &mut mpsc::UnboundedReceiver<(TestCommand, LogIndex)>> {
        self.nodes.values_mut().map(|node| node.as_rx())
    }
}
