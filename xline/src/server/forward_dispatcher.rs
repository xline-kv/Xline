use std::{collections::HashMap, sync::Arc, time::Duration};

use super::command::Command;
use curp::{
    client::{Client, ClientPool},
    cmd::{Command as CurpCommand, ProposeId},
    error::CommandProposeError,
};
use event_listener::Event;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    oneshot::Sender,
};
use futures::StreamExt;
/// The propose result
pub(super) type ProposeResult<Command> = Result<
    (
        <Command as CurpCommand>::ER,
        Option<<Command as CurpCommand>::ASR>,
    ),
    CommandProposeError<Command>,
>;

/// The proposal batch type alias
type ProposalBatch = (
    Vec<Command>,
    HashMap<ProposeId, Sender<ProposeResult<Command>>>,
    bool,
);

/// forward batch limit
const FORWARD_BATCH_LIMIT: usize = 10;
/// forward batch timeout
const FORWARD_BATCH_TIMEOUT: Duration = Duration::from_millis(8);

/// Batch collector
struct BatchCollector {
    /// batch collector to store the (cmd, sender) pair
    batch_collector: Vec<Command>,
    /// sender table
    cmd_sender_table: HashMap<ProposeId, Sender<ProposeResult<Command>>>,
    /// The max length of a batch
    batch_limit: usize,
    /// current batch flag
    batch_flag: bool,
}

impl BatchCollector {
    /// Create a new batch generator
    fn new(batch_limit: usize) -> Self {
        Self {
            batch_collector: Vec::with_capacity(batch_limit),
            cmd_sender_table: HashMap::with_capacity(batch_limit),
            batch_limit,
            batch_flag: false,
        }
    }

    /// insert a command into the `BatchCollector`
    fn insert_cmd(
        &mut self,
        cmd: Command,
        use_fast_path: bool,
        sender: Sender<ProposeResult<Command>>,
    ) -> Option<ProposalBatch> {
        if self.batch_collector.is_empty() {
            self.batch_flag = use_fast_path;
        }
        if self.batch_flag == use_fast_path {
            let id = cmd.id().clone();
            self.batch_collector.push(cmd);
            assert!(
                self.cmd_sender_table.insert(id, sender).is_none(),
                "cmd cannot be inserted twice"
            );
            (self.batch_collector.len() >= self.batch_limit).then(|| {
                (
                    self.batch_collector.drain(..).collect(),
                    self.cmd_sender_table.drain().collect(),
                    self.batch_flag,
                )
            })
        } else {
            let batch_flag = self.batch_flag;
            let batch: Vec<Command> = self.batch_collector.drain(..).collect();
            let cmd_sender_table: HashMap<ProposeId, Sender<ProposeResult<Command>>> =
                self.cmd_sender_table.drain().collect();
            let id = cmd.id().clone();
            self.batch_collector.push(cmd);
            assert!(
                self.cmd_sender_table.insert(id, sender).is_none(),
                "cmd cannot be inserted twice"
            );
            self.batch_flag = use_fast_path;
            Some((batch, cmd_sender_table, batch_flag))
        }
    }

    /// fetch a batch from the `BatchCollector`
    fn fetch_batch(&mut self) -> Option<ProposalBatch> {
        (!self.batch_collector.is_empty()).then(|| {
            (
                self.batch_collector.drain(..).collect(),
                self.cmd_sender_table.drain().collect(),
                self.batch_flag,
            )
        })
    }
}

/// Spawn a `forward_dispatcher` task
pub(super) async fn forward_dispatcher(
    client_pool: ClientPool<Command>,
    shutdown_trigger: Arc<Event>,
) -> UnboundedSender<(Command, bool, Sender<ProposeResult<Command>>)> {
    let (tx, mut rx) = unbounded_channel::<(Command, bool, Sender<ProposeResult<Command>>)>();
    let _handle = tokio::spawn(async move {
        let shutdown_trigger = shutdown_trigger.listen();
        let mut batch_collector = BatchCollector::new(FORWARD_BATCH_LIMIT);
        tokio::pin!(shutdown_trigger);
        #[allow(clippy::integer_arithmetic, clippy::unwrap_used)]
        loop {
            tokio::select! {
                _ = &mut shutdown_trigger => {
                    if let Some((final_batch, cmd_sender_table, final_flag)) = batch_collector.fetch_batch() {
                        let client = client_pool.get_client();
                        handle_forward(client, final_batch, cmd_sender_table, final_flag).await;
                    }
                    return;
                }
                res = tokio::time::timeout(FORWARD_BATCH_TIMEOUT, rx.recv()) => {
                    let batch = if let Ok(Some((cmd, fast_path, sender))) =  res {
                        batch_collector.insert_cmd(cmd, fast_path, sender)
                    } else {
                        batch_collector.fetch_batch()
                    };
                    if let Some((forward_batch, cmd_sender_table, forward_flag)) = batch {
                        let client = client_pool.get_client();
                        let _handle = tokio::spawn(handle_forward(client, forward_batch, cmd_sender_table, forward_flag));
                    }
                }
            }
        }
    });
    tx
}

/// handle a forward task
async fn handle_forward(
    client: Arc<Client<Command>>,
    cmd_batch: Vec<Command>,
    mut cmd_sender_table: HashMap<ProposeId, Sender<ProposeResult<Command>>>,
    fast_path: bool,
) {
    let cmds = Arc::new(cmd_batch);
    let mut propose_res = client.propose_batch(cmds, fast_path).await;
    while let Some((id, res)) = propose_res.next().await {
        if let Some(sender) = cmd_sender_table.remove(&id) {
            if let Err(_) = sender.send(res) {
                unreachable!("the receiver dropped");
            }
        } else {
            continue;
        }
    }
}

#[cfg(test)]
mod test {
    use curp::cmd::ProposeId;
    use xlineapi::{PutRequest, RequestWithToken, RequestWrapper};

    use super::*;
    use crate::server::KeyRange;
    #[test]
    fn batch_should_be_collected_when_its_length_reach_the_limit() {
        let mut batch_collector = BatchCollector::new(2);
        let (tx_1, _rx) = tokio::sync::oneshot::channel::<ProposeResult<Command>>();
        let (tx_2, _rx) = tokio::sync::oneshot::channel::<ProposeResult<Command>>();
        let cmd_1 = Command::new(
            vec![KeyRange::new("a", "e")],
            RequestWithToken::new(RequestWrapper::PutRequest(PutRequest::default())),
            ProposeId::from("cmd_1"),
        );
        assert!(batch_collector
            .insert_cmd(cmd_1.clone(), true, tx_1)
            .is_none());
        let cmd_2 = Command::new(
            vec![KeyRange::new("b", "f")],
            RequestWithToken::new(RequestWrapper::PutRequest(PutRequest::default())),
            ProposeId::from("cmd_2"),
        );
        assert!(batch_collector.insert_cmd(cmd_2, true, tx_2).is_some());
    }

    #[test]
    fn batch_should_be_collected_when_insert_a_different_flag() {
        let mut batch_collector = BatchCollector::new(2);
        let (tx_1, _rx) = tokio::sync::oneshot::channel::<ProposeResult<Command>>();
        let (tx_2, _rx) = tokio::sync::oneshot::channel::<ProposeResult<Command>>();
        let cmd = Command::new(
            vec![KeyRange::new("a", "e")],
            RequestWithToken::new(RequestWrapper::PutRequest(PutRequest::default())),
            ProposeId::from("id"),
        );
        assert!(batch_collector
            .insert_cmd(cmd.clone(), true, tx_1)
            .is_none());
        let (batch, _sender_table, flag) = batch_collector.insert_cmd(cmd, false, tx_2).unwrap();
        assert_eq!(batch.len(), 1);
        assert!(flag);
    }
}
