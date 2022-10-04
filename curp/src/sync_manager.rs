use std::{iter, sync::Arc, time::Duration};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use futures::{stream::FuturesUnordered, Future, StreamExt};
use parking_lot::Mutex;
use tracing::{error, warn};

use crate::{
    channel::{key_mpsc::MpscKeybasedReceiver, key_spmc::SpmcKeybasedSender, RecvError},
    cmd::Command,
    error::ProposeError,
    log::{EntryStatus, LogEntry},
    message::TermNum,
    rpc::{self, sync_response::SyncResponse, Connect, SyncRequest},
    util::MutexMap,
    LogIndex,
};

/// Sync request default timeout
static SYNC_TIMEOUT: Duration = Duration::from_secs(1);

/// "sync task complete" message
pub(crate) struct SyncCompleteMessage<C>
where
    C: Command,
{
    /// The log index
    log_index: LogIndex,
    /// Command
    cmd: Arc<C>,
}

impl<C> SyncCompleteMessage<C>
where
    C: Command,
{
    /// Create a new `SyncCompleteMessage`
    fn new(log_index: LogIndex, cmd: Arc<C>) -> Self {
        Self { log_index, cmd }
    }

    /// Get Log Index
    pub(crate) fn log_index(&self) -> u64 {
        self.log_index
    }

    /// Get commands
    pub(crate) fn cmd(&self) -> Arc<C> {
        Arc::clone(&self.cmd)
    }
}

/// The message sent to the `SyncManager`
pub(crate) struct SyncMessage<C>
where
    C: Command,
{
    /// Term number
    term: TermNum,
    /// Command
    cmd: Arc<C>,
}

impl<C> SyncMessage<C>
where
    C: Command,
{
    /// Create a new `SyncMessage`
    pub(crate) fn new(term: TermNum, cmd: Arc<C>) -> Self {
        Self { term, cmd }
    }

    /// Get all values from the message
    fn inner(&mut self) -> (TermNum, Arc<C>) {
        (self.term, Arc::clone(&self.cmd))
    }
}

/// The manager to sync commands to other follower servers
pub(crate) struct SyncManager<C: Command + 'static> {
    /// Other addrs
    connets: Vec<Arc<Connect>>,
    /// Get cmd sync request from speculative command
    sync_chan: MpscKeybasedReceiver<C::K, SyncMessage<C>>,
    /// Send cmd to sync complete handler
    comp_chan: SpmcKeybasedSender<C::K, SyncCompleteMessage<C>>,
    /// Consensus log
    log: Arc<Mutex<Vec<LogEntry<C>>>>,
}

impl<C: Command + 'static> SyncManager<C> {
    /// Create a `SyncedManager`
    pub(crate) async fn new(
        sync_chan: MpscKeybasedReceiver<C::K, SyncMessage<C>>,
        comp_chan: SpmcKeybasedSender<C::K, SyncCompleteMessage<C>>,
        others: Vec<String>,
        log: Arc<Mutex<Vec<LogEntry<C>>>>,
    ) -> Self {
        Self {
            sync_chan,
            comp_chan,
            log,
            connets: rpc::try_connect(others.into_iter().map(|a| format!("http://{a}")).collect())
                .await,
        }
    }

    /// Try to receive all the messages in `sync_chan`, preparing for the batch sync.
    /// The term number comes from the command received.
    // TODO: set a maximum value
    async fn fetch_sync_msgs(&mut self) -> Result<(Vec<Arc<C>>, u64), ()> {
        let mut term = 0;
        let mut met_msg = false;
        let mut cmds = vec![];
        loop {
            let sync_msg = match self.sync_chan.try_recv() {
                Ok(sync_msg) => sync_msg,
                Err(RecvError::ChannelStop) => return Err(()),
                Err(RecvError::NoAvailable) => {
                    if met_msg {
                        break;
                    }

                    match self.sync_chan.async_recv().await {
                        Ok(msg) => msg,
                        Err(_) => return Err(()),
                    }
                }
                Err(RecvError::Timeout) => unreachable!("try_recv won't return timeout error"),
            };
            met_msg = true;
            let (t, cmd) = sync_msg.map_msg(SyncMessage::inner);
            term = t;
            cmds.push(cmd);
        }
        Ok((cmds, term))
    }

    /// Broadcast the request via `send_fn`, handle each response with `resp_fn` and wait at least
    /// `wait_cnt + 1` response, then finally call `comp_fn` to complete .
    ///
    /// Return true if the broadcast success, otherwise return false.
    async fn broadcast_and_wait<I, SendFn, R, SendRet, RespFn, CompFn>(
        &self,
        args: I,
        send_fn: SendFn,
        resp_fn: RespFn,
        comp_fn: CompFn,
        wait_cnt: usize,
    ) -> bool
    where
        I: IntoIterator,
        SendRet: Future<Output = Result<tonic::Response<R>, ProposeError>>,
        SendFn: Fn((Arc<Connect>, I::Item)) -> SendRet,
        RespFn: Fn(tonic::Response<R>) -> usize,
        CompFn: Fn() -> bool,
    {
        let rpcs = self
            .connets
            .iter()
            .cloned()
            .zip(args.into_iter())
            .map(send_fn);

        let mut rpcs: FuturesUnordered<_> = rpcs.collect();
        let mut synced_cnt: usize = 0;

        while let Some(resp) = rpcs.next().await {
            let _result = resp
                .map_err(|err| {
                    warn!("rpc error when sending `Sync` request, {err}");
                })
                .map(|r| {
                    synced_cnt = synced_cnt.overflow_add(resp_fn(r));
                });

            if synced_cnt > wait_cnt {
                if comp_fn() {
                    return true;
                }
                // TODO: collect unfinished tasks
                break;
            }
        }

        false
    }

    /// Run the `SyncManager`
    pub(crate) async fn run(&mut self) {
        let max_fail = self.connets.len().wrapping_div(2);

        loop {
            let (cmds, term) = match self.fetch_sync_msgs().await {
                Ok((cmds, term)) => (cmds, term),
                Err(_) => return,
            };

            let index = self.log.map_lock(|mut log| {
                let len = log.len();
                log.push(LogEntry::new(term, &cmds, EntryStatus::Unsynced));
                len
            });
            let cmds_arc: Arc<[_]> = cmds.into();

            let comp_fn = || {
                for c in cmds_arc.iter() {
                    if self
                        .comp_chan
                        .send(
                            c.keys(),
                            SyncCompleteMessage::new(index.numeric_cast(), Arc::clone(c)),
                        )
                        .is_err()
                    {
                        error!("The comp_chan is closed on the remote side");
                        return false;
                    }
                }
                true
            };

            if !self
                .broadcast_and_wait(
                    iter::repeat_with(|| Arc::clone(&cmds_arc)),
                    |(connect, cmds_cloned)| async move {
                        connect
                            .sync(
                                SyncRequest::new(term, index.numeric_cast(), cmds_cloned.as_ref())?,
                                SYNC_TIMEOUT,
                            )
                            .await
                    },
                    |r| match r.into_inner().sync_response {
                        Some(SyncResponse::Synced(_)) => 1,
                        Some(
                            SyncResponse::WrongTerm(_)
                            | SyncResponse::EntryNotEmpty(_)
                            | SyncResponse::PrevNotReady(_),
                        ) => 0,
                        None => unreachable!("Should contain sync response"),
                    },
                    comp_fn,
                    max_fail,
                )
                .await
            {
                return;
            }
        }
    }
}
