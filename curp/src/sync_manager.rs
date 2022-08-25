use std::{iter, sync::Arc, time::Duration};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use tracing::{error, warn};

use crate::{
    channel::{key_mpsc::MpscKeybasedReceiver, key_spmc::SpmcKeybasedSender},
    cmd::Command,
    log::{EntryStatus, LogEntry},
    message::TermNum,
    rpc::{self, sync_response::SyncResponse, Connect, SyncRequest},
    util::MutexMap,
    LogIndex,
};

/// Sync request default timeout
static SYNC_TIMEOUT: Duration = Duration::from_secs(1);

/// "sync task complete" message
pub(crate) struct SyncCompleteMessage {
    /// The log index
    log_index: LogIndex,
}

impl SyncCompleteMessage {
    /// Create a new `SyncCompleteMessage`
    fn new(log_index: LogIndex) -> Self {
        Self { log_index }
    }

    /// Get Log Index
    pub(crate) fn log_index(&self) -> u64 {
        self.log_index
    }
}

/// The message sent to the `SyncManager`
pub(crate) struct SyncMessage<C>
where
    C: Command,
{
    /// Term number
    term: TermNum,
    /// Command, may be taken out
    cmd: Option<Arc<C>>,
}

impl<C> SyncMessage<C>
where
    C: Command,
{
    /// Create a new `SyncMessage`
    pub(crate) fn new(term: TermNum, cmd: C) -> Self {
        Self {
            term,
            cmd: Some(Arc::new(cmd)),
        }
    }

    /// Take all values from the message
    ///
    /// # Panic
    /// If the function is called more than once, it will panic
    #[allow(clippy::expect_used)]
    fn take_all(&mut self) -> (TermNum, Arc<C>) {
        (
            self.term,
            self.cmd.take().expect("cmd should only be taken once"),
        )
    }
}

/// The manager to sync commands to other follower servers
pub(crate) struct SyncManager<C: Command + 'static> {
    /// Other addrs
    connets: Vec<Connect>,
    /// Get cmd sync request from speculative command
    sync_chan: MpscKeybasedReceiver<C::K, SyncMessage<C>>,
    /// Send cmd to sync complete handler
    comp_chan: SpmcKeybasedSender<C::K, SyncCompleteMessage>,
    /// Consensus log
    log: Arc<Mutex<Vec<LogEntry<C>>>>,
}

impl<C: Command + 'static> SyncManager<C> {
    /// Create a `SyncedManager`
    pub(crate) async fn new(
        sync_chan: MpscKeybasedReceiver<C::K, SyncMessage<C>>,
        comp_chan: SpmcKeybasedSender<C::K, SyncCompleteMessage>,
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

    /// Run the `SyncManager`
    pub(crate) async fn run(&mut self) {
        let max_fail = self.connets.len().wrapping_div(2);

        loop {
            let sync_msg = match self.sync_chan.async_recv().await {
                Ok(sync_msg) => sync_msg,
                Err(_) => return,
            };
            let (term, cmd) = sync_msg.map_msg(SyncMessage::take_all);

            let index = self.log.map_lock(|mut log| {
                log.push(LogEntry::new(term, (*cmd).clone(), EntryStatus::Unsynced));
                // length must be larger than 1
                log.len().wrapping_sub(1)
            });

            let rpcs = self
                .connets
                .iter()
                .zip(iter::repeat_with(|| Arc::clone(&cmd)))
                .map(|(connect, cmd_cloned)| async move {
                    connect
                        .sync(
                            SyncRequest::new(term, index.numeric_cast(), &cmd_cloned)?,
                            SYNC_TIMEOUT,
                        )
                        .await
                });

            let mut rpcs: FuturesUnordered<_> = rpcs.collect();
            let mut synced_cnt: usize = 0;

            while let Some(resp) = rpcs.next().await {
                let _result = resp
                    .map_err(|err| {
                        warn!("rpc error when sending `Sync` request, {err}");
                    })
                    .map(|r| {
                        match r.into_inner().sync_response {
                            Some(SyncResponse::Synced(_)) => {
                                synced_cnt = synced_cnt.overflow_add(1);
                            }
                            Some(
                                SyncResponse::WrongTerm(_)
                                | SyncResponse::EntryNotEmpty(_)
                                | SyncResponse::PrevNotReady(_),
                            ) => {
                                // todo
                            }
                            None => unreachable!("Should contain sync response"),
                        }
                    });

                if synced_cnt > max_fail {
                    if self
                        .comp_chan
                        .send(cmd.keys(), SyncCompleteMessage::new(index.numeric_cast()))
                        .is_err()
                    {
                        error!("The comp_chan is closed on the remote side");
                        return;
                    }
                    break;
                }
            }
        }
    }
}
