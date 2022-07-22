use std::{net::SocketAddr, sync::Arc, time::Duration};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use futures::{stream::FuturesUnordered, StreamExt};
use madsim::net::Endpoint;
use parking_lot::{Mutex, RwLock};
use tokio::sync::oneshot;
use tracing::warn;

use crate::{
    cmd::Command,
    keybased_channel::{KeybasedChannelReceiver, KeysMessage},
    log::{EntryStatus, LogEntry},
    message::{SyncCommand, SyncResponse, TermNum},
    util::{MutexMap, RwLockMap},
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
    /// The keys message
    keys_msg: KeysMessage<C::K, SyncMessage<C>>,
}

impl<C> SyncCompleteMessage<C>
where
    C: Command,
{
    /// Create a new `SyncCompleteMessage`
    fn new(log_index: LogIndex, keys_msg: KeysMessage<C::K, SyncMessage<C>>) -> Self {
        Self {
            log_index,
            keys_msg,
        }
    }

    /// Get Log Index
    pub(crate) fn log_index(&self) -> u64 {
        self.log_index
    }

    /// Get the `KeysMessage`
    pub(crate) fn keys_msg(&self) -> KeysMessage<C::K, SyncMessage<C>> {
        self.keys_msg.clone()
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
    cmd: Option<Box<C>>,
    /// The sync task compelte hook, may be taken out
    sync_comp: Option<oneshot::Sender<SyncCompleteMessage<C>>>,
}

impl<C> SyncMessage<C>
where
    C: Command,
{
    /// Create a new `SyncMessage`
    pub(crate) fn new(
        term: TermNum,
        cmd: C,
        sync_comp: oneshot::Sender<SyncCompleteMessage<C>>,
    ) -> Self {
        Self {
            term,
            cmd: Some(Box::new(cmd)),
            sync_comp: Some(sync_comp),
        }
    }

    /// Take all values from the message
    ///
    /// # Panic
    /// If the function is called more than once, it will panic
    #[allow(clippy::expect_used)]
    fn take_all(&mut self) -> (TermNum, Box<C>, oneshot::Sender<SyncCompleteMessage<C>>) {
        (
            self.term,
            self.cmd.take().expect("cmd should only be taken once"),
            self.sync_comp
                .take()
                .expect("sync_comp should only be taken once"),
        )
    }
}

/// The manager to sync commands to other follower servers
pub(crate) struct SyncManager<C: Command + 'static> {
    /// The endpoint to call rpc to other servers
    ep: Endpoint,
    /// Get cmd sync request from speculative command
    sync_chan: KeybasedChannelReceiver<C::K, SyncMessage<C>>,
    /// Other server address
    others: Arc<RwLock<Vec<SocketAddr>>>,
    /// Consensus log
    log: Arc<Mutex<Vec<LogEntry<C>>>>,
}

impl<C: Command + 'static> SyncManager<C> {
    /// Create a `SyncedManager`
    pub(crate) fn new(
        ep: Endpoint,
        cmd_chan: KeybasedChannelReceiver<C::K, SyncMessage<C>>,
        others: Arc<RwLock<Vec<SocketAddr>>>,
        log: Arc<Mutex<Vec<LogEntry<C>>>>,
    ) -> Self {
        Self {
            ep,
            sync_chan: cmd_chan,
            others,
            log,
        }
    }

    /// Run the `SyncManager`
    pub(crate) async fn run(&mut self) {
        let f = self.others.read().len().wrapping_div(2);

        loop {
            let sync_msg = self.sync_chan.async_recv().await;
            let (term, cmd, sync_comp) =
                if let Some(real_msg) = sync_msg.map_msg(SyncMessage::take_all) {
                    real_msg
                } else {
                    // FIXME: should panic here?
                    warn!("empty sync message, should not happen");
                    continue;
                };

            let others: Vec<SocketAddr> = self
                .others
                .map_read(|others| others.iter().copied().collect());

            let index = self.log.map_lock(|mut log| {
                log.push(LogEntry::new(term, *(cmd.clone()), EntryStatus::Unsynced));
                // length must be larger than 1
                log.len().wrapping_sub(1)
            });

            let rpcs = others.iter().map(|addr| {
                self.ep.call_timeout(
                    *addr,
                    SyncCommand::new(term, index.numeric_cast(), *(cmd.clone())),
                    SYNC_TIMEOUT,
                )
            });

            let mut rpcs: FuturesUnordered<_> = rpcs.collect();
            let mut synced_cnt: usize = 0;

            while let Some(resp) = rpcs.next().await {
                let _result = resp
                    .map_err(|err| {
                        warn!("rpc error when sending `Sync` request, {err}");
                    })
                    .map(|r| {
                        match r {
                            SyncResponse::Synced => {
                                synced_cnt = synced_cnt.overflow_add(1);
                            }
                            SyncResponse::WrongTerm(_)
                            | SyncResponse::EntryNotEmpty(_)
                            | SyncResponse::PrevNotReady(_) => {
                                // todo
                            }
                        }
                    });

                if synced_cnt == f {
                    let _r =
                        sync_comp.send(SyncCompleteMessage::new(index.numeric_cast(), sync_msg));
                    break;
                }
            }
        }
    }
}
