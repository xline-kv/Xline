use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use curp::{client::ClientApi, error::ClientError};
use event_listener::Event;
use periodic_compactor::PeriodicCompactor;
use revision_compactor::RevisionCompactor;
use tokio::{sync::mpsc::Receiver, time::sleep};
use utils::{config::AutoCompactConfig, shutdown};
use xlineapi::command::Command;

use super::{
    index::{Index, IndexOperate},
    storage_api::StorageApi,
    KvStore,
};
use crate::{
    revision_number::RevisionNumberGenerator,
    rpc::{CompactionRequest, RequestWithToken},
};

/// mod revision compactor;
mod revision_compactor;

/// mod periodic compactor;
mod periodic_compactor;

/// compact task channel size
pub(crate) const COMPACT_CHANNEL_SIZE: usize = 32;

/// Compactor trait definition
#[async_trait]
pub(crate) trait Compactor<C: Compactable>: Send + Sync {
    /// run an auto-compactor
    async fn run(&self);
    /// pause an auto-compactor when the current node denotes to a non-leader role
    fn pause(&self);
    /// resume an auto-compactor when the current becomes a leader
    fn resume(&self);
    /// Set compactable
    async fn set_compactable(&self, c: C);
}

/// `Compactable` trait indicates a method that receives a given revision and proposes a compact proposal
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub(crate) trait Compactable: Send + Sync + 'static {
    /// do compact
    async fn compact(&self, revision: i64) -> Result<(), ClientError<Command>>;
}

#[async_trait]
impl Compactable
    for Arc<dyn ClientApi<Error = tonic::Status, Cmd = Command> + Sync + Send + 'static>
{
    async fn compact(&self, revision: i64) -> Result<(), ClientError<Command>> {
        let request = CompactionRequest {
            revision,
            physical: false,
        };
        let request_wrapper = RequestWithToken::new_with_token(request.into(), None);
        let cmd = Command::new(vec![], request_wrapper);
        let _ig = self.propose(&cmd, true).await?;
        Ok(())
    }
}

/// Boot up an auto-compactor background task.
pub(crate) async fn auto_compactor<C: Compactable>(
    is_leader: bool,
    revision_getter: Arc<RevisionNumberGenerator>,
    shutdown_listener: shutdown::Listener,
    auto_compact_cfg: AutoCompactConfig,
) -> Arc<dyn Compactor<C>> {
    let auto_compactor: Arc<dyn Compactor<C>> = match auto_compact_cfg {
        AutoCompactConfig::Periodic(period) => {
            PeriodicCompactor::new_arc(is_leader, revision_getter, shutdown_listener, period)
        }
        AutoCompactConfig::Revision(retention) => {
            RevisionCompactor::new_arc(is_leader, revision_getter, shutdown_listener, retention)
        }
        _ => {
            unreachable!("xline only supports two auto-compaction modes: periodic, revision")
        }
    };
    let compactor_handle = Arc::clone(&auto_compactor);
    let _hd = tokio::spawn(async move {
        auto_compactor.run().await;
    });
    compactor_handle
}

/// background compact executor
#[allow(clippy::integer_arithmetic)] // introduced bt tokio::select! macro
pub(crate) async fn compact_bg_task<DB>(
    kv_store: Arc<KvStore<DB>>,
    index: Arc<Index>,
    batch_limit: usize,
    interval: Duration,
    mut compact_task_rx: Receiver<(i64, Option<Arc<Event>>)>,
    mut shutdown_listener: shutdown::Listener,
) where
    DB: StorageApi,
{
    loop {
        let (revision, listener) = tokio::select! {
            recv = compact_task_rx.recv() => {
                if let Some((revision, listener)) = recv {
                    (revision, listener)
                } else {
                    break;
                }
            }
            _ = shutdown_listener.wait_self_shutdown() => {
                break;
            }
        };

        let target_revisions = index
            .compact(revision)
            .into_iter()
            .map(|key_rev| key_rev.as_revision().encode_to_vec())
            .collect::<Vec<Vec<_>>>();
        // Given that the Xline uses a lim-tree database with smaller write amplification as the storage backend ,  does using progressive compaction really good at improving performance?
        for revision_chunk in target_revisions.chunks(batch_limit) {
            if let Err(e) = kv_store.compact(revision_chunk) {
                panic!("failed to compact revision chunk {revision_chunk:?} due to {e}");
            }
            sleep(interval).await;
        }
        if let Some(notifier) = listener {
            notifier.notify(usize::MAX);
        }
    }
}
