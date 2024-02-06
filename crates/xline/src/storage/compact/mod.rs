use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use curp::client::ClientApi;
use event_listener::Event;
use periodic_compactor::PeriodicCompactor;
use revision_compactor::RevisionCompactor;
use tokio::{sync::mpsc::Receiver, time::sleep};
use utils::{
    config::AutoCompactConfig,
    task_manager::{tasks::TaskName, Listener, TaskManager},
};
use xlineapi::{command::Command, execute_error::ExecuteError, RequestWrapper};

use super::{
    index::{Index, IndexOperate},
    storage_api::StorageApi,
    KvStore,
};
use crate::{revision_number::RevisionNumberGenerator, rpc::CompactionRequest};

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
    async fn run(&self, shutdown_listener: Listener);
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
    /// do compact, return the compacted revision or rpc error
    async fn compact(&self, revision: i64) -> Result<i64, tonic::Status>;
}

#[async_trait]
impl Compactable
    for Arc<dyn ClientApi<Error = tonic::Status, Cmd = Command> + Sync + Send + 'static>
{
    async fn compact(&self, revision: i64) -> Result<i64, tonic::Status> {
        let request = RequestWrapper::from(CompactionRequest {
            revision,
            physical: false,
        });
        let cmd = Command::new(request.keys(), request);
        let err = match self.propose(&cmd, None, true).await? {
            Ok(_) => return Ok(revision),
            Err(err) => err,
        };
        if let ExecuteError::RevisionCompacted(_, compacted_rev) = err {
            return Ok(compacted_rev);
        }
        Err(tonic::Status::from(err))
    }
}

/// Boot up an auto-compactor background task.
pub(crate) async fn auto_compactor<C: Compactable>(
    is_leader: bool,
    revision_getter: Arc<RevisionNumberGenerator>,
    auto_compact_cfg: AutoCompactConfig,
    task_manager: Arc<TaskManager>,
) -> Arc<dyn Compactor<C>> {
    let auto_compactor: Arc<dyn Compactor<C>> = match auto_compact_cfg {
        AutoCompactConfig::Periodic(period) => {
            PeriodicCompactor::new_arc(is_leader, revision_getter, period)
        }
        AutoCompactConfig::Revision(retention) => {
            RevisionCompactor::new_arc(is_leader, revision_getter, retention)
        }
        _ => {
            unreachable!("xline only supports two auto-compaction modes: periodic, revision")
        }
    };
    let compactor_handle = Arc::clone(&auto_compactor);
    task_manager.spawn(TaskName::AutoCompactor, |n| async move {
        auto_compactor.run(n).await;
    });
    compactor_handle
}

/// background compact executor
#[allow(clippy::arithmetic_side_effects)] // introduced bt tokio::select! macro
pub(crate) async fn compact_bg_task<DB>(
    kv_store: Arc<KvStore<DB>>,
    index: Arc<Index>,
    batch_limit: usize,
    interval: Duration,
    mut compact_task_rx: Receiver<(i64, Option<Arc<Event>>)>,
    shutdown_listener: Listener,
) where
    DB: StorageApi,
{
    loop {
        let (revision, listener) = tokio::select! {
            recv = compact_task_rx.recv() => {
                let Some((revision, listener)) = recv else {
                    return;
                };
                (revision, listener)
            },
            _ = shutdown_listener.wait() => break,
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
        if let Err(e) = kv_store.compact_finished(revision) {
            panic!("failed to set finished compact revision {revision:?} due to {e}");
        }
        if let Some(notifier) = listener {
            notifier.notify(usize::MAX);
        }
    }
}
