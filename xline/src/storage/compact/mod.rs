use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use curp::{
    client::Client,
    cmd::generate_propose_id,
    error::CommandProposeError::{AfterSync, Execute},
};
use event_listener::Event;
use periodic_compactor::PeriodicCompactor;
use revision_compactor::RevisionCompactor;
use tokio::{sync::mpsc::Receiver, time::sleep};
use utils::config::AutoCompactConfig;
use utils::shutdown;

use super::{
    index::{Index, IndexOperate},
    storage_api::StorageApi,
    ExecuteError, KvStore,
};
use crate::{
    revision_number::RevisionNumberGenerator,
    rpc::{CompactionRequest, RequestWithToken},
    server::command::Command,
};

/// mod revision compactor;
mod revision_compactor;

/// mod periodic compactor;
mod periodic_compactor;

/// compact task channel size
pub(crate) const COMPACT_CHANNEL_SIZE: usize = 32;

/// Compactor trait definition
#[async_trait]
pub(crate) trait Compactor: std::fmt::Debug + Send + Sync {
    /// run an auto-compactor
    async fn run(&self);
    /// pause an auto-compactor when the current node denotes to a non-leader role
    fn pause(&self);
    /// resume an auto-compactor when the current becomes a leader
    fn resume(&self);
}

/// `Compactable` trait indicates a method that receives a given revision and proposes a compact proposal
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub(crate) trait Compactable: std::fmt::Debug + Send + Sync {
    /// do compact
    async fn compact(&self, revision: i64) -> Result<(), ExecuteError>;
}

#[async_trait]
impl Compactable for Client<Command> {
    async fn compact(&self, revision: i64) -> Result<(), ExecuteError> {
        let request = CompactionRequest {
            revision,
            physical: false,
        };
        let request_wrapper = RequestWithToken::new_with_token(request.into(), None);
        let propose_id = generate_propose_id("auto-compactor");
        let cmd = Command::new(vec![], request_wrapper, propose_id);
        if let Err(e) = self.propose(cmd, true).await {
            #[allow(clippy::wildcard_enum_match_arm)]
            match e {
                Execute(e) | AfterSync(e) => Err(e),
                _ => {
                    unreachable!("Compaction should not receive any errors other than ExecuteError, but it receives {e:?}");
                }
            }
        } else {
            Ok(())
        }
    }
}

/// Boot up an auto-compactor background task.
pub(crate) async fn auto_compactor(
    is_leader: bool,
    client: Arc<Client<Command>>,
    revision_getter: Arc<RevisionNumberGenerator>,
    shutdown_listener: shutdown::Listener,
    auto_compact_cfg: AutoCompactConfig,
) -> Arc<dyn Compactor> {
    let auto_compactor: Arc<dyn Compactor> = match auto_compact_cfg {
        AutoCompactConfig::Periodic(period) => PeriodicCompactor::new_arc(
            is_leader,
            client,
            revision_getter,
            shutdown_listener,
            period,
        ),
        AutoCompactConfig::Revision(retention) => RevisionCompactor::new_arc(
            is_leader,
            client,
            revision_getter,
            shutdown_listener,
            retention,
        ),
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
