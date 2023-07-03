use std::{sync::Arc, time::Duration};

use event_listener::Event;
use tokio::{sync::mpsc::UnboundedReceiver, time::sleep};

use super::{
    index::{Index, IndexOperate},
    storage_api::StorageApi,
    KvStore,
};

/// background compact executor
pub(crate) async fn compactor<DB>(
    kv_store: Arc<KvStore<DB>>,
    index: Arc<Index>,
    batch_limit: usize,
    interval: Duration,
    mut compact_task_rx: UnboundedReceiver<(i64, Option<Arc<Event>>)>,
) where
    DB: StorageApi,
{
    while let Some((revision, listener)) = compact_task_rx.recv().await {
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
