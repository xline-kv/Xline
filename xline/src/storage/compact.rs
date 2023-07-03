use std::sync::Arc;

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
    mut compact_task_rx: UnboundedReceiver<(i64, Option<Arc<Event>>)>,
) where
    DB: StorageApi,
{
    // TODO: make compact_interval and compact_batch_limit configurable
    let compact_interval = std::time::Duration::from_millis(10);
    let compact_batch_limit = 1000;
    while let Some((revision, listener)) = compact_task_rx.recv().await {
        let target_revisions = index
            .compact(revision)
            .into_iter()
            .map(|key_rev| key_rev.as_revision().encode_to_vec())
            .collect::<Vec<Vec<_>>>();
        for revision_chunk in target_revisions.chunks(compact_batch_limit) {
            if let Err(e) = kv_store.compact(revision_chunk) {
                panic!("failed to compact revision chunk {revision_chunk:?} due to {e}");
            }
            sleep(compact_interval).await;
        }
        if let Some(notifier) = listener {
            notifier.notify(usize::MAX);
        }
    }
}
