use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use chrono::Local;
use engine::{
    engine_api::SnapshotApi,
    rocksdb_engine::{RocksEngine, RocksSnapshot},
    StorageEngine, WriteOperation,
};
use uuid::Uuid;

use super::{StorageApi, StorageError};
use crate::{cmd::Command, log_entry::LogEntry, ServerId};

/// Key for persisted state
const VOTE_FOR: &[u8] = b"VoteFor";

/// Column family name for curp storage
const CF: &str = "curp";

/// `RocksDB` storage implementation
pub(in crate::server) struct RocksDBStorage<C> {
    /// DB handle
    db: RocksEngine,
    /// Storage Path
    data_dir: PathBuf,
    /// Phantom
    phantom: PhantomData<C>,
}

#[async_trait]
impl<C: 'static + Command> StorageApi for RocksDBStorage<C> {
    /// Command
    type Command = C;

    async fn flush_voted_for(&self, term: u64, voted_for: ServerId) -> Result<(), StorageError> {
        let bytes = bincode::serialize(&(term, voted_for))?;
        let op = WriteOperation::new_put(CF, VOTE_FOR.to_vec(), bytes);
        self.db.write_batch(vec![op], true)?;

        Ok(())
    }

    async fn put_log_entry(&self, entry: LogEntry<Self::Command>) -> Result<(), StorageError> {
        let bytes = bincode::serialize(&entry)?;
        let op = WriteOperation::new_put(CF, entry.index.to_be_bytes().to_vec(), bytes);
        self.db.write_batch(vec![op], false)?;

        Ok(())
    }

    async fn recover(
        &self,
    ) -> Result<(Option<(u64, ServerId)>, Vec<LogEntry<Self::Command>>), StorageError> {
        let voted_for = self
            .db
            .get(CF, VOTE_FOR)?
            .map(|bytes| bincode::deserialize::<(u64, ServerId)>(&bytes))
            .transpose()?;

        let mut entries = vec![];
        let mut prev_index = 0;
        for (k, v) in self.db.get_all(CF)? {
            // we can identify whether a kv is state or entry by the key length
            if k.len() == VOTE_FOR.len() {
                continue;
            }
            let entry: LogEntry<C> = bincode::deserialize(&v)?;
            #[allow(clippy::integer_arithmetic)] // won't overflow
            if entry.index != prev_index + 1 {
                // break when logs are no longer consistent
                break;
            }
            prev_index = entry.index;
            entries.push(entry);
        }

        Ok((voted_for, entries))
    }

    async fn new_snapshot(&self) -> Result<Box<dyn SnapshotApi>, StorageError> {
        // TODO: delete outdated snapshot
        // TODO: better snapshot file naming
        let dir = self
            .data_dir
            .join(format!("snapshot-{}-{}", Local::now(), Uuid::new_v4()));
        let snapshot = RocksSnapshot::new_for_receiving(dir)?;
        Ok(Box::new(snapshot))
    }
}

impl<C> RocksDBStorage<C> {
    /// Create a new `RocksDBStorage`
    pub(in crate::server) fn new(dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db = RocksEngine::new(dir.as_ref(), &[CF])?;
        Ok(Self {
            db,
            data_dir: dir.as_ref().into(),
            phantom: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use tokio::fs::remove_dir_all;

    use super::*;
    use crate::test_utils::{random_id, sleep_secs, test_cmd::TestCommand};

    #[tokio::test]
    async fn create_and_recover() -> Result<(), Box<dyn Error>> {
        let db_dir = format!("/tmp/curp-{}", random_id());

        {
            let s = RocksDBStorage::<TestCommand>::new(&db_dir)?;
            s.flush_voted_for(1, "S2".to_string()).await?;
            s.flush_voted_for(3, "S1".to_string()).await?;
            let entry0 = LogEntry::new(1, 3, Arc::new(TestCommand::default()));
            let entry1 = LogEntry::new(2, 3, Arc::new(TestCommand::default()));
            let entry2 = LogEntry::new(3, 3, Arc::new(TestCommand::default()));
            s.put_log_entry(entry0).await?;
            s.put_log_entry(entry1).await?;
            s.put_log_entry(entry2).await?;
            sleep_secs(2).await;
        }

        {
            let s = RocksDBStorage::<TestCommand>::new(&db_dir)?;
            let (voted_for, entries) = s.recover().await?;
            assert_eq!(voted_for, Some((3, "S1".to_string())));
            assert_eq!(entries[0].index, 1);
            assert_eq!(entries[1].index, 2);
            assert_eq!(entries[2].index, 3);
        }

        remove_dir_all(db_dir).await?;

        Ok(())
    }
}
