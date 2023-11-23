use std::marker::PhantomData;

use async_trait::async_trait;
use engine::{Engine, EngineType, StorageEngine, WriteOperation};
use utils::config::EngineConfig;

use super::{StorageApi, StorageError};
use crate::{cmd::Command, log_entry::LogEntry, members::ServerId};

/// Key for persisted state
const VOTE_FOR: &[u8] = b"VoteFor";

/// Column family name for curp storage
const CF: &str = "curp";

/// `DB` storage implementation
pub(in crate::server) struct DB<C> {
    /// DB handle
    db: Engine,
    /// Phantom
    phantom: PhantomData<C>,
}

#[async_trait]
impl<C: Command> StorageApi for DB<C> {
    /// Command
    type Command = C;

    async fn flush_voted_for(&self, term: u64, voted_for: ServerId) -> Result<(), StorageError> {
        let bytes = bincode::serialize(&(term, voted_for))?;
        let op = WriteOperation::new_put(CF, VOTE_FOR.to_vec(), bytes);
        self.db.write_batch(vec![op], true)?;

        Ok(())
    }

    async fn put_log_entry(&self, entry: &LogEntry<Self::Command>) -> Result<(), StorageError> {
        let bytes = bincode::serialize(entry)?;
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
}

impl<C> DB<C> {
    /// Create a new CURP `DB`
    pub(in crate::server) fn open(config: &EngineConfig) -> Result<Self, StorageError> {
        let engine_type = match *config {
            EngineConfig::Memory => EngineType::Memory,
            EngineConfig::RocksDB(ref path) => EngineType::Rocks(path.clone()),
            _ => unreachable!("Not supported storage type"),
        };
        let db = Engine::new(engine_type, &[CF])?;
        Ok(Self {
            db,
            phantom: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use curp_test_utils::{sleep_secs, test_cmd::TestCommand};
    use test_macros::abort_on_panic;
    use tokio::fs::remove_dir_all;

    use crate::rpc::ProposeId;

    use super::*;

    #[tokio::test]
    #[abort_on_panic]
    async fn create_and_recover() -> Result<(), Box<dyn Error>> {
        let db_dir = tempfile::tempdir().unwrap().into_path();
        let storage_cfg = EngineConfig::RocksDB(db_dir.clone());
        {
            let s = DB::<TestCommand>::open(&storage_cfg)?;
            s.flush_voted_for(1, 222).await?;
            s.flush_voted_for(3, 111).await?;
            let entry0 = LogEntry::new(1, 3, ProposeId(1, 1), Arc::new(TestCommand::default()));
            let entry1 = LogEntry::new(2, 3, ProposeId(1, 2), Arc::new(TestCommand::default()));
            let entry2 = LogEntry::new(3, 3, ProposeId(1, 3), Arc::new(TestCommand::default()));
            s.put_log_entry(&entry0).await?;
            s.put_log_entry(&entry1).await?;
            s.put_log_entry(&entry2).await?;
            sleep_secs(2).await;
        }

        {
            let s = DB::<TestCommand>::open(&storage_cfg)?;
            let (voted_for, entries) = s.recover().await?;
            assert_eq!(voted_for, Some((3, 111)));
            assert_eq!(entries[0].index, 1);
            assert_eq!(entries[1].index, 2);
            assert_eq!(entries[2].index, 3);
        }

        remove_dir_all(db_dir).await?;

        Ok(())
    }
}
