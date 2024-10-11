use std::ops::Deref;

use engine::{Engine, EngineType, StorageOps, WriteOperation};
use parking_lot::Mutex;
use utils::config::EngineConfig;

use super::{
    wal::{codec::DataFrame, config::WALConfig, WALStorage, WALStorageOps},
    RecoverData, StorageApi, StorageError,
};
use crate::{cmd::Command, log_entry::LogEntry, member::MembershipState, members::ServerId};

/// Key for persisted state
const VOTE_FOR: &[u8] = b"VoteFor";
/// Column family name for curp storage
const CF: &str = "curp";
/// Column family name for members
const MEMBERS_CF: &str = "members";

/// The sub dir for `RocksDB` files
const ROCKSDB_SUB_DIR: &str = "rocksdb";

/// The sub dir for WAL files
const WAL_SUB_DIR: &str = "wal";

/// Keys for membership persistent
const MEMBERSHIP: &[u8] = b"membership";

/// `DB` storage implementation
#[derive(Debug)]
pub struct DB<C> {
    /// The WAL storage
    wal: Mutex<WALStorage<C>>,
    /// DB handle
    db: Engine,
}

impl<C: Command> StorageApi for DB<C> {
    /// Command
    type Command = C;

    #[inline]
    fn flush_voted_for(&self, term: u64, voted_for: ServerId) -> Result<(), StorageError> {
        let bytes = bincode::serialize(&(term, voted_for))?;
        let op = WriteOperation::new_put(CF, VOTE_FOR.to_vec(), bytes);
        self.db.write_multi(vec![op], true)?;

        Ok(())
    }

    #[inline]
    fn put_log_entries(&self, entry: &[&LogEntry<Self::Command>]) -> Result<(), StorageError> {
        self.wal
            .lock()
            .send_sync(
                entry
                    .iter()
                    .map(Deref::deref)
                    .map(DataFrame::Entry)
                    .collect(),
            )
            .map_err(Into::into)
    }

    #[inline]
    fn recover(&self) -> Result<RecoverData<Self::Command>, StorageError> {
        let entries = self.wal.lock().recover()?;
        let voted_for = self
            .db
            .get(CF, VOTE_FOR)?
            .map(|bytes| bincode::deserialize::<(u64, ServerId)>(&bytes))
            .transpose()?;
        Ok((voted_for, entries))
    }

    #[inline]
    fn put_membership(
        &self,
        node_id: u64,
        membership: &MembershipState,
    ) -> Result<(), StorageError> {
        let data = bincode::serialize(&(node_id, membership))?;
        let op = WriteOperation::new_put(CF, MEMBERSHIP.to_vec(), data);
        self.db.write_multi(vec![op], true).map_err(Into::into)
    }

    #[inline]
    fn recover_membership(&self) -> Result<Option<(u64, MembershipState)>, StorageError> {
        self.db
            .get(CF, MEMBERSHIP)?
            .map(|bytes| bincode::deserialize::<(u64, MembershipState)>(&bytes))
            .transpose()
            .map_err(Into::into)
    }
}

impl<C> DB<C> {
    /// Create a new CURP `DB`
    ///
    /// WARN: The `recover` method must be called before any call to `put_log_entries`.
    ///
    /// # Errors
    /// Will return `StorageError` if failed to open the storage
    #[inline]
    pub fn open(config: &EngineConfig) -> Result<Self, StorageError> {
        let (engine_type, wal_config) = match *config {
            EngineConfig::Memory => (EngineType::Memory, WALConfig::Memory),
            EngineConfig::RocksDB(ref path) => {
                let mut rocksdb_dir = path.clone();
                rocksdb_dir.push(ROCKSDB_SUB_DIR);
                let mut wal_dir = path.clone();
                wal_dir.push(WAL_SUB_DIR);
                (
                    EngineType::Rocks(rocksdb_dir.clone()),
                    WALConfig::new(wal_dir),
                )
            }
            _ => unreachable!("Not supported storage type"),
        };

        let db = Engine::new(engine_type, &[CF, MEMBERS_CF])?;
        let wal = WALStorage::new(wal_config)?;

        Ok(Self {
            wal: Mutex::new(wal),
            db,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        error::Error,
        sync::Arc,
    };

    use curp_test_utils::{sleep_secs, test_cmd::TestCommand};
    use test_macros::abort_on_panic;
    use tokio::fs::remove_dir_all;

    use super::*;
    use crate::{
        member::Membership,
        rpc::{NodeMetadata, ProposeId},
    };

    #[tokio::test]
    #[abort_on_panic]
    async fn create_and_recover() -> Result<(), Box<dyn Error>> {
        let db_dir = tempfile::tempdir().unwrap().into_path();
        let storage_cfg = EngineConfig::RocksDB(db_dir.clone());
        {
            let s = DB::<TestCommand>::open(&storage_cfg)?;
            let (voted_for, entries) = s.recover()?;
            assert!(voted_for.is_none());
            assert!(entries.is_empty());
            s.flush_voted_for(1, 222)?;
            s.flush_voted_for(3, 111)?;
            let entry0 = LogEntry::new(1, 3, ProposeId(1, 1), Arc::new(TestCommand::default()));
            let entry1 = LogEntry::new(2, 3, ProposeId(1, 2), Arc::new(TestCommand::default()));
            let entry2 = LogEntry::new(3, 3, ProposeId(1, 3), Arc::new(TestCommand::default()));
            s.put_log_entries(&[&entry0])?;
            s.put_log_entries(&[&entry1])?;
            s.put_log_entries(&[&entry2])?;
            sleep_secs(2).await;
        }

        {
            let s = DB::<TestCommand>::open(&storage_cfg)?;
            let (voted_for, entries) = s.recover()?;
            assert_eq!(voted_for, Some((3, 111)));
            assert_eq!(entries[0].index, 1);
            assert_eq!(entries[1].index, 2);
            assert_eq!(entries[2].index, 3);
        }

        remove_dir_all(db_dir).await?;

        Ok(())
    }

    #[test]
    fn put_and_recover_membership() {
        let db_dir = tempfile::tempdir().unwrap().into_path();
        let storage_cfg = EngineConfig::RocksDB(db_dir.clone());
        let membership = Membership::new(
            vec![BTreeSet::from([1])],
            BTreeMap::from([(1, NodeMetadata::default())]),
        );
        let ms = MembershipState::new(membership);
        {
            let s = DB::<TestCommand>::open(&storage_cfg).unwrap();
            s.put_membership(1, &ms).unwrap();
        }
        {
            let s = DB::<TestCommand>::open(&storage_cfg).unwrap();
            let (id, ms_recovered) = s.recover_membership().unwrap().unwrap();
            assert_eq!(id, 1);
            assert_eq!(ms, ms_recovered);
        }
    }
}
