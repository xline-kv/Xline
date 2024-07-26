use std::ops::Deref;

use engine::{Engine, EngineType, StorageEngine, StorageOps, WriteOperation};
use parking_lot::Mutex;
use prost::Message;
use utils::config::EngineConfig;

use super::{
    wal::{codec::DataFrame, config::WALConfig, WALStorage, WALStorageOps},
    RecoverData, StorageApi, StorageError,
};
use crate::{
    cmd::Command,
    log_entry::LogEntry,
    members::{ClusterInfo, ServerId},
    rpc::Member,
};

/// Key for persisted state
const VOTE_FOR: &[u8] = b"VoteFor";
/// Key for cluster id
const CLUSTER_ID: &[u8] = b"ClusterId";
/// Key for member id
const MEMBER_ID: &[u8] = b"MemberId";

/// Column family name for curp storage
const CF: &str = "curp";
/// Column family name for members
const MEMBERS_CF: &str = "members";

/// The sub dir for `RocksDB` files
const ROCKSDB_SUB_DIR: &str = "rocksdb";

/// The sub dir for WAL files
const WAL_SUB_DIR: &str = "wal";

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
    fn put_member(&self, member: &Member) -> Result<(), StorageError> {
        let id = member.id;
        let data = member.encode_to_vec();
        let op = WriteOperation::new_put(MEMBERS_CF, id.to_le_bytes().to_vec(), data);
        self.db.write_multi(vec![op], true)?;
        Ok(())
    }

    #[inline]
    fn remove_member(&self, id: ServerId) -> Result<(), StorageError> {
        let id_bytes = id.to_le_bytes();
        let op = WriteOperation::new_delete(MEMBERS_CF, &id_bytes);
        self.db.write_multi(vec![op], true)?;
        Ok(())
    }

    #[inline]
    fn put_cluster_info(&self, cluster_info: &ClusterInfo) -> Result<(), StorageError> {
        let mut ops = Vec::new();
        ops.push(WriteOperation::new_put(
            CF,
            CLUSTER_ID.to_vec(),
            cluster_info.cluster_id().to_le_bytes().to_vec(),
        ));
        ops.push(WriteOperation::new_put(
            CF,
            MEMBER_ID.to_vec(),
            cluster_info.self_id().to_le_bytes().to_vec(),
        ));
        for m in cluster_info.all_members_vec() {
            ops.push(WriteOperation::new_put(
                MEMBERS_CF,
                m.id.to_le_bytes().to_vec(),
                m.encode_to_vec(),
            ));
        }
        self.db.write_multi(ops, true)?;
        Ok(())
    }

    #[inline]
    fn recover_cluster_info(&self) -> Result<Option<ClusterInfo>, StorageError> {
        let cluster_id = self.db.get(CF, CLUSTER_ID)?.map(|bytes| {
            u64::from_le_bytes(
                bytes
                    .as_slice()
                    .try_into()
                    .unwrap_or_else(|e| unreachable!("cannot decode index from backend, {e:?}")),
            )
        });
        let member_id = self.db.get(CF, MEMBER_ID)?.map(|bytes| {
            u64::from_le_bytes(
                bytes
                    .as_slice()
                    .try_into()
                    .unwrap_or_else(|e| unreachable!("cannot decode index from backend, {e:?}")),
            )
        });
        let mut members = vec![];
        for (_k, v) in self.db.get_all(MEMBERS_CF)? {
            let member = Member::decode(v.as_ref())?;
            members.push(member);
        }

        let cluster_info = match (cluster_id, member_id, members.is_empty()) {
            (Some(cluster_id), Some(member_id), false) => {
                Some(ClusterInfo::new(cluster_id, member_id, members))
            }
            _ => None,
        };

        Ok(cluster_info)
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
    use std::{error::Error, sync::Arc};

    use curp_test_utils::{sleep_secs, test_cmd::TestCommand};
    use test_macros::abort_on_panic;
    use tokio::fs::remove_dir_all;

    use super::*;
    use crate::rpc::ProposeId;

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
}
