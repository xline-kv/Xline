use std::marker::PhantomData;

use async_trait::async_trait;
use engine::{Engine, EngineType, StorageEngine, WriteOperation};
use prost::Message;
use utils::config::EngineConfig;

use super::{StorageApi, StorageError};
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
/// Column family name for logs
const LOGS_CF: &str = "logs";
/// Column family name for members
const MEMBERS_CF: &str = "members";

/// `DB` storage implementation
#[derive(Debug)]
pub struct DB<C> {
    /// DB handle
    db: Engine,
    /// Phantom
    phantom: PhantomData<C>,
}

#[async_trait]
impl<C: Command> StorageApi for DB<C> {
    /// Command
    type Command = C;

    #[inline]
    async fn flush_voted_for(&self, term: u64, voted_for: ServerId) -> Result<(), StorageError> {
        let bytes = bincode::serialize(&(term, voted_for))?;
        let op = WriteOperation::new_put(CF, VOTE_FOR.to_vec(), bytes);
        self.db.write_batch(vec![op], true)?;

        Ok(())
    }

    #[inline]
    async fn put_log_entry(&self, entry: &LogEntry<Self::Command>) -> Result<(), StorageError> {
        let bytes = bincode::serialize(entry)?;
        let op = WriteOperation::new_put(LOGS_CF, entry.index.to_be_bytes().to_vec(), bytes);
        self.db.write_batch(vec![op], false)?;

        Ok(())
    }

    #[inline]
    fn put_member(&self, member: &Member) -> Result<(), StorageError> {
        let id = member.id;
        let data = member.encode_to_vec();
        let op = WriteOperation::new_put(MEMBERS_CF, id.to_be_bytes().to_vec(), data);
        self.db.write_batch(vec![op], true)?;
        Ok(())
    }

    #[inline]
    fn remove_member(&self, id: ServerId) -> Result<(), StorageError> {
        let id_bytes = id.to_be_bytes();
        let op = WriteOperation::new_delete(MEMBERS_CF, &id_bytes);
        self.db.write_batch(vec![op], true)?;
        Ok(())
    }

    #[inline]
    fn put_cluster_info(&self, cluster_info: &ClusterInfo) -> Result<(), StorageError> {
        let mut ops = Vec::new();
        ops.push(WriteOperation::new_put(
            CF,
            CLUSTER_ID.to_vec(),
            cluster_info.cluster_id().to_be_bytes().to_vec(),
        ));
        ops.push(WriteOperation::new_put(
            CF,
            MEMBER_ID.to_vec(),
            cluster_info.self_id().to_be_bytes().to_vec(),
        ));
        for m in cluster_info.all_members_vec() {
            ops.push(WriteOperation::new_put(
                MEMBERS_CF,
                m.id.to_be_bytes().to_vec(),
                m.encode_to_vec(),
            ));
        }
        self.db.write_batch(ops, true)?;
        Ok(())
    }

    #[inline]
    fn recover_cluster_info(&self) -> Result<Option<ClusterInfo>, StorageError> {
        let cluster_id = self.db.get(CF, CLUSTER_ID)?.map(|bytes| {
            u64::from_be_bytes(
                bytes
                    .as_slice()
                    .try_into()
                    .unwrap_or_else(|e| unreachable!("cannot decode index from backend, {e:?}")),
            )
        });
        let member_id = self.db.get(CF, MEMBER_ID)?.map(|bytes| {
            u64::from_be_bytes(
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
        for (_k, v) in self.db.get_all(LOGS_CF)? {
            let entry: LogEntry<C> = bincode::deserialize(&v)?;
            #[allow(clippy::arithmetic_side_effects)] // won't overflow
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
    /// # Errors
    /// Will return `StorageError` if failed to open the storage
    #[inline]
    pub fn open(config: &EngineConfig) -> Result<Self, StorageError> {
        let engine_type = match *config {
            EngineConfig::Memory => EngineType::Memory,
            EngineConfig::RocksDB(ref path) => EngineType::Rocks(path.clone()),
            _ => unreachable!("Not supported storage type"),
        };
        let db = Engine::new(engine_type, &[CF, LOGS_CF, MEMBERS_CF])?;
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

    use super::*;
    use crate::rpc::ProposeId;

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
