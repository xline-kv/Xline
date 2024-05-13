#![allow(unused)] // TODO: remove this
use std::{
    collections::{HashMap, HashSet},
    io,
    path::Path,
    sync::Arc,
};

use clippy_utilities::OverflowArithmetic;
use curp_external_api::cmd::{Command, ConflictCheck};
use itertools::Itertools;
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use sha2::Sha256;
use tracing::{debug, error};
use utils::wal::{get_file_paths_with_ext, pipeline::FilePipeline, LockedFile};

use crate::rpc::{PoolEntry, PoolEntryInner, ProposeId};

use self::{
    codec::DataFrame,
    config::WALConfig,
    error::WALError,
    segment::{Segment, SegmentAttr, ToDrop},
};

/// WAL codec
mod codec;

/// WAL error
mod error;

/// WAL config
mod config;

/// WAL segment
mod segment;

/// WAL tests
#[cfg(test)]
mod tests;

/// WAL Result
type Result<T> = std::result::Result<T, WALError>;

/// Codec of this WAL
type WALCodec<C> = codec::WAL<C, Sha256>;

/// Operations of speculative pool WAL
pub(crate) trait PoolWALOps<C: Command> {
    /// Insert a command to WAL
    fn insert(&self, entries: Vec<PoolEntry<C>>) -> io::Result<()>;

    /// Removes a command from WAL
    fn remove(&self, propose_ids: Vec<ProposeId>) -> io::Result<()>;

    /// Recover all commands stored in WAL
    fn recover(&self) -> io::Result<Vec<PoolEntry<C>>>;

    /// Try GC by propose ids
    ///
    /// The `check_fn` should filter out obsolete propose ids
    fn gc<F>(&self, check_fn: F) -> io::Result<()>
    where
        F: Fn(&[ProposeId]) -> &[ProposeId];
}

/// WAL of speculative pool
struct SpeculativePoolWAL<C> {
    /// WAL config
    config: WALConfig,
    /// Insert WAL
    insert: Mutex<WAL<segment::Insert, C>>,
    /// Remove WAL
    remove: Mutex<WAL<segment::Remove, C>>,
    /// Drop tx
    drop_tx: Option<flume::Sender<ToDrop<WALCodec<C>>>>,
    /// Drop task handle
    drop_task_handle: Option<std::thread::JoinHandle<()>>,
}

impl<C> SpeculativePoolWAL<C>
where
    C: Command,
{
    #[allow(unused)]
    fn new(config: WALConfig) -> io::Result<Self> {
        if !config.insert_dir.try_exists()? {
            std::fs::create_dir_all(&config.insert_dir)?;
        }
        if !config.remove_dir.try_exists()? {
            std::fs::create_dir_all(&config.remove_dir)?;
        }
        let (drop_tx, drop_rx) = flume::unbounded();
        let handle = Self::spawn_dropping_task(drop_rx);

        Ok(Self {
            insert: Mutex::new(WAL::new(
                &config.insert_dir,
                config.max_insert_segment_size,
            )?),
            remove: Mutex::new(WAL::new(
                &config.remove_dir,
                config.max_remove_segment_size,
            )?),
            config,
            drop_tx: Some(drop_tx),
            drop_task_handle: Some(handle),
        })
    }

    fn spawn_dropping_task(
        drop_rx: flume::Receiver<ToDrop<WALCodec<C>>>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            while let Ok(segment) = drop_rx.recv() {
                match segment {
                    ToDrop::Insert(_) => debug!("Removing insert segment file"),
                    ToDrop::Remove(_) => debug!("Removing remove segment file"),
                }
                if let Err(err) = std::fs::remove_file(segment.path()) {
                    error!("Failed to remove segment file: {err}");
                }
            }
        })
    }

    /// Keeps only commute commands
    fn keep_commute_cmds(mut entries: Vec<PoolEntry<C>>) -> Vec<PoolEntry<C>> {
        let commute = |entry: &PoolEntry<C>, others: &[PoolEntry<C>]| {
            !others.iter().any(|e| e.is_conflict(&entry))
        };
        // start from last element
        entries.reverse();
        let keep = entries
            .iter()
            .enumerate()
            .take_while(|(i, ref e)| commute(*e, &entries[..*i]))
            .count();
        entries.drain(..keep).collect()
    }
}

impl<C: Command> PoolWALOps<C> for SpeculativePoolWAL<C> {
    fn insert(&self, entries: Vec<PoolEntry<C>>) -> io::Result<()> {
        self.insert.lock().insert(entries)
    }

    fn remove(&self, propose_ids: Vec<ProposeId>) -> io::Result<()> {
        self.remove.lock().remove(propose_ids)
    }

    fn recover(&self) -> io::Result<Vec<PoolEntry<C>>> {
        let mut insert_l = self.insert.lock();
        let mut remove_l = self.remove.lock();
        let mut cmds = insert_l.recover(&self.config.insert_dir)?;
        let removed = remove_l.recover(&self.config.remove_dir)?;
        let entries = cmds
            .into_iter()
            .filter_map(|(id, cmd)| (!removed.contains(&id)).then_some(PoolEntry::new(id, cmd)))
            .collect();
        Ok(Self::keep_commute_cmds(entries))
    }

    fn gc<F>(&self, check_fn: F) -> io::Result<()>
    where
        F: Fn(&[ProposeId]) -> &[ProposeId],
    {
        let mut insert_l = self.insert.lock();
        let mut remove_l = self.remove.lock();
        for segment in insert_l.gc(&check_fn) {
            if let Err(e) = self.drop_tx.as_ref().unwrap().send(ToDrop::Insert(segment)) {
                error!("Failed to send segment to dropping task: {e}");
            }
        }
        for segment in remove_l.gc(&check_fn) {
            if let Err(e) = self.drop_tx.as_ref().unwrap().send(ToDrop::Remove(segment)) {
                error!("Failed to send segment to dropping task: {e}");
            }
        }
        Ok(())
    }
}

impl<C> Drop for SpeculativePoolWAL<C> {
    #[allow(clippy::unwrap_used)]
    fn drop(&mut self) {
        // The task will exit after `drop_tx` is dropped
        drop(self.drop_tx.take());
        if let Err(err) = self.drop_task_handle.take().unwrap().join() {
            error!("Failed to join segment dropping task: {err:?}");
        }
    }
}

struct WAL<T, C> {
    /// WAL segments
    segments: Vec<Segment<T, WALCodec<C>>>,
    /// The pipeline that pre-allocates files
    // TODO: Fix conflict
    pipeline: FilePipeline,
    /// Next segment id
    next_segment_id: u64,
    /// The maximum size of this segment
    max_segment_size: u64,
}

impl<T, C> WAL<T, C>
where
    T: SegmentAttr,
    C: Serialize + DeserializeOwned,
{
    fn new(dir: impl AsRef<Path>, max_segment_size: u64) -> io::Result<Self> {
        Ok(Self {
            segments: Vec::new(),
            pipeline: FilePipeline::new(dir.as_ref().into(), max_segment_size)?,
            next_segment_id: 0,
            max_segment_size,
        })
    }

    fn write_frames(&mut self, item: Vec<DataFrame<C>>) -> io::Result<()> {
        let last_segment = self
            .segments
            .last_mut()
            .unwrap_or_else(|| unreachable!("there should be at least on segment"));
        last_segment.write_sync(item)?;

        if last_segment.is_full() {
            self.open_new_segment()?;
        }

        Ok(())
    }

    fn recover_frames(&mut self, dir: impl AsRef<Path>) -> Result<Vec<DataFrame<C>>> {
        let paths = get_file_paths_with_ext(dir, &T::ext())?;
        let lfiles: Vec<_> = paths
            .into_iter()
            .map(LockedFile::open_rw)
            .collect::<io::Result<_>>()?;
        let mut segments: Vec<_> = lfiles
            .into_iter()
            .map(|f| Segment::open(f, self.max_segment_size, WALCodec::new(), T::r#type()))
            .collect::<Result<_>>()?;
        segments.sort_unstable();
        let logs: Vec<_> = segments
            .iter_mut()
            .map(Segment::recover::<C>)
            .collect::<Result<_>>()?;

        self.next_segment_id = segments.last().map(Segment::segment_id).unwrap_or(0);
        self.segments = segments;
        self.open_new_segment();

        Ok(logs.into_iter().flatten().collect())
    }

    /// Opens a new WAL segment
    fn open_new_segment(&mut self) -> io::Result<()> {
        let lfile = self
            .pipeline
            .next()
            .ok_or(io::Error::from(io::ErrorKind::BrokenPipe))??;

        let segment = Segment::create(
            lfile,
            self.next_segment_id,
            self.max_segment_size,
            WALCodec::new(),
            T::r#type(),
        )?;

        self.segments.push(segment);
        self.next_segment_id = self.next_segment_id.overflow_add(1);

        Ok(())
    }

    /// Gets all propose ids stored in this WAL
    fn all_ids(&self) -> Vec<ProposeId> {
        self.segments
            .iter()
            .map(Segment::propose_ids)
            .flatten()
            .collect()
    }

    /// Try GC by propose ids
    ///
    /// The `check_fn` should filter out obsolete propose ids
    fn gc<F>(&mut self, check_fn: &F) -> Vec<Segment<T, WALCodec<C>>>
    where
        F: Fn(&[ProposeId]) -> &[ProposeId],
    {
        let all_ids = self.all_ids();
        let obsolete_ids = check_fn(&all_ids);

        let mut to_remove = Vec::new();
        for (pos, segment) in &mut self.segments.iter_mut().enumerate() {
            if segment.invalidate_propose_ids(&obsolete_ids) {
                to_remove.push(pos);
            }
        }
        to_remove
            .into_iter()
            .map(|pos| self.segments.remove(pos))
            .collect()
    }
}

impl<C> WAL<segment::Insert, C>
where
    C: Serialize + DeserializeOwned,
{
    fn insert(&mut self, entries: Vec<PoolEntry<C>>) -> io::Result<()> {
        self.write_frames(entries.into_iter().map(Into::into).collect())
    }

    fn recover(&mut self, dir: impl AsRef<Path>) -> Result<Vec<(ProposeId, Arc<C>)>> {
        Ok(self
            .recover_frames(dir)?
            .into_iter()
            .filter_map(|frame| match frame {
                DataFrame::Insert { propose_id, cmd } => Some((propose_id, cmd)),
                DataFrame::Remove(_) => None,
            })
            .collect())
    }
}

impl<C> WAL<segment::Remove, C>
where
    C: Serialize + DeserializeOwned,
{
    fn remove(&mut self, ids: Vec<ProposeId>) -> io::Result<()> {
        self.write_frames(ids.into_iter().map(DataFrame::Remove).collect())
    }

    fn recover(&mut self, dir: impl AsRef<Path>) -> Result<HashSet<ProposeId>> {
        Ok(self
            .recover_frames(dir)?
            .into_iter()
            .filter_map(|frame| match frame {
                DataFrame::Insert { .. } => None,
                DataFrame::Remove(propose_id) => Some(propose_id),
            })
            .collect())
    }
}

impl<C> From<PoolEntry<C>> for DataFrame<C> {
    fn from(entry: PoolEntry<C>) -> Self {
        match entry.inner {
            PoolEntryInner::Command(cmd) => DataFrame::Insert {
                propose_id: entry.id,
                cmd,
            },
            PoolEntryInner::ConfChange(_) => unreachable!("should not insert conf change entry"),
        }
    }
}
