use std::{collections::HashSet, io, path::Path, sync::Arc};

use clippy_utilities::OverflowArithmetic;
use curp_external_api::cmd::{Command, ConflictCheck};
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
    /// The `check_fn` should returns `true` if the propose id is obsolete
    fn gc<F>(&self, check_fn: F) -> io::Result<()>
    where
        F: Fn(&ProposeId) -> bool;
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
    /// Creates a new `SpeculativePoolWAL`
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

    /// Spawns the segment file removal task
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
    #[allow(
        clippy::indexing_slicing, // Slicings are checked
        clippy::pattern_type_mismatch, // Can't be fixed
    )]
    fn keep_commute_cmds(mut entries: Vec<PoolEntry<C>>) -> Vec<PoolEntry<C>> {
        let commute = |entry: &PoolEntry<C>, others: &[PoolEntry<C>]| {
            !others.iter().any(|e| e.is_conflict(entry))
        };
        // start from last element
        entries.reverse();
        let keep = entries
            .iter()
            .enumerate()
            .take_while(|(i, e)| commute(e, &entries[..*i]))
            .count();
        entries.drain(..keep).collect()
    }
}

impl<C: Command> PoolWALOps<C> for SpeculativePoolWAL<C> {
    fn insert(&self, entries: Vec<PoolEntry<C>>) -> io::Result<()> {
        self.insert.lock().insert(entries)
    }

    #[allow(clippy::unwrap_used, clippy::unwrap_in_result)]
    fn remove(&self, propose_ids: Vec<ProposeId>) -> io::Result<()> {
        let removed_insert_ids = self.insert.lock().remove_propose_ids(&propose_ids)?;
        let removed_ids: Vec<_> = removed_insert_ids
            .iter()
            .flat_map(Segment::propose_ids)
            .collect();
        for segment in removed_insert_ids {
            if let Err(e) = self.drop_tx.as_ref().unwrap().send(ToDrop::Insert(segment)) {
                error!("Failed to send segment to dropping task: {e}");
            }
        }

        let mut remove_l = self.remove.lock();
        let removed_remove_ids = remove_l.remove_propose_ids(&removed_ids)?;
        for segment in removed_remove_ids {
            if let Err(e) = self.drop_tx.as_ref().unwrap().send(ToDrop::Remove(segment)) {
                error!("Failed to send segment to dropping task: {e}");
            }
        }
        remove_l.remove(propose_ids)
    }

    fn recover(&self) -> io::Result<Vec<PoolEntry<C>>> {
        let mut insert_l = self.insert.lock();
        let mut remove_l = self.remove.lock();
        let cmds = insert_l.recover(&self.config.insert_dir)?;
        let removed = remove_l.recover(&self.config.remove_dir)?;
        let entries = cmds
            .into_iter()
            .filter_map(|(id, cmd)| (!removed.contains(&id)).then_some(PoolEntry::new(id, cmd)))
            .collect();
        Ok(Self::keep_commute_cmds(entries))
    }

    #[allow(clippy::unwrap_used, clippy::unwrap_in_result)]
    fn gc<F>(&self, check_fn: F) -> io::Result<()>
    where
        F: Fn(&ProposeId) -> bool,
    {
        let mut insert_l = self.insert.lock();
        let mut remove_l = self.remove.lock();
        for segment in insert_l.gc(&check_fn)? {
            if let Err(e) = self.drop_tx.as_ref().unwrap().send(ToDrop::Insert(segment)) {
                error!("Failed to send segment to dropping task: {e}");
            }
        }
        for segment in remove_l.gc(&check_fn)? {
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

#[cfg(test)]
impl<C> SpeculativePoolWAL<C>
where
    C: Command,
{
    /// Propose ids of the `Insert` segment
    fn insert_segment_ids(&self) -> Vec<Vec<ProposeId>> {
        self.insert.lock().segment_ids()
    }

    /// Propose ids of the `Remove` segment
    fn remove_segment_ids(&self) -> Vec<Vec<ProposeId>> {
        self.remove.lock().segment_ids()
    }
}

/// The WAL type
#[allow(clippy::upper_case_acronyms)]
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
    /// Creates a new `WAL`
    fn new(dir: impl AsRef<Path>, max_segment_size: u64) -> io::Result<Self> {
        Ok(Self {
            segments: Vec::new(),
            pipeline: FilePipeline::new(dir.as_ref().into(), max_segment_size)?,
            next_segment_id: 0,
            max_segment_size,
        })
    }

    /// Writes frames to the log
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

    /// Recovers all frames
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

        self.next_segment_id = segments.last().map_or(0, Segment::segment_id);
        self.segments = segments;
        self.open_new_segment()?;

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

    /// Try GC by propose ids
    ///
    /// The `check_fn` should filter out obsolete propose ids
    fn gc<F>(&mut self, check_fn: &F) -> Result<Vec<Segment<T, WALCodec<C>>>>
    where
        F: Fn(&ProposeId) -> bool,
    {
        for segment in &mut self.segments {
            segment.gc(check_fn);
        }
        self.remove_obsoletes()
    }

    /// Removes invalid propose ids
    fn remove_propose_ids(&mut self, ids: &[ProposeId]) -> Result<Vec<Segment<T, WALCodec<C>>>> {
        for segment in &mut self.segments {
            segment.remove_propose_ids(ids);
        }
        self.remove_obsoletes()
    }

    /// Remove obsolete segments
    fn remove_obsoletes(&mut self) -> Result<Vec<Segment<T, WALCodec<C>>>> {
        let (to_remove, segments): (Vec<_>, Vec<_>) =
            self.segments.drain(..).partition(Segment::is_obsolete);
        self.segments = segments;
        if self.segments.is_empty() {
            self.open_new_segment()?;
        }
        Ok(to_remove)
    }

    /// Returns the segment ids of this [`WAL<T, C>`].
    #[cfg(test)]
    fn segment_ids(&self) -> Vec<Vec<ProposeId>> {
        self.segments.iter().map(Segment::propose_ids).collect()
    }
}

impl<C> WAL<segment::Insert, C>
where
    C: Serialize + DeserializeOwned,
{
    /// Removes entries from this segment
    fn insert(&mut self, entries: Vec<PoolEntry<C>>) -> io::Result<()> {
        self.write_frames(entries.into_iter().map(Into::into).collect())
    }

    /// Recovers all entries
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
    /// Removes propose ids from this segment
    fn remove(&mut self, ids: Vec<ProposeId>) -> io::Result<()> {
        self.write_frames(ids.into_iter().map(DataFrame::Remove).collect())
    }

    /// Recovers all propose ids
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
