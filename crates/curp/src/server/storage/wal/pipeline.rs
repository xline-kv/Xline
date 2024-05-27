use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::Poll,
    thread::JoinHandle,
};

use clippy_utilities::OverflowArithmetic;
use event_listener::Event;
use thiserror::Error;
use tracing::error;

use super::util::LockedFile;

/// The temp file extension
const TEMP_FILE_EXT: &str = ".tmp";

/// The file pipeline, used for pipelining the creation of temp file
pub(super) struct FilePipeline {
    /// The directory where the temp files are created
    dir: PathBuf,
    /// The size of the temp file
    file_size: u64,
    /// The file receive iterator
    ///
    /// As tokio::fs is generally slower than std::fs, we use synchronous file allocation.
    /// Please also refer to the issue discussed on the tokio repo: https://github.com/tokio-rs/tokio/issues/3664
    file_iter: Option<flume::IntoIter<LockedFile>>,
    /// Stopped flag
    stopped: Arc<AtomicBool>,
    /// Join handle of the allocation task
    file_alloc_task_handle: Option<JoinHandle<()>>,
    // #[cfg_attr(not(madsim), allow(unused))]
    #[cfg(madsim)]
    /// File count used in madsim tests
    file_count: usize,
}

impl FilePipeline {
    /// Creates a new `FilePipeline`
    pub(super) fn new(dir: PathBuf, file_size: u64) -> Self {
        if let Err(e) = Self::clean_up(&dir) {
            error!("Failed to clean up tmp files: {e}");
        }

        let dir_c = dir.clone();
        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_c = Arc::clone(&stopped);

        #[cfg(not(madsim))]
        {
            let (file_tx, file_rx) = flume::bounded(1);
            let file_alloc_task_handle = std::thread::spawn(move || {
                let mut file_count = 0;
                loop {
                    match Self::alloc(&dir_c, file_size, &mut file_count) {
                        Ok(file) => {
                            if file_tx.send(file).is_err() {
                                // The receiver is already dropped, stop this task
                                break;
                            }
                            if stopped_c.load(Ordering::Relaxed) {
                                if let Err(e) = Self::clean_up(&dir_c) {
                                    error!("failed to clean up pipeline temp files: {e}");
                                }
                                break;
                            }
                        }
                        Err(e) => {
                            error!("failed to allocate file: {e}");
                            break;
                        }
                    }
                }
            });

            Self {
                dir,
                file_size,
                file_iter: Some(file_rx.into_iter()),
                stopped,
                file_alloc_task_handle: Some(file_alloc_task_handle),
            }
        }

        #[cfg(madsim)]
        {
            Self {
                dir,
                file_size,
                file_iter: None,
                stopped,
                file_alloc_task_handle: None,
                file_count: 0,
            }
        }
    }

    /// Stops the pipeline
    pub(super) fn stop(&mut self) {
        self.stopped.store(true, Ordering::Relaxed);
    }

    /// Allocates a a new tempfile
    fn alloc(dir: &PathBuf, file_size: u64, file_count: &mut usize) -> io::Result<LockedFile> {
        let fpath = PathBuf::from(dir).join(format!("{file_count}{TEMP_FILE_EXT}"));
        let mut file = LockedFile::open_rw(fpath)?;
        file.preallocate(file_size)?;
        *file_count = file_count.wrapping_add(1);
        Ok(file)
    }

    /// Cleans up all unused tempfiles
    fn clean_up(dir: &PathBuf) -> io::Result<()> {
        for result in std::fs::read_dir(dir)? {
            let file = result?;
            if let Some(filename) = file.file_name().to_str() {
                if filename.ends_with(TEMP_FILE_EXT) {
                    std::fs::remove_file(file.path())?;
                }
            }
        }
        Ok(())
    }
}

impl Drop for FilePipeline {
    fn drop(&mut self) {
        self.stop();
        // Drops the file rx so that the allocation task could exit
        drop(self.file_iter.take());
        if let Some(Err(e)) = self.file_alloc_task_handle.take().map(JoinHandle::join) {
            error!("failed to join file allocation task: {e:?}");
        }
    }
}

impl Iterator for FilePipeline {
    type Item = io::Result<LockedFile>;

    #[cfg(not(madsim))]
    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped.load(Ordering::Relaxed) {
            return None;
        }
        self.file_iter
            .as_mut()
            .unwrap_or_else(|| unreachable!("Option is always `Some`"))
            .next()
            .map(Ok)
    }

    #[cfg(madsim)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped.load(Ordering::Relaxed) {
            return None;
        }
        Some(Self::alloc(&self.dir, self.file_size, &mut self.file_count))
    }
}

impl std::fmt::Debug for FilePipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilePipeline")
            .field("dir", &self.dir)
            .field("file_size", &self.file_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::storage::wal::util::get_file_paths_with_ext;

    #[tokio::test]
    async fn file_pipeline_is_ok() {
        let file_size = 1024;
        let dir = tempfile::tempdir().unwrap();
        let mut pipeline = FilePipeline::new(dir.as_ref().into(), file_size);

        let check_size = |mut file: LockedFile| {
            let file = file.into_std();
            assert_eq!(file.metadata().unwrap().len(), file_size,);
        };
        let file0 = pipeline.next().unwrap().unwrap();
        check_size(file0);
        let file1 = pipeline.next().unwrap().unwrap();
        check_size(file1);
        pipeline.stop();
        assert!(pipeline.next().is_none());
    }
}
