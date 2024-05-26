use std::{
    io,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use tracing::error;

use super::LockedFile;

/// The temp file extension
const TEMP_FILE_EXT: &str = ".tmp";

/// The file pipeline, used for pipelining the creation of temp file
#[allow(clippy::module_name_repetitions)]
pub struct FilePipeline {
    /// The directory where the temp files are created
    dir: PathBuf,
    /// The size of the temp file
    file_size: u64,
    /// The file receive iterator
    ///
    /// As tokio::fs is generally slower than std::fs, we use synchronous file allocation.
    /// Please also refer to the issue discussed on the tokio repo: https://github.com/tokio-rs/tokio/issues/3664
    file_iter: flume::IntoIter<LockedFile>,
    /// Stopped flag
    stopped: Arc<AtomicBool>,
}

impl FilePipeline {
    /// Creates a new `FilePipeline`
    ///
    /// # Errors
    ///
    /// This function will return an error if failed to clean up the given directory.
    #[inline]
    pub fn new(dir: PathBuf, file_size: u64) -> io::Result<Self> {
        Self::clean_up(&dir)?;

        let (file_tx, file_rx) = flume::bounded(1);
        let dir_c = dir.clone();
        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_c = Arc::clone(&stopped);

        #[cfg(not(madsim))]
        let _ignore = std::thread::spawn(move || {
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

        #[cfg(madsim)]
        let _ignore = tokio::spawn(async move {
            let mut file_count = 0;
            loop {
                match Self::alloc(&dir_c, file_size, &mut file_count) {
                    Ok(file) => {
                        if file_tx.send_async(file).await.is_err() {
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

        Ok(Self {
            dir,
            file_size,
            file_iter: file_rx.into_iter(),
            stopped,
        })
    }

    /// Stops the pipeline
    #[inline]
    pub fn stop(&mut self) {
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
            if file
                .file_name()
                .to_str()
                .is_some_and(|fname| fname.ends_with(TEMP_FILE_EXT))
            {
                if let Err(err) = std::fs::remove_file(file.path()) {
                    // The file has already been deleted, continue
                    if matches!(err.kind(), io::ErrorKind::NotFound) {
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Ok(())
    }
}

impl Drop for FilePipeline {
    #[inline]
    fn drop(&mut self) {
        self.stop();
    }
}

impl Iterator for FilePipeline {
    type Item = io::Result<LockedFile>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped.load(Ordering::Relaxed) {
            return None;
        }
        self.file_iter.next().map(Ok)
    }
}

#[allow(clippy::missing_fields_in_debug)] // `flume::IntoIter` does not implement `Debug`
impl std::fmt::Debug for FilePipeline {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilePipeline")
            .field("dir", &self.dir)
            .field("file_size", &self.file_size)
            .field("stopped", &self.stopped)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn file_pipeline_is_ok() {
        let file_size = 1024;
        let dir = tempfile::tempdir().unwrap();
        let mut pipeline = FilePipeline::new(dir.as_ref().into(), file_size).unwrap();

        let check_size = |file: LockedFile| {
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
