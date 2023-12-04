use std::{
    io,
    path::{Path, PathBuf},
    task::Poll,
};

use clippy_utilities::OverflowArithmetic;
use event_listener::Event;
use flume::r#async::RecvStream;
use futures::{FutureExt, StreamExt};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_stream::Stream;
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
    /// The file receive stream
    file_stream: RecvStream<'static, LockedFile>,
    /// The stop event listener
    stop_event: Event,
    /// The handle of the background file creation task
    handle: JoinHandle<io::Result<()>>,
}

impl FilePipeline {
    /// Creates a new `FilePipeline`
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select! macro
    pub(super) fn new(dir: PathBuf, file_size: u64) -> Self {
        let (file_tx, file_rx) = flume::bounded(1);
        let stop_event = Event::new();
        let mut stop_listener = stop_event.listen();
        let dir_c = dir.clone();

        let handle = tokio::spawn(async move {
            let mut file_count = 0;
            loop {
                let file = Self::alloc(&dir_c, file_size, &mut file_count)?;
                tokio::select! {
                    _ = &mut stop_listener => {
                        break;
                    }
                    result = file_tx.send_async(file) => {
                        // The receiver is already dropped, stop this task
                        if let Err(e) = result {
                            break;
                        }
                    }
                }
            }
            Self::clean_up(&dir_c)?;
            Ok(())
        });
        Self {
            dir,
            file_size,
            file_stream: file_rx.into_stream(),
            stop_event,
            handle,
        }
    }

    /// Stops the pipeline
    pub(super) fn stop(&self) {
        self.stop_event.notify(1);
    }

    /// Allocates a a new tempfile
    fn alloc(
        dir: impl AsRef<Path>,
        file_size: u64,
        file_count: &mut usize,
    ) -> io::Result<LockedFile> {
        let fpath = PathBuf::from(dir.as_ref()).join(format!("{}{TEMP_FILE_EXT}", *file_count));
        let mut locked_file = LockedFile::open_rw(fpath)?;
        locked_file.preallocate(file_size)?;
        *file_count = file_count.overflow_add(1);
        Ok(locked_file)
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

impl Stream for FilePipeline {
    type Item = io::Result<LockedFile>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.handle.is_finished() {
            if let Poll::Ready(result) = self.handle.poll_unpin(cx) {
                match result {
                    Ok(Err(e)) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                    Err(e) => {
                        return Poll::Ready(Some(Err(e.into())));
                    }
                    Ok(Ok(_)) => return Poll::Ready(None),
                }
            }
            return Poll::Ready(None);
        }

        self.file_stream.poll_next_unpin(cx).map(|opt| opt.map(Ok))
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
    use crate::server::storage::wal::util::get_file_paths_with_ext;

    use super::*;

    #[tokio::test]
    async fn file_pipeline_is_ok() {
        let file_size = 1024;
        let dir = tempfile::tempdir().unwrap();
        let mut pipeline = FilePipeline::new(dir.as_ref().into(), file_size);

        let check_size = |mut file: LockedFile| {
            let file = file.into_std();
            assert_eq!(file.metadata().unwrap().len(), file_size,);
        };
        let file0 = pipeline.next().await.unwrap().unwrap();
        check_size(file0);
        let file1 = pipeline.next().await.unwrap().unwrap();
        check_size(file1);
        let paths = get_file_paths_with_ext(&dir, TEMP_FILE_EXT).unwrap();
        assert_eq!(paths.len(), 2);
        pipeline.stop();
        assert!(pipeline.next().await.is_none());
        let paths_cleaned = get_file_paths_with_ext(dir, TEMP_FILE_EXT).unwrap();
        assert_eq!(paths_cleaned.len(), 0);
    }
}
