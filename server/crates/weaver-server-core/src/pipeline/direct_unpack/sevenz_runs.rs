// Pure LZMA2 runs use positional sources and guarded provisional outputs.
// Ordered codec graphs keep their existing extraction route.

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use sevenz_turbo::ReadAt;

use super::SetCoverage;

mod decode;
mod output;
use super::coverage::RunEpoch;
pub(crate) use decode::{RunJournal, decode_pass};
pub(crate) use output::{MemberSpec, PositionalManifest};

struct OpenPart {
    rewritten: u64,
    file: Arc<File>,
}

// Each read owns an Arc to its file. The short handle-cache lock never spans
// source I/O; simultaneous folder workers do not share a seek cursor. An epoch
// owns this cache, and repair constructs a new source after old workers join.
pub(crate) struct RunSource {
    paths: Arc<[PathBuf]>,
    coverage: Arc<SetCoverage>,
    epoch: RunEpoch,
    files: Mutex<Vec<Option<OpenPart>>>,
}

impl RunSource {
    pub(crate) fn new(
        paths: Arc<[PathBuf]>,
        coverage: Arc<SetCoverage>,
        epoch: RunEpoch,
    ) -> io::Result<Self> {
        if paths.is_empty() || paths.len() != coverage.part_count() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let mut files = Vec::new();
        files
            .try_reserve_exact(paths.len())
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        files.resize_with(paths.len(), || None);
        Ok(Self {
            paths,
            coverage,
            epoch,
            files: Mutex::new(files),
        })
    }
}

impl ReadAt for RunSource {
    fn read_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
        self.epoch.check()?;
        if bytes.is_empty() {
            return Ok(0);
        }
        let (index, local, wanted, rewritten) =
            self.coverage
                .run_read_window(&self.epoch, offset, bytes.len())?;
        let file = {
            let mut files = self.files.lock().unwrap_or_else(|p| p.into_inner());
            let cached = &mut files[index];
            if cached
                .as_ref()
                .is_none_or(|part| part.rewritten != rewritten)
            {
                *cached = Some(OpenPart {
                    rewritten,
                    file: Arc::new(File::open(&self.paths[index])?),
                });
            }
            Arc::clone(&cached.as_ref().expect("opened part").file)
        };
        self.epoch.check()?;
        #[cfg(unix)]
        let read = { std::os::unix::fs::FileExt::read_at(&*file, &mut bytes[..wanted], local)? };
        #[cfg(windows)]
        let read = {
            use std::os::windows::fs::FileExt;
            file.seek_read(&mut bytes[..wanted], local)?
        };
        self.epoch.check()?;
        if !self
            .coverage
            .commit_read(index, local, read as u64, rewritten)?
        {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "positional source coverage changed",
            ));
        }
        self.epoch.check()?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        Ok(read)
    }

    fn size(&self) -> io::Result<u64> {
        self.epoch.check()?;
        let size = self.coverage.total_len()?;
        self.epoch.check()?;
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positional_source_crosses_parts_and_refuses_old_epoch_after_repair() {
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        std::fs::write(&first, b"abcd").unwrap();
        std::fs::write(&second, b"efgh").unwrap();
        let coverage = Arc::new(SetCoverage::new(2));
        coverage.note_part_len(0, 4);
        coverage.mark_part_complete(0);
        coverage.note_part_len(1, 4);
        coverage.mark_part_complete(1);
        let epoch = coverage.enable_run_replay().unwrap();
        let source =
            RunSource::new(Arc::from([first, second]), Arc::clone(&coverage), epoch).unwrap();
        let mut bytes = [0; 4];
        assert_eq!(source.read_at(&mut bytes, 2).unwrap(), 2);
        assert_eq!(&bytes[..2], b"cd");
        assert_eq!(source.read_at(&mut bytes, 4).unwrap(), 4);
        assert_eq!(&bytes, b"efgh");
        coverage.pause_for_repair();
        assert_eq!(
            source.read_at(&mut bytes, 0).unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
    }
}
