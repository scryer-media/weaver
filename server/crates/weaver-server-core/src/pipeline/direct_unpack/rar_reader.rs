//! Volume-addressed RAR input over the existing chase coverage lifecycle.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use super::coverage::SetCoverage;

/// Each open has an independent file cursor. Coverage and repair generations
/// are shared with the actor; a sparse file's length never admits its holes.
pub(crate) struct RarVolumeProvider {
    pub paths: Vec<PathBuf>,
    pub coverage: Arc<SetCoverage>,
}

impl unrar_rs::VolumeProvider for RarVolumeProvider {
    fn get_volume(
        &self,
        index: usize,
    ) -> Result<Box<dyn unrar_rs::ReadSeek>, unrar_rs::VolumeProviderError> {
        let path =
            self.paths
                .get(index)
                .ok_or_else(|| unrar_rs::VolumeProviderError::Unavailable {
                    volume: index,
                    reason: "volume is outside the RAR topology".into(),
                })?;
        Ok(Box::new(RarVolumeReader {
            path: path.clone(),
            coverage: Arc::clone(&self.coverage),
            index,
            position: 0,
            file: None,
        }))
    }
}

struct RarVolumeReader {
    path: PathBuf,
    coverage: Arc<SetCoverage>,
    index: usize,
    position: u64,
    file: Option<(u64, File)>,
}

impl Read for RarVolumeReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            self.coverage.wait_for_read()?;
            let available = self.coverage.readable_at(self.index, self.position)?;
            if available == 0 {
                return if self
                    .coverage
                    .part_progress(self.index)?
                    .len
                    .is_some_and(|len| self.position >= len)
                {
                    Ok(0)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "completed RAR volume contains an input hole",
                    ))
                };
            }
            let generation = self.coverage.part_progress(self.index)?.rewritten;
            if self
                .file
                .as_ref()
                .is_none_or(|(opened, _)| *opened != generation)
            {
                self.file = Some((generation, File::open(&self.path)?));
            }
            let wanted = out
                .len()
                .min(usize::try_from(available).unwrap_or(usize::MAX));
            let (_, file) = self.file.as_mut().expect("opened above");
            let result = file
                .seek(SeekFrom::Start(self.position))
                .and_then(|_| file.read(&mut out[..wanted]));
            let read = result.as_ref().copied().unwrap_or(0);
            if !self
                .coverage
                .commit_read(self.index, self.position, read as u64, generation)?
            {
                continue;
            }
            let read = result?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "RAR input ended inside committed coverage",
                ));
            }
            self.position += read as u64;
            return Ok(read);
        }
    }
}

impl Seek for RarVolumeReader {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        self.coverage.wait_for_read()?;
        let position = match from {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
            SeekFrom::End(offset) => self
                .coverage
                .part_progress(self.index)?
                .len
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::Unsupported,
                        "RAR volume length is not known yet",
                    )
                })?
                .checked_add_signed(offset),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "RAR seek overflow"))?;
        self.position = position;
        Ok(position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use unrar_rs::VolumeProvider;

    fn input(bytes: &[u8]) -> (tempfile::TempDir, RarVolumeProvider) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part.rar");
        std::fs::write(&path, bytes).unwrap();
        let provider = RarVolumeProvider {
            paths: vec![path],
            coverage: Arc::new(SetCoverage::new(1)),
        };
        (dir, provider)
    }

    fn wait_for_park(coverage: &SetCoverage) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while coverage.park_count() == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            coverage.park_count() > 0,
            "reader did not reach the input gap"
        );
    }

    #[test]
    fn sparse_file_length_never_publishes_a_hole() {
        let (_dir, provider) = input(b"abcdefgh");
        provider.coverage.note_committed_range(0, 0, 3);
        provider.coverage.note_committed_range(0, 5, 8);
        let mut reader = provider.get_volume(0).unwrap();
        let (tx, rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut bytes = [0; 8];
            let result = reader.read_exact(&mut bytes).map(|()| bytes);
            tx.send(result).unwrap();
        });
        wait_for_park(&provider.coverage);
        assert!(rx.try_recv().is_err());
        provider.coverage.note_committed_range(0, 3, 5);
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap(),
            *b"abcdefgh"
        );
        worker.join().unwrap();
    }

    #[test]
    fn cancellation_wakes_a_reader_waiting_for_a_header() {
        let (_dir, provider) = input(b"abcdefgh");
        let mut reader = provider.get_volume(0).unwrap();
        let worker = std::thread::spawn(move || reader.read(&mut [0; 8]));
        wait_for_park(&provider.coverage);
        provider.coverage.abort("job cancelled");
        assert!(
            worker
                .join()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
    }

    #[test]
    fn completed_hole_is_an_error_and_true_end_is_eof() {
        let (_dir, provider) = input(b"abcdefgh");
        provider.coverage.note_committed_range(0, 0, 3);
        provider.coverage.finish_ranged_part(0, 8);
        let mut reader = provider.get_volume(0).unwrap();
        reader.seek(SeekFrom::Start(3)).unwrap();
        assert_eq!(
            reader.read(&mut [0; 1]).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        reader.seek(SeekFrom::Start(8)).unwrap();
        assert_eq!(reader.read(&mut [0; 1]).unwrap(), 0);
    }

    #[test]
    fn repaired_tail_reopens_the_volume_at_the_existing_cursor() {
        let (_dir, provider) = input(b"abcXXXXX");
        provider.coverage.advance_watermark(0, 3);
        let mut reader = provider.get_volume(0).unwrap();
        let mut head = [0; 3];
        reader.read_exact(&mut head).unwrap();
        provider.coverage.pause_for_repair();
        std::fs::write(&provider.paths[0], b"abcdefgh").unwrap();
        provider.coverage.release_after_repair(0, 8);
        provider.coverage.resume_after_repair();
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).unwrap();
        assert_eq!(head, *b"abc");
        assert_eq!(tail, b"defgh");
        assert_eq!(provider.coverage.consumed_high_water(0), 8);
    }
}
