//! A `Read + Seek` view of a 7z set that is still downloading.
//!
//! Same shape as [`SplitFileReader`](crate::pipeline::archive::split_reader::SplitFileReader):
//! ordered part files presented as one contiguous archive stream. The
//! difference is what happens at the end of the bytes. `SplitFileReader` opens
//! finished files and a read past the end is simply end-of-file; here the files
//! are still growing, so a read past the verified watermark parks until the
//! download delivers more, and only an abort or a genuinely finished part ends
//! it.
//!
//! # Arbitrary access is fine; only the frontier blocks
//!
//! The parts are on disk, so every byte below a part's watermark stays readable
//! for the life of the set. Backward seeks, re-reads, and interleaved cursors
//! are all served straight from the file — the gate is a frontier, not a
//! ratchet. That is what lets this reader sit under a decoder whose access
//! pattern weaver does not control: a chain that reads strictly forward simply
//! never waits longer than the download, and one that jumps around still gets
//! correct bytes, at worst waiting for the furthest offset it asks for.
//!
//! Blocking is by design and belongs on a blocking thread — the same
//! `spawn_blocking` context that finalize-time extraction already uses.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::coverage::SetCoverage;

/// One part file, opened on first use.
#[derive(Debug)]
struct Part {
    path: PathBuf,
    /// Opened lazily: a later part often does not exist on disk yet when the
    /// reader is built, and opening it eagerly would fail the whole set.
    file: Option<File>,
    /// Cached part length. A declared length never changes, so one lookup is
    /// enough and the mapping walk stays off the shared lock.
    len: Option<u64>,
}

/// Coverage-gated reader over the ordered parts of one 7z set.
#[derive(Debug)]
pub struct GatedSplitReader {
    parts: Vec<Part>,
    coverage: Arc<SetCoverage>,
    position: u64,
    /// Cached archive total. Set once on the coverage and never changed, so one
    /// successful read of it is good for the reader's lifetime — which keeps
    /// every subsequent read and seek off the shared lock. An abort still
    /// reaches the reader through `part_len` and `readable_at`, both of which
    /// are consulted on the way to any actual byte.
    total_len: Option<u64>,
}

impl GatedSplitReader {
    /// Build a reader over `paths`, gated by `coverage`.
    ///
    /// `paths` must be in archive order and must match the part count the
    /// coverage was created with; the two describe the same set and a mismatch
    /// would silently misplace every offset.
    pub fn open(paths: &[impl AsRef<Path>], coverage: Arc<SetCoverage>) -> io::Result<Self> {
        if paths.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no files provided to GatedSplitReader",
            ));
        }
        if paths.len() != coverage.part_count() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "GatedSplitReader given {} parts but coverage tracks {}",
                    paths.len(),
                    coverage.part_count()
                ),
            ));
        }

        Ok(Self {
            parts: paths
                .iter()
                .map(|path| Part {
                    path: path.as_ref().to_path_buf(),
                    file: None,
                    len: None,
                })
                .collect(),
            coverage,
            position: 0,
            total_len: None,
        })
    }

    /// Current offset in the concatenated archive stream.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// The coverage this reader is gated by.
    pub fn coverage(&self) -> &Arc<SetCoverage> {
        &self.coverage
    }

    /// The archive's total length, parking only on the first call.
    fn total_len(&mut self) -> io::Result<u64> {
        if let Some(total) = self.total_len {
            return Ok(total);
        }
        let total = self.coverage.total_len()?;
        self.total_len = Some(total);
        Ok(total)
    }

    fn part_len(&mut self, index: usize) -> io::Result<u64> {
        if let Some(len) = self.parts[index].len {
            return Ok(len);
        }
        let len = self.coverage.part_len(index)?;
        self.parts[index].len = Some(len);
        Ok(len)
    }

    /// Map an archive offset onto a part and an offset within it.
    ///
    /// Parks on any part length it needs and does not have: where part `k`
    /// starts is not knowable until every part before it has declared a length.
    /// `Ok(None)` means the offset is at or past the end of the last part.
    fn locate(&mut self, position: u64) -> io::Result<Option<(usize, u64)>> {
        let total = self.total_len()?;
        let mut start = 0u64;

        for index in 0..self.parts.len() {
            let len = self.part_len(index)?;
            let end = start.checked_add(len).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("part lengths overflow the archive offset space at part {index}"),
                )
            })?;
            if end > total {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "parts through {index} span {end} bytes, past the declared archive length {total}"
                    ),
                ));
            }
            if position < end {
                return Ok(Some((index, position - start)));
            }
            start = end;
        }

        Ok(None)
    }

    fn file_for(&mut self, index: usize) -> io::Result<&mut File> {
        if self.parts[index].file.is_none() {
            let file = File::open(&self.parts[index].path)?;
            self.parts[index].file = Some(file);
        }
        Ok(self.parts[index]
            .file
            .as_mut()
            .expect("just opened the part file"))
    }
}

impl Read for GatedSplitReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let total = self.total_len()?;
        if self.position >= total {
            return Ok(0);
        }

        let Some((index, local)) = self.locate(self.position)? else {
            return Ok(0);
        };

        // Parks here until the download has carried this part past `local`.
        let available = self.coverage.readable_at(index, local)?;
        if available == 0 {
            let progress = self.coverage.part_progress(index)?;
            return Err(io::Error::other(format!(
                "part {index} finished at {} bytes, short of its declared length {:?}",
                progress.watermark, progress.len
            )));
        }

        let remaining = total - self.position;
        let wanted = buf
            .len()
            .min(available.min(remaining).try_into().unwrap_or(usize::MAX));

        let file = self.file_for(index)?;
        file.seek(SeekFrom::Start(local))?;
        let read = file.read(&mut buf[..wanted])?;
        self.position += read as u64;
        Ok(read)
    }
}

impl Seek for GatedSplitReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        // Every branch needs the total: `End` is relative to it, and the other
        // two are validated against it.
        let total = self.total_len()?;

        let target = match pos {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::End(offset) => total.checked_add_signed(offset),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
        };

        let Some(target) = target else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to a negative or overflowing position",
            ));
        };

        // The signature header makes the archive length exact, so an offset
        // past it is a mapping bug rather than an ordinary read past EOF.
        // Failing at the seek names it; allowing it would surface later as a
        // read that parks on bytes that are never coming.
        if target > total {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("seek to {target} is past the declared archive length {total}"),
            ));
        }

        self.position = target;
        Ok(self.position)
    }
}
