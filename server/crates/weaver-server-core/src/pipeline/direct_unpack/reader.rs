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

use super::coverage::{PositionInPart, SetCoverage};

/// One part file, opened on first use.
#[derive(Debug)]
struct Part {
    path: PathBuf,
    /// Opened lazily: a later part often does not exist on disk yet when the
    /// reader is built, and opening it eagerly would fail the whole set.
    file: Option<File>,
    /// The coverage's rewrite count for this part when `file` was opened.
    /// Repair installs a new file at the path rather than writing into the
    /// old one, so a handle from before a repair reads the file that was moved
    /// aside; when the count moves on, the path is opened again.
    opened_rewritten: u64,
    /// Cached boundary length. Coverage aborts if repair changes a boundary
    /// already used by this mapping walk.
    len: Option<u64>,
}

/// Coverage-gated reader over the ordered parts of one 7z set.
#[derive(Debug)]
pub struct GatedSplitReader {
    parts: Vec<Part>,
    coverage: Arc<SetCoverage>,
    position: u64,
    /// Sequential formats discover EOF from completed parts, without an
    /// archive-wide length declaration or a seek to the tail.
    sequential: bool,
    /// Cached signature-derived total. Coverage rejects contradictions; reads
    /// still check for abort and repair pauses, including at cached EOF.
    total_len: Option<u64>,
    /// Tests stop a disk read before it publishes consumption.
    #[cfg(test)]
    read_barrier: Option<(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
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
                    opened_rewritten: 0,
                    len: None,
                })
                .collect(),
            coverage,
            position: 0,
            sequential: false,
            total_len: None,
            #[cfg(test)]
            read_barrier: None,
        })
    }

    /// Read a sequential archive before its final size is known. Missing
    /// bytes still park, and only a completed final part supplies EOF.
    pub fn open_sequential(
        paths: &[impl AsRef<Path>],
        coverage: Arc<SetCoverage>,
    ) -> io::Result<impl Read> {
        let mut reader = Self::open(paths, coverage)?;
        reader.sequential = true;
        Ok(reader)
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

    /// Map an archive offset onto a part, the offset within it, and how many
    /// committed bytes follow.
    ///
    /// Walks the parts accumulating their lengths. Only parts the offset lies
    /// *past* need a settled length; the part the offset lands in needs only a
    /// watermark that has reached it, which is what lets the reader stream into
    /// a part that is still downloading. `Ok(None)` means the offset is at or
    /// past the end of the last part. The last element is the part's rewrite
    /// count from the same answer, for [`Self::file_for`].
    fn locate(&mut self, position: u64) -> io::Result<Option<(usize, u64, u64, u64)>> {
        let total = if self.sequential {
            u64::MAX
        } else {
            self.total_len()?
        };
        let mut start = 0u64;

        for index in 0..self.parts.len() {
            let local = position - start;
            // Coverage rejects changes to a boundary already used here.
            let resolved = match self.parts[index].len {
                Some(len) if local >= len => PositionInPart::Beyond { len },
                _ => self.coverage.resolve_position(index, local)?,
            };

            match resolved {
                PositionInPart::Inside {
                    available,
                    rewritten,
                } => {
                    return Ok(Some((index, local, available, rewritten)));
                }
                PositionInPart::Beyond { len } => {
                    self.parts[index].len = Some(len);
                    let end = start.checked_add(len).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "part lengths overflow the archive offset space at part {index}"
                            ),
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
                    start = end;
                }
            }
        }

        Ok(None)
    }

    /// The open handle for a part: opened on first use, and opened again after
    /// every repair of it.
    ///
    /// `rewritten` is the coverage's count for this part from the same
    /// `resolve_position` answer that placed the read, so the handle is judged
    /// against the moment the bytes it is about to serve were placed, never
    /// against a later or earlier one.
    fn file_for(&mut self, index: usize, rewritten: u64) -> io::Result<&mut File> {
        let part = &mut self.parts[index];
        if part.file.is_some() && part.opened_rewritten != rewritten {
            // The path leads to the repaired file now; the handle still leads
            // to the damaged one that was moved aside.
            part.file = None;
        }
        if part.file.is_none() {
            let file = File::open(&part.path)?;
            part.file = Some(file);
            part.opened_rewritten = rewritten;
        }
        Ok(part.file.as_mut().expect("just opened the part file"))
    }
}

impl Read for GatedSplitReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        loop {
            let total = if self.sequential {
                u64::MAX
            } else {
                self.total_len()?
            };
            if self.position >= total {
                self.coverage.wait_for_read()?;
                return Ok(0);
            }

            // Parks inside `locate` until the download has carried the target part
            // past this offset, or the part ends and the walk moves on.
            let Some((index, local, available, rewritten)) = self.locate(self.position)? else {
                self.coverage.wait_for_read()?;
                return Ok(0);
            };

            // `locate` only ever reports `Inside` with at least one byte behind the
            // watermark; a part that ends at this offset comes back as `Beyond` and
            // the walk moves on. A short part is caught in `resolve_position`.
            debug_assert!(available > 0, "locate returned an empty readable window");

            let remaining = total - self.position;
            let wanted = buf
                .len()
                .min(available.min(remaining).try_into().unwrap_or(usize::MAX));

            let result = (|| {
                let file = self.file_for(index, rewritten)?;
                file.seek(SeekFrom::Start(local))?;
                file.read(&mut buf[..wanted])
            })();
            #[cfg(test)]
            if let Some((entered, resume)) = self.read_barrier.take() {
                entered.send(()).expect("read barrier observer");
                resume
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .expect("read barrier release");
            }
            let read = result.as_ref().copied().unwrap_or(0);
            if !self
                .coverage
                .commit_read(index, local, read as u64, rewritten)?
            {
                continue;
            }
            let read = result?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("part {index} ended at {local} inside committed archive coverage"),
                ));
            }
            self.position += read as u64;
            return Ok(read);
        }
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

#[cfg(test)]
#[path = "reader_repair_tests.rs"]
mod repair_tests;
