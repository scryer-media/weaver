//! A [`weaver_par2::FileAccess`] over a direct set's virtual volumes
//! (plan 135, D5).
//!
//! PAR2 describes **source volumes**: every file id in the recovery set names a
//! `.partNN.rar`, and every slice checksum is defined at an offset inside it. A
//! direct set has no such file — the bytes live in per-volume envelopes and in
//! the members' `.direct.partial`s — so the verifier needs a `FileAccess` that
//! presents each source volume as if it were on disk. That is this module:
//! [`HybridVolumeProvider`] answers the reads, and everything the PAR2 pass
//! asks about a file (existence, length, ranged reads, a sequential stream) is
//! answered from the virtual volume instead of from `stat` and `open`.
//!
//! Files the set does not own — the PAR2 volumes themselves, and any data file
//! that is not a direct source volume — fall through to the ordinary
//! [`weaver_par2::PlacementFileAccess`] unchanged, so a job that mixes direct
//! and conventional files verifies both in one pass.
//!
//! # A hole is a short file, never zeros
//!
//! [`super::provider`] answers a read that lands on a byte the set never placed
//! with [`super::provider::HoleError`], precisely so nothing mistakes a
//! filesystem hole for data. The PAR2 contract has no way to say "this byte is
//! unknown": what it understands is a **short read**, which is what a truncated
//! file produces and what every read path here turns a hole into. The slice
//! that straddles the hole then fails its checksum and the file is reported
//! damaged — the same verdict the pass reaches for a physically truncated
//! volume, which is what keeps direct and conventional verdicts the same shape.
//! No byte is ever fabricated: a stopped read yields fewer bytes, not zeros.
//!
//! # Writes are refused
//!
//! Repair is phase 6. A virtual volume has nowhere to put a repaired slice —
//! the member bytes belong to a member and the envelope holds the rest — so
//! [`FileAccess::write_file_range`] fails loudly rather than silently writing
//! into a file the set does not own. Wave 2 demotes a damaged direct set and
//! lets the conventional path repair the materialized volumes.

use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use weaver_par2::{FileAccess, FileId, PlacementFileAccess};

use super::provider::{HybridVolumeProvider, VirtualVolumeReader, is_hole};

/// One direct source volume, bound to the PAR2 description that covers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VirtualPar2Volume {
    pub(crate) par2_file_id: FileId,
    pub(crate) volume_index: u32,
}

/// Which read path the pass actually took.
///
/// D5 requires the sequential path to exist, because whole-file MD5 for a set
/// with no IFSC packets — and the batched slice sweep — otherwise degrade into
/// thousands of ranged reads across member partials, and the "no worse than
/// today" claim fails. Counting both is what lets a test prove which one ran.
#[derive(Debug, Default)]
pub(crate) struct DirectAccessCounters {
    sequential_opens: AtomicU64,
    ranged_reads: AtomicU64,
}

impl DirectAccessCounters {
    pub(crate) fn sequential_opens(&self) -> u64 {
        self.sequential_opens.load(Ordering::Relaxed)
    }

    pub(crate) fn ranged_reads(&self) -> u64 {
        self.ranged_reads.load(Ordering::Relaxed)
    }
}

/// A [`FileAccess`] that answers a direct set's source volumes virtually and
/// delegates everything else to `inner`.
pub(crate) struct DirectVolumeFileAccess {
    inner: PlacementFileAccess,
    provider: HybridVolumeProvider,
    volumes: HashMap<FileId, u32>,
    counters: Arc<DirectAccessCounters>,
}

impl DirectVolumeFileAccess {
    pub(crate) fn new(
        inner: PlacementFileAccess,
        provider: HybridVolumeProvider,
        volumes: &[VirtualPar2Volume],
    ) -> Self {
        Self {
            inner,
            provider,
            volumes: volumes
                .iter()
                .map(|volume| (volume.par2_file_id, volume.volume_index))
                .collect(),
            counters: Arc::new(DirectAccessCounters::default()),
        }
    }

    pub(crate) fn counters(&self) -> Arc<DirectAccessCounters> {
        Arc::clone(&self.counters)
    }

    fn volume_index(&self, file_id: &FileId) -> Option<u32> {
        self.volumes.get(file_id).copied()
    }

    /// A reader over one virtual volume, positioned at `offset`, that reports a
    /// hole as end-of-file.
    fn open_at(&self, volume_index: u32, offset: u64) -> io::Result<HoleStoppingReader> {
        let mut reader = self.provider.open(volume_index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("direct-store volume {volume_index} is not registered"),
            )
        })?;
        if offset > 0 {
            reader.seek(SeekFrom::Start(offset))?;
        }
        Ok(HoleStoppingReader { inner: reader })
    }

    fn read_virtual_into(
        &self,
        volume_index: u32,
        offset: u64,
        dst: &mut [u8],
    ) -> io::Result<usize> {
        self.counters.ranged_reads.fetch_add(1, Ordering::Relaxed);
        let mut reader = self.open_at(volume_index, offset)?;
        let mut read = 0usize;
        while read < dst.len() {
            match reader.read(&mut dst[read..])? {
                0 => break,
                n => read += n,
            }
        }
        Ok(read)
    }
}

impl FileAccess for DirectVolumeFileAccess {
    fn read_file_range(&self, file_id: &FileId, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let Some(volume_index) = self.volume_index(file_id) else {
            return self.inner.read_file_range(file_id, offset, len);
        };
        let Ok(len) = usize::try_from(len) else {
            return Ok(Vec::new());
        };
        let mut bytes = vec![0u8; len];
        let read = self.read_virtual_into(volume_index, offset, &mut bytes)?;
        bytes.truncate(read);
        Ok(bytes)
    }

    fn read_file_range_into(
        &self,
        file_id: &FileId,
        offset: u64,
        dst: &mut [u8],
    ) -> io::Result<usize> {
        match self.volume_index(file_id) {
            Some(volume_index) => self.read_virtual_into(volume_index, offset, dst),
            None => self.inner.read_file_range_into(file_id, offset, dst),
        }
    }

    fn open_sequential_reader(&self, file_id: &FileId) -> io::Result<Option<Box<dyn Read>>> {
        let Some(volume_index) = self.volume_index(file_id) else {
            return self.inner.open_sequential_reader(file_id);
        };
        self.counters
            .sequential_opens
            .fetch_add(1, Ordering::Relaxed);
        Ok(Some(Box::new(self.open_at(volume_index, 0)?)))
    }

    /// A virtual volume exists exactly when the set placed a byte of it.
    ///
    /// Deliberately read off coverage rather than off the extent list: a volume
    /// the router knows about but never received a byte for holds nothing, and
    /// reporting it as present would have the pass read a whole file's worth of
    /// holes to conclude what `Missing` says in one call.
    fn file_exists(&self, file_id: &FileId) -> bool {
        match self.volume_index(file_id) {
            Some(volume_index) => self
                .provider
                .volume(volume_index)
                .is_some_and(|volume| !volume.covered.is_empty()),
            None => self.inner.file_exists(file_id),
        }
    }

    /// The volume's logical length — what a downloaded volume file's `stat`
    /// would report. It is the decoded length the download layer tracks, which
    /// is the only length in the coordinate space PAR2 describes; the NZB's
    /// declared totals are yEnc-encoded and never equal it.
    fn file_length(&self, file_id: &FileId) -> Option<u64> {
        match self.volume_index(file_id) {
            Some(volume_index) => self.provider.volume(volume_index).map(|volume| volume.len),
            None => self.inner.file_length(file_id),
        }
    }

    fn read_file(&self, file_id: &FileId) -> io::Result<Vec<u8>> {
        let Some(volume_index) = self.volume_index(file_id) else {
            return self.inner.read_file(file_id);
        };
        self.counters
            .sequential_opens
            .fetch_add(1, Ordering::Relaxed);
        let mut bytes = Vec::new();
        self.open_at(volume_index, 0)?.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    fn write_file_range(&mut self, file_id: &FileId, offset: u64, data: &[u8]) -> io::Result<()> {
        let Some(volume_index) = self.volume_index(file_id) else {
            return self.inner.write_file_range(file_id, offset, data);
        };
        Err(io::Error::other(format!(
            "direct-store volume {volume_index} is virtual and cannot be written (plan 135 \
             phase 6 owns repair); a damaged direct set demotes instead"
        )))
    }
}

/// A [`VirtualVolumeReader`] whose holes read as end-of-file.
struct HoleStoppingReader {
    inner: VirtualVolumeReader,
}

impl Read for HoleStoppingReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        match self.inner.read(out) {
            Ok(read) => Ok(read),
            // The set never placed this byte. `Ok(0)` is the only thing the
            // PAR2 contract understands here, and it is the truthful one: the
            // volume, as far as anything can read it, ends where the hole
            // begins. Zeros would be a lie, and an error would make a partly
            // downloaded volume indistinguishable from a broken disk.
            Err(error) if is_hole(&error) => Ok(0),
            Err(error) => Err(error),
        }
    }
}
