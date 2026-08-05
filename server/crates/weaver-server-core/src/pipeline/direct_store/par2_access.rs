//! A [`par2_rs::FileAccess`] over a direct set's virtual volumes
//! (plan 135, D5/D8).
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
//! [`par2_rs::PlacementFileAccess`] unchanged, so a job that mixes direct
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
//! # An interior hole refuses the sequential path (phase 6)
//!
//! That short-read contract is exact for a **truncated** volume and wrong for a
//! volume with an interior hole. `verify_slices_batched_md5` and
//! `verify_quick_and_full_hash` both prefer [`FileAccess::open_sequential_reader`],
//! and a `Read` has no way to say "skip 64 KiB, then resume": the sweep stops at
//! the first hole and marks every slice after it damaged, however healthy those
//! slices are. A repair sized from that count rebuilds good slices, spends
//! recovery capacity it did not need, and can turn a repairable set into an
//! unrepairable one — the wave-2 review note this phase opens with.
//!
//! So the reader is offered only when the volume's readable image is a prefix
//! (see [`super::provider::VirtualVolume::readable_prefix`]). Otherwise the
//! adapter answers `Ok(None)` and par2-rs falls back to its ranged path,
//! which opens at each slice's own offset and therefore seeks past the hole —
//! damaging exactly the slices that touch it, which is the verdict a physically
//! sparse volume produces. Clean volumes, the overwhelming majority and the only
//! ones where D5's whole-file-MD5 cost argument bites, keep the sequential path.
//!
//! # One reader per volume, kept across ranged reads (plan 136, E2 review F2)
//!
//! The ranged path is the one an encrypted set pays for. A
//! [`super::provider::VirtualVolumeReader`] carries a per-member CBC frontier
//! across its own `read` calls, so a *sweep* through one reader re-encrypts
//! every byte exactly once; open a fresh reader per call and that frontier
//! starts empty every time, and each slice re-seeds from the nearest retained
//! checkpoint — up to [`super::router::crypt::CHECKPOINT_STRIDE`] of plaintext
//! re-encrypted and thrown away *per slice*. Measured on E2's own fixture that
//! was 51,487 delivered bytes against 125,828,800 chained.
//!
//! So the reader is cached per volume and reused. It is taken out of the map
//! for the duration of a read and put back after, which keeps concurrent reads
//! of one volume from serialising on a lock: a caller that finds the slot empty
//! simply opens its own reader, and the last one to finish leaves its frontier
//! behind. Nothing about that is load bearing for correctness — a stale or
//! absent frontier costs a checkpoint seed, never a wrong byte: the reader
//! accepts a frontier only on an exact predecessor match or a strictly forward
//! one that beats the checkpoint, and falls back to the checkpoint otherwise,
//! so a descending or gapped sequence of reads reads exactly as it would have
//! through a reader of its own.
//!
//! # Writes: refused for virtual, allowed for materialized
//!
//! A virtual volume still has nowhere to put a repaired slice — the member bytes
//! belong to a member and the envelope holds the rest — so
//! [`FileAccess::write_file_range`] fails loudly for one rather than silently
//! writing into a file the set does not own. D8's repair-while-direct is what
//! makes that survivable: it materializes *only the damaged volumes* into
//! [`super::plan::DirectSetPlan::repair_path`] scratch files and registers them
//! here as [`MaterializedPar2Volume`]s, so the repairer reads every clean volume
//! virtually and writes only into files that really exist.

use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use par2_rs::{FileAccess, FileId, PlacementFileAccess};

use super::provider::{HybridVolumeProvider, VirtualVolumeReader, is_hole};

/// One direct source volume, bound to the PAR2 description that covers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VirtualPar2Volume {
    pub(crate) par2_file_id: FileId,
    pub(crate) volume_index: u32,
}

/// One damaged direct source volume that has been materialized to a real file
/// so a repair has somewhere to write (plan 135, D8).
///
/// `len` is the volume's decoded length, which is what PAR2 describes; the file
/// is created at exactly that length with holes wherever the set never placed a
/// byte, so a slice the repairer is about to rebuild reads short rather than as
/// fabricated zeros.
#[derive(Debug, Clone)]
pub(crate) struct MaterializedPar2Volume {
    pub(crate) par2_file_id: FileId,
    pub(crate) path: PathBuf,
    pub(crate) len: u64,
}

/// Which read path the pass actually took.
///
/// D5 requires the sequential path to exist, because whole-file MD5 for a set
/// with no IFSC packets — and the batched slice sweep — otherwise degrade into
/// thousands of ranged reads across member partials, and the "no worse than
/// today" claim fails. Counting all three is what lets a test prove which one
/// ran, including the phase-6 refusal that trades the fast path for an accurate
/// per-slice damage count.
#[derive(Debug, Default)]
pub(crate) struct DirectAccessCounters {
    sequential_opens: AtomicU64,
    sequential_refusals: AtomicU64,
    ranged_reads: AtomicU64,
}

impl DirectAccessCounters {
    pub(crate) fn sequential_opens(&self) -> u64 {
        self.sequential_opens.load(Ordering::Relaxed)
    }

    /// Sequential reads refused because the volume has an interior hole, so the
    /// caller re-reads it through the per-slice ranged path instead.
    pub(crate) fn sequential_refusals(&self) -> u64 {
        self.sequential_refusals.load(Ordering::Relaxed)
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
    /// Damaged volumes that have been materialized for a repair. Checked before
    /// [`Self::volumes`], so a volume that is both registered virtually and
    /// materialized reads and writes through the real file.
    materialized: HashMap<FileId, MaterializedPar2Volume>,
    /// One reader per virtual volume, kept across ranged reads so a slice sweep
    /// carries its CBC chain instead of re-seeding every read (E2 review F2).
    ///
    /// Behind a lock only because [`FileAccess`]'s reads take `&self`; the lock
    /// is never held across a read, since the reader is removed for the call and
    /// put back after it.
    readers: Mutex<HashMap<u32, HoleStoppingReader>>,
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
            materialized: HashMap::new(),
            readers: Mutex::new(HashMap::new()),
            counters: Arc::new(DirectAccessCounters::default()),
        }
    }

    /// Registers materialized damaged volumes, which take precedence over the
    /// virtual answer for the same file id (D8).
    pub(crate) fn with_materialized(mut self, volumes: Vec<MaterializedPar2Volume>) -> Self {
        self.materialized = volumes
            .into_iter()
            .map(|volume| (volume.par2_file_id, volume))
            .collect();
        self
    }

    pub(crate) fn counters(&self) -> Arc<DirectAccessCounters> {
        Arc::clone(&self.counters)
    }

    fn volume_index(&self, file_id: &FileId) -> Option<u32> {
        self.volumes.get(file_id).copied()
    }

    fn materialized(&self, file_id: &FileId) -> Option<&MaterializedPar2Volume> {
        self.materialized.get(file_id)
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

    /// The cached reader for `volume_index`, removed from the map so the lock is
    /// released before a byte is read. A miss — first read of the volume, or a
    /// concurrent read holding it — simply opens another one.
    fn take_reader(&self, volume_index: u32) -> Option<HoleStoppingReader> {
        self.readers.lock().ok()?.remove(&volume_index)
    }

    /// Leaves a reader — and the CBC frontier it reached — for the next read.
    fn put_reader(&self, volume_index: u32, reader: HoleStoppingReader) {
        if let Ok(mut readers) = self.readers.lock() {
            readers.insert(volume_index, reader);
        }
    }

    /// A positioned read of a virtual volume, through the volume's **cached**
    /// reader (E2 review F2).
    ///
    /// Reusing the reader is what makes an ascending sweep — which is what a
    /// PAR2 slice pass issues — carry its CBC chain from one slice to the next.
    /// The reader is put back whatever the read returned: a refusal leaves the
    /// chain untouched, and the next read seeks before it reads, so a
    /// half-finished position is not state anything can observe.
    fn read_virtual_into(
        &self,
        volume_index: u32,
        offset: u64,
        dst: &mut [u8],
    ) -> io::Result<usize> {
        self.counters.ranged_reads.fetch_add(1, Ordering::Relaxed);
        let mut reader = match self.take_reader(volume_index) {
            Some(reader) => reader,
            None => self.open_at(volume_index, offset)?,
        };
        let read = reader.read_at(offset, dst);
        self.put_reader(volume_index, reader);
        read
    }

    /// A positioned read of a materialized volume. Short reads are honest: the
    /// file was created at the volume's length with holes where the set placed
    /// nothing, so the repairer's own slice checks decide what to rebuild.
    fn read_materialized_into(
        &self,
        volume: &MaterializedPar2Volume,
        offset: u64,
        dst: &mut [u8],
    ) -> io::Result<usize> {
        self.counters.ranged_reads.fetch_add(1, Ordering::Relaxed);
        if offset >= volume.len {
            return Ok(0);
        }
        let want = ((volume.len - offset) as usize).min(dst.len());
        let mut file = std::fs::File::open(&volume.path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut read = 0usize;
        while read < want {
            match file.read(&mut dst[read..want])? {
                0 => break,
                n => read += n,
            }
        }
        Ok(read)
    }
}

impl FileAccess for DirectVolumeFileAccess {
    fn read_file_range(&self, file_id: &FileId, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        if self.materialized(file_id).is_none() && self.volume_index(file_id).is_none() {
            return self.inner.read_file_range(file_id, offset, len);
        }
        let Ok(len) = usize::try_from(len) else {
            return Ok(Vec::new());
        };
        let mut bytes = vec![0u8; len];
        let read = self.read_file_range_into(file_id, offset, &mut bytes)?;
        bytes.truncate(read);
        Ok(bytes)
    }

    fn read_file_range_into(
        &self,
        file_id: &FileId,
        offset: u64,
        dst: &mut [u8],
    ) -> io::Result<usize> {
        if let Some(volume) = self.materialized(file_id) {
            return self.read_materialized_into(volume, offset, dst);
        }
        match self.volume_index(file_id) {
            Some(volume_index) => self.read_virtual_into(volume_index, offset, dst),
            None => self.inner.read_file_range_into(file_id, offset, dst),
        }
    }

    /// Offered only when a whole-file forward sweep tells the truth about this
    /// volume — see the module docs on interior holes.
    fn open_sequential_reader(&self, file_id: &FileId) -> io::Result<Option<Box<dyn Read>>> {
        if let Some(volume) = self.materialized(file_id) {
            self.counters
                .sequential_opens
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Some(Box::new(std::fs::File::open(&volume.path)?)));
        }
        let Some(volume_index) = self.volume_index(file_id) else {
            return self.inner.open_sequential_reader(file_id);
        };
        if self
            .provider
            .volume(volume_index)
            .is_none_or(super::provider::VirtualVolume::has_interior_hole)
        {
            self.counters
                .sequential_refusals
                .fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
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
    ///
    /// A materialized volume always exists: it was created, at the volume's
    /// length, before this access was built.
    fn file_exists(&self, file_id: &FileId) -> bool {
        if self.materialized(file_id).is_some() {
            return true;
        }
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
        if let Some(volume) = self.materialized(file_id) {
            return Some(volume.len);
        }
        match self.volume_index(file_id) {
            Some(volume_index) => self.provider.volume(volume_index).map(|volume| volume.len),
            None => self.inner.file_length(file_id),
        }
    }

    fn read_file(&self, file_id: &FileId) -> io::Result<Vec<u8>> {
        if let Some(volume) = self.materialized(file_id) {
            self.counters
                .sequential_opens
                .fetch_add(1, Ordering::Relaxed);
            return std::fs::read(&volume.path);
        }
        let Some(volume_index) = self.volume_index(file_id) else {
            return self.inner.read_file(file_id);
        };
        // Deliberately still a forward sweep, hole rule and all: `read_file`
        // asks for one contiguous buffer, which is a question an interior hole
        // has no honest answer to. Nothing in verification or repair calls it —
        // both go through the ranged and sequential paths above — so the
        // per-slice attribution the interior-hole rule protects is unaffected.
        self.counters
            .sequential_opens
            .fetch_add(1, Ordering::Relaxed);
        let mut bytes = Vec::new();
        self.open_at(volume_index, 0)?.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    fn write_file_range(&mut self, file_id: &FileId, offset: u64, data: &[u8]) -> io::Result<()> {
        if let Some(volume) = self.materialized.get(file_id) {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .truncate(false)
                .open(&volume.path)?;
            return write_all_at(&file, offset, data);
        }
        let Some(volume_index) = self.volume_index(file_id) else {
            return self.inner.write_file_range(file_id, offset, data);
        };
        Err(io::Error::other(format!(
            "direct-store volume {volume_index} is virtual and cannot be written; D8 \
             materializes a damaged volume before repairing it"
        )))
    }
}

/// Positioned write, so the repairer's out-of-order slice writes need no seek
/// discipline and no exclusive handle.
fn write_all_at(file: &std::fs::File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(bytes, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut written = 0usize;
        while written < bytes.len() {
            let progress = file.seek_write(&bytes[written..], offset + written as u64)?;
            if progress == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "positional write reported no progress",
                ));
            }
            written += progress;
        }
        Ok(())
    }
}

/// A [`VirtualVolumeReader`] whose holes read as end-of-file.
struct HoleStoppingReader {
    inner: VirtualVolumeReader,
}

impl HoleStoppingReader {
    /// Fills `dst` from `offset`, stopping short at the volume's end or at a
    /// hole. Seeking rather than assuming the position is what lets one reader
    /// answer an arbitrary sequence of ranged reads.
    fn read_at(&mut self, offset: u64, dst: &mut [u8]) -> io::Result<usize> {
        self.inner.seek(SeekFrom::Start(offset))?;
        let mut read = 0usize;
        while read < dst.len() {
            match self.read(&mut dst[read..])? {
                0 => break,
                n => read += n,
            }
        }
        Ok(read)
    }
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
