//! The hybrid virtual-volume provider (plan 135, D5/D8).
//!
//! A direct set has no volume files, and several things downstream of routing
//! insist on reading one: PAR2 verification and repair, `extract_member_streaming`
//! for a tolerated member, and demotion's byte-exact reconstruction. This module
//! is what gives them a volume to read.
//!
//! A virtual volume is an **overlay of two on-disk images in one coordinate
//! space** — the source volume's physical offsets:
//!
//! - the volume's envelope file is that space, minus the bytes routing carried
//!   away: `<set>.vol00007.envelope` holds every non-member byte at its true
//!   physical offset, with sparse holes where member data used to be;
//! - each direct-routed member's `.direct.partial` fills those holes, read at
//!   `logical_offset + (physical position - extent start)`.
//!
//! The extents come from [`DirectSetRouter::volume_member_extents`], which is
//! `StoredLayoutBuilder::map_physical_range` run in reverse: routing asks it
//! "which member owns this physical byte", and the provider asks the same
//! question to decide which file to read the byte back from.
//!
//! # Holes are errors, never zeros
//!
//! Bytes that were never downloaded — or were downloaded, held, and lost — are
//! present in neither image. A reader that answered them with zeros would let a
//! PAR2 block, a header walk or a reconstruction sweep silently consume
//! fabricated data. Every read that starts inside a hole fails with
//! [`HoleError`], which [`is_hole`] recognises so a caller can tell "not
//! downloaded yet" from "the disk is broken". This is the on-disk sibling of the
//! in-memory `SparseImage` the router's header parser walks, with one deliberate
//! difference: that image answers `Ok(0)` at a hole so a truncated header walk
//! stops cleanly, while this one is read by callers that must never mistake a
//! hole for the end of the data.
//!
//! Coverage is therefore tracked **per source, not per volume** (B1). Knowing
//! that a physical byte was placed says nothing about *which* file received it,
//! and the envelope is a sparse file: a read at an offset the envelope never
//! received returns the filesystem's zeros, indistinguishable from real data, as
//! long as some later offset made the file that long. So the envelope answers
//! only for [`VirtualVolume::envelope_covered`] — the ranges an envelope write
//! actually recorded — and a member extent answers only inside itself. A byte
//! the volume-level map calls covered but no source claims is a hole, which is
//! the invariant this module's title states, held unconditionally rather than as
//! a consequence of the extent list happening to be right.
//!
//! # The re-encrypting overlay (plan 136, E-D4)
//!
//! For an **encrypted** set the two images no longer agree with the volume they
//! describe. The envelope still holds what was posted — headers, service
//! records, and the last cipher block's tail padding — but a routed member's
//! `.direct.partial` holds its *plaintext*, because plan 136 decrypts at write
//! time so the payload lands once. Every caller of this module wants posted
//! bytes: PAR2 checksums them, reconstruction writes them into a volume file, a
//! repair reads them as Reed–Solomon inputs.
//!
//! So a member extent belonging to an encrypted member is re-encrypted on the
//! way out ([`VirtualVolume::ciphers`]). AES-CBC encryption is deterministic
//! given key, IV and plaintext, so the posted stream is always reproducible —
//! the only question is where a read's CBC chain starts, since block *N*'s
//! cipher needs block *N−1*'s:
//!
//! - a **sequential** sweep chains naturally from each member's start, and
//!   [`VirtualVolumeReader::chains`] carries the frontier from one `read` to the
//!   next so a whole-volume pass re-encrypts every byte exactly once;
//! - a **ranged** read seeds from the nearest retained cipher checkpoint at or
//!   below its offset ([`super::router::crypt::MemberCipher::seed`]), and where
//!   there is none it chains from the member's IV — the sequential path, bounded
//!   and honest, rather than a guessed predecessor.
//!
//! Three things make a read **refuse** rather than fabricate, all of them
//! reported as [`HoleError`] because "refetch this" is exactly what they mean:
//! plaintext missing anywhere between the seed and the requested bytes; a read
//! touching the member's final cipher block with no retained tail padding; and a
//! member extent whose member has no cipher facts at all.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use unrar_rs::{ReadSeek, VolumeProvider, VolumeProviderError};

use super::ByteRanges;
use super::router::MemberExtent;
use super::router::crypt::{MemberCipher, block_ceil, block_floor};

/// How much plaintext a chain-to-seed pass re-encrypts per iteration.
///
/// Only the last 16 bytes of each pass survive it, so this bounds the transient
/// buffer a checkpoint miss costs — not the work, which is whatever the distance
/// to the seed is.
const CHAIN_CHUNK_BYTES: usize = 256 * 1024;

/// A read landed on a byte the set never placed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HoleError {
    pub(crate) volume_index: u32,
    pub(crate) offset: u64,
}

impl std::fmt::Display for HoleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "direct-store volume {} has no byte at offset {}",
            self.volume_index, self.offset
        )
    }
}

impl std::error::Error for HoleError {}

/// Whether an I/O error is a virtual volume reporting a hole rather than a real
/// failure. A hole means "refetch this"; anything else means the disk is wrong.
pub(crate) fn is_hole(error: &std::io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|inner| inner.downcast_ref::<HoleError>().is_some())
}

/// The end of the coalesced run of `ranges` containing `position`, or `None`
/// when `position` is outside every run.
fn covered_run_end(ranges: &ByteRanges, position: u64) -> Option<u64> {
    let runs = ranges.ranges();
    let index = runs
        .partition_point(|(start, _)| *start <= position)
        .checked_sub(1)?;
    let (_, end) = runs[index];
    (position < end).then_some(end)
}

fn hole(volume_index: u32, offset: u64) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        HoleError {
            volume_index,
            offset,
        },
    )
}

/// Everything one virtual volume needs, with no borrows, so the whole provider
/// can be moved onto the blocking pool.
#[derive(Debug, Clone)]
pub(crate) struct VirtualVolume {
    pub(crate) volume_index: u32,
    /// Absolute path of this volume's sparse envelope file.
    pub(crate) envelope: PathBuf,
    /// Direct-routed member extents in physical order, disjoint. The order is
    /// what makes the binary searches in [`VirtualVolumeReader`] valid, and
    /// `map_physical_range` produces it that way.
    pub(crate) extents: Vec<MemberExtent>,
    /// Absolute `.direct.partial` path per member id.
    ///
    /// Shared rather than owned: every volume of a set resolves member ids
    /// against the same map, and the provider only ever reads it.
    pub(crate) partials: Arc<HashMap<u32, PathBuf>>,
    /// Physical ranges that were actually placed. Everything else is a hole.
    pub(crate) covered: ByteRanges,
    /// Of `covered`, the physical ranges the **envelope file** received.
    ///
    /// Never derived here from `covered` minus the extents: the whole failure
    /// this exists to stop is an extent going missing, and a derivation would
    /// hand the missing member's range straight back to the envelope. It comes
    /// from the writes recorded against the envelope destination, so a range no
    /// envelope write ever claimed reads as a hole no matter what the extent
    /// list says.
    pub(crate) envelope_covered: ByteRanges,
    /// Logical length of the volume: what a `SeekFrom::End` means and where
    /// reads stop returning bytes.
    pub(crate) len: u64,
    /// Read-side crypt facts per member id, for the re-encrypting overlay
    /// (plan 136, E-D4). Empty for every unencrypted set, which is the overlay
    /// switched off by construction.
    ///
    /// Shared with the partial paths and for the same reason: every volume of a
    /// set resolves the same member ids, and the facts carry a checkpoint map
    /// that is not free to clone per volume.
    pub(crate) ciphers: Arc<HashMap<u32, MemberCipher>>,
}

/// What the re-encrypting overlay did, so a test can prove which path a read
/// took rather than only that it produced the right bytes (plan 136 open
/// question 1).
#[derive(Debug, Default)]
pub(crate) struct CipherOverlayCounters {
    reencrypted_bytes: AtomicU64,
    chained_bytes: AtomicU64,
    seeded_from_checkpoint: AtomicU64,
    seeded_from_start: AtomicU64,
    refusals: AtomicU64,
}

impl CipherOverlayCounters {
    /// Member bytes the overlay turned back into cipher and handed to a caller.
    pub(crate) fn reencrypted_bytes(&self) -> u64 {
        self.reencrypted_bytes.load(Ordering::Relaxed)
    }

    /// Bytes re-encrypted **only** to reach a read's CBC seed and then thrown
    /// away. The whole cost of a checkpoint miss, and the number
    /// [`super::router::crypt::CHECKPOINT_STRIDE`] exists to bound.
    pub(crate) fn chained_bytes(&self) -> u64 {
        self.chained_bytes.load(Ordering::Relaxed)
    }

    /// Reads whose chain started at a retained checkpoint or at the frontier the
    /// previous read left, i.e. those that paid nothing to seed.
    pub(crate) fn seeded_from_checkpoint(&self) -> u64 {
        self.seeded_from_checkpoint.load(Ordering::Relaxed)
    }

    /// Reads that had to chain from the member's IV — the sequential fallback.
    pub(crate) fn seeded_from_start(&self) -> u64 {
        self.seeded_from_start.load(Ordering::Relaxed)
    }

    /// Reads the overlay refused rather than fabricate: missing plaintext below
    /// the requested bytes, or a final block with no retained tail padding.
    pub(crate) fn refusals(&self) -> u64 {
        self.refusals.load(Ordering::Relaxed)
    }
}

impl VirtualVolume {
    /// The physical ranges a [`VirtualVolumeReader`] can actually answer, in
    /// order: covered, inside the volume, and claimed by a source that holds
    /// them — a member extent, or an envelope write that really happened.
    ///
    /// This is [`VirtualVolumeReader::run_end`]'s decision, hoisted out of the
    /// read loop so a caller can ask about the volume's *shape* without reading
    /// a byte of it.
    pub(crate) fn readable_ranges(&self) -> Vec<(u64, u64)> {
        let mut sources = ByteRanges::new();
        for extent in &self.extents {
            sources.insert(extent.physical_offset, extent.len);
        }
        for &(start, end) in self.envelope_covered.ranges() {
            sources.insert(start, end.saturating_sub(start));
        }

        let mut readable = Vec::new();
        for &(start, end) in self.covered.ranges() {
            let end = end.min(self.len);
            if end <= start {
                continue;
            }
            for &(source_start, source_end) in sources.ranges() {
                let overlap = (source_start.max(start), source_end.min(end));
                if overlap.0 < overlap.1 {
                    match readable.last_mut() {
                        Some((_, last_end)) if *last_end == overlap.0 => *last_end = overlap.1,
                        _ => readable.push(overlap),
                    }
                }
            }
        }
        readable
    }

    /// `Some(end)` when everything readable is one run from zero — the volume
    /// reads exactly like a whole or truncated file — and `None` when an
    /// **interior hole** sits below readable bytes.
    ///
    /// The distinction is the one D5's `FileAccess` adapter turns on, and it is
    /// a damage-accounting fact rather than a performance one. A sequential
    /// reader has no way to say "these bytes are unknown, the next ones are
    /// fine": it stops at the hole, and every PAR2 slice after it reads zero
    /// bytes and is counted damaged. Sizing a repair from that count rebuilds
    /// slices that were never broken — wasting recovery capacity, and able to
    /// flip a repairable set to unrepairable. A ranged read seeks past the hole
    /// and attributes damage to the slices that actually touch it, which is
    /// exactly the verdict a physically sparse volume produces, so refusing the
    /// sequential path here is what keeps direct and conventional verdicts the
    /// same shape.
    pub(crate) fn readable_prefix(&self) -> Option<u64> {
        match self.readable_ranges().as_slice() {
            [] => Some(0),
            [(0, end)] => Some(*end),
            _ => None,
        }
    }

    /// Whether an interior hole makes a sequential sweep lie about which slices
    /// are damaged. The inverse of [`Self::readable_prefix`], named for the
    /// question the adapter asks.
    pub(crate) fn has_interior_hole(&self) -> bool {
        self.readable_prefix().is_none()
    }
}

/// A [`VolumeProvider`] over a direct set's partials and envelopes.
#[derive(Debug, Clone, Default)]
pub(crate) struct HybridVolumeProvider {
    volumes: HashMap<u32, VirtualVolume>,
    /// Shared across clones on purpose: the provider is cloned into every
    /// `spawn_blocking` that reads it, and the overlay accounting is about the
    /// whole pass rather than about one closure.
    cipher_counters: Arc<CipherOverlayCounters>,
}

impl HybridVolumeProvider {
    pub(crate) fn new(volumes: Vec<VirtualVolume>) -> Self {
        Self {
            volumes: volumes
                .into_iter()
                .map(|volume| (volume.volume_index, volume))
                .collect(),
            cipher_counters: Arc::new(CipherOverlayCounters::default()),
        }
    }

    /// What the re-encrypting overlay has done through this provider.
    pub(crate) fn cipher_counters(&self) -> Arc<CipherOverlayCounters> {
        Arc::clone(&self.cipher_counters)
    }

    /// The registered shape of one virtual volume. Wave 2's PAR2 adapter reads
    /// existence and length off it: a `FileAccess` has to answer both without
    /// touching the filesystem, because for a direct volume there is nothing
    /// there to `stat`.
    pub(crate) fn volume(&self, volume_index: u32) -> Option<&VirtualVolume> {
        self.volumes.get(&volume_index)
    }

    /// Opens one virtual volume directly, without the trait's `usize` index and
    /// boxing. Reconstruction and this module's tests use it.
    pub(crate) fn open(&self, volume_index: u32) -> Option<VirtualVolumeReader> {
        self.volumes
            .get(&volume_index)
            .cloned()
            .map(|volume| VirtualVolumeReader::new(volume, Arc::clone(&self.cipher_counters)))
    }
}

impl VolumeProvider for HybridVolumeProvider {
    fn get_volume(&self, index: usize) -> Result<Box<dyn ReadSeek>, VolumeProviderError> {
        let volume_index = u32::try_from(index).map_err(|_| VolumeProviderError::Unavailable {
            volume: index,
            reason: "volume index out of range".into(),
        })?;
        let reader = self
            .open(volume_index)
            .ok_or_else(|| VolumeProviderError::Unavailable {
                volume: index,
                reason: "not a direct-routed volume of this set".into(),
            })?;
        Ok(Box::new(reader))
    }
}

/// Which file answers one physical byte, and how much of the request it can
/// answer in one go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// The member partial for `member_id`, starting at `offset` inside it.
    Member { member_id: u32, offset: u64 },
    /// The envelope file, at the same physical offset.
    Envelope { offset: u64 },
}

/// A seekable reader over one virtual volume.
///
/// File handles are opened lazily and kept for the reader's life: a sequential
/// sweep of a multi-member volume alternates between the envelope and each
/// partial many times, and reopening per read would turn one pass into thousands
/// of `open` calls.
pub(crate) struct VirtualVolumeReader {
    volume: VirtualVolume,
    position: u64,
    envelope_handle: Option<File>,
    partial_handles: HashMap<u32, File>,
    /// Per-member CBC frontier: the cipher offset the last re-encryption ended
    /// at and the 16 cipher bytes ending there (plan 136, E-D4).
    ///
    /// This is what makes a sequential sweep of an encrypted volume linear:
    /// every `read` continues the previous one's chain instead of seeding
    /// itself. It also carries a *ranged* caller forward when its reads happen
    /// to ascend, which a PAR2 slice sweep's do.
    chains: HashMap<u32, CipherChain>,
    counters: Arc<CipherOverlayCounters>,
}

/// One member's CBC frontier inside a reader.
///
/// Two seeds, not one, because a read rarely ends on a block boundary: a
/// sequential sweep's runs are article- and extent-shaped, so the next read
/// usually starts *inside* the last block this one produced. `frontier` answers
/// a read that starts where the last one stopped; `resume` answers one that
/// re-enters the last block. Without the second, every unaligned continuation
/// would fall back to a checkpoint and a whole-volume sweep would be quadratic.
#[derive(Debug, Clone, Copy)]
struct CipherChain {
    /// Cipher offset immediately past the last block produced.
    frontier: u64,
    /// The 16 cipher bytes ending at `frontier`.
    frontier_block: [u8; 16],
    /// Start of the last block produced.
    resume: u64,
    /// The 16 cipher bytes ending at `resume` — that block's CBC predecessor.
    resume_block: [u8; 16],
}

impl CipherChain {
    /// The predecessor of the block starting at `block_start`, if this frontier
    /// happens to hold it.
    fn seed_for(&self, block_start: u64) -> Option<[u8; 16]> {
        if self.frontier == block_start {
            return Some(self.frontier_block);
        }
        (self.resume == block_start).then_some(self.resume_block)
    }
}

impl std::fmt::Debug for VirtualVolumeReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VirtualVolumeReader")
            .field("volume_index", &self.volume.volume_index)
            .field("position", &self.position)
            .field("len", &self.volume.len)
            .finish()
    }
}

impl VirtualVolumeReader {
    pub(crate) fn new(volume: VirtualVolume, counters: Arc<CipherOverlayCounters>) -> Self {
        Self {
            volume,
            position: 0,
            envelope_handle: None,
            partial_handles: HashMap::new(),
            chains: HashMap::new(),
            counters,
        }
    }

    /// Same: the reconstruction sweep carries its own length, so this exists for
    /// the tests that hold the reader to a real file's `SeekFrom::End` semantics.
    #[cfg(test)]
    pub(crate) fn len(&self) -> u64 {
        self.volume.len
    }

    /// The member extent containing `position`, if any.
    ///
    /// Binary search rather than a scan: a header walk over a many-member volume
    /// issues thousands of small reads, and a linear probe per read would make
    /// that quadratic in the member count.
    fn extent_at(&self, position: u64) -> Option<&MemberExtent> {
        let candidate = self
            .volume
            .extents
            .partition_point(|extent| extent.physical_offset <= position)
            .checked_sub(1)?;
        let extent = &self.volume.extents[candidate];
        (position < extent.physical_offset.saturating_add(extent.len)).then_some(extent)
    }

    /// How far the current run of the *same* source reaches: the nearest of the
    /// volume's covered run end, the run end of the source that answers this
    /// byte, the source's own boundary (the member extent's end, or the next
    /// extent's start when the envelope owns these bytes), and the volume's end.
    ///
    /// `None` means `position` is a hole — either nothing was placed there, or
    /// something was and no source on disk backs it.
    fn run_end(&self, position: u64) -> Option<u64> {
        let covered_end = covered_run_end(&self.volume.covered, position)?;

        let (source_end, boundary) = match self.extent_at(position) {
            // A routed member's partial holds every byte of its own extent that
            // the volume map calls covered; a partial too short for that is
            // caught as a hole by the short read.
            Some(extent) => (
                covered_end,
                extent.physical_offset.saturating_add(extent.len),
            ),
            // The envelope owns these bytes as far as the next member extent —
            // but only as far as an envelope write actually reached.
            None => (
                covered_run_end(&self.volume.envelope_covered, position)?,
                self.volume
                    .extents
                    .get(
                        self.volume
                            .extents
                            .partition_point(|extent| extent.physical_offset <= position),
                    )
                    .map(|extent| extent.physical_offset)
                    .unwrap_or(u64::MAX),
            ),
        };
        Some(
            covered_end
                .min(source_end)
                .min(boundary)
                .min(self.volume.len),
        )
    }

    fn source_at(&self, position: u64) -> Source {
        match self.extent_at(position) {
            Some(extent) => Source::Member {
                member_id: extent.member_id,
                offset: extent
                    .logical_offset
                    .saturating_add(position - extent.physical_offset),
            },
            None => Source::Envelope { offset: position },
        }
    }

    /// A destination file that is not there holds no bytes, which is what a hole
    /// *is*. Reporting it as a plain `NotFound` would make a caller treat a
    /// deleted partial as an infrastructure failure rather than as "refetch
    /// this", which is the whole distinction [`is_hole`] exists to draw.
    fn open_or_hole(&self, path: &std::path::Path) -> std::io::Result<File> {
        File::open(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                hole(self.volume.volume_index, self.position)
            } else {
                error
            }
        })
    }

    fn read_from(&mut self, source: Source, out: &mut [u8]) -> std::io::Result<usize> {
        match source {
            Source::Member { member_id, offset } => {
                // Plan 136, E-D4. An encrypted member's partial holds plaintext
                // and this reader answers in *posted* space, so the bytes are
                // re-encrypted on the way out. Every other member reads through
                // unchanged, which is what keeps the overlay off for a set that
                // has no encrypted member at all.
                if self.volume.ciphers.contains_key(&member_id) {
                    return self.read_member_cipher(member_id, offset, out);
                }
                self.read_member_plain(member_id, offset, out)
            }
            Source::Envelope { offset } => {
                if self.envelope_handle.is_none() {
                    let path = self.volume.envelope.clone();
                    self.envelope_handle = Some(self.open_or_hole(&path)?);
                }
                let file = self
                    .envelope_handle
                    .as_mut()
                    .expect("the handle was just opened");
                file.seek(SeekFrom::Start(offset))?;
                file.read(out)
            }
        }
    }

    /// One member's bytes straight out of its partial, which for an unencrypted
    /// member is what was posted.
    fn read_member_plain(
        &mut self,
        member_id: u32,
        offset: u64,
        out: &mut [u8],
    ) -> std::io::Result<usize> {
        if !self.partial_handles.contains_key(&member_id) {
            let path = self
                .volume
                .partials
                .get(&member_id)
                .ok_or_else(|| hole(self.volume.volume_index, self.position))?
                .clone();
            let file = self.open_or_hole(&path)?;
            self.partial_handles.insert(member_id, file);
        }
        let file = self
            .partial_handles
            .get_mut(&member_id)
            .expect("the handle was just inserted");
        file.seek(SeekFrom::Start(offset))?;
        file.read(out)
    }

    // ---- The re-encrypting overlay (plan 136, E-D4) ------------------------

    /// One encrypted member's **posted** bytes for `[offset, offset + out.len())`.
    ///
    /// Whole blocks are re-encrypted and the requested window sliced out of
    /// them, because CBC has no smaller unit. The blocks needed are
    /// `[floor(offset), ceil(offset + len))`, clamped at the member's cipher
    /// size — the final one of which runs past `unpacked_size` into the retained
    /// tail padding, which is exactly why that padding is retained.
    ///
    /// A read whose window *is* those blocks — which is what an aligned slice
    /// sweep asks for — reads its plaintext straight into the caller's buffer
    /// and encrypts it there, so the bytes are touched once instead of copied
    /// out of a scratch `Vec` afterwards (E2 review).
    fn read_member_cipher(
        &mut self,
        member_id: u32,
        offset: u64,
        out: &mut [u8],
    ) -> std::io::Result<usize> {
        let ciphers = Arc::clone(&self.volume.ciphers);
        let Some(facts) = ciphers.get(&member_id) else {
            return Err(self.refuse());
        };
        let want = out.len() as u64;
        if want == 0 {
            return Ok(0);
        }
        let end = offset.saturating_add(want);
        // A member extent stops at `unpacked_size`: the tail padding's cipher
        // was routed to the envelope at its true physical offset, so it is the
        // envelope's to answer and never reaches here.
        if end > facts.unpacked_size() {
            return Err(self.refuse());
        }
        let block_start = block_floor(offset);
        let block_end = block_ceil(end).min(facts.cipher_size());

        let preceding = self.chain_to(member_id, facts, block_start)?;
        if block_start == offset && block_end == end {
            self.member_plaintext_into(member_id, facts, block_start, block_end, out)?;
            if facts.encrypt(&preceding, out).is_err() {
                return Err(self.refuse());
            }
            self.remember_chain(member_id, block_end, out, preceding);
        } else {
            let mut buffer = self.member_plaintext(member_id, facts, block_start, block_end)?;
            if facts.encrypt(&preceding, &mut buffer).is_err() {
                return Err(self.refuse());
            }
            self.remember_chain(member_id, block_end, &buffer, preceding);
            let at = (offset - block_start) as usize;
            out.copy_from_slice(&buffer[at..at + want as usize]);
        }
        self.counters
            .reencrypted_bytes
            .fetch_add(want, Ordering::Relaxed);
        Ok(out.len())
    }

    /// The 16 cipher bytes immediately before `block_start`, chaining forward
    /// from the nearest seed when no checkpoint sits exactly there.
    ///
    /// The chain is the *sequential path*, taken deliberately rather than
    /// guessing a predecessor: a wrong one corrupts exactly the first block and
    /// leaves the rest correct, which no checksum downstream could attribute.
    fn chain_to(
        &mut self,
        member_id: u32,
        facts: &MemberCipher,
        block_start: u64,
    ) -> std::io::Result<[u8; 16]> {
        let chain = self.chains.get(&member_id).copied();
        // The frontier this reader already reached beats every checkpoint, and
        // for a sequential sweep it *is* the answer.
        if let Some(preceding) = chain.and_then(|chain| chain.seed_for(block_start)) {
            self.counters
                .seeded_from_checkpoint
                .fetch_add(1, Ordering::Relaxed);
            return Ok(preceding);
        }
        let seed = facts.seed(block_start);
        let (mut cursor, mut preceding) = match chain {
            Some(chain) if chain.frontier <= block_start && chain.frontier > seed.chain_start => {
                (chain.frontier, chain.frontier_block)
            }
            _ => (seed.chain_start, seed.preceding),
        };
        match cursor {
            0 => self
                .counters
                .seeded_from_start
                .fetch_add(1, Ordering::Relaxed),
            _ => self
                .counters
                .seeded_from_checkpoint
                .fetch_add(1, Ordering::Relaxed),
        };

        while cursor < block_start {
            let step = (block_start - cursor).min(CHAIN_CHUNK_BYTES as u64);
            let mut buffer = self.member_plaintext(member_id, facts, cursor, cursor + step)?;
            if facts.encrypt(&preceding, &mut buffer).is_err() {
                return Err(self.refuse());
            }
            preceding.copy_from_slice(&buffer[buffer.len() - 16..]);
            cursor += step;
            self.counters
                .chained_bytes
                .fetch_add(step, Ordering::Relaxed);
        }
        Ok(preceding)
    }

    /// The plaintext behind `[from, to)` of a member's cipher stream: its
    /// partial below `unpacked_size`, the retained tail padding above it.
    ///
    /// Refuses — as a hole, because "refetch this" is what it means — whenever a
    /// byte of that range is not really there. The coverage test is the load
    /// bearing one: a partial is a sparse file, so a gap reads back as zeros,
    /// and CBC would turn those zeros into perfectly well-formed cipher for
    /// every block from there to the member's end.
    fn member_plaintext(
        &mut self,
        member_id: u32,
        facts: &MemberCipher,
        from: u64,
        to: u64,
    ) -> std::io::Result<Vec<u8>> {
        let mut plain = vec![0u8; (to - from) as usize];
        self.member_plaintext_into(member_id, facts, from, to, &mut plain)?;
        Ok(plain)
    }

    /// [`Self::member_plaintext`] into a buffer the caller already owns, which
    /// is `to - from` bytes long. The allocating form is this one plus a `vec!`.
    fn member_plaintext_into(
        &mut self,
        member_id: u32,
        facts: &MemberCipher,
        from: u64,
        to: u64,
        plain: &mut [u8],
    ) -> std::io::Result<()> {
        debug_assert_eq!(plain.len() as u64, to - from);
        if !facts.plaintext_present(from, to) {
            return Err(self.refuse());
        }
        let on_disk = to.min(facts.unpacked_size());
        if on_disk > from {
            let mut read = 0usize;
            let want = (on_disk - from) as usize;
            while read < want {
                match self.read_member_plain(
                    member_id,
                    from + read as u64,
                    &mut plain[read..want],
                )? {
                    0 => return Err(self.refuse()),
                    progress => read += progress,
                }
            }
        }
        if to > facts.unpacked_size() {
            // The final block. Its plaintext past the member's end is the
            // retained padding, and a member that never captured it whole cannot
            // serve this block at all — nor the destination bytes inside it.
            let Some(tail) = facts.tail_plain() else {
                return Err(self.refuse());
            };
            let at = (facts.unpacked_size().max(from) - from) as usize;
            let take = (to - facts.unpacked_size()) as usize;
            if take > tail.len() {
                return Err(self.refuse());
            }
            plain[at..at + take].copy_from_slice(&tail[..take]);
        }
        Ok(())
    }

    /// Files both seeds a following read could want: the frontier, and the
    /// predecessor of the last block produced.
    fn remember_chain(
        &mut self,
        member_id: u32,
        frontier: u64,
        cipher: &[u8],
        preceding: [u8; 16],
    ) {
        let Ok(frontier_block) = <[u8; 16]>::try_from(&cipher[cipher.len() - 16..]) else {
            return;
        };
        let resume_block = match cipher.len() >= 32 {
            true => <[u8; 16]>::try_from(&cipher[cipher.len() - 32..cipher.len() - 16])
                .unwrap_or(preceding),
            // One block: its predecessor is what this call was handed.
            false => preceding,
        };
        self.chains.insert(
            member_id,
            CipherChain {
                frontier,
                frontier_block,
                resume: frontier.saturating_sub(16),
                resume_block,
            },
        );
    }

    /// A refusal by the overlay, counted and reported as a hole.
    fn refuse(&self) -> std::io::Error {
        self.counters.refusals.fetch_add(1, Ordering::Relaxed);
        hole(self.volume.volume_index, self.position)
    }
}

impl Read for VirtualVolumeReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() || self.position >= self.volume.len {
            return Ok(0);
        }
        let Some(run_end) = self.run_end(self.position) else {
            return Err(hole(self.volume.volume_index, self.position));
        };
        if run_end <= self.position {
            return Err(hole(self.volume.volume_index, self.position));
        }
        let want = (run_end - self.position).min(out.len() as u64) as usize;
        let source = self.source_at(self.position);
        let read = self.read_from(source, &mut out[..want])?;
        if read == 0 {
            // The backing file is shorter than the coverage map claims: the
            // partial or the envelope was truncated or deleted under us. That is
            // a hole in every sense that matters to a caller.
            return Err(hole(self.volume.volume_index, self.position));
        }
        self.position = self.position.saturating_add(read as u64);
        Ok(read)
    }
}

impl Seek for VirtualVolumeReader {
    fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
        // Unlike the router's in-memory image, a virtual volume has a real
        // length, so an End-relative seek means exactly what it does on a file.
        self.position = match from {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => self.position.saturating_add_signed(offset),
            SeekFrom::End(offset) => self.volume.len.saturating_add_signed(offset),
        };
        Ok(self.position)
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        Ok(self.position)
    }
}
