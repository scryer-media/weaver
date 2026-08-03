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

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use weaver_unrar::{ReadSeek, VolumeProvider, VolumeProviderError};

use super::ByteRanges;
use super::router::MemberExtent;

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
    pub(crate) partials: HashMap<u32, PathBuf>,
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
}

/// A [`VolumeProvider`] over a direct set's partials and envelopes.
#[derive(Debug, Clone, Default)]
pub(crate) struct HybridVolumeProvider {
    volumes: HashMap<u32, VirtualVolume>,
}

impl HybridVolumeProvider {
    pub(crate) fn new(volumes: Vec<VirtualVolume>) -> Self {
        Self {
            volumes: volumes
                .into_iter()
                .map(|volume| (volume.volume_index, volume))
                .collect(),
        }
    }

    /// The registered shape of one virtual volume. No production caller yet —
    /// wave 2's PAR2 adapter needs it to size a settle read — so it is test-only
    /// rather than carrying a dead-code allow that would outlive its reason.
    #[cfg(test)]
    pub(crate) fn volume(&self, volume_index: u32) -> Option<&VirtualVolume> {
        self.volumes.get(&volume_index)
    }

    /// Opens one virtual volume directly, without the trait's `usize` index and
    /// boxing. Reconstruction and this module's tests use it.
    pub(crate) fn open(&self, volume_index: u32) -> Option<VirtualVolumeReader> {
        self.volumes
            .get(&volume_index)
            .cloned()
            .map(VirtualVolumeReader::new)
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
    pub(crate) fn new(volume: VirtualVolume) -> Self {
        Self {
            volume,
            position: 0,
            envelope_handle: None,
            partial_handles: HashMap::new(),
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
