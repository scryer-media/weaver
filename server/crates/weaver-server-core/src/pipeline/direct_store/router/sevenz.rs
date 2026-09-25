//! The 7z half of the layout seam: reading one container's map out of its end
//! header, and answering physical ranges from it.
//!
//! # Why this is a layout and not a second router
//!
//! Everything the router does once a member's coordinates are known — span
//! routing, split parts across volumes, holds, the coverage barrier, the
//! virtual-volume provider, reconstruction on a demotion, finalization by
//! rename — reads the layout through two accessors and never asks which format
//! produced it. A Copy-coded 7z member is, in those coordinates, exactly what a
//! stored RAR member is: one contiguous run of container bytes whose values are
//! the output file's values. So the 7z side builds [`unrar_rs::StoredMember`]s
//! and hands them to the same machinery rather than growing a parallel copy of
//! it.
//!
//! # What a 7z container looks like from here
//!
//! A `.7z` is a 32-byte signature header, then the packed streams, then an end
//! header at the tail that names every entry and every block. A `-v` split is a
//! pure byte split of that one container at a fixed volume size — no per-volume
//! headers, no per-volume signature — so the volumes concatenate and every
//! offset in this module is an offset into that concatenation.
//!
//! Two consequences shape the whole module:
//!
//! - **The map arrives at the end, and arrives whole.** There is no incremental
//!   front-to-back walk to run per volume: either the end header has been read
//!   and the container's every member is known, or nothing is. That is what
//!   makes the layout here a one-shot build with `chain_complete` already true,
//!   where the RAR builder grows volume by volume.
//! - **The geometry is two facts, and everything else is arithmetic.** The
//!   part size is volume zero's own length; the total is where the start
//!   header's coordinates put the end of the end header. Every volume boundary
//!   follows from those two, so no other volume has to be heard from before a
//!   container offset becomes a (volume, offset) pair. What the volumes go on
//!   to state about themselves is checked against that, never waited on: a
//!   disagreement demotes the set, it does not fail the job.

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};

use serde::{Deserialize, Serialize};
use unrar_rs::{MappedSlice, MemberEligibility, StoredMember, StoredMemberPart};

use super::{SparseImage, StagedChunk};

/// The signature header: magic, version, start-header CRC32, and the three
/// fields naming the end header.
pub(super) const SIGNATURE_HEADER_LEN: u64 = 32;

const SEVEN_Z_MAGIC: [u8; 6] = [b'7', b'z', 0xBC, 0xAF, 0x27, 0x1C];

/// The `Copy` coder's method id: the identity transform, which is the one
/// coder whose packed bytes *are* the member's bytes.
const COPY_METHOD_ID: &[u8] = &[0x00];

/// Why a 7z container could not be direct-routed.
///
/// Every variant is a property of the container itself rather than of how much
/// of it has arrived, so each one is a verdict the moment the end header parses
/// and none of them is worth waiting on another article for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SevenZipRefusal {
    /// A block is not a single `Copy` coder over a single pack stream whose
    /// packed length equals its unpacked length. Anything else — LZMA, LZMA2,
    /// BCJ, a filter chain — means the packed bytes are not the output bytes.
    Coder,
    /// A block's coder chain includes AES.
    ///
    /// An encrypted `Copy` member is routable in principle: the cipher is CBC
    /// over the whole stream, so cipher offset and member offset coincide
    /// exactly as they do for an encrypted stored RAR member. It is reported
    /// rather than routed because the decrypting half of that — the key ring,
    /// the block-boundary holds, the re-encrypting overlay the provider needs
    /// so PAR2 still sees the posted bytes — is the encrypted-member path, and
    /// pointing the 7z layout at it is a separate piece of work.
    EncryptedContent,
    /// The end header is itself encrypted, so nothing names the members.
    EncryptedHeader,
    /// An anti-item: a deletion marker carried by an incremental archive, which
    /// names a file the archive does not contain.
    AntiItem,
    /// An entry the header marks as a symlink or other redirection. Its
    /// "content" is a link target, not a file body.
    Redirection,
    /// Every entry is a directory or an empty file, so the container stores
    /// no byte of any file.
    ///
    /// There is nothing to route and nothing to verify: the whole container is
    /// headers, and a set is only ever finalized on members it verified. The
    /// conventional extractor creates the entries from a container this small
    /// at no cost worth routing around.
    NothingToRoute,
    /// An entry name the reader's own path check refuses: absolute, escaping,
    /// or otherwise not a name that may become a destination.
    ///
    /// Distinct from [`super::DemotionReason::UnsafeDestination`], which says the same
    /// thing about a RAR member and answers [`super::VolumeDemand::Virtual`] for it:
    /// that set has a layout, so the conventional extractor can read its
    /// volumes off the overlay and refuse the path itself. A container refused
    /// while its map is being read has no layout and no overlay, so its
    /// volumes have to be real.
    UnsafeDestination,
    /// Volume zero states no length this router can place a map against: none
    /// at all, or zero.
    ///
    /// A split container is a byte split at a fixed part size, and volume
    /// zero's length is the only statement of what that size is. Without it
    /// every container offset is unplaceable, so the set is not routable
    /// direct and says so before it holds anything beyond its probe articles.
    ///
    /// Not a judgement on the posting. A yEnc `size=` is advisory — weaver does
    /// not validate it, and a volume with none is still perfectly downloadable
    /// — so this refuses *this route* and hands the set to the conventional
    /// path, which needs no such hint.
    VolumeHintUnusable,
    /// The map is still unread and nothing is left that could change that: no
    /// article of the set is queued, retrying, downloading, decoding, reserved
    /// or pending decode, and no released lane result is outstanding.
    ///
    /// Covers a missing volume zero, a missing volume anywhere in the middle
    /// and a missing tail alike, because all three come to the same thing —
    /// a parse that is not settled and no article left that could settle it.
    /// Nothing else ends that wait: the budget ceilings only fire on a set big
    /// enough to reach them, and a small set would sit unresolved forever.
    ///
    /// The RAR analogue is [`super::DemotionReason::UnparsableVolume`], which
    /// says the same thing one volume at a time — a header walk shown its
    /// ceiling and still holding no member is never going to hold one. This is
    /// that verdict for a format whose layout is one fact about the whole
    /// concatenation, and it is reached by the set running out of articles
    /// rather than by it spending a ceiling.
    UnreadableMap,
    /// A volume whose length is not the one the container's own coordinates
    /// require of it.
    ///
    /// The container's geometry is two facts: the part size volume zero states,
    /// and the total the start header's own coordinates give. Together they say
    /// exactly how long every volume must be. This is raised when something
    /// disagrees with that — a volume's declared length when it arrives, a
    /// volume's **decoded** length when it completes, a part count that does
    /// not match the set's, or a total the hint cannot divide into a sane
    /// number of parts.
    ///
    /// The decoded check is the authoritative one and it is deliberately not
    /// conditional on having routed nothing yet: a set that routed against a
    /// wrong part size wrote its members' bytes at offsets no reader will look
    /// for them at, and the demotion is what stops those bytes from shipping.
    /// The declared check is only an earlier chance at the same verdict — a
    /// yEnc `size=` is advisory, so its agreement proves nothing and its
    /// disagreement is merely the first evidence to arrive.
    VolumeSize,
    /// The header's coordinates do not describe one archive: members that
    /// overlap, run backwards, reach past the container, leave a block's packed
    /// bytes unclaimed, or repeat a name.
    Geometry,
}

impl SevenZipRefusal {
    pub(crate) fn metric(self) -> &'static str {
        match self {
            Self::Coder => "7z_coder",
            Self::EncryptedContent => "7z_encrypted_content",
            Self::EncryptedHeader => "7z_encrypted_header",
            Self::AntiItem => "7z_anti_item",
            Self::Redirection => "7z_redirection",
            Self::NothingToRoute => "7z_nothing_to_route",
            Self::UnsafeDestination => "7z_unsafe_destination",
            Self::VolumeHintUnusable => "7z_volume_hint_unusable",
            Self::UnreadableMap => "7z_unreadable_map",
            Self::VolumeSize => "7z_volume_size",
            Self::Geometry => "7z_geometry",
        }
    }
}

/// The three fields of the signature header that place the end header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StartHeader {
    pub(super) next_header_offset: u64,
    pub(super) next_header_size: u64,
}

impl StartHeader {
    /// Reads the signature header out of the container's first 32 bytes.
    ///
    /// `None` for a prefix that is not a 7z signature at all, which the caller
    /// reports as an unsupported format rather than as a shortage of bytes: the
    /// magic is in the first six bytes of the first volume, so a set that has
    /// those and does not match is not a container this layout can read.
    pub(super) fn parse(prefix: &[u8; SIGNATURE_HEADER_LEN as usize]) -> Option<Self> {
        if prefix[..6] != SEVEN_Z_MAGIC {
            return None;
        }
        // Byte 6 is the major version. The format's own rule, and the reader's:
        // a major version this build does not know describes a layout whose
        // fields may not be where they are read from below.
        if prefix[6] != 0 {
            return None;
        }
        let word = |at: usize| {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&prefix[at..at + 8]);
            u64::from_le_bytes(bytes)
        };
        Some(Self {
            next_header_offset: word(12),
            next_header_size: word(20),
        })
    }

    /// Where the end header begins, as a container offset.
    pub(super) fn end_header_start(&self) -> Option<u64> {
        SIGNATURE_HEADER_LEN.checked_add(self.next_header_offset)
    }

    /// Where the end header ends. For a well-formed container this is the
    /// container's own length — the end header is the last thing in the file.
    pub(super) fn end_header_end(&self) -> Option<u64> {
        self.end_header_start()?.checked_add(self.next_header_size)
    }
}

/// The most parts a container may be split into before its own start header is
/// believed.
///
/// A start header is read before a single byte of it has been checked against
/// anything, so `total / part_size` is arithmetic over two numbers a bad
/// posting is free to have made up. Without a ceiling a header declaring a
/// total near `u64::MAX` asks this router to plan for more volumes than the
/// job has articles. Well clear of any real posting: the largest split sets
/// seen in the wild are a few thousand parts.
pub(super) const MAX_CONTAINER_PARTS: u64 = 100_000;

/// What a split container's geometry is, derived from the two facts that state
/// it.
///
/// A split 7z is a pure byte split at a fixed size, so the whole concatenation
/// is described by the part size and the total — and every volume's length
/// follows. That is what lets a set place its map without having heard from
/// every volume: it needs volume zero and the tail, not the whole set.
///
/// `part_size` is a **hint**. It comes from volume zero's yEnc `size=`, which
/// weaver does not validate anywhere else and which a posting is free to state
/// wrongly or not at all. Everything here is therefore provisional until the
/// volumes decode: see [`SevenZipRefusal::VolumeSize`] for what happens when
/// the bytes disagree with the plan derived from the hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ContainerGeometry {
    /// Volume zero's declared length, which every volume but the last has.
    pub(super) part_size: u64,
    /// The container's length, from the start header's own coordinates: the
    /// end header is the last thing in a 7z file, so where it ends is where the
    /// file ends.
    pub(super) total: u64,
    /// How many volumes that is.
    pub(super) parts: u64,
}

impl ContainerGeometry {
    /// Derives the geometry, or says why these two numbers do not describe one
    /// container.
    pub(super) fn derive(part_size: u64, start: &StartHeader) -> Result<Self, SevenZipRefusal> {
        let total = start.end_header_end().ok_or(SevenZipRefusal::VolumeSize)?;
        Self::derive_from_total(part_size, total)
    }

    /// The same derivation from a total already in hand — the restore path,
    /// where the start header's own bytes are long gone.
    pub(super) fn derive_from_total(part_size: u64, total: u64) -> Result<Self, SevenZipRefusal> {
        if part_size == 0 {
            return Err(SevenZipRefusal::VolumeHintUnusable);
        }
        let total = total.max(1);
        let parts = total.div_ceil(part_size);
        if parts > MAX_CONTAINER_PARTS {
            return Err(SevenZipRefusal::VolumeSize);
        }
        Ok(Self {
            part_size,
            total,
            parts,
        })
    }

    /// How long volume `volume_index` must be, or `None` for a volume this
    /// geometry does not have.
    ///
    /// Every part is `part_size` except the last, which is whatever is left —
    /// which is how a split writer produces the short tail real sets have.
    pub(super) fn expected_len(&self, volume_index: u32) -> Option<u64> {
        let index = u64::from(volume_index);
        if index >= self.parts {
            return None;
        }
        if index + 1 == self.parts {
            return Some(self.total - (self.parts - 1) * self.part_size);
        }
        Some(self.part_size)
    }

    /// Every volume's length, dense from volume zero, in the shape
    /// [`SevenZipLayout::build`] resolves coordinates against.
    pub(super) fn lengths(&self) -> BTreeMap<u32, u64> {
        (0..self.parts as u32)
            .filter_map(|volume| Some((volume, self.expected_len(volume)?)))
            .collect()
    }
}

/// One entry the end header names, in container coordinates.
///
/// This is the durable form: it is what the restart cache stores and what the
/// layout is rebuilt from, so it carries coordinates and never a reader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SevenZipEntryFacts {
    /// Name exactly as the header states it. Not sanitized — destination policy
    /// belongs to the plan, and the raw name is the archive's own key.
    pub(crate) name: String,
    /// Container offset of the entry's bytes, or `None` for a dataless entry:
    /// a directory, or a file the archive records with no stream at all (which
    /// is how 7z stores an empty file).
    pub(crate) start: Option<u64>,
    /// Unpacked length. Zero for every dataless entry.
    pub(crate) size: u64,
    /// The header's CRC32 of the entry's bytes. **Optional by design**: 7z
    /// records checksums per sub-stream and an archive may simply carry none.
    pub(crate) crc32: Option<u32>,
    /// The entry is a directory rather than a file.
    pub(crate) is_directory: bool,
    /// Last-modified time as the header states it (100 ns ticks since 1601), or
    /// `None` when the header states none.
    pub(crate) modified: Option<u64>,
    /// Last-access time on the same scale, likewise only when stated. 7z keeps
    /// each time behind its own presence bit, and an entry written by a tool
    /// that recorded none has none to restore.
    #[serde(default)]
    pub(crate) accessed: Option<u64>,
}

/// Everything one 7z container's end header states, plus the volume lengths the
/// coordinates were resolved against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SevenZipContainerFacts {
    pub(crate) entries: Vec<SevenZipEntryFacts>,
    /// The container's length, as the start header's coordinates gave it.
    ///
    /// Cached because the start header is not: its 32 bytes sit below the
    /// published floors and are never refetched, so a restored set has no way
    /// to re-read them. With this and volume zero's cached length the geometry
    /// is derivable again, which is what turns the map's container offsets back
    /// into the (volume, offset) pairs everything downstream is expressed in.
    ///
    /// Zero means a row written before the geometry was cached; such a set
    /// restores no layout and parses again over its refetched tail.
    #[serde(default)]
    pub(crate) total: u64,
}

/// One dataless entry finalization creates rather than routes.
///
/// A directory, or an empty file. Both are headers with no bytes anywhere in
/// the container, so there is nothing for the router to place and nothing for
/// an extractor to decode — and an archive whose only difference from another
/// is an empty `.nfo` is still an archive whose output must contain it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SevenZipDatalessEntry {
    pub(crate) name: String,
    pub(crate) is_directory: bool,
    pub(crate) modified: Option<u64>,
    pub(crate) accessed: Option<u64>,
}

/// The container map, resolved against the geometry's volume lengths.
///
/// Built once, complete, from the end header. There is no growing it: a 7z
/// container states its whole map in one place, so either this exists and is
/// authoritative for every byte of every volume, or the set has no layout yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SevenZipLayout {
    /// Container offset each volume begins at, dense from volume zero.
    bases: Vec<u64>,
    /// Each volume's length, in the same order.
    lengths: Vec<u64>,
    /// Data-bearing entries, in ascending container order, with their parts
    /// already cut at the volume boundaries.
    members: Vec<StoredMember>,
    /// Entries with no bytes at all, in header order.
    dataless: Vec<SevenZipDatalessEntry>,
}

impl SevenZipLayout {
    /// Resolves the container's entries against the volume lengths the
    /// geometry puts there.
    ///
    /// `lengths` must be dense from volume zero, and is: they are derived from
    /// the part size and the total rather than collected from the volumes, so
    /// every boundary is known as soon as those two facts are, whether or not
    /// the volume that sits at it has been heard from.
    pub(super) fn build(
        lengths: &BTreeMap<u32, u64>,
        facts: &SevenZipContainerFacts,
    ) -> Result<Self, SevenZipRefusal> {
        if lengths.is_empty()
            || lengths
                .keys()
                .enumerate()
                .any(|(position, volume)| position as u32 != *volume)
        {
            return Err(SevenZipRefusal::VolumeSize);
        }
        let mut bases = Vec::with_capacity(lengths.len());
        let mut sizes = Vec::with_capacity(lengths.len());
        let mut total = 0u64;
        for length in lengths.values() {
            bases.push(total);
            sizes.push(*length);
            total = total
                .checked_add(*length)
                .ok_or(SevenZipRefusal::VolumeSize)?;
        }

        let mut members = Vec::new();
        let mut dataless = Vec::new();
        // The container order the entries have to be disjoint and ascending in.
        // Sorted here rather than trusted from the header: block order is the
        // header's, and two blocks whose packed ranges cross would otherwise
        // only show up as two members claiming one byte.
        let mut placed: Vec<&SevenZipEntryFacts> = Vec::new();
        for entry in &facts.entries {
            match entry.start {
                Some(_) => placed.push(entry),
                None => dataless.push(SevenZipDatalessEntry {
                    name: entry.name.clone(),
                    is_directory: entry.is_directory,
                    modified: entry.modified,
                    accessed: entry.accessed,
                }),
            }
        }
        placed.sort_by_key(|entry| (entry.start.unwrap_or(0), entry.size));
        let mut frontier = 0u64;
        for entry in placed {
            let start = entry.start.unwrap_or(0);
            let end = start
                .checked_add(entry.size)
                .ok_or(SevenZipRefusal::Geometry)?;
            if start < frontier || end > total {
                return Err(SevenZipRefusal::Geometry);
            }
            frontier = end;
            members.push(member_over(&bases, &sizes, entry, start)?);
        }
        Ok(Self {
            bases,
            lengths: sizes,
            members,
            dataless,
        })
    }

    pub(super) fn members(&self) -> &[StoredMember] {
        &self.members
    }

    pub(super) fn dataless(&self) -> &[SevenZipDatalessEntry] {
        &self.dataless
    }

    /// The container offset one volume begins at.
    pub(super) fn volume_base(&self, volume: u32) -> Option<u64> {
        self.bases.get(volume as usize).copied()
    }

    /// Where one volume's physical bytes belong.
    ///
    /// The same contract [`unrar_rs::StoredLayoutBuilder::map_physical_range`]
    /// states: the returned slices tile `[offset, offset + len)` in order.
    /// Anything that is not inside a member's range is envelope — the signature
    /// header, the end header, and any padding a writer left between blocks.
    pub(super) fn map_physical_range(
        &self,
        volume: u32,
        offset: u64,
        len: u64,
    ) -> Vec<MappedSlice> {
        if len == 0 {
            return Vec::new();
        }
        let Some(base) = self.volume_base(volume) else {
            return vec![MappedSlice::Unroutable { len }];
        };
        let start = base.saturating_add(offset);
        let end = start.saturating_add(len);
        let mut slices = Vec::new();
        let mut cursor = start;
        // Members are ascending and disjoint by construction (`build` refuses
        // anything else), so one forward pass answers the whole range.
        for (member_index, member) in self.members.iter().enumerate() {
            if cursor >= end {
                break;
            }
            let Some(member_start) = self.member_start(member) else {
                continue;
            };
            let member_end = member_start.saturating_add(member.unpacked_size.unwrap_or(0));
            if member_end <= cursor {
                continue;
            }
            if member_start >= end {
                break;
            }
            if member_start > cursor {
                push_envelope(&mut slices, member_start - cursor);
                cursor = member_start;
            }
            let take = member_end.min(end) - cursor;
            slices.push(MappedSlice::Member {
                member_index,
                logical_offset: cursor - member_start,
                len: take,
            });
            cursor += take;
        }
        if cursor < end {
            push_envelope(&mut slices, end - cursor);
        }
        slices
    }

    /// The container offset a member's first part begins at.
    fn member_start(&self, member: &StoredMember) -> Option<u64> {
        let part = member.parts.first()?;
        Some(
            self.bases
                .get(part.volume as usize)?
                .saturating_add(part.data_offset),
        )
    }
}

fn push_envelope(slices: &mut Vec<MappedSlice>, len: u64) {
    if len == 0 {
        return;
    }
    match slices.last_mut() {
        Some(MappedSlice::Envelope { len: held }) => *held = held.saturating_add(len),
        _ => slices.push(MappedSlice::Envelope { len }),
    }
}

/// Cuts one entry's container range at the volume boundaries and expresses it
/// the way the router already understands a split stored member.
///
/// The volume seam is the only thing that splits a 7z member, and a member
/// crossing it becomes exactly the shape a stored RAR member spanning two
/// volumes has: an earlier part with `split_after`, a later one with
/// `split_before`, and logical offsets that are the prefix sums of the parts
/// before them.
fn member_over(
    bases: &[u64],
    lengths: &[u64],
    entry: &SevenZipEntryFacts,
    start: u64,
) -> Result<StoredMember, SevenZipRefusal> {
    let end = start
        .checked_add(entry.size)
        .ok_or(SevenZipRefusal::Geometry)?;
    let mut parts: Vec<StoredMemberPart> = Vec::new();
    let mut logical = 0u64;
    for (volume, (base, length)) in bases.iter().zip(lengths).enumerate() {
        let volume_end = base.saturating_add(*length);
        let from = start.max(*base);
        let to = end.min(volume_end);
        if from >= to {
            continue;
        }
        parts.push(StoredMemberPart {
            volume: volume as u32,
            data_offset: from - base,
            data_size: to - from,
            logical_offset: Some(logical),
            // 7z records no per-volume checksum: the split is a byte split of a
            // finished container, so nothing in the format describes a
            // fragment. Layer 1 has no analogue here and the router's part gate
            // composes without an expectation to compare against.
            packed_crc32: None,
            packed_blake2_hash: None,
            packed_hash_uses_mac: false,
            split_before: from > start,
            split_after: to < end,
        });
        logical += to - from;
    }
    // A zero-length entry is dataless and never reaches here; anything else
    // that produced no part is an entry outside every volume.
    if parts.is_empty() {
        return Err(SevenZipRefusal::Geometry);
    }
    let first_volume = parts[0].volume;
    Ok(StoredMember {
        name: entry.name.clone(),
        first_volume,
        unpacked_size: Some(entry.size),
        data_crc32: entry.crc32,
        data_blake2_hash: None,
        data_hash_uses_mac: false,
        parts,
        // The whole map came from one end header, so every chain is closed the
        // moment it exists. There is no later volume that can extend it.
        chain_complete: true,
        // The coder gate ran over the whole container before this was built, so
        // a member that reaches here is Copy-coded, unencrypted and whole. A
        // missing CRC32 is **not** a disqualification the way it is for RAR:
        // 7z simply may not carry one, and the router gates such a member on
        // coverage plus the job's PAR2 verdict instead.
        eligibility: MemberEligibility::DirectEligible,
    })
}

/// Reads one container's entries out of a parsed archive, refusing anything
/// whose packed bytes are not its output bytes.
///
/// This is the whole eligibility decision, and it is taken over the **set**
/// rather than per member. A 7z block is the unit of compression and several
/// entries can share one; there is no per-member tolerance to fall back on the
/// way there is for RAR, because tolerating one entry would mean decoding it
/// out of a container the rest of which has already been routed away. So a
/// container with anything but Copy blocks is refused whole, which hands it to
/// the conventional extractor with its bytes still on disk.
pub(super) fn container_facts(
    archive: &sevenz_turbo::Archive,
) -> Result<SevenZipContainerFacts, SevenZipRefusal> {
    let pack_base = SIGNATURE_HEADER_LEN.saturating_add(archive.pack_pos());
    let first_pack = archive.stream_map.block_first_pack_stream_index();
    let offsets = archive.stream_map.pack_stream_offsets();
    let pack_sizes = archive.pack_sizes();

    // Per block: the container offset its single pack stream begins at.
    let mut block_start = Vec::with_capacity(archive.blocks.len());
    for (index, block) in archive.blocks.iter().enumerate() {
        if block.coders.len() != 1 {
            // More than one coder is a filter chain — BCJ over LZMA, delta over
            // LZMA2 — and the outer coder's output is not the file's bytes.
            return Err(coder_refusal(block));
        }
        let coder = &block.coders[0];
        if coder.encoder_method_id() != COPY_METHOD_ID {
            return Err(coder_refusal(block));
        }
        let stream = *first_pack.get(index).ok_or(SevenZipRefusal::Geometry)?;
        // Exactly one pack stream. A Copy block with several would still be
        // contiguous, but nothing in the format says the streams are adjacent,
        // and a member placed across a gap that is not there is silent
        // corruption rather than a failed extraction.
        let next = first_pack
            .get(index + 1)
            .copied()
            .unwrap_or(pack_sizes.len());
        if next != stream + 1 {
            return Err(SevenZipRefusal::Coder);
        }
        let size = *pack_sizes.get(stream).ok_or(SevenZipRefusal::Geometry)?;
        // Copy is the identity, so a block whose packed and unpacked lengths
        // differ is not the block its coder claims to be.
        if size != block.get_unpack_size() {
            return Err(SevenZipRefusal::Coder);
        }
        let offset = *offsets.get(stream).ok_or(SevenZipRefusal::Geometry)?;
        block_start.push(
            pack_base
                .checked_add(offset)
                .ok_or(SevenZipRefusal::Geometry)?,
        );
    }

    // Each block's running cursor: the entries sharing a block are laid out end
    // to end inside its single pack stream, in file order.
    let mut cursor: Vec<u64> = block_start.clone();
    let mut entries = Vec::with_capacity(archive.files.len());
    let mut seen: std::collections::HashSet<&str> =
        std::collections::HashSet::with_capacity(archive.files.len());
    for (index, file) in archive.files.iter().enumerate() {
        if file.is_anti_item {
            return Err(SevenZipRefusal::AntiItem);
        }
        if file.is_symlink() {
            return Err(SevenZipRefusal::Redirection);
        }
        // The reader's own name check, applied before a name reaches a
        // destination policy that would have to guess what the writer meant.
        if file.unsafe_path_reason().is_some() {
            return Err(SevenZipRefusal::UnsafeDestination);
        }
        if !seen.insert(file.name.as_str()) {
            // Two entries claiming one name resolve by write order in an
            // extractor and by nothing at all here.
            return Err(SevenZipRefusal::Geometry);
        }
        let modified = file
            .has_last_modified_date
            .then(|| file.last_modified_date.into());
        let accessed = file.has_access_date.then(|| file.access_date.into());
        // A zero-length entry that nonetheless claims a stream is dataless
        // too: it consumes none of its block, and a member with no bytes has
        // no part in any volume to route. Left in the placed set it would be
        // the one entry `member_over` cannot place, and a refusal there is
        // the whole set demoted for a file that only needs creating. The
        // format's own writer never emits the shape — it marks empty files
        // stream-less — so this is the invariant stated, not a case observed.
        if !file.has_stream || file.is_directory || file.size == 0 {
            entries.push(SevenZipEntryFacts {
                name: file.name.clone(),
                start: None,
                size: 0,
                crc32: None,
                is_directory: file.is_directory,
                modified,
                accessed,
            });
            continue;
        }
        let block = archive
            .stream_map
            .file_block_index
            .get(index)
            .copied()
            .flatten()
            .ok_or(SevenZipRefusal::Geometry)?;
        let start = *cursor.get(block).ok_or(SevenZipRefusal::Geometry)?;
        let end = start
            .checked_add(file.size)
            .ok_or(SevenZipRefusal::Geometry)?;
        let block_end = block_start[block]
            .checked_add(archive.blocks[block].get_unpack_size())
            .ok_or(SevenZipRefusal::Geometry)?;
        if end > block_end {
            return Err(SevenZipRefusal::Geometry);
        }
        cursor[block] = end;
        entries.push(SevenZipEntryFacts {
            name: file.name.clone(),
            start: Some(start),
            size: file.size,
            crc32: file.has_crc.then_some(file.crc as u32),
            is_directory: false,
            modified,
            accessed,
        });
    }

    // Every block's packed bytes must be claimed by its entries. Bytes inside a
    // block that no entry accounts for are bytes the layout would route to the
    // envelope while the archive says they belong to a file, and a reader that
    // disagrees with the layout about which file's bytes those are is the one
    // failure mode this whole subsystem cannot tolerate.
    for (block, end) in cursor.iter().enumerate() {
        let declared = block_start[block].saturating_add(archive.blocks[block].get_unpack_size());
        if *end != declared {
            return Err(SevenZipRefusal::Geometry);
        }
    }

    if entries.iter().all(|entry| entry.start.is_none()) {
        return Err(SevenZipRefusal::NothingToRoute);
    }

    Ok(SevenZipContainerFacts { entries, total: 0 })
}

/// Which refusal a non-`Copy` block earns.
///
/// AES is separated from the rest because it is the one refusal that says
/// "routable, not yet implemented" rather than "not routable", and a metric
/// that cannot tell those apart cannot say how much of the field would benefit
/// from building the decrypting half.
fn coder_refusal(block: &sevenz_turbo::Block) -> SevenZipRefusal {
    if block
        .coders
        .iter()
        .any(|coder| coder.encoder_method_id() == sevenz_turbo::EncoderMethod::ID_AES256_SHA256)
    {
        return SevenZipRefusal::EncryptedContent;
    }
    SevenZipRefusal::Coder
}

/// A reader over the whole container, assembled from what the volumes have
/// staged.
///
/// [`SparseImage`] refuses an `End`-relative seek on purpose — a volume image
/// has no end, because the last staged run stops wherever the last article
/// happened to reach. A *container* does have one: its length is the sum of the
/// volumes' declared lengths, which is a fact off the wire rather than an
/// artefact of arrival order, and the 7z reader asks for it before it does
/// anything else. So the end lives here, beside the number that justifies it,
/// and the image underneath keeps its refusal.
pub(super) struct ContainerImage {
    image: SparseImage,
    total: u64,
    holed: bool,
}

impl ContainerImage {
    pub(super) fn new(
        volumes: &[(u64, &BTreeMap<u64, StagedChunk>)],
        scratch: Option<std::sync::Arc<std::fs::File>>,
        total: u64,
    ) -> Self {
        Self {
            image: SparseImage::over_volumes(volumes, scratch),
            total,
            holed: false,
        }
    }

    /// Whether a read stopped short of the container's end on a byte the
    /// volumes have not delivered.
    ///
    /// A sparse image answers a hole the way a file answers its end, so a
    /// reader cannot tell the two apart and reports whichever parse error the
    /// truncation produced. This says which one it was, and so whether the
    /// error is a verdict on the container or only on how much of it is here.
    pub(super) fn holed(&self) -> bool {
        self.holed
    }

    /// Puts the image back at its first byte with nothing recorded, so the read
    /// that follows is judged only on itself. What one pass over a keyed
    /// container learned about holes says nothing about the next pass, which
    /// stops in a different place or not at all.
    pub(super) fn restart(&mut self) -> std::io::Result<()> {
        self.image.seek(SeekFrom::Start(0))?;
        self.holed = false;
        Ok(())
    }

    /// Reads exactly `len` bytes at `offset`, or `None` when any of them is a
    /// hole the volumes have not delivered.
    pub(super) fn read_exact_at(&mut self, offset: u64, len: usize) -> Option<Vec<u8>> {
        let mut out = vec![0u8; len];
        self.image.seek(SeekFrom::Start(offset)).ok()?;
        self.image.read_exact(&mut out).ok()?;
        Some(out)
    }
}

impl Read for ContainerImage {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let taken = self.image.read(out)?;
        if taken == 0 && !out.is_empty() && self.image.stream_position()? < self.total {
            self.holed = true;
        }
        Ok(taken)
    }
}

impl Seek for ContainerImage {
    fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
        match from {
            SeekFrom::End(offset) => self
                .image
                .seek(SeekFrom::Start(self.total.saturating_add_signed(offset))),
            other => self.image.seek(other),
        }
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        self.image.stream_position()
    }
}

/// What one attempt at reading a container's map produced.
pub(super) enum ParseOutcome {
    /// The end header parsed and every entry passed the coder gate.
    Facts(Box<SevenZipContainerFacts>),
    /// Not enough of the container has arrived yet. The next article retries.
    Incomplete,
    /// A verdict about the container. No further byte changes it.
    Refused(SevenZipRefusal),
    /// The first bytes are not a 7z signature at all.
    NotSevenZip,
}

/// Reads the container's map, given an image over the volumes and their total
/// length.
///
/// `image_complete` says whether every byte of every volume has arrived. It is
/// the difference between "the reader hit a hole" — wait, the article is coming
/// — and "the reader hit a hole in a container that is entirely present", which
/// is a container whose own coordinates point outside itself.
///
/// `passwords` are the job's archive-password candidates, in the harvest's own
/// priority order, so that a header-encrypted container opens instead of being
/// refused unread. The list is the same one the `-hp` gate proves a RAR set's
/// headers against, and for the same reason: a set whose key is the NZB-meta
/// password while the spec carries an operator's guess would otherwise refuse
/// for a password that was sitting right there.
///
/// Tried in order, after one attempt with no key at all — the overwhelming
/// majority of containers have no encrypted header, and for those the first
/// attempt is the only one. A container with no encrypted header ignores a key
/// entirely, so the no-key attempt is not a special case so much as the first
/// candidate.
///
/// The list is bounded at four by construction — `Explicit`, `NzbMeta`,
/// `FilenameConvention`, at most one each, plus the no-key attempt — so the
/// work this loop can do is bounded however many times the reader is asked.
pub(super) fn parse_container(
    mut image: ContainerImage,
    total: u64,
    max_end_header_bytes: u64,
    image_complete: bool,
    passwords: &[&str],
) -> ParseOutcome {
    let Some(prefix) = image.read_exact_at(0, SIGNATURE_HEADER_LEN as usize) else {
        return ParseOutcome::Incomplete;
    };
    let mut fixed = [0u8; SIGNATURE_HEADER_LEN as usize];
    fixed.copy_from_slice(&prefix);
    let Some(start) = StartHeader::parse(&fixed) else {
        return ParseOutcome::NotSevenZip;
    };
    if start.next_header_size > max_end_header_bytes {
        // The same bound the reader would apply, applied before the allocation
        // it would apply it in front of — and, here, before the set spends the
        // rest of its holds budget waiting for bytes it would then refuse.
        return ParseOutcome::Refused(SevenZipRefusal::Geometry);
    }
    // The end header is the last thing in a 7z file. A container whose start
    // header says otherwise is either not the file the volumes concatenate into
    // or has bytes appended past its own end, and in both cases the offsets
    // every member is placed at are offsets into something else.
    if start.end_header_end() != Some(total) {
        return ParseOutcome::Refused(SevenZipRefusal::VolumeSize);
    }
    let Some(header_start) = start.end_header_start() else {
        return ParseOutcome::Refused(SevenZipRefusal::Geometry);
    };
    if image
        .read_exact_at(header_start, start.next_header_size as usize)
        .is_none()
    {
        return ParseOutcome::Incomplete;
    }

    let limits = sevenz_turbo::ArchiveLimits {
        max_end_header_bytes,
        ..Default::default()
    };
    let mut opened = None;
    for attempt in std::iter::once(None).chain(passwords.iter().copied().map(Some)) {
        let key = match attempt {
            Some(secret) if !secret.is_empty() => sevenz_turbo::Password::new(secret),
            _ => sevenz_turbo::Password::empty(),
        };
        // Each attempt reads the image from the top, and reads its own answer
        // about whether it stopped at a hole. Carrying either across would have
        // a later candidate judged on an earlier one's reading.
        if image.restart().is_err() {
            return ParseOutcome::Refused(SevenZipRefusal::Geometry);
        }
        match sevenz_turbo::Archive::read_with_limits(&mut image, &key, &limits) {
            Ok(archive) => {
                opened = Some(archive);
                break;
            }
            // `-mhe`: the end header is itself an encrypted block, so nothing in
            // it names a member without a key. This candidate is refuted; the
            // next one is tried, and running out of them is the refusal below.
            Err(sevenz_turbo::Error::PasswordRequired)
            | Err(sevenz_turbo::Error::MaybeBadPassword(_)) => continue,
            // A compressed end header keeps its packed bytes just before itself,
            // so a reader that got this far can still be short of them. That is
            // the one parse failure another article can change, and it is a
            // failure only because the image stopped at a hole. Every other one
            // is a verdict now: waiting on it spends the whole holds budget to
            // arrive at the same answer.
            //
            // Judged on the spot rather than after the loop, because it is a
            // statement about the *bytes* and not about the key: a truncated
            // image refutes nothing, and letting the remaining candidates run
            // against it would report a wrong password for a container nobody
            // has finished reading.
            Err(_) if !image_complete && image.holed() => return ParseOutcome::Incomplete,
            Err(_) => return ParseOutcome::Refused(SevenZipRefusal::Geometry),
        }
    }
    // Every candidate the job has was refuted, which for `-mhe` is the whole
    // verdict: it is a property of the archive that no further byte changes,
    // and it is the one refusal an operator can act on. Stated once, for the
    // list as a whole, exactly as the `-hp` gate states it for a RAR set.
    let Some(archive) = opened else {
        return ParseOutcome::Refused(SevenZipRefusal::EncryptedHeader);
    };
    match container_facts(&archive) {
        // Stamped here rather than inside `container_facts`, which reads the
        // end header and never sees the start header's coordinates.
        Ok(mut facts) => {
            facts.total = total;
            ParseOutcome::Facts(Box::new(facts))
        }
        Err(refusal) => ParseOutcome::Refused(refusal),
    }
}
