//! The range router — plan 135 phase 4, D2/D3/D4.
//!
//! One [`DirectSetRouter`] owns one archive set. It learns the set's stored
//! layout from [`StoredLayoutBuilder`] as each volume's headers become
//! readable, splits every decoded source span across the destinations that
//! intersect it, and runs the two RAR-level integrity gates (per-part packed
//! CRC32, whole-member CRC32) over the bytes it routed.
//!
//! Everything here is pure: the router decides *what* to write and *where*, and
//! returns that as [`RoutedSpan`]s. The caller performs the writes, and only
//! after every write for an article returned does it tell the router (and the
//! coverage barrier) that the span was placed. Partial failure leaves orphan
//! bytes; the coverage map is the truth, not the bytes.
//!
//! # Volume headers without a volume file
//!
//! Weaver's existing fact parsing opens the finished volume file. A direct set
//! has no file, so the router keeps a **sparse image** of every byte whose
//! destination is not a routed member — the header prefix while the volume is
//! still unmapped, then the envelope — and parses through that. The image is a
//! reader that returns real bytes inside a known run and EOF everywhere else,
//! which is exactly what `parse_volume_facts` needs: the header walk *seeks*
//! over data areas and never reads them, and it stops cleanly at EOF rather
//! than failing.
//!
//! That gives two parse points per volume, both from the same image:
//!
//! - **Provisional**, as soon as offset 0 is contiguously staged: normally the
//!   volume's first article. It yields the members whose headers precede the
//!   payload, and routing starts.
//! - **Confirming**, once the volume's trailing bytes have arrived: the walk now
//!   reaches the end-of-archive record. If it finds a member the provisional
//!   parse did not, that member is **adopted** — the layout is rebuilt from every
//!   volume's newest facts, because [`StoredLayoutBuilder::add_volume`] refuses a
//!   differing re-add and has no removal API. Phase 4 demoted here instead, which
//!   was safe (the file was not lost, it was refetched) but cost the whole set.
//!   A parse whose members are *not* an extension of the previous one is a real
//!   disagreement and still demotes.
//!
//! A volume stops re-parsing only on **proof** that no further header can
//! appear: either `more_volumes` (which the library sets from a parsed
//! end-of-archive record, in both RAR4 and RAR5) or the whole source volume
//! having arrived. Chain closure read off a *truncated* prefix is not proof —
//! the header that closes the chain is the first member's, and a second
//! member's header can sit past the first member's data area, beyond anything
//! the prefix reached.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom};

use weaver_unrar::{
    ArchiveFormat, IneligibilityReason, MappedSlice, MemberEligibility, RarVolumeFacts,
    StoredLayoutBuilder, StoredLayoutError,
};

use super::ByteRanges;
use super::plan::DirectSetPlan;

/// Default RAM ceiling for holds across one set. Phase 4 has no scratch paging
/// (phase 5 owns it), so a breach demotes.
pub(crate) const DEFAULT_HOLDS_BUDGET_BYTES: u64 = 64 * 1024 * 1024;

/// D1's absolute packed ceiling for the bounded small-member tolerance. The
/// effective ceiling is `min(this, 1% of the archive's packed bytes)`; the
/// relative half can only be applied once the archive's packed total is final,
/// so this one is the guard that holds from the first tolerated byte.
pub(crate) const TOLERANCE_PACKED_CEILING_BYTES: u64 = 64 * 1024 * 1024;

/// D1's unpacked ceiling. Read from the headers' declared unpacked size, which
/// is stated up front rather than accumulated, so this one is checkable the
/// moment a member turns ineligible.
pub(crate) const TOLERANCE_UNPACKED_CEILING_BYTES: u64 = 256 * 1024 * 1024;

/// The "1% of packed archive bytes" half of D1's packed ceiling, as a divisor.
const TOLERANCE_ARCHIVE_PERCENT: u64 = 100;

/// How far into a volume the provisional header parse may stage bytes before
/// the set is declared unroutable. Real RAR headers are a few hundred bytes;
/// this only exists so a set whose first article is entirely payload (a
/// corrupted or non-RAR file that reached us classified as a volume) demotes
/// instead of holding forever.
pub(crate) const MAX_HEADER_PREFIX_BYTES: u64 = 4 * 1024 * 1024;

/// Why a set left direct mode. Every variant is its own metric bucket (D1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DemotionReason {
    /// A member the layout classified `Ineligible` for a reason D1's tolerance
    /// does not cover — including a provisional member that resolved ineligible
    /// when its chain closed (revision 6).
    ///
    /// Solid, encrypted, directory, redirection and malformed-chain members
    /// always land here: the tolerance extracts its members with
    /// `extract_member_streaming`, which needs a per-member non-solid regular
    /// file it can decode on its own, and a solid member is only decodable
    /// against the rest of the solid run. A *compressed* or *BLAKE2sp-only*
    /// member instead rides [`Self::ToleranceBudgetExceeded`]'s budget.
    MemberIneligible(MemberIneligibility),
    /// The ineligible members are individually tolerable but collectively over
    /// D1's budget: `min(64 MiB, 1% of the archive's packed bytes)` packed, or
    /// 256 MiB unpacked.
    ///
    /// Distinct from [`Self::MemberIneligible`] on purpose — "this set has one
    /// small compressed extra" and "this set is half compressed" are different
    /// populations, and only the second one says direct-store's reach is
    /// narrower than the plan assumes.
    ToleranceBudgetExceeded,
    /// PAR2 verification found damage on one of the set's virtual volumes.
    ///
    /// Wave 2 produces verification **verdicts** only: repairing a virtual
    /// volume means writing a repaired slice into a file that does not exist,
    /// which is phase 6. Demoting materializes the volumes from the set's own
    /// routed bytes and hands them to the conventional repair path, which is
    /// exactly the shape a job with no direct set would have taken.
    Par2Damaged,
    /// A member riding D1's tolerance could not be extracted from the virtual
    /// volumes at finalization. The stored members are correct; the tolerated
    /// one is not produced, so the set is rebuilt the ordinary way.
    ToleratedExtractionFailed,
    /// Holds exceeded the RAM budget and phase 4 has no scratch paging.
    HoldsBudgetExceeded,
    /// A confirming parse disagreed with the provisional one, or a volume was
    /// re-added with facts that are not an extension of what it stated before.
    ConflictingVolumeFacts,
    /// The volume's headers could not be parsed from the staged image.
    UnparsableVolume,
    /// A non-final part's packed CRC32 did not match the bytes routed for it.
    PartChecksumMismatch,
    /// The composed whole-member CRC32 did not match the final part's header.
    MemberChecksumMismatch,
    /// Two volumes of the set disagree about the archive format.
    FormatMismatch,
    /// The set's signature names a format phase 4 does not route (RAR 1.4).
    UnsupportedFormat,
    /// A destination path the RAR path validator refuses.
    UnsafeDestination,
    /// Two members of the set sanitize to the same destination path.
    ///
    /// `ensure_unique_sanitized_rar_member_paths` refuses such an archive
    /// outright, so the conventional extractor would fail it too — demoting is
    /// what makes direct routing produce today's behaviour exactly, rather than
    /// silently overwriting one member with the other.
    CollidingDestinations,
    /// The volume's composed yEnc whole-file CRC32 disagreed with the trailer
    /// the articles declared. The transport layer's own gate, which a physical
    /// volume would have failed at file-complete time.
    VolumeCrcMismatch,
    /// A write to one of the set's destinations failed. The conventional path
    /// writes the same bytes to a different file, so this is a demotion rather
    /// than a job failure.
    DestinationWriteFailed,
    /// Committing a verified set to its destinations failed. The bytes are
    /// good; the filesystem refused the rename, so the set is rebuilt the
    /// ordinary way rather than left half-committed.
    FinalizationFailed,
}

/// The ineligibility reasons phase 4 distinguishes in metrics. The library's
/// [`IneligibilityReason`] carries byte counts that only phase 5's tolerance
/// budget needs, so this collapses it to the label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemberIneligibility {
    Compressed,
    Encrypted,
    Solid,
    Directory,
    Redirection,
    Blake2OnlyNoCrc32,
    NoChecksum,
    MalformedChain,
}

impl From<IneligibilityReason> for MemberIneligibility {
    fn from(reason: IneligibilityReason) -> Self {
        match reason {
            IneligibilityReason::Compressed { .. } => Self::Compressed,
            IneligibilityReason::Encrypted => Self::Encrypted,
            IneligibilityReason::Solid => Self::Solid,
            IneligibilityReason::Directory => Self::Directory,
            IneligibilityReason::Redirection => Self::Redirection,
            IneligibilityReason::Blake2OnlyNoCrc32 => Self::Blake2OnlyNoCrc32,
            IneligibilityReason::NoChecksum => Self::NoChecksum,
            IneligibilityReason::MalformedChain(_) => Self::MalformedChain,
        }
    }
}

impl DemotionReason {
    /// Stable metric label. `sets == direct + materialized + mixed` is worth
    /// asserting against these (D1).
    pub(crate) fn metric(self) -> &'static str {
        match self {
            Self::MemberIneligible(MemberIneligibility::Compressed) => "member_compressed",
            Self::MemberIneligible(MemberIneligibility::Encrypted) => "member_encrypted",
            Self::MemberIneligible(MemberIneligibility::Solid) => "member_solid",
            Self::MemberIneligible(MemberIneligibility::Directory) => "member_directory",
            Self::MemberIneligible(MemberIneligibility::Redirection) => "member_redirection",
            Self::MemberIneligible(MemberIneligibility::Blake2OnlyNoCrc32) => "member_blake2_only",
            Self::MemberIneligible(MemberIneligibility::NoChecksum) => "member_no_checksum",
            Self::MemberIneligible(MemberIneligibility::MalformedChain) => "member_malformed_chain",
            Self::ToleranceBudgetExceeded => "tolerance_budget",
            Self::Par2Damaged => "par2_damaged",
            Self::ToleratedExtractionFailed => "tolerated_extraction_failed",
            Self::HoldsBudgetExceeded => "holds_budget",
            Self::ConflictingVolumeFacts => "conflicting_volume_facts",
            Self::UnparsableVolume => "unparsable_volume",
            Self::PartChecksumMismatch => "part_checksum_mismatch",
            Self::MemberChecksumMismatch => "member_checksum_mismatch",
            Self::FormatMismatch => "format_mismatch",
            Self::UnsupportedFormat => "unsupported_format",
            Self::UnsafeDestination => "unsafe_destination",
            Self::CollidingDestinations => "colliding_destinations",
            Self::VolumeCrcMismatch => "volume_crc_mismatch",
            Self::DestinationWriteFailed => "destination_write_failed",
            Self::FinalizationFailed => "finalization_failed",
        }
    }
}

impl std::fmt::Display for DemotionReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.metric())
    }
}

/// Where one routed run of bytes goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectDestination {
    /// A direct-routed member's `.direct.partial`, at a logical offset.
    ///
    /// `member_id` is **weaver's** stable per-set identity, not the layout's
    /// index. The layout numbers members in first-seen order, and a confirming
    /// parse that finds a member hiding in an *earlier* volume renumbers
    /// everything after it; bytes already written must not change destination
    /// because of that. The id is assigned once per member name and never
    /// reused, and the durable identity in the checkpoint blob is the
    /// destination's relative path, which is derived from the same name.
    Member { member_id: u32 },
    /// The volume's own envelope file, at the byte's **true physical offset**
    /// inside that volume (envelope v2). Holes are wherever member data was
    /// routed away.
    Envelope { volume_index: u32 },
}

/// One run of bytes the caller must write before the article counts as placed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutedSpan {
    pub(crate) destination: DirectDestination,
    /// Offset within the destination file.
    pub(crate) destination_offset: u64,
    /// Volume the bytes came from, and their offset inside it. The coverage
    /// barrier keys source floors by this pair.
    pub(crate) volume_index: u32,
    pub(crate) source_offset: u64,
    pub(crate) bytes: Vec<u8>,
}

impl RoutedSpan {
    pub(crate) fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
}

/// (offset, len, crc32) runs over one contiguous logical space, composed on
/// demand with [`weaver_par2::checksum::Crc32CombineOp`].
///
/// The runs are kept exactly as they were fed — **never merged** (M3). Phase 5
/// wave 1's first shape coalesced adjacent neighbours into one value, which
/// answered "is this whole space composed" in one comparison and answered
/// nothing else: a covered range that stopped short of a merged run's end — a
/// held tail, a volume whose last article never came, a prefix under a
/// reconstruction floor — had no reference value at all and was written
/// unverified. Keeping the atoms means any sub-range that starts and ends on an
/// atom boundary can be composed, which is every range the coverage map can
/// name for an article that was wholly routed.
///
/// The cost is a `Vec` entry per article rather than per gap. That is bounded
/// by the articles of one volume (or one member part), which is the same order
/// as the coverage map itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CrcRuns {
    runs: Vec<(u64, u64, u32)>,
}

impl CrcRuns {
    /// Inserts one run. Overlapping an existing run is ignored — a duplicate
    /// article must not advance the composition twice (D3).
    pub(crate) fn insert(&mut self, start: u64, len: u64, crc: u32) {
        if len == 0 {
            return;
        }
        let end = start.saturating_add(len);
        let position = self
            .runs
            .partition_point(|(run_start, _, _)| *run_start < start);
        if position < self.runs.len() {
            let (next_start, _, _) = self.runs[position];
            if next_start < end {
                return;
            }
        }
        if let Some(index) = position.checked_sub(1) {
            let (previous_start, previous_len, _) = self.runs[index];
            if previous_start.saturating_add(previous_len) > start {
                return;
            }
        }
        self.runs.insert(position, (start, len, crc));
    }

    /// The composed value for `[start, start + len)`, when the runs fed in tile
    /// it exactly: the first starts at `start`, each one abuts the next, and the
    /// last ends at `start + len`.
    ///
    /// `None` means "no reference value", which every caller treats as *refuse*
    /// rather than *pass*: a range the composition can only bound is not a
    /// checksum, and D8's verification is what stands between a rebuilt volume
    /// and a published floor over bytes nothing checked.
    pub(crate) fn compose(&self, start: u64, len: u64) -> Option<u32> {
        if len == 0 {
            return None;
        }
        let end = start.checked_add(len)?;
        let mut index = self
            .runs
            .partition_point(|(run_start, _, _)| *run_start < start);
        let mut cursor = start;
        // CRC32 of no bytes is zero, and combining it with the first run is the
        // identity — the same seed `try_verify_member` composes parts from.
        let mut composed = 0u32;
        while cursor < end {
            let (run_start, run_len, run_crc) = *self.runs.get(index)?;
            if run_start != cursor {
                return None;
            }
            let run_end = run_start.checked_add(run_len)?;
            if run_end > end {
                return None;
            }
            composed =
                weaver_par2::checksum::Crc32CombineOp::new(run_len).combine(composed, run_crc);
            cursor = run_end;
            index += 1;
        }
        Some(composed)
    }
}

/// A sparse byte image of one volume, read through by the header parser.
///
/// Reads inside a staged run return real bytes; reads anywhere else return
/// `Ok(0)`, which `read_exact` turns into `UnexpectedEof` and the RAR header
/// walk turns into a clean stop. Seeks always succeed — the walk seeks over
/// data areas it never reads.
/// Constructing one is **O(chunks) pointer copies, not O(bytes)** (M2). The
/// first shape cloned every staged chunk on every article, so a `-rr` volume
/// paid a whole-image `memcpy` per article until it was confirmed. The parser
/// is handed a reader by value and the library's entry point requires
/// `'static`, so a plain borrow is not available; sharing the chunks is the
/// same zero-copy with an ownership story that outlives the borrow.
pub(super) struct SparseImage {
    runs: Vec<(u64, std::sync::Arc<[u8]>)>,
    position: u64,
}

impl SparseImage {
    pub(super) fn from_chunks(chunks: &BTreeMap<u64, std::sync::Arc<[u8]>>) -> Self {
        Self {
            runs: chunks
                .iter()
                .map(|(offset, bytes)| (*offset, std::sync::Arc::clone(bytes)))
                .collect(),
            position: 0,
        }
    }
}

impl Read for SparseImage {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let position = self.position;
        let index = self.runs.partition_point(|(start, _)| *start <= position);
        let Some(index) = index.checked_sub(1) else {
            return Ok(0);
        };
        let (start, bytes) = &self.runs[index];
        let inside = position - start;
        if inside >= bytes.len() as u64 {
            return Ok(0);
        }
        let available = &bytes[inside as usize..];
        let taken = available.len().min(out.len());
        out[..taken].copy_from_slice(&available[..taken]);
        self.position = position.saturating_add(taken as u64);
        Ok(taken)
    }
}

impl Seek for SparseImage {
    fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
        self.position = match from {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => self.position.saturating_add_signed(offset),
            // The image has no end: the last staged run ends wherever the last
            // article happened to reach, which is not the volume's length, so
            // an End-relative seek would silently mean something else every
            // time an article lands. The header walk never asks for one — a
            // refusal here is a loud "this reader is not a file", not a
            // behaviour change.
            SeekFrom::End(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "a direct-store sparse volume image has no end to seek from",
                ));
            }
        };
        Ok(self.position)
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        Ok(self.position)
    }
}

/// Per-volume staging: the bytes the router still needs, and what it has
/// already placed.
#[derive(Debug, Default)]
struct VolumeStaging {
    /// Non-overlapping byte runs, keyed by physical offset. Shared rather than
    /// owned so the header parser's image is a pointer copy (M2).
    chunks: BTreeMap<u64, std::sync::Arc<[u8]>>,
    /// Physical ranges already routed to a destination.
    routed: ByteRanges,
    /// Physical ranges staged but not yet routed — the holds.
    pending: ByteRanges,
    /// Facts have been added to the layout from a provisional parse.
    provisional: bool,
    /// A confirming parse ran with proof that no further header can appear.
    confirmed: bool,
    /// Every article of the source volume has arrived, so the image the parser
    /// walks is byte-contiguously complete: no header can still show up in a
    /// region the walk has already passed.
    source_complete: bool,
    /// Physical end of the last member extent the walk has reached.
    ///
    /// **The frontier of proven classification.** Below it the walk arrived
    /// sequentially — header, seek over data, header — so every byte is either a
    /// header it read or a data area it accounted for, and calling a non-member
    /// byte "envelope" there is a fact. At or above it nothing is proven: an
    /// undiscovered member's header and data both live in exactly that region.
    /// Writing those bytes into the envelope before the volume is confirmed
    /// would file a member's payload as scratch and delete it at finalization,
    /// which is the loss the confirming parse exists to prevent — so they are
    /// held until confirmation instead.
    tail_base: u64,
}

impl VolumeStaging {
    /// Stores the parts of `[offset, offset + len)` that are neither routed nor
    /// already pending, and marks them pending. Returns the newly staged bytes.
    fn stage(&mut self, offset: u64, data: &[u8]) -> u64 {
        let mut staged = 0u64;
        for (start, end) in self.routed.missing(offset, data.len() as u64) {
            for (start, end) in self.pending.missing(start, end - start) {
                let from = (start - offset) as usize;
                let to = (end - offset) as usize;
                self.chunks
                    .insert(start, std::sync::Arc::from(&data[from..to]));
                self.pending.insert(start, end - start);
                staged = staged.saturating_add(end - start);
            }
        }
        staged
    }

    /// Every byte this volume is still holding in RAM, routed or not.
    ///
    /// Deliberately the chunk map rather than [`Self::pending`] (M1). Envelope
    /// bytes are *routed* — they leave `pending` the moment they are emitted —
    /// but [`DirectSetRouter::trim_volume`] retains them until the volume is
    /// confirmed, because the header walk has to seek through them to reach the
    /// end-of-archive record. With envelope v2 that retained region is a `-rr`
    /// set's recovery record, which is a percentage of the volume, per volume:
    /// counting only `pending` left the largest term in the set's RSS outside
    /// the budget that exists to bound it.
    fn staged_bytes(&self) -> u64 {
        self.chunks.values().fold(0u64, |total, bytes| {
            total.saturating_add(bytes.len() as u64)
        })
    }

    /// Copies `[offset, offset + len)` out of the staged chunks. `None` when
    /// the range is not wholly staged, which the drain never asks for.
    fn slice(&self, offset: u64, len: u64) -> Option<Vec<u8>> {
        let mut out = Vec::with_capacity(len as usize);
        let mut cursor = offset;
        let end = offset.saturating_add(len);
        while cursor < end {
            let index = self
                .chunks
                .range(..=cursor)
                .next_back()
                .map(|(start, bytes)| (*start, bytes.as_ref()))?;
            let (start, bytes) = index;
            let inside = cursor - start;
            if inside >= bytes.len() as u64 {
                return None;
            }
            let take = ((bytes.len() as u64 - inside).min(end - cursor)) as usize;
            out.extend_from_slice(&bytes[inside as usize..inside as usize + take]);
            cursor = cursor.saturating_add(take as u64);
        }
        Some(out)
    }

    /// Drops staged bytes that are routed and belong to a member, keeping the
    /// envelope (the header parser reads through it) and the holds.
    fn trim(&mut self, keep: &ByteRanges) {
        let offsets: Vec<u64> = self.chunks.keys().copied().collect();
        for offset in offsets {
            let Some(bytes) = self.chunks.get(&offset) else {
                continue;
            };
            let len = bytes.len() as u64;
            let retained = intersect(keep, offset, len);
            if retained.len() == 1 && retained[0] == (offset, offset + len) {
                continue;
            }
            let chunk = self.chunks.remove(&offset).expect("chunk was just read");
            for (start, end) in retained {
                let from = (start - offset) as usize;
                let to = (end - offset) as usize;
                self.chunks
                    .insert(start, std::sync::Arc::from(&chunk[from..to]));
            }
        }
    }
}

/// The parts of `[offset, offset + len)` that `ranges` **does** cover — the
/// complement of [`ByteRanges::missing`] inside the same window.
fn intersect(ranges: &ByteRanges, offset: u64, len: u64) -> Vec<(u64, u64)> {
    let end = offset.saturating_add(len);
    let mut out = Vec::new();
    let mut cursor = offset;
    for (gap_start, gap_end) in ranges.missing(offset, len) {
        if gap_start > cursor {
            out.push((cursor, gap_start));
        }
        cursor = gap_end;
    }
    if cursor < end {
        out.push((cursor, end));
    }
    out
}

/// Per-member routing state, keyed by weaver's stable member id.
#[derive(Debug)]
struct MemberRouting {
    /// Raw header name, the layout's key and the source of the stable id.
    name: String,
    /// Working-directory-relative `.direct.partial`.
    relative_partial: String,
    unpacked_size: u64,
    /// Logical coverage, so a duplicate never advances a gate twice.
    covered: ByteRanges,
    /// Per-part CRC composition, indexed by the part's position in the chain.
    /// Kept per part rather than one member-wide map because runs merge across
    /// part boundaries the moment both sides complete, which would erase the
    /// per-part value before it could be checked.
    parts: BTreeMap<u32, CrcRuns>,
    /// Parts whose packed CRC32 has already been checked.
    checked_parts: BTreeMap<u32, u32>,
    /// The whole-member gate has passed.
    verified: bool,
}

/// One archive set's router.
pub(crate) struct DirectSetRouter {
    plan: DirectSetPlan,
    /// `None` until a volume's signature names the set's archive format.
    ///
    /// The format is **read, not assumed**: a RAR4 set opened against a RAR5
    /// layout fails `add_volume`'s format check on its very first header and
    /// pays the whole demotion cost for nothing. The layout is empty until the
    /// first parse succeeds, so binding it there costs a branch and rebinds
    /// nothing.
    layout: Option<StoredLayoutBuilder>,
    /// The newest accepted header facts per volume.
    ///
    /// [`StoredLayoutBuilder::add_volume`] refuses a re-add whose facts differ,
    /// and it has no removal API — so when a longer prefix reveals a header the
    /// provisional parse could not reach, the only way to adopt it is to build a
    /// fresh layout from every volume's newest facts. Keeping them here is what
    /// makes that rebuild possible, and comparing against them is what tells an
    /// *extension* (fine, rebuild) from a genuine *disagreement* (demote).
    volume_facts: BTreeMap<u32, RarVolumeFacts>,
    staging: BTreeMap<u32, VolumeStaging>,
    /// Routing state by stable member id.
    members: BTreeMap<u32, MemberRouting>,
    /// Member name to stable id. Assigned once, never reused, never renumbered.
    member_ids: HashMap<String, u32>,
    next_member_id: u32,
    /// Every member extent the router has ever **routed bytes for**, per source
    /// volume, coalesced and in physical order (B1).
    ///
    /// The layout's current classification cannot answer this. Eligibility is a
    /// running verdict: a `ProvisionallyDirect` member whose chain closes
    /// blake2-only flips to `Ineligible`, `map_physical_range` stops calling its
    /// packed range a member, and every byte already written into its partial
    /// becomes, to anything reading the layout, an envelope byte. The provider
    /// would then answer those offsets out of the envelope file — where they are
    /// a sparse hole inside its length, which is to say **zeros** — and
    /// reconstruction would write fabricated bytes into a volume under a
    /// published floor.
    ///
    /// History is keyed by the stable member id, so a rebuild that renumbers the
    /// layout moves nothing here, and it is the same record the `.direct.partial`
    /// files themselves are: what was written, where.
    routed_extents: BTreeMap<u32, Vec<MemberExtent>>,
    /// [`Self::member_partials`]'s archive-order result, rebuilt on adoption and
    /// on every layout change rather than recomputed per article (nit): the
    /// ordering scans the layout once per member, so the uncached form was
    /// O(members²) on a path that runs for every span of every article.
    member_order: Vec<u32>,
    /// A member was adopted, or the layout moved one. Only these two things can
    /// change the archive order, and both are rare — once per member and once
    /// per volume — where the read is per span.
    member_order_stale: bool,
    holds_budget: u64,
    demoted: Option<DemotionReason>,
}

impl std::fmt::Debug for DirectSetRouter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectSetRouter")
            .field("set_name", &self.plan.set_name)
            .field("volumes", &self.plan.volumes.len())
            .field(
                "format",
                &self.layout.as_ref().map(|layout| layout.format()),
            )
            .field("members", &self.members.len())
            .field("demoted", &self.demoted)
            .finish()
    }
}

impl DirectSetRouter {
    pub(crate) fn new(plan: DirectSetPlan) -> Self {
        Self {
            plan,
            layout: None,
            volume_facts: BTreeMap::new(),
            staging: BTreeMap::new(),
            members: BTreeMap::new(),
            member_ids: HashMap::new(),
            next_member_id: 0,
            routed_extents: BTreeMap::new(),
            member_order: Vec::new(),
            member_order_stale: false,
            holds_budget: DEFAULT_HOLDS_BUDGET_BYTES,
            demoted: None,
        }
    }

    pub(crate) fn plan(&self) -> &DirectSetPlan {
        &self.plan
    }

    /// Lowers the holds ceiling so a test can breach it without staging tens of
    /// megabytes.
    #[cfg(test)]
    pub(crate) fn set_holds_budget(&mut self, bytes: u64) {
        self.holds_budget = bytes;
    }

    /// The layout's members, or nothing while the format is still unknown.
    fn layout_members(&self) -> &[weaver_unrar::StoredMember] {
        self.layout
            .as_ref()
            .map(StoredLayoutBuilder::members)
            .unwrap_or(&[])
    }

    /// [`StoredLayoutBuilder::map_physical_range`], answering "no destination
    /// yet" while the layout is unbound — the same answer it gives for a volume
    /// it has not learned.
    fn map_physical_range(&self, volume: u32, offset: u64, len: u64) -> Vec<MappedSlice> {
        match self.layout.as_ref() {
            Some(layout) => layout.map_physical_range(volume, offset, len),
            None if len == 0 => Vec::new(),
            None => vec![MappedSlice::Unroutable { len }],
        }
    }

    pub(crate) fn demote(&mut self, reason: DemotionReason) {
        self.demoted.get_or_insert(reason);
    }

    /// Total bytes the set is currently holding in RAM: the holds proper, plus
    /// the envelope-classified bytes retained for the header walk (M1).
    ///
    /// Breaching the budget demotes. Paging the retained region out to scratch —
    /// which would let a large `-rr` set stay direct instead of materializing —
    /// is **wave 3**; until it exists the only lever is the demotion, so the
    /// count has to include everything RSS is actually paying for.
    pub(crate) fn staged_bytes(&self) -> u64 {
        self.staging.values().fold(0u64, |total, staging| {
            total.saturating_add(staging.staged_bytes())
        })
    }

    /// Members the router has learned, in **archive order**:
    /// `(stable member id, raw name, working-dir-relative partial)`.
    ///
    /// Archive order is `(first volume, physical offset in that volume)`, not
    /// the order weaver happened to discover them in and not the layout's
    /// first-seen index — volumes arrive out of order, and finalization commits
    /// members to their sanitized destinations in archive order so that two
    /// members sanitizing to the same path collide exactly the way the
    /// incremental extractor makes them collide (D3).
    pub(crate) fn member_partials(&self) -> Vec<(u32, &str, &str)> {
        self.member_order
            .iter()
            .filter_map(|member_id| {
                let member = self.members.get(member_id)?;
                Some((
                    *member_id,
                    member.name.as_str(),
                    member.relative_partial.as_str(),
                ))
            })
            .collect()
    }

    /// `(raw name, declared unpacked size)` per member, in archive order — what
    /// the checkpoint's plan digest binds (D6).
    ///
    /// The size is carried because it is stable in the facts and it is the one
    /// thing that changes when a claimed extent's underlying header changes
    /// without the name changing; digesting a literal zero for it, as the first
    /// shape did, made the digest blind to exactly the fact it cites as its
    /// reason for excluding the per-part extents.
    pub(crate) fn member_digest_entries(&self) -> Vec<(String, u64)> {
        self.member_order
            .iter()
            .filter_map(|member_id| {
                let member = self.members.get(member_id)?;
                Some((member.name.clone(), member.unpacked_size))
            })
            .collect()
    }

    /// Recomputes [`Self::member_partials`]' archive order. Called on adoption
    /// and after a layout rebuild — the only two things that can move a member.
    fn rebuild_member_order(&mut self) {
        let mut ordered: Vec<(u32, u64, u32)> = self
            .members
            .keys()
            .map(|member_id| {
                let position = self.archive_position(*member_id);
                (position.0, position.1, *member_id)
            })
            .collect();
        ordered.sort_unstable();
        self.member_order = ordered
            .into_iter()
            .map(|(_, _, member_id)| member_id)
            .collect();
    }

    /// `(first volume, physical offset in it)` for one member, or the far end of
    /// the space while the layout has not placed it yet — an unplaced member
    /// sorts last rather than jumping to the front of the commit order.
    fn archive_position(&self, member_id: u32) -> (u32, u64) {
        let Some(member) = self.members.get(&member_id) else {
            return (u32::MAX, u64::MAX);
        };
        self.layout_members()
            .iter()
            .find(|candidate| candidate.name == member.name)
            .and_then(|candidate| {
                let part = candidate.parts.first()?;
                Some((part.volume, part.data_offset))
            })
            .unwrap_or((u32::MAX, u64::MAX))
    }

    /// Stable id for a layout index, or `None` while the router has not adopted
    /// that member (an ineligible one, or one a parse has not synced yet).
    fn member_id_for_layout(&self, layout_index: usize) -> Option<u32> {
        let name = &self.layout_members().get(layout_index)?.name;
        self.member_ids.get(name).copied()
    }

    /// The layout index behind a stable member id, which is what the layout's
    /// per-part facts are keyed by.
    fn layout_index_for_member(&self, member_id: u32) -> Option<usize> {
        let name = &self.members.get(&member_id)?.name;
        self.layout_members()
            .iter()
            .position(|member| &member.name == name)
    }

    /// Whether every learned member has passed its whole-member gate.
    pub(crate) fn all_members_verified(&self) -> bool {
        !self.members.is_empty() && self.members.values().all(|member| member.verified)
    }

    /// Routes one decoded source span.
    ///
    /// The returned spans must **all** be written before the caller records the
    /// article as placed; a span that is not written is a coverage hole, not a
    /// silent loss, because the barrier is only told about writes that returned.
    pub(crate) fn route(
        &mut self,
        volume_index: u32,
        source_offset: u64,
        data: &[u8],
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        if let Some(reason) = self.demoted {
            return Err(reason);
        }
        if !self.plan.volumes.contains_key(&volume_index) {
            return Err(self.fail(DemotionReason::ConflictingVolumeFacts));
        }

        let staging = self.staging.entry(volume_index).or_default();
        staging.stage(source_offset, data);

        self.try_parse_volume(volume_index)?;
        // Every volume, not just this one: a header landing here is exactly what
        // resolves a *later* volume's split-continuation offset, so its holds
        // become routable in the same call.
        let volumes: Vec<u32> = self.staging.keys().copied().collect();
        let mut spans = Vec::new();
        for volume in volumes {
            spans.extend(self.drain_volume(volume)?);
        }

        if self.staged_bytes() > self.holds_budget {
            return Err(self.fail(DemotionReason::HoldsBudgetExceeded));
        }
        Ok(spans)
    }

    /// The volume's source bytes are all accounted for. Runs the confirming
    /// header parse, re-checks the chain-close eligibility rule, and drains
    /// whatever confirmation just made routable.
    ///
    /// The drain is not incidental: a volume's trailing region — trailing
    /// headers, the end-of-archive record, a recovery record — is held until the
    /// volume is confirmed, because until then an undiscovered member could live
    /// there. For the *last* volume of a set confirmation only ever arrives
    /// here, so without this call those bytes would be held for the life of the
    /// set and never reach the envelope.
    pub(crate) fn note_volume_complete(
        &mut self,
        volume_index: u32,
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        if let Some(reason) = self.demoted {
            return Err(reason);
        }
        // Set before the parse, not after: this is what licenses the parse
        // about to run to be the *confirming* one.
        self.staging
            .entry(volume_index)
            .or_default()
            .source_complete = true;
        self.try_parse_volume(volume_index)?;
        self.check_eligibility()?;
        let volumes: Vec<u32> = self.staging.keys().copied().collect();
        let mut spans = Vec::new();
        for volume in volumes {
            spans.extend(self.drain_volume(volume)?);
        }
        Ok(spans)
    }

    fn fail(&mut self, reason: DemotionReason) -> DemotionReason {
        self.demoted.get_or_insert(reason);
        reason
    }

    /// Parses the volume's headers out of its staged image, provisionally the
    /// first time and confirmingly once the walk reaches the archive end.
    fn try_parse_volume(&mut self, volume_index: u32) -> Result<(), DemotionReason> {
        let Some(staging) = self.staging.get(&volume_index) else {
            return Ok(());
        };
        if staging.confirmed {
            return Ok(());
        }
        // Nothing can be parsed until the volume's own prefix is staged from
        // zero: the signature lives there.
        if staging.pending.contiguous_from_zero() == 0 && staging.routed.contiguous_from_zero() == 0
        {
            return Ok(());
        }
        if !staging.provisional && staging.staged_bytes() > MAX_HEADER_PREFIX_BYTES {
            return Err(self.fail(DemotionReason::UnparsableVolume));
        }

        let image = SparseImage::from_chunks(&staging.chunks);
        let Ok(facts) = weaver_unrar::RarArchive::parse_volume_facts(image, None) else {
            // A prefix too short to hold a whole header is normal early on; the
            // next article retries. A genuinely unparsable volume is caught by
            // the prefix ceiling above.
            return Ok(());
        };
        if facts.members.is_empty() {
            return Ok(());
        }
        // Two proofs, and only two. `more_volumes` can only be true when the
        // library parsed an end-of-archive record, which is the last header a
        // volume can carry; a complete source image is the parse having seen
        // every byte there will ever be. Chain closure (`!split_after`) is
        // **not** a third: a truncated prefix closes the chain the moment the
        // first member's final part is read, while a second member's header
        // sits unread past that member's data area.
        let source_complete = self
            .staging
            .get(&volume_index)
            .is_some_and(|staging| staging.source_complete);
        let reached_end = facts.more_volumes || source_complete;

        if self.layout.is_none() {
            let format = facts.archive_format();
            if !matches!(format, ArchiveFormat::Rar4 | ArchiveFormat::Rar5) {
                return Err(self.fail(DemotionReason::UnsupportedFormat));
            }
            self.layout = Some(StoredLayoutBuilder::new(format));
        }

        // What this parse says about the volume, against what the last accepted
        // one said. A longer staged prefix can only *append* headers — the walk
        // is sequential from offset 0 — so an extension is the confirming parse
        // doing its job, and anything else is a real disagreement.
        match self.volume_facts.get(&volume_index) {
            // The layout consumes the member list and nothing else, so a parse
            // that only learned more *about the volume* (it finally reached the
            // end-of-archive record, say) needs no layout work at all.
            Some(previous) if previous.members == facts.members => {
                self.volume_facts.insert(volume_index, facts.clone());
            }
            Some(previous) if members_extend(previous, &facts) => {
                self.volume_facts.insert(volume_index, facts.clone());
                self.rebuild_layout()?;
            }
            Some(_) => return Err(self.fail(DemotionReason::ConflictingVolumeFacts)),
            None => {
                self.volume_facts.insert(volume_index, facts.clone());
                let added = self
                    .layout
                    .as_mut()
                    .expect("the layout was bound above")
                    .add_volume(volume_index, &facts);
                match added {
                    Ok(()) => {}
                    Err(StoredLayoutError::ConflictingVolume { .. }) => {
                        return Err(self.fail(DemotionReason::ConflictingVolumeFacts));
                    }
                    Err(StoredLayoutError::FormatMismatch { .. }) => {
                        return Err(self.fail(DemotionReason::FormatMismatch));
                    }
                }
                // A volume arriving out of order can put a member's *first* part
                // in a volume the layout only learned about now, which moves the
                // archive order the commit loop walks.
                self.member_order_stale = true;
            }
        }

        let tail_base = facts
            .members
            .iter()
            .map(|member| member.data_offset.saturating_add(member.data_size))
            .max()
            .unwrap_or(0);
        if let Some(staging) = self.staging.get_mut(&volume_index) {
            staging.provisional = true;
            staging.confirmed = reached_end;
            staging.tail_base = staging.tail_base.max(tail_base);
        }

        self.sync_members()?;
        // A member can become verifiable from a *parse* rather than from a
        // routed byte. A zero-length stored member has no byte to route at all
        // (B2), and a chain whose closing header arrives after its last byte
        // was already placed has nothing left to trigger the gate. Either one
        // would otherwise stay unverified for the life of the job: the set
        // never finalizes, never demotes, and its D7 suppressions stay armed
        // over files that will never exist.
        let member_ids: Vec<u32> = self.members.keys().copied().collect();
        for member_id in member_ids {
            self.try_verify_member(member_id)?;
        }
        self.check_eligibility()?;
        Ok(())
    }

    /// Rebuilds the layout from every volume's newest facts.
    ///
    /// Volumes are re-added in ascending order so the rebuild is deterministic,
    /// and members keep their weaver-side identity because that identity is the
    /// header name, not the layout's index — which the rebuild is free to move.
    fn rebuild_layout(&mut self) -> Result<(), DemotionReason> {
        let Some(format) = self.layout.as_ref().map(StoredLayoutBuilder::format) else {
            return Ok(());
        };
        let mut rebuilt = StoredLayoutBuilder::new(format);
        for (volume_index, facts) in &self.volume_facts {
            match rebuilt.add_volume(*volume_index, facts) {
                Ok(()) => {}
                Err(StoredLayoutError::ConflictingVolume { .. }) => {
                    return Err(self.fail(DemotionReason::ConflictingVolumeFacts));
                }
                Err(StoredLayoutError::FormatMismatch { .. }) => {
                    return Err(self.fail(DemotionReason::FormatMismatch));
                }
            }
        }
        self.layout = Some(rebuilt);
        // A rebuild is free to renumber and reposition every member.
        self.member_order_stale = true;
        Ok(())
    }

    /// Adopts every direct-routable member the layout now knows about.
    ///
    /// Phase 4 demoted a set the moment a second routable member appeared. There
    /// is nothing in the router that needs one member — the layout already maps
    /// several members' extents inside one volume, per-member state is a map,
    /// and every gate is per member. What the restriction bought was the
    /// finalization and demotion bookkeeping being trivially per-set; wave 1
    /// pays for those properly instead.
    fn sync_members(&mut self) -> Result<(), DemotionReason> {
        // Collisions are decided over **every member the layout has started**,
        // not just the routable ones, and not pairwise as members are adopted:
        // the second member of a colliding pair may be the one that arrives
        // first, and wave 2's small-member tolerance will keep an ineligible
        // member's bytes inside the set rather than demoting on sight — at which
        // point an ineligible member colliding with a routed one is a member
        // silently overwriting another, exactly what the extractor refuses.
        let started: Vec<String> = self
            .layout_members()
            .iter()
            .map(|member| member.name.clone())
            .collect();
        let mut seen: std::collections::HashSet<String> =
            std::collections::HashSet::with_capacity(started.len());
        // Second key, same sweep: two names that differ only past the filename
        // clamp resolve to distinct destinations but to *one* `.direct.partial`,
        // and the extractor-parity key above cannot see that because it folds
        // the unclamped path.
        let mut partials: std::collections::HashSet<String> =
            std::collections::HashSet::with_capacity(started.len());
        for name in &started {
            let Ok(key) = DirectSetPlan::member_collision_key(name) else {
                return Err(self.fail(DemotionReason::UnsafeDestination));
            };
            let Ok(partial) = self.plan.member_partial_path(name) else {
                return Err(self.fail(DemotionReason::UnsafeDestination));
            };
            if !seen.insert(key) || !partials.insert(partial.to_ascii_lowercase()) {
                return Err(self.fail(DemotionReason::CollidingDestinations));
            }
        }

        let routable: Vec<(String, u64)> = self
            .layout_members()
            .iter()
            .filter(|member| member.eligibility.routes_direct())
            .map(|member| (member.name.clone(), member.unpacked_size.unwrap_or(0)))
            .collect();
        for (name, unpacked_size) in routable {
            if let Some(member_id) = self.member_ids.get(&name).copied() {
                if let Some(existing) = self.members.get_mut(&member_id) {
                    existing.unpacked_size = unpacked_size;
                }
                continue;
            }
            let relative_partial = match self.plan.member_partial_path(&name) {
                Ok(path) => path,
                Err(()) => return Err(self.fail(DemotionReason::UnsafeDestination)),
            };
            let member_id = self.next_member_id;
            self.next_member_id = self.next_member_id.saturating_add(1);
            self.member_ids.insert(name.clone(), member_id);
            self.members.insert(
                member_id,
                MemberRouting {
                    name,
                    relative_partial,
                    unpacked_size,
                    covered: ByteRanges::new(),
                    parts: BTreeMap::new(),
                    checked_parts: BTreeMap::new(),
                    verified: false,
                },
            );
            self.member_order_stale = true;
        }
        if self.member_order_stale {
            self.rebuild_member_order();
            self.member_order_stale = false;
        }
        Ok(())
    }

    /// Revision 6 amendment 1: a provisional member that resolves `Ineligible`
    /// at chain close demotes the group at that transition — **unless** it fits
    /// inside D1's bounded small-member tolerance.
    ///
    /// The tolerance is a deliberate weaver extension over the oracle, and it is
    /// bounded three ways, each of which demotes on breach:
    ///
    /// 1. **By kind.** Only `Compressed` and `Blake2OnlyNoCrc32` are tolerable.
    ///    The library's classification order (malformed → directory →
    ///    redirection → encrypted → solid → compressed) means a `Compressed`
    ///    verdict already proves the member is an unencrypted, non-solid,
    ///    per-member regular file with a well-formed chain, which is exactly
    ///    D1's precondition — so the precondition is read off the reason rather
    ///    than re-derived from flags weaver would have to trust separately.
    /// 2. **By size**, aggregated over every tolerated member:
    ///    `min(64 MiB, 1% of the archive's packed bytes)` packed and 256 MiB
    ///    unpacked.
    /// 3. **By the set still being a store set.** A set whose members are *all*
    ///    ineligible has nothing to route and no benefit to gain; it demotes and
    ///    the ordinary extractor produces every member.
    ///
    /// **Provisional totals.** `packed_bytes` is a running sum over the parts
    /// seen so far until the member's chain closes (`totals_final`), so an open
    /// chain's value is a *lower bound*: breaching the budget on a lower bound
    /// demotes immediately, and staying under it admits only provisionally. The
    /// relative (1%) half needs the archive's packed total, which is final only
    /// once every planned volume has been parsed and every chain has closed —
    /// so it is enforced there, which is the close this rule re-checks at. A
    /// member whose true total breaches at close demotes exactly then, before
    /// the set can finalize.
    fn check_eligibility(&mut self) -> Result<(), DemotionReason> {
        let mut packed = 0u64;
        let mut unpacked = 0u64;
        let mut tolerated = 0usize;
        let mut routable = 0usize;
        let mut first_tolerated: Option<IneligibilityReason> = None;

        for member in self.layout_members() {
            let MemberEligibility::Ineligible(reason) = member.eligibility else {
                routable += 1;
                continue;
            };
            // A member the router **adopted** routed bytes into its own
            // `.direct.partial` while it was still `ProvisionallyDirect` — a
            // split BLAKE2sp-only member is the reachable case, since the digest
            // only disqualifies it once its chain closes. Those bytes are not in
            // the envelope, so the virtual volume the tolerated extraction reads
            // is not the volume the archive describes, and finalization would
            // additionally commit the partial as if it were a stored member's.
            // Moving them back is real work with no owner yet; until it has one,
            // an adopted member is not tolerable and the set demotes on its own
            // reason, exactly as wave 1 did.
            let already_routed = self.member_ids.contains_key(&member.name);
            let budget = tolerable_member_budget(member, reason).filter(|_| !already_routed);
            let Some(budget) = budget else {
                return Err(self.fail(DemotionReason::MemberIneligible(reason.into())));
            };
            tolerated += 1;
            first_tolerated.get_or_insert(reason);
            // Saturating rather than checked: the ceilings below are far under
            // `u64::MAX`, so a saturated sum breaches and demotes, which is the
            // fail-closed answer a header claiming impossible sizes deserves.
            packed = packed.saturating_add(budget.packed_bytes);
            unpacked = unpacked.saturating_add(budget.unpacked_bytes);
        }

        let Some(first_tolerated) = first_tolerated else {
            debug_assert_eq!(tolerated, 0);
            return Ok(());
        };
        if routable == 0 {
            // Nothing to route: every byte would land in the envelope and every
            // member would be extracted conventionally at the end, which is the
            // conventional path with an extra copy of the volumes in it. Reported
            // under the member's *own* reason, not the tolerance's — the set is
            // not over a budget, it is simply not a store set.
            return Err(self.fail(DemotionReason::MemberIneligible(first_tolerated.into())));
        }
        if packed > TOLERANCE_PACKED_CEILING_BYTES || unpacked > TOLERANCE_UNPACKED_CEILING_BYTES {
            return Err(self.fail(DemotionReason::ToleranceBudgetExceeded));
        }
        if self.archive_totals_final() {
            let archive_packed = self.archive_packed_bytes().unwrap_or(u64::MAX);
            // `packed * 100 > archive_packed`, without the multiply: a large
            // archive would overflow it, and the overflowing case is precisely
            // the one that must not demote.
            if packed > archive_packed / TOLERANCE_ARCHIVE_PERCENT {
                return Err(self.fail(DemotionReason::ToleranceBudgetExceeded));
            }
        }
        Ok(())
    }

    /// Whether every planned volume has been parsed and every member's chain has
    /// closed, so the archive's packed total can no longer grow.
    fn archive_totals_final(&self) -> bool {
        self.volume_facts.len() == self.plan.volumes.len()
            && self
                .layout_members()
                .iter()
                .all(|member| member.chain_complete)
    }

    /// Packed bytes over every member of the archive. `None` on overflow, which
    /// only a hostile header can claim.
    fn archive_packed_bytes(&self) -> Option<u64> {
        let mut total = 0u64;
        for member in self.layout_members() {
            for part in &member.parts {
                total = total.checked_add(part.data_size)?;
            }
        }
        Some(total)
    }

    /// Members riding D1's tolerance, by raw header name, in archive order.
    ///
    /// Finalization extracts exactly these and nothing else: the direct-routed
    /// members are already at their destinations, and re-extracting one would
    /// overwrite verified output with a second decode of the same bytes.
    pub(crate) fn tolerated_member_names(&self) -> Vec<String> {
        let mut names: Vec<(u32, u64, String)> = self
            .layout_members()
            .iter()
            .filter(|member| matches!(member.eligibility, MemberEligibility::Ineligible(_)))
            .map(|member| {
                let position = member
                    .parts
                    .first()
                    .map(|part| (part.volume, part.data_offset))
                    .unwrap_or((u32::MAX, u64::MAX));
                (position.0, position.1, member.name.clone())
            })
            .collect();
        names.sort_unstable();
        names.into_iter().map(|(_, _, name)| name).collect()
    }

    /// Maps and emits every pending byte of one volume whose destination the
    /// layout can now name.
    fn drain_volume(&mut self, volume_index: u32) -> Result<Vec<RoutedSpan>, DemotionReason> {
        let Some(staging) = self.staging.get(&volume_index) else {
            return Ok(Vec::new());
        };
        if staging.pending.is_empty() {
            return Ok(Vec::new());
        }
        let pending: Vec<(u64, u64)> = staging.pending.ranges().to_vec();
        // Beyond this the volume's classification is unproven; see
        // [`VolumeStaging::tail_base`]. A confirmed volume has no such region.
        let unproven_from = if staging.confirmed {
            u64::MAX
        } else {
            staging.tail_base
        };
        let mut spans = Vec::new();
        let mut routed = Vec::new();

        for (start, end) in pending {
            let mut cursor = start;
            for slice in self.map_physical_range(volume_index, start, end - start) {
                match slice {
                    MappedSlice::Unroutable { len } => {
                        cursor = cursor.saturating_add(len);
                    }
                    MappedSlice::Envelope { len } if cursor >= unproven_from => {
                        // Held, not routed: a member whose header the walk has
                        // not reached yet would have its payload written into
                        // the envelope and deleted with it.
                        cursor = cursor.saturating_add(len);
                    }
                    MappedSlice::Envelope { len } => {
                        // Envelope v2: the destination offset *is* the physical
                        // offset, so there is no slot arithmetic left to
                        // overflow and no ceiling left to demote against. A
                        // recovery record, a quick-open block or an ineligible
                        // member's packed range fits by definition — the file is
                        // a sparse image of the volume it came from.
                        let bytes = self
                            .staging
                            .get(&volume_index)
                            .and_then(|staging| staging.slice(cursor, len));
                        if let Some(bytes) = bytes {
                            spans.push(RoutedSpan {
                                destination: DirectDestination::Envelope { volume_index },
                                destination_offset: cursor,
                                volume_index,
                                source_offset: cursor,
                                bytes,
                            });
                            routed.push((cursor, len));
                        }
                        cursor = cursor.saturating_add(len);
                    }
                    MappedSlice::Member {
                        member_index,
                        logical_offset,
                        len,
                    } => {
                        // Unreachable by construction: the layout only maps a
                        // member the router has adopted, and adoption assigns
                        // the id. Leaving the run pending rather than asserting
                        // keeps a would-be panic as a holds-budget demotion.
                        let Some(member_id) = self.member_id_for_layout(member_index) else {
                            debug_assert!(
                                false,
                                "the layout mapped member {member_index} of {}, which the router \
                                 never adopted",
                                self.plan.set_name
                            );
                            cursor = cursor.saturating_add(len);
                            continue;
                        };
                        let bytes = self
                            .staging
                            .get(&volume_index)
                            .and_then(|staging| staging.slice(cursor, len));
                        if let Some(bytes) = bytes {
                            self.note_member_bytes(
                                member_id,
                                volume_index,
                                logical_offset,
                                &bytes,
                            )?;
                            // Recorded here, at the moment a member destination
                            // is chosen, and never revisited: this is the only
                            // account of where the bytes went that survives the
                            // member turning ineligible (B1).
                            self.record_routed_extent(
                                volume_index,
                                MemberExtent {
                                    member_id,
                                    physical_offset: cursor,
                                    logical_offset,
                                    len,
                                },
                            );
                            spans.push(RoutedSpan {
                                destination: DirectDestination::Member { member_id },
                                destination_offset: logical_offset,
                                volume_index,
                                source_offset: cursor,
                                bytes,
                            });
                            routed.push((cursor, len));
                        }
                        cursor = cursor.saturating_add(len);
                    }
                }
            }
        }

        if let Some(staging) = self.staging.get_mut(&volume_index) {
            for (start, len) in &routed {
                staging.routed.insert(*start, *len);
            }
            let mut still_pending = ByteRanges::new();
            for (start, end) in staging.pending.ranges() {
                still_pending.insert(*start, end - start);
            }
            for (start, len) in &routed {
                still_pending = subtract(&still_pending, *start, *len);
            }
            staging.pending = still_pending;
        }
        self.trim_volume(volume_index);
        Ok(spans)
    }

    /// Keeps only what the header parser still needs: the envelope, and any
    /// bytes still waiting for a destination.
    ///
    /// Once a volume is **confirmed** the parser will never walk it again, so
    /// its envelope bytes are dropped from RAM entirely. That matters much more
    /// with envelope v2 than it did with the 64 KiB slots: a `-rr` volume's
    /// recovery record is envelope-classified and can be percent-of-volume
    /// sized, so keeping it staged for the life of the set would put an
    /// unbounded, volume-count-proportional term in RSS. Until confirmation it
    /// is retained, because the walk has to seek past the recovery service
    /// header to reach the end-of-archive record.
    fn trim_volume(&mut self, volume_index: u32) {
        let mut keep = ByteRanges::new();
        if let Some(staging) = self.staging.get(&volume_index) {
            for (start, end) in staging.pending.ranges() {
                keep.insert(*start, end - start);
            }
            if staging.confirmed {
                let pending = keep;
                if let Some(staging) = self.staging.get_mut(&volume_index) {
                    staging.trim(&pending);
                }
                return;
            }
            let chunks: Vec<(u64, u64)> = staging
                .chunks
                .iter()
                .map(|(offset, bytes)| (*offset, bytes.len() as u64))
                .collect();
            for (offset, len) in chunks {
                let mut cursor = offset;
                for slice in self.map_physical_range(volume_index, offset, len) {
                    match slice {
                        MappedSlice::Member { len, .. } => cursor = cursor.saturating_add(len),
                        MappedSlice::Envelope { len } | MappedSlice::Unroutable { len } => {
                            keep.insert(cursor, len);
                            cursor = cursor.saturating_add(len);
                        }
                    }
                }
            }
        }
        if let Some(staging) = self.staging.get_mut(&volume_index) {
            staging.trim(&keep);
        }
    }

    /// Feeds one routed member run into the integrity gates (D4 layers 1 and 2).
    fn note_member_bytes(
        &mut self,
        member_id: u32,
        volume_index: u32,
        logical_offset: u64,
        bytes: &[u8],
    ) -> Result<(), DemotionReason> {
        let len = bytes.len() as u64;
        let Some(layout_index) = self.layout_index_for_member(member_id) else {
            return Ok(());
        };
        let Some(part) = self.part_for(layout_index, volume_index) else {
            return Ok(());
        };
        let (part_position, part_logical_offset, part_len, packed_crc32) = part;
        let Some(member) = self.members.get_mut(&member_id) else {
            return Ok(());
        };
        if member.covered.insert(logical_offset, len) == 0 {
            // Wholly duplicate: never advance a gate twice.
            return Ok(());
        }
        let crc = weaver_par2::checksum::crc32(bytes);
        member.parts.entry(part_position).or_default().insert(
            logical_offset.saturating_sub(part_logical_offset),
            len,
            crc,
        );

        // Layer 1: the part's packed CRC32, as soon as the part is complete.
        //
        // Guarded by the coverage map rather than attempted every time: the
        // composition now walks the runs it was fed instead of reading one
        // merged value (M3), so asking before the part is whole would be a scan
        // per span for an answer that cannot exist yet.
        let part_complete = member
            .covered
            .missing(part_logical_offset, part_len)
            .is_empty();
        let part_value = part_complete
            .then(|| {
                member
                    .parts
                    .get(&part_position)
                    .and_then(|runs| runs.compose(0, part_len))
            })
            .flatten();
        if let Some(value) = part_value {
            member.checked_parts.insert(part_position, value);
            if let Some(expected) = packed_crc32
                && expected != value
            {
                return Err(self.fail(DemotionReason::PartChecksumMismatch));
            }
        }

        self.try_verify_member(member_id)
    }

    /// `(position in chain, logical offset, packed length, packed CRC32)` for
    /// the part of the layout member at `layout_index` living in `volume_index`.
    fn part_for(
        &self,
        layout_index: usize,
        volume_index: u32,
    ) -> Option<(u32, u64, u64, Option<u32>)> {
        let member = self.layout_members().get(layout_index)?;
        member
            .parts
            .iter()
            .enumerate()
            .find(|(_, part)| part.volume == volume_index)
            .map(|(position, part)| {
                (
                    position as u32,
                    part.logical_offset.unwrap_or(0),
                    part.data_size,
                    part.packed_crc32,
                )
            })
    }

    /// Layer 2: the whole-member CRC32, composed from the parts in logical
    /// order once every part is complete and the chain has closed.
    fn try_verify_member(&mut self, member_id: u32) -> Result<(), DemotionReason> {
        let Some(layout_index) = self.layout_index_for_member(member_id) else {
            return Ok(());
        };
        let Some(layout_member) = self.layout_members().get(layout_index) else {
            return Ok(());
        };
        if !layout_member.chain_complete {
            return Ok(());
        }
        let Some(expected) = layout_member.data_crc32 else {
            // The chain closed with no whole-member CRC32, which the layout
            // reports as `Ineligible`; `check_eligibility` owns that demotion.
            return Ok(());
        };
        let part_lengths: Vec<u64> = layout_member
            .parts
            .iter()
            .map(|part| part.data_size)
            .collect();
        let unpacked_size = layout_member.unpacked_size.unwrap_or(0);
        if self
            .members
            .get(&member_id)
            .is_none_or(|member| member.verified)
        {
            return Ok(());
        }
        if unpacked_size == 0 {
            // A zero-length stored member (B2). Nothing will ever be routed for
            // it, so the byte-driven gate below can never fire: the first shape
            // returned here and left `verified` false for the life of the job,
            // which is a set that never finalizes, never demotes and keeps its
            // D7 suppressions armed — a permanent zombie.
            //
            // The CRC32 of no bytes is `0x00000000`, which is exactly what RAR
            // writes into an empty member's header, so the same gate closes it:
            // anything else is a header disagreeing with itself.
            if expected != 0 {
                return Err(self.fail(DemotionReason::MemberChecksumMismatch));
            }
            if let Some(member) = self.members.get_mut(&member_id) {
                member.verified = true;
            }
            return Ok(());
        }
        let Some(member) = self.members.get(&member_id) else {
            return Ok(());
        };
        if member.covered.contiguous_from_zero() < unpacked_size {
            return Ok(());
        }

        let mut composed = 0u32;
        for (position, len) in part_lengths.iter().enumerate() {
            let Some(value) = member.checked_parts.get(&(position as u32)).copied() else {
                return Ok(());
            };
            composed = weaver_par2::checksum::Crc32CombineOp::new(*len).combine(composed, value);
        }
        if composed != expected {
            return Err(self.fail(DemotionReason::MemberChecksumMismatch));
        }
        if let Some(member) = self.members.get_mut(&member_id) {
            member.verified = true;
        }
        Ok(())
    }

    /// Files one emitted member extent into the volume's routing history,
    /// coalescing it with the extent it continues (B1).
    ///
    /// A physical byte is routed at most once — [`VolumeStaging::stage`] never
    /// re-stages a routed range — so the history is disjoint by construction and
    /// an insert is either an extension of the previous extent or a new one.
    fn record_routed_extent(&mut self, volume_index: u32, extent: MemberExtent) {
        if extent.len == 0 {
            return;
        }
        let extents = self.routed_extents.entry(volume_index).or_default();
        let position =
            extents.partition_point(|held| held.physical_offset < extent.physical_offset);
        extents.insert(position, extent);
        // Coalesce forwards, then backwards. A member's bytes arrive article by
        // article and span by span, so without this the history would carry one
        // extent per span and the provider's binary search would walk a list as
        // long as the download.
        if position + 1 < extents.len() && continues(extents[position], extents[position + 1]) {
            extents[position].len = extents[position]
                .len
                .saturating_add(extents[position + 1].len);
            extents.remove(position + 1);
        }
        if let Some(previous) = position.checked_sub(1)
            && continues(extents[previous], extents[position])
        {
            extents[previous].len = extents[previous].len.saturating_add(extents[position].len);
            extents.remove(position);
        }
    }

    /// The physical map of one volume, as the hybrid virtual-volume provider
    /// needs it: every member extent the router **has routed bytes for**, in
    /// physical order, with the logical offset the extent starts at inside its
    /// member's partial.
    ///
    /// Read off the routing history, deliberately **not** off
    /// [`StoredLayoutBuilder::map_physical_range`]'s current answer (B1). The
    /// layout maps a member's packed range to the member only while
    /// `routes_direct()` holds, and that is a running verdict: a
    /// `ProvisionallyDirect` member whose chain closes with a BLAKE2sp digest
    /// and no CRC32 becomes `Ineligible` in the same call that demotes the set,
    /// and every byte already sitting in its `.direct.partial` would suddenly
    /// map to the envelope — where it is a hole inside the file's length, which
    /// a plain `read` answers with zeros. Demotion runs reconstruction, so those
    /// zeros would be written into the volume file under a published floor and
    /// never fetched again.
    ///
    /// The history is what the partials themselves are, so the two cannot
    /// disagree: an extent is here exactly when bytes were written for it.
    pub(crate) fn volume_member_extents(&self, volume_index: u32) -> Vec<MemberExtent> {
        self.routed_extents
            .get(&volume_index)
            .cloned()
            .unwrap_or_default()
    }

    // There is deliberately no accessor for the router's own routed map. It
    // records what routing *emitted*, spans whose write later failed included,
    // and reading it as coverage is what let a demotion sweep try to read a byte
    // back out of a file that never received it (B1). Everything that needs to
    // know what reached disk asks `DirectSet`, which is told only about writes
    // that returned.
}

/// What one ineligible member costs D1's tolerance budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ToleratedMemberBudget {
    /// Packed bytes: the member's total when the chain has closed, a lower
    /// bound over the parts seen so far otherwise.
    packed_bytes: u64,
    /// Unpacked bytes the headers declare. Stated up front, never accumulated.
    unpacked_bytes: u64,
}

/// The budget cost of one ineligible member, or `None` when the member is not
/// tolerable at all and the set must demote.
///
/// Fail-closed on every unknown: a packed sum that overflowed `u64`, or a
/// member whose headers declare no unpacked size, has no budget that can be
/// checked, and admitting an unbounded member under a bounded rule would make
/// the rule decorative.
fn tolerable_member_budget(
    member: &weaver_unrar::StoredMember,
    reason: IneligibilityReason,
) -> Option<ToleratedMemberBudget> {
    match reason {
        // The library's classification order has already ruled out encrypted,
        // solid, directory, redirection and malformed-chain members by the time
        // it reaches `Compressed`, so this arm carries D1's precondition rather
        // than restating it.
        IneligibilityReason::Compressed {
            packed_bytes,
            unpacked_bytes,
            // Read, and deliberately unused as a gate: a non-final total is a
            // lower bound, and the caller demotes on a lower bound that already
            // breaches while re-checking at every parse until the chain closes.
            totals_final: _,
        } => Some(ToleratedMemberBudget {
            packed_bytes: packed_bytes?,
            unpacked_bytes: unpacked_bytes?,
        }),
        // A stored member with a BLAKE2sp digest and no CRC32. Out-of-order
        // routing cannot verify it (D4), but `extract_member_streaming` feeds
        // the codec in order and checks BLAKE2sp natively — which is exactly
        // why D4 scopes its whole-member-CRC32 requirement to *direct-routed*
        // members and sends this one through the tolerance instead.
        IneligibilityReason::Blake2OnlyNoCrc32 => {
            let mut packed_bytes = 0u64;
            for part in &member.parts {
                packed_bytes = packed_bytes.checked_add(part.data_size)?;
            }
            Some(ToleratedMemberBudget {
                packed_bytes,
                unpacked_bytes: member.unpacked_size?,
            })
        }
        IneligibilityReason::Encrypted
        | IneligibilityReason::Solid
        | IneligibilityReason::Directory
        | IneligibilityReason::Redirection
        | IneligibilityReason::NoChecksum
        | IneligibilityReason::MalformedChain(_) => None,
    }
}

/// Whether `next` continues `held` in **both** coordinate spaces for the same
/// member, so the two describe one run of the member's partial.
///
/// Physical adjacency alone is not enough: two extents of the same member can be
/// physically adjacent across a header the layout mapped as unroutable, with a
/// logical gap between them, and merging those would slide every byte of the
/// second one to the wrong offset inside the partial.
fn continues(held: MemberExtent, next: MemberExtent) -> bool {
    held.member_id == next.member_id
        && held.physical_offset.saturating_add(held.len) == next.physical_offset
        && held.logical_offset.saturating_add(held.len) == next.logical_offset
}

/// One direct-routed member's slice of a volume, in both coordinate spaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MemberExtent {
    pub(crate) member_id: u32,
    /// Offset of the extent inside the source volume.
    pub(crate) physical_offset: u64,
    /// Offset of the same bytes inside the member's partial.
    pub(crate) logical_offset: u64,
    pub(crate) len: u64,
}

/// Whether `candidate`'s member list is `previous`'s with more members appended.
///
/// The header walk is sequential from offset 0, so a longer staged prefix can
/// only ever reveal *further* headers: the members it already reported keep
/// their facts and their order, and the new ones land at the end. Anything else
/// — a member's facts changing, or one disappearing — is a real disagreement
/// between two parses of the same volume, which is what
/// [`DemotionReason::ConflictingVolumeFacts`] exists for.
///
/// Only the member list is compared. The volume-level fields legitimately grow
/// with the prefix — `more_volumes` flips the moment the walk reaches the
/// end-of-archive record, a recovery-record or locator service header appears
/// later than the file headers — and the layout consumes none of them beyond
/// the archive format, which is checked separately.
fn members_extend(previous: &RarVolumeFacts, candidate: &RarVolumeFacts) -> bool {
    candidate.members.len() > previous.members.len()
        && candidate.members[..previous.members.len()] == previous.members[..]
}

/// `ranges` minus `[start, start + len)`.
fn subtract(ranges: &ByteRanges, start: u64, len: u64) -> ByteRanges {
    let cut_end = start.saturating_add(len);
    let mut out = ByteRanges::new();
    for (range_start, range_end) in ranges.ranges() {
        let (range_start, range_end) = (*range_start, *range_end);
        if range_end <= start || range_start >= cut_end {
            out.insert(range_start, range_end - range_start);
            continue;
        }
        if range_start < start {
            out.insert(range_start, start - range_start);
        }
        if range_end > cut_end {
            out.insert(cut_end, range_end - cut_end);
        }
    }
    out
}
