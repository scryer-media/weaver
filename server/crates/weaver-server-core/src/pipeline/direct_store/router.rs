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
//!   parse did not, [`StoredLayoutBuilder::add_volume`] rejects the re-add and
//!   the set demotes. Without this a second member hiding past the first one's
//!   data area would have its bytes silently filed as envelope — a lost file.
//!
//! A volume stops re-parsing only on **proof** that no further header can
//! appear: either `more_volumes` (which the library sets from a parsed
//! end-of-archive record, in both RAR4 and RAR5) or the whole source volume
//! having arrived. Chain closure read off a *truncated* prefix is not proof —
//! the header that closes the chain is the first member's, and a second
//! member's header can sit past the first member's data area, beyond anything
//! the prefix reached.

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};

use weaver_unrar::{
    ArchiveFormat, IneligibilityReason, MappedSlice, MemberEligibility, StoredLayoutBuilder,
    StoredLayoutError,
};

use super::ByteRanges;
use super::plan::{DirectSetPlan, ENVELOPE_SLOT_BYTES, ENVELOPE_SLOT_HALF};

/// Default RAM ceiling for holds across one set. Phase 4 has no scratch paging
/// (phase 5 owns it), so a breach demotes.
pub(crate) const DEFAULT_HOLDS_BUDGET_BYTES: u64 = 64 * 1024 * 1024;

/// How far into a volume the provisional header parse may stage bytes before
/// the set is declared unroutable. Real RAR headers are a few hundred bytes;
/// this only exists so a set whose first article is entirely payload (a
/// corrupted or non-RAR file that reached us classified as a volume) demotes
/// instead of holding forever.
pub(crate) const MAX_HEADER_PREFIX_BYTES: u64 = 4 * 1024 * 1024;

/// Why a set left direct mode. Every variant is its own metric bucket (D1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DemotionReason {
    /// A member the layout classified `Ineligible` — including a provisional
    /// member that resolved ineligible when its chain closed (revision 6).
    MemberIneligible(MemberIneligibility),
    /// More than one direct-routable member. Phase 5 owns multi-member sets.
    MultipleMembers,
    /// Holds exceeded the RAM budget and phase 4 has no scratch paging.
    HoldsBudgetExceeded,
    /// A volume's envelope did not fit its deterministic slot in the envelope
    /// file, which for phase 4's shape means the set is not "headers only".
    EnvelopeTooLarge,
    /// A confirming parse disagreed with the provisional one, or a volume was
    /// re-added with different facts.
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
            Self::MultipleMembers => "multiple_members",
            Self::HoldsBudgetExceeded => "holds_budget",
            Self::EnvelopeTooLarge => "envelope_too_large",
            Self::ConflictingVolumeFacts => "conflicting_volume_facts",
            Self::UnparsableVolume => "unparsable_volume",
            Self::PartChecksumMismatch => "part_checksum_mismatch",
            Self::MemberChecksumMismatch => "member_checksum_mismatch",
            Self::FormatMismatch => "format_mismatch",
            Self::UnsupportedFormat => "unsupported_format",
            Self::UnsafeDestination => "unsafe_destination",
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
    Member { member_index: u32 },
    /// The set's envelope file, at the volume's deterministic slot.
    Envelope,
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

/// Coalesced (offset, len, crc32) runs over one contiguous logical space.
///
/// Feeding is out-of-order by construction, so runs merge with
/// [`weaver_par2::checksum::Crc32CombineOp`] as their neighbours arrive. A run
/// that ends up covering the whole space is the composed checksum.
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
        self.merge_at(position);
    }

    /// Merges the run at `position` with its neighbours while they are adjacent.
    fn merge_at(&mut self, position: usize) {
        let mut index = position;
        while index > 0 {
            let (previous_start, previous_len, previous_crc) = self.runs[index - 1];
            let (start, len, crc) = self.runs[index];
            if previous_start.saturating_add(previous_len) != start {
                break;
            }
            let combined =
                weaver_par2::checksum::Crc32CombineOp::new(len).combine(previous_crc, crc);
            self.runs[index - 1] = (previous_start, previous_len.saturating_add(len), combined);
            self.runs.remove(index);
            index -= 1;
        }
        while index + 1 < self.runs.len() {
            let (start, len, crc) = self.runs[index];
            let (next_start, next_len, next_crc) = self.runs[index + 1];
            if start.saturating_add(len) != next_start {
                break;
            }
            let combined =
                weaver_par2::checksum::Crc32CombineOp::new(next_len).combine(crc, next_crc);
            self.runs[index] = (start, len.saturating_add(next_len), combined);
            self.runs.remove(index + 1);
        }
    }

    /// The composed value for `[start, start + len)`, when exactly one run
    /// covers it end to end.
    pub(crate) fn exact(&self, start: u64, len: u64) -> Option<u32> {
        self.runs
            .iter()
            .find(|(run_start, run_len, _)| *run_start == start && *run_len == len)
            .map(|(_, _, crc)| *crc)
    }
}

/// A sparse byte image of one volume, read through by the header parser.
///
/// Reads inside a staged run return real bytes; reads anywhere else return
/// `Ok(0)`, which `read_exact` turns into `UnexpectedEof` and the RAR header
/// walk turns into a clean stop. Seeks always succeed — the walk seeks over
/// data areas it never reads.
pub(super) struct SparseImage {
    runs: Vec<(u64, Vec<u8>)>,
    position: u64,
}

impl SparseImage {
    pub(super) fn from_chunks(chunks: &BTreeMap<u64, Vec<u8>>) -> Self {
        Self {
            runs: chunks
                .iter()
                .map(|(offset, bytes)| (*offset, bytes.clone()))
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
    /// Non-overlapping byte runs, keyed by physical offset.
    chunks: BTreeMap<u64, Vec<u8>>,
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
    /// Physical end of the last member extent, once facts are known. Envelope
    /// runs at or above it take the slot's tail half.
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
                self.chunks.insert(start, data[from..to].to_vec());
                self.pending.insert(start, end - start);
                staged = staged.saturating_add(end - start);
            }
        }
        staged
    }

    fn pending_bytes(&self) -> u64 {
        self.pending.covered()
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
                .map(|(start, bytes)| (*start, bytes))?;
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
                self.chunks.insert(start, chunk[from..to].to_vec());
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

/// Per-member routing state.
#[derive(Debug)]
struct MemberRouting {
    /// Raw header name, the layout's key.
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
    staging: BTreeMap<u32, VolumeStaging>,
    members: BTreeMap<u32, MemberRouting>,
    holds_budget: u64,
    demoted: Option<DemotionReason>,
}

impl std::fmt::Debug for DirectSetRouter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectSetRouter")
            .field("set_name", &self.plan.set_name)
            .field("volumes", &self.plan.volumes.len())
            .field("format", &self.layout.as_ref().map(|layout| layout.format()))
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
            staging: BTreeMap::new(),
            members: BTreeMap::new(),
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

    /// Total bytes currently held in RAM awaiting a destination.
    pub(crate) fn holds_bytes(&self) -> u64 {
        self.staging.values().fold(0u64, |total, staging| {
            total.saturating_add(staging.pending_bytes())
        })
    }

    /// Members the router has learned, in layout order.
    pub(crate) fn member_partials(&self) -> Vec<(u32, &str, &str)> {
        self.members
            .iter()
            .map(|(index, member)| {
                (
                    *index,
                    member.name.as_str(),
                    member.relative_partial.as_str(),
                )
            })
            .collect()
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

        if self.holds_bytes() > self.holds_budget {
            return Err(self.fail(DemotionReason::HoldsBudgetExceeded));
        }
        Ok(spans)
    }

    /// The volume's source bytes are all accounted for. Runs the confirming
    /// header parse and re-checks the chain-close eligibility rule.
    pub(crate) fn note_volume_complete(&mut self, volume_index: u32) -> Result<(), DemotionReason> {
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
        Ok(())
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
        if !staging.provisional && staging.pending_bytes() > MAX_HEADER_PREFIX_BYTES {
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

        let tail_base = facts
            .members
            .iter()
            .map(|member| member.data_offset.saturating_add(member.data_size))
            .max()
            .unwrap_or(0);
        if let Some(staging) = self.staging.get_mut(&volume_index) {
            staging.provisional = true;
            staging.confirmed = reached_end;
            staging.tail_base = tail_base;
        }

        self.sync_members()?;
        self.check_eligibility()?;
        Ok(())
    }

    /// Adopts every direct-routable member the layout now knows about.
    fn sync_members(&mut self) -> Result<(), DemotionReason> {
        let mut routable: Vec<(u32, String, u64)> = Vec::new();
        for (index, member) in self.layout_members().iter().enumerate() {
            if !member.eligibility.routes_direct() {
                continue;
            }
            routable.push((
                index as u32,
                member.name.clone(),
                member.unpacked_size.unwrap_or(0),
            ));
        }
        if routable.len() > 1 {
            return Err(self.fail(DemotionReason::MultipleMembers));
        }
        for (index, name, unpacked_size) in routable {
            if let Some(existing) = self.members.get_mut(&index) {
                existing.unpacked_size = unpacked_size;
                continue;
            }
            let relative_partial = match self.plan.member_partial_path(&name) {
                Ok(path) => path,
                Err(()) => return Err(self.fail(DemotionReason::UnsafeDestination)),
            };
            self.members.insert(
                index,
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
        }
        Ok(())
    }

    /// Revision 6 amendment 1: a provisional member that resolves `Ineligible`
    /// at chain close demotes the group at that transition.
    fn check_eligibility(&mut self) -> Result<(), DemotionReason> {
        let ineligible = self
            .layout_members()
            .iter()
            .find_map(|member| match member.eligibility {
                MemberEligibility::Ineligible(reason) => Some(reason),
                _ => None,
            });
        if let Some(reason) = ineligible {
            return Err(self.fail(DemotionReason::MemberIneligible(reason.into())));
        }
        Ok(())
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
        let tail_base = staging.tail_base;
        let mut spans = Vec::new();
        let mut routed = Vec::new();

        for (start, end) in pending {
            let mut cursor = start;
            for slice in self.map_physical_range(volume_index, start, end - start) {
                match slice {
                    MappedSlice::Unroutable { len } => {
                        cursor = cursor.saturating_add(len);
                    }
                    MappedSlice::Envelope { len } => {
                        let destination_offset =
                            match self
                                .plan
                                .envelope_offset(volume_index, cursor, len, tail_base)
                            {
                                Some(offset) => offset,
                                None => return Err(self.fail(DemotionReason::EnvelopeTooLarge)),
                            };
                        let bytes = self
                            .staging
                            .get(&volume_index)
                            .and_then(|staging| staging.slice(cursor, len));
                        if let Some(bytes) = bytes {
                            spans.push(RoutedSpan {
                                destination: DirectDestination::Envelope,
                                destination_offset,
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
                        let member_index = member_index as u32;
                        let bytes = self
                            .staging
                            .get(&volume_index)
                            .and_then(|staging| staging.slice(cursor, len));
                        if let Some(bytes) = bytes {
                            self.note_member_bytes(
                                member_index,
                                volume_index,
                                logical_offset,
                                &bytes,
                            )?;
                            spans.push(RoutedSpan {
                                destination: DirectDestination::Member { member_index },
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
    fn trim_volume(&mut self, volume_index: u32) {
        let mut keep = ByteRanges::new();
        if let Some(staging) = self.staging.get(&volume_index) {
            for (start, end) in staging.pending.ranges() {
                keep.insert(*start, end - start);
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
        member_index: u32,
        volume_index: u32,
        logical_offset: u64,
        bytes: &[u8],
    ) -> Result<(), DemotionReason> {
        let len = bytes.len() as u64;
        let Some(part) = self.part_for(member_index, volume_index) else {
            return Ok(());
        };
        let (part_position, part_logical_offset, part_len, packed_crc32) = part;
        let Some(member) = self.members.get_mut(&member_index) else {
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
        let part_value = member
            .parts
            .get(&part_position)
            .and_then(|runs| runs.exact(0, part_len));
        if let Some(value) = part_value {
            member.checked_parts.insert(part_position, value);
            if let Some(expected) = packed_crc32
                && expected != value
            {
                return Err(self.fail(DemotionReason::PartChecksumMismatch));
            }
        }

        self.try_verify_member(member_index)
    }

    /// `(position in chain, logical offset, packed length, packed CRC32)` for
    /// the part of `member_index` living in `volume_index`.
    fn part_for(
        &self,
        member_index: u32,
        volume_index: u32,
    ) -> Option<(u32, u64, u64, Option<u32>)> {
        let member = self.layout_members().get(member_index as usize)?;
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
    fn try_verify_member(&mut self, member_index: u32) -> Result<(), DemotionReason> {
        let Some(layout_member) = self.layout_members().get(member_index as usize) else {
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
        let Some(member) = self.members.get_mut(&member_index) else {
            return Ok(());
        };
        if member.verified {
            return Ok(());
        }
        if member.covered.contiguous_from_zero() < unpacked_size || unpacked_size == 0 {
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
        if let Some(member) = self.members.get_mut(&member_index) {
            member.verified = true;
        }
        Ok(())
    }
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

/// Compile-time reminder that the envelope slot constants are the router's, not
/// the plan's, to change: [`DirectSetPlan::envelope_offset`] splits each slot in
/// half and the tail half must be able to hold a volume's trailing headers.
const _: () = assert!(ENVELOPE_SLOT_HALF * 2 == ENVELOPE_SLOT_BYTES);
