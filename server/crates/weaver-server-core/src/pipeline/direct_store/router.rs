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
use super::sparse::SparseMarking;
use crypt::{AES_BLOCK, CryptRefusal, KeyRing, MemberCrypt, block_ceil, block_floor};

pub(crate) mod crypt;

/// Default RAM ceiling for holds across one set. A breach pages to the set's
/// holds scratch (D2); only a paging failure demotes.
pub(crate) const DEFAULT_HOLDS_BUDGET_BYTES: u64 = 64 * 1024 * 1024;

/// D2's **explicit** scratch ceiling, counted against the disk acceptance target
/// rather than derived from RAM the way the oracle's auto 4×-RAM rule is.
///
/// **Per archive set, not per job or per process.** Each set owns one scratch
/// file and one [`HoldsScratch`] carrying its own copy of this number, so a job
/// with three sets can have three times this on disk at once, and a busy server
/// that multiple. That is the same shape [`DEFAULT_HOLDS_BUDGET_BYTES`] has for
/// RAM, and it is deliberate at this size: the ceiling exists to stop one
/// pathological set from filling the disk, not to be a global disk quota — which
/// would need a shared accountant across sets and jobs, and a policy for what a
/// set does when another set is using the budget. If the aggregate ever needs
/// bounding, that is the design, not a smaller constant.
///
/// A `const` for now, deliberately: plan 135's open question 1 (config vs env
/// for direct-store's own switch) is unresolved, and adding a second operator
/// surface before that is settled would have to be undone. Phase 7 moves both
/// together.
pub(crate) const HOLDS_SCRATCH_CEILING_BYTES: u64 = 512 * 1024 * 1024;

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
    /// Solid, directory, redirection and malformed-chain members always land
    /// here: the tolerance extracts its members with
    /// `extract_member_streaming`, which needs a per-member non-solid regular
    /// file it can decode on its own, and a solid member is only decodable
    /// against the rest of the solid run. A *compressed* or *BLAKE2sp-only*
    /// member instead rides [`Self::ToleranceBudgetExceeded`]'s budget.
    ///
    /// **Encrypted** members land here only when their encryption is not the
    /// one shape direct-store can route (plan 136). `classify` sends a member
    /// whose parts are all `Store` and all state the same key material to
    /// [`MemberEligibility::EncryptedStore`] and the stored-chain path, and
    /// reserves `Ineligible(Encrypted)` for encrypted **and** compressed,
    /// encrypted **and** solid, and non-uniform or unkeyable encryption. An
    /// `EncryptedStore` member that fails admission demotes under
    /// [`Self::EncryptedMemberRefused`] instead, which says *why*.
    MemberIneligible(MemberIneligibility),
    /// An encrypted `Store` member the set may not route: no password reached
    /// it, the password its header states a check for is wrong, or its key
    /// material is one this build cannot derive from (E-D1).
    ///
    /// Always a demotion, never a job failure: the conventional extractor asks
    /// the job's whole password-candidate list, which is a superset of what
    /// direct-store is handed, so demoting costs the direct route and nothing
    /// else. A *wrong* password demotes to a conventional path that will fail
    /// the same way, which is the parity this reason keeps.
    EncryptedMemberRefused(CryptRefusal),
    /// A checkpoint's crypt facts are not the facts the rebuilt layout states,
    /// or a member this run classified encrypted has no crypt row at all (E-D4).
    ///
    /// Fail-closed by construction: the alternative is rebuilding a key from a
    /// row describing a different archive, which decrypts to garbage while every
    /// coverage gate keeps passing, because coverage is about *where* bytes are
    /// and says nothing about what they decrypt to.
    EncryptedFactsDisagree,
    /// A PAR2-bearing job holds a live encrypted direct set (plan 136).
    ///
    /// The authoritative pass reads the set's source volumes through the hybrid
    /// provider, which serves member ranges out of `.direct.partial` files — and
    /// for an encrypted set those hold plaintext while PAR2 describes the posted
    /// cipher. Every slice would mismatch, the set would be called damaged, and
    /// the repair would be handed a virtual volume to fix that was never broken.
    /// Demoting before the pass keeps its world binary, exactly as
    /// [`Self::Par2Unbindable`] does: either a fully readable virtual set, or
    /// real files on disk. Phase E2's re-encrypting overlay retires this.
    EncryptedPar2Unsupported,
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
    /// One of the set's source volumes could not be bound, unambiguously, to a
    /// PAR2 description in the job's recovery set.
    ///
    /// An unbound volume cannot be served through the virtual-volume overlay —
    /// the overlay is keyed by PAR2 file id — so the authoritative pass would
    /// read it off a disk it is not on, report it missing, and hand the repairer
    /// a file to write into that does not exist. Demoting before the pass runs
    /// is what keeps that pass looking at either a *fully* bound virtual set or
    /// at real files, never at a half-bound one (B2).
    Par2Unbindable,
    /// A member riding D1's tolerance could not be extracted from the virtual
    /// volumes at finalization. The stored members are correct; the tolerated
    /// one is not produced, so the set is rebuilt the ordinary way.
    ToleratedExtractionFailed,
    /// Holds exceeded the RAM budget and paging could not bring it back — every
    /// pageable run is already in scratch and RAM is still over, which means one
    /// staged run is larger than the whole budget.
    HoldsBudgetExceeded,
    /// The holds scratch file could not be created, written or read (D2).
    HoldsScratchFailed,
    /// Paging would push the holds scratch past its configured ceiling (D2).
    /// Counted separately from the RAM budget because they say different things:
    /// this one is the *disk* claim direct-store makes against its own 1.05×
    /// acceptance target.
    HoldsScratchCeiling,
    /// A confirming parse disagreed with the provisional one, or a volume was
    /// re-added with facts that are not an extension of what it stated before.
    ConflictingVolumeFacts,
    /// A volume restored from a checkpoint finished downloading without ever
    /// being confirmed, so its trailing region could never be classified (B2).
    ///
    /// Its pre-restart bytes live on disk rather than in the staged image, so
    /// the confirming parse has a hole from offset zero and cannot succeed. The
    /// alternative to demoting is holding the volume's end record and recovery
    /// record for the life of the set, which reads as PAR2 damage and costs a
    /// full redownload — this costs a materialization from bytes already on
    /// disk.
    UnconfirmedRestoredVolume,
    /// A restart-seeded run could not be placed back into the layout it was
    /// planned against, so its member gate can never be re-armed (M4).
    ///
    /// Failing open here is the one thing that must not happen: the seeded range
    /// stays, the member stays unverifiable, and the set is neither finalizable
    /// nor demotable while the completion gate re-reads it forever.
    RestartRearmUnplaceable,
    /// A restart-seeded run could not be **read back** from the partial that is
    /// supposed to hold it.
    ///
    /// Distinct from [`Self::DestinationWriteFailed`], which this used to borrow:
    /// nothing was being written, and a run that will not read is a partial that
    /// changed under a validated checkpoint — a different operational story and a
    /// different metric.
    RestartRereadFailed,
    /// A PAR2-repaired span could not be routed back into the set (D8/D3).
    ///
    /// The repair itself succeeded — the materialized volume is correct — but
    /// the router could not place its bytes: the layout maps part of the span
    /// to nothing, or a destination write for it failed. Demoting here is safe
    /// and cheap, because the repaired bytes that *were* routed are already in
    /// the partials and the composition was overwritten with them, so
    /// reconstruction rebuilds the repaired volume rather than the damaged one.
    RepairRerouteFailed,
    /// A stale composition gap left by a repair could not be re-read from the
    /// partial that holds it (D4).
    ///
    /// A gap is bytes nothing currently vouches for, so leaving the member
    /// "verified" over one would pass a member on the strength of a value that
    /// describes different bytes. Unreadable means unverifiable, and
    /// unverifiable demotes.
    RepairGapUnreadable,
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
    /// One of the set's destinations could not be marked sparse (D3). Raised
    /// **before** the file holds a hole, so demoting here is what keeps a
    /// Windows filesystem from allocating a whole volume's worth of zeros
    /// behind a member partial whose first routed byte lands near its end.
    SparseMarkFailed,
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
            Self::EncryptedMemberRefused(refusal) => refusal.metric(),
            Self::EncryptedFactsDisagree => "encrypted_facts_disagree",
            Self::EncryptedPar2Unsupported => "encrypted_par2_unsupported",
            Self::ToleranceBudgetExceeded => "tolerance_budget",
            Self::Par2Damaged => "par2_damaged",
            Self::Par2Unbindable => "par2_unbindable",
            Self::ToleratedExtractionFailed => "tolerated_extraction_failed",
            Self::HoldsBudgetExceeded => "holds_budget",
            Self::HoldsScratchFailed => "holds_scratch_io",
            Self::HoldsScratchCeiling => "holds_scratch_ceiling",
            Self::ConflictingVolumeFacts => "conflicting_volume_facts",
            Self::UnconfirmedRestoredVolume => "unconfirmed_restored_volume",
            Self::RestartRearmUnplaceable => "restart_rearm_unplaceable",
            Self::RestartRereadFailed => "restart_reread_failed",
            Self::RepairRerouteFailed => "repair_reroute_failed",
            Self::RepairGapUnreadable => "repair_gap_unreadable",
            Self::UnparsableVolume => "unparsable_volume",
            Self::PartChecksumMismatch => "part_checksum_mismatch",
            Self::MemberChecksumMismatch => "member_checksum_mismatch",
            Self::FormatMismatch => "format_mismatch",
            Self::UnsupportedFormat => "unsupported_format",
            Self::UnsafeDestination => "unsafe_destination",
            Self::CollidingDestinations => "colliding_destinations",
            Self::VolumeCrcMismatch => "volume_crc_mismatch",
            Self::DestinationWriteFailed => "destination_write_failed",
            Self::SparseMarkFailed => "sparse_mark_failed",
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

    /// Replaces every run overlapping `[start, start + len)` with a single run
    /// for the rewritten span, and returns the sub-ranges of the discarded runs
    /// that fall **outside** it — the stale gaps (plan 135, D3/D4).
    ///
    /// This is what a PAR2 repair needs and what [`Self::insert`] must never do.
    /// A duplicate article clips: its bytes are the same bytes, and advancing
    /// the composition twice would double-count them. A repaired span is the
    /// opposite — the bytes on disk **changed**, so the composed value has to
    /// change with them, or finalization demotes a job whose output is correct
    /// while the composition still carries the wire-damaged value.
    ///
    /// The gaps exist because the runs are article-shaped and a repair is
    /// slice-shaped: rewriting the middle of an article discards that article's
    /// value, and the bytes on either side of the rewrite are then covered by no
    /// run at all. They are not composed away and they are not assumed good —
    /// the caller re-reads them from the routed bytes on disk and feeds the
    /// value back, and a gap that cannot be read leaves the member unverifiable,
    /// which demotes rather than passes.
    pub(crate) fn overwrite(&mut self, start: u64, len: u64, crc: u32) -> Vec<(u64, u64)> {
        if len == 0 {
            return Vec::new();
        }
        let end = start.saturating_add(len);
        let mut gaps = Vec::new();
        self.runs.retain(|&(run_start, run_len, _)| {
            let run_end = run_start.saturating_add(run_len);
            if run_end <= start || run_start >= end {
                return true;
            }
            if run_start < start {
                gaps.push((run_start, start));
            }
            if run_end > end {
                gaps.push((end, run_end));
            }
            false
        });
        let position = self
            .runs
            .partition_point(|(run_start, _, _)| *run_start < start);
        self.runs.insert(position, (start, len, crc));
        gaps
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

/// One staged run, in RAM or paged out to the set's holds scratch (D2).
///
/// A scratch region is **write-once and append-only** for the life of the set,
/// so an offset handed out here is valid until the set closes — which is what
/// makes reading one back a single positioned read with no locking and no
/// re-validation. Nothing is ever reclaimed or compacted: the file's high-water
/// is bounded by the total bytes the set ever held, and phase 7 revisits that if
/// measurement says the bound is too loose in practice.
#[derive(Debug, Clone)]
enum StagedChunk {
    Memory(std::sync::Arc<[u8]>),
    Scratch { offset: u64, len: u64 },
}

impl StagedChunk {
    fn len(&self) -> u64 {
        match self {
            Self::Memory(bytes) => bytes.len() as u64,
            Self::Scratch { len, .. } => *len,
        }
    }

    /// RAM cost, which is what the holds budget bounds. A paged chunk is zero
    /// here and is counted against the scratch ceiling instead.
    fn resident_len(&self) -> u64 {
        match self {
            Self::Memory(bytes) => bytes.len() as u64,
            Self::Scratch { .. } => 0,
        }
    }

    /// The sub-chunk covering `[from, from + len)` of this chunk.
    fn slice_of(&self, from: u64, len: u64) -> Self {
        match self {
            Self::Memory(bytes) => Self::Memory(std::sync::Arc::from(
                &bytes[from as usize..(from + len) as usize],
            )),
            Self::Scratch { offset, .. } => Self::Scratch {
                offset: offset.saturating_add(from),
                len,
            },
        }
    }
}

/// Positioned read, so a shared handle needs no seek and no exclusive access.
fn read_at(file: &std::fs::File, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(out, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut written = 0usize;
        while written < out.len() {
            let read = file.seek_read(&mut out[written..], offset + written as u64)?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "holds scratch ended early",
                ));
            }
            written += read;
        }
        Ok(())
    }
}

fn write_at(file: &std::fs::File, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
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
            // A zero-byte `seek_write` is not an error and not progress, so
            // trusting the loop condition alone spins forever on a device that
            // reports it. `WriteZero` is what `write_all` raises for exactly this
            // and is what the caller already turns into a scratch failure.
            let progress = file.seek_write(&bytes[written..], offset + written as u64)?;
            if progress == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "positional write reported no progress",
                ));
            }
            written += progress;
        }
        Ok(())
    }
}

/// The per-set holds scratch file (plan 135, D2).
///
/// Append-only, write-once, with an in-memory index that lives in the staging
/// map: a paged chunk *is* its `(offset, len)`. There is no free list and no
/// compaction, deliberately — reclaiming space in a file whose regions are
/// handed out as stable offsets means either rewriting them (which breaks the
/// write-once property the lock-free read depends on) or a free-list allocator,
/// and neither is worth building before measurement says the append-only bound
/// hurts. The bound is stated rather than hidden: **scratch never exceeds the
/// total bytes the set holds over its life**, and the ceiling below is what
/// keeps that from being unbounded.
#[derive(Debug)]
pub(crate) struct HoldsScratch {
    path: std::path::PathBuf,
    file: Option<std::sync::Arc<std::fs::File>>,
    /// Append cursor, and the file's length.
    len: u64,
    ceiling: u64,
    /// D3's sparse marker. The scratch is append-only so it holds no hole of
    /// its own, but it is a direct-store-created file in the working directory
    /// and it inherits the same rule: marked at creation, before a byte is
    /// written, and a marking failure demotes rather than proceeding.
    sparse: SparseMarking,
}

impl HoldsScratch {
    pub(super) fn new(path: std::path::PathBuf, ceiling: u64) -> Self {
        Self {
            path,
            file: None,
            len: 0,
            ceiling,
            sparse: SparseMarking::default(),
        }
    }

    pub(super) fn bytes(&self) -> u64 {
        self.len
    }

    fn handle(&self) -> Option<std::sync::Arc<std::fs::File>> {
        self.file.clone()
    }

    /// Appends one run and returns its offset. `None` on a ceiling breach, which
    /// the caller turns into a demotion.
    pub(super) fn append(&mut self, bytes: &[u8]) -> Result<u64, DemotionReason> {
        let len = bytes.len() as u64;
        let end = self
            .len
            .checked_add(len)
            .ok_or(DemotionReason::HoldsScratchCeiling)?;
        if end > self.ceiling {
            return Err(DemotionReason::HoldsScratchCeiling);
        }
        if self.file.is_none() {
            // Marked sparse before the first `write_at` (D3). A killed run's
            // scratch is swept at restart, so an existing file here is not
            // state to preserve — truncating it is what keeps the append cursor
            // (`len`, reset to zero by `discard`) agreeing with the file.
            let file = super::sparse::create_sparse(&self.path, &self.sparse)
                .map_err(|_| DemotionReason::HoldsScratchFailed)?;
            file.set_len(0)
                .map_err(|_| DemotionReason::HoldsScratchFailed)?;
            self.file = Some(std::sync::Arc::new(file));
        }
        let file = self.file.as_ref().expect("just opened");
        write_at(file, self.len, bytes).map_err(|_| DemotionReason::HoldsScratchFailed)?;
        let offset = self.len;
        self.len = end;
        Ok(offset)
    }

    pub(super) fn read(&self, offset: u64, len: u64) -> Option<Vec<u8>> {
        let file = self.file.as_ref()?;
        let mut out = vec![0u8; len as usize];
        read_at(file, offset, &mut out).ok()?;
        Some(out)
    }

    /// Closes and deletes the file. Called at finalization and demotion, and by
    /// the restart sweep for a file no set claims.
    ///
    /// Keyed on whether the file was ever **opened**, not on whether anything was
    /// appended: `append` creates the file before its first `write_at`, so a
    /// first write that fails leaves a zero-length scratch on disk that a
    /// `len > 0` test would walk straight past — and the set is demoting, so
    /// nothing comes back for it until the next restart's sweep.
    pub(super) fn discard(&mut self) {
        let created = self.file.take().is_some() || self.len > 0;
        if created {
            let _ = std::fs::remove_file(&self.path);
        }
        self.len = 0;
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
    runs: Vec<(u64, StagedChunk)>,
    /// Read handle for the scratch-resident runs. Held as an `Arc<File>` so the
    /// image satisfies the parser's `'static` bound without reopening the file
    /// per parse, and read positionally so nothing here disturbs a concurrent
    /// append.
    scratch: Option<std::sync::Arc<std::fs::File>>,
    position: u64,
}

impl SparseImage {
    fn from_staged(
        chunks: &BTreeMap<u64, StagedChunk>,
        scratch: Option<std::sync::Arc<std::fs::File>>,
    ) -> Self {
        Self {
            runs: chunks
                .iter()
                .map(|(offset, chunk)| (*offset, chunk.clone()))
                .collect(),
            scratch,
            position: 0,
        }
    }

    /// Test constructor: a purely RAM-resident image.
    #[cfg(test)]
    pub(super) fn from_chunks(chunks: &BTreeMap<u64, std::sync::Arc<[u8]>>) -> Self {
        Self {
            runs: chunks
                .iter()
                .map(|(offset, bytes)| (*offset, StagedChunk::Memory(std::sync::Arc::clone(bytes))))
                .collect(),
            scratch: None,
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
        let (start, chunk) = &self.runs[index];
        let inside = position - start;
        if inside >= chunk.len() {
            return Ok(0);
        }
        let taken = (chunk.len() - inside).min(out.len() as u64) as usize;
        match chunk {
            StagedChunk::Memory(bytes) => {
                out[..taken].copy_from_slice(&bytes[inside as usize..inside as usize + taken]);
            }
            // Paged out: read back exactly what the walk asked for, which for a
            // header walk is a header at a time, never the data area it seeks
            // over. A missing handle answers `Ok(0)`, the same clean stop a hole
            // produces — a parse that cannot see a byte must never see a
            // fabricated one.
            StagedChunk::Scratch { offset, .. } => {
                let Some(file) = self.scratch.as_ref() else {
                    return Ok(0);
                };
                if read_at(file, offset.saturating_add(inside), &mut out[..taken]).is_err() {
                    return Ok(0);
                }
            }
        }
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

/// One staged run of a repaired span, in exactly the shape [`VolumeStaging`]
/// holds it: a physical offset and reference-counted bytes.
///
/// Reference-counted rather than borrowed so the reader that streams a repaired
/// volume off its scratch file can hand each bounded chunk **straight** into
/// staging. The first shape read every rewrite span whole into an owned `Vec`
/// and let `stage_repaired` copy it, so a repair peaked at twice the repaired
/// bytes with nothing bounding either term (phase 6 review, F3).
pub(crate) type RepairedChunk = (u64, std::sync::Arc<[u8]>);

/// Per-volume staging: the bytes the router still needs, and what it has
/// already placed.
#[derive(Debug, Default)]
pub(super) struct VolumeStaging {
    /// Non-overlapping byte runs, keyed by physical offset. Shared rather than
    /// owned so the header parser's image is a pointer copy (M2), and each one
    /// either RAM-resident or paged out to the set's holds scratch (D2).
    chunks: BTreeMap<u64, StagedChunk>,
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
    /// The volume's coverage was seeded from a checkpoint rather than from
    /// articles this run decoded.
    ///
    /// Load-bearing for confirmation, not just bookkeeping: a restored volume's
    /// bytes are on **disk**, not in `chunks`, so the confirming parse
    /// [`DirectSetRouter::try_parse_volume`] runs has a hole from zero and
    /// cannot succeed however many further articles arrive. Confirmation has to
    /// be decided at restore ([`restored_volume_is_confirmed`]) or not at all,
    /// and a volume that reaches completion still unconfirmed demotes rather
    /// than holding its trailing region for the life of the set.
    restored: bool,
    /// Physical ranges force-staged by a PAR2 repair (plan 135, D3/D8).
    ///
    /// The **repair marker**. A repaired span re-enters the router over bytes
    /// the volume already routed, so without a mark the drain cannot tell it
    /// from a duplicate article — and the two must behave in opposite ways: a
    /// duplicate clips (the same bytes, composed once), a repair overwrites (new
    /// bytes, so the composed value has to move with them). Marking the *range*
    /// rather than the router is what keeps a genuine duplicate arriving in the
    /// same drain on the clipping path.
    ///
    /// Cleared as the range drains, so the mark lives exactly as long as the
    /// bytes it describes.
    repaired: ByteRanges,
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

/// Whether a volume seeded from a checkpoint may be treated as **confirmed** —
/// its header walk finished, so no further header can appear in its trailing
/// region and the drain may route those bytes into the envelope.
///
/// This is decided here or not at all. A restored volume's pre-restart bytes are
/// on disk, not in [`VolumeStaging::chunks`], so the image
/// [`DirectSetRouter::try_parse_volume`] walks has a hole from offset zero: the
/// confirming parse fails, silently and identically, however many further
/// articles arrive. Getting it wrong in the permissive direction files a
/// member's payload into an envelope that finalization deletes; getting it wrong
/// in the strict direction holds the volume's end record forever, which reads as
/// PAR2 damage and costs a full redownload. So: two proofs, and only two.
///
/// - **The cached facts reached the end record.** `more_volumes` can only be true
///   when the library parsed an end-of-archive header, which is the last header a
///   volume can carry. (It is one-directional: the *last* volume of a set has no
///   `more_volumes` flag to raise, so this proof is silent about it — that is what
///   the second one is for.)
/// - **The coverage is contiguous from zero to the volume's whole decoded
///   length.** The previous run therefore held a byte-contiguously complete image
///   of the volume when it parsed, which is exactly the `source_complete` proof
///   the live path uses. `decoded_len` is `Some` only when the checkpoint calls
///   the volume complete, and the contiguity is re-checked rather than assumed:
///   the bit and the bytes are separate claims and this is the seam that can
///   still compare them.
///
/// Chain closure (`!split_after`) is deliberately **not** a third proof, for the
/// same reason it is not one in the live parse: a truncated prefix closes the
/// chain the moment the first member's final part is read, while a second
/// member's header sits unread past that member's data area.
pub(super) fn restored_volume_is_confirmed(
    covered: &ByteRanges,
    decoded_len: Option<u64>,
    more_volumes: bool,
) -> bool {
    if more_volumes {
        return true;
    }
    decoded_len.is_some_and(|len| covered.contiguous_from_zero() >= len)
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
                self.chunks.insert(
                    start,
                    StagedChunk::Memory(std::sync::Arc::from(&data[from..to])),
                );
                self.pending.insert(start, end - start);
                staged = staged.saturating_add(end - start);
            }
        }
        staged
    }

    /// Stores `[offset, offset + data.len())` **unconditionally**, replacing
    /// whatever was staged there and re-opening it for routing (D8).
    ///
    /// [`Self::stage`] deliberately refuses a range that is already routed —
    /// that is the duplicate-article rule. A repaired span is the one case
    /// where the same physical range must be routed twice, because the bytes
    /// changed, so it goes through here instead and is marked in
    /// [`Self::repaired`] for the drain.
    fn stage_repaired(&mut self, offset: u64, data: std::sync::Arc<[u8]>) {
        self.force_stage(offset, data, true);
    }

    /// [`Self::stage_repaired`] without the repair mark: the same force-stage of
    /// an already-routed range, marked as an ordinary duplicate.
    ///
    /// Test-only, and only because the shape it builds is otherwise unreachable
    /// by construction from outside: a duplicate never re-stages a routed range
    /// (that is what [`Self::stage`] refuses), so the one thing that can put
    /// unrepaired bytes next to repaired ones in a single drained run is state
    /// the router reached earlier and could not place. See
    /// `a_drain_run_straddling_repaired_and_duplicate_bytes_splits_at_the_boundary`.
    #[cfg(test)]
    fn stage_duplicate(&mut self, offset: u64, data: std::sync::Arc<[u8]>) {
        self.force_stage(offset, data, false);
    }

    fn force_stage(&mut self, offset: u64, data: std::sync::Arc<[u8]>, repaired: bool) {
        let len = data.len() as u64;
        if len == 0 {
            return;
        }
        let end = offset.saturating_add(len);
        // Chunks are keyed by start offset and never overlap, so a rewritten
        // range can only touch chunks starting below `end`, and the one it
        // starts inside is the last starting at or before `offset`.
        let touched: Vec<u64> = self
            .chunks
            .range(..end)
            .filter(|(start, chunk)| start.saturating_add(chunk.len()) > offset)
            .map(|(start, _)| *start)
            .collect();
        for start in touched {
            let Some(chunk) = self.chunks.remove(&start) else {
                continue;
            };
            let chunk_end = start.saturating_add(chunk.len());
            if start < offset {
                self.chunks.insert(start, chunk.slice_of(0, offset - start));
            }
            if chunk_end > end {
                self.chunks
                    .insert(end, chunk.slice_of(end - start, chunk_end - end));
            }
        }
        self.chunks.insert(offset, StagedChunk::Memory(data));
        self.routed = subtract(&self.routed, offset, len);
        self.pending.insert(offset, len);
        if repaired {
            self.repaired.insert(offset, len);
        }
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
        self.chunks
            .values()
            .fold(0u64, |total, chunk| total.saturating_add(chunk.len()))
    }

    /// The RAM half of [`Self::staged_bytes`] — what the holds budget bounds.
    /// A paged chunk still costs a scratch region, which the ceiling bounds
    /// separately (D2).
    fn resident_bytes(&self) -> u64 {
        self.chunks.values().fold(0u64, |total, chunk| {
            total.saturating_add(chunk.resident_len())
        })
    }

    /// Copies `[offset, offset + len)` out of the staged chunks. `None` when
    /// the range is not wholly staged, which the drain never asks for.
    ///
    /// A paged chunk costs **one positioned read** per drained run, which is the
    /// whole reason scratch regions are write-once: the offset was handed out
    /// when the bytes were paged and nothing can have moved them since.
    fn slice(&self, offset: u64, len: u64, scratch: &HoldsScratch) -> Option<Vec<u8>> {
        let mut out = Vec::with_capacity(len as usize);
        let mut cursor = offset;
        let end = offset.saturating_add(len);
        while cursor < end {
            let (start, chunk) = self
                .chunks
                .range(..=cursor)
                .next_back()
                .map(|(start, chunk)| (*start, chunk.clone()))?;
            let inside = cursor - start;
            if inside >= chunk.len() {
                return None;
            }
            let take = (chunk.len() - inside).min(end - cursor);
            match &chunk {
                StagedChunk::Memory(bytes) => {
                    out.extend_from_slice(&bytes[inside as usize..(inside + take) as usize]);
                }
                StagedChunk::Scratch {
                    offset: scratch_offset,
                    ..
                } => {
                    let bytes = scratch.read(scratch_offset.saturating_add(inside), take)?;
                    out.extend_from_slice(&bytes);
                }
            }
            cursor = cursor.saturating_add(take);
        }
        Some(out)
    }

    /// Drops staged bytes that are routed and belong to a member, keeping the
    /// envelope (the header parser reads through it) and the holds.
    fn trim(&mut self, keep: &ByteRanges) {
        let offsets: Vec<u64> = self.chunks.keys().copied().collect();
        for offset in offsets {
            let Some(chunk) = self.chunks.get(&offset) else {
                continue;
            };
            let len = chunk.len();
            let retained = intersect(keep, offset, len);
            if retained.len() == 1 && retained[0] == (offset, offset + len) {
                continue;
            }
            let chunk = self.chunks.remove(&offset).expect("chunk was just read");
            for (start, end) in retained {
                self.chunks
                    .insert(start, chunk.slice_of(start - offset, end - start));
            }
        }
    }

    /// Whether `[offset, offset + len)` was force-staged by a repair, so the
    /// drain must overwrite the composition rather than clip it.
    fn is_repaired(&self, offset: u64, len: u64) -> bool {
        len > 0 && self.repaired.missing(offset, len).is_empty()
    }

    /// Splits `[start, end)` at every [`Self::repaired`] boundary inside it, so
    /// each sub-range is **wholly** repaired or wholly not.
    ///
    /// The drain's `replace` flag is all-or-nothing per emitted run, and the two
    /// things that decide a run's extent decide it for unrelated reasons:
    /// `map_physical_range` splits at member and envelope boundaries, and
    /// `pending` coalesces every staged range that abuts another. A repair
    /// therefore routinely produces one member run covering repaired *and*
    /// unrepaired bytes, and that run took `replace = false` — so
    /// `CrcRuns::insert` refused it as overlapping, the wire-damaged value
    /// survived the repair, and the member failed its gate on bytes that are
    /// correct on disk. Splitting here first is what makes the flag exact
    /// (phase 6 review, F4).
    ///
    /// A volume with no repair in flight — every volume, nearly always — returns
    /// the range unchanged and costs one `is_empty` check.
    fn repair_partition(&self, start: u64, end: u64) -> Vec<(u64, u64)> {
        if end <= start {
            return Vec::new();
        }
        if self.repaired.is_empty() {
            return vec![(start, end)];
        }
        let len = end - start;
        let mut split = Vec::new();
        let mut cursor = start;
        for (gap_start, gap_end) in self.repaired.missing(start, len) {
            if gap_start > cursor {
                split.push((cursor, gap_start));
            }
            split.push((gap_start, gap_end));
            cursor = gap_end;
        }
        if cursor < end {
            split.push((cursor, end));
        }
        split
    }

    /// RAM-resident chunks, largest first, for the pager to choose from.
    fn resident_chunks(&self) -> Vec<(u64, u64)> {
        let mut chunks: Vec<(u64, u64)> = self
            .chunks
            .iter()
            .filter_map(|(offset, chunk)| match chunk {
                StagedChunk::Memory(bytes) => Some((*offset, bytes.len() as u64)),
                StagedChunk::Scratch { .. } => None,
            })
            .collect();
        chunks.sort_unstable_by_key(|chunk| std::cmp::Reverse(chunk.1));
        chunks
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
    /// Logical ranges this run did **not** write: they were claimed by a
    /// checkpoint a previous run committed, and restart seeded them into
    /// [`Self::covered`] so they are not refetched (D6).
    ///
    /// `CrcRuns` never survives a restart, so these bytes are covered and
    /// **unverified** — the gates stay disarmed over them until the bytes are
    /// re-read from disk. Non-empty is therefore a hard refusal in
    /// [`DirectSetRouter::try_verify_member`], not merely an absence of runs:
    /// composing around a seeded range would pass a member on the strength of
    /// what a previous process claimed to have written rather than on what is on
    /// disk now, which is exactly the assurance D6 refuses to trade away.
    restart_seeded: ByteRanges,
    /// Logical ranges whose composed value a **repair** discarded (D4).
    ///
    /// A repaired span is slice-shaped and the runs are article-shaped, so
    /// [`CrcRuns::overwrite`] drops the articles it straddles and the bytes on
    /// either side of the rewrite are left composed by nothing. They are still
    /// covered and still correct — nobody wrote over them — but no value in this
    /// process describes them, which is the same position restart-seeded
    /// coverage is in and gets the same treatment: a hard refusal in
    /// [`DirectSetRouter::try_verify_member`] until they are re-read from the
    /// partial and their value fed back.
    stale_gaps: ByteRanges,
    /// The whole-member gate has passed.
    verified: bool,
    /// Present exactly for a [`MemberEligibility::EncryptedStore`] member the
    /// set admitted (plan 136). Its presence is what makes the drain decrypt at
    /// write time, and its absence is what makes an encrypted member's bytes
    /// unroutable — the two can never disagree, because the member is only
    /// adopted at all once admission has returned keys.
    crypt: Option<MemberCrypt>,
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
    /// Volumes whose facts this run has accepted and the caller has not yet
    /// cached (D6's restart input).
    ///
    /// A direct set never writes a volume file, so `try_update_archive_topology`
    /// — the only thing that normally fills `active_rar_volume_facts` — has
    /// nothing to parse and D7 suppresses it anyway. The router's own parse is
    /// therefore the **only** producer of these facts, and without caching them
    /// a restart has no way to rebuild the layout: the header bytes sit below the
    /// published floors and are never refetched.
    dirty_facts: std::collections::BTreeSet<u32>,
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
    /// D2's paging destination. Opened on the first breach and never before, so
    /// a set that stays inside its RAM budget — which is nearly all of them —
    /// touches the filesystem for it exactly zero times.
    scratch: HoldsScratch,
    /// The set's password and the keys derived from it (plan 136, E-D1). Empty
    /// and untouched for a set with no encrypted member — which is every set
    /// plan 135 routed.
    crypt: KeyRing,
    /// How many times the drain has held a cipher block because the other half
    /// of it had not arrived (E-D2). The production account of this is the
    /// `direct_store.encrypted.block_held` probe; this is the same fact in a
    /// form a test can assert on, because byte-identical output cannot tell a
    /// set that held from one that never had to (E1 review F11).
    #[cfg(test)]
    blocks_held: u64,
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
            scratch: HoldsScratch::new(plan.holds_scratch_path(), HOLDS_SCRATCH_CEILING_BYTES),
            plan,
            layout: None,
            volume_facts: BTreeMap::new(),
            dirty_facts: std::collections::BTreeSet::new(),
            staging: BTreeMap::new(),
            members: BTreeMap::new(),
            member_ids: HashMap::new(),
            next_member_id: 0,
            routed_extents: BTreeMap::new(),
            member_order: Vec::new(),
            member_order_stale: false,
            holds_budget: DEFAULT_HOLDS_BUDGET_BYTES,
            crypt: KeyRing::new(),
            #[cfg(test)]
            blocks_held: 0,
            demoted: None,
        }
    }

    /// Cipher blocks the drain has held for a missing predecessor or a missing
    /// other half (E-D2). Test-only; see the field.
    #[cfg(test)]
    pub(crate) fn blocks_held(&self) -> u64 {
        self.blocks_held
    }

    /// Whether the set would still take a job password (plan 136, E-D1).
    ///
    /// The seam re-reads the live job spec while this is true, because a
    /// password can arrive **after** the job was added: `setJobPassword` and the
    /// NZBGet facade's `*Unpack:Password` both mutate the spec in place. It goes
    /// false once a password is *admitted*, or once the set leaves direct mode.
    ///
    /// # The window is pre-first-article, and only that (E1 review F5)
    ///
    /// Admission runs from the first successful header parse, which is the first
    /// article of the set's first volume — seconds into the download. A password
    /// that arrives after it does **not** revive the set: the `NoPassword`
    /// refusal is a demotion, `demoted` is then `Some`, and this goes false for
    /// good. That is deliberate. Waiting instead would mean holding every
    /// arriving byte against the holds budget for a set that will most likely
    /// never get a password, and then demoting on a scratch-ceiling breach
    /// having downloaded and thrown away everything up to it — the conventional
    /// path takes the set immediately and asks the job's whole candidate list,
    /// which is a superset of the single password direct-store sees.
    ///
    /// What this window *does* cover is a password **corrected** before the
    /// first parse, which is why it is `!admitted` rather than "no password
    /// held": see [`crypt::KeyRing::wants_password`].
    pub(crate) fn wants_password(&self) -> bool {
        self.demoted.is_none() && self.crypt.wants_password()
    }

    /// Binds the job's password. Never persisted, never logged.
    pub(crate) fn set_password(&mut self, password: Option<&str>) {
        self.crypt.set_password(password);
    }

    /// Refuses encrypted admission outright because the job declares a PAR2 file
    /// (plan 136, E1 review F1). Set once, when the set is built.
    pub(crate) fn refuse_encrypted_for_par2(&mut self) {
        self.crypt.refuse_encrypted_for_par2();
    }

    /// The key ring's own `Debug`, for the test that proves a password cannot
    /// reach a log through it. The router's `Debug` does not print the ring at
    /// all, so this is the only way to assert on the type that holds the
    /// password.
    #[cfg(test)]
    pub(crate) fn crypt_debug(&self) -> String {
        format!("{:?}", self.crypt)
    }

    /// Whether this set has admitted an encrypted member, i.e. whether any of
    /// its bytes are being decrypted on the way to their destination.
    ///
    /// Read by every consumer of **posted** bytes, because for such a set the
    /// destinations hold plaintext and the source volumes held cipher: until
    /// phase E2's re-encrypting overlay exists, those consumers must refuse
    /// rather than read plaintext where cipher belongs.
    ///
    /// The three that do, and why each one is a refusal rather than a fallback:
    ///
    /// - demotion-by-reconstruction, which would rebuild a source volume out of
    ///   decrypted partials (`reconstruct_demoted_set`);
    /// - the authoritative PAR2 overlay, which is what
    ///   `demote_unbindable_direct_sets`' encrypted arm keeps a set out of;
    /// - **live PAR2** (E1 review F3). Its in-stream feed is honest — those are
    ///   the posted bytes, taken before routing — but every block straddling an
    ///   article boundary is settled by a *read-back*, and the read-back goes
    ///   through `direct_virtual_volume` to the member's `.direct.partial`,
    ///   which is plaintext. So an encrypted set is kept out of live
    ///   verification entirely rather than fed a stream it can only ever settle
    ///   into `Bad` blocks.
    ///
    /// One residual, bounded and stated rather than left to be discovered: this
    /// only goes true at the first header parse, so an article that arrives
    /// *before* it — the payload-before-header case — is fed. Those bytes are
    /// correct posted cipher, and the blocks they cover simply never settle:
    /// the read-back that would finish them finds no virtual volume and no file,
    /// so they stay `Pending` for the authoritative pass rather than turning
    /// `Bad`. The cost is a few recorded ranges against the live partial budget,
    /// never a wrong verdict.
    pub(crate) fn routes_encrypted(&self) -> bool {
        self.crypt.admitted()
    }

    /// Sets the per-set scratch ceiling (D2). Configured, with the env override
    /// winning; the tests lower it so a breach is reachable without paging
    /// gigabytes.
    pub(crate) fn set_holds_scratch_ceiling(&mut self, bytes: u64) {
        self.scratch.ceiling = bytes;
    }

    /// Sets the sparse marker every file this set creates goes through (D3).
    pub(crate) fn set_sparse_marking(&mut self, marking: SparseMarking) {
        self.scratch.sparse = marking;
    }

    /// Bytes currently paged out to the set's holds scratch (D2), counted
    /// separately from RAM so the two ceilings stay legible in metrics.
    pub(crate) fn scratch_bytes(&self) -> u64 {
        self.scratch.bytes()
    }

    /// Closes and deletes the scratch file. Idempotent; called at finalization
    /// and demotion.
    pub(crate) fn discard_scratch(&mut self) {
        self.scratch.discard();
    }

    /// Pages RAM-resident staged runs out to scratch until the holds budget is
    /// satisfied (D2).
    ///
    /// Largest chunks first, across every volume: the goal is to get back under
    /// the ceiling in as few writes as possible, and a big run is exactly the
    /// recovery record or held payload the budget exists to bound. Whether a
    /// chunk is a hold or a retained envelope run does not matter — both are
    /// read back the same way, one positioned read per drained slice, and the
    /// header walk seeks over data areas rather than reading them.
    fn page_holds_to_scratch(&mut self) -> Result<(), DemotionReason> {
        let mut candidates: Vec<(u64, u32, u64)> = Vec::new();
        for (volume_index, staging) in &self.staging {
            for (offset, len) in staging.resident_chunks() {
                candidates.push((len, *volume_index, offset));
            }
        }
        candidates.sort_unstable_by_key(|candidate| std::cmp::Reverse(candidate.0));

        for (_, volume_index, offset) in candidates {
            if self.resident_bytes() <= self.holds_budget {
                return Ok(());
            }
            let bytes = match self
                .staging
                .get(&volume_index)
                .and_then(|staging| staging.chunks.get(&offset))
            {
                Some(StagedChunk::Memory(bytes)) => std::sync::Arc::clone(bytes),
                _ => continue,
            };
            let scratch_offset = self.scratch.append(&bytes)?;
            if let Some(staging) = self.staging.get_mut(&volume_index) {
                staging.chunks.insert(
                    offset,
                    StagedChunk::Scratch {
                        offset: scratch_offset,
                        len: bytes.len() as u64,
                    },
                );
            }
        }
        if self.resident_bytes() > self.holds_budget {
            // Everything pageable is paged and RAM is still over: the budget is
            // smaller than one staged run, which is a configuration the set
            // cannot route inside.
            return Err(DemotionReason::HoldsBudgetExceeded);
        }
        Ok(())
    }

    /// RAM-resident staged bytes across the set — what the holds budget bounds.
    fn resident_bytes(&self) -> u64 {
        self.staging.values().fold(0u64, |total, staging| {
            total.saturating_add(staging.resident_bytes())
        })
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

    /// The RAM ceiling this set's holds are bounded by (D2). Read by the repair
    /// seam, which must size its rewrite against it *before* reading a byte back
    /// — every repaired byte re-enters the router as a hold.
    pub(crate) fn holds_budget(&self) -> u64 {
        self.holds_budget
    }

    /// Test hook: force-stage a range without draining it, so a test can build
    /// the one drain shape ordinary routing reaches only through history — bytes
    /// the router staged and could not place, sitting next to a repaired range.
    /// `repaired` picks which of the two force-stage rules applies.
    #[cfg(test)]
    pub(crate) fn force_stage_for_test(
        &mut self,
        volume_index: u32,
        offset: u64,
        data: &[u8],
        repaired: bool,
    ) {
        let staging = self.staging.entry(volume_index).or_default();
        let bytes: std::sync::Arc<[u8]> = std::sync::Arc::from(data);
        if repaired {
            staging.stage_repaired(offset, bytes);
        } else {
            staging.stage_duplicate(offset, bytes);
        }
    }

    /// Test hook: stage a range the ordinary way, so a fixture can seed a
    /// volume's first, undamaged pass without a parseable RAR image.
    #[cfg(test)]
    pub(crate) fn stage_for_test(&mut self, volume_index: u32, offset: u64, data: &[u8]) {
        self.staging
            .entry(volume_index)
            .or_default()
            .stage(offset, data);
    }

    /// Test hook: one drain, with no parse in front of it.
    #[cfg(test)]
    pub(crate) fn drain_for_test(
        &mut self,
        volume_index: u32,
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        self.drain_volume(volume_index)
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

    /// Total bytes the set is currently holding, RAM and scratch together: the
    /// holds proper, plus the envelope-classified bytes retained for the header
    /// walk (M1).
    ///
    /// The *RAM* half is [`Self::resident_bytes`], which is what the holds
    /// budget bounds and what a breach pages down; the paged half is
    /// [`Self::scratch_bytes`]. Both terms have to be counted somewhere — a
    /// `-rr` volume's recovery record is envelope-classified and is a percentage
    /// of the volume, per volume — and the point of paging is to move that term
    /// from the one ceiling to the other rather than to demote the set.
    #[cfg(test)]
    pub(crate) fn staged_bytes(&self) -> u64 {
        self.staging.values().fold(0u64, |total, staging| {
            total.saturating_add(staging.staged_bytes())
        })
    }

    /// RAM-resident staged bytes across the set. Exposed for the tests that
    /// assert the budget actually bounds RSS rather than bookkeeping.
    #[cfg(test)]
    pub(crate) fn resident_staged_bytes(&self) -> u64 {
        self.resident_bytes()
    }

    /// Bytes the set is holding that are neither RAM-resident nor paged — which
    /// must always be zero, since every staged chunk is one or the other.
    #[cfg(test)]
    pub(crate) fn unaccounted_staged_bytes(&self) -> u64 {
        self.staged_bytes()
            .saturating_sub(self.resident_bytes())
            .saturating_sub(self.scratch_bytes())
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

        // D2: a breach pages rather than demoting. Demotion is what is left when
        // paging itself fails — a scratch I/O error, or the ceiling.
        if self.resident_bytes() > self.holds_budget
            && let Err(reason) = self.page_holds_to_scratch()
        {
            return Err(self.fail(reason));
        }
        Ok(spans)
    }

    /// Re-enters the router with a span a PAR2 repair rebuilt (plan 135, D3/D8).
    ///
    /// A repaired span is late-arriving article data with one difference that
    /// changes everything downstream: the bytes it carries are **not** the bytes
    /// already on disk for that range. So it takes the same path as an article —
    /// stage, parse, drain, one span per intersecting destination — through
    /// [`VolumeStaging::stage_repaired`], which force-stages the range and marks
    /// it so the drain overwrites the composition instead of clipping it as a
    /// duplicate.
    ///
    /// Two jobs, and the second is easy to overlook. The obvious one is the
    /// bytes: destination writes must land at the mapped offsets, or the member
    /// on disk stays damaged. The other is the **parse**: the lost articles that
    /// made the volume damaged may also have carried the header the walk stopped
    /// at, so feeding the repaired bytes back is what lets the walk resume — a
    /// repaired tail holding the end-of-archive record confirms a volume that
    /// could not otherwise be confirmed, and the set finishes instead of
    /// demoting.
    ///
    /// The returned spans must all be written before the caller records them,
    /// exactly as for [`Self::route`].
    /// Takes **all** of one volume's repaired spans at once, deliberately.
    /// Staging them one at a time would let the classification frontier hold an
    /// early span — its bytes sit at or past the header walk's tail, so they
    /// could still be an undiscovered member's payload — until a *later* span
    /// carrying the end record confirmed the volume. That is a real ordering,
    /// not a hypothetical: the article a set loses is often the last one, and it
    /// carries both a member's tail and the record that closes the archive.
    pub(crate) fn route_repaired(
        &mut self,
        volume_index: u32,
        chunks: &[RepairedChunk],
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        if let Some(reason) = self.demoted {
            return Err(reason);
        }
        if !self.plan.volumes.contains_key(&volume_index) {
            return Err(self.fail(DemotionReason::ConflictingVolumeFacts));
        }
        let staging = self.staging.entry(volume_index).or_default();
        let mut staged = false;
        for (source_offset, data) in chunks {
            if data.is_empty() {
                continue;
            }
            staging.stage_repaired(*source_offset, std::sync::Arc::clone(data));
            staged = true;
        }
        if !staged {
            return Ok(Vec::new());
        }

        self.try_parse_volume(volume_index)?;
        let volumes: Vec<u32> = self.staging.keys().copied().collect();
        let mut spans = Vec::new();
        for volume in volumes {
            spans.extend(self.drain_volume(volume)?);
        }

        // Every repaired byte must have found a destination. Unlike an ordinary
        // article — whose bytes may legitimately be held above the
        // classification frontier until a later header proves what they are —
        // a repair runs after the volume has finished downloading and been
        // parsed, so a byte with nowhere to go means the layout in front of us
        // cannot place bytes it previously placed. That is a demotion, not a
        // hold: leaving it staged would sit on a repaired byte the member is
        // waiting for, forever.
        if self
            .staging
            .get(&volume_index)
            .is_some_and(|staging| !staging.repaired.is_empty())
        {
            return Err(self.fail(DemotionReason::RepairRerouteFailed));
        }

        if self.resident_bytes() > self.holds_budget
            && let Err(reason) = self.page_holds_to_scratch()
        {
            return Err(self.fail(reason));
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
        // B2. A restored volume the parse above could not confirm will never be
        // confirmed *by a parse*: its pre-restart bytes are on disk rather than
        // in `chunks`, so the image the parser walks has a hole from offset zero
        // and every later attempt fails the same silent way.
        //
        // Leaving it unconfirmed is the failure that hides. The trailing region
        // — the end-of-archive record, and a `-rr` set's whole recovery record —
        // stays held for the life of the set, so the envelope never receives it,
        // so the virtual volume reads short, so PAR2 calls a byte-perfect set
        // damaged and the job pays a full redownload. Either the format proves
        // the region holds no undiscovered member, or the set demotes here:
        // named, counted, and while its routed bytes can still materialize the
        // volumes.
        if self
            .staging
            .get(&volume_index)
            .is_some_and(|staging| staging.restored && !staging.confirmed)
        {
            if !self.restored_volume_completes_confirmed(volume_index) {
                return Err(self.fail(DemotionReason::UnconfirmedRestoredVolume));
            }
            if let Some(staging) = self.staging.get_mut(&volume_index) {
                staging.confirmed = true;
            }
        }
        self.check_eligibility()?;
        let volumes: Vec<u32> = self.staging.keys().copied().collect();
        let mut spans = Vec::new();
        for volume in volumes {
            spans.extend(self.drain_volume(volume)?);
        }
        Ok(spans)
    }

    /// Whether a **restored** volume that has just finished downloading may be
    /// confirmed without the parse it can no longer run (B2).
    ///
    /// Two conditions, both necessary:
    ///
    /// 1. **Every byte of the volume is accounted for, with no gap** — the
    ///    checkpoint's restored ranges and this run's staged holds together form
    ///    one run from offset zero. The caller has already established that no
    ///    further article is coming, so a single run from zero *is* coverage to
    ///    the volume's decoded length; it is expressed as contiguity rather than
    ///    compared against a number because the assembly's `received_bytes` for a
    ///    restored volume is the spec's yEnc-**encoded** total, ~3% too large.
    ///    This is the same fact `source_complete` states in the live path:
    ///    nothing of this volume is still outstanding.
    /// 2. **The volume's last known member continues into the next volume**
    ///    (`split_after`). A split member is by construction the last *file* in
    ///    its volume — that is what splitting means: the volume filled up — so the
    ///    unproven region above `tail_base` can only hold service data (a `-rr`
    ///    recovery record) and the end-of-archive record, which are envelope
    ///    content by definition. No undiscovered member can live there, which is
    ///    the one thing the confirming parse was there to rule out.
    ///
    /// Condition 2 is what keeps this honest, and why the volume that *closes* a
    /// chain — the last of a set, or one whose member ends inside it — is not
    /// confirmed this way: a second member's header can sit past the first's data
    /// area, which is exactly the shape `payload_past_the_last_known_header…`
    /// pins. Those demote instead.
    fn restored_volume_completes_confirmed(&self, volume_index: u32) -> bool {
        let Some(staging) = self.staging.get(&volume_index) else {
            return false;
        };
        let mut held = ByteRanges::new();
        for &(start, end) in staging.routed.ranges() {
            held.insert(start, end - start);
        }
        for &(start, end) in staging.pending.ranges() {
            held.insert(start, end - start);
        }
        if !matches!(held.ranges(), [(0, _)]) {
            return false;
        }
        self.volume_facts
            .get(&volume_index)
            .and_then(|facts| facts.members.last())
            .is_some_and(|member| member.split_after)
    }

    fn fail(&mut self, reason: DemotionReason) -> DemotionReason {
        self.demoted.get_or_insert(reason);
        reason
    }

    /// Parses the volume's headers out of its staged image, provisionally the
    /// first time and confirmingly once the walk reaches the archive end.
    ///
    /// # Quick Open is dropped, not implemented (plan 135, D2 — phase 7)
    ///
    /// D2 allowed RAR5 Quick Open records to *prime* the layout, under one hard
    /// condition: no byte routes on QO evidence alone, so the corresponding
    /// physical header must be parsed and confirmed identical first. It also
    /// said, in the same breath, that if the confirmation erases the benefit the
    /// feature should be deleted rather than weakened. It does, and it is:
    ///
    /// - **The fetch saving QO exists for is already banked.** QO's purpose is
    ///   avoiding a seek-and-read walk across a large archive. This router never
    ///   walks an archive: each volume's mapping comes from *that volume's own*
    ///   headers, parsed out of the prefix its first article delivers during
    ///   ordinary download. There is no extra fetch for QO to save, because
    ///   there is no extra fetch.
    /// - **Confirmation would cost strictly more than it saves.** The physical
    ///   headers must be parsed anyway to admit the volume; priming from QO
    ///   first would add a second parse and a field-by-field comparison to reach
    ///   the same mapping.
    /// - **QO records live at the end of the archive**, past every member's
    ///   payload, so on a set that is still downloading they are the *last*
    ///   thing to arrive. Priming from them would resolve mappings after the
    ///   bytes they describe, which is the wrong end of the job.
    ///
    /// So there is no QO code here and none is wanted. What there *is* — and
    /// this is the part a future reader must not mistake for QO being absent —
    /// is the library's own preference: `parse_volume_facts` calls
    /// `parse_all_headers`, which on seeing a main header carrying a locator
    /// Quick Open offset tries the QO records first and returns **those**
    /// headers when they parse cleanly through an end-of-archive record. On a
    /// truncated prefix that read hits a hole and falls back to the physical
    /// walk, so a provisional parse is always physical; a *confirming* parse of
    /// a fully staged `-qo` volume can be QO-derived.
    ///
    /// Two of the three outcomes are already safe here: QO agreeing with the
    /// physical parse changes nothing, and QO disagreeing is
    /// [`DemotionReason::ConflictingVolumeFacts`]. The third — a forged QO
    /// record *appending* a member the physical walk never saw — would be
    /// adopted by the `members_extend` arm below on QO evidence alone, which is
    /// exactly what D2 forbids and what the RAR spec warns is craftable. Closing
    /// it needs the library to be able to parse with QO suppressed
    /// (`weaver-unrar` is not this crate's to change), so it is recorded here
    /// and carried as a follow-up rather than papered over with a heuristic.
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

        let image = SparseImage::from_staged(&staging.chunks, self.scratch.handle());
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
                let changed = *previous != facts;
                self.volume_facts.insert(volume_index, facts.clone());
                if changed {
                    self.dirty_facts.insert(volume_index);
                }
            }
            Some(previous) if members_extend(previous, &facts) => {
                self.volume_facts.insert(volume_index, facts.clone());
                self.dirty_facts.insert(volume_index);
                self.rebuild_layout()?;
            }
            Some(_) => return Err(self.fail(DemotionReason::ConflictingVolumeFacts)),
            None => {
                self.volume_facts.insert(volume_index, facts.clone());
                self.dirty_facts.insert(volume_index);
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

        // Plan 136 E1 checklist site 1. `routes_direct()` is now true for an
        // encrypted store member, so this filter admits ciphertext — and would
        // create a `.direct.partial` for it, size every extent off the
        // *plaintext* `unpacked_size` while the cipher stream runs to
        // `align16(unpacked_size)`, and route the bytes unchanged. Admission is
        // therefore decided **first**, before a single destination exists: no
        // password, a refuted one, or key material this build cannot use, and
        // the set demotes here rather than writing anything.
        let keys = self.admit_encrypted()?;

        let routable: Vec<(String, u64, Option<weaver_unrar::EncryptedStore>)> = self
            .layout_members()
            .iter()
            .filter(|member| member.eligibility.routes_direct())
            .map(|member| {
                (
                    member.name.clone(),
                    member.unpacked_size.unwrap_or(0),
                    member.eligibility.encrypted_store(),
                )
            })
            .collect();
        for (name, unpacked_size, encrypted) in routable {
            // Unreachable while `admit_encrypted` demotes on every refusal, and
            // stated anyway: an encrypted member with no key routes nothing, and
            // the alternative to skipping it is a destination full of cipher.
            if encrypted.is_some() && !keys.contains_key(&name) {
                continue;
            }
            if let Some(member_id) = self.member_ids.get(&name).copied() {
                if let Some(existing) = self.members.get_mut(&member_id) {
                    existing.unpacked_size = unpacked_size;
                    if let (Some(facts), Some(crypt)) = (encrypted, existing.crypt.as_mut()) {
                        // The cipher extent resolves — from unknown to known —
                        // as the headers that declare a size arrive.
                        crypt.observe(&facts);
                    }
                }
                continue;
            }
            let relative_partial = match self.plan.member_partial_path(&name) {
                Ok(path) => path,
                Err(()) => return Err(self.fail(DemotionReason::UnsafeDestination)),
            };
            let crypt = encrypted.and_then(|facts| {
                let (member_keys, record) = keys.get(&name)?;
                let mut crypt = MemberCrypt::new(*member_keys, record);
                crypt.observe(&facts);
                Some(crypt)
            });
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
                    restart_seeded: ByteRanges::new(),
                    stale_gaps: ByteRanges::new(),
                    verified: false,
                    crypt,
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

    /// The encrypted-store admission decision (plan 136, E-D1).
    ///
    /// Runs at every parse, before [`Self::sync_members`] creates anything, and
    /// answers one question per encrypted member: is there a password that may
    /// key it? Key derivation happens once per KDF tuple — a set whose members
    /// share one pays a single PBKDF2 — and the RAR5 password check is verified
    /// **before any byte routes**.
    ///
    /// Four refusals, all of them demotions:
    ///
    /// - the job's spec declares a PAR2 file (E1 review F1). An encrypted set's
    ///   destinations hold plaintext where PAR2 describes the posted cipher, and
    ///   the guard that catches this behind the authoritative pass cannot run
    ///   until the whole set has downloaded — at which point demoting costs a
    ///   full refetch, because plaintext partials cannot reconstruct posted
    ///   bytes. Refusing here is the pre-plan-136 behaviour exactly: one hard
    ///   demotion on the first header parse, one article back on the wire;
    /// - no password: an encrypted set routes only with one;
    /// - a check present that this password does not reproduce: nothing is
    ///   written on the strength of a refuted password;
    /// - key material this build cannot derive from — a RAR4 member (file
    ///   encryption for RAR4/RAR3 is phase E3, and a RAR4 header states no
    ///   `FHEXTRA_CRYPT` record at all), or a KDF count the crate refuses.
    ///
    /// A check the header **omits** admits provisionally: nothing can be
    /// concluded before the bytes, and the member's keyed checksum gate is then
    /// the earliest detector — the same position layer 1 is in for a plaintext
    /// member.
    fn admit_encrypted(
        &mut self,
    ) -> Result<
        HashMap<
            String,
            (
                crypt::MemberKeys,
                weaver_unrar::RarVolumeMemberEncryptionFacts,
            ),
        >,
        DemotionReason,
    > {
        let encrypted: Vec<(String, weaver_unrar::EncryptedStore)> = self
            .layout_members()
            .iter()
            .filter_map(|member| {
                member
                    .eligibility
                    .encrypted_store()
                    .map(|facts| (member.name.clone(), facts))
            })
            .collect();
        let mut keys = HashMap::with_capacity(encrypted.len());
        for (name, facts) in encrypted {
            let Some(record) = facts.crypt else {
                return Err(self.fail(DemotionReason::EncryptedMemberRefused(
                    CryptRefusal::Unkeyable,
                )));
            };
            match self.crypt.admit(&record) {
                Ok(member_keys) => {
                    keys.insert(name, (member_keys, record));
                }
                Err(refusal) => {
                    return Err(self.fail(DemotionReason::EncryptedMemberRefused(refusal)));
                }
            }
        }
        Ok(keys)
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
            let reason = match member.eligibility {
                MemberEligibility::DirectEligible | MemberEligibility::ProvisionallyDirect => {
                    routable += 1;
                    continue;
                }
                // Plan 136 E1 checklist site 2. The `let ... else` this replaces
                // counted **every** non-`Ineligible` member routable, and
                // `EncryptedStore` is not `Ineligible` — so an all-encrypted set
                // with no password would have sailed past the `routable == 0`
                // demotion below with nothing to route and no reason to stop,
                // silently deleting the hard demotion this path has always
                // guaranteed. Routable means *decryptable*: a member the key
                // ring admitted counts, and one it did not is the set's own
                // reason to leave direct mode.
                MemberEligibility::EncryptedStore(_) => {
                    if self.crypt.admitted() {
                        routable += 1;
                        continue;
                    }
                    let refusal = self.crypt.refusal().unwrap_or(CryptRefusal::NoPassword);
                    return Err(self.fail(DemotionReason::EncryptedMemberRefused(refusal)));
                }
                MemberEligibility::Ineligible(reason) => reason,
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
    ///
    /// # Plan 136 E1 checklist site 3
    ///
    /// `Ineligible(_)` stopped spanning "every member finalization must extract"
    /// the moment `EncryptedStore` existed: it is not `Ineligible`, so the old
    /// predicate dropped an encrypted member from this list while nothing put it
    /// on the routed one — a member in neither list is a member silently missing
    /// from the output. The decision is stated rather than implied. An
    /// **admitted** encrypted member is direct-routed and must not be
    /// re-extracted over its own verified bytes. One the set could **not** key
    /// belongs here, because the conventional extractor — which asks the job's
    /// whole password-candidate list, a superset of the single password
    /// direct-store is handed — is the only thing that can still produce it.
    /// (While the set lives that case is unreachable, since admission demotes
    /// the whole set rather than routing around one member; it is written down
    /// because the alternative to writing it down is losing a file.)
    pub(crate) fn tolerated_member_names(&self) -> Vec<String> {
        let admitted = self.crypt.admitted();
        let mut names: Vec<(u32, u64, String)> = self
            .layout_members()
            .iter()
            .filter(|member| match member.eligibility {
                MemberEligibility::Ineligible(_) => true,
                MemberEligibility::EncryptedStore(_) => !admitted,
                MemberEligibility::DirectEligible | MemberEligibility::ProvisionallyDirect => false,
            })
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
        // Split at the repair boundaries **before** anything is mapped: the
        // emitted run's `replace` flag is all-or-nothing, and neither the layout
        // nor `pending`'s coalescing knows where a repair starts and stops
        // ([`VolumeStaging::repair_partition`]).
        let pending: Vec<(u64, u64)> = staging
            .pending
            .ranges()
            .iter()
            .flat_map(|(start, end)| staging.repair_partition(*start, *end))
            .collect();
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
                            .and_then(|staging| staging.slice(cursor, len, &self.scratch));
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
                        let staging = self.staging.get(&volume_index);
                        // The repair marker (D3): read before the slice, from
                        // the same staging entry, so the decision is made on
                        // the range that is about to drain rather than on a
                        // router-wide mode a concurrent duplicate could ride.
                        let replace =
                            staging.is_some_and(|staging| staging.is_repaired(cursor, len));
                        let bytes =
                            staging.and_then(|staging| staging.slice(cursor, len, &self.scratch));
                        if let Some(bytes) = bytes {
                            self.note_member_bytes(
                                member_id,
                                volume_index,
                                logical_offset,
                                &bytes,
                                replace,
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
                    // Plan 136, E-D2. Cipher bytes: a routing decision of their
                    // own, never a copy of the `Member` arm above — writing them
                    // where that arm writes would put ciphertext in the
                    // destination, which is the whole reason the layout gives
                    // them their own variant. What they share is the
                    // *coordinates*: cipher offset and member-logical offset are
                    // the same number for a stored member, so every range answer
                    // plan 135 computes is unchanged and only the bytes differ.
                    MappedSlice::EncryptedMember {
                        member_index,
                        logical_offset,
                        len,
                    } => {
                        let staging = self.staging.get(&volume_index);
                        let replace =
                            staging.is_some_and(|staging| staging.is_repaired(cursor, len));
                        self.route_encrypted_slice(
                            volume_index,
                            cursor,
                            member_index,
                            logical_offset,
                            len,
                            replace,
                            &mut spans,
                            &mut routed,
                        )?;
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
                // The repair marker lives exactly as long as the bytes it
                // describes: a routed range is composed, so a duplicate of it
                // arriving later is a duplicate again and must clip.
                staging.repaired = subtract(&staging.repaired, *start, *len);
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
                .map(|(offset, chunk)| (*offset, chunk.len()))
                .collect();
            for (offset, len) in chunks {
                let mut cursor = offset;
                for slice in self.map_physical_range(volume_index, offset, len) {
                    match slice {
                        // An encrypted member's bytes are as droppable as a
                        // plaintext one's, and for the same reason: what is not
                        // droppable is still **pending**, which `keep` starts
                        // from. A sub-block remainder the drain could not decrypt
                        // was never routed, so it is pending here and pending in
                        // whichever neighbouring volume holds the rest of its
                        // block — and each side's own `pending` is what keeps
                        // both halves alive until the block closes. Keeping
                        // routed cipher on top of that would retain a second copy
                        // of the payload for the life of the volume, which is the
                        // RSS term envelope v2's trim exists to remove.
                        MappedSlice::Member { len, .. }
                        | MappedSlice::EncryptedMember { len, .. } => {
                            cursor = cursor.saturating_add(len)
                        }
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
    ///
    /// `replace` is D3's repair marker. Without it a run whose bytes the
    /// coverage map already claims is a duplicate and contributes nothing; with
    /// it the run is a PAR2 repair of those very bytes, so the composition is
    /// **overwritten** and whatever the rewrite half-covered becomes a stale gap
    /// the caller must re-read.
    fn note_member_bytes(
        &mut self,
        member_id: u32,
        volume_index: u32,
        logical_offset: u64,
        bytes: &[u8],
        replace: bool,
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
        if member.covered.insert(logical_offset, len) == 0 && !replace {
            // Wholly duplicate: never advance a gate twice.
            return Ok(());
        }
        let crc = weaver_par2::checksum::crc32(bytes);
        let part_relative = logical_offset.saturating_sub(part_logical_offset);
        if replace {
            let gaps =
                member
                    .parts
                    .entry(part_position)
                    .or_default()
                    .overwrite(part_relative, len, crc);
            // A repaired span can only *resolve* gaps that fall inside it, so
            // the rewritten range leaves the stale set before the new gaps join
            // it — and both are recorded in member-logical space, which is what
            // the re-read plan and the coverage map speak.
            member.stale_gaps = subtract(&member.stale_gaps, logical_offset, len);
            for (start, end) in gaps {
                member.stale_gaps.insert(
                    start.saturating_add(part_logical_offset),
                    end.saturating_sub(start),
                );
            }
            // Both of these described bytes that no longer exist. Dropping the
            // part's checked value is what keeps a stale one from surviving the
            // rewrite: while the gaps are open the composition below yields
            // nothing, so without the removal the member would go on verifying
            // against the value the damaged bytes produced.
            member.checked_parts.remove(&part_position);
            member.verified = false;
        } else {
            member
                .parts
                .entry(part_position)
                .or_default()
                .insert(part_relative, len, crc);
        }

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

    // ---- Encrypted members: decrypt at write (plan 136, E-D2) --------------

    /// Routes one encrypted member slice, decrypting on the way in.
    ///
    /// The nzbdav insight makes this nearly stateless: decrypting cipher block
    /// *N* needs only cipher block *N−1*, so a router holding spans out of order
    /// can decrypt each one the moment its predecessor has landed. There is no
    /// chain checkpoint to maintain and no forward-only constraint.
    ///
    /// What is left is arithmetic on three pieces, because a slice's edges are
    /// article- and volume-shaped while AES is block-shaped:
    ///
    /// - a **head** partial block, when the slice does not start on a 16-byte
    ///   boundary — its first bytes belong to the previous article or the
    ///   previous *volume*, since a split member's parts are not individually
    ///   block-aligned;
    /// - the **aligned middle**, which is all of the slice for an aligned span
    ///   and is decrypted in one pass;
    /// - a **tail** partial block, symmetric with the head.
    ///
    /// Each edge block is resolved once, by whichever side reaches it first, and
    /// its plaintext is kept in [`MemberCrypt::edge_plain`] for the other side.
    /// That is what stops the two halves of a straddling block from deadlocking:
    /// a drain emits spans **only for the volume it is draining**, so without
    /// the shared plaintext each side would sit holding its half waiting for the
    /// other to route bytes it is not allowed to route.
    ///
    /// Anything that cannot be resolved is simply **not routed**: it stays
    /// `pending` in this volume's staging and rides the existing holds
    /// machinery, bounded by one article per member per gap.
    ///
    /// No new re-drain trigger is needed for that, and it is worth saying why,
    /// because the obvious reading is that a volume whose articles have all
    /// arrived would never be revisited. [`Self::route`] and
    /// [`Self::note_volume_complete`] both stage first and then drain **every**
    /// volume in the set, in ascending order, precisely so a header landing in
    /// one volume can release another's holds. A straddling block therefore
    /// resolves in whichever call brings its missing half: the earlier volume's
    /// drain reads the later one's freshly staged bytes through
    /// [`Self::member_cipher`], and the later volume's own drain — later in the
    /// same loop — finds the plaintext waiting for it.
    #[allow(clippy::too_many_arguments)]
    fn route_encrypted_slice(
        &mut self,
        volume_index: u32,
        cursor: u64,
        member_index: usize,
        logical_offset: u64,
        len: u64,
        replace: bool,
        spans: &mut Vec<RoutedSpan>,
        routed: &mut Vec<(u64, u64)>,
    ) -> Result<(), DemotionReason> {
        if len == 0 {
            return Ok(());
        }
        let Some(member_id) = self.member_id_for_layout(member_index) else {
            debug_assert!(
                false,
                "the layout mapped encrypted member {member_index} of {}, which the router never \
                 adopted",
                self.plan.set_name
            );
            return Ok(());
        };
        // The cipher extent, never `unpacked_size`: the stream runs to
        // `align16(unpacked_size)` and every length check that used the declared
        // size would be short by the tail padding.
        let sizes = self.members.get(&member_id).and_then(|member| {
            let crypt = member.crypt.as_ref()?;
            Some((crypt.cipher_size()?, member.unpacked_size))
        });
        let Some((cipher_size, unpacked_size)) = sizes else {
            // No declared size yet — the headers have not reached the one that
            // states it — or a member with no keys, which admission has already
            // demoted for. Either way the bytes stay pending.
            return Ok(());
        };
        let slice_end = logical_offset.saturating_add(len);
        debug_assert!(slice_end <= cipher_size);

        let head_block =
            (!logical_offset.is_multiple_of(AES_BLOCK)).then(|| block_floor(logical_offset));
        let mid_start = block_ceil(logical_offset);
        let mid_end = block_floor(slice_end);
        let tail_block = (!slice_end.is_multiple_of(AES_BLOCK)
            && head_block != Some(block_floor(slice_end)))
        .then(|| block_floor(slice_end));

        // `(cipher offset, cipher bytes, plaintext bytes)`, ascending.
        let mut pieces: Vec<(u64, Vec<u8>, Vec<u8>)> = Vec::new();
        let mut held = false;

        for edge in [head_block, tail_block].into_iter().flatten() {
            let from = logical_offset.max(edge);
            let to = slice_end.min(edge.saturating_add(AES_BLOCK));
            let block = self.encrypted_block_plain(member_id, edge);
            let cipher =
                self.staged_bytes_at(volume_index, cursor + (from - logical_offset), to - from);
            match (block, cipher) {
                (Some(block), Some(cipher)) => {
                    let plain = block[(from - edge) as usize..(to - edge) as usize].to_vec();
                    pieces.push((from, cipher, plain));
                }
                _ => held = true,
            }
        }

        if mid_start < mid_end {
            let preceding = self.member_preceding_block(member_id, mid_start);
            let cipher = self.staged_bytes_at(
                volume_index,
                cursor + (mid_start - logical_offset),
                mid_end - mid_start,
            );
            match (preceding, cipher) {
                (Some(preceding), Some(cipher)) => {
                    let mut plain = cipher.clone();
                    let decrypted = self
                        .members
                        .get_mut(&member_id)
                        .and_then(|member| member.crypt.as_mut())
                        .is_some_and(|crypt| {
                            crypt.decrypt_range(mid_start, &preceding, &mut plain)
                        });
                    if decrypted {
                        pieces.push((mid_start, cipher, plain));
                    } else {
                        held = true;
                    }
                }
                _ => held = true,
            }
        }

        if held {
            #[cfg(test)]
            {
                self.blocks_held = self.blocks_held.saturating_add(1);
            }
            // The one new hold shape E-D2 introduces: a cipher block whose other
            // half has not arrived. Bounded by one article per member per gap,
            // so a large count here says the set is arriving badly out of order,
            // not that the transform is leaking.
            crate::runtime::perf_probe::record(
                "direct_store.encrypted.block_held",
                std::time::Duration::from_nanos(1),
            );
        }
        pieces.sort_by_key(|(start, _, _)| *start);
        for (start, cipher, plain) in pieces {
            let physical = cursor + (start - logical_offset);
            let piece_len = cipher.len() as u64;
            // Everything at or past the declared size is tail padding: real
            // cipher, never a destination byte.
            let destination_len = unpacked_size.saturating_sub(start).min(piece_len);
            self.note_encrypted_member_bytes(
                member_id,
                volume_index,
                start,
                &cipher,
                &plain,
                unpacked_size,
                replace,
            )?;
            if destination_len > 0 {
                self.record_routed_extent(
                    volume_index,
                    MemberExtent {
                        member_id,
                        physical_offset: physical,
                        logical_offset: start,
                        len: destination_len,
                    },
                );
                spans.push(RoutedSpan {
                    destination: DirectDestination::Member { member_id },
                    destination_offset: start,
                    volume_index,
                    source_offset: physical,
                    bytes: plain[..destination_len as usize].to_vec(),
                });
            }
            // The tail padding's **source** bytes (E-D2). Their plaintext is
            // never a destination byte, but they are real posted bytes with a
            // real physical offset, and leaving them unrouted would stall the
            // volume's coverage floor forever — 0–15 bytes short, at the end of
            // the last part, for the life of the job. They go to the envelope,
            // which is a sparse image of the volume at true physical offsets, so
            // what lands there is exactly what was posted: the one place the
            // last cipher block still exists once the plaintext is on disk.
            if piece_len > destination_len {
                spans.push(RoutedSpan {
                    destination: DirectDestination::Envelope { volume_index },
                    destination_offset: physical + destination_len,
                    volume_index,
                    source_offset: physical + destination_len,
                    bytes: cipher[destination_len as usize..].to_vec(),
                });
            }
            routed.push((physical, piece_len));
        }
        Ok(())
    }

    /// The plaintext of one whole cipher block of an encrypted member.
    ///
    /// Answered from [`MemberCrypt::edge_plain`] when another volume's drain has
    /// already decrypted it, and otherwise assembled: the block's 16 cipher
    /// bytes — which may span two source volumes — plus its CBC predecessor.
    /// `None` means one of those is not here yet, which is a hold, not an error.
    fn encrypted_block_plain(&mut self, member_id: u32, block_start: u64) -> Option<[u8; 16]> {
        if let Some(plain) = self
            .members
            .get(&member_id)
            .and_then(|member| member.crypt.as_ref())
            .and_then(|crypt| crypt.edge_plain(block_start))
        {
            return Some(plain);
        }
        let preceding = self.member_preceding_block(member_id, block_start)?;
        let cipher = self.member_cipher(member_id, block_start, AES_BLOCK)?;
        let mut plain = cipher;
        let crypt = self
            .members
            .get_mut(&member_id)?
            .crypt
            .as_mut()
            .expect("an encrypted member's crypt state is created with the member");
        if !crypt.decrypt_range(block_start, &preceding, &mut plain) {
            return None;
        }
        let block: [u8; 16] = plain.try_into().ok()?;
        crypt.retain_edge(block_start, block);
        Some(block)
    }

    /// The 16 cipher bytes immediately before `block_start`: the member's IV at
    /// offset 0, a retained checkpoint at a decrypted run's frontier, or — for a
    /// block whose predecessor is still staged — the staged bytes themselves.
    fn member_preceding_block(&self, member_id: u32, block_start: u64) -> Option<[u8; 16]> {
        if let Some(block) = self
            .members
            .get(&member_id)
            .and_then(|member| member.crypt.as_ref())
            .and_then(|crypt| crypt.preceding_block(block_start))
        {
            return Some(block);
        }
        let previous = block_start.checked_sub(AES_BLOCK)?;
        self.member_cipher(member_id, previous, AES_BLOCK)?
            .try_into()
            .ok()
    }

    /// Reads a member-logical (== cipher) range out of whatever source volumes
    /// hold it, through the layout's part table.
    ///
    /// Cross-volume by construction: the 16 bytes before a part's first byte are
    /// the tail of the previous volume's part, and that is the ordinary case for
    /// a split encrypted member. `None` when any byte of the range is not
    /// staged — routed bytes are gone from staging, which is exactly why
    /// [`MemberCrypt`] retains checkpoints and edge plaintext rather than
    /// re-reading them here.
    fn member_cipher(&self, member_id: u32, logical_offset: u64, len: u64) -> Option<Vec<u8>> {
        let layout_index = self.layout_index_for_member(member_id)?;
        let member = self.layout_members().get(layout_index)?;
        let end = logical_offset.checked_add(len)?;
        let mut out = Vec::with_capacity(len as usize);
        let mut cursor = logical_offset;
        while cursor < end {
            let mut located = None;
            for part in &member.parts {
                let Some(start) = part.logical_offset else {
                    continue;
                };
                let part_end = start.saturating_add(part.data_size);
                if cursor >= start && cursor < part_end {
                    located = Some((part.volume, part.data_offset + (cursor - start), part_end));
                    break;
                }
            }
            let (volume, physical, part_end) = located?;
            let take = (part_end - cursor).min(end - cursor);
            out.extend_from_slice(&self.staged_bytes_at(volume, physical, take)?);
            cursor += take;
        }
        Some(out)
    }

    /// One volume's staged bytes, or `None` when the range is not wholly staged.
    fn staged_bytes_at(&self, volume_index: u32, offset: u64, len: u64) -> Option<Vec<u8>> {
        if len == 0 {
            return Some(Vec::new());
        }
        self.staging
            .get(&volume_index)
            .and_then(|staging| staging.slice(offset, len, &self.scratch))
    }

    /// Feeds one decrypted run into the integrity gates (plan 136, E-D3).
    ///
    /// Two layers, two byte spaces, and that split is the whole point:
    ///
    /// - **Layer 1** composes the part's packed hash over **cipher** bytes,
    ///   before decryption. RARLAB `rar` leaves a split member's non-final
    ///   packed checksums *plain* even when it keys the whole-member one, so
    ///   this layer passes over ciphertext whatever the password was: it is a
    ///   wire-integrity check and **not** a wrong-password detector.
    /// - **Layer 2** composes plain CRC32 over the **plaintext** runs and folds
    ///   the result with the KDF hash key when the header keys it. That is the
    ///   real wrong-password backstop, and for a member whose header carries no
    ///   password check it is the *only* one.
    #[allow(clippy::too_many_arguments)]
    fn note_encrypted_member_bytes(
        &mut self,
        member_id: u32,
        volume_index: u32,
        cipher_offset: u64,
        cipher: &[u8],
        plain: &[u8],
        unpacked_size: u64,
        replace: bool,
    ) -> Result<(), DemotionReason> {
        let len = cipher.len() as u64;
        if len == 0 {
            return Ok(());
        }
        let Some(layout_index) = self.layout_index_for_member(member_id) else {
            return Ok(());
        };
        let Some(part) = self.part_for(layout_index, volume_index) else {
            return Ok(());
        };
        let (part_position, part_logical_offset, part_len, packed_crc32) = part;
        let packed_uses_mac = self
            .layout_members()
            .get(layout_index)
            .and_then(|member| member.parts.get(part_position as usize))
            .is_some_and(|part| part.packed_hash_uses_mac);
        let Some(member) = self.members.get_mut(&member_id) else {
            return Ok(());
        };
        let Some(crypt) = member.crypt.as_mut() else {
            return Ok(());
        };
        // Duplicate detection lives in **cipher** space here, not in the
        // destination coverage map: the tail padding is cipher the member routed
        // and destination bytes it never had, so a map that cannot name those
        // offsets cannot tell a duplicate of them from a first arrival.
        if crypt.note_emitted(cipher_offset, len) == 0 && !replace {
            return Ok(());
        }
        crypt.retain_tail_padding(unpacked_size, cipher_offset, plain);
        let destination_len = unpacked_size.saturating_sub(cipher_offset).min(len);
        let cipher_crc = weaver_par2::checksum::crc32(cipher);
        let part_relative = cipher_offset.saturating_sub(part_logical_offset);
        if destination_len > 0 {
            let plain_crc = weaver_par2::checksum::crc32(&plain[..destination_len as usize]);
            if replace {
                crypt
                    .plain_runs_mut()
                    .overwrite(cipher_offset, destination_len, plain_crc);
            } else {
                crypt
                    .plain_runs_mut()
                    .insert(cipher_offset, destination_len, plain_crc);
            }
        }
        let part_complete = crypt.emitted_covers(part_logical_offset, part_len);
        if replace {
            let gaps = member.parts.entry(part_position).or_default().overwrite(
                part_relative,
                len,
                cipher_crc,
            );
            member.stale_gaps = subtract(&member.stale_gaps, cipher_offset, destination_len);
            for (start, end) in gaps {
                member.stale_gaps.insert(
                    start.saturating_add(part_logical_offset),
                    end.saturating_sub(start),
                );
            }
            member.checked_parts.remove(&part_position);
            member.verified = false;
        } else {
            member
                .parts
                .entry(part_position)
                .or_default()
                .insert(part_relative, len, cipher_crc);
        }
        if destination_len > 0 {
            member.covered.insert(cipher_offset, destination_len);
        }

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
            if let Some(expected) = packed_crc32 {
                let composed = member
                    .crypt
                    .as_ref()
                    .map(|crypt| crypt.fold_member_crc(value, packed_uses_mac))
                    .unwrap_or(value);
                if expected != composed {
                    return Err(self.fail(DemotionReason::PartChecksumMismatch));
                }
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
        // D6's re-arm rule, stated as a refusal. A member carrying restart-seeded
        // coverage has bytes on disk that no `CrcRuns` in this process ever saw,
        // so there is no composed value for them — and there must not be one
        // until they are re-read. The composition below would already stall on
        // the missing `checked_parts` entry in every shape this can take; saying
        // it here means a future part-granularity change cannot quietly turn
        // "unverifiable" into "verified".
        //
        // D4 says the same thing about a repair's stale gaps, and for the same
        // reason: they are covered bytes whose composed value a rewrite threw
        // away, so composing around them would pass the member on the strength
        // of runs that describe a *different* span than the one on disk.
        if self.members.get(&member_id).is_some_and(|member| {
            !member.restart_seeded.is_empty() || !member.stale_gaps.is_empty()
        }) {
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
        let uses_mac = layout_member.data_hash_uses_mac;
        let Some(member) = self.members.get(&member_id) else {
            return Ok(());
        };
        if member.covered.contiguous_from_zero() < unpacked_size {
            return Ok(());
        }

        // Layer 2 for an encrypted member (plan 136, E-D3). Composed over
        // **plaintext**, member-wide rather than per part, then folded with the
        // KDF hash key when the header keys the checksum.
        //
        // This is the real wrong-password gate. Layer 1 above cannot be one: its
        // packed hashes cover cipher bytes and are plain CRC32s on the non-final
        // parts, so they pass identically whatever key the bytes were decrypted
        // with — a wrong password that got past admission (no check in the
        // header, or a forged one) reaches here with every earlier gate green.
        //
        // It deliberately does **not** read `checked_parts`. Those are cipher
        // values, and after a restart the cipher is gone: only the plaintext is
        // on disk, so a re-armed member composes exactly what its re-read
        // produced and nothing else.
        if let Some(crypt) = member.crypt.as_ref() {
            if !crypt.tail_padding_retained() {
                return Ok(());
            }
            let Some(composed) = crypt.plain_runs().compose(0, unpacked_size) else {
                return Ok(());
            };
            if crypt.fold_member_crc(composed, uses_mac) != expected {
                return Err(self.fail(DemotionReason::MemberChecksumMismatch));
            }
            if let Some(member) = self.members.get_mut(&member_id) {
                member.verified = true;
            }
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

    // ---- Restart (plan 135, D6) -------------------------------------------
    //
    // A restarted set rebuilds its layout from the **cached volume facts**, not
    // from bytes: the header bytes sit below the published floors, so they are
    // not refetched and nothing would re-parse them. Everything the router
    // derives from a parse — members, ids, destinations, the plan digest — comes
    // back from the same `add_volume` calls the live path makes, in ascending
    // volume order so the rebuild is deterministic.
    //
    // What deliberately does **not** come back is any integrity state. Coverage
    // is re-derived (it says which bytes are on disk); `CrcRuns` is not (it would
    // say those bytes are *good*, on the authority of a process that is gone).

    /// Takes the volume facts this run has accepted and not yet cached.
    ///
    /// Drained by the caller, which owns the database, and cleared only by the
    /// take: a failed write leaves nothing dirty, so the cache would go stale
    /// silently. That is deliberate — the caller re-marks on failure, and losing
    /// a fact costs a redownload of that set on the next restart, never a wrong
    /// restore, because the checkpoint's plan digest is computed from the same
    /// facts and a missing one cannot reproduce it.
    pub(crate) fn take_dirty_facts(&mut self) -> Vec<(u32, RarVolumeFacts)> {
        let dirty = std::mem::take(&mut self.dirty_facts);
        dirty
            .into_iter()
            .filter_map(|volume_index| {
                self.volume_facts
                    .get(&volume_index)
                    .map(|facts| (volume_index, facts.clone()))
            })
            .collect()
    }

    /// Puts a volume back in the dirty set after a failed cache write.
    pub(crate) fn remark_dirty_fact(&mut self, volume_index: u32) {
        if self.volume_facts.contains_key(&volume_index) {
            self.dirty_facts.insert(volume_index);
        }
    }

    /// Rebuilds the layout from cached `RarVolumeFacts`.
    ///
    /// The facts are the ones this router itself accepted before the restart, so
    /// re-adding them exercises exactly the paths the live parse does — the
    /// format check, the layout's conflict detection, member adoption, the
    /// collision keys and the chain-close eligibility rule. A set whose cached
    /// facts no longer form a routable archive demotes here, at restore, rather
    /// than after its first refetched article.
    pub(crate) fn restore_layout(
        &mut self,
        facts: &BTreeMap<u32, RarVolumeFacts>,
    ) -> Result<(), DemotionReason> {
        for (volume_index, volume_facts) in facts {
            if !self.plan.volumes.contains_key(volume_index) {
                // The row names a volume this job no longer plans. Refusing is
                // the same stance the checkpoint reader takes on an unknown set.
                return Err(self.fail(DemotionReason::ConflictingVolumeFacts));
            }
            if self.layout.is_none() {
                let format = volume_facts.archive_format();
                if !matches!(format, ArchiveFormat::Rar4 | ArchiveFormat::Rar5) {
                    return Err(self.fail(DemotionReason::UnsupportedFormat));
                }
                self.layout = Some(StoredLayoutBuilder::new(format));
            }
            let added = self
                .layout
                .as_mut()
                .expect("the layout was bound above")
                .add_volume(*volume_index, volume_facts);
            match added {
                Ok(()) => {}
                Err(StoredLayoutError::ConflictingVolume { .. }) => {
                    return Err(self.fail(DemotionReason::ConflictingVolumeFacts));
                }
                Err(StoredLayoutError::FormatMismatch { .. }) => {
                    return Err(self.fail(DemotionReason::FormatMismatch));
                }
            }
            self.volume_facts
                .insert(*volume_index, volume_facts.clone());
            self.member_order_stale = true;
        }
        if self.layout.is_none() {
            return Ok(());
        }
        self.sync_members()?;
        self.check_eligibility()?;
        Ok(())
    }

    /// Seeds one member's coverage from a checkpoint's destination claim.
    ///
    /// `extents` are half-open logical ranges of the member's `.direct.partial`.
    /// They enter [`MemberRouting::covered`], so those bytes are neither
    /// refetched nor re-routed, and [`MemberRouting::restart_seeded`], so the
    /// whole-member gate stays disarmed over them until they are re-read.
    ///
    /// Keyed by the destination's **relative path** rather than by the blob's
    /// member index: the index is an in-run counter, and a set whose volumes
    /// arrived in a different order last run numbered its members differently.
    /// The path is derived from the header name, which is the layout's own key.
    pub(crate) fn restore_member_coverage(
        &mut self,
        relative_partial: &str,
        extents: &[(u64, u64)],
    ) -> Option<u32> {
        let member_id = self.members.iter().find_map(|(member_id, member)| {
            (member.relative_partial == relative_partial).then_some(*member_id)
        })?;
        let member = self.members.get_mut(&member_id)?;
        for (start, end) in extents {
            let len = end.saturating_sub(*start);
            if len == 0 {
                continue;
            }
            member.covered.insert(*start, len);
            member.restart_seeded.insert(*start, len);
            // An encrypted member keeps a second, cipher-space account, because
            // its part completeness and its duplicate filter both live there
            // (the tail padding is cipher the destination map cannot name).
            // Seeded, like `covered`, purely so nothing is re-emitted: the
            // verification claim stays with `restart_seeded` alone.
            if let Some(crypt) = member.crypt.as_mut() {
                crypt.seed_emitted(*start, len);
            }
        }
        Some(member_id)
    }

    /// Seeds one source volume's restored state: what is already on disk, and
    /// whether the volume's header walk is finished.
    ///
    /// Three things depend on this and none of them can be re-derived from
    /// bytes, because the bytes are not coming back:
    ///
    /// - `routed` stops a refetched article from re-staging a range the previous
    ///   run already placed;
    /// - `confirmed` is what lets the drain route the volume's trailing region
    ///   instead of holding it as unproven classification — an unconfirmed volume
    ///   holds every envelope byte at or past `tail_base`, which for a restored
    ///   volume is offset zero, so without this a restart would hold the whole
    ///   volume and demote on the holds budget;
    /// - the routed-extent history is what the hybrid provider reads a virtual
    ///   volume through, and it is a **history**, not a classification (B1).
    ///
    /// The history is re-derived by mapping the covered physical ranges through
    /// the rebuilt layout and clipping each member slice to that member's own
    /// restored claim: a byte is claimed as a member's only when the volume floor
    /// and the destination claim agree it was written, which is the same
    /// both-sides rule the barrier records writes under.
    ///
    /// `decoded_len` is `Some(len)` exactly when the checkpoint calls the volume
    /// complete, and `len` is then the volume's whole decoded length — the same
    /// number as the row's floor, because the published `complete` bit is itself
    /// the conjunction of "the download finished" and "the floor covers all of
    /// it". It is passed as a length rather than a flag so the confirmation
    /// derivation can *check* the claim against the coverage in front of it
    /// instead of trusting a bit.
    pub(crate) fn restore_volume_coverage(
        &mut self,
        volume_index: u32,
        covered: &ByteRanges,
        decoded_len: Option<u64>,
    ) {
        let source_complete = decoded_len.is_some();
        let confirmed = restored_volume_is_confirmed(
            covered,
            decoded_len,
            self.volume_facts
                .get(&volume_index)
                .is_some_and(|facts| facts.more_volumes),
        );
        let tail_base = self
            .volume_facts
            .get(&volume_index)
            .map(|facts| {
                facts
                    .members
                    .iter()
                    .map(|member| member.data_offset.saturating_add(member.data_size))
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0);

        let ranges: Vec<(u64, u64)> = covered.ranges().to_vec();
        {
            let staging = self.staging.entry(volume_index).or_default();
            for (start, end) in &ranges {
                staging.routed.insert(*start, end - start);
            }
            staging.provisional = true;
            staging.confirmed = confirmed;
            staging.source_complete = source_complete;
            staging.restored = true;
            staging.tail_base = staging.tail_base.max(tail_base);
        }

        for (start, end) in ranges {
            let mut cursor = start;
            for slice in self.map_physical_range(volume_index, start, end - start) {
                // Plan 136 E1 checklist site 4. This was a refutable `let ...
                // else` on `MappedSlice::Member`, which `EncryptedMember` slid
                // straight past — silently, and *permanently* silently once
                // `slice_len` grew an `EncryptedMember` arm, since that
                // exhaustiveness was the only thing keeping this site loud. The
                // effect was not a lost byte but a lost **record**:
                // `record_routed_extent` never ran, so the routed-extent history
                // (B1) had no claim on those offsets and the post-restart
                // provider answered them out of the envelope — where they are a
                // sparse hole inside its length, which is to say zeros, in a
                // volume whose floor says the bytes are durable.
                //
                // An encrypted member's bytes are at exactly the same
                // coordinates (cipher offset and member-logical offset coincide
                // for a stored member), so the two arms share a body. The one
                // difference needs no code: an encrypted member's final ≤15
                // cipher bytes are tail padding that rode the envelope rather
                // than the destination, and the clip against the member's own
                // restored claim below already refuses to hand them back.
                let (member_index, logical_offset, len) = match slice {
                    MappedSlice::Member {
                        member_index,
                        logical_offset,
                        len,
                    }
                    | MappedSlice::EncryptedMember {
                        member_index,
                        logical_offset,
                        len,
                    } => (member_index, logical_offset, len),
                    MappedSlice::Envelope { .. } | MappedSlice::Unroutable { .. } => {
                        cursor = cursor.saturating_add(slice_len(&slice));
                        continue;
                    }
                };
                let Some(member_id) = self.member_id_for_layout(member_index) else {
                    cursor = cursor.saturating_add(len);
                    continue;
                };
                // Clip to what the member's own claim backs. A gap here is not a
                // contradiction to resolve — it is a byte the previous run's
                // floor covered but whose destination write the checkpoint never
                // claimed, so nothing may read it back.
                let claimed = self
                    .members
                    .get(&member_id)
                    .map(|member| &member.covered)
                    .map(|covered| covered.missing(logical_offset, len))
                    .unwrap_or_else(|| vec![(logical_offset, logical_offset + len)]);
                let mut logical_cursor = logical_offset;
                let logical_end = logical_offset.saturating_add(len);
                let mut runs: Vec<(u64, u64)> = Vec::new();
                for (gap_start, gap_end) in claimed {
                    if gap_start > logical_cursor {
                        runs.push((logical_cursor, gap_start));
                    }
                    logical_cursor = gap_end;
                }
                if logical_cursor < logical_end {
                    runs.push((logical_cursor, logical_end));
                }
                for (run_start, run_end) in runs {
                    self.record_routed_extent(
                        volume_index,
                        MemberExtent {
                            member_id,
                            physical_offset: cursor.saturating_add(run_start - logical_offset),
                            logical_offset: run_start,
                            len: run_end - run_start,
                        },
                    );
                }
                cursor = cursor.saturating_add(len);
            }
        }
    }

    /// The crypt rows the next checkpoint must carry, by member id (plan 136,
    /// E-D4). Empty for a set with no encrypted member.
    pub(crate) fn member_crypt_snapshots(&self) -> BTreeMap<u32, crypt::MemberCryptSnapshot> {
        let mut rows = BTreeMap::new();
        for (member_id, member) in &self.members {
            let Some(state) = member.crypt.as_ref() else {
                continue;
            };
            let uses_mac = self
                .layout_index_for_member(*member_id)
                .and_then(|index| self.layout_members().get(index))
                .is_some_and(|layout| layout.data_hash_uses_mac);
            if let Some(row) = state.snapshot(uses_mac) {
                rows.insert(*member_id, row);
            }
        }
        rows
    }

    /// Seeds one restored member's crypt state from its checkpoint row (plan
    /// 136, E-D4).
    ///
    /// Both directions are a refusal, because both are the same mistake seen
    /// from opposite sides: a row with facts the rebuilt layout does not state
    /// would rebuild a key against the wrong IV or the wrong salt — and every
    /// gate would go on passing, over ciphertext — while a row *missing* for a
    /// member this run classified encrypted means the checkpoint was written by
    /// something that did not know the member was encrypted at all. Demoting
    /// costs a materialization from bytes already on disk. Trusting either one
    /// costs the file.
    pub(crate) fn restore_member_crypt(
        &mut self,
        relative_partial: &str,
        stored: Option<&crypt::MemberCryptSnapshot>,
    ) -> Result<(), DemotionReason> {
        let member_id = self.members.iter().find_map(|(member_id, member)| {
            (member.relative_partial == relative_partial).then_some(*member_id)
        });
        let Some(member_id) = member_id else {
            // Not a member of this run's layout: the claim is dropped by the
            // caller's re-keying, which is the established behaviour.
            return Ok(());
        };
        let uses_mac = self
            .layout_index_for_member(member_id)
            .and_then(|index| self.layout_members().get(index))
            .is_some_and(|layout| layout.data_hash_uses_mac);
        let member = self
            .members
            .get_mut(&member_id)
            .expect("the member was just located");
        match (member.crypt.as_mut(), stored) {
            (None, None) => Ok(()),
            (Some(crypt), Some(stored)) => match crypt.restore(stored, uses_mac) {
                Ok(()) => Ok(()),
                Err(_) => Err(self.fail(DemotionReason::EncryptedFactsDisagree)),
            },
            _ => Err(self.fail(DemotionReason::EncryptedFactsDisagree)),
        }
    }

    /// The volumes the router holds parsed facts for — the volumes whose bytes
    /// it can classify. The restore seam validates a checkpoint's claims against
    /// this (M5).
    pub(crate) fn fact_volumes(&self) -> std::collections::HashSet<u32> {
        self.volume_facts.keys().copied().collect()
    }

    /// Whether any member is still carrying restart-seeded, unverified coverage.
    pub(crate) fn has_restart_seeded_coverage(&self) -> bool {
        self.members
            .values()
            .any(|member| !member.restart_seeded.is_empty())
    }

    /// The runs of member partials that must be re-read from disk before the
    /// whole-member gates can compose (D6's "PAR2 absent" arm).
    ///
    /// Split at part boundaries, because the composition is per part, and
    /// returned in `(member, ascending offset)` order so the caller's read is one
    /// forward pass per file rather than a seek per run.
    pub(crate) fn restart_read_plan(&self) -> Vec<RestartReadRun> {
        self.reread_plan(|member| &member.restart_seeded)
    }

    /// Whether any member is carrying a repair's stale composition gaps (D4).
    pub(crate) fn has_stale_gaps(&self) -> bool {
        self.members
            .values()
            .any(|member| !member.stale_gaps.is_empty())
    }

    /// The runs a repair left composed by nothing, in the same shape
    /// [`Self::restart_read_plan`] produces — the two are the same problem
    /// (covered bytes with no value in this process) reached from two
    /// directions, so they share a reader and a re-arm.
    pub(crate) fn stale_gap_read_plan(&self) -> Vec<RestartReadRun> {
        self.reread_plan(|member| &member.stale_gaps)
    }

    fn reread_plan(&self, pick: impl Fn(&MemberRouting) -> &ByteRanges) -> Vec<RestartReadRun> {
        let mut plan = Vec::new();
        for member_id in &self.member_order {
            let Some(member) = self.members.get(member_id) else {
                continue;
            };
            let ranges = pick(member);
            if ranges.is_empty() {
                continue;
            }
            let boundaries = self.part_boundaries(*member_id);
            for &(start, end) in ranges.ranges() {
                let mut cursor = start;
                while cursor < end {
                    let stop = boundaries
                        .iter()
                        .copied()
                        .find(|boundary| *boundary > cursor)
                        .unwrap_or(end)
                        .min(end);
                    plan.push(RestartReadRun {
                        member_id: *member_id,
                        relative_partial: member.relative_partial.clone(),
                        logical_offset: cursor,
                        len: stop - cursor,
                    });
                    cursor = stop;
                }
            }
        }
        plan
    }

    /// Exclusive logical end offsets of every part of a member's chain.
    fn part_boundaries(&self, member_id: u32) -> Vec<u64> {
        let Some(layout_index) = self.layout_index_for_member(member_id) else {
            return Vec::new();
        };
        let Some(member) = self.layout_members().get(layout_index) else {
            return Vec::new();
        };
        member
            .parts
            .iter()
            .map(|part| {
                part.logical_offset
                    .unwrap_or(0)
                    .saturating_add(part.data_size)
            })
            .collect()
    }

    /// Feeds one re-read run's CRC32 back into the member's composition and
    /// clears it from the restart-seeded set.
    ///
    /// This is the whole re-arm: the value comes from the bytes **on disk now**,
    /// so corruption introduced while the process was down fails the member gate
    /// exactly as a bad article would have.
    ///
    /// # Cannot-locate demotes (M4)
    ///
    /// A run whose part the layout cannot place — the member is gone from the
    /// layout, no part covers the offset, the member's routing state has been
    /// dropped — used to return `Ok(())` and leave the seeded range in place.
    /// That reads as success to the caller and as *never verifiable* to
    /// [`Self::try_verify_member`], so the set neither finalizes nor demotes: it
    /// sits there being re-read on every completion check for the life of the
    /// job. None of these are runtime conditions — each one means the layout the
    /// read plan was built from is not the layout in front of us — so each one
    /// demotes and lets the conventional path have the set.
    pub(crate) fn note_restored_member_crc(
        &mut self,
        member_id: u32,
        logical_offset: u64,
        len: u64,
        crc: u32,
    ) -> Result<(), DemotionReason> {
        let Some(layout_index) = self.layout_index_for_member(member_id) else {
            return Err(self.fail(DemotionReason::RestartRearmUnplaceable));
        };
        let part = self
            .layout_members()
            .get(layout_index)
            .and_then(|member| {
                member.parts.iter().enumerate().find(|(_, part)| {
                    let start = part.logical_offset.unwrap_or(0);
                    logical_offset >= start && logical_offset < start.saturating_add(part.data_size)
                })
            })
            .map(|(position, part)| {
                (
                    position as u32,
                    part.logical_offset.unwrap_or(0),
                    part.data_size,
                    part.packed_crc32,
                )
            });
        let Some((part_position, part_logical_offset, part_len, packed_crc32)) = part else {
            return Err(self.fail(DemotionReason::RestartRearmUnplaceable));
        };
        let Some(member) = self.members.get_mut(&member_id) else {
            return Err(self.fail(DemotionReason::RestartRearmUnplaceable));
        };
        // An encrypted member's re-read produces **plaintext** — that is what is
        // in the partial — so it feeds layer 2's member-wide composition and
        // nothing else (plan 136, E-D3). It must not touch `parts`, whose values
        // are cipher CRCs, and it must not be compared against the part's packed
        // hash, which describes cipher bytes this process no longer has. The
        // keyed member fold is what re-verifies the run, exactly as D6 intends:
        // the value comes from the bytes on disk now.
        if let Some(crypt) = member.crypt.as_mut() {
            crypt.plain_runs_mut().overwrite(logical_offset, len, crc);
            member.restart_seeded = subtract(&member.restart_seeded, logical_offset, len);
            member.stale_gaps = subtract(&member.stale_gaps, logical_offset, len);
            return self.try_verify_member(member_id);
        }
        // `overwrite`, not `insert`: a stale gap is by construction a *fragment*
        // of a run a repair discarded, and a plain insert would refuse it as
        // overlapping if any neighbour survived. Its own gaps are empty by
        // construction — the run it replaces was already removed — so this
        // cannot cascade.
        let gaps = member.parts.entry(part_position).or_default().overwrite(
            logical_offset.saturating_sub(part_logical_offset),
            len,
            crc,
        );
        debug_assert!(
            gaps.is_empty(),
            "re-reading member {member_id} at {logical_offset} left new stale gaps behind"
        );
        member.restart_seeded = subtract(&member.restart_seeded, logical_offset, len);
        member.stale_gaps = subtract(&member.stale_gaps, logical_offset, len);

        let part_value = member
            .parts
            .get(&part_position)
            .and_then(|runs| runs.compose(0, part_len));
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

    /// Files one emitted member extent into the volume's routing history,
    /// coalescing it with the extent it continues (B1).
    ///
    /// A physical byte is routed at most once *by ordinary routing* —
    /// [`VolumeStaging::stage`] never re-stages a routed range — but a PAR2
    /// repair re-routes bytes the history already holds
    /// ([`VolumeStaging::stage_repaired`]), so the parts already recorded are
    /// subtracted before anything is filed. The history stays disjoint, and a
    /// repair that also fills a range the set never routed (a slice lost to a
    /// missing article) still records that part.
    ///
    /// The subtraction is gated behind an **overlap pre-check**, because this
    /// runs once per emitted member run for the whole life of every set and the
    /// overlapping case is only ever a repair: without the gate, every ordinary
    /// article paid a `Vec` the length of the volume's extent history for a
    /// subtraction that removes nothing (phase 6 review, F7). The history is
    /// sorted by physical offset and disjoint, so its ends are monotonic too and
    /// one `partition_point` finds the first extent that could overlap.
    fn record_routed_extent(&mut self, volume_index: u32, extent: MemberExtent) {
        if extent.len == 0 {
            return;
        }
        let end = extent.physical_offset.saturating_add(extent.len);
        let overlapping = self
            .routed_extents
            .get(&volume_index)
            .map(|extents| {
                let first = extents.partition_point(|held| {
                    held.physical_offset.saturating_add(held.len) <= extent.physical_offset
                });
                &extents[first..]
            })
            .filter(|extents| {
                extents
                    .first()
                    .is_some_and(|held| held.physical_offset < end)
            });
        let held: Vec<(u64, u64)> = overlapping
            .map(|extents| {
                extents
                    .iter()
                    .map(|held| (held.physical_offset, held.physical_offset + held.len))
                    .collect()
            })
            .unwrap_or_default();
        if !held.is_empty() {
            // Re-routing a physical byte to a *different* destination is not a
            // shape this history can express: the overlapping part is subtracted
            // rather than corrected, so the old destination silently survives.
            // Nothing can produce it — a repair rewrites bytes, never the layout
            // that placed them, and a layout rebuild that moved a member would
            // have demoted the set — so it is asserted rather than handled.
            debug_assert!(
                self.routed_extents
                    .get(&volume_index)
                    .into_iter()
                    .flatten()
                    .filter(|old| {
                        old.physical_offset < end
                            && old.physical_offset.saturating_add(old.len) > extent.physical_offset
                    })
                    .all(|old| {
                        // The offset delta as a *signed* quantity: a member's
                        // logical offset routinely sits below the physical one
                        // (the volume's header comes first), so an unsigned
                        // `checked_sub` would answer `None` on both sides and
                        // make the comparison vacuously true — which is the one
                        // thing an assertion must never be.
                        old.member_id == extent.member_id
                            && i128::from(old.logical_offset) - i128::from(old.physical_offset)
                                == i128::from(extent.logical_offset)
                                    - i128::from(extent.physical_offset)
                    }),
                "volume {volume_index} re-routed the bytes at {} to a different member \
                 destination than the history already holds for them",
                extent.physical_offset
            );
            let mut cursor = extent.physical_offset;
            let mut fresh = Vec::new();
            for (start, stop) in held {
                if stop <= cursor {
                    continue;
                }
                if start >= end {
                    break;
                }
                if start > cursor {
                    fresh.push((cursor, start.min(end)));
                }
                cursor = cursor.max(stop);
                if cursor >= end {
                    break;
                }
            }
            if cursor < end {
                fresh.push((cursor, end));
            }
            for (start, stop) in fresh {
                self.record_fresh_extent(
                    volume_index,
                    MemberExtent {
                        member_id: extent.member_id,
                        physical_offset: start,
                        logical_offset: extent
                            .logical_offset
                            .saturating_add(start - extent.physical_offset),
                        len: stop - start,
                    },
                );
            }
            return;
        }
        self.record_fresh_extent(volume_index, extent);
    }

    /// [`Self::record_routed_extent`] once the range is known to be new.
    fn record_fresh_extent(&mut self, volume_index: u32, extent: MemberExtent) {
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
        // Ordering, stated exactly: `classify` reaches `Compressed` only after
        // the parse-level malformed reason, directory, redirection, encrypted
        // and solid have all been ruled out, so this arm may rely on those five
        // and on nothing else. The chain's *size* and *completeness* checks —
        // the `ExceedsDeclaredSize`, `SizeMismatch` and `MissingUnpackedSize`
        // malformed reasons — come after it and are therefore **not** implied
        // here: a member reaching this arm is not proven to be a well-formed
        // chain. That is why both declared totals stay `Option` and why the
        // caller re-checks the budget at every parse instead of waiting for
        // `totals_final`.
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

/// One run of a member's `.direct.partial` that restart seeded and the
/// finalization re-read must recompute (D6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RestartReadRun {
    pub(crate) member_id: u32,
    pub(crate) relative_partial: String,
    pub(crate) logical_offset: u64,
    pub(crate) len: u64,
}

/// The byte length of any mapped slice, whatever it maps to.
///
/// Its exhaustiveness was, until plan 136 E1, the *only* compile-time guard over
/// [`DirectSetRouter::restore_volume_coverage`]'s refutable `let ... else`: add
/// an arm here and that site went quiet forever. The restore now walks an
/// exhaustive match of its own and no longer depends on it — it still calls this
/// for the arms it skips, deliberately, so a future variant lands as an error in
/// both places rather than one.
fn slice_len(slice: &MappedSlice) -> u64 {
    match slice {
        MappedSlice::Member { len, .. }
        | MappedSlice::EncryptedMember { len, .. }
        | MappedSlice::Envelope { len }
        | MappedSlice::Unroutable { len } => *len,
    }
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
