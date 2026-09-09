//! The range router.
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
//! - **Confirming**, once the volume's trailing bytes have arrived: the walk
//!   now reaches the end-of-archive record. If it finds a member the
//!   provisional parse did not, that member is **adopted** — the layout is
//!   rebuilt from every volume's newest facts, because
//!   [`StoredLayoutBuilder::add_volume`] refuses a differing re-add and has no
//!   removal API. The first shape demoted here instead, which was safe (the
//!   file was not lost, it was refetched) but cost the whole set. A parse whose
//!   members are *not* an extension of the previous one is a real disagreement
//!   and still demotes.
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

use unrar_rs::{
    ArchiveFormat, IneligibilityReason, MappedSlice, MemberEligibility, RarVolumeFacts,
    StoredLayoutBuilder, StoredLayoutError,
};

use super::ByteRanges;
use super::plan::DirectSetPlan;
use super::sparse::SparseMarking;
use crypt::{
    AES_BLOCK, CryptRefusal, HeaderCryptRefusal, HeaderKeyRing, KeyRing, MemberCrypt, block_ceil,
    block_floor,
};

pub(crate) mod crypt;

/// Default RAM ceiling for holds across one set. A breach pages to the set's
/// holds scratch; only a paging failure demotes.
///
/// Per set. The process-wide sum is bounded separately, by the
/// [`super::accountant::HoldsAccountant`] every set charges to, whose limit
/// follows the host's memory.
pub(crate) const DEFAULT_HOLDS_BUDGET_BYTES: u64 = 64 * 1024 * 1024;

/// The **explicit** scratch ceiling, counted against the disk acceptance target
/// rather than derived from RAM the way the oracle's auto 4×-RAM rule is.
///
/// **Per archive set, not per job or per process.** Each set owns one scratch
/// file and one [`HoldsScratch`] carrying its own copy of this number: the
/// ceiling exists to stop one pathological set from filling the disk. The
/// aggregate — every set's scratch together, and the free space the working
/// directory's filesystem must keep — is bounded by the
/// [`super::accountant::HoldsAccountant`] every set charges to, which a spill
/// consults before it is written.
///
/// This is the fallback when no per-set override applies. The environment
/// override is resolved before a router is constructed, so every set still owns
/// one fixed ceiling for its lifetime.
pub(crate) const HOLDS_SCRATCH_CEILING_BYTES: u64 = 1024 * 1024 * 1024;

/// How much of a migrated member one envelope span carries. The whole
/// migration is bounded by [`MIGRATION_CEILING_BYTES`], so this only bounds the
/// *individual* allocation and lets the write fan out across the disk owner
/// threads the way ordinary routing does.
const MIGRATION_SPAN_BYTES: u64 = 4 * 1024 * 1024;

/// The most routed bytes one member migration may move back into the
/// envelopes.
///
/// A migration reads every extent the adopted member routed into its
/// `.direct.partial` and parks them in memory as [`RoutedSpan`]s until the
/// entry point on the stack drains them, so the member's whole routed size is
/// read synchronously on the parsing task and held at once. The tolerance
/// itself no longer caps a member's size — a compressed member is ineligible
/// from its first header, so it is never adopted and never migrates — but an
/// adopted BLAKE2sp-only member that resolves ineligible at chain close can be
/// as large as the set, and moving it would hold that much memory on the hot
/// path. Over this, the member keeps the answer it had before migration
/// existed: its own ineligibility demotes the set.
pub(crate) const MIGRATION_CEILING_BYTES: u64 = 64 * 1024 * 1024;

/// How much genuine **prefix** — contiguous coverage from offset zero, the
/// only bytes the header walk can consume — a volume may accumulate without a
/// successful provisional parse before the set is declared unroutable. Real
/// RAR headers are a few hundred bytes; this only exists so a set whose first
/// article is entirely payload (a corrupted or non-RAR file that reached us
/// classified as a volume) demotes instead of holding forever.
///
/// Measured against the zero-prefix, **never** against total staged bytes. A
/// 12-connection download delivers a volume's articles in whatever order they
/// complete, so several mid-file articles routinely land before the one
/// carrying offset zero — and judging their sum declared real sets unparsable
/// with their headers still unread: every large volume whose first article
/// arrived after ~6 siblings demoted `unparsable_volume`, which mislabeled
/// compressed sets and would demote a store-method set direct routing exists
/// to carry.
pub(crate) const MAX_HEADER_PREFIX_BYTES: u64 = 4 * 1024 * 1024;

/// Why a set left direct mode. Every variant is its own metric bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DemotionReason {
    /// A member the layout classified `Ineligible` for a reason the
    /// member tolerance does not cover — including a provisional member
    /// that resolved ineligible when its chain closed.
    ///
    /// Solid, redirection, no-checksum and malformed-chain members always land
    /// here; see [`member_shape_is_tolerable`] for why each one is a *set*
    /// verdict rather than a per-member one. A *compressed* or *BLAKE2sp-only*
    /// member of any size instead rides the tolerance and is stream-extracted
    /// from the virtual volumes at finalization.
    ///
    /// A **directory** member reaches this reason only when the set has nothing
    /// else to route. Directory entries are dataless headers, so nothing is
    /// decoded for them: they are created — with their archive metadata — at
    /// finalization; demoting a 6 GiB folder-tree set for the directory
    /// headers its archiver appends to the last volume cost a full
    /// reconstruction and a full conventional extraction at the very end of an
    /// otherwise complete direct route.
    ///
    /// **Encrypted** members land here only when their encryption is not the
    /// one shape direct-store can route. `classify` sends a member
    /// whose parts are all `Store` and all state the same key material to
    /// [`MemberEligibility::EncryptedStore`] and the stored-chain path, and
    /// reserves `Ineligible(Encrypted)` for encrypted **and** compressed,
    /// encrypted **and** solid, and non-uniform or unkeyable encryption. An
    /// `EncryptedStore` member that fails admission demotes under
    /// [`Self::EncryptedMemberRefused`] instead, which says *why*.
    MemberIneligible(MemberIneligibility),
    /// An encrypted `Store` member the set may not route: no password reached
    /// it, the password its header states a check for is wrong, or its key
    /// material is one this build cannot derive from.
    ///
    /// Always a demotion, never a job failure: the conventional extractor asks
    /// the job's whole password-candidate list, which is a superset of what
    /// direct-store is handed, so demoting costs the direct route and nothing
    /// else. A *wrong* password demotes to a conventional path that will fail
    /// the same way, which is the parity this reason keeps.
    EncryptedMemberRefused(CryptRefusal),
    /// A **header-encrypted** (`-hp`) set whose headers this router may not open.
    ///
    /// `-hp` withholds the *layout*, not the *keying facts*: RAR5's type-4
    /// record is plaintext, so a password candidate can be proved against the
    /// archive's own check before a header is decrypted. This is what fires when
    /// no candidate can be proved — no candidates, none that verify, no check to
    /// prove them against, RAR4 (which has no check anywhere in the format), or
    /// key material over the library's KDF ceiling.
    ///
    /// A demotion, and the older floor exactly: the volume materializes
    /// byte-exactly and the conventional extractor opens it with the job's
    /// whole candidate list. It is raised at the **first header parse**, which
    /// is what stops such a volume from also burning
    /// [`MAX_HEADER_PREFIX_BYTES`] of staging on its way to
    /// [`Self::UnparsableVolume`].
    HeaderEncryptedRefused(HeaderCryptRefusal),
    /// A checkpoint's crypt facts are not the facts the rebuilt layout states,
    /// or a member this run classified encrypted has no crypt row at all.
    ///
    /// Fail-closed by construction: the alternative is rebuilding a key from a
    /// row describing a different archive, which decrypts to garbage while every
    /// coverage gate keeps passing, because coverage is about *where* bytes are
    /// and says nothing about what they decrypt to.
    EncryptedFactsDisagree,
    /// An encrypted set that cannot reproduce its own posted bytes is about to
    /// be read as if it could.
    ///
    /// An earlier shape demoted **every** encrypted set in a PAR2-bearing job
    /// under this name, because nothing could turn a decrypted destination back
    /// into what was posted. The re-encrypting overlay retires that, and this
    /// reason narrows to the residual the overlay itself refuses:
    /// [`DirectSetRouter::posted_bytes_unavailable`] — an encrypted member with
    /// routed extents whose declared cipher size never arrived, or whose tail
    /// padding is not whole, so its final block has no byte-exact source.
    ///
    /// Still checked in front of the authoritative pass, and still a demotion
    /// rather than a fallback, for the reason [`Self::Par2Unbindable`] gives:
    /// the pass's world has to stay binary — a fully readable virtual set, or
    /// real files on disk — because a half-answerable set has the pass report
    /// damage that is not there and hand the repairer a volume nobody can write.
    EncryptedPostedBytesUnavailable,
    /// PAR2 verification found damage on one of the set's virtual volumes.
    ///
    /// Verification alone produces **verdicts** only: repairing a virtual
    /// volume means writing a repaired slice into a file that does not exist,
    /// which is what repair-while-direct does. Demoting materializes the
    /// volumes from the set's own routed bytes and hands them to the
    /// conventional repair path, which is exactly the shape a job with no
    /// direct set would have taken.
    Par2Damaged,
    /// PAR3 requires reconstruction before this direct archive can finalize.
    /// The conventional repair path currently owns installation for this set.
    Par3Damaged,
    /// One of the set's source volumes could not be bound, unambiguously, to a
    /// PAR2 description in the job's recovery set.
    ///
    /// An unbound volume cannot be served through the virtual-volume overlay —
    /// the overlay is keyed by PAR2 file id — so the authoritative pass would
    /// read it off a disk it is not on, report it missing, and hand the
    /// repairer a file to write into that does not exist. Demoting before the
    /// pass runs is what keeps that pass looking at either a *fully* bound
    /// virtual set or at real files, never at a half-bound one.
    Par2Unbindable,
    /// A member riding the member tolerance could not be extracted from
    /// the virtual volumes at finalization. The stored members are correct; the
    /// tolerated one is not produced, so the set is rebuilt the ordinary way.
    ToleratedExtractionFailed,
    /// Holds exceeded the RAM budget and paging could not bring it back — every
    /// pageable run is already in scratch and RAM is still over, which means one
    /// staged run is larger than the whole budget.
    HoldsBudgetExceeded,
    /// The holds scratch file could not be created, written or read.
    HoldsScratchFailed,
    /// Paging would push the holds scratch past its configured ceiling. Counted
    /// separately from the RAM budget because they say different things: this
    /// one is the *disk* claim direct-store makes against its own 1.05×
    /// acceptance target.
    HoldsScratchCeiling,
    /// Paging would leave the working directory's filesystem with less than
    /// the free space it must keep. Named apart from
    /// [`Self::HoldsScratchCeiling`] because the disk, not this set's holds,
    /// is what ran out — and the set that asked is simply the one that asked
    /// last.
    HoldsScratchDiskReserve,
    /// A confirming parse disagreed with the provisional one, or a volume was
    /// re-added with facts that are not an extension of what it stated before.
    ConflictingVolumeFacts,
    /// A volume's headers, as the library reported them, are not the headers a
    /// **physical** walk of the same image finds — so they came, wholly or
    /// partly, from the archive's Quick Open cache.
    ///
    /// The cache is a listing optimization the RAR spec explicitly says can be
    /// crafted to disagree with the real archive, and direct-store's parse is a
    /// routing decision: where posted bytes are written, and which members exist
    /// at all. A set whose two answers differ is refused rather than reconciled,
    /// because nothing in the format says which one is true.
    QuickOpenMismatch,
    /// A volume restored from a checkpoint finished downloading without ever
    /// being confirmed, so its trailing region could never be classified.
    ///
    /// Its pre-restart bytes live on disk rather than in the staged image, so
    /// the confirming parse has a hole from offset zero and cannot succeed. The
    /// alternative to demoting is holding the volume's end record and recovery
    /// record for the life of the set, which reads as PAR2 damage and costs a
    /// full redownload — this costs a materialization from bytes already on
    /// disk.
    UnconfirmedRestoredVolume,
    /// A restart-seeded run could not be placed back into the layout it was
    /// planned against, so its member gate can never be re-armed.
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
    /// A PAR2-repaired span could not be routed back into the set.
    ///
    /// The repair itself succeeded — the materialized volume is correct — but
    /// the router could not place its bytes: the layout maps part of the span
    /// to nothing, or a destination write for it failed. Demoting here is safe
    /// and cheap, because the repaired bytes that *were* routed are already in
    /// the partials and the composition was overwritten with them, so
    /// reconstruction rebuilds the repaired volume rather than the damaged one.
    RepairRerouteFailed,
    /// A stale composition gap left by a repair could not be re-read from the
    /// partial that holds it.
    ///
    /// A gap is bytes nothing currently vouches for, so leaving the member
    /// "verified" over one would pass a member on the strength of a value that
    /// describes different bytes. Unreadable means unverifiable, and
    /// unverifiable demotes.
    RepairGapUnreadable,
    /// A source volume arrived uuencoded.
    ///
    /// Sets are admitted from the NZB's filenames, before a single article has
    /// been decoded, so an archive posted in uuencode is admitted exactly like
    /// a yEnc one. It can never be routed: routing writes an article's bytes
    /// into a volume at the offset the article declares, and a uuencode article
    /// declares no offset — its position is the decoded length of its whole
    /// prefix, which only sequential assembly can supply.
    ///
    /// A demotion rather than a quiet exclusion, because a set that is merely
    /// starved never finalizes and never demotes, and its volumes keep
    /// answering `is_direct_source_file` — which suppresses the archive probe
    /// that dispatches extraction. The job would then complete with its archive
    /// sitting unextracted on disk.
    UuencodedSourceVolume,
    /// The volume's headers could not be parsed from the staged image.
    UnparsableVolume,
    /// A non-final part's packed CRC32 did not match the bytes routed for it.
    PartChecksumMismatch,
    /// The composed whole-member CRC32 did not match the final part's header.
    MemberChecksumMismatch,
    /// Two volumes of the set disagree about the archive format.
    FormatMismatch,
    /// The set's signature names a format direct-store does not route (RAR
    /// 1.4).
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
    /// One of the set's destinations could not be marked sparse. Raised
    /// **before** the file holds a hole, so demoting here is what keeps a
    /// Windows filesystem from allocating a whole volume's worth of zeros
    /// behind a member partial whose first routed byte lands near its end.
    SparseMarkFailed,
    /// Committing a verified set to its destinations failed. The bytes are
    /// good; the filesystem refused the rename, so the set is rebuilt the
    /// ordinary way rather than left half-committed.
    FinalizationFailed,
    /// An identity-admitted set's remaining roster volumes can no longer be
    /// claimed by any file.
    ///
    /// The roster is complete at admission — the recovery set describes every
    /// volume — but the file mapping is only established as each file's first
    /// decoded bytes match a described fingerprint. A file whose bytes took the
    /// conventional path before it could be matched, or whose matching window
    /// never arrived intact, leaves its volume unclaimable forever, and an
    /// identity set with an unclaimable volume is exactly a starved set: it
    /// never finalizes, never demotes on its own, and keeps suppressing the
    /// archive probe. Demoting hands the bound volumes back through the
    /// ordinary materialization and lets the conventional path — which is
    /// already writing the unclaimed files — own the whole set.
    IdentityRosterUnfillable,
    /// An identity-bound volume's own headers declare a different volume
    /// number than the binding assigned.
    ///
    /// The binding evidence is a content fingerprint, so a disagreement means
    /// the fingerprint and the headers describe different files — a hostile
    /// post, or identity metadata that lies about its own set. Either way the
    /// layout must not adopt members from the claimed position; demoting
    /// hands everything to the conventional path, whose extractor orders
    /// volumes by reading them.
    IdentityVolumeMismatch,
}

/// The ineligibility reasons this module distinguishes in metrics. The
/// library's [`IneligibilityReason`] carries byte counts no rule reads any
/// more, so this collapses it to the label.
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

/// One member the member tolerance carries to finalization.
///
/// Ordered by the same key the list is built with, so the derived `Ord` only
/// ever breaks a tie between two members at one archive position — which the
/// destination-collision rule has already refused.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ToleratedMember {
    /// Raw header name, exactly as the archive states it.
    pub(crate) name: String,
    /// The archive describes a directory: a dataless header finalization
    /// creates as a directory, never extracts and never writes a file for.
    pub(crate) is_directory: bool,
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

/// What a demoted set's consumers need under it: the virtual volumes the set
/// already has, or real files on disk.
///
/// The virtual volumes — each source volume's sparse envelope overlaid with the
/// member `.direct.partial`s that carried its payload away — are a
/// [`unrar_rs::VolumeProvider`], and the conventional extractor, PAR2
/// verification and repair-while-direct all already read through one. So a
/// demotion does **not** imply materialization by itself: it implies it only
/// when the image the overlay would serve is not the archive that was posted,
/// or when the consumer that has to run next cannot read an overlay at all.
///
/// This is the classification a demotion that keeps its virtual volumes would
/// switch on. Today every demotion materializes and this answer is *reported*
/// rather than acted on — see the `layout` field on the demotion warning and
/// the `direct_store.demoted.virtual`/`.real` probes — so the reasons are
/// classified, pinned by a test, and ready before any behaviour hangs off them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VolumeDemand {
    /// The layout the router learned is still the archive's layout and the
    /// overlay still answers for every byte it claims, so the set's remaining
    /// work can be done against the virtual volumes.
    Virtual,
    /// Real volume files are required: either the layout is wrong (or was never
    /// learned), or the overlay cannot answer for what it claims, or the
    /// consumer that runs next is filesystem-bound.
    Real,
}

impl VolumeDemand {
    /// Stable metric suffix.
    pub(crate) fn metric(self) -> &'static str {
        match self {
            Self::Virtual => "virtual",
            Self::Real => "real",
        }
    }
}

impl std::fmt::Display for VolumeDemand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.metric())
    }
}

impl DemotionReason {
    /// Whether this demotion needs real volume files under it, or could be
    /// served by the set's own virtual volumes.
    ///
    /// Exhaustive on purpose: a new reason must state its answer here rather
    /// than inherit one.
    pub(crate) fn volume_demand(self) -> VolumeDemand {
        match self {
            // The layout parsed, agreed with itself, and describes the archive.
            // One member's *shape* is something direct routing cannot carry —
            // that is a fact about the member, not about the image, and the
            // overlay serves the whole volume exactly as posted.
            Self::MemberIneligible(_) => VolumeDemand::Virtual,
            // The layout is known and the member's posted bytes are in the
            // envelope; what was refused is key material. The conventional
            // extractor asks the job's whole password-candidate list — a
            // superset of the one direct-store is handed — and it can ask it of
            // the overlay just as well as of a file.
            Self::EncryptedMemberRefused(_) => VolumeDemand::Virtual,
            // `-hp`: the headers themselves are sealed, so no layout was ever
            // learned and there is nothing to overlay. The volume must
            // materialize byte-exactly for the conventional extractor to open
            // it with the job's candidate list.
            Self::HeaderEncryptedRefused(_) => VolumeDemand::Real,
            // A checkpoint's crypt facts contradict the rebuilt layout. Nothing
            // here may be read as describing this archive, the layout included.
            Self::EncryptedFactsDisagree => VolumeDemand::Real,
            // The re-encrypting overlay itself refuses: a routed encrypted
            // member with no declared cipher size, or a tail that never arrived
            // whole. The virtual volume has a block with no byte-exact source,
            // so it is not the posted image.
            Self::EncryptedPostedBytesUnavailable => VolumeDemand::Real,
            // The layout is right and the *bytes* are not. The consumer is what
            // forces files here: the conventional `Par2Repairer` reads and
            // writes volumes through `DiskFileAccess`, which an overlay has
            // none of. `direct_store::repair` already repairs into the virtual
            // layout for a *live* set, and reusing it for a demoted one is what
            // would move this answer to `Virtual`.
            Self::Par2Damaged => VolumeDemand::Real,
            Self::Par3Damaged => VolumeDemand::Real,
            // The overlay `par2_access` presents is keyed by PAR2 file id, so a
            // volume with no unambiguous binding cannot be served through it at
            // all.
            Self::Par2Unbindable => VolumeDemand::Real,
            // The tolerated decode already failed against this exact image.
            // Handing the conventional extractor the same image to fail against
            // is not a fallback; real files are.
            Self::ToleratedExtractionFailed => VolumeDemand::Real,
            // Holds are staged, unrouted bytes: a budget or scratch failure
            // ends *routing* and says nothing about the layout or about the
            // bytes already placed.
            Self::HoldsBudgetExceeded
            | Self::HoldsScratchFailed
            | Self::HoldsScratchCeiling
            | Self::HoldsScratchDiskReserve => VolumeDemand::Virtual,
            // Two parses of one volume disagree. Nothing says which is true, so
            // no overlay built from either may be read as the archive.
            Self::ConflictingVolumeFacts => VolumeDemand::Real,
            // The library's headers and a physical walk of the same image
            // disagree — a Quick Open cache the format says may be crafted to
            // lie. Same verdict, same reason.
            Self::QuickOpenMismatch => VolumeDemand::Real,
            // The volume's trailing region was never classified, so its part of
            // the layout is incomplete and the overlay has no answer for it.
            Self::UnconfirmedRestoredVolume => VolumeDemand::Real,
            // A restart-seeded run could not be placed into the layout, or
            // could not be read back out of the partial that is supposed to
            // hold it. Either way the overlay would answer for a range nothing
            // vouches for.
            Self::RestartRearmUnplaceable | Self::RestartRereadFailed => VolumeDemand::Real,
            // A repaired span the router could not place, or a stale gap it
            // could not re-read: a hole inside the image with no source.
            Self::RepairRerouteFailed | Self::RepairGapUnreadable => VolumeDemand::Real,
            // A uuencode article declares no offset, so nothing was ever routed
            // and no overlay exists to read.
            Self::UuencodedSourceVolume => VolumeDemand::Real,
            // No layout at all.
            Self::UnparsableVolume | Self::UnsupportedFormat => VolumeDemand::Real,
            // The routed bytes contradict the archive's own checksums, or the
            // volume contradicts its yEnc trailer. The overlay would serve
            // bytes that are provably not what was posted.
            Self::PartChecksumMismatch | Self::MemberChecksumMismatch | Self::VolumeCrcMismatch => {
                VolumeDemand::Real
            }
            // Two volumes disagree about the archive format, so the layout is
            // not one archive's.
            Self::FormatMismatch => VolumeDemand::Real,
            // The image is truthful; what is refused is a *destination*. The
            // conventional extractor applies the same path validator and the
            // same collision rule, and it can apply them reading the overlay.
            Self::UnsafeDestination | Self::CollidingDestinations => VolumeDemand::Virtual,
            // A destination write failed, or a destination could not be marked
            // sparse. The overlay's member half is exactly those files, and the
            // path to them is the thing that is failing.
            Self::DestinationWriteFailed | Self::SparseMarkFailed => VolumeDemand::Real,
            // Finalization got part way: some members are renamed to their
            // destinations and the overlay's partial map points at paths that
            // are no longer there.
            Self::FinalizationFailed => VolumeDemand::Real,
            // An identity set with a volume no file can claim has no reader for
            // that volume; one whose headers contradict its binding has a
            // layout describing a different file.
            Self::IdentityRosterUnfillable | Self::IdentityVolumeMismatch => VolumeDemand::Real,
        }
    }

    /// Whether this demotion is *evidence that the posted bytes are wrong,
    /// found before any recovery set was asked* — as opposed to a reason
    /// direct routing could not be used.
    ///
    /// The distinction is what the completion gate needs and the metric label
    /// cannot give it: `holds_budget` says nothing about the archive, while a
    /// part CRC32 that does not match the bytes routed for it is a damaged
    /// volume established from the posting alone. A job carrying one of these
    /// must not be handed to the extractor on the strength of a *type* claim —
    /// a stored RAR set's "a clean decode would prove integrity" — because
    /// that claim has already been contradicted.
    ///
    /// [`Self::Par2Damaged`] is deliberately **not** here even though it is
    /// damage: it is raised *by* the authoritative pass, so the verdict the
    /// gate would wait for has already been given, and waiting for a second
    /// one is waiting forever — including for the set that pass could not
    /// repair, whose undamaged members the conventional path still delivers.
    ///
    /// Deliberately keyed on the damage rather than on any single reason: the
    /// gate has to keep answering correctly as reasons are added, retired, or
    /// (as with an in-place repair of a mismatched part) stop demoting at all.
    pub(crate) fn is_source_damage(self) -> bool {
        matches!(
            self,
            Self::PartChecksumMismatch | Self::MemberChecksumMismatch | Self::VolumeCrcMismatch
        )
    }

    /// Stable metric label. `sets == direct + materialized + mixed` is worth
    /// asserting against these.
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
            Self::HeaderEncryptedRefused(refusal) => refusal.metric(),
            Self::EncryptedFactsDisagree => "encrypted_facts_disagree",
            Self::EncryptedPostedBytesUnavailable => "encrypted_posted_bytes_unavailable",
            Self::Par2Damaged => "par2_damaged",
            Self::Par3Damaged => "par3_damaged",
            Self::Par2Unbindable => "par2_unbindable",
            Self::ToleratedExtractionFailed => "tolerated_extraction_failed",
            Self::HoldsBudgetExceeded => "holds_budget",
            Self::HoldsScratchFailed => "holds_scratch_io",
            Self::HoldsScratchCeiling => "holds_scratch_ceiling",
            Self::HoldsScratchDiskReserve => "holds_scratch_disk_reserve",
            Self::ConflictingVolumeFacts => "conflicting_volume_facts",
            Self::QuickOpenMismatch => "quick_open_mismatch",
            Self::UnconfirmedRestoredVolume => "unconfirmed_restored_volume",
            Self::RestartRearmUnplaceable => "restart_rearm_unplaceable",
            Self::RestartRereadFailed => "restart_reread_failed",
            Self::RepairRerouteFailed => "repair_reroute_failed",
            Self::RepairGapUnreadable => "repair_gap_unreadable",
            Self::UuencodedSourceVolume => "uuencoded_source_volume",
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
            Self::IdentityRosterUnfillable => "identity_roster_unfillable",
            Self::IdentityVolumeMismatch => "identity_volume_mismatch",
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
/// demand with [`weaver_yenc::crc32_combine`].
///
/// The runs are kept exactly as they were fed — **never merged**. The first
/// shape coalesced adjacent neighbours into one value, which answered "is this
/// whole space composed" in one comparison and answered nothing else: a covered
/// range that stopped short of a merged run's end — a held tail, a volume whose
/// last article never came, a prefix under a reconstruction floor — had no
/// reference value at all and was written unverified. Keeping the atoms means
/// any sub-range that starts and ends on an atom boundary can be composed,
/// which is every range the coverage map can name for an article that was
/// wholly routed.
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
    /// article must not advance the composition twice.
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

    /// The durable coverage whose complete article-shaped CRC atoms can be
    /// composed during reconstruction.
    ///
    /// Physical placement may stop inside an atom when routing demotes after
    /// writing only part of the current article. That partial range remains
    /// provisional: the conventional handoff (or a targeted requeue) owns it.
    /// Publishing it as reconstructed would either require inventing a
    /// sub-article reference CRC or weaken [`Self::compose`]'s exactness.
    pub(crate) fn materializable_coverage(&self, available: &ByteRanges) -> ByteRanges {
        let ranges = available.ranges();
        let mut materializable = ByteRanges::default();
        let mut range_index = 0usize;

        for &(run_start, run_len, _) in &self.runs {
            let run_end = run_start.saturating_add(run_len);
            while range_index < ranges.len() && ranges[range_index].1 <= run_start {
                range_index += 1;
            }
            let Some(&(range_start, range_end)) = ranges.get(range_index) else {
                break;
            };
            if range_start <= run_start && run_end <= range_end {
                materializable.insert(run_start, run_len);
            }
        }

        materializable
    }

    /// Replaces every run overlapping `[start, start + len)` with a single run
    /// for the rewritten span, and returns the sub-ranges of the discarded runs
    /// that fall **outside** it — the stale gaps.
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
    /// checksum, and the reconstruction's verification is what stands between a
    /// rebuilt volume and a published floor over bytes nothing checked.
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
            composed = weaver_yenc::crc32_combine(composed, run_crc, run_len);
            cursor = run_end;
            index += 1;
        }
        Some(composed)
    }

    /// The longest prefix of `[start, start + len)` the runs tile exactly, as
    /// `(prefix_len, composed)`.
    ///
    /// `None` means no run starts at `start`, so not a byte of the range has a
    /// reference. A zero-length prefix is a real answer: the run at `start` is
    /// longer than the range, so the whole range is a proper prefix of one atom.
    /// [`Self::compose`] is this with the prefix required to be the whole range.
    pub(crate) fn compose_prefix(&self, start: u64, len: u64) -> Option<(u64, u32)> {
        if len == 0 {
            return None;
        }
        let end = start.checked_add(len)?;
        let mut index = self
            .runs
            .partition_point(|(run_start, _, _)| *run_start < start);
        let (first_start, _, _) = *self.runs.get(index)?;
        if first_start != start {
            return None;
        }
        let mut cursor = start;
        let mut composed = 0u32;
        while cursor < end {
            let Some(&(run_start, run_len, run_crc)) = self.runs.get(index) else {
                break;
            };
            if run_start != cursor {
                break;
            }
            let run_end = run_start.checked_add(run_len)?;
            if run_end > end {
                break;
            }
            composed = weaver_yenc::crc32_combine(composed, run_crc, run_len);
            cursor = run_end;
            index += 1;
        }
        Some((cursor - start, composed))
    }

    /// The run that starts exactly at `offset`, as `(start, len)`.
    pub(crate) fn run_starting_at(&self, offset: u64) -> Option<(u64, u64)> {
        let index = self
            .runs
            .partition_point(|(run_start, _, _)| *run_start < offset);
        let &(run_start, run_len, _) = self.runs.get(index)?;
        (run_start == offset).then_some((run_start, run_len))
    }
}

/// One staged run, in RAM or paged out to the set's holds scratch.
///
/// A scratch region is **write-once**: an offset handed out here reads back
/// with a single positioned read, no locking and no re-validation, for as long
/// as the image it was taken from exists. The one thing that moves a region is
/// compaction, which rewrites the router's own index in the same call — and
/// which, while a reader holds a [`HoldsScratchPin`] on the image, relocates
/// into a fresh file rather than rewriting the one the reader is on.
#[derive(Debug, Clone)]
enum StagedChunk {
    Memory(std::sync::Arc<[u8]>),
    Scratch { offset: u64, len: u64 },
}

/// One region of the holds scratch that is still read from, and the staging
/// slot whose offset has to follow it when the scratch is compacted.
struct LiveScratchExtent {
    scratch_offset: u64,
    len: u64,
    volume_index: u32,
    chunk_offset: u64,
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

/// The per-set holds scratch file.
///
/// Append-only and write-once per region, with an in-memory index that lives in
/// the staging map: a paged chunk *is* its `(offset, len)`. There is no free
/// list. Space that placed holds leave behind is reclaimed only by
/// [`Self::compact`], and only when an append would otherwise breach the
/// ceiling — the bound is stated rather than hidden: **scratch never exceeds
/// the live holds plus what compaction has not yet reclaimed**, and the ceiling
/// below is what keeps that from being unbounded.
///
/// A reader that keeps offsets past the call that handed them out takes a
/// [`HoldsScratchPin`]. The pin is what makes those offsets safe to keep:
/// compaction under a pin relocates into a fresh file and leaves the pinned
/// image untouched, so the reader's offsets stay true for the image it holds.
#[derive(Debug)]
pub(crate) struct HoldsScratch {
    path: std::path::PathBuf,
    file: Option<std::sync::Arc<std::fs::File>>,
    /// Append cursor, and the file's length.
    len: u64,
    ceiling: u64,
    /// The sparse marker. The scratch is append-only so it holds no hole of its
    /// own, but it is a direct-store-created file in the working directory and
    /// it inherits the same rule: marked at creation, before a byte is written,
    /// and a marking failure demotes rather than proceeding.
    sparse: SparseMarking,
    /// Live pins over the **current** image. Replaced, not reset, whenever the
    /// image is: the pins of a relocated-away image keep decrementing their own
    /// counter, and the fresh file starts unpinned.
    pins: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Images a pinned compaction relocated away from, still allocated on disk
    /// for as long as their readers hold them. Their bytes stay charged to the
    /// process-wide accountant until the last pin drops — they are real disk
    /// the scratch ceiling and the reserve were promised to bound — and are
    /// forgotten at the next publication after that.
    retired: Vec<RetiredScratchImage>,
}

/// One relocated-away scratch image: its pin counter, and the bytes it holds
/// on disk until that counter reaches zero.
#[derive(Debug)]
struct RetiredScratchImage {
    pins: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    bytes: u64,
}

/// A reader's hold on one scratch image: the handle, and the promise that the
/// offsets handed out against that image stay true while the pin lives.
///
/// The provider carries one of these inside every scratch-backed held run so
/// that PAR2 can read holds on demand, positionally, instead of the router
/// copying every hold into RAM at provider construction — which is what let a
/// set's holds bypass the budget the scratch exists to enforce. Two things can
/// happen to the image underneath a pin, and both are safe: compaction
/// relocates into a fresh file and leaves this one alone, and `discard`
/// unlinks the path while the handle keeps the bytes readable until the last
/// pin drops.
#[derive(Debug)]
pub(crate) struct HoldsScratchPin {
    file: std::sync::Arc<std::fs::File>,
    pins: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl HoldsScratchPin {
    /// Positioned read from the pinned image; a short file is an I/O error,
    /// never a hole, because a region handed out was written in full.
    pub(crate) fn read_at(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        read_at(&self.file, offset, out)
    }
}

impl Drop for HoldsScratchPin {
    fn drop(&mut self) {
        self.pins.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// The path a pinned compaction packs into before taking over `path`. It
/// keeps the holds-scratch prefix, so a copy a crash leaves behind is swept at
/// restart exactly like the scratch itself.
fn compacting_scratch_path(path: &std::path::Path) -> std::path::PathBuf {
    scratch_sibling_path(path, "compacting")
}

/// The path the pinned image is moved aside to while the packed copy takes
/// over `path`. Same prefix, same sweep, for the same reason.
fn retired_scratch_path(path: &std::path::Path) -> std::path::PathBuf {
    scratch_sibling_path(path, "retired")
}

fn scratch_sibling_path(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    path.with_file_name(format!("{name}.{suffix}"))
}

/// Puts `packed` at `path` while a reader still holds the file that is there.
///
/// Not a rename over the path. A pinned image is an *open* file, and on
/// Windows an open file cannot be replaced, only moved or unlinked — both of
/// which its share-delete handle allows. So the pinned image steps aside
/// first, the packed copy takes the path, and the retired image is unlinked
/// last: immediately on POSIX, and on Windows the moment its last handle
/// closes, which is what the pin is. A failure between the two moves puts the
/// image back where it was, so the caller sees the file it had.
fn take_over_scratch_path(
    packed: &std::path::Path,
    path: &std::path::Path,
    retired: &std::path::Path,
) -> std::io::Result<()> {
    std::fs::rename(path, retired)?;
    if let Err(error) = std::fs::rename(packed, path) {
        let _ = std::fs::rename(retired, path);
        return Err(error);
    }
    if let Err(error) = std::fs::remove_file(retired) {
        // The path keeps the scratch prefix, so a restart sweeps it; nothing
        // reads it by name in the meantime.
        tracing::debug!(
            retired_path = %retired.display(),
            error = %error,
            "direct-store could not unlink a retired holds scratch image"
        );
    }
    Ok(())
}

/// Creates (or truncates) a scratch file, read/write, marked sparse before a
/// byte is written. On Windows it is opened share-delete: `discard` unlinks
/// the scratch under live pins, and a pinned compaction renames its packed copy
/// over the path, and neither is allowed against a handle opened without it.
fn open_scratch_file(
    path: &std::path::Path,
    sparse: &SparseMarking,
) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(true).read(true).write(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE
        options.share_mode(0x1 | 0x2 | 0x4);
    }
    let file = options.open(path)?;
    if let Err(error) = super::sparse::SparseMarker::mark_sparse(sparse, &file) {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(error);
    }
    Ok(file)
}

/// Copies `len` bytes from `src` at `src_offset` to `dst` at `dst_offset`,
/// front to back in bounded slices. `src` and `dst` may be the same file: an
/// in-place pack only ever moves a region toward the front, and front-to-back
/// order keeps the destination behind the not-yet-read source.
fn copy_scratch_region(
    src: &std::fs::File,
    src_offset: u64,
    dst: &std::fs::File,
    dst_offset: u64,
    len: u64,
) -> Option<()> {
    const COPY_SLICE_BYTES: u64 = 1024 * 1024;
    let mut copied = 0u64;
    while copied < len {
        let take = COPY_SLICE_BYTES.min(len - copied);
        let mut buffer = vec![0u8; take as usize];
        read_at(src, src_offset.saturating_add(copied), &mut buffer).ok()?;
        write_at(dst, dst_offset.saturating_add(copied), &buffer).ok()?;
        copied += take;
    }
    Some(())
}

impl HoldsScratch {
    pub(super) fn new(path: std::path::PathBuf, ceiling: u64) -> Self {
        Self {
            path,
            file: None,
            len: 0,
            ceiling,
            sparse: SparseMarking::default(),
            pins: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            retired: Vec::new(),
        }
    }

    /// The current image's length: what this set's own ceiling bounds.
    pub(super) fn bytes(&self) -> u64 {
        self.len
    }

    /// What this scratch occupies on disk: the current image, plus every
    /// relocated-away image a reader still pins. This is the figure the
    /// process-wide accountant is told, because it is the disk that is
    /// actually in use; the images whose last pin has dropped are forgotten
    /// here, and their bytes with them.
    pub(super) fn charged_bytes(&mut self) -> u64 {
        self.retired
            .retain(|image| image.pins.load(std::sync::atomic::Ordering::Acquire) > 0);
        self.retired
            .iter()
            .fold(self.len, |total, image| total.saturating_add(image.bytes))
    }

    fn handle(&self) -> Option<std::sync::Arc<std::fs::File>> {
        self.file.clone()
    }

    /// Pins the current image for a reader. `None` when nothing has been paged
    /// yet — there is no image to pin, and no scratch-backed chunk to read.
    pub(super) fn pin(&self) -> Option<std::sync::Arc<HoldsScratchPin>> {
        let file = std::sync::Arc::clone(self.file.as_ref()?);
        self.pins.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Some(std::sync::Arc::new(HoldsScratchPin {
            file,
            pins: std::sync::Arc::clone(&self.pins),
        }))
    }

    /// Whether a reader holds the current image. Read on the router's own
    /// thread, which is also the only thread that hands pins out, so the answer
    /// cannot change between this and the compaction that acts on it.
    pub(super) fn is_pinned(&self) -> bool {
        self.pins.load(std::sync::atomic::Ordering::Acquire) > 0
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
            // Marked sparse before the first `write_at`. A killed run's scratch
            // is swept at restart, so an existing file here is not state to
            // preserve — truncating it is what keeps the append cursor (`len`,
            // reset to zero by `discard`) agreeing with the file. A fresh image
            // starts unpinned: whatever pins a discarded image still has are
            // on their own counter.
            let file = open_scratch_file(&self.path, &self.sparse)
                .map_err(|_| DemotionReason::HoldsScratchFailed)?;
            self.file = Some(std::sync::Arc::new(file));
            self.pins = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            tracing::debug!(
                scratch_path = %self.path.display(),
                ceiling_bytes = self.ceiling,
                "direct-store created on-disk holds scratch"
            );
        }
        let file = self.file.as_ref().expect("just opened");
        write_at(file, self.len, bytes).map_err(|_| DemotionReason::HoldsScratchFailed)?;
        let offset = self.len;
        self.len = end;
        Ok(offset)
    }

    /// Rewrites the file so it holds only `live`, and returns each extent's new
    /// offset.
    ///
    /// `live` must be disjoint and sorted ascending, which is what the router's
    /// staging maps naturally yield: every append claims a fresh region and
    /// trimming only ever narrows one.
    ///
    /// The rewrite is in place and strictly forward. The write cursor
    /// accumulates live lengths only, while the extents it reads from also step
    /// over the dead ones, so an extent's destination is never past its own
    /// source and never lands on a source that has not been read yet. Copying
    /// each extent front to back keeps that true inside an extent too.
    ///
    /// While a [`HoldsScratchPin`] is alive the rewrite is **not** in place:
    /// the live extents are packed into a fresh file that is renamed over the
    /// path, the router carries on with that copy, and the pinned readers keep
    /// the handle to the image their offsets were taken from. The old image's
    /// space is returned when the last pin drops. This is what lets a provider
    /// read holds from the scratch on demand rather than copying them out
    /// first.
    ///
    /// `None` means the file is now in an unknown state: the caller must
    /// demote rather than trust any offset, including the ones it already had.
    pub(super) fn compact(&mut self, live: &[(u64, u64)]) -> Option<Vec<u64>> {
        let file = std::sync::Arc::clone(self.file.as_ref()?);
        if self.is_pinned() {
            return self.compact_relocating(&file, live);
        }
        let mut new_offsets = Vec::with_capacity(live.len());
        let mut cursor = 0u64;
        for (offset, len) in live.iter().copied() {
            if cursor > offset {
                // The caller handed extents that are not disjoint or not
                // sorted. Refusing is the only safe answer.
                return None;
            }
            if cursor < offset {
                copy_scratch_region(&file, offset, &file, cursor, len)?;
            }
            new_offsets.push(cursor);
            cursor = cursor.checked_add(len)?;
        }
        file.set_len(cursor).ok()?;
        self.len = cursor;
        Some(new_offsets)
    }

    /// [`Self::compact`] under a pin: pack into a fresh file, then take it over.
    ///
    /// The take-over is what keeps everything outside this type unchanged —
    /// the path is the path, `discard` deletes it, the restart sweep recognises
    /// it. A failure at any step leaves the current image exactly as it was
    /// and removes the half-written copy; the caller demotes on `None` as
    /// before. The caller has already had the packed copy's bytes admitted by
    /// the accountant: while the pins live, both images are on disk.
    fn compact_relocating(&mut self, old: &std::fs::File, live: &[(u64, u64)]) -> Option<Vec<u64>> {
        let packing_path = compacting_scratch_path(&self.path);
        let retired_path = retired_scratch_path(&self.path);
        let fresh = open_scratch_file(&packing_path, &self.sparse).ok()?;
        let packed = (|| {
            let mut new_offsets = Vec::with_capacity(live.len());
            let mut cursor = 0u64;
            for (offset, len) in live.iter().copied() {
                if cursor > offset {
                    return None;
                }
                copy_scratch_region(old, offset, &fresh, cursor, len)?;
                new_offsets.push(cursor);
                cursor = cursor.checked_add(len)?;
            }
            take_over_scratch_path(&packing_path, &self.path, &retired_path).ok()?;
            Some((new_offsets, cursor))
        })();
        match packed {
            Some((new_offsets, cursor)) => {
                tracing::debug!(
                    scratch_path = %self.path.display(),
                    old_bytes = self.len,
                    packed_bytes = cursor,
                    pins = self.pins.load(std::sync::atomic::Ordering::Acquire),
                    "direct-store relocated the holds scratch under a live reader"
                );
                let pins = std::mem::replace(
                    &mut self.pins,
                    std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                );
                self.retired.push(RetiredScratchImage {
                    pins,
                    bytes: self.len,
                });
                self.file = Some(std::sync::Arc::new(fresh));
                self.len = cursor;
                Some(new_offsets)
            }
            None => {
                drop(fresh);
                let _ = std::fs::remove_file(&packing_path);
                None
            }
        }
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

/// One run of a [`SparseImage`], and where its bytes are read back from.
enum ImageRun {
    /// A run the router is still holding for this volume — in RAM, or paged out
    /// to the set's holds scratch.
    Staged(StagedChunk),
    /// A run the volume's own **envelope file** holds, at its true physical
    /// offset (envelope v2), read back positionally on demand.
    ///
    /// The run's start *is* the file offset, so the variant carries only a
    /// length. Only a restored volume's image has these: its pre-restart bytes
    /// are on disk rather than in `chunks`, and the envelope is where every
    /// non-member byte of them went.
    Envelope { len: u64 },
}

impl ImageRun {
    fn len(&self) -> u64 {
        match self {
            Self::Staged(chunk) => chunk.len(),
            Self::Envelope { len } => *len,
        }
    }
}

/// A sparse byte image of one volume, read through by the header parser.
///
/// Reads inside a staged run return real bytes; reads anywhere else return
/// `Ok(0)`, which `read_exact` turns into `UnexpectedEof` and the RAR header
/// walk turns into a clean stop. Seeks always succeed — the walk seeks over
/// data areas it never reads. Constructing one is **O(chunks) pointer copies,
/// not O(bytes)**. The first shape cloned every staged chunk on every article,
/// so a `-rr` volume paid a whole-image `memcpy` per article until it was
/// confirmed. The parser is handed a reader by value and the library's entry
/// point requires `'static`, so a plain borrow is not available; sharing the
/// chunks is the same zero-copy with an ownership story that outlives the
/// borrow.
///
/// [`Self::over_envelope`] keeps that property against a file: it adds runs the
/// envelope backs, and each one is read only if and when the walk asks for it —
/// which for a header walk is a header at a time, never the recovery record it
/// seeks over.
pub(super) struct SparseImage {
    runs: Vec<(u64, ImageRun)>,
    /// Read handle for the scratch-resident runs. Held as an `Arc<File>` so the
    /// image satisfies the parser's `'static` bound without reopening the file
    /// per parse, and read positionally so nothing here disturbs a concurrent
    /// append.
    scratch: Option<std::sync::Arc<std::fs::File>>,
    /// Read handle for [`ImageRun::Envelope`] runs, held for the same reason.
    envelope: Option<std::sync::Arc<std::fs::File>>,
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
                .map(|(offset, chunk)| (*offset, ImageRun::Staged(chunk.clone())))
                .collect(),
            scratch,
            envelope: None,
            position: 0,
        }
    }

    /// [`Self::from_staged`] plus the bytes the volume's envelope file already
    /// holds — the image a **restored** volume's confirming parse needs.
    ///
    /// `envelope_ranges` are the physical ranges the envelope is known to hold:
    /// the volume's routed coverage minus every member extent the routing
    /// history claims. Anything outside them stays a hole, so a byte that went
    /// to a `.direct.partial` — or one a failed write never placed — is never
    /// served out of the sparse hole standing in for it.
    fn over_envelope(
        chunks: &BTreeMap<u64, StagedChunk>,
        scratch: Option<std::sync::Arc<std::fs::File>>,
        envelope: std::sync::Arc<std::fs::File>,
        envelope_ranges: &ByteRanges,
    ) -> Self {
        // Staged runs win wherever the two describe the same byte. They are the
        // same bytes either way — `trim_volume` retains an unconfirmed volume's
        // envelope bytes in RAM precisely so the walk can seek through them — and
        // preferring RAM keeps the read off the disk. Overlap is resolved by
        // subtraction rather than by priority at read time because the run list
        // has to stay disjoint and sorted for the binary search below.
        let mut only_envelope = ByteRanges::new();
        for &(start, end) in envelope_ranges.ranges() {
            only_envelope.insert(start, end - start);
        }
        for (offset, chunk) in chunks {
            only_envelope = subtract(&only_envelope, *offset, chunk.len());
        }
        let mut runs: Vec<(u64, ImageRun)> = chunks
            .iter()
            .map(|(offset, chunk)| (*offset, ImageRun::Staged(chunk.clone())))
            .chain(
                only_envelope
                    .ranges()
                    .iter()
                    .map(|&(start, end)| (start, ImageRun::Envelope { len: end - start })),
            )
            .collect();
        runs.sort_unstable_by_key(|(start, _)| *start);
        Self {
            runs,
            scratch,
            envelope: Some(envelope),
            position: 0,
        }
    }

    /// Test constructor: a purely RAM-resident image.
    #[cfg(test)]
    pub(super) fn from_chunks(chunks: &BTreeMap<u64, std::sync::Arc<[u8]>>) -> Self {
        Self {
            runs: chunks
                .iter()
                .map(|(offset, bytes)| {
                    (
                        *offset,
                        ImageRun::Staged(StagedChunk::Memory(std::sync::Arc::clone(bytes))),
                    )
                })
                .collect(),
            scratch: None,
            envelope: None,
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
        let (start, run) = &self.runs[index];
        let inside = position - start;
        if inside >= run.len() {
            return Ok(0);
        }
        let taken = (run.len() - inside).min(out.len() as u64) as usize;
        match run {
            ImageRun::Staged(StagedChunk::Memory(bytes)) => {
                out[..taken].copy_from_slice(&bytes[inside as usize..inside as usize + taken]);
            }
            // Paged out: read back exactly what the walk asked for, which for a
            // header walk is a header at a time, never the data area it seeks
            // over. A missing handle answers `Ok(0)`, the same clean stop a hole
            // produces — a parse that cannot see a byte must never see a
            // fabricated one.
            ImageRun::Staged(StagedChunk::Scratch { offset, .. }) => {
                let Some(file) = self.scratch.as_ref() else {
                    return Ok(0);
                };
                if read_at(file, offset.saturating_add(inside), &mut out[..taken]).is_err() {
                    return Ok(0);
                }
            }
            // Same rule against the envelope file, and the same failure
            // handling: a short envelope — one the previous run never finished
            // writing — reads as a hole, so the walk stops there instead of
            // walking whatever the filesystem answers.
            ImageRun::Envelope { .. } => {
                let Some(file) = self.envelope.as_ref() else {
                    return Ok(0);
                };
                if read_at(file, position, &mut out[..taken]).is_err() {
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
/// bytes with nothing bounding either term.
pub(crate) type RepairedChunk = (u64, std::sync::Arc<[u8]>);

/// Per-volume staging: the bytes the router still needs, and what it has
/// already placed.
#[derive(Debug, Default)]
pub(super) struct VolumeStaging {
    /// Non-overlapping byte runs, keyed by physical offset. Shared rather than
    /// owned so the header parser's image is a pointer copy, and each one
    /// either RAM-resident or paged out to the set's holds scratch.
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
    /// Physical ranges force-staged by a PAR2 repair.
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
    /// The offset the last header walk over this volume ran out of bytes at,
    /// as the walk itself reported it; `None` before the first walk and
    /// whenever one stopped for a reason more bytes cannot change.
    ///
    /// **The re-parse gate.** A store volume's end-of-archive record sits past
    /// every member's payload, so the confirming walk cannot succeed until the
    /// volume's *last* article lands — and without this the router walked the
    /// headers again for every article in between, each walk stopping at the
    /// same byte. On a `-hp` set each of those walks also re-derived the
    /// archive key.
    ///
    /// The number is a lower bound on what the walk needs: waiting for it can
    /// still cost a walk that comes up short, and can never skip one that
    /// would have succeeded.
    parse_short_at: Option<u64>,
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
    /// Whether the volume image now reaches `short_at`, the offset the last
    /// header walk reported it could not read.
    ///
    /// `short_at` is the **first** offset the walk could not read, so what the
    /// walk needs is every byte *below* it, and the byte to ask after is the
    /// one at `short_at - 1`: either it is staged, or it was routed away — in
    /// which case the walk's own answer may have moved and the gate must not
    /// hold it back. Asking after the byte *at* `short_at` is off by one, and
    /// not harmlessly so: the walk reports the least a header could occupy
    /// from where it stopped, and a `-hp` end-of-archive record is exactly
    /// that minimum, so its `short_at` is the volume's own length — an offset
    /// no image ever holds a byte at. A volume whose end record arrived by
    /// repair rather than by its last article would then never be re-walked,
    /// never confirmed, and never able to file the record it was repaired for.
    fn parse_can_reach(&self, short_at: u64) -> bool {
        let Some(last_needed) = short_at.checked_sub(1) else {
            return true;
        };
        let staged = self
            .chunks
            .range(..=last_needed)
            .next_back()
            .is_some_and(|(start, chunk)| last_needed < start.saturating_add(chunk.len()));
        staged || self.routed.missing(last_needed, 1).is_empty()
    }

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
    /// whatever was staged there and re-opening it for routing.
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
    /// Two callers, and both need the *absence* of the mark. A repair's cipher
    /// lead-in restages bytes that did not change, purely so an
    /// encrypted member's drain can rebuild the CBC chain into the span above
    /// them: marking them repaired would have them overwrite a composition they
    /// did not change and would put them under `route_repaired`'s "every
    /// repaired byte finds a destination" rule, which they cannot satisfy when
    /// they land in an unroutable region. The test caller builds the one shape
    /// that is otherwise unreachable from outside — unrepaired bytes next to
    /// repaired ones in a single drained run — since [`Self::stage`] refuses a
    /// routed range outright. See
    /// `a_drain_run_straddling_repaired_and_duplicate_bytes_splits_at_the_boundary`.
    fn stage_lead_in(&mut self, offset: u64, data: std::sync::Arc<[u8]>) {
        self.force_stage(offset, data, false);
    }

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

    #[cfg(test)]
    fn staged_bytes(&self) -> u64 {
        self.chunks
            .values()
            .fold(0u64, |total, chunk| total.saturating_add(chunk.len()))
    }

    /// Contiguous coverage from offset zero across everything the volume
    /// holds, routed or pending — the bytes the header walk can actually
    /// consume, and therefore the only measure the unparsable ceiling may
    /// judge (see [`MAX_HEADER_PREFIX_BYTES`]). The two range sets partition
    /// the volume's coverage and a prefix can alternate between them (routed
    /// bytes drain out of `pending`), so this walks their union to a fixed
    /// point rather than reading either alone.
    fn parse_prefix_len(&self) -> u64 {
        let mut prefix = 0u64;
        loop {
            let extended = self
                .pending
                .contiguous_from(prefix)
                .max(self.routed.contiguous_from(prefix));
            if extended == prefix {
                return prefix;
            }
            prefix = extended;
        }
    }

    /// The RAM half of [`Self::staged_bytes`] — what the holds budget bounds. A
    /// paged chunk still costs a scratch region, which the ceiling bounds
    /// separately.
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
    /// The drain's `replace` flag is all-or-nothing per emitted run, and the
    /// two things that decide a run's extent decide it for unrelated reasons:
    /// `map_physical_range` splits at member and envelope boundaries, and
    /// `pending` coalesces every staged range that abuts another. A repair
    /// therefore routinely produces one member run covering repaired *and*
    /// unrepaired bytes, and that run took `replace = false` — so
    /// `CrcRuns::insert` refused it as overlapping, the wire-damaged value
    /// survived the repair, and the member failed its gate on bytes that are
    /// correct on disk. Splitting here first is what makes the flag exact.
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
    /// [`Self::covered`] so they are not refetched.
    ///
    /// `CrcRuns` never survives a restart, so these bytes are covered and
    /// **unverified** — the gates stay disarmed over them until the bytes are
    /// re-read from disk. Non-empty is therefore a hard refusal in
    /// [`DirectSetRouter::try_verify_member`], not merely an absence of runs:
    /// composing around a seeded range would pass a member on the strength of
    /// what a previous process claimed to have written rather than on what is
    /// on disk now, which is exactly the assurance the re-arm refuses to trade
    /// away.
    restart_seeded: ByteRanges,
    /// Logical ranges whose composed value a **repair** discarded.
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
    /// set admitted. Its presence is what makes the drain decrypt at
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
    /// cached (the restart input).
    ///
    /// A direct set never writes a volume file, so
    /// `try_update_archive_topology` — the only thing that normally fills
    /// `active_rar_volume_facts` — has nothing to parse and it is suppressed
    /// anyway. The router's own parse is therefore the **only** producer of
    /// these facts, and without caching them a restart has no way to rebuild
    /// the layout: the header bytes sit below the published floors and are
    /// never refetched.
    dirty_facts: std::collections::BTreeSet<u32>,
    staging: BTreeMap<u32, VolumeStaging>,
    /// Routing state by stable member id.
    members: BTreeMap<u32, MemberRouting>,
    /// Member name to stable id. Assigned once, never reused, never renumbered.
    member_ids: HashMap<String, u32>,
    next_member_id: u32,
    /// Every member extent the router has ever **routed bytes for**, per source
    /// volume, coalesced and in physical order.
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
    /// The process-wide accountant every set of the pipeline charges its holds
    /// to, and this set's standing charge against it. Unbounded until the
    /// runtime installs its own — see [`super::accountant`].
    accountant: std::sync::Arc<super::accountant::HoldsAccountant>,
    charge: super::accountant::HoldsCharge,
    /// The paging destination. Opened on the first breach and never before, so
    /// a set that stays inside its RAM budget — which is nearly all of them —
    /// touches the filesystem for it exactly zero times.
    scratch: HoldsScratch,
    /// The set's password and the keys derived from it. Empty and untouched
    /// for a set with no encrypted member — which is every unencrypted set.
    crypt: KeyRing,
    /// The archive-header key for a `-hp` set. Empty and untouched for every
    /// set whose headers are readable, which is every set with readable
    /// headers: it is only consulted when a header parse comes back
    /// `EncryptedArchive`.
    header_crypt: HeaderKeyRing,
    /// The key derivations every header walk over this set's volumes shares.
    ///
    /// A `-hp` volume's archive key comes from (password, salt, KDF count),
    /// and all three are properties of the volume rather than of the parse —
    /// so a walk that derives its own throws the work away and the next
    /// article's walk pays for it again. On a set whose volumes are staged
    /// article by article that is one PBKDF2 run of up to 2^24 iterations per
    /// article, per volume, for nothing.
    ///
    /// Held for the life of the router, which is the set: the cache is keyed
    /// by password as well as by salt, so it is derived key material and its
    /// lifetime is deliberately this unit of work and not the process.
    kdf_cache: std::sync::Arc<unrar_rs::KdfCache>,
    /// Envelope spans produced by a member **migration** (the small-member
    /// tolerance), waiting to be handed to the caller.
    ///
    /// A migration is decided inside `check_eligibility`, which runs from the
    /// middle of a parse and has no way to return bytes. The spans are parked
    /// here and drained by whichever public entry point is on the stack, so they
    /// go out through the one path that writes spans, records them against the
    /// coverage barrier and syncs the envelope before publishing a floor — the
    /// same treatment every other envelope byte gets.
    migrated: Vec<RoutedSpan>,
    /// [`MIGRATION_CEILING_BYTES`], as a field so a test can lower it below
    /// what a fixture-sized member routes.
    migration_ceiling_bytes: u64,
    /// Destinations a migration deleted, waiting to be retired from the coverage
    /// barrier: `(member id, working-directory-relative partial)`.
    ///
    /// Parked for the same reason as [`Self::migrated`] — the router owns no
    /// barrier and the decision is taken mid-parse — and drained by
    /// [`super::set::DirectSet::ensure_registered`]. Until it is, the barrier
    /// still claims a file the migration unlinked, and a restart in that window
    /// refuses the row on a missing destination.
    retired_destinations: Vec<(u32, String)>,
    /// Bumped whenever the facts the checkpoint's **plan digest** binds change:
    /// a member adopted, a member migrated away, or a declared unpacked size
    /// that actually moved.
    ///
    /// The digest is recomputed off this rather than on every routed batch, for
    /// the reason [`Self::member_order`] is cached: the read is per article and
    /// the change is per member, and hashing the whole volume map into a fresh
    /// blake3 for every span of a 2 000-volume set is real work to conclude
    /// nothing happened.
    member_facts_revision: u64,
    /// Cached [`Self::member_ciphers`].
    ///
    /// The read is per **PAR2 read-back**: live verification issues one read
    /// per straddling block, each of which assembles a provider, and The
    /// overlay made the value it copies ~1000× bigger than the old one — one
    /// checkpoint per member became one per [`crypt::CHECKPOINT_STRIDE`], so a
    /// 50 GiB member is some 12,800 `BTreeMap` nodes plus a coverage map,
    /// deep-cloned per call. The facts change per *decrypt*, which for the same
    /// read is nothing at all.
    ///
    /// Behind a lock because the read path takes `&self`; dropped by
    /// [`Self::member_mut`], which every mutable path to a member goes through.
    member_ciphers_cache:
        std::sync::Mutex<Option<std::sync::Arc<HashMap<u32, crypt::MemberCipher>>>>,
    /// How many times that cache has been built, so a test can prove it is one.
    #[cfg(test)]
    member_ciphers_builds: std::sync::atomic::AtomicU64,
    /// How many times the drain has held a cipher block because the other half
    /// of it had not arrived. The production account of this is the
    /// `direct_store.encrypted.block_held` probe; this is the same fact in a
    /// form a test can assert on, because byte-identical output cannot tell a
    /// set that held from one that never had to.
    #[cfg(test)]
    blocks_held: u64,
    /// How many Quick Open cross-check walks this set has run, so a test can
    /// prove a cache the library never adopted does not cost a second parse.
    #[cfg(test)]
    quick_open_walks: u64,
    /// How many header walks this set has run over a staged image, so a test
    /// can prove the re-parse gate holds: the count is a property of the
    /// volumes, not of how many articles they arrived in.
    #[cfg(test)]
    parse_walks: u64,
    /// Does the job that owns this set carry PAR2 at all?
    ///
    /// The one fact that turns a part-checksum mismatch from a verdict into a
    /// question. With no PAR2 there is nothing that could ever answer it, so
    /// the mismatch is what it has always been: a demotion, taken immediately,
    /// so the conventional path gets the set while its bytes are still on disk.
    ///
    /// Read from the job's own NZB rather than from a parsed PAR2 index,
    /// because the mismatch is discovered *while the set is downloading* and
    /// the index may not have arrived yet. The NZB names its PAR2 files from
    /// the first moment the job exists, which is the only source that is
    /// already true when this is consulted. See
    /// [`super::plan::spec_carries_par2`] for why an index alone counts.
    par2_available: bool,
    /// Volumes whose posted bytes failed an archive-level checksum that the
    /// wire's own yEnc CRC could not see.
    ///
    /// A recorded fact, not a verdict. It says three things at once: the set
    /// stays direct, the PAR2 pass over this volume may not stand on wire
    /// evidence (the wire is exactly what lied), and any member spanning the
    /// volume holds its whole-member gate until the repair has had its say.
    ///
    /// Empty unless [`Self::par2_available`], and emptied per volume
    /// by [`Self::route_repaired`] — the repair's answer supersedes the
    /// question.
    damaged_volumes: std::collections::BTreeSet<u32>,
    /// Has a repair already re-routed bytes into this set?
    ///
    /// A part mismatch *before* any repair is a question PAR2 can answer. The
    /// same mismatch on bytes a repair just wrote is the answer, and it is no:
    /// recording it again would park the set on a question that has already
    /// been asked and lost, so the second one demotes.
    repair_rerouted: bool,
    /// Is [`Self::route_repaired`] mid-drain?
    ///
    /// A rewrite reaches the compositions in pieces — an encrypted slice as its
    /// edge blocks and aligned middle, a volume with several damaged slices as
    /// one run per slice — and between two of those pieces a part's runs still
    /// tile: the pieces already fed carry the repaired values and the rest still
    /// carry the wire-damaged ones. A gate that fires there composes a mixture
    /// that describes no bytes that ever existed, and with `repair_rerouted`
    /// set its mismatch is the demotion, not a question. So while this is set
    /// both integrity layers only record; [`Self::settle_repair_gates`] runs
    /// them once, over the finished rewrite.
    repair_draining: bool,
    /// A volume whose replacement spans are arriving across several calls.
    /// Its integrity gates and durable coverage remain pending until the last
    /// batch arrives. A different volume cannot finish this replacement.
    repair_batch: Option<u32>,
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

impl Drop for DirectSetRouter {
    /// A set's bytes go with it. The accountant is charged with live holds,
    /// and a router that is dropped — its job removed, its set cleared — has
    /// none left.
    fn drop(&mut self) {
        self.accountant.release(&mut self.charge);
    }
}

impl DirectSetRouter {
    pub(crate) fn new(plan: DirectSetPlan) -> Self {
        // One cache for the whole set: the key rings verify and derive into
        // it, and every header walk over every volume shares it.
        let kdf_cache = std::sync::Arc::new(unrar_rs::KdfCache::new());
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
            migration_ceiling_bytes: MIGRATION_CEILING_BYTES,
            member_order: Vec::new(),
            member_order_stale: false,
            holds_budget: DEFAULT_HOLDS_BUDGET_BYTES,
            accountant: std::sync::Arc::new(super::accountant::HoldsAccountant::unbounded()),
            charge: super::accountant::HoldsCharge::default(),
            crypt: KeyRing::with_shared_kdf_cache(std::sync::Arc::clone(&kdf_cache)),
            header_crypt: HeaderKeyRing::with_shared_kdf_cache(std::sync::Arc::clone(&kdf_cache)),
            kdf_cache,
            migrated: Vec::new(),
            retired_destinations: Vec::new(),
            member_facts_revision: 0,
            member_ciphers_cache: std::sync::Mutex::new(None),
            #[cfg(test)]
            member_ciphers_builds: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            blocks_held: 0,
            #[cfg(test)]
            quick_open_walks: 0,
            #[cfg(test)]
            parse_walks: 0,
            par2_available: false,
            damaged_volumes: std::collections::BTreeSet::new(),
            repair_rerouted: false,
            repair_draining: false,
            repair_batch: None,
            demoted: None,
        }
    }

    /// Tells the router whether the job carries any PAR2.
    ///
    /// Set once per set, from the NZB, at admission and at restore. See
    /// [`Self::par2_available`] for why the NZB and not the index.
    pub(crate) fn note_par2_available(&mut self, available: bool) {
        self.par2_available = available;
    }

    /// Volumes carrying a recorded part-checksum mismatch, awaiting PAR2's
    /// answer. See [`Self::damaged_volumes`].
    pub(crate) fn damaged_volumes(&self) -> &std::collections::BTreeSet<u32> {
        &self.damaged_volumes
    }

    /// Records a failed part checksum as a damaged-volume fact, or refuses.
    ///
    /// `true` means the fact was recorded and the set stays direct; `false`
    /// means the caller must demote, which is what every caller did
    /// unconditionally before there was anything that could answer the
    /// question.
    ///
    /// The part's composed value is dropped on the way through. That is what
    /// stalls [`Self::try_verify_member`] rather than failing it: the value
    /// describes bytes an archive-level checksum has just called wrong, and a
    /// member composed from it would either demote for the wrong reason
    /// (`MemberChecksumMismatch`, on damage the repair is about to fix) or —
    /// worse — verify, if the damage happened to compose back to the expected
    /// whole-member value.
    fn record_part_checksum_damage(
        &mut self,
        volume_index: u32,
        member_id: u32,
        part_position: u32,
    ) -> bool {
        if !self.par2_available || self.repair_rerouted {
            return false;
        }
        self.damaged_volumes.insert(volume_index);
        if let Some(member) = self.member_mut(member_id) {
            member.checked_parts.remove(&part_position);
            member.verified = false;
        }
        true
    }

    /// Does any part of this member live in a volume with damage on record?
    ///
    /// The whole-member gate's stall condition. Layer 1's own stall — a missing
    /// `checked_parts` entry — covers the plaintext composition, but the
    /// encrypted composition deliberately reads plaintext runs instead and
    /// would sail past it, so the question is asked once, here, in the terms
    /// both compositions share.
    fn member_spans_damaged_volume(&self, member_id: u32) -> bool {
        if self.damaged_volumes.is_empty() {
            return false;
        }
        let Some(layout_index) = self.layout_index_for_member(member_id) else {
            return false;
        };
        self.layout_members()
            .get(layout_index)
            .is_some_and(|member| {
                member
                    .parts
                    .iter()
                    .any(|part| self.damaged_volumes.contains(&part.volume))
            })
    }

    /// Cipher blocks the drain has held for a missing predecessor or a missing
    /// other half. Test-only; see the field.
    #[cfg(test)]
    pub(crate) fn blocks_held(&self) -> u64 {
        self.blocks_held
    }

    /// Quick Open cross-check walks run so far. Test-only; see the field.
    #[cfg(test)]
    pub(crate) fn quick_open_walks(&self) -> u64 {
        self.quick_open_walks
    }

    /// Header walks over a staged image run so far. Test-only; see the field.
    #[cfg(test)]
    pub(crate) fn parse_walks(&self) -> u64 {
        self.parse_walks
    }

    /// Key derivations this set has actually paid for — RAR5 and RAR4 alike —
    /// as opposed to the ones served from its cache.
    ///
    /// The cost this measures is not incidental: one RAR5 derivation is a
    /// PBKDF2 run of up to 2^24 iterations, and a set that pays one per
    /// arriving article spends more time on it than on everything else the
    /// router does.
    #[cfg(test)]
    pub(crate) fn kdf_derivations(&self) -> u64 {
        self.kdf_cache
            .rar5_derivation_count()
            .saturating_add(self.kdf_cache.rar4_derivation_count())
    }

    /// Whether the set would still take a job password.
    ///
    /// The seam re-reads the live job spec while this is true, because a
    /// password can arrive **after** the job was added: `setJobPassword` and the
    /// NZBGet facade's `*Unpack:Password` both mutate the spec in place. It goes
    /// false once a password is *admitted*, or once the set leaves direct mode.
    ///
    /// # The window is pre-first-article, and only that
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
        self.demoted.is_none()
            && (self.crypt.wants_password() || self.header_crypt.wants_password())
    }

    /// Binds the job's password. Never persisted, never logged.
    ///
    /// A **proved** `-hp` archive key wins over the spec's. RAR
    /// uses one password for headers and file data alike, so a set whose headers
    /// the archive's own check opened has already established which of the job's
    /// candidates is the set's password — while `spec.password` is only the
    /// harvest's first entry and may be an operator's guess that lost to the NZB
    /// meta password. Letting the spec replace a proved key would open the
    /// headers and then refuse the members.
    pub(crate) fn set_password(&mut self, password: Option<&str>) {
        if self.header_crypt.password().is_some() {
            return;
        }
        self.crypt.set_password(password);
    }

    /// Offers one of the job's archive-password candidates to the `-hp` gate.
    /// Never persisted, never logged.
    ///
    /// Separate from [`Self::set_password`] because the two rings answer
    /// different questions from different inputs. `set_password` binds the one
    /// password the job spec carries and keys *file data*; this offers the whole
    /// harvest — `Explicit`, `NzbMeta`, `FilenameConvention`, at most one each —
    /// and keys the *archive headers*. A set whose header key is the NZB-meta
    /// password while the spec carries an operator's guess would otherwise
    /// refuse for a password that was already in hand.
    ///
    /// Offers are ignored once the ring has verified or refused, so this is
    /// idempotent and safe to call per article.
    pub(crate) fn offer_header_password(&mut self, source: &'static str, value: &str) {
        self.header_crypt.offer(source, value);
    }

    /// Whether this set is still collecting `-hp` candidates, so the seam knows
    /// whether re-offering the harvest can still change anything.
    pub(crate) fn wants_header_password(&self) -> bool {
        self.demoted.is_none() && self.header_crypt.wants_password()
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
    /// This was once the refusal every consumer of **posted** bytes read, since
    /// nothing could turn a decrypted destination back into what was posted.
    /// The overlay makes it a statement of fact instead:
    /// [`Self::member_ciphers`] is how those consumers get posted bytes now,
    /// and the question they ask before trusting one is
    /// [`Self::posted_bytes_unavailable`].
    pub(crate) fn routes_encrypted(&self) -> bool {
        self.crypt.admitted()
    }

    /// The read-side crypt facts for every encrypted member the set has routed
    /// bytes for, keyed by stable member id.
    ///
    /// This is what makes a virtual volume answer in **cipher** space: the
    /// provider overlay resolves a physical byte to a member extent exactly as
    /// it always did, then re-encrypts the plaintext it reads back out of the
    /// partial. Empty for every unencrypted set, which is the overlay switched
    /// off by construction.
    ///
    /// Shared rather than rebuilt: every caller wraps the result in an `Arc`
    /// and hands it to a provider, and the facts only move when a member's
    /// crypt state or coverage does — see [`Self::member_ciphers_cache`].
    pub(crate) fn member_ciphers(&self) -> std::sync::Arc<HashMap<u32, crypt::MemberCipher>> {
        let Ok(mut slot) = self.member_ciphers_cache.lock() else {
            return std::sync::Arc::new(self.build_member_ciphers());
        };
        if let Some(cached) = slot.as_ref() {
            return std::sync::Arc::clone(cached);
        }
        let built = std::sync::Arc::new(self.build_member_ciphers());
        *slot = Some(std::sync::Arc::clone(&built));
        built
    }

    fn build_member_ciphers(&self) -> HashMap<u32, crypt::MemberCipher> {
        #[cfg(test)]
        self.member_ciphers_builds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.members
            .iter()
            .filter_map(|(member_id, member)| {
                let facts = member
                    .crypt
                    .as_ref()?
                    .cipher_facts(member.unpacked_size, &member.covered)?;
                Some((*member_id, facts))
            })
            .collect()
    }

    /// How many times [`Self::member_ciphers`] has really rebuilt its snapshot,
    /// which is the only way to tell a cache from a coincidence.
    #[cfg(test)]
    pub(crate) fn member_ciphers_builds(&self) -> u64 {
        self.member_ciphers_builds
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// One member, for mutation.
    ///
    /// **Every** `&mut` path to a member goes through here, and that is what
    /// keeps [`Self::member_ciphers`]' snapshot honest: the snapshot copies
    /// each member's checkpoints, retained padding and coverage, and nothing
    /// about handing out a `&mut MemberRouting` says which of those the caller
    /// is about to move. Dropping the snapshot on every mutable access costs
    /// one rebuild per batch of routing and cannot be forgotten the way a list
    /// of "the mutations that matter" can.
    fn member_mut(&mut self, member_id: u32) -> Option<&mut MemberRouting> {
        self.invalidate_member_ciphers();
        self.members.get_mut(&member_id)
    }

    /// Drops the cached snapshot. For the two member mutations that are not a
    /// `&mut` borrow of an existing one — adoption and removal.
    fn invalidate_member_ciphers(&mut self) {
        if let Ok(slot) = self.member_ciphers_cache.get_mut() {
            *slot = None;
        }
    }

    /// Posted cipher bytes in **other** volumes that a repair of `volume_index`
    /// needs staged beside it.
    ///
    /// A member's cipher blocks do not respect volume boundaries: a split
    /// member's part ends wherever the volume filled up, so the block at each
    /// edge of a member extent is usually half in this volume and half in its
    /// neighbour. During a download both halves are staged at once and whichever
    /// drain reaches the block first decrypts it for the other; a repair stages
    /// one volume long after the neighbour's bytes were routed and dropped, so
    /// the edge block cannot be assembled at all — and `route_repaired`'s "every
    /// repaired byte finds a destination" rule then demotes the whole set.
    ///
    /// The **low** edge is a block wider than the block that straddles it, and
    /// that block is why. Decrypting the extent's first block needs its CBC
    /// predecessor as well as its own 16 bytes — exactly what the same-volume
    /// lead-in spends its 32 bytes on — and for a non-first volume that
    /// predecessor is in the neighbour too. Reading only `[block_floor(low),
    /// low)` left it out, so damage in the first article of any non-first
    /// volume of a split encrypted member could not be re-routed at all: no
    /// checkpoint survives at `block_floor(low)` once the two volumes'
    /// decrypted runs coalesce, `member_cipher` has nothing staged below the
    /// extent, the span holds, and the whole set demotes under
    /// `RepairRerouteFailed` into a full refetch.
    ///
    /// Returns `(volume, physical offset, length)` reads — at most 31 bytes at a
    /// low edge and 15 at a high one — that the caller answers **through the
    /// provider overlay** (those bytes did not change, so re-encrypting them
    /// from the neighbour's own destination reproduces exactly what was posted)
    /// and hands back to [`Self::route_repaired`] as unrepaired lead-in.
    pub(crate) fn cipher_edge_reads(&self, volume_index: u32) -> Vec<(u32, u64, u64)> {
        self.cipher_edge_reads_bounded(volume_index, usize::MAX)
            .expect("unbounded edge plan")
    }

    /// Refuse before allocating more than `limit` edge requests. The caller
    /// reserves their metadata and bytes before requesting this plan.
    pub(crate) fn cipher_edge_reads_bounded(
        &self,
        volume_index: u32,
        limit: usize,
    ) -> Option<Vec<(u32, u64, u64)>> {
        let mut reads = Vec::new();
        for extent in self.routed_extents.get(&volume_index).into_iter().flatten() {
            let Some(member) = self.members.get(&extent.member_id) else {
                continue;
            };
            let Some(cipher_size) = member.crypt.as_ref().and_then(MemberCrypt::cipher_size) else {
                continue;
            };
            let low = extent.logical_offset;
            let high = extent.logical_offset.saturating_add(extent.len);
            for (from, to) in [
                // One block below the straddling block: that block's own CBC
                // predecessor, without which it cannot be decrypted. At offset
                // 0 this collapses to an empty read, which is right — block 0's
                // predecessor is the member's IV.
                (block_floor(low).saturating_sub(AES_BLOCK), low),
                (high, block_ceil(high).min(cipher_size)),
            ] {
                if from >= to {
                    continue;
                }
                for (volume, extents) in &self.routed_extents {
                    if *volume == volume_index {
                        continue;
                    }
                    for candidate in extents
                        .iter()
                        .filter(|candidate| candidate.member_id == extent.member_id)
                    {
                        let begin = from.max(candidate.logical_offset);
                        let end = to.min(candidate.logical_offset.saturating_add(candidate.len));
                        if begin < end {
                            if reads.len() == limit {
                                return None;
                            }
                            reads.push((
                                *volume,
                                candidate.physical_offset + (begin - candidate.logical_offset),
                                end - begin,
                            ));
                        }
                    }
                }
            }
        }
        Some(reads)
    }

    /// Whether some encrypted member this set has **routed bytes for** cannot
    /// reproduce them.
    ///
    /// The fail-closed question behind every posted-byte consumer, and the
    /// residual the `EncryptedPar2Unsupported` demotion now names. Two shapes
    /// reach it, and both are refusals rather than approximations:
    ///
    /// - a member with no declared `cipher_size`, so no read-side facts exist at
    ///   all. Nothing should have routed for such a member, and if anything did,
    ///   its extents would otherwise be answered out of the plaintext;
    /// - a member that has routed into its **final cipher block** without the
    ///   tail padding being whole. That block's plaintext runs past
    ///   `unpacked_size` into bytes no destination holds, so without them
    ///   neither it nor the destination bytes inside it can be re-encrypted.
    ///
    /// Both are scoped to a member with **routed extents**, and the second to
    /// one whose extents actually reach the final block. A member the set mapped
    /// but never wrote is nothing for the overlay to answer; one that is simply
    /// still downloading has no reads in the region it cannot serve, and
    /// demoting either is a refetch for no benefit. The guard's own caller only
    /// runs once the payload is in, so "still downloading" is belt rather than
    /// braces — but it is the difference between a narrow refusal and one that
    /// fires on every non-block-aligned member mid-flight.
    ///
    /// # What it does not ask
    ///
    /// It never asks whether every routed byte is *reachable*. A ranged
    /// re-encryption seeds at the nearest checkpoint at or below its offset and
    /// chains up from there, so an **interior coverage hole** makes every read
    /// whose seed lies below it refuse — the overlay will not chain across
    /// plaintext it does not have — while this returns `false`.
    ///
    /// In the download path that shape does not arise: CBC forces every byte
    /// above a gap to be held, so a member with a hole has no coverage above it
    /// to be unreachable. A restart can produce it, because `restore` seeds
    /// `covered` from the extents the checkpoint recorded and those may be
    /// disjoint. The consequence is bounded and already true: reads in the
    /// ≤[`crypt::CHECKPOINT_STRIDE`] band above the hole refuse, so the pass
    /// reports damage in a region that *is* damaged — the hole itself is missing
    /// bytes — and PAR2 repairs or the set demotes on the ordinary path. It is a
    /// wider damage report than strictly necessary, not a byte fabricated or a
    /// hole passed off as sound, which is why this stays a note rather than a
    /// third refusal that would have to walk every member's coverage per call.
    pub(crate) fn posted_bytes_unavailable(&self) -> bool {
        if !self.crypt.admitted() {
            return false;
        }
        let routed: std::collections::BTreeSet<u32> = self
            .routed_extents
            .values()
            .flatten()
            .map(|extent| extent.member_id)
            .collect();
        routed.iter().any(|member_id| {
            let Some(member) = self.members.get(member_id) else {
                return false;
            };
            let Some(crypt) = member.crypt.as_ref() else {
                // Not an encrypted member: its extents are plaintext both ways.
                return false;
            };
            let Some(facts) = crypt.cipher_facts(member.unpacked_size, &member.covered) else {
                return true;
            };
            if facts.tail_plain().is_some() {
                return false;
            }
            // The final block's destination bytes. Nothing has asked the overlay
            // for them until something is written there.
            let from = block_floor(member.unpacked_size);
            let len = member.unpacked_size.saturating_sub(from);
            len > 0 && member.covered.missing(from, len) != vec![(from, from + len)]
        })
    }

    /// Sets the per-set scratch ceiling. Configured, with the env override
    /// winning; the tests lower it so a breach is reachable without paging
    /// gigabytes.
    pub(crate) fn set_holds_scratch_ceiling(&mut self, bytes: u64) {
        self.scratch.ceiling = bytes;
    }

    /// Sets the sparse marker every file this set creates goes through.
    pub(crate) fn set_sparse_marking(&mut self, marking: SparseMarking) {
        self.scratch.sparse = marking;
    }

    /// Bytes currently paged out to the set's holds scratch, counted separately
    /// from RAM so the two ceilings stay legible in metrics.
    pub(crate) fn scratch_bytes(&self) -> u64 {
        self.scratch.bytes()
    }

    /// Closes and deletes the scratch file. Idempotent; called at finalization
    /// and demotion.
    pub(crate) fn discard_scratch(&mut self) {
        self.scratch.discard();
        self.publish_holds();
    }

    /// Whether a reader still pins the set's scratch image. A pin outlives
    /// [`Self::discard_scratch`]: the path is gone, the bytes are not, until
    /// the last reader drops.
    #[cfg(test)]
    pub(crate) fn scratch_is_pinned(&self) -> bool {
        self.scratch.is_pinned()
    }

    /// Pages RAM-resident staged runs out to scratch until the holds budget is
    /// satisfied.
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
            if !self.holds_over_budget() {
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
            let resident_bytes = self.resident_bytes();
            let scratch_bytes = self.scratch.bytes();
            let chunk_bytes = bytes.len() as u64;
            let scratch_offset = match self.spill_to_scratch(&bytes) {
                Ok(offset) => offset,
                // A ceiling breach is not automatically a full scratch: reclaim
                // what placed holds left behind and try the append once more.
                // Only a scratch that is genuinely full demotes the set.
                Err(DemotionReason::HoldsScratchCeiling) => {
                    tracing::debug!(
                        set_name = %self.plan.set_name,
                        volume_index,
                        chunk_offset = offset,
                        chunk_bytes,
                        resident_bytes,
                        holds_budget_bytes = self.holds_budget,
                        scratch_bytes,
                        scratch_ceiling_bytes = self.scratch.ceiling,
                        "direct-store holds scratch reached its cap; attempting compaction"
                    );
                    if !self.compact_scratch()? {
                        tracing::debug!(
                            set_name = %self.plan.set_name,
                            volume_index,
                            chunk_offset = offset,
                            chunk_bytes,
                            resident_bytes,
                            scratch_bytes,
                            scratch_ceiling_bytes = self.scratch.ceiling,
                            "direct-store holds scratch cap has no reclaimable extents"
                        );
                        return Err(DemotionReason::HoldsScratchCeiling);
                    }
                    let compacted_scratch_bytes = self.scratch.bytes();
                    tracing::debug!(
                        set_name = %self.plan.set_name,
                        volume_index,
                        chunk_offset = offset,
                        chunk_bytes,
                        scratch_bytes,
                        compacted_scratch_bytes,
                        reclaimed_bytes = scratch_bytes.saturating_sub(compacted_scratch_bytes),
                        scratch_ceiling_bytes = self.scratch.ceiling,
                        "direct-store compacted holds scratch before retrying the spill"
                    );
                    match self.spill_to_scratch(&bytes) {
                        Ok(retry_scratch_offset) => {
                            tracing::debug!(
                                set_name = %self.plan.set_name,
                                volume_index,
                                chunk_offset = offset,
                                scratch_offset = retry_scratch_offset,
                                chunk_bytes,
                                scratch_bytes = self.scratch.bytes(),
                                scratch_ceiling_bytes = self.scratch.ceiling,
                                "direct-store holds scratch spill succeeded after compaction"
                            );
                            retry_scratch_offset
                        }
                        Err(DemotionReason::HoldsScratchCeiling) => {
                            tracing::debug!(
                                set_name = %self.plan.set_name,
                                volume_index,
                                chunk_offset = offset,
                                chunk_bytes,
                                scratch_bytes = self.scratch.bytes(),
                                scratch_ceiling_bytes = self.scratch.ceiling,
                                "direct-store holds scratch remained at its cap after compaction"
                            );
                            return Err(DemotionReason::HoldsScratchCeiling);
                        }
                        Err(reason) => return Err(reason),
                    }
                }
                Err(reason) => return Err(reason),
            };
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
        if self.publish_holds() > self.holds_budget {
            // Everything pageable is paged and RAM is still over: the budget is
            // smaller than one staged run, which is a configuration the set
            // cannot route inside. The *shared* limit is not judged here: with
            // nothing left to page, this set has done what it can, and the
            // remainder is other sets' to page when they next route.
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

    /// Every scratch extent the set still reads from, ascending by scratch
    /// offset, tagged with the staging slot that points at it.
    fn live_scratch_extents(&self) -> Vec<LiveScratchExtent> {
        let mut extents: Vec<LiveScratchExtent> = self
            .staging
            .iter()
            .flat_map(|(volume_index, staging)| {
                staging
                    .chunks
                    .iter()
                    .filter_map(move |(chunk_offset, chunk)| match chunk {
                        StagedChunk::Scratch { offset, len } => Some(LiveScratchExtent {
                            scratch_offset: *offset,
                            len: *len,
                            volume_index: *volume_index,
                            chunk_offset: *chunk_offset,
                        }),
                        StagedChunk::Memory(_) => None,
                    })
            })
            .collect();
        extents.sort_unstable_by_key(|extent| extent.scratch_offset);
        extents
    }

    /// Reclaims the scratch space that placed holds left behind.
    ///
    /// The scratch is an append-only log. A paged chunk that later gets routed
    /// and placed drops out of `staging`, but the region it occupied stays
    /// inside the file's extent and keeps counting against the ceiling. A set
    /// that pages, places, and pages again therefore reaches the ceiling while
    /// holding far less than the ceiling — and demoting there means
    /// materializing and possibly refetching a set that had room all along.
    ///
    /// `Ok(false)` means there was nothing to reclaim, so the breach is real.
    /// `Err` means the rewrite failed partway and no offset can be trusted.
    fn compact_scratch(&mut self) -> Result<bool, DemotionReason> {
        let extents = self.live_scratch_extents();
        let live_bytes = extents
            .iter()
            .fold(0u64, |total, extent| total.saturating_add(extent.len));
        if live_bytes >= self.scratch.bytes() {
            return Ok(false);
        }

        // Under a pin the pack is a second file beside the first, and the
        // first stays on disk until its readers let go. That copy is scratch
        // like any other spill: admitted against the shared total and the
        // reserve before it is written, and charged for as long as both
        // images exist. A refusal is a demotion, as it is for a spill.
        if self.scratch.is_pinned() {
            self.publish_holds();
            self.accountant
                .admit_scratch(live_bytes, &self.plan.working_dir)?;
        }
        let ranges: Vec<(u64, u64)> = extents
            .iter()
            .map(|extent| (extent.scratch_offset, extent.len))
            .collect();
        let new_offsets = self
            .scratch
            .compact(&ranges)
            .ok_or(DemotionReason::HoldsScratchFailed)?;

        for (extent, scratch_offset) in extents.into_iter().zip(new_offsets) {
            if let Some(staging) = self.staging.get_mut(&extent.volume_index) {
                staging.chunks.insert(
                    extent.chunk_offset,
                    StagedChunk::Scratch {
                        offset: scratch_offset,
                        len: extent.len,
                    },
                );
            }
        }
        self.publish_holds();
        Ok(true)
    }

    pub(crate) fn plan(&self) -> &DirectSetPlan {
        &self.plan
    }

    /// Records one identity binding on the plan. The router keeps no per-volume
    /// state ahead of a volume's first bytes — every internal map is guarded by
    /// `plan.volumes.contains_key` and fills lazily — so growing the mapping
    /// here needs no cache invalidation.
    pub(crate) fn bind_identity_volume(&mut self, volume_index: u32, file_index: u32) -> bool {
        self.plan.bind_identity_volume(volume_index, file_index)
    }

    /// Installs the process-wide accountant this set charges its holds to.
    /// Applied by the runtime to every set it admits, restore included; what
    /// the set had charged elsewhere moves with it.
    pub(crate) fn set_holds_accountant(
        &mut self,
        accountant: std::sync::Arc<super::accountant::HoldsAccountant>,
    ) {
        self.accountant.release(&mut self.charge);
        self.accountant = accountant;
        self.publish_holds();
    }

    /// Publishes this set's resident and scratch bytes to the accountant.
    /// Called wherever staging changes size, so the process total is current
    /// whenever a set consults it.
    ///
    /// Returns the resident figure it published, so the caller that needs it
    /// next does not walk the staging map a second time: the article path
    /// pays exactly the one fold it paid before the accountant existed.
    fn publish_holds(&mut self) -> u64 {
        let resident = self.resident_bytes();
        let scratch = self.scratch.charged_bytes();
        self.accountant.publish(&mut self.charge, resident, scratch);
        resident
    }

    /// Whether this set must page: over its own budget, or holding anything
    /// at all while the process is over the limit every set shares.
    fn holds_over_budget(&mut self) -> bool {
        let resident = self.publish_holds();
        resident > self.holds_budget || (resident > 0 && self.accountant.resident_over_limit())
    }

    /// One spill: admitted by the accountant — the shared scratch total and
    /// the disk reserve — then appended to this set's own scratch, under its
    /// own ceiling. Published either way.
    fn spill_to_scratch(&mut self, bytes: &[u8]) -> Result<u64, DemotionReason> {
        self.accountant
            .admit_scratch(bytes.len() as u64, &self.plan.working_dir)?;
        let appended = self.scratch.append(bytes);
        self.publish_holds();
        appended
    }

    /// Lowers the holds ceiling so a test can breach it without staging tens of
    /// megabytes.
    #[cfg(test)]
    pub(crate) fn set_holds_budget(&mut self, bytes: u64) {
        self.holds_budget = bytes;
    }

    /// The RAM ceiling this set's holds are bounded by. Read by the repair
    /// seam, which must size its rewrite against it *before* reading a byte
    /// back — every repaired byte re-enters the router as a hold.
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
    fn layout_members(&self) -> &[unrar_rs::StoredMember] {
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
    /// walk.
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
    /// incremental extractor makes them collide.
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
    /// the checkpoint's plan digest binds.
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
        !self.repair_batch_in_progress()
            && !self.members.is_empty()
            && self.members.values().all(|member| member.verified)
    }
}

mod encrypted;
mod restart;
mod routing;

/// Whether one ineligible member's **shape** is one the member tolerance can
/// carry to finalization, where it is stream-extracted from the virtual
/// volumes.
///
/// Size is deliberately not an input. The tolerance is a per-member verdict
/// about what `extract_member_streaming` can decode out of the set's own
/// virtual volumes; a member whose shape it can decode is carried whatever its
/// size, and a member whose shape it cannot decode is a set-level demotion
/// however small it is. `check_eligibility` states why the size ceiling this
/// replaces is gone.
///
/// Every `false` arm is a **set** verdict, and each one for its own reason:
///
/// - `Solid` — a solid member is decodable only against the rest of its solid
///   run. `unrar-rs` will do that (`advance_solid_cursor_to` decodes and
///   discards every preceding member to rebuild the dictionary), but the
///   predecessors it has to decode include the *stored* members direct routing
///   carried away, so extracting one tolerated solid member costs a full decode
///   of the archive prefix — the conventional extraction, plus an envelope. It
///   also imposes monotonic member order on the tolerated loop and poisons the
///   decoder for every later solid member once one fails
///   (`RarError::SolidStatePoisoned`). Demoting is both cheaper and simpler.
/// - `Redirection` — a symlink, hardlink, junction or file copy. There are no
///   bytes to decode; the entry has to be *created* as a link, which is
///   `extract_member_to_file`'s job and not something the tolerated writer path
///   does. Until that arm exists (the directory arm is its shape), the
///   conventional extractor owns it.
/// - `NoChecksum` — no whole-member checksum at all. The tolerated decode would
///   produce the member with nothing to check it against, and direct-store's
///   contract is that every byte it delivers was verified by something.
/// - `MalformedChain` — the layout does not agree with itself about where the
///   member's parts are, so nothing here can be trusted to be routing the
///   member's bytes at all.
/// - `Encrypted` — reached only for encryption direct-store cannot route
///   (encrypted *and* compressed, encrypted *and* solid, non-uniform or
///   unkeyable keying). `EncryptedStore` members never reach this predicate.
pub(super) fn member_shape_is_tolerable(reason: IneligibilityReason) -> bool {
    match reason {
        // Ordering, stated exactly: `classify` reaches `Compressed` only after
        // the parse-level malformed reason, directory, redirection, encrypted
        // and solid have all been ruled out, so this arm may rely on those five
        // and on nothing else — which is exactly the tolerance's precondition:
        // an unencrypted, non-solid, per-member regular file.
        IneligibilityReason::Compressed { .. } => true,
        // A stored member with a BLAKE2sp digest and no CRC32. Out-of-order
        // routing cannot verify it, but `extract_member_streaming` feeds the
        // codec in order and checks BLAKE2sp natively — which is exactly why
        // the gate scopes its whole-member-CRC32 requirement to *direct-routed*
        // members and sends this one through the tolerance instead.
        IneligibilityReason::Blake2OnlyNoCrc32 => true,
        // A directory entry, which every archiver writes as a dataless header:
        // there is nothing to decode, and finalization creates it through the
        // extractor's own sandboxed root.
        IneligibilityReason::Directory => true,
        IneligibilityReason::Encrypted
        | IneligibilityReason::Solid
        | IneligibilityReason::Redirection
        | IneligibilityReason::NoChecksum
        | IneligibilityReason::MalformedChain(_) => false,
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
/// finalization re-read must recompute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RestartReadRun {
    pub(crate) member_id: u32,
    pub(crate) relative_partial: String,
    pub(crate) logical_offset: u64,
    pub(crate) len: u64,
}

/// The byte length of any mapped slice, whatever it maps to.
///
/// Its exhaustiveness was, until encrypted members existed, the *only*
/// compile-time guard over [`DirectSetRouter::restore_volume_coverage`]'s
/// refutable `let ... else`: add an arm here and that site went quiet forever.
/// The restore now walks an exhaustive match of its own and no longer depends on
/// it — it still calls this for the arms it skips, deliberately, so a future
/// variant lands as an error in both places rather than one.
fn slice_len(slice: &MappedSlice) -> u64 {
    match slice {
        MappedSlice::Member { len, .. }
        | MappedSlice::EncryptedMember { len, .. }
        | MappedSlice::Envelope { len }
        | MappedSlice::Unroutable { len } => *len,
    }
}

/// What [`DirectSetRouter::refuse_quick_open_derived_facts`] concluded about
/// one parse's facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuickOpenCrossCheck {
    /// The facts never came from the cache; no walk was needed.
    Physical,
    /// The facts came from the cache and the physical walk agrees with them.
    Agreed,
    /// The facts came from the cache, the physical walk stopped short of them
    /// at a hole in an image that is still arriving, and nothing may be adopted
    /// until more of the volume is staged.
    Inconclusive,
}

/// Which reader one volume's headers are parsed out of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VolumeImage {
    /// The bytes the router is still holding for the volume. The only image a
    /// volume this run downloaded ever needs.
    Staged,
    /// The staged bytes **plus** the volume's envelope file, read back at true
    /// physical offsets — the image a restored volume's headers live in, since
    /// its pre-restart bytes were written out and dropped from RAM.
    Envelope,
}

/// What a member header claims about itself, in the fields a routing decision
/// depends on. Compared field for field when a Quick Open cache's answer is
/// cross-examined against the physical walk.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MemberIdentity {
    name: String,
    data_offset: u64,
    data_size: u64,
    split_before: bool,
    split_after: bool,
}

impl MemberIdentity {
    fn of(member: &unrar_rs::RarVolumeMemberFacts) -> Self {
        Self {
            name: member.name.clone(),
            data_offset: member.data_offset,
            data_size: member.data_size,
            split_before: member.split_before,
            split_after: member.split_after,
        }
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
