//! Repair while still direct.
//!
//! Repair draws a line between two things an earlier revision ran together.
//! **Archive-group demotion** ends direct mode: every volume materializes, the
//! partial outputs are deleted and the conventional path takes the set.
//! **Repair while still direct** does none of that — it materializes *only the
//! damaged volumes*, repairs them, routes the repaired spans back through the
//! router, and throws the temporaries away. Clean volumes stay virtual, the set
//! stays direct, and no direct output is deleted. A set with a handful of bad
//! articles is the common case, and it must not cost a demotion.
//!
//! This module owns the blocking half: materialize, repair, read the repaired
//! spans back. Everything that touches pipeline state — the checkpoint delete
//! that has to happen first, the re-route, the stale-gap re-read, the fallback
//! to demotion — lives in [`super::wiring`], because only that half can await.
//!
//! # Which volumes materialize, and why the rest do not have to
//!
//! `par2_rs`'s repairer reads *available* input slices and writes the
//! *missing* ones, both through a [`FileAccess`]. Only the write side needs a
//! real file: a recovered slice has to land somewhere, and a virtual volume's
//! bytes belong to a member partial and an envelope, neither of which is
//! addressable in source-volume space. So the write targets — the damaged
//! volumes, and only those — are materialized into
//! [`super::plan::DirectSetPlan::repair_path`] scratch files, while every clean
//! volume the plan reads as a *source* is answered by the hybrid virtual-volume
//! provider exactly as verification answers it.
//!
//! That is stronger than the "expand to more volumes only if the repair plan
//! requires them as read sources": through this seam a read source never
//! requires materialization at all, so the expansion set is empty by
//! construction. [`super::par2_access::DirectVolumeFileAccess`] keeps refusing
//! `write_file_range` for anything still virtual, so the property is enforced
//! rather than merely intended — a repair that tried to write into a clean
//! volume would fail loudly instead of silently corrupting a member.
//!
//! # Fail-closed, then fall back
//!
//! Every step here can refuse, and every refusal falls back to today's
//! whole-set demotion, which is always correct. Materialization uses the same
//! [`super::reconstruct`] sweep demotion uses — covered runs only, each verified
//! against the yEnc part-CRC composition, an unverifiable run refused rather
//! than written — so a repair is never planned against bytes nothing checked.
//!
//! # There is no post-repair readback, and that is deliberate
//!
//! A conventional `Par2Repairer` verifies its own work: it repairs, then reads
//! the repaired files back and re-checks their slice hashes before calling the
//! job repaired. This path does **not**, and nothing here should be read as an
//! oversight of it.
//!
//! What a readback buys is a check that the bytes now on disk are the bytes PAR2
//! meant to write. Here those bytes never stay on disk: the scratch file is
//! deleted at the end of this call, and what survives is the *re-route*, which
//! is checked by strictly more than a slice hash would be.
//!
//! - Every repaired span re-enters the router, so the integrity layers fire
//!   over it: the per-part packed CRC32 at part completion and the whole-member
//!   CRC32 at member completion, both from the archive's own headers. A repair
//!   that produced wrong bytes fails those gates and demotes the set, which is
//!   the same answer a failed readback would reach and it reaches it over the
//!   bytes that were actually written to the destinations rather than over a
//!   temporary.
//! - The composition gaps the rewrite left are re-read **from the destination
//!   files** and fed back, so the value each gate composes comes off disk after
//!   the write, not out of the repair's own buffers.
//! - The set stays direct, so the job goes round again and the **next PAR2
//!   pass** verifies the repaired volumes virtually, slice for slice, through
//!   the same provider this one read them through. That is the readback,
//!   arriving one lap later and covering the whole recovery set rather than the
//!   repaired files alone.
//!
//! ## The one residual: `WEAVER_PAR2_FAST_VERIFY`
//!
//! That next pass is only equivalent while it is a *full* verify. Fast-verify
//! checks a span per slice rather than the whole slice, and the accounting it
//! produces is per-slice, so a repaired volume re-verified that way would have
//! its per-slice damage counted from a sampled span — re-inflating exactly the
//! accounting repair narrowed to the slices a hole actually touches. Weaver
//! never sets the flag on any path that can reach a direct set (see the pin at
//! [`super::wiring::Pipeline::verify_direct_sets_quietly`]), so the residual is
//! recorded rather than handled; if that ever changes, the repaired volumes
//! need a full pass of their own before this reasoning holds again.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use par2_rs::{FileId, Par2FileSet, VerificationResult};

use super::par2_access::{DirectVolumeFileAccess, MaterializedPar2Volume};
use super::provider::HybridVolumeProvider;
use super::reconstruct::{ReconstructionFailure, VolumeReconstruction, reconstruct_volumes};

/// One damaged direct source volume, as the repair sees it.
#[derive(Debug, Clone)]
pub(crate) struct DamagedDirectVolume {
    pub(crate) volume_index: u32,
    pub(crate) par2_file_id: FileId,
    /// Decoded length of the source volume — the length PAR2 describes.
    pub(crate) len: u64,
    /// Where the materialized copy goes, and what is deleted afterwards.
    pub(crate) path: PathBuf,
    /// Physical ranges the repair is expected to rewrite, already widened to
    /// whole articles wherever the decoded geometry is known.
    ///
    /// Widening is not cosmetic. The composition runs are article-shaped and a
    /// PAR2 slice is not, so a span that stops mid-article discards that
    /// article's composed value and leaves the bytes on either side vouched for
    /// by nothing — the stale composition gaps, each one a bounded read to
    /// close. Reading back whole articles instead means the volume-space
    /// composition is rewritten run for run and leaves no gap at all; the
    /// member-space composition still can, because a member run spans as many
    /// articles as one drain happened to coalesce, and that is what the
    /// stale-gap machinery is for.
    pub(crate) rewrite: Vec<(u64, u64)>,
    /// Everything needed to materialize the volume from the set's own bytes.
    pub(crate) reconstruction: VolumeReconstruction,
}

impl DamagedDirectVolume {
    /// Does the rewrite cover every byte of the volume?
    ///
    /// A volume nobody ever posted has no covered run to materialize from, so
    /// the repair target is created empty at the PAR2-described length and
    /// **every** slice is written by par2. What comes back is therefore not a
    /// patch over a volume the wire already established — it is the volume, in
    /// full, and it is the only image of it that will ever exist.
    ///
    /// That distinction is what the re-route needs. A partial rewrite must not
    /// be allowed to close the classification frontier, because the staged
    /// image is missing whatever the repair did not touch and a header chain
    /// that parses through it proves nothing about the bytes beyond. A rewrite
    /// that covers the volume end to end proves exactly that much: the staged
    /// image *is* the volume, so a parse over it is authoritative and the
    /// volume may be confirmed from itself.
    ///
    /// Deliberately conservative. Anything short of provable end-to-end
    /// coverage — a gap, a rewrite list that is not ascending, a zero-length
    /// volume — answers `false`, which costs only the confirmation shortcut and
    /// leaves the pre-existing behaviour in place.
    pub(crate) fn rewrote_whole_volume(&self) -> bool {
        if self.len == 0 {
            return false;
        }
        let mut cursor = 0u64;
        for &(offset, len) in &self.rewrite {
            if offset > cursor {
                return false;
            }
            cursor = cursor.max(offset.saturating_add(len));
        }
        cursor >= self.len
    }
}

/// Read/stage chunk for the repaired read-back. Matches the reconstruction
/// sweep's [`super::reconstruct`] chunk: large enough that a big span is a few
/// hundred iterations, small enough that the transient buffer is never the term
/// that decides a repair's peak RSS.
const READ_BACK_CHUNK_BYTES: usize = 256 * 1024;

/// One repaired span, ready to re-enter the router.
///
/// The bytes are carried as bounded, already reference-counted chunks rather
/// than one owned buffer, so handing them to
/// [`super::router::DirectSetRouter::route_repaired`] is a move into staging
/// rather than a second copy of the whole span. The `crc32` is still the whole
/// span's — the volume's yEnc composition is article-shaped and must be
/// rewritten at that granularity or [`super::set::DirectSet::note_repaired_volume_crcs`]
/// would tear an article's run into chunk-sized pieces and leave gaps the
/// reconstruction sweep then refuses.
#[derive(Debug, Clone)]
pub(crate) struct RepairedSpan {
    pub(crate) volume_index: u32,
    pub(crate) source_offset: u64,
    /// `(physical offset, bytes)` per chunk, ascending and abutting, together
    /// covering exactly `[source_offset, source_offset + len)`.
    pub(crate) chunks: Vec<super::router::RepairedChunk>,
    pub(crate) len: u64,
    /// CRC32 of the whole span, so the volume's yEnc composition can be
    /// rewritten without hashing the span a second time.
    pub(crate) crc32: u32,
    /// Up to [`CIPHER_LEAD_IN_BYTES`] posted bytes immediately **below**
    /// `source_offset`, for an encrypted set's re-route.
    ///
    /// Deliberately not part of `chunks`, `len` or `crc32`: these bytes did not
    /// change, they are staged only so the drain can decrypt the span above
    /// them, and folding them into the span would have
    /// [`super::set::DirectSet::note_repaired_volume_crcs`] rewrite the volume's
    /// article-shaped composition over a range that is not article-shaped — the
    /// one thing [`widen_to_articles`] exists to prevent.
    pub(crate) lead_in: Option<super::router::RepairedChunk>,
}

/// What the repaired volumes came out as.
///
/// Deliberately **no bytes**: the read-back is streamed per volume by
/// [`read_repaired_spans`], immediately before that volume's spans are routed
/// and dropped again. The first shape read every rewrite span of every damaged
/// volume into this struct and let the router copy them again on the way in, so
/// a three-volume repair of 500 MiB volumes peaked at about 3 GiB twice over,
/// bounded by nothing.
#[derive(Debug)]
pub(crate) struct DirectRepairOutcome {
    /// Scratch files to delete once the spans are routed — or immediately, if
    /// routing them fails.
    pub(crate) scratch: Vec<PathBuf>,
    pub(crate) recovery_blocks_used: usize,
    /// Volumes the materialization sweep actually rebuilt. Counted here rather
    /// than at the call site because the call site knows only what it *asked*
    /// for: a plan that refused before reconstruction would have been counted as
    /// a materialization that never happened.
    pub(crate) materialized_volumes: usize,
}

/// Why a set could not be repaired while direct. Each is a metric bucket, and
/// each falls back to the whole-set demotion shipped earlier.
#[derive(Debug, Clone)]
pub(crate) enum DirectRepairFailure {
    /// PAR2 says the damage cannot be repaired with the recovery on hand.
    /// Demoting is what today does and what the conventional path expects: it
    /// materializes the volumes and lets the job reach the same dead end it
    /// would have reached with the gate off.
    Unrepairable,
    /// Damage reaches a file this set does not own, so a repair here would be
    /// planned over half the recovery set. Handing the whole job to the
    /// conventional repairer is the honest answer.
    DamageOutsideDirectSets,
    /// The rewrite is larger than the set's holds budget, so re-routing it would
    /// put more bytes into staging than the ceiling that exists to bound them.
    ///
    /// Checked before anything is materialized, so an over-budget repair costs
    /// the set nothing at all — not even the checkpoint delete. **A later pass
    /// revisits this**: the bound is the honest one for a repair that re-enters
    /// the router in one pass, and lifting it means routing a repaired volume
    /// in budget-sized instalments, each drained and written before the next is
    /// read. Until then a whole-volume repair of a large set demotes, which is
    /// slower but always correct.
    RewriteOverBudget { bytes: u64, budget: u64 },
    /// This set has already had its one repair-while-direct attempt.
    ///
    /// The bound, not a diagnosis: a second damage verdict after a completed
    /// repair means the repair did not fix what the verifier reads, and
    /// repeating it would reach the same verdict on every completion check
    /// forever. See [`super::set::DirectSet::repair_attempted`].
    AlreadyRepaired,
    /// A damaged volume could not be materialized from the set's own routed
    /// bytes — the same fail-closed sweep demotion uses.
    Materialization(ReconstructionFailure),
    /// `plan_repair` refused: an unusable matrix, a resource limit, a set the
    /// planner will not touch.
    PlanRefused(String),
    /// The repair itself failed.
    ExecuteFailed(String),
    /// The plan wants to write into a file the materialization did not produce.
    /// Nothing here should be able to reach it — the write targets are derived
    /// from the same verification the plan is — so it is a refusal rather than
    /// an assert, and it never lets a virtual volume be a write target.
    UnmaterializedWriteTarget,
    /// The repaired bytes could not be read back out of the scratch file.
    ReadBackFailed { volume_index: u32, error: String },
}

impl DirectRepairFailure {
    pub(crate) fn metric(&self) -> &'static str {
        match self {
            Self::Unrepairable => "unrepairable",
            Self::DamageOutsideDirectSets => "damage_outside_direct_sets",
            Self::RewriteOverBudget { .. } => "rewrite_over_budget",
            Self::AlreadyRepaired => "already_repaired",
            Self::Materialization(_) => "materialization_failed",
            Self::PlanRefused(_) => "plan_refused",
            Self::ExecuteFailed(_) => "execute_failed",
            Self::UnmaterializedWriteTarget => "unmaterialized_write_target",
            Self::ReadBackFailed { .. } => "read_back_failed",
        }
    }
}

impl std::fmt::Display for DirectRepairFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unrepairable => {
                formatter.write_str("the damage exceeds the available recovery blocks")
            }
            Self::DamageOutsideDirectSets => {
                formatter.write_str("PAR2 damage reaches files no direct set owns")
            }
            Self::RewriteOverBudget { bytes, budget } => write!(
                formatter,
                "the repair would re-route {bytes} bytes through a {budget}-byte holds budget"
            ),
            Self::AlreadyRepaired => {
                formatter.write_str("the set has already been repaired once while direct")
            }
            Self::Materialization(failure) => {
                write!(
                    formatter,
                    "a damaged volume could not be rebuilt: {failure}"
                )
            }
            Self::PlanRefused(error) => {
                write!(formatter, "the repair could not be planned: {error}")
            }
            Self::ExecuteFailed(error) => write!(formatter, "the repair failed: {error}"),
            Self::UnmaterializedWriteTarget => formatter
                .write_str("the repair plan wants to write a file that was not materialized"),
            Self::ReadBackFailed {
                volume_index,
                error,
            } => write!(
                formatter,
                "volume {volume_index} could not be read back after repair: {error}"
            ),
        }
    }
}

/// The physical ranges of one file's damaged slices, coalesced.
///
/// `valid_slices` is the per-slice verdict both verification pipelines produce,
/// and it is the only thing that says *where* a file is damaged. A file the pass
/// reported `Missing` has every slice invalid, which comes out of here as the
/// whole volume — correct, and what makes a volume that never arrived repairable
/// like any other.
pub(crate) fn damaged_ranges(valid_slices: &[bool], slice_size: u64, len: u64) -> Vec<(u64, u64)> {
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    for (index, valid) in valid_slices.iter().enumerate() {
        if *valid {
            continue;
        }
        let start = (index as u64).saturating_mul(slice_size);
        if start >= len {
            break;
        }
        let end = start.saturating_add(slice_size).min(len);
        match ranges.last_mut() {
            Some((_, last_end)) if *last_end == start => *last_end = end,
            _ => ranges.push((start, end)),
        }
    }
    ranges
}

/// Widens `ranges` to the article boundaries `extents` describes, so a rewrite
/// covers whole articles wherever the decoded geometry is known.
///
/// Ranges outside every recorded article — a slice in a region the set never
/// received a byte of — are kept as they are. There is no composed run there to
/// half-cover, so nothing is lost by not widening.
pub(crate) fn widen_to_articles(
    ranges: &[(u64, u64)],
    extents: &std::collections::BTreeMap<u32, (u64, u64)>,
    len: u64,
) -> Vec<(u64, u64)> {
    let mut widened: Vec<(u64, u64)> = Vec::new();
    for &(start, end) in ranges {
        let mut start = start;
        let mut end = end.min(len);
        if end <= start {
            continue;
        }
        for &(article_start, article_len) in extents.values() {
            let article_end = article_start.saturating_add(article_len).min(len);
            if article_end <= start || article_start >= end {
                continue;
            }
            start = start.min(article_start);
            end = end.max(article_end);
        }
        match widened.last_mut() {
            Some((_, last_end)) if *last_end >= start => *last_end = (*last_end).max(end),
            _ => widened.push((start, end)),
        }
    }
    widened
}

/// How many posted bytes are read back below a repaired span so an **encrypted**
/// member's re-route has its CBC predecessor.
///
/// A repaired span decrypts on the way in, which needs the whole of the cipher
/// block its first byte lands in **and** the 16 bytes before that block. During
/// a download both belong to the previous article and are staged beside it; a
/// repair runs long after that article's bytes were routed and dropped, so
/// without a lead-in the span has no predecessor at all — it holds, and
/// `route_repaired`'s "every repaired byte must find a destination" rule demotes
/// the whole set under `RepairRerouteFailed`.
///
/// Two blocks is exactly enough for any alignment: the span's first byte sits at
/// most 15 bytes into its block, and that block's own predecessor is 16 more.
/// The bytes come off the **materialized** volume, so they are the posted ones.
const CIPHER_LEAD_IN_BYTES: u64 = 32;

/// Materializes the damaged volumes and repairs them. Blocking: call it on the
/// blocking pool.
///
/// The repaired bytes are **not** read back here. That is
/// [`read_repaired_spans`], called per volume immediately before that volume's
/// spans are routed, so only one volume's rewrite is ever resident.
///
/// The caller must already have deleted the set's checkpoint row: the re-route
/// this produces overwrites bytes the row claims, and a row that survived it
/// would let a restart trust floors over bytes that changed.
// Every argument is an independent input the blocking half genuinely needs, and
// they cross a `spawn_blocking` boundary — bundling them into a struct would
// mean a type that exists only to be destructured on the other side.
#[allow(clippy::too_many_arguments)]
pub(crate) fn repair_damaged_volumes(
    par2_set: &Par2FileSet,
    verification: &VerificationResult,
    provider: &HybridVolumeProvider,
    inner: par2_rs::PlacementFileAccess,
    virtual_volumes: &[super::par2_access::VirtualPar2Volume],
    damaged: &[DamagedDirectVolume],
    memory_limit: Option<usize>,
    sparse: super::sparse::SparseMarking,
) -> Result<DirectRepairOutcome, DirectRepairFailure> {
    let scratch: Vec<PathBuf> = damaged.iter().map(|volume| volume.path.clone()).collect();
    let cleanup = |failure: DirectRepairFailure| {
        for path in &scratch {
            let _ = std::fs::remove_file(path);
        }
        failure
    };

    let plans: Vec<VolumeReconstruction> = damaged
        .iter()
        .map(|volume| volume.reconstruction.clone())
        .collect();
    // All or nothing *here*, deliberately, and it is `cleanup` that makes it so:
    // the repair needs every damaged volume present as a scratch file before a
    // single slice can be recovered, so one volume that will not materialize
    // ends the repair and takes the rest of the scratch with it. The sweep
    // itself now reports per volume, which is what the demotion path wants; this
    // caller collapses that back to the first failure.
    if let Some(failure) = reconstruct_volumes(provider, &plans, sparse)
        .into_iter()
        .find_map(|volume| volume.failure)
    {
        return Err(cleanup(DirectRepairFailure::Materialization(failure)));
    }
    if let Err(failure) = create_absent_repair_targets(damaged, sparse) {
        return Err(cleanup(DirectRepairFailure::Materialization(failure)));
    }

    let materialized: Vec<MaterializedPar2Volume> = damaged
        .iter()
        .map(|volume| MaterializedPar2Volume {
            par2_file_id: volume.par2_file_id,
            path: volume.path.clone(),
            len: volume.len,
        })
        .collect();
    let materialized_ids: std::collections::HashSet<FileId> = materialized
        .iter()
        .map(|volume| volume.par2_file_id)
        .collect();
    let mut access = DirectVolumeFileAccess::new(inner, provider.clone(), virtual_volumes)
        .with_materialized(materialized);

    let plan = match par2_rs::plan_repair_with_memory_limit(par2_set, verification, memory_limit) {
        Ok(plan) => plan,
        Err(error) => {
            return Err(cleanup(match &verification.repairable {
                par2_rs::verify::Repairability::Insufficient { .. } => {
                    DirectRepairFailure::Unrepairable
                }
                _ => DirectRepairFailure::PlanRefused(error.to_string()),
            }));
        }
    };
    // The write targets and the materialization both come from `verification`,
    // so this can only fire if the two drifted. It is checked anyway, because
    // the failure it prevents is a repair writing into a still-virtual volume —
    // which `write_file_range` would refuse, but only after the plan had already
    // spent its recovery blocks.
    if plan
        .missing_slices
        .iter()
        .any(|(file_id, _)| !materialized_ids.contains(file_id))
    {
        return Err(cleanup(DirectRepairFailure::UnmaterializedWriteTarget));
    }

    let recovery_blocks_used = plan.recovery_exponents.len();
    let options = par2_rs::RepairOptions {
        memory_limit,
        ..Default::default()
    };
    if let Err(error) = par2_rs::execute_repair_with_options(
        &plan,
        par2_set,
        &mut access as &mut dyn par2_rs::FileAccess,
        &options,
    ) {
        return Err(cleanup(DirectRepairFailure::ExecuteFailed(
            error.to_string(),
        )));
    }

    Ok(DirectRepairOutcome {
        scratch,
        recovery_blocks_used,
        materialized_volumes: damaged.len(),
    })
}

/// Gives a **wholly absent** volume the empty file its repair writes into.
///
/// The reconstruction sweep produces nothing for a volume with no covered runs:
/// it has no bytes to write and — on the demotion path it is shared with — no
/// file it may leave in front of a refetch that publishes no floor over one.
/// That is right there and wrong here. A volume whose every article failed is
/// still fully described by PAR2: it knows the length, it knows every slice,
/// and with enough recovery it can write all of them. What it cannot do is
/// write them into a file that does not exist — [`DirectVolumeFileAccess`]
/// opens the materialized path without `create`, so the first slice fails
/// `ENOENT` at offset 0 and the set demotes *after* the targeted recovery has
/// already been downloaded, which is the most expensive moment to give up.
///
/// So the target is created here, at the **PAR2-described** length, sparse and
/// wholly holes, and the repair fills it slice by slice. Only volumes the sweep
/// left no file for are touched, so a volume that really was reconstructed is
/// never re-marked after its bytes were written — the ordering rule
/// [`super::sparse`] states.
fn create_absent_repair_targets(
    damaged: &[DamagedDirectVolume],
    sparse: super::sparse::SparseMarking,
) -> Result<(), ReconstructionFailure> {
    for volume in damaged {
        if volume.len == 0 || volume.path.exists() {
            continue;
        }
        let file =
            super::sparse::create_sparse(&volume.path, &sparse).map_err(|error| match error {
                super::sparse::SparseCreateError::Open(error) => {
                    ReconstructionFailure::WriteFailed {
                        volume_index: volume.volume_index,
                        error: error.to_string(),
                    }
                }
                super::sparse::SparseCreateError::Mark(error) => {
                    ReconstructionFailure::SparseMarkFailed {
                        volume_index: volume.volume_index,
                        error: error.to_string(),
                    }
                }
            })?;
        // After the marking and never before it: on Windows the length is what
        // makes NTFS allocate, so an unmarked file sized here would cost the
        // whole volume in zeros.
        file.set_len(volume.len)
            .map_err(|error| ReconstructionFailure::WriteFailed {
                volume_index: volume.volume_index,
                error: error.to_string(),
            })?;
    }
    Ok(())
}

/// Reads one repaired volume's rewrite spans back off its scratch file, in
/// bounded chunks. Blocking: call it on the blocking pool.
///
/// Called per volume rather than for the whole set, and its result routed and
/// dropped before the next volume is read, so a repair's resident cost is one
/// volume's rewrite plus one chunk — never the whole set's, and never twice
/// over. Taking a *whole volume* at a time is not incidental either: the router
/// needs all of one volume's spans in a single `route_repaired` call, or a span
/// staged before its volume's end record would be held rather than routed.
pub(crate) fn read_repaired_spans(
    volume: &DamagedDirectVolume,
    cipher_lead_in: bool,
) -> Result<Vec<RepairedSpan>, DirectRepairFailure> {
    let mut spans = Vec::with_capacity(volume.rewrite.len());
    for &(start, end) in &volume.rewrite {
        match read_span_chunked(&volume.path, start, end.saturating_sub(start)) {
            Ok(Some(span)) => {
                let lead_in = match cipher_lead_in && start > 0 {
                    true => {
                        let from = start.saturating_sub(CIPHER_LEAD_IN_BYTES);
                        read_span_chunked(&volume.path, from, start - from)
                            .map_err(|error| DirectRepairFailure::ReadBackFailed {
                                volume_index: volume.volume_index,
                                error: error.to_string(),
                            })?
                            .and_then(|lead| lead.chunks.into_iter().next())
                    }
                    false => None,
                };
                spans.push(RepairedSpan {
                    volume_index: volume.volume_index,
                    lead_in,
                    ..span
                });
            }
            Ok(None) => {}
            Err(error) => {
                return Err(DirectRepairFailure::ReadBackFailed {
                    volume_index: volume.volume_index,
                    error: error.to_string(),
                });
            }
        }
    }
    Ok(spans)
}

/// Reads exactly `len` bytes at `offset` as bounded chunks, combining their
/// CRC32s into the span's. A short read is an error: the repaired volume is a
/// whole file at this point, so anything less means the repair did not write
/// what the plan said it would.
///
/// `volume_index` is left at zero for the caller to fill in — it is the only
/// field this function has no business knowing.
fn read_span_chunked(
    path: &std::path::Path,
    offset: u64,
    len: u64,
) -> std::io::Result<Option<RepairedSpan>> {
    use std::io::{Read, Seek, SeekFrom};

    if len == 0 {
        return Ok(None);
    }
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;

    let mut chunks: Vec<super::router::RepairedChunk> = Vec::new();
    let mut crc32 = 0u32;
    let mut cursor = offset;
    let end = offset.saturating_add(len);
    let mut buffer = vec![0u8; READ_BACK_CHUNK_BYTES];
    while cursor < end {
        let want = usize::try_from(end - cursor)
            .unwrap_or(READ_BACK_CHUNK_BYTES)
            .min(READ_BACK_CHUNK_BYTES);
        file.read_exact(&mut buffer[..want])?;
        let chunk = &buffer[..want];
        crc32 = weaver_yenc::crc32_combine(crc32, par2_rs::checksum::crc32(chunk), want as u64);
        chunks.push((cursor, Arc::from(chunk)));
        cursor = cursor.saturating_add(want as u64);
    }
    Ok(Some(RepairedSpan {
        volume_index: 0,
        source_offset: offset,
        chunks,
        len,
        crc32,
        lead_in: None,
    }))
}

/// Groups damaged verification entries by the direct set that owns them.
///
/// `owner` answers "which live direct set owns this PAR2 file", which is exactly
/// the binding `direct_par2_overlay` resolved. A damaged file with no owner is
/// [`DirectRepairFailure::DamageOutsideDirectSets`]: repairing half a recovery
/// set through this seam while the conventional repairer owns the other half is
/// not a shape worth having.
pub(crate) fn damaged_files_by_set(
    verification: &VerificationResult,
    owner: impl Fn(&FileId) -> Option<usize>,
) -> Result<HashMap<usize, Vec<FileId>>, DirectRepairFailure> {
    let mut by_set: HashMap<usize, Vec<FileId>> = HashMap::new();
    for file in &verification.files {
        if matches!(
            file.status,
            par2_rs::verify::FileStatus::Complete | par2_rs::verify::FileStatus::Renamed(_)
        ) {
            continue;
        }
        match owner(&file.file_id) {
            Some(set_index) => by_set.entry(set_index).or_default().push(file.file_id),
            None => return Err(DirectRepairFailure::DamageOutsideDirectSets),
        }
    }
    Ok(by_set)
}
