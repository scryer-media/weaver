//! Where direct-store meets the download pipeline.
//!
//! Three seams, and nothing else:
//!
//! 1. **Admission** — the first decoded segment of a job admits its RAR sets
//!    from the job spec. Sets are named and their volume-to-file mapping fixed
//!    before a byte is written, which is what the coverage barrier needs and
//!    what the completion-gated topology layer cannot give.
//! 2. **Routing** — [`Pipeline::handle_direct_decode_success`] replaces the
//!    conventional write for a direct source volume: it maps the span, writes
//!    every destination it touches in one multi-path batch, and only then
//!    records coverage, feeds live PAR2 and commits the segment.
//! 3. **Finalization / demotion** — a set whose members all pass the
//!    whole-member gate commits its partials to the extractor's destinations in
//!    archive order and is marked extracted; a set that demotes materializes
//!    its volumes from its own routed bytes, persists the legacy state that
//!    replaces its coverage, and hands them to the conventional path — falling
//!    back to refetching everything only when reconstruction is impossible.
//!
//! # Suppression points
//!
//! Successful routing returns before `persist_ready_segments`, so for a direct
//! source volume there is no physical write, **no `active_file_progress` floor
//! upsert**, and no `commit_persisted_segment`. A demotion returns the still-live
//! article to that conventional seam instead. The file-complete work successful
//! routing would have done is re-implemented here without the parts that need a file:
//! **no completed-file row**, no whole-volume hashing, no archive re-probe and
//! no incremental-extraction dispatch. Live reporting keeps using
//! `FileAssembly`, which is source-space truth either way.
//!
//! Suppressing it *here* is not enough, because these are not the only callers:
//!
//! - `refresh_archive_state_for_completed_file` has nine callers — completion
//!   checks, PAR2 merge, RAR finalization, the job service — every one of which
//!   fires for a complete file whether or not routing suppressed its own call.
//!   It carries the rule at its own entry instead.
//! - `try_rar_extraction` is job-scoped and needs no guard: it dispatches from
//!   the archive topology, and the topology's only non-test writer is
//!   `try_update_archive_topology`, whose only non-test caller is the refresh
//!   above. A direct set therefore never enters the topology at all.
//! - The completed-file row has exactly one pipeline writer, in the conventional
//!   file-complete path successful routing returns before.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::time::Instant;

use tracing::{debug, info, warn};

use super::barrier::{BarrierDemand, BarrierDrain, DatabaseCoveragePersist, DestinationSync};
use super::plan::{DirectSetPlan, IdentityPlanFacts};
use super::reconstruct::{ReconstructionFailure, VolumeReconstruction};
use super::router::{DemotionReason, DirectDestination, RoutedSpan};
use super::set::DirectSet;
use super::sparse::SparseMarking;
use super::{DirectStoreGate, DirectStoreSettings};
use crate::DownloadWork;
use crate::events::model::PipelineEvent;
use crate::jobs::assembly::write_buffer::{BufferedChunk, WriteReorderBuffer};
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::pipeline::{
    BufferedDecodedSegment, DecodedChunk, DirectPostRepairCarry, DirectPostRepairWork,
    DirectPostRepairWorkDone, Pipeline,
};

/// Read chunk for the restart gate re-arm. Matches the reconstruction sweep's:
/// large enough that a big part is a few hundred iterations, small enough to
/// keep the whole plan's resident cost to one buffer.
const REARM_CHUNK_BYTES: usize = 256 * 1024;

#[derive(Clone, Default)]
struct PendingDemotionMaterialization {
    files: HashSet<NzbFileId>,
    handoffs: HashSet<SegmentId>,
    rescued: HashSet<SegmentId>,
}

/// One volume of an identity roster: the facts a file's first decoded bytes
/// are matched against. Straight from the recovery set's file description —
/// the same window semantics as the PAR2 content binder, whose fingerprint is
/// `md5(min(length, 16 KiB))` of the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IdentityRosterVolume {
    pub(crate) hash_16k: [u8; 16],
    pub(crate) length: u64,
}

/// One archive set the recovery metadata describes, tracked from arming until
/// the set finalizes, demotes, or proves unfillable.
#[derive(Debug, Default)]
pub(crate) struct IdentityRoster {
    /// Volume index to identity facts. Dense from zero, complete at arming.
    pub(crate) volumes: BTreeMap<u32, IdentityRosterVolume>,
    /// NZB file index to the volume it matched. Grows as files bind.
    pub(crate) bound: HashMap<u32, u32>,
    /// Index into the job's set vector once the first binding admitted the
    /// set. Stable: sets are only ever pushed, never removed, while a job
    /// lives.
    pub(crate) set_index: Option<usize>,
}

/// Per-job identity-admission state: rosters awaiting or holding bindings,
/// plus the evidence that decides when a roster can no longer be filled.
///
/// # Why this exists at all
///
/// [`DirectSetPlan::discover`] admits from the NZB's filenames, and an
/// obfuscated post carries none worth reading — every file is a hex string
/// with no role, so discovery finds nothing and the job settles conventional
/// forever, even though its PAR2 metadata names every real volume. This state
/// is the byte-driven second chance: the recovery set's descriptions supply
/// the roster (set names, dense volume indices, per-volume content
/// fingerprints), and each file identifies *itself* at the routing seam, by
/// hashing the first bytes of its offset-zero article against that roster —
/// before any of its bytes have been written anywhere.
///
/// # The one invariant
///
/// A file may only bind while it has **zero** conventionally written bytes.
/// The envelope model owns every byte of a routed volume; a volume whose
/// early articles already landed in a conventional file would leave the set
/// half-owned, its barrier waiting on bytes that live elsewhere. So arming
/// refuses rosters any of whose volumes may already have leaked, the seam
/// marks every conventionally written file, and a marked file that later
/// proves to *be* a roster volume condemns that roster instead of joining it.
#[derive(Debug, Default)]
pub(crate) struct IdentityAdmission {
    /// Set name to roster.
    pub(crate) rosters: HashMap<String, IdentityRoster>,
    /// Sets admitted from the volumes' own RAR5 headers — the rung for a
    /// post with no PAR2 anywhere. Mutually exclusive with `rosters` by
    /// construction: the header rung only fires while no rosters are armed,
    /// and arming skips a job whose header sets are live, so the two kinds
    /// of evidence never bid for the same file.
    pub(crate) header_sets: Vec<HeaderSet>,
    /// A header-declared volume position was claimed twice — two interleaved
    /// header-only sets, which nothing in the bytes can tell apart. Latched:
    /// no further header volume-set may form for this job, because a third
    /// claimant would resurrect exactly the ambiguity that was just refused.
    pub(crate) header_volume_sets_poisoned: bool,
    /// Files with at least one conventionally written segment. Never bindable.
    pub(crate) leaked: HashSet<u32>,
    /// Files whose offset-zero bytes were evaluated and matched no roster
    /// volume — the extras: samples, nfo files, unrelated payload.
    pub(crate) no_match: HashSet<u32>,
}

/// One set admitted from RAR5 headers rather than PAR2 descriptions.
///
/// The header is both the identity evidence and the position: a RAR5 volume
/// states its own number, so binding needs no roster — but the set's size is
/// unknowable until the final volume's end record parses (the plan stays
/// open; see [`super::plan::IdentityPlanFacts::expected_volumes`]), and the
/// set's name is synthetic, which costs nothing because member destinations
/// derive from the member names inside the archive, never from the set name.
#[derive(Debug)]
pub(crate) struct HeaderSet {
    /// Index into the job's set vector. Stable: sets are only pushed.
    pub(crate) set_index: usize,
    /// NZB file index to the volume position its header declared.
    pub(crate) bound: HashMap<u32, u32>,
    /// Whether this is the job's volume set (RAR5 volume flag) as opposed to
    /// a standalone archive. At most one volume set exists per job — the
    /// bytes carry positions but no set identity, so a second one is
    /// indistinguishable interleaving and is refused.
    pub(crate) volume_set: bool,
}

/// Per-pipeline direct-store state. Empty and inert while the gate is off.
#[derive(Default)]
pub(crate) struct DirectStoreRuntime {
    /// Resolved once at pipeline construction from config plus the env
    /// override, and never re-read: a set admitted under an enabled gate must
    /// not find it disabled at finalization. `None` only in the tests that
    /// build a runtime by hand, where [`Self::gate`] falls back to the
    /// all-defaults resolution (gate off).
    settings: Option<DirectStoreSettings>,
    /// Jobs whose spec has already been examined for candidate sets.
    examined: HashSet<JobId>,
    /// Jobs whose archive-password harvest has already been handed to their
    /// sets' `-hp` gates. Separate from [`Self::examined`]
    /// because a **restored** job is examined without ever passing through the
    /// admission seam, and its sets still need candidates.
    header_candidates_offered: HashSet<JobId>,
    sets: HashMap<JobId, Vec<DirectSet>>,
    /// Destinations already created and marked sparse, per job. A member stored
    /// inside a directory names a partial inside that directory and nothing
    /// else creates it, and every destination has to carry the sparse attribute
    /// before its first routed byte.
    prepared_destinations: HashMap<JobId, HashSet<PathBuf>>,
    /// Member names **direct finalization** wrote into `extracted_members`, per
    /// job. `extracted_members` blends two sources — the incremental extractor
    /// and direct sets — and the claim assertions need them apart: two sets of
    /// one job may legitimately finalize the same member *name* (last rename
    /// wins, as two conventionally extracted archives resolve), and without
    /// this record a sibling's finalized name is indistinguishable from an
    /// extraction checkpoint claiming ours.
    direct_extracted_members: HashMap<JobId, HashSet<String>>,
    /// Waves of targeted recovery a job's direct sets have waited for rather
    /// than demoting, per job.
    ///
    /// The termination budget for the defer, and nothing else. The structural
    /// bound is already there — the first wave asks for every block the verdict
    /// needs, so a second one only happens if the first arrived and still did
    /// not cover the damage — but "already promoted" is derived state, and a
    /// derivation that goes wrong here waits forever. Counting the waves makes
    /// the bound arithmetic instead. Deliberately **not** persisted: after a
    /// restart the damage is re-detected from scratch and the defer re-derives
    /// itself, so a stale count would only shorten a fresh job's budget.
    repair_defer_waves: HashMap<JobId, u32>,
    /// Demoted source volumes that have not reached the conventional durable
    /// seam yet, grouped by the direct set that owned them.
    pending_materializations: HashMap<JobId, HashMap<usize, PendingDemotionMaterialization>>,
    /// Identity-admission state for jobs whose spec named no candidate sets
    /// but whose PAR2 metadata describes some. See [`IdentityAdmission`].
    pub(crate) identity: HashMap<JobId, IdentityAdmission>,
    /// Test-only holds ceiling applied to every set this runtime admits.
    #[cfg(test)]
    holds_budget_override: Option<u64>,
    /// Test-only scratch ceiling, which shortcuts the configured one so a
    /// breach is reachable without paging half a gigabyte.
    #[cfg(test)]
    holds_scratch_ceiling_override: Option<u64>,
    /// Sparse marker for every file this runtime's sets create. Only the tests
    /// that drive the marking-failure demotion ever change it.
    sparse: SparseMarking,
    /// Source volumes a repair has materialized over this pipeline's life.
    ///
    /// Counted because "only the damaged volumes materialize" is the claim
    /// repair-while-direct rests on, and no artefact survives to prove it
    /// afterwards: the scratch is deleted as soon as its spans are routed, so a
    /// run that quietly materialized every volume of the set and then tidied up
    /// would look identical on disk to one that materialized a single volume.
    #[cfg(test)]
    pub(crate) repair_materialized_volumes: usize,
    /// Sets that committed their members from their own partials, ever.
    #[cfg(test)]
    pub(crate) finalized_sets: usize,
    /// Repairs that got as far as the checkpoint delete — the one irreversible
    /// step — over this pipeline's life.
    ///
    /// The repair once-latch is only observable as a *count*. Every other trace
    /// a second attempt leaves is one a first attempt leaves too, and an attempt
    /// that refuses somewhere downstream is indistinguishable from one that was
    /// never made: the scratch is deleted either way, and
    /// [`Self::repair_materialized_volumes`] counts successful repairs only.
    #[cfg(test)]
    pub(crate) repair_attempts: usize,
    /// Recovery blocks a repair spent, which is one per damaged slice.
    ///
    /// The number the damage accounting produces, in the only form that can be
    /// checked end to end: a count inflated by a sequential sweep stopping at an
    /// interior hole shows up here as blocks spent rebuilding slices that were
    /// never broken.
    #[cfg(test)]
    pub(crate) repair_recovery_blocks_used: usize,
    /// Damage verdicts that were answered by waiting for targeted recovery
    /// instead of repairing or demoting, over this pipeline's life.
    ///
    /// Counted for the same reason [`Self::repair_attempts`] is: a defer leaves
    /// no artefact. Nothing is materialized, nothing is deleted, the set is
    /// exactly as it was — which is the whole point, and which makes a defer
    /// indistinguishable from a pass that found nothing to do.
    #[cfg(test)]
    pub(crate) repair_defers: usize,
}

impl std::fmt::Debug for DirectStoreRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectStoreRuntime")
            .field("jobs", &self.sets.len())
            .finish()
    }
}

impl DirectStoreRuntime {
    /// Builds a runtime from the settings resolved at pipeline construction
    /// (config, with the env override winning).
    pub(crate) fn with_settings(settings: DirectStoreSettings) -> Self {
        Self {
            settings: Some(settings),
            ..Self::default()
        }
    }

    pub(crate) fn settings(&self) -> DirectStoreSettings {
        self.settings.unwrap_or_default()
    }

    pub(crate) fn gate(&mut self) -> DirectStoreGate {
        self.settings().gate
    }

    /// Test hook: whether the once-per-job `-hp` harvest has already run for
    /// this job.
    ///
    /// Only one test reads it, and only to establish the *precondition* of the
    /// thing it is testing: once this is true the harvest can never run again,
    /// so a password supplied later has exactly one route left into the `-hp`
    /// ring — the per-article re-offer in [`Pipeline::refresh_direct_passwords`].
    #[cfg(test)]
    pub(crate) fn header_candidates_offered(&self, job_id: JobId) -> bool {
        self.header_candidates_offered.contains(&job_id)
    }

    /// Test hook: force the gate without going through a config load.
    #[cfg(test)]
    pub(crate) fn set_gate(&mut self, gate: DirectStoreGate) {
        let mut settings = self.settings();
        settings.gate = gate;
        self.settings = Some(settings);
    }

    /// Test hook: lower the holds ceiling so a breach is reachable without
    /// staging tens of megabytes.
    #[cfg(test)]
    pub(crate) fn set_holds_budget(&mut self, bytes: u64) {
        self.holds_budget_override = Some(bytes);
    }

    /// Test hook: lower the scratch ceiling so a breach is reachable without
    /// paging half a gigabyte.
    #[cfg(test)]
    pub(crate) fn set_holds_scratch_ceiling(&mut self, bytes: u64) {
        self.holds_scratch_ceiling_override = Some(bytes);
    }

    /// Test hook: pre-spend the defer budget, so the exhausted arm is reachable
    /// without actually downloading three waves of recovery.
    #[cfg(test)]
    pub(crate) fn set_repair_defer_waves(&mut self, job_id: JobId, waves: u32) {
        self.repair_defer_waves.insert(job_id, waves);
    }

    /// Test hook: make every sparse marking attempt fail, which is the only way
    /// to reach the sparse-marking demotion arm on a platform whose marker
    /// cannot fail.
    #[cfg(test)]
    pub(crate) fn set_sparse_marking(&mut self, marking: SparseMarking) {
        self.sparse = marking;
    }

    pub(crate) fn sparse_marking(&self) -> SparseMarking {
        self.sparse
    }

    /// Applies this runtime's configured ceilings and sparse marker to a set it
    /// is about to own.
    ///
    /// Every path that builds a `DirectSet` goes through here, restore
    /// included. Restore used to skip it, so a restart test could set a budget
    /// and then watch the restored set quietly use the 64 MiB / 512 MiB
    /// defaults — which makes every budget assertion about a restored set
    /// vacuous, and those are exactly the assertions the holds ceilings need
    /// after a restart.
    pub(crate) fn apply_ceilings(&self, set: &mut DirectSet) {
        set.router
            .set_holds_scratch_ceiling(self.settings().holds_scratch_ceiling_bytes);
        set.router.set_sparse_marking(self.sparse);
        #[cfg(test)]
        {
            if let Some(bytes) = self.holds_budget_override {
                set.router.set_holds_budget(bytes);
            }
            if let Some(bytes) = self.holds_scratch_ceiling_override {
                set.router.set_holds_scratch_ceiling(bytes);
            }
        }
    }

    /// Drops every trace of a job. Called from the job-removal seam: a barrier
    /// for a job that no longer exists must stop being polled, and its sets
    /// hold the routed byte state of a working directory that is being deleted.
    pub(crate) fn clear_job(&mut self, job_id: JobId) {
        self.sets.remove(&job_id);
        self.examined.remove(&job_id);
        self.header_candidates_offered.remove(&job_id);
        self.prepared_destinations.remove(&job_id);
        self.direct_extracted_members.remove(&job_id);
        self.repair_defer_waves.remove(&job_id);
        self.pending_materializations.remove(&job_id);
        self.identity.remove(&job_id);
    }

    fn begin_materialization(
        &mut self,
        job_id: JobId,
        set_index: usize,
        files: impl IntoIterator<Item = NzbFileId>,
    ) {
        self.pending_materializations
            .entry(job_id)
            .or_default()
            .entry(set_index)
            .or_default()
            .files
            .extend(files);
    }

    fn note_materialization_handoff(&mut self, set_index: usize, segment_id: SegmentId) {
        if let Some(pending) = self
            .pending_materializations
            .get_mut(&segment_id.file_id.job_id)
            .and_then(|sets| sets.get_mut(&set_index))
        {
            pending.handoffs.insert(segment_id);
        }
    }

    pub(crate) fn finish_materialization_handoff(&mut self, segment_id: SegmentId) {
        if let Some(sets) = self
            .pending_materializations
            .get_mut(&segment_id.file_id.job_id)
        {
            for pending in sets.values_mut() {
                pending.handoffs.remove(&segment_id);
            }
        }
    }

    pub(crate) fn settle_materialized_file(&mut self, file_id: NzbFileId) {
        let job_id = file_id.job_id;
        if let Some(sets) = self.pending_materializations.get_mut(&job_id) {
            sets.retain(|_, pending| {
                pending.files.remove(&file_id);
                pending
                    .handoffs
                    .retain(|segment_id| segment_id.file_id != file_id);
                pending
                    .rescued
                    .retain(|segment_id| segment_id.file_id != file_id);
                !pending.files.is_empty()
            });
            if sets.is_empty() {
                self.pending_materializations.remove(&job_id);
            }
        }
    }

    fn pending_materializations(
        &self,
        job_id: JobId,
    ) -> Vec<(usize, PendingDemotionMaterialization)> {
        self.pending_materializations
            .get(&job_id)
            .map(|sets| {
                sets.iter()
                    .map(|(set_index, pending)| (*set_index, pending.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn note_materialization_rescue(
        &mut self,
        job_id: JobId,
        set_index: usize,
        segment_id: SegmentId,
    ) -> bool {
        self.pending_materializations
            .get_mut(&job_id)
            .and_then(|sets| sets.get_mut(&set_index))
            .is_some_and(|pending| pending.rescued.insert(segment_id))
    }

    pub(crate) fn clear_pending_materializations(&mut self, job_id: JobId) {
        self.pending_materializations.remove(&job_id);
    }

    #[cfg(test)]
    pub(crate) fn pending_materialization_files(&self, job_id: JobId) -> usize {
        self.pending_materializations
            .get(&job_id)
            .map(|sets| sets.values().map(|pending| pending.files.len()).sum())
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn rescued_materialization_segments(&self, job_id: JobId) -> usize {
        self.pending_materializations
            .get(&job_id)
            .map(|sets| sets.values().map(|pending| pending.rescued.len()).sum())
            .unwrap_or(0)
    }

    /// Whether this job has a repair defer outstanding — a wave of targeted
    /// recovery was promoted for a set that is still direct and still waiting.
    fn repair_defer_pending(&self, job_id: JobId) -> bool {
        self.repair_defer_waves
            .get(&job_id)
            .is_some_and(|waves| *waves > 0)
    }

    /// Installs the sets a job restore rebuilt, and marks the job examined so
    /// the lazy admission seam does not rediscover them from the spec and throw
    /// the restored coverage away.
    pub(crate) fn install_restored(&mut self, job_id: JobId, sets: Vec<DirectSet>) {
        self.examined.insert(job_id);
        if sets.is_empty() {
            return;
        }
        self.sets.insert(job_id, sets);
    }

    #[cfg(test)]
    pub(crate) fn is_empty_for(&self, job_id: JobId) -> bool {
        !self.sets.contains_key(&job_id)
            && !self.examined.contains(&job_id)
            && !self.header_candidates_offered.contains(&job_id)
            && !self.prepared_destinations.contains_key(&job_id)
    }

    pub(crate) fn sets_for(&self, job_id: JobId) -> &[DirectSet] {
        self.sets.get(&job_id).map(Vec::as_slice).unwrap_or(&[])
    }

    pub(crate) fn set_mut(&mut self, job_id: JobId, index: usize) -> Option<&mut DirectSet> {
        self.sets.get_mut(&job_id)?.get_mut(index)
    }

    /// Every set of one job, mutably. Used by the password refresh, which has to
    /// touch all of a job's sets rather than one indexed set.
    pub(crate) fn sets_mut(&mut self, job_id: JobId) -> &mut [DirectSet] {
        self.sets
            .get_mut(&job_id)
            .map(Vec::as_mut_slice)
            .unwrap_or(&mut [])
    }

    pub(crate) fn set(&self, job_id: JobId, index: usize) -> Option<&DirectSet> {
        self.sets.get(&job_id)?.get(index)
    }

    /// Jobs with at least one set still routing.
    pub(crate) fn active_jobs(&self) -> Vec<JobId> {
        self.sets
            .iter()
            .filter(|(_, sets)| {
                sets.iter()
                    .any(|set| !set.is_demoted() && !set.is_finalized())
            })
            .map(|(job_id, _)| *job_id)
            .collect()
    }
}

/// Step 1 of the barrier. Routing runs inline on the pipeline task and every
/// destination write is awaited before the span is recorded, so by the time a
/// barrier can be requested nothing for this set is in flight.
struct InlineDrain;

impl BarrierDrain for InlineDrain {
    fn drain(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// Step 2 of the barrier, pre-computed.
///
/// The barrier's sync hook is synchronous and weaver's durable sync goes
/// through the disk owner thread that holds the destination's handle, which is
/// an await. So the syncs run immediately before [`super::barrier::CoverageBarrier::barrier`]
/// and their outcomes are replayed here — same order, same failure semantics: a
/// destination that did not sync fails step 2 and nothing is published.
struct PreSyncedDestinations {
    results: HashMap<String, Result<(), String>>,
}

impl DestinationSync for PreSyncedDestinations {
    fn sync(&mut self, relative_path: &str) -> Result<(), String> {
        self.results
            .get(relative_path)
            .cloned()
            .unwrap_or_else(|| Err(format!("{relative_path} was not offered for sync")))
    }
}

/// Everything the authoritative PAR2 pass needs to read a job's direct sets
/// virtually.
///
/// The provider is keyed by **NZB file index**, not by volume index: one job can
/// carry several direct sets and every set numbers its volumes from zero, so the
/// volume index is not unique inside a job while the file index always is. The
/// adapter only ever uses the key to reach a reader, so any injective key works,
/// and this one is already the identity the PAR2 binding is resolved through.
pub(crate) struct DirectPar2Overlay {
    /// The recovery set every virtual volume in this overlay belongs to.
    pub(crate) recovery_set_id: par2_rs::RecoverySetId,
    pub(crate) provider: super::provider::HybridVolumeProvider,
    pub(crate) volumes: Vec<super::par2_access::VirtualPar2Volume>,
    /// Which direct set owns each bound PAR2 file, so damage demotes the set
    /// that produced the bytes rather than every set of the job.
    sets: HashMap<par2_rs::FileId, usize>,
    /// The job file index each bound PAR2 file resolved to. The overlay re-keys
    /// virtual volumes by it, so it is also how repair walks back from a
    /// damaged PAR2 file to the set's own volume index.
    file_indices: HashMap<par2_rs::FileId, u32>,
    /// The volume lengths the overlay was built with, so a repair can rebuild
    /// the very same provider without re-deriving them from the assembly.
    lengths: Vec<(usize, std::collections::BTreeMap<u32, u64>)>,
}

impl DirectPar2Overlay {
    /// The direct set that owns one bound PAR2 file.
    pub(crate) fn owner_of(&self, file_id: &par2_rs::FileId) -> Option<usize> {
        self.sets.get(file_id).copied()
    }

    /// The job file index one bound PAR2 file resolved to.
    pub(crate) fn file_index_of(&self, file_id: &par2_rs::FileId) -> Option<u32> {
        self.file_indices.get(file_id).copied()
    }

    /// Rebuilds the overlay's virtual volumes against the sets as they stand
    /// now, re-keyed by job file index exactly as [`Pipeline::direct_par2_overlay`]
    /// does.
    ///
    /// Deliberately re-derived rather than cloned out of `provider`: a repair
    /// materializes and re-routes between the pass and the repair, and the
    /// coverage the sets carry afterwards is the coverage the repair's reads
    /// must see.
    ///
    /// A **retained** finalized set is the one exception, and for the same
    /// reason: its image was captured at finalization precisely because nothing
    /// can re-derive it afterwards — the coverage controller has been retired
    /// and the partials renamed away — so it is replayed rather than rebuilt.
    pub(crate) fn virtual_volumes_for(
        &self,
        runtime: &DirectStoreRuntime,
        job_id: JobId,
    ) -> Option<Vec<super::provider::VirtualVolume>> {
        let mut volumes = Vec::new();
        for (set_index, lengths) in &self.lengths {
            let set = runtime.set(job_id, *set_index)?;
            let set_volumes = match set.retained_volumes() {
                Some(retained) => retained.to_vec(),
                None => set.virtual_volumes(lengths),
            };
            for mut volume in set_volumes {
                let file_index = set
                    .plan()
                    .volumes
                    .iter()
                    .find(|(index, _)| **index == volume.volume_index)
                    .map(|(_, file_index)| *file_index)?;
                volume.volume_index = file_index;
                volumes.push(volume);
            }
        }
        Some(volumes)
    }
}

/// What a live direct set turned out to need, just before the completion gate
/// would have handed the job to `Par2Repairer`.
#[derive(Debug)]
pub(crate) enum DirectPar2Resolution {
    /// Damage was found and repaired in place. The job goes round again and
    /// re-verifies over the repaired virtual volumes.
    Repaired,
    /// The direct sets verify clean.
    ///
    /// Load-bearing rather than a nicety: the branch that was about to run
    /// cannot read a virtual volume, so it would have demoted every live set to
    /// get files it could — for a job whose sets are *fine*. The caller instead
    /// skips the repairer and lets the ordinary verify path, which reads them
    /// virtually, record the same verdict this pass just reached.
    ///
    /// Carries the verdict itself, because the caller now settles it directly
    /// instead of throwing it away and asking [`Pipeline::verify_par2_with_placement`]
    /// to read the whole set again to reach the same answer. Boxed to keep this
    /// enum small on the branches that carry nothing.
    Clean(Box<par2_rs::VerificationResult>),
    /// Damage was found that the recovery *merged so far* cannot cover, but the
    /// recovery set as a whole can. Targeted recovery has been asked for and the
    /// sets stay direct until it lands. The caller must not run the repairer and
    /// must not demote: doing either throws away the direct outputs the wait
    /// exists to keep.
    Deferred,
    /// The post-processing lane owns the post-repair read-back. Its terminal
    /// verdict re-arms this job without holding the queue actor.
    Pending,
    /// Neither: no live set, no verdict, or a repair that refused. The caller
    /// falls back to demoting for the repairer, which is the earlier behaviour.
    Unresolved,
}

/// What the verify branch's direct-aware seam settled on.
///
/// The bool this replaced could say "act on it" or "fall through", and the
/// third answer — *wait* — is neither: bytes have not changed, so there is
/// nothing to re-verify, but the sets must not be handed on either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectDamageResolution {
    /// Repaired in place, or demoted. Either way bytes moved and the job's next
    /// move is a fresh pass over them.
    Resolved,
    /// Waiting for targeted recovery, still direct. The job's next move comes
    /// from the recovery arriving, not from this pass.
    Deferred,
    /// Nothing here answered the damage; the caller carries on.
    Unresolved,
}

/// What the repair seam did with a damaged live direct set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectRepairAnswer {
    /// A repair ran, or a refusal partway through one had already demoted the
    /// set. Both leave the job with changed bytes to re-read.
    Acted,
    /// The damage is repairable out of the recovery set but not out of the
    /// slices merged today, so the missing recovery was promoted and the set
    /// was left alone to wait for it.
    Deferred,
    /// Nothing was done, and waiting cannot help. The caller demotes.
    Declined,
}

/// How many waves of targeted recovery one job's direct sets may wait through
/// before demoting instead.
///
/// Three, because one is what the design predicts and two is what a bad article
/// costs. The first wave asks for every block the verdict needs, so a second
/// exists only because some of the first wave's articles turned out unavailable
/// and the re-verdict still comes up short; a third is the same thing happening
/// twice. Past that the recovery stream is not delivering, and the conventional
/// path — which has its own, better-instrumented dead end — should get the job.
pub(crate) const MAX_DIRECT_REPAIR_DEFER_WAVES: u32 = 3;

/// What the routing seam did with an article.
pub(crate) enum DirectRouteOutcome {
    /// The bytes were routed; the caller must not write the source volume.
    Routed,
    /// The set demoted before taking ownership. The caller must pass this same
    /// decoded article through the conventional assembly path.
    Conventional(BufferedDecodedSegment),
}

/// What the decode seam should do with one file's bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectFileTarget {
    /// A live set's source volume: route it.
    Route { set_index: usize, volume_index: u32 },
    /// A **finalized** set's source volume. The set's members are already at
    /// their destinations and the volume was never a file, so a late duplicate
    /// has nowhere to go: routing it would write through stale partial paths,
    /// and writing it conventionally would materialize a volume the whole point
    /// was never to create. It is dropped.
    Discard,
}

impl Pipeline {
    /// Admits the job's candidate RAR sets, once per job.
    ///
    /// Deliberately lazy rather than hooked into job start: the first decoded
    /// segment is the earliest moment a byte could be written, so admitting
    /// here is still "before any byte lands" while touching no submit or
    /// restore plumbing.
    fn ensure_direct_sets(&mut self, job_id: JobId) {
        if self.direct_store.examined.contains(&job_id) {
            return;
        }
        self.direct_store.examined.insert(job_id);
        if !self.direct_store.gate().is_enabled() {
            return;
        }
        // Deterministic rather than `extraction_staging_dir`, and deliberately
        // so: the restore seam derives the very same root before the job state
        // (and therefore `state.staging_dir`) exists, and the two must agree
        // byte for byte or a resumed set would probe destinations it never
        // wrote. `state.staging_dir` is only ever this path anyway — see
        // `Pipeline::extraction_staging_dir` — and it is recorded on the state
        // at the first prepared destination, which is what makes completion
        // sweep the root and the failure paths delete it.
        let destination_dir = self.deterministic_extraction_staging_dir(job_id);
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let (admitted, refused) =
            DirectSetPlan::discover(&state.spec, &state.working_dir, &destination_dir);
        let password = state.spec.password.clone();
        for (set_name, refusal) in refused {
            crate::runtime::perf_probe::record_owned(
                format!("direct_store.refused.{}", refusal.metric()),
                std::time::Duration::from_nanos(1),
            );
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                reason = refusal.metric(),
                "direct-store did not admit an archive set"
            );
        }
        // The one invariant every admission path shares: a volume binds only
        // while none of its bytes live in a conventional file. Admission is
        // lazy — the first decoded segment — and for a freshly submitted job
        // nothing has committed by then. A RESTORED job is the case this
        // refuses: restore rebuilds the conventional floor and commits the
        // skipped segments into the assembly, so admitting such a set would
        // route every remaining article away from the file that already owns
        // the prefix, and the two halves would never meet — a volume torn
        // between an envelope and a file, unreadable from either. (A restored
        // *direct* set never reaches this seam: `install_restored` marks the
        // job examined with its coverage re-validated.)
        let admitted: Vec<DirectSetPlan> = admitted
            .into_iter()
            .filter(|plan| {
                let prior_bytes = plan.files.keys().any(|file_index| {
                    state
                        .assembly
                        .file(NzbFileId {
                            job_id,
                            file_index: *file_index,
                        })
                        .is_some_and(|file| file.received_bytes() > 0)
                });
                if prior_bytes {
                    crate::runtime::perf_probe::record(
                        "direct_store.refused.prior_conventional_bytes",
                        std::time::Duration::from_nanos(1),
                    );
                    info!(
                        job_id = job_id.0,
                        set_name = %plan.set_name,
                        "direct-store did not admit an archive set: a volume already \
                         has conventional bytes, so the set stays on the path that \
                         owns them"
                    );
                }
                !prior_bytes
            })
            .collect();
        if admitted.is_empty() {
            return;
        }
        let sets: Vec<DirectSet> = admitted
            .into_iter()
            .map(|plan| {
                // The admission counter the refusal counters are read against:
                // `refused.*` alone cannot say whether a quiet install is
                // admitting everything or admitting nothing.
                crate::runtime::perf_probe::record(
                    "direct_store.admitted",
                    std::time::Duration::from_nanos(1),
                );
                crate::runtime::perf_probe::record_value(
                    "direct_store.admitted.volumes",
                    plan.volumes.len() as u64,
                );
                self.metrics
                    .direct_sets_admitted
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    job_id = job_id.0,
                    set_name = %plan.set_name,
                    volumes = plan.volumes.len(),
                    "direct-store admitted an archive set"
                );
                // No format is chosen here: the router reads it from the first
                // volume's signature, so a RAR4 set routes as RAR4 rather than
                // demoting on its first header.
                let mut set = DirectSet::new(job_id, plan);
                self.direct_store.apply_ceilings(&mut set);
                // The winning password the job was submitted
                // with, if any; `refresh_direct_passwords` picks up one that
                // arrives later. Held in memory only.
                set.router.set_password(password.as_deref());
                set
            })
            .collect();
        self.direct_store.sets.insert(job_id, sets);
    }

    /// Arms identity admission for a job whose PAR2 metadata just parsed.
    ///
    /// Called from the metadata-load seam. The name path admits from the spec
    /// before a byte lands; this is the second chance for the jobs that path
    /// cannot see — obfuscated posts, whose real volume names exist only in
    /// the recovery set's descriptions. Each description names a real file and
    /// carries its `md5(min(length, 16 KiB))` fingerprint, so the roster (set
    /// name, dense volume indices, per-volume fingerprints) is complete here,
    /// while the volume-to-file mapping is established later, file by file, at
    /// the routing seam.
    ///
    /// Fail-closed throughout: a roster is only armed when every one of its
    /// volumes is still provably clean of conventional writes, described by
    /// exactly one recovery set, and fingerprint-unique — anything less routes
    /// nothing and leaves the job on the conventional path it is on today.
    pub(crate) async fn arm_direct_identity_admission(&mut self, job_id: JobId) {
        if !self.direct_store.gate().is_enabled() {
            return;
        }
        // Only for jobs the name path found nothing for. Once identity mode is
        // engaged, later metadata loads may extend it with newly described
        // sets; a job with name-admitted sets never mixes in identity ones.
        let engaged = self.direct_store.identity.contains_key(&job_id);
        if !engaged && !self.direct_store.sets_for(job_id).is_empty() {
            return;
        }
        // A job whose header rung already owns sets keeps them: the headers
        // are binding evidence at least as direct as the descriptions, the
        // descriptions still verify the payload at completion through their
        // own per-file binding, and arming rosters beside live header sets
        // would put two kinds of evidence in a bidding war for the same
        // files.
        if self
            .direct_store
            .identity
            .get(&job_id)
            .is_some_and(|admission| !admission.header_sets.is_empty())
        {
            return;
        }
        let Some(runtime) = self.par2_runtime(job_id) else {
            return;
        };
        // Files the PAR2 machinery knows as metadata carriers or recovery
        // volumes. They have bytes on disk by construction — the index that
        // got us here was downloaded and parsed — and they are never source
        // volumes, so they are excluded from both the leak scan and the
        // candidate pool the viability arm counts.
        let carrier_files = self.identity_par2_carrier_files(job_id);
        let set_ids = runtime.ordered_set_ids();
        let mut candidates: BTreeMap<String, Vec<(u32, IdentityRosterVolume)>> = BTreeMap::new();
        let mut described_by: HashMap<String, HashSet<par2_rs::RecoverySetId>> = HashMap::new();
        for set_id in set_ids {
            let Some(set) = self.par2_set_for(job_id, set_id) else {
                continue;
            };
            for desc in set.files.values() {
                let name = weaver_model::files::sanitize_download_filename(&desc.filename);
                let role = weaver_model::files::FileRole::from_filename(&name);
                let weaver_model::files::FileRole::RarVolume { volume_number } = role else {
                    continue;
                };
                let Some(set_name) = weaver_model::files::archive_base_name(&name, &role) else {
                    continue;
                };
                candidates.entry(set_name.clone()).or_default().push((
                    volume_number,
                    IdentityRosterVolume {
                        hash_16k: desc.hash_16k,
                        length: desc.length,
                    },
                ));
                described_by.entry(set_name).or_default().insert(set_id);
            }
        }

        // The same admission rules the name path applies — dense from zero, no
        // volume claimed twice — plus the two this path needs on top: one
        // recovery set per archive set (a description answering from two sets
        // is ambiguous, exactly as it is for per-file binding), and globally
        // unique fingerprints (the fingerprint *is* the mapping evidence, so
        // two volumes sharing one could never be told apart at the seam).
        let mut fingerprints: HashMap<[u8; 16], u32> = HashMap::new();
        for volumes in candidates.values() {
            for (_, volume) in volumes {
                *fingerprints.entry(volume.hash_16k).or_default() += 1;
            }
        }
        let mut rosters: HashMap<String, IdentityRoster> = HashMap::new();
        'candidate: for (set_name, entries) in candidates {
            if described_by
                .get(&set_name)
                .is_none_or(|origins| origins.len() != 1)
            {
                continue;
            }
            if self
                .direct_store
                .identity
                .get(&job_id)
                .is_some_and(|admission| admission.rosters.contains_key(&set_name))
            {
                continue;
            }
            let mut volumes = BTreeMap::new();
            for (volume_index, volume) in entries {
                if fingerprints.get(&volume.hash_16k).copied().unwrap_or(0) != 1 {
                    continue 'candidate;
                }
                if volumes.insert(volume_index, volume).is_some() {
                    continue 'candidate;
                }
            }
            if volumes.is_empty()
                || volumes
                    .keys()
                    .enumerate()
                    .any(|(position, volume)| position as u32 != *volume)
            {
                continue;
            }
            rosters.insert(
                set_name,
                IdentityRoster {
                    volumes,
                    bound: HashMap::new(),
                    set_index: None,
                },
            );
        }
        if rosters.is_empty() {
            return;
        }

        // The leak scan: the one invariant (see [`IdentityAdmission`]) is that
        // a volume binds only while its file has zero conventionally written
        // bytes. A file that already received bytes is checked against the new
        // rosters by the same fingerprint the seam would have used — a match
        // means that roster's volume already leaked, and the roster is refused
        // as arriving too late. A started file whose offset-zero bytes are not
        // held (its first article has not decoded, or decoded short) cannot be
        // disproved against anything, so *every* new roster is refused: any of
        // them could own it.
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let mut leaked: HashSet<u32> = HashSet::new();
        let mut no_match: HashSet<u32> = HashSet::new();
        for file_index in 0..state.spec.files.len() as u32 {
            if carrier_files.contains(&file_index)
                || matches!(
                    state.spec.files[file_index as usize].role,
                    weaver_model::files::FileRole::Par2 { .. }
                )
            {
                continue;
            }
            let file_id = NzbFileId { job_id, file_index };
            let received = state
                .assembly
                .file(file_id)
                .map(|file| file.received_bytes())
                .unwrap_or(0);
            if received == 0 {
                continue;
            }
            leaked.insert(file_index);
            let prefix = self.file_prefix_16k.get(&file_id);
            let mut matched_sets: Vec<String> = Vec::new();
            let mut evaluated_all = true;
            for (set_name, roster) in &rosters {
                for volume in roster.volumes.values() {
                    let window = volume
                        .length
                        .min(crate::pipeline::PAR2_HASH_16K_BYTES as u64)
                        as usize;
                    let Some(prefix) = prefix.filter(|prefix| prefix.len() >= window) else {
                        evaluated_all = false;
                        continue;
                    };
                    if window > 0 && par2_rs::checksum::md5(&prefix[..window]) == volume.hash_16k {
                        matched_sets.push(set_name.clone());
                    }
                }
            }
            if !matched_sets.is_empty() {
                for set_name in matched_sets {
                    if rosters.remove(&set_name).is_some() {
                        crate::runtime::perf_probe::record(
                            "direct_store.identity.refused.identity_late",
                            std::time::Duration::from_nanos(1),
                        );
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            file_index,
                            "identity admission arrived after a described volume's bytes"
                        );
                    }
                }
            } else if evaluated_all {
                no_match.insert(file_index);
            } else {
                for set_name in rosters.keys() {
                    crate::runtime::perf_probe::record(
                        "direct_store.identity.refused.identity_late",
                        std::time::Duration::from_nanos(1),
                    );
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        file_index,
                        "identity admission refused: a started file cannot be disproved"
                    );
                }
                rosters.clear();
            }
            if rosters.is_empty() {
                return;
            }
        }

        let admission = self.direct_store.identity.entry(job_id).or_default();
        admission.leaked.extend(leaked);
        admission.no_match.extend(no_match);
        for (set_name, roster) in rosters {
            crate::runtime::perf_probe::record(
                "direct_store.identity.armed",
                std::time::Duration::from_nanos(1),
            );
            crate::runtime::perf_probe::record_value(
                "direct_store.identity.armed.volumes",
                roster.volumes.len() as u64,
            );
            info!(
                job_id = job_id.0,
                set_name = %set_name,
                volumes = roster.volumes.len(),
                "identity admission armed an archive set from PAR2 descriptions"
            );
            admission.rosters.insert(set_name, roster);
        }
        self.boost_identity_probe_segments(job_id);
        self.identity_viability_sweep(job_id).await;
    }

    /// The identity half of the routing seam: matches one file's offset-zero
    /// bytes against the job's armed rosters, and turns the unique match into
    /// a routed binding — admitting the set on its first one.
    ///
    /// Called only after [`Self::direct_route_target`] answered `None`, at the
    /// decode seam — after the binder's prefix capture and before the write —
    /// which is what makes the binding decision atomic with the write
    /// decision: the article either routes under the binding made here or
    /// takes the conventional path and marks the file leaked. There is no
    /// window in which a bindable file's bytes land somewhere a later binding
    /// would contradict. The bytes themselves are read from
    /// [`crate::pipeline::Pipeline::file_prefix_16k`], the same capture the
    /// PAR2 content binder answers from, populated earlier on this very call
    /// path.
    pub(crate) async fn direct_identity_route_target(
        &mut self,
        file_id: NzbFileId,
        file_offset: u64,
    ) -> Option<DirectFileTarget> {
        let job_id = file_id.job_id;
        let file_index = file_id.file_index;
        if file_offset != 0 {
            return None;
        }
        // Two rungs, mutually exclusive per job. Described rosters — PAR2
        // metadata named the volumes — are the stronger evidence and go
        // first; a job without them falls to the header rung, where the
        // volumes' own RAR5 headers are the remaining identity source.
        let has_rosters = self
            .direct_store
            .identity
            .get(&job_id)
            .is_some_and(|admission| !admission.rosters.is_empty());
        if !has_rosters {
            return self.direct_header_route_target(file_id).await;
        }
        // Evaluate against every roster's unclaimed volumes.
        let leaked;
        let mut matches: Vec<(String, u32)> = Vec::new();
        let mut evaluated_all = true;
        {
            let admission = self.direct_store.identity.get(&job_id)?;
            if admission.no_match.contains(&file_index) {
                return None;
            }
            if let Some(set_index) = admission.rosters.values().find_map(|roster| {
                roster
                    .bound
                    .contains_key(&file_index)
                    .then_some(roster.set_index)
                    .flatten()
            }) {
                // A bound file's articles route through its plan mapping, so
                // reaching this rung means the routing seam declined the set —
                // it is finalized or demoted. A finalized set's late duplicate
                // has nowhere to go and is dropped, exactly as the routing
                // seam drops it for name-admitted sets; a demoted set's file
                // belongs to the conventional path now and must be let
                // through, never discarded.
                let finalized = self
                    .direct_store
                    .set(job_id, set_index)
                    .is_some_and(|set| set.is_finalized());
                if finalized {
                    return Some(DirectFileTarget::Discard);
                }
                return None;
            }
            // A recovery carrier is never a described source volume, and its
            // conventional bytes say nothing about the rosters — evaluating
            // it would only burn hashes and pollute the evidence sets. The
            // predicate is evidence-based, never mere discovery presence:
            // see [`Self::identity_par2_carrier_files`].
            if self
                .identity_par2_carrier_files(job_id)
                .contains(&file_index)
            {
                return None;
            }
            leaked = admission.leaked.contains(&file_index);
            let prefix = self.file_prefix_16k.get(&file_id);
            for (set_name, roster) in &admission.rosters {
                let claimed: HashSet<u32> = roster.bound.values().copied().collect();
                for (volume_index, volume) in &roster.volumes {
                    if claimed.contains(volume_index) {
                        continue;
                    }
                    let window = volume
                        .length
                        .min(crate::pipeline::PAR2_HASH_16K_BYTES as u64)
                        as usize;
                    let Some(prefix) = prefix.filter(|prefix| prefix.len() >= window) else {
                        evaluated_all = false;
                        continue;
                    };
                    if window > 0 && par2_rs::checksum::md5(&prefix[..window]) == volume.hash_16k {
                        matches.push((set_name.clone(), *volume_index));
                    }
                }
            }
        }
        if matches.is_empty() {
            if !evaluated_all {
                let prefix_len = self
                    .file_prefix_16k
                    .get(&file_id)
                    .map(|prefix| prefix.len())
                    .unwrap_or(0);
                info!(
                    job_id = job_id.0,
                    file_index,
                    prefix_len,
                    "identity seam could not evaluate a file against every unclaimed volume"
                );
            }
            // Only a fully evaluated miss is a settled fact about the file.
            // A window the article did not cover proves nothing, and the file
            // simply proceeds conventionally — the leak mark and the
            // viability arm own what that means for the rosters.
            if evaluated_all {
                if let Some(admission) = self.direct_store.identity.get_mut(&job_id) {
                    admission.no_match.insert(file_index);
                }
                info!(
                    job_id = job_id.0,
                    file_index,
                    leaked,
                    "identity seam settled a file as matching no described volume"
                );
                self.identity_viability_sweep(job_id).await;
            }
            return None;
        }
        if leaked || matches.len() > 1 {
            // A match on a leaked file is proof its volume already has
            // conventional bytes: that roster can never be made whole. More
            // than one match should be unreachable — arming enforces global
            // fingerprint uniqueness — so it is treated with the same
            // fail-closed hand rather than reconciled.
            for (set_name, _) in matches {
                self.condemn_identity_roster(job_id, &set_name).await;
            }
            self.identity_viability_sweep(job_id).await;
            return None;
        }
        let (set_name, volume_index) = matches.pop().expect("exactly one match");

        // Bind. The set is admitted by its first binding; later bindings only
        // extend its plan.
        let existing_set_index = self
            .direct_store
            .identity
            .get(&job_id)
            .and_then(|admission| admission.rosters.get(&set_name))
            .and_then(|roster| roster.set_index);
        if let Some(set_index) = existing_set_index {
            let bound = {
                let set = self.direct_store.set_mut(job_id, set_index)?;
                if set.is_demoted() || set.is_finalized() {
                    info!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        file_index,
                        volume_index,
                        "identity match arrived after its set left the direct path"
                    );
                    if let Some(admission) = self.direct_store.identity.get_mut(&job_id) {
                        admission.rosters.remove(&set_name);
                    }
                    return None;
                }
                set.bind_identity_volume(volume_index, file_index)
            };
            if !bound {
                // The plan disagrees with the identity evidence — a volume or
                // file already claimed by a different partner. Nothing can
                // reconcile that; the set demotes and the roster is retired.
                self.condemn_identity_roster(job_id, &set_name).await;
                return None;
            }
            self.record_identity_binding(job_id, &set_name, file_index, volume_index);
            return Some(DirectFileTarget::Route {
                set_index,
                volume_index,
            });
        }

        let destination_dir = self.deterministic_extraction_staging_dir(job_id);
        let state = self.jobs.get(&job_id)?;
        let working_dir = state.working_dir.clone();
        let password = state.spec.password.clone();
        let expected_volumes = self
            .direct_store
            .identity
            .get(&job_id)
            .and_then(|admission| admission.rosters.get(&set_name))
            .map(|roster| roster.volumes.len() as u32)?;
        let plan = DirectSetPlan {
            set_name: set_name.clone(),
            volumes: BTreeMap::from([(volume_index, file_index)]),
            files: HashMap::from([(file_index, volume_index)]),
            identity: Some(IdentityPlanFacts {
                expected_volumes: Some(expected_volumes),
                // The first bound file's index: stable by construction, which
                // the derived minimum is not while the mapping grows.
                discriminator: file_index,
            }),
            working_dir,
            destination_dir,
        };
        crate::runtime::perf_probe::record(
            "direct_store.identity.admitted",
            std::time::Duration::from_nanos(1),
        );
        crate::runtime::perf_probe::record_value(
            "direct_store.identity.admitted.volumes",
            expected_volumes as u64,
        );
        info!(
            job_id = job_id.0,
            set_name = %plan.set_name,
            volumes = expected_volumes,
            "direct-store admitted an archive set from PAR2 identity"
        );
        let set_index = self.admit_identity_set(job_id, plan, password.as_deref());
        if let Some(roster) = self
            .direct_store
            .identity
            .get_mut(&job_id)
            .and_then(|admission| admission.rosters.get_mut(&set_name))
        {
            roster.set_index = Some(set_index);
        }
        self.record_identity_binding(job_id, &set_name, file_index, volume_index);
        Some(DirectFileTarget::Route {
            set_index,
            volume_index,
        })
    }

    /// The job's files that really are PAR2 material — declared by role, or
    /// carriers by the discovery machinery's own evidence. Deliberately NOT
    /// "every file the discovery has touched": obfuscated par2 discovery
    /// prefix-probes ordinary data files too, and an entry whose probe found
    /// nothing (or has not run) is a data file, not a carrier. Treating mere
    /// presence as carrierhood silently withheld a dozen volumes from binding
    /// on the first production sets.
    fn identity_par2_carrier_files(&self, job_id: JobId) -> HashSet<u32> {
        let mut carriers: HashSet<u32> = self
            .par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter(|(_, file)| {
                        !matches!(
                            file.discovery,
                            crate::pipeline::Par2DiscoveryState::Unseen
                                | crate::pipeline::Par2DiscoveryState::PrefixProbeQueued
                                | crate::pipeline::Par2DiscoveryState::ProbeInconclusive
                        )
                    })
                    .map(|(file_index, _)| *file_index)
                    .collect()
            })
            .unwrap_or_default();
        if let Some(state) = self.jobs.get(&job_id) {
            for (file_index, file) in state.spec.files.iter().enumerate() {
                if matches!(file.role, weaver_model::files::FileRole::Par2 { .. }) {
                    carriers.insert(file_index as u32);
                }
            }
        }
        carriers
    }

    /// Reorders the job's download queue so every identity candidate's first
    /// article arrives before any candidate's payload — the probe wave.
    ///
    /// Why dispatch order is a correctness lever here: an obfuscated post's
    /// NZB order routinely scrambles the volume order, and a mid-set volume's
    /// member payload cannot be *placed* until every earlier volume's headers
    /// have stated their part sizes. Streamed in NZB order, such a set piles
    /// its payload into holds until the scratch ceiling demotes it — the
    /// ceiling is direct-store's own disk promise and must not move. Pulling
    /// each candidate's first article forward binds every file within a few
    /// round trips (and carries exactly the headers the layout needs), after
    /// which [`Self::reprioritize_bound_identity_file`] streams the volumes
    /// in order, precisely as a name-classified job always has.
    fn boost_identity_probe_segments(&mut self, job_id: JobId) {
        let mut first_segments: HashMap<u32, u32> = HashMap::new();
        {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let carrier_files = self.identity_par2_carrier_files(job_id);
            let admission = self.direct_store.identity.get(&job_id);
            for (file_index, file) in state.spec.files.iter().enumerate() {
                let file_index = file_index as u32;
                if !matches!(
                    file.role,
                    weaver_model::files::FileRole::Unknown
                        | weaver_model::files::FileRole::SplitFile { .. }
                ) || carrier_files.contains(&file_index)
                    || admission.is_some_and(|admission| {
                        admission.leaked.contains(&file_index)
                            || admission.no_match.contains(&file_index)
                            || admission
                                .rosters
                                .values()
                                .any(|roster| roster.bound.contains_key(&file_index))
                            || admission
                                .header_sets
                                .iter()
                                .any(|header_set| header_set.bound.contains_key(&file_index))
                    })
                {
                    continue;
                }
                let Some(first) = file.segments.iter().map(|segment| segment.ordinal).min() else {
                    continue;
                };
                first_segments.insert(file_index, first);
            }
        }
        if first_segments.is_empty() {
            return;
        }
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let boosted = state
            .download_queue
            .promote_matching_to_completion_critical_with_rank(|work| {
                if work.priority <= 1 {
                    return Some((work.priority, None));
                }
                let file_index = work.segment_id.file_id.file_index;
                first_segments
                    .get(&file_index)
                    .and_then(|first| (work.segment_id.segment_number == *first).then_some(()))
                    // Right behind the PAR2 index (0) and a named first
                    // volume (1); ranked by file index only for determinism.
                    .map(|()| (2, Some(file_index)))
            });
        if boosted > 0 {
            info!(
                job_id = job_id.0,
                boosted, "identity probe wave scheduled ahead of candidate payload"
            );
        }
    }

    /// Re-ranks one bound file's queued articles to the priority a
    /// name-classified volume of the same position always had: `10 + volume`.
    /// This is what turns the probe wave's scattered bindings back into
    /// in-order volume streaming, which keeps the holds footprint at the
    /// out-of-order jitter of the connection pool rather than the whole set.
    fn reprioritize_bound_identity_file(
        &mut self,
        job_id: JobId,
        file_index: u32,
        volume_index: u32,
    ) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        state.download_queue.reprioritize_matching(|work| {
            (work.segment_id.file_id.file_index == file_index)
                .then_some(10u32.saturating_add(volume_index))
        });
    }

    /// Pushes one identity-admitted set into the job's set vector with the
    /// ceilings and password every admission path applies, and returns its
    /// stable index.
    fn admit_identity_set(
        &mut self,
        job_id: JobId,
        plan: DirectSetPlan,
        password: Option<&str>,
    ) -> usize {
        self.metrics
            .direct_sets_admitted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut set = DirectSet::new(job_id, plan);
        self.direct_store.apply_ceilings(&mut set);
        set.router.set_password(password);
        let sets = self.direct_store.sets.entry(job_id).or_default();
        sets.push(set);
        sets.len() - 1
    }

    /// The header rung of the identity seam: for a job with no described
    /// rosters, an unclassified file's own RAR5 head is the remaining
    /// identity source — the volume states its position itself.
    ///
    /// Grounded fail-closed, in order of appearance below: only a file whose
    /// name says nothing may be sniffed (a *named* file whose bytes are a RAR
    /// is the deliverable itself, not a volume); a file with earlier
    /// conventional bytes never binds, exactly as on the roster rung; RAR4 is
    /// declined outright — its headers carry no position, and the interior
    /// volumes of a stored RAR4 set are identical in every field that could
    /// place one, so there is nothing to bind on and the conventional path
    /// owns the shape; header-encrypted RAR5 withholds the position field
    /// itself; and at most one header volume set may exist per job, because
    /// the bytes carry positions but no set identity — a second claimant to a
    /// claimed position is indistinguishable interleaving, and both sets are
    /// refused rather than guessed apart.
    async fn direct_header_route_target(&mut self, file_id: NzbFileId) -> Option<DirectFileTarget> {
        let job_id = file_id.job_id;
        let file_index = file_id.file_index;
        if !self.direct_store.gate().is_enabled() {
            return None;
        }
        // Identity sets may accumulate; a name-admitted set means the job's
        // names were readable and this rung has no business running.
        if self
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| set.plan().identity.is_none())
        {
            return None;
        }
        {
            let admission = self.direct_store.identity.get(&job_id);
            if admission.is_some_and(|admission| {
                admission.no_match.contains(&file_index)
                    || admission
                        .header_sets
                        .iter()
                        .any(|header_set| header_set.bound.contains_key(&file_index))
            }) {
                return None;
            }
        }
        let (role, leaked) = {
            let state = self.jobs.get(&job_id)?;
            let role = state.spec.files.get(file_index as usize)?.role.clone();
            // `received_bytes` commits after the routing seam, so at this
            // file's offset-zero decision a nonzero count is exactly "an
            // earlier article of this file already went conventional".
            let leaked = state
                .assembly
                .file(file_id)
                .is_some_and(|file| file.received_bytes() > 0);
            (role, leaked)
        };
        // Unknown, and numeric-extension "split" names too: `.NNN` is a
        // classic obfuscation shape and collides with genuine split payloads,
        // so the name proves nothing either way — the byte sniff below is the
        // gate, exactly as it is for the hex names. A real split payload
        // sniffs as not-RAR and settles as an ordinary conventional file.
        if !matches!(
            role,
            weaver_model::files::FileRole::Unknown
                | weaver_model::files::FileRole::SplitFile { .. }
        ) {
            return None;
        }
        let sniff = super::sniff::sniff_rar_prefix(self.file_prefix_16k.get(&file_id)?);
        let super::sniff::PrefixSniff::Rar5 {
            volume_number,
            is_volume,
        } = sniff
        else {
            if let Some(admission) = self.direct_store.identity.get_mut(&job_id) {
                admission.no_match.insert(file_index);
            }
            return None;
        };

        if is_volume {
            let existing = self
                .direct_store
                .identity
                .get(&job_id)
                .and_then(|admission| {
                    admission
                        .header_sets
                        .iter()
                        .find(|header_set| header_set.volume_set)
                        .map(|header_set| {
                            (
                                header_set.set_index,
                                header_set
                                    .bound
                                    .values()
                                    .any(|bound| *bound == volume_number),
                            )
                        })
                });
            if let Some((set_index, position_claimed)) = existing {
                if position_claimed || leaked {
                    // A second file for a claimed position is a second set
                    // the bytes cannot tell apart; a leaked file that proves
                    // to be a set volume leaves the set unfillable. Both
                    // retire the set, and the claim collision additionally
                    // poisons the rung so a third claimant cannot rebuild
                    // the same ambiguity.
                    self.condemn_header_set(job_id, set_index).await;
                    if position_claimed
                        && let Some(admission) = self.direct_store.identity.get_mut(&job_id)
                    {
                        admission.header_volume_sets_poisoned = true;
                    }
                    return None;
                }
                let bound = {
                    let set = self.direct_store.set_mut(job_id, set_index)?;
                    if set.is_demoted() || set.is_finalized() {
                        if let Some(admission) = self.direct_store.identity.get_mut(&job_id) {
                            admission
                                .header_sets
                                .retain(|header_set| header_set.set_index != set_index);
                        }
                        return None;
                    }
                    set.bind_identity_volume(volume_number, file_index)
                };
                if !bound {
                    self.condemn_header_set(job_id, set_index).await;
                    return None;
                }
                self.record_header_binding(job_id, set_index, file_index, volume_number);
                return Some(DirectFileTarget::Route {
                    set_index,
                    volume_index: volume_number,
                });
            }
            if leaked
                || self
                    .direct_store
                    .identity
                    .get(&job_id)
                    .is_some_and(|admission| admission.header_volume_sets_poisoned)
            {
                return None;
            }
            // First volume seen of the job's one header volume set. The plan
            // opens with no expected count — nothing in the format states it
            // until the final volume's end record parses (see the router's
            // close in `accept_volume_facts`) — so the set can bind and
            // route but not finalize yet.
            let destination_dir = self.deterministic_extraction_staging_dir(job_id);
            let state = self.jobs.get(&job_id)?;
            let working_dir = state.working_dir.clone();
            let password = state.spec.password.clone();
            let plan = DirectSetPlan {
                set_name: format!("obfuscated-set.f{file_index}"),
                volumes: BTreeMap::from([(volume_number, file_index)]),
                files: HashMap::from([(file_index, volume_number)]),
                identity: Some(IdentityPlanFacts {
                    expected_volumes: None,
                    discriminator: file_index,
                }),
                working_dir,
                destination_dir,
            };
            crate::runtime::perf_probe::record(
                "direct_store.identity.header_admitted",
                std::time::Duration::from_nanos(1),
            );
            info!(
                job_id = job_id.0,
                set_name = %plan.set_name,
                volume = volume_number,
                "direct-store admitted an archive set from its own RAR5 headers"
            );
            let set_index = self.admit_identity_set(job_id, plan, password.as_deref());
            let admission = self.direct_store.identity.entry(job_id).or_default();
            admission.header_sets.push(HeaderSet {
                set_index,
                bound: HashMap::from([(file_index, volume_number)]),
                volume_set: true,
            });
            crate::runtime::perf_probe::record(
                "direct_store.identity.bound",
                std::time::Duration::from_nanos(1),
            );
            self.reprioritize_bound_identity_file(job_id, file_index, volume_number);
            // The job just proved itself an obfuscated RAR5 set: every other
            // unclassified file's first article is now worth having early,
            // for exactly the reasons the roster rung's probe wave states.
            self.boost_identity_probe_segments(job_id);
            return Some(DirectFileTarget::Route {
                set_index,
                volume_index: volume_number,
            });
        }

        // A standalone archive: a set of one, closed at admission, needing no
        // further bookkeeping — its single binding is made here and its
        // completeness is just the file's own download.
        if leaked {
            return None;
        }
        let destination_dir = self.deterministic_extraction_staging_dir(job_id);
        let state = self.jobs.get(&job_id)?;
        let working_dir = state.working_dir.clone();
        let password = state.spec.password.clone();
        let plan = DirectSetPlan {
            set_name: format!("obfuscated-archive.f{file_index}"),
            volumes: BTreeMap::from([(0, file_index)]),
            files: HashMap::from([(file_index, 0)]),
            identity: Some(IdentityPlanFacts {
                expected_volumes: Some(1),
                discriminator: file_index,
            }),
            working_dir,
            destination_dir,
        };
        crate::runtime::perf_probe::record(
            "direct_store.identity.header_admitted",
            std::time::Duration::from_nanos(1),
        );
        info!(
            job_id = job_id.0,
            set_name = %plan.set_name,
            "direct-store admitted a standalone archive from its own RAR5 head"
        );
        let set_index = self.admit_identity_set(job_id, plan, password.as_deref());
        Some(DirectFileTarget::Route {
            set_index,
            volume_index: 0,
        })
    }

    /// Routes a freshly bound file's parked reorder-stage segments into its
    /// volume.
    ///
    /// Decode order within a file is not arrival order: on a wide connection
    /// pool a later article routinely decodes before the file's offset-zero
    /// article, and the conventional path parks it in the write reorder
    /// buffer — in memory, unwritten, because nothing flushes until the
    /// stream is contiguous from zero. Those bytes are therefore still
    /// claimable when the offset-zero article establishes the binding, and
    /// reclaiming them is what makes the identity seam immune to in-file
    /// reordering. The flush seams mark a file unbindable the moment bytes
    /// actually leave the reorder stage, so a file this runs for has nothing
    /// conventional on disk by construction.
    pub(crate) async fn reclaim_parked_segments_for_identity_bind(
        &mut self,
        file_id: NzbFileId,
        set_index: usize,
        volume_index: u32,
    ) {
        let parked = match self.write_buffers.get_mut(&file_id) {
            Some(buffer) => buffer.take_all_buffered(),
            None => return,
        };
        if !parked.is_empty() {
            let bytes = parked
                .iter()
                .map(|(_, segment)| segment.len_bytes())
                .sum::<usize>();
            self.release_write_buffered(bytes, parked.len());
            crate::runtime::perf_probe::record_value(
                "direct_store.identity.reclaimed_segments",
                parked.len() as u64,
            );
            for (offset, segment) in parked {
                let segment_number = segment.segment_id.segment_number;
                let decoded_size = segment.decoded_size;
                match self
                    .handle_direct_decode_success(set_index, volume_index, segment, offset)
                    .await
                {
                    DirectRouteOutcome::Routed => {
                        crate::runtime::perf_probe::record(
                            "direct_store.article.routed",
                            std::time::Duration::from_nanos(1),
                        );
                    }
                    DirectRouteOutcome::Conventional(segment) => {
                        // The set demoted mid-reclaim. Exactly the seam's
                        // demotion handoff: the article rejoins the reorder
                        // stage, and its placement is re-recorded because the
                        // demotion's assembly reset cleared it.
                        if let Some(file) = self
                            .jobs
                            .get_mut(&file_id.job_id)
                            .and_then(|state| state.assembly.file_mut(file_id))
                        {
                            file.record_placement(segment_number, offset, decoded_size);
                        }
                        let max_pending = self.write_buf_max_pending;
                        let buffer = self
                            .write_buffers
                            .entry(file_id)
                            .or_insert_with(|| WriteReorderBuffer::new(max_pending));
                        let len = segment.len_bytes();
                        buffer.insert(offset, segment);
                        self.note_write_buffered(len, 1);
                    }
                }
            }
        }
        if self
            .write_buffers
            .get(&file_id)
            .is_some_and(WriteReorderBuffer::is_empty)
        {
            self.write_buffers.remove(&file_id);
        }
    }

    /// Books one header binding, retiring the set's bookkeeping once its plan
    /// closed and every position bound.
    fn record_header_binding(
        &mut self,
        job_id: JobId,
        set_index: usize,
        file_index: u32,
        volume_number: u32,
    ) {
        crate::runtime::perf_probe::record(
            "direct_store.identity.bound",
            std::time::Duration::from_nanos(1),
        );
        self.reprioritize_bound_identity_file(job_id, file_index, volume_number);
        let expected = self
            .direct_store
            .set(job_id, set_index)
            .and_then(|set| set.plan().identity)
            .and_then(|identity| identity.expected_volumes);
        let Some(admission) = self.direct_store.identity.get_mut(&job_id) else {
            return;
        };
        let whole = admission
            .header_sets
            .iter_mut()
            .find(|header_set| header_set.set_index == set_index)
            .is_some_and(|header_set| {
                header_set.bound.insert(file_index, volume_number);
                expected.is_some_and(|expected| header_set.bound.len() as u32 == expected)
            });
        if whole {
            admission
                .header_sets
                .retain(|header_set| header_set.set_index != set_index);
        }
        // The entry is dropped only after an unblemished run — every set
        // retired whole, nothing leaked, nothing refused, nothing poisoned.
        // Any recorded evidence stays for the job's life instead: a later
        // rung consulting a fresh entry would forget which files already
        // leaked and re-admit exactly the unfillable set the evidence
        // refused. The retained entry costs one map probe per offset-zero
        // article.
        if admission.rosters.is_empty()
            && admission.header_sets.is_empty()
            && admission.leaked.is_empty()
            && admission.no_match.is_empty()
            && !admission.header_volume_sets_poisoned
        {
            self.direct_store.identity.remove(&job_id);
        }
    }

    /// Retires one header set: drops its bookkeeping and demotes it through
    /// the ordinary materialization.
    async fn condemn_header_set(&mut self, job_id: JobId, set_index: usize) {
        if let Some(admission) = self.direct_store.identity.get_mut(&job_id) {
            admission
                .header_sets
                .retain(|header_set| header_set.set_index != set_index);
            // Same latch as a failed roster set: identity evidence just
            // proved unreliable for this job, and a fresh header set would
            // rebuild the failure.
            admission.header_volume_sets_poisoned = true;
        }
        self.demote_direct_set(job_id, set_index, DemotionReason::IdentityRosterUnfillable)
            .await;
    }

    /// Books one established binding and retires the roster once it is whole —
    /// a fully mapped set needs no further identity work, and dropping the
    /// bookkeeping is what returns the per-article cost of this whole seam to
    /// a single map miss for the rest of the job.
    fn record_identity_binding(
        &mut self,
        job_id: JobId,
        set_name: &str,
        file_index: u32,
        volume_index: u32,
    ) {
        crate::runtime::perf_probe::record(
            "direct_store.identity.bound",
            std::time::Duration::from_nanos(1),
        );
        info!(
            job_id = job_id.0,
            set_name = %set_name,
            file_index,
            volume_index,
            "identity binding established"
        );
        self.reprioritize_bound_identity_file(job_id, file_index, volume_index);
        let Some(admission) = self.direct_store.identity.get_mut(&job_id) else {
            return;
        };
        let whole = admission.rosters.get_mut(set_name).is_some_and(|roster| {
            roster.bound.insert(file_index, volume_index);
            roster.bound.len() == roster.volumes.len()
        });
        if whole {
            admission.rosters.remove(set_name);
        }
        // The entry is dropped only after an unblemished run — every set
        // retired whole, nothing leaked, nothing refused, nothing poisoned.
        // Any recorded evidence stays for the job's life instead: a later
        // rung consulting a fresh entry would forget which files already
        // leaked and re-admit exactly the unfillable set the evidence
        // refused. The retained entry costs one map probe per offset-zero
        // article.
        if admission.rosters.is_empty()
            && admission.header_sets.is_empty()
            && admission.leaked.is_empty()
            && admission.no_match.is_empty()
            && !admission.header_volume_sets_poisoned
        {
            self.direct_store.identity.remove(&job_id);
        }
    }

    /// Marks one conventionally written segment's file as leaked and lets the
    /// viability arm draw the consequences. A no-op — one map miss — for every
    /// job without armed rosters.
    pub(crate) async fn note_identity_conventional_segment(&mut self, file_id: NzbFileId) {
        let job_id = file_id.job_id;
        let newly_leaked = self
            .direct_store
            .identity
            .get_mut(&job_id)
            .is_some_and(|admission| admission.leaked.insert(file_id.file_index));
        if newly_leaked {
            info!(
                job_id = job_id.0,
                file_index = file_id.file_index,
                "conventional bytes flushed for a file while identity admission is engaged"
            );
            self.identity_viability_sweep(job_id).await;
        }
    }

    /// Retires one roster: a pending one is simply dropped, an admitted one
    /// demotes its set through the ordinary materialization.
    async fn condemn_identity_roster(&mut self, job_id: JobId, set_name: &str) {
        let removed = self
            .direct_store
            .identity
            .get_mut(&job_id)
            .and_then(|admission| admission.rosters.remove(set_name));
        let Some(roster) = removed else {
            return;
        };
        match roster.set_index {
            Some(set_index) => {
                // An identity set failed after routing bytes. That is the
                // strongest possible evidence this job's identity picture is
                // unreliable, so no further header volume set may form from
                // it — a fresh one would re-admit the tail of exactly the
                // set that just proved unfillable.
                if let Some(admission) = self.direct_store.identity.get_mut(&job_id) {
                    admission.header_volume_sets_poisoned = true;
                }
                self.demote_direct_set(job_id, set_index, DemotionReason::IdentityRosterUnfillable)
                    .await;
            }
            None => {
                crate::runtime::perf_probe::record(
                    "direct_store.identity.dropped.roster_unfillable",
                    std::time::Duration::from_nanos(1),
                );
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    "identity roster dropped before admission"
                );
            }
        }
    }

    /// The starvation arm: retires every roster whose unclaimed volumes
    /// outnumber the files that could still claim one.
    ///
    /// An identity set with an unclaimable volume is a starved set — it never
    /// finalizes and never demotes on its own (see
    /// [`DemotionReason::IdentityRosterUnfillable`]) — so this must fire from
    /// every event that shrinks the candidate pool: a leak, a settled
    /// no-match, and arming itself. The pool is counted conservatively: a
    /// file no evidence has touched stays a candidate for every roster.
    async fn identity_viability_sweep(&mut self, job_id: JobId) {
        let (condemned, condemned_header_sets): (Vec<String>, Vec<usize>) = {
            let Some(admission) = self.direct_store.identity.get(&job_id) else {
                return;
            };
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let carrier_files = self.identity_par2_carrier_files(job_id);
            let bound_files: HashSet<u32> = admission
                .rosters
                .values()
                .flat_map(|roster| roster.bound.keys().copied())
                .chain(
                    admission
                        .header_sets
                        .iter()
                        .flat_map(|header_set| header_set.bound.keys().copied()),
                )
                .collect();
            let viable = (0..state.spec.files.len() as u32)
                .filter(|file_index| {
                    !admission.leaked.contains(file_index)
                        && !admission.no_match.contains(file_index)
                        && !bound_files.contains(file_index)
                        && !carrier_files.contains(file_index)
                        && !matches!(
                            state.spec.files[*file_index as usize].role,
                            weaver_model::files::FileRole::Par2 { .. }
                        )
                })
                .count();
            let condemned: Vec<String> = admission
                .rosters
                .iter()
                .filter(|(_, roster)| roster.volumes.len() - roster.bound.len() > viable)
                .map(|(set_name, _)| set_name.clone())
                .collect();
            for set_name in &condemned {
                let roster = &admission.rosters[set_name];
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    unbound = roster.volumes.len() - roster.bound.len(),
                    viable,
                    leaked = admission.leaked.len(),
                    no_match = admission.no_match.len(),
                    "identity roster can no longer be filled"
                );
            }
            // A header set is judged only once its plan closed — an open one
            // has no size to fall short of, and its own arms (a leaked file
            // proving to be a set volume, a duplicate position claim) retire
            // it on direct evidence instead.
            let condemned_header_sets = admission
                .header_sets
                .iter()
                .filter(|header_set| {
                    header_set.volume_set
                        && self
                            .direct_store
                            .set(job_id, header_set.set_index)
                            .and_then(|set| set.plan().identity)
                            .and_then(|identity| identity.expected_volumes)
                            .is_some_and(|expected| {
                                (header_set.bound.len() as u32) < expected
                                    && expected as usize - header_set.bound.len() > viable
                            })
                })
                .map(|header_set| header_set.set_index)
                .collect();
            (condemned, condemned_header_sets)
        };
        for set_name in condemned {
            self.condemn_identity_roster(job_id, &set_name).await;
        }
        for set_index in condemned_header_sets {
            self.condemn_header_set(job_id, set_index).await;
        }
    }

    /// Re-reads the job's password into every set still willing to take one.
    ///
    /// The reason this exists at all: **weaver does support setting a password
    /// after add** — the GraphQL `setJobPassword` mutation and the NZBGet
    /// facade's `editqueue` / `GroupSetParameter *Unpack:Password` both mutate
    /// the live `JobSpec` in place — and [`Self::ensure_direct_sets`] is
    /// memoized per job, so a set built before the password arrived would never
    /// see it. Re-reading it here costs one map lookup per article and stops
    /// the moment a set **admits** a password or leaves direct mode, which for
    /// every set with no encrypted member is the first parse.
    ///
    /// # The window closes at the first header parse
    ///
    /// Admission runs from the first successful header parse, so "after the job
    /// was added" means *before the first article of the first volume*, not any
    /// time during the download. A password arriving later finds the set already
    /// demoted under `EncryptedMemberRefused(NoPassword)` and does not revive
    /// it — see [`super::router::DirectSetRouter::wants_password`] for why
    /// waiting instead would be worse than demoting.
    ///
    /// Within that window a **changed** password does land, which is the case
    /// the narrower "still has no password" test used to drop on the floor: a
    /// job added with the wrong password and corrected before its first parse
    /// now admits with the correction rather than deriving keys from the stale
    /// one and failing the keyed member gate a whole download later.
    ///
    /// It deliberately does **not** re-admit a set that already demoted for a
    /// wrong or missing password, or one that has already admitted. Re-admission
    /// would mean re-decrypting every byte already written under the old
    /// verdict, which is a demotion with extra steps; the conventional path
    /// takes the set and asks the job's whole candidate list, which is a
    /// superset of this one.
    fn refresh_direct_passwords(&mut self, job_id: JobId) {
        self.offer_direct_header_passwords(job_id);
        if !self
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| set.router.wants_password())
        {
            return;
        }
        let Some(password) = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.spec.password.clone())
        else {
            return;
        };
        // `offer_direct_header_passwords` runs **once** per job and
        // every set wants a header password from creation, so the harvest is
        // memoized on the job's first article and can never re-run. That is fine
        // for `NzbMeta` and `FilenameConvention`, which are immutable per job —
        // and not fine for the spec's password, which is not. This line is its
        // only route into the `-hp` ring afterwards, and without it a password
        // supplied mid-download reaches the *file* key and never the archive
        // one, so a `-hp` set that had a password all along still refuses under
        // `NoPassword`.
        //
        // Normalized the way the harvest normalizes, so a placeholder like
        // `"yes"` — which `archive_password_candidates_for_job` drops — is not
        // smuggled past it here and paid for in PBKDF2.
        //
        // Labelled `job_spec` rather than `explicit` because that is all that is
        // known: for a job imported from an NZB, `import.rs` seeds `spec.password`
        // from the harvest's *first* candidate, which is usually the NZB meta
        // password or the `{{…}}` filename convention. The label only reaches a
        // refusal's `sources` field, and a field that says where a candidate came
        // from should not guess.
        let offered = crate::ingest::normalize_archive_password_candidate(Some(password.as_str()));
        for set in self.direct_store.sets_mut(job_id) {
            set.router.set_password(Some(password.as_str()));
            // Offering is a no-op once the ring has verified or refused, and a
            // string compare against at most three held candidates otherwise.
            if let Some(value) = offered.as_deref() {
                set.router.offer_header_password("job_spec", value);
            }
        }
    }

    /// Hands the job's archive-password harvest to every set's `-hp` gate,
    /// once per job.
    ///
    /// # Why the whole harvest, and not `spec.password`
    ///
    /// `spec.password` is the harvest's *first* candidate, which for a job
    /// imported from an NZB is the `nzb.meta.password` or the `{{password}}`
    /// filename convention — so it is usually the right one already. It stops
    /// being enough the moment an operator supplies an explicit password:
    /// that one takes priority in the spec, and a set whose archive key is the
    /// NZB-meta password would then refuse for a password the job was holding
    /// all along. The list is bounded by construction — `Explicit`, `NzbMeta`,
    /// `FilenameConvention`, at most one each — so this bounds the `-hp` gate's
    /// KDF work at three derivations however deep the archive asks for.
    ///
    /// # Why here rather than in `ensure_direct_sets`
    ///
    /// The harvest reads the job's persisted NZB, so it must not run per
    /// article; and it must reach **restored** sets, which never go through
    /// `ensure_direct_sets` at all — `install_restored` marks the job examined
    /// precisely so the lazy seam does not rediscover them. This runs from the
    /// one seam both populations pass through, and memoizes on the same job set
    /// `clear_job` clears.
    ///
    /// # Cost
    ///
    /// One persisted-NZB read per job that admitted a direct set, ever — the
    /// `-hp` gate has to hold its candidates *before* the first header parse,
    /// because that parse is where admission happens, and nothing cheaper than
    /// the parse itself can say whether a set is `-hp`. That is strictly less
    /// than the conventional path already pays: `try_update_archive_topology`
    /// harvests once **per volume parse**.
    fn offer_direct_header_passwords(&mut self, job_id: JobId) {
        if self
            .direct_store
            .header_candidates_offered
            .contains(&job_id)
        {
            return;
        }
        if !self
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| set.router.wants_header_password())
        {
            return;
        }
        // Armed on the harvest having **run**, not on it having run *first*.
        // The NZB half is a database read and a parse, and both
        // warn-and-continue with an empty list; memoizing before that meant one
        // transient error at exactly this instant cost the job its `NzbMeta`
        // and `FilenameConvention` candidates for the rest of its life, with no
        // second chance — `wants_header_password()` is the only other gate and
        // it is still true. A harvest that ran and found nothing is a *fact*
        // about the job and is remembered; one that failed is not.
        //
        // Deliberately not "arm only when candidates were found": for the
        // overwhelming majority of jobs there is no password anywhere, and that
        // would re-read and re-parse the persisted NZB on **every article**.
        let (candidates, harvested) = self.harvest_archive_password_candidates(job_id);
        if harvested {
            self.direct_store.header_candidates_offered.insert(job_id);
        }
        if candidates.is_empty() {
            return;
        }
        for set in self.direct_store.sets_mut(job_id) {
            for candidate in &candidates {
                set.router
                    .offer_header_password(candidate.source().as_str(), candidate.value());
            }
        }
    }

    /// What to do with one NZB file's decoded bytes.
    ///
    /// `None` when the file is not a direct set's source volume, and `None`
    /// once its set has demoted — which is exactly what hands the volume back
    /// to the conventional path.
    pub(crate) fn direct_route_target(&mut self, file_id: NzbFileId) -> Option<DirectFileTarget> {
        self.ensure_direct_sets(file_id.job_id);
        self.refresh_direct_passwords(file_id.job_id);
        self.direct_store
            .sets_for(file_id.job_id)
            .iter()
            .enumerate()
            .find_map(|(index, set)| {
                if set.is_demoted() {
                    return None;
                }
                let volume_index = set.plan().volume_for_file(file_id.file_index)?;
                Some(if set.is_finalized() {
                    DirectFileTarget::Discard
                } else {
                    DirectFileTarget::Route {
                        set_index: index,
                        volume_index,
                    }
                })
            })
    }

    /// Take any set claiming this file off the direct path, because its
    /// articles arrived uuencoded.
    ///
    /// Sets are admitted from the NZB's filenames, before a single article has
    /// been decoded, so an archive posted in uuencode is admitted exactly like a
    /// yEnc one. It can never be routed: routing writes an article's bytes into
    /// a volume at the offset the article declares, and a uuencode article
    /// declares no offset — its position is the decoded length of its whole
    /// prefix, which only sequential assembly can supply.
    ///
    /// Excluding those articles from the routing seam is not enough on its own.
    /// A set that is merely starved never finalizes and never demotes, so every
    /// suppression keyed on [`Self::is_direct_source_file`] keeps holding for
    /// its volumes — including the archive probe that dispatches extraction,
    /// which would leave the job completing with its archive unextracted on
    /// disk. Demoting puts the volumes back on the conventional path, where the
    /// sequential cursor is already writing them.
    ///
    /// `ensure_direct_sets` runs first for the same reason
    /// [`Self::direct_route_target`] runs it: admission is lazy, so a job whose
    /// very first article is uuencoded would otherwise find no set to demote
    /// and admit one moments later.
    pub(crate) async fn demote_direct_sets_for_uu_article(&mut self, file_id: NzbFileId) {
        let job_id = file_id.job_id;
        self.ensure_direct_sets(job_id);
        // Identity rosters go with the sets, and for the same reason: nothing
        // uuencoded can ever be routed, so a binding that would only starve is
        // never made.
        self.direct_store.identity.remove(&job_id);
        let set_indices: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter_map(|(index, set)| {
                (!set.is_demoted() && set.plan().volume_for_file(file_id.file_index).is_some())
                    .then_some(index)
            })
            .collect();
        for set_index in set_indices {
            self.demote_direct_set(job_id, set_index, DemotionReason::UuencodedSourceVolume)
                .await;
        }
    }

    /// Whether this file's bytes are a direct set's source volume, so no legacy
    /// floor, completed-file row or archive re-probe may be written for it.
    /// `&self`, because the suppression checks sit inside paths that already
    /// hold the pipeline immutably.
    ///
    /// Deliberately **not** narrowed to still-routing sets the way
    /// [`Self::direct_route_target`] is: a finalized set's source volumes were
    /// never written and never will be, so every suppression the routing seam
    /// relied on has to keep holding afterwards. Only a demotion puts the
    /// volume back on the conventional path, and only then does it get a file.
    pub(crate) fn is_direct_source_file(&self, file_id: NzbFileId) -> bool {
        self.direct_store
            .sets_for(file_id.job_id)
            .iter()
            .any(|set| {
                !set.is_demoted() && set.plan().volume_for_file(file_id.file_index).is_some()
            })
    }

    /// The virtual volume behind one direct source file, as a **one-volume**
    /// provider plus its logical length.
    ///
    /// A test-only accessor: production reads a direct set through
    /// [`super::par2_access::DirectVolumeFileAccess`], which builds the whole
    /// set's provider once for the pass. This answers the one-volume question a
    /// test asks when it wants to inspect what a single volume reads back as,
    /// without rebuilding the set's plan lookup in test code.
    ///
    /// The length is the decoded total the download layer tracks, never a
    /// file's `metadata().len()`: for a direct volume there is no file to ask.
    /// `None` for anything that is not a live direct set's source volume,
    /// including a demoted set's — whose volumes are materializing or being
    /// refetched, and are read from disk like any other file.
    ///
    /// An **encrypted** set answers here like any other: the provider
    /// re-encrypts the member ranges it reads out of the partials, so what comes
    /// back is the posted bytes the caller asked for rather than the plaintext
    /// sitting on disk.
    #[cfg(test)]
    pub(crate) fn direct_virtual_volume(
        &self,
        file_id: NzbFileId,
    ) -> Option<(u32, u64, super::provider::HybridVolumeProvider)> {
        let job_id = file_id.job_id;
        let (set, volume_index) =
            self.direct_store
                .sets_for(job_id)
                .iter()
                .find_map(|set| match set.is_demoted() {
                    true => None,
                    false => set
                        .plan()
                        .volume_for_file(file_id.file_index)
                        .map(|volume_index| (set, volume_index)),
                })?;
        let received = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.file(file_id))
            .map(|file| file.received_bytes())
            .unwrap_or(0);
        let len = set.virtual_volume_len(volume_index, received);
        let lengths = std::collections::BTreeMap::from([(volume_index, len)]);
        Some((volume_index, len, set.virtual_provider(&lengths)))
    }

    /// The direct sets of `job_id` that the authoritative PAR2 pass must read
    /// virtually, or `None` when it has none and today's `PlacementFileAccess`
    /// is the whole answer.
    ///
    /// A volume is included only when its PAR2 identity resolves unambiguously
    /// through the same name candidates the grid's binding resolver uses. An
    /// unresolved one is skipped **here**, but that skip is not the safety net:
    /// a half-bound set would have the pass read its remaining volumes off a
    /// disk they are not on and report them missing, and
    /// [`Self::demote_direct_sets_with_par2_damage`] could not even attribute
    /// that damage back to the set, because attribution is keyed by the very
    /// binding that failed. The net is [`Self::demote_unbindable_direct_sets`],
    /// which runs *before* the pass and demotes any live set with an unbindable
    /// volume outright, so what reaches here is either a fully bound set or no
    /// set at all.
    pub(crate) fn direct_par2_overlay(&self, job_id: JobId) -> Option<DirectPar2Overlay> {
        self.direct_par2_overlay_for_set(job_id, self.par2_served_set_id(job_id)?)
    }

    /// The virtual direct volumes that bind wholly to one recovery set.
    ///
    /// The compatibility wrapper above still answers the served set. Callers
    /// that already know which recovery set they are verifying must use this
    /// form, so a direct set owned by another parsed set is neither read nor
    /// damaged by the wrong pass.
    pub(crate) fn direct_par2_overlay_for_set(
        &self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
    ) -> Option<DirectPar2Overlay> {
        let mut volumes = Vec::new();
        let mut virtual_volumes = Vec::new();
        let mut sets = HashMap::new();
        let mut file_indices = HashMap::new();
        let mut set_lengths = Vec::new();
        for (set_index, set) in self.direct_store.sets_for(job_id).iter().enumerate() {
            // A demoted set's volumes are materializing or being refetched, so
            // they are read from disk like any other file.
            if set.is_demoted() {
                continue;
            }
            // A **finalized** set has renamed its partials to their
            // destinations and — unless it was asked to keep them for a live
            // neighbour — deleted its envelopes, so nothing answers for its
            // source volumes and serving it would report damage that is not
            // there. One that *did* keep them serves the very same image out of
            // the committed members instead, which is what lets a neighbour's
            // repair read the surviving input slices Reed–Solomon needs from
            // every file the recovery set describes. Either way it is never a
            // repair *target*: `repair_direct_sets_with_par2_damage` skips a
            // finalized set, and `forgive_finalized_direct_volumes` still
            // excuses one whose image was not kept.
            let retained = set.retained_volumes();
            if set.is_finalized() && retained.is_none() {
                continue;
            }
            // One `virtual_volumes` call for the whole set, so the shared
            // partial-path map is built once rather than once per volume (nit).
            let mut lengths = std::collections::BTreeMap::new();
            let mut bindings = HashMap::new();
            let mut belongs_to_recovery_set = true;
            for (volume_index, file_index) in &set.plan().volumes {
                let file_id = NzbFileId {
                    job_id,
                    file_index: *file_index,
                };
                let Some(binding) = self.resolve_par2_file_binding(file_id) else {
                    belongs_to_recovery_set = false;
                    break;
                };
                if binding.recovery_set_id != recovery_set_id {
                    belongs_to_recovery_set = false;
                    break;
                }
                // A retained image carries the lengths it was captured with, so
                // it stops depending on an assembly the job may have moved on
                // from.
                let len = match retained {
                    Some(volumes) => volumes
                        .iter()
                        .find(|volume| volume.volume_index == *volume_index)
                        .map(|volume| volume.len)
                        .unwrap_or_default(),
                    None => {
                        let received = self
                            .jobs
                            .get(&job_id)
                            .and_then(|state| state.assembly.file(file_id))
                            .map(|file| file.received_bytes())
                            .unwrap_or(0);
                        set.virtual_volume_len(*volume_index, received)
                    }
                };
                lengths.insert(*volume_index, len);
                bindings.insert(
                    *volume_index,
                    (*file_index, binding.par2_file_id, binding.recovery_set_id),
                );
            }
            if !belongs_to_recovery_set {
                continue;
            }
            let set_volumes = match retained {
                Some(volumes) => volumes.to_vec(),
                None => set.virtual_volumes(&lengths),
            };
            for mut volume in set_volumes {
                let Some((file_index, par2_file_id, binding_set_id)) =
                    bindings.get(&volume.volume_index).copied()
                else {
                    continue;
                };
                debug_assert_eq!(binding_set_id, recovery_set_id);
                // Re-keyed from the set's own volume index to the job's file
                // index: a job can hold several sets, each numbering its volumes
                // from zero, and one provider answers for all of them.
                volume.volume_index = file_index;
                virtual_volumes.push(volume);
                volumes.push(super::par2_access::VirtualPar2Volume {
                    par2_file_id,
                    volume_index: file_index,
                });
                sets.insert(par2_file_id, set_index);
                file_indices.insert(par2_file_id, file_index);
            }
            if !lengths.is_empty() {
                set_lengths.push((set_index, lengths));
            }
        }
        if volumes.is_empty() {
            return None;
        }
        Some(DirectPar2Overlay {
            recovery_set_id,
            provider: super::provider::HybridVolumeProvider::new(virtual_volumes),
            volumes,
            sets,
            file_indices,
            lengths: set_lengths,
        })
    }

    /// Whether the authoritative PAR2 pass may run over `job_id`'s direct sets
    /// yet, or has to wait for their payload.
    ///
    /// Deliberately the same shape as the completion gate's own
    /// `par2_primary_payload_ready`: **every live set's volumes have finished
    /// downloading, or nothing more is coming**. A set that is still receiving
    /// articles reads its outstanding ranges as holes, and PAR2 cannot tell a
    /// hole from corruption — so a pass run early would report damage that is
    /// only a download in progress, demote a healthy set and hand the repairer
    /// volumes it would have to rebuild from scratch. The second half of the
    /// disjunction is what keeps this from waiting forever: once the download
    /// pipeline has drained, the holes are permanent and the verdict is real.
    ///
    /// `true` for every job with no live direct set, which is every conventional
    /// job — the gate is unchanged for them by construction.
    fn direct_set_binds_to_par2_set(
        &self,
        job_id: JobId,
        direct_set: &DirectSet,
        recovery_set_id: par2_rs::RecoverySetId,
    ) -> bool {
        direct_set
            .plan()
            .volumes
            .values()
            .copied()
            .any(|file_index| {
                self.resolve_par2_file_binding(NzbFileId { job_id, file_index })
                    .is_some_and(|binding| binding.recovery_set_id == recovery_set_id)
            })
    }

    pub(crate) fn direct_sets_ready_for_authoritative_par2_for_set(
        &self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
    ) -> bool {
        let waiting = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .filter(|set| self.direct_set_binds_to_par2_set(job_id, set, recovery_set_id))
            .any(|set| !set.is_demoted() && !set.is_finalized() && !set.all_volumes_complete());
        !waiting || !self.job_has_pending_download_pipeline_work(job_id)
    }

    pub(crate) fn direct_sets_ready_for_authoritative_par2(&self, job_id: JobId) -> bool {
        self.par2_served_set_id(job_id).is_none_or(|set_id| {
            self.direct_sets_ready_for_authoritative_par2_for_set(job_id, set_id)
        })
    }

    /// The one ownership gate between direct demotion and every PAR2 verdict.
    /// A pending file leaves through the durable conventional completion seam,
    /// or once every article it still lacks is terminally unavailable.
    pub(crate) fn demoted_materializations_ready_for_par2(
        &mut self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
    ) -> bool {
        let pending = self.direct_store.pending_materializations(job_id);
        if pending.is_empty() {
            return true;
        }
        if !self.jobs.contains_key(&job_id) {
            self.direct_store.clear_pending_materializations(job_id);
            return true;
        }

        let mut ready = true;
        for (set_index, pending) in pending {
            let applicability = self.direct_store.set(job_id, set_index).map(|set| {
                let mut unresolved = false;
                let binds_served =
                    set.plan().volumes.values().copied().any(|file_index| {
                        match self.resolve_par2_file_binding(NzbFileId { job_id, file_index }) {
                            Some(binding) => binding.recovery_set_id == recovery_set_id,
                            None => {
                                unresolved = true;
                                false
                            }
                        }
                    });
                binds_served || unresolved
            });
            match applicability {
                Some(true) => {}
                Some(false) => continue,
                None => {
                    for file_id in pending.files {
                        self.direct_store.settle_materialized_file(file_id);
                    }
                    continue;
                }
            }

            for file_id in pending.files {
                let Some((missing, file_has_owner)) = self.jobs.get(&job_id).and_then(|state| {
                    let file = state.spec.files.get(file_id.file_index as usize)?;
                    let assembly = state.assembly.file(file_id)?;
                    let mut owned = HashSet::new();
                    state.download_queue.extend_segment_ids(&mut owned);
                    state.recovery_queue.extend_segment_ids(&mut owned);
                    owned.extend(state.held_segments.iter().map(|work| work.segment_id));

                    let missing = file
                        .segments
                        .iter()
                        .filter(|segment| !assembly.has_segment(segment.ordinal))
                        .map(|segment| {
                            let segment_id = SegmentId {
                                file_id,
                                segment_number: segment.ordinal,
                            };
                            (
                                segment_id,
                                DownloadWork {
                                    segment_id,
                                    message_id: crate::jobs::ids::MessageId::new(
                                        &segment.message_id,
                                    ),
                                    groups: std::sync::Arc::from(file.groups.as_slice()),
                                    priority: file.role.download_priority(),
                                    byte_estimate: segment.bytes,
                                    retry_count: 0,
                                    is_recovery: false,
                                    completion_critical: false,
                                    exclude_servers: vec![],
                                    avoid_server: None,
                                },
                                owned.contains(&segment_id),
                            )
                        })
                        .collect::<Vec<_>>();
                    let file_has_owner = self
                        .active_downloads_by_file
                        .get(&file_id)
                        .is_some_and(|count| *count > 0)
                        || self
                            .active_decodes_by_file
                            .get(&file_id)
                            .is_some_and(|count| *count > 0)
                        || self
                            .write_buffers
                            .get(&file_id)
                            .is_some_and(|buffer| !buffer.is_empty())
                        || self
                            .pending_released_download_results_by_job
                            .get(&job_id)
                            .is_some_and(|count| *count > 0);
                    Some((missing, file_has_owner))
                }) else {
                    self.direct_store.settle_materialized_file(file_id);
                    continue;
                };

                // No missing article does not mean durable yet: the completing
                // commit still owes its buffer flush, handle release and row.
                if missing.is_empty() {
                    ready = false;
                    continue;
                }
                if missing
                    .iter()
                    .all(|(segment_id, _, _)| self.segment_terminal_states.contains_key(segment_id))
                {
                    self.direct_store.settle_materialized_file(file_id);
                    continue;
                }

                let has_owner = file_has_owner
                    || pending
                        .handoffs
                        .iter()
                        .any(|segment_id| segment_id.file_id == file_id)
                    || missing.iter().any(|(segment_id, _, queued)| {
                        *queued
                            || self.pending_retries_by_segment.contains_key(segment_id)
                            || self.server_quota_parked.contains(segment_id)
                    });
                if has_owner {
                    ready = false;
                    continue;
                }

                let missing_ids: Vec<SegmentId> = missing
                    .iter()
                    .map(|(segment_id, _, _)| *segment_id)
                    .collect();
                let mut rescued = Vec::new();
                for (segment_id, work, _) in missing {
                    if self.segment_terminal_states.contains_key(&segment_id) {
                        continue;
                    }
                    if pending.rescued.contains(&segment_id) {
                        self.book_failed_segment(segment_id);
                    } else if self
                        .direct_store
                        .note_materialization_rescue(job_id, set_index, segment_id)
                    {
                        rescued.push(work);
                    }
                }
                for work in rescued {
                    self.requeue_retry_work(work);
                    ready = false;
                }
                if missing_ids
                    .iter()
                    .all(|segment_id| self.segment_terminal_states.contains_key(segment_id))
                {
                    self.direct_store.settle_materialized_file(file_id);
                } else {
                    ready = false;
                }
            }
        }
        ready
    }

    /// Demotes every live direct set of `job_id` holding a source volume that
    /// cannot be bound, unambiguously, to a PAR2 description.
    ///
    /// The overlay is keyed by PAR2 file id, so an unbound volume is one the
    /// pass cannot be told about *and* one whose verdict cannot be attributed
    /// back to its set. Leaving it out — which is all
    /// [`Self::direct_par2_overlay`] can do on its own — produces the worst of
    /// both: the pass reads that volume off a disk it is not on and calls it
    /// missing, `demote_direct_sets_with_par2_damage` finds no set to blame, and
    /// the repairer is handed a virtual volume to write into. A set with *every*
    /// volume unbound does not even produce an overlay, so the damage path is
    /// skipped entirely.
    ///
    /// Demoting up front is what makes the pass's world binary: either a fully
    /// bound virtual set, or real files on disk.
    pub(crate) async fn demote_unbindable_direct_sets_for_set(
        &mut self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
    ) -> bool {
        // Without a parsed recovery set nothing can bind, and demoting every
        // direct set of a job whose PAR2 has simply not arrived yet would undo
        // the whole feature.
        if self.par2_set(job_id).is_none() {
            return false;
        }
        // An encrypted set is served to the pass through the
        // re-encrypting overlay like any other — but only while the overlay can
        // really reproduce what was posted. The residual it cannot is a routed
        // encrypted member with no declared cipher size, or one whose tail
        // padding is not whole, and such a set is taken out here rather than
        // half-answered: the pass's world has to stay binary, exactly as the
        // unbindable rule below keeps it.
        let unavailable: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| self.direct_set_binds_to_par2_set(job_id, set, recovery_set_id))
            .filter(|(_, set)| !set.is_demoted() && !set.is_finalized())
            .filter(|(_, set)| set.router.posted_bytes_unavailable())
            .map(|(set_index, _)| set_index)
            .collect();
        let mut demoted_any = !unavailable.is_empty();
        for set_index in unavailable {
            warn!(
                job_id = job_id.0,
                "an encrypted direct set cannot reproduce its posted bytes; demoting so the \
                 authoritative pass reads real files instead of a volume the overlay can only \
                 half answer"
            );
            self.demote_direct_set(
                job_id,
                set_index,
                DemotionReason::EncryptedPostedBytesUnavailable,
            )
            .await;
        }
        let unbindable: Vec<(usize, u32)> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| self.direct_set_binds_to_par2_set(job_id, set, recovery_set_id))
            .filter(|(_, set)| !set.is_demoted() && !set.is_finalized())
            .filter_map(|(set_index, set)| {
                set.plan()
                    .volumes
                    .iter()
                    .find(|(_, file_index)| {
                        self.resolve_par2_file_binding(NzbFileId {
                            job_id,
                            file_index: **file_index,
                        })
                        .is_none()
                    })
                    .map(|(volume_index, _)| (set_index, *volume_index))
            })
            .collect();
        if unbindable.is_empty() {
            return demoted_any;
        }
        demoted_any = true;
        for (set_index, volume_index) in unbindable {
            warn!(
                job_id = job_id.0,
                volume_index,
                "a direct set's source volume has no unambiguous PAR2 identity; demoting \
                 so the authoritative pass reads a real file instead of a volume it \
                 cannot name"
            );
            self.demote_direct_set(job_id, set_index, DemotionReason::Par2Unbindable)
                .await;
        }
        demoted_any
    }

    pub(crate) async fn demote_unbindable_direct_sets(&mut self, job_id: JobId) -> bool {
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return false;
        };
        self.demote_unbindable_direct_sets_for_set(job_id, set_id)
            .await
    }

    /// Rewrites `Missing` to `Complete` for every source volume of a
    /// **finalized** direct set, before the caller counts damage.
    ///
    /// Exactly the eager-delete precedent, and for exactly the same reason: the
    /// bytes were verified and the file is legitimately absent. A finalized set
    /// passed the whole-member CRC32 gate on every member *and* the job's own
    /// PAR2 verdict — finalization is gated on that verdict — and then renamed
    /// its partials to their destinations and deleted its envelopes. Nothing on
    /// disk answers for its source volumes afterwards, and nothing should: they
    /// were never written and never will be.
    ///
    /// Without this, any *later* pass over the same job — a conventional set's
    /// extraction failing after the direct set finalized is enough — reports
    /// every finalized volume missing and either fails the job as unrepairable
    /// or has the repairer reconstruct source volumes onto disk that the job
    /// already finished without.
    ///
    /// Live and demoted sets are deliberately untouched: a live set's volumes
    /// are served virtually and its verdict is real, and a demoted set's are
    /// materializing or being refetched, so missing means missing.
    ///
    /// # Retention does not replace it, and does not fight it either
    ///
    /// A set that finalized beside a live neighbour keeps its envelopes and
    /// serves its volumes out of the committed members
    /// ([`Self::retain_finalized_direct_volumes`]), so in that window they read
    /// `Complete` on their own and there is nothing here to forgive — the same
    /// verdict, reached by reading rather than by excusing. This still runs, and
    /// still has to: retention covers one window of one shape, and the pass that
    /// motivated this rule is the *later* one, over a job whose sets are all
    /// committed and whose envelopes are therefore gone. It is also the only
    /// answer on the paths that read no overlay at all — `analyze_par2_damage`'s
    /// filesystem-bound repairer among them.
    ///
    /// Deliberately confined to `Missing`. A retained volume that read `Damaged`
    /// would be one whose destination moved or whose image is short, and
    /// excusing that would hand the repair bytes it should not trust; it is left
    /// as damage, the repair refuses on an unmaterialized write target, and the
    /// job falls back to the demotion path.
    ///
    /// Returns the number of missing slices forgiven.
    pub(crate) fn forgive_finalized_direct_volumes(
        &self,
        job_id: JobId,
        verification: &mut par2_rs::VerificationResult,
    ) -> u32 {
        if self.par2_set(job_id).is_none() {
            return 0;
        }
        let finalized: HashSet<par2_rs::FileId> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .filter(|set| set.is_finalized() && !set.is_demoted())
            .flat_map(|set| set.plan().volumes.values().copied())
            .filter_map(|file_index| {
                let file_id = NzbFileId { job_id, file_index };
                // Belt and braces: the set says the volume is its own, and
                // `is_direct_source_file` is the rule every other suppression
                // point reads, so the two cannot drift apart here.
                if !self.is_direct_source_file(file_id) {
                    return None;
                }
                self.resolve_par2_file_binding(file_id)
                    .map(|binding| binding.par2_file_id)
            })
            .collect();
        if finalized.is_empty() {
            return 0;
        }

        let mut forgiven = 0u32;
        for file in &mut verification.files {
            if !matches!(file.status, par2_rs::verify::FileStatus::Missing)
                || !finalized.contains(&file.file_id)
            {
                continue;
            }
            forgiven = forgiven.saturating_add(file.missing_slice_count);
            file.status = par2_rs::verify::FileStatus::Complete;
            file.valid_slices.fill(true);
            file.missing_slice_count = 0;
        }
        if forgiven == 0 {
            return 0;
        }
        verification.total_missing_blocks =
            verification.total_missing_blocks.saturating_sub(forgiven);
        verification.refresh_repairability();
        forgiven
    }

    /// Answers PAR2 damage on a job's direct sets.
    ///
    /// The entry point, and the whole of *repair while still direct* transition
    /// seen from the pipeline. It tries the repair first and falls back to the
    /// whole-set demotion on any refusal, so `Resolved` means the job's next
    /// move is a fresh completion check, over either repaired virtual volumes
    /// or materialized physical ones.
    ///
    /// `Deferred` is the third answer and it is not a refusal: the damage is
    /// coverable by the recovery set, just not by the slices merged so far, so
    /// the missing recovery has been asked for and the sets are staying direct
    /// until it arrives. Falling through to the demotion there would materialize
    /// every volume moments before the blocks that would have repaired them in
    /// place land.
    ///
    /// The ordering is normative:
    ///
    /// 1. the set's **checkpoint row is deleted first**, because everything
    ///    below rewrites bytes the row claims. The next barrier recreates
    ///    coverage from scratch. Deliberately lossy: a crash between here and
    ///    that barrier costs a full redownload of the set, which is bounded and
    ///    is what the whole model already accepts for uncheckpointed work;
    /// 2. only the damaged volumes materialize, into scratch files;
    /// 3. the repair runs with every clean volume read **virtually**;
    /// 4. the repaired spans re-enter the router with replacement semantics and
    ///    their destination writes are awaited before anything is recorded;
    /// 5. the stale composition gaps the rewrite left are re-read from the
    ///    partials that hold them, which re-arms the whole-member gates;
    /// 6. the scratch is deleted, and the set is back to fully virtual.
    pub(crate) async fn resolve_direct_sets_with_par2_damage_for_set(
        &mut self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
        verification: &par2_rs::VerificationResult,
    ) -> DirectDamageResolution {
        match self
            .repair_direct_sets_with_par2_damage(job_id, verification)
            .await
        {
            DirectRepairAnswer::Acted => return DirectDamageResolution::Resolved,
            DirectRepairAnswer::Deferred => return DirectDamageResolution::Deferred,
            DirectRepairAnswer::Declined => {}
        }
        if self
            .demote_direct_sets_with_par2_damage_for_set(job_id, recovery_set_id, verification)
            .await
        {
            DirectDamageResolution::Resolved
        } else {
            DirectDamageResolution::Unresolved
        }
    }

    pub(crate) async fn resolve_direct_sets_with_par2_damage(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) -> DirectDamageResolution {
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return DirectDamageResolution::Unresolved;
        };
        self.resolve_direct_sets_with_par2_damage_for_set(job_id, set_id, verification)
            .await
    }

    /// The repair chance for a live direct set, taken **before** the completion
    /// gate hands the job to `Par2Repairer`.
    ///
    /// That branch exists for jobs the fast paths could not clear, and a live
    /// direct set reaches it routinely: it contributes nothing to the
    /// clean-PAR2 integrity gate — a direct set never enters the archive
    /// topology — so a damaged one always arrives here rather than at the
    /// verify branch. The repairer is filesystem-bound, so today's answer is
    /// [`Self::demote_live_direct_sets_for_par2_repair`]: materialize
    /// everything and let it work over real files. This is what repair puts in
    /// front of that, and `Unresolved` means the demotion is still the answer.
    ///
    /// The verdict is computed here rather than borrowed, because the branch has
    /// none yet. It is deliberately a **quiet** pass — no status transition, no
    /// verification events — for two reasons: the analyze pass immediately below
    /// emits its own, so a job that falls through would report verifying twice;
    /// and this one exists to answer a question about direct sets, not to record
    /// the job's verdict.
    pub(crate) async fn resolve_direct_sets_before_par2_repairer_for_set(
        &mut self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
        par2_set: std::sync::Arc<par2_rs::Par2FileSet>,
        working_dir: PathBuf,
    ) -> DirectPar2Resolution {
        if !self.direct_store.sets_for(job_id).iter().any(|set| {
            self.direct_set_binds_to_par2_set(job_id, set, recovery_set_id)
                && !set.is_demoted()
                && !set.is_finalized()
        }) {
            return DirectPar2Resolution::Unresolved;
        }
        // A job already waiting on a promoted recovery wave answers without
        // verifying anything. The pass below is a full PAR2 scan, the gate ticks
        // on every article that completes, and until the wave has merged the
        // scan can only reach the verdict that started the wait — so re-running
        // it is the repeated-scan storm this branch has already paid for once,
        // at ~64 slow scans on a single job while 75 others starved. When the
        // wave has drained the fast path lapses and the pass runs, which is
        // exactly the one moment it can learn something new.
        if self.direct_store.repair_defer_pending(job_id)
            && self.job_has_promoted_recovery_pipeline_work(job_id, "direct repair defer")
        {
            return DirectPar2Resolution::Deferred;
        }
        let Some(verification) = self
            .verify_direct_sets_quietly(job_id, par2_set, working_dir)
            .await
        else {
            return if self.direct_post_repair_in_flight.contains_key(&job_id) {
                DirectPar2Resolution::Pending
            } else {
                DirectPar2Resolution::Unresolved
            };
        };
        if !verification.needs_repair() {
            return DirectPar2Resolution::Clean(Box::new(verification));
        }
        match self
            .repair_direct_sets_with_par2_damage(job_id, &verification)
            .await
        {
            DirectRepairAnswer::Acted => {
                // The write set this repair actually touched, taken from the
                // verdict that decided the repair was needed — the same
                // reading [`par2_repair_write_set`] gives the conventional
                // selective pass. It is what lets the *next* completion
                // check's post-repair read stay selective too, instead of
                // reading every volume this set describes to answer a
                // question only these few volumes can have a new answer to.
                let write_set = crate::pipeline::completion::finalize::check::par2_repair_write_set(
                    &verification,
                );
                self.direct_post_repair_carry.insert(
                    job_id,
                    DirectPostRepairCarry {
                        recovery_set_id,
                        pre_repair: verification,
                        write_set,
                    },
                );
                DirectPar2Resolution::Repaired
            }
            DirectRepairAnswer::Deferred => DirectPar2Resolution::Deferred,
            DirectRepairAnswer::Declined => DirectPar2Resolution::Unresolved,
        }
    }

    pub(crate) async fn resolve_direct_sets_before_par2_repairer(
        &mut self,
        job_id: JobId,
        par2_set: std::sync::Arc<par2_rs::Par2FileSet>,
        working_dir: PathBuf,
    ) -> DirectPar2Resolution {
        self.resolve_direct_sets_before_par2_repairer_for_set(
            job_id,
            par2_set.recovery_set_id,
            par2_set,
            working_dir,
        )
        .await
    }

    fn take_or_start_direct_post_repair_verification(
        &mut self,
        job_id: JobId,
        par2_set: std::sync::Arc<par2_rs::Par2FileSet>,
        access: std::sync::Arc<super::par2_access::DirectVolumeFileAccess>,
        to_read: Vec<par2_rs::FileId>,
        selective: bool,
    ) -> Option<Result<par2_rs::VerificationResult, String>> {
        let recovery_set_id = par2_set.recovery_set_id;
        if let Some((result_set_id, result)) = self.direct_post_repair_results.remove(&job_id) {
            if result_set_id == recovery_set_id {
                self.direct_post_repair_in_flight.remove(&job_id);
                return Some(result);
            }
            self.direct_post_repair_results
                .insert(job_id, (result_set_id, result));
        }
        if let Some(in_flight) = self.direct_post_repair_in_flight.get(&job_id) {
            // A ticket for a *different* recovery set is not this call's to
            // wait on — the set it was reading has been rebound out from
            // under it (a re-parsed index, a different served set) — and
            // nothing ever clears it on its own: `handle_direct_post_repair_done`
            // only ever discards a mismatched *work id* against a `recovery_set_id`
            // it already agrees with, so a mismatched `recovery_set_id` here
            // means that done message, whenever it lands, will find no taker
            // either. Left alone, this was a permanent park: no new ticket
            // ever starts, so no result ever arrives, so nothing ever re-arms
            // the job. Dropping the stale entry (and any result parked
            // beside it under the old set id) frees the slot for a fresh
            // ticket against the set this call actually cares about; the
            // work id we are about to hand out fences the old task's done
            // message if it lands late.
            if in_flight.recovery_set_id != recovery_set_id {
                warn!(
                    job_id = job_id.0,
                    stale_recovery_set_id = ?in_flight.recovery_set_id,
                    "dropping a direct post-repair ticket parked against a recovery set this \
                     job no longer serves"
                );
                self.direct_post_repair_in_flight.remove(&job_id);
                self.direct_post_repair_results.remove(&job_id);
            } else {
                return None;
            }
        }

        self.next_direct_post_repair_work_id = self.next_direct_post_repair_work_id.wrapping_add(1);
        let work_id = self.next_direct_post_repair_work_id;
        let submitted_at = Instant::now();
        self.direct_post_repair_in_flight.insert(
            job_id,
            DirectPostRepairWork {
                work_id,
                recovery_set_id,
                submitted_at,
            },
        );
        info!(
            job_id = job_id.0,
            work_id,
            files = to_read.len(),
            selective,
            "submitting a direct post-repair verification ticket"
        );

        let pp_pool = self.pp_pool.clone();
        let done_tx = self.direct_post_repair_done_tx.clone();
        tokio::spawn(async move {
            let joined = tokio::task::spawn_blocking(move || {
                pp_pool.install(move || {
                    crate::e2e_failpoint::maybe_delay("direct_store.post_repair_verify");
                    if to_read.is_empty() {
                        return par2_rs::VerificationResult {
                            files: Vec::new(),
                            recovery_blocks_available: par2_set.recovery_block_count(),
                            total_missing_blocks: 0,
                            repairable: par2_rs::verify::Repairability::NotNeeded,
                        };
                    }
                    par2_rs::verify_selected_file_ids_with_options(
                        &par2_set,
                        access.as_ref(),
                        &to_read,
                        &crate::pipeline::completion::finalize::check::selective_pass_verify_options(),
                    )
                })
            })
            .await;
            let result = joined
                .map_err(|error| format!("direct post-repair verification panicked: {error}"));
            let _ = done_tx
                .send(DirectPostRepairWorkDone {
                    job_id,
                    work_id,
                    recovery_set_id,
                    result,
                })
                .await;
        });
        None
    }

    pub(in crate::pipeline) fn handle_direct_post_repair_done(
        &mut self,
        done: DirectPostRepairWorkDone,
    ) {
        let Some(in_flight) = self.direct_post_repair_in_flight.get(&done.job_id) else {
            return;
        };
        if in_flight.work_id != done.work_id || in_flight.recovery_set_id != done.recovery_set_id {
            debug!(
                job_id = done.job_id.0,
                work_id = done.work_id,
                "discarding stale direct post-repair verification"
            );
            return;
        }
        let elapsed = in_flight.submitted_at.elapsed();
        let outcome = match &done.result {
            Ok(verification) if verification.needs_repair() => "damaged",
            Ok(_) => "clean",
            Err(_) => "error",
        };
        info!(
            job_id = done.job_id.0,
            work_id = done.work_id,
            elapsed_ms = elapsed.as_millis() as u64,
            outcome,
            "direct post-repair verification ticket completed"
        );
        crate::runtime::perf_probe::record("direct_store.post_repair_verify", elapsed);
        if !self.jobs.contains_key(&done.job_id) {
            self.direct_post_repair_in_flight.remove(&done.job_id);
            return;
        }
        self.direct_post_repair_results
            .insert(done.job_id, (done.recovery_set_id, done.result));
        self.schedule_job_completion_check(done.job_id);
    }

    /// One verification pass over the job's recovery set, reading every live
    /// direct volume virtually and emitting nothing.
    ///
    /// The verdict is **adjusted before it is returned**, by exactly the two
    /// rules the authoritative pass applies to its own
    /// ([`Pipeline::apply_direct_damage_adjustments`]). Skipping them was not a
    /// small omission: a job with a *finalized* direct set beside a live
    /// damaged one reads every finalized volume as `Missing` here,
    /// `damaged_files_by_set` finds no live owner for them and refuses the
    /// whole attempt with `DamageOutsideDirectSets` — so the live set demotes
    /// for damage that belongs to files the job legitimately finished without,
    /// which is precisely the case repair exists for.
    ///
    /// # Before a repair, and after one
    ///
    /// The same pass runs on both sides of a repair-while-direct, and the two
    /// are not asking the same question.
    ///
    /// *Before*, it is asking whether the set is damaged, and a file the
    /// dual-CRC grid adjudicated in stream is answered from that evidence
    /// rather than read. That is the clean path and it is unchanged.
    ///
    /// *After*, it is asking whether the repair landed — and that question has
    /// to be answered by reading the bytes. Every claim source this pass has is
    /// a statement about what the **wire** delivered: the grid folds per-article
    /// CRCs recorded at the durability seam, and the session is seeded from the
    /// same verdicts. None of them can see a `pwrite` that silently short-wrote,
    /// a bad sector under the envelope, or a repaired span that never reached
    /// the platter. A direct set's source volumes are exactly the files nothing
    /// else ever re-reads, so if this pass stands on wire evidence, a disk fault
    /// under a repaired set ships in a `Completed` job.
    ///
    /// So a post-repair pass takes no *wire* claims — the grid and the
    /// session are both skipped, unconditionally and with no knob to turn
    /// that off. See [`Self::direct_sets_repaired_in_place`] for how the two
    /// are told apart.
    ///
    /// It does not follow that every described file is read, though. When
    /// [`Pipeline::resolve_direct_sets_before_par2_repairer_for_set`] left a
    /// [`DirectPostRepairCarry`] for this recovery set, the files the repair
    /// did not rewrite carry their entry forward from that *disk* read — the
    /// pre-repair pass's own, taken minutes ago in this same flow — rather
    /// than being re-read. That is not wire evidence standing in for a read;
    /// it is the same trust class [`Pipeline::verify_repaired_par2_files_with_placement`]
    /// already extends to a conventional set's untouched files, applied here
    /// for the same reason: the repair could only ever have rewritten the
    /// files its own pre-repair verdict called not-`Complete`, so re-reading
    /// the rest answers a question the disk already answered once this pass.
    /// A carry that is missing or stale for this recovery set gets no such
    /// shortcut; every described file is read, which is this pass's answer
    /// whenever it cannot prove a narrower one is enough.
    ///
    /// The reads themselves go to real files — [`super::provider::VirtualVolumeReader`]
    /// holds an open handle on the envelope and on each member `.direct.partial`
    /// — so "read the bytes" here means the same thing it means for a
    /// conventional file, even though the volume it reconstructs is virtual.
    ///
    /// # Why the post-repair pass may still verify from slice proof
    ///
    /// `fast_verify` is not a sampled read: par2-rs proves an intact candidate
    /// from its per-slice IFSC checksums scanned at read speed and skips only
    /// the inherently serial whole-file MD5, and a file it cannot prove that way
    /// falls through to the strict pipeline with its per-slice accounting fully
    /// intact (par2-rs `verify.rs`, the `fast_verify && let Some(..)` arms). So
    /// every byte is still read and a damaged volume — the only kind whose
    /// accounting a follow-up repair would be sized from — is still measured
    /// slice by slice.
    ///
    /// The pre-repair pass keeps the strict default. Its verdict is what sizes
    /// the repair, and it is not the pass this optimisation was measured for.
    pub(crate) async fn verify_direct_sets_quietly(
        &mut self,
        job_id: JobId,
        par2_set: std::sync::Arc<par2_rs::Par2FileSet>,
        working_dir: PathBuf,
    ) -> Option<par2_rs::VerificationResult> {
        let overlay = self.direct_par2_overlay_for_set(job_id, par2_set.recovery_set_id)?;
        let overlay_set_id = overlay.recovery_set_id;
        let volumes = overlay.volumes.clone();
        let provider = overlay.provider;
        // No placement scan: the direct volumes are absent from the directory
        // by construction and every other file is at its declared name, which
        // is the same assumption the repair's own fallback access makes.
        let plan = par2_rs::PlacementPlan {
            exact: volumes.iter().map(|volume| volume.par2_file_id).collect(),
            swaps: Vec::new(),
            renames: Vec::new(),
            unresolved: Vec::new(),
            conflicts: Vec::new(),
        };
        let inner = par2_rs::PlacementFileAccess::from_plan(working_dir.clone(), &par2_set, &plan);
        let access = std::sync::Arc::new(super::par2_access::DirectVolumeFileAccess::new(
            inner, provider, &volumes,
        ));

        // A repair already ran for one of this job's live sets, so this pass is
        // the read-back that decides whether it landed. Every claim below is
        // wire evidence; see this function's docs for why none of it may stand
        // in on this pass.
        let post_repair = self.direct_sets_repaired_in_place(job_id);

        // The narrower read this pass may take instead: the write set a live
        // carry names, but only when the carry is actually for the recovery
        // set this call is resolving. A mismatch means the carry belongs to a
        // repair against a set this job no longer serves — the set was
        // rebound by a later PAR2 index, say — and using its write set here
        // would silently stand in for files a *different* set's pre-repair
        // pass vouched for. Cloned out from under the borrow up front so the
        // mutable calls below are free to take the carry for real once the
        // read they start actually finishes.
        let selective_write_set: Option<Vec<par2_rs::FileId>> = post_repair
            .then(|| self.direct_post_repair_carry.get(&job_id))
            .flatten()
            .filter(|carry| carry.recovery_set_id == par2_set.recovery_set_id)
            .map(|carry| carry.write_set.clone());

        let session_verification = if post_repair {
            None
        } else {
            self.verify_direct_sets_through_session(
                job_id,
                overlay_set_id,
                &par2_set,
                &working_dir,
                &access,
            )
            .await
        };

        let mut verification = match session_verification {
            Some(verification) => verification,
            None if post_repair && selective_write_set.is_some() => {
                // The selective post-repair read-back: only the volumes the
                // repair rewrote, standing in for everything else with the
                // pre-repair pass's own entries. The direct-store mirror of
                // [`Pipeline::verify_repaired_par2_files_with_placement`] —
                // see this function's docs for why the carry, not the grid or
                // the session, is what a post-repair pass may stand on.
                let to_read = selective_write_set.expect("checked by the match guard");
                #[cfg(test)]
                {
                    self.direct_post_repair_read_splits.push((0, to_read.len()));
                }
                info!(
                    job_id = job_id.0,
                    rewritten = to_read.len(),
                    "post-repair direct-store verification reads only what the repair rewrote"
                );
                let fresh = match self.take_or_start_direct_post_repair_verification(
                    job_id,
                    std::sync::Arc::clone(&par2_set),
                    std::sync::Arc::clone(&access),
                    to_read,
                    true,
                ) {
                    Some(Ok(fresh)) => fresh,
                    Some(Err(error)) => {
                        warn!(
                            job_id = job_id.0,
                            error = %error,
                            "direct post-repair verification failed"
                        );
                        // The carry answered no question this attempt — the
                        // read that was meant to settle it never landed — so
                        // it must not survive to describe a future attempt
                        // against bytes that may have moved again by then.
                        self.direct_post_repair_carry.remove(&job_id);
                        return None;
                    }
                    None => return None,
                };
                // Taken only now that a fresh read actually landed: while the
                // ticket is still in flight, later laps of this same pass
                // need the carry's write set again to resubmit or to notice
                // the ticket is already running, so it stays in the map
                // until there is a result to fold it into.
                let carry = self
                    .direct_post_repair_carry
                    .remove(&job_id)
                    .expect("selective_write_set was read from a live carry moments ago");
                par2_rs::verify::merge_verification_results(&par2_set, &carry.pre_repair, fresh)
            }
            None => {
                // Read and verify through the access adapter. A direct set's
                // source volumes have no files, so the adapter answers every
                // read out of the envelope plus the routed member partials —
                // and for an encrypted set the overlay re-derives the posted
                // cipher on the way out — which is what lets the ordinary pass
                // reach a verdict without materializing a single volume.
                //
                // The grid's claims are honoured **here** too, per file. The
                // session above is all-or-nothing by necessity, and a set with
                // one damaged volume therefore always lands in this arm — where
                // re-reading the volumes the decode pass already proved clean is
                // pure cost. So the files the grid adjudicated are stood in for,
                // and only the rest are read. The bar is the session's own,
                // unchanged: every described slice `Intact` with independent
                // (pCRC-verified) article coverage at exactly the described
                // length, over bytes that were durable before the claim was
                // made. Anything less is not adjudicated and is read.
                //
                // Post-repair, no file is stood in for here either — this arm
                // is reached post-repair only when there is no live carry for
                // this recovery set (a restart, an evicted job, a set that was
                // rebound since the repair ran), and the sibling arm above is
                // what a fresh carry routes to instead. Without one there is
                // nothing to merge a selective read against, so the fallback
                // is the same full, unconditional read this pass has always
                // taken post-repair: every described file, standing in for
                // none of them.
                let claimed = if post_repair {
                    Vec::new()
                } else {
                    self.grid_claimed_file_verifications(job_id, &par2_set)
                };
                let claimed_ids: HashSet<par2_rs::FileId> =
                    claimed.iter().map(|file| file.file_id).collect();
                let to_read: Vec<par2_rs::FileId> = par2_set
                    .recovery_file_ids
                    .iter()
                    .copied()
                    .filter(|file_id| !claimed_ids.contains(file_id))
                    .collect();
                if !claimed.is_empty() {
                    debug!(
                        job_id = job_id.0,
                        claimed_in_stream = claimed.len(),
                        read = to_read.len(),
                        "the direct read-and-verify pass is standing in for volumes the \
                         dual-CRC grid already adjudicated"
                    );
                    crate::runtime::perf_probe::record_value(
                        "direct_store.verify.files_claimed_in_stream",
                        claimed.len() as u64,
                    );
                }
                if !post_repair && !to_read.is_empty() {
                    // Why each read is happening at all: the first failing
                    // rung of the claim ladder, per unclaimed file, folded
                    // into a histogram. A healthy 100%-grid-fed job that
                    // still pays a multi-minute read should say WHY in its
                    // own log line, not leave a silent gap to reconstruct
                    // from timestamps.
                    let to_read_set: HashSet<par2_rs::FileId> = to_read.iter().copied().collect();
                    let mut shortfalls: BTreeMap<&'static str, u32> = BTreeMap::new();
                    let mut bound: HashSet<par2_rs::FileId> = HashSet::new();
                    if let Some(state) = self.jobs.get(&job_id) {
                        let file_ids: Vec<NzbFileId> =
                            state.assembly.files().map(|file| file.file_id()).collect();
                        for file_id in file_ids {
                            let Some(binding) = self.resolve_par2_file_binding_in_set(
                                file_id,
                                par2_set.recovery_set_id,
                            ) else {
                                continue;
                            };
                            if !to_read_set.contains(&binding.par2_file_id) {
                                continue;
                            }
                            bound.insert(binding.par2_file_id);
                            if let Some(reason) =
                                self.in_stream_par2_claim_shortfall(file_id, &par2_set)
                            {
                                *shortfalls.entry(reason).or_default() += 1;
                            }
                        }
                    }
                    let unbound = to_read_set.len().saturating_sub(bound.len());
                    if unbound > 0 {
                        *shortfalls.entry("no_bound_pipeline_file").or_default() += unbound as u32;
                    }
                    info!(
                        job_id = job_id.0,
                        read = to_read.len(),
                        shortfalls = ?shortfalls,
                        "direct verify is reading files the grid could not claim"
                    );
                    for (reason, count) in &shortfalls {
                        crate::runtime::perf_probe::record_value_owned(
                            format!("direct_store.verify.claim_shortfall.{reason}"),
                            u64::from(*count),
                        );
                    }
                }
                #[cfg(test)]
                {
                    self.direct_verify_read_splits
                        .push((claimed.len(), to_read.len()));
                    if post_repair {
                        self.direct_post_repair_read_splits
                            .push((claimed.len(), to_read.len()));
                    }
                }
                if to_read.is_empty() {
                    // Nothing left to read: every described file carries an
                    // in-stream proof. Synthesised in exactly the shape the
                    // completion gate's quick pass synthesises for the same
                    // evidence on the conventional side.
                    par2_rs::VerificationResult {
                        files: claimed,
                        recovery_blocks_available: par2_set.recovery_block_count(),
                        total_missing_blocks: 0,
                        repairable: par2_rs::verify::Repairability::NotNeeded,
                    }
                } else {
                    let mut verification = if post_repair {
                        match self.take_or_start_direct_post_repair_verification(
                            job_id,
                            std::sync::Arc::clone(&par2_set),
                            std::sync::Arc::clone(&access),
                            to_read,
                            false,
                        ) {
                            Some(Ok(verification)) => verification,
                            Some(Err(error)) => {
                                warn!(
                                    job_id = job_id.0,
                                    error = %error,
                                    "direct post-repair verification failed"
                                );
                                return None;
                            }
                            None => return None,
                        }
                    } else {
                        // The grid's per-slice proofs for the very files being
                        // read: a file lands in `to_read` when one slice is
                        // damaged or unverdicted, but every slice the grid DID
                        // prove is attested here, so the pass seeks over the
                        // proven ranges and reads only the slices in
                        // question. Same bar as the whole-file claim above,
                        // applied per slice.
                        let to_read_ids: HashSet<par2_rs::FileId> =
                            to_read.iter().copied().collect();
                        let mut proven_slices: std::collections::HashMap<
                            par2_rs::FileId,
                            Vec<bool>,
                        > = std::collections::HashMap::new();
                        if let Some(state) = self.jobs.get(&job_id) {
                            let file_ids: Vec<NzbFileId> =
                                state.assembly.files().map(|file| file.file_id()).collect();
                            for file_id in file_ids {
                                let Some((par2_file_id, slices)) =
                                    self.in_stream_proven_slices(file_id, &par2_set)
                                else {
                                    continue;
                                };
                                if to_read_ids.contains(&par2_file_id) {
                                    proven_slices.insert(par2_file_id, slices);
                                }
                            }
                        }
                        if !proven_slices.is_empty() {
                            let slices_proven: usize = proven_slices
                                .values()
                                .map(|slices| slices.iter().filter(|proven| **proven).count())
                                .sum();
                            info!(
                                job_id = job_id.0,
                                partially_proven_files = proven_slices.len(),
                                slices_proven,
                                "direct verify reads only the slices the grid could not prove"
                            );
                            crate::runtime::perf_probe::record_value(
                                "direct_store.verify.slices_proven_in_stream",
                                slices_proven as u64,
                            );
                        }
                        let pp_pool = self.pp_pool.clone();
                        let read_set = std::sync::Arc::clone(&par2_set);
                        let access = std::sync::Arc::clone(&access);
                        tokio::task::spawn_blocking(move || {
                            pp_pool.install(move || {
                                let mut options = par2_rs::VerifyOptions::default();
                                options.proven_slices = proven_slices;
                                par2_rs::verify_selected_file_ids_with_options(
                                    &read_set,
                                    access.as_ref(),
                                    &to_read,
                                    &options,
                                )
                            })
                        })
                        .await
                        .ok()?
                    };
                    // Appended, then re-ordered to the recovery set's own file
                    // order so the result is shaped exactly as `verify_all`'s
                    // would have been. `total_missing_blocks` is untouched — a
                    // claimed file contributes no missing block — and
                    // `refresh_repairability` re-reads the assessment over the
                    // combined files, preserving a resource-limited verdict the
                    // read half may have reached.
                    verification.files.extend(claimed);
                    let order: HashMap<par2_rs::FileId, usize> = par2_set
                        .recovery_file_ids
                        .iter()
                        .enumerate()
                        .map(|(position, file_id)| (*file_id, position))
                        .collect();
                    verification.files.sort_by_key(|file| {
                        order.get(&file.file_id).copied().unwrap_or(usize::MAX)
                    });
                    verification.refresh_repairability();
                    verification
                }
            }
        };
        let adjustments = self.apply_direct_damage_adjustments(job_id, &mut verification);
        if adjustments.any() {
            debug!(
                job_id = job_id.0,
                skipped_blocks = adjustments.skipped_blocks,
                retained_suspect_blocks = adjustments.retained_suspect_blocks,
                forgiven_direct_blocks = adjustments.forgiven_direct_blocks,
                "adjusted the quiet direct-set pass before attributing damage"
            );
        }
        #[cfg(test)]
        {
            self.last_direct_verdict = Some(verification.clone());
        }
        Some(verification)
    }

    /// Has a repair-while-direct already run for one of this job's live sets?
    ///
    /// The discriminator between the two passes
    /// [`Self::verify_direct_sets_quietly`] serves. The latch it reads is burned
    /// at a repair's first irreversible step, so it is true from the moment any
    /// byte of a set could have moved — which is exactly when a claim about what
    /// the wire delivered stops being a claim about what is on disk.
    ///
    /// Per job rather than per set. The pass verifies the job's whole recovery
    /// set in one go and its claim sources are job-scoped, so there is no
    /// coherent way to read half of it from evidence and half from disk; one
    /// repaired set makes the whole pass a read-back.
    ///
    /// Demoted sets are skipped: their volumes are real files that the
    /// conventional repairer and its own post-repair pass now own.
    ///
    /// # This is defence in depth, and it is worth having anyway
    ///
    /// The repaired set's grid claims are already retired on a post-repair pass:
    /// the repair drops its affected files before it rewrites a byte, and the
    /// session arm is gated on that evidence. A counterfactual run with both
    /// guards removed still reads every volume back.
    ///
    /// It stays because the emptiness is a *consequence* of a decision made
    /// several hundred lines away, for a different reason — retiring claims over
    /// bytes that moved — and the requirement here is a different statement: a
    /// post-repair pass must read the disk. Deriving a safety property from
    /// another decision's side effect is how it lapses silently when that
    /// decision is refactored. One bool is a cheap price for saying it where it
    /// is meant.
    pub(in crate::pipeline) fn direct_sets_repaired_in_place(&self, job_id: JobId) -> bool {
        self.direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_demoted() && set.repair_attempted())
    }

    /// The `FileVerification` entries the dual-CRC grid can stand in for, in
    /// the shape `par2_rs::verify_all` would have produced by reading them.
    ///
    /// The claim is per description and it is the same claim
    /// [`Pipeline::grid_adjudicated_par2_bindings`] makes for the whole set:
    /// this file bound uniquely to this description, its assembled decoded
    /// length equals the described length, and every described slice closed
    /// `Intact` with independent article coverage. Nothing here is derived from
    /// the *pass*; it is derived from evidence the download seam recorded once
    /// the bytes were durable.
    ///
    /// Empty on ambiguity — two pipeline files claiming one description — so an
    /// unresolvable binding costs the reads it always did rather than producing
    /// a claim from a resolution that cannot be trusted.
    pub(crate) fn grid_claimed_file_verifications(
        &self,
        job_id: JobId,
        par2_set: &par2_rs::Par2FileSet,
    ) -> Vec<par2_rs::verify::FileVerification> {
        let Some(adjudicated) = self.grid_adjudicated_par2_file_ids(job_id, par2_set) else {
            return Vec::new();
        };
        if adjudicated.is_empty() {
            return Vec::new();
        }
        par2_set
            .recovery_file_ids
            .iter()
            .filter(|file_id| adjudicated.contains(file_id))
            .filter_map(|file_id| {
                let description = par2_set.file_description(file_id)?;
                let slice_count = par2_set.slice_count_for_file(description.length) as usize;
                Some(par2_rs::verify::FileVerification {
                    file_id: *file_id,
                    // The description's own name, not a sanitized one: that is
                    // what the read pass puts here, and a consumer that
                    // compares the two must not be able to tell which produced
                    // the entry.
                    filename: description.filename.clone(),
                    status: par2_rs::verify::FileStatus::Complete,
                    valid_slices: vec![true; slice_count],
                    missing_slice_count: 0,
                })
            })
            .collect()
    }

    /// The retained session's verdict for a job's direct sets, or `None` to
    /// fall back to the read-and-verify pass.
    ///
    /// # Why this can refuse
    ///
    /// An access-backed session reads **no** source bytes: `analyze()` skips
    /// the scan entirely, because `base_dir` holds no sources to find. It
    /// reports what its evidence established and nothing more. So it can stand
    /// in for the pass only when the dual-CRC grid already adjudicated every
    /// described slice in stream, which is what
    /// [`Pipeline::grid_adjudicated_par2_bindings`] checks. A slice with no
    /// verdict does not qualify, and one of those is enough to send the whole
    /// job back to `verify_all`, which can actually read a virtual volume.
    ///
    /// Refusing is therefore ordinary, not exceptional — any set the grid could
    /// not fully claim in stream takes the pass, as does every damaged one.
    ///
    /// # What feeds the gate
    ///
    /// The grid is fed for a direct volume by `commit_direct_segment`, in
    /// source-volume coordinates, on the same durability contract the
    /// conventional seam states: the article's destination writes returned
    /// before the claim was recorded. So a clean direct set can satisfy the gate
    /// and take this arm, and a set the grid could only partly claim falls to
    /// the pass below — which stands in for the files it *did* claim and reads
    /// only the rest.
    async fn verify_direct_sets_through_session(
        &mut self,
        job_id: JobId,
        overlay_set_id: par2_rs::RecoverySetId,
        par2_set: &std::sync::Arc<par2_rs::Par2FileSet>,
        working_dir: &std::path::Path,
        access: &std::sync::Arc<super::par2_access::DirectVolumeFileAccess>,
    ) -> Option<par2_rs::VerificationResult> {
        if overlay_set_id != par2_set.recovery_set_id {
            return None;
        }
        if !self.grid_adjudicated_par2_bindings(job_id, par2_set) {
            return None;
        }
        // Blocks the decode pass already adjudicated are what this session
        // reports from: they cost no I/O, and the gate above proved they cover
        // every described slice.
        let set_id = overlay_set_id;
        let in_stream = self.in_stream_slice_evidence_for_set(job_id, set_id);
        if in_stream.is_empty() {
            return None;
        }

        let memory_limit =
            crate::pipeline::completion::finalize::check::configured_par2_repair_memory_limit_bytes(
            );
        let handle: std::sync::Arc<dyn par2_rs::FileAccess + Send + Sync> =
            std::sync::Arc::clone(access) as std::sync::Arc<dyn par2_rs::FileAccess + Send + Sync>;
        let (mut session, _) = match self
            .take_or_open_par2_repair_session(
                job_id,
                set_id,
                working_dir.to_path_buf(),
                memory_limit,
                None,
                Some(handle),
            )
            .await
        {
            Ok(Some(session)) => session,
            Ok(None) => return None,
            Err(error) => {
                warn!(job_id = job_id.0, error = %error, "retained PAR2 session unavailable for the direct pass");
                return None;
            }
        };

        let pp_pool = self.pp_pool.clone();
        let par2_set = std::sync::Arc::clone(par2_set);
        let joined = tokio::task::spawn_blocking(move || {
            let outcome = pp_pool.install(|| {
                // Keyed by FileId, not by path: a direct volume has no path to
                // key on.
                for slice in in_stream {
                    if let Err(error) = session.add_slice_evidence_for_file(slice) {
                        return Err(format!("failed to seed in-stream slice evidence: {error}"));
                    }
                }
                session
                    .analyze()
                    .map_err(|error| format!("direct session analysis failed: {error}"))
            });
            (session, outcome, par2_set)
        })
        .await;

        let (session, outcome, _) = match joined {
            Ok(joined) => joined,
            Err(error) => {
                warn!(job_id = job_id.0, error = %error, "direct PAR2 session task panicked");
                return None;
            }
        };
        self.restore_par2_repair_session(job_id, set_id, session);
        match outcome {
            Ok(outcome) => {
                #[cfg(test)]
                {
                    self.direct_session_pass_calls += 1;
                }
                Some(outcome.verification)
            }
            Err(error) => {
                warn!(job_id = job_id.0, error = %error, "falling back to the direct read-and-verify pass");
                None
            }
        }
    }

    /// Repair-while-direct. [`DirectRepairAnswer::Declined`] means nothing was
    /// repaired and the caller should fall back to demotion;
    /// [`DirectRepairAnswer::Deferred`] means the sets are waiting for recovery
    /// that has been asked for, and the caller must leave them alone.
    async fn repair_direct_sets_with_par2_damage(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) -> DirectRepairAnswer {
        let Some(par2_set) = self.par2_set(job_id).cloned() else {
            return DirectRepairAnswer::Declined;
        };
        let Some(overlay) = self.direct_par2_overlay(job_id) else {
            return DirectRepairAnswer::Declined;
        };
        // The same settle guard the demotion path carries, in the same shape:
        // while articles are in flight a set's outstanding ranges read as
        // holes, and PAR2 cannot tell a hole from corruption. Repairing on that
        // verdict would spend recovery blocks rebuilding bytes that are still
        // on their way. A set whose volumes have all finished downloading is
        // settled whatever the rest of the job is doing, which is what keeps a
        // job with one slow conventional file from blocking its RAR set's
        // repair.
        let payload_settled = !self.job_has_pending_download_pipeline_work(job_id);
        if matches!(
            verification.repairable,
            par2_rs::verify::Repairability::NotNeeded
        ) {
            return DirectRepairAnswer::Declined;
        }

        let by_set = match super::repair::damaged_files_by_set(verification, |file_id| {
            overlay.owner_of(file_id)
        }) {
            Ok(by_set) => by_set,
            Err(failure) => {
                Self::record_direct_repair_failure(job_id, &failure);
                return DirectRepairAnswer::Declined;
            }
        };
        if by_set.is_empty() {
            return DirectRepairAnswer::Declined;
        }

        // The wait, decided **before** the first attempt.
        //
        // `blocks_available` counts recovery slices that have been *merged*, and
        // recovery volumes are only fetched once damage is known — so the first
        // damage verdict of a job's life always reads zero, and any damage at
        // all exceeds zero. Attempting the repair anyway is not free: the
        // attempt burns the set's one-shot latch, deletes its checkpoint row and
        // retires its live-PAR2 state before the planner gets far enough to say
        // it has nothing to repair with. So the set would arrive at its own
        // retry already latched, and demote for a verdict the arriving recovery
        // was about to answer.
        //
        // Deciding here instead costs the set nothing — the same reasoning the
        // over-budget pre-check is built on — and leaves the deferred pass as
        // the set's *first* real attempt, which is what the latch is for.
        if let par2_rs::verify::Repairability::Insufficient { blocks_needed, .. } =
            verification.repairable
        {
            // Waiting is only ever right for a set that could act on the
            // recovery when it arrives. A demoted or finalized set has left,
            // an unsettled one's "damage" may be bytes in flight — promoting
            // recovery to rebuild those would spend the bandwidth the deferred
            // fetch exists to save — and a latched one will refuse the retry
            // with `AlreadyRepaired` however much recovery lands.
            let any_set_could_use_it = by_set.keys().any(|set_index| {
                self.direct_store
                    .set(job_id, *set_index)
                    .is_some_and(|set| {
                        !set.is_demoted()
                            && !set.is_finalized()
                            && (payload_settled || set.all_volumes_complete())
                            && !set.repair_attempted()
                    })
            });
            if blocks_needed > 0
                && any_set_could_use_it
                && self.defer_direct_repair_for_recovery(
                    job_id,
                    blocks_needed,
                    verification.recovery_blocks_available,
                )
            {
                return DirectRepairAnswer::Deferred;
            }
            // Not waiting, and not attempting either: the planner refuses an
            // insufficient verdict outright, so the attempt below could only
            // burn the latch, the checkpoint row and the live-PAR2 state on
            // its way to the same refusal — and the set would then face its
            // retry already latched. Declining from here costs the sets
            // nothing, and the demotion answers exactly as it always did. The
            // wave budget goes with it: it belongs to the wait that just
            // ended, and the next damage verdict starts its own.
            self.direct_store.repair_defer_waves.remove(&job_id);
            let any_live_settled = by_set.keys().any(|set_index| {
                self.direct_store
                    .set(job_id, *set_index)
                    .is_some_and(|set| {
                        !set.is_demoted()
                            && !set.is_finalized()
                            && (payload_settled || set.all_volumes_complete())
                    })
            });
            if any_live_settled {
                Self::record_direct_repair_failure(
                    job_id,
                    &super::repair::DirectRepairFailure::Unrepairable,
                );
                warn!(
                    job_id = job_id.0,
                    failure = %super::repair::DirectRepairFailure::Unrepairable,
                    "repairing a direct set in place was not possible; demoting it"
                );
            }
            return DirectRepairAnswer::Declined;
        }
        // Any wave the job was waiting through has delivered: the verdict no
        // longer reads Insufficient, and the attempt below is the wait's
        // conclusion whichever way it goes.
        self.direct_store.repair_defer_waves.remove(&job_id);

        let mut repaired_any = false;
        for (set_index, files) in by_set {
            if !self
                .direct_store
                .set(job_id, set_index)
                .is_some_and(|set| !set.is_demoted() && !set.is_finalized())
            {
                continue;
            }
            if !payload_settled
                && !self
                    .direct_store
                    .set(job_id, set_index)
                    .is_some_and(DirectSet::all_volumes_complete)
            {
                continue;
            }
            // The bound. A set that has already been repaired and is damaged
            // again is a set the repair did not fix, and running it a second
            // time reaches the same verdict — so it demotes instead, which is
            // what every other refusal here does.
            if self
                .direct_store
                .set(job_id, set_index)
                .is_some_and(DirectSet::repair_attempted)
            {
                Self::record_direct_repair_failure(
                    job_id,
                    &super::repair::DirectRepairFailure::AlreadyRepaired,
                );
                continue;
            }
            match self
                .repair_one_direct_set(job_id, set_index, &par2_set, verification, &overlay, &files)
                .await
            {
                Ok(()) => repaired_any = true,
                Err(failure) => {
                    Self::record_direct_repair_failure(job_id, &failure);
                    warn!(
                        job_id = job_id.0,
                        failure = %failure,
                        "repairing a direct set in place was not possible; demoting it"
                    );
                    // A refusal that got as far as routing has already demoted
                    // the set itself — a destination write failed, a repaired
                    // span found no destination — and a demoted set is a state
                    // change the caller has to act on exactly as a repair is:
                    // its volumes are materializing, so the job's next move is a
                    // fresh pass over them, not another lap of the verdict that
                    // sent it here.
                    let already_demoted = self
                        .direct_store
                        .set(job_id, set_index)
                        .is_some_and(DirectSet::is_demoted);
                    return if repaired_any || already_demoted {
                        DirectRepairAnswer::Acted
                    } else {
                        DirectRepairAnswer::Declined
                    };
                }
            }
        }
        if repaired_any {
            DirectRepairAnswer::Acted
        } else {
            DirectRepairAnswer::Declined
        }
    }

    /// Asks for the recovery the verdict needs and says whether the sets should
    /// wait for it instead of demoting.
    ///
    /// Three questions, in the order that makes each one cheap:
    ///
    /// 1. **Can the recovery set cover this at all?** `blocks_available` is what
    ///    is merged; the NZB's advertised recovery is the ceiling. If the damage
    ///    exceeds even that, no amount of downloading helps, and the demotion
    ///    has to be immediate — the conventional path reaches the same dead end
    ///    with better diagnostics, and delaying it helps nobody.
    /// 2. **Has this job spent its waves?** The budget below.
    /// 3. **Is recovery actually coming?** Either this call promoted some, or a
    ///    previous wave is still on the wire. Neither, and there is nothing to
    ///    wait for: waiting on recovery that cannot arrive is how this branch
    ///    livelocked before, so the exhausted case demotes rather than parks.
    pub(crate) fn defer_direct_repair_for_recovery(
        &mut self,
        job_id: JobId,
        blocks_needed: u32,
        recovery_merged_now: u32,
    ) -> bool {
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return false;
        };
        let total_capacity = self.total_recovery_block_capacity(job_id, set_id);
        if total_capacity < blocks_needed {
            debug!(
                job_id = job_id.0,
                blocks_needed,
                total_capacity,
                "not waiting for recovery on a direct set: the damage exceeds every \
                 recovery block the NZB advertises"
            );
            return false;
        }

        let waves = self
            .direct_store
            .repair_defer_waves
            .get(&job_id)
            .copied()
            .unwrap_or(0);
        // A new wave is only started while the budget lasts. Spent, the sets
        // still see out whatever is already on the wire — that wave was paid
        // for, and throwing it away one article short is the same waste the
        // whole defer exists to avoid — but nothing new is asked for, so the
        // next verdict with a quiet pipeline demotes.
        let promoted = if waves < MAX_DIRECT_REPAIR_DEFER_WAVES {
            self.promote_recovery_targeted(job_id, set_id, blocks_needed)
        } else {
            0
        };
        if promoted == 0 && !self.job_has_promoted_recovery_pipeline_work(job_id, "direct repair") {
            debug!(
                job_id = job_id.0,
                blocks_needed,
                waves,
                "not waiting for recovery on a direct set: none was promoted and none \
                 is still arriving"
            );
            return false;
        }
        if promoted > 0 {
            // Counted only for a genuinely new wave. The gate ticks many times
            // while one wave downloads and each tick re-reaches this point with
            // nothing left to promote; charging those against the budget would
            // spend it on the waiting itself.
            self.direct_store
                .repair_defer_waves
                .insert(job_id, waves + 1);
        }

        crate::runtime::perf_probe::record(
            "direct_store.repair_deferred",
            std::time::Duration::from_nanos(1),
        );
        #[cfg(test)]
        {
            self.direct_store.repair_defers += 1;
        }
        info!(
            job_id = job_id.0,
            blocks_needed,
            recovery_merged_now,
            promoted_blocks = promoted,
            total_capacity,
            wave = if promoted > 0 { waves + 1 } else { waves },
            "a direct set's damage needs recovery that has not been downloaded yet; \
             staying direct while the targeted recovery arrives"
        );
        true
    }

    fn record_direct_repair_failure(job_id: JobId, failure: &super::repair::DirectRepairFailure) {
        crate::runtime::perf_probe::record_owned(
            format!("direct_store.repair_refused.{}", failure.metric()),
            std::time::Duration::from_nanos(1),
        );
        debug!(job_id = job_id.0, failure = %failure, "direct-store repair refused");
    }

    /// One set's repair, from the checkpoint delete to the scratch cleanup.
    async fn repair_one_direct_set(
        &mut self,
        job_id: JobId,
        set_index: usize,
        par2_set: &std::sync::Arc<par2_rs::Par2FileSet>,
        verification: &par2_rs::VerificationResult,
        overlay: &DirectPar2Overlay,
        files: &[par2_rs::FileId],
    ) -> Result<(), super::repair::DirectRepairFailure> {
        let slice_size = par2_set.slice_size;
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Err(super::repair::DirectRepairFailure::DamageOutsideDirectSets);
        };
        let set_name = set.set_name().to_string();
        let holds_budget = set.holds_budget();
        // The set's own volume lengths, in *set* volume space, which is what a
        // provider over this set alone needs — the overlay's own copy is the
        // same numbers under the same key, and it is the only place they live.
        let set_lengths: std::collections::BTreeMap<u32, u64> = overlay
            .lengths
            .iter()
            .find(|(index, _)| *index == set_index)
            .map(|(_, lengths)| lengths.clone())
            .unwrap_or_default();

        // The damaged volumes, in the set's own volume space. `overlay` is keyed
        // by the job's file index, which is what makes one provider answer for
        // every set of a job; the set's plan translates back.
        let mut damaged = Vec::new();
        let mut affected_files = Vec::new();
        for file_id in files {
            let Some(file_index) = overlay.file_index_of(file_id) else {
                return Err(super::repair::DirectRepairFailure::DamageOutsideDirectSets);
            };
            affected_files.push(NzbFileId { job_id, file_index });
            let Some(volume_index) = set.plan().volume_for_file(file_index) else {
                return Err(super::repair::DirectRepairFailure::DamageOutsideDirectSets);
            };
            let Some(file) = verification
                .files
                .iter()
                .find(|file| &file.file_id == file_id)
            else {
                return Err(super::repair::DirectRepairFailure::DamageOutsideDirectSets);
            };
            // The **PAR2-described** length, not the assembly's received bytes.
            // A volume whose damage is a lost article is short by exactly that
            // article, and materializing it at the short length would truncate
            // the very slices the repair is about to write. The description is
            // the authoritative length in the coordinate space every slice
            // offset is defined in, which is what the repair needs and what the
            // conventional path would have restored the file to.
            let Some(len) = par2_set
                .file_description(file_id)
                .map(|description| description.length)
            else {
                return Err(super::repair::DirectRepairFailure::DamageOutsideDirectSets);
            };
            let ranges = super::repair::damaged_ranges(&file.valid_slices, slice_size, len);
            let rewrite =
                super::repair::widen_to_articles(&ranges, &set.segment_extents(volume_index), len);
            damaged.push(super::repair::DamagedDirectVolume {
                volume_index,
                par2_file_id: *file_id,
                len,
                path: set.plan().repair_path(volume_index),
                rewrite,
                reconstruction: VolumeReconstruction {
                    // **The job's file index, not the set's volume index.** The
                    // sweep reads through the hybrid provider, and the provider
                    // is keyed by file index so that one instance can answer for
                    // every set of a job — see `virtual_volumes_for`. The two
                    // coincide only when a set's volumes happen to be NZB files
                    // `0..n-1`, which is true of every fixture (PAR2 is appended
                    // last) and false the moment a `.par2` or `.nfo` leads the
                    // NZB or the job carries a second set: the sweep would then
                    // read *another* volume's bytes, fail its composed CRC32 and
                    // demote the whole set with only a metric to say why. The
                    // scratch `path` stays in set space, because that is what
                    // names the file.
                    //
                    // Nothing on this path reads the index back:
                    // `repair_damaged_volumes` discards `reconstruct_volumes`'
                    // `Ok`, so it survives only inside a
                    // [`ReconstructionFailure`]'s message.
                    volume_index: file_index,
                    path: set.plan().repair_path(volume_index),
                    len,
                    // The materialized copy is a repair target, never a
                    // completed-file claim, so nothing reads the `complete`
                    // flag the sweep derives from this — and the pass only runs
                    // once the payload has settled anyway.
                    assembly_complete: true,
                    // Placed bytes and holds alike: the provider serves both,
                    // and an encrypted member's held edge block is the byte
                    // the composition needs to reach the article boundary.
                    covered: set.volume_coverage_with_holds(volume_index),
                    crcs: set.volume_crc_runs(volume_index),
                    // The raw physical coverage, not the article-whole clip the
                    // demotion sweep takes: PAR2 needs every slice it judged
                    // valid to be in the scratch, and those reach to the placed
                    // frontier, not to the last whole article. An encrypted
                    // member's frontier before a hole is *always* inside the
                    // last article — its final cipher block waits for the block
                    // after it — so refusing that run would demote every
                    // encrypted set the moment it needed a repair.
                    partial_article: super::reconstruct::PartialArticle::CarryThrough,
                },
            });
        }
        if damaged.is_empty() {
            return Err(super::repair::DirectRepairFailure::DamageOutsideDirectSets);
        }
        // Sized **before** anything is materialized, read or deleted, so an
        // over-budget repair costs the set nothing and demotes with a name.
        // Every repaired byte re-enters the router as a hold, so the holds
        // budget is the ceiling it is charged against; reading first and
        // finding out afterwards is what let a three-volume rewrite of a large
        // set peak at gigabytes with nothing bounding it.
        let rewrite_bytes: u64 = damaged
            .iter()
            .flat_map(|volume| volume.rewrite.iter())
            .map(|(start, end)| end.saturating_sub(*start))
            .sum();
        if rewrite_bytes > holds_budget {
            return Err(super::repair::DirectRepairFailure::RewriteOverBudget {
                bytes: rewrite_bytes,
                budget: holds_budget,
            });
        }

        // Step 1: the row goes **before** any byte the row claims changes. The
        // materialization writes only scratch, but the re-route below rewrites
        // member partials and envelopes at offsets the checkpoint's floors
        // cover, and a row that outlived that would let a restart trust floors
        // over bytes that moved underneath them.
        //
        // The repair once-latch is burned in the same statement, because this is
        // the first step that cannot be undone: everything above refuses for
        // free, and everything below leaves the set changed whether or not it
        // ends up repaired.
        let mut persist = DatabaseCoveragePersist::new(self.db.clone());
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            set.note_repair_attempted();
        }
        #[cfg(test)]
        {
            self.direct_store.repair_attempts += 1;
        }
        if let Some(set) = self.direct_store.set_mut(job_id, set_index)
            && let Err(error) = set.delete_checkpoint_row(&mut persist)
        {
            warn!(
                job_id = job_id.0,
                set_name = %set_name,
                error = %error,
                "failed to delete a direct-store checkpoint before repairing; the set \
                 demotes rather than repairing over a row that still claims its bytes"
            );
            return Err(super::repair::DirectRepairFailure::PlanRefused(format!(
                "checkpoint delete failed: {error}"
            )));
        }
        // A repair rewrites only these direct volumes. Retire their byte-owned
        // grid evidence before the first rewrite without discarding another
        // set's untouched claims.
        for file_id in affected_files {
            self.block_crcs.forget_file(file_id);
        }
        // Announced from here rather than from a status transition: the set
        // never enters `JobStatus::Repairing` — that status carries the repair
        // concurrency queue, and this repair holds no slot in it — so the event
        // stream is the only public record that a repair ran. Consumers derive
        // the repair stage from the `RepairStarted`/`RepairComplete` pair, and
        // a job whose history shows a repair it never announced would read as
        // one that was never damaged. Sent at the first irreversible step for
        // the same reason the latch burns here: everything above refuses for
        // free and unannounced, everything below is a repair in progress. A
        // refusal past this point sends no terminal — the demotion hands the
        // job to the conventional repairer, whose own pair records how the
        // repair actually ended.
        let _ = self.event_tx.send(PipelineEvent::RepairStarted { job_id });

        let working_dir = self
            .jobs
            .get(&job_id)
            .map(|state| state.working_dir.clone())
            .unwrap_or_default();
        let provider = super::provider::HybridVolumeProvider::new(
            overlay
                .virtual_volumes_for(&self.direct_store, job_id)
                .unwrap_or_default(),
        );
        // No overrides: the fallback answers only files this set does not own,
        // and each one is at its declared PAR2 name — the placement scan that
        // produced the verification already ran and reported no conflicts, and
        // a rename since then would have invalidated the verdict this repair is
        // planned from.
        let inner_plan = par2_rs::PlacementPlan {
            exact: Vec::new(),
            swaps: Vec::new(),
            renames: Vec::new(),
            unresolved: Vec::new(),
            conflicts: Vec::new(),
        };
        let inner = par2_rs::PlacementFileAccess::from_plan(
            working_dir.clone(),
            par2_set.as_ref(),
            &inner_plan,
        );
        let memory_limit = Some(self.par2_repair_memory_limit_bytes());
        let volumes = overlay.volumes.clone();
        let set_bytes = par2_set.clone();
        let verification = verification.clone();
        let damaged_for_task = damaged.clone();
        let pp_pool = self.pp_pool.clone();
        let sparse = self.direct_store.sparse_marking();
        let outcome = tokio::task::spawn_blocking(move || {
            pp_pool.install(move || {
                super::repair::repair_damaged_volumes(
                    set_bytes.as_ref(),
                    &verification,
                    &provider,
                    inner,
                    &volumes,
                    &damaged_for_task,
                    memory_limit,
                    sparse,
                )
            })
        })
        .await;
        let outcome = match outcome {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(failure)) => return Err(failure),
            Err(error) => {
                for volume in &damaged {
                    let _ = tokio::fs::remove_file(&volume.path).await;
                }
                return Err(super::repair::DirectRepairFailure::ExecuteFailed(format!(
                    "the repair task did not complete: {error}"
                )));
            }
        };

        info!(
            job_id = job_id.0,
            set_name = %set_name,
            volumes = outcome.materialized_volumes,
            recovery_blocks = outcome.recovery_blocks_used,
            rewrite_bytes,
            "repaired a direct set's damaged volumes in place; its clean volumes stayed virtual"
        );
        // "Only the damaged volumes materialize" is the claim repair-while-direct
        // rests on, and the scratch is deleted as soon as its spans are routed —
        // so this counter is the only thing that can contradict it in
        // production. The test build asserts the same number through
        // `repair_materialized_volumes`.
        crate::runtime::perf_probe::record_value(
            "direct_store.repair.materialized_volumes",
            outcome.materialized_volumes as u64,
        );
        crate::runtime::perf_probe::record_value(
            "direct_store.repair.recovery_blocks",
            outcome.recovery_blocks_used as u64,
        );
        #[cfg(test)]
        {
            // Counted from the outcome, so it is volumes the sweep actually
            // rebuilt rather than volumes this seam intended to rebuild: a plan
            // that refuses before reconstruction materializes nothing, and the
            // scratch is deleted either way, so nothing on disk could tell the
            // two apart afterwards.
            self.direct_store.repair_materialized_volumes += outcome.materialized_volumes;
            self.direct_store.repair_recovery_blocks_used += outcome.recovery_blocks_used;
        }

        let routed = self
            .route_repaired_volumes(job_id, set_index, &damaged, &set_lengths)
            .await;
        for path in &outcome.scratch {
            let _ = tokio::fs::remove_file(path).await;
        }
        if !routed {
            return Err(super::repair::DirectRepairFailure::ExecuteFailed(
                "the repaired spans could not be routed back into the set".to_string(),
            ));
        }
        // Every byte of a repaired volume is now accounted for, whatever the
        // assembly thinks: the damage may well have *been* a lost article, and
        // that article is never coming. Saying so is what runs the confirming
        // parse over the repaired image — a volume whose end record was in the
        // lost bytes can only be confirmed here — and what lets the set finalize
        // instead of waiting forever for a download that already finished by
        // another route.
        for volume in &damaged {
            let spans = {
                let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
                    return Err(super::repair::DirectRepairFailure::ExecuteFailed(
                        "the set went away mid-repair".to_string(),
                    ));
                };
                set.note_volume_complete(volume.volume_index, volume.len)
            };
            let spans = match spans {
                Ok(spans) => spans,
                Err(reason) => {
                    return Err(super::repair::DirectRepairFailure::ExecuteFailed(format!(
                        "the repaired volume could not be confirmed: {}",
                        reason.metric()
                    )));
                }
            };
            if !self
                .place_direct_spans(job_id, set_index, None, &spans)
                .await
            {
                return Err(super::repair::DirectRepairFailure::ExecuteFailed(
                    "a confirming parse's spans could not be written".to_string(),
                ));
            }
        }
        self.reread_direct_stale_gaps(job_id, set_index).await;
        // The other half: the row was deleted before anything moved, so the set
        // has no durable coverage at all until a barrier writes one. Demanding
        // it here rather than waiting for the 5 s timer is what keeps the
        // deliberately-lossy window to the length of this call.
        self.run_direct_barrier(
            job_id,
            set_index,
            super::barrier::BarrierTrigger::Demand(BarrierDemand::RepairRecreate),
        )
        .await;
        crate::runtime::perf_probe::record(
            "direct_store.repaired_while_direct",
            std::time::Duration::from_nanos(1),
        );
        self.metrics
            .direct_sets_repaired_while_direct
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Low-frequency: one observation per job-level repair, never on a
        // per-segment path. Records the metric next to the event that already
        // announces the same fact.
        self.metrics.job_lifecycle.note_repair(
            crate::operations::instrumentation::StageOutcomeKind::Complete,
            outcome.recovery_blocks_used as u64,
        );
        let _ = self.event_tx.send(PipelineEvent::RepairComplete {
            job_id,
            slices_repaired: u32::try_from(outcome.recovery_blocks_used).unwrap_or(u32::MAX),
        });
        Ok(())
    }

    /// Reads each repaired volume's spans back and feeds them through the
    /// router and out to every destination they touch (replacement semantics).
    ///
    /// **One volume at a time, read then routed then dropped.** Both halves of
    /// that are load-bearing and they pull in opposite directions:
    ///
    /// - a whole volume at once, because the classification frontier is a
    ///   per-volume fact — a span routed before its volume's end record was
    ///   staged would be held rather than routed, and the repair would refuse;
    /// - never more than one volume, because the spans are bytes, and holding
    ///   every damaged volume's rewrite so the last one could be staged put the
    ///   set's whole repair in RAM twice over with nothing bounding it.
    async fn route_repaired_volumes(
        &mut self,
        job_id: JobId,
        set_index: usize,
        damaged: &[super::repair::DamagedDirectVolume],
        lengths: &std::collections::BTreeMap<u32, u64>,
    ) -> bool {
        // An encrypted member's repaired span decrypts on the way
        // in, and every byte its CBC chain needs was dropped from staging when
        // the original article was routed. Two sources put them back, and
        // neither of them changes a byte: the ones just below the span come off
        // the materialized volume, and the ≤46 in a *neighbouring* volume that
        // complete an edge block of a member extent — and, at the low edge, that
        // block's own CBC predecessor — come off that neighbour's own virtual
        // volume, re-encrypted by the overlay.
        let cipher_lead_in = self
            .direct_store
            .set(job_id, set_index)
            .is_some_and(|set| set.router.routes_encrypted());
        for volume in damaged {
            let volume_index = volume.volume_index;
            let for_task = volume.clone();
            let spans = match tokio::task::spawn_blocking(move || {
                super::repair::read_repaired_spans(&for_task, cipher_lead_in)
            })
            .await
            {
                Ok(Ok(spans)) => spans,
                Ok(Err(failure)) => {
                    warn!(
                        job_id = job_id.0,
                        volume = volume_index,
                        failure = %failure,
                        "a repaired volume could not be read back"
                    );
                    return false;
                }
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        volume = volume_index,
                        error = %error,
                        "the repaired read-back task did not complete"
                    );
                    return false;
                }
            };
            if spans.is_empty() {
                continue;
            }
            // The neighbouring-volume halves of this volume's member edge
            // blocks, read through the overlay so what is staged is what was
            // posted rather than the plaintext on disk. A read that refuses —
            // the neighbour has a hole there — simply contributes nothing, and
            // the span that needed it holds, which `route_repaired` turns into
            // the whole-set demotion the fallback exists for.
            let edges = match cipher_lead_in {
                true => self.read_cipher_edges(job_id, set_index, volume_index, lengths),
                false => Vec::new(),
            };
            let routed = {
                let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
                    return false;
                };
                set.note_repaired_volume_crcs(volume_index, &spans);
                // The chunks are reference-counted, so this hands staging the
                // very buffers the read produced rather than a second copy of
                // them; `spans` drops its side at the end of the iteration.
                let staged: Vec<super::router::RepairedChunk> = spans
                    .iter()
                    .flat_map(|span| span.chunks.iter().cloned())
                    .collect();
                let mut lead_in: Vec<(u32, u64, std::sync::Arc<[u8]>)> = spans
                    .iter()
                    .filter_map(|span| span.lead_in.clone())
                    .map(|(offset, data)| (volume_index, offset, data))
                    .collect();
                lead_in.extend(edges);
                set.route_repaired(volume_index, &staged, &lead_in)
            };
            drop(spans);
            let routed = match routed {
                Ok(routed) => routed,
                Err(reason) => {
                    warn!(
                        job_id = job_id.0,
                        volume = volume_index,
                        reason = reason.metric(),
                        "a repaired span could not be routed back into its direct set"
                    );
                    self.demote_direct_set(job_id, set_index, reason).await;
                    return false;
                }
            };
            if !self
                .place_direct_spans(job_id, set_index, None, &routed)
                .await
            {
                return false;
            }
        }
        true
    }

    /// The few posted bytes per member-extent edge that live in a
    /// **neighbouring** volume of the same set.
    ///
    /// Read through the set's own virtual provider, which re-encrypts them out
    /// of the neighbour's destination — those bytes did not change, so what
    /// comes back is exactly what was posted there. Blocking work, but bounded
    /// at 46 bytes per member extent of one volume — ≤31 below it, which is the
    /// straddling block plus its CBC predecessor, and ≤15 above — so it is done
    /// inline rather than on the pool.
    fn read_cipher_edges(
        &self,
        job_id: JobId,
        set_index: usize,
        volume_index: u32,
        lengths: &std::collections::BTreeMap<u32, u64>,
    ) -> Vec<(u32, u64, std::sync::Arc<[u8]>)> {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Vec::new();
        };
        let reads = set.router.cipher_edge_reads(volume_index);
        if reads.is_empty() {
            return Vec::new();
        }
        let provider = set.virtual_provider(lengths);
        let mut edges = Vec::with_capacity(reads.len());
        for (volume, offset, len) in reads {
            let Some(mut reader) = provider.open(volume) else {
                continue;
            };
            if std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(offset)).is_err() {
                continue;
            }
            let mut bytes = vec![0u8; len as usize];
            if std::io::Read::read_exact(&mut reader, &mut bytes).is_err() {
                continue;
            }
            edges.push((volume, offset, std::sync::Arc::from(bytes.as_slice())));
        }
        edges
    }

    /// Closes the composition gaps a repair's rewrite left, with one bounded
    /// read of the partials that hold them.
    ///
    /// The shape is deliberately the restart re-arm: the same plan, the same
    /// reader, the same "a run that will not read demotes rather than passes"
    /// rule. What differs is only why the value is missing — a rewrite
    /// discarded it rather than a restart losing it — and that difference has
    /// no bearing on what it costs to recover.
    async fn reread_direct_stale_gaps(&mut self, job_id: JobId, set_index: usize) {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        if set.is_demoted() || !set.router.has_stale_gaps() {
            return;
        }
        let destination_dir = set.plan().destination_dir.clone();
        let set_name = set.set_name().to_string();
        let runs = set.router.stale_gap_read_plan();
        if runs.is_empty() {
            return;
        }
        let total: u64 = runs.iter().map(|run| run.len).sum();
        debug!(
            job_id = job_id.0,
            set_name = %set_name,
            runs = runs.len(),
            bytes = total,
            "re-reading the composition gaps a direct-store repair left behind"
        );

        let read_runs = runs.clone();
        let read_dir = destination_dir;
        let checksums =
            tokio::task::spawn_blocking(move || read_restart_seeded_runs(&read_dir, &read_runs))
                .await;
        let checksums = match checksums {
            Ok(Ok(checksums)) => checksums,
            _ => {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    "failed to re-read a repaired member's composition gaps; demoting the set"
                );
                self.demote_direct_set(job_id, set_index, DemotionReason::RepairGapUnreadable)
                    .await;
                return;
            }
        };
        crate::runtime::perf_probe::record_value("direct_store.repair.gap_reread_bytes", total);

        let mut failure = None;
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            for (run, crc) in runs.iter().zip(checksums) {
                if let Err(reason) = set.router.note_restored_member_crc(
                    run.member_id,
                    run.logical_offset,
                    run.len,
                    crc,
                ) {
                    failure = Some(reason);
                    break;
                }
            }
        }
        if let Some(reason) = failure {
            warn!(
                job_id = job_id.0,
                set_name = %set_name,
                reason = reason.metric(),
                "a repaired member failed its gate once its composition gaps were re-read"
            );
            self.demote_direct_set(job_id, set_index, reason).await;
            return;
        }
        // Same terminating condition as the restart re-arm: the pass read every
        // run the plan named, so a gap that survives it is one no plan reached,
        // and re-running would reach the same place. One pass, then a verdict.
        if self
            .direct_store
            .set(job_id, set_index)
            .is_some_and(|set| !set.is_demoted() && set.router.has_stale_gaps())
        {
            warn!(
                job_id = job_id.0,
                set_name = %set_name,
                "a repaired member's composition gaps survived their re-read; demoting the set"
            );
            self.demote_direct_set(job_id, set_index, DemotionReason::RepairGapUnreadable)
                .await;
        }
    }

    /// Demotes every direct set the PAR2 pass found damage on, and reports
    /// whether any did.
    ///
    /// The fallback, and the earlier whole answer. A demoted set materializes
    /// its volumes from its own routed bytes, refetches whatever reconstruction
    /// could not verify, and hands the job to the conventional repair path —
    /// which is exactly the shape the same job would have had with the gate
    /// off.
    pub(crate) async fn demote_direct_sets_with_par2_damage_for_set(
        &mut self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
        verification: &par2_rs::VerificationResult,
    ) -> bool {
        // Scoped to the set this pass is verifying, not to whichever set the
        // gate currently has selected: the damage below is filtered by
        // `recovery_set_id`, so the table its file ids are looked up in has to
        // describe the same set or every lookup misses and nothing demotes.
        let Some(overlay) = self.direct_par2_overlay_for_set(job_id, recovery_set_id) else {
            return false;
        };
        // The second settle guard, paired with
        // [`Self::direct_sets_ready_for_authoritative_par2`]: while articles
        // are still arriving, a set's outstanding ranges read as holes and PAR2
        // calls them damage. The caller is supposed to have deferred already,
        // so this is the belt to that braces — and it is scoped the same way,
        // so a set whose bytes are genuinely never coming still demotes and
        // still gets materialized for the conventional repair path.
        let payload_settled = !self.job_has_pending_download_pipeline_work(job_id);
        let mut damaged: Vec<(usize, String)> = Vec::new();
        for file in &verification.files {
            if matches!(
                file.status,
                par2_rs::verify::FileStatus::Complete | par2_rs::verify::FileStatus::Renamed(_)
            ) {
                continue;
            }
            let Some(set_index) = overlay.sets.get(&file.file_id).copied() else {
                continue;
            };
            if damaged.iter().any(|(index, _)| *index == set_index) {
                continue;
            }
            damaged.push((set_index, file.filename.clone()));
        }
        let mut demoted = false;
        for (set_index, filename) in damaged {
            // Claimed before the log line, so "demoted" means a set really left
            // direct mode. A caller that returned early on a set it did not
            // actually demote would leave the job waiting for a materialization
            // that never happens.
            if !self.direct_store.set(job_id, set_index).is_some_and(|set| {
                self.direct_set_binds_to_par2_set(job_id, set, recovery_set_id)
                    && !set.is_demoted()
                    && !set.is_finalized()
            }) {
                continue;
            }
            if !payload_settled
                && !self
                    .direct_store
                    .set(job_id, set_index)
                    .is_some_and(DirectSet::all_volumes_complete)
            {
                debug!(
                    job_id = job_id.0,
                    volume = %filename,
                    "PAR2 reported damage on a direct set that is still downloading; \
                     leaving it direct until its volumes complete"
                );
                continue;
            }
            warn!(
                job_id = job_id.0,
                volume = %filename,
                "PAR2 verification found damage on a direct set's virtual volume; \
                 demoting so the conventional path can repair a materialized volume"
            );
            self.demote_direct_set(job_id, set_index, DemotionReason::Par2Damaged)
                .await;
            demoted = true;
        }
        demoted
    }

    #[allow(dead_code)]
    pub(crate) async fn demote_direct_sets_with_par2_damage(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) -> bool {
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return false;
        };
        self.demote_direct_sets_with_par2_damage_for_set(job_id, set_id, verification)
            .await
    }

    /// Demotes every set of `job_id` that is still routing, because a PAR2
    /// **repair** is about to run and repair needs a file to write into.
    ///
    /// Returns whether anything demoted, so the caller can let the job go round
    /// again over materialized volumes rather than repairing against nothing.
    pub(crate) async fn demote_live_direct_sets_for_par2_repair_for_set(
        &mut self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
    ) -> bool {
        let live: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| self.direct_set_binds_to_par2_set(job_id, set, recovery_set_id))
            .filter(|(_, set)| !set.is_demoted() && !set.is_finalized())
            .map(|(index, _)| index)
            .collect();
        if live.is_empty() {
            return false;
        }
        for set_index in live {
            self.demote_direct_set(job_id, set_index, DemotionReason::Par2Damaged)
                .await;
        }
        true
    }

    pub(crate) async fn demote_live_direct_sets_for_par2_repair(&mut self, job_id: JobId) -> bool {
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return false;
        };
        self.demote_live_direct_sets_for_par2_repair_for_set(job_id, set_id)
            .await
    }

    /// The routing seam. Replaces the conventional write for one decoded
    /// segment of a direct source volume.
    pub(crate) async fn handle_direct_decode_success(
        &mut self,
        set_index: usize,
        volume_index: u32,
        segment: BufferedDecodedSegment,
        file_offset: u64,
    ) -> DirectRouteOutcome {
        let segment_id = segment.segment_id;
        let file_id = segment_id.file_id;
        let job_id = file_id.job_id;
        let decoded_size = segment.decoded_size;
        let part_crc = segment.part_crc;
        // The dual-CRC grid's half of the article, carried past routing to the
        // commit seam. `contiguous_bytes` gives the router a contiguous view
        // while the original decoded buffer stays owned here for fallback.
        let part_crc_verified = segment.part_crc_verified;
        let bytes = contiguous_bytes(&segment.data);

        let routed = {
            let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
                return DirectRouteOutcome::Conventional(segment);
            };
            set.note_volume_part_crc(volume_index, file_offset, u64::from(decoded_size), part_crc);
            // The decoded geometry of this article, which only the decoder
            // knows: demotion-by-reconstruction uses it to decide which
            // articles it does *not* have to fetch again.
            set.note_segment_extent(
                volume_index,
                segment_id.segment_number,
                file_offset,
                u64::from(decoded_size),
            );
            set.route(volume_index, file_offset, &bytes)
        };
        let spans = match routed {
            Ok(spans) => spans,
            Err(reason) => {
                self.demote_direct_set_with_handoff(job_id, set_index, reason, Some(segment_id))
                    .await;
                return DirectRouteOutcome::Conventional(segment);
            }
        };

        // Before the writes, so a fact can never be newer on disk than in the
        // cache the restart reader rebuilds from. Cheap when nothing parsed: a
        // set parses a volume once provisionally and once confirmingly, so this
        // is two writes per volume for the life of the job.
        self.cache_direct_volume_facts(job_id, set_index).await;

        if !self
            .place_direct_spans(job_id, set_index, Some(segment_id), &spans)
            .await
        {
            return DirectRouteOutcome::Conventional(segment);
        }

        drop(bytes);

        self.commit_direct_segment(
            segment_id,
            decoded_size,
            set_index,
            volume_index,
            file_offset,
            part_crc,
            part_crc_verified,
            &segment.checkpoint_plan,
            &segment.segments,
        )
        .await;
        DirectRouteOutcome::Routed
    }

    /// Writes every destination a batch of routed spans touches, then records
    /// them as coverage. `false` means the set demoted and the caller must stop.
    ///
    /// The record only happens once **all** the writes returned: partial failure
    /// leaves orphan bytes, and the coverage map is the truth, not the bytes.
    /// Both span producers go through here — the routing seam and the confirming
    /// parse's drain at volume completion — so neither can grow its own,
    /// subtly different, ordering.
    async fn place_direct_spans(
        &mut self,
        job_id: JobId,
        set_index: usize,
        handoff: Option<SegmentId>,
        spans: &[RoutedSpan],
    ) -> bool {
        if spans.is_empty() {
            return true;
        }
        let batches = self.direct_write_batches(job_id, set_index, spans);
        if let Err(path) = self.prepare_direct_destinations(job_id, &batches).await {
            // A destination that could not be marked sparse is refused *before*
            // it holds a hole, so nothing has been allocated for it yet. Demote
            // and let the conventional path own the bytes.
            warn!(
                job_id = job_id.0,
                path = %path.display(),
                "could not mark a direct-store destination sparse; demoting the set"
            );
            self.demote_direct_set_with_handoff(
                job_id,
                set_index,
                DemotionReason::SparseMarkFailed,
                handoff,
            )
            .await;
            return false;
        }
        if let Err(error) = crate::pipeline::orchestrator::write_direct_batches(batches).await {
            // A destination write failure is a demotion, not a job failure: the
            // conventional path writes the same bytes to a different file, and
            // only if *that* also fails is the job genuinely unfinishable.
            warn!(
                job_id = job_id.0,
                error = %error,
                "direct-store destination write failed; demoting the set"
            );
            self.demote_direct_set_with_handoff(
                job_id,
                set_index,
                DemotionReason::DestinationWriteFailed,
                handoff,
            )
            .await;
            if !self
                .direct_store
                .set(job_id, set_index)
                .is_some_and(DirectSet::is_demoted)
            {
                self.fail_job(
                    job_id,
                    format!(
                        "direct-store destination write failed for job {}: {error}",
                        job_id.0
                    ),
                );
            }
            return false;
        }
        // Where the bytes went, split by destination kind. Two counters answer
        // the question the disk acceptance target is stated in: how much of
        // a set landed at its final offset versus how much rode the envelope
        // as service data. Summed over the spans already in hand, and
        // `record_value` is a no-op unless the profiler is on.
        let (member_bytes, envelope_bytes) =
            spans.iter().fold((0u64, 0u64), |(member, envelope), span| {
                let len = span.bytes.len() as u64;
                match span.destination {
                    DirectDestination::Member { .. } => (member + len, envelope),
                    DirectDestination::Envelope { .. } => (member, envelope + len),
                }
            });
        if member_bytes > 0 {
            crate::runtime::perf_probe::record_value("direct_store.bytes.member", member_bytes);
        }
        if envelope_bytes > 0 {
            crate::runtime::perf_probe::record_value("direct_store.bytes.envelope", envelope_bytes);
        }
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            set.record_writes(spans, Instant::now());
        }
        true
    }

    /// Caches whatever volume facts the set's parse just accepted, so a restart
    /// can rebuild its layout.
    ///
    /// The rows go into `active_rar_volume_facts` — the same table, keyed the
    /// same way, that the conventional path fills from a parsed volume file.
    /// There is no writer conflict: `try_update_archive_topology` needs a file
    /// to parse and it is suppressed for direct volumes, so for a live direct
    /// set this is the only writer, and after a demotion the conventional path
    /// upserts the same facts over the materialized volumes.
    async fn cache_direct_volume_facts(&mut self, job_id: JobId, set_index: usize) {
        let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
            return;
        };
        let dirty = set.router.take_dirty_facts();
        if dirty.is_empty() {
            return;
        }
        let set_name = set.set_name().to_string();
        for (volume_index, facts) in dirty {
            let encoded = match rmp_serde::to_vec_named(&facts) {
                Ok(encoded) => encoded,
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        volume = volume_index,
                        error = %error,
                        "failed to encode direct-store volume facts"
                    );
                    continue;
                }
            };
            let name = set_name.clone();
            let saved = self
                .db_blocking(move |db| {
                    db.save_rar_volume_facts(job_id, &name, volume_index, &encoded)
                })
                .await;
            if let Err(error) = saved {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    volume = volume_index,
                    error = %error,
                    "failed to cache direct-store volume facts; the set will redownload on restart"
                );
                if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
                    set.router.remark_dirty_fact(volume_index);
                }
            }
        }
    }

    /// The gate re-arm: recomputes the member CRC for every restart-seeded
    /// range with **one sequential read** of the partials that hold them.
    ///
    /// `CrcRuns` does not survive a restart, so the bytes a previous run wrote
    /// are covered and unverified; the whole-member gate refuses to compose over
    /// them until they are re-read. This is the "PAR2 absent" arm — the direct
    /// analogue of `checksum_completed_file`'s fallback for physical files, at
    /// the same cost and the same assurance. It deliberately verifies what is on
    /// **disk now**, so a byte corrupted while the process was down fails the
    /// member gate and demotes the set instead of being committed.
    ///
    /// Runs **once** per set: every run it reads leaves the seeded set, so a
    /// second call finds nothing to do — and if anything is still seeded after
    /// a full pass, the set demotes rather than being re-read on every
    /// completion check for the life of the job.
    async fn rearm_restart_seeded_gates(&mut self, job_id: JobId, set_index: usize) {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        if set.is_demoted() || set.is_finalized() || !set.has_restart_seeded_coverage() {
            return;
        }
        let destination_dir = set.plan().destination_dir.clone();
        let set_name = set.set_name().to_string();
        let runs = set.router.restart_read_plan();
        if runs.is_empty() {
            return;
        }
        let total: u64 = runs.iter().map(|run| run.len).sum();
        info!(
            job_id = job_id.0,
            set_name = %set_name,
            runs = runs.len(),
            bytes = total,
            "re-reading restart-seeded direct-store coverage to re-arm the member gates"
        );

        let read_runs = runs.clone();
        let read_dir = destination_dir;
        let checksums =
            tokio::task::spawn_blocking(move || read_restart_seeded_runs(&read_dir, &read_runs))
                .await;
        let checksums = match checksums {
            Ok(Ok(checksums)) => checksums,
            Ok(Err(error)) => {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    error = %error,
                    "failed to re-read restart-seeded direct-store coverage; demoting the set"
                );
                self.demote_direct_set(job_id, set_index, DemotionReason::RestartRereadFailed)
                    .await;
                return;
            }
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    error = %error,
                    "the restart-seeded re-read task did not complete; demoting the set"
                );
                self.demote_direct_set(job_id, set_index, DemotionReason::RestartRereadFailed)
                    .await;
                return;
            }
        };

        crate::runtime::perf_probe::record_value("direct_store.restart.reread_bytes", total);
        let mut failure = None;
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            for (run, crc) in runs.iter().zip(checksums) {
                if let Err(reason) = set.router.note_restored_member_crc(
                    run.member_id,
                    run.logical_offset,
                    run.len,
                    crc,
                ) {
                    failure = Some(reason);
                    break;
                }
            }
        }
        if let Some(reason) = failure {
            warn!(
                job_id = job_id.0,
                set_name = %set_name,
                reason = reason.metric(),
                "restart-seeded direct-store coverage failed its checksum on re-read"
            );
            self.demote_direct_set(job_id, set_index, reason).await;
            return;
        }

        // The terminating condition. The pass above read every run the plan
        // named, so nothing may still be seeded — a range that survives it is
        // one no plan reached, and re-running the pass would read the same runs
        // and reach the same place. Left alone, `try_verify_member` refuses
        // that member forever while the completion gate calls this back on
        // every check: a zombie that costs I/O. One pass, then a verdict.
        if self
            .direct_store
            .set(job_id, set_index)
            .is_some_and(|set| !set.is_demoted() && set.has_restart_seeded_coverage())
        {
            warn!(
                job_id = job_id.0,
                set_name = %set_name,
                "restart-seeded direct-store coverage survived its re-read pass; demoting the set"
            );
            self.demote_direct_set(job_id, set_index, DemotionReason::RestartRearmUnplaceable)
                .await;
        }
    }

    /// Groups routed spans into one sub-batch per destination path.
    fn direct_write_batches(
        &self,
        job_id: JobId,
        set_index: usize,
        spans: &[RoutedSpan],
    ) -> crate::pipeline::orchestrator::DirectWriteBatches {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Vec::new();
        };
        // Borrowed, never cloned (nit): this runs once per routed batch, and a
        // plan carries two maps sized by the set's volume count — 2 000 of them
        // on the sets this subsystem is sized for.
        let plan = set.plan();
        let partials: HashMap<u32, String> = set
            .router
            .member_partials()
            .into_iter()
            .map(|(member_id, _, partial)| (member_id, partial.to_string()))
            .collect();

        let mut grouped: HashMap<PathBuf, Vec<(u64, Vec<u8>)>> = HashMap::new();
        for span in spans {
            // The two roots part company here, and this is the seam the whole
            // split exists for: member payload is written straight into the
            // job's staging root on the **complete** volume, so the commit
            // rename and completion's publish are both same-filesystem, while
            // an envelope is working data and stays in the intermediate dir.
            let path = match span.destination {
                DirectDestination::Member { member_id } => match partials.get(&member_id) {
                    Some(relative) => plan.destination_path(relative),
                    None => continue,
                },
                // Envelope v2: one file per volume, written at true physical
                // offsets. The owner thread seeks to the offset and writes, so
                // the gaps member routing carried away are ordinary filesystem
                // holes on every platform that gives them for free. Windows
                // needs `FSCTL_SET_SPARSE` at creation to get the same, which a
                // later pass adds.
                DirectDestination::Envelope { volume_index } => plan.envelope_path(volume_index),
            };
            grouped
                .entry(path)
                .or_default()
                .push((span.destination_offset, span.bytes.clone()));
        }
        let mut batches: crate::pipeline::orchestrator::DirectWriteBatches =
            grouped.into_iter().collect();
        batches.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        // Sub-batches are ordered so the owner thread's sequential-write fast
        // path (no seek between adjacent runs) still applies inside a fragment.
        for (_, writes) in &mut batches {
            writes.sort_by_key(|(offset, _)| *offset);
        }
        batches
    }

    /// Creates the parent directory of every destination that needs one, and
    /// creates the destination file itself **marked sparse**, once per job (the
    /// Windows sparse rule).
    ///
    /// A member stored inside a directory — `Silver.Horizon/S01E06.mkv` — names
    /// a partial inside that directory, and the disk owner thread opens
    /// destinations with `create(true)` but never `create_dir_all`, so the
    /// first routed byte would fail with `ENOENT`. The conventional path never
    /// hits this because extraction creates the directory as it writes the
    /// member out; routing writes the member *before* extraction exists.
    ///
    /// The file is created here for the same reason, one step earlier than the
    /// disk owner would: `FSCTL_SET_SPARSE` has to be issued on a handle that
    /// has had nothing written through it, and the owner pool is shared with
    /// every conventional write in the process — it is not the place to teach
    /// about direct-store's sparseness. Creating (and marking) here leaves the
    /// pool's `open_or_reuse` opening a file that already exists and already
    /// carries the attribute, on Windows and everywhere else.
    ///
    /// Records a member name that **direct** finalization produced, in both the
    /// job-wide `extracted_members` (which completion reads) and the runtime's
    /// direct-only mirror (which the claim assertions subtract).
    ///
    /// Recorded under the *destination-relative* name, not the archive's own.
    /// RAR4 stores paths with `\` separators, and the destination is derived
    /// through `resolve_member_path`, which rewrites them to `/`. Recording the
    /// raw name left the two disagreeing for any RAR4 member with a directory
    /// component: completion resolved `work\sample.mkv` against the job's
    /// roots, found nothing on disk, declared the member a stale extracted
    /// record and re-ran conventional extraction — which then failed with "no
    /// on-disk RAR volumes", because direct finalization had deliberately never
    /// written any. A flat RAR4 member has no separator and so never showed it.
    ///
    /// The name is relative to the **staging root**, which is where the commit
    /// rename put the file and where the incremental extractor writes the
    /// members it produces — so completion resolves a direct member and an
    /// extracted one through exactly the same root
    /// (`Pipeline::resolve_job_input_path` tries the working dir and then the
    /// staging dir, and only the second can match a direct member).
    fn record_direct_extracted(&mut self, job_id: JobId, name: String) {
        let name = DirectSetPlan::destination_relative_name(&name).unwrap_or(name);
        self.direct_store
            .direct_extracted_members
            .entry(job_id)
            .or_default()
            .insert(name.clone());
        self.extracted_members
            .entry(job_id)
            .or_default()
            .insert(name);
    }

    /// Member names the **incremental extractor** owns for this job: the
    /// blended `extracted_members` minus everything direct finalization put
    /// there. The claim assertions compare against this, not the blend — a
    /// sibling direct set finalizing the same member name is last-writer-wins
    /// by design, not a second checkpoint system claiming the member.
    fn extraction_claimed_members(&self, job_id: JobId) -> HashSet<String> {
        let mut claimed = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        if let Some(direct) = self.direct_store.direct_extracted_members.get(&job_id) {
            claimed.retain(|name| !direct.contains(name));
        }
        claimed
    }

    /// `Err(path)` names the first destination that could not be marked. The
    /// caller demotes; nothing has a hole yet.
    async fn prepare_direct_destinations(
        &mut self,
        job_id: JobId,
        batches: &crate::pipeline::orchestrator::DirectWriteBatches,
    ) -> Result<(), PathBuf> {
        // The choke point every direct write passes through, and the one place
        // that reliably runs for a **restored** set as well as a freshly
        // admitted one (`install_restored` marks the job examined, so
        // `ensure_direct_sets` returns early for it). Registering the staging
        // root on the job state here is what makes the rest of the pipeline
        // treat direct output like extraction output: `start_move_to_complete`
        // only sweeps a staging dir the state names, and the cancel and fail
        // paths only `remove_dir_all` one the state names. Idempotent and
        // cached after the first call — see `Pipeline::extraction_staging_dir`.
        let _ = self.extraction_staging_dir(job_id);
        let marking = self.direct_store.sparse_marking();
        for (path, _) in batches {
            if self
                .direct_store
                .prepared_destinations
                .get(&job_id)
                .is_some_and(|prepared| prepared.contains(path))
            {
                continue;
            }
            if let Some(parent) = path.parent()
                && let Err(error) = tokio::fs::create_dir_all(parent).await
            {
                warn!(
                    job_id = job_id.0,
                    path = %parent.display(),
                    error = %error,
                    "failed to create a direct-store destination directory"
                );
                // Left unprepared on purpose: the write below fails and demotes,
                // and a later attempt retries the directory rather than trusting
                // a failure it never saw succeed. Not a sparse refusal — the
                // write error path already distinguishes it.
                continue;
            }
            let created = {
                let path = path.clone();
                tokio::task::spawn_blocking(move || {
                    super::sparse::create_sparse(&path, &marking).map(drop)
                })
                .await
            };
            match created {
                Ok(Ok(())) => {}
                Ok(Err(super::sparse::SparseCreateError::Open(error))) => {
                    // An ordinary filesystem failure, and exactly the one the
                    // first routed write would have hit. Left unprepared so the
                    // write path reports it as `destination_write_failed`,
                    // which is what it is.
                    warn!(
                        job_id = job_id.0,
                        path = %path.display(),
                        error = %error,
                        "failed to create a direct-store destination"
                    );
                    continue;
                }
                Ok(Err(error @ super::sparse::SparseCreateError::Mark(_))) => {
                    warn!(
                        job_id = job_id.0,
                        path = %path.display(),
                        error = %error,
                        "a direct-store destination could not be marked sparse"
                    );
                    return Err(path.clone());
                }
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        path = %path.display(),
                        error = %error,
                        "the sparse-marking task did not complete"
                    );
                    return Err(path.clone());
                }
            }
            // Keyed on the destination itself rather than its directory: the
            // marking is per file, and the directory is created on the way to
            // it. One entry per destination, which is `members + volumes` for
            // the life of the job.
            self.direct_store
                .prepared_destinations
                .entry(job_id)
                .or_default()
                .insert(path.clone());
        }
        Ok(())
    }

    /// The suppressed twin of `commit_persisted_segment`.
    // The extra four arguments are the conventional seam's own dual-CRC
    // contract, carried here rather than re-derived: placement, the article's
    // pCRC and whether it was independently verified, and the block-aligned
    // segments the decoder cut. Bundling them would only rename the tuple.
    #[allow(clippy::too_many_arguments)]
    async fn commit_direct_segment(
        &mut self,
        segment_id: SegmentId,
        decoded_size: u32,
        set_index: usize,
        volume_index: u32,
        file_offset: u64,
        part_crc: u32,
        part_crc_verified: bool,
        checkpoint_plan: &weaver_yenc::CheckpointPlan,
        segments: &[weaver_yenc::Segment],
    ) {
        let file_id = segment_id.file_id;
        let job_id = file_id.job_id;

        let commit = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file_mut(file_id) else {
                return;
            };
            match file_asm.commit_segment(segment_id.segment_number, decoded_size) {
                Ok(commit) => (commit.file_complete, commit.was_duplicate),
                Err(error) => {
                    warn!(segment = %segment_id, error = %error, "direct-store assembly commit failed");
                    return;
                }
            }
        };
        let (file_complete, was_duplicate) = commit;
        if was_duplicate {
            // A duplicate must not advance CRC composition, coverage or
            // progress twice. Counted because a run where this is *never* zero
            // is a server or retry problem, and because the counter is what
            // makes "the duplicate did nothing" observable rather than assumed.
            crate::runtime::perf_probe::record(
                "direct_store.article.duplicate",
                std::time::Duration::from_nanos(1),
            );
        }
        if !was_duplicate {
            self.metrics
                .bytes_committed
                .fetch_add(decoded_size as u64, std::sync::atomic::Ordering::Relaxed);
            self.metrics
                .segments_committed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.send_segment_event(|| PipelineEvent::SegmentCommitted { segment_id });
        }

        // The dual-CRC grid, fed in **source-volume space** — the same
        // coordinates the conventional seam uses for the same file, because
        // `file_offset` is an offset into the volume either way. The only
        // difference is where those bytes are durable: a routed article is on
        // disk in member partials and envelopes rather than in a volume file,
        // and `place_direct_spans` awaited every one of those writes before this
        // seam was reached. That is the same ordering contract
        // `commit_persisted_segment` states — a block claimed here describes
        // content a later read (through the virtual-volume access adapter, which
        // reads exactly those partials) would find.
        //
        // Duplicates are fed on purpose, exactly as the conventional seam feeds
        // them: the grid is positional, and a replay that rewrote a range must
        // invalidate the verdicts derived over it whether or not its bytes
        // agreed. Withholding the feed would leave a claim describing content
        // that may no longer be there.
        self.note_block_crc_segments_for_plan(
            file_id,
            checkpoint_plan,
            file_offset,
            u64::from(decoded_size),
            part_crc,
            part_crc_verified,
            was_duplicate,
            segments,
        );

        if !file_complete {
            return;
        }

        let filename = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.file(file_id))
            .map(|file| file.filename().to_string())
            .unwrap_or_default();
        let total_bytes = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.file(file_id))
            .map(|file| file.received_bytes())
            .unwrap_or(0);
        info!(file_id = %file_id, filename = %filename, "direct source volume complete");
        let _ = self.event_tx.send(PipelineEvent::FileComplete {
            file_id,
            filename,
            total_bytes,
        });

        // The volume's length is what makes its short final block closable:
        // until now that block's extent was undecided, so no tiling of it could
        // be trusted to be the whole block.
        //
        // `total_bytes` is the assembly's **decoded** `received_bytes`, never
        // `total_bytes()` — the NZB's declared segment sum is yEnc-*encoded*,
        // around 3% larger on a real post, and a length that overstates the file
        // pushes the final block's boundary past the described extent, where
        // `verdicts_against` refuses to compare it at all. Same value, same
        // reason, as the conventional seam's `note_file_len`.
        self.block_crcs.note_file_len(file_id, total_bytes);

        // The file-complete state a physical volume drops here. Every one of
        // these is keyed by a file that will never exist, so leaving them
        // behind leaks for the life of the job and, worse, leaves
        // `unverified_segments` naming articles a whole-file CRC recovery would
        // try to replace by rewriting a file that is not there.
        let expected_file_crc = self.expected_file_crcs.remove(&file_id);
        self.pending_file_progress.remove(&file_id);
        self.persisted_file_progress.remove(&file_id);
        self.file_hash_reread_required.remove(&file_id);
        self.unverified_segments.remove(&file_id);
        self.file_crc_recoveries.remove(&file_id);
        self.unavailable_promoted_recovery_segments
            .retain(|segment_id| segment_id.file_id != file_id);

        // The yEnc whole-volume gate, composed rather than re-read.
        //
        // A physical volume is checked against its `=yend crc32` trailer when
        // the file completes; the per-article part CRC32s compose into exactly
        // that value, so the gate survives with no file to read. A mismatch
        // demotes: `schedule_file_crc_recovery` is deliberately *not* wired
        // here, because it replaces segments by rewriting a physical file, and
        // the provider is what gives a direct volume one.
        if let Some(expected) = expected_file_crc
            && let Some(composed) = self
                .direct_store
                .set(job_id, set_index)
                .and_then(|set| set.volume_crc(volume_index, total_bytes))
            && composed != expected
        {
            warn!(
                job_id = job_id.0,
                file_id = %file_id,
                expected = format!("{expected:08x}"),
                composed = format!("{composed:08x}"),
                "direct source volume failed its yEnc whole-file CRC32"
            );
            self.demote_direct_set(job_id, set_index, DemotionReason::VolumeCrcMismatch)
                .await;
            return;
        }

        // `total_bytes` — the assembly's decoded `received_bytes`, not the spec's
        // yEnc-encoded segment sizes — travels with the completion so the
        // checkpoint can tell "the download finished" from "every byte of it is
        // durable". Only the conjunction licenses restart to skip the volume's
        // segments; see `snapshot::VolumeFloor::complete`.
        let outcome = self
            .direct_store
            .set_mut(job_id, set_index)
            .map(|set| set.note_volume_complete(volume_index, total_bytes));
        match outcome {
            Some(Err(reason)) => {
                self.demote_direct_set(job_id, set_index, reason).await;
                return;
            }
            // The confirming parse can make the volume's trailing region
            // routable — it was held until the parse proved no further header
            // could appear there — so those spans are written here, before the
            // set is allowed to finalize and delete its envelopes.
            Some(Ok(spans)) => {
                self.cache_direct_volume_facts(job_id, set_index).await;
                if !self
                    .place_direct_spans(job_id, set_index, None, &spans)
                    .await
                {
                    return;
                }
            }
            None => return,
        }

        // The phase-change demand. The set's download phase ends exactly here,
        // at its last volume, and for a par2-bearing job the next thing that
        // happens is a verification wait that can run for the whole PAR2
        // download — which is precisely the window a restart is most likely to
        // land in. Checkpointing at the boundary means that restart resumes a
        // byte-complete set instead of refetching one.
        if self
            .direct_store
            .set(job_id, set_index)
            .is_some_and(|set| set.all_volumes_complete() && !set.is_demoted())
        {
            self.demand_direct_store_barriers(job_id, BarrierDemand::PhaseChange)
                .await;
        }

        // A completed volume's confirming parse is the one place an open
        // identity plan learns its size, and a set that closes over a gap —
        // its missing volume's file already settled — has no later event to
        // judge it. The sweep here is that judgment.
        if self
            .direct_store
            .set(job_id, set_index)
            .is_some_and(|set| set.plan().identity.is_some())
        {
            self.identity_viability_sweep(job_id).await;
        }

        self.finalize_ready_direct_sets(job_id).await;
        self.check_job_completion(job_id).await;
    }

    /// Commits every set of `job_id` whose members have all passed their gates
    /// and whose job is allowed to finalize (see
    /// [`Self::direct_finalization_waits_for_par2`]).
    ///
    /// Called from the routing seam and from the completion gate, because those
    /// are the two moments the answer can change: the last article of the last
    /// volume, and the verification that clears a par2-bearing job.
    pub(crate) async fn finalize_ready_direct_sets(&mut self, job_id: JobId) {
        if self.direct_store.sets_for(job_id).is_empty() {
            return;
        }
        // The gate re-arm, and deliberately **before** the PAR2 wait: the
        // re-read is about the member gates, not about verification, and
        // running it at the download/verify boundary means a par2-bearing set
        // is already gate-passed the moment its job's verification concludes. A
        // set still receiving articles is skipped — its unwritten ranges are
        // holes, not coverage to verify.
        let seeded: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| set.all_volumes_complete() && set.has_restart_seeded_coverage())
            .map(|(index, _)| index)
            .collect();
        for set_index in seeded {
            self.rearm_restart_seeded_gates(job_id, set_index).await;
        }
        if self.direct_finalization_waits_for_par2(job_id) {
            return;
        }
        let ready: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| set.ready_to_finalize())
            .map(|(index, _)| index)
            .collect();
        for set_index in ready {
            self.finalize_direct_set(job_id, set_index).await;
        }
        // The last set of a job finalizing is one of the two moments the answer
        // to "can anything still read a retained image" changes.
        self.release_retained_direct_volumes(job_id).await;
    }

    /// Whether a direct set must keep its envelopes and partials because the
    /// job's PAR2 verification has not concluded.
    ///
    /// Finalization renames the partials to their destinations and deletes the
    /// envelopes, which together *are* the virtual volume image: after it,
    /// nothing can answer a PAR2 read about a source volume, and nothing can
    /// reconstruct one for a demotion either. A par2-bearing set therefore
    /// waits — routed, gated, byte-complete, but uncommitted — until the job is
    /// verified, bypassed, or has no parsed PAR2 set to verify against.
    ///
    /// The release conditions are the completion gate's own, so a job that will
    /// never verify releases rather than waiting for something that is not
    /// coming — which matters because PAR2 is posted last and downloaded last:
    /// at the moment a set's final volume lands there is usually **no parsed
    /// PAR2 set yet**, and "no set" must mean "not yet" while an article can
    /// still arrive, and "never" once the download pipeline has drained.
    fn direct_finalization_waits_for_par2(&self, job_id: JobId) -> bool {
        if !self.job_spec_has_par2_file(job_id) {
            return false;
        }
        if self.par2_bypassed.contains(&job_id) || self.par2_verified.contains(&job_id) {
            return false;
        }
        // The aggregate remains open until every servable recovery set has
        // settled, so any such set must retain direct source bytes for its own
        // pass rather than letting an earlier set commit them away.
        if !self.par2_servable_set_ids(job_id).is_empty() {
            return true;
        }
        self.job_has_pending_download_pipeline_work(job_id)
    }

    /// Polls the automatic barrier triggers for every live set. Called from the
    /// orchestrator's existing periodic seam.
    pub(crate) async fn poll_direct_store_barriers(&mut self) {
        let now = Instant::now();
        for job_id in self.direct_store.active_jobs() {
            // Polled once per orchestrator turn, so the common answer is
            // "nothing due": decide that without allocating.
            let sets = self.direct_store.sets_for(job_id);
            if !sets.iter().any(|set| set.due(now).is_some()) {
                continue;
            }
            let due: Vec<(usize, super::barrier::BarrierTrigger)> = sets
                .iter()
                .enumerate()
                .filter_map(|(index, set)| set.due(now).map(|trigger| (index, trigger)))
                .collect();
            for (set_index, trigger) in due {
                self.run_direct_barrier(job_id, set_index, trigger).await;
            }
        }
    }

    /// Demands a barrier for every live set of every job. Shutdown's entry
    /// point: a demanded barrier is always attempted, however many have just
    /// failed, so the last interval's work is not lost for free.
    pub(crate) async fn demand_direct_store_barriers_for_all_jobs(
        &mut self,
        demand: BarrierDemand,
    ) {
        for job_id in self.direct_store.active_jobs() {
            self.demand_direct_store_barriers(job_id, demand).await;
        }
    }

    /// Demands a barrier for every live set of a job — pause, shutdown, phase
    /// change, demotion and finalization all go through here.
    pub(crate) async fn demand_direct_store_barriers(
        &mut self,
        job_id: JobId,
        demand: BarrierDemand,
    ) {
        let indices: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| !set.is_demoted())
            .map(|(index, _)| index)
            .collect();
        for set_index in indices {
            self.run_direct_barrier(
                job_id,
                set_index,
                super::barrier::BarrierTrigger::Demand(demand),
            )
            .await;
        }
    }

    async fn run_direct_barrier(
        &mut self,
        job_id: JobId,
        set_index: usize,
        trigger: super::barrier::BarrierTrigger,
    ) {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        // Read before the barrier runs, which resets it. Two numbers, because
        // the interesting one is the second: the barrier's 256 MiB trigger is
        // checked per routed batch, so anything above it is the overshoot the
        // barrier bounds to "one decoded write batch" — and an overshoot that
        // starts tracking set size instead is the shape that regression looks
        // like.
        let dirty_bytes = set.dirty_bytes();
        // Relative name and absolute path together, straight from the set. An
        // earlier shape recovered the relative name by stripping the working
        // directory off the absolute path; with member payload under the
        // staging root and envelopes under the working directory there is no
        // single prefix to strip, and a silent `strip_prefix` failure would
        // have dropped exactly the payload destinations from the sync set.
        let touched = set.touched_paths();

        // Every sync is queued to its owner thread before any of them is
        // awaited. Envelope v2 made this set `members + volumes` rather than
        // two, and one `await` per destination serialized that many independent
        // fsyncs on the pipeline task; the barrier's contract only asks that
        // they have all completed before it persists, not that they happened
        // one after another.
        let paths: Vec<PathBuf> = touched.iter().map(|(_, path)| path.clone()).collect();
        let outcomes = crate::pipeline::orchestrator::sync_direct_destinations(paths).await;
        let results: HashMap<String, Result<(), String>> = touched
            .into_iter()
            .zip(outcomes)
            .map(|((relative, _), outcome)| (relative, outcome.map_err(|error| error.to_string())))
            .collect();

        let mut drain = InlineDrain;
        let mut sync = PreSyncedDestinations { results };
        let mut persist = DatabaseCoveragePersist::new(self.db.clone());
        let now = Instant::now();
        let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
            return;
        };
        match set.run_barrier(trigger, now, &mut drain, &mut sync, &mut persist) {
            Some(Ok(report)) => {
                crate::runtime::perf_probe::record_value(
                    "direct_store.barrier.snapshot_bytes",
                    report.snapshot_bytes as u64,
                );
                crate::runtime::perf_probe::record_value(
                    "direct_store.barrier.dirty_bytes",
                    dirty_bytes,
                );
                crate::runtime::perf_probe::record_value(
                    "direct_store.barrier.overshoot_bytes",
                    dirty_bytes.saturating_sub(super::barrier::BARRIER_DIRTY_BYTES),
                );
                crate::runtime::perf_probe::record_value(
                    "direct_store.barrier.synced_destinations",
                    report.synced_destinations as u64,
                );
                debug!(
                    job_id = job_id.0,
                    generation = report.generation,
                    synced = report.synced_destinations,
                    "direct-store coverage barrier committed"
                );
            }
            Some(Err(error)) => {
                warn!(job_id = job_id.0, error = %error, "direct-store coverage barrier failed");
            }
            None => {}
        }
    }

    /// Commits a finished set: every member's partial becomes its destination
    /// through the extractor's own path resolution, and the set is marked
    /// extracted so the `Extracting` phase is pure bookkeeping.
    ///
    /// # The phase looks instant, and that is the documented behaviour
    ///
    /// There is nothing left to extract here — the payload has been at its
    /// destination since the articles arrived — so `Extracting` completes
    /// immediately and may not be visible at all. The settled answer to that:
    /// **document it, add no synthetic delay, and change no GraphQL surface.**
    /// The README carries the user-facing wording; the rule for this function
    /// is that it must not slow down, and must not emit a phase it did not
    /// really run, to make the UI look more familiar. A set that demotes
    /// reports a real extraction phase because it really runs one.
    async fn finalize_direct_set(&mut self, job_id: JobId, set_index: usize) {
        self.run_direct_barrier(
            job_id,
            set_index,
            super::barrier::BarrierTrigger::Demand(BarrierDemand::Finalization),
        )
        .await;

        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        let set_name = set.set_name().to_string();
        // Both are the set's own working files and both are meaningless once the
        // members are committed, but they part company under retention: the
        // envelopes *are* the virtual volume image and can be asked to outlive
        // finalization, while repair scratch is a materialized write target
        // nothing reads afterwards. A repair that ran earlier in this job has
        // already deleted its own.
        let envelopes = set.plan().envelope_paths();
        let repair_scratch = set.plan().repair_paths();
        // `member_partials` is in **archive order** — `(first volume, physical
        // offset)` — and the commit loop below walks it in that order, so two
        // members whose names sanitize to the same destination overwrite each
        // other exactly the way the incremental extractor makes them overwrite
        // each other: last one in the archive wins.
        let unpacked_sizes: HashMap<String, u64> =
            set.router.member_digest_entries().into_iter().collect();
        let members: Vec<(String, u64, PathBuf, PathBuf)> = set
            .router
            .member_partials()
            .into_iter()
            .filter_map(|(_, name, partial)| {
                let destination = set.plan().member_output_path(name).ok()?;
                // Both under the staging root, so the commit below is a
                // same-directory rename — never the cross-device rename the
                // working-dir-relative shape produced on a split-volume
                // install.
                Some((
                    name.to_string(),
                    unpacked_sizes.get(name).copied().unwrap_or(0),
                    set.plan().destination_path(partial),
                    destination,
                ))
            })
            .collect();
        // Extractor-claimed names only: a sibling direct set that finalized the
        // same member *name* is rename-order semantics, not a second checkpoint
        // system owning this member (see `extraction_claimed_members`).
        let extraction_claimed = self.extraction_claimed_members(job_id);
        set.assert_not_extraction_owned(&extraction_claimed);

        // The small-member tolerance, and strictly **before** the commit loop
        // below. The extraction reads the *virtual volumes*, which are the
        // envelopes overlaid with the members' `.direct.partial`s — so every
        // one of those files has to still be where the provider says it is.
        // Running it after the renames pointed the provider's partial map at
        // paths that had just been renamed away, turning every stored member's
        // extent into a hole: it happened to work only while a tolerated
        // member's header walk and decode never read through a stored extent,
        // and its failure mode was a demotion that could no longer reconstruct,
        // i.e. a full redownload.
        //
        // Nothing here needs the commit to have happened: the overwrite refusal
        // compares `plan().member_output_path` against the tolerated
        // destinations, which is derived from the layout and not from the
        // filesystem. It still has to run before the envelopes are deleted.
        match self.extract_tolerated_members(job_id, set_index).await {
            Ok(extracted) => {
                for name in extracted {
                    self.record_direct_extracted(job_id, name);
                }
            }
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    error = %error,
                    "failed to extract a tolerated member from the virtual volumes; demoting the set"
                );
                self.demote_direct_set(
                    job_id,
                    set_index,
                    DemotionReason::ToleratedExtractionFailed,
                )
                .await;
                return;
            }
        }

        // A failure here leaves the set neither committed nor abandoned: its
        // partials still hold every verified byte, but nothing downstream will
        // ever look at them again, so the job would sit in `Extracting`
        // forever. Demote instead — the volumes are refetched and the ordinary
        // extractor produces the same member (nit).
        //
        // A failure **part way through the loop** leaves the members before it
        // already renamed to their destinations, and the demotion then deletes
        // the partials of the ones after it and refetches every volume of the
        // set. The already-committed members are overwritten by the extractor
        // with byte-identical content, so the outcome is correct and the cost is
        // one wasted extraction of the members that had already landed.
        // Reviewed and accepted: unwinding the renames would mean moving
        // finished output back into scratch paths on a path that is already the
        // unhappy one, and the alternative — staging every rename and
        // committing them together — needs a directory-level atomic swap the
        // filesystem does not offer.
        for (name, unpacked_size, partial, destination) in &members {
            crate::pipeline::release_cached_write_handle(partial);
            if let Some(parent) = destination.parent()
                && let Err(error) = tokio::fs::create_dir_all(parent).await
            {
                warn!(job_id = job_id.0, error = %error, "failed to create direct-store destination directory; demoting the set");
                self.demote_direct_set(job_id, set_index, DemotionReason::FinalizationFailed)
                    .await;
                return;
            }
            // A zero-length stored member never had a byte routed for it, so it
            // has no partial to rename — but the archive declares the file and
            // the conventional extractor creates it, so finalization does too,
            // in the same archive order as every other member.
            let committed = match tokio::fs::rename(partial, destination).await {
                Err(error)
                    if *unpacked_size == 0 && error.kind() == std::io::ErrorKind::NotFound =>
                {
                    tokio::fs::File::create(destination).await.map(drop)
                }
                other => other,
            };
            if let Err(error) = committed {
                warn!(
                    job_id = job_id.0,
                    member = %name,
                    error = %error,
                    "failed to commit a direct-store member to its destination; demoting the set"
                );
                self.demote_direct_set(job_id, set_index, DemotionReason::FinalizationFailed)
                    .await;
                return;
            }
            self.record_direct_extracted(job_id, name.clone());
        }

        for scratch in &repair_scratch {
            crate::pipeline::release_cached_write_handle(scratch);
            let _ = tokio::fs::remove_file(scratch).await;
        }
        // The set's members are at their destinations now, which is the earliest
        // moment the retained image can point at them and the last moment its
        // coverage is still readable — `retire` below resets the controller.
        if self.retain_finalized_direct_volumes(job_id, set_index) {
            info!(
                job_id = job_id.0,
                set_name = %set_name,
                "keeping a finalized direct set's envelopes so a live neighbour's PAR2 repair \
                 can still read its source volumes"
            );
        } else {
            for envelope in &envelopes {
                crate::pipeline::release_cached_write_handle(envelope);
                let _ = tokio::fs::remove_file(envelope).await;
            }
        }
        // The scratch dies with the set, and its high-water is reported
        // separately from RAM so the disk claim stays legible against the 1.05×
        // acceptance target.
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            let scratch_bytes = set.router.scratch_bytes();
            if scratch_bytes > 0 {
                crate::runtime::perf_probe::record_value(
                    "direct_store.holds.scratch_bytes",
                    scratch_bytes,
                );
            }
            set.router.discard_scratch();
        }

        let mut persist = DatabaseCoveragePersist::new(self.db.clone());
        if let Some(set) = self.direct_store.set_mut(job_id, set_index)
            && let Err(error) = set.retire(&mut persist)
        {
            warn!(job_id = job_id.0, error = %error, "failed to retire a direct-store checkpoint");
        }
        // The other end of the direct phase, and the same rule the demotion
        // applies: the commit above renamed the member partials to their
        // destinations and (unless a live neighbour still needs to read them)
        // deleted the envelopes, so the virtual volume image the grid's claims
        // describe has been taken apart. A claim that outlived it would say a
        // *file* is intact when nothing can be read to check, and a later pass
        // would offer its slices to `plan_repair` as input it cannot open.
        // Retiring it costs at most the read a finalized volume always cost.
        let finalized_volume_files: Vec<NzbFileId> = self
            .direct_store
            .set(job_id, set_index)
            .map(|set| {
                set.plan()
                    .volumes
                    .values()
                    .map(|file_index| NzbFileId {
                        job_id,
                        file_index: *file_index,
                    })
                    .collect()
            })
            .unwrap_or_default();
        for file_id in finalized_volume_files {
            self.block_crcs.forget_file(file_id);
        }
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            set.mark_finalized();
        }
        #[cfg(test)]
        {
            // Sticky, because the status is not observable after the fact: a set
            // can finalize, let its job complete and have its whole runtime
            // pruned inside a single completion check, so a test sampling the
            // set list between calls sees `Routing` and then nothing at all.
            self.direct_store.finalized_sets += 1;
        }
        self.extracted_archives
            .entry(job_id)
            .or_default()
            .insert(set_name.clone());
        crate::runtime::perf_probe::record(
            "direct_store.set.finalized",
            std::time::Duration::from_nanos(1),
        );
        self.metrics
            .direct_sets_finalized_direct
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        info!(
            job_id = job_id.0,
            set_name = %set_name,
            members = members.len(),
            "direct-store set finalized without materializing a volume"
        );
    }

    /// Keeps a finalizing set's virtual volume image alive past its own commit,
    /// when the job's PAR2 story can still need it.
    ///
    /// # The gap this closes
    ///
    /// Finalization renames a set's member partials to their destinations and
    /// deletes its per-volume envelopes, so nothing can serve its source volumes
    /// afterwards. If the job's recovery set also covers a **second** direct set
    /// that is later found damaged, Reed–Solomon needs the surviving input
    /// slices from *every* file it describes — the finalized set's volumes
    /// included — and `execute_repair` fails on the ones it cannot open. The two
    /// halves were mutually exclusive: with the finalized set's volumes absent
    /// the neighbour could not repair, and materializing them under their own
    /// names is the whole thing direct-store exists not to do.
    ///
    /// Retaining is the narrow answer. The bytes are already on disk twice over
    /// — the envelope holds every non-member byte at its true physical offset,
    /// and the member bytes are byte-identical at their destinations, because a
    /// commit is a rename — so the image needs no reconstruction, only a pointer
    /// swap and a stay of execution for the envelopes.
    ///
    /// # What is *not* retained
    ///
    /// - a job with no PAR2 file: there is no repair to serve;
    /// - a set with no **live** neighbour: nothing left in this job can ask, and
    ///   the release sweep below deletes what the last one held;
    /// - an **encrypted** set that cannot reproduce its posted bytes. One that
    ///   can is retained like any other: a commit is a rename, so
    ///   the overlay re-encrypts out of the committed member exactly as it did
    ///   out of the partial;
    /// - an image with a hole in it — see
    ///   [`DirectSet::retain_finalized_volumes`].
    ///
    /// A set that retains nothing keeps today's behaviour exactly, and
    /// `forgive_finalized_direct_volumes` keeps excusing its absent volumes.
    fn retain_finalized_direct_volumes(&mut self, job_id: JobId, set_index: usize) -> bool {
        if !self.job_spec_has_par2_file(job_id) {
            return false;
        }
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return false;
        };
        if set.router.posted_bytes_unavailable() {
            return false;
        }
        // A neighbour that can still reach the repair path. Demoted is terminal
        // for this purpose too: a demoted set's volumes go back on disk and its
        // repair is the filesystem-bound `Par2Repairer`'s, which reads no
        // overlay at all.
        let has_live_neighbour =
            self.direct_store
                .sets_for(job_id)
                .iter()
                .enumerate()
                .any(|(index, other)| {
                    index != set_index && !other.is_finalized() && !other.is_demoted()
                });
        if !has_live_neighbour {
            return false;
        }

        // The same lengths `direct_par2_overlay` would have derived, captured
        // here because the assembly is the only place a virtual volume's length
        // lives and a retained image has to stop depending on it.
        let mut lengths = std::collections::BTreeMap::new();
        for (volume_index, file_index) in &set.plan().volumes {
            let file_id = NzbFileId {
                job_id,
                file_index: *file_index,
            };
            let received = self
                .jobs
                .get(&job_id)
                .and_then(|state| state.assembly.file(file_id))
                .map(|file| file.received_bytes())
                .unwrap_or(0);
            lengths.insert(
                *volume_index,
                set.virtual_volume_len(*volume_index, received),
            );
        }
        let retained = self
            .direct_store
            .set_mut(job_id, set_index)
            .is_some_and(|set| set.retain_finalized_volumes(&lengths));
        crate::runtime::perf_probe::record_owned(
            format!(
                "direct_store.finalized_retained.{}",
                if retained { "kept" } else { "refused" }
            ),
            std::time::Duration::from_nanos(1),
        );
        retained
    }

    /// Deletes what [`Self::retain_finalized_direct_volumes`] kept, once the job
    /// has no live direct set left to ask for it.
    ///
    /// The window is deliberately the job's *direct* sets rather than the job:
    /// the only reader of a retained image is the repair behind
    /// [`Self::resolve_direct_sets_before_par2_repairer`], which runs for live
    /// sets only, so the moment the last one finalizes or demotes there is
    /// nothing left that can read one. Called from the two seams that can change
    /// that answer — a set finalizing and a set demoting — so the deferral costs
    /// one directory of envelopes for one job for the span between them and not
    /// a byte longer.
    ///
    /// Anything a crash leaves behind is swept at restart: a finalized set
    /// retired its checkpoint row, so restore rebuilds it fresh, claims none of
    /// its envelopes and `sweep_orphan_direct_files` deletes every one of them.
    /// Nothing about retention is persisted, and nothing needs to be.
    async fn release_retained_direct_volumes(&mut self, job_id: JobId) {
        let sets = self.direct_store.sets_for(job_id);
        if sets.iter().all(|set| set.retained_volumes().is_none()) {
            return;
        }
        if sets
            .iter()
            .any(|set| !set.is_finalized() && !set.is_demoted())
        {
            return;
        }
        let retained: Vec<usize> = sets
            .iter()
            .enumerate()
            .filter(|(_, set)| set.retained_volumes().is_some())
            .map(|(index, _)| index)
            .collect();
        for set_index in retained {
            let Some(set) = self.direct_store.set(job_id, set_index) else {
                continue;
            };
            let set_name = set.set_name().to_string();
            let envelopes = set.plan().envelope_paths();
            for envelope in &envelopes {
                crate::pipeline::release_cached_write_handle(envelope);
                let _ = tokio::fs::remove_file(envelope).await;
            }
            if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
                set.release_retained_volumes();
            }
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                "released a finalized direct set's retained envelopes"
            );
        }
    }

    /// The bounded small-member tolerance: extracts **only** the tolerated
    /// member indices, through the hybrid virtual-volume provider, straight to
    /// their destinations.
    ///
    /// Returns the raw member names that were produced, for
    /// `extracted_members`. The distinction that separates this from the
    /// out-of-scope per-member physical fallback is that it extracts a strict
    /// *subset*: a direct-routed `Store` member is never re-extracted and never
    /// overwritten here, which is checked rather than assumed — a tolerated
    /// member resolving onto a stored member's destination is refused, and the
    /// set demotes.
    async fn extract_tolerated_members(
        &mut self,
        job_id: JobId,
        set_index: usize,
    ) -> Result<Vec<String>, String> {
        let (set_name, names) = {
            let Some(set) = self.direct_store.set(job_id, set_index) else {
                return Ok(Vec::new());
            };
            (
                set.set_name().to_string(),
                set.router.tolerated_member_names(),
            )
        };
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let staging = self.extraction_staging_dir(job_id);
        let extraction_budget = self.extraction_budget(job_id, &staging)?;
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Ok(Vec::new());
        };
        // An `-hp` set's virtual volumes are as header-encrypted as
        // the posted ones, so this extraction cannot even *open* the archive
        // without the key the router proved — and for a `-p` set a tolerated
        // member's data is encrypted too. `None` for a plaintext set, which is
        // every set that reached here before encryption existed.
        let password = set.router.archive_password().map(str::to_string);

        // The ordering invariant this extraction depends on, stated where it is
        // depended on. The provider serves every stored member's extent out of
        // its `.direct.partial`; the commit loop renames those away and records
        // the member in `extracted_members` as it goes. Running after it
        // therefore hands the header walk and the decode a volume whose stored
        // extents are all holes, and the failure path costs a full redownload.
        debug_assert!(
            !self
                .extraction_claimed_members(job_id)
                .iter()
                .any(|committed| {
                    set.router
                        .member_partials()
                        .iter()
                        .any(|(_, name, _)| committed == *name)
                }),
            "a stored member of {set_name} was committed before its set's tolerated \
             members were extracted; the virtual volumes no longer resolve"
        );

        // Every stored member's *eventual* destination, so the assertion below
        // compares resolved paths rather than raw header names — two names can
        // sanitize onto one path, which is exactly the collision that would let
        // a tolerated member overwrite verified direct output. Derived from the
        // layout, not from the filesystem, which is what lets this run before
        // the commit loop renames anything.
        let mut stored_outputs: HashSet<PathBuf> = HashSet::new();
        for (_, name, _) in set.router.member_partials() {
            if let Ok(destination) = set.plan().member_output_path(name) {
                stored_outputs.insert(destination);
            }
        }

        let mut targets: Vec<(String, PathBuf)> = Vec::with_capacity(names.len());
        for name in &names {
            let destination = set
                .plan()
                .member_output_path(name)
                .map_err(|()| format!("tolerated member '{name}' has no safe destination"))?;
            if stored_outputs.contains(&destination) {
                return Err(format!(
                    "tolerated member '{name}' resolves onto a direct-store output at {}",
                    destination.display()
                ));
            }
            targets.push((name.clone(), destination));
        }
        debug_assert!(
            targets
                .iter()
                .all(|(_, destination)| !stored_outputs.contains(destination)),
            "a tolerated member of {set_name} would overwrite a direct-store output"
        );

        // The volumes' decoded lengths, which only the download layer knows: a
        // virtual volume has no file whose length could be read instead.
        let mut lengths = std::collections::BTreeMap::new();
        for (volume_index, file_index) in &set.plan().volumes {
            let file_id = NzbFileId {
                job_id,
                file_index: *file_index,
            };
            let received = self
                .jobs
                .get(&job_id)
                .and_then(|state| state.assembly.file(file_id))
                .map(|file| file.received_bytes())
                .unwrap_or(0);
            lengths.insert(
                *volume_index,
                set.virtual_volume_len(*volume_index, received),
            );
        }
        let first_volume = *lengths
            .keys()
            .next()
            .ok_or_else(|| format!("direct set '{set_name}' has no volumes to extract from"))?;
        let provider = set.virtual_provider(&lengths);
        let other_volumes: Vec<u32> = lengths
            .keys()
            .copied()
            .filter(|volume_index| *volume_index != first_volume)
            .collect();
        let extraction_memory_limit = self.extraction_limits.max_memory_bytes;

        let extracted = tokio::task::spawn_blocking(move || {
            let reader = provider
                .open(first_volume)
                .ok_or_else(|| format!("virtual volume {first_volume} is not registered"))?;
            let mut archive = match password.as_deref() {
                Some(password) => unrar_rs::RarArchive::open_with_password(reader, password),
                None => unrar_rs::RarArchive::open(reader),
            }
            .map_err(|error| format!("failed to open the virtual archive: {error}"))?;
            // The same decode ceilings the incremental extractor applies. A
            // tolerated member is small by budget, but the *declared* dictionary
            // in a hostile header is not bounded by anything the budget checks.
            let max_dict_bytes =
                crate::pipeline::extraction::apply_server_rar_limits_with_memory_limit(
                    &mut archive,
                    extraction_memory_limit,
                );
            for volume_index in other_volumes {
                let Some(reader) = provider.open(volume_index) else {
                    continue;
                };
                archive
                    .add_volume(volume_index as usize, Box::new(reader))
                    .map_err(|error| {
                        format!("failed to add virtual volume {volume_index}: {error}")
                    })?;
            }
            crate::pipeline::extraction::ensure_rar_dictionary_within_limit(
                &archive,
                max_dict_bytes,
            )
            .map_err(|error| format!("RAR dictionary admission failed: {error}"))?;
            let _memory_permit = extraction_budget
                .reserve_memory_wait(crate::pipeline::extraction::rar_decoder_memory_bytes(
                    &archive,
                ))
                .map_err(|error| format!("RAR decoder memory admission failed: {error}"))?;
            for (_, destination) in &targets {
                if let Some(parent) = destination.parent() {
                    std::fs::create_dir_all(parent).map_err(|error| {
                        format!(
                            "failed to create {} for a tolerated member: {error}",
                            parent.display()
                        )
                    })?;
                }
            }
            let options = unrar_rs::ExtractOptions {
                verify: true,
                password: password.clone(),
                restore_owners: false,
            };
            let mut produced = Vec::with_capacity(targets.len());
            for (name, destination) in &targets {
                let index = archive
                    .find_member(name)
                    .ok_or_else(|| format!("tolerated member '{name}' is not in the archive"))?;
                let mut file = std::fs::File::create(destination).map_err(|error| {
                    format!("failed to create {}: {error}", destination.display())
                })?;
                // The provider is the set's, keyed by the set's own volume
                // indices, which is what `extract_member_streaming` asks for —
                // a member starting in volume 3 requests volume 3.
                archive
                    .extract_member_streaming(index, &options, &provider, &mut file)
                    .map_err(|error| format!("failed to extract '{name}': {error}"))?;
                // The tolerated half of the byte account: everything else a
                // direct set produces is counted at the router as
                // `direct_store.bytes.member`, and a set whose tolerated bytes
                // start rivalling its stored ones is one the tolerance budget
                // is no longer holding.
                if let Ok(metadata) = file.metadata() {
                    crate::runtime::perf_probe::record_value(
                        "direct_store.bytes.tolerated",
                        metadata.len(),
                    );
                }
                produced.push(name.clone());
            }
            Ok::<Vec<String>, String>(produced)
        })
        .await
        .map_err(|error| format!("tolerated extraction task panicked: {error}"))??;

        crate::runtime::perf_probe::record(
            "direct_store.tolerated_members_extracted",
            std::time::Duration::from_nanos(1),
        );
        info!(
            job_id = job_id.0,
            set_name = %set_name,
            members = extracted.len(),
            "extracted tolerated small members from the virtual volumes"
        );
        Ok(extracted)
    }

    /// Abandons direct output for a set and hands its volumes back (the
    /// **archive-group demotion**, the transition that ends direct mode).
    ///
    /// Two shapes, and the first one is tried first:
    ///
    /// 1. **Reconstruction.** Every volume is rebuilt byte-exactly from the
    ///    envelope plus the member extents, its covered runs are verified against
    ///    the yEnc part-CRC composition, and only then are legacy floors and
    ///    completed-file rows persisted, the coverage row retired, and the
    ///    partials and envelopes deleted. Covered bytes are never refetched.
    /// 2. **Refetch** — the conservative form, kept as the fallback for
    ///    everything reconstruction cannot do: a deleted envelope, a truncated
    ///    partial, a covered run whose CRC32 disagrees. The routed bytes are
    ///    thrown away and every article comes back off the wire.
    ///
    /// Ordering is normative. Unlike *repair* over checkpoint-covered output —
    /// which deletes the checkpoint row **first**, because it is about to
    /// overwrite the very bytes the row claims — demotion retires the row as
    /// part of reconciliation, after the legacy state that replaces it is
    /// durable. Retiring first would leave a window where neither the direct
    /// coverage nor the legacy floors describe what is on disk.
    pub(in crate::pipeline) async fn demote_direct_set(
        &mut self,
        job_id: JobId,
        set_index: usize,
        reason: DemotionReason,
    ) {
        self.demote_direct_set_with_handoff(job_id, set_index, reason, None)
            .await;
    }

    async fn demote_direct_set_with_handoff(
        &mut self,
        job_id: JobId,
        set_index: usize,
        reason: DemotionReason,
        handoff: Option<SegmentId>,
    ) {
        let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
            return;
        };
        // One cleanup per set, and never for a finalized one. The original
        // guard read `is_demoted() && is_finalized()`, which two mutually
        // exclusive states can never both satisfy, so a finalized set could be
        // flipped to `Demoted` and have its committed members deleted out from
        // under a job that had already counted them.
        if !set.claim_demotion(reason) {
            if let Some(segment_id) = handoff {
                self.direct_store
                    .note_materialization_handoff(set_index, segment_id);
            }
            return;
        }
        let set_name = set.set_name().to_string();
        // A demoted set's volumes become real files and hand off to the
        // conventional repairer, which brings its own post-repair pass — so
        // any post-repair state this job is carrying for the direct gate is
        // no longer this set's business, and must not outlive the demotion
        // to describe bytes a different repair path now owns. Cleared for the
        // whole job rather than filtered to this set: the carry and the
        // ticket bookkeeping are job-scoped (a job serves one recovery set
        // through this gate at a time), so a demotion of any of its sets
        // invalidates whatever the gate was mid-resolving.
        self.direct_post_repair_carry.remove(&job_id);
        self.direct_post_repair_in_flight.remove(&job_id);
        self.direct_post_repair_results.remove(&job_id);
        if reason == DemotionReason::HoldsScratchCeiling {
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                scratch_bytes = set.router.scratch_bytes(),
                "direct-store demoting after a holds scratch cap event"
            );
        }
        // Every volume of this set is about to become a real file: either
        // reconstruction writes it from the routed bytes, or the refetch pulls
        // it back off the wire article by article — and in both cases the
        // conventional seam owns the feeds from here.
        //
        // The direct phase's grid state has to go *before* that can happen. It
        // describes a virtual volume assembled out of member partials and
        // envelopes, and the file the conventional path is about to fill is a
        // different image of the same coordinates: reconstruction may write a
        // shorter prefix than the direct phase claimed, and a refetch rewrites
        // ranges wholesale. Leaving it would let a block closed in one image
        // adjudicate bytes of the other — and worse, survive into
        // `in_stream_verified_par2_match`, whose whole job is to say a file need
        // not be read. Per file rather than per job: a job's other sets, and its
        // conventional files, are untouched by this demotion.
        let demoted_volume_files: Vec<NzbFileId> = set
            .plan()
            .volumes
            .values()
            .map(|file_index| NzbFileId {
                job_id,
                file_index: *file_index,
            })
            .collect();
        self.direct_store.begin_materialization(
            job_id,
            set_index,
            demoted_volume_files.iter().copied(),
        );
        if let Some(segment_id) = handoff {
            self.direct_store
                .note_materialization_handoff(set_index, segment_id);
        }
        for file_id in &demoted_volume_files {
            self.block_crcs.forget_file(*file_id);
        }

        crate::runtime::perf_probe::record_owned(
            format!("direct_store.demoted.{}", reason.metric()),
            std::time::Duration::from_nanos(1),
        );
        // Guarded by `claim_demotion` above, so this counts each set exactly
        // once; the per-reason breakdown lives in the perf-probe key and the
        // warn line.
        self.metrics
            .direct_sets_demoted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        warn!(
            job_id = job_id.0,
            set_name = %set_name,
            reason = reason.metric(),
            "direct-store set demoted"
        );

        match self.reconstruct_demoted_set(job_id, set_index).await {
            Ok(volumes) => {
                crate::runtime::perf_probe::record(
                    "direct_store.demoted.reconstructed",
                    std::time::Duration::from_nanos(1),
                );
                // The other half of the materialization account: a whole-set
                // demotion materializes *every* volume of the group, and this
                // is how many that was. Read against
                // `direct_store.repair.materialized_volumes`, whose whole point
                // is being much smaller.
                crate::runtime::perf_probe::record_value(
                    "direct_store.demote.materialized_volumes",
                    volumes as u64,
                );
                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    volumes,
                    "direct-store set materialized from its own routed bytes"
                );
            }
            Err(failure) => {
                crate::runtime::perf_probe::record_owned(
                    format!("direct_store.demote_refetch.{}", failure.metric()),
                    std::time::Duration::from_nanos(1),
                );
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    failure = %failure,
                    "direct-store reconstruction is not possible; refetching the set's volumes"
                );
                self.refetch_demoted_set(job_id, set_index).await;
            }
        }
        // Re-enter every complete volume into the conventional completion
        // seam. While the set was direct, `refresh_archive_state_for_completed_file`
        // suppressed itself for these files at its own entry — a direct set
        // never enters the archive topology, and its extraction never needs
        // one. Demotion is the moment that stops being true: the volumes are
        // ordinary files now, and everything downstream of the conventional
        // decode-completion hook — classification, RAR volume facts, the
        // topology entry the extraction planner chains from — has never run
        // for any of them that completed while direct. Without this replay a
        // materialized volume is invisible to the topology forever: the plan
        // waits on a volume whose bytes sit complete on disk, and nothing
        // ever arrives to change its mind. The replay walks the same door a
        // conventional completion walks (`allow_probe` included), and the
        // hook's own guards skip files that are still incomplete — those
        // complete later through the decode path and get the hook naturally.
        for file_id in demoted_volume_files {
            self.refresh_archive_state_for_completed_file(job_id, file_id, true)
                .await;
        }
        // The other moment a retained image can lose its last possible reader: a
        // demoted set is repaired by the filesystem-bound `Par2Repairer`, which
        // reads no overlay, so a job whose last live set demotes has nothing
        // left that could open one.
        self.release_retained_direct_volumes(job_id).await;
    }

    /// The reconstruction path. `Ok(n)` when `n` volumes were materialized.
    async fn reconstruct_demoted_set(
        &mut self,
        job_id: JobId,
        set_index: usize,
    ) -> Result<usize, ReconstructionFailure> {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Err(ReconstructionFailure::NoLayout);
        };
        if set.router.member_partials().is_empty() {
            // Nothing was ever routed to a member, so there is nothing to
            // reconstruct *from* beyond headers. Refetching is both correct and
            // cheaper than materializing header-only volumes.
            return Err(ReconstructionFailure::NoLayout);
        }
        if set.router.posted_bytes_unavailable() {
            // The member partials hold **plaintext**; the volume
            // being rebuilt holds cipher, and the overlay is what turns one into
            // the other on the way out. This is the residual it cannot do — a
            // routed encrypted member with no declared cipher size, or one whose
            // tail padding never arrived whole — and the sweep must refuse
            // rather than write a volume that is right except for one block,
            // because the yEnc part CRCs it checks against would then refuse
            // *after* the write rather than before it.
            return Err(ReconstructionFailure::EncryptedPostedBytes);
        }
        let working_dir = set.plan().working_dir.clone();
        let volume_files: Vec<(u32, u32)> = set
            .plan()
            .volumes
            .iter()
            .map(|(volume_index, file_index)| (*volume_index, *file_index))
            .collect();

        // Decoded volume lengths and per-volume article geometry, both of which
        // only the download layer knows.
        let mut lengths = std::collections::BTreeMap::new();
        let mut targets = Vec::with_capacity(volume_files.len());
        let mut extents_by_volume = HashMap::new();
        for (volume_index, file_index) in &volume_files {
            let file_id = NzbFileId {
                job_id,
                file_index: *file_index,
            };
            let Some(state) = self.jobs.get(&job_id) else {
                return Err(ReconstructionFailure::NoLayout);
            };
            let Some(file_asm) = state.assembly.file(file_id) else {
                continue;
            };
            // The same name the conventional write path resolves, not the
            // assembly's raw one: a file whose identity was rewritten (a PAR2
            // canonical name, say) is about to be downloaded into *that* path,
            // and reconstructing into a different one would leave the refetch
            // filling a file with a hole where the rebuilt prefix should be.
            let filename = self.current_filename_for_file(job_id, file_asm);
            let received = file_asm.received_bytes();
            // Placed bytes only, deliberately. The provider can serve holds
            // too, but this sweep hands the set to the conventional path, whose
            // decode handoff owns the article that was routing when demotion
            // struck and whose targeted requeue owns every segment the atoms do
            // not wholly back; a hold materialized here would be written twice
            // and counted against a completion gate nothing then clears.
            let physical_coverage = set.volume_coverage(*volume_index);
            let crcs = set.volume_crc_runs(*volume_index);
            // Routing can demote after durably placing only part of the current
            // article. Keep that range provisional: the decode handoff owns the
            // current segment, and the targeted requeue below owns any other
            // segment that is not wholly backed by an article CRC atom.
            let coverage = crcs.materializable_coverage(&physical_coverage);
            let extents = set.segment_extents(*volume_index);
            // A volume that never completed has no authoritative length; its
            // received bytes are the most that can be on disk. A routed range
            // above a hole can end later than that aggregate, though, so its
            // durable coverage is also a lower bound for this sweep.
            let len = set
                .virtual_volume_len(*volume_index, received)
                .max(physical_coverage.end());
            lengths.insert(*volume_index, len);
            extents_by_volume.insert(*volume_index, extents);
            let path = working_dir.join(&filename);
            targets.push((
                *volume_index,
                *file_index,
                filename,
                VolumeReconstruction {
                    volume_index: *volume_index,
                    path,
                    len,
                    assembly_complete: file_asm.is_complete(),
                    covered: coverage,
                    crcs,
                    // `coverage` is already clipped to whole articles, so this
                    // never fires; it states that a floor is published over
                    // what this sweep writes and nothing unverified may sit
                    // under one.
                    partial_article: super::reconstruct::PartialArticle::Refuse,
                },
            ));
        }

        let provider = set.virtual_provider(&lengths);
        let plans: Vec<VolumeReconstruction> =
            targets.iter().map(|(_, _, _, plan)| plan.clone()).collect();
        let sparse = self.direct_store.sparse_marking();
        let rebuilt = tokio::task::spawn_blocking(move || {
            crate::pipeline::direct_store::reconstruct::reconstruct_volumes(
                &provider, &plans, sparse,
            )
        })
        .await
        .map_err(|error| ReconstructionFailure::WriteFailed {
            volume_index: u32::MAX,
            error: error.to_string(),
        })??;

        // Everything above is read-only against the job; from here the
        // reconciliation mutates durable state, in that order: legacy floors
        // and completed-file rows, then the coverage row, then the direct
        // outputs.
        let mut materialized = 0usize;
        let mut keep: HashMap<u32, Vec<u32>> = HashMap::new();
        for (outcome, (volume_index, file_index, filename, plan)) in
            rebuilt.iter().zip(targets.iter())
        {
            debug_assert_eq!(outcome.volume_index, *volume_index);
            let file_id = NzbFileId {
                job_id,
                file_index: *file_index,
            };
            let extents = extents_by_volume.remove(volume_index).unwrap_or_default();
            let (on_disk, floor) = crate::pipeline::direct_store::reconstruct::segments_on_disk(
                &extents,
                &plan.covered,
                outcome.contiguous,
            );
            keep.insert(*file_index, on_disk);
            if outcome.contiguous == 0 {
                continue;
            }
            materialized += 1;

            self.pending_file_progress.remove(&file_id);
            self.persisted_file_progress.remove(&file_id);
            if outcome.complete && outcome.contiguous >= plan.len {
                let md5 = outcome.md5;
                let name = filename.clone();
                let index = *file_index;
                if let Err(error) = self
                    .db_blocking(move |db| {
                        db.complete_file_with_optional_hash(job_id, index, &name, md5.as_ref())
                    })
                    .await
                {
                    warn!(
                        job_id = job_id.0,
                        file_index, error = %error,
                        "failed to record a reconstructed volume as complete"
                    );
                }
            } else if floor > 0 {
                // A partial volume persists only a contiguous, segment-aligned
                // floor. `note_file_progress_floor` suppresses direct source
                // files, and this one still is one until the set's status is
                // read again — so the upsert goes straight to the batch the
                // flush drains, which is the same row `coverage_skip_plan` and
                // `segments_covered_by_floor` read back at restart.
                self.pending_file_progress.insert(file_id, floor);
            }
        }
        // Awaited, not fire-and-forget: the coverage row is retired immediately
        // below, so until these floors are committed the job has no durable
        // account of the volumes at all.
        if let Err(error) = self
            .flush_file_progress_batch_awaited("direct_store.demote.reconstructed_floors")
            .await
        {
            warn!(job_id = job_id.0, error = %error, "failed to persist reconstructed volume floors");
        }

        let mut persist = DatabaseCoveragePersist::new(self.db.clone());
        if let Some(set) = self.direct_store.set_mut(job_id, set_index)
            && let Err(error) = set.retire(&mut persist)
        {
            warn!(job_id = job_id.0, error = %error, "failed to retire a reconstructed direct-store checkpoint");
        }
        self.delete_direct_outputs(job_id, set_index).await;
        self.requeue_after_reconstruction(job_id, set_index, &keep)
            .await;
        for (outcome, (_, file_index, _, plan)) in rebuilt.iter().zip(targets.iter()) {
            if outcome.complete && outcome.contiguous >= plan.len {
                self.direct_store.settle_materialized_file(NzbFileId {
                    job_id,
                    file_index: *file_index,
                });
            }
        }
        Ok(materialized)
    }

    /// The last-resort demotion: retire routed storage and requeue only articles
    /// whose previously committed bytes cannot be reconstructed.
    async fn refetch_demoted_set(&mut self, job_id: JobId, set_index: usize) {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        let volumes: Vec<u32> = set.plan().volumes.values().copied().collect();

        // On this path the checkpoint row goes first, because everything it
        // claims is about to be deleted and nothing replaces it. A crash
        // between here and the refetch costs a redownload, which is what the
        // fallback is doing anyway.
        let mut persist = DatabaseCoveragePersist::new(self.db.clone());
        if let Some(set) = self.direct_store.set_mut(job_id, set_index)
            && let Err(error) = set.retire(&mut persist)
        {
            warn!(job_id = job_id.0, error = %error, "failed to retire a demoted direct-store checkpoint");
        }

        self.delete_direct_outputs(job_id, set_index).await;
        self.refetch_direct_volumes(job_id, &volumes).await;
    }

    /// Deletes a set's partial members, envelope files and holds scratch.
    ///
    /// A sparse half-written output would masquerade as finished work, and the
    /// envelopes and the scratch are scratch by construction.
    async fn delete_direct_outputs(&mut self, job_id: JobId, set_index: usize) {
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            set.router.discard_scratch();
        }
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        let mut doomed: Vec<PathBuf> = set
            .router
            .member_partials()
            .into_iter()
            .map(|(_, _, partial)| set.plan().destination_path(partial))
            .collect();
        doomed.extend(set.plan().envelope_paths());
        // Repair scratch. Normally deleted the moment its spans are routed, so
        // this only ever finds one a demotion interrupted — but a leftover
        // would sit in the working directory for the life of the job, and the
        // reconstruction sweep is about to write the real volume files beside
        // it.
        doomed.extend(set.plan().repair_paths());
        for path in doomed {
            crate::pipeline::release_cached_write_handle(&path);
            let _ = tokio::fs::remove_file(&path).await;
        }
    }

    /// Hands a reconstructed set back to the conventional path, keeping the
    /// articles that are now genuinely on disk.
    ///
    /// Unlike the full-refetch fallback, `keep` names, per NZB file, the
    /// articles whose decoded extents the sweep rebuilt. Those stay committed
    /// in the assembly and are never fetched again. Everything else that the
    /// direct path had committed comes back exactly as the refetch path would
    /// have brought it back. The decode seam still owns its current article and
    /// carries it directly into conventional assembly.
    ///
    /// A file with nothing kept takes the full refetch treatment, including
    /// `mark_file_incomplete`: there is no reconstructed state to protect.
    async fn requeue_after_reconstruction(
        &mut self,
        job_id: JobId,
        set_index: usize,
        keep: &HashMap<u32, Vec<u32>>,
    ) {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        let volume_files: Vec<(u32, u32)> = set
            .plan()
            .volumes
            .iter()
            .map(|(volume_index, file_index)| (*volume_index, *file_index))
            .collect();
        let extents: HashMap<u32, std::collections::BTreeMap<u32, (u64, u64)>> = volume_files
            .iter()
            .map(|(volume_index, file_index)| (*file_index, set.segment_extents(*volume_index)))
            .collect();

        let scheduled_retries: HashSet<SegmentId> = self
            .pending_retries_by_segment
            .keys()
            .copied()
            .filter(|segment_id| segment_id.file_id.job_id == job_id)
            .collect();

        let mut work = Vec::new();
        let mut fully_reset: Vec<u32> = Vec::new();
        let write_buf_max_pending = self.write_buf_max_pending;
        {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return;
            };
            let mut queued: HashSet<SegmentId> = HashSet::new();
            state.download_queue.extend_segment_ids(&mut queued);
            state.recovery_queue.extend_segment_ids(&mut queued);

            let mut lost_bytes = 0u64;
            for (_, file_index) in &volume_files {
                let file_id = NzbFileId {
                    job_id,
                    file_index: *file_index,
                };
                let kept: HashSet<u32> = keep
                    .get(file_index)
                    .map(|segments| segments.iter().copied().collect())
                    .unwrap_or_default();
                if kept.is_empty() {
                    fully_reset.push(*file_index);
                }
                let Some(file) = state.spec.files.get(*file_index as usize) else {
                    continue;
                };
                let file = file.clone();
                let Some(file_asm) = state.assembly.file(file_id) else {
                    continue;
                };
                let previously_received = file_asm.received_bytes();
                let committed: HashSet<u32> = file
                    .segments
                    .iter()
                    .filter(|segment| file_asm.has_segment(segment.ordinal))
                    .map(|segment| segment.ordinal)
                    .collect();

                // Rebuild the assembly to exactly the kept set. `commit_segment`
                // is the only way in and `reset` the only way out, so the
                // sequence is reset-then-re-commit rather than a surgical
                // removal; the decoded sizes come from the recorded extents, so
                // the byte counters land where they were.
                if let Some(file_asm) = state.assembly.file_mut(file_id) {
                    file_asm.reset();
                }
                let mut kept_bytes = 0u64;
                let mut materialized_extents = Vec::with_capacity(kept.len());
                let file_extents = extents.get(file_index).cloned().unwrap_or_default();
                for segment_number in &kept {
                    let Some((offset, len)) = file_extents.get(segment_number).copied() else {
                        continue;
                    };
                    if let Some(file_asm) = state.assembly.file_mut(file_id)
                        && file_asm.commit_segment(*segment_number, len as u32).is_ok()
                    {
                        kept_bytes = kept_bytes.saturating_add(len);
                        materialized_extents.push((offset, len));
                    }
                }
                lost_bytes =
                    lost_bytes.saturating_add(previously_received.saturating_sub(kept_bytes));
                let needs_more_bytes = state
                    .assembly
                    .file(file_id)
                    .is_some_and(|file| !file.is_complete());
                if !materialized_extents.is_empty() && needs_more_bytes {
                    // Reconstruction made these article extents durable without
                    // passing through the conventional writer. Seed its sparse
                    // markers so a later missing article bridges the cursor;
                    // only the contiguous floor is persisted across restart.
                    let write_buf = self
                        .write_buffers
                        .entry(file_id)
                        .or_insert_with(|| WriteReorderBuffer::new(write_buf_max_pending));
                    for (offset, len) in materialized_extents {
                        write_buf.mark_persisted(offset, len as usize);
                    }
                    let (unexpected, contiguous_end) = write_buf.drain_ready_with_contiguous_end();
                    debug_assert!(unexpected.is_empty());
                    debug_assert!(contiguous_end <= kept_bytes);
                }

                for segment in &file.segments {
                    if kept.contains(&segment.ordinal) {
                        continue;
                    }
                    let segment_id = SegmentId {
                        file_id,
                        segment_number: segment.ordinal,
                    };
                    if queued.contains(&segment_id) || scheduled_retries.contains(&segment_id) {
                        continue;
                    }
                    if !committed.contains(&segment.ordinal) {
                        continue;
                    }
                    work.push(DownloadWork {
                        segment_id,
                        message_id: crate::jobs::ids::MessageId::new(&segment.message_id),
                        groups: std::sync::Arc::from(file.groups.as_slice()),
                        priority: file.role.download_priority(),
                        byte_estimate: segment.bytes,
                        retry_count: 0,
                        is_recovery: false,
                        completion_critical: false,
                        exclude_servers: vec![],
                        avoid_server: None,
                    });
                }
            }
            state.downloaded_bytes = state.downloaded_bytes.saturating_sub(lost_bytes);
        }

        // Only files the sweep rebuilt nothing for: everything else has legacy
        // rows this path just wrote, and `mark_file_incomplete` deletes exactly
        // those.
        for file_index in fully_reset {
            let file_id = NzbFileId { job_id, file_index };
            self.pending_file_progress.remove(&file_id);
            self.persisted_file_progress.remove(&file_id);
            if let Err(error) = self.db.mark_file_incomplete(job_id, file_index) {
                warn!(
                    job_id = job_id.0,
                    file_index, error = %error,
                    "failed to invalidate a demoted direct-store volume"
                );
            }
        }
        for item in work {
            self.requeue_retry_work(item);
        }
    }

    /// Hands a demoted set's source volumes back to the conventional path.
    ///
    /// Requeues **only what nothing else owns**: the articles whose bytes were
    /// routed into direct destinations that have just been deleted. A segment
    /// still sitting in a queue, still in flight, waiting on a scheduled retry,
    /// or held by the decode seam is left alone; each reaches the conventional
    /// path through its existing owner.
    ///
    /// The job's byte counter is *adjusted*, never zeroed: it is job-wide, and
    /// the other files' contribution to it has nothing to do with this set.
    async fn refetch_direct_volumes(&mut self, job_id: JobId, file_indices: &[u32]) {
        // Snapshotted before the job borrow: a segment whose retry is already
        // scheduled re-enters the queue on its own.
        let scheduled_retries: HashSet<SegmentId> = self
            .pending_retries_by_segment
            .keys()
            .copied()
            .filter(|segment_id| segment_id.file_id.job_id == job_id)
            .collect();

        let mut work = Vec::new();
        {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return;
            };
            let mut queued: HashSet<SegmentId> = HashSet::new();
            state.download_queue.extend_segment_ids(&mut queued);
            state.recovery_queue.extend_segment_ids(&mut queued);

            let mut routed_bytes = 0u64;
            for file_index in file_indices {
                let file_id = NzbFileId {
                    job_id,
                    file_index: *file_index,
                };
                let Some(file) = state.spec.files.get(*file_index as usize) else {
                    continue;
                };
                let Some(file_asm) = state.assembly.file(file_id) else {
                    continue;
                };
                routed_bytes = routed_bytes.saturating_add(file_asm.received_bytes());
                for segment in &file.segments {
                    let segment_id = SegmentId {
                        file_id,
                        segment_number: segment.ordinal,
                    };
                    if queued.contains(&segment_id) || scheduled_retries.contains(&segment_id) {
                        continue;
                    }
                    // Committed articles lost their bytes with the partials.
                    // Every other segment is somebody else's outstanding work.
                    let committed = file_asm.has_segment(segment.ordinal);
                    if !committed {
                        continue;
                    }
                    work.push(DownloadWork {
                        segment_id,
                        message_id: crate::jobs::ids::MessageId::new(&segment.message_id),
                        groups: std::sync::Arc::from(file.groups.as_slice()),
                        priority: file.role.download_priority(),
                        byte_estimate: segment.bytes,
                        retry_count: 0,
                        is_recovery: false,
                        completion_critical: false,
                        exclude_servers: vec![],
                        avoid_server: None,
                    });
                }
                if let Some(file_asm) = state.assembly.file_mut(file_id) {
                    file_asm.reset();
                }
            }
            state.downloaded_bytes = state.downloaded_bytes.saturating_sub(routed_bytes);
        }
        for file_index in file_indices {
            let file_id = NzbFileId {
                job_id,
                file_index: *file_index,
            };
            self.pending_file_progress.remove(&file_id);
            self.persisted_file_progress.remove(&file_id);
            if let Err(error) = self.db.mark_file_incomplete(job_id, *file_index) {
                warn!(
                    job_id = job_id.0,
                    file_index, error = %error,
                    "failed to invalidate a demoted direct-store volume"
                );
            }
        }
        for item in work {
            self.requeue_retry_work(item);
        }
    }
}

/// One contiguous copy of a decoded span. Routing splits the span at
/// destination boundaries, which a batched chunk list cannot express.
/// Reads every restart-seeded run and returns its CRC32, in the order asked.
///
/// **One sequential pass per file**: the plan arrives grouped by member and in
/// ascending offset, and the reader keeps the file open across a member's runs
/// and seeks forward only. A short read is a failure, not a zero-filled answer —
/// a partial that is shorter than the coverage claimed for it is exactly the
/// state restart's length probe refuses, and reaching it here means the file
/// changed under a validated checkpoint.
///
/// **Streamed, not slurped.** A run is one whole RAR *part*, which is a whole
/// volume's worth of a member — hundreds of megabytes on an ordinary set, and
/// this runs on the blocking pool at restore for every restored set at once. The
/// CRC32 composes over a rolling buffer, so the resident cost is one
/// [`REARM_CHUNK_BYTES`] buffer for the whole plan rather than the largest part
/// in it.
///
/// `destination_dir` is the job's staging root, because every run names a member
/// `.direct.partial` and those are payload.
fn read_restart_seeded_runs(
    destination_dir: &std::path::Path,
    runs: &[super::router::RestartReadRun],
) -> std::io::Result<Vec<u32>> {
    use std::io::{Read, Seek, SeekFrom};

    let mut checksums = Vec::with_capacity(runs.len());
    let mut open: Option<(String, std::fs::File)> = None;
    let mut buffer = vec![0u8; REARM_CHUNK_BYTES];
    for run in runs {
        let file = match &mut open {
            Some((path, file)) if path == &run.relative_partial => file,
            _ => {
                let file = std::fs::File::open(destination_dir.join(&run.relative_partial))?;
                open = Some((run.relative_partial.clone(), file));
                &mut open.as_mut().expect("just assigned").1
            }
        };
        file.seek(SeekFrom::Start(run.logical_offset))?;
        let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
        let mut remaining = run.len;
        while remaining > 0 {
            let want = (remaining.min(buffer.len() as u64)) as usize;
            // `read_exact` rather than `read`: a run the checkpoint claims must be
            // wholly present, and a short read here is the file having changed
            // under a validated row — not a partial answer to compose over.
            file.read_exact(&mut buffer[..want])?;
            hasher.update(&buffer[..want]);
            remaining -= want as u64;
        }
        checksums.push(hasher.finalize() as u32);
    }
    Ok(checksums)
}

fn contiguous_bytes(data: &DecodedChunk) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len_bytes());
    data.for_each_slice(|slice| out.extend_from_slice(slice));
    out
}
