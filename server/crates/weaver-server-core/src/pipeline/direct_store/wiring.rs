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
    BufferedDecodedSegment, DecodedChunk, DirectDemotionWork, DirectDemotionWorkDone,
    DirectPostRepairCarry, DirectPostRepairWork, DirectPostRepairWorkDone, DirectToleratedWork,
    DirectToleratedWorkDone, Pipeline,
};

/// Read chunk for the restart gate re-arm. Matches the reconstruction sweep's:
/// large enough that a big part is a few hundred iterations, small enough to
/// keep the whole plan's resident cost to one buffer.
const REARM_CHUNK_BYTES: usize = 256 * 1024;

/// A placement failure before any coverage is admitted. The caller decides
/// whether to reconstruct conventional volumes or retain verified repair output.
#[derive(Debug)]
pub(in crate::pipeline) enum DirectPlacementError {
    Sparse {
        path: PathBuf,
        error: std::io::Error,
    },
    Write(std::io::Error),
}

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
    /// The process-wide holds accountant every set this runtime admits charges
    /// to. Built from the settings' limits; unbounded for a runtime built by
    /// hand. See [`super::accountant`].
    accountant: std::sync::Arc<super::accountant::HoldsAccountant>,
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
            accountant: std::sync::Arc::new(super::accountant::HoldsAccountant::new(
                settings.holds_limits(),
            )),
            ..Self::default()
        }
    }

    /// The process-wide holds accountant.
    #[cfg(test)]
    pub(crate) fn holds_accountant(&self) -> &super::accountant::HoldsAccountant {
        &self.accountant
    }

    /// Test hook: replace the shared limits, so a process-wide breach is
    /// reachable with a few hundred bytes across two sets. Applies to the sets
    /// admitted afterwards.
    #[cfg(test)]
    pub(crate) fn set_holds_limits(&mut self, limits: super::accountant::HoldsLimits) {
        self.accountant = std::sync::Arc::new(super::accountant::HoldsAccountant::new(limits));
    }

    /// Test hook: [`Self::set_holds_limits`] with the free-space reading
    /// behind the disk reserve replaced.
    #[cfg(test)]
    pub(crate) fn set_holds_limits_with_disk_probe(
        &mut self,
        limits: super::accountant::HoldsLimits,
        probe: super::accountant::DiskProbe,
    ) {
        self.accountant = std::sync::Arc::new(super::accountant::HoldsAccountant::with_probe(
            limits, probe,
        ));
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
            .set_holds_accountant(std::sync::Arc::clone(&self.accountant));
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

/// What one demotion's reconstruction sweep did to the set's volumes.
///
/// The two counts are **not** disjoint any more, and that is the point: a
/// volume that refuses one run is both materialized (for everything the sweep
/// verified) and refetched (for the articles it did not). The byte totals are
/// what actually says how much the demotion cost, because a volume count cannot
/// tell a whole volume off the wire from one missing article.
#[derive(Debug, Default)]
struct ReconstructionSummary {
    /// Volumes that came out of the sweep with a verified contiguous prefix.
    materialized: usize,
    /// Volumes the sweep could not rebuild in full, and the first reason each
    /// refused. Each costs the articles its verified ranges do not back, and
    /// nothing of its siblings.
    refetched: Vec<(u32, ReconstructionFailure)>,
    /// Decoded bytes the sweep verified and handed to the conventional path as
    /// already on disk. These are the bytes a demotion no longer pays for
    /// twice.
    retained_bytes: u64,
    /// Decoded bytes that were routed once and have to come off the wire again:
    /// everything a volume had committed that its verified ranges do not back.
    refetched_bytes: u64,
}

/// The reconciliation's half of a detached demotion sweep, snapshotted when the
/// sweep was handed off and carried by the ticket until it lands.
///
/// It holds no borrow of the set or the job on purpose: by the time it is read
/// back, the actor has processed an unbounded number of other messages, and
/// only the sweep's own outcomes plus this snapshot may decide what the volumes
/// now contain.
pub(crate) struct DemotedSweepPlan {
    set_name: String,
    /// Carried for the ticket's log lines only. The demotion has already
    /// recorded the reason everywhere it is acted on.
    reason: DemotionReason,
    /// `(volume_index, file_index, filename, sweep plan)`, in the order the
    /// sweep reports its outcomes.
    targets: Vec<(u32, u32, String, VolumeReconstruction)>,
    /// Per volume, the decoded extent of each of its articles — the geometry
    /// that turns verified ranges into a keep set and a segment-aligned floor.
    extents_by_volume: HashMap<u32, std::collections::BTreeMap<u32, (u64, u64)>>,
    /// Every volume of the set as an NZB file, for the completion replay the
    /// handback ends with.
    volume_files: Vec<NzbFileId>,
    /// Articles the decode seam took ownership of at the demotion instant: the
    /// one that was routing when the set demoted, and any that reached an
    /// already-demoted set behind it.
    ///
    /// Snapshotted here because the seam writes them while the sweep runs and
    /// clears its own record as soon as it has. The reconciliation must not
    /// then read their segments as "committed but not verified" and requeue
    /// them: their bytes are on disk, written by their owner, at the offsets
    /// the conventional path expects.
    handoffs: HashSet<SegmentId>,
}

/// Chunks a demoted volume's handback unblocked in the write buffer: the file
/// they belong to, the ready `(offset, chunk)` pairs in write order, and the
/// contiguous end the buffer reports after draining them.
type UnblockedHandbackWrites = (NzbFileId, Vec<(u64, BufferedDecodedSegment)>, u64);

/// A demotion sweep ready to be handed off: the worker's half (which is moved
/// into the blocking task and never comes back) and the reconciliation's.
struct PreparedDemotedSweep {
    provider: super::provider::HybridVolumeProvider,
    plans: Vec<VolumeReconstruction>,
    sparse: super::sparse::SparseMarking,
    plan: DemotedSweepPlan,
}

/// One member the member tolerance produces at finalization, with
/// the two path forms the two arms need.
struct ToleratedTarget {
    /// Raw header name, which is what `find_member` is asked for.
    name: String,
    /// Absolute destination under the job's staging root.
    destination: PathBuf,
    /// The same path relative to that root, for [`ExtractionRoot`].
    relative: PathBuf,
    is_directory: bool,
}

/// What the tolerance produced: the member names, and the directory entries
/// whose metadata still has to be restored.
#[derive(Debug, Default)]
pub(crate) struct ToleratedExtraction {
    /// Raw header names produced, for `extracted_members`. Directory entries
    /// are in here too — the conventional extractor reports one the same way,
    /// and a name in neither list is a name completion cannot account for.
    members: Vec<String>,
    /// `(metadata, absolute path)` per directory entry, applied **after** the
    /// commit loop: every file renamed into a directory bumps that directory's
    /// mtime, so restoring it before the members land would restore a value the
    /// next rename overwrites.
    directories: Vec<(unrar_rs::MemberInfo, PathBuf)>,
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
    /// A set left direct mode inside this call and its volumes are being
    /// materialized. Bytes are moving, so the caller must go round again rather
    /// than settle anything on a verdict taken over the virtual volumes the set
    /// no longer has — the same answer [`Self::Repaired`] gets, for the same
    /// reason.
    Demoted,
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
        let par2_available = super::plan::spec_carries_par2(&state.spec);
        let par3_available = !par2_available && self.par3_direct_checks_available(job_id);
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
                set.router.note_par2_available(par2_available);
                set.router.note_par3_available(par3_available);
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
        let par2_available = self
            .jobs
            .get(&job_id)
            .is_some_and(|state| super::plan::spec_carries_par2(&state.spec));
        let par3_available = !par2_available && self.par3_direct_checks_available(job_id);
        let mut set = DirectSet::new(job_id, plan);
        self.direct_store.apply_ceilings(&mut set);
        set.router.set_password(password);
        set.router.note_par2_available(par2_available);
        set.router.note_par3_available(par3_available);
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
}

mod commit;
mod demotion;
mod par2;

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
