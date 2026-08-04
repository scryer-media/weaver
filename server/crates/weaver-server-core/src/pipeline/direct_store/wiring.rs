//! Where direct-store meets the download pipeline (plan 135, D3/D7).
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
//!    archive order and is marked extracted; a set that demotes materializes its
//!    volumes from its own routed bytes (D8), persists the legacy state that
//!    replaces its coverage, and hands them to the conventional path — falling
//!    back to refetching everything only when reconstruction is impossible.
//!
//! # D7, stated as suppression points
//!
//! The routing seam returns before `persist_ready_segments`, so for a direct
//! source volume there is no physical write, **no `active_file_progress` floor
//! upsert**, and no `commit_persisted_segment`. The file-complete work it would
//! have done is re-implemented here without the parts that need a file:
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
//!   file-complete path the routing seam returns before.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Instant;

use tracing::{debug, info, warn};

use super::barrier::{BarrierDemand, BarrierDrain, DatabaseCoveragePersist, DestinationSync};
use super::plan::DirectSetPlan;
use super::reconstruct::{ReconstructionFailure, VolumeReconstruction};
use super::router::{DemotionReason, DirectDestination, RoutedSpan};
use super::set::DirectSet;
use super::sparse::SparseMarking;
use super::{DirectStoreGate, DirectStoreSettings};
use crate::DownloadWork;
use crate::events::model::PipelineEvent;
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::pipeline::{BufferedDecodedSegment, DecodedChunk, Pipeline};

/// Read chunk for the restart gate re-arm. Matches the reconstruction sweep's:
/// large enough that a big part is a few hundred iterations, small enough to
/// keep the whole plan's resident cost to one buffer.
const REARM_CHUNK_BYTES: usize = 256 * 1024;

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
    sets: HashMap<JobId, Vec<DirectSet>>,
    /// Destinations already created and marked sparse, per job (B3, D3). A
    /// member stored inside a directory names a partial inside that directory
    /// and nothing else creates it, and every destination has to carry the
    /// sparse attribute before its first routed byte.
    prepared_destinations: HashMap<JobId, HashSet<PathBuf>>,
    /// Test-only holds ceiling applied to every set this runtime admits.
    #[cfg(test)]
    holds_budget_override: Option<u64>,
    /// Test-only scratch ceiling (D2), which shortcuts the configured one so a
    /// breach is reachable without paging half a gigabyte.
    #[cfg(test)]
    holds_scratch_ceiling_override: Option<u64>,
    /// Sparse marker for every file this runtime's sets create (D3). Only the
    /// tests that drive the marking-failure demotion ever change it.
    sparse: SparseMarking,
    /// Source volumes a repair has materialized over this pipeline's life (D8).
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
    /// step — over this pipeline's life (phase 6 review, F8).
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

    /// Test hook: make every sparse marking attempt fail, which is the only way
    /// to reach D3's demotion arm on a platform whose marker cannot fail.
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
    /// Every path that builds a `DirectSet` goes through here, restore included.
    /// Restore used to skip it, so a restart test could set a budget and then
    /// watch the restored set quietly use the 64 MiB / 512 MiB defaults — which
    /// makes every budget assertion about a restored set vacuous, and those are
    /// exactly the assertions D2's ceilings need after a restart.
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
        self.prepared_destinations.remove(&job_id);
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
/// virtually (plan 135, D5).
///
/// The provider is keyed by **NZB file index**, not by volume index: one job can
/// carry several direct sets and every set numbers its volumes from zero, so the
/// volume index is not unique inside a job while the file index always is. The
/// adapter only ever uses the key to reach a reader, so any injective key works,
/// and this one is already the identity the PAR2 binding is resolved through.
pub(crate) struct DirectPar2Overlay {
    pub(crate) provider: super::provider::HybridVolumeProvider,
    pub(crate) volumes: Vec<super::par2_access::VirtualPar2Volume>,
    /// Which direct set owns each bound PAR2 file, so damage demotes the set
    /// that produced the bytes rather than every set of the job.
    sets: HashMap<weaver_par2::FileId, usize>,
    /// The job file index each bound PAR2 file resolved to. The overlay re-keys
    /// virtual volumes by it, so it is also how phase 6 walks back from a
    /// damaged PAR2 file to the set's own volume index.
    file_indices: HashMap<weaver_par2::FileId, u32>,
    /// The volume lengths the overlay was built with, so a repair can rebuild
    /// the very same provider without re-deriving them from the assembly.
    lengths: Vec<(usize, std::collections::BTreeMap<u32, u64>)>,
}

impl DirectPar2Overlay {
    /// The direct set that owns one bound PAR2 file.
    pub(crate) fn owner_of(&self, file_id: &weaver_par2::FileId) -> Option<usize> {
        self.sets.get(file_id).copied()
    }

    /// The job file index one bound PAR2 file resolved to.
    pub(crate) fn file_index_of(&self, file_id: &weaver_par2::FileId) -> Option<u32> {
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
    pub(crate) fn virtual_volumes_for(
        &self,
        runtime: &DirectStoreRuntime,
        job_id: JobId,
    ) -> Option<Vec<super::provider::VirtualVolume>> {
        let mut volumes = Vec::new();
        for (set_index, lengths) in &self.lengths {
            let set = runtime.set(job_id, *set_index)?;
            for mut volume in set.virtual_volumes(lengths) {
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
/// would have handed the job to `Par2Repairer` (plan 135, D8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    Clean,
    /// Neither: no live set, no verdict, or a repair that refused. The caller
    /// falls back to demoting for the repairer, which is phase 5's behaviour.
    Unresolved,
}

/// What the routing seam did with an article.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectRouteOutcome {
    /// The bytes were routed; the caller must not write the source volume.
    Routed,
    /// The set demoted; the caller must not write the source volume either,
    /// because the volume is being refetched from scratch.
    Demoted,
}

/// What the decode seam should do with one file's bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectFileTarget {
    /// A live set's source volume: route it.
    Route { set_index: usize, volume_index: u32 },
    /// A **finalized** set's source volume. The set's members are already at
    /// their destinations and the volume was never a file, so a late duplicate
    /// has nowhere to go: routing it would write through stale partial paths,
    /// and writing it conventionally would materialize a volume the whole
    /// point was never to create. It is dropped (B5).
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
        // Plan 136, E1 review F1. Read before the sets exist, because it decides
        // whether an encrypted member of any of them may be admitted at all.
        let par2_declared = self.job_spec_has_par2_file(job_id);
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let (admitted, refused) = DirectSetPlan::discover(&state.spec, &state.working_dir);
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
                info!(
                    job_id = job_id.0,
                    set_name = %plan.set_name,
                    volumes = plan.volumes.len(),
                    "direct-store admitted an archive set"
                );
                // No format is chosen here: the router reads it from the first
                // volume's signature, so a RAR4 set routes as RAR4 rather than
                // demoting on its first header (H1).
                let mut set = DirectSet::new(job_id, plan);
                self.direct_store.apply_ceilings(&mut set);
                // Plan 136, E-D1. The winning password the job was submitted
                // with, if any; `refresh_direct_passwords` picks up one that
                // arrives later. Held in memory only.
                set.router.set_password(password.as_deref());
                // E1 review F1. A par2-bearing job refuses encrypted admission
                // on the *first* header parse, where the refusal costs one
                // article. The `EncryptedPar2Unsupported` guard behind the
                // authoritative pass stays as the belt for a recovery set the
                // spec did not declare — a deobfuscated or renamed PAR2 file can
                // still turn up mid-job — but as the *only* guard it fired after
                // the whole set had downloaded, and an encrypted set cannot
                // reconstruct posted bytes from its plaintext partials, so the
                // demotion refetched every volume. Twice the bytes and twice the
                // wall for a job that used to pay one article.
                if par2_declared {
                    set.router.refuse_encrypted_for_par2();
                }
                set
            })
            .collect();
        self.direct_store.sets.insert(job_id, sets);
    }

    /// Re-reads the job's password into every set still willing to take one
    /// (plan 136, E-D1).
    ///
    /// This is the answer to plan 136's open question 2 written as code:
    /// **weaver does support setting a password after add** — the GraphQL
    /// `setJobPassword` mutation and the NZBGet facade's `editqueue` /
    /// `GroupSetParameter *Unpack:Password` both mutate the live `JobSpec` in
    /// place — and [`Self::ensure_direct_sets`] is memoized per job, so a set
    /// built before the password arrived would never see it. Re-reading it here
    /// costs one map lookup per article and stops the moment a set **admits** a
    /// password or leaves direct mode, which for every set with no encrypted
    /// member is the first parse.
    ///
    /// # The window closes at the first header parse (E1 review F5)
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
        for set in self.direct_store.sets_mut(job_id) {
            set.router.set_password(Some(password.as_str()));
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

    /// D7: whether this file's bytes are a direct set's source volume, so no
    /// legacy floor, completed-file row or archive re-probe may be written for
    /// it. `&self`, because the suppression checks sit inside paths that
    /// already hold the pipeline immutably.
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

    /// Whether this file is a source volume of a live direct set that is
    /// **decrypting on the way to its destinations** (plan 136, E1 review F3).
    ///
    /// The one question every posted-byte consumer has to ask before it reads a
    /// virtual volume: for such a set the partials hold plaintext and the volume
    /// it is being asked to answer for held cipher, so the honest answer is no
    /// answer at all.
    pub(crate) fn direct_volume_routes_encrypted(&self, file_id: NzbFileId) -> bool {
        self.direct_store
            .sets_for(file_id.job_id)
            .iter()
            .any(|set| {
                !set.is_demoted()
                    && set.router.routes_encrypted()
                    && set.plan().volume_for_file(file_id.file_index).is_some()
            })
    }

    /// The virtual volume behind one direct source file, as a **one-volume**
    /// provider plus its logical length (plan 135, D5).
    ///
    /// Live PAR2's settle and backfill reads are defined in source-volume space,
    /// which is exactly the space a virtual volume answers in — but they arrive
    /// one file at a time, so building the whole set's provider per read would
    /// be O(volumes) work for a one-volume question. The length is the decoded
    /// total the download layer tracks, never a file's `metadata().len()`: for a
    /// direct volume there is no file to ask.
    ///
    /// `None` for anything that is not a live direct set's source volume,
    /// including a demoted set's — whose volumes are materializing or being
    /// refetched, and are read from disk like any other file.
    ///
    /// Also `None` for an **encrypted** set's volume (E1 review F3). The
    /// provider would assemble the answer out of the envelope plus the member
    /// partials, and those partials are plaintext where the source volume held
    /// cipher — so every read that crossed a routed member would come back
    /// wrong, and PAR2 would call a byte-perfect volume damaged. A caller that
    /// falls back to the on-disk path finds no file and leaves its blocks
    /// pending, which is the correct verdict: nothing has been verified.
    pub(crate) fn direct_virtual_volume(
        &self,
        file_id: NzbFileId,
    ) -> Option<(u32, u64, super::provider::HybridVolumeProvider)> {
        if self.direct_volume_routes_encrypted(file_id) {
            return None;
        }
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
    /// through the same name candidates live verification binds on. An
    /// unresolved one is skipped **here**, but that skip is not the safety net:
    /// a half-bound set would have the pass read its remaining volumes off a
    /// disk they are not on and report them missing, and
    /// [`Self::demote_direct_sets_with_par2_damage`] could not even attribute
    /// that damage back to the set, because attribution is keyed by the very
    /// binding that failed. The net is
    /// [`Self::demote_unbindable_direct_sets`], which runs *before* the pass and
    /// demotes any live set with an unbindable volume outright, so what reaches
    /// here is either a fully bound set or no set at all (B2).
    pub(crate) fn direct_par2_overlay(&self, job_id: JobId) -> Option<DirectPar2Overlay> {
        let mut volumes = Vec::new();
        let mut virtual_volumes = Vec::new();
        let mut sets = HashMap::new();
        let mut file_indices = HashMap::new();
        let mut set_lengths = Vec::new();
        for (set_index, set) in self.direct_store.sets_for(job_id).iter().enumerate() {
            // A demoted set's volumes are materializing or being refetched, and
            // a **finalized** set no longer owns a readable volume image at all:
            // its partials have been renamed to their destinations and its
            // envelopes deleted. Serving either one through the provider would
            // answer a verifier's reads with holes and report damage that is not
            // there. Finalization is gated on the job's verdict for exactly this
            // reason, so a set can only be finalized here on a *re*-verify, and
            // then the ordinary file access is the honest answer — see
            // `forgive_finalized_direct_volumes` for what makes it honest.
            if set.is_demoted() || set.is_finalized() {
                continue;
            }
            // One `virtual_volumes` call for the whole set, so the shared
            // partial-path map is built once rather than once per volume (nit).
            let mut lengths = std::collections::BTreeMap::new();
            let mut bindings = HashMap::new();
            for (volume_index, file_index) in &set.plan().volumes {
                let file_id = NzbFileId {
                    job_id,
                    file_index: *file_index,
                };
                let Some(binding) = self.resolve_live_par2_binding(file_id) else {
                    continue;
                };
                let received = self
                    .jobs
                    .get(&job_id)
                    .and_then(|state| state.assembly.file(file_id))
                    .map(|file| file.received_bytes())
                    .unwrap_or(0);
                let len = set.virtual_volume_len(*volume_index, received);
                lengths.insert(*volume_index, len);
                bindings.insert(*volume_index, (*file_index, binding.par2_file_id));
            }
            for mut volume in set.virtual_volumes(&lengths) {
                let Some((file_index, par2_file_id)) = bindings.get(&volume.volume_index).copied()
                else {
                    continue;
                };
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
            provider: super::provider::HybridVolumeProvider::new(virtual_volumes),
            volumes,
            sets,
            file_indices,
            lengths: set_lengths,
        })
    }

    /// Whether the authoritative PAR2 pass may run over `job_id`'s direct sets
    /// yet, or has to wait for their payload (plan 135, D5; H3).
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
    pub(crate) fn direct_sets_ready_for_authoritative_par2(&self, job_id: JobId) -> bool {
        let waiting = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_demoted() && !set.is_finalized() && !set.all_volumes_complete());
        !waiting || !self.job_has_pending_download_pipeline_work(job_id)
    }

    /// Demotes every live direct set of `job_id` holding a source volume that
    /// cannot be bound, unambiguously, to a PAR2 description (B2).
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
    pub(crate) async fn demote_unbindable_direct_sets(&mut self, job_id: JobId) -> bool {
        // Without a parsed recovery set nothing can bind, and demoting every
        // direct set of a job whose PAR2 has simply not arrived yet would undo
        // the whole feature.
        if self.par2_set(job_id).is_none() {
            return false;
        }
        // Plan 136. An encrypted set's destinations hold plaintext and PAR2
        // describes the posted cipher, so serving one to the pass would report
        // every slice damaged. Demoted here rather than refused at admission
        // because the PAR2 file can arrive at any point in a job's life, long
        // after the set started routing — this is the seam that already knows a
        // recovery set exists, and it is the seam the pass runs behind.
        let encrypted: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| !set.is_demoted() && !set.is_finalized())
            .filter(|(_, set)| set.router.routes_encrypted())
            .map(|(set_index, _)| set_index)
            .collect();
        let mut demoted_any = !encrypted.is_empty();
        for set_index in encrypted {
            warn!(
                job_id = job_id.0,
                "a PAR2-bearing job holds an encrypted direct set; demoting so the authoritative \
                 pass reads posted bytes instead of decrypted destinations"
            );
            self.demote_direct_set(
                job_id,
                set_index,
                DemotionReason::EncryptedPar2Unsupported,
                None,
            )
            .await;
        }
        let unbindable: Vec<(usize, u32)> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| !set.is_demoted() && !set.is_finalized())
            .filter_map(|(set_index, set)| {
                set.plan()
                    .volumes
                    .iter()
                    .find(|(_, file_index)| {
                        self.resolve_live_par2_binding(NzbFileId {
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
            self.demote_direct_set(job_id, set_index, DemotionReason::Par2Unbindable, None)
                .await;
        }
        demoted_any
    }

    /// Rewrites `Missing` to `Complete` for every source volume of a
    /// **finalized** direct set, before the caller counts damage (B1).
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
    /// Returns the number of missing slices forgiven.
    pub(crate) fn forgive_finalized_direct_volumes(
        &self,
        job_id: JobId,
        verification: &mut weaver_par2::VerificationResult,
    ) -> u32 {
        if self.par2_set(job_id).is_none() {
            return 0;
        }
        let finalized: HashSet<weaver_par2::FileId> = self
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
                self.resolve_live_par2_binding(file_id)
                    .map(|binding| binding.par2_file_id)
            })
            .collect();
        if finalized.is_empty() {
            return 0;
        }

        let mut forgiven = 0u32;
        for file in &mut verification.files {
            if !matches!(file.status, weaver_par2::verify::FileStatus::Missing)
                || !finalized.contains(&file.file_id)
            {
                continue;
            }
            forgiven = forgiven.saturating_add(file.missing_slice_count);
            file.status = weaver_par2::verify::FileStatus::Complete;
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

    /// Answers PAR2 damage on a job's direct sets (plan 135, D8).
    ///
    /// Phase 6's entry point, and the whole of D8's *repair while still direct*
    /// transition seen from the pipeline. It tries the repair first and falls
    /// back to wave 2's whole-set demotion on any refusal, so the caller's
    /// contract is unchanged: `true` means the job's next move is a fresh
    /// completion check, over either repaired virtual volumes or materialized
    /// physical ones.
    ///
    /// Ordering is D8's, and it is normative:
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
    pub(crate) async fn resolve_direct_sets_with_par2_damage(
        &mut self,
        job_id: JobId,
        verification: &weaver_par2::VerificationResult,
    ) -> bool {
        if self
            .repair_direct_sets_with_par2_damage(job_id, verification)
            .await
        {
            return true;
        }
        self.demote_direct_sets_with_par2_damage(job_id, verification)
            .await
    }

    /// Phase 6's chance for a live direct set, taken **before** the completion
    /// gate hands the job to `Par2Repairer` (plan 135, D8).
    ///
    /// That branch exists for jobs the fast paths could not clear, and a live
    /// direct set reaches it routinely: it contributes nothing to the clean-PAR2
    /// integrity gate — a direct set never enters the archive topology — so a
    /// damaged one always arrives here rather than at the verify branch. The
    /// repairer is filesystem-bound, so today's answer is
    /// [`Self::demote_live_direct_sets_for_par2_repair`]: materialize everything
    /// and let it work over real files. This is what phase 6 puts in front of
    /// that, and `false` means the demotion is still the answer.
    ///
    /// The verdict is computed here rather than borrowed, because the branch has
    /// none yet. It is deliberately a **quiet** pass — no status transition, no
    /// verification events — for two reasons: the analyze pass immediately below
    /// emits its own, so a job that falls through would report verifying twice;
    /// and this one exists to answer a question about direct sets, not to record
    /// the job's verdict.
    pub(crate) async fn resolve_direct_sets_before_par2_repairer(
        &mut self,
        job_id: JobId,
        par2_set: std::sync::Arc<weaver_par2::Par2FileSet>,
        working_dir: PathBuf,
    ) -> DirectPar2Resolution {
        if !self
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_demoted() && !set.is_finalized())
        {
            return DirectPar2Resolution::Unresolved;
        }
        let Some(verification) = self
            .verify_direct_sets_quietly(job_id, par2_set, working_dir)
            .await
        else {
            return DirectPar2Resolution::Unresolved;
        };
        if !verification.needs_repair() {
            return DirectPar2Resolution::Clean;
        }
        match self
            .repair_direct_sets_with_par2_damage(job_id, &verification)
            .await
        {
            true => DirectPar2Resolution::Repaired,
            false => DirectPar2Resolution::Unresolved,
        }
    }

    /// One verification pass over the job's recovery set, reading every live
    /// direct volume virtually and emitting nothing.
    ///
    /// The verdict is **adjusted before it is returned**, by exactly the two
    /// rules the authoritative pass applies to its own
    /// ([`Pipeline::apply_direct_damage_adjustments`]). Skipping them was not a
    /// small omission: a job with a *finalized* direct set beside a live damaged
    /// one reads every finalized volume as `Missing` here, `damaged_files_by_set`
    /// finds no live owner for them and refuses the whole attempt with
    /// `DamageOutsideDirectSets` — so the live set demotes for damage that
    /// belongs to files the job legitimately finished without, which is precisely
    /// the case phase 6 exists to repair (phase 6 review, F2).
    ///
    /// Deliberately a **full** `verify_all`, never `WEAVER_PAR2_FAST_VERIFY`'s
    /// sampled form: fast-verify's per-slice accounting is what phase 6 narrowed
    /// a repair's size away from, and a sampled span would re-inflate it. Weaver
    /// sets the flag on no path that can reach here; the pin is in
    /// [`super::repair`]'s module docs.
    async fn verify_direct_sets_quietly(
        &self,
        job_id: JobId,
        par2_set: std::sync::Arc<weaver_par2::Par2FileSet>,
        working_dir: PathBuf,
    ) -> Option<weaver_par2::VerificationResult> {
        let overlay = self.direct_par2_overlay(job_id)?;
        let pp_pool = self.pp_pool.clone();
        let volumes = overlay.volumes.clone();
        let provider = overlay.provider;
        let mut verification = tokio::task::spawn_blocking(move || {
            pp_pool.install(move || {
                // No placement scan: the direct volumes are absent from the
                // directory by construction and every other file is at its
                // declared name, which is the same assumption the repair's own
                // fallback access makes.
                let plan = weaver_par2::PlacementPlan {
                    exact: volumes.iter().map(|volume| volume.par2_file_id).collect(),
                    swaps: Vec::new(),
                    renames: Vec::new(),
                    unresolved: Vec::new(),
                    conflicts: Vec::new(),
                };
                let inner =
                    weaver_par2::PlacementFileAccess::from_plan(working_dir, &par2_set, &plan);
                let access =
                    super::par2_access::DirectVolumeFileAccess::new(inner, provider, &volumes);
                weaver_par2::verify_all(&par2_set, &access)
            })
        })
        .await
        .ok()?;
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
        Some(verification)
    }

    /// D8's repair-while-direct. `false` means nothing was repaired and the
    /// caller should fall back to demotion.
    async fn repair_direct_sets_with_par2_damage(
        &mut self,
        job_id: JobId,
        verification: &weaver_par2::VerificationResult,
    ) -> bool {
        let Some(par2_set) = self.par2_set(job_id).cloned() else {
            return false;
        };
        let Some(overlay) = self.direct_par2_overlay(job_id) else {
            return false;
        };
        // The same settle guard the demotion path carries, in the same shape
        // (H3): while articles are in flight a set's outstanding ranges read as
        // holes, and PAR2 cannot tell a hole from corruption. Repairing on that
        // verdict would spend recovery blocks rebuilding bytes that are still on
        // their way. A set whose volumes have all finished downloading is
        // settled whatever the rest of the job is doing, which is what keeps a
        // job with one slow conventional file from blocking its RAR set's
        // repair.
        let payload_settled = !self.job_has_pending_download_pipeline_work(job_id);
        if matches!(
            verification.repairable,
            weaver_par2::verify::Repairability::NotNeeded
        ) {
            return false;
        }

        let by_set = match super::repair::damaged_files_by_set(verification, |file_id| {
            overlay.owner_of(file_id)
        }) {
            Ok(by_set) => by_set,
            Err(failure) => {
                Self::record_direct_repair_failure(job_id, &failure);
                return false;
            }
        };
        if by_set.is_empty() {
            return false;
        }

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
                    return repaired_any || already_demoted;
                }
            }
        }
        repaired_any
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
        par2_set: &std::sync::Arc<weaver_par2::Par2FileSet>,
        verification: &weaver_par2::VerificationResult,
        overlay: &DirectPar2Overlay,
        files: &[weaver_par2::FileId],
    ) -> Result<(), super::repair::DirectRepairFailure> {
        let slice_size = par2_set.slice_size;
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Err(super::repair::DirectRepairFailure::DamageOutsideDirectSets);
        };
        let set_name = set.set_name().to_string();
        let holds_budget = set.holds_budget();

        // The damaged volumes, in the set's own volume space. `overlay` is keyed
        // by the job's file index, which is what makes one provider answer for
        // every set of a job; the set's plan translates back.
        let mut damaged = Vec::new();
        for file_id in files {
            let Some(file_index) = overlay.file_index_of(file_id) else {
                return Err(super::repair::DirectRepairFailure::DamageOutsideDirectSets);
            };
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
                    covered: set.volume_coverage(volume_index),
                    crcs: set.volume_crc_runs(volume_index),
                },
            });
        }
        if damaged.is_empty() {
            return Err(super::repair::DirectRepairFailure::DamageOutsideDirectSets);
        }
        // Sized **before** anything is materialized, read or deleted, so an
        // over-budget repair costs the set nothing and demotes with a name (F3).
        // Every repaired byte re-enters the router as a hold, so the holds
        // budget is the ceiling it is charged against; reading first and finding
        // out afterwards is what let a three-volume rewrite of a large set peak
        // at gigabytes with nothing bounding it.
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

        // D8, step 1: the row goes **before** any byte the row claims changes.
        // The materialization writes only scratch, but the re-route below
        // rewrites member partials and envelopes at offsets the checkpoint's
        // floors cover, and a row that outlived that would let a restart trust
        // floors over bytes that moved underneath them.
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
        // A repair rewrites bytes the live verifier already claimed as good
        // blocks, so its state for this job is retired rather than trusted —
        // the same stance `run_par2_repairer` takes for a conventional repair.
        self.live_par2.remove_job(job_id);

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
        let inner_plan = weaver_par2::PlacementPlan {
            exact: Vec::new(),
            swaps: Vec::new(),
            renames: Vec::new(),
            unresolved: Vec::new(),
            conflicts: Vec::new(),
        };
        let inner = weaver_par2::PlacementFileAccess::from_plan(
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
            .route_repaired_volumes(job_id, set_index, &damaged)
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
                .place_direct_spans(job_id, set_index, &spans, None)
                .await
            {
                return Err(super::repair::DirectRepairFailure::ExecuteFailed(
                    "a confirming parse's spans could not be written".to_string(),
                ));
            }
        }
        self.reread_direct_stale_gaps(job_id, set_index).await;
        // D8's other half: the row was deleted before anything moved, so the set
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
        Ok(())
    }

    /// Reads each repaired volume's spans back and feeds them through the router
    /// and out to every destination they touch (D3's replacement semantics).
    ///
    /// **One volume at a time, read then routed then dropped.** Both halves of
    /// that are load-bearing and they pull in opposite directions:
    ///
    /// - a whole volume at once, because the classification frontier is a
    ///   per-volume fact — a span routed before its volume's end record was
    ///   staged would be held rather than routed, and the repair would refuse;
    /// - never more than one volume, because the spans are bytes, and holding
    ///   every damaged volume's rewrite so the last one could be staged put the
    ///   set's whole repair in RAM twice over with nothing bounding it (F3).
    async fn route_repaired_volumes(
        &mut self,
        job_id: JobId,
        set_index: usize,
        damaged: &[super::repair::DamagedDirectVolume],
    ) -> bool {
        for volume in damaged {
            let volume_index = volume.volume_index;
            let for_task = volume.clone();
            let spans = match tokio::task::spawn_blocking(move || {
                super::repair::read_repaired_spans(&for_task)
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
                set.route_repaired(volume_index, &staged)
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
                    self.demote_direct_set(job_id, set_index, reason, None)
                        .await;
                    return false;
                }
            };
            if !self
                .place_direct_spans(job_id, set_index, &routed, None)
                .await
            {
                return false;
            }
        }
        true
    }

    /// Closes the composition gaps a repair's rewrite left, with one bounded
    /// read of the partials that hold them (D4).
    ///
    /// The shape is deliberately D6's restart re-arm: the same plan, the same
    /// reader, the same "a run that will not read demotes rather than passes"
    /// rule. What differs is only why the value is missing — a rewrite discarded
    /// it rather than a restart losing it — and that difference has no bearing
    /// on what it costs to recover.
    async fn reread_direct_stale_gaps(&mut self, job_id: JobId, set_index: usize) {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        if set.is_demoted() || !set.router.has_stale_gaps() {
            return;
        }
        let working_dir = set.plan().working_dir.clone();
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
        let read_dir = working_dir.clone();
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
                self.demote_direct_set(
                    job_id,
                    set_index,
                    DemotionReason::RepairGapUnreadable,
                    None,
                )
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
            self.demote_direct_set(job_id, set_index, reason, None)
                .await;
            return;
        }
        // Same terminating condition as the restart re-arm (M4): the pass read
        // every run the plan named, so a gap that survives it is one no plan
        // reached, and re-running would reach the same place. One pass, then a
        // verdict.
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
            self.demote_direct_set(job_id, set_index, DemotionReason::RepairGapUnreadable, None)
                .await;
        }
    }

    /// Demotes every direct set the PAR2 pass found damage on, and reports
    /// whether any did (plan 135, D5/D8).
    ///
    /// Phase 6's fallback, and phase 5's whole answer. A demoted set
    /// materializes its volumes from its own routed bytes, refetches whatever
    /// reconstruction could not verify, and hands the job to the conventional
    /// repair path — which is exactly the shape the same job would have had with
    /// the gate off.
    pub(crate) async fn demote_direct_sets_with_par2_damage(
        &mut self,
        job_id: JobId,
        verification: &weaver_par2::VerificationResult,
    ) -> bool {
        let Some(overlay) = self.direct_par2_overlay(job_id) else {
            return false;
        };
        // H3's second guard, paired with
        // [`Self::direct_sets_ready_for_authoritative_par2`]: while articles are
        // still arriving, a set's outstanding ranges read as holes and PAR2
        // calls them damage. The caller is supposed to have deferred already, so
        // this is the belt to that braces — and it is scoped the same way, so a
        // set whose bytes are genuinely never coming still demotes and still
        // gets materialized for the conventional repair path.
        let payload_settled = !self.job_has_pending_download_pipeline_work(job_id);
        let mut damaged: Vec<(usize, String)> = Vec::new();
        for file in &verification.files {
            if matches!(
                file.status,
                weaver_par2::verify::FileStatus::Complete
                    | weaver_par2::verify::FileStatus::Renamed(_)
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
            self.demote_direct_set(job_id, set_index, DemotionReason::Par2Damaged, None)
                .await;
            demoted = true;
        }
        demoted
    }

    /// Demotes every set of `job_id` that is still routing, because a PAR2
    /// **repair** is about to run and repair needs a file to write into.
    ///
    /// Returns whether anything demoted, so the caller can let the job go round
    /// again over materialized volumes rather than repairing against nothing.
    pub(crate) async fn demote_live_direct_sets_for_par2_repair(&mut self, job_id: JobId) -> bool {
        let live: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| !set.is_demoted() && !set.is_finalized())
            .map(|(index, _)| index)
            .collect();
        if live.is_empty() {
            return false;
        }
        for set_index in live {
            self.demote_direct_set(job_id, set_index, DemotionReason::Par2Damaged, None)
                .await;
        }
        true
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
        let bytes = contiguous_bytes(&segment.data);
        // The article the seam is holding: if the set demotes here it is
        // dropped without ever reaching the assembly, so nothing else would
        // ever ask for it again (B4).
        let dropped = Some((segment_id, u64::from(decoded_size)));

        let routed = {
            let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
                return DirectRouteOutcome::Demoted;
            };
            set.note_volume_part_crc(volume_index, file_offset, u64::from(decoded_size), part_crc);
            // The decoded geometry of this article, which only the decoder
            // knows: demotion-by-reconstruction uses it to decide which articles
            // it does *not* have to fetch again (D8).
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
                self.demote_direct_set(job_id, set_index, reason, dropped)
                    .await;
                return DirectRouteOutcome::Demoted;
            }
        };

        // Before the writes, so a fact can never be newer on disk than in the
        // cache the restart reader rebuilds from (D6). Cheap when nothing
        // parsed: a set parses a volume once provisionally and once
        // confirmingly, so this is two writes per volume for the life of the job.
        self.cache_direct_volume_facts(job_id, set_index).await;

        if !self
            .place_direct_spans(job_id, set_index, &spans, dropped)
            .await
        {
            return DirectRouteOutcome::Demoted;
        }

        // Ordering contract (D5): every routed destination write for this span
        // has returned, so a later settle read of the same range sees exactly
        // these bytes. Live PAR2 is defined in *source volume* space, which is
        // what `file_offset` already is.
        //
        // With par2-bearing jobs refused at admission (B2), this only ever runs
        // for a job whose spec declares no PAR2 file, where the registry's
        // first call answers `skip_job` and every later one is a set lookup.
        // The call stays because the refusal is phase 4's, not the seam's:
        // phase 5 admits those jobs and this is where their coverage comes from.
        //
        // Plan 136, E1 review F3: an encrypted set is kept out of live
        // verification entirely. These bytes are honest — posted cipher, taken
        // before the write transform — but a block straddling an article
        // boundary is only ever settled by a read-back, and the read-back
        // resolves to the member's plaintext partial. Feeding a stream whose
        // every boundary block must come back `Bad` is worse than not feeding
        // one, so the refusal is on both halves and `direct_virtual_volume` is
        // the other.
        if !self.direct_volume_routes_encrypted(file_id) {
            self.note_live_par2_segment(file_id, file_offset, &segment.data);
        }
        drop(bytes);

        self.commit_direct_segment(segment_id, decoded_size, set_index, volume_index)
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
        spans: &[RoutedSpan],
        dropped: Option<(SegmentId, u64)>,
    ) -> bool {
        if spans.is_empty() {
            return true;
        }
        let batches = self.direct_write_batches(job_id, set_index, spans);
        if let Err(path) = self.prepare_direct_destinations(job_id, &batches).await {
            // D3: a destination that could not be marked sparse is refused
            // *before* it holds a hole, so nothing has been allocated for it
            // yet. Demote and let the conventional path own the bytes.
            warn!(
                job_id = job_id.0,
                path = %path.display(),
                "could not mark a direct-store destination sparse; demoting the set"
            );
            self.demote_direct_set(job_id, set_index, DemotionReason::SparseMarkFailed, dropped)
                .await;
            return false;
        }
        if let Err(error) = crate::pipeline::orchestrator::write_direct_batches(batches).await {
            // A destination write failure is a demotion, not a job failure: the
            // conventional path writes the same bytes to a different file, and
            // only if *that* also fails is the job genuinely unfinishable (M6).
            warn!(
                job_id = job_id.0,
                error = %error,
                "direct-store destination write failed; demoting the set"
            );
            self.demote_direct_set(
                job_id,
                set_index,
                DemotionReason::DestinationWriteFailed,
                dropped,
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
        // Where the bytes went, split by destination kind (plan 135,
        // Observability). Two counters answer the question the disk acceptance
        // target is stated in: how much of a set landed at its final offset
        // versus how much rode the envelope as service data. Summed over the
        // spans already in hand, and `record_value` is a no-op unless the
        // profiler is on.
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
    /// can rebuild its layout (D6).
    ///
    /// The rows go into `active_rar_volume_facts` — the same table, keyed the
    /// same way, that the conventional path fills from a parsed volume file.
    /// There is no writer conflict: `try_update_archive_topology` needs a file to
    /// parse and D7 suppresses it for direct volumes, so for a live direct set
    /// this is the only writer, and after a demotion the conventional path
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

    /// D6's gate re-arm: recomputes the member CRC for every restart-seeded
    /// range with **one sequential read** of the partials that hold them.
    ///
    /// `CrcRuns` does not survive a restart, so the bytes a previous run wrote
    /// are covered and unverified; the whole-member gate refuses to compose over
    /// them until they are re-read. This is D6's "PAR2 absent" arm — the direct
    /// analogue of `checksum_completed_file`'s fallback for physical files, at
    /// the same cost and the same assurance. It deliberately verifies what is on
    /// **disk now**, so a byte corrupted while the process was down fails the
    /// member gate and demotes the set instead of being committed.
    ///
    /// Runs **once** per set: every run it reads leaves the seeded set, so a
    /// second call finds nothing to do — and if anything is still seeded after a
    /// full pass, the set demotes rather than being re-read on every completion
    /// check for the life of the job (M4).
    async fn rearm_restart_seeded_gates(&mut self, job_id: JobId, set_index: usize) {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        if set.is_demoted() || set.is_finalized() || !set.has_restart_seeded_coverage() {
            return;
        }
        let working_dir = set.plan().working_dir.clone();
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
        let read_dir = working_dir.clone();
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
                self.demote_direct_set(
                    job_id,
                    set_index,
                    DemotionReason::RestartRereadFailed,
                    None,
                )
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
                self.demote_direct_set(
                    job_id,
                    set_index,
                    DemotionReason::RestartRereadFailed,
                    None,
                )
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
            self.demote_direct_set(job_id, set_index, reason, None)
                .await;
            return;
        }

        // M4's terminating condition. The pass above read every run the plan
        // named, so nothing may still be seeded — a range that survives it is one
        // no plan reached, and re-running the pass would read the same runs and
        // reach the same place. Left alone, `try_verify_member` refuses that
        // member forever while the completion gate calls this back on every
        // check: a zombie that costs I/O. One pass, then a verdict.
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
            self.demote_direct_set(
                job_id,
                set_index,
                DemotionReason::RestartRearmUnplaceable,
                None,
            )
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
        let working_dir = set.plan().working_dir.clone();
        let partials: HashMap<u32, String> = set
            .router
            .member_partials()
            .into_iter()
            .map(|(member_id, _, partial)| (member_id, partial.to_string()))
            .collect();

        let mut grouped: HashMap<PathBuf, Vec<(u64, Vec<u8>)>> = HashMap::new();
        for span in spans {
            let relative = match span.destination {
                DirectDestination::Member { member_id } => match partials.get(&member_id) {
                    Some(path) => path.clone(),
                    None => continue,
                },
                // Envelope v2: one file per volume, written at true physical
                // offsets. The owner thread seeks to the offset and writes, so
                // the gaps member routing carried away are ordinary filesystem
                // holes on every platform that gives them for free. Windows
                // needs `FSCTL_SET_SPARSE` at creation to get the same, which is
                // phase 7.
                DirectDestination::Envelope { volume_index } => {
                    set.plan().envelope_relative_path(volume_index)
                }
            };
            grouped
                .entry(working_dir.join(relative))
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
    /// creates the destination file itself **marked sparse**, once per job
    /// (B3, and D3's Windows rule).
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
    /// `Err(path)` names the first destination that could not be marked. The
    /// caller demotes; nothing has a hole yet.
    async fn prepare_direct_destinations(
        &mut self,
        job_id: JobId,
        batches: &crate::pipeline::orchestrator::DirectWriteBatches,
    ) -> Result<(), PathBuf> {
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

    /// The D7-suppressed twin of `commit_persisted_segment`.
    async fn commit_direct_segment(
        &mut self,
        segment_id: SegmentId,
        decoded_size: u32,
        set_index: usize,
        volume_index: u32,
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
            // D3: a duplicate must not advance CRC composition, coverage or
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
            let _ = self
                .event_tx
                .send(PipelineEvent::SegmentCommitted { segment_id });
        }
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

        // D5: the live verifier's fail-safe length verdict, which the
        // conventional file-complete path feeds at exactly this point. A direct
        // volume has no file to `stat`, but `received_bytes` is the decoded
        // length — the space PAR2 describes — so the check is the same one, from
        // the same number, and a volume whose decoded length disagrees with its
        // description retires its live state instead of short-circuiting on
        // blocks that hashed clean against the wrong file.
        self.note_live_par2_file_complete(file_id, total_bytes);

        // The file-complete state a physical volume drops here (M5/M7). Every
        // one of these is keyed by a file that will never exist, so leaving
        // them behind leaks for the life of the job and, worse, leaves
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

        // M4: the yEnc whole-volume gate, composed rather than re-read.
        //
        // A physical volume is checked against its `=yend crc32` trailer when
        // the file completes; the per-article part CRC32s compose into exactly
        // that value, so the gate survives with no file to read. A mismatch
        // demotes: `schedule_file_crc_recovery` is deliberately *not* wired
        // here, because it replaces segments by rewriting a physical file, and
        // phase 5's provider is what gives a direct volume one.
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
            self.demote_direct_set(job_id, set_index, DemotionReason::VolumeCrcMismatch, None)
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
                self.demote_direct_set(job_id, set_index, reason, None)
                    .await;
                return;
            }
            // The confirming parse can make the volume's trailing region
            // routable — it was held until the parse proved no further header
            // could appear there — so those spans are written here, before the
            // set is allowed to finalize and delete its envelopes.
            Some(Ok(spans)) => {
                self.cache_direct_volume_facts(job_id, set_index).await;
                if !self
                    .place_direct_spans(job_id, set_index, &spans, None)
                    .await
                {
                    return;
                }
            }
            None => return,
        }

        // D6's phase-change demand. The set's download phase ends exactly here,
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
        // D6's gate re-arm, and deliberately **before** the PAR2 wait: the
        // re-read is about the member gates, not about verification, and running
        // it at the download/verify boundary means a par2-bearing set is already
        // gate-passed the moment its job's verification concludes. A set still
        // receiving articles is skipped — its unwritten ranges are holes, not
        // coverage to verify.
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
    }

    /// Whether a direct set must keep its envelopes and partials because the
    /// job's PAR2 verification has not concluded (plan 135, D5).
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
        if self.par2_set(job_id).is_some() {
            return true;
        }
        self.job_has_pending_download_pipeline_work(job_id)
    }

    /// Polls the automatic barrier triggers for every live set. Called from the
    /// orchestrator's existing periodic seam.
    pub(crate) async fn poll_direct_store_barriers(&mut self) {
        let now = Instant::now();
        for job_id in self.direct_store.active_jobs() {
            let due: Vec<(usize, super::barrier::BarrierTrigger)> = self
                .direct_store
                .sets_for(job_id)
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
        let working_dir = set.plan().working_dir.clone();
        // Read before the barrier runs, which resets it. Two numbers, because
        // the interesting one is the second: the barrier's 256 MiB trigger is
        // checked per routed batch, so anything above it is the overshoot D6
        // bounds to "one decoded write batch" — and an overshoot that starts
        // tracking set size instead is the shape that regression looks like.
        let dirty_bytes = set.dirty_bytes();
        let touched: Vec<String> = set
            .touched_paths()
            .into_iter()
            .filter_map(|path| {
                path.strip_prefix(&working_dir)
                    .ok()
                    .map(|relative| relative.to_string_lossy().replace('\\', "/"))
            })
            .collect();

        // Every sync is queued to its owner thread before any of them is
        // awaited (M4). Envelope v2 made this set `members + volumes` rather
        // than two, and one `await` per destination serialized that many
        // independent fsyncs on the pipeline task; the barrier's contract only
        // asks that they have all completed before it persists, not that they
        // happened one after another.
        let paths: Vec<PathBuf> = touched
            .iter()
            .map(|relative| working_dir.join(relative))
            .collect();
        let outcomes = crate::pipeline::orchestrator::sync_direct_destinations(paths).await;
        let results: HashMap<String, Result<(), String>> = touched
            .into_iter()
            .zip(outcomes)
            .map(|(relative, outcome)| (relative, outcome.map_err(|error| error.to_string())))
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
    /// immediately and may not be visible at all. Plan 135's open question 2
    /// settles what to do about that: **document it, add no synthetic delay,
    /// and change no GraphQL surface.** The README carries the user-facing
    /// wording; the rule for this function is that it must not slow down, and
    /// must not emit a phase it did not really run, to make the UI look more
    /// familiar. A set that demotes reports a real extraction phase because it
    /// really runs one.
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
        let working_dir = set.plan().working_dir.clone();
        // Repair scratch rides with the envelopes: both are the set's own
        // working files, both are meaningless once the members are committed,
        // and a repair that ran earlier in this job has already deleted its own.
        let mut envelopes = set.plan().envelope_paths();
        envelopes.extend(set.plan().repair_paths());
        // `member_partials` is in **archive order** — `(first volume, physical
        // offset)` — and the commit loop below walks it in that order, so two
        // members whose names sanitize to the same destination overwrite each
        // other exactly the way the incremental extractor makes them overwrite
        // each other: last one in the archive wins (D3).
        let unpacked_sizes: HashMap<String, u64> =
            set.router.member_digest_entries().into_iter().collect();
        let members: Vec<(String, u64, PathBuf, PathBuf)> = set
            .router
            .member_partials()
            .into_iter()
            .filter_map(|(_, name, partial)| {
                let destination = set.plan().member_output_path(name).ok()?;
                Some((
                    name.to_string(),
                    unpacked_sizes.get(name).copied().unwrap_or(0),
                    working_dir.join(partial),
                    destination,
                ))
            })
            .collect();
        let extracted_members = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        set.assert_not_extraction_owned(&extracted_members);

        // D1's tolerance, and strictly **before** the commit loop below (M4).
        // The extraction reads the *virtual volumes*, which are the envelopes
        // overlaid with the members' `.direct.partial`s — so every one of those
        // files has to still be where the provider says it is. Running it after
        // the renames pointed the provider's partial map at paths that had just
        // been renamed away, turning every stored member's extent into a hole:
        // it happened to work only while a tolerated member's header walk and
        // decode never read through a stored extent, and its failure mode was a
        // demotion that could no longer reconstruct, i.e. a full redownload.
        //
        // Nothing here needs the commit to have happened: the overwrite refusal
        // compares `plan().member_output_path` against the tolerated
        // destinations, which is derived from the layout and not from the
        // filesystem. It still has to run before the envelopes are deleted.
        match self.extract_tolerated_members(job_id, set_index).await {
            Ok(extracted) => {
                for name in extracted {
                    self.extracted_members
                        .entry(job_id)
                        .or_default()
                        .insert(name);
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
                    None,
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
                self.demote_direct_set(job_id, set_index, DemotionReason::FinalizationFailed, None)
                    .await;
                return;
            }
            // A zero-length stored member never had a byte routed for it, so it
            // has no partial to rename — but the archive declares the file and
            // the conventional extractor creates it, so finalization does too,
            // in the same archive order as every other member (B2).
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
                self.demote_direct_set(job_id, set_index, DemotionReason::FinalizationFailed, None)
                    .await;
                return;
            }
            self.extracted_members
                .entry(job_id)
                .or_default()
                .insert(name.clone());
        }

        for envelope in &envelopes {
            crate::pipeline::release_cached_write_handle(envelope);
            let _ = tokio::fs::remove_file(envelope).await;
        }
        // D2: the scratch dies with the set, and its high-water is reported
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
        info!(
            job_id = job_id.0,
            set_name = %set_name,
            members = members.len(),
            "direct-store set finalized without materializing a volume"
        );
    }

    /// D1's bounded small-member tolerance: extracts **only** the tolerated
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
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Ok(Vec::new());
        };
        let names = set.router.tolerated_member_names();
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let set_name = set.set_name().to_string();

        // The ordering invariant this extraction depends on, stated where it is
        // depended on (M4). The provider serves every stored member's extent out
        // of its `.direct.partial`; the commit loop renames those away and
        // records the member in `extracted_members` as it goes. Running after it
        // therefore hands the header walk and the decode a volume whose stored
        // extents are all holes, and the failure path costs a full redownload.
        debug_assert!(
            !self
                .extracted_members
                .get(&job_id)
                .is_some_and(|committed| {
                    set.router
                        .member_partials()
                        .iter()
                        .any(|(_, name, _)| committed.contains(*name))
                }),
            "a stored member of {set_name} was committed before its set's tolerated \
             members were extracted; the virtual volumes no longer resolve"
        );

        // Every stored member's *eventual* destination, so the assertion below
        // compares resolved paths rather than raw header names — two names can
        // sanitize onto one path, which is exactly the collision that would let
        // a tolerated member overwrite verified direct output. Derived from the
        // layout, not from the filesystem, which is what lets this run before
        // the commit loop renames anything (M4).
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

        for (_, destination) in &targets {
            if let Some(parent) = destination.parent()
                && let Err(error) = tokio::fs::create_dir_all(parent).await
            {
                return Err(format!(
                    "failed to create {} for a tolerated member: {error}",
                    parent.display()
                ));
            }
        }

        let extracted = tokio::task::spawn_blocking(move || {
            let reader = provider
                .open(first_volume)
                .ok_or_else(|| format!("virtual volume {first_volume} is not registered"))?;
            let mut archive = weaver_unrar::RarArchive::open(reader)
                .map_err(|error| format!("failed to open the virtual archive: {error}"))?;
            // The same decode ceilings the incremental extractor applies. A
            // tolerated member is small by budget, but the *declared* dictionary
            // in a hostile header is not bounded by anything the budget checks.
            crate::pipeline::extraction::apply_server_rar_limits(&mut archive);
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
            let options = weaver_unrar::ExtractOptions {
                verify: true,
                password: None,
                restore_owners: false,
            };
            let mut produced = Vec::with_capacity(targets.len());
            for (name, destination) in &targets {
                let index = archive
                    .find_member(name)
                    .ok_or_else(|| format!("tolerated member '{name}' is not in the archive"))?;
                let base = archive
                    .member_info(index)
                    .map(|member| member.volumes.first_volume as u32)
                    .unwrap_or(0);
                // `extract_member_streaming` addresses volumes relative to the
                // member's first one; the set speaks absolute indices.
                let scoped = provider.rebased(base);
                let mut file = std::fs::File::create(destination).map_err(|error| {
                    format!("failed to create {}: {error}", destination.display())
                })?;
                archive
                    .extract_member_streaming(index, &options, &scoped, &mut file)
                    .map_err(|error| format!("failed to extract '{name}': {error}"))?;
                // The tolerated half of the byte account: everything else a
                // direct set produces is counted at the router as
                // `direct_store.bytes.member`, and a set whose tolerated bytes
                // start rivalling its stored ones is one the D1 budget is no
                // longer holding.
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
            "extracted D1-tolerated members from the virtual volumes"
        );
        Ok(extracted)
    }

    /// Abandons direct output for a set and hands its volumes back (D8's
    /// **archive-group demotion**, the transition that ends direct mode).
    ///
    /// Two shapes, and the first one is tried first:
    ///
    /// 1. **Reconstruction.** Every volume is rebuilt byte-exactly from the
    ///    envelope plus the member extents, its covered runs are verified against
    ///    the yEnc part-CRC composition, and only then are legacy floors and
    ///    completed-file rows persisted, the coverage row retired, and the
    ///    partials and envelopes deleted. Covered bytes are never refetched.
    /// 2. **Refetch** — phase 4's conservative form, kept as the fallback for
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
        dropped: Option<(SegmentId, u64)>,
    ) {
        let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
            return;
        };
        // One cleanup per set, and never for a finalized one. The original
        // guard read `is_demoted() && is_finalized()`, which two mutually
        // exclusive states can never both satisfy, so a finalized set could be
        // flipped to `Demoted` and have its committed members deleted out from
        // under a job that had already counted them (B5).
        if !set.claim_demotion(reason) {
            return;
        }
        let set_name = set.set_name().to_string();

        crate::runtime::perf_probe::record_owned(
            format!("direct_store.demoted.{}", reason.metric()),
            std::time::Duration::from_nanos(1),
        );
        warn!(
            job_id = job_id.0,
            set_name = %set_name,
            reason = reason.metric(),
            "direct-store set demoted"
        );

        match self
            .reconstruct_demoted_set(job_id, set_index, dropped)
            .await
        {
            Ok(volumes) => {
                crate::runtime::perf_probe::record(
                    "direct_store.demoted.reconstructed",
                    std::time::Duration::from_nanos(1),
                );
                // The other half of D8's materialization account: a whole-set
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
                self.refetch_demoted_set(job_id, set_index, dropped).await;
            }
        }
    }

    /// D8's reconstruction path. `Ok(n)` when `n` volumes were materialized.
    async fn reconstruct_demoted_set(
        &mut self,
        job_id: JobId,
        set_index: usize,
        dropped: Option<(SegmentId, u64)>,
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
        if set.router.routes_encrypted() {
            // Plan 136, E-D2/E-D4. The member partials hold **plaintext**; the
            // volume being rebuilt holds cipher. Reading one back as the other
            // is the one way this path can put wrong bytes under a published
            // floor, and the yEnc part CRCs it checks against describe the
            // posted bytes, so the mismatch would surface as a refusal rather
            // than as a corrupt volume — but only after writing. Refuse up
            // front; phase E2's re-encrypting overlay is what makes this path
            // available to an encrypted set.
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
            let coverage = set.volume_coverage(*volume_index);
            // A volume that never completed has no authoritative length; its
            // received bytes are the most that can be on disk, which is exactly
            // what bounds the sweep. A restart-seeded volume's received bytes
            // are the spec's yEnc-encoded sizes and would overstate it, so the
            // coverage map answers for those instead.
            let len = set.virtual_volume_len(*volume_index, received);
            lengths.insert(*volume_index, len);
            extents_by_volume.insert(*volume_index, set.segment_extents(*volume_index));
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
                    crcs: set.volume_crc_runs(*volume_index),
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
        // reconciliation mutates durable state, in D8's order: legacy floors and
        // completed-file rows, then the coverage row, then the direct outputs.
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
                // floor (D8). `note_file_progress_floor` suppresses direct source
                // files, and this one still is one until the set's status is read
                // again — so the upsert goes straight to the batch the flush
                // drains, which is the same row `coverage_skip_plan` and
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
        self.requeue_after_reconstruction(job_id, set_index, &keep, dropped)
            .await;
        Ok(materialized)
    }

    /// Phase 4's demotion, kept as the fallback: throw the routed bytes away and
    /// hand every article back to the download queue.
    async fn refetch_demoted_set(
        &mut self,
        job_id: JobId,
        set_index: usize,
        dropped: Option<(SegmentId, u64)>,
    ) {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        let volumes: Vec<u32> = set.plan().volumes.values().copied().collect();

        // D8: on this path the checkpoint row goes first, because everything it
        // claims is about to be deleted and nothing replaces it. A crash between
        // here and the refetch costs a redownload, which is what the fallback is
        // doing anyway.
        let mut persist = DatabaseCoveragePersist::new(self.db.clone());
        if let Some(set) = self.direct_store.set_mut(job_id, set_index)
            && let Err(error) = set.retire(&mut persist)
        {
            warn!(job_id = job_id.0, error = %error, "failed to retire a demoted direct-store checkpoint");
        }

        self.delete_direct_outputs(job_id, set_index).await;
        self.refetch_direct_volumes(job_id, &volumes, dropped).await;
    }

    /// Deletes a set's partial members, envelope files and holds scratch.
    ///
    /// A sparse half-written output would masquerade as finished work (D1), and
    /// the envelopes and the scratch are scratch by construction.
    async fn delete_direct_outputs(&mut self, job_id: JobId, set_index: usize) {
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            set.router.discard_scratch();
        }
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        let working_dir = set.plan().working_dir.clone();
        let mut doomed: Vec<PathBuf> = set
            .router
            .member_partials()
            .into_iter()
            .map(|(_, _, partial)| working_dir.join(partial))
            .collect();
        doomed.extend(set.plan().envelope_paths());
        // Repair scratch (D8). Normally deleted the moment its spans are routed,
        // so this only ever finds one a demotion interrupted — but a leftover
        // would sit in the working directory for the life of the job, and the
        // reconstruction sweep is about to write the real volume files beside it.
        doomed.extend(set.plan().repair_paths());
        for path in doomed {
            crate::pipeline::release_cached_write_handle(&path);
            let _ = tokio::fs::remove_file(&path).await;
        }
    }

    /// Hands a reconstructed set back to the conventional path, keeping the
    /// articles that are now genuinely on disk.
    ///
    /// This is the difference between D8's demotion and phase 4's: `keep` names,
    /// per NZB file, the articles whose decoded extents lie wholly below the
    /// contiguous prefix the sweep rebuilt. Those stay committed in the assembly
    /// and are never fetched again. Everything else — an article held in RAM and
    /// never written, an article above a coverage hole, the one article the
    /// routing seam dropped — comes back, exactly as the refetch path would have
    /// brought it back.
    ///
    /// A file with nothing kept takes the full refetch treatment, including
    /// `mark_file_incomplete`: there is no reconstructed state to protect.
    async fn requeue_after_reconstruction(
        &mut self,
        job_id: JobId,
        set_index: usize,
        keep: &HashMap<u32, Vec<u32>>,
        dropped: Option<(SegmentId, u64)>,
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
                let file_extents = extents.get(file_index).cloned().unwrap_or_default();
                for segment_number in &kept {
                    let Some((_, len)) = file_extents.get(segment_number).copied() else {
                        continue;
                    };
                    if let Some(file_asm) = state.assembly.file_mut(file_id)
                        && file_asm.commit_segment(*segment_number, len as u32).is_ok()
                    {
                        kept_bytes = kept_bytes.saturating_add(len);
                    }
                }
                lost_bytes =
                    lost_bytes.saturating_add(previously_received.saturating_sub(kept_bytes));

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
                    let was_dropped = dropped.is_some_and(|(id, _)| id == segment_id);
                    if !committed.contains(&segment.ordinal) && !was_dropped {
                        continue;
                    }
                    work.push(DownloadWork {
                        segment_id,
                        message_id: crate::jobs::ids::MessageId::new(&segment.message_id),
                        groups: file.groups.clone(),
                        priority: file.role.download_priority(),
                        byte_estimate: segment.bytes,
                        retry_count: 0,
                        is_recovery: false,
                        exclude_servers: vec![],
                        avoid_server: None,
                    });
                }
            }
            let dropped_bytes = dropped.map(|(_, bytes)| bytes).unwrap_or(0);
            state.downloaded_bytes = state
                .downloaded_bytes
                .saturating_sub(lost_bytes.saturating_add(dropped_bytes));
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
    /// routed into direct destinations that have just been deleted, plus the
    /// one article the routing seam dropped without ever committing it. A
    /// segment still sitting in a queue, still in flight, or waiting on a
    /// scheduled retry is left alone — the set is demoted, so when it lands it
    /// takes the conventional path by itself, and requeueing it would fetch the
    /// same article from the server twice.
    ///
    /// The job's byte counter is *adjusted*, never zeroed: it is job-wide, and
    /// the other files' contribution to it has nothing to do with this set.
    async fn refetch_direct_volumes(
        &mut self,
        job_id: JobId,
        file_indices: &[u32],
        dropped: Option<(SegmentId, u64)>,
    ) {
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
                    // Committed articles lost their bytes with the partials;
                    // the dropped one never reached the assembly at all. Every
                    // other segment is somebody else's outstanding work.
                    let committed = file_asm.has_segment(segment.ordinal);
                    let was_dropped = dropped.is_some_and(|(id, _)| id == segment_id);
                    if !committed && !was_dropped {
                        continue;
                    }
                    work.push(DownloadWork {
                        segment_id,
                        message_id: crate::jobs::ids::MessageId::new(&segment.message_id),
                        groups: file.groups.clone(),
                        priority: file.role.download_priority(),
                        byte_estimate: segment.bytes,
                        retry_count: 0,
                        is_recovery: false,
                        exclude_servers: vec![],
                        avoid_server: None,
                    });
                }
                if let Some(file_asm) = state.assembly.file_mut(file_id) {
                    file_asm.reset();
                }
            }
            // The dropped article was counted into the job total before routing
            // and never reached the assembly, so it is not in `routed_bytes`.
            let dropped_bytes = dropped.map(|(_, bytes)| bytes).unwrap_or(0);
            state.downloaded_bytes = state
                .downloaded_bytes
                .saturating_sub(routed_bytes.saturating_add(dropped_bytes));
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
fn read_restart_seeded_runs(
    working_dir: &std::path::Path,
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
                let file = std::fs::File::open(working_dir.join(&run.relative_partial))?;
                open = Some((run.relative_partial.clone(), file));
                &mut open.as_mut().expect("just assigned").1
            }
        };
        file.seek(SeekFrom::Start(run.logical_offset))?;
        let mut hasher = crc32fast::Hasher::new();
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
        checksums.push(hasher.finalize());
    }
    Ok(checksums)
}

fn contiguous_bytes(data: &DecodedChunk) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len_bytes());
    data.for_each_slice(|slice| out.extend_from_slice(slice));
    out
}
