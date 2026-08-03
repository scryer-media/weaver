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

use super::DirectStoreGate;
use super::barrier::{BarrierDemand, BarrierDrain, DatabaseCoveragePersist, DestinationSync};
use super::plan::DirectSetPlan;
use super::reconstruct::{ReconstructionFailure, VolumeReconstruction};
use super::router::{DemotionReason, DirectDestination, RoutedSpan};
use super::set::DirectSet;
use crate::DownloadWork;
use crate::events::model::PipelineEvent;
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::pipeline::{BufferedDecodedSegment, DecodedChunk, Pipeline};

/// Per-pipeline direct-store state. Empty and inert while the gate is off.
#[derive(Default)]
pub(crate) struct DirectStoreRuntime {
    gate: Option<DirectStoreGate>,
    /// Jobs whose spec has already been examined for candidate sets.
    examined: HashSet<JobId>,
    sets: HashMap<JobId, Vec<DirectSet>>,
    /// Destination parent directories already created, per job (B3). A member
    /// stored inside a directory names a partial inside that directory, and
    /// nothing else creates it.
    prepared_dirs: HashMap<JobId, HashSet<PathBuf>>,
    /// Test-only holds ceiling applied to every set this runtime admits.
    #[cfg(test)]
    holds_budget_override: Option<u64>,
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
    pub(crate) fn gate(&mut self) -> DirectStoreGate {
        *self.gate.get_or_insert_with(DirectStoreGate::from_env)
    }

    /// Test hook: force the gate without racing the process-wide `OnceLock`.
    #[cfg(test)]
    pub(crate) fn set_gate(&mut self, gate: DirectStoreGate) {
        self.gate = Some(gate);
    }

    /// Test hook: lower the holds ceiling so a breach is reachable without
    /// staging tens of megabytes.
    #[cfg(test)]
    pub(crate) fn set_holds_budget(&mut self, bytes: u64) {
        self.holds_budget_override = Some(bytes);
    }

    /// Drops every trace of a job. Called from the job-removal seam: a barrier
    /// for a job that no longer exists must stop being polled, and its sets
    /// hold the routed byte state of a working directory that is being deleted.
    pub(crate) fn clear_job(&mut self, job_id: JobId) {
        self.sets.remove(&job_id);
        self.examined.remove(&job_id);
        self.prepared_dirs.remove(&job_id);
    }

    #[cfg(test)]
    pub(crate) fn is_empty_for(&self, job_id: JobId) -> bool {
        !self.sets.contains_key(&job_id)
            && !self.examined.contains(&job_id)
            && !self.prepared_dirs.contains_key(&job_id)
    }

    pub(crate) fn sets_for(&self, job_id: JobId) -> &[DirectSet] {
        self.sets.get(&job_id).map(Vec::as_slice).unwrap_or(&[])
    }

    pub(crate) fn set_mut(&mut self, job_id: JobId, index: usize) -> Option<&mut DirectSet> {
        self.sets.get_mut(&job_id)?.get_mut(index)
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
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let (admitted, refused) = DirectSetPlan::discover(&state.spec, &state.working_dir);
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
        #[cfg(test)]
        let holds_budget_override = self.direct_store.holds_budget_override;
        let sets = admitted
            .into_iter()
            .map(|plan| {
                info!(
                    job_id = job_id.0,
                    set_name = %plan.set_name,
                    volumes = plan.volumes.len(),
                    "direct-store admitted an archive set"
                );
                // No format is chosen here: the router reads it from the first
                // volume's signature, so a RAR4 set routes as RAR4 rather than
                // demoting on its first header (H1).
                #[allow(unused_mut)]
                let mut set = DirectSet::new(job_id, plan);
                #[cfg(test)]
                if let Some(bytes) = holds_budget_override {
                    set.router.set_holds_budget(bytes);
                }
                set
            })
            .collect();
        self.direct_store.sets.insert(job_id, sets);
    }

    /// What to do with one NZB file's decoded bytes.
    ///
    /// `None` when the file is not a direct set's source volume, and `None`
    /// once its set has demoted — which is exactly what hands the volume back
    /// to the conventional path.
    pub(crate) fn direct_route_target(&mut self, file_id: NzbFileId) -> Option<DirectFileTarget> {
        self.ensure_direct_sets(file_id.job_id);
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
        let len = received.max(set.volume_coverage(volume_index).end());
        let lengths = std::collections::BTreeMap::from([(volume_index, len)]);
        Some((volume_index, len, set.virtual_provider(&lengths)))
    }

    /// The direct sets of `job_id` that the authoritative PAR2 pass must read
    /// virtually, or `None` when it has none and today's `PlacementFileAccess`
    /// is the whole answer.
    ///
    /// A volume is included only when its PAR2 identity resolves unambiguously
    /// through the same name candidates live verification binds on. One that
    /// does not resolve is left out, and the pass then reads it off disk, finds
    /// nothing, and reports it missing — which demotes the set through the
    /// damage path rather than silently verifying a set nobody could name.
    pub(crate) fn direct_par2_overlay(&self, job_id: JobId) -> Option<DirectPar2Overlay> {
        let mut volumes = Vec::new();
        let mut virtual_volumes = Vec::new();
        let mut sets = HashMap::new();
        for (set_index, set) in self.direct_store.sets_for(job_id).iter().enumerate() {
            // A demoted set's volumes are materializing or being refetched, and
            // a **finalized** set no longer owns a readable volume image at all:
            // its partials have been renamed to their destinations and its
            // envelopes deleted. Serving either one through the provider would
            // answer a verifier's reads with holes and report damage that is not
            // there. Finalization is gated on the job's verdict for exactly this
            // reason, so a set can only be finalized here on a *re*-verify, and
            // then the ordinary file access is the honest answer.
            if set.is_demoted() || set.is_finalized() {
                continue;
            }
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
                let len = received.max(set.volume_coverage(*volume_index).end());
                let lengths = std::collections::BTreeMap::from([(*volume_index, len)]);
                let mut built = set.virtual_volumes(&lengths);
                let Some(mut volume) = built.pop() else {
                    continue;
                };
                volume.volume_index = *file_index;
                virtual_volumes.push(volume);
                volumes.push(super::par2_access::VirtualPar2Volume {
                    par2_file_id: binding.par2_file_id,
                    volume_index: *file_index,
                });
                sets.insert(binding.par2_file_id, set_index);
            }
        }
        if volumes.is_empty() {
            return None;
        }
        Some(DirectPar2Overlay {
            provider: super::provider::HybridVolumeProvider::new(virtual_volumes),
            volumes,
            sets,
        })
    }

    /// Demotes every direct set the PAR2 pass found damage on, and reports
    /// whether any did (plan 135, D5/D8).
    ///
    /// Wave 2 produces **verdicts**, not repairs: repairing a virtual volume
    /// means writing a recovered slice into a file that does not exist, which is
    /// phase 6. So a damaged set materializes its volumes from its own routed
    /// bytes, refetches whatever reconstruction could not verify, and hands the
    /// job to the conventional repair path — which is exactly the shape the same
    /// job would have had with the gate off.
    pub(crate) async fn demote_direct_sets_with_par2_damage(
        &mut self,
        job_id: JobId,
        verification: &weaver_par2::VerificationResult,
    ) -> bool {
        let Some(overlay) = self.direct_par2_overlay(job_id) else {
            return false;
        };
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
        self.note_live_par2_segment(file_id, file_offset, &segment.data);
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
        self.prepare_direct_destination_dirs(job_id, &batches).await;
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
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            set.record_writes(spans, Instant::now());
        }
        true
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

    /// Creates the parent directory of every destination that needs one, once
    /// per job (B3).
    ///
    /// A member stored inside a directory — `Silver.Horizon/S01E06.mkv` — names
    /// a partial inside that directory, and the disk owner thread opens
    /// destinations with `create(true)` but never `create_dir_all`, so the
    /// first routed byte would fail with `ENOENT`. The conventional path never
    /// hits this because extraction creates the directory as it writes the
    /// member out; routing writes the member *before* extraction exists.
    async fn prepare_direct_destination_dirs(
        &mut self,
        job_id: JobId,
        batches: &crate::pipeline::orchestrator::DirectWriteBatches,
    ) {
        for (path, _) in batches {
            let Some(parent) = path.parent() else {
                continue;
            };
            if self
                .direct_store
                .prepared_dirs
                .get(&job_id)
                .is_some_and(|prepared| prepared.contains(parent))
            {
                continue;
            }
            if let Err(error) = tokio::fs::create_dir_all(parent).await {
                warn!(
                    job_id = job_id.0,
                    path = %parent.display(),
                    error = %error,
                    "failed to create a direct-store destination directory"
                );
                // Left unprepared on purpose: the write below fails and demotes,
                // and a later attempt retries the directory rather than trusting
                // a failure it never saw succeed.
                continue;
            }
            self.direct_store
                .prepared_dirs
                .entry(job_id)
                .or_default()
                .insert(parent.to_path_buf());
        }
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

        let outcome = self
            .direct_store
            .set_mut(job_id, set_index)
            .map(|set| set.note_volume_complete(volume_index));
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
                if !self
                    .place_direct_spans(job_id, set_index, &spans, None)
                    .await
                {
                    return;
                }
            }
            None => return,
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
        let envelopes = set.plan().envelope_paths();
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

        // D1's tolerance, and strictly after the stored members are committed:
        // the extraction reads the *virtual volumes*, whose member extents are
        // served out of the `.direct.partial`s — so it has to run before the
        // envelopes are deleted, and its own reads never touch a stored member's
        // destination because the tolerated set is disjoint from it by
        // construction (asserted in `extract_tolerated_members`).
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

        for envelope in &envelopes {
            crate::pipeline::release_cached_write_handle(envelope);
            let _ = tokio::fs::remove_file(envelope).await;
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

        // Every stored member's committed destination, so the assertion below
        // compares resolved paths rather than raw header names — two names can
        // sanitize onto one path, which is exactly the collision that would let
        // a tolerated member overwrite verified direct output.
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
                received.max(set.volume_coverage(*volume_index).end()),
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
            // what bounds the sweep.
            let len = received.max(coverage.end());
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
        let rebuilt = tokio::task::spawn_blocking(move || {
            crate::pipeline::direct_store::reconstruct::reconstruct_volumes(&provider, &plans)
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

    /// Deletes a set's partial members and envelope files.
    ///
    /// A sparse half-written output would masquerade as finished work (D1), and
    /// the envelopes are scratch by construction.
    async fn delete_direct_outputs(&mut self, job_id: JobId, set_index: usize) {
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
fn contiguous_bytes(data: &DecodedChunk) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len_bytes());
    data.for_each_slice(|slice| out.extend_from_slice(slice));
    out
}
