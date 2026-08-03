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
//!    whole-member gate commits its partials to the extractor's destinations
//!    and is marked extracted; a set that demotes deletes its direct output,
//!    retires its checkpoint row and refetches its volumes conventionally.
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

        if !spans.is_empty() {
            let batches = self.direct_write_batches(job_id, set_index, &spans);
            self.prepare_direct_destination_dirs(job_id, &batches).await;
            if let Err(error) = crate::pipeline::orchestrator::write_direct_batches(batches).await {
                // A destination write failure is a demotion, not a job
                // failure: the conventional path writes the same bytes to a
                // different file, and only if *that* also fails is the job
                // genuinely unfinishable (M6).
                warn!(
                    job_id = job_id.0,
                    segment = %segment_id,
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
                        format!("direct-store destination write failed for {segment_id}: {error}"),
                    );
                }
                return DirectRouteOutcome::Demoted;
            }
            if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
                set.record_writes(&spans, Instant::now());
            }
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
            .map(|(index, _, partial)| (index, partial.to_string()))
            .collect();
        let envelope = set.plan().envelope_relative_path();

        let mut grouped: HashMap<PathBuf, Vec<(u64, Vec<u8>)>> = HashMap::new();
        for span in spans {
            let relative = match span.destination {
                DirectDestination::Member { member_index } => match partials.get(&member_index) {
                    Some(path) => path.clone(),
                    None => continue,
                },
                DirectDestination::Envelope => envelope.clone(),
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
        if let Some(Err(reason)) = outcome {
            self.demote_direct_set(job_id, set_index, reason, None)
                .await;
            return;
        }

        if self
            .direct_store
            .set(job_id, set_index)
            .is_some_and(DirectSet::ready_to_finalize)
        {
            self.finalize_direct_set(job_id, set_index).await;
        }
        self.check_job_completion(job_id).await;
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

        let mut results = HashMap::with_capacity(touched.len());
        for relative in touched {
            let path = working_dir.join(&relative);
            let outcome = crate::pipeline::orchestrator::sync_direct_destination(&path)
                .await
                .map_err(|error| error.to_string());
            results.insert(relative, outcome);
        }

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
        let envelope = set.plan().envelope_path();
        let members: Vec<(String, PathBuf, PathBuf)> = set
            .router
            .member_partials()
            .into_iter()
            .filter_map(|(_, name, partial)| {
                let destination = set.plan().member_output_path(name).ok()?;
                Some((name.to_string(), working_dir.join(partial), destination))
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
        for (name, partial, destination) in &members {
            crate::pipeline::release_cached_write_handle(partial);
            if let Some(parent) = destination.parent()
                && let Err(error) = tokio::fs::create_dir_all(parent).await
            {
                warn!(job_id = job_id.0, error = %error, "failed to create direct-store destination directory; demoting the set");
                self.demote_direct_set(job_id, set_index, DemotionReason::FinalizationFailed, None)
                    .await;
                return;
            }
            if let Err(error) = tokio::fs::rename(partial, destination).await {
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
        crate::pipeline::release_cached_write_handle(&envelope);
        let _ = tokio::fs::remove_file(&envelope).await;

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

    /// Abandons direct output for a set and hands its volumes back.
    ///
    /// Phase 4's demotion is the conservative form: the routed bytes are
    /// discarded and the volumes refetched, rather than reconstructed
    /// byte-exactly from the envelope (phase 5 owns that). Losing already-routed
    /// bytes to a refetch is the accepted cost.
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
        let working_dir = set.plan().working_dir.clone();
        let envelope = set.plan().envelope_path();
        let partials: Vec<PathBuf> = set
            .router
            .member_partials()
            .into_iter()
            .map(|(_, _, partial)| working_dir.join(partial))
            .collect();
        let volumes: Vec<u32> = set.plan().volumes.values().copied().collect();

        crate::runtime::perf_probe::record_owned(
            format!("direct_store.demoted.{}", reason.metric()),
            std::time::Duration::from_nanos(1),
        );
        warn!(
            job_id = job_id.0,
            set_name = %set_name,
            reason = reason.metric(),
            "direct-store set demoted; refetching its volumes conventionally"
        );

        // D8: the checkpoint row goes first, because everything it claims is
        // about to be deleted. A crash between here and the refetch costs a
        // redownload, which is what the demotion is doing anyway.
        let mut persist = DatabaseCoveragePersist::new(self.db.clone());
        if let Some(set) = self.direct_store.set_mut(job_id, set_index)
            && let Err(error) = set.retire(&mut persist)
        {
            warn!(job_id = job_id.0, error = %error, "failed to retire a demoted direct-store checkpoint");
        }

        for partial in partials.iter().chain(std::iter::once(&envelope)) {
            crate::pipeline::release_cached_write_handle(partial);
            let _ = tokio::fs::remove_file(partial).await;
        }

        self.refetch_direct_volumes(job_id, &volumes, dropped).await;
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
