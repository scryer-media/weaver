// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[derive(Debug)]
pub(crate) struct JobPhaseRuntime {
    pub(super) counters: Arc<PhaseCounters>,
    pub(super) started_at_epoch_ms: f64,
}

// The metric stage label for a user-visible job phase. Verification and
// post-processing are not phases, so they arm their timers from their own
// start/finish points.
const fn stage_kind_for_phase(phase: JobPhase) -> crate::operations::instrumentation::JobStageKind {
    use crate::operations::instrumentation::JobStageKind;
    match phase {
        JobPhase::Downloading => JobStageKind::Download,
        JobPhase::Repairing => JobStageKind::Repair,
        JobPhase::Extracting => JobStageKind::Extract,
        JobPhase::Moving => JobStageKind::Move,
    }
}

impl Pipeline {
    pub(crate) fn phase_begin(
        &mut self,
        job_id: JobId,
        phase: JobPhase,
        initial_total: Option<u64>,
    ) -> Arc<PhaseCounters> {
        let key = (job_id, phase);
        if let Some(runtime) = self.phase_progress.get(&key) {
            return Arc::clone(&runtime.counters);
        }

        let counters = Arc::new(PhaseCounters::default());
        if let Some(total) = initial_total {
            counters.total_bytes.store(total, Ordering::Relaxed);
        }
        let runtime = JobPhaseRuntime {
            counters: Arc::clone(&counters),
            started_at_epoch_ms: crate::jobs::epoch_ms_now(),
        };
        self.shared_state.queue.begin_phase(
            job_id,
            phase,
            Arc::clone(&counters),
            runtime.started_at_epoch_ms,
        );
        self.phase_progress.insert(key, runtime);
        if phase != JobPhase::Downloading {
            self.post_download_phases += 1;
        }
        // Stage timing rides the phase lifecycle, which is per job per phase —
        // a handful of events over a job's whole life, never a per-segment
        // path. One `Instant::now()` here is the cheapest honest measurement
        // available and costs nothing that matters at this frequency.
        self.note_stage_started(job_id, stage_kind_for_phase(phase));
        counters
    }

    pub(crate) fn phase_end(&mut self, job_id: JobId, phase: JobPhase) {
        self.shared_state.queue.end_phase(job_id, phase);
        self.note_stage_finished(job_id, stage_kind_for_phase(phase));
        let removed = self.phase_progress.remove(&(job_id, phase)).is_some();
        if removed && phase != JobPhase::Downloading {
            self.post_download_phases = self.post_download_phases.saturating_sub(1);
        }
    }

    // Arm the wall-clock timer for one job stage.
    //
    // Low-frequency by construction: a job enters each stage a handful of
    // times. Re-arming an already-armed stage is ignored so a stage that is
    // re-entered (a repair pass returning to extraction, say) reports the span
    // of its first entry rather than restarting the clock mid-stage.
    pub(crate) fn note_stage_started(
        &mut self,
        job_id: JobId,
        stage: crate::operations::instrumentation::JobStageKind,
    ) {
        self.job_stage_started_at
            .entry((job_id, stage))
            .or_insert_with(Instant::now);
    }

    // Observe one job stage's wall duration, if it was armed.
    //
    // Low-frequency: see [`Self::note_stage_started`].
    pub(crate) fn note_stage_finished(
        &mut self,
        job_id: JobId,
        stage: crate::operations::instrumentation::JobStageKind,
    ) {
        if let Some(started) = self.job_stage_started_at.remove(&(job_id, stage)) {
            self.metrics
                .job_lifecycle
                .note_stage_duration(stage, started.elapsed());
        }
    }

    // Drop any stage timers still armed for a job that is going away, so the
    // map cannot outlive the jobs it describes.
    pub(crate) fn discard_stage_timers(&mut self, job_id: JobId) {
        self.job_stage_started_at.retain(|(id, _), _| *id != job_id);
    }

    // Credit one landed article's wire bytes to its job's download rate, and
    // to the server that served it.
    //
    // Called on the completion path right beside the global `bytes_downloaded`
    // counter it must stay in lockstep with: one map lookup, no lock, no
    // allocation. A job that has already been removed simply drops the credit.
    //
    // `source_server_idx` is the runtime index of the serving server, already
    // in hand at every call site. Naming it costs one vector index, and the
    // per-job ledger costs a walk of a handful of entries, so attribution
    // rides along on the lookup this function already performs. An article
    // whose server cannot be named is credited to the job's bytes and left
    // out of the ledger: an uncounted article is recoverable from the totals,
    // a misattributed one is not.
    // Write the job's debug ring out beside a stall report, once per stall
    // episode. The job's downloaded wire bytes are its progress: a report at
    // the same count as the last replay belongs to the same episode.
    pub(crate) fn dump_job_debug_ring(&self, job_id: JobId, reason: &'static str) {
        let progress = self
            .jobs
            .get(&job_id)
            .map_or(0, |state| state.downloaded_wire_bytes);
        crate::runtime::job_debug_ring::dump(job_id.0, progress, reason);
    }

    pub(crate) fn note_job_wire_bytes(
        &mut self,
        segment_id: SegmentId,
        raw_size: u64,
        source_server_idx: Option<usize>,
    ) {
        let server_id = source_server_idx.and_then(|idx| {
            self.nntp
                .pool()
                .stable_server_id(weaver_nntp::pool::ServerId(idx))
                .map(|stable| stable.0)
        });
        if let Some(state) = self.jobs.get_mut(&segment_id.file_id.job_id) {
            state.downloaded_wire_bytes = state.downloaded_wire_bytes.saturating_add(raw_size);
            if let Some(server_id) = server_id {
                state.server_attribution.note_article(server_id, raw_size);
                self.dirty_server_attribution
                    .insert(segment_id.file_id.job_id);
            }
        }
    }

    pub(crate) fn clear_job_phase_progress_runtime(&mut self, job_id: JobId) {
        let mut removed_post_download = 0;
        self.phase_progress.retain(|(jid, phase), _| {
            let keep = *jid != job_id;
            if !keep {
                self.shared_state.queue.end_phase(job_id, *phase);
            }
            if !keep && *phase != JobPhase::Downloading {
                removed_post_download += 1;
            }
            keep
        });
        self.post_download_phases = self
            .post_download_phases
            .saturating_sub(removed_post_download);
    }
}
