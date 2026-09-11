use super::*;

use crate::operations::metrics::RateSeries;

const PHASE_RATE_WARMUP: Duration = Duration::from_millis(250);
const PHASE_PUBLISH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub(crate) struct JobPhaseRuntime {
    pub(super) counters: Arc<PhaseCounters>,
    pub(super) started_at_epoch_ms: f64,
    /// The same estimator the global speed gauge uses, advanced on the same
    /// 100 ms tick, so a row's rate is comparable to the nav counter.
    pub(super) rate: RateSeries,
    pub(super) first_sample_at: Option<Instant>,
}

/// The metric stage label for a user-visible job phase. Verification and
/// post-processing are not phases, so they arm their timers from their own
/// start/finish points.
const fn stage_kind_for_phase(phase: JobPhase) -> crate::operations::instrumentation::JobStageKind {
    use crate::operations::instrumentation::JobStageKind;
    match phase {
        JobPhase::Downloading => JobStageKind::Download,
        JobPhase::Repairing => JobStageKind::Repair,
        JobPhase::Extracting => JobStageKind::Extract,
        JobPhase::Moving => JobStageKind::Move,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PhasePublishSignature(Vec<(JobPhase, u8, bool)>);

#[derive(Debug)]
pub(crate) struct PhasePublishState {
    signature: PhasePublishSignature,
    sent_at: Instant,
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
            rate: RateSeries::new(),
            first_sample_at: None,
        };
        self.phase_progress.insert(key, runtime);
        self.phase_publish_state.remove(&job_id);
        // Stage timing rides the phase lifecycle, which is per job per phase —
        // a handful of events over a job's whole life, never a per-segment
        // path. One `Instant::now()` here is the cheapest honest measurement
        // available and costs nothing that matters at this frequency.
        self.note_stage_started(job_id, stage_kind_for_phase(phase));
        counters
    }

    pub(crate) fn phase_end(&mut self, job_id: JobId, phase: JobPhase) {
        self.note_stage_finished(job_id, stage_kind_for_phase(phase));
        if self.phase_progress.remove(&(job_id, phase)).is_some()
            && let Some(phases) = self.phase_progress_snapshots.get_mut(&job_id)
        {
            phases.retain(|progress| progress.phase != phase);
            if phases.is_empty() {
                self.phase_progress_snapshots.remove(&job_id);
            }
        }
    }

    /// Arm the wall-clock timer for one job stage.
    ///
    /// Low-frequency by construction: a job enters each stage a handful of
    /// times. Re-arming an already-armed stage is ignored so a stage that is
    /// re-entered (a repair pass returning to extraction, say) reports the span
    /// of its first entry rather than restarting the clock mid-stage.
    pub(crate) fn note_stage_started(
        &mut self,
        job_id: JobId,
        stage: crate::operations::instrumentation::JobStageKind,
    ) {
        self.job_stage_started_at
            .entry((job_id, stage))
            .or_insert_with(Instant::now);
    }

    /// Observe one job stage's wall duration, if it was armed.
    ///
    /// Low-frequency: see [`Self::note_stage_started`].
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

    /// Drop any stage timers still armed for a job that is going away, so the
    /// map cannot outlive the jobs it describes.
    pub(crate) fn discard_stage_timers(&mut self, job_id: JobId) {
        self.job_stage_started_at.retain(|(id, _), _| *id != job_id);
    }

    pub(crate) fn phase_subtract_completed_bytes(
        &mut self,
        job_id: JobId,
        phase: JobPhase,
        bytes: u64,
    ) {
        if bytes == 0 {
            return;
        }
        let Some(runtime) = self.phase_progress.get(&(job_id, phase)) else {
            return;
        };
        let _ = runtime.counters.completed_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(bytes)),
        );
    }

    pub(crate) fn phase_end_extracting_if_idle(&mut self, job_id: JobId) {
        let has_inflight_extractions = self
            .inflight_extractions
            .get(&job_id)
            .is_some_and(|sets| !sets.is_empty());
        if !has_inflight_extractions && !self.has_active_rar_workers(job_id) {
            self.phase_end(job_id, JobPhase::Extracting);
        }
    }

    /// Credit one landed article's wire bytes to its job's download rate.
    ///
    /// Called on the completion path right beside the global `bytes_downloaded`
    /// counter it must stay in lockstep with: one map lookup, no lock, no
    /// allocation. A job that has already been removed simply drops the credit.
    pub(crate) fn note_job_wire_bytes(&mut self, segment_id: SegmentId, raw_size: u64) {
        if let Some(state) = self.jobs.get_mut(&segment_id.file_id.job_id) {
            state.downloaded_wire_bytes = state.downloaded_wire_bytes.saturating_add(raw_size);
        }
    }

    pub(crate) fn clear_job_phase_progress_runtime(&mut self, job_id: JobId) {
        self.phase_progress.retain(|(jid, _), _| *jid != job_id);
        self.phase_progress_snapshots.remove(&job_id);
        self.phase_publish_state.remove(&job_id);
    }

    pub(crate) fn sample_phase_progress(&mut self) {
        let now = Instant::now();
        let updated_at_epoch_ms = crate::jobs::epoch_ms_now();
        let keys: Vec<_> = self.phase_progress.keys().copied().collect();
        let mut by_job: HashMap<JobId, Vec<JobPhaseProgress>> = HashMap::new();

        for (job_id, phase) in keys {
            let download_active =
                phase != JobPhase::Downloading || self.job_has_current_download_activity(job_id);
            // The download phase deliberately reports two different byte
            // counts. Progress is decoded bytes against the declared total, so
            // the bar cannot overshoot 100%. The rate is wire bytes, the same
            // counter the global speed gauge integrates: yEnc runs ~3% larger
            // than its payload (uuencode ~38%), and a decoded-byte rate also
            // trails the network by however deep the decode queue is, so a row
            // built from it never agreed with the nav counter.
            let derived_download = if phase == JobPhase::Downloading {
                self.jobs.get(&job_id).map(|state| {
                    (
                        Self::effective_downloaded_bytes(state),
                        state.spec.total_bytes,
                        state.downloaded_wire_bytes,
                    )
                })
            } else {
                None
            };

            let Some(runtime) = self.phase_progress.get_mut(&(job_id, phase)) else {
                continue;
            };
            let (completed_bytes, total_bytes, rate_bytes) =
                derived_download.unwrap_or_else(|| {
                    let completed = runtime.counters.completed_bytes.load(Ordering::Relaxed);
                    (
                        completed,
                        runtime.counters.total_bytes.load(Ordering::Relaxed),
                        completed,
                    )
                });
            if phase == JobPhase::Downloading && !download_active {
                runtime.rate = RateSeries::new();
                runtime.first_sample_at = None;
                continue;
            }
            if total_bytes == 0 {
                runtime.rate.update(now, rate_bytes);
                continue;
            }

            if runtime.first_sample_at.is_none() {
                runtime.first_sample_at = Some(now);
            }
            let rate = runtime.rate.update(now, rate_bytes);

            let rate_warm = runtime
                .first_sample_at
                .is_some_and(|first| now.duration_since(first) >= PHASE_RATE_WARMUP);
            // Same rounding as the global gauge: below 1 B/s reads as idle.
            let rate_bps = (rate_warm && rate.is_finite() && rate >= 1.0).then_some(rate as u64);
            let effective_total = total_bytes.max(completed_bytes);
            let progress_percent = if effective_total == 0 {
                0.0
            } else {
                ((completed_bytes as f64 / effective_total as f64) * 100.0).clamp(0.0, 100.0) as f32
            };
            let estimated_remaining_ms = rate_bps.and_then(|rate| {
                if rate == 0 || completed_bytes >= total_bytes {
                    None
                } else {
                    Some(
                        (total_bytes - completed_bytes)
                            .saturating_mul(1000)
                            .checked_div(rate)
                            .unwrap_or(u64::MAX),
                    )
                }
            });

            by_job.entry(job_id).or_default().push(JobPhaseProgress {
                phase,
                completed_bytes,
                total_bytes,
                progress_percent,
                rate_bps,
                estimated_remaining_ms,
                started_at_epoch_ms: runtime.started_at_epoch_ms,
                updated_at_epoch_ms,
            });
        }

        for phases in by_job.values_mut() {
            phases.sort_by_key(|progress| progress.phase);
        }

        self.phase_progress_snapshots = by_job;
        self.publish_snapshot();
        self.emit_phase_progress_update_events(now);
    }

    fn emit_phase_progress_update_events(&mut self, now: Instant) {
        let mut job_ids: HashSet<JobId> = self.phase_progress_snapshots.keys().copied().collect();
        job_ids.extend(self.phase_publish_state.keys().copied());

        for job_id in job_ids {
            let signature = phase_publish_signature(
                self.phase_progress_snapshots
                    .get(&job_id)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            );
            if signature.0.is_empty() {
                if self.phase_publish_state.remove(&job_id).is_some() {
                    let _ = self
                        .event_tx
                        .send(PipelineEvent::PhaseProgressUpdated { job_id });
                }
                continue;
            }

            let should_send = self.phase_publish_state.get(&job_id).is_none_or(|state| {
                state.signature != signature
                    || now.duration_since(state.sent_at) >= PHASE_PUBLISH_INTERVAL
            });
            if should_send {
                self.phase_publish_state.insert(
                    job_id,
                    PhasePublishState {
                        signature,
                        sent_at: now,
                    },
                );
                let _ = self
                    .event_tx
                    .send(PipelineEvent::PhaseProgressUpdated { job_id });
            }
        }
    }
}

fn phase_publish_signature(progress: &[JobPhaseProgress]) -> PhasePublishSignature {
    PhasePublishSignature(
        progress
            .iter()
            .map(|item| {
                (
                    item.phase,
                    item.progress_percent.floor().clamp(0.0, 100.0) as u8,
                    item.rate_bps.is_some(),
                )
            })
            .collect(),
    )
}
