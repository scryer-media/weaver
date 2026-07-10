use super::*;

const PHASE_EMA_HALF_LIFE_SECS: f64 = 3.0;
const PHASE_RATE_WARMUP: Duration = Duration::from_millis(250);
const PHASE_PUBLISH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub(crate) struct JobPhaseRuntime {
    pub(super) counters: Arc<PhaseCounters>,
    pub(super) started_at_epoch_ms: f64,
    pub(super) ema_bps: Option<f64>,
    pub(super) first_sample_at: Option<Instant>,
    pub(super) last_sample: Option<(Instant, u64)>,
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
            ema_bps: None,
            first_sample_at: None,
            last_sample: None,
        };
        self.phase_progress.insert(key, runtime);
        self.phase_publish_state.remove(&job_id);
        counters
    }

    pub(crate) fn phase_end(&mut self, job_id: JobId, phase: JobPhase) {
        if self.phase_progress.remove(&(job_id, phase)).is_some() {
            if let Some(phases) = self.phase_progress_snapshots.get_mut(&job_id) {
                phases.retain(|progress| progress.phase != phase);
                if phases.is_empty() {
                    self.phase_progress_snapshots.remove(&job_id);
                }
            }
        }
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
            let derived_download = if phase == JobPhase::Downloading {
                self.jobs.get(&job_id).map(|state| {
                    (
                        Self::effective_downloaded_bytes(state),
                        state.spec.total_bytes,
                    )
                })
            } else {
                None
            };

            let Some(runtime) = self.phase_progress.get_mut(&(job_id, phase)) else {
                continue;
            };
            let (completed_bytes, total_bytes) = derived_download.unwrap_or_else(|| {
                (
                    runtime.counters.completed_bytes.load(Ordering::Relaxed),
                    runtime.counters.total_bytes.load(Ordering::Relaxed),
                )
            });
            if total_bytes == 0 {
                runtime.last_sample = Some((now, completed_bytes));
                continue;
            }

            if runtime.first_sample_at.is_none() {
                runtime.first_sample_at = Some(now);
            }
            if let Some((last_at, last_completed)) = runtime.last_sample {
                let dt = now.duration_since(last_at).as_secs_f64();
                if dt > 0.0 {
                    let delta = completed_bytes.saturating_sub(last_completed) as f64;
                    let sample_bps = delta / dt;
                    let alpha = 1.0 - 0.5f64.powf(dt / PHASE_EMA_HALF_LIFE_SECS);
                    runtime.ema_bps = Some(match runtime.ema_bps {
                        Some(current) => current + alpha * (sample_bps - current),
                        None => sample_bps,
                    });
                }
            }
            runtime.last_sample = Some((now, completed_bytes));

            let rate_warm = runtime
                .first_sample_at
                .is_some_and(|first| now.duration_since(first) >= PHASE_RATE_WARMUP);
            let rate_bps = runtime
                .ema_bps
                .filter(|rate| rate_warm && rate.is_finite() && *rate > 0.0)
                .map(|rate| rate.round().max(0.0) as u64);
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
        self.shared_state.publish_jobs(self.list_jobs());
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
