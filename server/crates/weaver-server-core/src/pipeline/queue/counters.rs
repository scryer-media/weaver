// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::{Change, Queue};
use crate::events::model::PipelineEvent;
use crate::jobs::JobId;
use crate::jobs::phase_progress::{JobPhase, JobPhaseProgress, PhaseCounters};
use crate::operations::metrics::{PipelineMetrics, RateSeries};

pub(crate) struct PhaseSample {
    counters: Arc<PhaseCounters>,
    started: f64,
    first: Option<Instant>,
    rate: RateSeries,
}

impl Queue {
    pub(crate) fn begin_phase(
        &self,
        id: JobId,
        phase: JobPhase,
        counters: Arc<PhaseCounters>,
        started: f64,
    ) {
        self.write(|inner| {
            if let Some(row) = inner.jobs.iter_mut().find(|row| row.id == id)
                && let std::collections::hash_map::Entry::Vacant(entry) = row.phases.entry(phase)
            {
                entry.insert(PhaseSample {
                    counters,
                    started,
                    first: None,
                    rate: RateSeries::new(),
                });
                return Change::changed(());
            }
            Change::unchanged(())
        });
    }

    pub(crate) fn end_phase(&self, id: JobId, phase: JobPhase) {
        self.write(|inner| {
            if let Some(row) = inner.jobs.iter_mut().find(|row| row.id == id) {
                row.phases.remove(&phase);
                row.sampled_phases.retain(|sample| sample.phase != phase);
                row.report
                    .job
                    .phase_progress
                    .retain(|sample| sample.phase != phase);
                return Change::changed(());
            }
            Change::unchanged(())
        });
    }

    /// Runs independently of pipeline turns; retains no queue ownership while idle.
    pub(crate) fn start_ticker(
        self: &Arc<Self>,
        metrics: Arc<PipelineMetrics>,
        events: tokio::sync::broadcast::Sender<PipelineEvent>,
    ) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        if self.ticker_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let queue = Arc::downgrade(self);
        runtime.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let Some(queue) = queue.upgrade() else { break };
                if queue.closed.load(Ordering::Acquire) {
                    break;
                }
                queue.sample_counters(&metrics, &events, tokio::time::Instant::now().into_std());
            }
        });
    }
    pub(crate) fn sample_counters(
        &self,
        metrics: &PipelineMetrics,
        events: &tokio::sync::broadcast::Sender<PipelineEvent>,
        now: Instant,
    ) {
        if let Err(error) = self.sample_bandwidth(false) {
            tracing::warn!(%error, "failed to sample bandwidth usage");
        }
        let snapshot = metrics.sample_totals(
            now,
            self.counters.bandwidth.total(),
            self.counters.decoded.load(Ordering::Relaxed),
            self.counters.articles.load(Ordering::Relaxed),
        );
        self.write(|inner| {
            let mut changed = inner.metrics.as_ref().is_none_or(|old| {
                old.bytes_downloaded != snapshot.bytes_downloaded
                    || old.current_download_speed != snapshot.current_download_speed
                    || old.decode_rate_mbps != snapshot.decode_rate_mbps
            });
            inner.metrics = Some(snapshot);
            let epoch = crate::jobs::epoch_ms_now();
            for row in &mut inner.jobs {
                if row.phases.is_empty() || row.intents.cancelled.is_some() {
                    continue;
                }
                let mut phases = Vec::with_capacity(row.phases.len());
                // The download phase samples only while something moves bytes:
                // a started download between lanes keeps its lane-less state
                // but shows no phase and no rate.
                let downloading = row.download_activity();
                for (&phase, sample) in &mut row.phases {
                    let download = phase == JobPhase::Downloading;
                    if download && !downloading {
                        sample.rate = RateSeries::new();
                        sample.first = None;
                        continue;
                    }
                    let completed = if download {
                        // Decoder throughput includes replacement articles.
                        // Only the report accounts for unique accepted bytes.
                        row.report.job.downloaded_bytes
                    } else {
                        sample.counters.completed_bytes.load(Ordering::Relaxed)
                    };
                    let total = sample.counters.total_bytes.load(Ordering::Relaxed);
                    if total == 0 {
                        continue;
                    }
                    let bytes = if download {
                        row.counters.downloaded.load(Ordering::Relaxed)
                    } else {
                        completed
                    };
                    let rate = sample.rate.update(now, bytes);
                    let rate_bps = (now.duration_since(*sample.first.get_or_insert(now))
                        >= Duration::from_millis(250)
                        && rate.is_finite()
                        && rate >= 1.0)
                        .then_some(rate as u64);
                    phases.push(JobPhaseProgress {
                        phase,
                        completed_bytes: completed,
                        total_bytes: total,
                        progress_percent: ((completed as f64 / total.max(completed) as f64) * 100.0)
                            as f32,
                        rate_bps,
                        estimated_remaining_ms: rate_bps
                            .filter(|rate| *rate > 0 && completed < total)
                            .map(|rate| (total - completed).saturating_mul(1000) / rate),
                        started_at_epoch_ms: sample.started,
                        updated_at_epoch_ms: epoch,
                    });
                }
                phases.sort_by_key(|sample| sample.phase);
                row.sampled_phases = phases;
                changed = true;
                let _ = events.send(PipelineEvent::PhaseProgressUpdated { job_id: row.id });
            }
            if changed {
                Change::changed(())
            } else {
                Change::unchanged(())
            }
        });
    }

    pub(crate) fn sample_bandwidth(&self, flush: bool) -> Result<(), crate::StateError> {
        if let Some(resources) = self.resources.get() {
            self.bandwidth_ledger
                .lock()
                .expect("bandwidth ledger lock poisoned")
                .sample(&resources.db, &self.counters.bandwidth, flush)?;
        }
        Ok(())
    }
}
