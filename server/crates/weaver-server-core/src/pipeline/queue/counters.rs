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

// Counts the ticker's samples, so a test can pin that a parked ticker takes
// none.
#[cfg(test)]
pub(crate) static COUNTER_SAMPLES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

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
            if let Some(row) = inner.job_mut(id)
                && let std::collections::hash_map::Entry::Vacant(entry) = row.phases.entry(phase)
            {
                // A later phase shows from the moment it opens, so a phase
                // the drivers never sample still says when it started.
                if phase != JobPhase::Downloading {
                    let completed = counters.completed_bytes.load(Ordering::Relaxed);
                    let total = counters.total_bytes.load(Ordering::Relaxed);
                    row.sampled_phases.push(JobPhaseProgress {
                        phase,
                        completed_bytes: completed,
                        total_bytes: total,
                        progress_percent: 0.0,
                        rate_bps: None,
                        estimated_remaining_ms: None,
                        started_at_epoch_ms: started,
                        updated_at_epoch_ms: started,
                    });
                    row.sampled_phases.sort_by_key(|sample| sample.phase);
                }
                entry.insert(PhaseSample {
                    counters,
                    started,
                    first: None,
                    rate: RateSeries::new(),
                });
                inner.phased_jobs.insert(id);
                return Change::changed(());
            }
            Change::unchanged(())
        });
    }

    pub(crate) fn end_phase(&self, id: JobId, phase: JobPhase) {
        self.write(|inner| {
            if let Some(row) = inner.job_mut(id) {
                row.phases.remove(&phase);
                row.sampled_phases.retain(|sample| sample.phase != phase);
                row.report
                    .job
                    .phase_progress
                    .retain(|sample| sample.phase != phase);
                if row.phases.is_empty() {
                    // A row out of every phase is no longer sampled; a later
                    // phase starts its download series afresh from the gauge.
                    row.download_rate = None;
                    inner.phased_jobs.remove(&id);
                }
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
        let mut drive = self.subscribe_drive();
        runtime.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Whether the ticker has driven since it last parked.
            let mut drove = false;
            loop {
                if !*drive.borrow_and_update() {
                    // Nothing moves, so a sample would find what the last
                    // one found. Parking after driving takes one sample, so
                    // bytes counted since the last tick, such as a speed
                    // test's last articles, are published. Write out the
                    // bandwidth the samples recorded, then wait for a turn
                    // that finds work.
                    let Some(queue) = queue.upgrade() else { break };
                    if std::mem::take(&mut drove) && !queue.closed.load(Ordering::Acquire) {
                        queue.sample_counters(
                            &metrics,
                            &events,
                            tokio::time::Instant::now().into_std(),
                        );
                    }
                    if let Err(error) = queue.sample_bandwidth(true) {
                        tracing::warn!(%error, "failed to flush bandwidth usage");
                    }
                    drop(queue);
                    if drive.changed().await.is_err() {
                        break;
                    }
                    interval.reset_immediately();
                    continue;
                }
                drove = true;
                tokio::select! {
                    _ = interval.tick() => {}
                    // A turn that finds nothing to drive parks the ticker now,
                    // not after one more tick.
                    changed = drive.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        continue;
                    }
                }
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
        #[cfg(test)]
        COUNTER_SAMPLES.fetch_add(1, Ordering::Relaxed);
        if let Err(error) = self.sample_bandwidth(false) {
            tracing::warn!(%error, "failed to sample bandwidth usage");
        }
        let seed = metrics.download_rate_seed();
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
            let phased: Vec<JobId> = inner.phased_jobs().collect();
            for id in phased {
                let Some(row) = inner.job_mut(id) else {
                    continue;
                };
                // A row's download series advances on every tick while the
                // row is in a phase, and starts from the gauge's own sample
                // instants: the gauge integrates the transfer total each
                // row's counter is credited beside, so the rows' rates sum
                // to the gauge. A row in no phase moves no bytes and shows
                // no rate, so the tick leaves it alone.
                let downloaded = row.counters.downloaded.load(Ordering::Relaxed);
                let download_rate = row
                    .download_rate
                    .get_or_insert_with(|| seed.clone())
                    .update(now, downloaded);
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
                    // A download with nothing to fetch is not shown. A later
                    // phase with no byte total, such as a repair, is still
                    // shown with its start, so its time can be read.
                    if download && total == 0 {
                        continue;
                    }
                    // The download rate needs no warm-up: it never starts a
                    // window of its own.
                    let (rate, warm) = if download {
                        (download_rate, true)
                    } else {
                        let rate = sample.rate.update(now, completed);
                        let first = *sample.first.get_or_insert(now);
                        (
                            rate,
                            now.duration_since(first) >= Duration::from_millis(250),
                        )
                    };
                    let rate_bps = (warm && rate.is_finite() && rate >= 1.0).then_some(rate as u64);
                    phases.push(JobPhaseProgress {
                        phase,
                        completed_bytes: completed,
                        total_bytes: total,
                        progress_percent: if total.max(completed) == 0 {
                            0.0
                        } else {
                            ((completed as f64 / total.max(completed) as f64) * 100.0) as f32
                        },
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
