use tracing::error;

use weaver_server_core::operations::metrics::MetricsSnapshot;
use weaver_server_core::operations::{JobStatusCounts, MetricsHistoryCadence};
use weaver_server_core::{Database, SchedulerHandle};

pub(crate) async fn wait_for_shutdown() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
    }
}

pub(crate) fn pipeline_exit_error(result: Result<(), tokio::task::JoinError>) -> std::io::Error {
    match result {
        Ok(()) => {
            error!("pipeline task exited unexpectedly");
            std::io::Error::other("pipeline task exited unexpectedly")
        }
        Err(join_error) => {
            error!(error = %join_error, "pipeline task exited unexpectedly");
            std::io::Error::other(format!("pipeline task exited unexpectedly: {join_error}"))
        }
    }
}

pub(crate) fn spawn_metrics_history_task(
    handle: SchedulerHandle,
    db: Database,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut sampler = MetricsHistorySampler::default();

        loop {
            interval.tick().await;
            sampler
                .record(
                    &db,
                    epoch_sec_now(),
                    handle.get_metrics(),
                    handle.job_status_counts(),
                )
                .await;
        }
    })
}

/// Writes metrics history samples, skipping one that only repeats the last.
#[derive(Default)]
struct MetricsHistorySampler {
    cadence: MetricsHistoryCadence,
}

impl MetricsHistorySampler {
    async fn record(
        &mut self,
        db: &Database,
        recorded_at_epoch_sec: i64,
        metrics: MetricsSnapshot,
        job_counts: JobStatusCounts,
    ) {
        let plan = self.cadence.plan(recorded_at_epoch_sec);
        let values = MetricsHistoryCadence::sample_values(&metrics, &job_counts);
        if self.cadence.repeats_last_write(&plan, &values) {
            return;
        }
        let db = db.clone();
        match tokio::task::spawn_blocking(move || {
            db.record_metrics_history_point(recorded_at_epoch_sec, &metrics, &job_counts, plan)
        })
        .await
        {
            Ok(Ok(())) => self
                .cadence
                .commit_written(recorded_at_epoch_sec, &plan, values),
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "failed to persist metrics history sample");
            }
            Err(join_error) => {
                tracing::warn!(
                    error = %join_error,
                    "metrics history persistence task failed"
                );
            }
        }
    }
}

fn epoch_sec_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use weaver_server_core::operations::metrics::PipelineMetrics;
    use weaver_server_core::operations::{MetricsHistoryQueryData, MetricsHistoryTier};

    async fn raw_rows(db: &Database) -> usize {
        let db = db.clone();
        let result = tokio::task::spawn_blocking(move || {
            db.read_metrics_history(MetricsHistoryTier::Raw10s, 0, i64::MAX)
        })
        .await
        .unwrap()
        .unwrap();
        match result.data {
            MetricsHistoryQueryData::Raw(points) => points.len(),
            MetricsHistoryQueryData::Rollup(_) => unreachable!("raw tier"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_unchanged_sample_is_written_once() {
        let db = Database::open_in_memory().unwrap();
        let mut sampler = MetricsHistorySampler::default();
        let snapshot = PipelineMetrics::new().snapshot();
        let jobs = JobStatusCounts::from_jobs(&[]);
        // Two ticks inside one roll-up bucket, ten seconds apart.
        let first = 1_700_000_000 / 3600 * 3600 + 100;

        sampler.record(&db, first, snapshot.clone(), jobs).await;
        assert_eq!(raw_rows(&db).await, 1);
        tokio::time::advance(std::time::Duration::from_secs(10)).await;
        sampler
            .record(&db, first + 10, snapshot.clone(), jobs)
            .await;
        assert_eq!(raw_rows(&db).await, 1);

        // A changed sample is written.
        let mut changed = snapshot;
        changed.bytes_downloaded += 1;
        sampler.record(&db, first + 20, changed, jobs).await;
        assert_eq!(raw_rows(&db).await, 2);
    }
}
