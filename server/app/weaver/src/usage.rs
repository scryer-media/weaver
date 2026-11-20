// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Feeds the data-used-per-day ledger.
//!
//! Every few seconds this reads the running totals the download path already
//! keeps, hands them to the recorder, and writes what moved. The download
//! path itself is untouched: no article pays anything for the ledger.

use std::sync::Arc;
use weaver_server_core::operations::usage_history::OperationsUsageHistoryQueries;

use chrono::Local;
use tracing::warn;

use weaver_server_core::operations::{
    JobLifecycleMetricsSnapshot, JobResultKind, ServerStateReading, ServerUsageReading,
    StageReading, TorrentUsageReading, USAGE_FLUSH_INTERVAL_SECS, UsageReading, UsageRecorder,
};
use weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry;
use weaver_server_core::torrents::metrics::Published;
use weaver_server_core::torrents::service::TorrentService;
use weaver_server_core::{Database, SchedulerHandle};

/// Where the running totals come from.
#[derive(Clone)]
pub(crate) struct UsageSources {
    pub handle: SchedulerHandle,
    pub transfer_policy: Arc<ServerTransferPolicyRegistry>,
    /// The torrent client, when the server runs one.
    pub torrents: Option<TorrentService>,
}

impl UsageSources {
    pub(crate) async fn read(&self) -> UsageReading {
        let mut servers: Vec<ServerUsageReading> = Vec::new();
        for snapshot in self.transfer_policy.transfer_registry().snapshots() {
            server_entry(&mut servers, snapshot.stable_server_id.0).bytes =
                Some(snapshot.lifetime_body_bytes);
        }
        for snapshot in self.handle.server_metrics_snapshot() {
            let entry = server_entry(&mut servers, snapshot.stable_server_id);
            for attempt in &snapshot.attempts {
                // A quota hold never reached the server.
                if attempt.outcome == "quota_blocked" {
                    continue;
                }
                entry.articles_tried = entry.articles_tried.saturating_add(attempt.count);
                if attempt.outcome == "success" {
                    entry.articles_succeeded =
                        entry.articles_succeeded.saturating_add(attempt.count);
                }
            }
        }
        if let Some(pool) = self.handle.nntp_pool() {
            let count = pool.server_configs().len();
            let ids: Vec<Option<u32>> = (0..count)
                .map(|idx| {
                    pool.stable_server_id(weaver_nntp::ServerId(idx))
                        .map(|id| id.0)
                })
                .collect();
            // The health lock sits on the connection path: only field reads
            // inside it.
            let mut states: Vec<ServerStateReading> = Vec::with_capacity(count);
            {
                let health = pool.health().lock().await;
                let now = std::time::Instant::now();
                for idx in 0..count {
                    states.push(state_reading(health.server(idx).state(), now));
                }
            }
            for (id, state) in ids.into_iter().zip(states) {
                if let Some(id) = id {
                    server_entry(&mut servers, id).state = Some(state);
                }
            }
        }
        let lifecycle = self.handle.job_lifecycle_metrics_snapshot();
        let torrents = self
            .torrents
            .as_ref()
            .map(|torrents| torrent_readings(&torrents.metrics().published))
            .unwrap_or_default();
        let mut reading = job_reading(&lifecycle);
        reading.servers = servers;
        reading.categories = lifecycle.bytes_by_category;
        reading.torrents = torrents;
        reading
    }
}

/// The NZB jobs finished and repaired and each stage's timed runs, from the
/// counters the exporter keeps. A cancelled job never finished.
fn job_reading(lifecycle: &JobLifecycleMetricsSnapshot) -> UsageReading {
    UsageReading {
        jobs_finished: lifecycle
            .finished
            .iter()
            .filter(|finish| finish.result != JobResultKind::Cancelled.as_str())
            .map(|finish| finish.count)
            .sum(),
        jobs_repaired: lifecycle.jobs_repaired_total,
        stages: lifecycle
            .stage_duration
            .iter()
            .map(|(stage, histogram)| StageReading {
                stage,
                runs: histogram.count,
                total_ms: seconds_to_ms(histogram.sum),
            })
            .collect(),
        ..UsageReading::default()
    }
}

fn seconds_to_ms(seconds: f64) -> u64 {
    if seconds.is_finite() && seconds > 0.0 {
        (seconds * 1000.0).round() as u64
    } else {
        0
    }
}

/// Each torrent's seeding ledger and tracker hosts, as last published.
fn torrent_readings(published: &Published) -> Vec<TorrentUsageReading> {
    published
        .torrents
        .iter()
        .map(|torrent| TorrentUsageReading {
            job_id: torrent.job_id.0,
            trackers: torrent
                .trackers
                .iter()
                .map(|tracker| tracker.host.clone())
                .collect(),
            downloaded_bytes: torrent.downloaded_bytes,
            uploaded_bytes: torrent.uploaded_bytes,
            seeding_seconds: torrent.seeding_seconds,
        })
        .collect()
}

fn server_entry(servers: &mut Vec<ServerUsageReading>, id: u32) -> &mut ServerUsageReading {
    let at = match servers.iter().position(|s| s.server_id == id) {
        Some(at) => at,
        None => {
            servers.push(ServerUsageReading {
                server_id: id,
                bytes: None,
                articles_tried: 0,
                articles_succeeded: 0,
                state: None,
            });
            servers.len() - 1
        }
    };
    &mut servers[at]
}

fn state_reading(state: &weaver_nntp::ServerState, now: std::time::Instant) -> ServerStateReading {
    ServerStateReading {
        state: match state {
            weaver_nntp::ServerState::Healthy => "healthy",
            weaver_nntp::ServerState::Degraded { .. } => "degraded",
            weaver_nntp::ServerState::CoolingDown { .. } => "cooling_down",
            weaver_nntp::ServerState::Disabled { .. } => "disabled",
        },
        down: state.is_down(now),
    }
}

fn epoch_sec_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX)
        })
}

/// Read once and write what moved. A failed write keeps its counts for the
/// next one. A server removal waits for this, so what the batch holds of a
/// removed server is written before its rows move, never after.
pub(crate) async fn record_and_flush(
    recorder: &Arc<UsageRecorder>,
    sources: &UsageSources,
    db: &Database,
) {
    let _writes = recorder.hold_writes().await;
    let reading = sources.read().await;
    let today = Local::now().date_naive();
    recorder.observe(&reading, today, epoch_sec_now());
    let flush = recorder.take(today);
    if flush.is_empty() {
        return;
    }
    let write_db = db.clone();
    let batch = flush.clone();
    let failed = match tokio::task::spawn_blocking(move || write_db.write_usage_flush(&batch)).await
    {
        Ok(Ok(())) => false,
        Ok(Err(error)) => {
            warn!(error = %error, "failed to write data used per day");
            true
        }
        Err(error) => {
            warn!(error = %error, "data used per day write task failed");
            true
        }
    };
    if failed {
        recorder.restore(flush);
    } else if flush.expire_removed_at.is_some() {
        note_next_expiry(recorder, db).await;
    }
}

/// Read when a removed server's kept rows are next due to go, so a batch
/// asks for an expiry only then. A failed read leaves the time unknown, and
/// the next batch asks.
async fn note_next_expiry(recorder: &UsageRecorder, db: &Database) {
    let read_db = db.clone();
    match tokio::task::spawn_blocking(move || read_db.read_next_usage_expiry()).await {
        Ok(Ok(next)) => recorder.note_next_expiry(next),
        Ok(Err(error)) => {
            warn!(error = %error, "failed to read when kept data used rows expire");
            recorder.forget_next_expiry();
        }
        Err(error) => {
            warn!(error = %error, "kept data used expiry read task failed");
            recorder.forget_next_expiry();
        }
    }
}

/// Take the totals as they stand before any download, so a server's kept
/// lifetime is not read as today's use, and read when kept rows next expire.
pub(crate) async fn prime(recorder: &UsageRecorder, sources: &UsageSources, db: &Database) {
    let reading = sources.read().await;
    recorder.prime(&reading, epoch_sec_now());
    let _writes = recorder.hold_writes().await;
    note_next_expiry(recorder, db).await;
}

pub(crate) fn spawn_usage_history_task(
    recorder: Arc<UsageRecorder>,
    sources: UsageSources,
    db: Database,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_secs(USAGE_FLUSH_INTERVAL_SECS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            record_and_flush(&recorder, &sources, &db).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};
    use weaver_server_core::operations::usage_history::OperationsUsageHistoryQueries;
    use weaver_server_core::servers::persistence::ServersPersistenceQueries;

    use weaver_nntp::ServerState;
    use weaver_nntp::health::{CooldownReason, DisableReason};

    use chrono::NaiveDate;
    use weaver_server_core::operations::{
        JOBS_FINISHED, JOBS_REPAIRED, JobLifecycleMetrics, JobResultKind, JobStageKind,
        UsageConsumerKind, UsageRecorder,
    };

    use super::{job_reading, state_reading};

    // An idle instance whose kept rows came due writes their expiry once:
    // the batch that drops them reads that none are left, and the batches
    // after it are empty, so nothing is written per tick.
    #[tokio::test]
    async fn after_an_expiry_the_idle_batches_write_nothing() {
        use std::sync::Arc;
        use weaver_server_core::servers::{
            ServerConfig, ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod,
        };
        use weaver_server_core::{Database, SchedulerHandle, SharedPipelineState};

        let db = Database::open_in_memory().unwrap();
        db.insert_server(&ServerConfig {
            id: 3,
            host: "news.example.com".to_string(),
            port: 563,
            tls: true,
            username: None,
            password: None,
            connections: 4,
            active: false,
            supports_pipelining: false,
            pipelining_depth: None,
            tls_name_mismatch_certificate_der: None,
            priority: 0,
            backfill: false,
            retention_days: 0,
            max_download_speed: 0,
            download_quota: ServerDownloadQuotaConfig {
                enabled: true,
                limit_bytes: 1_000_000,
                period: ServerDownloadQuotaPeriod::Daily,
                monthly_reset_day: 1,
                ..ServerDownloadQuotaConfig::default()
            },
            tls_ca_cert: None,
        })
        .unwrap();
        // Removed long ago: its kept rows are past their reset.
        let removed_at = chrono::Local::now() - chrono::Duration::days(30);
        assert!(
            db.delete_server_at(3, removed_at)
                .unwrap()
                .kept_usage
                .is_some()
        );
        assert!(db.read_next_usage_expiry().unwrap().is_some());

        let state = SharedPipelineState::new(weaver_server_core::PipelineMetrics::new(), vec![]);
        let (events, _) = tokio::sync::broadcast::channel(16);
        let sources = super::UsageSources {
            handle: SchedulerHandle::new(events, state),
            transfer_policy: Arc::new(
                weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry::new(
                    db.clone(),
                    &[],
                )
                .unwrap(),
            ),
            torrents: None,
        };
        let recorder = Arc::new(UsageRecorder::new());
        super::prime(&recorder, &sources, &db).await;

        super::record_and_flush(&recorder, &sources, &db).await;
        assert!(db.read_removed_servers().unwrap().is_empty());
        for _ in 0..3 {
            super::record_and_flush(&recorder, &sources, &db).await;
        }
        // Without the read after the expiry, every batch would still ask.
        let today = chrono::Local::now().date_naive();
        assert!(recorder.take(today).is_empty());
    }

    // Each job counter moves once at its event, and the ledger counts what
    // moved once.
    #[test]
    fn jobs_and_stage_times_reach_the_ledger_once() {
        let metrics = JobLifecycleMetrics::new();
        let recorder = UsageRecorder::new();
        recorder.prime(&job_reading(&metrics.snapshot()), 0);
        metrics.note_finished(JobResultKind::Complete, "tv", None);
        metrics.note_finished(JobResultKind::Failed, "", None);
        metrics.note_finished(JobResultKind::Cancelled, "", None);
        metrics.note_job_repaired();
        metrics.note_stage_duration(JobStageKind::Repair, Duration::from_millis(1_250));
        let today = NaiveDate::from_ymd_opt(2026, 3, 4).unwrap();
        recorder.observe(&job_reading(&metrics.snapshot()), today, 5);
        recorder.observe(&job_reading(&metrics.snapshot()), today, 10);
        let rows = recorder.take(today).rows;
        let find = |kind: UsageConsumerKind, id: &str| {
            rows.iter()
                .find(|row| row.kind == kind.as_str() && row.consumer_id == id)
                .map(|row| row.counts)
                .unwrap_or_default()
        };
        assert_eq!(find(UsageConsumerKind::Job, JOBS_FINISHED).events, 2);
        assert_eq!(find(UsageConsumerKind::Job, JOBS_REPAIRED).events, 1);
        let repair = find(UsageConsumerKind::Stage, "repair");
        assert_eq!((repair.events, repair.duration_ms), (1, 1_250));
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn down_means_disabled_or_a_transport_cooldown_still_running() {
        let now = Instant::now();
        let later = now + Duration::from_secs(30);
        let reading = |state: &ServerState| {
            let reading = state_reading(state, now);
            (reading.state, reading.down)
        };
        assert_eq!(reading(&ServerState::Healthy), ("healthy", false));
        assert_eq!(
            reading(&ServerState::Degraded {
                consecutive_failures: 2
            }),
            ("degraded", false)
        );
        assert_eq!(
            reading(&ServerState::Disabled {
                until: later,
                reason: DisableReason::AuthFailure,
            }),
            ("disabled", true)
        );
        assert_eq!(
            reading(&ServerState::Disabled {
                until: now,
                reason: DisableReason::AuthFailure,
            }),
            ("disabled", false)
        );
        assert_eq!(
            reading(&ServerState::CoolingDown {
                until: later,
                reason: CooldownReason::Transport,
                resume_degraded: None,
            }),
            ("cooling_down", true)
        );
        // A short hold for capacity is the server busy, not down.
        assert_eq!(
            reading(&ServerState::CoolingDown {
                until: later,
                reason: CooldownReason::Capacity,
                resume_degraded: None,
            }),
            ("cooling_down", false)
        );
    }
}
