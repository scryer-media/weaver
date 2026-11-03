// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Feeds the data-used-per-day ledger.
//!
//! Every few seconds this reads the running totals the download path already
//! keeps, hands them to the recorder, and writes what moved. The download
//! path itself is untouched: no article pays anything for the ledger.

use std::sync::Arc;

use chrono::Local;
use tracing::warn;

use weaver_server_core::operations::{
    ServerStateReading, ServerUsageReading, USAGE_FLUSH_INTERVAL_SECS, UsageReading, UsageRecorder,
};
use weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry;
use weaver_server_core::{Database, SchedulerHandle};

/// Where the running totals come from.
#[derive(Clone)]
pub(crate) struct UsageSources {
    pub handle: SchedulerHandle,
    pub transfer_policy: Arc<ServerTransferPolicyRegistry>,
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
        UsageReading {
            servers,
            categories: self
                .handle
                .job_lifecycle_metrics_snapshot()
                .bytes_by_category,
        }
    }
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
/// next one.
pub(crate) async fn record_and_flush(
    recorder: &Arc<UsageRecorder>,
    sources: &UsageSources,
    db: &Database,
) {
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
    }
}

/// Take the totals as they stand before any download, so a server's kept
/// lifetime is not read as today's use.
pub(crate) async fn prime(recorder: &UsageRecorder, sources: &UsageSources) {
    let reading = sources.read().await;
    recorder.prime(&reading, epoch_sec_now());
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

    use weaver_nntp::ServerState;
    use weaver_nntp::health::{CooldownReason, DisableReason};

    use super::state_reading;

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
