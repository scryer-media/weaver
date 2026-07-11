use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Local, Utc};
use tracing::{info, warn};
use weaver_nntp::transfer::{
    QuotaRuntimeConfig, ServerTransferConfig, ServerTransferInitialState, ServerTransferRegistry,
    StableServerId,
};

use crate::bandwidth::{IspBandwidthCapConfig, IspBandwidthCapPeriod};
use crate::{Database, StateError};

use super::ServerDownloadUsage;
use super::model::{ServerConfig, ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod};

/// Authoritative live download-policy state for one configured server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerDownloadQuotaSnapshot {
    pub server_id: u32,
    pub lifetime_bytes: u64,
    pub used_bytes: u64,
    pub reserved_bytes: u64,
    pub remaining_bytes: Option<u64>,
    pub blocked: bool,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    pub timezone: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ServerQuotaWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

pub(crate) fn server_quota_window(
    now: DateTime<Local>,
    quota: &ServerDownloadQuotaConfig,
) -> Option<ServerQuotaWindow> {
    let period = match quota.period {
        ServerDownloadQuotaPeriod::OneTime => return None,
        ServerDownloadQuotaPeriod::Daily => IspBandwidthCapPeriod::Daily,
        ServerDownloadQuotaPeriod::Weekly => IspBandwidthCapPeriod::Weekly,
        ServerDownloadQuotaPeriod::Monthly => IspBandwidthCapPeriod::Monthly,
    };
    let policy = IspBandwidthCapConfig {
        enabled: true,
        period,
        limit_bytes: quota.limit_bytes,
        reset_time_minutes_local: quota.reset_time_minutes_local,
        weekly_reset_weekday: quota.weekly_reset_weekday,
        monthly_reset_day: quota.monthly_reset_day,
    };
    let window = crate::bandwidth::service::compute_window(now, &policy);
    Some(ServerQuotaWindow {
        start: window.starts_at().with_timezone(&Utc),
        end: window.ends_at().with_timezone(&Utc),
    })
}

pub(crate) fn local_timezone_label(now: DateTime<Local>) -> String {
    std::env::var("TZ").unwrap_or_else(|_| now.offset().to_string())
}

#[derive(Debug, Clone)]
struct ServerPolicyState {
    quota: ServerDownloadQuotaConfig,
    window: Option<ServerQuotaWindow>,
    generation: u64,
}

struct RegistryState {
    policies: HashMap<u32, ServerPolicyState>,
    last_flush: Instant,
}

/// Long-lived application policy registry shared by every NNTP client rebuild.
pub struct ServerTransferPolicyRegistry {
    db: Database,
    transfers: Arc<ServerTransferRegistry>,
    maintenance_gate: Mutex<()>,
    state: Mutex<RegistryState>,
    policy_revision: tokio::sync::watch::Sender<u64>,
}

impl ServerTransferPolicyRegistry {
    const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

    pub fn new(db: Database, servers: &[ServerConfig]) -> Result<Self, StateError> {
        let (policy_revision, _) = tokio::sync::watch::channel(0);
        let registry = Self {
            db,
            transfers: Arc::new(ServerTransferRegistry::new()),
            maintenance_gate: Mutex::new(()),
            state: Mutex::new(RegistryState {
                policies: HashMap::new(),
                last_flush: Instant::now(),
            }),
            policy_revision,
        };
        registry.reconfigure(servers)?;
        Ok(registry)
    }

    pub fn transfer_registry(&self) -> Arc<ServerTransferRegistry> {
        Arc::clone(&self.transfers)
    }

    pub(crate) fn with_maintenance_quiesced<T>(&self, operation: impl FnOnce() -> T) -> T {
        let _guard = self
            .maintenance_gate
            .lock()
            .expect("server policy maintenance gate poisoned");
        operation()
    }

    /// Drop live controls so the next reconfigure restores counters from the
    /// database. Used after a stable-state import where persisted usage must
    /// replace any pre-restore runtime state for overlapping server IDs.
    pub fn clear_runtime_state(&self) {
        {
            let mut state = self.state.lock().expect("server policy registry poisoned");
            state.policies.clear();
            state.last_flush = Instant::now();
        }
        self.transfers.clear();
        self.notify_changed();
    }

    pub fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.policy_revision.subscribe()
    }

    pub(crate) fn quota_rejection_is_current(
        &self,
        rejection: &weaver_nntp::transfer::QuotaRejection,
    ) -> bool {
        self.transfers.capacity_revision() == rejection.registry_capacity_revision
            && self
                .transfers
                .get(rejection.stable_server_id)
                .and_then(|control| control.quota_rejection_for(rejection.requested_body_bytes))
                .is_some_and(|current| current.capacity_revision == rejection.capacity_revision)
    }

    pub(crate) fn subscribe_capacity_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.transfers.subscribe_capacity_changes()
    }

    fn notify_changed(&self) {
        self.policy_revision
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    pub fn reconfigure(&self, servers: &[ServerConfig]) -> Result<(), StateError> {
        let _maintenance = self
            .maintenance_gate
            .lock()
            .expect("server policy maintenance gate poisoned");
        let now = Local::now();
        let configured_ids = servers
            .iter()
            .map(|server| server.id)
            .collect::<HashSet<_>>();
        let usages = servers
            .iter()
            .map(|server| {
                self.db
                    .server_download_usage(server.id)
                    .map(|usage| usage.unwrap_or_else(|| ServerDownloadUsage::empty(server.id)))
            })
            .collect::<Result<Vec<_>, StateError>>()?;
        let mut usage_updates = Vec::new();
        let mut state = self.state.lock().expect("server policy registry poisoned");

        let removed = state
            .policies
            .keys()
            .copied()
            .filter(|id| !configured_ids.contains(id))
            .collect::<Vec<_>>();
        for id in removed {
            state.policies.remove(&id);
            self.transfers.remove(StableServerId(id));
        }

        for (server, usage) in servers.iter().zip(usages) {
            let previous = state.policies.get(&server.id).cloned();
            let mut window = server_quota_window(now, &server.download_quota);
            let mut baseline = usage.quota_baseline_bytes.min(usage.lifetime_bytes);
            let anchors_changed = previous.as_ref().is_some_and(|previous| {
                quota_anchors_changed(&previous.quota, &server.download_quota)
            });
            let newly_enabled = previous
                .as_ref()
                .is_some_and(|previous| !previous.quota.enabled && server.download_quota.enabled);
            let persisted_window_matches = match (window, usage.window_start, usage.window_end) {
                (Some(current), Some(start), Some(end)) => {
                    current.start == start && current.end == end
                }
                (None, None, None) => true,
                _ => false,
            };

            if anchors_changed || newly_enabled || !persisted_window_matches {
                baseline = self
                    .transfers
                    .snapshot(StableServerId(server.id))
                    .lifetime_body_bytes
                    .max(usage.lifetime_bytes);
            }
            if matches!(
                server.download_quota.period,
                ServerDownloadQuotaPeriod::OneTime
            ) {
                window = None;
            }

            let generation = previous.as_ref().map_or_else(
                || initial_generation(server.id, window, usage.updated_at),
                |value| {
                    if anchors_changed || newly_enabled || !persisted_window_matches {
                        value.generation.wrapping_add(1).max(1)
                    } else {
                        value.generation
                    }
                },
            );
            let policy = ServerPolicyState {
                quota: server.download_quota.clone(),
                window,
                generation,
            };
            let config = transfer_config(server, &policy);
            let quota_used_bytes = usage.lifetime_bytes.saturating_sub(baseline);

            if previous.is_none() {
                self.transfers.restore(
                    StableServerId(server.id),
                    config,
                    ServerTransferInitialState {
                        lifetime_body_bytes: usage.lifetime_bytes,
                        quota_used_bytes,
                    },
                );
            } else {
                self.transfers.configure(StableServerId(server.id), config);
            }
            state.policies.insert(server.id, policy);

            if baseline != usage.quota_baseline_bytes
                || window.map(|value| value.start) != usage.window_start
                || window.map(|value| value.end) != usage.window_end
            {
                usage_updates.push(ServerDownloadUsage {
                    server_id: server.id,
                    lifetime_bytes: usage.lifetime_bytes.max(baseline),
                    quota_baseline_bytes: baseline,
                    window_start: window.map(|value| value.start),
                    window_end: window.map(|value| value.end),
                    updated_at: Utc::now(),
                });
            }
        }
        drop(state);
        self.notify_changed();
        for usage in usage_updates {
            self.db.upsert_server_download_usage(&usage)?;
        }
        Ok(())
    }

    pub fn snapshot(&self, server_id: u32) -> Option<ServerDownloadQuotaSnapshot> {
        let state = self.state.lock().expect("server policy registry poisoned");
        let policy = state.policies.get(&server_id)?;
        let snapshot = self.transfers.snapshot(StableServerId(server_id));
        Some(snapshot_for_policy(
            server_id,
            &snapshot,
            policy,
            Local::now(),
        ))
    }

    pub fn snapshots(&self) -> Vec<ServerDownloadQuotaSnapshot> {
        let now = Local::now();
        let state = self.state.lock().expect("server policy registry poisoned");
        let mut snapshots = state
            .policies
            .iter()
            .map(|(&server_id, policy)| {
                let snapshot = self.transfers.snapshot(StableServerId(server_id));
                snapshot_for_policy(server_id, &snapshot, policy, now)
            })
            .collect::<Vec<_>>();
        snapshots.sort_unstable_by_key(|snapshot| snapshot.server_id);
        snapshots
    }

    pub fn reset_usage(&self, server_id: u32) -> Result<ServerDownloadQuotaSnapshot, StateError> {
        let _maintenance = self
            .maintenance_gate
            .lock()
            .expect("server policy maintenance gate poisoned");
        let now = Local::now();
        let (usage, result) = {
            let mut state = self.state.lock().expect("server policy registry poisoned");
            let policy = state.policies.get_mut(&server_id).ok_or_else(|| {
                StateError::Database(format!("server {server_id} has no transfer policy"))
            })?;
            policy.generation = policy.generation.wrapping_add(1).max(1);
            policy.window = server_quota_window(now, &policy.quota);

            let before = self.transfers.snapshot(StableServerId(server_id));
            self.transfers.configure(
                StableServerId(server_id),
                transfer_config_parts(before.rate_bytes_per_sec, policy),
            );
            let after = self.transfers.snapshot(StableServerId(server_id));
            let usage = ServerDownloadUsage {
                server_id,
                lifetime_bytes: after.lifetime_body_bytes,
                quota_baseline_bytes: after.lifetime_body_bytes,
                window_start: policy.window.map(|value| value.start),
                window_end: policy.window.map(|value| value.end),
                updated_at: Utc::now(),
            };
            let result = snapshot_for_policy(server_id, &after, policy, now);
            (usage, result)
        };
        self.notify_changed();
        self.db.upsert_server_download_usage(&usage)?;
        info!(server_id, "server download quota usage reset");
        Ok(result)
    }

    pub fn refresh_windows(&self) -> Result<(), StateError> {
        let _maintenance = self
            .maintenance_gate
            .lock()
            .expect("server policy maintenance gate poisoned");
        self.refresh_windows_inner()
    }

    fn refresh_windows_inner(&self) -> Result<(), StateError> {
        let now = Local::now();
        let now_utc = now.with_timezone(&Utc);
        let mut changed = Vec::new();
        {
            let mut state = self.state.lock().expect("server policy registry poisoned");
            for (&server_id, policy) in &mut state.policies {
                let Some(window) = policy.window else {
                    continue;
                };
                if now_utc < window.end {
                    continue;
                }
                let Some(next_window) = server_quota_window(now, &policy.quota) else {
                    continue;
                };
                let before = self.transfers.snapshot(StableServerId(server_id));
                policy.window = Some(next_window);
                policy.generation = policy.generation.wrapping_add(1).max(1);
                self.transfers.configure(
                    StableServerId(server_id),
                    transfer_config_parts(before.rate_bytes_per_sec, policy),
                );
                changed.push((server_id, before.lifetime_body_bytes, policy.clone()));
            }
        }
        let any_changed = !changed.is_empty();
        if any_changed {
            self.notify_changed();
        }
        for (server_id, lifetime_bytes, policy) in changed {
            self.db.upsert_server_download_usage(&ServerDownloadUsage {
                server_id,
                lifetime_bytes,
                quota_baseline_bytes: lifetime_bytes,
                window_start: policy.window.map(|value| value.start),
                window_end: policy.window.map(|value| value.end),
                updated_at: Utc::now(),
            })?;
            info!(server_id, "server download quota window reset");
        }
        Ok(())
    }

    pub fn flush_usage(&self) -> Result<(), StateError> {
        let _maintenance = self
            .maintenance_gate
            .lock()
            .expect("server policy maintenance gate poisoned");
        self.refresh_windows_inner()?;
        let usages = {
            let state = self.state.lock().expect("server policy registry poisoned");
            state
                .policies
                .iter()
                .map(|(&server_id, policy)| {
                    let snapshot = self.transfers.snapshot(StableServerId(server_id));
                    ServerDownloadUsage {
                        server_id,
                        lifetime_bytes: snapshot.lifetime_body_bytes,
                        quota_baseline_bytes: snapshot
                            .lifetime_body_bytes
                            .saturating_sub(snapshot.quota_used_bytes),
                        window_start: policy.window.map(|value| value.start),
                        window_end: policy.window.map(|value| value.end),
                        updated_at: Utc::now(),
                    }
                })
                .collect::<Vec<_>>()
        };
        for usage in usages {
            self.db.upsert_server_download_usage(&usage)?;
        }
        Ok(())
    }

    pub fn spawn_maintenance(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let registry = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let Some(registry) = registry.upgrade() else {
                    break;
                };
                let should_flush = {
                    let mut state = registry
                        .state
                        .lock()
                        .expect("server policy registry poisoned");
                    if state.last_flush.elapsed() >= Self::FLUSH_INTERVAL {
                        state.last_flush = Instant::now();
                        true
                    } else {
                        false
                    }
                };
                let result = tokio::task::spawn_blocking(move || {
                    if should_flush {
                        registry.flush_usage()
                    } else {
                        registry.refresh_windows()
                    }
                })
                .await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        warn!(error = %error, "failed to maintain server download usage");
                    }
                    Err(error) => {
                        warn!(error = %error, "server download maintenance task failed");
                    }
                }
            }
        })
    }
}

fn transfer_config(server: &ServerConfig, policy: &ServerPolicyState) -> ServerTransferConfig {
    transfer_config_parts(server.max_download_speed, policy)
}

fn transfer_config_parts(
    rate_bytes_per_sec: u64,
    policy: &ServerPolicyState,
) -> ServerTransferConfig {
    ServerTransferConfig {
        rate_bytes_per_sec,
        quota: policy.quota.enabled.then(|| QuotaRuntimeConfig {
            limit_bytes: policy.quota.limit_bytes,
            generation: policy.generation,
            retry_at: policy.window.and_then(monotonic_deadline),
        }),
    }
}

fn monotonic_deadline(window: ServerQuotaWindow) -> Option<Instant> {
    let delay = (window.end - Utc::now()).to_std().ok()?;
    Some(Instant::now() + delay)
}

fn quota_anchors_changed(
    previous: &ServerDownloadQuotaConfig,
    next: &ServerDownloadQuotaConfig,
) -> bool {
    previous.period != next.period
        || previous.reset_time_minutes_local != next.reset_time_minutes_local
        || previous.weekly_reset_weekday != next.weekly_reset_weekday
        || previous.monthly_reset_day != next.monthly_reset_day
}

fn initial_generation(
    server_id: u32,
    window: Option<ServerQuotaWindow>,
    updated_at: DateTime<Utc>,
) -> u64 {
    let epoch = window
        .map(|value| value.start.timestamp())
        .unwrap_or_else(|| updated_at.timestamp())
        .unsigned_abs();
    (epoch << 16) ^ u64::from(server_id).max(1)
}

fn snapshot_for_policy(
    server_id: u32,
    snapshot: &weaver_nntp::transfer::ServerTransferSnapshot,
    policy: &ServerPolicyState,
    now: DateTime<Local>,
) -> ServerDownloadQuotaSnapshot {
    ServerDownloadQuotaSnapshot {
        server_id,
        lifetime_bytes: snapshot.lifetime_body_bytes,
        used_bytes: snapshot.quota_used_bytes,
        reserved_bytes: snapshot.quota_reserved_bytes,
        remaining_bytes: snapshot
            .quota_enabled
            .then_some(snapshot.quota_remaining_bytes),
        blocked: snapshot.quota_blocked,
        window_start: policy.window.map(|value| value.start),
        window_end: policy.window.map(|value| value.end),
        timezone: local_timezone_label(now),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Local, TimeZone};

    use crate::bandwidth::IspBandwidthCapWeekday;

    use super::*;

    fn quota(period: ServerDownloadQuotaPeriod) -> ServerDownloadQuotaConfig {
        ServerDownloadQuotaConfig {
            enabled: true,
            limit_bytes: 1_000,
            period,
            reset_time_minutes_local: 4 * 60,
            weekly_reset_weekday: IspBandwidthCapWeekday::Mon,
            monthly_reset_day: 31,
        }
    }

    fn quota_server(id: u32) -> ServerConfig {
        ServerConfig {
            id,
            host: format!("news-{id}.example.com"),
            port: 563,
            tls: true,
            username: None,
            password: None,
            connections: 1,
            active: true,
            supports_pipelining: true,
            priority: 0,
            backfill: false,
            retention_days: 0,
            max_download_speed: 0,
            download_quota: quota(ServerDownloadQuotaPeriod::OneTime),
            tls_ca_cert: None,
        }
    }

    #[test]
    fn one_time_quota_has_no_automatic_window() {
        assert!(
            server_quota_window(Local::now(), &quota(ServerDownloadQuotaPeriod::OneTime)).is_none()
        );
    }

    #[test]
    fn recurring_quota_reuses_dst_safe_bandwidth_windows() {
        let now = Local.with_ymd_and_hms(2026, 7, 9, 12, 0, 0).unwrap();
        let daily = server_quota_window(now, &quota(ServerDownloadQuotaPeriod::Daily)).unwrap();
        assert!(daily.start < now.with_timezone(&Utc));
        assert!(daily.end > now.with_timezone(&Utc));

        let monthly = server_quota_window(now, &quota(ServerDownloadQuotaPeriod::Monthly)).unwrap();
        assert!(monthly.start < monthly.end);
    }

    #[test]
    fn policy_change_subscription_retains_changes_until_observed() {
        let registry =
            ServerTransferPolicyRegistry::new(Database::open_in_memory().unwrap(), &[]).unwrap();
        let changes = registry.subscribe_changes();

        registry.reconfigure(&[]).unwrap();

        assert!(changes.has_changed().unwrap());
    }

    #[test]
    fn reset_after_rejection_makes_the_old_rejection_stale() {
        let server = quota_server(7);
        let db = Database::open_in_memory().unwrap();
        db.insert_server(&server).unwrap();
        let registry = ServerTransferPolicyRegistry::new(db, &[server]).unwrap();
        let control = registry.transfer_registry().control(StableServerId(7));
        let _reservation = control.try_reserve(1_000).unwrap();
        let rejection = control
            .try_reserve(1)
            .err()
            .expect("quota should reject an overbooked reservation");
        assert!(registry.quota_rejection_is_current(&rejection));

        registry.reset_usage(7).unwrap();

        assert!(!registry.quota_rejection_is_current(&rejection));
    }

    #[test]
    fn reset_notifies_waiters_even_when_persistence_fails() {
        let registry = ServerTransferPolicyRegistry::new(
            Database::open_in_memory().unwrap(),
            &[quota_server(8)],
        )
        .unwrap();
        let mut changes = registry.subscribe_changes();
        let before = *changes.borrow_and_update();

        assert!(registry.reset_usage(8).is_err());

        assert!(changes.has_changed().unwrap());
        assert_eq!(*changes.borrow_and_update(), before.wrapping_add(1));
    }

    #[test]
    fn multi_server_reconfigure_publishes_one_complete_revision() {
        let db = Database::open_in_memory().unwrap();
        let servers = [quota_server(9), quota_server(10)];
        for server in &servers {
            db.insert_server(server).unwrap();
        }
        let registry = ServerTransferPolicyRegistry::new(db, &[]).unwrap();
        let mut changes = registry.subscribe_changes();
        let before = *changes.borrow_and_update();

        registry.reconfigure(&servers).unwrap();

        assert!(changes.has_changed().unwrap());
        assert_eq!(*changes.borrow_and_update(), before.wrapping_add(1));
    }

    #[test]
    fn remainder_too_small_rejection_stays_current_until_capacity_changes() {
        let db = Database::open_in_memory().unwrap();
        let server = quota_server(11);
        db.insert_server(&server).unwrap();
        let registry = ServerTransferPolicyRegistry::new(db, &[server]).unwrap();
        let control = registry.transfer_registry().control(StableServerId(11));
        let _reservation = control.try_reserve(900).unwrap();
        let rejection = control
            .try_reserve(200)
            .err()
            .expect("remaining quota must reject an oversized estimate");
        assert!(!rejection.snapshot.quota_blocked);
        assert!(registry.quota_rejection_is_current(&rejection));

        control.update_config(ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(QuotaRuntimeConfig {
                limit_bytes: 1_200,
                generation: rejection.snapshot.quota_generation,
                retry_at: rejection.retry_at,
            }),
        });

        assert!(!registry.quota_rejection_is_current(&rejection));
    }

    #[test]
    fn snapshots_are_cached_reads_and_leave_rollover_to_maintenance() {
        let db = Database::open_in_memory().unwrap();
        let mut server = quota_server(12);
        server.download_quota.period = ServerDownloadQuotaPeriod::Daily;
        db.insert_server(&server).unwrap();
        let registry = ServerTransferPolicyRegistry::new(db.clone(), &[server]).unwrap();
        let expired = ServerQuotaWindow {
            start: Utc::now() - chrono::Duration::days(2),
            end: Utc::now() - chrono::Duration::days(1),
        };
        {
            let mut state = registry.state.lock().unwrap();
            state.policies.get_mut(&12).unwrap().window = Some(expired);
        }

        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            crate::persistence::sql_runtime::SqlRuntime::execute(
                datastore.read_exec(),
                "DROP TABLE server_download_usage",
                &[],
            )
            .await?;
            Ok(())
        })
        .unwrap();

        let snapshot = registry.snapshot(12).unwrap();
        assert_eq!(snapshot.window_end, Some(expired.end));
        let snapshots = registry.snapshots();
        assert_eq!(snapshots[0].window_end, Some(expired.end));
        assert_eq!(
            registry.state.lock().unwrap().policies[&12].window,
            Some(expired)
        );
    }
}
