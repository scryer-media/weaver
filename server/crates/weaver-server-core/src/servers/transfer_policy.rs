use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Local, Utc};
use tracing::{info, warn};
use weaver_nntp::transfer::{
    QuotaRuntimeConfig, ServerTransferConfig, ServerTransferInitialState, ServerTransferRegistry,
    StableServerId, TransferScope,
};

use crate::{Database, StateError};

use super::ServerDownloadUsage;
use super::model::{ServerConfig, ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod};

// Authoritative live download-policy state for one configured server.
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
    let window = crate::bandwidth::service::compute_window(now, quota)?;
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

// One holder of a download policy: a server or an egress.
struct QuotaHolder<'a> {
    id: u32,
    rate_bytes_per_sec: u64,
    quota: &'a ServerDownloadQuotaConfig,
}

// The policies and live controls for one kind of holder.
struct PolicyBook {
    scope: TransferScope,
    transfers: Arc<ServerTransferRegistry>,
    policies: Mutex<HashMap<u32, ServerPolicyState>>,
}

impl PolicyBook {
    fn new(scope: TransferScope, transfers: ServerTransferRegistry) -> Self {
        Self {
            scope,
            transfers: Arc::new(transfers),
            policies: Mutex::new(HashMap::new()),
        }
    }

    fn policies(&self) -> std::sync::MutexGuard<'_, HashMap<u32, ServerPolicyState>> {
        self.policies
            .lock()
            .expect("server policy registry poisoned")
    }
}

// Long-lived application policy registry shared by every NNTP client and
// network runtime rebuild. It holds the download policy of every server and
// of every egress.
pub struct ServerTransferPolicyRegistry {
    db: Database,
    servers: PolicyBook,
    egresses: PolicyBook,
    maintenance_gate: Mutex<()>,
    last_flush: Mutex<Instant>,
    // The usage each control had when it was last written, so an idle
    // daemon's flush writes nothing.
    flushed_usage: Mutex<HashMap<(TransferScope, u32), FlushedUsage>>,
    policy_revision: tokio::sync::watch::Sender<u64>,
}

// The parts of a stored usage row that change between flushes.
#[derive(Clone, Copy, PartialEq, Eq)]
struct FlushedUsage {
    lifetime_bytes: u64,
    quota_baseline_bytes: u64,
    window_end: Option<DateTime<Utc>>,
}

impl FlushedUsage {
    fn of(usage: &ServerDownloadUsage) -> Self {
        Self {
            lifetime_bytes: usage.lifetime_bytes,
            quota_baseline_bytes: usage.quota_baseline_bytes,
            window_end: usage.window_end,
        }
    }
}

// A quota window that rolled over in memory and still has to be written.
struct WindowReset {
    scope: TransferScope,
    id: u32,
    lifetime_bytes: u64,
    policy: ServerPolicyState,
}

impl ServerTransferPolicyRegistry {
    const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

    pub fn new(db: Database, servers: &[ServerConfig]) -> Result<Self, StateError> {
        let (policy_revision, _) = tokio::sync::watch::channel(0);
        let server_transfers = ServerTransferRegistry::with_scope(TransferScope::Server);
        let egress_transfers = server_transfers.sibling(TransferScope::Egress);
        let registry = Self {
            db,
            servers: PolicyBook::new(TransferScope::Server, server_transfers),
            egresses: PolicyBook::new(TransferScope::Egress, egress_transfers),
            maintenance_gate: Mutex::new(()),
            last_flush: Mutex::new(Instant::now()),
            flushed_usage: Mutex::new(HashMap::new()),
            policy_revision,
        };
        registry.reconfigure(servers)?;
        Ok(registry)
    }

    pub fn transfer_registry(&self) -> Arc<ServerTransferRegistry> {
        Arc::clone(&self.servers.transfers)
    }

    // The controls every connection's egress is metered by. Network runtime
    // rebuilds share it, so an egress keeps its counters across them.
    pub fn egress_transfer_registry(&self) -> Arc<ServerTransferRegistry> {
        Arc::clone(&self.egresses.transfers)
    }

    fn book(&self, scope: TransferScope) -> &PolicyBook {
        match scope {
            TransferScope::Server => &self.servers,
            TransferScope::Egress => &self.egresses,
        }
    }

    fn books(&self) -> [&PolicyBook; 2] {
        [&self.servers, &self.egresses]
    }

    fn load_usage(&self, scope: TransferScope, id: u32) -> Result<ServerDownloadUsage, StateError> {
        let usage = match scope {
            TransferScope::Server => self.db.server_download_usage(id)?,
            TransferScope::Egress => self.db.egress_download_usage(id)?,
        };
        Ok(usage.unwrap_or_else(|| ServerDownloadUsage::empty(id)))
    }

    fn store_usage(
        &self,
        scope: TransferScope,
        usage: &ServerDownloadUsage,
    ) -> Result<(), StateError> {
        match scope {
            TransferScope::Server => self.db.upsert_server_download_usage(usage),
            TransferScope::Egress => self.db.upsert_egress_download_usage(usage),
        }
    }

    // Drop live controls so the next reconfigure restores counters from the
    // database. Used after a stable-state import where persisted usage must
    // replace any pre-restore runtime state for overlapping IDs.
    pub fn clear_runtime_state(&self) {
        for book in self.books() {
            book.policies().clear();
            book.transfers.clear();
        }
        *self
            .last_flush
            .lock()
            .expect("server policy registry poisoned") = Instant::now();
        self.flushed_usage
            .lock()
            .expect("server policy registry poisoned")
            .clear();
        self.notify_changed();
    }

    pub fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.policy_revision.subscribe()
    }

    // Whether the control that turned a request away, server or egress,
    // would still turn it away.
    pub(crate) fn quota_rejection_is_current(
        &self,
        rejection: &weaver_nntp::transfer::QuotaRejection,
    ) -> bool {
        let transfers = &self.book(rejection.scope).transfers;
        transfers.capacity_revision() == rejection.registry_capacity_revision
            && transfers
                .get(rejection.stable_server_id)
                .and_then(|control| control.quota_rejection_for(rejection.requested_body_bytes))
                .is_some_and(|current| current.capacity_revision == rejection.capacity_revision)
    }

    // Capacity changes for servers and egresses alike: the two registries
    // share one signal.
    pub(crate) fn subscribe_capacity_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.servers.transfers.subscribe_capacity_changes()
    }

    fn notify_changed(&self) {
        self.policy_revision
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    pub fn reconfigure(&self, servers: &[ServerConfig]) -> Result<(), StateError> {
        let holders = servers
            .iter()
            .map(|server| QuotaHolder {
                id: server.id,
                rate_bytes_per_sec: server.max_download_speed,
                quota: &server.download_quota,
            })
            .collect::<Vec<_>>();
        self.reconfigure_book(TransferScope::Server, &holders)
    }

    // Apply every egress's speed limit and download quota. Counters carry
    // over for an egress that stays; one that is new is restored from its
    // stored usage.
    pub fn reconfigure_egresses(
        &self,
        egresses: &[crate::proxies::EgressInterface],
    ) -> Result<(), StateError> {
        let holders = egresses
            .iter()
            .map(|egress| QuotaHolder {
                id: egress.id,
                rate_bytes_per_sec: egress.max_download_speed,
                quota: &egress.download_quota,
            })
            .collect::<Vec<_>>();
        self.reconfigure_book(TransferScope::Egress, &holders)
    }

    // Start or stop every egress quota counting bytes. Usage already counted
    // in a window is kept; while stopped no egress quota turns work away.
    pub fn set_egress_quota_metering(&self, enabled: bool) {
        self.egresses.transfers.set_quota_metering(enabled);
        self.notify_changed();
    }

    // Start or stop one egress's quota counting bytes. It keeps that setting
    // whatever later happens to every egress's.
    pub fn set_one_egress_quota_metering(&self, egress_id: u32, enabled: bool) {
        self.egresses
            .transfers
            .set_quota_metering_for(StableServerId(egress_id), enabled);
        self.notify_changed();
    }

    // Hand one egress's quota counting back to every egress's setting.
    pub fn clear_one_egress_quota_metering(&self, egress_id: u32) {
        self.egresses
            .transfers
            .clear_quota_metering_for(StableServerId(egress_id));
        self.notify_changed();
    }

    fn reconfigure_book(
        &self,
        scope: TransferScope,
        holders: &[QuotaHolder<'_>],
    ) -> Result<(), StateError> {
        let _maintenance = self
            .maintenance_gate
            .lock()
            .expect("server policy maintenance gate poisoned");
        let book = self.book(scope);
        let now = crate::e2e_clock::local_now();
        let configured_ids = holders
            .iter()
            .map(|holder| holder.id)
            .collect::<HashSet<_>>();
        let usages = holders
            .iter()
            .map(|holder| self.load_usage(scope, holder.id))
            .collect::<Result<Vec<_>, StateError>>()?;
        let mut usage_updates = Vec::new();
        let mut policies = book.policies();

        let removed = policies
            .keys()
            .copied()
            .filter(|id| !configured_ids.contains(id))
            .collect::<Vec<_>>();
        for id in removed {
            policies.remove(&id);
            book.transfers.remove(StableServerId(id));
        }

        for (holder, usage) in holders.iter().zip(usages) {
            let previous = policies.get(&holder.id).cloned();
            let mut window = server_quota_window(now, holder.quota);
            let mut baseline = usage.quota_baseline_bytes.min(usage.lifetime_bytes);
            let anchors_changed = previous
                .as_ref()
                .is_some_and(|previous| quota_anchors_changed(&previous.quota, holder.quota));
            let newly_enabled = previous
                .as_ref()
                .is_some_and(|previous| !previous.quota.enabled && holder.quota.enabled);
            let persisted_window_matches = match (window, usage.window_start, usage.window_end) {
                (Some(current), Some(start), Some(end)) => {
                    current.start == start && current.end == end
                }
                (None, None, None) => true,
                _ => false,
            };

            if anchors_changed || newly_enabled || !persisted_window_matches {
                baseline = book
                    .transfers
                    .snapshot(StableServerId(holder.id))
                    .lifetime_body_bytes
                    .max(usage.lifetime_bytes);
            }
            if matches!(holder.quota.period, ServerDownloadQuotaPeriod::OneTime) {
                window = None;
            }

            let generation = previous.as_ref().map_or_else(
                || initial_generation(holder.id, window, usage.updated_at),
                |value| {
                    if anchors_changed || newly_enabled || !persisted_window_matches {
                        value.generation.wrapping_add(1).max(1)
                    } else {
                        value.generation
                    }
                },
            );
            let policy = ServerPolicyState {
                quota: holder.quota.clone(),
                window,
                generation,
            };
            let config = transfer_config_parts(holder.rate_bytes_per_sec, &policy);
            let quota_used_bytes = usage.lifetime_bytes.saturating_sub(baseline);

            if previous.is_none() {
                book.transfers.restore(
                    StableServerId(holder.id),
                    config,
                    ServerTransferInitialState {
                        lifetime_body_bytes: usage.lifetime_bytes,
                        quota_used_bytes,
                    },
                );
            } else {
                book.transfers.configure(StableServerId(holder.id), config);
            }
            policies.insert(holder.id, policy);

            if baseline != usage.quota_baseline_bytes
                || window.map(|value| value.start) != usage.window_start
                || window.map(|value| value.end) != usage.window_end
            {
                usage_updates.push(ServerDownloadUsage {
                    server_id: holder.id,
                    lifetime_bytes: usage.lifetime_bytes.max(baseline),
                    quota_baseline_bytes: baseline,
                    window_start: window.map(|value| value.start),
                    window_end: window.map(|value| value.end),
                    updated_at: crate::e2e_clock::utc_now(),
                });
            }
        }
        drop(policies);
        self.notify_changed();
        for usage in usage_updates {
            self.store_usage(scope, &usage)?;
        }
        Ok(())
    }

    fn snapshot_in(&self, scope: TransferScope, id: u32) -> Option<ServerDownloadQuotaSnapshot> {
        let book = self.book(scope);
        let policies = book.policies();
        let policy = policies.get(&id)?;
        let snapshot = book.transfers.snapshot(StableServerId(id));
        Some(snapshot_for_policy(
            id,
            &snapshot,
            policy,
            crate::e2e_clock::local_now(),
        ))
    }

    fn snapshots_in(&self, scope: TransferScope) -> Vec<ServerDownloadQuotaSnapshot> {
        let now = crate::e2e_clock::local_now();
        let book = self.book(scope);
        let policies = book.policies();
        let mut snapshots = policies
            .iter()
            .map(|(&id, policy)| {
                let snapshot = book.transfers.snapshot(StableServerId(id));
                snapshot_for_policy(id, &snapshot, policy, now)
            })
            .collect::<Vec<_>>();
        snapshots.sort_unstable_by_key(|snapshot| snapshot.server_id);
        snapshots
    }

    pub fn snapshot(&self, server_id: u32) -> Option<ServerDownloadQuotaSnapshot> {
        self.snapshot_in(TransferScope::Server, server_id)
    }

    pub fn snapshots(&self) -> Vec<ServerDownloadQuotaSnapshot> {
        self.snapshots_in(TransferScope::Server)
    }

    // The live usage of one egress; its id is carried in `server_id`.
    pub fn egress_snapshot(&self, egress_id: u32) -> Option<ServerDownloadQuotaSnapshot> {
        self.snapshot_in(TransferScope::Egress, egress_id)
    }

    // The live usage of every egress, by id; each id is carried in
    // `server_id`.
    pub fn egress_snapshots(&self) -> Vec<ServerDownloadQuotaSnapshot> {
        self.snapshots_in(TransferScope::Egress)
    }

    pub fn reset_usage(&self, server_id: u32) -> Result<ServerDownloadQuotaSnapshot, StateError> {
        self.reset_usage_in(TransferScope::Server, server_id)
    }

    pub fn reset_egress_usage(
        &self,
        egress_id: u32,
    ) -> Result<ServerDownloadQuotaSnapshot, StateError> {
        self.reset_usage_in(TransferScope::Egress, egress_id)
    }

    fn reset_usage_in(
        &self,
        scope: TransferScope,
        server_id: u32,
    ) -> Result<ServerDownloadQuotaSnapshot, StateError> {
        let _maintenance = self
            .maintenance_gate
            .lock()
            .expect("server policy maintenance gate poisoned");
        let now = crate::e2e_clock::local_now();
        let book = self.book(scope);
        let (usage, result) = {
            let mut policies = book.policies();
            let policy = policies.get_mut(&server_id).ok_or_else(|| {
                let scope = match scope {
                    TransferScope::Server => "server",
                    TransferScope::Egress => "egress",
                };
                StateError::Database(format!("{scope} {server_id} has no transfer policy"))
            })?;
            policy.generation = policy.generation.wrapping_add(1).max(1);
            policy.window = server_quota_window(now, &policy.quota);

            let before = book.transfers.snapshot(StableServerId(server_id));
            book.transfers.configure(
                StableServerId(server_id),
                transfer_config_parts(before.rate_bytes_per_sec, policy),
            );
            let after = book.transfers.snapshot(StableServerId(server_id));
            let usage = ServerDownloadUsage {
                server_id,
                lifetime_bytes: after.lifetime_body_bytes,
                quota_baseline_bytes: after.lifetime_body_bytes,
                window_start: policy.window.map(|value| value.start),
                window_end: policy.window.map(|value| value.end),
                updated_at: crate::e2e_clock::utc_now(),
            };
            let result = snapshot_for_policy(server_id, &after, policy, now);
            (usage, result)
        };
        self.notify_changed();
        self.store_usage(scope, &usage)?;
        info!(?scope, id = server_id, "download quota usage reset");
        Ok(result)
    }

    pub fn refresh_windows(&self) -> Result<(), StateError> {
        let _maintenance = self
            .maintenance_gate
            .lock()
            .expect("server policy maintenance gate poisoned");
        let resets = self.advance_windows();
        self.persist_window_resets(resets)
    }

    // Roll every elapsed quota window over in memory. Cheap when nothing
    // elapsed, which is nearly every call; the returned resets still have to
    // be written with [`persist_window_resets`](Self::persist_window_resets).
    fn advance_windows(&self) -> Vec<WindowReset> {
        let now = crate::e2e_clock::local_now();
        let now_utc = now.with_timezone(&Utc);
        let mut resets = Vec::new();
        for book in self.books() {
            let mut policies = book.policies();
            for (&id, policy) in policies.iter_mut() {
                let Some(window) = policy.window else {
                    continue;
                };
                if now_utc < window.end {
                    continue;
                }
                let Some(next_window) = server_quota_window(now, &policy.quota) else {
                    continue;
                };
                let before = book.transfers.snapshot(StableServerId(id));
                policy.window = Some(next_window);
                policy.generation = policy.generation.wrapping_add(1).max(1);
                book.transfers.configure(
                    StableServerId(id),
                    transfer_config_parts(before.rate_bytes_per_sec, policy),
                );
                resets.push(WindowReset {
                    scope: book.scope,
                    id,
                    lifetime_bytes: before.lifetime_body_bytes,
                    policy: policy.clone(),
                });
            }
        }
        if !resets.is_empty() {
            self.notify_changed();
        }
        resets
    }

    fn persist_window_resets(&self, resets: Vec<WindowReset>) -> Result<(), StateError> {
        for reset in resets {
            let usage = ServerDownloadUsage {
                server_id: reset.id,
                lifetime_bytes: reset.lifetime_bytes,
                quota_baseline_bytes: reset.lifetime_bytes,
                window_start: reset.policy.window.map(|value| value.start),
                window_end: reset.policy.window.map(|value| value.end),
                updated_at: crate::e2e_clock::utc_now(),
            };
            self.store_usage(reset.scope, &usage)?;
            self.record_flushed(reset.scope, &usage);
            match reset.scope {
                TransferScope::Server => {
                    info!(server_id = reset.id, "server download quota window reset")
                }
                TransferScope::Egress => {
                    info!(egress_id = reset.id, "egress download quota window reset")
                }
            }
        }
        Ok(())
    }

    fn record_flushed(&self, scope: TransferScope, usage: &ServerDownloadUsage) {
        self.flushed_usage
            .lock()
            .expect("server policy registry poisoned")
            .insert((scope, usage.server_id), FlushedUsage::of(usage));
    }

    // The current usage of every control, with whether it differs from what
    // was last written.
    fn usage_rows(&self) -> Vec<(TransferScope, ServerDownloadUsage, bool)> {
        let flushed = self
            .flushed_usage
            .lock()
            .expect("server policy registry poisoned");
        let mut rows = Vec::new();
        for book in self.books() {
            let policies = book.policies();
            rows.extend(policies.iter().map(|(&id, policy)| {
                let snapshot = book.transfers.snapshot(StableServerId(id));
                let usage = ServerDownloadUsage {
                    server_id: id,
                    lifetime_bytes: snapshot.lifetime_body_bytes,
                    quota_baseline_bytes: snapshot
                        .lifetime_body_bytes
                        .saturating_sub(snapshot.quota_used_bytes),
                    window_start: policy.window.map(|value| value.start),
                    window_end: policy.window.map(|value| value.end),
                    updated_at: crate::e2e_clock::utc_now(),
                };
                let changed = flushed.get(&(book.scope, id)) != Some(&FlushedUsage::of(&usage));
                (book.scope, usage, changed)
            }));
        }
        rows
    }

    // Whether a flush now would write anything.
    fn usage_changed_since_flush(&self) -> bool {
        self.usage_rows().iter().any(|(_, _, changed)| *changed)
    }

    pub fn flush_usage(&self) -> Result<(), StateError> {
        let _maintenance = self
            .maintenance_gate
            .lock()
            .expect("server policy maintenance gate poisoned");
        let resets = self.advance_windows();
        self.persist_window_resets(resets)?;
        for (scope, usage, changed) in self.usage_rows() {
            if !changed {
                continue;
            }
            self.store_usage(scope, &usage)?;
            self.record_flushed(scope, &usage);
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
                // The in-memory part runs here: with no elapsed window and
                // no new bytes, an idle daemon touches neither a worker
                // thread nor the database.
                let resets = {
                    let _maintenance = registry
                        .maintenance_gate
                        .lock()
                        .expect("server policy maintenance gate poisoned");
                    registry.advance_windows()
                };
                let should_flush = {
                    let mut last_flush = registry
                        .last_flush
                        .lock()
                        .expect("server policy registry poisoned");
                    if last_flush.elapsed() >= Self::FLUSH_INTERVAL
                        && registry.usage_changed_since_flush()
                    {
                        *last_flush = Instant::now();
                        true
                    } else {
                        false
                    }
                };
                if resets.is_empty() && !should_flush {
                    continue;
                }
                let result = tokio::task::spawn_blocking(move || {
                    {
                        let _maintenance = registry
                            .maintenance_gate
                            .lock()
                            .expect("server policy maintenance gate poisoned");
                        registry.persist_window_resets(resets)?;
                    }
                    if should_flush {
                        registry.flush_usage()
                    } else {
                        Ok(())
                    }
                })
                .await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        warn!(error = %error, "failed to maintain download usage");
                    }
                    Err(error) => {
                        warn!(error = %error, "download usage maintenance task failed");
                    }
                }
            }
        })
    }
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
    let delay = (window.end - crate::e2e_clock::utc_now()).to_std().ok()?;
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

    use crate::bandwidth::QuotaWeekday;

    use super::*;

    fn quota(period: ServerDownloadQuotaPeriod) -> ServerDownloadQuotaConfig {
        ServerDownloadQuotaConfig {
            enabled: true,
            limit_bytes: 1_000,
            period,
            reset_time_minutes_local: 4 * 60,
            weekly_reset_weekday: QuotaWeekday::Mon,
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
            pipelining_depth: None,
            tls_name_mismatch_certificate_der: None,
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
    fn resetting_egress_usage_preserves_lifetime_and_other_scopes() {
        let db = Database::open_in_memory().unwrap();
        let egress = db
            .create_egress_interface(&crate::proxies::EgressInterface {
                id: 0,
                name: "Metered link".into(),
                binding: crate::proxies::EgressBinding::SourceAddress {
                    address: "127.0.0.1".parse().unwrap(),
                },
                enabled: true,
                max_download_speed: 0,
                download_quota: quota(ServerDownloadQuotaPeriod::OneTime),
            })
            .unwrap();
        let server = quota_server(egress.id);
        db.insert_server(&server).unwrap();
        let registry = ServerTransferPolicyRegistry::new(db.clone(), &[server]).unwrap();
        registry
            .reconfigure_egresses(std::slice::from_ref(&egress))
            .unwrap();
        let control = registry
            .egress_transfer_registry()
            .control(StableServerId(egress.id));
        let mut permit = control.try_reserve(1_000).unwrap();
        permit.record_blocking(1_000);
        permit.finish();
        let rejection = control.try_reserve(1).err().unwrap();
        let server_before = registry.snapshot(egress.id).unwrap();
        let changes = registry.subscribe_changes();
        let reset = registry.reset_egress_usage(egress.id).unwrap();
        assert_eq!(reset.lifetime_bytes, 1_000);
        assert_eq!(reset.used_bytes, 0);
        assert!(!reset.blocked);
        assert!(!registry.quota_rejection_is_current(&rejection));
        assert!(changes.has_changed().unwrap());
        assert_eq!(registry.snapshot(egress.id).unwrap(), server_before);
        let stored = db.egress_download_usage(egress.id).unwrap().unwrap();
        assert_eq!(stored.lifetime_bytes, 1_000);
        assert_eq!(stored.quota_baseline_bytes, 1_000);
    }

    #[test]
    fn an_egress_rejection_is_judged_against_the_egress_not_a_server_with_its_id() {
        // A server and an egress share the id 7; only the egress is metered.
        let mut server = quota_server(7);
        server.download_quota.enabled = false;
        let db = Database::open_in_memory().unwrap();
        db.insert_server(&server).unwrap();
        let registry = ServerTransferPolicyRegistry::new(db, &[server]).unwrap();
        let mut egress = crate::proxies::EgressInterface {
            id: 7,
            name: "Metered link".into(),
            binding: crate::proxies::EgressBinding::System,
            enabled: true,
            max_download_speed: 0,
            download_quota: quota(ServerDownloadQuotaPeriod::OneTime),
        };
        registry
            .reconfigure_egresses(std::slice::from_ref(&egress))
            .unwrap();
        let control = registry
            .egress_transfer_registry()
            .control(StableServerId(7));
        let limit = egress.download_quota.limit_bytes;
        let _reservation = control.try_reserve(limit).unwrap();
        let rejection = control
            .try_reserve(1)
            .err()
            .expect("the egress quota should reject an overbooked reservation");
        assert_eq!(rejection.scope, TransferScope::Egress);
        assert!(registry.quota_rejection_is_current(&rejection));
        assert!(registry.egress_snapshot(7).unwrap().blocked);
        assert!(!registry.snapshot(7).unwrap().blocked);

        // The server with the same id has nothing to refuse; only a change to
        // the egress makes the rejection stale.
        assert!(
            registry
                .transfer_registry()
                .control(StableServerId(7))
                .quota_rejection_for(1)
                .is_none()
        );
        egress.download_quota.limit_bytes = limit * 2;
        registry
            .reconfigure_egresses(std::slice::from_ref(&egress))
            .unwrap();
        assert!(!registry.quota_rejection_is_current(&rejection));
        assert!(!registry.egress_snapshot(7).unwrap().blocked);
    }

    #[test]
    fn an_egress_given_a_deleted_egress_id_follows_the_metering_for_every_egress() {
        let db = Database::open_in_memory().unwrap();
        let registry = ServerTransferPolicyRegistry::new(db, &[]).unwrap();
        let egress = crate::proxies::EgressInterface {
            id: 7,
            name: "Metered link".into(),
            binding: crate::proxies::EgressBinding::System,
            enabled: true,
            max_download_speed: 0,
            download_quota: quota(ServerDownloadQuotaPeriod::OneTime),
        };
        registry
            .reconfigure_egresses(std::slice::from_ref(&egress))
            .unwrap();
        registry.set_egress_quota_metering(true);
        registry.set_one_egress_quota_metering(7, false);
        let transfers = registry.egress_transfer_registry();
        assert!(!transfers.quota_metering_of(StableServerId(7)));

        // Deleted, then made again under the same id.
        registry.reconfigure_egresses(&[]).unwrap();
        registry
            .reconfigure_egresses(std::slice::from_ref(&egress))
            .unwrap();
        assert!(transfers.quota_metering_of(StableServerId(7)));
        let control = transfers.control(StableServerId(7));
        let _reservation = control
            .try_reserve(egress.download_quota.limit_bytes)
            .unwrap();
        assert!(control.try_reserve(1).is_err());
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
        registry.servers.policies().get_mut(&12).unwrap().window = Some(expired);

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
        assert_eq!(registry.servers.policies()[&12].window, Some(expired));
    }
}
