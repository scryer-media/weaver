// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Local, Utc};
use tracing::{info, warn};
use weaver_nntp::transfer::{
    QuotaRuntimeConfig, ServerTransferConfig, ServerTransferInitialState, ServerTransferRegistry,
    StableServerId, TransferScope,
};

use crate::bandwidth::service::QuotaCalendar;
use crate::bandwidth::{Direction, QuotaState, RateLimits, effective_rate};
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

pub(crate) fn quota_window(
    now: DateTime<Local>,
    calendar: QuotaCalendar,
) -> Option<ServerQuotaWindow> {
    let window = crate::bandwidth::service::compute_window(now, calendar)?;
    Some(ServerQuotaWindow {
        start: window.starts_at().with_timezone(&Utc),
        end: window.ends_at().with_timezone(&Utc),
    })
}

pub(crate) fn local_timezone_label(now: DateTime<Local>) -> String {
    std::env::var("TZ").unwrap_or_else(|_| now.offset().to_string())
}

// A holder's quota as the registry runs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PolicyQuota {
    // Whether usage counts in periods at all.
    counted: bool,
    // The download budget of a period. `None` admits every request.
    download_bytes: Option<u64>,
    calendar: QuotaCalendar,
}

impl From<&ServerDownloadQuotaConfig> for PolicyQuota {
    fn from(quota: &ServerDownloadQuotaConfig) -> Self {
        Self {
            counted: quota.enabled,
            download_bytes: quota.enabled.then_some(quota.limit_bytes),
            calendar: quota.into(),
        }
    }
}

impl From<&crate::proxies::EgressQuota> for PolicyQuota {
    fn from(quota: &crate::proxies::EgressQuota) -> Self {
        Self {
            counted: quota.is_set(),
            download_bytes: quota.download_bytes,
            calendar: quota.calendar(),
        }
    }
}

// How fast a holder may upload, and the caps its uploads count against.
// Servers upload nothing, so theirs is empty.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct UploadCaps {
    rate: Option<u64>,
    soft_bytes: Option<u64>,
    hard_bytes: Option<u64>,
    trickle_bps: u64,
}

// Bytes uploaded over a holder: ever, and when its current period began.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct UploadCount {
    lifetime_bytes: u64,
    baseline_bytes: u64,
}

impl UploadCount {
    const fn used_bytes(self) -> u64 {
        self.lifetime_bytes.saturating_sub(self.baseline_bytes)
    }

    // A new period: what was uploaded so far no longer counts.
    const fn restart(self) -> Self {
        Self {
            lifetime_bytes: self.lifetime_bytes,
            baseline_bytes: self.lifetime_bytes,
        }
    }
}

#[derive(Debug, Clone)]
struct ServerPolicyState {
    quota: PolicyQuota,
    // The download rate. `Some(0)` admits nothing.
    rate: Option<u64>,
    window: Option<ServerQuotaWindow>,
    generation: u64,
    upload_caps: UploadCaps,
    upload: UploadCount,
}

impl ServerPolicyState {
    fn upload_state(&self) -> QuotaState {
        QuotaState::of(
            self.upload.used_bytes(),
            self.upload_caps.soft_bytes,
            self.upload_caps.hard_bytes,
        )
    }
}

// One holder of a transfer policy: a server or an egress.
struct QuotaHolder {
    id: u32,
    rate: Option<u64>,
    quota: PolicyQuota,
    upload_caps: UploadCaps,
}

// Where an egress stands against its quota in each direction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EgressQuotaStates {
    pub download: QuotaState,
    pub upload: QuotaState,
}

// One direction of an egress's usage in its current period.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectionUsage {
    pub used_bytes: u64,
    // `None` when the direction has no cap.
    pub remaining_bytes: Option<u64>,
    pub state: QuotaState,
}

// An egress's usage in both directions over its current period.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressQuotaUsage {
    pub egress_id: u32,
    pub download: DirectionUsage,
    pub upload: DirectionUsage,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    pub timezone: String,
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
    upload: UploadCount,
}

impl FlushedUsage {
    fn of(usage: &ServerDownloadUsage, upload: UploadCount) -> Self {
        Self {
            lifetime_bytes: usage.lifetime_bytes,
            quota_baseline_bytes: usage.quota_baseline_bytes,
            window_end: usage.window_end,
            upload,
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

    fn load_upload(&self, scope: TransferScope, id: u32) -> Result<UploadCount, StateError> {
        Ok(match scope {
            TransferScope::Server => UploadCount::default(),
            TransferScope::Egress => self.db.egress_upload_usage(id)?.map_or_else(
                UploadCount::default,
                |(lifetime_bytes, baseline_bytes)| UploadCount {
                    lifetime_bytes,
                    baseline_bytes,
                },
            ),
        })
    }

    fn store_usage(
        &self,
        scope: TransferScope,
        usage: &ServerDownloadUsage,
        upload: UploadCount,
    ) -> Result<(), StateError> {
        match scope {
            TransferScope::Server => self.db.upsert_server_download_usage(usage),
            TransferScope::Egress => {
                self.db.upsert_egress_download_usage(usage)?;
                self.db.set_egress_upload_usage(
                    usage.server_id,
                    upload.lifetime_bytes,
                    upload.baseline_bytes,
                )
            }
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
                // A server's limit of 0 is no limit.
                rate: (server.max_download_speed != 0).then_some(server.max_download_speed),
                quota: (&server.download_quota).into(),
                upload_caps: UploadCaps::default(),
            })
            .collect::<Vec<_>>();
        self.reconfigure_book(TransferScope::Server, &holders)
    }

    // Apply every egress's rates and quota. Counters carry
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
                rate: egress.download_bps,
                quota: (&egress.quota).into(),
                upload_caps: UploadCaps {
                    rate: egress.upload_bps,
                    soft_bytes: egress.quota.upload_soft_bytes,
                    hard_bytes: egress.quota.upload_hard_bytes,
                    trickle_bps: egress.quota.upload_trickle_bps,
                },
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
        holders: &[QuotaHolder],
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
            .map(|holder| {
                Ok((
                    self.load_usage(scope, holder.id)?,
                    self.load_upload(scope, holder.id)?,
                ))
            })
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

        for (holder, (usage, stored_upload)) in holders.iter().zip(usages) {
            let previous = policies.get(&holder.id).cloned();
            let mut window = quota_window(now, holder.quota.calendar);
            let mut baseline = usage.quota_baseline_bytes.min(usage.lifetime_bytes);
            let anchors_changed = previous
                .as_ref()
                .is_some_and(|previous| previous.quota.calendar != holder.quota.calendar);
            let newly_enabled = previous
                .as_ref()
                .is_some_and(|previous| !previous.quota.counted && holder.quota.counted);
            let mut upload = previous
                .as_ref()
                .map_or(stored_upload, |previous| previous.upload);
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
                upload = upload.restart();
            }
            if matches!(
                holder.quota.calendar.period,
                ServerDownloadQuotaPeriod::OneTime
            ) {
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
                quota: holder.quota,
                rate: holder.rate,
                window,
                generation,
                upload_caps: holder.upload_caps,
                upload,
            };
            let config = pacer_config(&policy);
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
                || upload != stored_upload
            {
                usage_updates.push((
                    ServerDownloadUsage {
                        server_id: holder.id,
                        lifetime_bytes: usage.lifetime_bytes.max(baseline),
                        quota_baseline_bytes: baseline,
                        window_start: window.map(|value| value.start),
                        window_end: window.map(|value| value.end),
                        updated_at: crate::e2e_clock::utc_now(),
                    },
                    upload,
                ));
            }
        }
        drop(policies);
        self.notify_changed();
        for (usage, upload) in usage_updates {
            self.store_usage(scope, &usage, upload)?;
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

    // Count bytes uploaded over an egress. Crossing a cap changes where the
    // egress stands, and every subscriber hears of it.
    pub fn record_egress_upload(&self, egress_id: u32, bytes: u64) -> Option<QuotaState> {
        let (before, after) = {
            let mut policies = self.egresses.policies();
            let policy = policies.get_mut(&egress_id)?;
            let before = policy.upload_state();
            policy.upload.lifetime_bytes = policy.upload.lifetime_bytes.saturating_add(bytes);
            (before, policy.upload_state())
        };
        if before != after {
            info!(
                egress_id,
                state = after.as_str(),
                "egress upload quota state changed"
            );
            self.notify_changed();
        }
        Some(after)
    }

    // Where an egress stands against its quota in each direction.
    pub fn egress_quota_states(&self, egress_id: u32) -> Option<EgressQuotaStates> {
        let policies = self.egresses.policies();
        let policy = policies.get(&egress_id)?;
        let snapshot = self.egresses.transfers.snapshot(StableServerId(egress_id));
        Some(quota_states(&snapshot, policy))
    }

    // The rate one direction of an egress may move at under the global and
    // scheduled limits given. An egress without a policy adds no limit.
    pub fn egress_rate(
        &self,
        egress_id: u32,
        direction: Direction,
        global: Option<u64>,
        schedule: Option<u64>,
    ) -> Option<u64> {
        let policies = self.egresses.policies();
        let limits = policies.get(&egress_id).map_or(
            RateLimits {
                global,
                schedule,
                ..RateLimits::default()
            },
            |policy| {
                let snapshot = self.egresses.transfers.snapshot(StableServerId(egress_id));
                let states = quota_states(&snapshot, policy);
                match direction {
                    Direction::Download => RateLimits {
                        global,
                        schedule,
                        egress: policy.rate,
                        quota: states.download,
                        trickle_bps: 0,
                    },
                    Direction::Upload => RateLimits {
                        global,
                        schedule,
                        egress: policy.upload_caps.rate,
                        quota: states.upload,
                        trickle_bps: policy.upload_caps.trickle_bps,
                    },
                }
            },
        );
        effective_rate(limits)
    }

    // An egress's usage in both directions over its current period.
    pub fn egress_usage(&self, egress_id: u32) -> Option<EgressQuotaUsage> {
        let policies = self.egresses.policies();
        let policy = policies.get(&egress_id)?;
        let snapshot = self.egresses.transfers.snapshot(StableServerId(egress_id));
        let states = quota_states(&snapshot, policy);
        let projected = snapshot
            .quota_used_bytes
            .saturating_add(snapshot.quota_reserved_bytes);
        let upload_used = policy.upload.used_bytes();
        Some(EgressQuotaUsage {
            egress_id,
            download: DirectionUsage {
                used_bytes: snapshot.quota_used_bytes,
                remaining_bytes: policy
                    .quota
                    .download_bytes
                    .map(|limit| limit.saturating_sub(projected)),
                state: states.download,
            },
            upload: DirectionUsage {
                used_bytes: upload_used,
                remaining_bytes: policy
                    .upload_caps
                    .hard_bytes
                    .map(|limit| limit.saturating_sub(upload_used)),
                state: states.upload,
            },
            window_start: policy.window.map(|value| value.start),
            window_end: policy.window.map(|value| value.end),
            timezone: local_timezone_label(crate::e2e_clock::local_now()),
        })
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
        let (usage, upload, result) = {
            let mut policies = book.policies();
            let policy = policies.get_mut(&server_id).ok_or_else(|| {
                let scope = match scope {
                    TransferScope::Server => "server",
                    TransferScope::Egress => "egress",
                };
                StateError::Database(format!("{scope} {server_id} has no transfer policy"))
            })?;
            policy.generation = policy.generation.wrapping_add(1).max(1);
            policy.window = quota_window(now, policy.quota.calendar);
            policy.upload = policy.upload.restart();

            book.transfers
                .configure(StableServerId(server_id), pacer_config(policy));
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
            (usage, policy.upload, result)
        };
        self.notify_changed();
        self.store_usage(scope, &usage, upload)?;
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
                let Some(next_window) = quota_window(now, policy.quota.calendar) else {
                    continue;
                };
                let before = book.transfers.snapshot(StableServerId(id));
                policy.window = Some(next_window);
                policy.generation = policy.generation.wrapping_add(1).max(1);
                policy.upload = policy.upload.restart();
                book.transfers
                    .configure(StableServerId(id), pacer_config(policy));
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
            self.store_usage(reset.scope, &usage, reset.policy.upload)?;
            self.record_flushed(reset.scope, &usage, reset.policy.upload);
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

    fn record_flushed(
        &self,
        scope: TransferScope,
        usage: &ServerDownloadUsage,
        upload: UploadCount,
    ) {
        self.flushed_usage
            .lock()
            .expect("server policy registry poisoned")
            .insert((scope, usage.server_id), FlushedUsage::of(usage, upload));
    }

    // The current usage of every control, with whether it differs from what
    // was last written.
    fn usage_rows(&self) -> Vec<(TransferScope, ServerDownloadUsage, UploadCount, bool)> {
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
                let changed = flushed.get(&(book.scope, id))
                    != Some(&FlushedUsage::of(&usage, policy.upload));
                (book.scope, usage, policy.upload, changed)
            }));
        }
        rows
    }

    // Whether a flush now would write anything.
    fn usage_changed_since_flush(&self) -> bool {
        self.usage_rows().iter().any(|(_, _, _, changed)| *changed)
    }

    pub fn flush_usage(&self) -> Result<(), StateError> {
        let _maintenance = self
            .maintenance_gate
            .lock()
            .expect("server policy maintenance gate poisoned");
        let resets = self.advance_windows();
        self.persist_window_resets(resets)?;
        for (scope, usage, upload, changed) in self.usage_rows() {
            if !changed {
                continue;
            }
            self.store_usage(scope, &usage, upload)?;
            self.record_flushed(scope, &usage, upload);
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

// What the pacer enforces for a policy. A rate of `Some(0)` moves nothing,
// which the pacer holds as a budget of no bytes.
fn pacer_config(policy: &ServerPolicyState) -> ServerTransferConfig {
    let quota = policy
        .quota
        .download_bytes
        .map(|limit_bytes| QuotaRuntimeConfig {
            limit_bytes,
            generation: policy.generation,
            retry_at: policy.window.and_then(monotonic_deadline),
        });
    rate_config(policy.rate, quota)
}

// A download rate and quota as the pacer takes them: its rate of 0 is no
// limit, so a rate of `Some(0)` closes the holder instead.
pub(crate) fn rate_config(
    rate: Option<u64>,
    quota: Option<QuotaRuntimeConfig>,
) -> ServerTransferConfig {
    ServerTransferConfig {
        rate_bytes_per_sec: rate.unwrap_or(0),
        quota,
        closed: rate == Some(0),
    }
}

// Where a holder stands in each direction. Downloads stop at the budget;
// a download quota has no soft cap.
fn quota_states(
    snapshot: &weaver_nntp::transfer::ServerTransferSnapshot,
    policy: &ServerPolicyState,
) -> EgressQuotaStates {
    EgressQuotaStates {
        download: QuotaState::of(snapshot.quota_used_bytes, None, policy.quota.download_bytes),
        upload: policy.upload_state(),
    }
}

fn monotonic_deadline(window: ServerQuotaWindow) -> Option<Instant> {
    let delay = (window.end - crate::e2e_clock::utc_now()).to_std().ok()?;
    Some(Instant::now() + delay)
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
        remaining_bytes: policy.quota.download_bytes.map(|limit| {
            limit.saturating_sub(
                snapshot
                    .quota_used_bytes
                    .saturating_add(snapshot.quota_reserved_bytes),
            )
        }),
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

    fn egress_quota(period: ServerDownloadQuotaPeriod) -> crate::proxies::EgressQuota {
        let server = quota(period);
        crate::proxies::EgressQuota {
            download_bytes: Some(server.limit_bytes),
            period,
            reset_time_minutes_local: server.reset_time_minutes_local,
            weekly_reset_weekday: server.weekly_reset_weekday,
            monthly_reset_day: server.monthly_reset_day,
            ..Default::default()
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
            quota_window(
                Local::now(),
                (&quota(ServerDownloadQuotaPeriod::OneTime)).into()
            )
            .is_none()
        );
    }

    #[test]
    fn recurring_quota_reuses_dst_safe_bandwidth_windows() {
        let now = Local.with_ymd_and_hms(2026, 7, 9, 12, 0, 0).unwrap();
        let daily = quota_window(now, (&quota(ServerDownloadQuotaPeriod::Daily)).into()).unwrap();
        assert!(daily.start < now.with_timezone(&Utc));
        assert!(daily.end > now.with_timezone(&Utc));

        let monthly =
            quota_window(now, (&quota(ServerDownloadQuotaPeriod::Monthly)).into()).unwrap();
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
                download_bps: None,
                upload_bps: None,
                quota: egress_quota(ServerDownloadQuotaPeriod::OneTime),
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
            download_bps: None,
            upload_bps: None,
            quota: egress_quota(ServerDownloadQuotaPeriod::OneTime),
        };
        registry
            .reconfigure_egresses(std::slice::from_ref(&egress))
            .unwrap();
        let control = registry
            .egress_transfer_registry()
            .control(StableServerId(7));
        let limit = egress.quota.download_bytes.unwrap();
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
        egress.quota.download_bytes = Some(limit * 2);
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
            download_bps: None,
            upload_bps: None,
            quota: egress_quota(ServerDownloadQuotaPeriod::OneTime),
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
            .try_reserve(egress.quota.download_bytes.unwrap())
            .unwrap();
        assert!(control.try_reserve(1).is_err());
    }

    fn upload_capped_egress(db: &Database) -> crate::proxies::EgressInterface {
        db.create_egress_interface(&crate::proxies::EgressInterface {
            id: 0,
            name: "Upload capped".into(),
            binding: crate::proxies::EgressBinding::SourceAddress {
                address: "127.0.0.1".parse().unwrap(),
            },
            enabled: true,
            download_bps: Some(1_000),
            upload_bps: Some(4_000),
            quota: crate::proxies::EgressQuota {
                upload_soft_bytes: Some(100),
                upload_trickle_bps: 512,
                upload_hard_bytes: Some(200),
                ..Default::default()
            },
        })
        .unwrap()
    }

    #[test]
    fn upload_moves_an_egress_through_trickle_to_stopped_and_a_reset_restores_it() {
        let db = Database::open_in_memory().unwrap();
        let egress = upload_capped_egress(&db);
        let registry = ServerTransferPolicyRegistry::new(db, &[]).unwrap();
        registry
            .reconfigure_egresses(std::slice::from_ref(&egress))
            .unwrap();
        let mut changes = registry.subscribe_changes();
        changes.borrow_and_update();

        assert_eq!(registry.record_egress_upload(egress.id + 1, 1), None);
        assert_eq!(
            registry.record_egress_upload(egress.id, 99),
            Some(QuotaState::Normal)
        );
        assert!(!changes.has_changed().unwrap());
        assert_eq!(
            registry.record_egress_upload(egress.id, 1),
            Some(QuotaState::Trickle)
        );
        assert!(changes.has_changed().unwrap());
        changes.borrow_and_update();
        assert_eq!(
            registry.record_egress_upload(egress.id, 99),
            Some(QuotaState::Trickle)
        );
        assert!(!changes.has_changed().unwrap());
        assert_eq!(
            registry.record_egress_upload(egress.id, 1),
            Some(QuotaState::Stopped)
        );
        assert!(changes.has_changed().unwrap());
        let usage = registry.egress_usage(egress.id).unwrap();
        assert_eq!(usage.upload.used_bytes, 200);
        assert_eq!(usage.upload.remaining_bytes, Some(0));
        assert_eq!(
            registry.egress_quota_states(egress.id).unwrap(),
            EgressQuotaStates {
                download: QuotaState::Normal,
                upload: QuotaState::Stopped,
            }
        );

        registry.reset_egress_usage(egress.id).unwrap();
        assert_eq!(
            registry.egress_quota_states(egress.id).unwrap().upload,
            QuotaState::Normal
        );
        assert_eq!(
            registry.egress_usage(egress.id).unwrap().upload.used_bytes,
            0
        );
    }

    #[test]
    fn an_egress_rate_is_the_tightest_limit_and_its_quota_state_a_floor() {
        let db = Database::open_in_memory().unwrap();
        let egress = upload_capped_egress(&db);
        let registry = ServerTransferPolicyRegistry::new(db, &[]).unwrap();
        registry
            .reconfigure_egresses(std::slice::from_ref(&egress))
            .unwrap();
        let rate = |direction, global, schedule| {
            registry.egress_rate(egress.id, direction, global, schedule)
        };
        // The egress cap, then a tighter global or scheduled limit.
        assert_eq!(rate(Direction::Download, None, None), Some(1_000));
        assert_eq!(rate(Direction::Download, Some(500), None), Some(500));
        assert_eq!(rate(Direction::Download, Some(500), Some(300)), Some(300));
        assert_eq!(rate(Direction::Download, None, Some(0)), Some(0));
        assert_eq!(rate(Direction::Upload, None, None), Some(4_000));
        // An egress the registry does not hold adds no limit of its own.
        assert_eq!(
            registry.egress_rate(egress.id + 1, Direction::Upload, Some(9), None),
            Some(9)
        );
        assert_eq!(
            registry.egress_rate(egress.id + 1, Direction::Upload, None, None),
            None
        );

        // Past the soft cap uploads trickle; a looser schedule cannot lift it.
        registry.record_egress_upload(egress.id, 100);
        assert_eq!(rate(Direction::Upload, None, Some(1_000_000)), Some(512));
        assert_eq!(rate(Direction::Upload, None, Some(100)), Some(100));
        assert_eq!(rate(Direction::Download, None, None), Some(1_000));
        // Past the hard cap they stop.
        registry.record_egress_upload(egress.id, 100);
        assert_eq!(rate(Direction::Upload, Some(1_000_000), None), Some(0));
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
            closed: false,
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
