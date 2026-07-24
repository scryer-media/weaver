//! Shared per-server BODY transfer policy.
//!
//! A [`ServerTransferRegistry`] is intentionally independent from an
//! [`NntpClient`](crate::client::NntpClient). Keeping it alive across client
//! rebuilds preserves rate-limit debt, quota reservations, and byte counters.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::error::NntpError;

const RATE_BURST_MICROS: u64 = 1_000_000;
const RATE_SCHEDULE_EPOCH_SHIFT: u32 = 48;
const RATE_SCHEDULE_TARGET_MASK: u64 = (1_u64 << RATE_SCHEDULE_EPOCH_SHIFT) - 1;

/// Durable server identifier supplied by the application database.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StableServerId(pub u32);

/// Runtime quota window calculated by the application calendar policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaRuntimeConfig {
    pub limit_bytes: u64,
    /// Changes on a scheduled rollover or explicit usage reset.
    pub generation: u64,
    /// Monotonic wake deadline for recurring quotas. `None` means manual-only.
    pub retry_at: Option<Instant>,
}

/// Live transfer policy for one server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerTransferConfig {
    /// Aggregate BODY payload bytes per second. Zero means unlimited.
    pub rate_bytes_per_sec: u64,
    pub quota: Option<QuotaRuntimeConfig>,
}

/// Durable counters restored before download workers start.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerTransferInitialState {
    pub lifetime_body_bytes: u64,
    pub quota_used_bytes: u64,
}

/// Authoritative live view of one server's transfer policy and counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerTransferSnapshot {
    pub stable_server_id: StableServerId,
    pub rate_bytes_per_sec: u64,
    pub lifetime_body_bytes: u64,
    pub quota_enabled: bool,
    pub quota_limit_bytes: u64,
    pub quota_used_bytes: u64,
    pub quota_reserved_bytes: u64,
    pub quota_remaining_bytes: u64,
    pub quota_blocked: bool,
    pub quota_generation: u64,
    /// Monotonic revision for admission-capacity increases and config changes.
    pub capacity_revision: u64,
    pub retry_at: Option<Instant>,
    pub throttle_wait: Duration,
}

/// Per-BODY elapsed-time budget that excludes deliberate local rate-limit waits.
#[derive(Debug)]
pub(crate) struct ActiveTransferBudget {
    started: Instant,
    limit: Duration,
    excluded_wait: Duration,
}

impl ActiveTransferBudget {
    pub(crate) fn new(limit: Duration) -> Self {
        Self {
            started: Instant::now(),
            limit,
            excluded_wait: Duration::ZERO,
        }
    }

    pub(crate) fn exclude_wait(&mut self, waited: Duration) {
        self.excluded_wait = self.excluded_wait.saturating_add(waited);
    }

    pub(crate) fn remaining(&self) -> Duration {
        self.limit
            .saturating_sub(self.started.elapsed().saturating_sub(self.excluded_wait))
    }

    pub(crate) fn limit(&self) -> Duration {
        self.limit
    }
}

pub(crate) fn active_transfer_timeout(budget: &ActiveTransferBudget) -> NntpError {
    NntpError::SoftTimeout(budget.limit().as_secs())
}

pub(crate) fn active_transfer_read_timeout(
    command_timeout: Duration,
    budget: Option<&ActiveTransferBudget>,
) -> crate::error::Result<(Duration, bool)> {
    let Some(budget) = budget else {
        return Ok((command_timeout, false));
    };
    let remaining = budget.remaining();
    if remaining.is_zero() {
        return Err(active_transfer_timeout(budget));
    }
    Ok((command_timeout.min(remaining), remaining <= command_timeout))
}

/// Admission failure for a BODY request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaRejection {
    pub stable_server_id: StableServerId,
    pub requested_body_bytes: u64,
    pub capacity_revision: u64,
    /// Registry-wide revision captured while selecting quota-blocked servers.
    pub registry_capacity_revision: u64,
    pub retry_at: Option<Instant>,
    pub snapshot: Box<ServerTransferSnapshot>,
}

/// Result returned when a BODY permit is explicitly completed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BodyTransferReceipt {
    pub body_bytes: u64,
    pub throttle_wait: Duration,
}

/// Per-response accounting selected at BODY admission. The unlimited variant
/// carries no allocation, reference-count operation, reservation, or lock.
pub(crate) enum BodyTransferAccounting {
    Unlimited,
    Tracked(BodyTransferPermit),
}

/// Stable-ID registry shared by every runtime client generation.
struct RegistryCapacitySignal {
    revision: AtomicU64,
    changed: watch::Sender<u64>,
}

impl Default for RegistryCapacitySignal {
    fn default() -> Self {
        let (changed, _) = watch::channel(1);
        Self {
            revision: AtomicU64::new(1),
            changed,
        }
    }
}

impl RegistryCapacitySignal {
    fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    fn publish(&self) {
        self.revision
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |revision| {
                Some(revision.wrapping_add(1).max(1))
            })
            .expect("registry capacity revision update cannot fail");
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1).max(1));
    }
}

pub struct ServerTransferRegistry {
    controls: RwLock<HashMap<StableServerId, Arc<ServerTransferControl>>>,
    capacity: Arc<RegistryCapacitySignal>,
}

impl Default for ServerTransferRegistry {
    fn default() -> Self {
        Self {
            controls: RwLock::new(HashMap::new()),
            capacity: Arc::new(RegistryCapacitySignal::default()),
        }
    }
}

impl ServerTransferRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Monotonic revision for capacity changes in any registered server.
    pub fn capacity_revision(&self) -> u64 {
        self.capacity.revision()
    }

    /// Subscribe before checking a parked selection's revision so capacity
    /// changes racing selection cannot be missed.
    pub fn subscribe_capacity_changes(&self) -> watch::Receiver<u64> {
        self.capacity.subscribe()
    }

    /// Return an existing durable control without creating one.
    pub fn get(&self, id: StableServerId) -> Option<Arc<ServerTransferControl>> {
        self.controls
            .read()
            .expect("transfer registry poisoned")
            .get(&id)
            .map(Arc::clone)
    }

    /// Get or create the durable control for `id`.
    pub fn control(&self, id: StableServerId) -> Arc<ServerTransferControl> {
        if let Some(control) = self.get(id) {
            return control;
        }

        let capacity = Arc::clone(&self.capacity);
        let mut controls = self.controls.write().expect("transfer registry poisoned");
        Arc::clone(
            controls
                .entry(id)
                .or_insert_with(|| Arc::new(ServerTransferControl::new(id, capacity))),
        )
    }

    /// Apply a live policy update while preserving counters and reservations.
    pub fn configure(
        &self,
        id: StableServerId,
        config: ServerTransferConfig,
    ) -> Arc<ServerTransferControl> {
        let control = self.control(id);
        control.configure(config, None);
        control
    }

    /// Restore durable counters and apply the initial live policy.
    ///
    /// This is intended for startup before workers receive the control. Later
    /// calls never reduce the lifetime counter.
    pub fn restore(
        &self,
        id: StableServerId,
        config: ServerTransferConfig,
        initial: ServerTransferInitialState,
    ) -> Arc<ServerTransferControl> {
        let control = self.control(id);
        control.configure(config, Some(initial));
        control
    }

    pub fn snapshot(&self, id: StableServerId) -> ServerTransferSnapshot {
        self.control(id).snapshot()
    }

    pub fn snapshot_if_present(&self, id: StableServerId) -> Option<ServerTransferSnapshot> {
        self.get(id).map(|control| control.snapshot())
    }

    pub fn snapshots(&self) -> Vec<ServerTransferSnapshot> {
        let controls = self.controls.read().expect("transfer registry poisoned");
        let mut snapshots = controls
            .values()
            .map(|control| control.snapshot())
            .collect::<Vec<_>>();
        snapshots.sort_unstable_by_key(|snapshot| snapshot.stable_server_id);
        snapshots
    }

    /// Remove an inactive server control. Existing lanes keep their `Arc` and
    /// remain safe until they drain.
    pub fn remove(&self, id: StableServerId) -> Option<Arc<ServerTransferControl>> {
        let removed = self
            .controls
            .write()
            .expect("transfer registry poisoned")
            .remove(&id);
        if removed.is_some() {
            self.capacity.publish();
        }
        removed
    }

    /// Remove every registered control. Existing lane-held `Arc`s remain valid.
    pub fn clear(&self) {
        let had_controls = {
            let mut controls = self.controls.write().expect("transfer registry poisoned");
            let had_controls = !controls.is_empty();
            controls.clear();
            had_controls
        };
        if had_controls {
            self.capacity.publish();
        }
    }
}

/// Shared transfer state for one durable server.
pub struct ServerTransferControl {
    id: StableServerId,
    state: Mutex<TransferState>,
    blocking_changed: Condvar,
    capacity_changed: watch::Sender<u64>,
    registry_capacity: Arc<RegistryCapacitySignal>,
    lifetime_body_bytes: AtomicU64,
    throttle_wait_nanos: AtomicU64,
    quota_epoch: AtomicU64,
    rate_bytes_per_sec: AtomicU64,
    rate_schedule: AtomicU64,
    rate_origin: Instant,
    rate_wait_lock: Mutex<()>,
    rate_changed: Condvar,
    #[cfg(test)]
    path_counters: TransferPathCounters,
    #[cfg(test)]
    rate_reserve_pause: Mutex<Option<RateReservePause>>,
}

#[derive(Debug)]
struct TransferState {
    initialized: bool,
    config: ServerTransferConfig,
    quota_used_bytes: u64,
    quota_reserved_bytes: u64,
    quota_epoch: u64,
    capacity_revision: u64,
}

#[derive(Debug, Clone, Copy)]
struct RateTicket {
    epoch: u64,
    target_micros: u64,
    bytes: u64,
    #[cfg(test)]
    rate_bytes_per_sec: u64,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct TransferPathCounters {
    quota_lock_acquisitions: AtomicU64,
    terminal_quota_reconciliations: AtomicU64,
    rate_ticket_reservations: AtomicU64,
    rate_wait_lock_acquisitions: AtomicU64,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TransferPathSnapshot {
    quota_lock_acquisitions: u64,
    terminal_quota_reconciliations: u64,
    rate_ticket_reservations: u64,
    rate_wait_lock_acquisitions: u64,
}

#[cfg(test)]
struct RateReservePause {
    schedule_loaded: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
}

fn rate_cost_micros(bytes: u64, rate_bytes_per_sec: u64) -> u64 {
    let numerator = (bytes as u128).saturating_mul(1_000_000);
    numerator
        .div_ceil(rate_bytes_per_sec as u128)
        .max(1)
        .min(RATE_SCHEDULE_TARGET_MASK as u128) as u64
}

impl ServerTransferControl {
    fn new(id: StableServerId, registry_capacity: Arc<RegistryCapacitySignal>) -> Self {
        let (capacity_changed, _) = watch::channel(1);
        Self {
            id,
            state: Mutex::new(TransferState {
                initialized: false,
                config: ServerTransferConfig::default(),
                quota_used_bytes: 0,
                quota_reserved_bytes: 0,
                quota_epoch: 0,
                capacity_revision: 1,
            }),
            blocking_changed: Condvar::new(),
            capacity_changed,
            registry_capacity,
            lifetime_body_bytes: AtomicU64::new(0),
            throttle_wait_nanos: AtomicU64::new(0),
            quota_epoch: AtomicU64::new(0),
            rate_bytes_per_sec: AtomicU64::new(0),
            rate_schedule: AtomicU64::new(1_u64 << RATE_SCHEDULE_EPOCH_SHIFT),
            rate_origin: Instant::now(),
            rate_wait_lock: Mutex::new(()),
            rate_changed: Condvar::new(),
            #[cfg(test)]
            path_counters: TransferPathCounters::default(),
            #[cfg(test)]
            rate_reserve_pause: Mutex::new(None),
        }
    }

    pub fn stable_server_id(&self) -> StableServerId {
        self.id
    }

    pub fn update_config(&self, config: ServerTransferConfig) {
        self.configure(config, None);
    }

    fn configure(&self, config: ServerTransferConfig, initial: Option<ServerTransferInitialState>) {
        if let Some(initial) = initial {
            self.lifetime_body_bytes
                .fetch_max(initial.lifetime_body_bytes, Ordering::Relaxed);
        }

        let (revision, rate_changed) = {
            let mut state = self.state.lock().expect("server transfer state poisoned");
            let rate_changed = state.config.rate_bytes_per_sec != config.rate_bytes_per_sec;

            let generation_changed = match (state.config.quota, config.quota) {
                (Some(previous), Some(next)) => previous.generation != next.generation,
                (None, Some(_)) => true,
                _ => false,
            };
            let quota_epoch_changed =
                generation_changed || matches!((state.config.quota, config.quota), (Some(_), None));
            if generation_changed {
                state.quota_used_bytes = initial.map_or(0, |value| value.quota_used_bytes);
            } else if !state.initialized {
                state.quota_used_bytes = initial.map_or(0, |value| value.quota_used_bytes);
            }
            if quota_epoch_changed {
                state.quota_epoch = state.quota_epoch.wrapping_add(1).max(1);
            }

            state.config = config;
            state.initialized = true;
            self.quota_epoch.store(
                state.config.quota.map_or(0, |_| state.quota_epoch),
                Ordering::Release,
            );
            state.capacity_revision = state.capacity_revision.wrapping_add(1).max(1);
            (state.capacity_revision, rate_changed)
        };
        if rate_changed {
            self.reconfigure_rate(config.rate_bytes_per_sec);
        }
        self.publish_capacity_change(revision);
    }

    fn publish_capacity_change(&self, revision: u64) {
        self.capacity_changed.send_replace(revision);
        self.registry_capacity.publish();
        self.blocking_changed.notify_all();
    }

    /// Check BODY admission atomically without reserving capacity.
    pub fn quota_rejection_for(&self, requested_body_bytes: u64) -> Option<QuotaRejection> {
        if self.quota_epoch.load(Ordering::Acquire) == 0 {
            return None;
        }
        #[cfg(test)]
        self.path_counters
            .quota_lock_acquisitions
            .fetch_add(1, Ordering::Relaxed);
        let state = self.state.lock().expect("server transfer state poisoned");
        self.quota_rejection_locked(&state, requested_body_bytes)
    }

    pub(crate) fn start_body(
        self: &Arc<Self>,
        estimated_body_bytes: u64,
    ) -> Result<BodyTransferAccounting, QuotaRejection> {
        if self.quota_epoch.load(Ordering::Acquire) == 0
            && self.rate_bytes_per_sec.load(Ordering::Acquire) == 0
        {
            return Ok(BodyTransferAccounting::Unlimited);
        }
        self.try_reserve(estimated_body_bytes)
            .map(BodyTransferAccounting::Tracked)
    }

    /// Record BODY bytes for an admission proven to have neither rate nor
    /// quota policy. This is the production unlimited marker's only hot-path
    /// operation and intentionally performs one relaxed atomic update.
    #[doc(hidden)]
    pub fn record_unlimited_body_bytes(&self, bytes: usize) {
        self.record_lifetime_bytes(bytes as u64);
    }

    /// Reserve the estimated raw BODY payload before issuing `BODY`.
    pub fn try_reserve(
        self: &Arc<Self>,
        estimated_body_bytes: u64,
    ) -> Result<BodyTransferPermit, QuotaRejection> {
        let mut reserved_quota_bytes = 0;
        if self.quota_epoch.load(Ordering::Acquire) != 0 {
            #[cfg(test)]
            self.path_counters
                .quota_lock_acquisitions
                .fetch_add(1, Ordering::Relaxed);
            let mut state = self.state.lock().expect("server transfer state poisoned");
            if let Some(rejection) = self.quota_rejection_locked(&state, estimated_body_bytes) {
                return Err(rejection);
            }
            if state.config.quota.is_some() {
                state.quota_reserved_bytes = state
                    .quota_reserved_bytes
                    .saturating_add(estimated_body_bytes);
                reserved_quota_bytes = estimated_body_bytes;
            }
        }

        Ok(BodyTransferPermit {
            control: Arc::clone(self),
            reserved_quota_bytes,
            quota_epoch: 0,
            quota_body_bytes: 0,
            body_bytes: 0,
            throttle_wait: Duration::ZERO,
            finished: false,
        })
    }

    pub fn snapshot(&self) -> ServerTransferSnapshot {
        let state = self.state.lock().expect("server transfer state poisoned");
        self.snapshot_locked(&state)
    }

    #[cfg(test)]
    fn pause_next_rate_reservation(
        &self,
        schedule_loaded: Arc<std::sync::Barrier>,
        resume: Arc<std::sync::Barrier>,
    ) {
        *self
            .rate_reserve_pause
            .lock()
            .expect("rate reservation test hook poisoned") = Some(RateReservePause {
            schedule_loaded,
            resume,
        });
    }

    #[cfg(test)]
    fn path_snapshot(&self) -> TransferPathSnapshot {
        TransferPathSnapshot {
            quota_lock_acquisitions: self
                .path_counters
                .quota_lock_acquisitions
                .load(Ordering::Relaxed),
            terminal_quota_reconciliations: self
                .path_counters
                .terminal_quota_reconciliations
                .load(Ordering::Relaxed),
            rate_ticket_reservations: self
                .path_counters
                .rate_ticket_reservations
                .load(Ordering::Relaxed),
            rate_wait_lock_acquisitions: self
                .path_counters
                .rate_wait_lock_acquisitions
                .load(Ordering::Relaxed),
        }
    }

    /// Subscribe before checking admission state to avoid missing a release.
    pub fn subscribe_capacity_changes(&self) -> watch::Receiver<u64> {
        self.capacity_changed.subscribe()
    }

    /// Wait until a quota config/reset update, reservation release, or rate change.
    pub async fn changed(&self) {
        let mut changed = self.subscribe_capacity_changes();
        let _ = changed.changed().await;
    }

    pub fn wait_for_capacity_change_blocking(
        &self,
        observed_revision: u64,
        timeout: Option<Duration>,
    ) -> u64 {
        let started = Instant::now();
        let mut state = self.state.lock().expect("server transfer state poisoned");
        loop {
            if state.capacity_revision != observed_revision {
                return state.capacity_revision;
            }
            if let Some(timeout) = timeout {
                let remaining = timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    return state.capacity_revision;
                }
                let (next, wait) = self
                    .blocking_changed
                    .wait_timeout(state, remaining)
                    .expect("server transfer state poisoned");
                state = next;
                if wait.timed_out() {
                    return state.capacity_revision;
                }
            } else {
                state = self
                    .blocking_changed
                    .wait(state)
                    .expect("server transfer state poisoned");
            }
        }
    }

    fn snapshot_locked(&self, state: &TransferState) -> ServerTransferSnapshot {
        let quota = state.config.quota;
        let limit = quota.map_or(0, |value| value.limit_bytes);
        let projected = state
            .quota_used_bytes
            .saturating_add(state.quota_reserved_bytes);
        ServerTransferSnapshot {
            stable_server_id: self.id,
            rate_bytes_per_sec: state.config.rate_bytes_per_sec,
            lifetime_body_bytes: self.lifetime_body_bytes.load(Ordering::Relaxed),
            quota_enabled: quota.is_some(),
            quota_limit_bytes: limit,
            quota_used_bytes: state.quota_used_bytes,
            quota_reserved_bytes: if quota.is_some() {
                state.quota_reserved_bytes
            } else {
                0
            },
            quota_remaining_bytes: if quota.is_some() {
                limit.saturating_sub(projected)
            } else {
                u64::MAX
            },
            quota_blocked: quota.is_some_and(|_| projected >= limit),
            quota_generation: quota.map_or(0, |value| value.generation),
            capacity_revision: state.capacity_revision,
            retry_at: quota.and_then(|value| value.retry_at),
            throttle_wait: Duration::from_nanos(self.throttle_wait_nanos.load(Ordering::Relaxed)),
        }
    }

    fn quota_rejection_locked(
        &self,
        state: &TransferState,
        requested_body_bytes: u64,
    ) -> Option<QuotaRejection> {
        let quota = state.config.quota?;
        let projected = state
            .quota_used_bytes
            .saturating_add(state.quota_reserved_bytes);
        if projected < quota.limit_bytes
            && projected.saturating_add(requested_body_bytes) <= quota.limit_bytes
        {
            return None;
        }
        let snapshot = Box::new(self.snapshot_locked(state));
        Some(QuotaRejection {
            stable_server_id: self.id,
            requested_body_bytes,
            capacity_revision: snapshot.capacity_revision,
            registry_capacity_revision: self.registry_capacity.revision(),
            retry_at: snapshot.retry_at,
            snapshot,
        })
    }

    fn record_lifetime_bytes(&self, bytes: u64) {
        if bytes != 0 {
            self.lifetime_body_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    fn current_quota_epoch(&self) -> u64 {
        self.quota_epoch.load(Ordering::Acquire)
    }

    fn reserve_rate(&self, bytes: u64) -> Option<RateTicket> {
        if bytes == 0 {
            return None;
        }
        loop {
            // Load the packed epoch first. A concurrent reconfiguration stores
            // the new rate before advancing this epoch, so the CAS below either
            // validates a matching rate/epoch pair or fails and retries.
            let current = self.rate_schedule.load(Ordering::Acquire);
            #[cfg(test)]
            if let Some(pause) = self
                .rate_reserve_pause
                .lock()
                .expect("rate reservation test hook poisoned")
                .take()
            {
                pause.schedule_loaded.wait();
                pause.resume.wait();
            }
            let rate = self.rate_bytes_per_sec.load(Ordering::Acquire);
            if rate == 0 {
                return None;
            }
            let epoch = current >> RATE_SCHEDULE_EPOCH_SHIFT;
            let previous_target = current & RATE_SCHEDULE_TARGET_MASK;
            let now = self.rate_now_micros();
            let base = previous_target.max(now.saturating_sub(RATE_BURST_MICROS));
            let cost = rate_cost_micros(bytes, rate);
            let target_micros = base.saturating_add(cost).min(RATE_SCHEDULE_TARGET_MASK);
            let next = (epoch << RATE_SCHEDULE_EPOCH_SHIFT) | target_micros;
            if self
                .rate_schedule
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                #[cfg(test)]
                self.path_counters
                    .rate_ticket_reservations
                    .fetch_add(1, Ordering::Relaxed);
                return Some(RateTicket {
                    epoch,
                    target_micros,
                    bytes,
                    #[cfg(test)]
                    rate_bytes_per_sec: rate,
                });
            }
        }
    }

    fn rate_now_micros(&self) -> u64 {
        RATE_BURST_MICROS
            .saturating_add(
                self.rate_origin
                    .elapsed()
                    .as_micros()
                    .min(RATE_SCHEDULE_TARGET_MASK as u128) as u64,
            )
            .min(RATE_SCHEDULE_TARGET_MASK)
    }

    fn reconfigure_rate(&self, rate_bytes_per_sec: u64) {
        let _waiters = self
            .rate_wait_lock
            .lock()
            .expect("server transfer rate wait state poisoned");
        self.rate_bytes_per_sec
            .store(rate_bytes_per_sec, Ordering::Release);
        let mut current = self.rate_schedule.load(Ordering::Acquire);
        loop {
            let epoch = (current >> RATE_SCHEDULE_EPOCH_SHIFT).wrapping_add(1) & 0xffff;
            let next = epoch << RATE_SCHEDULE_EPOCH_SHIFT;
            match self.rate_schedule.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        self.rate_changed.notify_all();
    }

    async fn wait_async(&self, mut ticket: RateTicket) -> Duration {
        let mut waited = Duration::ZERO;
        let mut changed = self.subscribe_capacity_changes();
        loop {
            let schedule = self.rate_schedule.load(Ordering::Acquire);
            if schedule >> RATE_SCHEDULE_EPOCH_SHIFT != ticket.epoch {
                let Some(refreshed) = self.reserve_rate(ticket.bytes) else {
                    break;
                };
                ticket = refreshed;
                continue;
            }
            if self.rate_bytes_per_sec.load(Ordering::Acquire) == 0 {
                break;
            }
            let delay =
                Duration::from_micros(ticket.target_micros.saturating_sub(self.rate_now_micros()));
            if delay.is_zero() {
                break;
            }
            let wait_started = Instant::now();
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = changed.changed() => {}
            }
            waited = waited.saturating_add(wait_started.elapsed());
        }
        self.add_throttle_wait(waited);
        waited
    }

    fn wait_blocking(&self, mut ticket: RateTicket) -> Duration {
        let schedule = self.rate_schedule.load(Ordering::Acquire);
        if schedule >> RATE_SCHEDULE_EPOCH_SHIFT == ticket.epoch
            && (self.rate_bytes_per_sec.load(Ordering::Acquire) == 0
                || ticket.target_micros <= self.rate_now_micros())
        {
            return Duration::ZERO;
        }
        #[cfg(test)]
        self.path_counters
            .rate_wait_lock_acquisitions
            .fetch_add(1, Ordering::Relaxed);
        let mut waited = Duration::ZERO;
        let mut wait_guard = self
            .rate_wait_lock
            .lock()
            .expect("server transfer rate wait state poisoned");
        loop {
            let schedule = self.rate_schedule.load(Ordering::Acquire);
            if schedule >> RATE_SCHEDULE_EPOCH_SHIFT != ticket.epoch {
                let Some(refreshed) = self.reserve_rate(ticket.bytes) else {
                    break;
                };
                ticket = refreshed;
                continue;
            }
            if self.rate_bytes_per_sec.load(Ordering::Acquire) == 0 {
                break;
            }
            let delay =
                Duration::from_micros(ticket.target_micros.saturating_sub(self.rate_now_micros()));
            if delay.is_zero() {
                break;
            }
            let wait_started = Instant::now();
            let (next, _) = self
                .rate_changed
                .wait_timeout(wait_guard, delay)
                .expect("server transfer rate wait state poisoned");
            wait_guard = next;
            waited = waited.saturating_add(wait_started.elapsed());
        }
        drop(wait_guard);
        self.add_throttle_wait(waited);
        waited
    }

    fn add_throttle_wait(&self, waited: Duration) {
        if !waited.is_zero() {
            self.throttle_wait_nanos.fetch_add(
                waited.as_nanos().min(u64::MAX as u128) as u64,
                Ordering::Relaxed,
            );
        }
    }

    fn finish_transfer(
        &self,
        reserved_quota_bytes: u64,
        body_quota_epoch: u64,
        quota_body_bytes: u64,
    ) {
        if reserved_quota_bytes == 0 && (body_quota_epoch == 0 || quota_body_bytes == 0) {
            return;
        }
        #[cfg(test)]
        {
            self.path_counters
                .quota_lock_acquisitions
                .fetch_add(1, Ordering::Relaxed);
            self.path_counters
                .terminal_quota_reconciliations
                .fetch_add(1, Ordering::Relaxed);
        }
        let mut state = self.state.lock().expect("server transfer state poisoned");
        let projected_before = state.config.quota.map(|_| {
            state
                .quota_used_bytes
                .saturating_add(state.quota_reserved_bytes)
        });
        state.quota_reserved_bytes = state
            .quota_reserved_bytes
            .saturating_sub(reserved_quota_bytes);
        if state.config.quota.is_some()
            && body_quota_epoch != 0
            && body_quota_epoch == state.quota_epoch
        {
            state.quota_used_bytes = state.quota_used_bytes.saturating_add(quota_body_bytes);
        }
        let projected_after = state.config.quota.map(|_| {
            state
                .quota_used_bytes
                .saturating_add(state.quota_reserved_bytes)
        });
        let revision = if matches!(
            (projected_before, projected_after),
            (Some(before), Some(after)) if after < before
        ) {
            state.capacity_revision = state.capacity_revision.wrapping_add(1).max(1);
            Some(state.capacity_revision)
        } else {
            None
        };
        drop(state);
        if let Some(revision) = revision {
            self.publish_capacity_change(revision);
        }
    }
}

/// RAII admission permit for exactly one BODY response.
///
/// Dropping it refunds any unused estimate. Bytes already reported remain
/// charged, including bytes received before a decode or transport failure.
pub struct BodyTransferPermit {
    control: Arc<ServerTransferControl>,
    reserved_quota_bytes: u64,
    quota_epoch: u64,
    quota_body_bytes: u64,
    body_bytes: u64,
    throttle_wait: Duration,
    finished: bool,
}

impl BodyTransferPermit {
    pub fn stable_server_id(&self) -> StableServerId {
        self.control.id
    }

    pub fn body_bytes(&self) -> u64 {
        self.body_bytes
    }

    pub fn throttle_wait(&self) -> Duration {
        self.throttle_wait
    }

    /// Charge bytes and wait on the shared async aggregate limiter.
    pub async fn record_async(&mut self, bytes: usize) -> Duration {
        let bytes = bytes as u64;
        self.record_bytes(bytes);
        let waited = if let Some(ticket) = self.control.reserve_rate(bytes) {
            self.control.wait_async(ticket).await
        } else {
            Duration::ZERO
        };
        self.throttle_wait = self.throttle_wait.saturating_add(waited);
        waited
    }

    /// Charge received bytes and preserve rate debt without delaying cleanup.
    pub(crate) fn record_without_wait(&mut self, bytes: usize) {
        let bytes = bytes as u64;
        self.record_bytes(bytes);
        let _ = self.control.reserve_rate(bytes);
    }

    /// Charge bytes and wait on the shared blocking aggregate limiter.
    pub fn record_blocking(&mut self, bytes: usize) -> Duration {
        let bytes = bytes as u64;
        self.record_bytes(bytes);
        let waited = if let Some(ticket) = self.control.reserve_rate(bytes) {
            self.control.wait_blocking(ticket)
        } else {
            Duration::ZERO
        };
        self.throttle_wait = self.throttle_wait.saturating_add(waited);
        waited
    }

    fn record_bytes(&mut self, bytes: u64) {
        self.control.record_lifetime_bytes(bytes);
        self.body_bytes = self.body_bytes.saturating_add(bytes);
        let quota_epoch = self.control.current_quota_epoch();
        if quota_epoch == 0 {
            return;
        }
        if self.quota_epoch != quota_epoch {
            self.quota_epoch = quota_epoch;
            self.quota_body_bytes = 0;
        }
        self.quota_body_bytes = self.quota_body_bytes.saturating_add(bytes);
    }

    pub fn finish(mut self) -> BodyTransferReceipt {
        self.finish_inner();
        BodyTransferReceipt {
            body_bytes: self.body_bytes,
            throttle_wait: self.throttle_wait,
        }
    }

    fn finish_inner(&mut self) {
        if !self.finished {
            self.control.finish_transfer(
                self.reserved_quota_bytes,
                self.quota_epoch,
                self.quota_body_bytes,
            );
            self.finished = true;
        }
    }
}

impl Drop for BodyTransferPermit {
    fn drop(&mut self) {
        self.finish_inner();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use super::*;

    #[test]
    fn active_transfer_read_timeout_preserves_command_and_budget_bounds() {
        let command_timeout = Duration::from_secs(10);
        assert_eq!(
            active_transfer_read_timeout(command_timeout, None).unwrap(),
            (command_timeout, false)
        );

        let long_budget = ActiveTransferBudget::new(Duration::from_secs(60));
        assert_eq!(
            active_transfer_read_timeout(command_timeout, Some(&long_budget)).unwrap(),
            (command_timeout, false)
        );

        let short_budget = ActiveTransferBudget::new(Duration::from_secs(1));
        let (timeout, active) =
            active_transfer_read_timeout(command_timeout, Some(&short_budget)).unwrap();
        assert!(active);
        assert!(!timeout.is_zero());
        assert!(timeout <= Duration::from_secs(1));

        let expired = ActiveTransferBudget::new(Duration::ZERO);
        assert!(matches!(
            active_transfer_read_timeout(command_timeout, Some(&expired)),
            Err(NntpError::SoftTimeout(0))
        ));
    }

    fn quota(limit_bytes: u64, generation: u64) -> ServerTransferConfig {
        ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(QuotaRuntimeConfig {
                limit_bytes,
                generation,
                retry_at: None,
            }),
        }
    }

    #[test]
    fn reservation_reconciles_actual_and_refunds_unused_estimate() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(StableServerId(7), quota(1_000, 1));
        let mut permit = control.try_reserve(800).unwrap();
        assert_eq!(control.snapshot().quota_reserved_bytes, 800);
        permit.record_blocking(600);
        drop(permit);

        let snapshot = control.snapshot();
        assert_eq!(snapshot.lifetime_body_bytes, 600);
        assert_eq!(snapshot.quota_used_bytes, 600);
        assert_eq!(snapshot.quota_reserved_bytes, 0);
        assert_eq!(snapshot.quota_remaining_bytes, 400);
    }

    #[test]
    fn actual_body_may_finish_over_quota_but_next_body_is_rejected() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(StableServerId(8), quota(1_000, 1));
        let mut permit = control.try_reserve(900).unwrap();
        permit.record_blocking(1_100);
        drop(permit);

        let rejection = control.try_reserve(1).err().unwrap();
        assert!(rejection.snapshot.quota_blocked);
        assert_eq!(rejection.snapshot.quota_used_bytes, 1_100);
    }

    #[test]
    fn rollover_keeps_outstanding_reservations_in_new_generation() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(StableServerId(9), quota(1_000, 1));
        let mut permit = control.try_reserve(700).unwrap();
        permit.record_blocking(200);

        control.update_config(quota(1_000, 2));
        let snapshot = control.snapshot();
        assert_eq!(snapshot.quota_generation, 2);
        assert_eq!(snapshot.quota_used_bytes, 0);
        assert_eq!(snapshot.quota_reserved_bytes, 700);
        assert!(control.try_reserve(301).is_err());
        assert!(control.try_reserve(300).is_ok());
    }

    #[test]
    fn restore_is_monotonic_and_snapshots_are_stable_id_sorted() {
        let registry = ServerTransferRegistry::new();
        registry.restore(
            StableServerId(20),
            quota(2_000, 3),
            ServerTransferInitialState {
                lifetime_body_bytes: 8_000,
                quota_used_bytes: 900,
            },
        );
        registry.configure(StableServerId(2), ServerTransferConfig::default());
        registry.restore(
            StableServerId(20),
            quota(2_000, 3),
            ServerTransferInitialState {
                lifetime_body_bytes: 7_000,
                quota_used_bytes: 100,
            },
        );
        assert_eq!(
            registry.snapshot(StableServerId(20)).lifetime_body_bytes,
            8_000
        );
        assert_eq!(
            registry
                .snapshots()
                .into_iter()
                .map(|snapshot| snapshot.stable_server_id.0)
                .collect::<Vec<_>>(),
            vec![2, 20]
        );
    }

    #[test]
    fn aggregate_rate_tickets_share_one_second_of_burst_credit() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(
            StableServerId(30),
            ServerTransferConfig {
                rate_bytes_per_sec: 100,
                quota: None,
            },
        );

        let warmup = control.reserve_rate(100).unwrap();
        assert!(warmup.target_micros <= control.rate_now_micros());
        let first = control.reserve_rate(50).expect("burst credit is exhausted");
        let second = control.reserve_rate(50).expect("aggregate debt is shared");
        let first_delay = first
            .target_micros
            .saturating_sub(control.rate_now_micros());
        let second_delay = second
            .target_micros
            .saturating_sub(control.rate_now_micros());
        assert!((490_000..=500_000).contains(&first_delay));
        assert!((990_000..=1_000_000).contains(&second_delay));
    }

    #[test]
    fn rate_reconfigure_invalidates_schedule_loaded_before_rate_read() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(
            StableServerId(31),
            ServerTransferConfig {
                rate_bytes_per_sec: 1_000,
                quota: None,
            },
        );
        let schedule_loaded = Arc::new(std::sync::Barrier::new(2));
        let resume = Arc::new(std::sync::Barrier::new(2));
        control.pause_next_rate_reservation(schedule_loaded.clone(), resume.clone());

        let worker_control = control.clone();
        let worker = std::thread::spawn(move || worker_control.reserve_rate(1_000).unwrap());
        schedule_loaded.wait();
        control.update_config(ServerTransferConfig {
            rate_bytes_per_sec: 100,
            quota: None,
        });
        let expected_epoch =
            control.rate_schedule.load(Ordering::Acquire) >> RATE_SCHEDULE_EPOCH_SHIFT;
        resume.wait();

        let ticket = worker.join().unwrap();
        assert_eq!(ticket.epoch, expected_epoch);
        assert_eq!(ticket.rate_bytes_per_sec, 100);
        assert!(ticket.target_micros >= 10_000_000);
    }

    #[test]
    fn concurrent_quota_reservations_cannot_overbook() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(StableServerId(10), quota(1_000, 1));
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let control = control.clone();
            let barrier = barrier.clone();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                let permit = control.try_reserve(600).ok();
                let admitted = permit.is_some();
                barrier.wait();
                admitted
            }));
        }
        barrier.wait();
        barrier.wait();
        let admitted = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|admitted| *admitted)
            .count();
        assert_eq!(admitted, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_and_blocking_waiters_share_one_rate_budget() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(
            StableServerId(11),
            ServerTransferConfig {
                rate_bytes_per_sec: 20_000,
                quota: None,
            },
        );
        let mut warmup = control.try_reserve(0).unwrap();
        assert_eq!(warmup.record_async(20_000).await, Duration::ZERO);

        let barrier = Arc::new(Barrier::new(3));
        let async_barrier = Arc::clone(&barrier);
        let mut async_permit = control.try_reserve(0).unwrap();
        let async_waiter = tokio::spawn(async move {
            async_barrier.wait();
            async_permit.record_async(2_000).await
        });
        let blocking_barrier = Arc::clone(&barrier);
        let mut blocking_permit = control.try_reserve(0).unwrap();
        let blocking_waiter = tokio::task::spawn_blocking(move || {
            blocking_barrier.wait();
            blocking_permit.record_blocking(2_000)
        });

        barrier.wait();
        let mut waits = [async_waiter.await.unwrap(), blocking_waiter.await.unwrap()];
        waits.sort_unstable();
        assert!(waits[0] >= Duration::from_millis(50), "waits={waits:?}");
        assert!(waits[1] >= Duration::from_millis(150), "waits={waits:?}");
        assert!(waits[1] < Duration::from_secs(1), "waits={waits:?}");
    }

    #[tokio::test]
    async fn raising_rate_wakes_and_reprices_async_waiter() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(
            StableServerId(12),
            ServerTransferConfig {
                rate_bytes_per_sec: 10,
                quota: None,
            },
        );
        let mut warmup = control.try_reserve(0).unwrap();
        warmup.record_async(10).await;

        let mut permit = control.try_reserve(0).unwrap();
        let waiter = tokio::spawn(async move { permit.record_async(100).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        registry.configure(
            StableServerId(12),
            ServerTransferConfig {
                rate_bytes_per_sec: 1_000,
                quota: None,
            },
        );
        let waited = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("rate raise should wake waiter")
            .unwrap();
        assert!(waited < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn decreasing_rate_clamps_existing_burst_credit() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(
            StableServerId(13),
            ServerTransferConfig {
                rate_bytes_per_sec: 1_000,
                quota: None,
            },
        );
        let mut warmup = control.try_reserve(0).unwrap();
        warmup.record_async(100).await;
        registry.configure(
            StableServerId(13),
            ServerTransferConfig {
                rate_bytes_per_sec: 100,
                quota: None,
            },
        );

        let mut permit = control.try_reserve(0).unwrap();
        let waited = permit.record_async(150).await;
        assert!(waited >= Duration::from_millis(400), "waited={waited:?}");
        assert!(waited < Duration::from_secs(2), "waited={waited:?}");
    }

    #[tokio::test]
    async fn canceling_waiter_reconciles_quota_and_does_not_strand_rate_state() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(
            StableServerId(14),
            ServerTransferConfig {
                rate_bytes_per_sec: 10,
                quota: Some(QuotaRuntimeConfig {
                    limit_bytes: 1_000,
                    generation: 1,
                    retry_at: None,
                }),
            },
        );
        let mut warmup = control.try_reserve(0).unwrap();
        warmup.record_async(10).await;
        warmup.finish();

        let mut permit = control.try_reserve(100).unwrap();
        let waiter = tokio::spawn(async move { permit.record_async(100).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        waiter.abort();
        let _ = waiter.await;

        let snapshot = control.snapshot();
        assert_eq!(snapshot.quota_used_bytes, 110);
        assert_eq!(snapshot.quota_reserved_bytes, 0);
        registry.configure(StableServerId(14), ServerTransferConfig::default());
        let mut next = control.try_reserve(0).unwrap();
        assert_eq!(next.record_async(1).await, Duration::ZERO);
    }

    #[test]
    fn generation_change_racing_completion_never_leaks_reservations() {
        let registry = Arc::new(ServerTransferRegistry::new());
        let control = registry.configure(
            StableServerId(15),
            ServerTransferConfig {
                rate_bytes_per_sec: 0,
                quota: Some(QuotaRuntimeConfig {
                    limit_bytes: 1_000,
                    generation: 1,
                    retry_at: None,
                }),
            },
        );
        let permit = control.try_reserve(800).unwrap();
        let barrier = Arc::new(Barrier::new(3));

        let configure_registry = Arc::clone(&registry);
        let configure_barrier = Arc::clone(&barrier);
        let configure = std::thread::spawn(move || {
            configure_barrier.wait();
            configure_registry.configure(
                StableServerId(15),
                ServerTransferConfig {
                    rate_bytes_per_sec: 0,
                    quota: Some(QuotaRuntimeConfig {
                        limit_bytes: 1_000,
                        generation: 2,
                        retry_at: None,
                    }),
                },
            );
        });
        let complete_barrier = Arc::clone(&barrier);
        let complete = std::thread::spawn(move || {
            let mut permit = permit;
            complete_barrier.wait();
            permit.record_blocking(600);
        });

        barrier.wait();
        configure.join().unwrap();
        complete.join().unwrap();
        let snapshot = control.snapshot();
        assert_eq!(snapshot.quota_generation, 2);
        assert_eq!(snapshot.quota_reserved_bytes, 0);
        assert!(snapshot.quota_used_bytes <= 600);

        let remaining = 1_000 - snapshot.quota_used_bytes;
        let exact = control.try_reserve(remaining).unwrap();
        assert!(control.try_reserve(1).is_err());
        drop(exact);
    }

    #[test]
    fn request_larger_than_remaining_quota_is_current_without_global_block() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(StableServerId(20), quota(100, 1));
        let mut permit = control.try_reserve(60).unwrap();
        permit.record_blocking(60);
        permit.finish();

        let snapshot = control.snapshot();
        assert!(!snapshot.quota_blocked);
        assert_eq!(snapshot.quota_remaining_bytes, 40);
        assert!(control.quota_rejection_for(40).is_none());
        let rejection = control
            .quota_rejection_for(41)
            .expect("request larger than the remainder must be rejected");
        assert_eq!(rejection.requested_body_bytes, 41);
        assert_eq!(rejection.capacity_revision, snapshot.capacity_revision);
        assert!(!rejection.snapshot.quota_blocked);
    }

    #[tokio::test]
    async fn reservation_refund_wakes_async_and_blocking_capacity_waiters() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(StableServerId(21), quota(100, 1));
        let permit = control.try_reserve(80).unwrap();
        let rejection = control.quota_rejection_for(30).unwrap();
        let observed = rejection.capacity_revision;
        let mut async_changes = control.subscribe_capacity_changes();
        let blocking_control = Arc::clone(&control);
        let blocking_waiter = std::thread::spawn(move || {
            blocking_control
                .wait_for_capacity_change_blocking(observed, Some(Duration::from_secs(1)))
        });

        drop(permit);
        tokio::time::timeout(Duration::from_secs(1), async_changes.changed())
            .await
            .expect("refund must wake async capacity watcher")
            .unwrap();
        let blocking_revision = blocking_waiter.join().unwrap();
        assert!(blocking_revision > observed);
        assert!(control.quota_rejection_for(30).is_none());
        assert_eq!(control.snapshot().quota_reserved_bytes, 0);
    }

    #[test]
    fn registry_capacity_wakes_cover_policy_lifecycle_changes() {
        let registry = ServerTransferRegistry::new();
        let mut changes = registry.subscribe_capacity_changes();
        let mut observed = registry.capacity_revision();

        let control = registry.configure(StableServerId(126), quota(100, 1));
        assert_ne!(registry.capacity_revision(), observed);
        assert!(changes.has_changed().unwrap());
        drop(changes.borrow_and_update());
        observed = registry.capacity_revision();

        control.update_config(quota(100, 2));
        assert_ne!(registry.capacity_revision(), observed);
        assert!(changes.has_changed().unwrap());
        drop(changes.borrow_and_update());
        observed = registry.capacity_revision();

        control.update_config(ServerTransferConfig::default());
        assert_ne!(registry.capacity_revision(), observed);
        assert!(changes.has_changed().unwrap());
        drop(changes.borrow_and_update());
        observed = registry.capacity_revision();

        registry.remove(StableServerId(126));
        assert_ne!(registry.capacity_revision(), observed);
        assert!(changes.has_changed().unwrap());
        drop(changes.borrow_and_update());
        observed = registry.capacity_revision();

        registry.configure(StableServerId(127), quota(100, 1));
        assert_ne!(registry.capacity_revision(), observed);
        assert!(changes.has_changed().unwrap());
        drop(changes.borrow_and_update());
        observed = registry.capacity_revision();

        registry.clear();
        assert_ne!(registry.capacity_revision(), observed);
        assert!(changes.has_changed().unwrap());
    }

    #[tokio::test]
    async fn other_server_refund_wakes_registry_capacity_waiter() {
        let registry = ServerTransferRegistry::new();
        let selected = registry.configure(StableServerId(120), quota(100, 1));
        let other = registry.configure(StableServerId(121), quota(100, 1));
        let selected_permit = selected.try_reserve(100).unwrap();
        let other_permit = other.try_reserve(100).unwrap();
        let rejection = selected.quota_rejection_for(1).unwrap();
        assert!(rejection.retry_at.is_none());
        let mut changes = registry.subscribe_capacity_changes();

        drop(other_permit);

        tokio::time::timeout(Duration::from_secs(1), changes.changed())
            .await
            .expect("another server refund must wake the registry waiter")
            .unwrap();
        assert_ne!(
            registry.capacity_revision(),
            rejection.registry_capacity_revision
        );
        assert_eq!(
            selected.quota_rejection_for(1).unwrap().capacity_revision,
            rejection.capacity_revision,
            "the selected server remains locally blocked and unchanged"
        );
        assert!(other.quota_rejection_for(1).is_none());
        drop(selected_permit);
    }

    #[test]
    fn refund_before_registry_subscription_stales_stored_rejection() {
        let registry = ServerTransferRegistry::new();
        let selected = registry.configure(StableServerId(122), quota(100, 1));
        let other = registry.configure(StableServerId(123), quota(100, 1));
        let selected_permit = selected.try_reserve(100).unwrap();
        let other_permit = other.try_reserve(100).unwrap();
        let rejection = selected.quota_rejection_for(1).unwrap();

        drop(other_permit);
        let _changes = registry.subscribe_capacity_changes();

        assert_ne!(
            registry.capacity_revision(),
            rejection.registry_capacity_revision,
            "subscribe-then-currentness must detect a refund that preceded subscription"
        );
        assert_eq!(
            selected.quota_rejection_for(1).unwrap().capacity_revision,
            rejection.capacity_revision
        );
        drop(selected_permit);
    }

    #[test]
    fn concurrent_refunds_publish_distinct_registry_revisions() {
        let registry = ServerTransferRegistry::new();
        let first = registry.configure(StableServerId(124), quota(100, 1));
        let second = registry.configure(StableServerId(125), quota(100, 1));
        let first_permit = first.try_reserve(100).unwrap();
        let second_permit = second.try_reserve(100).unwrap();
        let observed = registry.capacity_revision();
        let changes = registry.subscribe_capacity_changes();
        let gate = Arc::new(Barrier::new(3));
        let first_gate = Arc::clone(&gate);
        let first_refund = std::thread::spawn(move || {
            first_gate.wait();
            drop(first_permit);
        });
        let second_gate = Arc::clone(&gate);
        let second_refund = std::thread::spawn(move || {
            second_gate.wait();
            drop(second_permit);
        });

        gate.wait();
        first_refund.join().unwrap();
        second_refund.join().unwrap();

        assert_eq!(
            registry.capacity_revision(),
            observed.wrapping_add(2).max(1)
        );
        assert!(changes.has_changed().unwrap());
    }

    #[tokio::test]
    async fn canceling_task_drops_permit_and_publishes_refund_revision() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(StableServerId(22), quota(100, 1));
        let permit = control.try_reserve(80).unwrap();
        let observed = control.snapshot().capacity_revision;
        let mut changes = control.subscribe_capacity_changes();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            let _permit = permit;
            let _ = ready_tx.send(());
            std::future::pending::<()>().await;
        });
        ready_rx.await.unwrap();
        waiter.abort();
        let _ = waiter.await;

        tokio::time::timeout(Duration::from_secs(1), changes.changed())
            .await
            .expect("canceled permit owner must publish its refund")
            .unwrap();
        let snapshot = control.snapshot();
        assert!(snapshot.capacity_revision > observed);
        assert_eq!(snapshot.quota_reserved_bytes, 0);
    }

    #[test]
    fn clear_allows_lower_imported_counters_to_replace_monotonic_controls() {
        let registry = ServerTransferRegistry::new();
        let old = registry.restore(
            StableServerId(23),
            quota(1_000, 1),
            ServerTransferInitialState {
                lifetime_body_bytes: 900,
                quota_used_bytes: 900,
            },
        );
        let monotonic = registry.restore(
            StableServerId(23),
            quota(1_000, 1),
            ServerTransferInitialState {
                lifetime_body_bytes: 100,
                quota_used_bytes: 100,
            },
        );
        assert!(Arc::ptr_eq(&old, &monotonic));
        assert_eq!(monotonic.snapshot().lifetime_body_bytes, 900);
        registry.control(StableServerId(24));

        registry.clear();
        assert!(registry.get(StableServerId(23)).is_none());
        assert!(registry.snapshot_if_present(StableServerId(24)).is_none());
        let restored = registry.restore(
            StableServerId(23),
            quota(1_000, 1),
            ServerTransferInitialState {
                lifetime_body_bytes: 100,
                quota_used_bytes: 100,
            },
        );
        assert!(!Arc::ptr_eq(&old, &restored));
        assert_eq!(restored.snapshot().lifetime_body_bytes, 100);
    }

    #[test]
    fn unlimited_body_path_uses_marker_and_relaxed_counter_only() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(StableServerId(40), ServerTransferConfig::default());
        let strong_count = Arc::strong_count(&control);

        for _ in 0..32 {
            let accounting = control.start_body(64 * 1024).unwrap();
            assert!(matches!(accounting, BodyTransferAccounting::Unlimited));
            assert_eq!(Arc::strong_count(&control), strong_count);
            control.record_unlimited_body_bytes(64 * 1024);
        }

        assert_eq!(control.snapshot().lifetime_body_bytes, 2 * 1024 * 1024);
        assert_eq!(control.path_snapshot(), TransferPathSnapshot::default());
    }

    #[test]
    fn rate_only_body_path_uses_atomic_tickets_without_quota_or_wait_locks() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(
            StableServerId(41),
            ServerTransferConfig {
                rate_bytes_per_sec: 1_000_000,
                quota: None,
            },
        );
        let BodyTransferAccounting::Tracked(mut permit) = control.start_body(0).unwrap() else {
            panic!("rate-limited BODY must carry local accounting");
        };

        for _ in 0..8 {
            assert_eq!(permit.record_blocking(1), Duration::ZERO);
        }
        let receipt = permit.finish();
        assert_eq!(receipt.body_bytes, 8);
        assert_eq!(
            control.path_snapshot(),
            TransferPathSnapshot {
                rate_ticket_reservations: 8,
                ..TransferPathSnapshot::default()
            }
        );
    }

    #[test]
    fn quota_only_body_path_locks_only_at_admission_and_terminal_reconcile() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(StableServerId(42), quota(1_000, 1));
        let BodyTransferAccounting::Tracked(mut permit) = control.start_body(100).unwrap() else {
            panic!("quota-limited BODY must carry local accounting");
        };

        for _ in 0..3 {
            assert_eq!(permit.record_blocking(20), Duration::ZERO);
        }
        let in_flight = control.snapshot();
        assert_eq!(in_flight.lifetime_body_bytes, 60);
        assert_eq!(in_flight.quota_used_bytes, 0);
        assert_eq!(in_flight.quota_reserved_bytes, 100);
        assert_eq!(
            control.path_snapshot(),
            TransferPathSnapshot {
                quota_lock_acquisitions: 1,
                ..TransferPathSnapshot::default()
            }
        );

        permit.finish();
        let completed = control.snapshot();
        assert_eq!(completed.quota_used_bytes, 60);
        assert_eq!(completed.quota_reserved_bytes, 0);
        assert_eq!(
            control.path_snapshot(),
            TransferPathSnapshot {
                quota_lock_acquisitions: 2,
                terminal_quota_reconciliations: 1,
                ..TransferPathSnapshot::default()
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mixed_async_and_blocking_chunks_never_take_the_quota_mutex() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(
            StableServerId(43),
            ServerTransferConfig {
                rate_bytes_per_sec: 1_000_000,
                quota: Some(QuotaRuntimeConfig {
                    limit_bytes: 10_000,
                    generation: 1,
                    retry_at: None,
                }),
            },
        );
        let BodyTransferAccounting::Tracked(mut async_permit) = control.start_body(100).unwrap()
        else {
            panic!("mixed-policy BODY must carry local accounting");
        };
        let BodyTransferAccounting::Tracked(mut blocking_permit) = control.start_body(100).unwrap()
        else {
            panic!("mixed-policy BODY must carry local accounting");
        };

        let blocking = tokio::task::spawn_blocking(move || {
            for _ in 0..8 {
                assert_eq!(blocking_permit.record_blocking(10), Duration::ZERO);
            }
            blocking_permit.finish()
        });
        for _ in 0..8 {
            assert_eq!(async_permit.record_async(10).await, Duration::ZERO);
        }
        async_permit.finish();
        assert_eq!(blocking.await.unwrap().body_bytes, 80);

        let snapshot = control.snapshot();
        assert_eq!(snapshot.lifetime_body_bytes, 160);
        assert_eq!(snapshot.quota_used_bytes, 160);
        assert_eq!(snapshot.quota_reserved_bytes, 0);
        assert_eq!(
            control.path_snapshot(),
            TransferPathSnapshot {
                quota_lock_acquisitions: 4,
                terminal_quota_reconciliations: 2,
                rate_ticket_reservations: 16,
                rate_wait_lock_acquisitions: 0,
            }
        );
    }

    #[tokio::test]
    async fn clearing_rate_wakes_async_waiter() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(
            StableServerId(11),
            ServerTransferConfig {
                rate_bytes_per_sec: 10,
                quota: None,
            },
        );
        let mut permit = control.try_reserve(0).unwrap();
        permit.record_async(10).await;

        let waiter = tokio::spawn(async move { permit.record_async(100).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        registry.configure(StableServerId(11), ServerTransferConfig::default());
        let waited = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("rate clear should wake waiter")
            .unwrap();
        assert!(waited < Duration::from_secs(1));
    }
}
