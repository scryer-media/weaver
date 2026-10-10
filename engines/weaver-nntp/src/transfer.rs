// Shared per-server BODY transfer policy.
//
// A [`ServerTransferRegistry`] is intentionally independent from an
// [`NntpClient`](crate::client::NntpClient). Keeping it alive across client
// rebuilds preserves rate-limit debt, quota reservations, and byte counters.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::error::NntpError;

// Credit an idle or starved schedule may spend at once before pacing
// resumes. It is small on purpose: a configured limit is a promise about
// every second, not just the long-run average, so a stall must not be
// followed by a second that reads far above the limit.
const RATE_BURST_MICROS: u64 = 50_000;
const RATE_SCHEDULE_EPOCH_SHIFT: u32 = 48;
const RATE_SCHEDULE_TARGET_MASK: u64 = (1_u64 << RATE_SCHEDULE_EPOCH_SHIFT) - 1;

// Durable server identifier supplied by the application database.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StableServerId(pub u32);

// What a transfer control meters: one server, or one egress shared by every
// server routed over it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum TransferScope {
    #[default]
    Server,
    Egress,
}

// Runtime quota window calculated by the application calendar policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaRuntimeConfig {
    pub limit_bytes: u64,
    // Changes on a scheduled rollover or explicit usage reset.
    pub generation: u64,
    // Monotonic wake deadline for recurring quotas. `None` means manual-only.
    pub retry_at: Option<Instant>,
}

// Live transfer policy for one server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerTransferConfig {
    // Aggregate BODY payload bytes per second. Zero means unlimited.
    pub rate_bytes_per_sec: u64,
    pub quota: Option<QuotaRuntimeConfig>,
}

// Durable counters restored before download workers start.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerTransferInitialState {
    pub lifetime_body_bytes: u64,
    pub quota_used_bytes: u64,
}

// Authoritative live view of one server's transfer policy and counters.
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
    // Monotonic revision for admission-capacity increases and config changes.
    pub capacity_revision: u64,
    pub retry_at: Option<Instant>,
    pub throttle_wait: Duration,
}

// Per-BODY elapsed-time budget that excludes deliberate local rate-limit waits.
//
// Kept as a deadline (`started + limit + excluded wait`) rather than as an
// elapsed-time subtraction, so a check is one clock read and a compare, and
// the read loop can share that clock read between the budget check and the
// read timeout it derives next.
#[derive(Debug)]
pub(crate) struct ActiveTransferBudget {
    limit: Duration,
    excluded_wait: Duration,
    deadline: Instant,
}

// Far enough that an unrepresentable deadline never expires within the life
// of a connection, while staying well inside `Instant`'s range.
const UNBOUNDED_DEADLINE: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

impl ActiveTransferBudget {
    pub(crate) fn new(limit: Duration) -> Self {
        let started = Instant::now();
        let deadline = started
            .checked_add(limit)
            .unwrap_or_else(|| started + UNBOUNDED_DEADLINE);
        Self {
            limit,
            excluded_wait: Duration::ZERO,
            deadline,
        }
    }

    pub(crate) fn exclude_wait(&mut self, waited: Duration) {
        self.excluded_wait = self.excluded_wait.saturating_add(waited);
        self.deadline = self.deadline.checked_add(waited).unwrap_or(self.deadline);
    }

    pub(crate) fn remaining(&self) -> Duration {
        self.remaining_at(Instant::now())
    }

    // Budget left as of a clock reading the caller already took.
    pub(crate) fn remaining_at(&self, now: Instant) -> Duration {
        self.deadline.saturating_duration_since(now)
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
    active_transfer_read_timeout_at(Instant::now(), command_timeout, budget)
}

// [`active_transfer_read_timeout`] against a clock reading the caller took
// for its budget check, so a read turn costs one clock read, not two.
pub(crate) fn active_transfer_read_timeout_at(
    now: Instant,
    command_timeout: Duration,
    budget: Option<&ActiveTransferBudget>,
) -> crate::error::Result<(Duration, bool)> {
    let Some(budget) = budget else {
        return Ok((command_timeout, false));
    };
    let remaining = budget.remaining_at(now);
    if remaining.is_zero() {
        return Err(active_transfer_timeout(budget));
    }
    Ok((command_timeout.min(remaining), remaining <= command_timeout))
}

// Admission failure for a BODY request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaRejection {
    // Whether a server or an egress turned the request away.
    pub scope: TransferScope,
    // The server or egress id, as `scope` says.
    pub stable_server_id: StableServerId,
    pub requested_body_bytes: u64,
    pub capacity_revision: u64,
    // Registry-wide revision captured while selecting quota-blocked servers.
    pub registry_capacity_revision: u64,
    pub retry_at: Option<Instant>,
    pub snapshot: Box<ServerTransferSnapshot>,
}

// Result returned when a BODY permit is explicitly completed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BodyTransferReceipt {
    pub body_bytes: u64,
    pub throttle_wait: Duration,
}

// Per-response accounting selected at BODY admission. The unlimited variant
// carries no allocation, reference-count operation, reservation, or lock.
pub(crate) enum BodyTransferAccounting {
    Unlimited,
    Tracked(BodyTransferPermit),
}

// Stable-ID registry shared by every runtime client generation.
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
    scope: TransferScope,
    controls: RwLock<HashMap<StableServerId, Arc<ServerTransferControl>>>,
    capacity: Arc<RegistryCapacitySignal>,
    // Whether quotas count bytes; new controls start with it.
    quota_metering: AtomicBool,
    // Holders whose metering was set on its own, which the registry-wide
    // setting leaves alone. Removing a holder drops its setting, so a later
    // holder given the same id follows the registry-wide setting; dropping
    // every control at once with [`Self::clear`] keeps them.
    quota_metering_overrides: RwLock<HashMap<StableServerId, bool>>,
}

impl Default for ServerTransferRegistry {
    fn default() -> Self {
        Self::with_scope(TransferScope::Server)
    }
}

impl ServerTransferRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    // An empty registry whose controls meter `scope`.
    pub fn with_scope(scope: TransferScope) -> Self {
        Self {
            scope,
            controls: RwLock::new(HashMap::new()),
            capacity: Arc::new(RegistryCapacitySignal::default()),
            quota_metering: AtomicBool::new(true),
            quota_metering_overrides: RwLock::new(HashMap::new()),
        }
    }

    // An empty registry for `scope` that shares this one's capacity signal,
    // so a capacity change in either is a change in both. A selection parked
    // on one revision then wakes for a server or an egress alike.
    pub fn sibling(&self, scope: TransferScope) -> Self {
        Self {
            scope,
            controls: RwLock::new(HashMap::new()),
            capacity: Arc::clone(&self.capacity),
            quota_metering: AtomicBool::new(true),
            quota_metering_overrides: RwLock::new(HashMap::new()),
        }
    }

    pub fn scope(&self) -> TransferScope {
        self.scope
    }

    // Monotonic revision for capacity changes in any registered server.
    pub fn capacity_revision(&self) -> u64 {
        self.capacity.revision()
    }

    // Subscribe before checking a parked selection's revision so capacity
    // changes racing selection cannot be missed.
    pub fn subscribe_capacity_changes(&self) -> watch::Receiver<u64> {
        self.capacity.subscribe()
    }

    // Return an existing durable control without creating one.
    pub fn get(&self, id: StableServerId) -> Option<Arc<ServerTransferControl>> {
        self.controls
            .read()
            .expect("transfer registry poisoned")
            .get(&id)
            .map(Arc::clone)
    }

    // Get or create the durable control for `id`.
    pub fn control(&self, id: StableServerId) -> Arc<ServerTransferControl> {
        if let Some(control) = self.get(id) {
            return control;
        }

        let capacity = Arc::clone(&self.capacity);
        let scope = self.scope;
        let mut controls = self.controls.write().expect("transfer registry poisoned");
        Arc::clone(controls.entry(id).or_insert_with(|| {
            let control = ServerTransferControl::new(id, scope, capacity);
            control
                .quota_metering
                .store(self.quota_metering_of(id), Ordering::Release);
            Arc::new(control)
        }))
    }

    // Start or stop quotas counting bytes on every control, present and
    // future, except those set on their own with
    // [`Self::set_quota_metering_for`]. While stopped, bytes still count
    // toward lifetime totals but not toward any quota, and no quota turns
    // work away. Usage already counted in a window is kept.
    pub fn set_quota_metering(&self, enabled: bool) {
        self.quota_metering.store(enabled, Ordering::Release);
        let overrides = self
            .quota_metering_overrides
            .read()
            .expect("transfer registry poisoned")
            .clone();
        let controls = self
            .controls
            .read()
            .expect("transfer registry poisoned")
            .iter()
            .filter(|(id, _)| !overrides.contains_key(id))
            .map(|(_, control)| Arc::clone(control))
            .collect::<Vec<_>>();
        for control in controls {
            control.set_quota_metering(enabled);
        }
    }

    // Start or stop one holder's quota counting bytes, whatever the
    // registry-wide setting is now or is set to later.
    pub fn set_quota_metering_for(&self, id: StableServerId, enabled: bool) {
        self.quota_metering_overrides
            .write()
            .expect("transfer registry poisoned")
            .insert(id, enabled);
        if let Some(control) = self.get(id) {
            control.set_quota_metering(enabled);
        }
    }

    // Hand one holder's quota counting back to the registry-wide setting.
    pub fn clear_quota_metering_for(&self, id: StableServerId) {
        let removed = self
            .quota_metering_overrides
            .write()
            .expect("transfer registry poisoned")
            .remove(&id)
            .is_some();
        if removed && let Some(control) = self.get(id) {
            control.set_quota_metering(self.quota_metering.load(Ordering::Acquire));
        }
    }

    // The registry-wide setting, which holders without their own follow.
    pub fn quota_metering(&self) -> bool {
        self.quota_metering.load(Ordering::Acquire)
    }

    // Whether `id`'s quota counts bytes.
    pub fn quota_metering_of(&self, id: StableServerId) -> bool {
        self.quota_metering_overrides
            .read()
            .expect("transfer registry poisoned")
            .get(&id)
            .copied()
            .unwrap_or_else(|| self.quota_metering.load(Ordering::Acquire))
    }

    // Apply a live policy update while preserving counters and reservations.
    pub fn configure(
        &self,
        id: StableServerId,
        config: ServerTransferConfig,
    ) -> Arc<ServerTransferControl> {
        let control = self.control(id);
        control.configure(config, None);
        control
    }

    // Restore durable counters and apply the initial live policy.
    //
    // This is intended for startup before workers receive the control. Later
    // calls never reduce the lifetime counter.
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

    // Remove an inactive server control, and the quota metering set on its
    // own. Existing lanes keep their `Arc` and remain safe until they drain.
    pub fn remove(&self, id: StableServerId) -> Option<Arc<ServerTransferControl>> {
        self.quota_metering_overrides
            .write()
            .expect("transfer registry poisoned")
            .remove(&id);
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

    // Remove every registered control. Existing lane-held `Arc`s remain valid.
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

// Shared transfer state for one durable server.
pub struct ServerTransferControl {
    id: StableServerId,
    scope: TransferScope,
    pub(crate) socket_budget: Arc<crate::socket_budget::SocketBudget>,
    pub(crate) recovery: Arc<crate::recovery::RecoveryGate>,
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
    // False while quota metering is suspended.
    quota_metering: AtomicBool,
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
    // Sticky signal that a BODY reservation was refused for quota. Conservative
    // reservation reserves the estimate and reconciles down to the smaller
    // actual, so `quota_used_bytes` stays strictly below the limit in steady
    // state and `used >= limit` almost never latches. This records the real
    // "refusing work" condition instead. Set when a reservation-sized request
    // is rejected, cleared when one is admitted or the window/config resets.
    quota_saturated: bool,
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
    fn new(
        id: StableServerId,
        scope: TransferScope,
        registry_capacity: Arc<RegistryCapacitySignal>,
    ) -> Self {
        let (capacity_changed, _) = watch::channel(1);
        Self {
            id,
            scope,
            socket_budget: crate::socket_budget::SocketBudget::new(0),
            recovery: Arc::default(),
            state: Mutex::new(TransferState {
                initialized: false,
                config: ServerTransferConfig::default(),
                quota_used_bytes: 0,
                quota_reserved_bytes: 0,
                quota_saturated: false,
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
            quota_metering: AtomicBool::new(true),
            #[cfg(test)]
            path_counters: TransferPathCounters::default(),
            #[cfg(test)]
            rate_reserve_pause: Mutex::new(None),
        }
    }

    pub fn stable_server_id(&self) -> StableServerId {
        self.id
    }

    pub fn scope(&self) -> TransferScope {
        self.scope
    }

    pub fn update_config(&self, config: ServerTransferConfig) {
        self.configure(config, None);
    }

    fn set_quota_metering(&self, enabled: bool) {
        if self.quota_metering.swap(enabled, Ordering::AcqRel) == enabled {
            return;
        }
        let revision = {
            let mut state = self.state.lock().expect("server transfer state poisoned");
            state.quota_saturated = false;
            state.capacity_revision = state.capacity_revision.wrapping_add(1).max(1);
            state.capacity_revision
        };
        self.publish_capacity_change(revision);
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
            // A changed allowance in the same window keeps its usage but may
            // fit work it turned away.
            let limit_changed = matches!(
                (state.config.quota, config.quota),
                (Some(previous), Some(next)) if previous.limit_bytes != next.limit_bytes
            );
            // A window rollover, manual reset, or quota edit all bump the
            // generation (or drop the quota); either way the refusal condition
            // no longer holds, so the server is free to take work again.
            if quota_epoch_changed || limit_changed || !state.initialized || config.quota.is_none()
            {
                state.quota_saturated = false;
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

    // Check BODY admission atomically without reserving capacity.
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

    // Check BODY admission for a dispatch that is choosing its server.
    //
    // An owned lane picks its server before it reserves, so a request that
    // does not fit is skipped here and never reaches `try_reserve`. That skip
    // is the moment this server turns the work away, and it latches
    // `quota_blocked` exactly as a refused reservation does; a request that
    // fits clears the latch the way an admitted reservation does.
    pub fn quota_rejection_for_dispatch(
        &self,
        requested_body_bytes: u64,
    ) -> Option<QuotaRejection> {
        if self.quota_epoch.load(Ordering::Acquire) == 0 {
            return None;
        }
        #[cfg(test)]
        self.path_counters
            .quota_lock_acquisitions
            .fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.lock().expect("server transfer state poisoned");
        let rejection = self.quota_rejection_locked(&state, requested_body_bytes);
        state.quota_saturated = rejection.is_some();
        rejection
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

    // Record BODY bytes for an admission proven to have neither rate nor
    // quota policy. This is the production unlimited marker's only hot-path
    // operation and intentionally performs one relaxed atomic update.
    #[doc(hidden)]
    pub fn record_unlimited_body_bytes(&self, bytes: usize) {
        self.record_lifetime_bytes(bytes as u64);
    }

    // Read-path pacing without quota admission, shared by all connections on an egress.
    pub async fn pace_read_async(&self, bytes: usize) -> Duration {
        self.record_lifetime_bytes(bytes as u64);
        match self.reserve_rate(bytes as u64) {
            Some(ticket) => self.wait_async(ticket).await,
            None => Duration::ZERO,
        }
    }
    pub fn pace_read_blocking(&self, bytes: usize) -> Duration {
        self.record_lifetime_bytes(bytes as u64);
        match self.reserve_rate(bytes as u64) {
            Some(ticket) => self.wait_blocking(ticket),
            None => Duration::ZERO,
        }
    }
    pub fn pace_read_without_wait(&self, bytes: usize) {
        self.record_lifetime_bytes(bytes as u64);
        let _ = self.reserve_rate(bytes as u64);
    }

    // Read-path pacing without quota admission, reserved but not yet waited
    // for. See [`RateCharge`].
    pub(crate) fn charge_read(self: &Arc<Self>, bytes: usize) -> Option<RateCharge> {
        self.record_lifetime_bytes(bytes as u64);
        self.reserve_rate(bytes as u64).map(|ticket| RateCharge {
            control: Arc::clone(self),
            ticket,
        })
    }

    // Reserve the estimated raw BODY payload before issuing `BODY`.
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
                // A real dispatch reservation was refused: latch blocked so the
                // per-server signal reflects that the server is turning work
                // away, which the strict `used >= limit` check misses under
                // conservative (estimate-then-reconcile) reservation.
                state.quota_saturated = true;
                return Err(rejection);
            }
            if state.config.quota.is_some() {
                state.quota_reserved_bytes = state
                    .quota_reserved_bytes
                    .saturating_add(estimated_body_bytes);
                reserved_quota_bytes = estimated_body_bytes;
                // Admitting a reservation means the server is taking work again.
                state.quota_saturated = false;
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

    // Subscribe before checking admission state to avoid missing a release.
    pub fn subscribe_capacity_changes(&self) -> watch::Receiver<u64> {
        self.capacity_changed.subscribe()
    }

    // Wait until a quota config/reset update, reservation release, or rate change.
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
            quota_blocked: self.quota_metering.load(Ordering::Acquire)
                && quota.is_some_and(|_| projected >= limit || state.quota_saturated),
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
        if !self.quota_metering.load(Ordering::Acquire) {
            return None;
        }
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
            scope: self.scope,
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
        self.reserve_rate_with_clock(bytes, || self.rate_now_micros())
    }

    fn reserve_rate_with_clock(&self, bytes: u64, now: impl Fn() -> u64) -> Option<RateTicket> {
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
            let now = now();
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

    // The instant, on the same scale as [`Self::rate_now_micros`], that the
    // last reservation scheduled. Tests compare reservations and waits
    // against this rather than against wall-clock durations.
    #[cfg(test)]
    fn rate_schedule_target_micros(&self) -> u64 {
        self.rate_schedule.load(Ordering::Acquire) & RATE_SCHEDULE_TARGET_MASK
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
            // Nothing dispatches to an egress that reads as blocked, so freed
            // allowance has to lift the refusal itself; the next body that
            // still does not fit sets it again.
            if self.scope == TransferScope::Egress {
                state.quota_saturated = false;
            }
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

// A read's rate cost on one control, reserved but not yet waited for.
//
// A read through an egress to a provider is paced by both. Reserving every
// level's charge before waiting on any makes the read wait until the latest
// of their deadlines, so it runs at the lowest of the rates. Waiting on one
// level before reserving on the next would add the waits instead, and hold
// the read below every limit that applies to it.
pub(crate) struct RateCharge {
    control: Arc<ServerTransferControl>,
    ticket: RateTicket,
}

impl RateCharge {
    // When this level lets the read go on.
    #[cfg(test)]
    fn deadline(&self) -> Instant {
        self.control.rate_origin
            + Duration::from_micros(self.ticket.target_micros.saturating_sub(RATE_BURST_MICROS))
    }

    pub(crate) async fn wait_async(self) -> Duration {
        self.control.wait_async(self.ticket).await
    }

    pub(crate) fn wait_blocking(self) -> Duration {
        self.control.wait_blocking(self.ticket)
    }
}

// RAII admission permit for exactly one BODY response.
//
// Dropping it refunds any unused estimate. Bytes already reported remain
// charged, including bytes received before a decode or transport failure.
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

    // Charge bytes and wait on the shared async aggregate limiter.
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

    // Charge received bytes and preserve rate debt without delaying cleanup.
    pub(crate) fn record_without_wait(&mut self, bytes: usize) {
        let bytes = bytes as u64;
        self.record_bytes(bytes);
        let _ = self.control.reserve_rate(bytes);
    }

    // Charge bytes and wait on the shared blocking aggregate limiter.
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

    // Charge bytes and reserve their rate cost without waiting. The caller
    // waits on the charge and credits the wait with
    // [`Self::add_throttle_wait`].
    pub(crate) fn charge(&mut self, bytes: usize) -> Option<RateCharge> {
        let bytes = bytes as u64;
        self.record_bytes(bytes);
        self.control.reserve_rate(bytes).map(|ticket| RateCharge {
            control: Arc::clone(&self.control),
            ticket,
        })
    }

    pub(crate) fn add_throttle_wait(&mut self, waited: Duration) {
        self.throttle_wait = self.throttle_wait.saturating_add(waited);
    }

    fn record_bytes(&mut self, bytes: u64) {
        self.control.record_lifetime_bytes(bytes);
        self.body_bytes = self.body_bytes.saturating_add(bytes);
        let quota_epoch = self.control.current_quota_epoch();
        if quota_epoch == 0 || !self.control.quota_metering.load(Ordering::Acquire) {
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

        // Read each budget at the instant it started, so the test is not
        // racing the clock it is checking.
        let long_budget = ActiveTransferBudget::new(Duration::from_secs(60));
        let started = long_budget.deadline - Duration::from_secs(60);
        assert_eq!(
            active_transfer_read_timeout_at(started, command_timeout, Some(&long_budget)).unwrap(),
            (command_timeout, false)
        );

        let short_budget = ActiveTransferBudget::new(Duration::from_secs(1));
        let started = short_budget.deadline - Duration::from_secs(1);
        assert_eq!(
            active_transfer_read_timeout_at(started, command_timeout, Some(&short_budget)).unwrap(),
            (Duration::from_secs(1), true)
        );

        let expired = ActiveTransferBudget::new(Duration::ZERO);
        assert!(matches!(
            active_transfer_read_timeout(command_timeout, Some(&expired)),
            Err(NntpError::SoftTimeout(0))
        ));
    }

    // The deadline is the budget: excluded waits push it out, and a check
    // against a caller-supplied clock reading agrees with the arithmetic.
    #[test]
    fn active_transfer_budget_deadline_moves_with_excluded_waits() {
        let mut budget = ActiveTransferBudget::new(Duration::from_secs(10));
        let started = budget.deadline - Duration::from_secs(10);
        assert_eq!(budget.remaining_at(started), Duration::from_secs(10));
        assert_eq!(
            budget.remaining_at(started + Duration::from_secs(4)),
            Duration::from_secs(6)
        );

        budget.exclude_wait(Duration::from_secs(3));
        assert_eq!(
            budget.remaining_at(started + Duration::from_secs(12)),
            Duration::from_secs(1)
        );
        assert!(
            budget
                .remaining_at(started + Duration::from_secs(13))
                .is_zero()
        );
        assert!(matches!(
            active_transfer_read_timeout_at(
                started + Duration::from_secs(13),
                Duration::from_secs(60),
                Some(&budget)
            ),
            Err(NntpError::SoftTimeout(10))
        ));
        assert_eq!(
            active_transfer_read_timeout_at(
                started + Duration::from_secs(12),
                Duration::from_secs(60),
                Some(&budget)
            )
            .unwrap(),
            (Duration::from_secs(1), true)
        );

        let unbounded = ActiveTransferBudget::new(Duration::MAX);
        assert!(!unbounded.remaining().is_zero());
    }

    // Wait until a spawned rate waiter is parked on its wake-up: it
    // subscribes to capacity changes only once it has a ticket to wait on.
    async fn wait_for_rate_waiter(control: &ServerTransferControl) {
        while control.capacity_changed.receiver_count() == 0 {
            tokio::task::yield_now().await;
        }
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
    fn refused_reservation_reports_blocked_below_the_limit_and_clears_on_reset() {
        let registry = ServerTransferRegistry::new();
        // A limit that admits one 800-estimate reservation but not two: after
        // one article the used bytes reconcile below the limit, so the strict
        // `used >= limit` check would never latch.
        let control = registry.configure(StableServerId(11), quota(1_000, 1));

        let mut permit = control.try_reserve(800).unwrap();
        permit.record_blocking(650);
        drop(permit);
        let snapshot = control.snapshot();
        assert!(snapshot.quota_used_bytes < snapshot.quota_limit_bytes);
        assert!(
            !snapshot.quota_blocked,
            "one admitted article is not blocked"
        );

        // The next reservation cannot fit and is refused; the server must now
        // report blocked even though used (650) is below the limit (1000).
        assert!(control.try_reserve(800).is_err());
        let snapshot = control.snapshot();
        assert_eq!(snapshot.quota_used_bytes, 650);
        assert!(snapshot.quota_used_bytes < snapshot.quota_limit_bytes);
        assert!(
            snapshot.quota_blocked,
            "a refused reservation latches blocked"
        );
        assert_eq!(snapshot.quota_remaining_bytes, 350);

        // A smaller reservation that still fits admits and clears the stale
        // refusal without any reset.
        let mut permit = control.try_reserve(200).unwrap();
        assert!(!control.snapshot().quota_blocked);
        permit.record_blocking(150);
        drop(permit);
        assert!(!control.snapshot().quota_blocked);

        // Refuse again, then prove a window/quota reset (new generation) clears
        // the refusal.
        assert!(control.try_reserve(800).is_err());
        assert!(control.snapshot().quota_blocked);
        registry.configure(StableServerId(11), quota(1_000, 2));
        assert!(!control.snapshot().quota_blocked);
    }

    #[test]
    fn a_dispatch_skipped_for_headroom_latches_blocked_like_a_refused_reservation() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(StableServerId(12), quota(1_000, 1));
        let mut permit = control.try_reserve(800).unwrap();
        permit.record_blocking(650);
        drop(permit);

        // The read-only check answers without touching the signal.
        assert!(control.quota_rejection_for(800).is_some());
        assert!(!control.snapshot().quota_blocked);

        // A dispatch that skips the server for headroom is the server turning
        // work away, and the operator-facing signal must say so.
        let rejection = control
            .quota_rejection_for_dispatch(800)
            .expect("800 cannot fit beside 650 used");
        assert!(!rejection.snapshot.quota_blocked);
        assert!(control.snapshot().quota_blocked);

        // A dispatch the server can take clears the latch without a reset.
        assert!(control.quota_rejection_for_dispatch(200).is_none());
        assert!(!control.snapshot().quota_blocked);
    }

    #[test]
    fn a_raised_quota_lifts_the_refusal_and_keeps_the_usage() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(StableServerId(13), quota(1_000, 1));
        let mut permit = control.try_reserve(800).unwrap();
        permit.record_blocking(650);
        drop(permit);
        assert!(control.try_reserve(800).is_err());
        assert!(control.snapshot().quota_blocked);

        registry.configure(StableServerId(13), quota(5_000, 1));
        let snapshot = control.snapshot();
        assert!(!snapshot.quota_blocked);
        assert_eq!(snapshot.quota_used_bytes, 650);
    }

    #[test]
    fn freed_egress_quota_lifts_the_refusal_without_a_new_reservation() {
        let registry = ServerTransferRegistry::with_scope(TransferScope::Egress);
        let control = registry.configure(StableServerId(14), quota(1_000, 1));
        let mut in_flight = control.try_reserve(800).unwrap();
        // The second body does not fit beside the first one's estimate.
        assert!(control.try_reserve(250).is_err());
        assert!(control.snapshot().quota_blocked);

        // The first body comes in well under its estimate.
        in_flight.record_blocking(300);
        drop(in_flight);
        let snapshot = control.snapshot();
        assert_eq!(snapshot.quota_used_bytes, 300);
        assert!(!snapshot.quota_blocked);
        assert!(control.try_reserve(250).is_ok());
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
    fn aggregate_rate_tickets_share_the_burst_credit() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(
            StableServerId(30),
            ServerTransferConfig {
                rate_bytes_per_sec: 100,
                quota: None,
            },
        );

        // Exactly the burst credit's worth of bytes schedules without a wait.
        let now = || RATE_BURST_MICROS;
        let warmup = control.reserve_rate_with_clock(5, now).unwrap();
        assert_eq!(warmup.target_micros, now());
        let first = control
            .reserve_rate_with_clock(50, now)
            .expect("burst credit is exhausted");
        let second = control
            .reserve_rate_with_clock(50, now)
            .expect("aggregate debt is shared");
        // Each ticket is scheduled after the one before it, by its own cost:
        // the burst credit went to the warmup and the debt is shared. The
        // targets are compared with each other, not with the clock.
        assert_eq!(first.target_micros - warmup.target_micros, 500_000);
        assert_eq!(second.target_micros - first.target_micros, 500_000);
    }

    #[test]
    fn low_rates_sustain_the_configured_throughput_after_the_initial_burst() {
        for rate in [1, 10, 100, 1024] {
            let registry = ServerTransferRegistry::new();
            let control = registry.configure(
                StableServerId(1),
                ServerTransferConfig {
                    rate_bytes_per_sec: rate,
                    quota: None,
                },
            );
            for second in 0..20 {
                let now = RATE_BURST_MICROS + second * 1_000_000;
                let ticket = control.reserve_rate_with_clock(rate, || now).unwrap();
                // One second's bytes costs one second on the shared ledger,
                // even when the burst allowance is smaller than one byte.
                assert_eq!(ticket.target_micros, (second + 1) * 1_000_000);
            }
        }
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
        // Spend exactly the burst credit so both waiters below are paced.
        let mut warmup = control.try_reserve(0).unwrap();
        assert_eq!(warmup.record_async(1_000).await, Duration::ZERO);
        let scheduled_before = control.rate_schedule_target_micros();

        // Each waiter reports the schedule clock it saw on return, so the
        // assertions below can talk about where in the shared schedule that
        // waiter landed. Which of the two reserves first is a race, so they
        // are compared as a sorted pair rather than individually.
        let barrier = Arc::new(Barrier::new(3));
        let async_barrier = Arc::clone(&barrier);
        let async_control = Arc::clone(&control);
        let mut async_permit = control.try_reserve(0).unwrap();
        let async_waiter = tokio::spawn(async move {
            async_barrier.wait();
            let waited = async_permit.record_async(2_000).await;
            (async_control.rate_now_micros(), waited)
        });
        let blocking_barrier = Arc::clone(&barrier);
        let blocking_control = Arc::clone(&control);
        let mut blocking_permit = control.try_reserve(0).unwrap();
        let blocking_waiter = tokio::task::spawn_blocking(move || {
            blocking_barrier.wait();
            let waited = blocking_permit.record_blocking(2_000);
            (blocking_control.rate_now_micros(), waited)
        });

        barrier.wait();
        let mut returns = [async_waiter.await.unwrap(), blocking_waiter.await.unwrap()];
        returns.sort_unstable();

        // Three properties say the budget is shared: one schedule absorbed
        // both reservations, and each waiter returned no earlier than the
        // instant that schedule handed it -- the first one a whole charge
        // after the warmup, the second a charge after that. None of them is a
        // duration threshold. The waited durations themselves are not
        // assertable, because each also measures how long the runtime took to
        // put that waiter on a core, which on a busy machine outlasts the wait
        // under test and drives the reported duration to zero.
        let cost = rate_cost_micros(2_000, 20_000);
        let scheduled_after = control.rate_schedule_target_micros();
        assert!(
            scheduled_after >= scheduled_before.saturating_add(2 * cost),
            "one schedule must carry both reservations, returns={returns:?}"
        );
        assert!(
            returns[0].0 >= scheduled_before.saturating_add(cost),
            "the first waiter returned before its scheduled instant, returns={returns:?}"
        );
        assert!(
            returns[1].0 >= scheduled_after,
            "the second waiter returned before its scheduled instant, returns={returns:?}"
        );
    }

    #[tokio::test]
    async fn raising_rate_wakes_and_reprices_async_waiter() {
        let registry = ServerTransferRegistry::new();
        let control = registry.configure(
            StableServerId(12),
            ServerTransferConfig {
                rate_bytes_per_sec: 1,
                quota: None,
            },
        );
        let mut warmup = control.try_reserve(0).unwrap();
        warmup.record_async(1).await;

        // At the old rate this waiter would sleep for more than a day, so
        // only the reconfigure can finish it.
        let mut permit = control.try_reserve(0).unwrap();
        let waiter = tokio::spawn(async move { permit.record_async(100_000).await });
        wait_for_rate_waiter(&control).await;
        registry.configure(
            StableServerId(12),
            ServerTransferConfig {
                rate_bytes_per_sec: 1_000_000_000,
                quota: None,
            },
        );
        waiter.await.unwrap();
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

        // Only the burst credit at the new rate survives: 5 of the 150 bytes,
        // so the ticket lands 1.45 s past the clock reading taken before it
        // was reserved.
        let before = control.rate_now_micros();
        let ticket = control.reserve_rate(150).expect("a rate is configured");
        assert!(
            ticket.target_micros >= before + 1_450_000,
            "target {} is less than 1.45 s past {before}",
            ticket.target_micros
        );
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
        wait_for_rate_waiter(&control).await;
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
            blocking_control.wait_for_capacity_change_blocking(observed, None)
        });

        drop(permit);
        async_changes.changed().await.unwrap();
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

        changes.changed().await.unwrap();
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

        changes.changed().await.unwrap();
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
                rate_bytes_per_sec: 1,
                quota: None,
            },
        );
        let mut permit = control.try_reserve(0).unwrap();
        permit.record_async(1).await;

        // At the old rate this waiter would sleep for more than a day, so
        // only clearing the rate can finish it.
        let waiter = tokio::spawn(async move { permit.record_async(100_000).await });
        wait_for_rate_waiter(&control).await;
        registry.configure(StableServerId(11), ServerTransferConfig::default());
        waiter.await.unwrap();
    }

    #[test]
    fn suspended_metering_neither_counts_nor_refuses_and_keeps_window_usage() {
        let registry = ServerTransferRegistry::with_scope(TransferScope::Egress);
        let control = registry.configure(StableServerId(3), quota(1_000, 1));
        let mut permit = control.try_reserve(600).unwrap();
        permit.record_blocking(600);
        drop(permit);
        let rejection = control.try_reserve(500).err().unwrap();
        assert_eq!(rejection.scope, TransferScope::Egress);
        assert!(control.snapshot().quota_blocked);

        registry.set_quota_metering(false);
        assert!(!control.snapshot().quota_blocked);
        let mut permit = control.try_reserve(500).unwrap();
        permit.record_blocking(500);
        drop(permit);
        let snapshot = control.snapshot();
        assert_eq!(snapshot.lifetime_body_bytes, 1_100);
        assert_eq!(snapshot.quota_used_bytes, 600);
        // A control made while metering is off starts off too.
        let later = registry.configure(StableServerId(4), quota(1, 1));
        assert!(later.try_reserve(10).is_ok());

        registry.set_quota_metering(true);
        assert!(control.try_reserve(500).is_err());
        assert!(control.try_reserve(400).is_ok());
    }

    #[test]
    fn one_egress_quota_metering_wins_over_the_setting_for_every_egress() {
        let registry = ServerTransferRegistry::with_scope(TransferScope::Egress);
        let own = registry.configure(StableServerId(1), quota(1, 1));
        let other = registry.configure(StableServerId(2), quota(1, 1));

        registry.set_quota_metering_for(StableServerId(1), false);
        assert!(own.try_reserve(10).is_ok());
        assert!(other.try_reserve(10).is_err());

        // Every egress's setting leaves the one set on its own alone, either way.
        registry.set_quota_metering(true);
        assert!(own.try_reserve(10).is_ok());
        registry.set_quota_metering(false);
        assert!(other.try_reserve(10).is_ok());
        registry.set_quota_metering_for(StableServerId(1), true);
        assert!(own.try_reserve(10).is_err());
        assert!(other.try_reserve(10).is_ok());

        // A holder made again under a removed one's id follows every
        // egress's setting, not the removed one's own.
        registry.remove(StableServerId(1));
        let reused = registry.configure(StableServerId(1), quota(1, 1));
        assert!(reused.try_reserve(10).is_ok());
        assert!(!registry.quota_metering_of(StableServerId(1)));
        registry.set_quota_metering(true);
        assert!(reused.try_reserve(10).is_err());
    }

    #[test]
    fn clearing_one_egress_quota_metering_hands_it_back_to_every_egress() {
        let registry = ServerTransferRegistry::with_scope(TransferScope::Egress);
        let own = registry.configure(StableServerId(1), quota(1, 1));
        registry.set_quota_metering(false);
        registry.set_quota_metering_for(StableServerId(1), true);
        assert!(own.try_reserve(10).is_err());

        registry.clear_quota_metering_for(StableServerId(1));
        assert!(own.try_reserve(10).is_ok());
        assert!(!registry.quota_metering_of(StableServerId(1)));
        registry.set_quota_metering(true);
        assert!(own.try_reserve(10).is_err());
    }

    #[test]
    fn dropping_every_control_keeps_one_egress_quota_metering() {
        let registry = ServerTransferRegistry::with_scope(TransferScope::Egress);
        registry.configure(StableServerId(1), quota(1, 1));
        registry.set_quota_metering_for(StableServerId(1), false);
        registry.clear();
        let restored = registry.configure(StableServerId(1), quota(1, 1));
        assert!(restored.try_reserve(10).is_ok());
    }

    // An egress and a provider pacing the same reads. The rates are a few
    // bytes a second against reads of thousands of bytes, so each charge is
    // minutes of schedule and every later charge chains onto the one before
    // it however slowly the test runs.
    fn paced_pair(
        egress_rate: u64,
        server_rate: u64,
    ) -> (Arc<ServerTransferControl>, Arc<ServerTransferControl>) {
        let servers = ServerTransferRegistry::new();
        let egresses = servers.sibling(TransferScope::Egress);
        let rate = |rate_bytes_per_sec| ServerTransferConfig {
            rate_bytes_per_sec,
            quota: None,
        };
        (
            egresses.configure(StableServerId(1), rate(egress_rate)),
            servers.configure(StableServerId(2), rate(server_rate)),
        )
    }

    // When each read of `reads` bytes may go on: the latest deadline of the
    // levels it was charged on, both charged before either is waited for.
    fn read_deadlines(
        egress: &Arc<ServerTransferControl>,
        server: &Arc<ServerTransferControl>,
        reads: &[usize],
    ) -> Vec<(Instant, Option<Instant>, Option<Instant>)> {
        let mut permit = server.try_reserve(0).unwrap();
        reads
            .iter()
            .map(|&bytes| {
                let egress = egress.charge_read(bytes).map(|charge| charge.deadline());
                let server = permit.charge(bytes).map(|charge| charge.deadline());
                let read = egress.into_iter().chain(server).max().unwrap();
                (read, egress, server)
            })
            .collect()
    }

    #[test]
    fn a_provider_faster_than_its_egress_is_held_to_the_egress_rate() {
        let (egress, server) = paced_pair(2, 8);
        let reads = read_deadlines(&egress, &server, &[1_000; 4]);
        for (read, egress, server) in &reads {
            assert_eq!(Some(*read), *egress);
            assert!(server.unwrap() < *read);
        }
        // 1000 bytes at 2 bytes a second is 500 seconds a read.
        for pair in reads.windows(2) {
            assert_eq!(pair[1].0 - pair[0].0, Duration::from_secs(500));
        }
    }

    #[test]
    fn an_egress_faster_than_its_provider_is_held_to_the_provider_rate() {
        let (egress, server) = paced_pair(8, 2);
        let reads = read_deadlines(&egress, &server, &[1_000; 4]);
        for (read, _, server) in &reads {
            assert_eq!(Some(*read), *server);
        }
        for pair in reads.windows(2) {
            assert_eq!(pair[1].0 - pair[0].0, Duration::from_secs(500));
        }
    }

    #[test]
    fn raising_the_provider_above_its_egress_changes_nothing() {
        let spacing = |server_rate| {
            let (egress, server) = paced_pair(2, server_rate);
            let reads = read_deadlines(&egress, &server, &[1_000; 3]);
            reads
                .windows(2)
                .map(|pair| pair[1].0 - pair[0].0)
                .collect::<Vec<_>>()
        };
        assert_eq!(spacing(4), spacing(4_000));
        assert_eq!(spacing(4), vec![Duration::from_secs(500); 2]);
    }

    #[test]
    fn no_limit_at_one_level_leaves_the_other_in_charge() {
        let (egress, server) = paced_pair(0, 2);
        let reads = read_deadlines(&egress, &server, &[1_000; 3]);
        for (read, egress, server) in &reads {
            assert_eq!(*egress, None);
            assert_eq!(Some(*read), *server);
        }
    }
}
