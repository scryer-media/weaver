//! Process-wide PAR3 admission, resolved once outside the download loop.

use super::*;
use par3_rs::runtime::MemoryBudget;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, Weak};

#[derive(Debug, Clone, Copy)]
struct Limits {
    native: usize,
    metadata: usize,
    payload: usize,
}

impl Limits {
    fn for_memory(memory: Option<u64>) -> Self {
        let total = memory.map_or(256 << 20, |bytes| (bytes / 8).min(2 << 30)) as usize;
        let native = total / 2;
        let metadata = (total / 16).min(64 << 20);
        Self {
            native,
            metadata,
            payload: total - native - metadata,
        }
    }
}

pub(super) struct Budget {
    limit: usize,
    used: AtomicUsize,
    label: &'static str,
}

impl Budget {
    /// Bytes currently held by live reservations against this budget.
    fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    fn new(limit: usize, label: &'static str) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
            label,
        })
    }

    pub fn acquire(self: &Arc<Self>, bytes: usize) -> EngineResult<Reservation> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.limit)
            })
            .map_err(|used| {
                tracing::debug!(
                    stage = "admission",
                    budget = self.label,
                    requested_bytes = bytes,
                    used_bytes = used,
                    limit_bytes = self.limit,
                    "PAR3 memory admission refused"
                );
                EngineError::ResourceLimit(self.label)
            })?;
        Ok(Reservation {
            budget: Arc::clone(self),
            bytes,
        })
    }
}

pub(super) struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Reservation {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

pub(super) struct PayloadLease {
    registry: Arc<Mutex<BTreeMap<usize, Weak<PayloadLease>>>>,
    key: usize,
    // Owning the allocation prevents pointer reuse while a lease is live.
    _bytes: Arc<[u8]>,
    _payload: Reservation,
    _metadata: Reservation,
}

impl Drop for PayloadLease {
    fn drop(&mut self) {
        if let Ok(mut registry) = self.registry.lock()
            && registry
                .get(&self.key)
                .is_some_and(|lease| std::ptr::eq(lease.as_ptr(), self))
        {
            registry.remove(&self.key);
        }
    }
}

pub(super) struct Budgets {
    pub native: MemoryBudget,
    pub metadata: Arc<Budget>,
    payload: Arc<Budget>,
    allocations: Arc<Mutex<BTreeMap<usize, Weak<PayloadLease>>>>,
}

impl Budgets {
    fn new(limits: Limits) -> Self {
        Self {
            native: MemoryBudget::new(limits.native),
            metadata: Budget::new(limits.metadata, "PAR3 host state"),
            payload: Budget::new(limits.payload, "PAR3 retained payload"),
            allocations: Arc::default(),
        }
    }

    /// Host-side bytes currently reserved across both weaver-owned budgets.
    /// The engine's own reservations are reported separately by its budget.
    pub fn host_used(&self) -> u64 {
        (self.metadata.used() as u64).saturating_add(self.payload.used() as u64)
    }

    pub fn retain(&self, bytes: &Arc<[u8]>) -> EngineResult<Arc<PayloadLease>> {
        let mut allocations = self
            .allocations
            .lock()
            .map_err(|_| EngineError::InvalidState("PAR3 allocation registry poisoned"))?;
        let key = bytes.as_ptr() as usize;
        if let Some(lease) = allocations.get(&key).and_then(Weak::upgrade) {
            return Ok(lease);
        }
        let metadata = self.metadata.acquire(256)?;
        let payload = self.payload.acquire(bytes.len())?;
        let lease = Arc::new(PayloadLease {
            registry: Arc::clone(&self.allocations),
            key,
            _bytes: Arc::clone(bytes),
            _payload: payload,
            _metadata: metadata,
        });
        allocations.insert(key, Arc::downgrade(&lease));
        Ok(lease)
    }
}

pub(super) fn budgets() -> &'static Budgets {
    static BUDGETS: OnceLock<Budgets> = OnceLock::new();
    BUDGETS.get_or_init(|| {
        let limits = Limits::for_memory(crate::runtime::system_probe::detect_total_memory_bytes());
        tracing::info!(
            native_bytes = limits.native,
            metadata_bytes = limits.metadata,
            payload_bytes = limits.payload,
            "PAR3 memory budgets initialized"
        );
        Budgets::new(limits)
    })
}

pub(super) fn is_native_pressure(error: &EngineError) -> bool {
    match error {
        EngineError::ResourceLimit("memory budget" | "minimum repair stripe" | "open handles") => {
            true
        }
        EngineError::RepairInterrupted { cause, .. } => is_native_pressure(cause),
        _ => false,
    }
}

pub(super) fn is_host_pressure(error: &EngineError) -> bool {
    match error {
        EngineError::ResourceLimit("PAR3 host state" | "PAR3 retained payload") => true,
        EngineError::Io(error) => {
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<EngineError>())
                .is_some_and(is_host_pressure)
                || pressure_source(error).is_some()
        }
        _ => false,
    }
}

#[derive(Debug)]
struct SourcePressure(SourceId, EngineError);

impl std::fmt::Display for SourcePressure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.1.fmt(f)
    }
}
impl std::error::Error for SourcePressure {}

pub(super) fn source_pressure(source: SourceId, error: EngineError) -> std::io::Error {
    std::io::Error::other(SourcePressure(source, error))
}

pub(super) fn pressure_source(error: &std::io::Error) -> Option<SourceId> {
    let inner = error.get_ref()?;
    if let Some(pressure) = inner.downcast_ref::<SourcePressure>() {
        return is_host_pressure(&pressure.1).then_some(pressure.0);
    }
    inner
        .downcast_ref::<EngineError>()
        .and_then(error_pressure_source)
}

/// Native repair wraps source errors after staging or installing output.
/// The caller must reconcile those installations before acting on the source.
pub(super) fn error_pressure_source(error: &EngineError) -> Option<SourceId> {
    match error {
        EngineError::Io(error) => pressure_source(error),
        EngineError::RepairInterrupted { cause, .. } => error_pressure_source(cause),
        _ => None,
    }
}

#[cfg(test)]
mod tests;

// Conservatively share outstanding spill claims across filesystems. These
// claims cover concurrent sweeps, before their writes appear in free space.
static DISK_RESERVED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub(in crate::pipeline) struct DiskReservation(u64);

impl DiskReservation {
    pub(super) fn acquire(bytes: u64, available: u64, reserve: u64) -> EngineResult<Self> {
        DISK_RESERVED
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                let total = used.checked_add(bytes)?;
                (total.checked_add(reserve)? <= available).then_some(total)
            })
            .map_err(|used| {
                tracing::warn!(
                    stage = "disk_admission",
                    requested_bytes = bytes,
                    reserved_bytes = used,
                    available_bytes = available,
                    reserve_bytes = reserve,
                    "PAR3 disk fallback admission refused"
                );
                EngineError::ResourceLimit("PAR3 disk fallback space")
            })?;
        Ok(Self(bytes))
    }
}

impl Drop for DiskReservation {
    fn drop(&mut self) {
        DISK_RESERVED.fetch_sub(self.0, Ordering::AcqRel);
    }
}
