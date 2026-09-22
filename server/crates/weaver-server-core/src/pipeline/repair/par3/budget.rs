//! Process-wide PAR3 admission, resolved once outside the download loop.

use super::*;
use par3_rs::runtime::{MemoryBudget, ResourceLimit};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, Weak};

/// A ceiling weaver's own admission enforced, in the vocabulary the engine
/// refuses in.
///
/// The engine's `ResourceLimit` is `#[non_exhaustive]` and both its
/// constructors are crate-private, so only par3-rs can build one. Weaver's own
/// budgets — carrier counts, retained payload, assessment views, disk fallback
/// — still have to refuse in the same terms, so they carry the same four fields
/// and travel inside the engine's error type the way [`SourcePressure`] below
/// already does. Everything that reads a refusal goes through [`limit_label`]
/// and sees both kinds alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) struct HostLimit {
    /// Name of the budget that refused, in the same style as the engine's.
    pub what: &'static str,
    /// Bytes the refused request needed, when the refusal was measured.
    pub need: u64,
    /// Configured ceiling for `what`, when the refusal was measured.
    pub limit: u64,
    /// Bytes still available under that ceiling when the request was refused.
    pub available: u64,
}

impl std::fmt::Display for HostLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.limit == 0 {
            f.write_str(self.what)
        } else if self.need > self.limit {
            write!(
                f,
                "{} does not fit alone (needs {} bytes, ceiling {})",
                self.what, self.need, self.limit
            )
        } else {
            write!(
                f,
                "{} does not fit beside the memory already reserved (needs {} bytes, {} of {} \
                 available)",
                self.what, self.need, self.available, self.limit
            )
        }
    }
}

impl std::error::Error for HostLimit {}

/// A weaver-side refusal whose ceiling is a count or a structural bound rather
/// than a byte budget.
pub(in crate::pipeline) fn host_limit(what: &'static str) -> EngineError {
    EngineError::Io(std::io::Error::other(HostLimit {
        what,
        need: 0,
        limit: 0,
        available: 0,
    }))
}

/// A weaver-side refusal measured against a byte ceiling.
pub(in crate::pipeline) fn host_budget_limit(
    what: &'static str,
    need: u64,
    limit: u64,
    available: u64,
) -> EngineError {
    EngineError::Io(std::io::Error::other(HostLimit {
        what,
        need,
        limit,
        available,
    }))
}

/// The budget a refusal names, whether the engine or weaver refused. Anything
/// that is not a refusal answers `None`.
pub(in crate::pipeline) fn limit_label(error: &EngineError) -> Option<&'static str> {
    match error {
        EngineError::ResourceLimit(limit) => Some(limit.what),
        EngineError::Io(error) => error.get_ref().and_then(|inner| {
            inner
                .downcast_ref::<HostLimit>()
                .map(|limit| limit.what)
                .or_else(|| inner.downcast_ref::<EngineError>().and_then(limit_label))
        }),
        EngineError::RepairInterrupted { cause, .. } => limit_label(cause),
        _ => None,
    }
}

/// Whether this error is a refused reservation at all.
pub(in crate::pipeline) fn is_limit(error: &EngineError) -> bool {
    limit_label(error).is_some()
}

/// The engine's own measured refusal, when the engine is the one that refused.
/// A weaver-side [`HostLimit`] is deliberately not reported here: the outcome
/// that depends on `LimitCause` is about the engine's native budget.
pub(in crate::pipeline) fn engine_limit(error: &EngineError) -> Option<ResourceLimit> {
    match error {
        EngineError::ResourceLimit(limit) => Some(*limit),
        EngineError::Io(error) => error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<EngineError>())
            .and_then(engine_limit),
        EngineError::RepairInterrupted { cause, .. } => engine_limit(cause),
        _ => None,
    }
}

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
                host_budget_limit(
                    self.label,
                    bytes as u64,
                    self.limit as u64,
                    self.limit.saturating_sub(used) as u64,
                )
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
    _bytes: bytes::Bytes,
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

    pub fn retain(&self, bytes: &bytes::Bytes) -> EngineResult<Arc<PayloadLease>> {
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
            _bytes: bytes.clone(),
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

/// Whether the engine's own native budget is what refused.
///
/// A charge against that budget now names the memory category it pays for, so
/// the label is one of the engine's category names rather than the single
/// `memory budget` string every charge used to report. `Uncategorized` still
/// carries that string, so nothing that refused before stops being recognised;
/// the two labels beside the categories are refusals the budget raises without
/// charging a category.
pub(super) fn is_native_pressure(error: &EngineError) -> bool {
    limit_label(error).is_some_and(|label| {
        matches!(label, "minimum repair stripe" | "open handles")
            || par3_rs::runtime::MemoryCategory::ALL
                .iter()
                .any(|category| category.name() == label)
    })
}

pub(super) fn is_host_pressure(error: &EngineError) -> bool {
    matches!(
        limit_label(error),
        Some("PAR3 host state" | "PAR3 retained payload")
    ) || matches!(error, EngineError::Io(error) if pressure_source(error).is_some())
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

/// A real engine refusal of the requested cause, for tests that have to hand a
/// `ResourceLimit` to code that classifies one.
///
/// par3-rs is the only crate that can build one: the struct is
/// `#[non_exhaustive]` and both its constructors are crate-private. Rather than
/// model what a refusal would look like, this provokes the engine into
/// producing one through its published APIs, so the numbers and the `cause()`
/// are the engine's own.
#[cfg(test)]
pub(in crate::pipeline) fn engine_refusal(cause: par3_rs::runtime::LimitCause) -> ResourceLimit {
    use par3_rs::runtime::{ExecutionOptions, HandleBudget, LimitCause};

    let limit_of = |error: EngineError| {
        engine_limit(&error).unwrap_or_else(|| panic!("the engine refused with {error}"))
    };
    if cause == LimitCause::Unmeasured {
        // A handle ceiling is a count, so its refusal is never measured.
        let handles = HandleBudget::new(1);
        let _held = handles.acquire().expect("the first handle fits");
        return limit_of(handles.acquire().expect_err("the second handle does not"));
    }
    // A carrier scanner reserves two stripes before it reads anything, so a
    // budget sized against that reservation refuses on demand.
    let scanner = |options: &ExecutionOptions| {
        let mut access = par3_rs::source::MemorySourceAccess::default();
        access.insert(SourceId(1), 1, std::sync::Arc::from(&b"unscanned"[..]));
        par3_rs::ingest::PacketScanner::new(
            std::sync::Arc::new(access),
            SourceId(1),
            options.clone(),
            par3_rs::ScanLimits::default(),
        )
    };
    let stripe = 16 << 10;
    let mut options = ExecutionOptions::default();
    options.stripe_bytes = stripe;
    options.memory = MemoryBudget::new(if cause == LimitCause::ExceedsLimit {
        // Less than one scanner's two stripes: nothing frees enough.
        stripe
    } else {
        // Room for one scanner, not two: the second waits on the first.
        3 * stripe
    });
    let held =
        (cause == LimitCause::PeerContention).then(|| scanner(&options).expect("first fits"));
    let refusal = limit_of(scanner(&options).err().expect("the budget refuses"));
    drop(held);
    assert_eq!(refusal.cause(), cause, "{refusal}");
    refusal
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
                host_budget_limit(
                    "PAR3 disk fallback space",
                    bytes.saturating_add(reserve),
                    available,
                    available.saturating_sub(used).saturating_sub(reserve),
                )
            })?;
        Ok(Self(bytes))
    }
}

impl Drop for DiskReservation {
    fn drop(&mut self) {
        DISK_RESERVED.fetch_sub(self.0, Ordering::AcqRel);
    }
}
