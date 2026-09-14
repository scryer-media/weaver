//! Process-wide accounting of what direct-store holds cost the host.
//!
//! Every set bounds its own holds twice — a RAM budget that pages to scratch
//! and a scratch ceiling that demotes — and both are per set. Nothing summed
//! across sets, so a queue with many direct sets in flight multiplied both
//! numbers with no upper bound, and neither was derived from anything the box
//! actually has. This module is the sum and the host: one accountant per
//! pipeline, shared by every router it admits, charged with each set's live
//! resident and scratch bytes and consulted before a set spends more.
//!
//! The accountant never routes and never demotes on its own. It answers two
//! questions the router already asks of its own ceilings — *am I over RAM?*
//! and *may I write this much more scratch?* — with the process total in view,
//! and the router acts exactly as it does for its own limits: a RAM breach
//! pages, a scratch refusal demotes the set that asked. Which set pages under a
//! shared breach is the one routing at the time; a set that stops routing keeps
//! what it holds resident, up to its own budget, which is what keeps the policy
//! local to the router's existing seams rather than a scheduler over sets.
//!
//! The scratch question has a second half the per-set ceiling could not ask:
//! the working directory's free space. A scratch that fills the disk demotes
//! its set gracefully through the write-failed path, but only after starving
//! the downloads sharing that volume. The accountant keeps a reserve — the same
//! rule the extractor keeps for its own output — and refuses a spill that would
//! eat into it, before the write.

use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::router::DemotionReason;

/// How long a free-space reading stays authoritative before the accountant
/// asks the filesystem again. Between readings the estimate is decremented by
/// every spill it admits, so a burst of paging inside one interval cannot run
/// ahead of the reserve on a stale number.
const DISK_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// The process-wide ceilings, resolved once with the rest of the direct-store
/// settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HoldsLimits {
    /// RAM-resident holds across every set, in bytes. Over it, the routing set
    /// pages its own holds to scratch exactly as it does over its own budget.
    pub(crate) resident_bytes: u64,
    /// Holds scratch across every set, in bytes. A spill that would exceed it
    /// demotes the set that asked, after that set has compacted its own
    /// scratch and found it still does not fit.
    pub(crate) scratch_bytes: u64,
    /// Free space the working directory's filesystem must keep. A spill that
    /// would leave less demotes the set that asked. Zero disables the check.
    pub(crate) disk_reserve_bytes: u64,
}

impl HoldsLimits {
    /// No ceilings at all, for routers built by hand in tests and for a
    /// runtime that was never given settings.
    pub(crate) const UNBOUNDED: Self = Self {
        resident_bytes: u64::MAX,
        scratch_bytes: u64::MAX,
        disk_reserve_bytes: 0,
    };
}

/// One router's standing charge against the accountant: what it last
/// published, so the next publication is a delta rather than a re-count.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct HoldsCharge {
    resident: u64,
    scratch: u64,
}

/// Reads the free bytes on the filesystem backing a path. `None` when the
/// filesystem cannot say, which the accountant treats as no reserve to
/// enforce rather than as an empty disk: a probe failure must not demote a
/// set that was routing fine.
pub(crate) type DiskProbe = Box<dyn Fn(&Path) -> Option<u64> + Send + Sync>;

#[derive(Debug)]
struct DiskEstimate {
    refreshed: Option<Instant>,
    /// Free bytes at the last successful reading, less every spill admitted
    /// since. Held across probe failures so admissions keep being accounted.
    available: Option<u64>,
    /// The most recent probe failed; `available` is the last good reading.
    /// A stale reading is debited but never refuses, since only a fresh
    /// reading can confirm the reserve is really gone.
    stale: bool,
}

/// See the module documentation.
pub(crate) struct HoldsAccountant {
    limits: HoldsLimits,
    resident: AtomicU64,
    scratch: AtomicU64,
    disk: Mutex<DiskEstimate>,
    probe: DiskProbe,
}

impl std::fmt::Debug for HoldsAccountant {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HoldsAccountant")
            .field("limits", &self.limits)
            .field("resident", &self.resident.load(Ordering::Acquire))
            .field("scratch", &self.scratch.load(Ordering::Acquire))
            .finish()
    }
}

impl Default for HoldsAccountant {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl HoldsAccountant {
    /// An accountant over the real filesystem.
    pub(crate) fn new(limits: HoldsLimits) -> Self {
        Self::with_probe(
            limits,
            Box::new(|path| {
                // A missing working directory must not borrow capacity from
                // its parent. Keep the estimate stale until this path returns.
                match crate::operations::disk::probe_disk_space(path) {
                    Ok(space) => Some(space.available_bytes),
                    Err(error) => {
                        tracing::debug!(
                            path = %path.display(),
                            error = %error,
                            "holds scratch free-space reading unavailable"
                        );
                        None
                    }
                }
            }),
        )
    }

    /// An accountant that never refuses anything.
    pub(crate) fn unbounded() -> Self {
        Self::new(HoldsLimits::UNBOUNDED)
    }

    /// An accountant whose free-space reading is whatever `probe` says. The
    /// tests drive the reserve with it; production uses [`Self::new`].
    pub(crate) fn with_probe(limits: HoldsLimits, probe: DiskProbe) -> Self {
        Self {
            limits,
            resident: AtomicU64::new(0),
            scratch: AtomicU64::new(0),
            disk: Mutex::new(DiskEstimate {
                refreshed: None,
                available: None,
                stale: false,
            }),
            probe,
        }
    }

    /// RAM-resident holds across every set that has published.
    pub(crate) fn resident_bytes(&self) -> u64 {
        self.resident.load(Ordering::Acquire)
    }

    /// Scratch bytes across every set that has published.
    pub(crate) fn scratch_bytes(&self) -> u64 {
        self.scratch.load(Ordering::Acquire)
    }

    /// Whether the process total of resident holds is over the shared limit.
    /// The caller publishes first, so its own bytes are in the total.
    pub(crate) fn resident_over_limit(&self) -> bool {
        self.resident_bytes() > self.limits.resident_bytes
    }

    /// Replaces one router's charge with its current figures.
    pub(crate) fn publish(&self, charge: &mut HoldsCharge, resident: u64, scratch: u64) {
        adjust(&self.resident, charge.resident, resident);
        adjust(&self.scratch, charge.scratch, scratch);
        charge.resident = resident;
        charge.scratch = scratch;
    }

    /// Withdraws one router's charge entirely. A router dropping is the one
    /// caller: its bytes are gone with it, whatever it last published.
    pub(crate) fn release(&self, charge: &mut HoldsCharge) {
        self.publish(charge, 0, 0);
    }

    /// Whether `bytes` more scratch may be written under `dir`: inside the
    /// shared scratch ceiling, and leaving the filesystem its reserve.
    ///
    /// Judged on the published totals, so a router calls this with its own
    /// scratch charge current. Admission spends the free-space estimate; a
    /// spill the caller then fails to make is reconciled at the next reading.
    pub(crate) fn admit_scratch(&self, bytes: u64, dir: &Path) -> Result<(), DemotionReason> {
        if self.scratch_bytes().saturating_add(bytes) > self.limits.scratch_bytes {
            return Err(DemotionReason::HoldsScratchCeiling);
        }
        if self.limits.disk_reserve_bytes == 0 {
            return Ok(());
        }
        let mut disk = self
            .disk
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let stale = disk
            .refreshed
            .is_none_or(|refreshed| refreshed.elapsed() >= DISK_REFRESH_INTERVAL);
        if stale {
            match (self.probe)(dir) {
                Some(available) => {
                    disk.available = Some(available);
                    disk.stale = false;
                }
                None => disk.stale = true,
            }
            disk.refreshed = Some(Instant::now());
        }
        let Some(available) = disk.available else {
            return Ok(());
        };
        if !disk.stale && available < bytes.saturating_add(self.limits.disk_reserve_bytes) {
            return Err(DemotionReason::HoldsScratchDiskReserve);
        }
        disk.available = Some(available.saturating_sub(bytes));
        Ok(())
    }
}

fn adjust(total: &AtomicU64, from: u64, to: u64) {
    if to > from {
        total.fetch_add(to - from, Ordering::AcqRel);
    } else if from > to {
        total.fetch_sub(from - to, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    fn limits(resident: u64, scratch: u64, reserve: u64) -> HoldsLimits {
        HoldsLimits {
            resident_bytes: resident,
            scratch_bytes: scratch,
            disk_reserve_bytes: reserve,
        }
    }

    fn probe_returning(free: Arc<Mutex<Option<u64>>>, calls: Arc<AtomicUsize>) -> DiskProbe {
        Box::new(move |_| {
            calls.fetch_add(1, Ordering::AcqRel);
            *free.lock().unwrap()
        })
    }

    #[test]
    fn publishing_is_a_delta_and_release_takes_it_all_back() {
        let accountant = HoldsAccountant::new(limits(1000, 1000, 0));
        let mut first = HoldsCharge::default();
        let mut second = HoldsCharge::default();

        accountant.publish(&mut first, 300, 40);
        accountant.publish(&mut second, 500, 0);
        assert_eq!(accountant.resident_bytes(), 800);
        assert_eq!(accountant.scratch_bytes(), 40);
        assert!(!accountant.resident_over_limit());

        // Re-publishing moves the total by the difference, both ways.
        accountant.publish(&mut first, 100, 90);
        assert_eq!(accountant.resident_bytes(), 600);
        assert_eq!(accountant.scratch_bytes(), 90);
        accountant.publish(&mut second, 950, 0);
        assert!(accountant.resident_over_limit());

        accountant.release(&mut second);
        assert_eq!(accountant.resident_bytes(), 100);
        assert!(!accountant.resident_over_limit());
        accountant.release(&mut first);
        assert_eq!(accountant.resident_bytes(), 0);
        assert_eq!(accountant.scratch_bytes(), 0);
        // Releasing twice is a no-op, not an underflow.
        accountant.release(&mut first);
        assert_eq!(accountant.scratch_bytes(), 0);
    }

    #[test]
    fn scratch_admission_is_judged_on_the_shared_total() {
        let accountant = HoldsAccountant::new(limits(u64::MAX, 100, 0));
        let mut other = HoldsCharge::default();
        accountant.publish(&mut other, 0, 70);
        let dir = Path::new("/nonexistent");

        assert_eq!(accountant.admit_scratch(30, dir), Ok(()));
        assert_eq!(
            accountant.admit_scratch(31, dir),
            Err(DemotionReason::HoldsScratchCeiling),
            "another set's scratch counts against this one's spill"
        );
        accountant.release(&mut other);
        assert_eq!(accountant.admit_scratch(100, dir), Ok(()));
    }

    #[test]
    fn the_disk_reserve_refuses_a_spill_that_would_eat_into_it() {
        let free = Arc::new(Mutex::new(Some(1000u64)));
        let calls = Arc::new(AtomicUsize::new(0));
        let accountant = HoldsAccountant::with_probe(
            limits(u64::MAX, u64::MAX, 600),
            probe_returning(Arc::clone(&free), Arc::clone(&calls)),
        );
        let dir = Path::new("/nonexistent");

        // 1000 free, 600 reserved: 400 may be spent, and admissions inside one
        // refresh interval spend the same reading rather than re-asking.
        assert_eq!(accountant.admit_scratch(250, dir), Ok(()));
        assert_eq!(accountant.admit_scratch(150, dir), Ok(()));
        assert_eq!(
            accountant.admit_scratch(1, dir),
            Err(DemotionReason::HoldsScratchDiskReserve)
        );
        assert_eq!(
            calls.load(Ordering::Acquire),
            1,
            "one reading serves every admission inside the interval"
        );
    }

    #[test]
    fn an_unreadable_filesystem_enforces_no_reserve() {
        let free = Arc::new(Mutex::new(None));
        let calls = Arc::new(AtomicUsize::new(0));
        let accountant = HoldsAccountant::with_probe(
            limits(u64::MAX, u64::MAX, 600),
            probe_returning(free, calls),
        );
        assert_eq!(
            accountant.admit_scratch(u64::MAX / 2, Path::new("/nonexistent")),
            Ok(()),
            "a probe that cannot answer must not demote a set that was routing fine"
        );
    }

    #[test]
    fn a_probe_outage_holds_the_last_reading_without_refusing() {
        let free = Arc::new(Mutex::new(Some(1000u64)));
        let calls = Arc::new(AtomicUsize::new(0));
        let accountant = HoldsAccountant::with_probe(
            limits(u64::MAX, u64::MAX, 600),
            probe_returning(Arc::clone(&free), Arc::clone(&calls)),
        );
        let dir = Path::new("/nonexistent");
        assert_eq!(accountant.admit_scratch(300, dir), Ok(()));

        // The filesystem stops answering: the 700 left on the last reading is
        // still debited, but a breach on a stale number does not demote.
        *free.lock().unwrap() = None;
        {
            let mut disk = accountant.disk.lock().unwrap();
            disk.refreshed = Some(Instant::now() - DISK_REFRESH_INTERVAL);
        }
        assert_eq!(accountant.admit_scratch(500, dir), Ok(()));
        assert_eq!(accountant.disk.lock().unwrap().available, Some(200));
        assert!(accountant.disk.lock().unwrap().stale);

        // A fresh reading takes over and enforces again.
        *free.lock().unwrap() = Some(650);
        {
            let mut disk = accountant.disk.lock().unwrap();
            disk.refreshed = Some(Instant::now() - DISK_REFRESH_INTERVAL);
        }
        assert_eq!(
            accountant.admit_scratch(100, dir),
            Err(DemotionReason::HoldsScratchDiskReserve)
        );
        assert_eq!(calls.load(Ordering::Acquire), 3);
    }

    #[test]
    fn a_zero_reserve_never_probes() {
        let free = Arc::new(Mutex::new(Some(0u64)));
        let calls = Arc::new(AtomicUsize::new(0));
        let accountant = HoldsAccountant::with_probe(
            limits(u64::MAX, u64::MAX, 0),
            probe_returning(free, Arc::clone(&calls)),
        );
        assert_eq!(
            accountant.admit_scratch(1 << 40, Path::new("/nonexistent")),
            Ok(())
        );
        assert_eq!(calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn unbounded_refuses_nothing() {
        let accountant = HoldsAccountant::unbounded();
        let mut charge = HoldsCharge::default();
        accountant.publish(&mut charge, u64::MAX / 2, u64::MAX / 2);
        assert!(!accountant.resident_over_limit());
        assert_eq!(
            accountant.admit_scratch(u64::MAX / 4, Path::new("/nonexistent")),
            Ok(())
        );
    }
}
