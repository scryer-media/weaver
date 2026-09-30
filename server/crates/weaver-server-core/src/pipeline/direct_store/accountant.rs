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

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use super::router::DemotionReason;
use crate::operations::disk::{CapacityDebits, CapacityReader};

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

/// The working directory's latest free-space reading. Reading it never
/// touches the filesystem: the runtime's sampler refreshes it on its own
/// thread. No reading at all is treated as no reserve to enforce rather than
/// as an empty disk, since a probe failure must not demote a set that was
/// routing fine.
pub(crate) type DiskProbe = CapacityReader;

/// See the module documentation.
pub(crate) struct HoldsAccountant {
    limits: HoldsLimits,
    resident: AtomicU64,
    scratch: AtomicU64,
    /// Spills admitted against the current reading, so a burst of paging
    /// between refreshes cannot run ahead of the reserve on one number.
    disk: Mutex<CapacityDebits>,
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
    /// An accountant with no free-space reading: the reserve is not
    /// enforced until [`Self::with_probe`] gives it one.
    pub(crate) fn new(limits: HoldsLimits) -> Self {
        Self::with_probe(limits, CapacityReader::unknown())
    }

    /// An accountant that never refuses anything.
    pub(crate) fn unbounded() -> Self {
        Self::new(HoldsLimits::UNBOUNDED)
    }

    /// An accountant whose free-space reading is whatever `probe` reads:
    /// the working root's sampler in production, a fixed reading in tests.
    pub(crate) fn with_probe(limits: HoldsLimits, probe: DiskProbe) -> Self {
        Self {
            limits,
            resident: AtomicU64::new(0),
            scratch: AtomicU64::new(0),
            disk: Mutex::new(CapacityDebits::default()),
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

    /// Whether `bytes` more scratch may be written to the working directory:
    /// inside the shared scratch ceiling, and leaving the filesystem its
    /// reserve.
    ///
    /// Judged on the published totals, so a router calls this with its own
    /// scratch charge current. Admission spends the free-space estimate; a
    /// spill the caller then fails to make is reconciled at the next reading.
    pub(crate) fn admit_scratch(&self, bytes: u64) -> Result<(), DemotionReason> {
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
        let Some(reading) = disk.apply(self.probe.current()).reading() else {
            return Ok(());
        };
        // A stale reading is debited but never refuses: only a fresh reading
        // can confirm the reserve is really gone.
        if !reading.stale
            && reading.available_bytes < bytes.saturating_add(self.limits.disk_reserve_bytes)
        {
            return Err(DemotionReason::HoldsScratchDiskReserve);
        }
        disk.debit(bytes);
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
    use crate::operations::disk::{Capacity, CapacityReading};
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::time::{Duration, Instant};

    fn limits(resident: u64, scratch: u64, reserve: u64) -> HoldsLimits {
        HoldsLimits {
            resident_bytes: resident,
            scratch_bytes: scratch,
            disk_reserve_bytes: reserve,
        }
    }

    /// A reader of whatever `reading` holds, counting reads.
    fn probe_returning(reading: Arc<Mutex<Capacity>>, calls: Arc<AtomicUsize>) -> DiskProbe {
        CapacityReader::from_fn(move || {
            calls.fetch_add(1, Ordering::AcqRel);
            *reading.lock().unwrap()
        })
    }

    fn known(available_bytes: u64, sampled_at: Instant, stale: bool) -> Capacity {
        Capacity::Known(CapacityReading {
            available_bytes,
            total_bytes: u64::MAX,
            sampled_at,
            stale,
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

        assert_eq!(accountant.admit_scratch(30), Ok(()));
        assert_eq!(
            accountant.admit_scratch(31),
            Err(DemotionReason::HoldsScratchCeiling),
            "another set's scratch counts against this one's spill"
        );
        accountant.release(&mut other);
        assert_eq!(accountant.admit_scratch(100), Ok(()));
    }

    #[test]
    fn the_disk_reserve_refuses_a_spill_that_would_eat_into_it() {
        let free = Arc::new(Mutex::new(known(1000, Instant::now(), false)));
        let calls = Arc::new(AtomicUsize::new(0));
        let accountant = HoldsAccountant::with_probe(
            limits(u64::MAX, u64::MAX, 600),
            probe_returning(Arc::clone(&free), Arc::clone(&calls)),
        );

        // 1000 free, 600 reserved: 400 may be spent, and admissions against
        // one reading spend it rather than each seeing the same headroom.
        assert_eq!(accountant.admit_scratch(250), Ok(()));
        assert_eq!(accountant.admit_scratch(150), Ok(()));
        assert_eq!(
            accountant.admit_scratch(1),
            Err(DemotionReason::HoldsScratchDiskReserve)
        );
    }

    #[test]
    fn an_unreadable_filesystem_enforces_no_reserve() {
        let free = Arc::new(Mutex::new(Capacity::Unknown));
        let calls = Arc::new(AtomicUsize::new(0));
        let accountant = HoldsAccountant::with_probe(
            limits(u64::MAX, u64::MAX, 600),
            probe_returning(free, calls),
        );
        assert_eq!(
            accountant.admit_scratch(u64::MAX / 2),
            Ok(()),
            "a probe that cannot answer must not demote a set that was routing fine"
        );
    }

    #[test]
    fn a_probe_outage_holds_the_last_reading_without_refusing() {
        let taken = Instant::now();
        let free = Arc::new(Mutex::new(known(1000, taken, false)));
        let calls = Arc::new(AtomicUsize::new(0));
        let accountant = HoldsAccountant::with_probe(
            limits(u64::MAX, u64::MAX, 600),
            probe_returning(Arc::clone(&free), Arc::clone(&calls)),
        );
        assert_eq!(accountant.admit_scratch(300), Ok(()));

        // The filesystem stops answering and the sampler holds its last good
        // reading as stale: the 700 left on it is still debited, but a breach
        // on a stale number does not demote.
        *free.lock().unwrap() = known(1000, taken, true);
        assert_eq!(accountant.admit_scratch(500), Ok(()));
        assert_eq!(
            accountant
                .disk
                .lock()
                .unwrap()
                .apply(*free.lock().unwrap())
                .best_available_bytes(),
            Some(200)
        );

        // A fresh reading takes over and enforces again.
        *free.lock().unwrap() = known(650, taken + Duration::from_secs(5), false);
        assert_eq!(
            accountant.admit_scratch(100),
            Err(DemotionReason::HoldsScratchDiskReserve)
        );
        assert_eq!(calls.load(Ordering::Acquire), 3);
    }

    #[test]
    fn a_zero_reserve_never_probes() {
        let free = Arc::new(Mutex::new(known(0, Instant::now(), false)));
        let calls = Arc::new(AtomicUsize::new(0));
        let accountant = HoldsAccountant::with_probe(
            limits(u64::MAX, u64::MAX, 0),
            probe_returning(free, Arc::clone(&calls)),
        );
        assert_eq!(accountant.admit_scratch(1 << 40), Ok(()));
        assert_eq!(calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn unbounded_refuses_nothing() {
        let accountant = HoldsAccountant::unbounded();
        let mut charge = HoldsCharge::default();
        accountant.publish(&mut charge, u64::MAX / 2, u64::MAX / 2);
        assert!(!accountant.resident_over_limit());
        assert_eq!(accountant.admit_scratch(u64::MAX / 4), Ok(()));
    }
}
