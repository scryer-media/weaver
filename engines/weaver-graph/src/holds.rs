// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Holds are bytes that arrived before the headers that say where they go.
// They wait in RAM, and on a namespace's scratch file when RAM runs short.
//
// Two types share the work. The `HoldsAccountant` is one per process and
// only counts: it never sees a byte, so every namespace can ask it under one
// short lock. The `HoldLedger` is one per namespace and knows where each
// held range lives. Neither touches the filesystem: the ledger says what to
// write and where, and the caller reports free space, so both stay
// deterministic and testable.
//
// A breach is never a pause. It is a decision handed to the caller, which
// refuses the member to a leaf (`Why::HoldsBreached`) and keeps downloading.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;

use crate::tree::NodeId;
use crate::volume::{ByteRange, VolumeIndex};

// Which namespace a hold belongs to, unique across the process. A node id is
// only unique within its tree and the accountant spans every job, so the
// caller names the tree too: `scope` is the job's own process-wide id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HoldsNamespace {
    pub scope: u64,
    pub node: NodeId,
}

// The four ceilings, in bytes, with the meanings the settings already give
// them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HoldBudgets {
    // Process-wide ceiling on bytes held in RAM across every namespace.
    pub resident_limit: u64,
    // Process-wide ceiling on bytes paged to scratch across every namespace.
    pub scratch_total: u64,
    // Per-namespace ceiling on bytes paged to scratch.
    pub scratch_ceiling: u64,
    // Bytes that must stay free on the scratch volume. Zero disables the
    // check, as the setting documents.
    pub disk_reserve: u64,
}

// The ceiling a reservation would have crossed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Budget {
    ResidentLimit,
    ScratchTotal,
    ScratchCeiling,
    DiskReserve,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reserve {
    Granted,
    Breached(Budget),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NamespaceUsage {
    pub resident: u64,
    pub scratch: u64,
}

impl NamespaceUsage {
    fn is_empty(&self) -> bool {
        self.resident == 0 && self.scratch == 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HoldsUsage {
    pub resident: u64,
    pub scratch: u64,
    // Namespaces with anything accounted. An idle namespace is forgotten, so
    // the map never outgrows the work in flight.
    pub namespaces: usize,
}

#[derive(Debug)]
struct Accounts {
    budgets: HoldBudgets,
    resident: u64,
    scratch: u64,
    namespaces: HashMap<HoldsNamespace, NamespaceUsage>,
}

impl Accounts {
    fn usage(&self, namespace: HoldsNamespace) -> NamespaceUsage {
        self.namespaces.get(&namespace).copied().unwrap_or_default()
    }

    fn store(&mut self, namespace: HoldsNamespace, usage: NamespaceUsage) {
        if usage.is_empty() {
            self.namespaces.remove(&namespace);
        } else {
            self.namespaces.insert(namespace, usage);
        }
    }

    // The scratch checks shared by a page-out and a direct spill. The
    // namespace's own ceiling is named first because it is the breach the
    // caller can pin on this one set; the process-wide ones follow.
    fn check_scratch(&self, namespace: HoldsNamespace, len: u64, free_disk: u64) -> Reserve {
        let budgets = self.budgets;
        let within = |held: u64, limit: u64| held.checked_add(len).is_some_and(|sum| sum <= limit);
        if !within(self.usage(namespace).scratch, budgets.scratch_ceiling) {
            return Reserve::Breached(Budget::ScratchCeiling);
        }
        if !within(self.scratch, budgets.scratch_total) {
            return Reserve::Breached(Budget::ScratchTotal);
        }
        if budgets.disk_reserve != 0 && !within(budgets.disk_reserve, free_disk) {
            return Reserve::Breached(Budget::DiskReserve);
        }
        Reserve::Granted
    }
}

// Subtract a release from a counter. Releasing more than was accounted is a
// bug in the caller, never input, and silently clamping it would let the
// budgets drift until they meant nothing.
fn released(held: u64, len: u64, what: &str) -> u64 {
    match held.checked_sub(len) {
        Some(left) => left,
        None => panic!("released {len} {what} bytes with only {held} accounted"),
    }
}

#[derive(Debug)]
pub struct HoldsAccountant {
    accounts: Mutex<Accounts>,
}

impl HoldsAccountant {
    pub fn new(budgets: HoldBudgets) -> Self {
        Self {
            accounts: Mutex::new(Accounts {
                budgets,
                resident: 0,
                scratch: 0,
                namespaces: HashMap::new(),
            }),
        }
    }

    // Every mutation checks before it writes, so a panic under the lock
    // leaves the counters as they were and a poisoned lock is still sound.
    fn lock(&self) -> MutexGuard<'_, Accounts> {
        self.accounts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn budgets(&self) -> HoldBudgets {
        self.lock().budgets
    }

    // New budgets judge the next reservation. Nothing already held is evicted
    // when a ceiling falls below it: the holds drain as their headers come.
    #[cfg(test)]
    pub fn set_budgets(&self, budgets: HoldBudgets) {
        self.lock().budgets = budgets;
    }

    pub fn usage(&self) -> HoldsUsage {
        let accounts = self.lock();
        HoldsUsage {
            resident: accounts.resident,
            scratch: accounts.scratch,
            namespaces: accounts.namespaces.len(),
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn namespace_usage(&self, namespace: HoldsNamespace) -> NamespaceUsage {
        self.lock().usage(namespace)
    }

    // Room `len` new bytes of RAM, or say which ceiling they would cross.
    pub fn reserve_resident(&self, namespace: HoldsNamespace, len: u64) -> Reserve {
        let mut accounts = self.lock();
        let Some(resident) = accounts
            .resident
            .checked_add(len)
            .filter(|&sum| sum <= accounts.budgets.resident_limit)
        else {
            return Reserve::Breached(Budget::ResidentLimit);
        };
        let mut usage = accounts.usage(namespace);
        usage.resident += len;
        accounts.resident = resident;
        accounts.store(namespace, usage);
        Reserve::Granted
    }

    // Move `len` resident bytes of this namespace to scratch. The bytes stay
    // accounted throughout: on a breach they are still resident.
    pub fn page_out(&self, namespace: HoldsNamespace, len: u64, free_disk: u64) -> Reserve {
        let mut accounts = self.lock();
        let check = accounts.check_scratch(namespace, len, free_disk);
        if check != Reserve::Granted {
            return check;
        }
        let mut usage = accounts.usage(namespace);
        usage.resident = released(usage.resident, len, "resident");
        usage.scratch += len;
        accounts.resident = released(accounts.resident, len, "resident");
        accounts.scratch += len;
        accounts.store(namespace, usage);
        Reserve::Granted
    }

    // Room `len` new bytes straight on scratch, for bytes RAM cannot take
    // even after this namespace has paged out everything it holds.
    pub fn reserve_scratch(&self, namespace: HoldsNamespace, len: u64, free_disk: u64) -> Reserve {
        let mut accounts = self.lock();
        let check = accounts.check_scratch(namespace, len, free_disk);
        if check != Reserve::Granted {
            return check;
        }
        let mut usage = accounts.usage(namespace);
        usage.scratch += len;
        accounts.scratch += len;
        accounts.store(namespace, usage);
        Reserve::Granted
    }

    pub fn release_resident(&self, namespace: HoldsNamespace, len: u64) {
        let mut accounts = self.lock();
        let mut usage = accounts.usage(namespace);
        let total = released(accounts.resident, len, "resident");
        usage.resident = released(usage.resident, len, "resident");
        accounts.resident = total;
        accounts.store(namespace, usage);
    }

    pub fn release_scratch(&self, namespace: HoldsNamespace, len: u64) {
        let mut accounts = self.lock();
        let mut usage = accounts.usage(namespace);
        let total = released(accounts.scratch, len, "scratch");
        usage.scratch = released(usage.scratch, len, "scratch");
        accounts.scratch = total;
        accounts.store(namespace, usage);
    }

    // The RAM this namespace could have if it paged out everything it holds:
    // what other namespaces hold is not its to free.
    fn resident_room_if_paged(&self, namespace: HoldsNamespace) -> u64 {
        let accounts = self.lock();
        let others = accounts.resident - accounts.usage(namespace).resident;
        accounts.budgets.resident_limit.saturating_sub(others)
    }
}

// Where one held range lives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Location {
    Ram(Bytes),
    // A span of this namespace's scratch file.
    Scratch { offset: u64, len: u64 },
}

// Resident bytes the caller must write to the scratch file at
// `scratch_offset`. The ledger already records them there, so the caller
// writes before it reads anything back from that span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageOut {
    pub volume: VolumeIndex,
    pub range: ByteRange,
    pub scratch_offset: u64,
    pub bytes: Bytes,
}

// A hold on scratch: where it is in its volume and where its bytes start in
// the namespace's scratch file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScratchHold {
    pub volume: VolumeIndex,
    pub range: ByteRange,
    pub scratch_offset: u64,
}

// Where the bytes of a `hold` went.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placed {
    Ram,
    // Straight to scratch; their write is the last `PageOut`.
    Scratch,
    // Not held. Page-outs made before the breach stand and must still be
    // written; whatever the new bytes overlapped is no longer held either,
    // since a re-hold replaces it.
    Breached(Budget),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HoldOutcome {
    pub placed: Placed,
    pub page_outs: Vec<PageOut>,
}

// A held range released because it now has a home. A scratch location is
// the caller's to read back; its accounting is already released.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Taken {
    pub range: ByteRange,
    pub location: Location,
}

#[derive(Clone, Debug)]
struct Hold {
    range: ByteRange,
    location: Location,
    // When it was held. A piece cut from a hold keeps its age, so a split
    // never makes old bytes look new.
    age: u64,
}

impl Hold {
    fn is_resident(&self) -> bool {
        matches!(self.location, Location::Ram(_))
    }

    // The location of `piece`, which lies inside this hold.
    fn slice(&self, piece: ByteRange) -> Location {
        let skip = piece.start - self.range.start;
        match &self.location {
            Location::Ram(bytes) => {
                Location::Ram(bytes.slice(index(skip)..index(skip + piece.len())))
            }
            Location::Scratch { offset, .. } => Location::Scratch {
                offset: offset + skip,
                len: piece.len(),
            },
        }
    }

    // The location of a piece this hold keeps after a cut. RAM is copied out
    // so the cut part's buffer is really freed when its taker drops it;
    // otherwise the remainder would pin bytes the accountant has released.
    fn keep(&self, piece: ByteRange) -> Location {
        match self.slice(piece) {
            Location::Ram(bytes) => Location::Ram(Bytes::copy_from_slice(&bytes)),
            scratch @ Location::Scratch { .. } => scratch,
        }
    }
}

// Weaver is 64-bit only, so a held length always fits a `usize`; failing
// here means a range and its bytes disagree, which is a bug.
fn index(offset: u64) -> usize {
    usize::try_from(offset).expect("held offset exceeds the address space")
}

fn byte_len(bytes: &Bytes) -> u64 {
    u64::try_from(bytes.len()).expect("held length exceeds u64")
}

// The holds of one namespace.
//
// Scratch space is a bump allocator: offsets grow and are not reused while
// anything lives on scratch, and start again at zero once nothing does. The
// file can therefore be larger than what is live; the accountant counts the
// live bytes, and the disk reserve sees the real file through the free space
// the caller reports.
// It shares the accountant it reports to, so dropping it releases whatever
// it still holds: a namespace that goes away without a `discard` must not
// leave its bytes counted against every other job.
#[derive(Debug)]
pub struct HoldLedger {
    namespace: HoldsNamespace,
    accountant: Arc<HoldsAccountant>,
    volumes: BTreeMap<VolumeIndex, BTreeMap<u64, Hold>>,
    // Resident holds oldest first, as (age, volume, start), which is the
    // order they page out in.
    resident_by_age: BTreeSet<(u64, VolumeIndex, u64)>,
    next_age: u64,
    scratch_end: u64,
    resident: u64,
    scratch: u64,
}

impl HoldLedger {
    pub fn new(namespace: HoldsNamespace, accountant: Arc<HoldsAccountant>) -> Self {
        Self {
            namespace,
            accountant,
            volumes: BTreeMap::new(),
            resident_by_age: BTreeSet::new(),
            next_age: 0,
            scratch_end: 0,
            resident: 0,
            scratch: 0,
        }
    }

    pub fn namespace(&self) -> HoldsNamespace {
        self.namespace
    }

    // Source-local identities at a worker transition. Cuts and page-outs
    // retain their ages; replacement bytes always get a new age.
    pub fn revisions(&self, volume: VolumeIndex, range: ByteRange) -> Vec<(ByteRange, u64)> {
        self.volumes
            .get(&volume)
            .map(|holds| {
                holds
                    .values()
                    .filter_map(|hold| Some((hold.range.intersect(range)?, hold.age)))
                    .collect()
            })
            .unwrap_or_default()
    }

    // Hold `bytes` of `volume` at `offset`, replacing any hold they overlap.
    //
    // RAM first. When RAM is short, this namespace's oldest resident holds
    // page out one at a time until the new bytes fit: the oldest have waited
    // longest for headers that have not come, so they are the least likely
    // to be claimed next. When even paging everything would not make room,
    // nothing is paged and the new bytes go straight to scratch, keeping the
    // resident holds where they are fastest to place.
    pub fn hold(
        &mut self,
        volume: VolumeIndex,
        offset: u64,
        bytes: Bytes,
        free_disk: u64,
    ) -> HoldOutcome {
        let accountant = Arc::clone(&self.accountant);
        let len = byte_len(&bytes);
        let range = ByteRange::at(offset, len);
        let mut page_outs = Vec::new();
        if range.is_empty() {
            return HoldOutcome {
                placed: Placed::Ram,
                page_outs,
            };
        }
        self.cut(volume, range);

        // Each page-out in this call spends free space the caller counted
        // before any of them was written.
        let mut free_disk = free_disk;
        if len <= accountant.resident_room_if_paged(self.namespace) {
            loop {
                if accountant.reserve_resident(self.namespace, len) == Reserve::Granted {
                    self.insert(volume, range, Location::Ram(bytes));
                    return HoldOutcome {
                        placed: Placed::Ram,
                        page_outs,
                    };
                }
                match self.page_out_oldest(free_disk) {
                    Some(Ok(page_out)) => {
                        free_disk = free_disk.saturating_sub(page_out.range.len());
                        page_outs.push(page_out);
                    }
                    Some(Err(budget)) => {
                        return HoldOutcome {
                            placed: Placed::Breached(budget),
                            page_outs,
                        };
                    }
                    // Another namespace took the room meanwhile.
                    None => break,
                }
            }
        }

        match accountant.reserve_scratch(self.namespace, len, free_disk) {
            Reserve::Granted => {
                let scratch_offset = self.allocate(len);
                self.insert(
                    volume,
                    range,
                    Location::Scratch {
                        offset: scratch_offset,
                        len,
                    },
                );
                page_outs.push(PageOut {
                    volume,
                    range,
                    scratch_offset,
                    bytes,
                });
                HoldOutcome {
                    placed: Placed::Scratch,
                    page_outs,
                }
            }
            Reserve::Breached(budget) => HoldOutcome {
                placed: Placed::Breached(budget),
                page_outs,
            },
        }
    }

    // Release the held parts of `range`, which now have a home, in offset
    // order. Parts of a hold outside `range` stay held.
    pub fn take(&mut self, volume: VolumeIndex, range: ByteRange) -> Vec<Taken> {
        self.cut(volume, range)
    }

    // The held parts of `range`, in offset order, left held: a repair reads
    // bytes before any object has given them a home.
    pub fn peek(&self, volume: VolumeIndex, range: ByteRange) -> Vec<Taken> {
        if range.is_empty() {
            return Vec::new();
        }
        self.volumes
            .get(&volume)
            .map(|holds| {
                let start = holds
                    .range(..=range.start)
                    .next_back()
                    .filter(|(_, hold)| hold.range.end > range.start)
                    .map_or(range.start, |(&start, _)| start);
                holds
                    .range(start..range.end)
                    .filter_map(|(_, hold)| {
                        let inside = hold.range.intersect(range)?;
                        Some(Taken {
                            range: inside,
                            location: hold.slice(inside),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    // Drop every hold, for a namespace that is going away. The caller
    // removes the scratch file.
    pub fn discard(&mut self) {
        self.accountant
            .release_resident(self.namespace, self.resident);
        self.accountant
            .release_scratch(self.namespace, self.scratch);
        self.volumes.clear();
        self.resident_by_age.clear();
        self.resident = 0;
        self.scratch = 0;
        self.scratch_end = 0;
    }

    // The held ranges of `volume`, in offset order. Neighbouring holds are
    // not merged, since each has its own location.
    pub fn held_ranges(&self, volume: VolumeIndex) -> Vec<ByteRange> {
        self.volumes
            .get(&volume)
            .map(|holds| holds.values().map(|hold| hold.range).collect())
            .unwrap_or_default()
    }

    // The first volume after `after` that holds anything, in volume order,
    // or the first of them when `after` is none.
    pub fn next_held_volume(&self, after: Option<VolumeIndex>) -> Option<VolumeIndex> {
        let from = after.map_or(Bound::Unbounded, Bound::Excluded);
        self.volumes
            .range((from, Bound::Unbounded))
            .find(|(_, holds)| !holds.is_empty())
            .map(|(volume, _)| *volume)
    }

    pub fn total_held(&self) -> u64 {
        self.resident + self.scratch
    }

    pub fn resident_bytes(&self) -> u64 {
        self.resident
    }

    pub fn scratch_bytes(&self) -> u64 {
        self.scratch
    }

    // Where the next scratch allocation starts, which is how long the
    // scratch file needs to be.
    #[cfg(test)]
    pub fn scratch_extent(&self) -> u64 {
        self.scratch_end
    }

    // Every hold on scratch, in volume and offset order: what a checkpoint
    // records, since the scratch file outlives the process and RAM does not.
    pub fn scratch_holds(&self) -> Vec<ScratchHold> {
        self.volumes
            .iter()
            .flat_map(|(volume, holds)| {
                holds.values().filter_map(move |hold| match hold.location {
                    Location::Scratch { offset, .. } => Some(ScratchHold {
                        volume: *volume,
                        range: hold.range,
                        scratch_offset: offset,
                    }),
                    Location::Ram(_) => None,
                })
            })
            .collect()
    }

    // Page every resident hold to scratch, oldest first, under the same
    // budgets as any page-out: a graceful stop keeps what scratch can take.
    // The first breach stops it, and what is left stays resident.
    pub fn page_out_all(&mut self, free_disk: u64) -> Vec<PageOut> {
        let mut free_disk = free_disk;
        let mut page_outs = Vec::new();
        while let Some(Ok(page_out)) = self.page_out_oldest(free_disk) {
            free_disk = free_disk.saturating_sub(page_out.range.len());
            page_outs.push(page_out);
        }
        page_outs
    }

    // A hold a checkpoint recorded on scratch, put back where it was. The
    // scratch file is already written, so nothing is paged; the bytes are
    // accounted as any spill is, and on a breach they are not held.
    pub fn restore(&mut self, held: ScratchHold, free_disk: u64) -> Reserve {
        let len = held.range.len();
        if len == 0 {
            return Reserve::Granted;
        }
        self.cut(held.volume, held.range);
        let reserve = self
            .accountant
            .reserve_scratch(self.namespace, len, free_disk);
        if reserve == Reserve::Granted {
            self.insert(
                held.volume,
                held.range,
                Location::Scratch {
                    offset: held.scratch_offset,
                    len,
                },
            );
            self.scratch_end = self.scratch_end.max(held.scratch_offset + len);
        }
        reserve
    }

    fn allocate(&mut self, len: u64) -> u64 {
        let offset = self.scratch_end;
        self.scratch_end += len;
        offset
    }

    fn insert(&mut self, volume: VolumeIndex, range: ByteRange, location: Location) {
        let age = self.next_age;
        self.next_age += 1;
        self.place(
            volume,
            Hold {
                range,
                location,
                age,
            },
        );
    }

    fn place(&mut self, volume: VolumeIndex, hold: Hold) {
        let len = hold.range.len();
        if hold.is_resident() {
            self.resident += len;
            self.resident_by_age
                .insert((hold.age, volume, hold.range.start));
        } else {
            self.scratch += len;
        }
        self.volumes
            .entry(volume)
            .or_default()
            .insert(hold.range.start, hold);
    }

    // Take a hold out of the maps and the ledger's own totals. Accounting
    // with the accountant is the caller's, since a cut and a page-out settle
    // it differently.
    fn unplace(&mut self, volume: VolumeIndex, start: u64) -> Option<Hold> {
        let holds = self.volumes.get_mut(&volume)?;
        let hold = holds.remove(&start)?;
        if holds.is_empty() {
            self.volumes.remove(&volume);
        }
        let len = hold.range.len();
        if hold.is_resident() {
            self.resident -= len;
            self.resident_by_age.remove(&(hold.age, volume, start));
        } else {
            self.scratch -= len;
        }
        Some(hold)
    }

    // Remove `range` from every hold it overlaps, releasing the accounting
    // of the removed parts and keeping the parts outside it.
    fn cut(&mut self, volume: VolumeIndex, range: ByteRange) -> Vec<Taken> {
        let Some(holds) = self.volumes.get(&volume) else {
            return Vec::new();
        };
        let touched: Vec<u64> = holds
            .range(..range.end)
            .filter(|(_, hold)| hold.range.intersect(range).is_some())
            .map(|(&start, _)| start)
            .collect();
        let mut taken = Vec::with_capacity(touched.len());
        for start in touched {
            let Some(hold) = self.unplace(volume, start) else {
                continue;
            };
            let Some(inside) = hold.range.intersect(range) else {
                continue;
            };
            if hold.is_resident() {
                self.accountant
                    .release_resident(self.namespace, inside.len());
            } else {
                self.accountant
                    .release_scratch(self.namespace, inside.len());
            }
            let left = ByteRange::new(hold.range.start, inside.start);
            let right = ByteRange::new(inside.end, hold.range.end);
            for piece in [left, right] {
                if !piece.is_empty() {
                    let location = hold.keep(piece);
                    self.place(
                        volume,
                        Hold {
                            range: piece,
                            location,
                            age: hold.age,
                        },
                    );
                }
            }
            taken.push(Taken {
                range: inside,
                location: hold.slice(inside),
            });
        }
        if self.scratch == 0 {
            self.scratch_end = 0;
        }
        taken
    }

    // Page this namespace's oldest resident hold to scratch. `None` when
    // nothing here is resident.
    fn page_out_oldest(&mut self, free_disk: u64) -> Option<Result<PageOut, Budget>> {
        let &(_, volume, start) = self.resident_by_age.first()?;
        let len = self.volumes.get(&volume)?.get(&start)?.range.len();
        if let Reserve::Breached(budget) = self.accountant.page_out(self.namespace, len, free_disk)
        {
            return Some(Err(budget));
        }
        let hold = self.unplace(volume, start)?;
        let Location::Ram(bytes) = hold.location else {
            unreachable!("a hold indexed as resident lives on scratch");
        };
        let scratch_offset = self.allocate(len);
        self.place(
            volume,
            Hold {
                range: hold.range,
                location: Location::Scratch {
                    offset: scratch_offset,
                    len,
                },
                age: hold.age,
            },
        );
        Some(Ok(PageOut {
            volume,
            range: hold.range,
            scratch_offset,
            bytes,
        }))
    }
}

impl Drop for HoldLedger {
    fn drop(&mut self) {
        self.discard();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NS: HoldsNamespace = HoldsNamespace {
        scope: 1,
        node: NodeId(0),
    };
    const OTHER: HoldsNamespace = HoldsNamespace {
        scope: 2,
        node: NodeId(0),
    };
    const PLENTY: u64 = u64::MAX;

    fn budgets(resident_limit: u64) -> HoldBudgets {
        HoldBudgets {
            resident_limit,
            scratch_total: 1_000,
            scratch_ceiling: 1_000,
            disk_reserve: 0,
        }
    }

    fn bytes(fill: u8, len: usize) -> Bytes {
        Bytes::from(vec![fill; len])
    }

    fn assert_idle(accountant: &HoldsAccountant) {
        assert_eq!(
            accountant.usage(),
            HoldsUsage {
                resident: 0,
                scratch: 0,
                namespaces: 0
            }
        );
    }

    #[test]
    fn peek_clips_an_overlapping_predecessor_and_preserves_gaps_and_accounting() {
        for resident in [0, 100] {
            let accountant = Arc::new(HoldsAccountant::new(budgets(resident)));
            let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
            for (offset, fill) in [(10, 1), (30, 2), (50, 3)] {
                ledger.hold(VolumeIndex(0), offset, bytes(fill, 10), PLENTY);
            }
            let usage = accountant.usage();
            let taken = ledger.peek(VolumeIndex(0), ByteRange::new(18, 35));
            let location = |fill, offset, len| {
                if resident == 0 {
                    Location::Scratch { offset, len }
                } else {
                    Location::Ram(bytes(fill, len as usize))
                }
            };
            assert_eq!(
                taken,
                vec![
                    Taken {
                        range: ByteRange::new(18, 20),
                        location: location(1, 8, 2)
                    },
                    Taken {
                        range: ByteRange::new(30, 35),
                        location: location(2, 10, 5)
                    },
                ]
            );
            assert_eq!(
                ledger.peek(VolumeIndex(0), ByteRange::new(45, 55)),
                vec![Taken {
                    range: ByteRange::new(50, 55),
                    location: location(3, 20, 5)
                },]
            );
            for range in [
                ByteRange::new(20, 30),
                ByteRange::new(35, 35),
                ByteRange::new(60, 70),
            ] {
                assert!(ledger.peek(VolumeIndex(0), range).is_empty());
            }
            assert!(
                ledger
                    .peek(VolumeIndex(1), ByteRange::new(0, 100))
                    .is_empty()
            );
            assert_eq!(accountant.usage(), usage);
            assert_eq!(
                ledger.held_ranges(VolumeIndex(0)),
                vec![
                    ByteRange::new(10, 20),
                    ByteRange::new(30, 40),
                    ByteRange::new(50, 60)
                ]
            );
        }
    }

    #[test]
    fn a_hold_that_fits_stays_in_ram_and_a_take_returns_it_and_zeroes_the_accountant() {
        let accountant = Arc::new(HoldsAccountant::new(budgets(100)));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        let outcome = ledger.hold(VolumeIndex(0), 10, bytes(7, 5), PLENTY);
        assert_eq!(outcome.placed, Placed::Ram);
        assert!(outcome.page_outs.is_empty());
        assert_eq!(accountant.namespace_usage(NS).resident, 5);
        assert_eq!(
            ledger.held_ranges(VolumeIndex(0)),
            vec![ByteRange::new(10, 15)]
        );

        let taken = ledger.take(VolumeIndex(0), ByteRange::new(0, 100));
        assert_eq!(
            taken,
            vec![Taken {
                range: ByteRange::new(10, 15),
                location: Location::Ram(bytes(7, 5)),
            }]
        );
        assert_eq!(ledger.total_held(), 0);
        assert_idle(&accountant);
    }

    #[test]
    fn pressure_pages_the_oldest_resident_holds_out_first() {
        let accountant = Arc::new(HoldsAccountant::new(budgets(10)));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        ledger.hold(VolumeIndex(0), 0, bytes(1, 4), PLENTY);
        ledger.hold(VolumeIndex(1), 0, bytes(2, 4), PLENTY);
        let outcome = ledger.hold(VolumeIndex(2), 0, bytes(3, 4), PLENTY);
        assert_eq!(outcome.placed, Placed::Ram);
        assert_eq!(
            outcome.page_outs,
            vec![PageOut {
                volume: VolumeIndex(0),
                range: ByteRange::new(0, 4),
                scratch_offset: 0,
                bytes: bytes(1, 4),
            }]
        );
        assert_eq!(
            accountant.namespace_usage(NS),
            NamespaceUsage {
                resident: 8,
                scratch: 4
            }
        );

        let taken = ledger.take(VolumeIndex(0), ByteRange::new(0, 4));
        assert_eq!(taken[0].location, Location::Scratch { offset: 0, len: 4 });
        ledger.take(VolumeIndex(1), ByteRange::new(0, 4));
        ledger.take(VolumeIndex(2), ByteRange::new(0, 4));
        assert_idle(&accountant);
    }

    #[test]
    fn bytes_larger_than_ram_go_straight_to_scratch_without_paging_anything() {
        let accountant = Arc::new(HoldsAccountant::new(budgets(10)));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        ledger.hold(VolumeIndex(0), 0, bytes(1, 4), PLENTY);
        let outcome = ledger.hold(VolumeIndex(0), 4, bytes(2, 20), PLENTY);
        assert_eq!(outcome.placed, Placed::Scratch);
        assert_eq!(
            outcome.page_outs,
            vec![PageOut {
                volume: VolumeIndex(0),
                range: ByteRange::new(4, 24),
                scratch_offset: 0,
                bytes: bytes(2, 20),
            }]
        );
        assert_eq!(ledger.resident_bytes(), 4);
        assert_eq!(ledger.scratch_bytes(), 20);
        ledger.take(VolumeIndex(0), ByteRange::new(0, 24));
        assert_idle(&accountant);
    }

    #[test]
    fn ram_another_namespace_holds_sends_new_bytes_to_scratch() {
        let accountant = Arc::new(HoldsAccountant::new(budgets(10)));
        let mut first = HoldLedger::new(NS, Arc::clone(&accountant));
        let mut second = HoldLedger::new(OTHER, Arc::clone(&accountant));
        first.hold(VolumeIndex(0), 0, bytes(1, 8), PLENTY);
        let outcome = second.hold(VolumeIndex(0), 0, bytes(2, 4), PLENTY);
        assert_eq!(outcome.placed, Placed::Scratch);
        assert_eq!(first.resident_bytes(), 8);
        first.discard();
        second.discard();
        assert_idle(&accountant);
    }

    #[test]
    fn each_scratch_budget_is_named_when_it_breaches() {
        let cases = [
            (
                HoldBudgets {
                    scratch_ceiling: 3,
                    ..budgets(0)
                },
                PLENTY,
                Budget::ScratchCeiling,
            ),
            (
                HoldBudgets {
                    scratch_total: 3,
                    ..budgets(0)
                },
                PLENTY,
                Budget::ScratchTotal,
            ),
            (
                HoldBudgets {
                    disk_reserve: 10,
                    ..budgets(0)
                },
                13,
                Budget::DiskReserve,
            ),
        ];
        for (budgets, free_disk, budget) in cases {
            let accountant = Arc::new(HoldsAccountant::new(budgets));
            let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
            let outcome = ledger.hold(VolumeIndex(0), 0, bytes(1, 4), free_disk);
            assert_eq!(outcome.placed, Placed::Breached(budget));
            assert_eq!(ledger.total_held(), 0);
            assert_idle(&accountant);
        }
    }

    #[test]
    fn page_outs_in_one_call_spend_the_reported_free_space() {
        let accountant = Arc::new(HoldsAccountant::new(HoldBudgets {
            disk_reserve: 10,
            ..budgets(8)
        }));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        ledger.hold(VolumeIndex(0), 0, bytes(1, 4), PLENTY);
        ledger.hold(VolumeIndex(1), 0, bytes(2, 4), PLENTY);
        // Room for one page-out of 4 above the reserve, not two.
        let outcome = ledger.hold(VolumeIndex(2), 0, bytes(3, 8), 15);
        assert_eq!(outcome.placed, Placed::Breached(Budget::DiskReserve));
        assert_eq!(outcome.page_outs.len(), 1);
        assert_eq!(outcome.page_outs[0].volume, VolumeIndex(0));
        ledger.discard();
        assert_idle(&accountant);
    }

    #[test]
    fn a_reheld_overlap_replaces_the_old_bytes_and_their_accounting() {
        let accountant = Arc::new(HoldsAccountant::new(budgets(100)));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        ledger.hold(VolumeIndex(0), 0, bytes(1, 10), PLENTY);
        ledger.hold(VolumeIndex(0), 4, bytes(2, 2), PLENTY);
        assert_eq!(
            ledger.held_ranges(VolumeIndex(0)),
            vec![
                ByteRange::new(0, 4),
                ByteRange::new(4, 6),
                ByteRange::new(6, 10)
            ]
        );
        assert_eq!(accountant.usage().resident, 10);
        let taken = ledger.take(VolumeIndex(0), ByteRange::new(3, 7));
        let ranges: Vec<_> = taken.iter().map(|piece| piece.range).collect();
        assert_eq!(
            ranges,
            vec![
                ByteRange::new(3, 4),
                ByteRange::new(4, 6),
                ByteRange::new(6, 7)
            ]
        );
        assert_eq!(taken[1].location, Location::Ram(bytes(2, 2)));
        assert_eq!(accountant.usage().resident, 6);
        ledger.take(VolumeIndex(0), ByteRange::new(0, 10));
        assert_idle(&accountant);
    }

    #[test]
    fn a_partial_take_of_scratch_returns_the_span_and_keeps_the_rest() {
        let accountant = Arc::new(HoldsAccountant::new(budgets(0)));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        ledger.hold(VolumeIndex(3), 100, bytes(1, 10), PLENTY);
        let taken = ledger.take(VolumeIndex(3), ByteRange::new(104, 106));
        assert_eq!(
            taken,
            vec![Taken {
                range: ByteRange::new(104, 106),
                location: Location::Scratch { offset: 4, len: 2 },
            }]
        );
        assert_eq!(accountant.usage().scratch, 8);
        let rest = ledger.take(VolumeIndex(3), ByteRange::new(0, 200));
        assert_eq!(
            rest.iter()
                .map(|piece| piece.location.clone())
                .collect::<Vec<_>>(),
            vec![
                Location::Scratch { offset: 0, len: 4 },
                Location::Scratch { offset: 6, len: 4 },
            ]
        );
        assert_idle(&accountant);
    }

    #[test]
    fn scratch_offsets_start_over_once_nothing_lives_there() {
        let accountant = Arc::new(HoldsAccountant::new(budgets(0)));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        ledger.hold(VolumeIndex(0), 0, bytes(1, 4), PLENTY);
        ledger.hold(VolumeIndex(0), 4, bytes(1, 4), PLENTY);
        assert_eq!(ledger.scratch_extent(), 8);
        ledger.take(VolumeIndex(0), ByteRange::new(0, 4));
        assert_eq!(ledger.scratch_extent(), 8);
        ledger.take(VolumeIndex(0), ByteRange::new(4, 8));
        assert_eq!(ledger.scratch_extent(), 0);
        let outcome = ledger.hold(VolumeIndex(0), 0, bytes(1, 4), PLENTY);
        assert_eq!(outcome.page_outs[0].scratch_offset, 0);
        ledger.discard();
        assert_idle(&accountant);
    }

    #[test]
    fn lowered_budgets_evict_nothing_and_judge_the_next_hold() {
        let accountant = Arc::new(HoldsAccountant::new(budgets(100)));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        ledger.hold(VolumeIndex(0), 0, bytes(1, 50), PLENTY);
        accountant.set_budgets(HoldBudgets {
            scratch_ceiling: 10,
            ..budgets(20)
        });
        assert_eq!(ledger.resident_bytes(), 50);
        let outcome = ledger.hold(VolumeIndex(1), 0, bytes(2, 10), PLENTY);
        assert_eq!(outcome.placed, Placed::Breached(Budget::ScratchCeiling));
        assert!(outcome.page_outs.is_empty());
        ledger.discard();
        assert_idle(&accountant);
    }

    #[test]
    fn an_empty_hold_holds_nothing() {
        let accountant = Arc::new(HoldsAccountant::new(budgets(0)));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        let outcome = ledger.hold(VolumeIndex(0), 5, Bytes::new(), 0);
        assert_eq!(outcome.placed, Placed::Ram);
        assert_eq!(ledger.total_held(), 0);
        assert_idle(&accountant);
    }

    #[test]
    fn a_dropped_ledger_releases_what_it_still_holds() {
        let accountant = Arc::new(HoldsAccountant::new(budgets(4)));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        ledger.hold(VolumeIndex(0), 0, bytes(1, 4), PLENTY);
        let outcome = ledger.hold(VolumeIndex(1), 0, bytes(2, 4), PLENTY);
        assert_eq!(outcome.page_outs.len(), 1);
        assert_eq!(accountant.namespace_usage(NS).scratch, 4);
        assert_eq!(accountant.namespace_usage(NS).resident, 4);
        drop(ledger);
        assert_idle(&accountant);
    }

    #[test]
    #[should_panic(expected = "released 1 resident bytes with only 0 accounted")]
    fn releasing_more_than_was_accounted_is_a_bug() {
        let accountant = Arc::new(HoldsAccountant::new(budgets(10)));
        accountant.release_resident(NS, 1);
    }

    #[test]
    fn paging_out_all_goes_oldest_first_and_stops_at_the_first_breach() {
        let accountant = Arc::new(HoldsAccountant::new(HoldBudgets {
            resident_limit: 100,
            scratch_total: 1_000,
            scratch_ceiling: 6,
            disk_reserve: 0,
        }));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        ledger.hold(VolumeIndex(2), 0, bytes(1, 4), PLENTY);
        ledger.hold(VolumeIndex(0), 0, bytes(2, 4), PLENTY);
        ledger.hold(VolumeIndex(1), 0, bytes(3, 4), PLENTY);

        let page_outs = ledger.page_out_all(PLENTY);
        assert_eq!(
            page_outs,
            vec![PageOut {
                volume: VolumeIndex(2),
                range: ByteRange::new(0, 4),
                scratch_offset: 0,
                bytes: bytes(1, 4),
            }]
        );
        assert_eq!(
            accountant.namespace_usage(NS),
            NamespaceUsage {
                resident: 8,
                scratch: 4
            }
        );
        assert_eq!(
            ledger.scratch_holds(),
            vec![ScratchHold {
                volume: VolumeIndex(2),
                range: ByteRange::new(0, 4),
                scratch_offset: 0,
            }]
        );
        let left = ledger.take(VolumeIndex(0), ByteRange::new(0, 4));
        assert_eq!(left[0].location, Location::Ram(bytes(2, 4)));
    }

    #[test]
    fn paging_out_all_with_room_leaves_nothing_resident() {
        let accountant = Arc::new(HoldsAccountant::new(budgets(100)));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        ledger.hold(VolumeIndex(0), 10, bytes(1, 4), PLENTY);
        ledger.hold(VolumeIndex(1), 0, bytes(2, 3), PLENTY);
        let page_outs = ledger.page_out_all(PLENTY);
        let offsets: Vec<(VolumeIndex, u64)> = page_outs
            .iter()
            .map(|page_out| (page_out.volume, page_out.scratch_offset))
            .collect();
        assert_eq!(offsets, vec![(VolumeIndex(0), 0), (VolumeIndex(1), 4)]);
        assert_eq!(ledger.resident_bytes(), 0);
        assert_eq!(ledger.scratch_extent(), 7);
    }

    #[test]
    fn a_restored_scratch_hold_sets_the_extent_and_is_accounted() {
        let accountant = Arc::new(HoldsAccountant::new(budgets(100)));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        let restored = [
            ScratchHold {
                volume: VolumeIndex(1),
                range: ByteRange::new(20, 25),
                scratch_offset: 9,
            },
            ScratchHold {
                volume: VolumeIndex(0),
                range: ByteRange::new(0, 9),
                scratch_offset: 0,
            },
        ];
        for held in restored {
            assert_eq!(ledger.restore(held, PLENTY), Reserve::Granted);
        }
        assert_eq!(ledger.scratch_extent(), 14);
        assert_eq!(ledger.scratch_bytes(), 14);
        assert_eq!(
            accountant.namespace_usage(NS),
            NamespaceUsage {
                resident: 0,
                scratch: 14
            }
        );
        assert_eq!(ledger.scratch_holds(), vec![restored[1], restored[0]]);

        // A new spill goes past what was restored.
        let outcome = ledger.hold(VolumeIndex(2), 0, bytes(5, 2), PLENTY);
        assert_eq!(outcome.placed, Placed::Ram);
        assert_eq!(ledger.page_out_all(PLENTY)[0].scratch_offset, 14);

        let taken = ledger.take(VolumeIndex(1), ByteRange::new(22, 25));
        assert_eq!(taken[0].location, Location::Scratch { offset: 11, len: 3 });
        drop(ledger);
        assert_idle(&accountant);
    }

    #[test]
    fn a_restored_hold_past_the_scratch_ceiling_is_not_held() {
        let accountant = Arc::new(HoldsAccountant::new(HoldBudgets {
            resident_limit: 100,
            scratch_total: 1_000,
            scratch_ceiling: 4,
            disk_reserve: 0,
        }));
        let mut ledger = HoldLedger::new(NS, Arc::clone(&accountant));
        let held = ScratchHold {
            volume: VolumeIndex(0),
            range: ByteRange::new(0, 5),
            scratch_offset: 0,
        };
        assert_eq!(
            ledger.restore(held, PLENTY),
            Reserve::Breached(Budget::ScratchCeiling)
        );
        assert_eq!(ledger.total_held(), 0);
        assert_eq!(ledger.scratch_extent(), 0);
        assert_idle(&accountant);
    }
}
