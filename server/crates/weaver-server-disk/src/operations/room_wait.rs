// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A write that found its disk full waits for room on that disk: the one
// judgement every kind of job waits by, a usenet job held for a full disk
// and a torrent in upload mode alike. It reads only what the capacity layer
// holds already: nothing here touches the disk.
//
// The wait has one mark: what its disk showed free as the write failed, or
// what a probe running then published after. A reading counts when it is
// good and its probe began after the write failed. What it has to show
// depends on what the step that failed did with its own output. A step that
// removed what it had written (or never wrote it) names what it needs free
// to run again: a reading has to show at least that much. A step that gave
// nothing back has to see the figure rise over the mark by what was left of
// its write, or by the reserve when it names neither. Either way the disk
// must also leave more than the reserve free to spend. A disk with no good
// reading keeps it waiting.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

pub use super::disk::StartBooking;
use super::disk::{CapacityReader, DiskLedger, StartPart, StorageCapacity, StorageRoot};
use crate::jobs::ids::JobId;

// The disk a wait is on: a root's, or the disk of a path that lies on none
// of theirs.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Disk {
    Root(StorageRoot),
    Path(PathBuf),
}

impl Disk {
    // The disk a write under `path` lands on: the working or complete
    // root's when the capacity layer knows `path` lies on it, else the
    // path's own. Nothing is looked at.
    pub fn of_path(capacity: &StorageCapacity, path: &Path) -> Self {
        Self::on(capacity.known_root_of(path), path)
    }

    // The disk a write under `path` lands on, looked up now, by the same
    // choice: `None` while the root `path` lies on, or none, is not known,
    // which is never taken for a disk off the roots'.
    pub async fn look_up(capacity: &StorageCapacity, path: &Path) -> Option<Self> {
        Some(Self::on(capacity.root_of_path(path).await?, path))
    }

    // `path`'s disk when it lies on `root`: the working or complete root's,
    // else its own.
    fn on(root: Option<StorageRoot>, path: &Path) -> Self {
        root.filter(|root| matches!(root, StorageRoot::Complete | StorageRoot::Intermediate))
            .map_or_else(|| Self::Path(path.to_path_buf()), Self::Root)
    }

    // The reading and ledger writes on this disk are booked by: a root's.
    // A path off the roots' disks has none, and is read only while someone
    // waits on it.
    pub fn reader(&self, capacity: &StorageCapacity) -> Option<CapacityReader> {
        match self {
            Self::Root(root) => Some(capacity.reader(*root)),
            Self::Path(_) => None,
        }
    }

    // The device the capacity layer holds for this disk already; nothing is
    // looked at.
    pub fn device(&self, capacity: &StorageCapacity) -> Option<u64> {
        match self {
            Self::Root(root) => capacity.root_slot_device(*root),
            Self::Path(path) => capacity.held_path_device(path),
        }
    }

    // How many probes of this disk had begun: a reading counts only when
    // its probe began after a count taken before.
    pub(crate) fn begun(&self, capacity: &StorageCapacity) -> Option<u64> {
        match self {
            Self::Root(root) => Some(capacity.look(*root).begun),
            Self::Path(path) => capacity.path_look(path).map(|look| look.begun),
        }
    }
}

// What the write that failed says of the room it waits for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Wants {
    // What the step needs free to run again, when it removed what it had
    // written as it failed, or never wrote it.
    pub needs: Option<u64>,
    // What was left to write of the file, spool or scratch whose write was
    // refused, when the step names no `needs`: the least a reading has to
    // show over the mark.
    pub left: Option<u64>,
    // The write was refused by a quota, not by the disk: the disk's free
    // figure already shows room the writer cannot use.
    pub quota: bool,
}

impl Wants {
    // The write that failed with `error` wants `needs` free to run again.
    pub fn of(error: &std::io::Error, needs: Option<u64>) -> Self {
        Self {
            needs,
            left: None,
            quota: super::disk::is_quota_exceeded(error),
        }
    }
}

impl From<Option<u64>> for Wants {
    fn from(needs: Option<u64>) -> Self {
        Self {
            needs,
            ..Self::default()
        }
    }
}

pub struct RoomWait {
    // The least a reading has to show over the mark, when the step names
    // no `needs`: what was left of the file whose write was refused.
    pub rise: Option<u64>,
    // The wait began with a quota error: the free figure has to rise from
    // the mark by what the step needs, since it showed that much already.
    pub quota: bool,
    pub disk: Disk,
    // The disk's count of the probes it had begun as the write failed: only
    // a later probe's reading can show room made since.
    pub(crate) since: u64,
    // What the disk showed free when the write failed: the figure of its
    // newest reading whose probe began no later than the failure, even a
    // stale one. A reading has to show more than this to show room made.
    // It is this wait's alone, and ends with it. `None` while the disk has
    // shown nothing: the first reading since the failure then sets it.
    pub mark: Option<u64>,
    // What the waiter itself gave back to the disk after the mark was
    // taken. A reading shows it as room, and it is room the waiter fills
    // again, so it counts on top of the mark.
    pub freed: u64,
    // What the step that failed needs free to run again, when it removed
    // what it had written as it failed. A reading then has to show this
    // much, and the mark is not consulted.
    pub needs: Option<u64>,
    // A probe was running as the write failed: it began before the failure,
    // so the figure it publishes takes the mark's place, once.
    pub(crate) running: bool,
    // How many times the root waited on had been pointed elsewhere, and the
    // directory it was pointed at as the write failed: when the root is
    // pointed elsewhere, the wait goes on reading that directory's disk.
    pub(crate) moved: u64,
    pub(crate) dir: PathBuf,
}

impl RoomWait {
    // What a wait is made with. No wait is let go by less than the write
    // that was refused: a site that names neither what its step needs nor
    // what was left of its write waits for the free figure to rise by the
    // reserve. Refused by a quota with a `needs`, the mark is not what the
    // disk showed before: a reading from then may have been taken while the
    // step's own output was on disk, so the first reading whose probe began
    // after the failure, and after the step gave its output back, sets it.
    fn terms(
        wants: Wants,
        shown: Option<u64>,
        running: bool,
        reserve: u64,
    ) -> (Wants, Option<u64>, bool) {
        let left = match (wants.needs, wants.left) {
            (None, None) => Some(reserve.max(1)),
            (_, left) => left,
        };
        let wants = Wants { left, ..wants };
        if wants.quota && wants.needs.is_some() {
            (wants, None, false)
        } else {
            (wants, shown, running)
        }
    }

    // A write found `disk`, whose directory is `dir`, full: the wait for
    // room there begins. What the disk showed free as the write failed is
    // what a later reading has to beat. Waiting on a root's ledger asks
    // for a reading; a path is read while anyone waits on it.
    pub fn begin(
        capacity: &StorageCapacity,
        disk: Disk,
        dir: PathBuf,
        wants: Wants,
        reserve: u64,
    ) -> Self {
        let look = match &disk {
            Disk::Root(root) => {
                let look = capacity.look(*root);
                capacity.ledger(*root).wait();
                look
            }
            Disk::Path(path) => capacity.hold_path(path),
        };
        let (
            Wants {
                needs,
                left: rise,
                quota,
            },
            mark,
            running,
        ) = Self::terms(wants, look.shown, look.probe < look.begun, reserve);
        Self {
            rise,
            quota,
            disk,
            since: look.begun,
            mark,
            freed: 0,
            needs,
            running,
            moved: look.moved,
            dir,
        }
    }

    // A reading came. Whether it shows room made: the bytes of it that are
    // the waiter's once it goes on, and the device of its disk. `going`
    // says the waiter goes on already, so only the mark moves. `promised`
    // is what is promised already on the disk, of the device given, to
    // waiters let go before this one whose steps have not ended. A reading
    // that does not show room changes nothing but the mark.
    pub fn judge(
        &mut self,
        capacity: &StorageCapacity,
        reserve: u64,
        going: bool,
        promised: impl FnOnce(&Disk, Option<u64>) -> u64,
    ) -> Option<(u64, Option<u64>)> {
        // The root waited on was pointed at another directory: the wait
        // stays on the disk that failed, read from here as a path of its
        // own. Its mark stands, and readings count from now.
        let repointed = match &self.disk {
            Disk::Root(root) if capacity.look(*root).moved != self.moved => Some(*root),
            _ => None,
        };
        if let Some(root) = repointed {
            capacity.ledger(root).unwait();
            let look = capacity.hold_path(&self.dir);
            self.disk = Disk::Path(self.dir.clone());
            self.since = look.begun;
            self.running = false;
            self.moved = look.moved;
        }
        // What the disk shows now, and what is free on it to spend: for a
        // root, less what is promised on it.
        let (look, spendable) = match &self.disk {
            Disk::Root(root) => (
                Some(capacity.look(*root)),
                capacity
                    .free(*root)
                    .filter(|free| !free.stale)
                    .map(|free| free.bytes),
            ),
            Disk::Path(path) => {
                let look = capacity.path_look(path);
                (look, look.and_then(|look| look.shown))
            }
        };
        let look = look?;
        // A probe running as the write failed began before it: what it
        // published is the newest figure from before the failure, and takes
        // the mark's place.
        if self.running && look.probe >= self.since {
            if let Some(before) = look.figure_of(self.since) {
                self.mark = Some(before);
            }
            self.running = false;
        }
        // A good reading whose probe began after the failure counts. With
        // nothing shown before, it is the mark. One that shows more than the
        // mark, with more than the reserve free to spend, shows room was
        // made.
        if look.probe > self.since
            && look.good
            && let Some(shown) = look.shown
        {
            let spends = spendable.is_some_and(|free| free > reserve);
            // What the reading has to show at least, and how much of it is
            // the waiter's once it is let go. The step gave its output back
            // as it failed: the room the step takes. Refused by a quota, the
            // disk showed that room already: the figure has to rise by it
            // from the mark. A step that names no need waits for the figure
            // to rise by what was left of the file it was writing. What the
            // waiter gave back counts on top of the mark.
            let floor = match (self.needs, self.mark, self.rise) {
                (Some(needs), _, _) if !self.quota => Some((needs, needs)),
                (Some(bytes), Some(mark), _) | (None, Some(mark), Some(bytes)) => {
                    Some((mark.saturating_add(self.freed).saturating_add(bytes), bytes))
                }
                _ => None,
            };
            match (floor, self.mark) {
                // On top of what is promised to the waiters let go before
                // it whose steps have not ended.
                (Some((floor, bytes)), _) if !going => {
                    let device = self.disk.device(capacity);
                    let promised = promised(&self.disk, device);
                    if shown >= floor.saturating_add(promised) && spends {
                        return Some((bytes, device));
                    }
                }
                (Some(_), _) => {}
                // Nothing was shown before to compare with: the first
                // reading that counts only sets the mark.
                (None, None) => self.mark = Some(shown),
                // Every wait has a figure once it has a mark.
                (None, Some(_)) => {}
            }
        }
        None
    }

    // Whether the newest reading of the wait's root, a good one whose probe
    // began after the write failed, shows more than the disk did then: the
    // floor `judge` lets a waiter go on, less what is promised. A step that
    // gave its output back as it failed names what it needs, which the
    // start's room judges, and has no mark to beat. It changes nothing; a
    // wait with no mark yet shows none.
    pub fn shows_rise(&self, capacity: &StorageCapacity) -> bool {
        let Disk::Root(root) = &self.disk else {
            return false;
        };
        let look = capacity.look(*root);
        if look.moved != self.moved || look.probe <= self.since || !look.good {
            return false;
        }
        let Some(shown) = look.shown else {
            return false;
        };
        match (self.needs, self.mark, self.rise) {
            (Some(_), _, _) if !self.quota => true,
            (Some(bytes), Some(mark), _) | (None, Some(mark), Some(bytes)) => {
                shown >= mark.saturating_add(self.freed).saturating_add(bytes)
            }
            _ => false,
        }
    }

    // Whether the wait's root has a good reading whose probe began after
    // the write failed: the only kind a start after a full disk is judged
    // on. `None` when the wait is on a path, not a root.
    pub fn root_read_since(&self, capacity: &StorageCapacity) -> Option<(StorageRoot, bool)> {
        match &self.disk {
            Disk::Root(root) if capacity.look(*root).moved == self.moved => {
                let look = capacity.look(*root);
                Some((*root, look.probe > self.since && look.good))
            }
            _ => None,
        }
    }

    // The wait ends: the waiter went on, or is gone.
    pub fn end(&self, capacity: &StorageCapacity) {
        match &self.disk {
            Disk::Root(root) => capacity.ledger(*root).unwait(),
            Disk::Path(path) => capacity.release_path(path),
        }
    }

    // The step of a waiter let go on `ended`, a promise on a disk whose
    // probes had begun `begun` times as it ended, is over: this wait, on
    // that disk, counts readings from now, since one from before shows
    // room that step may have filled. A wait on the same device under
    // another root or path counts its own disk's probes.
    pub fn promise_ended(&mut self, capacity: &StorageCapacity, ended: &Promised, begun: u64) {
        let begun = if self.disk == ended.disk {
            Some(begun)
        } else if ended.on(&self.disk, self.disk.device(capacity)) {
            self.disk.begun(capacity)
        } else {
            None
        };
        if let Some(begun) = begun {
            self.since = self.since.max(begun);
            self.running = false;
        }
    }
}

// Room promised to a waiter let go on the figure its step needs: the disk,
// and how much of what it showed is the waiter's until its step ends.
#[derive(Clone, Debug)]
pub struct Promised {
    pub disk: Disk,
    pub bytes: u64,
    // The device of that disk as the capacity layer held it at the promise,
    // so a waiter held on a root and one held on a path of the same
    // physical disk are judged against each other. `None` while it was not
    // known: the promise then counts only for waiters held on the very
    // same root or path.
    pub device: Option<u64>,
}

impl Promised {
    // Whether the promise is on the disk `disk`, whose device is `device`.
    pub(crate) fn on(&self, disk: &Disk, device: Option<u64>) -> bool {
        self.disk == *disk || (device.is_some() && self.device == device)
    }
}

// What is promised by disk and by device, kept up to date with every
// promise made or taken back, so a wait reads its total without walking
// every promise. A promise counts for a disk when it is on that disk or on
// the same known device, once when both.
#[derive(Default)]
struct PromisedTotals {
    disk: HashMap<Disk, u128>,
    device: HashMap<u64, u128>,
    // Promises on the disk that also know their device: the ones the two
    // totals above both count.
    both: HashMap<Disk, HashMap<u64, u128>>,
}

impl PromisedTotals {
    fn add(&mut self, promised: &Promised) {
        let bytes = u128::from(promised.bytes);
        *self.disk.entry(promised.disk.clone()).or_default() += bytes;
        if let Some(device) = promised.device {
            *self.device.entry(device).or_default() += bytes;
            *self
                .both
                .entry(promised.disk.clone())
                .or_default()
                .entry(device)
                .or_default() += bytes;
        }
    }

    // `promised`, added before, is taken back. A total that reaches zero
    // goes, so the maps hold only disks and devices something is promised
    // on.
    fn remove(&mut self, promised: &Promised) {
        let bytes = u128::from(promised.bytes);
        take(&mut self.disk, &promised.disk, bytes);
        if let Some(device) = promised.device {
            take(&mut self.device, &device, bytes);
            if let Some(devices) = self.both.get_mut(&promised.disk) {
                take(devices, &device, bytes);
                if devices.is_empty() {
                    self.both.remove(&promised.disk);
                }
            }
        }
    }

    // What is promised on `disk`, whose device is `device`, held at
    // `u64::MAX` as a saturating sum is.
    fn on(&self, disk: &Disk, device: Option<u64>) -> u64 {
        let mut total = self.disk.get(disk).copied().unwrap_or(0);
        if let Some(device) = device {
            total += self.device.get(&device).copied().unwrap_or(0);
            total -= self
                .both
                .get(disk)
                .and_then(|devices| devices.get(&device))
                .copied()
                .unwrap_or(0);
        }
        u64::try_from(total).unwrap_or(u64::MAX)
    }
}

// `bytes` less on `key`'s total, which goes once it is zero.
fn take<K: std::hash::Hash + Eq>(totals: &mut HashMap<K, u128>, key: &K, bytes: u128) {
    if let Some(total) = totals.get_mut(key) {
        *total -= bytes;
        if *total == 0 {
            totals.remove(key);
        }
    }
}

// The promises, and their totals by disk and device: of all of them, and
// of those whose bytes are not also promised on a disk ledger, which a start
// judged on the ledger counts beside it.
#[derive(Default)]
struct Book {
    promised: BTreeMap<(Waiter, JobId), Promised>,
    totals: PromisedTotals,
    ledgered: BTreeSet<(Waiter, JobId)>,
    off_ledger: PromisedTotals,
}

impl Book {
    fn add(&mut self, key: (Waiter, JobId), promised: &Promised, ledgered: bool) {
        self.totals.add(promised);
        if ledgered {
            self.ledgered.insert(key);
        } else {
            self.off_ledger.add(promised);
        }
    }

    fn take_back(&mut self, key: (Waiter, JobId), promised: &Promised) {
        self.totals.remove(promised);
        if !self.ledgered.remove(&key) {
            self.off_ledger.remove(promised);
        }
    }
}

// Whose a promise is: a usenet job's, or a torrent's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Waiter {
    Usenet,
    Torrent,
}

// The room promised to waiters let go on a reading, of every kind, held by
// the capacity layer, so each kind judges its waits beside what the other
// was promised. A promise lasts until its waiter's step ends. Each kind's
// count is kept apart, so a kind with none of its own checks one count.
#[derive(Default)]
pub struct RoomPromises {
    book: Mutex<Book>,
    counts: [AtomicUsize; 2],
    // The promises of one kind that ended, with how many probes of their
    // disk had begun then, for the other kind's waits to count readings
    // from: each kind takes those of the other at its next pass. A wait
    // hears an end on its own disk, where it reads the count, or on its
    // device, where it reads only that one ended. So the list is kept
    // bounded by what is waited on, whether or not the other kind ever
    // takes them: one entry per disk and device, holding the latest count;
    // a root's while its device is the root's own, at most two per root; a
    // path's while some wait reads that path. Past that an end is kept
    // only by its device, one entry per device a root or a read path lies
    // on that no entry above holds, and dropped once no wait can be on it.
    // A wait that begins later counts readings from its own start, so an
    // end dropped then is one no wait needs.
    ended: [Mutex<Vec<(Promised, u64)>>; 2],
    ends: [AtomicUsize; 2],
}

impl RoomPromises {
    fn locked(&self) -> std::sync::MutexGuard<'_, Book> {
        self.book.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn count(&self, waiter: Waiter) -> &AtomicUsize {
        &self.counts[waiter as usize]
    }

    // Whether `waiter`'s kind holds no promise.
    pub(crate) fn none_of(&self, waiter: Waiter) -> bool {
        self.count(waiter).load(Ordering::Acquire) == 0
    }

    // `job_id` of `waiter`'s kind is promised `promised`, in place of any
    // promise it held.
    pub fn insert(&self, waiter: Waiter, job_id: JobId, promised: Promised) {
        self.insert_as(waiter, job_id, promised, false);
    }

    // [`Self::insert`] for a waiter whose bytes are promised on the disk's
    // ledger as well, as a usenet start let go on its figure is: a start
    // judged on the ledger counts them once, there.
    pub fn insert_ledgered(&self, waiter: Waiter, job_id: JobId, promised: Promised) {
        self.insert_as(waiter, job_id, promised, true);
    }

    fn insert_as(&self, waiter: Waiter, job_id: JobId, promised: Promised, ledgered: bool) {
        let key = (waiter, job_id);
        let mut book = self.locked();
        if let Some(replaced) = book.promised.remove(&key) {
            book.take_back(key, &replaced);
        } else {
            self.count(waiter).fetch_add(1, Ordering::AcqRel);
        }
        book.add(key, &promised, ledgered);
        book.promised.insert(key, promised);
    }

    // The promise to `job_id` of `waiter`'s kind ends: what it was.
    pub fn remove(&self, waiter: Waiter, job_id: JobId) -> Option<Promised> {
        if self.none_of(waiter) {
            return None;
        }
        let mut book = self.locked();
        let removed = book.promised.remove(&(waiter, job_id));
        if let Some(removed) = &removed {
            book.take_back((waiter, job_id), removed);
            self.count(waiter).fetch_sub(1, Ordering::AcqRel);
        }
        removed
    }

    // The promise to `job_id` of `waiter`'s kind ended with its step: what
    // it was, and how many probes of its disk had begun. The other kind's
    // waits hear of it at their next pass.
    pub fn end(
        &self,
        capacity: &StorageCapacity,
        waiter: Waiter,
        job_id: JobId,
    ) -> Option<(Promised, u64)> {
        let ended = self.remove(waiter, job_id)?;
        let begun = ended.disk.begun(capacity)?;
        let other = match waiter {
            Waiter::Usenet => Waiter::Torrent,
            Waiter::Torrent => Waiter::Usenet,
        } as usize;
        {
            let mut list = self.ended[other]
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            list.push((ended.clone(), begun));
            compact(&mut list, capacity);
            self.ends[other].store(list.len(), Ordering::Release);
        }
        Some((ended, begun))
    }

    // The promises of the other kind that ended since `waiter`'s kind last
    // took them.
    pub fn take_ended(&self, waiter: Waiter) -> Vec<(Promised, u64)> {
        let at = waiter as usize;
        if self.ends[at].load(Ordering::Acquire) == 0 {
            return Vec::new();
        }
        let taken = std::mem::take(
            &mut *self.ended[at]
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        self.ends[at].store(0, Ordering::Release);
        taken
    }

    // Of `waiter`'s kind, only the promises `keep` keeps stay.
    pub fn retain(&self, waiter: Waiter, mut keep: impl FnMut(JobId) -> bool) {
        if self.none_of(waiter) {
            return;
        }
        let mut book = self.locked();
        let Book {
            promised,
            totals,
            ledgered,
            off_ledger,
        } = &mut *book;
        let mut left = 0;
        promised.retain(|key, promise| {
            if key.0 != waiter {
                return true;
            }
            if keep(key.1) {
                left += 1;
                return true;
            }
            totals.remove(promise);
            if !ledgered.remove(key) {
                off_ledger.remove(promise);
            }
            false
        });
        self.count(waiter).store(left, Ordering::Release);
    }

    // What is promised on `disk`, whose device is `device`, to every
    // waiter of either kind.
    pub fn on(&self, disk: &Disk, device: Option<u64>) -> u64 {
        if self.none_of(Waiter::Usenet) && self.none_of(Waiter::Torrent) {
            return 0;
        }
        self.locked().totals.on(disk, device)
    }

    // What of [`Self::on`] no disk ledger counts: the room a start judged on
    // the ledger of `disk` leaves aside for those let go before it.
    pub fn off_ledger(&self, disk: &Disk, device: Option<u64>) -> u64 {
        if self.none_of(Waiter::Usenet) && self.none_of(Waiter::Torrent) {
            return 0;
        }
        self.locked().off_ledger.on(disk, device)
    }

    // The walk the totals replace: every promise, for one disk, or only
    // those no ledger counts.
    #[cfg(test)]
    fn on_by_walk(&self, disk: &Disk, device: Option<u64>, off_ledger: bool) -> u64 {
        let book = self.locked();
        book.promised
            .iter()
            .filter(|(key, promised)| {
                promised.on(disk, device) && !(off_ledger && book.ledgered.contains(key))
            })
            .fold(0u64, |sum, (_, promised)| {
                sum.saturating_add(promised.bytes)
            })
    }

    // What is promised to `waiter`'s kind, in all.
    #[cfg(any(test, feature = "test-support"))]
    pub fn bytes_of(&self, waiter: Waiter) -> u64 {
        self.locked()
            .promised
            .iter()
            .filter(|((of, _), _)| *of == waiter)
            .map(|(_, promised)| promised.bytes)
            .sum()
    }
}

// `list` kept to what a wait can still hear of, as [`RoomPromises::ended`]
// tells: ends on one disk and device fold into one, holding the latest
// count, and an end on a disk no wait reads stays only as its device's.
fn compact(list: &mut Vec<(Promised, u64)>, capacity: &StorageCapacity) {
    let mut kept: Vec<(Promised, u64)> = Vec::with_capacity(list.len());
    let mut by_device = Vec::new();
    for (promised, begun) in list.drain(..) {
        let read = match &promised.disk {
            Disk::Root(root) => {
                promised.device.is_none() || promised.device == capacity.root_slot_device(*root)
            }
            Disk::Path(path) => capacity.path_look(path).is_some(),
        };
        if !read {
            by_device.push((promised, begun));
            continue;
        }
        match kept
            .iter_mut()
            .find(|(at, _)| at.disk == promised.disk && at.device == promised.device)
        {
            Some((_, at)) => *at = (*at).max(begun),
            None => kept.push((promised, begun)),
        }
    }
    for (promised, begun) in by_device {
        let Some(device) = promised.device else {
            continue;
        };
        // Heard by its device alone, it needs no count of its own: one on
        // that device already says as much, and its count is its own
        // disk's, which is not this one's.
        if !capacity.device_waited_on(device)
            || kept.iter().any(|(at, _)| at.device == Some(device))
        {
            continue;
        }
        kept.push((promised, begun));
    }
    *list = kept;
}

// Whether the room a waiter sees, by [`waking_room`], holds `need`: any
// room does with no reading to check, none with a reading at or under the
// reserve.
pub(crate) fn room_holds(room: Option<u64>, need: u64) -> bool {
    room.is_none_or(|room| need <= room)
}

// A writer whose booking a ledger refused waits there for room, as a usenet
// job whose write the reserve holds back does: it holds nothing promised
// while it waits, and a reading that shows room above the reserve for all
// it needs lets it go, its need promised at once so the next waiter woken
// is judged without it. Waiters are woken in the order of their tickets.
#[derive(Debug)]
pub struct LedgerWait {
    pub ticket: u64,
    pub needs: u64,
}

impl LedgerWait {
    // The wait begins: a reading is asked for, and the ledger counts one
    // more waiter.
    pub fn begin(ledger: &DiskLedger, needs: u64) -> Self {
        Self {
            ticket: ledger.wait(),
            needs,
        }
    }

    // A reading came: whether it lets the waiter go, by the judgement every
    // ledger waiter wakes by. Let go, what it needs is promised to it and it
    // waits no more.
    pub fn wake(&self, ledger: &DiskLedger, reader: &CapacityReader, reserve: u64) -> bool {
        if !ledger.book_woken(reader, reserve, self.needs) {
            return false;
        }
        ledger.unwait();
        true
    }

    // The wait ends with nothing promised: the waiter went on otherwise, or
    // is gone.
    pub fn end(&self, ledger: &DiskLedger) {
        ledger.unwait();
    }
}

// The room on `root`'s disk promised to writers let go from a wait for room
// and not counted on its ledger: a start judged there leaves it aside.
fn held_on(capacity: &StorageCapacity, root: StorageRoot) -> u64 {
    capacity
        .promises()
        .off_ledger(&Disk::Root(root), capacity.root_slot_device(root))
}

// Whether a torrent ever downloaded, from the bytes it counts as
// downloaded: one that has is not kept from starting by
// [`StartCheck::hold`], since what it writes next waits for room as a write
// that finds none does. A usenet job starts by [`StartCheck::admit`]
// instead, which judges what it still has to fetch, whatever it fetched.
pub fn ever_downloaded(downloaded_bytes: u64) -> bool {
    downloaded_bytes > 0
}

// Whether jobs with bytes to fetch may start on a root's disk: the one
// check every kind of job starts by, a usenet job that never downloaded and
// a torrent with bytes left alike. Once the disk shows no room over the
// reserve, none starts until a good reading shows room, or the disk has no
// reading. A disk with no reading, or only the last good one held across a
// failed probe, or no reserve, is not checked: only a good reading begins
// the hold. A hold begun lasts across a stale reading, as a wait for room
// does. While it lasts, its root's ledger is waited on, so readings come.
#[derive(Clone, Copy, Debug)]
pub struct StartCheck {
    pub root: StorageRoot,
    pub reserve: u64,
}

impl StartCheck {
    // A usenet job would start with `need` bytes still to fetch: they are
    // promised on the root's ledger when they fit within what a fresh,
    // good reading leaves above the reserve less every promise there.
    // Nothing else is judged or promised; see [`DiskLedger::book_start`].
    pub fn admit(self, capacity: &StorageCapacity, need: u64) -> StartBooking {
        self.admit_on(capacity, &[(self.root, self.root, need)])
    }

    // A usenet job would start: each `(judged, promised, need)` is judged
    // by a fresh, good reading of the `judged` root and promised on the
    // `promised` root, the one its bytes land against and are given back
    // to; all, or none. Roots on one filesystem are judged once, for the sum
    // of their needs. See [`DiskLedger::book_start_on`].
    pub fn admit_on(
        self,
        capacity: &StorageCapacity,
        parts: &[(StorageRoot, StorageRoot, u64)],
    ) -> StartBooking {
        let readers: Vec<CapacityReader> = parts
            .iter()
            .map(|(judged, ..)| capacity.reader(*judged))
            .collect();
        let parts: Vec<StartPart<'_>> = parts
            .iter()
            .zip(&readers)
            .map(|((judged, promised, need), reader)| StartPart {
                judge: capacity.ledger(*judged),
                reader,
                fresh: capacity.fresh(*judged),
                held: held_on(capacity, *judged),
                promise: capacity.ledger(*promised),
                need: *need,
            })
            .collect();
        DiskLedger::book_start_on(&parts, self.reserve)
    }

    // The most a start on this root could ever be promised: what a fresh,
    // good reading says the disk holds above the reserve. `None` without
    // one. It takes no lock and books nothing, so a walk reads it once and
    // turns away every job larger than it without asking the ledger.
    pub fn most(self, capacity: &StorageCapacity) -> Option<u64> {
        if !capacity.fresh(self.root) {
            return None;
        }
        capacity
            .reader(self.root)
            .current()
            .reading()
            .filter(|reading| !reading.stale)
            .map(|reading| reading.total_bytes.saturating_sub(self.reserve))
    }

    // The room a start on this root would be judged by now, `None` without
    // a fresh, good reading. It books nothing.
    pub fn room_now(self, capacity: &StorageCapacity) -> Option<u64> {
        capacity.ledger(self.root).start_room_now(
            &capacity.reader(self.root),
            self.reserve,
            held_on(capacity, self.root),
            capacity.fresh(self.root),
        )
    }

    // Whether the root's cached reading is a good one with no room over the
    // reserve. It reads the cache and begins nothing.
    pub fn short(self, capacity: &StorageCapacity) -> bool {
        self.reserve > 0
            && capacity
                .free(self.root)
                .is_some_and(|free| !free.stale && free.bytes <= self.reserve)
    }

    // Whether a job with bytes to fetch is kept from starting: while `held`,
    // or once the disk shows short, which begins the hold.
    pub fn hold(self, capacity: &StorageCapacity, held: &mut bool) -> bool {
        if *held {
            return true;
        }
        let short = self.short(capacity);
        if short {
            *held = true;
            capacity.ledger(self.root).wait();
        }
        short
    }

    // A reading came while jobs are kept from starting: a good one with
    // room over the reserve, or none at all, ends the hold. Returns whether
    // it ended, so the caller starts them.
    pub fn release(self, capacity: &StorageCapacity, held: &mut bool) -> bool {
        if !*held {
            return false;
        }
        let room = capacity
            .free(self.root)
            .is_none_or(|free| !free.stale && free.bytes > self.reserve);
        if room {
            *held = false;
            capacity.ledger(self.root).unwait();
        }
        room
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::operations::disk::DiskSpace;

    // The promises one kind's ended stay bounded though the other kind
    // never takes them, as on an install with no torrents: one per disk
    // and device, holding the latest count, and a path's only while a
    // wait reads it.
    #[test]
    fn ended_promises_stay_bounded_when_the_other_kind_never_takes_them() {
        let dir = tempfile::tempdir().unwrap();
        let capacity = StorageCapacity::with_probe(
            [0, 1, 2].map(|_| dir.path().to_path_buf()),
            Duration::from_secs(365 * 24 * 60 * 60),
            Arc::new(|_: &Path| {
                Ok(DiskSpace {
                    total_bytes: 1 << 40,
                    available_bytes: 1 << 30,
                })
            }),
        );
        let promises = capacity.promises();
        let path = dir.path().join("off");
        capacity.hold_path(&path);
        let root = Disk::Root(StorageRoot::Complete);
        for job in 0..1_000 {
            for disk in [root.clone(), Disk::Path(path.clone())] {
                let promised = Promised {
                    disk,
                    bytes: 1,
                    device: None,
                };
                promises.insert(Waiter::Usenet, JobId(job), promised);
                assert!(
                    promises
                        .end(&capacity, Waiter::Usenet, JobId(job))
                        .is_some()
                );
            }
        }
        // No wait reads the path any more: its entry goes at the next end.
        capacity.release_path(&path);
        let promised = Promised {
            disk: root.clone(),
            bytes: 1,
            device: None,
        };
        promises.insert(Waiter::Usenet, JobId(0), promised);
        promises.end(&capacity, Waiter::Usenet, JobId(0));
        let taken = promises.take_ended(Waiter::Torrent);
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].0.disk, root);
        assert_eq!(Some(taken[0].1), root.begun(&capacity));
        assert!(promises.take_ended(Waiter::Usenet).is_empty());
    }

    // An end on a path no wait reads any more is still heard by a wait on
    // a root of the same device: it stays as that device's.
    #[test]
    fn an_end_on_a_path_no_one_reads_reaches_a_wait_on_its_device() {
        let complete = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        crate::runtime::fs::test_devices::assign(complete.path(), 74_001);
        crate::runtime::fs::test_devices::assign(other.path(), 74_002);
        let capacity = StorageCapacity::with_probe(
            // Data, working, complete.
            [
                other.path().to_path_buf(),
                other.path().to_path_buf(),
                complete.path().to_path_buf(),
            ],
            Duration::from_secs(365 * 24 * 60 * 60),
            Arc::new(|_: &Path| {
                Ok(DiskSpace {
                    total_bytes: 1 << 40,
                    available_bytes: 1 << 30,
                })
            }),
        );
        for root in StorageRoot::ALL {
            capacity.wait_for_probes(root, 1);
        }
        let promises = capacity.promises();
        let path = complete.path().join("off");
        capacity.hold_path(&path);
        let root = Disk::Root(StorageRoot::Complete);
        // A torrent's promise on the path ends; then the path's last wait.
        promises.insert(
            Waiter::Torrent,
            JobId(1),
            Promised {
                disk: Disk::Path(path.clone()),
                bytes: 1,
                device: Some(74_001),
            },
        );
        promises.end(&capacity, Waiter::Torrent, JobId(1)).unwrap();
        capacity.release_path(&path);
        // Another end compacts the list.
        promises.insert(
            Waiter::Torrent,
            JobId(2),
            Promised {
                disk: Disk::Root(StorageRoot::Intermediate),
                bytes: 1,
                device: Some(74_002),
            },
        );
        promises.end(&capacity, Waiter::Torrent, JobId(2)).unwrap();

        let taken = promises.take_ended(Waiter::Usenet);
        assert_eq!(taken.len(), 2);
        // A wait on the complete root hears the path's end by its device.
        let device = root.device(&capacity);
        assert_eq!(device, Some(74_001));
        let heard: Vec<&Promised> = taken
            .iter()
            .map(|(ended, _)| ended)
            .filter(|ended| ended.on(&root, device))
            .collect();
        assert_eq!(heard.len(), 1);
        assert_eq!(heard[0].disk, Disk::Path(path));
    }

    // A small fixed generator, so every run sees the same seeded promises.
    struct Seeded(u64);

    impl Seeded {
        fn next(&mut self, below: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % below
        }
    }

    // Over seeded promises of both kinds, on roots and paths that share
    // devices and ones whose device is not known, some also on a ledger, the
    // totals, of all and of those off the ledger, give what the walk over
    // every promise gave, for every disk and device after each promise made,
    // replaced, ended, taken back or dropped by `retain`.
    #[test]
    fn promised_totals_match_the_walk_over_every_promise() {
        let disks = [
            Disk::Root(StorageRoot::Data),
            Disk::Root(StorageRoot::Intermediate),
            Disk::Root(StorageRoot::Complete),
            Disk::Path(PathBuf::from("/mnt/a")),
            Disk::Path(PathBuf::from("/mnt/b")),
        ];
        const DEVICES: [Option<u64>; 3] = [None, Some(1), Some(2)];
        let check = |promises: &RoomPromises| {
            for disk in &disks {
                for device in DEVICES {
                    assert_eq!(
                        promises.on(disk, device),
                        promises.on_by_walk(disk, device, false),
                        "{disk:?} {device:?}"
                    );
                    assert_eq!(
                        promises.off_ledger(disk, device),
                        promises.on_by_walk(disk, device, true),
                        "{disk:?} {device:?} off the ledger"
                    );
                }
            }
        };
        let mut seeded = Seeded(0x9e37_79b9_7f4a_7c15);
        let mut changes = 0;
        for _ in 0..200 {
            let promises = RoomPromises::default();
            for _ in 0..seeded.next(120) {
                let waiter = [Waiter::Usenet, Waiter::Torrent][seeded.next(2) as usize];
                // A small id space, so promises are replaced as well as made.
                let job_id = JobId(seeded.next(40));
                match seeded.next(6) {
                    0 => {
                        promises.remove(waiter, job_id);
                    }
                    1 => {
                        let cut = seeded.next(40);
                        promises.retain(waiter, |job_id| job_id.0 < cut);
                    }
                    _ => {
                        let bytes = match seeded.next(10) {
                            0 => u64::MAX - seeded.next(1_000),
                            _ => seeded.next(1 << 30),
                        };
                        let promised = Promised {
                            disk: disks[seeded.next(disks.len() as u64) as usize].clone(),
                            bytes,
                            device: DEVICES[seeded.next(3) as usize],
                        };
                        // Some are promised on a ledger as well.
                        if seeded.next(3) == 0 {
                            promises.insert_ledgered(waiter, job_id, promised);
                        } else {
                            promises.insert(waiter, job_id, promised);
                        }
                    }
                }
                changes += 1;
                check(&promises);
            }
            // Taken back one at a time, the totals end empty.
            for waiter in [Waiter::Usenet, Waiter::Torrent] {
                for job in 0..40 {
                    promises.remove(waiter, JobId(job));
                    check(&promises);
                }
            }
            let book = promises.locked();
            assert!(book.promised.is_empty());
            assert!(book.totals.disk.is_empty());
            assert!(book.totals.device.is_empty());
            assert!(book.totals.both.is_empty());
            assert!(book.ledgered.is_empty());
            assert!(book.off_ledger.disk.is_empty());
            assert!(book.off_ledger.device.is_empty());
            assert!(book.off_ledger.both.is_empty());
        }
        assert!(changes > 5_000, "{changes}");
    }
}
