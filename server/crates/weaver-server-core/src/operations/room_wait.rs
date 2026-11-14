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

use std::path::PathBuf;

use super::disk::{StorageCapacity, StorageRoot};

// The disk a wait is on: a root's, or the disk of a path that lies on none
// of theirs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Disk {
    Root(StorageRoot),
    Path(PathBuf),
}

impl Disk {
    // The device the capacity layer holds for this disk already; nothing is
    // looked at.
    pub(crate) fn device(&self, capacity: &StorageCapacity) -> Option<u64> {
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
pub(crate) struct Wants {
    // What the step needs free to run again, when it removed what it had
    // written as it failed, or never wrote it.
    pub(crate) needs: Option<u64>,
    // What was left to write of the file, spool or scratch whose write was
    // refused, when the step names no `needs`: the least a reading has to
    // show over the mark.
    pub(crate) left: Option<u64>,
    // The write was refused by a quota, not by the disk: the disk's free
    // figure already shows room the writer cannot use.
    pub(crate) quota: bool,
}

impl Wants {
    // The write that failed with `error` wants `needs` free to run again.
    pub(crate) fn of(error: &std::io::Error, needs: Option<u64>) -> Self {
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

pub(crate) struct RoomWait {
    // The least a reading has to show over the mark, when the step names
    // no `needs`: what was left of the file whose write was refused.
    pub(crate) rise: Option<u64>,
    // The wait began with a quota error: the free figure has to rise from
    // the mark by what the step needs, since it showed that much already.
    pub(crate) quota: bool,
    pub(crate) disk: Disk,
    // The disk's count of the probes it had begun as the write failed: only
    // a later probe's reading can show room made since.
    pub(crate) since: u64,
    // What the disk showed free when the write failed: the figure of its
    // newest reading whose probe began no later than the failure, even a
    // stale one. A reading has to show more than this to show room made.
    // It is this wait's alone, and ends with it. `None` while the disk has
    // shown nothing: the first reading since the failure then sets it.
    pub(crate) mark: Option<u64>,
    // What the waiter itself gave back to the disk after the mark was
    // taken. A reading shows it as room, and it is room the waiter fills
    // again, so it counts on top of the mark.
    pub(crate) freed: u64,
    // What the step that failed needs free to run again, when it removed
    // what it had written as it failed. A reading then has to show this
    // much, and the mark is not consulted.
    pub(crate) needs: Option<u64>,
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
    pub(crate) fn begin(
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
    pub(crate) fn judge(
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

    // The wait ends: the waiter went on, or is gone.
    pub(crate) fn end(&self, capacity: &StorageCapacity) {
        match &self.disk {
            Disk::Root(root) => capacity.ledger(*root).unwait(),
            Disk::Path(path) => capacity.release_path(path),
        }
    }
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
pub(crate) struct StartCheck {
    pub(crate) root: StorageRoot,
    pub(crate) reserve: u64,
}

impl StartCheck {
    // Whether the root's cached reading is a good one with no room over the
    // reserve. It reads the cache and begins nothing.
    pub(crate) fn short(self, capacity: &StorageCapacity) -> bool {
        self.reserve > 0
            && capacity
                .free(self.root)
                .is_some_and(|free| !free.stale && free.bytes <= self.reserve)
    }

    // Whether a job with bytes to fetch is kept from starting: while `held`,
    // or once the disk shows short, which begins the hold.
    pub(crate) fn hold(self, capacity: &StorageCapacity, held: &mut bool) -> bool {
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
    pub(crate) fn release(self, capacity: &StorageCapacity, held: &mut bool) -> bool {
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
