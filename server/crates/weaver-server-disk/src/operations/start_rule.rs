// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The one start rule every job class starts by: a job starts only when what
// it still has to fetch fits on its disks by a fresh reading, less every
// promise there, and is promised as it starts. A job refused waits, and its
// wait ends only when a start could succeed now. Which jobs are walked, in
// what order, and how a held job shows stays with each class.

use crate::operations::disk::{StartBooking, StorageCapacity, StorageRoot};
use crate::operations::room_wait::StartCheck;

// Why a job did not start: its bytes do not fit in the `room` the disk
// leaves now, `None` without a fresh reading; or they never fit, the disk
// holding at `most` that much above its reserve.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartRefused {
    Short { need: u64, room: Option<u64> },
    TooSmall { need: u64, most: u64 },
}

impl StartRefused {
    // A refused start from the start check's answer: `None` when it was
    // booked. No fresh reading is a shortage of `need` with no room known.
    pub fn of(answer: StartBooking, need: u64) -> Option<Self> {
        match answer {
            StartBooking::Booked => None,
            StartBooking::Short { room, need, .. } => Some(Self::Short {
                need,
                room: Some(room),
            }),
            StartBooking::TooLarge { most, need, .. } => Some(Self::TooSmall { need, most }),
            StartBooking::Unread => Some(Self::Short { need, room: None }),
        }
    }

    // What a job kept for the disk shows: what it needs, and what the disk
    // leaves, or the most it ever holds.
    pub fn figures(self) -> (Option<u64>, Option<u64>) {
        match self {
            Self::Short { need, room } => (Some(need), room),
            Self::TooSmall { need, most } => (Some(need), Some(most)),
        }
    }

    // Whether the job never fits on the disk as it stands.
    pub fn too_small(self) -> bool {
        matches!(self, Self::TooSmall { .. })
    }
}

// The most the disks could ever promise a start: `working` above the
// reserve on the working disk, `output` on the output disk when it is
// another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StartMost {
    working: u64,
    output: Option<u64>,
}

impl StartMost {
    // Why a job with `need` still to fetch and `size` to deliver can never
    // start on these disks, if it cannot.
    pub fn refuses(self, need: u64, size: u64) -> Option<StartRefused> {
        if need > self.working {
            return Some(StartRefused::TooSmall {
                need,
                most: self.working,
            });
        }
        let output = self.output?;
        let size = size.max(need);
        (size > output).then_some(StartRefused::TooSmall {
            need: size,
            most: output,
        })
    }

    // Whether these disks hold more than `before` did: a job turned away
    // for its size may fit now.
    pub fn grew_from(self, before: Option<Self>) -> bool {
        before.is_some_and(|before| {
            self.working > before.working || self.output.unwrap_or(0) > before.output.unwrap_or(0)
        })
    }
}

// Where a class's jobs start: the root their bytes are fetched to, the one
// they deliver to, and the reserve each keeps. A class whose jobs fetch and
// deliver on one root names it twice.
#[derive(Clone, Copy, Debug)]
pub struct StartRule {
    pub working: StorageRoot,
    pub output: StorageRoot,
    pub reserve: u64,
}

impl StartRule {
    // The start check on the working disk.
    pub fn check(self) -> StartCheck {
        StartCheck {
            root: self.working,
            reserve: self.reserve,
            waiters: true,
        }
    }

    fn output_check(self) -> StartCheck {
        StartCheck {
            root: self.output,
            reserve: self.reserve,
            waiters: true,
        }
    }

    // Whether the output root lies on another disk than the working one: a
    // job's bytes may land on either, so a start is promised on both.
    pub fn other_disk(self, capacity: &StorageCapacity) -> bool {
        !capacity
            .ledger(self.working)
            .same_disk(capacity.ledger(self.output))
    }

    // `need` bytes still to fetch are promised for a job that would start,
    // on the working disk, and its `size` on the output disk when that is
    // another, when they fit there by a fresh reading; judged on `read`
    // when both roots share one disk. What was promised on each, working
    // and output.
    pub fn book_on(
        self,
        capacity: &StorageCapacity,
        read: StorageRoot,
        need: u64,
        size: u64,
    ) -> Result<(u64, u64), StartBooking> {
        self.book_judged(self.check(), capacity, read, need, size)
    }

    // [`Self::book_on`] for a job held for a full disk starting again: it
    // is one of those whose wait a new start leaves aside, so its start is
    // judged without them.
    pub fn book_held_on(
        self,
        capacity: &StorageCapacity,
        read: StorageRoot,
        need: u64,
        size: u64,
    ) -> Result<(u64, u64), StartBooking> {
        let check = StartCheck {
            waiters: false,
            ..self.check()
        };
        self.book_judged(check, capacity, read, need, size)
    }

    fn book_judged(
        self,
        check: StartCheck,
        capacity: &StorageCapacity,
        read: StorageRoot,
        need: u64,
        size: u64,
    ) -> Result<(u64, u64), StartBooking> {
        let other = self.other_disk(capacity);
        let output = if other { size.max(need) } else { 0 };
        let answer = if other {
            check.admit_on(
                capacity,
                &[
                    (self.working, self.working, need),
                    (self.output, self.output, output),
                ],
            )
        } else {
            check.admit_on(capacity, &[(read, self.working, need)])
        };
        match answer {
            StartBooking::Booked => Ok((need, output)),
            refused => Err(refused),
        }
    }

    // `need` is promised on the working disk, and `size` on the output disk
    // when that is another, with nothing judged: the job's room was judged
    // otherwise. What was promised on each.
    pub fn promise_unjudged(self, capacity: &StorageCapacity, need: u64, size: u64) -> (u64, u64) {
        let output = if self.other_disk(capacity) {
            size.max(need)
        } else {
            0
        };
        capacity.ledger(self.working).promise(need);
        capacity.ledger(self.output).promise(output);
        (need, output)
    }

    // Bytes promised for a start that did not take its slot go back.
    pub fn give_back(self, capacity: &StorageCapacity, working: u64, output: u64) {
        capacity.ledger(self.working).release(working);
        capacity.ledger(self.output).release(output);
    }

    // The most a start could ever be promised on the working disk, and on
    // the output disk when that is another, by fresh, good readings: `None`
    // while either has none, and the ledgers judge.
    pub fn most(self, capacity: &StorageCapacity) -> Option<StartMost> {
        let working = self.check().most(capacity)?;
        let output = match self.other_disk(capacity) {
            true => Some(self.output_check().most(capacity)?),
            false => None,
        };
        Some(StartMost { working, output })
    }
}

// A class's wait for room to start a job it refused: begun as a walk keeps
// a job for the disk, ended once a start could succeed now.
#[derive(Debug)]
pub struct StartWait {
    held: bool,
    // The output disk was waited on as well, being another.
    waits_output: bool,
    // By disk, working and output: the least a job refused there needs.
    need_min: [u64; 2],
    // A refusal found no fresh reading.
    unread: bool,
    // Promises given back on the disks waited on, at the walk.
    releases: u64,
    most: Option<StartMost>,
}

impl Default for StartWait {
    fn default() -> Self {
        Self {
            held: false,
            waits_output: false,
            need_min: [u64::MAX; 2],
            unread: false,
            releases: 0,
            most: None,
        }
    }
}

impl StartWait {
    // Whether a job is kept for the disk.
    pub fn held(&self) -> bool {
        self.held
    }

    // The disks the hold waits on begin or stop being waited on, so their
    // readings, and the promises given back, ring.
    pub fn wait(&mut self, capacity: &StorageCapacity, rule: StartRule, held: bool) {
        if held == self.held {
            return;
        }
        self.held = held;
        if held {
            self.waits_output = rule.other_disk(capacity);
            capacity.ledger(rule.working).wait();
            if self.waits_output {
                capacity.ledger(rule.output).wait();
            }
        } else {
            capacity.ledger(rule.working).unwait();
            if self.waits_output {
                capacity.ledger(rule.output).unwait();
            }
            self.waits_output = false;
        }
    }

    // A walk ended: the hold begins, or goes on, while a job was `kept` for
    // the disk, and ends once none was. A kept job's walk leaves the least
    // a refused job needs on each disk, whether a refusal found no fresh
    // reading, and the most the disks could hold.
    pub fn walked(
        &mut self,
        capacity: &StorageCapacity,
        rule: StartRule,
        kept: bool,
        (need_min, unread, most): ([u64; 2], bool, Option<StartMost>),
    ) {
        self.wait(capacity, rule, kept);
        if kept {
            self.need_min = need_min;
            self.unread = unread;
            self.releases = self.releases_now(capacity, rule);
            self.most = most;
        }
    }

    // A job judged on its own, not in a walk, was `refused`: the hold
    // begins, or goes on, waking for the least any job kept short needs.
    pub fn kept(&mut self, capacity: &StorageCapacity, rule: StartRule, refused: StartRefused) {
        let held = self.held;
        let mut need_min = if held {
            self.need_min
        } else {
            [u64::MAX; 2]
        };
        let mut unread = held && self.unread;
        if let StartRefused::Short { need, room } = refused {
            need_min[0] = need_min[0].min(need);
            unread |= room.is_none();
        }
        self.walked(capacity, rule, true, (need_min, unread, rule.most(capacity)));
    }

    // How many times a promise was given back on the disks a start is
    // judged by. A promise that lands moves nothing here: it frees no room.
    fn releases_now(&self, capacity: &StorageCapacity, rule: StartRule) -> u64 {
        let working = capacity.ledger(rule.working).releases();
        if self.waits_output {
            working.saturating_add(capacity.ledger(rule.output).releases())
        } else {
            working
        }
    }

    // A disk rang while jobs are kept from starting for it: whether a walk
    // is due, which ends the hold. One is due only when a job could start
    // now, that is when a fresh reading of a disk a job was refused on
    // leaves room for the least any job refused there needs on it, when the
    // first fresh reading came to a hold begun without one, when a promise
    // was given back, or when the disks grew, for a job turned away for its
    // size. Bytes that land as promised bring none: they free no room. Each
    // is one look at a cached reading or a count.
    pub fn ring(&mut self, capacity: &StorageCapacity, rule: StartRule) -> bool {
        if !self.held {
            return false;
        }
        let check = rule.check();
        let output = rule.output_check();
        let fresh = capacity.fresh(rule.working)
            && (!self.waits_output || capacity.fresh(rule.output));
        // Each disk's room against the least a job refused on that disk
        // needs there.
        let [working_need, output_need] = self.need_min;
        let room = check
            .room_now(capacity)
            .is_some_and(|room| room >= working_need)
            || (self.waits_output
                && output
                    .room_now(capacity)
                    .is_some_and(|room| room >= output_need));
        let releases = self.releases_now(capacity, rule);
        let given_back = releases != self.releases;
        self.releases = releases;
        let grew = rule
            .most(capacity)
            .is_some_and(|most| most.grew_from(self.most));
        let ring = (self.unread && fresh) || given_back || grew || room;
        if ring {
            self.wait(capacity, rule, false);
        }
        ring
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::operations::disk::DiskSpace;

    const RESERVE: u64 = 1 << 20;
    // What a job held for a full disk waits for.
    const WAITS: u64 = 60;

    // One disk under every root, read with `RESERVE + 100` free.
    fn one_disk(dir: &Path) -> StorageCapacity {
        let capacity = StorageCapacity::with_probe(
            [0, 1, 2].map(|_| dir.to_path_buf()),
            Duration::from_secs(365 * 24 * 60 * 60),
            Arc::new(|_: &Path| {
                Ok(DiskSpace {
                    total_bytes: 1 << 40,
                    available_bytes: RESERVE + 100,
                })
            }),
        );
        for root in StorageRoot::ALL {
            capacity.wait_for_probes(root, 1);
        }
        capacity
    }

    fn rule() -> StartRule {
        StartRule {
            working: StorageRoot::Intermediate,
            output: StorageRoot::Intermediate,
            reserve: RESERVE,
        }
    }

    // A held job waiting for N keeps out a new start of S when free less
    // the reserve less N is under S, and lets one in that fits beside N.
    // The held job's own start again is not kept out by it, and neither is
    // a write.
    #[test]
    fn a_new_start_leaves_aside_what_a_held_job_waits_for() {
        let dir = tempfile::tempdir().unwrap();
        let capacity = one_disk(dir.path());
        let ledger = capacity.ledger(StorageRoot::Intermediate);
        ledger.hold_waiting(WAITS);

        let root = StorageRoot::Intermediate;
        assert_eq!(
            rule().book_on(&capacity, root, 41, 41),
            Err(StartBooking::Short {
                room: 40,
                need: 41,
                part: 0
            }),
            "100 free past the reserve, 60 of it waited for"
        );
        assert_eq!(rule().check().room_now(&capacity), Some(40));
        assert_eq!(rule().book_on(&capacity, root, 40, 40), Ok((40, 0)));
        rule().give_back(&capacity, 40, 0);

        assert_eq!(
            rule().book_held_on(&capacity, root, 100, 100),
            Ok((100, 0)),
            "a held job's start again is judged without the waits"
        );
        rule().give_back(&capacity, 100, 0);
        let reader = capacity.reader(root);
        assert_eq!(
            ledger.book(&reader, RESERVE, 100, 0),
            Some(0),
            "a write may take the room a held job waits for"
        );
        ledger.release(100);
    }

    // Let go and promised its figure, the held job's wait comes off in the
    // same step: a start counts the room once.
    #[test]
    fn a_held_job_let_go_is_counted_once() {
        let dir = tempfile::tempdir().unwrap();
        let capacity = one_disk(dir.path());
        let ledger = capacity.ledger(StorageRoot::Intermediate);
        ledger.hold_waiting(WAITS);
        assert_eq!(rule().check().room_now(&capacity), Some(100 - WAITS));

        ledger.promise(WAITS);
        ledger.end_waiting(WAITS);
        assert_eq!(ledger.waiting(), 0);
        assert_eq!(
            rule().check().room_now(&capacity),
            Some(100 - WAITS),
            "promised, not also waited for"
        );
        ledger.release(WAITS);
        assert_eq!(rule().check().room_now(&capacity), Some(100));
    }

    // A start kept out by a held job's wait is woken when that wait ends,
    // as by a promise given back, though the disk reads as before.
    #[test]
    fn a_start_waiting_wakes_when_the_hold_ends() {
        let dir = tempfile::tempdir().unwrap();
        let capacity = one_disk(dir.path());
        let ledger = capacity.ledger(StorageRoot::Intermediate);
        ledger.hold_waiting(WAITS);
        let refused = StartRefused::of(
            rule()
                .book_on(&capacity, StorageRoot::Intermediate, 41, 41)
                .unwrap_err(),
            41,
        )
        .unwrap();
        let mut wait = StartWait::default();
        wait.kept(&capacity, rule(), refused);
        assert!(wait.held());
        assert!(!wait.ring(&capacity, rule()), "nothing changed");

        ledger.end_waiting(WAITS);
        assert!(wait.ring(&capacity, rule()), "the hold ended");
        assert!(!wait.held());
        assert!(!ledger.waited_on());
    }
}
