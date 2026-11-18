// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::live::Live;
use super::{TorrentService, files_dir, locked};
use crate::jobs::JobId;
use crate::operations::disk::{CapacityReader, StorageCapacity, StorageRoot};
use crate::operations::room_wait::{
    Disk, LedgerWait, Promised, RoomWait, StartCheck, Waiter, Wants, ever_downloaded,
};
use crate::torrents::intake::Intake;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

// How a torrent waits for room on the disk its folder lies on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DiskHeld {
    // A piece found no room: its session is in upload mode, fetching
    // nothing and seeding, until a reading shows room made or its user
    // resumes it.
    Writing,
    // It has bytes to fetch and its turn came while the disk had no room
    // over the reserve: it stays paused, as a usenet job is kept from
    // starting, until a reading shows room. `Inner::start_held` holds the
    // check's latch.
    Starting,
}

// What a torrent's write that found no room was refused by: the disk
// itself, or the ledger every writer on the disk books on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NoRoom {
    Disk(Wants),
    Ledger(u64),
}

// The run a wait began in. The weak pointer keeps the run's allocation,
// so no later run can share its address: a wait is the current run's only
// while that run is the one it began in.
pub(super) struct RunMark(std::sync::Weak<dyn std::any::Any + Send + Sync>);

impl RunMark {
    fn of<T: Send + Sync + 'static>(run: &Arc<T>) -> Self {
        let run: Arc<dyn std::any::Any + Send + Sync> = Arc::clone(run) as _;
        Self(Arc::downgrade(&run))
    }

    fn is<T>(&self, run: &Arc<T>) -> bool {
        std::ptr::addr_eq(self.0.as_ptr(), Arc::as_ptr(run))
    }

    fn same(&self, other: &RunMark) -> bool {
        std::sync::Weak::ptr_eq(&self.0, &other.0)
    }
}

// A torrent's wait for room, by the wait a usenet job shares for the same
// refusal: a write the disk refused waits for a reading that shows room
// made, as a usenet job held for a full disk does; a booking the ledger
// refused waits on the ledger, as a usenet job's refused booking does.
pub(super) enum Waiting {
    Room(RoomWait),
    Ledger(LedgerWait, CapacityReader),
    // A piece was refused while the disk of its folder was not known, or
    // was known for a folder it no longer goes to: it waits for that disk
    // to be known, then on its ledger for what it needed; on a disk off
    // the roots', which books nothing, it goes on.
    Place(u64),
}

pub(super) struct DiskWait {
    pub(super) how: Waiting,
    run: RunMark,
}

impl DiskWait {
    // The wait ends with nothing promised.
    fn end(&self, capacity: &StorageCapacity) {
        match &self.how {
            Waiting::Room(wait) => wait.end(capacity),
            Waiting::Ledger(wait, reader) => wait.end(reader.ledger()),
            Waiting::Place(_) => {}
        }
    }
}

// Where a torrent whose turn lets it run stands at the start check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StartHold {
    // It may start.
    Free,
    // Its disk has no room over the reserve: it waits for room, shown so.
    Short,
    // It has bytes to fetch and the disk of its folder is being looked
    // up: it starts once it is known, so no piece meets a disk unknown.
    Unplaced,
}

// The index of a root among the start check's latches.
fn root_index(root: StorageRoot) -> usize {
    StorageRoot::ALL
        .iter()
        .position(|each| *each == root)
        .unwrap_or_default()
}

// The torrents whose disk is to be looked up again at the next reading,
// the looks a reading began that are still out, and whether the complete
// root's ledger is waited on so a reading comes. The wait is kept while a
// torrent is listed or a look a reading began is out, so a look that
// fails again waits for the next reading and does not ask for one.
#[derive(Default)]
pub(super) struct Relook {
    pub(super) jobs: BTreeSet<JobId>,
    out: usize,
    pub(super) waiting: bool,
}

// A look for a torrent's disk on its way. However it ends, panicking
// included, the torrent is never left with a look marked out that no one
// makes: one that found no place ends the look and is looked up again at
// the next reading; one that found its place says so to whoever waits.
struct PlaceLook {
    service: TorrentService,
    job_id: JobId,
    intake: Arc<Intake>,
    placed: bool,
    // A reading began this look.
    again: bool,
    // The batch whose looks ask one pass between them, when it is one.
    batch: Option<Arc<PlaceBatch>>,
}

// The looks begun together for many torrents (a batch of moves, a
// reading's looks again, a restore pass, a pass of the queue): the pass
// their torrents wait for is asked once, as the last of them ends, not
// once a look, so many looks are one pass and one publish of every
// torrent, whenever they end (libtorrent `trigger_auto_manage`, posted
// once).
pub(super) struct PlaceBatch {
    service: TorrentService,
    placed: std::sync::atomic::AtomicBool,
}

impl PlaceBatch {
    pub(super) fn new(service: &TorrentService) -> Arc<Self> {
        Arc::new(Self {
            service: service.clone(),
            placed: std::sync::atomic::AtomicBool::new(false),
        })
    }

    // Every look ended: whether one placed its torrent, the pass it waits
    // for then the caller's to make, and asked of no one else.
    pub(super) fn take_placed(self) -> bool {
        self.placed.swap(false, Ordering::AcqRel)
    }
}

impl Drop for PlaceBatch {
    fn drop(&mut self) {
        if self.placed.load(Ordering::Acquire) {
            self.service.inner.signal.want_pass();
        }
    }
}

impl PlaceLook {
    // The look ended with the torrent's place known, or none to find.
    fn found(&mut self) {
        self.placed = true;
    }
}

impl Drop for PlaceLook {
    fn drop(&mut self) {
        if self.placed {
            self.service.relooked(self.again);
            // A torrent kept for its disk to be known is judged at the next
            // pass, and a piece that waited for it at the next wake. A
            // batch's looks ask it once, as the last ends.
            match &self.batch {
                Some(batch) => batch.placed.store(true, Ordering::Release),
                None => self.service.inner.signal.want_pass(),
            }
        } else {
            self.intake.room().end_placing();
            // A look the stop ended is looked again by no one: no reading
            // is waited for after the stop.
            if !self.service.inner.stopping.load(Ordering::Acquire) {
                self.service.place_later(self.job_id, self.again);
            }
        }
        // The batch's pass, when this was its last look, is asked before
        // the look is seen ended.
        drop(self.batch.take());
        self.service
            .inner
            .disk_waits_moved
            .send_modify(|moved| *moved += 1);
    }
}

impl TorrentService {
    // A torrent waiting for room is judged at each reading of its disk, as
    // a usenet job held for a full disk is: a reading that shows room made
    // lets it fetch again. Nothing runs while none waits, and a pass that
    // lets none go writes nothing and publishes nothing.
    pub(super) fn spawn_follow_disk_room(&self) {
        let Some(capacity) = self.inner.env.capacity.clone() else {
            return;
        };
        let service = self.clone();
        let mut moved = self.inner.disk_waits_moved.subscribe();
        self.keep(tokio::spawn(async move {
            let mut rings = capacity.rings();
            loop {
                let rung = capacity.rung();
                tokio::pin!(rung);
                rung.as_mut().enable();
                moved.borrow_and_update();
                // Only a reading looks again, so a look that keeps failing
                // is made once a reading, never in a loop of its own. The
                // count of rings, read once the next ring is listened for,
                // keeps a ring between two turns from being missed.
                let now = capacity.rings();
                if std::mem::replace(&mut rings, now) != now {
                    service.place_again_at_reading().await;
                }
                if !locked(&service.inner.disk_waits).is_empty() {
                    service.wake_disk_waits(&capacity).await;
                }
                if service.release_start_holds(&capacity) {
                    tracing::info!(
                        "room was made on the disk; torrents with bytes to fetch may start"
                    );
                    service.inner.signal.want_pass();
                }
                tokio::select! {
                    () = &mut rung => {}
                    _ = moved.changed() => {}
                }
            }
        }));
    }

    // A reading came: each root's start hold whose disk shows room, or no
    // reading, ends. Whether one did.
    fn release_start_holds(&self, capacity: &StorageCapacity) -> bool {
        let reserve = self.inner.env.limits.min_free_bytes;
        let mut held = locked(&self.inner.start_held);
        if !held.iter().any(|held| *held) {
            return false;
        }
        let mut released = false;
        for root in StorageRoot::ALL {
            released |= StartCheck { root, reserve }.release(capacity, &mut held[root_index(root)]);
        }
        released
    }

    // Where `torrent`, whose turn lets it run, stands at the start check
    // on the disk its folder lies on. One with bytes to fetch waits for
    // that disk to be known, since its pieces book there. Then one that
    // never downloaded is kept from starting while the disk has no room
    // over the reserve, as a usenet job that never downloaded is; one that
    // downloaded, or is complete, starts. "Never downloaded" is read from
    // the bytes its ledger counts as downloaded, over every run. A disk off
    // the roots' is not checked.
    pub(super) fn start_hold(&self, torrent: &Live) -> StartHold {
        self.start_hold_in(torrent, None)
    }

    // `start_hold`, a look it begins one of `batch`'s when it is one.
    pub(super) fn start_hold_in(
        &self,
        torrent: &Live,
        batch: Option<&Arc<PlaceBatch>>,
    ) -> StartHold {
        if torrent.seeding() {
            return StartHold::Free;
        }
        let (Some(capacity), Some(intake)) = (&self.inner.env.capacity, &torrent.intake) else {
            return StartHold::Free;
        };
        let Some(place) = intake.room().place() else {
            // Looked up once at a time; the store's handover began a look
            // already, unless the torrent was complete then.
            self.look_for_place_in(torrent.record.job_id, intake, files_dir(torrent), batch);
            return StartHold::Unplaced;
        };
        if ever_downloaded(torrent.counters.downloaded_bytes) {
            return StartHold::Free;
        }
        let Disk::Root(root) = place.disk else {
            return StartHold::Free;
        };
        let check = StartCheck {
            root,
            reserve: self.inner.env.limits.min_free_bytes,
        };
        let mut held = locked(&self.inner.start_held);
        let latch = &mut held[root_index(root)];
        let was = *latch;
        let kept = check.hold(capacity, latch);
        drop(held);
        if kept && !was {
            tracing::info!(
                job_id = torrent.record.job_id.0,
                "the disk has no room over its reserve; torrents with bytes to fetch wait to start"
            );
            // The follower reads the latch at its next turn.
            self.inner.disk_waits_moved.send_modify(|moved| *moved += 1);
        }
        if kept {
            StartHold::Short
        } else {
            StartHold::Free
        }
    }

    // Whether `torrent`, whose turn lets it run, is kept from starting.
    pub(super) fn kept_from_starting(&self, torrent: &Live) -> bool {
        self.start_hold(torrent) != StartHold::Free
    }

    // The disk of `dir`, the folder the torrent's pieces go to, is looked
    // up now, unless a look is out, which takes the latest folder, or the
    // last one failed, which the next reading looks again for.
    pub(super) fn look_for_place(&self, job_id: JobId, intake: &Arc<Intake>, dir: PathBuf) {
        self.look_for_place_in(job_id, intake, dir, None);
    }

    // `look_for_place`, the look one of `batch`'s when it is one.
    pub(super) fn look_for_place_in(
        &self,
        job_id: JobId,
        intake: &Arc<Intake>,
        dir: PathBuf,
        batch: Option<&Arc<PlaceBatch>>,
    ) {
        if locked(&self.inner.relook).jobs.contains(&job_id) {
            return;
        }
        if intake.room().begin_placing() {
            self.spawn_place_in(job_id, intake, dir, false, batch);
        }
    }

    // The disk of `dir`, the folder the torrent's pieces go to, is looked
    // up, by the choice a usenet job held for a full disk is placed by: its
    // pieces book there from then on, and a wait for room reads that disk.
    // The folder's device is read once, off the async runtime and under no
    // lock, and the answer is that look's own; nothing is read per piece.
    // A folder the pieces stopped going to while it looked is followed by
    // a look for the one they go to. A look that cannot tell the disk
    // leaves the torrent unplaced, never placed off the roots', and it is
    // looked up again at the next reading. The caller began the look on
    // the torrent's room, and counted it as out when a reading began it.
    pub(super) fn spawn_place(
        &self,
        job_id: JobId,
        intake: &Arc<Intake>,
        dir: PathBuf,
        again: bool,
    ) {
        self.spawn_place_in(job_id, intake, dir, again, None);
    }

    // `spawn_place`, as one of `batch`'s looks when it is one: the pass
    // its torrent waits for is the batch's.
    pub(super) fn spawn_place_in(
        &self,
        job_id: JobId,
        intake: &Arc<Intake>,
        dir: PathBuf,
        again: bool,
        batch: Option<&Arc<PlaceBatch>>,
    ) {
        let mut placing = PlaceLook {
            service: self.clone(),
            job_id,
            intake: Arc::clone(intake),
            placed: false,
            again,
            batch: batch.cloned(),
        };
        let Some(capacity) = self.inner.env.capacity.clone() else {
            // Nothing to look on: no look is left out.
            placing.intake.room().end_placing();
            placing.found();
            return;
        };
        self.keep(tokio::spawn(async move {
            let mut dir = dir;
            loop {
                let found = Disk::look_up(&capacity, &dir).await;
                let room = placing.intake.room();
                if let Some(disk) = &found {
                    let reader = disk.reader(&capacity);
                    room.set_place(crate::torrents::intake::Place::new(
                        disk.clone(),
                        dir.clone(),
                        reader,
                    ));
                }
                match room.finish_placing() {
                    Ok(()) => {
                        placing.found();
                        return;
                    }
                    // This folder's disk could not be told.
                    Err(next) if found.is_none() && next == dir => return,
                    Err(next) => dir = next,
                }
            }
        }));
    }

    // The torrent's disk could not be told: it is looked up again at the
    // next reading of a root's disk, which is asked for now unless one is
    // waited for already.
    fn place_later(&self, job_id: JobId, again: bool) {
        let Some(capacity) = &self.inner.env.capacity else {
            return;
        };
        let mut relook = locked(&self.inner.relook);
        if again {
            relook.out = relook.out.saturating_sub(1);
        }
        relook.jobs.insert(job_id);
        if !relook.waiting {
            relook.waiting = true;
            capacity.ledger(StorageRoot::Complete).wait();
        }
    }

    // A look found its torrent's place: with none listed to look again,
    // and no look a reading began out, readings are no longer waited for.
    fn relooked(&self, again: bool) {
        let Some(capacity) = &self.inner.env.capacity else {
            return;
        };
        let mut relook = locked(&self.inner.relook);
        if again {
            relook.out = relook.out.saturating_sub(1);
        }
        if relook.waiting && relook.jobs.is_empty() && relook.out == 0 {
            relook.waiting = false;
            capacity.ledger(StorageRoot::Complete).unwait();
        }
    }

    // A reading came: each torrent whose disk could not be told is looked
    // up again, unless it is gone or found its place since.
    async fn place_again_at_reading(&self) {
        let jobs = std::mem::take(&mut locked(&self.inner.relook).jobs);
        if jobs.is_empty() {
            self.relooked(false);
            return;
        }
        // The looks of one reading ask one pass between them.
        let batch = PlaceBatch::new(self);
        let live = self.inner.live.lock().await;
        for job_id in jobs {
            let Some(torrent) = live.get(&job_id) else {
                continue;
            };
            let Some(intake) = &torrent.intake else {
                continue;
            };
            if intake.room().place().is_none() && intake.room().begin_placing() {
                locked(&self.inner.relook).out += 1;
                self.spawn_place_in(job_id, intake, files_dir(torrent), true, Some(&batch));
            }
        }
        drop(live);
        drop(batch);
        self.relooked(false);
    }

    // A piece of the torrent found no room on its disk: it waits for room
    // there, by the wait a usenet job shares for the same refusal, while
    // its session keeps it in upload mode, seeding. The wait reads the disk
    // its own folder lies on, as it stands now. A wait left from another
    // run is replaced. With no disks to read, its session tries again by
    // itself.
    pub(super) async fn hold_for_room(&self, job_id: JobId, no_room: NoRoom) {
        let Some(capacity) = &self.inner.env.capacity else {
            return;
        };
        let reserve = self.inner.env.limits.min_free_bytes;
        {
            let mut live = self.inner.live.lock().await;
            let Some(torrent) = live.get_mut(&job_id) else {
                return;
            };
            if torrent.disk_held == Some(DiskHeld::Starting) {
                return;
            }
            let Some(run) = torrent
                .attached
                .as_ref()
                .and_then(|attached| attached.handle.live())
            else {
                return;
            };
            // A run out of upload mode installs no wait: nothing could end
            // it, since a wake reads the run in upload mode. A refusal from
            // before a wake enters nothing in the run, and is spent; one
            // made since enters upload mode as its write returns, and is
            // heard again then.
            if !run.upload_mode() {
                self.hold_in_upload_mode(
                    job_id,
                    RunMark::of(&run),
                    run.upload_mode_entered(),
                    no_room,
                );
                return;
            }
            let dir = files_dir(torrent);
            let place = torrent
                .intake
                .as_ref()
                .and_then(|intake| intake.room().place())
                .filter(|place| place.dir == dir);
            let mut waits = locked(&self.inner.disk_waits);
            if let Some(wait) = waits.get(&job_id) {
                if wait.run.is(&run) {
                    return;
                }
                // Left from a run that ended: the new run's refusal is its
                // own.
                if let Some(old) = waits.remove(&job_id) {
                    old.end(capacity);
                }
            }
            let how = match no_room {
                NoRoom::Disk(wants) => {
                    let disk =
                        place.map_or_else(|| Disk::of_path(capacity, &dir), |place| place.disk);
                    Waiting::Room(RoomWait::begin(capacity, disk, dir, wants, reserve))
                }
                NoRoom::Ledger(bytes) => match place.and_then(|place| place.reader().cloned()) {
                    Some(reader) => {
                        Waiting::Ledger(LedgerWait::begin(reader.ledger(), bytes), reader)
                    }
                    // Its disk is not known for the folder it goes to, or
                    // lies off the roots' and books nothing: it waits for
                    // the look, which the wake reads, and a look is begun
                    // when none is out.
                    None => {
                        if let Some(intake) = &torrent.intake
                            && intake.room().aim(&dir)
                        {
                            self.look_for_place(job_id, intake, dir.clone());
                        }
                        Waiting::Place(bytes)
                    }
                },
            };
            waits.insert(
                job_id,
                DiskWait {
                    how,
                    run: RunMark::of(&run),
                },
            );
            drop(waits);
            torrent.disk_held = Some(DiskHeld::Writing);
            // What a wake handed it and it did not spend goes back: it
            // waits for all its write needs again.
            if let Some(intake) = &torrent.intake {
                intake.room().give_back();
            }
        }
        self.end_room_promise(job_id);
        tracing::info!(
            job_id = job_id.0,
            "a torrent's write found no room; it seeds and waits for room on the disk"
        );
        self.inner.disk_waits_moved.send_modify(|moved| *moved += 1);
    }

    // A refusal heard while its run is not in upload mode is heard again
    // once the run enters it: its write returns after the refusal is said.
    // One the run never enters for, made before a wake, is heard only if a
    // later refusal enters it, and is dropped as the run ends. The task
    // holds nothing of the run while it waits.
    fn hold_in_upload_mode(
        &self,
        job_id: JobId,
        run: RunMark,
        entered: impl std::future::Future<Output = bool> + Send + 'static,
        no_room: NoRoom,
    ) {
        let mine = RunMark(run.0.clone());
        {
            let mut entering = locked(&self.inner.entering);
            if let Some((waiting, held)) = entering.get_mut(&job_id)
                && waiting.same(&run)
            {
                *held = no_room;
                return;
            }
            // One left from a run that ended is replaced: its task finds
            // the entry another run's and leaves it.
            entering.insert(job_id, (run, no_room));
        }
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            let entered = entered.await;
            let no_room = {
                let mut entering = locked(&service.inner.entering);
                match entering.get(&job_id) {
                    Some((waiting, _)) if waiting.same(&mine) => {
                        entering.remove(&job_id).map(|(_, no_room)| no_room)
                    }
                    _ => None,
                }
            };
            if let Some(no_room) = no_room.filter(|_| entered) {
                service.inner.signal.hold_again(job_id, no_room);
            }
        }));
    }

    // The torrent waits for room no longer: room was made, its user
    // resumed it, its run ended, or it is gone.
    pub(super) fn end_disk_wait(&self, job_id: JobId, torrent: Option<&mut Live>) {
        if let Some(torrent) = torrent {
            torrent.disk_held = None;
        }
        let wait = {
            let mut waits = locked(&self.inner.disk_waits);
            if waits.is_empty() {
                return;
            }
            waits.remove(&job_id)
        };
        if let (Some(wait), Some(capacity)) = (wait, &self.inner.env.capacity) {
            wait.end(capacity);
            self.inner.disk_waits_moved.send_modify(|moved| *moved += 1);
        }
    }

    // The torrent's run ended or was replaced: its wait for room, what a
    // wake handed it, and any promise beside the other waiters' end with
    // it, so a new run never inherits them.
    pub(super) fn end_disk_room(&self, torrent: &mut Live) {
        let job_id = torrent.record.job_id;
        if torrent.disk_held == Some(DiskHeld::Writing) {
            self.end_disk_wait(job_id, Some(torrent));
        } else {
            self.end_disk_wait(job_id, None);
        }
        if let Some(intake) = &torrent.intake {
            intake.room().give_back();
        }
        self.end_room_promise(job_id);
    }

    // The promise a wake made the torrent beside the other waiters' ends:
    // the waits on its disk, of either kind, count readings from now, since
    // one from before shows room its write may have filled.
    pub(super) fn end_room_promise(&self, job_id: JobId) {
        let Some(capacity) = &self.inner.env.capacity else {
            return;
        };
        let Some((ended, begun)) = capacity.promises().end(capacity, Waiter::Torrent, job_id)
        else {
            return;
        };
        for wait in locked(&self.inner.disk_waits).values_mut() {
            if let Waiting::Room(wait) = &mut wait.how {
                wait.promise_ended(capacity, &ended, begun);
            }
        }
    }

    // A reading came, or a ledger rang: each torrent waiting for room is
    // judged, by the judgement a usenet job waiting for the same refusal
    // is judged by. Ledger waiters go in the order they began to wait;
    // each let go is promised what it needs at once, on the ledger every
    // kind books on, so the next waiter of either kind is judged without
    // it. A torrent let go on a reading of a full disk is promised its
    // figure beside the usenet jobs let go so, until a piece lands. One
    // let go leaves upload mode and fetches again; one whose run ended or
    // was replaced waits no more, its next run fetching again; one gone
    // ends its wait.
    async fn wake_disk_waits(&self, capacity: &StorageCapacity) {
        let reserve = self.inner.env.limits.min_free_bytes;
        let promises = capacity.promises();
        let ended = promises.take_ended(Waiter::Torrent);
        let mut woken = Vec::new();
        let mut placed = false;
        {
            let mut live = self.inner.live.lock().await;
            promises.retain(Waiter::Torrent, |job_id| live.contains_key(&job_id));
            let mut waits = locked(&self.inner.disk_waits);
            for (ended, begun) in &ended {
                for wait in waits.values_mut() {
                    if let Waiting::Room(wait) = &mut wait.how {
                        wait.promise_ended(capacity, ended, *begun);
                    }
                }
            }
            // A piece that waited for its disk to be known waits on that
            // disk's ledger once it is, judged with those waiting there.
            for (job_id, wait) in waits.iter_mut() {
                let Waiting::Place(bytes) = wait.how else {
                    continue;
                };
                let reader = live.get(job_id).and_then(|torrent| {
                    let dir = files_dir(torrent);
                    let place = torrent.intake.as_ref()?.room().place()?;
                    (place.dir == dir)
                        .then(|| place.reader().cloned())
                        .flatten()
                });
                if let Some(reader) = reader {
                    wait.how = Waiting::Ledger(LedgerWait::begin(reader.ledger(), bytes), reader);
                    placed = true;
                }
            }
            let mut order: Vec<(u8, u64, JobId)> = waits
                .iter()
                .map(|(job_id, wait)| match &wait.how {
                    Waiting::Ledger(wait, _) => (0, wait.ticket, *job_id),
                    Waiting::Room(_) => (1, 0, *job_id),
                    Waiting::Place(_) => (2, 0, *job_id),
                })
                .collect();
            order.sort_unstable();
            for (_, _, job_id) in order {
                let Some(torrent) = live.get_mut(&job_id) else {
                    if let Some(wait) = waits.remove(&job_id) {
                        wait.end(capacity);
                    }
                    continue;
                };
                let run = torrent
                    .attached
                    .as_ref()
                    .and_then(|attached| attached.handle.live());
                let Some(wait) = waits.get_mut(&job_id) else {
                    continue;
                };
                let Some(run) = run.filter(|run| wait.run.is(run)) else {
                    wait.end(capacity);
                    waits.remove(&job_id);
                    torrent.disk_held = None;
                    woken.push(job_id);
                    continue;
                };
                let room = torrent.intake.as_ref().map(|intake| intake.room());
                let dir = files_dir(torrent);
                // A run not in upload mode yet is about to be: the session
                // enters it as its write returns, and the wake runs again
                // once it has.
                let go = match &mut wait.how {
                    // Its disk is known, off the roots': nothing books there.
                    // One found on a root's since this pass began waits on
                    // that ledger from the next, which its look rang.
                    Waiting::Place(_) => {
                        run.upload_mode()
                            && room
                                .and_then(|room| room.place())
                                .is_some_and(|place| place.dir == dir && place.reader().is_none())
                    }
                    Waiting::Ledger(wait, reader) => {
                        run.upload_mode() && {
                            let woke = wait.wake(reader.ledger(), reader, reserve);
                            if woke {
                                match room {
                                    Some(room) => room.hand(wait.needs),
                                    None => reader.ledger().release(wait.needs),
                                }
                            }
                            woke
                        }
                    }
                    Waiting::Room(wait) => {
                        let shown = wait.judge(capacity, reserve, false, |disk, device| {
                            promises.on(disk, device)
                        });
                        match shown {
                            Some((bytes, device)) if run.upload_mode() => {
                                wait.end(capacity);
                                promises.insert(
                                    Waiter::Torrent,
                                    job_id,
                                    Promised {
                                        disk: wait.disk.clone(),
                                        bytes,
                                        device,
                                    },
                                );
                                if let Some(room) = room {
                                    room.promise_room();
                                }
                                true
                            }
                            _ => false,
                        }
                    }
                };
                if go {
                    tracing::info!(
                        job_id = job_id.0,
                        "room was made on the disk; the torrent fetches again"
                    );
                    run.leave_upload_mode();
                    waits.remove(&job_id);
                    torrent.disk_held = None;
                    woken.push(job_id);
                }
            }
        }
        // Rung with no wait locked: a watcher of the count may read them.
        if placed || !woken.is_empty() {
            self.inner.disk_waits_moved.send_modify(|moved| *moved += 1);
        }
        for job_id in woken {
            self.publish_job(job_id).await;
        }
    }
}

/// The warning a full disk raises.
pub fn disk_warning(volume: &Path, waiting: &[&str]) -> String {
    format!(
        "Disk full on {}: waiting {}",
        volume.display(),
        waiting.join(", ")
    )
}
