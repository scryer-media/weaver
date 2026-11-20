// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which volumes are the limit on downloads, so the operator is told the
// destination disk is slow instead of watching the speed fall to zero; and
// which jobs still have bytes on their way to disk once nothing is left to
// fetch.
//
// A volume is slow when dispatch was held for its write buffer in at least
// [`VOTE_NEEDED`] of the last [`VOTE_WINDOWS`] one-second windows, and
// stops being slow by the same count. Dispatch marks a volume as it holds a
// job for it (`Pipeline::ask_room_wake`) and leaves the article that did
// not fit with the vote. A held dispatch is not tried again until a room
// wake, so the hold is kept as state: each count marks the window again
// for every volume where that article still does not fit, and forgets it
// once it would. The metrics tick counts the vote. It lives in memory
// only: a few words per volume that held dispatch in the last windows, and
// nothing at all once none has. Nothing here writes to the database, and
// an idle vote is one flag read per tick.
//
// What the vote and the job disks say is published on the queue's shared
// state ([`DiskLimits`]) at most once a second and only when it changed:
// the slow volumes and the volumes jobs are still writing to, with their
// figures rounded to what a screen shows, and each such job. It does not
// move the queue's revision: a figure that moves reaches clients through
// the metrics snapshot they already poll, and only a change of which job
// waits on which disk rings its own signal (`SharedPipelineState::
// subscribe_disk_rows`), which queue pages follow.
//
// "Waiting" is what waits in memory for a volume's writer
// (`Volume::buffered_bytes`), not what is counted against its limit
// (`Volume::unwritten_bytes`), which also holds what callers reserved
// ahead of their writes.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use crate::JobId;
use crate::jobs::handle::DiskVolumeState;
use crate::pipeline::Pipeline;
use crate::pipeline::graph::disk::VolumeLeases;
use weaver_server_disk::operations::disk::volume::Volume;

// The one-second windows the vote looks back over, the current one first.
pub const VOTE_WINDOWS: u32 = 20;
// The windows of those in which dispatch was held that make a volume slow.
pub const VOTE_NEEDED: u32 = 12;

const WINDOW_MASK: u32 = (1 << VOTE_WINDOWS) - 1;

// One volume that held dispatch in the last windows.
struct Row {
    volume: Arc<Volume>,
    // One bit for each window, the current one lowest: set when dispatch
    // was held for the volume in it.
    held: u32,
    // The window the lowest bit is.
    second: u64,
    // The article that did not fit when dispatch was last held for the
    // volume, while it still does not: dispatch stays held until a room
    // wake, and tries nothing meanwhile.
    need: Option<u64>,
    // The vote as last counted.
    slow: bool,
}

impl Row {
    // Move the windows on to `second`; the ones that passed with no mark
    // count as not held.
    fn advance(&mut self, second: u64) {
        if second <= self.second {
            return;
        }
        let gap = second - self.second;
        self.held = if gap >= u64::from(VOTE_WINDOWS) {
            0
        } else {
            (self.held << gap) & WINDOW_MASK
        };
        self.second = second;
    }
}

// Bytes waiting in memory for `volume`'s writer: what the disk has still
// to write, without what callers reserved ahead of their writes.
fn waiting(volume: &Volume) -> u64 {
    volume.buffered_bytes.load(Ordering::SeqCst)
}

// Whether an article of `need` bytes still does not fit below `volume`'s
// lease limit for what waits for its writer: dispatch stays held on the
// disk, not on bytes reserved ahead.
fn still_holds(volume: &Volume, need: u64) -> bool {
    waiting(volume).saturating_add(need) > volume.lease_limit()
}

// The vote of every volume, kept from `epoch`.
pub struct DiskVote {
    epoch: Instant,
    // Whether any row is kept: the tick of an idle pipeline reads this
    // and nothing else.
    any: AtomicBool,
    // The second after the one last published at, 0 before the first.
    published: AtomicU64,
    rows: Mutex<Vec<Row>>,
}

impl DiskVote {
    pub fn new(epoch: Instant) -> Self {
        Self {
            epoch,
            any: AtomicBool::new(false),
            published: AtomicU64::new(0),
            rows: Mutex::new(Vec::new()),
        }
    }

    fn rows(&self) -> MutexGuard<'_, Vec<Row>> {
        self.rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn second(&self, at: Instant) -> u64 {
        at.saturating_duration_since(self.epoch).as_secs()
    }

    // Whether any volume has a vote kept.
    pub fn any(&self) -> bool {
        self.any.load(Ordering::Relaxed)
    }

    // Whether `at` is in a later second than the last publish, which it
    // then takes: what is published moves on at most once a second.
    pub fn publish_due(&self, at: Instant) -> bool {
        let next = self.second(at) + 1;
        self.published.fetch_max(next, Ordering::Relaxed) < next
    }

    // Dispatch was held for `volume`'s write buffer at `at`: an article of
    // `need` bytes did not fit.
    pub fn note_held(&self, volume: &Arc<Volume>, need: u64, at: Instant) {
        let second = self.second(at);
        let mut rows = self.rows();
        match rows.iter_mut().find(|row| Arc::ptr_eq(&row.volume, volume)) {
            Some(row) => {
                row.advance(second);
                row.held |= 1;
                row.need = Some(row.need.map_or(need, |kept| kept.max(need)));
            }
            None => rows.push(Row {
                volume: Arc::clone(volume),
                held: 1,
                second,
                need: Some(need),
                slow: false,
            }),
        }
        self.any.store(true, Ordering::Relaxed);
    }

    // Count the vote of every volume at `at`. A volume where the article
    // dispatch was held on still does not fit is held in this window too;
    // one where it would fit is released. Whether a volume became slow or
    // stopped being slow. A volume with no mark left in the windows and no
    // hold is forgotten.
    pub fn tally(&self, at: Instant) -> bool {
        if !self.any.load(Ordering::Relaxed) {
            return false;
        }
        let second = self.second(at);
        let mut rows = self.rows();
        let mut changed = false;
        for row in rows.iter_mut() {
            row.advance(second);
            match row.need {
                Some(need) if still_holds(&row.volume, need) => row.held |= 1,
                Some(_) => row.need = None,
                None => {}
            }
            let slow = row.held.count_ones() >= VOTE_NEEDED;
            changed |= slow != row.slow;
            row.slow = slow;
        }
        rows.retain(|row| row.held != 0 || row.need.is_some());
        self.any.store(!rows.is_empty(), Ordering::Relaxed);
        changed
    }

    // The volumes the last count found slow.
    pub fn slow_volumes(&self) -> Vec<Arc<Volume>> {
        if !self.any.load(Ordering::Relaxed) {
            return Vec::new();
        }
        self.rows()
            .iter()
            .filter(|row| row.slow)
            .map(|row| Arc::clone(&row.volume))
            .collect()
    }

    // Forget every vote: nothing dispatches, so nothing can be held. Whether
    // a volume was slow.
    pub fn clear(&self) -> bool {
        if !self.any.load(Ordering::Relaxed) {
            return false;
        }
        let mut rows = self.rows();
        let was_slow = rows.iter().any(|row| row.slow);
        rows.clear();
        self.any.store(false, Ordering::Relaxed);
        was_slow
    }
}

// A figure in bytes, rounded to what a screen shows of it: three
// significant figures of its binary unit, or whole units from 100 of one.
// Two figures a screen shows the same compare equal, so a publish that
// would move nothing on a screen is no change.
pub fn coarse(bytes: u64) -> u64 {
    if bytes < 1024 {
        return bytes;
    }
    let bytes = u128::from(bytes);
    let mut unit = 1u128;
    while bytes / unit >= 1024 {
        unit *= 1024;
    }
    // Tenths of a unit below 100 of one, whole units from there.
    let parts = if bytes / unit >= 100 { 1 } else { 10 };
    let steps = (bytes * parts + unit / 2) / unit;
    u64::try_from(steps * unit / parts).unwrap_or(u64::MAX)
}

// One job's write to its volume, as published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct JobWrite {
    // The volume, by its place in [`DiskLimits::volumes`].
    at: usize,
    // Nothing is left to fetch, the job's disk still has writes out, and
    // bytes wait for the volume's writer.
    draining: bool,
}

// What the job disks and the vote say about one job, for a request: which
// disk, and why it is named. Its figures are the disk's, on
// [`DiskLimits::volume_states`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobDiskWrite {
    // The volume's label.
    pub volume: String,
    // Whether the volume is slow: dispatch keeps waiting on its write buffer.
    pub slow: bool,
    // Whether the job has nothing left to fetch and bytes still waiting
    // for the volume's writer.
    pub draining: bool,
}

// What the vote and the job disks found, as the queue's shared state holds
// it for requests: the volumes that are slow or still being written for a
// job with nothing left to fetch, each with its figures as published, and
// each such job with its volume. Only jobs the pipeline has installed are
// listed, so the lists are bounded by the queue window.
#[derive(Clone, Default)]
pub struct DiskLimits {
    volumes: Vec<Arc<Volume>>,
    states: Vec<DiskVolumeState>,
    jobs: BTreeMap<JobId, JobWrite>,
}

impl DiskLimits {
    pub fn is_empty(&self) -> bool {
        self.volumes.is_empty()
    }

    // The volume `volume` is, by its place, added with its figures read
    // now when it is not listed yet.
    fn place(&mut self, volume: &Arc<Volume>, slow: bool) -> usize {
        if let Some(at) = self.volumes.iter().position(|kept| Arc::ptr_eq(kept, volume)) {
            self.states[at].slow |= slow;
            return at;
        }
        self.volumes.push(Arc::clone(volume));
        self.states.push(volume_state(volume, slow));
        self.volumes.len() - 1
    }

    // Every listed volume with its figures as published: the slow ones and
    // the ones jobs are still writing out to.
    pub fn volume_states(&self) -> Vec<DiskVolumeState> {
        self.states.clone()
    }

    // What `job_id` is writing, while its volume is slow or it has nothing
    // left to fetch and bytes still waiting; None otherwise.
    pub fn job(&self, job_id: JobId) -> Option<JobDiskWrite> {
        self.jobs.get(&job_id).map(|write| {
            let state = &self.states[write.at];
            JobDiskWrite {
                volume: state.label.clone(),
                slow: state.slow,
                draining: write.draining,
            }
        })
    }

    // The jobs listed.
    pub fn job_ids(&self) -> impl Iterator<Item = JobId> + '_ {
        self.jobs.keys().copied()
    }

    // The jobs listed as still writing out.
    fn draining_ids(&self) -> impl Iterator<Item = JobId> + '_ {
        self.jobs
            .iter()
            .filter(|(_, write)| write.draining)
            .map(|(job_id, _)| *job_id)
    }

    // Whether the two name the same jobs on the same disks for the same
    // reasons: what a queue row shows. Figures are not compared.
    pub fn same_rows(&self, other: &Self) -> bool {
        self.jobs.len() == other.jobs.len()
            && self.jobs.iter().zip(&other.jobs).all(|((a_id, a), (b_id, b))| {
                a_id == b_id
                    && a.draining == b.draining
                    && self.states[a.at].label == other.states[b.at].label
                    && self.states[a.at].slow == other.states[b.at].slow
            })
    }

    // Whether the two say the same: the same rows, volumes and figures.
    fn same_as(&self, other: &Self) -> bool {
        self.jobs == other.jobs
            && self.states == other.states
            && self.volumes.len() == other.volumes.len()
            && self
                .volumes
                .iter()
                .zip(&other.volumes)
                .all(|(a, b)| Arc::ptr_eq(a, b))
    }
}

// A volume's figures as published: rounded as a screen shows them.
pub fn volume_state(volume: &Volume, slow: bool) -> DiskVolumeState {
    DiskVolumeState {
        label: volume.label.clone(),
        slow,
        write_rate_bytes_per_sec: coarse(volume.write_rate.load(Ordering::Relaxed)),
        waiting_bytes: coarse(waiting(volume)),
    }
}

// What one job's disk says, for the lists: its volumes, whether it has
// nothing left to fetch, and whether its writes are all back.
pub struct JobDiskView<'a> {
    pub job_id: JobId,
    pub volumes: Vec<&'a Arc<Volume>>,
    pub fetched: bool,
    pub quiet: bool,
}

// Build what is published from the slow volumes and each installed job's
// disk. A job is listed when one of its volumes is slow (on that volume),
// or when it has nothing left to fetch, writes still out and bytes waiting
// for one of its volumes' writers (on the first such volume). A job whose
// volumes have nothing waiting is not listed, whatever else it does: an
// unpack or a repair that waits on no disk shows its own status.
pub fn disk_limits<'a>(
    slow: &[Arc<Volume>],
    jobs: impl IntoIterator<Item = JobDiskView<'a>>,
) -> DiskLimits {
    let mut limits = DiskLimits::default();
    for volume in slow {
        limits.place(volume, true);
    }
    for job in jobs {
        let on_slow = job
            .volumes
            .iter()
            .find(|volume| slow.iter().any(|kept| Arc::ptr_eq(kept, volume)));
        let writing = (job.fetched && !job.quiet)
            .then(|| job.volumes.iter().find(|volume| waiting(volume) > 0))
            .flatten();
        let (volume, draining) = match (on_slow, writing) {
            (Some(volume), writing) => (*volume, writing.is_some()),
            (None, Some(volume)) => (*volume, true),
            (None, None) => continue,
        };
        let at = limits.place(volume, false);
        limits.jobs.insert(job.job_id, JobWrite { at, draining });
    }
    limits
}

impl Pipeline {
    // Count the disk vote at `now`, and publish what it and the job disks
    // say when that changed: at once when the vote changed, else at most
    // once a second. With no vote kept, nothing published and no job
    // installed, this is the flag read and nothing more.
    pub fn tally_disk_vote(&mut self, now: Instant) {
        let changed = self.disk_vote.tally(now);
        if !changed
            && !self.disk_vote.any()
            && self.disk_limits.is_empty()
            && self.graph_jobs.is_empty()
            && self.retiring_graph_jobs.is_empty()
        {
            return;
        }
        if !self.disk_vote.publish_due(now) && !changed {
            return;
        }
        self.publish_disk_limits();
    }

    // Nothing dispatches any more, so no volume holds it: the vote is
    // dropped, and what is published says so. Jobs still writing stay
    // listed as they are now.
    pub(in crate::pipeline) fn clear_disk_vote(&mut self) {
        self.disk_vote.clear();
        if !self.disk_limits.is_empty() {
            self.publish_disk_limits();
        }
    }

    // A volume made room, or a listed job's writes returned (each listed
    // job asks for that, [`Self::ask_disk_tells`]): what is published moves
    // with it, with the ticks parked as well. A listed job whose writes are
    // all back, or whose volumes have nothing waiting any more, or that has
    // gone, is dropped at once; figures move at most once a second.
    // Nothing is done while nothing is listed.
    pub(in crate::pipeline) fn refresh_parked_disk_limits(&mut self) {
        if self.disk_limits.is_empty() {
            return;
        }
        let graphs = (&self.graph_jobs, &self.retiring_graph_jobs);
        let limits = &self.disk_limits;
        let done = limits.job_ids().any(|job_id| {
            let draining = limits.job(job_id).is_some_and(|write| write.draining);
            graphs
                .0
                .get(&job_id)
                .or_else(|| graphs.1.get(&job_id))
                .and_then(|graph| graph.disk())
                .is_none_or(|disk| {
                    draining
                        && (disk.quiet()
                            || disk.each().all(|submitter| waiting(submitter.volume()) == 0))
                })
        });
        if self.disk_vote.publish_due(Instant::now()) || done {
            self.publish_disk_limits();
        }
    }

    // One word of room from the volumes' writers, as the loop hears it:
    // dispatch held for a volume looks again, and what is published moves
    // with it.
    pub fn hear_disk_room(&mut self) {
        self.volume_room_heard();
        self.refresh_parked_disk_limits();
    }

    fn publish_disk_limits(&mut self) {
        let slow = self.disk_vote.slow_volumes();
        let jobs = &self.jobs;
        // Nothing left to fetch: the job's queue holds no article. One
        // still leased is out on a lane, which the page reads as bytes
        // still to come.
        let fetched = |job_id: &JobId| {
            jobs.get(job_id)
                .and_then(|state| state.articles.download_queue())
                .is_some_and(|queue| queue.is_empty())
        };
        // A retiring job is done fetching; it is kept until its disk is
        // quiet.
        let views = self
            .graph_jobs
            .iter()
            .map(|(job_id, graph)| (job_id, graph, fetched(job_id)))
            .chain(self.retiring_graph_jobs.iter().map(|(job_id, graph)| (job_id, graph, true)))
            .filter_map(|(job_id, graph, fetched)| {
                let disk = graph.disk()?;
                Some(JobDiskView {
                    job_id: *job_id,
                    volumes: disk.each().map(|submitter| submitter.volume()).collect(),
                    fetched,
                    quiet: disk.quiet(),
                })
            });
        let next = disk_limits(&slow, views);
        self.ask_disk_tells(&next);
        if next.same_as(&self.disk_limits) {
            return;
        }
        let rows = !next.same_rows(&self.disk_limits);
        let next = Arc::new(next);
        self.disk_limits = Arc::clone(&next);
        self.shared_state.set_disk_limits(next, rows);
    }

    // Each job listed as still writing out asks to be told when what it
    // has submitted so far returns: its volumes' writers ring the room
    // channel then, and [`Self::refresh_parked_disk_limits`] drops it or
    // asks again for what it submitted since. One ask per listed job at a
    // time; a job no longer listed is forgotten.
    fn ask_disk_tells(&mut self, limits: &DiskLimits) {
        if self.disk_tells.is_empty() && limits.draining_ids().next().is_none() {
            return;
        }
        self.disk_tells
            .retain(|job_id, _| limits.job(*job_id).is_some_and(|write| write.draining));
        for job_id in limits.draining_ids() {
            let asked = self
                .disk_tells
                .get(&job_id)
                .is_some_and(|barriers| barriers.iter().any(|barrier| !barrier.returned()));
            if asked {
                continue;
            }
            let Some(disk) = self
                .graph_jobs
                .get(&job_id)
                .or_else(|| self.retiring_graph_jobs.get(&job_id))
                .and_then(|graph| graph.disk())
            else {
                continue;
            };
            let barriers: Vec<_> = disk
                .each()
                .map(|submitter| submitter.barrier())
                .filter(|barrier| !barrier.returned())
                .collect();
            for barrier in &barriers {
                let ring = self.disk_tell.clone();
                barrier.then(move || {
                    let _ = ring.send(());
                });
            }
            self.disk_tells.insert(job_id, barriers);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn volume(label: &str) -> Arc<Volume> {
        Arc::new(Volume::new(Some(1), label.into()))
    }

    // The instant `seconds` (and a part) into the vote.
    fn at(epoch: Instant, seconds: u64, millis: u64) -> Instant {
        epoch + Duration::from_secs(seconds) + Duration::from_millis(millis)
    }

    // Mark `volume` held in each of the windows `seconds`.
    fn hold(vote: &DiskVote, epoch: Instant, volume: &Arc<Volume>, seconds: impl Iterator<Item = u64>) {
        for second in seconds {
            vote.note_held(volume, 1, at(epoch, second, 500));
        }
    }

    fn labels(vote: &DiskVote) -> Vec<String> {
        vote.slow_volumes()
            .iter()
            .map(|volume| volume.label.clone())
            .collect()
    }

    #[test]
    fn eleven_held_windows_are_not_slow_and_twelve_are() {
        let epoch = Instant::now();
        let vote = DiskVote::new(epoch);
        let nas = volume("/mnt/nas");
        hold(&vote, epoch, &nas, 0..11);
        assert!(!vote.tally(at(epoch, 10, 900)));
        assert!(labels(&vote).is_empty());
        hold(&vote, epoch, &nas, 11..12);
        assert!(vote.tally(at(epoch, 11, 900)));
        assert_eq!(labels(&vote), ["/mnt/nas"]);
        // Counting again with nothing new changes nothing.
        assert!(!vote.tally(at(epoch, 11, 950)));
    }

    // Many marks inside one window count as one.
    #[test]
    fn marks_in_one_window_count_once() {
        let epoch = Instant::now();
        let vote = DiskVote::new(epoch);
        let nas = volume("/mnt/nas");
        for millis in (0..1000).step_by(10) {
            vote.note_held(&nas, 1, at(epoch, 3, millis));
        }
        assert!(!vote.tally(at(epoch, 3, 999)));
        assert!(labels(&vote).is_empty());
    }

    // Held in twelve of twenty windows spread out, not in a row.
    #[test]
    fn the_vote_counts_windows_not_a_run() {
        let epoch = Instant::now();
        let vote = DiskVote::new(epoch);
        let nas = volume("/mnt/nas");
        // Windows 0..20, held in every one but 8 of them: 12 held.
        let held = (0..20).filter(|second| ![1, 3, 5, 7, 9, 11, 13, 15].contains(second));
        hold(&vote, epoch, &nas, held);
        assert!(vote.tally(at(epoch, 19, 900)));
        assert_eq!(labels(&vote), ["/mnt/nas"]);
    }

    // Once nothing holds, the vote clears as the held windows pass out of
    // the last twenty: twenty held, cleared the window the twelfth-last
    // held one leaves.
    #[test]
    fn the_same_vote_clears_it() {
        let epoch = Instant::now();
        let vote = DiskVote::new(epoch);
        let nas = volume("/mnt/nas");
        hold(&vote, epoch, &nas, 0..20);
        assert!(vote.tally(at(epoch, 19, 900)));
        // At window 27, windows 8..=19 are held: still twelve.
        assert!(!vote.tally(at(epoch, 27, 0)));
        assert_eq!(labels(&vote), ["/mnt/nas"]);
        // At window 28 only eleven are.
        assert!(vote.tally(at(epoch, 28, 0)));
        assert!(labels(&vote).is_empty());
        // Once the last mark leaves, the volume is forgotten.
        assert!(!vote.tally(at(epoch, 39, 0)));
        assert!(!vote.any.load(Ordering::Relaxed));
        assert!(vote.rows().is_empty());
    }

    // A gap longer than the windows forgets every mark at once.
    #[test]
    fn a_long_gap_forgets_every_mark() {
        let epoch = Instant::now();
        let vote = DiskVote::new(epoch);
        let nas = volume("/mnt/nas");
        hold(&vote, epoch, &nas, 0..20);
        assert!(vote.tally(at(epoch, 19, 0)));
        assert!(vote.tally(at(epoch, 500, 0)));
        assert!(labels(&vote).is_empty());
        assert!(vote.rows().is_empty());
    }

    // Each volume has its own vote.
    #[test]
    fn volumes_are_voted_apart() {
        let epoch = Instant::now();
        let vote = DiskVote::new(epoch);
        let nas = volume("/mnt/nas");
        let ssd = volume("/");
        hold(&vote, epoch, &nas, 0..14);
        hold(&vote, epoch, &ssd, 0..5);
        assert!(vote.tally(at(epoch, 13, 900)));
        assert_eq!(labels(&vote), ["/mnt/nas"]);
    }

    // An idle vote does nothing: no row, no change, no lock taken for a
    // count.
    #[test]
    fn an_idle_vote_keeps_nothing() {
        let epoch = Instant::now();
        let vote = DiskVote::new(epoch);
        assert!(!vote.tally(at(epoch, 5, 0)));
        assert!(vote.slow_volumes().is_empty());
        assert!(!vote.clear());
    }

    #[test]
    fn clear_drops_a_slow_vote() {
        let epoch = Instant::now();
        let vote = DiskVote::new(epoch);
        let nas = volume("/mnt/nas");
        hold(&vote, epoch, &nas, 0..12);
        assert!(vote.tally(at(epoch, 11, 900)));
        assert!(vote.clear());
        assert!(vote.slow_volumes().is_empty());
        assert!(!vote.tally(at(epoch, 12, 0)));
    }

    // `bytes` waiting in memory for the volume's writer, counted as such.
    fn wait(volume: &Volume, bytes: u64) {
        volume.count_for_test(bytes);
        volume.buffered_bytes.store(bytes, Ordering::SeqCst);
    }

    // A volume whose buffer is full: `unwritten` bytes waiting against a
    // hard line of 64 MiB.
    fn full(label: &str, unwritten: u64) -> Arc<Volume> {
        let volume = volume(label);
        volume.set_buffer_limits(32 << 20, 64 << 20);
        wait(&volume, unwritten);
        volume
    }

    // Dispatch is held once and tries nothing more, as it does while no
    // room wake comes: the count marks each window the article still does
    // not fit, so a disk that writes nothing for twenty seconds is held in
    // twenty windows, and slow from the twelfth.
    #[test]
    fn held_once_counts_every_window_it_stays_held() {
        let epoch = Instant::now();
        let vote = DiskVote::new(epoch);
        let nas = full("/mnt/nas", 64 << 20);
        vote.note_held(&nas, 1 << 20, at(epoch, 0, 100));
        for second in 0..11 {
            assert!(!vote.tally(at(epoch, second, 900)), "second {second}");
        }
        assert!(labels(&vote).is_empty());
        assert!(vote.tally(at(epoch, 11, 900)));
        assert_eq!(labels(&vote), ["/mnt/nas"]);
        for second in 12..20 {
            assert!(!vote.tally(at(epoch, second, 900)));
        }
        assert_eq!(vote.rows()[0].held.count_ones(), VOTE_WINDOWS);
    }

    // Once the article would fit the hold is released, and the volume
    // stops being slow by the same vote: the windows held pass out of the
    // last twenty, and then it is forgotten.
    #[test]
    fn a_released_hold_clears_by_the_vote() {
        let epoch = Instant::now();
        let vote = DiskVote::new(epoch);
        let nas = full("/mnt/nas", 64 << 20);
        vote.note_held(&nas, 1 << 20, at(epoch, 0, 100));
        for second in 0..19 {
            vote.tally(at(epoch, second, 900));
        }
        assert!(!vote.tally(at(epoch, 19, 900)));
        assert_eq!(labels(&vote), ["/mnt/nas"]);
        // The disk catches up.
        wait(&nas, 0);
        assert!(!vote.tally(at(epoch, 20, 900)));
        assert!(vote.rows()[0].need.is_none());
        // Windows 1..=19 held at 20; 8..=19 still twelve at 27.
        for second in 21..28 {
            assert!(!vote.tally(at(epoch, second, 900)));
        }
        assert_eq!(labels(&vote), ["/mnt/nas"]);
        assert!(vote.tally(at(epoch, 28, 900)));
        assert!(labels(&vote).is_empty());
        assert!(!vote.tally(at(epoch, 39, 900)));
        assert!(!vote.any());
        assert!(vote.rows().is_empty());
    }

    // A hold the disk drains in the same window is not carried on.
    #[test]
    fn a_hold_that_fits_by_the_count_marks_its_window_only() {
        let epoch = Instant::now();
        let vote = DiskVote::new(epoch);
        let nas = full("/mnt/nas", 64 << 20);
        vote.note_held(&nas, 1 << 20, at(epoch, 0, 100));
        wait(&nas, 1 << 20);
        vote.tally(at(epoch, 0, 900));
        vote.tally(at(epoch, 5, 900));
        assert_eq!(vote.rows()[0].held.count_ones(), 1);
    }

    #[test]
    fn coarse_figures_round_to_what_a_screen_shows() {
        assert_eq!(coarse(0), 0);
        assert_eq!(coarse(1000), 1000);
        // 26.70 MiB and 26.72 MiB both show as 26.7 MB.
        let a = 28_000_000;
        let b = 28_020_000;
        assert_eq!(coarse(a), coarse(b));
        assert_eq!(coarse(a), 267 * (1 << 20) / 10);
        // A figure a screen shows exactly is kept as it is.
        assert_eq!(coarse(28 << 20), 28 << 20);
        // From 100 of a unit, whole units.
        assert_eq!(coarse(240_000_000), 229 << 20);
        assert_eq!(coarse(u64::MAX), coarse(u64::MAX - 1));
    }

    #[test]
    fn publish_is_due_once_a_second() {
        let epoch = Instant::now();
        let vote = DiskVote::new(epoch);
        assert!(vote.publish_due(at(epoch, 0, 0)));
        assert!(!vote.publish_due(at(epoch, 0, 250)));
        assert!(!vote.publish_due(at(epoch, 0, 999)));
        assert!(vote.publish_due(at(epoch, 1, 0)));
        assert!(!vote.publish_due(at(epoch, 1, 500)));
    }

    fn view<'a>(job: u64, volumes: Vec<&'a Arc<Volume>>, fetched: bool, quiet: bool) -> JobDiskView<'a> {
        JobDiskView {
            job_id: JobId(job),
            volumes,
            fetched,
            quiet,
        }
    }

    // Bytes counted on the volume that are not waiting in memory, credit
    // reserved ahead for articles still on their way, neither hold the vote
    // nor are shown as waiting.
    #[test]
    fn only_bytes_waiting_in_memory_hold_or_show() {
        let epoch = Instant::now();
        let vote = DiskVote::new(epoch);
        let nas = volume("/mnt/nas");
        nas.set_buffer_limits(32 << 20, 64 << 20);
        nas.count_for_test(64 << 20);
        vote.note_held(&nas, 1 << 20, at(epoch, 0, 100));
        vote.tally(at(epoch, 0, 900));
        vote.tally(at(epoch, 5, 900));
        assert_eq!(vote.rows()[0].held.count_ones(), 1);
        assert_eq!(volume_state(&nas, false).waiting_bytes, 0);
        let limits = disk_limits(&[], [view(1, vec![&nas], true, false)]);
        assert!(limits.is_empty());
    }

    // A job on a slow volume is listed on it; a job with nothing left to
    // fetch and writes out is listed on its volume with bytes waiting,
    // whatever the vote; a job still fetching on a volume that is not slow,
    // one whose writes are all back, and one with nothing left to fetch and
    // nothing waiting on its volumes (an unpack or repair that waits on no
    // disk) are not listed.
    #[test]
    fn limits_list_slow_and_draining_jobs_with_rounded_figures() {
        let nas = volume("/mnt/nas");
        nas.write_rate.store(28_000_000, Ordering::Relaxed);
        wait(&nas, 240_000_000);
        let ssd = volume("/");
        let usb = volume("/mnt/usb");
        wait(&usb, 5_000_000);
        let limits = disk_limits(
            &[Arc::clone(&nas)],
            [
                view(1, vec![&ssd, &nas], false, false),
                view(2, vec![&ssd, &usb], true, false),
                view(3, vec![&ssd], false, false),
                view(4, vec![&usb], true, true),
                view(5, vec![&ssd], true, false),
            ],
        );
        assert_eq!(
            limits.volume_states(),
            [
                DiskVolumeState {
                    label: "/mnt/nas".into(),
                    slow: true,
                    write_rate_bytes_per_sec: coarse(28_000_000),
                    waiting_bytes: coarse(240_000_000),
                },
                DiskVolumeState {
                    label: "/mnt/usb".into(),
                    slow: false,
                    write_rate_bytes_per_sec: 0,
                    waiting_bytes: coarse(5_000_000),
                },
            ]
        );
        assert_eq!(
            limits.job(JobId(1)),
            Some(JobDiskWrite {
                volume: "/mnt/nas".into(),
                slow: true,
                draining: false,
            })
        );
        assert_eq!(
            limits.job(JobId(2)),
            Some(JobDiskWrite {
                volume: "/mnt/usb".into(),
                slow: false,
                draining: true,
            })
        );
        assert!(limits.job(JobId(3)).is_none());
        assert!(limits.job(JobId(4)).is_none());
        assert!(limits.job(JobId(5)).is_none());
    }

    // What is published changes only when a figure moves on a screen, and
    // what the rows say changes only with the jobs, disks and reasons, not
    // the figures.
    #[test]
    fn limits_compare_by_what_they_say() {
        let nas = volume("/mnt/nas");
        nas.write_rate.store(28_000_000, Ordering::Relaxed);
        let jobs = || [view(1, vec![&nas], false, false)];
        let first = disk_limits(&[Arc::clone(&nas)], jobs());
        nas.write_rate.store(28_020_000, Ordering::Relaxed);
        assert!(first.same_as(&disk_limits(&[Arc::clone(&nas)], jobs())));
        nas.write_rate.store(30_000_000, Ordering::Relaxed);
        wait(&nas, 9 << 20);
        let moved = disk_limits(&[Arc::clone(&nas)], jobs());
        assert!(!first.same_as(&moved));
        assert!(first.same_rows(&moved));
        assert!(!first.same_rows(&disk_limits(&[], jobs())));
        assert!(!first.same_as(&DiskLimits::default()));
        assert!(DiskLimits::default().same_as(&disk_limits(&[], jobs())));
    }
}
