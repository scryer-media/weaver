// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a job's files reach the disk: through the writer of the volume each
// lies on. The job's records of its files run ahead of the disk by what
// those writers still hold, so everything that needs the two to agree, a
// checkpoint, a read of a file by its path, a move or the job's end, waits
// for the job's barrier first.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::operations::StorageCapacity;
use crate::operations::disk::volume::Volume;
use crate::operations::disk::writer::{Barrier, Failure, Report, Submitter, VolumeWriters};
use crate::pipeline::Pipeline;
use crate::pipeline::download::volume_pace::{VolumeBuffers, VolumeIntake, intake_of};
use crate::pipeline::graph::job::GraphError;
use crate::pipeline::graph::leaf::LeafError;
use crate::pipeline::graph::namespace::WriteError;
use crate::JobId;
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::job::GraphJob;

// The volumes of a job's directories as a thread off the loop read them.
pub(crate) type ReadVolumes = Vec<(PathBuf, Arc<Volume>)>;

// The writers of the volumes `working_dir` and `output_dir` lie on, for the
// job `key`. `buffers` counts the job among those writing to each volume
// until the last copy of the job's disk is dropped, and sizes the volumes'
// buffer limits as a volume gains its first job or loses its last. A
// directory `read` holds takes the volume read for it: memory may have
// forgotten the path since, and would answer the stand-in of an unknown
// device, whose limits are its own, so the job's writes would not count
// against the limit of the volume they land on.
#[allow(clippy::too_many_arguments)]
pub(crate) fn job_disk(
    writers: &VolumeWriters,
    capacity: &StorageCapacity,
    probes: &VolumeProbes,
    key: u64,
    working_dir: &Path,
    output_dir: Option<&Path>,
    buffers: &Arc<VolumeBuffers>,
    read: &[(PathBuf, Arc<Volume>)],
) -> JobDisk {
    let volume_of = |dir: &Path| {
        read.iter()
            .find(|(at, _)| at == dir)
            .map_or_else(|| capacity.volume(dir), |(_, volume)| Arc::clone(volume))
    };
    let volume = volume_of(working_dir);
    let mut writing = Writing {
        buffers: Arc::clone(buffers),
        volumes: Vec::with_capacity(2),
    };
    writing.join(&volume);
    // The job holds the volume before the probe is queued: a volume held
    // and not written to yet keeps its probe for later.
    let working = writers.submitter(&volume, key);
    probes.first_seen(writers, &volume, working_dir);
    let output = output_dir.and_then(|dir| {
        let other = volume_of(dir);
        (!Arc::ptr_eq(&other, &volume)).then(|| {
            writing.join(&other);
            let output = writers.submitter(&other, key);
            probes.first_seen(writers, &other, dir);
            (dir.to_path_buf(), output)
        })
    });
    JobDisk {
        working,
        output,
        _writing: Some(Arc::new(writing)),
    }
}

// A job's place among those writing to its volumes, held by every copy of
// its disk and given up when the last is dropped.
struct Writing {
    buffers: Arc<VolumeBuffers>,
    volumes: Vec<Arc<Volume>>,
}

impl Writing {
    fn join(&mut self, volume: &Arc<Volume>) {
        self.buffers.joined(volume);
        self.volumes.push(Arc::clone(volume));
    }
}

impl Drop for Writing {
    fn drop(&mut self) {
        for volume in &self.volumes {
            self.buffers.left(volume);
        }
    }
}

impl std::fmt::Debug for Writing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writing")
            .field("volumes", &self.volumes.len())
            .finish()
    }
}

type Probe = Box<dyn FnOnce() -> std::io::Result<()> + Send>;

// What gives each volume its first write rate: the first time a job is
// given a volume's writer, the volume's probe is left with that writer for
// when it has nothing queued, and never again for that volume: the job's
// first writes and its first read do not stand behind the probe. It runs
// the first time the writer runs out of work, on the volume's thread;
// nothing here looks at a disk.
pub(crate) struct VolumeProbes {
    // The probe of a volume, given a directory on it that is there.
    probe: Option<Arc<dyn Fn(Arc<Volume>, PathBuf) -> Probe + Send + Sync>>,
    // The volumes whose probe was queued: one entry for each device.
    seen: std::sync::Mutex<Vec<Arc<Volume>>>,
}

impl VolumeProbes {
    // Probes that read and keep their figures in `store`.
    pub(crate) fn stored(store: Arc<crate::operations::disk::ProbeStore>) -> Self {
        Self::with(move |volume, dir| {
            Box::new(crate::operations::disk::probe_once(
                volume,
                dir,
                Arc::clone(&store),
            ))
        })
    }

    pub(crate) fn with(
        probe: impl Fn(Arc<Volume>, PathBuf) -> Probe + Send + Sync + 'static,
    ) -> Self {
        Self {
            probe: Some(Arc::new(probe)),
            seen: Default::default(),
        }
    }

    // No volume is probed.
    pub(crate) fn none() -> Self {
        Self {
            probe: None,
            seen: Default::default(),
        }
    }

    fn first_seen(&self, writers: &VolumeWriters, volume: &Arc<Volume>, dir: &Path) {
        let Some(probe) = &self.probe else {
            return;
        };
        {
            let mut seen = self
                .seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if seen.iter().any(|known| Arc::ptr_eq(known, volume)) {
                return;
            }
            seen.push(Arc::clone(volume));
        }
        // The job's directory may not be made yet: the probe is given the
        // nearest directory above it that is, found on the volume's thread.
        let (probe, probed, under) = (Arc::clone(probe), Arc::clone(volume), dir.to_path_buf());
        writers.volume_submitter(volume).when_idle(move || {
            if let Some(dir) = under.ancestors().find(|dir| dir.is_dir()) {
                let _ = probe(probed, dir.to_path_buf())();
            }
        });
    }
}

// A test's say over how the pipelines it builds write: one thread per
// volume, as outside tests, instead of each operation as it is submitted.
#[cfg(test)]
pub(crate) mod threads {
    use std::sync::atomic::{AtomicBool, Ordering};

    static ASKED: AtomicBool = AtomicBool::new(false);

    pub(crate) struct Threads(());

    pub(crate) fn real() -> Threads {
        ASKED.store(true, Ordering::SeqCst);
        Threads(())
    }

    pub(crate) fn asked() -> bool {
        ASKED.load(Ordering::SeqCst)
    }

    impl Drop for Threads {
        fn drop(&mut self) {
            ASKED.store(false, Ordering::SeqCst);
        }
    }
}

impl Pipeline {
    // What the volumes the job writes to ask of the lanes fetching for it:
    // the most held back of them. Each volume is read against its own
    // limits.
    pub(in crate::pipeline) fn volume_intake(&self, job_id: JobId) -> VolumeIntake {
        self.graph_jobs
            .get(&job_id)
            .and_then(|graph| graph.disk())
            .map_or(VolumeIntake::Open, JobDisk::intake)
    }

    // The bytes the job's volumes may still take below their hard limits,
    // the least of them: what may arrive for them before they hold their
    // limit. `None` when the job writes through no writer or none of its
    // volumes is sized.
    pub(in crate::pipeline) fn volume_room(&self, job_id: JobId) -> Option<u64> {
        let disk = self.graph_jobs.get(&job_id)?.disk()?;
        disk.each()
            .filter_map(|submitter| {
                let volume = submitter.volume();
                let (_, hard) = volume.buffer_limits();
                (hard != u64::MAX).then(|| hard.saturating_sub(volume.unwritten_bytes()))
            })
            .min()
    }

    // Whether a volume the job writes to holds its own hard limit or more
    // of bytes that arrived and are not written. Dispatch for the job
    // stops while one does, and whatever brings it below wakes dispatch.
    pub(in crate::pipeline) fn volume_holds_budget(&self, job_id: JobId) -> bool {
        let held = self.volume_intake(job_id) == VolumeIntake::Stopped;
        if held {
            self.volume_budget_held
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        held
    }

    // The limits of every volume were sized on other numbers, or a
    // volume's unwritten bytes fell to the line dispatch asked to be woken
    // at: dispatch that stopped for a volume looks again.
    pub(in crate::pipeline) fn volume_room_changed(&mut self) {
        if self
            .volume_budget_held
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            self.download_dispatch_wake = true;
        }
    }

    // A stopped job has nothing left out, on a thread or with a volume's
    // writer: it goes, and the end that waited for it is carried out.
    pub(in crate::pipeline) fn finish_graph_retirement(&mut self, job_id: JobId) {
        let mut graph = self.retiring_graph_jobs.remove(&job_id);
        if let Some(ticket) = graph.as_mut().and_then(GraphJob::forget_slot) {
            self.graph_status.repairs.release(ticket);
        }
        match self.pending_graph_terminals.remove(&job_id) {
            Some(crate::pipeline::health::PendingGraphTerminal::Failed(failure)) => {
                let cancelling = self.shared_state.queue.read(|inner| {
                    inner
                        .jobs
                        .iter()
                        .any(|row| row.id == job_id && row.intents.cancelled.is_some())
                });
                // Retirement blocks readmission; never apply an old
                // failure to a job that no longer has its failed state.
                if self.jobs.get(&job_id).is_some_and(|state| {
                    matches!(&state.status, JobStatus::Failed { error } if error == &failure.error)
                }) {
                    if let Some(graph) = graph {
                        self.graph_jobs.insert(job_id, graph);
                    }
                    if cancelling {
                        // The archive reply decides whether cancellation
                        // wins. Worker completion must not race it into
                        // failure cleanup or a second history write.
                        self.pending_graph_terminals.insert(
                            job_id,
                            crate::pipeline::health::PendingGraphTerminal::Failed(failure),
                        );
                    } else {
                        self.continue_job_failure(job_id, failure);
                    }
                }
            }
            Some(crate::pipeline::health::PendingGraphTerminal::Cancelled {
                cleanup,
                keep_files,
            }) => {
                drop(graph);
                self.finish_cancelled_job(cleanup, keep_files);
            }
            None => {}
        }
        self.download_dispatch_wake = true;
    }

    // The volumes' writers returned what the job had submitted when a
    // checkpoint or its end began to wait: the checkpoint may go, and a
    // stopped job that waited only for its last file operation ends.
    pub(in crate::pipeline) fn graph_disk_returned(&mut self, job_id: JobId) {
        let mail = self.graph_runs.written();
        if let Some(graph) = self
            .graph_jobs
            .get(&job_id)
            .or_else(|| self.retiring_graph_jobs.get(&job_id))
        {
            graph.pending().queue_released(&self.db, job_id, &mail);
        }
        if self.retiring_graph_jobs.contains_key(&job_id) {
            self.settle_graph_retirement(job_id);
            self.wake_repair_waiters();
            self.wake_memory_waiters();
        }
        self.retry_move_after_disk(job_id, false);
    }

    // A stopped job after something of it was heard: it goes when nothing
    // holds it any more. One held only by file operations its volumes'
    // writers still have is told when they returned, and is asked after
    // again then. The operations are whatever is queued now: mail heard
    // after the job stopped may have queued more than its stop waited for.
    pub(in crate::pipeline) fn settle_graph_retirement(&mut self, job_id: JobId) {
        let Some(graph) = self.retiring_graph_jobs.get(&job_id) else {
            return;
        };
        if !graph.holds_retirement() {
            self.finish_graph_retirement(job_id);
        } else if !graph.awaits_mail()
            && let Some(disk) = graph.disk()
        {
            disk.barrier()
                .then_tell_of_the_rest(job_id, &self.graph_runs.written());
        }
    }

    // Hear every batch report that is waiting. Whether any was.
    pub(in crate::pipeline) fn hear_volume_reports(&mut self) -> bool {
        let mut heard = false;
        while let Ok(report) = self.volume_reports.try_recv() {
            heard = true;
            self.volume_batch_returned(report);
        }
        heard
    }

    // A volume's writer could not complete an operation on one of a job's
    // files. A failed write fails its job as a write the job made itself
    // would, and a refused name skips its member. A refusal that names no
    // member to skip, or a member an object reads, stands as a failure.
    pub(in crate::pipeline) fn graph_disk_failed(&mut self, failure: Failure) {
        let job_id = JobId(failure.key);
        let refused =
            failure.creating && failure.error.kind() == std::io::ErrorKind::InvalidFilename;
        if self.retiring_graph_jobs.contains_key(&job_id) {
            self.retiring_disk_failed(job_id, failure, refused);
            return;
        }
        let Some(graph) = self.graph_jobs.get_mut(&job_id) else {
            return;
        };
        if refused {
            let skipped = match failure.file {
                Some(file) => graph.name_refused(file),
                None => Ok(false),
            };
            // What the waiting checkpoints say of the file is undone by
            // the next one, and they are written only with it.
            graph.pending().await_correction();
            if let Some(disk) = graph.disk() {
                disk.heard_refusal(&failure.path);
            }
            match skipped {
                Ok(false) => {}
                skipped => {
                    self.after_graph_result(job_id, skipped.map(|_| ()));
                    self.retry_move_after_disk(job_id, true);
                    return;
                }
            }
        }
        tracing::warn!(
            job_id = job_id.0,
            path = %failure.path.display(),
            error = %failure.error,
            "a volume's writer could not complete a file operation"
        );
        self.moves_awaiting_disk.remove(&job_id);
        self.after_graph_result(job_id, Err(disk_error(failure)));
    }

    // The same for a job that was stopped while the operation was queued.
    // Its last checkpoints waited for that operation and say more than the
    // disk holds: they are dropped, so a restore reads the last one the
    // disk bore out and fetches the rest again, and whoever waited for
    // their commit hears it will not come. A refused name is met again by
    // the job's next run, which skips the member then. Anything else ends
    // the job Failed once the rest of its retirement is over.
    fn retiring_disk_failed(&mut self, job_id: JobId, failure: Failure, refused: bool) {
        let Some(graph) = self.retiring_graph_jobs.get(&job_id) else {
            return;
        };
        graph.pending().disk_failed();
        if refused {
            if let Some(disk) = graph.disk() {
                disk.heard_refusal(&failure.path);
            }
            return;
        }
        tracing::warn!(
            job_id = job_id.0,
            path = %failure.path.display(),
            error = %failure.error,
            "a volume's writer could not complete a file operation of a stopped job"
        );
        self.fail_job_on_extraction_error(job_id, disk_error(failure).to_extraction());
    }

    // A delivery that waited for the job's file operations looks again:
    // after a failure's report, or once the barrier it waits on returned.
    // The job's other mail of returned operations, a checkpoint's, comes
    // while the delivery's own operations are still out and starts nothing.
    fn retry_move_after_disk(&mut self, job_id: JobId, reported: bool) {
        let due = self
            .moves_awaiting_disk
            .get(&job_id)
            .is_some_and(|barrier| reported || barrier.returned());
        if due {
            self.moves_awaiting_disk.remove(&job_id);
            self.schedule_job_completion_check(job_id);
        }
    }

    // A volume's writer has something for the loop: a volume dispatch was
    // stopped for fell below its line, or an operation failed and may
    // have freed one. Dispatch that stopped for a volume looks again.
    pub(in crate::pipeline) fn volume_batch_returned(&mut self, report: Report) {
        for failure in report.failures {
            self.graph_disk_failed(failure);
        }
        self.volume_room_changed();
    }
}

fn disk_error(failure: Failure) -> GraphError {
    GraphError::Write(WriteError::Leaf(LeafError::Io {
        path: failure.path,
        source: failure.error,
    }))
}

#[derive(Clone, Debug)]
pub(crate) struct JobDisk {
    // The volume the working directory lies on.
    working: Arc<Submitter>,
    // Where the delivered files lie and their volume, when it is another.
    output: Option<(PathBuf, Arc<Submitter>)>,
    // The job's place among those writing to its volumes, when the volumes'
    // buffer limits are kept for it.
    _writing: Option<Arc<Writing>>,
}

// The bytes a volume may hold in memory for its writer before a thread
// that produces a job's output waits for it: the volume's own hard limit.
fn output_limit(submitter: &Submitter) -> u64 {
    submitter.volume().buffer_limits().1
}

impl JobDisk {
    #[cfg(test)]
    pub(crate) fn new(working: Arc<Submitter>, output: Option<(PathBuf, Arc<Submitter>)>) -> Self {
        Self {
            working,
            output,
            _writing: None,
        }
    }

    // Stand still until no volume the job writes to holds more than its
    // hard limit for its writer: the wait of a thread that produces what
    // the job writes, an extraction's run, before it hands the loop more.
    // Never for the loop. Returns at once for a job whose writes failed
    // or were given up.
    pub(crate) fn wait_below(&self) {
        for submitter in self.each() {
            submitter.wait_below(output_limit(submitter));
        }
    }

    // The writer of the volume a file under `root` lies on.
    pub(crate) fn under(&self, root: &Path) -> &Arc<Submitter> {
        match &self.output {
            Some((output, submitter)) if root.starts_with(output) => submitter,
            _ => &self.working,
        }
    }

    pub(crate) fn working(&self) -> &Arc<Submitter> {
        &self.working
    }

    pub(crate) fn each(&self) -> impl Iterator<Item = &Arc<Submitter>> {
        std::iter::once(&self.working).chain(self.output.iter().map(|(_, submitter)| submitter))
    }

    // Everything the job submitted so far, on every volume it writes to.
    pub(crate) fn barrier(&self) -> JobBarrier {
        JobBarrier(self.each().map(|submitter| submitter.barrier()).collect())
    }

    // The loop's own wait on a volume's hard limit, for the one kind of
    // work in which the loop itself produces what is written: a repair's or
    // a replay's output read back from its staging file and said again,
    // slice after slice, in one step. Before the next slice is queued the
    // loop stands still while a volume holds its limit for its writer, so
    // no more than the limit and a slice is ever queued. Counted as a loop
    // wait on the volume. For those applications only: nothing else on the
    // loop may call it.
    //
    // A stop does not end the wait: what the loop says again while the
    // owner stops is held to the limit like any other. It ends at the end
    // the stop gives its volumes; false then, and the caller queues no
    // more of the job.
    #[must_use]
    pub(crate) fn loop_wait_below(&self) -> bool {
        use std::sync::atomic::Ordering;
        for submitter in self.each() {
            let volume = submitter.volume();
            let limit = output_limit(submitter);
            if volume.buffered_bytes.load(Ordering::Relaxed) < limit {
                continue;
            }
            let started = std::time::Instant::now();
            let below = submitter.loop_wait_below(limit.saturating_sub(1));
            volume.writer.loop_waited(
                crate::operations::disk::volume::LoopWait::Budget,
                started.elapsed().as_nanos() as u64,
            );
            if !below {
                return false;
            }
        }
        true
    }

    // Whether a wait of the loop on one of the job's volumes was given up
    // at the end of a stop (`Submitter::left`): the step that made it, and
    // every later one, stands on bytes that are not written.
    pub(crate) fn left_at_stop(&self) -> bool {
        self.each().any(|submitter| submitter.left())
    }

    // What the volumes the job writes to ask of its lanes: the most held
    // back of them, each read against its own limits.
    pub(in crate::pipeline) fn intake(&self) -> VolumeIntake {
        self.each()
            .map(|submitter| intake_of(submitter.volume()))
            .fold(VolumeIntake::Open, |most, intake| match (most, intake) {
                (VolumeIntake::Stopped, _) | (_, VolumeIntake::Stopped) => VolumeIntake::Stopped,
                _ => VolumeIntake::Open,
            })
    }

    // Whether everything the job submitted has returned, failed or not.
    pub(crate) fn quiet(&self) -> bool {
        self.each().all(|submitter| submitter.barrier().returned())
    }

    #[cfg(test)]
    pub(crate) fn failed(&self) -> bool {
        self.each().any(|submitter| submitter.failed())
    }

    pub(crate) fn heard_refusal(&self, root: &Path) {
        self.under(root).heard_refusal();
    }

    // The job is done with its files: nothing still queued for them runs.
    // The barrier passes once nothing of the job can touch the disk again.
    pub(crate) fn abandon(&self) -> JobBarrier {
        JobBarrier(self.each().map(|submitter| submitter.abandon()).collect())
    }
}

// What a test runs between the two looks `JobBarrier::passed_or_tell` takes
// at a barrier, on the thread that takes them: once, then it is gone.
#[cfg(test)]
pub(crate) mod between_looks {
    use std::cell::RefCell;

    thread_local! {
        static RUN: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    }

    pub(crate) fn set(run: impl FnOnce() + 'static) {
        RUN.with(|slot| *slot.borrow_mut() = Some(Box::new(run)));
    }

    pub(super) fn run() {
        let run = RUN.with(|slot| slot.borrow_mut().take());
        if let Some(run) = run {
            run();
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct JobBarrier(Vec<Barrier>);

impl JobBarrier {
    // Every operation behind it returned and none failed.
    pub(crate) fn passed(&self) -> bool {
        self.0.iter().all(Barrier::passed)
    }

    // Run `run` as the operations behind it return, on each volume's own
    // thread or here when they already have: once per volume, so it must
    // not mind running twice, and it must not hold the thread up.
    pub(crate) fn then(&self, run: impl Fn() + Send + Sync + 'static) {
        let run = Arc::new(run);
        for barrier in &self.0 {
            let run = Arc::clone(&run);
            barrier.then(move || run());
        }
    }

    // The job hears `DiskReturned` once the operations behind it returned.
    pub(crate) fn then_tell(
        &self,
        job_id: JobId,
        mail: &tokio::sync::mpsc::UnboundedSender<crate::pipeline::graph::engine::Said>,
    ) {
        let mail = mail.clone();
        self.then(move || {
            let _ = mail.send(crate::pipeline::graph::engine::Said {
                scope: job_id.0,
                node: weaver_graph::NodeId::ROOT,
                generation: 0,
                heard: crate::pipeline::graph::engine::Heard::DiskReturned,
            });
        });
    }

    // Whether the barrier has passed, for a job that steps back when it has
    // not and starts again on its mail: when it has not, the job hears
    // `DiskReturned` once the operations behind it returned, unless they
    // already had, failed or refused, which their own report says. Whether
    // they returned is looked at once, before whether the barrier passed:
    // operations that return on a volume's thread between the two looks
    // then either pass the barrier here or are told of. Looked at the other
    // way round, they left a barrier that had not passed and asked for no
    // mail, and the job waited for a mail that never came.
    pub(crate) fn passed_or_tell(
        &self,
        job_id: JobId,
        mail: &tokio::sync::mpsc::UnboundedSender<crate::pipeline::graph::engine::Said>,
    ) -> bool {
        let returned = self.returned();
        #[cfg(test)]
        between_looks::run();
        if self.passed() {
            return true;
        }
        if !returned {
            self.then_tell(job_id, mail);
        }
        false
    }

    // As `then_tell`, asking only the volumes whose operations are still
    // out: a job that asks again each time it hears is not answered at once
    // by a volume that already returned while another has not. The job
    // hears once when none is out.
    pub(crate) fn then_tell_of_the_rest(
        &self,
        job_id: JobId,
        mail: &tokio::sync::mpsc::UnboundedSender<crate::pipeline::graph::engine::Said>,
    ) {
        let tell = {
            let mail = mail.clone();
            move || {
                let _ = mail.send(crate::pipeline::graph::engine::Said {
                    scope: job_id.0,
                    node: weaver_graph::NodeId::ROOT,
                    generation: 0,
                    heard: crate::pipeline::graph::engine::Heard::DiskReturned,
                });
            }
        };
        let mut asked = false;
        for barrier in self.0.iter().filter(|barrier| !barrier.returned()) {
            asked = true;
            barrier.then(tell.clone());
        }
        if !asked {
            tell();
        }
    }

    // Every operation behind it returned, failed or not.
    pub(crate) fn returned(&self) -> bool {
        self.0.iter().all(Barrier::returned)
    }

    // Whether a test holds a volume behind it.
    #[cfg(test)]
    pub(crate) fn test_held(&self) -> bool {
        self.0.iter().any(Barrier::test_held)
    }

    // Wait until every operation behind it returned, on a thread the loop
    // does not wait for: a repair, a check or a replay on its own thread.
    pub(crate) fn wait_aside(&self) {
        for barrier in &self.0 {
            barrier.wait_aside();
        }
    }

    // What a thread asks after `wait_aside`: whether it must read nothing
    // back. See `Barrier::unwritten`.
    pub(crate) fn unwritten(&self) -> bool {
        self.0.iter().any(Barrier::unwritten)
    }

    // Wait until every operation behind it returned, counted against the
    // loop.
    #[cfg(test)]
    pub(crate) fn wait(&self) {
        for barrier in &self.0 {
            barrier.wait();
        }
    }

    // Wait until every operation behind it returned, on a thread that is
    // not the loop: not counted as a wait of any kind.
    #[cfg(test)]
    pub(crate) fn wait_returned(&self) {
        for barrier in &self.0 {
            let (returned, heard) = std::sync::mpsc::channel();
            barrier.then(move || {
                let _ = returned.send(());
            });
            heard.recv().expect("the volume's thread tells what returned");
        }
    }
}

#[cfg(test)]
mod tests;
