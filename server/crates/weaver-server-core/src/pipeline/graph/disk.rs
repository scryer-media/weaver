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
use crate::operations::disk::writer::{Barrier, Failure, Report, Submitter, VolumeWriters};
use crate::pipeline::Pipeline;
use crate::pipeline::graph::job::GraphError;
use crate::pipeline::graph::leaf::LeafError;
use crate::pipeline::graph::namespace::WriteError;
use crate::JobId;
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::job::GraphJob;

// The writers of the volumes `working_dir` and `output_dir` lie on, for the
// job `key`.
pub(crate) fn job_disk(
    writers: &VolumeWriters,
    capacity: &StorageCapacity,
    key: u64,
    working_dir: &Path,
    output_dir: Option<&Path>,
) -> JobDisk {
    let volume = capacity.volume(working_dir);
    let working = writers.submitter(&volume, key);
    let output = output_dir.and_then(|dir| {
        let other = capacity.volume(dir);
        (!Arc::ptr_eq(&other, &volume)).then(|| (dir.to_path_buf(), writers.submitter(&other, key)))
    });
    JobDisk::new(working, output)
}

impl Pipeline {
    // Whether a volume the job writes to holds the write budget or more for
    // its writer. Dispatch for the job stops while one does, and the batch
    // that brings it below wakes dispatch.
    pub(in crate::pipeline) fn volume_holds_budget(&self, job_id: JobId) -> bool {
        let held = self
            .graph_jobs
            .get(&job_id)
            .and_then(|graph| graph.disk())
            .is_some_and(|disk| disk.over(self.write_backlog_budget_bytes as u64));
        if held {
            self.volume_budget_held
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        held
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
        if self
            .retiring_graph_jobs
            .get(&job_id)
            .is_some_and(|graph| !graph.holds_retirement())
        {
            self.finish_graph_retirement(job_id);
            self.wake_repair_waiters();
            self.wake_memory_waiters();
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
    // would, and a refused name skips its member.
    pub(in crate::pipeline) fn graph_disk_failed(&mut self, failure: Failure) {
        let job_id = JobId(failure.key);
        let refused =
            failure.creating && failure.error.kind() == std::io::ErrorKind::InvalidFilename;
        if refused {
            let Some(graph) = self.graph_jobs.get_mut(&job_id) else {
                return;
            };
            let skipped = match failure.file {
                Some(file) => graph.name_refused(file),
                None => Ok(()),
            };
            // What the waiting checkpoints say of the file is undone by
            // the next one, and they are written only with it.
            graph.pending().await_correction();
            if let Some(disk) = graph.disk() {
                disk.heard_refusal(&failure.path);
            }
            self.after_graph_result(job_id, skipped);
            return;
        }
        if !self.graph_jobs.contains_key(&job_id) {
            return;
        }
        tracing::warn!(
            job_id = job_id.0,
            path = %failure.path.display(),
            error = %failure.error,
            "a volume's writer could not complete a file operation"
        );
        let error = GraphError::Write(WriteError::Leaf(LeafError::Io {
            path: failure.path,
            source: failure.error,
        }));
        self.after_graph_result(job_id, Err(error));
    }

    // A volume's writer finished a batch: dispatch that stopped for the
    // volume's budget looks again.
    pub(in crate::pipeline) fn volume_batch_returned(&mut self, report: Report) {
        for failure in report.failures {
            self.graph_disk_failed(failure);
        }
        if self
            .volume_budget_held
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            self.download_dispatch_wake = true;
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct JobDisk {
    // The volume the working directory lies on.
    working: Arc<Submitter>,
    // Where the delivered files lie and their volume, when it is another.
    output: Option<(PathBuf, Arc<Submitter>)>,
}

impl JobDisk {
    pub(crate) fn new(working: Arc<Submitter>, output: Option<(PathBuf, Arc<Submitter>)>) -> Self {
        Self { working, output }
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

    fn each(&self) -> impl Iterator<Item = &Arc<Submitter>> {
        std::iter::once(&self.working).chain(self.output.iter().map(|(_, submitter)| submitter))
    }

    // Everything the job submitted so far, on every volume it writes to.
    pub(crate) fn barrier(&self) -> JobBarrier {
        JobBarrier(self.each().map(|submitter| submitter.barrier()).collect())
    }

    // Whether a volume the job writes to holds `budget` bytes or more for
    // its writer.
    pub(crate) fn over(&self, budget: u64) -> bool {
        self.each().any(|submitter| {
            submitter
                .volume()
                .buffered_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
                >= budget
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

    // Every operation behind it returned, failed or not.
    #[cfg(test)]
    pub(crate) fn returned(&self) -> bool {
        self.0.iter().all(Barrier::returned)
    }

    // Whether a test holds a volume behind it.
    #[cfg(test)]
    pub(crate) fn test_held(&self) -> bool {
        self.0.iter().any(Barrier::test_held)
    }

    // Wait until every operation behind it returned. For a thread that may
    // wait on the disk.
    pub(crate) fn wait(&self) {
        for barrier in &self.0 {
            barrier.wait();
        }
    }
}

#[cfg(test)]
mod tests;
