// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's articles exist only while it runs.
//
// A job waiting its turn holds its files' names and identities and, per file,
// how many articles it has and their size: nothing per article, and nothing
// charged to the process ledger. The first time the scheduler reaches it, the
// job becomes activating and its article lists are read back from the NZB the
// job was admitted with, and its graph from its checkpoint when it has one,
// off the pipeline thread. When they arrive the job's assembly, queues, graph
// root and repeated-article index are built in
// [`Pipeline::activate_job_articles`], the one place they are built, and their
// whole size is charged. They are dropped, and the charge released, in
// [`Pipeline::release_job_articles`] when the job leaves the pipeline, or
// once a paused job has nothing left out ([`Pipeline::park_job_articles`]).
// A paused job is waiting its turn again, and is activated afresh when it is
// next reached, as a restart restores it, keeping the parts that arrived and
// wait to be placed.

use super::*;
use crate::jobs::model::{ActiveArticles, FileSummaries, FileSummary, JobArticles, SegmentSpec};
use crate::jobs::persistence::JobsPersistenceInternalQueries;
use crate::jobs::persistence::JobsPersistenceQueries;
use crate::jobs::queries::JobsQueries;
use crate::pipeline::download::queue::DownloadQueue;
use crate::pipeline::download::repeated::RepeatedArticles;
use crate::pipeline::graph::restore::{GraphRestore, GraphRestoreError, RestoreSeat, root_files};
use crate::pipeline::held::HeldPart;

// A point a move stops at, for a test, until the test opens it.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
#[derive(Default)]
pub(crate) struct MoveGate {
    // Reached, and opened.
    state: std::sync::Mutex<(bool, bool)>,
    moved: std::sync::Condvar,
}

#[cfg(any(test, feature = "pipeline-test-fixtures"))]
impl MoveGate {
    // On a blocking thread: say it is reached, and wait until it is open.
    pub(crate) fn pass(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.moved.notify_all();
        while !state.1 {
            state = self.moved.wait(state).unwrap();
        }
    }

    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn open(&self) {
        self.state.lock().unwrap().1 = true;
        self.moved.notify_all();
    }

    // Until a move has reached it.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) async fn reached(self: &Arc<Self>) {
        let gate = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let mut state = gate.state.lock().unwrap();
            while !state.0 {
                state = gate.moved.wait(state).unwrap();
            }
        })
        .await
        .unwrap();
    }
}

// Where a test may stop a move: `moving`, just before the move's last step
// on disk (leaving the old folder, or giving up the new one), and
// `leaving`, just before the removal of a folder a move claimed for a job
// that left.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
#[derive(Default)]
pub(crate) struct MoveGates {
    pub(crate) moving: Option<Arc<MoveGate>>,
    pub(crate) leaving: Option<Arc<MoveGate>>,
}

// What the read of one job's stored NZB produced.
pub(crate) struct JobActivated {
    pub(crate) job_id: JobId,
    // The number the job's activation was told by when the read was sent.
    pub(crate) activation: u64,
    // The working directory a job that never started moved to, under the
    // intermediate folder set now. It moved whatever the read found.
    pub(crate) moved_to: Option<PathBuf>,
    // The working directory the job had when the read was sent: a move is
    // taken only by what still names this one.
    pub(crate) started_in: PathBuf,
    pub(crate) result: Result<ReadArticles, String>,
}

// Whether a move of a job's working directory is out, was cancelled while
// out, or is done with. A cancel marks it under the queue's lock in the same
// write that holds the job's cleanup, so the guard that ends it after that
// write always lets the job go, and a guard that ended first holds nothing.
pub(crate) struct MoveWatch(std::sync::atomic::AtomicU8);

const MOVE_OUT: u8 = 0;
const MOVE_CANCELLED: u8 = 1;
const MOVE_ENDED: u8 = 2;

impl MoveWatch {
    fn new() -> Self {
        Self(std::sync::atomic::AtomicU8::new(MOVE_OUT))
    }

    // The job is cancelled while the move is out: true when the move is
    // still out, and its guard will let the job go.
    pub(crate) fn cancel(&self) -> bool {
        self.0
            .compare_exchange(
                MOVE_OUT,
                MOVE_CANCELLED,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
    }
}

// The guard of one holder of a job's pending cleanup: dropped, however its
// work ends, it lets that holder go. A move's guard carries the move's
// watch, and lets the job go only when it was cancelled while the move was
// out: nothing is written for a move whose job was not cancelled.
pub(crate) struct CleanupHold {
    queue: Arc<crate::pipeline::queue::Queue>,
    job_id: JobId,
    holder: crate::pipeline::queue::CleanupHolder,
    watch: Option<Arc<MoveWatch>>,
}

impl Drop for CleanupHold {
    fn drop(&mut self) {
        let held = self.watch.as_ref().is_none_or(|watch| {
            watch
                .0
                .swap(MOVE_ENDED, std::sync::atomic::Ordering::AcqRel)
                == MOVE_CANCELLED
        });
        if held {
            self.queue.release_cleanup(self.job_id, self.holder);
        }
    }
}

pub(crate) struct ReadArticles {
    // Every file's article list, in the spec's file order.
    lists: Vec<Vec<SegmentSpec>>,
    repeated: Option<Arc<RepeatedArticles>>,
    // The job's graph rebuilt from its checkpoint, and the articles that
    // checkpoint has in hand; none for a job never checkpointed.
    restored: Option<GraphRestore>,
    // The staging directory a job starting afresh delivers into, chosen for
    // where it delivers now: None when it stages nothing. A job its
    // checkpoint brings back keeps the one it has.
    staging: Option<Option<PathBuf>>,
    // The volumes of the job's directories, read off the loop before the
    // job is admitted: its writes go through these, whatever memory says of
    // the paths by then.
    volumes: crate::pipeline::graph::disk::ReadVolumes,
    // That staging directory, when it is not the one the job had: made by
    // this read, and the job's to lose should the job leave first.
    made_staging: Option<PathBuf>,
}

#[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
impl ReadArticles {
    // Whether the job was built again from its checkpoint.
    pub(crate) fn restored(&self) -> bool {
        self.restored.is_some()
    }
}

// Where a job delivers as its articles are read, so a job starting afresh
// stages on its destination's device.
struct StagingChoice {
    current: Option<PathBuf>,
    intermediate_dir: PathBuf,
    complete_dir: PathBuf,
    config: crate::settings::SharedConfig,
    category: Option<String>,
    // The disk owner, which answers whether the intermediate directory and
    // the destination share a device.
    storage: Arc<crate::operations::StorageCapacity>,
}

// What the staging choice needs settled before its blocking part: the
// destination, and whether it shares the intermediate directory's device.
struct StagingDestination {
    root: PathBuf,
    one_device: bool,
    // The kind of storage under the destination and under the intermediate
    // directory, as the disk owner holds them, for the log.
    root_kind: weaver_server_disk::operations::disk::StorageKind,
    intermediate_kind: weaver_server_disk::operations::disk::StorageKind,
}

impl StagingChoice {
    // The job's destination, and whether it shares the intermediate
    // directory's device, from the disk owner: no disk call when both
    // devices are current, else one shared look for each that is not.
    async fn destination(&self) -> StagingDestination {
        let root = {
            let config = self.config.read().await;
            crate::pipeline::queue::destination_root(
                &self.complete_dir,
                &config.categories,
                self.category.as_deref(),
            )
        };
        let one_device = self
            .storage
            .same_filesystem(&self.intermediate_dir, &root)
            .await;
        StagingDestination {
            root_kind: self.storage.kind(&root),
            intermediate_kind: self.storage.kind(&self.intermediate_dir),
            root,
            one_device,
        }
    }

    // The staging directory for the job's destination, made and marked the
    // job's, and the one it had gone when that is another. One that cannot
    // be made leaves the job delivering from its working directory. A staging
    // directory that already holds delivered files is kept: the final move
    // carries them to wherever the job then delivers.
    fn choose(self, job_id: JobId, destination: StagingDestination) -> Option<PathBuf> {
        // Read without following a symbolic link: a staging directory swapped
        // for a link holds nothing of the job's, and is not delivered into.
        if let Some(current) = self.current.as_ref() {
            match crate::jobs::working_dir::job_staging_dir_holds_files(current, job_id) {
                Ok(true) => return self.current,
                Ok(false) => {}
                Err(error) => {
                    warn!(job_id = job_id.0, dir = %current.display(), %error, "could not read the job's staging directory; it is not delivered into");
                }
            }
        }
        let wanted = crate::pipeline::queue::staging_dir_on(
            destination.one_device,
            &destination.root,
            job_id,
        );
        if let Some(current) = self
            .current
            .as_ref()
            .filter(|dir| Some(*dir) != wanted.as_ref())
            && let Err(error) = crate::jobs::working_dir::remove_job_staging_dir(current, job_id)
        {
            warn!(job_id = job_id.0, dir = %current.display(), %error, "could not remove a staging directory the job no longer delivers into");
        }
        let staging = wanted.filter(|staging| {
            crate::pipeline::queue::make_staging_dir(staging, job_id)
                .inspect_err(|error| {
                    warn!(job_id = job_id.0, dir = %staging.display(), %error, "could not make the staging directory; the job delivers from its working directory");
                })
                .is_ok()
        });
        // A staging directory lies on its destination's storage; without
        // one, the job writes to the intermediate directory's.
        let output_kind = if staging.is_some() {
            destination.root_kind
        } else {
            destination.intermediate_kind
        };
        info!(
            job_id = job_id.0,
            storage = output_kind.as_str(),
            split = staging.is_some(),
            "output storage"
        );
        staging
    }
}

// A job that never started takes a working directory under the intermediate
// folder set now, in the order that leaves its stored row naming a folder
// that exists whatever stops in between: the new one is claimed and marked,
// the row names it once that write commits, and only then the old one goes.
// The row takes the new one only while it still names the one the job
// started in. A row that cannot be written, or did not take it, gives the
// new one up, and the job keeps what it has.
async fn follow_working_dir(
    db: &crate::Database,
    job_id: JobId,
    root: PathBuf,
    current: PathBuf,
    name: String,
    #[cfg(any(test, feature = "pipeline-test-fixtures"))] gate: Option<Arc<MoveGate>>,
) -> Option<PathBuf> {
    let from = current.clone();
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    let seam_root = root.clone();
    let followed = tokio::task::spawn_blocking(move || {
        crate::jobs::working_dir::follow_intermediate_dir(&root, &current, job_id, &name)
    })
    .await
    .map_err(std::io::Error::other)
    .and_then(|followed| followed);
    let followed = match followed {
        Ok(followed) => followed?,
        Err(error) => {
            warn!(job_id = job_id.0, dir = %from.display(), %error, "could not move the job's working directory to the intermediate folder; it keeps the one it has");
            return None;
        }
    };
    let dir = followed.path.clone();
    let stored = dir.clone();
    let moved = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let moved_in_row = Arc::clone(&moved);
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    move_seams::before_row_write(&seam_root).await;
    let recorded =
        match db.try_queue_job_write_committed(job_id, "move_active_job_output_dir", move |db| {
            let took = db.move_active_job_output_dir(job_id, &from, &stored)?;
            moved_in_row.store(took, std::sync::atomic::Ordering::Release);
            Ok(())
        }) {
            Ok(committed) => committed
                .await
                .map_err(|error| error.to_string())
                .and_then(|written| written.map_err(|error| error.to_string())),
            Err(error) => Err(error.to_string()),
        };
    if let Err(error) = &recorded {
        warn!(job_id = job_id.0, dir = %dir.display(), %error, "could not record the job's new working directory; it keeps the one it has");
    }
    // The row is the arbiter: a row that no longer names the directory the
    // job started in, or has gone, did not move, and the claim is given up.
    let recorded = recorded.is_ok() && moved.load(std::sync::atomic::Ordering::Acquire);
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    if !recorded {
        move_seams::before_abandon(&seam_root).await;
    }
    let _ = tokio::task::spawn_blocking(move || {
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        if let Some(gate) = gate {
            gate.pass();
        }
        if recorded {
            followed.leave_old();
        } else {
            followed.abandon();
        }
    })
    .await;
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    if !recorded {
        move_seams::after_abandon(&seam_root);
    }
    recorded.then_some(dir)
}

impl Pipeline {
    // Whether the job is waiting for its articles to be built: their read is
    // out, its earlier graph still has something out on a thread, or an
    // earlier activation is still moving its working directory.
    pub(in crate::pipeline) fn awaits_job_articles(&self, job_id: JobId) -> bool {
        self.retiring_graph_jobs.contains_key(&job_id)
            || self.moving_jobs.contains_key(&job_id)
            || self
                .jobs
                .get(&job_id)
                .is_some_and(|state| matches!(state.articles, JobArticles::Activating(..)))
    }

    // Start reading a queued job's articles back. A job already activating
    // or active is left as it is, so the scheduler may ask on every pass. So
    // is a job whose earlier graph still has something out on a thread: its
    // new graph is not built over the old one's runs. So is a job an earlier
    // activation is still moving the working directory of: the next one
    // reads the directory that move left it.
    pub(in crate::pipeline) fn request_job_activation(&mut self, job_id: JobId) {
        if self.retiring_graph_jobs.contains_key(&job_id) || self.moving_jobs.contains_key(&job_id)
        {
            return;
        }
        self.next_activation += 1;
        let activation = self.next_activation;
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let summaries = match &mut state.articles {
            JobArticles::Queued(summaries) => std::mem::take(summaries),
            JobArticles::Activating(..) | JobArticles::Active(_) => return,
        };
        let category = state.spec.category.clone();
        let metadata = state.spec.metadata.clone();
        let working_dir = state.working_dir.clone();
        let staging_dir = state.staging_dir.clone();
        let spec_password = state.spec.password.clone();
        let expected = summaries.clone();
        // Lists read ahead are taken with the job's files, to be checked
        // as any read is. They are unpacked on the read's blocking thread.
        let ahead = self
            .next_up
            .take(job_id)
            .map(|held| (state.spec.clone(), held));
        let staging = StagingChoice {
            current: staging_dir.clone(),
            intermediate_dir: self.intermediate_dir.clone(),
            complete_dir: self.complete_dir.clone(),
            config: Arc::clone(&self.config),
            category: category.clone(),
            storage: Arc::clone(&self.storage_capacity),
        };
        // A job that never started follows the intermediate folder set now,
        // and is given a working directory there again when the one it had
        // is gone; one that started keeps the working directory it has.
        let follow = Some((self.intermediate_dir.clone(), state.spec.name.clone()));
        state.articles = JobArticles::Activating(summaries, activation);
        let follow = follow.filter(|_| {
            !self
                .shared_state
                .queue
                .read(|inner| inner.row(job_id).is_none_or(|row| self.job_started(row)))
        });
        // The move is out from here until its last step on disk returned,
        // and so is its hold, which a cancel meanwhile turns into a hold on
        // the job's cleanup. A folder the reply then removes is counted out
        // again, in the write that judged it.
        let (move_out, hold) = follow
            .is_some()
            .then(|| {
                let watch = Arc::new(MoveWatch::new());
                self.moving_jobs
                    .insert(job_id, (activation, Arc::clone(&watch)));
                let move_out =
                    crate::pipeline::queue::MoveOut::take(&self.shared_state.queue, job_id);
                let hold = CleanupHold {
                    queue: Arc::clone(&self.shared_state.queue),
                    job_id,
                    holder: crate::pipeline::queue::CleanupHolder::Move,
                    watch: Some(watch),
                };
                (move_out, hold)
            })
            .unzip();
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        let gate = self.move_gates.moving.clone();
        let seat = self
            .graph_restore_seat(None, &working_dir)
            .for_job(spec_password, &working_dir, staging_dir.as_deref())
            .lifted(&metadata);
        // The read waits for every write already queued for the job, so a
        // checkpoint taken as the job paused is the one it reads back.
        let (written_tx, written_rx) = tokio::sync::oneshot::channel();
        if let Err(error) = self
            .db
            .try_queue_job_write(job_id, "job_articles_read", move |_| {
                let _ = written_tx.send(());
                Ok(())
            })
        {
            warn!(job_id = job_id.0, error = %error, "could not queue the job's article read behind its writes");
        }
        let db = self.db.clone();
        let memory = Arc::clone(&self.process_memory_budget);
        let tx = self.job_activated_tx.clone();
        tokio::spawn(async move {
            let _ = written_rx.await;
            let started_in = working_dir.clone();
            let moved_to = match follow {
                Some((root, name)) => {
                    follow_working_dir(
                        &db,
                        job_id,
                        root,
                        working_dir,
                        name,
                        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
                        gate,
                    )
                    .await
                }
                None => None,
            };
            drop(move_out);
            drop(hold);
            let moved = moved_to.clone();
            let result = tokio::task::spawn_blocking(move || {
                let seat = match moved {
                    Some(dir) => seat.moved_to(dir),
                    None => seat,
                };
                read_job_articles(
                    &db, job_id, category, metadata, &expected, ahead, memory, seat,
                )
            })
            .await
            .unwrap_or_else(|error| Err(format!("article read worker failed: {error}")));
            // A job restored from its checkpoint keeps the staging it had,
            // and asks nothing of the disk.
            let result = match result {
                Ok(mut read) if read.restored.is_none() => {
                    let destination = staging.destination().await;
                    // Where the job works from here on: where its directory
                    // was moved to, or where it began.
                    let working_dir = moved_to.clone().unwrap_or_else(|| started_in.clone());
                    tokio::task::spawn_blocking(move || {
                        // This thread may wait on a disk: the volumes of the
                        // job's directories are read here when memory does
                        // not cover them, so the loop, which admits the job
                        // from memory alone, never gives it the volume of an
                        // unknown device.
                        let storage = Arc::clone(&staging.storage);
                        let had = staging.current.clone();
                        let chosen = staging.choose(job_id, destination);
                        read.made_staging = chosen.clone().filter(|dir| had.as_ref() != Some(dir));
                        read.volumes
                            .push((working_dir.clone(), storage.read_volume(&working_dir)));
                        if let Some(dir) = &chosen {
                            read.volumes.push((dir.clone(), storage.read_volume(dir)));
                        }
                        read.staging = Some(chosen);
                        read
                    })
                    .await
                    .map_err(|error| format!("staging choice worker failed: {error}"))
                }
                other => other,
            };
            let _ = tx
                .send(JobActivated {
                    job_id,
                    activation,
                    moved_to,
                    started_in,
                    result,
                })
                .await;
        });
    }

    // The installed job and its queue row take the directory a move made
    // where they still name the one it started in. False when the directory
    // is to go: nothing took it, nothing names it, and no one is working
    // with the id. An id being admitted, or held by a history operation,
    // may have taken the directory as its own, so it stays, and that job's
    // own cleanup takes any directory that carries its marker.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn take_moved_dir(&mut self, job_id: JobId, started_in: &Path, dir: &Path) -> bool {
        self.take_or_hold_moved_dir(job_id, started_in, dir, false)
    }

    // `take_moved_dir`; with `hold`, a directory that is to go is counted
    // as a move out for the job in the same write that judged it, so no one
    // who starts working with the id after that write removes or makes the
    // job's folders before the removal returned. The caller then owns that
    // count, as a `MoveOut`.
    fn take_or_hold_moved_dir(
        &mut self,
        job_id: JobId,
        started_in: &Path,
        dir: &Path,
        hold: bool,
    ) -> bool {
        let mut kept = false;
        if let Some(state) = self.jobs.get_mut(&job_id) {
            kept |= state.working_dir == dir;
            if state.working_dir == started_in {
                state.working_dir = dir.to_path_buf();
                kept = true;
            }
        }
        let (from, to) = (started_in.display().to_string(), dir.display().to_string());
        self.shared_state.queue.write(|inner| {
            let busy =
                inner.admissions.contains(&job_id) || inner.history_operations.contains(&job_id);
            let (taken, changed) = match inner.job_mut(job_id) {
                Some(mut row)
                    if !row.report.retiring
                        && row.report.job.output_dir.as_deref() == Some(from.as_str()) =>
                {
                    row.report.job.output_dir = Some(to);
                    (true, true)
                }
                Some(row) => (
                    kept || busy || row.report.job.output_dir.as_deref() == Some(to.as_str()),
                    false,
                ),
                None => (kept || busy, false),
            };
            if hold && !taken {
                *inner.moves_out.entry(job_id).or_default() += 1;
            }
            if changed {
                crate::pipeline::queue::Change::changed(taken)
            } else {
                crate::pipeline::queue::Change::unchanged(taken)
            }
        })
    }

    // The read came back: build the job's articles, or fail the job if they
    // could not be read. Only the reply to the job's current activation is
    // heard: a job that left the pipeline, paused, or was activated again
    // meanwhile drops what was read with this message.
    pub(crate) fn handle_job_activated(&mut self, done: JobActivated) {
        let JobActivated {
            job_id,
            activation,
            moved_to,
            started_in,
            result,
        } = done;
        // The activation that had the job's working directory to move is
        // heard: the job may be sent for again.
        if self
            .moving_jobs
            .get(&job_id)
            .is_some_and(|(current, _)| *current == activation)
        {
            self.moving_jobs.remove(&job_id);
            self.download_dispatch_wake = true;
        }
        // A move is on disk, and on the job's stored row, whichever
        // activation it came with and whether or not the job is still
        // installed. The installed job and its queue row take the new
        // directory only where they still name the one the move started in:
        // a row being retired, and a job added again under the same id, are
        // left as they are. A directory nothing took, and nothing names, is
        // of a job that left, and goes as that job's own cleanup would have
        // taken it.
        let left_dir = moved_to
            .filter(|dir| !self.take_or_hold_moved_dir(job_id, &started_in, dir, true))
            .map(|dir| {
                let out =
                    crate::pipeline::queue::MoveOut::counted(&self.shared_state.queue, job_id);
                (dir, out)
            });
        let heard = self.jobs.get(&job_id).is_some_and(|state| {
            matches!(state.articles, JobArticles::Activating(_, current) if current == activation)
        });
        // A staging directory this read made for a job that has left, with
        // no row and no one working with the id, goes too: an id being
        // admitted or restarted owns any staging directory with its marker.
        // The same write holds the id's cleanup until it is gone, so no one
        // admitted after it makes that directory again under the removal.
        let left_staging = match &result {
            Ok(read) if !heard && !self.jobs.contains_key(&job_id) => {
                read.made_staging.clone().filter(|_| {
                    self.shared_state.queue.write(|inner| {
                        let free = inner.row(job_id).is_none()
                            && !inner.admissions.contains(&job_id)
                            && !inner.history_operations.contains(&job_id);
                        if free {
                            let holder = crate::pipeline::queue::CleanupHolder::Staging;
                            inner.pending_cleanup.hold(job_id, holder);
                        }
                        crate::pipeline::queue::Change::unchanged(free)
                    })
                })
            }
            _ => None,
        }
        .map(|staging| {
            let hold = CleanupHold {
                queue: Arc::clone(&self.shared_state.queue),
                job_id,
                holder: crate::pipeline::queue::CleanupHolder::Staging,
                watch: None,
            };
            (staging, hold)
        });
        // Both go off the actor, their counts and holds with them.
        if left_dir.is_some() || left_staging.is_some() {
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            let gate = self
                .move_gates
                .leaving
                .clone()
                .filter(|_| left_dir.is_some());
            tokio::task::spawn_blocking(move || {
                #[cfg(any(test, feature = "pipeline-test-fixtures"))]
                if let Some(gate) = gate {
                    gate.pass();
                }
                if let Some((dir, _out)) = left_dir
                    && let Some(root) = dir.parent()
                    && let Err(error) =
                        crate::jobs::working_dir::remove_job_working_dir(root, &dir, job_id)
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(job_id = job_id.0, dir = %dir.display(), %error, "could not remove the working directory claimed for a job that left");
                }
                if let Some((staging, _staging_hold)) = left_staging {
                    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
                    move_seams::before_staging_removal(&staging);
                    // The read named it, not necessarily made it: one that
                    // holds files, or cannot be read, is kept.
                    match crate::jobs::working_dir::job_staging_dir_holds_files(&staging, job_id) {
                        Ok(false) => {
                            if let Err(error) =
                                crate::jobs::working_dir::remove_job_staging_dir(&staging, job_id)
                            {
                                warn!(job_id = job_id.0, dir = %staging.display(), %error, "could not remove the staging directory made for a job that left");
                            }
                        }
                        held => {
                            tracing::debug!(job_id = job_id.0, dir = %staging.display(), ?held, "kept the staging directory of a job that left: it holds files or cannot be read");
                        }
                    }
                    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
                    move_seams::after_staging_removal(&staging);
                }
            });
        }
        if !heard {
            return;
        }
        match result {
            Ok(read) => {
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    for (file, list) in state.spec.files.iter_mut().zip(read.lists) {
                        file.segments = list;
                    }
                    if let Some(staging) = read.staging
                        && staging != state.staging_dir
                    {
                        state.staging_dir = staging.clone();
                        if let Err(error) = self.db.try_queue_job_write(
                            job_id,
                            "set_active_job_staging_dir",
                            move |db| db.set_active_job_staging_dir(job_id, staging.as_deref()),
                        ) {
                            warn!(job_id = job_id.0, error = %error, "could not record the job's staging directory");
                        }
                    }
                }
                let (graph, skip, held) = match read.restored {
                    Some(GraphRestore {
                        job,
                        skip,
                        held,
                        feed_verdict,
                        in_hand_bytes,
                    }) => {
                        self.post_probes.restore_verdict(job_id, feed_verdict);
                        // What the checkpoint has in hand is downloaded, in
                        // the decoded units a run counts in.
                        if let Some(state) = self.jobs.get_mut(&job_id) {
                            state.downloaded_bytes = state.downloaded_bytes.max(in_hand_bytes);
                            state.restored_download_floor_bytes =
                                state.restored_download_floor_bytes.max(in_hand_bytes);
                        }
                        (Some(job), skip, held)
                    }
                    None => (None, HashSet::new(), Vec::new()),
                };
                self.activate_job_articles(
                    job_id,
                    &skip,
                    graph,
                    read.repeated,
                    held,
                    &read.volumes,
                );
                self.report_job(job_id);
                self.download_dispatch_wake = true;
            }
            Err(error) => {
                self.fail_job(job_id, format!("job articles could not be read: {error}"));
            }
        }
    }

    // The one place a job's articles are built: its assembly and queues from
    // the article lists its spec now holds, less the articles in `skip`
    // already in hand and the parts that arrived and wait to be placed, its
    // graph root (restored from a checkpoint, or admitted afresh), the held
    // parts its checkpoint names, and its repeated-article index. Everything
    // built is charged, never refused, and held until the job pauses or
    // leaves the pipeline.
    pub(crate) fn activate_job_articles(
        &mut self,
        job_id: JobId,
        skip: &HashSet<SegmentId>,
        graph: Option<crate::pipeline::graph::job::GraphJob>,
        repeated: Option<Arc<RepeatedArticles>>,
        held_parts: Vec<HeldPart>,
        volumes: &[(
            PathBuf,
            Arc<weaver_server_disk::operations::disk::volume::Volume>,
        )],
    ) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let mut held = Self::held_parts(&self.uu_files, &self.unanchored_parked, job_id);
        held.extend(held_parts.iter().map(HeldPart::segment_id));
        let (mut assembly, download_queue) =
            Self::build_job_assembly(job_id, &state.spec, skip, &held);
        // A graph root brought back holds the articles it placed. Each one's
        // placement is recorded again, so an article that arrives now is
        // checked against its neighbours exactly as it would have been
        // before the job left the pipeline.
        if let Some(graph) = &graph {
            for file in assembly.files_mut() {
                let volume = weaver_graph::VolumeIndex(file.file_id().file_index);
                for (ordinal, range) in graph.placed_intact(volume) {
                    if file.has_segment(ordinal)
                        && let Ok(len) = u32::try_from(range.end - range.start)
                    {
                        file.record_placement(ordinal, range.start, len);
                    }
                }
            }
        }
        state.articles = JobArticles::Active(Box::new(ActiveArticles {
            assembly,
            download_queue,
            recovery_queue: DownloadQueue::new(),
        }));
        if let Some(cache) = repeated {
            self.repeated_articles.insert(job_id, cache);
        }
        self.charge_job_article_memory(job_id);
        match graph {
            Some(graph) => {
                self.install_graph_job(job_id, graph, held_parts);
                self.post_restored(job_id);
            }
            None => self.admit_graph_job(job_id, volumes),
        }
        self.abandon_dead_files(job_id);
    }

    // The one place a job's articles end, as a paused job parks or a job
    // leaves the pipeline: its article lists, assembly and queues, its graph
    // runtime, its repeated-article index, the retries kept for its
    // articles, and their charge. A job that stays holds its summaries again,
    // counting as still to fetch what its graph root has not settled and no
    // park holds, as its next activation would, and a read of its articles
    // still out is disowned. The caller removes a job that leaves.
    pub(crate) fn release_job_articles(&mut self, job_id: JobId) {
        let graph = self.graph_jobs.get(&job_id);
        if let (Some(graph), Some(state)) = (graph, self.jobs.get(&job_id)) {
            let figures = Self::graph_remaining(graph, &state.spec);
            self.released_remaining.insert(job_id, figures);
        }
        let held = Self::held_parts(&self.uu_files, &self.unanchored_parked, job_id);
        let summaries = self
            .jobs
            .get_mut(&job_id)
            .and_then(|state| match &mut state.articles {
                JobArticles::Queued(_) => None,
                JobArticles::Activating(summaries, _) => Some(std::mem::take(summaries)),
                JobArticles::Active(_) => {
                    Some(state.spec.drop_article_lists_with_in_hand(|file, ordinal| {
                        graph.is_some_and(|graph| {
                            graph.article_settled(weaver_graph::VolumeIndex(file), ordinal)
                        }) || held.contains(&SegmentId {
                            file_id: NzbFileId {
                                job_id,
                                file_index: file,
                            },
                            segment_number: ordinal,
                        })
                    }))
                }
            });
        self.job_article_memory.remove(&job_id);
        self.repeated_articles.remove(&job_id);
        self.retire_graph_job(job_id);
        self.pending_retries_by_job.remove(&job_id);
        self.pending_retries_by_segment
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.transport_failure_streaks
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.cancel_infrastructure_retries_for_job(job_id);
        self.decode_retries
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        if let (Some(summaries), Some(state)) = (summaries, self.jobs.get_mut(&job_id)) {
            state.articles = JobArticles::Queued(summaries);
        }
    }

    // A paused job holds nothing per article, once nothing of it is out: what
    // a connection or the decoder still has for it comes back and is taken
    // as a running job takes it, so no article that arrives is fetched
    // again. Every part that arrived and waits to be placed is kept, so no
    // delivered article is fetched again: those in memory spill to the job's
    // spool first, on the next turn, so the checkpoint names them. Then its
    // checkpoint is taken, so it holds every article in hand, and its
    // articles are released, with no terminal state kept per article: its
    // graph root records the articles given up. With nothing out, no
    // article of it is in flight for the checkpoint. A job resumed before
    // that keeps its articles, and runs on with them.
    pub(crate) fn park_job_articles(&mut self, job_id: JobId) {
        self.parking_jobs.insert(job_id);
        self.park_drained_jobs();
    }

    // Park every paused job, and every job held for a full disk, with nothing
    // left out, a paused one once its held parts are spooled. Each turn asks.
    pub(crate) fn park_drained_jobs(&mut self) {
        if self.parking_jobs.is_empty() {
            return;
        }
        let parking: Vec<JobId> = self.parking_jobs.iter().copied().collect();
        for job_id in parking {
            let paused = self
                .jobs
                .get(&job_id)
                .is_some_and(|state| state.run_state == crate::jobs::model::RunState::Paused);
            let full_disk = self.held_for_full_disk(job_id);
            if (paused || full_disk) && self.job_has_results_out(job_id) {
                continue;
            }
            self.parking_jobs.remove(&job_id);
            // A job held for a full disk has no graph to checkpoint: it parks
            // as it is, what it kept past its checkpoint let go, and goes on at once if a reading
            // since its write already shows room.
            if full_disk {
                self.forget_parts_past_checkpoint(job_id);
                self.park_drained_job(job_id);
                self.settle_held_summaries(job_id);
                self.wake_full_disk();
                continue;
            }
            if !paused {
                continue;
            }
            if self.job_holds_parts_in_memory(job_id) {
                self.spilling_jobs.insert(job_id);
            } else {
                self.park_drained_job(job_id);
            }
        }
    }

    // The spill turn has spooled what of the job's held parts the spool
    // took: the job parks, unless something of it came out meanwhile, when
    // it waits to be drained again. A part the spool could not take stays in
    // memory, kept across the park and left out of the checkpoint.
    pub(crate) fn park_spilled_job(&mut self, job_id: JobId) {
        if self.job_has_results_out(job_id) {
            self.parking_jobs.insert(job_id);
            return;
        }
        self.park_drained_job(job_id);
    }

    fn park_drained_job(&mut self, job_id: JobId) {
        debug_assert!(
            !self.checkpoint_progress_articles.contains_key(&job_id),
            "a drained job has no article in flight for its checkpoint"
        );
        // What the graph holds in RAM, bytes of a member whose front has not
        // yet said what it is among them, goes to scratch as a stop pages
        // it, so the checkpoint names it and the resumed job reads it back.
        if let Some(graph) = self.graph_jobs.get_mut(&job_id)
            && let Err(error) = graph.page_out_holds()
        {
            warn!(job_id = job_id.0, %error, "could not page graph holds out for a pause");
        }
        self.checkpoint_graph_job(job_id);
        self.release_job_articles(job_id);
        self.segment_terminal_states
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.given_up.remove(&job_id);
        self.report_job(job_id);
    }

    // Whether a part of the job that arrived and waits to be placed is held
    // only in memory.
    fn job_holds_parts_in_memory(&self, job_id: JobId) -> bool {
        let uu = self
            .uu_files
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .any(|(_, uu)| uu.parked.values().any(|part| !part.bytes.is_spilled()));
        uu || self
            .unanchored_parked
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .any(|(_, parked)| parked.values().any(|part| !part.bytes.is_spilled()))
    }

    // The parts of a job that arrived and wait to be placed: uuencode parts
    // ahead of their file's cursor, and articles that declared no usable
    // start ahead of the ordinal they follow.
    fn held_parts(
        uu_files: &HashMap<NzbFileId, UuFileAssembly>,
        unanchored_parked: &UnanchoredPark,
        job_id: JobId,
    ) -> HashSet<SegmentId> {
        let uu = uu_files
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .flat_map(|(file_id, uu)| uu.parked.keys().map(move |ordinal| (*file_id, *ordinal)));
        let unanchored = unanchored_parked
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .flat_map(|(file_id, parked)| parked.keys().map(move |ordinal| (*file_id, *ordinal)));
        uu.chain(unanchored)
            .map(|(file_id, segment_number)| SegmentId {
                file_id,
                segment_number,
            })
            .collect()
    }

    // Whether anything of the job is still out: an article a lane holds, a
    // result a lane released and the pipeline has yet to take, a decode
    // queued or running, what a restored recovery object reads again of
    // its volumes, without which its root would say less than it did, or an
    // object out for its work, without which no checkpoint can be taken.
    fn job_has_results_out(&self, job_id: JobId) -> bool {
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        if self.job_has_queued_decode(job_id) {
            return true;
        }
        let counted = |counts: &HashMap<JobId, usize>| counts.get(&job_id).is_some_and(|n| *n > 0);
        self.graph_jobs
            .get(&job_id)
            .is_some_and(|graph| graph.rewalks_owed() || graph.working())
            || counted(&self.articles_in_flight_by_job)
            || counted(&self.pending_released_download_results_by_job)
            || self
                .active_decodes_by_job
                .get(&job_id)
                .is_some_and(|n| *n > 0)
    }

    // Stop the job's graph runtime. One with nothing out on a thread goes at
    // once; one with a run or a repair out is kept, apart from the graphs
    // that serve jobs, until the last of its mail is heard. A cancelled job
    // gives up its files: what its volumes' writers still hold of it is not
    // run. A failed job does not, by whichever path it is stopped: it keeps
    // what it delivered, so every file operation queued for it runs, a
    // file's last bytes, a move to its name, a removal of what no longer
    // belongs and the release of its handles, before its working directory
    // is released.
    pub fn retire_graph_job(&mut self, job_id: JobId) {
        if let Some(graph) = self.stop_graph_job(job_id) {
            // Gone with what its runs reserved: the memory goes to whoever
            // waits for it.
            drop(graph);
            self.wake_memory_waiters();
        }
    }

    // As `retire_graph_job`, but a stopped graph that holds nothing is
    // handed back rather than dropped. Whether a graph holds anything is
    // known only here, as it stops: a volume's thread may return the job's
    // last operation at any moment before.
    pub(in crate::pipeline) fn stop_graph_job(
        &mut self,
        job_id: JobId,
    ) -> Option<crate::pipeline::graph::job::GraphJob> {
        let keep_files = self.pipeline_failure_stages.contains_key(&job_id);
        self.forget_graph_repair(job_id);
        self.disk_waiting.remove(&job_id);
        self.disk_idle.remove(&job_id);
        let mut graph = self.graph_jobs.remove(&job_id)?;
        // A job that stops fetches no more: what its start promised and did
        // not land goes back before the graph may be kept.
        graph.release_posted();
        let pending = graph.pending().clone();
        if !pending.control().cancelled()
            && pending
                .await_commit(
                    pending.stamp(),
                    job_id.0,
                    &self.graph_runs.written(),
                    &self.db,
                )
                .is_none()
        {
            graph.owe_mail(pending.stamp());
        }
        graph.request_stop();
        if pending.control().cancelled() && !keep_files {
            graph.abandon_disk();
        }
        // A job kept only for its last file operation hears when it returned.
        if let Some(disk) = graph.disk().filter(|disk| !disk.quiet()) {
            disk.barrier().then_tell(job_id, &self.graph_runs.written());
        }
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        before_kept::run();
        if graph.holds_retirement() {
            self.retiring_graph_jobs.insert(job_id, graph);
            None
        } else {
            Some(graph)
        }
    }
}

// What a test runs as a graph stops, just before whether it holds anything
// is looked at, on the thread that stops it: once, then it is gone.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub mod before_kept {
    use std::cell::RefCell;

    thread_local! {
        static RUN: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    }

    pub fn set(run: impl FnOnce() + 'static) {
        RUN.with(|slot| *slot.borrow_mut() = Some(Box::new(run)));
    }

    pub(super) fn run() {
        let run = RUN.with(|slot| slot.borrow_mut().take());
        if let Some(run) = run {
            run();
        }
    }
}

// One job's spec read back from its stored NZB, every article list in it,
// streamed from the stored bytes, with its password candidates and outline.
pub(crate) fn read_stored_lists(
    db: &crate::Database,
    job_id: JobId,
) -> Result<(PathBuf, crate::ingest::outline::StoredRead), String> {
    let (nzb_path, nzb_zstd) = db
        .load_active_job_persisted_nzb(job_id)
        .map_err(|error| format!("stored NZB could not be loaded: {error}"))?
        .ok_or_else(|| "the job has no stored row".to_string())?;
    let nzb_zstd = nzb_zstd.ok_or_else(|| "the job has no stored NZB".to_string())?;
    let read = crate::ingest::outline::read_stored_lists(&nzb_zstd, &nzb_path)
        .map_err(|error| format!("stored NZB could not be parsed: {error}"))?;
    Ok((nzb_path, read))
}

// One job's spec read back from its stored NZB, every article list in it.
pub(crate) fn read_stored_spec(
    db: &crate::Database,
    job_id: JobId,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> Result<JobSpec, String> {
    let (nzb_path, mut read) = read_stored_lists(db, job_id)?;
    Ok(read.spec(&nzb_path, category, metadata))
}

// As `read_stored_spec`, with the password candidates the job carries taken
// from the same parse. The candidates are None when the row's password
// sources do not load, which leaves the harvest to read the row itself.
pub(crate) fn read_stored_spec_and_passwords(
    db: &crate::Database,
    job_id: JobId,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> Result<(JobSpec, Option<Vec<crate::jobs::ArchivePasswordCandidate>>), String> {
    let Ok(sources) = db.load_active_job_password_sources(job_id) else {
        return read_stored_spec(db, job_id, category, metadata).map(|spec| (spec, None));
    };
    let ((nzb_path, nzb_zstd), supplied) =
        sources.ok_or_else(|| "the job has no stored row".to_string())?;
    let nzb_zstd = nzb_zstd.ok_or_else(|| "the job has no stored NZB".to_string())?;
    let mut read = crate::ingest::outline::read_stored_lists(&nzb_zstd, &nzb_path)
        .map_err(|error| format!("stored NZB could not be parsed: {error}"))?;
    drop(nzb_zstd);
    let passwords = crate::pipeline::job_carried_password_candidates(
        &supplied,
        std::mem::take(&mut read.passwords),
    );
    let spec = read.spec(&nzb_path, category, metadata);
    Ok((spec, Some(passwords)))
}

// Read one job's article lists back from its stored NZB, check they are the
// ones the job was admitted with, and rebuild its graph from its checkpoint
// when it has one. Lists read ahead are used when they still match.
#[allow(clippy::too_many_arguments)]
fn read_job_articles(
    db: &crate::Database,
    job_id: JobId,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    expected: &FileSummaries,
    ahead: Option<(JobSpec, crate::pipeline::next_up::HeldLists)>,
    memory: Arc<ProcessMemoryBudget>,
    seat: RestoreSeat,
) -> Result<ReadArticles, String> {
    let matches = |spec: &JobSpec| {
        spec.files.len() == expected.0.len()
            && spec
                .files
                .iter()
                .zip(&expected.0)
                .all(|(file, expected)| FileSummary::of(file).same_list(expected))
    };
    let ahead = ahead.map(|(mut spec, held)| {
        for (file, list) in spec.files.iter_mut().zip(held.unpack()) {
            file.segments = list;
        }
        spec
    });
    // Lists read ahead that no longer match are read again.
    let spec = match ahead.filter(|spec| matches(spec)) {
        Some(spec) => spec,
        None => read_stored_spec(db, job_id, category, metadata)?,
    };
    if !matches(&spec) {
        return Err("stored NZB no longer matches the job's files".to_string());
    }
    let repeated = RepeatedArticles::new(&spec, seat.working_dir(), memory)?;
    let restored = match seat.restore(job_id, root_files(&spec)) {
        Ok(restored) => Some(restored),
        Err(GraphRestoreError::NoCheckpoint(_)) => None,
        Err(error) => return Err(error.to_string()),
    };
    let lists = spec.files.into_iter().map(|file| file.segments).collect();
    Ok(ReadArticles {
        lists,
        repeated,
        restored,
        staging: None,
        volumes: Vec::new(),
        made_staging: None,
    })
}

// Seams that hold a move of a working directory, and a restart's clearing of
// the folders left with the job's marker, at chosen points, so a test sets
// the order of each step against a cancel and a restart. Armed for one
// intermediate root at a time; unarmed, each is a no-op.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub(crate) mod move_seams {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Condvar, Mutex};

    #[derive(Default)]
    pub(crate) struct Gate {
        open: Mutex<bool>,
        opened: Condvar,
        notify: tokio::sync::Notify,
    }

    impl Gate {
        #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
        pub(crate) fn open(&self) {
            *self.open.lock().unwrap() = true;
            self.opened.notify_all();
            self.notify.notify_waiters();
        }

        fn is_open(&self) -> bool {
            *self.open.lock().unwrap()
        }

        async fn pass(&self) {
            loop {
                let notified = self.notify.notified();
                if self.is_open() {
                    return;
                }
                notified.await;
            }
        }

        fn pass_blocking(&self) {
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.opened.wait(open).unwrap();
            }
        }
    }

    #[derive(Default)]
    pub(crate) struct Seams {
        // Before the move's row write.
        pub(crate) row: Gate,
        pub(crate) claimed: AtomicBool,
        // Before the move gives up a directory the row did not take.
        pub(crate) abandon: Gate,
        pub(crate) abandoned: AtomicBool,
        // After a restart has listed the folders it clears: held there until
        // this gate opens. None holds nothing.
        pub(crate) listing: Option<Gate>,
        pub(crate) listed: AtomicBool,
        // A wait for a pending cleanup has found one.
        pub(crate) cleanup_waited: AtomicBool,
        // Before the removal of a staging directory made for a job that
        // left: held there until this gate opens. None holds nothing.
        pub(crate) staging_removal: Option<Gate>,
        pub(crate) staging_removed: AtomicBool,
    }

    static ARMED: Mutex<Option<(PathBuf, Arc<Seams>)>> = Mutex::new(None);

    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn arm(root: &Path, seams: Seams) -> Arc<Seams> {
        let seams = Arc::new(seams);
        *ARMED.lock().unwrap() = Some((root.to_path_buf(), Arc::clone(&seams)));
        seams
    }

    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn disarm() {
        *ARMED.lock().unwrap() = None;
    }

    fn armed(root: Option<&Path>) -> Option<Arc<Seams>> {
        ARMED
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(armed, _)| root.is_none_or(|root| root == armed))
            .map(|(_, seams)| Arc::clone(seams))
    }

    pub(super) async fn before_row_write(root: &Path) {
        if let Some(seams) = armed(Some(root)) {
            seams.claimed.store(true, Ordering::Release);
            seams.row.pass().await;
        }
    }

    pub(super) async fn before_abandon(root: &Path) {
        if let Some(seams) = armed(Some(root)) {
            seams.abandon.pass().await;
        }
    }

    pub(super) fn after_abandon(root: &Path) {
        if let Some(seams) = armed(Some(root)) {
            seams.abandoned.store(true, Ordering::Release);
        }
    }

    pub(crate) fn after_listing(root: &Path) {
        if let Some(seams) = armed(Some(root)) {
            seams.listed.store(true, Ordering::Release);
            if let Some(gate) = &seams.listing {
                gate.pass_blocking();
            }
        }
    }

    // A staging directory lies at `<root>/<staging>/<id>`: armed by its root.
    fn staging_root(staging: &Path) -> Option<&Path> {
        staging.parent().and_then(Path::parent)
    }

    pub(super) fn before_staging_removal(staging: &Path) {
        if let Some(seams) = staging_root(staging).and_then(|root| armed(Some(root)))
            && let Some(gate) = &seams.staging_removal
        {
            gate.pass_blocking();
        }
    }

    pub(super) fn after_staging_removal(staging: &Path) {
        if let Some(seams) = staging_root(staging).and_then(|root| armed(Some(root))) {
            seams.staging_removed.store(true, Ordering::Release);
        }
    }

    pub(crate) fn cleanup_waited() {
        if let Some(seams) = armed(None) {
            seams.cleanup_waited.store(true, Ordering::Release);
        }
    }
}
