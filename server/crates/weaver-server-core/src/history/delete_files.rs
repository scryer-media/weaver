// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::JobId;
use crate::history::output_files::OutputFileListing;
use crate::torrents::persistence::HistoryFile;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;
use tracing::{debug, info, warn};

// One job's working directories a history delete will remove, and the ones
// it will leave on disk because their ownership marker no longer matches.
#[derive(Debug, Default)]
struct JobCleanupDirs {
    owned: BTreeSet<PathBuf>,
    left_in_place: Vec<PathBuf>,
}

async fn retry_script_stop_wait<F, W>(job_id: JobId, mut wait: W) -> Result<(), crate::StateError>
where
    F: std::future::Future<Output = Result<(), crate::StateError>>,
    W: FnMut() -> F,
{
    for attempt in 0..7 {
        match wait().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                if attempt == 6 {
                    return Err(crate::StateError::Conflict(format!(
                        "could not confirm scripts stopped for job {} after seven attempts: {error}",
                        job_id.0
                    )));
                }
                warn!(job_id = job_id.0, %error, "could not confirm scripts stopped; retrying history cleanup barrier");
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
            }
        }
    }
    unreachable!("the last attempt returns its error")
}

impl JobCleanupDirs {
    async fn admit(
        &mut self,
        intermediate_dir: &std::path::Path,
        job_id: JobId,
        path: PathBuf,
    ) -> Result<(), crate::SchedulerError> {
        let judged =
            crate::jobs::working_dir::prepare_history_working_dir(intermediate_dir, &path, job_id)
                .await?;
        match judged {
            crate::jobs::working_dir::HistoryWorkingDir::Owned => {
                self.owned.insert(path);
            }
            crate::jobs::working_dir::HistoryWorkingDir::LeftInPlace => {
                if !self.left_in_place.contains(&path) {
                    self.left_in_place.push(path);
                }
            }
        }
        Ok(())
    }

    // Which of a job's working directories a history cleanup may remove: the
    // one its record names when that is a direct child of the intermediate
    // root and not a delivery folder, and every one still carrying its
    // marker. An unmarked recorded working directory is marked the job's, as
    // a delete always took it. A directory that refuses is returned with the
    // reason, and then nothing of the job may go. A recorded folder whose
    // parent is one of `destinations` is where the job delivered, and is
    // never taken for a working directory: it goes, if at all, as an output
    // folder, on its own marker.
    async fn plan(
        intermediate_dir: &std::path::Path,
        job_id: JobId,
        recorded: Option<&std::path::Path>,
        marked: Vec<PathBuf>,
        destinations: &[PathBuf],
    ) -> Result<Self, (PathBuf, crate::SchedulerError)> {
        let mut dirs = Self::default();
        let candidates = recorded
            .and_then(|dir| working_dir_on_record(intermediate_dir, dir, destinations))
            .into_iter()
            .chain(marked);
        for path in candidates {
            if let Err(error) = dirs.admit(intermediate_dir, job_id, path.clone()).await {
                return Err((path, error));
            }
        }
        Ok(dirs)
    }

    // Removes every owned directory. Returns how many it removed, and every
    // directory left on disk: the ones already set aside plus any whose
    // marker stopped matching between the two looks.
    async fn remove(
        self,
        intermediate_dir: &std::path::Path,
        job_id: JobId,
    ) -> Result<RemovedWorkingDirs, crate::SchedulerError> {
        let mut left_in_place = self.left_in_place;
        let mut removed = 0;
        for dir in self.owned {
            let root = intermediate_dir.to_path_buf();
            let target = dir.clone();
            let removal = tokio::task::spawn_blocking(move || {
                crate::jobs::working_dir::remove_job_working_dir(&root, &target, job_id)
            })
            .await
            .map_err(|error| crate::SchedulerError::Io(std::io::Error::other(error)))?;
            match removal {
                Ok(crate::jobs::working_dir::HistoryWorkingDir::Owned) => {
                    removed += 1;
                    info!(dir = %dir.display(), "removed historical intermediate directory");
                }
                Ok(crate::jobs::working_dir::HistoryWorkingDir::LeftInPlace) => {
                    left_in_place.push(dir);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(crate::SchedulerError::Io(std::io::Error::new(
                        error.kind(),
                        format!(
                            "failed to remove historical intermediate directory '{}': {error}",
                            dir.display()
                        ),
                    )));
                }
            }
        }
        Ok(RemovedWorkingDirs {
            removed,
            left_in_place,
        })
    }
}

struct RemovedWorkingDirs {
    removed: usize,
    left_in_place: Vec<PathBuf>,
}

// What a history cleanup found at a job's recorded output directory before
// removing anything.
#[derive(Debug)]
enum OutputDirProof {
    // Nothing to remove: no directory recorded, or none on disk while the
    // folder that would hold it is there.
    Absent,
    // Neither it nor the folder that would hold it is there, as when a mount
    // is offline: whether its files are gone cannot be told.
    Unreachable,
    // On disk without a marker naming this job, or a legacy marker another
    // job's record shares; left as it is.
    NotManaged,
    // On disk with a marker proving it this job's.
    Owned,
    Failed(std::io::Error),
}

// A directory a history delete may remove: a direct child of the
// intermediate root, never the root itself or anything deeper.
fn cleanupable_history_output_dir(
    intermediate_dir: &std::path::Path,
    output_dir: &std::path::Path,
) -> Option<PathBuf> {
    output_dir
        .strip_prefix(intermediate_dir)
        .ok()
        .filter(|suffix| {
            let mut components = suffix.components();
            matches!(components.next(), Some(std::path::Component::Normal(_)))
                && components.next().is_none()
        })
        .map(|_| output_dir.to_path_buf())
}

// The recorded folder as the job's working directory: a direct child of the
// intermediate root that is neither a destination root nor a delivery
// folder, one whose parent is a destination root (as when the complete
// folder is the intermediate one).
pub(crate) fn working_dir_on_record(
    intermediate_dir: &std::path::Path,
    recorded: &std::path::Path,
    destinations: &[PathBuf],
) -> Option<PathBuf> {
    cleanupable_history_output_dir(intermediate_dir, recorded).filter(|dir| {
        !destinations.iter().any(|root| root == dir) && !is_delivery_dir(dir, destinations)
    })
}

// Whether `dir` sits directly in one of `destinations`, where jobs deliver.
pub(crate) fn is_delivery_dir(dir: &std::path::Path, destinations: &[PathBuf]) -> bool {
    dir.parent()
        .is_some_and(|parent| destinations.iter().any(|root| root == parent))
}

// What a removed output folder held: its first `keep` files in listing
// order and its totals, links neither listed nor followed. A folder that
// cannot be read gives no record; the removal goes ahead as it would.
async fn record_output_files(dir: &std::path::Path, keep: usize) -> Option<OutputFileListing> {
    let path = dir.to_path_buf();
    match tokio::task::spawn_blocking(move || {
        crate::history::output_files::list_output_files(&path, keep)
    })
    .await
    {
        Ok(Ok(listing)) => Some(listing),
        Ok(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
        Ok(Err(error)) => {
            warn!(dir = %dir.display(), %error, "could not record the files of an output directory before removing it");
            None
        }
        Err(error) => {
            warn!(dir = %dir.display(), %error, "the output directory record task failed");
            None
        }
    }
}

// Whether `dir` is there and proven `job_id`'s: a marker naming the job, or
// a legacy marker when `legacy_ok` says no other job records the folder.
async fn prove_output_dir(dir: &std::path::Path, job_id: JobId, legacy_ok: bool) -> OutputDirProof {
    let path = dir.to_path_buf();
    let proof = tokio::task::spawn_blocking(move || match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent_there = path
                .parent()
                .is_some_and(|parent| std::fs::metadata(parent).is_ok_and(|meta| meta.is_dir()));
            if parent_there {
                OutputDirProof::Absent
            } else {
                OutputDirProof::Unreachable
            }
        }
        Err(error) => OutputDirProof::Failed(error),
        Ok(_) => {
            if crate::jobs::working_dir::output_dir_owner(&path)
                .is_some_and(|owner| owner.admits(job_id, legacy_ok))
            {
                OutputDirProof::Owned
            } else {
                OutputDirProof::NotManaged
            }
        }
    })
    .await
    .unwrap_or_else(|error| OutputDirProof::Failed(std::io::Error::other(error)));
    match &proof {
        OutputDirProof::NotManaged => warn!(
            dir = %dir.display(),
            job_id = job_id.0,
            "refusing recursive cleanup of an output directory without a Weaver ownership marker of the job"
        ),
        OutputDirProof::Unreachable => warn!(
            dir = %dir.display(),
            "neither the output directory nor the folder holding it is reachable"
        ),
        OutputDirProof::Failed(error) => warn!(
            dir = %dir.display(),
            error = %error,
            "could not inspect complete output directory before cleanup"
        ),
        OutputDirProof::Absent | OutputDirProof::Owned => {}
    }
    proof
}

// What removing a proven output directory came to.
#[derive(Debug)]
enum OutputDirRemoval {
    Removed,
    // Gone by the time the removal ran.
    Absent,
    // Its marker stopped proving it the job's before the removal ran.
    NotManaged,
}

// Removes an output directory `prove_output_dir` proved the job's, proving
// it again on the blocking thread first, the marker last.
async fn remove_proven_output_dir(
    dir: &std::path::Path,
    job_id: JobId,
    legacy_ok: bool,
) -> std::io::Result<OutputDirRemoval> {
    let path = dir.to_path_buf();
    let removal = tokio::task::spawn_blocking(move || {
        match std::fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(OutputDirRemoval::Absent);
            }
            Err(error) => return Err(error),
            Ok(_) => {}
        }
        if !crate::jobs::working_dir::output_dir_owner(&path)
            .is_some_and(|owner| owner.admits(job_id, legacy_ok))
        {
            return Ok(OutputDirRemoval::NotManaged);
        }
        crate::jobs::working_dir::remove_owned_output_dir(&path).map(|()| OutputDirRemoval::Removed)
    })
    .await
    .map_err(std::io::Error::other)?;
    match &removal {
        Ok(OutputDirRemoval::Removed) => {
            debug!(dir = %dir.display(), "removed complete output directory");
        }
        Ok(OutputDirRemoval::NotManaged) => warn!(
            dir = %dir.display(),
            "an output directory stopped carrying the job's ownership marker; cleanup leaves it"
        ),
        Ok(OutputDirRemoval::Absent) => {}
        Err(error) => warn!(
            dir = %dir.display(),
            error = %error,
            "failed to remove complete output directory"
        ),
    }
    removal
}

// Proves and removes a job's recorded output directory for a delete with
// files. What came of it is logged; a delete does not report it.
async fn delete_owned_output_dir(dir: &std::path::Path, job_id: JobId, legacy_ok: bool) {
    if matches!(
        prove_output_dir(dir, job_id, legacy_ok).await,
        OutputDirProof::Owned
    ) {
        let _ = remove_proven_output_dir(dir, job_id, legacy_ok).await;
    }
}

// Which of the recorded output folders more than one job records: a folder
// whose marker predates job-named markers is removed only when no other
// job records it. One read for the whole request.
// Which jobs of a batch wait at the script-stop barrier: those with a
// script queued, running or waiting to be admitted, read for the whole
// batch at once. A script a job gains after that read, such as one a mark
// queues while earlier jobs are removed, is seen before that job's turn:
// the jobs still to go are read again, in one statement, whenever any job
// gained a script since the last read.
struct LiveScripts {
    starts: u64,
    live: std::collections::HashSet<u64>,
}

impl LiveScripts {
    async fn read(db: &crate::Database, job_ids: Vec<u64>) -> Self {
        let starts = db.script_starts();
        let live = jobs_with_live_scripts(db, job_ids).await;
        Self { starts, live }
    }

    // Whether `rest[0]` waits at its barrier; `rest` is that job and the
    // ones after it.
    async fn waits(&mut self, db: &crate::Database, rest: &[u64]) -> bool {
        if db.script_starts() != self.starts {
            *self = Self::read(db, rest.to_vec()).await;
        }
        rest.first()
            .is_some_and(|job_id| self.live.contains(job_id))
    }
}

// The jobs among `job_ids` with a script queued, running or waiting to be
// admitted, in one read per bind-safe chunk. A read that fails counts every
// job live, so each keeps its own barrier.
async fn jobs_with_live_scripts(
    db: &crate::Database,
    job_ids: Vec<u64>,
) -> std::collections::HashSet<u64> {
    if job_ids.is_empty() {
        return std::collections::HashSet::new();
    }
    let read_db = db.clone();
    let ids = job_ids.clone();
    match tokio::task::spawn_blocking(move || read_db.jobs_with_live_script_events(&ids)).await {
        Ok(Ok(live)) => live,
        Ok(Err(error)) => {
            warn!(%error, "could not list jobs with live scripts; each job waits at its own script-stop barrier");
            job_ids.into_iter().collect()
        }
        Err(error) => {
            warn!(%error, "the live-script listing task failed; each job waits at its own script-stop barrier");
            job_ids.into_iter().collect()
        }
    }
}

async fn shared_output_dirs(
    db: &crate::Database,
    dirs: Vec<String>,
) -> Result<BTreeSet<String>, crate::SchedulerError> {
    if dirs.is_empty() {
        return Ok(BTreeSet::new());
    }
    let lookup_db = db.clone();
    tokio::task::spawn_blocking(move || lookup_db.shared_output_dirs(&dirs))
        .await
        .map_err(|error| {
            crate::SchedulerError::Internal(format!(
                "failed to join output folder lookup task: {error}"
            ))
        })?
        .map_err(crate::SchedulerError::State)
}

fn legacy_ok(shared: &BTreeSet<String>, dir: &std::path::Path) -> bool {
    !shared.contains::<str>(&dir.to_string_lossy())
}

// The job's staging directories on disk now, under each of `roots`.
async fn present_staging_dirs(staging_roots: &[PathBuf], job_id: JobId) -> Vec<PathBuf> {
    let mut staged = Vec::new();
    for root in staging_roots {
        let staging = crate::pipeline::queue::staging_dir_for(root, job_id);
        if tokio::fs::symlink_metadata(&staging).await.is_ok() {
            staged.push(staging);
        }
    }
    staged
}

// The marked working directories of `job_ids` under the intermediate root,
// and which of them are live in a class. A job a class still holds is live:
// its folders are the class's, never a history row's to take, not those it
// is marked in nor the one its row records. Which are live is read from the
// database, one batched lookup per class that keeps live jobs, before the
// scan: whether or not the class is running.
async fn scan_marked_dirs(
    db: &crate::Database,
    intermediate_dir: &std::path::Path,
    job_ids: &BTreeSet<JobId>,
) -> Result<(BTreeMap<JobId, Vec<PathBuf>>, BTreeSet<JobId>), crate::SchedulerError> {
    let root = intermediate_dir.to_path_buf();
    let mut owners = job_ids.clone();
    let db = db.clone();
    match tokio::task::spawn_blocking(move || {
        let ids = owners.iter().copied().collect::<Vec<_>>();
        let live = crate::jobs::Protocol::live_among(&db, &ids)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        owners.retain(|job_id| !live.contains(job_id));
        crate::jobs::working_dir::job_working_dirs(&root, &owners).map(|marked| (marked, live))
    })
    .await
    {
        Ok(Ok(marked)) => Ok(marked),
        Ok(Err(error)) => Err(crate::SchedulerError::Io(error)),
        Err(error) => Err(crate::SchedulerError::Internal(format!(
            "failed to join working directory lookup task: {error}"
        ))),
    }
}

// The files the history rows of `job_ids` list, the jobs of torrents that
// are no longer live: a torrent's job lists the files it wrote with its row,
// as an NZB job marks its folders. Batched lookups, served by their keys; a
// torrent's job that lists none has an empty list, and an NZB job none.
async fn listed_files(
    db: &crate::Database,
    job_ids: Vec<JobId>,
) -> Result<BTreeMap<JobId, Vec<HistoryFile>>, crate::SchedulerError> {
    if job_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let db = db.clone();
    tokio::task::spawn_blocking(move || db.torrent_history_lists(&job_ids))
        .await
        .map_err(|error| {
            crate::SchedulerError::Internal(format!(
                "failed to join history files lookup task: {error}"
            ))
        })?
        .map_err(crate::SchedulerError::State)
}

// What the live torrents hold under the folders of the listed jobs of one
// call, read once for all of them (`LiveHeld::under`). A read that fails leaves
// every listed file of the call where it is.
async fn live_held(
    db: &crate::Database,
    listed: &BTreeMap<JobId, (PathBuf, Vec<HistoryFile>)>,
) -> Result<std::sync::Arc<crate::torrents::service::LiveHeld>, String> {
    if listed.is_empty() {
        return Ok(Default::default());
    }
    let dirs = listed
        .values()
        .map(|(dir, _)| dir.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let db = db.clone();
    match tokio::task::spawn_blocking(move || crate::torrents::service::LiveHeld::under(&db, &dirs))
        .await
    {
        Ok(Ok(held)) => Ok(std::sync::Arc::new(held)),
        Ok(Err(error)) => Err(error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

// Removes the files a torrent's job listed under `dir`, the folder its row
// records, while its marker names the job, leaving what a live torrent
// holds (`remove_history_files`).
async fn remove_listed_files(
    job_id: JobId,
    intermediate_dir: &std::path::Path,
    dir: PathBuf,
    files: Vec<HistoryFile>,
    held: std::sync::Arc<crate::torrents::service::LiveHeld>,
) -> std::io::Result<crate::torrents::service::ListedRemoval> {
    let intermediate = intermediate_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        crate::torrents::service::remove_history_files(job_id, &intermediate, &dir, &files, &held)
    })
    .await
    .map_err(std::io::Error::other)
}

// History deletion owns persistence and files independently of the pipeline.
// In order: one read of the
// batch's history rows, the marker judgement of every directory, one delete
// transaction for every row, then per job the script-stopped barrier and only
// after it the job's directories. Returns the ids whose rows the transaction
// removed, and the batch outcome.
pub(crate) async fn delete_history_rows(
    db: &crate::Database,
    intermediate_dir: &std::path::Path,
    staging_roots: &[PathBuf],
    job_ids: BTreeSet<JobId>,
    delete_files: bool,
) -> (
    BTreeSet<JobId>,
    Result<crate::HistoryDeleteBatchOutcome, crate::SchedulerError>,
) {
    db.cancel_event_scripts_of(&job_ids.iter().map(|id| id.0).collect());
    let mut output_dirs = BTreeMap::new();
    let mut refused = BTreeMap::new();
    let ids = job_ids.iter().copied().collect::<Vec<_>>();
    let lookup_db = db.clone();
    let rows = match tokio::task::spawn_blocking(move || {
        let _probe =
            crate::runtime::perf_probe::scope("db.list_job_history_output_dirs.history_delete");
        lookup_db.list_job_history_output_dirs(&ids)
    })
    .await
    {
        Ok(Ok(rows)) => rows,
        Ok(Err(error)) => return (BTreeSet::new(), Err(crate::SchedulerError::State(error))),
        Err(error) => {
            return (
                BTreeSet::new(),
                Err(crate::SchedulerError::Internal(format!(
                    "failed to join history lookup task: {error}"
                ))),
            );
        }
    };

    // A job's working directory is the one its record names, or one that
    // still carries its marker: a delivered job's record names where it
    // delivered, and its working directory stays only for what it could not
    // remove.
    let (mut marked, live) = match scan_marked_dirs(db, intermediate_dir, &job_ids).await {
        Ok(scanned) => scanned,
        Err(error) => return (BTreeSet::new(), Err(error)),
    };
    // Read before the rows go with them.
    let mut listed = if delete_files {
        let ids = job_ids
            .iter()
            .filter(|id| !live.contains(id))
            .copied()
            .collect();
        match listed_files(db, ids).await {
            Ok(listed) => listed,
            Err(error) => return (BTreeSet::new(), Err(error)),
        }
    } else {
        BTreeMap::new()
    };
    let mut listed_dirs = BTreeMap::new();

    let mut cleanup = BTreeMap::new();
    let mut delete_ids = Vec::with_capacity(job_ids.len());
    for job_id in job_ids {
        let recorded = rows
            .get(&job_id)
            .cloned()
            .flatten()
            .filter(|_| !live.contains(&job_id))
            .map(PathBuf::from);
        // A job that lists its files is a torrent's: its folder is where its
        // files are, theirs, and what goes of it is what it lists.
        let (recorded, files) = take_listed(recorded, listed.remove(&job_id));
        let planned = JobCleanupDirs::plan(
            intermediate_dir,
            job_id,
            recorded.as_deref(),
            marked.remove(&job_id).unwrap_or_default(),
            staging_roots,
        )
        .await;
        let dirs = match planned {
            Ok(dirs) => dirs,
            Err((_, error)) => {
                refused.insert(job_id, error);
                output_dirs.remove(&job_id);
                continue;
            }
        };
        if delete_files {
            if let Some(dir) = recorded {
                output_dirs.entry(job_id).or_insert(dir);
            }
        } else {
            output_dirs.remove(&job_id);
        }
        if let Some(files) = files {
            listed_dirs.insert(job_id, files);
        }
        cleanup.insert(job_id, dirs);
        delete_ids.push(job_id);
    }

    // Read while every row is still there: a legacy marker is proof only
    // when no other job records the same folder.
    let shared = match shared_output_dirs(
        db,
        output_dirs
            .values()
            .map(|dir| dir.to_string_lossy().into_owned())
            .collect(),
    )
    .await
    {
        Ok(shared) => shared,
        Err(error) => return (BTreeSet::new(), Err(error)),
    };

    let delete_db = db.clone();
    let delete_set = delete_ids.clone();
    match tokio::task::spawn_blocking(move || {
        delete_db.delete_job_histories_and_forget_duplicate_identities(&delete_set)
    })
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(crate::StateError::Conflict(message))) => {
            return (
                BTreeSet::new(),
                Err(crate::SchedulerError::Conflict(message)),
            );
        }
        Ok(Err(error)) => {
            return (
                BTreeSet::new(),
                Err(crate::SchedulerError::Internal(format!(
                    "failed to delete history bundles: {error}"
                ))),
            );
        }
        Err(error) => {
            return (
                BTreeSet::new(),
                Err(crate::SchedulerError::Internal(format!(
                    "failed to join history delete task: {error}"
                ))),
            );
        }
    }

    // Every accepted id's record is gone now, including an id that had no
    // row. No pipeline state participates in a history deletion.
    let removed = delete_ids.iter().copied().collect::<BTreeSet<_>>();
    // Every job's scripts stop first; what live torrents hold is read once
    // after, so no wait leaves that read stale before the files go.
    let mut deleted = BTreeMap::new();
    let mut stopped = Vec::with_capacity(cleanup.len());
    let order: Vec<u64> = cleanup.keys().map(|id: &JobId| id.0).collect();
    let mut scripted = LiveScripts::read(db, order.clone()).await;
    for (turn, (job_id, dirs)) in cleanup.into_iter().enumerate() {
        if scripted.waits(db, &order[turn..]).await
            && let Err(error) = retry_script_stop_wait(job_id, || {
                crate::post_processing::events::wait_for_job_events_stopped(db, job_id.0)
            })
            .await
        {
            deleted.insert(job_id, Err(crate::SchedulerError::State(error)));
            continue;
        }
        stopped.push((job_id, dirs));
    }
    let held = live_held(db, &listed_dirs).await;
    if let Err(error) = &held {
        warn!(%error, "could not read what live torrents hold; deleted torrents' files stay");
    }
    // A script a job gained since its barrier above stops before the job's
    // files go. Nothing is read again unless a script has started since.
    let removal_order: Vec<u64> = stopped.iter().map(|(id, _)| id.0).collect();
    for (turn, (job_id, dirs)) in stopped.into_iter().enumerate() {
        if scripted.waits(db, &removal_order[turn..]).await
            && let Err(error) = retry_script_stop_wait(job_id, || {
                crate::post_processing::events::wait_for_job_events_stopped(db, job_id.0)
            })
            .await
        {
            deleted.insert(job_id, Err(crate::SchedulerError::State(error)));
            continue;
        }
        match dirs.remove(intermediate_dir, job_id).await {
            Ok(RemovedWorkingDirs { left_in_place, .. }) => {
                // A delete reports only what it left of the working
                // directories; the output directory's fate is logged.
                if let Some(dir) = output_dirs.get(&job_id) {
                    delete_owned_output_dir(dir, job_id, legacy_ok(&shared, dir)).await;
                }
                if let Some((dir, files)) = listed_dirs.remove(&job_id)
                    && let Ok(held) = &held
                {
                    match remove_listed_files(job_id, intermediate_dir, dir, files, held.clone())
                        .await
                    {
                        Ok(listed) if !listed.not_managed.is_empty() => info!(
                            job_id = job_id.0,
                            left = ?listed.not_managed,
                            "a deleted torrent's files stay where nothing proves Weaver may remove them"
                        ),
                        Ok(_) => {}
                        Err(error) => {
                            warn!(job_id = job_id.0, %error, "could not remove a torrent's listed files");
                        }
                    }
                }
                // What the staging directory still holds is the rest of the
                // delivery the working directory held, and goes the way it
                // went, with or without the files.
                let staged =
                    crate::pipeline::queue::remove_job_staging(staging_roots, job_id).await;
                deleted.insert(
                    job_id,
                    staged.map(|()| crate::HistoryDeleteOutcome { left_in_place }),
                );
            }
            Err(error) => {
                deleted.insert(job_id, Err(error));
            }
        }
    }
    (
        removed,
        Ok(crate::HistoryDeleteBatchOutcome { deleted, refused }),
    )
}

// Housekeeping for a failed job whose scripts have stopped: the working
// directories that carry its marker, `marked`, as a scan of the intermediate
// root found them, and its staging directory go, and its record stays. The
// folder its record names goes only as one of those: unmarked, another
// job's, or a delivery folder, it is not housekeeping's to remove and stays,
// said once. Returns whether housekeeping has done all it ever will for the
// job; false when its recorded folder carries its marker and is still there,
// which the next sweep's scan finds, and false when a category folder could
// not be searched for its staging, which the next sweep tries again.
pub(crate) async fn sweep_failed_job_files(
    intermediate_dir: &std::path::Path,
    staging_roots: &[PathBuf],
    job_id: JobId,
    recorded: Option<PathBuf>,
    marked: Vec<PathBuf>,
) -> Result<bool, crate::SchedulerError> {
    let dirs = JobCleanupDirs {
        owned: marked.into_iter().collect(),
        left_in_place: Vec::new(),
    };
    let left_in_place = dirs.remove(intermediate_dir, job_id).await?.left_in_place;
    for dir in &left_in_place {
        warn!(
            job_id = job_id.0,
            dir = %dir.display(),
            "a failed job's working directory stopped matching its marker; housekeeping leaves it"
        );
    }
    let staged = present_staging_dirs(staging_roots, job_id).await;
    let checked = crate::pipeline::queue::remove_job_staging_checked(staging_roots, job_id).await?;
    for staging in staged {
        info!(dir = %staging.display(), "removed a failed job's staging directory");
    }
    let Some(recorded) = recorded else {
        return Ok(checked);
    };
    let (root, destinations) = (intermediate_dir.to_path_buf(), staging_roots.to_vec());
    tokio::task::spawn_blocking(move || {
        match std::fs::symlink_metadata(&recorded) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
            Err(error) => return Err(error),
            Ok(_) => {}
        }
        // Still there. A delivery folder, or one outside the intermediate
        // root, was never a working directory housekeeping takes.
        let Some(dir) = working_dir_on_record(&root, &recorded, &destinations) else {
            return Ok(true);
        };
        match crate::jobs::working_dir::working_dir_owner(&root, &dir)? {
            Some(owner) if owner == job_id => return Ok(false),
            Some(owner) => debug!(
                job_id = job_id.0,
                owner = owner.0,
                dir = %dir.display(),
                "a failed job's recorded folder belongs to another job; housekeeping leaves it"
            ),
            None => warn!(
                job_id = job_id.0,
                dir = %dir.display(),
                "a failed job's recorded folder does not carry its marker; housekeeping leaves it, and removing the entry's files takes it"
            ),
        }
        Ok(true)
    })
    .await
    .map_err(|error| crate::SchedulerError::Io(std::io::Error::other(error)))?
    .map_err(crate::SchedulerError::Io)
    .map(|swept| swept && checked)
}

// Archiving history entries: their files are removed while the entries, and
// their stored NZBs, stay. What goes is what a delete with files takes, proven the same
// way: the job's working directories, marked or the unmarked one its record
// names inside the intermediate root, its staging directories, and its
// recorded output directory when its marker names the job (or is a legacy
// marker no other job's record shares). In order:
//
// 1. one read of the batch's rows, and the marker judgement of every
//    working directory;
// 2. one read of which recorded output folders other jobs share;
// 3. per job, the script-stopped barrier, then the proof of its output
//    directory and the listing of what it holds, before anything goes;
// 4. one transaction keeping the record of every listed folder, unstamped;
// 5. per job, the script-stopped barrier again for a script the job gained
//    since step 3, then the removal;
// 6. one transaction stamping every entry whose files are gone as archived.
//
// Every id gets its own outcome whatever fails: a record that cannot be
// kept leaves its job untouched, and a stamp that cannot be written turns
// the removals it would have stamped into failures a retry settles, as the
// retry finds their folders gone and the record kept in step 4 still there.
pub(crate) async fn archive_history(
    db: &crate::Database,
    intermediate_dir: &std::path::Path,
    staging_roots: &[PathBuf],
    job_ids: BTreeSet<JobId>,
    now: i64,
    keep: usize,
) -> Result<BTreeMap<JobId, crate::ArchiveHistoryResult>, crate::SchedulerError> {
    use crate::{ArchiveHistoryOutcome as Outcome, ArchiveHistoryResult as Archive};

    let ids = job_ids.iter().copied().collect::<Vec<_>>();
    let lookup_db = db.clone();
    let states = tokio::task::spawn_blocking(move || lookup_db.list_job_history_file_states(&ids))
        .await
        .map_err(|error| {
            crate::SchedulerError::Internal(format!("failed to join history lookup task: {error}"))
        })?
        .map_err(crate::SchedulerError::State)?;

    let mut outcomes = BTreeMap::new();
    let mut pending = BTreeMap::new();
    for job_id in job_ids {
        match states.get(&job_id) {
            None => {
                outcomes.insert(job_id, Archive::new(Outcome::NotFound, None, None));
            }
            Some(state) => match state.archived_at {
                Some(archived_at) => {
                    outcomes.insert(
                        job_id,
                        Archive::new(Outcome::AlreadyGone, Some(archived_at), None),
                    );
                }
                None => {
                    pending.insert(job_id, state.output_dir.clone().map(PathBuf::from));
                }
            },
        }
    }
    if pending.is_empty() {
        return Ok(outcomes);
    }

    let pending_ids = pending.keys().copied().collect::<BTreeSet<_>>();
    let (mut marked, live) = scan_marked_dirs(db, intermediate_dir, &pending_ids).await?;
    let ids = pending_ids
        .iter()
        .filter(|id| !live.contains(id))
        .copied()
        .collect();
    let mut listed = listed_files(db, ids).await?;
    let mut listed_dirs = BTreeMap::new();
    let mut planned = Vec::with_capacity(pending.len());
    for (job_id, recorded) in pending {
        // A torrent still live holds its files: its entry is not archived,
        // and keeps no stamp, until the torrent goes.
        if live.contains(&job_id) {
            outcomes.insert(
                job_id,
                Archive::new(
                    Outcome::NotManaged,
                    None,
                    Some("the torrent is still live and its files are its own; remove the torrent to remove them".to_owned()),
                ),
            );
            continue;
        }
        let (recorded, files) = take_listed(recorded, listed.remove(&job_id));
        if let Some(files) = files {
            listed_dirs.insert(job_id, files);
        }
        match JobCleanupDirs::plan(
            intermediate_dir,
            job_id,
            recorded.as_deref(),
            marked.remove(&job_id).unwrap_or_default(),
            staging_roots,
        )
        .await
        {
            Ok(dirs) => planned.push((job_id, recorded, dirs)),
            // A working directory that refuses leaves the whole job as it
            // is, as it refuses a delete.
            Err((path, error)) => {
                outcomes.insert(
                    job_id,
                    Archive::new(
                        Outcome::NotManaged,
                        None,
                        Some(format!(
                            "{} is not managed by Weaver: {error}",
                            path.display()
                        )),
                    ),
                );
            }
        }
    }
    db.cancel_event_scripts_of(&planned.iter().map(|(id, _, _)| id.0).collect());
    let shared = shared_output_dirs(
        db,
        planned
            .iter()
            .filter_map(|(_, recorded, _)| recorded.as_ref())
            .map(|dir| dir.to_string_lossy().into_owned())
            .collect(),
    )
    .await?;

    // Proven and listed before anything of any job goes.
    let mut proven = Vec::with_capacity(planned.len());
    let mut records = Vec::new();
    let order: Vec<u64> = planned.iter().map(|(id, _, _)| id.0).collect();
    let mut scripted = LiveScripts::read(db, order.clone()).await;
    for (turn, (job_id, recorded, dirs)) in planned.into_iter().enumerate() {
        if scripted.waits(db, &order[turn..]).await
            && let Err(error) = retry_script_stop_wait(job_id, || {
                crate::post_processing::events::wait_for_job_events_stopped(db, job_id.0)
            })
            .await
        {
            outcomes.insert(
                job_id,
                Archive::new(Outcome::Failed, None, Some(error.to_string())),
            );
            continue;
        }
        let output = match recorded.as_deref() {
            None => OutputDirProof::Absent,
            // One of the job's own working directories goes with them.
            Some(dir) if dirs.owned.contains(dir) => OutputDirProof::Absent,
            Some(dir) => prove_output_dir(dir, job_id, legacy_ok(&shared, dir)).await,
        };
        let listed = match (&output, recorded.as_deref()) {
            (OutputDirProof::Owned, Some(dir)) => Some(dir),
            (_, Some(dir)) if dirs.owned.contains(dir) => Some(dir),
            _ => None,
        };
        match output {
            OutputDirProof::Unreachable => {
                outcomes.insert(
                    job_id,
                    Archive::new(
                        Outcome::Failed,
                        None,
                        Some(format!(
                            "'{}' cannot be reached: neither it nor the folder holding it is there; nothing was removed",
                            recorded
                                .as_deref()
                                .map(|dir| dir.display().to_string())
                                .unwrap_or_default()
                        )),
                    ),
                );
                continue;
            }
            OutputDirProof::Failed(error) => {
                outcomes.insert(
                    job_id,
                    Archive::new(
                        Outcome::Failed,
                        None,
                        Some(format!(
                            "could not inspect '{}': {error}; nothing was removed",
                            recorded
                                .as_deref()
                                .map(|dir| dir.display().to_string())
                                .unwrap_or_default()
                        )),
                    ),
                );
                continue;
            }
            output => {
                if let Some(dir) = listed
                    && let Some(listing) = record_output_files(dir, keep).await
                {
                    records.push((job_id, listing));
                }
                proven.push((job_id, recorded, dirs, output));
            }
        }
    }

    // Kept before the removal, so a stamp that fails after it loses nothing.
    if !records.is_empty() {
        let record_db = db.clone();
        let kept = records.clone();
        let recorded =
            tokio::task::spawn_blocking(move || record_db.record_archived_history_files(&kept))
                .await
                .map_err(|error| crate::StateError::Database(error.to_string()))
                .and_then(|result| result);
        if let Err(error) = recorded {
            warn!(%error, "could not keep the record of output folders before removing them");
            let unrecorded = records
                .iter()
                .map(|(job_id, _)| *job_id)
                .collect::<BTreeSet<_>>();
            proven.retain(|(job_id, ..)| {
                if !unrecorded.contains(job_id) {
                    return true;
                }
                outcomes.insert(
                    *job_id,
                    Archive::new(
                        Outcome::Failed,
                        None,
                        Some(format!(
                            "could not record the files before removing them: {error}; nothing was removed"
                        )),
                    ),
                );
                false
            });
        }
    }

    let held = live_held(db, &listed_dirs).await;
    // A script that started after a job's barrier in step 3, such as one a
    // mark queued while later jobs were listed, stops before the job's files
    // go. Nothing is read again unless a script has started since.
    let removal_order: Vec<u64> = proven.iter().map(|(id, ..)| id.0).collect();
    let mut stamp = Vec::new();
    for (turn, (job_id, recorded, dirs, output)) in proven.into_iter().enumerate() {
        #[cfg(test)]
        archive_removals::run(archive_removals::Step::Turn(job_id));
        if scripted.waits(db, &removal_order[turn..]).await
            && let Err(error) = retry_script_stop_wait(job_id, || {
                #[cfg(test)]
                archive_removals::run(archive_removals::Step::Wait(job_id));
                crate::post_processing::events::wait_for_job_events_stopped(db, job_id.0)
            })
            .await
        {
            outcomes.insert(
                job_id,
                Archive::new(Outcome::Failed, None, Some(error.to_string())),
            );
            continue;
        }
        let removal = remove_job_files(
            intermediate_dir,
            staging_roots,
            job_id,
            recorded.as_deref(),
            dirs,
            output,
            &shared,
        )
        .await;
        let removal = match (removal, listed_dirs.remove(&job_id)) {
            (Ok(mut removal), Some((dir, files))) => match &held {
                Ok(held) => {
                    match remove_listed_files(job_id, intermediate_dir, dir, files, held.clone())
                        .await
                    {
                        Ok(listed) => {
                            removal.removed += listed.removed;
                            for left in listed.not_managed {
                                if !removal.not_managed.contains(&left) {
                                    removal.not_managed.push(left);
                                }
                            }
                            Ok(removal)
                        }
                        Err(error) => Err(crate::SchedulerError::Io(error)),
                    }
                }
                Err(error) => Err(crate::SchedulerError::Internal(format!(
                    "could not read what live torrents hold: {error}; nothing listed was removed"
                ))),
            },
            (removal, _) => removal,
        };
        let outcome = match removal {
            Err(error) => Archive::new(Outcome::Failed, None, Some(error.to_string())),
            Ok(JobFilesRemoval { not_managed, .. }) if !not_managed.is_empty() => Archive::new(
                Outcome::NotManaged,
                None,
                Some(format!(
                    "left {}: not managed by Weaver",
                    not_managed
                        .iter()
                        .map(|dir| dir.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )),
            ),
            Ok(JobFilesRemoval { removed, .. }) => {
                stamp.push(job_id);
                let outcome = if removed > 0 {
                    Outcome::Archived
                } else {
                    Outcome::AlreadyGone
                };
                Archive::new(outcome, Some(now), None)
            }
        };
        outcomes.insert(job_id, outcome);
    }

    if !stamp.is_empty() {
        let stamp_db = db.clone();
        let stamped = stamp.clone();
        let result =
            tokio::task::spawn_blocking(move || stamp_db.stamp_history_archived(&stamped, now))
                .await
                .map_err(|error| crate::StateError::Database(error.to_string()))
                .and_then(|result| result);
        if let Err(error) = result {
            warn!(%error, "could not mark history entries archived");
            for job_id in stamp {
                outcomes.insert(
                    job_id,
                    Archive::new(
                        Outcome::Failed,
                        None,
                        Some(format!(
                            "the files were removed but the entry could not be marked archived: {error}; archiving again marks it"
                        )),
                    ),
                );
            }
        }
    }
    Ok(outcomes)
}

// What a test runs at each job's turn of an archive's removals, and when
// that turn waits for the job's scripts, on the thread that archives.
#[cfg(test)]
pub(crate) mod archive_removals {
    use std::cell::RefCell;

    use crate::jobs::ids::JobId;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Step {
        Turn(JobId),
        Wait(JobId),
    }

    type Hook = Box<dyn FnMut(Step)>;

    thread_local! {
        static RUN: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    pub(crate) fn set(run: impl FnMut(Step) + 'static) {
        RUN.with(|slot| *slot.borrow_mut() = Some(Box::new(run)));
    }

    pub(super) fn run(step: Step) {
        let taken = RUN.with(|slot| slot.borrow_mut().take());
        if let Some(mut run) = taken {
            run(step);
            RUN.with(|slot| {
                let mut slot = slot.borrow_mut();
                if slot.is_none() {
                    *slot = Some(run);
                }
            });
        }
    }
}

struct JobFilesRemoval {
    // How many directories were on disk and went.
    removed: usize,
    // Directories of the job left on disk because nothing proves Weaver
    // manages them.
    not_managed: Vec<PathBuf>,
}

// One job's part of an archive, once its directories are proven and
// its scripts have stopped: its working directories, its recorded output
// directory when proven the job's, and its staging directories.
// A job's recorded folder and the files its row lists: with a list, the
// folder is where those files are, and is no folder of the job's to prove or
// take; only what the list names under it goes.
fn take_listed(
    recorded: Option<PathBuf>,
    files: Option<Vec<HistoryFile>>,
) -> (Option<PathBuf>, Option<(PathBuf, Vec<HistoryFile>)>) {
    match (recorded, files) {
        (Some(dir), Some(files)) => (None, Some((dir, files))),
        (recorded, _) => (recorded, None),
    }
}

async fn remove_job_files(
    intermediate_dir: &std::path::Path,
    staging_roots: &[PathBuf],
    job_id: JobId,
    recorded: Option<&std::path::Path>,
    dirs: JobCleanupDirs,
    output: OutputDirProof,
    shared: &BTreeSet<String>,
) -> Result<JobFilesRemoval, crate::SchedulerError> {
    let RemovedWorkingDirs {
        mut removed,
        left_in_place: mut not_managed,
    } = dirs.remove(intermediate_dir, job_id).await?;
    let leave = |dir: &std::path::Path, not_managed: &mut Vec<PathBuf>| {
        if !not_managed.iter().any(|left| left == dir) {
            not_managed.push(dir.to_path_buf());
        }
    };
    match (output, recorded) {
        (OutputDirProof::Owned, Some(dir)) => {
            match remove_proven_output_dir(dir, job_id, legacy_ok(shared, dir)).await {
                Ok(OutputDirRemoval::Removed) => removed += 1,
                Ok(OutputDirRemoval::Absent) => {}
                Ok(OutputDirRemoval::NotManaged) => leave(dir, &mut not_managed),
                Err(error) => {
                    return Err(crate::SchedulerError::Io(std::io::Error::new(
                        error.kind(),
                        format!("failed to remove '{}': {error}", dir.display()),
                    )));
                }
            }
        }
        (OutputDirProof::NotManaged, Some(dir)) => leave(dir, &mut not_managed),
        _ => {}
    }
    let staged = present_staging_dirs(staging_roots, job_id).await;
    crate::pipeline::queue::remove_job_staging(staging_roots, job_id).await?;
    removed += staged.len();
    Ok(JobFilesRemoval {
        removed,
        not_managed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    // A recorded folder is the job's working directory only as a plain child
    // of the intermediate root: never a destination root that sits there,
    // nor a folder delivered into one.
    #[test]
    fn a_recorded_destination_root_is_never_a_working_dir() {
        let intermediate = std::path::Path::new("/data/intermediate");
        let complete = intermediate.join("complete");
        let destinations = [complete.clone()];
        assert_eq!(
            working_dir_on_record(intermediate, &complete, &destinations),
            None
        );
        assert_eq!(
            working_dir_on_record(intermediate, &complete.join("show"), &destinations),
            None
        );
        let working = intermediate.join("show.42");
        assert_eq!(
            working_dir_on_record(intermediate, &working, &destinations),
            Some(working)
        );
    }

    // A job no script held when the batch was read, then given one before
    // its turn, waits at its barrier: the jobs still to go are read again.
    #[tokio::test]
    async fn a_script_a_later_job_gains_after_the_batch_read_holds_its_barrier() {
        let db = crate::Database::open_in_memory().unwrap();
        let order = [61, 62, 63];
        let mut scripted = LiveScripts::read(&db, order.to_vec()).await;
        assert!(!scripted.waits(&db, &order[0..]).await);
        // Job 62's script starts while job 61's files go.
        let held = crate::post_processing::events::hold_test_event_run(&db, 62, "marked");
        assert!(scripted.waits(&db, &order[1..]).await);
        assert!(!scripted.waits(&db, &order[2..]).await);
        drop(held);
    }

    #[tokio::test(start_paused = true)]
    async fn history_cleanup_surfaces_persistent_failure_after_bounded_backoff() {
        let mut attempts = Vec::new();
        let error = retry_script_stop_wait(JobId(42), || {
            attempts.push(tokio::time::Instant::now());
            std::future::ready(Err(crate::StateError::Database(
                "fixture unavailable".into(),
            )))
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("after seven attempts"));
        assert_eq!(attempts.len(), 7);
        let gaps: Vec<_> = attempts.windows(2).map(|pair| pair[1] - pair[0]).collect();
        assert_eq!(gaps, [1, 2, 4, 8, 16, 32].map(Duration::from_secs));
    }

    #[tokio::test(start_paused = true)]
    async fn history_cleanup_retries_uncertain_wait_and_still_waits_for_active_script() {
        let db = crate::Database::open_in_memory().unwrap();
        let job_id = JobId(41);
        let active =
            crate::post_processing::events::hold_test_event_run(&db, job_id.0, "cleanup-retry");
        let (failed, failure_seen) = oneshot::channel();
        let (retried, retry_seen) = oneshot::channel();
        let cleanup = tokio::spawn(async move {
            let mut failed = Some(failed);
            let mut retried = Some(retried);
            retry_script_stop_wait(job_id, || {
                let failed = failed.take();
                let retried = if failed.is_none() {
                    retried.take()
                } else {
                    None
                };
                let db = db.clone();
                async move {
                    if let Some(failed) = failed {
                        let _ = failed.send(());
                        return Err(crate::StateError::Database(
                            "injected transient read failure".into(),
                        ));
                    }
                    let _ = retried.unwrap().send(());
                    crate::post_processing::events::wait_for_job_events_stopped(&db, job_id.0).await
                }
            })
            .await
            .unwrap();
        });
        failure_seen.await.unwrap();
        assert!(!cleanup.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        retry_seen.await.unwrap();
        assert!(!cleanup.is_finished());
        drop(active);
        cleanup.await.unwrap();
    }
}
