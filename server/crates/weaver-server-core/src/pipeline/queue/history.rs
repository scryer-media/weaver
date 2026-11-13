// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{Change, Queue};
use crate::{HistoryDeleteBatchOutcome, JobId, SchedulerError};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

// How many failed jobs one read of the failed-job sweep lists.
pub(crate) const FAILED_SWEEP_PAGE: usize = 128;

pub(crate) async fn clear_restart_artifacts(
    resources: &super::AdmissionResources,
    id: JobId,
    working_dir: &std::path::Path,
) -> Result<(), SchedulerError> {
    // The reservation and memory preflight precede cleanup. History paths
    // alone are never permission to remove an arbitrary directory.
    crate::post_processing::events::wait_for_job_events_stopped(&resources.db, id.0).await?;
    if tokio::fs::try_exists(working_dir).await? {
        // A folder the job delivered to, or a destination root, is never
        // taken for its working directory, even when the complete folder is
        // the intermediate one: it goes only on its own output marker.
        let working = crate::history::delete_files::working_dir_on_record(
            &resources.intermediate_dir,
            working_dir,
            &resources.staging_roots().await,
        );
        if working.is_some() {
            use crate::jobs::working_dir::{
                HistoryWorkingDir, prepare_history_working_dir, remove_job_working_dir,
            };
            if prepare_history_working_dir(&resources.intermediate_dir, working_dir, id).await?
                != HistoryWorkingDir::Owned
            {
                return Err(SchedulerError::Conflict(
                    "restart directory belongs to another job".into(),
                ));
            }
            let root = resources.intermediate_dir.clone();
            let path = working_dir.to_path_buf();
            let removed =
                tokio::task::spawn_blocking(move || remove_job_working_dir(&root, &path, id))
                    .await
                    .map_err(|error| SchedulerError::Internal(error.to_string()))??;
            if removed != HistoryWorkingDir::Owned {
                return Err(SchedulerError::Conflict(
                    "restart directory ownership changed".into(),
                ));
            }
        } else {
            // A marker naming another job refuses; one from before markers
            // named their job is the job's own output, as it always was here.
            let path = working_dir.to_path_buf();
            tokio::task::spawn_blocking(move || {
                if !crate::jobs::working_dir::output_dir_owner(&path)
                    .is_some_and(|owner| owner.admits(id, true))
                {
                    return Err(SchedulerError::Conflict(
                        "restart output directory has no valid ownership marker of the job".into(),
                    ));
                }
                crate::jobs::working_dir::remove_owned_output_dir(&path)
                    .map_err(SchedulerError::from)
            })
            .await
            .map_err(|error| SchedulerError::Internal(error.to_string()))??;
        }
    }
    clear_left_working_dirs(&resources.intermediate_dir, id, working_dir).await?;
    remove_job_staging(&resources.staging_roots().await, id).await
}

// A working directory that kept the job's marker for what its move could
// not remove goes before the job runs again: the restarted job claims its
// own, or works in `keep`, and no second directory carries its marker.
pub(crate) async fn clear_left_working_dirs(
    intermediate_dir: &std::path::Path,
    id: JobId,
    keep: &std::path::Path,
) -> Result<(), SchedulerError> {
    let root = intermediate_dir.to_path_buf();
    let keep = keep.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let jobs = BTreeSet::from([id]);
        let marked = crate::jobs::working_dir::job_working_dirs(&root, &jobs)?;
        for dir in marked.into_values().flatten().filter(|dir| *dir != keep) {
            crate::jobs::working_dir::remove_job_working_dir(&root, &dir, id)?;
        }
        Ok::<_, std::io::Error>(())
    })
    .await
    .map_err(|error| SchedulerError::Internal(error.to_string()))??;
    Ok(())
}

// The job's staging directories under each of `roots`, whatever they still
// hold. Only a directory carrying the job's marker goes, and no symbolic link
// on the way down from a root is followed.
//
// `roots` is `destination_roots`: the complete folder first, then each
// category's folder. A job's staging directory is under the complete folder
// unless its category delivers elsewhere, and a history row does not say
// which, so every root is swept. The complete folder must be reachable. A
// category folder in which the job's staging directory cannot be looked up
// is passed over with a warning, so one unreachable category does not stop
// the cleanup of jobs that never used it; whatever is found under a folder
// that can be searched is removed or fails as before.
pub(crate) async fn remove_job_staging(
    roots: &[std::path::PathBuf],
    id: JobId,
) -> Result<(), SchedulerError> {
    for (index, root) in roots.iter().enumerate() {
        let staging = super::staging_dir_for(root, id);
        if index > 0 && !staging_dir_reachable(root.clone(), staging.clone(), id).await? {
            continue;
        }
        remove_staging_dir(staging, id).await?;
    }
    Ok(())
}

// Whether the job's staging directory under a category's folder can be looked
// up, which is what removing it needs. Opening the folder is not the test: on
// Linux a directory opens with `O_PATH` even when its mode lets nothing be
// looked up in it. A staging directory that does not exist is reachable, as
// there is nothing to remove.
async fn staging_dir_reachable(
    root: std::path::PathBuf,
    staging: std::path::PathBuf,
    id: JobId,
) -> Result<bool, SchedulerError> {
    tokio::task::spawn_blocking(move || match std::fs::symlink_metadata(&staging) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => {
            tracing::warn!(
                job_id = id.0,
                dir = %root.display(),
                %error,
                "a category folder could not be searched; its staging directory for the job is not checked"
            );
            false
        }
    })
    .await
    .map_err(|error| SchedulerError::Internal(error.to_string()))
}

async fn remove_staging_dir(staging: std::path::PathBuf, id: JobId) -> Result<(), SchedulerError> {
    let removed = tokio::task::spawn_blocking(move || {
        crate::jobs::working_dir::remove_job_staging_dir(&staging, id)
    })
    .await
    .map_err(|error| SchedulerError::Internal(error.to_string()))?;
    match removed {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {
            Err(SchedulerError::Conflict(error.to_string()))
        }
        Err(error) => Err(error.into()),
    }
}

struct HistoryOperation {
    queue: Arc<Queue>,
    ids: BTreeSet<JobId>,
}

impl Drop for HistoryOperation {
    fn drop(&mut self) {
        self.queue.write(|inner| {
            for id in &self.ids {
                inner.history_operations.remove(id);
                inner.admissions.remove(id);
            }
            Change::changed(())
        });
    }
}

// What one failed-job sweep has learned of the places its jobs' files are
// under, each learned once and kept for every page.
pub(crate) struct SweepPlaces {
    // The sweep's one reading of the intermediate root, by the job each
    // marked directory names. None when the root could not be read: no job
    // is judged, held or stamped.
    pub(crate) marked: Option<BTreeMap<JobId, Vec<std::path::PathBuf>>>,
    // The reading missed no entry, so finding nothing of a job means there
    // is nothing of it.
    pub(crate) complete: bool,
    // Each other folder a record stands in, and whether it was there: one
    // check per folder per sweep.
    pub(crate) parents: std::collections::HashMap<std::path::PathBuf, bool>,
    // The jobs left waiting, by the place that could not be judged.
    pub(crate) waiting: BTreeMap<std::path::PathBuf, usize>,
}

impl SweepPlaces {
    pub(crate) fn new(scan: Option<&crate::jobs::working_dir::RootScan>) -> Self {
        Self {
            marked: scan.map(|scan| {
                let mut marked = BTreeMap::<JobId, Vec<std::path::PathBuf>>::new();
                for (owner, path) in &scan.marked {
                    marked.entry(*owner).or_default().push(path.clone());
                }
                marked
            }),
            complete: scan.is_some_and(|scan| scan.unreadable == 0),
            parents: std::collections::HashMap::new(),
            waiting: BTreeMap::new(),
        }
    }
}

// The ids of one page of the failed-job sweep, listed as due while the page
// is swept and unlisted when it ends, however it ends.
struct SweepListing {
    queue: Arc<Queue>,
    ids: Vec<JobId>,
}

impl Drop for SweepListing {
    fn drop(&mut self) {
        self.queue.write(|inner| {
            for id in &self.ids {
                inner.sweep_listed.remove(id);
            }
            Change::unchanged(())
        });
    }
}

impl Queue {
    pub(super) async fn wait_for_cleanup(&self, ids: &BTreeSet<JobId>) {
        let mut revision = self.subscribe();
        while self.read(|inner| ids.iter().any(|id| inner.pending_cleanup.contains(id))) {
            if revision.changed().await.is_err() {
                return;
            }
        }
    }

    pub(crate) async fn restart(
        self: Arc<Self>,
        id: JobId,
        password: Option<String>,
        reprocess: bool,
        no_memory_limit: bool,
        events: tokio::sync::broadcast::Sender<crate::events::model::PipelineEvent>,
    ) -> Result<(), SchedulerError> {
        let mut resources = self
            .resources
            .get()
            .cloned()
            .ok_or(SchedulerError::ChannelClosed)?;
        if let Some(paths) = self.read(|inner| inner.globals.runtime_paths.clone()) {
            resources.intermediate_dir = paths.intermediate;
            resources.complete_dir = paths.complete;
        }
        self.write(|inner| {
            if inner.globals.shutting_down || self.closed.load(std::sync::atomic::Ordering::Acquire)
            {
                return Change::unchanged(Err(SchedulerError::ChannelClosed));
            }
            if inner.contains_job(id) || inner.admissions.contains(&id) {
                return Change::unchanged(Err(SchedulerError::Conflict(
                    "job is still in the queue".into(),
                )));
            }
            if !inner.history_operations.insert(id) {
                return Change::unchanged(Err(SchedulerError::Conflict(
                    "a history operation is still running".into(),
                )));
            }
            inner.admissions.insert(id);
            inner.sweep_listed.remove(&id);
            Change::unchanged(Ok(()))
        })?;
        let guard = HistoryOperation {
            queue: self.clone(),
            ids: BTreeSet::from([id]),
        };
        tokio::spawn(async move {
            let _guard = guard;
            self.wait_for_cleanup(&BTreeSet::from([id])).await;
            let db = resources.db.clone();
            let (spec, path, bytes, previous_dir, downloaded_bytes) =
                tokio::task::spawn_blocking(move || {
                    let row = db
                        .get_job_history_profiled(id.0, "queue.restart.history")?
                        .ok_or(SchedulerError::JobNotFound(id))?;
                    let status = crate::job_status_from_persisted_str(
                        &row.status,
                        row.error_message.as_deref(),
                    );
                    if !matches!(
                        status,
                        crate::JobStatus::Complete
                            | crate::JobStatus::Partial
                            | crate::JobStatus::Failed { .. }
                    ) {
                        return Err(SchedulerError::Conflict(
                            "job is not complete or failed".into(),
                        ));
                    }
                    let (path, bytes) = db
                        .load_history_job_persisted_nzb(id.0)?
                        .ok_or(SchedulerError::JobNotFound(id))?;
                    let bytes = bytes.ok_or_else(|| {
                        SchedulerError::InvalidInput("persisted NZB is missing".into())
                    })?;
                    let nzb =
                        crate::ingest::parse_persisted_nzb_bytes(&bytes).map_err(|error| {
                            SchedulerError::InvalidInput(format!("invalid persisted NZB: {error}"))
                        })?;
                    let mut metadata: Vec<(String, String)> = row
                        .metadata
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(|error| SchedulerError::Internal(error.to_string()))?
                        .unwrap_or_default();
                    use crate::history::attributes::{
                        FAILED_FOR_MEMORY_ATTRIBUTE_KEY, NO_MEMORY_LIMIT_ATTRIBUTE_KEY,
                    };
                    metadata.retain(|(key, _)| {
                        !crate::history::attributes::is_password_attribute_key(key)
                            && key != NO_MEMORY_LIMIT_ATTRIBUTE_KEY
                            && key != FAILED_FOR_MEMORY_ATTRIBUTE_KEY
                            && (reprocess || key != crate::ingest::FULL_FILE_NAMES_METADATA_KEY)
                    });
                    // Kept with the job, so a restart or a restore of it
                    // keeps the limit lifted; a later restart decides again.
                    if no_memory_limit {
                        metadata.push((NO_MEMORY_LIMIT_ATTRIBUTE_KEY.to_string(), "1".to_string()));
                    }
                    let mut spec = crate::ingest::nzb_to_spec(&nzb, &path, row.category, metadata);
                    if let Some(password) = password {
                        spec.password = Some(password);
                    }
                    Ok::<_, SchedulerError>((
                        spec,
                        path,
                        bytes,
                        row.output_dir,
                        row.downloaded_bytes,
                    ))
                })
                .await
                .map_err(|error| SchedulerError::Internal(error.to_string()))??;
            let working_dir = previous_dir
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| {
                    crate::jobs::working_dir::compute_working_dir(
                        &resources.intermediate_dir,
                        id,
                        &spec.name,
                    )
                });
            let prepared = crate::pipeline::Pipeline::prepare_admission(
                &resources,
                id,
                spec,
                path,
                bytes,
                crate::jobs::AddJobOptions::default(),
                Some(super::RestartAdmission {
                    reprocess,
                    working_dir,
                    downloaded_bytes,
                }),
            )
            .await?;
            let info = super::admission::initial_info(
                id,
                &prepared.spec,
                Some(&prepared.summaries),
                prepared.nzb_hash,
                &prepared.working_dir,
                false,
            );
            self.write_intent(|inner| {
                let history = self
                    .history()
                    .iter()
                    .filter(|job| job.job_id != id)
                    .cloned()
                    .collect::<Vec<_>>()
                    .into();
                self.replace_history(inner, history);
                let mut row = super::JobRow::new(info);
                row.admission = Some(prepared);
                inner.push_job(row);
                let _ = events.send(crate::events::model::PipelineEvent::JobResumed { job_id: id });
                Change::changed(())
            });
            Ok(())
        })
        .await
        .map_err(|error| SchedulerError::Internal(error.to_string()))?
    }

    pub(crate) async fn update_history(
        self: Arc<Self>,
        id: JobId,
        mut update: crate::jobs::JobUpdate,
    ) -> Result<(), SchedulerError> {
        let db = self
            .resources
            .get()
            .ok_or(SchedulerError::ChannelClosed)?
            .db
            .clone();
        self.write(|inner| {
            if inner.contains_job(id) || inner.admissions.contains(&id) {
                return Change::unchanged(Err(SchedulerError::Conflict(
                    "the job is still in the queue".into(),
                )));
            }
            if !inner.history_operations.insert(id) {
                return Change::unchanged(Err(SchedulerError::Conflict(
                    "a history operation is still running".into(),
                )));
            }
            Change::unchanged(Ok(()))
        })?;
        let guard = HistoryOperation {
            queue: self.clone(),
            ids: BTreeSet::from([id]),
        };
        update.password = crate::jobs::FieldUpdate::Unchanged;
        tokio::spawn(async move {
            let _guard = guard;
            let persisted = update.clone();
            let found =
                tokio::task::spawn_blocking(move || db.update_job_history_fields(id.0, &persisted))
                    .await
                    .map_err(|error| SchedulerError::Internal(error.to_string()))??;
            if !found {
                return Err(SchedulerError::JobNotFound(id));
            }
            self.write(|inner| {
                let mut history = self.history().to_vec();
                if let Some(job) = history.iter_mut().find(|job| job.job_id == id) {
                    super::intents::apply_update(job, &update);
                }
                self.replace_history(inner, history.into());
                Change::changed(())
            });
            Ok(())
        })
        .await
        .map_err(|error| SchedulerError::Internal(error.to_string()))?
    }

    // Housekeeping for what jobs left on disk: the failed jobs' files when
    // `failed_finished_by` is given, then the working directories of jobs
    // with no row. The intermediate root is read once, off the runtime, and
    // that one reading serves both.
    pub(crate) async fn sweep_job_files(
        self: Arc<Self>,
        failed_finished_by: Option<i64>,
        marked_before: std::time::SystemTime,
    ) {
        let Some(mut resources) = self.resources.get().cloned() else {
            return;
        };
        if let Some(paths) = self.read(|inner| inner.globals.runtime_paths.clone()) {
            resources.intermediate_dir = paths.intermediate;
            resources.complete_dir = paths.complete;
        }
        let root = resources.intermediate_dir.clone();
        let scan =
            tokio::task::spawn_blocking(move || crate::jobs::working_dir::scan_working_dirs(&root))
                .await
                .map_err(std::io::Error::other)
                .and_then(|scan| scan);
        match &scan {
            Ok(scan) if scan.unreadable > 0 => tracing::warn!(
                root = %resources.intermediate_dir.display(),
                entries = scan.unreadable,
                "entries of the intermediate directory could not be read; housekeeping goes on without them and marks no failed job as swept this time"
            ),
            Ok(_) => {}
            Err(error) => tracing::debug!(
                root = %resources.intermediate_dir.display(),
                error = %error,
                "the intermediate directory could not be read for housekeeping"
            ),
        }
        if let Some(finished_by) = failed_finished_by {
            let mut places = SweepPlaces::new(scan.as_ref().ok());
            self.clone()
                .sweep_failed_job_files(&resources, &mut places, finished_by)
                .await;
            for (place, jobs) in &places.waiting {
                tracing::warn!(
                    place = %place.display(),
                    jobs,
                    "a folder failed jobs' files are under cannot be read now; housekeeping leaves those jobs for a later sweep"
                );
            }
        }
        if let Ok(scan) = scan {
            self.sweep_unrecorded_working_dirs(&resources, marked_before, scan.marked)
                .await;
        }
    }

    // Every failed job whose record says it finished by `finished_by`, in
    // epoch seconds, gives up the working directories that carry its marker
    // and its staging directory, and keeps its record. The pages are walked
    // by a cursor, so a job left unstamped is passed, not listed again in
    // the same sweep.
    async fn sweep_failed_job_files(
        self: Arc<Self>,
        resources: &super::AdmissionResources,
        places: &mut SweepPlaces,
        finished_by: i64,
    ) {
        let mut staging_roots = None;
        let mut after = None;
        loop {
            let db = resources.db.clone();
            let page = match tokio::task::spawn_blocking(move || {
                db.failed_job_history_page(finished_by, after, FAILED_SWEEP_PAGE)
            })
            .await
            {
                Ok(Ok(page)) => page,
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, "could not list failed jobs for housekeeping");
                    return;
                }
                Err(error) => {
                    tracing::warn!(error = %error, "could not list failed jobs for housekeeping");
                    return;
                }
            };
            let Some(&(completed_at, last, _)) = page.last() else {
                return;
            };
            after = Some((completed_at, last));
            let full = page.len() == FAILED_SWEEP_PAGE;
            if staging_roots.is_none() && places.marked.is_some() {
                staging_roots = Some(resources.staging_roots().await);
            }
            let page = page
                .into_iter()
                .map(|(_, id, recorded)| (id, recorded.map(std::path::PathBuf::from)))
                .collect();
            if !self
                .clone()
                .sweep_failed_page(
                    resources,
                    staging_roots.as_deref().unwrap_or_default(),
                    places,
                    finished_by,
                    page,
                )
                .await
                || !full
            {
                return;
            }
        }
    }

    // One job of a page: held, swept and released, with its id back when
    // the sweep has done all it ever will for it and it is to be stamped.
    // The hold is refused for a job the page no longer lists as due (it was
    // admitted since the page's reading), one in the queue or being
    // admitted, and one another history operation holds; it is gone when
    // this returns, so no other job waits on this one's files.
    pub(crate) async fn sweep_failed_job(
        self: &Arc<Self>,
        resources: &super::AdmissionResources,
        staging_roots: &[std::path::PathBuf],
        id: JobId,
        recorded: Option<std::path::PathBuf>,
        marked: Vec<std::path::PathBuf>,
    ) -> Option<JobId> {
        let held = self.write(|inner| {
            Change::unchanged(
                inner.sweep_listed.remove(&id)
                    && !inner.contains_job(id)
                    && !inner.admissions.contains(&id)
                    && inner.history_operations.insert(id),
            )
        });
        if !held {
            return None;
        }
        let _guard = HistoryOperation {
            queue: self.clone(),
            ids: BTreeSet::from([id]),
        };
        match crate::history::delete_files::sweep_failed_job_files(
            &resources.intermediate_dir,
            staging_roots,
            id,
            recorded,
            marked,
        )
        .await
        {
            Ok(swept) => swept.then_some(id),
            Err(error) => {
                tracing::warn!(
                    job_id = id.0,
                    error = %error,
                    "could not remove a failed job's files; the next housekeeping sweep tries again"
                );
                None
            }
        }
    }

    // One page of the failed-job sweep. Disk calls that learn whether a
    // place can be judged are made per place, never per job: the sweep's
    // one reading of the intermediate root, and one existence check for
    // each other folder a page's records stand in, kept in `places` for the
    // whole sweep. A job under a place that cannot be judged now is counted
    // there and passed with no disk call, unstamped. Two reads serve the
    // page: which of its jobs are still the failures listed, and which
    // still have a script out. The rest are held, swept and released one at
    // a time, and stamped together at the end. False when the sweep stops
    // here.
    pub(crate) async fn sweep_failed_page(
        self: Arc<Self>,
        resources: &super::AdmissionResources,
        staging_roots: &[std::path::PathBuf],
        places: &mut SweepPlaces,
        finished_by: i64,
        page: Vec<(JobId, Option<std::path::PathBuf>)>,
    ) -> bool {
        let root = resources.intermediate_dir.clone();
        if places.marked.is_none() {
            // The root cannot be judged: every job's answer waits on it.
            *places.waiting.entry(root).or_default() += page.len();
            return true;
        }
        // Each folder this page's records stand in that the sweep has not
        // judged yet: one check each, off the runtime.
        let unjudged: BTreeSet<std::path::PathBuf> = page
            .iter()
            .filter_map(|(_, recorded)| recorded.as_deref()?.parent())
            .filter(|parent| *parent != root && !places.parents.contains_key(*parent))
            .map(std::path::Path::to_path_buf)
            .collect();
        if !unjudged.is_empty() {
            match tokio::task::spawn_blocking(move || {
                unjudged
                    .into_iter()
                    .map(|parent| {
                        let there = std::fs::symlink_metadata(&parent).is_ok();
                        (parent, there)
                    })
                    .collect::<Vec<_>>()
            })
            .await
            {
                Ok(judged) => places.parents.extend(judged),
                Err(error) => {
                    tracing::warn!(error = %error, "could not judge failed jobs' folders for housekeeping");
                    return false;
                }
            }
        }
        let db = resources.db.clone();
        let ids: Vec<JobId> = page.iter().map(|(id, _)| *id).collect();
        // Listed before the read below: a job admitted from here on is
        // unlisted by its admission, and its hold is then refused, so each
        // job is swept only while the page's reading of its row still
        // stands. No job's row is read again under its hold.
        self.write(|inner| {
            inner.sweep_listed.extend(ids.iter().copied());
            Change::unchanged(())
        });
        let _listing = SweepListing {
            queue: self.clone(),
            ids: ids.clone(),
        };
        // A job retried or deleted since the listing, and one whose scripts
        // still run, are left for a later sweep, unstamped.
        let ready = tokio::task::spawn_blocking(move || {
            let still_failed = db.failed_jobs_finished_by(finished_by, &ids)?;
            let raw = ids.iter().map(|id| id.0).collect::<Vec<_>>();
            let scripts = db.jobs_with_live_script_events(&raw)?;
            Ok::<_, crate::StateError>(
                ids.into_iter()
                    .filter(|id| still_failed.contains(id) && !scripts.contains(&id.0))
                    .collect::<BTreeSet<_>>(),
            )
        })
        .await;
        let ready = match ready {
            Ok(Ok(ready)) => ready,
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "could not list failed jobs for housekeeping");
                return false;
            }
            Err(error) => {
                tracing::warn!(error = %error, "could not list failed jobs for housekeeping");
                return false;
            }
        };
        // A job housekeeping has done all it ever will for gets the internal
        // swept stamp, with the others of its page in one write, so no later
        // sweep lists, holds or reads for it again; nothing the entry shows
        // changes. The stamp takes only an entry that is still the failure
        // listed: one retried after its hold went is listed afresh. A
        // reading of the root that missed entries stamps no one: a folder
        // it could not read may be any job's.
        let mut swept = Vec::new();
        for (id, recorded) in page {
            if !ready.contains(&id) {
                continue;
            }
            if let Some(parent) = recorded.as_deref().and_then(std::path::Path::parent)
                && parent != resources.intermediate_dir
                && places.parents.get(parent) == Some(&false)
            {
                *places.waiting.entry(parent.to_path_buf()).or_default() += 1;
                continue;
            }
            let marked = places
                .marked
                .as_mut()
                .and_then(|marked| marked.remove(&id))
                .unwrap_or_default();
            if let Some(id) = self
                .sweep_failed_job(resources, staging_roots, id, recorded, marked)
                .await
                && places.complete
            {
                swept.push(id);
            }
        }
        if !swept.is_empty() {
            let db = resources.db.clone();
            let swept_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |since| since.as_secs() as i64);
            match tokio::task::spawn_blocking(move || {
                db.stamp_history_swept(&swept, swept_at, finished_by)
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, "could not record the failed jobs housekeeping swept; the next sweep tries them again");
                }
                Err(error) => {
                    tracing::warn!(error = %error, "could not record the failed jobs housekeeping swept; the next sweep tries them again");
                }
            }
        }
        true
    }

    // The working directories of jobs with no row, from the sweep's one
    // reading of the intermediate root: each carries a marker older than
    // `marked_before` naming a job nothing records. Two batched reads judge
    // them all under their holds: which owners are recorded after all, and
    // which still have a script out; those are left for a later sweep.
    async fn sweep_unrecorded_working_dirs(
        self: Arc<Self>,
        resources: &super::AdmissionResources,
        marked_before: std::time::SystemTime,
        marked: Vec<(JobId, std::path::PathBuf)>,
    ) {
        if marked.is_empty() {
            return;
        }
        let root = resources.intermediate_dir.clone();
        let db = resources.db.clone();
        let unclaimed = match tokio::task::spawn_blocking(move || {
            let known = db.recorded_job_ids()?;
            Ok::<_, crate::StateError>(
                marked
                    .into_iter()
                    .filter(|(owner, path)| {
                        !known.contains(&owner.0)
                            && std::fs::metadata(crate::jobs::working_dir::working_dir_marker_path(
                                path,
                            ))
                            .and_then(|marker| marker.modified())
                            .is_ok_and(|stamped| stamped < marked_before)
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .await
        {
            Ok(Ok(unclaimed)) => unclaimed,
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "could not list recorded jobs for housekeeping");
                return;
            }
            Err(error) => {
                tracing::warn!(error = %error, "could not list recorded jobs for housekeeping");
                return;
            }
        };
        if unclaimed.is_empty() {
            return;
        }
        let mut held = Vec::new();
        for (owner, path) in unclaimed {
            let hold = self.write(|inner| {
                Change::unchanged(
                    !inner.contains_job(owner)
                        && !inner.admissions.contains(&owner)
                        && inner.history_operations.insert(owner),
                )
            });
            if hold {
                held.push((
                    HistoryOperation {
                        queue: self.clone(),
                        ids: BTreeSet::from([owner]),
                    },
                    owner,
                    path,
                ));
            }
        }
        // The rows are read again under the holds: a job admitted or
        // restored since the scan has the last word. The removals follow in
        // the same step, off the runtime.
        let db = resources.db.clone();
        let removed = tokio::task::spawn_blocking(move || {
            let known = db.recorded_job_ids()?;
            let owners = held.iter().map(|(_, owner, _)| owner.0).collect::<Vec<_>>();
            let scripts = db.jobs_with_live_script_events(&owners)?;
            for (_guard, owner, path) in held {
                if !known.contains(&owner.0) && !scripts.contains(&owner.0) {
                    crate::jobs::working_dir::remove_unclaimed_working_dir(&root, &path);
                }
            }
            Ok::<_, crate::StateError>(())
        })
        .await;
        match removed {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "could not list recorded jobs for housekeeping");
            }
            Err(error) => {
                tracing::warn!(error = %error, "could not remove unreferenced intermediate directories");
            }
        }
    }

    pub(crate) async fn delete_history(
        self: Arc<Self>,
        ids: BTreeSet<JobId>,
        delete_files: bool,
    ) -> Result<HistoryDeleteBatchOutcome, SchedulerError> {
        let mut resources = self
            .resources
            .get()
            .cloned()
            .ok_or(SchedulerError::ChannelClosed)?;
        if let Some(paths) = self.read(|inner| inner.globals.runtime_paths.clone()) {
            resources.intermediate_dir = paths.intermediate;
        }
        let (accepted, refused) = self.write(|inner| {
            let mut accepted = BTreeSet::new();
            let mut refused = BTreeMap::new();
            for id in ids {
                if inner.contains_job(id) || inner.admissions.contains(&id) {
                    refused.insert(
                        id,
                        SchedulerError::Conflict(
                            "cannot delete active job — cancel it first".into(),
                        ),
                    );
                } else if !inner.history_operations.insert(id) {
                    refused.insert(
                        id,
                        SchedulerError::Conflict("history deletion is still running".into()),
                    );
                } else {
                    accepted.insert(id);
                }
            }
            Change::unchanged((accepted, refused))
        });
        if accepted.is_empty() {
            return Ok(HistoryDeleteBatchOutcome {
                deleted: BTreeMap::new(),
                refused,
            });
        }
        let guard = HistoryOperation {
            queue: self.clone(),
            ids: accepted.clone(),
        };
        tokio::spawn(async move {
            let _guard = guard;
            self.wait_for_cleanup(&accepted).await;
            let (removed, result) = crate::history::delete_files::delete_history_rows(
                &resources.db,
                &resources.intermediate_dir,
                &resources.staging_roots().await,
                accepted,
                delete_files,
            )
            .await;
            self.write(|inner| {
                let history = self
                    .history()
                    .iter()
                    .filter(|job| !removed.contains(&job.job_id))
                    .cloned()
                    .collect::<Vec<_>>()
                    .into();
                self.replace_history(inner, history);
                Change::changed(())
            });
            let mut outcome = result?;
            outcome.refused.extend(refused);
            Ok(outcome)
        })
        .await
        .map_err(|error| {
            SchedulerError::Internal(format!("history delete worker failed: {error}"))
        })?
    }

    // Archives history entries: removes their files and keeps the entries, under the
    // same hold a history delete takes. An id in the queue, being admitted,
    // or held by another history operation is busy and nothing of it is
    // touched.
    #[cfg(test)]
    pub(crate) async fn archive_history(
        self: Arc<Self>,
        ids: BTreeSet<JobId>,
        now: i64,
    ) -> Result<BTreeMap<JobId, crate::ArchiveHistoryResult>, SchedulerError> {
        self.archive_history_keeping(
            ids,
            now,
            crate::history::output_files::ARCHIVED_HISTORY_FILES_KEPT,
        )
        .await
    }

    // `archive_history`, keeping the record of at most `keep` files of
    // each archived entry's output directory.
    #[cfg(test)]
    pub(crate) async fn archive_history_keeping(
        self: Arc<Self>,
        ids: BTreeSet<JobId>,
        now: i64,
        keep: usize,
    ) -> Result<BTreeMap<JobId, crate::ArchiveHistoryResult>, SchedulerError> {
        self.archive_history_settling(ids, now, keep, None).await
    }

    // The archive, and with `operation` the settling of that archive
    // operation's target of every id with its outcome. The targets are
    // written before the hold is released, so the revision the release
    // rings finds them; when every id is busy and no hold is taken, the
    // revision is rung once they are written.
    pub(crate) async fn archive_history_settling(
        self: Arc<Self>,
        ids: BTreeSet<JobId>,
        now: i64,
        keep: usize,
        operation: Option<u64>,
    ) -> Result<BTreeMap<JobId, crate::ArchiveHistoryResult>, SchedulerError> {
        let mut resources = self
            .resources
            .get()
            .cloned()
            .ok_or(SchedulerError::ChannelClosed)?;
        if let Some(paths) = self.read(|inner| inner.globals.runtime_paths.clone()) {
            resources.intermediate_dir = paths.intermediate;
        }
        let (accepted, mut outcomes) = self.write(|inner| {
            let mut accepted = BTreeSet::new();
            let mut busy = BTreeMap::new();
            for id in ids {
                let message = if inner.contains_job(id) || inner.admissions.contains(&id) {
                    "the job is in the queue"
                } else if !inner.history_operations.insert(id) {
                    "another history operation is running"
                } else {
                    accepted.insert(id);
                    continue;
                };
                busy.insert(
                    id,
                    crate::ArchiveHistoryResult::new(
                        crate::ArchiveHistoryOutcome::Busy,
                        None,
                        Some(message.into()),
                    ),
                );
            }
            Change::unchanged((accepted, busy))
        });
        if accepted.is_empty() {
            if let Some(operation) = operation {
                settle_archive_history(&resources.db, operation, &outcomes).await?;
                self.write(|_| Change::changed(()));
            }
            return Ok(outcomes);
        }
        let guard = HistoryOperation {
            queue: self.clone(),
            ids: accepted.clone(),
        };
        tokio::spawn(async move {
            let _guard = guard;
            self.wait_for_cleanup(&accepted).await;
            let archived = crate::history::delete_files::archive_history(
                &resources.db,
                &resources.intermediate_dir,
                &resources.staging_roots().await,
                accepted,
                now,
                keep,
            )
            .await?;
            outcomes.extend(archived);
            if let Some(operation) = operation {
                settle_archive_history(&resources.db, operation, &outcomes).await?;
            }
            // Releasing the hold rings the queue revision, as a history
            // edit's does, so the history list reads the stamp and the
            // settled targets again.
            Ok(outcomes)
        })
        .await
        .map_err(|error| {
            SchedulerError::Internal(format!("history archive worker failed: {error}"))
        })?
    }
}

// Writes each id's outcome on its target of the archive `operation`:
// an entry left with nothing to remove completes, every other one fails
// with its message, which the entry shows until a later archive settles it.
async fn settle_archive_history(
    db: &crate::Database,
    operation: u64,
    outcomes: &BTreeMap<JobId, crate::ArchiveHistoryResult>,
) -> Result<(), SchedulerError> {
    use crate::AsyncOperationTargetState as State;
    let settled = outcomes
        .iter()
        .map(|(id, archive)| crate::ArchiveHistorySettlement {
            target_id: id.0,
            state: if archive.outcome.settles() {
                State::Completed
            } else {
                State::Failed
            },
            outcome: archive.outcome,
            message: archive.message.clone(),
        })
        .collect::<Vec<_>>();
    let db = db.clone();
    tokio::task::spawn_blocking(move || db.settle_archive_history_targets(operation, &settled))
        .await
        .map_err(|error| SchedulerError::Internal(error.to_string()))?
        .map_err(SchedulerError::State)
}

#[cfg(all(test, unix))]
mod staging_root_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    // A category folder Weaver cannot open does not stop the cleanup: the
    // job's staging directory under the complete folder still goes.
    #[tokio::test]
    async fn an_unreadable_category_folder_does_not_block_the_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let complete = temp.path().join("complete");
        let category = temp.path().join("locked");
        let job_id = JobId(41);
        let staging = crate::pipeline::queue::staging_dir_for(&complete, job_id);
        crate::jobs::working_dir::make_job_staging_dir(&staging, job_id).unwrap();
        std::fs::write(staging.join("episode.mkv"), b"staged").unwrap();
        std::fs::create_dir_all(&category).unwrap();
        std::fs::set_permissions(&category, std::fs::Permissions::from_mode(0o000)).unwrap();

        let result = remove_job_staging(&[complete.clone(), category.clone()], job_id).await;
        std::fs::set_permissions(&category, std::fs::Permissions::from_mode(0o755)).unwrap();

        result.unwrap();
        assert!(!staging.exists());
    }

    // The complete folder is where a job stages unless its category says
    // otherwise, so it must be reachable.
    #[tokio::test]
    async fn an_unreadable_complete_folder_fails_the_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let complete = temp.path().join("complete");
        std::fs::create_dir_all(&complete).unwrap();
        std::fs::set_permissions(&complete, std::fs::Permissions::from_mode(0o000)).unwrap();
        let locked_out = std::fs::read_dir(&complete).is_err();

        let result = remove_job_staging(std::slice::from_ref(&complete), JobId(42)).await;
        std::fs::set_permissions(&complete, std::fs::Permissions::from_mode(0o755)).unwrap();

        // A user the mode does not hold back (root) reaches it either way.
        if locked_out {
            assert!(result.is_err());
        }
    }
}
