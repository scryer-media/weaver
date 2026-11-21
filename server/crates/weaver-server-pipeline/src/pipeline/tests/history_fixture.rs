// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::ArchiveHistoryResult;
use crate::history::queries::HistoryQueries;

pub const NOW: i64 = 1_800_000_000;

// Archiving needs no pipeline turn, as deleting history does not.
pub async fn archive_entries(
    pipeline: &mut Pipeline,
    job_ids: &[JobId],
    now: i64,
) -> BTreeMap<JobId, ArchiveHistoryResult> {
    pipeline.report_all_for_test();
    pipeline
        .shared_state
        .queue
        .clone()
        .archive_history(job_ids.iter().copied().collect(), now)
        .await
        .unwrap()
}

// A delivered output directory under the complete folder with Weaver's
// ownership marker, holding one file.
pub fn owned_output_dir(complete_dir: &Path, name: &str, job_id: JobId) -> PathBuf {
    let dir = complete_dir.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("lantern.mkv"), b"delivered").unwrap();
    crate::jobs::working_dir::mark_weaver_owned_output_dir(&dir, job_id).unwrap();
    dir
}

pub fn marked_working_dir(intermediate_dir: &Path, name: &str, job_id: JobId) -> PathBuf {
    let dir = intermediate_dir.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    crate::jobs::working_dir::mark_weaver_owned_working_dir(intermediate_dir, &dir, job_id)
        .unwrap();
    std::fs::write(dir.join("lantern.par2"), b"left").unwrap();
    dir
}

pub fn staging_dir(complete_dir: &Path, job_id: JobId) -> PathBuf {
    let staging = crate::pipeline::queue::staging_dir_for(complete_dir, job_id);
    crate::pipeline::queue::make_staging_dir(&staging, job_id).unwrap();
    std::fs::write(staging.join("lantern.mkv"), b"staged").unwrap();
    staging
}

// The record an archive kept of an entry's files: its rows, then the totals
// on the entry.
pub type Record = (Vec<(String, u64)>, Option<u64>, Option<u64>);

pub fn record_of(pipeline: &Pipeline, job_id: JobId) -> Record {
    let rows = archived_rows(pipeline, job_id);
    let state = pipeline
        .db
        .list_job_history_file_states(&[job_id])
        .unwrap()
        .remove(&job_id)
        .expect("the history entry stays");
    (rows, state.archived_file_count, state.archived_file_bytes)
}

pub fn archived_rows(pipeline: &Pipeline, job_id: JobId) -> Vec<(String, u64)> {
    pipeline
        .db
        .list_archived_history_files(job_id.0, 0, 1_000)
        .unwrap()
}

pub const NO_RECORD: fn() -> Record = || (Vec::new(), None, None);

pub async fn archive_entries_keeping(
    pipeline: &mut Pipeline,
    job_ids: &[JobId],
    now: i64,
    keep: usize,
) -> BTreeMap<JobId, ArchiveHistoryResult> {
    pipeline.report_all_for_test();
    pipeline
        .shared_state
        .queue
        .clone()
        .archive_history_keeping(job_ids.iter().copied().collect(), now, keep)
        .await
        .unwrap()
}

pub fn write_file(dir: &Path, rel: &str, bytes: usize) {
    let path = dir.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, vec![b'x'; bytes]).unwrap();
}

pub fn stamp_of(pipeline: &Pipeline, job_id: JobId) -> Option<i64> {
    pipeline
        .db
        .get_job_history(job_id.0)
        .unwrap()
        .expect("the history entry stays")
        .archived_at
}

// History deletion finishes without a pipeline turn.
pub async fn delete_history_batch(
    pipeline: &mut Pipeline,
    job_ids: &[JobId],
    delete_files: bool,
) -> crate::HistoryDeleteBatchOutcome {
    pipeline.report_all_for_test();
    pipeline
        .shared_state
        .queue
        .clone()
        .delete_history(job_ids.iter().copied().collect(), delete_files)
        .await
        .unwrap()
}

pub fn database(pipeline: &Pipeline) -> &Database {
    &pipeline.db
}

pub use crate::history::delete_files::archive_removals;

pub fn hold_history(pipeline: &Pipeline, id: JobId) {
    pipeline.shared_state.queue.write(|inner| {
        inner.history_operations.insert(id);
        crate::pipeline::queue::Change::unchanged(())
    });
}

pub fn history_holds(pipeline: &Pipeline) -> std::collections::HashSet<JobId> {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.history_operations.clone())
}

pub async fn delete_reported_history(
    pipeline: &Pipeline,
    ids: &[JobId],
    delete_files: bool,
) -> crate::HistoryDeleteBatchOutcome {
    pipeline
        .shared_state
        .queue
        .clone()
        .delete_history(ids.iter().copied().collect(), delete_files)
        .await
        .unwrap()
}

// A working directory under `intermediate_dir` carrying `job_id`'s marker,
// holding one file.
pub async fn marked_sweep_working_dir(
    intermediate_dir: &Path,
    name: &str,
    job_id: JobId,
) -> PathBuf {
    let dir = intermediate_dir.join(name);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    crate::jobs::working_dir::mark_weaver_owned_working_dir(intermediate_dir, &dir, job_id)
        .unwrap();
    tokio::fs::write(dir.join("lantern.part01.rar"), b"kept")
        .await
        .unwrap();
    dir
}

pub fn history_row_finished_at(
    job_id: JobId,
    status: &str,
    output_dir: PathBuf,
    completed_at: i64,
) -> crate::JobHistoryRow {
    crate::JobHistoryRow {
        completed_at,
        ..history_row_with_output_dir(job_id, "Housekeeping", status, output_dir)
    }
}

// A marker written now is younger than this, so every marked directory
// without a row is old enough to go.
pub fn after_every_marker() -> std::time::SystemTime {
    std::time::SystemTime::now() + std::time::Duration::from_secs(24 * 60 * 60)
}

// The internal swept stamp of each job, read raw: no view selects it.
pub async fn swept_at(pipeline: &Pipeline, id: JobId) -> Option<i64> {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    SqlRuntime::fetch_all(
        pipeline.db.datastore().read_exec(),
        "SELECT files_swept_at FROM job_history WHERE job_id = {}",
        &[SqlArg::I64(id.0 as i64)],
    )
    .await
    .unwrap()
    .remove(0)
    .opt_i64("files_swept_at")
    .unwrap()
}

pub const FAILED_SWEEP_PAGE: usize = crate::pipeline::queue::FAILED_SWEEP_PAGE;

pub async fn sweep_job_files(
    pipeline: &Pipeline,
    failed_finished_by: Option<i64>,
    marked_before: std::time::SystemTime,
) {
    pipeline
        .shared_state
        .queue
        .clone()
        .sweep_job_files(failed_finished_by, marked_before)
        .await;
}

pub fn staging_dir_for(root: &Path, id: JobId) -> PathBuf {
    crate::pipeline::queue::staging_dir_for(root, id)
}

pub fn make_staging_dir(path: &Path, id: JobId) -> std::io::Result<()> {
    crate::pipeline::queue::make_staging_dir(path, id)
}

pub struct SweepPage(crate::pipeline::queue::SweepPlaces);

impl SweepPage {
    pub fn unread() -> Self {
        Self(crate::pipeline::queue::SweepPlaces::new(None))
    }
    pub fn read(marked: Vec<(JobId, PathBuf)>, unreadable: usize) -> Self {
        Self(crate::pipeline::queue::SweepPlaces::new(Some(
            &crate::jobs::working_dir::RootScan { marked, unreadable },
        )))
    }
    pub fn parents(&self) -> &std::collections::HashMap<PathBuf, bool> {
        &self.0.parents
    }
    pub fn waiting(&self) -> &BTreeMap<PathBuf, usize> {
        &self.0.waiting
    }
    pub async fn sweep(
        &mut self,
        pipeline: &Pipeline,
        staging_roots: &[PathBuf],
        finished_by: i64,
        page: Vec<(JobId, Option<PathBuf>)>,
    ) -> bool {
        let queue = pipeline.shared_state.queue.clone();
        let resources = queue.resources.get().cloned().unwrap();
        queue
            .sweep_failed_page(&resources, staging_roots, &mut self.0, finished_by, page)
            .await
    }
}

pub fn sweep_root(pipeline: &Pipeline) -> &Path {
    &pipeline
        .shared_state
        .queue
        .resources
        .get()
        .unwrap()
        .intermediate_dir
}

pub fn sweep_list_and_holds_empty(pipeline: &Pipeline) -> bool {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.sweep_listed.is_empty() && inner.history_operations.is_empty())
}

pub fn between_sweeps(run: impl FnMut(JobId) + 'static) {
    crate::pipeline::queue::between_sweeps::set(run);
}

pub struct SweepJob {
    queue: Arc<crate::pipeline::queue::Queue>,
    resources: crate::pipeline::queue::AdmissionResources,
}
impl SweepJob {
    pub fn observe(pipeline: &Pipeline) -> Self {
        let queue = pipeline.shared_state.queue.clone();
        let resources = queue.resources.get().cloned().unwrap();
        Self { queue, resources }
    }
    pub fn list(&self, ids: impl IntoIterator<Item = JobId>) {
        self.queue.write(|inner| {
            inner.sweep_listed.extend(ids);
            crate::pipeline::queue::Change::unchanged(())
        });
    }
    pub async fn sweep(
        &self,
        roots: &[PathBuf],
        id: JobId,
        recorded: Option<PathBuf>,
        marked: Vec<PathBuf>,
    ) -> Option<JobId> {
        self.queue
            .sweep_failed_job(&self.resources, roots, id, recorded, marked)
            .await
    }
    pub fn warned(&self, id: JobId) -> bool {
        self.queue.read(|inner| inner.sweep_warned.contains(&id))
    }
    pub fn warning_count(&self) -> usize {
        self.queue.read(|inner| inner.sweep_warned.len())
    }
}

pub const STAGING_DIR_NAME: &str = crate::jobs::working_dir::STAGING_DIR_NAME;

pub fn set_empty_memory_budget(pipeline: &mut Pipeline) {
    pipeline.process_memory_budget = Arc::new(ProcessMemoryBudget::new(0));
}
pub fn memory_reserved(pipeline: &Pipeline) -> u64 {
    pipeline.process_memory_budget.reserved_bytes()
}

pub async fn admit_job(
    pipeline: &mut Pipeline,
    id: JobId,
    spec: JobSpec,
    nzb_path: PathBuf,
    nzb: Vec<u8>,
) -> PathBuf {
    let resources = pipeline.admission_resources();
    let prepared = Pipeline::prepare_add(
        &resources,
        id,
        spec,
        nzb_path,
        nzb,
        crate::jobs::AddJobOptions::default(),
    )
    .await
    .unwrap();
    let dir = prepared.working_dir.clone();
    pipeline.install_prepared_add(id, prepared);
    dir
}
pub async fn clear_restart_artifacts(
    pipeline: &Pipeline,
    id: JobId,
    dir: &Path,
) -> Result<(), crate::SchedulerError> {
    crate::pipeline::queue::clear_restart_artifacts(&pipeline.admission_resources(), id, dir).await
}
pub async fn clear_left_working_dirs(
    root: &Path,
    id: JobId,
    dir: &Path,
) -> Result<(), crate::SchedulerError> {
    crate::pipeline::queue::clear_left_working_dirs(root, id, dir).await
}
pub fn start_history_delete(
    pipeline: &Pipeline,
    ids: BTreeSet<JobId>,
    delete_files: bool,
) -> tokio::task::JoinHandle<Result<crate::HistoryDeleteBatchOutcome, crate::SchedulerError>> {
    tokio::spawn(
        pipeline
            .shared_state
            .queue
            .clone()
            .delete_history(ids, delete_files),
    )
}

pub fn answer_diagnostics_request(pipeline: &Pipeline) {
    pipeline.answer_diagnostics_request();
}
pub fn set_installed_status(pipeline: &mut Pipeline, id: JobId, status: JobStatus) {
    pipeline.jobs.get_mut(&id).unwrap().status = status;
}

pub fn stamp_history_swept(
    pipeline: &Pipeline,
    ids: &[JobId],
    swept_at: i64,
    finished_by: i64,
) -> Result<(), crate::StateError> {
    use crate::history::persistence::HistoryPersistenceRecordArchivedHistoryFilesInternalQueries;
    pipeline.db.stamp_history_swept(ids, swept_at, finished_by)
}

// Park the serialized database writer until the returned sender is dropped, so
// a queued archive cannot commit while the test inspects the event stream.
pub fn hold_database_writer(pipeline: &Pipeline) -> std::sync::mpsc::Sender<()> {
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    pipeline
        .db
        .try_queue_write("test_hold_database_writer", move |_db| {
            let _ = release_rx.recv();
            Ok(())
        })
        .unwrap();
    release_tx
}

pub async fn wait_for_job_event(
    events: &mut broadcast::Receiver<PipelineEvent>,
    is_expected: impl Fn(&PipelineEvent) -> bool,
) {
    async {
        loop {
            let event = events.recv().await.expect("pipeline event channel closed");
            if is_expected(&event) {
                return;
            }
        }
    }
    .await;
}

pub fn record_history(pipeline: &mut Pipeline, id: JobId) {
    pipeline.record_job_history(id, None);
}
pub async fn finished_jobs(pipeline: &mut Pipeline) -> Vec<JobInfo> {
    pipeline.finished_jobs().await
}
pub fn published_job(pipeline: &Pipeline, id: JobId) -> Option<JobInfo> {
    pipeline.shared_state.get_job(id)
}
pub fn pending_completion_checks(pipeline: &Pipeline) -> Vec<JobId> {
    pipeline.pending_completion_checks.iter().copied().collect()
}
pub fn installed_download_state(pipeline: &Pipeline, id: JobId) -> crate::DownloadState {
    pipeline.jobs[&id].download_state
}
pub fn installed_staging_dir(pipeline: &Pipeline, id: JobId) -> Option<PathBuf> {
    pipeline
        .jobs
        .get(&id)
        .and_then(|state| state.staging_dir.clone())
}
pub fn is_finalizing_download(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.jobs_finalizing_download.contains(&id)
}
pub async fn install_carries_post_processing(
    pipeline: &Pipeline,
    id: JobId,
    request: RestoreJobRequest,
) -> bool {
    crate::jobs::service::read_install(pipeline.install_seat(id), request)
        .await
        .carries_post_processing()
}
pub fn fill_finished_history(pipeline: &Pipeline, base_job_id: u64) {
    pipeline.shared_state.queue.write(|inner| {
        let history = (0..crate::jobs::FINISHED_JOBS_RUNTIME_CAP)
            .map(|offset| finished_job_info(JobId(base_job_id + offset as u64)))
            .collect::<Vec<_>>()
            .into();
        pipeline.shared_state.queue.replace_history(inner, history);
        crate::pipeline::queue::Change::changed(())
    });
}
pub const FINISHED_JOBS_RUNTIME_CAP: usize = crate::jobs::FINISHED_JOBS_RUNTIME_CAP;

pub fn update_queue_metrics(pipeline: &Pipeline) {
    pipeline.update_queue_metrics();
}
pub fn release_job_articles(pipeline: &mut Pipeline, id: JobId) {
    pipeline.release_job_articles(id);
}
pub fn set_queued_status(pipeline: &mut Pipeline, id: JobId) {
    let state = pipeline.jobs.get_mut(&id).unwrap();
    state.status = JobStatus::Queued;
    state.download_state = crate::jobs::model::DownloadState::Queued;
}
pub async fn update_job(pipeline: &mut Pipeline, job_id: JobId, update: crate::jobs::JobUpdate) {
    pipeline.report_all_for_test();
    pipeline
        .shared_state
        .queue
        .update_job(job_id, update)
        .unwrap();
    pipeline.apply_queue_intents().await;
}
pub async fn update_history(
    pipeline: &Pipeline,
    id: JobId,
    update: crate::jobs::JobUpdate,
) -> Result<(), crate::SchedulerError> {
    pipeline
        .shared_state
        .queue
        .clone()
        .update_history(id, update)
        .await
}
pub fn install_minimal_job(pipeline: &mut Pipeline, id: JobId, name: &str, dir: PathBuf) {
    pipeline.jobs.insert(id, minimal_job_state(id, name, dir));
}
pub fn schedule_completion_check(pipeline: &mut Pipeline, id: JobId) {
    pipeline.schedule_job_completion_check(id);
}
pub fn pause_runtime(pipeline: &mut Pipeline, id: JobId) -> Result<(), crate::SchedulerError> {
    pipeline.pause_job_runtime(id)
}
pub async fn check_completion(pipeline: &mut Pipeline, id: JobId) {
    pipeline.check_job_completion(id).await;
}
pub fn complete_after_post_processing(pipeline: &mut Pipeline, id: JobId) {
    pipeline.complete_job_after_terminal_post_processing(id);
}
pub fn terminal_status(status: &JobStatus) -> bool {
    is_terminal_status(status)
}

pub async fn take_failed_release(
    pipeline: &mut Pipeline,
) -> (JobId, impl Future<Output = ()> + use<>) {
    let released = pipeline
        .failed_release_rx
        .recv()
        .await
        .expect("the release reports");
    let id = released.job_id;
    let late = pipeline.failed_release_tx.clone();
    (id, async move {
        let _ = late.send(released).await;
    })
}
pub fn releasing_failed_job(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.releasing_failed_jobs.contains(&id)
}
pub async fn drain_pipeline(pipeline: &mut Pipeline) {
    pipeline.drain().await;
}
pub async fn set_categories(
    pipeline: &Pipeline,
    categories: Vec<weaver_server_settings::categories::CategoryConfig>,
) {
    pipeline.config.write().await.categories = categories;
}
pub async fn add_job_for_restore(
    pipeline: &mut Pipeline,
    id: JobId,
    spec: JobSpec,
    path: PathBuf,
    nzb: Vec<u8>,
    options: crate::jobs::AddJobOptions,
) -> Result<(), crate::SchedulerError> {
    pipeline.add_job(id, spec, path, nzb, options).await
}
pub fn mark_torrents_retiring(
    pipeline: &Pipeline,
    ids: &[(JobId, bool)],
) -> Result<(), crate::StateError> {
    use crate::torrents::persistence::TorrentsPersistenceMarkTorrentRetiringInternalQueries;
    pipeline.db.mark_torrents_retiring(ids)
}
pub use crate::torrents::test_fixture::torrent;
