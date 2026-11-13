// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{Change, JobRow, Queue};
use crate::events::model::PipelineEvent;
use crate::jobs::AddJobOptions;
use crate::jobs::model::{DownloadState, PostState, RunState};
use crate::{JobId, JobInfo, JobSpec, JobStatus, SchedulerError};

#[derive(Clone)]
pub(crate) struct AdmissionResources {
    pub db: crate::Database,
    pub intermediate_dir: PathBuf,
    pub complete_dir: PathBuf,
    pub config: crate::settings::SharedConfig,
}

impl AdmissionResources {
    // Every directory a job's staging directory can lie under, as the
    // categories stand now.
    pub(crate) async fn staging_roots(&self) -> Vec<PathBuf> {
        destination_roots(&self.complete_dir, &self.config.read().await.categories)
    }
}

pub(crate) struct PreparedAdd {
    // The job's files, with their article lists let go once its NZB is
    // stored: an add waiting to be installed holds nothing per article.
    pub spec: JobSpec,
    // What the lists held, in the spec's file order.
    pub summaries: crate::jobs::model::FileSummaries,
    pub options: AddJobOptions,
    pub working_dir: PathBuf,
    pub nzb_hash: [u8; 32],
    pub nzb_password_candidates: Option<Vec<crate::jobs::ArchivePasswordCandidate>>,
    pub restart: Option<RestartAdmission>,
}

pub(crate) struct RestartAdmission {
    pub reprocess: bool,
    pub working_dir: PathBuf,
    pub downloaded_bytes: u64,
}

struct Reservation {
    queue: Arc<Queue>,
    id: JobId,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.queue.write(|inner| {
            inner.admissions.remove(&self.id);
            Change::changed(())
        });
    }
}

impl Queue {
    pub(crate) fn restore(
        &self,
        request: crate::jobs::handle::RestoreJobRequest,
    ) -> Result<(), SchedulerError> {
        self.write_intent(|inner| {
            if inner.globals.shutting_down || self.closed.load(std::sync::atomic::Ordering::Acquire)
            {
                return Change::unchanged(Err(SchedulerError::ChannelClosed));
            }
            let id = request.job_id;
            if inner.contains_job(id) || inner.admissions.contains(&id) {
                return Change::unchanged(Err(SchedulerError::JobExists(id)));
            }
            let mut info = initial_info(
                id,
                &request.spec,
                request.restart.as_ref().map(|read| &read.summaries),
                request.job_hash,
                &request.working_dir,
                request.status == JobStatus::Paused,
            );
            info.download_wait_reason = Some("Restoring saved job".into());
            if let Some(read) = &request.restart {
                info.created_at_epoch_ms = read.created_at_epoch_ms;
            }
            let mut row = JobRow::new(info);
            row.report.restoring = true;
            row.intents.restore = Some(request);
            inner.push_job(row);
            Change::changed(Ok(()))
        })
    }

    pub(crate) async fn add(
        self: Arc<Self>,
        id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
        nzb_zstd: Vec<u8>,
        options: AddJobOptions,
        events: tokio::sync::broadcast::Sender<PipelineEvent>,
    ) -> Result<(), SchedulerError> {
        let mut resources = self
            .resources
            .get()
            .cloned()
            .ok_or(SchedulerError::ChannelClosed)?;
        if let Some(paths) = self.read(|inner| inner.globals.runtime_paths.clone()) {
            resources.intermediate_dir = paths.intermediate;
        }
        self.write(|inner| {
            if inner.globals.shutting_down || self.closed.load(std::sync::atomic::Ordering::Acquire)
            {
                return Change::unchanged(Err(SchedulerError::ChannelClosed));
            }
            if inner.history_operations.contains(&id) {
                return Change::unchanged(Err(SchedulerError::Conflict(
                    "a history operation is still running".into(),
                )));
            }
            if inner.contains_job(id) || !inner.admissions.insert(id) {
                return Change::unchanged(Err(SchedulerError::JobExists(id)));
            }
            Change::unchanged(Ok(()))
        })?;
        let reservation = Reservation {
            queue: self.clone(),
            id,
        };
        // Completion belongs to the admission, even if the HTTP request is
        // cancelled after the durable insert has begun.
        tokio::spawn(async move {
            let _reservation = reservation;
            self.wait_for_cleanup(&std::collections::BTreeSet::from([id]))
                .await;
            let prepared = crate::pipeline::Pipeline::prepare_add(
                &resources, id, spec, nzb_path, nzb_zstd, options,
            )
            .await?;
            let job = prepared.info(id);
            let event = PipelineEvent::JobCreated {
                job_id: id,
                name: job.name.clone(),
                total_files: job.total_files,
                total_bytes: job.total_bytes,
            };
            self.write_intent(|inner| {
                let mut row = JobRow::new(job);
                row.admission = Some(prepared);
                inner.push_job(row);
                let _ = events.send(event);
                Change::changed(())
            });
            Ok(())
        })
        .await
        .map_err(|error| SchedulerError::Internal(format!("admission worker failed: {error}")))?
    }

    pub(crate) async fn finish_admissions(&self) {
        let mut revision = self.subscribe();
        while self.read(|inner| !inner.admissions.is_empty()) {
            if revision.changed().await.is_err() {
                break;
            }
        }
    }
}

impl PreparedAdd {
    fn info(&self, id: JobId) -> JobInfo {
        initial_info(
            id,
            &self.spec,
            Some(&self.summaries),
            self.nzb_hash,
            &self.working_dir,
            self.options.initially_paused,
        )
    }
}

// The figures a job waiting its turn shows, from its spec and the summaries
// of the article lists it let go, so the queue answers for a job outside the
// window without installing it or reading its lists back. A file with no
// summary counts as it is listed in the spec. As the pipeline counts them:
// the recovery volumes apart from what a run starts out to bring, an
// article not in hand still to come at its share of its file's bytes, and
// every PAR2 or PAR3 file, index included, with an article still to come.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct LeanFigures {
    pub remaining_bytes: u64,
    pub wanted_bytes: u64,
    pub optional_recovery_bytes: u64,
    pub optional_recovery_downloaded_bytes: u64,
    pub remaining_par_files: u32,
}

impl LeanFigures {
    pub(crate) fn of(
        spec: &JobSpec,
        summaries: Option<&crate::jobs::model::FileSummaries>,
    ) -> Self {
        use weaver_model::files::FileRole;
        let mut figures = Self::default();
        for (index, file) in spec.files.iter().enumerate() {
            let summary = summaries
                .and_then(|summaries| summaries.0.get(index).copied())
                .unwrap_or_else(|| crate::jobs::model::FileSummary::of(file));
            let left = if summary.articles == 0 {
                0
            } else {
                (u128::from(summary.bytes) * u128::from(summary.remaining.min(summary.articles))
                    / u128::from(summary.articles)) as u64
            };
            if file.role.is_recovery() {
                figures.optional_recovery_bytes += summary.bytes;
                figures.optional_recovery_downloaded_bytes += summary.bytes - left;
            } else {
                figures.remaining_bytes += left;
                figures.wanted_bytes += summary.bytes;
            }
            if matches!(file.role, FileRole::Par2 { .. } | FileRole::Par3 { .. })
                && summary.remaining > 0
            {
                figures.remaining_par_files += 1;
            }
        }
        figures
    }
}

pub(super) fn initial_info(
    id: JobId,
    spec: &JobSpec,
    summaries: Option<&crate::jobs::model::FileSummaries>,
    hash: [u8; 32],
    working_dir: &std::path::Path,
    initially_paused: bool,
) -> JobInfo {
    let figures = LeanFigures::of(spec, summaries);
    JobInfo {
        job_id: id,
        job_hash: Some(hash),
        name: spec.name.clone(),
        status: if initially_paused {
            JobStatus::Paused
        } else {
            JobStatus::Queued
        },
        download_state: DownloadState::Queued,
        post_state: PostState::Idle,
        run_state: if initially_paused {
            RunState::Paused
        } else {
            RunState::Active
        },
        finalizing_download: false,
        fetching_repair_data: false,
        // As the pipeline reports it: done is what is wanted less what is
        // still to come.
        progress: if figures.wanted_bytes == 0 {
            0.0
        } else {
            (figures.wanted_bytes - figures.remaining_bytes) as f64 / figures.wanted_bytes as f64
        },
        total_bytes: spec.total_bytes,
        downloaded_bytes: 0,
        optional_recovery_bytes: figures.optional_recovery_bytes,
        optional_recovery_downloaded_bytes: figures.optional_recovery_downloaded_bytes,
        phase_progress: Vec::new(),
        failed_bytes: 0,
        health: 1000,
        terminal_discards: Vec::new(),
        total_files: spec.files.len() as u32,
        completed_files: 0,
        remaining_par_files: figures.remaining_par_files,
        file_remaining_bytes: Vec::new(),
        file_parked_bytes: Vec::new(),
        remaining_bytes: figures.remaining_bytes,
        wanted_bytes: figures.wanted_bytes,
        password: spec.password.clone(),
        category: spec.category.clone(),
        metadata: spec.metadata.clone(),
        output_dir: Some(working_dir.display().to_string()),
        server_attribution: Vec::new(),
        error: None,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
    }
}

// The directory a job's finished files are delivered under: its category's
// folder, else the complete directory. A category whose folder cannot be
// resolved stages under the complete directory; its move then says why it
// cannot deliver.
pub(crate) fn destination_root(
    complete_dir: &Path,
    categories: &[crate::categories::CategoryConfig],
    category: Option<&str>,
) -> PathBuf {
    crate::categories::completion_parent(complete_dir, categories, category)
        .unwrap_or_else(|_| complete_dir.to_path_buf())
}

// Where a job writes the files it delivers when its intermediate directory
// and its destination lie on different devices: its staging directory on the
// destination's device, so finalize renames them into place there instead of
// copying every byte across. On one device nothing is staged. Whether they
// share a device is the disk owner's answer,
// [`crate::operations::StorageCapacity::same_filesystem`].
pub(crate) fn staging_dir_on(
    one_device: bool,
    destination_root: &Path,
    job_id: JobId,
) -> Option<PathBuf> {
    (!one_device).then(|| staging_dir_for(destination_root, job_id))
}

// [`staging_dir_on`] with the devices read on the spot, as the choice was
// made before the disk owner answered it: what a test compares the owner's
// choice with.
#[cfg(test)]
pub(crate) fn delivery_staging_dir(
    intermediate_dir: &Path,
    destination_root: &Path,
    job_id: JobId,
) -> Option<PathBuf> {
    staging_dir_on(
        crate::runtime::fs::same_device(intermediate_dir, destination_root),
        destination_root,
        job_id,
    )
}

pub(crate) fn staging_dir_for(destination_root: &Path, job_id: JobId) -> PathBuf {
    destination_root
        .join(crate::jobs::working_dir::STAGING_DIR_NAME)
        .join(job_id.0.to_string())
}

// Make the job's staging directory and mark it the job's, as its working
// directory is. Nothing under the destination is reached through a symbolic
// link.
pub(crate) fn make_staging_dir(staging_dir: &Path, job_id: JobId) -> std::io::Result<()> {
    crate::jobs::working_dir::make_job_staging_dir(staging_dir, job_id)
}

// Every directory a job's staging directory can lie under: the complete
// directory and each category's folder.
pub(crate) fn destination_roots(
    complete_dir: &Path,
    categories: &[crate::categories::CategoryConfig],
) -> Vec<PathBuf> {
    let mut seen = std::collections::HashSet::new();
    std::iter::once(complete_dir.to_path_buf())
        .chain(categories.iter().map(|category| {
            destination_root(
                complete_dir,
                std::slice::from_ref(category),
                Some(&category.name),
            )
        }))
        .filter(|root| seen.insert(root.clone()))
        .collect()
}

#[cfg(test)]
mod staging_tests {
    use super::*;

    #[test]
    fn one_device_stages_nothing() {
        let root = tempfile::tempdir().expect("temp dir");
        let intermediate = root.path().join("intermediate");
        let complete = root.path().join("complete");
        assert_eq!(
            delivery_staging_dir(&intermediate, &complete, JobId(7)),
            None
        );
    }

    #[test]
    fn two_devices_stage_under_the_destination() {
        let root = tempfile::tempdir().expect("temp dir");
        let intermediate = root.path().join("intermediate");
        let complete = root.path().join("complete");
        crate::runtime::fs::test_devices::assign(&intermediate, 1);
        crate::runtime::fs::test_devices::assign(&complete, 2);
        assert_eq!(
            delivery_staging_dir(&intermediate, &complete, JobId(7)),
            Some(complete.join(".weaver-staging").join("7"))
        );
    }

    fn category(name: &str, dest_dir: Option<&Path>) -> crate::categories::CategoryConfig {
        crate::categories::CategoryConfig {
            id: 1,
            name: name.to_string(),
            dest_dir: dest_dir.map(|dir| dir.display().to_string()),
            aliases: String::new(),
        }
    }

    // A category with its own folder on another device stages there, never
    // on the complete directory's device; one on the intermediate device
    // stages nothing.
    #[test]
    fn a_category_folder_decides_where_the_job_stages() {
        let root = tempfile::tempdir().expect("temp dir");
        let intermediate = root.path().join("intermediate");
        let complete = root.path().join("complete");
        let movies = root.path().join("movies");
        let beside = root.path().join("beside");
        crate::runtime::fs::test_devices::assign(&intermediate, 1);
        crate::runtime::fs::test_devices::assign(&complete, 2);
        crate::runtime::fs::test_devices::assign(&movies, 3);
        crate::runtime::fs::test_devices::assign(&beside, 1);
        let categories = vec![
            category("movies", Some(&movies)),
            category("beside", Some(&beside)),
            category("tv", None),
        ];
        let stage = |name: Option<&str>| {
            delivery_staging_dir(
                &intermediate,
                &destination_root(&complete, &categories, name),
                JobId(7),
            )
        };
        assert_eq!(
            stage(Some("movies")),
            Some(movies.join(".weaver-staging").join("7"))
        );
        assert_eq!(stage(Some("beside")), None);
        assert_eq!(
            stage(Some("tv")),
            Some(complete.join("tv").join(".weaver-staging").join("7"))
        );
        assert_eq!(
            stage(None),
            Some(complete.join(".weaver-staging").join("7"))
        );
        // A category that cannot be resolved stages under the complete
        // directory.
        assert_eq!(
            stage(Some("../outside")),
            Some(complete.join(".weaver-staging").join("7"))
        );
        assert_eq!(
            destination_roots(&complete, &categories),
            vec![complete.clone(), movies, beside, complete.join("tv")]
        );
    }

    #[test]
    fn a_staging_directory_is_made_and_marked_the_job_s() {
        let complete = tempfile::tempdir().expect("temp dir");
        let staging = staging_dir_for(complete.path(), JobId(9));
        make_staging_dir(&staging, JobId(9)).expect("staging");
        assert!(crate::jobs::working_dir::is_weaver_owned_working_dir(
            &staging
        ));
        assert!(
            crate::jobs::working_dir::is_job_working_dir(
                &complete.path().join(".weaver-staging"),
                &staging,
                JobId(9)
            )
            .expect("marker")
        );
    }
}

#[cfg(test)]
mod lean_figure_tests {
    use super::*;
    use crate::jobs::model::{FileSpec, FileSummaries, FileSummary, MessageIds, SegmentSpec};
    use weaver_model::files::FileRole;

    fn file(name: &str, sizes: &[u32]) -> FileSpec {
        FileSpec {
            filename: name.to_string(),
            role: FileRole::from_filename(name),
            groups: Vec::new(),
            posted_at_epoch: None,
            segments: (0u32..)
                .zip(sizes)
                .map(|(ordinal, &bytes)| SegmentSpec {
                    ordinal,
                    article_number: ordinal + 1,
                    bytes,
                    message_ids: MessageIds::new(vec![format!("{name}-{ordinal}")]),
                })
                .collect(),
        }
    }

    fn spec() -> JobSpec {
        JobSpec {
            name: "post".to_string(),
            password: None,
            files: vec![
                file("post.part01.rar", &[100, 100]),
                file("post.par2", &[10]),
                file("post.vol00+02.par2", &[50, 50]),
                file("post.vol00+04.par3", &[40]),
                file("post.part01.rev", &[30]),
            ],
            total_bytes: 430,
            category: None,
            metadata: Vec::new(),
        }
    }

    // A job that never ran shows every PAR2 and PAR3 file to come, its
    // recovery volumes apart from what it is out to bring, and the same
    // wanted bytes the queue showed before.
    #[test]
    fn a_job_that_never_ran_counts_its_recovery_from_the_spec() {
        let spec = spec();
        let figures = LeanFigures::of(&spec, None);
        let summaries = FileSummaries(spec.files.iter().map(FileSummary::of).collect());
        assert_eq!(figures, LeanFigures::of(&spec, Some(&summaries)));
        assert_eq!(figures.wanted_bytes, spec.wanted_bytes());
        assert_eq!(figures.wanted_bytes, summaries.wanted_bytes(&spec));
        assert_eq!(
            figures,
            LeanFigures {
                remaining_bytes: 240,
                wanted_bytes: 240,
                optional_recovery_bytes: 140,
                optional_recovery_downloaded_bytes: 0,
                remaining_par_files: 3,
            }
        );
    }

    // A job waiting its turn after it ran shows what its summaries hold in
    // hand, without its article lists.
    #[test]
    fn a_job_that_ran_counts_what_its_summaries_hold_in_hand() {
        let mut spec = spec();
        let in_hand = |file: &FileSpec, held: u32| FileSummary {
            remaining: file.segments.len() as u32 - held,
            ..FileSummary::of(file)
        };
        let summaries = FileSummaries(vec![
            in_hand(&spec.files[0], 1),
            in_hand(&spec.files[1], 1),
            in_hand(&spec.files[2], 1),
            in_hand(&spec.files[3], 0),
            in_hand(&spec.files[4], 1),
        ]);
        spec.drop_article_lists();
        assert_eq!(
            LeanFigures::of(&spec, Some(&summaries)),
            LeanFigures {
                remaining_bytes: 100,
                wanted_bytes: 240,
                optional_recovery_bytes: 140,
                optional_recovery_downloaded_bytes: 50,
                remaining_par_files: 2,
            }
        );
        let info = initial_info(
            JobId(1),
            &spec,
            Some(&summaries),
            [0; 32],
            Path::new("/work"),
            true,
        );
        assert_eq!(info.optional_recovery_bytes, 140);
        assert_eq!(info.optional_recovery_downloaded_bytes, 50);
        assert_eq!(info.remaining_par_files, 2);
        assert_eq!((info.remaining_bytes, info.wanted_bytes), (100, 240));
        assert!((info.progress - 140.0 / 240.0).abs() < 1e-9);
    }
}
