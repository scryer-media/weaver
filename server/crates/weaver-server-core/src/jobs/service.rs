// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use tracing::{debug, error, info, warn};

use crate::events::model::PipelineEvent;
use crate::history::timeline::JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER;
use crate::jobs::assembly::{DetectedArchiveIdentity, JobAssembly};
use crate::jobs::ids::{JobId, MessageId, NzbFileId, SegmentId};
use crate::jobs::model::{FileSummaries, FileSummary, JobArticles, JobSpec, JobState, JobStatus};
use crate::jobs::record::{ActiveFileIdentity, FileIdentitySource};
use crate::jobs::working_dir::{claim_working_dir, stamp_working_dir};
use crate::pipeline::Pipeline;
use crate::{DownloadQueue, DownloadWork, RestoreJobRequest};

impl Pipeline {
    async fn restore_has_open_download_finalization(&self, job_id: JobId) -> bool {
        let events = match self
            .db_blocking(move |db| db.get_job_events(job_id.0))
            .await
        {
            Ok(events) => events,
            Err(error) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to load job events while restoring download finalization"
                );
                return false;
            }
        };

        let mut finalization_open = false;
        for event in events {
            match event.kind.as_str() {
                "JobCreated" | "JobCompleted" | "JobFailed" | "JobCancelled" => {
                    finalization_open = false;
                }
                "DownloadFinished"
                    if event.file_id.as_deref() == Some(JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER) =>
                {
                    finalization_open = true;
                }
                "DownloadPipelineDrained" => {
                    finalization_open = false;
                }
                _ => {}
            }
        }

        finalization_open
    }

    async fn restore_download_finalization_runtime(&mut self, job_id: JobId) -> bool {
        if !self.restore_has_open_download_finalization(job_id).await {
            self.jobs_finalizing_download.remove(&job_id);
            return false;
        }

        self.jobs_finalizing_download.insert(job_id);
        self.emit_job_event(PipelineEvent::DownloadFinished {
            job_id,
            finalization_pending: true,
        });
        true
    }

    fn normalize_restored_download_state(
        download_state: crate::jobs::model::DownloadState,
        nothing_queued: bool,
    ) -> crate::jobs::model::DownloadState {
        if !matches!(download_state, crate::jobs::model::DownloadState::Checking) {
            return download_state;
        }

        if nothing_queued {
            crate::jobs::model::DownloadState::Complete
        } else {
            crate::jobs::model::DownloadState::Queued
        }
    }

    fn restored_download_state_from_status(
        status: &JobStatus,
        nothing_queued: bool,
    ) -> crate::jobs::model::DownloadState {
        let download_state = match status {
            JobStatus::Queued => crate::jobs::model::DownloadState::Queued,
            JobStatus::Checking => crate::jobs::model::DownloadState::Checking,
            JobStatus::Complete | JobStatus::Partial | JobStatus::Moving => {
                crate::jobs::model::DownloadState::Complete
            }
            JobStatus::Failed { .. } => crate::jobs::model::DownloadState::Failed,
            _ => {
                if nothing_queued {
                    crate::jobs::model::DownloadState::Complete
                } else {
                    crate::jobs::model::DownloadState::Downloading
                }
            }
        };
        Self::normalize_restored_download_state(download_state, nothing_queued)
    }

    fn normalize_restored_status(status: JobStatus, nothing_queued: bool) -> JobStatus {
        if !matches!(status, JobStatus::Checking) {
            return status;
        }

        if nothing_queued {
            JobStatus::Complete
        } else {
            JobStatus::Queued
        }
    }

    fn normalize_paused_resume_status(
        paused_resume_status: Option<JobStatus>,
    ) -> Option<JobStatus> {
        paused_resume_status.map(|status| match status {
            JobStatus::Queued => JobStatus::Queued,
            _ => JobStatus::Downloading,
        })
    }

    async fn persist_file_identities(
        &self,
        job_id: JobId,
        identities: &HashMap<u32, ActiveFileIdentity>,
    ) {
        if identities.is_empty() {
            return;
        }
        let identities = identities.values().cloned().collect::<Vec<_>>();
        if let Err(error) = self
            .db_blocking(move |db| db.save_file_identities(job_id, &identities))
            .await
        {
            warn!(
                job_id = job_id.0,
                error = %error,
                "failed to persist file identities"
            );
        }
    }

    fn declared_archive_identity(
        filename: &str,
        role: &weaver_model::files::FileRole,
    ) -> Option<DetectedArchiveIdentity> {
        let set_name = weaver_model::files::archive_base_name(filename, role)?;
        match role {
            weaver_model::files::FileRole::RarVolume { volume_number } => {
                Some(DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name,
                    volume_index: Some(*volume_number),
                })
            }
            weaver_model::files::FileRole::SevenZipArchive => Some(DetectedArchiveIdentity {
                kind: crate::jobs::assembly::DetectedArchiveKind::SevenZipSingle,
                set_name,
                volume_index: None,
            }),
            weaver_model::files::FileRole::SevenZipSplit { number } => {
                Some(DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::SevenZipSplit,
                    set_name,
                    volume_index: Some(*number),
                })
            }
            _ => None,
        }
    }

    fn default_file_identity(
        file_index: u32,
        filename: &str,
        role: &weaver_model::files::FileRole,
        detected: Option<&DetectedArchiveIdentity>,
    ) -> ActiveFileIdentity {
        ActiveFileIdentity {
            file_index,
            source_filename: filename.to_string(),
            current_filename: filename.to_string(),
            canonical_filename: None,
            classification: detected
                .cloned()
                .or_else(|| Self::declared_archive_identity(filename, role)),
            classification_source: if detected.is_some() {
                FileIdentitySource::Probe
            } else {
                FileIdentitySource::Declared
            },
        }
    }

    fn build_initial_file_identities(
        spec: &JobSpec,
        detected_archives: &HashMap<u32, DetectedArchiveIdentity>,
    ) -> HashMap<u32, ActiveFileIdentity> {
        spec.files
            .iter()
            .enumerate()
            .map(|(file_index, file_spec)| {
                let file_index = file_index as u32;
                (
                    file_index,
                    Self::default_file_identity(
                        file_index,
                        &file_spec.filename,
                        &file_spec.role,
                        detected_archives.get(&file_index),
                    ),
                )
            })
            .collect()
    }

    // Charge what a job's articles hold, at the one moment they are built:
    // when a queued job is first handed work, or when a job that was already
    // running before a restart is restored with them. Held until the job
    // leaves the pipeline, and never refused: the ledger can never hold back a
    // job that is downloading.
    pub(crate) fn charge_job_article_memory(&mut self, job_id: JobId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let permit = self
            .process_memory_budget
            .charge_retained(state.spec.scheduling_memory_estimate());
        self.job_article_memory.insert(job_id, permit);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) async fn add_job(
        &mut self,
        job_id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
        nzb_zstd: Vec<u8>,
        options: crate::jobs::AddJobOptions,
    ) -> Result<(), crate::SchedulerError> {
        if self.jobs.contains_key(&job_id) {
            return Err(crate::SchedulerError::JobExists(job_id));
        }
        self.shared_state
            .queue
            .clone()
            .add(
                job_id,
                spec,
                nzb_path,
                nzb_zstd,
                options,
                self.event_tx.clone(),
            )
            .await?;
        self.apply_queue_intents().await;
        Ok(())
    }

    pub(crate) fn admission_resources(&self) -> crate::pipeline::queue::AdmissionResources {
        crate::pipeline::queue::AdmissionResources {
            db: self.db.clone(),
            intermediate_dir: self.intermediate_dir.clone(),
            complete_dir: self.complete_dir.clone(),
        }
    }

    pub(crate) async fn prepare_add(
        resources: &crate::pipeline::queue::AdmissionResources,
        job_id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
        nzb_zstd: Vec<u8>,
        options: crate::jobs::AddJobOptions,
    ) -> Result<crate::pipeline::queue::PreparedAdd, crate::SchedulerError> {
        Self::prepare_admission(resources, job_id, spec, nzb_path, nzb_zstd, options, None).await
    }

    pub(crate) async fn prepare_admission(
        resources: &crate::pipeline::queue::AdmissionResources,
        job_id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
        nzb_zstd: Vec<u8>,
        options: crate::jobs::AddJobOptions,
        restart: Option<crate::pipeline::queue::RestartAdmission>,
    ) -> Result<crate::pipeline::queue::PreparedAdd, crate::SchedulerError> {
        let started = std::time::Instant::now();
        let mut stage_start = started;
        if let Some(generation) = options.semantic_materialization_generation {
            let db = resources.db.clone();
            let current = tokio::task::spawn_blocking(move || {
                db.semantic_materialization_is_current(job_id, generation)
            })
            .await
            .map_err(|error| {
                crate::SchedulerError::Internal(format!(
                    "semantic materialization check worker panicked: {error}"
                ))
            })??;
            if !current {
                return Err(crate::SchedulerError::SemanticSuperseded);
            }
        }
        if let Some(generation) = options.semantic_promotion_generation {
            let db = resources.db.clone();
            let current = tokio::task::spawn_blocking(move || {
                db.semantic_promotion_materialization_is_current(job_id, generation)
            })
            .await
            .map_err(|error| {
                crate::SchedulerError::Internal(format!(
                    "semantic promotion check worker panicked: {error}"
                ))
            })??;
            if !current {
                return Err(crate::SchedulerError::SemanticSuperseded);
            }
        }

        let reprocess = restart.as_ref().is_some_and(|restart| restart.reprocess);
        if let Some(restart) = restart.as_ref().filter(|restart| !restart.reprocess) {
            crate::pipeline::queue::clear_restart_artifacts(
                resources,
                job_id,
                &restart.working_dir,
            )
            .await?;
        }
        let (working_dir, directory_existed) =
            match restart.as_ref().filter(|restart| restart.reprocess) {
                Some(restart) => {
                    let working_dir = restart.working_dir.clone();
                    let directory_existed = tokio::fs::try_exists(&working_dir).await?;
                    tokio::fs::create_dir_all(&working_dir)
                        .await
                        .map_err(crate::SchedulerError::Io)?;
                    (working_dir, directory_existed)
                }
                None => {
                    let intermediate_dir = resources.intermediate_dir.clone();
                    let name = spec.name.clone();
                    let claimed = tokio::task::spawn_blocking(move || {
                        claim_working_dir(&intermediate_dir, job_id, &name)
                    })
                    .await
                    .map_err(|error| {
                        crate::SchedulerError::Internal(format!(
                            "working directory claim worker panicked: {error}"
                        ))
                    })?
                    .map_err(crate::SchedulerError::Io)?;
                    (claimed.path, claimed.existed)
                }
            };
        if working_dir.starts_with(&resources.intermediate_dir) {
            stamp_working_dir(&resources.intermediate_dir, &working_dir, job_id)
                .await
                .map_err(crate::SchedulerError::Io)?;
        }
        crate::runtime::perf_probe::record(
            "pipeline.add_job.working_dir_ready",
            stage_start.elapsed(),
        );
        debug!(
            job_id = job_id.0,
            working_dir = %working_dir.display(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            stage = "working_dir_ready",
            "pipeline add_job stage"
        );

        stage_start = std::time::Instant::now();
        let nzb_hash = crate::ingest::hash_persisted_nzb_bytes(&nzb_zstd);
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let file_identities = Self::build_initial_file_identities(&spec, &HashMap::new());
        let initial_file_identities = file_identities.values().cloned().collect::<Vec<_>>();
        let (initial_status, initial_download_state, initial_post_state, initial_run_state) =
            if reprocess {
                (
                    JobStatus::Downloading,
                    crate::jobs::model::DownloadState::Downloading,
                    crate::jobs::model::PostState::Idle,
                    crate::jobs::model::RunState::Active,
                )
            } else if options.initially_paused {
                (
                    JobStatus::Paused,
                    crate::jobs::model::DownloadState::Queued,
                    crate::jobs::model::PostState::Idle,
                    crate::jobs::model::RunState::Paused,
                )
            } else {
                (
                    JobStatus::Queued,
                    crate::jobs::model::DownloadState::Queued,
                    crate::jobs::model::PostState::Idle,
                    crate::jobs::model::RunState::Active,
                )
            };
        crate::runtime::perf_probe::record(
            "pipeline.add_job.build_runtime_inputs",
            stage_start.elapsed(),
        );

        stage_start = std::time::Instant::now();
        let active_job = crate::ActiveJob {
            job_id,
            nzb_hash,
            nzb_path,
            nzb_zstd,
            output_dir: working_dir.clone(),
            created_at,
            category: spec.category.clone(),
            metadata: spec.metadata.clone(),
            status: initial_status.persisted_status(),
            download_state: initial_download_state.as_str(),
            post_state: initial_post_state.as_str(),
            run_state: initial_run_state.as_str(),
            paused_resume_status: options
                .initially_paused
                .then_some(JobStatus::Queued.persisted_status()),
            paused_resume_download_state: options
                .initially_paused
                .then_some(crate::jobs::model::DownloadState::Queued.as_str()),
            paused_resume_post_state: options
                .initially_paused
                .then_some(crate::jobs::model::PostState::Idle.as_str()),
            password_override: Some(spec.password.clone().unwrap_or_default()),
            supplied_password_candidates: options.supplied_password_candidates.clone(),
        };
        // Biggest single write on the add-job path: keep it off the
        // orchestrator loop, but never create an in-memory job without durable
        // active-job state. SCORE candidates additionally consume their
        // admission generation in this same transaction, closing the window
        // between a higher-score supersession and scheduler materialization.
        let db = resources.db.clone();
        let semantic_materialization_generation = options.semantic_materialization_generation;
        let semantic_promotion_generation = options.semantic_promotion_generation;
        let is_restart = restart.is_some();
        let (persisted, nzb_password_candidates) = tokio::task::spawn_blocking(move || {
            let persisted = db.materialize_admission(
                &active_job,
                &initial_file_identities,
                semantic_materialization_generation,
                semantic_promotion_generation,
                is_restart,
            );
            // Derived here, once, from the bytes just persisted, so no archive
            // file has to reload and re-parse the NZB to learn them. A parse
            // failure leaves the harvest to read the row itself, as before.
            let nzb_password_candidates = crate::pipeline::persisted_nzb_password_candidates(
                &active_job.nzb_path,
                &active_job.nzb_zstd,
            )
            .ok()
            .map(|nzb| {
                crate::pipeline::job_carried_password_candidates(
                    &active_job.supplied_password_candidates,
                    nzb,
                )
            });
            (persisted, nzb_password_candidates)
        })
        .await
        .map_err(|error| {
            crate::SchedulerError::Internal(format!(
                "create active job persistence worker panicked: {error}"
            ))
        })?;
        match persisted {
            Err(error) => {
                if !directory_existed
                    && let Err(cleanup_error) = tokio::fs::remove_dir_all(&working_dir).await
                    && cleanup_error.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(job_id = job_id.0, error = %cleanup_error, "failed to clean working directory after active job persistence failure");
                }
                return Err(error.into());
            }
            Ok(false) => {
                if !directory_existed
                    && let Err(cleanup_error) = tokio::fs::remove_dir_all(&working_dir).await
                    && cleanup_error.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(job_id = job_id.0, error = %cleanup_error, "failed to clean superseded job working directory");
                }
                return Err(crate::SchedulerError::SemanticSuperseded);
            }
            Ok(true) => {}
        }
        crate::runtime::perf_probe::record(
            "pipeline.add_job.persist_active_job",
            stage_start.elapsed(),
        );
        debug!(
            job_id = job_id.0,
            elapsed_ms = started.elapsed().as_millis() as u64,
            stage = "active_job_persisted",
            "pipeline add_job stage"
        );

        Ok(crate::pipeline::queue::PreparedAdd {
            spec,
            options,
            working_dir,
            nzb_hash,
            nzb_password_candidates,
            restart,
        })
    }

    pub(crate) fn install_prepared_add(
        &mut self,
        job_id: JobId,
        prepared: crate::pipeline::queue::PreparedAdd,
    ) {
        let crate::pipeline::queue::PreparedAdd {
            spec,
            options,
            working_dir,
            nzb_hash,
            nzb_password_candidates,
            restart,
        } = prepared;
        let started = std::time::Instant::now();
        let stage_start = started;
        self.queue_scripts_completed.remove(&job_id);
        let reprocess = restart.as_ref().is_some_and(|restart| restart.reprocess);
        let mut spec = spec;
        let file_identities = Self::build_initial_file_identities(&spec, &HashMap::new());
        let par2_bytes = spec.par2_bytes();
        // The job waits its turn holding nothing per article. Its article
        // lists are read back from the stored NZB when it is first handed
        // work; a reprocess fetches every article again then, as it has no
        // checkpoint to say where any was placed.
        let summaries = spec.drop_article_lists();
        let queue_depth: u64 = summaries
            .0
            .iter()
            .map(|file| u64::from(file.articles))
            .sum();
        let initial_status = if reprocess {
            Self::reprocess_status(queue_depth)
        } else if options.initially_paused {
            JobStatus::Paused
        } else {
            JobStatus::Queued
        };
        let initial_download_state = crate::jobs::model::DownloadState::Queued;
        let initial_post_state = crate::jobs::model::PostState::Idle;
        let initial_run_state = if options.initially_paused {
            crate::jobs::model::RunState::Paused
        } else {
            crate::jobs::model::RunState::Active
        };
        // Resolve the category's shared decoded-byte counter once, here, so the
        // per-segment accounting site does a single `Relaxed` `fetch_add`
        // through an already-resolved pointer instead of a map lookup.
        let category_bytes = self
            .metrics
            .job_lifecycle
            .category_bytes_counter(spec.category.as_deref());
        let mut state = JobState {
            job_id,
            job_hash: nzb_hash,
            spec,
            status: initial_status,
            download_state: initial_download_state,
            post_state: initial_post_state,
            run_state: initial_run_state,
            articles: JobArticles::Queued(summaries),
            created_at: std::time::Instant::now(),
            created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: options.initially_paused.then_some(JobStatus::Queued),
            paused_resume_download_state: options
                .initially_paused
                .then_some(crate::jobs::model::DownloadState::Queued),
            paused_resume_post_state: options
                .initially_paused
                .then_some(crate::jobs::model::PostState::Idle),
            failure_error: None,
            working_dir: working_dir.clone(),
            downloaded_bytes: restart
                .as_ref()
                .filter(|restart| restart.reprocess)
                .map_or(0, |restart| restart.downloaded_bytes),
            restored_download_floor_bytes: 0,
            downloaded_wire_bytes: 0,
            server_attribution: Default::default(),
            failed_bytes: 0,
            par2_bytes,
            support_facts: Default::default(),
            detected_archives: HashMap::new(),
            file_identities,
            staging_dir: None,
            category_bytes: Some(category_bytes),
            nzb_password_candidates: nzb_password_candidates
                .map(std::sync::OnceLock::from)
                .unwrap_or_default(),
        };
        state.refresh_runtime_lanes_from_status();
        self.jobs.insert(job_id, state);
        self.note_download_activity(job_id);
        if !self.job_order.contains(&job_id) {
            self.job_order.push(job_id);
        }
        if !reprocess {
            if self.nzb_admitted.contains(&job_id) {
                self.raise_added_script_event(job_id);
            } else {
                self.nzb_added_scripts_pending.insert(job_id);
            }
        }

        crate::runtime::perf_probe::record(
            "pipeline.add_job.runtime_state_inserted",
            stage_start.elapsed(),
        );
        info!(
            job_id = job_id.0,
            queue_depth,
            working_dir = %working_dir.display(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            stage = "runtime_state_inserted",
            "job added"
        );
    }

    // The job's assembly and its download queue. Every article starts at the
    // one neutral priority, in posted order: the job's root is the only
    // ranker, and its classes raise what it wants and park what its objects
    // defer. The articles in `skip` are already in hand.
    pub(crate) fn build_job_assembly(
        job_id: JobId,
        spec: &JobSpec,
        skip: &HashSet<SegmentId>,
    ) -> (JobAssembly, DownloadQueue) {
        let mut assembly = JobAssembly::new(job_id);
        let mut download_queue = DownloadQueue::new();

        for (file_index, file_spec) in spec.files.iter().enumerate() {
            let file_id = NzbFileId {
                job_id,
                file_index: file_index as u32,
            };

            let segment_sizes: Vec<u32> = file_spec.segments.iter().map(|s| s.bytes).collect();

            let mut file_assembly = crate::jobs::assembly::FileAssembly::new(
                file_id,
                file_spec.filename.clone(),
                file_spec.role.clone(),
                segment_sizes,
            );

            // One shared group list per file; every segment's work item holds
            // a reference to it rather than its own copy.
            let groups: std::sync::Arc<[String]> =
                std::sync::Arc::from(file_spec.groups.as_slice());
            for seg in &file_spec.segments {
                let segment_id = SegmentId {
                    file_id,
                    segment_number: seg.ordinal,
                };
                if skip.contains(&segment_id) {
                    let _ = file_assembly.commit_segment(seg.ordinal, seg.bytes);
                } else {
                    download_queue.push(DownloadWork {
                        counters: None,
                        segment_id,
                        message_id: MessageId::new(seg.message_ids.in_use()),
                        groups: std::sync::Arc::clone(&groups),
                        priority: crate::pipeline::graph::NEUTRAL_PRIORITY,
                        byte_estimate: seg.bytes,
                        retry_count: 0,
                        is_recovery: false,
                        completion_critical: false,
                        exclude_servers: vec![],
                        avoid_server: None,
                    });
                }
            }

            file_assembly.note_restored_ordinals();
            assembly.add_file(file_assembly);
        }

        (assembly, download_queue)
    }

    #[cfg(test)]
    pub(crate) async fn reprocess_job(
        &mut self,
        job_id: JobId,
    ) -> Result<(), crate::SchedulerError> {
        self.reprocess_job_with_password(job_id, None).await
    }

    #[cfg(test)]
    pub(crate) async fn reprocess_job_with_password(
        &mut self,
        job_id: JobId,
        password: Option<String>,
    ) -> Result<(), crate::SchedulerError> {
        self.restart_through_control_plane(job_id, password, true)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn redownload_job(
        &mut self,
        job_id: JobId,
    ) -> Result<(), crate::SchedulerError> {
        self.redownload_job_with_password(job_id, None).await
    }

    #[cfg(test)]
    pub(crate) async fn redownload_job_with_password(
        &mut self,
        job_id: JobId,
        password: Option<String>,
    ) -> Result<(), crate::SchedulerError> {
        self.restart_through_control_plane(job_id, password, false)
            .await
    }

    #[cfg(test)]
    async fn restart_through_control_plane(
        &mut self,
        job_id: JobId,
        password: Option<String>,
        reprocess: bool,
    ) -> Result<(), crate::SchedulerError> {
        if let Some(state) = self.jobs.get(&job_id) {
            if !crate::pipeline::is_terminal_status(&state.status) {
                return Err(crate::SchedulerError::Conflict(
                    "job is not complete or failed".into(),
                ));
            }
            let mut changes = self.shared_state.queue.subscribe();
            self.record_job_history(job_id, None);
            while !self
                .shared_state
                .queue
                .history()
                .iter()
                .any(|job| job.job_id == job_id)
            {
                changes
                    .changed()
                    .await
                    .map_err(|_| crate::SchedulerError::ChannelClosed)?;
            }
        }
        self.shared_state
            .queue
            .clone()
            .restart(job_id, password, reprocess, self.event_tx.clone())
            .await?;
        self.apply_queue_intents().await;
        Ok(())
    }

    // A reprocess with nothing to fetch is downloading, so the completion
    // check moves it on; one with work is queued until its first dispatch.
    fn reprocess_status(queued_articles: u64) -> JobStatus {
        if queued_articles == 0 {
            JobStatus::Downloading
        } else {
            JobStatus::Queued
        }
    }

    pub(crate) async fn install_restored_job(
        &mut self,
        mut request: RestoreJobRequest,
    ) -> Result<(), crate::SchedulerError> {
        if self.jobs.contains_key(&request.job_id) {
            return Err(crate::SchedulerError::JobExists(request.job_id));
        }
        // The status the job goes back to when unpaused: the stored one, else
        // the one its stored resume lanes name, else the one its lanes at
        // shutdown name. Read here once, so the job installs with it whether
        // its restore succeeds or blocks.
        request.paused_resume_status = Self::normalize_paused_resume_status(
            request.paused_resume_status.take().or_else(|| {
                request
                    .paused_resume_download_state
                    .zip(request.paused_resume_post_state)
                    .or(request.download_state.zip(request.post_state))
                    .map(|(download_state, post_state)| {
                        crate::jobs::model::derive_legacy_job_status(
                            download_state,
                            post_state,
                            crate::jobs::model::RunState::Active,
                            None,
                        )
                    })
            }),
        );
        let root = self.intermediate_dir.clone();
        let path = request.working_dir.clone();
        let id = request.job_id;
        let db = self.db.clone();
        match tokio::task::spawn_blocking(move || {
            crate::jobs::working_dir::recover_working_dir_rename(&db, &root, &path, id)
        })
        .await
        {
            Ok(Ok(path)) => request.working_dir = path,
            result => {
                let error = match result {
                    Ok(Err(error)) => error.to_string(),
                    Err(error) => error.to_string(),
                    Ok(Ok(_)) => unreachable!(),
                };
                self.install_blocked_restore(request, error).await;
                return Ok(());
            }
        }
        let original = request.clone();
        match self.restore_job_inner(request).await {
            Ok(()) => {
                self.blocked_restores.remove(&original.job_id);
                self.persist_active_runtime(original.job_id);
            }
            Err(error) => {
                self.install_blocked_restore(original, error.to_string())
                    .await;
            }
        }
        // A blocked restore is a retained queue entry and must be published.
        Ok(())
    }

    async fn install_blocked_restore(&mut self, request: RestoreJobRequest, error: String) {
        let job_id = request.job_id;
        let server_attribution = self
            .db_blocking(move |db| db.load_active_server_attribution(job_id))
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(%error, job_id = job_id.0, "could not load blocked job attribution");
                Default::default()
            });
        let support_facts = self
            .db_blocking(move |db| db.load_job_support_facts(job_id))
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(%error, job_id = job_id.0, "could not load blocked job support facts");
                Default::default()
            });
        let identities = if request.file_identities.is_empty() {
            Self::build_initial_file_identities(&request.spec, &request.detected_archives)
        } else {
            request.file_identities.clone()
        };
        let mut spec = request.spec.clone();
        let summaries = spec.drop_article_lists();
        let state = JobState {
            job_id,
            job_hash: request.job_hash,
            spec,
            status: JobStatus::Paused,
            download_state: crate::jobs::model::DownloadState::Queued,
            post_state: crate::jobs::model::PostState::Idle,
            run_state: crate::jobs::model::RunState::Paused,
            articles: JobArticles::Queued(summaries),
            created_at: std::time::Instant::now(),
            created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
            queued_repair_at_epoch_ms: request.queued_repair_at_epoch_ms,
            queued_extract_at_epoch_ms: request.queued_extract_at_epoch_ms,
            paused_resume_status: request.paused_resume_status.clone(),
            paused_resume_download_state: request.paused_resume_download_state,
            paused_resume_post_state: request.paused_resume_post_state,
            failure_error: Some(format!("Checkpoint could not be restored: {error}")),
            working_dir: request.working_dir.clone(),
            downloaded_bytes: 0,
            restored_download_floor_bytes: 0,
            downloaded_wire_bytes: 0,
            server_attribution,
            failed_bytes: 0,
            par2_bytes: request.spec.par2_bytes(),
            support_facts,
            detected_archives: request.detected_archives.clone(),
            file_identities: identities,
            staging_dir: None,
            category_bytes: None,
            nzb_password_candidates: Default::default(),
        };
        let message = state.failure_error.clone().unwrap();
        let queued_repair = state.queued_repair_at_epoch_ms;
        let queued_extract = state.queued_extract_at_epoch_ms;
        let resume_status = state
            .paused_resume_status
            .as_ref()
            .map(Self::persist_active_status_for);
        let resume_download = state
            .paused_resume_download_state
            .map(|value| value.as_str());
        let resume_post = state.paused_resume_post_state.map(|value| value.as_str());
        self.jobs.insert(job_id, state);
        if !self.job_order.contains(&job_id) {
            self.job_order.push(job_id);
        }
        self.blocked_restores.insert(job_id, request);
        self.remove_pending_completion_check(job_id);
        self.update_queue_metrics();
        // Await this write: the next startup must retain the pause even if
        // filesystem recovery succeeds by then.
        if let Err(persist_error) = self
            .db_blocking(move |db| {
                db.set_active_job_runtime(
                    job_id,
                    "paused",
                    Some("queued"),
                    Some("idle"),
                    Some("paused"),
                    Some(&message),
                    queued_repair,
                    queued_extract,
                    resume_status,
                    resume_download,
                    resume_post,
                )
            })
            .await
        {
            tracing::error!(job_id = job_id.0, %persist_error, "failed to persist blocked restore pause");
        }
        tracing::warn!(job_id = job_id.0, %error, "placement recovery blocked; job retained paused");
    }

    pub(crate) async fn resume_restored_job(
        &mut self,
        job_id: JobId,
    ) -> Result<(), crate::SchedulerError> {
        if let Some(mut request) = self.blocked_restores.get(&job_id).cloned() {
            let recovered = self
                .db_blocking(move |db| db.load_active_jobs())
                .await
                .map_err(crate::SchedulerError::State)?
                .remove(&job_id)
                .ok_or(crate::SchedulerError::JobNotFound(job_id))?;
            request.file_identities = recovered.file_identities;
            request.detected_archives = recovered.detected_archives;
            request.queued_repair_at_epoch_ms = recovered.queued_repair_at_epoch_ms;
            request.queued_extract_at_epoch_ms = recovered.queued_extract_at_epoch_ms;
            request.status = JobStatus::Paused;
            let position = self
                .job_order
                .iter()
                .position(|id| *id == job_id)
                .unwrap_or(self.job_order.len());
            self.jobs.remove(&job_id);
            self.graph_jobs.remove(&job_id);
            self.job_order.retain(|id| *id != job_id);
            self.install_restored_job(request).await?;
            self.job_order.retain(|id| *id != job_id);
            self.job_order
                .insert(position.min(self.job_order.len()), job_id);
            if self.blocked_restores.contains_key(&job_id) {
                self.report_job(job_id);
                return Err(crate::SchedulerError::Conflict(
                    self.jobs[&job_id].failure_error.clone().unwrap(),
                ));
            }
        }
        self.resume_job_runtime(job_id)
    }

    async fn restore_job_inner(
        &mut self,
        request: RestoreJobRequest,
    ) -> Result<(), crate::SchedulerError> {
        let RestoreJobRequest {
            job_id,
            job_hash,
            spec,
            detected_archives,
            file_identities,
            status,
            download_state,
            post_state,
            run_state,
            queued_repair_at_epoch_ms,
            queued_extract_at_epoch_ms,
            paused_resume_status,
            paused_resume_download_state,
            paused_resume_post_state,
            working_dir,
        } = request;
        if self.jobs.contains_key(&job_id) {
            return Err(crate::SchedulerError::JobExists(job_id));
        }
        let mut spec = spec;
        if working_dir.starts_with(&self.intermediate_dir)
            && tokio::fs::try_exists(&working_dir).await.unwrap_or(false)
            && let Err(error) =
                stamp_working_dir(&self.intermediate_dir, &working_dir, job_id).await
        {
            tracing::warn!(
                job_id = job_id.0,
                dir = %working_dir.display(),
                error = %error,
                "failed to stamp restored working directory as Weaver-owned"
            );
        }

        let file_identities = if file_identities.is_empty() {
            Self::build_initial_file_identities(&spec, &detected_archives)
        } else {
            file_identities
        };
        // A job resumes from its checkpoint, which alone says which of its
        // articles are on disk, so the queue skips exactly those. A job with
        // no checkpoint has nothing that says what its files hold: it is
        // admitted afresh from its spec and fetches from the start.
        let (graph_restore, skip) = match self.restore_graph_job(job_id, &spec, &working_dir).await
        {
            Ok(restored) => (Some(restored.job), restored.skip),
            Err(crate::pipeline::graph::restore::GraphRestoreError::NoCheckpoint(_)) => {
                (None, HashSet::new())
            }
            Err(error) => return Err(crate::SchedulerError::Internal(error.to_string())),
        };
        // A job that never started waits its turn again holding nothing per
        // article: no checkpoint, and nothing that says it was ever handed
        // work. Every other job comes back holding its articles, as it ran.
        let waits_its_turn = graph_restore.is_none()
            && match &status {
                JobStatus::Queued => true,
                JobStatus::Paused => paused_resume_status
                    .as_ref()
                    .is_none_or(|resume| matches!(resume, JobStatus::Queued)),
                _ => false,
            };
        // Every article not in hand is queued again.
        let queue_depth: usize = spec
            .files
            .iter()
            .zip(0u32..)
            .map(|(file_spec, file_index)| {
                file_spec
                    .segments
                    .iter()
                    .filter(|seg| {
                        !skip.contains(&SegmentId {
                            file_id: NzbFileId { job_id, file_index },
                            segment_number: seg.ordinal,
                        })
                    })
                    .count()
            })
            .sum();
        let nothing_queued = queue_depth == 0;
        let status = Self::normalize_restored_status(status, nothing_queued);

        // What the checkpoint settled is downloaded, and is the floor a
        // restart resumes from.
        let settled = &skip;
        let downloaded_bytes: u64 = spec
            .files
            .iter()
            .zip(0u32..)
            .flat_map(|(file_spec, file_index)| {
                file_spec.segments.iter().filter_map(move |seg| {
                    let segment_id = SegmentId {
                        file_id: NzbFileId { job_id, file_index },
                        segment_number: seg.ordinal,
                    };
                    settled
                        .contains(&segment_id)
                        .then_some(u64::from(seg.bytes))
                })
            })
            .sum();
        let restored_download_floor_bytes = downloaded_bytes;
        let paused_resume_status = matches!(status, JobStatus::Paused)
            .then_some(paused_resume_status)
            .flatten();
        let restored_staging_dir = {
            let staging_dir = self.deterministic_extraction_staging_dir(job_id);
            tokio::fs::try_exists(&staging_dir)
                .await
                .unwrap_or(false)
                .then_some(staging_dir)
        };

        let server_attribution = self
            .db_blocking(move |db| db.load_active_server_attribution(job_id))
            .await?;
        let support_facts = self
            .db_blocking(move |db| db.load_job_support_facts(job_id))
            .await?;
        // One read of the row, decrypt of the supplied passwords and parse of
        // the persisted NZB per restored job, off the pipeline thread, instead
        // of one per archive file. Anything short of a parsed NZB leaves the
        // harvest to read the row itself, as before.
        let nzb_password_candidates = self
            .db_blocking(
                move |db| match db.load_active_job_password_sources(job_id) {
                    Ok(Some(((nzb_path, Some(nzb_zstd)), supplied))) => {
                        crate::pipeline::persisted_nzb_password_candidates(&nzb_path, &nzb_zstd)
                            .ok()
                            .map(|nzb| {
                                crate::pipeline::job_carried_password_candidates(&supplied, nzb)
                            })
                    }
                    _ => None,
                },
            )
            .await;

        self.emit_job_event(PipelineEvent::JobCreated {
            job_id,
            name: spec.name.clone(),
            total_files: spec.files.len() as u32,
            total_bytes: spec.total_bytes,
        });

        // Resolve the category's shared decoded-byte counter once, here, so the
        // per-segment accounting site does a single `Relaxed` `fetch_add`
        // through an already-resolved pointer instead of a map lookup.
        let category_bytes = self
            .metrics
            .job_lifecycle
            .category_bytes_counter(spec.category.as_deref());
        let par2_bytes = spec.par2_bytes();
        // Built below, once the job is in place, unless it waits its turn.
        let articles = if waits_its_turn {
            JobArticles::Queued(spec.drop_article_lists())
        } else {
            JobArticles::Activating(FileSummaries(
                spec.files.iter().map(FileSummary::of).collect(),
            ))
        };
        let mut state = JobState {
            job_id,
            job_hash,
            spec,
            status: status.clone(),
            download_state: Self::normalize_restored_download_state(
                download_state.unwrap_or_else(|| {
                    Self::restored_download_state_from_status(&status, nothing_queued)
                }),
                nothing_queued,
            ),
            post_state: post_state.unwrap_or(match &status {
                JobStatus::AwaitingQueueScripts => {
                    crate::jobs::model::PostState::AwaitingQueueScripts
                }
                JobStatus::Queued => crate::jobs::model::PostState::Idle,
                JobStatus::Verifying => crate::jobs::model::PostState::Verifying,
                JobStatus::QueuedRepair => crate::jobs::model::PostState::QueuedRepair,
                JobStatus::Repairing => crate::jobs::model::PostState::Repairing,
                JobStatus::QueuedExtract => crate::jobs::model::PostState::QueuedExtract,
                JobStatus::Extracting => crate::jobs::model::PostState::Extracting,
                JobStatus::Moving => crate::jobs::model::PostState::Finalizing,
                JobStatus::QueuedPostProcessing => {
                    crate::jobs::model::PostState::QueuedPostProcessing
                }
                JobStatus::PostProcessing => crate::jobs::model::PostState::PostProcessing,
                JobStatus::Complete | JobStatus::Partial => {
                    crate::jobs::model::PostState::Completed
                }
                JobStatus::Failed { .. } => crate::jobs::model::PostState::Failed,
                JobStatus::Paused => crate::jobs::model::PostState::Idle,
                JobStatus::Downloading | JobStatus::Checking => crate::jobs::model::PostState::Idle,
            }),
            run_state: run_state.unwrap_or({
                if matches!(status, JobStatus::Paused) {
                    crate::jobs::model::RunState::Paused
                } else {
                    crate::jobs::model::RunState::Active
                }
            }),
            articles,
            created_at: std::time::Instant::now(),
            created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
            queued_repair_at_epoch_ms,
            queued_extract_at_epoch_ms,
            paused_resume_status: paused_resume_status.clone(),
            paused_resume_download_state: paused_resume_download_state.or_else(|| {
                paused_resume_status
                    .as_ref()
                    .map(|status| Self::restored_download_state_from_status(status, nothing_queued))
            }),
            paused_resume_post_state: paused_resume_post_state.or_else(|| {
                paused_resume_status.as_ref().map(|status| match status {
                    JobStatus::Verifying => crate::jobs::model::PostState::Verifying,
                    JobStatus::QueuedRepair => crate::jobs::model::PostState::QueuedRepair,
                    JobStatus::Repairing => crate::jobs::model::PostState::Repairing,
                    JobStatus::QueuedExtract => crate::jobs::model::PostState::QueuedExtract,
                    JobStatus::Extracting => crate::jobs::model::PostState::Extracting,
                    JobStatus::Moving => crate::jobs::model::PostState::Finalizing,
                    JobStatus::Complete | JobStatus::Partial => {
                        crate::jobs::model::PostState::Completed
                    }
                    JobStatus::Failed { .. } => crate::jobs::model::PostState::Failed,
                    _ => crate::jobs::model::PostState::Idle,
                })
            }),
            failure_error: match status {
                JobStatus::Failed { ref error } => Some(error.clone()),
                _ => None,
            },
            working_dir,
            downloaded_bytes,
            restored_download_floor_bytes,
            downloaded_wire_bytes: 0,
            server_attribution,
            failed_bytes: 0,
            par2_bytes,
            support_facts,
            detected_archives: HashMap::new(),
            file_identities,
            staging_dir: restored_staging_dir,
            category_bytes: Some(category_bytes),
            nzb_password_candidates: nzb_password_candidates
                .map(std::sync::OnceLock::from)
                .unwrap_or_default(),
        };
        state.refresh_runtime_lanes_from_status();
        let repeated = if waits_its_turn {
            None
        } else {
            crate::pipeline::download::repeated::RepeatedArticles::new(
                &state.spec,
                &state.working_dir,
                std::sync::Arc::clone(&self.process_memory_budget),
            )
            .map_err(crate::SchedulerError::InvalidInput)?
        };
        self.jobs.insert(job_id, state);
        if !waits_its_turn {
            self.activate_job_articles(job_id, &skip, graph_restore, repeated);
        }
        self.restore_download_finalization_runtime(job_id).await;
        self.note_download_activity(job_id);
        self.job_order.push(job_id);
        if let Some(file_identities) = self
            .jobs
            .get(&job_id)
            .map(|state| state.file_identities.clone())
        {
            self.persist_file_identities(job_id, &file_identities).await;
        }
        self.reconcile_job_progress(job_id).await;
        let restored_terminal_post_processing =
            self.recover_restored_terminal_post_processing(job_id);

        if self
            .jobs
            .get(&job_id)
            .is_some_and(|state| matches!(state.status, JobStatus::Repairing))
        {
            self.metrics.repair_active.fetch_add(1, Ordering::Relaxed);
        }
        if self
            .jobs
            .get(&job_id)
            .is_some_and(|state| matches!(state.status, JobStatus::Extracting))
        {
            self.metrics.extract_active.fetch_add(1, Ordering::Relaxed);
        }
        if self
            .jobs
            .get(&job_id)
            .is_some_and(|state| matches!(state.status, JobStatus::Verifying))
        {
            self.metrics.verify_active.fetch_add(1, Ordering::Relaxed);
        }

        let (queued_repair_at_epoch_ms, queued_extract_at_epoch_ms) = self
            .jobs
            .get(&job_id)
            .map(|state| {
                (
                    state.queued_repair_at_epoch_ms,
                    state.queued_extract_at_epoch_ms,
                )
            })
            .unwrap_or((None, None));

        info!(
            job_id = job_id.0,
            downloaded_bytes,
            restored_download_floor_bytes,
            status = ?status,
            queued_repair_at_epoch_ms = queued_repair_at_epoch_ms.unwrap_or(0.0),
            queued_extract_at_epoch_ms = queued_extract_at_epoch_ms.unwrap_or(0.0),
            queue_depth,
            restore_skip_segments = skip.len(),
            "job restored from journal"
        );
        if !restored_terminal_post_processing
            && self.jobs.get(&job_id).is_some_and(|state| {
                !matches!(
                    state.status,
                    JobStatus::Paused
                        | JobStatus::Moving
                        | JobStatus::Complete
                        | JobStatus::Partial
                        | JobStatus::Failed { .. }
                )
            })
        {
            self.schedule_job_completion_check(job_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::jobs::ids::{JobId, NzbFileId};
    use crate::jobs::{FileSpec, JobSpec, SegmentSpec};
    use weaver_model::files::FileRole;

    fn sparse_segment_job_spec() -> JobSpec {
        JobSpec {
            name: "Sparse Segments".to_string(),
            password: None,
            total_bytes: 60,
            category: None,
            metadata: vec![],
            files: vec![FileSpec {
                filename: "episode.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    SegmentSpec {
                        ordinal: 0,
                        article_number: 1,
                        bytes: 10,
                        message_ids: crate::jobs::MessageIds::new(vec![
                            "one@example.com".to_string(),
                        ]),
                    },
                    SegmentSpec {
                        ordinal: 1,
                        article_number: 4,
                        bytes: 20,
                        message_ids: crate::jobs::MessageIds::new(vec![
                            "four@example.com".to_string(),
                        ]),
                    },
                    SegmentSpec {
                        ordinal: 2,
                        article_number: 6,
                        bytes: 30,
                        message_ids: crate::jobs::MessageIds::new(vec![
                            "six@example.com".to_string(),
                        ]),
                    },
                ],
            }],
        }
    }

    #[test]
    fn sparse_article_numbers_queue_dense_ordinals() {
        let job_id = JobId(41);
        let spec = sparse_segment_job_spec();
        let (assembly, mut download_queue) =
            Pipeline::build_job_assembly(job_id, &spec, &HashSet::new());

        let mut queued = Vec::new();
        while let Some(work) = download_queue.pop() {
            queued.push(work.segment_id.segment_number);
        }
        queued.sort_unstable();

        assert_eq!(queued, vec![0, 1, 2]);
        assert_eq!(
            assembly
                .file(NzbFileId {
                    job_id,
                    file_index: 0,
                })
                .unwrap()
                .total_segments(),
            3
        );
    }

    #[test]
    fn sparse_article_numbers_do_not_overflow_committed_assembly() {
        let job_id = JobId(42);
        let spec = sparse_segment_job_spec();
        let (_, mut queue) = Pipeline::build_job_assembly(job_id, &spec, &HashSet::new());
        let mut committed = HashSet::new();
        while let Some(work) = queue.pop() {
            committed.insert(work.segment_id);
        }
        let (assembly, mut download_queue) =
            Pipeline::build_job_assembly(job_id, &spec, &committed);

        assert!(download_queue.pop().is_none());
        assert!(
            assembly
                .file(NzbFileId {
                    job_id,
                    file_index: 0,
                })
                .unwrap()
                .is_complete()
        );
    }
}
