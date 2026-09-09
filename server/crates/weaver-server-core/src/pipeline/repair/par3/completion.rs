//! PAR3 completion policy and reconciliation of native verified installations.

use super::*;
use crate::pipeline::JobStatus;
use par3_rs::session::RepairStatus;
use par3_rs::session_repair::InstalledFile;

impl Pipeline {
    /// Return true when PAR3 owns the next completion step. PAR2 keeps its
    /// existing first opportunity when a job has a usable PAR2 set.
    pub(in crate::pipeline) async fn check_par3_completion(&mut self, job_id: JobId) -> bool {
        let admitted = self
            .par3_runtime
            .as_ref()
            .is_some_and(|runtime| runtime.contains_job(job_id));
        if !admitted {
            if !self
                .jobs
                .get(&job_id)
                .is_some_and(|state| state.assembly.has_par3_candidates())
            {
                return false;
            }
            // A completely unavailable index never emits a decode event. Its
            // remaining carriers still deserve bounded metadata discovery.
            let runtime = self.par3_runtime.get_or_insert_with(|| {
                Box::new(work::Coordinator::new(self.repair_work_done_tx.clone()))
            });
            if let Err(error) = runtime
                .admit(job_id)
                .and_then(|()| self.refresh_par3_sources(job_id))
            {
                self.fail_job(job_id, format!("PAR3 discovery admission failed: {error}"));
                return true;
            }
        }
        let Some(runtime) = self.par3_runtime.as_ref() else {
            return false;
        };
        if !runtime.contains_job(job_id) {
            return false;
        }
        if self.job_has_pending_download_pipeline_work(job_id) || runtime.has_work(job_id) {
            return true;
        }
        if !self.par2_servable_set_ids(job_id).is_empty() && !self.par2_verified.contains(&job_id) {
            return false;
        }
        if let Some(error) = runtime.error(job_id) {
            self.fail_job(job_id, format!("PAR3 assessment failed: {error}"));
            return true;
        }
        let next = runtime
            .assessments(job_id)
            .find(|(_, view)| view.status != RepairStatus::Complete)
            .map(|(id, view)| (id, view.status));
        let Some((set, status)) = next else {
            if runtime.authenticated_set_count(job_id) == 0 {
                if self.promote_par3_recovery(job_id) {
                    return true;
                }
                self.fail_job(
                    job_id,
                    "PAR3 carriers contain no complete authenticated set".into(),
                );
                return true;
            }
            return false;
        };
        match status {
            RepairStatus::Complete => false,
            RepairStatus::Ready => {
                self.prepare_direct_unpack_for_par3_repair(job_id);
                if self.job_has_active_extraction_tasks(job_id) {
                    return true;
                }
                if !self.maybe_start_repair(job_id).await {
                    return true;
                }
                let output = self.jobs[&job_id].working_dir.clone();
                if let Err(error) = self
                    .par3_runtime
                    .as_mut()
                    .expect("admitted job")
                    .request_repair(job_id, set, output)
                {
                    self.fail_direct_unpack_after_repair(job_id, &error.to_string());
                    self.fail_job(job_id, format!("PAR3 repair dispatch failed: {error}"));
                }
                true
            }
            RepairStatus::IncompleteMetadata | RepairStatus::NeedRecovery => {
                if !self.promote_par3_recovery(job_id) {
                    let reason = if status == RepairStatus::IncompleteMetadata {
                        "authenticated metadata remains incomplete"
                    } else {
                        "compatible recovery remains insufficient for one or more cohorts"
                    };
                    self.fail_job(job_id, format!("PAR3 recovery exhausted: {reason}"));
                }
                true
            }
            RepairStatus::Unsupported => {
                self.fail_job(
                    job_id,
                    "PAR3 set requires an unsupported repair geometry".into(),
                );
                true
            }
        }
    }

    pub(super) async fn finish_par3_repair(
        &mut self,
        job_id: JobId,
        completion: work::RepairCompletion,
    ) {
        let work::RepairCompletion {
            result,
            _reservation,
        } = completion;
        if !self.jobs.contains_key(&job_id) {
            return;
        }
        let installed = match &result {
            Ok(report) => report.installed.as_slice(),
            Err(EngineError::RepairInterrupted { installed, .. }) => installed.as_slice(),
            Err(_) => &[],
        };
        // Reconcile independently verified installations even on a later
        // failure. Never turn an engine error into a successful job verdict.
        if let Err(error) = self.reconcile_par3_installations(job_id, installed).await {
            self.fail_direct_unpack_after_repair(job_id, &error);
            self.fail_job(job_id, error);
            return;
        }
        match result {
            Ok(report) => {
                tracing::info!(
                    job_id = job_id.0,
                    files = report.installed.len(),
                    reconstructed_blocks = report.reconstructed_blocks,
                    "PAR3 repair installed verified outputs"
                );
                self.release_direct_unpack_after_repair(job_id);
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Downloading,
                    Some("downloading"),
                );
                self.schedule_job_completion_check(job_id);
            }
            Err(error) => {
                self.fail_direct_unpack_after_repair(job_id, &error.to_string());
                self.fail_job(job_id, format!("PAR3 repair failed: {error}"));
            }
        }
        self.promote_queued_repairs();
    }

    async fn reconcile_par3_installations(
        &mut self,
        job_id: JobId,
        installed: &[InstalledFile],
    ) -> Result<(), String> {
        let state = &self.jobs[&job_id];
        let mut files = Vec::with_capacity(installed.len());
        for output in installed {
            let Some(file) = state.assembly.files().find(|file| {
                state
                    .working_dir
                    .join(self.current_filename_for_file(job_id, file))
                    == output.path
            }) else {
                return Err(format!(
                    "PAR3 installed an output without a job binding: {}",
                    output.path.display()
                ));
            };
            files.push((file.file_id(), self.current_filename_for_file(job_id, file)));
        }
        // PAR3 fingerprints are not PAR2 MD5 values. Clear any previous digest
        // instead of persisting a different algorithm under the existing field.
        let entries: Vec<_> = files
            .iter()
            .map(|(id, name)| (id.file_index, name.clone(), None))
            .collect();
        if !entries.is_empty() {
            self.db_blocking(move |db| {
                db.complete_files(
                    job_id,
                    &entries,
                    crate::jobs::persistence::CompletedHashProvenance::Verified,
                )
            })
            .await
            .map_err(|error| format!("failed to persist PAR3 outputs: {error}"))?;
        }
        let ids: Vec<_> = files.iter().map(|(id, _)| *id).collect();
        let sets = self.rar_set_names_for_files(job_id, &ids);
        for (id, _) in files {
            self.invalidate_par2_session_for_file_write(id);
            self.jobs
                .get_mut(&job_id)
                .expect("live job")
                .assembly
                .file_mut(id)
                .expect("matched file")
                .mark_complete();
            self.pending_file_progress.remove(&id);
            self.persisted_file_progress.remove(&id);
            self.file_hash_states.remove(&id);
            self.expected_file_crcs.remove(&id);
            self.file_hash_reread_required.remove(&id);
            self.refresh_archive_state_for_completed_file(job_id, id, true)
                .await;
            self.enqueue_par3_installed_file(job_id, id)
                .map_err(|error| error.to_string())?;
        }
        self.invalidate_rar_plans_for_repaired_sets(job_id, sets);
        self.par3_runtime
            .as_mut()
            .expect("admitted job")
            .dispatch()
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn enqueue_par3_installed_file(&mut self, job_id: JobId, id: NzbFileId) -> EngineResult<()> {
        let state = &self.jobs[&job_id];
        let file = state
            .assembly
            .file(id)
            .ok_or(EngineError::InvalidState("missing repaired file"))?;
        let name = self.current_filename_for_file(job_id, file);
        let path = state.working_dir.join(&name);
        // Native installation verified the entire rebuilt file. Article
        // placements still describe the old download and cannot supply its new
        // coverage; the worker publishes the installed file at its actual size.
        self.par3_runtime
            .as_mut()
            .expect("admitted job")
            .enqueue_installed(job_id, SourceId(u64::from(id.file_index)), path, name)?;
        Ok(())
    }

    fn promote_par3_recovery(&mut self, job_id: JobId) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let candidates: std::collections::BTreeSet<_> = state
            .spec
            .files
            .iter()
            .enumerate()
            .filter(|(_, file)| matches!(file.role, FileRole::Par3 { .. }))
            .map(|(index, _)| index as u32)
            .collect();
        let state = self.jobs.get_mut(&job_id).expect("live job");
        let mut pool = state.recovery_queue.drain_all();
        pool.extend(
            state
                .download_queue
                .extract_matching(|work| candidates.contains(&work.segment_id.file_id.file_index)),
        );
        let runtime = self.par3_runtime.as_ref().expect("admitted job");
        let selected = pool
            .iter()
            .map(|work| work.segment_id)
            .filter(|id| {
                candidates.contains(&id.file_id.file_index)
                    && !runtime.article_promoted(job_id, id.file_id.file_index, id.segment_number)
            })
            .min_by_key(|id| {
                let needed = runtime.needed_offset(job_id, id.file_id.file_index);
                let established = needed.is_some_and(|needed| {
                    state
                        .assembly
                        .file(id.file_id)
                        .and_then(|file| file.placement_of(id.segment_number))
                        .is_some_and(|(offset, len)| {
                            offset <= needed && needed < offset.saturating_add(u64::from(len))
                        })
                });
                let rank = if established {
                    0
                } else if needed.is_some() {
                    1
                } else {
                    2
                };
                (rank, id.file_id.file_index, id.segment_number)
            });
        let Some(selected) = selected else {
            for work in pool {
                state.recovery_queue.push(work);
            }
            return false;
        };
        if let Err(error) = self.par3_runtime.as_mut().expect("admitted job").promote(
            job_id,
            selected.file_id.file_index,
            selected.segment_number,
        ) {
            for work in pool {
                state.recovery_queue.push(work);
            }
            self.fail_job(job_id, format!("PAR3 acquisition failed: {error}"));
            return true;
        }
        for mut work in pool {
            if work.segment_id == selected {
                work.priority = crate::pipeline::repair::PROMOTED_RECOVERY_PRIORITY;
                work.completion_critical = true;
                state.download_queue.push(work);
            } else {
                state.recovery_queue.push(work);
            }
        }
        // The candidate's name is only a discovery hint. Reassess authenticated
        // matrix/cohort requirements after each article. Unknown decoded
        // offsets use finite ordinal probes; NZB encoded sizes never substitute
        // for established decoded positions.
        self.transition_postprocessing_status(job_id, JobStatus::Downloading, Some("downloading"));
        self.update_queue_metrics();
        true
    }
}
