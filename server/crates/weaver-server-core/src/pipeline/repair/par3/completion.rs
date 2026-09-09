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
        if self.reopen_par2_strong_decode_claims_on_par3_damage(job_id) {
            self.schedule_job_completion_check(job_id);
            return true;
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
        if !self.par2_bypassed.contains(&job_id)
            && !self.par2_servable_set_ids(job_id).is_empty()
            && !self.par2_gate_settlement_complete(job_id)
        {
            return false;
        }
        if self.aggregate_par2_failure_message(job_id).is_some()
            && !self.par3_has_work_after_par2_failure(job_id)
        {
            return false;
        }
        if let Some(error) = runtime.error(job_id) {
            self.fail_job(job_id, format!("PAR3 assessment failed: {error}"));
            return true;
        }
        if let Some(source) = runtime.name_match_source(job_id) {
            let direct = self.direct_store.sets_for(job_id).iter().position(|set| {
                !set.is_demoted()
                    && !set.is_finalized()
                    && u32::try_from(source.0)
                        .ok()
                        .is_some_and(|index| set.plan().volume_for_file(index).is_some())
            });
            if let Some(index) = direct {
                self.demote_direct_set(
                    job_id,
                    index,
                    crate::pipeline::direct_store::router::DemotionReason::IdentityRosterUnfillable,
                )
                .await;
                return true;
            }
            self.prepare_direct_unpack_for_par3_repair(job_id);
            if self.job_has_active_extraction_tasks(job_id) {
                return true;
            }
            if let Err(error) = self.apply_par3_content_identity(job_id).await {
                self.fail_job(job_id, format!("PAR3 content placement failed: {error}"));
            }
            return true;
        }
        self.note_par3_verification(job_id);
        let runtime = self.par3_runtime.as_ref().expect("admitted PAR3 job");
        let next = runtime
            .assessments(job_id)
            .find(|(_, view)| {
                view.status != RepairStatus::Complete
                    || view.files.iter().any(|file| {
                        file.source.is_some_and(|source| {
                            view.embedded_source == Some(source)
                                && runtime.embedded_start(job_id, source).is_some()
                                && u32::try_from(source.0).ok().is_some_and(|file_index| {
                                    self.jobs[&job_id]
                                        .assembly
                                        .file(NzbFileId { job_id, file_index })
                                        .is_some_and(|file| !file.is_complete())
                                })
                        })
                    })
            })
            .map(|(id, view)| {
                // Intact protected bytes with a hole in the embedded packet
                // gap still need an explicit replacement carrier before the
                // assembly can claim a complete archive. Request no extra parity.
                let status = if view.status == RepairStatus::Complete {
                    RepairStatus::Ready
                } else {
                    view.status
                };
                (
                    id,
                    status,
                    self.par3_damage_overlaps_settled_par2(job_id, view),
                )
            });
        let Some((set, status, conflicts)) = next else {
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
            return self.settle_par3_archive_checks(job_id).await;
        };
        if conflicts {
            self.fail_job(job_id, "conflicting PAR2 and PAR3 source verdicts; refusing to overwrite completed PAR2 data".into());
            return true;
        }
        match status {
            RepairStatus::Complete => false,
            RepairStatus::Ready => {
                self.prepare_direct_unpack_for_par3_repair(job_id);
                if self.job_has_active_extraction_tasks(job_id) {
                    return true;
                }
                if self.jobs.get(&job_id).is_some_and(|state| {
                    matches!(
                        state.status,
                        JobStatus::Extracting | JobStatus::QueuedExtract
                    )
                }) {
                    // A failed RAR extraction can leave its phase selected
                    // after the worker retires. Native damage now requires
                    // repair; do not park behind that idle extraction phase.
                    self.transition_postprocessing_status(
                        job_id,
                        JobStatus::Downloading,
                        Some("downloading"),
                    );
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

    /// Resolve a deferred archive check only after all native PAR3 assessments
    /// are complete. PAR3 verification does not override an archive checksum.
    async fn settle_par3_archive_checks(&mut self, job_id: JobId) -> bool {
        let sets: Vec<_> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| {
                !set.is_demoted() && !set.is_finalized() && set.router.awaits_par3_verdict()
            })
            .map(|(index, _)| index)
            .collect();
        let settled = !sets.is_empty();
        for index in sets {
            let set = self
                .direct_store
                .set_mut(job_id, index)
                .expect("selected set");
            let repaired = set.repair_attempted();
            if let Err(reason) = set.router.settle_par3_verification() {
                if repaired {
                    let error = format!(
                        "PAR3 verified sources failed the archive checksum: {}",
                        reason.metric()
                    );
                    self.fail_direct_unpack_after_repair(job_id, &error);
                    self.fail_job(job_id, error);
                } else {
                    self.invalidate_par3_direct_set(job_id, index);
                    self.demote_direct_set(job_id, index, reason).await;
                    self.schedule_job_completion_check(job_id);
                }
                return true;
            }
        }
        if settled {
            // Finalization precedes this gate in the completion pass. Re-enter
            // it before conventional extraction can consume the direct set.
            self.schedule_job_completion_check(job_id);
        }
        settled
    }

    pub(super) async fn complete_par3_repair(
        &mut self,
        job_id: JobId,
        completion: work::RepairCompletion,
    ) {
        let work::RepairCompletion {
            result,
            outputs: _outputs,
            embedded_replacement,
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
                self.metrics.job_lifecycle.note_repair(
                    crate::operations::instrumentation::StageOutcomeKind::Complete,
                    report.reconstructed_blocks,
                );
                let event = if embedded_replacement {
                    crate::events::model::PipelineEvent::EmbeddedProtectionReplaced {
                        job_id,
                        blocks_repaired: report.reconstructed_blocks,
                    }
                } else {
                    crate::events::model::PipelineEvent::RepairComplete {
                        job_id,
                        slices_repaired: u32::try_from(report.reconstructed_blocks)
                            .unwrap_or(u32::MAX),
                    }
                };
                let _ = self.event_tx.send(event);
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
            .filter(|(id, _)| self.par3_virtual_volume(*id).is_none())
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
        self.rearm_par2_after_par3_installations(job_id, &files);
        for (id, _) in files {
            self.block_crcs.forget_file(id);
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
            if self.par3_virtual_volume(id).is_some() {
                self.enqueue_par3_file(job_id, id)
                    .map_err(|error| error.to_string())?;
            } else {
                self.refresh_archive_state_for_completed_file(job_id, id, true)
                    .await;
                self.enqueue_par3_installed_file(job_id, id)
                    .map_err(|error| error.to_string())?;
            }
        }
        if !sets.is_empty() {
            let repaired_members: std::collections::HashSet<_> = sets
                .iter()
                .filter_map(|name| self.rar_sets.get(&(job_id, name.clone())))
                .filter_map(|state| state.plan.as_ref())
                .flat_map(|plan| plan.member_names.iter().cloned())
                .collect();
            let remaining = self
                .failed_extractions
                .get(&job_id)
                .into_iter()
                .flatten()
                .filter(|name| !sets.contains(*name) && !repaired_members.contains(*name))
                .cloned()
                .collect();
            // Retire failures for the installed archive sources, so the old
            // extraction result cannot trigger a refetch over verified output.
            // Other archive groups retain their own failure and retry state.
            self.replace_failed_extraction_members(job_id, remaining);
        }
        self.invalidate_rar_plans_for_repaired_sets(job_id, sets);
        self.par3_runtime
            .as_mut()
            .expect("admitted job")
            .dispatch()
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub(super) fn enqueue_par3_installed_file(
        &mut self,
        job_id: JobId,
        id: NzbFileId,
    ) -> EngineResult<()> {
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
            .enqueue_complete_file(job_id, SourceId(u64::from(id.file_index)), path, name)?;
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
