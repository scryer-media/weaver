//! PAR3 completion policy and reconciliation of native verified installations.

use super::*;
use crate::pipeline::JobStatus;
use par3_rs::session::RepairStatus;
use par3_rs::session_repair::InstalledFile;

mod pressure;

impl Pipeline {
    /// A refused virtual image becomes ordinary disk-backed source work. The
    /// existing demotion ticket fences verification and owns reconstruction.
    pub(in crate::pipeline) async fn spill_par3_source(&mut self, job_id: JobId) -> bool {
        if self.direct_demotion_in_flight.contains_key(&job_id) {
            return true;
        }
        let Some(source) = self
            .par3_runtime
            .as_mut()
            .and_then(|runtime| runtime.take_spill(job_id))
        else {
            return false;
        };
        let index = u32::try_from(source.0).ok().and_then(|file| {
            self.direct_store.sets_for(job_id).iter().position(|set| {
                !set.is_demoted()
                    && !set.is_finalized()
                    && set.plan().volume_for_file(file).is_some()
            })
        });
        let Some(index) = index else {
            // No direct set backs this source, so there is no disk image to
            // fall back to at any budget. Waiting cannot change that.
            self.settle_par3_outcome(
                job_id,
                outcome::Par3Outcome::NotExecutable {
                    limit: "the refused source has no disk fallback",
                },
            );
            return true;
        };
        let set = self.direct_store.set(job_id, index).expect("selected set");
        let sources: Vec<_> = set
            .plan()
            .volumes
            .values()
            .map(|file| SourceId(u64::from(*file)))
            .collect();
        let required = set.plan().volumes.keys().try_fold(0u64, |bytes, volume| {
            bytes.checked_add(set.virtual_volume_len(*volume, 0))
        });
        // What the refused image alone asks for. The sum above is what the
        // fallback needs; this is the floor to report when that sum cannot be
        // taken, so a refusal never claims it needed nothing.
        let refused_bytes = u32::try_from(source.0).ok().and_then(|file| {
            set.plan()
                .volumes
                .iter()
                .find(|(_, index)| **index == file)
                .map(|(volume, _)| set.virtual_volume_len(*volume, 0))
        });
        let path = self.jobs[&job_id].working_dir.clone();
        let reserve = self.direct_store.settings().holds_disk_reserve_bytes;
        let space =
            tokio::task::spawn_blocking(move || crate::operations::disk::probe_disk_space(&path))
                .await;
        let admitted = match (space, required) {
            (Ok(Ok(space)), Some(bytes)) => self
                .par3_runtime
                .as_mut()
                .expect("spill coordinator")
                .reserve_spill_disk(job_id, bytes, space.available_bytes, reserve)
                .is_ok(),
            _ => false,
        };
        if !admitted {
            self.metrics.par3.note_admission_refused(
                crate::operations::metrics::Par3AdmissionReason::DiskFallbackSpace,
            );
            // The peer work unit's handback can still free the native memory
            // this image was refused for, so that refusal is a wait. Nothing
            // frees disk in the meantime: with no peer in flight the verdict
            // is terminal and names the budget that actually refused it.
            let peer = self
                .par3_runtime
                .as_ref()
                .is_some_and(|runtime| runtime.peer_holds_par3_memory(job_id));
            if peer {
                self.refuse_par3_memory(job_id, source, required.or(refused_bytes).unwrap_or(0));
            } else {
                self.settle_par3_outcome(
                    job_id,
                    outcome::Par3Outcome::NotExecutable {
                        limit: "the disk fallback has no room for the refused source",
                    },
                );
            }
            return true;
        }
        self.metrics
            .par3
            .spills_to_disk_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(runtime) = self.par3_runtime.as_mut() {
            runtime.resume_from_memory(job_id);
        }
        tracing::info!(
            job_id = job_id.0,
            source = source.0,
            required_bytes = required,
            stage = "disk_fallback",
            "PAR3 memory pressure: reconstructing direct set on disk"
        );
        self.demote_direct_set(
            job_id,
            index,
            crate::pipeline::direct_store::router::DemotionReason::Par3MemoryPressure,
        )
        .await;
        // A sweep takes the lease through its blocking worker. A synchronous
        // demotion refusal must not leave an unused reservation behind.
        if let Some(runtime) = self.par3_runtime.as_mut() {
            drop(runtime.take_spill_disk(job_id));
            if let Err(error) = runtime.release_spilled_images(job_id, &sources) {
                self.fail_job(
                    job_id,
                    format!("PAR3 disk fallback invalidation failed: {error}"),
                );
            }
        }
        true
    }

    /// Read-only presentation of a drained download awaiting native work.
    /// Scheduler phases retain their own transition and completion contracts.
    pub(in crate::pipeline) fn show_par3_verification_wait(&self, job_id: JobId) -> bool {
        self.par3_runtime
            .as_ref()
            .is_some_and(|runtime| runtime.has_work(job_id))
            && self
                .jobs
                .get(&job_id)
                .is_some_and(|state| matches!(state.status, JobStatus::Downloading))
            && !self.job_has_pending_download_pipeline_work(job_id)
    }

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
                Box::new(work::Coordinator::new(
                    self.repair_work_done_tx.clone(),
                    Arc::clone(&self.metrics),
                ))
            });
            if let Err(error) = runtime
                .admit(job_id)
                .and_then(|()| self.refresh_par3_sources(job_id))
            {
                self.fail_job(job_id, format!("PAR3 discovery admission failed: {error}"));
                return true;
            }
        }
        if self.spill_par3_source(job_id).await {
            return true;
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
            .filter(|(_, view)| {
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
            })
            // Repair an independent ready set before requesting recovery for
            // a blocked sibling. One set's deficit need not delay the others.
            .min_by_key(|(_, status, _)| *status != RepairStatus::Ready);
        let Some((set, status, conflicts)) = next else {
            if runtime.authenticated_set_count(job_id) == 0 {
                if self.promote_par3_recovery(job_id) {
                    return true;
                }
                let missing = self.par3_missing_metadata(job_id);
                self.settle_par3_outcome(
                    job_id,
                    outcome::Par3Outcome::MetadataIncomplete { missing },
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
                if let Err(error) = self.prepare_par3_outputs(job_id, set) {
                    self.fail_job(job_id, format!("PAR3 output planning failed: {error}"));
                    return true;
                }
                if !self.maybe_start_par3_repair(job_id).await {
                    return true;
                }
                let output = self.jobs[&job_id].working_dir.clone();
                // Low-frequency: once per dispatched repair, never per block.
                self.note_stage_started(
                    job_id,
                    crate::operations::instrumentation::JobStageKind::Repair,
                );
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
                    // Donor search finds *source* blocks, which lower a
                    // cohort's `lost`; it never manufactures a recovery index.
                    // Running it is only worth a worker while some cohort that
                    // has outrun its own recovery span still has losses left
                    // to cover.
                    let spent = self.par3_cohort_plan(job_id).exhausted();
                    let donors_could_close = !self
                        .par3_runtime
                        .as_ref()
                        .expect("admitted job")
                        .donor_search_exhausted(job_id)
                        && (spent.is_empty() || spent.iter().any(|cohort| cohort.lost != 0));
                    if status == RepairStatus::NeedRecovery && donors_could_close {
                        match self
                            .par3_runtime
                            .as_mut()
                            .expect("admitted job")
                            .request_donor_search(job_id)
                        {
                            Ok(true) => return true,
                            Ok(false) => {}
                            Err(error) => {
                                self.fail_job(
                                    job_id,
                                    format!("PAR3 donor discovery failed: {error}"),
                                );
                                return true;
                            }
                        }
                    }
                    let outcome = if status == RepairStatus::IncompleteMetadata {
                        outcome::Par3Outcome::MetadataIncomplete {
                            missing: self.par3_missing_metadata(job_id),
                        }
                    } else {
                        // Name the cohorts whose own admissible span is spent
                        // when there are any; otherwise every deficient cohort
                        // is equally responsible for the verdict.
                        let plan = self.par3_cohort_plan(job_id);
                        let exhausted = plan.exhausted();
                        outcome::Par3Outcome::Unrecoverable {
                            cohorts: if exhausted.is_empty() {
                                plan.deficits()
                            } else {
                                exhausted
                            },
                        }
                    };
                    self.settle_par3_outcome(job_id, outcome);
                }
                true
            }
            RepairStatus::Unsupported => {
                self.settle_par3_outcome(
                    job_id,
                    outcome::Par3Outcome::Unsupported {
                        detail: "the engine does not execute this matrix kind",
                    },
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
            outputs,
            embedded_replacement,
            _reservation,
        } = completion;
        if !self.jobs.contains_key(&job_id) {
            return;
        }
        // Low-frequency: once per repair handback. Closes the timer armed when
        // the repair was dispatched, so a PAR3 repair lands in the same stage
        // histogram a PAR2 repair does.
        self.note_stage_finished(
            job_id,
            crate::operations::instrumentation::JobStageKind::Repair,
        );
        let installed = match &result {
            Ok(report) => report.installed.as_slice(),
            Err(EngineError::RepairInterrupted { installed, .. }) => installed.as_slice(),
            Err(_) => &[],
        };
        let outputs = match outputs {
            Ok(outputs) => outputs,
            Err(error) => {
                self.fail_job(job_id, format!("PAR3 output capture failed: {error}"));
                return;
            }
        };
        // Reconcile independently verified installations even on a later
        // failure. Never turn an engine error into a successful job verdict.
        if let Err(error) = self
            .reconcile_par3_installations(job_id, installed, &outputs)
            .await
        {
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
            Err(error)
                if budget::is_native_pressure(&error)
                    && self
                        .par3_runtime
                        .as_mut()
                        .is_some_and(|runtime| runtime.take_repair_retry(job_id)) =>
            {
                if let Err(error) = self.clear_par3_pressure_temporaries(job_id, &error).await {
                    self.fail_job(job_id, error);
                    return;
                }
                if let Err(error) = self
                    .par3_runtime
                    .as_mut()
                    .expect("admitted job")
                    .queue_reassessment(job_id)
                {
                    self.fail_job(
                        job_id,
                        format!("PAR3 reassessment admission failed: {error}"),
                    );
                    return;
                }
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Downloading,
                    Some("downloading"),
                );
                self.schedule_job_completion_check(job_id);
            }
            Err(error) if budget::error_pressure_source(&error).is_some() => {
                if let Err(cleanup) = self.clear_par3_pressure_temporaries(job_id, &error).await {
                    self.fail_direct_unpack_after_repair(job_id, &cleanup);
                    self.fail_job(job_id, cleanup);
                    return;
                }
                // The coordinator fenced further work when it received this
                // error. Keep chases parked until the affected set is demoted;
                // a partial repair has not vouched for their remaining bytes.
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Downloading,
                    Some("downloading"),
                );
                self.schedule_job_completion_check(job_id);
            }
            // An execution mode the engine recognizes but does not run is a
            // verdict about the set, not an I/O failure of this attempt.
            Err(EngineError::Unsupported(detail)) => {
                self.fail_direct_unpack_after_repair(job_id, detail);
                self.settle_par3_outcome(job_id, outcome::Par3Outcome::Unsupported { detail });
            }
            Err(error) => {
                self.fail_direct_unpack_after_repair(job_id, &error.to_string());
                self.fail_job(job_id, format!("PAR3 repair failed: {error}"));
            }
        }
        self.promote_queued_repairs();
    }

    async fn clear_par3_pressure_temporaries(
        &self,
        job_id: JobId,
        error: &EngineError,
    ) -> Result<(), String> {
        let EngineError::RepairInterrupted { temporary, .. } = error else {
            return Ok(());
        };
        if temporary.is_empty() {
            return Ok(());
        }
        let paths = temporary.clone();
        let root = self.jobs[&job_id].working_dir.clone();
        tokio::task::spawn_blocking(move || pressure::clear_temporaries(&root, &paths))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| format!("PAR3 pressure staging cleanup failed: {error}"))
    }

    async fn reconcile_par3_installations(
        &mut self,
        job_id: JobId,
        installed: &[InstalledFile],
        outputs: &[super::readback::VerifiedOutput],
    ) -> Result<(), String> {
        let state = &self.jobs[&job_id];
        let mut files = Vec::with_capacity(installed.len());
        for output in installed {
            if !outputs.iter().any(|image| image.path == output.path) {
                return Err("PAR3 installed output has no verified decoded length".into());
            }
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
        for ((id, _), output) in files.into_iter().zip(installed) {
            let decoded_len = outputs
                .iter()
                .find(|image| image.path == output.path)
                .expect("validated output image")
                .len;
            self.block_crcs.forget_file(id);
            self.invalidate_par2_session_for_file_write(id);
            self.jobs
                .get_mut(&job_id)
                .expect("live job")
                .assembly
                .file_mut(id)
                .expect("matched file")
                .mark_complete_decoded(decoded_len);
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
        self.promote_par3_recovery_window(job_id, false)
    }
}
