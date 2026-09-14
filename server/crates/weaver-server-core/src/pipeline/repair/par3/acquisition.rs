//! Bounded recovery windows. Download estimates control scheduling, never proof.
use super::*;
use crate::jobs::ids::SegmentId;
use crate::pipeline::{JobStatus, SegmentTerminalState};
use par3_rs::session::RepairStatus;

const BATCH_ARTICLES: usize = 32;
const BATCH_BYTES: u64 = 32 << 20;
const PREFETCH_BYTES: u64 = 8 << 20;

impl Pipeline {
    fn par3_batch_has_activity(&self, job_id: JobId) -> bool {
        let Some(batch) = self
            .par3_runtime
            .as_ref()
            .and_then(|runtime| runtime.acquisition(job_id))
            .and_then(|acquisition| acquisition.batch.as_ref())
        else {
            return false;
        };
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        batch.articles.iter().any(|id| {
            state
                .download_queue
                .count_matching(|work| work.segment_id == *id)
                != 0
                || self
                    .active_downloads_by_file
                    .get(&id.file_id)
                    .copied()
                    .unwrap_or(0)
                    != 0
                || self
                    .active_decodes_by_file
                    .get(&id.file_id)
                    .copied()
                    .unwrap_or(0)
                    != 0
                || self
                    .pending_decode
                    .iter()
                    .any(|work| work.segment_id == *id)
                || self
                    .write_buffers
                    .get(&id.file_id)
                    .is_some_and(|buffer| buffer.buffered_len() != 0)
        })
        // Parked retries deliberately do not keep an acquisition window active.
        // Their existing work retains retry ownership and is never duplicated.
    }

    pub(in crate::pipeline) fn maybe_prefetch_par3_recovery(&mut self, job_id: JobId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        if !matches!(state.status, JobStatus::Downloading)
            || self.direct_demotion_in_flight.contains_key(&job_id)
            || !self.par2_servable_set_ids(job_id).is_empty()
        {
            return;
        }
        let Some(runtime) = self.par3_runtime.as_ref() else {
            return;
        };
        if !runtime.contains_job(job_id) || runtime.is_installing(job_id) {
            return;
        }
        // A carrier can finish its window while one of its other articles is
        // parked. Publish committed arrivals without waiting for that retry.
        if !self.par3_batch_has_activity(job_id) {
            let dirty = runtime.dirty_sources(job_id);
            let publish: Vec<_> = runtime
                .acquisition(job_id)
                .and_then(|acquisition| acquisition.batch.as_ref())
                .into_iter()
                .flat_map(|batch| &batch.articles)
                .map(|id| id.file_id.file_index)
                .filter(|file| dirty.contains(&SourceId(u64::from(*file))))
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            for file_index in publish {
                if let Err(error) = self.enqueue_par3_file(job_id, NzbFileId { job_id, file_index })
                {
                    self.fail_job(job_id, format!("PAR3 recovery publication failed: {error}"));
                    return;
                }
            }
        }
        let runtime = self.par3_runtime.as_ref().expect("admitted job");
        // Match confirmed missing articles to authenticated protected identities.
        // A wholly absent source has no publication yet: its authenticated
        // expected path permits bounded discovery, never a recovery/repair verdict.
        let confirmed_missing = self.segment_terminal_states.iter().any(|(id, terminal)| {
            id.file_id.job_id == job_id
                && (runtime.protects_source(job_id, SourceId(u64::from(id.file_id.file_index)))
                    || runtime.assessments(job_id).any(|(_, view)| {
                        view.files.iter().any(|file| {
                            file.source.is_none()
                                && self.jobs[&job_id]
                                    .spec
                                    .files
                                    .get(id.file_id.file_index as usize)
                                    .is_some_and(|candidate| candidate.filename == file.path)
                        })
                    }))
                && matches!(
                    terminal,
                    SegmentTerminalState::Missing | SegmentTerminalState::DecodeExhausted
                )
        });
        let confirmed = confirmed_missing
            || runtime.assessments(job_id).any(|(_, view)| {
                view.files.iter().any(|file| {
                    file.source.is_some_and(|source| {
                        (view.status == RepairStatus::NeedRecovery
                            && !file.unresolved.is_empty()
                            && u32::try_from(source.0).ok().is_some_and(|file_index| {
                                self.jobs[&job_id]
                                    .assembly
                                    .file(NzbFileId { job_id, file_index })
                                    .is_some_and(|file| file.is_complete())
                            }))
                            || self.segment_terminal_states.iter().any(|(id, terminal)| {
                                id.file_id.job_id == job_id
                                    && u64::from(id.file_id.file_index) == source.0
                                    && matches!(
                                        terminal,
                                        SegmentTerminalState::Missing
                                            | SegmentTerminalState::DecodeExhausted
                                    )
                            })
                    })
                })
            });
        if confirmed {
            // Before the payload drains, only the one speculative window is
            // allowed. Afterwards authenticated deficits can drive full batches
            // even if unrelated retries keep the job's pipeline nonempty.
            let payload_pending = self.jobs[&job_id].download_queue.count_matching(|work| {
                !matches!(
                    self.jobs[&job_id].spec.files[work.segment_id.file_id.file_index as usize].role,
                    FileRole::Par3 { .. } | FileRole::Par2 { .. }
                )
            }) != 0
                || self.active_downloads_by_file.iter().any(|(id, count)| {
                    id.job_id == job_id
                        && *count != 0
                        && !matches!(
                            self.jobs[&job_id].spec.files[id.file_index as usize].role,
                            FileRole::Par3 { .. } | FileRole::Par2 { .. }
                        )
                });
            self.promote_par3_recovery_window(job_id, payload_pending);
        }
    }

    /// Tally a drained acquisition window: each article it admitted either
    /// reached the assembly or did not. Counted once per window.
    fn settle_par3_recovery_batch(&mut self, job_id: JobId) {
        let articles = match self.par3_runtime.as_mut() {
            Some(runtime) => runtime.take_unsettled_articles(job_id),
            None => return,
        };
        if articles.is_empty() {
            return;
        }
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let received = articles
            .iter()
            .filter(|id| {
                state
                    .assembly
                    .file(id.file_id)
                    .is_some_and(|file| file.has_segment(id.segment_number))
            })
            .count() as u64;
        let par3 = &self.metrics.par3;
        use std::sync::atomic::Ordering::Relaxed;
        if received != 0 {
            par3.recovery_articles_received_total
                .fetch_add(received, Relaxed);
        }
        let failed = articles.len() as u64 - received;
        if failed != 0 {
            par3.recovery_articles_failed_total
                .fetch_add(failed, Relaxed);
        }
    }

    pub(in crate::pipeline) fn promote_par3_recovery_window(
        &mut self,
        job_id: JobId,
        prefetch: bool,
    ) -> bool {
        let Some(runtime) = self.par3_runtime.as_ref() else {
            return false;
        };
        let Some(acquisition) = runtime.acquisition(job_id) else {
            return false;
        };
        let prefetched = acquisition.prefetched;
        let engine_busy = runtime.has_work(job_id);
        if self.par3_batch_has_activity(job_id) {
            return true;
        }
        // Nothing of the previous window is still moving, so every article it
        // admitted has either landed or been given up on. Account for it
        // before a new window can admit the same indices again.
        self.settle_par3_recovery_batch(job_id);
        if (prefetch && prefetched) || (!prefetch && engine_busy) {
            return false;
        }
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        if matches!(
            state.status,
            JobStatus::Paused | JobStatus::Failed { .. } | JobStatus::Complete
        ) {
            return false;
        }
        // Only cohorts that are actually short enter the plan. A cohort in
        // surplus contributes no bytes and admits no index, so nothing below
        // can spend this window's budget on parity it cannot use.
        let plan = self.par3_cohort_plan(job_id);
        let needed_bytes = plan.needed_bytes;
        if plan.views != 0 && needed_bytes == 0 && !plan.metadata_incomplete {
            return false;
        }
        if plan.metadata_incomplete {
            self.metrics
                .par3
                .metadata_incomplete_waits_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let Some(runtime) = self.par3_runtime.as_ref() else {
            return false;
        };
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let limit = if prefetch {
            self.nntp.pool().fill_connection_capacity().clamp(1, 8)
        } else {
            BATCH_ARTICLES
        };
        let byte_limit = if prefetch {
            PREFETCH_BYTES
        } else if needed_bytes == 0 {
            BATCH_BYTES
        } else {
            needed_bytes.saturating_add(1 << 20).min(BATCH_BYTES)
        };
        let candidates: std::collections::BTreeSet<_> = state
            .spec
            .files
            .iter()
            .enumerate()
            .filter(|(_, file)| matches!(file.role, FileRole::Par3 { .. }))
            // A volume whose advertised span provably holds no index any
            // deficient cohort still admits cannot close this window. The name
            // is only ever used to exclude: a volume that advertises nothing
            // parsable stays eligible.
            .filter(|(_, file)| {
                super::cohorts::volume_span(&file.filename)
                    .is_none_or(|span| !plan.excludes_span(&span))
            })
            .map(|(index, _)| index as u32)
            .collect();
        let state = self.jobs.get_mut(&job_id).expect("live job");
        let mut pool = state.recovery_queue.drain_all();
        pool.extend(state.download_queue.extract_matching(|work| {
            let id = work.segment_id;
            candidates.contains(&id.file_id.file_index)
                && !runtime.article_promoted(job_id, id.file_id.file_index, id.segment_number)
        }));
        pool.sort_by_key(|work| {
            let id = work.segment_id;
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
            (
                if established {
                    0
                } else if needed.is_some() {
                    1
                } else {
                    2
                },
                id.file_id.file_index,
                id.segment_number,
            )
        });
        let mut selected: Vec<SegmentId> = Vec::new();
        let mut bytes = 0u64;
        for work in &pool {
            let id = work.segment_id;
            if !candidates.contains(&id.file_id.file_index)
                || selected.contains(&id)
                || runtime.article_promoted(job_id, id.file_id.file_index, id.segment_number)
            {
                continue;
            }
            let estimate = u64::from(work.byte_estimate.max(1));
            // Speculative fetching has a hard per-job byte cap. An oversized
            // article can use ordinary admission once recovery is demanded.
            if prefetch && estimate > byte_limit {
                continue;
            }
            if selected.len() == limit
                || (!selected.is_empty() && bytes.saturating_add(estimate) > byte_limit)
            {
                break;
            }
            selected.push(id);
            bytes = bytes.saturating_add(estimate);
        }
        if selected.is_empty() {
            for work in pool {
                state.recovery_queue.push(work);
            }
            if needed_bytes != 0 {
                self.metrics
                    .par3
                    .need_data_waits_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            return false;
        }
        let runtime = self.par3_runtime.as_mut().expect("admitted job");
        let admission = runtime
            .begin_recovery_batch(job_id, selected.clone(), prefetch)
            .and_then(|()| {
                selected.iter().try_for_each(|id| {
                    runtime.promote(job_id, id.file_id.file_index, id.segment_number)
                })
            });
        if let Err(error) = admission {
            for work in pool {
                state.recovery_queue.push(work);
            }
            self.metrics
                .par3
                .note_admission_refused(super::outcome::admission_reason(&error));
            self.fail_job(job_id, format!("PAR3 acquisition failed: {error}"));
            return true;
        }
        let mut remaining: std::collections::HashSet<_> = selected.iter().copied().collect();
        for mut work in pool {
            if remaining.remove(&work.segment_id) {
                work.priority = crate::pipeline::repair::PROMOTED_RECOVERY_PRIORITY;
                work.completion_critical = true;
                state.download_queue.push(work);
            } else {
                state.recovery_queue.push(work);
            }
        }
        {
            use std::sync::atomic::Ordering::Relaxed;
            let par3 = &self.metrics.par3;
            par3.recovery_windows_admitted_total.fetch_add(1, Relaxed);
            par3.recovery_articles_requested_total
                .fetch_add(selected.len() as u64, Relaxed);
        }
        tracing::info!(
            job_id = job_id.0,
            articles = selected.len(),
            estimated_bytes = bytes,
            needed_bytes,
            cohorts_short = plan.windows.len(),
            prefetch,
            "PAR3 recovery window admitted"
        );
        if !prefetch {
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Downloading,
                Some("downloading"),
            );
        }
        self.update_queue_metrics();
        // Not a failure: the admitted window *is* the plan for the cohorts
        // that are still short, and recording it keeps every verdict, wait and
        // failure on one counter.
        self.settle_par3_outcome(
            job_id,
            super::outcome::Par3Outcome::NeedsRecovery {
                cohorts: plan.deficits(),
                bytes: needed_bytes,
            },
        );
        true
    }
}
