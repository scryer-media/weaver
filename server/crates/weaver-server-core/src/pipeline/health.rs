use super::*;

const HEALTH_PROBE_REARM_MIN_BYTES: u64 = 128 * 1024 * 1024;
const HEALTH_PROBE_REARM_PAYLOAD_DIVISOR: u64 = 200;

impl Pipeline {
    fn health_tracked_bytes(total_bytes: u64, par2_bytes: u64) -> u64 {
        total_bytes.saturating_sub(par2_bytes)
    }

    fn critical_health_milli(total: u64, par2_bytes: u64) -> u32 {
        if par2_bytes == 0 {
            850
        } else if par2_bytes * 2 > total {
            0
        } else {
            let denom = total.saturating_sub(par2_bytes);
            total
                .saturating_sub(par2_bytes * 2)
                .saturating_mul(1000)
                .checked_div(denom)
                .unwrap_or(0) as u32
        }
    }

    fn next_health_probe_failed_bytes(
        state: &crate::jobs::model::JobState,
        missed: usize,
        inconclusive: bool,
    ) -> u64 {
        let immediate_rearm = state.failed_bytes.saturating_add(1);
        if inconclusive || missed > 0 {
            return immediate_rearm;
        }

        let tracked_bytes = Self::health_tracked_bytes(state.spec.total_bytes, state.par2_bytes);
        let rearm_delta = (tracked_bytes / HEALTH_PROBE_REARM_PAYLOAD_DIVISOR)
            .max(HEALTH_PROBE_REARM_MIN_BYTES)
            .max(1);
        let critical = Self::critical_health_milli(state.spec.total_bytes, state.par2_bytes);
        let critical_failed_bytes = state
            .spec
            .total_bytes
            .saturating_sub(((state.spec.total_bytes as u128 * critical as u128) / 1000) as u64);
        let critical_rearm = critical_failed_bytes.saturating_add(1);

        state
            .failed_bytes
            .saturating_add(rearm_delta)
            .min(critical_rearm)
            .max(immediate_rearm)
    }

    pub(crate) fn health_probe_candidates(spec: &crate::jobs::model::JobSpec) -> Vec<String> {
        spec.files
            .iter()
            .filter(|file_spec| file_spec.role.counts_toward_health())
            .flat_map(|file_spec| {
                file_spec
                    .segments
                    .iter()
                    .map(|segment| segment.message_id.clone())
            })
            .collect()
    }

    fn projected_probe_failed_bytes(
        state: &crate::jobs::model::JobState,
        total: usize,
        missed: usize,
    ) -> Option<u64> {
        if total == 0 || missed == 0 {
            return None;
        }

        let tracked_bytes = Self::health_tracked_bytes(state.spec.total_bytes, state.par2_bytes);
        if tracked_bytes == 0 {
            return None;
        }

        Some((tracked_bytes as u128 * missed as u128 / total as u128) as u64)
    }

    pub(crate) fn health_probe_sample_indices(total_segs: usize, probe_round: u32) -> Vec<usize> {
        if total_segs == 0 {
            return Vec::new();
        }

        let probe_count = (total_segs * 8 / 100).max(10).min(total_segs);
        let stride = (total_segs / probe_count).max(1);
        let offset = if stride > 1 {
            probe_round as usize % stride
        } else {
            0
        };

        (offset..total_segs).step_by(stride).collect()
    }

    /// Check job health and abort if below critical threshold.
    ///
    /// Health = (total_bytes - failed_bytes) / total_bytes × 1000.
    /// Critical health is derived from available PAR2 recovery data:
    ///   critical = (total - 2 × par2_bytes) / (total - par2_bytes) × 1000
    /// If no PAR2 data, defaults to 850 (85%).
    pub(super) fn check_health(&mut self, job_id: JobId) {
        // Extract all needed values upfront so the borrow on self.jobs is dropped
        // before we call activate_health_probes or mutate state.
        let (health, critical, failed_bytes, total, needs_probes) = {
            let state = match self.jobs.get(&job_id) {
                Some(s) => s,
                None => return,
            };

            if matches!(state.status, JobStatus::Failed { .. } | JobStatus::Complete) {
                return;
            }

            let total = state.spec.total_bytes;
            if total == 0 {
                return;
            }

            let health = health_milli(total, state.failed_bytes);

            let par2_bytes = state.par2_bytes;

            let critical = Self::critical_health_milli(total, par2_bytes);

            let needs_probes = health < 980
                && !state.health_probing
                && state.failed_bytes >= state.next_health_probe_failed_bytes;
            (health, critical, state.failed_bytes, total, needs_probes)
        };

        // Activate health probes when health drops 2% — sample segments across the
        // NZB to get a fast overall health estimate instead of waiting for sequential
        // processing to naturally reach damaged areas.
        if needs_probes {
            self.activate_health_probes(job_id);
        }

        if health <= critical {
            warn!(
                job_id = job_id.0,
                health_pct = health as f64 / 10.0,
                critical_pct = critical as f64 / 10.0,
                failed_bytes,
                total_bytes = total,
                "aborting job: health below critical threshold"
            );
            let error = format!(
                "health {:.1}% below critical {:.1}%",
                health as f64 / 10.0,
                critical as f64 / 10.0
            );
            self.fail_job(job_id, error);
        }
    }

    /// Handle a probe update (partial or final).
    ///
    /// Final updates either confirm a new payload-health estimate or discard
    /// the round if probe confirmation was inconclusive. If probe activation
    /// parked work for a given job, it is restored before resuming.
    pub(super) fn handle_probe_update(&mut self, update: ProbeUpdate) {
        let ProbeUpdate {
            job_id,
            total,
            missed,
            done,
            inconclusive,
        } = update;

        if let Some(state) = self.jobs.get(&job_id) {
            if matches!(state.status, JobStatus::Failed { .. } | JobStatus::Complete) {
                return;
            }
        } else {
            return;
        }

        let miss_pct = missed.saturating_mul(100).checked_div(total).unwrap_or(0);

        if done {
            info!(
                job_id = job_id.0,
                total, missed, miss_pct, inconclusive, "health probe complete"
            );
        }

        // All probes missed → release is completely gone.
        if done && !inconclusive && missed == total && total > 0 {
            let error = format!(
                "health probe: all {} samples missing — release is unavailable",
                total
            );
            warn!(job_id = job_id.0, "{error}");
            self.fail_job(job_id, error);
            return;
        }

        if done {
            // Restore to Downloading and re-enqueue held segments into job's queues.
            if let Some(state) = self.jobs.get_mut(&job_id) {
                if !inconclusive
                    && let Some(projected) =
                        Self::projected_probe_failed_bytes(state, total, missed)
                    && projected > state.failed_bytes
                {
                    state.failed_bytes = projected;
                }
                state.last_health_probe_failed_bytes = state.failed_bytes;
                state.next_health_probe_failed_bytes =
                    Self::next_health_probe_failed_bytes(state, missed, inconclusive);
                state.health_probing = false;
                let held = std::mem::take(&mut state.held_segments);
                if !held.is_empty() {
                    info!(
                        job_id = job_id.0,
                        restored = held.len(),
                        "restoring held segments"
                    );
                    for work in held {
                        if work.is_recovery {
                            state.recovery_queue.push(work);
                        } else {
                            state.download_queue.push(work);
                        }
                    }
                }
            }
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Downloading,
                Some("downloading"),
            );
            if !inconclusive {
                self.check_health(job_id);
            }
            if self.jobs.contains_key(&job_id)
                && !self.job_has_pending_download_pipeline_work(job_id)
            {
                self.schedule_job_completion_check(job_id);
            }
        }
    }

    /// Mark a job as failed and purge its queued segments.
    pub(super) fn fail_job(&mut self, job_id: JobId, error: String) {
        let (staging_dir, released_repair, released_extract) =
            if let Some(state) = self.jobs.get_mut(&job_id) {
                let released_repair = matches!(state.status, JobStatus::Repairing);
                let released_extract = matches!(state.status, JobStatus::Extracting);
                state.queued_repair_at_epoch_ms = None;
                state.queued_extract_at_epoch_ms = None;
                state.paused_resume_status = None;
                state.paused_resume_download_state = None;
                state.paused_resume_post_state = None;
                state.set_failure(error.clone());
                state.held_segments.clear();
                // Clear per-job queues to free memory.
                state.download_queue = DownloadQueue::new();
                state.recovery_queue = DownloadQueue::new();
                (state.staging_dir.take(), released_repair, released_extract)
            } else {
                (None, false, false)
            };
        if released_repair {
            self.metrics.repair_active.fetch_sub(1, Ordering::Relaxed);
        }
        if released_extract {
            self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);
        }
        // Clean up staging directory if it was created.
        if let Some(staging) = staging_dir {
            tokio::spawn(async move {
                if let Err(e) = tokio::fs::remove_dir_all(&staging).await
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    tracing::warn!(
                        dir = %staging.display(),
                        error = %e,
                        "failed to clean up staging directory on job failure"
                    );
                }
            });
        }
        self.failed_extractions.remove(&job_id);
        self.pending_concat.remove(&job_id);
        self.active_download_passes.remove(&job_id);
        self.active_downloads_by_job.remove(&job_id);
        self.active_decodes_by_job.remove(&job_id);
        self.pending_retries_by_job.remove(&job_id);
        self.remove_pending_completion_check(job_id);
        self.clear_par2_runtime_state(job_id);
        self.clear_job_rar_runtime(job_id);
        self.clear_job_write_backlog(job_id);
        self.record_job_history(job_id);
        self.job_order.retain(|id| *id != job_id);
        let _ = self
            .event_tx
            .send(PipelineEvent::JobFailed { job_id, error });
        if released_repair {
            self.promote_queued_repairs();
        }
        if released_extract {
            self.promote_queued_extractions();
        }
        self.publish_snapshot();
    }

    /// Spawn a dedicated STAT probe task to quickly estimate job health.
    ///
    /// Instead of pushing probes into the download queue (where they compete
    /// with 150k+ regular segments), this issues batched STAT checks through
    /// the high-level NNTP client so probes inherit the normal server ordering,
    /// failover, and soft-timeout semantics used by real downloads.
    ///
    /// e.g. 150k segs × 8% = ~12k probes / 50 per batch = ~240 batched checks.
    /// If every single probe returns 430 across the usable server set, the job
    /// is failed immediately.
    pub(super) fn activate_health_probes(&mut self, job_id: JobId) {
        // Set the flag and status, then collect probes separately to avoid borrow conflicts.
        let probe_round = match self.jobs.get_mut(&job_id) {
            Some(s) => {
                s.health_probing = true;
                let probe_round = s.health_probe_round;
                s.health_probe_round = s.health_probe_round.wrapping_add(1);
                probe_round
            }
            None => return,
        };
        self.transition_postprocessing_status(job_id, JobStatus::Checking, Some("checking"));

        // Build probe list from the immutable job spec.
        let state = self.jobs.get(&job_id).unwrap();
        let all_segments = Self::health_probe_candidates(&state.spec);

        let total_segs = all_segments.len();
        if total_segs == 0 {
            if let Some(state) = self.jobs.get_mut(&job_id) {
                state.last_health_probe_failed_bytes = state.failed_bytes;
                state.next_health_probe_failed_bytes = state.failed_bytes.saturating_add(1);
                state.health_probing = false;
                let held = std::mem::take(&mut state.held_segments);
                for work in held {
                    if work.is_recovery {
                        state.recovery_queue.push(work);
                    } else {
                        state.download_queue.push(work);
                    }
                }
            }
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Downloading,
                Some("downloading"),
            );
            if !self.job_has_pending_download_pipeline_work(job_id) {
                self.schedule_job_completion_check(job_id);
            }
            return;
        }

        // Rotate evenly strided samples across rounds so repeat probes widen
        // coverage instead of re-checking the same optimistic slice forever.
        let probe_indexes = Self::health_probe_sample_indices(total_segs, probe_round);

        // Collect the message ids for each probe.
        let probes: Vec<String> = probe_indexes
            .into_iter()
            .map(|i| all_segments[i].clone())
            .collect();

        let probe_count = probes.len();
        let nntp = Arc::clone(&self.nntp);
        let probe_tx = self.probe_result_tx.clone();

        info!(
            job_id = job_id.0,
            probe_round,
            probes = probe_count,
            total_segments = total_segs,
            "health probe activated — batched STAT sampling"
        );

        // Spawn a dedicated task that checks articles in batches via the NNTP
        // client. The client handles batching per server, load-aware ordering,
        // failover, and soft timeouts so probes match the real downloader path.
        tokio::spawn(async move {
            info!(
                job_id = job_id.0,
                probe_round,
                probes = probe_count,
                "health probe starting"
            );

            let mut missed: usize = 0;
            let mut checked: usize = 0;
            const BATCH_SIZE: usize = 50;
            const UPDATE_INTERVAL: usize = 10;
            let mut batches_since_update: usize = 0;

            for batch in probes.chunks(BATCH_SIZE) {
                let msg_ids: Vec<&str> = batch.iter().map(|mid| mid.as_str()).collect();

                let results = nntp.confirm_exists_for_probe(&msg_ids).await;
                if results.inconclusive {
                    warn!("health probe: confirmation batch inconclusive, aborting probe");
                    let _ = probe_tx
                        .send(ProbeUpdate {
                            job_id,
                            total: checked,
                            missed,
                            done: true,
                            inconclusive: true,
                        })
                        .await;
                    return;
                }

                for exists in results.exists {
                    checked += 1;
                    if !exists {
                        missed += 1;
                    }
                }

                batches_since_update += 1;
                if batches_since_update >= UPDATE_INTERVAL {
                    batches_since_update = 0;
                    let _ = probe_tx
                        .send(ProbeUpdate {
                            job_id,
                            total: checked,
                            missed,
                            done: false,
                            inconclusive: false,
                        })
                        .await;
                }
            }

            info!(
                job_id = job_id.0,
                probes = probe_count,
                missed,
                "health probe complete"
            );
            let _ = probe_tx
                .send(ProbeUpdate {
                    job_id,
                    total: probe_count,
                    missed,
                    done: true,
                    inconclusive: false,
                })
                .await;
        });
    }
}
