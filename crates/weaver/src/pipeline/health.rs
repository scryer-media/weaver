use super::*;

impl Pipeline {
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

            let health = (total.saturating_sub(state.failed_bytes) * 1000 / total) as u32;

            let par2_bytes: u64 = state
                .spec
                .files
                .iter()
                .filter(|f| matches!(f.role, weaver_core::classify::FileRole::Par2 { .. }))
                .flat_map(|f| f.segments.iter())
                .map(|s| s.bytes as u64)
                .sum();

            let critical = if par2_bytes == 0 {
                850
            } else if par2_bytes * 2 > total {
                0
            } else {
                let denom = total.saturating_sub(par2_bytes);
                if denom == 0 {
                    0
                } else {
                    ((total.saturating_sub(par2_bytes * 2)) * 1000 / denom) as u32
                }
            };

            let needs_probes = health < 980 && !state.health_probing;
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
    /// Partial updates project `failed_bytes` from the running sample so the
    /// UI shows health declining in real time. The final update additionally
    /// restores held segments (or fails the job).
    pub(super) fn handle_probe_update(&mut self, update: ProbeUpdate) {
        let ProbeUpdate {
            job_id,
            total,
            missed,
            done,
        } = update;

        if let Some(state) = self.jobs.get(&job_id) {
            if matches!(state.status, JobStatus::Failed { .. } | JobStatus::Complete) {
                return;
            }
        } else {
            return;
        }

        let miss_pct = if total > 0 { missed * 100 / total } else { 0 };

        if done {
            info!(
                job_id = job_id.0,
                total, missed, miss_pct, "health probe complete"
            );
        }

        // All probes missed → release is completely gone.
        if done && missed == total && total > 0 {
            let error = format!(
                "health probe: all {} samples missing — release is unavailable",
                total
            );
            warn!(job_id = job_id.0, "{error}");
            self.fail_job(job_id, error);
            return;
        }

        // Project failed_bytes from the current sample ratio.
        if missed > 0
            && let Some(state) = self.jobs.get_mut(&job_id)
        {
            let projected = state.spec.total_bytes as u128 * missed as u128 / total as u128;
            let projected = projected as u64;
            if projected > state.failed_bytes {
                state.failed_bytes = projected;
            }
        }

        if done {
            // Restore to Downloading and re-enqueue held segments into job's queues.
            if let Some(state) = self.jobs.get_mut(&job_id) {
                if matches!(state.status, JobStatus::Checking) {
                    state.status = JobStatus::Downloading;
                }
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
        }

        // Evaluate health threshold — may abort the job on partial or final.
        self.check_health(job_id);
    }

    /// Mark a job as failed and purge its queued segments.
    pub(super) fn fail_job(&mut self, job_id: JobId, error: String) {
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.status = JobStatus::Failed {
                error: error.clone(),
            };
            state.held_segments.clear();
            // Clear per-job queues to free memory.
            state.download_queue = DownloadQueue::new();
            state.recovery_queue = DownloadQueue::new();
        }
        self.failed_extractions.remove(&job_id);
        self.pending_concat.remove(&job_id);
        self.clear_par2_runtime_state(job_id);
        self.clear_job_rar_runtime(job_id);
        self.clear_job_write_backlog(job_id);
        self.record_job_history(job_id);
        self.job_order.retain(|id| *id != job_id);
        let _ = self
            .event_tx
            .send(PipelineEvent::JobFailed { job_id, error });
        self.publish_snapshot();
    }

    /// Spawn a dedicated STAT probe task to quickly estimate job health.
    ///
    /// Instead of pushing probes into the download queue (where they compete
    /// with 150k+ regular segments), this acquires a single NNTP connection
    /// and pipelines STAT commands in batches. STAT doesn't need GROUP, returns
    /// only a status line, and pipelining amortizes RTT across the batch.
    ///
    /// e.g. 150k segs × 8% = ~12k probes / 50 per batch = ~240 RTTs ≈ ~12 seconds.
    /// If every single probe returns 430, the job is failed immediately.
    pub(super) fn activate_health_probes(&mut self, job_id: JobId) {
        // Set the flag and status, then collect probes separately to avoid borrow conflicts.
        match self.jobs.get_mut(&job_id) {
            Some(s) => {
                s.health_probing = true;
                s.status = JobStatus::Checking;
            }
            None => return,
        }

        // Pull this job's segments out of its queues so the dispatcher skips it
        // and connections are freed for other jobs. Segments are restored (or
        // dropped) in handle_probe_update.
        if let Some(state) = self.jobs.get_mut(&job_id) {
            let mut held = state.download_queue.drain_all();
            held.append(&mut state.recovery_queue.drain_all());
            let held_count = held.len();
            state.held_segments = held;
            info!(job_id = job_id.0, held_count, "held segments during probe");
        }

        // Build probe list from the immutable job spec.
        let state = self.jobs.get(&job_id).unwrap();
        let all_segments: Vec<(u32, usize)> = state
            .spec
            .files
            .iter()
            .enumerate()
            .flat_map(|(file_idx, file_spec)| {
                (0..file_spec.segments.len()).map(move |seg_idx| (file_idx as u32, seg_idx))
            })
            .collect();

        let total_segs = all_segments.len();
        if total_segs == 0 {
            return;
        }

        // Sample 8% of segments evenly strided across the NZB.
        let probe_count = (total_segs * 8 / 100).max(10).min(total_segs);
        let stride = (total_segs / probe_count).max(1);

        // Collect (segment_id, message_id, byte_estimate) for each probe.
        let probes: Vec<(SegmentId, String, u32)> = (0..total_segs)
            .step_by(stride)
            .map(|i| {
                let (file_index, seg_idx) = all_segments[i];
                let file_spec = &state.spec.files[file_index as usize];
                let seg = &file_spec.segments[seg_idx];
                let segment_id = SegmentId {
                    file_id: NzbFileId { job_id, file_index },
                    segment_number: seg.number,
                };
                (segment_id, seg.message_id.clone(), seg.bytes)
            })
            .collect();

        let probe_count = probes.len();
        let nntp = Arc::clone(&self.nntp);
        let probe_tx = self.probe_result_tx.clone();

        info!(
            job_id = job_id.0,
            probes = probe_count,
            total_segments = total_segs,
            "health probe activated — pipelined STAT sampling"
        );

        // Spawn a dedicated task that checks articles via STAT on a single connection.
        // Uses pipelining if the server supports it, otherwise falls back to sequential.
        // Tracks total misses and reports back to the pipeline loop.
        tokio::spawn(async move {
            let mut conn = match nntp.pool().acquire(weaver_nntp::ServerId(0)).await {
                Ok(c) => c,
                Err(e) => {
                    warn!(error = %e, "health probe: failed to acquire connection");
                    return;
                }
            };

            let pipelining = conn.capabilities().supports_pipelining();
            info!(
                job_id = job_id.0,
                pipelining,
                probes = probe_count,
                "health probe starting"
            );

            let mut missed: usize = 0;
            let mut checked: usize = 0;

            if pipelining {
                // Pipeline STAT in batches of 50 — N articles per RTT.
                // Send partial updates every 10 batches (500 STATs).
                const BATCH_SIZE: usize = 50;
                const UPDATE_INTERVAL: usize = 10;
                let mut batches_since_update: usize = 0;

                for batch in probes.chunks(BATCH_SIZE) {
                    let msg_ids: Vec<&str> = batch.iter().map(|(_, mid, _)| mid.as_str()).collect();

                    let results = match conn.stat_pipeline(&msg_ids).await {
                        Ok(r) => r,
                        Err(e) => {
                            warn!(error = %e, "health probe: pipeline batch failed");
                            missed += batch.len();
                            checked += batch.len();
                            batches_since_update += 1;
                            continue;
                        }
                    };

                    for (exists, (_seg_id, _mid, _bytes)) in results.iter().zip(batch) {
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
                            })
                            .await;
                    }
                }
            } else {
                // Sequential STAT — 1 article per RTT.
                // Send partial updates every 100 STATs.
                const UPDATE_INTERVAL: usize = 100;

                for (_seg_id, mid, _bytes) in &probes {
                    match conn.stat_by_id(mid).await {
                        Ok(false) => {
                            missed += 1;
                        }
                        Ok(true) => {}
                        Err(e) => {
                            warn!(error = %e, "health probe: STAT failed, aborting probe");
                            return;
                        }
                    }
                    checked += 1;
                    if checked.is_multiple_of(UPDATE_INTERVAL) {
                        let _ = probe_tx
                            .send(ProbeUpdate {
                                job_id,
                                total: checked,
                                missed,
                                done: false,
                            })
                            .await;
                    }
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
                })
                .await;
        });
    }
}
