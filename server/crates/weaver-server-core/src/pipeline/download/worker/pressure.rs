use super::*;

impl DownloadPipelineBacklog {
    pub(in crate::pipeline::download::worker) fn has_durable_catch_up_work(self) -> bool {
        self.active_decodes != 0
            || self.delayed_retries != 0
            || self.released_results != 0
            || self.pending_decodes != 0
    }
}

impl DownloadPressure {
    pub(in crate::pipeline::download::worker) fn is_hard(self) -> bool {
        self.state == DownloadPressureState::Hard
    }

    pub(in crate::pipeline::download::worker) fn suppresses_spillover(self) -> bool {
        self.state == DownloadPressureState::Soft || self.uu_spool_admission_capped
    }
}

impl Pipeline {
    /// The cache avoids turning every dispatch decision into a filesystem
    /// query while still making low-space admission responsive.
    const UU_SPOOL_DISK_SPACE_CHECK_INTERVAL: Duration = Duration::from_secs(1);

    pub(in crate::pipeline) fn uu_spool_admission_capped(
        &mut self,
        additional_spooled_bytes: usize,
    ) -> bool {
        let byte_limit_reached = self
            .uu_spooled_bytes
            .saturating_add(additional_spooled_bytes)
            > self.uu_spool_max_bytes;
        let segment_limit_reached = self.uu_parked_segments >= self.uu_spool_max_segments;
        let required_free = self
            .uu_spool_min_free_bytes
            .saturating_add(additional_spooled_bytes as u64);
        let free_space_too_low = self
            .uu_spool_available_bytes()
            .is_none_or(|available| available < required_free);

        byte_limit_reached || segment_limit_reached || free_space_too_low
    }

    fn uu_spool_available_bytes(&mut self) -> Option<u64> {
        #[cfg(test)]
        if let Some(available) = self.uu_spool_available_bytes_for_test {
            return available;
        }

        let now = Instant::now();
        if self
            .uu_spool_last_free_space_check
            .is_some_and(|checked_at| {
                now.saturating_duration_since(checked_at) < Self::UU_SPOOL_DISK_SPACE_CHECK_INTERVAL
            })
        {
            return self.uu_spool_available_bytes;
        }

        let available = crate::operations::disk_space(&self.intermediate_dir)
            .map(|space| space.available_bytes);
        self.uu_spool_last_free_space_check = Some(now);
        self.uu_spool_available_bytes = available;
        available
    }

    pub(in crate::pipeline::download::worker) fn uu_spool_cursor_ordinals(
        &self,
    ) -> HashMap<NzbFileId, u32> {
        self.uu_files
            .iter()
            .map(|(file_id, uu)| (*file_id, uu.next_index))
            .collect()
    }

    pub(in crate::pipeline::download::worker) fn uu_work_closes_cursor(
        cursors: &HashMap<NzbFileId, u32>,
        work: &DownloadWork,
    ) -> bool {
        // Before a file's first UU part identifies its encoding, ordinal zero
        // is the only work that could establish its cursor. Permitting it also
        // leaves yEnc's ordinary ordering unchanged while the UU cache pauses.
        cursors.get(&work.segment_id.file_id).copied().unwrap_or(0)
            == work.segment_id.segment_number
    }

    pub(in crate::pipeline::download::worker) fn download_pressure_limits(
        &self,
    ) -> (u64, u64, u64, u64) {
        let decode_hard = (self.decode_backlog_budget_bytes as u64).max(1);
        let decode_soft = (decode_hard.saturating_mul(DOWNLOAD_PRESSURE_SOFT_PERCENT) / 100)
            .max(1)
            .min(decode_hard);
        let write_hard = (self.write_backlog_budget_bytes as u64).max(1);
        let write_soft = (write_hard.saturating_mul(DOWNLOAD_PRESSURE_SOFT_PERCENT) / 100)
            .max(1)
            .min(write_hard);
        (decode_soft, decode_hard, write_soft, write_hard)
    }

    pub(in crate::pipeline::download::worker) fn pressure_reason(
        decode_pressure: bool,
        write_pressure: bool,
    ) -> DownloadPressureReason {
        match (decode_pressure, write_pressure) {
            (true, true) => DownloadPressureReason::DecodeAndWrite,
            (true, false) => DownloadPressureReason::Decode,
            (false, true) => DownloadPressureReason::Write,
            (false, false) => DownloadPressureReason::None,
        }
    }

    pub(in crate::pipeline::download::worker) fn elapsed_ms(started_at: Instant) -> u64 {
        started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
    }

    pub(in crate::pipeline::download::worker) fn normal_download_connection_capacity_available(
        &self,
    ) -> bool {
        let params = self.tuner.params();
        let total = self.effective_download_connection_capacity(params.max_concurrent_downloads);
        let mut limit = self.normal_download_connection_capacity_limit(total);
        // Ordinary work can only run on fill servers; lanes beyond the fill
        // tier's connection budget would block on saturated fill semaphores
        // without ever reaching backfill. Escalated demand (queued work with
        // failure exclusions, or a job whose age retention-excludes servers)
        // may legitimately need backfill lanes, so it restores the full
        // budget. The retention read uses the warm cache only — leases keep
        // it fresh for active jobs. All-fill configs skip the clamp entirely:
        // the tuner already bounds lanes against total capacity there.
        let fill_capacity = self.nntp.pool().fill_connection_capacity();
        if self.nntp.pool().has_backfill_servers() && fill_capacity < limit {
            let escalated_demand = self
                .job_retention_exclude_cache
                .values()
                .any(|(_, excludes)| !excludes.is_empty())
                || self
                    .jobs
                    .values()
                    .any(|state| state.download_queue.excluded_work_count() > 0);
            if !escalated_demand {
                limit = fill_capacity;
            }
        }
        self.active_download_connections < limit
    }

    pub(in crate::pipeline::download::worker) fn durable_download_floor_bytes_for_job(
        &self,
        job_id: JobId,
    ) -> u64 {
        let Some(state) = self.jobs.get(&job_id) else {
            return 0;
        };
        let floor = state
            .assembly
            .files()
            .map(|file| {
                if file.is_complete() {
                    return file.total_bytes();
                }
                let file_id = file.file_id();
                self.persisted_file_progress
                    .get(&file_id)
                    .copied()
                    .unwrap_or(0)
                    .max(
                        self.pending_file_progress
                            .get(&file_id)
                            .copied()
                            .unwrap_or(0),
                    )
                    .min(file.total_bytes())
            })
            .sum::<u64>();
        floor.max(state.restored_download_floor_bytes)
    }

    pub(in crate::pipeline::download::worker) fn estimated_undurable_download_bytes_for_job(
        &self,
        job_id: JobId,
    ) -> u64 {
        let Some(state) = self.jobs.get(&job_id) else {
            return 0;
        };
        let durable_floor = self.durable_download_floor_bytes_for_job(job_id);
        let accepted_bytes = state.downloaded_bytes.saturating_sub(durable_floor);
        let speculative_active_items = self
            .active_downloads_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            .saturating_add(
                self.active_decodes_by_job
                    .get(&job_id)
                    .copied()
                    .unwrap_or(0),
            );
        let released_result_bytes = self
            .pending_released_download_result_bytes_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0);
        accepted_bytes
            .saturating_add((speculative_active_items as u64).saturating_mul(1024 * 1024))
            .saturating_add(released_result_bytes)
    }

    pub(in crate::pipeline::download::worker) fn download_pipeline_backlog_for_job(
        &self,
        job_id: JobId,
    ) -> DownloadPipelineBacklog {
        let pending_decodes = self
            .pending_decode
            .iter()
            .filter(|work| work.segment_id.file_id.job_id == job_id)
            .count();
        let (buffered_write_segments, buffered_write_bytes) = self
            .write_buffers
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .fold((0usize, 0u64), |(segments, bytes), (_, write_buf)| {
                (
                    segments.saturating_add(write_buf.buffered_len()),
                    bytes.saturating_add(write_buf.buffered_bytes() as u64),
                )
            });

        DownloadPipelineBacklog {
            active_downloads: self
                .active_downloads_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0),
            active_connections: self
                .active_download_connections_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0),
            active_decodes: self
                .active_decodes_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0),
            delayed_retries: self
                .pending_retries_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0),
            released_results: self
                .pending_released_download_results_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0),
            released_result_bytes: self
                .pending_released_download_result_bytes_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0),
            pending_decodes,
            buffered_write_segments,
            buffered_write_bytes,
        }
    }

    pub(in crate::pipeline::download::worker) fn next_queued_download_exceeds_restart_durable_lead(
        &self,
        job_id: JobId,
    ) -> bool {
        self.jobs
            .get(&job_id)
            .and_then(|state| state.download_queue.peek_next_matching(|_| true))
            .is_some_and(|work| !self.primary_download_within_restart_durable_lead(job_id, work))
    }

    pub(in crate::pipeline::download::worker) fn restart_durable_lead_block(
        &self,
        job_id: JobId,
        work: &DownloadWork,
    ) -> Option<(u64, u64)> {
        self.restart_durable_lead_block_with_extra(job_id, work, 0)
    }

    pub(in crate::pipeline::download::worker) fn should_enforce_restart_durable_lead(
        &self,
        job_id: JobId,
    ) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        if state.restored_download_floor_bytes == 0 {
            return false;
        }
        self.download_restart_durable_lead_retry_after
            .contains_key(&job_id)
            || self
                .download_pipeline_backlog_for_job(job_id)
                .has_durable_catch_up_work()
    }

    pub(in crate::pipeline::download::worker) fn should_cap_lease_for_restart_durable_lead(
        &self,
        job_id: JobId,
    ) -> bool {
        self.should_enforce_restart_durable_lead(job_id)
    }

    pub(in crate::pipeline::download::worker) fn normal_download_connection_capacity_limit(
        &self,
        effective_total: usize,
    ) -> usize {
        let params = self.tuner.params();
        let recovery_reserve = params
            .recovery_slots
            .saturating_sub(self.active_recovery)
            .min(effective_total);
        effective_total.saturating_sub(recovery_reserve)
    }

    pub(in crate::pipeline::download::worker) fn effective_download_connection_capacity(
        &self,
        configured_max: usize,
    ) -> usize {
        let active_probes = self
            .jobs
            .values()
            .filter(|s| matches!(s.status, JobStatus::Checking))
            .count();
        let adaptive_limit = self.nntp.pool().effective_connection_capacity();
        let connection_limit = if adaptive_limit == 0 {
            // A zero-server generation is already governed by the scheduler's
            // configured limit. Treat zero here as "no adaptive cap" so direct
            // pipeline fixtures and generation transitions do not collapse a
            // separately managed limit.
            configured_max
        } else {
            configured_max.min(adaptive_limit)
        };
        connection_limit.saturating_sub(active_probes)
    }

    pub(in crate::pipeline::download::worker) fn soft_pressure_dispatch_delay(
        &self,
        pressure: DownloadPressure,
    ) -> Option<Duration> {
        if pressure.state != DownloadPressureState::Soft {
            return None;
        }

        let (decode_soft, decode_hard, write_soft, write_hard) = self.download_pressure_limits();
        let decode_delay =
            Self::soft_pressure_delay_for(pressure.decode_backlog_bytes, decode_soft, decode_hard);
        let write_delay =
            Self::soft_pressure_delay_for(pressure.write_pending_bytes, write_soft, write_hard);
        decode_delay.max(write_delay)
    }

    pub(in crate::pipeline::download::worker) fn soft_pressure_delay_for(
        bytes: u64,
        soft: u64,
        hard: u64,
    ) -> Option<Duration> {
        if bytes < soft || hard <= soft {
            return None;
        }

        let numerator = bytes.saturating_sub(soft).min(hard - soft);
        let denominator = hard - soft;
        let max_ms = SOFT_PRESSURE_DISPATCH_MAX_DELAY.as_millis() as u64;
        let delay_ms = numerator.saturating_mul(max_ms) / denominator;
        Some(SOFT_PRESSURE_DISPATCH_MIN_DELAY.max(Duration::from_millis(delay_ms.max(1))))
    }

    pub(in crate::pipeline::download::worker) fn update_download_pressure_stall_metrics(
        &mut self,
        state: DownloadPressureState,
    ) {
        if state == DownloadPressureState::Hard {
            let started_at = match self.download_pressure_hard_stall_started_at {
                Some(started_at) => started_at,
                None => {
                    let started_at = Instant::now();
                    self.download_pressure_hard_stall_started_at = Some(started_at);
                    self.metrics
                        .download_pressure_stalls_total
                        .fetch_add(1, Ordering::Relaxed);
                    started_at
                }
            };
            self.metrics
                .download_pressure_current_stall_ms
                .store(Self::elapsed_ms(started_at), Ordering::Relaxed);
            return;
        }

        if let Some(started_at) = self.download_pressure_hard_stall_started_at.take() {
            let elapsed_ms = Self::elapsed_ms(started_at);
            self.metrics
                .download_pressure_stall_duration_ms
                .fetch_add(elapsed_ms, Ordering::Relaxed);
        }
        self.metrics
            .download_pressure_current_stall_ms
            .store(0, Ordering::Relaxed);
    }

    pub(crate) fn restart_durable_lead_limit_bytes() -> u64 {
        download_restart_checkpoint_bytes()
            .saturating_mul(DOWNLOAD_RESTART_MAX_DURABLE_LEAD_MULTIPLIER)
    }

    pub(crate) fn primary_download_within_restart_durable_lead(
        &self,
        job_id: JobId,
        work: &DownloadWork,
    ) -> bool {
        if !self.should_enforce_restart_durable_lead(job_id) {
            return true;
        }
        self.restart_durable_lead_block(job_id, work).is_none()
    }

    pub(in crate::pipeline::download::worker) fn restart_durable_lead_block_with_extra(
        &self,
        job_id: JobId,
        work: &DownloadWork,
        extra_undurable_bytes: u64,
    ) -> Option<(u64, u64)> {
        if work.is_recovery {
            return None;
        }
        let limit = Self::restart_durable_lead_limit_bytes();
        if limit == 0 {
            return None;
        }
        let projected = self
            .estimated_undurable_download_bytes_for_job(job_id)
            .saturating_add(extra_undurable_bytes)
            .saturating_add(work.byte_estimate as u64);
        (projected > limit).then_some((projected, limit))
    }

    pub(crate) fn refresh_download_pressure(&mut self) -> DownloadPressure {
        let (decode_soft, decode_hard, write_soft, write_hard) = self.download_pressure_limits();
        let released_result_bytes = self
            .pending_released_download_result_bytes_by_job
            .values()
            .copied()
            .fold(0, u64::saturating_add);
        let decode_bytes = self
            .metrics
            .decode_pending_bytes
            .load(Ordering::Relaxed)
            .saturating_add(self.metrics.decode_active_bytes.load(Ordering::Relaxed))
            .saturating_add(released_result_bytes);
        // Keep hard pressure tied to resident memory. The total pending gauge
        // includes transient UU spill files and is deliberately soft-only so
        // a missing prefix can still dispatch and make the spool drain.
        let write_bytes = self.metrics.write_buffered_bytes.load(Ordering::Relaxed);
        let write_pending_bytes = self
            .metrics
            .write_pending_bytes
            .load(Ordering::Relaxed)
            .max(write_bytes);
        let uu_spool_admission_capped = self.uu_spool_admission_capped(0);

        if decode_bytes >= decode_hard {
            self.download_decode_hard_pressure_latched = true;
        } else if decode_bytes < decode_soft {
            self.download_decode_hard_pressure_latched = false;
        }
        if write_bytes >= write_hard {
            self.download_write_hard_pressure_latched = true;
        } else if write_bytes < write_soft {
            self.download_write_hard_pressure_latched = false;
        }

        let decode_hard_pressure = self.download_decode_hard_pressure_latched;
        let write_hard_pressure = self.download_write_hard_pressure_latched;
        let decode_soft_pressure = decode_bytes >= decode_soft;
        let write_soft_pressure = write_pending_bytes >= write_soft;

        let (state, reason) = if decode_hard_pressure || write_hard_pressure {
            (
                DownloadPressureState::Hard,
                Self::pressure_reason(decode_hard_pressure, write_hard_pressure),
            )
        } else if decode_soft_pressure || write_soft_pressure {
            (
                DownloadPressureState::Soft,
                Self::pressure_reason(decode_soft_pressure, write_soft_pressure),
            )
        } else {
            (DownloadPressureState::Clear, DownloadPressureReason::None)
        };

        self.metrics
            .decode_pressure_soft_limit_bytes
            .store(decode_soft, Ordering::Relaxed);
        self.metrics
            .decode_pressure_hard_limit_bytes
            .store(decode_hard, Ordering::Relaxed);
        self.metrics
            .write_pressure_soft_limit_bytes
            .store(write_soft, Ordering::Relaxed);
        self.metrics
            .write_pressure_hard_limit_bytes
            .store(write_hard, Ordering::Relaxed);
        self.metrics
            .download_pressure_state
            .store(state.as_code(), Ordering::Relaxed);
        self.metrics
            .download_pressure_reason
            .store(reason.as_code(), Ordering::Relaxed);
        self.update_download_pressure_stall_metrics(state);

        DownloadPressure {
            state,
            reason,
            decode_backlog_bytes: decode_bytes,
            write_buffered_bytes: write_bytes,
            write_pending_bytes,
            uu_spool_admission_capped,
            decode_hard_limit_bytes: decode_hard,
            write_hard_limit_bytes: write_hard,
        }
    }
}
