use std::time::Instant;

use super::*;

impl Pipeline {
    pub(crate) fn note_decode_started(&mut self, job_id: JobId) {
        *self.active_decodes_by_job.entry(job_id).or_default() += 1;
    }

    fn note_decode_finished(&mut self, job_id: JobId) {
        let Some(active) = self.active_decodes_by_job.get_mut(&job_id) else {
            return;
        };
        *active = active.saturating_sub(1);
        if *active == 0 {
            self.active_decodes_by_job.remove(&job_id);
        }
    }

    pub(crate) async fn flush_quiescent_write_backlog(&mut self) {
        if self.active_downloads > 0
            || !self.pending_decode.is_empty()
            || self.metrics.decode_pending.load(Ordering::Relaxed) > 0
        {
            return;
        }

        let stalled_jobs: Vec<JobId> = self
            .jobs
            .iter()
            .filter_map(|(job_id, state)| {
                if is_terminal_status(&state.status) || !state.download_queue.is_empty() {
                    return None;
                }
                let has_buffered_segments = self
                    .write_buffers
                    .keys()
                    .any(|file_id| file_id.job_id == *job_id);
                has_buffered_segments.then_some(*job_id)
            })
            .collect();

        for job_id in stalled_jobs {
            let file_ids: Vec<NzbFileId> = self
                .write_buffers
                .keys()
                .copied()
                .filter(|file_id| file_id.job_id == job_id)
                .collect();

            if file_ids.is_empty() {
                continue;
            }

            info!(
                job_id = job_id.0,
                files = file_ids.len(),
                "flushing quiescent write backlog"
            );

            for file_id in file_ids {
                loop {
                    let candidate = self
                        .write_buffers
                        .get_mut(&file_id)
                        .and_then(WriteReorderBuffer::take_oldest_buffered);
                    let Some((offset, segment)) = candidate else {
                        self.remove_empty_write_buffer(file_id);
                        break;
                    };

                    if let Err(e) = self
                        .persist_out_of_order_segment(file_id, offset, segment)
                        .await
                    {
                        warn!(
                            file_id = %file_id,
                            error = %e,
                            "failed to flush quiescent buffered segment"
                        );
                        break;
                    }
                }
            }
        }
    }

    /// Handle a completed decode — persist the segment, update assembly, journal.
    pub(crate) async fn handle_decode_done(&mut self, result: DecodeDone) {
        self.metrics.decode_pending.fetch_sub(1, Ordering::Relaxed);

        let job_id = match &result {
            DecodeDone::Success(result) => result.segment_id.file_id.job_id,
            DecodeDone::Failed { segment_id, .. } => segment_id.file_id.job_id,
        };
        self.note_decode_finished(job_id);

        match result {
            DecodeDone::Success(result) => self.handle_decode_success(result).await,
            DecodeDone::Failed { segment_id, error } => {
                self.handle_decode_failure(segment_id, &error);
            }
        }

        self.pump_decode_queue();
    }

    /// Handle a decode failure by re-queuing the segment for re-download.
    ///
    /// yEnc decode failures (CRC/size mismatch, malformed data) indicate the
    /// article body was corrupted — either in transit or on the server. Following
    /// NZBGet's approach, we re-download the segment (which may hit a different
    /// server via the connection pool's failover logic). After `MAX_SEGMENT_RETRIES`
    /// decode failures for the same segment, mark it as permanently failed and
    /// update health.
    pub(crate) fn handle_decode_failure(&mut self, segment_id: SegmentId, error: &str) {
        let job_id = segment_id.file_id.job_id;

        if self
            .jobs
            .get(&job_id)
            .is_none_or(|state| is_terminal_status(&state.status))
        {
            debug!(
                segment = %segment_id,
                error,
                "decode failed for inactive job — not retrying"
            );
            return;
        }

        let retries = self
            .decode_retries
            .entry(segment_id)
            .and_modify(|c| *c += 1)
            .or_insert(1);
        let retry_count = *retries;

        if retry_count > MAX_SEGMENT_RETRIES {
            warn!(
                segment = %segment_id,
                error,
                retries = MAX_SEGMENT_RETRIES,
                "decode failed permanently after max retries"
            );
            self.metrics
                .segments_failed_permanent
                .fetch_add(1, Ordering::Relaxed);
            if let Some(state) = self.jobs.get_mut(&job_id) {
                let file_idx = segment_id.file_id.file_index as usize;
                if let Some(file_spec) = state.spec.files.get(file_idx)
                    && let Some(seg_spec) = file_spec
                        .segments
                        .iter()
                        .find(|s| s.number == segment_id.segment_number)
                {
                    state.failed_bytes += seg_spec.bytes as u64;
                }
            }
            self.check_health(job_id);
            return;
        }

        // Re-queue for download — the NNTP pool may select a different server.
        if let Some(state) = self.jobs.get(&job_id) {
            let file_idx = segment_id.file_id.file_index as usize;
            if let Some(file_spec) = state.spec.files.get(file_idx)
                && let Some(seg_spec) = file_spec
                    .segments
                    .iter()
                    .find(|s| s.number == segment_id.segment_number)
            {
                // Exclude servers that have already returned bad data for this
                // segment. On decode retry N, exclude the first N servers in
                // priority order so each retry tries a different server.
                let exclude: Vec<usize> = (0..retry_count as usize).collect();
                let work = DownloadWork {
                    segment_id,
                    message_id: crate::jobs::ids::MessageId::new(&seg_spec.message_id),
                    groups: file_spec.groups.clone(),
                    priority: file_spec.role.download_priority(),
                    byte_estimate: seg_spec.bytes,
                    retry_count: 0,
                    is_recovery: file_spec.role.is_recovery(),
                    exclude_servers: exclude,
                };
                self.metrics
                    .segments_retried
                    .fetch_add(1, Ordering::Relaxed);
                let delay = std::time::Duration::from_secs(1 << (retry_count - 1));
                self.note_retry_scheduled(job_id);
                warn!(
                    segment = %segment_id,
                    error,
                    decode_retry = retry_count,
                    delay_secs = delay.as_secs(),
                    "decode failed — re-downloading"
                );
                let retry_tx = self.retry_tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let _ = retry_tx.send(work).await;
                });
            }
        }
    }

    pub(crate) async fn handle_decode_success(&mut self, result: DecodeResult) {
        let DecodeResult {
            segment_id,
            file_offset,
            decoded_size,
            crc_valid,
            crc32,
            data,
            yenc_name,
        } = result;

        let job_id = segment_id.file_id.job_id;
        let file_id = segment_id.file_id;

        if self
            .jobs
            .get(&job_id)
            .is_none_or(|state| is_terminal_status(&state.status))
        {
            debug!(
                job_id = job_id.0,
                segment = %segment_id,
                "discarding decode result for inactive job"
            );
            return;
        }

        self.note_recovery_count_from_yenc_name(job_id, file_id.file_index, &yenc_name);

        if !crc_valid {
            self.metrics.crc_errors.fetch_add(1, Ordering::Relaxed);
        }

        let _ = self.event_tx.send(PipelineEvent::SegmentDecoded {
            segment_id,
            decoded_size,
            file_offset,
            crc_valid,
        });

        // Track decoded (not raw/yEnc-encoded) bytes so progress never exceeds 100%.
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.downloaded_bytes += decoded_size as u64;
        }

        let buffered_segment = BufferedDecodedSegment {
            segment_id,
            decoded_size,
            crc32,
            data,
            yenc_name,
        };
        let buffered_len = buffered_segment.len_bytes();

        let ready = {
            let write_buf = self
                .write_buffers
                .entry(file_id)
                .or_insert_with(|| WriteReorderBuffer::new(self.write_buf_max_pending));
            write_buf.insert(file_offset, buffered_segment);
            write_buf.drain_ready_with_contiguous_end()
        };
        self.note_write_buffered(buffered_len, 1);

        if let Err(e) = self.persist_ready_segments(file_id, ready.0, ready.1).await {
            warn!(
                file_id = %file_id,
                error = %e,
                "disk write failed for sequential decoded segments"
            );
            return;
        }

        if let Err(e) = self.enforce_file_write_backlog(file_id).await {
            warn!(
                file_id = %file_id,
                error = %e,
                "failed to relieve per-file write backlog"
            );
            return;
        }

        if let Err(e) = self.relieve_global_write_backlog().await {
            warn!(error = %e, "failed to relieve global write backlog");
        }
    }

    async fn persist_ready_segments(
        &mut self,
        file_id: NzbFileId,
        ready: Vec<(u64, BufferedDecodedSegment)>,
        contiguous_end_after_ready: u64,
    ) -> Result<(), std::io::Error> {
        if ready.is_empty() {
            self.remove_empty_write_buffer(file_id);
            return Ok(());
        }

        let Some((_job_id, filename, working_dir, file_path)) = self.write_target_for_file(file_id)
        else {
            let released_bytes = ready.iter().map(|(_, segment)| segment.len_bytes()).sum();
            self.release_write_buffered(released_bytes, ready.len());
            self.remove_empty_write_buffer(file_id);
            return Ok(());
        };

        let write_start = Instant::now();
        for (offset, segment) in ready {
            let segment_bytes = segment.len_bytes();
            let write_result =
                write_segment_to_disk(&file_path, offset, segment.data.as_slice()).await;
            self.release_write_buffered(segment_bytes, 1);
            write_result?;
            crate::e2e_failpoint::maybe_trip("download.after_disk_write_before_commit");
            self.commit_persisted_segment(offset, segment, &filename, &working_dir)
                .await;
        }
        self.note_file_progress_floor(file_id, contiguous_end_after_ready, false);
        let write_us = write_start.elapsed().as_micros() as u64;
        self.metrics
            .disk_write_latency_us
            .store(write_us, Ordering::Relaxed);

        self.remove_empty_write_buffer(file_id);
        Ok(())
    }

    async fn enforce_file_write_backlog(
        &mut self,
        file_id: NzbFileId,
    ) -> Result<(), std::io::Error> {
        loop {
            let to_persist = {
                let Some(write_buf) = self.write_buffers.get_mut(&file_id) else {
                    return Ok(());
                };
                if !write_buf.exceeds_max_pending() {
                    return Ok(());
                }
                write_buf.take_oldest_buffered()
            };

            let Some((offset, segment)) = to_persist else {
                self.remove_empty_write_buffer(file_id);
                return Ok(());
            };
            self.persist_out_of_order_segment(file_id, offset, segment)
                .await?;
        }
    }

    async fn relieve_global_write_backlog(&mut self) -> Result<(), std::io::Error> {
        while self.write_buffered_bytes > self.write_backlog_budget_bytes {
            let candidate_file = self
                .write_buffers
                .iter()
                .filter(|(_, write_buf)| write_buf.buffered_len() > 0)
                .max_by_key(|(_, write_buf)| write_buf.buffered_bytes())
                .map(|(file_id, _)| *file_id);

            let Some(file_id) = candidate_file else {
                break;
            };

            let candidate = self
                .write_buffers
                .get_mut(&file_id)
                .and_then(WriteReorderBuffer::take_oldest_buffered);
            let Some((offset, segment)) = candidate else {
                self.remove_empty_write_buffer(file_id);
                continue;
            };

            self.persist_out_of_order_segment(file_id, offset, segment)
                .await?;
        }

        Ok(())
    }

    async fn persist_out_of_order_segment(
        &mut self,
        file_id: NzbFileId,
        offset: u64,
        segment: BufferedDecodedSegment,
    ) -> Result<(), std::io::Error> {
        let segment_bytes = segment.len_bytes();
        let Some((_job_id, filename, working_dir, file_path)) = self.write_target_for_file(file_id)
        else {
            self.release_write_buffered(segment_bytes, 1);
            self.remove_empty_write_buffer(file_id);
            return Ok(());
        };

        let write_start = Instant::now();
        let write_result = write_segment_to_disk(&file_path, offset, segment.data.as_slice()).await;
        let write_us = write_start.elapsed().as_micros() as u64;
        self.metrics
            .disk_write_latency_us
            .store(write_us, Ordering::Relaxed);
        self.release_write_buffered(segment_bytes, 1);
        write_result?;
        crate::e2e_failpoint::maybe_trip("download.after_disk_write_before_commit");

        if let Some(write_buf) = self.write_buffers.get_mut(&file_id) {
            write_buf.mark_persisted(offset, segment_bytes);
        }
        self.metrics
            .direct_write_evictions
            .fetch_add(1, Ordering::Relaxed);

        self.commit_persisted_segment(offset, segment, &filename, &working_dir)
            .await;
        self.remove_empty_write_buffer(file_id);
        Ok(())
    }

    fn remove_empty_write_buffer(&mut self, file_id: NzbFileId) {
        let should_remove = self
            .write_buffers
            .get(&file_id)
            .is_some_and(WriteReorderBuffer::is_empty);
        if !should_remove {
            return;
        }

        let file_complete = self.jobs.get(&file_id.job_id).is_none_or(|state| {
            state
                .assembly
                .file(file_id)
                .is_none_or(crate::jobs::assembly::FileAssembly::is_complete)
        });

        // Preserve the per-file write cursor until the file is actually complete.
        // Otherwise a long in-order file resets to cursor 0 after every drain and
        // leaves its tail permanently buffered behind the max-pending window.
        if file_complete {
            self.write_buffers.remove(&file_id);
        }
    }

    async fn commit_persisted_segment(
        &mut self,
        file_offset: u64,
        segment: BufferedDecodedSegment,
        filename: &str,
        working_dir: &std::path::Path,
    ) {
        let job_id = segment.segment_id.file_id.job_id;
        let file_id = segment.segment_id.file_id;

        let commit_result = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file_mut(file_id) else {
                return;
            };

            match file_asm.commit_segment(segment.segment_id.segment_number, segment.decoded_size) {
                Ok(commit) => Ok((commit.file_complete, file_asm.total_bytes())),
                Err(e) => Err(e),
            }
        };

        match commit_result {
            Ok((file_complete, total_bytes)) => {
                self.metrics
                    .bytes_committed
                    .fetch_add(segment.decoded_size as u64, Ordering::Relaxed);
                self.metrics
                    .segments_committed
                    .fetch_add(1, Ordering::Relaxed);

                let _ = self.event_tx.send(PipelineEvent::SegmentCommitted {
                    segment_id: segment.segment_id,
                });

                self.segment_batch.push(CommittedSegment {
                    job_id,
                    file_index: segment.segment_id.file_id.file_index,
                    segment_number: segment.segment_id.segment_number,
                    file_offset,
                    decoded_size: segment.decoded_size,
                    crc32: segment.crc32,
                });
                if self.segment_batch.len() >= 100 {
                    let batch = std::mem::take(&mut self.segment_batch);
                    let db = self.db.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = db.commit_segments(&batch) {
                            tracing::error!(count = batch.len(), error = %e, "failed to commit segment batch");
                        } else {
                            crate::e2e_failpoint::maybe_trip(
                                "download.after_batch_commit_before_ack",
                            );
                        }
                    });
                }

                if file_complete {
                    self.note_file_progress_floor(file_id, total_bytes, true);
                    if let Some(mut write_buf) = self.write_buffers.remove(&file_id) {
                        let leftovers = write_buf.flush_all();
                        if !leftovers.is_empty() {
                            warn!(
                                file_id = %file_id,
                                leftover_segments = leftovers.len(),
                                "file reached complete state with buffered decoded segments still pending; flushing directly"
                            );
                            let file_path = working_dir.join(filename);
                            for (offset, buffered) in leftovers {
                                let buffered_bytes = buffered.len_bytes();
                                if let Err(e) = write_segment_to_disk(
                                    &file_path,
                                    offset,
                                    buffered.data.as_slice(),
                                )
                                .await
                                {
                                    warn!(
                                        file = %filename,
                                        offset,
                                        error = %e,
                                        "disk write failed during final buffered flush"
                                    );
                                }
                                self.release_write_buffered(buffered_bytes, 1);
                            }
                        }
                    }

                    if !segment.yenc_name.is_empty() && segment.yenc_name != filename {
                        warn!(
                            job_id = job_id.0,
                            assembly = %filename,
                            yenc = %segment.yenc_name,
                            "yEnc name disagrees with assembly filename"
                        );
                    }

                    info!(file_id = %file_id, filename = %filename, "file complete");
                    let _ = self.event_tx.send(PipelineEvent::FileComplete {
                        file_id,
                        filename: filename.to_string(),
                        total_bytes,
                    });

                    if !self.segment_batch.is_empty() {
                        let batch = std::mem::take(&mut self.segment_batch);
                        let db = self.db.clone();
                        tokio::task::spawn_blocking(move || {
                            if let Err(e) = db.commit_segments(&batch) {
                                tracing::error!(count = batch.len(), error = %e, "failed to commit segment batch");
                            } else {
                                crate::e2e_failpoint::maybe_trip(
                                    "download.after_batch_commit_before_ack",
                                );
                            }
                        });
                    }

                    {
                        let file_index = file_id.file_index;
                        let fname = filename.to_string();
                        if let Err(e) = self
                            .db_blocking(move |db| {
                                db.complete_file(job_id, file_index, &fname, &[0u8; 16])
                            })
                            .await
                        {
                            error!(error = %e, "db write failed for complete_file");
                        }
                    }
                    self.pending_file_progress.remove(&file_id);
                    self.persisted_file_progress.remove(&file_id);

                    self.try_load_par2_metadata(job_id, file_id).await;
                    self.try_merge_par2_recovery(job_id, file_id).await;
                    self.refresh_archive_state_for_completed_file(job_id, file_id, true)
                        .await;
                    self.retry_par2_authoritative_identity(job_id).await;
                    debug!(job_id = job_id.0, "post-topology");
                    debug!(job_id = job_id.0, "entering try_rar_extraction");
                    self.try_rar_extraction(job_id).await;
                    debug!(job_id = job_id.0, "post-extraction");
                    self.check_job_completion(job_id).await;
                    debug!(job_id = job_id.0, "post-completion-check");
                }
            }
            Err(e) => {
                warn!(
                    segment = %segment.segment_id,
                    error = %e,
                    "assembly commit failed"
                );
            }
        }
    }
}
