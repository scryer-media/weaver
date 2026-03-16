use super::*;

impl Pipeline {
    pub(super) fn flush_quiescent_write_backlog(&mut self) {
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
                    self.send_evicted_segment_to_writer(file_id, offset, segment);
                }
            }
        }
    }

    /// Handle a completed decode — persist the segment, update assembly, journal.
    pub(super) async fn handle_decode_done(&mut self, result: DecodeDone) {
        self.metrics.decode_pending.fetch_sub(1, Ordering::Relaxed);

        match result {
            DecodeDone::Success(result) => self.handle_decode_success(result).await,
            DecodeDone::Failed { segment_id, error } => {
                debug!(
                    segment = %segment_id,
                    error = %error,
                    "decode work finished with failure"
                );
            }
        }

        self.pump_decode_queue();
    }

    async fn handle_decode_success(&mut self, result: DecodeResult) {
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
            write_buf.drain_ready()
        };
        self.note_write_buffered(buffered_len, 1);

        self.send_ready_segments_to_writer(file_id, ready);

        self.enforce_file_write_backlog(file_id);

        self.relieve_global_write_backlog();
    }

    /// Send ready segments to the background writer task. Returns immediately —
    /// the pipeline loop is never blocked on disk I/O.
    fn send_ready_segments_to_writer(
        &mut self,
        file_id: NzbFileId,
        ready: Vec<(u64, BufferedDecodedSegment)>,
    ) {
        if ready.is_empty() {
            self.remove_empty_write_buffer(file_id);
            return;
        }

        let Some((_job_id, _filename, _working_dir, file_path)) =
            self.write_target_for_file(file_id)
        else {
            let released_bytes: usize = ready.iter().map(|(_, segment)| segment.len_bytes()).sum();
            self.release_write_buffered(released_bytes, ready.len());
            self.remove_empty_write_buffer(file_id);
            return;
        };

        let segments: Vec<_> = ready
            .into_iter()
            .map(|(offset, seg)| {
                let ci = SegmentCommitInfo {
                    segment_id: seg.segment_id,
                    decoded_size: seg.decoded_size,
                    crc32: seg.crc32,
                    yenc_name: seg.yenc_name,
                };
                (offset, seg.data.into_boxed_bytes(), ci)
            })
            .collect();

        let _ = self.write_req_tx.send(WriteMessage::Segments(WriteRequest {
            file_id,
            file_path,
            segments,
        }));
    }

    fn enforce_file_write_backlog(&mut self, file_id: NzbFileId) {
        loop {
            let to_evict = {
                let Some(write_buf) = self.write_buffers.get_mut(&file_id) else {
                    return;
                };
                if !write_buf.exceeds_max_pending() {
                    return;
                }
                write_buf.take_oldest_buffered()
            };

            let Some((offset, segment)) = to_evict else {
                self.remove_empty_write_buffer(file_id);
                return;
            };
            self.send_evicted_segment_to_writer(file_id, offset, segment);
        }
    }

    fn relieve_global_write_backlog(&mut self) {
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

            self.send_evicted_segment_to_writer(file_id, offset, segment);
        }
    }

    fn send_evicted_segment_to_writer(
        &mut self,
        file_id: NzbFileId,
        offset: u64,
        segment: BufferedDecodedSegment,
    ) {
        let segment_bytes = segment.len_bytes();
        let Some((_job_id, _filename, _working_dir, file_path)) =
            self.write_target_for_file(file_id)
        else {
            self.release_write_buffered(segment_bytes, 1);
            self.remove_empty_write_buffer(file_id);
            return;
        };

        if let Some(write_buf) = self.write_buffers.get_mut(&file_id) {
            write_buf.mark_persisted(offset, segment_bytes);
        }
        self.metrics
            .direct_write_evictions
            .fetch_add(1, Ordering::Relaxed);

        let ci = SegmentCommitInfo {
            segment_id: segment.segment_id,
            decoded_size: segment.decoded_size,
            crc32: segment.crc32,
            yenc_name: segment.yenc_name,
        };
        let _ = self.write_req_tx.send(WriteMessage::Segments(WriteRequest {
            file_id,
            file_path,
            segments: vec![(offset, segment.data.into_boxed_bytes(), ci)],
        }));
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
                .is_none_or(weaver_assembly::FileAssembly::is_complete)
        });

        // Preserve the per-file write cursor until the file is actually complete.
        // Otherwise a long in-order file resets to cursor 0 after every drain and
        // leaves its tail permanently buffered behind the max-pending window.
        if file_complete {
            self.write_buffers.remove(&file_id);
        }
    }

    /// Handle completed disk writes from the background writer task.
    pub(super) async fn handle_write_complete(&mut self, complete: WriteComplete) {
        match complete {
            WriteComplete::Barrier { id } => {
                // All writes submitted before this barrier are durable.
                // Recompute RAR state (now that all segment writes are on disk)
                // then make irreversible deletion decisions.
                if let Some(pending) = self.pending_delete_barriers.remove(&id) {
                    for (job_id, set_name) in pending {
                        if let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await {
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                error,
                                "failed to recompute RAR state after write barrier"
                            );
                        }
                        self.try_delete_volumes(job_id, &set_name);
                    }
                }
            }
            WriteComplete::Segments {
                file_id,
                written,
                bytes_written,
                segments_written,
                write_latency_us,
                error,
            } => {
                self.release_write_buffered(bytes_written, segments_written);
                self.metrics
                    .disk_write_latency_us
                    .store(write_latency_us, Ordering::Relaxed);

                if let Some(ref error) = error {
                    warn!(
                        file_id = %file_id,
                        error,
                        "background disk write failed"
                    );
                    return;
                }

                let Some((_job_id, filename, working_dir, _file_path)) =
                    self.write_target_for_file(file_id)
                else {
                    return;
                };

                for (offset, info) in written {
                    self.commit_persisted_segment(offset, info, &filename, &working_dir)
                        .await;
                }

                self.remove_empty_write_buffer(file_id);
            }
        }
    }

    async fn commit_persisted_segment(
        &mut self,
        file_offset: u64,
        info: SegmentCommitInfo,
        filename: &str,
        working_dir: &std::path::Path,
    ) {
        let job_id = info.segment_id.file_id.job_id;
        let file_id = info.segment_id.file_id;

        let commit_result = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file_mut(file_id) else {
                return;
            };

            match file_asm.commit_segment(info.segment_id.segment_number, info.decoded_size) {
                Ok(commit) => Ok((commit.file_complete, file_asm.total_bytes())),
                Err(e) => Err(e),
            }
        };

        match commit_result {
            Ok((file_complete, total_bytes)) => {
                self.metrics
                    .bytes_committed
                    .fetch_add(info.decoded_size as u64, Ordering::Relaxed);
                self.metrics
                    .segments_committed
                    .fetch_add(1, Ordering::Relaxed);

                let _ = self.event_tx.send(PipelineEvent::SegmentCommitted {
                    segment_id: info.segment_id,
                });

                self.segment_batch.push(CommittedSegment {
                    job_id,
                    file_index: info.segment_id.file_id.file_index,
                    segment_number: info.segment_id.segment_number,
                    file_offset,
                    decoded_size: info.decoded_size,
                    crc32: info.crc32,
                });
                if self.segment_batch.len() >= 100 {
                    let batch = std::mem::take(&mut self.segment_batch);
                    let db = self.db.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = db.commit_segments(&batch) {
                            tracing::error!(count = batch.len(), error = %e, "failed to commit segment batch");
                        }
                    });
                }

                if file_complete {
                    if let Some(mut write_buf) = self.write_buffers.remove(&file_id) {
                        let leftovers = write_buf.flush_all();
                        if !leftovers.is_empty() {
                            warn!(
                                file_id = %file_id,
                                leftover_segments = leftovers.len(),
                                "file reached complete state with buffered decoded segments still pending; flushing via writer"
                            );
                            let file_path = working_dir.join(filename);
                            let segments: Vec<_> = leftovers
                                .into_iter()
                                .map(|(offset, seg)| {
                                    let ci = SegmentCommitInfo {
                                        segment_id: seg.segment_id,
                                        decoded_size: seg.decoded_size,
                                        crc32: seg.crc32,
                                        yenc_name: seg.yenc_name,
                                    };
                                    (offset, seg.data.into_boxed_bytes(), ci)
                                })
                                .collect();
                            let _ = self.write_req_tx.send(WriteMessage::Segments(WriteRequest {
                                file_id,
                                file_path,
                                segments,
                            }));
                        }
                    }

                    if !info.yenc_name.is_empty() && info.yenc_name != filename {
                        warn!(
                            job_id = job_id.0,
                            assembly = %filename,
                            yenc = %info.yenc_name,
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

                    self.try_load_par2_metadata(job_id, file_id).await;
                    self.try_merge_par2_recovery(job_id, file_id).await;
                    self.try_update_archive_topology(job_id, file_id).await;
                    debug!(job_id = job_id.0, "post-topology");
                    self.try_update_7z_topology(job_id, file_id);
                    debug!(job_id = job_id.0, "entering try_partial_extraction");
                    self.try_partial_extraction(job_id).await;
                    debug!(job_id = job_id.0, "post-extraction");
                    self.check_job_completion(job_id).await;
                    debug!(job_id = job_id.0, "post-completion-check");
                }
            }
            Err(e) => {
                warn!(
                    segment = %info.segment_id,
                    error = %e,
                    "assembly commit failed"
                );
            }
        }
    }
}
