use std::time::Instant;

use super::*;

impl Pipeline {
    /// Handle a completed decode — write to disk, update assembly, journal.
    pub(super) async fn handle_decode_done(&mut self, result: DecodeResult) {
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

        // Look up the filename and working dir for disk I/O.
        let (filename, working_dir) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file(file_id) else {
                warn!(file_id = %file_id, "file assembly not found");
                return;
            };
            (file_asm.filename().to_string(), state.working_dir.clone())
        };

        // Insert into the per-file write reorder buffer so segments are
        // written sequentially even when connections deliver out of order.
        let write_buf = self
            .write_buffers
            .entry(file_id)
            .or_insert_with(|| WriteReorderBuffer::new(self.write_buf_max_pending));
        let segments_to_write = write_buf.insert(file_offset, data);

        // Write the sequentially-ordered segments to disk.
        let file_path = working_dir.join(&filename);
        let write_start = Instant::now();
        for (offset, data) in &segments_to_write {
            if let Err(e) = write_segment_to_disk(&file_path, *offset, data.as_slice()).await {
                warn!(
                    file = %filename,
                    offset = offset,
                    error = %e,
                    "disk write failed"
                );
                return;
            }
        }
        let write_us = write_start.elapsed().as_micros() as u64;
        self.metrics
            .disk_write_latency_us
            .store(write_us, Ordering::Relaxed);

        // Update assembly state.
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let Some(file_asm) = state.assembly.file_mut(file_id) else {
            return;
        };

        let segment_number = segment_id.segment_number;

        match file_asm.commit_segment(segment_number, decoded_size) {
            Ok(commit) => {
                self.metrics
                    .bytes_committed
                    .fetch_add(decoded_size as u64, Ordering::Relaxed);
                self.metrics
                    .segments_committed
                    .fetch_add(1, Ordering::Relaxed);

                let _ = self
                    .event_tx
                    .send(PipelineEvent::SegmentCommitted { segment_id });

                // Batch segment commit for SQLite persistence.
                self.segment_batch.push(CommittedSegment {
                    job_id,
                    file_index: segment_id.file_id.file_index,
                    segment_number: segment_id.segment_number,
                    file_offset,
                    decoded_size,
                    crc32,
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

                if commit.file_complete {
                    // Flush any remaining buffered segments to disk.
                    if let Some(mut write_buf) = self.write_buffers.remove(&file_id) {
                        let file_path = working_dir.join(&filename);
                        for (offset, data) in write_buf.flush_all().iter() {
                            if let Err(e) =
                                write_segment_to_disk(&file_path, *offset, data.as_slice()).await
                            {
                                warn!(
                                    file = %filename,
                                    offset = offset,
                                    error = %e,
                                    "disk write failed during flush"
                                );
                            }
                        }
                    }

                    let filename = file_asm.filename().to_string();
                    let total_bytes = file_asm.total_bytes();

                    // Check for yEnc name mismatch (observability for NZB swap detection).
                    if !yenc_name.is_empty() && yenc_name != filename {
                        warn!(
                            job_id = job_id.0,
                            assembly = %filename,
                            yenc = %yenc_name,
                            "yEnc name disagrees with assembly filename"
                        );
                    }

                    info!(file_id = %file_id, filename = %filename, "file complete");
                    let _ = self.event_tx.send(PipelineEvent::FileComplete {
                        file_id,
                        filename: filename.clone(),
                        total_bytes,
                    });

                    // Flush pending segment batch before recording file completion
                    // (inline to avoid borrow conflict with self.jobs).
                    if !self.segment_batch.is_empty() {
                        let batch = std::mem::take(&mut self.segment_batch);
                        let db = self.db.clone();
                        tokio::task::spawn_blocking(move || {
                            if let Err(e) = db.commit_segments(&batch) {
                                tracing::error!(count = batch.len(), error = %e, "failed to commit segment batch");
                            }
                        });
                    }

                    // Record file completion in SQLite.
                    if let Err(e) =
                        self.db
                            .complete_file(job_id, file_id.file_index, &filename, &[0u8; 16])
                    {
                        error!(error = %e, "db write failed for complete_file");
                    }

                    // If this is a PAR2 index file, parse and retain for repair.
                    self.try_load_par2_metadata(job_id, file_id).await;

                    // If this is a PAR2 recovery volume, merge its data into the retained set.
                    self.try_merge_par2_recovery(job_id, file_id).await;

                    // If this is a RAR volume, update archive topology.
                    self.try_update_archive_topology(job_id, file_id).await;
                    debug!(job_id = job_id.0, "post-topology");

                    // If this is a 7z file, update 7z topology.
                    self.try_update_7z_topology(job_id, file_id);

                    // Try partial extraction if some archive members are ready.
                    debug!(job_id = job_id.0, "entering try_partial_extraction");
                    self.try_partial_extraction(job_id).await;
                    debug!(job_id = job_id.0, "post-extraction");

                    self.check_job_completion(job_id).await;
                    debug!(job_id = job_id.0, "post-completion-check");
                }
            }
            Err(e) => {
                warn!(
                    segment = %segment_id,
                    error = %e,
                    "assembly commit failed"
                );
            }
        }
    }
}
