use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::time::Instant;

use super::*;

const MAX_DEFERRED_FILE_HASH_DATA_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
enum SegmentHashMode {
    UpdateNow,
    DeferRange,
}

#[derive(Clone, Copy, Debug)]
enum OutOfOrderPersistReason {
    PerFileMaxPending,
    GlobalWriteBacklog,
    QuiescentFlush,
}

impl OutOfOrderPersistReason {
    fn profile_bucket(self) -> &'static str {
        match self {
            Self::PerFileMaxPending => "download.write_buffer.out_of_order.per_file_max_pending",
            Self::GlobalWriteBacklog => "download.write_buffer.out_of_order.global_write_backlog",
            Self::QuiescentFlush => "download.write_buffer.out_of_order.quiescent_flush",
        }
    }
}

#[derive(Debug)]
struct SegmentWriteError {
    file_id: NzbFileId,
    source: io::Error,
}

impl SegmentWriteError {
    fn new(file_id: NzbFileId, source: io::Error) -> Self {
        Self { file_id, source }
    }
}

impl fmt::Display for SegmentWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.file_id, self.source)
    }
}

impl std::error::Error for SegmentWriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

impl Pipeline {
    pub(crate) fn yenc_name_matches_rewritten_source(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
        yenc_name: &str,
        current_filename: &str,
    ) -> bool {
        self.file_identity(job_id, file_id).is_some_and(|identity| {
            identity.source_filename == yenc_name
                && identity.current_filename == current_filename
                && identity.source_filename != identity.current_filename
        })
    }

    pub(crate) fn note_file_hash_chunk(
        &mut self,
        file_id: NzbFileId,
        file_offset: u64,
        data: &[u8],
        part_crc: u32,
        part_crc_verified: bool,
    ) {
        let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.file_hash.update");
        if self.file_hash_reread_required.contains(&file_id) {
            return;
        }

        let expected_offset = self
            .file_hash_states
            .get(&file_id)
            .map(|state| state.bytes_fed())
            .unwrap_or(0);
        if expected_offset != file_offset {
            self.mark_file_hash_reread_required_for(file_id, "offset_mismatch");
            return;
        }

        let track_md5 = self.should_stream_md5_for_file(file_id);
        self.file_hash_states.entry(file_id).or_default().update(
            data,
            part_crc,
            part_crc_verified,
            track_md5,
        );
    }

    fn note_file_hash_decoded_chunk(
        &mut self,
        file_id: NzbFileId,
        file_offset: u64,
        data: &DecodedChunk,
        part_crc: u32,
        part_crc_verified: bool,
    ) {
        let total_len = data.len_bytes();
        if total_len == 0 {
            return;
        }

        let end_offset = file_offset.saturating_add(total_len as u64);
        let mut next_offset = file_offset;
        data.for_each_slice(|slice| {
            if slice.is_empty() {
                return;
            }
            let slice_end = next_offset.saturating_add(slice.len() as u64);
            self.note_file_hash_chunk(
                file_id,
                next_offset,
                slice,
                part_crc,
                part_crc_verified && slice_end == end_offset,
            );
            next_offset = slice_end;
        });
    }

    fn defer_file_hash_chunk(
        &mut self,
        file_id: NzbFileId,
        file_offset: u64,
        data: DecodedChunk,
        part_crc: u32,
        part_crc_verified: bool,
    ) {
        let len = data.len_bytes();
        if len == 0 || self.file_hash_reread_required.contains(&file_id) {
            return;
        }
        crate::runtime::perf_probe::record(
            "download.file_hash.deferred_range",
            std::time::Duration::from_nanos(1),
        );
        self.deferred_file_hash_ranges
            .entry(file_id)
            .or_default()
            .insert(file_offset, len);

        if self.deferred_file_hash_data_bytes.saturating_add(len)
            <= MAX_DEFERRED_FILE_HASH_DATA_BYTES
        {
            let previous = self
                .deferred_file_hash_data
                .entry(file_id)
                .or_default()
                .insert(
                    file_offset,
                    DeferredFileHashChunk {
                        data,
                        part_crc,
                        part_crc_verified,
                    },
                );
            if let Some(previous) = previous {
                self.deferred_file_hash_data_bytes = self
                    .deferred_file_hash_data_bytes
                    .saturating_sub(previous.len_bytes());
            }
            self.deferred_file_hash_data_bytes += len;
            crate::runtime::perf_probe::record(
                "download.file_hash.deferred_data_stored",
                std::time::Duration::from_nanos(1),
            );
        } else {
            crate::runtime::perf_probe::record(
                "download.file_hash.deferred_data_skipped_capacity",
                std::time::Duration::from_nanos(1),
            );
        }
    }

    fn take_deferred_file_hash_data(
        &mut self,
        file_id: NzbFileId,
        file_offset: u64,
    ) -> Option<DeferredFileHashChunk> {
        let chunk = self
            .deferred_file_hash_data
            .get_mut(&file_id)
            .and_then(|chunks| chunks.remove(&file_offset))?;
        if self
            .deferred_file_hash_data
            .get(&file_id)
            .is_some_and(BTreeMap::is_empty)
        {
            self.deferred_file_hash_data.remove(&file_id);
        }
        self.deferred_file_hash_data_bytes = self
            .deferred_file_hash_data_bytes
            .saturating_sub(chunk.len_bytes());
        Some(chunk)
    }

    fn drop_deferred_file_hash_data_for(&mut self, file_id: NzbFileId) {
        let Some(chunks) = self.deferred_file_hash_data.remove(&file_id) else {
            return;
        };
        let removed = chunks
            .into_values()
            .map(|chunk| chunk.len_bytes())
            .sum::<usize>();
        self.deferred_file_hash_data_bytes =
            self.deferred_file_hash_data_bytes.saturating_sub(removed);
    }

    async fn drain_deferred_file_hash_ranges(
        &mut self,
        file_id: NzbFileId,
        file_path: &std::path::Path,
    ) {
        loop {
            if self.file_hash_reread_required.contains(&file_id) {
                return;
            }

            let expected_offset = self
                .file_hash_states
                .get(&file_id)
                .map(|state| state.bytes_fed())
                .unwrap_or(0);
            let Some(len) = self
                .deferred_file_hash_ranges
                .get(&file_id)
                .and_then(|ranges| ranges.get(&expected_offset).copied())
            else {
                return;
            };

            if let Some(ranges) = self.deferred_file_hash_ranges.get_mut(&file_id) {
                ranges.remove(&expected_offset);
                if ranges.is_empty() {
                    self.deferred_file_hash_ranges.remove(&file_id);
                }
            }
            crate::runtime::perf_probe::record(
                "download.file_hash.deferred_range_replayed",
                std::time::Duration::from_nanos(1),
            );

            if let Some(chunk) = self.take_deferred_file_hash_data(file_id, expected_offset) {
                if chunk.len_bytes() != len {
                    self.mark_file_hash_reread_required_for(file_id, "deferred_data_len_mismatch");
                    return;
                }
                crate::runtime::perf_probe::record(
                    "download.file_hash.deferred_data_replayed",
                    std::time::Duration::from_nanos(1),
                );
                self.note_file_hash_decoded_chunk(
                    file_id,
                    expected_offset,
                    &chunk.data,
                    chunk.part_crc,
                    chunk.part_crc_verified,
                );
                continue;
            }

            let path = file_path.to_path_buf();
            let read_result = tokio::task::spawn_blocking(move || {
                let _cpu_scope =
                    crate::runtime::perf_probe::cpu_scope("download.file_hash.deferred_range_read");
                read_file_range(&path, expected_offset, len)
            })
            .await;

            let bytes = match read_result {
                Ok(Ok(bytes)) => bytes,
                Ok(Err(error)) => {
                    warn!(
                        file_id = %file_id,
                        offset = expected_offset,
                        len,
                        error = %error,
                        "failed to read deferred file hash range; falling back to full-file checksum"
                    );
                    self.mark_file_hash_reread_required_for(file_id, "deferred_range_read_failed");
                    return;
                }
                Err(error) => {
                    warn!(
                        file_id = %file_id,
                        offset = expected_offset,
                        len,
                        error = %error,
                        "deferred file hash range task panicked; falling back to full-file checksum"
                    );
                    self.mark_file_hash_reread_required_for(file_id, "deferred_range_task_failed");
                    return;
                }
            };

            let part_crc = weaver_par2::checksum::crc32(&bytes);
            self.note_file_hash_chunk(file_id, expected_offset, &bytes, part_crc, false);
        }
    }

    fn should_stream_md5_for_file(&self, file_id: NzbFileId) -> bool {
        let Some(state) = self.jobs.get(&file_id.job_id) else {
            return true;
        };
        let Some(file) = state.assembly.file(file_id) else {
            return true;
        };
        if !matches!(
            self.classified_role_for_file(file_id.job_id, file),
            weaver_model::files::FileRole::RarVolume { .. }
        ) {
            return true;
        }
        self.par2_set(file_id.job_id).is_none()
    }

    pub(crate) fn note_expected_file_crc(
        &mut self,
        file_id: NzbFileId,
        expected_file_crc: Option<u32>,
    ) -> Result<(), String> {
        let Some(expected_file_crc) = expected_file_crc else {
            return Ok(());
        };

        match self.expected_file_crcs.entry(file_id) {
            std::collections::hash_map::Entry::Occupied(existing) => {
                if *existing.get() == expected_file_crc {
                    Ok(())
                } else {
                    Err(format!(
                        "conflicting yEnc whole-file CRC32 for {file_id}: {:08x} vs {:08x}",
                        existing.get(),
                        expected_file_crc
                    ))
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(expected_file_crc);
                Ok(())
            }
        }
    }

    fn mark_file_hash_reread_required_for(&mut self, file_id: NzbFileId, reason: &'static str) {
        self.file_hash_states.remove(&file_id);
        self.drop_deferred_file_hash_data_for(file_id);
        if self.file_hash_reread_required.insert(file_id) {
            crate::runtime::perf_probe::record_owned(
                format!("download.file_hash.reread_required.{reason}"),
                std::time::Duration::from_nanos(1),
            );
        }
    }

    pub(crate) async fn finalize_completed_file_hash(
        &mut self,
        file_id: NzbFileId,
        filename: &str,
        file_path: std::path::PathBuf,
        total_bytes: u64,
        expected_file_crc: Option<u32>,
    ) -> Result<CompletedFileChecksum, String> {
        self.drain_deferred_file_hash_ranges(file_id, &file_path)
            .await;
        let hash_state = self.file_hash_states.remove(&file_id);
        self.deferred_file_hash_ranges.remove(&file_id);
        self.drop_deferred_file_hash_data_for(file_id);
        let reread_required = self.file_hash_reread_required.remove(&file_id);
        if reread_required {
            crate::runtime::perf_probe::record(
                "download.file_hash.reread_fallback.marked_required",
                std::time::Duration::from_nanos(1),
            );
        } else {
            match hash_state {
                Some(hash_state) if hash_state.bytes_fed() == total_bytes => {
                    let streamed = hash_state.finalize();
                    if let Some(md5) = streamed.md5 {
                        return Ok(CompletedFileChecksum {
                            md5,
                            crc32: streamed.crc32,
                        });
                    }
                    if let Some(md5) = self.expected_par2_hash_for_fast_verified_rar_file(
                        file_id,
                        filename,
                        total_bytes,
                        expected_file_crc,
                        &streamed,
                    ) {
                        crate::runtime::perf_probe::record(
                            "download.file_hash.md5.par2_expected_fast_path",
                            std::time::Duration::from_nanos(1),
                        );
                        return Ok(CompletedFileChecksum {
                            md5,
                            crc32: streamed.crc32,
                        });
                    }
                    crate::runtime::perf_probe::record(
                        "download.file_hash.reread_fallback.md5_disabled_not_eligible",
                        std::time::Duration::from_nanos(1),
                    );
                }
                Some(_) => crate::runtime::perf_probe::record(
                    "download.file_hash.reread_fallback.incomplete_stream_state",
                    std::time::Duration::from_nanos(1),
                ),
                None => crate::runtime::perf_probe::record(
                    "download.file_hash.reread_fallback.no_stream_state",
                    std::time::Duration::from_nanos(1),
                ),
            }
        }

        tokio::task::spawn_blocking(move || checksum_completed_file(&file_path))
            .await
            .map_err(|error| format!("file checksum task panicked: {error}"))?
            .map_err(|error| format!("failed to checksum completed file: {error}"))
    }

    fn expected_par2_hash_for_fast_verified_rar_file(
        &self,
        file_id: NzbFileId,
        filename: &str,
        total_bytes: u64,
        expected_file_crc: Option<u32>,
        streamed: &StreamedCompletedFileChecksum,
    ) -> Option<[u8; 16]> {
        if let Some(expected_file_crc) = expected_file_crc {
            if streamed.crc32 != expected_file_crc {
                crate::runtime::perf_probe::record(
                    "download.file_hash.md5.fast_path_reject.file_crc_mismatch",
                    std::time::Duration::from_nanos(1),
                );
                return None;
            }
        } else {
            crate::runtime::perf_probe::record(
                "download.file_hash.md5.fast_path_without_file_crc",
                std::time::Duration::from_nanos(1),
            );
        }
        if !streamed.all_parts_crc_verified {
            crate::runtime::perf_probe::record(
                "download.file_hash.md5.fast_path_reject.unverified_part_crc",
                std::time::Duration::from_nanos(1),
            );
            return None;
        }

        let job_id = file_id.job_id;
        let Some(state) = self.jobs.get(&job_id) else {
            return None;
        };
        let Some(file) = state.assembly.file(file_id) else {
            return None;
        };
        if !matches!(
            self.classified_role_for_file(job_id, file),
            weaver_model::files::FileRole::RarVolume { .. }
        ) {
            crate::runtime::perf_probe::record(
                "download.file_hash.md5.fast_path_reject.not_rar",
                std::time::Duration::from_nanos(1),
            );
            return None;
        }
        let Some(par2_set) = self.par2_set(job_id) else {
            crate::runtime::perf_probe::record(
                "download.file_hash.md5.fast_path_reject.no_par2_metadata",
                std::time::Duration::from_nanos(1),
            );
            return None;
        };

        let mut candidate_names = BTreeSet::new();
        candidate_names.insert(weaver_model::files::sanitize_download_filename(filename));
        candidate_names.insert(weaver_model::files::sanitize_download_filename(
            file.filename(),
        ));
        if let Some(identity) = self.effective_file_identity(job_id, file_id) {
            candidate_names.insert(weaver_model::files::sanitize_download_filename(
                &identity.source_filename,
            ));
            candidate_names.insert(weaver_model::files::sanitize_download_filename(
                &identity.current_filename,
            ));
            if let Some(canonical) = identity.canonical_filename.as_ref() {
                candidate_names.insert(weaver_model::files::sanitize_download_filename(canonical));
            }
        }

        let mut hashes = par2_set
            .files
            .values()
            .filter(|desc| desc.length == total_bytes)
            .filter(|desc| {
                candidate_names.contains(&weaver_model::files::sanitize_download_filename(
                    &desc.filename,
                ))
            })
            .map(|desc| desc.hash_full)
            .collect::<Vec<_>>();
        hashes.sort_unstable();
        hashes.dedup();
        match hashes.as_slice() {
            [hash] => Some(*hash),
            [] => {
                crate::runtime::perf_probe::record(
                    "download.file_hash.md5.fast_path_reject.no_par2_match",
                    std::time::Duration::from_nanos(1),
                );
                None
            }
            _ => {
                crate::runtime::perf_probe::record(
                    "download.file_hash.md5.fast_path_reject.ambiguous_par2_match",
                    std::time::Duration::from_nanos(1),
                );
                None
            }
        }
    }

    pub(crate) fn note_decode_started(&mut self, segment_id: SegmentId) {
        let job_id = segment_id.file_id.job_id;
        *self.active_decodes_by_job.entry(job_id).or_default() += 1;
        *self
            .active_decodes_by_file
            .entry(segment_id.file_id)
            .or_default() += 1;
        self.publish_active_stage_metrics();
    }

    fn note_decode_finished(&mut self, segment_id: SegmentId) {
        let job_id = segment_id.file_id.job_id;
        if let Some(active) = self.active_decodes_by_job.get_mut(&job_id) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                self.active_decodes_by_job.remove(&job_id);
            }
        }
        if let Some(active) = self.active_decodes_by_file.get_mut(&segment_id.file_id) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                self.active_decodes_by_file.remove(&segment_id.file_id);
            }
        }
        self.publish_active_stage_metrics();
    }

    pub(in crate::pipeline) fn decode_retry_exclude_servers(
        existing_excludes: &[usize],
        source_server_idx: Option<usize>,
    ) -> Vec<usize> {
        let mut exclude_servers = existing_excludes.to_vec();
        if let Some(source_server_idx) = source_server_idx
            && !exclude_servers.contains(&source_server_idx)
        {
            exclude_servers.push(source_server_idx);
        }
        exclude_servers
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

            let mut flushed_segments = 0usize;
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

                    if let Err(error) = self
                        .persist_out_of_order_segment(
                            file_id,
                            offset,
                            segment,
                            OutOfOrderPersistReason::QuiescentFlush,
                        )
                        .await
                    {
                        self.fail_job_for_disk_write(
                            error,
                            "failed to flush quiescent buffered segment",
                        );
                        break;
                    }
                    flushed_segments += 1;
                }
            }

            if flushed_segments > 0 {
                self.schedule_job_completion_check_if_download_pipeline_drained(
                    job_id,
                    "quiescent_write_backlog_drained",
                );
            }
        }
    }

    /// Handle a completed decode — persist the segment, update assembly, journal.
    pub(crate) async fn handle_decode_done(&mut self, result: DecodeDone) {
        let _profile_scope = crate::runtime::perf_probe::scope("download.handle_decode_done");

        let (segment_id, raw_size) = match &result {
            DecodeDone::Success(result) => (result.segment_id, result.raw_size),
            DecodeDone::Failed {
                segment_id,
                raw_size,
                ..
            } => (*segment_id, *raw_size),
        };
        self.metrics.note_decode_task_finished(raw_size);
        self.note_decode_finished(segment_id);

        match result {
            DecodeDone::Success(result) => self.handle_decode_success(result).await,
            DecodeDone::Failed {
                segment_id,
                raw_size: _,
                error,
                source_server_idx,
                exclude_servers,
            } => {
                self.handle_decode_failure(segment_id, &error, &exclude_servers, source_server_idx);
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
    pub(crate) fn handle_decode_failure(
        &mut self,
        segment_id: SegmentId,
        error: &str,
        exclude_servers: &[usize],
        source_server_idx: Option<usize>,
    ) {
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
                    && file_spec.role.counts_toward_health()
                    && let Some(seg_spec) = file_spec
                        .segments
                        .iter()
                        .find(|s| s.ordinal == segment_id.segment_number)
                {
                    state.failed_bytes += seg_spec.bytes as u64;
                }
            }
            self.mark_promoted_recovery_segment_unavailable(segment_id);
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
                    .find(|s| s.ordinal == segment_id.segment_number)
            {
                let exclude =
                    Self::decode_retry_exclude_servers(exclude_servers, source_server_idx);
                let work = DownloadWork {
                    segment_id,
                    message_id: crate::jobs::ids::MessageId::new(&seg_spec.message_id),
                    groups: file_spec.groups.clone(),
                    priority: file_spec.role.download_priority(),
                    byte_estimate: seg_spec.bytes,
                    retry_count: 0,
                    is_recovery: file_spec.role.is_recovery(),
                    exclude_servers: exclude.clone(),
                };
                self.metrics
                    .segments_retried
                    .fetch_add(1, Ordering::Relaxed);
                let delay = std::time::Duration::from_secs(1 << (retry_count - 1));
                self.note_retry_scheduled(segment_id);
                warn!(
                    segment = %segment_id,
                    error,
                    decode_retry = retry_count,
                    source_server_idx,
                    exclude_servers = ?exclude,
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
        let _profile_scope = crate::runtime::perf_probe::scope("download.handle_decode_success");
        let DecodeResult {
            segment_id,
            raw_size: _,
            file_offset,
            decoded_size,
            crc_valid,
            part_crc_verified,
            part_crc,
            expected_file_crc,
            data,
            yenc_name,
        } = result;

        let job_id = segment_id.file_id.job_id;
        let file_id = segment_id.file_id;

        let ready = {
            let _cpu_scope =
                crate::runtime::perf_probe::cpu_scope("download.handle_decode_success.pre_persist");
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
            if let Err(error) = self.note_expected_file_crc(file_id, expected_file_crc) {
                self.metrics.crc_errors.fetch_add(1, Ordering::Relaxed);
                self.fail_job(job_id, error);
                return;
            }

            if !crc_valid {
                self.metrics.crc_errors.fetch_add(1, Ordering::Relaxed);
            }
            if part_crc_verified {
                crate::runtime::perf_probe::record(
                    "download.yenc_part_crc.verified",
                    std::time::Duration::from_nanos(1),
                );
            } else {
                crate::runtime::perf_probe::record(
                    "download.yenc_part_crc.not_verified",
                    std::time::Duration::from_nanos(1),
                );
                let yenc_name_lower = yenc_name.to_ascii_lowercase();
                let is_rar_volume = yenc_name_lower.ends_with(".rar")
                    || yenc_name_lower.rsplit_once('.').is_some_and(|(_, ext)| {
                        ext.len() == 3
                            && ext.starts_with('r')
                            && ext.as_bytes()[1..].iter().all(u8::is_ascii_digit)
                    });
                let unverified_bucket = if yenc_name_lower.ends_with(".par2") {
                    "download.yenc_part_crc.not_verified.par2"
                } else if is_rar_volume {
                    "download.yenc_part_crc.not_verified.rar"
                } else {
                    "download.yenc_part_crc.not_verified.other"
                };
                crate::runtime::perf_probe::record(
                    unverified_bucket,
                    std::time::Duration::from_nanos(1),
                );
                info!(
                    job_id = job_id.0,
                    file_id = %file_id,
                    segment = %segment_id,
                    file_index = file_id.file_index,
                    yenc_name = %yenc_name,
                    "yEnc part CRC was not independently verified"
                );
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
                data,
                part_crc,
                part_crc_verified,
                yenc_name,
            };
            let buffered_len = buffered_segment.len_bytes();

            let ready = {
                let _cpu_scope =
                    crate::runtime::perf_probe::cpu_scope("download.write_buffer.insert_drain");
                let write_buf = self
                    .write_buffers
                    .entry(file_id)
                    .or_insert_with(|| WriteReorderBuffer::new(self.write_buf_max_pending));
                write_buf.insert(file_offset, buffered_segment);
                write_buf.drain_ready_with_contiguous_end()
            };
            self.note_write_buffered(buffered_len, 1);
            ready
        };

        if let Err(error) = self.persist_ready_segments(file_id, ready.0, ready.1).await {
            self.fail_job_for_disk_write(
                error,
                "disk write failed for sequential decoded segments",
            );
            return;
        }

        if let Err(error) = self.enforce_file_write_backlog(file_id).await {
            self.fail_job_for_disk_write(error, "failed to relieve per-file write backlog");
            return;
        }

        if let Err(error) = self.relieve_global_write_backlog().await {
            self.fail_job_for_disk_write(error, "failed to relieve global write backlog");
        }
    }

    fn fail_job_for_disk_write(&mut self, error: SegmentWriteError, context: &'static str) {
        let job_id = error.file_id.job_id;
        let message = format!("{context} for {}: {}", error.file_id, error.source);
        error!(
            job_id = job_id.0,
            file_id = %error.file_id,
            error = %error.source,
            context,
            "disk write failed; failing job"
        );
        self.fail_job(job_id, message);
    }

    fn release_unwritten_segments<I>(&mut self, segments: I)
    where
        I: IntoIterator<Item = (u64, BufferedDecodedSegment)>,
    {
        let (released_bytes, released_segments) = segments
            .into_iter()
            .fold((0usize, 0usize), |(bytes, count), (_, segment)| {
                (bytes + segment.len_bytes(), count + 1)
            });
        if released_bytes > 0 || released_segments > 0 {
            self.release_write_buffered(released_bytes, released_segments);
        }
    }

    async fn persist_ready_segments(
        &mut self,
        file_id: NzbFileId,
        ready: Vec<(u64, BufferedDecodedSegment)>,
        contiguous_end_after_ready: u64,
    ) -> Result<(), SegmentWriteError> {
        if ready.is_empty() {
            self.remove_empty_write_buffer(file_id);
            return Ok(());
        }
        let _profile_scope = crate::runtime::perf_probe::scope("download.persist_ready_segments");

        let Some((_job_id, filename, _working_dir, file_path)) =
            self.write_target_for_file(file_id)
        else {
            let released_bytes = ready.iter().map(|(_, segment)| segment.len_bytes()).sum();
            self.release_write_buffered(released_bytes, ready.len());
            self.remove_empty_write_buffer(file_id);
            return Ok(());
        };

        let write_start = Instant::now();
        let ready_bytes = ready.iter().map(|(_, segment)| segment.len_bytes()).sum();
        let ready_count = ready.len();
        let write_result = write_segments_to_disk(&file_path, ready).await;
        self.release_write_buffered(ready_bytes, ready_count);

        let (written, write_error) = match write_result {
            Ok(written) => (written, None),
            Err(error) => {
                let source = error.source;
                let written = error.written;
                drop(error.unwritten);
                (written, Some(source))
            }
        };

        for (offset, segment) in written {
            crate::e2e_failpoint::maybe_trip("download.after_disk_write_before_commit");
            self.commit_persisted_segment(
                offset,
                segment,
                &filename,
                &file_path,
                SegmentHashMode::UpdateNow,
            )
            .await;
        }
        if let Some(source) = write_error {
            return Err(SegmentWriteError::new(file_id, source));
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
    ) -> Result<(), SegmentWriteError> {
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
            self.persist_out_of_order_segment(
                file_id,
                offset,
                segment,
                OutOfOrderPersistReason::PerFileMaxPending,
            )
            .await?;
        }
    }

    async fn relieve_global_write_backlog(&mut self) -> Result<(), SegmentWriteError> {
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

            self.persist_out_of_order_segment(
                file_id,
                offset,
                segment,
                OutOfOrderPersistReason::GlobalWriteBacklog,
            )
            .await?;
        }

        Ok(())
    }

    async fn persist_out_of_order_segment(
        &mut self,
        file_id: NzbFileId,
        offset: u64,
        segment: BufferedDecodedSegment,
        reason: OutOfOrderPersistReason,
    ) -> Result<(), SegmentWriteError> {
        crate::runtime::perf_probe::record(
            reason.profile_bucket(),
            std::time::Duration::from_nanos(1),
        );
        let segment_bytes = segment.len_bytes();
        let Some((_job_id, filename, _working_dir, file_path)) =
            self.write_target_for_file(file_id)
        else {
            self.release_write_buffered(segment_bytes, 1);
            self.remove_empty_write_buffer(file_id);
            return Ok(());
        };

        let write_start = Instant::now();
        let write_result = write_segment_to_disk(&file_path, offset, segment).await;
        let write_us = write_start.elapsed().as_micros() as u64;
        self.metrics
            .disk_write_latency_us
            .store(write_us, Ordering::Relaxed);
        self.release_write_buffered(segment_bytes, 1);
        let segment = write_result.map_err(|source| SegmentWriteError::new(file_id, source))?;
        crate::e2e_failpoint::maybe_trip("download.after_disk_write_before_commit");

        if let Some(write_buf) = self.write_buffers.get_mut(&file_id) {
            write_buf.mark_persisted(offset, segment_bytes);
        }
        self.metrics
            .direct_write_evictions
            .fetch_add(1, Ordering::Relaxed);

        self.commit_persisted_segment(
            offset,
            segment,
            &filename,
            &file_path,
            SegmentHashMode::DeferRange,
        )
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
        file_path: &std::path::Path,
        hash_mode: SegmentHashMode,
    ) {
        let _profile_scope = crate::runtime::perf_probe::scope("download.commit_persisted_segment");
        let BufferedDecodedSegment {
            segment_id,
            decoded_size,
            data,
            part_crc,
            part_crc_verified,
            yenc_name,
        } = segment;
        let job_id = segment_id.file_id.job_id;
        let file_id = segment_id.file_id;

        let commit_result = {
            let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.assembly.commit");
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file_mut(file_id) else {
                return;
            };

            match file_asm.commit_segment(segment_id.segment_number, decoded_size) {
                Ok(commit) => Ok((commit.file_complete, file_asm.total_bytes())),
                Err(e) => Err(e),
            }
        };

        match commit_result {
            Ok((file_complete, total_bytes)) => {
                self.metrics
                    .bytes_committed
                    .fetch_add(decoded_size as u64, Ordering::Relaxed);
                self.metrics
                    .segments_committed
                    .fetch_add(1, Ordering::Relaxed);

                let _ = self
                    .event_tx
                    .send(PipelineEvent::SegmentCommitted { segment_id });

                match hash_mode {
                    SegmentHashMode::UpdateNow => {
                        self.drain_deferred_file_hash_ranges(file_id, file_path)
                            .await;
                        self.note_file_hash_decoded_chunk(
                            file_id,
                            file_offset,
                            &data,
                            part_crc,
                            part_crc_verified,
                        );
                        self.drain_deferred_file_hash_ranges(file_id, file_path)
                            .await;
                    }
                    SegmentHashMode::DeferRange => {
                        self.defer_file_hash_chunk(
                            file_id,
                            file_offset,
                            data,
                            part_crc,
                            part_crc_verified,
                        );
                    }
                }

                if file_complete {
                    crate::runtime::perf_probe::record(
                        "download.file_progress.complete_file_row_covers_restart",
                        std::time::Duration::ZERO,
                    );
                    self.unavailable_promoted_recovery_segments
                        .retain(|segment_id| segment_id.file_id != file_id);
                    if let Some(mut write_buf) = self.write_buffers.remove(&file_id) {
                        let leftovers = write_buf.flush_all();
                        if !leftovers.is_empty() {
                            self.mark_file_hash_reread_required_for(file_id, "final_buffer_flush");
                            warn!(
                                file_id = %file_id,
                                leftover_segments = leftovers.len(),
                                "file reached complete state with buffered decoded segments still pending; flushing directly"
                            );
                            let mut leftovers = leftovers.into_iter();
                            while let Some((offset, buffered)) = leftovers.next() {
                                let buffered_bytes = buffered.len_bytes();
                                if let Err(e) =
                                    write_segment_to_disk(file_path, offset, buffered).await
                                {
                                    warn!(
                                        file = %filename,
                                        offset,
                                        error = %e,
                                        "disk write failed during final buffered flush"
                                    );
                                    self.release_write_buffered(buffered_bytes, 1);
                                    self.release_unwritten_segments(leftovers);
                                    self.fail_job_for_disk_write(
                                        SegmentWriteError::new(file_id, e),
                                        "disk write failed during final buffered flush",
                                    );
                                    return;
                                }
                                self.release_write_buffered(buffered_bytes, 1);
                            }
                        }
                    }

                    let expected_file_crc = self.expected_file_crcs.get(&file_id).copied();
                    let file_checksum = match self
                        .finalize_completed_file_hash(
                            file_id,
                            filename,
                            file_path.to_path_buf(),
                            total_bytes,
                            expected_file_crc,
                        )
                        .await
                    {
                        Ok(checksum) => checksum,
                        Err(error) => {
                            if expected_file_crc.is_some() {
                                warn!(file_id = %file_id, error = %error, "failed to verify yEnc whole-file CRC32");
                                self.fail_job(
                                    job_id,
                                    format!("failed to verify yEnc whole-file CRC32 for {file_id}: {error}"),
                                );
                                return;
                            }

                            warn!(file_id = %file_id, error = %error, "failed to persist real completed-file hash");
                            CompletedFileChecksum {
                                md5: [0u8; 16],
                                crc32: 0,
                            }
                        }
                    };
                    if let Some(expected_crc) = expected_file_crc
                        && file_checksum.crc32 != expected_crc
                    {
                        self.metrics.crc_errors.fetch_add(1, Ordering::Relaxed);
                        self.fail_job(
                            job_id,
                            format!(
                                "yEnc whole-file CRC32 mismatch for {filename}: expected {expected_crc:08x}, actual {:08x}",
                                file_checksum.crc32
                            ),
                        );
                        return;
                    }
                    let file_hash = file_checksum.md5;

                    if !yenc_name.is_empty() && yenc_name != filename {
                        if self.yenc_name_matches_rewritten_source(
                            job_id, file_id, &yenc_name, filename,
                        ) {
                            debug!(
                                job_id = job_id.0,
                                current = %filename,
                                yenc = %yenc_name,
                                "yEnc name differs from current filename after file identity rewrite"
                            );
                        } else {
                            warn!(
                                job_id = job_id.0,
                                assembly = %filename,
                                yenc = %yenc_name,
                                "yEnc name disagrees with assembly filename"
                            );
                        }
                    }

                    info!(file_id = %file_id, filename = %filename, "file complete");
                    let _ = self.event_tx.send(PipelineEvent::FileComplete {
                        file_id,
                        filename: filename.to_string(),
                        total_bytes,
                    });

                    {
                        let file_index = file_id.file_index;
                        let fname = filename.to_string();
                        if let Err(e) = self
                            .db_blocking(move |db| {
                                db.complete_file(job_id, file_index, &fname, &file_hash)
                            })
                            .await
                        {
                            error!(error = %e, "db write failed for complete_file");
                        }
                    }
                    self.pending_file_progress.remove(&file_id);
                    self.persisted_file_progress.remove(&file_id);
                    self.file_hash_states.remove(&file_id);
                    self.expected_file_crcs.remove(&file_id);
                    self.file_hash_reread_required.remove(&file_id);

                    let mut stage_start = Instant::now();
                    self.try_load_par2_metadata(job_id, file_id).await;
                    crate::runtime::perf_probe::record(
                        "file_complete.try_load_par2_metadata",
                        stage_start.elapsed(),
                    );
                    debug!(
                        job_id = job_id.0,
                        stage_ms = stage_start.elapsed().as_millis() as u64,
                        "file-complete stage: try_load_par2_metadata"
                    );
                    stage_start = Instant::now();
                    self.try_merge_par2_recovery(job_id, file_id).await;
                    crate::runtime::perf_probe::record(
                        "file_complete.try_merge_par2_recovery",
                        stage_start.elapsed(),
                    );
                    debug!(
                        job_id = job_id.0,
                        stage_ms = stage_start.elapsed().as_millis() as u64,
                        "file-complete stage: try_merge_par2_recovery"
                    );
                    stage_start = Instant::now();
                    self.refresh_archive_state_for_completed_file(job_id, file_id, true)
                        .await;
                    crate::runtime::perf_probe::record(
                        "file_complete.refresh_archive_state_for_completed_file",
                        stage_start.elapsed(),
                    );
                    debug!(
                        job_id = job_id.0,
                        stage_ms = stage_start.elapsed().as_millis() as u64,
                        "file-complete stage: refresh_archive_state_for_completed_file"
                    );
                    stage_start = Instant::now();
                    self.retry_par2_authoritative_identity(job_id).await;
                    crate::runtime::perf_probe::record(
                        "file_complete.retry_par2_authoritative_identity",
                        stage_start.elapsed(),
                    );
                    debug!(
                        job_id = job_id.0,
                        stage_ms = stage_start.elapsed().as_millis() as u64,
                        "file-complete stage: retry_par2_authoritative_identity"
                    );
                    stage_start = Instant::now();
                    self.try_rar_extraction(job_id).await;
                    crate::runtime::perf_probe::record(
                        "file_complete.try_rar_extraction",
                        stage_start.elapsed(),
                    );
                    debug!(
                        job_id = job_id.0,
                        stage_ms = stage_start.elapsed().as_millis() as u64,
                        "file-complete stage: try_rar_extraction"
                    );
                    stage_start = Instant::now();
                    self.check_job_completion(job_id).await;
                    crate::runtime::perf_probe::record(
                        "file_complete.check_job_completion",
                        stage_start.elapsed(),
                    );
                    debug!(
                        job_id = job_id.0,
                        stage_ms = stage_start.elapsed().as_millis() as u64,
                        "file-complete stage: check_job_completion"
                    );
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

fn checksum_completed_file(path: &std::path::Path) -> io::Result<CompletedFileChecksum> {
    let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.file_hash.reread");
    let mut file = File::open(path)?;
    let mut md5 = weaver_par2::checksum::FileHashState::new();
    let mut crc32 = crc32fast::Hasher::new();
    let mut buffer = [0u8; 256 * 1024];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        md5.update(&buffer[..bytes_read]);
        crc32.update(&buffer[..bytes_read]);
    }
    Ok(CompletedFileChecksum {
        md5: md5.finalize(),
        crc32: crc32.finalize(),
    })
}

fn read_file_range(path: &std::path::Path, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0u8; len];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}
