use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::time::Instant;

use super::*;
use crate::pipeline::direct_store::wiring::{DirectFileTarget, DirectRouteOutcome};

const MAX_DEFERRED_FILE_HASH_DATA_BYTES: usize = 128 * 1024 * 1024;
const OUT_OF_ORDER_DISK_WRITE_BATCH_SEGMENTS: usize = 16;

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

    /// Obfuscated posts routinely name articles (yEnc) differently from both
    /// the subject-declared filename and the PAR2 canonical name. Once PAR2
    /// metadata is loaded the recovery set is the naming authority: the yEnc
    /// name either already matches the canonical name, or the rebind that
    /// follows completion settles the disagreement.
    pub(crate) fn yenc_name_expected_from_par2_identity(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
        yenc_name: &str,
    ) -> bool {
        let Some(identity) = self.file_identity(job_id, file_id) else {
            return false;
        };
        if identity.canonical_filename.as_deref() == Some(yenc_name) {
            return true;
        }
        identity.classification_source != crate::jobs::record::FileIdentitySource::Par2
            && self.par2_set(job_id).is_some()
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
        self.invalidate_par2_session_for_file_write(file_id);
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
        self.invalidate_par2_session_for_file_write(file_id);

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
        self.file_hash_states
            .entry(file_id)
            .or_default()
            .update_decoded_chunk(data, part_crc, part_crc_verified, track_md5);
    }

    fn note_file_hash_crc_metadata(
        &mut self,
        file_id: NzbFileId,
        file_offset: u64,
        len: usize,
        part_crc: u32,
        part_crc_verified: bool,
    ) {
        if len == 0 {
            return;
        }
        self.invalidate_par2_session_for_file_write(file_id);

        let expected_offset = self
            .file_hash_states
            .get(&file_id)
            .map(|state| state.bytes_fed())
            .unwrap_or(0);
        if expected_offset != file_offset {
            self.mark_file_hash_reread_required_for(file_id, "offset_mismatch");
            return;
        }

        self.file_hash_states
            .entry(file_id)
            .or_default()
            .update_crc_metadata(len as u64, part_crc, part_crc_verified);
    }

    fn should_use_completed_file_crc_metadata(&self, file_id: NzbFileId) -> bool {
        self.can_defer_completed_file_md5(file_id)
            && self.expected_file_crcs.contains_key(&file_id)
            && !self.file_hash_reread_required.contains(&file_id)
    }

    fn note_deferred_file_hash_range(
        &mut self,
        file_id: NzbFileId,
        file_offset: u64,
        len: usize,
        part_crc: u32,
        part_crc_verified: bool,
        source: DeferredFileHashRangeSource,
    ) {
        self.deferred_file_hash_ranges
            .entry(file_id)
            .or_default()
            .insert(
                file_offset,
                DeferredFileHashRange {
                    len,
                    part_crc,
                    part_crc_verified,
                    source,
                },
            );
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
        self.note_deferred_file_hash_range(
            file_id,
            file_offset,
            len,
            part_crc,
            part_crc_verified,
            DeferredFileHashRangeSource::DecodedData,
        );

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

    fn defer_file_hash_crc_metadata(
        &mut self,
        file_id: NzbFileId,
        file_offset: u64,
        len: usize,
        part_crc: u32,
        part_crc_verified: bool,
    ) {
        if len == 0 || self.file_hash_reread_required.contains(&file_id) {
            return;
        }
        crate::runtime::perf_probe::record(
            "download.file_hash.deferred_crc_metadata",
            std::time::Duration::from_nanos(1),
        );
        self.note_deferred_file_hash_range(
            file_id,
            file_offset,
            len,
            part_crc,
            part_crc_verified,
            DeferredFileHashRangeSource::CrcMetadata,
        );
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

    pub(in crate::pipeline) async fn drain_deferred_file_hash_ranges(
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
            let Some(range) = self
                .deferred_file_hash_ranges
                .get(&file_id)
                .and_then(|ranges| ranges.get(&expected_offset))
            else {
                return;
            };
            let len = range.len;
            let read_fallback_bucket = range.source.read_fallback_bucket();
            let read_fallback_bytes_bucket = range.source.read_fallback_bytes_bucket();
            let metadata_replay_bucket = range.source.metadata_replay_bucket();
            let metadata_replay_bytes_bucket = range.source.metadata_replay_bytes_bucket();
            let part_crc = range.part_crc;
            let part_crc_verified = range.part_crc_verified;

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

            if part_crc_verified && !self.should_stream_md5_for_file(file_id) {
                crate::runtime::perf_probe::record(
                    "download.file_hash.deferred_crc_metadata_replayed",
                    std::time::Duration::from_nanos(1),
                );
                crate::runtime::perf_probe::record(
                    metadata_replay_bucket,
                    std::time::Duration::from_nanos(1),
                );
                crate::runtime::perf_probe::record_value(metadata_replay_bytes_bucket, len as u64);
                self.note_file_hash_crc_metadata(
                    file_id,
                    expected_offset,
                    len,
                    part_crc,
                    part_crc_verified,
                );
                continue;
            }

            crate::runtime::perf_probe::record(
                read_fallback_bucket,
                std::time::Duration::from_nanos(1),
            );
            crate::runtime::perf_probe::record_value(read_fallback_bytes_bucket, len as u64);
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

            let part_crc = par2_rs::checksum::crc32(&bytes);
            self.note_file_hash_chunk(file_id, expected_offset, &bytes, part_crc, false);
        }
    }

    /// Whether this file's completed-file checksum can be produced without the
    /// streamed MD5 *and* without reading the file back.
    ///
    /// The streamed per-file MD5 has exactly one consumer that turns it into a
    /// decision: PAR2 committed-file evidence, which uses it to bind a finished
    /// file to a recovery-set description by hash identity and admit it as a
    /// repair source without a re-read. That consumer has a substitute — the
    /// contiguous-assembly proof, which pairs the whole-file CRC32 this
    /// pipeline already composes from part CRCs with a 16 KiB head read — and
    /// the substitute needs the recovery set's per-slice checksums to derive the
    /// expected whole-file CRC32 from. A set without them can serve neither the
    /// substitute nor in-stream block verification, so the streamed hash stays.
    ///
    /// No posted `=yend crc32` is required. Two independent CRC alignments
    /// already adjudicate every assembled byte on this path: the decoder
    /// verifies each article's yEnc pcrc32, and PAR2 evidence checks the
    /// recovery set's IFSC CRC32s — in stream on the slice grid, and again as
    /// the composed whole-file CRC32 the contiguous-assembly proof carries.
    /// Bytes no slice verdict vouches for are settled by read-back hashing,
    /// and repair re-derives CRC32 and MD5 over every byte it consumes. The
    /// aggregate `=yend` CRC is still checked whenever a poster supplies one,
    /// but multipart posts routinely omit it, and holding the MD5 skip
    /// hostage to it kept the hash alive on most real downloads.
    ///
    /// The whole-file MD5 defends nothing the CRCs leave open: a recovery set
    /// travels with the payload it describes, so whoever can substitute the
    /// payload can post a self-consistent set beside it and the MD5
    /// comparison passes anyway; defending that would take an out-of-band
    /// trust root no client has. Completing clean downloads on CRC evidence
    /// is also the ecosystem default — mainstream clients rest on a single
    /// combined CRC32 equality where this path demands two independent
    /// alignments agree.
    fn completed_file_md5_substitutable(&self, file_id: NzbFileId) -> bool {
        self.par2_set(file_id.job_id)
            .is_some_and(|set| !set.slice_checksums.is_empty())
    }

    fn should_stream_md5_for_file(&self, file_id: NzbFileId) -> bool {
        if self.can_defer_completed_file_md5(file_id) {
            return false;
        }
        // In-stream block verification replaced the whole-file hash as the
        // download path's evidence: PAR2 block CRC32s are strictly finer than
        // one MD5 over the whole file, and they cost nothing beyond the CRC
        // pass the decoder already runs. The hash is kept only where no
        // settle-time substitute can stand in for it -- see
        // `completed_file_md5_substitutable` -- because its absence would
        // otherwise be paid for with a whole-file re-read at completion.
        if self.completed_file_md5_substitutable(file_id) {
            return false;
        }
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

    fn can_defer_completed_file_md5(&self, file_id: NzbFileId) -> bool {
        if self.par2_set(file_id.job_id).is_some() {
            return false;
        }
        let Some(state) = self.jobs.get(&file_id.job_id) else {
            return false;
        };
        let Some(file) = state.assembly.file(file_id) else {
            return false;
        };
        matches!(
            self.classified_role_for_file(file_id.job_id, file),
            weaver_model::files::FileRole::Standalone | weaver_model::files::FileRole::Unknown
        )
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

    fn completed_file_checksum_from_crc_metadata(
        &self,
        file_id: NzbFileId,
        total_bytes: u64,
    ) -> Option<StreamedCompletedFileChecksum> {
        let mut crc32 = 0;
        let mut bytes_fed = 0;
        let mut all_parts_crc_verified = true;

        if let Some(state) = self.file_hash_states.get(&file_id) {
            crc32 = state.crc32();
            bytes_fed = state.bytes_fed();
            all_parts_crc_verified = state.all_parts_crc_verified();
        }

        let ranges = self.deferred_file_hash_ranges.get(&file_id);
        while bytes_fed < total_bytes {
            let range = ranges.and_then(|ranges| ranges.get(&bytes_fed))?;
            let len = range.len as u64;
            if len == 0 {
                return None;
            }
            let op = par2_rs::checksum::Crc32CombineOp::new(len);
            crc32 = op.combine(crc32, range.part_crc);
            all_parts_crc_verified &= range.part_crc_verified;
            bytes_fed = bytes_fed.checked_add(len)?;
        }

        (bytes_fed == total_bytes).then_some(StreamedCompletedFileChecksum {
            md5: None,
            crc32,
            all_parts_crc_verified,
        })
    }

    pub(crate) async fn finalize_completed_file_hash(
        &mut self,
        file_id: NzbFileId,
        filename: &str,
        file_path: std::path::PathBuf,
        total_bytes: u64,
        expected_file_crc: Option<u32>,
    ) -> Result<CompletedFileChecksum, String> {
        if expected_file_crc.is_some()
            && self.can_defer_completed_file_md5(file_id)
            && !self.file_hash_reread_required.contains(&file_id)
            && let Some(streamed) =
                self.completed_file_checksum_from_crc_metadata(file_id, total_bytes)
        {
            crate::runtime::perf_probe::record(
                "download.file_hash.crc32.from_deferred_metadata",
                std::time::Duration::from_nanos(1),
            );
            self.file_hash_states.remove(&file_id);
            self.drop_deferred_file_hash_data_for(file_id);
            self.deferred_file_hash_ranges.remove(&file_id);
            return Ok(CompletedFileChecksum {
                md5: None,
                crc32: streamed.crc32,
                all_parts_crc_verified: streamed.all_parts_crc_verified,
            });
        }

        let deferred_range_count_before = self
            .deferred_file_hash_ranges
            .get(&file_id)
            .map_or(0, BTreeMap::len);
        let deferred_data_count_before = self
            .deferred_file_hash_data
            .get(&file_id)
            .map_or(0, BTreeMap::len);
        let deferred_data_bytes_before = self.deferred_file_hash_data_bytes;
        self.drain_deferred_file_hash_ranges(file_id, &file_path)
            .await;
        let deferred_range_count_after = self
            .deferred_file_hash_ranges
            .get(&file_id)
            .map_or(0, BTreeMap::len);
        let deferred_data_count_after = self
            .deferred_file_hash_data
            .get(&file_id)
            .map_or(0, BTreeMap::len);
        let hash_state = self.file_hash_states.remove(&file_id);
        let streamed_bytes = hash_state.as_ref().map_or(0, |state| state.bytes_fed());
        let streamed_md5_active = hash_state
            .as_ref()
            .is_some_and(CompletedFileChecksumState::tracks_md5);
        self.deferred_file_hash_ranges.remove(&file_id);
        self.drop_deferred_file_hash_data_for(file_id);
        let reread_required = self.file_hash_reread_required.remove(&file_id);
        if crate::runtime::perf_probe::enabled() {
            info!(
                file_id = %file_id,
                filename,
                total_bytes,
                streamed_bytes,
                streamed_md5_active,
                reread_required,
                has_expected_file_crc = expected_file_crc.is_some(),
                expected_file_crc = expected_file_crc.unwrap_or_default(),
                deferred_range_count_before,
                deferred_range_count_after,
                deferred_data_count_before,
                deferred_data_count_after,
                deferred_data_bytes_before,
                "completed-file hash finalize state"
            );
        }
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
                            md5: Some(md5),
                            crc32: streamed.crc32,
                            all_parts_crc_verified: streamed.all_parts_crc_verified,
                        });
                    }
                    // The PAR2 expected-MD5 substitution that used to live here is
                    // deliberately gone: a PAR2 description's hash is an
                    // EXPECTATION, and storing it as though it had been
                    // calculated from the downloaded bytes let quick
                    // verification certify a file the in-stream IFSC verdicts
                    // had already proven Damaged (the yEnc aggregate CRC it was
                    // gated on is the poster's own declaration, not independent
                    // evidence). Files land in the deferral paths below instead
                    // and are adjudicated by the dual-CRC slice verdicts, like
                    // SABnzbd's quick-check and NZBGet's ParQuick, which only
                    // ever compare observed values against expectations.
                    let file_crc_matched = expected_file_crc
                        .is_some_and(|expected_file_crc| streamed.crc32 == expected_file_crc);
                    if file_crc_matched && self.can_defer_completed_file_md5(file_id) {
                        crate::runtime::perf_probe::record(
                            "download.file_hash.md5.deferred_no_par2_expected_crc",
                            std::time::Duration::from_nanos(1),
                        );
                        return Ok(CompletedFileChecksum {
                            md5: None,
                            crc32: streamed.crc32,
                            all_parts_crc_verified: streamed.all_parts_crc_verified,
                        });
                    }
                    // The matching half of `should_stream_md5_for_file`: where
                    // the hash was skipped because PAR2 evidence can be captured
                    // from the composed whole-file CRC32 instead, completion must
                    // not undo that saving by reading the file back for it.
                    if self.completed_file_md5_substitutable(file_id) {
                        // A poster-supplied aggregate `=yend crc32` that
                        // disagrees while every article CRC passed means the
                        // header lied about content the PAR2 block grid will
                        // adjudicate; count that case separately.
                        let label = if expected_file_crc.is_some() && !file_crc_matched {
                            "download.file_hash.md5.deferred_to_par2_slice_evidence.file_crc_mismatch"
                        } else {
                            "download.file_hash.md5.deferred_to_par2_slice_evidence"
                        };
                        crate::runtime::perf_probe::record(
                            label,
                            std::time::Duration::from_nanos(1),
                        );
                        return Ok(CompletedFileChecksum {
                            md5: None,
                            crc32: streamed.crc32,
                            all_parts_crc_verified: streamed.all_parts_crc_verified,
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

    async fn schedule_file_crc_recovery(
        &mut self,
        file_id: NzbFileId,
        expected_crc: u32,
        actual_crc: u32,
    ) -> Result<bool, String> {
        let Some(file_candidates) = self.unverified_segments.get(&file_id) else {
            return Ok(false);
        };
        let candidate_count = file_candidates.len();

        let Some(state) = self.jobs.get(&file_id.job_id) else {
            return Ok(false);
        };
        let Some(file_spec) = state.spec.files.get(file_id.file_index as usize) else {
            return Ok(false);
        };
        let mut queued = Vec::with_capacity(candidate_count);
        for seg_spec in &file_spec.segments {
            let Some(provenance) = file_candidates.get(&seg_spec.ordinal) else {
                continue;
            };
            let segment_id = SegmentId {
                file_id,
                segment_number: seg_spec.ordinal,
            };
            let retry_count = self.decode_retries.get(&segment_id).copied().unwrap_or(0);
            if retry_count >= MAX_SEGMENT_RETRIES {
                warn!(
                    file_id = %file_id,
                    segment = %segment_id,
                    retries = retry_count,
                    expected_crc = format_args!("{expected_crc:08x}"),
                    actual_crc = format_args!("{actual_crc:08x}"),
                    "whole-file CRC recovery exhausted segment retry budget"
                );
                return Ok(false);
            }
            let exclude_servers = Self::decode_retry_exclude_servers(
                &provenance.exclude_servers,
                provenance.source_server_idx,
            );
            queued.push((
                segment_id,
                retry_count + 1,
                DownloadWork {
                    segment_id,
                    message_id: crate::jobs::ids::MessageId::new(&seg_spec.message_id),
                    groups: file_spec.groups.clone(),
                    priority: file_spec.role.download_priority(),
                    byte_estimate: seg_spec.bytes,
                    retry_count: 0,
                    // A CRC recovery candidate is mandatory even when the source
                    // file is normally optional recovery material.
                    is_recovery: false,
                    exclude_servers,
                    avoid_server: None,
                },
            ));
        }
        if queued.len() != candidate_count {
            return Ok(false);
        }

        let recovery_job_id = file_id.job_id;
        let recovery_file_index = file_id.file_index;
        self.db_blocking(move |db| db.mark_file_incomplete(recovery_job_id, recovery_file_index))
            .await
            .map_err(|error| {
                format!("failed to persist file invalidation before CRC recovery: {error}")
            })?;
        self.pending_file_progress.remove(&file_id);
        self.persisted_file_progress.remove(&file_id);
        self.mark_file_hash_reread_required_for(file_id, "whole_file_crc_recovery");
        // CRC recovery rewrites this file's bytes through `write_segment_to_disk`
        // directly — its early returns never reach the dual-CRC seam — so
        // blocks the grid already claimed would sit `Intact` over changed disk
        // content. The job's block state is retired here for the same reason
        // the yEnc hash state is invalidated above.
        self.block_crcs.forget_job(file_id.job_id);

        self.file_crc_recoveries.insert(
            file_id,
            FileCrcRecoveryState {
                pending_segments: queued
                    .iter()
                    .map(|(segment_id, _, _)| *segment_id)
                    .collect(),
                expected_crc,
                last_actual_crc: actual_crc,
            },
        );

        for (segment_id, retry_count, work) in queued {
            self.decode_retries.insert(segment_id, retry_count);
            self.metrics
                .segments_retried
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                file_id = %file_id,
                segment = %segment_id,
                retry_count,
                exclude_servers = ?work.exclude_servers,
                "queued unverified segment for whole-file CRC recovery"
            );
            self.requeue_retry_work(work);
        }
        self.update_queue_metrics();
        warn!(
            file_id = %file_id,
            candidate_count,
            expected_crc = format_args!("{expected_crc:08x}"),
            actual_crc = format_args!("{actual_crc:08x}"),
            "whole-file CRC mismatch; recovering unverified segments"
        );
        Ok(true)
    }

    async fn persist_file_crc_recovery_segment(
        &mut self,
        file_offset: u64,
        segment: BufferedDecodedSegment,
    ) {
        let segment_id = segment.segment_id;
        let file_id = segment_id.file_id;
        let Some((_job_id, filename, _working_dir, file_path)) =
            self.write_target_for_file(file_id)
        else {
            self.fail_job(
                file_id.job_id,
                format!("missing write target during whole-file CRC recovery for {file_id}"),
            );
            return;
        };

        let segment = match write_segment_to_disk(&file_path, file_offset, segment).await {
            Ok(segment) => segment,
            Err(error) => {
                self.fail_job_for_disk_write(
                    SegmentWriteError::new(file_id, error),
                    "disk write failed during whole-file CRC recovery",
                );
                return;
            }
        };
        self.mark_file_hash_reread_required_for(file_id, "whole_file_crc_recovery_write");

        let remaining = self.file_crc_recoveries.get_mut(&file_id).map(|recovery| {
            recovery.pending_segments.remove(&segment_id);
            recovery.pending_segments.len()
        });
        let Some(remaining) = remaining else {
            debug!(segment = %segment_id, "discarding stale whole-file CRC recovery result");
            return;
        };
        if remaining > 0 {
            debug!(
                file_id = %file_id,
                segment = %segment_id,
                remaining,
                "whole-file CRC recovery replacement persisted"
            );
            return;
        }

        let recovery = self
            .file_crc_recoveries
            .remove(&file_id)
            .expect("CRC recovery state must exist after final replacement");
        info!(
            file_id = %file_id,
            expected_crc = format_args!("{:08x}", recovery.expected_crc),
            previous_actual_crc = format_args!("{:08x}", recovery.last_actual_crc),
            "whole-file CRC recovery batch persisted; verifying file"
        );
        self.commit_persisted_segment(
            file_offset,
            segment,
            &filename,
            &file_path,
            SegmentHashMode::UpdateNow,
        )
        .await;
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
                    let batch = self
                        .write_buffers
                        .get_mut(&file_id)
                        .map(|write_buf| {
                            write_buf
                                .take_oldest_buffered_batch(OUT_OF_ORDER_DISK_WRITE_BATCH_SEGMENTS)
                        })
                        .unwrap_or_default();
                    if batch.is_empty() {
                        self.remove_empty_write_buffer(file_id);
                        break;
                    }
                    let batch_len = batch.len();

                    if let Err(error) = self
                        .persist_out_of_order_segments(
                            file_id,
                            batch,
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
                    flushed_segments += batch_len;
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
            DecodeDone::Success { result, .. } => (result.segment_id, result.raw_size),
            DecodeDone::Failed {
                segment_id,
                raw_size,
                ..
            } => (*segment_id, *raw_size),
        };
        self.metrics.note_decode_task_finished(raw_size);
        self.note_decode_finished(segment_id);

        match result {
            DecodeDone::Success { result, source } => {
                self.handle_decode_success(result, source).await;
            }
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
            if let Some((expected_crc, actual_crc)) = self
                .file_crc_recoveries
                .get(&segment_id.file_id)
                .filter(|recovery| recovery.pending_segments.contains(&segment_id))
                .map(|recovery| (recovery.expected_crc, recovery.last_actual_crc))
            {
                self.fail_job(
                    job_id,
                    format!(
                        "whole-file CRC32 recovery exhausted for {segment_id}: expected {expected_crc:08x}, last actual {actual_crc:08x}; final decode error: {error}"
                    ),
                );
                return;
            }
            self.book_failed_segment(segment_id);
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
                    avoid_server: None,
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
                let scheduled_pool_generation = self.pool_generation;
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let _ = retry_tx
                        .send(crate::pipeline::RetryWork {
                            scheduled_pool_generation,
                            infrastructure_retry: false,
                            work,
                        })
                        .await;
                });
            }
        }
    }

    pub(crate) async fn handle_decode_success(
        &mut self,
        result: DecodeResult,
        source: SegmentSource,
    ) {
        let _profile_scope = crate::runtime::perf_probe::scope("download.handle_decode_success");
        let DecodeResult {
            segment_id,
            raw_size: _,
            yenc_layout,
            crc_valid,
            part_crc_verified,
            part_crc,
            expected_file_crc,
            data,
            yenc_name,
            segments,
        } = result;

        let job_id = segment_id.file_id.job_id;
        let file_id = segment_id.file_id;

        let ready = {
            let _cpu_scope =
                crate::runtime::perf_probe::cpu_scope("download.handle_decode_success.pre_persist");
            let Some(state) = self.jobs.get(&job_id) else {
                debug!(
                    job_id = job_id.0,
                    segment = %segment_id,
                    "discarding decode result for inactive job"
                );
                return;
            };
            if is_terminal_status(&state.status) {
                debug!(
                    job_id = job_id.0,
                    segment = %segment_id,
                    "discarding decode result for inactive job"
                );
                return;
            }
            let expected_layout = state
                .assembly
                .file(file_id)
                .ok_or(AuthoritativeLayoutError::FileMissing)
                .and_then(|file| expected_segment_layout(file, segment_id.segment_number));
            let expected_layout = match expected_layout {
                Ok(layout) => layout,
                Err(error) => {
                    let error = format_authoritative_layout_error(error);
                    self.fail_job(job_id, error);
                    return;
                }
            };
            let decoded_len = data.len_bytes();
            let file_offset = match validate_yenc_layout(expected_layout, yenc_layout, decoded_len)
            {
                Ok(file_offset) => file_offset,
                Err(mismatch) => {
                    let error = format_yenc_layout_mismatch(
                        mismatch,
                        expected_layout,
                        yenc_layout,
                        decoded_len,
                    );
                    self.metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
                    self.handle_decode_failure(
                        segment_id,
                        &error,
                        &source.exclude_servers,
                        source.source_server_idx,
                    );
                    return;
                }
            };
            // The bytes that actually decoded, not what the NZB declared: its
            // sizes are yEnc-encoded and run ~3% large.
            let decoded_size = decoded_len as u32;

            // The per-segment bounds above cannot see across segments, so they
            // would still let an article claim a range an earlier ordinal
            // already owns. Check against this segment's neighbours and record
            // the placement in the same borrow: this is per-article work on the
            // orchestrator thread, so it is two range probes and one insert.
            let conflict = match self
                .jobs
                .get_mut(&job_id)
                .and_then(|state| state.assembly.file_mut(file_id))
            {
                Some(file) => {
                    match file.placement_conflict(
                        segment_id.segment_number,
                        file_offset,
                        decoded_size,
                    ) {
                        Some(other) => Some(other),
                        None => {
                            file.record_placement(
                                segment_id.segment_number,
                                file_offset,
                                decoded_size,
                            );
                            None
                        }
                    }
                }
                None => None,
            };
            if let Some(other) = conflict {
                let error = format!(
                    "yEnc layout conflict: segment {} claims [{file_offset}, {}) which runs into segment {other}",
                    segment_id.segment_number,
                    file_offset.saturating_add(u64::from(decoded_size)),
                );
                self.metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
                self.handle_decode_failure(
                    segment_id,
                    &error,
                    &source.exclude_servers,
                    source.source_server_idx,
                );
                return;
            }

            self.metrics
                .bytes_decoded
                .fetch_add(u64::from(decoded_size), Ordering::Relaxed);
            self.metrics
                .segments_decoded
                .fetch_add(1, Ordering::Relaxed);

            let is_file_crc_recovery = !self.file_crc_recoveries.is_empty()
                && self
                    .file_crc_recoveries
                    .get(&file_id)
                    .is_some_and(|recovery| recovery.pending_segments.contains(&segment_id));

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

            if part_crc_verified {
                if is_file_crc_recovery {
                    let remove_file_bucket = self
                        .unverified_segments
                        .get_mut(&file_id)
                        .is_some_and(|segments| {
                            segments.remove(&segment_id.segment_number);
                            segments.is_empty()
                        });
                    if remove_file_bucket {
                        self.unverified_segments.remove(&file_id);
                    }
                }
            } else {
                self.unverified_segments
                    .entry(file_id)
                    .or_default()
                    .insert(segment_id.segment_number, source);
            }

            let buffered_segment = BufferedDecodedSegment {
                segment_id,
                decoded_size,
                data,
                part_crc,
                part_crc_verified,
                yenc_name,
                segments,
            };

            if is_file_crc_recovery {
                drop(_cpu_scope);
                self.persist_file_crc_recovery_segment(file_offset, buffered_segment)
                    .await;
                return;
            }

            // Track decoded (not raw/yEnc-encoded) bytes so progress never exceeds 100%.
            if let Some(state) = self.jobs.get_mut(&job_id) {
                state.downloaded_bytes += decoded_size as u64;
                // Hot-path safe: the category's counter was resolved once when
                // the job was added, so this is a single `Relaxed` `fetch_add`
                // through an already-held `Arc` — no map lookup, no allocation,
                // no lock, and it rides the job-state lookup this path already
                // performs.
                if let Some(category_bytes) = state.category_bytes.as_ref() {
                    category_bytes.fetch_add(decoded_size as u64, Ordering::Relaxed);
                }
            }

            // A direct set's source volume never becomes a file, so its bytes
            // leave the conventional path here — before the write reorder
            // buffer, before `write_segments_to_disk`, and therefore before any
            // legacy progress floor or completed-file row.
            match self.direct_route_target(file_id) {
                Some(DirectFileTarget::Route {
                    set_index,
                    volume_index,
                }) => {
                    drop(_cpu_scope);
                    let outcome = self
                        .handle_direct_decode_success(
                            set_index,
                            volume_index,
                            buffered_segment,
                            file_offset,
                        )
                        .await;
                    // The per-article half of `sets == direct + materialized`:
                    // an article either reached a destination or handed its
                    // volume back, and the two counts must add up to the
                    // articles that entered the seam.
                    crate::runtime::perf_probe::record(
                        match outcome {
                            DirectRouteOutcome::Routed => "direct_store.article.routed",
                            DirectRouteOutcome::Demoted => "direct_store.article.demoted",
                        },
                        std::time::Duration::from_nanos(1),
                    );
                    return;
                }
                Some(DirectFileTarget::Discard) => {
                    drop(_cpu_scope);
                    debug!(
                        segment = %segment_id,
                        "discarding a duplicate article for a finalized direct set"
                    );
                    crate::runtime::perf_probe::record(
                        "direct_store.article.discarded_after_finalization",
                        std::time::Duration::from_nanos(1),
                    );
                    return;
                }
                None => {}
            }

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
        crate::runtime::perf_probe::record_value(
            "download.persist_ready_segments.batch_count",
            ready_count as u64,
        );
        crate::runtime::perf_probe::record_value(
            "download.persist_ready_segments.batch_bytes",
            ready_bytes as u64,
        );
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
        // Hot-path safe: `write_start.elapsed()` is the single clock read this
        // path already performs for the `disk_write_latency_us` gauge, and the
        // histogram observes exactly that same span so the two agree. The
        // observation itself is three `Relaxed` `fetch_add`s.
        let write_elapsed = write_start.elapsed();
        let write_us = write_elapsed.as_micros() as u64;
        self.metrics
            .disk_write_latency_us
            .store(write_us, Ordering::Relaxed);
        self.metrics
            .pipeline_histograms
            .observe_disk_write(write_elapsed);

        self.remove_empty_write_buffer(file_id);
        Ok(())
    }

    async fn enforce_file_write_backlog(
        &mut self,
        file_id: NzbFileId,
    ) -> Result<(), SegmentWriteError> {
        loop {
            let batch = {
                let Some(write_buf) = self.write_buffers.get_mut(&file_id) else {
                    return Ok(());
                };
                if !write_buf.exceeds_max_pending() {
                    return Ok(());
                }
                write_buf.take_oldest_buffered_batch(OUT_OF_ORDER_DISK_WRITE_BATCH_SEGMENTS)
            };

            if batch.is_empty() {
                self.remove_empty_write_buffer(file_id);
                return Ok(());
            }
            self.persist_out_of_order_segments(
                file_id,
                batch,
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

            let batch = self
                .write_buffers
                .get_mut(&file_id)
                .map(|write_buf| {
                    write_buf.take_oldest_buffered_batch(OUT_OF_ORDER_DISK_WRITE_BATCH_SEGMENTS)
                })
                .unwrap_or_default();
            if batch.is_empty() {
                self.remove_empty_write_buffer(file_id);
                continue;
            }

            self.persist_out_of_order_segments(
                file_id,
                batch,
                OutOfOrderPersistReason::GlobalWriteBacklog,
            )
            .await?;
        }

        Ok(())
    }

    async fn persist_out_of_order_segments(
        &mut self,
        file_id: NzbFileId,
        segments: Vec<(u64, BufferedDecodedSegment)>,
        reason: OutOfOrderPersistReason,
    ) -> Result<(), SegmentWriteError> {
        if segments.is_empty() {
            self.remove_empty_write_buffer(file_id);
            return Ok(());
        }

        for _ in 0..segments.len() {
            crate::runtime::perf_probe::record(
                reason.profile_bucket(),
                std::time::Duration::from_nanos(1),
            );
        }
        let segment_count = segments.len();
        let segment_bytes = segments
            .iter()
            .map(|(_, segment)| segment.len_bytes())
            .sum::<usize>();
        crate::runtime::perf_probe::record_value(
            "download.persist_out_of_order_segment.batch_count",
            segment_count as u64,
        );
        crate::runtime::perf_probe::record_value(
            "download.persist_out_of_order_segment.batch_bytes",
            segment_bytes as u64,
        );
        let Some((_job_id, filename, _working_dir, file_path)) =
            self.write_target_for_file(file_id)
        else {
            self.release_write_buffered(segment_bytes, segment_count);
            self.remove_empty_write_buffer(file_id);
            return Ok(());
        };

        let write_start = Instant::now();
        let write_result = write_segments_to_disk(&file_path, segments).await;
        // Hot-path safe: reuses the `write_start` this path already keeps for
        // the `disk_write_latency_us` gauge, so the histogram costs no extra
        // clock read — only the three `Relaxed` `fetch_add`s inside `observe`.
        // The last-write gauge is left untouched for compatibility.
        let write_elapsed = write_start.elapsed();
        let write_us = write_elapsed.as_micros() as u64;
        self.metrics
            .disk_write_latency_us
            .store(write_us, Ordering::Relaxed);
        self.metrics
            .pipeline_histograms
            .observe_disk_write(write_elapsed);
        self.release_write_buffered(segment_bytes, segment_count);
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
            let segment_bytes = segment.len_bytes();

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
        }
        if let Some(source) = write_error {
            return Err(SegmentWriteError::new(file_id, source));
        }
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
            segments,
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
                Ok(commit) => Ok((
                    commit.file_complete,
                    file_asm.received_bytes(),
                    commit.was_duplicate,
                )),
                Err(e) => Err(e),
            }
        };

        match commit_result {
            Ok((file_complete, total_bytes, was_duplicate)) => {
                if !was_duplicate {
                    self.metrics
                        .bytes_committed
                        .fetch_add(decoded_size as u64, Ordering::Relaxed);
                    self.metrics
                        .segments_committed
                        .fetch_add(1, Ordering::Relaxed);

                    let _ = self
                        .event_tx
                        .send(PipelineEvent::SegmentCommitted { segment_id });
                }

                // Ordering contract: this seam runs only after
                // `write_segments_to_disk` returned for this segment, so these
                // bytes are already on disk and any later read of the same
                // range — a settle-time verification read, the authoritative
                // pass — sees exactly them. That is what lets an in-stream
                // block verdict stand in for a read: it describes bytes that
                // are durable, not bytes still in a buffer.
                //
                // A duplicate is fed here on purpose, unlike the file hash
                // below. The dual-CRC grid is positional, not sequential: a
                // whole-block re-feed recomputes the same verdict from the same
                // bytes. It is also required rather than merely harmless — this
                // arrival rewrote the range, so if the replay carried different
                // bytes than the first copy did, skipping the feed would leave
                // a verdict describing content that is no longer on disk.
                self.note_block_crc_segments(
                    file_id,
                    file_offset,
                    data.len_bytes() as u64,
                    part_crc,
                    part_crc_verified,
                    was_duplicate,
                    &segments,
                );

                // The file hash is a *running* stream: every chunk must be fed
                // once, in offset order. A duplicate's bytes were already fed
                // by the original arrival, so re-feeding them is what trips the
                // running-offset check in `note_file_hash_*` and condemns a
                // perfectly good file to a whole-file re-read at completion.
                // Below-cursor duplicates reach this seam by design since
                // 815f1a12 (they are handed back and rewritten idempotently
                // rather than wedging the reorder buffer), so the feed — both
                // the immediate and the deferred arm — is skipped for them.
                //
                // Skipping the feed is not enough on its own: this arrival
                // REWROTE the range on disk, and nothing at this seam can
                // prove it wrote the same bytes the stream already digested.
                // A hash state left standing here could complete into a
                // persisted digest of the pre-rewrite bytes and quick-verify
                // a file whose content it no longer describes — with no block
                // verdict to veto it when PAR2 metadata arrives only later.
                // So the whole streamed state (MD5 and CRC-metadata arms
                // alike) is condemned to the completion-time re-read, which
                // digests the disk as the rewrite left it. Duplicates are the
                // exceptional path; clean downloads never pay this.
                if was_duplicate {
                    self.mark_file_hash_reread_required_for(file_id, "duplicate_rewrite");
                    drop(data);
                } else {
                    let use_crc_metadata = self.should_use_completed_file_crc_metadata(file_id);
                    match hash_mode {
                        SegmentHashMode::UpdateNow if use_crc_metadata => {
                            let decoded_len = data.len_bytes();
                            self.defer_file_hash_crc_metadata(
                                file_id,
                                file_offset,
                                decoded_len,
                                part_crc,
                                part_crc_verified,
                            );
                            drop(data);
                        }
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
                            drop(data);
                        }
                        SegmentHashMode::DeferRange if use_crc_metadata => {
                            let decoded_len = data.len_bytes();
                            self.defer_file_hash_crc_metadata(
                                file_id,
                                file_offset,
                                decoded_len,
                                part_crc,
                                part_crc_verified,
                            );
                            drop(data);
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
                }

                // Deliberately not gated on `was_duplicate`: completion is a
                // property of the file, not of this arrival. A duplicate leaves
                // the received bitvec untouched, so it can never *newly*
                // complete a file — `file_complete` is true here only when the
                // file was already complete, and this branch runs the same
                // finalization it would have run for the arrival that finished
                // it. Skipping the hash feed above is safe for that case too:
                // `finalize_completed_file_hash` drains its own deferred ranges
                // and falls back to the whole-file read when the streamed state
                // does not cover the file.
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

                    // Queued behind the file's final write on its owner
                    // thread, so the fd is released before verification,
                    // repair, or the final move touch this path.
                    crate::pipeline::release_cached_write_handle(file_path);

                    // The file's length is what makes its short final block
                    // closable: until now that block's extent was undecided.
                    // Blocks the grid could not claim in stream (including the
                    // leftover flush above, which writes directly without
                    // re-entering this seam) are left unclaimed for the
                    // completion gate's verification pass to read back.
                    self.block_crcs.note_file_len(file_id, total_bytes);

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
                                md5: None,
                                crc32: 0,
                                all_parts_crc_verified: false,
                            }
                        }
                    };
                    if let Some(expected_crc) = expected_file_crc
                        && file_checksum.crc32 != expected_crc
                    {
                        self.metrics.crc_errors.fetch_add(1, Ordering::Relaxed);
                        match self
                            .schedule_file_crc_recovery(file_id, expected_crc, file_checksum.crc32)
                            .await
                        {
                            Ok(true) => return,
                            Ok(false) => {}
                            Err(error) => {
                                self.fail_job(job_id, error);
                                return;
                            }
                        }
                        self.fail_job(
                            job_id,
                            format!(
                                "yEnc whole-file CRC32 mismatch for {filename}: expected {expected_crc:08x}, actual {:08x}",
                                file_checksum.crc32
                            ),
                        );
                        return;
                    }
                    self.ensure_par2_runtime(job_id)
                        .completed_checksums
                        .insert(file_id, file_checksum);
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
                        } else if self
                            .yenc_name_expected_from_par2_identity(job_id, file_id, &yenc_name)
                        {
                            debug!(
                                job_id = job_id.0,
                                assembly = %filename,
                                yenc = %yenc_name,
                                "yEnc name deferred to PAR2 canonical identity"
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
                                db.complete_file_with_optional_hash(
                                    job_id,
                                    file_index,
                                    &fname,
                                    file_hash.as_ref(),
                                )
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
                    self.unverified_segments.remove(&file_id);
                    self.file_crc_recoveries.remove(&file_id);

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
    let mut md5 = par2_rs::checksum::FileHashState::new();
    let mut crc32 = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
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
        md5: Some(md5.finalize()),
        crc32: crc32.finalize() as u32,
        all_parts_crc_verified: false,
    })
}

fn read_file_range(path: &std::path::Path, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0u8; len];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}
