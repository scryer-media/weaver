// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::io::{self, Read};

use super::*;
use tempfile::Builder;

// Where a uuencode part can go right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UuPlacement {
    // Its prefix is complete; place it at this decoded offset.
    Place(u64),
    // Its prefix has not arrived; hold it.
    Park,
    // The park is full and this part is the furthest from the cursor. Its
    // bytes are dropped and its ordinal goes back to the download queue.
    Displaced,
    // The cursor already shifted past this ordinal after it was booked failed,
    // so its bytes have no home and never will. Dropped terminally.
    Stale,
}

#[derive(Debug)]
pub(in crate::pipeline) struct SegmentWriteError {
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
    // Whether a download or decode result for the job is still taken: the job
    // is here, has not ended, and is not publishing what it delivered. Once
    // the final move begins, cleanup has chosen what the delivery holds, and
    // a late duplicate must not make an input file again.
    pub(in crate::pipeline) fn job_takes_results(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            !is_terminal_status(&state.status)
                && !matches!(
                    state.status,
                    JobStatus::Moving | JobStatus::QueuedPostProcessing | JobStatus::PostProcessing
                )
        })
    }

    pub(crate) fn note_decode_started(&mut self, segment_id: SegmentId, raw_bytes: u64) {
        self.active_decode_bytes.insert(segment_id, raw_bytes);
        let job_id = segment_id.file_id.job_id;
        self.active_decodes_by_job.started(job_id);
        *self
            .active_decodes_by_file
            .entry(segment_id.file_id)
            .or_default() += 1;
        self.publish_active_stage_metrics();
    }

    fn note_decode_finished(&mut self, segment_id: SegmentId) {
        self.active_decode_bytes.remove(&segment_id);
        let job_id = segment_id.file_id.job_id;
        self.active_decodes_by_job.finished(job_id);
        if let Some(active) = self.active_decodes_by_file.get_mut(&segment_id.file_id) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                self.active_decodes_by_file.remove(&segment_id.file_id);
            }
        }
        self.publish_active_stage_metrics();
    }

    // Whether the decode stage holds nothing at all for `job_id`: no decode
    // running and none queued behind the decode semaphore.
    //
    // This is deliberately narrower than
    // [`Self::job_has_pending_download_work_beyond_health_probe`] — it asks
    // only about the decode stage, and is the cheap gate that decides whether
    // a settling decode is worth re-running the download-drain sequence for.
    pub(in crate::pipeline) fn job_decode_stage_drained(&self, job_id: JobId) -> bool {
        self.active_decodes_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            == 0
            && !self
                .pending_decode
                .iter()
                .any(|work| work.segment_id.file_id.job_id == job_id)
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

    // Handle a completed decode — persist the segment, update assembly, journal.
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

        // A decode that exhausted this ordinal's servers may have settled the
        // damaged bytes a parked part was waiting to start after.
        self.release_settled_unanchored_runs().await;

        self.pump_decode_queue();

        // The download-result path already ran the drain sequence for this
        // job — but it ran it with this decode still queued or in flight, so
        // it necessarily refused to retire an in-flight health probe: an
        // unsettled decode is pending download work.
        //
        // Nothing re-runs that sequence once the decode settles. A
        // direct-routed article leaves `handle_decode_success` at its early
        // return, the conventional file-complete seam schedules no drain, and
        // the dispatch-idle and quiescent-flush hooks only fire when something
        // else pokes the pipeline. A probe that outlived the job's last byte
        // would then hold the completion checkpoint — and PAR2 recovery
        // promotion with it — until its own soft timeout expired.
        //
        // So the last decode of a job re-runs the same sequence. Everything in
        // it is idempotent (`DownloadFinished` is gated on
        // `active_download_passes`), and its own `in_flight == 0 && no queued
        // work` gate makes this a no-op for every decode that is not the last.
        let job_id = segment_id.file_id.job_id;
        if self.jobs.contains_key(&job_id) && self.job_decode_stage_drained(job_id) {
            self.maybe_finish_download_pass(job_id);
        }
        self.report_job(job_id);
    }

    // Whether a body that failed to decode is fetched again: the job still
    // downloads, and the article holds another message-id, or has decode
    // retries left and a server that has not served it. With neither, none
    // configured among them, the damage stands as it came.
    pub(crate) fn decode_retry_left(&mut self, segment_id: SegmentId, exclude: &[usize]) -> bool {
        let job_id = segment_id.file_id.job_id;
        if self
            .jobs
            .get(&job_id)
            .is_none_or(|state| is_terminal_status(&state.status))
            || self.segment_terminal_states.contains_key(&segment_id)
        {
            return false;
        }
        let retries = self.decode_retries.get(&segment_id).copied().unwrap_or(0);
        self.message_id_left(segment_id)
            || retries < MAX_SEGMENT_RETRIES
                && self.unavailable_server_count(job_id, exclude) < self.nntp.pool().server_count()
    }

    // Handle a decode failure by re-queuing the segment for re-download.
    //
    // yEnc decode failures (CRC/size mismatch, malformed data) indicate the
    // article body was corrupted — either in transit or on the server. The
    // segment is re-downloaded with the source server excluded, so the
    // retry lands on another server. Once every server has produced a bad
    // body (immediately on a one-server setup) or `MAX_SEGMENT_RETRIES`
    // decode failures have accrued, the segment is marked permanently
    // failed and health is updated so repair can take over.
    pub(crate) fn handle_decode_failure(
        &mut self,
        segment_id: SegmentId,
        error: &str,
        exclude_servers: &[usize],
        source_server_idx: Option<usize>,
    ) {
        let job_id = segment_id.file_id.job_id;

        if !self.job_takes_results(job_id) {
            debug!(
                segment = %segment_id,
                error,
                "decode failed for inactive job — not retrying"
            );
            return;
        }

        if self.segment_terminal_states.contains_key(&segment_id) {
            // A later metadata probe still needs its unavailable marker, while
            // the existing terminal booking and metrics remain unchanged.
            self.book_terminal_segment(segment_id, SegmentTerminalState::DecodeExhausted);
            return;
        }

        // A body that arrived and did not decode spends its message-id while
        // the article holds another: the next id is fetched, not this one
        // from another server.
        if self.spend_message_id(segment_id) {
            warn!(segment = %segment_id, error, "decode failed; fetching the part's next message-id");
            return;
        }

        let retries = self
            .decode_retries
            .entry(segment_id)
            .and_modify(|c| *c += 1)
            .or_insert(1);
        let retry_count = *retries;

        // The server that produced the undecodable body is off the list, and
        // the article moves on only while another server remains to try.
        // When none does — on a one-server setup, right after the first
        // failure — the article is given up on now so repair can cover it.
        // Re-queuing it would leave work no lane may serve sitting in the
        // queue forever while the job reports downloading at 0 B/s.
        let exclude = Self::decode_retry_exclude_servers(exclude_servers, source_server_idx);
        let no_server_left = self.no_server_can_serve(job_id, &exclude);

        if retry_count > MAX_SEGMENT_RETRIES || no_server_left {
            if no_server_left {
                warn!(
                    segment = %segment_id,
                    error,
                    decode_failures = retry_count,
                    exclude_servers = ?exclude,
                    "decode failed on every server — giving the article up for repair"
                );
            } else {
                warn!(
                    segment = %segment_id,
                    error,
                    retries = MAX_SEGMENT_RETRIES,
                    "decode failed permanently after max retries"
                );
            }
            self.metrics
                .segments_failed_permanent
                .fetch_add(1, Ordering::Relaxed);
            self.book_terminal_segment(segment_id, SegmentTerminalState::DecodeExhausted);
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
                let work = DownloadWork {
                    counters: None,
                    segment_id,
                    message_id: crate::jobs::ids::MessageId::new(seg_spec.message_ids.in_use()),
                    groups: std::sync::Arc::from(file_spec.groups.as_slice()),
                    priority: file_spec.role.download_priority(),
                    byte_estimate: seg_spec.bytes,
                    retry_count: 0,
                    is_recovery: file_spec.role.is_recovery(),
                    completion_critical: self.segment_is_completion_critical(segment_id),
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

    // Withdraw the placement `other` was accepted at when `segment_id`, whose
    // claim it contradicts, is the one the NZB's declared sizes vouch for and
    // `other` is not. The withdrawn article is fetched again like any other
    // that failed to decode, and when its copy carries the same bad header it
    // meets placed neighbours and fails on arrival. Returns whether it was
    // withdrawn; when it was not, `segment_id` is the one that fails.
    //
    // An accepted placement is all an article header can be judged by, so a
    // placement reconstructed from durable coverage is never withdrawn.
    fn withdraw_contradicted_placement(
        &mut self,
        segment_id: SegmentId,
        offset: u64,
        len: u32,
        expected: ExpectedSegmentLayout,
        other: u32,
    ) -> bool {
        let file_id = segment_id.file_id;
        if !placement_within_nzb_envelope(expected, offset, len) {
            return false;
        }
        let Some(file) = self
            .jobs
            .get_mut(&file_id.job_id)
            .and_then(|state| state.articles.assembly_mut()?.file_mut(file_id))
        else {
            return false;
        };
        let Some((other_offset, other_len)) = file.placement_of(other) else {
            return false;
        };
        let Ok(other_expected) = expected_segment_layout(file, other) else {
            return false;
        };
        if placement_within_nzb_envelope(other_expected, other_offset, other_len) {
            return false;
        }
        file.withdraw_placement(other);
        let other_id = SegmentId {
            file_id,
            segment_number: other,
        };
        let error = format!(
            "yEnc layout conflict: segment {other} claims [{other_offset}, {}) past what the NZB declares for it, which segment {} contradicts",
            other_offset.saturating_add(u64::from(other_len)),
            segment_id.segment_number,
        );
        self.metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
        self.handle_decode_failure(other_id, &error, &[], None);
        true
    }

    // Commit a decoded segment, then release any parked parts its arrival
    // unblocked: uuencode parts behind the sequential cursor, and yEnc parts
    // that could not say where they start.
    //
    // Sequential assembly means one part's placement can make the next one
    // placeable, and that one the next again. Released parts re-enter through
    // the same path rather than through a second copy of the commit logic, so
    // there is exactly one place where a segment is placed, written and
    // accounted for.
    pub(crate) async fn handle_decode_success(
        &mut self,
        result: DecodeResult,
        source: SegmentSource,
    ) {
        let file_id = result.segment_id.file_id;
        let segment_number = result.segment_id.segment_number;
        let is_uu = result.encoding.is_uu();
        self.handle_decode_success_inner(result, source).await;

        if !is_uu {
            let mut placed = segment_number;
            while let Some((result, source)) = self.take_unanchored_successor(file_id, placed).await
            {
                placed = result.segment_id.segment_number;
                self.handle_decode_success_inner(result, source).await;
            }
            // Bytes kept for repair are settled by the booking their own
            // retries end in, which happens inside the commit above and cannot
            // write the parts it unblocks.
            self.release_settled_unanchored_runs().await;
            return;
        }
        self.release_ready_uu_parts(file_id).await;
    }

    // Write every parked uuencode part the file's cursor has reached, each
    // through the ordinary placement path, which moves the cursor on to the
    // next.
    async fn release_ready_uu_parts(&mut self, file_id: NzbFileId) {
        loop {
            let next = match self.take_next_ready_uu_segment(file_id).await {
                Ok(next) => next,
                Err(error) => {
                    self.fail_job_for_disk_write(error, "UU spool read failed");
                    return;
                }
            };
            let Some((segment_number, data)) = next else {
                break;
            };
            let released = DecodeResult {
                segment_id: SegmentId {
                    file_id,
                    segment_number,
                },
                raw_size: data.len_bytes() as u64,
                // Damage and the end marker were folded into the file's state
                // when this part first arrived; replaying them here would be
                // harmless but redundant.
                encoding: SegmentEncoding::Uu(crate::pipeline::UuSegmentFacts {
                    damaged: false,
                    ended: false,
                }),
                yenc_layout: YencLayoutAssertions {
                    file_size: 0,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                crc_valid: true,
                part_crc_verified: false,
                part_crc: 0,
                truncation_suspected: false,
                expected_file_crc: None,
                data,
                yenc_name: String::new(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                segments: Vec::new(),
            };
            self.handle_decode_success_inner(
                released,
                SegmentSource {
                    source_server_idx: None,
                    exclude_servers: Vec::new(),
                },
            )
            .await;
        }
    }

    async fn handle_decode_success_inner(&mut self, result: DecodeResult, source: SegmentSource) {
        let _profile_scope = crate::runtime::perf_probe::scope("download.handle_decode_success");
        let DecodeResult {
            segment_id,
            raw_size,
            encoding,
            yenc_layout,
            crc_valid,
            part_crc_verified,
            part_crc,
            truncation_suspected,
            expected_file_crc,
            data,
            yenc_name,
            checkpoint_plan,
            segments,
        } = result;

        let job_id = segment_id.file_id.job_id;
        let file_id = segment_id.file_id;

        {
            let _cpu_scope =
                crate::runtime::perf_probe::cpu_scope("download.handle_decode_success.pre_persist");
            if !self.job_takes_results(job_id) {
                debug!(
                    job_id = job_id.0,
                    segment = %segment_id,
                    "discarding decode result for inactive job"
                );
                return;
            }
            let state = &self.jobs[&job_id];
            let expected_layout = state
                .articles
                .assembly()
                .and_then(|assembly| assembly.file(file_id))
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
            // Set when this part starts where damaged bytes kept for repair
            // were judged to end, rather than after a placement.
            let mut anchored_on_settled_damage = false;

            // uuencode cannot be placed by the yEnc layout rules. An article
            // declares no range, so the validator's no-range arm falls back to
            // the NZB's declared prefix — and NZB segment byte counts are
            // *encoded* sizes. yEnc encodes at roughly 1.03x, so that fallback
            // is nearly right for yEnc; uuencode encodes at roughly 1.38x, so
            // the same number would place every part far past where its bytes
            // belong and scatter the file. The only correct offset is the
            // cumulative DECODED length of the preceding parts, which the
            // sequential cursor below owns. No declared byte count enters that
            // computation anywhere.
            let file_offset = if let Some(facts) = encoding.uu_facts() {
                // Before any placement decision, because a part can be parked,
                // displaced or dropped below and every one of those returns
                // early — and the part that carries the `begin` header is a
                // part like any other. The retained name is recorded here so
                // it survives whatever happens to its bytes.
                if !yenc_name.is_empty() {
                    self.note_uu_filename(file_id, &yenc_name);
                }
                match self.place_uu_segment(
                    file_id,
                    segment_id.segment_number,
                    facts,
                    decoded_len,
                    &data,
                ) {
                    UuPlacement::Place(offset) => offset,
                    UuPlacement::Park => {
                        // Held until its prefix arrives. `data` moves into the
                        // park; the cursor releases it later through this same
                        // path, so nothing downstream sees a half-placed part.
                        //
                        // Anything the park had to displace to stay inside its
                        // bound must be re-fetched: its bytes are gone, and the
                        // download layer already counts it as delivered.
                        drop(_cpu_scope);
                        let displaced = match self
                            .park_uu_segment(file_id, segment_id.segment_number, data)
                            .await
                        {
                            Ok(displaced) => displaced,
                            Err(error) => {
                                self.fail_job_for_disk_write(error, "UU spool write failed");
                                return;
                            }
                        };
                        for ordinal in displaced {
                            self.requeue_displaced_uu_segment(SegmentId {
                                file_id,
                                segment_number: ordinal,
                            });
                        }
                        return;
                    }
                    UuPlacement::Displaced => {
                        // The park is full and this part sits furthest from the
                        // cursor, so it is dropped rather than held — but it
                        // still has to come back, on the same zero-burn terms
                        // as anything the park displaces.
                        drop(_cpu_scope);
                        self.requeue_displaced_uu_segment(segment_id);
                        return;
                    }
                    UuPlacement::Stale => {
                        // The cursor shifted past this ordinal when it was
                        // booked failed, so these bytes have nowhere to go and
                        // a re-fetch would only produce the same homeless
                        // article again. Drop it terminally.
                        drop(_cpu_scope);
                        crate::runtime::perf_probe::record(
                            "download.uu.stale_arrival_dropped",
                            std::time::Duration::from_nanos(1),
                        );
                        debug!(
                            segment = %segment_id,
                            "uuencode part arrived after its ordinal was shifted away; dropping"
                        );
                        return;
                    }
                }
            } else {
                // Only consulted for an article that declared no usable start
                // of its own: ordinal zero opens the file, and every other
                // ordinal follows the one before it.
                let sequential_anchor = if segment_id.segment_number == 0 {
                    Some(0)
                } else {
                    let predecessor = segment_id.segment_number - 1;
                    // A part given up, or whose damaged copy stands, knows
                    // where it ended; assembly only needs to be told that no
                    // further copy of it is coming.
                    let settled = self.ordinal_settled(SegmentId {
                        file_id,
                        segment_number: predecessor,
                    });
                    let file = self
                        .jobs
                        .get(&job_id)
                        .and_then(|state| state.articles.assembly()?.file(file_id));
                    let anchor = file.and_then(|file| file.anchor_end_after(predecessor, settled));
                    // An anchor the part before it never placed is one read off
                    // bytes that are being kept only for repair — and it is
                    // used only by an article that named no start of its own.
                    anchored_on_settled_damage = anchor.is_some()
                        && yenc_layout.begin.is_none()
                        && file.is_some_and(|file| file.placement_of(predecessor).is_none());
                    anchor
                };
                match validate_yenc_layout(
                    expected_layout,
                    yenc_layout,
                    decoded_len,
                    sequential_anchor,
                ) {
                    Ok(file_offset) => file_offset,
                    Err(YencLayoutMismatch::PredecessorNotPlaced) => {
                        // An ordering condition, not damage. The article waits,
                        // decoded, for the part before it, and spends no retry.
                        drop(_cpu_scope);
                        self.park_or_requeue_unanchored(
                            DecodeResult {
                                segment_id,
                                raw_size,
                                encoding,
                                yenc_layout,
                                crc_valid,
                                part_crc_verified,
                                part_crc,
                                truncation_suspected,
                                expected_file_crc,
                                data,
                                yenc_name,
                                checkpoint_plan,
                                segments,
                            },
                            source,
                        );
                        return;
                    }
                    Err(mismatch) => {
                        let error = format_yenc_layout_mismatch(
                            mismatch,
                            expected_layout,
                            yenc_layout,
                            decoded_len,
                        );
                        self.metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
                        drop(_cpu_scope);
                        self.handle_decode_failure(
                            segment_id,
                            &error,
                            &source.exclude_servers,
                            source.source_server_idx,
                        );
                        return;
                    }
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
            let held_damage = self
                .jobs
                .get(&job_id)
                .and_then(|state| state.articles.assembly()?.file(file_id))
                .map(|file| {
                    (
                        file.segment_has_retained_damage(segment_id.segment_number),
                        file.segment_damage_is_truncation_only(segment_id.segment_number),
                    )
                })
                .unwrap_or((false, false));
            // A copy whose own length is accounted for settles a body that was
            // merely suspected of being cut short. It says nothing about a
            // checksum that actually disagreed, so that damage still stands.
            let unresolved_replacement =
                !part_crc_verified && held_damage.0 && !(!truncation_suspected && held_damage.1);
            let retain_damage = !crc_valid || truncation_suspected || unresolved_replacement;
            // A late corrupt duplicate must not replace bytes the root already
            // holds (or their placement), nor stand in for an article it has
            // ruled lost.
            if (!crc_valid || truncation_suspected) && self.graph_article_settled(segment_id) {
                debug!(segment = %segment_id, "damaged copy of a settled article; dropped");
                return;
            }
            let mut conflict = match self
                .jobs
                .get_mut(&job_id)
                .and_then(|state| state.articles.assembly_mut()?.file_mut(file_id))
            {
                Some(file) => {
                    if retain_damage {
                        None
                    } else {
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
                                if anchored_on_settled_damage {
                                    // The tiling is now only as good as the
                                    // boundary damaged bytes were judged to
                                    // have, so completion stays gated on a
                                    // repair verdict for this file.
                                    file.require_geometry_verification();
                                }
                                None
                            }
                        }
                    }
                }
                None => None,
            };
            // Two accepted claims that contradict each other cannot both be
            // right, and the one that fails is the one the NZB cannot vouch
            // for: a bad extent costs only the article that carries it, in
            // whichever order its neighbours arrive.
            while let Some(other) = conflict
                && self.withdraw_contradicted_placement(
                    segment_id,
                    file_offset,
                    decoded_size,
                    expected_layout,
                    other,
                )
            {
                conflict = self
                    .jobs
                    .get_mut(&job_id)
                    .and_then(|state| state.articles.assembly_mut()?.file_mut(file_id))
                    .and_then(|file| {
                        let other = file.placement_conflict(
                            segment_id.segment_number,
                            file_offset,
                            decoded_size,
                        );
                        if other.is_none() {
                            file.record_placement(
                                segment_id.segment_number,
                                file_offset,
                                decoded_size,
                            );
                            if anchored_on_settled_damage {
                                file.require_geometry_verification();
                            }
                        }
                        other
                    });
            }
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

            // The job's root takes an article's bytes once, so a body that
            // failed its checksum goes to another server while one remains,
            // and only the last server's copy stands as damage.
            if retain_damage
                && self.decode_retry_left(
                    segment_id,
                    &Self::decode_retry_exclude_servers(
                        &source.exclude_servers,
                        source.source_server_idx,
                    ),
                )
            {
                if !crc_valid {
                    self.metrics.crc_errors.fetch_add(1, Ordering::Relaxed);
                }
                self.handle_decode_failure(
                    segment_id,
                    "damaged yEnc article; another server is asked before its bytes stand",
                    &source.exclude_servers,
                    source.source_server_idx,
                );
                return;
            }
            // The job's root takes the article as placed, damaged or not, and
            // is the job's only writer; only the job's progress counts it here.
            self.graph_article_decoded(
                segment_id,
                file_offset,
                data,
                !retain_damage,
                !encoding.is_uu(),
                yenc_layout.file_size,
                declared_part_end(expected_layout, yenc_layout),
                segments,
            );
            if retain_damage && !encoding.is_uu() {
                // The damaged copy stands. Its extent, and the range its header
                // named, still say where the part behind it begins.
                if let Some(file) = self
                    .jobs
                    .get_mut(&job_id)
                    .and_then(|state| state.articles.assembly_mut()?.file_mut(file_id))
                {
                    file.note_retained_damage(
                        segment_id.segment_number,
                        file_offset,
                        decoded_size,
                        crc_valid && truncation_suspected,
                        declared_part_end(expected_layout, yenc_layout),
                    );
                }
                self.settle_unanchored_behind(segment_id);
            }
            if !crc_valid {
                self.metrics.crc_errors.fetch_add(1, Ordering::Relaxed);
            }
            self.metrics
                .bytes_decoded
                .fetch_add(u64::from(decoded_size), Ordering::Relaxed);
            self.metrics
                .segments_decoded
                .fetch_add(1, Ordering::Relaxed);
            if let Some(state) = self.jobs.get_mut(&job_id) {
                state.downloaded_bytes += u64::from(decoded_size);
                if let Some(category_bytes) = state.category_bytes.as_ref() {
                    category_bytes.fetch_add(u64::from(decoded_size), Ordering::Relaxed);
                }
            }
            self.send_segment_event(|| PipelineEvent::SegmentDecoded {
                segment_id,
                decoded_size,
                file_offset,
                crc_valid,
            });
        }
    }

    // Retain the name a uuencode `begin` header stated for this file.
    //
    // First non-empty wins. A well-formed post states it once, and a
    // duplicate of that part restates the same thing; if two parts of one file
    // somehow disagree, the earlier claim is the one the assembly has already
    // been reasoning about, so changing identity mid-file would be the more
    // surprising answer.
    fn note_uu_filename(&mut self, file_id: NzbFileId, name: &str) {
        let uu = self.uu_file(file_id);
        if uu.filename.is_none() {
            uu.filename = Some(name.to_string());
        }
    }

    // Decide where a uuencode part goes, or that it cannot go anywhere yet.
    //
    // Every offset here is a sum of DECODED lengths. No NZB-declared byte
    // count participates: those are encoded sizes, and mixing the two units is
    // exactly how a uuencode file gets scattered.
    fn place_uu_segment(
        &mut self,
        file_id: NzbFileId,
        segment_number: u32,
        facts: crate::pipeline::UuSegmentFacts,
        decoded_len: usize,
        _data: &DecodedChunk,
    ) -> UuPlacement {
        let max_pending = self.uu_park_max_segments;
        let already_placed = self
            .jobs
            .get(&file_id.job_id)
            .and_then(|state| state.articles.assembly()?.file(file_id))
            .and_then(|file| file.placement_of(segment_number));

        let uu = self.uu_file(file_id);
        uu.damaged |= facts.damaged;
        uu.saw_end |= facts.ended;

        // A part that already has a placement is a duplicate: the same article
        // decoded twice. Re-place it exactly where its first copy went, so the
        // rewrite lands on its own bytes. Deriving a fresh offset from the
        // cursor would be wrong — the cursor has already moved past it.
        if let Some((offset, _)) = already_placed {
            return UuPlacement::Place(offset);
        }

        if segment_number == uu.next_index {
            let offset = uu.next_offset;
            uu.next_index = uu.next_index.saturating_add(1);
            uu.next_offset = uu.next_offset.saturating_add(decoded_len as u64);
            self.pass_given_up_uu(file_id);
            return UuPlacement::Place(offset);
        }

        if segment_number < uu.next_index {
            // Behind the cursor with no recorded placement: the cursor was
            // advanced past this ordinal because it failed permanently, and the
            // file was shifted to close the gap. Its bytes no longer have a
            // home — re-placing them would overwrite a later part, and
            // re-fetching them would fetch bytes that can never be placed.
            return UuPlacement::Stale;
        }

        if uu.parked.len() >= max_pending
            && uu
                .parked
                .last_key_value()
                .is_some_and(|(highest, _)| segment_number >= *highest)
        {
            return UuPlacement::Displaced;
        }

        UuPlacement::Park
    }

    // Hold a part that arrived ahead of its prefix, returning any part the
    // park had to displace to stay inside its bound.
    //
    // The displaced part's bytes are dropped here, so the caller **must**
    // return its ordinal to the download queue. The download layer already
    // considers that segment finished; without a fresh fetch its data exists
    // nowhere, and the cursor would wedge permanently the moment it reached
    // that ordinal.
    async fn spill_parked_bytes(
        &self,
        file_id: NzbFileId,
        data: DecodedChunk,
    ) -> Result<ParkedBytes, SegmentWriteError> {
        let decoded_bytes = data.len_bytes();
        let spool_dir = self
            .jobs
            .get(&file_id.job_id)
            .map(|state| crate::pipeline::held::held_spool_dir(&state.working_dir))
            .ok_or_else(|| {
                SegmentWriteError::new(file_id, io::Error::other("the job has left the pipeline"))
            })?;
        let entry = tokio::task::spawn_blocking(move || {
            crate::pipeline::held::prepare_held_spool_dir(&spool_dir)?;
            // tempfile creates its file with owner-only permissions, under a
            // name no other part has. A write that fails removes its file.
            let mut file = Builder::new()
                .prefix(crate::pipeline::held::HELD_SPOOL_PREFIX)
                .tempfile_in(&spool_dir)?;
            data.write_to(file.as_file_mut())?;
            // The file is closed before it is published to the park, so a
            // reader only observes a full write; it is not synced, and a
            // restart reads back only a file of the length its row records.
            let path = file.into_temp_path().keep().map_err(|error| error.error)?;
            Ok::<_, io::Error>(ParkedBytes::Spilled {
                path,
                decoded_bytes,
            })
        })
        .await
        .map_err(|error| {
            SegmentWriteError::new(
                file_id,
                io::Error::other(format!("UU spool task failed: {error}")),
            )
        })?
        .map_err(|error| SegmentWriteError::new(file_id, error))?;
        Ok(entry)
    }

    fn release_uu_parked_entry(&mut self, entry: ParkedBytes) {
        self.release_parked_bytes(entry);
        self.release_uu_parked_segment();
    }

    // Spill to its spool every part a parking job holds in memory, releasing
    // the memory ledger for what left it, then park the job, so its
    // checkpoint names the parts spooled. A part the spool cannot admit
    // stays in memory; a spill that fails to write fails the job, as any
    // spool write does. A job resumed meanwhile keeps its parts where they
    // are, and runs on.
    pub(crate) async fn spill_parked_parts(&mut self) {
        let spilling: Vec<JobId> = self.spilling_jobs.drain().collect();
        for job_id in spilling {
            let paused = self
                .jobs
                .get(&job_id)
                .is_some_and(|state| state.run_state == crate::jobs::model::RunState::Paused);
            if !paused {
                continue;
            }
            match self.spill_job_parts(job_id).await {
                Ok(()) => self.park_spilled_job(job_id),
                Err(error) => self.fail_job_for_disk_write(error, "held part spool write failed"),
            }
        }
    }

    async fn spill_job_parts(&mut self, job_id: JobId) -> Result<(), SegmentWriteError> {
        let uu: Vec<(NzbFileId, u32, usize)> = self
            .uu_files
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .flat_map(|(file_id, uu)| {
                uu.parked
                    .iter()
                    .filter_map(move |(ordinal, entry)| match entry {
                        ParkedBytes::Memory(data) => Some((*file_id, *ordinal, data.len_bytes())),
                        ParkedBytes::Spilled { .. } => None,
                    })
            })
            .collect();
        for (file_id, ordinal, bytes) in uu {
            if !self.admit_spool_bytes(bytes) {
                continue;
            }
            let Some(ParkedBytes::Memory(data)) = self
                .uu_files
                .get_mut(&file_id)
                .and_then(|uu| uu.parked.remove(&ordinal))
            else {
                continue;
            };
            let spilled = self.spill_parked_bytes(file_id, data).await;
            self.release_write_buffered(bytes, 1);
            let Ok(spilled) = spilled else {
                self.release_uu_parked_segment();
                return spilled.map(|_| ());
            };
            self.note_uu_spooled(bytes, 1);
            if let Some(uu) = self.uu_files.get_mut(&file_id) {
                uu.parked.insert(ordinal, spilled);
            }
        }
        let unanchored: Vec<(NzbFileId, u32, usize)> = self
            .unanchored_parked
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .flat_map(|(file_id, parked)| {
                parked
                    .iter()
                    .filter_map(move |(ordinal, part)| match &part.bytes {
                        ParkedBytes::Memory(data) => Some((*file_id, *ordinal, data.len_bytes())),
                        ParkedBytes::Spilled { .. } => None,
                    })
            })
            .collect();
        for (file_id, ordinal, bytes) in unanchored {
            if !self.admit_spool_bytes(bytes) {
                continue;
            }
            let Some(parked) = self.unanchored_parked.get_mut(&file_id) else {
                continue;
            };
            let Some(mut part) = parked.remove(&ordinal) else {
                continue;
            };
            let ParkedBytes::Memory(data) = part.bytes else {
                continue;
            };
            let spilled = self.spill_parked_bytes(file_id, data).await;
            self.release_write_buffered(bytes, 1);
            part.bytes = spilled?;
            self.note_uu_spooled(bytes, 1);
            self.unanchored_parked
                .entry(file_id)
                .or_default()
                .insert(ordinal, part);
        }
        Ok(())
    }

    // Drop a held part's bytes and release the ledger they were charged to.
    pub(in crate::pipeline) fn release_parked_bytes(&mut self, bytes: ParkedBytes) {
        match bytes {
            ParkedBytes::Memory(data) => self.release_write_buffered(data.len_bytes(), 1),
            ParkedBytes::Spilled {
                path,
                decoded_bytes,
            } => {
                self.release_uu_spooled(decoded_bytes, 1);
                if let Err(error) = std::fs::remove_file(&path)
                    && error.kind() != io::ErrorKind::NotFound
                {
                    warn!(path = %path.display(), %error, "could not remove a held part's spool file");
                }
            }
        }
    }

    // Store an ahead-of-cursor part after its memory-or-disk form has been
    // fully prepared. Memory is charged only after insertion succeeds; disk
    // spill writes complete before their entry becomes visible to the cursor.
    async fn park_uu_segment(
        &mut self,
        file_id: NzbFileId,
        segment_number: u32,
        data: DecodedChunk,
    ) -> Result<Vec<u32>, SegmentWriteError> {
        let max_pending = self.uu_park_max_segments;
        let decoded_bytes = data.len_bytes();
        let projected_resident = self.write_buffered_bytes.saturating_add(decoded_bytes);
        // Resident parked bytes only drain when the cursor's own part arrives,
        // so the park spills at the soft write threshold rather than the hard
        // one: a park that filled the whole budget would latch hard write
        // pressure, pause dispatch, and wait on a fetch that pressure holds.
        let (_, _, write_soft, _) = self.download_pressure_limits();
        let spills = projected_resident as u64 >= write_soft;
        if self.uu_spool_admission_capped(0) || (spills && !self.admit_uu_spill(decoded_bytes)) {
            // The part is already decoded, but holding it would exceed the
            // aggregate cache cap or consume the intermediate filesystem's
            // reserved free space (or, for a spill, the filesystem cannot be
            // measured at all). Requeue without retry burn, exactly like a
            // per-file farthest-part displacement.
            return Ok(vec![segment_number]);
        }
        let entry = if spills {
            self.spill_parked_bytes(file_id, data).await?
        } else {
            ParkedBytes::Memory(data)
        };
        let inserted_spilled = entry.is_spilled();
        let inserted_bytes = entry.decoded_bytes();
        let Some(uu) = self.uu_files.get_mut(&file_id) else {
            return Ok(Vec::new());
        };

        let replacement = uu.parked.insert(segment_number, entry);
        let mut displaced = Vec::new();
        let mut displaced_entries = Vec::new();
        while uu.parked.len() > max_pending {
            // Displace from the far end: those parts are the furthest from the
            // cursor, so re-fetching them is the least urgent work.
            let Some(highest) = uu.parked.keys().next_back().copied() else {
                break;
            };
            let entry = uu
                .parked
                .remove(&highest)
                .expect("UU park contained its selected farthest ordinal");
            displaced.push(highest);
            displaced_entries.push(entry);
        }

        if inserted_spilled {
            self.note_uu_spooled(inserted_bytes, 1);
        } else {
            self.note_write_buffered(inserted_bytes, 1);
        }
        self.note_uu_parked_segment();
        if let Some(entry) = replacement {
            self.release_uu_parked_entry(entry);
        }
        for entry in displaced_entries {
            self.release_uu_parked_entry(entry);
        }
        Ok(displaced)
    }

    // Return a uuencode segment to the download queue because of park
    // pressure, without charging it any retry budget.
    //
    // Park pressure is an ORDERING condition, not a data condition: the
    // segment downloaded and decoded perfectly, it simply arrived too far
    // ahead of the cursor to be held. Charging it against
    // `MAX_SEGMENT_RETRIES` would let a pathological arrival order manufacture
    // permanent file damage out of articles that were never actually bad, so
    // this mirrors the zero-burn requeue the 430-exclusion path uses.
    //
    // The per-segment counter here exists only to bound livelock, and is
    // deliberately not the decode-failure counter. It counts displacements
    // seen while the file's cursor stood at one ordinal, and starts over once
    // the cursor has moved: a part of a long file legitimately bounces many
    // times while lanes ahead of the cursor drain, and each of those bounces
    // follows cursor progress. Only a run of displacements with no progress
    // between them is a cycle, and that is what the bound catches.
    // Return a park-displaced segment to the download queue, falling back to
    // the counted failure path only if it has been displaced implausibly often.
    fn requeue_displaced_uu_segment(&mut self, segment_id: SegmentId) {
        if self.requeue_uu_segment_for_ordering(segment_id) {
            return;
        }
        // Livelock guard reached: treat it as a real failure so the segment
        // cannot cycle forever. See the note on the requeue budget for why this
        // is not expected outside a test with an artificially tiny park.
        self.metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
        self.handle_decode_failure(
            segment_id,
            "uuencode reorder park displaced this segment repeatedly",
            &[],
            None,
        );
    }

    // An article that could not say where it starts, arriving before the
    // ordinal it must follow.
    //
    // The bytes are fine; they simply have nowhere to go yet. They are held,
    // decoded, until the predecessor is placed, and released through the same
    // commit path the moment it is — so an out-of-order arrival costs memory
    // for a while, not another fetch. The hold is bounded by the same soft
    // write threshold and per-file part count as the uuencode reorder park, and
    // is not spilled while its job runs: an article that does not fit is
    // asked for again instead, without its retry budget. A paused job's held
    // articles spill to its spool as it parks.
    fn park_or_requeue_unanchored(&mut self, mut result: DecodeResult, source: SegmentSource) {
        let segment_id = result.segment_id;
        let file_id = segment_id.file_id;
        let ordinal = segment_id.segment_number;
        let predecessor = SegmentId {
            file_id,
            segment_number: ordinal.saturating_sub(1),
        };
        if self.segment_terminal_states.contains_key(&predecessor) {
            // The part it must follow was given up leaving nothing to measure
            // from — no placement and no extent this article could follow, or
            // the anchor would have been found above and these bytes placed.
            // Waiting is pointless and a fetch would bring back the same
            // article.
            self.give_up_unanchored(segment_id);
            return;
        }
        let bytes = result.data.len_bytes();
        let replaced = self
            .unanchored_parked
            .get(&file_id)
            .and_then(|parked| parked.get(&ordinal))
            .map(|part| match &part.bytes {
                ParkedBytes::Memory(data) => data.len_bytes(),
                ParkedBytes::Spilled { .. } => 0,
            });
        let parked_parts = self
            .unanchored_parked
            .get(&file_id)
            .map_or(0, BTreeMap::len);
        let (_, _, write_soft, _) = self.download_pressure_limits();
        let resident = self
            .write_buffered_bytes
            .saturating_sub(replaced.unwrap_or(0))
            .saturating_add(bytes);
        let fits = (resident as u64) < write_soft
            && (replaced.is_some() || parked_parts < self.uu_park_max_segments);
        if !fits {
            self.requeue_segment_awaiting_predecessor(segment_id);
            return;
        }
        let data = std::mem::replace(&mut result.data, DecodedChunk::from(Vec::<u8>::new()));
        self.note_write_buffered(bytes, 1);
        let replaced = self.unanchored_parked.entry(file_id).or_default().insert(
            ordinal,
            UnanchoredPart {
                result,
                bytes: ParkedBytes::Memory(data),
                source,
            },
        );
        if let Some(replaced) = replaced {
            self.release_parked_bytes(replaced.bytes);
        }
        debug!(
            segment = %segment_id,
            "article declared no usable start and its predecessor is unplaced; held until it is"
        );
    }

    // The parked article that directly follows `predecessor`, once
    // `predecessor` has an end for it to start at: a placement, or — once no
    // further copy of it is coming — the extent its damaged bytes occupy. A
    // part that left neither behind anchors nothing and releases nothing.
    // A held article whose bytes cannot be read back from the spool fails
    // its job, as any other spool read does.
    async fn take_unanchored_successor(
        &mut self,
        file_id: NzbFileId,
        predecessor: u32,
    ) -> Option<(DecodeResult, SegmentSource)> {
        if self.unanchored_parked.is_empty() {
            return None;
        }
        let successor = predecessor.checked_add(1)?;
        if !self
            .unanchored_parked
            .get(&file_id)
            .is_some_and(|parked| parked.contains_key(&successor))
        {
            return None;
        }
        let settled = self.ordinal_settled(SegmentId {
            file_id,
            segment_number: predecessor,
        });
        let anchored = self
            .jobs
            .get(&file_id.job_id)
            .and_then(|state| state.articles.assembly()?.file(file_id))
            .is_some_and(|file| file.anchor_end_after(predecessor, settled).is_some());
        if !anchored {
            return None;
        }
        let parked = self.unanchored_parked.get_mut(&file_id)?;
        let UnanchoredPart {
            mut result,
            bytes,
            source,
        } = parked.remove(&successor)?;
        if parked.is_empty() {
            self.unanchored_parked.remove(&file_id);
        }
        match self.take_parked_bytes(file_id, bytes).await {
            Ok(data) => {
                result.data = data;
                Some((result, source))
            }
            Err(error) => {
                self.fail_job_for_disk_write(error, "held article spool read failed");
                None
            }
        }
    }

    // Whether no further copy of the ordinal is coming: it was given up, or
    // its damaged copy stands.
    fn ordinal_settled(&self, segment_id: SegmentId) -> bool {
        self.segment_terminal_states.contains_key(&segment_id)
            || self
                .jobs
                .get(&segment_id.file_id.job_id)
                .and_then(|state| state.articles.assembly()?.file(segment_id.file_id))
                .is_some_and(|file| file.segment_has_retained_damage(segment_id.segment_number))
    }

    // Parked articles that were waiting on `segment_id`, which has just been
    // given up. Each of them anchors on the one before it, so the whole run
    // after it has lost its only way to be placed.
    pub(crate) fn take_unanchored_dependents(&mut self, segment_id: SegmentId) -> Vec<SegmentId> {
        let file_id = segment_id.file_id;
        let Some(parked) = self.unanchored_parked.get_mut(&file_id) else {
            return Vec::new();
        };
        let mut released = Vec::new();
        let mut next = segment_id.segment_number;
        while let Some(ordinal) = next.checked_add(1) {
            let Some(part) = parked.remove(&ordinal) else {
                break;
            };
            released.push(part);
            next = ordinal;
        }
        if parked.is_empty() {
            self.unanchored_parked.remove(&file_id);
        }
        released
            .into_iter()
            .map(|part| {
                let segment_id = part.result.segment_id;
                self.release_parked_bytes(part.bytes);
                segment_id
            })
            .collect()
    }

    // Release the articles held behind an ordinal that has just settled for
    // good with its damaged bytes left on disk.
    //
    // The booking that settles an ordinal is not a place where a decoded part
    // can be written, so it only notes which ordinals it unblocked; the run
    // behind each of them is released here, through the same commit path
    // every other part takes, and each part placed anchors the next.
    pub(crate) async fn release_settled_unanchored_runs(&mut self) {
        while let Some(settled) = self.pending_unanchored_release.pop() {
            let file_id = settled.file_id;
            if self.uu_files.contains_key(&file_id) {
                self.release_ready_uu_parts(file_id).await;
                continue;
            }
            let mut anchor = settled.segment_number;
            while let Some((result, source)) = self.take_unanchored_successor(file_id, anchor).await
            {
                anchor = result.segment_id.segment_number;
                self.handle_decode_success_inner(result, source).await;
            }
        }
    }

    // Retire an unanchored article whose predecessor was given up. It is
    // booked like any other article that never decoded into a placement, so
    // repair accounts for it and the job does not wait on it.
    pub(crate) fn give_up_unanchored(&mut self, segment_id: SegmentId) {
        warn!(
            segment = %segment_id,
            "article declared no usable start and the part before it was given up; leaving it to repair"
        );
        self.metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .segments_failed_permanent
            .fetch_add(1, Ordering::Relaxed);
        self.book_terminal_segment(segment_id, SegmentTerminalState::DecodeExhausted);
    }

    // The fallback when an unanchored article cannot be held: ask for it
    // again, without excluding the server that served it and without spending
    // its retry budget. The bound counts attempts since the file last placed
    // anything, so a long file that is steadily filling in never exhausts it,
    // while an article behind an ordinal that never arrives still stops.
    fn requeue_segment_awaiting_predecessor(&mut self, segment_id: SegmentId) {
        const MAX_ANCHOR_REQUEUES: u32 = 8;

        let placed = self
            .jobs
            .get(&segment_id.file_id.job_id)
            .and_then(|state| state.articles.assembly()?.file(segment_id.file_id))
            .map_or(0, |file| file.placed_segment_count());
        let (attempts, seen_placed) = self
            .unanchored_requeues
            .entry(segment_id)
            .or_insert((0, placed));
        if *seen_placed != placed {
            *seen_placed = placed;
            *attempts = 0;
        }
        *attempts += 1;
        if *attempts <= MAX_ANCHOR_REQUEUES && self.push_requeued_segment(segment_id) {
            debug!(
                segment = %segment_id,
                "article declared no usable start and its predecessor is unplaced; requeued without retry budget"
            );
            return;
        }
        // Bound reached: treat it as a real failure so the segment cannot
        // cycle forever behind an ordinal that may never arrive.
        self.metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
        self.handle_decode_failure(
            segment_id,
            "article declared no usable start and its predecessor never arrived",
            &[],
            None,
        );
    }

    fn requeue_uu_segment_for_ordering(&mut self, segment_id: SegmentId) -> bool {
        const MAX_UU_PARK_REQUEUES: u32 = 8;

        let cursor = self
            .uu_files
            .get(&segment_id.file_id)
            .map(|uu| uu.next_index)
            .unwrap_or(0);
        let (attempts, seen_at) = self
            .uu_park_requeues
            .entry(segment_id)
            .or_insert((0, cursor));
        if *seen_at != cursor {
            *seen_at = cursor;
            *attempts = 0;
        }
        *attempts += 1;
        if *attempts > MAX_UU_PARK_REQUEUES {
            return false;
        }

        if !self.push_requeued_segment(segment_id) {
            return false;
        }
        crate::runtime::perf_probe::record(
            "download.uu.park_requeue",
            std::time::Duration::from_nanos(1),
        );
        debug!(
            segment = %segment_id,
            "uuencode part displaced by park pressure; requeued without retry budget"
        );
        true
    }

    // Queue a segment again exactly as first dispatched: full retry budget,
    // no excluded server. Used where an article has to come back for an
    // ordering reason rather than because anything was wrong with it.
    fn push_requeued_segment(&mut self, segment_id: SegmentId) -> bool {
        let job_id = segment_id.file_id.job_id;
        let Some(work) = self.download_retry_work(segment_id, 0, Vec::new()) else {
            return false;
        };
        if !self.jobs.contains_key(&job_id) {
            return false;
        }
        self.enqueue_download_work(work);
        self.update_queue_metrics();
        true
    }

    // The next parked part the cursor can release, if any.
    //
    // Called after a placement advances the cursor. Exactly one part is
    // released per call: it goes back through the ordinary placement path,
    // which advances the cursor again and so makes the call after it the one
    // that finds the next part. That keeps a single place where a segment is
    // placed, written and accounted for, however long the released run is.
    async fn take_next_ready_uu_segment(
        &mut self,
        file_id: NzbFileId,
    ) -> Result<Option<(u32, DecodedChunk)>, SegmentWriteError> {
        let next = self.uu_files.get_mut(&file_id).and_then(|uu| {
            let index = uu.next_index;
            uu.parked.remove(&index).map(|entry| (index, entry))
        });
        let Some((index, entry)) = next else {
            return Ok(None);
        };
        self.release_uu_parked_segment();
        let data = self.take_parked_bytes(file_id, entry).await?;
        Ok(Some((index, data)))
    }

    // A held part's bytes, out of memory or read back from the spool, with
    // the ledger they were charged to released.
    async fn take_parked_bytes(
        &mut self,
        file_id: NzbFileId,
        bytes: ParkedBytes,
    ) -> Result<DecodedChunk, SegmentWriteError> {
        match bytes {
            ParkedBytes::Memory(data) => {
                self.release_write_buffered(data.len_bytes(), 1);
                Ok(data)
            }
            ParkedBytes::Spilled {
                path,
                decoded_bytes,
            } => {
                // The entry left the disk park before its read starts. Its
                // recorded decoded length, rather than file metadata, bounds
                // allocation and validates the entire spool contents.
                self.release_uu_spooled(decoded_bytes, 1);
                let read_result = tokio::task::spawn_blocking(move || {
                    let read_result = (|| -> io::Result<DecodedChunk> {
                        let mut file = File::open(&path)?;
                        let mut bytes = Vec::new();
                        bytes.try_reserve_exact(decoded_bytes).map_err(|error| {
                            io::Error::other(format!(
                                "failed to reserve {decoded_bytes} bytes for UU spool read: {error}"
                            ))
                        })?;
                        bytes.resize(decoded_bytes, 0);
                        file.read_exact(&mut bytes)?;
                        let mut extra = [0u8; 1];
                        if file.read(&mut extra)? != 0 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "UU spool file contains more bytes than recorded",
                            ));
                        }
                        Ok(bytes.into())
                    })();
                    // Always remove the spool file, including after a
                    // truncated, oversized, or missing-file read failure.
                    let remove_result = std::fs::remove_file(&path);
                    match (read_result, remove_result) {
                        (Err(error), _) => Err(error),
                        (Ok(_), Err(error)) => Err(error),
                        (Ok(data), Ok(())) => Ok(data),
                    }
                })
                .await;
                read_result
                    .map_err(|error| {
                        SegmentWriteError::new(
                            file_id,
                            io::Error::other(format!("UU spool read task failed: {error}")),
                        )
                    })?
                    .map_err(|error| SegmentWriteError::new(file_id, error))
            }
        }
    }

    pub(in crate::pipeline) fn fail_job_for_disk_write(
        &mut self,
        error: SegmentWriteError,
        context: &'static str,
    ) {
        let job_id = error.file_id.job_id;
        let out_of_space = crate::operations::is_out_of_space(&error.source);
        let message = if out_of_space {
            format!(
                "{context} for {}: filesystem out of space or over quota ({})",
                error.file_id, error.source
            )
        } else {
            format!("{context} for {}: {}", error.file_id, error.source)
        };
        error!(
            job_id = job_id.0,
            file_id = %error.file_id,
            error = %error.source,
            out_of_space,
            context,
            "disk write failed; failing job"
        );
        self.fail_job(job_id, message);
    }
}
