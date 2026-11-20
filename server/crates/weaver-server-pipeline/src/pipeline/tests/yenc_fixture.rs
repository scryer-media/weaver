// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub fn truncation_suspected(result: &weaver_yenc::DecodeResult) -> bool {
    crate::pipeline::yenc_truncation_suspected(result)
}

pub async fn setup(temp: &TempDir, id: u64, sizes: &[u32]) -> (Pipeline, NzbFileId, PathBuf) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp).await;
    let job_id = JobId(id);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("yenc compatibility", "sample.bin", sizes),
    )
    .await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.par2_bytes = state.spec.total_bytes;
    // These tests deliver the results of already dispatched requests.
    state
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .drain_all();
    let path = state.working_dir.join("sample.bin");
    (
        pipeline,
        NzbFileId {
            job_id,
            file_index: 0,
        },
        path,
    )
}

pub async fn deliver(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    number: u32,
    offset: u64,
    bytes: &[u8],
    valid: bool,
) {
    let status = if valid {
        weaver_yenc::CrcVerification::Verified
    } else {
        weaver_yenc::CrcVerification::Mismatch
    };
    deliver_status(pipeline, file_id, number, offset, bytes, status).await;
}

pub async fn deliver_status(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    number: u32,
    offset: u64,
    bytes: &[u8],
    status: weaver_yenc::CrcVerification,
) {
    deliver_from(pipeline, file_id, number, offset, bytes, status, None).await;
}

pub async fn deliver_from(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    number: u32,
    offset: u64,
    bytes: &[u8],
    status: weaver_yenc::CrcVerification,
    source_server_idx: Option<usize>,
) {
    // Every part of these fixtures decodes to four bytes, and the article says
    // the file it belongs to is as long as that makes it. An article for a
    // job already gone names no length.
    let file_size = pipeline.jobs.get(&file_id.job_id).map_or(0, |state| {
        state.spec.files[file_id.file_index as usize].segments.len() as u64 * 4
    });
    deliver_declared(
        pipeline,
        file_id,
        number,
        offset,
        bytes,
        status,
        source_server_idx,
        file_size,
        None,
        false,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
pub async fn deliver_declared(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    number: u32,
    offset: u64,
    bytes: &[u8],
    status: weaver_yenc::CrcVerification,
    source_server_idx: Option<usize>,
    declared_size: u64,
    expected_file_crc: Option<u32>,
    truncation_suspected: bool,
) {
    let len = bytes.len() as u64;
    let yenc_name = pipeline
        .jobs
        .get(&file_id.job_id)
        .map(|state| {
            state.spec.files[file_id.file_index as usize]
                .filename
                .clone()
        })
        .unwrap_or_default();
    pipeline
        .handle_decode_success(
            DecodeResult {
                segment_id: SegmentId {
                    file_id,
                    segment_number: number,
                },
                raw_size: len,
                encoding: SegmentEncoding::Yenc,
                // Consistently wrong part, count and end must never override
                // usable begin plus actual bytes, even after the old threshold.
                yenc_layout: YencLayoutAssertions {
                    file_size: declared_size,
                    part: Some(99),
                    total: Some(402),
                    begin: Some(offset + 1),
                    end: None,
                },
                crc_valid: status != weaver_yenc::CrcVerification::Mismatch,
                part_crc_verified: status == weaver_yenc::CrcVerification::Verified,
                part_crc: par2_rs::checksum::crc32(bytes),
                truncation_suspected,
                expected_file_crc,
                data: DecodedChunk::from(bytes.to_vec()),
                yenc_name,
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                segments: vec![weaver_yenc::Segment {
                    file_offset: offset,
                    len,
                    crc32: par2_rs::checksum::crc32(bytes),
                }],
            },
            SegmentSource {
                servers: PoolServers::stamped(
                    pipeline.pool_generation,
                    source_server_idx,
                    weaver_nntp::ServerSet::EMPTY,
                ),
            },
        )
        .await;
}

// An article whose `=ypart begin=` could not be read: intact bytes with no
// position of their own.
pub async fn deliver_unanchored(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    number: u32,
    bytes: &[u8],
) {
    deliver_unanchored_status(
        pipeline,
        file_id,
        number,
        bytes,
        weaver_yenc::CrcVerification::Verified,
        false,
    )
    .await;
}

// The same article, with whatever its body turned out to be worth.
pub async fn deliver_unanchored_status(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    number: u32,
    bytes: &[u8],
    status: weaver_yenc::CrcVerification,
    truncation_suspected: bool,
) {
    let len = bytes.len() as u64;
    let yenc_name = pipeline.jobs[&file_id.job_id].spec.files[file_id.file_index as usize]
        .filename
        .clone();
    pipeline
        .handle_decode_success(
            DecodeResult {
                segment_id: SegmentId {
                    file_id,
                    segment_number: number,
                },
                raw_size: len,
                encoding: SegmentEncoding::Yenc,
                yenc_layout: YencLayoutAssertions {
                    file_size: 8,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                crc_valid: status != weaver_yenc::CrcVerification::Mismatch,
                part_crc_verified: status == weaver_yenc::CrcVerification::Verified,
                part_crc: par2_rs::checksum::crc32(bytes),
                truncation_suspected,
                expected_file_crc: None,
                data: DecodedChunk::from(bytes.to_vec()),
                yenc_name,
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                // No grid may be published against a position the article
                // could not state.
                segments: Vec::new(),
            },
            SegmentSource {
                servers: PoolServers::stamped(
                    pipeline.pool_generation,
                    Some(0),
                    weaver_nntp::ServerSet::EMPTY,
                ),
            },
        )
        .await;
}

// Every article of the file is in the job's root intact: none lost, none
// standing as damage.
pub fn in_hand(pipeline: &Pipeline, file_id: NzbFileId) -> bool {
    let state = &pipeline.jobs[&file_id.job_id];
    let count = state.spec.files[file_id.file_index as usize].segments.len() as u32;
    let file = state
        .articles
        .active()
        .unwrap()
        .assembly
        .file(file_id)
        .unwrap();
    (0..count).all(|number| {
        article_in_hand(pipeline, file_id, number) && !file.segment_has_retained_damage(number)
    })
}

// The article's bytes are in the job's root.
pub fn article_in_hand(pipeline: &Pipeline, file_id: NzbFileId, number: u32) -> bool {
    let segment_id = SegmentId {
        file_id,
        segment_number: number,
    };
    pipeline.graph_article_settled(segment_id)
        && !pipeline.segment_terminal_states.contains_key(&segment_id)
}

// A damaged article that still named its own range in its header. The bytes
// are kept for repair; the range it declared is where the next part starts.
pub async fn deliver_damaged_declaring(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    number: u32,
    offset: u64,
    declared_end: u64,
    declared_file_size: u64,
    bytes: &[u8],
) {
    let len = bytes.len() as u64;
    let yenc_name = pipeline.jobs[&file_id.job_id].spec.files[file_id.file_index as usize]
        .filename
        .clone();
    pipeline
        .handle_decode_success(
            DecodeResult {
                segment_id: SegmentId {
                    file_id,
                    segment_number: number,
                },
                raw_size: len,
                encoding: SegmentEncoding::Yenc,
                yenc_layout: YencLayoutAssertions {
                    file_size: declared_file_size,
                    part: Some(number + 1),
                    total: None,
                    begin: Some(offset + 1),
                    end: Some(declared_end),
                },
                crc_valid: false,
                part_crc_verified: false,
                part_crc: par2_rs::checksum::crc32(bytes),
                truncation_suspected: false,
                expected_file_crc: None,
                data: DecodedChunk::from(bytes.to_vec()),
                yenc_name,
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                segments: Vec::new(),
            },
            SegmentSource {
                servers: PoolServers::stamped(
                    pipeline.pool_generation,
                    Some(0),
                    weaver_nntp::ServerSet::EMPTY,
                ),
            },
        )
        .await;
}
// A four-part file of four-byte parts whose NZB declares eight encoded bytes
// a part. The bad article is ordinal 0 carrying a header that places it at
// 12, where part 3 belongs: its own declared file size admits that, but the
// NZB cannot, since nothing comes before ordinal 0.
pub async fn deliver_part(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    number: u32,
    offset: u64,
    bytes: &[u8],
) {
    deliver_declared(
        pipeline,
        file_id,
        number,
        offset,
        bytes,
        weaver_yenc::CrcVerification::Verified,
        None,
        16,
        None,
        false,
    )
    .await;
}

// Ordinal 1 arrives first and claims where ordinal 0 belongs; ordinal 0 then
// claims where ordinal 1 belongs. The two contradict each other, and the one
// the NZB cannot vouch for is refused, whether or not the process restarted
// between the two arrivals: a restored job knows where its intact articles
// were placed.
pub async fn assert_a_crossed_neighbour_is_refused(pipeline: &mut Pipeline, file_id: NzbFileId) {
    deliver_part(pipeline, file_id, 0, 4, b"aaaa").await;
    let file = pipeline.jobs[&file_id.job_id]
        .articles
        .active()
        .unwrap()
        .assembly
        .file(file_id)
        .unwrap();
    assert_eq!(file.placement_of(1), Some((0, 4)));
    assert_eq!(
        file.placement_of(0),
        None,
        "the crossed article is placed nowhere"
    );
}

pub fn assembled_file(pipeline: &Pipeline, id: NzbFileId) -> &crate::jobs::assembly::FileAssembly {
    pipeline.jobs[&id.job_id]
        .articles
        .active()
        .unwrap()
        .assembly
        .file(id)
        .unwrap()
}
pub fn retained(file: &crate::jobs::assembly::FileAssembly, number: u32) -> bool {
    file.segment_has_retained_damage(number)
}
pub fn truncation_only(file: &crate::jobs::assembly::FileAssembly, number: u32) -> bool {
    file.segment_damage_is_truncation_only(number)
}
pub fn is_decode_exhausted(pipeline: &Pipeline, id: SegmentId) -> bool {
    pipeline.segment_terminal_states.get(&id) == Some(&SegmentTerminalState::DecodeExhausted)
}
pub fn mark_missing(pipeline: &mut Pipeline, id: SegmentId) {
    pipeline.book_terminal_segment(id, SegmentTerminalState::Missing);
}
pub fn is_terminal(pipeline: &Pipeline, id: SegmentId) -> bool {
    pipeline.segment_terminal_states.contains_key(&id)
}
pub fn terminals_empty(pipeline: &Pipeline) -> bool {
    pipeline.segment_terminal_states.is_empty()
}
pub fn set_total_park_limit(pipeline: &mut Pipeline, n: usize) {
    pipeline.unanchored_park_max_parts = n;
}
pub fn file_park_limit(pipeline: &Pipeline) -> usize {
    pipeline.uu_park_max_segments
}
pub fn held_empty(pipeline: &Pipeline) -> bool {
    pipeline.unanchored_parked.is_empty()
}
pub fn held_parts(pipeline: &Pipeline) -> usize {
    pipeline.unanchored_parked.parts()
}
pub fn held_for_file(pipeline: &Pipeline, id: NzbFileId) -> usize {
    pipeline.unanchored_parked[&id].len()
}
pub fn has_held(pipeline: &Pipeline, id: NzbFileId, n: u32) -> bool {
    pipeline.unanchored_parked[&id].contains_key(&n)
}
pub fn requeue(pipeline: &Pipeline, id: SegmentId) -> Option<&(u32, usize)> {
    pipeline.unanchored_requeues.get(&id)
}
pub fn requeues_empty(pipeline: &Pipeline) -> bool {
    pipeline.unanchored_requeues.is_empty()
}
pub fn retries(pipeline: &Pipeline) -> &std::collections::HashMap<SegmentId, u32> {
    &pipeline.decode_retries
}
pub fn exhaust_retries(pipeline: &mut Pipeline, id: SegmentId) {
    pipeline.decode_retries.insert(id, MAX_SEGMENT_RETRIES);
}
pub async fn release_settled(pipeline: &mut Pipeline) {
    pipeline.release_settled_unanchored_runs().await;
}
pub fn is_settled(pipeline: &Pipeline, id: SegmentId) -> bool {
    pipeline.graph_article_settled(id)
}
pub fn failed_bytes(pipeline: &Pipeline, id: JobId) -> u64 {
    pipeline.jobs[&id].failed_bytes
}
pub fn set_retention(pipeline: &mut Pipeline, days: &[u32]) {
    pipeline.nntp = std::sync::Arc::new(retention_client(days));
}
pub async fn next_retry(pipeline: &mut Pipeline) -> DownloadWork {
    pipeline.retry_rx.recv().await.unwrap().work
}
pub fn groups(pipeline: &Pipeline, id: NzbFileId) -> std::sync::Arc<[String]> {
    std::sync::Arc::clone(
        pipeline.jobs[&id.job_id]
            .articles
            .download_queue()
            .unwrap()
            .file_groups(id)
            .unwrap(),
    )
}
pub fn range_covered(pipeline: &Pipeline, id: NzbFileId, start: u64, end: u64) -> bool {
    pipeline.graph_jobs[&id.job_id]
        .namespace()
        .get(weaver_graph::MemberId(0))
        .unwrap()
        .coverage
        .covered(weaver_graph::ByteRange::new(start, end))
}

pub fn take_queued(pipeline: &mut Pipeline, id: SegmentId) {
    pipeline
        .jobs
        .get_mut(&id.file_id.job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .extract_matching(|work| work.segment_id == id);
}
pub fn queued_ids(pipeline: &Pipeline, id: JobId) -> std::collections::HashSet<SegmentId> {
    let mut ids = std::collections::HashSet::new();
    pipeline.jobs[&id]
        .articles
        .active()
        .unwrap()
        .download_queue
        .extend_segment_ids(&mut ids);
    ids
}
pub fn has_active_articles(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.jobs[&id].articles.active().is_some()
}
pub fn spool_path(pipeline: &Pipeline, file: NzbFileId, ordinal: u32) -> Option<&std::path::Path> {
    match &pipeline.unanchored_parked[&file][&ordinal].bytes {
        ParkedBytes::Memory(_) => None,
        ParkedBytes::Spilled { path, .. } => Some(path),
    }
}
pub async fn spill(pipeline: &mut Pipeline) {
    pipeline.spill_parked_parts().await;
}
pub fn resume_runtime(pipeline: &mut Pipeline, id: JobId) {
    pipeline.resume_job_runtime(id).unwrap();
}
pub fn checkpoint_held(pipeline: &Pipeline, id: JobId) -> Vec<(i64, i64, i64)> {
    use crate::jobs::graph_checkpoint::JobsGraphCheckpointInternalQueries;
    pipeline
        .db
        .load_graph_checkpoint(id)
        .unwrap()
        .expect("the running job left a checkpoint")
        .held_parts
        .into_iter()
        .map(|row| (row.file, row.ordinal, row.decoded_len))
        .collect()
}
