// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

async fn setup(temp: &TempDir, id: u64, sizes: &[u32]) -> (Pipeline, NzbFileId, PathBuf) {
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
    state.download_queue.drain_all();
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

pub(super) async fn deliver(
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

async fn deliver_status(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    number: u32,
    offset: u64,
    bytes: &[u8],
    status: weaver_yenc::CrcVerification,
) {
    deliver_from(pipeline, file_id, number, offset, bytes, status, None).await;
}

async fn deliver_from(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    number: u32,
    offset: u64,
    bytes: &[u8],
    status: weaver_yenc::CrcVerification,
    source_server_idx: Option<usize>,
) {
    deliver_declared(
        pipeline,
        file_id,
        number,
        offset,
        bytes,
        status,
        source_server_idx,
        // Consistently wrong: the declared size must never override a usable
        // begin plus the bytes that actually decoded.
        6,
        None,
        false,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn deliver_declared(
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
                source_server_idx,
                exclude_servers: Vec::new(),
            },
        )
        .await;
}

// Build a yEnc article out of bytes that need no escaping.
fn plain_article(headers: &[&str], body: &[u8], trailer: Option<&str>) -> Vec<u8> {
    let mut article = Vec::new();
    for header in headers {
        article.extend_from_slice(header.as_bytes());
        article.extend_from_slice(b"\r\n");
    }
    article.extend(body.iter().map(|byte| byte.wrapping_add(42)));
    if let Some(trailer) = trailer {
        article.extend_from_slice(b"\r\n");
        article.extend_from_slice(trailer.as_bytes());
        article.extend_from_slice(b"\r\n");
    }
    article
}

fn decoded(article: &[u8]) -> weaver_yenc::DecodeResult {
    let mut output = vec![0u8; 4096];
    weaver_yenc::decode(article, &mut output).unwrap()
}

// A part whose declared span accounts for every byte that arrived is complete
// even without a trailer: there is nothing left for a missing `=yend` to hide.
#[test]
fn a_confirmed_length_survives_a_missing_trailer() {
    let body = vec![0u8; 16];
    let article = plain_article(
        &[
            "=ybegin part=2 line=128 size=64 name=sample.bin",
            "=ypart begin=17 end=32",
        ],
        &body,
        None,
    );
    assert!(!crate::pipeline::yenc_truncation_suspected(&decoded(
        &article
    )));
}

// A body that stops short of its declared span, with no trailer and no
// checksum, is exactly the response another server should be asked for.
#[test]
fn a_short_body_without_a_trailer_is_suspected() {
    let body = vec![0u8; 8];
    let article = plain_article(
        &[
            "=ybegin part=2 line=128 size=64 name=sample.bin",
            "=ypart begin=17 end=32",
        ],
        &body,
        None,
    );
    let result = decoded(&article);
    assert!(result.defects.ypart_size_mismatch);
    assert!(crate::pipeline::yenc_truncation_suspected(&result));
}

// A single-part article that neither ends with a trailer nor accounts for its
// declared size is suspect for the same reason.
#[test]
fn a_single_part_body_short_of_its_declared_size_is_suspected() {
    let body = vec![0u8; 8];
    let article = plain_article(&["=ybegin line=128 size=64 name=sample.bin"], &body, None);
    assert!(crate::pipeline::yenc_truncation_suspected(&decoded(
        &article
    )));
}

// A trailer that disagrees with the bytes delivered is evidence on its own.
#[test]
fn a_trailer_size_that_disagrees_is_suspected() {
    let body = vec![0u8; 16];
    let article = plain_article(
        &[
            "=ybegin part=2 line=128 size=64 name=sample.bin",
            "=ypart begin=17 end=32",
        ],
        &body,
        Some("=yend size=999 part=2"),
    );
    let result = decoded(&article);
    assert!(result.defects.yend_size_mismatch);
    assert!(crate::pipeline::yenc_truncation_suspected(&result));
}

// A checksum that verifies proves the bytes whatever the size fields claim.
#[test]
fn a_verified_checksum_outranks_every_size_disagreement() {
    let body = vec![0u8; 16];
    let crc = par2_rs::checksum::crc32(&body);
    let article = plain_article(
        &[
            "=ybegin part=2 line=128 size=64 name=sample.bin",
            "=ypart begin=17 end=32",
        ],
        &body,
        Some(&format!("=yend size=999 part=2 pcrc32={crc:08x}")),
    );
    let result = decoded(&article);
    assert_eq!(result.crc_status, weaver_yenc::CrcVerification::Verified);
    assert!(!crate::pipeline::yenc_truncation_suspected(&result));
}

// An article whose `=ypart begin=` could not be read: intact bytes with no
// position of their own.
async fn deliver_unanchored(
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
async fn deliver_unanchored_status(
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
                source_server_idx: Some(0),
                exclude_servers: Vec::new(),
            },
        )
        .await;
}

// Held articles have no offset once the part they follow is given up, so they
// are retired with it rather than held for a placement that cannot come.
#[tokio::test]
async fn giving_up_a_predecessor_retires_the_articles_held_behind_it() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _path) = setup(&temp, 40231, &[12, 12, 12]).await;
    let buffered = pipeline.write_buffered_bytes;
    deliver_unanchored(&mut pipeline, file_id, 1, b"bbbb").await;
    deliver_unanchored(&mut pipeline, file_id, 2, b"cccc").await;
    let head = SegmentId {
        file_id,
        segment_number: 0,
    };
    pipeline.book_terminal_segment(head, SegmentTerminalState::Missing);
    assert!(pipeline.unanchored_parked.is_empty());
    assert_eq!(pipeline.write_buffered_bytes, buffered);
    for number in [1, 2] {
        assert_eq!(
            pipeline.segment_terminal_states.get(&SegmentId {
                file_id,
                segment_number: number,
            }),
            Some(&SegmentTerminalState::DecodeExhausted)
        );
    }
    assert!(pipeline.jobs[&file_id.job_id].download_queue.is_empty());
}

// An article that cannot be held is asked for again without retry budget, and
// the bound on that counts only attempts since the file last placed a part.
#[tokio::test]
async fn an_article_that_cannot_be_held_is_requeued_until_the_file_stalls() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _path) = setup(&temp, 40232, &[12, 12, 12]).await;
    pipeline.uu_park_max_segments = 0;
    let segment = SegmentId {
        file_id,
        segment_number: 2,
    };
    deliver_unanchored(&mut pipeline, file_id, 2, b"cccc").await;
    assert!(pipeline.unanchored_parked.is_empty());
    assert_eq!(pipeline.unanchored_requeues.get(&segment), Some(&(1, 0)));
    assert!(!pipeline.decode_retries.contains_key(&segment));
    assert_eq!(pipeline.jobs[&file_id.job_id].download_queue.len(), 1);

    deliver(&mut pipeline, file_id, 0, 0, b"aaaa", true).await;
    deliver_unanchored(&mut pipeline, file_id, 2, b"cccc").await;
    // The file moved on, so the count starts over rather than accumulating.
    assert_eq!(pipeline.unanchored_requeues.get(&segment), Some(&(1, 1)));
    assert!(!pipeline.segment_terminal_states.contains_key(&segment));
}

// An article arriving after the part it follows was given up is retired at
// once: there is no offset to wait for.
#[tokio::test]
async fn an_article_behind_a_given_up_predecessor_is_not_held() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _path) = setup(&temp, 40234, &[8, 8]).await;
    let head = SegmentId {
        file_id,
        segment_number: 0,
    };
    pipeline.book_terminal_segment(head, SegmentTerminalState::Missing);
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    assert!(pipeline.unanchored_parked.is_empty());
    assert_eq!(
        pipeline.segment_terminal_states.get(&SegmentId {
            file_id,
            segment_number: 1,
        }),
        Some(&SegmentTerminalState::DecodeExhausted)
    );
    assert!(pipeline.jobs[&file_id.job_id].download_queue.is_empty());
}
