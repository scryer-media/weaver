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

// Every article of the file is in the job's root intact: none lost, none
// standing as damage.
fn in_hand(pipeline: &Pipeline, file_id: NzbFileId) -> bool {
    let state = &pipeline.jobs[&file_id.job_id];
    let count = state.spec.files[file_id.file_index as usize].segments.len() as u32;
    let file = state.assembly.file(file_id).unwrap();
    (0..count).all(|number| {
        article_in_hand(pipeline, file_id, number) && !file.segment_has_retained_damage(number)
    })
}

// The article's bytes are in the job's root.
fn article_in_hand(pipeline: &Pipeline, file_id: NzbFileId, number: u32) -> bool {
    let segment_id = SegmentId {
        file_id,
        segment_number: number,
    };
    pipeline.graph_article_settled(segment_id)
        && !pipeline.segment_terminal_states.contains_key(&segment_id)
}

#[tokio::test]
async fn stale_metadata_assembles_twenty_parts_without_abandonment() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40201, &[8; 20]).await;
    for number in 0..20 {
        deliver(
            &mut pipeline,
            file_id,
            number,
            u64::from(number) * 4,
            &[number as u8; 4],
            true,
        )
        .await;
    }
    let expected: Vec<u8> = (0..20).flat_map(|number| [number; 4]).collect();
    assert_eq!(std::fs::read(path).unwrap(), expected);
    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert!(in_hand(&pipeline, file_id));
    for number in 0..20u32 {
        assert_eq!(file.placement_of(number), Some((u64::from(number) * 4, 4)));
    }
    assert!(pipeline.segment_terminal_states.is_empty());
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        20
    );
    assert_eq!(pipeline.jobs[&file_id.job_id].failed_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn shorter_replacement_next_segment_overwrites_the_old_tail() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40205, &[16, 16]).await;
    // One server, so the damaged copy is fetched again.
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    deliver(&mut pipeline, file_id, 0, 0, b"bad tail", false).await;
    let _retry = pipeline.retry_rx.recv().await.unwrap();
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    deliver(&mut pipeline, file_id, 1, 4, b"next", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"goodnext");
    assert!(in_hand(&pipeline, file_id));
    assert_eq!(pipeline.metrics.bytes_committed.load(Ordering::Relaxed), 8);
}

#[tokio::test]
async fn a_late_damaged_duplicate_cannot_overwrite_committed_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40206, &[8, 8]).await;
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    deliver(&mut pipeline, file_id, 0, 0, b"bad!", false).await;
    assert_eq!(&std::fs::read(path).unwrap()[..4], b"good");
    assert!(pipeline.decode_retries.is_empty());
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn cancellation_does_not_allow_a_late_retry_to_write() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40210, &[8, 8]).await;
    // One server, so the damaged copy is fetched again.
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    deliver(&mut pipeline, file_id, 0, 0, b"bad!", false).await;
    pipeline.fail_job(file_id.job_id, "cancelled fixture".to_string());
    settle_failed_releases(&mut pipeline).await;
    // Whatever the failed job left on disk is all there ever is.
    let left = std::fs::read(&path).ok();
    let retry = pipeline.retry_rx.recv().await.unwrap();
    assert_eq!(retry.work.segment_id.file_id, file_id);
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    assert_eq!(std::fs::read(&path).ok(), left);
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        0
    );
    assert_eq!(pipeline.write_buffered_bytes, 0);
}

// A part with no usable start is laid immediately after the part before it.
#[tokio::test]
async fn an_article_without_a_start_follows_the_one_before_it() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40227, &[8, 8]).await;
    deliver(&mut pipeline, file_id, 0, 0, b"head", true).await;
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    assert_eq!(std::fs::read(path).unwrap(), b"headtail");
    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert!(in_hand(&pipeline, file_id));
    assert_eq!(file.placement_of(1), Some((4, 4)));
    assert!(pipeline.unanchored_requeues.is_empty());
}

// Arriving before the part it must follow is an ordering condition, not
// damage: the decoded article is held, costs no fetch and no retry, and lands
// the moment its predecessor is placed.
#[tokio::test]
async fn an_article_without_a_start_waits_for_its_predecessor() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40228, &[8, 8]).await;
    let segment = SegmentId {
        file_id,
        segment_number: 1,
    };
    let buffered = pipeline.write_buffered_bytes;
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    assert!(pipeline.unanchored_parked[&file_id].contains_key(&1));
    assert_eq!(pipeline.write_buffered_bytes, buffered + 4);
    assert!(pipeline.unanchored_requeues.is_empty());
    assert!(!pipeline.decode_retries.contains_key(&segment));
    assert!(!pipeline.segment_terminal_states.contains_key(&segment));
    assert_eq!(pipeline.metrics.decode_errors.load(Ordering::Relaxed), 0);
    assert!(pipeline.jobs[&file_id.job_id].download_queue.is_empty());
    assert!(!article_in_hand(&pipeline, file_id, 1));
    deliver(&mut pipeline, file_id, 0, 0, b"head", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"headtail");
    assert!(pipeline.unanchored_parked.is_empty());
    assert_eq!(pipeline.write_buffered_bytes, buffered);
    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert!(in_hand(&pipeline, file_id));
    assert_eq!(file.placement_of(1), Some((4, 4)));
    assert!(pipeline.jobs[&file_id.job_id].download_queue.is_empty());
}

// A run of held articles releases in one pass once its head is placed, each
// laid after the one before it.
#[tokio::test]
async fn a_run_of_held_articles_releases_behind_its_head() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40230, &[12, 12, 12]).await;
    deliver_unanchored(&mut pipeline, file_id, 2, b"cccc").await;
    deliver_unanchored(&mut pipeline, file_id, 1, b"bbbb").await;
    assert_eq!(pipeline.unanchored_parked[&file_id].len(), 2);
    deliver(&mut pipeline, file_id, 0, 0, b"aaaa", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"aaaabbbbcccc");
    assert!(pipeline.unanchored_parked.is_empty());
    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert_eq!(file.placement_of(2), Some((8, 4)));
    assert!(in_hand(&pipeline, file_id));
}

// A predecessor that lands as damage is sent for again rather than placed,
// so the article held behind it keeps waiting and lands behind the verified
// replacement.
#[tokio::test]
async fn a_held_article_waits_out_a_damaged_predecessor() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40233, &[8, 8]).await;
    // A server the damaged copy can be asked of again.
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    deliver(&mut pipeline, file_id, 0, 0, b"hexd", false).await;
    assert!(pipeline.unanchored_parked[&file_id].contains_key(&1));
    deliver(&mut pipeline, file_id, 0, 0, b"head", true).await;
    assert!(pipeline.unanchored_parked.is_empty());
    assert_eq!(std::fs::read(path).unwrap(), b"headtail");
    assert!(in_hand(&pipeline, file_id));
}

// A damaged article that still named its own range in its header. The bytes
// are kept for repair; the range it declared is where the next part starts.
async fn deliver_damaged_declaring(
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
                source_server_idx: Some(0),
                exclude_servers: Vec::new(),
            },
        )
        .await;
}

// Bytes kept for repair still say where the part behind them begins. Once no
// further copy of the damaged part is coming, the article held behind it is
// laid at the boundary that part declared for itself — not booked as failed
// for repair to rebuild from scratch.
#[tokio::test(start_paused = true)]
async fn a_held_article_anchors_on_the_range_a_given_up_predecessor_declared() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40235, &[8, 8]).await;
    let head = SegmentId {
        file_id,
        segment_number: 0,
    };
    let tail = SegmentId {
        file_id,
        segment_number: 1,
    };
    let buffered = pipeline.write_buffered_bytes;
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    assert!(pipeline.unanchored_parked[&file_id].contains_key(&1));

    pipeline.decode_retries.insert(head, MAX_SEGMENT_RETRIES);
    // Three bytes of a five-byte range: the header still knows the range.
    deliver_damaged_declaring(&mut pipeline, file_id, 0, 0, 5, 9, b"bad").await;
    pipeline.release_settled_unanchored_runs().await;

    // The damaged copy stands in the job's root for repair.
    assert!(pipeline.graph_article_settled(head));
    assert!(!pipeline.segment_terminal_states.contains_key(&tail));
    assert!(pipeline.unanchored_parked.is_empty());
    assert_eq!(pipeline.write_buffered_bytes, buffered);
    assert_eq!(std::fs::read(&path).unwrap(), b"bad\0\0tail");

    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert_eq!(file.placement_of(1), Some((5, 4)));
    assert!(article_in_hand(&pipeline, file_id, 1));
    assert!(file.segment_has_retained_damage(0));
    // Completion stays gated on a repair verdict.
    assert!(!in_hand(&pipeline, file_id));
}

// The whole run held behind settled damage releases, each part anchoring the
// next exactly as it does behind a placement.
#[tokio::test(start_paused = true)]
async fn a_run_held_behind_settled_damage_releases_in_order() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40238, &[12, 12, 12]).await;
    let head = SegmentId {
        file_id,
        segment_number: 0,
    };
    deliver_unanchored(&mut pipeline, file_id, 2, b"cccc").await;
    deliver_unanchored(&mut pipeline, file_id, 1, b"bbbb").await;
    assert_eq!(pipeline.unanchored_parked[&file_id].len(), 2);

    pipeline.decode_retries.insert(head, MAX_SEGMENT_RETRIES);
    deliver_damaged_declaring(&mut pipeline, file_id, 0, 0, 4, 12, b"aa").await;
    pipeline.release_settled_unanchored_runs().await;

    assert!(pipeline.unanchored_parked.is_empty());
    assert_eq!(std::fs::read(&path).unwrap(), b"aa\0\0bbbbcccc");
    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert_eq!(file.placement_of(1), Some((4, 4)));
    assert_eq!(file.placement_of(2), Some((8, 4)));
    for number in [1, 2] {
        assert!(!pipeline.segment_terminal_states.contains_key(&SegmentId {
            file_id,
            segment_number: number,
        }));
    }
}

// A damaged part that named no range of its own still measured its own bytes,
// as long as nothing suggests the body was cut short: its extent is the
// boundary the part behind it starts at.
#[tokio::test(start_paused = true)]
async fn a_believable_length_anchors_even_without_a_declared_range() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _path) = setup(&temp, 40239, &[8, 8]).await;
    let head = SegmentId {
        file_id,
        segment_number: 0,
    };
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    pipeline.decode_retries.insert(head, MAX_SEGMENT_RETRIES);
    deliver_unanchored_status(
        &mut pipeline,
        file_id,
        0,
        b"bad!",
        weaver_yenc::CrcVerification::Mismatch,
        false,
    )
    .await;

    assert!(pipeline.unanchored_parked.is_empty());
    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert_eq!(file.placement_of(1), Some((4, 4)));
    assert!(!pipeline.segment_terminal_states.contains_key(&SegmentId {
        file_id,
        segment_number: 1,
    }));
}

// A body that looked cut short, from a part that named no range either, knows
// neither where it ends nor how much is missing. There is nothing to anchor
// on, so the run held behind it is retired for repair rather than laid at a
// guessed offset.
#[tokio::test(start_paused = true)]
async fn a_body_cut_short_without_a_range_anchors_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _path) = setup(&temp, 40240, &[8, 8]).await;
    let head = SegmentId {
        file_id,
        segment_number: 0,
    };
    let tail = SegmentId {
        file_id,
        segment_number: 1,
    };
    let buffered = pipeline.write_buffered_bytes;
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    pipeline.decode_retries.insert(head, MAX_SEGMENT_RETRIES);
    deliver_unanchored_status(
        &mut pipeline,
        file_id,
        0,
        b"cut",
        weaver_yenc::CrcVerification::Unverified,
        true,
    )
    .await;

    assert!(pipeline.unanchored_parked.is_empty());
    assert_eq!(pipeline.write_buffered_bytes, buffered);
    assert_eq!(
        pipeline.segment_terminal_states.get(&tail),
        Some(&SegmentTerminalState::DecodeExhausted)
    );
    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert_eq!(file.placement_of(1), None);
    assert!(file.segment_damage_is_truncation_only(0));
}

// A four-part file of four-byte parts whose NZB declares eight encoded bytes
// a part. The bad article is ordinal 0 carrying a header that places it at
// 12, where part 3 belongs: its own declared file size admits that, but the
// NZB cannot, since nothing comes before ordinal 0.
async fn deliver_part(
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

async fn assert_only_the_bad_part_is_fetched_again(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    path: &std::path::Path,
) {
    let bad = SegmentId {
        file_id,
        segment_number: 0,
    };
    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert_eq!(
        file.placement_of(0),
        None,
        "a contradicted placement is not kept"
    );
    for number in 1..4u32 {
        assert_eq!(file.placement_of(number), Some((u64::from(number) * 4, 4)));
    }
    assert_eq!(
        pipeline.decode_retries.keys().copied().collect::<Vec<_>>(),
        vec![bad],
        "only the article with the bad header fails"
    );
    assert!(pipeline.segment_terminal_states.is_empty());
    let retry = pipeline.retry_rx.recv().await.unwrap();
    assert_eq!(retry.work.segment_id, bad);
    deliver_part(pipeline, file_id, 0, 0, b"aaaa").await;
    assert!(in_hand(pipeline, file_id));
    assert_eq!(std::fs::read(path).unwrap(), b"aaaabbbbccccdddd");
    assert_eq!(pipeline.write_buffered_bytes, 0);
}

// Its neighbour is already placed, so the bad article is refused on arrival
// and recorded nowhere; every later part still places.
#[tokio::test(start_paused = true)]
async fn a_bad_extent_after_its_neighbour_is_refused_and_not_recorded() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40230, &[8; 4]).await;
    deliver_part(&mut pipeline, file_id, 1, 4, b"bbbb").await;
    deliver_part(&mut pipeline, file_id, 0, 12, b"XXXX").await;
    for (number, offset, bytes) in [(2u32, 8u64, b"cccc"), (3, 12, b"dddd")] {
        deliver_part(&mut pipeline, file_id, number, offset, bytes).await;
    }
    assert_only_the_bad_part_is_fetched_again(&mut pipeline, file_id, &path).await;
}

// After a restart nothing of the file is placed yet, so the bad article can be
// the first to arrive and be accepted. Its neighbours, which the NZB vouches
// for, then contradict it: it is withdrawn and fetched again, and none of them
// fails.
#[tokio::test(start_paused = true)]
async fn a_bad_extent_that_arrives_first_fails_alone_and_its_neighbours_place() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40231, &[8; 4]).await;
    deliver_part(&mut pipeline, file_id, 0, 12, b"XXXX").await;
    assert_eq!(
        pipeline.jobs[&file_id.job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .placement_of(0),
        Some((12, 4))
    );
    for (number, offset, bytes) in [(1u32, 4u64, b"bbbb"), (2, 8, b"cccc"), (3, 12, b"dddd")] {
        deliver_part(&mut pipeline, file_id, number, offset, bytes).await;
    }
    assert_only_the_bad_part_is_fetched_again(&mut pipeline, file_id, &path).await;
}
