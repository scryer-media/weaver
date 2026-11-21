// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use weaver_server_composition::{JobId, JobStatus, NzbFileId, RestoreJobRequest, SegmentId};
use weaver_server_pipeline::Pipeline;
use weaver_server_pipeline::pipeline::tests::{
    activate_job,
    engines::harness::collect,
    history_fixture::{check_completion, database, drain_pipeline, pause_runtime},
    insert_active_job, new_direct_pipeline, pump_pipeline_runtime_queues, retire_pipeline_database,
    segmented_job_spec,
    uu_fixture::{job_status, set_available},
};
use weaver_server_pipeline::pipeline::tests::{
    queue_fixture::{clear_download_queue, download_queue_len, fail_job, metrics},
    settle_failed_releases,
    uu_fixture::{backlog, set_park_limit},
    yenc_fixture::*,
};

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
    assert!(!truncation_suspected(&decoded(&article)));
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
    assert!(truncation_suspected(&result));
}

// A single-part article that neither ends with a trailer nor accounts for its
// declared size is suspect for the same reason.
#[test]
fn a_single_part_body_short_of_its_declared_size_is_suspected() {
    let body = vec![0u8; 8];
    let article = plain_article(&["=ybegin line=128 size=64 name=sample.bin"], &body, None);
    assert!(truncation_suspected(&decoded(&article)));
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
    assert!(truncation_suspected(&result));
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
    assert!(!truncation_suspected(&result));
}

// Held articles have no offset once the part they follow is given up, so they
// are retired with it rather than held for a placement that cannot come.
#[tokio::test]
async fn giving_up_a_predecessor_retires_the_articles_held_behind_it() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _path) = setup(&temp, 40231, &[12, 12, 12]).await;
    let buffered = backlog(&pipeline).write_buffered_bytes;
    deliver_unanchored(&mut pipeline, file_id, 1, b"bbbb").await;
    deliver_unanchored(&mut pipeline, file_id, 2, b"cccc").await;
    assert_eq!(held_parts(&pipeline), 2);
    let head = SegmentId {
        file_id,
        segment_number: 0,
    };
    mark_missing(&mut pipeline, head);
    assert!(held_empty(&pipeline));
    assert_eq!(held_parts(&pipeline), 0);
    assert_eq!(backlog(&pipeline).write_buffered_bytes, buffered);
    for number in [1, 2] {
        assert!(is_decode_exhausted(
            &pipeline,
            SegmentId {
                file_id,
                segment_number: number,
            }
        ));
    }
    assert!(download_queue_len(&pipeline, file_id.job_id) == 0);
}

// An article that cannot be held is asked for again without retry budget, and
// the bound on that counts only attempts since the file last placed a part.
#[tokio::test]
async fn an_article_that_cannot_be_held_is_requeued_until_the_file_stalls() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _path) = setup(&temp, 40232, &[12, 12, 12]).await;
    set_park_limit(&mut pipeline, 0);
    let segment = SegmentId {
        file_id,
        segment_number: 2,
    };
    deliver_unanchored(&mut pipeline, file_id, 2, b"cccc").await;
    assert!(held_empty(&pipeline));
    assert_eq!(requeue(&pipeline, segment), Some(&(1, 0)));
    assert!(!retries(&pipeline).contains_key(&segment));
    assert_eq!(download_queue_len(&pipeline, file_id.job_id), 1);

    deliver(&mut pipeline, file_id, 0, 0, b"aaaa", true).await;
    deliver_unanchored(&mut pipeline, file_id, 2, b"cccc").await;
    // The file moved on, so the count starts over rather than accumulating.
    assert_eq!(requeue(&pipeline, segment), Some(&(1, 1)));
    assert!(!is_terminal(&pipeline, segment));
}

// The park holds only so many articles across every file. One past that is
// asked for again without retry budget, exactly as one past its file's share
// is, while a copy of a part already held still takes its place and an
// article that states its own start is placed as always. Once the run's head
// lands, the park drains and the refused article is placed behind it.
#[tokio::test]
async fn an_article_past_the_parks_total_is_requeued_without_retry_budget() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40243, &[16, 16, 16, 16]).await;
    set_total_park_limit(&mut pipeline, 1);
    let refused = SegmentId {
        file_id,
        segment_number: 3,
    };
    let queued = |pipeline: &Pipeline| download_queue_len(pipeline, file_id.job_id);
    let buffered = backlog(&pipeline).write_buffered_bytes;
    let buffered_segments = backlog(&pipeline).write_buffered_segments;

    deliver_unanchored(&mut pipeline, file_id, 1, b"bbbb").await;
    assert_eq!(held_parts(&pipeline), 1);
    assert!(held_for_file(&pipeline, file_id) < file_park_limit(&pipeline));

    deliver_unanchored(&mut pipeline, file_id, 3, b"dddd").await;
    assert_eq!(held_parts(&pipeline), 1);
    assert!(!has_held(&pipeline, file_id, 3));
    assert_eq!(requeue(&pipeline, refused), Some(&(1, 0)));
    assert!(!retries(&pipeline).contains_key(&refused));
    assert!(!is_terminal(&pipeline, refused));
    assert_eq!(metrics(&pipeline).decode_errors.load(Ordering::Relaxed), 0);
    assert_eq!(queued(&pipeline), 1);
    clear_download_queue(&mut pipeline, file_id.job_id);

    // A second copy of the held part replaces it, its charge with it.
    deliver_unanchored(&mut pipeline, file_id, 1, b"bbbb").await;
    assert_eq!(held_parts(&pipeline), 1);
    assert_eq!(backlog(&pipeline).write_buffered_bytes, buffered + 4);
    assert_eq!(
        backlog(&pipeline).write_buffered_segments,
        buffered_segments + 1
    );
    assert_eq!(queued(&pipeline), 0);

    // An article that states its start never meets the park.
    deliver(&mut pipeline, file_id, 2, 8, b"cccc", true).await;
    assert!(article_in_hand(&pipeline, file_id, 2));
    assert_eq!(held_parts(&pipeline), 1);

    deliver(&mut pipeline, file_id, 0, 0, b"aaaa", true).await;
    assert!(held_empty(&pipeline));
    assert_eq!(held_parts(&pipeline), 0);
    assert_eq!(backlog(&pipeline).write_buffered_bytes, buffered);

    deliver_unanchored(&mut pipeline, file_id, 3, b"dddd").await;
    assert_eq!(std::fs::read(path).unwrap(), b"aaaabbbbccccdddd");
    assert!(in_hand(&pipeline, file_id));
    assert!(!retries(&pipeline).contains_key(&refused));
    assert_eq!(metrics(&pipeline).decode_errors.load(Ordering::Relaxed), 0);
    assert_eq!(queued(&pipeline), 0);
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
    mark_missing(&mut pipeline, head);
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    assert!(held_empty(&pipeline));
    assert!(is_decode_exhausted(
        &pipeline,
        SegmentId {
            file_id,
            segment_number: 1,
        }
    ));
    assert!(download_queue_len(&pipeline, file_id.job_id) == 0);
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
    let file = assembled_file(&pipeline, file_id);
    assert!(in_hand(&pipeline, file_id));
    for number in 0..20u32 {
        assert_eq!(file.placement_of(number), Some((u64::from(number) * 4, 4)));
    }
    assert!(terminals_empty(&pipeline));
    assert_eq!(
        metrics(&pipeline)
            .segments_committed
            .load(Ordering::Relaxed),
        20
    );
    assert_eq!(failed_bytes(&pipeline, file_id.job_id), 0);
}

#[tokio::test(start_paused = true)]
async fn shorter_replacement_next_segment_overwrites_the_old_tail() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40205, &[16, 16]).await;
    // One server, so the damaged copy is fetched again.
    set_retention(&mut pipeline, &[0]);
    deliver(&mut pipeline, file_id, 0, 0, b"bad tail", false).await;
    let _retry = next_retry(&mut pipeline).await;
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    deliver(&mut pipeline, file_id, 1, 4, b"next", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"goodnext");
    assert!(in_hand(&pipeline, file_id));
    assert_eq!(
        metrics(&pipeline).bytes_committed.load(Ordering::Relaxed),
        8
    );
}

#[tokio::test]
async fn a_late_damaged_duplicate_cannot_overwrite_committed_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40206, &[8, 8]).await;
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    deliver(&mut pipeline, file_id, 0, 0, b"bad!", false).await;
    assert_eq!(&std::fs::read(path).unwrap()[..4], b"good");
    assert!(retries(&pipeline).is_empty());
    assert_eq!(
        metrics(&pipeline)
            .segments_committed
            .load(Ordering::Relaxed),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn cancellation_does_not_allow_a_late_retry_to_write() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40210, &[8, 8]).await;
    // One server, so the damaged copy is fetched again.
    set_retention(&mut pipeline, &[0]);
    deliver(&mut pipeline, file_id, 0, 0, b"bad!", false).await;
    fail_job(
        &mut pipeline,
        file_id.job_id,
        "cancelled fixture".to_string(),
    );
    settle_failed_releases(&mut pipeline).await;
    // Whatever the failed job left on disk is all there ever is.
    let left = std::fs::read(&path).ok();
    let retry = next_retry(&mut pipeline).await;
    assert_eq!(retry.segment_id.file_id, file_id);
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    assert_eq!(std::fs::read(&path).ok(), left);
    assert_eq!(
        metrics(&pipeline)
            .segments_committed
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(backlog(&pipeline).write_buffered_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn decode_retries_share_the_files_group_list() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _) = setup(&temp, 40211, &[16, 16, 16, 16]).await;
    // One server, so each damaged copy is fetched again.
    set_retention(&mut pipeline, &[0]);
    let shared = groups(&pipeline, file_id);
    for number in 0..4u32 {
        deliver(
            &mut pipeline,
            file_id,
            number,
            u64::from(number) * 4,
            b"bad!",
            false,
        )
        .await;
    }
    for _ in 0..4 {
        let retry = next_retry(&mut pipeline).await;
        assert_eq!(retry.segment_id.file_id, file_id);
        assert!(std::sync::Arc::ptr_eq(&retry.groups, &shared));
    }
}

// A part with no usable start is laid immediately after the part before it.
#[tokio::test]
async fn an_article_without_a_start_follows_the_one_before_it() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40227, &[8, 8]).await;
    deliver(&mut pipeline, file_id, 0, 0, b"head", true).await;
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    assert_eq!(std::fs::read(path).unwrap(), b"headtail");
    let file = assembled_file(&pipeline, file_id);
    assert!(in_hand(&pipeline, file_id));
    assert_eq!(file.placement_of(1), Some((4, 4)));
    assert!(requeues_empty(&pipeline));
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
    let buffered = backlog(&pipeline).write_buffered_bytes;
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    assert!(has_held(&pipeline, file_id, 1));
    assert_eq!(held_parts(&pipeline), 1);
    assert_eq!(backlog(&pipeline).write_buffered_bytes, buffered + 4);
    assert!(requeues_empty(&pipeline));
    assert!(!retries(&pipeline).contains_key(&segment));
    assert!(!is_terminal(&pipeline, segment));
    assert_eq!(metrics(&pipeline).decode_errors.load(Ordering::Relaxed), 0);
    assert!(download_queue_len(&pipeline, file_id.job_id) == 0);
    assert!(!article_in_hand(&pipeline, file_id, 1));
    deliver(&mut pipeline, file_id, 0, 0, b"head", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"headtail");
    assert!(held_empty(&pipeline));
    assert_eq!(held_parts(&pipeline), 0);
    assert_eq!(backlog(&pipeline).write_buffered_bytes, buffered);
    let file = assembled_file(&pipeline, file_id);
    assert!(in_hand(&pipeline, file_id));
    assert_eq!(file.placement_of(1), Some((4, 4)));
    assert!(download_queue_len(&pipeline, file_id.job_id) == 0);
}

// A run of held articles releases in one pass once its head is placed, each
// laid after the one before it.
#[tokio::test]
async fn a_run_of_held_articles_releases_behind_its_head() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40230, &[12, 12, 12]).await;
    deliver_unanchored(&mut pipeline, file_id, 2, b"cccc").await;
    deliver_unanchored(&mut pipeline, file_id, 1, b"bbbb").await;
    assert_eq!(held_for_file(&pipeline, file_id), 2);
    assert_eq!(held_parts(&pipeline), 2);
    deliver(&mut pipeline, file_id, 0, 0, b"aaaa", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"aaaabbbbcccc");
    assert!(held_empty(&pipeline));
    assert_eq!(held_parts(&pipeline), 0);
    let file = assembled_file(&pipeline, file_id);
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
    set_retention(&mut pipeline, &[0]);
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    deliver(&mut pipeline, file_id, 0, 0, b"hexd", false).await;
    assert!(has_held(&pipeline, file_id, 1));
    deliver(&mut pipeline, file_id, 0, 0, b"head", true).await;
    assert!(held_empty(&pipeline));
    assert_eq!(std::fs::read(path).unwrap(), b"headtail");
    assert!(in_hand(&pipeline, file_id));
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
    let buffered = backlog(&pipeline).write_buffered_bytes;
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    assert!(has_held(&pipeline, file_id, 1));

    exhaust_retries(&mut pipeline, head);
    // Three bytes of a five-byte range: the header still knows the range.
    deliver_damaged_declaring(&mut pipeline, file_id, 0, 0, 5, 9, b"bad").await;
    release_settled(&mut pipeline).await;

    // The damaged copy stands in the job's root for repair.
    assert!(is_settled(&pipeline, head));
    assert!(!is_terminal(&pipeline, tail));
    assert!(held_empty(&pipeline));
    assert_eq!(backlog(&pipeline).write_buffered_bytes, buffered);
    assert_eq!(std::fs::read(&path).unwrap(), b"bad\0\0tail");

    let file = assembled_file(&pipeline, file_id);
    assert_eq!(file.placement_of(1), Some((5, 4)));
    assert!(article_in_hand(&pipeline, file_id, 1));
    assert!(retained(file, 0));
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
    assert_eq!(held_for_file(&pipeline, file_id), 2);

    exhaust_retries(&mut pipeline, head);
    deliver_damaged_declaring(&mut pipeline, file_id, 0, 0, 4, 12, b"aa").await;
    release_settled(&mut pipeline).await;

    assert!(held_empty(&pipeline));
    assert_eq!(held_parts(&pipeline), 0);
    assert_eq!(std::fs::read(&path).unwrap(), b"aa\0\0bbbbcccc");
    let file = assembled_file(&pipeline, file_id);
    assert_eq!(file.placement_of(1), Some((4, 4)));
    assert_eq!(file.placement_of(2), Some((8, 4)));
    for number in [1, 2] {
        assert!(!is_terminal(
            &pipeline,
            SegmentId {
                file_id,
                segment_number: number,
            }
        ));
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
    exhaust_retries(&mut pipeline, head);
    deliver_unanchored_status(
        &mut pipeline,
        file_id,
        0,
        b"bad!",
        weaver_yenc::CrcVerification::Mismatch,
        false,
    )
    .await;

    assert!(held_empty(&pipeline));
    let file = assembled_file(&pipeline, file_id);
    assert_eq!(file.placement_of(1), Some((4, 4)));
    assert!(!is_terminal(
        &pipeline,
        SegmentId {
            file_id,
            segment_number: 1,
        }
    ));
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
    let buffered = backlog(&pipeline).write_buffered_bytes;
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    exhaust_retries(&mut pipeline, head);
    deliver_unanchored_status(
        &mut pipeline,
        file_id,
        0,
        b"cut",
        weaver_yenc::CrcVerification::Unverified,
        true,
    )
    .await;

    assert!(held_empty(&pipeline));
    assert_eq!(backlog(&pipeline).write_buffered_bytes, buffered);
    assert!(is_decode_exhausted(&pipeline, tail));
    let file = assembled_file(&pipeline, file_id);
    assert_eq!(file.placement_of(1), None);
    assert!(truncation_only(file, 0));
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
    let file = assembled_file(pipeline, file_id);
    assert_eq!(
        file.placement_of(0),
        None,
        "a contradicted placement is not kept"
    );
    for number in 1..4u32 {
        assert_eq!(file.placement_of(number), Some((u64::from(number) * 4, 4)));
    }
    assert_eq!(
        retries(pipeline).keys().copied().collect::<Vec<_>>(),
        vec![bad],
        "only the article with the bad header fails"
    );
    assert!(terminals_empty(pipeline));
    let retry = next_retry(pipeline).await;
    assert_eq!(retry.segment_id, bad);
    deliver_part(pipeline, file_id, 0, 0, b"aaaa").await;
    assert!(in_hand(pipeline, file_id));
    assert_eq!(std::fs::read(path).unwrap(), b"aaaabbbbccccdddd");
    assert_eq!(backlog(pipeline).write_buffered_bytes, 0);
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
        assembled_file(&pipeline, file_id).placement_of(0),
        Some((12, 4))
    );
    for (number, offset, bytes) in [(1u32, 4u64, b"bbbb"), (2, 8, b"cccc"), (3, 12, b"dddd")] {
        deliver_part(&mut pipeline, file_id, number, offset, bytes).await;
    }
    assert_only_the_bad_part_is_fetched_again(&mut pipeline, file_id, &path).await;
}

// The bad article arrives first at 10, where the end of part 2 and the start
// of part 3 belong, and its bytes are in the root. Part 2 contradicts it and
// withdraws it: the range only it covered, 12 to 14, is covered no more, so
// the file holds there no bytes that were never part 3's.
#[tokio::test(start_paused = true)]
async fn a_withdrawn_article_no_longer_covers_what_it_wrote() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40232, &[8; 4]).await;
    let covered = |pipeline: &Pipeline, start, end| range_covered(pipeline, file_id, start, end);
    deliver_part(&mut pipeline, file_id, 0, 10, b"XXXX").await;
    assert!(covered(&pipeline, 10, 14), "its bytes are in the root");
    deliver_part(&mut pipeline, file_id, 2, 8, b"cccc").await;
    assert!(covered(&pipeline, 8, 12));
    assert!(
        !covered(&pipeline, 12, 13),
        "what only it wrote is uncovered"
    );
    assert!(!covered(&pipeline, 13, 14));
    for (number, offset, bytes) in [(1u32, 4u64, b"bbbb"), (3, 12, b"dddd")] {
        deliver_part(&mut pipeline, file_id, number, offset, bytes).await;
    }
    assert_only_the_bad_part_is_fetched_again(&mut pipeline, file_id, &path).await;
}

#[tokio::test(start_paused = true)]
async fn a_crossed_neighbour_is_refused_on_the_live_path() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _) = setup(&temp, 40233, &[8, 8]).await;
    deliver_part(&mut pipeline, file_id, 1, 0, b"bbbb").await;
    assert_a_crossed_neighbour_is_refused(&mut pipeline, file_id).await;
}

#[tokio::test(start_paused = true)]
async fn a_crossed_neighbour_is_refused_after_a_restart() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _) = setup(&temp, 40234, &[8, 8]).await;
    let job_id = file_id.job_id;
    deliver_part(&mut pipeline, file_id, 1, 0, b"bbbb").await;
    let working_dir =
        weaver_server_pipeline::pipeline::tests::queue_fixture::working_dir(&pipeline, job_id)
            .clone();
    drain_pipeline(&mut pipeline).await;

    // The process stops with ordinal 1 written and in the checkpoint.
    retire_pipeline_database(pipeline).await;
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec: segmented_job_spec("yenc compatibility", "sample.bin", &[8, 8]),
            detected_archives: Default::default(),
            file_identities: Default::default(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
            restart: None,
        })
        .await
        .unwrap();
    assert_a_crossed_neighbour_is_refused(&mut pipeline, file_id).await;
}

// An article held for the one before it is kept across a pause, as every
// article that arrived is: it spills to the job's spool as the job parks, is
// not asked for again when the job is resumed and built afresh, and is placed
// behind its predecessor when that arrives.
#[tokio::test]
async fn an_article_held_across_a_pause_is_placed_without_being_fetched_again() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp).await;
    let job_id = JobId(40240);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("yenc compatibility", "sample.bin", &[8, 8]),
    )
    .await;
    set_available(&mut pipeline, Some(u64::MAX));
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let held = SegmentId {
        file_id,
        segment_number: 1,
    };
    let head = SegmentId {
        file_id,
        segment_number: 0,
    };
    // The held article came back from its connection; its predecessor is
    // still queued.
    take_queued(&mut pipeline, held);
    let buffered = backlog(&pipeline).write_buffered_bytes;
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    assert!(has_held(&pipeline, file_id, 1));

    pause_runtime(&mut pipeline, job_id).unwrap();
    spill(&mut pipeline).await;
    assert!(!has_active_articles(&pipeline, job_id));
    assert!(spool_path(&pipeline, file_id, 1).is_some());
    assert_eq!(backlog(&pipeline).write_buffered_bytes, buffered);
    assert_eq!(backlog(&pipeline).uu_spooled_bytes, 4);
    // Spilling moves the part; it is still one part held.
    assert_eq!(held_parts(&pipeline), 1);

    resume_runtime(&mut pipeline, job_id);
    activate_job(&mut pipeline, job_id).await;
    let asked = queued_ids(&pipeline, job_id);
    assert_eq!(asked, std::collections::HashSet::from([head]));

    clear_download_queue(&mut pipeline, job_id);
    deliver(&mut pipeline, file_id, 0, 0, b"head", true).await;
    assert!(held_empty(&pipeline));
    assert_eq!(held_parts(&pipeline), 0);
    assert_eq!(backlog(&pipeline).uu_spooled_bytes, 0);
    assert_eq!(backlog(&pipeline).write_buffered_bytes, buffered);
    check_completion(&mut pipeline, job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;
    assert_eq!(job_status(&pipeline, job_id), Some(JobStatus::Complete));
    database(&pipeline).flush_write_queue().await.unwrap();
    let mut files = std::collections::BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    assert_eq!(
        files.into_values().collect::<Vec<_>>(),
        vec![b"headtail".to_vec()]
    );
}

// An article held for the one before it survives a restart: it spills to
// the job's spool as the job parks, the park's checkpoint names it, the
// restarted pipeline takes it back from there, and it is placed behind its
// predecessor without being asked for again.
#[tokio::test]
async fn an_article_held_across_a_restart_is_placed_without_being_fetched_again() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(40241);
    let spec = || segmented_job_spec("yenc compatibility", "sample.bin", &[8, 8]);
    insert_active_job(&mut pipeline, job_id, spec()).await;
    set_available(&mut pipeline, Some(u64::MAX));
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let held = SegmentId {
        file_id,
        segment_number: 1,
    };
    let head = SegmentId {
        file_id,
        segment_number: 0,
    };
    // The held article came back from its connection; its predecessor is
    // still queued.
    take_queued(&mut pipeline, held);
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    pause_runtime(&mut pipeline, job_id).unwrap();
    spill(&mut pipeline).await;
    let spool = spool_path(&pipeline, file_id, 1)
        .expect("the held article spills as its job parks")
        .to_path_buf();
    let working_dir =
        weaver_server_pipeline::pipeline::tests::queue_fixture::working_dir(&pipeline, job_id)
            .clone();

    // The process goes down; a new one stands up over the same database and
    // directories and restores the job from its checkpoint.
    retire_pipeline_database(pipeline).await;
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp).await;
    set_available(&mut pipeline, Some(u64::MAX));
    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec: spec(),
            detected_archives: Default::default(),
            file_identities: Default::default(),
            status: JobStatus::Paused,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: Some(JobStatus::Downloading),
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
            restart: None,
        })
        .await
        .unwrap();
    assert_eq!(std::fs::read(&spool).unwrap(), b"tail");

    resume_runtime(&mut pipeline, job_id);
    activate_job(&mut pipeline, job_id).await;
    let asked = queued_ids(&pipeline, job_id);
    assert_eq!(asked, std::collections::HashSet::from([head]));
    assert_eq!(backlog(&pipeline).uu_spooled_bytes, 4);
    // The restarted pipeline counts the part it took back.
    assert_eq!(held_parts(&pipeline), 1);

    clear_download_queue(&mut pipeline, job_id);
    let buffered = backlog(&pipeline).write_buffered_bytes;
    deliver(&mut pipeline, file_id, 0, 0, b"head", true).await;
    assert!(held_empty(&pipeline));
    assert_eq!(held_parts(&pipeline), 0);
    assert_eq!(backlog(&pipeline).uu_spooled_bytes, 0);
    assert_eq!(backlog(&pipeline).write_buffered_bytes, buffered);
    assert!(!spool.exists());
    check_completion(&mut pipeline, job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;
    assert_eq!(job_status(&pipeline, job_id), Some(JobStatus::Complete));
    database(&pipeline).flush_write_queue().await.unwrap();
    let mut files = std::collections::BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    assert_eq!(
        files.into_values().collect::<Vec<_>>(),
        vec![b"headtail".to_vec()]
    );
}

// A held article restored into a running job is named by the checkpoint
// that job takes as it starts, so a second restart before its predecessor
// arrives still finds its row and its spool file, and asks only for the
// predecessor.
#[tokio::test]
async fn an_article_held_across_two_restarts_keeps_its_checkpoint_row() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(40242);
    let spec = || segmented_job_spec("yenc compatibility", "sample.bin", &[8, 8]);
    let request = |status: JobStatus, working_dir: PathBuf| RestoreJobRequest {
        job_id,
        job_hash: [0; 32],
        spec: spec(),
        detected_archives: Default::default(),
        file_identities: Default::default(),
        status,
        download_state: None,
        post_state: None,
        run_state: None,
        queued_repair_at_epoch_ms: None,
        queued_extract_at_epoch_ms: None,
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        working_dir,
        restart: None,
    };
    insert_active_job(&mut pipeline, job_id, spec()).await;
    set_available(&mut pipeline, Some(u64::MAX));
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let held = SegmentId {
        file_id,
        segment_number: 1,
    };
    let head = SegmentId {
        file_id,
        segment_number: 0,
    };
    take_queued(&mut pipeline, held);
    deliver_unanchored(&mut pipeline, file_id, 1, b"tail").await;
    pause_runtime(&mut pipeline, job_id).unwrap();
    spill(&mut pipeline).await;
    let spool = spool_path(&pipeline, file_id, 1)
        .expect("the held article spills as its job parks")
        .to_path_buf();
    let working_dir =
        weaver_server_pipeline::pipeline::tests::queue_fixture::working_dir(&pipeline, job_id)
            .clone();

    // The first restart restores the job as running, and the job takes its
    // first checkpoint as its graph starts.
    retire_pipeline_database(pipeline).await;
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    set_available(&mut pipeline, Some(u64::MAX));
    pipeline
        .restore_job(request(JobStatus::Downloading, working_dir.clone()))
        .await
        .unwrap();

    // The second restart comes before the predecessor arrives.
    retire_pipeline_database(pipeline).await;
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    set_available(&mut pipeline, Some(u64::MAX));
    assert_eq!(checkpoint_held(&pipeline, job_id), vec![(0, 1, 4)]);
    pipeline
        .restore_job(request(JobStatus::Downloading, working_dir))
        .await
        .unwrap();
    let asked = queued_ids(&pipeline, job_id);
    assert_eq!(asked, std::collections::HashSet::from([head]));
    assert_eq!(std::fs::read(&spool).unwrap(), b"tail");
}
