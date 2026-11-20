// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A turn busy with download results skips the wait, and a finished move
// was heard only in the wait: under a steady stream of results a job whose
// move was done stayed moving. Here results keep arriving, one for every
// article handed to the lane, and the finished move is heard while they do.

use tokio::sync::broadcast::error::TryRecvError;

use weaver_server_composition::events::model::PipelineEvent;
use weaver_server_composition::{JobId, NzbFileId, SegmentId};
use weaver_server_pipeline::pipeline::tests::dispatch_fixture::busy_turns::{
    BusyTurns, ResultStream,
};

fn held_segment(job_id: JobId, file_index: u32, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId { job_id, file_index },
        segment_number,
    }
}

#[tokio::test]
async fn a_finished_move_is_heard_while_download_results_keep_arriving() {
    // Articles handed to the lane, each released by one result: the most
    // the stream can carry before it stops.
    const ARTICLES: usize = 2_048;
    // Results already sent when the loop starts: its first turn is busy.
    const AHEAD: usize = 200;

    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BusyTurns::new(&temp_dir).await;
    let moved = JobId(40_310);
    pipeline.add_moving_job(moved, "moved");

    let fetching = JobId(40_311);
    let segments: Vec<SegmentId> = (0..ARTICLES as u32)
        .map(|number| held_segment(fetching, 0, number))
        .collect();
    pipeline.hold_segments(&segments);

    let results = pipeline.result_stream();
    let mut sent = 0;
    while sent < AHEAD {
        results.fail(segments[sent]).await;
        sent += 1;
    }
    pipeline.finish_move(moved, "moved").await;

    let handle = pipeline.handle();
    let mut events = handle.subscribe_events();
    let runner = pipeline.spawn();

    // A result follows each one the loop takes, until the move is heard.
    loop {
        match events.try_recv() {
            Ok(PipelineEvent::MoveToCompleteFinished { job_id }) if job_id == moved => break,
            Ok(_) | Err(TryRecvError::Lagged(_)) => continue,
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Closed) => panic!("the pipeline stopped"),
        }
        assert!(
            sent < ARTICLES,
            "the finished move was not heard while results kept arriving"
        );
        results.fail(segments[sent]).await;
        sent += 1;
    }
    assert!(sent < ARTICLES, "{sent}");

    // The rest of the articles come back, so none is in flight at shutdown.
    while sent < ARTICLES {
        results.fail(segments[sent]).await;
        sent += 1;
    }
    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}

// The articles held on the lane and the results already sent, so the
// loop's first turn is busy, for the tests below.
async fn streaming(
    pipeline: &mut BusyTurns,
    articles: usize,
    ahead: usize,
) -> (Vec<SegmentId>, ResultStream, usize) {
    let fetching = JobId(40_311);
    let segments: Vec<SegmentId> = (0..articles as u32)
        .map(|number| held_segment(fetching, 0, number))
        .collect();
    pipeline.hold_segments(&segments);
    let results = pipeline.result_stream();
    for segment in &segments[..ahead] {
        results.fail(*segment).await;
    }
    (segments, results, ahead)
}

// A finished decode is heard while results keep arriving: its bytes leave
// the decode count.
#[tokio::test]
async fn a_finished_decode_is_heard_while_download_results_keep_arriving() {
    const ARTICLES: usize = 2_048;
    const RAW: u64 = 700 * 1024;

    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BusyTurns::new(&temp_dir).await;
    let (segments, results, mut sent) = streaming(&mut pipeline, ARTICLES, 200).await;
    let metrics = pipeline.metrics();
    metrics
        .decode_active_bytes
        .store(RAW, std::sync::atomic::Ordering::Relaxed);
    pipeline
        .fail_decode(held_segment(JobId(40_320), 0, 0), RAW)
        .await;

    let handle = pipeline.handle();
    let runner = pipeline.spawn();

    while metrics
        .decode_active_bytes
        .load(std::sync::atomic::Ordering::Relaxed)
        != 0
    {
        assert!(
            sent < ARTICLES,
            "the finished decode was not heard while results kept arriving"
        );
        results.fail(segments[sent]).await;
        sent += 1;
    }

    while sent < ARTICLES {
        results.fail(segments[sent]).await;
        sent += 1;
    }
    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}

// A database reply is heard while results keep arriving: the writer says
// an archive is kept, and the database's path is read for room.
#[tokio::test]
async fn a_database_reply_is_heard_while_download_results_keep_arriving() {
    const ARTICLES: usize = 2_048;

    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BusyTurns::new(&temp_dir).await;
    let (segments, results, mut sent) = streaming(&mut pipeline, ARTICLES, 200).await;
    let capacity = pipeline.storage_room();
    assert_eq!(capacity.waited_paths(), 0);
    pipeline.archive_kept(JobId(40_321));

    let handle = pipeline.handle();
    let runner = pipeline.spawn();

    while capacity.waited_paths() == 0 {
        assert!(
            sent < ARTICLES,
            "the database reply was not heard while results kept arriving"
        );
        results.fail(segments[sent]).await;
        sent += 1;
    }

    while sent < ARTICLES {
        results.fail(segments[sent]).await;
        sent += 1;
    }
    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}

// Results still go through while another arm is ready on every turn. The
// database replies outnumber the results many times over and are always
// waiting; every result is still taken, and a send waits only on the
// results channel's room, which only the loop taking results frees.
#[tokio::test]
async fn results_are_taken_while_another_arm_is_always_ready() {
    const ARTICLES: usize = 2_048;
    // Replies waiting for every result sent: more than the loop hears.
    const REPLIES_PER_RESULT: usize = 8;

    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BusyTurns::new(&temp_dir).await;
    let (segments, results, mut sent) = streaming(&mut pipeline, ARTICLES, 200).await;
    let replies = pipeline.archive_replies();
    // With no archive kept, this news does nothing but be heard.
    for _ in 0..sent * REPLIES_PER_RESULT {
        replies.more();
    }

    let handle = pipeline.handle();
    let runner = pipeline.spawn();

    while sent < ARTICLES {
        for _ in 0..REPLIES_PER_RESULT {
            replies.more();
        }
        results.fail(segments[sent]).await;
        sent += 1;
    }
    // The last results are taken too: the channel is empty, with replies
    // still waiting behind them.
    while !results.is_empty() {
        replies.more();
        tokio::task::yield_now().await;
    }
    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}
