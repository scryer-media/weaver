// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A turn busy with download results skips the wait, and a finished move
// was heard only in the wait: under a steady stream of results a job whose
// move was done stayed moving. Here results keep arriving, one for every
// article handed to the lane, and the finished move is heard while they do.

use tokio::sync::broadcast::error::TryRecvError;

use super::*;

fn failed_fetch(segment_id: SegmentId) -> DownloadResult {
    DownloadResult {
        lane_id: 0,
        job_id: segment_id.file_id.job_id,
        runtime_generation: 0,
        segment_id,
        data: Err(DownloadError::fetch(
            DownloadFailureKind::EstablishedTransport,
            "busy turn test",
        )),
        attempts: Vec::new(),
        lane_observation: None,
        source_server_idx: None,
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: weaver_nntp::ServerSet::EMPTY,
        release_connection_slot: false,
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
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let moved = JobId(40_310);
    pipeline.jobs.insert(
        moved,
        minimal_job_state(moved, "moved", intermediate_dir.join("moved")),
    );
    pipeline.job_order.push(moved);
    pipeline.report_all_for_test();

    let fetching = JobId(40_311);
    let segments: Vec<SegmentId> = (0..ARTICLES as u32)
        .map(|number| held_segment(fetching, 0, number))
        .collect();
    hold_on_test_lane(&mut pipeline, &segments);

    let results = pipeline.download_done_tx.clone();
    let mut sent = 0;
    while sent < AHEAD {
        results.send(failed_fetch(segments[sent])).await.unwrap();
        sent += 1;
    }
    pipeline
        .move_done_tx
        .send(MoveToCompleteDone {
            job_id: moved,
            dest: complete_dir.join("moved"),
            result: Ok(MoveToCompleteResult {
                moved_entries: 0,
                renamed_members: Vec::new(),
            }),
        })
        .await
        .unwrap();

    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let mut events = handle.subscribe_events();
    let runner = tokio::spawn(async move { pipeline.run().await });

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
        results.send(failed_fetch(segments[sent])).await.unwrap();
        sent += 1;
    }
    assert!(sent < ARTICLES, "{sent}");

    // The rest of the articles come back, so none is in flight at shutdown.
    while sent < ARTICLES {
        results.send(failed_fetch(segments[sent])).await.unwrap();
        sent += 1;
    }
    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}

// The articles held on the lane and the results already sent, so the
// loop's first turn is busy, for the tests below.
async fn streaming(
    pipeline: &mut Pipeline,
    articles: usize,
    ahead: usize,
) -> (Vec<SegmentId>, mpsc::Sender<DownloadResult>, usize) {
    let fetching = JobId(40_311);
    let segments: Vec<SegmentId> = (0..articles as u32)
        .map(|number| held_segment(fetching, 0, number))
        .collect();
    hold_on_test_lane(pipeline, &segments);
    let results = pipeline.download_done_tx.clone();
    for segment in &segments[..ahead] {
        results.send(failed_fetch(*segment)).await.unwrap();
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
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let (segments, results, mut sent) = streaming(&mut pipeline, ARTICLES, 200).await;
    let metrics = pipeline.metrics.clone();
    metrics
        .decode_active_bytes
        .store(RAW, std::sync::atomic::Ordering::Relaxed);
    pipeline
        .decode_done_tx
        .send(DecodeDone::Failed {
            segment_id: held_segment(JobId(40_320), 0, 0),
            raw_size: RAW,
            error: "busy turn test".to_owned(),
            servers: PoolServers::stamped(
                pipeline.pool_generation,
                None,
                weaver_nntp::ServerSet::EMPTY,
            ),
        })
        .await
        .unwrap();

    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let runner = tokio::spawn(async move { pipeline.run().await });

    while metrics
        .decode_active_bytes
        .load(std::sync::atomic::Ordering::Relaxed)
        != 0
    {
        assert!(
            sent < ARTICLES,
            "the finished decode was not heard while results kept arriving"
        );
        results.send(failed_fetch(segments[sent])).await.unwrap();
        sent += 1;
    }

    while sent < ARTICLES {
        results.send(failed_fetch(segments[sent])).await.unwrap();
        sent += 1;
    }
    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}

// A database reply is heard while results keep arriving: the writer says
// an archive is kept, and the database's path is read for room.
#[tokio::test]
async fn a_database_reply_is_heard_while_download_results_keep_arriving() {
    use crate::persistence::ArchiveNews;
    const ARTICLES: usize = 2_048;

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let (segments, results, mut sent) = streaming(&mut pipeline, ARTICLES, 200).await;
    let capacity = pipeline.storage_capacity.clone();
    assert_eq!(capacity.waited_paths(), 0);
    pipeline
        .db_reply_tx
        .send(db_reply::DbReply::Archive(ArchiveNews::Kept(JobId(40_321))))
        .unwrap();

    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let runner = tokio::spawn(async move { pipeline.run().await });

    while capacity.waited_paths() == 0 {
        assert!(
            sent < ARTICLES,
            "the database reply was not heard while results kept arriving"
        );
        results.send(failed_fetch(segments[sent])).await.unwrap();
        sent += 1;
    }

    while sent < ARTICLES {
        results.send(failed_fetch(segments[sent])).await.unwrap();
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
    use crate::persistence::ArchiveNews;
    const ARTICLES: usize = 2_048;
    // Replies waiting for every result sent: more than the loop hears.
    const REPLIES_PER_RESULT: usize = 8;

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let (segments, results, mut sent) = streaming(&mut pipeline, ARTICLES, 200).await;
    let replies = pipeline.db_reply_tx.clone();
    // With no archive kept, this news does nothing but be heard.
    let idle_news = || db_reply::DbReply::Archive(ArchiveNews::More { from: 0 });
    for _ in 0..sent * REPLIES_PER_RESULT {
        replies.send(idle_news()).unwrap();
    }

    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let runner = tokio::spawn(async move { pipeline.run().await });

    while sent < ARTICLES {
        for _ in 0..REPLIES_PER_RESULT {
            replies.send(idle_news()).unwrap();
        }
        results.send(failed_fetch(segments[sent])).await.unwrap();
        sent += 1;
    }
    // The last results are taken too: the channel is empty, with replies
    // still waiting behind them.
    while results.capacity() < results.max_capacity() {
        replies.send(idle_news()).unwrap();
        tokio::task::yield_now().await;
    }
    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}
