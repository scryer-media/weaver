// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What abandoning a dead post's files costs, and what it leaves. Every
// queued article of a file that lost one is refused to the root, and each
// refusal tells the feed the post lost an article again; the feed weighs
// the post once for all of them, not once per refusal.

use std::collections::BTreeSet;

use super::harness::{ARTICLE, JOB, Post, Scenario, admit, par2_set, payload};
use crate::jobs::ids::{NzbFileId, SegmentId};
use crate::pipeline::SegmentTerminalState;
use crate::pipeline::post_health::{AbandonWork, abandon_work, keep_booked};
use crate::pipeline::tests::retire_pipeline_database;

fn segment(file_index: u32, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index,
        },
        segment_number,
    }
}

// One file of `articles` articles: the post an NZB of one file makes.
fn alone(articles: usize) -> Scenario {
    Scenario::new(vec![Post::plain(
        "lantern.mkv",
        payload(articles * ARTICLE, 7),
    )])
}

// The file beside another of as many articles, still queued and whole.
fn beside(articles: usize) -> Scenario {
    Scenario::new(vec![
        Post::plain("lantern.mkv", payload(articles * ARTICLE, 7)),
        Post::plain("beacon.mkv", payload(articles * ARTICLE, 8)),
    ])
}

// Sixty articles beside a PAR2 set of fifteen blocks, whose articles stay
// queued behind the abandoned file.
fn covered() -> Scenario {
    let bytes = payload(60 * ARTICLE, 7);
    let mut posts = vec![Post::plain("lantern.mkv", bytes.clone())];
    posts.extend(par2_set(
        "lantern",
        &[("lantern.mkv", bytes.as_slice())],
        15,
    ));
    Scenario::new(posts)
}

// What the abandon left: every terminal state booked, in order, the post's
// counted bytes, the verdict's files, what is still queued, the job's
// status, and every event sent, in order.
#[derive(Debug, PartialEq, Eq)]
struct Ended {
    booked: Vec<(u32, u32, SegmentTerminalState)>,
    failed_bytes: u64,
    derived_failed_bytes: u64,
    missing_bytes: u64,
    abandoned_files: Option<BTreeSet<u32>>,
    queued: Option<(usize, usize)>,
    status: String,
    events: Vec<String>,
}

// The post given up, then the first article of its first file lost: every
// queued article of that file is abandoned.
async fn abandon(scenario: &Scenario, downloading: bool) -> (AbandonWork, Ended) {
    let (_temp_dir, mut pipeline, _) = admit(scenario).await;
    if downloading {
        // A download pass is open for the job, so the pass that drains its
        // queue closes it.
        pipeline.active_download_passes.insert(JOB);
    }
    let mut sent = pipeline.event_tx.subscribe();
    pipeline
        .post_probes
        .dead
        .insert(JOB, "fixture: the post is given up".into());
    keep_booked();
    let before = abandon_work();
    pipeline.book_terminal_segment(segment(0, 0), SegmentTerminalState::Missing);
    let after = abandon_work();
    let work = AbandonWork {
        passes: after.passes - before.passes,
        scanned: after.scanned - before.scanned,
        lost_files: after.lost_files - before.lost_files,
        checkpoints: after.checkpoints - before.checkpoints,
    };
    let booked = keep_booked()
        .into_iter()
        .map(|(segment, state)| (segment.file_id.file_index, segment.segment_number, state))
        .collect();
    let mut events = Vec::new();
    loop {
        match sent.try_recv() {
            Ok(event) => events.push(format!("{event:?}")),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(error) => panic!("{error}"),
        }
    }
    let state = pipeline.jobs.get(&JOB);
    let ended = Ended {
        booked,
        failed_bytes: state.map_or(0, |state| state.failed_bytes),
        derived_failed_bytes: pipeline.derived_failed_bytes(JOB),
        missing_bytes: pipeline.missing_bytes(JOB),
        abandoned_files: pipeline
            .post_probes
            .verdicts
            .get(&JOB)
            .map(|verdict| verdict.abandoned_files.clone()),
        queued: state
            .and_then(|state| state.articles.active())
            .map(|active| (active.download_queue.len(), active.recovery_queue.len())),
        status: format!("{:?}", state.map(|state| &state.status)),
        events,
    };
    retire_pipeline_database(pipeline).await;
    (work, ended)
}

// The file's first article refused, then every other article of it
// abandoned, each once, in the order it was queued.
fn refused(articles: u32) -> Vec<(u32, u32, SegmentTerminalState)> {
    std::iter::once((0, 0, SegmentTerminalState::Missing))
        .chain((1..articles).map(|ordinal| (0, ordinal, SegmentTerminalState::Abandoned)))
        .collect()
}

// What each post below ended with before the feed weighed the post once
// per batch of refusals rather than once per refusal.
fn ended(articles: u32, queued: (usize, usize), events: Vec<String>) -> Ended {
    let bytes = u64::from(articles) * ARTICLE as u64;
    Ended {
        booked: refused(articles),
        failed_bytes: bytes,
        derived_failed_bytes: bytes,
        missing_bytes: ARTICLE as u64,
        abandoned_files: Some(BTreeSet::from([0])),
        queued: Some(queued),
        status: "Some(Downloading)".into(),
        events,
    }
}

#[tokio::test]
async fn abandoning_a_dead_posts_file_ends_as_it_did_when_each_refusal_weighed_the_post() {
    let (_, alone_ended) = abandon(&alone(40), false).await;
    assert_eq!(alone_ended, ended(40, (0, 0), Vec::new()));
    let (_, beside_ended) = abandon(&beside(40), false).await;
    assert_eq!(beside_ended, ended(40, (40, 0), Vec::new()));
    let (_, covered_ended) = abandon(&covered(), false).await;
    assert_eq!(covered_ended, ended(60, (152, 0), Vec::new()));
    let (_, draining) = abandon(&alone(40), true).await;
    assert_eq!(
        draining,
        ended(40, (0, 0), DRAINED.map(String::from).to_vec())
    );
}

const DRAINED: [&str; 1] =
    ["DownloadFinished { job_id: JobId(93001), finalization_pending: false }"];

// Abandoning a file walks the queues twice, once to take its articles and
// once after their refusals, whatever its size: the work grows with the
// queue, not with the queue times the file. Before, each refusal walked the
// queues again and took a checkpoint, so a file of 2,000 articles beside
// another read 4,002,000 queued items and one of 8,000 read 64,008,000.
#[tokio::test]
async fn abandoning_a_file_walks_the_queues_once_more_not_once_per_article() {
    for articles in [2_000, 20_000] {
        let (work, ended) = abandon(&alone(articles), false).await;
        assert_eq!(ended.booked.len(), articles);
        assert_eq!(
            work,
            AbandonWork {
                passes: 2,
                scanned: articles,
                lost_files: 2,
                checkpoints: 1,
            },
            "alone {articles}"
        );
        let (work, ended) = abandon(&beside(articles), false).await;
        assert_eq!(ended.booked.len(), articles);
        assert_eq!(
            work,
            AbandonWork {
                passes: 2,
                scanned: 3 * articles,
                lost_files: 2,
                checkpoints: 1,
            },
            "beside {articles}"
        );
    }
}
