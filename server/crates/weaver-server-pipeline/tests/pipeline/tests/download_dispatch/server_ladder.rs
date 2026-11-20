// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An article missing everywhere walks the server ladder: one ask per server
// that may still answer, best level first, backfill only once every fill
// server has said no. A server that already said no for a queue costs that
// server one walk of the queue, not one walk per ask.

use std::collections::HashMap;
use weaver_server_pipeline::jobs::SegmentId;
use weaver_server_pipeline::pipeline::tests::{
    dispatch_fixture::DispatchFixture, segmented_job_spec,
};
use weaver_server_pipeline::{DownloadWork, JobId, NzbFileId};

// `count` servers spread over ten levels so that pool order and level order
// differ; the two worst levels are backfill.
fn ten_levels(count: usize, stride: usize) -> Vec<(u32, bool)> {
    (0..count)
        .map(|idx| {
            let group = ((idx * stride) % 10) as u32;
            (group, group >= 8)
        })
        .collect()
}

// Run one article missing everywhere to its end. Each round asks every
// server in pool order; the one handed the article answers "not found".
// Returns the servers asked, in order, and the articles booked missing.
async fn run_ladder(levels: &[(u32, bool)], refusing: &[usize]) -> (Vec<usize>, u64) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::ladder(&temp_dir, levels).await;
    for server in refusing {
        pipeline.refuse_login(*server).await;
    }
    let job_id = JobId(42001);
    pipeline
        .start_pass(
            job_id,
            segmented_job_spec("Missing Everywhere", "payload.bin", &[128]),
        )
        .await;

    let mut asked = Vec::new();
    // One round per server at most, plus the round that finds nothing.
    for _ in 0..=levels.len() {
        let mut handed = None;
        for server in 0..levels.len() {
            let mut works = pipeline.ask(server, 1);
            if works.is_empty() {
                continue;
            }
            assert_eq!(works.len(), 1);
            handed = Some((server, works.pop().unwrap()));
            break;
        }
        let Some((server, work)) = handed else {
            break;
        };
        asked.push(server);
        pipeline.answer_not_found(server, work).await;
    }
    let lost = pipeline.lost_articles();
    (asked, lost)
}

// Servers in ladder order: fill before backfill, then level, then pool order.
fn ladder_order(levels: &[(u32, bool)], skip: impl Fn(usize) -> bool) -> Vec<usize> {
    let mut order: Vec<usize> = (0..levels.len()).filter(|idx| !skip(*idx)).collect();
    order.sort_by_key(|idx| (levels[*idx].1, levels[*idx].0, *idx));
    order
}

async fn assert_ladder(levels: &[(u32, bool)]) {
    let (asked, lost) = run_ladder(levels, &[]).await;
    assert_eq!(
        asked,
        ladder_order(levels, |_| false),
        "one ask per server, level by level, backfill last"
    );
    assert_eq!(lost, 1, "booked missing once every server said no");
}

#[tokio::test]
async fn an_article_missing_everywhere_asks_25_servers_on_10_levels_once_each() {
    assert_ladder(&ten_levels(25, 3)).await;
}

#[tokio::test]
async fn an_article_missing_everywhere_asks_50_servers_on_10_levels_once_each() {
    assert_ladder(&ten_levels(50, 7)).await;
}

// A fill server refusing the credentials has answered: the ladder steps past
// it. It never opens backfill, which only a "not found" from every fill
// server does, so the article is booked missing without a backfill ask.
#[tokio::test]
async fn a_fill_server_refusing_credentials_is_passed_and_keeps_backfill_shut() {
    let levels = ten_levels(50, 7);
    let refusing = 3;
    assert!(!levels[refusing].1);
    let (asked, lost) = run_ladder(&levels, &[refusing]).await;
    assert_eq!(
        asked,
        ladder_order(&levels, |idx| idx == refusing || levels[idx].1)
    );
    assert_eq!(lost, 1);
}

// The field's layout: three servers at the top level, two at the next,
// three at the third, then backfill at two levels.
fn field_levels() -> Vec<(u32, bool)> {
    let mut levels = vec![(0, false); 3];
    levels.extend([(1, false); 2]);
    levels.extend([(2, false); 3]);
    levels.extend([(3, true); 2]);
    levels.extend([(4, true); 3]);
    levels
}

// A dead post, `files` files of `articles` articles each, no recovery,
// every article missing on every server, run with every server asking each
// round as a free lane would: each server with something it may take is
// handed one article, and every article handed out that round is answered
// "not found" through the lane result path. Returns the asks made until the
// post was given up (all of them when it never was), the asks a full walk
// would make, and the order of each article's asks.
struct DeadPostRun {
    asks_at_give_up: Option<usize>,
    rounds_at_give_up: Option<usize>,
    asks: usize,
    // (ask, article) for each article, in the order its walk ended.
    walked_out: Vec<(usize, SegmentId)>,
    per_article: HashMap<SegmentId, Vec<usize>>,
    reason: Option<String>,
    lost: u64,
    // The articles booked missing, and booked unservable past a server
    // refusing the login.
    missing: usize,
    unservable: usize,
    cause: Option<weaver_server_pipeline::jobs::SemanticTerminalCause>,
}

async fn run_dead_post(levels: &[(u32, bool)], files: u32, articles: u32) -> DeadPostRun {
    run_dead_post_refused(levels, files, articles, &[]).await
}

// As [`run_dead_post`], with the servers in `refusing` refusing the login
// from the start, so none of them is ever asked.
async fn run_dead_post_refused(
    levels: &[(u32, bool)],
    files: u32,
    articles: u32,
    refusing: &[usize],
) -> DeadPostRun {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::ladder(&temp_dir, levels).await;
    for &server in refusing {
        pipeline.refuse_login(server).await;
    }
    // A fill server that refuses keeps backfill shut, so the walk is the
    // fill servers left; a backfill server that refuses is only passed.
    let fill_refuses = refusing.iter().any(|server| !levels[*server].1);
    let walk = (0..levels.len())
        .filter(|server| !refusing.contains(server) && !(fill_refuses && levels[*server].1))
        .count();
    let mut spec = segmented_job_spec("Dead Field Post", "f00.mkv", &vec![128; articles as usize]);
    let template = spec.files[0].clone();
    spec.files = (0..files)
        .map(|file| {
            let mut spec = template.clone();
            spec.filename = format!("f{file:02}.mkv");
            for segment in &mut spec.segments {
                segment.message_ids = weaver_server_pipeline::jobs::MessageIds::new(vec![format!(
                    "f{file}-s{}@example.com",
                    segment.ordinal
                )]);
            }
            spec
        })
        .collect();
    spec.total_bytes *= u64::from(files);
    let job_id = JobId(42003);
    pipeline.start_pass(job_id, spec).await;
    {
        let queue = &mut pipeline
            .job_mut(job_id)
            .articles
            .active_mut()
            .unwrap()
            .download_queue;
        for file in 0..files {
            queue.note_first_article(held_segment(job_id, file, 0));
        }
    }
    pipeline.rewrite_queue(job_id, |_| {});

    let mut run = DeadPostRun {
        asks_at_give_up: None,
        rounds_at_give_up: None,
        asks: 0,
        walked_out: Vec::new(),
        per_article: HashMap::new(),
        reason: None,
        lost: 0,
        missing: 0,
        unservable: 0,
        cause: None,
    };
    let mut rounds = 0;
    loop {
        rounds += 1;
        let handed: Vec<(usize, DownloadWork)> = (0..levels.len())
            .filter_map(|server| {
                let mut works = pipeline.ask(server, 1);
                works.pop().map(|work| (server, work))
            })
            .collect();
        if handed.is_empty() {
            break;
        }
        for (server, work) in handed {
            run.asks += 1;
            let asked = run.per_article.entry(work.segment_id).or_default();
            asked.push(server);
            if asked.len() == walk {
                run.walked_out.push((run.asks, work.segment_id));
            }
            pipeline.answer_not_found(server, work).await;
            if run.asks_at_give_up.is_none() && pipeline.dead_post_reason(job_id).is_some() {
                run.asks_at_give_up = Some(run.asks);
                run.rounds_at_give_up = Some(rounds);
            }
        }
    }
    run.reason = pipeline.dead_post_reason(job_id).cloned();
    run.lost = pipeline.lost_articles();
    (run.missing, run.unservable) = pipeline.missing_and_unservable_counts();
    run.cause = pipeline.terminal_cause(job_id);
    run
}

// The same dead posts with one server refusing the login and never asked.
// Every article that walks out is booked unservable, not missing, and the
// post is given up on the same ask, in the same round, in the same words and
// with the same count as on the same servers without the one that refuses,
// where every article is booked missing.
#[tokio::test]
async fn a_dead_post_past_a_refusing_server_is_given_up_where_a_missing_one_is() {
    let levels = field_levels();
    let last = levels.len() - 1;
    // The last backfill server refuses: the walk is every server before it.
    // The first fill server refuses: backfill stays shut, and the walk is
    // the fill servers after it.
    let fills = levels.iter().filter(|level| !level.1).count();
    for (refusing, without) in [(last, &levels[..last]), (0, &levels[1..fills])] {
        for (files, articles) in [(25, 4), (6, 4)] {
            let refused = run_dead_post_refused(&levels, files, articles, &[refusing]).await;
            let plain = run_dead_post(without, files, articles).await;
            eprintln!(
                "server {refusing} refusing, {files} files: give-up after {:?} asks, round {:?}, \
                 {} booked ({} unservable, {} missing): {:?}; without it: {:?} asks, round {:?}, \
                 {} booked ({} missing): {:?}",
                refused.asks_at_give_up,
                refused.rounds_at_give_up,
                refused.lost,
                refused.unservable,
                refused.missing,
                refused.reason,
                plain.asks_at_give_up,
                plain.rounds_at_give_up,
                plain.lost,
                plain.missing,
                plain.reason
            );
            assert!(refused.asks_at_give_up.is_some());
            assert_eq!(refused.asks_at_give_up, plain.asks_at_give_up);
            assert_eq!(refused.rounds_at_give_up, plain.rounds_at_give_up);
            assert_eq!(refused.asks, plain.asks);
            assert_eq!(refused.reason, plain.reason);
            assert_eq!(refused.lost, plain.lost);
            assert_eq!(refused.unservable, plain.missing);
            assert!(refused.unservable > 0);
            assert_eq!((refused.missing, plain.unservable), (0, 0));
            // The cause is the missing articles either way: the servers
            // asked said so, and the job's next duplicate is tried.
            assert_eq!(refused.cause, plain.cause);
            assert_eq!(
                plain.cause,
                Some(
                    weaver_server_pipeline::jobs::SemanticTerminalCause::MissingArticlesOrLowHealth
                )
            );
        }
    }
}

// The field's shape: thirteen servers over five levels, 25 files of four
// articles, no recovery, all missing. Each article walks the servers in
// level order,
// and the post is given up, in the sample's words, on the ask that ends the
// twentieth first article's walk: 20 of 25 is the sample's 80% share. Its
// health crossed its line at the sixteenth, but a sample with first
// articles still to come rules first, so the files it would leave stay
// fetchable.
#[tokio::test]
async fn a_dead_post_on_the_field_layout_asks_first_articles_first_and_ends() {
    const FILES: u32 = 25;
    const ARTICLES: u32 = 4;
    let levels = field_levels();
    let run = run_dead_post(&levels, FILES, ARTICLES).await;
    let full_walk = (FILES * ARTICLES) as usize * levels.len();
    eprintln!(
        "field shape: give-up after {:?} of {} asks, round {:?} ({} made), {} booked lost: {:?}",
        run.asks_at_give_up, full_walk, run.rounds_at_give_up, run.asks, run.lost, run.reason
    );
    let twentieth_head = run
        .walked_out
        .iter()
        .filter(|(_, segment)| segment.segment_number == 0)
        .nth(19)
        .map(|(ask, _)| *ask);
    assert_eq!(run.asks_at_give_up, twentieth_head);
    assert!(
        run.reason
            .as_deref()
            .is_some_and(|reason| reason.contains("20 of 25 first articles")),
        "{:?} after {} asks",
        run.reason,
        run.asks
    );
    for file in 0..FILES {
        let head = held_segment(JobId(42003), file, 0);
        let asked = run.per_article.get(&head).cloned().unwrap_or_default();
        assert!(
            asked
                .windows(2)
                .all(|pair| (levels[pair[0]].1, levels[pair[0]].0)
                    <= (levels[pair[1]].1, levels[pair[1]].0)),
            "{head} walked the levels out of order: {asked:?}"
        );
    }
    // No sooner is possible: a first article is asked by one server a
    // round, so the last server first gets one in the round after the
    // twelve before it, and it asks one a round from then. A first article
    // some server already missed goes back ahead of the untried ones, so
    // the last server is never left waiting while one is open to it.
    assert_eq!(run.rounds_at_give_up, Some(levels.len() - 1 + 20));
}

// Fewer than ten files: the first-article sample does not rule, and the
// post's health ends it. Four of 24 equal articles lost take it to 83.3%,
// below its 85% line, but two files still have their first article out,
// and were they recovery volumes their eight articles would draw the line
// at 50%: the verdict waits for them. On the ask that ends the fifth first
// article's walk, 5 of 24 lost take it to 79.1%, below the 80% line even
// the one file still out could draw, and the post is given up then.
#[tokio::test]
async fn a_small_dead_post_on_the_field_layout_ends_on_its_health() {
    let levels = field_levels();
    let run = run_dead_post(&levels, 6, 4).await;
    eprintln!(
        "small shape: give-up after {:?} of {} asks, round {:?} ({} made), {} booked lost: {:?}",
        run.asks_at_give_up,
        6 * 4 * levels.len(),
        run.rounds_at_give_up,
        run.asks,
        run.lost,
        run.reason
    );
    assert_eq!(run.asks_at_give_up, Some(run.walked_out[4].0));
    assert!(
        run.reason
            .as_deref()
            .is_some_and(|reason| reason.contains("below critical")),
        "{:?}",
        run.reason
    );
}

// Segment `segment_number` of file `file_index` of `job_id`.
fn held_segment(job_id: JobId, file_index: u32, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId { job_id, file_index },
        segment_number,
    }
}
