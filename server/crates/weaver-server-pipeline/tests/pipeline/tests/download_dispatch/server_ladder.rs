// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An article missing everywhere walks the server ladder: one ask per server
// that may still answer, best level first, backfill only once every fill
// server has said no. A server that already said no for a queue costs that
// server one walk of the queue, not one walk per ask.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use weaver_model::files::FileRole;
use weaver_nntp::client::{NntpClient, NntpClientConfig};
use weaver_server_pipeline::JobStatus;
use weaver_server_pipeline::jobs::SegmentId;
use weaver_server_pipeline::pipeline::tests::build_test_par2_index_for_files;
use weaver_server_pipeline::pipeline::tests::dispatch_fixture::ladder_failures::{
    MAX_SEGMENT_RETRIES, SegmentOutcome,
};
use weaver_server_pipeline::pipeline::tests::dispatch_fixture::ladder_quota::{
    charge, metered, quota_config,
};
use weaver_server_pipeline::pipeline::tests::{
    dispatch_fixture::DispatchFixture, segmented_job_spec,
};
use weaver_server_pipeline::{DownloadQueue, FileSpec, JobSpec, MessageIds, SegmentSpec};
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

const ALPHA: JobId = JobId(42005);

// Job "Alpha": one file of two articles and no recovery, on three fill
// servers, `refusing` refusing the login. Its first article is offered to
// every server and answered "not found" by each one handed it. Returns the
// servers asked.
async fn walk_alphas_first_article(
    temp_dir: &tempfile::TempDir,
    refusing: Option<usize>,
) -> (DispatchFixture, Vec<usize>) {
    let levels = vec![(0, false), (1, false), (2, false)];
    let mut pipeline = DispatchFixture::ladder(temp_dir, &levels).await;
    if let Some(server) = refusing {
        pipeline.refuse_login(server).await;
    }
    pipeline
        .start_pass(
            ALPHA,
            segmented_job_spec("Alpha", "payload.bin", &[128, 128]),
        )
        .await;
    let head = held_segment(ALPHA, 0, 0);
    let mut asked = Vec::new();
    for server in 0..levels.len() {
        let mut works = pipeline.ask(server, 1);
        if let Some(work) = works.pop() {
            assert_eq!(work.segment_id, head);
            asked.push(server);
            pipeline.answer_not_found(server, work).await;
        }
    }
    (pipeline, asked)
}

// The reason a failed job gives.
fn failed_for(pipeline: &DispatchFixture, job_id: JobId) -> Option<String> {
    match pipeline.job_if_present(job_id).map(|state| &state.status) {
        Some(JobStatus::Failed { error }) => Some(error.clone()),
        _ => None,
    }
}

// An article every server asked said "not found", in a job where another
// server refuses the login and is never asked, is booked unservable, not
// missing. The job ends where it would have, its cause the missing articles,
// with the refusing server named.
#[tokio::test]
async fn a_not_found_walk_past_a_refusing_server_is_booked_unservable_and_names_the_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, asked) = walk_alphas_first_article(&temp_dir, Some(2)).await;
    assert_eq!(asked, vec![0, 1], "the refusing server is never handed it");
    assert_eq!(pipeline.lost_articles(), 1);
    // The file cannot be whole and nothing can rebuild it, so the job ends
    // there, as it does on a missing article, and the article it never
    // asked for is abandoned.
    assert_eq!(
        pipeline.ended_segments(ALPHA),
        vec![
            (0, 0, SegmentOutcome::UNSERVABLE_NOT_FOUND),
            (0, 1, SegmentOutcome::ABANDONED)
        ]
    );
    let missing = Some(weaver_server_pipeline::SemanticTerminalCause::MissingArticlesOrLowHealth);
    assert_eq!(pipeline.terminal_cause(ALPHA), missing);
    let words = pipeline.login_refusal_words(ALPHA).unwrap();
    assert!(words.contains("s2.example"), "{words}");
    assert!(!words.contains("s0.example") && !words.contains("s1.example"));
    // Failing for want of articles, its reason carries those words and its
    // cause stays the missing articles.
    pipeline.fail_job_on_health(ALPHA, "the post cannot complete".into());
    let reason = failed_for(&pipeline, ALPHA).unwrap();
    assert_eq!(reason, format!("the post cannot complete; {words}"));
    assert_eq!(pipeline.terminal_cause(ALPHA), missing);
}

// Failing for any other reason, the same job's reason is its own.
#[tokio::test]
async fn a_failure_of_another_kind_past_a_refusing_server_says_nothing_of_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _) = walk_alphas_first_article(&temp_dir, Some(2)).await;
    assert!(pipeline.login_refusal_words(ALPHA).is_some());
    pipeline.fail_job(ALPHA, "the password is wrong".into());
    assert_eq!(
        failed_for(&pipeline, ALPHA).as_deref(),
        Some("the password is wrong")
    );
    assert_eq!(
        pipeline.terminal_cause(ALPHA),
        Some(weaver_server_pipeline::SemanticTerminalCause::MissingArticlesOrLowHealth)
    );
}

// The ladder's servers in another order, or fewer of them: server `n` is the
// same server in every client built here.
fn ladder_servers(order: &[usize]) -> NntpClient {
    NntpClient::new(NntpClientConfig {
        servers: order
            .iter()
            .map(|&idx| {
                let mut config = weaver_nntp::pool::ServerPoolConfig {
                    max_connections: 100,
                    stable_id: weaver_nntp::transfer::StableServerId(idx as u32),
                    ..Default::default()
                };
                config.server.host = format!("s{idx}.example");
                config
            })
            .collect(),
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    })
}

// The refusing servers a job holds follow the server list through a change
// of it: a reordered server is still the one named, a removed one is not
// named, and with none left there are no words. One server reads as one and
// several as several.
#[tokio::test]
async fn the_refusing_servers_named_follow_a_reordered_and_a_shortened_server_list() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _) = walk_alphas_first_article(&temp_dir, Some(2)).await;
    let rest = "never asked for the articles no other server had";
    let one = |name: &str| Some(format!("server {name} refuses the login and was {rest}"));
    assert_eq!(pipeline.login_refusal_words(ALPHA), one("s2.example:563"));

    // Reordered: what was third is first, and is still the one named.
    let generation = pipeline.pool_generation();
    pipeline.activate_nntp(ladder_servers(&[2, 0, 1]), 8, generation + 1);
    assert_eq!(pipeline.login_refusal_words(ALPHA), one("s2.example:563"));

    // A second server refuses too: s0, now at the second place.
    pipeline.note_login_refusals(ALPHA, [1usize].into_iter().collect());
    pipeline.note_login_refusals(ALPHA, [0usize, 1].into_iter().collect());
    assert_eq!(pipeline.login_refusal_count(ALPHA), 2);
    assert_eq!(
        pipeline.login_refusal_words(ALPHA),
        Some(format!(
            "servers s2.example:563, s0.example:563 refuse the login and were {rest}"
        ))
    );

    // s2 leaves the list: only s0 is named.
    pipeline.activate_nntp(ladder_servers(&[1, 0]), 8, generation + 2);
    assert_eq!(pipeline.login_refusal_words(ALPHA), one("s0.example:563"));

    // s0 leaves too: no words, and a failure for want of articles adds none.
    pipeline.activate_nntp(ladder_servers(&[1]), 8, generation + 3);
    assert_eq!(pipeline.login_refusal_words(ALPHA), None);
    pipeline.fail_job_on_health(ALPHA, "the post cannot complete".into());
    assert_eq!(
        failed_for(&pipeline, ALPHA).as_deref(),
        Some("the post cannot complete")
    );
}

// The same job with no server refusing: every server is asked, the article
// is booked missing, and the cause and the words are the missing-article
// ones.
#[tokio::test]
async fn a_not_found_walk_over_every_server_is_still_booked_missing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, asked) = walk_alphas_first_article(&temp_dir, None).await;
    assert_eq!(asked, vec![0, 1, 2]);
    assert_eq!(
        pipeline.ended_segments(ALPHA),
        vec![
            (0, 0, SegmentOutcome::MISSING),
            (0, 1, SegmentOutcome::ABANDONED)
        ]
    );
    assert_eq!(
        pipeline.terminal_cause(ALPHA),
        Some(weaver_server_pipeline::SemanticTerminalCause::MissingArticlesOrLowHealth)
    );
    assert_eq!(pipeline.login_refusal_words(ALPHA), None);
    pipeline.fail_job(ALPHA, "the post cannot complete".into());
    assert_eq!(
        failed_for(&pipeline, ALPHA).as_deref(),
        Some("the post cannot complete")
    );
    assert_eq!(
        pipeline.terminal_cause(ALPHA),
        Some(weaver_server_pipeline::SemanticTerminalCause::MissingArticlesOrLowHealth)
    );
}

// A job with both kinds: six files of four articles, no recovery. The first
// article to walk out was asked of every server and is missing. Then one
// server starts refusing the login, and each article that walks out after
// was never asked of it and is unservable. The job's cause is the missing
// articles, and its words name the refusing server.
#[tokio::test]
async fn a_job_with_missing_and_unservable_articles_books_each_as_it_is() {
    let levels = vec![(0, false), (1, false), (2, false)];
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::ladder(&temp_dir, &levels).await;
    let mut spec = segmented_job_spec("Both Kinds", "f00.mkv", &[128; 4]);
    let template = spec.files[0].clone();
    spec.files = (0..6)
        .map(|file| {
            let mut spec = template.clone();
            spec.filename = format!("f{file:02}.mkv");
            for segment in &mut spec.segments {
                segment.message_ids = weaver_server_pipeline::MessageIds::new(vec![format!(
                    "f{file}-s{}@example.com",
                    segment.ordinal
                )]);
            }
            spec
        })
        .collect();
    spec.total_bytes *= 6;
    let job_id = JobId(42006);
    pipeline.start_pass(job_id, spec).await;

    // Every server asks each round, as a free lane would, and every article
    // handed out is answered "not found". Server 2 starts refusing the login
    // once the first article has walked out.
    let mut refused = false;
    loop {
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
            pipeline.answer_not_found(server, work).await;
        }
        if !refused && pipeline.has_terminal_segments() {
            pipeline.refuse_login(2).await;
            refused = true;
        }
    }

    let ended = pipeline.ended_segments(job_id);
    let booked = |state| ended.iter().filter(|(_, _, held)| *held == state).count();
    assert!(booked(SegmentOutcome::MISSING) >= 1, "{ended:?}");
    assert!(
        booked(SegmentOutcome::UNSERVABLE_NOT_FOUND) >= 1,
        "{ended:?}"
    );
    assert_eq!(
        pipeline.lost_articles() as usize,
        booked(SegmentOutcome::MISSING) + booked(SegmentOutcome::UNSERVABLE_NOT_FOUND)
    );
    // The post is given up on its health, both kinds counted.
    assert!(pipeline.dead_post_reason(job_id).is_some());
    assert_eq!(
        pipeline.terminal_cause(job_id),
        Some(weaver_server_pipeline::SemanticTerminalCause::MissingArticlesOrLowHealth)
    );
    let words = pipeline.login_refusal_words(job_id).unwrap();
    assert!(words.contains("s2.example"), "{words}");
}

fn body_reply(code: u16, message: &str) -> weaver_nntp::NntpError {
    weaver_nntp::NntpError::from_status(weaver_nntp::StatusCode::new(code), message)
}

async fn one_article_job(pipeline: &mut DispatchFixture, job_id: JobId) {
    pipeline
        .start_pass(
            job_id,
            segmented_job_spec("Odd Replies", "payload.bin", &[128]),
        )
        .await;
}

// 503 and 499 to BODY are the server's trouble, as 400 is: the article keeps
// its tries even with all of them spent, and waits to be asked again.
#[tokio::test]
async fn a_503_or_499_to_body_does_not_charge_the_article() {
    for (job, code) in [(42101, 503), (42102, 499)] {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut pipeline = DispatchFixture::ladder(&temp_dir, &[(0, false)]).await;
        let job_id = JobId(job);
        one_article_job(&mut pipeline, job_id).await;
        let mut work = pipeline.ask(0, 1).pop().unwrap();
        work.retry_count = MAX_SEGMENT_RETRIES;
        pipeline
            .answer_nntp(0, work, body_reply(code, "internal error"))
            .await;

        assert_eq!(
            pipeline
                .metrics()
                .segments_failed_permanent
                .load(Ordering::Relaxed),
            0,
            "{code}"
        );
        assert_eq!(pipeline.job(job_id).failed_bytes, 0, "{code}");
        let due = pipeline.retry_deadline().unwrap();
        let ready = pipeline.take_due_retry_works(due);
        assert_eq!(ready.len(), 1, "{code}");
        assert_eq!(ready[0].retry_count, MAX_SEGMENT_RETRIES, "{code}");
        assert!(ready[0].exclude_servers.is_empty(), "{code}");
    }
}

// An answer to BODY the client has no class for costs the article its tries
// on that server as before. Once they are spent and another server may still
// serve it, the server joins the article's exclusions and the next server is
// handed it with a fresh count.
#[tokio::test]
async fn an_unknown_body_reply_moves_the_article_to_the_next_server_once_its_tries_are_spent() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::ladder(&temp_dir, &[(0, false), (0, false)]).await;
    let job_id = JobId(42103);
    one_article_job(&mut pipeline, job_id).await;

    // The last try on server 0 spent: the article goes to the other server
    // at once.
    let mut work = pipeline.ask(0, 1).pop().unwrap();
    work.retry_count = MAX_SEGMENT_RETRIES;
    pipeline
        .answer_nntp(0, work, body_reply(501, "syntax error"))
        .await;
    assert!(pipeline.ask(0, 1).is_empty(), "server 0 is done with it");
    let moved = pipeline.ask(1, 1).pop().expect("server 1 is handed it");
    assert_eq!(moved.exclude_servers, [0]);
    assert_eq!(moved.retry_count, 0);
    assert_eq!(
        pipeline
            .metrics()
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(pipeline.job(job_id).failed_bytes, 0);
}

// The same with one server: nothing is left to ask, so the spent tries end
// the article as before.
#[tokio::test]
async fn an_unknown_body_reply_on_the_only_server_ends_the_article_once_its_tries_are_spent() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::ladder(&temp_dir, &[(0, false)]).await;
    let job_id = JobId(42104);
    one_article_job(&mut pipeline, job_id).await;

    let mut work = pipeline.ask(0, 1).pop().unwrap();
    let segment_id = work.segment_id;
    work.retry_count = MAX_SEGMENT_RETRIES;
    pipeline
        .answer_nntp(0, work, body_reply(501, "syntax error"))
        .await;

    assert_eq!(
        pipeline
            .metrics()
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        pipeline.segment_outcome(segment_id),
        Some(SegmentOutcome::RETRIES_EXHAUSTED)
    );
    assert!(pipeline.ask(0, 1).is_empty());
}

// With one server there is nowhere else to ask: an article it keeps
// answering 503 for while others download is booked lost once its tries are
// spent, as before.
#[tokio::test]
async fn an_article_the_only_server_keeps_answering_503_for_is_booked_lost() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::ladder(&temp_dir, &[(0, false)]).await;
    let job_id = JobId(42112);
    one_article_job(&mut pipeline, job_id).await;
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    let answers = pipeline.answer_503_until_spent(0).await;
    assert_eq!(answers, 3 + MAX_SEGMENT_RETRIES);
    assert_eq!(
        pipeline
            .metrics()
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        pipeline.segment_outcome(segment_id),
        Some(SegmentOutcome::RETRIES_EXHAUSTED)
    );
}

// Two fill servers: a 503 from one hands the article's retry to the other
// at once, with no try spent and nothing excluded, as before.
#[tokio::test]
async fn a_503_from_one_fill_server_rotates_the_article_to_the_other_fill_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::ladder(&temp_dir, &[(0, false), (0, false)]).await;
    let job_id = JobId(42113);
    one_article_job(&mut pipeline, job_id).await;

    let work = pipeline.ask(0, 1).pop().unwrap();
    pipeline
        .answer_nntp(0, work, body_reply(503, "service unavailable"))
        .await;
    pipeline.requeue_infrastructure_retry();
    assert!(pipeline.ask(0, 1).is_empty(), "server 0 is avoided");
    let moved = pipeline.ask(1, 1).pop().expect("server 1 is handed it");
    assert!(moved.exclude_servers.is_empty());
    assert_eq!(moved.retry_count, 0);
    assert_eq!(
        pipeline
            .metrics()
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );
}

// A server that needs a group selected and answers 411 for every newsgroup
// of the work does not carry the post: the leased article leaves that
// server for the next as on a 430, and spends no try.
#[tokio::test]
async fn every_group_refused_moves_the_lease_to_the_next_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::ladder(&temp_dir, &[(0, false), (0, false)]).await;
    let job_id = JobId(42105);
    one_article_job(&mut pipeline, job_id).await;

    let work = pipeline.ask(0, 1).pop().unwrap();
    assert_eq!(pipeline.group_acquire_failed(job_id, work).await, [Some(0)]);

    assert!(pipeline.ask(0, 1).is_empty());
    let moved = pipeline.ask(1, 1).pop().expect("server 1 is handed it");
    assert_eq!(moved.exclude_servers, [0]);
    assert_eq!(moved.retry_count, 0);
    assert_eq!(pipeline.lost_articles(), 0);
}

// Three top servers that lack every article of a large queue, ten lower
// ones that hold them, and one more lower one on a quota that downloads
// between asks. However often a top server asks, it walks the queue once;
// a whole walk per ask is the servers-times-queue cost. Bytes charged to a
// quota with room to spare, and a job restored after a restart with bytes
// already down, keep it at one walk.
#[tokio::test]
async fn a_server_that_refused_a_queue_walks_it_once_however_often_it_asks() {
    const ARTICLES: usize = 2_000;
    const ASKS: usize = 40;
    let mut levels = vec![(0, false); 3];
    levels.extend(std::iter::repeat_n((1, false), 11));

    let mut walked = Vec::new();
    for (floors_off, restored) in [(true, false), (false, false), (false, true)] {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut pipeline = DispatchFixture::ladder(&temp_dir, &levels).await;
        let control = metered(13, 1 << 40);
        pipeline.meter_ladder_server(&levels, 13, Arc::clone(&control));
        let job_id = JobId(42002);
        pipeline
            .add_job(
                job_id,
                segmented_job_spec(
                    "Top Servers Lack It",
                    "payload.bin",
                    &vec![128u32; ARTICLES],
                ),
            )
            .await;
        pipeline.rewrite_queue(job_id, |work| {
            work.exclude_servers = weaver_nntp::ServerSet::from([0, 1, 2]);
        });
        if restored {
            // Restored with bytes already down: the restart checkpoint is
            // in force, but it refuses none of what the top servers are
            // refused for.
            pipeline.job_mut(job_id).restored_download_floor_bytes = 4_096;
            assert!(!pipeline.checkpoint_admits_all(job_id));
        }

        DispatchFixture::set_scan_floors_off(floors_off);
        let skipped = |pipeline: &DispatchFixture| {
            pipeline
                .metrics()
                .download_scheduler_scan_items_skipped_total
                .load(Ordering::Relaxed)
        };
        let before = skipped(&pipeline);
        for _ in 0..ASKS {
            charge(&control, 700_000);
            for server in 0..3 {
                assert!(pipeline.take_servable_works(job_id, server, 8).is_none());
            }
        }
        walked.push(skipped(&pipeline) - before);

        // The lower servers still take the whole queue, in queue order.
        let mut taken = 0;
        let mut server = 3;
        while let Some(works) = { pipeline.take_servable_works(job_id, server, 64) } {
            taken += works.len();
            server = 3 + (server - 2) % 10;
        }
        DispatchFixture::set_scan_floors_off(false);
        assert_eq!(taken, ARTICLES);
    }
    assert_eq!(
        walked[0],
        (3 * ASKS * ARTICLES) as u64,
        "a whole walk per ask"
    );
    assert_eq!(walked[1], (3 * ARTICLES) as u64, "one walk per server");
    assert_eq!(
        walked[2],
        (3 * ARTICLES) as u64,
        "one walk per server under a restored checkpoint"
    );
}

// A quota that runs out hands its level's work to the next level, and one
// that comes back takes it again, however the memo of the earlier walks
// stands: the walk is never kept past a refusal that no longer holds.
#[tokio::test]
async fn work_follows_a_quota_that_runs_out_and_comes_back() {
    let levels = vec![(0, false), (1, false)];
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::ladder(&temp_dir, &levels).await;
    let control = metered(0, 1 << 30);
    pipeline.meter_ladder_server(&levels, 0, Arc::clone(&control));
    let job_id = JobId(42003);
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Metered Reel", "payload.bin", &vec![128u32; 300]),
        )
        .await;
    pipeline.open_pass(job_id);

    // The top server has quota: the lower one is refused the whole queue,
    // twice, which leaves it a memo past the end.
    for _ in 0..2 {
        assert!(pipeline.ask(1, 4).is_empty());
    }
    assert_eq!(pipeline.ask(0, 4).len(), 4);

    // Bytes the top server downloads, with room to spare, change nothing.
    charge(&control, 100_000);
    assert!(pipeline.ask(1, 4).is_empty());

    // Its quota runs out: the lower server takes the work.
    control.update_config(quota_config(100_001));
    assert!(pipeline.ask(0, 4).is_empty());
    assert_eq!(pipeline.ask(1, 4).len(), 4);

    // The quota comes back: the top server takes the work again, and the
    // lower one is refused it.
    control.update_config(quota_config(1 << 30));
    assert!(pipeline.ask(1, 4).is_empty());
    assert_eq!(pipeline.ask(0, 4).len(), 4);
}

// An article put back after a "not found" sends back over the queue only
// the servers that may take it. Two top servers, a lower level not reached
// yet and a backfill server, all asking through the dispatch path. Server 0
// is handed an article and answers "not found" each round: the first
// article first, which goes back at the head of the queue, then ordinary
// ones, which go back behind their band. The lower level and the backfill
// server are refused the whole queue once; an article that comes back is one
// more refusal each, never a new walk, so what they read does not grow with
// the queue.
#[tokio::test]
async fn a_requeued_article_sends_no_refused_server_back_over_the_queue() {
    const ROUNDS: usize = 12;
    let levels: Vec<(u32, bool)> = vec![(0, false), (0, false), (1, false), (2, true)];
    const REFUSED: [usize; 2] = [2, 3];
    let mut reread = Vec::new();
    // The same holds for an ordinary retry: the connection fails under the
    // article, which waits out its retry and goes back with no server
    // refused to it, steered off the server it failed on.
    for (articles, not_found) in [
        (400usize, true),
        (1_600, true),
        (400, false),
        (1_600, false),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut pipeline = DispatchFixture::ladder(&temp_dir, &levels).await;
        let job_id = JobId(42030);
        pipeline
            .add_job(
                job_id,
                segmented_job_spec("Comes Back", "payload.bin", &vec![128u32; articles]),
            )
            .await;
        pipeline
            .job_mut(job_id)
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .note_first_article(held_segment(job_id, 0, 0));
        pipeline.rewrite_queue(job_id, |_| {});
        pipeline.open_pass(job_id);
        // What one ask of a refused server reads of the queue to be told
        // there is nothing for it. A debug build checks every idle answer
        // with a whole walk past the floors, which reads the queue once
        // more; that check is taken out of the count.
        let refused_ask = |pipeline: &mut DispatchFixture, server: usize| {
            let skipped = |pipeline: &DispatchFixture| {
                pipeline
                    .metrics()
                    .download_scheduler_scan_items_skipped_total
                    .load(Ordering::Relaxed)
            };
            let before = skipped(pipeline);
            assert!(pipeline.ask(server, 8).is_empty());
            let queued = pipeline
                .job(job_id)
                .articles
                .download_queue()
                .map_or(0, DownloadQueue::len) as u64;
            let checked = if cfg!(debug_assertions) { queued } else { 0 };
            skipped(pipeline) - before - checked
        };

        // Each refused server walks the queue once.
        let mut walked = 0;
        for server in REFUSED {
            walked += refused_ask(&mut pipeline, server);
        }
        assert_eq!(walked, (REFUSED.len() * articles) as u64);

        let mut read = 0;
        for round in 0..ROUNDS {
            let mut works = pipeline.ask(0, 1);
            let work = works.pop().expect("server 0 is handed an article");
            assert_eq!(
                work.segment_id == held_segment(job_id, 0, 0),
                round == 0,
                "the first article goes out first"
            );
            if not_found {
                pipeline.answer_not_found(0, work).await;
            } else {
                pipeline.answer_connection_failed(0, work).await;
                assert_eq!(pipeline.wake_all_retries(), 1);
            }
            for server in REFUSED {
                read += refused_ask(&mut pipeline, server);
            }
        }
        reread.push(read);

        // Server 1 may take what came back: the first article leads.
        let back = pipeline.ask(1, ROUNDS);
        assert_eq!(back.len(), ROUNDS);
        assert_eq!(back[0].segment_id, held_segment(job_id, 0, 0));
        assert_eq!(
            back[0].exclude_servers,
            if not_found {
                weaver_nntp::ServerSet::from([0])
            } else {
                weaver_nntp::ServerSet::EMPTY
            }
        );
    }
    assert_eq!(
        reread,
        vec![0; 4],
        "an article put back is refused where it is put, not by a new walk, however long the queue"
    );
}

// The floors change how much is read, never what is handed out: the same
// seeded run hands out the same articles in the same order with the floors
// on and with every walk whole. Between asks the run moves everything a
// floor's memo may depend on: not-founds and plain retries, rotation hints,
// a quota that downloads, runs out and comes back, a server that starts
// refusing its credentials (out of the policy for good), the restart
// checkpoint coming into force with a progress article and lifting again, a
// declared recovery index settling under a job whose payload waited for it,
// and the cursor of a uuencoded file moving under a capped spool.
#[tokio::test]
async fn the_scan_floors_hand_out_what_a_whole_walk_does() {
    let levels: Vec<(u32, bool)> = vec![(0, false), (0, false), (1, false), (2, true)];
    const METERED: usize = 1;
    const REFUSES_LATER: usize = 2;
    let jobs = [JobId(42010), JobId(42011)];
    const WAITS_FOR_INDEX: JobId = JobId(42012);
    const UUENCODED: JobId = JobId(42013);
    let mut runs = Vec::new();
    for floors_off in [true, false] {
        DispatchFixture::set_scan_floors_off(floors_off);
        let temp_dir = tempfile::tempdir().unwrap();
        let mut pipeline = DispatchFixture::ladder(&temp_dir, &levels).await;
        let control = metered(METERED as u32, 1 << 30);
        pipeline.meter_ladder_server(&levels, METERED, Arc::clone(&control));
        for (index, job_id) in jobs.into_iter().enumerate() {
            pipeline
                .add_job(
                    job_id,
                    segmented_job_spec(
                        &format!("Seeded Reel {index}"),
                        "payload.bin",
                        &vec![128u32; 150],
                    ),
                )
                .await;
            pipeline.open_pass(job_id);
        }
        // A job whose payload waits for its declared recovery index, and a
        // uuencoded job under a capped spool, which hands out only the
        // article its cursor waits on.
        let index = build_test_par2_index_for_files(&[("payload.mkv", &[7u8; 256])], 64);
        pipeline
            .add_job(
                WAITS_FOR_INDEX,
                JobSpec {
                    name: "Seeded Index".to_string(),
                    password: None,
                    total_bytes: index.len() as u64 + 40 * 128,
                    category: None,
                    metadata: vec![],
                    files: vec![
                        FileSpec {
                            filename: "00-index.par2".to_string(),
                            role: FileRole::Par2 {
                                is_index: true,
                                recovery_block_count: 0,
                            },
                            groups: vec!["alt.binaries.test".to_string()],
                            posted_at_epoch: None,
                            segments: vec![SegmentSpec {
                                ordinal: 0,
                                article_number: 0,
                                bytes: index.len() as u32,
                                message_ids: MessageIds::new(vec![
                                    "seeded-index@example.com".to_string(),
                                ]),
                            }],
                        },
                        FileSpec {
                            filename: "payload.mkv".to_string(),
                            role: FileRole::Standalone,
                            groups: vec!["alt.binaries.test".to_string()],
                            posted_at_epoch: None,
                            segments: (0..40)
                                .map(|number| SegmentSpec {
                                    ordinal: number,
                                    article_number: number,
                                    bytes: 128,
                                    message_ids: MessageIds::new(vec![format!(
                                        "seeded-payload-{number}@example.com"
                                    )]),
                                })
                                .collect(),
                        },
                    ],
                },
            )
            .await;
        pipeline.open_pass(WAITS_FOR_INDEX);
        pipeline
            .add_job(
                UUENCODED,
                segmented_job_spec("Seeded Uu", "payload.bin", &[128u32; 40]),
            )
            .await;
        pipeline.open_pass(UUENCODED);
        let index_file = held_segment(WAITS_FOR_INDEX, 0, 0).file_id;
        let uu_file = held_segment(UUENCODED, 0, 0).file_id;
        pipeline.cap_uu_spool(uu_file);
        let mut rng = Lcg(0x001a_dde7_5eed);
        let mut handouts = Vec::new();
        let mut changes = [0usize; 7];
        for step in 0..900 {
            // Something a memo may depend on moves, about one step in four.
            match rng.below(16) {
                // The metered server downloads, when its quota lets it.
                0 => {
                    let bytes = 1 + rng.below(200_000) as u64;
                    if let Ok(mut permit) = control.try_reserve(bytes) {
                        permit.record_unpaced(bytes as usize);
                        let _ = permit.finish();
                        changes[0] += 1;
                    }
                }
                // Its quota runs out, or comes back.
                1 => {
                    let limit = if rng.below(2) == 0 { 1 } else { 1 << 30 };
                    control.update_config(quota_config(limit));
                    changes[1] += 1;
                }
                // The restart checkpoint comes into force with a progress
                // article, or lifts.
                2 => {
                    let job_id = jobs[rng.below(2) as usize];
                    let floor = if rng.below(2) == 0 { 0 } else { 4_096 };
                    pipeline.job_mut(job_id).restored_download_floor_bytes = floor;
                    pipeline.set_checkpoint_progress(
                        job_id,
                        (floor != 0).then(|| held_segment(job_id, 0, 0)),
                    );
                    changes[2] += 1;
                }
                // Halfway, a fill server starts refusing its credentials.
                3 if step >= 450 && changes[3] == 0 => {
                    pipeline.refuse_login(REFUSES_LATER).await;
                    changes[3] += 1;
                }
                _ => {}
            }
            let server = rng.below(levels.len() as u32) as usize;
            let want = 1 + rng.below(6) as usize;
            let works = pipeline.ask(server, want);
            handouts.push((
                server,
                works.iter().map(|work| work.segment_id).collect::<Vec<_>>(),
            ));
            for mut work in works {
                match rng.below(4) {
                    // Not found here: back to the queue for the others.
                    // Half the time another server has said so too, so the
                    // article goes back refused to several floors at once.
                    0 => {
                        work.exclude_servers.insert(server);
                        if rng.below(2) == 0 {
                            work.exclude_servers
                                .insert(rng.below(levels.len() as u32) as usize);
                        }
                        pipeline.enqueue(work);
                    }
                    // A plain retry.
                    1 => pipeline.enqueue(work),
                    // A retry steered off this server.
                    2 => {
                        work.avoid_server = Some(server);
                        changes[4] += 1;
                        pipeline.enqueue(work);
                    }
                    // Delivered. A uuencoded part moves its file's cursor
                    // on; the recovery index settles, which lets the
                    // payload it held go.
                    _ => {
                        let delivered = work.segment_id;
                        if delivered.file_id == uu_file {
                            pipeline.set_uu_cursor(uu_file, delivered.segment_number + 1);
                            changes[5] += 1;
                        } else if delivered.file_id == index_file {
                            pipeline.lose_index(WAITS_FOR_INDEX);
                            changes[6] += 1;
                        }
                    }
                }
            }
        }
        DispatchFixture::set_scan_floors_off(false);
        assert!(
            changes.iter().all(|count| *count > 0),
            "every kind of change was made: {changes:?}"
        );
        runs.push((handouts, pipeline.lost_articles()));
    }
    assert!(runs[0].0.iter().any(|(_, works)| !works.is_empty()));
    assert_eq!(runs[0], runs[1]);
}

// A "not found" puts the article straight back for the other servers. It
// does not wait on the retry channel, which a busy link never drains.
#[tokio::test]
async fn a_not_found_article_is_back_on_the_queue_when_the_result_is_handled() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    pipeline.set_retention(&[0, 0]);
    let job_id = JobId(42020);
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Missing On One", "payload.bin", &[128]),
        )
        .await;
    {
        let active = pipeline.job_mut(job_id).articles.active_mut().unwrap();
        active.download_queue = DownloadQueue::new();
        active.recovery_queue = DownloadQueue::new();
    }
    let segment_id = held_segment(job_id, 0, 0);
    pipeline.hold_segments(&[segment_id]);
    pipeline.open_pass(job_id);

    pipeline
        .missing_result(segment_id, 0, 0, 0, weaver_nntp::ServerSet::EMPTY, true)
        .await;

    assert!(
        pipeline.retry_channel_empty(),
        "nothing went round the channel"
    );
    let queued: Vec<_> = pipeline
        .drain_dispatched_queue(job_id)
        .into_iter()
        .map(|work| (work.segment_id, work.exclude_servers))
        .collect();
    assert_eq!(
        queued,
        vec![(segment_id, weaver_nntp::ServerSet::from([0]))]
    );
    assert_eq!(pipeline.lost_articles(), 0);
}

struct Lcg(u64);

impl Lcg {
    fn below(&mut self, bound: u32) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 33) as u32) % bound
    }
}

// A client over `servers`: each server's stable id and priority group. The
// same stable id is the same server in every client built here.
fn identified_client(servers: &[(u32, u32)]) -> NntpClient {
    NntpClient::new(NntpClientConfig {
        servers: servers
            .iter()
            .map(|&(stable, group)| weaver_nntp::pool::ServerPoolConfig {
                max_connections: 100,
                group,
                stable_id: weaver_nntp::transfer::StableServerId(stable),
                ..Default::default()
            })
            .collect(),
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    })
}

// The server list is edited while articles are in flight: A, B, C, D become
// C, B, A, E. Each article keeps the servers it was tried on, under their new
// numbers, whether it sat on the queue, came back unasked from a lane, or
// came back as a 430 from a server the rebuild removed. A and C are never
// asked again, the added server is, and no index from the old pool is read
// against the new one.
#[tokio::test]
async fn a_server_list_edited_mid_flight_keeps_whom_each_article_was_tried_on() {
    const A: u32 = 1;
    const B: u32 = 2;
    const C: u32 = 3;
    const D: u32 = 4;
    const E: u32 = 5;
    // Ladder order before: A, C, D, B.
    let before = [(A, 0), (B, 3), (C, 1), (D, 2)];
    // After: E and A lead, C, then B; D is gone.
    let after = [(C, 1), (B, 3), (A, 0), (E, 0)];
    let old_index = |id: u32| before.iter().position(|(stable, _)| *stable == id).unwrap();
    let new_index = |id: u32| after.iter().position(|(stable, _)| *stable == id).unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    pipeline.set_client(identified_client(&before));
    let job_id = JobId(42006);
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Edited Mid Flight", "payload.bin", &[128; 3]),
        )
        .await;
    pipeline.open_pass(job_id);
    let ours = [0, 1, 2].map(|segment| held_segment(job_id, 0, segment));

    // A and C say "not found" for all three.
    for server in [A, C] {
        let works = pipeline.ask(old_index(server), 8);
        assert_eq!(works.len(), 3);
        for work in works {
            pipeline.answer_not_found(old_index(server), work).await;
        }
    }
    // D is handed all three: it is asking for one when the list is edited,
    // holds another on its lane unasked, and the third goes back untried.
    let mut on_d = pipeline.ask(old_index(D), 8);
    assert_eq!(on_d.len(), 3);
    on_d.sort_by_key(|work| work.segment_id.segment_number);
    let [back, unasked, asking] = <[DownloadWork; 3]>::try_from(on_d)
        .unwrap_or_else(|_| unreachable!("three articles were handed"));
    for work in [&back, &unasked, &asking] {
        assert_eq!(work.exclude_servers, [old_index(A), old_index(C)]);
    }
    pipeline.enqueue(back);
    pipeline.hold_works(vec![unasked.clone(), asking.clone()]);

    let old_generation = pipeline.pool_generation();
    pipeline.activate_nntp(identified_client(&after), 8, old_generation + 1);
    // The old pool has let go of its sockets. Set directly: the drain's own
    // handler runs a dispatch pass, and this test hands out by asking.
    pipeline.set_handoff_draining(false);

    // The lane gives back the article it never asked for, carrying the
    // old pool's numbers; the 430 from D arrives stamped with the old pool.
    assert_eq!(pipeline.release_unasked(unasked), 0);
    pipeline
        .missing_result(
            asking.segment_id,
            old_index(D),
            old_generation,
            asking.retry_count,
            asking.exclude_servers,
            false,
        )
        .await;

    // All three are queued, tried on A and C under their new numbers.
    let tried = [new_index(A), new_index(C)];
    let mut tried_sorted = tried.to_vec();
    tried_sorted.sort_unstable();
    {
        let queue = &pipeline
            .job(job_id)
            .articles
            .active()
            .unwrap()
            .download_queue;
        assert_eq!(queue.len(), 3);
        let carried = queue.count_matching(|work| {
            let mut set = work.exclude_servers.to_vec();
            set.sort_unstable();
            ours.contains(&work.segment_id) && set == tried_sorted
        });
        assert_eq!(carried, 3);
    }

    // Walk the new pool. E says "not found"; B is the last server left and
    // keeps what it is handed.
    let mut handed: HashMap<SegmentId, Vec<u32>> = HashMap::new();
    // One round per server at most, plus the round that finds nothing.
    for _ in 0..=after.len() {
        let mut any = false;
        for (index, &(stable, _)) in after.iter().enumerate() {
            for work in pipeline.ask(index, 8) {
                any = true;
                handed.entry(work.segment_id).or_default().push(stable);
                if stable == E {
                    pipeline.answer_not_found(index, work).await;
                }
            }
        }
        if !any {
            break;
        }
    }
    for segment in ours {
        assert_eq!(
            handed.get(&segment).map(Vec::as_slice),
            Some([E, B].as_slice()),
            "{segment}: A and C not asked again, E asked, B last"
        );
    }
    assert_eq!(pipeline.lost_articles(), 0);
}

// A fill server answers 503 for one article only while it serves the rest.
// Once the article has spent its tries there, the backfill server, which has
// it, is asked, and the article downloads from it: nothing is booked lost.
#[tokio::test]
async fn an_article_a_fill_server_keeps_answering_503_for_moves_to_backfill() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::ladder(&temp_dir, &[(0, false), (0, true)]).await;
    let job_id = JobId(42111);
    let payload = b"the article the fill server would not serve";
    pipeline
        .add_payload_job(job_id, "Busy Server", "payload.bin", payload.len() as u32)
        .await;

    assert!(pipeline.ask(1, 1).is_empty(), "backfill waits at first");
    let answers = pipeline.answer_503_until_spent(0).await;
    assert!(answers > MAX_SEGMENT_RETRIES, "{answers} answers");

    let work = pipeline
        .ask(1, 1)
        .pop()
        .expect("the backfill server is handed it");
    assert_eq!(work.exclude_servers, [0]);
    assert_eq!(work.retry_count, 0);
    assert!(
        !pipeline.has_transport_streak(work.segment_id),
        "the backfill server starts without the fill server's run"
    );
    let segment_id = work.segment_id;
    pipeline
        .deliver_payload(work, 1, "payload.bin", payload)
        .await;

    assert_eq!(
        pipeline
            .metrics()
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(pipeline.job(job_id).failed_bytes, 0);
    assert!(pipeline.segment_outcome(segment_id).is_none());
    assert!(pipeline.ask(0, 1).is_empty());
    assert!(pipeline.ask(1, 1).is_empty());
    pipeline.check_and_pump(job_id).await;
    assert_eq!(pipeline.status(job_id), Some(JobStatus::Complete));
}
