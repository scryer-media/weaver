// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use weaver_server_composition::{JobId, JobStatus};
use weaver_server_pipeline::DownloadWork;
use weaver_server_pipeline::pipeline::tests::dispatch_fixture::scheduler_lanes::{
    Handout, SaturationWake, YieldReason,
};
use weaver_server_pipeline::pipeline::tests::{
    dispatch_fixture::DispatchFixture, segmented_job_spec,
};
const SERVER_A: usize = 0;
const SERVER_B: usize = 1;

// The job every article in a handout came from, proving a handout never
// spans jobs.
fn single_job(works: &[DownloadWork]) -> JobId {
    let job_id = works[0].segment_id.file_id.job_id;
    assert!(
        works
            .iter()
            .all(|work| work.segment_id.file_id.job_id == job_id),
        "a handout must never mix jobs"
    );
    job_id
}

// Articles handed out per job over `calls` asks on one server.
fn article_counts(
    pipeline: &mut DispatchFixture,
    server_idx: usize,
    want: usize,
    calls: usize,
) -> HashMap<JobId, usize> {
    let mut counts: HashMap<JobId, usize> = HashMap::new();
    for _ in 0..calls {
        let works = pipeline.ask_with_spill(server_idx, want, None);
        if works.is_empty() {
            continue;
        }
        *counts.entry(single_job(&works)).or_default() += works.len();
    }
    counts
}

fn queued(pipeline: &DispatchFixture, job_id: JobId) -> usize {
    pipeline
        .job(job_id)
        .articles
        .active()
        .unwrap()
        .download_queue
        .len()
}

// A job of `segments` equal-sized articles, appended to the dispatch order.
async fn add_job(pipeline: &mut DispatchFixture, job_id: JobId, name: &str, segments: usize) {
    let spec = segmented_job_spec(name, "payload.bin", &vec![512u32; segments]);
    pipeline.add_job(job_id, spec).await;
}

// [`add_job`] with a submitted dispatch priority.
async fn add_job_with_priority(
    pipeline: &mut DispatchFixture,
    job_id: JobId,
    name: &str,
    segments: usize,
    priority: &str,
) {
    let mut spec = segmented_job_spec(name, "payload.bin", &vec![512u32; segments]);
    spec.metadata = vec![("priority".to_string(), priority.to_string())];
    pipeline.add_job(job_id, spec).await;
}

fn handouts_hot(pipeline: &DispatchFixture) -> u64 {
    pipeline
        .metrics()
        .download_scheduler_handouts_total_hot
        .load(Ordering::Relaxed)
}

fn handouts_spill(pipeline: &DispatchFixture) -> u64 {
    pipeline
        .metrics()
        .download_scheduler_handouts_total_spill
        .load(Ordering::Relaxed)
}

fn idle_with_servable(pipeline: &DispatchFixture) -> u64 {
    pipeline
        .metrics()
        .download_scheduler_idle_with_servable_total
        .load(Ordering::Relaxed)
}

// Rule 2: while the hot job can serve this server, nothing else is touched.
#[tokio::test]
async fn hot_job_takes_every_handout_while_it_can_serve_the_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let hot = JobId(71001);
    let next = JobId(71002);
    let third = JobId(71003);
    add_job(&mut pipeline, hot, "Harbour Lights Reel", 800).await;
    add_job(&mut pipeline, next, "Tin Roof Sessions", 800).await;
    add_job(&mut pipeline, third, "Copper Valley Notes", 800).await;

    let counts = article_counts(&mut pipeline, SERVER_A, 8, 100);

    assert_eq!(counts.get(&hot).copied(), Some(800));
    assert_eq!(counts.get(&next).copied(), None);
    assert_eq!(counts.get(&third).copied(), None);
    assert_eq!(handouts_spill(&pipeline), 0);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

// Rule 1: the link is never left idle. Once the hot job runs out the calls
// keep being answered, from the next job in order, and no third job opens.
#[tokio::test]
async fn the_next_job_takes_over_the_moment_the_hot_job_runs_dry() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let hot = JobId(71011);
    let next = JobId(71012);
    let third = JobId(71013);
    add_job(&mut pipeline, hot, "Short Straw Cut", 20).await;
    add_job(&mut pipeline, next, "Long Meadow Cut", 800).await;
    add_job(&mut pipeline, third, "Quiet Siding Cut", 800).await;

    let counts = article_counts(&mut pipeline, SERVER_A, 8, 100);

    assert_eq!(counts.get(&hot).copied(), Some(20));
    // 100 asks, three of which the short job answered: everything else came
    // from the one job behind it, and nothing was refused in between.
    assert_eq!(counts.get(&next).copied(), Some(97 * 8));
    assert_eq!(counts.get(&third).copied(), None);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

// Rule 3: the restart checkpoint is one of the per-job blocks, and a blocked
// hot job hands the server to the next job only — not to the one after it.
#[tokio::test]
async fn a_checkpoint_held_hot_job_passes_the_server_to_the_next_job_only() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let hot = JobId(71021);
    let next = JobId(71022);
    let third = JobId(71023);
    add_job(&mut pipeline, hot, "Held Ledger", 200).await;
    add_job(&mut pipeline, next, "Open Ledger", 200).await;
    add_job(&mut pipeline, third, "Spare Ledger", 200).await;
    pipeline.block_on_checkpoint(hot);

    let counts = article_counts(&mut pipeline, SERVER_A, 8, 20);

    assert_eq!(counts.get(&hot).copied(), None);
    assert_eq!(counts.get(&next).copied(), Some(160));
    assert_eq!(counts.get(&third).copied(), None);
    assert_eq!(queued(&pipeline, hot), 200);
    assert!(
        pipeline.checkpoint_retry_pending(hot),
        "a checkpoint-only refusal must leave the recheck note the queue owes"
    );
    assert_eq!(idle_with_servable(&pipeline), 0);
}

// Rule 1 outranks "the next job only": the walk keeps going past a second
// blocked job. A spill job with articles out on the server then keeps it
// until that ring drains; only then does the earlier job take over.
#[tokio::test]
async fn the_walk_passes_two_blocked_jobs_and_a_spill_in_flight_keeps_its_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let hot = JobId(71031);
    let next = JobId(71032);
    let third = JobId(71033);
    let fourth = JobId(71034);
    add_job(&mut pipeline, hot, "First Ledger", 200).await;
    add_job(&mut pipeline, next, "Second Ledger", 200).await;
    add_job(&mut pipeline, third, "Third Ledger", 200).await;
    add_job(&mut pipeline, fourth, "Fourth Ledger", 200).await;
    pipeline.block_on_checkpoint(hot);
    pipeline.block_on_checkpoint(next);

    let counts = article_counts(&mut pipeline, SERVER_A, 8, 10);
    assert_eq!(counts.get(&third).copied(), Some(80));
    assert_eq!(counts.get(&fourth).copied(), None);
    assert_eq!(counts.get(&hot).copied(), None);
    assert_eq!(counts.get(&next).copied(), None);

    // The second job opens while the third still has work out on this
    // server: the third keeps the server until its ring drains, so no second
    // spill job opens beside it.
    pipeline.unblock_checkpoint(next);
    let third_queued = queued(&pipeline, third);
    let works = pipeline.ask_with_spill(SERVER_A, 8, Some(third));
    assert_eq!(single_job(&works), third);
    assert_eq!(queued(&pipeline, third), third_queued - 8);

    // With nothing of the third's in flight, the earlier job takes over.
    let works = pipeline.ask_with_spill(SERVER_A, 8, None);
    assert_eq!(single_job(&works), next);
    assert_eq!(queued(&pipeline, third), third_queued - 8);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

// Rule 3: blocked is per server. The same call answers differently on two
// servers of the same pool in the same actor state.
#[tokio::test]
async fn a_retention_excluded_hot_job_is_blocked_on_that_server_alone() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let hot = JobId(71041);
    let next = JobId(71042);
    add_job(&mut pipeline, hot, "Deep Archive Reel", 200).await;
    add_job(&mut pipeline, next, "Fresh Post Reel", 200).await;
    pipeline.cache_retention_excludes(hot, weaver_nntp::ServerSet::from([SERVER_B]));

    let on_b = pipeline.ask_with_spill(SERVER_B, 4, None);
    assert_eq!(single_job(&on_b), next);

    let on_a = pipeline.ask_with_spill(SERVER_A, 4, None);
    assert_eq!(single_job(&on_a), hot);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

// Rule 5: a short hot job is a short handout, never a handout topped up from
// the job behind it.
#[tokio::test]
async fn a_handout_stops_at_the_hot_jobs_last_article() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let hot = JobId(71051);
    let next = JobId(71052);
    add_job(&mut pipeline, hot, "Three Frame Loop", 3).await;
    add_job(&mut pipeline, next, "Full Frame Loop", 200).await;

    let works = pipeline.ask_with_spill(SERVER_A, 8, None);

    assert_eq!(works.len(), 3);
    assert_eq!(single_job(&works), hot);
    assert_eq!(queued(&pipeline, next), 200);
    assert_eq!(handouts_hot(&pipeline), 1);
    assert_eq!(handouts_spill(&pipeline), 0);
}

// Rule 6: the completion-critical class orders work inside a job and only
// inside it. Another job's promoted recovery does not pre-empt the hot job's
// ordinary payload, and the hot job's own critical heap leads its ordinary
// one.
#[tokio::test]
async fn completion_critical_work_orders_a_job_but_never_outranks_the_hot_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let hot = JobId(71061);
    let filler = [JobId(71062), JobId(71063), JobId(71064)];
    let promoted = JobId(71065);
    add_job(&mut pipeline, hot, "Mixed Class Reel", 40).await;
    for (index, job_id) in filler.iter().enumerate() {
        add_job(&mut pipeline, *job_id, &format!("Filler Reel {index}"), 40).await;
    }
    add_job(&mut pipeline, promoted, "Repair Blocks Reel", 40).await;

    // The fifth job is entirely promoted recovery: the most urgent class the
    // queue has, on the job furthest from the front.
    pipeline.rewrite_queue(promoted, |work| {
        work.completion_critical = true;
        work.is_recovery = true;
    });
    // The hot job carries a few critical articles of its own behind ordinary
    // payload that was queued first.
    let mut remaining_to_promote = 4;
    pipeline.rewrite_queue(hot, |work| {
        if work.segment_id.segment_number >= 36 && remaining_to_promote > 0 {
            work.completion_critical = true;
            remaining_to_promote -= 1;
        }
    });

    let works = pipeline.ask_with_spill(SERVER_A, 6, None);

    assert_eq!(single_job(&works), hot);
    assert!(
        works.iter().take(4).all(|work| work.completion_critical),
        "the hot job's own critical heap leads its ordinary heap"
    );
    assert!(
        works.iter().skip(4).all(|work| !work.completion_critical),
        "and the ordinary heap follows in the same handout"
    );
    assert_eq!(
        queued(&pipeline, promoted),
        40,
        "no other job's critical work pre-empts the hot job's ordinary work"
    );
}

// Rule 6: soft byte pressure clamps the handout to a single article and
// changes nothing else. The hot job still leads, and a blocked hot job still
// passes the server down the walk.
#[tokio::test]
async fn soft_pressure_clamps_to_one_article_and_still_spills() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let hot = JobId(71071);
    let next = JobId(71072);
    add_job(&mut pipeline, hot, "Narrow Window Reel", 200).await;
    add_job(&mut pipeline, next, "Wide Window Reel", 200).await;
    pipeline.set_soft_write_pressure();

    let works = pipeline.ask_with_spill(SERVER_A, 8, None);
    assert_eq!(works.len(), 1);
    assert_eq!(single_job(&works), hot);

    // The hot job blocked on this server passes it to the next job, still
    // one article at a time.
    pipeline.block_on_checkpoint(hot);
    let works = pipeline.ask_with_spill(SERVER_A, 8, None);
    assert_eq!(works.len(), 1);
    assert_eq!(single_job(&works), next);
    assert_eq!(queued(&pipeline, next), 199);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

// The wedge of issue 97: the hot job's own buffered bytes hold the link at
// soft pressure while a clause refuses it on every server, and the bytes
// wait on articles that clause holds back. Nothing else may idle: the next
// job takes the server, one article at a time, so the link keeps carrying
// and the backlog has something running to drain it.
#[tokio::test]
async fn soft_pressure_raised_by_a_hot_job_blocked_everywhere_does_not_idle_the_link() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let hot = JobId(71097);
    add_job(&mut pipeline, hot, "Buffered Reel", 40).await;
    for index in 1..6 {
        add_job(&mut pipeline, JobId(hot.0 + index), "Waiting Reel", 40).await;
    }
    // The hot job's remaining articles are refused on both servers.
    pipeline.rewrite_queue(hot, |work| {
        work.exclude_servers = weaver_nntp::ServerSet::from([SERVER_A, SERVER_B]);
    });
    // Its reorder buffers alone put the write backlog at the soft mark.
    pipeline.set_soft_write_pressure();
    assert!(pipeline.has_soft_pressure());

    for server in [SERVER_A, SERVER_B] {
        let works = pipeline.ask_with_spill(server, 8, None);
        assert_eq!(works.len(), 1, "server {server} was sent away empty");
        assert_eq!(single_job(&works), JobId(hot.0 + 1));
    }
    assert_eq!(queued(&pipeline, hot), 40);
    assert_eq!(queued(&pipeline, JobId(hot.0 + 1)), 38);
    assert_eq!(handouts_hot(&pipeline), 0);
    assert_eq!(handouts_spill(&pipeline), 2);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

// Rule 3: the work's own exclusions and rotation hint are per server, and a
// job whose whole queue refuses this server is blocked on it.
#[tokio::test]
async fn per_article_exclusions_decide_which_server_a_job_is_blocked_on() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let hot = JobId(71091);
    let next = JobId(71092);
    add_job(&mut pipeline, hot, "Failed Over Reel", 40).await;
    add_job(&mut pipeline, next, "Untouched Reel", 40).await;
    // Both servers are up in this dispatch attempt, so a rotated article
    // has somewhere else to go.
    pipeline.set_fill_servers(Some(weaver_nntp::ServerSet::from([SERVER_A, SERVER_B])));
    // Half the hot job's articles failed on this server, the other half were
    // merely rotated away from it: neither may come back to it.
    pipeline.rewrite_queue(hot, |work| {
        if work.segment_id.segment_number % 2 == 0 {
            work.exclude_servers = weaver_nntp::ServerSet::from([SERVER_A]);
        } else {
            work.avoid_server = Some(SERVER_A);
        }
    });

    let on_a = pipeline.ask_with_spill(SERVER_A, 8, None);
    assert_eq!(
        single_job(&on_a),
        next,
        "a job whose every queued article refuses this server is blocked on it"
    );
    assert_eq!(queued(&pipeline, hot), 40);

    let on_b = pipeline.ask_with_spill(SERVER_B, 8, None);
    assert_eq!(single_job(&on_b), hot);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

// The rotation hint is a preference: with the alternative out of this
// dispatch attempt, the hinted server serves the article after all.
#[tokio::test]
async fn a_rotated_article_returns_to_its_server_while_the_alternative_is_down() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job_id = JobId(71093);
    add_job(&mut pipeline, job_id, "Rotated Reel", 4).await;
    pipeline.rewrite_queue(job_id, |work| {
        work.avoid_server = Some(SERVER_A);
    });
    pipeline.set_fill_servers(Some(weaver_nntp::ServerSet::from([SERVER_A])));

    let on_a = pipeline.ask_with_spill(SERVER_A, 8, None);
    assert_eq!(on_a.len(), 4);
    assert_eq!(single_job(&on_a), job_id);
    assert_eq!(queued(&pipeline, job_id), 0);
}

// With the alternative in this dispatch attempt, the hinted server is
// refused and the alternative takes the article.
#[tokio::test]
async fn a_rotated_article_goes_to_the_alternative_while_it_is_up() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job_id = JobId(71094);
    add_job(&mut pipeline, job_id, "Rotated Reel", 4).await;
    pipeline.rewrite_queue(job_id, |work| {
        work.avoid_server = Some(SERVER_A);
    });
    pipeline.set_fill_servers(Some(weaver_nntp::ServerSet::from([SERVER_A, SERVER_B])));

    assert!(pipeline.ask_with_spill(SERVER_A, 8, None).is_empty());
    assert_eq!(queued(&pipeline, job_id), 4);

    let on_b = pipeline.ask_with_spill(SERVER_B, 8, None);
    assert_eq!(on_b.len(), 4);
    assert_eq!(single_job(&on_b), job_id);
    assert_eq!(queued(&pipeline, job_id), 0);
}

// A fill server with every lane busy cannot take a hinted article this pass,
// so it is no alternative: the hinted server serves the article now instead
// of idling for as long as the other server's current body runs.
#[tokio::test]
async fn a_rotated_article_stays_on_its_server_while_the_alternative_has_no_idle_lane() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job_id = JobId(71095);
    add_job(&mut pipeline, job_id, "Rotated Reel", 4).await;
    pipeline.rewrite_queue(job_id, |work| {
        work.avoid_server = Some(SERVER_A);
    });
    let ranked = [SERVER_A, SERVER_B];
    let backfill = [false, false];
    // Only the hinted server has an idle lane this pass.
    let idle_by_server = std::collections::HashMap::from([(SERVER_A, 1)]);
    pipeline.set_fill_servers(DispatchFixture::rotation_alternatives_for_pass(
        Some(&ranked),
        &backfill,
        &idle_by_server,
    ));

    let on_a = pipeline.ask_with_spill(SERVER_A, 8, None);
    assert_eq!(on_a.len(), 4);
    assert_eq!(single_job(&on_a), job_id);
    assert_eq!(queued(&pipeline, job_id), 0);
}

// With an idle lane on the alternative, the hint still rotates the article.
#[tokio::test]
async fn a_rotated_article_goes_to_an_alternative_with_an_idle_lane() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job_id = JobId(71096);
    add_job(&mut pipeline, job_id, "Rotated Reel", 4).await;
    pipeline.rewrite_queue(job_id, |work| {
        work.avoid_server = Some(SERVER_A);
    });
    let ranked = [SERVER_A, SERVER_B];
    let backfill = [false, false];
    let idle_by_server = std::collections::HashMap::from([(SERVER_A, 1), (SERVER_B, 1)]);
    pipeline.set_fill_servers(DispatchFixture::rotation_alternatives_for_pass(
        Some(&ranked),
        &backfill,
        &idle_by_server,
    ));

    assert!(pipeline.ask_with_spill(SERVER_A, 8, None).is_empty());
    assert_eq!(queued(&pipeline, job_id), 4);
    let on_b = pipeline.ask_with_spill(SERVER_B, 8, None);
    assert_eq!(on_b.len(), 4);
    assert_eq!(single_job(&on_b), job_id);
}

// A pass that never ranked knows no alternatives, whatever lanes are idle.
#[test]
fn an_unranked_pass_knows_no_rotation_alternatives() {
    let idle_by_server = std::collections::HashMap::from([(SERVER_B, 1)]);
    assert_eq!(
        DispatchFixture::rotation_alternatives_for_pass(None, &[false, false], &idle_by_server),
        None
    );
}

// Rule 2: the hot job is recomputed on every call, so a higher-priority job
// submitted later takes the link on the next ask.
#[tokio::test]
async fn a_higher_priority_job_becomes_hot_on_the_next_call() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let first = JobId(71101);
    let urgent = JobId(71102);
    add_job(&mut pipeline, first, "Steady Reel", 200).await;

    let before = pipeline.ask_with_spill(SERVER_A, 4, None);
    assert_eq!(single_job(&before), first);

    add_job_with_priority(&mut pipeline, urgent, "Jumped Queue Reel", 200, "high").await;

    let after = pipeline.ask_with_spill(SERVER_A, 4, None);
    assert_eq!(single_job(&after), urgent);
    assert_eq!(handouts_spill(&pipeline), 0);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

// A small deterministic sequence, so a shuffled run reproduces exactly.
struct Lcg(u64);

impl Lcg {
    fn next_u32(&mut self) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) as u32
    }

    fn below(&mut self, bound: u32) -> u32 {
        self.next_u32() % bound
    }
}

// The guard counters: over a shuffled multi-job, multi-server run the
// scheduler never answers "nothing to do" while a job could have been
// served, and every handout it made is accounted for as hot or spill.
#[tokio::test]
async fn the_guard_counters_account_for_a_shuffled_multi_server_run() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let jobs = [
        JobId(71111),
        JobId(71112),
        JobId(71113),
        JobId(71114),
        JobId(71115),
    ];
    for (index, job_id) in jobs.iter().enumerate() {
        add_job(
            &mut pipeline,
            *job_id,
            &format!("Shuffled Reel {index}"),
            120,
        )
        .await;
    }

    let mut rng = Lcg(0x5eed_1234_9abc_def0);
    let mut answers = 0u64;
    for step in 0..400 {
        // Blocks and exclusions come and go under the scheduler, exactly as
        // they do while a run is live.
        let victim = jobs[rng.below(jobs.len() as u32) as usize];
        match rng.below(4) {
            0 => {
                pipeline.try_block_on_checkpoint(victim);
            }
            1 => pipeline.unblock_checkpoint(victim),
            2 => {
                let server = rng.below(2) as usize;
                pipeline.rewrite_queue(victim, |work| {
                    work.exclude_servers = weaver_nntp::ServerSet::from([server]);
                });
            }
            _ => pipeline.rewrite_queue(victim, |work| {
                work.exclude_servers.clear();
                work.avoid_server = None;
            }),
        }

        let server_idx = rng.below(2) as usize;
        let want = 1 + rng.below(8) as usize;
        let spill_in_flight = (step % 3 == 0).then(|| jobs[rng.below(jobs.len() as u32) as usize]);
        let works = pipeline.ask_with_spill(server_idx, want, spill_in_flight);
        if works.is_empty() {
            continue;
        }
        single_job(&works);
        assert!(works.len() <= want);
        answers += 1;
    }

    assert!(answers > 0, "the run must actually have handed work out");
    assert_eq!(
        idle_with_servable(&pipeline),
        0,
        "the scheduler never left a server idle while a job could serve it"
    );
    assert_eq!(handouts_hot(&pipeline) + handouts_spill(&pipeline), answers);
    assert_eq!(
        pipeline
            .metrics()
            .download_scheduler_handouts_total_probe
            .load(Ordering::Relaxed),
        0
    );
}

// The articles of a handout that must not be a yield.
fn taken(handout: Handout) -> Vec<DownloadWork> {
    match handout {
        Handout::Works(works) => works,
        Handout::Idle => Vec::new(),
        Handout::Yield(reason) => panic!("a whole-link gate fired unexpectedly: {reason:?}"),
        Handout::Saturated(wake) => {
            panic!("a caller with no lane share was reported saturated: {wake:?}")
        }
    }
}

#[tokio::test(start_paused = true)]
async fn only_a_failed_pause_or_server_disable_holds_admission() {
    use weaver_server_orchestration::bandwidth::schedule::{
        ScheduleServices, spawn_evaluator_with_services,
    };
    use weaver_server_pipeline::bandwidth::{ScheduleAction, ScheduleEntry};
    use weaver_server_pipeline::jobs::handle::DownloadBlockKind;
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    add_job(&mut pipeline, JobId(71080), "Schedule Gate", 200).await;
    let (events, _) = tokio::sync::broadcast::channel(1);
    let handle =
        weaver_server_pipeline::SchedulerHandle::new(events, pipeline.shared_state().clone());
    let rule = |active| ScheduleEntry {
        id: "server-rule".into(),
        enabled: true,
        label: "Backup server".into(),
        days: vec![],
        time: "00:00".into(),
        times: vec![],
        every_hour_at_minute: None,
        action: ScheduleAction::SetServerActive {
            server_id: 42,
            active,
        },
    };
    // No servers service is wired, so every server rule fails to apply.
    let schedules = Arc::new(tokio::sync::RwLock::new(vec![rule(true)]));
    let (task, ready) = spawn_evaluator_with_services(
        handle.clone(),
        schedules.clone(),
        ScheduleServices::default(),
    );

    // A server that failed to come online leaves the others downloading.
    assert!(ready.await.unwrap().is_err());
    assert!(!pipeline.schedule_replay_paused());
    assert_eq!(handle.get_download_block().kind, DownloadBlockKind::None);
    assert!(!taken(pipeline.ask_handout(SERVER_A, 8, None)).is_empty());

    // A server that failed to go offline holds new downloads, and says why.
    *schedules.write().await = vec![rule(false)];
    tokio::time::advance(weaver_server_base::e2e_clock::schedule_poll_interval()).await;
    while !pipeline.schedule_replay_paused() {
        tokio::task::yield_now().await;
    }
    let block = handle.get_download_block();
    assert_eq!(block.kind, DownloadBlockKind::Scheduled);
    assert!(
        block
            .schedule_hold_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("Backup server")),
        "{block:?}"
    );
    assert!(!pipeline.shared_state().is_paused());
    assert!(!pipeline.shared_state().is_paused());
    assert!(matches!(
        pipeline.ask_handout(SERVER_A, 8, None),
        Handout::Yield(YieldReason::Paused)
    ));

    // Removing the rule releases the hold.
    schedules.write().await.clear();
    tokio::time::advance(weaver_server_base::e2e_clock::schedule_poll_interval()).await;
    while pipeline.schedule_replay_paused() {
        tokio::task::yield_now().await;
    }
    let block = handle.get_download_block();
    assert_eq!(block.kind, DownloadBlockKind::None);
    assert_eq!(block.schedule_hold_reason, None);
    assert!(!taken(pipeline.ask_handout(SERVER_A, 8, None)).is_empty());
    task.shutdown().await;
}

// The whole-link gates, each reported as itself. These are the only reasons
// a slot may be left empty while a job has servable work.
#[tokio::test]
async fn every_whole_link_gate_yields_under_its_own_name() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let hot = JobId(71081);
    add_job(&mut pipeline, hot, "Gated Reel", 200).await;

    pipeline
        .shared_state()
        .hold(weaver_server_pipeline::jobs::Hold::manual(
            weaver_server_pipeline::jobs::HoldOwner::All,
        ));
    assert!(matches!(
        pipeline.ask_handout(SERVER_A, 8, None),
        Handout::Yield(YieldReason::Paused)
    ));
    pipeline.shared_state().release(
        weaver_server_pipeline::jobs::HoldOrigin::Manual,
        weaver_server_pipeline::jobs::HoldOwner::All,
    );

    pipeline.set_handoff_draining(true);
    assert!(matches!(
        pipeline.ask_handout(SERVER_A, 8, None),
        Handout::Yield(YieldReason::HandoffDraining)
    ));
    pipeline.set_handoff_draining(false);

    pipeline
        .metrics()
        .write_buffered_bytes
        .store(u64::MAX, Ordering::Relaxed);
    assert!(matches!(
        pipeline.ask_handout(SERVER_A, 8, None),
        Handout::Yield(YieldReason::HardPressure)
    ));
    pipeline
        .metrics()
        .write_buffered_bytes
        .store(0, Ordering::Relaxed);

    // Nothing was taken from the queue while the gates were shut.
    assert_eq!(queued(&pipeline, hot), 200);
    assert_eq!(handouts_hot(&pipeline), 0);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

// Rule 7: a lane holds its share of a job. One hundred connections asking
// for a runway each would reserve a six-hundred-article job among the first
// twenty; the share holds each of them to the job divided over the link.
#[tokio::test]
async fn a_small_job_is_shared_over_every_connection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    pipeline.set_connection_limit(100);
    let hot = JobId(71301);
    let next = JobId(71302);
    add_job(&mut pipeline, hot, "Sixty Second Reel", 600).await;
    add_job(&mut pipeline, next, "Sixty Second Reel Two", 600).await;

    assert_eq!(
        pipeline.lane_share_of_job(hot, 8),
        9,
        "600 / 100 rounds up to 6, floored at depth + 1"
    );
    let lane = pipeline.lane_holding(SERVER_A, hot, 0, 8);
    let works = taken(pipeline.ask_handout(SERVER_A, 18, Some(lane)));
    assert_eq!(works.len(), 9, "a runway ask is held to the share");
    assert_eq!(single_job(&works), hot);

    // Sixty fresh lanes at that share leave the hot job with work for the
    // rest; none of them is sent on to the next job.
    for _ in 0..60 {
        let lane = pipeline.lane_holding(SERVER_A, hot, 0, 8);
        let works = taken(pipeline.ask_handout(SERVER_A, 18, Some(lane)));
        assert_eq!(single_job(&works), hot);
    }
    assert!(
        queued(&pipeline, hot) > 0,
        "the hot job is not reserved whole"
    );
    assert_eq!(queued(&pipeline, next), 600, "the next job is untouched");
    assert_eq!(handouts_spill(&pipeline), 0);

    // The lanes after those empty the hot job's queue: every article of it
    // is now out on a lane. The next job becomes the hot one, but a lane
    // still full of the first is not sent on to it — it waits for its own
    // pipe to drain, and the next job stays untouched.
    let mut lanes = Vec::new();
    for _ in 0..40 {
        if queued(&pipeline, hot) == 0 {
            break;
        }
        let lane = pipeline.lane_holding(SERVER_A, hot, 0, 8);
        let works = taken(pipeline.ask_handout(SERVER_A, 18, Some(lane)));
        assert_eq!(single_job(&works), hot);
        lanes.push((lane, works));
    }
    assert_eq!(queued(&pipeline, hot), 0, "the hot job has left the queue");
    // The last lane took the remainder; the first took a full share.
    let (lane, works) = lanes.swap_remove(0);
    assert_eq!(works.len(), 9);
    let held = pipeline.lane_holding(SERVER_A, hot, works.len(), lane.depth);
    match pipeline.ask_handout(SERVER_A, 18, Some(held)) {
        Handout::Saturated(wake) => assert_eq!(
            wake,
            SaturationWake {
                of: None,
                below: 9,
                room: false
            }
        ),
        other => panic!(
            "a full lane does not open the next job, got {:?}",
            taken(other).len()
        ),
    }
    assert_eq!(
        queued(&pipeline, next),
        600,
        "the next job is still untouched"
    );
    assert_eq!(handouts_spill(&pipeline), 0);

    // Below a full pipe it opens the next job, with a share of that job.
    let draining = pipeline.lane_holding(SERVER_A, hot, 8, 8);
    let works = taken(pipeline.ask_handout(SERVER_A, 18, Some(draining)));
    assert_eq!(single_job(&works), next);
    assert_eq!(works.len(), 9);
}

// A lane already at its share is reported busy, not sent on to the next
// job — the link must not open a second job while the hot one still has
// work for other lanes.
#[tokio::test]
async fn a_lane_at_its_share_is_saturated_rather_than_spilled() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    pipeline.set_connection_limit(100);
    let hot = JobId(71311);
    let next = JobId(71312);
    add_job(&mut pipeline, hot, "Held Share Reel", 600).await;
    add_job(&mut pipeline, next, "Held Share Reel Two", 600).await;

    let share = pipeline.lane_share_of_job(hot, 8);
    let full = pipeline.lane_holding(SERVER_A, hot, share, 8);
    match pipeline.ask_handout(SERVER_A, 18, Some(full)) {
        Handout::Saturated(wake) => assert_eq!(
            wake,
            SaturationWake {
                of: Some(hot),
                below: share,
                room: false,
            }
        ),
        other => panic!(
            "a lane at its share must be saturated, got {:?}",
            taken(other).len()
        ),
    }
    assert_eq!(queued(&pipeline, hot), 600);
    assert_eq!(queued(&pipeline, next), 600);
    assert_eq!(handouts_spill(&pipeline), 0);

    // Holding fewer than the share opens the difference.
    let short = pipeline.lane_holding(SERVER_A, hot, share - 2, 8);
    let works = taken(pipeline.ask_handout(SERVER_A, 18, Some(short)));
    assert_eq!(works.len(), 2);
    assert_eq!(single_job(&works), hot);

    // Articles of another job do not count against this one's share: a
    // lane carrying a tail of the next job, below a full pipe, is served the
    // whole share of the hot one.
    let elsewhere = pipeline.lane_holding(SERVER_A, next, 8, 8);
    let works = taken(pipeline.ask_handout(SERVER_A, 18, Some(elsewhere)));
    assert_eq!(works.len(), share);
    assert_eq!(single_job(&works), hot);

    // A lane with a full pipe of another job holds nothing of the hot one
    // and is not opened to it: it waits on its own pipe, not on any share.
    let full_elsewhere = pipeline.lane_holding(SERVER_A, next, 17, 8);
    match pipeline.ask_handout(SERVER_A, 18, Some(full_elsewhere)) {
        Handout::Saturated(wake) => assert_eq!(
            wake,
            SaturationWake {
                of: None,
                below: 9,
                room: false
            }
        ),
        other => panic!(
            "a full lane does not open another job, got {:?}",
            taken(other).len()
        ),
    }
}

// The share is measured against the job's unfetched articles, queued and
// out on lanes alike, so it is the same for the last lane to ask as for the
// first instead of shrinking as the queue drains.
#[tokio::test]
async fn the_share_counts_articles_already_out_on_lanes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    pipeline.set_connection_limit(100);
    let hot = JobId(71341);
    add_job(&mut pipeline, hot, "Half Fetched Pack", 10_000).await;

    assert_eq!(pipeline.lane_share_of_job(hot, 8), 100);
    pipeline.set_in_flight_articles(hot, 10_000);
    assert_eq!(
        pipeline.lane_share_of_job(hot, 8),
        200,
        "articles out on lanes are still the job's to spread"
    );
}

// A large job's share is beyond any runway, so its lanes are served exactly
// as before: the full ask, every time.
#[tokio::test]
async fn a_large_job_hands_out_the_whole_runway() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    pipeline.set_connection_limit(100);
    let hot = JobId(71321);
    add_job(&mut pipeline, hot, "Long Haul Pack", 20_000).await;

    assert_eq!(pipeline.lane_share_of_job(hot, 8), 200);
    let lane = pipeline.lane_holding(SERVER_A, hot, 30, 8);
    let works = taken(pipeline.ask_handout(SERVER_A, 18, Some(lane)));
    assert_eq!(works.len(), 18);
}

// The share is measured against the job that would serve the lane. A lane at
// its share of a job it cannot fetch from — retention rules this server out
// — is not held on that job's account: the walk goes on, as rule 3 says.
#[tokio::test]
async fn saturation_is_only_charged_by_a_job_that_could_serve_the_lane() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    pipeline.set_connection_limit(100);
    let hot = JobId(71331);
    let next = JobId(71332);
    add_job(&mut pipeline, hot, "Elsewhere Only Reel", 600).await;
    add_job(&mut pipeline, next, "Here Too Reel", 600).await;
    pipeline.rewrite_queue(hot, |work| {
        work.exclude_servers = weaver_nntp::ServerSet::from([SERVER_A]);
    });

    let share = pipeline.lane_share_of_job(hot, 8);
    let lane = pipeline.lane_holding(SERVER_A, hot, share, 8);
    match pipeline.ask_handout(SERVER_A, 18, Some(lane)) {
        Handout::Saturated(wake) => assert_eq!(
            wake,
            SaturationWake {
                of: None,
                below: 9,
                room: false
            },
            "the walk went on to the next job; only the lane's full pipe holds it"
        ),
        other => panic!(
            "a full lane does not open the next job, got {:?}",
            taken(other).len()
        ),
    }
    assert_eq!(handouts_spill(&pipeline), 0);

    // With room in its pipe the lane is served from the next job outright:
    // the blocked hot job never charges it.
    let lane = pipeline.lane_holding(SERVER_A, hot, share - 1, 8);
    let works = taken(pipeline.ask_handout(SERVER_A, 18, Some(lane)));
    assert_eq!(
        single_job(&works),
        next,
        "the blocked hot job does not hold the lane"
    );
    assert_eq!(handouts_spill(&pipeline), 1);
}

// A lane's share is a floor of a full pipe plus one even for a job with a
// handful of articles, so a tiny job still fills a pipelined socket.
#[tokio::test]
async fn the_share_never_starves_a_pipe() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    pipeline.set_connection_limit(100);
    let hot = JobId(71341);
    add_job(&mut pipeline, hot, "Three Article Note", 3).await;

    assert_eq!(pipeline.lane_share_of_job(hot, 8), 9);
    assert_eq!(pipeline.lane_share_of_job(hot, 1), 2);
    let lane = pipeline.lane_holding(SERVER_A, hot, 0, 8);
    let works = taken(pipeline.ask_handout(SERVER_A, 18, Some(lane)));
    assert_eq!(
        works.len(),
        3,
        "everything the job has, well inside the floor"
    );
}

// What the lanes hold is reserved on the volume as it is leased, so a lane
// may lease only its part of the room the job's volume has left below its
// lease limit, over every connection, and never more than that room holds.
// On the slow disk run the lanes held 262 MB of a job's queue while its
// volume read near empty, and all of it arrived at once: 354.9 MB against
// 268.4 MB.
#[tokio::test]
async fn a_lane_leases_only_its_part_of_the_room_its_jobs_volume_has_left() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    pipeline.set_connection_limit(4);
    let hot = JobId(71351);
    add_job(&mut pipeline, hot, "Slow Disk Reel", 20_000).await;
    assert_eq!(pipeline.lane_share_of_job(hot, 8), 5_000);
    let volume = pipeline.volume(hot);
    let hard = 87_382;
    volume.set_buffer_limits(hard, hard);
    let limit = DispatchFixture::lease_limit(&volume);
    assert_eq!(limit, 81_921);
    assert_eq!(pipeline.volume_room(hot), Some(limit));
    // The volume counts so much that room for 10 articles of 512 bytes is
    // left on each of the 4 connections.
    let room_for = |articles: u64| limit - articles * 512;

    // A lane holding 30 is given the 10 more its part of the room takes,
    // though its ask and its share of the queue are larger, and they are
    // reserved as they are leased.
    volume.count_for_test(room_for(40));
    let lane = pipeline.lane_holding(SERVER_A, hot, 30, 8);
    let works = taken(pipeline.ask_handout(SERVER_A, 18, Some(lane)));
    assert_eq!(works.len(), 10);
    assert_eq!(single_job(&works), hot);
    pipeline.lease_to_lane(SERVER_A, lane, works);
    assert_eq!(volume.unwritten_bytes(), room_for(30));

    // Below the volume's resume line, a lane whose part of the room is
    // short of a whole pipe of 9, with room left for others, is answered
    // once it has fetched its pipe down to where one fits, never trickled.
    volume.count_for_test(volume.resume_line() - 6 * 512);
    let part = ((limit - volume.unwritten_bytes()) / 4 / 512) as usize;
    assert!(part + 1 < 9, "{part}");
    let full = pipeline.lane_holding(SERVER_A, hot, 40, 8);
    assert_eq!(
        saturation(pipeline.ask_handout(SERVER_A, 18, Some(full))),
        SaturationWake {
            of: Some(hot),
            below: 40 + part + 1 - 9,
            room: false,
        }
    );

    // The room itself holds less than a whole pipe: the lane takes what
    // fits, so the room is not left unused while the writer may run dry.
    volume.count_for_test(room_for(4));
    let short = pipeline.lane_holding(SERVER_A, hot, 2, 8);
    let works = taken(pipeline.ask_handout(SERVER_A, 18, Some(short)));
    assert_eq!(works.len(), 4, "what the room holds");
    pipeline.lease_to_lane(SERVER_A, short, works);
    assert_eq!(volume.unwritten_bytes(), limit);

    // No article fits: the lane waits for the volume's line, not for what
    // it holds, and is never handed one past the limit.
    volume.count_for_test(limit - 100);
    assert!(volume.unwritten_bytes() > volume.resume_line());
    let none_fits = pipeline.lane_holding(SERVER_A, hot, 2, 8);
    assert_eq!(
        saturation(pipeline.ask_handout(SERVER_A, 18, Some(none_fits))),
        SaturationWake {
            of: Some(hot),
            below: 0,
            room: true,
        }
    );
    assert!(
        pipeline.ask_room_wake(hot),
        "the volume holds a wake at its line"
    );
    assert_eq!(volume.unwritten_bytes(), limit - 100);
    assert!(volume.peak_bytes() <= hard);
}

fn saturation(handout: Handout) -> SaturationWake {
    match handout {
        Handout::Saturated(wake) => wake,
        other => panic!("the lane must be held, was handed {}", taken(other).len()),
    }
}

fn scan_skips(pipeline: &DispatchFixture) -> u64 {
    pipeline
        .metrics()
        .download_scheduler_scan_items_skipped_total
        .load(Ordering::Relaxed)
}

// A hot job the restart checkpoint holds whole is passed without reading its
// queue: the walk reaches the next job having looked at none of the held
// job's articles, and the recheck the checkpoint owes is still noted. A
// recovery article, which the checkpoint never holds, is still found.
#[tokio::test]
async fn a_checkpoint_held_hot_job_is_passed_without_reading_its_queue() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let hot = JobId(71201);
    let next = JobId(71202);
    add_job(&mut pipeline, hot, "Held Archive", 500).await;
    add_job(&mut pipeline, next, "Open Archive", 200).await;
    pipeline.block_on_checkpoint(hot);

    let works = taken(pipeline.ask_handout(SERVER_A, 8, None));
    assert_eq!(single_job(&works), next);
    assert_eq!(scan_skips(&pipeline), 0);
    assert!(
        pipeline.checkpoint_retry_pending(hot),
        "a whole-queue checkpoint refusal must leave the recheck note the queue owes"
    );

    pipeline.rewrite_queue(hot, |work| {
        work.is_recovery = work.segment_id.segment_number == 7;
    });
    let works = taken(pipeline.ask_handout(SERVER_A, 8, None));
    assert_eq!(single_job(&works), hot);
    assert_eq!(works.len(), 1);
    assert!(works[0].is_recovery);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

// A job waiting its turn holds nothing per article and is charged nothing:
// its files keep their article counts and sizes, and nothing else.
#[tokio::test]
async fn a_queued_job_is_charged_nothing_and_holds_no_article_list() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job = JobId(71211);
    let spec = pipeline.add_queued_job(job, 24).await;

    let state = pipeline.job(job);
    let weaver_server_pipeline::jobs::model::JobArticles::Queued(summaries) = &state.articles
    else {
        panic!("a job waiting its turn must hold no built articles");
    };
    assert_eq!(summaries.0.len(), 1);
    assert_eq!(summaries.0[0].articles, 24);
    assert_eq!(
        summaries.0[0].bytes,
        spec.files[0]
            .segments
            .iter()
            .map(|segment| u64::from(segment.bytes))
            .sum::<u64>()
    );
    assert!(state.spec.files.iter().all(|file| file.segments.is_empty()));
    assert_eq!(state.spec.files[0].filename, spec.files[0].filename);
    assert_eq!(state.articles.queued_download_len(), 24);
    assert!(!pipeline.has_repeated_articles(job));
    assert!(!pipeline.has_article_memory(job));
    assert_eq!(pipeline.reserved_memory(), 0);
}

// A job's articles are built the first time the scheduler reaches it, read
// back from its stored NZB, and charged then, past a full ledger. Until they
// arrive its turn passes to the next job.
#[tokio::test]
async fn a_queued_job_is_built_on_its_first_handout_and_charged() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job = JobId(71221);
    let next = JobId(71222);
    let spec = pipeline.add_queued_job(job, 16).await;
    // The job holds its place among the NZBs being downloaded.
    pipeline.persist_admitted(job);
    pipeline.admit(job);
    add_job(&mut pipeline, next, "Running Reel", 16).await;
    // A ledger with no room: what a job builds is charged past it.
    pipeline.set_memory_budget(0);

    // The first reach sends for the articles; the job's turn spills on.
    assert_eq!(
        single_job(&taken(pipeline.ask_handout(SERVER_A, 4, None))),
        next
    );
    assert!(matches!(
        pipeline.job(job).articles,
        weaver_server_pipeline::jobs::model::JobArticles::Activating(..)
    ));
    assert_eq!(pipeline.reserved_memory(), 0);

    pipeline.deliver_activation().await;
    let state = pipeline.job(job);
    let ordinals = |segments: &[weaver_server_composition::SegmentSpec]| -> Vec<(u32, u32)> {
        segments
            .iter()
            .map(|segment| (segment.ordinal, segment.bytes))
            .collect()
    };
    assert_eq!(
        ordinals(&state.spec.files[0].segments),
        ordinals(&spec.files[0].segments)
    );
    assert_eq!(state.articles.active().unwrap().download_queue.len(), 16);
    let charged = DispatchFixture::estimate_scheduling_memory(&spec);
    assert!(pipeline.has_article_memory(job));
    assert_eq!(pipeline.reserved_memory(), charged);

    let works = taken(pipeline.ask_handout(SERVER_A, 4, None));
    assert_eq!(single_job(&works), job);
    // Handing work out again charges nothing more.
    assert_eq!(pipeline.reserved_memory(), charged);
}

// A job's articles, its repeated-article index and their charge go when the
// job leaves the pipeline, as it ends or is cancelled.
#[tokio::test]
async fn a_built_job_drops_its_articles_and_charge_when_it_ends() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let ended = JobId(71231);
    let cancelled = JobId(71232);
    pipeline.add_queued_job(ended, 8).await;
    let cancelled_spec = pipeline.add_queued_job(cancelled, 8).await;
    for job in [ended, cancelled] {
        pipeline.request_job_activation(job);
        pipeline.deliver_activation().await;
        assert!(pipeline.job(job).articles.active().is_some());
    }
    assert!(pipeline.reserved_memory() > 0);

    pipeline.purge_terminal_runtime(ended);
    assert!(pipeline.job_if_present(ended).is_none());
    assert!(!pipeline.has_article_memory(ended));
    assert!(!pipeline.has_repeated_articles(ended));
    assert_eq!(
        pipeline.reserved_memory(),
        DispatchFixture::estimate_scheduling_memory(&cancelled_spec)
    );

    pipeline.cancel_job(cancelled).await.unwrap();
    assert!(pipeline.job_if_present(cancelled).is_none());
    assert!(!pipeline.has_article_memory(cancelled));
    assert_eq!(pipeline.reserved_memory(), 0);
}

// A read of a job's articles answers only the activation that sent it. A
// job cancelled while its read was out and added again under the same id
// is built from its own read, never from the one sent before the cancel.
#[tokio::test]
async fn a_reply_to_an_earlier_activation_of_the_same_id_is_dropped() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job = JobId(71251);
    pipeline.add_queued_job(job, 8).await;
    pipeline.request_job_activation(job);
    pipeline.cancel_job(job).await.unwrap();
    assert!(pipeline.job_if_present(job).is_none());
    pipeline.add_queued_job(job, 8).await;
    pipeline.request_job_activation(job);
    let weaver_server_pipeline::jobs::model::JobArticles::Activating(_, current) =
        pipeline.job(job).articles
    else {
        panic!("the job added again must be activating");
    };

    let mut replies = Vec::new();
    for _ in 0..2 {
        replies.push(pipeline.next_activation().await);
    }
    replies.sort_by_key(|reply| reply.generation() == current);
    let [earlier, own] = <[_; 2]>::try_from(replies).ok().unwrap();
    assert_ne!(earlier.generation(), current);
    assert_eq!(own.generation(), current);

    pipeline.handle_activation(earlier);
    assert!(matches!(
        pipeline.job(job).articles,
        weaver_server_pipeline::jobs::model::JobArticles::Activating(_, activation) if activation == current
    ));
    assert!(!pipeline.has_article_memory(job));

    pipeline.handle_activation(own);
    assert_eq!(
        pipeline
            .job(job)
            .articles
            .active()
            .expect("the job's own read builds it")
            .download_queue
            .len(),
        8
    );
}

// A job whose stored NZB cannot be read back ends Failed through the
// failure path, and is never left waiting.
#[tokio::test]
async fn a_job_whose_articles_cannot_be_read_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job = JobId(71241);
    pipeline.add_queued_job(job, 8).await;
    pipeline.delete_active_job(job);
    pipeline.request_job_activation(job);
    pipeline.deliver_activation().await;
    assert!(
        pipeline
            .job_if_present(job)
            .is_none_or(|state| matches!(state.status, JobStatus::Failed { .. }))
    );
    assert!(!pipeline.has_article_memory(job));
    assert_eq!(pipeline.reserved_memory(), 0);
}
