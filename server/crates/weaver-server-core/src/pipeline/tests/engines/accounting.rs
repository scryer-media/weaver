// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job's figures as the graph earned them: what came, what was lost,
// what recovery was fetched, read off the live report and the history row
// the job leaves.

use sevenz_turbo::encoder_options::Lzma2Options;
use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, par3_set, payload, run};
use super::rar::with_par2;
use super::restart::{Down, run_restarting_at};
use crate::JobHistoryRow;
use crate::jobs::JobPhase;
use crate::jobs::ids::JobId;
use crate::pipeline::health_milli;
use crate::pipeline::tests::direct_store::single_member_store_set;

// A plain file and a PAR2 set of two blocks over it: the file is post 0,
// the index post 1, the volume post 2.
fn covered(seed: u8) -> Vec<Post> {
    let data = payload(4 * ARTICLE, seed);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    posts
}

fn history(outcome: &Outcome) -> &JobHistoryRow {
    outcome
        .history
        .as_ref()
        .expect("the job left a history row")
}

// The recovery volume's posted size.
fn recovery_set(posts: &[Post]) -> u64 {
    posts
        .iter()
        .filter(|post| post.role.is_recovery())
        .map(|post| post.bytes.len() as u64)
        .sum()
}

// The recovery bytes the run asked for, each article once.
fn recovery_fetched(posts: &[Post], outcome: &Outcome) -> u64 {
    outcome
        .requests
        .keys()
        .filter(|(file, _)| posts[*file as usize].role.is_recovery())
        .map(|(file, ordinal)| {
            posts[*file as usize]
                .articles(ARTICLE)
                .nth(*ordinal as usize)
                .map_or(0, |article| article.len() as u64)
        })
        .sum()
}

// One article lost under PAR2: the job fetches recovery, repairs the slice
// and completes, and both the live report and the history row count the
// recovery it fetched, never more than the set.
#[tokio::test]
async fn a_repaired_loss_counts_the_recovery_it_fetched() {
    let scenario = Scenario::new(covered(71)).lost(0, 1);
    let posts = &scenario.posts;
    let set = recovery_set(posts);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    let row = history(&graph);
    assert_eq!(row.optional_recovery_bytes, set);
    assert!(
        row.optional_recovery_downloaded_bytes > 0 && row.optional_recovery_downloaded_bytes <= set,
        "{} of {set}",
        row.optional_recovery_downloaded_bytes
    );
    assert_eq!(
        row.optional_recovery_downloaded_bytes,
        recovery_fetched(posts, &graph)
    );
    let report = graph.graph().report.as_ref().expect("the job was reported");
    assert!(report.optional_recovery_downloaded_bytes > 0);
}

// Nothing lost: no recovery block is fetched. The volume's opening
// article still comes, as every posted file's does, to say what it is,
// and that is all that is counted.
#[tokio::test]
async fn a_clean_post_counts_only_the_recovery_heads_it_read() {
    let scenario = Scenario::new(covered(72));
    let posts = &scenario.posts;
    let set = recovery_set(posts);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    let row = history(&graph);
    assert_eq!(row.optional_recovery_bytes, set);
    let heads: Vec<_> = graph
        .requests
        .keys()
        .filter(|(file, _)| posts[*file as usize].role.is_recovery())
        .collect();
    assert_eq!(heads, [&(2, 0)]);
    assert_eq!(row.optional_recovery_downloaded_bytes, ARTICLE as u64);
}

// Two articles lost against a volume of two blocks: the whole volume is
// fetched, so the live report has no recovery volume left to fetch (the
// NZBGet facade's RemainingParCount), and the whole set is counted.
#[tokio::test]
async fn a_volume_fetched_whole_leaves_no_par_volume_remaining() {
    let scenario = Scenario::new(covered(73)).lost(0, 1).lost(0, 2);
    let set = recovery_set(&scenario.posts);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 2 })]
    );
    let report = graph.graph().report.as_ref().expect("the job was reported");
    assert_eq!(report.remaining_par_files, 0);
    assert_eq!(history(&graph).optional_recovery_downloaded_bytes, set);
}

// A clean post leaves its volume unfetched but for its head: one volume
// remains.
#[tokio::test]
async fn a_clean_post_leaves_its_par_volume_remaining() {
    let graph = run(&Scenario::new(covered(74))).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    let report = graph.graph().report.as_ref().expect("the job was reported");
    assert_eq!(report.remaining_par_files, 1);
}

// The bytes of the articles the run asked for and was served, and of the
// ones every server lacked, each article once.
fn served_and_lost(scenario: &Scenario, outcome: &Outcome) -> (u64, u64) {
    let size = |(file, ordinal): (u32, u32)| {
        scenario.posts[file as usize]
            .articles(scenario.article)
            .nth(ordinal as usize)
            .map_or(0, |article| article.len() as u64)
    };
    outcome
        .requests
        .keys()
        .fold((0, 0), |(served, lost), article| {
            if scenario.lost.contains(article) {
                (served, lost + size(*article))
            } else {
                (served + size(*article), lost)
            }
        })
}

// The history row's byte figures add up to what the run fetched. Its
// failed bytes are what the delivery still lacks: a loss repaired into a
// whole file fails nothing.
fn assert_accounted(scenario: &Scenario, outcome: &Outcome, failed: u64) {
    let row = history(outcome);
    let total: u64 = scenario
        .posts
        .iter()
        .map(|post| post.bytes.len() as u64)
        .sum();
    let (served, _) = served_and_lost(scenario, outcome);
    assert_eq!(row.total_bytes, total);
    assert_eq!(row.downloaded_bytes, served);
    assert_eq!(row.failed_bytes, failed);
    assert_eq!(row.health, health_milli(total, failed));
    assert_eq!(
        row.optional_recovery_downloaded_bytes,
        recovery_fetched(&scenario.posts, outcome)
    );
}

#[tokio::test]
async fn a_clean_post_accounts_every_byte() {
    let scenario = Scenario::new(covered(75));
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_accounted(&scenario, &graph, 0);
    assert_eq!(history(&graph).health, 1000);
}

#[tokio::test]
async fn a_repaired_post_accounts_every_byte() {
    let scenario = Scenario::new(covered(76)).lost(0, 2);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    let (_, lost) = served_and_lost(&scenario, &graph);
    assert_eq!(lost, ARTICLE as u64);
    assert_accounted(&scenario, &graph, 0);
}

// A stored RAR set beside PAR2, one volume article lost: repaired in the
// stream and extracted, every figure still adds up.
#[tokio::test]
async fn an_extracted_rar_set_accounts_every_byte() {
    let body = payload(7 * ARTICLE + 13, 77);
    let scenario =
        Scenario::new(with_par2(single_member_store_set("beacon.bin", &body, 2))).lost(0, 2);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(graph.files["lantern beacon/beacon.bin"], body);
    assert_accounted(&scenario, &graph, 0);
    assert!(history(&graph).optional_recovery_downloaded_bytes > 0);
}

// How often the run timed `stage`.
fn timed(outcome: &Outcome, stage: &str) -> u64 {
    outcome
        .stages_timed
        .iter()
        .find(|(name, _)| *name == stage)
        .map_or(0, |(_, count)| *count)
}

// A repaired post times its repair once, however often the root went back
// to fetch recovery for it.
#[tokio::test]
async fn a_repaired_post_times_its_repair_stage_once() {
    let graph = run(&Scenario::new(covered(78)).lost(0, 1)).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(timed(&graph, "repair"), 1, "{:?}", graph.events);
}

// A clean post with recovery times its verification once.
#[tokio::test]
async fn a_clean_post_times_its_verify_stage() {
    let graph = run(&Scenario::new(covered(80))).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(timed(&graph, "verify"), 1, "{:?}", graph.events);
}

// A coded 7z read after it lands times its extraction once. A stored RAR
// set extracts as it streams in, inside the download, and has no stage of
// its own.
#[tokio::test]
async fn a_coded_7z_times_its_extract_stage() {
    let archive = super::sevenz::container(Lzma2Options::from_level(1).into(), None);
    let graph = run(&Scenario::new(vec![Post::plain("lantern.7z", archive)])).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(timed(&graph, "extract"), 1, "{:?}", graph.events);
}

// A covered file whose loss PAR2 repairs, beside an uncovered .nfo whose
// only article every server lacks: Complete, one slice repaired, and the
// .nfo's bytes stay failed. The job page calls those never recovered.
#[tokio::test]
async fn a_lost_nfo_beside_a_repair_stays_failed_on_a_complete_job() {
    let data = payload(4 * ARTICLE, 91);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let nfo = payload(ARTICLE / 2, 92);
    posts.push(Post::plain("lantern.nfo", nfo.clone()));
    let graph = run(&Scenario::new(posts).lost(0, 1).lost(3, 0)).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    let row = history(&graph);
    assert_eq!(row.failed_bytes, nfo.len() as u64);
    assert!(row.health < 1000);
}

// A phase opened with no byte total, as the graph opens its repair, shows
// in the job's phase rows from its start, and does not hold the pipeline
// live while it waits.
#[tokio::test]
async fn a_phase_with_no_total_shows_its_row_without_holding_the_pipeline_live() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) =
        crate::pipeline::tests::new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(99);
    pipeline.shared_state.queue.write(|inner| {
        inner.push_job(crate::pipeline::queue::JobRow::new(
            crate::pipeline::tests::finished_job_info(job_id),
        ));
        crate::pipeline::queue::Change::changed(())
    });
    pipeline.phase_begin_unsampled(job_id, JobPhase::Repairing);
    let rows = pipeline
        .shared_state
        .get_job(job_id)
        .unwrap()
        .phase_progress;
    assert_eq!(
        rows.iter().map(|row| row.phase).collect::<Vec<_>>(),
        [JobPhase::Repairing]
    );
    assert!(rows[0].started_at_epoch_ms > 0.0);
    assert!(!pipeline.pipeline_is_live());
    pipeline.shared_state.queue.sample_counters(
        &pipeline.metrics,
        &pipeline.event_tx,
        std::time::Instant::now(),
    );
    let rows = pipeline
        .shared_state
        .get_job(job_id)
        .unwrap()
        .phase_progress;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].progress_percent, 0.0);
    pipeline.phase_end(job_id, JobPhase::Repairing);
    assert!(
        pipeline
            .shared_state
            .get_job(job_id)
            .unwrap()
            .phase_progress
            .is_empty()
    );
}

// A clean post under PAR3 leaves both its PAR3 volumes to fetch, as NZBGet
// counts every par file still queued.
#[tokio::test]
async fn a_clean_par3_post_leaves_its_volumes_remaining() {
    let data = payload(4 * ARTICLE, 81);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par3_set("lantern", &[("lantern.bin", &data)], 2));
    let graph = run(&Scenario::new(posts)).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    let report = graph.graph().report.as_ref().expect("the job was reported");
    assert_eq!(report.remaining_par_files, 2);
}

// A volume fetched to its end, one of its articles lost on every server,
// has nothing left to fetch, and is not remaining.
#[tokio::test]
async fn a_volume_with_a_lost_article_fetched_to_its_end_is_not_remaining() {
    let data = payload(4 * ARTICLE, 82);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 3));
    let graph = run(&Scenario::new(posts).lost(0, 1).lost(0, 2).lost(2, 2)).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    let report = graph.graph().report.as_ref().expect("the job was reported");
    assert_eq!(report.remaining_par_files, 0, "{:?}", graph.requests);
}

fn posted(posts: &[Post]) -> u64 {
    posts.iter().map(|post| post.bytes.len() as u64).sum()
}

// A clean post ends its download with nothing still to come and every byte
// it wanted done: the recovery volume, but for its head, stays parked and is
// neither still to come nor wanted. Nothing asks for recovery, so what is
// still to come only ever falls.
#[tokio::test]
async fn a_clean_post_ends_its_download_with_nothing_remaining() {
    let scenario = Scenario::new(covered(77));
    let posts = &scenario.posts;
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    let report = graph.graph().report.as_ref().expect("the job was reported");
    let parked = recovery_set(posts) - recovery_fetched(posts, &graph);
    assert!(parked > 0, "the volume holds more than its head");
    assert_eq!(report.wanted_bytes, posted(posts) - parked);
    assert_eq!(report.remaining_bytes, 0);
    assert_eq!(report.progress, 1.0);
    assert_eq!(report.file_remaining_bytes.iter().sum::<u64>(), 0);
    let seen = &graph.graph().remaining;
    assert!(
        seen.windows(2).all(|pair| pair[1].0 <= pair[0].0),
        "{seen:?}"
    );
}

// A lost article asks for recovery: the volume's articles it asks for are
// wanted and still to come from then on, so what is still to come grows by
// them, and the download still ends with nothing remaining.
#[tokio::test]
async fn a_loss_grows_remaining_by_the_recovery_it_asks_for() {
    let scenario = Scenario::new(covered(78)).lost(0, 2);
    let posts = &scenario.posts;
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    let report = graph.graph().report.as_ref().expect("the job was reported");
    let fetched = recovery_fetched(posts, &graph);
    let unfetched = recovery_set(posts) - fetched;
    assert_eq!(report.wanted_bytes, posted(posts) - unfetched);
    assert_eq!(report.remaining_bytes, 0);
    assert_eq!(report.progress, 1.0);
    let seen = &graph.graph().remaining;
    let parked_most = seen.iter().map(|(_, wanted)| *wanted).min().unwrap();
    let head = posts[2].articles(ARTICLE).next().unwrap().len() as u64;
    assert_eq!(
        report.wanted_bytes - parked_most,
        fetched - head,
        "{seen:?}"
    );
    assert!(
        seen.windows(2).any(|pair| pair[1].0 > pair[0].0),
        "{seen:?}"
    );
}

// A job shut down and restored from its checkpoint reports the bytes still
// to come and wanted it reported before, at every restart, and still ends
// its download with nothing remaining.
#[tokio::test]
async fn a_restart_reports_the_same_remaining() {
    let scenario = Scenario::new(covered(79));
    let restarted = run_restarting_at(&scenario, &mut |_| {}, Down::Shutdown, &|_| true).await;
    let pairs = &restarted.graph.restored_remaining;
    assert!(!pairs.is_empty(), "the job restarted while reported");
    for (before, after) in pairs {
        assert_eq!(before, after, "{pairs:?}");
    }
    let report = restarted
        .graph
        .report
        .as_ref()
        .expect("the job was reported");
    assert_eq!(report.remaining_bytes, 0);
    assert_eq!(report.progress, 1.0);
}
