// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job's remaining and wanted figures at every step of a run, from the
// figure it was admitted with on: remaining never above wanted, wanted never
// above the post, and remaining rising only when recovery is asked for,
// which wanted then grows by.

use super::*;
use crate::pipeline::tests::direct_store::single_member_store_set;
use crate::pipeline::tests::engines::rar::posts as rar_posts;
use crate::pipeline::tests::engines::rar_rev::rar5_revs;

#[derive(Debug, Clone, PartialEq)]
struct Look {
    step: usize,
    remaining: u64,
    wanted: u64,
    total: u64,
    progress: f64,
    status: String,
}

struct Walk {
    admission: u64,
    looks: Vec<Look>,
    status: Option<JobStatus>,
    // The articles asked for more than once.
    twice: Vec<(u32, u32)>,
}

fn look(pipeline: &Pipeline, step: usize, looks: &mut Vec<Look>) {
    let Some(report) = pipeline.job_report(JOB) else {
        return;
    };
    let seen = Look {
        step,
        remaining: report.remaining_bytes,
        wanted: report.wanted_bytes,
        total: report.total_bytes,
        progress: report.progress,
        status: format!("{:?}", report.status),
    };
    assert!(
        seen.remaining <= seen.wanted && seen.wanted <= seen.total,
        "invariant broken: {seen:?}"
    );
    let same = looks.last().is_some_and(|last| {
        (last.remaining, last.wanted, &last.status) == (seen.remaining, seen.wanted, &seen.status)
    });
    if !same {
        looks.push(seen);
    }
}

async fn walk(scenario: &Scenario, pause_after: Option<usize>) -> Walk {
    let admission = scenario.spec().wanted_bytes();
    let (_temp_dir, mut pipeline, _complete_dir) = admit(scenario).await;
    let mut looks = Vec::new();
    let mut requests = BTreeMap::new();
    let mut step = 0usize;
    let mut paused = false;
    let mut stalled = None;
    look(&pipeline, step, &mut looks);
    loop {
        while let Some((batch, plan)) = handed_out(&mut pipeline, scenario) {
            for work in batch {
                let article = (
                    work.segment_id.file_id.file_index,
                    work.segment_id.segment_number,
                );
                *requests.entry(article).or_insert(0usize) += 1;
                serve_from(
                    &mut pipeline,
                    JOB,
                    scenario,
                    article,
                    &work.exclude_servers,
                    plan.as_ref(),
                )
                .await;
                hear_probe(&mut pipeline).await;
                step += 1;
                look(&pipeline, step, &mut looks);
            }
            if !paused && pause_after.is_some_and(|at| step >= at) {
                paused = true;
                pipeline.pump_decode_queue();
                pipeline.pause_job_runtime(JOB).unwrap();
                look(&pipeline, step, &mut looks);
                assert!(
                    !pipeline.graph_jobs.contains_key(&JOB),
                    "the pause released the graph"
                );
                pipeline.resume_job_runtime(JOB).unwrap();
                look(&pipeline, step, &mut looks);
                crate::pipeline::tests::activate_job(&mut pipeline, JOB).await;
                look(&pipeline, step, &mut looks);
            }
        }
        if finished(&pipeline) {
            break;
        }
        let before = fingerprint(&pipeline, &requests);
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        look(&pipeline, step, &mut looks);
        if finished(&pipeline) {
            break;
        }
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            look(&pipeline, step, &mut looks);
            continue;
        }
        if take_retry(&mut pipeline).await {
            continue;
        }
        let after = fingerprint(&pipeline, &requests);
        if before == after && stalled.replace(after.clone()) == Some(after.clone()) {
            panic!("stalled: {after}");
        }
    }
    let status = job_status_for_assert(&pipeline, JOB);
    retire_pipeline_database(pipeline).await;
    let twice = requests
        .into_iter()
        .filter(|(_, asked)| *asked > 1)
        .map(|(article, _)| article)
        .collect();
    Walk {
        admission,
        looks,
        status,
        twice,
    }
}

fn print(name: &str, walk: &Walk) {
    eprintln!(
        "== {name}: admission wanted {} final status {:?}",
        walk.admission, walk.status
    );
    for seen in &walk.looks {
        eprintln!("   {seen:?}");
    }
}

// The rises in remaining between two looks that no rise in wanted covers:
// what is still to come grew by more than recovery that was asked for.
fn rises(walk: &Walk) -> Vec<(Look, Look)> {
    let mut out = Vec::new();
    let (mut last, mut last_wanted) = (walk.admission, walk.admission);
    let mut prev: Option<Look> = None;
    for seen in &walk.looks {
        if seen.remaining - seen.remaining.min(last) > seen.wanted - seen.wanted.min(last_wanted) {
            out.push((
                prev.clone().unwrap_or(Look {
                    step: 0,
                    remaining: walk.admission,
                    wanted: walk.admission,
                    total: seen.total,
                    progress: 0.0,
                    status: "admission".into(),
                }),
                seen.clone(),
            ));
        }
        last = seen.remaining;
        last_wanted = seen.wanted;
        prev = Some(seen.clone());
    }
    out
}

// Every look whose remaining is above the one before it, from the figure the
// job was admitted with on: with nothing lost, nothing should rise.
fn any_rise(walk: &Walk) -> Vec<Look> {
    let mut last = walk.admission;
    let mut out = Vec::new();
    for seen in &walk.looks {
        if seen.remaining > last {
            out.push(seen.clone());
        }
        last = seen.remaining;
    }
    out
}

fn last_live(walk: &Walk) -> &Look {
    walk.looks.last().expect("the job reported")
}

fn data(len_articles: usize, seed: u8) -> Vec<u8> {
    payload(len_articles * ARTICLE, seed)
}

fn par2_post(seed: u8) -> Vec<Post> {
    let body = data(20, seed);
    let mut posts = vec![Post::plain("lantern.bin", body.clone())];
    posts.extend(par2_volumes("lantern", &[("lantern.bin", &body)], 2, 1));
    posts
}

#[tokio::test]
async fn a_clean_post_never_raises_remaining() {
    let scenario = Scenario::new(par2_post(101));
    let walk = walk(&scenario, None).await;
    print("clean", &walk);
    assert_eq!(last_live(&walk).remaining, 0);
    assert_eq!(last_live(&walk).progress, 1.0);
    let rises = any_rise(&walk);
    assert!(
        rises.is_empty(),
        "remaining rose with nothing lost: {rises:?}"
    );
}

#[tokio::test]
async fn one_lost_article_ends_with_nothing_remaining() {
    let scenario = Scenario::new(par2_post(102)).lost(0, 3);
    let walk = walk(&scenario, None).await;
    print("one lost", &walk);
    assert_eq!(last_live(&walk).remaining, 0);
    assert_eq!(last_live(&walk).progress, 1.0);
}

#[tokio::test]
async fn recovery_itself_short_ends_with_nothing_remaining() {
    let scenario = Scenario::new(par2_post(103))
        .lost(0, 3)
        .lost(0, 4)
        .lost(2, 1);
    let walk = walk(&scenario, None).await;
    print("recovery short", &walk);
    assert_eq!(last_live(&walk).remaining, 0);
}

#[tokio::test]
async fn a_par3_post_with_a_loss_ends_with_nothing_remaining() {
    let body = data(20, 104);
    let mut posts = vec![Post::plain("lantern.bin", body.clone())];
    posts.extend(par3_set("lantern", &[("lantern.bin", &body)], 2));
    let scenario = Scenario::new(posts).lost(0, 5);
    let walk = walk(&scenario, None).await;
    print("par3", &walk);
    assert_eq!(last_live(&walk).remaining, 0);
    assert_eq!(last_live(&walk).progress, 1.0);
}

#[tokio::test]
async fn a_clean_par3_post_never_raises_remaining() {
    let body = data(20, 109);
    let mut posts = vec![Post::plain("lantern.bin", body.clone())];
    posts.extend(par3_set("lantern", &[("lantern.bin", &body)], 2));
    let scenario = Scenario::new(posts);
    let walk = walk(&scenario, None).await;
    print("par3 clean", &walk);
    assert_eq!(last_live(&walk).remaining, 0);
    assert_eq!(last_live(&walk).progress, 1.0);
    let rises = any_rise(&walk);
    assert!(
        rises.is_empty(),
        "remaining rose with nothing lost: {rises:?}"
    );
}

#[tokio::test]
async fn a_clean_rar_set_raises_remaining_only_by_recovery_it_asks_for() {
    let body = super::super::rar_rev::body();
    let volumes = single_member_store_set("beacon.bin", &body, 4);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = rar5_revs("silver.horizon", &bytes, 1);
    let mut posts = rar_posts(volumes);
    posts.extend(revs);
    let scenario = Scenario::new(posts);
    let walk = walk(&scenario, None).await;
    print("rar rev clean", &walk);
    assert_eq!(last_live(&walk).remaining, 0);
    assert_eq!(last_live(&walk).progress, 1.0);
    let rises = rises(&walk);
    assert!(
        rises.is_empty(),
        "remaining rose with nothing lost: {rises:?}"
    );
}

#[tokio::test]
async fn a_post_without_recovery_never_raises_remaining() {
    let scenario = Scenario::new(vec![Post::plain("lantern.bin", data(20, 105))]);
    let walk = walk(&scenario, None).await;
    print("no recovery", &walk);
    assert_eq!(last_live(&walk).remaining, 0);
    assert_eq!(last_live(&walk).progress, 1.0);
    let rises = any_rise(&walk);
    assert!(rises.is_empty(), "{rises:?}");
}

// Obfuscated names: no role says recovery, and the PAR2 posts are last.
#[tokio::test]
async fn obfuscated_names_never_raise_remaining() {
    let mut posts = par2_post(106);
    for (index, post) in posts.iter_mut().enumerate() {
        *post = Post::plain(
            &format!("a9f3c1e{index}d77b0"),
            std::mem::take(&mut post.bytes),
        );
    }
    let scenario = Scenario::new(posts);
    let walk = walk(&scenario, None).await;
    print("obfuscated", &walk);
    assert_eq!(last_live(&walk).remaining, 0);
    assert_eq!(last_live(&walk).progress, 1.0);
    let rises = any_rise(&walk);
    assert!(
        rises.is_empty(),
        "remaining rose with nothing lost: {rises:?}"
    );
}

#[tokio::test]
async fn a_pause_raises_remaining_only_by_an_article_fetched_again() {
    let scenario = Scenario::new(par2_post(107));
    let walk = walk(&scenario, Some(8)).await;
    print("paused", &walk);
    assert_eq!(last_live(&walk).remaining, 0);
    // The resumed root does not hold the payload's first article, which came
    // before the pause, so it is fetched again and is still to come again.
    // Nothing else rises.
    assert!(walk.twice.len() <= 1, "{:?}", walk.twice);
    let rises = rises(&walk);
    let risen: u64 = rises
        .iter()
        .map(|(from, to)| to.remaining - from.remaining)
        .sum();
    assert!(
        risen <= (walk.twice.len() * ARTICLE) as u64,
        "remaining rose across a pause by more than what was fetched again: {rises:?}"
    );
}
