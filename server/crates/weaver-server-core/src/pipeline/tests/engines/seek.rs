// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Seeks under both engines: the RAR store's bids bringing volumes forward
// into the root's window, the PAR2 object's recovery seeks promoted over
// every bid, lost answers, a store read out of a stream, a seek lifted
// through a store's extent map, a store nothing names, an open-ended
// recovery seek, and a recovery seek standing across a restart. Where the
// engines part, the row names how.

use std::collections::{BTreeMap, HashSet};
use std::io::Write;

use weaver_graph::recovery::RepairOutcome;

use super::harness::{
    ARTICLE, Claim, JOB, Outcome, Post, Scenario, Stall, finished, one_block_article, par2_set,
    par2_volumes, payload, recovery_blocks, run, serve, try_run,
};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::Pipeline;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::{multi_member_store_set, obfuscate_volumes};
use crate::pipeline::tests::{
    insert_active_job, new_direct_pipeline_with_buffers, pump_pipeline_runtime_queues,
    retire_pipeline_database,
};

const FOLDER: &str = "lantern beacon";

fn members(count: usize, seed: u8) -> Vec<(String, Vec<u8>)> {
    (0..count)
        .map(|index| {
            (
                format!("beacon{index}.bin"),
                payload(9 * ARTICLE + 7 * (index + 1), seed + index as u8),
            )
        })
        .collect()
}

fn store_set(members: &[(String, Vec<u8>)], volumes: usize) -> Vec<(String, Vec<u8>)> {
    let named: Vec<(&str, Vec<u8>)> = members
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    multi_member_store_set(&named, volumes)
}

fn renamed(volumes: Vec<(String, Vec<u8>)>, base: &str) -> Vec<(String, Vec<u8>)> {
    volumes
        .into_iter()
        .map(|(name, bytes)| (name.replace("silver.horizon", base), bytes))
        .collect()
}

fn posts(volumes: &[(String, Vec<u8>)]) -> Vec<Post> {
    volumes
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect()
}

fn named(files: &[(String, Vec<u8>)]) -> Vec<(&str, &[u8])> {
    files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect()
}

fn xz(bytes: &[u8]) -> Vec<u8> {
    let props = lzma_turbo::LzmaEncProps::xz_preset(0, false).unwrap();
    let mut encoder = lzma_turbo::XzWriter::new(Vec::new(), &props).unwrap();
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn find(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
        .expect("the needle is in the haystack")
}

// Every member delivered at its name with its bytes, and no article asked
// for twice.
fn assert_members(outcome: &Outcome, members: &[(String, Vec<u8>)]) {
    assert!(
        outcome.completed(),
        "{}: {:?} {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure
    );
    let mut expected: Vec<String> = members
        .iter()
        .map(|(name, _)| format!("{FOLDER}/{name}"))
        .collect();
    expected.sort();
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        expected,
        "{}: delivered names",
        outcome.engine
    );
    for (name, bytes) in members {
        assert!(
            outcome.files[&format!("{FOLDER}/{name}")] == *bytes,
            "{}: {name} bytes",
            outcome.engine
        );
    }
    assert_eq!(
        outcome.refetched(),
        Vec::new(),
        "{}: refetched",
        outcome.engine
    );
}

// A run that sat at `status` with nothing left to do.
fn assert_stalled(result: Result<Outcome, Stall>, engine: Engine, status: JobStatus) {
    match result {
        Err(stall) => {
            assert_eq!(stall.engine, engine);
            assert_eq!(stall.status, Some(status), "{engine}: {}", stall.state);
        }
        Ok(outcome) => panic!(
            "{engine} finished {:?} {:?}, expected a stall at {status:?}",
            outcome.status, outcome.failure
        ),
    }
}

fn assert_failed(outcome: &Outcome, kind: HistoryFailureKind) {
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "{}: {:?} {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure
    );
    assert_eq!(outcome.failure_kind, Some(kind), "{}: kind", outcome.engine);
    assert_eq!(
        outcome.refetched(),
        Vec::new(),
        "{}: refetched",
        outcome.engine
    );
}

// Where in the order a file's first article was asked for.
fn first(outcome: &Outcome, file: u32) -> usize {
    outcome
        .order
        .iter()
        .position(|(at, _)| *at == file)
        .unwrap_or_else(|| panic!("{}: file {file} asked for", outcome.engine))
}

// Where in the order a file's first article past its head was asked for.
// The graph samples every recovery volume's head early, to learn what it
// holds, so a recovery seek shows from the volume's second article on.
fn past_head(outcome: &Outcome, file: u32) -> usize {
    outcome
        .order
        .iter()
        .position(|(at, ordinal)| *at == file && *ordinal > 0)
        .unwrap_or_else(|| panic!("{}: file {file} past its head", outcome.engine))
}

// Where in the order a file's last article was asked for.
fn last(outcome: &Outcome, file: u32) -> usize {
    outcome
        .order
        .iter()
        .rposition(|(at, _)| *at == file)
        .unwrap_or_else(|| panic!("{}: file {file} asked for", outcome.engine))
}

// Row 1. A store posted out of volume order, reversed and shuffled, a plain
// file between its volumes: the store's bids bring each next volume forward
// as far as the window lets them, and both engines ask in one order.
async fn out_of_order(order: &[usize]) {
    let members = members(4, 11);
    let volumes = store_set(&members, 12);
    let mut posts: Vec<Post> = order
        .iter()
        .map(|index| Post::plain(&volumes[*index].0, volumes[*index].1.clone()))
        .collect();
    let plain = payload(6 * ARTICLE, 3);
    posts.insert(6, Post::plain("ember.bin", plain.clone()));
    let scenario = Scenario::new(posts);
    let classic = run(&scenario, Engine::Classic).await;
    let graph = run(&scenario, Engine::Graph).await;
    let mut all = members.clone();
    all.push(("ember.bin".to_string(), plain));
    assert_members(&classic, &all);
    assert_members(&graph, &all);
    assert_eq!(graph.order, classic.order, "one order of asks");
}

#[tokio::test]
async fn a_store_posted_in_reverse_is_asked_for_in_one_order_by_both_engines() {
    out_of_order(&(0..12).rev().collect::<Vec<_>>()).await;
}

#[tokio::test]
async fn a_store_posted_shuffled_is_asked_for_in_one_order_by_both_engines() {
    out_of_order(&[5, 0, 9, 3, 11, 1, 7, 10, 2, 8, 4, 6]).await;
}

// Row 2. Two stores interleaved volume by volume: the window ranks every
// bid by its distance from where its store reads, so volume k of both
// stores is asked for before volume k + 1 of either, and neither starves.
#[tokio::test]
async fn two_stores_share_the_window_volume_by_volume() {
    let left: Vec<(String, Vec<u8>)> = members(4, 21)
        .into_iter()
        .map(|(name, bytes)| (format!("amber.{name}"), bytes))
        .collect();
    let right = members(4, 41);
    let a = renamed(store_set(&left, 10), "amber.dusk");
    let b = store_set(&right, 10);
    let mut posts = Vec::new();
    for (first, second) in a.iter().zip(&b) {
        posts.push(Post::plain(&first.0, first.1.clone()));
        posts.push(Post::plain(&second.0, second.1.clone()));
    }
    let scenario = Scenario::new(posts);
    let classic = run(&scenario, Engine::Classic).await;
    let graph = run(&scenario, Engine::Graph).await;
    for outcome in [&classic, &graph] {
        assert!(
            outcome.completed(),
            "{}: {:?}",
            outcome.engine,
            outcome.failure
        );
        assert_eq!(outcome.refetched(), Vec::new(), "{}", outcome.engine);
        assert_eq!(
            outcome.files.len(),
            8,
            "{}: both stores' members",
            outcome.engine
        );
        for volume in 0..9u32 {
            for store in 0..2u32 {
                assert!(
                    first(outcome, 2 * volume + store) < first(outcome, 2 * (volume + 1)),
                    "{}: volume {volume} of store {store} before volume {} of either",
                    outcome.engine,
                    volume + 1
                );
                assert!(
                    first(outcome, 2 * volume + store) < first(outcome, 2 * (volume + 1) + 1),
                    "{}: volume {volume} of store {store} before volume {} of either",
                    outcome.engine,
                    volume + 1
                );
            }
        }
    }
    assert_eq!(graph.order, classic.order, "one order of asks");
}

// Row 3. A damaged article in the second volume: the PAR2 object's recovery
// seek is promoted over every store bid, so the recovery volume that holds
// enough blocks comes whole before the store's remaining volumes, and the
// others are only sampled. Classic leaves the job at Downloading: it holds
// the store's damage for an unpack that a store fed in order never asks
// for, where the graph repairs as soon as the blocks are in.
#[tokio::test]
async fn a_recovery_seek_is_promoted_over_every_store_bid() {
    let members = members(3, 61);
    let volumes = store_set(&members, 6);
    let mut posts = posts(&volumes);
    posts.extend(par2_volumes("silver.horizon", &named(&volumes), 3, 3));
    let scenario = Scenario::new(posts).damaged(3, 2);
    let graph = run(&scenario, Engine::Graph).await;
    assert_members(&graph, &members);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    let damaged = graph
        .order
        .iter()
        .position(|article| *article == (3, 2))
        .unwrap();
    let recovery = past_head(&graph, 7);
    assert!(damaged < recovery, "the recovery seek follows the damage");
    assert!(
        last(&graph, 7) < last(&graph, 5),
        "the recovery volume comes whole before the store's last volume"
    );
    assert_stalled(
        try_run(&scenario, Engine::Classic).await,
        Engine::Classic,
        JobStatus::Downloading,
    );
}

// Row 4a. A recovery article lost in the first recovery volume: the seek is
// lost there, but the blocks beyond the lost article still come, and the
// next volume makes up the shortfall. Classic stalls at Downloading, as in
// row 3.
#[tokio::test]
async fn a_lost_recovery_article_is_made_up_from_the_next_volume() {
    let members = members(3, 71);
    let volumes = store_set(&members, 4);
    let mut posts = posts(&volumes);
    posts.extend(par2_volumes("silver.horizon", &named(&volumes), 3, 3));
    let blocks = recovery_blocks(&posts[5]);
    let scenario = Scenario::new(posts).damaged(1, 2).lost(5, blocks[0][1]);
    let graph = run(&scenario, Engine::Graph).await;
    assert_members(&graph, &members);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert_eq!(graph.graph().blocks_reachable, 2);
    assert_stalled(
        try_run(&scenario, Engine::Classic).await,
        Engine::Classic,
        JobStatus::Downloading,
    );
}

// Row 4b. The only recovery block lost: nothing can make it up, so the
// graph rules the repair impossible at once and asks for nothing again.
// Classic leaves the job at Downloading: its RAR set never takes in the
// damaged second volume, holding it for a repair that cannot come, where
// the graph fails the repair.
#[tokio::test]
async fn a_lost_recovery_article_nothing_makes_up_fails_the_repair() {
    let members = members(3, 81);
    let volumes = store_set(&members, 4);
    let mut posts = posts(&volumes);
    posts.extend(par2_set("silver.horizon", &named(&volumes), 1));
    let blocks = recovery_blocks(&posts[5]);
    let scenario = Scenario::new(posts).damaged(1, 2).lost(5, blocks[0][1]);
    let graph = run(&scenario, Engine::Graph).await;
    assert_failed(&graph, HistoryFailureKind::Repair);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Impossible {
            damaged: 1,
            reachable: 0
        })]
    );
    assert_stalled(
        try_run(&scenario, Engine::Classic).await,
        Engine::Classic,
        JobStatus::Downloading,
    );
}

// Row 4c. A recovery article lost in the one volume that holds the blocks:
// the block it carried is gone, and the volume's next block repairs.
#[tokio::test]
async fn a_lost_recovery_article_is_made_up_from_the_same_volume() {
    let members = members(3, 75);
    let volumes = store_set(&members, 4);
    let mut posts = posts(&volumes);
    posts.extend(par2_volumes("silver.horizon", &named(&volumes), 2, 1));
    let lost = one_block_article(&posts[5]);
    let scenario = Scenario::new(posts).damaged(1, 2).lost(5, lost);
    let graph = run(&scenario, Engine::Graph).await;
    assert_members(&graph, &members);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    let classic = try_run(&scenario, Engine::Classic).await;
    assert_stalled(classic, Engine::Classic, JobStatus::Downloading);
}

// Row 5. A data article lost with no recovery: the loss reaches the store
// as damage at its exact range. The graph delivers the volumes as posted,
// the damaged one marked, where classic fails the unpack; the same parting
// as depth.rs's undecodable member.
#[tokio::test]
async fn a_lost_data_article_without_recovery_delivers_the_volumes_as_posted() {
    let members = members(4, 91);
    let volumes = store_set(&members, 8);
    let scenario = Scenario::new(posts(&volumes)).lost(5, 2);
    let graph = run(&scenario, Engine::Graph).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(graph.refetched(), Vec::new());
    let mut names: Vec<String> = volumes
        .iter()
        .map(|(name, _)| format!("{FOLDER}/{name}"))
        .collect();
    names.sort();
    assert_eq!(graph.files.keys().cloned().collect::<Vec<_>>(), names);
    assert_eq!(
        graph.graph().claims.get("silver.horizon.part06.rar"),
        Some(&Claim::Damaged)
    );
    assert_eq!(graph.graph().outcomes, Vec::new());
    let classic = run(&scenario, Engine::Classic).await;
    assert_failed(&classic, HistoryFailureKind::Unpack);
}

// Row 6a. A store posted inside an xz stream: the stream cannot seek, so
// every bid is answered Unseekable and the store reads in stream order.
// Classic leaves the job at Extracting: it has no reader for a store inside
// a stream.
#[tokio::test]
async fn a_store_read_out_of_a_stream_reads_in_stream_order() {
    let members = members(3, 101);
    let volume = store_set(&members, 1).remove(0);
    let scenario = Scenario::new(vec![Post::plain("silver.horizon.rar.xz", xz(&volume.1))]);
    let graph = run(&scenario, Engine::Graph).await;
    assert_members(&graph, &members);
    assert_stalled(
        try_run(&scenario, Engine::Classic).await,
        Engine::Classic,
        JobStatus::Extracting,
    );
}

// Row 6b. A PAR2 set stored beside a damaged member in one RAR volume: the
// set reads its blocks back from its volumes' files and the member's slices
// through the store's namespace, and the repaired range is delivered to the
// member. `wrap` posts the volume plain or inside an xz stream, the one
// difference between the rows.
async fn par2_in_a_store(wrap: impl Fn(Vec<u8>) -> Post) -> (Scenario, Vec<u8>, Vec<u8>) {
    let body = payload(6 * ARTICLE, 111);
    let set = par2_set("ember", &[("ember.bin", &body)], 2);
    let mut bad = body.clone();
    bad[2 * ARTICLE + 3] ^= 0x41;
    let inside: Vec<(String, Vec<u8>)> = vec![
        (set[0].name.clone(), set[0].bytes.clone()),
        ("ember.bin".to_string(), bad.clone()),
        (set[1].name.clone(), set[1].bytes.clone()),
    ];
    let volume = store_set(&inside, 1).remove(0);
    (Scenario::new(vec![wrap(volume.1)]), body, bad)
}

// The graph repaired ember.bin in place, reading back what the evidence did
// not settle, and asked for nothing twice.
fn assert_repaired_in_store(graph: &Outcome, body: &[u8], reads: u64) {
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(graph.refetched(), Vec::new());
    assert!(
        graph.files[&format!("{FOLDER}/ember.bin")] == body,
        "ember.bin repaired"
    );
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert_eq!(graph.reads, reads, "read-backs");
}

// The plain twin. Classic runs no repair inside an archive: it extracts
// ember.bin as stored, damaged, and completes.
#[tokio::test]
async fn a_par2_set_in_a_plain_store_repairs_the_member_beside_it() {
    let (scenario, body, bad) =
        par2_in_a_store(|bytes| Post::plain("silver.horizon.rar", bytes)).await;
    let graph = run(&scenario, Engine::Graph).await;
    assert_repaired_in_store(&graph, &body, 11);
    let classic = run(&scenario, Engine::Classic).await;
    assert!(classic.completed(), "{:?}", classic.failure);
    assert!(
        classic.files[&format!("{FOLDER}/ember.bin")] == bad,
        "classic: ember.bin as stored"
    );
}

// The same volume inside an xz stream: the store reads it as the stream
// decodes it, and the set repairs exactly as in the plain twin. Classic
// leaves the job at Extracting, as in row 6a.
#[tokio::test]
async fn a_par2_set_in_a_store_in_a_stream_repairs_the_member_beside_it() {
    let (scenario, body, _) =
        par2_in_a_store(|bytes| Post::plain("silver.horizon.rar.xz", xz(&bytes))).await;
    let graph = run(&scenario, Engine::Graph).await;
    assert_repaired_in_store(&graph, &body, 11);
    assert_stalled(
        try_run(&scenario, Engine::Classic).await,
        Engine::Classic,
        JobStatus::Extracting,
    );
}

// Row 6c. A PAR2 volume posted as an xz stream beside its data: the volume
// inside the stream is never bound to the set at the root, so both engines
// fail the repair with no block to spend.
#[tokio::test]
async fn a_par2_volume_posted_in_a_stream_is_not_bound_to_its_set() {
    let body = payload(6 * ARTICLE, 113);
    let set = par2_set("ember", &[("ember.bin", &body)], 2);
    let scenario = Scenario::new(vec![
        Post::plain("ember.bin", body.clone()),
        Post::plain(&set[0].name, set[0].bytes.clone()),
        Post::plain(&format!("{}.xz", set[1].name), xz(&set[1].bytes)),
    ])
    .damaged(0, 2);
    let graph = run(&scenario, Engine::Graph).await;
    assert_failed(&graph, HistoryFailureKind::Repair);
    let classic = run(&scenario, Engine::Classic).await;
    assert_failed(&classic, HistoryFailureKind::Repair);
}

// Row 7. A PAR2 set stored inside a two-volume RAR, over a member stored
// beside it with an article damaged: the recovery seek is lifted through
// the store's extent map to the volume ranges that hold the blocks, and the
// set rules only once the store's volumes are in. The graph repairs inside
// the store; classic runs no repair inside an archive and fails the unpack.
#[tokio::test]
async fn a_recovery_seek_is_lifted_through_a_store_extent_map() {
    let a = payload(4 * ARTICLE, 121);
    let set = par2_set("ember", &[("ember.bin", &a)], 4);
    let (index, recovery) = (&set[0], &set[1]);
    let b = payload(recovery.bytes.len() - index.bytes.len() - a.len(), 122);
    let inside: Vec<(String, Vec<u8>)> = vec![
        (index.name.clone(), index.bytes.clone()),
        ("ember.bin".to_string(), a.clone()),
        ("ash.bin".to_string(), b),
        ("e.par2".to_string(), recovery.bytes.clone()),
    ];
    let volumes = store_set(&inside, 2);
    let at = find(&volumes[0].1, &a[ARTICLE..2 * ARTICLE]);
    let ordinal = (at / ARTICLE + 1) as u32;
    let scenario = Scenario::new(posts(&volumes)).damaged(0, ordinal);
    let graph = run(&scenario, Engine::Graph).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(graph.refetched(), Vec::new());
    assert!(
        graph.files[&format!("{FOLDER}/ember.bin")] == a,
        "ember.bin repaired"
    );
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    let classic = run(&scenario, Engine::Classic).await;
    assert_failed(&classic, HistoryFailureKind::Unpack);
}

// Row 8. A store whose volumes no name places, its first volume posted
// last, with a PAR2 set at the root: the store bids for nothing it cannot
// name, places each volume by its headers as it arrives, and both engines
// deliver its members, asking for nothing twice.
#[tokio::test]
async fn a_store_nothing_names_bids_for_nothing() {
    let members = members(2, 131);
    let volumes = store_set(&members, 2);
    let mut posts = posts(&obfuscate_volumes(&volumes));
    posts.swap(0, 1);
    posts.extend(par2_set("silver.horizon", &named(&volumes), 1));
    let scenario = Scenario::new(posts);
    let graph = run(&scenario, Engine::Graph).await;
    assert_members(&graph, &members);
    let classic = run(&scenario, Engine::Classic).await;
    assert_members(&classic, &members);
}

// Row 9. A plain file damaged with a one-volume set: the recovery seek asks
// for the rest of the volume, open-ended, and the volume is fetched right
// after the damage. Both engines repair.
#[tokio::test]
async fn an_open_ended_recovery_seek_fetches_the_volume_after_the_damage() {
    let body = payload(5 * ARTICLE, 141);
    let mut posts = vec![Post::plain("ember.bin", body.clone())];
    posts.extend(par2_set("ember", &[("ember.bin", &body)], 1));
    let scenario = Scenario::new(posts).damaged(0, 2);
    let expected = vec![("ember.bin".to_string(), body)];
    let classic = run(&scenario, Engine::Classic).await;
    let graph = run(&scenario, Engine::Graph).await;
    assert_members(&classic, &expected);
    assert_members(&graph, &expected);
    let damaged = graph
        .order
        .iter()
        .position(|article| *article == (0, 2))
        .unwrap();
    assert!(
        damaged < past_head(&graph, 2),
        "the volume follows the damage"
    );
}

// Row 10. A graph run restarted once, the moment the recovery seek was
// answered: the restored objects restate their wants, and the run finishes
// in the order the whole run took, asking for nothing twice.
#[tokio::test]
async fn a_recovery_seek_stands_across_a_restart() {
    let members = members(3, 151);
    let volumes = store_set(&members, 4);
    let mut posts = posts(&volumes);
    posts.extend(par2_set("silver.horizon", &named(&volumes), 2));
    let scenario = Scenario::new(posts).damaged(1, 2);
    let whole = run(&scenario, Engine::Graph).await;
    assert_members(&whole, &members);
    let (status, order, requests) = run_restarting_once(&scenario).await;
    assert_eq!(status, Some(JobStatus::Complete));
    assert_eq!(order, whole.order, "the restarted run's order");
    assert!(
        requests.values().all(|count| *count == 1),
        "the restarted run asks for nothing twice: {requests:?}"
    );
}

// A graph run restarted once, the moment the recovery seek was answered:
// recovery work sits queued and none of it was handed out yet.
async fn run_restarting_once(
    scenario: &Scenario,
) -> (
    Option<JobStatus>,
    Vec<(u32, u32)>,
    BTreeMap<(u32, u32), usize>,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(&temp_dir, buffers(), 1).await;
    insert_active_job(&mut pipeline, JOB, scenario.spec()).await;
    stamp_graph(&pipeline.db);
    pipeline.admit_graph_job(JOB, Engine::Graph);
    let mut order = Vec::new();
    let mut requests = BTreeMap::new();
    let mut restarted = false;
    loop {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.download_queue.pop())
        {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            *requests.entry(article).or_insert(0usize) += 1;
            order.push(article);
            serve(&mut pipeline, scenario, article).await;
            let promoted = pipeline
                .jobs
                .get(&JOB)
                .is_some_and(|state| state.download_queue.has_recovery_work());
            if !restarted && promoted {
                restarted = true;
                pipeline = restart(&temp_dir, pipeline, scenario).await;
            }
        }
        if finished(&pipeline) {
            break;
        }
        if pipeline.write_buffered_bytes > 0 {
            pipeline.flush_quiescent_write_backlog().await;
        }
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        if finished(&pipeline) {
            break;
        }
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.runs().live())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        let queued = pipeline
            .jobs
            .get(&JOB)
            .map_or(0, |state| state.download_queue.len());
        assert!(queued > 0, "the restarted job stalled");
    }
    assert!(restarted, "the run restarted");
    let status = crate::pipeline::tests::job_status_for_assert(&pipeline, JOB);
    retire_pipeline_database(pipeline).await;
    (status, order, requests)
}

fn buffers() -> crate::runtime::buffers::BufferPoolConfig {
    crate::runtime::buffers::BufferPoolConfig {
        small_count: 8,
        medium_count: 4,
        large_count: 2,
    }
}

// The pipeline goes down and a new one restores the job from what reached
// the database, as `restart.rs` does after every article.
async fn restart(
    temp_dir: &tempfile::TempDir,
    pipeline: Pipeline,
    scenario: &Scenario,
) -> Pipeline {
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    let db = pipeline.db.clone();
    retire_pipeline_database(pipeline).await;
    drop(db);
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(temp_dir, buffers(), 1).await;
    pipeline
        .restore_job(crate::RestoreJobRequest {
            job_id: JOB,
            job_hash: [0; 32],
            spec: scenario.spec(),
            complete_files: HashSet::new(),
            file_progress: Default::default(),
            detected_archives: Default::default(),
            file_identities: Default::default(),
            extracted_members: HashSet::new(),
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
        })
        .await
        .unwrap();
    assert!(pipeline.graph_jobs.contains_key(&JOB), "the job restored");
    pipeline
}

// The row stamped graph as an admission stamps it.
fn stamp_graph(db: &crate::Database) {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.read_exec(),
            "UPDATE active_jobs SET engine = {} WHERE job_id = {}",
            &[
                SqlArg::Text(Engine::Graph.as_str().to_string()),
                SqlArg::I64(JOB.0 as i64),
            ],
        )
        .await?;
        Ok(())
    })
    .unwrap();
}
