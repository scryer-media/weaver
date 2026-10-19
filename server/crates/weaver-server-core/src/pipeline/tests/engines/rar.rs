// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Stored RAR sets under both engines: a member split across RAR5 and RAR4
// volumes, the set's volumes damaged, lost, renamed or posted out of order
// beside a PAR2 set, the set encrypted, and a compressed stream stored in
// a volume. Nothing is compressed by RAR, so the store streams every member
// unless its headers are shut to it.

use std::io::Write;

use weaver_graph::Why;
use weaver_graph::recovery::RepairOutcome;

use weaver_model::files::FileRole;

use super::harness::{
    ARTICLE, Claim, JOB, Outcome, Post, Scenario, Stall, admit, assert_same, par2_set, payload,
    run, run_both, serve_job, try_run,
};
use crate::e2e_failpoint::DIRECT_STORE_POST_REPAIR_VERIFY;
use crate::history::HistoryFailureKind;
use crate::jobs::ids::JobId;
use crate::jobs::model::JobStatus;
use crate::pipeline::Pipeline;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::scenarios::{
    HeaderCheck, encrypted_rar4_store_set, encrypted_store_set, header_encrypted_store_set,
};
use crate::pipeline::tests::direct_store::{
    obfuscate_volumes, recovery_record_store_set, single_member_rar4_store_set_numbered,
    single_member_store_set,
};
use crate::pipeline::tests::{
    debug_job_state, insert_active_job, job_status_for_assert, retire_pipeline_database,
};

const MEMBER: &str = "beacon.bin";
const SECRET: &str = "quill-ember-secret";
// A second job beside the harness's own.
const OTHER: JobId = JobId(93_002);

pub(super) fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// A member that ends mid-article, over enough articles that every volume
// of a four-volume set holds some of it.
fn body() -> Vec<u8> {
    payload(7 * ARTICLE + 13, 51)
}

pub(super) fn posts(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    volumes
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect()
}

// The volumes, then an index and a volume of two recovery blocks over them.
pub(super) fn with_par2(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let set = par2_set("silver.horizon", &files, 2);
    let mut out = posts(volumes.clone());
    out.extend(set);
    out
}

fn xz(bytes: &[u8]) -> Vec<u8> {
    let props = lzma_turbo::LzmaEncProps::xz_preset(0, false).unwrap();
    let mut encoder = lzma_turbo::XzWriter::new(Vec::new(), &props).unwrap();
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

// The member alone, every byte as written.
fn assert_member(outcome: &Outcome, body: &[u8]) {
    assert_delivers(outcome, MEMBER, body);
}

fn assert_delivers(outcome: &Outcome, name: &str, body: &[u8]) {
    assert!(
        outcome.completed(),
        "{}: {:?} {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure
    );
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered(name)],
        "{}: delivered names",
        outcome.engine
    );
    assert!(
        outcome.files[&delivered(name)] == body,
        "{}: member bytes",
        outcome.engine
    );
}

// The graph repaired one slice in the stream, reading members back for it,
// and asked for no article twice.
fn assert_repaired_in_stream(graph: &Outcome) {
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert!(graph.reads > 0, "the repair read members back");
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// Every volume's mark is what its bytes are now.
fn assert_volumes_complete(graph: &Outcome) {
    let claims = &graph.graph().claims;
    assert!(
        claims.values().all(|claim| *claim == Claim::Complete),
        "{claims:?}"
    );
}

// Classic waits at Downloading on these sets and never settles.
pub(super) fn assert_classic_stalls(stall: Result<Outcome, Stall>) {
    let stall = stall.expect_err("classic settles");
    assert_eq!(stall.engine, Engine::Classic);
    assert_eq!(
        stall.status,
        Some(JobStatus::Downloading),
        "{}",
        stall.state
    );
}

#[tokio::test]
async fn a_two_volume_rar5_set_delivers_its_members_under_both_engines() {
    let body = body();
    let scenario = Scenario::new(posts(single_member_store_set(MEMBER, &body, 2)));
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

#[tokio::test]
async fn a_four_volume_rar5_set_delivers_its_member_under_both_engines() {
    let body = body();
    let scenario = Scenario::new(posts(single_member_store_set(MEMBER, &body, 4)));
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

#[tokio::test]
async fn a_two_volume_rar4_set_delivers_its_member_under_both_engines() {
    let body = body();
    let scenario = Scenario::new(posts(single_member_rar4_store_set_numbered(
        MEMBER, &body, 2,
    )));
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

#[tokio::test]
async fn a_four_volume_rar4_set_delivers_its_member_under_both_engines() {
    let body = body();
    let scenario = Scenario::new(posts(single_member_rar4_store_set_numbered(
        MEMBER, &body, 4,
    )));
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

// Served last article first, as a crowd of workers may: each volume's data
// is in hand before the headers that place it. Graph only: the batch the
// harness serves backwards is in no queue classic can see.
#[tokio::test]
async fn a_four_volume_rar5_set_served_last_article_first_delivers_its_member_under_graph() {
    let body = body();
    let scenario = Scenario::new(posts(single_member_store_set(MEMBER, &body, 4))).backwards();
    let graph = run(&scenario, Engine::Graph).await;
    assert_member(&graph, &body);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// Divergence: classic never settles on a stored set with a damaged article
// beside PAR2. The graph repairs the slice in the stream, and every volume
// ends complete: the one damaged mark was the article's, and its repair
// replaced it.
#[tokio::test]
async fn a_member_damaged_mid_volume_is_repaired_in_the_stream_under_graph() {
    let body = body();
    let scenario =
        Scenario::new(with_par2(single_member_store_set(MEMBER, &body, 2))).damaged(0, 2);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_member(&graph, &body);
    assert_repaired_in_stream(&graph);
    assert_volumes_complete(&graph);
}

// Divergence: classic delivers the member without reading anything back;
// the graph repairs the lost slice in the stream.
#[tokio::test]
async fn a_lost_article_beside_par2_is_rebuilt_under_both_engines() {
    let body = body();
    let scenario = Scenario::new(with_par2(single_member_store_set(MEMBER, &body, 2))).lost(0, 2);
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_repaired_in_stream(&graph);
    assert_eq!(classic.reads, 0);
    assert!(classic.refetched().is_empty(), "{:?}", classic.refetched());
    assert!(
        graph
            .graph()
            .claims
            .values()
            .all(|claim| *claim == Claim::Complete)
    );
}

// Every article of the scenario's recovery volumes.
fn recovery_articles(scenario: &Scenario) -> Vec<(u32, u32)> {
    scenario
        .posts
        .iter()
        .zip(0u32..)
        .filter(|(post, _)| {
            matches!(
                post.role,
                FileRole::Par2 {
                    is_index: false,
                    ..
                }
            )
        })
        .flat_map(|(post, file)| {
            (0..post.bytes.len().div_ceil(scenario.article) as u32)
                .map(move |ordinal| (file, ordinal))
        })
        .collect()
}

// A RAR beside PAR2 whose opening article is gone, every article of its
// recovery volumes refused at the first ask, as a primary that holds none
// of them answers.
fn opening_lost_recovery_refused() -> Scenario {
    let mut scenario =
        Scenario::new(with_par2(single_member_store_set(MEMBER, &body(), 1))).lost(0, 0);
    let refused = recovery_articles(&scenario);
    assert!(!refused.is_empty(), "the set has recovery volumes");
    for (file, ordinal) in refused {
        scenario = scenario.refused_once(file, ordinal);
    }
    scenario
}

// The set's opening article is gone, so nothing says what the volume is
// until PAR2 rebuilds it: the recovery volume is asked for, and every
// article of it the first ask is refused for is asked again, not parked,
// until it is fetched and the slice repaired. The harness has one server,
// so the second ask goes to it; the backup taking the ask is e2e's leg.
#[tokio::test]
async fn a_lost_opening_article_beside_par2_fetches_the_recovery_volume_under_graph() {
    let scenario = opening_lost_recovery_refused();
    let graph = run(&scenario, Engine::Graph).await;
    assert_member(&graph, &body());
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    // Asked again are the refused articles alone, each once more.
    let refetched = graph.refetched();
    assert!(!refetched.is_empty(), "{:?}", graph.requests);
    assert!(
        refetched
            .iter()
            .all(|article| scenario.refused.contains(article) && graph.requests[article] == 2),
        "{refetched:?}"
    );
}

// Serve `job` and run its completion gate until its delivery move is out.
async fn drive_to_move(pipeline: &mut Pipeline, job: JobId, scenario: &Scenario) {
    let mut idle = 0;
    while !pipeline.inflight_moves.contains(&job) {
        let mut served = false;
        while let Some(work) = pipeline
            .jobs
            .get_mut(&job)
            .and_then(|state| state.download_queue.pop())
        {
            served = true;
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            serve_job(pipeline, job, scenario, article).await;
        }
        if pipeline.inflight_moves.contains(&job) {
            break;
        }
        if pipeline.write_buffered_bytes > 0 {
            pipeline.flush_quiescent_write_backlog().await;
        }
        pipeline.check_job_completion(job).await;
        while let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued).await;
        }
        if pipeline
            .graph_jobs
            .get(&job)
            .is_some_and(|graph| graph.runs().live())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        idle = if served { 0 } else { idle + 1 };
        assert!(
            idle < 2 || pipeline.inflight_moves.contains(&job),
            "{job:?} stalled: {:?} {}",
            job_status_for_assert(pipeline, job),
            debug_job_state(pipeline, job)
        );
    }
}

// A graph job held at the post-repair hook stands at its move while another
// graph job is delivered beside it: the hold is the held job's alone.
#[test]
fn a_graph_job_held_after_its_repair_holds_no_other_job() {
    // The hook is read from the environment once per process and holds far
    // past any run. The row ends on the other job's delivery, never on the
    // hold, and the held thread is left behind when the runtime stops.
    // SAFETY: nothing else in this test's process reads the environment yet.
    unsafe {
        std::env::set_var(
            "WEAVER_E2E_DELAY",
            format!("{DIRECT_STORE_POST_REPAIR_VERIFY}=3600000"),
        );
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let held = opening_lost_recovery_refused();
        let other = Scenario::new(vec![Post::plain("ember.bin", payload(3 * ARTICLE + 5, 23))]);
        let (_temp_dir, mut pipeline, _) = admit(&held, Engine::Graph).await;
        insert_active_job(&mut pipeline, OTHER, other.spec()).await;
        pipeline.admit_graph_job(OTHER, Engine::Graph);

        drive_to_move(&mut pipeline, JOB, &held).await;
        assert!(
            pipeline
                .graph_recovery_summary(JOB)
                .is_some_and(|summary| summary.repaired()),
            "the held job repaired"
        );
        drive_to_move(&mut pipeline, OTHER, &other).await;
        let done = pipeline.move_done_rx.recv().await.expect("a move reports");
        assert_eq!(done.job_id, OTHER, "the held job's move is still held");
        pipeline.handle_move_to_complete_done(done).await;
        assert_eq!(
            job_status_for_assert(&pipeline, OTHER),
            Some(JobStatus::Complete)
        );
        assert!(pipeline.inflight_moves.contains(&JOB));
        assert_eq!(
            job_status_for_assert(&pipeline, JOB),
            Some(JobStatus::Moving)
        );
    });
    runtime.shutdown_background();
}

// Recovery a graph job has not asked for is not parked work: dispatch
// schedules no completion check for it the way it does for classic's
// parked queue, whose check is what unparks it.
#[tokio::test]
async fn dispatch_schedules_no_parked_recovery_check_for_a_graph_job() {
    let scenario = Scenario::new(with_par2(single_member_store_set(MEMBER, &body(), 1)));
    for engine in [Engine::Classic, Engine::Graph] {
        let (_temp_dir, mut pipeline, _) = admit(&scenario, engine).await;
        let state = pipeline.jobs.get_mut(&JOB).unwrap();
        while state.download_queue.pop().is_some() {}
        assert!(
            !state.recovery_queue.is_empty(),
            "{engine}: recovery queued"
        );
        pipeline.dispatch_downloads();
        assert_eq!(
            pipeline.pending_completion_checks.contains(&JOB),
            engine == Engine::Classic,
            "{engine}: a parked-recovery completion check"
        );
        retire_pipeline_database(pipeline).await;
    }
}

#[tokio::test]
async fn nameless_volumes_renamed_by_par2_deliver_under_both_engines() {
    let body = body();
    let volumes = single_member_store_set(MEMBER, &body, 2);
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut out = posts(obfuscate_volumes(&volumes));
    out.extend(par2_set("silver.horizon", &files, 2));
    let (classic, graph) = run_both(&Scenario::new(out)).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
    assert_eq!(graph.graph().outcomes, vec![Some(RepairOutcome::Clean)]);
    assert!(
        graph
            .graph()
            .claims
            .values()
            .all(|claim| *claim == Claim::Complete)
    );
}

// Nameless and posted last volume first, so the store is built on the
// set's last volume and every earlier one arrives after it, and nothing but
// the headers says where each goes. A named set is fetched in its names'
// order whatever order it is posted in. A nameless RAR4 set posted
// backwards places nothing: a RAR4 volume states its number only in its
// end record, and classic stalls on it too.
fn reversed(mut volumes: Vec<(String, Vec<u8>)>) -> Scenario {
    volumes.reverse();
    Scenario::new(posts(volumes))
}

async fn assert_reversed_delivers(scenario: &Scenario, body: &[u8]) {
    let (classic, graph) = run_both(scenario).await;
    let head = |file: u32| graph.order.iter().position(|ask| *ask == (file, 0));
    assert!(
        head(0) < head(1),
        "the last volume's head first: {:?}",
        graph.order
    );
    assert_member(&classic, body);
    assert_member(&graph, body);
    assert_same(&classic, &graph);
}

#[tokio::test]
async fn a_nameless_rar5_set_whose_first_volume_arrives_last_delivers_under_both_engines() {
    let body = body();
    let set = obfuscate_volumes(&single_member_store_set(MEMBER, &body, 2));
    assert_reversed_delivers(&reversed(set), &body).await;
}

#[tokio::test]
async fn a_nameless_four_volume_rar5_set_posted_backwards_delivers_under_both_engines() {
    let body = body();
    let set = obfuscate_volumes(&single_member_store_set(MEMBER, &body, 4));
    assert_reversed_delivers(&reversed(set), &body).await;
}

// A nameless RAR4 volume's front states no position: its number is in its
// end record, and the first-volume flag of its main header is not among the
// facts unrar-rs reads. The set's last volume founds the store at the first
// position, and its end record says it is not the set's first: that store
// lost its set's first bytes, and the three volumes it holds are delivered
// as they stand, never whole. The true first volume, last to arrive, founds
// a store of its own that never places its member, and is withheld. Classic
// stalls. Under graph nothing is whole, so the job fails and nothing moves
// to the complete directory.
#[tokio::test]
async fn a_nameless_four_volume_rar4_set_posted_backwards_stalls_under_classic_and_is_refused_under_graph()
 {
    let body = body();
    let set = obfuscate_volumes(&single_member_rar4_store_set_numbered(MEMBER, &body, 4));
    let scenario = reversed(set);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = try_run(&scenario, Engine::Graph)
        .await
        .expect("the graph settles");
    let (first, tail) = scenario.posts.split_last().expect("four volumes");
    assert_eq!(
        graph.status,
        Some(JobStatus::Failed {
            error: format!("{} never placed every member it named", first.name),
        }),
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Unpack));
    let delivery = &graph.graph().delivery;
    assert!(delivery.whole.is_empty(), "{delivery:?}");
    assert_eq!(
        delivery.damaged,
        tail.iter()
            .map(|post| post.name.clone())
            .collect::<Vec<_>>(),
        "the tail volumes as they stand"
    );
    assert_eq!(delivery.withheld, [(first.name.clone(), Why::Unmapped)]);
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

#[tokio::test]
async fn an_encrypted_rar5_set_with_its_password_up_front_delivers_under_both_engines() {
    let body = body();
    let volumes = encrypted_store_set(MEMBER, &body, 2, SECRET, Some(SECRET), false);
    let scenario = Scenario::new(posts(volumes)).password(SECRET);
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

// The job is submitted with no password; the only one is among the
// candidates, offered once the store asks. Divergence: classic never
// settles.
#[tokio::test]
async fn an_encrypted_rar5_set_opened_by_a_password_candidate_delivers_under_graph() {
    let body = body();
    let volumes = encrypted_store_set(MEMBER, &body, 2, SECRET, Some(SECRET), false);
    let scenario = Scenario::new(posts(volumes)).global_password(SECRET);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_member(&graph, &body);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// The headers carry a password check, so the store verifies the password
// and deciphers the headers itself: the set streams through the store and
// no engine is needed.
#[tokio::test]
async fn a_header_encrypted_rar5_set_with_a_password_check_streams_under_both_engines() {
    let body = body();
    let volumes = header_encrypted_store_set(MEMBER, &body, 2, SECRET, HeaderCheck::For(SECRET));
    let scenario = Scenario::new(posts(volumes)).password(SECRET);
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

// No password check: the store cannot verify a password before deciphering
// the headers, so it gives the set to the RAR engine with EncryptedHeader.
// Divergence: classic never settles.
#[tokio::test]
async fn a_header_encrypted_rar5_set_without_a_password_check_goes_to_the_engine_under_graph() {
    let body = body();
    let volumes = header_encrypted_store_set(MEMBER, &body, 2, SECRET, HeaderCheck::Absent);
    let scenario = Scenario::new(posts(volumes)).password(SECRET);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_member(&graph, &body);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A three-volume set the RAR engine reads, posted without one volume. The
// engine reads volume p as bound volume p, so the member cannot be read
// whole: its object announced it and never closed it, so the refusal reads
// the posted volumes back as they were, short, and with nothing whole the
// job fails naming them. Divergence: classic never settles.
async fn assert_missing_volume_fails(missing: &str) {
    let body = body();
    let volumes: Vec<(String, Vec<u8>)> =
        header_encrypted_store_set(MEMBER, &body, 3, SECRET, HeaderCheck::Absent)
            .into_iter()
            .zip([
                "silver.horizon.rar",
                "silver.horizon.r00",
                "silver.horizon.r01",
            ])
            .filter(|(_, name)| *name != missing)
            .map(|((_, bytes), name)| (name.to_owned(), bytes))
            .collect();
    let posted = volumes.clone();
    let scenario = Scenario::new(posts(volumes)).password(SECRET);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert!(
        graph.graph().kinds.contains("rar_engine"),
        "{:?}",
        graph.graph().kinds
    );
    let short: Vec<&str> = posted.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        graph.failure,
        Some(format!(
            "no file was delivered whole; {} short: {}",
            short.len(),
            short.join(", ")
        ))
    );
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// Without `.r01`, the last: the engine finds no volume past `.r00`.
#[tokio::test]
async fn a_three_volume_set_the_engine_reads_posted_without_its_r01_fails_under_graph() {
    assert_missing_volume_fails("silver.horizon.r01").await;
}

// Without `.r00`, inside the set: the engine reads position 1 as lost.
#[tokio::test]
async fn a_three_volume_set_the_engine_reads_posted_without_its_r00_fails_under_graph() {
    assert_missing_volume_fails("silver.horizon.r00").await;
}

#[tokio::test]
async fn an_encrypted_rar4_set_delivers_under_both_engines() {
    let body = body();
    let volumes = encrypted_rar4_store_set(MEMBER, &body, 2, SECRET, Some([7; 8]));
    let scenario = Scenario::new(posts(volumes)).password(SECRET);
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

// One damaged article of cipher text: the graph repairs it through the
// posted bytes the store rebuilds, and asks for nothing again. Divergence:
// classic never settles. Every volume ends complete, as in the plain
// damaged row.
#[tokio::test]
async fn an_encrypted_rar5_set_with_a_damaged_cipher_article_is_repaired_under_graph() {
    let body = body();
    let volumes = encrypted_store_set(MEMBER, &body, 2, SECRET, Some(SECRET), false);
    let scenario = Scenario::new(with_par2(volumes))
        .password(SECRET)
        .damaged(0, 2);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_member(&graph, &body);
    assert_repaired_in_stream(&graph);
    assert_volumes_complete(&graph);
}

// Divergence: classic also leaves its direct-unpack scratch file, empty,
// in the complete directory.
#[tokio::test]
async fn an_xz_stream_with_a_damaged_article_is_repaired_under_both_engines() {
    let plain = payload(9 * ARTICLE + 21, 61);
    let packed = xz(&plain);
    let mut out = vec![Post::plain("ember.bin.xz", packed.clone())];
    out.extend(par2_set("ember", &[("ember.bin.xz", &packed)], 2));
    let scenario = Scenario::new(out).damaged(0, 1);
    let (classic, graph) = run_both(&scenario).await;
    assert_delivers(&graph, "ember.bin", &plain);
    assert_repaired_in_stream(&graph);
    assert!(
        classic.completed(),
        "{:?} {:?}",
        classic.status,
        classic.failure
    );
    let scratch = ".weaver-direct-unpack/93001/ember.bin.xz.0/ember.bin".to_string();
    assert_eq!(
        classic.files.keys().cloned().collect::<Vec<_>>(),
        vec![scratch.clone(), delivered("ember.bin")]
    );
    assert!(classic.files[&scratch].is_empty());
    assert!(classic.files[&delivered("ember.bin")] == plain);
    assert!(classic.refetched().is_empty(), "{:?}", classic.refetched());
}

// The store places the stream's bytes in its member and the stream
// decompresses them as they come: no engine reads the volume.
#[tokio::test]
async fn an_xz_stream_inside_a_stored_rar_volume_streams_under_both_engines() {
    let plain = payload(9 * ARTICLE + 21, 61);
    let packed = xz(&plain);
    let scenario = Scenario::new(posts(single_member_store_set("ember.bin.xz", &packed, 1)));
    let (classic, graph) = run_both(&scenario).await;
    assert_delivers(&classic, "ember.bin", &plain);
    assert_delivers(&graph, "ember.bin", &plain);
    assert_same(&classic, &graph);
    assert!(
        graph
            .graph()
            .claims
            .values()
            .all(|claim| *claim == Claim::Complete)
    );
}

// Each volume carries a recovery record and there is no PAR2. Neither
// engine decodes the record, so nothing can repair the damaged article.
// Divergence: classic fails the job at unpack and delivers nothing. The
// graph rules that nothing will repair the set: the store gives its
// volumes up unrepaired, they are delivered as they came, the damaged one
// claimed damaged, and the job completes.
#[tokio::test]
async fn a_rar5_set_with_recovery_records_and_a_damaged_article_ends_unrepaired_under_both_engines()
{
    let body = body();
    let set = recovery_record_store_set(MEMBER, &body, 2, 96);
    let scenario = Scenario::new(posts(set.clone())).damaged(0, 2);
    let (classic, graph) = (
        run(&scenario, Engine::Classic).await,
        run(&scenario, Engine::Graph).await,
    );
    assert!(
        matches!(classic.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        classic.status
    );
    assert_eq!(classic.failure_kind, Some(HistoryFailureKind::Unpack));
    assert!(classic.files.is_empty(), "{:?}", classic.files.keys());
    // The damaged volume is delivered as it stands: the job ends Partial.
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        set.iter()
            .map(|(name, _)| delivered(name))
            .collect::<Vec<_>>()
    );
    assert!(graph.files[&delivered(&set[0].0)] != set[0].1);
    assert_eq!(graph.files[&delivered(&set[1].0)], set[1].1);
    let view = graph.graph();
    assert_eq!(view.claims[&set[0].0], Claim::Damaged);
    assert_eq!(view.claims[&set[1].0], Claim::Complete);
    assert_eq!(view.blocks_reachable, 0);
    assert!(view.outcomes.is_empty(), "{:?}", view.outcomes);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    assert!(classic.refetched().is_empty(), "{:?}", classic.refetched());
}
