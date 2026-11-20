// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Stored RAR sets: a member split across RAR5 and RAR4
// volumes, the set's volumes damaged, lost, renamed or posted out of order
// beside a PAR2 set, the set encrypted, and a compressed stream stored in
// a volume. Nothing is compressed by RAR, so the store streams every member
// unless its headers are shut to it.

use super::harness::{
    ARTICLE, Claim, GraphView, JOB, Post, Scenario, admit, kinds, par2_set, payload, run,
    run_unprompted, serve_job,
};
use super::rar_fixtures::store::{
    MEMBER, SECRET, assert_delivers, assert_volumes_complete, body, opening_lost_recovery_refused,
};
pub(super) use super::rar_fixtures::{delivered, posts, with_par2};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::Pipeline;
use crate::pipeline::tests::direct_store::scenarios::{
    HeaderCheck, encrypted_store_set, header_encrypted_store_set,
    protected_header_encrypted_store_set,
};
use crate::pipeline::tests::direct_store::{
    protected_rar4_store_set, protected_sealed_rar4_store_set, protected_store_set,
    protected_store_set_led_by, single_member_store_set,
};
use crate::pipeline::tests::{insert_active_job, job_status_for_assert, retire_pipeline_database};
use weaver_graph::recovery::RepairOutcome;
use weaver_server_base::e2e_failpoint::DIRECT_STORE_POST_REPAIR_VERIFY;
use weaver_server_base::jobs::ids::JobId;

// A second job beside the harness's own.
const OTHER: JobId = JobId(93_002);

// Serve `job` and run its completion gate until its delivery move is out.
async fn drive_to_move(pipeline: &mut Pipeline, job: JobId, scenario: &Scenario) {
    let mut idle = 0;
    while !pipeline.inflight_moves.contains(&job) {
        let mut served = false;
        while let Some(work) = pipeline
            .jobs
            .get_mut(&job)
            .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
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
        pipeline.check_job_completion(job).await;
        while let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued).await;
        }
        if pipeline
            .graph_jobs
            .get(&job)
            .is_some_and(|graph| graph.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        idle = if served { 0 } else { idle + 1 };
        assert!(
            idle < 2 || pipeline.inflight_moves.contains(&job),
            "{job:?} stalled: {:?}",
            job_status_for_assert(pipeline, job)
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
        let (_temp_dir, mut pipeline, _) = admit(&held).await;
        insert_active_job(&mut pipeline, OTHER, other.spec()).await;

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
        crate::pipeline::tests::settle_script_admissions(&mut pipeline).await;
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

// Recovery a job has deferred is not work a completion check unparks: its
// recovery queue holds what its objects deferred, so the recovery volume's
// head arrives first and its PAR2 object defers the rest, and dispatch
// schedules no check for it.
#[tokio::test]
async fn dispatch_schedules_no_parked_recovery_check() {
    let scenario = Scenario::new(with_par2(single_member_store_set(MEMBER, &body(), 1)));
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    serve_job(&mut pipeline, JOB, &scenario, (2, 0)).await;
    let state = pipeline.jobs.get_mut(&JOB).unwrap();
    while state
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .is_some()
    {}
    assert!(
        !state.articles.active().unwrap().recovery_queue.is_empty(),
        "recovery queued"
    );
    pipeline.dispatch_downloads();
    assert!(
        !pipeline.pending_completion_checks.contains(&JOB),
        "a parked-recovery completion check"
    );
    retire_pipeline_database(pipeline).await;
}

// The encrypted fronts are claimed under swapped names before metadata
// supplies their real names. The password is offered only after those
// provisional claims exist; the verified names must correct the layout.
#[tokio::test]
async fn verified_renames_reassign_swapped_held_volumes() {
    use super::refused_then_repaired::{drive_to_finish, serve_posts, serve_queued};
    use crate::pipeline::tests::pump_pipeline_runtime_queues;
    use weaver_graph::{MemberId, NodeId, VolumeIndex};

    let body = payload(48 * 1024 + 13, 51);
    let volumes = header_encrypted_store_set(MEMBER, &body, 2, SECRET, HeaderCheck::For(SECRET));
    let files: Vec<_> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posted = vec![
        Post::plain("3c9e1f.rar", volumes[1].1.clone()),
        Post::plain("3c9e1f.r00", volumes[0].1.clone()),
    ];
    posted.extend(par2_set("silver.horizon", &files, 2));
    let scenario = Scenario::new(posted).framed(16 * 1024);
    let (_temp, mut pipeline, complete_dir) = admit(&scenario).await;
    for file in 0..2 {
        serve_queued(&mut pipeline, &scenario, (file, 0)).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }
    let tree = pipeline.graph_jobs[&JOB].tree();
    let (owner, index) = tree.child_of(NodeId::ROOT, MemberId(0)).unwrap();
    assert_eq!(
        index,
        VolumeIndex(0),
        "the wrong posted name must first claim index zero"
    );
    assert_eq!(
        tree.child_of(NodeId::ROOT, MemberId(1)),
        Some((owner, VolumeIndex(1)))
    );
    let job = pipeline.graph_jobs.remove(&JOB).unwrap();
    pipeline
        .graph_jobs
        .insert(JOB, job.offering(vec![SECRET.to_owned()]));
    serve_posts(&mut pipeline, &scenario, 2..scenario.posts.len() as u32).await;
    loop {
        let job = &pipeline.graph_jobs[&JOB];
        let tree = job.tree();
        if let Some((owner, VolumeIndex(0))) = tree.child_of(NodeId::ROOT, MemberId(1))
            && tree.child_of(NodeId::ROOT, MemberId(0)) == Some((owner, VolumeIndex(1)))
        {
            break;
        }
        assert!(job.awaits_mail(), "verified volumes must be reassigned");
        let said = pipeline
            .graph_runs
            .recv()
            .await
            .expect("the owning worker answers");
        pipeline.graph_run_said(said);
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }
    let tree = pipeline.graph_jobs[&JOB].tree();
    assert_eq!(
        tree.namespace(NodeId::ROOT)
            .unwrap()
            .get(MemberId(0))
            .unwrap()
            .name,
        volumes[1].0
    );
    let (owner, index) = tree
        .child_of(NodeId::ROOT, MemberId(1))
        .expect("verified first volume is bound");
    assert_eq!(index, VolumeIndex(0));
    assert_eq!(
        tree.child_of(NodeId::ROOT, MemberId(0)),
        Some((owner, VolumeIndex(1)))
    );
    drive_to_finish(&mut pipeline, &scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut delivered_files = std::collections::BTreeMap::new();
    super::harness::collect(&complete_dir, &complete_dir, &mut delivered_files);
    retire_pipeline_database(pipeline).await;
    assert_eq!(status, Some(JobStatus::Complete));
    assert_eq!(delivered_files.get(&delivered(MEMBER)), Some(&body));
}

// A RAR4 set whose volumes each carry a RAR3 recovery record, one article
// of the first volume served damaged: the graph rebuilds the sector from
// that volume's record and delivers the member whole.
#[tokio::test]
async fn a_rar4_set_with_recovery_records_and_a_damaged_article_is_repaired_from_the_record() {
    let body = body();
    let set = protected_rar4_store_set(MEMBER, &body, 2, 1);
    let scenario = Scenario::new(posts(set)).damaged(0, 2);
    let graph = run(&scenario).await;
    assert_delivers(&graph, MEMBER, &body);
    assert_eq!(
        graph.graph().rulings,
        vec![
            (
                "rar_record".to_string(),
                Some(RepairOutcome::Repaired { slices: 1 })
            ),
            ("rar_record".to_string(), Some(RepairOutcome::Clean)),
        ]
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A RAR4 `-hp` set whose volumes each carry a RAR3 recovery record, one
// article of the first volume served damaged, under `scenario`'s
// candidates: the store proves the password on the first enciphered
// header, the record reads under it and rebuilds the sector, and the member
// is delivered whole with nothing asked for again.
async fn assert_sealed_rar4_repaired_from_its_record(scenario: impl Fn(Scenario) -> Scenario) {
    let body = body();
    let set = protected_sealed_rar4_store_set(MEMBER, &body, 2, 1, SECRET);
    let graph = run(&scenario(Scenario::new(posts(set)).damaged(0, 2))).await;
    assert_delivers(&graph, MEMBER, &body);
    assert_eq!(
        graph.graph().rulings,
        vec![
            (
                "rar_record".to_string(),
                Some(RepairOutcome::Repaired { slices: 1 })
            ),
            ("rar_record".to_string(), Some(RepairOutcome::Clean)),
        ]
    );
    assert_volumes_complete(&graph);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

#[tokio::test]
async fn a_rar4_hp_set_with_recovery_records_and_a_damaged_article_is_repaired_under_its_password()
{
    assert_sealed_rar4_repaired_from_its_record(|scenario| scenario.password(SECRET)).await;
}

#[tokio::test]
async fn a_rar4_hp_set_with_recovery_records_is_repaired_under_the_right_candidate_after_a_wrong_one()
 {
    assert_sealed_rar4_repaired_from_its_record(|scenario| {
        scenario
            .global_password("quill-ember-wrong")
            .global_password(SECRET)
    })
    .await;
}

// No password: nothing proves a candidate, so the headers stay shut, the
// record is never announced, and the job fails for the password with
// nothing delivered.
#[tokio::test]
async fn a_rar4_hp_set_with_recovery_records_and_no_password_fails_for_the_password() {
    let body = body();
    let set = protected_sealed_rar4_store_set(MEMBER, &body, 2, 1, SECRET);
    let graph = run(&Scenario::new(posts(set)).damaged(0, 2)).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Password));
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    assert_eq!(kinds(&graph), ["rar_store"]);
    assert!(
        graph.graph().rulings.is_empty(),
        "{:?}",
        graph.graph().rulings
    );
    let delivery = &graph.graph().delivery;
    assert!(delivery.whole.is_empty(), "{delivery:?}");
    assert!(delivery.files.is_empty(), "{delivery:?}");
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A RAR5 `-hp` set whose volumes each carry a recovery record, under its
// password, the last article of the first volume wholly in the member's
// cipher bytes served damaged: the enciphered headers still open. The store
// opens them on the password and hands it to each volume's record; the
// first record rebuilds the damaged unit and the second finds its volume
// whole, so neither is a record that read nothing and the job is clean on
// what they checked: the member is delivered whole, nothing asked again.
#[tokio::test]
async fn a_rar5_hp_set_with_recovery_records_and_a_damaged_article_is_repaired_from_the_record() {
    let body = body();
    let set = protected_header_encrypted_store_set(MEMBER, &body, 2, SECRET, 1);
    let layout = unrar_rs::locate_recovery_record(&mut std::io::Cursor::new(&set[0].1), None)
        .unwrap()
        .unwrap();
    let in_data = (layout.protected_size / ARTICLE as u64 - 1) as u32;
    let scenario = Scenario::new(posts(set))
        .damaged(0, in_data)
        .password(SECRET);
    let graph = run(&scenario).await;
    assert_delivers(&graph, MEMBER, &body);
    assert_eq!(
        graph.graph().rulings,
        vec![
            (
                "rar_record".to_string(),
                Some(RepairOutcome::Repaired { slices: 1 })
            ),
            ("rar_record".to_string(), Some(RepairOutcome::Clean)),
        ]
    );
    assert_volumes_complete(&graph);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// One RAR4 volume whose record of one parity sector cannot rebuild two
// damaged sectors: the record rules past repair rather than waiting, and
// the job fails for the repair.
#[tokio::test]
async fn a_rar4_volume_damaged_past_what_its_recovery_record_covers_ends_impossible_not_stalled() {
    let body = payload(30 * ARTICLE + 7, 57);
    let set = protected_rar4_store_set(MEMBER, &body, 1, 1);
    let scenario = Scenario::new(posts(set)).damaged(0, 2).damaged(0, 20);
    let graph = run(&scenario).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert!(
        matches!(
            graph.graph().rulings[..],
            [(_, Some(RepairOutcome::Impossible { .. }))]
        ),
        "{:?}",
        graph.graph().rulings
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A RAR4 volume with its record beside a PAR2 set over both, one article
// of the first volume served damaged: the record repairs first, the set
// finds every volume whole.
#[tokio::test]
async fn a_rar4_volume_with_a_recovery_record_beside_par2_is_repaired_once_by_the_record_and_the_set_finds_it_whole()
 {
    let body = body();
    let scenario =
        Scenario::new(with_par2(protected_rar4_store_set(MEMBER, &body, 2, 1))).damaged(0, 2);
    let graph = run(&scenario).await;
    assert_delivers(&graph, MEMBER, &body);
    let mut rulings = graph.graph().rulings.clone();
    rulings.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        rulings,
        vec![
            ("par2".to_string(), Some(RepairOutcome::Clean)),
            (
                "rar_record".to_string(),
                Some(RepairOutcome::Repaired { slices: 1 })
            ),
            ("rar_record".to_string(), Some(RepairOutcome::Clean)),
        ]
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A five-volume set's member, `lead` beside it in the first volume when
// given, and of the third volume's articles one of the member's lost and,
// when `unreadable`, every one wholly inside its record: the first body
// whose third volume starts its record on an article boundary, so the
// record's own header is lost and the service header before it is not.
fn middle_volume_lost(lead: Option<&[u8]>, unreadable: bool) -> (Vec<u8>, Scenario) {
    const MIDDLE: u32 = 2;
    let article = ARTICLE as u64;
    let build = |body: &[u8]| match lead {
        Some(lead) => protected_store_set_led_by(("ember.mkv", lead), MEMBER, body, 5, 1),
        None => protected_store_set(MEMBER, body, 5, 1),
    };
    (0..10 * ARTICLE)
        .find_map(|extra| {
            let body = payload(30 * ARTICLE + extra, 61);
            let set = build(&body);
            let volume = &set[MIDDLE as usize].1;
            let layout = unrar_rs::locate_recovery_record(&mut std::io::Cursor::new(volume), None)
                .unwrap()
                .unwrap();
            if layout.record_offset % article != 0 {
                return None;
            }
            let first = (layout.record_offset / article) as u32;
            let last = ((layout.record_offset + layout.record_size) / article) as u32;
            let mut scenario = Scenario::new(posts(set)).lost(MIDDLE, 2);
            if unreadable {
                for ordinal in first..last {
                    scenario = scenario.lost(MIDDLE, ordinal);
                }
            }
            Some((body, scenario))
        })
        .unwrap()
}

// One file in a five-volume set, an article of the third volume lost and
// that volume's record unreadable. The record says nothing of its volume,
// nothing rebuilds the member, and nothing comes out whole: the job ends
// Failed, not Partial and not stalled.
#[tokio::test]
async fn one_file_whose_middle_volume_is_lost_and_its_record_unreadable_fails() {
    let (_, scenario) = middle_volume_lost(None, true);
    let graph = run(&scenario).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    let view = graph.graph();
    assert!(view.delivery.whole.is_empty(), "{:?}", view.delivery);
    assert_record_said_nothing(view);
}

// Every other volume's record ruled clean; the third's read nothing, so it
// gives no ruling and is no set the job reads: it vouched for none of its
// volume, whose damage stands.
fn assert_record_said_nothing(view: &GraphView) {
    assert_eq!(
        view.rulings,
        vec![("rar_record".to_string(), Some(RepairOutcome::Clean)); 4]
    );
    assert_eq!(view.claims["silver.horizon.part03.rar"], Claim::Damaged);
}

// The same shape with the record readable: it rebuilds the lost article and
// the file comes out whole.
#[tokio::test]
async fn one_file_whose_middle_volume_is_lost_and_its_record_readable_is_repaired() {
    let (body, scenario) = middle_volume_lost(None, false);
    let graph = run(&scenario).await;
    assert_delivers(&graph, MEMBER, &body);
    assert!(
        graph.graph().rulings.contains(&(
            "rar_record".to_string(),
            Some(RepairOutcome::Repaired { slices: 1 })
        )),
        "{:?}",
        graph.graph().rulings
    );
}

// Two files, the first whole in the first volume and the second across all
// five, an article of the third volume lost and its record unreadable. The
// first comes out whole and the second does not: the job ends Partial.
#[tokio::test]
async fn two_files_one_whole_beside_a_middle_volume_lost_with_its_record_unreadable_are_partial() {
    let lead = payload(3 * ARTICLE + 5, 62);
    let (_, scenario) = middle_volume_lost(Some(&lead), true);
    let graph = run(&scenario).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    let view = graph.graph();
    assert_eq!(
        view.delivery.whole,
        vec!["ember.mkv".to_string()],
        "{:?}",
        view.delivery
    );
    assert_record_said_nothing(view);
    assert!(
        graph.files[&delivered("ember.mkv")] == lead,
        "the whole file's bytes"
    );
}

// The same two files with the record readable: both come out whole.
#[tokio::test]
async fn two_files_beside_a_middle_volume_lost_with_its_record_readable_are_complete() {
    let lead = payload(3 * ARTICLE + 5, 62);
    let (body, scenario) = middle_volume_lost(Some(&lead), false);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert!(
        graph.files[&delivered("ember.mkv")] == lead,
        "the first file's bytes"
    );
    assert!(
        graph.files[&delivered(MEMBER)] == body,
        "the second file's bytes"
    );
}

// A stored RAR beside PAR2 with one article damaged: the set repairs, and
// the delivery follows from the job's own steps, never from a look the
// pipeline takes when it falls idle.
#[tokio::test]
async fn a_damaged_rar_store_under_par2_rules_and_delivers_without_an_idle_look() {
    let scenario =
        Scenario::new(with_par2(single_member_store_set(MEMBER, &body(), 1))).damaged(0, 2);
    let (outcomes, moved) = run_unprompted(&scenario).await;
    assert_eq!(outcomes, vec![Some(RepairOutcome::Repaired { slices: 1 })]);
    assert!(moved, "the delivery waited for an idle look");
}

// Articles of 60 bytes over a one-volume set: the one in the middle,
// `sent_back`, holds `MEMBER`'s bytes only.
const FRAME: usize = 60;

// One volume of `MEMBER`, enciphered under `password` when there is one,
// with `spoiled` the copy of the article at `sent_back` a server holds that
// is not the posted bytes, though it passes its own check. One set stands
// behind every copy: each enciphering draws its own salt.
fn sent_back_scenario(
    posted: &[(String, Vec<u8>)],
    password: Option<&str>,
    sent_back: u32,
    spoiled: bool,
) -> Scenario {
    let mut volumes = posted.to_vec();
    let at = sent_back as usize * FRAME;
    if spoiled {
        volumes[0].1[at..at + FRAME]
            .iter_mut()
            .for_each(|byte| *byte ^= 0x5a);
    }
    let scenario = Scenario::new(posts(volumes)).framed(FRAME);
    match password {
        Some(password) => scenario.password(password),
        None => scenario,
    }
}

// Whether `MEMBER` of the job's store is closed.
fn member_closed(pipeline: &Pipeline) -> bool {
    let tree = pipeline.graph_jobs[&JOB].tree();
    tree.child_of(weaver_graph::NodeId::ROOT, weaver_graph::MemberId(0))
        .and_then(|(store, _)| tree.namespace(store))
        .is_some_and(|namespace| {
            namespace
                .members()
                .any(|member| member.name == MEMBER && member.closed)
        })
}

// The articles come in order, `first`'s copy of each: every one with
// `closed`, so `MEMBER` closes, or else up to and with the one in the
// middle. Then that one is sent back for another copy, as the decoder sends
// back an article its neighbours refute, and `again`'s copy comes; then the
// rest. What the job delivers once it ends, and how it ends.
async fn sent_back_once(
    sealing: Sealing,
    first_spoiled: bool,
    closed: bool,
) -> (
    Option<JobStatus>,
    std::collections::BTreeMap<String, Vec<u8>>,
) {
    use super::refused_then_repaired::{drive_to_finish, serve_queued};
    use crate::pipeline::tests::pump_pipeline_runtime_queues;
    use weaver_server_base::jobs::ids::{NzbFileId, SegmentId};

    let body = body();
    let (posted, password) = match sealing {
        Sealing::Stored => (single_member_store_set(MEMBER, &body, 1), None),
        Sealing::Enciphered { keyed } => (
            encrypted_store_set(MEMBER, &body, 1, SECRET, Some(SECRET), keyed),
            Some(SECRET),
        ),
    };
    let articles = posted[0].1.len().div_ceil(FRAME) as u32;
    let sent_back = articles / 2;
    let first = sent_back_scenario(&posted, password, sent_back, first_spoiled);
    let again = sent_back_scenario(&posted, password, sent_back, !first_spoiled);
    let held = if closed { articles } else { sent_back + 1 };
    let (_temp, mut pipeline, complete_dir) = admit(&first).await;
    for ordinal in 0..held {
        serve_queued(&mut pipeline, &first, (0, ordinal)).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }
    assert_eq!(member_closed(&pipeline), closed);
    pipeline.graph_article_sent_back(SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: 0,
        },
        segment_number: sent_back,
    });
    serve_queued(&mut pipeline, &again, (0, sent_back)).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;
    for ordinal in held..articles {
        serve_queued(&mut pipeline, &again, (0, ordinal)).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }
    drive_to_finish(&mut pipeline, &again).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = std::collections::BTreeMap::new();
    super::harness::collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;
    (status, files)
}

// How the set's one member is posted: stored as it is, or enciphered, its
// checksum either the member's CRC32 or one keyed by the password.
#[derive(Clone, Copy)]
enum Sealing {
    Stored,
    Enciphered { keyed: bool },
}

// The copy that came first was not the article's bytes and is sent back
// before `MEMBER` closes, and the one sent for comes right: the member is
// checked over the bytes it holds, and comes out whole.
async fn a_wrong_copy_sent_back_is_replaced_and_proved(sealing: Sealing) {
    let (status, files) = sent_back_once(sealing, true, false).await;
    assert_eq!(status, Some(JobStatus::Complete), "{:?}", files.keys());
    assert!(
        files.get(&delivered(MEMBER)) == Some(&body()),
        "member bytes"
    );
}

// The copy that came first was right and `MEMBER` closed on it, and the one
// that comes after the send back is not: what the member's checksum proved
// of the first copy does not stand for the second, so `MEMBER` is not
// delivered as whole.
async fn a_wrong_copy_after_a_send_back_fails_the_member(sealing: Sealing) {
    let (status, files) = sent_back_once(sealing, false, true).await;
    assert!(
        !matches!(status, Some(JobStatus::Complete)),
        "{status:?} {:?}",
        files.keys()
    );
    assert!(
        files.get(&delivered(MEMBER)) != Some(&body()),
        "member whole"
    );
}

#[tokio::test]
async fn a_stored_rar_member_whose_wrong_article_was_sent_back_is_proved_on_its_right_copy() {
    a_wrong_copy_sent_back_is_replaced_and_proved(Sealing::Stored).await;
}

#[tokio::test]
async fn an_enciphered_rar_member_whose_wrong_article_was_sent_back_is_proved_on_its_right_copy() {
    a_wrong_copy_sent_back_is_replaced_and_proved(Sealing::Enciphered { keyed: false }).await;
}

#[tokio::test]
async fn a_keyed_rar_member_whose_wrong_article_was_sent_back_is_proved_on_its_right_copy() {
    a_wrong_copy_sent_back_is_replaced_and_proved(Sealing::Enciphered { keyed: true }).await;
}

#[tokio::test]
async fn a_stored_rar_member_whose_article_came_again_wrong_fails_its_checksum() {
    a_wrong_copy_after_a_send_back_fails_the_member(Sealing::Stored).await;
}

#[tokio::test]
async fn an_enciphered_rar_member_whose_article_came_again_wrong_fails_its_checksum() {
    a_wrong_copy_after_a_send_back_fails_the_member(Sealing::Enciphered { keyed: false }).await;
}

#[tokio::test]
async fn a_keyed_rar_member_whose_article_came_again_wrong_fails_its_checksum() {
    a_wrong_copy_after_a_send_back_fails_the_member(Sealing::Enciphered { keyed: true }).await;
}
