// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::pipeline::extraction::ExtractionFailure;
use weaver_graph::recovery::RepairOutcome;
use weaver_graph::slots::AskerId;
use weaver_graph::{ByteRange, Kind, Member, MemberId, Namespace, NodeId, Provenance, Why};

use super::claims::{Claim, ClaimedMember, refused};
use super::{
    Delivery, RecoverySummary, RepairSlots, RootJob, SetSummary, Step, drive, failure, shown_status,
};
use crate::history::HistoryFailureKind;
use crate::jobs::ids::{JobId, NzbFileId};
use crate::jobs::model::{
    DownloadState, JobStatus, PostState, RunState, runtime_lanes_from_status_snapshot,
};
use crate::pipeline::Pipeline;
use crate::pipeline::PipelineEvent;
use crate::pipeline::download::queue::DownloadQueue;
use crate::pipeline::graph::root::phase::{self, RootPhase};
use crate::pipeline::tests::{
    hear_graph_mail, insert_active_job, job_status_for_assert, new_direct_pipeline,
    standalone_job_spec, submit_decoded_segment,
};

// A graph job's reads, prepared by the test: a namespace, a summary, and the
// outcomes each repair pass reports. Its phase derives from what the last
// pass ruled, as the job's does from its objects.
struct Prepared {
    namespace: Namespace,
    summary: RecoverySummary,
    outcomes: Vec<Vec<RepairOutcome>>,
    ruled: Vec<RepairOutcome>,
    extracting: bool,
    repairs: usize,
}

impl Prepared {
    fn new(namespace: Namespace, summary: RecoverySummary) -> Self {
        Self {
            namespace,
            summary,
            outcomes: Vec::new(),
            ruled: Vec::new(),
            extracting: false,
            repairs: 0,
        }
    }

    fn repairing_to(mut self, outcomes: Vec<RepairOutcome>) -> Self {
        self.outcomes.push(outcomes);
        self
    }
}

impl RootJob for Prepared {
    fn phase(&self) -> RootPhase {
        let activity = phase::activity(
            self.summary
                .sets
                .iter()
                .enumerate()
                .map(|(at, set)| self.ruled.get(at).copied().or(set.outcome)),
            false,
        );
        RootPhase::derive(false, activity, self.extracting)
    }

    fn namespace(&self, _: NodeId) -> Option<&Namespace> {
        Some(&self.namespace)
    }

    fn recovery_summary(&self) -> RecoverySummary {
        self.summary.clone()
    }

    fn repair(&mut self) -> Vec<RepairOutcome> {
        let outcomes = self.outcomes.get(self.repairs).cloned().unwrap_or_default();
        self.repairs += 1;
        self.ruled.clone_from(&outcomes);
        outcomes
    }

    fn unpacked(&self) -> bool {
        false
    }

    fn refusal(&self) -> Option<&ExtractionFailure> {
        None
    }

    fn repair_out(&self) -> bool {
        false
    }
}

// A member: its name, its length, and the runs it holds with how each came.
type MemberRuns<'a> = (&'a str, u64, &'a [(u64, u64, Provenance)]);

fn namespace(members: &[MemberRuns]) -> Namespace {
    let mut namespace = Namespace::new();
    for (name, length, runs) in members {
        let id = namespace.announce(name, Some(*length));
        let member = namespace.get_mut(id).unwrap();
        for (start, end, provenance) in *runs {
            member
                .coverage
                .write(ByteRange::new(*start, *end), *provenance);
        }
    }
    namespace
}

fn set(kind: Kind, name: &str) -> SetSummary {
    SetSummary {
        node: NodeId(1),
        kind,
        namespace: NodeId::ROOT,
        name: name.to_string(),
        outcome: None,
    }
}

fn ruled(mut set: SetSummary, outcome: RepairOutcome) -> SetSummary {
    set.outcome = Some(outcome);
    set
}

fn summary(sets: Vec<SetSummary>) -> RecoverySummary {
    RecoverySummary { sets }
}

fn member(namespace: &Namespace, id: u32) -> &Member {
    namespace.get(MemberId(id)).unwrap()
}

// The job as the completion gate sees it once the download pass is over.
fn settle_download_state(pipeline: &mut Pipeline, job_id: JobId) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
    state.status = JobStatus::Downloading;
    state.refresh_runtime_lanes_from_status();
}

fn sfv_line_with_crc(name: &str, crc32: u32) -> String {
    format!("{name} {crc32:08x}\n")
}

// A payload and a listing that agrees with it or not.
async fn job_with_a_listing(pipeline: &mut Pipeline, job_id: JobId, agrees: bool) {
    let payload: Vec<u8> = (0..96u32).map(|value| (value % 251) as u8).collect();
    let crc = par2_rs::checksum::crc32(&payload);
    let listed = if agrees { crc } else { crc ^ 0xffff_ffff };
    let listing = sfv_line_with_crc("ember.field.bin", listed).into_bytes();
    let spec = standalone_job_spec(
        "Ember Field",
        &[
            ("ember.field.bin".to_string(), payload.len() as u32),
            ("ember.field.sfv".to_string(), listing.len() as u32),
        ],
    );
    insert_active_job(pipeline, job_id, spec).await;
    let file = |file_index| NzbFileId { job_id, file_index };
    submit_decoded_segment(
        pipeline,
        file(1),
        0,
        0,
        &listing,
        "ember.field.sfv",
        Some(par2_rs::checksum::crc32(&listing)),
    )
    .await;
    submit_decoded_segment(
        pipeline,
        file(0),
        0,
        0,
        &payload,
        "ember.field.bin",
        Some(crc),
    )
    .await;
    hear_graph_mail(pipeline, job_id).await;
}

#[test]
fn each_phase_shows_as_its_status_and_lanes() {
    let lanes = |phase, current: &JobStatus| {
        shown_status(phase, current).map(|status| {
            let (download, post, run) = runtime_lanes_from_status_snapshot(&status);
            (status, download, post, run)
        })
    };
    // A repair that wants more bytes takes the job back to downloading.
    assert_eq!(
        lanes(RootPhase::Downloading, &JobStatus::Repairing),
        Some((
            JobStatus::Downloading,
            DownloadState::Downloading,
            PostState::Idle,
            RunState::Active
        ))
    );
    let lanes = |phase| lanes(phase, &JobStatus::Queued);
    assert_eq!(
        lanes(RootPhase::Verifying),
        Some((
            JobStatus::Verifying,
            DownloadState::Complete,
            PostState::Verifying,
            RunState::Active
        ))
    );
    assert_eq!(
        lanes(RootPhase::Repairing),
        Some((
            JobStatus::Repairing,
            DownloadState::Complete,
            PostState::Repairing,
            RunState::Active
        ))
    );
    assert_eq!(
        lanes(RootPhase::Extracting),
        Some((
            JobStatus::Extracting,
            DownloadState::Complete,
            PostState::Extracting,
            RunState::Active
        ))
    );
    // Done is finalisation's, and a pause or a failure is whoever set it.
    assert_eq!(lanes(RootPhase::Done), None);
    assert_eq!(shown_status(RootPhase::Repairing, &JobStatus::Paused), None);
    assert_eq!(shown_status(RootPhase::Verifying, &JobStatus::Moving), None);
    // No change is no report.
    assert_eq!(
        shown_status(RootPhase::Verifying, &JobStatus::Verifying),
        None
    );
    // A queued job starts downloading at its first dispatch, not here.
    assert_eq!(
        shown_status(RootPhase::Downloading, &JobStatus::Queued),
        None
    );
}

#[test]
fn the_drive_walks_the_phases_the_root_reports() {
    let names = || namespace(&[("ember.bin", 100, &[(0, 100, Provenance::Damaged)])]);
    let damaged = || summary(vec![set(Kind::Par2, "ember.par2")]);
    let mut slots = RepairSlots::default();

    // Repaired: delivered, and the root is done.
    let mut job =
        Prepared::new(names(), damaged()).repairing_to(vec![RepairOutcome::Repaired { slices: 1 }]);
    assert_eq!(drive(&mut job, &mut slots, AskerId(1), 1), Step::Deliver);
    assert_eq!((job.repairs, job.phase()), (1, RootPhase::Done));

    // Waiting on recovery bytes: the root fetches them, and no repair is
    // out until they come.
    let mut job = Prepared::new(names(), damaged()).repairing_to(vec![RepairOutcome::Waiting]);
    assert_eq!(drive(&mut job, &mut slots, AskerId(2), 1), Step::Wait);
    assert_eq!(job.phase(), RootPhase::Downloading);

    // Not ruled yet: the root verifies.
    let job = Prepared::new(names(), damaged());
    assert_eq!(job.phase(), RootPhase::Verifying);

    // An object below the root still producing members holds delivery.
    let mut job = Prepared::new(names(), RecoverySummary::default());
    job.extracting = true;
    assert_eq!(drive(&mut job, &mut slots, AskerId(3), 1), Step::Wait);
    assert_eq!(job.phase(), RootPhase::Extracting);
    assert_eq!(job.repairs, 0, "a settled namespace runs no repair");
}

#[test]
fn a_repair_waiting_on_recovery_yields_its_slot_to_the_next_job() {
    let names = || namespace(&[("ember.bin", 100, &[(0, 100, Provenance::Damaged)])]);
    let damaged = || summary(vec![set(Kind::Par2, "ember.par2")]);
    let mut slots = RepairSlots::default();
    let mut first = Prepared::new(names(), damaged())
        .repairing_to(vec![RepairOutcome::Waiting])
        .repairing_to(vec![RepairOutcome::Waiting])
        .repairing_to(vec![RepairOutcome::Repaired { slices: 1 }]);
    let mut second =
        Prepared::new(names(), damaged()).repairing_to(vec![RepairOutcome::Repaired { slices: 1 }]);

    // Alone, a waiting repair keeps the slot no one else wants.
    assert_eq!(drive(&mut first, &mut slots, AskerId(1), 1), Step::Wait);
    assert_eq!(drive(&mut second, &mut slots, AskerId(2), 1), Step::Wait);
    assert_eq!(second.repairs, 0, "the second queues behind the holder");

    // Waiting again with someone queued, it hands the slot over.
    assert_eq!(drive(&mut first, &mut slots, AskerId(1), 1), Step::Wait);
    assert_eq!(slots.take_woken(), vec![AskerId(2)]);
    assert_eq!(drive(&mut second, &mut slots, AskerId(2), 1), Step::Deliver);
    assert_eq!(slots.take_woken(), vec![AskerId(1)]);
    assert_eq!(drive(&mut first, &mut slots, AskerId(1), 1), Step::Deliver);
    assert_eq!(first.repairs, 3);
}

#[test]
fn claims_read_complete_damaged_and_missing_off_the_namespace() {
    let names = namespace(&[
        ("ember.bin", 100, &[(0, 100, Provenance::Fresh)]),
        (
            "ember.rebuilt",
            100,
            &[(0, 50, Provenance::Fresh), (50, 100, Provenance::Repaired)],
        ),
        (
            "ember.marked",
            100,
            &[(0, 60, Provenance::Fresh), (60, 100, Provenance::Damaged)],
        ),
        ("ember.short", 100, &[(0, 30, Provenance::Fresh)]),
    ]);
    assert_eq!(Claim::of(member(&names, 0)), Claim::Complete);
    assert_eq!(Claim::of(member(&names, 1)), Claim::Complete);
    assert_eq!(Claim::of(member(&names, 2)), Claim::Damaged { bytes: 40 });
    assert_eq!(Claim::of(member(&names, 3)), Claim::Missing { bytes: 70 });

    let mut unknown = Namespace::new();
    unknown.announce("ember.unknown", None);
    assert_eq!(Claim::of(member(&unknown, 0)), Claim::Missing { bytes: 0 });

    // A complete member owes nothing, a short one what its articles lost.
    let mut row = ClaimedMember::read(member(&names, 0), false);
    row.lost_bytes = 64;
    assert_eq!(row.failed_bytes(), 0);
    let mut row = ClaimedMember::read(member(&names, 3), false);
    row.lost_bytes = 64;
    assert_eq!(row.failed_bytes(), 64);
    assert!(row.evidence, "thirty of a hundred bytes is a delivery");
    // A damaged one owes its damage, retained with nothing lost or beside
    // a smaller loss, and a larger loss as it stands.
    let mut row = ClaimedMember::read(member(&names, 2), false);
    assert_eq!(row.failed_bytes(), 40);
    row.lost_bytes = 10;
    assert_eq!(row.failed_bytes(), 40);
    row.lost_bytes = 64;
    assert_eq!(row.failed_bytes(), 64);
}

#[test]
fn only_a_payload_that_delivered_nothing_is_refused() {
    let names = namespace(&[
        ("ember.bin", 100, &[(0, 5, Provenance::Fresh)]),
        ("ember.dat", 100, &[(0, 100, Provenance::Damaged)]),
        ("ember.nfo", 100, &[]),
        ("ember.mkv", 100, &[(0, 100, Provenance::Fresh)]),
    ]);
    let row = |id, furniture| ClaimedMember::read(member(&names, id), furniture);

    let none = Delivery::default();
    let nothing = vec![row(0, false), row(1, false), row(2, true)];
    let refused_names: Vec<_> = refused(&nothing, &none)
        .iter()
        .map(|row| row.name.clone())
        .collect();
    assert_eq!(refused_names, vec!["ember.bin", "ember.dat"]);

    // Beside a delivered sibling the same holes ship as damage.
    let delivered = vec![row(0, false), row(1, false), row(2, true), row(3, false)];
    assert!(refused(&delivered, &none).is_empty());

    // So they do beside a file unpacked whole from one of them.
    let unpacked = Delivery {
        whole: vec!["ember.inner".to_string()],
        ..Delivery::default()
    };
    assert!(refused(&nothing, &unpacked).is_empty());

    // Furniture neither fails a job nor delivers one.
    assert!(refused(&[row(2, true)], &none).is_empty());
    assert_eq!(refused(&[row(2, true), row(0, false)], &none).len(), 1);
}

#[test]
fn the_scripts_are_told_verified_only_when_every_object_vouched() {
    assert!(!RecoverySummary::default().verified());
    assert!(!summary(vec![set(Kind::Par2, "a")]).verified());
    assert!(summary(vec![ruled(set(Kind::Par2, "a"), RepairOutcome::Clean)]).verified());
    assert!(!summary(vec![ruled(set(Kind::Par2, "a"), RepairOutcome::Waiting)]).verified());
}

#[test]
fn an_impossible_repair_fails_in_the_recovery_failure_shape() {
    let names = namespace(&[
        ("ember.bin", 100, &[(0, 100, Provenance::Damaged)]),
        ("ember.dat", 100, &[(0, 100, Provenance::Fresh)]),
    ]);
    let mut job =
        Prepared::new(names, summary(vec![set(Kind::Par2, "ember.par2")])).repairing_to(vec![
            RepairOutcome::Impossible {
                damaged: 3,
                reachable: 1,
            },
        ]);
    let mut slots = RepairSlots::default();
    // The job delivers; what it ends as is read off its claims.
    assert_eq!(drive(&mut job, &mut slots, AskerId(1), 1), Step::Deliver);
    assert_eq!(
        failure(&job.summary, &job.ruled, &job).map(|failure| failure.message),
        Some(
            "PAR2 recovery failed for 1 set(s): ember.par2 (ember.bin): \
             not repairable: 3 damaged slices, only 1 recovery blocks reachable"
                .to_string()
        )
    );
    // The slot came back.
    assert!(slots.admit(AskerId(2), 1).is_some());
}

#[test]
fn a_checking_object_never_overrules_one_that_repaired() {
    // The repair left the member whole.
    let names = namespace(&[("ember.bin", 100, &[(0, 100, Provenance::Repaired)])]);
    let mut job = Prepared::new(
        names,
        summary(vec![
            set(Kind::Sfv, "ember.sfv"),
            set(Kind::Par2, "ember.par2"),
            // Nor does a second listing, whatever it named.
            set(Kind::Sfv, "stray.sfv"),
        ]),
    )
    .repairing_to(vec![
        RepairOutcome::Impossible {
            damaged: 1,
            reachable: 0,
        },
        RepairOutcome::Repaired { slices: 1 },
        RepairOutcome::Impossible {
            damaged: 1,
            reachable: 0,
        },
    ]);
    let mut slots = RepairSlots::default();
    assert_eq!(drive(&mut job, &mut slots, AskerId(1), 1), Step::Deliver);
}

#[test]
fn a_repair_of_one_set_never_excuses_another() {
    let names = namespace(&[
        ("ember.bin", 100, &[(0, 100, Provenance::Repaired)]),
        (
            "ember.dat",
            100,
            &[(0, 40, Provenance::Damaged), (40, 100, Provenance::Fresh)],
        ),
    ]);
    let mut job = Prepared::new(
        names,
        summary(vec![
            set(Kind::Par2, "ember.par2"),
            set(Kind::Par2, "ember.dat.par2"),
        ]),
    )
    .repairing_to(vec![
        RepairOutcome::Repaired { slices: 1 },
        RepairOutcome::Impossible {
            damaged: 2,
            reachable: 0,
        },
    ]);
    let mut slots = RepairSlots::default();
    assert_eq!(drive(&mut job, &mut slots, AskerId(1), 1), Step::Deliver);
    assert_eq!(
        failure(&job.summary, &job.ruled, &job).map(|failure| failure.message),
        Some(
            "PAR2 recovery failed for 1 set(s): ember.dat.par2 (ember.dat): \
             not repairable: 2 damaged slices, only 0 recovery blocks reachable"
                .to_string()
        )
    );
}

// The one derivation a job with nothing left to wait for ends by: nothing
// short, short beside whole, or nothing whole.
#[test]
fn a_delivery_ends_complete_partial_or_failed_by_its_short_files() {
    let names = |list: &[&str]| list.iter().map(ToString::to_string).collect::<Vec<_>>();
    let delivery = |whole: &[&str], damaged: &[&str], unrepaired: &[&str]| Delivery {
        whole: names(whole),
        damaged: names(damaged),
        unrepaired: names(unrepaired),
        withheld: Vec::new(),
        files: Vec::new(),
    };
    let error = "PAR2 recovery failed for 1 set(s)";
    assert_eq!(
        delivery(&["ember.bin"], &[], &[]).status(None),
        JobStatus::Complete
    );
    assert_eq!(
        delivery(&["ember.bin"], &[], &["ember.dat"]).status(Some(&failed(error))),
        JobStatus::Partial
    );
    assert_eq!(
        delivery(&["ember.bin"], &["ember.dat"], &[]).status(None),
        JobStatus::Partial
    );
    // Nothing is whole: the job failed, whatever ships as it stands.
    assert_eq!(
        delivery(&[], &["ember.dat"], &[]).status(None),
        JobStatus::Failed {
            error: "no file was delivered whole; 1 short: ember.dat".to_string()
        }
    );
    assert_eq!(
        delivery(&[], &["ember.dat"], &["ember.idx"]).status(None),
        JobStatus::Failed {
            error: "no file was delivered whole; 2 short: ember.dat, ember.idx".to_string()
        }
    );
    assert_eq!(
        delivery(&[], &[], &["ember.dat"]).status(Some(&failed(error))),
        JobStatus::Failed {
            error: error.to_string()
        }
    );
    assert_eq!(
        delivery(&[], &["ember.dat"], &[]).status(Some(&failed(error))),
        JobStatus::Failed {
            error: error.to_string()
        }
    );
}

// A member withheld by its object's refusal is neither whole nor short:
// nothing whole beside it fails the job, worded by the refusal, one wording
// for a password whether it shut the bytes or the headers.
#[test]
fn a_delivery_with_a_withheld_member_fails_worded_by_the_refusal() {
    let withheld = |whole: &[&str], withheld: &[(&str, Why)]| Delivery {
        whole: whole.iter().map(ToString::to_string).collect(),
        withheld: withheld
            .iter()
            .map(|(name, why)| (name.to_string(), *why))
            .collect(),
        ..Delivery::default()
    };
    let password = "WEAVER_PASSWORD_REQUIRED: lantern.rar needs a password and none was given";
    for why in [Why::Encrypted, Why::EncryptedHeader] {
        assert_eq!(
            withheld(&[], &[("lantern.rar", why)]).status(None),
            JobStatus::Failed {
                error: password.to_string()
            }
        );
    }
    assert_eq!(
        withheld(
            &[],
            &[
                ("lantern.part1.rar", Why::EncryptedHeader),
                ("lantern.part2.rar", Why::Encrypted),
                ("ember.7z", Why::Unmapped),
            ]
        )
        .withheld_error()
        .as_deref(),
        Some(
            "WEAVER_PASSWORD_REQUIRED: lantern.part1.rar, lantern.part2.rar needs a password and none was given; \
             ember.7z never placed every member it named"
        )
    );
    assert_eq!(
        withheld(&["ember.bin"], &[("lantern.rar", Why::Encrypted)]).status(None),
        JobStatus::Partial
    );
    // A recovery object's failure is the one that left the job short.
    let error = "PAR2 recovery failed for 1 set(s)";
    assert_eq!(
        withheld(&[], &[("lantern.rar", Why::Encrypted)]).status(Some(&failed(error))),
        JobStatus::Failed {
            error: error.to_string()
        }
    );
}

#[tokio::test]
async fn a_job_with_a_listing_moves_to_complete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(91_202);
    job_with_a_listing(&mut pipeline, job_id, true).await;
    // The last article may already have advanced the job to its move.
    if !pipeline.inflight_moves.contains(&job_id) {
        settle_download_state(&mut pipeline, job_id);
        pipeline.check_job_completion(job_id).await;
    }

    if let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) {
        panic!("the graph job failed: {error}");
    }
    assert!(pipeline.inflight_moves.contains(&job_id));
    let record = &pipeline.terminal_reconciliations[&job_id];
    assert_eq!((record.failed_bytes, record.health), (0, 1000));
}

// The listing's own object rules for a graph job, and its failure reads as a
// recovery failure does today, at the repair stage.
#[tokio::test]
async fn a_disagreeing_listing_fails_a_graph_job_through_its_object() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(91_208);
    job_with_a_listing(&mut pipeline, job_id, false).await;
    if pipeline.jobs.contains_key(&job_id) {
        settle_download_state(&mut pipeline, job_id);
        pipeline.check_job_completion(job_id).await;
    }

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed {
            error: "SFV recovery failed for 1 set(s): ember.field.sfv (ember.field.bin): \
                    not repairable: \
                    1 damaged slices, only 0 recovery blocks reachable"
                .to_string()
        })
    );
    while !matches!(
        events.recv().await.expect("pipeline event channel closed"),
        PipelineEvent::JobFailed { job_id: failed, .. } if failed == job_id
    ) {}
    let states = pipeline.db.nzbget_history_states(&[job_id.0]).unwrap();
    assert_eq!(
        states[&job_id.0].failure_kind,
        Some(HistoryFailureKind::Repair)
    );
}

#[tokio::test]
async fn a_graph_census_keeps_a_damaged_member_and_refuses_an_empty_delivery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;

    // Nothing of the payload ever arrived: refused as never delivered.
    let job_id = JobId(91_206);
    let spec = standalone_job_spec("Ember Empty", &[("ember.bin".to_string(), 64)]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let Err(error) = pipeline.reconcile_terminal_delivery(job_id) else {
        panic!("an empty graph delivery is refused");
    };
    assert_eq!(
        error,
        "1 payload file(s) were never delivered and no verification, repair or discard \
         accounts for them (64 declared bytes): ember.bin"
    );
}

// A recovery failure as the rulings word it, naming no object.
fn failed(message: &str) -> super::Failure {
    super::Failure {
        objects: Vec::new(),
        message: message.to_owned(),
        rejection: None,
    }
}
