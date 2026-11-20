// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Each rule of a pass over the torrents, asked about hand-built torrents
// with no service and no session: every branch of what it decides.

use super::*;
use crate::jobs::handle::HoldSet;
use crate::torrents::FilePriority;
use crate::torrents::service::disk_room::DiskHeld;
use crate::torrents::service::live::Live;
use crate::torrents::service::policy::{
    self, Checkpoint, Context, Goals, Intent, Limits, Picking, Picks, Policy, QueueOrder, Reading,
    RunCommand, Seats, Standing, Turns,
};
use crate::torrents::status::{Queued, Turn};
use std::collections::BTreeSet;
use weaver_torrent::limits::LimitsConfig;

// A torrent of job `job_id`, as `record` keeps it, with nothing read from a
// session.
fn torrent(record: TorrentRecord) -> Live {
    Live::new(
        record,
        TorrentCounters::default(),
        ResumePeers::default(),
        EPOCH_MS,
    )
}

// A seed whose seeding goal is a ratio of `ratio`, then `action`.
fn seed(ratio: f32, action: SeedingAction) -> Live {
    let (mut record, _, _) = chain();
    record.completed_at_ms = Some(EPOCH_MS);
    record.policy_override = Some(SeedingPolicy {
        seed_time_counting: Default::default(),
        ratio_goal: Leaf::Set(ratio),
        seed_time_goal_minutes: Leaf::Inherit,
        inactive_goal_minutes: Leaf::Inherit,
        mode: Leaf::Set(SeedingGoalMode::Any),
        action: Leaf::Set(action),
    });
    torrent(record)
}

fn downloading() -> Live {
    torrent(chain().0)
}

// What the pass's context holds besides the settings, for one question.
#[derive(Default)]
struct Ask {
    downloads_only: bool,
    unsaved: BTreeSet<JobId>,
    seats: Seats,
}

// What `policy` decides for `torrent` under `settings`, as of `EPOCH_MS`.
fn decide(
    policy: &dyn Policy,
    torrent: &Live,
    settings: &TorrentSettings,
    ask: &Ask,
) -> Vec<Intent> {
    let holds = HoldSet::default();
    let cx = Context {
        settings,
        rules: &[],
        holds: &holds,
        now: EPOCH_MS,
        at_once: false,
        downloads_only: ask.downloads_only,
        unsaved: &ask.unsaved,
        seats: &ask.seats,
    };
    let mut out = Vec::new();
    policy.decide(torrent, &cx, &mut out);
    out
}

fn settings() -> TorrentSettings {
    chain().2
}

// Reading: one with no session is read and nothing else.
#[test]
fn reading_a_torrent_with_no_session_only_takes_its_reading() {
    let settings = settings();
    let mut torrent = downloading();
    torrent.record.seed_mode = true;
    assert_eq!(
        decide(&Reading, &torrent, &settings, &Ask::default()),
        [Intent::TakeReading]
    );
}

// Seed mode: none kept, nothing; ended in the session, left; still in it,
// the pieces checked since they were last kept, read only then, and copied
// only when more passed than were kept.
#[test]
fn seed_mode_is_left_once_the_session_leaves_it_and_its_checks_are_kept() {
    type Bits = (usize, Vec<u8>);
    let count = |bits: &Bits| bits.0;
    let copy = |bits: &Bits| bits.1.clone();
    let uncopied = |_: &Bits| -> Vec<u8> { panic!("copied with no more pieces passed") };
    let unread = || -> Option<Bits> { panic!("read with no seed mode kept") };
    assert_eq!(
        policy::seed_mode_step(false, true, unread, count, copy, 0),
        None
    );
    assert_eq!(
        policy::seed_mode_step(false, false, unread, count, copy, 0),
        None
    );
    let unread = || -> Option<Bits> { panic!("read once seed mode ended") };
    assert_eq!(
        policy::seed_mode_step(true, false, unread, count, copy, 3),
        Some(Intent::LeaveSeedMode)
    );
    assert_eq!(
        policy::seed_mode_step(true, true, || None, count, copy, 0),
        None
    );
    let bits = || Some((2, vec![0b1100_0000]));
    assert_eq!(
        policy::seed_mode_step(true, true, bits, count, copy, 1),
        Some(Intent::KeepVerified {
            pieces: vec![0b1100_0000],
            count: 2
        })
    );
    assert_eq!(
        policy::seed_mode_step(true, true, bits, count, uncopied, 2),
        None
    );
    assert_eq!(
        policy::seed_mode_step(true, true, bits, count, uncopied, 3),
        None
    );
}

// Checkpoint: only a torrent whose checkpoint is behind.
#[test]
fn a_checkpoint_is_written_only_when_it_is_behind() {
    let settings = settings();
    let torrent = downloading();
    assert_eq!(
        decide(&Checkpoint, &torrent, &settings, &Ask::default()),
        []
    );
    let behind = Ask {
        unsaved: BTreeSet::from([torrent.record.job_id]),
        ..Ask::default()
    };
    let other = Ask {
        unsaved: BTreeSet::from([JobId(99)]),
        ..Ask::default()
    };
    assert_eq!(decide(&Checkpoint, &torrent, &settings, &other), []);
    assert_eq!(
        decide(&Checkpoint, &torrent, &settings, &behind),
        [Intent::Checkpoint]
    );
}

// Goals: a seed whose goal is met does what its policy says, each action.
#[test]
fn a_seed_whose_goal_is_met_does_what_its_policy_says() {
    let settings = settings();
    for action in [
        SeedingAction::Stop,
        SeedingAction::Remove,
        SeedingAction::RemoveWithData,
    ] {
        assert_eq!(
            decide(&Goals, &seed(0.0, action), &settings, &Ask::default()),
            [Intent::GoalMet(action)]
        );
    }
}

// Goals: nothing for a seed with a goal left, a forced seed, a stopped seed
// or one still downloading.
#[test]
fn no_goal_is_met_for_a_goal_left_a_forced_or_stopped_seed_or_a_download() {
    let settings = settings();
    assert_eq!(
        decide(
            &Goals,
            &seed(2.0, SeedingAction::Stop),
            &settings,
            &Ask::default()
        ),
        []
    );
    let mut forced = seed(0.0, SeedingAction::Stop);
    forced.record.forced = true;
    assert_eq!(decide(&Goals, &forced, &settings, &Ask::default()), []);
    let mut stopped = seed(0.0, SeedingAction::Stop);
    stopped.record.kept = KeptIntent::Intent(TorrentIntent::Stopped);
    assert_eq!(decide(&Goals, &stopped, &settings, &Ask::default()), []);
    assert_eq!(
        decide(&Goals, &downloading(), &settings, &Ask::default()),
        []
    );
}

// Goals, winding down to the downloads alone: one done stops for it, forced
// or with a goal left; a seed meeting its goal ends by its goal; one still
// downloading, or stopped, is left.
#[test]
fn winding_down_to_the_downloads_stops_those_done_unless_their_goal_is_met() {
    let settings = settings();
    let only = &Ask {
        downloads_only: true,
        ..Ask::default()
    };
    assert_eq!(
        decide(&Goals, &seed(2.0, SeedingAction::Stop), &settings, only),
        [Intent::WindDown]
    );
    let mut forced = seed(0.0, SeedingAction::Stop);
    forced.record.forced = true;
    assert_eq!(decide(&Goals, &forced, &settings, only), [Intent::WindDown]);
    let mut in_seed_mode = downloading();
    in_seed_mode.record.seed_mode = true;
    in_seed_mode.record.policy_override = seed(2.0, SeedingAction::Stop).record.policy_override;
    assert_eq!(
        decide(&Goals, &in_seed_mode, &settings, only),
        [Intent::WindDown]
    );
    assert_eq!(
        decide(&Goals, &seed(0.0, SeedingAction::Remove), &settings, only),
        [Intent::GoalMet(SeedingAction::Remove)]
    );
    assert_eq!(decide(&Goals, &downloading(), &settings, only), []);
    let mut stopped = seed(2.0, SeedingAction::Stop);
    stopped.record.kept = KeptIntent::Intent(TorrentIntent::Stopped);
    assert_eq!(decide(&Goals, &stopped, &settings, only), []);
}

// Standing: how the queue stands the torrent, as the queue reads it.
#[test]
fn a_torrents_standing_is_how_the_queue_stands_it() {
    let settings = settings();
    for torrent in [downloading(), seed(0.0, SeedingAction::Stop)] {
        assert_eq!(
            decide(&Standing, &torrent, &settings, &Ask::default()),
            [Intent::Stand(queued(&settings, &[], &torrent, EPOCH_MS))]
        );
    }
}

fn standing(job_id: u64, seeding: bool, forced: bool, paused: bool) -> Queued {
    Queued {
        job_id: JobId(job_id),
        seeding,
        forced,
        paused,
        runs_through_schedule: false,
    }
}

// Turns: a torrent takes the seat the pass gave it, and none without one.
#[test]
fn a_torrent_takes_the_seat_the_pass_gave_it() {
    let settings = settings();
    let torrent = downloading();
    let mut order = QueueOrder::default();
    let (seats, _) = policy::seat(
        vec![(0, standing(1, false, false, false))],
        &mut order,
        1,
        &HoldSet::default(),
    );
    assert_eq!(
        decide(
            &Turns,
            &torrent,
            &settings,
            &Ask {
                seats,
                ..Ask::default()
            }
        ),
        [Intent::TakeTurn {
            queued: standing(1, false, false, false),
            turn: Turn {
                active: true,
                rank: Some(1)
            }
        }]
    );
    assert_eq!(decide(&Turns, &torrent, &settings, &Ask::default()), []);
}

// The seats are worked out in queue order, by place then job: the first
// downloads take the slots and ranks, seeds take neither and forced ones
// run past the limit. The queue's end is the last in that order.
#[test]
fn the_seats_follow_the_queue_order_by_place_then_job() {
    let mut order = QueueOrder::default();
    // By job: places 3, 1, 1, 0, 2.
    let standings = vec![
        (3, standing(1, false, false, false)),
        (1, standing(2, false, false, false)),
        (1, standing(3, true, false, false)),
        (0, standing(4, false, false, true)),
        (2, standing(5, false, true, false)),
    ];
    let (seats, end) = policy::seat(standings.clone(), &mut order, 1, &HoldSet::default());
    let turn = |active, rank| Turn { active, rank };
    let of = |job_id| seats.of(JobId(job_id)).unwrap().1;
    // Queue order: 4 (paused), 2, 3 (seed), 5 (forced), 1.
    assert_eq!(of(4), turn(false, Some(1)));
    assert_eq!(of(2), turn(true, Some(2)));
    assert_eq!(of(3), turn(true, None));
    assert_eq!(of(5), turn(true, Some(3)));
    assert_eq!(of(1), turn(false, Some(4)));
    assert_eq!(end.last, Some((3, JobId(1))));
    // The same as the queue's own walk over that order.
    let in_order: Vec<Queued> = [4, 2, 3, 5, 1]
        .iter()
        .map(|job_id| seats.of(JobId(*job_id)).unwrap().0)
        .collect();
    let (turns, slots) = status::turns(&in_order, 1, &HoldSet::default());
    assert_eq!(
        turns,
        [4, 2, 3, 5, 1]
            .iter()
            .map(|job_id| of(*job_id))
            .collect::<Vec<_>>()
    );
    assert_eq!(slots, end.slots);
    assert_eq!(seats.of(JobId(6)), None);
}

// The order is kept while no place moves, and worked out anew once one
// does.
#[test]
fn the_queue_order_is_kept_until_a_place_moves() {
    let mut order = QueueOrder::default();
    assert_eq!(order.of(vec![(2, JobId(1)), (1, JobId(2))]), [1, 0]);
    assert_eq!(order.of(vec![(2, JobId(1)), (1, JobId(2))]), [1, 0]);
    assert_eq!(order.of(vec![(0, JobId(1)), (1, JobId(2))]), [0, 1]);
    assert_eq!(
        order.of(vec![(0, JobId(1)), (1, JobId(2)), (1, JobId(3))]),
        [0, 1, 2]
    );
    assert_eq!(order.of(Vec::new()), [] as [u32; 0]);
}

// A torrent runs when its turn lets it and it is resumed.
#[test]
fn a_torrent_runs_when_its_turn_lets_it_and_it_is_resumed() {
    let turn = |active| Turn { active, rank: None };
    assert!(policy::runs(turn(true), true));
    assert!(!policy::runs(turn(true), false));
    assert!(!policy::runs(turn(false), true));
    assert!(!policy::runs(turn(false), false));
}

// Its disk is looked at only when it would run and is starting, or is held
// for room to start.
#[test]
fn the_disk_is_looked_at_only_for_a_start_or_a_start_held() {
    let held = Some(DiskHeld::Starting);
    assert!(policy::checks_start(true, true, None));
    assert!(policy::checks_start(true, false, held));
    assert!(!policy::checks_start(true, false, None));
    assert!(!policy::checks_start(false, true, held));
    assert!(!policy::checks_start(false, true, None));
}

// The session is told only what changes its run.
#[test]
fn a_session_is_told_only_a_change_of_run() {
    assert_eq!(policy::run_command(true, true), Some(RunCommand::Unpause));
    assert_eq!(policy::run_command(false, false), Some(RunCommand::Pause));
    assert_eq!(policy::run_command(true, false), None);
    assert_eq!(policy::run_command(false, true), None);
}

// Picking: each pick that differs, the levels as the files that differ,
// nothing built when none does.
#[test]
fn picking_pushes_only_what_differs() {
    let normal = [FilePriority::Normal, FilePriority::Normal];
    let mixed = [FilePriority::Normal, FilePriority::High];
    let picks = |sequential, first_last, levels| Picks {
        sequential,
        first_last,
        levels,
    };
    let run = |stored: Picks<'_>, running: Picks<'_>| {
        let mut out = Vec::new();
        policy::picks(&stored, &running, &mut out);
        out
    };
    assert_eq!(
        run(picks(false, false, &normal), picks(false, false, &normal)),
        []
    );
    assert_eq!(
        run(picks(true, false, &normal), picks(false, false, &normal)),
        [Intent::Sequential(true)]
    );
    assert_eq!(
        run(picks(false, false, &normal), picks(false, true, &normal)),
        [Intent::FirstLast(false)]
    );
    assert_eq!(
        run(picks(false, false, &mixed), picks(false, false, &normal)),
        [Intent::Levels(vec![(1, FilePriority::High)])]
    );
    // A file the session does not list yet is pushed.
    assert_eq!(
        run(picks(true, true, &mixed), picks(false, false, &normal[..1])),
        [
            Intent::Sequential(true),
            Intent::FirstLast(true),
            Intent::Levels(vec![(1, FilePriority::High)])
        ]
    );
}

// Picking and limits leave a torrent with no session alone.
#[test]
fn picking_and_limits_leave_a_torrent_with_no_session_alone() {
    let settings = settings();
    let mut torrent = downloading();
    torrent.record.sequential = true;
    torrent.record.upload_limit = Some(4096);
    assert_eq!(decide(&Picking, &torrent, &settings, &Ask::default()), []);
    assert_eq!(decide(&Limits, &torrent, &settings, &Ask::default()), []);
}

// Limits: pushed only when the session's differ.
#[test]
fn limits_are_pushed_only_when_they_differ() {
    let (record, _, _) = chain();
    let wanted = ratelimits_of(
        &TorrentRecord {
            upload_limit: Some(4096),
            ..record
        },
        None,
    );
    let none = LimitsConfig::default();
    assert_eq!(policy::limits_change(wanted, wanted), None);
    assert_eq!(policy::limits_change(none, none), None);
    assert_eq!(
        policy::limits_change(wanted, none),
        Some(Intent::Limits(wanted))
    );
}
