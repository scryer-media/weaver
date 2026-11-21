// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The rules a pass over the torrents runs, each a policy that looks at one
// torrent and says what should change, and the pass that applies what they
// say. A policy awaits nothing, writes nothing and locks nothing: it reads
// the torrent and the pass's context, and returns intents. The pass takes
// `live` once, asks each policy in its list in turn for each torrent,
// applies one policy's intents before it asks the next, and gathers what
// goes out after: the seed-mode rows as one write, queued ahead of the
// ledgers and checkpoints queued on the torrents' lanes, the stops as one
// write, and the removals.

use super::checkpoints::{CheckpointSave, CheckpointTaken, Landing, Resume, take_checkpoint};
use super::disk_room::{DiskHeld, PlaceBatch, StartHold};
use super::live::Live;
use super::queue::QueueEnd;
use super::scripts::Ending;
use super::settings::{TorrentSettings, WindDown};
use super::{TorrentService, queued, ratelimits_of, take_turn};
use crate::StateError;
use crate::jobs::JobId;
use crate::jobs::handle::HoldSet;
use crate::post_processing::events::EventContext;
use crate::post_processing::model::{QueueEvent, vars};
use crate::settings::model::TorrentConfig;
use crate::settings::{Seeding, SeedingAction};
use crate::torrents::persistence::TorrentSeedWrites;
use crate::torrents::persistence::TorrentsPersistenceInsertPlacedTorrentInternalQueries;
use crate::torrents::status::{self, Queued, Turn};
use crate::torrents::{FilePriority, KeptIntent, TorrentIntent};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{LazyLock, PoisonError};
use tokio::sync::oneshot;
use weaver_torrent::limits::LimitsConfig;

/// What a pass looks at besides the torrent: the settings, rules and holds
/// as it took them, and its instant.
#[derive(Clone, Copy)]
pub struct Context<'a> {
    pub settings: &'a TorrentSettings,
    pub rules: &'a super::live::Rules,
    pub holds: &'a HoldSet,
    pub now: i64,
    // Reading: the peers are taken now, not once a minute.
    pub at_once: bool,
    // Goals: torrents are off, winding down to the downloads alone.
    pub downloads_only: bool,
    // Checkpoint: the torrents whose checkpoints are behind.
    pub unsaved: &'a BTreeSet<JobId>,
    // Turn: each torrent's seat in the queue, as the pass worked it out.
    pub seats: &'a Seats,
}

/// What a policy says should change for one torrent: plain data, applied by
/// the pass in the order given.
#[derive(Debug, Clone, PartialEq)]
pub enum Intent {
    // Reading. Seed mode ended in the session: what the job has is what
    // the torrent has, after a restart too.
    LeaveSeedMode,
    // Still in it: the pieces that passed since they were last kept are
    // kept (libtorrent saves `verified_pieces` in resume data).
    KeepVerified { pieces: Vec<u8>, count: usize },
    // The session's figures, and the ledger kept from them.
    TakeReading,
    // Checkpoint: its pieces with its ledger, behind as they are.
    Checkpoint,
    // Goals. Winding down to the downloads alone: stopped, not by the
    // operator, so it resumes when torrents are on again.
    WindDown,
    // Its seeding goal is met: it does what its policy says.
    GoalMet(SeedingAction),
    // Turn. How the queue stands it, once its goals are applied.
    Stand(Queued),
    // Its seat in the queue: its rank and holds, and whether it runs.
    TakeTurn { queued: Queued, turn: Turn },
    // Picking, each only where the session differs.
    Sequential(bool),
    FirstLast(bool),
    Levels(Vec<(usize, FilePriority)>),
    // Limits, only where the session differs.
    Limits(LimitsConfig),
}

/// One rule of a pass: what one torrent should be doing, as intents.
pub trait Policy: Sync {
    fn decide(&self, torrent: &Live, cx: &Context<'_>, out: &mut Vec<Intent>);
}

/// The tick's pass up to the goals: each torrent read, its checkpoint
/// written when it is behind, its goals applied and its standing taken. A
/// new rule is one more entry here or in [`ON_TURNS`].
pub(super) const UP_TO_GOALS: &[&dyn Policy] = &[&Reading, &Checkpoint, &Goals, &Standing];

/// The tick's pass once every torrent's turn is known: its turn, then its
/// picking, then its limits.
pub(super) const ON_TURNS: &[&dyn Policy] = &[&Turns, &Picking, &Limits];

/// The session's figures for the torrent, and seed mode as it stands.
pub struct Reading;

impl Policy for Reading {
    fn decide(&self, torrent: &Live, _: &Context<'_>, out: &mut Vec<Intent>) {
        if let Some(attached) = &torrent.attached {
            let handle = &attached.handle;
            out.extend(seed_mode_step(
                torrent.record.seed_mode,
                handle.seed_mode(),
                || handle.seed_verified(),
                |verified| verified.count_ones(),
                |verified| verified.as_raw_slice().to_vec(),
                torrent.seed_verified,
            ));
        }
        out.push(Intent::TakeReading);
    }
}

// What seed mode asks of a torrent the record keeps `stored` in seed mode,
// whose session is `running` in it, which kept `kept` pieces as checked:
// left once the session left it; else the pieces checked since kept. The
// session's checked pieces are read from `verified` only while it is in
// seed mode, counted by `count`, and copied by `pieces` only when more
// passed than were kept.
pub fn seed_mode_step<V>(
    stored: bool,
    running: bool,
    verified: impl FnOnce() -> Option<V>,
    count: impl FnOnce(&V) -> usize,
    pieces: impl FnOnce(&V) -> Vec<u8>,
    kept: usize,
) -> Option<Intent> {
    if !stored {
        return None;
    }
    if !running {
        return Some(Intent::LeaveSeedMode);
    }
    let verified = verified()?;
    let count = count(&verified);
    (count > kept).then(|| Intent::KeepVerified {
        pieces: pieces(&verified),
        count,
    })
}

/// A torrent whose checkpoint is behind has it written, after its reading.
pub struct Checkpoint;

impl Policy for Checkpoint {
    fn decide(&self, torrent: &Live, cx: &Context<'_>, out: &mut Vec<Intent>) {
        if cx.unsaved.contains(&torrent.record.job_id) {
            out.push(Intent::Checkpoint);
        }
    }
}

/// A seeding torrent whose goal is met does what its policy says; winding
/// down to the downloads alone, one done downloading stops.
pub struct Goals;

impl Policy for Goals {
    fn decide(&self, torrent: &Live, cx: &Context<'_>, out: &mut Vec<Intent>) {
        // A seed whose goal is met in this pass ends by its goal, wind-down
        // or not.
        let met = (torrent.seeding() && torrent.resumed())
            .then(|| goal_met(cx.settings, cx.rules, torrent, cx.now))
            .flatten();
        // Winding down to the downloads alone, a torrent done downloading,
        // forced or not, seeding or waiting to, stops: its goal is not met.
        if met.is_none()
            && cx.downloads_only
            && torrent.resumed()
            && (torrent.complete() || torrent.seeding())
        {
            out.push(Intent::WindDown);
            return;
        }
        if let Some(policy) = met {
            out.push(Intent::GoalMet(policy.action));
        }
    }
}

// The seeding terms of a torrent whose seeding goal is met, `None` while one
// is left or for a forced torrent, which seeds past its goal.
pub(super) fn goal_met(
    settings: &TorrentSettings,
    rules: &super::live::Rules,
    torrent: &Live,
    now: i64,
) -> Option<Seeding> {
    if torrent.record.forced {
        return None;
    }
    let policy = settings.policy(&torrent.record, torrent.rule(rules));
    status::goal_left(&policy, &torrent.counters, torrent.have_bytes, now)
        .is_none()
        .then_some(policy)
}

/// How the download queue and its holds stand the torrent, for its turn.
pub struct Standing;

impl Policy for Standing {
    fn decide(&self, torrent: &Live, cx: &Context<'_>, out: &mut Vec<Intent>) {
        out.push(Intent::Stand(queued(
            cx.settings,
            cx.rules,
            torrent,
            cx.now,
        )));
    }
}

/// The torrent's seat in the queue, as the pass worked it out from every
/// torrent's standing.
pub struct Turns;

impl Policy for Turns {
    fn decide(&self, torrent: &Live, cx: &Context<'_>, out: &mut Vec<Intent>) {
        if let Some((queued, turn)) = cx.seats.of(torrent.record.job_id) {
            out.push(Intent::TakeTurn { queued, turn });
        }
    }
}

// Whether a torrent whose turn is `turn` runs, before its disk is looked
// at: its turn lets it and the operator has it resumed.
pub fn runs(turn: Turn, resumed: bool) -> bool {
    turn.active && resumed
}

// Whether a torrent that would `run` has its disk looked at: one not
// running yet whose turn lets it run is kept from starting while the disk is
// short, and one held for that, paused or with its download held, is let go
// once it has room.
pub fn checks_start(run: bool, starting: bool, disk_held: Option<DiskHeld>) -> bool {
    run
        && (starting
            || matches!(
                disk_held,
                Some(DiskHeld::Starting | DiskHeld::Fetching)
            ))
}

/// What a session is told of a torrent's run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunCommand {
    Pause,
    Unpause,
}

// What a session running a torrent `paused` or not is told so that it
// `run`s or not: nothing when it already does.
pub fn run_command(run: bool, paused: bool) -> Option<RunCommand> {
    match (run, paused) {
        (true, true) => Some(RunCommand::Unpause),
        (false, false) => Some(RunCommand::Pause),
        _ => None,
    }
}

/// What each session runs is what is stored: the levels the operator set
/// for the files, sequential and first-and-last, each pushed only where it
/// differs. A full disk leaves the levels as they are: the session stops
/// fetching in upload mode, not by lower levels.
pub struct Picking;

impl Policy for Picking {
    fn decide(&self, torrent: &Live, _: &Context<'_>, out: &mut Vec<Intent>) {
        let (Some(attached), Some(_)) = (&torrent.attached, &torrent.store) else {
            return;
        };
        let handle = &attached.handle;
        let running = Picks {
            sequential: handle.sequential(),
            first_last: handle.first_last_piece_first(),
            levels: &handle.file_priorities(),
        };
        let stored = Picks {
            sequential: torrent.record.sequential,
            first_last: torrent.record.first_last_piece_first,
            levels: &torrent.levels,
        };
        picks(&stored, &running, out);
    }
}

/// How a torrent's pieces are picked: in order, its files' first and last
/// pieces first, and each file's level.
pub struct Picks<'a> {
    pub sequential: bool,
    pub first_last: bool,
    pub levels: &'a [FilePriority],
}

// What reaches a session `running` the picks to run the `stored` ones. The
// levels go as the files that differ, and only when one does: nothing is
// built for a torrent whose levels the session runs already.
pub fn picks(stored: &Picks<'_>, running: &Picks<'_>, out: &mut Vec<Intent>) {
    if running.sequential != stored.sequential {
        out.push(Intent::Sequential(stored.sequential));
    }
    if running.first_last != stored.first_last {
        out.push(Intent::FirstLast(stored.first_last));
    }
    let mut changes: Option<Vec<(usize, FilePriority)>> = None;
    for (index, level) in stored.levels.iter().enumerate() {
        if running.levels.get(index) != Some(level) {
            changes.get_or_insert_with(Vec::new).push((index, *level));
        }
    }
    if let Some(changes) = changes {
        out.push(Intent::Levels(changes));
    }
}

/// The limits each torrent's session holds it to, from its own and its
/// seeding policy's as they are now: a change of either at any level
/// reaches the torrent's peers without reconnecting them.
pub struct Limits;

impl Policy for Limits {
    fn decide(&self, torrent: &Live, cx: &Context<'_>, out: &mut Vec<Intent>) {
        let Some(attached) = &torrent.attached else {
            return;
        };
        let rule = torrent.rule(cx.rules);
        let wanted = ratelimits_of(
            &torrent.record,
            cx.settings.upload_cap(&torrent.record, rule),
        );
        out.extend(limits_change(wanted, attached.handle.ratelimits()));
    }
}

// The limits that reach a session holding a torrent to `running`, so that
// it holds it to `wanted`: none when it does.
pub fn limits_change(wanted: LimitsConfig, running: LimitsConfig) -> Option<Intent> {
    (running != wanted).then_some(Intent::Limits(wanted))
}

/// Each torrent's seat in the queue, by job.
#[derive(Debug, Default)]
pub struct Seats(Vec<(JobId, Queued, Turn)>);

impl Seats {
    pub fn of(&self, job_id: JobId) -> Option<(Queued, Turn)> {
        let at = self
            .0
            .binary_search_by_key(&job_id, |(id, _, _)| *id)
            .ok()?;
        let (_, queued, turn) = self.0[at];
        Some((queued, turn))
    }
}

/// The queue's order as the last pass worked it out: the place and job of
/// each torrent, by job, and the torrents in place order, as indexes into
/// those. A pass whose places are the same takes the same order.
#[derive(Debug, Default)]
pub struct QueueOrder {
    places: Vec<(i64, JobId)>,
    order: Vec<u32>,
}

impl QueueOrder {
    // The torrents of `places`, which are by job, in place order: the last
    // order again when no place moved, else sorted anew.
    pub fn of(&mut self, places: Vec<(i64, JobId)>) -> &[u32] {
        if places != self.places {
            let mut order: Vec<u32> = (0..places.len() as u32).collect();
            order.sort_unstable_by_key(|&at| places[at as usize]);
            self.places = places;
            self.order = order;
        }
        &self.order
    }
}

// Each standing's seat, the standings taken by job: the turns are worked
// out in queue order, by place then job, and the walk's end with them.
pub fn seat(
    standings: Vec<(i64, Queued)>,
    order: &mut QueueOrder,
    max_active: u32,
    holds: &HoldSet,
) -> (Seats, QueueEnd) {
    let places: Vec<(i64, JobId)> = standings
        .iter()
        .map(|(place, queued)| (*place, queued.job_id))
        .collect();
    let order = order.of(places);
    let last = order.last().map(|&at| {
        let (place, queued) = &standings[at as usize];
        (*place, queued.job_id)
    });
    let mut slots = status::Slots::new(max_active);
    let mut turns = vec![
        Turn {
            active: false,
            rank: None
        };
        standings.len()
    ];
    for &at in order {
        turns[at as usize] = slots.take(&standings[at as usize].1, holds);
    }
    let seats = standings
        .into_iter()
        .zip(turns)
        .map(|((_, queued), turn)| (queued.job_id, queued, turn))
        .collect();
    (Seats(seats), QueueEnd { slots, last })
}

/// A write a walk took from a torrent, queued on the torrent's lane once
/// the walk is over and the seed-mode rows are queued, in the order taken.
enum Taken {
    Resume(JobId, Resume),
    Checkpoint(CheckpointTaken),
}

/// What a walk undoes in memory of seed mode when its rows are not kept:
/// the torrent left it, or kept `count` pieces; it had `prior` kept.
/// `refused`: its last row of the same kind was refused, and a row kept now
/// ends that streak.
struct SeedUndo {
    job_id: JobId,
    left: bool,
    count: usize,
    prior: usize,
    refused: bool,
}

/// A pass's seed-mode rows, queued, and what to undo should they fail.
pub(super) struct SeedLanding {
    landed: oneshot::Receiver<Result<(), StateError>>,
    undo: Vec<SeedUndo>,
}

/// What a pass gathers to do once its walks are over.
#[derive(Default)]
pub(super) struct Gathered {
    // The seed-mode rows, and what memory undoes should they fail.
    seed: TorrentSeedWrites,
    undo: Vec<SeedUndo>,
    // The ledgers and checkpoints taken, in the order taken.
    taken: Vec<Taken>,
    // Stopped by a wind-down to the downloads alone, and by their goals.
    wound_down: Vec<JobId>,
    met: Vec<JobId>,
    // Once queued: the seed-mode rows, the ledgers and the checkpoints.
    seed_landing: Option<SeedLanding>,
    landings: Vec<Landing>,
    saves: Vec<CheckpointSave>,
    // Each torrent's standing, by job, with its place in the queue.
    standings: Vec<(i64, Queued)>,
    // The torrents whose goal removes them, with their data or not, and
    // the event raised once each is in history.
    removals: Vec<(JobId, bool, Option<EventContext>)>,
    // The looks for disks the walks began, which ask one pass between
    // them as the last ends.
    looks: Option<std::sync::Arc<PlaceBatch>>,
    // By root, whether a torrent the walk gave its turn is still kept by
    // that root's start wait.
    start_kept: [bool; 3],
    // The walks made, and the writes queued or made, counted in tests.
    walks: u64,
    writes: u64,
}

/// What a session is told for an intent, awaited by the walk.
enum Command {
    Run(RunCommand),
    Levels(Vec<(usize, FilePriority)>),
}

type LiveMap = BTreeMap<JobId, Live>;

// Settings for the passes whose policies read none (reading, picking): the
// settings are not cloned for them.
static NO_SETTINGS: LazyLock<TorrentSettings> = LazyLock::new(|| TorrentSettings {
    complete_dir: PathBuf::new(),
    categories: super::settings::Categories::default(),
    seeding: None,
    upload_cap: None,
    torrent: TorrentConfig::default(),
});

// No tracker rules, for the same passes.
static NO_RULES: LazyLock<super::live::Rules> = LazyLock::new(super::live::Rules::default);

impl TorrentService {
    // A pass's context with nothing of the reading, goals, checkpoint or
    // turn steps set.
    fn context<'a>(
        settings: &'a TorrentSettings,
        rules: &'a super::live::Rules,
        holds: &'a HoldSet,
        now: i64,
    ) -> Context<'a> {
        static NO_SEATS: Seats = Seats(Vec::new());
        static NO_JOBS: BTreeSet<JobId> = BTreeSet::new();
        Context {
            settings,
            rules,
            holds,
            now,
            at_once: false,
            downloads_only: false,
            unsaved: &NO_JOBS,
            seats: &NO_SEATS,
        }
    }

    // Winding down to the downloads alone.
    fn downloads_only(&self, settings: &TorrentSettings) -> bool {
        !settings.torrent.enabled && self.wind_down() == Some(WindDown::FinishDownloads)
    }

    /// The tick's pass: every torrent read, its checkpoint written when it
    /// is behind, its goals applied, then its turn, picking and limits, in
    /// that order for each torrent, under one take of `live`. Once the
    /// first walk is over, still under `live`: the seed-mode rows are
    /// queued as one write ahead of every ledger and checkpoint the walk
    /// took, those are queued in the order taken, and the stops are written
    /// with `live` held, as every intent write holds it. A goal that
    /// removes a torrent removes it before any turn is taken, as the passes
    /// one after another did: the turns are then taken after the removals,
    /// under a second take.
    pub(super) async fn policy_pass(&self, now: i64) {
        let settings = self.settings();
        let rules = self.rules();
        let holds = self.holds();
        let unsaved = self.inner.signal.take_unsaved();
        let cx = Context {
            downloads_only: self.downloads_only(&settings),
            unsaved: unsaved.jobs(),
            ..Self::context(&settings, &rules, &holds, now)
        };
        let mut gathered = Gathered::default();
        #[cfg(any(test, feature = "test-support"))]
        let mut locks = 1;
        {
            let mut live = self.inner.live.lock().await;
            gathered.standings.reserve(live.len());
            self.walk(&mut live, None, UP_TO_GOALS, &cx, &mut gathered)
                .await;
            self.queue_taken(&mut live, &mut gathered);
            self.write_stops(&mut gathered).await;
            if gathered.removals.is_empty() {
                let standings = std::mem::take(&mut gathered.standings);
                let seats = self.seat_queue(&settings, &holds, standings);
                let cx = Context {
                    seats: &seats,
                    ..cx
                };
                self.walk(&mut live, None, ON_TURNS, &cx, &mut gathered)
                    .await;
                self.end_unkept_start_holds(std::mem::take(&mut gathered.start_kept));
            }
        }
        // Their checkpoints were queued with the walk's.
        unsaved.queued();
        let removals = std::mem::take(&mut gathered.removals);
        #[cfg(any(test, feature = "test-support"))]
        let (mut walks, writes) = (gathered.walks, gathered.writes);
        #[cfg(any(test, feature = "test-support"))]
        {
            locks += u64::from(self.land(gathered).await);
        }
        #[cfg(not(any(test, feature = "test-support")))]
        self.land(gathered).await;
        if !removals.is_empty() {
            self.remove_met(removals).await;
            let _turn_walks = self.apply_turns(ON_TURNS).await;
            #[cfg(any(test, feature = "test-support"))]
            {
                locks += 1;
                walks += _turn_walks;
            }
        }
        #[cfg(any(test, feature = "test-support"))]
        self.inner.pass_counts.send_modify(|counts| {
            counts.passes += 1;
            counts.locks += locks;
            counts.walks += walks;
            counts.writes += writes;
        });
    }

    // Each torrent's turn, and what `then` says once it is known: the
    // standings walked, the turns worked out, then `then` walked. Returns
    // the walks made.
    pub(super) async fn apply_turns(&self, then: &[&dyn Policy]) -> u64 {
        let settings = self.settings();
        let rules = self.rules();
        let now = self.inner.clock.unix_ms();
        let holds = self.holds();
        let cx = Self::context(&settings, &rules, &holds, now);
        let mut gathered = Gathered::default();
        let mut live = self.inner.live.lock().await;
        gathered.standings.reserve(live.len());
        self.walk(&mut live, None, &[&Standing], &cx, &mut gathered)
            .await;
        let standings = std::mem::take(&mut gathered.standings);
        let seats = self.seat_queue(&settings, &holds, standings);
        let cx = Context {
            seats: &seats,
            ..cx
        };
        self.walk(&mut live, None, then, &cx, &mut gathered).await;
        self.end_unkept_start_holds(std::mem::take(&mut gathered.start_kept));
        debug_assert!(gathered.taken.is_empty() && gathered.seed.is_empty());
        gathered.walks
    }

    // The queue's seats from `standings`, and its end kept for the adds
    // after: worked out, and kept, under `live`'s lock.
    fn seat_queue(
        &self,
        settings: &TorrentSettings,
        holds: &HoldSet,
        standings: Vec<(i64, Queued)>,
    ) -> Seats {
        let (seats, end) = {
            let mut order = self
                .inner
                .queue_order
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            // While only some of the stored torrents are here, no
            // torrent takes a download slot among them: the pass at the
            // load's end gives the turns, among all of them.
            let slots = if self.partial() {
                0
            } else {
                settings.torrent.max_active_downloads
            };
            seat(standings, &mut order, slots, holds)
        };
        *self
            .inner
            .queue_end
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = end;
        seats
    }

    // The policies of `policies` over the torrents of `only`, or every one,
    // in job order: for each torrent, each policy's intents applied before
    // the next policy looks.
    pub(super) async fn walk(
        &self,
        live: &mut LiveMap,
        only: Option<&BTreeSet<JobId>>,
        policies: &[&dyn Policy],
        cx: &Context<'_>,
        gathered: &mut Gathered,
    ) {
        gathered.walks += 1;
        let mut intents = Vec::new();
        match only {
            None => {
                for torrent in live.values_mut() {
                    self.visit(torrent, policies, cx, &mut intents, gathered)
                        .await;
                }
            }
            Some(only) => {
                for job_id in only {
                    if let Some(torrent) = live.get_mut(job_id) {
                        self.visit(torrent, policies, cx, &mut intents, gathered)
                            .await;
                    }
                }
            }
        }
    }

    async fn visit(
        &self,
        torrent: &mut Live,
        policies: &[&dyn Policy],
        cx: &Context<'_>,
        intents: &mut Vec<Intent>,
        gathered: &mut Gathered,
    ) {
        for policy in policies {
            policy.decide(torrent, cx, intents);
            for intent in intents.drain(..) {
                if let Some(command) = self.apply(torrent, intent, cx, gathered) {
                    // Boxed: the walk's own future stays small for the
                    // torrents no session is told anything of.
                    Box::pin(self.command(torrent, command)).await;
                }
            }
        }
    }

    // One intent taken: in memory now, or gathered for after the walk. What
    // a session is told and awaited is returned: a pause, an unpause or the
    // file levels, as the passes before awaited them.
    fn apply(
        &self,
        torrent: &mut Live,
        intent: Intent,
        cx: &Context<'_>,
        gathered: &mut Gathered,
    ) -> Option<Command> {
        let job_id = torrent.record.job_id;
        match intent {
            Intent::LeaveSeedMode => {
                gathered.undo.push(SeedUndo {
                    job_id,
                    left: true,
                    count: 0,
                    prior: torrent.seed_verified,
                    refused: torrent.seed_left_refused,
                });
                torrent.record.seed_mode = false;
                torrent.seed_verified = 0;
                // Seeding no more, it may be queued again.
                self.inner.signal.want_pass();
                gathered.seed.left.push(job_id);
            }
            Intent::KeepVerified { pieces, count } => {
                gathered.undo.push(SeedUndo {
                    job_id,
                    left: false,
                    count,
                    prior: torrent.seed_verified,
                    refused: torrent.seed_verified_refused,
                });
                // Kept now, and undone should the row not be.
                torrent.seed_verified = count;
                gathered.seed.verified.push((job_id, pieces));
            }
            Intent::TakeReading => {
                let reading = torrent.reading(cx.now);
                torrent.engine = reading.engine.clone();
                torrent.speeds = reading.speeds;
                torrent.peers = reading.peers;
                torrent.keep(reading);
                if let Some(resume) = torrent.take_resume(cx.now, cx.at_once) {
                    gathered.taken.push(Taken::Resume(job_id, resume));
                }
            }
            Intent::Checkpoint => {
                if let Some(taken) = take_checkpoint(torrent, cx.now) {
                    gathered.taken.push(Taken::Checkpoint(taken));
                }
            }
            Intent::WindDown => {
                torrent.settle(cx.now);
                // Not the operator's stop: it resumes when torrents are
                // turned on again.
                torrent.record.kept = KeptIntent::WoundDown;
                gathered.wound_down.push(job_id);
                if let Some(resume) = torrent.take_resume(cx.now, true) {
                    gathered.taken.push(Taken::Resume(job_id, resume));
                }
            }
            Intent::GoalMet(action) => {
                let finished = self.script_event(
                    torrent,
                    QueueEvent::SeedingFinished,
                    &[(vars::SEEDING_ACTION, action.as_str())],
                );
                match action {
                    SeedingAction::Stop => {
                        torrent.settle(cx.now);
                        torrent.record.kept = KeptIntent::Intent(TorrentIntent::Stopped);
                        gathered.met.push(job_id);
                        if let Some(resume) = torrent.take_resume(cx.now, true) {
                            gathered.taken.push(Taken::Resume(job_id, resume));
                        }
                        self.queue_script_event(finished);
                    }
                    SeedingAction::Remove => gathered.removals.push((job_id, false, finished)),
                    SeedingAction::RemoveWithData => {
                        gathered.removals.push((job_id, true, finished));
                    }
                }
            }
            Intent::Stand(queued) => {
                gathered
                    .standings
                    .push((torrent.record.queue_position, queued));
            }
            Intent::TakeTurn { queued, turn } => {
                take_turn(torrent, &queued, turn, cx.holds, cx.now);
                let mut run = runs(turn, torrent.resumed());
                let starting = torrent
                    .attached
                    .as_ref()
                    .is_none_or(|attached| attached.handle.is_paused());
                // A magnet whose metadata came while it ran is judged now.
                let starting = starting || (run && self.start_unjudged(torrent));
                if checks_start(run, starting, torrent.disk_held) {
                    let looks = gathered.looks.get_or_insert_with(|| PlaceBatch::new(self));
                    match self.start_hold_in(torrent, Some(looks), false) {
                        // One running that holds verified data has its
                        // download held and seeds; one yet to run, or with
                        // nothing to seed, stays paused.
                        StartHold::Short(refused)
                            if torrent.disk_held == Some(DiskHeld::Fetching)
                                || self.seeding_run(torrent) =>
                        {
                            self.hold_download(torrent, refused);
                        }
                        StartHold::Short(refused) => {
                            torrent.disk_held = Some(DiskHeld::Starting);
                            torrent.start_refused = Some(refused);
                            run = false;
                        }
                        StartHold::Unplaced => {
                            run &= torrent.disk_held == Some(DiskHeld::Fetching);
                        }
                        StartHold::Free if torrent.disk_held == Some(DiskHeld::Starting) => {
                            torrent.disk_held = None;
                            torrent.start_refused = None;
                        }
                        StartHold::Free => self.end_fetch_hold(torrent),
                    }
                } else if !run
                    && matches!(
                        torrent.disk_held,
                        Some(DiskHeld::Starting | DiskHeld::Fetching)
                    )
                {
                    torrent.disk_held = None;
                    torrent.start_refused = None;
                }
                if let Some(at) = Self::start_kept_on(torrent) {
                    gathered.start_kept[at] = true;
                }
                if run && torrent.start_waits_on_move() {
                    return None;
                }
                let attached = torrent.attached.as_ref()?;
                return run_command(run, attached.handle.is_paused()).map(Command::Run);
            }
            Intent::Sequential(on) => {
                if let Some(attached) = &torrent.attached {
                    attached.handle.set_sequential(on);
                }
            }
            Intent::FirstLast(on) => {
                if let Some(attached) = &torrent.attached
                    && let Err(error) = attached.handle.set_first_last_piece_first(on)
                {
                    tracing::debug!(job_id = job_id.0, error = %error, "a torrent's first and last pieces did not move");
                }
            }
            Intent::Levels(changes) => {
                torrent.attached.as_ref()?;
                return Some(Command::Levels(changes));
            }
            Intent::Limits(limits) => {
                if let Some(attached) = &torrent.attached {
                    attached.handle.set_ratelimits(limits);
                }
            }
        }
        None
    }

    // What `apply` returned, told the torrent's session and awaited.
    async fn command(&self, torrent: &mut Live, command: Command) {
        let job_id = torrent.record.job_id;
        let Some(attached) = &torrent.attached else {
            return;
        };
        match command {
            Command::Run(command) => {
                // A pause gives back what its start promised and did not
                // land: its next start is judged again.
                if matches!(command, RunCommand::Pause)
                    && let Some(intake) = &torrent.intake
                {
                    intake.room().release_start();
                }
                let result = match command {
                    RunCommand::Unpause => attached.session.unpause(&attached.handle).await,
                    RunCommand::Pause => attached.session.pause(&attached.handle).await,
                };
                // A run that ends or begins is not in upload mode: a wait for
                // room on it, and what a wake handed it, end.
                self.end_disk_room(torrent);
                torrent.ran(result);
            }
            Command::Levels(changes) => {
                if let Err(error) = attached
                    .session
                    .set_file_priorities(&attached.handle, &changes)
                    .await
                {
                    tracing::debug!(job_id = job_id.0, error = %error, "a torrent's file levels did not change");
                }
            }
        }
    }

    // What a walk took, queued before `live` is let go: the seed-mode rows
    // first, as one write that waits out every write queued before it, so
    // they land ahead of each torrent's ledger and checkpoint, as they did
    // when written before those were queued; then each ledger and
    // checkpoint on its torrent's lane, in the order taken. No await: no
    // other taking of a torrent's state comes between.
    fn queue_taken(&self, live: &mut LiveMap, gathered: &mut Gathered) {
        if !gathered.seed.is_empty() {
            let writes = std::mem::take(&mut gathered.seed);
            let undo = std::mem::take(&mut gathered.undo);
            gathered.writes += 1;
            let (landed, receiver) = oneshot::channel();
            match self
                .inner
                .db
                .try_queue_write("torrent_seed_modes", move |db| {
                    let _ = landed.send(db.write_torrent_seed_modes(writes));
                    Ok(())
                }) {
                Ok(()) => {
                    gathered.seed_landing = Some(SeedLanding {
                        landed: receiver,
                        undo,
                    });
                }
                Err(error) => {
                    undo_seed(live, &undo, &error.to_string());
                }
            }
        }
        for taken in gathered.taken.drain(..) {
            match taken {
                Taken::Resume(job_id, resume) => {
                    gathered
                        .landings
                        .push(self.queue_taken_resume(job_id, resume));
                }
                Taken::Checkpoint(taken) => {
                    gathered.saves.push(self.queue_taken_checkpoint(taken));
                }
            }
        }
    }

    // The kept intents of the torrents a walk stopped, in one transaction
    // off the runtime, `live` held across it as for every intent write: a
    // resume taken between the two would otherwise be written over.
    async fn write_stops(&self, gathered: &mut Gathered) {
        if gathered.wound_down.is_empty() && gathered.met.is_empty() {
            return;
        }
        gathered.writes += 1;
        let wound_down = std::mem::take(&mut gathered.wound_down);
        let met = std::mem::take(&mut gathered.met);
        let db = self.inner.db.clone();
        match tokio::task::spawn_blocking(move || {
            db.update_torrent_intents(&[
                (KeptIntent::WoundDown, &wound_down),
                (KeptIntent::Intent(TorrentIntent::Stopped), &met),
            ])
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "could not keep the stops of torrents done");
            }
            Err(error) => {
                tracing::warn!(error = %error, "could not keep the stops of torrents done");
            }
        }
    }

    // What a pass queued, landed: its seed-mode rows, its ledgers, then its
    // checkpoints. Seed-mode rows that were not kept are undone in memory,
    // under `live` taken again; returns whether it was.
    async fn land(&self, gathered: Gathered) -> bool {
        let mut relocked = false;
        if let Some(SeedLanding { landed, undo }) = gathered.seed_landing {
            let failed = match landed.await {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(error) => Some(error.to_string()),
            };
            if let Some(error) = failed {
                let mut live = self.inner.live.lock().await;
                relocked = true;
                undo_seed(&mut live, &undo, &error);
            } else if undo.iter().any(|undo| undo.refused) {
                // Kept at last: a later refusal is warned of again. Only
                // when a streak ends is `live` taken for it.
                let mut live = self.inner.live.lock().await;
                relocked = true;
                for undo in undo.iter().filter(|undo| undo.refused) {
                    if let Some(torrent) = live.get_mut(&undo.job_id) {
                        if undo.left {
                            torrent.seed_left_refused = false;
                        } else {
                            torrent.seed_verified_refused = false;
                        }
                    }
                }
            }
        }
        for landing in gathered.landings {
            let _ = self.landed(Some(landing)).await;
        }
        for save in gathered.saves {
            self.checkpoint_landed(save).await;
        }
        relocked
    }

    // A removed torrent's event is queued once its job is in history, where
    // the queue finds it.
    async fn remove_met(&self, removals: Vec<(JobId, bool, Option<EventContext>)>) {
        for (job_id, delete_files, finished) in removals {
            match self
                .remove_as(job_id, delete_files, true, Ending::Goal)
                .await
            {
                Ok(()) => self.queue_script_event(finished),
                Err(error) => {
                    tracing::warn!(job_id = job_id.0, error = %error, "a torrent that met its goal was not removed");
                }
            }
        }
    }

    /// A seeding torrent whose seeding goal is met does what its policy
    /// says, and one done downloading stops while torrents wind down to the
    /// downloads alone: the goals policy over every torrent. The tick
    /// applies them in its pass; tests ask for them alone.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn apply_goals(&self, now: i64) {
        let settings = self.settings();
        let rules = self.rules();
        let holds = HoldSet::default();
        let cx = Context {
            downloads_only: self.downloads_only(&settings),
            ..Self::context(&settings, &rules, &holds, now)
        };
        let mut gathered = Gathered::default();
        {
            let mut live = self.inner.live.lock().await;
            self.walk(&mut live, None, &[&Goals], &cx, &mut gathered)
                .await;
            self.queue_taken(&mut live, &mut gathered);
            self.write_stops(&mut gathered).await;
        }
        let removals = std::mem::take(&mut gathered.removals);
        self.land(gathered).await;
        self.remove_met(removals).await;
    }

    // The session's figures for each torrent, and the ledger kept from
    // them: written whenever it changed, with its peers once a minute while
    // they change, or now when `at_once`. The reading policy over every
    // torrent, which reads no settings.
    pub async fn read(&self, now: i64, at_once: bool) {
        let holds = HoldSet::default();
        let cx = Context {
            at_once,
            ..Self::context(&NO_SETTINGS, &NO_RULES, &holds, now)
        };
        let mut gathered = Gathered::default();
        {
            let mut live = self.inner.live.lock().await;
            self.walk(&mut live, None, &[&Reading], &cx, &mut gathered)
                .await;
            self.queue_taken(&mut live, &mut gathered);
        }
        self.land(gathered).await;
    }

    // Which torrents the queue lets download, applied to their sessions.
    pub async fn apply_queue(&self) {
        self.apply_turns(&[&Turns]).await;
    }

    // The queue applied, then each torrent's picking, under one take: for
    // each torrent its turn, then its picking, as the two passes one after
    // the other gave them.
    pub async fn apply_queue_and_picking(&self) {
        self.apply_turns(&[&Turns, &Picking]).await;
    }

    // What each session runs is what is stored, read from the torrent's
    // handle and pushed only where it differs. A change the session refuses
    // (while it checks its files) is pushed again on the next pass.
    pub async fn apply_picking(&self) {
        self.pick(None).await;
    }

    // Picking for the torrents in `only`, or for every one. Picking reads
    // no settings.
    pub(super) async fn pick(&self, only: Option<&BTreeSet<JobId>>) {
        let holds = HoldSet::default();
        let cx = Self::context(&NO_SETTINGS, &NO_RULES, &holds, self.inner.clock.unix_ms());
        self.walk_alone(only, &[&Picking], &cx).await;
    }

    // The limits each torrent's session holds it to.
    pub(super) async fn apply_limits(&self) {
        let settings = self.settings();
        let rules = self.rules();
        let holds = HoldSet::default();
        let cx = Self::context(&settings, &rules, &holds, self.inner.clock.unix_ms());
        self.walk_alone(None, &[&Limits], &cx).await;
    }

    // `policies`, which gather nothing, over the torrents of `only`, or
    // every one, under one take of `live`.
    async fn walk_alone(
        &self,
        only: Option<&BTreeSet<JobId>>,
        policies: &[&dyn Policy],
        cx: &Context<'_>,
    ) {
        let mut gathered = Gathered::default();
        let mut live = self.inner.live.lock().await;
        self.walk(&mut live, only, policies, cx, &mut gathered)
            .await;
        debug_assert!(gathered.taken.is_empty() && gathered.seed.is_empty());
    }
}

// Seed-mode rows not kept, undone in memory so that it agrees with the
// database and the next pass writes them again: a torrent that left seed
// mode is in it again, with the pieces it had kept; one that kept more
// pieces has what it had kept. A torrent changed since, or gone, is left.
// A refused row is warned of once per torrent until one of its kind is
// kept.
fn undo_seed(live: &mut LiveMap, undo: &[SeedUndo], error: &str) {
    for undo in undo {
        let Some(torrent) = live.get_mut(&undo.job_id) else {
            continue;
        };
        if undo.left {
            if !torrent.seed_left_refused {
                torrent.seed_left_refused = true;
                tracing::warn!(job_id = undo.job_id.0, error = %error, "could not record that a torrent left seed mode");
            }
            if !torrent.record.seed_mode {
                torrent.record.seed_mode = true;
                torrent.seed_verified = undo.prior;
            }
        } else {
            if !torrent.seed_verified_refused {
                torrent.seed_verified_refused = true;
                tracing::warn!(job_id = undo.job_id.0, error = %error, "could not keep the pieces a torrent checked in seed mode");
            }
            if torrent.record.seed_mode && torrent.seed_verified == undo.count {
                torrent.seed_verified = undo.prior;
            }
        }
    }
}

/// What the tick's passes did, counted in tests: each pass, each take of
/// `live` and each walk over the torrents it made, and each write it queued
/// or made of its rows.
#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PassCounts {
    pub passes: u64,
    pub locks: u64,
    pub walks: u64,
    pub writes: u64,
}
