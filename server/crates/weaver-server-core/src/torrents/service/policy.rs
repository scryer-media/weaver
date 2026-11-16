// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The rules a pass over the torrents runs, each a policy that looks at one
// torrent and says what should change, and the pass that applies what they
// say. A policy awaits nothing, writes nothing and locks nothing: it reads
// the torrent and the pass's context, and returns intents. The pass takes
// `live` once, asks each policy in its list in turn for each torrent,
// applies one policy's intents before it asks the next, and gathers what
// goes out after: the database writes, as one, the ledgers and checkpoints
// queued on the torrents' lanes, and the removals.

use super::checkpoints::{CheckpointSave, Landing};
use super::disk_room::{DiskHeld, StartHold};
use super::live::Live;
use super::queue::QueueEnd;
use super::scripts::Ending;
use super::settings::{TorrentSettings, WindDown};
use super::{TorrentService, queued, ratelimits_of, take_turn};
use crate::jobs::JobId;
use crate::jobs::handle::HoldSet;
use crate::post_processing::events::EventContext;
use crate::post_processing::model::{QueueEvent, vars};
use crate::settings::model::TrackerRule;
use crate::settings::{Seeding, SeedingAction};
use crate::torrents::persistence::TorrentPassWrites as PassWrites;
use crate::torrents::status::{self, Queued, Turn};
use crate::torrents::{FilePriority, KeptIntent, TorrentIntent};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::PoisonError;
use weaver_torrent::limits::LimitsConfig;

/// What a pass looks at besides the torrent: the settings, rules and holds
/// as it took them, and its instant.
#[derive(Clone, Copy)]
pub(super) struct Context<'a> {
    pub(super) settings: &'a TorrentSettings,
    pub(super) rules: &'a [TrackerRule],
    pub(super) holds: &'a HoldSet,
    pub(super) now: i64,
    // Reading: the peers are taken now, not once a minute.
    pub(super) at_once: bool,
    // Goals: torrents are off, winding down to the downloads alone.
    pub(super) downloads_only: bool,
    // Checkpoint: the torrents whose checkpoints are behind.
    pub(super) unsaved: &'a BTreeSet<JobId>,
    // Turn: each torrent's seat in the queue, as the pass worked it out.
    pub(super) seats: &'a Seats,
}

/// What a policy says should change for one torrent: plain data, applied by
/// the pass in the order given.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Intent {
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
pub(super) trait Policy: Sync {
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
pub(super) struct Reading;

impl Policy for Reading {
    fn decide(&self, torrent: &Live, _: &Context<'_>, out: &mut Vec<Intent>) {
        if let Some(attached) = &torrent.attached {
            let handle = &attached.handle;
            out.extend(seed_mode_step(
                torrent.record.seed_mode,
                handle.seed_mode(),
                || {
                    handle
                        .seed_verified()
                        .map(|verified| (verified.count_ones(), verified.as_raw_slice().to_vec()))
                },
                torrent.seed_verified,
            ));
        }
        out.push(Intent::TakeReading);
    }
}

// What seed mode asks of a torrent the record keeps `stored` in seed mode,
// whose session is `running` in it, which kept `kept` pieces as checked:
// left once the session left it; else the pieces checked since kept, read
// from `verified` only then.
pub(super) fn seed_mode_step(
    stored: bool,
    running: bool,
    verified: impl FnOnce() -> Option<(usize, Vec<u8>)>,
    kept: usize,
) -> Option<Intent> {
    if !stored {
        return None;
    }
    if !running {
        return Some(Intent::LeaveSeedMode);
    }
    let (count, pieces) = verified()?;
    (count > kept).then_some(Intent::KeepVerified { pieces, count })
}

/// A torrent whose checkpoint is behind has it written, after its reading.
pub(super) struct Checkpoint;

impl Policy for Checkpoint {
    fn decide(&self, torrent: &Live, cx: &Context<'_>, out: &mut Vec<Intent>) {
        if cx.unsaved.contains(&torrent.record.job_id) {
            out.push(Intent::Checkpoint);
        }
    }
}

/// A seeding torrent whose goal is met does what its policy says; winding
/// down to the downloads alone, one done downloading stops.
pub(super) struct Goals;

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
    rules: &[TrackerRule],
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
pub(super) struct Standing;

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
pub(super) struct Turns;

impl Policy for Turns {
    fn decide(&self, torrent: &Live, cx: &Context<'_>, out: &mut Vec<Intent>) {
        if let Some((queued, turn)) = cx.seats.of(torrent.record.job_id) {
            out.push(Intent::TakeTurn { queued, turn });
        }
    }
}

// Whether a torrent whose turn is `turn` runs, before its disk is looked
// at: its turn lets it and the operator has it resumed.
pub(super) fn runs(turn: Turn, resumed: bool) -> bool {
    turn.active && resumed
}

// Whether a torrent that would `run` has its disk looked at: one not
// running yet whose turn lets it run is kept from starting while the disk is
// short, and one held for that is let go once it has room.
pub(super) fn checks_start(run: bool, starting: bool, disk_held: Option<DiskHeld>) -> bool {
    run && (starting || disk_held == Some(DiskHeld::Starting))
}

/// What a session is told of a torrent's run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RunCommand {
    Pause,
    Unpause,
}

// What a session running a torrent `paused` or not is told so that it
// `run`s or not: nothing when it already does.
pub(super) fn run_command(run: bool, paused: bool) -> Option<RunCommand> {
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
pub(super) struct Picking;

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
pub(super) struct Picks<'a> {
    pub(super) sequential: bool,
    pub(super) first_last: bool,
    pub(super) levels: &'a [FilePriority],
}

// What reaches a session `running` the picks to run the `stored` ones. The
// levels go as the files that differ, and only when one does: nothing is
// built for a torrent whose levels the session runs already.
pub(super) fn picks(stored: &Picks<'_>, running: &Picks<'_>, out: &mut Vec<Intent>) {
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
pub(super) struct Limits;

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
pub(super) fn limits_change(wanted: LimitsConfig, running: LimitsConfig) -> Option<Intent> {
    (running != wanted).then_some(Intent::Limits(wanted))
}

/// Each torrent's seat in the queue, by job.
#[derive(Debug, Default)]
pub(super) struct Seats(Vec<(JobId, Queued, Turn)>);

impl Seats {
    pub(super) fn of(&self, job_id: JobId) -> Option<(Queued, Turn)> {
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
pub(super) struct QueueOrder {
    places: Vec<(i64, JobId)>,
    order: Vec<u32>,
}

impl QueueOrder {
    // The torrents of `places`, which are by job, in place order: the last
    // order again when no place moved, else sorted anew.
    pub(super) fn of(&mut self, places: Vec<(i64, JobId)>) -> &[u32] {
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
pub(super) fn seat(
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

/// What a pass gathers to do once its walks are over.
#[derive(Default)]
pub(super) struct Gathered {
    // The ledgers queued on their torrents' lanes, and the checkpoints.
    landings: Vec<Landing>,
    saves: Vec<CheckpointSave>,
    // The pass's database writes, made together.
    pub(super) writes: PassWrites,
    // Each torrent's standing, by job, with its place in the queue.
    standings: Vec<(i64, Queued)>,
    // The torrents whose goal removes them, with their data or not, and
    // the event raised once each is in history.
    removals: Vec<(JobId, bool, Option<EventContext>)>,
}

type LiveMap = BTreeMap<JobId, Live>;

impl TorrentService {
    // `live`, taken for a pass: counted in tests.
    async fn pass_lock(&self) -> tokio::sync::MutexGuard<'_, LiveMap> {
        let live = self.inner.live.lock().await;
        #[cfg(test)]
        self.inner
            .pass_counts
            .send_modify(|counts| counts.locks += 1);
        live
    }

    // A pass's context with nothing of the reading, goals, checkpoint or
    // turn steps set.
    fn context<'a>(
        settings: &'a TorrentSettings,
        rules: &'a [TrackerRule],
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
    /// that order for each torrent, under one take of `live`. The database
    /// writes go as one, off the runtime, with `live` held across them as
    /// every intent write holds it. A goal that removes a torrent removes it
    /// before any turn is taken, as the passes one after another did: the
    /// turns are then taken after the removals, under a second take.
    pub(super) async fn policy_pass(&self, now: i64) {
        let settings = self.settings();
        let rules = self.rules();
        let holds = self.holds();
        let unsaved = self.inner.signal.take_unsaved();
        let cx = Context {
            downloads_only: self.downloads_only(&settings),
            unsaved: &unsaved,
            ..Self::context(&settings, &rules, &holds, now)
        };
        let mut gathered = Gathered::default();
        {
            let mut live = self.pass_lock().await;
            self.walk(&mut live, None, UP_TO_GOALS, &cx, &mut gathered)
                .await;
            self.write_pass(&mut live, &mut gathered.writes).await;
            if gathered.removals.is_empty() {
                let standings = std::mem::take(&mut gathered.standings);
                let seats = self.seat_queue(&settings, &holds, standings);
                let cx = Context {
                    seats: &seats,
                    ..cx
                };
                self.walk(&mut live, None, ON_TURNS, &cx, &mut gathered)
                    .await;
            }
        }
        let removals = std::mem::take(&mut gathered.removals);
        self.land(gathered).await;
        if removals.is_empty() {
            return;
        }
        self.remove_met(removals).await;
        self.apply_turns(ON_TURNS).await;
    }

    // Each torrent's turn, and what `then` says once it is known: the
    // standings walked, the turns worked out, then `then` walked.
    pub(super) async fn apply_turns(&self, then: &[&dyn Policy]) {
        let settings = self.settings();
        let rules = self.rules();
        let now = self.inner.clock.now_ms();
        let holds = self.holds();
        let cx = Self::context(&settings, &rules, &holds, now);
        let mut gathered = Gathered::default();
        let mut live = self.pass_lock().await;
        self.walk(&mut live, None, &[&Standing], &cx, &mut gathered)
            .await;
        let standings = std::mem::take(&mut gathered.standings);
        let seats = self.seat_queue(&settings, &holds, standings);
        let cx = Context {
            seats: &seats,
            ..cx
        };
        self.walk(&mut live, None, then, &cx, &mut gathered).await;
        debug_assert!(gathered.landings.is_empty() && gathered.writes.is_empty());
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
            seat(
                standings,
                &mut order,
                settings.torrent.max_active_downloads,
                holds,
            )
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
        #[cfg(test)]
        self.inner
            .pass_counts
            .send_modify(|counts| counts.walks += 1);
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
                self.apply(torrent, intent, cx, gathered).await;
            }
        }
    }

    // One intent taken: in memory now, a session told now, or gathered for
    // after the walk. Only a session's pause, unpause and file levels are
    // awaited, as the passes before awaited them.
    async fn apply(
        &self,
        torrent: &mut Live,
        intent: Intent,
        cx: &Context<'_>,
        gathered: &mut Gathered,
    ) {
        let job_id = torrent.record.job_id;
        match intent {
            Intent::LeaveSeedMode => {
                torrent.record.seed_mode = false;
                torrent.seed_verified = 0;
                // Seeding no more, it may be queued again.
                self.inner.signal.want_pass();
                gathered.writes.left_seed_mode.push(job_id);
            }
            Intent::KeepVerified { pieces, count } => {
                gathered.writes.verified.push((job_id, pieces, count));
            }
            Intent::TakeReading => {
                let reading = torrent.reading(cx.now);
                torrent.engine = reading.engine.clone();
                torrent.speeds = reading.speeds;
                torrent.peers = reading.peers;
                torrent.keep(reading);
                // Queued in the step that took it, so ledgers land in the
                // order they were read.
                gathered
                    .landings
                    .extend(self.queue_resume(torrent, cx.now, cx.at_once));
            }
            Intent::Checkpoint => {
                gathered
                    .saves
                    .extend(self.queue_checkpoint(torrent, cx.now));
            }
            Intent::WindDown => {
                torrent.settle(cx.now);
                // Not the operator's stop: it resumes when torrents are
                // turned on again.
                torrent.record.kept = KeptIntent::WoundDown;
                gathered.writes.wound_down.push(job_id);
                gathered
                    .landings
                    .extend(self.queue_resume(torrent, cx.now, true));
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
                        gathered.writes.met.push(job_id);
                        gathered
                            .landings
                            .extend(self.queue_resume(torrent, cx.now, true));
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
                if checks_start(run, starting, torrent.disk_held) {
                    match self.start_hold(torrent) {
                        StartHold::Short => {
                            torrent.disk_held = Some(DiskHeld::Starting);
                            run = false;
                        }
                        StartHold::Unplaced => run = false,
                        StartHold::Free if torrent.disk_held == Some(DiskHeld::Starting) => {
                            torrent.disk_held = None;
                        }
                        StartHold::Free => {}
                    }
                } else if !run && torrent.disk_held == Some(DiskHeld::Starting) {
                    torrent.disk_held = None;
                }
                let Some(attached) = &torrent.attached else {
                    return;
                };
                let result = match run_command(run, attached.handle.is_paused()) {
                    Some(RunCommand::Unpause) => attached.session.unpause(&attached.handle).await,
                    Some(RunCommand::Pause) => attached.session.pause(&attached.handle).await,
                    None => return,
                };
                // A run that ends or begins is not in upload mode: a wait for
                // room on it, and what a wake handed it, end.
                self.end_disk_room(torrent);
                torrent.ran(result);
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
                if let Some(attached) = &torrent.attached
                    && let Err(error) = attached
                        .session
                        .set_file_priorities(&attached.handle, &changes)
                        .await
                {
                    tracing::debug!(job_id = job_id.0, error = %error, "a torrent's file levels did not change");
                }
            }
            Intent::Limits(limits) => {
                if let Some(attached) = &torrent.attached {
                    attached.handle.set_ratelimits(limits);
                }
            }
        }
    }

    // The pass's rows, written in one transaction off the runtime, `live`
    // held across it as for every intent write: a resume taken between the
    // two would otherwise be written over. The pieces a torrent checked in
    // seed mode count as kept once they are.
    pub(super) async fn write_pass(&self, live: &mut LiveMap, writes: &mut PassWrites) {
        if writes.is_empty() {
            return;
        }
        let writes = std::mem::take(writes);
        #[cfg(test)]
        self.inner
            .pass_counts
            .send_modify(|counts| counts.writes += 1);
        let db = self.inner.db.clone();
        let written = tokio::task::spawn_blocking(move || {
            let result = db.write_torrent_pass(&writes);
            (writes, result)
        })
        .await;
        let (writes, error) = match written {
            Ok((writes, Ok(()))) => (writes, None),
            Ok((writes, Err(error))) => (writes, Some(error.to_string())),
            Err(error) => {
                tracing::warn!(error = %error, "could not keep what a pass over the torrents changed");
                return;
            }
        };
        let Some(error) = error else {
            for (job_id, _, count) in &writes.verified {
                if let Some(torrent) = live.get_mut(job_id) {
                    torrent.seed_verified = *count;
                }
            }
            return;
        };
        for job_id in &writes.left_seed_mode {
            tracing::warn!(job_id = job_id.0, error = %error, "could not record that a torrent left seed mode");
        }
        for (job_id, _, _) in &writes.verified {
            tracing::warn!(job_id = job_id.0, error = %error, "could not keep the pieces a torrent checked in seed mode");
        }
        if !writes.wound_down.is_empty() || !writes.met.is_empty() {
            tracing::warn!(error = %error, "could not keep the stops of torrents done");
        }
    }

    // What a pass queued, landed: its ledgers, then its checkpoints.
    async fn land(&self, gathered: Gathered) {
        for landing in gathered.landings {
            let _ = self.landed(Some(landing)).await;
        }
        for save in gathered.saves {
            self.checkpoint_landed(save).await;
        }
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
    #[cfg(test)]
    pub(super) async fn apply_goals(&self, now: i64) {
        let settings = self.settings();
        let rules = self.rules();
        let holds = HoldSet::default();
        let cx = Context {
            downloads_only: self.downloads_only(&settings),
            ..Self::context(&settings, &rules, &holds, now)
        };
        let mut gathered = Gathered::default();
        {
            let mut live = self.pass_lock().await;
            self.walk(&mut live, None, &[&Goals], &cx, &mut gathered)
                .await;
            self.write_pass(&mut live, &mut gathered.writes).await;
        }
        let removals = std::mem::take(&mut gathered.removals);
        self.land(gathered).await;
        self.remove_met(removals).await;
    }

    // The session's figures for each torrent, and the ledger kept from
    // them: written whenever it changed, with its peers once a minute while
    // they change, or now when `at_once`. The reading policy over every
    // torrent.
    pub(super) async fn read(&self, now: i64, at_once: bool) {
        let settings = self.settings();
        let rules = self.rules();
        let holds = HoldSet::default();
        let cx = Context {
            at_once,
            ..Self::context(&settings, &rules, &holds, now)
        };
        let mut gathered = Gathered::default();
        {
            let mut live = self.pass_lock().await;
            self.walk(&mut live, None, &[&Reading], &cx, &mut gathered)
                .await;
            self.write_pass(&mut live, &mut gathered.writes).await;
        }
        self.land(gathered).await;
    }

    // Which torrents the queue lets download, applied to their sessions.
    pub(super) async fn apply_queue(&self) {
        self.apply_turns(&[&Turns]).await;
    }

    // The queue applied, then each torrent's picking, under one take: for
    // each torrent its turn, then its picking, as the two passes one after
    // the other gave them.
    pub(super) async fn apply_queue_and_picking(&self) {
        self.apply_turns(&[&Turns, &Picking]).await;
    }

    // What each session runs is what is stored, read from the torrent's
    // handle and pushed only where it differs. A change the session refuses
    // (while it checks its files) is pushed again on the next pass.
    pub(super) async fn apply_picking(&self) {
        self.pick(None).await;
    }

    // Picking for the torrents in `only`, or for every one.
    pub(super) async fn pick(&self, only: Option<&BTreeSet<JobId>>) {
        self.only(only, &[&Picking]).await;
    }

    // The limits each torrent's session holds it to.
    pub(super) async fn apply_limits(&self) {
        self.only(None, &[&Limits]).await;
    }

    // `policies`, which gather nothing, over the torrents of `only`, or
    // every one.
    async fn only(&self, only: Option<&BTreeSet<JobId>>, policies: &[&dyn Policy]) {
        let settings = self.settings();
        let rules = self.rules();
        let holds = HoldSet::default();
        let cx = Self::context(&settings, &rules, &holds, self.inner.clock.now_ms());
        let mut gathered = Gathered::default();
        let mut live = self.pass_lock().await;
        self.walk(&mut live, only, policies, &cx, &mut gathered)
            .await;
        debug_assert!(gathered.landings.is_empty() && gathered.writes.is_empty());
    }
}

/// What the passes did, counted in tests: each take of `live`, each walk
/// over the torrents and each write of a pass's rows.
#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PassCounts {
    pub(super) locks: u64,
    pub(super) walks: u64,
    pub(super) writes: u64,
}
