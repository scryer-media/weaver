// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::checkpoints::{Ledger, Resume};
use super::disk_room::DiskHeld;
use super::intake::{About, Resolving};
use super::moves::Moving;
use super::restore::Attached;
use super::tick::Leveling;
use super::views::TorrentPieces;
use super::{PEERS_EVERY_MS, Reading, TIMES_EVERY_MS, files_of, on_disk, times_offset_ms};
use crate::jobs::handle::HoldOrigin;
use crate::settings::model::TrackerRule;
use crate::torrents::identity::TorrentIdentity;
use crate::torrents::intake::Intake;
use crate::torrents::layout::PieceLayout;
use crate::torrents::sessions::RouteKey;
use crate::torrents::status::{Engine, Held, Observed};
use crate::torrents::store::{Kept, TorrentStore};
use crate::torrents::{
    FilePriority, TorrentCounters, TorrentFile, TorrentIntent, TorrentRecord, TorrentState,
    TrackerEntry, status,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use weaver_torrent::{ResumePeers, TorrentTotals, TrackerAnnounce};

// The tracker rule of id `id`, while it stands.
fn rule_of(id: Option<u32>, rules: &[TrackerRule]) -> Option<&TrackerRule> {
    let id = id?;
    rules.iter().find(|rule| rule.id == id)
}

// What the service keeps of each torrent while it lives.
pub(super) struct Live {
    pub(super) record: TorrentRecord,
    // Who it is to its peers and trackers from its add to its remove, in
    // every session it runs in.
    pub(super) identity: TorrentIdentity,
    // The tracker rule its trackers match, picked again whenever they or
    // the rules change: the trackers are what is kept, not the rule.
    pub(super) rule_id: Option<u32>,
    // The trackers it lists, as its rows in the database hold them, in
    // `in_table_order`: each change is written to the table first, then
    // here as a new list, under the lock that holds this torrent, so a
    // reader takes the list without copying it.
    pub(super) trackers: Arc<[TrackerEntry]>,
    // Whether its metainfo marks it private; read from the metainfo, never
    // kept apart from it.
    pub(super) private: bool,
    // The peers and bans its next start begins with: the one place a ban
    // made here lives until it is written.
    pub(super) resume: ResumePeers,
    // What the database holds of its ledger and peers.
    ledger: Ledger,
    pub(super) names: Vec<String>,
    pub(super) total: u64,
    pub(super) layout: Option<Arc<PieceLayout>>,
    // What it has in its files, once it has metainfo.
    pub(super) store: Option<Arc<TorrentStore>>,
    pub(super) intake: Option<Arc<Intake>>,
    pub(super) attached: Option<Attached>,
    // A magnet's metadata fetch, while it runs: its announce table, whose
    // trackers are announced to before the torrent is in a session, and
    // where it asks for peers.
    pub(super) fetching: Option<weaver_torrent::MetadataFetch>,
    // A magnet's metadata fetch, until it ends.
    pub(super) resolving: Option<Resolving>,
    // Its wait for its route's session to open, off the add or action that
    // asked for it, until the wait ends.
    pub(super) attaching: Option<tokio::task::AbortHandle>,
    pub(super) route: Option<RouteKey>,
    pub(super) held: Option<Held>,
    pub(super) about: About,
    pub(super) counters: TorrentCounters,
    // The payload its ledger held as downloaded when this run of weaver took
    // it: more since means it downloaded in this run.
    pub(super) downloaded_at_start: u64,
    // What the session reported at the last kept reading, for the ledger's
    // deltas; the next session it runs in counts on from here.
    pub(super) seen: TorrentTotals,
    // The instant its durations were last advanced to, with what each ran
    // past its whole seconds.
    timed: Timed,
    pub(super) engine: Engine,
    // Why the session last refused to pause or resume the torrent, until it
    // takes the next change.
    pub(super) run_error: Option<String>,
    // Why its restore is held, while it is: a checkpoint the database did
    // not give, tried again at each restore until it does.
    pub(super) restore_held: Option<String>,
    // Not one of the files its pieces are in was there when its store was
    // made: it is in no session, its pieces kept, until a start, a recheck
    // or a move looks again. Found again at each load, never kept.
    pub(super) files_missing: bool,
    // Waits for room on its disk, and how: the service's wait for it is in
    // `Inner::disk_waits`.
    pub(super) disk_held: Option<DiskHeld>,
    pub(super) queued: bool,
    // Its place among the torrents still downloading, from 1, as the queue
    // was last applied; None while it seeds (qBittorrent
    // `TorrentImpl::queuePosition`).
    pub(super) queue_rank: Option<u32>,
    // Who holds it stopped, while a hold does.
    pub(super) paused_by: Option<HoldOrigin>,
    // The operator's level for each file, as stored.
    pub(super) levels: Vec<FilePriority>,
    pub(super) speeds: (u64, u64),
    // Its established peers, the seeders among them, and the peers still
    // in their handshake.
    pub(super) peers: (u32, u32, u32),
    // Where a move goes, while it is under way.
    pub(super) moving: Option<Moving>,
    // A removal has begun writing its mark: no move starts, and a failure
    // leaves the torrent to the removal (`remove_as`).
    pub(super) removing: bool,
    // A failure that came while `removing` was set, for the removal to
    // act on if it does not go ahead (`fail`).
    pub(super) failed_removing: Option<String>,
    // A change of its files' levels that copies bytes out of its side
    // store, while it is under way.
    pub(super) leveling: Option<Leveling>,
    // Why its files' bytes did not copy out of its side store, until it is
    // started again: it is in error, stopped.
    pub(super) copy_error: Option<String>,
    // How many pieces had passed their check in seed mode when they were
    // last kept.
    pub(super) seed_verified: usize,
    // Whether the database refused the row that says it left seed mode,
    // and has not kept one since: warned of once, not at every pass that
    // writes it again.
    pub(super) seed_left_refused: bool,
    // The same for the row that keeps the pieces it checked in seed mode.
    pub(super) seed_verified_refused: bool,
    // Each file's bytes in hand: the checkpoint's until the session has
    // checked the files, then the session's.
    file_have: Vec<u64>,
    // Their sum, padding left out: the bytes it holds, kept with them so a
    // goal check walks no file.
    pub(super) have_bytes: u64,
    // The pieces it kept, until its store is back.
    pub(super) kept_pieces: u32,
    // Why unpacking refused it, as its history kept it when it was
    // restored: a restored job does not say.
    pub(super) failed: Option<String>,
    // What the boot read of its rows for its first store and its first
    // attach, each taken by the one it is for; a later one reads its row.
    pub(super) kept_at_boot: Option<Option<Kept>>,
    pub(super) verified_at_boot: Option<Option<Vec<u8>>>,
    // Held while a session takes it and while it is removed, so the two
    // never cross: a torrent removed is never put in a session after, and
    // one put in a session is removed from it.
    pub(super) gate: Arc<tokio::sync::Mutex<()>>,
}

impl Live {
    // Its files: the metainfo's names and lengths, at the levels it holds.
    pub(super) fn files(&self) -> Vec<TorrentFile> {
        files_of(self.layout.as_deref(), &self.levels)
    }

    // The tracker rule it is under, while that rule stands.
    pub(super) fn rule<'r>(&self, rules: &'r [TrackerRule]) -> Option<&'r TrackerRule> {
        rule_of(self.rule_id, rules)
    }

    // A torrent whose ledger and peers the database holds as given, as of
    // `now`. Its seeding run starts again from zero: the time Weaver was
    // down is no seeding.
    pub(super) fn new(
        record: TorrentRecord,
        mut counters: TorrentCounters,
        resume: ResumePeers,
        now: i64,
    ) -> Self {
        let written = counters;
        counters.consecutive_seeding_s = 0;
        // Spread by job, so torrents restored together do not all write
        // their times at one tick.
        let times_at_ms = now - times_offset_ms(record.job_id);
        Self {
            record,
            identity: TorrentIdentity::fresh(),
            rule_id: None,
            trackers: Arc::from(Vec::new()),
            private: false,
            ledger: Ledger {
                written: Some((written, resume.clone())),
                peers_at_ms: now,
                times_at_ms,
                lost: Arc::default(),
            },
            resume,
            names: Vec::new(),
            total: 0,
            layout: None,
            store: None,
            intake: None,
            attached: None,
            fetching: None,
            resolving: None,
            attaching: None,
            route: None,
            held: None,
            about: About::default(),
            downloaded_at_start: counters.downloaded_bytes,
            counters,
            seen: TorrentTotals::default(),
            timed: Timed::at(now),
            engine: Engine::Absent,
            run_error: None,
            restore_held: None,
            files_missing: false,
            disk_held: None,
            queued: false,
            queue_rank: None,
            paused_by: None,
            levels: Vec::new(),
            speeds: (0, 0),
            peers: (0, 0, 0),
            moving: None,
            removing: false,
            failed_removing: None,
            leveling: None,
            copy_error: None,
            seed_verified: 0,
            seed_left_refused: false,
            seed_verified_refused: false,
            file_have: Vec::new(),
            have_bytes: 0,
            kept_pieces: 0,
            failed: None,
            kept_at_boot: None,
            verified_at_boot: None,
            gate: Arc::default(),
        }
    }

    pub(super) fn complete(&self) -> bool {
        self.record.completed_at_ms.is_some()
    }

    // Complete, or with every piece it wants had as the session last said.
    // A torrent in seed mode has every piece as added and seeds from the
    // start (libtorrent `torrent_flags::seed_mode`: the picker is created
    // having all; qBittorrent queues it as a seed), until its session says
    // a piece failed and it left seed mode.
    pub(super) fn seeding(&self) -> bool {
        status::seeding(self.complete(), &self.engine) || self.record.seed_mode
    }

    // What it shows until its store is back, from what was stored: the
    // pieces it kept, as the store restored from them will have, and the
    // failure its history keeps.
    pub(super) fn kept(&mut self, failed: Option<String>, kept: Option<&Kept>) {
        if let (Some(kept), Some(layout)) = (kept, &self.layout) {
            let (count, file_have) = kept.had(layout);
            self.hold(file_have);
            self.kept_pieces = count;
        }
        self.failed = failed;
    }

    // Fold what the session moved since the last reading into the ledger,
    // and run its durations to `now`: taken before anything changes what
    // the torrent is doing, so what came before the change counts as what
    // the torrent was doing then.
    pub(super) fn settle(&mut self, now: i64) {
        let reading = self.reading(now);
        self.keep(reading);
    }

    // Keep each file's bytes in hand, and their sum.
    pub(super) fn hold(&mut self, file_have: Vec<u64>) {
        self.have_bytes = on_disk(self.layout.as_deref(), &file_have);
        self.file_have = file_have;
    }

    // Keep a reading as what the session last said; the bytes it moved
    // since the reading before are counted on the egress its leg runs on.
    pub(super) fn keep(&mut self, reading: Reading) {
        let (received, sent) = reading.moved;
        if let Some(leg) = self.attached.as_ref().and_then(|a| a.leg.as_ref()) {
            leg.network
                .record_torrent_bytes(leg.egress_id, received, sent);
        }
        self.hold(reading.file_have);
        self.counters = reading.counters;
        self.seen = reading.seen;
        self.timed = reading.timed;
    }

    // What of the ledger and peers the database does not hold yet, taken
    // as written, and none when nothing changed. The ledger goes whenever
    // it changed, so after a crash the torrent shows what its last tick
    // knew, as its pieces do. The peers go once a minute has passed since
    // they were last taken, or at once when `at_once`; they are the
    // session's while it runs the torrent, and one stopped by an error
    // keeps those last taken.
    pub(super) fn take_resume(&mut self, now: i64, at_once: bool) -> Option<Resume> {
        if self.ledger.lost.swap(false, Ordering::AcqRel) {
            self.ledger.written = None;
        }
        if at_once || now - self.ledger.peers_at_ms >= PEERS_EVERY_MS {
            if let Some(peers) = self
                .attached
                .as_ref()
                .and_then(|attached| attached.handle.resume_peers())
            {
                self.resume = peers;
            }
            self.ledger.peers_at_ms = now;
        }
        let times_due = at_once || now - self.ledger.times_at_ms >= TIMES_EVERY_MS;
        let (counters, peers) = match &self.ledger.written {
            Some((counters, peers)) => (
                (if times_due {
                    *counters != self.counters
                } else {
                    !same_but_times(counters, &self.counters)
                })
                .then_some(self.counters),
                (!same_peers(peers, &self.resume)).then(|| self.resume.clone()),
            ),
            None => (Some(self.counters), Some(self.resume.clone())),
        };
        if counters.is_some() {
            self.ledger.times_at_ms = now;
        }
        let kept = match (&counters, &self.ledger.written) {
            (None, Some((written, _))) => *written,
            _ => self.counters,
        };
        self.ledger.written = Some((kept, self.resume.clone()));
        (counters.is_some() || peers.is_some()).then(|| Resume {
            counters,
            peers,
            lost: Arc::clone(&self.ledger.lost),
        })
    }

    pub(super) fn resumed(&self) -> bool {
        self.record.intent() == TorrentIntent::Resumed
    }

    // The session took a change to the torrent's run, or refused it.
    pub(super) fn ran(&mut self, result: anyhow::Result<()>) {
        if let Err(error) = &result {
            tracing::warn!(job_id = self.record.job_id.0, error = %error, "a torrent's run did not change");
        }
        self.run_error = result.err().map(|error| format!("{error:#}"));
    }

    // Why unpacking refused the torrent's output, while that stands: the
    // job's word, or the one its history keeps.
    pub(super) fn refusal(&self) -> Option<String> {
        self.store
            .as_ref()
            .and_then(|store| store.tee())
            .and_then(|tee| tee.refusal())
            .or_else(|| self.failed.clone())
    }

    // A write found no room: the session fetches nothing for the torrent
    // and keeps seeding it, as it last reported.
    pub(super) fn upload_mode(&self) -> bool {
        matches!(
            self.engine,
            Engine::Live {
                upload_mode: true,
                ..
            }
        )
    }

    // The pieces, and how many of them it has verified; until its store
    // is back, how many it kept.
    pub(super) fn pieces(&self) -> Option<TorrentPieces> {
        let layout = self.layout.as_ref()?;
        let have = match &self.store {
            Some(store) => store.count(),
            None => self.kept_pieces,
        };
        Some(TorrentPieces {
            count: layout.piece_count(),
            size: layout.piece_length(),
            have,
        })
    }

    // How announcing to each tracker went: in the session's latest run, or
    // while a magnet's metadata is fetched, in the fetch.
    pub(super) fn announces(&self) -> Vec<TrackerAnnounce> {
        match &self.attached {
            Some(attached) => attached.handle.tracker_announces(),
            None => self
                .fetching
                .as_ref()
                .and_then(|fetch| fetch.trackers.as_ref())
                .map(|table| table.announces())
                .unwrap_or_default(),
        }
    }

    // What the session says of the torrent now, over what was kept of it:
    // the one reading the views show and the ledger keeps. Its durations
    // run to `now` as its bytes do: the time since the last kept reading
    // is active while the session runs the torrent, and finished while it
    // seeds.
    pub(super) fn reading(&self, now: i64) -> Reading {
        let mut reading = self.sampled(now);
        let (state, _) = status::project(&self.observed(reading.engine.clone()));
        reading.timed = self.timed.advanced(
            now,
            matches!(reading.engine, Engine::Live { .. }),
            state == TorrentState::Seeding,
            &mut reading.counters,
        );
        reading
    }

    // The session's figures for the torrent now, over what was kept of it,
    // with its durations as last kept.
    fn sampled(&self, now: i64) -> Reading {
        let Some(attached) = &self.attached else {
            return Reading {
                engine: Engine::Absent,
                file_have: self.file_have.clone(),
                checked: None,
                counters: self.counters,
                seen: self.seen,
                timed: self.timed,
                moved: (0, 0),
                run: (0, 0),
                speeds: (0, 0),
                peers: (0, 0, 0),
            };
        };
        let stats = attached.handle.stats();
        let engine = Engine::of(&stats);
        let mut file_have = self.file_have.clone();
        take_file_have(&mut file_have, &stats.file_progress, self.store.as_deref());
        // How far a check under way has read (libtorrent
        // `torrent_status::progress` while `checking_files`, torrent.cpp:2625
        // and :12278-12292, v2.0.15).
        let checked = matches!(engine, Engine::Starting | Engine::Checking).then(|| {
            if stats.total_bytes == 0 {
                0.0
            } else {
                (stats.progress_bytes as f64 / stats.total_bytes as f64).min(1.0)
            }
        });
        let live = stats.live.as_ref();
        // The session counts on from what the torrent had moved when it
        // took it, through every pause.
        let seen = attached.handle.totals();
        let (up, down) = (
            seen.uploaded_bytes.saturating_sub(self.seen.uploaded_bytes),
            seen.fetched_bytes.saturating_sub(self.seen.fetched_bytes),
        );
        // On the wire: the blocks and every other message around them.
        let moved = (
            down + seen
                .fetched_protocol_bytes
                .saturating_sub(self.seen.fetched_protocol_bytes),
            up + seen
                .uploaded_protocol_bytes
                .saturating_sub(self.seen.uploaded_protocol_bytes),
        );
        let mut counters = self.counters;
        counters.uploaded_bytes += up;
        counters.downloaded_bytes += down;
        count_waste(&mut counters, &self.seen, &seen);
        if up + down > 0 {
            counters.last_activity_ms = Some(now);
        }
        // The session's sighting of a complete copy, ours or a connected
        // peer's (libtorrent `torrent::status`'s `last_seen_complete`, the
        // `m_swarm_last_seen_complete` of `second_tick`); the latest kept
        // stands, as one from a run before or a peer since gone.
        let seen_complete = attached
            .handle
            .live()
            .and_then(|live| live.swarm_complete_ago())
            .map(|ago| now.saturating_sub(i64::try_from(ago.as_millis()).unwrap_or(i64::MAX)));
        counters.last_seen_complete_ms = counters.last_seen_complete_ms.max(seen_complete);
        Reading {
            engine,
            file_have,
            checked,
            counters,
            seen,
            timed: self.timed,
            moved,
            run: live.map_or((0, 0), |live| {
                (live.snapshot.fetched_bytes, live.snapshot.uploaded_bytes)
            }),
            speeds: live.map_or((0, 0), |live| {
                (live.download_speed.as_bytes(), live.upload_speed.as_bytes())
            }),
            peers: live.map_or((0, 0, 0), |live| {
                (
                    live.snapshot.peer_stats.live,
                    live.snapshot.peer_stats.live_seeders,
                    live.snapshot.peer_stats.connecting,
                )
            }),
        }
    }

    // Whether it was brought back with its record alone and its job is yet
    // to be restored from its checkpoint.
    pub(super) fn awaits_restore(&self) -> bool {
        self.store.is_none() && self.layout.is_some()
    }

    // Everything the state is read from, with the session reporting
    // `engine`.
    pub(super) fn observed(&self, engine: Engine) -> Observed {
        // Shown once its session is in upload mode for it.
        let disk_space = match self.disk_held {
            Some(DiskHeld::Writing) => matches!(
                engine,
                Engine::Live {
                    upload_mode: true,
                    ..
                }
            ),
            Some(DiskHeld::Starting) => true,
            None => false,
        };
        let engine = self
            .run_error
            .clone()
            .or_else(|| self.copy_error.clone())
            .or_else(|| self.restore_held.clone())
            .map_or(engine, Engine::Error);
        Observed {
            // In no session while its job is yet to come back, it shows that
            // it starts from what was saved, as qBittorrent shows a torrent
            // whose resume data it is still loading (CheckingResumeData).
            engine: match engine {
                Engine::Absent if self.awaits_restore() => Engine::Starting,
                engine => engine,
            },
            has_metadata: self.layout.is_some(),
            fetch: self.fetching.as_ref().map(|fetch| *fetch.phase.borrow()),
            complete: self.complete(),
            intent: self.record.intent(),
            queued: self.queued,
            paused_by: self.paused_by,
            failed: self.refusal(),
            held: self.held.clone(),
            moving: self.moving.is_some(),
            restore_held: self.restore_held.clone(),
            files_missing: self.files_missing,
            disk_space,
        }
    }
}

// Fold into the ledger the bytes the session threw away between its totals
// `then` and `now`, so its wasted bytes are the torrent's across restarts.
pub(super) fn count_waste(
    counters: &mut TorrentCounters,
    then: &TorrentTotals,
    now: &TorrentTotals,
) {
    counters.failed_bytes = counters
        .failed_bytes
        .saturating_add(now.failed_bytes.saturating_sub(then.failed_bytes));
    counters.redundant_bytes = counters
        .redundant_bytes
        .saturating_add(now.redundant_bytes.saturating_sub(then.redundant_bytes));
}

// The instant a torrent's durations run to, and the milliseconds each ran
// past the whole seconds its counter holds: a reading carries them over,
// so no part of a second is lost between readings.
#[derive(Debug, Clone, Copy)]
pub(super) struct Timed {
    at_ms: i64,
    pub(super) active_ms: u64,
    finished_ms: u64,
    consecutive_ms: u64,
}

impl Timed {
    pub(super) fn at(now: i64) -> Self {
        Self {
            at_ms: now,
            active_ms: 0,
            finished_ms: 0,
            consecutive_ms: 0,
        }
    }

    // Run on to `now`: the time since runs into the active duration while
    // `active`, and into the finished one and the current seeding run while
    // `seeding`; a reading that is not `seeding` ends the run, and it starts
    // again from zero. A clock stepped back credits nothing for that
    // reading, and the next runs from `now`.
    pub(super) fn advanced(
        self,
        now: i64,
        active: bool,
        seeding: bool,
        counters: &mut TorrentCounters,
    ) -> Self {
        let elapsed = u64::try_from(now - self.at_ms).unwrap_or(0);
        let run = |part: u64, running: bool, seconds: &mut u64| {
            if !running {
                return part;
            }
            let ms = part.saturating_add(elapsed);
            *seconds = seconds.saturating_add(ms / 1000);
            ms % 1000
        };
        Self {
            at_ms: now,
            active_ms: run(self.active_ms, active, &mut counters.active_duration_s),
            finished_ms: run(self.finished_ms, seeding, &mut counters.finished_duration_s),
            consecutive_ms: if seeding {
                run(
                    self.consecutive_ms,
                    true,
                    &mut counters.consecutive_seeding_s,
                )
            } else {
                counters.consecutive_seeding_s = 0;
                0
            },
        }
    }
}

// Whether two ledgers differ at most in the times that run on by themselves
// while a torrent sits: its durations and when a complete copy was last
// seen. Bytes moved, bytes wasted and activity are written at the tick they
// change.
fn same_but_times(a: &TorrentCounters, b: &TorrentCounters) -> bool {
    a.uploaded_bytes == b.uploaded_bytes
        && a.downloaded_bytes == b.downloaded_bytes
        && a.failed_bytes == b.failed_bytes
        && a.redundant_bytes == b.redundant_bytes
        && a.last_activity_ms == b.last_activity_ms
}

// The same peers and bans, in whatever order: the session lists the peers
// it never reached in no fixed order.
fn same_peers(a: &ResumePeers, b: &ResumePeers) -> bool {
    fn sorted<T: Ord + Copy>(items: &[T]) -> Vec<T> {
        let mut items = items.to_vec();
        items.sort_unstable();
        items
    }
    sorted(&a.peers) == sorted(&b.peers)
        && sorted(&a.banned) == sorted(&b.banned)
        && sorted(&a.found) == sorted(&b.found)
}

// Every file's bytes in hand. The session's progress counts only the files
// above Ignored, so each file's own figure is what is kept. While the
// session reports none, as while it checks the files, the store's figure
// stands: the pieces the check has found so far, or those the checkpoint
// it starts from vouches for, which are the pieces the torrent counts.
pub(super) fn take_file_have(kept: &mut Vec<u64>, reported: &[u64], store: Option<&TorrentStore>) {
    if !reported.is_empty() {
        kept.clear();
        kept.extend_from_slice(reported);
    } else if let Some(store) = store {
        store.file_have_into(kept);
    }
}
