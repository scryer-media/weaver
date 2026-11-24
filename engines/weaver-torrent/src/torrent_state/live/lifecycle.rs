// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use super::{hash_fetch, web_seed};
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use crate::core::lengths::ValidPieceIndex;
use crate::spawn_utils::DropOffShard;
use anyhow::Context;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::sync::{Notify, Semaphore, mpsc::unbounded_channel, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, debug_span, warn};

use crate::{file_ops::Verdict, piece_tracker::PieceTracker, type_aliases::PeerHandle};

use super::{peer::rate_estimator, peers::PeerStates, stats::atomic::AtomicStats};

use crate::torrent_state::{
    RunCounters, TorrentTotals, paused::TorrentStatePaused, utils::TimedExistence,
};

use super::{OPTIMISTIC_DISK_RETRY, RunGauges, TorrentStateLive, TorrentStateLocked};

impl TorrentStateLive {
    pub(crate) fn new(
        paused: TorrentStatePaused,
        fatal_errors_tx: tokio::sync::oneshot::Sender<anyhow::Error>,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<Arc<Self>> {
        let session = paused
            .shared
            .session
            .upgrade()
            .context("session is dead, cannot start torrent")?;
        let session_stats = session.stats.clone();
        // The torrent's rates average over the same window its connections'
        // rates do: sampled every tick, a default estimator spans under half
        // a second and reads 0 between the bursts of a slow upload.
        let down_speed_estimator = Arc::new(rate_estimator());
        let up_speed_estimator = rate_estimator();

        let have_bytes = paused.chunk_tracker.get_hns().have_bytes;
        let finished_at_start = paused.chunk_tracker.get_hns().finished();
        let lengths = *paused.chunk_tracker.get_lengths();
        let seed_at_start = paused.chunk_tracker.bytes_left() == 0;
        let super_seeding = paused.shared.options.super_seeding && seed_at_start;
        let peer_limit = paused.shared.options.peer_limit.unwrap_or(128);

        let (upload_turns, upload_turns_rx) = unbounded_channel::<PeerHandle>();

        let finished = paused.chunk_tracker.is_finished();
        let pieces = PieceTracker::new(paused.chunk_tracker);
        let slots = pieces.slots().clone();
        let state = Arc::new(TorrentStateLive {
            shared: paused.shared.clone(),
            metadata: paused.metadata.clone(),
            peers: PeerStates::founded_on(
                paused.peers,
                session_stats.peers.clone(),
                lengths.total_pieces(),
            )
            .allowing_multiple_connections_per_ip(Arc::clone(
                &session.allow_multiple_connections_per_ip,
            ))
            .sharing_session_bans(Arc::clone(&session.bad_data_bans))
            .ordering_connects_by(session.connect_order()),
            _locked: RwLock::new(TorrentStateLocked {
                finished,
                pieces: Some(pieces),
                fatal_errors_tx: Some(fatal_errors_tx),
                unflushed_bitv_bytes: 0,
            }),
            reads: arc_swap::ArcSwap::from_pointee(paused.files.read_turns()),
            files: DropOffShard::new(paused.files, &paused.shared.spawner),
            stats: AtomicStats {
                have_bytes: AtomicU64::new(have_bytes),
                ..Default::default()
            },
            run_counted: AtomicBool::new(false),
            lengths,
            peer_semaphore: Arc::new(Semaphore::new(peer_limit)),
            peer_limit,
            places: Mutex::new(HashMap::new()),
            tcp_wants: Default::default(),
            new_pieces_notify: Notify::new(),
            room_notify: Arc::new(Notify::new()),
            layer_fetch: hash_fetch::LayerFetch::new(paused.metadata.v2.as_ref()),
            finished_notify: Notify::new(),
            wanting_notify: Notify::new(),
            seed_at_start,
            down_speed_estimator,
            up_speed_estimator,
            rate_ticked: AtomicBool::new(false),
            cancellation_token,
            storage_fence: RwLock::new(()),
            tasks: TaskTracker::new(),
            session_stats,
            streams: paused.streams,
            slots,
            upload_turns,
            pex: crate::session::speaks_pex(
                !session.disable_pex,
                paused.metadata.info.info().private,
                paused.shared.options.disable_pex,
            ),
            super_seeding,
            // What a handshake of this run's first connections says.
            told_upload_only: AtomicBool::new(finished_at_start && !super_seeding),
            last_seen_complete: Mutex::new(None),
            upload_mode: watch::Sender::new(None),
            upload_epoch: AtomicU64::new(super::upload::FIRST_UPLOAD_EPOCH),
            given_back: Mutex::new(HashMap::new()),
            #[cfg(test)]
            retry_declined: watch::Sender::new(0),
            web_layout: web_seed::Layout::new(&paused.metadata, paused.shared.info_hash),
            uploaded_notify: Notify::new(),
            upload_watches: AtomicUsize::new(0),
            upload_origin: tokio::time::Instant::now(),
            last_upload: AtomicU64::new(0),
            utp: session.utp,
        });
        state.shared.run.store(Some(Arc::new(RunGauges {
            peers: state.peers.stats.clone(),
            download: state.down_speed_estimator.clone(),
        })));
        // The list a pause left is held to the filter as it is now. Under the
        // torrent's lock, as a filter change reads `live()` under it: the
        // change finds this run, or this run finds the change.
        state.refilter();

        // Upload mode ends once it lasted OPTIMISTIC_DISK_RETRY (libtorrent
        // `torrent::second_tick`), unless it waits for its owner. Only the
        // mode slept on ends: one entered again since, or turned over to
        // the owner, lasts on from then.
        state.spawn(
            debug_span!(parent: state.shared.span.clone(), "upload_mode_retry"),
            format!("[{}]upload_mode_retry", state.shared.id),
            {
                let mut entered = state.upload_mode.subscribe();
                let state = Arc::downgrade(&state);
                async move {
                    loop {
                        let Ok(Some(mode)) =
                            entered.wait_for(Option::is_some).await.map(|mode| *mode)
                        else {
                            return Ok(());
                        };
                        // A mode its owner ends is left be until it
                        // changes.
                        if !mode.retry {
                            #[cfg(test)]
                            if let Some(state) = state.upgrade() {
                                state.retry_declined.send_modify(|seen| *seen += 1);
                            }
                            if entered.changed().await.is_err() {
                                return Ok(());
                            }
                            continue;
                        }
                        tokio::time::sleep_until(mode.at + OPTIMISTIC_DISK_RETRY).await;
                        let Some(state) = state.upgrade() else {
                            return Ok(());
                        };
                        if *state.upload_mode.borrow() == Some(mode) {
                            state.set_upload_mode(false);
                        }
                    }
                }
            },
        );

        // Joins the session's rate tick each time a peer goes live; the
        // tick lets the run go once it moves nothing.
        state.spawn(
            debug_span!(parent: state.shared.span.clone(), "rate_tick_join"),
            format!("[{}]rate_tick_join", state.shared.id),
            {
                let went_live = state.peers.went_live.clone();
                let state = Arc::downgrade(&state);
                async move {
                    loop {
                        went_live.notified().await;
                        let Some(state) = state.upgrade() else {
                            return Ok(());
                        };
                        if let Some(session) = state.shared.session.upgrade() {
                            session.rate_tick.join(&state);
                        }
                    }
                }
            },
        );

        state.spawn(
            debug_span!(parent: state.shared.span.clone(), "peer_adder"),
            format!("[{}]peer_adder", state.shared.id),
            state.clone().task_peer_adder(),
        );

        state.spawn(
            debug_span!(parent: state.shared.span.clone(), "upload_scheduler"),
            format!("[{}]upload_scheduler", state.shared.id),
            state.clone().task_upload_scheduler(upload_turns_rx),
        );

        state.spawn(
            debug_span!(parent: state.shared.span.clone(), "web_seeds"),
            format!("[{}]web_seeds", state.shared.id),
            state.clone().task_web_seeds(),
        );
        Ok(state)
    }

    /// Samples the run's rate estimators and its live peers' rates at
    /// `now`; returns whether the run stays on the session's rate tick: it
    /// does while it has a live peer or a rate above nothing.
    pub(crate) fn sample_rates(&self, now: Instant) -> bool {
        let stats = self.stats_snapshot();
        let remaining = match self.lock_read("get_remaining_bytes").get_chunks() {
            Ok(chunks) => chunks.get_remaining_bytes(),
            Err(e) => {
                debug!(error = %e, "rate tick: no chunks, leaving the tick");
                self.rate_ticked.store(false, Ordering::SeqCst);
                return false;
            }
        };
        self.down_speed_estimator
            .add_snapshot(stats.fetched_bytes, Some(remaining), now);
        self.up_speed_estimator
            .add_snapshot(stats.uploaded_bytes, None, now);
        self.peers.for_each_live(|_, peer, live| {
            live.sample_rates(&peer.stats.counters, now);
        });
        if self.down_speed_estimator.bps() > 0 || self.up_speed_estimator.bps() > 0 {
            return true;
        }
        // Off the tick first, then look. The flag is taken down with a
        // swap, which reads the last join's write: a peer whose join came
        // before it is counted by the look, and one whose join comes after
        // finds the flag down and its join puts the run back on.
        self.rate_ticked.swap(false, Ordering::AcqRel);
        self.peers.stats.live.load(Ordering::Acquire) > 0
            && !self.rate_ticked.swap(true, Ordering::AcqRel)
    }

    #[track_caller]
    pub(crate) fn spawn(
        &self,
        span: tracing::Span,
        name: impl Into<Cow<'static, str>>,
        fut: impl std::future::Future<Output = crate::Result<()>> + Send + 'static,
    ) {
        self.spawn_on(None, span, name, fut);
    }

    /// As [`Self::spawn`], on `home`'s runtime when given.
    pub(crate) fn spawn_on(
        &self,
        home: Option<&tokio::runtime::Handle>,
        span: tracing::Span,
        name: impl Into<Cow<'static, str>>,
        fut: impl std::future::Future<Output = crate::Result<()>> + Send + 'static,
    ) {
        crate::core::spawn_utils::spawn_with_cancel_on(
            home,
            span,
            name,
            self.cancellation_token.clone(),
            self.tasks.track_future(fut),
        );
    }

    // A piece a seed-mode torrent took as had is checked the first time a
    // peer asks for it (libtorrent `peer_connection::fill_send_buffer` hashes
    // a piece not verified in seed mode before it serves it). One that
    // passes is had for good, and the storage hears the check found it. One
    // that fails ends seed mode and every piece is checked again
    // (`torrent::leave_seed_mode` with `check_files`). True when the piece
    // is served; the connection stays either way, and the check of every
    // piece closes it as ours, as `torrent::force_recheck` does
    // (`disconnect_all`).
    //
    // A piece is checked once however many ask for it meanwhile: the first
    // to ask checks it and the rest wait for what that check finds
    // (libtorrent skips a request whose piece is `verifying_piece`,
    // peer_connection.cpp:5316, v2.0.15). The wait here blocks its thread,
    // for a permit as for another's check: called off the async runtime.
    pub(crate) fn check_seeded(&self, piece: ValidPieceIndex) -> anyhow::Result<bool> {
        use crate::torrent_state::seed_mode::{Ask, Claim};
        let seed = &self.shared.options.seed;
        loop {
            match seed.ask(piece) {
                Ask::Served => return Ok(true),
                Ask::Refused => return Ok(false),
                Ask::Unchecked => {}
            }
            match seed.claim(piece) {
                Claim::Check(checking) => {
                    // The check holds one read turn of the torrent's
                    // volume, as a peer's does, and only while it runs.
                    let _turn = crate::torrent_state::seed_mode::read_turn(
                        &self.shared.spawner,
                        &self.reads.load(),
                    );
                    return self.check_claimed(piece, checking);
                }
                Claim::Wait(mut found) => {
                    // A check that ended in an error told nothing: the
                    // piece is claimed again.
                    if let Ok(served) = found.blocking_recv() {
                        crate::session_stats::check::CHECK_STATS.seed_checks_joined(1);
                        return Ok(served);
                    }
                }
            }
        }
    }

    // A peer's request for a block of `piece` waits here while the torrent
    // is in seed mode and the piece is not yet checked. The check runs on a
    // blocking thread of the session and holds one read turn of the
    // torrent's volume, as every upload read and initial check of it does:
    // as many seed-mode pieces are read at once as the volume has turns
    // (eight, one on a spinning disk), however many peers ask. A downloaded
    // piece's check runs on the session's hashing thread. libtorrent hashes a seed-mode
    // piece with a hash job that has no `sequential_access` flag
    // (peer_connection.cpp:5329-5337, v2.0.15), which goes to its generic
    // disk threads, `aio_threads` (10), not to its hashing thread
    // (mmap_disk_io.cpp:1683-1690), and allows each peer three
    // (peer_connection.cpp:5318-5319). Waiting for another's check holds
    // neither a thread nor a permit.
    pub(crate) async fn serve_seeded(
        self: &Arc<Self>,
        piece: ValidPieceIndex,
    ) -> anyhow::Result<bool> {
        use crate::torrent_state::seed_mode::{Ask, Claim};
        let seed = &self.shared.options.seed;
        loop {
            match seed.ask(piece) {
                Ask::Served => return Ok(true),
                Ask::Refused => return Ok(false),
                Ask::Unchecked => {}
            }
            match seed.claim(piece) {
                Claim::Check(checking) => {
                    let state = Arc::clone(self);
                    let turns = self.reads.load_full();
                    // On the session's pool: a uTP thread's runtime has a
                    // pool of its own, which nothing bounds.
                    return self
                        .shared
                        .spawner
                        .spawn_blocking_in_turn(&turns, move || {
                            state.check_claimed(piece, checking)
                        })
                        .await
                        .context("a seed-mode check did not run to its end")?;
                }
                Claim::Wait(mut found) => {
                    if let Ok(served) = found.recv().await {
                        crate::session_stats::check::CHECK_STATS.seed_checks_joined(1);
                        return Ok(served);
                    }
                }
            }
        }
    }

    // The check of a seed-mode piece by the one that claimed it. Those
    // that wait on the claim hear what it found; an error tells them
    // nothing and lets go of the claim.
    fn check_claimed(
        &self,
        piece: ValidPieceIndex,
        checking: crate::torrent_state::seed_mode::Checking,
    ) -> anyhow::Result<bool> {
        let served = self.check_unchecked(piece)?;
        checking.found(served);
        Ok(served)
    }

    // The check holds the run's turn at its storage while it reads it and
    // tells it the piece passed, as a downloaded piece's check does: the
    // run's end waits for the one under way, and one that would begin after
    // it finds no turn and reads nothing, an error that tells those waiting
    // on its claim nothing (libtorrent's stop job fences every disk job of
    // the storage, the seed-mode hash job among them,
    // `mmap_disk_io::async_stop_torrent`, mmap_disk_io.cpp:980, v2.0.15).
    //
    // A piece that fails leaves seed mode on a failure: from then on the
    // run serves no piece, one that passes meanwhile included, until the
    // check of every piece ends it (libtorrent lets every peer go at once,
    // `torrent::force_recheck` from `torrent::leave_seed_mode`).
    fn check_unchecked(&self, piece: ValidPieceIndex) -> anyhow::Result<bool> {
        use crate::torrent_state::seed_mode::Ask;
        let seed = &self.shared.options.seed;
        match seed.ask(piece) {
            Ask::Served => return Ok(true),
            Ask::Refused => return Ok(false),
            Ask::Unchecked => {}
        }
        let Some(_turn) = self.storage_turn() else {
            anyhow::bail!("the run ended before the seed-mode check of piece {piece} began");
        };
        let verdict = crate::torrent_state::seed_mode::counted_read(
            &self.shared.spawner,
            u64::from(self.lengths.piece_length(piece)),
            || self.file_ops().check_piece_whole(piece),
        );
        if matches!(verdict, Ok(Verdict::Passed)) {
            self.files.on_piece_checked(piece)?;
            if seed.checked(piece) {
                debug!("every piece passed its check: seed mode ends");
            }
            return Ok(seed.ask(piece) != Ask::Refused);
        }
        if seed.fail() {
            warn!(%piece, "a piece failed its check in seed mode: checking every piece");
            self.recheck_detached();
        }
        Ok(false)
    }

    // Check every piece again, from outside the run it ends.
    fn recheck_detached(&self) {
        let Some(session) = self.shared.session.upgrade() else {
            return;
        };
        let id = self.shared.id;
        tokio::spawn(async move {
            let Some(handle) = session.get(crate::api::TorrentIdOrHash::Id(id)) else {
                return;
            };
            if let Err(error) = session.force_recheck(&handle).await {
                warn!(error = %error, "could not check a torrent that left seed mode");
            }
        });
    }

    /// Resolves once the torrent is finished. The wait is taken before the
    /// check, so a finish between the two still wakes it (libtorrent
    /// `alert_manager::wait_for_alert` checks the queue and waits under the
    /// one lock `maybe_notify` signals under).
    pub async fn wait_until_completed(&self) {
        let mut finished = std::pin::pin!(self.finished_notify.notified());
        finished.as_mut().enable();
        if self.is_finished() {
            return;
        }
        finished.await;
    }

    /// Resolves once the torrent has every piece, whichever are wanted
    /// (libtorrent `torrent::is_seed`). The piece that makes it a seed
    /// finishes any selection, so it rings `finished_notify`; one that
    /// finishes a selection short of a seed is waited past. The wait is
    /// taken before the check, as in `wait_until_completed`.
    pub async fn wait_until_seed(&self) {
        loop {
            let mut finished = std::pin::pin!(self.finished_notify.notified());
            finished.as_mut().enable();
            if self.is_seed() {
                return;
            }
            finished.await;
        }
    }

    /// The run's turn at its storage, which every write and check it queued
    /// holds while it touches the storage; none once the run has ended. The
    /// run's end passes the fence after it is cancelled (`pause`), so it
    /// waits for the write and the check under way, and those still queued
    /// find no turn and are dropped: no job of a run touches the storage
    /// once it is handed on (libtorrent's stop job is such a fence,
    /// `mmap_disk_io::async_stop_torrent`, mmap_disk_io.cpp:980, v2.0.15).
    pub(super) fn storage_turn(&self) -> Option<RwLockReadGuard<'_, ()>> {
        // Taken again by a check dropped under a turn already held, so the
        // read never waits behind the run's end; the end cannot starve, as
        // every turn asked for after it is cancelled is given back at once.
        let turn = self.storage_fence.read_recursive();
        (!self.cancellation_token.is_cancelled()).then_some(turn)
    }

    /// Whether the run has ended, or is ending: its storage turns are over.
    pub(crate) fn ended(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    pub fn pause(&self) -> anyhow::Result<TorrentStatePaused> {
        // What a `stopped` announce of this run waits for before it reads
        // the counters, named before anything ends so no announce the end
        // sets off can miss it: libtorrent's `torrent::do_pause` disconnects
        // every connection, each counting the block it was cut in
        // (`peer_connection::disconnect`), before `stop_announcing` builds
        // `stopped`.
        self.tasks.close();
        *self.shared.ending_run.lock() = Some(self.tasks.clone());
        self.cancellation_token.cancel();
        // A paused torrent dials no one: it wants no TCP socket, and a dial
        // still under way leaves none.
        self.tcp_wants.close();
        // The write and the check under way end before the storage is handed
        // on, and none queued begins after: at most one check and one batch
        // of writes are waited for, before the torrent's lock is taken, as
        // both take it.
        drop(self.storage_fence.write());
        // The run ends here: its torrent holds nothing from now on, whatever
        // its closing connections still count.
        self.shared.run.store(None);

        // The peer list is read before the state lock: a dying peer holds
        // its own entry while it takes the state lock, so the state lock
        // never waits on a peer entry.
        self.peers.close_all(tokio::time::Instant::now());
        let peers = self.peers.idle_list();
        let mut g = self.lock_write("pause");

        // It should be impossible to make a fatal error after pausing.
        g.fatal_errors_tx.take();

        let piece_tracker = g
            .pieces
            .take()
            .context("bug: pausing already paused torrent")?;
        // into_chunks() will requeue any in-flight pieces
        let chunk_tracker = piece_tracker.into_chunks();

        *self.shared.last_run.lock() = Some(RunCounters {
            left_bytes: chunk_tracker.bytes_left(),
        });
        // Under the lock a closing connection counts under, so what it
        // drops counts in this run or after it, once.
        let mut moved = self.shared.moved.lock();
        self.run_counted.store(true, Ordering::Relaxed);
        let run = TorrentTotals::from(&self.stats_snapshot());
        *moved = *moved + run;
        drop(moved);

        Ok(TorrentStatePaused {
            shared: self.shared.clone(),
            metadata: self.metadata.clone(),
            files: self.files.take()?,
            chunk_tracker,
            streams: self.streams.clone(),
            peers,
        })
    }

    pub(super) fn on_fatal_error(&self, e: anyhow::Error) -> anyhow::Result<()> {
        let mut g = self.lock_write("fatal_error");
        let tx = g
            .fatal_errors_tx
            .take()
            .context("fatal_errors_tx already taken")?;
        let res = anyhow::anyhow!("fatal error: {:?}", e);
        if tx.send(e).is_err() {
            warn!(id=self.shared.id, info_hash=?self.shared.info_hash, "there's nowhere to send fatal error, receiver is dead");
        }
        Err(res)
    }

    /// A write made at `epoch` failed. One that later writes will likely
    /// repeat puts the torrent in upload mode, unless upload mode was left
    /// since the write was made; returns whether it was one (libtorrent
    /// `torrent::handle_disk_error`).
    pub(super) fn handle_disk_error(&self, error: &anyhow::Error, epoch: u64) -> bool {
        let refused = crate::storage::write_refused(error);
        if refused {
            warn!(
                id = self.shared.id,
                info_hash = ?self.shared.info_hash,
                "a write found no room, only uploading: {error:#}"
            );
            // An owner that wakes the torrent decides every refused write:
            // it waits for room, or fails the torrent, with no retry here.
            self.enter_upload_mode(!self.shared.options.owner_wakes_full_disk, epoch);
        }
        refused
    }

    // If we have all selected pieces but not necessarily all pieces.
    pub(crate) fn is_finished(&self) -> bool {
        self.get_hns().map(|h| h.finished()).unwrap_or_default()
    }

    /// Whether the torrent has every piece, whichever are wanted (libtorrent
    /// `torrent::is_seed`).
    pub(crate) fn is_seed(&self) -> bool {
        self.get_hns()
            .is_some_and(|h| h.have_bytes == self.lengths.total_length())
    }

    /// Whether the torrent had every piece when this run began.
    pub(crate) fn seed_at_start(&self) -> bool {
        self.seed_at_start
    }

    fn has_active_streams_unfinished_files(&self, state: &TorrentStateLocked) -> bool {
        let chunks = match state.get_chunks() {
            Ok(c) => c,
            Err(_) => return false,
        };
        self.streams
            .streamed_file_ids()
            .any(|file_id| !chunks.is_file_finished(&self.metadata.file_infos[file_id]))
    }

    // We might have the torrent "finished" i.e. no selected files. But if someone is streaming files despite
    // them being selected, we aren't fully "finished".
    pub(super) fn is_finished_and_no_active_streams(&self) -> bool {
        self.is_finished()
            && !self.has_active_streams_unfinished_files(
                &self.lock_read("is_finished_and_dont_need_peers"),
            )
    }

    /// What follows the torrent finishing, when it turned finished under
    /// this hold of the lock, which is let go here: by its last wanted
    /// piece had, or by the pieces it lacked being wanted no longer
    /// (libtorrent `torrent::finished`, from `piece_passed` and from
    /// `update_piece_priorities` alike).
    pub(super) fn settle_finish(
        &self,
        mut g: TimedExistence<RwLockWriteGuard<'_, TorrentStateLocked>>,
    ) -> anyhow::Result<()> {
        if !g.turned_finished()? {
            return Ok(());
        }
        g.try_flush_bitv(&self.shared, false);
        self.finished_notify.notify_waiters();

        if !self.has_active_streams_unfinished_files(&g) {
            // prevent deadlocks.
            drop(g);
            // Each peer's requester, woken by finished_notify, lets a
            // peer that only uploads go (PeerHandler::request_chunks).
            // A web seed only uploads too (libtorrent
            // `torrent::finished`).
            self.shared.web_seeds.disconnect_all();
            // The peers left are told we only upload now (libtorrent
            // `torrent::finished`).
            self.update_upload_only();
        }
        Ok(())
    }
}
