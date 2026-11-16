// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// The tick writes a ledger whenever it changed, as it writes the
// checkpoint, and nothing while it stands still.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ledger_is_written_at_each_tick_it_changed() {
    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    // No session is built while the route is down.
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let written = rig.service.written();
    let base = written.borrow().ledgers;
    // What a session moved for it.
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&job_id)
        .unwrap()
        .counters
        .uploaded_bytes = 5;
    clock.shift_unix_ms(TICK.as_millis() as i64);
    rig.service.tick().await;
    assert_eq!(written.borrow().ledgers, base + 1);
    assert_eq!(
        rig.db
            .get_torrent_counters(job_id)
            .unwrap()
            .unwrap()
            .uploaded_bytes,
        5
    );
    clock.shift_unix_ms(TICK.as_millis() as i64);
    rig.service.tick().await;
    assert_eq!(written.borrow().ledgers, base + 1);
}

// However often a job is marked between ticks with no piece reaching it,
// its checkpoint is written once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_checkpoint_is_written_once_a_tick() {
    let rig = rig(set_clock());
    // No session is built while the route is down.
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let written = rig.service.written();
    rig.service.tick().await;
    let base = written.borrow().checkpoints;
    for _ in 0..3 {
        rig.service.inner.signal.mark(job_id);
    }
    rig.service.tick().await;
    assert_eq!(written.borrow().checkpoints, base + 1);
    rig.service.tick().await;
    assert_eq!(written.borrow().checkpoints, base + 1);
}

// A downloading torrent's checkpoint follows its pieces with no tick: it is
// written each time [`CHECKPOINT_PIECES`] more pieces reach the job, so the
// checkpoint on disk is never that many pieces behind what verified.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_checkpoint_follows_the_verified_pieces_between_ticks() {
    const PIECE: usize = 1024;
    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let pieces = 3 * CHECKPOINT_PIECES + CHECKPOINT_PIECES / 2;
    let bytes: Vec<u8> = (0..pieces as usize * PIECE)
        .map(|at| (at % 251) as u8)
        .collect();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set",
            &[("set/a.bin", &bytes)],
            PIECE,
            false,
        ))))
        .await
        .unwrap();
    let (intake, layout) = {
        let live = rig.service.inner.live.lock().await;
        let torrent = &live[&job_id];
        (
            torrent.intake.clone().unwrap(),
            torrent.layout.clone().unwrap(),
        )
    };
    let on_disk = || kept_pieces(&rig.db, job_id);
    let mut written = rig.service.written();
    let base = written.borrow().checkpoints;
    for piece in 0..pieces {
        let range = layout.piece_range(piece);
        intake
            .verified(
                piece,
                &bytes::Bytes::copy_from_slice(&bytes[range.start as usize..range.end as usize]),
            )
            .unwrap();
        let verified = piece + 1;
        if verified % CHECKPOINT_PIECES == 0 {
            let saves = base + u64::from(verified / CHECKPOINT_PIECES);
            written
                .wait_for(|written| written.checkpoints >= saves)
                .await
                .unwrap();
            assert_eq!(on_disk(), verified);
        }
        assert!(verified - on_disk() < CHECKPOINT_PIECES, "piece {piece}");
    }
    rig.service.stop().await;
}

// The checkpoint written as pieces reach the job, between ticks, carries the
// ledger the service holds as it is written: a crash recovers the bytes and
// the active seconds of the moment its pieces were taken, not those of the
// last tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_checkpoint_between_ticks_carries_the_ledger_with_its_pieces() {
    const PIECE: usize = 1024;
    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    // Twice what one write takes, so the job is not complete, and its
    // completion's own write of the ledger cannot stand in for this one.
    let pieces = CHECKPOINT_PIECES;
    let bytes: Vec<u8> = (0..2 * pieces as usize * PIECE)
        .map(|at| (at % 251) as u8)
        .collect();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set",
            &[("set/a.bin", &bytes)],
            PIECE,
            false,
        ))))
        .await
        .unwrap();
    let (intake, layout) = {
        let mut live = rig.service.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).unwrap();
        // What a session moved for it since the last tick.
        torrent.counters.downloaded_bytes = 1_805_238_272;
        torrent.counters.uploaded_bytes = 7;
        torrent.counters.active_duration_s = 24;
        (
            torrent.intake.clone().unwrap(),
            torrent.layout.clone().unwrap(),
        )
    };
    let mut written = rig.service.written();
    let base = written.borrow().checkpoints;
    for piece in 0..pieces {
        let range = layout.piece_range(piece);
        intake
            .verified(
                piece,
                &bytes::Bytes::copy_from_slice(&bytes[range.start as usize..range.end as usize]),
            )
            .unwrap();
    }
    written
        .wait_for(|written| written.checkpoints > base)
        .await
        .unwrap();
    let held = rig.service.inner.live.lock().await[&job_id].counters;
    let on_disk = rig.db.get_torrent_counters(job_id).unwrap().unwrap();
    assert_eq!(
        (
            on_disk.downloaded_bytes,
            on_disk.uploaded_bytes,
            on_disk.active_duration_s
        ),
        (
            held.downloaded_bytes,
            held.uploaded_bytes,
            held.active_duration_s
        )
    );
    assert_eq!(on_disk.downloaded_bytes, 1_805_238_272);
    assert_eq!(on_disk.active_duration_s, 24);
    assert_eq!(kept_pieces(&rig.db, job_id), pieces);
    rig.service.stop().await;
}

// libtorrent sends every resume-data write of a torrent through its disk job
// queue, and `torrent::abort` waits out the jobs outstanding: a save already
// on its way when the torrent stops lands before stop's own, never after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_save_on_its_way_at_stop_lands_before_stops_own() {
    use weaver_torrent::ResumePeers;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    // A ban the session holds and the database does not yet: stop writes
    // only what changed.
    let handle = handle_of(&rig.service, job_id).await;
    handle.ban_peers(&["127.0.0.9".parse().unwrap()]);
    let current = handle.resume_peers().unwrap();
    // An older save of the torrent on its way: the writer runs it, held
    // until the test lets it go.
    let older = ResumePeers {
        peers: vec!["127.0.0.2:6881".parse().unwrap()],
        banned: vec!["127.0.0.3".parse().unwrap()],
        found: Vec::new(),
    };
    assert_ne!(older, current);
    let (running, run) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::channel::<()>();
    rig.db
        .try_queue_job_write(job_id, "older_peers", move |db| {
            let _ = running.send(());
            let _ = held.recv();
            db.save_torrent_peers(job_id, &older).map(drop)
        })
        .unwrap();
    run.await.unwrap();
    let taken = rig.db.writes_taken();
    let parked = rig.db.writes_parked();

    let service = rig.service.clone();
    let stopping = tokio::spawn(async move { service.stop().await });
    // Stop's save is the next write the writer takes, and it takes it onto
    // the torrent's lane, parked behind the one on its way; stop waits for
    // it to land, so it queues nothing more meanwhile.
    while rig.db.writes_taken() == taken {
        tokio::task::yield_now().await;
    }
    assert_eq!(rig.db.writes_taken(), taken + 1);
    assert_eq!(rig.db.writes_parked(), parked + 1);
    assert!(!stopping.is_finished());
    release.send(()).unwrap();
    stopping.await.unwrap();
    assert_eq!(rig.db.torrent_peers(job_id).unwrap(), current);
}

// A checkpoint writes only what changed since the database last held the
// job: a second save of a job that did nothing writes no row, and a job
// brought back from its checkpoint writes none either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_checkpoint_writes_only_what_changed() {
    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set.rar",
            &[("set.rar", archive)],
            128,
            false,
        ))))
        .await
        .unwrap();
    let job = |service: &TorrentService| {
        let service = service.clone();
        async move { tee_of(&service, job_id).await }
    };
    let first = job(&rig.service).await;
    rig.service
        .checkpoint_tee(job_id, Arc::clone(&first), true)
        .await;
    let whole = first.with(|job| job.pending().tally());
    assert!(whole.rows > 0);
    rig.service
        .checkpoint_tee(job_id, Arc::clone(&first), true)
        .await;
    let again = first.with(|job| job.pending().tally());
    assert_eq!(again.checkpoints, whole.checkpoints + 1);
    assert_eq!(again.rows, whole.rows);
    // The tee's checkpoint is queued, not awaited.
    rig.db.flush_write_queue().await.unwrap();
    assert!(rig.db.load_graph_checkpoint(job_id).unwrap().is_some());
    rig.service.stop().await;

    let restarted = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&restarted).await;
    let restored = job(&restarted).await;
    restarted
        .checkpoint_tee(job_id, Arc::clone(&restored), true)
        .await;
    assert_eq!(restored.with(|job| job.pending().tally()).rows, 0);
    restarted.stop().await;
}

// An idle seed's times run on at every tick, but its ledger is not written
// at every tick for them: they go once TIMES_EVERY_MS has passed since it
// was last written, and a stop writes them as they stand.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_seeds_times_are_written_on_the_long_timer_not_each_tick() {
    let seeded = seeded().await;
    seeded.advance(super::TIMES_EVERY_MS);
    seeded.rig.service.tick().await;
    let written = seeded.stored();
    assert_eq!(written, seeded.kept().await);
    let ticks = TICK.as_millis() as i64;
    seeded.advance(ticks);
    seeded.rig.service.tick().await;
    seeded.advance(ticks);
    seeded.rig.service.tick().await;
    assert_eq!(seeded.stored(), written);
    assert_eq!(
        seeded.kept().await.active_duration_s,
        written.active_duration_s + 2 * TICK.as_secs()
    );
    seeded.advance(super::TIMES_EVERY_MS);
    seeded.rig.service.tick().await;
    assert_eq!(seeded.stored(), seeded.kept().await);
    seeded.advance(ticks);
    let at_stop = seeded.view().await.active_seconds;
    let rig_db = seeded.rig.db.clone();
    let job_id = seeded.job_id;
    seeded.stop().await;
    assert_eq!(
        rig_db
            .get_torrent_counters(job_id)
            .unwrap()
            .unwrap()
            .active_duration_s,
        at_stop
    );
}

// A torrent's wasted bytes are its own across restarts: the bytes of a
// piece that failed its check and of a block that arrived when already in
// hand go into its ledger, the next tick writes them, and the service a
// restart builds over the same database reads the same wasted bytes and
// the same two parts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrents_wasted_bytes_survive_a_restart() {
    let seeded = seeded().await;
    {
        let mut live = seeded.rig.service.inner.live.lock().await;
        let torrent = live.get_mut(&seeded.job_id).unwrap();
        // The session's totals after one 16 KiB piece failed its check
        // and one 16 KiB block came twice.
        let then = torrent.seen;
        let now = TorrentTotals {
            failed_bytes: then.failed_bytes + 16_384,
            redundant_bytes: then.redundant_bytes + 16_384,
            ..then
        };
        count_waste(&mut torrent.counters, &then, &now);
    }
    seeded.advance(TICK.as_millis() as i64);
    seeded.rig.service.tick().await;
    let kept = seeded.kept().await;
    assert_eq!((kept.failed_bytes, kept.redundant_bytes), (16_384, 16_384));
    let stored = seeded.stored();
    assert_eq!(
        (stored.failed_bytes, stored.redundant_bytes),
        (kept.failed_bytes, kept.redundant_bytes)
    );
    let parts = |support: &TorrentSupport| {
        (
            support.view.wasted_bytes,
            support.counters.failed_bytes,
            support.counters.redundant_bytes,
        )
    };
    let before = parts(&seeded.rig.service.support(seeded.job_id).await.unwrap());
    assert_eq!(before, (32_768, 16_384, 16_384));
    assert_eq!(seeded.view().await.wasted_bytes, before.0);

    let Seeded {
        rig,
        job_id,
        seeder,
        _seeds,
        ..
    } = seeded;
    rig.service.stop().await;
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    let after = parts(&again.support(job_id).await.unwrap());
    assert_eq!(after, before);
    assert_eq!(again.torrent(job_id).await.unwrap().wasted_bytes, before.0);
    again.stop().await;
    seeder.stop().await;
}

// A tick that finds a torrent's ledger as last written writes nothing of
// it; one that finds more wasted bytes writes the ledger once, with them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unchanged_ledger_is_not_written_and_wasted_bytes_are() {
    let seeded = seeded().await;
    seeded.advance(super::TIMES_EVERY_MS);
    seeded.rig.service.tick().await;
    let now = seeded.clock.unix_ms();
    let mut live = seeded.rig.service.inner.live.lock().await;
    let torrent = live.get_mut(&seeded.job_id).unwrap();
    assert!(torrent.take_resume(now, false).is_none());
    torrent.counters.redundant_bytes += 16_384;
    let written = torrent
        .take_resume(now, false)
        .and_then(|resume| resume.counters)
        .unwrap();
    assert_eq!(written, torrent.counters);
    assert!(torrent.take_resume(now, false).is_none());
    drop(live);
    seeded.stop().await;
}

// Ten thousand torrents added one after another write their times spread
// over the whole period: no tick takes more than twice its even share.
#[test]
fn sequential_jobs_spread_their_times_over_the_whole_period() {
    let ticks = (TIMES_EVERY_MS / TICK.as_millis() as i64) as usize;
    let mut per_tick = vec![0usize; ticks];
    let jobs = 10_000u64;
    for id in 1..=jobs {
        let offset = times_offset_ms(JobId(id));
        assert!((0..TIMES_EVERY_MS).contains(&offset));
        per_tick[(offset / TICK.as_millis() as i64) as usize] += 1;
    }
    let even = jobs as usize / ticks;
    let most = *per_tick.iter().max().unwrap();
    let least = *per_tick.iter().min().unwrap();
    assert!(most <= 2 * even, "{most} in one tick, even share {even}");
    assert!(least > 0, "a tick took none");
}
