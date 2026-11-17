// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Every torrent's followers share their readings, and each is told from them
// what `changes` tells a follower that reads for itself.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use super::listing::settled;
use super::*;
use crate::torrents::service::updates::{Follow, Need, Reading};

const TORRENTS: u64 = 10_000;

// A view of one torrent, read from a rig, to copy.
async fn one_view() -> TorrentView {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("copied", 3))))
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    rig.service.stop().await;
    view
}

fn copy(view: &TorrentView, id: u64) -> TorrentView {
    TorrentView {
        job_id: JobId(id),
        name: format!("t{id}"),
        ratio: (id % 13) as f64,
        ..view.clone()
    }
}

// The views at each step: TORRENTS of them, then steady fields moved in
// some, torrents gone and new ones come, and steps where nothing moves.
fn steps(view: &TorrentView) -> Vec<Vec<TorrentView>> {
    let mut views: Vec<TorrentView> = (0..TORRENTS).map(|id| copy(view, id)).collect();
    let mut steps = vec![views.clone()];
    let mut next_id = TORRENTS;
    for step in 1..14u64 {
        match step % 4 {
            // Nothing moves.
            0 => {}
            1 => {
                for view in views.iter_mut().skip(step as usize).step_by(97) {
                    view.ratio += 0.5;
                }
                for view in views.iter_mut().skip(3).step_by(501) {
                    view.state = TorrentState::Paused;
                }
            }
            2 => {
                let gone = (step as usize * 31) % 9_000;
                views.drain(gone..gone + 40);
                for _ in 0..25 {
                    views.push(copy(view, next_id));
                    next_id += 1;
                }
            }
            _ => {
                for view in views.iter_mut().skip(step as usize).step_by(211) {
                    view.tags = vec![format!("tag{step}")];
                }
                // A clock field alone, which every torrent's follower leaves
                // out.
                for view in views.iter_mut().step_by(7) {
                    view.active_seconds += 1;
                }
            }
        }
        steps.push(views.clone());
    }
    steps
}

// Followers on several schedules are told from the shared readings what
// `changes` tells each of them over the same views: one at every step, one
// at every other, one at every third, one from the fifth, and one that is
// given the same reading twice. Some readings are made with the one before
// them let go.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_reading_tells_each_follower_what_its_own_reading_would() {
    use fingerprint::ViewParts::Steady;

    let view = one_view().await;
    let steps = steps(&view);
    let at = Instant::now();
    let mut readings: Vec<Arc<Reading>> = Vec::new();
    for (step, views) in steps.iter().enumerate() {
        let before = readings.last().filter(|_| step % 5 != 3).map(Arc::as_ref);
        let reading = Reading::of(
            step as u64 + 1,
            at,
            (None, None),
            Vec::new(),
            views.clone(),
            before,
        );
        readings.push(Arc::new(reading));
    }

    struct Follower {
        from: usize,
        every: usize,
        last: Option<HashMap<JobId, u64>>,
        held: Option<Arc<Reading>>,
        told: usize,
    }
    let mut followers: Vec<Follower> = [(0, 1), (0, 2), (0, 3), (5, 1)]
        .into_iter()
        .map(|(from, every)| Follower {
            from,
            every,
            last: None,
            held: None,
            told: 0,
        })
        .collect();
    let mut moved_told = 0;
    for (step, views) in steps.iter().enumerate() {
        for follower in &mut followers {
            if step < follower.from || (step - follower.from) % follower.every != 0 {
                continue;
            }
            let own = updates::changes(&mut follower.last, views.clone(), Steady);
            let shared = readings[step].changes_since(follower.held.as_deref());
            assert_eq!(shared, own, "step {step}, every {}", follower.every);
            if let Some(changes) = &shared {
                follower.told += 1;
                moved_told += usize::from(!changes.full_update);
            }
            follower.held = Some(Arc::clone(&readings[step]));
        }
    }
    assert!(followers.iter().all(|follower| follower.told > 1));
    assert!(moved_told > 10);

    // The same reading twice tells nothing the second time.
    let reading = &readings[4];
    assert!(reading.changes_since(None).unwrap().full_update);
    assert_eq!(reading.changes_since(Some(reading)), None);
}

// A reading serves a follower at the revisions it has seen or after, begun
// after the count it woke at, and younger than its beat.
#[test]
fn a_reading_serves_the_need_it_meets() {
    let at = Instant::now();
    let beat = Duration::from_secs(3);
    let reading = Reading::of(7, at, (Some(5), Some(2)), Vec::new(), Vec::new(), None);
    let need = |queue, tags, begun_after, younger_than| Need {
        queue,
        tags,
        begun_after,
        younger_than,
    };
    assert!(reading.serves(Need::default(), at + beat * 10));
    assert!(reading.serves(need(Some(5), Some(2), None, None), at));
    assert!(reading.serves(need(Some(4), None, None, None), at));
    assert!(!reading.serves(need(Some(6), None, None, None), at));
    assert!(!reading.serves(need(None, Some(3), None, None), at));
    assert!(reading.serves(need(None, None, Some(6), None), at));
    assert!(!reading.serves(need(None, None, Some(7), None), at));

    let young = need(Some(5), Some(2), None, Some(beat));
    assert!(reading.serves(young, at));
    assert!(reading.serves(young, at + beat - Duration::from_millis(1)));
    assert!(!reading.serves(young, at + beat));
    assert!(!reading.serves(young, at + beat * 2));

    // A reading taken with no queue serves no follower that has one.
    let unqueued = Reading::of(8, at, (None, Some(2)), Vec::new(), Vec::new(), None);
    assert!(!unqueued.serves(need(Some(0), None, None, None), at));
    assert!(unqueued.serves(need(None, Some(2), None, None), at));
}

// Twenty followers of every torrent hear one tag change from one reading,
// and the reading is let go once none of them is left. The rig has no
// queue, and the beat is far past the test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn twenty_followers_hear_a_change_from_one_reading() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let mut jobs = Vec::new();
    for (name, seed) in [("one", 1), ("two", 2), ("three", 3)] {
        let job_id = rig
            .service
            .add_opened(AddRequest {
                paused: true,
                ..request(AddSource::Metainfo(plain(name, seed)))
            })
            .await
            .unwrap();
        jobs.push(job_id);
    }
    settled(&rig.service, jobs.len()).await;

    let beat = Duration::from_secs(86_400);
    let mut followers: Vec<_> = (0..20)
        .map(|_| rig.service.follow(Follow::Every, beat))
        .collect();
    let every = rig.service.torrents(None).await;
    for follower in &mut followers {
        let first = follower.next().await;
        assert!(first.full_update);
        assert_eq!(first.torrents, every);
    }

    let before = rig.service.readings_begun();
    rig.db.add_job_tags(&[jobs[1]], ["shared"]).unwrap();
    let tagged = rig
        .service
        .torrents(None)
        .await
        .into_iter()
        .find(|view| view.job_id == jobs[1])
        .unwrap();
    assert_eq!(tagged.tags, ["shared"]);
    for follower in &mut followers {
        let changes = follower.next().await;
        assert!(!changes.full_update);
        assert_eq!(changes.torrents, std::slice::from_ref(&tagged));
        assert!(changes.removed.is_empty());
    }
    assert_eq!(rig.service.readings_begun() - before, 1);

    assert!(rig.service.holds_reading().await);
    drop(followers);
    assert!(!rig.service.holds_reading().await);
    rig.service.stop().await;
}

// A torrent shows what it has the moment it seeds, and after a restart it
// seeds from what it kept: nothing is fetched again and its counters stand.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_complete_torrent_seeds_after_a_restart_without_fetching_again() {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, 16_384, true);

    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &private, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let (bytes, url) = (private.to_vec(), tracker(peer).await);

    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    on_loopback(&rig.routes);
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    rig.service.publish_to(handle);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    let total = data.len() as u64;
    handle_of(&rig.service, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    let view = seeding_view(&rig.service, &mut changes, job_id).await;
    assert_eq!(
        (view.have_bytes, view.downloaded_bytes, view.progress),
        (total, total, 1.0)
    );
    let sent = uploaded(&seeding);
    assert!(sent >= total, "the seeder sent {sent} of {total}");
    // Every piece verified, so nothing fetched was wasted.
    assert_eq!(view.wasted_bytes, 0);
    assert_eq!(
        view.pieces,
        Some(TorrentPieces {
            count: 4,
            size: 16_384,
            have: 4,
        })
    );
    // The ledger as the running tick keeps it: the time between two ticks
    // the torrent ran in its session is active, and it ran complete. The
    // tick writes the ledger it changed, and a clean stop writes the ledger
    // as it stands.
    rig.service.tick().await;
    rig.service.restored().await;
    clock.shift_unix_ms(30_000);
    rig.service.tick().await;
    rig.service.restored().await;
    let kept = rig.service.inner.live.lock().await[&job_id].counters;
    assert_eq!(kept.active_duration_s, 30);
    rig.service.stop().await;
    let ledger = rig.db.get_torrent_counters(job_id).unwrap().unwrap();
    assert_eq!(
        (
            ledger.downloaded_bytes,
            ledger.active_duration_s,
            ledger.last_seen_complete_ms,
        ),
        (total, 30, Some(EPOCH_MS + 30_000))
    );
    // What the server's recovery clears before anything starts.
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    again.publish_to(handle);
    again.open().await.unwrap();
    // Before the tick restores its job, it shows what was stored: complete,
    // so its file whole and every piece.
    let kept = again.torrent(job_id).await.unwrap();
    assert_eq!(
        (kept.have_bytes, kept.progress, kept.file_have),
        (total, 1.0, vec![total])
    );
    assert_eq!(kept.pieces.map(|pieces| pieces.have), Some(4));
    again.tick().await;
    again.restored().await;
    handle_of(&again, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    let view = seeding_view(&again, &mut changes, job_id).await;
    assert_eq!(
        (view.have_bytes, view.downloaded_bytes, view.progress),
        (total, total, 1.0)
    );
    assert_eq!(view.file_have, [total]);
    assert_eq!((view.active_seconds, view.wasted_bytes), (30, 0));
    assert_eq!(uploaded(&seeding), sent);
    again.stop().await;
    seeding.stop().await;
}

// A torrent's averages run over the seconds it reads as active, a second
// begun counting whole: its first second reads its bytes as its rate, not
// 0, and past it the bytes over the seconds is the average.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrents_first_second_reads_a_rate() {
    let seeded = seeded().await;
    seeded.advance(500);
    let view = seeded.view().await;
    assert!(view.downloaded_bytes > 0);
    assert_eq!(
        (view.active_seconds, view.download_avg_bps),
        (1, view.downloaded_bytes)
    );
    seeded.advance(3_443);
    let view = seeded.view().await;
    assert_eq!(
        (view.active_seconds, view.download_avg_bps),
        (4, view.downloaded_bytes / 4)
    );
    seeded.stop().await;
}

// A torrent's active time is read as of now, with its bytes: a view between
// ticks counts the time since the last one, its averages are its bytes
// over that time, and the tick lands the same figures in the ledger with no
// step. Seeding time runs with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrents_active_time_reads_as_of_now_between_ticks() {
    let seeded = seeded().await;
    seeded.advance(1_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let at_tick = seeded.view().await;
    let kept = seeded.kept().await;
    assert_eq!(at_tick.active_seconds, 1);
    assert_eq!(
        (
            kept.active_duration_s,
            kept.finished_duration_s,
            kept.downloaded_bytes,
        ),
        (
            at_tick.active_seconds,
            at_tick.active_seconds,
            at_tick.downloaded_bytes,
        )
    );

    seeded.advance(7_000);
    let later = seeded.view().await;
    assert_eq!(later.active_seconds, at_tick.active_seconds + 7);
    assert_eq!(
        (later.download_avg_bps, later.upload_avg_bps),
        (
            later.downloaded_bytes / later.active_seconds,
            later.uploaded_bytes / later.active_seconds,
        )
    );

    seeded.advance(3_000);
    let before = seeded.view().await;
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let after = seeded.view().await;
    assert_eq!(before.active_seconds, at_tick.active_seconds + 10);
    assert_eq!(
        (after.active_seconds, after.download_avg_bps),
        (before.active_seconds, before.download_avg_bps)
    );
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.active_duration_s, kept.finished_duration_s),
        (after.active_seconds, after.active_seconds)
    );
    seeded.stop().await;
}

// A pause takes the torrent's reading first: the time it ran since the
// last tick is active and seeding, and the time it stays paused is
// neither, between ticks or across one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pause_credits_the_time_before_it_and_nothing_after() {
    let seeded = seeded().await;
    seeded.advance(1_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let at_tick = seeded.view().await;
    seeded.advance(7_000);
    seeded.rig.service.pause(seeded.job_id).await.unwrap();
    let paused = seeded.view().await;
    assert_eq!(paused.state, TorrentState::Paused);
    assert_eq!(paused.active_seconds, at_tick.active_seconds + 7);
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.active_duration_s, kept.finished_duration_s),
        (paused.active_seconds, paused.active_seconds)
    );
    seeded.advance(7_000);
    assert_eq!(seeded.view().await.active_seconds, paused.active_seconds);
    seeded.advance(3_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    assert_eq!(seeded.view().await.active_seconds, paused.active_seconds);
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.active_duration_s, kept.finished_duration_s),
        (paused.active_seconds, paused.active_seconds)
    );
    seeded.stop().await;
}

// A pause ends the current seeding run: all the seeding there has been
// keeps the time before it, the run goes back to zero, and a resume starts
// a new run from there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pause_ends_the_seeding_run_and_keeps_the_whole() {
    let seeded = seeded().await;
    seeded.advance(10_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let whole = seeded.kept().await.finished_duration_s;
    assert_eq!(seeded.kept().await.consecutive_seeding_s, whole);
    let view = seeded.view().await;
    assert_eq!(
        (view.seeding_seconds, view.consecutive_seeding_seconds),
        (whole, whole)
    );

    // The pause takes the torrent's reading first, so the run it ends goes
    // to zero at the first reading that finds it paused.
    seeded.rig.service.pause(seeded.job_id).await.unwrap();
    assert_eq!(seeded.kept().await.finished_duration_s, whole);
    seeded.advance(6_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.finished_duration_s, kept.consecutive_seeding_s),
        (whole, 0)
    );
    let view = seeded.view().await;
    assert_eq!(
        (view.seeding_seconds, view.consecutive_seeding_seconds),
        (whole, 0)
    );

    seeded.rig.service.resume(seeded.job_id).await.unwrap();
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    seeded.advance(4_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.finished_duration_s, kept.consecutive_seeding_s),
        (whole + 4, 4)
    );
    seeded.stop().await;
}

// A route that goes down ends the current seeding run the same way, and
// all the seeding there has been keeps what it had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_route_going_down_ends_the_seeding_run() {
    let seeded = seeded().await;
    seeded.advance(10_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let whole = seeded.kept().await.finished_duration_s;

    seeded.rig.routes.set_down(Some("tunnel gone"));
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.finished_duration_s, kept.consecutive_seeding_s),
        (whole, 0)
    );
    seeded.advance(5_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.finished_duration_s, kept.consecutive_seeding_s),
        (whole, 0)
    );
    seeded.stop().await;
}

// The download and upload limits a session holds now.
fn session_limits(session: &Session) -> (Option<u32>, Option<u32>) {
    (
        session.ratelimits.get_download_bps().into(),
        session.ratelimits.get_upload_bps().into(),
    )
}

// An egress's upload quota reaches the open session on it live. Past the
// soft cap its uploads trickle and the torrent still seeds; at the hard cap
// they move nothing, and the torrent still seeds and still announces; lifted,
// the rate comes back. A torrent on another egress is untouched throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_egress_upload_quota_throttles_its_seeds_and_never_pauses_them() {
    use crate::bandwidth::ClassRates;

    let mut seeded = seeded().await;
    let service = seeded.rig.service.clone();
    let routes = Arc::clone(&seeded.rig.routes);
    let mut applied = service.inner.sessions.egress_limits_applied();
    // The follower put the limits in force once as it started, and waits.
    applied.wait_for(|applied| *applied >= 1).await.unwrap();
    let mut rings = *applied.borrow_and_update();
    let mut ring = |egress_id, rates| {
        routes.set_egress_rates(egress_id, rates);
        rings += 1;
        rings
    };

    routes.route(TorrentScope::Category(1), 2);
    routes.route_on_egress(TorrentScope::Category(1), 7);
    let other = service
        .add_opened(AddRequest {
            category: Some("tv".into()),
            ..request(AddSource::Metainfo(plain("other", 2)))
        })
        .await
        .unwrap();
    service.tick().await;
    service.restored().await;
    let seeding = session_of(&service, seeded.job_id).await;
    let elsewhere = session_of(&service, other).await;
    assert!(!Arc::ptr_eq(&seeding, &elsewhere));
    assert_eq!(session_limits(&seeding), (None, None));

    // Past the soft cap: the egress's uploads trickle.
    let trickle = ClassRates {
        download: None,
        upload: Some(crate::proxies::DEFAULT_UPLOAD_TRICKLE_BPS),
    };
    let at = ring(SYSTEM_EGRESS_ID, trickle);
    applied.wait_for(|applied| *applied >= at).await.unwrap();
    assert_eq!(
        session_limits(&seeding),
        (
            None,
            Some(crate::proxies::DEFAULT_UPLOAD_TRICKLE_BPS as u32)
        )
    );
    assert_eq!(session_limits(&elsewhere), (None, None));
    assert_eq!(seeded.view().await.state, TorrentState::Seeding);

    // At the hard cap: a budget of no bytes. The torrent still seeds, and
    // its next announce still goes out as a seed's.
    let stopped = ClassRates {
        download: None,
        upload: Some(0),
    };
    let at = ring(SYSTEM_EGRESS_ID, stopped);
    applied.wait_for(|applied| *applied >= at).await.unwrap();
    assert_eq!(session_limits(&seeding), (None, Some(0)));
    assert_eq!(session_limits(&elsewhere), (None, None));
    assert_eq!(seeded.view().await.state, TorrentState::Seeding);
    assert_eq!(seeded.reannounced().await[0].0, "");
    assert_eq!(seeded.view().await.state, TorrentState::Seeding);

    // Lifted: the session moves as it did.
    let at = ring(SYSTEM_EGRESS_ID, ClassRates::default());
    applied.wait_for(|applied| *applied >= at).await.unwrap();
    assert_eq!(session_limits(&seeding), (None, None));
    assert_eq!(session_limits(&elsewhere), (None, None));
    assert!(Arc::ptr_eq(
        &seeding,
        &session_of(&service, seeded.job_id).await
    ));
    seeded.stop().await;
}

// Time Weaver was down credits neither counter, and the restore starts the
// seeding run again: all the seeding there has been comes back as it was,
// the run at zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_keeps_the_whole_and_starts_the_seeding_run_again() {
    let seeded = seeded().await;
    seeded.advance(10_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let whole = seeded.kept().await.finished_duration_s;
    assert_eq!(seeded.kept().await.consecutive_seeding_s, whole);
    seeded.rig.service.stop().await;

    seeded.advance(600_000);
    let again = service_on(
        &seeded.rig.db,
        seeded.rig.dir.path(),
        &seeded.rig.routes,
        job_env(),
        Arc::clone(&seeded.clock) as Arc<dyn Clock>,
    );
    reopen(&again).await;
    // The restored torrent's session has checked it: the tick below reads
    // it seeding, not still at its check.
    handle_of(&again, seeded.job_id)
        .await
        .wait_until_initialized()
        .await
        .unwrap();
    let view = again.torrent(seeded.job_id).await.unwrap();
    assert_eq!(
        (view.seeding_seconds, view.consecutive_seeding_seconds),
        (whole, 0)
    );
    seeded.advance(3_000);
    again.tick().await;
    again.restored().await;
    let kept = again.inner.live.lock().await[&seeded.job_id].counters;
    assert_eq!(
        (kept.finished_duration_s, kept.consecutive_seeding_s),
        (whole + 3, 3)
    );
    again.stop().await;
    seeded.stop().await;
}

// A torrent that wants none of its files has every piece it wants: it seeds
// at 100 % with nothing in hand and no peers, though its job is not
// complete, and its seeding time runs. Wanting the file again takes it back
// to fetching, and its seeding time stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_that_wants_nothing_seeds_until_it_wants_a_file() {
    use FilePriority::*;
    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    rig.service
        .set_file_priorities(job_id, &[(0, Ignored)])
        .await
        .unwrap();
    rig.service.resume(job_id).await.unwrap();
    handle_of(&rig.service, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (
            view.state,
            view.progress,
            view.have_bytes,
            view.total_bytes,
            view.peers,
            view.completed_at_ms,
        ),
        (TorrentState::Seeding, 1.0, 0, 3000, 0, None)
    );

    rig.service.tick().await;
    rig.service.restored().await;
    clock.shift_unix_ms(5_000);
    rig.service.tick().await;
    rig.service.restored().await;
    let kept = rig.service.inner.live.lock().await[&job_id].counters;
    assert_eq!((kept.active_duration_s, kept.finished_duration_s), (5, 5));

    rig.service
        .set_file_priorities(job_id, &[(0, Normal)])
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.state, view.progress, view.completed_at_ms),
        (TorrentState::Stalled, 0.0, None)
    );
    clock.shift_unix_ms(5_000);
    rig.service.tick().await;
    rig.service.restored().await;
    let kept = rig.service.inner.live.lock().await[&job_id].counters;
    assert_eq!((kept.active_duration_s, kept.finished_duration_s), (10, 5));
    rig.service.stop().await;
}

// Turning torrents off reads every torrent before its session closes: the
// time it ran since the last tick is in the ledger.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turning_torrents_off_keeps_the_time_they_ran_since_the_tick() {
    let seeded = seeded().await;
    seeded.advance(1_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let at_tick = seeded.view().await;
    seeded.advance(7_000);
    let mut settings = seeded.rig.service.settings();
    settings.torrent.enabled = false;
    seeded.rig.service.set_settings(settings).await;
    let kept = seeded.kept().await;
    assert_eq!(
        (
            kept.active_duration_s,
            kept.finished_duration_s,
            kept.downloaded_bytes,
        ),
        (
            at_tick.active_seconds + 7,
            at_tick.active_seconds + 7,
            at_tick.downloaded_bytes,
        )
    );
    let off = seeded.view().await;
    assert_eq!(off.active_seconds, at_tick.active_seconds + 7);
    seeded.advance(7_000);
    assert_eq!(seeded.view().await.active_seconds, off.active_seconds);
    seeded.stop().await;
}

// A clock stepped back credits nothing for the reading that sees it, and
// the time from there on counts again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clock_stepped_back_credits_nothing_and_counts_on() {
    let seeded = seeded().await;
    seeded.advance(1_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let at_tick = seeded.view().await.active_seconds;
    seeded.advance(-60_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    assert_eq!(seeded.kept().await.active_duration_s, at_tick);
    seeded.advance(7_000);
    assert_eq!(seeded.view().await.active_seconds, at_tick + 7);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    assert_eq!(seeded.kept().await.active_duration_s, at_tick + 7);
    seeded.stop().await;
}

// No part of a second is lost at a tick: ticks 2.4 s, 2.4 s, 1.6 s and
// 1.6 s apart add the 8 s they span.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ticks_lose_no_part_of_a_second() {
    let seeded = seeded().await;
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let base = seeded.view().await.active_seconds;
    for ms in [2_400, 2_400] {
        seeded.advance(ms);
        seeded.rig.service.tick().await;
        seeded.rig.service.restored().await;
    }
    assert_eq!(seeded.kept().await.active_duration_s, base + 4);
    for ms in [1_600, 1_600] {
        seeded.advance(ms);
        seeded.rig.service.tick().await;
        seeded.rig.service.restored().await;
    }
    assert_eq!(seeded.kept().await.active_duration_s, base + 8);
    assert_eq!(seeded.view().await.active_seconds, base + 8);
    seeded.stop().await;
}

// Readings 2.4 s and 2.4 s apart add 4 s, the 4.8 s rounded down once;
// 1.6 s and 1.6 s add 3. A duration the torrent was not in takes nothing.
#[test]
fn a_reading_carries_the_part_of_a_second_to_the_next() {
    let run = |steps: &[i64]| {
        let mut counters = TorrentCounters::default();
        let mut timed = Timed::at(EPOCH_MS);
        let mut now = EPOCH_MS;
        for step in steps {
            now += step;
            timed = timed.advanced(now, true, false, &mut counters);
        }
        (counters.active_duration_s, counters.finished_duration_s)
    };
    assert_eq!(run(&[2_400, 2_400]), (4, 0));
    assert_eq!(run(&[1_600, 1_600]), (3, 0));
    assert_eq!(run(&[999]), (0, 0));
    assert_eq!(run(&[999, 1]), (1, 0));
    // Stepped back 60 s: nothing for that reading, then 7 s from there.
    assert_eq!(run(&[1_000, -60_000]), (1, 0));
    assert_eq!(run(&[1_000, -60_000, 7_000]), (8, 0));
}

// All the seeding there has been keeps running across a break; the current
// run starts again from zero at the first reading that is not seeding, and
// the part of a second it held goes with it.
#[test]
fn a_break_in_seeding_keeps_the_whole_and_restarts_the_run() {
    let run = |steps: &[(i64, bool)]| {
        let mut counters = TorrentCounters::default();
        let mut timed = Timed::at(EPOCH_MS);
        let mut now = EPOCH_MS;
        for (step, seeding) in steps {
            now += step;
            timed = timed.advanced(now, true, *seeding, &mut counters);
        }
        (counters.finished_duration_s, counters.consecutive_seeding_s)
    };
    assert_eq!(run(&[(10_000, true), (5_000, true)]), (15, 15));
    assert_eq!(
        run(&[(10_000, true), (5_000, false), (7_000, true)]),
        (17, 7)
    );
    // The 900 ms the run held is dropped by the break, so the 600 ms after
    // it make no second.
    assert_eq!(run(&[(1_900, true), (1_000, false), (1_600, true)]), (3, 1));
}

// A peer on `listener` that says it saw a complete copy `ago` seconds ago,
// then that it has piece 0: the BitTorrent handshake with the extension
// bit, an extended handshake carrying `complete_ago` (BEP 10), and a have.
// Returns once the session says it is interested, which it reads after the
// extended handshake. A connection that opens encrypted is let go: the
// session tries plaintext next.
async fn claims_complete(listener: &tokio::net::TcpListener, ago: u64) -> tokio::net::TcpStream {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const PROTOCOL: &[u8] = b"\x13BitTorrent protocol";
    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut theirs = [0u8; 68];
        if stream.read_exact(&mut theirs).await.is_err() || &theirs[..20] != PROTOCOL {
            continue;
        }
        let mut ours = PROTOCOL.to_vec();
        ours.extend_from_slice(&[0, 0, 0, 0, 0, 0x10, 0, 0]);
        ours.extend_from_slice(&theirs[28..48]);
        ours.extend_from_slice(b"-XX0000-claimscomplt");
        let extended = format!("d12:complete_agoi{ago}e1:mdee").into_bytes();
        let mut message = |id: &[u8], payload: &[u8]| {
            ours.extend_from_slice(
                &u32::try_from(id.len() + payload.len())
                    .unwrap()
                    .to_be_bytes(),
            );
            ours.extend_from_slice(id);
            ours.extend_from_slice(payload);
        };
        message(&[20, 0], &extended);
        message(&[4], &0u32.to_be_bytes());
        stream.write_all(&ours).await.unwrap();
        loop {
            let mut len = [0u8; 4];
            stream.read_exact(&mut len).await.unwrap();
            let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
            stream.read_exact(&mut body).await.unwrap();
            if body.first() == Some(&2) {
                return stream;
            }
        }
    }
}

// libtorrent `torrent::second_tick` keeps `m_swarm_last_seen_complete`, the
// latest of our own sighting and each connected peer's `complete_ago`, and
// `torrent::status` reports it as `last_seen_complete` (qBittorrent's
// `seen_complete`). A peer's word that it saw a complete copy dates the
// torrent's, though nobody connected is a seed; once the peer is gone the
// date stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peers_sighting_of_a_complete_copy_dates_the_torrents() {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, 16_384, true);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(peer) = listener.local_addr().unwrap() else {
        panic!("the peer listens on IPv4 loopback");
    };
    let (bytes, url) = (private.to_vec(), tracker(peer).await);

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    let seen = || {
        rig.db
            .get_torrent_counters(job_id)
            .unwrap()
            .unwrap()
            .last_seen_complete_ms
    };
    rig.service.tick().await;
    rig.service.restored().await;
    assert_eq!(seen(), None);

    let connection = claims_complete(&listener, 100).await;
    rig.service.tick().await;
    rig.service.restored().await;
    let at = rig
        .service
        .torrent(job_id)
        .await
        .unwrap()
        .last_seen_complete_ms
        .expect("the peer's sighting is the torrent's");
    assert!(at <= EPOCH_MS - 100_000, "{at}");

    // With the peer gone, as a pause disconnects every peer, nobody is
    // left to say it: the date kept stands, written as the pause is.
    rig.service.pause(job_id).await.unwrap();
    assert_eq!(seen(), Some(at));
    let handle = handle_of(&rig.service, job_id).await;
    assert!(handle.live().is_none());
    assert_eq!(rig.service.torrent(job_id).await.unwrap().peers, 0);
    drop(connection);
    rig.service.tick().await;
    rig.service.restored().await;
    assert_eq!(seen(), Some(at));
    rig.service.stop().await;
}

// A torrent's ETA is over the bytes it wants and has not: an Ignored file,
// never to come, adds nothing to it (qBittorrent `TorrentImpl::eta`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_eta_counts_only_the_wanted_bytes_left() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let files = [
        ("set/a.bin", &[1u8; 6_000][..]),
        ("set/b.bin", &[2u8; 30_000][..]),
    ];
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(metainfo("set", &files, 4_096, false)))
        })
        .await
        .unwrap();
    let settings = rig.service.settings();
    let mut live = rig.service.inner.live.lock().await;
    let torrent = live.get_mut(&job_id).unwrap();
    torrent.levels = vec![FilePriority::Normal, FilePriority::Ignored];
    let mut reading = torrent.reading(0);
    reading.file_have = vec![2_000, 10_000];
    reading.speeds = (1_000, 0);
    let view = rig.service.view_of(&settings, torrent, reading, 0);
    drop(live);
    assert_eq!((view.wanted_bytes, view.wanted_have_bytes), (6_000, 2_000));
    assert_eq!(view.eta_seconds, Some(4));
    rig.service.stop().await;
}
