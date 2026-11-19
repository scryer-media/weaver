// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::persistence::sql_runtime::SqlRuntime;

// A restart brings back what the operator asked, not the state last shown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_restores_the_intent_not_the_state() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let mut settings = rig.service.settings();
    settings.torrent.enabled = false;
    rig.service.set_settings(settings).await;
    rig.service.tick().await;
    rig.service.restored().await;
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().state,
        TorrentState::Stopped
    );
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent(),
        TorrentIntent::Resumed
    );
    let view = again.torrent(job_id).await.unwrap();
    assert_ne!(view.state, TorrentState::Stopped);
    assert_ne!(view.state, TorrentState::Paused);
    again.stop().await;
}

// None of a torrent's files there when it is loaded, as when the drive
// they are on is not mounted yet, holds it for its files missing. A start
// with them still gone leaves it held, making nothing where they were; a
// start once they are back runs it with every piece it kept (qBittorrent
// `TorrentImpl::start`, torrentimpl.cpp:2048-2053).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_whose_files_are_all_gone_at_load_is_held_until_a_start_finds_them() {
    let rig = rig(set_clock());
    let (job_id, dir, away) = gone_at_restart(&rig).await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    held_for_its_files(&again, job_id, &dir).await;
    // A tick puts it in no session either.
    again.tick().await;
    again.restored().await;
    held_for_its_files(&again, job_id, &dir).await;

    again.resume(job_id).await.unwrap();
    held_for_its_files(&again, job_id, &dir).await;

    std::fs::rename(&away, &dir).unwrap();
    again.resume(job_id).await.unwrap();
    again.opened(job_id).await;
    runs_with_every_piece(&again, job_id).await;
    again.stop().await;
}

// The hold is found again at each load, never kept: a restart with the
// files back runs the torrent with every piece.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_held_for_its_files_runs_after_a_restart_that_finds_them() {
    let rig = rig(set_clock());
    let (job_id, dir, away) = gone_at_restart(&rig).await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    held_for_its_files(&again, job_id, &dir).await;
    again.stop().await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();

    std::fs::rename(&away, &dir).unwrap();
    let third = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&third).await;
    runs_with_every_piece(&third, job_id).await;
    third.stop().await;
}

// A recheck lets go of the hold and of every piece kept, and the session
// it joins checks the files: back in place, every piece passes
// (qBittorrent `TorrentImpl::forceRecheck`, torrentimpl.cpp:1736-1744).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recheck_lets_go_of_a_torrent_held_for_its_files() {
    let rig = rig(set_clock());
    let (job_id, dir, away) = gone_at_restart(&rig).await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    held_for_its_files(&again, job_id, &dir).await;

    std::fs::rename(&away, &dir).unwrap();
    again.recheck(job_id).await.unwrap();
    again.opened(job_id).await;
    runs_with_every_piece(&again, job_id).await;
    again.stop().await;
}

// A start of every torrent looks again at each one held for its files, as a
// start of one does (qBittorrent `torrents/start` with `all` starts each
// torrent, `TorrentImpl::start`, torrentimpl.cpp:2048-2053): one whose files
// are back runs with every piece, one whose files are still gone stays held,
// and one the hold over every torrent stopped starts as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_of_every_torrent_looks_again_at_those_held_for_their_files() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (back, back_dir) = with_every_piece_of(&rig, "back", 1).await;
    let (gone, gone_dir) = with_every_piece_of(&rig, "gone", 2).await;
    let stopped = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("stopped", 3))))
        .await
        .unwrap();
    rig.service.stop().await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();
    let back_away = back_dir.with_extension("away");
    std::fs::rename(&back_dir, &back_away).unwrap();
    std::fs::rename(&gone_dir, gone_dir.with_extension("away")).unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let queue = queue();
    queue.pause_class(crate::torrents::CLASS).await.unwrap();
    again.publish_to(queue.clone());
    reopen(&again).await;
    held_for_its_files(&again, back, &back_dir).await;
    held_for_its_files(&again, gone, &gone_dir).await;
    assert_eq!(paused_by(&again, stopped).await, Some(HoldOrigin::Manual));

    std::fs::rename(&back_away, &back_dir).unwrap();
    let mut published = queue.state().subscribe_job_changes();
    queue.resume_class(crate::torrents::CLASS).await.unwrap();
    // The torrents held for their files are all looked at before any of
    // them joins a session, so the one that runs says the other was looked
    // at too.
    loop {
        published.borrow_and_update();
        let running = again.inner.live.lock().await[&back].attached.is_some();
        if running && paused_by(&again, stopped).await.is_none() {
            break;
        }
        published.changed().await.unwrap();
    }
    runs_with_every_piece(&again, back).await;
    held_for_its_files(&again, gone, &gone_dir).await;
    assert!(again.inner.live.lock().await[&stopped].attached.is_some());
    assert_ne!(
        again.torrent(stopped).await.unwrap().state,
        TorrentState::Paused
    );
    again.stop().await;
}

// A start of every torrent lets go of those held for their files without
// waiting on their route's session: while that route's open does not
// return they read as opening, and the next start of every torrent still
// looks again, and puts a torrent whose route's session runs in it. Once
// the open returns, each joins its route's session by itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_of_every_torrent_is_not_held_by_a_route_whose_session_does_not_open() {
    let rig = rig(set_clock());
    rig.routes.route(TorrentScope::Category(1), 9);
    let opening = route_key(&rig, TorrentScope::Category(1));
    rig.service.open().await.unwrap();
    let mut held = Vec::new();
    for (name, seed) in [("one", 1), ("two", 2), ("three", 3)] {
        let job_id = rig
            .service
            .add_opened(AddRequest {
                category: Some("tv".into()),
                ..request(AddSource::Metainfo(plain(name, seed)))
            })
            .await
            .unwrap();
        fetch_every_piece(&rig.service, job_id, &[seed; 3000]).await;
        let dir = PathBuf::from(
            rig.service
                .torrent(job_id)
                .await
                .unwrap()
                .storage_dir
                .unwrap(),
        );
        held.push((job_id, dir));
    }
    let (late, late_dir) = with_every_piece_of(&rig, "late", 4).await;
    let keeper = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("keeper", 5))))
        .await
        .unwrap();
    rig.service.stop().await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();
    for (_, dir) in &held {
        std::fs::rename(dir, dir.with_extension("away")).unwrap();
    }
    let late_away = late_dir.with_extension("away");
    std::fs::rename(&late_dir, &late_away).unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let queue = queue();
    again.publish_to(queue.clone());
    reopen(&again).await;
    again.opened(keeper).await;
    // The default route's session runs, the torrents' own route's has not
    // opened.
    assert!(again.inner.live.lock().await[&keeper].attached.is_some());
    for (job_id, dir) in &held {
        held_for_its_files(&again, *job_id, dir).await;
    }
    held_for_its_files(&again, late, &late_dir).await;

    // Their route's open does not return. It is asked for once every look
    // of the start is done, the late torrent's among them.
    rig.routes.unanswered.send_replace(true);
    let mut asked = rig.routes.asked.subscribe();
    let before = *asked.borrow_and_update();
    for (_, dir) in &held {
        std::fs::rename(dir.with_extension("away"), dir).unwrap();
    }
    queue.resume_class(crate::torrents::CLASS).await.unwrap();
    asked.wait_for(|asked| *asked > before).await.unwrap();
    held_for_its_files(&again, late, &late_dir).await;

    // The next start of every torrent finds the late one's files, and puts
    // it in the default route's session, which runs.
    std::fs::rename(&late_away, &late_dir).unwrap();
    let mut published = queue.state().subscribe_job_changes();
    queue.resume_class(crate::torrents::CLASS).await.unwrap();
    loop {
        published.borrow_and_update();
        if again.inner.live.lock().await[&late].attached.is_some() {
            break;
        }
        published.changed().await.unwrap();
    }
    runs_with_every_piece(&again, late).await;
    // The first start let them go to wait on their route's open.
    for (job_id, _) in &held {
        let view = again.torrent(*job_id).await.unwrap();
        assert_eq!(
            (view.route.as_deref(), view.route_reason),
            (Some(opening.as_str()), Some(TorrentRouteReason::Opening))
        );
        assert!(again.attaching(*job_id).await);
    }

    rig.routes.unanswered.send_replace(false);
    for (job_id, _) in &held {
        again.opened(*job_id).await;
        runs_with_every_piece(&again, *job_id).await;
        let view = again.torrent(*job_id).await.unwrap();
        assert_eq!(
            (view.route.as_deref(), view.route_reason),
            (Some(opening.as_str()), None)
        );
    }
    again.stop().await;
}

// A torrent held for its files moved to where they are now takes that
// place as its own: nothing is moved or made, and it runs with every piece
// (qBittorrent `TorrentImpl::handleMoveStorageJobFinished`,
// torrentimpl.cpp:2140-2146; libtorrent `move_storage` passes over a file
// that is not there, storage_utils.cpp:236-239 at v2.0.15).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_held_for_its_files_moved_to_where_they_are_runs() {
    let rig = rig(set_clock());
    let (job_id, dir, away) = gone_at_restart(&rig).await;
    let elsewhere = rig.dir.path().join("elsewhere");
    std::fs::create_dir(&elsewhere).unwrap();
    let there = elsewhere.join(dir.file_name().unwrap());
    std::fs::rename(&away, &there).unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    held_for_its_files(&again, job_id, &dir).await;

    again
        .set_location(job_id, elsewhere.to_string_lossy().into_owned())
        .await
        .unwrap();
    again.opened(job_id).await;
    runs_with_every_piece(&again, job_id).await;
    assert_eq!(
        again.torrent(job_id).await.unwrap().storage_dir.as_deref(),
        there.to_str()
    );
    assert_eq!(
        rig.db
            .get_torrent(job_id)
            .unwrap()
            .unwrap()
            .storage_dir
            .as_deref(),
        there.to_str()
    );
    assert!(!dir.exists());
    assert_eq!(
        std::fs::read_dir(&elsewhere)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>(),
        [there]
    );
    again.stop().await;
}

// A torrent held for its files moved to a place they are not either stays
// held there with every piece kept, and nothing is made at either place.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_held_for_its_files_moved_to_an_empty_place_stays_held() {
    let rig = rig(set_clock());
    let (job_id, dir, _away) = gone_at_restart(&rig).await;
    let empty = rig.dir.path().join("empty");
    std::fs::create_dir(&empty).unwrap();
    let there = empty.join(dir.file_name().unwrap());

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    held_for_its_files(&again, job_id, &dir).await;

    again
        .set_location(job_id, empty.to_string_lossy().into_owned())
        .await
        .unwrap();
    held_for_its_files(&again, job_id, &there).await;
    assert_eq!(
        again.torrent(job_id).await.unwrap().storage_dir.as_deref(),
        there.to_str()
    );
    assert!(!dir.exists());
    assert_eq!(std::fs::read_dir(&empty).unwrap().count(), 0);
    again.stop().await;
}

// A torrent whose work crashed the daemon is kept paused at the next start,
// saying why, and its job is not restored into the same panic: a panic in
// its tee's step leaves the torrent's marker, which stays until a resume
// takes it, and the resume restores it.
#[tokio::test(start_paused = true)]
async fn a_torrent_that_crashed_the_daemon_stays_paused_until_resumed() {
    use crate::runtime::panic_trap::{Trap, boot_marks, read_marks, trapped};
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
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
    let tee = tee_of(&rig.service, job_id).await;
    let marks = rig.dir.path().join("marks");
    let trap = Arc::new(Trap::new(&marks, "test"));
    let caught = trapped(
        &trap,
        std::panic::AssertUnwindSafe(|| {
            tee.step(
                1,
                |_, _, _| -> Result<(), crate::pipeline::graph::job::GraphError> {
                    panic!("in its unpacking")
                },
            )
        }),
    );
    assert!(caught.is_err());
    let written = read_marks(&marks);
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].job, Some(job_id));
    assert_eq!(written[0].message, "in its unpacking");
    drop(tee);
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.park_crashed(boot_marks(&marks, |job| job == job_id));
    reopen(&again).await;
    {
        let live = again.inner.live.lock().await;
        assert!(live[&job_id].store.is_none());
        assert!(live[&job_id].attached.is_none());
    }
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent(),
        TorrentIntent::Paused
    );
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.state, view.detail.as_deref()),
        (
            TorrentState::Paused,
            Some("crashed the daemon: in its unpacking")
        )
    );
    assert_eq!(read_marks(&marks).len(), 1);

    again.resume(job_id).await.unwrap();
    assert!(read_marks(&marks).is_empty());
    again.tick().await;
    again.restored().await;
    assert!(again.inner.live.lock().await[&job_id].attached.is_some());
    assert_eq!(again.torrent(job_id).await.unwrap().detail, None);
    again.stop().await;
}

// The restore of a torrent's job that panics leaves the torrent's marker,
// so the next start keeps it paused, saying why, rather than restoring it
// into the same panic. The panic here is the one a restore made off the
// runtime meets as its job joins the runs' mail.
#[tokio::test(start_paused = true)]
async fn a_panic_in_a_torrents_restore_leaves_its_marker_and_parks_it() {
    use crate::runtime::panic_trap::{Trap, boot_marks, read_marks, trapped};
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
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
    let tee = tee_of(&rig.service, job_id).await;
    assert!(tee.deliver(0, &bytes::Bytes::copy_from_slice(&archive[..128])));
    drop(tee);
    rig.service.stop().await;
    assert!(rig.db.load_graph_checkpoint(job_id).unwrap().is_some());

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    let (layout, passwords) = {
        let live = again.inner.live.lock().await;
        let torrent = &live[&job_id];
        assert!(torrent.store.is_none());
        (
            Arc::clone(torrent.layout.as_ref().unwrap()),
            again.archive_passwords(torrent, &again.global_passwords()),
        )
    };
    let plan = crate::torrents::tee::Plan::of(&layout).unwrap();
    let marks = rig.dir.path().join("marks");
    let trap = Arc::new(Trap::new(&marks, "test"));
    let caught = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                trapped(
                    &trap,
                    std::panic::AssertUnwindSafe(|| {
                        let _ = again
                            .tee::<std::convert::Infallible>(job_id, &layout, plan, passwords, Err);
                    }),
                )
            })
            .join()
            .unwrap()
    });
    assert!(caught.is_err());
    let written = read_marks(&marks);
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].job, Some(job_id));
    assert!(
        written[0].message.contains("Tokio"),
        "{}",
        written[0].message
    );
    assert!(
        written[0]
            .location
            .as_deref()
            .is_some_and(|location| location.contains("tee.rs")),
        "{:?}",
        written[0].location
    );
    again.stop().await;

    let third = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    third.park_crashed(boot_marks(&marks, |job| job == job_id));
    reopen(&third).await;
    assert!(third.inner.live.lock().await[&job_id].store.is_none());
    let view = third.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Paused);
    assert!(
        view.detail
            .as_deref()
            .is_some_and(|detail| detail.starts_with("crashed the daemon: ")),
        "{:?}",
        view.detail
    );
    third.stop().await;
}

// A resume is never undone by the restore pass that parks a crashed
// torrent: the pass reads the marker under the torrent's lock, which the
// resume takes after taking the marker. Resumed before the pass parks it,
// the torrent restores; parked first, the resume lets it go.
#[tokio::test(start_paused = true)]
async fn a_resume_racing_the_restore_pass_is_never_undone() {
    use crate::runtime::panic_trap::{Trap, boot_marks, read_marks};
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.service.stop().await;
    let marks = rig.dir.path().join("marks");
    let intent = || rig.db.get_torrent(job_id).unwrap().unwrap().intent();

    // The resume lands between the pass starting and its park.
    Trap::new(&marks, "test")
        .write(Some(job_id), "in its unpacking", None)
        .unwrap();
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.park_crashed(boot_marks(&marks, |job| job == job_id));
    again.open().await.unwrap();
    again.resume(job_id).await.unwrap();
    assert!(!again.park(job_id).await);
    assert_eq!(intent(), TorrentIntent::Resumed);
    assert_eq!(again.torrent(job_id).await.unwrap().detail, None);
    assert!(read_marks(&marks).is_empty());
    again.stop().await;

    // The park lands first: the resume after it lets the torrent go.
    Trap::new(&marks, "test")
        .write(Some(job_id), "in its unpacking", None)
        .unwrap();
    let third = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    third.park_crashed(boot_marks(&marks, |job| job == job_id));
    third.open().await.unwrap();
    assert!(third.park(job_id).await);
    assert_eq!(intent(), TorrentIntent::Paused);
    third.resume(job_id).await.unwrap();
    assert!(!third.park(job_id).await);
    assert_eq!(intent(), TorrentIntent::Resumed);
    let view = third.torrent(job_id).await.unwrap();
    assert_ne!(view.state, TorrentState::Paused);
    assert_eq!(view.detail, None);
    third.stop().await;
}

// A torrent stopped when it crashed the daemon stays stopped, and says
// why until a resume.
#[tokio::test(start_paused = true)]
async fn a_stopped_torrent_that_crashed_the_daemon_says_why() {
    use crate::runtime::panic_trap::{Trap, boot_marks};
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.service
        .set_intent(job_id, TorrentIntent::Stopped)
        .await
        .unwrap();
    rig.service.stop().await;
    let marks = rig.dir.path().join("marks");
    Trap::new(&marks, "test")
        .write(Some(job_id), "in its unpacking", None)
        .unwrap();
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.park_crashed(boot_marks(&marks, |job| job == job_id));
    reopen(&again).await;
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent(),
        TorrentIntent::Stopped
    );
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.state, view.detail.as_deref()),
        (
            TorrentState::Stopped,
            Some("crashed the daemon: in its unpacking")
        )
    );
    again.stop().await;
}

// Starting never waits on a route. After a restart whose route has not
// answered, the service starts with its torrents waiting: the torrent at
// the tick, the magnet on the task that fetches its metainfo. Once the
// route answers, the torrent goes into its session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_starts_before_the_torrents_route_answers() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let magnet = rig
        .service
        .add_opened(request(AddSource::Magnet(format!(
            "magnet:?xt=urn:btih:{hash}&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce"
        ))))
        .await
        .unwrap();
    rig.service.stop().await;

    rig.routes.unanswered.send_replace(true);
    let before = *rig.routes.asked.borrow();
    let mut asked = rig.routes.asked.subscribe();
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let mut ticks = again.ticks();
    // As the server starts it.
    again.inner.sessions.turn(true).await;
    again.start().await.unwrap();
    asked.wait_for(|asked| *asked > before).await.unwrap();
    // Its job is back, by the restore pass or by this call, which waits for
    // the pass's restore of it; the route has still not answered.
    again.restore_job(job_id).await;
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Queued);
    assert!(again.inner.live.lock().await[&job_id].attached.is_none());
    assert_eq!(
        again.torrent(magnet).await.unwrap().state,
        TorrentState::FetchingMetadata
    );

    rig.routes.unanswered.send_replace(false);
    // A whole tick begun after the route answered, and the restore pass,
    // put it in its session.
    let from = *ticks.borrow_and_update();
    ticks.wait_for(|ticks| *ticks >= from + 2).await.unwrap();
    again.restored().await;
    assert!(again.inner.live.lock().await[&job_id].attached.is_some());
    assert!(again.torrent(job_id).await.unwrap().route.is_some());
    again.stop().await;
}

// A torrent of two files where only the second is fetched: the first's
// front is never in hand, while its last bytes come in the piece that
// starts the second, so they are verified before anything says what the
// first file is.
const SPLIT_PIECE: usize = 16_384;

struct PartlyFetched {
    rig: Rig,
    clock: Arc<ManualClock>,
    job_id: JobId,
    seeding: Arc<Session>,
    _seeds: tempfile::TempDir,
    sizes: [u64; 2],
    sent: u64,
}

async fn partly_fetched() -> PartlyFetched {
    use FilePriority::*;
    let first: Vec<u8> = (0..(3 * SPLIT_PIECE + SPLIT_PIECE / 2) as u32)
        .map(|at| (at % 251) as u8)
        .collect();
    let second: Vec<u8> = (0..(SPLIT_PIECE / 2 + 2 * SPLIT_PIECE) as u32)
        .map(|at| (at % 241) as u8 ^ 0x5a)
        .collect();
    let files = [
        ("set/a.bin", first.as_slice()),
        ("set/b.bin", second.as_slice()),
    ];
    let private = metainfo("set", &files, SPLIT_PIECE, true);

    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &private, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let (bytes, url) = (private.to_vec(), tracker(peer).await);

    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(bytes))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
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
    // Pieces 3, 4 and 5: the second file whole, and the first file's last
    // half piece with it.
    assert_eq!(job_have(&rig.service, job_id).await, 3);
    let sent = uploaded(&seeding);
    PartlyFetched {
        rig,
        clock,
        job_id,
        seeding,
        _seeds: seeds,
        sizes: [first.len() as u64, second.len() as u64],
        sent,
    }
}

// The restart of a partly fetched torrent: it has every piece it had, and
// with the first file wanted it fetches that file's three missing pieces
// and nothing else. The file is wanted before the torrent goes into its
// session, so the seeder is of use from its first connection: a session
// that wants nothing of a seed drops it, and dials it again only much
// later.
async fn resumes_with_its_pieces(fetched: PartlyFetched) {
    use FilePriority::*;
    let PartlyFetched {
        rig,
        job_id,
        seeding,
        sizes,
        sent,
        ..
    } = fetched;
    // What the server's recovery clears before anything starts.
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    assert_eq!(job_have(&again, job_id).await, 3);
    again
        .set_file_priorities(job_id, &[(0, Normal)])
        .await
        .unwrap();
    again.tick().await;
    again.restored().await;
    let handle = handle_of(&again, job_id).await;
    handle.wait_until_completed().await.unwrap();
    assert_eq!(job_have(&again, job_id).await, 6);
    assert_eq!(again.torrent(job_id).await.unwrap().file_have, sizes);
    assert_eq!(uploaded(&seeding) - sent, 3 * SPLIT_PIECE as u64);
    again.stop().await;
    seeding.stop().await;
}

// A verified piece is durable: a torrent stopped part-way restarts with
// every piece it had, a piece no file had yet said what it was included,
// and fetches only the pieces it lacks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_stopped_part_way_restarts_with_its_pieces() {
    let fetched = partly_fetched().await;
    fetched.rig.service.stop().await;
    resumes_with_its_pieces(fetched).await;
}

// As a clean stop, a crash: the checkpoint written by then is all the
// restart has, and it has every piece the torrent had. The torrent
// completed with the files it wants, and the settle that completed it wrote
// that checkpoint; a tick after writes any it still owes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_that_crashed_part_way_restarts_with_its_pieces() {
    let fetched = partly_fetched().await;
    fetched.rig.service.tick().await;
    fetched.rig.service.restored().await;
    let (service, db, job_id) = (&fetched.rig.service, &fetched.rig.db, fetched.job_id);
    service
        .inner
        .settled
        .subscribe()
        .wait_for(|_| {
            db.get_torrent(job_id)
                .unwrap()
                .unwrap()
                .completed_at_ms
                .is_some()
                && !service
                    .inner
                    .settling
                    .load(std::sync::atomic::Ordering::SeqCst)
        })
        .await
        .unwrap();
    assert_eq!(kept_pieces(db, job_id), 3);
    // The process ends here: its sessions go with it, and nothing else is
    // written.
    fetched.rig.service.inner.sessions.stop_all().await;
    resumes_with_its_pieces(fetched).await;
}

// Starting never reads a torrent's files. The service opens with each
// torrent's record, and its view is served from what was kept, before its
// job is restored from its checkpoint; the job is restored at the tick, as
// the torrent goes into its session, with every piece it had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_restores_a_torrents_job_at_the_tick_not_at_start() {
    let PartlyFetched {
        rig,
        job_id,
        seeding,
        sizes,
        ..
    } = partly_fetched().await;
    rig.service.stop().await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();
    let ledger = rig.db.get_torrent_counters(job_id).unwrap().unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let written = again.written();
    let reads = || written.borrow().checkpoints_read;
    again.open().await.unwrap();
    // The tick has not run: the job's checkpoint is unread, and the torrent
    // shows what was stored of it: its ledger, its files, and what its
    // checkpoint has in hand of them.
    assert_eq!(reads(), 0);
    let kept = again.torrent(job_id).await.unwrap();
    assert_eq!(
        lifetime(&kept),
        (
            ledger.downloaded_bytes,
            ledger.uploaded_bytes,
            ledger.active_duration_s
        )
    );
    assert_eq!(kept.total_bytes, sizes.iter().sum::<u64>());
    assert_eq!(kept.files.len(), 2);
    assert_eq!(in_hand(&kept), checkpointed(sizes));

    // The tick begins the restore off its path; the job is back once the
    // restore pass ends.
    again.tick().await;
    again.restored().await;
    assert_eq!(reads(), 1);
    assert!(again.inner.live.lock().await[&job_id].attached.is_some());
    assert_eq!(job_have(&again, job_id).await, 3);
    let ticked = again.torrent(job_id).await.unwrap();
    assert_eq!(in_hand(&ticked), checkpointed(sizes));
    again.stop().await;
    seeding.stop().await;
}

// A restart while a check of every piece had found only some of them: the
// torrent shows before its job is back exactly what the job restored from
// its checkpoint holds, the pieces the check found and none it had yet to
// find.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_during_a_check_shows_what_its_job_will_hold() {
    let PartlyFetched {
        rig,
        job_id,
        seeding,
        sizes,
        ..
    } = partly_fetched().await;
    // The process ends while its session checks every piece and has found
    // piece 4 of the three the job held.
    rig.service.inner.sessions.stop_all().await;
    let intake = rig.service.inner.live.lock().await[&job_id]
        .intake
        .clone()
        .unwrap();
    assert!(intake.checking().unwrap());
    assert!(intake.found(4).unwrap());
    save_with_tee(&rig.service, job_id).await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let written = again.written();
    again.open().await.unwrap();
    assert_eq!(written.borrow().checkpoints_read, 0);
    let kept = in_hand(&again.torrent(job_id).await.unwrap());
    // Its progress is over the second file, the one it wants.
    let found = SPLIT_PIECE as u64;
    assert_eq!(
        kept,
        (
            vec![0, found],
            found,
            found as f64 / sizes[1] as f64,
            Some(1)
        )
    );

    // The restore alone, before the torrent attaches and its session checks
    // on.
    assert!(again.restore_job(job_id).await);
    assert_eq!(written.borrow().checkpoints_read, 1);
    assert!(again.inner.live.lock().await[&job_id].store.is_some());
    assert_eq!(in_hand(&again.torrent(job_id).await.unwrap()), kept);
    again.stop().await;
    seeding.stop().await;
}

// A torrent whose job is restored at start is given its session as soon as
// that job is back, not at the routes pass of a later tick: the first
// torrent restored seeds without waiting for the rest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_torrent_attaches_as_soon_as_its_job_is_back() {
    let PartlyFetched {
        rig,
        job_id,
        seeding,
        ..
    } = partly_fetched().await;
    rig.service.stop().await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    assert!(again.inner.live.lock().await[&job_id].attached.is_none());
    again.restore_jobs().await;
    {
        let live = again.inner.live.lock().await;
        assert!(live[&job_id].store.is_some());
        assert!(live[&job_id].attached.is_some());
    }
    assert!(again.inner.restoring.lock().unwrap().is_empty());
    again.stop().await;
    seeding.stop().await;
}

// A torrent restarted, its job not yet restored, on a service whose every
// restore permit `held` takes: a restore begun waits for one, so the job
// comes back only once `held` is dropped.
async fn restarted_held() -> (
    Rig,
    TorrentService,
    JobId,
    tokio::sync::OwnedSemaphorePermit,
) {
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.service.stop().await;
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    let held = Arc::clone(&again.inner.restore_permits)
        .acquire_many_owned(crate::torrents::service::RESTORES_AT_ONCE as u32)
        .await
        .unwrap();
    (rig, again, job_id, held)
}

// Restores run off the tick: with every restore held, the tick ends, a
// second tick ends, and the list answers, showing the torrent from its
// record as starting from what was saved (CheckingResumeData). Once the
// restores go on, its job comes back, read once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_tick_and_the_list_answer_while_restores_are_held() {
    use crate::torrents::service::listing::{Pick, Shown, page};
    let (_rig, again, job_id, held) = restarted_held().await;
    let written = again.written();
    let ticks = again.ticks();
    again.tick().await;
    again.tick().await;
    assert_eq!(*ticks.borrow(), 2);
    let listed = again
        .picked(Pick::default(), page(0, 10), Shown::LISTED)
        .await
        .unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|view| (view.job_id, view.state))
            .collect::<Vec<_>>(),
        [(job_id, TorrentState::CheckingResumeData)]
    );
    assert_eq!(written.borrow().checkpoints_read, 0);
    assert!(again.inner.live.lock().await[&job_id].store.is_none());

    drop(held);
    again.restored().await;
    assert_eq!(written.borrow().checkpoints_read, 1);
    assert!(again.inner.live.lock().await[&job_id].store.is_some());
    // Back in its session, the torrent checks what was saved of it until
    // the engine has it, and then no longer reads as starting.
    super::listing::settled(&again, 1).await;
    assert_ne!(
        again.torrent(job_id).await.unwrap().state,
        TorrentState::CheckingResumeData
    );
    again.stop().await;
}

// A pause while the torrent's restore waits acts on its stored row at once,
// and the job restored after it stays paused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pause_while_restoring_holds_once_the_job_is_back() {
    let (rig, again, job_id, held) = restarted_held().await;
    again.tick().await;
    again.pause(job_id).await.unwrap();
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent(),
        TorrentIntent::Paused
    );
    assert!(again.inner.live.lock().await[&job_id].store.is_none());

    drop(held);
    again.restored().await;
    assert!(again.inner.live.lock().await[&job_id].store.is_some());
    // It reads as checking what was saved of it until its session has it.
    super::listing::settled(&again, 1).await;
    assert_eq!(
        again.torrent(job_id).await.unwrap().state,
        TorrentState::Paused
    );
    again.stop().await;
}

// A removal while the torrent's restore waits queues behind that restore:
// the job is read once, by whichever restore takes the torrent first, and
// the removal then removes the torrent and its rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removal_while_restoring_waits_for_the_restore() {
    let (rig, again, job_id, held) = restarted_held().await;
    let written = again.written();
    again.tick().await;
    let removing = {
        let again = again.clone();
        tokio::spawn(async move { again.remove(job_id, false, true).await })
    };
    drop(held);
    removing.await.unwrap().unwrap();
    again.restored().await;
    assert_eq!(written.borrow().checkpoints_read, 1);
    assert!(again.torrent(job_id).await.is_none());
    assert_eq!(rig.db.get_torrent(job_id).unwrap(), None);
    assert!(again.inner.restoring.lock().unwrap().is_empty());
    again.stop().await;
}

// What a partly fetched torrent of files `sizes` has in hand: pieces 3, 4
// and 5, the first file's last half piece and the second file whole, which
// is all it wants.
fn checkpointed(sizes: [u64; 2]) -> (Vec<u64>, u64, f64, Option<u32>) {
    let have = [SPLIT_PIECE as u64 / 2, sizes[1]];
    let all = have.iter().sum::<u64>();
    (have.to_vec(), all, 1.0, Some(3))
}

// A paused torrent restarts paused, showing what its checkpoint has in hand
// before its job is back, and the same once it is. Until then it shows that
// it starts from what was saved, as qBittorrent shows CheckingResumeData.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paused_torrent_restarts_showing_what_it_has() {
    let PartlyFetched {
        rig,
        job_id,
        seeding,
        sizes,
        ..
    } = partly_fetched().await;
    rig.service.pause(job_id).await.unwrap();
    rig.service.stop().await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let written = again.written();
    again.open().await.unwrap();
    assert_eq!(written.borrow().checkpoints_read, 0);
    let kept = again.torrent(job_id).await.unwrap();
    assert_eq!(kept.state, TorrentState::CheckingResumeData);
    assert_eq!(in_hand(&kept), checkpointed(sizes));

    again.tick().await;
    again.restored().await;
    assert_eq!(written.borrow().checkpoints_read, 1);
    // Back in its session, it reads as paused once the session has started
    // it from what it was given, at the tick after.
    let _ = handle_of(&again, job_id)
        .await
        .wait_until_initialized()
        .await;
    again.tick().await;
    let ticked = again.torrent(job_id).await.unwrap();
    assert_eq!(ticked.state, TorrentState::Paused);
    assert_eq!(in_hand(&ticked), checkpointed(sizes));
    again.stop().await;
    seeding.stop().await;
}

// What a torrent's view shows of its ledger: the figures that outlive a
// run.
fn lifetime(view: &TorrentView) -> (u64, u64, u64) {
    (
        view.downloaded_bytes,
        view.uploaded_bytes,
        view.active_seconds,
    )
}

// A partly fetched torrent read at two ticks a tick apart: what it shows of
// its ledger then.
async fn ticked(fetched: &PartlyFetched) -> (u64, u64, u64) {
    let service = &fetched.rig.service;
    service.tick().await;
    service.restored().await;
    fetched.clock.shift_unix_ms(TICK.as_millis() as i64);
    service.tick().await;
    service.restored().await;
    let shown = lifetime(&service.torrent(fetched.job_id).await.unwrap());
    assert!(shown.0 >= 3 * SPLIT_PIECE as u64, "{shown:?}");
    assert_eq!(shown.2, TICK.as_secs(), "{shown:?}");
    shown
}

// The restart of a partly fetched torrent shows `shown` of its ledger.
async fn restarts_showing(fetched: PartlyFetched, shown: (u64, u64, u64)) {
    let PartlyFetched {
        rig,
        job_id,
        seeding,
        ..
    } = fetched;
    // What the server's recovery clears before anything starts.
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    handle_of(&again, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    assert_eq!(lifetime(&again.torrent(job_id).await.unwrap()), shown);
    again.stop().await;
    seeding.stop().await;
}

// A clean stop keeps the ledger exactly as the torrent showed it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_stopped_part_way_restarts_with_its_ledger() {
    let fetched = partly_fetched().await;
    let shown = ticked(&fetched).await;
    fetched.rig.service.stop().await;
    restarts_showing(fetched, shown).await;
}

// As its pieces do, a torrent's ledger outlives a crash: the restart shows
// the bytes the last tick knew, and the times last written with them, which
// a tick that moved no byte does not write (TIMES_EVERY_MS).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_that_crashed_part_way_restarts_with_its_last_ticks_ledger() {
    let fetched = partly_fetched().await;
    let shown = ticked(&fetched).await;
    let written = fetched
        .rig
        .db
        .get_torrent_counters(fetched.job_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        (written.downloaded_bytes, written.uploaded_bytes),
        (shown.0, shown.1)
    );
    let shown = (shown.0, shown.1, written.active_duration_s);
    // The process ends here: its sessions go with it, and nothing else is
    // written.
    fetched.rig.service.inner.sessions.stop_all().await;
    restarts_showing(fetched, shown).await;
}

// A checkpoint that does not restore says so, naming the job and why, and
// the torrent starts fresh rather than not at all.
#[tokio::test]
async fn a_corrupted_checkpoint_is_logged_and_the_torrent_starts_fresh() {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

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
    save_with_tee(&rig.service, job_id).await;
    rig.service.stop().await;
    assert!(rig.db.load_graph_checkpoint(job_id).unwrap().is_some());
    let datastore = rig.db.datastore();
    rig.db
        .run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE graph_nodes SET facts = {} WHERE job_id = {} AND node_id = 0",
                &[SqlArg::Bytes(vec![0xff; 4]), SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            Ok(())
        })
        .unwrap();

    let (lines, _guard) = crate::pipeline::graph::lines::Lines::capture_at(
        "weaver_server_core::torrents",
        tracing::Level::WARN,
    );
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    let heard = lines.of("a torrent's unpacking checkpoint did not restore; it starts fresh");
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(
        heard[0].contains(&format!(" job_id={} error=", job_id.0)),
        "{heard:?}"
    );
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(view.pieces.unwrap().have, 0);
    again.stop().await;
}

// A checkpoint the database fails to read is not one that does not
// restore: the torrent is not restored, not failed and not started fresh,
// its rows stand, and the next restore brings it back from them. Meanwhile
// it shows why, and says so once, not at every try.
#[tokio::test]
async fn a_checkpoint_the_database_fails_to_read_holds_the_restore_until_it_reads() {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

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
    save_with_tee(&rig.service, job_id).await;
    rig.service.stop().await;
    assert!(rig.db.load_graph_checkpoint(job_id).unwrap().is_some());
    let rename = |from: &'static str, to: &'static str| {
        let datastore = rig.db.datastore();
        rig.db
            .run_sql_blocking(async move {
                SqlRuntime::execute(
                    datastore.write_exec(),
                    &format!("ALTER TABLE {from} RENAME TO {to}"),
                    &[] as &[SqlArg],
                )
                .await?;
                Ok(())
            })
            .unwrap();
    };
    rename("graph_nodes", "graph_nodes_away");
    assert!(matches!(
        rig.db.load_graph_checkpoint(job_id),
        Err(crate::LoadError::Read(_))
    ));

    let (lines, _guard) = crate::pipeline::graph::lines::Lines::capture_at(
        "weaver_server_core::torrents",
        tracing::Level::WARN,
    );
    let fresh = |lines: &crate::pipeline::graph::lines::Lines| {
        let mut heard =
            lines.of("a torrent's unpacking checkpoint did not restore; it starts fresh");
        heard.extend(
            lines.of("a torrent's unpacking checkpoint could not be read; it starts fresh"),
        );
        heard
    };
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    let held = || lines.of("a torrent's restore is held; it is tried again");
    assert_eq!(held().len(), 1, "{:?}", held());
    assert!(
        held()[0].contains(&format!(" job_id={} error=", job_id.0)),
        "{:?}",
        held()
    );
    assert!(fresh(&lines).is_empty(), "{:?}", fresh(&lines));
    again.tick().await;
    again.restored().await;
    assert_eq!(held().len(), 1, "{:?}", held());
    {
        let live = again.inner.live.lock().await;
        let torrent = &live[&job_id];
        assert!(torrent.store.is_none());
        assert!(
            matches!(
                &torrent.observed(Engine::Absent).engine,
                Engine::Error(why) if why.starts_with("its restore is held: ")
            ),
            "{:?}",
            torrent.observed(Engine::Absent).engine
        );
    }

    rename("graph_nodes_away", "graph_nodes");
    again.tick().await;
    again.restored().await;
    {
        let live = again.inner.live.lock().await;
        let torrent = &live[&job_id];
        let store = torrent.store.as_ref().expect("restored once it reads");
        assert!(store.tee().is_some());
        assert_eq!(torrent.observed(Engine::Absent).engine, Engine::Absent);
    }
    assert!(fresh(&lines).is_empty(), "{:?}", fresh(&lines));
    assert!(rig.db.load_graph_checkpoint(job_id).unwrap().is_some());
    again.stop().await;
}

// A restart that finds an unpacking torrent's archive file gone, as when its
// drive is not there yet, does not fail the torrent: it is held for its
// files missing with every piece it kept, and nothing is read from the
// files for its tee. Its tee, whose checkpoint did not restore, starts
// fresh.
#[tokio::test]
async fn a_restart_that_finds_an_unpacking_torrents_file_gone_holds_it_with_its_pieces() {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
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
    let (layout, intake) = {
        let live = rig.service.inner.live.lock().await;
        let torrent = &live[&job_id];
        (
            torrent.layout.clone().unwrap(),
            torrent.intake.clone().unwrap(),
        )
    };
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        intake as Arc<dyn Pieces>,
        rig.service.resident(job_id),
    );
    let lengths = Lengths::new(
        layout.files().map(|file| file.length).sum(),
        layout.piece_length() as u32,
    )
    .unwrap();
    let fed = 2;
    for piece in 0..fed {
        let range = layout.piece_range(piece);
        let index = lengths.try_validate_piece_index(piece).unwrap();
        assert!(storage.reserve_piece(index, &Arc::default()));
        for span in layout.spans(piece) {
            let at = (range.start + span.at) as usize;
            storage
                .pwrite_all(
                    span.file,
                    span.range.start,
                    &archive[at..at + span.range.len() as usize],
                )
                .unwrap();
        }
        storage.on_piece_completed(index).unwrap();
    }
    let dir = PathBuf::from(
        rig.service
            .torrent(job_id)
            .await
            .unwrap()
            .storage_dir
            .unwrap(),
    );
    save_with_tee(&rig.service, job_id).await;
    rig.service.stop().await;
    assert_eq!(
        rig.service
            .inner
            .db
            .torrent_pieces(job_id)
            .unwrap()
            .unwrap()
            .had(&layout)
            .0,
        fed
    );
    std::fs::remove_file(dir.join("set.rar")).unwrap();
    let datastore = rig.db.datastore();
    rig.db
        .run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE graph_nodes SET facts = {} WHERE job_id = {} AND node_id = 0",
                &[SqlArg::Bytes(vec![0xff; 4]), SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            Ok(())
        })
        .unwrap();

    let (lines, _guard) = crate::pipeline::graph::lines::Lines::capture_at(
        "weaver_server_core::torrents",
        tracing::Level::WARN,
    );
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    let heard = lines.of("a torrent's pieces did not read from its files; they are fetched again");
    assert!(heard.is_empty(), "{heard:?}");
    assert_eq!(
        lines
            .of("a torrent's unpacking checkpoint did not restore; it starts fresh")
            .len(),
        1
    );
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::FilesMissing);
    assert_eq!(view.pieces.unwrap().have, fed);
    assert!(again.inner.live.lock().await[&job_id].attached.is_none());
    assert!(!dir.join("set.rar").exists());
    again.stop().await;
}

// libtorrent `storage_utils::verify_resume_data`: a restart has the pieces
// it kept only while its files hold them. A restart with its file as it
// was has every piece; one with its file cut short has only the pieces
// still in it, says so once, and fetches the rest again.
#[tokio::test]
async fn a_restart_has_no_piece_its_file_lost() {
    let data: Vec<u8> = (0..512u32).map(|at| (at % 251) as u8 ^ 0x5a).collect();
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "a.bin",
            &[("a.bin", &data)],
            128,
            false,
        ))))
        .await
        .unwrap();
    let (layout, intake) = {
        let live = rig.service.inner.live.lock().await;
        let torrent = &live[&job_id];
        (
            torrent.layout.clone().unwrap(),
            torrent.intake.clone().unwrap(),
        )
    };
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        intake as Arc<dyn Pieces>,
        rig.service.resident(job_id),
    );
    let lengths = Lengths::new(data.len() as u64, layout.piece_length() as u32).unwrap();
    for piece in 0..4 {
        let index = lengths.try_validate_piece_index(piece).unwrap();
        assert!(storage.reserve_piece(index, &Arc::default()));
        let range = layout.piece_range(piece);
        storage
            .pwrite_all(
                0,
                range.start,
                &data[range.start as usize..range.end as usize],
            )
            .unwrap();
        storage.on_piece_completed(index).unwrap();
    }
    let dir = PathBuf::from(
        rig.service
            .torrent(job_id)
            .await
            .unwrap()
            .storage_dir
            .unwrap(),
    );
    save_with_tee(&rig.service, job_id).await;
    rig.service.stop().await;
    assert_eq!(kept_pieces(&rig.db, job_id), 4);

    let lost = "a torrent's pieces are not in its files; they are checked or fetched again";
    let (lines, _guard) = crate::pipeline::graph::lines::Lines::capture_at(
        "weaver_server_core::torrents",
        tracing::Level::WARN,
    );
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(again.torrent(job_id).await.unwrap().pieces.unwrap().have, 4);
    assert!(lines.of(lost).is_empty());
    again.stop().await;
    assert_eq!(kept_pieces(&rig.db, job_id), 4);

    // Cut to 256 bytes: pieces 0 and 1 are still in it, 2 and 3 are not.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join("a.bin"))
        .unwrap();
    file.set_len(256).unwrap();
    drop(file);
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    let heard = lines.of(lost);
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(
        heard[0].contains(&format!(" job_id={} pieces=2", job_id.0)),
        "{heard:?}"
    );
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(view.pieces.unwrap().have, 2);
    assert_eq!(view.file_have, vec![256]);
    again.stop().await;
    assert_eq!(kept_pieces(&rig.db, job_id), 2);

    // Its file not there at all, as on a drive not mounted yet: the pieces
    // are kept, said once, and saved as they were; back again, they are had.
    let aside = rig.dir.path().join("aside.bin");
    std::fs::rename(dir.join("a.bin"), &aside).unwrap();
    let absent = "none of a torrent's files is there; its pieces are kept";
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    let heard = lines.of(absent);
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(
        heard[0].contains(&format!(" job_id={} pieces=2", job_id.0)),
        "{heard:?}"
    );
    assert_eq!(again.torrent(job_id).await.unwrap().pieces.unwrap().have, 2);
    again.stop().await;
    assert_eq!(kept_pieces(&rig.db, job_id), 2);
    assert!(!dir.join("a.bin").exists());
    std::fs::rename(&aside, dir.join("a.bin")).unwrap();
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(again.torrent(job_id).await.unwrap().pieces.unwrap().have, 2);
    assert_eq!(lines.of(absent).len(), 1);
    assert_eq!(lines.of(lost).len(), 1);
    again.stop().await;
    assert_eq!(kept_pieces(&rig.db, job_id), 2);
}

// A tee's checkpoint that the database refuses for good does not hold the
// torrent's writes up for ever: as a job of the pipeline pauses for one,
// the torrent's unpacking ends refused for it, said on its unpacking line
// and nowhere of the torrent's, the checkpoint is let go, and the writes
// behind it, the torrent's own and a flush of every write, go through.
#[tokio::test]
async fn a_checkpoint_the_database_refuses_ends_unpacking_and_holds_no_write_up() {
    use crate::persistence::sql_runtime::SqlRuntime;

    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
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
    save_with_tee(&rig.service, job_id).await;
    rig.db.flush_write_queue().await.unwrap();
    let datastore = rig.db.datastore();
    rig.db
        .run_sql_blocking(async move {
            for event in ["INSERT", "UPDATE"] {
                SqlRuntime::execute(
                    datastore.write_exec(),
                    &format!(
                        "CREATE TRIGGER refuse_graph_nodes_{event} BEFORE {event} ON graph_nodes \
                         BEGIN SELECT RAISE(ABORT, 'refused for good'); END"
                    ),
                    &[],
                )
                .await?;
            }
            Ok(())
        })
        .unwrap();
    let (layout, intake) = {
        let live = rig.service.inner.live.lock().await;
        let torrent = &live[&job_id];
        (
            torrent.layout.clone().unwrap(),
            torrent.intake.clone().unwrap(),
        )
    };
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        intake as Arc<dyn Pieces>,
        rig.service.resident(job_id),
    );
    let lengths = Lengths::new(
        layout.files().map(|file| file.length).sum(),
        layout.piece_length() as u32,
    )
    .unwrap();
    let range = layout.piece_range(0);
    let index = lengths.try_validate_piece_index(0).unwrap();
    assert!(storage.reserve_piece(index, &Arc::default()));
    for span in layout.spans(0) {
        let at = (range.start + span.at) as usize;
        storage
            .pwrite_all(
                span.file,
                span.range.start,
                &archive[at..at + span.range.len() as usize],
            )
            .unwrap();
    }
    // Every settle is counted, the one that ends the unpacking among them:
    // the torrent's queue row does not change for it.
    let mut settles = rig.service.inner.settled.subscribe();
    storage.on_piece_completed(index).unwrap();
    save_with_tee(&rig.service, job_id).await;

    settles
        .wait_for(|_| {
            rig.db
                .get_torrent(job_id)
                .unwrap()
                .unwrap()
                .unpack_end
                .is_some()
        })
        .await
        .unwrap();
    let end = rig
        .db
        .get_torrent(job_id)
        .unwrap()
        .unwrap()
        .unpack_end
        .unwrap();
    assert_eq!(end.result, UnpackResult::Refused);
    assert!(
        end.why
            .as_deref()
            .unwrap()
            .contains("Checkpoint could not be persisted"),
        "{end:?}"
    );
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_ne!(view.state, TorrentState::Stopped);
    assert_eq!(view.detail, None);
    save_with_tee(&rig.service, job_id).await;
    rig.db.flush_write_queue().await.unwrap();
    assert!(rig.service.torrent(job_id).await.is_some());
    rig.service.stop().await;
}

// An unpacking a crash cut short after it recorded its files, run again,
// finds them already beside the torrent's and places nothing new: what the
// first one kept stays kept, so a deletion still takes it.
#[tokio::test]
async fn an_unpacking_run_again_keeps_the_files_the_first_one_placed() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set.rar",
            &[("set.rar", b"rar")],
            128,
            false,
        ))))
        .await
        .unwrap();
    rig.db
        .finish_torrent_unpack(
            job_id,
            &["small.txt".to_owned()],
            &[],
            &[],
            &UnpackEnd::whole(),
        )
        .unwrap();
    rig.db
        .finish_torrent_unpack(
            job_id,
            &[],
            &[],
            &[],
            &UnpackEnd::short(false, "none".into()),
        )
        .unwrap();
    rig.db
        .finish_torrent_unpack(
            job_id,
            &["small.txt".to_owned(), "notes.txt".to_owned()],
            &[],
            &[],
            &UnpackEnd::short(true, "damaged: x".into()),
        )
        .unwrap();
    assert_eq!(
        rig.db.torrent_outputs(job_id).unwrap(),
        ["small.txt", "notes.txt"]
    );
    // How it came out is the last placing's, as the row reads back.
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().unpack_end,
        Some(UnpackEnd::short(true, "damaged: x".into()))
    );
    rig.service.stop().await;
}

// A rule for torrents announcing under `host`, holding `passwords`.
fn password_rule(host: &str, passwords: &[&str]) -> TrackerRule {
    TrackerRule {
        transports: Default::default(),
        archive_passwords: crate::settings::model::ArchivePasswords::new(
            passwords.iter().map(|value| (*value).to_owned()),
        )
        .unwrap(),
        bound: None,
        id: 0,
        host_pattern: host.into(),
        seeding: None,
        never_pause: false,
        unpack: None,
        client: None,
        torrent_route: false,
        dht: None,
        pex: None,
        lsd: None,
        encryption: None,
        upload_cap: Default::default(),
        forced: false,
        ssl: None,
    }
}

// The candidates the graph of torrent `job_id`'s tee holds.
async fn graph_passwords(service: &TorrentService, job_id: JobId) -> Vec<String> {
    let live = service.inner.live.lock().await;
    let tee = live[&job_id].store.as_ref().unwrap().tee().unwrap();
    tee.with(|job| job.password_candidates().to_vec())
}

// A torrent under no rule tries exactly the global list; one under a rule
// tries that rule's passwords first, then the global list, a value in both
// tried once where the rule puts it; one under another rule never sees
// them. A graph already built keeps its list; one restored after a restart
// takes the current one, and an unbound rule's passwords still go to its
// torrents alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrents_archive_passwords_are_its_rules_then_the_global_list() {
    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    rig.db
        .save_archive_password_settings(Some(values(&["shared", "global"])), None)
        .unwrap();
    let a = rig
        .db
        .insert_tracker_rule(&password_rule("*.a.test", &["a-first", "shared"]), EPOCH_MS)
        .unwrap();
    rig.db
        .insert_tracker_rule(&password_rule("*.b.test", &["b-only"]), EPOCH_MS)
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    let archive: &'static [u8] = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let add = |name: &'static str, url: Option<&'static str>| {
        let service = rig.service.clone();
        async move {
            let bytes = metainfo(name, &[("set.rar", archive)], 128, false);
            let bytes = match url {
                Some(url) => announcing(&bytes, url),
                None => bytes,
            };
            service
                .add_opened(request(AddSource::Metainfo(bytes)))
                .await
                .unwrap()
        }
    };
    let under_a = add("under-a", Some("http://tracker.a.test/announce")).await;
    let under_b = add("under-b", Some("http://tracker.b.test/announce")).await;
    let under_none = add("under-none", None).await;

    let global: Vec<String> = rig
        .db
        .global_archive_password_candidates()
        .unwrap()
        .iter()
        .map(|candidate| candidate.value().to_owned())
        .collect();
    assert_eq!(graph_passwords(&rig.service, under_none).await, global);
    assert_eq!(
        graph_passwords(&rig.service, under_a).await,
        values(&["a-first", "shared", "global"])
    );
    assert_eq!(
        graph_passwords(&rig.service, under_b).await,
        values(&["b-only", "shared", "global"])
    );

    rig.db
        .save_archive_password_settings(Some(values(&["later"])), None)
        .unwrap();
    assert_eq!(
        graph_passwords(&rig.service, under_a).await,
        values(&["a-first", "shared", "global"])
    );
    rig.service
        .save_checkpoints([under_a, under_none].into())
        .await;
    assert!(rig.db.unbind_tracker_rule(a.id, EPOCH_MS).unwrap());
    rig.service.stop().await;

    let restarted = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&restarted).await;
    assert_eq!(
        graph_passwords(&restarted, under_a).await,
        values(&["a-first", "shared", "later"])
    );
    assert_eq!(
        graph_passwords(&restarted, under_b).await,
        values(&["b-only", "later"])
    );
    assert_eq!(
        graph_passwords(&restarted, under_none).await,
        values(&["later"])
    );
    restarted.stop().await;
}

// A tracker rule's passwords never reach an NZB job's harvest: the harvest
// takes no tracker candidate a job could carry, and no stored label reads
// back as one.
#[test]
fn an_nzb_harvest_takes_no_tracker_password() {
    use crate::jobs::model::{ArchivePasswordCandidate, ArchivePasswordSource};
    let carried = [
        ArchivePasswordCandidate::new(ArchivePasswordSource::Tracker, "tracker".to_owned()),
        ArchivePasswordCandidate::new(ArchivePasswordSource::NzbMeta, "nzb".to_owned()),
    ];
    let global = [ArchivePasswordCandidate::new(
        ArchivePasswordSource::Settings,
        "global".to_owned(),
    )];
    let harvest =
        crate::pipeline::assemble_archive_password_candidates(Some("spec"), &carried, &global);
    assert_eq!(
        harvest
            .iter()
            .map(|candidate| (candidate.source(), candidate.value()))
            .collect::<Vec<_>>(),
        [
            (ArchivePasswordSource::Explicit, "spec"),
            (ArchivePasswordSource::NzbMeta, "nzb"),
            (ArchivePasswordSource::Settings, "global"),
        ]
    );
    assert_eq!(
        ArchivePasswordSource::from_label(ArchivePasswordSource::Tracker.as_str()),
        None
    );
}

// The list a torrent takes: its rule's passwords, then the global list,
// each value once where it first appears; under no rule, the global list
// alone.
#[test]
fn a_torrents_password_list_is_its_rules_then_the_global_list_once_each() {
    use crate::jobs::model::{ArchivePasswordCandidate, ArchivePasswordSource};
    let global = [
        ArchivePasswordCandidate::new(ArchivePasswordSource::Settings, "shared".to_owned()),
        ArchivePasswordCandidate::new(ArchivePasswordSource::PasswordFile, "file".to_owned()),
    ];
    let listed = |rule: Option<&TrackerRule>| {
        torrent_archive_passwords(rule, &global)
            .iter()
            .map(|candidate| (candidate.source(), candidate.value().to_owned()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        listed(None),
        [
            (ArchivePasswordSource::Settings, "shared".to_owned()),
            (ArchivePasswordSource::PasswordFile, "file".to_owned()),
        ]
    );
    let rule = password_rule("*.a.test", &["first", "shared"]);
    assert_eq!(
        listed(Some(&rule)),
        [
            (ArchivePasswordSource::Tracker, "first".to_owned()),
            (ArchivePasswordSource::Tracker, "shared".to_owned()),
            (ArchivePasswordSource::PasswordFile, "file".to_owned()),
        ]
    );
}

// The stored torrents load behind the start: with the load held before its
// first page, the start has ended and the service answers. A list of every
// torrent, or of more than a page, is told they are not all here yet, never
// given some of them as all; one named by its key, or on a page of the
// list, is read then and there; an
// add of a torrent stored but not yet in is refused from its row; a remove
// of one not yet in removes it, and the load does not bring it back; a new
// torrent is kept at once. Once the load goes on, every stored torrent is
// in, once, and the one added under it is last in the queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_service_answers_while_its_stored_torrents_load() {
    use crate::torrents::service::listing::{PAGE_WHILE_LOADING, Pick, Shown, Window, page};
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let mut stored = Vec::new();
    for (name, seed) in [("one", 1), ("two", 2), ("three", 3)] {
        stored.push(
            rig.service
                .add_opened(request(AddSource::Metainfo(plain(name, seed))))
                .await
                .unwrap(),
        );
    }
    let [one, two, three] = stored[..] else {
        unreachable!()
    };
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.hold_load(true);
    again.start_seated().await.unwrap();
    assert!(again.loading());
    assert_eq!(again.whole(), Err(NotReady::Loading));
    for window in [Window::Every, page(0, PAGE_WHILE_LOADING + 1)] {
        assert!(matches!(
            again.picked(Pick::default(), window, Shown::LISTED).await,
            Err(ListError::NotReady(NotReady::Loading))
        ));
    }
    assert_eq!(again.torrents(None).await.len(), 0);

    assert_eq!(again.torrent(one).await.unwrap().name, "one");
    let listed: Vec<JobId> = again
        .torrents(None)
        .await
        .iter()
        .map(|view| view.job_id)
        .collect();
    assert_eq!(listed, [one]);
    // A page of the list brings in the torrents it shows.
    let paged: Vec<JobId> = again
        .picked(Pick::default(), page(0, 2), Shown::LISTED)
        .await
        .unwrap()
        .iter()
        .map(|view| view.job_id)
        .collect();
    assert_eq!(paged, [one, two]);
    assert_eq!(
        again
            .add(request(AddSource::Metainfo(plain("two", 2))))
            .await,
        Err(AddError::Duplicate(two))
    );
    again.remove(three, false, true).await.unwrap();
    assert_eq!(rig.db.get_torrent(three).unwrap(), None);
    let four = again
        .add(request(AddSource::Metainfo(plain("four", 4))))
        .await
        .unwrap();
    assert!(rig.db.get_torrent(four).unwrap().is_some());

    again.hold_load(false);
    again.loaded().await;
    assert_eq!(again.whole(), Ok(()));
    let listed: Vec<JobId> = again
        .picked(Pick::default(), Window::Every, Shown::LISTED)
        .await
        .unwrap()
        .iter()
        .map(|view| view.job_id)
        .collect();
    assert_eq!(listed, [one, two, four]);
    let place = |job_id| rig.db.get_torrent(job_id).unwrap().unwrap().queue_position;
    assert!(place(one) < place(two) && place(two) < place(four));
    again.stop().await;
}

// The stored torrents in the order a restart would bring their queue back.
fn kept_order(db: &Database, jobs: &[JobId]) -> Vec<JobId> {
    let mut kept: Vec<(i64, JobId)> = jobs
        .iter()
        .map(|job_id| {
            (
                db.get_torrent(*job_id).unwrap().unwrap().queue_position,
                *job_id,
            )
        })
        .collect();
    kept.sort_unstable();
    kept.into_iter().map(|(_, job_id)| job_id).collect()
}

// A torrent an asker read while the load ran, taken out by the operator
// before that asker put it among the live ones, and the load ended in
// between: the asker does not bring it back. Its tracker, which the torrent
// kept beside it shares, hears that one start and stop, and nothing of the
// one removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_removed_under_a_read_of_it_is_not_brought_back() {
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    let (url, mut answered) = answering_tracker("127.0.0.1:1".parse().unwrap()).await;
    rig.service.open().await.unwrap();
    let mut jobs = Vec::new();
    for (name, seed) in [("gone", 1), ("kept", 2)] {
        let job_id = rig
            .service
            .add_opened(request(AddSource::Metainfo(plain(name, seed))))
            .await
            .unwrap();
        tracked(&rig.service, job_id, &url).await;
        jobs.push(job_id);
    }
    let [gone, kept] = jobs[..] else {
        unreachable!()
    };
    answered
        .wait_for(|events| {
            events
                .iter()
                .filter(|(event, _)| event == "started")
                .count()
                == 2
        })
        .await
        .unwrap();
    rig.service.stop().await;
    let before = answered.borrow().len();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.hold_load(true);
    again.start_seated().await.unwrap();
    // The asker: its read begun, and the torrent's rows read.
    let (reader, _) = again.reader().unwrap();
    let page = again.read_page(vec![gone]).await.unwrap();
    again.remove(gone, false, true).await.unwrap();
    assert_eq!(rig.db.get_torrent(gone).unwrap(), None);
    again.hold_load(false);
    again.loaded().await;
    // The asker goes on with what it read, the load over.
    again.take_page(page).await;
    drop(reader);
    {
        let live = again.inner.live.lock().await;
        assert_eq!(live.keys().copied().collect::<Vec<_>>(), [kept]);
    }
    assert!(locked(&again.inner.load_marks).gone.is_empty());

    again.tick().await;
    again.restored().await;
    answered
        .wait_for(|events| events.len() > before)
        .await
        .unwrap();
    again.pause(kept).await.unwrap();
    let heard = answered
        .wait_for(|events| events.len() > before + 1)
        .await
        .unwrap()[before..]
        .iter()
        .map(|(event, _)| event.clone())
        .collect::<Vec<_>>();
    assert_eq!(heard, ["started", "stopped"]);
    let taken: usize = again
        .inner
        .sessions
        .running()
        .await
        .values()
        .map(|session| session.with_torrents(|torrents| torrents.count()))
        .sum();
    assert_eq!(taken, 1);
    again.stop().await;
}

// A page the load fails to read leaves the load where it stands, with why
// shown among the service's warnings: the torrents read so far take their
// turns, an answer over every torrent is refused with the reason, and a
// read that fails again changes no row. The tick reads the page again, and
// when the last is in, a torrent added meanwhile is last in the queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_that_fails_is_read_again_from_where_the_load_stood() {
    use std::sync::atomic::Ordering::Relaxed;
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let mut stored = Vec::new();
    for (name, seed) in [("one", 1), ("two", 2), ("three", 3)] {
        stored.push(
            rig.service
                .add_opened(request(AddSource::Metainfo(plain(name, seed))))
                .await
                .unwrap(),
        );
    }
    let [one, two, three] = stored[..] else {
        unreachable!()
    };
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.inner.load_seam.page_rows.store(1, Relaxed);
    // The first page reads; the second does not.
    again.inner.load_seam.fail_in.store(2, Relaxed);
    let why = again.open().await.unwrap_err();
    assert!(again.loading());
    assert_eq!(again.whole(), Err(NotReady::Faulted(why.clone())));
    assert_eq!(
        again.warnings().await,
        [NotReady::Faulted(why.clone()).to_string()]
    );
    let here = || async {
        again
            .inner
            .live
            .lock()
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>()
    };
    assert_eq!(here().await, [one]);
    // The one read so far takes its turn.
    again.apply_queue().await;
    assert!(!again.inner.live.lock().await[&one].queued);
    let four = again
        .add(request(AddSource::Metainfo(plain("four", 4))))
        .await
        .unwrap();

    // A read that fails again: no row changes, and the reason stands.
    let rows = || [one, two, three, four].map(|job_id| rig.db.get_torrent(job_id).unwrap());
    let before = rows();
    again.inner.load_seam.fail_in.store(1, Relaxed);
    again.resumed_load().unwrap().await;
    assert_eq!(rows(), before);
    assert_eq!(again.whole(), Err(NotReady::Faulted(why)));
    assert_eq!(here().await, [one, four]);

    // The tick reads on from the page that failed.
    again.tick().await;
    again.loaded().await;
    assert_eq!(again.whole(), Ok(()));
    assert!(again.warnings().await.is_empty());
    assert_eq!(here().await, [one, two, three, four]);
    assert_eq!(
        kept_order(&rig.db, &[four, three, two, one]),
        [one, two, three, four]
    );
    again.stop().await;
}

// What is asked of the queue's order while the stored torrents load is
// done among all of them when the last is in, in the order asked: a move
// to the top, an add to the top, and a move to the bottom all hold. Until
// then the order kept is the stored one, and no torrent of the part read
// so far takes a download turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_move_asked_for_under_the_load_holds_after_it() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let mut stored = Vec::new();
    for (name, seed) in [("a", 1), ("b", 2), ("c", 3)] {
        stored.push(
            rig.service
                .add_opened(request(AddSource::Metainfo(plain(name, seed))))
                .await
                .unwrap(),
        );
    }
    let [a, b, c] = stored[..] else {
        unreachable!()
    };
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.hold_load(true);
    again.start_seated().await.unwrap();
    again.reorder(&[c], QueueMoveTarget::Top).await.unwrap();
    let d = again
        .add(request(AddSource::Metainfo(plain("d", 4))))
        .await
        .unwrap();
    again.reorder(&[d], QueueMoveTarget::Top).await.unwrap();
    let moved = again.move_many(&[(a, QueueMoveTarget::Bottom)]).await;
    assert!(moved.iter().all(|(_, moved)| moved.is_ok()));
    assert!(matches!(
        again.reorder(&[JobId(9_999)], QueueMoveTarget::Top).await,
        Err(ActionError::NotFound(JobId(9_999)))
    ));
    // The stored order stands, and none of the part here has a turn.
    assert_eq!(kept_order(&rig.db, &[a, b, c]), [a, b, c]);
    again.apply_queue().await;
    {
        let live = again.inner.live.lock().await;
        assert_eq!(live.keys().copied().collect::<Vec<_>>(), [a, c, d]);
        assert!(live.values().all(|torrent| torrent.queued));
    }

    again.hold_load(false);
    again.loaded().await;
    assert_eq!(kept_order(&rig.db, &[a, b, c, d]), [d, c, b, a]);
    {
        let live = again.inner.live.lock().await;
        let mut order: Vec<(i64, JobId)> = live
            .values()
            .map(|torrent| (torrent.record.queue_position, torrent.record.job_id))
            .collect();
        order.sort_unstable();
        assert_eq!(
            order
                .into_iter()
                .map(|(_, job_id)| job_id)
                .collect::<Vec<_>>(),
            [d, c, b, a]
        );
        assert!(live.values().any(|torrent| !torrent.queued));
    }
    again.stop().await;
}

// A torrent the load is reading is not read again by one that asks for it
// meanwhile: the asker waits for that read alone, and reads no row of the
// torrent itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_being_read_is_waited_for_not_read_twice() {
    let reads = |db: &Database| db.torrent_reads.load(std::sync::atomic::Ordering::Relaxed);
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let one = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("one", 1))))
        .await
        .unwrap();
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.hold_load(true);
    again.start_seated().await.unwrap();
    // The load's page, as far as its read: the torrent is its to read.
    let mut wanted = vec![(one, 0)];
    let (claim, theirs) = again.claim(&mut wanted);
    assert!(theirs.is_empty());
    let page = again.read_page(vec![one]).await.unwrap();
    let base = reads(&rig.db);
    // The asker waits on that read before the page joins the live
    // torrents: the load's own hold on the read's end is one, the asker's
    // the second.
    let asker = {
        let again = again.clone();
        tokio::spawn(async move { again.torrent(one).await })
    };
    while claim.ended.receiver_count() < 2 {
        tokio::task::yield_now().await;
    }
    again.take_page(page).await;
    drop(claim);
    assert_eq!(asker.await.unwrap().unwrap().name, "one");
    // The sizes it asked for, to know what to read; never the torrent.
    assert_eq!(reads(&rig.db), base + 1);
    again.hold_load(false);
    again.loaded().await;
    again.stop().await;
}

// A page holds no more stored metainfo than its bound, a torrent larger
// than the bound a page alone, and never more torrents than its rows.
#[test]
fn a_page_of_the_load_is_bounded_by_bytes_as_by_rows() {
    use super::super::restore::{LOAD_PAGE_BYTES, pages};
    let mib = 1024 * 1024;
    let sized = |sizes: &[u64]| -> Vec<(JobId, u64)> {
        sizes
            .iter()
            .enumerate()
            .map(|(at, size)| (JobId(at as u64 + 1), *size))
            .collect()
    };
    let lens = |sizes: &[u64], rows| -> Vec<usize> {
        pages(&sized(sizes), rows).iter().map(Vec::len).collect()
    };
    assert_eq!(lens(&[1; 5], 2), [2, 2, 1]);
    assert_eq!(lens(&[16 * mib, 16 * mib, 1], 64), [2, 1]);
    assert_eq!(lens(&[1, 100 * mib, 1, 1], 64), [1, 1, 2]);
    assert_eq!(lens(&[LOAD_PAGE_BYTES, LOAD_PAGE_BYTES], 64), [1, 1]);
    assert!(lens(&[], 64).is_empty());
    let all: Vec<JobId> = pages(&sized(&[7; 9]), 4).concat();
    assert_eq!(all, (1..=9).map(JobId).collect::<Vec<_>>());
}

// A tick that finds no torrent waiting for its job begins no restore pass:
// the pass's state never changes. A restart leaves one waiting, the next
// tick begins the pass, and the tick after it begins none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_tick_begins_no_restore_pass() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let mut passes = rig.service.inner.restores.subscribe();
    passes.borrow_and_update();
    rig.service.tick().await;
    rig.service.tick().await;
    assert!(!passes.has_changed().unwrap());
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    let mut passes = again.inner.restores.subscribe();
    passes.borrow_and_update();
    again.tick().await;
    again.restored().await;
    assert!(passes.has_changed().unwrap());
    assert!(again.inner.live.lock().await[&job_id].store.is_some());
    passes.borrow_and_update();
    again.tick().await;
    again.tick().await;
    assert!(!passes.has_changed().unwrap());
    again.stop().await;
}

// A recheck and a move of a torrent that still waits for its job restore
// the job first, as a remove does, and then act.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recheck_and_a_move_restore_the_job_they_need() {
    for moved in [false, true] {
        let rig = rig(set_clock());
        on_loopback(&rig.routes);
        rig.service.open().await.unwrap();
        let job_id = rig
            .service
            .add_opened(request(AddSource::Metainfo(plain("set", 1))))
            .await
            .unwrap();
        rig.service.stop().await;

        let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
        let written = again.written();
        again.open().await.unwrap();
        assert!(again.inner.live.lock().await[&job_id].store.is_none());
        if moved {
            let elsewhere = rig.dir.path().join("elsewhere");
            again
                .set_location(job_id, elsewhere.to_str().unwrap().to_owned())
                .await
                .unwrap();
            let stored = rig.db.get_torrent(job_id).unwrap().unwrap().storage_dir;
            assert_eq!(
                stored.as_deref().map(Path::new),
                Some(&*elsewhere.join("set"))
            );
        } else {
            again.recheck(job_id).await.unwrap();
        }
        assert_eq!(written.borrow().checkpoints_read, 1);
        assert!(again.inner.live.lock().await[&job_id].store.is_some());
        again.stop().await;
    }
}

// The restore pass and the tick's look at the routes may both come to put
// one torrent in a session, each having read it before the other put it
// there, and on two routes when its route changed between them. One does:
// the torrent is in one session, and its tracker hears it start once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restored_torrent_joins_one_session_though_its_route_changed() {
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    let (url, mut answered) = answering_tracker("127.0.0.1:1".parse().unwrap()).await;
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    answered
        .wait_for(|events| events.iter().any(|(event, _)| event == "started"))
        .await
        .unwrap();
    rig.service.stop().await;
    let before = answered.borrow().len();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    let gate = Arc::clone(&again.inner.live.lock().await[&job_id].gate);
    let held = gate.lock().await;
    let mut asked = rig.routes.asked.subscribe();
    let opens = *asked.borrow_and_update();
    // The pass: the job back, then its route's session opened for it.
    let pass = {
        let again = again.clone();
        tokio::spawn(async move {
            if again.restore_job(job_id).await {
                again.attach(job_id, None).await;
            }
        })
    };
    asked.wait_for(|asked| *asked > opens).await.unwrap();
    // The tick: its route is another now, whose session opens for it too.
    rig.routes.route(TorrentScope::Default, 2);
    let tick = {
        let again = again.clone();
        tokio::spawn(async move { again.follow_routes().await })
    };
    asked.wait_for(|asked| *asked > opens + 1).await.unwrap();
    drop(held);
    pass.await.unwrap();
    tick.await.unwrap();

    let running = again.inner.sessions.running().await;
    let taken: usize = running
        .values()
        .map(|session| session.with_torrents(|torrents| torrents.count()))
        .sum();
    assert_eq!(taken, 1);
    answered
        .wait_for(|events| events.len() > before)
        .await
        .unwrap();
    again.pause(job_id).await.unwrap();
    let heard = answered
        .wait_for(|events| events.len() > before + 1)
        .await
        .unwrap()[before..]
        .iter()
        .map(|(event, _)| event.clone())
        .collect::<Vec<_>>();
    assert_eq!(heard, ["started", "stopped"]);
    // Each session numbers the torrents it takes from 0: the one that took
    // the torrent gives the next 1, and the other, which never took it, 0.
    assert_eq!(running.len(), 2);
    let mut next = Vec::new();
    for (at, session) in running.values().enumerate() {
        let options = AddTorrentOptions {
            paused: true,
            output_folder: Some(
                rig.dir
                    .path()
                    .join(format!("probe-{at}"))
                    .to_string_lossy()
                    .into_owned(),
            ),
            ..Default::default()
        };
        let probe = session
            .add_torrent(
                weaver_torrent::AddTorrent::from_bytes(plain("probe", 9)),
                Some(options),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        next.push(probe.id());
    }
    next.sort_unstable();
    assert_eq!(next, [0, 1], "the torrent was put in both sessions");
    again.stop().await;
}

// A recheck of a torrent whose job is not back yet answers without waiting
// for the torrent to join its route's session: the join is held here, and
// the recheck returns all the same, the job restored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recheck_of_a_torrent_not_restored_does_not_wait_on_its_route() {
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    let gate = Arc::clone(&again.inner.live.lock().await[&job_id].gate);
    let held = gate.lock().await;
    again.recheck(job_id).await.unwrap();
    {
        let live = again.inner.live.lock().await;
        assert!(live[&job_id].store.is_some());
        assert!(live[&job_id].attached.is_none());
    }
    drop(held);
    again.stop().await;
}

// The torrents kept before whether each unpacks was, a page of them: the
// load fixes it for each and keeps them all in one write of the page.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_keeps_whether_its_older_torrents_unpack() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let mut stored = Vec::new();
    for (name, seed) in [("one", 1), ("two", 2), ("three", 3)] {
        stored.push(
            rig.service
                .add_opened(request(AddSource::Metainfo(plain(name, seed))))
                .await
                .unwrap(),
        );
    }
    rig.service.stop().await;
    {
        let datastore = rig.db.datastore();
        rig.db
            .run_sql_blocking(async move {
                SqlRuntime::execute(
                    datastore.write_exec(),
                    "UPDATE torrents SET unpack_in_force = NULL",
                    &[],
                )
                .await?;
                Ok(())
            })
            .unwrap();
    }
    let kept = |job_id| rig.db.get_torrent(job_id).unwrap().unwrap().unpack_in_force;
    assert!(stored.iter().all(|job_id| kept(*job_id).is_none()));

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    for job_id in &stored {
        assert_eq!(kept(*job_id), Some(true));
        let live = again.inner.live.lock().await;
        assert_eq!(live[job_id].record.unpack_in_force, Some(true));
    }
    again.stop().await;
}

// A seed of two files, `set/a.bin` and `set/b.bin`, of `PIECES` pieces each,
// found where they were placed and stopped: its checkpoint holds every
// piece, and each file's time as it took them.
const PIECES: usize = 8;

struct StampedSeed {
    rig: Rig,
    job_id: JobId,
    files: [std::path::PathBuf; 2],
}

async fn stamped_seed() -> StampedSeed {
    let piece = 16_384;
    let a: Vec<u8> = (0..(PIECES * piece) as u32)
        .map(|at| (at % 251) as u8)
        .collect();
    let b: Vec<u8> = (0..(PIECES * piece) as u32)
        .map(|at| (at % 241) as u8 ^ 0x5a)
        .collect();
    let metainfo = metainfo("set", &[("a.bin", &a), ("b.bin", &b)], piece, true);
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let save = rig.dir.path().join("stamped");
    std::fs::create_dir_all(save.join("set")).unwrap();
    let files = [save.join("set/a.bin"), save.join("set/b.bin")];
    std::fs::write(&files[0], &a).unwrap();
    std::fs::write(&files[1], &b).unwrap();
    let (job_id, _) = seeding_placed(
        &rig,
        metainfo,
        Placement {
            save_path: Some(save.to_string_lossy().into_owned()),
            ..Placement::default()
        },
    )
    .await;
    rig.service.stop().await;
    let stamps = rig.db.torrent_file_stamps(job_id).unwrap();
    assert_eq!(stamps.len(), 2, "each file's time went with the pieces");
    StampedSeed { rig, job_id, files }
}

// The file's bytes of its `piece`th piece turned over, its size kept.
fn corrupt(path: &std::path::Path, piece: usize) {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start((piece * 16_384) as u64)).unwrap();
    file.write_all(&[0xa5; 64]).unwrap();
}

fn modified(path: &std::path::Path) -> std::time::SystemTime {
    std::fs::metadata(path).unwrap().modified().unwrap()
}

fn set_modified(path: &std::path::Path, at: std::time::SystemTime) {
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(at)
        .unwrap();
}

// The seed restarted and through its check: its store's pieces, and the
// reads its files took from the restart on.
async fn restarted(seed: &StampedSeed) -> (TorrentService, u32, u64) {
    let reads = crate::torrents::files::STATS.snapshot().reads.calls;
    let again = service_on(
        &seed.rig.db,
        seed.rig.dir.path(),
        &seed.rig.routes,
        job_env(),
        set_clock(),
    );
    again.open().await.unwrap();
    again.tick().await;
    again.restored().await;
    handle_of(&again, seed.job_id)
        .await
        .wait_until_initialized()
        .await
        .unwrap();
    let have = again.inner.live.lock().await[&seed.job_id]
        .store
        .as_ref()
        .unwrap()
        .count();
    let read = crate::torrents::files::STATS.snapshot().reads.calls - reads;
    (again, have, read)
}

// A file changed between runs, its size the same and its time new, is
// checked again, and the pieces it no longer holds are dropped. The other
// file, its bytes changed too but its time put back, is not checked: only
// the changed file's pieces are read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_changed_between_runs_is_checked_again_and_its_bad_pieces_dropped() {
    let seed = stamped_seed().await;
    let [a, b] = &seed.files;
    let a_was = modified(a);
    corrupt(a, 1);
    set_modified(a, a_was);
    let b_was = modified(b);
    corrupt(b, 2);
    corrupt(b, 5);
    set_modified(b, b_was + std::time::Duration::from_secs(10));

    let (again, have, read) = restarted(&seed).await;
    // b's two changed pieces gone; a's, its time as it was, kept.
    assert_eq!(have, 2 * PIECES as u32 - 2);
    assert!(read > 0, "the changed file's pieces were read");
    let store = again.inner.live.lock().await[&seed.job_id]
        .store
        .clone()
        .unwrap();
    assert!(store.has(1), "a file whose time held was not read");
    assert!(!store.has((PIECES + 2) as u32) && !store.has((PIECES + 5) as u32));
    assert!(store.has(PIECES as u32));
    again.stop().await;
    // The check took the new time: the next restart checks nothing.
    let stamps = seed.rig.db.torrent_file_stamps(seed.job_id).unwrap();
    assert_eq!(
        stamps.get(&1).copied(),
        crate::torrents::files::stamp(Some(modified(b)))
    );
}

// A torrent whose files are as it left them checks nothing and reads no byte
// of them as it starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_untouched_torrent_checks_nothing_and_reads_nothing_at_restart() {
    let seed = stamped_seed().await;
    let (again, have, read) = restarted(&seed).await;
    assert_eq!((have, read), (2 * PIECES as u32, 0));
    again.stop().await;
}

// A file with no usable time is judged by its size alone: its bytes changed
// and its time at the epoch, nothing is checked, and nothing read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_with_no_usable_time_is_judged_by_its_size() {
    let seed = stamped_seed().await;
    let b = &seed.files[1];
    corrupt(b, 2);
    set_modified(b, std::time::UNIX_EPOCH);
    let (again, have, read) = restarted(&seed).await;
    assert_eq!((have, read), (2 * PIECES as u32, 0));
    again.stop().await;
}

// A torrent's checkpoint is written as it completes, however few pieces it
// had unsaved, so a crash while it seeds restores every piece it had: the
// restart fetches nothing, is not reopened, and its tracker hears no second
// `completed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_completing_restores_every_piece() {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, 16_384, true);
    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &private, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let (url, mut answered) = answering_tracker(peer).await;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(private.to_vec())))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    // Complete, and no settle under way after that was read: the one that
    // completed it is over.
    let mut settles = rig.service.inner.settled.subscribe();
    settles
        .wait_for(|_| {
            rig.db
                .get_torrent(job_id)
                .unwrap()
                .unwrap()
                .completed_at_ms
                .is_some()
                && !rig
                    .service
                    .inner
                    .settling
                    .load(std::sync::atomic::Ordering::SeqCst)
        })
        .await
        .unwrap();
    // Four pieces, fewer than the CHECKPOINT_PIECES a checkpoint waits for,
    // all kept by the settle that completed it.
    assert_eq!(kept_pieces(&rig.db, job_id), 4);
    answered
        .wait_for(|events| events.iter().any(|(event, _)| event == "completed"))
        .await
        .unwrap();
    // The process ends here: its sessions go with it, and nothing else is
    // written.
    rig.service.inner.sessions.stop_all().await;
    seeding.stop().await;
    let before = answered.borrow().len();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    {
        let live = again.inner.live.lock().await;
        let torrent = &live[&job_id];
        assert!(torrent.complete());
        assert_eq!(torrent.store.as_ref().unwrap().count(), 4);
    }
    answered
        .wait_for(|events| events.len() > before)
        .await
        .unwrap();
    again.pause(job_id).await.unwrap();
    let heard = answered
        .wait_for(|events| events.len() > before + 1)
        .await
        .unwrap()[before..]
        .iter()
        .map(|(event, _)| event.clone())
        .collect::<Vec<_>>();
    assert_eq!(heard, ["started", "stopped"]);
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(view.run_downloaded_bytes, 0);
    assert!(view.completed_at_ms.is_some());
    again.stop().await;
}
