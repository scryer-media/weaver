// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// While the session reports no bytes, as while it checks the files, the
// bytes in hand are the store's: the pieces the check has found so far, so
// they move with the pieces counted. Once it reports, they are the
// session's.
#[test]
fn a_checking_torrent_has_the_bytes_of_the_pieces_it_counts() {
    use crate::torrents::storage::Pieces;

    let layout = Arc::new(
        crate::torrents::layout::PieceLayout::new(
            64,
            vec![
                crate::torrents::layout::PieceFile {
                    name: "a.bin".to_owned(),
                    length: 100,
                    padding: false,
                },
                crate::torrents::layout::PieceFile {
                    name: "b.bin".to_owned(),
                    length: 92,
                    padding: false,
                },
            ],
        )
        .unwrap(),
    );
    let dir = tempfile::tempdir().unwrap();
    let files = crate::torrents::files::Files::new(&layout, dir.path().to_path_buf()).unwrap();
    let every = crate::torrents::store::Kept {
        bits: vec![0b1110_0000],
        checking: false,
    };
    let store = crate::torrents::store::TorrentStore::new(
        layout,
        files,
        Some(&every),
        None,
        crate::torrents::writes::Writes::for_test(),
    );
    let mut have = Vec::new();
    take_file_have(&mut have, &[], Some(&store));
    assert_eq!((store.count(), have.clone()), (3, vec![100, 92]));

    // A recheck: nothing is had until the check finds it.
    store.checking().unwrap();
    take_file_have(&mut have, &[], Some(&store));
    assert_eq!((store.count(), have.clone()), (0, vec![0, 0]));
    store.found(0).unwrap();
    take_file_have(&mut have, &[], Some(&store));
    assert_eq!((store.count(), have.clone()), (1, vec![64, 0]));

    take_file_have(&mut have, &[100, 92], Some(&store));
    assert_eq!(have, [100, 92]);
}

// The bytes in hand over the torrent's total leave padding out, as the total
// and the session's progress do.
#[test]
fn padding_in_hand_is_not_progress() {
    use crate::torrents::layout::PieceFile;

    let file = |name: &str, length, padding| PieceFile {
        name: name.to_owned(),
        length,
        padding,
    };
    let layout = PieceLayout::new(
        16,
        vec![
            file("a", 10, false),
            file(".pad/6", 6, true),
            file("b", 16, false),
        ],
    )
    .unwrap();
    assert_eq!(on_disk(Some(&layout), &[10, 6, 4]), 14);
    assert_eq!(on_disk(None, &[]), 0);
}

// A follower is given every torrent first, then only the torrents that
// changed or came and the ids of the ones gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follower_is_given_only_what_moved_after_the_first_push() {
    use fingerprint::ViewParts::Steady;

    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let mut views = Vec::new();
    for (name, seed) in [("one", 1), ("two", 2)] {
        let job_id = rig
            .service
            .add_opened(request(AddSource::Metainfo(plain(name, seed))))
            .await
            .unwrap();
        views.push(rig.service.torrent(job_id).await.unwrap());
    }
    let (one, two) = (views[0].clone(), views[1].clone());
    let mut last = None;

    let first = updates::changes(&mut last, views.clone(), Steady).unwrap();
    assert!(first.full_update);
    assert_eq!(first.torrents, views);
    assert!(first.removed.is_empty());
    assert_eq!(updates::changes(&mut last, views, Steady), None);

    let mut paused = two.clone();
    paused.state = TorrentState::Paused;
    let moved = updates::changes(&mut last, vec![one.clone(), paused.clone()], Steady).unwrap();
    assert!(!moved.full_update);
    assert_eq!(moved.torrents, [paused]);
    assert!(moved.removed.is_empty());

    let gone = updates::changes(&mut last, vec![one.clone()], Steady).unwrap();
    assert!(gone.torrents.is_empty());
    assert_eq!(gone.removed, [two.job_id]);

    let back = updates::changes(&mut last, vec![one, two.clone()], Steady).unwrap();
    assert_eq!(back.torrents, [two]);
    assert!(back.removed.is_empty());
}

// Equal views have equal fingerprints; one steady field moved, down to a
// file's length or a tracker's message, moves both; a clock field moved
// moves only the whole one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fingerprint_moves_with_the_fields_it_covers() {
    use fingerprint::ViewParts::{Steady, Whole};
    let seeded = seeded().await;
    let view = seeded.view().await;
    assert!(!view.files.is_empty() && !view.trackers.is_empty());
    let same = view.clone();
    for parts in [Steady, Whole] {
        assert_eq!(view.fingerprint(parts), same.fingerprint(parts));
    }
    let mut file = view.clone();
    file.files[0].length += 1;
    let mut tracker = view.clone();
    tracker.trackers[0].message = Some("moved".to_owned());
    let mut ratio = view.clone();
    ratio.ratio += 0.5;
    for moved in [file, tracker, ratio] {
        for parts in [Steady, Whole] {
            assert_ne!(view.fingerprint(parts), moved.fingerprint(parts));
        }
    }
    let mut aged = view.clone();
    aged.active_seconds += 1;
    assert_eq!(view.fingerprint(Steady), aged.fingerprint(Steady));
    assert_ne!(view.fingerprint(Whole), aged.fingerprint(Whole));
    seeded.rig.service.stop().await;
}

// The clock alone moves a seeding torrent's seconds: every torrent's
// follower, which leaves them out, has nothing to push; one torrent's
// follower, which shows them, pushes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_clock_alone_moves_only_one_torrents_follower() {
    use fingerprint::ViewParts::{Steady, Whole};

    let seeded = seeded().await;
    // Nothing but the clock moves it from here: its seeder is gone, its
    // rates have run down, its tracker has answered the `completed` the
    // run owes it, the last announce before one far past the test, and the
    // queue pass its completion asked for has taken its rank.
    seeded.seeder.stop().await;
    seeded.advance(60_000);
    let completed = seeded
        .answered
        .clone()
        .wait_for(|answers| answers.iter().any(|(event, _)| event == "completed"))
        .await
        .unwrap()
        .iter()
        .find(|(event, _)| event == "completed")
        .unwrap()
        .1;
    let mut follow = seeded.rig.service.updates(seeded.job_id);
    loop {
        let view = &follow.next().await.torrents[0];
        let answered = view.trackers.iter().all(|tracker| {
            !tracker.updating && tracker.last_announce_ms.is_some_and(|at| at >= completed)
        });
        let quiet = view.peers == 0 && view.download_bps == 0 && view.upload_bps == 0;
        if quiet && answered && view.queue_position.is_none() {
            break;
        }
    }
    let before = seeded.view().await;
    seeded.advance(7_000);
    let after = seeded.view().await;
    assert_eq!(after.state, TorrentState::Seeding);
    assert_eq!(after.active_seconds, before.active_seconds + 7);
    assert_eq!(after.seeding_seconds, before.seeding_seconds + 7);

    let (mut every, mut one) = (None, None);
    assert!(updates::changes(&mut every, vec![before.clone()], Steady).is_some());
    assert!(updates::changes(&mut one, vec![before], Whole).is_some());
    assert_eq!(
        updates::changes(&mut every, vec![after.clone()], Steady),
        None
    );
    let pushed = updates::changes(&mut one, vec![after.clone()], Whole).unwrap();
    assert!(!pushed.full_update);
    assert_eq!(pushed.torrents, [after]);
    seeded.rig.service.stop().await;
}

// A follower hears a tracker's answer as it lands, with nothing in the queue
// to say so: the rig has no queue, and the answer moves the torrent's row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_followed_torrent_pushes_its_trackers_answer() {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("pushed", 2))))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    let mut updates = rig.service.updates(job_id);

    let mut started = heard_announce(&listener).await;
    let first = updates.next().await.torrents;
    assert!(first[0].trackers[0].updating);

    let body = b"d8:completei3e10:incompletei5e8:intervali1800e5:peers0:e";
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    started
        .write_all(&[head.as_bytes(), body].concat())
        .await
        .unwrap();
    let working = loop {
        let views = updates.next().await.torrents;
        if views[0].trackers[0].status == TrackerStatus::Working {
            break views[0].trackers[0].clone();
        }
    };
    assert_eq!((working.seeders, working.leechers), (Some(3), Some(5)));
    rig.service.stop().await;
}

// The figures the exporter reads are the ones the tick read: a torrent
// whose ledger and policy are known renders them, its trackers one series
// per host, and each add and removal counts once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_metrics_carry_what_the_tick_read() {
    use crate::torrents::metrics::{FetchOutcome, SeedingGoal, TorrentEvent};

    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    // No session is built while the route is down, so nothing but the
    // test moves the ledger.
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            category: Some("tv".into()),
            policy_override: Some(SeedingPolicy {
                seed_time_counting: Default::default(),
                ratio_goal: Leaf::Set(3.0),
                seed_time_goal_minutes: Leaf::Set(10),
                inactive_goal_minutes: Leaf::Inherit,
                mode: Leaf::Set(SeedingGoalMode::All),
                action: Leaf::Set(SeedingAction::Stop),
            }),
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    let gone = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("gone", 2))))
        .await
        .unwrap();
    rig.service
        .add_trackers(
            job_id,
            vec![
                vec!["http://127.0.0.1:1/announce".to_owned()],
                vec!["http://127.0.0.1:2/announce".to_owned()],
            ],
        )
        .await
        .unwrap();
    rig.service.remove(gone, true).await.unwrap();
    {
        let mut live = rig.service.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).unwrap();
        torrent.counters.downloaded_bytes = 3000;
        torrent.counters.uploaded_bytes = 6000;
        torrent.counters.finished_duration_s = 120;
    }
    clock.shift_unix_ms(TICK.as_millis() as i64);
    rig.service.tick().await;
    rig.service.restored().await;

    let metrics = rig.service.metrics();
    let view = rig.service.torrent(job_id).await.unwrap();
    let published = &metrics.published;
    assert_eq!(published.torrents.len(), 1, "{published:?}");
    let job = published.job(job_id).unwrap();
    let torrent = job.torrent;
    assert_eq!(torrent.category, "tv");
    assert_eq!(Some(torrent.route.as_str()), view.route.as_deref());
    assert_eq!(torrent.total_bytes, 3000);
    assert_eq!(torrent.wanted_bytes, 3000);
    assert_eq!(torrent.downloaded_bytes, 3000);
    assert_eq!(torrent.uploaded_bytes, 6000);
    assert_eq!(torrent.seeding_seconds, 120);
    assert_eq!(torrent.ratio, 2.0);
    assert_eq!(torrent.ratio, view.ratio);
    assert_eq!(torrent.state, view.state);
    assert_eq!(torrent.fetch, None);
    assert_eq!(
        torrent.seeding.goals,
        [
            (SeedingGoal::Ratio, 3.0, 1.0),
            (SeedingGoal::SeedTime, 600.0, 480.0)
        ]
    );
    assert!(!torrent.seeding.met);
    // Both trackers are on one host: one series, the first listed.
    assert_eq!(torrent.trackers.len(), 1, "{:?}", torrent.trackers);
    assert_eq!(torrent.trackers[0].host, "127.0.0.1");
    assert_eq!(torrent.trackers[0].status, view.trackers[0].status);
    assert_eq!(torrent.trackers[0].last_announce_ms, None);
    // The route is down: no session runs on it.
    assert!(job.session.is_none_or(|session| !session.open));

    assert_eq!(
        metrics.events,
        [
            (TorrentEvent::Added, 2),
            (TorrentEvent::Completed, 0),
            (TorrentEvent::Removed, 1),
            (TorrentEvent::Failed, 0),
        ]
    );
    assert_eq!(
        metrics.fetches,
        FetchOutcome::ALL.map(|outcome| (outcome, 0)).to_vec()
    );
    rig.service.stop().await;
}

// The figures the service publishes for `job_id`, once `holds` says they
// hold of them.
async fn published_once(
    service: &TorrentService,
    job_id: JobId,
    holds: impl Fn(&TorrentSeries) -> bool,
) -> TorrentSeries {
    let mut published = service.inner.published.subscribe();
    let published = published
        .wait_for(|published| published.job(job_id).is_some_and(|job| holds(job.torrent)))
        .await
        .unwrap();
    published.job(job_id).unwrap().torrent.clone()
}

// A magnet's fetch is published as it runs, with no tick: its phase while
// its tracker holds the announce, and the phase the tracker's answer moves
// it to, one phase at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fetching_magnets_phase_is_published_as_it_moves() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = fetching_from(&rig.service, &"fd".repeat(20), port).await;

    let (started, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("started"));
    let fetching = published_once(&rig.service, job_id, |torrent| torrent.fetch.is_some()).await;
    assert_eq!(fetching.state, TorrentState::FetchingMetadata);
    assert_eq!(fetching.fetch, Some(weaver_torrent::FetchPhase::Trackers));

    answer_no_peers(started).await;
    let moved = published_once(&rig.service, job_id, |torrent| {
        torrent
            .fetch
            .is_some_and(|phase| phase != weaver_torrent::FetchPhase::Trackers)
    })
    .await;
    let phase = {
        let live = rig.service.inner.live.lock().await;
        let fetch = live[&job_id].fetching.as_ref().unwrap();
        *fetch.phase.borrow()
    };
    assert_eq!(moved.fetch, Some(phase));
    assert_eq!(moved.state, TorrentState::FetchingMetadata);
    drop(listener);
    rig.service.stop().await;
}

// A tracker's answer is published as it lands, with no tick: the exporter
// reads the tracker as the view does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trackers_answer_is_published_as_it_lands() {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://127.0.0.1:{}/announce",
        listener.local_addr().unwrap().port()
    );
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("answered", 3)))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();

    let (mut started, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("started"));
    let body = b"d8:completei3e10:incompletei5e8:intervali1800e5:peers0:e";
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    started
        .write_all(&[head.as_bytes(), body].concat())
        .await
        .unwrap();
    let answered = published_once(&rig.service, job_id, |torrent| {
        torrent
            .trackers
            .first()
            .is_some_and(|tracker| tracker.status == TrackerStatus::Working)
    })
    .await;
    let tracker = &answered.trackers[0];
    assert_eq!((tracker.seeders, tracker.leechers), (Some(3), Some(5)));
    assert_eq!(tracker.interval_seconds, Some(1800));
    assert_eq!(tracker.fails, 0);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.trackers[0].status, TrackerStatus::Working);
    assert_eq!(tracker.last_announce_ms, view.trackers[0].last_announce_ms);
    drop(listener);
    rig.service.stop().await;
}

// A magnet's metainfo is published as it arrives, with no tick: its size
// is the metainfo's from then on. Its tracker holds the announce until the
// resume has published, then names a peer that has the metainfo and none
// of the pieces, so no piece landing publishes it instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnets_metainfo_is_published_as_it_arrives() {
    use tokio::io::AsyncWriteExt;

    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let shared = metainfo("set", &files, 16_384, false);
    let seeds = tempfile::tempdir().unwrap();
    let (seeding, _) = sharing(seeds.path(), &shared, &[]).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let hash = parse(&shared).unwrap().hashes.v1.unwrap().as_string();

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = fetching_from(&rig.service, &hash, port).await;
    let (mut started, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("started"));
    let mut body = b"d8:intervali1800e5:peers6:".to_vec();
    body.extend_from_slice(&peer.ip().octets());
    body.extend_from_slice(&peer.port().to_be_bytes());
    body.push(b'e');
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    started
        .write_all(&[head.as_bytes(), &body].concat())
        .await
        .unwrap();

    let total = data.len() as u64;
    let arrived = published_once(&rig.service, job_id, |torrent| torrent.total_bytes != 0).await;
    assert_eq!(arrived.total_bytes, total);
    assert_eq!(arrived.wanted_bytes, total);
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().total_bytes,
        total
    );
    drop(listener);
    rig.service.stop().await;
    seeding.stop().await;
}

// Turning torrents off closes their sessions, and what is published says
// so at once: the route a torrent names holds no session, and shows none
// of the closed session's figures.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turning_torrents_off_publishes_their_routes_closed() {
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("closed", 4))))
        .await
        .unwrap();
    rig.service.tick().await;
    rig.service.restored().await;
    let route = {
        let metrics = rig.service.metrics();
        let job = metrics.published.job(job_id).unwrap();
        let session = job.session.unwrap();
        assert!(session.open, "{session:?}");
        job.torrent.route.clone()
    };

    let mut settings = rig.service.settings();
    settings.torrent.enabled = false;
    rig.service.set_settings(settings).await;
    let metrics = rig.service.metrics();
    let published = &metrics.published;
    let job = published.job(job_id).unwrap();
    assert_eq!(job.torrent.route, route);
    // A held route carries its name and that it is not open, nothing more:
    // no figure of the closed session, no listener, no DHT.
    let held = SessionSeries {
        route: route.clone(),
        open: false,
        ..SessionSeries::default()
    };
    assert_eq!(job.session, Some(&held));
    assert_eq!(SessionSeries::held(&route), held);
    assert_eq!(
        published
            .sessions
            .iter()
            .filter(|session| session.route == route)
            .count(),
        1,
        "{:?}",
        published.sessions
    );

    // Turned on again, the first publish that shows the torrent back in
    // its session shows the session open.
    let mut watch = rig.service.inner.published.subscribe();
    let mut settings = rig.service.settings();
    settings.torrent.enabled = true;
    rig.service.set_settings(settings).await;
    let back = watch
        .wait_for(|published| {
            published.job(job_id).is_some_and(|job| {
                !matches!(
                    job.torrent.state,
                    TorrentState::Stopped | TorrentState::Queued
                )
            })
        })
        .await
        .unwrap();
    let job = back.job(job_id).unwrap();
    assert!(job.session.is_some_and(|session| session.open), "{job:?}");
    drop(back);
    rig.service.stop().await;
}

// A torrent put in its route's session is published in an open session by
// any publish from then on, not only once the tick has read the sessions:
// an action's publish can come between the two.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_given_its_session_is_published_in_it_open() {
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("given", 4))))
        .await
        .unwrap();
    rig.service.follow_routes().await;
    // The session takes the torrent through its start on its own task.
    handle_of(&rig.service, job_id)
        .await
        .wait_until_initialized()
        .await
        .unwrap();
    rig.service.publish().await;
    let metrics = rig.service.metrics();
    let job = metrics.published.job(job_id).unwrap();
    assert!(
        !matches!(
            job.torrent.state,
            TorrentState::Stopped | TorrentState::Queued
        ),
        "{job:?}"
    );
    assert!(job.session.is_some_and(|session| session.open), "{job:?}");
    rig.service.stop().await;
}

// A file's level changed on a paused torrent moves nothing a list's row
// shows: one torrent's follower is told of it, with the new level, and the
// row is not marked moved for a list.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_files_level_reaches_its_torrents_follower_and_no_list() {
    use FilePriority::*;
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("level", 1)))
        })
        .await
        .unwrap();
    // Its session finishes starting it on its own task, and each publish
    // reads it live: the rows are compared only once it shows paused and
    // no settle is under way, so nothing but the level moves them.
    handle_of(&rig.service, job_id)
        .await
        .wait_until_initialized()
        .await
        .unwrap();
    view_once(&rig.service, job_id, |view| {
        view.state == TorrentState::Paused
    })
    .await;
    rig.service
        .inner
        .settled
        .subscribe()
        .wait_for(|_| {
            !rig.service.inner.signal.pass_wanted.load(Ordering::SeqCst)
                && !rig.service.inner.settling.load(Ordering::SeqCst)
        })
        .await
        .unwrap();
    rig.service.publish().await;
    let mut one = rig.service.updates(job_id);
    let first = one.next().await.torrents;
    assert_eq!(first[0].files[0].priority, Normal);
    let row = rig.service.snapshot().row(job_id).unwrap();

    rig.service
        .set_file_priorities(job_id, &[(0, High)])
        .await
        .unwrap();
    let told = one.next().await.torrents;
    assert_eq!(told[0].files[0].priority, High);
    let after = rig.service.snapshot().row(job_id).unwrap();
    assert_eq!((after.moved, after.listed), (row.moved, row.listed));
    rig.service.stop().await;
}

// A change the sessions tell of one torrent's trackers publishes that
// torrent alone: its row, its figures and its job, read as a publish of
// every torrent reads them, while every other torrent stands as last
// published.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tracker_change_publishes_the_torrent_it_names_alone() {
    // Restored and never attached, as no session is open: nothing but the
    // calls below builds a view.
    let rig = rig(set_clock());
    let jobs = super::readings::stored_magnets(&rig.db, 2);
    rig.service.restore().await.unwrap();
    let (named, other) = (jobs[0], jobs[1]);
    rig.service.publish().await;
    let before = rig.service.inner.published.borrow().clone();
    let hash = {
        let mut live = rig.service.inner.live.lock().await;
        for job_id in [named, other] {
            live.get_mut(&job_id).unwrap().counters.uploaded_bytes = 777;
        }
        hashes_of(&live[&named].record).best()
    };

    // A hash no torrent carries publishes nothing.
    let built = rig.service.views_built();
    rig.service
        .publish_hashes(&std::collections::HashSet::from([
            weaver_torrent::core::Id20::new([9; 20]),
        ]))
        .await;
    assert_eq!(rig.service.views_built(), built);
    rig.service
        .publish_hashes(&std::collections::HashSet::from([hash]))
        .await;
    assert_eq!(rig.service.views_built() - built, 1);
    let told = rig.service.inner.published.borrow().clone();
    assert_eq!(told.job(named).unwrap().torrent.uploaded_bytes, 777);
    assert_eq!(told.job(other), before.job(other));
    assert_eq!(told.sessions, before.sessions);
    assert_eq!(
        rig.service
            .inner
            .snapshot
            .row(named)
            .unwrap()
            .view
            .uploaded_bytes,
        777
    );
    assert_eq!(
        rig.service
            .inner
            .snapshot
            .row(other)
            .unwrap()
            .view
            .uploaded_bytes,
        before.job(other).unwrap().torrent.uploaded_bytes
    );

    // What a publish of every torrent reads of the one told is what the
    // publish of it alone read.
    let row = rig.service.inner.snapshot.row(named).unwrap();
    rig.service.publish().await;
    let every = rig.service.inner.published.borrow().clone();
    assert_eq!(every.job(named), told.job(named));
    assert_eq!(
        rig.service.inner.snapshot.row(named).unwrap().view,
        row.view
    );
    assert_eq!(every.job(other).unwrap().torrent.uploaded_bytes, 777);
    rig.service.stop().await;
}
