// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_takes_the_tags_it_was_added_with_and_its_view_shows_them() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            tags: vec!["tv".to_owned(), "HD".to_owned()],
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    assert_eq!(rig.db.job_tags(job_id).unwrap(), ["HD", "tv"]);
    let before = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(before.tags, ["HD", "tv"]);
    rig.db.add_job_tags(&[job_id], ["new"]).unwrap();
    assert_eq!(
        rig.service.torrents(None).await.remove(0).tags,
        ["HD", "new", "tv"]
    );
    let after = rig.service.torrent(job_id).await.unwrap();
    let steady = super::fingerprint::ViewParts::Steady;
    let before_without = TorrentView {
        tags: after.tags.clone(),
        ..before.clone()
    };
    assert_eq!(before_without.tags, after.tags);
    assert_ne!(
        before.fingerprint(steady),
        before_without.fingerprint(steady)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_metainfo_becomes_a_job_and_its_duplicate_is_refused() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let bytes = plain("set", 1);
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes.clone())))
        .await
        .unwrap();
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(stored.name, "set");
    assert_eq!(stored.source, TorrentSource::Metainfo);
    assert_eq!(
        stored.storage_dir.as_deref(),
        Some(rig.dir.path().join("complete/set").to_str().unwrap())
    );
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.total_bytes, 3000);
    assert_eq!(view.files.len(), 1);
    assert_eq!(view.files[0].path, "set/a.bin");
    assert!(view.route.is_some());
    assert_eq!(
        rig.service
            .add_opened(request(AddSource::Metainfo(bytes)))
            .await,
        Err(AddError::Duplicate(job_id))
    );
    let infos = rig.service.job_infos().await;
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].protocol, Protocol::Torrent);
}

// A file with an extension the operator does not want is left out of the
// download, its level kept as `Ignored`, and the torrent completes on the
// files it wants. One that leaves no file wanted is refused, and so is one
// named after an executable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_leaves_out_unwanted_files_and_refuses_nothing_wanted_and_executables() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.db
        .save_post_processing_settings(&PostProcessingSettings {
            unacceptable_extensions: vec!["nfo".into()],
            ..PostProcessingSettings::default()
        })
        .unwrap();
    let files: [(&str, &[u8]); 2] = [("a.bin", &[1; 128]), ("b.nfo", &[2; 64])];
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set", &files, 64, false,
        ))))
        .await
        .unwrap();
    assert_eq!(
        rig.db.torrent_file_priorities(job_id).unwrap(),
        [FilePriority::Normal, FilePriority::Ignored]
    );
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.wanted_bytes, 128);

    // Only the wanted file's pieces come, and it completes.
    let mut published = rig.service.inner.published.subscribe();
    let whole: Vec<u8> = files.iter().flat_map(|(_, bytes)| bytes.to_vec()).collect();
    fetch_pieces(&rig.service, job_id, &whole, Some(&[0, 1]), |_| {}).await;
    published
        .wait_for(|_| {
            rig.db
                .get_torrent(job_id)
                .unwrap()
                .unwrap()
                .completed_at_ms
                .is_some()
        })
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert!(view.completed_at_ms.is_some());
    assert_eq!(view.wanted_bytes, 128);

    let unwanted = metainfo("set", &[("set/b.nfo", &[2; 10])], 64, false);
    assert_eq!(
        rig.service
            .add_opened(request(AddSource::Metainfo(unwanted)))
            .await,
        Err(AddError::Refused(Refusal::Unwanted {
            path: "set/b.nfo".into(),
            pattern: "nfo".into()
        }))
    );
    let executable = metainfo("setup.exe", &[("setup.exe", &[1; 100])], 64, false);
    assert_eq!(
        rig.service
            .add_opened(request(AddSource::Metainfo(executable)))
            .await,
        Err(AddError::Refused(Refusal::Executable("setup.exe".into())))
    );
    assert_eq!(rig.db.list_torrents().unwrap().len(), 1);
    rig.service.stop().await;
}

// The magnet of `job_id` once its fetch has ended and left it held: a fetch
// publishes as it ends.
async fn magnet_held(service: &TorrentService, job_id: JobId) {
    let mut published = service.inner.published.subscribe();
    loop {
        published.borrow_and_update();
        {
            let live = service.inner.live.lock().await;
            if live[&job_id].held.is_some() && live[&job_id].resolving.is_none() {
                return;
            }
        }
        published.changed().await.unwrap();
    }
}

// A magnet is added whatever its route's state (libtorrent
// `session_impl::add_torrent` starts a magnet at once): on a route that is
// down it is held, as a torrent file's is, and fetches nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_magnet_added_on_a_route_that_is_down_is_held() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let hash = "0123456789abcdef0123456789abcdef01234567";
    rig.routes.set_down(Some("tunnel gone"));
    let magnet = format!("magnet:?xt=urn:btih:{hash}&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce");
    let job_id = rig
        .service
        .add(request(AddSource::Magnet(magnet)))
        .await
        .unwrap();
    magnet_held(&rig.service, job_id).await;
    assert_eq!(rig.routes.opened.load(Ordering::SeqCst), 0);
    // A base32 hash names the same torrent as its hex form.
    let base32 = "AERUKZ4JVPG66AJDIVTYTK6N54ASGRLH";
    let magnet = format!("magnet:?xt=urn:btih:{base32}&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce");
    assert_eq!(
        rig.service.add(request(AddSource::Magnet(magnet))).await,
        Err(AddError::Duplicate(job_id))
    );
    assert_eq!(rig.db.list_torrents().unwrap().len(), 1);
    rig.service.stop().await;
}

// A magnet's add is answered while its route's session is still opening: it
// does not wait on the route, and its fetch does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_magnets_add_is_answered_before_its_routes_session_opens() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.routes.unanswered.send_replace(true);
    let mut asked = rig.routes.asked.subscribe();
    let before = *asked.borrow_and_update();

    // The route's open is held unanswered, and the add still answers.
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let magnet = format!("magnet:?xt=urn:btih:{hash}&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce");
    let job_id = rig
        .service
        .add(request(AddSource::Magnet(magnet)))
        .await
        .unwrap();
    asked.wait_for(|asked| *asked > before).await.unwrap();
    assert_eq!(rig.routes.opened.load(Ordering::SeqCst), 0);
    assert!(matches!(
        rig.service.inner.live.lock().await[&job_id].resolving,
        Some(Resolving::Fetching(_))
    ));
    rig.service.stop().await;
}

// A route with no DHT cannot find peers for a magnet with no trackers. The
// add says so when the route's session runs; one added before the route
// opened fails with the same words once it has.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_magnet_with_no_trackers_is_turned_away_on_a_route_with_no_dht() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let bare = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";

    // No session runs on the route yet: the add answers, and the fetch
    // fails the job once the route is open.
    let mut retired = rig.service.retired();
    let before = *retired.borrow_and_update();
    let job_id = rig
        .service
        .add(request(AddSource::Magnet(bare.to_owned())))
        .await
        .unwrap();
    retired.wait_for(|retired| *retired > before).await.unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "failed");
    assert_eq!(history.error_message.as_deref(), Some(NEEDS_DHT));

    // The route's session runs: the add is refused.
    rig.service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let other = "magnet:?xt=urn:btih:89abcdef0123456789abcdef0123456789abcdef";
    assert_eq!(
        rig.service
            .add(request(AddSource::Magnet(other.to_owned())))
            .await,
        Err(AddError::NoNetwork(NEEDS_DHT.into()))
    );
    rig.service.stop().await;
}

const V2_ONLY: &[u8] = include_bytes!(concat!(
    std::env!("CARGO_MANIFEST_DIR"),
    "/../../../engines/weaver-torrent/resources/test/libtorrent/v2_only.torrent"
));
const V2_HYBRID: &[u8] = include_bytes!(concat!(
    std::env!("CARGO_MANIFEST_DIR"),
    "/../../../engines/weaver-torrent/resources/test/libtorrent/v2_hybrid.torrent"
));

// A v2 or hybrid torrent is one job, found by either of its hashes, whether
// it arrives as metainfo or as a magnet naming one hash.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_v2_torrent_is_added_and_found_by_either_hash() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();

    let v2_only = rig
        .service
        .add_opened(request(AddSource::Metainfo(V2_ONLY.to_vec())))
        .await
        .unwrap();
    let stored = rig.db.get_torrent(v2_only).unwrap().unwrap();
    assert_eq!(stored.infohash_v1, None);
    let v2 = Id32::new(stored.infohash_v2.unwrap()).as_string();
    let view = rig.service.torrent(v2_only).await.unwrap();
    assert_eq!(view.version, Some(TorrentVersion::V2));
    assert_eq!(view.infohash_v1, None);
    assert_eq!(view.infohash_v2.as_deref(), Some(v2.as_str()));
    assert_eq!(view.infohash, v2[..40]);
    assert_eq!(
        rig.service
            .add_opened(request(AddSource::Metainfo(V2_ONLY.to_vec())))
            .await,
        Err(AddError::Duplicate(v2_only))
    );

    let hybrid = rig
        .service
        .add_opened(request(AddSource::Metainfo(V2_HYBRID.to_vec())))
        .await
        .unwrap();
    let stored = rig.db.get_torrent(hybrid).unwrap().unwrap();
    let v1 = Id20::new(stored.infohash_v1.unwrap()).as_string();
    let v2 = Id32::new(stored.infohash_v2.unwrap()).as_string();
    let view = rig.service.torrent(hybrid).await.unwrap();
    assert_eq!(view.version, Some(TorrentVersion::Hybrid));
    assert_eq!(view.infohash, v1);
    assert!(view.magnet.contains(&format!("urn:btih:{v1}")));
    assert!(view.magnet.contains(&format!("urn:btmh:1220{v2}")));
    let tracker = "&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce";
    for xt in [format!("urn:btih:{v1}"), format!("urn:btmh:1220{v2}")] {
        assert_eq!(
            rig.service
                .add_opened(request(AddSource::Magnet(format!(
                    "magnet:?xt={xt}{tracker}"
                ))))
                .await,
            Err(AddError::Duplicate(hybrid))
        );
    }

    // A magnet naming only a v2 hash waits for its metainfo with that hash.
    let fresh = "ab".repeat(32);
    let magnet = rig
        .service
        .add_opened(request(AddSource::Magnet(format!(
            "magnet:?xt=urn:btmh:1220{fresh}{tracker}"
        ))))
        .await
        .unwrap();
    let stored = rig.db.get_torrent(magnet).unwrap().unwrap();
    assert_eq!(stored.infohash_v1, None);
    assert_eq!(stored.infohash_v2, Some([0xab; 32]));
}

// The metainfo's own word on who made it, when, and why stays with the
// torrent; a torrent without it says nothing.
#[test]
fn a_metainfo_says_who_made_it_and_when() {
    let bare = plain("set", 1);
    let mut said = b"d".to_vec();
    bstr(&mut said, b"comment");
    bstr(&mut said, b"  a note ");
    bstr(&mut said, b"created by");
    bstr(&mut said, b"maker 1.0");
    bstr(&mut said, b"creation date");
    bint(&mut said, 1_600_000_000);
    said.extend_from_slice(&bare[1..]);
    let about = parse(&said).unwrap().about;
    assert_eq!(
        (about.created_by, about.created_at_ms, about.comment),
        (
            Some("maker 1.0".to_owned()),
            Some(1_600_000_000_000),
            Some("a note".to_owned()),
        )
    );
    let about = parse(&bare).unwrap().about;
    assert_eq!(
        (about.created_by, about.created_at_ms, about.comment),
        (None, None, None)
    );
}

// Two torrents with one name each get a directory of their own, made when
// they are added.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrents_with_one_name_get_their_own_directories() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let mut dirs = Vec::new();
    for seed in [1, 2] {
        let job_id = rig
            .service
            .add_opened(request(AddSource::Metainfo(plain("set", seed))))
            .await
            .unwrap();
        let dir = rig
            .db
            .get_torrent(job_id)
            .unwrap()
            .unwrap()
            .storage_dir
            .unwrap();
        assert!(Path::new(&dir).is_dir());
        dirs.push(dir);
    }
    assert_eq!(
        dirs[0],
        rig.dir.path().join("complete/set").to_str().unwrap()
    );
    assert_ne!(dirs[0], dirs[1]);
}

// A magnet's trackers are announced to while its metainfo is fetched, and
// its rows say so: updating while the announce waits, working once the
// tracker answers, with the next announce due at the tracker's interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnets_trackers_read_as_announced_while_its_metainfo_is_fetched() {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("http://127.0.0.1:{port}/announce");
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = fetching_from(&rig.service, &"fe".repeat(20), port).await;
    // A fetch's table is followed like a run's; the beat covers the view
    // changes nothing pushes.
    let mut updates = rig.service.updates(Some(job_id), Duration::from_millis(10));

    let mut started = heard_announce(&listener).await;
    let updating = loop {
        let views = updates.next().await.torrents;
        if views[0].trackers[0].updating {
            break views[0].clone();
        }
    };
    assert_eq!(updating.state, TorrentState::FetchingMetadata);
    assert_eq!(updating.trackers[0].url, url);

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
            break views[0].clone();
        }
    };
    assert_eq!(working.state, TorrentState::FetchingMetadata);
    let tracker = &working.trackers[0];
    assert!(!tracker.updating);
    assert_eq!((tracker.seeders, tracker.leechers), (Some(3), Some(5)));
    let at = tracker.last_announce_ms.unwrap();
    assert_eq!(tracker.next_announce_ms, Some(at + 1_800_000));
    // Gone, the tracker refuses the stopped announce at once.
    drop(listener);
    rig.service.stop().await;
}

// A magnet's fetch announces `started`, held unanswered until its row reads
// updating, then answered: the tracker is owed `stopped`.
async fn started_and_answered(
    listener: &tokio::net::TcpListener,
    updates: &mut updates::TorrentUpdates,
) {
    let (stream, event) = heard_event(listener).await;
    assert_eq!(event.as_deref(), Some("started"));
    while !updates.next().await.torrents[0].trackers[0].updating {}
    answer_no_peers(stream).await;
    loop {
        let views = updates.next().await.torrents;
        let tracker = &views[0].trackers[0];
        if !tracker.updating && tracker.status == TrackerStatus::Working {
            break;
        }
    }
}

// A magnet paused while its metainfo is fetched stops at its tracker, once,
// and fetches again when resumed; removed while fetching, it stops again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnet_paused_or_removed_while_its_metainfo_is_fetched_stops_at_its_tracker() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = fetching_from(&rig.service, &"fd".repeat(20), port).await;
    let mut updates = rig.service.updates(Some(job_id), Duration::from_millis(10));

    started_and_answered(&listener, &mut updates).await;
    let table = rig.service.inner.live.lock().await[&job_id]
        .fetching
        .clone();

    rig.service.pause(job_id).await.unwrap();
    let (stream, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("stopped"));
    answer_no_peers(stream).await;
    // The ended fetch's table, given late, is not the paused magnet's.
    assert!(table.is_some());
    rig.service.fetching(job_id, table).await;
    assert!(
        rig.service.inner.live.lock().await[&job_id]
            .fetching
            .is_none()
    );
    // Paused, the magnet shows no fetch's table, so the rows read from here
    // on are the next fetch's.
    while updates.next().await.torrents[0].trackers[0].status != TrackerStatus::NotContacted {}

    rig.service.resume(job_id).await.unwrap();
    started_and_answered(&listener, &mut updates).await;

    rig.service.remove(job_id, false, false).await.unwrap();
    let (stream, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("stopped"));
    answer_no_peers(stream).await;
    rig.service.stop().await;
}

// A magnet claims its directory only once its metainfo is in hand: paused
// and then removed while it is fetched, it leaves no directory behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnet_removed_before_its_metainfo_leaves_no_directory() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let complete = rig.dir.path().join("complete");
    let left = || {
        std::fs::read_dir(&complete)
            .map(|entries| entries.count())
            .unwrap_or(0)
    };
    let job_id = fetching_from(&rig.service, &"fc".repeat(20), port).await;
    let mut updates = rig.service.updates(Some(job_id), Duration::from_millis(10));
    started_and_answered(&listener, &mut updates).await;
    assert_eq!(left(), 0);

    rig.service.pause(job_id).await.unwrap();
    let (stream, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("stopped"));
    answer_no_peers(stream).await;
    rig.service.remove(job_id, false, false).await.unwrap();
    assert!(rig.service.torrent(job_id).await.is_none());
    assert_eq!(left(), 0);
    rig.service.stop().await;
}

// A magnet whose route changes while its metainfo is fetched ends that
// fetch, which its old session lets go with `stopped`, and fetches again on
// the new route's session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnet_fetching_when_its_route_changes_fetches_again_on_the_new_route() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = fetching_from(&rig.service, &"fc".repeat(20), port).await;
    let mut updates = rig.service.updates(Some(job_id), Duration::from_millis(10));
    started_and_answered(&listener, &mut updates).await;
    let old = rig.service.inner.live.lock().await[&job_id]
        .route
        .clone()
        .unwrap();

    rig.routes.route(TorrentScope::Default, 2);
    let new = rig.routes.plan(TorrentScope::Default).key;
    assert_ne!(old, new);
    rig.service.follow_routes().await;

    // The old fetch's `stopped` and the new one's `started`, in either order.
    let mut events = Vec::new();
    for _ in 0..2 {
        let (stream, event) = heard_event(&listener).await;
        events.push(event.unwrap());
        answer_no_peers(stream).await;
    }
    events.sort();
    assert_eq!(events, ["started", "stopped"]);
    let live = rig.service.inner.live.lock().await;
    assert_eq!(live[&job_id].route.as_ref(), Some(&new));
    assert!(matches!(
        live[&job_id].resolving,
        Some(Resolving::Fetching(_))
    ));
    drop(live);
    let running = rig.service.inner.sessions.running().await;
    assert!(running.contains_key(&new), "{:?}", running.keys());
    rig.service.stop().await;
}

// A magnet added paused fetches nothing and announces nothing until it is
// resumed; resumed, its fetch announces `started` once, and removed, it
// stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnet_added_paused_announces_only_once_resumed() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Magnet(format!(
                "magnet:?xt=urn:btih:{}&tr=http%3A%2F%2F127.0.0.1%3A{port}%2Fannounce",
                "fe".repeat(20)
            )))
        })
        .await
        .unwrap();
    // The add started no fetch: the magnet waits for its resume.
    assert!(matches!(
        rig.service.inner.live.lock().await[&job_id].resolving,
        Some(Resolving::Paused)
    ));
    tracked(
        &rig.service,
        job_id,
        &format!("http://127.0.0.1:{port}/announce"),
    )
    .await;
    let mut updates = rig.service.updates(Some(job_id), Duration::from_millis(10));

    rig.service.resume(job_id).await.unwrap();
    started_and_answered(&listener, &mut updates).await;

    // The next announce the tracker hears is the remove's `stopped`: one
    // fetch, one presence.
    rig.service.remove(job_id, false, false).await.unwrap();
    let (stream, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("stopped"));
    answer_no_peers(stream).await;
    rig.service.stop().await;
}
