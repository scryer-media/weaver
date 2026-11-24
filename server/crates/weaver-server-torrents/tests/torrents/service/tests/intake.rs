// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::history::queries::HistoryQueries;
use crate::post_processing::settings::PostProcessingSettingsQueries;
use crate::proxies::ProxiesDnsPersistenceQueries;
use crate::proxies::ProxiesPersistenceQueries;
use crate::tags::TagsQueries;
use crate::torrents::queries::TorrentsQueries;
use weaver_server_settings::categories::persistence::CategoriesPersistenceQueries;
use weaver_server_settings::categories::queries::CategoriesQueries;

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
        Some(
            rig.dir
                .path()
                .join("complete")
                .join("set")
                .to_str()
                .unwrap()
        )
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
// files it wants. One whose every file is unwanted is added with nothing
// wanted and completes at once, fetching nothing, as qBittorrent's filename
// filter leaves such a torrent finished. One named after an executable is
// refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_leaves_out_unwanted_files_even_all_of_them_and_refuses_executables() {
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

    let unwanted = metainfo("notes", &[("set/b.nfo", &[2; 10])], 64, false);
    let mut published = rig.service.inner.published.subscribe();
    let nothing_wanted = rig
        .service
        .add_opened(request(AddSource::Metainfo(unwanted)))
        .await
        .unwrap();
    assert_eq!(
        rig.db.torrent_file_priorities(nothing_wanted).unwrap(),
        [FilePriority::Ignored]
    );
    published
        .wait_for(|_| {
            rig.db
                .get_torrent(nothing_wanted)
                .unwrap()
                .unwrap()
                .completed_at_ms
                .is_some()
        })
        .await
        .unwrap();
    let view = rig.service.torrent(nothing_wanted).await.unwrap();
    assert_eq!((view.wanted_bytes, view.downloaded_bytes), (0, 0));
    let executable = metainfo("setup.exe", &[("setup.exe", &[1; 100])], 64, false);
    assert_eq!(
        rig.service
            .add_opened(request(AddSource::Metainfo(executable)))
            .await,
        Err(AddError::Refused(Refusal::Executable("setup.exe".into())))
    );
    assert_eq!(rig.db.list_torrents().unwrap().len(), 2);
    rig.service.stop().await;
}

// The magnet of `job_id`, the service's first, once its fetch has ended and
// left it held. A fetch's end rings after the hold is set; the published
// figures do not move for a torrent no publish of every torrent has placed.
async fn magnet_held(service: &TorrentService, job_id: JobId) {
    service.fetches_ended(1).await;
    let live = service.inner.live.lock().await;
    assert!(live[&job_id].held.is_some() && live[&job_id].resolving.is_none());
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
        rig.dir
            .path()
            .join("complete")
            .join("set")
            .to_str()
            .unwrap()
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
    // A fetch's table is followed like a run's: its tracker's answers move
    // the torrent's row, which wakes its follower.
    let mut updates = rig.service.updates(job_id);

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
    let mut updates = rig.service.updates(job_id);

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

    rig.service.remove(job_id, false).await.unwrap();
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
    let mut updates = rig.service.updates(job_id);
    started_and_answered(&listener, &mut updates).await;
    assert_eq!(left(), 0);

    rig.service.pause(job_id).await.unwrap();
    let (stream, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("stopped"));
    answer_no_peers(stream).await;
    rig.service.remove(job_id, false).await.unwrap();
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
    let mut updates = rig.service.updates(job_id);
    started_and_answered(&listener, &mut updates).await;
    let old = rig.service.inner.live.lock().await[&job_id]
        .route
        .key()
        .cloned()
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
    assert_eq!(live[&job_id].route.key(), Some(&new));
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
    let mut updates = rig.service.updates(job_id);

    rig.service.resume(job_id).await.unwrap();
    started_and_answered(&listener, &mut updates).await;

    // The next announce the tracker hears is the remove's `stopped`: one
    // fetch, one presence.
    rig.service.remove(job_id, false).await.unwrap();
    let (stream, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("stopped"));
    answer_no_peers(stream).await;
    rig.service.stop().await;
}

// The magnet added, the `fetch`th whose fetch ends on `service`, and the
// figures published for it once its metainfo is in hand. A fetch's end
// publishes only the torrents already in the figures; a new one joins them
// at the next publish of every torrent, the tick's, which this rig's clock
// never makes, so the test makes it. A magnet retired, as a failed fetch
// retires it, fails.
async fn resolved_magnet(
    service: &TorrentService,
    magnet: String,
    fetch: u64,
) -> (JobId, TorrentSeries) {
    let retired = *service.retired().borrow();
    let job_id = service
        .add(request(AddSource::Magnet(magnet)))
        .await
        .unwrap();
    service.fetches_ended(fetch).await;
    assert_eq!(
        *service.retired().borrow(),
        retired,
        "the magnet was retired: {:?}",
        service
            .inner
            .db
            .get_job_history(job_id.0)
            .unwrap()
            .map(|job| job.error_message)
    );
    service.publish().await;
    let published = service.inner.published.borrow();
    let torrent = published.job(job_id).unwrap().torrent;
    assert_ne!(torrent.total_bytes, 0, "the metainfo is in hand");
    (job_id, TorrentSeries::clone(torrent))
}

// A magnet of `metainfo` naming the seeder at `peer` as its one source:
// no trackers, and `more` after its peer.
fn magnet_of_peer(metainfo: &[u8], peer: std::net::SocketAddr, more: &str) -> String {
    let hash = parse(metainfo).unwrap().hashes.v1.unwrap().as_string();
    format!("magnet:?xt=urn:btih:{hash}&x.pe={peer}{more}")
}

// A magnet with no trackers that names its peers (`x.pe`) finds them on a
// route with no DHT: its fetch is not failed for the DHT it does not need,
// and, once the route's session runs, neither is its add.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnet_that_names_its_peers_needs_no_dht() {
    let data: Vec<u8> = (0..40_000u32).map(|at| (at % 241) as u8).collect();
    let first = metainfo("first", &[("first/a.bin", &data)], 16_384, false);
    let second = metainfo(
        "second",
        &[("second/b.bin", &data[..30_000])],
        16_384,
        false,
    );
    let seeds = tempfile::tempdir().unwrap();
    let (one, _) = sharing(seeds.path(), &first, &[]).await;
    let (two, _) = sharing(seeds.path(), &second, &[]).await;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    // The route's session is opened by this magnet's fetch.
    let magnet = magnet_of_peer(&first, one.listen_addr().unwrap(), "");
    let (_, resolved) = resolved_magnet(&rig.service, magnet, 1).await;
    assert_eq!(resolved.total_bytes, 40_000);
    // The route's session runs now.
    let magnet = magnet_of_peer(&second, two.listen_addr().unwrap(), "");
    let (_, resolved) = resolved_magnet(&rig.service, magnet, 2).await;
    assert_eq!(resolved.total_bytes, 30_000);
    rig.service.stop().await;
    one.stop().await;
    two.stop().await;
}

// A magnet that selects some of its files (`so=`) downloads those alone,
// as libtorrent's `parse_magnet_uri` has it: the files it leaves out are
// Ignored once its metainfo is in hand.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnets_selection_is_kept_with_its_metainfo() {
    let data: Vec<u8> = (0..50_000u32).map(|at| (at % 239) as u8).collect();
    let files = [
        ("set/a.bin", &data[..20_000]),
        ("set/b.bin", &data[20_000..]),
    ];
    let shared = metainfo("set", &files, 16_384, false);
    let seeds = tempfile::tempdir().unwrap();
    let (seeding, _) = sharing(seeds.path(), &shared, &[]).await;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let magnet = magnet_of_peer(&shared, seeding.listen_addr().unwrap(), "&so=0");
    let (job_id, resolved) = resolved_magnet(&rig.service, magnet, 1).await;
    assert_eq!(resolved.total_bytes, 50_000);
    assert_eq!(resolved.wanted_bytes, 20_000);
    assert_eq!(
        rig.db.torrent_file_priorities(job_id).unwrap(),
        [FilePriority::Normal, FilePriority::Ignored]
    );
    rig.service.stop().await;
    seeding.stop().await;
}

// The web seeds a torrent's run has, once the torrent is attached.
async fn attached_seeds(service: &TorrentService, job_id: JobId) -> Vec<String> {
    let mut published = service.inner.published.subscribe();
    loop {
        published.borrow_and_update();
        {
            let live = service.inner.live.lock().await;
            if let Some(attached) = live
                .get(&job_id)
                .and_then(|torrent| torrent.attached.as_ref())
            {
                let mut seeds: Vec<String> = attached
                    .handle
                    .web_seeds()
                    .into_iter()
                    .map(|seed| seed.url)
                    .collect();
                seeds.sort();
                return seeds;
            }
        }
        published.changed().await.unwrap();
    }
}

// The metainfo column of a torrent's row, as stored.
fn stored_metainfo(db: &Database, job_id: JobId) -> Vec<u8> {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    let datastore = db.datastore();
    db.run_sql_blocking_read(async move {
        let rows = SqlRuntime::fetch_all(
            datastore.read_exec(),
            "SELECT metainfo FROM torrents WHERE job_id = {}",
            &[SqlArg::I64(job_id.0 as i64)],
        )
        .await?;
        let metainfo = rows[0].bytes("metainfo")?;
        Ok::<_, crate::StateError>(metainfo)
    })
    .unwrap()
}

// `urls` as a bencoded list.
fn bencoded_list(urls: &[&str]) -> Vec<u8> {
    let mut list = b"l".to_vec();
    for url in urls {
        list.extend_from_slice(format!("{}:{url}", url.len()).as_bytes());
    }
    list.push(b'e');
    list
}

// A magnet's web seeds (`ws`) are kept with its metainfo, each once, in the
// stored row sealed with the rest of it, and the torrent's run has them, as
// it does again after a restart reads them back from that row. The seeds
// are on loopback, so the run never asks them: a seed the torrent names is
// asked at a public address only (`NamedBy::may_ask`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnets_web_seeds_are_stored_with_its_metainfo_and_kept_across_a_restart() {
    use weaver_torrent::torrent_from_bytes;

    let data: Vec<u8> = (0..40_000u32).map(|at| (at % 233) as u8).collect();
    let shared = metainfo("set", &[("set/a.bin", &data)], 16_384, false);
    let seeds = tempfile::tempdir().unwrap();
    let (seeding, _) = sharing(seeds.path(), &shared, &[]).await;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let one = "http://127.0.0.1:9/one";
    let two = "http://127.0.0.1:9/two";
    let magnet = magnet_of_peer(
        &shared,
        seeding.listen_addr().unwrap(),
        &format!("&ws={one}&ws={two}&ws={one}"),
    );
    let (job_id, _) = resolved_magnet(&rig.service, magnet, 1).await;
    assert_eq!(attached_seeds(&rig.service, job_id).await, [one, two]);

    // Stored once each, after the fetched metainfo's own (it has none), in
    // a metainfo of the torrent's info hash.
    let opened = rig
        .db
        .get_torrent(job_id)
        .unwrap()
        .unwrap()
        .metainfo
        .unwrap();
    let read = torrent_from_bytes(&opened).unwrap();
    assert_eq!(
        read.url_list.as_ref().map(|list| list.0.0),
        Some(&bencoded_list(&[one, two])[..])
    );
    assert_eq!(
        parse(&opened).unwrap().hashes.v1,
        parse(&shared).unwrap().hashes.v1
    );
    // The row holds it sealed: no seed is readable in it.
    let stored = stored_metainfo(&rig.db, job_id);
    assert!(stored.starts_with(b"enc:b1:"));
    assert!(!stored.windows(11).any(|at| at == b"127.0.0.1:9"));

    rig.service.stop().await;
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(attached_seeds(&again, job_id).await, [one, two]);
    again.stop().await;
    seeding.stop().await;
}

// A magnet's web seed, named by a public-looking host that the route's
// exit resolves, is asked for the torrent's data once its metainfo is in
// hand, and again after a restart reads the seed back from the stored
// metainfo. The route is a SOCKS5 proxy double that is handed the name and
// maps it to a seed server on loopback; the magnet's peer gives the
// metainfo and no piece, so only the web seed can serve the data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnets_web_seed_is_asked_once_its_metainfo_is_known_and_after_a_restart() {
    use crate::proxies::{DnsProfile, DnsProfileKind, DnsTrust, DnsWho, Rung};
    use tokio::io::AsyncReadExt;
    use weaver_tunnel::test_support::proxy::ProxyServerDouble;
    use weaver_tunnel::transport::TransportKind;

    let data: Vec<u8> = (0..40_000u32).map(|at| (at % 227) as u8).collect();
    let shared = metainfo("set", &[("set/a.bin", &data)], 16_384, false);
    let seeds = tempfile::tempdir().unwrap();
    // It holds no file: it gives the metainfo and no piece.
    let (peer, _) = sharing(seeds.path(), &shared, &[]).await;
    let peer_addr = peer.listen_addr().unwrap();

    // The seed server: each request's head, in the order they came. Each
    // connection is held open unanswered, so the seed asks no more on it.
    let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin_addr = origin.local_addr().unwrap();
    let (heads_tx, mut heads) = tokio::sync::mpsc::unbounded_channel::<String>();
    let served = tokio::spawn(async move {
        loop {
            let (mut stream, _) = origin.accept().await.unwrap();
            let heads_tx = heads_tx.clone();
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut buf = [0u8; 1024];
                while !head.windows(4).any(|end| end == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(read) if read > 0 => head.extend_from_slice(&buf[..read]),
                        _ => return,
                    }
                }
                let _ = heads_tx.send(String::from_utf8_lossy(&head).into_owned());
                while matches!(stream.read(&mut buf).await, Ok(read) if read > 0) {}
            });
        }
    });
    let proxy = ProxyServerDouble::start(
        TransportKind::Socks5,
        None,
        std::collections::HashMap::from([
            (("seed.example".to_owned(), 80), origin_addr),
            ((peer_addr.ip().to_string(), peer_addr.port()), peer_addr),
        ]),
    )
    .await;

    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    db.save_proxy_profile(&loopback_proxy(proxy.addr.port()))
        .unwrap();
    // Names resolve at the proxy: the seed's host is handed to it.
    let handed = db
        .save_dns_profile(
            DnsProfile {
                kind: DnsProfileKind::Route,
                id: 0,
                name: "Proxy".into(),
                who: DnsWho::Proxy,
                transit: None,
                trust: DnsTrust::AsSent,
                server_list_id: None,
                system_fallback: false,
            },
            true,
        )
        .unwrap();
    db.save_proxy_routing_policy(
        Consumer::Torrent(TorrentScope::Default),
        &RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: SYSTEM_EGRESS_ID,
                weight: 100,
                path: LegPath::Ladder {
                    rungs: vec![Rung::Proxy { id: 1 }],
                    direct_fallback: false,
                },
            }],
            dns_profile_id: Some(handed.id),
            ..Default::default()
        },
    )
    .unwrap();
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let routes: Arc<dyn Routes> = Arc::new(crate::torrents::sessions::NetworkRoutes::new(
        Arc::clone(&proxies),
        Duration::from_secs(30),
    ));
    let asked = |head: &str| {
        let lower = head.to_ascii_lowercase();
        assert!(head.starts_with("GET /"), "{head}");
        assert!(lower.contains("host: seed.example"), "{head}");
        assert!(lower.contains("range: bytes="), "{head}");
    };

    let service = service_over(&db, dir.path(), Arc::clone(&routes), job_env(), set_clock());
    service.open().await.unwrap();
    let magnet = magnet_of_peer(&shared, peer_addr, "&ws=http://seed.example/");
    let (job_id, _) = resolved_magnet(&service, magnet, 1).await;
    asked(&heads.recv().await.unwrap());
    service.stop().await;
    // What the first run asked is behind it.
    while heads.try_recv().is_ok() {}

    let again = service_over(&db, dir.path(), Arc::clone(&routes), job_env(), set_clock());
    reopen(&again).await;
    assert!(again.inner.live.lock().await.contains_key(&job_id));
    asked(&heads.recv().await.unwrap());
    // The name went to the proxy, never to a resolver here.
    assert!(
        proxy
            .targets
            .lock()
            .unwrap()
            .iter()
            .any(|(host, port)| host == "seed.example" && *port == 80)
    );
    again.stop().await;
    served.abort();
    peer.stop().await;
    proxies.network.shutdown().await;
}

// The seeds a magnet names join the metainfo's own `url-list` after them,
// each URL once; a link that names none, or names more than a torrent file
// may hold, leaves the metainfo as fetched.
#[test]
fn a_magnets_web_seeds_join_the_metainfos_url_list_once() {
    use super::super::intake::with_web_seeds;
    use weaver_torrent::bencode::bencode_serialize_to_writer;
    use weaver_torrent::{
        ByteBuf, MAX_TORRENT_FILE_BYTES, SeedUrls, WebSeed, WebSeedKind, torrent_from_bytes,
    };

    let seed = |url: &str, kind| WebSeed {
        url: url.to_owned(),
        kind,
        named_by: Default::default(),
    };
    let own = "http://a.example/";
    let added = "http://b.example/";
    let fetched = plain("set", 1);
    let url_list = |bytes: &[u8]| {
        torrent_from_bytes(bytes)
            .unwrap()
            .url_list
            .map(|list| list.0.0.to_vec())
    };

    // None named: the bytes as fetched.
    assert_eq!(with_web_seeds(Bytes::from(fetched.clone()), &[]), fetched);
    let http = [seed(added, WebSeedKind::Http), seed("", WebSeedKind::Url)];
    assert_eq!(with_web_seeds(Bytes::from(fetched.clone()), &http), fetched);

    // The metainfo's own seed, a string, stays first; the link's copy of it
    // and its own second copy are written once.
    let mut meta = torrent_from_bytes(&fetched).unwrap();
    let string = format!("{}:{own}", own.len()).into_bytes();
    meta.url_list = Some(SeedUrls(ByteBuf(&string)));
    let mut with_own = Vec::new();
    bencode_serialize_to_writer(&meta, &mut with_own).unwrap();
    let named = [
        seed(own, WebSeedKind::Url),
        seed(added, WebSeedKind::Url),
        seed(added, WebSeedKind::Url),
    ];
    let merged = with_web_seeds(Bytes::from(with_own.clone()), &named);
    assert_eq!(url_list(&merged), Some(bencoded_list(&[own, added])));
    assert_eq!(
        parse(&merged).unwrap().hashes.v1,
        parse(&fetched).unwrap().hashes.v1
    );
    // Merged again, nothing changes.
    assert_eq!(with_web_seeds(merged.clone(), &named), merged);

    // A seed that takes the metainfo past what a torrent file may hold is
    // not kept.
    let huge = format!("http://c.example/{}", "x".repeat(MAX_TORRENT_FILE_BYTES));
    let refused = with_web_seeds(
        Bytes::from(with_own.clone()),
        &[seed(&huge, WebSeedKind::Url)],
    );
    assert_eq!(refused, with_own);
}

// The server's settings over the rig's categories, stored as a server stores
// them, for the service to make a category in.
fn making_in(rig: &Rig) -> crate::settings::SharedConfig {
    for category in rig.service.settings().categories {
        rig.db.insert_category(&category).unwrap();
    }
    let config = Arc::new(tokio::sync::RwLock::new(crate::settings::Config {
        seeding: None,
        upload_cap: None,
        torrent: Default::default(),
        data_dir: rig.dir.path().to_string_lossy().into_owned(),
        hardware_profile: None,
        intermediate_dir: None,
        complete_dir: Some(
            rig.dir
                .path()
                .join("complete")
                .to_string_lossy()
                .into_owned(),
        ),
        servers: vec![],
        categories: rig.service.settings().categories.to_vec(),
        retry: None,
        bandwidth: Default::default(),
        cleanup_after_extract: None,
        delete_failed_job_files: None,
        history_retention_days: None,
        propagation_delay_secs: None,
        max_concurrent_nzbs: None,
        watch_folder: Default::default(),
        duplicate_policy: Default::default(),
        direct_store: None,
        delivery_naming: None,
        metrics: Default::default(),
        config_path: None,
    }));
    rig.service.make_categories_in(config.clone());
    config
}

fn in_category(source: AddSource, category: &str) -> AddRequest {
    AddRequest {
        category: Some(category.to_owned()),
        ..request(source)
    }
}

// qBittorrent `SessionImpl::addTorrent_impl` (sessionimpl.cpp:2853-2857):
// a name no category goes by is made a category, stored and in the settings,
// with no folder of its own, and the torrent is in it, saved under the
// default save folder.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_add_in_a_category_there_is_none_of_makes_it() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let config = making_in(&rig);
    let job_id = rig
        .service
        .add_opened(in_category(AddSource::Metainfo(plain("set", 1)), "films"))
        .await
        .unwrap();
    let stored = rig.db.list_categories().unwrap();
    let names: Vec<&str> = stored
        .iter()
        .map(|category| category.name.as_str())
        .collect();
    assert_eq!(names, ["films", "tv"]);
    let films = &stored[0];
    assert_eq!(films.dest_dir, None);
    assert_ne!(films.id, stored[1].id);
    assert!(
        config
            .read()
            .await
            .categories
            .iter()
            .any(|category| category.id == films.id && category.name == "films")
    );
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.category.as_deref(), Some("films"));
    assert_eq!(
        rig.db
            .get_torrent(job_id)
            .unwrap()
            .unwrap()
            .storage_dir
            .as_deref(),
        Some(
            rig.dir
                .path()
                .join("complete")
                .join("films")
                .join("set")
                .to_str()
                .unwrap()
        )
    );
    rig.service.stop().await;
}

// A name that matches a category by its aliases is that category: nothing is
// made. A name that cannot be a category adds the torrent with none, as
// qBittorrent adds one whose category it cannot make.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_alias_match_or_a_bad_name_makes_no_category() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let mut settings = rig.service.settings();
    settings
        .categories
        .edit(|list| list[0].aliases = "series*".to_owned());
    rig.service.set_settings(settings).await;
    let config = making_in(&rig);
    let by_alias = rig
        .service
        .add_opened(in_category(
            AddSource::Metainfo(plain("one", 1)),
            "series-hd",
        ))
        .await
        .unwrap();
    let bad = rig
        .service
        .add_opened(in_category(AddSource::Metainfo(plain("two", 2)), "a/b"))
        .await
        .unwrap();
    assert_eq!(rig.db.list_categories().unwrap().len(), 1);
    assert_eq!(config.read().await.categories.len(), 1);
    assert_eq!(
        rig.service
            .torrent(by_alias)
            .await
            .unwrap()
            .category
            .as_deref(),
        Some("tv")
    );
    assert_eq!(rig.service.torrent(bad).await.unwrap().category, None);
    rig.service.stop().await;
}

// Two adds that name one new category at once make one, and both torrents
// are in it; a refused add, here a duplicate, makes none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_adds_in_one_new_category_make_one() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let config = making_in(&rig);
    let (one, two) = tokio::join!(
        rig.service
            .add_opened(in_category(AddSource::Metainfo(plain("one", 1)), "films")),
        rig.service
            .add_opened(in_category(AddSource::Metainfo(plain("two", 2)), "FILMS")),
    );
    let stored = rig.db.list_categories().unwrap();
    assert_eq!(stored.len(), 2, "{stored:?}");
    assert_eq!(
        config
            .read()
            .await
            .categories
            .iter()
            .filter(|category| category.name.eq_ignore_ascii_case("films"))
            .count(),
        1
    );
    for job_id in [one.unwrap(), two.unwrap()] {
        let view = rig.service.torrent(job_id).await.unwrap();
        assert!(
            view.category
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case("films")),
            "{:?}",
            view.category
        );
    }
    assert!(matches!(
        rig.service
            .add_opened(in_category(AddSource::Metainfo(plain("one", 1)), "docs"))
            .await,
        Err(AddError::Duplicate(_))
    ));
    assert_eq!(rig.db.list_categories().unwrap().len(), 2);
    rig.service.stop().await;
}

// A service with nowhere to make a category in, as one built without the
// server's settings, refuses an add in a category there is none of, as it
// did before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_service_with_nowhere_to_make_a_category_refuses_a_new_one() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    assert_eq!(
        rig.service
            .add_opened(in_category(AddSource::Metainfo(plain("one", 1)), "films"))
            .await,
        Err(AddError::Invalid("no category films".to_owned()))
    );
    assert!(rig.db.list_categories().unwrap().is_empty());
    rig.service.stop().await;
}
