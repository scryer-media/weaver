// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// The list of torrents carries each torrent's trackers, the same rows its
// own view does, and no other torrent's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_list_carries_each_torrents_trackers() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let add = |name: &'static str, seed| {
        rig.service.add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain(name, seed)))
        })
    };
    let tracked = add("tracked", 1).await.unwrap();
    let bare = add("bare", 2).await.unwrap();
    rig.service
        .add_trackers(
            tracked,
            vec![
                vec!["http://127.0.0.1:1/announce".to_owned()],
                vec!["http://127.0.0.1:2/announce".to_owned()],
            ],
        )
        .await
        .unwrap();

    let own = rig.service.torrent(tracked).await.unwrap().trackers;
    assert_eq!(
        own.iter()
            .map(|tracker| (tracker.tier, tracker.url.as_str()))
            .collect::<Vec<_>>(),
        [
            (0, "http://127.0.0.1:1/announce"),
            (1, "http://127.0.0.1:2/announce")
        ]
    );
    let listed: Vec<(JobId, Vec<TorrentTracker>)> = rig
        .service
        .torrents(None)
        .await
        .into_iter()
        .map(|view| (view.job_id, view.trackers))
        .collect();
    assert_eq!(listed, [(tracked, own), (bare, Vec::new())]);
    assert_eq!(
        rig.service.torrent(bare).await.unwrap().trackers,
        Vec::new()
    );
    rig.service.stop().await;
}

// A tracker reads as the torrent's session last heard it: updating while an
// announce waits, working with its counts once it answers and once its
// `stopped` is answered as the torrent pauses, and not working once it
// stops answering.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trackers_status_follows_its_announces() {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;

    let mut started = heard_announce(&listener).await;
    let trackers = rig.service.torrent(job_id).await.unwrap().trackers;
    assert_eq!(
        trackers
            .iter()
            .map(|tracker| (
                tracker.tier,
                tracker.url.as_str(),
                tracker.status,
                tracker.updating
            ))
            .collect::<Vec<_>>(),
        [(0, url.as_str(), TrackerStatus::NotContacted, true)]
    );

    let answer = |complete: u32| {
        let body = format!("d8:completei{complete}e10:incompletei5e8:intervali1800e5:peers0:e");
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    };
    started.write_all(answer(3).as_bytes()).await.unwrap();
    let trackers = trackers_once(&rig.service, job_id, |announces| {
        announces[0].last.is_some()
    })
    .await;
    let working = &trackers[0];
    assert_eq!(working.status, TrackerStatus::Working);
    assert!(!working.updating);
    assert_eq!((working.seeders, working.leechers), (Some(3), Some(5)));
    assert_eq!((working.message.as_deref(), working.fails), (None, 0));
    let at = working.last_announce_ms.unwrap();
    assert_eq!(working.next_announce_ms, Some(at + 1_800_000));

    // Paused, the stopped announce is the row's last outcome: nothing is
    // due until the torrent runs again, and no floor stands for a next
    // announce.
    rig.service.pause(job_id).await.unwrap();
    let mut stopped = heard_announce(&listener).await;
    stopped.write_all(answer(7).as_bytes()).await.unwrap();
    let trackers = trackers_once(&rig.service, job_id, |announces| {
        !announces[0].in_flight
            && announces[0]
                .last
                .as_ref()
                .and_then(|last| last.result.as_ref().ok())
                .is_some_and(|accepted| accepted.seeders == Some(7))
    })
    .await;
    let paused = &trackers[0];
    assert_eq!(paused.status, TrackerStatus::Working);
    assert!(!paused.updating);
    assert_eq!(
        (
            paused.next_announce_ms,
            paused.min_announce_ms,
            paused.fails
        ),
        (None, None, 0)
    );
    assert_eq!((paused.seeders, paused.peers), (Some(7), Some(0)));
    assert!(paused.last_announce_ms >= Some(at));

    // The tracker is gone: the next run's first announce finds no tracker.
    drop(listener);
    rig.service.resume(job_id).await.unwrap();
    let trackers = trackers_once(&rig.service, job_id, |announces| {
        !announces[0].in_flight
            && announces[0]
                .last
                .as_ref()
                .is_some_and(|last| last.result.is_err())
    })
    .await;
    let failed = &trackers[0];
    assert_eq!(failed.status, TrackerStatus::NotWorking);
    assert!(failed.message.is_some());
    assert_eq!((failed.seeders, failed.leechers), (None, None));
    assert!(failed.last_announce_ms.is_some());
    rig.service.stop().await;
}

// A torrent under a tracker rule that names a client names itself as that
// client to its peers and trackers; a torrent under no rule names itself as
// Weaver. The route both run on names the client of the torrent that opened
// it, the rule's, though the second torrent names another.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rules_client_names_its_torrents() {
    use crate::torrents::identity::IDENTITY;

    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.db
        .insert_tracker_rule(
            &TrackerRule {
                transports: Default::default(),
                archive_passwords: Default::default(),
                bound: None,
                id: 0,
                host_pattern: "*.test".into(),
                seeding: None,
                never_pause: false,
                unpack: None,
                client: Some(Client::QBittorrent),
                torrent_route: false,
                dht: None,
                pex: None,
                lsd: None,
                encryption: None,
                upload_cap: Default::default(),
                forced: false,
                ssl: None,
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    let kept = rig
        .service
        .add_opened(request(AddSource::Metainfo(announcing(
            &plain("kept", 1),
            "http://tracker.test/announce",
        ))))
        .await
        .unwrap();
    let plain = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("plain", 2))))
        .await
        .unwrap();

    let named = |identity: weaver_torrent::Identity| {
        (
            identity.peer_id.0[..8].to_vec(),
            identity.user_agent,
            identity.version,
        )
    };
    let qbittorrent = Client::QBittorrent.name();
    assert_eq!(
        named(handle_of(&rig.service, kept).await.shared().identity()),
        (
            qbittorrent.prefix().to_vec(),
            qbittorrent.user_agent().to_owned(),
            qbittorrent.version().to_owned()
        )
    );
    assert_eq!(
        named(handle_of(&rig.service, plain).await.shared().identity()),
        (
            IDENTITY.prefix().to_vec(),
            IDENTITY.user_agent().to_owned(),
            IDENTITY.version().to_owned()
        )
    );
    let session = session_of(&rig.service, kept).await;
    assert!(Arc::ptr_eq(
        &session,
        &session_of(&rig.service, plain).await
    ));
    assert_eq!(
        named(session.identity().clone()),
        (
            qbittorrent.prefix().to_vec(),
            qbittorrent.user_agent().to_owned(),
            qbittorrent.version().to_owned()
        )
    );
}

// libtorrent `torrent::write_resume_data` and `read_resume_data`: the peers
// and bans a torrent saved come back when it next starts, before it runs,
// and it saves them again as it stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrents_saved_peers_and_bans_outlive_a_restart() {
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
    assert_eq!(
        handle_of(&rig.service, job_id).await.resume_peers(),
        Some(ResumePeers::default())
    );
    rig.service.stop().await;
    assert_eq!(
        rig.db.torrent_peers(job_id).unwrap(),
        ResumePeers::default()
    );
    let saved = ResumePeers {
        peers: vec!["127.0.0.2:6881".parse().unwrap()],
        banned: vec!["127.0.0.3".parse().unwrap()],
        found: Vec::new(),
    };
    assert!(rig.db.save_torrent_peers(job_id, &saved).unwrap());

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(
        handle_of(&again, job_id).await.resume_peers(),
        Some(saved.clone())
    );
    again.stop().await;
    assert_eq!(rig.db.torrent_peers(job_id).unwrap(), saved);
}

// libtorrent `torrent_handle::get_peer_info` lists a torrent's connections:
// none while it is paused, and a torrent the service does not hold has none
// to list.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_torrents_peers_are_none_and_an_unknown_ones_are_not_found() {
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
    assert!(rig.service.peers(job_id).await.unwrap().is_empty());
    let unknown = JobId(job_id.0 + 100);
    assert!(matches!(
        rig.service.peers(unknown).await,
        Err(ActionError::NotFound(id)) if id == unknown
    ));
    assert!(matches!(
        rig.service
            .ban_peers(unknown, &["127.0.0.3".parse().unwrap()])
            .await,
        Err(ActionError::NotFound(id)) if id == unknown
    ));
    rig.service.stop().await;
}

// libtorrent `torrent::ban_peer` bans the address on the torrent's peer
// list, and `write_resume_data` saves it with the bans: a ban made in a
// session, and one made while the torrent is in none, are both on the list
// its next start begins with, its saved peer at a banned address gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_banned_peer_stays_banned_across_a_restart() {
    use std::net::IpAddr;
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
    let in_session: IpAddr = "127.0.0.3".parse().unwrap();
    rig.service.ban_peers(job_id, &[in_session]).await.unwrap();
    let held = handle_of(&rig.service, job_id)
        .await
        .resume_peers()
        .unwrap();
    assert_eq!(held.banned, [in_session]);
    assert_eq!(rig.db.torrent_peers(job_id).unwrap().banned, [in_session]);
    rig.service.stop().await;
    // Two peers it knows of when it next starts, beside the ban written.
    let mut known = rig.db.torrent_peers(job_id).unwrap();
    known.peers = vec![
        "127.0.0.4:6881".parse().unwrap(),
        "127.0.0.5:6881".parse().unwrap(),
    ];
    assert!(rig.db.save_torrent_peers(job_id, &known).unwrap());

    // The ban made in the session holds in the next one.
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(
        handle_of(&again, job_id)
            .await
            .resume_peers()
            .unwrap()
            .banned,
        [in_session]
    );

    // In no session: torrents are off.
    let mut settings = again.settings();
    settings.torrent.enabled = false;
    again.set_settings(settings).await;
    assert!(again.peers(job_id).await.unwrap().is_empty());
    let off: IpAddr = "127.0.0.4".parse().unwrap();
    again.ban_peers(job_id, &[off]).await.unwrap();
    let saved = ResumePeers {
        peers: vec!["127.0.0.5:6881".parse().unwrap()],
        banned: vec![in_session, off],
        found: Vec::new(),
    };
    assert_eq!(rig.db.torrent_peers(job_id).unwrap(), saved);
    again.stop().await;

    let third = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&third).await;
    assert_eq!(
        handle_of(&third, job_id).await.resume_peers(),
        Some(saved.clone())
    );
    third.stop().await;
    assert_eq!(rig.db.torrent_peers(job_id).unwrap(), saved);
}

// A ban made while the torrent is in no session is on its list once it is
// in one: attach hands the session the bans the torrent holds, with no
// read of the database between.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ban_made_in_no_session_holds_once_the_torrent_is_in_one() {
    use std::net::IpAddr;

    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    let early: IpAddr = "127.0.0.6".parse().unwrap();
    rig.service.ban_peers(job_id, &[early]).await.unwrap();
    assert_eq!(rig.db.torrent_peers(job_id).unwrap().banned, [early]);
    rig.routes.set_down(None);
    on_loopback(&rig.routes);
    rig.service.attach(job_id, None).await;
    assert_eq!(
        handle_of(&rig.service, job_id)
            .await
            .resume_peers()
            .unwrap()
            .banned,
        [early]
    );
    rig.service.stop().await;
}

// The torrent's trackers as the service lists them: tier, URL and who
// named it.
async fn listed_trackers(service: &TorrentService, job_id: JobId) -> Vec<(u32, String, NamedBy)> {
    service
        .trackers(job_id)
        .await
        .unwrap()
        .into_iter()
        .map(|tracker| (tracker.tier, tracker.url, tracker.named_by))
        .collect()
}

// A torrent file that names one tracker twice in a tier is kept with it
// once, in the table and in memory alike: the list held is the list
// written, at the add and after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_a_torrent_names_twice_is_held_as_it_is_stored() {
    use super::super::once_a_tier;
    let (a, b) = ("http://a.invalid/announce", "http://b.invalid/announce");
    let entry = |tier: u32, url: &str| TrackerEntry {
        tier,
        url: url.to_owned(),
        named_by: NamedBy::Torrent,
    };
    // Once in a tier, the first kept; another tier is another row.
    assert_eq!(
        once_a_tier(&[entry(0, a), entry(0, b), entry(0, a), entry(1, a)]),
        [entry(0, a), entry(0, b), entry(1, a)]
    );

    // d 8:announce <a> 13:announce-list l l <a> <b> <a> e e ...
    let rest = plain("twice", 7);
    let mut bytes = b"d".to_vec();
    bstr(&mut bytes, b"announce");
    bstr(&mut bytes, a.as_bytes());
    bstr(&mut bytes, b"announce-list");
    bytes.extend_from_slice(b"ll");
    for url in [a, b, a] {
        bstr(&mut bytes, url.as_bytes());
    }
    bytes.extend_from_slice(b"ee");
    bytes.extend_from_slice(&rest[1..]);

    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(bytes))
        })
        .await
        .unwrap();
    let held = |service: &TorrentService| {
        let service = service.clone();
        async move {
            let live = service.inner.live.lock().await;
            live.get(&job_id).unwrap().trackers.to_vec()
        }
    };
    let stored = |db: &Database| {
        let mut stored = db.list_torrent_trackers(job_id).unwrap();
        stored.sort_by(|a, b| (a.tier, &a.url).cmp(&(b.tier, &b.url)));
        stored
    };
    let at_add = held(&rig.service).await;
    assert_eq!(at_add, [entry(0, a), entry(0, b)]);
    assert_eq!(at_add, stored(&rig.db));
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    assert_eq!(held(&again).await, at_add);
    assert_eq!(stored(&rig.db), at_add);
    again.stop().await;
}

// Each torrent's trackers are held with it, and a boot reads the last
// torrent's key, then, once a page of torrents, the bytes each takes and
// each torrent table a restore and an attach read: the torrents' rows,
// their trackers', their counters', their files' times and their history's. A boot of no torrent reads the key alone. From then on
// publishing, listing, viewing and matching the tracker rules again read
// none, whether one torrent is live or fifty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrents_list_their_trackers_from_memory_after_one_boot_read() {
    // Fewer torrents than a page holds, so one page.
    const BOOT_READS: usize = 7;
    let a = "http://a.invalid/announce";
    let reads = |db: &Database| db.torrent_reads.load(std::sync::atomic::Ordering::Relaxed);
    for count in [1, 50] {
        let rig = rig(set_clock());
        rig.service.open().await.unwrap();
        assert_eq!(reads(&rig.db), 1, "the boot of no torrent");
        let mut jobs = Vec::new();
        for seed in 0..count {
            let name = format!("listed-{seed}");
            let job_id = rig
                .service
                .add_opened(AddRequest {
                    paused: true,
                    ..request(AddSource::Metainfo(announcing(&plain(&name, seed), a)))
                })
                .await
                .unwrap();
            jobs.push(job_id);
        }
        let first = jobs[0];
        // Two in one tier, given out of their bytes' order.
        let (b, z) = ("http://b.invalid/announce", "http://z.invalid/announce");
        rig.service
            .add_trackers(first, vec![vec![z.into(), b.into()]])
            .await
            .unwrap();
        // A magnet resolved: its trackers and its metainfo's, each tier in
        // the URLs' bytes' order.
        let bytes = announcing(&plain("magnet-listed", 200), a);
        let v1 = parse(&bytes).unwrap().hashes.v1.unwrap().0;
        let hash: String = v1.iter().map(|byte| format!("{byte:02x}")).collect();
        let magnet = rig
            .service
            .add_opened(AddRequest {
                paused: true,
                ..request(AddSource::Magnet(format!(
                    "magnet:?xt=urn:btih:{hash}&tr=http%3A%2F%2Fy.invalid%2Fannounce"
                )))
            })
            .await
            .unwrap();
        rig.service
            .resolved(magnet, Bytes::from(bytes), &[], None)
            .await
            .unwrap();
        jobs.push(magnet);
        // One in seed mode, whose attach takes the pieces it checked.
        let (metainfo, data) = placed_torrent();
        let save = rig.dir.path().join("seeded");
        std::fs::create_dir_all(save.join("placed")).unwrap();
        std::fs::write(save.join("placed/a.bin"), &data).unwrap();
        let seeded = rig
            .service
            .add_opened(AddRequest {
                paused: true,
                placement: Placement {
                    save_path: Some(save.to_string_lossy().into_owned()),
                    seed_mode: true,
                    ..Placement::default()
                },
                ..request(AddSource::Metainfo(metainfo))
            })
            .await
            .unwrap();
        jobs.push(seeded);

        let added = reads(&rig.db);
        rig.service.publish().await;
        rig.service.tick().await;
        rig.service.restored().await;
        rig.service.reload_tracker_rules().await.unwrap();
        let listed = rig.service.torrents(None).await;
        let with_files = rig.service.torrents_with_files().await;
        let one = rig.service.torrent(first).await.unwrap();
        let support = rig.service.support(first).await.unwrap();
        rig.service.reannounce_tracker(first, a).await.unwrap();
        assert_eq!(reads(&rig.db), added, "{count} torrents, after the adds");

        let entries = |view: &TorrentView| -> Vec<(u32, String, NamedBy)> {
            view.trackers
                .iter()
                .map(|tracker| (tracker.tier, tracker.url.clone(), tracker.named_by))
                .collect()
        };
        let stored = |job_id: JobId| -> Vec<(u32, String, NamedBy)> {
            rig.db
                .list_torrent_trackers(job_id)
                .unwrap()
                .into_iter()
                .map(|tracker| (tracker.tier, tracker.url, tracker.named_by))
                .collect()
        };
        let (torrent, operator) = (NamedBy::Torrent, NamedBy::Operator);
        assert_eq!(
            entries(&one),
            [
                (0, a.to_owned(), torrent),
                (1, b.to_owned(), operator),
                (1, z.to_owned(), operator)
            ]
        );
        assert_eq!(entries(&support.view), entries(&one));
        assert_eq!(
            entries(&rig.service.torrent(magnet).await.unwrap()),
            [
                (0, a.to_owned(), torrent),
                (0, "http://y.invalid/announce".to_owned(), torrent)
            ]
        );
        // What each lists is what its rows hold, read apart from it.
        assert_eq!(listed.len(), jobs.len());
        let mut held = HashMap::new();
        for view in listed.iter().chain(&with_files) {
            assert_eq!(entries(view), stored(view.job_id), "{count} torrents");
            held.insert(view.job_id, entries(view));
        }
        // The rows read apart count too.
        let before = reads(&rig.db);
        rig.service.stop().await;

        // A restart that brings every torrent back and attaches it reads
        // each torrent table once, and lists each as it was listed before.
        let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
        again.open().await.unwrap();
        again.tick().await;
        again.restored().await;
        for &job_id in &jobs {
            handle_of(&again, job_id).await;
        }
        assert_eq!(
            reads(&rig.db),
            before + BOOT_READS,
            "{count} torrents, after a restart"
        );
        for job_id in jobs {
            assert_eq!(
                entries(&again.torrent(job_id).await.unwrap()),
                held[&job_id],
                "{count} torrents"
            );
        }
        again.stop().await;
    }
}

// The trackers a metainfo or a magnet link names are the torrent's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_trackers_a_torrent_comes_with_are_the_torrents() {
    let a = "http://a.invalid/announce";
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let metainfo = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(announcing(&plain("named", 3), a)))
        })
        .await
        .unwrap();
    let magnet = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Magnet(format!(
                "magnet:?xt=urn:btih:{}&tr=http%3A%2F%2Fa.invalid%2Fannounce&tr=udp%3A%2F%2Fb.invalid%3A80",
                "ab".repeat(20)
            )))
        })
        .await
        .unwrap();
    assert_eq!(
        listed_trackers(&rig.service, metainfo).await,
        [(0, a.into(), NamedBy::Torrent)]
    );
    assert_eq!(
        listed_trackers(&rig.service, magnet).await,
        [
            (0, a.into(), NamedBy::Torrent),
            (1, "udp://b.invalid:80".into(), NamedBy::Torrent)
        ]
    );
    // As stored.
    let stored: Vec<_> = rig
        .service
        .inner
        .db
        .list_torrent_trackers(magnet)
        .unwrap()
        .into_iter()
        .map(|tracker| tracker.named_by)
        .collect();
    assert_eq!(stored, [NamedBy::Torrent, NamedBy::Torrent]);
    rig.service.stop().await;
}

// Added, replaced and removed trackers are written where the torrent's
// trackers live, reach its session, and are what a restart starts with. A
// tracker an edit names that the torrent did not list is the operator's;
// one it listed keeps who named it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracker_edits_outlive_a_restart() {
    let a = "http://a.invalid/announce";
    let b = "http://b.invalid/announce";
    let c = "udp://c.invalid:80";
    let d = "http://d.invalid/announce";
    let (torrent, operator) = (NamedBy::Torrent, NamedBy::Operator);
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    // Paused, it announces to none of them.
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(announcing(&plain("edited", 3), a)))
        })
        .await
        .unwrap();

    // Added tiers go after the last; a tracker it lists is left out.
    rig.service
        .add_trackers(
            job_id,
            vec![vec![b.into(), a.into()], vec![], vec![c.into()]],
        )
        .await
        .unwrap();
    assert_eq!(
        listed_trackers(&rig.service, job_id).await,
        [
            (0, a.into(), torrent),
            (1, b.into(), operator),
            (2, c.into(), operator)
        ]
    );

    rig.service
        .replace_trackers(
            job_id,
            vec![(1, a.into()), (0, c.into()), (0, c.into()), (2, d.into())],
        )
        .await
        .unwrap();
    assert_eq!(
        listed_trackers(&rig.service, job_id).await,
        [
            (0, c.into(), operator),
            (1, a.into(), torrent),
            (2, d.into(), operator)
        ]
    );

    rig.service
        .remove_trackers(job_id, &[a.into(), d.into()])
        .await
        .unwrap();
    assert_eq!(
        listed_trackers(&rig.service, job_id).await,
        [(0, c.into(), operator)]
    );
    let only_c = [vec![weaver_torrent::tracker::Tracker::operator(
        weaver_torrent::Url::parse(c).unwrap(),
    )]];
    assert_eq!(
        handle_of(&rig.service, job_id)
            .await
            .shared()
            .tracker_tiers(),
        only_c
    );

    // An edit naming no URL changes nothing.
    assert!(matches!(
        rig.service
            .add_trackers(job_id, vec![vec!["not a tracker".into()]])
            .await,
        Err(ActionError::Invalid(_))
    ));
    assert_eq!(
        rig.service
            .add_trackers(JobId(9_999), vec![vec![b.into()]])
            .await,
        Err(ActionError::NotFound(JobId(9_999)))
    );
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(
        listed_trackers(&again, job_id).await,
        [(0, c.into(), operator)]
    );
    assert_eq!(
        handle_of(&again, job_id).await.shared().tracker_tiers(),
        only_c
    );
    again.stop().await;
}

// A tracker edit picks the rule the torrent's trackers now match, and a
// restart reads that rule, not the one its first trackers matched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_rule_a_tracker_edit_picks_is_the_one_a_restart_reads() {
    let a = "http://a.invalid/announce";
    let b = "http://b.invalid/announce";
    let rig = rig(set_clock());
    let rule = rig
        .db
        .insert_tracker_rule(
            &TrackerRule {
                transports: Default::default(),
                archive_passwords: Default::default(),
                bound: None,
                id: 0,
                host_pattern: "b.invalid".into(),
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
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(announcing(&plain("ruled", 5), a)))
        })
        .await
        .unwrap();
    let rule_of = async |service: &TorrentService| service.inner.live.lock().await[&job_id].rule_id;
    assert_eq!(rule_of(&rig.service).await, None);

    rig.service
        .replace_trackers(job_id, vec![(0, b.into())])
        .await
        .unwrap();
    assert_eq!(rule_of(&rig.service).await, Some(rule.id));
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(rule_of(&again).await, Some(rule.id));
    again.stop().await;
}

// A tracker edit that lands after the attach read the list and before the
// session took the torrent reaches the session: the torrent runs on the
// edited list, not the one the attach read. The attach is polled by hand on
// a runtime with no other worker, so the edit lands at that one point, while
// the attach waits for its route's session.
#[tokio::test(flavor = "current_thread")]
async fn a_tracker_edit_racing_an_attach_reaches_the_session() {
    use std::task::Poll;

    let a = "http://a.invalid/announce";
    let b = "http://b.invalid/announce";
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(announcing(&plain("raced", 6), a)))
        })
        .await
        .unwrap();
    {
        let mut live = rig.service.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).unwrap();
        rig.service.detach(torrent).await;
        torrent.route = None;
    }
    // Its route's session goes with it, so the attach waits for a new one,
    // whose leg has not answered yet.
    rig.service.drop_idle_sessions().await;
    rig.routes.unanswered.send_replace(true);

    let mut attach = std::pin::pin!(rig.service.attach(job_id, None));
    let first = std::future::poll_fn(|cx| Poll::Ready(attach.as_mut().poll(cx))).await;
    assert!(first.is_pending(), "the session took the torrent at once");
    assert!(
        rig.service.inner.live.lock().await[&job_id]
            .attached
            .is_none(),
        "the attach finished before the edit"
    );
    rig.service
        .replace_trackers(job_id, vec![(0, b.into())])
        .await
        .unwrap();
    rig.routes.unanswered.send_replace(false);
    attach.await;

    assert_eq!(
        handle_of(&rig.service, job_id)
            .await
            .shared()
            .tracker_tiers(),
        [vec![weaver_torrent::tracker::Tracker::operator(
            weaver_torrent::Url::parse(b).unwrap(),
        )]]
    );
    rig.service.stop().await;
}

// A reannounce reaches the tracker now, long before the interval it set
// is up, for every tracker or for the one named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reannounce_reaches_the_tracker_now() {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("again", 4))))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    let body = b"d8:completei3e10:incompletei5e8:intervali1800e12:min intervali1800e5:peers0:e";
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let answer = [head.as_bytes(), body].concat();
    // The announce table once an answer other than `before` has landed.
    let answered = |before: Option<std::time::SystemTime>| {
        move |announces: &[TrackerAnnounce]| {
            !announces[0].in_flight
                && announces[0]
                    .last
                    .as_ref()
                    .is_some_and(|last| last.result.is_ok() && Some(last.at) != before)
        }
    };
    let last_at = |service: &TorrentService| {
        let service = service.clone();
        async move {
            handle_of(&service, job_id).await.tracker_announces()[0]
                .last
                .as_ref()
                .map(|last| last.at)
        }
    };

    let mut started = heard_announce(&listener).await;
    started.write_all(&answer).await.unwrap();
    trackers_once(&rig.service, job_id, answered(None)).await;
    let first = last_at(&rig.service).await;

    // The tracker asked for 30 minutes between announces.
    rig.service.reannounce(job_id).await.unwrap();
    let mut again = heard_announce(&listener).await;
    again.write_all(&answer).await.unwrap();
    let second = trackers_once(&rig.service, job_id, answered(first)).await;
    assert_eq!(second[0].status, TrackerStatus::Working);

    assert!(matches!(
        rig.service
            .reannounce_tracker(job_id, "http://other.invalid/announce")
            .await,
        Err(ActionError::Invalid(_))
    ));
    rig.service.reannounce_tracker(job_id, &url).await.unwrap();
    let mut once_more = heard_announce(&listener).await;
    once_more.write_all(&answer).await.unwrap();

    rig.service.dht_announce(job_id).await.unwrap();
    assert_eq!(
        rig.service.reannounce(JobId(9_999)).await,
        Err(ActionError::NotFound(JobId(9_999)))
    );
    rig.service.stop().await;
}

// An announce a tracker heard: its event, the torrent's key and peer id,
// and the counters it carried.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Heard {
    event: Option<String>,
    key: String,
    peer_id: String,
    downloaded: u64,
    uploaded: u64,
    left: u64,
}

impl Heard {
    fn of(query: &str) -> Self {
        let field = |name: &str| {
            query
                .split('&')
                .find_map(|kv| kv.strip_prefix(name)?.strip_prefix('='))
                .map(str::to_owned)
        };
        let count = |name: &str| field(name).unwrap().parse().unwrap();
        Self {
            event: field("event"),
            key: field("key").unwrap(),
            peer_id: field("peer_id").unwrap(),
            downloaded: count("downloaded"),
            uploaded: count("uploaded"),
            left: count("left"),
        }
    }

    fn identity(&self) -> (&str, &str) {
        (&self.key, &self.peer_id)
    }
}

// A tracker on loopback that names `peers` to every announce and hands the
// test each announce in the order it read them.
async fn heard_tracker(
    peers: &[std::net::SocketAddrV4],
) -> (String, tokio::sync::mpsc::UnboundedReceiver<Heard>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let mut body = format!("d8:intervali1800e5:peers{}:", peers.len() * 6).into_bytes();
    for peer in peers {
        body.extend_from_slice(&peer.ip().octets());
        body.extend_from_slice(&peer.port().to_be_bytes());
    }
    body.push(b'e');
    let (heard, announces) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let heard = heard.clone();
            let body = body.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|end| end == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => request.extend_from_slice(&buf[..read]),
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let path = request.split(' ').nth(1).unwrap();
                let (_, query) = path.split_once('?').unwrap();
                let _ = heard.send(Heard::of(query));
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                if let Err(error) = stream.write_all(&[head.as_bytes(), &body].concat()).await {
                    tracing::debug!(error = %error, "a client hung up before its answer");
                }
            });
        }
    });
    (url, announces)
}

// Once the torrent's session has the answer to its `started`: a tracker is
// owed `stopped` only once it accepted `started`.
async fn started_accepted(service: &TorrentService, job_id: JobId) {
    trackers_once(service, job_id, |announces| {
        announces.iter().any(|announce| {
            announce
                .last
                .as_ref()
                .is_some_and(|last| last.result.is_ok())
        })
    })
    .await;
}

// What a tracker hears of a seed whose move kept someone else's file at the
// new place, its pieces not forgotten for the move. The torrent stays
// moving until the check the move owes has forgotten them: a recheck asked
// for in between is refused, so none ends early on files not read and
// tells the tracker of a start the move's check then stops. The tracker
// hears one `stopped`, of the run that ended when the pieces were
// forgotten, and one `started`, of the run after the check; then the
// `stopped` of the shutdown, and nothing else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_that_kept_a_file_is_one_stopped_and_one_started_to_a_tracker() {
    use super::moves::{move_held_before_its_check, seeding_beside_a_taken_name, strangers};
    let (url, mut announces) = heard_tracker(&[]).await;
    let (rig, job_id, data, _, _) = seeding_beside_a_taken_name().await;
    tracked(&rig.service, job_id, &url).await;
    assert_eq!(
        next_event(&mut announces).await.event.as_deref(),
        Some("started")
    );
    announces_answered(&rig.service, job_id).await;

    let free = rig.dir.path().join("free");
    std::fs::create_dir_all(&free).unwrap();
    {
        let (at, theirs) = (free.join("a.bin"), strangers(&data));
        *locked(&crate::torrents::service::MOVE_BEGIN_MEETS) =
            Some(Box::new(move || std::fs::write(at, theirs).unwrap()));
    }
    let held = move_held_before_its_check(&rig, job_id, &free).await;
    let refused = rig.service.recheck(job_id).await.unwrap_err();
    assert!(
        matches!(&refused, ActionError::Invalid(why) if why.contains("moving")),
        "{refused:?}"
    );
    held.release.send(()).unwrap();
    held.moving.await.unwrap().unwrap();
    let handle = handle_of(&rig.service, job_id).await;
    handle.wait_until_initialized().await.unwrap();
    assert_eq!(handle.stats().progress_bytes, 0);

    // The run that ended and the run that began each tell the tracker on
    // their own, so it may read the two in either order.
    let mut between = [
        next_event(&mut announces).await.event.unwrap(),
        next_event(&mut announces).await.event.unwrap(),
    ];
    between.sort();
    assert_eq!(between, ["started", "stopped"]);
    announces_answered(&rig.service, job_id).await;
    rig.service.stop().await;
    let mut after = Vec::new();
    while let Ok(heard) = announces.try_recv() {
        after.extend(heard.event);
    }
    assert_eq!(after, ["stopped"], "only the shutdown's");
}

// Once no announce of the torrent's session waits for its answer and a
// tracker has answered one. Asked after an announce was heard, it says that
// announce's answer is taken: a tracker is sent no `stopped` while its last
// announce waits (see `TrackerList::stop`).
async fn announces_answered(service: &TorrentService, job_id: JobId) {
    trackers_once(service, job_id, |announces| {
        announces.iter().all(|announce| !announce.in_flight)
            && announces.iter().any(|announce| {
                announce
                    .last
                    .as_ref()
                    .is_some_and(|last| last.result.is_ok())
            })
    })
    .await;
}

// The next announce heard that carries an event.
async fn next_event(announces: &mut tokio::sync::mpsc::UnboundedReceiver<Heard>) -> Heard {
    loop {
        let heard = announces.recv().await.unwrap();
        if heard.event.is_some() {
            return heard;
        }
    }
}

// A torrent paused and resumed announces as the same torrent: one key and
// one peer id from its `started` through its `stopped` to its next
// `started`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paused_and_resumed_torrent_keeps_its_key_and_peer_id() {
    let (url, mut announces) = heard_tracker(&[]).await;
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("kept", 7)))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();
    let started = next_event(&mut announces).await;
    assert_eq!(started.event.as_deref(), Some("started"));
    started_accepted(&rig.service, job_id).await;

    rig.service.pause(job_id).await.unwrap();
    let stopped = next_event(&mut announces).await;
    assert_eq!(stopped.event.as_deref(), Some("stopped"));
    rig.service.resume(job_id).await.unwrap();
    let again = next_event(&mut announces).await;
    assert_eq!(again.event.as_deref(), Some("started"));

    assert_eq!(stopped.identity(), started.identity());
    assert_eq!(again.identity(), started.identity());
    rig.service.stop().await;
}

// A private torrent whose every file is on the unwanted list is added with
// nothing wanted, as qBittorrent adds it (`applyFilenameFilter`), and is
// finished at once (libtorrent `piece_picker::is_finished` with every piece
// filtered). Its tracker hears `started` with every byte left and nothing
// moved, and then only the `stopped` of the shutdown: the torrent never
// becomes a seed, so it never sends `completed` (libtorrent
// `torrent::finished` calls `completed` only `if (is_seed())`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_with_every_file_unwanted_starts_and_stops_and_never_completes() {
    let (url, mut announces) = heard_tracker(&[]).await;
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    rig.db
        .save_post_processing_settings(&PostProcessingSettings {
            unacceptable_extensions: vec!["exe".into()],
            ..PostProcessingSettings::default()
        })
        .unwrap();
    let files: [(&str, &[u8]); 2] = [("pack/setup.exe", &[1; 100]), ("pack/readme.exe", &[2; 60])];
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(metainfo("pack", &files, 64, true)))
        })
        .await
        .unwrap();
    assert_eq!(
        rig.db.torrent_file_priorities(job_id).unwrap(),
        [FilePriority::Ignored, FilePriority::Ignored]
    );
    tracked(&rig.service, job_id, &url).await;
    let mut published = rig.service.inner.published.subscribe();
    rig.service.resume(job_id).await.unwrap();
    let started = next_event(&mut announces).await;
    assert_eq!(started.event.as_deref(), Some("started"));
    assert_eq!(
        (started.left, started.downloaded, started.uploaded),
        (160, 0, 0)
    );
    started_accepted(&rig.service, job_id).await;
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
    assert_eq!((view.wanted_bytes, view.downloaded_bytes), (0, 0));
    announces_answered(&rig.service, job_id).await;

    rig.service.stop().await;
    let mut after = Vec::new();
    while let Ok(heard) = announces.try_recv() {
        after.push(heard);
    }
    let events: Vec<_> = after
        .iter()
        .filter_map(|heard| heard.event.as_deref())
        .collect();
    assert_eq!(events, ["stopped"], "{after:?}");
    let stopped = after.iter().find(|heard| heard.event.is_some()).unwrap();
    assert_eq!(
        (stopped.left, stopped.downloaded, stopped.uploaded),
        (160, 0, 0)
    );
    assert_eq!(stopped.identity(), started.identity());
}

// A torrent whose route changes while its old leg stands goes on over the
// new one as the same torrent: no `stopped` over the old leg, no `started`
// again over the new one, and its counters go on from where they were: the
// new session's totals are the old session's, and the trackers hear them so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_moved_to_a_new_route_keeps_its_identity_and_counters() {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, 16_384, true);
    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &private, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let (url, mut announces) = heard_tracker(&[peer]).await;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(private.to_vec()))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();
    let started = next_event(&mut announces).await;
    assert_eq!(started.event.as_deref(), Some("started"));
    let old = handle_of(&rig.service, job_id).await;
    old.wait_until_completed().await.unwrap();
    let before = old.totals();
    assert_eq!(before.downloaded_bytes, data.len() as u64);
    // The old session's `completed` is answered before the move, so the new
    // session owes none and nothing of the old one's is under way.
    let completed = next_event(&mut announces).await;
    assert_eq!(completed.event.as_deref(), Some("completed"));
    announces_answered(&rig.service, job_id).await;

    rig.routes.route(TorrentScope::Default, 2);
    rig.service.follow_routes().await;

    // A move is not a start: the old session sends no `stopped`, and the
    // new one, its tracker having answered `started`, goes on with a plain
    // announce counted on from the same start.
    let again = announces.recv().await.unwrap();
    assert_eq!(again.event, None);
    assert_eq!(again.identity(), started.identity());
    assert_eq!(
        (again.downloaded, again.uploaded),
        (before.downloaded_bytes, before.uploaded_bytes)
    );

    // The new session starts from the old one's totals as they ended; only
    // the bytes of its own connections move after, and no payload does.
    let ended = old.totals();
    let now = handle_of(&rig.service, job_id).await.totals();
    assert_eq!(
        (now.downloaded_bytes, now.uploaded_bytes, now.fetched_bytes),
        (
            ended.downloaded_bytes,
            ended.uploaded_bytes,
            ended.fetched_bytes
        )
    );
    assert_eq!(
        (ended.downloaded_bytes, ended.uploaded_bytes),
        (before.downloaded_bytes, before.uploaded_bytes)
    );
    assert!(now.fetched_protocol_bytes >= ended.fetched_protocol_bytes);
    assert!(now.uploaded_protocol_bytes >= ended.uploaded_protocol_bytes);

    // Its one `stopped` is the shutdown's, from the new session, counted on;
    // the stop waits for every session's, so all are heard by now. A
    // tracker whose last announce still waits for its answer is sent none,
    // so the new session's announce is answered first.
    announces_answered(&rig.service, job_id).await;
    rig.service.stop().await;
    let mut stopped = Vec::new();
    while let Ok(heard) = announces.try_recv() {
        if heard.event.as_deref() == Some("stopped") {
            stopped.push(heard);
        }
    }
    let [stopped] = <[Heard; 1]>::try_from(stopped).unwrap();
    assert_eq!(stopped.identity(), started.identity());
    assert_eq!(stopped.downloaded, before.downloaded_bytes);
}

// A torrent whose route changes while its old leg is out of quota says
// nothing more over that leg: the tracker hears the new session go on as the
// same torrent, with no second `started`, and no `stopped` until the torrent
// is removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_moved_off_a_spent_leg_sends_no_stopped_over_it() {
    let (url, mut announces) = heard_tracker(&[]).await;
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("spent", 8)))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();
    let started = next_event(&mut announces).await;
    assert_eq!(started.event.as_deref(), Some("started"));
    started_accepted(&rig.service, job_id).await;

    // A move is not a start: the tracker answered `started`, so the new
    // session goes on with a plain announce.
    rig.routes.spend(0);
    rig.routes.route(TorrentScope::Default, 2);
    rig.service.follow_routes().await;
    let again = announces.recv().await.unwrap();
    assert_eq!(again.event, None);
    assert_eq!(again.identity(), started.identity());
    started_accepted(&rig.service, job_id).await;

    // The one `stopped` the tracker hears is the new session's, at the
    // remove.
    rig.service.remove(job_id, false, false).await.unwrap();
    let stopped = next_event(&mut announces).await;
    assert_eq!(stopped.event.as_deref(), Some("stopped"));
    assert_eq!(stopped.identity(), started.identity());
    rig.service.stop().await;
    while let Ok(heard) = announces.try_recv() {
        assert_eq!(heard.event, None, "the tracker heard more: {heard:?}");
    }
}

// A torrent whose leg is revoked under its session, as saving its proxy's
// profile again revokes it, moves to a session on the leg its route has now:
// the tracker hears the new session go on as the same torrent, with no second
// `started`, and no `stopped` from the old session, which the revocation
// withheld, until the torrent is removed. The old leg's sockets still reach the tracker, so a
// `stopped` the old session sent would be heard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_moved_off_a_revoked_leg_sends_no_stopped_over_it() {
    let (url, mut announces) = heard_tracker(&[]).await;
    let rig = rig(set_clock());
    let (db, network) = on_network(&rig.routes);
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("revoked", 10)))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();
    let started = next_event(&mut announces).await;
    assert_eq!(started.event.as_deref(), Some("started"));
    started_accepted(&rig.service, job_id).await;
    let before = session_of(&rig.service, job_id).await;

    save_again(&db, &network).await;
    rig.service.follow_routes().await;
    let again = announces.recv().await.unwrap();
    assert_eq!(again.event, None);
    assert_eq!(again.identity(), started.identity());
    assert!(!Arc::ptr_eq(
        &session_of(&rig.service, job_id).await,
        &before
    ));
    started_accepted(&rig.service, job_id).await;
    before.cancellation_token().cancelled().await;

    // The one `stopped` the tracker hears is the new session's, at the
    // remove.
    rig.service.remove(job_id, false, false).await.unwrap();
    let stopped = next_event(&mut announces).await;
    assert_eq!(stopped.event.as_deref(), Some("stopped"));
    assert_eq!(stopped.identity(), started.identity());
    rig.service.stop().await;
    while let Ok(heard) = announces.try_recv() {
        assert_eq!(heard.event, None, "the tracker heard more: {heard:?}");
    }
    network.shutdown().await;
}

// A torrent removed and added again is a new torrent to its trackers: a new
// key and a new peer id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_removed_and_added_again_takes_a_new_identity() {
    let (url, mut announces) = heard_tracker(&[]).await;
    let bytes = plain("again", 9);
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(bytes.clone()))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();
    let first = next_event(&mut announces).await;
    assert_eq!(first.event.as_deref(), Some("started"));
    started_accepted(&rig.service, job_id).await;
    rig.service.remove(job_id, false, false).await.unwrap();
    let stopped = next_event(&mut announces).await;
    assert_eq!(stopped.event.as_deref(), Some("stopped"));

    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(bytes))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();
    let second = next_event(&mut announces).await;
    assert_eq!(second.event.as_deref(), Some("started"));
    assert_ne!(second.key, first.key);
    assert_ne!(second.peer_id, first.peer_id);
    rig.service.stop().await;
}

// Trackers named `count` ways over three tiers, some twice, some not URLs,
// some with spaces about them: what a long edit or a big metainfo gives.
fn many_trackers(count: usize, named_by: NamedBy) -> Vec<TrackerEntry> {
    (0..count)
        .map(|index| {
            let url = match index % 7 {
                0 => format!("http://host{}.invalid/announce", index / 2),
                1 => "not a url".to_owned(),
                2 => format!("  udp://host{index}.invalid:80/announce "),
                _ => format!("http://host{index}.invalid/announce"),
            };
            TrackerEntry {
                tier: (index % 3) as u32,
                url,
                named_by,
            }
        })
        .collect()
}

// `tidy_trackers` keeps the same trackers, in the same order, as it did
// when it looked for each URL in every tracker kept before it.
#[test]
fn tidy_trackers_keeps_what_the_scan_kept() {
    fn by_scan(trackers: Vec<TrackerEntry>) -> Vec<TrackerEntry> {
        let mut tidy: Vec<TrackerEntry> = Vec::with_capacity(trackers.len());
        for entry in trackers {
            let url = entry.url.trim();
            if weaver_torrent::Url::parse(url).is_err() {
                continue;
            }
            if tidy.iter().all(|known| known.url != url) {
                tidy.push(TrackerEntry {
                    tier: entry.tier,
                    url: url.to_owned(),
                    named_by: entry.named_by,
                });
            }
        }
        tidy.sort_by_key(|entry| entry.tier);
        tidy
    }
    let mut trackers = many_trackers(3_000, NamedBy::Torrent);
    trackers.extend(many_trackers(1_000, NamedBy::Operator));
    let tidy = tidy_trackers(trackers.clone());
    assert!(tidy.len() > 2_000);
    assert_eq!(tidy, by_scan(trackers));
}

// An edit names each tracker as whoever first named it in the list it
// edits, and the operator any other, as the search of the list did.
#[test]
fn an_edit_names_trackers_as_the_search_named_them() {
    use crate::torrents::service::trackers::named_as_listed;
    let mut listed = many_trackers(2_000, NamedBy::Torrent);
    listed.extend(many_trackers(500, NamedBy::Operator));
    let edited = tidy_trackers(many_trackers(3_000, NamedBy::Operator));
    let by_search: Vec<TrackerEntry> = edited
        .clone()
        .into_iter()
        .map(|tracker| TrackerEntry {
            named_by: listed
                .iter()
                .find(|known| known.url == tracker.url)
                .map_or(NamedBy::Operator, |known| known.named_by),
            ..tracker
        })
        .collect();
    let named = named_as_listed(&listed, edited);
    assert!(
        named
            .iter()
            .any(|tracker| tracker.named_by == NamedBy::Torrent)
    );
    assert!(
        named
            .iter()
            .any(|tracker| tracker.named_by == NamedBy::Operator)
    );
    assert_eq!(named, by_search);
}

// A removal of many URLs leaves the trackers the search of the URLs left.
#[test]
fn a_removal_leaves_what_the_search_left() {
    use crate::torrents::service::trackers::without_urls;
    let trackers = tidy_trackers(many_trackers(3_000, NamedBy::Torrent));
    let urls: Vec<String> = trackers
        .iter()
        .step_by(3)
        .map(|tracker| format!(" {} ", tracker.url))
        .chain(["http://absent.invalid/announce".to_owned()])
        .collect();
    let mut by_search = trackers.clone();
    by_search.retain(|tracker| !urls.iter().any(|url| url.trim() == tracker.url));
    let left = without_urls(trackers.clone(), &urls);
    assert!(!left.is_empty() && left.len() < trackers.len());
    assert_eq!(left, by_search);
}

// A private tracker's URL is sealed at rest, and a restart opens it: the
// torrent comes back with its tracker, and its announce carries the URL as
// the torrent was added with it, passkey and all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_torrent_announces_its_sealed_tracker_url() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/announce?passkey=f00dfacecafe",
        listener.local_addr().unwrap()
    );
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(announcing(&plain("sealed", 7), &url)))
        })
        .await
        .unwrap();
    // The operator names it, as a loopback tracker the torrent names alone
    // is not announced to; a loopback one is asked only at /announce, so
    // its passkey rides in the query.
    tracked(&rig.service, job_id, &url).await;
    let before = listed_trackers(&rig.service, job_id).await;
    assert!(before.iter().any(|(_, listed, _)| *listed == url));
    rig.service.stop().await;
    let datastore = rig.db.datastore();
    let stored = rig
        .db
        .run_sql_blocking_read(async move {
            crate::persistence::sql_runtime::SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT url FROM torrent_trackers",
                &[],
            )
            .await?
            .iter()
            .map(|row| row.text("url"))
            .collect::<Result<Vec<_>, _>>()
        })
        .unwrap();
    assert_eq!(stored.len(), before.len());
    assert!(stored.iter().all(|url| !url.contains("f00dfacecafe")));

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(listed_trackers(&again, job_id).await, before);
    again.resume(job_id).await.unwrap();
    let (stream, heard) = heard_request(&listener).await;
    let line = heard.lines().next().unwrap();
    assert!(line.starts_with("GET /announce?"), "{line}");
    assert!(line.contains("&passkey=f00dfacecafe "), "{line}");
    drop(stream);
    again.stop().await;
}

// A torrent whose stored secrets do not decrypt with the instance key, as
// after a key that was replaced, is held at start with why: it stays in
// the table, unrestored and unfetched, and fails nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_whose_secrets_do_not_decrypt_is_held_with_why() {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(announcing(
            &plain("other key", 3),
            "http://a.invalid/f00dfacecafe/announce",
        ))))
        .await
        .unwrap();
    rig.service.stop().await;
    let other = crate::persistence::encryption::EncryptionKey::generate();
    let foreign = crate::torrents::sealed::seal_metainfo(Some(&other), b"d4:infodee").unwrap();
    let datastore = rig.db.datastore();
    rig.db
        .run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET metainfo = {} WHERE job_id = {}",
                &[SqlArg::Bytes(foreign), SqlArg::I64(job_id.0 as i64)],
            )
            .await
        })
        .unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let written = again.written();
    reopen(&again).await;
    {
        let live = again.inner.live.lock().await;
        let torrent = &live[&job_id];
        assert!(
            torrent
                .restore_held
                .as_deref()
                .is_some_and(|why| why.contains("do not decrypt")),
            "{:?}",
            torrent.restore_held
        );
        assert!(torrent.store.is_none() && torrent.layout.is_none());
        assert!(torrent.fetching.is_none() && torrent.resolving.is_none());
    }
    assert_eq!(written.borrow().checkpoints_read, 0);
    assert_eq!(
        again.torrent(job_id).await.unwrap().state,
        TorrentState::Stopped
    );
    assert!(rig.db.get_torrent(job_id).unwrap().is_some());
    // Its trackers are not changed while it is held: an edit would write
    // the list back without whatever did not open.
    let kept = rig.db.list_torrent_trackers(job_id).unwrap();
    let refused = again
        .add_trackers(job_id, vec![vec!["http://b.invalid/announce".into()]])
        .await
        .unwrap_err();
    assert!(
        matches!(&refused, ActionError::Invalid(why) if why.contains("do not decrypt")),
        "{refused:?}"
    );
    assert_eq!(rig.db.list_torrent_trackers(job_id).unwrap(), kept);
    again.stop().await;
}
