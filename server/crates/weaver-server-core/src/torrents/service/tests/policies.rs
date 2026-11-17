// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// libtorrent `torrent::set_upload_limit` / `set_download_limit` throttle the
// torrent's own channels in place, and qBittorrent's `TorrentImpl` keeps
// them in its resume data: the limits reach the running torrent's session
// at once, are what it reads back, and are handed to its session again
// after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrents_rate_limits_reach_its_session_and_outlive_a_restart() {
    use weaver_torrent::limits::{LimitKind, LimitsConfig};

    let bps = |bps: u32| LimitKind::Bps(std::num::NonZeroU32::new(bps).unwrap());
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
    let handle = handle_of(&rig.service, job_id).await;
    assert_eq!(handle.ratelimits(), LimitsConfig::default());
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!((view.upload_limit, view.download_limit), (None, None));

    // Zero holds the download closed.
    rig.service
        .set_rate_limits(job_id, Some(Some(65_536)), Some(Some(0)))
        .await
        .unwrap();
    let limited = LimitsConfig {
        upload_bps: bps(65_536),
        download_bps: LimitKind::Closed,
    };
    assert_eq!(handle.ratelimits(), limited);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.upload_limit, view.download_limit),
        (Some(65_536), Some(0))
    );
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(
        (stored.upload_limit, stored.download_limit),
        (Some(65_536), Some(0))
    );
    assert!(matches!(
        rig.service
            .set_rate_limits(JobId(999), Some(None), None)
            .await,
        Err(ActionError::NotFound(_))
    ));
    // A limit past what the row holds is refused before the torrent or
    // its row takes it.
    assert!(matches!(
        rig.service
            .set_rate_limits(job_id, Some(Some(u64::MAX)), Some(None))
            .await,
        Err(ActionError::Invalid(_))
    ));
    assert_eq!(handle.ratelimits(), limited);
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(
        (stored.upload_limit, stored.download_limit),
        (Some(65_536), Some(0))
    );
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(handle_of(&again, job_id).await.ratelimits(), limited);
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.upload_limit, view.download_limit),
        (Some(65_536), Some(0))
    );
    again.stop().await;
}

// The upload cap its levels give a torrent holds it beside its own limit,
// as a second channel over its peers would: the tighter of the two is in
// force, and a change of either reaches the session without a restart.
// The torrent reads back its own limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_tighter_of_a_torrents_limit_and_its_upload_cap_holds() {
    use weaver_torrent::limits::LimitKind;

    let bps = |bps: u32| LimitKind::Bps(std::num::NonZeroU32::new(bps).unwrap());
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
    let handle = handle_of(&rig.service, job_id).await;
    rig.service
        .set_rate_limits(job_id, Some(Some(65_536)), None)
        .await
        .unwrap();
    let capped = |cap: u64| {
        let mut settings = rig.service.settings();
        settings.upload_cap = Some(cap);
        settings
    };
    rig.service.set_settings(capped(1_000)).await;
    rig.service.tick().await;
    assert_eq!(handle.ratelimits().upload_bps, bps(1_000));
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().upload_limit,
        Some(65_536)
    );

    rig.service.set_settings(capped(1_000_000)).await;
    rig.service.tick().await;
    assert_eq!(handle.ratelimits().upload_bps, bps(65_536));

    rig.service
        .set_rate_limits(job_id, Some(None), None)
        .await
        .unwrap();
    assert_eq!(handle.ratelimits().upload_bps, bps(1_000_000));
    rig.service.stop().await;
}

// A tracker rule's terms reach its torrents: one admitted under it starts
// forced, looks for peers on none of the DHT, local discovery and peer
// exchange while staying public, makes its connections under
// the rule's encryption and uploads under the rule's limit in place of the
// global seeding cap. A change of the rule reaches the running torrent at
// the next tick, through its session again; a torrent under no rule keeps
// the global terms throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rules_privacy_terms_reach_its_torrents() {
    use crate::settings::{TorrentEncryption, TorrentPeerProtocol};
    use weaver_torrent::limits::LimitKind;
    use weaver_torrent::{DialOrder, EncryptionPolicy};

    let bps = |bps: u32| LimitKind::Bps(std::num::NonZeroU32::new(bps).unwrap());
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let mut settings = rig.service.settings();
    settings.upload_cap = Some(2_000);
    rig.service.set_settings(settings).await;
    let rule = rig
        .db
        .insert_tracker_rule(
            &TrackerRule {
                peer_protocol: Some(TorrentPeerProtocol::UtpFirst),
                archive_passwords: Default::default(),
                bound: None,
                id: 0,
                host_pattern: "*.example-tracker.test".into(),
                seeding: None,
                never_pause: false,
                unpack: None,
                client: None,
                torrent_route: false,
                dht: Some(false),
                pex: Some(false),
                lsd: Some(false),
                encryption: Some(TorrentEncryption::Prefer),
                upload_cap: Leaf::Off,
                forced: true,
                ssl: None,
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    // Paused, so nothing announces.
    let paused = |metainfo| AddRequest {
        paused: true,
        ..request(AddSource::Metainfo(metainfo))
    };
    let ruled = rig
        .service
        .add_opened(paused(announcing(
            &plain("ruled", 1),
            "http://tracker.example-tracker.test/announce",
        )))
        .await
        .unwrap();
    let unruled = rig
        .service
        .add_opened(paused(plain("unruled", 2)))
        .await
        .unwrap();

    assert!(rig.service.torrent(ruled).await.unwrap().forced);
    assert!(rig.db.get_torrent(ruled).unwrap().unwrap().forced);
    assert!(!rig.service.torrent(unruled).await.unwrap().forced);
    let handle = handle_of(&rig.service, ruled).await;
    assert!(!handle.is_private());
    assert_eq!(
        handle.discovery(),
        weaver_torrent::Discovery {
            dht: false,
            lsd: false
        }
    );
    assert!(!handle.speaks_pex());
    assert_eq!(handle.encryption(), Some(EncryptionPolicy::Prefer));
    assert_eq!(handle.dial_order(), Some(DialOrder::UtpFirst));
    assert_eq!(handle.ratelimits().upload_bps, LimitKind::Unlimited);
    let other = handle_of(&rig.service, unruled).await;
    assert!(other.discovery().dht && other.discovery().lsd && other.speaks_pex());
    assert_eq!(other.encryption(), None);
    assert_eq!(other.dial_order(), None);
    assert_eq!(other.ratelimits().upload_bps, bps(2_000));

    rig.db
        .update_tracker_rule(
            &TrackerRule {
                dht: None,
                pex: None,
                lsd: None,
                encryption: Some(TorrentEncryption::Require),
                peer_protocol: Some(TorrentPeerProtocol::UtpOnly),
                upload_cap: Leaf::Set(1_000),
                ..rule.clone()
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    rig.service.tick().await;
    let again = handle_of(&rig.service, ruled).await;
    assert!(!Arc::ptr_eq(&handle, &again));
    assert!(again.discovery().dht && again.discovery().lsd && again.speaks_pex());
    assert_eq!(again.encryption(), Some(EncryptionPolicy::Require));
    assert_eq!(again.dial_order(), Some(DialOrder::UtpOnly));
    assert_eq!(again.ratelimits().upload_bps, bps(1_000));
    assert!(Arc::ptr_eq(&other, &handle_of(&rig.service, unruled).await));
    assert_eq!(other.ratelimits().upload_bps, bps(2_000));

    // A rule may turn encryption off for its torrents, and a protocol it
    // stops setting goes back to the session's.
    rig.db
        .update_tracker_rule(
            &TrackerRule {
                encryption: Some(TorrentEncryption::Disabled),
                peer_protocol: None,
                ..rule
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    rig.service.tick().await;
    let off = handle_of(&rig.service, ruled).await;
    assert_eq!(off.encryption(), Some(EncryptionPolicy::Disabled));
    assert_eq!(off.dial_order(), None);
    rig.service.stop().await;
}

// Each of a tracker rule's DHT, peer exchange and local discovery switches
// reaches its torrents on its own: one turned off holds them off that one
// source, one unset leaves it on, and the torrent stays public. A change reaches the running torrent at the next
// tick, through its session again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rules_dht_pex_and_lsd_each_reach_its_torrents() {
    use weaver_torrent::Discovery;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let rule = rig
        .db
        .insert_tracker_rule(
            &TrackerRule {
                host_pattern: "*.example-tracker.test".into(),
                dht: Some(false),
                pex: Some(false),
                lsd: None,
                ..TrackerRule::default()
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    let ruled = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(announcing(
                &plain("ruled", 1),
                "http://tracker.example-tracker.test/announce",
            )))
        })
        .await
        .unwrap();

    let handle = handle_of(&rig.service, ruled).await;
    assert!(!handle.is_private());
    assert_eq!(
        handle.discovery(),
        Discovery {
            dht: false,
            lsd: true
        }
    );
    assert!(!handle.speaks_pex());

    rig.db
        .update_tracker_rule(
            &TrackerRule {
                dht: None,
                pex: Some(true),
                lsd: Some(false),
                ..rule
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    rig.service.tick().await;
    let again = handle_of(&rig.service, ruled).await;
    assert!(!Arc::ptr_eq(&handle, &again));
    assert!(!again.is_private());
    assert_eq!(
        again.discovery(),
        Discovery {
            dht: true,
            lsd: false
        }
    );
    assert!(again.speaks_pex());
    rig.service.stop().await;
}

// The stored levels reach the session through the one reconcile, an
// ignored file stays ignored, and a toggle the session runs against what is
// stored is put back on the next pass.
#[tokio::test(start_paused = true)]
async fn the_stored_levels_and_toggles_reach_the_session_through_one_reconcile() {
    use FilePriority::*;
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let bytes = metainfo(
        "trio",
        // Whole pieces each, so a raise of one file raises no piece of another.
        &[
            ("trio/a.bin", &[1; 3072]),
            ("trio/b.bin", &[2; 3072]),
            ("trio/c.bin", &[3; 3072]),
        ],
        1024,
        false,
    );
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let handle = handle_of(&rig.service, job_id).await;
    handle.wait_until_initialized().await.unwrap();
    rig.service
        .set_file_priorities(job_id, &[(0, High), (2, Ignored)])
        .await
        .unwrap();
    assert_eq!(handle.file_priorities(), [High, Normal, Ignored]);

    rig.service.apply_picking().await;
    assert_eq!(handle.file_priorities(), [High, Normal, Ignored]);
    let stored = rig.db.torrent_file_priorities(job_id).unwrap();
    assert_eq!(stored, [High, Normal, Ignored]);

    rig.service
        .set_first_last_piece_first(job_id, true)
        .await
        .unwrap();
    handle.set_sequential(true);
    handle.set_first_last_piece_first(false).unwrap();
    rig.service.apply_picking().await;
    assert!(!handle.sequential());
    assert!(handle.first_last_piece_first());
    rig.service.stop().await;
}

// A file turned off is not wanted: the queue's row wants only the other
// files' bytes, and what is still to come of them, not the whole torrent.
#[tokio::test(start_paused = true)]
async fn a_file_turned_off_counts_in_neither_the_wanted_nor_the_remaining_bytes() {
    use FilePriority::*;
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let bytes = metainfo(
        "trio",
        &[
            ("trio/a.bin", &[1; 3072]),
            ("trio/b.bin", &[2; 3072]),
            ("trio/c.bin", &[3; 3072]),
        ],
        1024,
        false,
    );
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(bytes))
        })
        .await
        .unwrap();
    rig.service
        .set_file_priorities(job_id, &[(2, Ignored)])
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.total_bytes, view.wanted_bytes, view.wanted_have_bytes),
        (3 * 3072, 2 * 3072, 0)
    );
    let info = report(view.clone(), job_status(&view), EPOCH_MS).job_info();
    assert_eq!(
        (info.total_bytes, info.wanted_bytes, info.remaining_bytes),
        (3 * 3072, 2 * 3072, 2 * 3072)
    );
    rig.service.stop().await;
}

// The two picking toggles set at once both hold: each call writes only its
// own, so neither puts back the other's old value, in the database or in
// memory.
#[tokio::test(start_paused = true)]
async fn the_two_picking_toggles_set_at_once_both_hold() {
    use std::future::Future;
    use std::task::Poll;
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let bytes = metainfo("pair", &[("pair/a.bin", &[1; 3000])], 1024, false);
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let handle = handle_of(&rig.service, job_id).await;
    handle.wait_until_initialized().await.unwrap();

    // Both calls queue on the live lock before either runs, so each reads
    // the torrent before the other has changed it.
    let live = rig.service.inner.live.lock().await;
    let mut both = std::pin::pin!(async {
        tokio::join!(
            rig.service.set_sequential(job_id, true),
            rig.service.set_first_last_piece_first(job_id, true),
        )
    });
    std::future::poll_fn(|cx| {
        assert!(both.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(live);
    let (sequential, first_last) = both.await;
    sequential.unwrap();
    first_last.unwrap();

    let record = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert!(record.sequential && record.first_last_piece_first);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert!(view.sequential && view.first_last_piece_first);
    assert!(handle.sequential() && handle.first_last_piece_first());
    rig.service.stop().await;
}

// A level change and the two picking toggles are stored, reach the
// torrent's session at once, and come back with it after a restart.
#[tokio::test(start_paused = true)]
async fn file_levels_and_picking_toggles_reach_the_session_and_outlive_a_restart() {
    use FilePriority::*;
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let bytes = metainfo(
        "pair",
        &[("pair/a.bin", &[1; 3000]), ("pair/b.bin", &[2; 3000])],
        1024,
        false,
    );
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let handle = handle_of(&rig.service, job_id).await;
    handle.wait_until_initialized().await.unwrap();
    assert_eq!(handle.file_priorities(), [Normal, Normal]);

    rig.service
        .set_file_priorities(job_id, &[(0, Ignored), (1, High)])
        .await
        .unwrap();
    assert!(
        rig.service
            .set_file_priorities(job_id, &[(2, High)])
            .await
            .is_err()
    );
    let stored = rig.db.torrent_file_priorities(job_id).unwrap();
    assert_eq!(stored, [Ignored, High]);
    assert_eq!(handle.file_priorities(), [Ignored, High]);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        view.files
            .iter()
            .map(|file| file.selected())
            .collect::<Vec<_>>(),
        [false, true]
    );

    rig.service.set_sequential(job_id, true).await.unwrap();
    rig.service
        .set_first_last_piece_first(job_id, true)
        .await
        .unwrap();
    assert!(handle.sequential() && handle.first_last_piece_first());
    let view = rig.service.torrent(job_id).await.unwrap();
    assert!(view.sequential && view.first_last_piece_first);
    rig.service.set_sequential(job_id, false).await.unwrap();
    assert!(!handle.sequential());
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    let handle = handle_of(&again, job_id).await;
    assert_eq!(handle.file_priorities(), [Ignored, High]);
    assert!(!handle.sequential());
    assert!(handle.first_last_piece_first());
    let view = again.torrent(job_id).await.unwrap();
    assert!(!view.sequential && view.first_last_piece_first);
    again.stop().await;
}

// Downloading again a file whose share of a piece the side store keeps
// copies that share out as a job of its own: the call does not wait for it,
// the levels change only once it is done, and a level asked meanwhile is
// taken after it. The copy puts the torrent's own bytes in the file, and
// the side store goes when the torrent does (libtorrent
// `torrent::prioritize_files`, `on_file_priority`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_level_asked_while_a_copy_is_out_is_taken_after_it() {
    use FilePriority::*;
    let rig = rig(set_clock() as Arc<dyn Clock>);
    rig.service.open().await.unwrap();
    let bytes: Vec<u8> = (0..3584u32).map(|at| (at * 7 % 251) as u8).collect();
    let meta = metainfo(
        "side",
        &[
            ("side/a.bin", &bytes[..1536]),
            ("side/b.bin", &bytes[1536..]),
        ],
        1024,
        false,
    );
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(meta))
        })
        .await
        .unwrap();
    rig.service
        .set_file_priorities(job_id, &[(1, Ignored)])
        .await
        .unwrap();
    let store = rig.service.inner.live.lock().await[&job_id]
        .store
        .clone()
        .unwrap();
    // Piece 1 is a's last 512 bytes and b's first 512.
    Pieces::verified(
        &*store,
        1,
        &bytes::Bytes::copy_from_slice(&bytes[1024..2048]),
    )
    .unwrap();
    let b = store.files().path(1).unwrap();
    assert!(!b.exists());
    let side = store.side_path().unwrap();
    assert!(side.exists());

    // Reads held on a thread of their own, as a piece's write under way
    // holds them, until the second change is asked.
    let (held_tx, held) = std::sync::mpsc::channel();
    let (release, release_rx) = std::sync::mpsc::channel::<()>();
    let holding = Arc::clone(&store);
    let holder = std::thread::spawn(move || {
        let _reads = holding.hold_reads();
        held_tx.send(()).unwrap();
        let _ = release_rx.recv();
    });
    held.recv().unwrap();
    rig.service
        .set_file_priorities(job_id, &[(1, Normal)])
        .await
        .unwrap();
    rig.service
        .set_file_priorities(job_id, &[(0, High)])
        .await
        .unwrap();
    let task = {
        let mut live = rig.service.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).unwrap();
        assert_eq!(torrent.levels, [Normal, Ignored]);
        let leveling = torrent.leveling.as_mut().unwrap();
        assert_eq!(leveling.next.as_deref(), Some(&[High, Normal][..]));
        leveling.task.take().unwrap()
    };
    assert_eq!(
        rig.db.torrent_file_priorities(job_id).unwrap(),
        [Normal, Ignored]
    );
    assert!(!b.exists());
    release.send(()).unwrap();
    holder.join().unwrap();
    task.await.unwrap();

    {
        let live = rig.service.inner.live.lock().await;
        let torrent = &live[&job_id];
        assert_eq!(torrent.levels, [High, Normal]);
        assert!(torrent.leveling.is_none());
    }
    // The levels are stored with the default ones after the last left out.
    let mut stored = rig.db.torrent_file_priorities(job_id).unwrap();
    stored.resize(2, Normal);
    assert_eq!(stored, [High, Normal]);
    assert_eq!(std::fs::read(&b).unwrap(), bytes[1536..2048]);
    assert!(store.side_pieces().is_empty());
    rig.service.remove(job_id, false, true).await.unwrap();
    assert!(!side.exists());
    rig.service.stop().await;
}
