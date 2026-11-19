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
    rig.service.restored().await;
    assert_eq!(handle.ratelimits().upload_bps, bps(1_000));
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().upload_limit,
        Some(65_536)
    );

    rig.service.set_settings(capped(1_000_000)).await;
    rig.service.tick().await;
    rig.service.restored().await;
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
    use crate::settings::{PeerTransport, TorrentEncryption, TransportRule};
    use weaver_torrent::limits::LimitKind;
    use weaver_torrent::{DialOrder, DialOrders, EncryptionPolicy};

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
                transports: TransportRule {
                    prefer_downloading: Some(PeerTransport::Utp),
                    ..TransportRule::default()
                },
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
    // uTP first while downloading, from the rule, and while seeding, from
    // the global setting.
    assert_eq!(
        handle.dial_orders(),
        Some(DialOrders::both(DialOrder::UtpFirst))
    );
    assert_eq!(handle.ratelimits().upload_bps, LimitKind::Unlimited);
    let other = handle_of(&rig.service, unruled).await;
    assert!(other.discovery().dht && other.discovery().lsd && other.speaks_pex());
    assert_eq!(other.encryption(), None);
    assert_eq!(other.dial_orders(), None);
    assert_eq!(other.ratelimits().upload_bps, bps(2_000));

    rig.db
        .update_tracker_rule(
            &TrackerRule {
                dht: None,
                pex: None,
                lsd: None,
                encryption: Some(TorrentEncryption::Require),
                transports: TransportRule {
                    tcp: Some(false),
                    ..TransportRule::default()
                },
                upload_cap: Leaf::Set(1_000),
                ..rule.clone()
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    rig.service.tick().await;
    rig.service.restored().await;
    let again = handle_of(&rig.service, ruled).await;
    assert!(!Arc::ptr_eq(&handle, &again));
    assert!(again.discovery().dht && again.discovery().lsd && again.speaks_pex());
    assert_eq!(again.encryption(), Some(EncryptionPolicy::Require));
    assert_eq!(
        again.dial_orders(),
        Some(DialOrders::both(DialOrder::UtpOnly))
    );
    assert_eq!(again.ratelimits().upload_bps, bps(1_000));
    assert!(Arc::ptr_eq(&other, &handle_of(&rig.service, unruled).await));
    assert_eq!(other.ratelimits().upload_bps, bps(2_000));

    // A rule may turn encryption off for its torrents, and transports it
    // stops setting go back to the session's.
    rig.db
        .update_tracker_rule(
            &TrackerRule {
                encryption: Some(TorrentEncryption::Disabled),
                transports: Default::default(),
                ..rule
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    rig.service.tick().await;
    let off = handle_of(&rig.service, ruled).await;
    assert_eq!(off.encryption(), Some(EncryptionPolicy::Disabled));
    assert_eq!(off.dial_orders(), None);
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
    rig.service.restored().await;
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
    // The change under way is stored before its copy, the one asked
    // meanwhile not yet; a store opened before the copy lands copies the
    // bytes out first (`TorrentStore::with_side`).
    let mut stored = rig.db.torrent_file_priorities(job_id).unwrap();
    stored.resize(2, Normal);
    assert_eq!(stored, [Normal, Normal]);
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

// A database that refuses every write of a torrent's file levels, while
// `on`, as a full or broken disk would.
fn refuse_level_writes(db: &Database, on: bool) {
    use crate::persistence::sql_runtime::SqlRuntime;
    let sql = if on {
        "CREATE TRIGGER refuse_levels BEFORE UPDATE OF file_priorities ON torrents \
         BEGIN SELECT RAISE(ABORT, 'refused'); END"
    } else {
        "DROP TRIGGER refuse_levels"
    };
    let store = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&store, "refuse_level_writes_fixture", |tx| {
            Box::pin(async move {
                tx.execute(sql, &[]).await?;
                Ok(())
            })
        })
        .await
    })
    .unwrap();
}

// What a complete torrent is, in memory and as stored: when it completed,
// its levels, its place in the download queue, and the next place a torrent
// takes.
async fn completion_of(
    rig: &Rig,
    job_id: JobId,
) -> (Option<i64>, Option<i64>, Vec<FilePriority>, i64, i64, i64) {
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    let live = rig.service.inner.live.lock().await;
    let torrent = &live[&job_id];
    // The levels are stored with the default ones after the last left out.
    let mut levels = rig.db.torrent_file_priorities(job_id).unwrap();
    levels.resize(torrent.levels.len(), FilePriority::Normal);
    assert_eq!(levels, torrent.levels);
    (
        torrent.record.completed_at_ms,
        stored.completed_at_ms,
        torrent.levels.clone(),
        torrent.record.queue_position,
        stored.queue_position,
        rig.service
            .inner
            .next_queue_position
            .load(std::sync::atomic::Ordering::SeqCst),
    )
}

// What an API caller is shown of a torrent: its state, job status, when it
// completed and the bytes it wants.
async fn shown_of(
    rig: &Rig,
    job_id: JobId,
) -> (TorrentState, crate::jobs::JobStatus, Option<i64>, u64) {
    let view = rig.service.torrent(job_id).await.unwrap();
    (
        view.state,
        crate::torrents::service::views::job_status(&view),
        view.completed_at_ms,
        view.wanted_bytes,
    )
}

// A complete torrent's file wanted again whose levels cannot be stored
// changes nothing: the call fails, and the torrent stays complete with its
// levels, its place and the queue's next place as they were, so no second
// finish can run (`completed` returns for a complete torrent). The same
// change stored afterwards reopens it once, at the end of the queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reopen_that_cannot_be_stored_changes_nothing() {
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
    let mut settles = rig.service.inner.settled.subscribe();
    rig.service
        .set_file_priorities(job_id, &[(0, Ignored)])
        .await
        .unwrap();
    settles
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
    let before = completion_of(&rig, job_id).await;
    assert_eq!(before.0, Some(EPOCH_MS));
    assert_eq!(before.2, [Ignored]);
    let shown = shown_of(&rig, job_id).await;

    clock.shift_unix_ms(5_000);
    refuse_level_writes(&rig.db, true);
    assert!(
        rig.service
            .set_file_priorities(job_id, &[(0, Normal)])
            .await
            .is_err()
    );
    assert_eq!(completion_of(&rig, job_id).await, before);
    assert_eq!(shown_of(&rig, job_id).await, shown);

    refuse_level_writes(&rig.db, false);
    rig.service
        .set_file_priorities(job_id, &[(0, Normal)])
        .await
        .unwrap();
    let after = completion_of(&rig, job_id).await;
    assert_eq!((after.0, after.1, after.2), (None, None, vec![Normal]));
    assert_eq!((after.3, after.4), (before.5, before.5));
    assert_eq!(after.5, before.5 + 1);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_ne!(view.state, TorrentState::Seeding);
    rig.service.stop().await;
}

// The same with a copy out of the side store: the levels are stored before
// the copy, so one that cannot be stored fails the call, starts no copy and
// leaves the bytes, the levels and the torrent's completion where they were.
// Stored afterwards, the copy runs and the torrent is reopened once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reopen_with_a_copy_that_cannot_be_stored_changes_nothing() {
    use FilePriority::*;
    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
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
    // a.bin is whole: piece 0, and piece 1, which is a's last 512 bytes and
    // b's first 512, kept in the side store.
    for piece in [0u32, 1] {
        let at = piece as usize * 1024;
        Pieces::verified(
            &*store,
            piece,
            &bytes::Bytes::copy_from_slice(&bytes[at..at + 1024]),
        )
        .unwrap();
    }
    let mut settles = rig.service.inner.settled.subscribe();
    rig.service.inner.signal.look(job_id);
    settles
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
    let b = store.files().path(1).unwrap();
    assert!(!b.exists());
    assert_eq!(store.side_pieces(), [1]);
    let before = completion_of(&rig, job_id).await;
    assert_eq!(before.2, [Normal, Ignored]);
    let shown = shown_of(&rig, job_id).await;

    clock.shift_unix_ms(5_000);
    refuse_level_writes(&rig.db, true);
    assert!(
        rig.service
            .set_file_priorities(job_id, &[(1, Normal)])
            .await
            .is_err()
    );
    assert!(
        rig.service.inner.live.lock().await[&job_id]
            .leveling
            .is_none()
    );
    assert_eq!(completion_of(&rig, job_id).await, before);
    assert!(!b.exists());
    assert_eq!(store.side_pieces(), [1]);
    assert_eq!(shown_of(&rig, job_id).await, shown);

    refuse_level_writes(&rig.db, false);
    rig.service
        .set_file_priorities(job_id, &[(1, Normal)])
        .await
        .unwrap();
    let task = rig
        .service
        .inner
        .live
        .lock()
        .await
        .get_mut(&job_id)
        .unwrap()
        .leveling
        .as_mut()
        .and_then(|leveling| leveling.task.take());
    if let Some(task) = task {
        task.await.unwrap();
    }
    assert!(
        rig.service.inner.live.lock().await[&job_id]
            .leveling
            .is_none()
    );
    let after = completion_of(&rig, job_id).await;
    assert_eq!(
        (after.0, after.1, after.2),
        (None, None, vec![Normal, Normal])
    );
    assert_eq!((after.3, after.4), (before.5, before.5));
    assert_eq!(after.5, before.5 + 1);
    assert_eq!(std::fs::read(&b).unwrap(), bytes[1536..2048]);
    assert!(store.side_pieces().is_empty());
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_ne!(view.state, TorrentState::Seeding);
    rig.service.stop().await;
}

// A paused torrent of two files that share no piece, complete with its
// second set aside, and a second torrent added after it: the first, the
// second, and the second file's bytes.
async fn complete_with_one_aside(rig: &Rig) -> (JobId, JobId, Vec<u8>) {
    use FilePriority::*;
    rig.service.open().await.unwrap();
    let bytes: Vec<u8> = (0..4096u32).map(|at| (at * 7 % 251) as u8).collect();
    let meta = metainfo(
        "two",
        &[("two/a.bin", &bytes[..2048]), ("two/b.bin", &bytes[2048..])],
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
    for piece in [0u32, 1] {
        let at = piece as usize * 1024;
        Pieces::verified(
            &*store,
            piece,
            &bytes::Bytes::copy_from_slice(&bytes[at..at + 1024]),
        )
        .unwrap();
    }
    let mut settles = rig.service.inner.settled.subscribe();
    rig.service.inner.signal.look(job_id);
    settles
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
    let after = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("after", 9)))
        })
        .await
        .unwrap();
    rig.service.save_checkpoints([job_id].into()).await;
    (job_id, after, bytes[2048..].to_vec())
}

// A torrent's completion and queue place, in memory and as stored.
async fn place_of(
    service: &TorrentService,
    db: &Database,
    job_id: JobId,
) -> (Option<i64>, Option<i64>, i64, i64) {
    let stored = db.get_torrent(job_id).unwrap().unwrap();
    let live = service.inner.live.lock().await;
    let torrent = &live[&job_id];
    (
        torrent.record.completed_at_ms,
        stored.completed_at_ms,
        torrent.record.queue_position,
        stored.queue_position,
    )
}

// The second file's pieces arrive in the restored torrent's store, and it
// completes again.
async fn completes_again(service: &TorrentService, db: &Database, job_id: JobId, b: &[u8]) {
    let store = service.inner.live.lock().await[&job_id]
        .store
        .clone()
        .unwrap();
    for piece in [2u32, 3] {
        let at = (piece as usize - 2) * 1024;
        Pieces::verified(
            &*store,
            piece,
            &bytes::Bytes::copy_from_slice(&b[at..at + 1024]),
        )
        .unwrap();
    }
    let mut settles = service.inner.settled.subscribe();
    service.inner.signal.look(job_id);
    settles
        .wait_for(|_| {
            db.get_torrent(job_id)
                .unwrap()
                .unwrap()
                .completed_at_ms
                .is_some()
        })
        .await
        .unwrap();
    assert!(service.inner.live.lock().await[&job_id].complete());
    // Kept with the pieces, as a stop after the session's word keeps them.
    service.save_checkpoints([job_id].into()).await;
}

// A file of a complete torrent wanted again after a restart, before the
// torrent's store is open (the tick begins that), reopens it all the same:
// once, at the end of the queue, and it completes again once the file's
// pieces arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_wanted_again_before_the_store_opens_reopens_the_torrent() {
    use FilePriority::*;
    let rig = rig(set_clock() as Arc<dyn Clock>);
    let (job_id, after, b) = complete_with_one_aside(&rig).await;
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    let (completed, stored, _, _) = place_of(&again, &rig.db, job_id).await;
    assert!(completed.is_some() && stored.is_some());
    assert!(again.inner.live.lock().await[&job_id].store.is_none());
    let next = again
        .inner
        .next_queue_position
        .load(std::sync::atomic::Ordering::SeqCst);
    again
        .set_file_priorities(job_id, &[(1, Normal)])
        .await
        .unwrap();
    assert!(again.inner.live.lock().await[&job_id].store.is_none());
    assert_eq!(
        place_of(&again, &rig.db, job_id).await,
        (None, None, next, next)
    );
    assert!(next > place_of(&again, &rig.db, after).await.3);

    again.tick().await;
    again.restored().await;
    // Once: the store's open takes the stored levels and moves nothing.
    assert_eq!(
        place_of(&again, &rig.db, job_id).await,
        (None, None, next, next)
    );
    assert_eq!(
        again.inner.live.lock().await[&job_id].levels,
        [Normal, Normal]
    );
    completes_again(&again, &rig.db, job_id, &b).await;
    again.stop().await;
}

// The same asked while the stored torrents are still being read: the
// torrent goes past every stored one at the load's end, as one added then.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_wanted_again_while_the_load_runs_reopens_the_torrent_last() {
    use FilePriority::*;
    let rig = rig(set_clock() as Arc<dyn Clock>);
    let (job_id, after, b) = complete_with_one_aside(&rig).await;
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.hold_load(true);
    again.start_seated().await.unwrap();
    again
        .set_file_priorities(job_id, &[(1, Normal)])
        .await
        .unwrap();
    assert!(again.inner.live.lock().await[&job_id].store.is_none());
    let (completed, stored, _, _) = place_of(&again, &rig.db, job_id).await;
    assert_eq!((completed, stored), (None, None));
    again.hold_load(false);
    again.loaded().await;
    let reopened = place_of(&again, &rig.db, job_id).await;
    let last = place_of(&again, &rig.db, after).await;
    assert_eq!(reopened.2, reopened.3);
    assert!(reopened.2 > last.2 && reopened.3 > last.3);

    again.tick().await;
    again.restored().await;
    assert_eq!(place_of(&again, &rig.db, job_id).await, reopened);
    completes_again(&again, &rig.db, job_id, &b).await;
    again.stop().await;
}

// A torrent stored complete with levels that want a file it lacks, as a
// change of levels whose copy landed leaves it when its reopen could not be
// stored, is restored short of complete, past every stored torrent, and
// completes again once the file's pieces arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_stored_complete_that_wants_a_missing_file_is_reopened_at_restore() {
    use FilePriority::*;
    let rig = rig(set_clock() as Arc<dyn Clock>);
    let (job_id, after, b) = complete_with_one_aside(&rig).await;
    rig.service.stop().await;
    // The levels landed; the completion and the place did not.
    rig.db
        .set_torrent_file_priorities(job_id, &[Normal, Normal])
        .unwrap();
    assert!(
        rig.db
            .get_torrent(job_id)
            .unwrap()
            .unwrap()
            .completed_at_ms
            .is_some()
    );

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    let reopened = place_of(&again, &rig.db, job_id).await;
    let last = place_of(&again, &rig.db, after).await;
    assert_eq!((reopened.0, reopened.1), (None, None));
    assert_eq!(reopened.2, reopened.3);
    assert!(reopened.2 > last.2 && reopened.3 > last.3);
    assert_ne!(
        again.torrent(job_id).await.unwrap().state,
        TorrentState::Seeding
    );
    completes_again(&again, &rig.db, job_id, &b).await;
    again.stop().await;

    // Complete as stored, it is not reopened again.
    let third = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&third).await;
    let (completed, stored, at, stored_at) = place_of(&third, &rig.db, job_id).await;
    assert!(completed.is_some() && stored.is_some());
    assert_eq!((at, stored_at), (reopened.2, reopened.3));
    third.stop().await;
}

// A change of levels with no bytes to copy is stored before the store takes
// it, so no file is written in place while its stored level still sets it
// aside. One the store then refuses is stored back as memory holds it: the
// call fails, the file is not made, and the torrent stays complete with its
// levels, its place and the queue's next place as they were. Asked again,
// it reopens once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_change_the_store_refuses_is_stored_back() {
    use FilePriority::*;
    let rig = rig(set_clock() as Arc<dyn Clock>);
    let (job_id, _, _) = complete_with_one_aside(&rig).await;
    let store = rig.service.inner.live.lock().await[&job_id]
        .store
        .clone()
        .unwrap();
    let b = store.files().path(1).unwrap();
    assert!(!b.exists());
    let before = completion_of(&rig, job_id).await;
    assert_eq!(before.2, [Normal, Ignored]);
    assert!(before.1.is_some());
    let shown = shown_of(&rig, job_id).await;

    store.fail_next_relevel();
    assert!(
        rig.service
            .set_file_priorities(job_id, &[(1, Normal)])
            .await
            .is_err()
    );
    assert_eq!(completion_of(&rig, job_id).await, before);
    assert_eq!(shown_of(&rig, job_id).await, shown);
    assert!(!b.exists());

    rig.service
        .set_file_priorities(job_id, &[(1, Normal)])
        .await
        .unwrap();
    let after = completion_of(&rig, job_id).await;
    assert_eq!(
        (after.0, after.1, after.2),
        (None, None, vec![Normal, Normal])
    );
    assert_eq!((after.3, after.4), (before.5, before.5));
    assert_eq!(after.5, before.5 + 1);
    rig.service.stop().await;
}

// A database that refuses every write that stores a torrent complete with
// its file levels, while `on`: what stores a reopen back, and not the reopen.
fn refuse_complete_level_writes(db: &Database, on: bool) {
    use crate::persistence::sql_runtime::SqlRuntime;
    let sql = if on {
        "CREATE TRIGGER refuse_complete_levels BEFORE UPDATE OF file_priorities ON torrents \
         WHEN NEW.completed_at_ms IS NOT NULL \
         BEGIN SELECT RAISE(ABORT, 'refused'); END"
    } else {
        "DROP TRIGGER refuse_complete_levels"
    };
    let store = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&store, "refuse_complete_level_writes_fixture", |tx| {
            Box::pin(async move {
                tx.execute(sql, &[]).await?;
                Ok(())
            })
        })
        .await
    })
    .unwrap();
}

// A reopen the store refuses whose storing back the database refuses too
// keeps the place it stored: the queue does not hand that place out again.
// Stored short of complete with the levels asked, the next run downloads
// what was asked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_place_stored_by_a_reopen_that_cannot_be_stored_back_is_not_handed_out_again() {
    use FilePriority::*;
    let rig = rig(set_clock() as Arc<dyn Clock>);
    let (job_id, _, _) = complete_with_one_aside(&rig).await;
    let store = rig.service.inner.live.lock().await[&job_id]
        .store
        .clone()
        .unwrap();
    let before = completion_of(&rig, job_id).await;

    store.fail_next_relevel();
    refuse_complete_level_writes(&rig.db, true);
    assert!(
        rig.service
            .set_file_priorities(job_id, &[(1, Normal)])
            .await
            .is_err()
    );
    refuse_complete_level_writes(&rig.db, false);
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(
        (stored.completed_at_ms, stored.queue_position),
        (None, before.5)
    );
    let next = rig
        .service
        .inner
        .next_queue_position
        .load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(next, before.5 + 1);
    let added = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("later", 7)))
        })
        .await
        .unwrap();
    assert_ne!(
        rig.db.get_torrent(added).unwrap().unwrap().queue_position,
        before.5
    );
    // Memory is as it was: complete, the file set aside.
    let live = rig.service.inner.live.lock().await;
    assert_eq!(live[&job_id].record.completed_at_ms, before.0);
    assert_eq!(live[&job_id].levels, [Normal, Ignored]);
    drop(live);
    rig.service.stop().await;
}

// A checkpoint taken off the unsaved for a write that never comes, as when a
// stop ends the task that took it part way, stays owed: the stop writes it,
// pieces and file times.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_checkpoint_its_taker_dropped_before_writing_is_written_at_stop() {
    use std::future::Future;
    use std::sync::atomic::Ordering::SeqCst;
    let rig = rig(set_clock() as Arc<dyn Clock>);
    rig.service.open().await.unwrap();
    let bytes: Vec<u8> = (0..4096u32).map(|at| (at * 7 % 251) as u8).collect();
    let meta = metainfo(
        "two",
        &[("two/a.bin", &bytes[..2048]), ("two/b.bin", &bytes[2048..])],
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
    let store = rig.service.inner.live.lock().await[&job_id]
        .store
        .clone()
        .unwrap();
    for piece in [0u32, 1] {
        let at = piece as usize * 1024;
        Pieces::verified(
            &*store,
            piece,
            &bytes::Bytes::copy_from_slice(&bytes[at..at + 1024]),
        )
        .unwrap();
    }
    // The settles the pieces asked for are over: none is under way, and
    // none is asked for.
    let signal = &rig.service.inner.signal;
    let mut settles = rig.service.inner.settled.subscribe();
    signal.look(job_id);
    settles
        .wait_for(|_| locked(&signal.dirty).is_empty() && !rig.service.inner.settling.load(SeqCst))
        .await
        .unwrap();
    assert_eq!(
        kept_pieces(&rig.db, job_id),
        0,
        "two pieces wait for no checkpoint"
    );
    {
        // The settle takes the checkpoint as due, and waits on `live` to
        // write it; it is dropped there, as an aborted task is.
        let live = rig.service.inner.live.lock().await;
        *locked(&signal.unsaved).entry(job_id).or_default() += CHECKPOINT_PIECES;
        let mut settle = Box::pin(rig.service.settle());
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(settle.as_mut().poll(&mut cx).is_pending());
        drop(settle);
        drop(live);
    }
    rig.service.stop().await;
    assert_eq!(kept_pieces(&rig.db, job_id), 2);
    assert!(
        rig.db.torrent_file_stamps(job_id).unwrap().contains_key(&0),
        "the first file's time went with its pieces"
    );
}

// The run ids of the latest TORRENT_DOWNLOADED and JOB_DOWNLOADED queued
// for the job, and the occurrence of each.
fn download_ends(db: &Database, job_id: JobId) -> [(String, u64); 2] {
    use crate::post_processing::model::QueueEvent;
    [QueueEvent::TorrentDownloaded, QueueEvent::JobDownloaded]
        .map(|event| db.latest_download_end(job_id.0, event).unwrap().unwrap())
}

// A reopened torrent that completes again is finished again and runs its
// program a second time, as qBittorrent does (`prioritizeFiles` clears its
// finished state, torrentimpl.cpp:3045-3057), with or without a restart
// between the reopen and the completion: once more, and only once.
async fn completes_again_and_ends_again(restart: bool) {
    use FilePriority::*;
    let rig = rig(set_clock() as Arc<dyn Clock>);
    rig.db.enable_queue_scripts_for_test(rig.dir.path());
    let (job_id, _, b) = complete_with_one_aside(&rig).await;
    rig.db.admitted_so_far().await;
    let first = download_ends(&rig.db, job_id);
    assert!(first.iter().all(|(_, occurrence)| *occurrence == 0));
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().download_ends,
        1
    );
    rig.service
        .set_file_priorities(job_id, &[(1, Normal)])
        .await
        .unwrap();
    let service = if restart {
        rig.service.stop().await;
        let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
        reopen(&again).await;
        assert!(!again.inner.live.lock().await[&job_id].complete());
        again
    } else {
        rig.service.clone()
    };
    completes_again(&service, &rig.db, job_id, &b).await;
    // Raised within the settle that wrote the completion `completes_again`
    // waited for.
    rig.db.admitted_so_far().await;
    let again = download_ends(&rig.db, job_id);
    for (first, again) in first.iter().zip(&again) {
        assert_ne!(first.0, again.0);
        assert_eq!(again.1, 1);
    }
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().download_ends,
        2
    );
    service.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reopened_torrent_that_completes_again_runs_its_download_end_again() {
    completes_again_and_ends_again(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_reopened_then_restarted_runs_its_download_end_once_more() {
    completes_again_and_ends_again(true).await;
}
