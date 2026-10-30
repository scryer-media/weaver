// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::settings::{Leaf, SeedingAction, SeedingGoalMode, SeedingPolicy};
use crate::torrents::{
    FilePriority, Infohash, NamedBy, TorrentCounters, TorrentInsert, TorrentIntent, TorrentRecord,
    TorrentSource, TrackerEntry,
};

fn torrent(job_id: u64, v1: Option<[u8; 20]>, v2: Option<[u8; 32]>) -> TorrentRecord {
    TorrentRecord {
        unpack_in_force: None,
        job_id: JobId(job_id),
        infohash_v1: v1,
        infohash_v2: v2,
        name: format!("torrent {job_id}"),
        source: TorrentSource::Magnet,
        metainfo: None,
        category_id: None,
        added_at_ms: 1_000 + job_id as i64,
        intent: TorrentIntent::Resumed,
        forced: false,
        completed_at_ms: None,
        storage_dir: None,
        policy_override: None,
        unpack: None,
        metadata: Vec::new(),
        upload_limit: None,
        download_limit: None,
        sequential: false,
        first_last_piece_first: false,
        save_path: None,
        content_layout: None,
        files_dir: None,
        seed_mode: false,
        move_to: None,
        queue_position: job_id as i64,
    }
}

fn trackers() -> Vec<TrackerEntry> {
    vec![
        TrackerEntry {
            tier: 0,
            url: "udp://tracker.invalid:80/announce".to_string(),
            named_by: NamedBy::Torrent,
        },
        TrackerEntry {
            tier: 1,
            url: "https://backup.invalid/announce".to_string(),
            named_by: NamedBy::Operator,
        },
    ]
}

#[test]
fn a_torrent_round_trips_with_its_files_trackers_and_ledger() {
    let db = Database::open_in_memory().unwrap();
    let mut record = torrent(7, Some([1; 20]), Some([2; 32]));
    record.source = TorrentSource::Metainfo;
    record.metainfo = Some(bytes::Bytes::from_static(b"d4:infod4:name1:aee"));
    record.forced = true;
    record.category_id = Some(4);
    record.storage_dir = Some("/data/torrents/7".to_string());
    record.policy_override = Some(SeedingPolicy {
        seed_time_counting: Default::default(),
        ratio_goal: Leaf::Set(1.5),
        seed_time_goal_minutes: Leaf::Off,
        inactive_goal_minutes: Leaf::Set(30),
        mode: Leaf::Set(SeedingGoalMode::All),
        action: Leaf::Inherit,
    });
    record.unpack = Some(false);
    record.unpack_in_force = Some(true);
    record.upload_limit = Some(0);
    record.download_limit = Some(5_000_000_000);

    assert_eq!(
        db.insert_torrent(&record, &trackers()).unwrap(),
        TorrentInsert::Inserted
    );

    assert_eq!(db.get_torrent(JobId(7)).unwrap(), Some(record.clone()));
    assert_eq!(
        db.get_torrent_by_infohash(&[Infohash::V1([1; 20])])
            .unwrap(),
        Some(record.clone())
    );
    assert_eq!(
        db.get_torrent_by_infohash(&[Infohash::V2([2; 32])])
            .unwrap(),
        Some(record)
    );
    assert_eq!(db.list_torrent_trackers(JobId(7)).unwrap(), trackers());
    assert_eq!(
        db.get_torrent_counters(JobId(7)).unwrap(),
        Some(TorrentCounters::default())
    );
    assert_eq!(db.get_torrent(JobId(8)).unwrap(), None);
}

#[test]
fn rate_limits_update_in_place() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(3, Some([3; 20]), None), &[])
        .unwrap();
    let limits = |db: &Database| {
        let stored = db.get_torrent(JobId(3)).unwrap().unwrap();
        (stored.upload_limit, stored.download_limit)
    };
    assert_eq!(limits(&db), (None, None));

    assert!(
        db.set_torrent_rate_limits(JobId(3), Some(Some(65_536)), Some(Some(0)))
            .unwrap()
    );
    assert_eq!(limits(&db), (Some(65_536), Some(0)));
    // A direction left out is kept.
    assert!(
        db.set_torrent_rate_limits(JobId(3), Some(None), None)
            .unwrap()
    );
    assert_eq!(limits(&db), (None, Some(0)));
    assert!(
        db.set_torrent_rate_limits(JobId(3), None, Some(None))
            .unwrap()
    );
    assert_eq!(limits(&db), (None, None));
    assert!(
        !db.set_torrent_rate_limits(JobId(4), Some(Some(1)), None)
            .unwrap()
    );
}

#[test]
fn intent_and_counters_update_in_place() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(3, Some([3; 20]), None), &[])
        .unwrap();

    assert!(db.set_torrent_completed(JobId(3), 9_000).unwrap());
    // An intent written after, or racing, the completion keeps it: the two
    // are apart.
    assert!(
        db.update_torrent_intent(JobId(3), TorrentIntent::Paused)
            .unwrap()
    );
    let stored = db.get_torrent(JobId(3)).unwrap().unwrap();
    assert_eq!(stored.intent, TorrentIntent::Paused);
    assert_eq!(stored.completed_at_ms, Some(9_000));
    assert!(
        db.update_torrent_intent(JobId(3), TorrentIntent::Stopped)
            .unwrap()
    );
    let stored = db.get_torrent(JobId(3)).unwrap().unwrap();
    assert_eq!(stored.intent, TorrentIntent::Stopped);
    assert_eq!(stored.completed_at_ms, Some(9_000));

    let counters = TorrentCounters {
        consecutive_seeding_s: 0,
        uploaded_bytes: 7_000_000_000,
        downloaded_bytes: 3_500_000_000,
        active_duration_s: 900,
        last_seen_complete_ms: Some(8_800),
        finished_duration_s: 600,
        last_activity_ms: Some(8_500),
    };
    assert!(db.update_torrent_counters(JobId(3), &counters).unwrap());
    assert_eq!(db.get_torrent_counters(JobId(3)).unwrap(), Some(counters));
    // A write of the peers alone leaves the ledger as it is.
    let peers = weaver_torrent::ResumePeers {
        peers: vec!["10.0.0.1:6881".parse().unwrap()],
        banned: Vec::new(),
    };
    assert!(
        db.save_torrent_resume(JobId(3), None, Some(&peers))
            .unwrap()
    );
    assert_eq!(db.get_torrent_counters(JobId(3)).unwrap(), Some(counters));
    assert_eq!(db.torrent_peers(JobId(3)).unwrap(), peers);
    assert!(!db.save_torrent_resume(JobId(3), None, None).unwrap());

    assert!(
        !db.update_torrent_intent(JobId(4), TorrentIntent::Paused)
            .unwrap()
    );
    assert!(!db.set_torrent_completed(JobId(4), 1).unwrap());
    assert!(!db.update_torrent_counters(JobId(4), &counters).unwrap());
}

#[test]
fn a_v1_infohash_admits_one_job() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(1, Some([9; 20]), None), &[])
        .unwrap();

    // The same v1 hash is the same torrent whatever v2 hash comes with it.
    assert_eq!(
        db.insert_torrent(&torrent(2, Some([9; 20]), Some([5; 32])), &[])
            .unwrap(),
        TorrentInsert::Duplicate(JobId(1))
    );
    assert_eq!(db.get_torrent(JobId(2)).unwrap(), None);
}

#[test]
fn a_v2_only_infohash_admits_one_job() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(1, None, Some([6; 32])), &[])
        .unwrap();

    assert_eq!(
        db.insert_torrent(&torrent(2, None, Some([6; 32])), &[])
            .unwrap(),
        TorrentInsert::Duplicate(JobId(1))
    );
    // A different v2-only torrent is its own job.
    assert_eq!(
        db.insert_torrent(&torrent(3, None, Some([7; 32])), &[])
            .unwrap(),
        TorrentInsert::Inserted
    );
}

#[test]
fn a_v2_hash_admits_one_job_whether_or_not_a_v1_hash_comes_with_it() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(1, Some([3; 20]), Some([3; 32])), &[])
        .unwrap();
    db.insert_torrent(&torrent(2, None, Some([8; 32])), &[])
        .unwrap();

    // A hybrid's v2 hash alone is the hybrid.
    assert_eq!(
        db.insert_torrent(&torrent(3, None, Some([3; 32])), &[])
            .unwrap(),
        TorrentInsert::Duplicate(JobId(1))
    );
    // A v2-only torrent is the hybrid that shares its v2 hash.
    assert_eq!(
        db.insert_torrent(&torrent(4, Some([8; 20]), Some([8; 32])), &[])
            .unwrap(),
        TorrentInsert::Duplicate(JobId(2))
    );
}

#[test]
fn a_hybrid_torrent_is_found_by_either_hash() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(1, Some([4; 20]), Some([4; 32])), &[])
        .unwrap();

    assert_eq!(
        db.get_torrent_by_infohash(&[Infohash::V2([4; 32])])
            .unwrap()
            .map(|t| t.job_id),
        Some(JobId(1))
    );
    // A torrent with no hash at all is refused before it reaches the table.
    assert!(db.insert_torrent(&torrent(2, None, None), &[]).is_err());
}

#[test]
fn the_hosts_a_torrent_announces_to_outlive_it() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(5, Some([5; 20]), None), &trackers())
        .unwrap();
    // A magnet's metainfo names its trackers again, spelled otherwise, and
    // more: each host is stored once, as a rule matches it.
    let tracker = |tier, url: &str| TrackerEntry {
        tier,
        url: url.to_string(),
        named_by: NamedBy::Torrent,
    };
    let more = [
        tracker(0, "UDP://Tracker.Invalid.:6969/announce"),
        tracker(1, "https://BACKUP.invalid/other/announce"),
        tracker(2, "https://Private.Invalid./a1b2c3/announce"),
        tracker(2, "udp://private.invalid:80/a1b2c3"),
        tracker(3, "udp://bücher.invalid:80"),
        tracker(3, "udp://0x7f.1:6969"),
        tracker(4, "not a url"),
    ];
    assert!(
        db.set_torrent_metainfo(
            JobId(5),
            Some([5; 20]),
            None,
            "torrent 5",
            b"d4:infodee",
            "/data/t5",
            &more
        )
        .unwrap()
    );
    assert!(db.delete_torrent(JobId(5)).unwrap());

    assert_eq!(
        db.list_tracker_hosts().unwrap(),
        [
            "127.0.0.1",
            "backup.invalid",
            "private.invalid",
            "tracker.invalid",
            "xn--bcher-kva.invalid"
        ]
    );
}

#[test]
fn deleting_a_torrent_removes_its_rows() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(5, Some([5; 20]), None), &trackers())
        .unwrap();

    assert!(db.delete_torrent(JobId(5)).unwrap());
    assert_eq!(db.get_torrent(JobId(5)).unwrap(), None);
    assert!(db.list_torrent_trackers(JobId(5)).unwrap().is_empty());
    assert_eq!(db.get_torrent_counters(JobId(5)).unwrap(), None);
    assert!(!db.delete_torrent(JobId(5)).unwrap());

    // The hash is free again once the job is gone.
    assert_eq!(
        db.insert_torrent(&torrent(6, Some([5; 20]), None), &[])
            .unwrap(),
        TorrentInsert::Inserted
    );
}

#[test]
fn torrents_list_by_category_in_the_order_added() {
    let db = Database::open_in_memory().unwrap();
    for (job_id, category) in [(1, Some(2)), (2, None), (3, Some(2)), (4, Some(3))] {
        let mut record = torrent(job_id, Some([job_id as u8; 20]), None);
        record.category_id = category;
        db.insert_torrent(&record, &[]).unwrap();
    }

    let ids = |category| {
        db.list_torrents_by_category(category)
            .unwrap()
            .into_iter()
            .map(|t| t.job_id.0)
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(Some(2)), vec![1, 3]);
    assert_eq!(ids(Some(3)), vec![4]);
    assert_eq!(ids(None), vec![2]);
    assert!(ids(Some(9)).is_empty());
}

// Each option is its own write: one leaves the others as they are.
#[test]
fn each_option_writes_its_own_column() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(1, Some([1; 20]), None), &[])
        .unwrap();
    let policy = SeedingPolicy {
        ratio_goal: Leaf::Set(2.0),
        action: Leaf::Set(SeedingAction::Stop),
        ..SeedingPolicy::default()
    };
    assert!(db.set_torrent_category(JobId(1), Some(4)).unwrap());
    assert!(db.set_torrent_forced(JobId(1), true).unwrap());
    assert!(
        db.set_torrent_policy_override(JobId(1), Some(policy))
            .unwrap()
    );
    let stored = db.get_torrent(JobId(1)).unwrap().unwrap();
    assert_eq!(
        (stored.category_id, stored.forced, stored.policy_override),
        (Some(4), true, Some(policy))
    );
    assert!(db.set_torrent_forced(JobId(1), false).unwrap());
    let stored = db.get_torrent(JobId(1)).unwrap().unwrap();
    assert_eq!(
        (stored.category_id, stored.forced, stored.policy_override),
        (Some(4), false, Some(policy))
    );
    // A policy whose every leaf inherits is kept as none.
    assert!(
        db.set_torrent_policy_override(JobId(1), Some(SeedingPolicy::default()))
            .unwrap()
    );
    assert_eq!(
        db.get_torrent(JobId(1)).unwrap().unwrap().policy_override,
        None
    );
    assert!(!db.set_torrent_forced(JobId(2), true).unwrap());
}

// A torrent's saved peers and bans read back as written, an empty list as
// none, and a torrent that is not one saves nothing.
#[test]
fn a_torrents_peers_and_bans_round_trip() {
    use weaver_torrent::ResumePeers;

    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(1, Some([1; 20]), None), &[])
        .unwrap();
    assert_eq!(db.torrent_peers(JobId(1)).unwrap(), ResumePeers::default());

    let saved = ResumePeers {
        peers: vec![
            "10.0.0.1:6881".parse().unwrap(),
            "[2001:db8::1]:51413".parse().unwrap(),
        ],
        banned: vec!["10.0.0.9".parse().unwrap(), "2001:db8::9".parse().unwrap()],
    };
    assert!(db.save_torrent_peers(JobId(1), &saved).unwrap());
    assert_eq!(db.torrent_peers(JobId(1)).unwrap(), saved);

    let unbanned = ResumePeers {
        banned: Vec::new(),
        ..saved.clone()
    };
    assert!(db.save_torrent_peers(JobId(1), &unbanned).unwrap());
    assert_eq!(db.torrent_peers(JobId(1)).unwrap(), unbanned);

    assert!(!db.save_torrent_peers(JobId(2), &saved).unwrap());
    assert_eq!(db.torrent_peers(JobId(2)).unwrap(), ResumePeers::default());
}

#[test]
fn file_levels_and_picking_toggles_are_stored() {
    use FilePriority::*;
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(1, Some([1; 20]), None), &[])
        .unwrap();
    // Nothing set: every file is normal, and nothing is stored.
    assert!(db.torrent_file_priorities(JobId(1)).unwrap().is_empty());

    // The normal files at the end are left off.
    assert!(
        db.set_torrent_file_priorities(JobId(1), &[Maximum, Ignored, High, Normal, Normal])
            .unwrap()
    );
    assert_eq!(
        db.torrent_file_priorities(JobId(1)).unwrap(),
        [Maximum, Ignored, High]
    );
    // Back to all normal is back to nothing stored.
    assert!(
        db.set_torrent_file_priorities(JobId(1), &[Normal, Normal])
            .unwrap()
    );
    assert!(db.torrent_file_priorities(JobId(1)).unwrap().is_empty());
    assert!(!db.set_torrent_file_priorities(JobId(2), &[High]).unwrap());

    let record = db.get_torrent(JobId(1)).unwrap().unwrap();
    assert!(!record.sequential && !record.first_last_piece_first);
    // Each toggle is written alone, so setting one keeps the other.
    assert!(db.set_torrent_sequential(JobId(1), true).unwrap());
    assert!(
        db.set_torrent_first_last_piece_first(JobId(1), true)
            .unwrap()
    );
    let record = db.get_torrent(JobId(1)).unwrap().unwrap();
    assert!(record.sequential && record.first_last_piece_first);
    assert!(db.set_torrent_sequential(JobId(1), false).unwrap());
    let record = db.get_torrent(JobId(1)).unwrap().unwrap();
    assert!(!record.sequential && record.first_last_piece_first);
    assert!(!db.set_torrent_sequential(JobId(2), true).unwrap());
    assert!(
        !db.set_torrent_first_last_piece_first(JobId(2), true)
            .unwrap()
    );
}

#[test]
fn a_stored_file_level_reads_back_as_the_level_it_was() {
    use crate::torrents::model::{file_priorities_str, parse_file_priorities};
    use FilePriority::*;
    assert_eq!(file_priorities_str(&[]), None);
    assert_eq!(file_priorities_str(&[Normal, Normal]), None);
    assert_eq!(
        file_priorities_str(&[Ignored, Normal, High, Maximum, Normal]).as_deref(),
        Some("0167")
    );
    assert_eq!(
        parse_file_priorities("0167"),
        Some(vec![Ignored, Normal, High, Maximum])
    );
    // A number qBittorrent does not define is no level.
    for value in ["2", "4", "x", "1-"] {
        assert_eq!(parse_file_priorities(value), None, "{value}");
    }
}
