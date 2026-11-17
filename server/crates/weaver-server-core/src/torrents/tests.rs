// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::settings::{Leaf, SeedingAction, SeedingGoalMode, SeedingPolicy};
use crate::torrents::{
    FilePriority, Infohash, KeptIntent, NamedBy, TorrentCounters, TorrentInsert, TorrentIntent,
    TorrentRecord, TorrentSource, TrackerEntry,
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
        kept: KeptIntent::Intent(TorrentIntent::Resumed),
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
        pool_member: None,
        private: false,
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

// What was on disk before a torrent's files is kept with its row, from the
// add and from a magnet's metadata, and reads back as nothing for a torrent
// that found nothing.
#[test]
fn what_was_on_disk_before_a_torrent_is_kept_with_it() {
    use crate::torrents::model::Adopted;
    let db = Database::open_in_memory().unwrap();
    let found = Adopted {
        all_files: false,
        files: vec!["kept/a.bin".to_owned()],
        dirs: vec![String::new(), "kept".to_owned()],
    };
    db.insert_placed_torrent(&torrent(5, Some([5; 20]), None), &[], &found, None)
        .unwrap();
    assert_eq!(db.torrent_adopted(JobId(5)).unwrap(), Some(found));

    db.insert_torrent(&torrent(6, Some([6; 20]), None), &[])
        .unwrap();
    assert_eq!(
        db.torrent_adopted(JobId(6)).unwrap(),
        Some(Adopted::default())
    );
    let all = Adopted {
        all_files: true,
        ..Adopted::default()
    };
    db.set_torrent_adopted(JobId(6), &all).unwrap();
    assert_eq!(db.torrent_adopted(JobId(6)).unwrap(), Some(all));
    db.set_torrent_adopted(JobId(6), &Adopted::default())
        .unwrap();
    assert_eq!(
        db.torrent_adopted(JobId(6)).unwrap(),
        Some(Adopted::default())
    );
    db.forget_torrent_adopted(JobId(6)).unwrap();
    assert_eq!(db.torrent_adopted(JobId(6)).unwrap(), None, "never written");
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
    assert_eq!(stored.intent(), TorrentIntent::Paused);
    assert_eq!(stored.completed_at_ms, Some(9_000));
    assert!(
        db.update_torrent_intent(JobId(3), TorrentIntent::Stopped)
            .unwrap()
    );
    let stored = db.get_torrent(JobId(3)).unwrap().unwrap();
    assert_eq!(stored.intent(), TorrentIntent::Stopped);
    assert_eq!(stored.completed_at_ms, Some(9_000));

    let counters = TorrentCounters {
        consecutive_seeding_s: 0,
        uploaded_bytes: 7_000_000_000,
        downloaded_bytes: 3_500_000_000,
        active_duration_s: 900,
        last_seen_complete_ms: Some(8_800),
        finished_duration_s: 600,
        last_activity_ms: Some(8_500),
        failed_bytes: 32_768,
        redundant_bytes: 16_384,
    };
    assert!(db.update_torrent_counters(JobId(3), &counters).unwrap());
    assert_eq!(db.get_torrent_counters(JobId(3)).unwrap(), Some(counters));
    // A write of the peers alone leaves the ledger as it is.
    let peers = weaver_torrent::ResumePeers {
        peers: vec!["10.0.0.1:6881".parse().unwrap()],
        banned: Vec::new(),
        found: Vec::new(),
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
            false,
            "/data/t5",
            None,
            &more,
            &[],
            &crate::torrents::model::Adopted::default()
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

// A magnet is public in the table until its metainfo arrives; the metainfo
// writes its privacy with it, and a list of private torrents then keeps it.
#[test]
fn a_magnets_privacy_is_written_with_its_metainfo() {
    use crate::torrents::queries::TorrentListFilter;
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(5, Some([5; 20]), None), &[])
        .unwrap();
    let private = TorrentListFilter {
        private: Some(true),
        ..TorrentListFilter::default()
    };
    assert!(!db.get_torrent(JobId(5)).unwrap().unwrap().private);
    assert_eq!(db.count_torrent_list(&private).unwrap(), 0);
    assert!(
        db.set_torrent_metainfo(
            JobId(5),
            Some([5; 20]),
            None,
            "torrent 5",
            b"d4:infodee",
            true,
            "/data/t5",
            None,
            &[],
            &[],
            &crate::torrents::model::Adopted::default()
        )
        .unwrap()
    );
    assert!(db.get_torrent(JobId(5)).unwrap().unwrap().private);
    assert_eq!(db.count_torrent_list(&private).unwrap(), 1);
    assert_eq!(db.torrent_list_ids(&private, 0, None).unwrap(), [JobId(5)]);
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
        found: vec!["[2001:db8::1]:51413".parse().unwrap()],
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

// A saved peer mark that does not read costs the marks alone: the
// torrent's peers and bans still restore, on its own read and the boot's.
#[test]
fn a_mark_that_does_not_read_keeps_the_peers_and_bans() {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    use weaver_torrent::ResumePeers;

    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(1, Some([1; 20]), None), &[])
        .unwrap();
    let saved = ResumePeers {
        peers: vec!["10.0.0.1:6881".parse().unwrap()],
        banned: vec!["10.0.0.9".parse().unwrap()],
        found: vec!["10.0.0.1:6881".parse().unwrap()],
    };
    assert!(db.save_torrent_peers(JobId(1), &saved).unwrap());
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.write_exec(),
            "UPDATE torrent_counters SET found_peers = {} WHERE job_id = {}",
            &[SqlArg::Text("not a list".to_owned()), SqlArg::I64(1)],
        )
        .await?;
        Ok(())
    })
    .unwrap();

    let unmarked = ResumePeers {
        found: Vec::new(),
        ..saved
    };
    assert_eq!(db.torrent_peers(JobId(1)).unwrap(), unmarked);
    let boot = db.restored_torrents(&[JobId(1)]).unwrap();
    assert_eq!(
        boot[0].counters.as_ref().unwrap().peers.as_ref().unwrap(),
        &unmarked
    );
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

// The intents of more torrents than one statement can bind are written in
// one call, each named torrent's and no other's.
#[test]
fn torrent_intents_past_the_bind_limit_are_written_together() {
    let db = Database::open_in_memory().unwrap();
    const TORRENTS: u64 = 2_000;
    for job_id in 1..=TORRENTS {
        let mut hash = [0u8; 20];
        hash[..8].copy_from_slice(&job_id.to_be_bytes());
        db.insert_torrent(&torrent(job_id, Some(hash), None), &[])
            .unwrap();
    }
    // The first half stopped by a wind-down, the rest but the last by
    // their goals, in one call.
    let wound = (1..TORRENTS / 2).map(JobId).collect::<Vec<_>>();
    let met = (TORRENTS / 2..TORRENTS).map(JobId).collect::<Vec<_>>();
    db.update_torrent_intents(&[
        (KeptIntent::WoundDown, &wound),
        (KeptIntent::Intent(TorrentIntent::Stopped), &met),
    ])
    .unwrap();
    for job_id in 1..=TORRENTS {
        let record = db.get_torrent(JobId(job_id)).unwrap().unwrap();
        let want = if job_id < TORRENTS / 2 {
            (TorrentIntent::Stopped, true)
        } else if job_id < TORRENTS {
            (TorrentIntent::Stopped, false)
        } else {
            (TorrentIntent::Resumed, false)
        };
        assert_eq!(
            (record.intent(), record.wound_down()),
            want,
            "torrent {job_id}"
        );
    }
    // A single intent written later writes the wind-down's mark away.
    db.update_torrent_intent(JobId(1), TorrentIntent::Paused)
        .unwrap();
    let first = db.get_torrent(JobId(1)).unwrap().unwrap();
    assert_eq!(
        (first.intent(), first.wound_down()),
        (TorrentIntent::Paused, false)
    );
    db.update_torrent_intents(&[(KeptIntent::Intent(TorrentIntent::Paused), &[])])
        .unwrap();
}

// Every stored word of a torrent's row reads back as what wrote it, and
// stays the word rows already hold: a rename here would strand them.
#[test]
fn each_stored_word_of_a_torrent_reads_back_and_keeps_its_text() {
    use crate::torrents::model::{ContentLayout, HistoryEnd, parse_named_by};
    use crate::torrents::persistence::RetiringMark;
    use crate::torrents::{TorrentState, model::named_by_str};

    let sources = [TorrentSource::Magnet, TorrentSource::Metainfo];
    for source in sources {
        assert_eq!(TorrentSource::parse(source.as_str()), Some(source));
    }
    assert_eq!(sources.map(TorrentSource::as_str), ["magnet", "metainfo"]);

    let layouts = [
        ContentLayout::Original,
        ContentLayout::Subfolder,
        ContentLayout::NoSubfolder,
    ];
    for layout in layouts {
        assert_eq!(ContentLayout::parse(layout.as_str()), Some(layout));
    }
    assert_eq!(
        layouts.map(ContentLayout::as_str),
        ["Original", "Subfolder", "NoSubfolder"]
    );

    let states = [
        TorrentState::Queued,
        TorrentState::CheckingResumeData,
        TorrentState::FetchingMetadata,
        TorrentState::CheckingFiles,
        TorrentState::Downloading,
        TorrentState::Stalled,
        TorrentState::Seeding,
        TorrentState::Paused,
        TorrentState::Stopped,
        TorrentState::Moving,
        TorrentState::NeedsCertificate,
        TorrentState::FilesMissing,
    ];
    for state in states {
        assert_eq!(TorrentState::parse(state.as_str()), Some(state));
    }
    assert_eq!(
        states.map(TorrentState::as_str),
        [
            "queued",
            "checking_resume_data",
            "fetching_metadata",
            "checking_files",
            "downloading",
            "stalled",
            "seeding",
            "paused",
            "stopped",
            "moving",
            "needs_certificate",
            "files_missing",
        ]
    );

    let kept = [
        KeptIntent::Intent(TorrentIntent::Resumed),
        KeptIntent::Intent(TorrentIntent::Paused),
        KeptIntent::Intent(TorrentIntent::Stopped),
        KeptIntent::WoundDown,
    ];
    for kept in kept {
        assert_eq!(KeptIntent::parse(kept.as_str()), Some(kept));
        if let KeptIntent::Intent(intent) = kept {
            assert_eq!(TorrentIntent::parse(intent.as_str()), Some(intent));
        }
    }
    assert_eq!(
        kept.map(KeptIntent::as_str),
        ["resumed", "paused", "stopped", "wound_down"]
    );
    // A wind-down's stop is a stop; it is no intent of the operator's.
    assert_eq!(KeptIntent::WoundDown.intent(), TorrentIntent::Stopped);
    assert_eq!(TorrentIntent::parse("wound_down"), None);
    assert_eq!(KeptIntent::parse("Stopped"), None);

    for named_by in [NamedBy::Torrent, NamedBy::Operator] {
        assert_eq!(parse_named_by(named_by_str(named_by)), Some(named_by));
    }
    assert_eq!(
        [NamedBy::Torrent, NamedBy::Operator].map(named_by_str),
        ["torrent", "operator"]
    );

    for mark in RetiringMark::ALL {
        assert_eq!(RetiringMark::parse(mark.as_str()), Some(mark));
    }
    assert_eq!(
        RetiringMark::ALL.map(RetiringMark::as_str),
        ["kept", "deleted"]
    );
    assert_eq!(RetiringMark::of(true), RetiringMark::Deleted);
    assert_eq!(RetiringMark::of(false), RetiringMark::Kept);
    assert_eq!(RetiringMark::parse("delete"), None);

    for end in HistoryEnd::ALL {
        assert_eq!(HistoryEnd::parse(end.as_str()), Some(end));
    }
    assert_eq!(
        HistoryEnd::ALL.map(HistoryEnd::as_str),
        ["complete", "partial", "failed", "cancelled"]
    );
}

// How a torrent's job ends is stored as the jobs model's own status words,
// and reads back from history as the status it ended with.
#[test]
fn a_torrents_history_end_is_the_jobs_stored_status() {
    use crate::jobs::model::{JobStatus, job_status_from_persisted_str};
    use crate::torrents::model::HistoryEnd;

    let failed = JobStatus::Failed {
        error: "why".to_owned(),
    };
    for (status, end) in [
        (JobStatus::Complete, HistoryEnd::Complete),
        (JobStatus::Partial, HistoryEnd::Partial),
        (failed.clone(), HistoryEnd::Failed),
    ] {
        assert_eq!(HistoryEnd::delivered(&status), end);
        assert_eq!(end.as_str(), status.persisted_status());
        assert_eq!(
            job_status_from_persisted_str(end.as_str(), Some("why")),
            status
        );
    }
    assert_eq!(
        job_status_from_persisted_str(HistoryEnd::Cancelled.as_str(), None),
        JobStatus::Failed {
            error: "cancelled".to_owned()
        }
    );
}

// Trackers enough to take several statements' worth of binds, many hosts
// named twice.
fn many_tracker_rows(count: u32) -> Vec<TrackerEntry> {
    (0..count)
        .map(|index| TrackerEntry {
            tier: index % 5,
            url: format!(
                "http://host{}.invalid:{}/announce",
                index / 3,
                1_000 + index
            ),
            named_by: if index % 2 == 0 {
                NamedBy::Torrent
            } else {
                NamedBy::Operator
            },
        })
        .collect()
}

// A torrent's trackers written in a few statements read back as the rows a
// statement per tracker wrote: every one, and every host once.
#[test]
fn many_trackers_are_written_as_one_statement_each_wrote_them() {
    let trackers = many_tracker_rows(700);
    let db = Database::open_in_memory().unwrap();
    assert_eq!(
        db.insert_torrent(&torrent(3, Some([3; 20]), None), &trackers)
            .unwrap(),
        TorrentInsert::Inserted
    );
    let mut expected = trackers.clone();
    expected.sort_by(|a, b| (a.tier, &a.url).cmp(&(b.tier, &b.url)));
    let mut read = db.list_torrent_trackers(JobId(3)).unwrap();
    read.sort_by(|a, b| (a.tier, &a.url).cmp(&(b.tier, &b.url)));
    assert_eq!(read, expected);
    let mut hosts: Vec<String> = (0..700 / 3 + 1)
        .map(|host| format!("host{host}.invalid"))
        .collect();
    hosts.sort();
    assert_eq!(db.list_tracker_hosts().unwrap(), hosts);

    // A second torrent naming the same hosts adds none.
    db.set_torrent_trackers(JobId(3), &many_tracker_rows(10))
        .unwrap();
    assert_eq!(db.list_tracker_hosts().unwrap(), hosts);
    assert_eq!(db.list_torrent_trackers(JobId(3)).unwrap().len(), 10);
}

// The batched writes of queue places, categories, forced flags and removal
// marks leave each torrent as a statement per row, in turn, left it, a
// torrent named twice taking the value named last.
#[test]
fn batched_torrent_columns_end_as_a_statement_per_row_left_them() {
    const COUNT: u64 = 700;
    let seeded = || {
        let db = Database::open_in_memory().unwrap();
        for job in 1..=COUNT {
            let mut v1 = [0; 20];
            v1[..8].copy_from_slice(&job.to_be_bytes());
            db.insert_torrent(&torrent(job, Some(v1), None), &[])
                .unwrap();
        }
        db
    };
    let positions: Vec<(JobId, i64)> = (1..=COUNT)
        .map(|job| (JobId(job), (COUNT - job) as i64 * 3))
        .chain([(JobId(5), -1), (JobId(COUNT + 1), 9)])
        .collect();
    let categories: Vec<(JobId, Option<u32>)> = (1..=COUNT)
        .step_by(2)
        .map(|job| (JobId(job), (job % 3 != 0).then_some(job as u32 % 7)))
        .chain([(JobId(1), None), (JobId(3), Some(42))])
        .collect();
    let forced: Vec<(JobId, bool)> = (1..=COUNT)
        .step_by(3)
        .map(|job| (JobId(job), job % 2 == 0))
        .chain([(JobId(4), false), (JobId(4), true)])
        .collect();
    let marks: Vec<(JobId, bool)> = (1..=COUNT)
        .step_by(4)
        .map(|job| (JobId(job), job % 3 == 0))
        .chain([(JobId(1), true), (JobId(1), false)])
        .collect();

    let batched = seeded();
    batched.set_torrent_queue_positions(&positions).unwrap();
    batched
        .set_torrent_categories_and_forced(&categories, &forced)
        .unwrap();

    let by_row = seeded();
    for &(job_id, position) in &positions {
        by_row
            .set_torrent_queue_positions(&[(job_id, position)])
            .unwrap();
    }
    for &(job_id, category) in &categories {
        by_row.set_torrent_category(job_id, category).unwrap();
    }
    for &(job_id, on) in &forced {
        by_row.set_torrent_forced(job_id, on).unwrap();
    }
    let mut listed = batched.list_torrents().unwrap();
    listed.sort_by_key(|record| record.job_id);
    let mut expected = by_row.list_torrents().unwrap();
    expected.sort_by_key(|record| record.job_id);
    assert_eq!(listed.len(), COUNT as usize);
    assert_eq!(listed, expected);
    assert_eq!(listed[4].queue_position, -1);
    assert!(listed[3].forced);

    // A torrent marked retiring no longer lists.
    batched.mark_torrents_retiring(&marks).unwrap();
    for &(job_id, delete_files) in &marks {
        by_row.mark_torrent_retiring(job_id, delete_files).unwrap();
    }
    let retiring = |db: &Database| {
        let mut rows: Vec<(JobId, bool)> = db
            .retiring_torrents()
            .unwrap()
            .into_iter()
            .map(|row| (row.job_id, row.delete_files))
            .collect();
        rows.sort();
        rows
    };
    let marked = retiring(&batched);
    assert_eq!(marked.len(), COUNT.div_ceil(4) as usize);
    assert_eq!(marked[0], (JobId(1), false));
    assert_eq!(marked, retiring(&by_row));
}

// Every stored text and byte of the torrents' tables and the tracker hosts,
// as the database holds them.
// As stored: each torrent's metainfo, each tracker row, each host.
type RawSecrets = (Vec<(i64, Vec<u8>)>, Vec<(i64, i64, String)>, Vec<String>);

fn raw_secrets(db: &Database) -> RawSecrets {
    use crate::persistence::sql_runtime::SqlRuntime;
    let datastore = db.datastore();
    db.run_sql_blocking_read(async move {
        let metainfo = SqlRuntime::fetch_all(
            datastore.read_exec(),
            "SELECT job_id, metainfo FROM torrents WHERE metainfo IS NOT NULL ORDER BY job_id",
            &[],
        )
        .await?
        .iter()
        .map(|row| Ok((row.i64("job_id")?, row.opt_bytes("metainfo")?.unwrap())))
        .collect::<Result<Vec<_>, crate::StateError>>()?;
        let urls = SqlRuntime::fetch_all(
            datastore.read_exec(),
            "SELECT job_id, tier, url FROM torrent_trackers ORDER BY job_id, tier",
            &[],
        )
        .await?
        .iter()
        .map(|row| Ok((row.i64("job_id")?, row.i64("tier")?, row.text("url")?)))
        .collect::<Result<Vec<_>, crate::StateError>>()?;
        let hosts = SqlRuntime::fetch_all(
            datastore.read_exec(),
            "SELECT host FROM tracker_hosts ORDER BY host",
            &[],
        )
        .await?
        .iter()
        .map(|row| row.text("host"))
        .collect::<Result<Vec<_>, _>>()?;
        Ok((metainfo, urls, hosts))
    })
    .unwrap()
}

const PASSKEY: &str = "f00dfacecafe";

// A torrent's metainfo, announcing to a private tracker with a passkey.
fn passkeyed(job_id: u64) -> (Vec<u8>, String) {
    let url = format!("https://tracker{job_id}.invalid/{PASSKEY}/announce");
    (
        format!("d8:announce{}:{url}4:infod4:name1:xee", url.len()).into_bytes(),
        url,
    )
}

fn holds_passkey(bytes: &[u8]) -> bool {
    bytes
        .windows(PASSKEY.len())
        .any(|at| at == PASSKEY.as_bytes())
}

// A torrent's passkey is in none of the bytes stored for it: its metainfo
// and its tracker URLs are sealed, and the host list keeps the host alone.
// Each reads back as it was written.
#[test]
fn no_stored_byte_holds_a_trackers_passkey() {
    let db = Database::open_in_memory().unwrap();
    let (metainfo, url) = passkeyed(1);
    let mut record = torrent(1, Some([1; 20]), None);
    record.metainfo = Some(metainfo.clone().into());
    let trackers = [TrackerEntry {
        tier: 0,
        url: url.clone(),
        named_by: NamedBy::Torrent,
    }];
    db.insert_torrent(&record, &trackers).unwrap();
    // A magnet's metainfo and an edit write them again.
    db.insert_torrent(&torrent(2, Some([2; 20]), None), &[])
        .unwrap();
    let (second, second_url) = passkeyed(2);
    let second_trackers = [TrackerEntry {
        tier: 0,
        url: second_url.clone(),
        named_by: NamedBy::Torrent,
    }];
    assert!(
        db.set_torrent_metainfo(
            JobId(2),
            Some([2; 20]),
            None,
            "torrent 2",
            &second,
            true,
            "/data/t2",
            None,
            &second_trackers,
            &[],
            &crate::torrents::model::Adopted::default()
        )
        .unwrap()
    );
    let edited = [TrackerEntry {
        tier: 1,
        url: format!("{url}?again"),
        named_by: NamedBy::Operator,
    }];
    assert!(db.set_torrent_trackers(JobId(1), &edited).unwrap());

    let (metainfos, urls, hosts) = raw_secrets(&db);
    assert_eq!(metainfos.len(), 2);
    assert_eq!(urls.len(), 2);
    for (_, stored) in &metainfos {
        assert!(!holds_passkey(stored));
        assert!(crate::torrents::sealed::is_sealed(stored));
    }
    for (_, _, stored) in &urls {
        assert!(!holds_passkey(stored.as_bytes()));
    }
    assert_eq!(hosts, ["tracker1.invalid", "tracker2.invalid"]);

    assert_eq!(
        db.get_torrent(JobId(1))
            .unwrap()
            .unwrap()
            .metainfo
            .as_deref(),
        Some(metainfo.as_slice())
    );
    assert_eq!(
        db.get_torrent(JobId(2))
            .unwrap()
            .unwrap()
            .metainfo
            .as_deref(),
        Some(second.as_slice())
    );
    assert_eq!(db.list_torrent_trackers(JobId(1)).unwrap(), edited);
    assert_eq!(db.list_torrent_trackers(JobId(2)).unwrap(), second_trackers);
}

// Rows stored before sealing, as an upgrade finds them, written in the
// clear past the sealing.
fn unseal(db: &Database, rows: &[(i64, Vec<u8>, String)]) {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    let datastore = db.datastore();
    let rows = rows.to_vec();
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&datastore, "unseal", |tx| {
            let rows = rows.clone();
            Box::pin(async move {
                for (job_id, metainfo, url) in rows {
                    tx.execute(
                        "UPDATE torrents SET metainfo = {} WHERE job_id = {}",
                        &[SqlArg::Bytes(metainfo), SqlArg::I64(job_id)],
                    )
                    .await?;
                    tx.execute(
                        "UPDATE torrent_trackers SET url = {}, url_key = NULL WHERE job_id = {}",
                        &[SqlArg::Text(url), SqlArg::I64(job_id)],
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
    })
    .unwrap();
}

// A start seals what an older one stored in the clear, a page of torrents
// at a time, each page in one transaction. A pass cut short leaves pages
// sealed and pages not: the next start seals the rest and passes by the
// sealed ones, and a start with nothing to seal changes nothing.
#[test]
fn a_sealing_pass_cut_short_completes_at_the_next_start() {
    const TORRENTS: u64 = 150;
    let db = Database::open_in_memory().unwrap();
    let mut written = Vec::new();
    for job_id in 1..=TORRENTS {
        let (metainfo, url) = passkeyed(job_id);
        let mut record = torrent(job_id, Some([job_id as u8; 20]), None);
        record.infohash_v1.as_mut().unwrap()[1] = (job_id >> 8) as u8;
        record.metainfo = Some(metainfo.clone().into());
        let trackers = [TrackerEntry {
            tier: 0,
            url: url.clone(),
            named_by: NamedBy::Torrent,
        }];
        db.insert_torrent(&record, &trackers).unwrap();
        written.push((job_id as i64, metainfo, url));
    }
    // The first page was sealed before the cut; the rest is in the clear.
    unseal(&db, &written[64..]);
    // One metainfo is sealed as text, the form before the binary one, and
    // one torrent lists its tracker twice in a tier, once sealed, as the
    // key over the ciphertext let it.
    {
        use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
        use base64::Engine as _;
        let key = db.encryption_key().cloned().unwrap();
        let text = crate::persistence::encryption::encrypt_value(
            &key,
            &base64::engine::general_purpose::STANDARD.encode(&written[148].1),
        )
        .unwrap();
        let twice = crate::persistence::encryption::encrypt_value(&key, &written[149].2).unwrap();
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET metainfo = {} WHERE job_id = 149",
                &[SqlArg::Bytes(text.into_bytes())],
            )
            .await?;
            SqlRuntime::execute(
                datastore.write_exec(),
                "INSERT INTO torrent_trackers (job_id, tier, url, named_by) \
                 VALUES (150, 0, {}, 'torrent')",
                &[SqlArg::Text(twice)],
            )
            .await
        })
        .unwrap();
    }
    let (_, before, _) = raw_secrets(&db);
    assert_eq!(before.len(), TORRENTS as usize + 1);
    assert!(
        before[..64]
            .iter()
            .all(|(.., url)| crate::persistence::encryption::is_encrypted(url))
    );
    assert!(
        before[64..]
            .iter()
            .filter(|(.., url)| !crate::persistence::encryption::is_encrypted(url))
            .all(|(.., url)| holds_passkey(url.as_bytes()))
    );

    db.seal_stored_secrets().unwrap();
    let sealed = raw_secrets(&db);
    assert_eq!(sealed.0.len(), TORRENTS as usize);
    // The tracker listed twice is listed once.
    assert_eq!(sealed.1.len(), TORRENTS as usize);
    assert!(sealed.0.iter().all(|(_, stored)| !holds_passkey(stored)));
    // Each metainfo is sealed as bytes, the one sealed as text with them.
    assert!(
        sealed
            .0
            .iter()
            .all(|(_, stored)| stored.starts_with(b"enc:b1:"))
    );
    assert!(
        sealed
            .1
            .iter()
            .all(|(.., url)| !holds_passkey(url.as_bytes()))
    );
    // The first page's values are the ones sealed before the cut.
    assert_eq!(sealed.1[..64], before[..64]);
    for (job_id, metainfo, url) in &written {
        let job_id = JobId(*job_id as u64);
        assert_eq!(
            db.get_torrent(job_id).unwrap().unwrap().metainfo.as_deref(),
            Some(metainfo.as_slice())
        );
        assert_eq!(db.list_torrent_trackers(job_id).unwrap()[0].url, *url);
    }

    db.seal_stored_secrets().unwrap();
    assert_eq!(raw_secrets(&db), sealed);

    // The pass is one pass: a start after it reads no torrent to look for
    // work, so a row written in the clear past it stays as it is.
    unseal(&db, &written[..1]);
    let clear = raw_secrets(&db);
    db.seal_stored_secrets().unwrap();
    assert_eq!(raw_secrets(&db), clear);
    assert!(holds_passkey(&clear.0[0].1));
}

// The history of a torrent removed long ago has no row among the torrents:
// the pass reads the history itself, a page at a time, and takes an RSS
// item's link and id out of every torrent's row. A usenet job's row is
// left as it is.
#[test]
fn the_history_of_a_removed_torrent_loses_its_rss_link() {
    const REMOVED: u64 = 150;
    let db = Database::open_in_memory().unwrap();
    let kept = |job_id: u64| {
        serde_json::to_string(&[
            ("rss.feed_name", "feed".to_owned()),
            ("rss.item_id", format!("guid-{job_id}")),
            (
                "rss.item_url",
                format!("https://tracker.example/dl/{job_id}?passkey=0123abcd"),
            ),
            ("label", "kept".to_owned()),
        ])
        .unwrap()
    };
    let row = |job_id: u64, protocol, metadata: Option<String>| crate::JobHistoryRow {
        job_id,
        job_hash: None,
        name: format!("job {job_id}"),
        status: "complete".to_owned(),
        error_message: None,
        total_bytes: 1,
        downloaded_bytes: 1,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1,
        completed_at: 2,
        metadata,
        server_attribution: None,
        protocol,
        archived_at: None,
    };
    // More than two pages of removed torrents, none with a torrent row.
    for job_id in 1..=REMOVED {
        db.insert_job_history(&row(
            job_id,
            crate::jobs::Protocol::Torrent,
            Some(kept(job_id)),
        ))
        .unwrap();
    }
    let usenet = REMOVED + 1;
    db.insert_job_history(&row(usenet, crate::jobs::Protocol::Nzb, Some(kept(usenet))))
        .unwrap();
    let plain = REMOVED + 2;
    let none = serde_json::to_string(&[("label", "kept")]).unwrap();
    db.insert_job_history(&row(
        plain,
        crate::jobs::Protocol::Torrent,
        Some(none.clone()),
    ))
    .unwrap();

    db.seal_stored_secrets().unwrap();
    let after: std::collections::HashMap<u64, Option<String>> = {
        use crate::persistence::sql_runtime::SqlRuntime;
        let datastore = db.datastore();
        db.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, metadata FROM job_history",
                &[],
            )
            .await?
            .iter()
            .map(|row| Ok((row.i64("job_id")? as u64, row.opt_text("metadata")?)))
            .collect::<Result<_, crate::StateError>>()
        })
        .unwrap()
    };
    let stored = |job_id: u64| after[&job_id].clone();
    let stripped = serde_json::to_string(&[("rss.feed_name", "feed"), ("label", "kept")]).unwrap();
    for job_id in 1..=REMOVED {
        assert_eq!(
            stored(job_id).as_deref(),
            Some(stripped.as_str()),
            "{job_id}"
        );
    }
    assert_eq!(stored(usenet), Some(kept(usenet)));
    assert_eq!(stored(plain), Some(none));
}

// A tracker's URL is sealed, and its ciphertext differs each time it is
// written: what keeps one URL from a tier twice is its keyed digest.
#[test]
fn a_tracker_listed_twice_in_a_tier_is_stored_once() {
    let db = Database::open_in_memory().unwrap();
    let (metainfo, url) = passkeyed(1);
    let mut record = torrent(1, Some([1; 20]), None);
    record.metainfo = Some(metainfo.into());
    let entry = |tier: u32, url: &str| TrackerEntry {
        tier,
        url: url.to_owned(),
        named_by: NamedBy::Torrent,
    };
    // Twice in one tier, and once in another, which is another row.
    let listed = [entry(0, &url), entry(0, &url), entry(1, &url)];
    db.insert_torrent(&record, &listed).unwrap();
    assert_eq!(raw_secrets(&db).1.len(), 2);
    assert_eq!(
        db.list_torrent_trackers(JobId(1)).unwrap(),
        [entry(0, &url), entry(1, &url)]
    );
    let other = format!("{url}&other");
    assert!(
        db.set_torrent_trackers(
            JobId(1),
            &[entry(0, &other), entry(0, &url), entry(0, &other)]
        )
        .unwrap()
    );
    assert_eq!(raw_secrets(&db).1.len(), 2);
    let mut kept = db.list_torrent_trackers(JobId(1)).unwrap();
    kept.sort_by(|a, b| a.url.cmp(&b.url));
    assert_eq!(kept, [entry(0, &url), entry(0, &other)]);
}

// A start that finds no key must know that sealed torrent and RSS secrets
// need the one they were sealed with, or it would make a new key over them;
// and a start with the wrong key is refused by one stored value of a sealed
// column, not found torrent by torrent.
#[test]
fn sealed_secrets_are_counted_and_proved_against_the_key_at_start() {
    let db = Database::open_in_memory().unwrap();
    let credentials = db.has_encrypted_credentials().unwrap();
    db.seal_stored_secrets().unwrap();
    // Marked, with nothing sealed: no more than before.
    assert_eq!(db.has_encrypted_credentials().unwrap(), credentials);
    let key = db.encryption_key().cloned().unwrap();
    db.validate_encrypted_credentials(&key).unwrap();

    let (metainfo, url) = passkeyed(1);
    let mut record = torrent(1, Some([1; 20]), None);
    record.metainfo = Some(metainfo.into());
    let trackers = [TrackerEntry {
        tier: 0,
        url,
        named_by: NamedBy::Torrent,
    }];
    db.insert_torrent(&record, &trackers).unwrap();
    assert!(db.has_encrypted_credentials().unwrap());
    db.validate_encrypted_credentials(&key).unwrap();
    let other = crate::persistence::encryption::EncryptionKey::generate();
    let refused = db.validate_encrypted_credentials(&other).unwrap_err();
    assert!(
        refused.to_string().contains("torrent metainfo"),
        "{refused}"
    );
}

// A key held in memory only is gone at the next start, and all it sealed
// with it: no torrent is stored under one, with why, and the sealing pass
// neither seals nor marks itself run.
#[test]
fn nothing_is_sealed_under_a_key_held_in_memory_only() {
    let mut db = Database::open_in_memory().unwrap();
    db.set_encryption_key(crate::persistence::encryption::EncryptionKey::generate().ephemeral());
    let (metainfo, url) = passkeyed(1);
    let mut record = torrent(1, Some([1; 20]), None);
    record.metainfo = Some(metainfo.into());
    let trackers = [TrackerEntry {
        tier: 0,
        url,
        named_by: NamedBy::Torrent,
    }];
    let refused = db.insert_torrent(&record, &trackers).unwrap_err();
    assert!(
        refused.to_string().contains("held in memory only"),
        "{refused}"
    );
    assert!(db.get_torrent(JobId(1)).unwrap().is_none());
    db.seal_stored_secrets().unwrap();
    assert_eq!(
        db.get_setting(crate::torrents::sealed::SEALED_MARK)
            .unwrap(),
        None
    );
}
