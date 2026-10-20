// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::settings::{SeedingAction, SeedingGoalMode, SeedingPolicy};
use crate::torrents::{
    Infohash, TorrentCounters, TorrentFile, TorrentInsert, TorrentIntent, TorrentRecord,
    TorrentSource, TrackerEntry,
};

fn torrent(job_id: u64, v1: Option<[u8; 20]>, v2: Option<[u8; 32]>) -> TorrentRecord {
    TorrentRecord {
        job_id: JobId(job_id),
        infohash_v1: v1,
        infohash_v2: v2,
        name: format!("torrent {job_id}"),
        source: TorrentSource::Magnet,
        metainfo: None,
        private: false,
        category_id: None,
        added_at_ms: 1_000 + job_id as i64,
        intent: TorrentIntent::Resumed,
        forced: false,
        completed_at_ms: None,
        storage_dir: None,
        policy_override: None,
        tracker_rule_id: None,
        unpack: None,
    }
}

fn files() -> Vec<TorrentFile> {
    vec![
        TorrentFile {
            index: 0,
            path: "a/one.bin".to_string(),
            length: 10,
            selected: true,
        },
        TorrentFile {
            index: 1,
            path: "a/two.bin".to_string(),
            length: 5_000_000_000,
            selected: false,
        },
    ]
}

fn trackers() -> Vec<TrackerEntry> {
    vec![
        TrackerEntry {
            tier: 0,
            url: "udp://tracker.invalid:80/announce".to_string(),
        },
        TrackerEntry {
            tier: 1,
            url: "https://backup.invalid/announce".to_string(),
        },
    ]
}

#[test]
fn a_torrent_round_trips_with_its_files_trackers_and_ledger() {
    let db = Database::open_in_memory().unwrap();
    let mut record = torrent(7, Some([1; 20]), Some([2; 32]));
    record.source = TorrentSource::Metainfo;
    record.metainfo = Some(b"d4:infod4:name1:aee".to_vec());
    record.private = true;
    record.forced = true;
    record.category_id = Some(4);
    record.storage_dir = Some("/data/torrents/7".to_string());
    record.policy_override = Some(SeedingPolicy {
        ratio_goal: Some(1.5),
        seed_time_goal_minutes: None,
        inactive_goal_minutes: Some(30),
        mode: SeedingGoalMode::All,
        action: SeedingAction::RemoveWithData,
        max_upload_bps: Some(1_000_000),
    });

    assert_eq!(
        db.insert_torrent(&record, &files(), &trackers()).unwrap(),
        TorrentInsert::Inserted
    );

    assert_eq!(db.get_torrent(JobId(7)).unwrap(), Some(record.clone()));
    assert_eq!(
        db.get_torrent_by_infohash(Infohash::V1([1; 20])).unwrap(),
        Some(record.clone())
    );
    assert_eq!(
        db.get_torrent_by_infohash(Infohash::V2([2; 32])).unwrap(),
        Some(record)
    );
    assert_eq!(db.list_torrent_files(JobId(7)).unwrap(), files());
    assert_eq!(db.list_torrent_trackers(JobId(7)).unwrap(), trackers());
    assert_eq!(
        db.get_torrent_counters(JobId(7)).unwrap(),
        Some(TorrentCounters {
            updated_at_ms: 1_007,
            ..TorrentCounters::default()
        })
    );
    assert_eq!(db.get_torrent(JobId(8)).unwrap(), None);
}

#[test]
fn intent_and_counters_update_in_place() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(3, Some([3; 20]), None), &[], &[])
        .unwrap();

    assert!(
        db.update_torrent_intent(JobId(3), TorrentIntent::Stopped, Some(9_000))
            .unwrap()
    );
    let stored = db.get_torrent(JobId(3)).unwrap().unwrap();
    assert_eq!(stored.intent, TorrentIntent::Stopped);
    assert_eq!(stored.completed_at_ms, Some(9_000));

    let counters = TorrentCounters {
        uploaded_bytes: 7_000_000_000,
        downloaded_bytes: 3_500_000_000,
        finished_duration_s: 600,
        last_activity_ms: Some(8_500),
        last_announce_uploaded_bytes: 6_000_000_000,
        updated_at_ms: 9_100,
    };
    assert!(db.update_torrent_counters(JobId(3), &counters).unwrap());
    assert_eq!(db.get_torrent_counters(JobId(3)).unwrap(), Some(counters));

    assert!(
        !db.update_torrent_intent(JobId(4), TorrentIntent::Paused, None)
            .unwrap()
    );
    assert!(!db.update_torrent_counters(JobId(4), &counters).unwrap());
}

#[test]
fn a_v1_infohash_admits_one_job() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(1, Some([9; 20]), None), &[], &[])
        .unwrap();

    // The same v1 hash is the same torrent whatever v2 hash comes with it.
    assert_eq!(
        db.insert_torrent(&torrent(2, Some([9; 20]), Some([5; 32])), &files(), &[])
            .unwrap(),
        TorrentInsert::Duplicate(JobId(1))
    );
    assert_eq!(db.get_torrent(JobId(2)).unwrap(), None);
    assert!(db.list_torrent_files(JobId(2)).unwrap().is_empty());
}

#[test]
fn a_v2_only_infohash_admits_one_job() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(1, None, Some([6; 32])), &[], &[])
        .unwrap();

    assert_eq!(
        db.insert_torrent(&torrent(2, None, Some([6; 32])), &[], &[])
            .unwrap(),
        TorrentInsert::Duplicate(JobId(1))
    );
    // A different v2-only torrent is its own job.
    assert_eq!(
        db.insert_torrent(&torrent(3, None, Some([7; 32])), &[], &[])
            .unwrap(),
        TorrentInsert::Inserted
    );
}

#[test]
fn a_hybrid_torrent_is_found_by_either_hash() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(1, Some([4; 20]), Some([4; 32])), &[], &[])
        .unwrap();

    assert_eq!(
        db.get_torrent_by_infohash(Infohash::V2([4; 32]))
            .unwrap()
            .map(|t| t.job_id),
        Some(JobId(1))
    );
    // A torrent with no hash at all is refused before it reaches the table.
    assert!(
        db.insert_torrent(&torrent(2, None, None), &[], &[])
            .is_err()
    );
}

#[test]
fn deleting_a_torrent_removes_its_rows() {
    let db = Database::open_in_memory().unwrap();
    db.insert_torrent(&torrent(5, Some([5; 20]), None), &files(), &trackers())
        .unwrap();

    assert!(db.delete_torrent(JobId(5)).unwrap());
    assert_eq!(db.get_torrent(JobId(5)).unwrap(), None);
    assert!(db.list_torrent_files(JobId(5)).unwrap().is_empty());
    assert!(db.list_torrent_trackers(JobId(5)).unwrap().is_empty());
    assert_eq!(db.get_torrent_counters(JobId(5)).unwrap(), None);
    assert!(!db.delete_torrent(JobId(5)).unwrap());

    // The hash is free again once the job is gone.
    assert_eq!(
        db.insert_torrent(&torrent(6, Some([5; 20]), None), &[], &[])
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
        db.insert_torrent(&record, &[], &[]).unwrap();
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

#[test]
fn a_torrent_keeps_the_tracker_rule_it_matched_until_the_rule_goes() {
    use crate::settings::model::TrackerRule;

    let db = Database::open_in_memory().unwrap();
    let rule = db
        .insert_tracker_rule(
            &TrackerRule {
                id: 0,
                host_pattern: "*.invalid".into(),
                seeding: None,
                never_pause: true,
                unpack: None,
            },
            1,
        )
        .unwrap();
    let mut record = torrent(1, Some([1; 20]), None);
    record.tracker_rule_id = Some(rule.id);
    db.insert_torrent(&record, &[], &trackers()).unwrap();
    assert_eq!(db.get_torrent(JobId(1)).unwrap(), Some(record.clone()));

    assert!(db.set_torrent_tracker_rule(JobId(1), None).unwrap());
    assert_eq!(
        db.get_torrent(JobId(1)).unwrap().unwrap().tracker_rule_id,
        None
    );
    assert!(
        db.set_torrent_tracker_rule(JobId(1), Some(rule.id))
            .unwrap()
    );
    assert!(!db.set_torrent_tracker_rule(JobId(2), None).unwrap());

    // A rule that goes leaves its torrents with none.
    db.delete_tracker_rule(rule.id).unwrap();
    assert_eq!(
        db.get_torrent(JobId(1)).unwrap().unwrap().tracker_rule_id,
        None
    );
}
