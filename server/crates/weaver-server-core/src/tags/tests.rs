// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::JobHistoryRow;

#[test]
fn a_tag_is_trimmed_and_has_no_comma() {
    assert_eq!(clean_tag("  TV "), Some("TV".to_owned()));
    assert_eq!(clean_tag(""), None);
    assert_eq!(clean_tag("   "), None);
    assert_eq!(clean_tag("a,b"), None);
}

#[test]
fn a_list_skips_empty_parts_and_names_each_tag_once() {
    assert_eq!(
        split_tags(" tv, ,movies,,tv ,TV"),
        vec!["tv".to_owned(), "movies".to_owned(), "TV".to_owned()]
    );
    assert!(split_tags(",, ,").is_empty());
}

#[test]
fn tags_list_in_natural_order_case_aside_then_by_case() {
    let tags = sorted_tags(
        ["tag10", "b", "tag2", "tv", "A", "TV", "a"]
            .map(str::to_owned)
            .to_vec(),
    );
    assert_eq!(tags, ["A", "a", "b", "tag2", "tag10", "TV", "tv"]);
    assert_eq!(tag_order("x", "x1"), Ordering::Less);
    assert_eq!(tag_order("TV", "tv"), Ordering::Less);
}

fn history_row(job_id: u64) -> JobHistoryRow {
    JobHistoryRow {
        protocol: crate::jobs::Protocol::Nzb,
        job_id,
        job_hash: None,
        name: format!("job {job_id}"),
        status: "complete".to_owned(),
        error_message: None,
        total_bytes: 10,
        downloaded_bytes: 10,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1_767_000_000,
        completed_at: 1_767_000_090,
        metadata: None,
        server_attribution: None,
    }
}

fn names(db: &Database) -> Vec<(String, u64)> {
    db.tag_counts()
        .unwrap()
        .into_iter()
        .map(|count| (count.name, count.jobs))
        .collect()
}

// The tag rules against a store, the same on SQLite and on PostgreSQL.
pub(crate) fn exercise_tags(db: &Database) {
    let (one, two, absent) = (JobId(20_001), JobId(20_002), JobId(20_009));
    db.insert_job_history(&history_row(one.0)).unwrap();
    db.insert_job_history(&history_row(two.0)).unwrap();
    let revision = db.tag_revision();

    // A tag made with no job keeps.
    db.create_tags(["kept", " ", "bad,tag"]).unwrap();
    assert_eq!(names(db), [("kept".to_owned(), 0)]);
    assert!(db.tag_revision() > revision);

    // Adding makes the tag, and passes over an id of no job.
    db.add_job_tags(&[one, two, absent], ["tv", "TV"]).unwrap();
    db.add_job_tags(&[two], ["hd"]).unwrap();
    assert_eq!(db.job_tags(one).unwrap(), ["TV", "tv"]);
    assert_eq!(db.job_tags(two).unwrap(), ["hd", "TV", "tv"]);
    assert!(db.job_tags(absent).unwrap().is_empty());
    assert_eq!(
        names(db),
        [
            ("hd".to_owned(), 1),
            ("kept".to_owned(), 0),
            ("TV".to_owned(), 2),
            ("tv".to_owned(), 2),
        ]
    );

    // The filter: a tag, or the empty tag for none.
    let snapshot = db.tag_snapshot().unwrap();
    assert!(snapshot.passes(two, "hd"));
    assert!(!snapshot.passes(one, "hd"));
    assert!(snapshot.passes(absent, ""));
    assert!(!snapshot.passes(one, ""));

    // Removing takes the tag off, and the tag stays made.
    db.remove_job_tags(&[one], ["TV"]).unwrap();
    assert_eq!(db.job_tags(one).unwrap(), ["tv"]);
    assert!(db.tag_snapshot().unwrap().made().contains(&"TV".to_owned()));

    // Setting replaces the whole set.
    db.set_job_tags(&[two], ["new", "tv"]).unwrap();
    assert_eq!(db.job_tags(two).unwrap(), ["new", "tv"]);

    // Removing with no tag clears the job.
    db.remove_job_tags(&[two], []).unwrap();
    assert!(db.job_tags(two).unwrap().is_empty());

    // Deleting a tag takes it off every job.
    db.delete_tags(["tv"]).unwrap();
    assert!(db.job_tags(one).unwrap().is_empty());
    assert!(!db.tag_snapshot().unwrap().made().contains(&"tv".to_owned()));

    // A job's tags go when its history is deleted.
    db.add_job_tags(&[one], ["gone"]).unwrap();
    assert!(db.delete_job_history(one.0).unwrap());
    assert!(db.job_tags(one).unwrap().is_empty());
    assert_eq!(
        names(db)
            .into_iter()
            .find(|(name, _)| name == "gone")
            .map(|(_, jobs)| jobs),
        Some(0)
    );
}

#[test]
fn tags_follow_the_rules_on_sqlite() {
    let db = Database::open_in_memory().unwrap();
    exercise_tags(&db);
}

#[test]
fn a_new_job_takes_its_tags_and_loses_them_when_never_added() {
    let db = Database::open_in_memory().unwrap();
    let job = JobId(30_001);
    db.tag_new_job(job, ["queued", ""]).unwrap();
    assert_eq!(db.job_tags(job).unwrap(), ["queued"]);
    db.untag_job(job).unwrap();
    assert!(db.job_tags(job).unwrap().is_empty());
}

#[test]
fn a_tag_change_wakes_a_watcher() {
    let db = Database::open_in_memory().unwrap();
    let mut changes = db.subscribe_tag_changes();
    let seen = *changes.borrow_and_update();
    db.create_tags(["wake"]).unwrap();
    assert!(changes.has_changed().unwrap());
    assert!(*changes.borrow_and_update() > seen);
}

#[test]
fn a_tagged_id_is_never_handed_out_again() {
    let db = Database::open_in_memory().unwrap();
    let first = db.reserve_next_job_id().unwrap();
    db.tag_new_job(JobId(first.0 + 50), ["ahead"]).unwrap();
    db.set_setting("next_job_id", &first.0.to_string()).unwrap();
    assert!(db.reserve_next_job_id().unwrap().0 > first.0 + 50);
}
