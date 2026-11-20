// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::JobHistoryRow;
use crate::history::persistence::HistoryPersistenceInsertJobHistoryQueries;
use crate::jobs::persistence::JobsPersistenceInternalQueries;
use crate::settings::persistence::SettingsPersistenceQueries;

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
        archived_at: None,
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
    // Put on no job, a tag is not made.
    db.add_job_tags(&[absent], ["nowhere"]).unwrap();
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

// After a change, one job's tags, the made tags and the counts are read
// without every job's tags, and read the same as the tags read whole: the
// same values, order and counts over many history jobs, a job with no tags
// and a tag on no job among them.
#[test]
fn reads_after_a_change_match_the_tags_read_whole() {
    let db = Database::open_in_memory().unwrap();
    let ids: Vec<JobId> = (1..=1_500u64).map(|n| JobId(40_000 + n)).collect();
    for id in &ids {
        db.insert_job_history(&history_row(id.0)).unwrap();
    }
    let every_third: Vec<JobId> = ids.iter().copied().step_by(3).collect();
    let every_fifth: Vec<JobId> = ids.iter().copied().step_by(5).collect();
    db.add_job_tags(&ids[..1_200], ["tag10", "TV", "a"])
        .unwrap();
    db.add_job_tags(&every_third, ["tag2", "tv"]).unwrap();
    db.remove_job_tags(&every_fifth, ["a"]).unwrap();
    db.create_tags(["unused"]).unwrap();
    let samples = [ids[0], ids[3], ids[5], ids[1_299], ids[1_499], JobId(9)];

    // Read with nothing in memory: the change just dropped it.
    assert!(db.cached_tag_snapshot().is_none());
    let one: Vec<Vec<String>> = samples.iter().map(|id| db.job_tags(*id).unwrap()).collect();
    let made = db.made_tags().unwrap();
    let counts = db.tag_counts().unwrap();
    assert!(db.cached_tag_snapshot().is_none());

    let whole = db.tag_snapshot().unwrap();
    let expected: Vec<Vec<String>> = samples.iter().map(|id| whole.of(*id).to_vec()).collect();
    assert_eq!(one, expected);
    assert_eq!(made, whole.made());
    assert_eq!(counts, whole.counts());
    assert_eq!(one[0], ["tag2", "tag10", "TV", "tv"]);
    assert_eq!(one[1], ["a", "tag2", "tag10", "TV", "tv"]);
    assert!(one[5].is_empty());
    assert_eq!(
        counts
            .iter()
            .map(|count| (count.name.as_str(), count.jobs))
            .collect::<Vec<_>>(),
        [
            ("a", 960),
            ("tag2", 500),
            ("tag10", 1_200),
            ("TV", 1_200),
            ("tv", 500),
            ("unused", 0)
        ]
    );
    // From memory now, the same again.
    assert!(db.cached_tag_snapshot().is_some());
    assert_eq!(db.job_tags(samples[0]).unwrap(), one[0]);
    assert_eq!(db.tag_counts().unwrap(), counts);
    assert_eq!(db.made_tags().unwrap(), made);
}

#[test]
fn a_script_is_given_its_jobs_tags() {
    let db = Database::open_in_memory().unwrap();
    let job = JobId(30_002);
    db.tag_new_job(job, ["tv", "hd"]).unwrap();
    let mut facts = crate::post_processing::runner::CompatibilityFacts::default();
    db.refresh_script_job_inputs(job.0, &mut facts).unwrap();
    assert_eq!(facts.tags, ["hd", "tv"]);
}

// Changes over hundreds of jobs and many tags, written a few statements at
// a time, leave each job the tags a statement per job and tag left it.
#[test]
fn tag_changes_to_many_jobs_end_as_one_statement_per_pair_ended() {
    use std::collections::{BTreeMap, BTreeSet};
    let db = Database::open_in_memory().unwrap();
    let jobs: Vec<JobId> = (40_000..40_600).map(JobId).collect();
    for job in &jobs {
        db.insert_job_history(&history_row(job.0)).unwrap();
    }
    let tags: Vec<String> = (0..500).map(|index| format!("tag{index}")).collect();
    let tag_refs = |range: std::ops::Range<usize>| -> Vec<&str> {
        tags[range].iter().map(String::as_str).collect()
    };
    let mut model: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    let mut made: BTreeSet<String> = BTreeSet::new();
    let put = |model: &mut BTreeMap<u64, BTreeSet<String>>, jobs: &[JobId], names: &[&str]| {
        for job in jobs {
            for name in names {
                model.entry(job.0).or_default().insert((*name).to_owned());
            }
        }
    };

    // Three tags on every job, and a new job's own.
    db.add_job_tags(&jobs, tag_refs(0..3)).unwrap();
    put(&mut model, &jobs, &tag_refs(0..3));
    made.extend(tag_refs(0..3).into_iter().map(str::to_owned));
    // Many tags on a few jobs, some already on them.
    db.add_job_tags(&jobs[..4], tag_refs(1..500)).unwrap();
    put(&mut model, &jobs[..4], &tag_refs(1..500));
    made.extend(tag_refs(1..500).into_iter().map(str::to_owned));
    // Many tags off half the jobs.
    db.remove_job_tags(&jobs[..300], tag_refs(2..480)).unwrap();
    for job in &jobs[..300] {
        for name in tag_refs(2..480) {
            model.entry(job.0).or_default().remove(name);
        }
    }
    // A set of two on a third.
    db.set_job_tags(&jobs[400..], tag_refs(7..9)).unwrap();
    for job in &jobs[400..] {
        model.insert(
            job.0,
            tag_refs(7..9).into_iter().map(str::to_owned).collect(),
        );
    }
    // Every tag off a few.
    db.remove_job_tags(&jobs[100..110], []).unwrap();
    for job in &jobs[100..110] {
        model.remove(&job.0);
    }
    // Deleting many tags takes them off every job.
    db.delete_tags(tag_refs(0..2).into_iter().chain(tag_refs(450..500)))
        .unwrap();
    for name in tag_refs(0..2).into_iter().chain(tag_refs(450..500)) {
        made.remove(name);
        for carried in model.values_mut() {
            carried.remove(name);
        }
    }

    let snapshot = db.tag_snapshot().unwrap();
    for job in &jobs {
        let expected = sorted_tags(
            model
                .get(&job.0)
                .map(|carried| carried.iter().cloned().collect())
                .unwrap_or_default(),
        );
        assert_eq!(snapshot.of(*job), expected.as_slice(), "job {}", job.0);
    }
    assert_eq!(snapshot.made(), sorted_tags(made.into_iter().collect()));
}
