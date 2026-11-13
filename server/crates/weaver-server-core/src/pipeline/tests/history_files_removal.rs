// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::{HistoryFilesRemoval, HistoryFilesRemovalOutcome as Outcome};

const NOW: i64 = 1_800_000_000;

// Removing files needs no pipeline turn, as deleting history does not.
async fn remove_files(
    pipeline: &mut Pipeline,
    job_ids: &[JobId],
    now: i64,
) -> BTreeMap<JobId, HistoryFilesRemoval> {
    pipeline.report_all_for_test();
    pipeline
        .shared_state
        .queue
        .clone()
        .remove_history_files(job_ids.iter().copied().collect(), now)
        .await
        .unwrap()
}

// A delivered output directory under the complete folder with Weaver's
// ownership marker, holding one file.
fn owned_output_dir(complete_dir: &Path, name: &str, job_id: JobId) -> PathBuf {
    let dir = complete_dir.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("lantern.mkv"), b"delivered").unwrap();
    crate::jobs::working_dir::mark_weaver_owned_output_dir(&dir, job_id).unwrap();
    dir
}

fn marked_working_dir(intermediate_dir: &Path, name: &str, job_id: JobId) -> PathBuf {
    let dir = intermediate_dir.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    crate::jobs::working_dir::mark_weaver_owned_working_dir(intermediate_dir, &dir, job_id)
        .unwrap();
    std::fs::write(dir.join("lantern.par2"), b"left").unwrap();
    dir
}

fn staging_dir(complete_dir: &Path, job_id: JobId) -> PathBuf {
    let staging = crate::pipeline::queue::staging_dir_for(complete_dir, job_id);
    crate::pipeline::queue::make_staging_dir(&staging, job_id).unwrap();
    std::fs::write(staging.join("lantern.mkv"), b"staged").unwrap();
    staging
}

// The record a removal kept of an entry's files: its rows, then the totals
// on the entry.
type Record = (Vec<(String, u64)>, Option<u64>, Option<u64>);

fn record_of(pipeline: &Pipeline, job_id: JobId) -> Record {
    let rows = removed_rows(pipeline, job_id);
    let state = pipeline
        .db
        .list_job_history_file_states(&[job_id])
        .unwrap()
        .remove(&job_id)
        .expect("the history entry stays");
    (rows, state.removed_file_count, state.removed_file_bytes)
}

fn removed_rows(pipeline: &Pipeline, job_id: JobId) -> Vec<(String, u64)> {
    pipeline
        .db
        .list_history_removed_files(job_id.0, 0, 1_000)
        .unwrap()
}

const NO_RECORD: fn() -> Record = || (Vec::new(), None, None);

async fn remove_files_keeping(
    pipeline: &mut Pipeline,
    job_ids: &[JobId],
    now: i64,
    keep: usize,
) -> BTreeMap<JobId, HistoryFilesRemoval> {
    pipeline.report_all_for_test();
    pipeline
        .shared_state
        .queue
        .clone()
        .remove_history_files_keeping(job_ids.iter().copied().collect(), now, keep)
        .await
        .unwrap()
}

fn write_file(dir: &Path, rel: &str, bytes: usize) {
    let path = dir.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, vec![b'x'; bytes]).unwrap();
}

fn stamp_of(pipeline: &Pipeline, job_id: JobId) -> Option<i64> {
    pipeline
        .db
        .get_job_history(job_id.0)
        .unwrap()
        .expect("the history entry stays")
        .files_removed_at
}

// The output, working and staging directories go, the entry and its NZB
// stay, and the entry says when.
#[tokio::test]
async fn owned_files_are_removed_and_the_entry_is_stamped() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40001);
    let output = owned_output_dir(&complete_dir, "Lantern", job_id);
    let working = marked_working_dir(&intermediate_dir, "Lantern", job_id);
    let staging = staging_dir(&complete_dir, job_id);
    insert_history_row_with_nzb_zstd(
        &pipeline.db,
        &history_row_with_output_dir(job_id, "Lantern", "complete", output.clone()),
        &sample_nzb_zstd(),
    );

    let outcome = remove_files(&mut pipeline, &[job_id], NOW).await;

    assert_eq!(
        outcome[&job_id],
        HistoryFilesRemoval::new(Outcome::Removed, Some(NOW), None)
    );
    assert!(!output.exists());
    assert!(!working.exists());
    assert!(!staging.exists());
    assert_eq!(stamp_of(&pipeline, job_id), Some(NOW));
    assert!(
        pipeline
            .db
            .load_history_job_persisted_nzb(job_id.0)
            .unwrap()
            .is_some()
    );
    assert!(
        pipeline
            .shared_state
            .queue
            .read(|inner| inner.history_operations.is_empty())
    );
    retire_pipeline_database(pipeline).await;
}

// Nothing on disk is already gone, and is stamped. A second removal changes
// nothing and returns the first stamp.
#[tokio::test]
async fn a_missing_folder_is_already_gone_and_a_retry_keeps_the_first_stamp() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40002);
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "Gone",
            "complete",
            complete_dir.join("never-delivered"),
        ))
        .unwrap();

    let first = remove_files(&mut pipeline, &[job_id], NOW).await;
    assert_eq!(
        first[&job_id],
        HistoryFilesRemoval::new(Outcome::AlreadyGone, Some(NOW), None)
    );
    assert_eq!(stamp_of(&pipeline, job_id), Some(NOW));

    let second = remove_files(&mut pipeline, &[job_id], NOW + 60).await;
    assert_eq!(
        second[&job_id],
        HistoryFilesRemoval::new(Outcome::AlreadyGone, Some(NOW), None)
    );
    assert_eq!(stamp_of(&pipeline, job_id), Some(NOW));
    retire_pipeline_database(pipeline).await;
}

// An output directory without a valid marker is left whole and named, and
// the entry is not stamped; the job's own working and staging leftovers
// still go.
#[tokio::test]
async fn an_unmarked_output_dir_is_not_managed_and_kept() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40003);
    let output = complete_dir.join("Shared");
    std::fs::create_dir_all(&output).unwrap();
    std::fs::write(output.join("someone-else.mkv"), b"theirs").unwrap();
    let working = marked_working_dir(&intermediate_dir, "Shared", job_id);
    let staging = staging_dir(&complete_dir, job_id);
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "Shared",
            "complete",
            output.clone(),
        ))
        .unwrap();

    let outcome = remove_files(&mut pipeline, &[job_id], NOW).await;

    let removal = &outcome[&job_id];
    assert_eq!(removal.outcome, Outcome::NotManaged);
    assert_eq!(removal.files_removed_at, None);
    assert!(
        removal
            .message
            .as_deref()
            .unwrap()
            .contains(&output.display().to_string()),
        "{removal:?}"
    );
    assert!(output.join("someone-else.mkv").exists());
    assert!(!working.exists());
    assert!(!staging.exists());
    assert_eq!(stamp_of(&pipeline, job_id), None);
    retire_pipeline_database(pipeline).await;
}

// A script moved the delivery: its marker names the folder it was made in,
// so the new place is not proven Weaver's and stays.
#[tokio::test]
async fn a_relocated_output_dir_is_not_managed_and_kept() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40004);
    let delivered = owned_output_dir(&complete_dir, "Lantern", job_id);
    let moved = complete_dir.join("Library").join("Lantern");
    std::fs::create_dir_all(moved.parent().unwrap()).unwrap();
    std::fs::rename(&delivered, &moved).unwrap();
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "Lantern",
            "complete",
            moved.clone(),
        ))
        .unwrap();

    let outcome = remove_files(&mut pipeline, &[job_id], NOW).await;

    assert_eq!(outcome[&job_id].outcome, Outcome::NotManaged);
    assert!(moved.join("lantern.mkv").exists());
    assert_eq!(stamp_of(&pipeline, job_id), None);
    retire_pipeline_database(pipeline).await;
}

// A symbolic link in the output's place is never followed or removed.
#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_output_dir_is_not_managed_and_kept() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40005);
    let target = owned_output_dir(&complete_dir, "Real", job_id);
    let link = complete_dir.join("Link");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "Link",
            "complete",
            link.clone(),
        ))
        .unwrap();

    let outcome = remove_files(&mut pipeline, &[job_id], NOW).await;

    assert_eq!(outcome[&job_id].outcome, Outcome::NotManaged);
    assert!(link.symlink_metadata().is_ok());
    assert!(target.join("lantern.mkv").exists());
    retire_pipeline_database(pipeline).await;
}

// A job in the queue, and an entry another history operation holds, are
// busy and touched not at all; an unknown id is not found. Each id of a
// mixed batch gets its own outcome.
#[tokio::test]
async fn a_mixed_batch_returns_each_id_s_own_outcome() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let removed = JobId(40010);
    let held = JobId(40011);
    let active = JobId(40012);
    let unknown = JobId(40013);
    let unmanaged = JobId(40014);
    let removed_dir = owned_output_dir(&complete_dir, "Removed", removed);
    let held_dir = owned_output_dir(&complete_dir, "Held", held);
    let unmanaged_dir = complete_dir.join("Unmanaged");
    std::fs::create_dir_all(&unmanaged_dir).unwrap();
    for (job_id, dir) in [
        (removed, &removed_dir),
        (held, &held_dir),
        (unmanaged, &unmanaged_dir),
    ] {
        pipeline
            .db
            .insert_job_history(&history_row_with_output_dir(
                job_id,
                "Mixed",
                "complete",
                dir.clone(),
            ))
            .unwrap();
    }
    insert_active_job(
        &mut pipeline,
        active,
        standalone_job_spec("Active", &[("active.bin".into(), 1)]),
    )
    .await;
    pipeline.report_all_for_test();
    pipeline.shared_state.queue.write(|inner| {
        inner.history_operations.insert(held);
        crate::pipeline::queue::Change::unchanged(())
    });

    let outcome = remove_files(
        &mut pipeline,
        &[removed, held, active, unknown, unmanaged],
        NOW,
    )
    .await;

    assert_eq!(outcome[&removed].outcome, Outcome::Removed);
    assert_eq!(outcome[&held].outcome, Outcome::Busy);
    assert_eq!(outcome[&active].outcome, Outcome::Busy);
    assert_eq!(
        outcome[&unknown],
        HistoryFilesRemoval::new(Outcome::NotFound, None, None)
    );
    assert_eq!(outcome[&unmanaged].outcome, Outcome::NotManaged);
    assert_eq!(outcome.len(), 5);
    assert!(!removed_dir.exists());
    assert!(held_dir.join("lantern.mkv").exists());
    assert!(unmanaged_dir.exists());
    assert_eq!(stamp_of(&pipeline, removed), Some(NOW));
    assert_eq!(stamp_of(&pipeline, held), None);
    // The other operation's hold is still its own.
    assert!(pipeline.shared_state.queue.read(
        |inner| inner.history_operations.contains(&held) && inner.history_operations.len() == 1
    ));
    retire_pipeline_database(pipeline).await;
}

// An error part way is reported and stamps nothing. The ownership marker is
// the last thing a removal takes, so it is still there, and a retry proves
// the rest Weaver's and finishes. The record kept before the first attempt
// is of the whole folder.
#[tokio::test]
async fn a_failed_removal_is_unstamped_and_a_retry_finishes_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40020);
    let output = owned_output_dir(&complete_dir, "Locked", job_id);
    write_file(&output, "Sample/sample.mkv", 6);
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "Locked",
            "complete",
            output.clone(),
        ))
        .unwrap();

    crate::jobs::working_dir::inject_removal_failure(&output, true);
    let failed = remove_files(&mut pipeline, &[job_id], NOW).await;
    crate::jobs::working_dir::inject_removal_failure(&output, false);

    assert_eq!(failed[&job_id].outcome, Outcome::Failed);
    assert_eq!(failed[&job_id].files_removed_at, None);
    assert!(failed[&job_id].message.is_some());
    assert_eq!(stamp_of(&pipeline, job_id), None);
    assert!(
        output
            .join(crate::jobs::working_dir::OUTPUT_DIR_MARKER)
            .is_file()
    );
    let (rows, count, bytes) = record_of(&pipeline, job_id);
    assert_eq!((rows.len(), count, bytes), (2, Some(2), Some(15)));

    let retried = remove_files(&mut pipeline, &[job_id], NOW + 1).await;
    assert_eq!(
        retried[&job_id],
        HistoryFilesRemoval::new(Outcome::Removed, Some(NOW + 1), None)
    );
    assert!(!output.exists());
    assert_eq!(stamp_of(&pipeline, job_id), Some(NOW + 1));
    assert_eq!(record_of(&pipeline, job_id), (rows, count, bytes));
    retire_pipeline_database(pipeline).await;
}

// A folder that is not there while the folder that would hold it is also
// gone, as when a mount is offline, cannot be told gone: the removal fails
// and stamps nothing.
#[tokio::test]
async fn a_folder_whose_parent_is_missing_fails_and_is_not_stamped() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40021);
    let output = complete_dir.join("offline-mount").join("Lantern");
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "Offline",
            "complete",
            output.clone(),
        ))
        .unwrap();

    let outcome = remove_files(&mut pipeline, &[job_id], NOW).await;

    assert_eq!(outcome[&job_id].outcome, Outcome::Failed);
    assert_eq!(outcome[&job_id].files_removed_at, None);
    assert!(
        outcome[&job_id]
            .message
            .as_deref()
            .unwrap()
            .contains(&output.display().to_string())
    );
    assert_eq!(stamp_of(&pipeline, job_id), None);
    assert_eq!(record_of(&pipeline, job_id), NO_RECORD());
    retire_pipeline_database(pipeline).await;
}

// Job B delivered to the folder job A once delivered to: B's marker names
// B. Removing A's files leaves B's and answers not managed; B's own removal
// takes them. A delete with files of A leaves them too.
#[tokio::test]
async fn a_folder_another_job_delivered_to_is_not_managed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let gone = JobId(40060);
    let current = JobId(40061);
    let output = owned_output_dir(&complete_dir, "Shared", current);
    for job_id in [gone, current] {
        pipeline
            .db
            .insert_job_history(&history_row_with_output_dir(
                job_id,
                "Shared",
                "complete",
                output.clone(),
            ))
            .unwrap();
    }

    let outcome = remove_files(&mut pipeline, &[gone], NOW).await;
    assert_eq!(outcome[&gone].outcome, Outcome::NotManaged);
    assert!(output.join("lantern.mkv").is_file());
    assert_eq!(stamp_of(&pipeline, gone), None);

    let deleted = pipeline
        .shared_state
        .queue
        .clone()
        .delete_history([gone].into_iter().collect(), true)
        .await
        .unwrap();
    assert_eq!(deleted.deleted.len(), 1);
    assert!(output.join("lantern.mkv").is_file());

    let outcome = remove_files(&mut pipeline, &[current], NOW).await;
    assert_eq!(outcome[&current].outcome, Outcome::Removed);
    assert!(!output.exists());
    retire_pipeline_database(pipeline).await;
}

// A marker from before markers named their job proves the folder only when
// no other job records it.
#[tokio::test]
async fn a_legacy_marker_is_removable_only_when_no_other_job_records_the_folder() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let unique = JobId(40070);
    let first = JobId(40071);
    let second = JobId(40072);
    let legacy = |name: &str| {
        let dir = complete_dir.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("lantern.mkv"), b"delivered").unwrap();
        crate::jobs::working_dir::mark_legacy_output_dir(&dir).unwrap();
        dir
    };
    let unique_dir = legacy("Unique");
    let shared_dir = legacy("Shared");
    for (job_id, dir) in [
        (unique, &unique_dir),
        (first, &shared_dir),
        (second, &shared_dir),
    ] {
        pipeline
            .db
            .insert_job_history(&history_row_with_output_dir(
                job_id,
                "Legacy",
                "complete",
                dir.clone(),
            ))
            .unwrap();
    }

    let outcome = remove_files(&mut pipeline, &[unique, first], NOW).await;

    assert_eq!(outcome[&unique].outcome, Outcome::Removed);
    assert!(!unique_dir.exists());
    assert_eq!(outcome[&first].outcome, Outcome::NotManaged);
    assert!(shared_dir.join("lantern.mkv").is_file());
    assert_eq!(stamp_of(&pipeline, first), None);
    retire_pipeline_database(pipeline).await;
}

// The job's own working directory inside the intermediate folder goes even
// without a marker, and the entry is stamped.
#[tokio::test]
async fn an_unmarked_working_dir_is_removed_by_a_files_removal() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40080);
    let working = intermediate_dir.join("Unmarked");
    write_file(&working, "lantern.mkv", 9);
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "Unmarked",
            "failed",
            working.clone(),
        ))
        .unwrap();

    let outcome = remove_files(&mut pipeline, &[job_id], NOW).await;

    assert_eq!(outcome[&job_id].outcome, Outcome::Removed, "{outcome:?}");
    assert!(!working.exists());
    assert_eq!(stamp_of(&pipeline, job_id), Some(NOW));
    retire_pipeline_database(pipeline).await;
}

// With the complete folder set to the intermediate folder, a recorded
// delivery folder that lost its marker is not taken for a working folder:
// it stays, and the entry is not stamped.
#[tokio::test]
async fn an_unmarked_output_dir_under_the_intermediate_dir_is_kept() {
    let temp_dir = tempfile::tempdir().unwrap();
    let shared = temp_dir.path().join("shared");
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline_at_roots(
        temp_dir.path().join("data"),
        shared.clone(),
        shared,
        temp_dir.path().join("weaver.db"),
        16 * 1024 * 1024,
        0,
        None,
    )
    .await;
    assert_eq!(intermediate_dir, complete_dir);
    let job_id = JobId(40081);
    let delivery = complete_dir.join("Replaced");
    write_file(&delivery, "someone-else.mkv", 6);
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "Replaced",
            "complete",
            delivery.clone(),
        ))
        .unwrap();

    let outcome = remove_files(&mut pipeline, &[job_id], NOW).await;

    assert_eq!(outcome[&job_id].outcome, Outcome::NotManaged, "{outcome:?}");
    assert!(delivery.join("someone-else.mkv").is_file());
    assert_eq!(stamp_of(&pipeline, job_id), None);
    retire_pipeline_database(pipeline).await;
}

// A job that finishes again has its files again: the archive writes the
// entry with no removal time and forgets the record of the removed files.
#[tokio::test]
async fn finishing_again_clears_the_stamp() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40030);
    let output = owned_output_dir(&complete_dir, "Again", job_id);
    let row = history_row_with_output_dir(job_id, "Again", "complete", output.clone());
    pipeline.db.insert_job_history(&row).unwrap();
    remove_files(&mut pipeline, &[job_id], NOW).await;
    assert_eq!(stamp_of(&pipeline, job_id), Some(NOW));
    assert_eq!(
        record_of(&pipeline, job_id),
        (vec![("lantern.mkv".to_string(), 9)], Some(1), Some(9))
    );

    // Even a row that arrives stamped is written without the stamp.
    pipeline
        .db
        .archive_job(
            job_id,
            &crate::JobHistoryRow {
                files_removed_at: Some(NOW),
                ..row
            },
        )
        .unwrap();

    assert_eq!(stamp_of(&pipeline, job_id), None);
    assert_eq!(record_of(&pipeline, job_id), NO_RECORD());
    retire_pipeline_database(pipeline).await;
}

// Writing the entry again, as a status change does, forgets the record when
// the row says the files are back.
#[tokio::test]
async fn rewriting_the_entry_without_a_stamp_clears_the_record() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40031);
    let output = owned_output_dir(&complete_dir, "Rewritten", job_id);
    let row = history_row_with_output_dir(job_id, "Rewritten", "complete", output);
    pipeline.db.insert_job_history(&row).unwrap();
    remove_files(&mut pipeline, &[job_id], NOW).await;

    // Written again with the stamp, the record stays.
    pipeline
        .db
        .insert_job_history(&crate::JobHistoryRow {
            files_removed_at: Some(NOW),
            ..row.clone()
        })
        .unwrap();
    assert_eq!(
        record_of(&pipeline, job_id),
        (vec![("lantern.mkv".to_string(), 9)], Some(1), Some(9))
    );

    pipeline.db.insert_job_history(&row).unwrap();
    assert_eq!(stamp_of(&pipeline, job_id), None);
    assert_eq!(record_of(&pipeline, job_id), NO_RECORD());
    retire_pipeline_database(pipeline).await;
}

// The delete paths keep their behaviour: without files a marked delivery
// stays, with files it goes, and an unmarked one stays either way.
#[tokio::test]
async fn deleting_history_still_takes_only_what_it_took() {
    for delete_files in [false, true] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
        let marked = JobId(40040);
        let unmarked = JobId(40041);
        let marked_dir = owned_output_dir(&complete_dir, "Marked", marked);
        let unmarked_dir = complete_dir.join("Unmarked");
        std::fs::create_dir_all(&unmarked_dir).unwrap();
        for (job_id, dir) in [(marked, &marked_dir), (unmarked, &unmarked_dir)] {
            pipeline
                .db
                .insert_job_history(&history_row_with_output_dir(
                    job_id,
                    "Delete",
                    "complete",
                    dir.clone(),
                ))
                .unwrap();
        }
        pipeline.report_all_for_test();

        let outcome = pipeline
            .shared_state
            .queue
            .clone()
            .delete_history([marked, unmarked].into_iter().collect(), delete_files)
            .await
            .unwrap();

        assert!(outcome.refused.is_empty());
        assert_eq!(outcome.deleted.len(), 2);
        assert_eq!(marked_dir.exists(), !delete_files);
        assert!(unmarked_dir.exists());
        for job_id in [marked, unmarked] {
            assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
        }
        retire_pipeline_database(pipeline).await;
    }
}

// The removed folder is recorded: each file by its path relative to the
// folder, in the listing order, with its size, and the totals of all of it.
// Weaver's ownership marker is not a file of the job.
#[tokio::test]
async fn the_removed_folder_is_recorded_in_listing_order_with_its_totals() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40050);
    let output = owned_output_dir(&complete_dir, "Recorded", job_id);
    write_file(&output, "B.nfo", 2);
    write_file(&output, "Sub/a.txt", 3);
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "Recorded",
            "complete",
            output.clone(),
        ))
        .unwrap();

    let outcome = remove_files(&mut pipeline, &[job_id], NOW).await;

    assert_eq!(outcome[&job_id].outcome, Outcome::Removed);
    assert!(!output.exists());
    let sep = std::path::MAIN_SEPARATOR;
    assert_eq!(
        record_of(&pipeline, job_id),
        (
            vec![
                (format!("Sub{sep}a.txt"), 3),
                ("B.nfo".to_string(), 2),
                ("lantern.mkv".to_string(), 9),
            ],
            Some(3),
            Some(14),
        )
    );
    // Pages of the record are ranges of it.
    assert_eq!(
        pipeline
            .db
            .list_history_removed_files(job_id.0, 1, 1)
            .unwrap(),
        vec![("B.nfo".to_string(), 2)]
    );
    assert!(pipeline.db.has_history_removed_file(job_id.0, 2).unwrap());
    assert!(!pipeline.db.has_history_removed_file(job_id.0, 3).unwrap());
    retire_pipeline_database(pipeline).await;
}

// A folder larger than the cap keeps only its first files, and its totals
// still count every file.
#[tokio::test]
async fn the_record_keeps_the_first_files_up_to_the_cap_and_whole_totals() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40051);
    let output = owned_output_dir(&complete_dir, "Capped", job_id);
    for (name, bytes) in [("d.bin", 4), ("a.bin", 1), ("c.bin", 3), ("b.bin", 2)] {
        write_file(&output, name, bytes);
    }
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "Capped",
            "complete",
            output.clone(),
        ))
        .unwrap();

    let outcome = remove_files_keeping(&mut pipeline, &[job_id], NOW, 2).await;

    assert_eq!(outcome[&job_id].outcome, Outcome::Removed);
    assert_eq!(
        record_of(&pipeline, job_id),
        (
            vec![("a.bin".to_string(), 1), ("b.bin".to_string(), 2)],
            Some(5),
            Some(19),
        )
    );
    retire_pipeline_database(pipeline).await;
}

// A link inside the folder is neither recorded nor walked, and what it
// points at stays.
#[cfg(unix)]
#[tokio::test]
async fn the_record_does_not_follow_symbolic_links() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40052);
    let output = owned_output_dir(&complete_dir, "Linked", job_id);
    let outside = temp_dir.path().join("outside");
    write_file(&outside, "elsewhere.mkv", 7);
    std::os::unix::fs::symlink(&outside, output.join("folder-link")).unwrap();
    std::os::unix::fs::symlink(outside.join("elsewhere.mkv"), output.join("file-link.mkv"))
        .unwrap();
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "Linked",
            "complete",
            output.clone(),
        ))
        .unwrap();

    let outcome = remove_files(&mut pipeline, &[job_id], NOW).await;

    assert_eq!(outcome[&job_id].outcome, Outcome::Removed);
    assert_eq!(
        record_of(&pipeline, job_id),
        (vec![("lantern.mkv".to_string(), 9)], Some(1), Some(9))
    );
    assert!(outside.join("elsewhere.mkv").is_file());
    retire_pipeline_database(pipeline).await;
}

// A folder that is not Weaver's, or is not there, is not removed and no
// record is kept of it.
#[tokio::test]
async fn an_unmanaged_or_absent_folder_keeps_no_record() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let unmanaged = JobId(40053);
    let absent = JobId(40054);
    let unmanaged_dir = complete_dir.join("Theirs");
    write_file(&unmanaged_dir, "theirs.mkv", 6);
    for (job_id, dir) in [
        (unmanaged, unmanaged_dir.clone()),
        (absent, complete_dir.join("never-delivered")),
    ] {
        pipeline
            .db
            .insert_job_history(&history_row_with_output_dir(
                job_id, "None", "complete", dir,
            ))
            .unwrap();
    }

    let outcome = remove_files(&mut pipeline, &[unmanaged, absent], NOW).await;

    assert_eq!(outcome[&unmanaged].outcome, Outcome::NotManaged);
    assert_eq!(outcome[&absent].outcome, Outcome::AlreadyGone);
    assert_eq!(record_of(&pipeline, unmanaged), NO_RECORD());
    assert_eq!(record_of(&pipeline, absent), NO_RECORD());
    assert!(unmanaged_dir.join("theirs.mkv").is_file());
    retire_pipeline_database(pipeline).await;
}

// Deleting the entry deletes the record with it.
#[tokio::test]
async fn deleting_the_entry_deletes_its_record() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40055);
    let kept = JobId(40056);
    for (id, name) in [(job_id, "Deleted"), (kept, "Kept")] {
        let output = owned_output_dir(&complete_dir, name, id);
        pipeline
            .db
            .insert_job_history(&history_row_with_output_dir(id, name, "complete", output))
            .unwrap();
    }
    remove_files(&mut pipeline, &[job_id, kept], NOW).await;
    assert_eq!(removed_rows(&pipeline, job_id).len(), 1);

    let outcome = pipeline
        .shared_state
        .queue
        .clone()
        .delete_history([job_id].into_iter().collect(), false)
        .await
        .unwrap();

    assert_eq!(outcome.deleted.len(), 1);
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
    assert!(removed_rows(&pipeline, job_id).is_empty());
    assert_eq!(removed_rows(&pipeline, kept).len(), 1);
    retire_pipeline_database(pipeline).await;
}
