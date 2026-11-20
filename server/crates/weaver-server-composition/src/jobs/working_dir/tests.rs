// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::settings::persistence::SettingsPersistenceQueries;
use crate::settings::queries::SettingsQueries;

#[test]
fn working_directory_ownership_is_bound_to_job_and_directory() {
    let temp = tempfile::tempdir().unwrap();
    let owned = temp.path().join("owned");
    let other = temp.path().join("other");
    std::fs::create_dir(&owned).unwrap();
    std::fs::create_dir(&other).unwrap();
    mark_weaver_owned_working_dir(temp.path(), &owned, JobId(7)).unwrap();
    assert!(is_weaver_owned_working_dir(&owned));
    assert!(mark_weaver_owned_working_dir(temp.path(), &owned, JobId(8)).is_err());
    std::fs::copy(
        working_dir_marker_path(&owned),
        working_dir_marker_path(&other),
    )
    .unwrap();
    assert!(!is_weaver_owned_working_dir(&other));
    assert!(remove_weaver_owned_working_dir(temp.path(), &other).is_err());
    assert!(other.exists());
    remove_weaver_owned_working_dir(temp.path(), &owned).unwrap();
    assert!(!owned.exists());
}

#[tokio::test]
async fn history_cleanup_rejects_a_valid_marker_for_a_replacement_job() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("job");
    std::fs::create_dir(&path).unwrap();
    mark_weaver_owned_working_dir(temp.path(), &path, JobId(7)).unwrap();
    assert_eq!(
        prepare_history_working_dir(temp.path(), &path, JobId(7))
            .await
            .unwrap(),
        HistoryWorkingDir::Owned
    );
    std::fs::rename(&path, temp.path().join("previous")).unwrap();
    std::fs::create_dir(&path).unwrap();
    mark_weaver_owned_working_dir(temp.path(), &path, JobId(8)).unwrap();
    std::fs::write(path.join("payload"), b"replacement").unwrap();
    assert!(remove_job_working_dir(temp.path(), &path, JobId(7)).is_err());
    assert_eq!(std::fs::read(path.join("payload")).unwrap(), b"replacement");
    remove_job_working_dir(temp.path(), &path, JobId(8)).unwrap();
    assert!(!path.exists());
}

#[cfg(unix)]
// Writes a v1 marker for `path` computed with `dev`, the way a binary that
// still hashed the device number would have.
fn write_v1_marker(path: &Path, dev: Option<u64>, job_id: JobId) {
    let ino = std::fs::metadata(path).unwrap().ino();
    std::fs::write(
        working_dir_marker_path(path),
        format!(
            "{MARKER_V1_PREFIX}{}:{}\n",
            job_id.0,
            marker_hash(path, dev, ino, job_id)
        ),
    )
    .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn a_v1_marker_for_the_same_directory_is_accepted_and_rewritten_as_v2() {
    let temp = tempfile::tempdir().unwrap();
    for (name, with_dev) in [("current-dev", true), ("no-dev", false)] {
        let path = temp.path().join(name);
        std::fs::create_dir(&path).unwrap();
        let dev = with_dev.then(|| std::fs::metadata(&path).unwrap().dev());
        write_v1_marker(&path, dev, JobId(21));

        assert_eq!(
            prepare_history_working_dir(temp.path(), &path, JobId(21))
                .await
                .unwrap(),
            HistoryWorkingDir::Owned,
            "{name}: a v1 marker this directory could have written is its owner"
        );
        let rewritten = std::fs::read_to_string(working_dir_marker_path(&path)).unwrap();
        assert!(
            rewritten.starts_with(MARKER_V2_PREFIX),
            "{name}: an accepted v1 marker is rewritten as v2, got {rewritten:?}"
        );
        assert!(is_weaver_owned_working_dir(&path));
        assert_eq!(
            remove_job_working_dir(temp.path(), &path, JobId(21)).unwrap(),
            HistoryWorkingDir::Owned
        );
        assert!(!path.exists());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn a_temporary_left_by_an_interrupted_upgrade_does_not_block_the_next_one() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("interrupted");
    std::fs::create_dir(&path).unwrap();
    write_v1_marker(&path, None, JobId(25));
    std::fs::write(path.join(WORKING_DIR_MARKER_UPGRADE), b"partial").unwrap();

    assert_eq!(
        prepare_history_working_dir(temp.path(), &path, JobId(25))
            .await
            .unwrap(),
        HistoryWorkingDir::Owned
    );
    let rewritten = std::fs::read_to_string(working_dir_marker_path(&path)).unwrap();
    assert!(
        rewritten.starts_with(MARKER_V2_PREFIX),
        "the upgrade completes over the stale temporary, got {rewritten:?}"
    );
    assert!(
        !path.join(WORKING_DIR_MARKER_UPGRADE).exists(),
        "the temporary is consumed by the upgrade"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn an_upgrade_that_cannot_write_keeps_the_v1_marker() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("unwritable");
    std::fs::create_dir(&path).unwrap();
    write_v1_marker(&path, None, JobId(26));
    let original = std::fs::read_to_string(working_dir_marker_path(&path)).unwrap();
    // A directory where the temporary goes cannot be removed as a file,
    // so the upgrade fails before it has written anything.
    std::fs::create_dir(path.join(WORKING_DIR_MARKER_UPGRADE)).unwrap();

    assert!(
        prepare_history_working_dir(temp.path(), &path, JobId(26))
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(working_dir_marker_path(&path)).unwrap(),
        original,
        "a failed upgrade leaves the v1 marker as it was"
    );
    let dir = open_working_directory(temp.path(), &path).unwrap();
    assert_eq!(
        match_marker(&dir, &path, &original, JobId(26)).unwrap(),
        MarkerMatch::Legacy
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_marker_for_this_job_that_no_longer_matches_leaves_the_directory_in_place() {
    // A v1 marker written under a device number the filesystem has since
    // renumbered: it names the right job, and its hash cannot be
    // reproduced from the directory as it is now.
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("renumbered");
    std::fs::create_dir(&path).unwrap();
    let dev = std::fs::metadata(&path).unwrap().dev();
    write_v1_marker(&path, Some(dev.wrapping_add(1)), JobId(22));
    std::fs::write(path.join("payload"), b"kept").unwrap();

    assert_eq!(
        prepare_history_working_dir(temp.path(), &path, JobId(22))
            .await
            .unwrap(),
        HistoryWorkingDir::LeftInPlace
    );
    assert_eq!(
        remove_job_working_dir(temp.path(), &path, JobId(22)).unwrap(),
        HistoryWorkingDir::LeftInPlace
    );
    assert_eq!(std::fs::read(path.join("payload")).unwrap(), b"kept");
}

#[cfg(unix)]
#[tokio::test]
async fn a_marker_naming_another_job_is_still_refused() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("foreign");
    std::fs::create_dir(&path).unwrap();
    write_v1_marker(&path, None, JobId(23));
    std::fs::write(path.join("payload"), b"not yours").unwrap();

    assert!(
        prepare_history_working_dir(temp.path(), &path, JobId(24))
            .await
            .is_err()
    );
    assert!(remove_job_working_dir(temp.path(), &path, JobId(24)).is_err());
    assert_eq!(std::fs::read(path.join("payload")).unwrap(), b"not yours");
}

fn staging_under(destination: &Path, job_id: JobId) -> PathBuf {
    destination
        .join(STAGING_DIR_NAME)
        .join(job_id.0.to_string())
}

#[test]
fn a_job_s_staging_directory_is_removed_with_what_it_holds() {
    let temp = tempfile::tempdir().unwrap();
    let staging = staging_under(temp.path(), JobId(31));
    make_job_staging_dir(&staging, JobId(31)).unwrap();
    // The marker alone is not a delivered file.
    assert!(!job_staging_dir_holds_files(&staging, JobId(31)).unwrap());
    std::fs::write(staging.join("payload"), b"staged").unwrap();
    assert!(job_staging_dir_holds_files(&staging, JobId(31)).unwrap());

    assert_eq!(
        remove_job_staging_dir(&staging, JobId(31)).unwrap(),
        HistoryWorkingDir::Owned
    );
    assert!(!staging.exists());
    // One already gone is removed.
    assert_eq!(
        remove_job_staging_dir(&staging, JobId(31)).unwrap(),
        HistoryWorkingDir::Owned
    );
}

#[test]
fn a_staging_directory_that_is_not_the_job_s_keeps_its_files() {
    let temp = tempfile::tempdir().unwrap();
    // Never marked, and holding a file.
    let unmarked = staging_under(temp.path(), JobId(32));
    std::fs::create_dir_all(&unmarked).unwrap();
    std::fs::write(unmarked.join("payload"), b"not yours").unwrap();
    assert_eq!(
        remove_job_staging_dir(&unmarked, JobId(32)).unwrap(),
        HistoryWorkingDir::LeftInPlace
    );
    assert_eq!(
        std::fs::read(unmarked.join("payload")).unwrap(),
        b"not yours"
    );

    // Never marked and empty: nothing is lost by taking it.
    let empty = staging_under(temp.path(), JobId(33));
    std::fs::create_dir_all(&empty).unwrap();
    assert_eq!(
        remove_job_staging_dir(&empty, JobId(33)).unwrap(),
        HistoryWorkingDir::Owned
    );
    assert!(!empty.exists());

    // A path that is not `<destination>/.weaver-staging/<job id>`.
    let elsewhere = temp.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    assert!(remove_job_staging_dir(&elsewhere, JobId(34)).is_err());
    assert!(make_job_staging_dir(&elsewhere, JobId(34)).is_err());
    assert!(elsewhere.exists());
}

#[cfg(unix)]
#[test]
fn a_staging_parent_that_is_a_symbolic_link_is_never_followed() {
    let temp = tempfile::tempdir().unwrap();
    let job_id = JobId(35);
    // Another location holds a directory named for the job, with this
    // job's own valid marker inside it, and the staging parent is swapped
    // for a link to that location.
    let victim = temp.path().join("victim");
    let victim_job = staging_under(&victim, job_id);
    make_job_staging_dir(&victim_job, job_id).unwrap();
    std::fs::write(victim_job.join("payload"), b"keep").unwrap();
    let destination = temp.path().join("complete");
    std::fs::create_dir_all(&destination).unwrap();
    std::os::unix::fs::symlink(
        victim.join(STAGING_DIR_NAME),
        destination.join(STAGING_DIR_NAME),
    )
    .unwrap();
    let staging = staging_under(&destination, job_id);

    let error = remove_job_staging_dir(&staging, job_id).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(std::fs::read(victim_job.join("payload")).unwrap(), b"keep");
    // Nor is what the link points at taken for the job's delivered files.
    assert!(job_staging_dir_holds_files(&victim_job, job_id).unwrap());
    assert!(job_staging_dir_holds_files(&staging, job_id).is_err());
    assert!(make_job_staging_dir(&staging, job_id).is_err());
}

#[cfg(unix)]
#[test]
fn a_staging_directory_that_is_a_symbolic_link_is_never_followed() {
    let temp = tempfile::tempdir().unwrap();
    let job_id = JobId(36);
    let victim = temp.path().join("victim");
    std::fs::create_dir_all(&victim).unwrap();
    std::fs::write(victim.join("payload"), b"keep").unwrap();
    let staging = staging_under(temp.path(), job_id);
    std::fs::create_dir_all(staging.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink(&victim, &staging).unwrap();

    let error = remove_job_staging_dir(&staging, job_id).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(job_staging_dir_holds_files(&staging, job_id).is_err());
    assert!(make_job_staging_dir(&staging, job_id).is_err());
    assert_eq!(std::fs::read(victim.join("payload")).unwrap(), b"keep");
    assert!(!victim.join(WORKING_DIR_MARKER).exists());
}

#[test]
fn sanitize_dirname_bounds_long_names() {
    let sanitized = sanitize_dirname(&"a".repeat(400));

    assert!(sanitized.len() <= weaver_model::files::DOWNLOAD_FILENAME_MAX_BYTES);
}

#[test]
fn sanitize_dirname_disarms_windows_device_names() {
    assert_eq!(sanitize_dirname("CON"), "_CON");
}

#[test]
fn compute_working_dir_bounds_collision_suffix() {
    let temp = tempfile::tempdir().unwrap();
    let long_name = "a".repeat(400);
    let original = compute_working_dir(temp.path(), JobId(42), &long_name);
    std::fs::create_dir(&original).unwrap();

    let suffixed = compute_working_dir(temp.path(), JobId(42), &long_name);
    let file_name = suffixed.file_name().unwrap().to_string_lossy();

    assert!(file_name.ends_with(".#42"));
    assert!(file_name.len() <= weaver_model::files::DOWNLOAD_FILENAME_MAX_BYTES);
}

#[test]
fn a_claim_takes_the_plain_name_once_and_the_jobs_own_name_after() {
    let temp = tempfile::tempdir().unwrap();
    let first = claim_working_dir(temp.path(), JobId(1), "same name").unwrap();
    let second = claim_working_dir(temp.path(), JobId(2), "same name").unwrap();
    let again = claim_working_dir(temp.path(), JobId(2), "same name").unwrap();

    assert_eq!(first.path, temp.path().join("same name"));
    assert!(!first.existed);
    assert_eq!(second.path, temp.path().join("same name.#2"));
    assert!(!second.existed);
    assert_eq!(again.path, second.path);
    assert!(again.existed);
    mark_weaver_owned_working_dir(temp.path(), &first.path, JobId(1)).unwrap();
    mark_weaver_owned_working_dir(temp.path(), &second.path, JobId(2)).unwrap();
}

#[test]
fn concurrent_claims_of_one_name_never_share_a_directory() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().to_path_buf();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let claims = [JobId(7), JobId(8)].map(|job_id| {
        let root = root.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            let claimed = claim_working_dir(&root, job_id, "same name").unwrap();
            mark_weaver_owned_working_dir(&root, &claimed.path, job_id).unwrap();
            claimed
        })
    });
    let [first, second] = claims.map(|claim| claim.join().unwrap());

    assert_ne!(first.path, second.path);
    assert!(!first.existed);
    assert!(!second.existed);
    assert!(is_weaver_owned_working_dir(&first.path));
    assert!(is_weaver_owned_working_dir(&second.path));
}

#[test]
fn output_ownership_marker_is_bound_to_its_directory() {
    let temp = tempfile::tempdir().unwrap();
    let owned = temp.path().join("owned");
    let copied = temp.path().join("copied");
    std::fs::create_dir(&owned).unwrap();
    std::fs::create_dir(&copied).unwrap();
    mark_weaver_owned_output_dir(&owned, JobId(7)).unwrap();

    assert!(is_weaver_owned_output_dir(&owned));
    assert_eq!(
        output_dir_owner(&owned),
        Some(OutputDirOwner::Job(JobId(7)))
    );
    std::fs::copy(
        owned.join(OUTPUT_DIR_MARKER),
        copied.join(OUTPUT_DIR_MARKER),
    )
    .unwrap();
    assert!(!is_weaver_owned_output_dir(&copied));
}

#[test]
fn a_marker_names_its_job_and_a_legacy_one_names_none() {
    let temp = tempfile::tempdir().unwrap();
    let legacy = temp.path().join("legacy");
    std::fs::create_dir(&legacy).unwrap();
    mark_legacy_output_dir(&legacy).unwrap();
    assert_eq!(output_dir_owner(&legacy), Some(OutputDirOwner::Legacy));

    let owner = OutputDirOwner::Job(JobId(7));
    assert!(owner.admits(JobId(7), false));
    assert!(!owner.admits(JobId(8), true));
    assert!(OutputDirOwner::Legacy.admits(JobId(8), true));
    assert!(!OutputDirOwner::Legacy.admits(JobId(8), false));
}

#[test]
fn rename_journal_recovers_before_and_after_the_directory_move() {
    for moved in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let old = root.join("original");
        let new = root.join("renamed");
        let id = JobId(9010);
        let db = crate::Database::open_in_memory().unwrap();
        std::fs::create_dir(&old).unwrap();
        mark_weaver_owned_working_dir(root, &old, id).unwrap();
        std::fs::write(old.join("payload"), b"kept").unwrap();
        db.set_setting(
            &rename_journal_key(id),
            &serde_json::to_string(&(&old, &new)).unwrap(),
        )
        .unwrap();
        if moved {
            weaver_server_disk::runtime::fs::rename_path_exclusive(&old, &new).unwrap();
        }
        assert_eq!(
            recover_working_dir_rename(&db, root, &old, id).unwrap(),
            new
        );
        assert!(!old.exists());
        assert_eq!(std::fs::read(new.join("payload")).unwrap(), b"kept");
        assert!(is_weaver_owned_working_dir(&new));
        assert!(db.get_setting(&rename_journal_key(id)).unwrap().is_none());
        assert_eq!(
            recover_working_dir_rename(&db, root, &new, id).unwrap(),
            new
        );
    }
}

// A missing directory under a root that is not there, as under a mount
// that is not up yet, is not claimed again: nothing makes the root.
// With the root there, it is.
#[test]
fn a_reclaim_under_the_same_root_never_makes_the_root() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("mount");
    let current = root.join("job");

    assert!(
        follow_intermediate_dir(&root, &current, JobId(3), "job")
            .unwrap()
            .is_none()
    );
    assert!(!root.exists());

    std::fs::create_dir(&root).unwrap();
    let followed = follow_intermediate_dir(&root, &current, JobId(3), "job")
        .unwrap()
        .unwrap();
    assert!(is_job_working_dir(&root, &followed.path, JobId(3)).unwrap());
}

// A claim given up removes what it made, and spares a directory that
// was there before it under the job's own suffixed name.
#[test]
fn a_claim_given_up_spares_a_directory_that_was_there() {
    let temp = tempfile::tempdir().unwrap();
    let (old_root, root) = (temp.path().join("old"), temp.path().join("new"));
    let made = follow_intermediate_dir(&root, &old_root.join("job"), JobId(4), "job")
        .unwrap()
        .unwrap();
    let made_path = made.path.clone();
    made.abandon();
    assert!(!made_path.exists());

    std::fs::create_dir(root.join("job")).unwrap();
    let earlier = suffixed_working_dir(&root, "job", JobId(4));
    std::fs::create_dir(&earlier).unwrap();
    std::fs::write(earlier.join("kept.bin"), b"kept").unwrap();
    let found = follow_intermediate_dir(&root, &old_root.join("job"), JobId(4), "job")
        .unwrap()
        .unwrap();
    assert_eq!(found.path, earlier);
    found.abandon();
    assert!(earlier.join("kept.bin").exists());
}

// A claim given up after a removal of what it made was cut short, its
// marker gone and the folder left, still leaves no folder; one that
// holds anything but its marker is not the claim's to remove.
#[test]
fn a_claim_given_up_removes_its_folder_with_or_without_its_marker() {
    let temp = tempfile::tempdir().unwrap();
    let (old_root, root) = (temp.path().join("old"), temp.path().join("new"));
    let old = claim_working_dir(&old_root, JobId(6), "job").unwrap().path;

    let unmarked = follow_intermediate_dir(&root, &old, JobId(6), "job")
        .unwrap()
        .unwrap();
    let path = unmarked.path.clone();
    std::fs::remove_file(working_dir_marker_path(&path)).unwrap();
    unmarked.abandon();
    assert!(!path.exists(), "the unmarked folder the claim made is left");

    let marked = follow_intermediate_dir(&root, &old, JobId(6), "job")
        .unwrap()
        .unwrap();
    let path = marked.path.clone();
    marked.abandon();
    assert!(!path.exists(), "the marked folder the claim made is left");

    let filled = follow_intermediate_dir(&root, &old, JobId(6), "job")
        .unwrap()
        .unwrap();
    let path = filled.path.clone();
    std::fs::write(path.join("other.bin"), b"other").unwrap();
    filled.abandon();
    assert!(path.join("other.bin").exists());
}

// While a move is out, the directory the job had is removed whole by
// whoever removes it, as a cancel does: the move holds no handle to it.
// Given up, the move leaves nothing of it; a move that keeps going and
// finds it gone removes nothing, and finds a directory made there since
// with another's marker not its to remove.
#[test]
fn a_move_that_is_out_holds_nothing_open_of_the_directory_it_leaves() {
    let temp = tempfile::tempdir().unwrap();
    let (old_root, root) = (temp.path().join("old"), temp.path().join("new"));
    let old = claim_working_dir(&old_root, JobId(9), "job").unwrap().path;

    let followed = follow_intermediate_dir(&root, &old, JobId(9), "job")
        .unwrap()
        .unwrap();
    std::fs::remove_dir_all(&old).unwrap();
    assert!(!old.exists());
    followed.abandon();
    assert!(!old.exists());

    let old = claim_working_dir(&old_root, JobId(9), "job").unwrap().path;
    let followed = follow_intermediate_dir(&root, &old, JobId(9), "job")
        .unwrap()
        .unwrap();
    std::fs::remove_dir_all(&old).unwrap();
    followed.leave_old();
    assert!(!old.exists());

    let old = claim_working_dir(&old_root, JobId(9), "job").unwrap().path;
    let followed = follow_intermediate_dir(&root, &old, JobId(9), "job")
        .unwrap()
        .unwrap();
    std::fs::remove_dir_all(&old).unwrap();
    let other = claim_working_dir(&old_root, JobId(10), "job").unwrap().path;
    assert_eq!(other, old);
    followed.leave_old();
    assert!(is_job_working_dir(&old_root, &other, JobId(10)).unwrap());
}

// A root that is not there cannot be read, and says so; a root that is
// names each marked directory by its job and passes what is no
// directory.
#[test]
fn a_scan_of_a_missing_root_is_an_error_not_an_empty_reading() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("intermediate");
    assert!(scan_working_dirs(&root).is_err());

    let claimed = claim_working_dir(&root, JobId(5), "job").unwrap();
    std::fs::create_dir(root.join("unmarked")).unwrap();
    std::fs::write(root.join("a-file"), b"x").unwrap();
    let scan = scan_working_dirs(&root).unwrap();
    assert_eq!(scan.marked, vec![(JobId(5), claimed.path)]);
    assert_eq!(scan.unreadable, 0);
}

// A folder the service may not open, and a marker that is no regular
// file, are no working directory of ours: a reading passes them and is
// still whole. Any other failure counts against the reading.
#[test]
fn an_entry_that_is_not_ours_to_judge_does_not_count_against_a_reading() {
    use std::io::ErrorKind;
    assert!(not_ours_to_judge(ErrorKind::PermissionDenied));
    assert!(not_ours_to_judge(ErrorKind::InvalidData));
    for kind in [
        ErrorKind::Other,
        ErrorKind::TimedOut,
        ErrorKind::Interrupted,
        ErrorKind::UnexpectedEof,
        ErrorKind::NotFound,
    ] {
        assert!(!not_ours_to_judge(kind), "{kind:?}");
    }

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("intermediate");
    let claimed = claim_working_dir(&root, JobId(6), "job").unwrap();
    std::fs::create_dir_all(root.join("system-folder").join(WORKING_DIR_MARKER)).unwrap();
    let scan = scan_working_dirs(&root).unwrap();
    assert_eq!(scan.marked, vec![(JobId(6), claimed.path)]);
    assert_eq!(scan.unreadable, 0);
}

// A directory found unclaimed goes only while its marker still names
// the job it was judged by: one replaced since by another job's stays.
#[test]
fn an_unclaimed_directory_that_changed_hands_is_left() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("intermediate");
    let orphan = claim_working_dir(&root, JobId(7), "job").unwrap().path;
    // Between the reading and the removal the orphan goes and another
    // job claims the name.
    std::fs::remove_dir_all(&orphan).unwrap();
    let taken = claim_working_dir(&root, JobId(8), "job").unwrap().path;
    assert_eq!(taken, orphan);
    std::fs::write(taken.join("payload.bin"), b"kept").unwrap();

    assert!(!remove_unclaimed_working_dir(&root, &orphan, JobId(7)));
    assert!(taken.join("payload.bin").exists());
    assert!(remove_unclaimed_working_dir(&root, &taken, JobId(8)));
    assert!(!taken.exists());
}

// A job added to work in a directory it names has it made and marked in
// one step, and is told whether it was there.
#[test]
fn a_named_claim_makes_and_marks_the_directory() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("intermediate");
    let path = root.join("named");
    assert!(!claim_named_working_dir(&root, &path, JobId(6)).unwrap());
    assert!(is_job_working_dir(&root, &path, JobId(6)).unwrap());
    assert!(claim_named_working_dir(&root, &path, JobId(6)).unwrap());
}

// An NZB job named "torrent-7" and torrent 7's unpacking each keep a
// folder of their own, whichever claims first, and neither's removal
// takes the other's.
#[test]
fn an_nzb_job_named_as_a_torrents_unpacking_and_the_torrent_keep_their_own_folders() {
    let (nzb, torrent) = (JobId(9), JobId(7));
    for nzb_first in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("intermediate");
        let (nzb_dir, torrent_dir) = if nzb_first {
            let nzb_dir = claim_working_dir(&root, nzb, "torrent-7").unwrap().path;
            let torrent_dir = own_named_dir(&root, "torrent-7", torrent);
            claim_named_working_dir(&root, &torrent_dir, torrent).unwrap();
            (nzb_dir, torrent_dir)
        } else {
            let torrent_dir = own_named_dir(&root, "torrent-7", torrent);
            claim_named_working_dir(&root, &torrent_dir, torrent).unwrap();
            let nzb_dir = claim_working_dir(&root, nzb, "torrent-7").unwrap().path;
            (nzb_dir, torrent_dir)
        };
        assert_ne!(nzb_dir, torrent_dir, "nzb first: {nzb_first}");
        assert_eq!(
            own_named_dir(&root, "torrent-7", torrent),
            torrent_dir,
            "asked again, the torrent names the same folder"
        );
        assert!(is_job_working_dir(&root, &nzb_dir, nzb).unwrap());
        assert!(is_job_working_dir(&root, &torrent_dir, torrent).unwrap());
        std::fs::write(nzb_dir.join("nzb.bin"), b"n").unwrap();
        std::fs::write(torrent_dir.join("torrent.bin"), b"t").unwrap();

        assert!(remove_job_working_dir(&root, &nzb_dir, torrent).is_err());
        assert!(remove_job_working_dir(&root, &torrent_dir, nzb).is_err());
        assert_eq!(
            remove_job_working_dir(&root, &torrent_dir, torrent).unwrap(),
            HistoryWorkingDir::Owned
        );
        assert!(!torrent_dir.exists());
        assert!(nzb_dir.join("nzb.bin").exists(), "the NZB job's stays");
    }
}

// A torrent's unpacking that took its suffixed folder because an NZB
// job held the plain name keeps naming that folder once the NZB job's
// is gone: the name is the one its marker is in, never recomputed back.
#[test]
fn a_torrents_suffixed_unpacking_folder_keeps_its_name_once_the_plain_one_is_free() {
    let (nzb, torrent) = (JobId(9), JobId(7));
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("intermediate");
    let nzb_dir = claim_working_dir(&root, nzb, "torrent-7").unwrap().path;
    let torrent_dir = own_named_dir(&root, "torrent-7", torrent);
    assert_ne!(torrent_dir, nzb_dir);
    claim_named_working_dir(&root, &torrent_dir, torrent).unwrap();
    remove_job_working_dir(&root, &nzb_dir, nzb).unwrap();
    assert!(!nzb_dir.exists());
    assert_eq!(own_named_dir(&root, "torrent-7", torrent), torrent_dir);
}
