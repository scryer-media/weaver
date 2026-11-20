// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

const JOB: JobId = JobId(40243);

// A working directory whose spool directory holds `held-a`, four bytes.
fn working_dir() -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let spool = held_spool_dir(temp.path());
    std::fs::create_dir(&spool).unwrap();
    std::fs::write(spool.join("held-a"), b"tail").unwrap();
    temp
}

// A uuencode part's row naming `spool_path`, four bytes long.
fn row(spool_path: &str) -> HeldPartRow {
    HeldPartRow {
        file: 0,
        ordinal: 1,
        decoded_len: 4,
        spool_path: spool_path.to_owned(),
        article: None,
    }
}

fn restored(working_dir: &Path, rows: &[HeldPartRow]) -> Vec<PathBuf> {
    held_parts_from_rows(JOB, working_dir, rows)
        .into_iter()
        .map(|part| part.path)
        .collect()
}

#[test]
fn a_held_row_naming_its_spool_file_is_restored() {
    let temp = working_dir();
    assert_eq!(
        restored(temp.path(), &[row(".weaver-chunks/held-a")]),
        vec![held_spool_dir(temp.path()).join("held-a")]
    );
}

#[test]
fn a_held_row_whose_path_climbs_out_of_the_spool_is_refused() {
    let temp = working_dir();
    std::fs::write(temp.path().join("held-b"), b"tail").unwrap();
    assert!(restored(temp.path(), &[row(".weaver-chunks/../held-b")]).is_empty());
}

#[test]
fn a_held_row_whose_path_has_a_current_dir_component_is_refused() {
    let temp = working_dir();
    assert!(restored(temp.path(), &[row("./.weaver-chunks/held-a")]).is_empty());
}

#[test]
fn a_held_row_with_an_absolute_path_is_refused() {
    let temp = working_dir();
    let outside = tempfile::tempdir().unwrap();
    let elsewhere = outside.path().join("held-c");
    std::fs::write(&elsewhere, b"tail").unwrap();
    assert!(restored(temp.path(), &[row(elsewhere.to_str().unwrap())]).is_empty());
}

#[cfg(unix)]
#[test]
fn a_held_row_naming_a_symlink_is_refused() {
    let temp = working_dir();
    let outside = tempfile::tempdir().unwrap();
    let elsewhere = outside.path().join("held-d");
    std::fs::write(&elsewhere, b"tail").unwrap();
    std::os::unix::fs::symlink(&elsewhere, held_spool_dir(temp.path()).join("held-d")).unwrap();
    assert!(restored(temp.path(), &[row(".weaver-chunks/held-d")]).is_empty());
}

#[test]
fn a_held_row_whose_file_length_differs_is_refused() {
    let temp = working_dir();
    std::fs::write(held_spool_dir(temp.path()).join("held-e"), b"tai").unwrap();
    assert!(restored(temp.path(), &[row(".weaver-chunks/held-e")]).is_empty());
}

#[cfg(unix)]
#[test]
fn held_rows_under_a_symlinked_spool_directory_are_refused() {
    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("held-a"), b"tail").unwrap();
    std::os::unix::fs::symlink(outside.path(), held_spool_dir(temp.path())).unwrap();
    assert!(restored(temp.path(), &[row(".weaver-chunks/held-a")]).is_empty());
    // A spill refuses the same directory before it writes.
    assert!(prepare_held_spool_dir(&held_spool_dir(temp.path())).is_err());
}
