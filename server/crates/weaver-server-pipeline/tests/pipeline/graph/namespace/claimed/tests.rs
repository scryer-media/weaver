// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::graph::leaf::Leaf;

// A file system that gives no second name leaves a file at the name all
// the same, so the leaf that is told its file is there can be moved and
// measured before a byte is written to it.
#[test]
fn a_refused_second_name_still_leaves_a_file_the_leaf_can_move() {
    refused::SECOND_NAMES.set(true);
    let dir = tempfile::tempdir().unwrap();
    let from = dir.path().join("lantern.part2.rar");
    std::fs::write(&from, b"volume").unwrap();
    let at = dir.path().join("kept").join("envelope-1");

    second_name_or_empty(&from, &at);

    let mut leaf = Leaf::over(dir.path(), "kept/envelope-1", true);
    assert_eq!(leaf.len().unwrap(), 0, "nothing was copied");
    leaf.intend("kept/moved");
    assert_eq!(leaf.move_on().unwrap().as_deref(), Some("kept/envelope-1"));
    assert!(dir.path().join("kept").join("moved").is_file());
    assert!(!at.exists());
    assert_eq!(std::fs::read(&from).unwrap(), b"volume");
}

// Where the second name is given, it is the file itself and nothing
// else is made.
#[cfg(unix)]
#[test]
fn a_given_second_name_is_the_file_itself() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempfile::tempdir().unwrap();
    let from = dir.path().join("lantern.part2.rar");
    std::fs::write(&from, b"volume").unwrap();
    let at = dir.path().join("envelope-1");

    second_name_or_empty(&from, &at);

    assert_eq!(
        std::fs::metadata(&from).unwrap().ino(),
        std::fs::metadata(&at).unwrap().ino()
    );
}
