// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::{place_outputs, walk};

// The outputs as a scan of `listed` and `spent` per file found gives them:
// what the sets must give, file for file and in the same order.
fn found_by_scans(work: &Path, listed: &[PathBuf], spent: &[PathBuf]) -> Vec<String> {
    let mut found = Vec::new();
    walk(work, Path::new(""), &mut found);
    found
        .into_iter()
        .filter(|relative| {
            let own = relative.components().next().is_some_and(|first| {
                crate::pipeline::graph::restore::the_jobs(Path::new(first.as_os_str()))
            });
            !own && !listed.contains(relative) && !spent.contains(&work.join(relative))
        })
        .map(|relative| relative.to_string_lossy().into_owned())
        .collect()
}

// Of a job's work with thousands of files, the outputs are those that are
// neither the torrent's own, nor spent, nor the job's records: the same
// names, in the same order, as the scans gave, each placed under its own
// name where nothing holds it.
#[test]
fn outputs_are_placed_as_the_scans_found_them() {
    let root = tempfile::tempdir().unwrap();
    let work = root.path().join("work");
    let dir = root.path().join("dir");
    let mut listed = Vec::new();
    let mut spent = Vec::new();
    for index in 0..3_000 {
        let relative = match index % 5 {
            2 => Path::new(weaver_graph::names::INTERNAL_DIR).join(format!("state{index:04}")),
            _ => PathBuf::from(format!("set/part{index:04}.bin")),
        };
        let path = work.join(&relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"x").unwrap();
        match index % 5 {
            0 => listed.push(relative),
            1 => spent.push(path),
            _ => {}
        }
    }

    let expected = found_by_scans(&work, &listed, &spent);
    let placed = place_outputs(
        &work,
        &dir,
        &listed.iter().cloned().collect(),
        &spent.iter().cloned().collect(),
        &[],
        crate::runtime::fs::move_file_exclusive,
    );
    assert!(placed.failed.is_none());
    assert_eq!(expected.len(), 1_200);
    assert_eq!(placed.names, expected);
    let wanted: HashSet<String> = (0..3_000)
        .filter(|index| index % 5 > 2)
        .map(|index| format!("set/part{index:04}.bin"))
        .collect();
    assert_eq!(placed.names.iter().cloned().collect::<HashSet<_>>(), wanted);
    let mut there = Vec::new();
    walk(&dir, Path::new(""), &mut there);
    assert_eq!(there.len(), 1_200, "one file for each, and no other");
}
