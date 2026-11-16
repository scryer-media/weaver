// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::{Path, PathBuf};

use super::{plan_outputs, walk};

// `plan_outputs` as it was, a scan of `listed` and `spent` per file found:
// what the sets must give, file for file and in the same order.
fn planned_by_scans(work: &Path, dir: &Path, listed: &[PathBuf], spent: &[PathBuf]) -> Vec<String> {
    let mut found = Vec::new();
    walk(work, Path::new(""), &mut found);
    found
        .into_iter()
        .filter(|relative| {
            let own = relative.components().next().is_some_and(|first| {
                crate::pipeline::graph::restore::the_jobs(Path::new(first.as_os_str()))
            });
            !own && !listed.contains(relative)
                && !spent.contains(&work.join(relative))
                && !dir.join(relative).exists()
        })
        .map(|relative| relative.to_string_lossy().into_owned())
        .collect()
}

// Of a job's work with thousands of files, the outputs are those that are
// neither the torrent's own, nor spent, nor the job's records, nor already
// beside the torrent's files: the same names, in the same order, as the
// scans gave.
#[test]
fn outputs_are_planned_as_the_scans_planned_them() {
    let root = tempfile::tempdir().unwrap();
    let work = root.path().join("work");
    let dir = root.path().join("dir");
    let mut listed = Vec::new();
    let mut spent = Vec::new();
    for index in 0..3_000 {
        let relative = PathBuf::from(format!("set/part{index:04}.bin"));
        let path = work.join(&relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"x").unwrap();
        match index % 5 {
            0 => listed.push(relative),
            1 => spent.push(path),
            2 => {
                let there = dir.join(&relative);
                std::fs::create_dir_all(there.parent().unwrap()).unwrap();
                std::fs::write(there, b"y").unwrap();
            }
            _ => {}
        }
    }
    let own = work.join(weaver_graph::names::INTERNAL_DIR).join("state");
    std::fs::create_dir_all(own.parent().unwrap()).unwrap();
    std::fs::write(own, b"z").unwrap();

    let planned = plan_outputs(&work, &dir, &listed, &spent);
    let expected = planned_by_scans(&work, &dir, &listed, &spent);
    assert_eq!(expected.len(), 1_200);
    assert_eq!(planned.names, expected);
}
