// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job whose complete directory is on another device than its working
// directory delivers into a staging directory there: only the files it
// delivers are written under it, and everything it keeps for itself stays
// in the working directory.

use super::*;

struct Split {
    job: Job,
    output: tempfile::TempDir,
}

impl Split {
    fn new() -> Self {
        let mut job = Job::new(PLENTY);
        let output = tempfile::tempdir().expect("output dir");
        let writer = std::mem::replace(
            &mut job.writer,
            NamespaceWriter::new(
                0,
                PathBuf::new(),
                Arc::clone(&job.accountant),
                CapacityReader::unknown(),
            ),
        );
        job.writer = writer.delivering_to(output.path().to_path_buf(), CapacityReader::unknown());
        Self { job, output }
    }

    fn delivered(&self, name: &str) -> Vec<u8> {
        std::fs::read(self.output.path().join(name)).expect("delivered file")
    }

    // Every file under the output directory, by its path there.
    fn output_files(&self) -> Vec<String> {
        files_under(self.output.path())
    }

    fn working_files(&self) -> Vec<String> {
        files_under(self.job.dir.path())
    }
}

fn files_under(root: &Path) -> Vec<String> {
    let mut found = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir").flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                let name = path.strip_prefix(root).expect("under root");
                found.push(name.to_string_lossy().replace('\\', "/"));
            }
        }
    }
    found.sort();
    found
}

#[test]
fn a_store_volume_stays_in_the_working_directory_and_its_member_is_delivered() {
    let mut split = Split::new();
    bound_store(&mut split.job);
    assert_eq!(split.delivered("inner.bin"), b"hello");
    assert_eq!(split.output_files(), ["inner.bin"]);
    let working = split.working_files();
    assert!(!working.iter().any(|name| name == "pack.bin" || name == "inner.bin"));
    assert!(
        working.iter().any(|name| name.starts_with(weaver_graph::names::INTERNAL_DIR)),
        "the volume's envelope is kept in the working directory: {working:?}"
    );
}

#[test]
fn a_store_volume_under_an_obfuscated_name_stays_in_the_working_directory() {
    let mut split = Split::new();
    bound_store_at(&mut split.job, "3f9a0c7e21");
    assert_eq!(split.output_files(), ["inner.bin"]);
    assert!(!split.working_files().iter().any(|name| name == "3f9a0c7e21"));
}

#[test]
fn a_recovery_volume_is_never_written_under_the_output_directory() {
    let mut split = Split::new();
    let job = std::mem::replace(&mut split.job, Job::new(PLENTY));
    split.job = job.with_set(None);
    assert!(split.output_files().is_empty(), "{:?}", split.output_files());
    assert!(split.working_files().iter().any(|name| name == "set.par2"));
}

#[test]
fn a_plain_member_is_written_under_the_output_directory_only() {
    let mut split = Split::new();
    let member = split.job.member("notes.txt", Some(11));
    split.job.send(member, 0, b"plain words", Provenance::Fresh);
    assert_eq!(split.delivered("notes.txt"), b"plain words");
    assert!(!split.working_files().iter().any(|name| name == "notes.txt"));
}

#[test]
fn a_delivered_file_moves_to_its_new_name_within_the_output_directory() {
    let mut split = Split::new();
    let member = split.job.member("3f9a.bin", Some(5));
    split.job.send(member, 0, b"plain", Provenance::Fresh);
    let mut build = split.job.build();
    split
        .job
        .writer
        .rename(&mut split.job.tree, &mut build, NodeId::ROOT, member, "movie.mkv")
        .expect("rename");
    split.job.commit();
    assert_eq!(split.output_files(), ["movie.mkv"]);
    assert_eq!(split.delivered("movie.mkv"), b"plain");
}

#[test]
fn a_member_waiting_for_its_name_waits_under_the_output_directory() {
    let mut split = Split::new();
    let first = split.job.member("subs.srt", Some(5));
    let second = split.job.member("subs.srt", Some(6));
    split.job.send(first, 0, b"first", Provenance::Fresh);
    split.job.send(second, 0, b"second", Provenance::Fresh);
    split.job.commit();
    assert_eq!(split.delivered("subs.srt"), b"first");
    assert_eq!(split.delivered("subs (1).srt"), b"second");
    assert!(split.working_files().iter().all(|name| !name.ends_with(".srt")));
}

#[test]
fn a_restored_file_is_found_under_the_directory_it_lies_in() {
    let mut split = Split::new();
    std::fs::write(split.output.path().join("kept.bin"), b"kept").expect("seed");
    let working = split.job.dir.path().to_path_buf();
    let leaf = split
        .job
        .writer
        .restored_leaf((NodeId::ROOT, MemberId(7)), "kept.bin", None, &working)
        .expect("restored");
    assert!(leaf.on_disk());
    assert_eq!(leaf.path(), split.output.path().join("kept.bin"));
}
