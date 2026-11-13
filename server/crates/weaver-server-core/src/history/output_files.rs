// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The files of a job's output folder, as the job page lists them: by file
//! name, ASCII case-insensitive, then by the path relative to the folder.
//! A walk keeps only the first files of that order, however large the
//! folder, and counts every file and byte. Weaver's own ownership markers
//! are not output and are left out.

use std::collections::BinaryHeap;
use std::io;
use std::path::Path;

/// How many files a history entry keeps the record of when its files are
/// removed. The totals always cover the whole folder.
pub const REMOVED_FILES_KEPT: usize = 5_000;

/// One file of an output folder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputFile {
    /// The path relative to the output folder.
    pub rel_path: String,
    pub name: String,
    pub size_bytes: u64,
}

/// The first files of a folder in listing order, and the totals of all of it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutputFileListing {
    pub files: Vec<OutputFile>,
    pub total_count: u64,
    pub total_bytes: u64,
}

// Listing order: lowercased name, then relative path, which is unique.
type Keyed = (String, String, String, u64);

struct Selection {
    keep: usize,
    // The largest kept key on top, so a smaller one replaces it.
    kept: BinaryHeap<Keyed>,
    total_count: u64,
    total_bytes: u64,
}

impl Selection {
    fn offer(&mut self, rel_path: String, name: String, size_bytes: u64) {
        self.total_count += 1;
        self.total_bytes = self.total_bytes.saturating_add(size_bytes);
        if self.keep == 0 {
            return;
        }
        let key = (name.to_ascii_lowercase(), rel_path, name, size_bytes);
        if self.kept.len() < self.keep {
            self.kept.push(key);
        } else if self.kept.peek().is_some_and(|largest| key < *largest) {
            self.kept.pop();
            self.kept.push(key);
        }
    }

    fn finish(self) -> OutputFileListing {
        OutputFileListing {
            files: self
                .kept
                .into_sorted_vec()
                .into_iter()
                .map(|(_, rel_path, name, size_bytes)| OutputFile {
                    rel_path,
                    name,
                    size_bytes,
                })
                .collect(),
            total_count: self.total_count,
            total_bytes: self.total_bytes,
        }
    }
}

/// Walks `dir` and keeps its first `keep` files in listing order, holding
/// no more than `keep` of them at a time. A symbolic link is neither listed
/// nor walked, so the listing is what a removal records. A folder that
/// cannot be read is an error; an entry whose metadata cannot be read is
/// passed over. Blocks: call it off the async runtime.
pub fn list_output_files(dir: &Path, keep: usize) -> io::Result<OutputFileListing> {
    let mut selection = Selection {
        keep,
        kept: BinaryHeap::with_capacity(keep.min(1024)),
        total_count: 0,
        total_bytes: 0,
    };
    walk(dir, dir, &mut selection)?;
    Ok(selection.finish())
}

// The files Weaver writes to mark a folder as its own: bookkeeping, not
// output, so never listed or counted.
fn is_ownership_marker(name: &str) -> bool {
    name == crate::jobs::working_dir::WORKING_DIR_MARKER
        || name == crate::jobs::working_dir::OUTPUT_DIR_MARKER
}

fn walk(root: &Path, dir: &Path, selection: &mut Selection) -> io::Result<()> {
    for entry in std::fs::read_dir(dir)?.flatten() {
        let path = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_dir() {
            walk(root, &path, selection)?;
            continue;
        }
        let rel_path = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned();
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        if is_ownership_marker(&name) {
            continue;
        }
        selection.offer(rel_path, name, meta.len());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(root: &Path, rel: &str, bytes: usize) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, vec![b'x'; bytes]).unwrap();
    }

    fn names(listing: &OutputFileListing) -> Vec<&str> {
        listing
            .files
            .iter()
            .map(|file| file.rel_path.as_str())
            .collect()
    }

    // Files sort by name, ignoring ASCII case, then by path; the totals
    // count every file.
    #[test]
    fn files_are_in_listing_order_with_whole_totals() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "b.mkv", 3);
        write(dir.path(), "A.nfo", 1);
        write(dir.path(), "Sub/a.nfo", 2);
        write(dir.path(), "Other/B.MKV", 4);
        write(dir.path(), "c.txt", 5);
        write(dir.path(), crate::jobs::working_dir::OUTPUT_DIR_MARKER, 9);
        write(
            dir.path(),
            &format!("Sub/{}", crate::jobs::working_dir::WORKING_DIR_MARKER),
            9,
        );

        let all = list_output_files(dir.path(), 100).unwrap();
        let sep = std::path::MAIN_SEPARATOR;
        assert_eq!(
            names(&all),
            [
                "A.nfo".to_string(),
                format!("Sub{sep}a.nfo"),
                format!("Other{sep}B.MKV"),
                "b.mkv".to_string(),
                "c.txt".to_string(),
            ]
        );
        assert_eq!((all.total_count, all.total_bytes), (5, 15));
        assert_eq!(all.files[0].name, "A.nfo");
        assert_eq!(all.files[0].size_bytes, 1);

        // A smaller keep is the first files of the same order, and the same
        // totals.
        for keep in 0..=5 {
            let some = list_output_files(dir.path(), keep).unwrap();
            assert_eq!(some.files, all.files[..keep]);
            assert_eq!((some.total_count, some.total_bytes), (5, 15));
        }
    }

    // A walk lists no link and walks no linked folder.
    #[cfg(unix)]
    #[test]
    fn symbolic_links_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        write(dir.path(), "real.mkv", 2);
        write(outside.path(), "elsewhere.mkv", 7);
        std::os::unix::fs::symlink(outside.path(), dir.path().join("linked")).unwrap();
        std::os::unix::fs::symlink(
            outside.path().join("elsewhere.mkv"),
            dir.path().join("link.mkv"),
        )
        .unwrap();

        let skipped = list_output_files(dir.path(), 10).unwrap();
        assert_eq!(names(&skipped), ["real.mkv"]);
        assert_eq!((skipped.total_count, skipped.total_bytes), (1, 2));
    }

    #[test]
    fn a_missing_folder_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(list_output_files(&dir.path().join("absent"), 10).is_err());
    }
}
