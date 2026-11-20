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

/// How many files a history entry keeps the record of when it is archived.
/// The totals always cover the whole folder.
pub const ARCHIVED_HISTORY_FILES_KEPT: usize = 5_000;

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

/// A place in listing order: the lowercased file name, then the relative
/// path as shown, then the relative path's own bytes. Two paths the display
/// form makes equal still differ in their bytes, so the key is unique.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ListingKey {
    pub name: String,
    pub rel_path: String,
    pub raw: Vec<u8>,
}

/// One page of a folder's files after a place in listing order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutputFilePage {
    /// The page's files, with the totals of the whole folder.
    pub listing: OutputFileListing,
    /// The place of the page's last file, where the next page starts.
    pub last: Option<ListingKey>,
    /// Whether any file comes after the page's last one, as this walk
    /// found the folder.
    pub more: bool,
}

// Listing order, then what the file shows.
type Keyed = (ListingKey, String, u64);

struct Selection {
    keep: usize,
    // Only files after this key are kept; every file is still counted.
    after: Option<ListingKey>,
    // The largest kept key on top, so a smaller one replaces it.
    kept: BinaryHeap<Keyed>,
    // The files after `after`, kept or not.
    following: u64,
    total_count: u64,
    total_bytes: u64,
}

impl Selection {
    fn offer(&mut self, key: ListingKey, name: String, size_bytes: u64) {
        self.total_count += 1;
        self.total_bytes = self.total_bytes.saturating_add(size_bytes);
        if self.after.as_ref().is_some_and(|after| key <= *after) {
            return;
        }
        self.following += 1;
        if self.keep == 0 {
            return;
        }
        let keyed = (key, name, size_bytes);
        if self.kept.len() < self.keep {
            self.kept.push(keyed);
        } else if self.kept.peek().is_some_and(|largest| keyed < *largest) {
            self.kept.pop();
            self.kept.push(keyed);
        }
    }

    fn finish(self) -> OutputFilePage {
        let kept = self.kept.into_sorted_vec();
        let more = self.following > kept.len() as u64;
        let last = kept.last().map(|(key, _, _)| key.clone());
        OutputFilePage {
            listing: OutputFileListing {
                files: kept
                    .into_iter()
                    .map(|(key, name, size_bytes)| OutputFile {
                        rel_path: key.rel_path,
                        name,
                        size_bytes,
                    })
                    .collect(),
                total_count: self.total_count,
                total_bytes: self.total_bytes,
            },
            last,
            more,
        }
    }
}

/// Walks `dir` and keeps its first `keep` files in listing order, holding
/// no more than `keep` of them at a time. A symbolic link is neither listed
/// nor walked, so the listing is what an archive records. A folder that
/// cannot be read is an error; an entry whose metadata cannot be read is
/// passed over. Blocks: call it off the async runtime.
pub fn list_output_files(dir: &Path, keep: usize) -> io::Result<OutputFileListing> {
    list_output_files_after(dir, None, keep).map(|page| page.listing)
}

/// As [`list_output_files`], keeping the first `keep` files that come after
/// `after` in listing order, so a later page holds no more than its own
/// files however deep into the folder it starts. The totals still count
/// every file.
pub fn list_output_files_after(
    dir: &Path,
    after: Option<&ListingKey>,
    keep: usize,
) -> io::Result<OutputFilePage> {
    let mut selection = Selection {
        keep,
        after: after.cloned(),
        kept: BinaryHeap::with_capacity(keep.min(1024)),
        following: 0,
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
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        if is_ownership_marker(&name) {
            continue;
        }
        let rel = path.strip_prefix(root).unwrap_or(&path);
        let key = ListingKey {
            name: name.to_ascii_lowercase(),
            rel_path: rel.to_string_lossy().into_owned(),
            raw: rel.as_os_str().as_encoded_bytes().to_vec(),
        };
        selection.offer(key, name, meta.len());
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

    // Pages taken one after another, each after the last file of the one
    // before, are the whole order, with the whole folder's totals, and
    // each says whether a file follows it.
    #[test]
    fn pages_after_a_key_are_the_files_that_follow_it() {
        let dir = tempfile::tempdir().unwrap();
        for (n, rel) in ["b.mkv", "A.nfo", "Sub/a.nfo", "Other/B.MKV", "c.txt"]
            .iter()
            .enumerate()
        {
            write(dir.path(), rel, n + 1);
        }
        let all = list_output_files(dir.path(), 100).unwrap();
        for keep in 1..=6 {
            let mut after = None;
            let mut seen = Vec::new();
            loop {
                let page = list_output_files_after(dir.path(), after.as_ref(), keep).unwrap();
                assert_eq!(
                    (page.listing.total_count, page.listing.total_bytes),
                    (5, 15)
                );
                seen.extend(page.listing.files);
                assert_eq!(page.more, seen.len() < all.files.len(), "keep {keep}");
                if !page.more {
                    break;
                }
                after = page.last;
            }
            assert_eq!(seen, all.files, "keep {keep}");
        }
    }

    // Whether a file follows the page is read from the walk, not from a
    // count against the position: files listed on earlier pages that have
    // since gone leave the rest still to come.
    #[test]
    fn a_page_says_more_follows_when_earlier_files_have_gone() {
        let dir = tempfile::tempdir().unwrap();
        for rel in ["a.mkv", "b.mkv", "c.mkv", "d.mkv", "e.mkv"] {
            write(dir.path(), rel, 1);
        }
        let first = list_output_files_after(dir.path(), None, 2).unwrap();
        assert!(first.more);
        std::fs::remove_file(dir.path().join("a.mkv")).unwrap();
        std::fs::remove_file(dir.path().join("b.mkv")).unwrap();
        let second = list_output_files_after(dir.path(), first.last.as_ref(), 2).unwrap();
        assert_eq!(names(&second.listing), ["c.mkv", "d.mkv"]);
        // Three files are left and two are past the page's position of
        // four, yet "e.mkv" is still to come.
        assert_eq!(second.listing.total_count, 3);
        assert!(second.more);
        let third = list_output_files_after(dir.path(), second.last.as_ref(), 2).unwrap();
        assert_eq!(names(&third.listing), ["e.mkv"]);
        assert!(!third.more);
    }

    // Two paths whose shown forms are equal, as two names that are not
    // valid text can be, are told apart by their bytes: a page that ends
    // on the first still lists the second.
    #[test]
    fn paths_equal_once_shown_are_ordered_by_their_bytes() {
        let key = |raw: &[u8]| ListingKey {
            name: "\u{fffd}.mkv".into(),
            rel_path: "\u{fffd}.mkv".into(),
            raw: raw.to_vec(),
        };
        let page = |after: Option<ListingKey>| {
            let mut selection = Selection {
                keep: 1,
                after,
                kept: BinaryHeap::new(),
                following: 0,
                total_count: 0,
                total_bytes: 0,
            };
            selection.offer(key(b"\xfe.mkv"), "\u{fffd}.mkv".into(), 2);
            selection.offer(key(b"\xff.mkv"), "\u{fffd}.mkv".into(), 3);
            selection.finish()
        };
        let first = page(None);
        assert_eq!(first.listing.files[0].size_bytes, 2);
        assert!(first.more);
        let second = page(first.last);
        assert_eq!(second.listing.files.len(), 1);
        assert_eq!(second.listing.files[0].size_bytes, 3);
        assert!(!second.more);
    }

    #[test]
    fn a_missing_folder_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(list_output_files(&dir.path().join("absent"), 10).is_err());
    }
}
